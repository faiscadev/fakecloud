mod helpers;

use aws_sdk_eventbridge::types::{
    ConnectionAuthorizationType, ConnectionState, CreateConnectionApiKeyAuthRequestParameters,
    CreateConnectionAuthRequestParameters, EndpointEventBus, FailoverConfig, Primary,
    PutEventsRequestEntry, ReplayDestination, RoutingConfig, RuleState, Secondary, Target,
};
use aws_sdk_sqs::types::QueueAttributeName;
use helpers::TestServer;

#[tokio::test]
async fn eb_list_default_bus() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client.list_event_buses().send().await.unwrap();
    let buses = resp.event_buses();
    assert!(buses.iter().any(|b| b.name().unwrap() == "default"));
}

#[tokio::test]
async fn eb_create_delete_event_bus() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .create_event_bus()
        .name("custom-bus")
        .send()
        .await
        .unwrap();
    assert!(resp.event_bus_arn().unwrap().contains("custom-bus"));

    let resp = client.list_event_buses().send().await.unwrap();
    assert!(resp
        .event_buses()
        .iter()
        .any(|b| b.name().unwrap() == "custom-bus"));

    client
        .delete_event_bus()
        .name("custom-bus")
        .send()
        .await
        .unwrap();

    let resp = client.list_event_buses().send().await.unwrap();
    assert!(!resp
        .event_buses()
        .iter()
        .any(|b| b.name().unwrap() == "custom-bus"));
}

#[tokio::test]
async fn eb_put_rule_with_targets() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    // Put rule
    let resp = client
        .put_rule()
        .name("my-rule")
        .event_pattern(r#"{"source": ["my.app"]}"#)
        .send()
        .await
        .unwrap();
    assert!(resp.rule_arn().unwrap().contains("my-rule"));

    // List rules
    let resp = client.list_rules().send().await.unwrap();
    assert_eq!(resp.rules().len(), 1);
    assert_eq!(resp.rules()[0].name().unwrap(), "my-rule");

    // Put targets
    client
        .put_targets()
        .rule("my-rule")
        .targets(
            Target::builder()
                .id("target-1")
                .arn("arn:aws:sqs:us-east-1:123456789012:my-queue")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // List targets
    let resp = client
        .list_targets_by_rule()
        .rule("my-rule")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.targets().len(), 1);
    assert_eq!(resp.targets()[0].id(), "target-1");

    // Remove targets
    client
        .remove_targets()
        .rule("my-rule")
        .ids("target-1")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_targets_by_rule()
        .rule("my-rule")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.targets().len(), 0);

    // Delete rule
    client.delete_rule().name("my-rule").send().await.unwrap();

    let resp = client.list_rules().send().await.unwrap();
    assert_eq!(resp.rules().len(), 0);
}

#[tokio::test]
async fn eb_put_events() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("my.app")
                .detail_type("OrderPlaced")
                .detail(r#"{"orderId": "123"}"#)
                .build(),
        )
        .entries(
            PutEventsRequestEntry::builder()
                .source("my.app")
                .detail_type("OrderShipped")
                .detail(r#"{"orderId": "456"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.failed_entry_count(), 0);
    assert_eq!(resp.entries().len(), 2);
    assert!(resp.entries()[0].event_id().is_some());
}

#[tokio::test]
async fn eb_cli_put_events() {
    let server = TestServer::start().await;
    let output = server
        .aws_cli(&[
            "events",
            "put-events",
            "--entries",
            r#"[{"Source":"test","DetailType":"Test","Detail":"{}"}]"#,
        ])
        .await;
    assert!(output.success(), "failed: {}", output.stderr_text());
    let json = output.stdout_json();
    assert_eq!(json["FailedEntryCount"], 0);
}

#[tokio::test]
async fn eb_schedule_fires_to_sqs() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;
    let sqs = server.sqs_client().await;

    // Create an SQS queue to receive scheduled events
    let queue = sqs
        .create_queue()
        .queue_name("schedule-target")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap();

    // Create a rule with a 1-second rate schedule
    eb.put_rule()
        .name("fast-schedule")
        .schedule_expression("rate(1 second)")
        .state(RuleState::Enabled)
        .send()
        .await
        .unwrap();

    // Add the SQS queue as a target
    eb.put_targets()
        .rule("fast-schedule")
        .targets(
            Target::builder()
                .id("sqs-target")
                .arn("arn:aws:sqs:us-east-1:123456789012:schedule-target")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Wait for the scheduler to fire at least once
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Receive messages from SQS
    let resp = sqs
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    let messages = resp.messages();
    assert!(
        !messages.is_empty(),
        "expected at least one scheduled event message in SQS"
    );

    // Verify the event content
    let body = messages[0].body().unwrap();
    let event: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(event["source"], "aws.events");
    assert_eq!(event["detail-type"], "Scheduled Event");
}

/// Verify EventBridge PutEvents delivers matching events to an SQS target.
#[tokio::test]
async fn eb_put_events_delivers_to_sqs_target() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;
    let sqs = server.sqs_client().await;

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("eb-delivery-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = attrs
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    // Create rule matching "app.orders" source
    eb.put_rule()
        .name("delivery-rule")
        .event_pattern(r#"{"source": ["app.orders"]}"#)
        .send()
        .await
        .unwrap();

    // Add SQS target
    eb.put_targets()
        .rule("delivery-rule")
        .targets(
            Target::builder()
                .id("sqs-delivery")
                .arn(&queue_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Put a matching event
    let resp = eb
        .put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("app.orders")
                .detail_type("OrderCreated")
                .detail(r#"{"orderId": "100"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.failed_entry_count(), 0);

    // Verify the event was delivered to SQS
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    assert_eq!(msgs.messages().len(), 1, "expected 1 event in SQS");
    let body: serde_json::Value = serde_json::from_str(msgs.messages()[0].body().unwrap()).unwrap();
    assert_eq!(body["source"], "app.orders");
    assert_eq!(body["detail-type"], "OrderCreated");
    assert_eq!(body["detail"]["orderId"], "100");
}

/// Verify EventBridge PutEvents succeeds when targeting Lambda, Logs, and StepFunctions
/// (stub delivery — no error, event is accepted).
#[tokio::test]
async fn eb_put_events_stub_targets_no_error() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;

    // Create rule
    eb.put_rule()
        .name("multi-target-rule")
        .event_pattern(r#"{"source": ["test.stubs"]}"#)
        .send()
        .await
        .unwrap();

    // Add Lambda, Logs, and StepFunctions targets
    eb.put_targets()
        .rule("multi-target-rule")
        .targets(
            Target::builder()
                .id("lambda-target")
                .arn("arn:aws:lambda:us-east-1:123456789012:function:my-func")
                .build()
                .unwrap(),
        )
        .targets(
            Target::builder()
                .id("logs-target")
                .arn("arn:aws:logs:us-east-1:123456789012:log-group:/aws/events/my-log")
                .build()
                .unwrap(),
        )
        .targets(
            Target::builder()
                .id("sfn-target")
                .arn("arn:aws:states:us-east-1:123456789012:stateMachine:my-machine")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Put a matching event — should succeed without errors
    let resp = eb
        .put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("test.stubs")
                .detail_type("TestEvent")
                .detail(r#"{"key": "value"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.failed_entry_count(), 0);
    assert_eq!(resp.entries().len(), 1);
    assert!(resp.entries()[0].event_id().is_some());
}

// ---- EventBridge Archive Tests ----

#[tokio::test]
async fn eb_archive_lifecycle() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    // Create archive
    let resp = client
        .create_archive()
        .archive_name("my-archive")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .event_pattern(r#"{"source": ["my.app"]}"#)
        .send()
        .await
        .unwrap();
    assert!(resp.archive_arn().unwrap().contains("my-archive"));

    // Describe archive
    let desc = client
        .describe_archive()
        .archive_name("my-archive")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.archive_name().unwrap(), "my-archive");

    // List archives
    let list = client.list_archives().send().await.unwrap();
    assert!(list
        .archives()
        .iter()
        .any(|a| a.archive_name().unwrap() == "my-archive"));

    // Update archive
    client
        .update_archive()
        .archive_name("my-archive")
        .description("Updated description")
        .send()
        .await
        .unwrap();

    // Delete archive
    client
        .delete_archive()
        .archive_name("my-archive")
        .send()
        .await
        .unwrap();

    let list = client.list_archives().send().await.unwrap();
    assert!(!list
        .archives()
        .iter()
        .any(|a| a.archive_name().unwrap() == "my-archive"));
}

// ---- EventBridge Connection Tests ----

#[tokio::test]
async fn eb_connection_lifecycle() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    use aws_sdk_eventbridge::types::{
        ConnectionAuthorizationType, CreateConnectionApiKeyAuthRequestParameters,
        CreateConnectionAuthRequestParameters,
    };

    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret123")
                .build()
                .unwrap(),
        )
        .build();

    let resp = client
        .create_connection()
        .name("my-connection")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();
    assert!(resp.connection_arn().unwrap().contains("my-connection"));

    // Describe
    let desc = client
        .describe_connection()
        .name("my-connection")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.name().unwrap(), "my-connection");

    // List
    let list = client.list_connections().send().await.unwrap();
    assert!(list
        .connections()
        .iter()
        .any(|c| c.name().unwrap() == "my-connection"));

    // Delete
    client
        .delete_connection()
        .name("my-connection")
        .send()
        .await
        .unwrap();
}

// ---- EventBridge Rule Management Tests ----

#[tokio::test]
async fn eb_describe_enable_disable_rule() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("toggle-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .state(RuleState::Enabled)
        .send()
        .await
        .unwrap();

    // Describe
    let desc = client
        .describe_rule()
        .name("toggle-rule")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.name().unwrap(), "toggle-rule");

    // Disable
    client
        .disable_rule()
        .name("toggle-rule")
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_rule()
        .name("toggle-rule")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.state().unwrap(), &RuleState::Disabled);

    // Enable
    client
        .enable_rule()
        .name("toggle-rule")
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_rule()
        .name("toggle-rule")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.state().unwrap(), &RuleState::Enabled);
}

// ---- EventBridge Tag Tests ----

#[tokio::test]
async fn eb_tag_untag_rule() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .put_rule()
        .name("tag-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();
    let rule_arn = resp.rule_arn().unwrap().to_string();

    use aws_sdk_eventbridge::types::Tag;
    client
        .tag_resource()
        .resource_arn(&rule_arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(&rule_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);

    client
        .untag_resource()
        .resource_arn(&rule_arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(&rule_arn)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

// ---- EventBridge Error Cases ----

#[tokio::test]
async fn eb_describe_nonexistent_rule_fails() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let result = client.describe_rule().name("ghost-rule").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn eb_delete_nonexistent_event_bus() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    // Deleting a non-existent bus may return error or succeed silently
    let _ = client.delete_event_bus().name("no-such-bus").send().await;
}

// ---- EventBridge Multiple Targets ----

#[tokio::test]
async fn eb_rule_with_multiple_targets() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("multi-target")
        .event_pattern(r#"{"source": ["multi"]}"#)
        .send()
        .await
        .unwrap();

    client
        .put_targets()
        .rule("multi-target")
        .targets(
            Target::builder()
                .id("t1")
                .arn("arn:aws:sqs:us-east-1:123456789012:queue1")
                .build()
                .unwrap(),
        )
        .targets(
            Target::builder()
                .id("t2")
                .arn("arn:aws:sqs:us-east-1:123456789012:queue2")
                .build()
                .unwrap(),
        )
        .targets(
            Target::builder()
                .id("t3")
                .arn("arn:aws:sns:us-east-1:123456789012:topic1")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let targets = client
        .list_targets_by_rule()
        .rule("multi-target")
        .send()
        .await
        .unwrap();
    assert_eq!(targets.targets().len(), 3);

    // Remove one target
    client
        .remove_targets()
        .rule("multi-target")
        .ids("t2")
        .send()
        .await
        .unwrap();

    let targets = client
        .list_targets_by_rule()
        .rule("multi-target")
        .send()
        .await
        .unwrap();
    assert_eq!(targets.targets().len(), 2);
}

/// Regression: ListRules with an out-of-range NextToken should not panic.
#[tokio::test]
async fn eb_list_rules_pagination_out_of_range() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    // Create one rule so the list is non-empty
    client
        .put_rule()
        .name("pagination-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();

    // Use a NextToken far beyond the number of rules (should return empty, not panic)
    let resp = client.list_rules().next_token("9999").send().await.unwrap();
    assert!(
        resp.rules().is_empty(),
        "expected empty rules with out-of-range token, got {} rules",
        resp.rules().len()
    );
    assert!(
        resp.next_token().is_none(),
        "expected no next token for out-of-range pagination"
    );
}

// ---- TestEventPattern E2E ----

#[tokio::test]
async fn eb_test_event_pattern_match() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .test_event_pattern()
        .event_pattern(r#"{"source": ["my.app"]}"#)
        .event(r#"{"source": "my.app", "detail-type": "Test", "detail": {}}"#)
        .send()
        .await
        .unwrap();

    assert!(resp.result());
}

#[tokio::test]
async fn eb_test_event_pattern_no_match() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .test_event_pattern()
        .event_pattern(r#"{"source": ["other.app"]}"#)
        .event(r#"{"source": "my.app", "detail-type": "Test", "detail": {}}"#)
        .send()
        .await
        .unwrap();

    assert!(!resp.result());
}

// ---- Endpoint CRUD E2E ----

#[tokio::test]
async fn eb_endpoint_lifecycle() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let routing = RoutingConfig::builder()
        .failover_config(
            FailoverConfig::builder()
                .primary(Primary::builder().health_check("").build().unwrap())
                .secondary(Secondary::builder().route("us-west-2").build().unwrap())
                .build(),
        )
        .build();

    let event_buses = vec![EndpointEventBus::builder()
        .event_bus_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .build()
        .unwrap()];

    // Create
    let resp = client
        .create_endpoint()
        .name("my-endpoint")
        .routing_config(routing.clone())
        .set_event_buses(Some(event_buses))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "my-endpoint");

    // Describe
    let resp = client
        .describe_endpoint()
        .name("my-endpoint")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "my-endpoint");
    assert!(resp.endpoint_id().is_some());

    // List
    let resp = client.list_endpoints().send().await.unwrap();
    assert!(resp
        .endpoints()
        .iter()
        .any(|e| e.name().unwrap() == "my-endpoint"));

    // Update
    client
        .update_endpoint()
        .name("my-endpoint")
        .description("updated description")
        .routing_config(routing)
        .send()
        .await
        .unwrap();

    // Verify update
    let resp = client
        .describe_endpoint()
        .name("my-endpoint")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.description().unwrap(), "updated description");

    // Delete
    client
        .delete_endpoint()
        .name("my-endpoint")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client.describe_endpoint().name("my-endpoint").send().await;
    assert!(result.is_err());
}

// ---- DeauthorizeConnection E2E ----

#[tokio::test]
async fn eb_deauthorize_connection() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret123")
                .build()
                .unwrap(),
        )
        .build();

    client
        .create_connection()
        .name("deauth-test")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();

    let resp = client
        .deauthorize_connection()
        .name("deauth-test")
        .send()
        .await
        .unwrap();

    assert!(resp.connection_arn().unwrap().contains("deauth-test"));

    // Verify state changed
    let desc = client
        .describe_connection()
        .name("deauth-test")
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.connection_state().unwrap(),
        &ConnectionState::Deauthorizing
    );
}

// ---- Archive Replay Delivery E2E ----

#[tokio::test]
async fn eventbridge_archive_replay_delivers_events() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;
    let sqs = server.sqs_client().await;

    // Create SQS queue for delivery
    let queue = sqs
        .create_queue()
        .queue_name("replay-delivery-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = attrs
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    // Create a rule matching "replay.app" source
    eb.put_rule()
        .name("replay-rule")
        .event_pattern(r#"{"source": ["replay.app"]}"#)
        .state(RuleState::Enabled)
        .send()
        .await
        .unwrap();

    // Add SQS target
    eb.put_targets()
        .rule("replay-rule")
        .targets(
            Target::builder()
                .id("sqs-replay-target")
                .arn(&queue_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Create an archive on the default bus
    let create_resp = eb
        .create_archive()
        .archive_name("replay-test-archive")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .send()
        .await
        .unwrap();
    let archive_arn = create_resp.archive_arn().unwrap().to_string();

    // Put events
    let resp = eb
        .put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("replay.app")
                .detail_type("OrderCreated")
                .detail(r#"{"orderId": "100"}"#)
                .build(),
        )
        .entries(
            PutEventsRequestEntry::builder()
                .source("replay.app")
                .detail_type("OrderShipped")
                .detail(r#"{"orderId": "200"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.failed_entry_count(), 0);

    // Drain the SQS messages from PutEvents delivery
    let _ = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    // Purge the queue
    let _ = sqs.purge_queue().queue_url(&queue_url).send().await;

    // Small delay for purge to take effect
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Start replay
    let replay_resp = eb
        .start_replay()
        .replay_name("my-replay-test")
        .event_source_arn(&archive_arn)
        .destination(
            ReplayDestination::builder()
                .arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
                .build()
                .unwrap(),
        )
        .event_start_time(aws_sdk_eventbridge::primitives::DateTime::from_secs(0))
        .event_end_time(aws_sdk_eventbridge::primitives::DateTime::from_secs(
            chrono::Utc::now().timestamp() + 3600,
        ))
        .send()
        .await
        .unwrap();
    assert!(replay_resp.replay_arn().is_some());

    // Receive replayed messages
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    assert_eq!(
        msgs.messages().len(),
        2,
        "expected 2 replayed events in SQS"
    );

    // Verify the replayed events
    for msg in msgs.messages() {
        let body: serde_json::Value = serde_json::from_str(msg.body().unwrap()).unwrap();
        assert_eq!(body["source"], "replay.app");
        assert!(body["replay-name"].as_str().is_some());
    }
}

// ---- UpdateEventBus E2E ----

#[tokio::test]
async fn eb_update_event_bus() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_event_bus()
        .name("update-bus")
        .send()
        .await
        .unwrap();

    client
        .update_event_bus()
        .name("update-bus")
        .description("new desc")
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_event_bus()
        .name("update-bus")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.description().unwrap(), "new desc");
}

#[tokio::test]
async fn eventbridge_introspection_history() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let entry = PutEventsRequestEntry::builder()
        .source("test.source")
        .detail_type("TestDetail")
        .detail(r#"{"key": "value"}"#)
        .build();

    client.put_events().entries(entry).send().await.unwrap();

    let url = format!("{}/_fakecloud/events/history", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let events = resp["events"].as_array().unwrap();
    assert!(!events.is_empty(), "expected at least one event");

    let evt = &events[events.len() - 1];
    assert_eq!(evt["source"], "test.source");
    assert_eq!(evt["detailType"], "TestDetail");
    assert_eq!(evt["detail"], r#"{"key": "value"}"#);
    assert_eq!(evt["busName"], "default");
    assert!(!evt["eventId"].as_str().unwrap().is_empty());
    assert!(!evt["timestamp"].as_str().unwrap().is_empty());

    // Verify deliveries structure exists
    assert!(resp["deliveries"]["lambda"].is_array());
    assert!(resp["deliveries"]["logs"].is_array());
}

#[tokio::test]
async fn eb_simulation_fire_rule_to_sqs() {
    let server = TestServer::start().await;
    let eb_client = server.eventbridge_client().await;
    let sqs_client = server.sqs_client().await;

    // Create SQS queue to use as target
    let q_resp = sqs_client
        .create_queue()
        .queue_name("fire-rule-target")
        .send()
        .await
        .unwrap();
    let queue_url = q_resp.queue_url().unwrap().to_string();

    // Get queue ARN
    let q_attrs = sqs_client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = q_attrs
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .clone();

    // Create a scheduled rule on the default bus
    eb_client
        .put_rule()
        .name("test-fire-rule")
        .schedule_expression("rate(1 hour)")
        .state(RuleState::Enabled)
        .send()
        .await
        .unwrap();

    // Add SQS target
    eb_client
        .put_targets()
        .rule("test-fire-rule")
        .targets(
            Target::builder()
                .id("sqs-target")
                .arn(&queue_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Fire the rule via simulation endpoint
    let url = format!("{}/_fakecloud/events/fire-rule", server.endpoint());
    let resp: serde_json::Value = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({
            "busName": "default",
            "ruleName": "test-fire-rule"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let targets = resp["targets"].as_array().unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0]["type"], "sqs");
    assert_eq!(targets[0]["arn"], queue_arn);

    // Verify message appeared in SQS queue
    let recv = sqs_client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert_eq!(recv.messages().len(), 1);
    let body: serde_json::Value = serde_json::from_str(recv.messages()[0].body().unwrap()).unwrap();
    assert_eq!(body["source"], "aws.events");
    assert_eq!(body["detail-type"], "Scheduled Event");

    // Verify event recorded in introspection
    let history_url = format!("{}/_fakecloud/events/history", server.endpoint());
    let history: serde_json::Value = reqwest::get(&history_url)
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let events = history["events"].as_array().unwrap();
    assert!(events.iter().any(|e| e["source"] == "aws.events"));
}

#[tokio::test]
async fn eb_put_events_delivers_to_kinesis_target() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;
    let kinesis = server.kinesis_client().await;

    // Create Kinesis stream
    kinesis
        .create_stream()
        .stream_name("eb-kinesis-target")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    // Create rule matching our source
    eb.put_rule()
        .name("kinesis-rule")
        .event_pattern(r#"{"source": ["app.kinesis"]}"#)
        .send()
        .await
        .unwrap();

    // Add Kinesis stream as target
    eb.put_targets()
        .rule("kinesis-rule")
        .targets(
            Target::builder()
                .id("kinesis-target")
                .arn("arn:aws:kinesis:us-east-1:123456789012:stream/eb-kinesis-target")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Get shard iterator before putting events
    let desc = kinesis
        .describe_stream()
        .stream_name("eb-kinesis-target")
        .send()
        .await
        .unwrap();
    let shard_id = desc
        .stream_description()
        .unwrap()
        .shards()
        .first()
        .unwrap()
        .shard_id()
        .to_string();

    let iter_resp = kinesis
        .get_shard_iterator()
        .stream_name("eb-kinesis-target")
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let shard_iterator = iter_resp.shard_iterator().unwrap().to_string();

    // Put a matching event
    let resp = eb
        .put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("app.kinesis")
                .detail_type("TestEvent")
                .detail(r#"{"key": "value"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.failed_entry_count(), 0);

    // Read the Kinesis record
    let records_resp = kinesis
        .get_records()
        .shard_iterator(&shard_iterator)
        .send()
        .await
        .unwrap();

    let records = records_resp.records();
    assert_eq!(records.len(), 1, "expected 1 event in Kinesis stream");

    // The data should be the EventBridge event JSON
    let event: serde_json::Value = serde_json::from_slice(records[0].data().as_ref()).unwrap();
    assert_eq!(event["source"], "app.kinesis");
    assert_eq!(event["detail-type"], "TestEvent");
    assert_eq!(event["detail"]["key"], "value");
}

/// Verify EventBridge rule targeting Step Functions actually starts an execution.
#[tokio::test]
async fn eb_rule_starts_stepfunctions_execution() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;
    let sfn = server.sfn_client().await;

    // Create a simple Pass state machine
    let definition = serde_json::json!({
        "StartAt": "PassState",
        "States": {
            "PassState": {
                "Type": "Pass",
                "End": true
            }
        }
    });
    let sm = sfn
        .create_state_machine()
        .name("eb-target-machine")
        .definition(definition.to_string())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .expect("create state machine");
    let sm_arn = sm.state_machine_arn();

    // Create EventBridge rule targeting the state machine
    eb.put_rule()
        .name("sfn-trigger")
        .event_pattern(r#"{"source": ["test.sfn"]}"#)
        .send()
        .await
        .unwrap();

    eb.put_targets()
        .rule("sfn-trigger")
        .targets(
            Target::builder()
                .id("sfn-target")
                .arn(sm_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Put a matching event
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("test.sfn")
                .detail_type("TriggerExecution")
                .detail(r#"{"message": "hello from EventBridge"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Give the async execution a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify execution was started
    let executions = sfn
        .list_executions()
        .state_machine_arn(sm_arn)
        .send()
        .await
        .expect("list executions");

    assert_eq!(
        executions.executions().len(),
        1,
        "expected 1 execution started by EventBridge rule"
    );
    let exec = &executions.executions()[0];
    assert_eq!(
        exec.status(),
        &aws_sdk_sfn::types::ExecutionStatus::Succeeded
    );
}
