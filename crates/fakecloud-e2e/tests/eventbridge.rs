mod helpers;

use aws_sdk_eventbridge::types::{PutEventsRequestEntry, RuleState, Target};
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
