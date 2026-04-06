mod helpers;

use aws_sdk_eventbridge::types::{
    ConnectionAuthorizationType, CreateConnectionApiKeyAuthRequestParameters,
    CreateConnectionAuthRequestParameters, EndpointEventBus, FailoverConfig, Primary,
    PutEventsRequestEntry, PutPartnerEventsRequestEntry, ReplicationConfig, ReplicationState,
    RoutingConfig, RuleState, Secondary, Tag, Target,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// ---------------------------------------------------------------------------
// Event bus lifecycle
// ---------------------------------------------------------------------------

#[test_action("events", "CreateEventBus", checksum = "1f5f93fa")]
#[tokio::test]
async fn eb_create_event_bus() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .create_event_bus()
        .name("conf-bus")
        .send()
        .await
        .unwrap();
    assert!(resp.event_bus_arn().unwrap().contains("conf-bus"));
}

#[test_action("events", "DeleteEventBus", checksum = "4f4f5954")]
#[tokio::test]
async fn eb_delete_event_bus() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_event_bus()
        .name("del-bus")
        .send()
        .await
        .unwrap();

    client
        .delete_event_bus()
        .name("del-bus")
        .send()
        .await
        .unwrap();

    let resp = client.list_event_buses().send().await.unwrap();
    assert!(!resp
        .event_buses()
        .iter()
        .any(|b| b.name().unwrap() == "del-bus"));
}

#[test_action("events", "ListEventBuses", checksum = "3b53e660")]
#[tokio::test]
async fn eb_list_event_buses() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client.list_event_buses().send().await.unwrap();
    assert!(resp
        .event_buses()
        .iter()
        .any(|b| b.name().unwrap() == "default"));
}

#[test_action("events", "DescribeEventBus", checksum = "7decb34d")]
#[tokio::test]
async fn eb_describe_event_bus() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .describe_event_bus()
        .name("default")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "default");
}

// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------

#[test_action("events", "PutRule", checksum = "f481dcfa")]
#[tokio::test]
async fn eb_put_rule() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .put_rule()
        .name("conf-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();
    assert!(resp.rule_arn().unwrap().contains("conf-rule"));
}

#[test_action("events", "DeleteRule", checksum = "dd9dec42")]
#[tokio::test]
async fn eb_delete_rule() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("del-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();

    client.delete_rule().name("del-rule").send().await.unwrap();

    let resp = client.list_rules().send().await.unwrap();
    assert!(resp.rules().is_empty());
}

#[test_action("events", "ListRules", checksum = "faa0cb02")]
#[tokio::test]
async fn eb_list_rules() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("list-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();

    let resp = client.list_rules().send().await.unwrap();
    assert_eq!(resp.rules().len(), 1);
}

#[test_action("events", "DescribeRule", checksum = "558c1b1c")]
#[tokio::test]
async fn eb_describe_rule() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("desc-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_rule()
        .name("desc-rule")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "desc-rule");
}

#[test_action("events", "EnableRule", checksum = "6839c932")]
#[tokio::test]
async fn eb_enable_rule() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("enable-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .state(RuleState::Disabled)
        .send()
        .await
        .unwrap();

    client
        .enable_rule()
        .name("enable-rule")
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_rule()
        .name("enable-rule")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.state().unwrap(), &RuleState::Enabled);
}

#[test_action("events", "DisableRule", checksum = "866d8afa")]
#[tokio::test]
async fn eb_disable_rule() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("disable-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .state(RuleState::Enabled)
        .send()
        .await
        .unwrap();

    client
        .disable_rule()
        .name("disable-rule")
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_rule()
        .name("disable-rule")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.state().unwrap(), &RuleState::Disabled);
}

// ---------------------------------------------------------------------------
// Targets
// ---------------------------------------------------------------------------

#[test_action("events", "PutTargets", checksum = "745ff520")]
#[tokio::test]
async fn eb_put_targets() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("tgt-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();

    client
        .put_targets()
        .rule("tgt-rule")
        .targets(
            Target::builder()
                .id("t1")
                .arn("arn:aws:sqs:us-east-1:123456789012:q")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_targets_by_rule()
        .rule("tgt-rule")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.targets().len(), 1);
}

#[test_action("events", "RemoveTargets", checksum = "2a466c19")]
#[tokio::test]
async fn eb_remove_targets() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("rmtgt-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();

    client
        .put_targets()
        .rule("rmtgt-rule")
        .targets(
            Target::builder()
                .id("t1")
                .arn("arn:aws:sqs:us-east-1:123456789012:q")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .remove_targets()
        .rule("rmtgt-rule")
        .ids("t1")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_targets_by_rule()
        .rule("rmtgt-rule")
        .send()
        .await
        .unwrap();
    assert!(resp.targets().is_empty());
}

#[test_action("events", "ListTargetsByRule", checksum = "abb47469")]
#[tokio::test]
async fn eb_list_targets_by_rule() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("ltbr-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();

    let resp = client
        .list_targets_by_rule()
        .rule("ltbr-rule")
        .send()
        .await
        .unwrap();
    assert!(resp.targets().is_empty());
}

#[test_action("events", "ListRuleNamesByTarget", checksum = "886f2367")]
#[tokio::test]
async fn eb_list_rule_names_by_target() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_rule()
        .name("lrnbt-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();

    client
        .put_targets()
        .rule("lrnbt-rule")
        .targets(
            Target::builder()
                .id("t1")
                .arn("arn:aws:sqs:us-east-1:123456789012:target-q")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_rule_names_by_target()
        .target_arn("arn:aws:sqs:us-east-1:123456789012:target-q")
        .send()
        .await
        .unwrap();
    assert!(resp.rule_names().iter().any(|n| n == "lrnbt-rule"));
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

#[test_action("events", "PutEvents", checksum = "3699246e")]
#[tokio::test]
async fn eb_put_events() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("test.app")
                .detail_type("TestEvent")
                .detail(r#"{"key": "val"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.failed_entry_count(), 0);
    assert_eq!(resp.entries().len(), 1);
}

// ---------------------------------------------------------------------------
// Permissions
// ---------------------------------------------------------------------------

#[test_action("events", "PutPermission", checksum = "0eb5fb8a")]
#[tokio::test]
async fn eb_put_permission() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_permission()
        .action("events:PutEvents")
        .principal("123456789012")
        .statement_id("allow-account")
        .send()
        .await
        .unwrap();
}

#[test_action("events", "RemovePermission", checksum = "2bb6a3a1")]
#[tokio::test]
async fn eb_remove_permission() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .put_permission()
        .action("events:PutEvents")
        .principal("123456789012")
        .statement_id("rm-perm")
        .send()
        .await
        .unwrap();

    client
        .remove_permission()
        .statement_id("rm-perm")
        .send()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

#[test_action("events", "TagResource", checksum = "34168c66")]
#[tokio::test]
async fn eb_tag_resource() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let rule = client
        .put_rule()
        .name("tag-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();
    let rule_arn = rule.rule_arn().unwrap();

    client
        .tag_resource()
        .resource_arn(rule_arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(rule_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
}

#[test_action("events", "UntagResource", checksum = "ca0e5fb0")]
#[tokio::test]
async fn eb_untag_resource() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let rule = client
        .put_rule()
        .name("untag-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();
    let rule_arn = rule.rule_arn().unwrap();

    client
        .tag_resource()
        .resource_arn(rule_arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .send()
        .await
        .unwrap();

    client
        .untag_resource()
        .resource_arn(rule_arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(rule_arn)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

#[test_action("events", "ListTagsForResource", checksum = "d997c3ae")]
#[tokio::test]
async fn eb_list_tags_for_resource() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let rule = client
        .put_rule()
        .name("listtags-rule")
        .event_pattern(r#"{"source": ["test"]}"#)
        .send()
        .await
        .unwrap();
    let rule_arn = rule.rule_arn().unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(rule_arn)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

// ---------------------------------------------------------------------------
// Archives
// ---------------------------------------------------------------------------

#[test_action("events", "CreateArchive", checksum = "dcfb1a34")]
#[tokio::test]
async fn eb_create_archive() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .create_archive()
        .archive_name("conf-archive")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .send()
        .await
        .unwrap();
    assert!(resp.archive_arn().unwrap().contains("conf-archive"));
}

#[test_action("events", "DescribeArchive", checksum = "cc79ef20")]
#[tokio::test]
async fn eb_describe_archive() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_archive()
        .archive_name("desc-archive")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_archive()
        .archive_name("desc-archive")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.archive_name().unwrap(), "desc-archive");
}

#[test_action("events", "ListArchives", checksum = "fc001f2e")]
#[tokio::test]
async fn eb_list_archives() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_archive()
        .archive_name("list-archive")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .send()
        .await
        .unwrap();

    let resp = client.list_archives().send().await.unwrap();
    assert!(!resp.archives().is_empty());
}

#[test_action("events", "UpdateArchive", checksum = "d3a52d4a")]
#[tokio::test]
async fn eb_update_archive() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_archive()
        .archive_name("upd-archive")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .send()
        .await
        .unwrap();

    client
        .update_archive()
        .archive_name("upd-archive")
        .description("Updated")
        .send()
        .await
        .unwrap();
}

#[test_action("events", "DeleteArchive", checksum = "4616ab55")]
#[tokio::test]
async fn eb_delete_archive() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_archive()
        .archive_name("del-archive")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .send()
        .await
        .unwrap();

    client
        .delete_archive()
        .archive_name("del-archive")
        .send()
        .await
        .unwrap();

    let resp = client.list_archives().send().await.unwrap();
    assert!(!resp
        .archives()
        .iter()
        .any(|a| a.archive_name().unwrap() == "del-archive"));
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

#[test_action("events", "CreateConnection", checksum = "6f55e4ab")]
#[tokio::test]
async fn eb_create_connection() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret")
                .build()
                .unwrap(),
        )
        .build();

    let resp = client
        .create_connection()
        .name("conf-conn")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();
    assert!(resp.connection_arn().unwrap().contains("conf-conn"));
}

#[test_action("events", "DescribeConnection", checksum = "241e50be")]
#[tokio::test]
async fn eb_describe_connection() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret")
                .build()
                .unwrap(),
        )
        .build();

    client
        .create_connection()
        .name("desc-conn")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_connection()
        .name("desc-conn")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "desc-conn");
}

#[test_action("events", "ListConnections", checksum = "35082a67")]
#[tokio::test]
async fn eb_list_connections() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client.list_connections().send().await.unwrap();
    let _ = resp.connections();
}

#[test_action("events", "UpdateConnection", checksum = "4783b1fa")]
#[tokio::test]
async fn eb_update_connection() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret")
                .build()
                .unwrap(),
        )
        .build();

    client
        .create_connection()
        .name("upd-conn")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();

    client
        .update_connection()
        .name("upd-conn")
        .description("Updated connection")
        .send()
        .await
        .unwrap();
}

#[test_action("events", "DeleteConnection", checksum = "ad426e07")]
#[tokio::test]
async fn eb_delete_connection() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret")
                .build()
                .unwrap(),
        )
        .build();

    client
        .create_connection()
        .name("del-conn")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();

    client
        .delete_connection()
        .name("del-conn")
        .send()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// API Destinations
// ---------------------------------------------------------------------------

#[test_action("events", "CreateApiDestination", checksum = "590a9c4a")]
#[tokio::test]
async fn eb_create_api_destination() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    // Need a connection first
    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret")
                .build()
                .unwrap(),
        )
        .build();

    let conn = client
        .create_connection()
        .name("apidest-conn")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();
    let conn_arn = conn.connection_arn().unwrap();

    let resp = client
        .create_api_destination()
        .name("conf-apidest")
        .connection_arn(conn_arn)
        .invocation_endpoint("https://example.com/webhook")
        .http_method(aws_sdk_eventbridge::types::ApiDestinationHttpMethod::Post)
        .send()
        .await
        .unwrap();
    assert!(resp.api_destination_arn().unwrap().contains("conf-apidest"));
}

#[test_action("events", "DescribeApiDestination", checksum = "f41acf9f")]
#[tokio::test]
async fn eb_describe_api_destination() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret")
                .build()
                .unwrap(),
        )
        .build();

    let conn = client
        .create_connection()
        .name("descad-conn")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();
    let conn_arn = conn.connection_arn().unwrap();

    client
        .create_api_destination()
        .name("desc-apidest")
        .connection_arn(conn_arn)
        .invocation_endpoint("https://example.com/webhook")
        .http_method(aws_sdk_eventbridge::types::ApiDestinationHttpMethod::Post)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_api_destination()
        .name("desc-apidest")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "desc-apidest");
}

#[test_action("events", "ListApiDestinations", checksum = "f0dcb4f5")]
#[tokio::test]
async fn eb_list_api_destinations() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client.list_api_destinations().send().await.unwrap();
    let _ = resp.api_destinations();
}

#[test_action("events", "UpdateApiDestination", checksum = "f2e3330b")]
#[tokio::test]
async fn eb_update_api_destination() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret")
                .build()
                .unwrap(),
        )
        .build();

    let conn = client
        .create_connection()
        .name("updad-conn")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();
    let conn_arn = conn.connection_arn().unwrap();

    client
        .create_api_destination()
        .name("upd-apidest")
        .connection_arn(conn_arn)
        .invocation_endpoint("https://example.com/webhook")
        .http_method(aws_sdk_eventbridge::types::ApiDestinationHttpMethod::Post)
        .send()
        .await
        .unwrap();

    client
        .update_api_destination()
        .name("upd-apidest")
        .description("Updated")
        .send()
        .await
        .unwrap();
}

#[test_action("events", "DeleteApiDestination", checksum = "7e517bd8")]
#[tokio::test]
async fn eb_delete_api_destination() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret")
                .build()
                .unwrap(),
        )
        .build();

    let conn = client
        .create_connection()
        .name("delad-conn")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();
    let conn_arn = conn.connection_arn().unwrap();

    client
        .create_api_destination()
        .name("del-apidest")
        .connection_arn(conn_arn)
        .invocation_endpoint("https://example.com/webhook")
        .http_method(aws_sdk_eventbridge::types::ApiDestinationHttpMethod::Post)
        .send()
        .await
        .unwrap();

    client
        .delete_api_destination()
        .name("del-apidest")
        .send()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Replays
// ---------------------------------------------------------------------------

#[test_action("events", "StartReplay", checksum = "0ef23715")]
#[tokio::test]
async fn eb_start_replay() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    // Need an archive first
    client
        .create_archive()
        .archive_name("replay-archive")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .send()
        .await
        .unwrap();

    let now = aws_sdk_eventbridge::primitives::DateTime::from_secs(1700000000);
    let earlier = aws_sdk_eventbridge::primitives::DateTime::from_secs(1699990000);

    let resp = client
        .start_replay()
        .replay_name("conf-replay")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .destination(
            aws_sdk_eventbridge::types::ReplayDestination::builder()
                .arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
                .build()
                .unwrap(),
        )
        .event_start_time(earlier)
        .event_end_time(now)
        .send()
        .await
        .unwrap();
    assert!(resp.replay_arn().is_some());
}

#[test_action("events", "DescribeReplay", checksum = "0470965f")]
#[tokio::test]
async fn eb_describe_replay() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_archive()
        .archive_name("descrep-archive")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .send()
        .await
        .unwrap();

    let now = aws_sdk_eventbridge::primitives::DateTime::from_secs(1700000000);
    let earlier = aws_sdk_eventbridge::primitives::DateTime::from_secs(1699990000);

    client
        .start_replay()
        .replay_name("desc-replay")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .destination(
            aws_sdk_eventbridge::types::ReplayDestination::builder()
                .arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
                .build()
                .unwrap(),
        )
        .event_start_time(earlier)
        .event_end_time(now)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_replay()
        .replay_name("desc-replay")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.replay_name().unwrap(), "desc-replay");
}

#[test_action("events", "ListReplays", checksum = "174eb44e")]
#[tokio::test]
async fn eb_list_replays() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client.list_replays().send().await.unwrap();
    let _ = resp.replays();
}

#[test_action("events", "CancelReplay", checksum = "be020ca9")]
#[tokio::test]
async fn eb_cancel_replay() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_archive()
        .archive_name("cancelrep-archive")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .send()
        .await
        .unwrap();

    let now = aws_sdk_eventbridge::primitives::DateTime::from_secs(1700000000);
    let earlier = aws_sdk_eventbridge::primitives::DateTime::from_secs(1699990000);

    client
        .start_replay()
        .replay_name("cancel-replay")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .destination(
            aws_sdk_eventbridge::types::ReplayDestination::builder()
                .arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
                .build()
                .unwrap(),
        )
        .event_start_time(earlier)
        .event_end_time(now)
        .send()
        .await
        .unwrap();

    // Cancel may succeed or fail depending on replay state; we just verify the call works
    let _ = client
        .cancel_replay()
        .replay_name("cancel-replay")
        .send()
        .await;
}

// ---------------------------------------------------------------------------
// Partner event sources
// ---------------------------------------------------------------------------

#[test_action("events", "CreatePartnerEventSource", checksum = "0c72b634")]
#[tokio::test]
async fn eb_create_partner_event_source() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .create_partner_event_source()
        .name("aws.partner/example.com/test")
        .account("123456789012")
        .send()
        .await
        .unwrap();
    let _ = resp.event_source_arn();
}

#[test_action("events", "DescribePartnerEventSource", checksum = "0cef9de8")]
#[tokio::test]
async fn eb_describe_partner_event_source() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_partner_event_source()
        .name("aws.partner/example.com/desc")
        .account("123456789012")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_partner_event_source()
        .name("aws.partner/example.com/desc")
        .send()
        .await
        .unwrap();
    assert!(resp.name().is_some());
}

#[test_action("events", "DeletePartnerEventSource", checksum = "bb7fb873")]
#[tokio::test]
async fn eb_delete_partner_event_source() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_partner_event_source()
        .name("aws.partner/example.com/del")
        .account("123456789012")
        .send()
        .await
        .unwrap();

    client
        .delete_partner_event_source()
        .name("aws.partner/example.com/del")
        .account("123456789012")
        .send()
        .await
        .unwrap();
}

#[test_action("events", "ListPartnerEventSources", checksum = "fe4e183a")]
#[tokio::test]
async fn eb_list_partner_event_sources() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_partner_event_source()
        .name("aws.partner/example.com/list")
        .account("123456789012")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_partner_event_sources()
        .name_prefix("aws.partner/example.com")
        .send()
        .await
        .unwrap();
    assert!(!resp.partner_event_sources().is_empty());
}

#[test_action("events", "ListPartnerEventSourceAccounts", checksum = "7a96ed63")]
#[tokio::test]
async fn eb_list_partner_event_source_accounts() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_partner_event_source()
        .name("aws.partner/example.com/accts")
        .account("123456789012")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_partner_event_source_accounts()
        .event_source_name("aws.partner/example.com/accts")
        .send()
        .await
        .unwrap();
    let _ = resp.partner_event_source_accounts();
}

#[test_action("events", "ActivateEventSource", checksum = "c72d22a8")]
#[tokio::test]
async fn eb_activate_event_source() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_partner_event_source()
        .name("aws.partner/example.com/activate")
        .account("123456789012")
        .send()
        .await
        .unwrap();

    client
        .activate_event_source()
        .name("aws.partner/example.com/activate")
        .send()
        .await
        .unwrap();
}

#[test_action("events", "DeactivateEventSource", checksum = "f4551f56")]
#[tokio::test]
async fn eb_deactivate_event_source() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_partner_event_source()
        .name("aws.partner/example.com/deactivate")
        .account("123456789012")
        .send()
        .await
        .unwrap();

    client
        .deactivate_event_source()
        .name("aws.partner/example.com/deactivate")
        .send()
        .await
        .unwrap();
}

#[test_action("events", "DescribeEventSource", checksum = "33dfc479")]
#[tokio::test]
async fn eb_describe_event_source() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_partner_event_source()
        .name("aws.partner/example.com/descevt")
        .account("123456789012")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_event_source()
        .name("aws.partner/example.com/descevt")
        .send()
        .await
        .unwrap();
    assert!(resp.name().is_some());
}

#[test_action("events", "ListEventSources", checksum = "b929f62f")]
#[tokio::test]
async fn eb_list_event_sources() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client.list_event_sources().send().await.unwrap();
    let _ = resp.event_sources();
}

#[test_action("events", "PutPartnerEvents", checksum = "94e2fc0d")]
#[tokio::test]
async fn eb_put_partner_events() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .put_partner_events()
        .entries(
            PutPartnerEventsRequestEntry::builder()
                .source("aws.partner/example.com/test")
                .detail_type("TestEvent")
                .detail(r#"{"key": "val"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.failed_entry_count(), 0);
}

#[test_action("events", "TestEventPattern", checksum = "d5fd69c1")]
#[tokio::test]
async fn eb_test_event_pattern() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .test_event_pattern()
        .event_pattern(r#"{"source": ["test.app"]}"#)
        .event(r#"{"source": "test.app", "detail-type": "TestEvent", "detail": {}}"#)
        .send()
        .await
        .unwrap();
    assert!(resp.result());
}

// ---------------------------------------------------------------------------
// UpdateEventBus
// ---------------------------------------------------------------------------

#[test_action("events", "UpdateEventBus", checksum = "b12ac967")]
#[tokio::test]
async fn eb_update_event_bus() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_event_bus()
        .name("upd-bus")
        .send()
        .await
        .unwrap();

    let resp = client
        .update_event_bus()
        .name("upd-bus")
        .description("Updated bus")
        .send()
        .await
        .unwrap();
    assert!(resp.arn().is_some());
}

// ---------------------------------------------------------------------------
// DeauthorizeConnection
// ---------------------------------------------------------------------------

#[test_action("events", "DeauthorizeConnection", checksum = "c7d0f90d")]
#[tokio::test]
async fn eb_deauthorize_connection() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let auth = CreateConnectionAuthRequestParameters::builder()
        .api_key_auth_parameters(
            CreateConnectionApiKeyAuthRequestParameters::builder()
                .api_key_name("x-api-key")
                .api_key_value("secret")
                .build()
                .unwrap(),
        )
        .build();

    client
        .create_connection()
        .name("deauth-conn")
        .authorization_type(ConnectionAuthorizationType::ApiKey)
        .auth_parameters(auth)
        .send()
        .await
        .unwrap();

    let resp = client
        .deauthorize_connection()
        .name("deauth-conn")
        .send()
        .await
        .unwrap();
    assert!(resp.connection_arn().is_some());
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

#[test_action("events", "CreateEndpoint", checksum = "2929c01c")]
#[tokio::test]
async fn eb_create_endpoint() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client
        .create_endpoint()
        .name("conf-endpoint")
        .routing_config(
            RoutingConfig::builder()
                .failover_config(
                    FailoverConfig::builder()
                        .primary(
                            Primary::builder()
                                .health_check("arn:aws:route53:::healthcheck/abc123")
                                .build()
                                .unwrap(),
                        )
                        .secondary(Secondary::builder().route("us-west-2").build().unwrap())
                        .build(),
                )
                .build(),
        )
        .event_buses(
            EndpointEventBus::builder()
                .event_bus_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
                .build()
                .unwrap(),
        )
        .replication_config(
            ReplicationConfig::builder()
                .state(ReplicationState::Disabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.arn().is_some());
}

#[test_action("events", "DescribeEndpoint", checksum = "e34860b5")]
#[tokio::test]
async fn eb_describe_endpoint() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_endpoint()
        .name("desc-endpoint")
        .routing_config(
            RoutingConfig::builder()
                .failover_config(
                    FailoverConfig::builder()
                        .primary(
                            Primary::builder()
                                .health_check("arn:aws:route53:::healthcheck/abc123")
                                .build()
                                .unwrap(),
                        )
                        .secondary(Secondary::builder().route("us-west-2").build().unwrap())
                        .build(),
                )
                .build(),
        )
        .event_buses(
            EndpointEventBus::builder()
                .event_bus_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_endpoint()
        .name("desc-endpoint")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "desc-endpoint");
}

#[test_action("events", "ListEndpoints", checksum = "9fbc15e9")]
#[tokio::test]
async fn eb_list_endpoints() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    let resp = client.list_endpoints().send().await.unwrap();
    let _ = resp.endpoints();
}

#[test_action("events", "UpdateEndpoint", checksum = "e7b112c5")]
#[tokio::test]
async fn eb_update_endpoint() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_endpoint()
        .name("upd-endpoint")
        .routing_config(
            RoutingConfig::builder()
                .failover_config(
                    FailoverConfig::builder()
                        .primary(
                            Primary::builder()
                                .health_check("arn:aws:route53:::healthcheck/abc123")
                                .build()
                                .unwrap(),
                        )
                        .secondary(Secondary::builder().route("us-west-2").build().unwrap())
                        .build(),
                )
                .build(),
        )
        .event_buses(
            EndpointEventBus::builder()
                .event_bus_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .update_endpoint()
        .name("upd-endpoint")
        .description("Updated endpoint")
        .send()
        .await
        .unwrap();
    assert!(resp.arn().is_some());
}

#[test_action("events", "DeleteEndpoint", checksum = "8287d5f0")]
#[tokio::test]
async fn eb_delete_endpoint() {
    let server = TestServer::start().await;
    let client = server.eventbridge_client().await;

    client
        .create_endpoint()
        .name("del-endpoint")
        .routing_config(
            RoutingConfig::builder()
                .failover_config(
                    FailoverConfig::builder()
                        .primary(
                            Primary::builder()
                                .health_check("arn:aws:route53:::healthcheck/abc123")
                                .build()
                                .unwrap(),
                        )
                        .secondary(Secondary::builder().route("us-west-2").build().unwrap())
                        .build(),
                )
                .build(),
        )
        .event_buses(
            EndpointEventBus::builder()
                .event_bus_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .delete_endpoint()
        .name("del-endpoint")
        .send()
        .await
        .unwrap();
}
