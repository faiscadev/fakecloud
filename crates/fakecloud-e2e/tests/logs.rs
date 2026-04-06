mod helpers;

use aws_sdk_cloudwatchlogs::types::InputLogEvent;
use helpers::TestServer;
use serde_json::Value;

#[tokio::test]
async fn logs_create_describe_delete_log_group() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    // Create
    client
        .create_log_group()
        .log_group_name("/test/app")
        .send()
        .await
        .unwrap();

    // Describe
    let resp = client.describe_log_groups().send().await.unwrap();
    let groups = resp.log_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].log_group_name().unwrap(), "/test/app");

    // Delete
    client
        .delete_log_group()
        .log_group_name("/test/app")
        .send()
        .await
        .unwrap();

    let resp = client.describe_log_groups().send().await.unwrap();
    assert!(resp.log_groups().is_empty());
}

#[tokio::test]
async fn logs_create_duplicate_group_fails() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/dup/group")
        .send()
        .await
        .unwrap();

    let result = client
        .create_log_group()
        .log_group_name("/dup/group")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn logs_delete_nonexistent_group_fails() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    let result = client
        .delete_log_group()
        .log_group_name("/no/such/group")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn logs_create_describe_delete_log_stream() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/stream/test")
        .send()
        .await
        .unwrap();

    // Create stream
    client
        .create_log_stream()
        .log_group_name("/stream/test")
        .log_stream_name("my-stream")
        .send()
        .await
        .unwrap();

    // Describe streams
    let resp = client
        .describe_log_streams()
        .log_group_name("/stream/test")
        .send()
        .await
        .unwrap();
    let streams = resp.log_streams();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].log_stream_name().unwrap(), "my-stream");

    // Delete stream
    client
        .delete_log_stream()
        .log_group_name("/stream/test")
        .log_stream_name("my-stream")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_log_streams()
        .log_group_name("/stream/test")
        .send()
        .await
        .unwrap();
    assert!(resp.log_streams().is_empty());
}

#[tokio::test]
async fn logs_put_and_get_log_events() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/events/test")
        .send()
        .await
        .unwrap();
    client
        .create_log_stream()
        .log_group_name("/events/test")
        .log_stream_name("stream-1")
        .send()
        .await
        .unwrap();

    let now = chrono::Utc::now().timestamp_millis();

    // Put log events
    client
        .put_log_events()
        .log_group_name("/events/test")
        .log_stream_name("stream-1")
        .log_events(
            InputLogEvent::builder()
                .timestamp(now)
                .message("Hello from logs")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 1000)
                .message("Second event")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Get log events
    let resp = client
        .get_log_events()
        .log_group_name("/events/test")
        .log_stream_name("stream-1")
        .start_from_head(true)
        .send()
        .await
        .unwrap();

    let events = resp.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].message().unwrap(), "Hello from logs");
    assert_eq!(events[1].message().unwrap(), "Second event");
}

#[tokio::test]
async fn logs_filter_log_events() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/filter/test")
        .send()
        .await
        .unwrap();
    client
        .create_log_stream()
        .log_group_name("/filter/test")
        .log_stream_name("stream-a")
        .send()
        .await
        .unwrap();

    let now = chrono::Utc::now().timestamp_millis();

    client
        .put_log_events()
        .log_group_name("/filter/test")
        .log_stream_name("stream-a")
        .log_events(
            InputLogEvent::builder()
                .timestamp(now)
                .message("ERROR: something broke")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 1000)
                .message("INFO: all good")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 2000)
                .message("ERROR: another failure")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Filter for ERROR
    let resp = client
        .filter_log_events()
        .log_group_name("/filter/test")
        .filter_pattern("ERROR")
        .send()
        .await
        .unwrap();

    let events = resp.events();
    assert_eq!(events.len(), 2);
    assert!(events[0].message().unwrap().contains("ERROR"));
    assert!(events[1].message().unwrap().contains("ERROR"));
}

#[tokio::test]
async fn logs_filter_log_events_applies_pattern() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/filter-pattern/e2e")
        .send()
        .await
        .unwrap();
    client
        .create_log_stream()
        .log_group_name("/filter-pattern/e2e")
        .log_stream_name("stream-1")
        .send()
        .await
        .unwrap();

    let now = chrono::Utc::now().timestamp_millis();

    client
        .put_log_events()
        .log_group_name("/filter-pattern/e2e")
        .log_stream_name("stream-1")
        .log_events(
            InputLogEvent::builder()
                .timestamp(now)
                .message("ERROR: disk full")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 1000)
                .message("INFO: request complete")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 2000)
                .message("ERROR: connection timeout")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 3000)
                .message(r#"{"level":"ERROR","msg":"json error"}"#)
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 4000)
                .message(r#"{"level":"INFO","msg":"json info"}"#)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Test 1: Simple text pattern
    let resp = client
        .filter_log_events()
        .log_group_name("/filter-pattern/e2e")
        .filter_pattern("ERROR")
        .send()
        .await
        .unwrap();
    // 2 plain-text ERROR + 1 JSON ERROR in message text
    assert_eq!(
        resp.events().len(),
        3,
        "simple text 'ERROR' should match 3 events"
    );

    // Test 2: Multiple terms (AND)
    let resp = client
        .filter_log_events()
        .log_group_name("/filter-pattern/e2e")
        .filter_pattern("ERROR timeout")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.events().len(), 1, "AND terms should match 1 event");
    assert!(resp.events()[0].message().unwrap().contains("timeout"));

    // Test 3: Quoted exact phrase
    let resp = client
        .filter_log_events()
        .log_group_name("/filter-pattern/e2e")
        .filter_pattern("\"request complete\"")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.events().len(), 1, "quoted phrase should match 1 event");

    // Test 4: JSON field pattern
    let resp = client
        .filter_log_events()
        .log_group_name("/filter-pattern/e2e")
        .filter_pattern("{ $.level = \"ERROR\" }")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.events().len(),
        1,
        "JSON pattern should match only JSON events with level=ERROR"
    );
    assert!(resp.events()[0].message().unwrap().contains("json error"));
}

#[tokio::test]
async fn logs_retention_policy() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/retention/test")
        .send()
        .await
        .unwrap();

    // Put retention
    client
        .put_retention_policy()
        .log_group_name("/retention/test")
        .retention_in_days(30)
        .send()
        .await
        .unwrap();

    // Verify via describe
    let resp = client
        .describe_log_groups()
        .log_group_name_prefix("/retention/test")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.log_groups()[0].retention_in_days(), Some(30));

    // Delete retention
    client
        .delete_retention_policy()
        .log_group_name("/retention/test")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_log_groups()
        .log_group_name_prefix("/retention/test")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.log_groups()[0].retention_in_days(), None);
}

#[tokio::test]
#[allow(deprecated)]
async fn logs_tag_untag_log_group() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/tag/test")
        .send()
        .await
        .unwrap();

    // Tag
    client
        .tag_log_group()
        .log_group_name("/tag/test")
        .tags("env", "prod")
        .tags("team", "platform")
        .send()
        .await
        .unwrap();

    // List tags
    let resp = client
        .list_tags_log_group()
        .log_group_name("/tag/test")
        .send()
        .await
        .unwrap();
    let tags = resp.tags().unwrap();
    assert_eq!(tags.get("env").unwrap(), "prod");
    assert_eq!(tags.get("team").unwrap(), "platform");

    // Untag
    client
        .untag_log_group()
        .log_group_name("/tag/test")
        .tags("team")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_tags_log_group()
        .log_group_name("/tag/test")
        .send()
        .await
        .unwrap();
    let tags = resp.tags().unwrap();
    assert_eq!(tags.len(), 1);
    assert!(tags.get("team").is_none());
}

#[tokio::test]
async fn logs_describe_groups_with_prefix() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/app/web")
        .send()
        .await
        .unwrap();
    client
        .create_log_group()
        .log_group_name("/app/api")
        .send()
        .await
        .unwrap();
    client
        .create_log_group()
        .log_group_name("/other/service")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_log_groups()
        .log_group_name_prefix("/app/")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.log_groups().len(), 2);
}

#[tokio::test]
async fn logs_describe_log_groups_validates_limit() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    let result = client.describe_log_groups().limit(0).send().await;
    assert!(result.is_err(), "limit=0 should be rejected");
}

#[tokio::test]
async fn logs_put_metric_filter_requires_filter_pattern() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    // Create the log group first
    client
        .create_log_group()
        .log_group_name("/test/metric-filter")
        .send()
        .await
        .unwrap();

    // Omit filterPattern via CLI (the SDK may enforce required fields locally)
    let result = server
        .aws_cli(&[
            "logs",
            "put-metric-filter",
            "--log-group-name",
            "/test/metric-filter",
            "--filter-name",
            "test-filter",
            "--metric-transformations",
            "metricName=TestMetric,metricNamespace=TestNS,metricValue=1",
        ])
        .await;
    // The CLI should fail because filterPattern is required
    assert!(!result.success(), "omitting filterPattern should fail");
}

#[tokio::test]
async fn logs_put_events_to_nonexistent_stream_fails() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/err/test")
        .send()
        .await
        .unwrap();

    let result = client
        .put_log_events()
        .log_group_name("/err/test")
        .log_stream_name("nonexistent")
        .log_events(
            InputLogEvent::builder()
                .timestamp(1000)
                .message("test")
                .build()
                .unwrap(),
        )
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn logs_account_policy_lifecycle() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    // Put account policy
    client
        .put_account_policy()
        .policy_name("test-acct-policy")
        .policy_type(aws_sdk_cloudwatchlogs::types::PolicyType::DataProtectionPolicy)
        .policy_document("{\"Name\":\"test\"}")
        .send()
        .await
        .unwrap();

    // Describe
    let resp = client
        .describe_account_policies()
        .policy_type(aws_sdk_cloudwatchlogs::types::PolicyType::DataProtectionPolicy)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.account_policies().len(), 1);
    assert_eq!(
        resp.account_policies()[0].policy_name().unwrap(),
        "test-acct-policy"
    );

    // Delete
    client
        .delete_account_policy()
        .policy_name("test-acct-policy")
        .policy_type(aws_sdk_cloudwatchlogs::types::PolicyType::DataProtectionPolicy)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_account_policies()
        .policy_type(aws_sdk_cloudwatchlogs::types::PolicyType::DataProtectionPolicy)
        .send()
        .await
        .unwrap();
    assert!(resp.account_policies().is_empty());
}

#[tokio::test]
async fn logs_data_protection_lifecycle() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/dp/test")
        .send()
        .await
        .unwrap();

    // Put data protection policy
    client
        .put_data_protection_policy()
        .log_group_identifier("/dp/test")
        .policy_document("{\"Name\":\"dp\"}")
        .send()
        .await
        .unwrap();

    // Get
    let resp = client
        .get_data_protection_policy()
        .log_group_identifier("/dp/test")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.policy_document().unwrap(), "{\"Name\":\"dp\"}");

    // Delete
    client
        .delete_data_protection_policy()
        .log_group_identifier("/dp/test")
        .send()
        .await
        .unwrap();

    // Verify gone
    let resp = client
        .get_data_protection_policy()
        .log_group_identifier("/dp/test")
        .send()
        .await
        .unwrap();
    assert!(resp.policy_document().is_none());
}

#[tokio::test]
async fn logs_transformer_lifecycle() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/tx/test")
        .send()
        .await
        .unwrap();

    // Put transformer
    client
        .put_transformer()
        .log_group_identifier("/tx/test")
        .transformer_config(
            aws_sdk_cloudwatchlogs::types::Processor::builder()
                .add_keys(
                    aws_sdk_cloudwatchlogs::types::AddKeys::builder()
                        .entries(
                            aws_sdk_cloudwatchlogs::types::AddKeyEntry::builder()
                                .key("testKey")
                                .value("testValue")
                                .build()
                                .unwrap(),
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Get transformer
    let resp = client
        .get_transformer()
        .log_group_identifier("/tx/test")
        .send()
        .await
        .unwrap();
    assert!(!resp.transformer_config().is_empty());

    // Delete transformer
    client
        .delete_transformer()
        .log_group_identifier("/tx/test")
        .send()
        .await
        .unwrap();

    // Verify gone
    let resp = client
        .get_transformer()
        .log_group_identifier("/tx/test")
        .send()
        .await
        .unwrap();
    assert!(resp.transformer_config().is_empty());
}

#[tokio::test]
async fn logs_anomaly_detector_lifecycle() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/anomaly/test")
        .send()
        .await
        .unwrap();

    // Get log group ARN
    let groups = client
        .describe_log_groups()
        .log_group_name_prefix("/anomaly/test")
        .send()
        .await
        .unwrap();
    let group_arn = groups.log_groups()[0].arn().unwrap().to_string();

    // Create anomaly detector
    let resp = client
        .create_log_anomaly_detector()
        .log_group_arn_list(&group_arn)
        .detector_name("test-detector")
        .send()
        .await
        .unwrap();
    let detector_arn = resp.anomaly_detector_arn().unwrap().to_string();

    // Get anomaly detector
    let resp = client
        .get_log_anomaly_detector()
        .anomaly_detector_arn(&detector_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.detector_name().unwrap(), "test-detector");

    // List anomaly detectors
    let resp = client.list_log_anomaly_detectors().send().await.unwrap();
    assert_eq!(resp.anomaly_detectors().len(), 1);

    // Update
    client
        .update_log_anomaly_detector()
        .anomaly_detector_arn(&detector_arn)
        .enabled(false)
        .send()
        .await
        .unwrap();

    // Delete
    client
        .delete_log_anomaly_detector()
        .anomaly_detector_arn(&detector_arn)
        .send()
        .await
        .unwrap();

    let resp = client.list_log_anomaly_detectors().send().await.unwrap();
    assert!(resp.anomaly_detectors().is_empty());
}

#[tokio::test]
async fn logs_import_task_lifecycle() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    // Create import task
    let create = client
        .create_import_task()
        .import_source_arn("arn:aws:s3:::test-import-bucket/logs")
        .import_role_arn("arn:aws:iam::123456789012:role/import-role")
        .send()
        .await
        .unwrap();
    let import_id = create.import_id().unwrap().to_string();

    // Describe import tasks
    let resp = client.describe_import_tasks().send().await.unwrap();
    assert_eq!(resp.imports().len(), 1);

    // Describe import task batches
    let resp = client
        .describe_import_task_batches()
        .import_id(&import_id)
        .send()
        .await
        .unwrap();
    assert!(resp.import_batches().is_empty());

    // Cancel import task
    client
        .cancel_import_task()
        .import_id(&import_id)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn logs_integration_lifecycle() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .put_integration()
        .integration_name("test-integration")
        .integration_type(aws_sdk_cloudwatchlogs::types::IntegrationType::Opensearch)
        .resource_config(
            aws_sdk_cloudwatchlogs::types::ResourceConfig::OpenSearchResourceConfig(
                aws_sdk_cloudwatchlogs::types::OpenSearchResourceConfig::builder()
                    .data_source_role_arn("arn:aws:iam::123456789012:role/data-source-role")
                    .dashboard_viewer_principals("arn:aws:iam::123456789012:user/viewer")
                    .retention_days(30)
                    .build()
                    .unwrap(),
            ),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .get_integration()
        .integration_name("test-integration")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.integration_name().unwrap(), "test-integration");

    let resp = client.list_integrations().send().await.unwrap();
    assert_eq!(resp.integration_summaries().len(), 1);

    client
        .delete_integration()
        .integration_name("test-integration")
        .send()
        .await
        .unwrap();

    let resp = client.list_integrations().send().await.unwrap();
    assert!(resp.integration_summaries().is_empty());
}

#[tokio::test]
async fn logs_lookup_table_lifecycle() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    let create = client
        .create_lookup_table()
        .lookup_table_name("test-lt")
        .table_body("key,value\na,b")
        .send()
        .await
        .unwrap();
    let arn = create.lookup_table_arn().unwrap().to_string();

    let resp = client
        .get_lookup_table()
        .lookup_table_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.lookup_table_name().unwrap(), "test-lt");

    client
        .update_lookup_table()
        .lookup_table_arn(&arn)
        .table_body("key,value\nc,d")
        .send()
        .await
        .unwrap();

    client
        .delete_lookup_table()
        .lookup_table_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn logs_scheduled_query_lifecycle() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    let create = client
        .create_scheduled_query()
        .name("test-sq")
        .query_language(aws_sdk_cloudwatchlogs::types::QueryLanguage::Cwli)
        .query_string("fields @timestamp | limit 10")
        .schedule_expression("rate(1 hour)")
        .execution_role_arn("arn:aws:iam::123456789012:role/exec")
        .send()
        .await
        .unwrap();
    let arn = create.scheduled_query_arn().unwrap().to_string();

    let resp = client
        .get_scheduled_query()
        .identifier(&arn)
        .send()
        .await
        .unwrap();
    assert!(resp.name().is_some());

    let resp = client.list_scheduled_queries().send().await.unwrap();
    assert_eq!(resp.scheduled_queries().len(), 1);

    client
        .update_scheduled_query()
        .identifier(&arn)
        .query_language(aws_sdk_cloudwatchlogs::types::QueryLanguage::Cwli)
        .query_string("fields @message | limit 5")
        .schedule_expression("rate(2 hours)")
        .execution_role_arn("arn:aws:iam::123456789012:role/exec")
        .send()
        .await
        .unwrap();

    client
        .delete_scheduled_query()
        .identifier(&arn)
        .send()
        .await
        .unwrap();

    let resp = client.list_scheduled_queries().send().await.unwrap();
    assert!(resp.scheduled_queries().is_empty());
}

#[tokio::test]
async fn logs_misc_stubs() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    // ListLogGroups (alias for DescribeLogGroups)
    client
        .create_log_group()
        .log_group_name("/misc/listgroups")
        .send()
        .await
        .unwrap();
    let resp = client.list_log_groups().send().await.unwrap();
    assert!(!resp.log_groups().is_empty());

    // ListLogGroupsForQuery
    let resp = client
        .list_log_groups_for_query()
        .query_id("dummy-query")
        .send()
        .await
        .unwrap();
    assert!(resp.log_group_identifiers().is_empty());

    // DescribeConfigurationTemplates
    let resp = client
        .describe_configuration_templates()
        .send()
        .await
        .unwrap();
    assert!(resp.configuration_templates().is_empty());

    // PutBearerTokenAuthentication
    client
        .put_bearer_token_authentication()
        .log_group_identifier("/misc/listgroups")
        .bearer_token_authentication_enabled(true)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn logs_query_filters_events() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/query/e2e")
        .send()
        .await
        .unwrap();
    client
        .create_log_stream()
        .log_group_name("/query/e2e")
        .log_stream_name("stream-1")
        .send()
        .await
        .unwrap();

    let now = chrono::Utc::now().timestamp_millis();
    client
        .put_log_events()
        .log_group_name("/query/e2e")
        .log_stream_name("stream-1")
        .log_events(
            InputLogEvent::builder()
                .timestamp(now)
                .message("ERROR: disk full")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 1000)
                .message("INFO: request complete")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 2000)
                .message("ERROR: connection timeout")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Start a query with filter
    let start_secs = (now / 1000) - 1;
    let end_secs = (now / 1000) + 10;
    let resp = client
        .start_query()
        .log_group_name("/query/e2e")
        .start_time(start_secs)
        .end_time(end_secs)
        .query_string("filter @message like /ERROR/ | limit 10")
        .send()
        .await
        .unwrap();
    let query_id = resp.query_id().unwrap().to_string();

    // Get results
    let resp = client
        .get_query_results()
        .query_id(&query_id)
        .send()
        .await
        .unwrap();

    let results = resp.results();
    assert_eq!(results.len(), 2, "Should return only ERROR events");

    // Verify all returned events contain ERROR
    for row in results {
        let msg: &str = row
            .iter()
            .find(|f| f.field() == Some("@message"))
            .and_then(|f| f.value())
            .unwrap();
        assert!(msg.contains("ERROR"), "Expected ERROR in message: {msg}");
    }
}

#[tokio::test]
async fn logs_query_sort_and_limit() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/query-sort/e2e")
        .send()
        .await
        .unwrap();
    client
        .create_log_stream()
        .log_group_name("/query-sort/e2e")
        .log_stream_name("stream-1")
        .send()
        .await
        .unwrap();

    let now = chrono::Utc::now().timestamp_millis();
    client
        .put_log_events()
        .log_group_name("/query-sort/e2e")
        .log_stream_name("stream-1")
        .log_events(
            InputLogEvent::builder()
                .timestamp(now)
                .message("first")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 1000)
                .message("second")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 2000)
                .message("third")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let start_secs = (now / 1000) - 1;
    let end_secs = (now / 1000) + 10;
    let resp = client
        .start_query()
        .log_group_name("/query-sort/e2e")
        .start_time(start_secs)
        .end_time(end_secs)
        .query_string("sort @timestamp desc | limit 2")
        .send()
        .await
        .unwrap();
    let query_id = resp.query_id().unwrap().to_string();

    let resp = client
        .get_query_results()
        .query_id(&query_id)
        .send()
        .await
        .unwrap();

    let results = resp.results();
    assert_eq!(results.len(), 2, "Should be limited to 2 results");

    // First result should be "third" (desc sort)
    let first_msg: &str = results[0]
        .iter()
        .find(|f| f.field() == Some("@message"))
        .and_then(|f| f.value())
        .unwrap();
    assert_eq!(first_msg, "third");
}

#[tokio::test]
async fn logs_export_task_writes_to_storage() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/export/e2e")
        .send()
        .await
        .unwrap();
    client
        .create_log_stream()
        .log_group_name("/export/e2e")
        .log_stream_name("stream-1")
        .send()
        .await
        .unwrap();

    let now = chrono::Utc::now().timestamp_millis();
    client
        .put_log_events()
        .log_group_name("/export/e2e")
        .log_stream_name("stream-1")
        .log_events(
            InputLogEvent::builder()
                .timestamp(now)
                .message("export event A")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 1000)
                .message("export event B")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Create export task
    let resp = client
        .create_export_task()
        .log_group_name("/export/e2e")
        .from(now - 1000)
        .to(now + 10000)
        .destination("e2e-export-bucket")
        .destination_prefix("logs")
        .send()
        .await
        .unwrap();
    let task_id = resp.task_id().unwrap().to_string();

    // Verify task completed
    let resp = client
        .describe_export_tasks()
        .task_id(&task_id)
        .send()
        .await
        .unwrap();
    let task = &resp.export_tasks()[0];
    assert_eq!(task.status().unwrap().code().unwrap().as_str(), "COMPLETED");

    // Verify exported data via internal GetExportedData action (raw HTTP)
    let http_client = reqwest::Client::new();
    let resp = http_client
        .post(server.endpoint())
        .header("Content-Type", "application/x-amz-json-1.1")
        .header("X-Amz-Target", "Logs_20140328.GetExportedData")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/logs/aws4_request, SignedHeaders=host, Signature=dummy",
        )
        .body(r#"{"keyPrefix": "e2e-export-bucket/logs"}"#)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let entries = body["entries"].as_array().unwrap();
    assert!(!entries.is_empty(), "Should have exported data");
    let data = entries[0]["data"].as_str().unwrap();
    assert!(data.contains("export event A"));
    assert!(data.contains("export event B"));
}

#[tokio::test]
async fn logs_delivery_pipeline_forwards_events() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    // Create log group and stream
    client
        .create_log_group()
        .log_group_name("/delivery/e2e")
        .send()
        .await
        .unwrap();
    client
        .create_log_stream()
        .log_group_name("/delivery/e2e")
        .log_stream_name("stream-1")
        .send()
        .await
        .unwrap();

    // Get log group ARN
    let groups = client
        .describe_log_groups()
        .log_group_name_prefix("/delivery/e2e")
        .send()
        .await
        .unwrap();
    let group_arn = groups.log_groups()[0].arn().unwrap().to_string();

    // Set up delivery source
    client
        .put_delivery_source()
        .name("e2e-source")
        .resource_arn(&group_arn)
        .log_type("APPLICATION_LOGS")
        .send()
        .await
        .unwrap();

    // Set up delivery destination (S3 bucket)
    let dest_resp = client
        .put_delivery_destination()
        .name("e2e-dest")
        .delivery_destination_configuration(
            aws_sdk_cloudwatchlogs::types::DeliveryDestinationConfiguration::builder()
                .destination_resource_arn("arn:aws:s3:::e2e-delivery-bucket")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let dest_arn = dest_resp
        .delivery_destination()
        .unwrap()
        .arn()
        .unwrap()
        .to_string();

    // Create delivery
    client
        .create_delivery()
        .delivery_source_name("e2e-source")
        .delivery_destination_arn(&dest_arn)
        .send()
        .await
        .unwrap();

    // Put log events — should be forwarded via delivery pipeline
    let now = chrono::Utc::now().timestamp_millis();
    client
        .put_log_events()
        .log_group_name("/delivery/e2e")
        .log_stream_name("stream-1")
        .log_events(
            InputLogEvent::builder()
                .timestamp(now)
                .message("delivered msg 1")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 1000)
                .message("delivered msg 2")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Verify delivery data via internal API
    let http_client = reqwest::Client::new();
    let resp = http_client
        .post(server.endpoint())
        .header("Content-Type", "application/x-amz-json-1.1")
        .header("X-Amz-Target", "Logs_20140328.GetExportedData")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/logs/aws4_request, SignedHeaders=host, Signature=dummy",
        )
        .body(r#"{"keyPrefix": "e2e-delivery-bucket/delivery"}"#)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let entries = body["entries"].as_array().unwrap();
    assert!(!entries.is_empty(), "Should have delivery data");
    let data = entries[0]["data"].as_str().unwrap();
    assert!(data.contains("delivered msg 1"));
    assert!(data.contains("delivered msg 2"));
}
