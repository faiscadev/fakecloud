mod helpers;

use aws_sdk_cloudwatchlogs::types::InputLogEvent;
use helpers::TestServer;

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
