mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// -- Log group lifecycle --

#[test_action("logs", "CreateLogGroup", checksum = "76f16905")]
#[test_action("logs", "DescribeLogGroups", checksum = "c88d7e2d")]
#[test_action("logs", "DeleteLogGroup", checksum = "44642caf")]
#[tokio::test]
async fn logs_log_group_lifecycle() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/group1")
        .send()
        .await
        .unwrap();

    let resp = client.describe_log_groups().send().await.unwrap();
    assert!(!resp.log_groups().is_empty());

    client
        .delete_log_group()
        .log_group_name("/conf/group1")
        .send()
        .await
        .unwrap();
}

// -- Log stream lifecycle --

#[test_action("logs", "CreateLogStream", checksum = "8cd142c6")]
#[test_action("logs", "DescribeLogStreams", checksum = "a25ff658")]
#[test_action("logs", "DeleteLogStream", checksum = "3955d87d")]
#[tokio::test]
async fn logs_log_stream_lifecycle() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/streams")
        .send()
        .await
        .unwrap();

    client
        .create_log_stream()
        .log_group_name("/conf/streams")
        .log_stream_name("stream1")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_log_streams()
        .log_group_name("/conf/streams")
        .send()
        .await
        .unwrap();
    assert!(!resp.log_streams().is_empty());

    client
        .delete_log_stream()
        .log_group_name("/conf/streams")
        .log_stream_name("stream1")
        .send()
        .await
        .unwrap();
}

// -- PutLogEvents + GetLogEvents + FilterLogEvents --

#[test_action("logs", "PutLogEvents", checksum = "73acad9c")]
#[test_action("logs", "GetLogEvents", checksum = "a8cec2f4")]
#[test_action("logs", "FilterLogEvents", checksum = "3316f933")]
#[tokio::test]
async fn logs_put_get_filter_events() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/events")
        .send()
        .await
        .unwrap();
    client
        .create_log_stream()
        .log_group_name("/conf/events")
        .log_stream_name("s1")
        .send()
        .await
        .unwrap();

    client
        .put_log_events()
        .log_group_name("/conf/events")
        .log_stream_name("s1")
        .log_events(
            aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                .timestamp(1700000000000)
                .message("hello conformance")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .get_log_events()
        .log_group_name("/conf/events")
        .log_stream_name("s1")
        .send()
        .await
        .unwrap();
    assert!(!resp.events().is_empty());

    let filter = client
        .filter_log_events()
        .log_group_name("/conf/events")
        .send()
        .await
        .unwrap();
    assert!(!filter.events().is_empty());
}

// -- Tag/Untag log group (legacy) --

#[test_action("logs", "TagLogGroup", checksum = "a1eb0891")]
#[test_action("logs", "UntagLogGroup", checksum = "34dfdbcb")]
#[test_action("logs", "ListTagsLogGroup", checksum = "5d78a6cc")]
#[tokio::test]
async fn logs_tag_untag_log_group_legacy() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/taglegacy")
        .send()
        .await
        .unwrap();

    #[allow(deprecated)]
    client
        .tag_log_group()
        .log_group_name("/conf/taglegacy")
        .tags("env", "test")
        .send()
        .await
        .unwrap();

    #[allow(deprecated)]
    let resp = client
        .list_tags_log_group()
        .log_group_name("/conf/taglegacy")
        .send()
        .await
        .unwrap();
    assert!(resp.tags().is_some());

    #[allow(deprecated)]
    client
        .untag_log_group()
        .log_group_name("/conf/taglegacy")
        .tags("env")
        .send()
        .await
        .unwrap();
}

// -- Tag/Untag resource (new API) --

#[test_action("logs", "TagResource", checksum = "14d91a68")]
#[test_action("logs", "UntagResource", checksum = "8875847f")]
#[test_action("logs", "ListTagsForResource", checksum = "8aa1b0cf")]
#[tokio::test]
async fn logs_tag_untag_resource() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/tagres")
        .send()
        .await
        .unwrap();

    // Get the ARN from describe
    let groups = client
        .describe_log_groups()
        .log_group_name_prefix("/conf/tagres")
        .send()
        .await
        .unwrap();
    let arn = groups.log_groups()[0].arn().unwrap().to_string();

    client
        .tag_resource()
        .resource_arn(&arn)
        .tags("project", "conformance")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(resp.tags().is_some());

    client
        .untag_resource()
        .resource_arn(&arn)
        .tag_keys("project")
        .send()
        .await
        .unwrap();
}

// -- Retention policy --

#[test_action("logs", "PutRetentionPolicy", checksum = "12c2dc65")]
#[test_action("logs", "DeleteRetentionPolicy", checksum = "64b06c60")]
#[tokio::test]
async fn logs_retention_policy() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/retention")
        .send()
        .await
        .unwrap();

    client
        .put_retention_policy()
        .log_group_name("/conf/retention")
        .retention_in_days(7)
        .send()
        .await
        .unwrap();

    client
        .delete_retention_policy()
        .log_group_name("/conf/retention")
        .send()
        .await
        .unwrap();
}

// -- Subscription filters --

#[test_action("logs", "PutSubscriptionFilter", checksum = "9a4c1504")]
#[test_action("logs", "DescribeSubscriptionFilters", checksum = "e21c42f4")]
#[test_action("logs", "DeleteSubscriptionFilter", checksum = "22aad43a")]
#[tokio::test]
async fn logs_subscription_filters() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/subfilt")
        .send()
        .await
        .unwrap();

    client
        .put_subscription_filter()
        .log_group_name("/conf/subfilt")
        .filter_name("conf-filter")
        .filter_pattern("")
        .destination_arn("arn:aws:lambda:us-east-1:123456789012:function:dummy")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_subscription_filters()
        .log_group_name("/conf/subfilt")
        .send()
        .await
        .unwrap();
    assert!(!resp.subscription_filters().is_empty());

    client
        .delete_subscription_filter()
        .log_group_name("/conf/subfilt")
        .filter_name("conf-filter")
        .send()
        .await
        .unwrap();
}

// -- Metric filters --

#[test_action("logs", "PutMetricFilter", checksum = "947ca578")]
#[test_action("logs", "DescribeMetricFilters", checksum = "42368c89")]
#[test_action("logs", "DeleteMetricFilter", checksum = "a589d7d0")]
#[tokio::test]
async fn logs_metric_filters() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/metric")
        .send()
        .await
        .unwrap();

    client
        .put_metric_filter()
        .log_group_name("/conf/metric")
        .filter_name("conf-metric")
        .filter_pattern("[ip, user, ...]")
        .metric_transformations(
            aws_sdk_cloudwatchlogs::types::MetricTransformation::builder()
                .metric_name("TestMetric")
                .metric_namespace("Conformance")
                .metric_value("1")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_metric_filters()
        .log_group_name("/conf/metric")
        .send()
        .await
        .unwrap();
    assert!(!resp.metric_filters().is_empty());

    client
        .delete_metric_filter()
        .log_group_name("/conf/metric")
        .filter_name("conf-metric")
        .send()
        .await
        .unwrap();
}

// -- Resource policies --

#[test_action("logs", "PutResourcePolicy", checksum = "37d29085")]
#[test_action("logs", "DescribeResourcePolicies", checksum = "0fb0c571")]
#[test_action("logs", "DeleteResourcePolicy", checksum = "791340ba")]
#[tokio::test]
async fn logs_resource_policies() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"es.amazonaws.com"},"Action":["logs:PutLogEvents","logs:CreateLogStream"],"Resource":"*"}]}"#;

    client
        .put_resource_policy()
        .policy_name("conf-policy")
        .policy_document(policy_doc)
        .send()
        .await
        .unwrap();

    let resp = client.describe_resource_policies().send().await.unwrap();
    assert!(!resp.resource_policies().is_empty());

    client
        .delete_resource_policy()
        .policy_name("conf-policy")
        .send()
        .await
        .unwrap();
}

// -- Destinations --

#[test_action("logs", "PutDestination", checksum = "12a2c10d")]
#[test_action("logs", "DescribeDestinations", checksum = "d793380f")]
#[test_action("logs", "PutDestinationPolicy", checksum = "d1d9652d")]
#[test_action("logs", "DeleteDestination", checksum = "ca130692")]
#[tokio::test]
async fn logs_destinations() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .put_destination()
        .destination_name("conf-dest")
        .target_arn("arn:aws:kinesis:us-east-1:123456789012:stream/dummy")
        .role_arn("arn:aws:iam::123456789012:role/dummy")
        .send()
        .await
        .unwrap();

    let resp = client.describe_destinations().send().await.unwrap();
    assert!(!resp.destinations().is_empty());

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"logs:PutSubscriptionFilter","Resource":"*"}]}"#;
    client
        .put_destination_policy()
        .destination_name("conf-dest")
        .access_policy(policy)
        .send()
        .await
        .unwrap();

    client
        .delete_destination()
        .destination_name("conf-dest")
        .send()
        .await
        .unwrap();
}

// -- Queries --

#[test_action("logs", "StartQuery", checksum = "a61f0343")]
#[test_action("logs", "GetQueryResults", checksum = "0312b275")]
#[test_action("logs", "DescribeQueries", checksum = "fb7f2a3c")]
#[tokio::test]
async fn logs_queries() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/queries")
        .send()
        .await
        .unwrap();

    let start = client
        .start_query()
        .log_group_name("/conf/queries")
        .start_time(0)
        .end_time(9999999999)
        .query_string("fields @timestamp, @message")
        .send()
        .await
        .unwrap();
    let query_id = start.query_id().unwrap().to_string();

    client
        .get_query_results()
        .query_id(&query_id)
        .send()
        .await
        .unwrap();

    client.describe_queries().send().await.unwrap();
}

// -- Export tasks --

#[test_action("logs", "CreateExportTask", checksum = "f339e521")]
#[test_action("logs", "DescribeExportTasks", checksum = "a4564b5c")]
#[test_action("logs", "CancelExportTask", checksum = "042d9396")]
#[tokio::test]
async fn logs_export_tasks() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/export")
        .send()
        .await
        .unwrap();

    let create = client
        .create_export_task()
        .log_group_name("/conf/export")
        .from(0)
        .to(9999999999)
        .destination("conf-export-bucket")
        .send()
        .await
        .unwrap();
    let task_id = create.task_id().unwrap().to_string();

    client.describe_export_tasks().send().await.unwrap();

    let _ = client.cancel_export_task().task_id(&task_id).send().await;
}

// -- Delivery destinations --

#[test_action("logs", "PutDeliveryDestination", checksum = "5b7e444e")]
#[test_action("logs", "GetDeliveryDestination", checksum = "bcd70ca0")]
#[test_action("logs", "DescribeDeliveryDestinations", checksum = "04c820de")]
#[test_action("logs", "DeleteDeliveryDestination", checksum = "260fbf3b")]
#[tokio::test]
async fn logs_delivery_destinations() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .put_delivery_destination()
        .name("conf-dd")
        .delivery_destination_configuration(
            aws_sdk_cloudwatchlogs::types::DeliveryDestinationConfiguration::builder()
                .destination_resource_arn("arn:aws:s3:::conf-bucket")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .get_delivery_destination()
        .name("conf-dd")
        .send()
        .await
        .unwrap();

    client
        .describe_delivery_destinations()
        .send()
        .await
        .unwrap();

    client
        .delete_delivery_destination()
        .name("conf-dd")
        .send()
        .await
        .unwrap();
}

// -- Delivery destination policy --

#[test_action("logs", "PutDeliveryDestinationPolicy", checksum = "32b99512")]
#[test_action("logs", "GetDeliveryDestinationPolicy", checksum = "7488a90e")]
#[test_action("logs", "DeleteDeliveryDestinationPolicy", checksum = "23cb82ab")]
#[tokio::test]
async fn logs_delivery_destination_policy() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .put_delivery_destination()
        .name("conf-dd-pol")
        .delivery_destination_configuration(
            aws_sdk_cloudwatchlogs::types::DeliveryDestinationConfiguration::builder()
                .destination_resource_arn("arn:aws:s3:::conf-bucket-pol")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"logs:CreateDelivery","Resource":"*"}]}"#;
    client
        .put_delivery_destination_policy()
        .delivery_destination_name("conf-dd-pol")
        .delivery_destination_policy(policy)
        .send()
        .await
        .unwrap();

    let _ = client
        .get_delivery_destination_policy()
        .delivery_destination_name("conf-dd-pol")
        .send()
        .await;

    let _ = client
        .delete_delivery_destination_policy()
        .delivery_destination_name("conf-dd-pol")
        .send()
        .await;
}

// -- Delivery sources --

#[test_action("logs", "PutDeliverySource", checksum = "9e3b97b5")]
#[test_action("logs", "GetDeliverySource", checksum = "a9ee52ac")]
#[test_action("logs", "DescribeDeliverySources", checksum = "0c5b2fc9")]
#[test_action("logs", "DeleteDeliverySource", checksum = "69db1c4e")]
#[tokio::test]
async fn logs_delivery_sources() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .put_delivery_source()
        .name("conf-ds")
        .resource_arn("arn:aws:logs:us-east-1:123456789012:log-group:dummy")
        .log_type("APPLICATION_LOGS")
        .send()
        .await
        .unwrap();

    client
        .get_delivery_source()
        .name("conf-ds")
        .send()
        .await
        .unwrap();

    client.describe_delivery_sources().send().await.unwrap();

    client
        .delete_delivery_source()
        .name("conf-ds")
        .send()
        .await
        .unwrap();
}

// -- Deliveries --

#[test_action("logs", "CreateDelivery", checksum = "b4ff169a")]
#[test_action("logs", "GetDelivery", checksum = "7a1b7136")]
#[test_action("logs", "DescribeDeliveries", checksum = "e5773338")]
#[test_action("logs", "DeleteDelivery", checksum = "3ffa7911")]
#[tokio::test]
async fn logs_deliveries() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    // Set up source and destination first
    client
        .put_delivery_source()
        .name("conf-ds-dlv")
        .resource_arn("arn:aws:logs:us-east-1:123456789012:log-group:dummy")
        .log_type("APPLICATION_LOGS")
        .send()
        .await
        .unwrap();

    client
        .put_delivery_destination()
        .name("conf-dd-dlv")
        .delivery_destination_configuration(
            aws_sdk_cloudwatchlogs::types::DeliveryDestinationConfiguration::builder()
                .destination_resource_arn("arn:aws:s3:::conf-bucket-dlv")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let create = client
        .create_delivery()
        .delivery_source_name("conf-ds-dlv")
        .delivery_destination_arn(
            "arn:aws:logs:us-east-1:123456789012:delivery-destination:conf-dd-dlv",
        )
        .send()
        .await
        .unwrap();
    let delivery_id = create.delivery().unwrap().id().unwrap().to_string();

    client.get_delivery().id(&delivery_id).send().await.unwrap();

    client.describe_deliveries().send().await.unwrap();

    client
        .delete_delivery()
        .id(&delivery_id)
        .send()
        .await
        .unwrap();
}

// -- KMS association --

#[test_action("logs", "AssociateKmsKey", checksum = "745c2912")]
#[test_action("logs", "DisassociateKmsKey", checksum = "20bbd380")]
#[tokio::test]
async fn logs_kms_association() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    client
        .create_log_group()
        .log_group_name("/conf/kms")
        .send()
        .await
        .unwrap();

    client
        .associate_kms_key()
        .log_group_name("/conf/kms")
        .kms_key_id("arn:aws:kms:us-east-1:123456789012:key/dummy-key-id")
        .send()
        .await
        .unwrap();

    client
        .disassociate_kms_key()
        .log_group_name("/conf/kms")
        .send()
        .await
        .unwrap();
}

// -- Query definitions --

#[test_action("logs", "PutQueryDefinition", checksum = "007ecea3")]
#[test_action("logs", "DescribeQueryDefinitions", checksum = "ec21fa12")]
#[test_action("logs", "DeleteQueryDefinition", checksum = "89913a2a")]
#[tokio::test]
async fn logs_query_definitions() {
    let server = TestServer::start().await;
    let client = server.logs_client().await;

    let put = client
        .put_query_definition()
        .name("conf-qd")
        .query_string("fields @timestamp, @message | limit 20")
        .send()
        .await
        .unwrap();
    let qd_id = put.query_definition_id().unwrap().to_string();

    client.describe_query_definitions().send().await.unwrap();

    client
        .delete_query_definition()
        .query_definition_id(&qd_id)
        .send()
        .await
        .unwrap();
}
