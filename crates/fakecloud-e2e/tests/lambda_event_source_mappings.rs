//! E2E coverage for Lambda event-source-mapping SourceAccessConfigurations
//! (bug-audit 2026-05-28, 1.17): CreateEventSourceMapping must persist the
//! caller's SourceAccessConfigurations and echo them on Get, not always [].

mod helpers;

use aws_sdk_lambda::types::{SourceAccessConfiguration, SourceAccessType};
use helpers::TestServer;

const MINIMAL_HANDLER: &str = "index.handler";

async fn create_function(lambda: &aws_sdk_lambda::Client, name: &str) -> String {
    let zip = aws_sdk_lambda::primitives::Blob::new(minimal_zip());
    lambda
        .create_function()
        .function_name(name)
        .runtime(aws_sdk_lambda::types::Runtime::Provided)
        .role("arn:aws:iam::000000000000:role/lambda-test-role")
        .handler(MINIMAL_HANDLER)
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(zip)
                .build(),
        )
        .send()
        .await
        .unwrap()
        .function_arn()
        .unwrap()
        .to_string()
}

fn minimal_zip() -> Vec<u8> {
    use std::io::Write;
    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let opts: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default();
        zip.start_file("index.sh", opts).unwrap();
        zip.write_all(b"#!/bin/sh\necho hi\n").unwrap();
        zip.finish().unwrap();
    }
    buf
}

#[tokio::test]
async fn source_access_configurations_round_trip() {
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    create_function(&lambda, "esm-sac-fn").await;

    let sac = SourceAccessConfiguration::builder()
        .r#type(SourceAccessType::BasicAuth)
        .uri("arn:aws:secretsmanager:us-east-1:123456789012:secret:mq-creds")
        .build();

    let create = lambda
        .create_event_source_mapping()
        .function_name("esm-sac-fn")
        .event_source_arn("arn:aws:mq:us-east-1:123456789012:broker:b1")
        .source_access_configurations(sac)
        .send()
        .await
        .unwrap();
    let uuid = create.uuid().unwrap().to_string();
    let created = create.source_access_configurations();
    assert_eq!(created.len(), 1, "create must echo the configuration");
    assert_eq!(
        created[0].uri(),
        Some("arn:aws:secretsmanager:us-east-1:123456789012:secret:mq-creds")
    );

    let got = lambda
        .get_event_source_mapping()
        .uuid(&uuid)
        .send()
        .await
        .unwrap();
    let got_sac = got.source_access_configurations();
    assert_eq!(
        got_sac.len(),
        1,
        "get must return the persisted configuration"
    );
    assert_eq!(got_sac[0].r#type(), Some(&SourceAccessType::BasicAuth));
}

/// CreateEventSourceMapping refuses a source that does not exist, as AWS
/// does (messages recorded against Lambda in us-east-1, see
/// `crates/fakecloud-lambda/tests/fixtures/create-event-source-mapping-missing-source.json`),
/// and maps the ones that do.
#[tokio::test]
async fn event_sources_must_exist() {
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    create_function(&lambda, "esm-source-fn").await;
    let create = |arn: &str| {
        lambda
            .create_event_source_mapping()
            .function_name("esm-source-fn")
            .event_source_arn(arn)
            .starting_position(aws_sdk_lambda::types::EventSourcePosition::Latest)
            .send()
    };
    let refused = |result: Result<_, aws_sdk_lambda::error::SdkError<_>>| {
        let err = result.expect_err("a missing source was mapped");
        let err = aws_sdk_lambda::error::SdkError::into_service_error(err);
        let meta = aws_sdk_lambda::error::ProvideErrorMetadata::meta(&err).clone();
        (
            meta.code().unwrap_or_default().to_string(),
            meta.message().unwrap_or_default().to_string(),
        )
    };

    let account = "123456789012";
    let missing_queue = format!("arn:aws:sqs:us-east-1:{account}:esm-missing-queue");
    assert_eq!(
        refused(
            lambda
                .create_event_source_mapping()
                .function_name("esm-source-fn")
                .event_source_arn(&missing_queue)
                .send()
                .await
        ),
        (
            "InvalidParameterValueException".to_string(),
            "Error occurred while ReceiveMessage. SQS Error Code: \
             AWS.SimpleQueueService.NonExistentQueue. SQS Error Message: The specified \
             queue does not exist."
                .to_string()
        )
    );
    for missing in [
        format!("arn:aws:kinesis:us-east-1:{account}:stream/esm-missing-stream"),
        format!(
            "arn:aws:dynamodb:us-east-1:{account}:table/esm-missing/stream/2024-01-01T00:00:00.000"
        ),
    ] {
        assert_eq!(
            refused(create(&missing).await),
            (
                "InvalidParameterValueException".to_string(),
                format!("Stream not found: {missing}")
            )
        );
    }
    let (code, message) = refused(create("").await);
    assert_eq!(code, "ValidationException");
    assert!(
        message.starts_with("1 validation error detected: Value '' at 'eventSourceArn'"),
        "{message}"
    );
    let (_, message) = refused(
        create(&format!(
            "arn:aws:sqs:us-west-2:{account}:esm-missing-queue"
        ))
        .await,
    );
    assert_eq!(
        message,
        "Event source region must match Lambda region us-east-1"
    );

    // Sources that exist are mapped.
    let sqs = server.sqs_client().await;
    let url = sqs
        .create_queue()
        .queue_name("esm-real-queue")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let queue_arn = sqs
        .get_queue_attributes()
        .queue_url(url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()[&aws_sdk_sqs::types::QueueAttributeName::QueueArn]
        .clone();
    lambda
        .create_event_source_mapping()
        .function_name("esm-source-fn")
        .event_source_arn(&queue_arn)
        .send()
        .await
        .unwrap();
    let kinesis = server.kinesis_client().await;
    kinesis
        .create_stream()
        .stream_name("esm-real-stream")
        .shard_count(1)
        .send()
        .await
        .unwrap();
    let stream_arn = kinesis
        .describe_stream_summary()
        .stream_name("esm-real-stream")
        .send()
        .await
        .unwrap()
        .stream_description_summary()
        .unwrap()
        .stream_arn()
        .to_string();
    create(&stream_arn).await.unwrap();
    let ddb = server.dynamodb_client().await;
    let table = ddb
        .create_table()
        .table_name("esm-real-table")
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .stream_specification(
            aws_sdk_dynamodb::types::StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(aws_sdk_dynamodb::types::StreamViewType::NewImage)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let ddb_stream = table
        .table_description()
        .unwrap()
        .latest_stream_arn()
        .unwrap()
        .to_string();
    create(&ddb_stream).await.unwrap();
}

fn error_code<E: aws_sdk_lambda::error::ProvideErrorMetadata>(
    err: aws_sdk_lambda::error::SdkError<E>,
) -> String {
    aws_sdk_lambda::error::ProvideErrorMetadata::code(&err)
        .unwrap_or_default()
        .to_string()
}

/// An Amazon MSK mapping keeps its `AmazonManagedKafkaEventSourceConfig`
/// (consumer group, schema registry), `ProvisionedPollerConfig` and
/// `LoggingConfig` on every response; the consumer group defaults to the
/// mapping's UUID and cannot be updated, the schema registry can.
#[tokio::test]
async fn msk_event_source_config_round_trips() {
    use aws_sdk_lambda::types::{
        AmazonManagedKafkaEventSourceConfig, EventSourceMappingLoggingConfig,
        EventSourceMappingSystemLogLevel, KafkaSchemaRegistryAccessConfig,
        KafkaSchemaRegistryAuthType, KafkaSchemaRegistryConfig, KafkaSchemaValidationAttribute,
        KafkaSchemaValidationConfig, ProvisionedPollerConfig, SchemaRegistryEventRecordFormat,
    };

    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    create_function(&lambda, "esm-msk-fn").await;
    let cluster = "arn:aws:kafka:us-east-1:123456789012:cluster/orders/0b1c2d3e-1111-2222-3333-444455556666-1";

    let registry = KafkaSchemaRegistryConfig::builder()
        .schema_registry_uri("arn:aws:glue:us-east-1:123456789012:registry/orders")
        .event_record_format(SchemaRegistryEventRecordFormat::Json)
        .access_configs(
            KafkaSchemaRegistryAccessConfig::builder()
                .r#type(KafkaSchemaRegistryAuthType::BasicAuth)
                .uri("arn:aws:secretsmanager:us-east-1:123456789012:secret:registry")
                .build(),
        )
        .schema_validation_configs(
            KafkaSchemaValidationConfig::builder()
                .attribute(KafkaSchemaValidationAttribute::Value)
                .build(),
        )
        .build();
    let created = lambda
        .create_event_source_mapping()
        .function_name("esm-msk-fn")
        .event_source_arn(cluster)
        .topics("orders")
        .starting_position(aws_sdk_lambda::types::EventSourcePosition::TrimHorizon)
        .amazon_managed_kafka_event_source_config(
            AmazonManagedKafkaEventSourceConfig::builder()
                .consumer_group_id("orders-consumers")
                .schema_registry_config(registry)
                .build(),
        )
        .provisioned_poller_config(
            ProvisionedPollerConfig::builder()
                .maximum_pollers(50)
                .build(),
        )
        .logging_config(
            EventSourceMappingLoggingConfig::builder()
                .system_log_level(EventSourceMappingSystemLogLevel::Info)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let uuid = created.uuid().unwrap().to_string();
    assert_eq!(
        created.event_source_mapping_arn(),
        Some(format!("arn:aws:lambda:us-east-1:123456789012:event-source-mapping:{uuid}").as_str())
    );

    let got = lambda
        .get_event_source_mapping()
        .uuid(&uuid)
        .send()
        .await
        .unwrap();
    let kafka = got.amazon_managed_kafka_event_source_config().unwrap();
    assert_eq!(kafka.consumer_group_id(), Some("orders-consumers"));
    let got_registry = kafka.schema_registry_config().unwrap();
    assert_eq!(
        got_registry.schema_registry_uri(),
        Some("arn:aws:glue:us-east-1:123456789012:registry/orders")
    );
    assert_eq!(
        got_registry.event_record_format(),
        Some(&SchemaRegistryEventRecordFormat::Json)
    );
    assert_eq!(got_registry.access_configs().len(), 1);
    assert_eq!(
        got_registry.schema_validation_configs()[0].attribute(),
        Some(&KafkaSchemaValidationAttribute::Value)
    );
    let pollers = got.provisioned_poller_config().unwrap();
    assert_eq!(pollers.maximum_pollers(), Some(50));
    assert_eq!(pollers.minimum_pollers(), Some(1));
    assert_eq!(
        got.logging_config().unwrap().system_log_level(),
        Some(&EventSourceMappingSystemLogLevel::Info)
    );
    let listed = lambda
        .list_event_source_mappings()
        .function_name("esm-msk-fn")
        .send()
        .await
        .unwrap();
    assert_eq!(
        listed.event_source_mappings()[0]
            .amazon_managed_kafka_event_source_config()
            .and_then(|c| c.consumer_group_id()),
        Some("orders-consumers")
    );

    // The consumer group is fixed at creation.
    let err = lambda
        .update_event_source_mapping()
        .uuid(&uuid)
        .amazon_managed_kafka_event_source_config(
            AmazonManagedKafkaEventSourceConfig::builder()
                .consumer_group_id("other-group")
                .build(),
        )
        .send()
        .await
        .expect_err("ConsumerGroupId was updated");
    assert_eq!(error_code(err), "InvalidParameterValueException");

    // The schema registry is not.
    let updated = lambda
        .update_event_source_mapping()
        .uuid(&uuid)
        .amazon_managed_kafka_event_source_config(
            AmazonManagedKafkaEventSourceConfig::builder()
                .schema_registry_config(
                    KafkaSchemaRegistryConfig::builder()
                        .schema_registry_uri("https://registry.example.com:8081")
                        .event_record_format(SchemaRegistryEventRecordFormat::Source)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let kafka = updated.amazon_managed_kafka_event_source_config().unwrap();
    assert_eq!(kafka.consumer_group_id(), Some("orders-consumers"));
    assert_eq!(
        kafka
            .schema_registry_config()
            .unwrap()
            .schema_registry_uri(),
        Some("https://registry.example.com:8081")
    );

    // An empty ProvisionedPollerConfig returns the mapping to on-demand mode.
    let updated = lambda
        .update_event_source_mapping()
        .uuid(&uuid)
        .provisioned_poller_config(ProvisionedPollerConfig::builder().build())
        .send()
        .await
        .unwrap();
    assert!(updated.provisioned_poller_config().is_none());

    let deleted = lambda
        .delete_event_source_mapping()
        .uuid(&uuid)
        .send()
        .await
        .unwrap();
    assert_eq!(
        deleted
            .amazon_managed_kafka_event_source_config()
            .and_then(|c| c.consumer_group_id()),
        Some("orders-consumers")
    );

    // Without one, the consumer group is the mapping's UUID.
    let defaulted = lambda
        .create_event_source_mapping()
        .function_name("esm-msk-fn")
        .event_source_arn(cluster)
        .topics("payments")
        .starting_position(aws_sdk_lambda::types::EventSourcePosition::Latest)
        .send()
        .await
        .unwrap();
    assert_eq!(
        defaulted
            .amazon_managed_kafka_event_source_config()
            .and_then(|c| c.consumer_group_id()),
        defaulted.uuid()
    );

    // A malformed consumer group, and a Kafka config on an SQS mapping, are refused.
    let err = lambda
        .create_event_source_mapping()
        .function_name("esm-msk-fn")
        .event_source_arn(cluster)
        .topics("orders")
        .amazon_managed_kafka_event_source_config(
            AmazonManagedKafkaEventSourceConfig::builder()
                .consumer_group_id("bad#group")
                .build(),
        )
        .send()
        .await
        .expect_err("a malformed ConsumerGroupId was accepted");
    assert_eq!(error_code(err), "InvalidParameterValueException");
    let sqs = server.sqs_client().await;
    let url = sqs
        .create_queue()
        .queue_name("esm-msk-queue")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let queue_arn = sqs
        .get_queue_attributes()
        .queue_url(url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()[&aws_sdk_sqs::types::QueueAttributeName::QueueArn]
        .clone();
    let err = lambda
        .create_event_source_mapping()
        .function_name("esm-msk-fn")
        .event_source_arn(&queue_arn)
        .amazon_managed_kafka_event_source_config(
            AmazonManagedKafkaEventSourceConfig::builder()
                .consumer_group_id("g")
                .build(),
        )
        .send()
        .await
        .expect_err("an MSK config was accepted on an SQS mapping");
    assert_eq!(error_code(err), "InvalidParameterValueException");
}

/// An SQS mapping keeps its `ScalingConfig`; an empty one removes it.
#[tokio::test]
async fn sqs_scaling_config_round_trips() {
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    create_function(&lambda, "esm-scaling-fn").await;
    let sqs = server.sqs_client().await;
    let url = sqs
        .create_queue()
        .queue_name("esm-scaling-queue")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let queue_arn = sqs
        .get_queue_attributes()
        .queue_url(url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()[&aws_sdk_sqs::types::QueueAttributeName::QueueArn]
        .clone();
    let created = lambda
        .create_event_source_mapping()
        .function_name("esm-scaling-fn")
        .event_source_arn(&queue_arn)
        .scaling_config(
            aws_sdk_lambda::types::ScalingConfig::builder()
                .maximum_concurrency(10)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let uuid = created.uuid().unwrap().to_string();
    assert!(created.amazon_managed_kafka_event_source_config().is_none());
    let got = lambda
        .get_event_source_mapping()
        .uuid(&uuid)
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.scaling_config().and_then(|s| s.maximum_concurrency()),
        Some(10)
    );
    let updated = lambda
        .update_event_source_mapping()
        .uuid(&uuid)
        .scaling_config(aws_sdk_lambda::types::ScalingConfig::builder().build())
        .send()
        .await
        .unwrap();
    assert!(updated.scaling_config().is_none());
}
