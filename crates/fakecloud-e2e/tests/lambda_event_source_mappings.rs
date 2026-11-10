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
