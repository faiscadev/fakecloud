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
