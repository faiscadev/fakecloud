// bug-audit 2026-05-28, 1.17: CreateEventSourceMapping must persist the caller's
// SourceAccessConfigurations and echo them on Get, not always return [].
#[tokio::test]
async fn source_access_configurations_round_trip() {
    use aws_sdk_lambda::types::{SourceAccessConfiguration, SourceAccessType};
    let server = TestServer::start().await;
    let client = lambda_client(&server).await;
    create_fn(&client, "esm-sac-fn").await;

    let sac = SourceAccessConfiguration::builder()
        .r#type(SourceAccessType::BasicAuth)
        .uri("arn:aws:secretsmanager:us-east-1:123456789012:secret:mq-creds")
        .build();

    let create = client
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

    let got = client
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
