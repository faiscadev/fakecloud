mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn server_starts_and_responds() {
    let server = TestServer::start().await;

    // SQS list queues should work and return empty list
    let client = server.sqs_client().await;
    let result = client.list_queues().send().await.unwrap();
    assert_eq!(result.queue_urls().len(), 0);
}

#[tokio::test]
async fn server_responds_to_cli() {
    let server = TestServer::start().await;

    let output = server.aws_cli(&["sts", "get-caller-identity"]).await;
    assert!(
        output.success(),
        "CLI should succeed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(json["Account"], "123456789012");
}
