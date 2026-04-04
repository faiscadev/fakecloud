mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn server_starts_and_responds() {
    let server = TestServer::start().await;

    // Any SQS call should get a "not implemented" style error since the
    // service is registered but has no actions implemented yet.
    let client = server.sqs_client().await;
    let result = client.list_queues().send().await;

    // We expect an error since nothing is implemented yet
    assert!(result.is_err(), "expected error from stub service");
}

#[tokio::test]
async fn server_returns_error_for_unknown_service_via_cli() {
    let server = TestServer::start().await;

    // STS get-caller-identity should hit our stub and return an error
    let output = server.aws_cli(&["sts", "get-caller-identity"]).await;
    // The CLI should get some response (even if it's an error)
    // Just verify the server didn't crash
    let stderr = output.stderr_text();
    assert!(
        !stderr.contains("Could not connect"),
        "server should be reachable: {stderr}"
    );
}
