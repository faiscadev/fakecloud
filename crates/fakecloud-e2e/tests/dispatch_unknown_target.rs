//! Dispatch compatibility: a request carrying an unrecognized `X-Amz-Target`
//! header is an awsJson call whose operation we can't map to a known service.
//! AWS answers these with `UnknownOperationException`; fakecloud must not route
//! them to the apigateway catch-all (which 404s with a misleading "Stage not
//! found").

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn unknown_x_amz_target_returns_unknown_operation_exception() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();

    let resp = http
        .post(server.endpoint())
        .header("content-type", "application/x-amz-json-1.1")
        .header("x-amz-target", "NotARealService_20990101.DoSomething")
        .body("{}")
        .send()
        .await
        .expect("request sent");

    assert_eq!(resp.status(), 400, "unknown target must be a 400");
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("UnknownOperationException"),
        "expected UnknownOperationException, got: {body}"
    );
    // And crucially not the misleading apigateway catch-all.
    assert!(
        !body.contains("Stage") && !body.contains("NotFoundException"),
        "must not fall through to the apigateway catch-all: {body}"
    );
}
