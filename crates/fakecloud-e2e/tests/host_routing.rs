//! Host header decoding for LocalStack- and AWS-shaped hostnames.
//!
//! Fakecloud decodes both
//! `<service>.<region>.localhost.localstack.cloud[:port]` (LocalStack) and
//! `<service>.<region>.amazonaws.com` (real AWS) as routing signals, plus
//! every S3 virtual-hosted and path-style variant of each. Lets fixtures,
//! presigned URLs, and dev scripts persisted against either system replay
//! against fakecloud unchanged.

mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

fn localstack_host(sub: &str, port: u16) -> String {
    format!("{sub}.localhost.localstack.cloud:{port}")
}

fn aws_host(sub: &str) -> String {
    format!("{sub}.amazonaws.com")
}

#[tokio::test]
async fn unsigned_sqs_list_queues_via_host_header() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();

    let resp = http
        .post(format!("{}/", server.endpoint()))
        .header("Host", localstack_host("sqs.us-east-1", server.port()))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("Action=ListQueues&Version=2012-11-05")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("ListQueuesResponse") || body.contains("ListQueuesResult"),
        "expected SQS XML response, got: {body}"
    );
}

#[tokio::test]
async fn unsigned_s3_list_buckets_via_host_header() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("{}/", server.endpoint()))
        .header("Host", localstack_host("s3.us-east-1", server.port()))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("ListAllMyBucketsResult"),
        "expected S3 ListAllMyBuckets XML, got: {body}"
    );
}

#[tokio::test]
async fn unsigned_lambda_list_functions_via_host_header() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();

    // Both the bare `/functions` and the trailing-slash `/functions/`
    // forms are `ListFunctions`. Real AWS tolerates both, and botocore
    // serialized `ListFunctions` as `GET /2015-03-31/functions/` until
    // botocore 1.40 (mid-2025), so the whole pre-1.40 install base sends
    // the trailing slash (issue #1645). Assert both return the list.
    for path in ["/2015-03-31/functions", "/2015-03-31/functions/"] {
        let resp = http
            .get(format!("{}{path}", server.endpoint()))
            .header("Host", localstack_host("lambda.us-east-1", server.port()))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "GET {path} should be ListFunctions");
        let body = resp.text().await.unwrap();
        // Lambda returns JSON with a Functions array.
        assert!(
            body.contains("Functions"),
            "expected Lambda ListFunctions JSON for {path}, got: {body}"
        );
    }
}

#[tokio::test]
async fn s3_virtual_hosted_put_get_delete() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    // Create the bucket path-style via the SDK.
    s3.create_bucket()
        .bucket("vhost-bucket")
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let vhost_host = localstack_host("vhost-bucket.s3.us-east-1", server.port());

    // PUT object via virtual-hosted-style: URL path is `/key` (no bucket),
    // bucket lives in the Host header.
    let put = http
        .put(format!("{}/hello.txt", server.endpoint()))
        .header("Host", &vhost_host)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fake",
        )
        .body("hello world")
        .send()
        .await
        .unwrap();
    assert!(
        put.status().is_success(),
        "PUT failed: {} — {}",
        put.status(),
        put.text().await.unwrap_or_default()
    );

    // GET it back virtual-hosted.
    let get = http
        .get(format!("{}/hello.txt", server.endpoint()))
        .header("Host", &vhost_host)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(get.text().await.unwrap(), "hello world");

    // DELETE virtual-hosted.
    let del = http
        .delete(format!("{}/hello.txt", server.endpoint()))
        .header("Host", &vhost_host)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert!(del.status().is_success());

    // Confirm through the SDK (path-style) that the object is gone.
    let list = s3
        .list_objects_v2()
        .bucket("vhost-bucket")
        .send()
        .await
        .unwrap();
    assert_eq!(list.contents().len(), 0);
}

#[tokio::test]
async fn s3_virtual_hosted_bucket_with_dots() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("a.b.c").send().await.unwrap();
    s3.put_object()
        .bucket("a.b.c")
        .key("obj")
        .body(ByteStream::from_static(b"dotted"))
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/obj", server.endpoint()))
        .header("Host", localstack_host("a.b.c.s3.us-east-1", server.port()))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "dotted");
}

#[tokio::test]
async fn s3_virtual_hosted_list_objects_on_bucket_root() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("root-bucket")
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    // GET / against the vhost hostname = ListObjectsV1 on the bucket.
    let resp = http
        .get(format!("{}/", server.endpoint()))
        .header(
            "Host",
            localstack_host("root-bucket.s3.us-east-1", server.port()),
        )
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("ListBucketResult"),
        "expected ListBucketResult XML, got: {body}"
    );
    assert!(body.contains("<Name>root-bucket</Name>"));
}

#[tokio::test]
async fn sigv4_credential_scope_wins_over_host_header() {
    // A signed S3 request whose Host header is shaped like a Lambda
    // hostname must still be dispatched to S3 — the SigV4 scope is
    // the canonical truth, otherwise CDN-style proxies mutating the
    // Host header could redirect signed traffic to the wrong service.
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("sigv4-wins")
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/sigv4-wins/", server.endpoint()))
        .header("Host", localstack_host("lambda.us-east-1", server.port()))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    // S3 ListObjectsV1: status 200 with ListBucketResult XML.
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("ListBucketResult"),
        "expected S3 response despite Lambda-shaped Host, got: {body}"
    );
}

// ---- Real-AWS hostname shapes ----

#[tokio::test]
async fn unsigned_sqs_list_queues_via_aws_host() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();

    let resp = http
        .post(format!("{}/", server.endpoint()))
        .header("Host", aws_host("sqs.us-east-1"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("Action=ListQueues&Version=2012-11-05")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("ListQueuesResponse") || body.contains("ListQueuesResult"),
        "expected SQS XML response, got: {body}"
    );
}

#[tokio::test]
async fn unsigned_s3_list_buckets_via_aws_host_modern() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("{}/", server.endpoint()))
        .header("Host", aws_host("s3.us-east-1"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp
        .text()
        .await
        .unwrap()
        .contains("ListAllMyBucketsResult"));
}

#[tokio::test]
async fn unsigned_s3_list_buckets_via_aws_host_legacy_global() {
    // `s3.amazonaws.com` with no region is the legacy global endpoint;
    // AWS treats it as us-east-1. Verify fakecloud routes it the same way.
    let server = TestServer::start().await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("{}/", server.endpoint()))
        .header("Host", "s3.amazonaws.com")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp
        .text()
        .await
        .unwrap()
        .contains("ListAllMyBucketsResult"));
}

#[tokio::test]
async fn s3_virtual_hosted_put_get_via_aws_modern() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("aws-vhost").send().await.unwrap();
    s3.put_object()
        .bucket("aws-vhost")
        .key("obj")
        .body(ByteStream::from_static(b"aws-modern"))
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/obj", server.endpoint()))
        .header("Host", aws_host("aws-vhost.s3.us-east-1"))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "aws-modern");
}

#[tokio::test]
async fn s3_virtual_hosted_put_get_via_aws_legacy_global() {
    // `<bucket>.s3.amazonaws.com` (no region) is the legacy virtual-hosted
    // form AWS still serves for us-east-1.
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("legacy-vhost")
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("legacy-vhost")
        .key("obj")
        .body(ByteStream::from_static(b"legacy"))
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/obj", server.endpoint()))
        .header("Host", "legacy-vhost.s3.amazonaws.com")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "legacy");
}

#[tokio::test]
async fn s3_virtual_hosted_legacy_global_dotted_bucket() {
    // `a.b.c.s3.amazonaws.com` — dotted bucket name on the legacy
    // us-east-1 global endpoint. AWS accepts it; fakecloud must too.
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("a.b.c").send().await.unwrap();
    s3.put_object()
        .bucket("a.b.c")
        .key("obj")
        .body(ByteStream::from_static(b"dotted-legacy"))
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/obj", server.endpoint()))
        .header("Host", "a.b.c.s3.amazonaws.com")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "dotted-legacy");
}

#[tokio::test]
async fn s3_virtual_hosted_put_get_via_aws_dash_separated() {
    // The older `<bucket>.s3-<region>.amazonaws.com` form AWS still serves
    // — some long-lived fixtures encode it.
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("dash-vhost")
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("dash-vhost")
        .key("obj")
        .body(ByteStream::from_static(b"dashed"))
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/obj", server.endpoint()))
        .header("Host", "dash-vhost.s3-us-west-2.amazonaws.com")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-west-2/s3/aws4_request, \
             SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "dashed");
}
