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

/// A Query `Action` in the body is an API call only when the body is declared
/// a form (`application/x-www-form-urlencoded`), before and after the body is
/// read alike, as on AWS (recorded: the same STS body as `text/plain` or
/// untyped is not processed as an API call). Under `--iam strict` too, the
/// form reaches STS and the rest never does.
#[tokio::test]
async fn query_body_needs_a_form_content_type() {
    for env in [&[][..], &[("FAKECLOUD_IAM", "strict")][..]] {
        let server = TestServer::start_with_env(env).await;
        let http = reqwest::Client::new();
        let body = "Action=GetCallerIdentity&Version=2011-06-15";
        for content_type in [
            Some("application/x-www-form-urlencoded"),
            Some("application/x-www-form-urlencoded; charset=utf-8"),
            Some("text/plain"),
            None,
        ] {
            let mut req = http.post(format!("{}/", server.endpoint())).body(body);
            if let Some(ct) = content_type {
                req = req.header("Content-Type", ct);
            }
            let resp = req.send().await.unwrap();
            let text = resp.text().await.unwrap();
            let reached_sts = text.contains("GetCallerIdentityResponse")
                || (text.contains("<ErrorResponse") && text.contains("<Type>Sender</Type>"));
            let is_form = content_type.is_some_and(|ct| ct.starts_with("application/x-www"));
            assert_eq!(reached_sts, is_form, "{env:?} {content_type:?}: {text}");
        }
    }
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

/// A Query `Action` in the query string routes an unsigned GET by the
/// action's service, as a form body does: `ListTopics` is SNS's, on the
/// plain host too (it used to fall to S3's ListBuckets).
#[tokio::test]
async fn unsigned_query_string_action_routes_by_action() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    for (query, marker) in [
        (
            "Action=ListTopics&Version=2010-03-31",
            "<ListTopicsResponse",
        ),
        (
            "Action=ListQueues&Version=2012-11-05",
            "<ListQueuesResponse",
        ),
    ] {
        let resp = http
            .get(format!("{}/?{query}", server.endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "{query}");
        let body = resp.text().await.unwrap();
        assert!(body.contains(marker), "{query}: {body}");
    }
}

/// A query-string `Action` routes only a request nothing else routes: an S3
/// object (path-style on the plain host, virtual-hosted, or presigned) or an
/// API Gateway invocation carrying an `Action` parameter stays theirs.
#[tokio::test]
async fn query_string_action_does_not_hijack_s3_or_api_gateway() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("pubb").send().await.unwrap();
    s3.put_object()
        .bucket("pubb")
        .key("k.txt")
        .body(ByteStream::from_static(b"object body"))
        .send()
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let get = |path: String, host: Option<String>| {
        let mut req = http.get(format!("{}{path}", server.endpoint()));
        if let Some(host) = host {
            req = req.header("Host", host);
        }
        async move {
            let resp = req.send().await.unwrap();
            (resp.status().as_u16(), resp.text().await.unwrap())
        }
    };

    // Path-style on the plain host: the bucket exists, so it is S3's.
    let (status, body) = get("/pubb/k.txt?Action=ListQueues".into(), None).await;
    assert_eq!((status, body.as_str()), (200, "object body"));
    // Virtual-hosted.
    let (status, body) = get(
        "/k.txt?Action=DescribeRegions".into(),
        Some(aws_host("pubb.s3.us-east-1")),
    )
    .await;
    assert_eq!((status, body.as_str()), (200, "object body"));
    // Presigned (unverified without --verify-sigv4): its scope names S3.
    let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let presigned = format!(
        "/pubb/k.txt?Action=ListQueues&X-Amz-Algorithm=AWS4-HMAC-SHA256\
         &X-Amz-Credential=test%2F{}%2Fus-east-1%2Fs3%2Faws4_request\
         &X-Amz-Date={date}&X-Amz-Expires=300&X-Amz-SignedHeaders=host\
         &X-Amz-Signature=00",
        &date[..8]
    );
    let (status, body) = get(presigned, None).await;
    assert_eq!((status, body.as_str()), (200, "object body"));
    // An execute-api host is the API's: SNS never sees the Publish.
    let (_, body) = get(
        "/prod/items?Action=Publish&Message=x".into(),
        Some(aws_host("abc123.execute-api.us-east-1")),
    )
    .await;
    assert!(!body.contains("sns.amazonaws.com"), "{body}");
    assert!(!body.contains("PublishResponse"), "{body}");
}

/// SES v1 is Query on SES's signing scope (v2 is restJson1 on the same
/// scope): an `Action` in the URL of a signed GET, a presigned GET, or a POST
/// whose other parameters are in the body is SES v1's.
#[tokio::test]
async fn ses_v1_query_action_in_the_url_reaches_ses() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let scope = format!("test/{}/us-east-1/ses/aws4_request", &date[..8]);
    let auth = format!("AWS4-HMAC-SHA256 Credential={scope}, SignedHeaders=host, Signature=00");
    let base = server.endpoint();

    let signed_get = http
        .get(format!("{base}/?Action=ListIdentities&Version=2010-12-01"))
        .header("authorization", &auth);
    let presigned_get = http.get(format!(
        "{base}/?Action=ListIdentities&Version=2010-12-01&X-Amz-Algorithm=AWS4-HMAC-SHA256\
         &X-Amz-Credential={}&X-Amz-Date={date}&X-Amz-Expires=300\
         &X-Amz-SignedHeaders=host&X-Amz-Signature=00",
        scope.replace('/', "%2F")
    ));
    let post = http
        .post(format!("{base}/?Action=ListIdentities"))
        .header("authorization", &auth)
        .header("content-type", "application/x-www-form-urlencoded")
        .body("Version=2010-12-01&IdentityType=EmailAddress");
    for (name, req) in [
        ("signed GET", signed_get),
        ("presigned GET", presigned_get),
        ("POST", post),
    ] {
        let resp = req.send().await.unwrap();
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap();
        assert_eq!(status, 200, "{name}: {body}");
        assert!(body.contains("<ListIdentitiesResponse"), "{name}: {body}");
    }
}

/// SigV2 Query authentication (`SignatureVersion=2` + `Timestamp`, no
/// `Expires`) is not an S3 presign: the request is its Action's service's.
#[tokio::test]
async fn sigv2_query_auth_routes_by_action() {
    let server = TestServer::start().await;
    for (action, version, marker) in [
        ("ListQueues", "2012-11-05", "<ListQueuesResponse"),
        ("DescribeRegions", "2016-11-15", "<DescribeRegionsResponse"),
    ] {
        let resp = reqwest::Client::new()
            .get(format!(
                "{}/?Action={action}&Version={version}&AWSAccessKeyId=test\
                 &SignatureVersion=2&SignatureMethod=HmacSHA256\
                 &Timestamp=2026-11-13T00%3A00%3A00Z&Signature=abc",
                server.endpoint()
            ))
            .send()
            .await
            .unwrap();
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap();
        assert_eq!(status, 200, "{action}: {body}");
        assert!(body.contains(marker), "{action}: {body}");
    }
}

/// SES (v1 and v2) is served at `email.<region>`: an unsigned SES v2 REST
/// call on that host is SES's, not the ECR registry's `/v2/`.
#[tokio::test]
async fn unsigned_sesv2_on_the_email_host_reaches_ses() {
    let server = TestServer::start().await;
    for host in [
        aws_host("email.us-east-1"),
        localstack_host("email.us-east-1", 4566),
    ] {
        let resp = reqwest::Client::new()
            .get(format!("{}/v2/email/identities", server.endpoint()))
            .header("Host", &host)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "{host}");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(body["EmailIdentities"].is_array(), "{host}: {body}");
    }
}
