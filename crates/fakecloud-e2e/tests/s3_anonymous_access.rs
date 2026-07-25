//! Anonymous (unsigned) S3 access to public buckets/objects (#1707).
//!
//! An object created via `put_object` and reachable by a SigV4 presigned GET
//! used to 404 on a plain anonymous `GET /bucket/key` — the unsigned request
//! fell through service detection to the apigateway catch-all ("Stage not
//! found"). These tests pin the routing fix (default mode) and the
//! authorization behavior under `--iam strict` (public bucket policy / ACL
//! grant access; private objects are denied).

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_s3::types::ObjectCannedAcl;
use helpers::TestServer;

const PUBLIC_READ_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::probe/*"}]}"#;

/// An S3 client signing with root-bypass `test` creds — always passes SigV4
/// verification, so it can seed bucket/object/policy state even when the server
/// runs with `--verify-sigv4`.
async fn root_bypass_s3_client(server: &TestServer) -> aws_sdk_s3::Client {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "root-bypass"))
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(s3_config)
}

/// Default mode: an anonymous GET serves the object (fakecloud is permissive
/// by default), both before and after a public-read bucket policy. The bug was
/// a 404 here; this is the core regression test for #1707.
#[tokio::test]
async fn anonymous_get_object_default_mode_serves_object() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    s3.create_bucket().bucket("probe").send().await.unwrap();
    s3.put_object()
        .bucket("probe")
        .key("a.txt")
        .body(b"hello".to_vec().into())
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let url = format!("{}/probe/a.txt", server.endpoint());

    // Before any policy: permissive default mode still serves it.
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200, "anonymous GET should return 200");
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"hello");

    // After a public-read bucket policy: still 200.
    s3.put_bucket_policy()
        .bucket("probe")
        .policy(PUBLIC_READ_POLICY)
        .send()
        .await
        .unwrap();
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"hello");
}

/// Routing only claims unsigned requests whose first path segment is an
/// existing bucket. A request for a nonexistent bucket must still fall through
/// to the apigateway catch-all (404), unchanged.
#[tokio::test]
async fn anonymous_get_nonexistent_bucket_falls_through() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/ghostbucket/x.txt", server.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// IAM strict mode: an anonymous GET of a private object is denied (403);
/// after a public-read bucket policy it succeeds (200).
#[tokio::test]
async fn anonymous_get_object_iam_strict_bucket_policy() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let s3 = server.s3_client().await;

    s3.create_bucket().bucket("probe").send().await.unwrap();
    s3.put_object()
        .bucket("probe")
        .key("a.txt")
        .body(b"hello".to_vec().into())
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let url = format!("{}/probe/a.txt", server.endpoint());

    // Private: anonymous access denied.
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        403,
        "anonymous GET of a private object must be denied in IAM strict mode"
    );

    // Public-read bucket policy: anonymous access allowed.
    s3.put_bucket_policy()
        .bucket("probe")
        .policy(PUBLIC_READ_POLICY)
        .send()
        .await
        .unwrap();
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"hello");
}

/// IAM strict mode: a SigV2-presigned request carries its access key in the
/// `AWSAccessKeyId` query parameter (outside the SigV4 grammar). dispatch must
/// recover it so the caller is attributed instead of being treated as
/// anonymous. Without the fix (#1709) the request falls through the
/// anonymous-read gate and is denied 403 even with a valid key.
#[tokio::test]
async fn sigv2_presigned_request_is_attributed_to_its_caller() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let s3 = server.s3_client().await;

    s3.create_bucket().bucket("probe").send().await.unwrap();
    s3.put_object()
        .bucket("probe")
        .key("a.txt")
        .body(b"hello".to_vec().into())
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let base = format!("{}/probe/a.txt", server.endpoint());

    // Sanity: an unsigned anonymous GET of this private object is denied.
    let resp = http.get(&base).send().await.unwrap();
    assert_eq!(resp.status(), 403, "anonymous GET must be denied");

    // A SigV2-presigned URL (AWSAccessKeyId + Signature + Expires) for the
    // bucket-owning account's key is attributed to that caller and allowed.
    // The default test credentials resolve to the default account's root.
    let signed =
        format!("{base}?AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&Signature=dummysig&Expires=9999999999");
    let resp = http.get(&signed).send().await.unwrap();
    assert_eq!(
        resp.status(),
        200,
        "SigV2-presigned GET with a valid AWSAccessKeyId must be authorized"
    );
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"hello");
}

/// IAM strict mode: a public-read object ACL grants anonymous access, while a
/// sibling private object in the same bucket stays denied.
#[tokio::test]
async fn anonymous_get_object_iam_strict_public_acl() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let s3 = server.s3_client().await;

    s3.create_bucket().bucket("probe").send().await.unwrap();
    s3.put_object()
        .bucket("probe")
        .key("public.txt")
        .body(b"world".to_vec().into())
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("probe")
        .key("private.txt")
        .body(b"secret".to_vec().into())
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();

    let resp = http
        .get(format!("{}/probe/public.txt", server.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "public-read ACL object must be readable"
    );
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"world");

    let resp = http
        .get(format!("{}/probe/private.txt", server.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "private object must stay denied");
}

/// SigV4 verification ON + IAM strict: a fully anonymous (unsigned) request
/// must NOT be hard-rejected with `IncompleteSignature` before authorization
/// runs. AWS treats an unsigned request as the anonymous principal and lets a
/// public-read policy/ACL authorize it. A public-read object is served (200);
/// a private object is denied by the anonymous-authorization path with
/// `AccessDenied` (403) — NOT a pre-auth `IncompleteSignature`.
#[tokio::test]
async fn anonymous_get_reaches_public_read_under_verify_sigv4() {
    let server = TestServer::start_with_env(&[
        ("FAKECLOUD_IAM", "strict"),
        ("FAKECLOUD_VERIFY_SIGV4", "true"),
    ])
    .await;
    // Seed S3 state with root-bypass creds (always pass verification).
    let s3 = root_bypass_s3_client(&server).await;

    s3.create_bucket().bucket("probe").send().await.unwrap();
    s3.put_object()
        .bucket("probe")
        .key("public.txt")
        .body(b"world".to_vec().into())
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("probe")
        .key("private.txt")
        .body(b"secret".to_vec().into())
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();

    // Public-read object: the anonymous GET is authorized (200), proving the
    // request reached the anonymous-authorization path rather than being
    // hard-403'd for lacking a signature.
    let resp = http
        .get(format!("{}/probe/public.txt", server.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "public-read object must be served to an anonymous caller under --verify-sigv4"
    );
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"world");

    // Private object: denied by authorization, not by a pre-auth signature
    // check. The error must be AccessDenied, never IncompleteSignature.
    let resp = http
        .get(format!("{}/probe/private.txt", server.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("AccessDenied"),
        "expected AccessDenied for a private object, got {body}"
    );
    assert!(
        !body.contains("IncompleteSignature"),
        "anonymous request must not be pre-auth rejected under --verify-sigv4: {body}"
    );
}

/// IAM strict mode: an explicit `Deny` in the bucket policy overrides a
/// public-read object ACL, matching AWS's Deny-overrides precedence. Before the
/// fix the anonymous gate ORed the ACL onto the policy decision, so a public
/// ACL could grant access the bucket owner had explicitly denied.
#[tokio::test]
async fn anonymous_get_explicit_deny_overrides_public_acl() {
    const EXPLICIT_DENY_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::probe/*"}]}"#;

    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let s3 = server.s3_client().await;

    s3.create_bucket().bucket("probe").send().await.unwrap();
    s3.put_object()
        .bucket("probe")
        .key("public.txt")
        .body(b"world".to_vec().into())
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let url = format!("{}/probe/public.txt", server.endpoint());

    // With only the public-read ACL, the object is served.
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        200,
        "public-read ACL object must be readable"
    );

    // After an explicit-Deny bucket policy, the anonymous GET must be denied
    // even though the public ACL still grants it.
    s3.put_bucket_policy()
        .bucket("probe")
        .policy(EXPLICIT_DENY_POLICY)
        .send()
        .await
        .unwrap();
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        403,
        "explicit Deny must override the public-read ACL"
    );
}
