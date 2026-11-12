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

/// Create the `probe` bucket the way a user publishing objects must on a
/// bucket created since April 2023: ACLs enabled (`ObjectWriter`) and the
/// default Block Public Access removed, so public-read ACLs and public bucket
/// policies are accepted.
async fn create_public_probe(s3: &aws_sdk_s3::Client) {
    s3.create_bucket()
        .bucket("probe")
        .object_ownership(aws_sdk_s3::types::ObjectOwnership::ObjectWriter)
        .send()
        .await
        .unwrap();
    s3.delete_public_access_block()
        .bucket("probe")
        .send()
        .await
        .unwrap();
}

/// Default mode: an anonymous GET serves the object (fakecloud is permissive
/// by default), both before and after a public-read bucket policy. The bug was
/// a 404 here; this is the core regression test for #1707.
#[tokio::test]
async fn anonymous_get_object_default_mode_serves_object() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    create_public_probe(&s3).await;
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

/// Send an unsigned request and return its status, `Content-Type` and body.
async fn unsigned(req: reqwest::RequestBuilder) -> (u16, String, String) {
    let resp = req.send().await.unwrap();
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    (status, content_type, resp.text().await.unwrap())
}

/// An unsigned path-style request for a bucket that does not exist is still an
/// S3 request on S3's endpoint: S3 answers it exactly as it answers the signed
/// request (`NoSuchBucket` naming the bucket, as recorded against real S3;
/// HEAD is a bare 404), instead of the apigateway catch-all ("Stage not
/// found").
#[tokio::test]
async fn anonymous_request_to_missing_bucket_is_answered_by_s3() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let base = server.endpoint();
    for (req, code) in [
        (
            http.get(format!("{base}/ghostbucket/x.txt")),
            "NoSuchBucket",
        ),
        (
            http.get(format!("{base}/ghostbucket/x.txt?attributes")),
            "NoSuchBucket",
        ),
        (
            http.get(format!("{base}/ghostbucket/x.txt?acl")),
            "NoSuchBucket",
        ),
        (http.head(format!("{base}/ghostbucket/x.txt")), ""),
        (
            http.put(format!("{base}/ghostbucket/x.txt")).body("hi"),
            "NoSuchBucket",
        ),
        (
            http.get(format!("{base}/ghostbucket?list-type=2")),
            "NoSuchBucket",
        ),
        (http.get(format!("{base}/ghostbucket")), "NoSuchBucket"),
    ] {
        let (status, content_type, body) = unsigned(req).await;
        assert_eq!(status, 404, "{body}");
        if code.is_empty() {
            assert!(body.is_empty(), "{body}");
        } else {
            assert!(content_type.contains("xml"), "{content_type}: {body}");
            assert!(body.contains(&format!("<Code>{code}</Code>")), "{body}");
            assert!(
                body.contains("<BucketName>ghostbucket</BucketName>"),
                "{body}"
            );
        }
    }
}

/// A signed GetObject (and GetObjectAttributes / GetObjectAcl) on a missing
/// bucket is `NoSuchBucket` too, as real S3 answers.
#[tokio::test]
async fn signed_object_reads_on_missing_bucket_are_no_such_bucket() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let get = s3
        .get_object()
        .bucket("ghostbucket")
        .key("x.txt")
        .send()
        .await
        .unwrap_err();
    let get = get.raw_response().unwrap();
    assert_eq!(get.status().as_u16(), 404);
    let body = std::str::from_utf8(get.body().bytes().unwrap()).unwrap();
    assert!(body.contains("<Code>NoSuchBucket</Code>"), "{body}");
    assert!(
        body.contains("<Message>The specified bucket does not exist</Message>"),
        "{body}"
    );
    assert!(
        body.contains("<BucketName>ghostbucket</BucketName>"),
        "{body}"
    );

    let attrs = s3
        .get_object_attributes()
        .bucket("ghostbucket")
        .key("x.txt")
        .object_attributes(aws_sdk_s3::types::ObjectAttributes::Etag)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        aws_sdk_s3::error::ProvideErrorMetadata::code(&attrs),
        Some("NoSuchBucket")
    );
    let acl = s3
        .get_object_acl()
        .bucket("ghostbucket")
        .key("x.txt")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        aws_sdk_s3::error::ProvideErrorMetadata::code(&acl),
        Some("NoSuchBucket")
    );
    let head = s3
        .head_object()
        .bucket("ghostbucket")
        .key("x.txt")
        .send()
        .await
        .unwrap_err();
    assert_eq!(head.raw_response().unwrap().status().as_u16(), 404);
}

/// IAM strict mode: an anonymous object request for a missing bucket is
/// `NoSuchBucket`, as AWS answers whoever the caller is. This depends on S3
/// reporting a missing bucket ahead of anonymous authorization (the ordering
/// the S3 action-names change introduces). Creating a bucket anonymously is
/// still `AccessDenied`.
#[tokio::test]
async fn anonymous_request_to_missing_bucket_iam_strict_is_no_such_bucket() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let http = reqwest::Client::new();
    let base = server.endpoint();
    for req in [
        http.get(format!("{base}/ghostbucket/x.txt")),
        http.put(format!("{base}/ghostbucket/x.txt")).body("hi"),
    ] {
        let (status, _, body) = unsigned(req).await;
        assert_eq!(status, 404, "{body}");
        assert!(body.contains("<Code>NoSuchBucket</Code>"), "{body}");
        assert!(
            body.contains("<BucketName>ghostbucket</BucketName>"),
            "{body}"
        );
    }
}

/// IAM strict mode: S3 refuses an anonymous CreateBucket outright with its own
/// message (recorded in us-east-1), and creates nothing.
#[tokio::test]
async fn anonymous_create_bucket_iam_strict_is_refused() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let http = reqwest::Client::new();
    let base = server.endpoint();
    let (status, _, body) = unsigned(http.put(format!("{base}/ghostbucket"))).await;
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
    assert!(
        body.contains(
            "<Message>Anonymous users cannot invoke this API. Please authenticate.</Message>"
        ),
        "{body}"
    );
    // The bucket was not created by the denied anonymous PUT.
    let s3 = helpers::root_s3_client(&server).await;
    assert!(s3.head_bucket().bucket("ghostbucket").send().await.is_err());
}

/// Default mode: an anonymous PUT to an existing bucket writes the object, as
/// fakecloud is permissive by default; a public-read object is then served to
/// an anonymous GET.
#[tokio::test]
async fn anonymous_put_to_existing_bucket_default_mode() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    create_public_probe(&s3).await;
    let http = reqwest::Client::new();
    let url = format!("{}/probe/up.txt", server.endpoint());
    let (status, _, body) = unsigned(http.put(&url).body("uploaded")).await;
    assert_eq!(status, 200, "{body}");
    let got = s3
        .get_object()
        .bucket("probe")
        .key("up.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes().as_ref(),
        b"uploaded"
    );
    let (status, _, body) = unsigned(http.get(&url)).await;
    assert_eq!(status, 200);
    assert_eq!(body, "uploaded");

    // The same upload addressed to S3's endpoint host (path-style and
    // virtual-hosted) streams its body too.
    for (host, path) in [
        ("s3.us-east-1.amazonaws.com", "/probe/host.txt"),
        ("probe.s3.us-east-1.amazonaws.com", "/vhost.txt"),
    ] {
        let (status, _, body) = unsigned(
            http.put(format!("{}{path}", server.endpoint()))
                .header("host", host)
                .body("via-host"),
        )
        .await;
        assert_eq!(status, 200, "{host}: {body}");
    }
    for key in ["host.txt", "vhost.txt"] {
        let got = s3
            .get_object()
            .bucket("probe")
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            got.body.collect().await.unwrap().into_bytes().as_ref(),
            b"via-host"
        );
    }
}

/// IAM strict mode: an anonymous GET of a private object is denied (403);
/// after a public-read bucket policy it succeeds (200).
#[tokio::test]
async fn anonymous_get_object_iam_strict_bucket_policy() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let s3 = helpers::root_s3_client(&server).await;

    create_public_probe(&s3).await;
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
    let s3 = helpers::root_s3_client(&server).await;

    create_public_probe(&s3).await;
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

    // A SigV2-presigned URL (AWSAccessKeyId + Signature + Expires) for an IAM
    // user allowed to read the object is attributed to that user and allowed.
    let iam = aws_sdk_iam::Client::new(&helpers::root_config(&server).await);
    iam.create_user().user_name("reader").send().await.unwrap();
    iam.put_user_policy()
        .user_name("reader")
        .policy_name("read")
        .policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::probe/*"}]}"#,
        )
        .send()
        .await
        .unwrap();
    let key = iam
        .create_access_key()
        .user_name("reader")
        .send()
        .await
        .unwrap();
    let akid = key.access_key().unwrap().access_key_id().to_string();

    // A key that resolves to no identity is rejected, not read as anonymous
    // or as the account root.
    // (Expires 2000000000 is a valid far-future epoch; S3 rejects 9999999999.)
    let unknown =
        format!("{base}?AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&Signature=dummysig&Expires=2000000000");
    let resp = http.get(&unknown).send().await.unwrap();
    assert_eq!(resp.status(), 403, "unresolved SigV2 key must be rejected");
    assert!(resp.text().await.unwrap().contains("InvalidClientTokenId"));

    let signed = format!("{base}?AWSAccessKeyId={akid}&Signature=dummysig&Expires=2000000000");
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
    let s3 = helpers::root_s3_client(&server).await;

    create_public_probe(&s3).await;
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

    create_public_probe(&s3).await;
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
    let s3 = helpers::root_s3_client(&server).await;

    create_public_probe(&s3).await;
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

/// An expired SigV2 query string (recorded: `Expires=-1`) is 403
/// `AccessDenied` "Request has expired", naming the expiry and the server's
/// clock; the last valid 32-bit time is accepted and reaches S3.
#[tokio::test]
async fn expired_sigv2_query_auth_is_request_has_expired() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let base = server.endpoint();
    let (status, _, body) = unsigned(http.get(format!(
        "{base}/ghostbucket/k?AWSAccessKeyId=AKID&Signature=sig&Expires=-1"
    )))
    .await;
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
    assert!(
        body.contains("<Message>Request has expired</Message>"),
        "{body}"
    );
    assert!(
        body.contains("<Expires>1969-12-31T23:59:59Z</Expires>"),
        "{body}"
    );
    assert!(body.contains("<ServerTime>20"), "{body}");

    let (status, _, body) = unsigned(http.get(format!(
        "{base}/ghostbucket/k?AWSAccessKeyId=AKID&Signature=sig&Expires=2147483648"
    )))
    .await;
    assert_eq!(status, 403, "{body}");
    assert!(
        body.contains("Invalid date (should be seconds since epoch): 2147483648"),
        "{body}"
    );
    let (status, _, body) = unsigned(http.get(format!(
        "{base}/ghostbucket/k?AWSAccessKeyId=AKID&Signature=sig&Expires=2147483647"
    )))
    .await;
    assert_eq!(status, 404, "{body}");
    assert!(body.contains("<Code>NoSuchBucket</Code>"), "{body}");
}

/// Under `--verify-sigv4` an expired SigV4 presigned S3 URL is 403
/// `AccessDenied` "Request has expired" carrying S3's `X-Amz-Expires`,
/// `Expires` and `ServerTime` elements.
#[tokio::test]
async fn expired_sigv4_presign_names_its_lifetime_and_times() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_VERIFY_SIGV4", "true")]).await;
    let iam = aws_sdk_iam::Client::new(&helpers::root_config(&server).await);
    iam.create_user()
        .user_name("presigner")
        .send()
        .await
        .unwrap();
    let key = iam
        .create_access_key()
        .user_name("presigner")
        .send()
        .await
        .unwrap();
    let key = key.access_key().unwrap();
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            key.access_key_id(),
            key.secret_access_key(),
            None,
            None,
            "presigner",
        ))
        .load()
        .await;
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build(),
    );
    let signed_at = std::time::SystemTime::now() - std::time::Duration::from_secs(7200);
    let presigned = s3
        .get_object()
        .bucket("probe")
        .key("k")
        .presigned(
            aws_sdk_s3::presigning::PresigningConfig::builder()
                .start_time(signed_at)
                .expires_in(std::time::Duration::from_secs(60))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, _, body) = unsigned(reqwest::Client::new().get(presigned.uri())).await;
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
    assert!(
        body.contains("<Message>Request has expired</Message>"),
        "{body}"
    );
    assert!(body.contains("<X-Amz-Expires>60</X-Amz-Expires>"), "{body}");
    let expiry =
        chrono::Timelike::with_nanosecond(&chrono::DateTime::<chrono::Utc>::from(signed_at), 0)
            .unwrap()
            + chrono::Duration::seconds(60);
    assert!(
        body.contains(&format!(
            "<Expires>{}</Expires>",
            expiry.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        )),
        "{body}"
    );
    assert!(body.contains("<ServerTime>20"), "{body}");
}
