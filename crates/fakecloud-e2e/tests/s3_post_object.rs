//! End-to-end tests for S3 POST Object (the browser-form "POST Policy"
//! upload flow targeted by boto3's `generate_presigned_post`).
//!
//! The AWS SDK has no client method for this flow — it's meant to be driven
//! by an untrusted browser posting a raw `multipart/form-data` body — so
//! these tests hand-craft the request with `reqwest`, following the pattern
//! `sigv4_verification.rs` uses for its own hand-signed requests.

mod helpers;

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use helpers::TestServer;

type HmacSha256 = Hmac<Sha256>;

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Re-derive the SigV4 signing key + sign an arbitrary string, matching
/// `fakecloud_aws::sigv4::sign_string` (the production implementation). The
/// test recomputes it independently rather than depending on `fakecloud-aws`
/// so it exercises the wire contract end-to-end rather than sharing code
/// with what it's testing.
fn sign_policy(
    secret: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    policy_b64: &str,
) -> String {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    hex::encode(hmac_sha256(&k_signing, policy_b64.as_bytes()))
}

struct PostPolicy {
    policy_b64: String,
    credential: String,
    date_stamp: String,
    amz_date: String,
    region: String,
}

fn build_policy(
    bucket: &str,
    key_prefix: &str,
    akid: &str,
    region: &str,
    max_bytes: u64,
) -> PostPolicy {
    let now = chrono::Utc::now();
    let date_stamp = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let expiration = (now + chrono::Duration::minutes(15)).to_rfc3339();
    let credential = format!("{akid}/{date_stamp}/{region}/s3/aws4_request");

    let policy = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": bucket},
            ["starts-with", "$key", key_prefix],
            ["content-length-range", 1, max_bytes],
            {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
            {"x-amz-credential": credential},
            {"x-amz-date": amz_date},
        ]
    });
    let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy.to_string());

    PostPolicy {
        policy_b64,
        credential,
        date_stamp,
        amz_date,
        region: region.to_string(),
    }
}

fn post_form(
    policy: &PostPolicy,
    key: &str,
    signature: &str,
    file_bytes: Vec<u8>,
) -> reqwest::multipart::Form {
    reqwest::multipart::Form::new()
        .text("key", key.to_string())
        .text("Content-Type", "text/plain")
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", policy.credential.clone())
        .text("x-amz-date", policy.amz_date.clone())
        .text("policy", policy.policy_b64.clone())
        .text("x-amz-signature", signature.to_string())
        .text("success_action_status", "201")
        .part(
            "file",
            reqwest::multipart::Part::bytes(file_bytes).file_name("upload.txt"),
        )
}

#[tokio::test]
async fn valid_post_policy_upload_stores_the_object() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("post-policy-bucket")
        .send()
        .await
        .expect("create bucket");

    // Root-bypass ("test"/"test") credentials: signature is not
    // cryptographically checked (matches `is_root_bypass`'s codebase-wide
    // convention), so any hex string works as long as the policy itself is
    // well-formed and its conditions are satisfied.
    let policy = build_policy(
        "post-policy-bucket",
        "uploads/",
        "test",
        "us-east-1",
        1_000_000,
    );
    let body = b"hello from post policy".to_vec();
    let form = post_form(
        &policy,
        "uploads/hello.txt",
        "0".repeat(64).as_str(),
        body.clone(),
    );

    let resp = reqwest::Client::new()
        .post(format!("{}/post-policy-bucket", server.endpoint()))
        .multipart(form)
        .send()
        .await
        .expect("POST Object request should succeed at the transport level");

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "expected 201 (success_action_status) POST response, got {status}: {text}"
    );
    assert!(
        text.contains("<Key>uploads/hello.txt</Key>"),
        "PostResponse body should echo the resolved key: {text}"
    );

    // Retrievable via a normal GET afterwards.
    let get = s3
        .get_object()
        .bucket("post-policy-bucket")
        .key("uploads/hello.txt")
        .send()
        .await
        .expect("uploaded object should be retrievable");
    let bytes = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(bytes.as_ref(), body.as_slice());
}

#[tokio::test]
async fn oversized_upload_violates_content_length_range_and_is_not_stored() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("post-policy-bucket-oversize")
        .send()
        .await
        .expect("create bucket");

    // content-length-range caps the upload at 10 bytes; send more than that.
    let policy = build_policy(
        "post-policy-bucket-oversize",
        "uploads/",
        "test",
        "us-east-1",
        10,
    );
    let body = b"this body is definitely longer than ten bytes".to_vec();
    let form = post_form(&policy, "uploads/big.txt", "0".repeat(64).as_str(), body);

    let resp = reqwest::Client::new()
        .post(format!("{}/post-policy-bucket-oversize", server.endpoint()))
        .multipart(form)
        .send()
        .await
        .expect("POST Object request should succeed at the transport level");

    assert!(
        resp.status().is_client_error(),
        "expected a 4xx rejection for a content-length-range violation, got {}",
        resp.status()
    );

    let err = s3
        .get_object()
        .bucket("post-policy-bucket-oversize")
        .key("uploads/big.txt")
        .send()
        .await
        .expect_err("object exceeding content-length-range must not be stored");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("NoSuchKey") || msg.contains("404"),
        "expected NoSuchKey, got {msg}"
    );
}

#[tokio::test]
async fn signature_mismatch_is_rejected() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("post-policy-bucket-sig")
        .send()
        .await
        .expect("create bucket");

    // Bootstrap a real IAM user (non-root access key) via root-bypass creds
    // so the server's credential_resolver has a real secret to check the
    // POST-Policy signature against.
    let iam = server.iam_client().await;
    iam.create_user()
        .user_name("post-policy-user")
        .send()
        .await
        .expect("create iam user");
    let ak = iam
        .create_access_key()
        .user_name("post-policy-user")
        .send()
        .await
        .expect("create access key");
    let key = ak.access_key().expect("access key present");
    let akid = key.access_key_id().to_string();
    // Deliberately unused: this test signs with the wrong secret below to
    // exercise the SignatureDoesNotMatch path.
    let _secret = key.secret_access_key().to_string();

    let policy = build_policy(
        "post-policy-bucket-sig",
        "uploads/",
        &akid,
        "us-east-1",
        1_000_000,
    );

    // Correct signature would be `sign_policy(&secret, ...)`; sign with the
    // wrong secret instead so the server's recomputed signature can never
    // match what we send.
    let bad_signature = sign_policy(
        "definitely-the-wrong-secret",
        &policy.date_stamp,
        &policy.region,
        "s3",
        &policy.policy_b64,
    );
    let body = b"should never be stored".to_vec();
    let form = post_form(&policy, "uploads/tampered.txt", &bad_signature, body);

    let resp = reqwest::Client::new()
        .post(format!("{}/post-policy-bucket-sig", server.endpoint()))
        .multipart(form)
        .send()
        .await
        .expect("POST Object request should succeed at the transport level");

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "expected 403 SignatureDoesNotMatch, got {status}: {text}"
    );
    assert!(
        text.contains("SignatureDoesNotMatch"),
        "expected SignatureDoesNotMatch in the error body: {text}"
    );

    let err = s3
        .get_object()
        .bucket("post-policy-bucket-sig")
        .key("uploads/tampered.txt")
        .send()
        .await
        .expect_err("object with a bad signature must not be stored");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("NoSuchKey") || msg.contains("404"),
        "expected NoSuchKey, got {msg}"
    );
}

#[tokio::test]
async fn correct_signature_from_a_real_iam_user_is_accepted() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("post-policy-bucket-iam")
        .send()
        .await
        .expect("create bucket");

    let iam = server.iam_client().await;
    iam.create_user()
        .user_name("post-policy-user-2")
        .send()
        .await
        .expect("create iam user");
    let ak = iam
        .create_access_key()
        .user_name("post-policy-user-2")
        .send()
        .await
        .expect("create access key");
    let key = ak.access_key().expect("access key present");
    let akid = key.access_key_id().to_string();
    let secret = key.secret_access_key().to_string();

    let policy = build_policy(
        "post-policy-bucket-iam",
        "uploads/",
        &akid,
        "us-east-1",
        1_000_000,
    );
    let signature = sign_policy(
        &secret,
        &policy.date_stamp,
        &policy.region,
        "s3",
        &policy.policy_b64,
    );
    let body = b"signed by a real iam user".to_vec();
    let form = post_form(&policy, "uploads/iam-signed.txt", &signature, body.clone());

    let resp = reqwest::Client::new()
        .post(format!("{}/post-policy-bucket-iam", server.endpoint()))
        .multipart(form)
        .send()
        .await
        .expect("POST Object request should succeed at the transport level");

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "expected 201 for a correctly-signed IAM-user upload, got {status}: {text}"
    );

    let get = s3
        .get_object()
        .bucket("post-policy-bucket-iam")
        .key("uploads/iam-signed.txt")
        .send()
        .await
        .expect("uploaded object should be retrievable");
    let bytes = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(bytes.as_ref(), body.as_slice());
}
