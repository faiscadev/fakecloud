//! End-to-end tests for binding a signed request body to its SigV4 signature
//! for NON-S3 services under `--verify-sigv4`.
//!
//! The sigv4 canonical-request builder uses the client-supplied
//! `x-amz-content-sha256` header value verbatim (it is a signed header) and
//! deliberately does NOT re-hash the body there — streaming / aws-chunked
//! routes leave the buffered body empty at that layer, so re-hashing would
//! reject legitimate signed requests. S3 re-binds the hash in its own write
//! path; every other service is bound at the dispatch layer, where the full
//! buffered body is known. These tests drive hand-crafted signed requests to
//! prove:
//!   * a correctly signed request whose header hash matches the body passes,
//!   * an on-path body tamper (signed headers kept, body altered) is rejected
//!     with `SignatureDoesNotMatch`,
//!   * an `UNSIGNED-PAYLOAD` request is unaffected by the body-hash check.

mod helpers;

use aws_credential_types::Credentials;
use helpers::TestServer;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

async fn start_verified() -> TestServer {
    TestServer::start_with_env(&[("FAKECLOUD_VERIFY_SIGV4", "true")]).await
}

/// Bootstrap a real IAM user + access key using root-bypass `test` creds
/// (which always pass verification). Returns a resolvable (akid, secret) pair
/// so hand-signed requests exercise the real verify path.
async fn bootstrap_access_key(server: &TestServer, user: &str) -> (String, String) {
    let boot = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "root-bypass"))
        .load()
        .await;
    let iam = aws_sdk_iam::Client::new(&boot);
    iam.create_user().user_name(user).send().await.unwrap();
    let ak = iam
        .create_access_key()
        .user_name(user)
        .send()
        .await
        .unwrap();
    let key = ak.access_key().unwrap();
    (
        key.access_key_id().to_string(),
        key.secret_access_key().to_string(),
    )
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Produce a SigV4 `Authorization` header value for a header-signed POST.
/// `payload_hash_header` is the literal value placed in (and signed as) the
/// `x-amz-content-sha256` header — a real 64-char hex digest, or a marker like
/// `UNSIGNED-PAYLOAD`. The signature covers exactly that value, mirroring what
/// a real client (or an on-path attacker who keeps the signed headers) sends.
#[allow(clippy::too_many_arguments)]
fn authorization_header(
    host: &str,
    payload_hash_header: &str,
    akid: &str,
    secret: &str,
    region: &str,
    service: &str,
    amz_date: &str,
) -> String {
    let date_stamp = &amz_date[..8];
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash_header}\nx-amz-date:{amz_date}\n");
    // POST "/" with an empty canonical query string.
    let canonical_request =
        format!("POST\n/\n\n{canonical_headers}\n{signed_headers}\n{payload_hash_header}");
    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={akid}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )
}

fn host_of(endpoint: &str) -> String {
    endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint)
        .trim_end_matches('/')
        .to_string()
}

const STS_BODY: &[u8] = b"Action=GetCallerIdentity&Version=2011-06-15";

/// A correctly signed STS request whose `x-amz-content-sha256` matches the body
/// verifies and reaches the handler.
#[tokio::test]
async fn correctly_signed_body_is_accepted() {
    let server = start_verified().await;
    let (akid, secret) = bootstrap_access_key(&server, "signer-ok").await;
    let host = host_of(server.endpoint());
    let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let body_hash = sha256_hex(STS_BODY);
    let auth = authorization_header(
        &host,
        &body_hash,
        &akid,
        &secret,
        "us-east-1",
        "sts",
        &amz_date,
    );

    let resp = reqwest::Client::new()
        .post(server.endpoint())
        .header("host", &host)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &body_hash)
        .header("authorization", &auth)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(STS_BODY.to_vec())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "correctly signed request must pass: {text}");
    assert!(
        text.contains("GetCallerIdentityResponse"),
        "expected a GetCallerIdentity response, got {text}"
    );
}

/// An on-path body tamper — the signed headers (including the original
/// `x-amz-content-sha256`) are kept, but the body is replaced — must be
/// rejected. The signature still matches (it covers the header value verbatim),
/// so this is caught by the body-hash re-binding, returning
/// `SignatureDoesNotMatch`.
#[tokio::test]
async fn tampered_body_is_rejected() {
    let server = start_verified().await;
    let (akid, secret) = bootstrap_access_key(&server, "signer-tamper").await;
    let host = host_of(server.endpoint());
    let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();

    // Sign the ORIGINAL body's hash...
    let original_hash = sha256_hex(STS_BODY);
    let auth = authorization_header(
        &host,
        &original_hash,
        &akid,
        &secret,
        "us-east-1",
        "sts",
        &amz_date,
    );

    // ...but send a DIFFERENT body while keeping every signed header.
    let tampered = b"Action=GetCallerIdentity&Version=2011-06-15&injected=1".to_vec();
    let resp = reqwest::Client::new()
        .post(server.endpoint())
        .header("host", &host)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &original_hash)
        .header("authorization", &auth)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(tampered)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 403, "tampered body must be rejected: {text}");
    assert!(
        text.contains("SignatureDoesNotMatch"),
        "expected SignatureDoesNotMatch, got {text}"
    );
}

/// An `UNSIGNED-PAYLOAD` request carries no body hash to bind against, so the
/// body-hash check is skipped entirely and the request is unaffected: a valid
/// signature over `UNSIGNED-PAYLOAD` still reaches the handler.
#[tokio::test]
async fn unsigned_payload_marker_is_unaffected() {
    let server = start_verified().await;
    let (akid, secret) = bootstrap_access_key(&server, "signer-unsigned").await;
    let host = host_of(server.endpoint());
    let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let auth = authorization_header(
        &host,
        "UNSIGNED-PAYLOAD",
        &akid,
        &secret,
        "us-east-1",
        "sts",
        &amz_date,
    );

    let resp = reqwest::Client::new()
        .post(server.endpoint())
        .header("host", &host)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
        .header("authorization", &auth)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(STS_BODY.to_vec())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(
        status, 200,
        "UNSIGNED-PAYLOAD request must be unaffected by the body-hash check: {text}"
    );
    assert!(text.contains("GetCallerIdentityResponse"), "got {text}");
}
