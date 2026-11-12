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
            // Every field the browser posts must be authorized by a condition,
            // just as boto3's `generate_presigned_post` emits them.
            ["starts-with", "$Content-Type", ""],
            {"success_action_status": "201"},
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

/// A form field carrying no matching policy condition is rejected the way real
/// S3 rejects it: `AccessDenied` "Extra input fields". This is the whole point
/// of POST Policy -- the signed policy enumerates exactly what may be posted.
#[tokio::test]
async fn unauthorized_extra_form_field_is_rejected() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("post-policy-extra")
        .send()
        .await
        .expect("create bucket");

    let policy = build_policy(
        "post-policy-extra",
        "uploads/",
        "test",
        "us-east-1",
        1_000_000,
    );
    // `x-amz-meta-foo` is not covered by any condition in `build_policy`.
    let form = post_form(
        &policy,
        "uploads/extra.txt",
        "0".repeat(64).as_str(),
        b"body".to_vec(),
    )
    .text("x-amz-meta-foo", "bar");

    let resp = reqwest::Client::new()
        .post(format!("{}/post-policy-extra", server.endpoint()))
        .multipart(form)
        .send()
        .await
        .expect("transport ok");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "got {status}: {text}"
    );
    assert!(
        text.contains("Extra input fields"),
        "expected extra-field AccessDenied: {text}"
    );

    let err = s3
        .get_object()
        .bucket("post-policy-extra")
        .key("uploads/extra.txt")
        .send()
        .await
        .expect_err("object with an unauthorized field must not be stored");
    assert!(format!("{err:?}").contains("NoSuchKey") || format!("{err:?}").contains("404"));
}

/// A policy document without an `expiration` is rejected (real S3 requires it).
#[tokio::test]
async fn policy_without_expiration_is_rejected() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("post-policy-noexp")
        .send()
        .await
        .expect("create bucket");

    // Hand-build a policy with no `expiration` field; root-bypass creds so the
    // signature isn't checked and we exercise the policy-validation path.
    let policy_json = serde_json::json!({
        "conditions": [
            {"bucket": "post-policy-noexp"},
            ["starts-with", "$key", "uploads/"],
        ]
    });
    let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy_json.to_string());
    let form = reqwest::multipart::Form::new()
        .text("key", "uploads/x.txt")
        .text(
            "x-amz-credential",
            "test/20240101/us-east-1/s3/aws4_request",
        )
        .text("policy", policy_b64)
        .text("x-amz-signature", "0".repeat(64))
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"body".to_vec()).file_name("x.txt"),
        );

    let resp = reqwest::Client::new()
        .post(format!("{}/post-policy-noexp", server.endpoint()))
        .multipart(form)
        .send()
        .await
        .expect("transport ok");
    assert!(
        resp.status().is_client_error(),
        "expected 4xx for a policy missing expiration, got {}",
        resp.status()
    );
}

/// `success_action_redirect` yields a 303 to the given URL with `bucket`,
/// `key`, and `etag` appended, taking precedence over `success_action_status`.
#[tokio::test]
async fn success_action_redirect_returns_303() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("post-policy-redirect")
        .send()
        .await
        .expect("create bucket");

    let now = chrono::Utc::now();
    let expiration = (now + chrono::Duration::minutes(15)).to_rfc3339();
    let policy_json = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": "post-policy-redirect"},
            ["starts-with", "$key", "uploads/"],
            {"success_action_redirect": "https://example.com/done"},
        ]
    });
    let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy_json.to_string());
    let form = reqwest::multipart::Form::new()
        .text("key", "uploads/r.txt")
        .text(
            "x-amz-credential",
            "test/20240101/us-east-1/s3/aws4_request",
        )
        .text("policy", policy_b64)
        .text("x-amz-signature", "0".repeat(64))
        .text("success_action_redirect", "https://example.com/done")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"body".to_vec()).file_name("r.txt"),
        );

    // Don't follow the redirect -- assert the 303 + Location directly.
    let resp = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
        .post(format!("{}/post-policy-redirect", server.endpoint()))
        .multipart(form)
        .send()
        .await
        .expect("transport ok");
    assert_eq!(resp.status(), reqwest::StatusCode::SEE_OTHER);
    let location = resp
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        location.starts_with("https://example.com/done?"),
        "loc={location}"
    );
    assert!(
        location.contains("bucket=post-policy-redirect"),
        "loc={location}"
    );
    assert!(location.contains("key=uploads"), "loc={location}");

    // The object still got stored.
    s3.get_object()
        .bucket("post-policy-redirect")
        .key("uploads/r.txt")
        .send()
        .await
        .expect("redirect upload should still be stored");
}

#[tokio::test]
async fn post_object_fires_object_created_post_notification() {
    // A browser form upload is ObjectCreated:Post on real S3, not
    // ObjectCreated:Put -- rules filtered on s3:ObjectCreated:Post must fire.
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let sqs = server.sqs_client().await;

    s3.create_bucket()
        .bucket("post-notif-bucket")
        .send()
        .await
        .expect("create bucket");

    let queue = sqs
        .create_queue()
        .queue_name("post-events")
        .send()
        .await
        .expect("create queue");
    let queue_url = queue.queue_url().unwrap().to_string();
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .expect("queue attributes");
    let queue_arn = attrs
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap()
        .clone();
    let notif_config = format!(
        r#"{{"QueueConfigurations":[{{"QueueArn":"{queue_arn}","Events":["s3:ObjectCreated:Post"]}}]}}"#
    );
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            "post-notif-bucket",
            "--notification-configuration",
            &notif_config,
        ])
        .await;
    assert!(
        output.success(),
        "failed to set notification: {}",
        output.stderr_text()
    );

    let policy = build_policy(
        "post-notif-bucket",
        "uploads/",
        "test",
        "us-east-1",
        1_000_000,
    );
    let form = post_form(
        &policy,
        "uploads/form.txt",
        "0".repeat(64).as_str(),
        b"posted".to_vec(),
    );
    let resp = reqwest::Client::new()
        .post(format!("{}/post-notif-bucket", server.endpoint()))
        .multipart(form)
        .send()
        .await
        .expect("POST Object request should succeed at the transport level");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(2)
        .max_number_of_messages(1)
        .send()
        .await
        .expect("receive");
    let messages = msgs.messages();
    assert!(
        !messages.is_empty(),
        "POST Object should fire an s3:ObjectCreated:Post notification"
    );
    let event: serde_json::Value = serde_json::from_str(messages[0].body().unwrap()).unwrap();
    assert_eq!(event["Records"][0]["eventName"], "ObjectCreated:Post");
    assert_eq!(
        event["Records"][0]["s3"]["object"]["key"],
        "uploads/form.txt"
    );
}

/// A form signed by `akid`/`secret` for `key` in `bucket`, whose policy
/// names (exactly) each of `fields`, which the form also sets.
fn signed_form(
    bucket: &str,
    key: &str,
    akid: &str,
    secret: &str,
    fields: &[(&str, &str)],
) -> reqwest::multipart::Form {
    let now = chrono::Utc::now();
    let date_stamp = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let credential = format!("{akid}/{date_stamp}/us-east-1/s3/aws4_request");
    let mut conditions = vec![
        serde_json::json!({"bucket": bucket}),
        serde_json::json!({"key": key}),
        serde_json::json!({"x-amz-algorithm": "AWS4-HMAC-SHA256"}),
        serde_json::json!({"x-amz-credential": credential}),
        serde_json::json!({"x-amz-date": amz_date}),
    ];
    for (name, value) in fields {
        let mut condition = serde_json::Map::new();
        condition.insert(name.to_string(), serde_json::Value::from(*value));
        conditions.push(serde_json::Value::Object(condition));
    }
    let policy = serde_json::json!({
        "expiration": (now + chrono::Duration::minutes(15)).to_rfc3339(),
        "conditions": conditions,
    });
    let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy.to_string());
    let signature = sign_policy(secret, &date_stamp, "us-east-1", "s3", &policy_b64);
    let mut form = reqwest::multipart::Form::new()
        .text("key", key.to_string())
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("policy", policy_b64)
        .text("x-amz-signature", signature);
    for (name, value) in fields {
        form = form.text(name.to_string(), value.to_string());
    }
    form.part(
        "file",
        reqwest::multipart::Part::bytes(b"form body".to_vec()).file_name("f.txt"),
    )
}

/// A form upload is authorized as the PutObject its fields make it: the
/// fields feed the condition keys (`s3:x-amz-server-side-encryption`,
/// `s3:x-amz-acl`, `aws:RequestTag/*`), and the `acl` / `tagging` fields are
/// applied and need `s3:PutObjectAcl` / `s3:PutObjectTagging`.
#[tokio::test]
async fn post_object_fields_are_the_put_objects_headers_under_strict() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let s3 = helpers::root_s3_client(&server).await;
    s3.create_bucket().bucket("form-keys").send().await.unwrap();
    let iam = aws_sdk_iam::Client::new(&helpers::root_config(&server).await);
    let user = |name: &'static str, extra: &'static str| {
        let iam = iam.clone();
        async move {
            iam.create_user().user_name(name).send().await.unwrap();
            iam.put_user_policy()
                .user_name(name)
                .policy_name("p")
                .policy_document(format!(
                    r#"{{"Version":"2012-10-17","Statement":[
                        {{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::form-keys/*"}}{extra}]}}"#
                ))
                .send()
                .await
                .unwrap();
            let ak = iam
                .create_access_key()
                .user_name(name)
                .send()
                .await
                .unwrap();
            let ak = ak.access_key().unwrap();
            (
                ak.access_key_id().to_string(),
                ak.secret_access_key().to_string(),
            )
        }
    };
    let post = |form: reqwest::multipart::Form| {
        reqwest::Client::new()
            .post(format!("{}/form-keys", server.endpoint()))
            .multipart(form)
            .send()
    };

    // `DenyIncorrectEncryptionHeader`: a correct AES256 form is allowed, a
    // form naming no encryption is denied.
    let (a, s) = user(
        "aes",
        r#",{"Effect":"Deny","Action":"s3:PutObject","Resource":"*",
            "Condition":{"StringNotEquals":{"s3:x-amz-server-side-encryption":"AES256"}}}"#,
    )
    .await;
    let ok = post(signed_form(
        "form-keys",
        "aes.txt",
        &a,
        &s,
        &[("x-amz-server-side-encryption", "AES256")],
    ))
    .await
    .unwrap();
    assert!(ok.status().is_success(), "{:?}", ok.text().await);
    let denied = post(signed_form("form-keys", "plain.txt", &a, &s, &[]))
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);

    // A deny on aws:kms applies to a kms form.
    let (a, s) = user(
        "nokms",
        r#",{"Effect":"Deny","Action":"s3:PutObject","Resource":"*",
            "Condition":{"StringEquals":{"s3:x-amz-server-side-encryption":"aws:kms"}}}"#,
    )
    .await;
    let denied = post(signed_form(
        "form-keys",
        "kms.txt",
        &a,
        &s,
        &[("x-amz-server-side-encryption", "aws:kms")],
    ))
    .await
    .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
    assert!(s3
        .head_object()
        .bucket("form-keys")
        .key("kms.txt")
        .send()
        .await
        .is_err());

    // `acl` and `tagging` fields need their permissions, and are applied.
    let tagging =
        "<Tagging><TagSet><Tag><Key>team</Key><Value>a b</Value></Tag></TagSet></Tagging>";
    let (a, s) = user("putonly", "").await;
    for fields in [
        vec![("acl", "bucket-owner-full-control")],
        vec![("tagging", tagging)],
    ] {
        let denied = post(signed_form("form-keys", "extra.txt", &a, &s, &fields))
            .await
            .unwrap();
        assert_eq!(
            denied.status(),
            reqwest::StatusCode::FORBIDDEN,
            "{fields:?}"
        );
    }
    let (a, s) = user(
        "tagger",
        r#",{"Effect":"Allow","Action":["s3:PutObjectTagging","s3:PutObjectAcl"],"Resource":"arn:aws:s3:::form-keys/*",
            "Condition":{"StringEquals":{"aws:RequestTag/team":"a b"}}}"#,
    )
    .await;
    let ok = post(signed_form(
        "form-keys",
        "tagged.txt",
        &a,
        &s,
        &[("acl", "bucket-owner-full-control"), ("tagging", tagging)],
    ))
    .await
    .unwrap();
    assert!(ok.status().is_success(), "{:?}", ok.text().await);
    let tags = s3
        .get_object_tagging()
        .bucket("form-keys")
        .key("tagged.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tag_set().len(), 1);
    assert_eq!(tags.tag_set()[0].key(), "team");
    assert_eq!(tags.tag_set()[0].value(), "a b");
    // A field naming a copy source does not turn the upload into a copy
    // (whose request tags would be empty): the tags are still the form's,
    // and a Deny on them applies.
    let (e, es) = user(
        "noevil",
        r#",{"Effect":"Allow","Action":"s3:PutObjectTagging","Resource":"*"},
           {"Effect":"Deny","Action":["s3:PutObject","s3:PutObjectTagging"],"Resource":"*",
            "Condition":{"StringEquals":{"aws:RequestTag/team":"evil"}}}"#,
    )
    .await;
    let evil = "<Tagging><TagSet><Tag><Key>team</Key><Value>evil</Value></Tag></TagSet></Tagging>";
    for fields in [
        vec![("tagging", evil)],
        vec![
            ("tagging", evil),
            ("x-amz-copy-source", "form-keys/aes.txt"),
        ],
    ] {
        let denied = post(signed_form("form-keys", "evil.txt", &e, &es, &fields))
            .await
            .unwrap();
        assert_eq!(
            denied.status(),
            reqwest::StatusCode::FORBIDDEN,
            "{fields:?}"
        );
    }
    assert!(s3
        .head_object()
        .bucket("form-keys")
        .key("evil.txt")
        .send()
        .await
        .is_err());
    // A `tagging` field is a validated Tagging document.
    let dup = "<Tagging><TagSet><Tag><Key>k</Key><Value>1</Value></Tag>\
               <Tag><Key>k</Key><Value>2</Value></Tag></TagSet></Tagging>";
    for (tagging, code) in [("<Tagging><TagSet>", "MalformedXML"), (dup, "InvalidTag")] {
        let resp = post(signed_form(
            "form-keys",
            "bad-tags.txt",
            &e,
            &es,
            &[("tagging", tagging)],
        ))
        .await
        .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "{tagging}");
        let text = resp.text().await.unwrap();
        assert!(text.contains(&format!("<Code>{code}</Code>")), "{text}");
    }

    // A tag the condition does not allow is denied.
    let other = "<Tagging><TagSet><Tag><Key>team</Key><Value>z</Value></Tag></TagSet></Tagging>";
    let denied = post(signed_form(
        "form-keys",
        "other.txt",
        &a,
        &s,
        &[("tagging", other)],
    ))
    .await
    .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
}

/// Under `--iam strict` a browser-form upload is authorized as the PutObject
/// it is, on the key the form names, for the uploader whose key signed the
/// policy -- or, for a form with no policy, anonymously on the bucket policy.
#[tokio::test]
async fn post_object_is_authorized_as_put_object_under_strict() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let s3 = helpers::root_s3_client(&server).await;
    s3.create_bucket()
        .bucket("strict-post")
        .send()
        .await
        .expect("create bucket");

    let iam = aws_sdk_iam::Client::new(&helpers::root_config(&server).await);
    let mut keys = Vec::new();
    for (user, resource) in [
        ("uploader", "arn:aws:s3:::strict-post/uploads/*"),
        ("elsewhere", "arn:aws:s3:::strict-post/other/*"),
    ] {
        iam.create_user().user_name(user).send().await.unwrap();
        iam.put_user_policy()
            .user_name(user)
            .policy_name("put")
            .policy_document(format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":"s3:PutObject","Resource":"{resource}"}}]}}"#
            ))
            .send()
            .await
            .unwrap();
        let ak = iam
            .create_access_key()
            .user_name(user)
            .send()
            .await
            .unwrap();
        let ak = ak.access_key().unwrap();
        keys.push((
            ak.access_key_id().to_string(),
            ak.secret_access_key().to_string(),
        ));
    }
    let signed_post = |akid: &str, secret: &str, key: &str| {
        let policy = build_policy("strict-post", "uploads/", akid, "us-east-1", 1_000_000);
        let signature = sign_policy(
            secret,
            &policy.date_stamp,
            &policy.region,
            "s3",
            &policy.policy_b64,
        );
        let form = post_form(&policy, key, &signature, b"form body".to_vec());
        reqwest::Client::new()
            .post(format!("{}/strict-post", server.endpoint()))
            .multipart(form)
            .send()
    };
    let (akid, secret) = &keys[0];
    let ok = signed_post(akid, secret, "uploads/a.txt").await.unwrap();
    assert_eq!(ok.status(), reqwest::StatusCode::CREATED);
    let (akid, secret) = &keys[1];
    let denied = signed_post(akid, secret, "uploads/b.txt").await.unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
    assert!(s3
        .head_object()
        .bucket("strict-post")
        .key("uploads/b.txt")
        .send()
        .await
        .is_err());

    // Anonymous form uploads: only where the bucket policy lets anyone put.
    s3.delete_public_access_block()
        .bucket("strict-post")
        .send()
        .await
        .unwrap();
    s3.put_bucket_policy()
        .bucket("strict-post")
        .policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
                "Action":"s3:PutObject","Resource":"arn:aws:s3:::strict-post/public/*"}]}"#,
        )
        .send()
        .await
        .unwrap();
    let anonymous = |key: &str| {
        let form = reqwest::multipart::Form::new()
            .text("key", key.to_string())
            .part(
                "file",
                reqwest::multipart::Part::bytes(b"anon".to_vec()).file_name("a.txt"),
            );
        reqwest::Client::new()
            .post(format!("{}/strict-post", server.endpoint()))
            .multipart(form)
            .send()
    };
    let ok = anonymous("public/x.txt").await.unwrap();
    assert!(ok.status().is_success(), "{:?}", ok.text().await);
    let denied = anonymous("private/x.txt").await.unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
    assert!(s3
        .head_object()
        .bucket("strict-post")
        .key("private/x.txt")
        .send()
        .await
        .is_err());
}
