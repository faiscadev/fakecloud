//! AWS-managed KMS keys (`alias/aws/<service>`) work only through their
//! service, as on AWS (recordings in
//! `crates/fakecloud-kms/tests/fixtures/aws-managed-keys/`):
//! - a direct Encrypt / GenerateDataKey on the key is refused with 400
//!   AccessDeniedException naming the key the alias resolves to;
//! - GetKeyPolicy returns AWS's policy, whose cryptographic grant is
//!   conditioned on `kms:ViaService` and `kms:CallerAccount`, and refuses an
//!   alias with InvalidArnException;
//! - S3 SSE-KMS and Secrets Manager still encrypt and decrypt under their
//!   AWS-managed keys.

mod helpers;

use aws_sdk_kms::error::ProvideErrorMetadata;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use helpers::TestServer;

async fn key_arn(kms: &aws_sdk_kms::Client, alias: &str) -> String {
    kms.describe_key()
        .key_id(alias)
        .send()
        .await
        .unwrap_or_else(|e| panic!("DescribeKey {alias}: {e:?}"))
        .key_metadata
        .and_then(|m| m.arn)
        .expect("key ARN")
}

/// `code` / `message` are AWS's key-policy refusal of `action` on `arn`.
fn assert_denied(code: Option<&str>, message: Option<&str>, action: &str, arn: &str) {
    assert_eq!(code, Some("AccessDeniedException"), "{message:?}");
    let message = message.unwrap_or_default();
    assert!(
        message.starts_with("User: arn:aws:iam::123456789012:"),
        "{message}"
    );
    assert!(
        message.ends_with(&format!(
            " is not authorized to perform: {action} on resource: {arn} because no resource-based policy allows the {action} action"
        )),
        "{message}"
    );
}

#[tokio::test]
async fn direct_crypto_on_aws_managed_keys_is_refused() {
    let server = TestServer::start().await;
    let kms = server.kms_client().await;
    // The predefined `alias/aws/*` aliases are listed (and their keys exist)
    // once ListAliases is called, as Terraform's `aws_kms_alias` data source
    // does before reading one.
    kms.list_aliases().send().await.expect("ListAliases");

    let s3_key = key_arn(&kms, "alias/aws/s3").await;
    let err = kms
        .encrypt()
        .key_id("alias/aws/s3")
        .plaintext(Blob::new(b"hello".to_vec()))
        .send()
        .await
        .expect_err("direct Encrypt under alias/aws/s3");
    assert_eq!(
        err.raw_response().map(|r| r.status().as_u16()),
        Some(400),
        "{err:?}"
    );
    assert_denied(err.code(), err.message(), "kms:Encrypt", &s3_key);

    let sm_key = key_arn(&kms, "alias/aws/secretsmanager").await;
    let err = kms
        .generate_data_key()
        .key_id("alias/aws/secretsmanager")
        .key_spec(aws_sdk_kms::types::DataKeySpec::Aes256)
        .send()
        .await
        .expect_err("direct GenerateDataKey under alias/aws/secretsmanager");
    assert_denied(err.code(), err.message(), "kms:GenerateDataKey", &sm_key);

    // The key's policy is AWS's: crypto only through S3, for this account.
    let policy = kms
        .get_key_policy()
        .key_id(&s3_key)
        .policy_name("default")
        .send()
        .await
        .expect("GetKeyPolicy")
        .policy
        .expect("policy");
    let policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
    assert_eq!(policy["Id"], "auto-s3-2");
    let crypto = &policy["Statement"][0];
    assert_eq!(crypto["Principal"]["AWS"], "*");
    assert_eq!(
        crypto["Condition"]["StringEquals"]["kms:ViaService"],
        "s3.us-east-1.amazonaws.com"
    );
    assert_eq!(
        crypto["Condition"]["StringEquals"]["kms:CallerAccount"],
        "123456789012"
    );

    let err = kms
        .get_key_policy()
        .key_id("alias/aws/s3")
        .policy_name("default")
        .send()
        .await
        .expect_err("GetKeyPolicy with an alias");
    assert_eq!(err.code(), Some("InvalidArnException"), "{err:?}");
    assert_eq!(
        err.message(),
        Some("Key Aliases are not supported for this operation.")
    );

    // Key metadata stays readable.
    kms.describe_key()
        .key_id("alias/aws/s3")
        .send()
        .await
        .expect("DescribeKey");
}

#[tokio::test]
async fn services_still_use_their_aws_managed_keys() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let secrets = server.secretsmanager_client().await;
    let kms = server.kms_client().await;

    // S3 SSE-KMS with no key named uses aws/s3.
    s3.create_bucket().bucket("enc").send().await.unwrap();
    let put = s3
        .put_object()
        .bucket("enc")
        .key("k")
        .body(ByteStream::from_static(b"payload"))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .send()
        .await
        .expect("PutObject SSE-KMS under aws/s3");
    let s3_key = key_arn(&kms, "alias/aws/s3").await;
    assert_eq!(put.ssekms_key_id(), Some(s3_key.as_str()));
    let got = s3
        .get_object()
        .bucket("enc")
        .key("k")
        .send()
        .await
        .expect("GetObject");
    assert_eq!(got.ssekms_key_id(), Some(s3_key.as_str()));
    assert_eq!(
        &got.body.collect().await.unwrap().into_bytes()[..],
        b"payload"
    );

    // Secrets Manager under aws/secretsmanager.
    secrets
        .create_secret()
        .name("db")
        .secret_string("hunter2")
        .kms_key_id("alias/aws/secretsmanager")
        .send()
        .await
        .expect("CreateSecret under aws/secretsmanager");
    let value = secrets
        .get_secret_value()
        .secret_id("db")
        .send()
        .await
        .expect("GetSecretValue");
    assert_eq!(value.secret_string(), Some("hunter2"));
}
