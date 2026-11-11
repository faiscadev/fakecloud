//! AWS-managed KMS keys (`alias/aws/<service>`) work only through their
//! service, as on AWS (recordings in
//! `crates/fakecloud-kms/tests/fixtures/aws-managed-keys/`):
//! - a direct Encrypt / GenerateDataKey on the key is refused with 400
//!   AccessDeniedException naming the key the alias resolves to;
//! - GetKeyPolicy returns AWS's policy, whose cryptographic grant is
//!   conditioned on `kms:ViaService` and `kms:CallerAccount`, and refuses an
//!   alias with InvalidArnException;
//! - S3 SSE-KMS and Secrets Manager still encrypt and decrypt under their
//!   AWS-managed keys;
//! - the predefined aliases are listed without a key until first use, and a
//!   name AWS has no AWS-managed key for is not found;
//! - each service reports a KMS refusal with its own error;
//! - service ciphertexts bind the service's encryption context;
//! - SES encrypts received mail client-side, as SES, under `aws/ses`.

mod helpers;

use aes_gcm::aead::{Aead, KeyInit};
use aws_sdk_kms::error::ProvideErrorMetadata;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use base64::Engine;
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

    // DescribeKey creates the AWS-managed key on first use.
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

/// Recorded (list-aliases-before.json, unmaterialized.txt): a fresh account
/// lists the predefined AWS-managed aliases with no target key; the first
/// use of one (here Encrypt, which is then refused) creates its key, as does
/// DescribeKey on an AWS-managed alias that is not even listed
/// (`alias/aws/sns`). A name AWS has no AWS-managed key for is not found.
#[tokio::test]
async fn aws_managed_aliases_exist_from_first_use() {
    let server = TestServer::start().await;
    let kms = server.kms_client().await;
    let listed = |aliases: &[aws_sdk_kms::types::AliasListEntry]| {
        aliases
            .iter()
            .map(|a| {
                (
                    a.alias_name().unwrap_or_default().to_string(),
                    a.target_key_id().map(str::to_string),
                )
            })
            .collect::<Vec<_>>()
    };
    let before = kms.list_aliases().send().await.unwrap();
    let before = listed(before.aliases());
    assert_eq!(
        before,
        [
            "dynamodb",
            "ebs",
            "elasticfilesystem",
            "es",
            "glue",
            "kinesisvideo",
            "rds",
            "redshift",
            "s3",
            "ssm",
            "xray"
        ]
        .map(|s| (format!("alias/aws/{s}"), None))
    );

    let err = kms
        .encrypt()
        .key_id("alias/aws/dynamodb")
        .plaintext(Blob::new(b"hello".to_vec()))
        .send()
        .await
        .expect_err("direct Encrypt under alias/aws/dynamodb");
    let message = err.message().unwrap_or_default().to_string();
    assert_eq!(err.code(), Some("AccessDeniedException"), "{message}");
    let sns = kms
        .describe_key()
        .key_id("alias/aws/sns")
        .send()
        .await
        .unwrap()
        .key_metadata
        .unwrap();
    assert_eq!(
        sns.description(),
        Some("Default key that protects my SNS data when no other key is defined")
    );
    let after = kms.list_aliases().send().await.unwrap();
    let after = listed(after.aliases());
    let target = |name: &str| {
        after
            .iter()
            .find(|(n, _)| n == name)
            .and_then(|(_, t)| t.clone())
    };
    let ddb = target("alias/aws/dynamodb").expect("aws/dynamodb created on first use");
    assert!(
        message.contains(&format!(
            "on resource: arn:aws:kms:us-east-1:123456789012:key/{ddb} "
        )),
        "{message}"
    );
    assert_eq!(target("alias/aws/sns").as_deref(), Some(sns.key_id()));
    assert_eq!(target("alias/aws/rds"), None);

    for name in ["alias/aws/logs", "alias/aws/nosuchservice"] {
        let err = kms
            .describe_key()
            .key_id(name)
            .send()
            .await
            .expect_err("no AWS-managed key");
        assert_eq!(err.code(), Some("NotFoundException"));
        assert_eq!(
            err.message(),
            Some(format!("Alias arn:aws:kms:us-east-1:123456789012:{name} is not found.").as_str())
        );
    }
}

/// The KMS message every service relays, recorded through SQS, SNS, SSM:
/// "<KMS message> (Service: AWSKMS; Status Code: 400; Error Code: <code>;
/// Request ID: <id>; Proxy: null)".
fn assert_relayed(message: &str, kms_message: &str, kms_code: &str) {
    let prefix = format!(
        "{kms_message} (Service: AWSKMS; Status Code: 400; Error Code: {kms_code}; Request ID: "
    );
    assert!(message.starts_with(&prefix), "{message}");
    assert!(message.ends_with("; Proxy: null)"), "{message}");
}

/// Each service reports a KMS refusal with the error its API declares
/// (recordings in services.txt / services-wire.txt): a queue, topic,
/// parameter, secret or table under another service's AWS-managed key is
/// refused by that key's policy, and a missing alias is not found.
#[tokio::test]
async fn services_report_kms_refusals_with_their_own_errors() {
    let server = TestServer::start().await;
    let cfg = server.aws_config().await;
    let kms = server.kms_client().await;
    let s3_key = key_arn(&kms, "alias/aws/s3").await;
    let sqs_key = key_arn(&kms, "alias/aws/sqs").await;
    let missing = "Alias arn:aws:kms:us-east-1:123456789012:alias/fc-does-not-exist is not found.";
    let denied = |action: &str, arn: &str| {
        format!("User: arn:aws:iam::123456789012:root is not authorized to perform: {action} on resource: {arn} because no resource-based policy allows the {action} action")
    };

    // SQS SendMessage: KMS.AccessDeniedException / KMS.NotFoundException.
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    for (queue, key, code, kms_message, kms_code) in [
        (
            "q-s3key",
            "alias/aws/s3",
            "KMS.AccessDeniedException",
            denied("kms:GenerateDataKey", &s3_key),
            "AccessDeniedException",
        ),
        (
            "q-nokey",
            "alias/fc-does-not-exist",
            "KMS.NotFoundException",
            missing.to_string(),
            "NotFoundException",
        ),
    ] {
        let url = sqs
            .create_queue()
            .queue_name(queue)
            .attributes(aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId, key)
            .send()
            .await
            .unwrap()
            .queue_url
            .unwrap();
        let err = sqs
            .send_message()
            .queue_url(&url)
            .message_body("hello")
            .send()
            .await
            .expect_err("SendMessage under an unusable key");
        assert_eq!(err.code(), Some(code), "{err:?}");
        assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(400));
        assert_relayed(err.message().unwrap_or_default(), &kms_message, kms_code);
    }

    // SNS Publish: KMSAccessDenied / KMSNotFound.
    let sns = aws_sdk_sns::Client::new(&cfg);
    for (topic, key, code, kms_message, kms_code) in [
        (
            "t-s3key",
            "alias/aws/s3",
            "KMSAccessDenied",
            denied("kms:GenerateDataKey", &s3_key),
            "AccessDeniedException",
        ),
        (
            "t-nokey",
            "alias/fc-does-not-exist",
            "KMSNotFound",
            missing.to_string(),
            "NotFoundException",
        ),
    ] {
        let arn = sns
            .create_topic()
            .name(topic)
            .attributes("KmsMasterKeyId", key)
            .send()
            .await
            .unwrap()
            .topic_arn
            .unwrap();
        let err = sns
            .publish()
            .topic_arn(&arn)
            .message("hello")
            .send()
            .await
            .expect_err("Publish under an unusable key");
        assert_eq!(err.code(), Some(code), "{err:?}");
        assert_relayed(err.message().unwrap_or_default(), &kms_message, kms_code);
    }

    // S3 PutObject: 403 AccessDenied / 400 KMS.NotFoundException, with KMS's
    // message as is.
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("kms-errors")
        .send()
        .await
        .unwrap();
    for (key, code, status, message) in [
        ("alias/aws/sqs", "AccessDenied", 403, denied("kms:GenerateDataKey", &sqs_key)),
        ("alias/fc-does-not-exist", "KMS.NotFoundException", 400, missing.to_string()),
        (
            "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000000",
            "KMS.NotFoundException",
            400,
            "Key 'arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000000' does not exist".to_string(),
        ),
    ] {
        let err = s3
            .put_object()
            .bucket("kms-errors")
            .key("k")
            .body(ByteStream::from_static(b"x"))
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(key)
            .send()
            .await
            .expect_err("PutObject under an unusable key");
        assert_eq!(err.code(), Some(code), "{key}: {err:?}");
        assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(status));
        assert_eq!(err.message(), Some(message.as_str()));
    }

    // SSM PutParameter: AccessDeniedException (kms:Encrypt) / InvalidKeyId.
    let ssm = aws_sdk_ssm::Client::new(&cfg);
    for (name, key, code, kms_message, kms_code) in [
        (
            "/p/a",
            "alias/aws/s3",
            "AccessDeniedException",
            denied("kms:Encrypt", &s3_key),
            "AccessDeniedException",
        ),
        (
            "/p/b",
            "alias/fc-does-not-exist",
            "InvalidKeyId",
            missing.to_string(),
            "NotFoundException",
        ),
    ] {
        let err = ssm
            .put_parameter()
            .name(name)
            .value("v")
            .r#type(aws_sdk_ssm::types::ParameterType::SecureString)
            .key_id(key)
            .send()
            .await
            .expect_err("PutParameter under an unusable key");
        assert_eq!(err.code(), Some(code), "{err:?}");
        assert_relayed(err.message().unwrap_or_default(), &kms_message, kms_code);
    }

    // Secrets Manager CreateSecret: AccessDeniedException "Access to KMS is
    // not allowed" / InvalidParameterException naming the KMS failure.
    let secrets = server.secretsmanager_client().await;
    let err = secrets
        .create_secret()
        .name("a")
        .secret_string("v")
        .kms_key_id("alias/aws/s3")
        .send()
        .await
        .expect_err("CreateSecret under aws/s3");
    assert_eq!(
        (err.code(), err.message()),
        (
            Some("AccessDeniedException"),
            Some("Access to KMS is not allowed")
        )
    );
    let err = secrets
        .create_secret()
        .name("b")
        .secret_string("v")
        .kms_key_id("alias/fc-does-not-exist")
        .send()
        .await
        .expect_err("CreateSecret under a missing alias");
    assert_eq!(err.code(), Some("InvalidParameterException"));
    let message = err.message().unwrap_or_default();
    let rest = message
        .strip_prefix("The operation failed because of an invalid KMS key: ")
        .unwrap_or_else(|| panic!("{message}"));
    assert_relayed(rest, missing, "NotFoundException");
    for name in ["a", "b"] {
        assert!(
            secrets
                .describe_secret()
                .secret_id(name)
                .send()
                .await
                .is_err(),
            "{name} was not created"
        );
    }

    // DynamoDB CreateTable checks the key: AccessDeniedException naming
    // kms:CreateGrant / ValidationException.
    let ddb = aws_sdk_dynamodb::Client::new(&cfg);
    for (table, key, code, what, exception, kms_message, kms_code) in [
        (
            "kms-t1",
            "alias/aws/s3",
            "AccessDeniedException",
            "KMS key access denied error",
            "AWSKMSException",
            denied("kms:CreateGrant", &s3_key),
            "AccessDeniedException",
        ),
        (
            "kms-t2",
            "alias/fc-does-not-exist",
            "ValidationException",
            "KMS validation error",
            "NotFoundException",
            missing.to_string(),
            "NotFoundException",
        ),
    ] {
        use aws_sdk_dynamodb::types::*;
        let err = ddb
            .create_table()
            .table_name(table)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("pk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::PayPerRequest)
            .sse_specification(
                SseSpecification::builder()
                    .enabled(true)
                    .sse_type(SseType::Kms)
                    .kms_master_key_id(key)
                    .build(),
            )
            .send()
            .await
            .expect_err("CreateTable under an unusable key");
        assert_eq!(err.code(), Some(code), "{err:?}");
        let message = err.message().unwrap_or_default();
        let rest = message
            .strip_prefix(&format!(
                "{what}: com.amazonaws.services.kms.model.{exception}: "
            ))
            .unwrap_or_else(|| panic!("{message}"));
        assert_relayed(rest, &kms_message, kms_code);
    }
    // The table's own key works.
    assert!(ddb
        .list_tables()
        .send()
        .await
        .unwrap()
        .table_names()
        .is_empty());
}

/// A service ciphertext binds the service's encryption context, as a KMS
/// ciphertext does: the body SQS stored for a message on a queue encrypted
/// with a customer key decrypts through the KMS API with the queue's
/// `aws:sqs:arn` context, and with no context it does not.
#[tokio::test]
async fn service_ciphertexts_bind_their_encryption_context() {
    let server = TestServer::start().await;
    let cfg = server.aws_config().await;
    let kms = server.kms_client().await;
    let key = kms.create_key().send().await.unwrap().key_metadata.unwrap();
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let url = sqs
        .create_queue()
        .queue_name("sealed")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId,
            key.key_id(),
        )
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    sqs.send_message()
        .queue_url(&url)
        .message_body("secret body")
        .send()
        .await
        .unwrap();
    let messages: serde_json::Value =
        reqwest::get(format!("{}/_fakecloud/sqs/messages", server.endpoint()))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let queue = messages["queues"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["queueName"] == "sealed")
        .unwrap();
    let message = &queue["messages"][0];
    assert_eq!(message["body"], "secret body");
    let stored = message["storedBody"].as_str().unwrap();
    let blob = base64::engine::general_purpose::STANDARD
        .decode(stored)
        .unwrap();
    let queue_arn = queue["queueArn"].as_str().unwrap();

    let out = kms
        .decrypt()
        .ciphertext_blob(Blob::new(blob.clone()))
        .encryption_context("aws:sqs:arn", queue_arn)
        .send()
        .await
        .expect("Decrypt with the queue's context");
    assert_eq!(out.plaintext().unwrap().as_ref(), b"secret body");
    assert_eq!(out.key_id(), key.arn());
    for wrong in [None, Some("arn:aws:sqs:us-east-1:123456789012:other")] {
        let mut call = kms.decrypt().ciphertext_blob(Blob::new(blob.clone()));
        if let Some(arn) = wrong {
            call = call.encryption_context("aws:sqs:arn", arn);
        }
        let err = call.send().await.expect_err("Decrypt with another context");
        assert_eq!(err.code(), Some("InvalidCiphertextException"), "{wrong:?}");
    }
    // The queue itself still reads its message.
    let got = sqs.receive_message().queue_url(&url).send().await.unwrap();
    assert_eq!(got.messages()[0].body(), Some("secret body"));
}

/// Recorded policy of `aws/ses`: SES encrypts received mail as its service
/// principal with its encryption context, and the account decrypts directly.
/// The S3 action stores the message client-side encrypted (S3 encryption
/// client envelope in the object's metadata), which the test opens the way a
/// reader does: KMS Decrypt of `x-amz-key-v2` with `x-amz-matdesc` as the
/// context, then AES-GCM. A customer key whose policy does not grant SES
/// stores nothing.
#[tokio::test]
async fn ses_s3_action_seals_mail_under_aws_ses() {
    use aws_sdk_ses::types::{ReceiptAction, ReceiptRule, S3Action};
    let server = TestServer::start().await;
    let ses = server.ses_client().await;
    let s3 = server.s3_client().await;
    let kms = server.kms_client().await;
    s3.create_bucket().bucket("mail").send().await.unwrap();
    let plain_key = kms.create_key().send().await.unwrap().key_metadata.unwrap();
    ses.create_receipt_rule_set()
        .rule_set_name("rs")
        .send()
        .await
        .unwrap();
    for (rule, prefix, key) in [
        (
            "managed",
            "managed/",
            "arn:aws:kms:us-east-1:123456789012:alias/aws/ses".to_string(),
        ),
        (
            "ungranted",
            "ungranted/",
            plain_key.arn().unwrap().to_string(),
        ),
    ] {
        ses.create_receipt_rule()
            .rule_set_name("rs")
            .rule(
                ReceiptRule::builder()
                    .name(rule)
                    .enabled(true)
                    .actions(
                        ReceiptAction::builder()
                            .s3_action(
                                S3Action::builder()
                                    .bucket_name("mail")
                                    .object_key_prefix(prefix)
                                    .kms_key_arn(key)
                                    .build()
                                    .unwrap(),
                            )
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
    }
    ses.set_active_receipt_rule_set()
        .rule_set_name("rs")
        .send()
        .await
        .unwrap();
    let resp = reqwest::Client::new()
        .post(format!("{}/_fakecloud/ses/inbound", server.endpoint()))
        .json(&serde_json::json!({
            "from": "alice@example.com",
            "to": ["bob@example.com"],
            "subject": "Hi",
            "body": "Hello under aws/ses",
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let list = |prefix: &'static str| {
        let s3 = s3.clone();
        async move {
            s3.list_objects_v2()
                .bucket("mail")
                .prefix(prefix)
                .send()
                .await
                .unwrap()
                .contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string))
                .collect::<Vec<_>>()
        }
    };
    assert!(
        list("ungranted/").await.is_empty(),
        "a key that does not grant SES stores nothing"
    );
    let keys = list("managed/").await;
    assert_eq!(keys.len(), 1);
    let got = s3
        .get_object()
        .bucket("mail")
        .key(&keys[0])
        .send()
        .await
        .unwrap();
    // SES encrypts client-side; the bucket's default encryption (SSE-S3 for
    // every new bucket) applies to the stored object as to any other.
    assert_eq!(
        got.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "SES encrypts client-side, not with SSE-KMS"
    );
    let meta = got.metadata().cloned().unwrap_or_default();
    assert_eq!(
        meta.get("x-amz-cek-alg").map(String::as_str),
        Some("AES/GCM/NoPadding")
    );
    assert_eq!(meta.get("x-amz-wrap-alg").map(String::as_str), Some("kms"));
    let matdesc: std::collections::HashMap<String, String> =
        serde_json::from_str(&meta["x-amz-matdesc"]).unwrap();
    assert_eq!(matdesc["aws:ses:source-account"], "123456789012");
    assert_eq!(matdesc["aws:ses:rule-name"], "managed");
    let body = got.body.collect().await.unwrap().into_bytes();
    assert!(!String::from_utf8_lossy(&body).contains("Hello under aws/ses"));

    let b64 = base64::engine::general_purpose::STANDARD;
    let mut decrypt = kms
        .decrypt()
        .ciphertext_blob(Blob::new(b64.decode(&meta["x-amz-key-v2"]).unwrap()));
    for (k, v) in &matdesc {
        decrypt = decrypt.encryption_context(k, v);
    }
    let data_key = decrypt
        .send()
        .await
        .expect("the account decrypts SES's data key");
    assert_eq!(
        data_key.key_id(),
        Some(key_arn(&kms, "alias/aws/ses").await.as_str())
    );
    let cipher =
        aes_gcm::Aes256Gcm::new_from_slice(data_key.plaintext().unwrap().as_ref()).unwrap();
    let iv = b64.decode(&meta["x-amz-iv"]).unwrap();
    let mail = cipher
        .decrypt(aes_gcm::Nonce::from_slice(&iv), body.as_ref())
        .unwrap();
    assert!(String::from_utf8_lossy(&mail).contains("Hello under aws/ses"));
}
