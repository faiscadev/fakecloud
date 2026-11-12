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

/// Sum of an `AWS/<service>` metric's data points with `dimension = value`.
async fn metric_sum(
    cw: &aws_sdk_cloudwatch::Client,
    namespace: &str,
    metric: &str,
    dimension: &str,
    value: &str,
) -> f64 {
    let now = std::time::SystemTime::now();
    let out = cw
        .get_metric_statistics()
        .namespace(namespace)
        .metric_name(metric)
        .dimensions(
            aws_sdk_cloudwatch::types::Dimension::builder()
                .name(dimension)
                .value(value)
                .build(),
        )
        .start_time(aws_sdk_cloudwatch::primitives::DateTime::from(
            now - std::time::Duration::from_secs(3600),
        ))
        .end_time(aws_sdk_cloudwatch::primitives::DateTime::from(
            now + std::time::Duration::from_secs(60),
        ))
        .period(60)
        .statistics(aws_sdk_cloudwatch::types::Statistic::Sum)
        .send()
        .await
        .unwrap();
    out.datapoints().iter().filter_map(|d| d.sum()).sum()
}

async fn queue_bodies(sqs: &aws_sdk_sqs::Client, url: &str) -> Vec<String> {
    let mut bodies = Vec::new();
    for _ in 0..3 {
        let got = sqs
            .receive_message()
            .queue_url(url)
            .max_number_of_messages(10)
            .send()
            .await
            .unwrap();
        bodies.extend(
            got.messages()
                .iter()
                .filter_map(|m| m.body().map(str::to_string)),
        );
    }
    bodies
}

/// Recorded (service-principal-deliveries.txt): SNS, EventBridge and S3
/// deliver to SQS as their service principal, which the AWS-managed
/// `aws/sqs` key's policy does not admit. SNS delivers nothing and counts
/// `NumberOfNotificationsFailed`, EventBridge delivers nothing and counts
/// `FailedInvocations`, S3 refuses the notification configuration. A
/// customer key whose policy grants those principals keeps working, and the
/// account itself still sends to the `aws/sqs` queue.
#[tokio::test]
async fn service_principal_deliveries_need_a_key_that_admits_them() {
    let server = TestServer::start().await;
    let cfg = server.aws_config().await;
    let kms = server.kms_client().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let sns = aws_sdk_sns::Client::new(&cfg);
    let events = aws_sdk_eventbridge::Client::new(&cfg);
    let s3 = server.s3_client().await;
    let cw = aws_sdk_cloudwatch::Client::new(&cfg);

    let granting = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::123456789012:root"}, "Action": "kms:*", "Resource": "*"},
            {"Effect": "Allow",
             "Principal": {"Service": ["sns.amazonaws.com", "events.amazonaws.com", "s3.amazonaws.com"]},
             "Action": ["kms:GenerateDataKey", "kms:Decrypt"], "Resource": "*"}
        ]
    });
    let key = kms
        .create_key()
        .policy(granting.to_string())
        .send()
        .await
        .unwrap()
        .key_metadata
        .unwrap();
    let mut queues = Vec::new();
    for (name, key_id) in [("managed", "alias/aws/sqs"), ("granting", key.key_id())] {
        let url = sqs
            .create_queue()
            .queue_name(name)
            .attributes(
                aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId,
                key_id,
            )
            .send()
            .await
            .unwrap()
            .queue_url
            .unwrap();
        queues.push((url, format!("arn:aws:sqs:us-east-1:123456789012:{name}")));
    }
    let (managed_url, managed_arn) = queues[0].clone();
    let (granting_url, granting_arn) = queues[1].clone();

    // SNS.
    let topic = sns
        .create_topic()
        .name("t")
        .send()
        .await
        .unwrap()
        .topic_arn
        .unwrap();
    for arn in [&managed_arn, &granting_arn] {
        sns.subscribe()
            .topic_arn(&topic)
            .protocol("sqs")
            .endpoint(arn)
            .attributes("RawMessageDelivery", "true")
            .send()
            .await
            .unwrap();
    }
    sns.publish()
        .topic_arn(&topic)
        .message("from-sns")
        .send()
        .await
        .unwrap();

    // EventBridge.
    events
        .put_rule()
        .name("r")
        .event_pattern(r#"{"source":["fc.test"]}"#)
        .send()
        .await
        .unwrap();
    events
        .put_targets()
        .rule("r")
        .targets(
            aws_sdk_eventbridge::types::Target::builder()
                .id("m")
                .arn(&managed_arn)
                .build()
                .unwrap(),
        )
        .targets(
            aws_sdk_eventbridge::types::Target::builder()
                .id("g")
                .arn(&granting_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    events
        .put_events()
        .entries(
            aws_sdk_eventbridge::types::PutEventsRequestEntry::builder()
                .source("fc.test")
                .detail_type("t")
                .detail("{}")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // S3: the aws/sqs queue fails validation, the granting one is accepted.
    s3.create_bucket().bucket("notify").send().await.unwrap();
    let config = |arn: &str| {
        aws_sdk_s3::types::NotificationConfiguration::builder()
            .queue_configurations(
                aws_sdk_s3::types::QueueConfiguration::builder()
                    .queue_arn(arn)
                    .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                    .build()
                    .unwrap(),
            )
            .build()
    };
    let err = s3
        .put_bucket_notification_configuration()
        .bucket("notify")
        .notification_configuration(config(&managed_arn))
        .send()
        .await
        .expect_err("aws/sqs queue refused as a destination");
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    assert_eq!(
        err.message(),
        Some("Unable to validate the following destination configurations")
    );
    let raw =
        String::from_utf8_lossy(err.raw_response().unwrap().body().bytes().unwrap()).to_string();
    assert!(
        raw.contains(&format!("<ArgumentName1>{managed_arn}</ArgumentName1>")),
        "{raw}"
    );
    assert!(
        raw.contains("<ArgumentValue1>The message to the SSE queue could not be encrypted using KMS</ArgumentValue1>"),
        "{raw}"
    );
    s3.put_bucket_notification_configuration()
        .bucket("notify")
        .notification_configuration(config(&granting_arn))
        .send()
        .await
        .expect("a key that grants S3 validates");
    s3.put_object()
        .bucket("notify")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(queue_bodies(&sqs, &managed_url).await.is_empty());
    let granted = queue_bodies(&sqs, &granting_url).await;
    assert!(granted.iter().any(|b| b == "from-sns"), "{granted:?}");
    assert!(
        granted.iter().any(|b| b.contains("\"source\":\"fc.test\"")),
        "{granted:?}"
    );
    assert!(
        granted.iter().any(|b| b.contains("ObjectCreated:Put")),
        "{granted:?}"
    );

    assert_eq!(
        metric_sum(
            &cw,
            "AWS/SNS",
            "NumberOfNotificationsFailed",
            "TopicName",
            "t"
        )
        .await,
        1.0
    );
    assert_eq!(
        metric_sum(&cw, "AWS/Events", "FailedInvocations", "RuleName", "r").await,
        1.0
    );

    // The account itself uses the aws/sqs queue through SQS.
    sqs.send_message()
        .queue_url(&managed_url)
        .message_body("direct")
        .send()
        .await
        .unwrap();
    assert_eq!(queue_bodies(&sqs, &managed_url).await, ["direct"]);
}

/// A service using a key on a caller's behalf names that caller in KMS's
/// refusal: an IAM user's SendMessage to a queue under another service's
/// AWS-managed key is refused for `user/<name>`, not the account root.
#[tokio::test]
async fn kms_refusals_name_the_calling_principal() {
    let server = TestServer::start().await;
    let iam = aws_sdk_iam::Client::new(&server.aws_config().await);
    iam.create_user().user_name("alice").send().await.unwrap();
    let key = iam
        .create_access_key()
        .user_name("alice")
        .send()
        .await
        .unwrap()
        .access_key
        .unwrap();
    let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            key.access_key_id(),
            key.secret_access_key(),
            None,
            None,
            "alice",
        ))
        .load()
        .await;
    let kms = aws_sdk_kms::Client::new(&cfg);
    let s3_key = key_arn(&kms, "alias/aws/s3").await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let url = sqs
        .create_queue()
        .queue_name("under-s3-key")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId,
            "alias/aws/s3",
        )
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    let err = sqs
        .send_message()
        .queue_url(&url)
        .message_body("x")
        .send()
        .await
        .expect_err("refused by aws/s3's policy");
    assert_relayed(
        err.message().unwrap_or_default(),
        &format!("User: arn:aws:iam::123456789012:user/alice is not authorized to perform: kms:GenerateDataKey on resource: {s3_key} because no resource-based policy allows the kms:GenerateDataKey action"),
        "AccessDeniedException",
    );
}

/// Secrets Manager encrypts every secret: one created without `KmsKeyId`
/// under the region's AWS-managed `aws/secretsmanager` key (created on that
/// first use), and `GetSecretValue` decrypts it through Secrets Manager.
#[tokio::test]
async fn secrets_without_a_key_are_sealed_under_aws_secretsmanager() {
    let server = TestServer::start().await;
    let secrets = server.secretsmanager_client().await;
    let kms = server.kms_client().await;
    let created = secrets
        .create_secret()
        .name("plain")
        .secret_string("hunter2")
        .send()
        .await
        .unwrap();
    let described = secrets
        .describe_secret()
        .secret_id("plain")
        .send()
        .await
        .unwrap();
    assert_eq!(
        described.kms_key_id(),
        None,
        "the default key is not reported"
    );
    let got = secrets
        .get_secret_value()
        .secret_id("plain")
        .send()
        .await
        .unwrap();
    assert_eq!(got.secret_string(), Some("hunter2"));

    let managed = key_arn(&kms, "alias/aws/secretsmanager").await;
    let usage: serde_json::Value =
        reqwest::get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let records = usage["records"].as_array().unwrap();
    for op in ["GenerateDataKey", "Decrypt"] {
        assert!(
            records.iter().any(|r| r["operation"] == op
                && r["keyArn"] == managed.as_str()
                && r["servicePrincipal"] == "secretsmanager.amazonaws.com"
                && r["encryptionContext"]["SecretARN"] == created.arn().unwrap()),
            "{op}: {records:?}"
        );
    }
}

/// A customer managed key the account can use, for tests that then disable it.
async fn account_key(kms: &aws_sdk_kms::Client) -> String {
    kms.create_key()
        .send()
        .await
        .unwrap()
        .key_metadata
        .unwrap()
        .key_id()
        .to_string()
}

#[tokio::test]
async fn secret_ciphertexts_that_do_not_open_fail_instead_of_leaking() {
    let server = TestServer::start().await;
    let secrets = server.secretsmanager_client().await;
    let kms = server.kms_client().await;
    let key = account_key(&kms).await;
    secrets
        .create_secret()
        .name("s")
        .kms_key_id(&key)
        .secret_string("hunter2")
        .send()
        .await
        .unwrap();
    secrets
        .create_secret()
        .name("b")
        .kms_key_id(&key)
        .secret_binary(aws_sdk_secretsmanager::primitives::Blob::new(vec![
            0u8, 1, 2, 255,
        ]))
        .send()
        .await
        .unwrap();
    secrets
        .create_secret()
        .name("ok")
        .secret_string("fine")
        .send()
        .await
        .unwrap();
    kms.disable_key().key_id(&key).send().await.unwrap();

    // Reads fail with Secrets Manager's error, never return the ciphertext.
    for name in ["s", "b"] {
        let err = secrets
            .get_secret_value()
            .secret_id(name)
            .send()
            .await
            .expect_err("a disabled key must fail the read");
        assert_eq!(
            aws_sdk_secretsmanager::error::ProvideErrorMetadata::code(&err),
            Some("DecryptionFailure"),
            "{err:?}"
        );
        // Recorded in fakecloud-codebuild/tests/fixtures/env-var-resolution-direct-reads.txt.
        let message = aws_sdk_secretsmanager::error::ProvideErrorMetadata::message(&err).unwrap();
        assert!(
            message.starts_with(&format!(
                "Secrets Manager can't decrypt the secret value: arn:aws:kms:us-east-1:123456789012:key/{key} is disabled. (Service: AWSKMS; Status Code: 400; Error Code: DisabledException; Request ID: "
            )),
            "{message}"
        );
    }

    // One secret KMS will not decrypt is one of the batch's errors.
    let batch = secrets
        .batch_get_secret_value()
        .secret_id_list("ok")
        .secret_id_list("s")
        .send()
        .await
        .unwrap();
    assert_eq!(batch.secret_values().len(), 1);
    assert_eq!(batch.secret_values()[0].secret_string(), Some("fine"));
    assert_eq!(batch.errors().len(), 1);
    assert_eq!(batch.errors()[0].secret_id(), Some("s"));
    assert_eq!(batch.errors()[0].error_code(), Some("DecryptionFailure"));

    // Rotation cannot re-seal the value: it fails and leaves the secret,
    // its rotation configuration included.
    let before = secrets
        .list_secret_version_ids()
        .secret_id("s")
        .send()
        .await
        .unwrap();
    let described_before = secrets
        .describe_secret()
        .secret_id("s")
        .send()
        .await
        .unwrap();
    let err = secrets
        .rotate_secret()
        .secret_id("s")
        .rotation_rules(
            aws_sdk_secretsmanager::types::RotationRulesType::builder()
                .automatically_after_days(7)
                .build(),
        )
        .send()
        .await
        .expect_err("rotation under a disabled key must fail");
    assert_eq!(
        aws_sdk_secretsmanager::error::ProvideErrorMetadata::code(&err),
        Some("DecryptionFailure"),
        "{err:?}"
    );
    let after = secrets
        .list_secret_version_ids()
        .secret_id("s")
        .send()
        .await
        .unwrap();
    assert_eq!(before.versions().len(), after.versions().len());
    let described_after = secrets
        .describe_secret()
        .secret_id("s")
        .send()
        .await
        .unwrap();
    assert_eq!(described_after.rotation_enabled(), None);
    assert_eq!(described_after.rotation_rules(), None);
    assert_eq!(
        described_after.rotation_enabled(),
        described_before.rotation_enabled()
    );
    assert_eq!(
        described_after.last_changed_date(),
        described_before.last_changed_date()
    );

    // A replica KMS cannot seal for is Failed, holding nothing.
    let replicated = secrets
        .replicate_secret_to_regions()
        .secret_id("s")
        .add_replica_regions(
            aws_sdk_secretsmanager::types::ReplicaRegionType::builder()
                .region("us-west-2")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let status = &replicated.replication_status()[0];
    assert_eq!(
        status.status(),
        Some(&aws_sdk_secretsmanager::types::StatusType::Failed),
        "{status:?}"
    );
    let west = aws_sdk_secretsmanager::Client::new(&server.aws_config_in("us-west-2").await);
    assert!(west.get_secret_value().secret_id("s").send().await.is_err());

    // Once the key is back, both values read as stored.
    kms.enable_key().key_id(&key).send().await.unwrap();
    let got = secrets
        .get_secret_value()
        .secret_id("s")
        .send()
        .await
        .unwrap();
    assert_eq!(got.secret_string(), Some("hunter2"));
    let got = secrets
        .get_secret_value()
        .secret_id("b")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.secret_binary().map(|b| b.as_ref().to_vec()),
        Some(vec![0u8, 1, 2, 255])
    );
}

#[tokio::test]
async fn secret_binary_is_sealed_under_the_secret_key() {
    let server = TestServer::start().await;
    let secrets = server.secretsmanager_client().await;
    let created = secrets
        .create_secret()
        .name("bin")
        .secret_binary(aws_sdk_secretsmanager::primitives::Blob::new(
            b"raw".to_vec(),
        ))
        .send()
        .await
        .unwrap();
    let usage: serde_json::Value =
        reqwest::get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert!(
        usage["records"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["operation"] == "GenerateDataKey"
                && r["encryptionContext"]["SecretARN"] == created.arn().unwrap()),
        "{usage}"
    );
    let versions = secrets
        .list_secret_version_ids()
        .secret_id("bin")
        .send()
        .await
        .unwrap();
    assert_eq!(
        versions.versions()[0].kms_key_ids(),
        ["DefaultEncryptionKey".to_string()]
    );
}

#[tokio::test]
async fn update_secret_kms_key_validates_and_re_encrypts() {
    // Recorded in fixtures/aws-managed-keys/secrets-eventbridge-dlq-sse-sqs.txt.
    let server = TestServer::start().await;
    let secrets = server.secretsmanager_client().await;
    let kms = server.kms_client().await;
    secrets
        .create_secret()
        .name("s")
        .secret_string("v1")
        .send()
        .await
        .unwrap();

    let err = secrets
        .update_secret()
        .secret_id("s")
        .kms_key_id("alias/aws/s3")
        .send()
        .await
        .expect_err("another service's AWS-managed key");
    assert_eq!(
        aws_sdk_secretsmanager::error::ProvideErrorMetadata::code(&err),
        Some("AccessDeniedException")
    );
    assert_eq!(
        aws_sdk_secretsmanager::error::ProvideErrorMetadata::message(&err),
        Some("Access to KMS is not allowed")
    );
    let err = secrets
        .update_secret()
        .secret_id("s")
        .kms_key_id("alias/fc-does-not-exist")
        .send()
        .await
        .expect_err("a missing key");
    assert_eq!(
        aws_sdk_secretsmanager::error::ProvideErrorMetadata::code(&err),
        Some("InvalidParameterException")
    );
    assert!(aws_sdk_secretsmanager::error::ProvideErrorMetadata::message(&err)
        .unwrap()
        .starts_with("The operation failed because of an invalid KMS key: Alias arn:aws:kms:us-east-1:123456789012:alias/fc-does-not-exist is not found. (Service: AWSKMS; Status Code: 400; Error Code: NotFoundException;"));
    let described = secrets
        .describe_secret()
        .secret_id("s")
        .send()
        .await
        .unwrap();
    assert_eq!(
        described.kms_key_id(),
        None,
        "a refused update changes nothing"
    );

    let updated = secrets
        .update_secret()
        .secret_id("s")
        .kms_key_id("alias/aws/secretsmanager")
        .send()
        .await
        .unwrap();
    assert_eq!(updated.version_id(), None);
    let described = secrets
        .describe_secret()
        .secret_id("s")
        .send()
        .await
        .unwrap();
    assert_eq!(described.kms_key_id(), Some("alias/aws/secretsmanager"));
    let versions = secrets
        .list_secret_version_ids()
        .secret_id("s")
        .send()
        .await
        .unwrap();
    assert_eq!(versions.versions().len(), 1);
    assert_eq!(
        versions.versions()[0].kms_key_ids(),
        [
            "DefaultEncryptionKey".to_string(),
            "alias/aws/secretsmanager".to_string()
        ]
    );

    // The value is now sealed under the new key: disabling it fails reads.
    let key = account_key(&kms).await;
    secrets
        .update_secret()
        .secret_id("s")
        .kms_key_id(&key)
        .send()
        .await
        .unwrap();
    let got = secrets
        .get_secret_value()
        .secret_id("s")
        .send()
        .await
        .unwrap();
    assert_eq!(got.secret_string(), Some("v1"));
    kms.disable_key().key_id(&key).send().await.unwrap();
    assert!(secrets
        .get_secret_value()
        .secret_id("s")
        .send()
        .await
        .is_err());
}

#[tokio::test]
async fn encrypted_messages_stay_readable_in_a_plain_dead_letter_queue() {
    let server = TestServer::start().await;
    let sqs = aws_sdk_sqs::Client::new(&server.aws_config().await);
    let kms = server.kms_client().await;
    let key = account_key(&kms).await;
    let dlq = sqs
        .create_queue()
        .queue_name("dlq")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::SqsManagedSseEnabled,
            "false",
        )
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    let src = sqs
        .create_queue()
        .queue_name("src")
        .attributes(aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId, &key)
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::RedrivePolicy,
            r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dlq","maxReceiveCount":"1"}"#,
        )
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout,
            "0",
        )
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    sqs.send_message()
        .queue_url(&src)
        .message_body("secret body")
        .send()
        .await
        .unwrap();
    // The first receive takes it; the second moves it to the DLQ.
    for _ in 0..2 {
        sqs.receive_message().queue_url(&src).send().await.unwrap();
    }
    let got = sqs.receive_message().queue_url(&dlq).send().await.unwrap();
    assert_eq!(got.messages().len(), 1);
    assert_eq!(got.messages()[0].body(), Some("secret body"));

    // The same for an SSE-SQS source: its body is sealed with SQS's own key
    // and keeps that seal in the plain dead-letter queue.
    let sse_src = sqs
        .create_queue()
        .queue_name("sse-src")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::RedrivePolicy,
            r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dlq","maxReceiveCount":"1"}"#,
        )
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout,
            "0",
        )
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    sqs.send_message()
        .queue_url(&sse_src)
        .message_body("sse-sqs body")
        .send()
        .await
        .unwrap();
    for _ in 0..2 {
        sqs.receive_message()
            .queue_url(&sse_src)
            .send()
            .await
            .unwrap();
    }
    let messages: serde_json::Value =
        reqwest::get(format!("{}/_fakecloud/sqs/messages", server.endpoint()))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let in_dlq = messages["queues"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["queueName"] == "dlq")
        .unwrap()["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["body"] == "sse-sqs body")
        .cloned()
        .expect("the SSE-SQS message is in the dead-letter queue");
    assert_ne!(in_dlq["storedBody"], "sse-sqs body", "{in_dlq}");
    let got = sqs.receive_message().queue_url(&dlq).send().await.unwrap();
    assert_eq!(got.messages().len(), 1);
    assert_eq!(got.messages()[0].body(), Some("sse-sqs body"));
}

#[tokio::test]
async fn eventbridge_sends_events_kms_refuses_to_the_target_dead_letter_queue() {
    // Recorded in fixtures/aws-managed-keys/secrets-eventbridge-dlq-sse-sqs.txt.
    let server = TestServer::start().await;
    let cfg = server.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let events = aws_sdk_eventbridge::Client::new(&cfg);
    let policy = |name: &str| {
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "events.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": format!("arn:aws:sqs:us-east-1:123456789012:{name}")}]
        })
        .to_string()
    };
    let dlq = sqs
        .create_queue()
        .queue_name("dlq")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::Policy,
            policy("dlq"),
        )
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    sqs.create_queue()
        .queue_name("target")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::Policy,
            policy("target"),
        )
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId,
            "alias/aws/sqs",
        )
        .send()
        .await
        .unwrap();
    let rule_arn = events
        .put_rule()
        .name("r")
        .event_pattern(r#"{"source":["fc.test"]}"#)
        .send()
        .await
        .unwrap()
        .rule_arn
        .unwrap();
    events
        .put_targets()
        .rule("r")
        .targets(
            aws_sdk_eventbridge::types::Target::builder()
                .id("t")
                .arn("arn:aws:sqs:us-east-1:123456789012:target")
                .dead_letter_config(
                    aws_sdk_eventbridge::types::DeadLetterConfig::builder()
                        .arn("arn:aws:sqs:us-east-1:123456789012:dlq")
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let archive_arn = events
        .create_archive()
        .archive_name("a")
        .event_source_arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
        .send()
        .await
        .unwrap()
        .archive_arn
        .unwrap();
    let put = events
        .put_events()
        .entries(
            aws_sdk_eventbridge::types::PutEventsRequestEntry::builder()
                .source("fc.test")
                .detail_type("t")
                .detail(r#"{"k":"v"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let event_id = put.entries()[0].event_id().unwrap().to_string();

    let mut messages = Vec::new();
    for _ in 0..50 {
        messages = sqs
            .receive_message()
            .queue_url(&dlq)
            .message_attribute_names("All")
            .send()
            .await
            .unwrap()
            .messages
            .unwrap_or_default();
        if !messages.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(messages.len(), 1);
    let msg = &messages[0];
    let body: serde_json::Value = serde_json::from_str(msg.body().unwrap()).unwrap();
    assert_eq!(body["id"], event_id.as_str());
    assert_eq!(body["detail"]["k"], "v");
    let attr = |name: &str| {
        msg.message_attributes()
            .and_then(|a| a.get(name))
            .and_then(|v| v.string_value())
            .unwrap_or_else(|| panic!("{name} missing: {msg:?}"))
            .to_string()
    };
    assert_eq!(attr("ERROR_CODE"), "NO_PERMISSIONS");
    assert_eq!(attr("RULE_ARN"), rule_arn);
    assert_eq!(
        attr("TARGET_ARN"),
        "arn:aws:sqs:us-east-1:123456789012:target"
    );
    let message = attr("ERROR_MESSAGE");
    assert!(
        message.starts_with("User: events.amazonaws.com is not authorized to perform: kms:GenerateDataKey on this resource because the resource does not exist in this Region, no resource-based policies allow access, or a resource-based policy explicitly denies access (Service: AWSKMS; Status Code: 400; Error Code: AccessDeniedException; Request ID: "),
        "{message}"
    );
    assert!(
        message.contains("; Proxy: null) (Service: AmazonSQS; Status Code: 400; Error Code: KMS.AccessDeniedException; Request ID: "),
        "{message}"
    );

    // A replayed event is delivered through the rule that matches it, so
    // its dead-letter message names that rule too.
    sqs.delete_message()
        .queue_url(&dlq)
        .receipt_handle(msg.receipt_handle().unwrap())
        .send()
        .await
        .unwrap();
    events
        .start_replay()
        .replay_name("rp")
        .event_source_arn(&archive_arn)
        .destination(
            aws_sdk_eventbridge::types::ReplayDestination::builder()
                .arn("arn:aws:events:us-east-1:123456789012:event-bus/default")
                .build()
                .unwrap(),
        )
        .event_start_time(aws_sdk_eventbridge::primitives::DateTime::from_secs(0))
        .event_end_time(aws_sdk_eventbridge::primitives::DateTime::from_secs(
            chrono::Utc::now().timestamp() + 3600,
        ))
        .send()
        .await
        .unwrap();
    let mut replayed = Vec::new();
    for _ in 0..50 {
        replayed = sqs
            .receive_message()
            .queue_url(&dlq)
            .message_attribute_names("All")
            .send()
            .await
            .unwrap()
            .messages
            .unwrap_or_default();
        if !replayed.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(replayed.len(), 1);
    let replayed_attr = |name: &str| {
        replayed[0]
            .message_attributes()
            .and_then(|a| a.get(name))
            .and_then(|v| v.string_value())
            .unwrap_or_else(|| panic!("{name} missing: {:?}", replayed[0]))
            .to_string()
    };
    assert_eq!(replayed_attr("ERROR_CODE"), "NO_PERMISSIONS");
    assert_eq!(replayed_attr("RULE_ARN"), rule_arn);
    let body: serde_json::Value = serde_json::from_str(replayed[0].body().unwrap()).unwrap();
    assert_eq!(body["id"], event_id.as_str());
}

#[tokio::test]
async fn sse_sqs_queues_never_call_kms() {
    // Recorded: an SNS topic feeds a default (SSE-SQS) queue, which reports
    // no KMS key.
    let server = TestServer::start().await;
    let cfg = server.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let sns = aws_sdk_sns::Client::new(&cfg);
    let kms = server.kms_client().await;
    let url = sqs
        .create_queue()
        .queue_name("q")
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::All)
        .send()
        .await
        .unwrap();
    let attrs = attrs.attributes().unwrap();
    assert_eq!(
        attrs
            .get(&aws_sdk_sqs::types::QueueAttributeName::SqsManagedSseEnabled)
            .map(String::as_str),
        Some("true")
    );
    assert!(!attrs.contains_key(&aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId));
    let topic = sns
        .create_topic()
        .name("t")
        .send()
        .await
        .unwrap()
        .topic_arn
        .unwrap();
    sns.subscribe()
        .topic_arn(&topic)
        .protocol("sqs")
        .endpoint("arn:aws:sqs:us-east-1:123456789012:q")
        .attributes("RawMessageDelivery", "true")
        .send()
        .await
        .unwrap();
    sns.publish()
        .topic_arn(&topic)
        .message("to-sse-sqs")
        .send()
        .await
        .unwrap();
    sqs.send_message()
        .queue_url(&url)
        .message_body("direct")
        .send()
        .await
        .unwrap();
    // A body that is a KMS ciphertext the sender made is delivered as sent.
    let key = account_key(&kms).await;
    let blob = kms
        .encrypt()
        .key_id(&key)
        .plaintext(Blob::new(b"app-sealed".to_vec()))
        .send()
        .await
        .unwrap()
        .ciphertext_blob
        .unwrap();
    let app_sealed = base64::engine::general_purpose::STANDARD.encode(blob.as_ref());
    sqs.send_message()
        .queue_url(&url)
        .message_body(&app_sealed)
        .send()
        .await
        .unwrap();
    let mut bodies = queue_bodies(&sqs, &url).await;
    bodies.sort();
    let mut expected = vec![
        "direct".to_string(),
        "to-sse-sqs".to_string(),
        app_sealed.clone(),
    ];
    expected.sort();
    assert_eq!(bodies, expected);

    // KMS was never called: no aws/sqs key exists (it is created on first
    // use, and is not one of the aliases listed before then).
    let aliases = kms.list_aliases().send().await.unwrap();
    assert!(
        !aliases
            .aliases()
            .iter()
            .any(|a| a.alias_name() == Some("alias/aws/sqs")),
        "{aliases:?}"
    );
}

/// The recorded EventBridge SQS-target failures
/// (fixtures/aws-managed-keys/eventbridge-sqs-target-failures.json): a queue
/// under a key pending deletion and one under a missing alias both refuse
/// `events.amazonaws.com` as AccessDenied (`NO_PERMISSIONS`), and a missing
/// queue is `NO_RESOURCE` with SQS's NonExistentQueue message. Each counts
/// in `Invocations`, `FailedInvocations` and `InvocationsSentToDlq`.
#[tokio::test]
async fn eventbridge_sqs_target_failures_match_the_recording() {
    let server = TestServer::start().await;
    let cfg = server.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let events = aws_sdk_eventbridge::Client::new(&cfg);
    let kms = server.kms_client().await;
    let cw = aws_sdk_cloudwatch::Client::new(&cfg);
    let policy = |name: &str| {
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "events.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": format!("arn:aws:sqs:us-east-1:123456789012:{name}")}]
        })
        .to_string()
    };
    let pending_key = account_key(&kms).await;
    kms.schedule_key_deletion()
        .key_id(&pending_key)
        .pending_window_in_days(7)
        .send()
        .await
        .unwrap();
    let dlq = sqs
        .create_queue()
        .queue_name("dlq")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::Policy,
            policy("dlq"),
        )
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    for (name, key) in [
        ("pending", pending_key.as_str()),
        ("nokey", "alias/fc-does-not-exist"),
    ] {
        sqs.create_queue()
            .queue_name(name)
            .attributes(aws_sdk_sqs::types::QueueAttributeName::Policy, policy(name))
            .attributes(aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId, key)
            .send()
            .await
            .unwrap();
    }
    for name in ["pending", "nokey", "missing"] {
        events
            .put_rule()
            .name(name)
            .event_pattern(format!(r#"{{"source":["fc.rec.{name}"]}}"#))
            .send()
            .await
            .unwrap();
        events
            .put_targets()
            .rule(name)
            .targets(
                aws_sdk_eventbridge::types::Target::builder()
                    .id("t")
                    .arn(format!("arn:aws:sqs:us-east-1:123456789012:{name}"))
                    .dead_letter_config(
                        aws_sdk_eventbridge::types::DeadLetterConfig::builder()
                            .arn("arn:aws:sqs:us-east-1:123456789012:dlq")
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        events
            .put_events()
            .entries(
                aws_sdk_eventbridge::types::PutEventsRequestEntry::builder()
                    .source(format!("fc.rec.{name}"))
                    .detail_type("t")
                    .detail(format!(r#"{{"k":"{name}"}}"#))
                    .build(),
            )
            .send()
            .await
            .unwrap();
    }

    let mut messages = Vec::new();
    for _ in 0..50 {
        let got = sqs
            .receive_message()
            .queue_url(&dlq)
            .max_number_of_messages(10)
            .message_attribute_names("All")
            .send()
            .await
            .unwrap()
            .messages
            .unwrap_or_default();
        messages.extend(got);
        if messages.len() >= 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(messages.len(), 3);
    let refused = "User: events.amazonaws.com is not authorized to perform: kms:GenerateDataKey on this resource because the resource does not exist in this Region, no resource-based policies allow access, or a resource-based policy explicitly denies access (Service: AWSKMS; Status Code: 400; Error Code: AccessDeniedException; Request ID: ";
    for msg in &messages {
        let body: serde_json::Value = serde_json::from_str(msg.body().unwrap()).unwrap();
        let name = body["detail"]["k"].as_str().unwrap().to_string();
        let attr = |key: &str| {
            msg.message_attributes()
                .and_then(|a| a.get(key))
                .and_then(|v| v.string_value())
                .unwrap_or_else(|| panic!("{key} missing: {msg:?}"))
                .to_string()
        };
        assert_eq!(
            attr("RULE_ARN"),
            format!("arn:aws:events:us-east-1:123456789012:rule/{name}")
        );
        assert_eq!(
            attr("TARGET_ARN"),
            format!("arn:aws:sqs:us-east-1:123456789012:{name}")
        );
        let message = attr("ERROR_MESSAGE");
        if name == "missing" {
            assert_eq!(attr("ERROR_CODE"), "NO_RESOURCE");
            assert!(
                message.starts_with("The specified queue does not exist or you do not have access to it. (Service: AmazonSQS; Status Code: 400; Error Code: AWS.SimpleQueueService.NonExistentQueue; Request ID: ")
                    && message.ends_with("; Proxy: null)"),
                "{message}"
            );
        } else {
            assert_eq!(attr("ERROR_CODE"), "NO_PERMISSIONS", "{name}");
            assert!(message.starts_with(refused), "{name}: {message}");
            assert!(
                message.contains("; Proxy: null) (Service: AmazonSQS; Status Code: 400; Error Code: KMS.AccessDeniedException; Request ID: "),
                "{name}: {message}"
            );
        }
        for metric in ["Invocations", "FailedInvocations", "InvocationsSentToDlq"] {
            assert_eq!(
                metric_sum(&cw, "AWS/Events", metric, "RuleName", &name).await,
                1.0,
                "{name} {metric}"
            );
        }
        assert_eq!(
            metric_sum(
                &cw,
                "AWS/Events",
                "InvocationsFailedToBeSentToDlq",
                "RuleName",
                &name
            )
            .await,
            0.0
        );
    }
}
