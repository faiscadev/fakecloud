//! SQS encrypted queue + KMS hook end-to-end:
//! - SendMessage on a queue with KmsMasterKeyId triggers
//!   `kms:GenerateDataKey` with the queue-arn encryption context
//! - ReceiveMessage decrypts via `kms:Decrypt` and returns the original
//!   plaintext body
//! - Both calls land in `/_fakecloud/kms/usage`
//! - `/_fakecloud/sqs/messages` shows the plaintext `body` next to the
//!   at-rest `storedBody`

mod helpers;

use helpers::TestServer;

/// `(body, storedBody)` of the single message on `queue_name`, read from
/// `/_fakecloud/sqs/messages`.
async fn introspected_body(server: &TestServer, queue_name: &str) -> (String, String) {
    let resp: serde_json::Value =
        reqwest::get(format!("{}/_fakecloud/sqs/messages", server.endpoint()))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let queue = resp["queues"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["queueName"] == queue_name)
        .unwrap_or_else(|| panic!("{queue_name} missing from {resp}"));
    let msg = &queue["messages"][0];
    (
        msg["body"].as_str().unwrap().to_string(),
        msg["storedBody"].as_str().unwrap().to_string(),
    )
}

/// How many KMS usage records SQS has left.
async fn sqs_kms_usage_count(server: &TestServer) -> usize {
    let usage: serde_json::Value =
        reqwest::get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    usage["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["servicePrincipal"] == "sqs.amazonaws.com")
        .count()
}

#[tokio::test]
async fn sqs_send_receive_encrypted_round_trips_through_kms() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let http = reqwest::Client::new();

    let queue = sqs
        .create_queue()
        .queue_name("encrypted")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId,
            "alias/aws/sqs",
        )
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();

    sqs.send_message()
        .queue_url(&queue_url)
        .message_body("payload-42")
        .send()
        .await
        .unwrap();

    let got = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .unwrap();
    let msgs = got.messages();
    assert_eq!(msgs.len(), 1, "expected one message back");
    assert_eq!(
        msgs[0].body(),
        Some("payload-42"),
        "ReceiveMessage must return decrypted plaintext"
    );

    let usage: serde_json::Value = http
        .get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let records = usage["records"].as_array().expect("records array");
    let sqs_records: Vec<&serde_json::Value> = records
        .iter()
        .filter(|r| r["servicePrincipal"].as_str() == Some("sqs.amazonaws.com"))
        .collect();
    assert!(
        sqs_records.iter().any(|r| {
            r["operation"].as_str() == Some("GenerateDataKey")
                && r["encryptionContext"]["aws:sqs:arn"].as_str().is_some()
        }),
        "expected GenerateDataKey bound to queue arn, got: {sqs_records:?}"
    );
    assert!(
        sqs_records.iter().any(|r| {
            r["operation"].as_str() == Some("Decrypt")
                && r["encryptionContext"]["aws:sqs:arn"].as_str().is_some()
        }),
        "expected Decrypt bound to queue arn, got: {sqs_records:?}"
    );
}

/// Regression: ReceiveMessage against an SSE-KMS queue whose key became
/// unusable (disabled) must return an error WITHOUT destroying the batch.
/// Previously the messages were popped out of the queue before the decrypt
/// `?` unwound, permanently losing them. Real SQS leaves them visible.
#[tokio::test]
async fn sqs_receive_decrypt_failure_does_not_destroy_messages() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let kms = server.kms_client().await;

    // Customer-managed key we can disable (managed alias/aws/sqs can't be).
    let key = kms.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let queue = sqs
        .create_queue()
        .queue_name("decrypt-fail")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId,
            &key_id,
        )
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();

    sqs.send_message()
        .queue_url(&queue_url)
        .message_body("do-not-lose-me")
        .send()
        .await
        .unwrap();

    let (body, stored_body) = introspected_body(&server, "decrypt-fail").await;
    assert_eq!(body, "do-not-lose-me");
    assert_ne!(stored_body, "do-not-lose-me");

    // Disable the key: the next receive must fail to decrypt.
    kms.disable_key().key_id(&key_id).send().await.unwrap();
    // Introspection still reads the plaintext and leaves no KMS usage.
    let usage_before = sqs_kms_usage_count(&server).await;
    let (body, _) = introspected_body(&server, "decrypt-fail").await;
    assert_eq!(body, "do-not-lose-me");
    assert_eq!(sqs_kms_usage_count(&server).await, usage_before);
    let failed = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await;
    assert!(
        failed.is_err(),
        "ReceiveMessage must error when the KMS key is disabled, got: {failed:?}"
    );

    // Re-enable the key: the message must still be there (not destroyed).
    kms.enable_key().key_id(&key_id).send().await.unwrap();
    let recovered = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .unwrap();
    let msgs = recovered.messages();
    assert_eq!(
        msgs.len(),
        1,
        "message must survive a decrypt failure and be receivable after the key is re-enabled"
    );
    assert_eq!(msgs[0].body(), Some("do-not-lose-me"));
}

#[tokio::test]
async fn sqs_unencrypted_queue_does_not_record_kms_usage() {
    use aws_sdk_sqs::types::QueueAttributeName;

    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let http = reqwest::Client::new();

    // Real AWS defaults `SqsManagedSseEnabled=true` since May 2023, so a
    // queue created without explicit attributes still encrypts messages
    // under `alias/aws/sqs` and DOES record KMS usage. To prove the
    // unencrypted path stays inert, opt out of managed SSE explicitly.
    let queue = sqs
        .create_queue()
        .queue_name("plain")
        .attributes(QueueAttributeName::SqsManagedSseEnabled, "false")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    sqs.send_message()
        .queue_url(&queue_url)
        .message_body("plain")
        .send()
        .await
        .unwrap();
    let (body, stored_body) = introspected_body(&server, "plain").await;
    assert_eq!(body, "plain");
    assert_eq!(stored_body, "plain");

    let usage: serde_json::Value = http
        .get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let records = usage["records"].as_array().expect("records array");
    assert!(
        !records
            .iter()
            .any(|r| r["servicePrincipal"].as_str() == Some("sqs.amazonaws.com")),
        "queue with managed SSE off and no KmsMasterKeyId must not record KMS usage, got: {records:?}"
    );
}

/// SQS-managed SSE (`SqsManagedSseEnabled=true`, the post-May-2023
/// default) must encrypt the body at rest under `alias/aws/sqs` and
/// decrypt it on Receive — same audit trail as customer-managed
/// SSE-KMS, but using the AWS-managed key.
#[tokio::test]
async fn sqs_managed_sse_round_trips_through_alias_aws_sqs() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let http = reqwest::Client::new();

    // Default-attribute queue: SqsManagedSseEnabled=true is the AWS
    // default, so we don't need to set anything explicit.
    let queue = sqs
        .create_queue()
        .queue_name("sse-managed")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();

    sqs.send_message()
        .queue_url(&queue_url)
        .message_body("managed-sse-payload")
        .send()
        .await
        .unwrap();

    // Stored body is an opaque ciphertext (no plaintext leak); `body` is
    // the plaintext.
    let (body, stored_body) = introspected_body(&server, "sse-managed").await;
    assert_ne!(
        stored_body, "managed-sse-payload",
        "SqsManagedSseEnabled must encrypt the body at rest, got plaintext"
    );
    assert_eq!(body, "managed-sse-payload");

    // Receive must surface the original plaintext.
    let got = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .unwrap();
    let msgs = got.messages();
    assert_eq!(msgs.len(), 1, "expected one message back");
    assert_eq!(
        msgs[0].body(),
        Some("managed-sse-payload"),
        "ReceiveMessage must return decrypted plaintext under SSE-SQS"
    );

    // Audit trail records the managed alias as the key behind both
    // GenerateDataKey (send) and Decrypt (receive).
    let usage: serde_json::Value = http
        .get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let records = usage["records"].as_array().expect("records array");
    let sqs_records: Vec<&serde_json::Value> = records
        .iter()
        .filter(|r| r["servicePrincipal"].as_str() == Some("sqs.amazonaws.com"))
        .collect();
    assert!(
        sqs_records
            .iter()
            .any(|r| r["operation"].as_str() == Some("GenerateDataKey")
                && r["keyArn"].as_str().is_some_and(|a| a.contains("kms:"))),
        "expected GenerateDataKey via alias/aws/sqs, got: {sqs_records:?}"
    );
    assert!(
        sqs_records
            .iter()
            .any(|r| r["operation"].as_str() == Some("Decrypt")),
        "expected paired Decrypt for SSE-SQS receive, got: {sqs_records:?}"
    );
}
