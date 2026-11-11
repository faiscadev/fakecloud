//! Secrets Manager + KMS hook end-to-end:
//! - CreateSecret with `KmsKeyId` triggers `kms:GenerateDataKey`
//! - GetSecretValue triggers `kms:Decrypt` and returns the original
//!   plaintext (transparent encrypt/decrypt round-trip)
//! - Both calls land in `/_fakecloud/kms/usage` with the encryption
//!   context AWS uses (`SecretARN` + `SecretVersionId`)
//!
//! No KMS key is created up-front — the hook auto-provisions an
//! `aws/secretsmanager` AWS-managed key on first use, mirroring real
//! AWS.

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn secretsmanager_create_get_round_trips_through_kms() {
    let server = TestServer::start().await;
    let secrets = server.secretsmanager_client().await;
    let http = reqwest::Client::new();

    let created = secrets
        .create_secret()
        .name("db-password")
        .secret_string("hunter2")
        .kms_key_id("alias/aws/secretsmanager")
        .send()
        .await
        .unwrap();
    let arn = created.arn().unwrap().to_string();

    let got = secrets
        .get_secret_value()
        .secret_id("db-password")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.secret_string(),
        Some("hunter2"),
        "round-trip should return the original plaintext through KMS"
    );

    // Both KMS calls should be visible via the introspection endpoint.
    let usage: serde_json::Value = http
        .get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let records = usage["records"].as_array().expect("records array");
    let svc_records: Vec<&serde_json::Value> = records
        .iter()
        .filter(|r| r["servicePrincipal"].as_str() == Some("secretsmanager.amazonaws.com"))
        .collect();
    assert!(
        svc_records.iter().any(|r| {
            r["operation"].as_str() == Some("GenerateDataKey")
                && r["encryptionContext"]["SecretARN"].as_str() == Some(arn.as_str())
                && r["encryptionContext"]["SecretVersionId"].as_str().is_some()
        }),
        "expected GenerateDataKey record bound to secret ARN, got: {svc_records:?}"
    );
    assert!(
        svc_records.iter().any(|r| {
            r["operation"].as_str() == Some("Decrypt")
                && r["encryptionContext"]["SecretARN"].as_str() == Some(arn.as_str())
                && r["encryptionContext"]["SecretVersionId"].as_str().is_some()
        }),
        "expected Decrypt record bound to secret ARN, got: {svc_records:?}"
    );
}

#[tokio::test]
async fn secretsmanager_without_kms_key_does_not_record_usage() {
    let server = TestServer::start().await;
    let secrets = server.secretsmanager_client().await;
    let http = reqwest::Client::new();

    secrets
        .create_secret()
        .name("plaintext-secret")
        .secret_string("nope")
        .send()
        .await
        .unwrap();

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
            .any(|r| r["servicePrincipal"].as_str() == Some("secretsmanager.amazonaws.com")),
        "secret without KmsKeyId must not trigger KMS calls, got: {records:?}"
    );
}
