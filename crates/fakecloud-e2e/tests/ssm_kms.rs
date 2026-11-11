//! SSM SecureString + KMS hook end-to-end:
//! - PutParameter with `Type=SecureString` triggers `kms:Encrypt`
//! - GetParameter with `WithDecryption=true` triggers `kms:Decrypt` and
//!   returns the original plaintext (transparent round-trip)
//! - Both calls land in `/_fakecloud/kms/usage` with the
//!   parameter-arn-shaped encryption context

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn ssm_securestring_round_trips_through_kms() {
    let server = TestServer::start().await;
    let ssm = server.ssm_client().await;
    let http = reqwest::Client::new();

    ssm.put_parameter()
        .name("/app/db/password")
        .value("hunter2")
        .r#type(aws_sdk_ssm::types::ParameterType::SecureString)
        .send()
        .await
        .unwrap();

    let got = ssm
        .get_parameter()
        .name("/app/db/password")
        .with_decryption(true)
        .send()
        .await
        .unwrap();
    let p = got.parameter().expect("parameter present");
    assert_eq!(
        p.value(),
        Some("hunter2"),
        "SecureString GetParameter with WithDecryption=true must return plaintext"
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
    let svc_records: Vec<&serde_json::Value> = records
        .iter()
        .filter(|r| r["servicePrincipal"].as_str() == Some("ssm.amazonaws.com"))
        .collect();
    assert!(
        svc_records.iter().any(|r| {
            r["operation"].as_str() == Some("Encrypt")
                && r["encryptionContext"]["PARAMETER_ARN"].as_str().is_some()
        }),
        "expected Encrypt record bound to PARAMETER_ARN, got: {svc_records:?}"
    );
    assert!(
        svc_records.iter().any(|r| {
            r["operation"].as_str() == Some("Decrypt")
                && r["encryptionContext"]["PARAMETER_ARN"].as_str().is_some()
        }),
        "expected Decrypt record bound to PARAMETER_ARN, got: {svc_records:?}"
    );
}

#[tokio::test]
async fn ssm_string_does_not_record_kms_usage() {
    let server = TestServer::start().await;
    let ssm = server.ssm_client().await;
    let http = reqwest::Client::new();

    ssm.put_parameter()
        .name("/app/feature-flag")
        .value("on")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
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
            .any(|r| r["servicePrincipal"].as_str() == Some("ssm.amazonaws.com")),
        "plain String parameters must not trigger KMS calls, got: {records:?}"
    );
}
