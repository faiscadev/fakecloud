mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("account", "AcceptPrimaryEmailUpdate", checksum = "29f24d27")]
#[test_action("account", "DeleteAlternateContact", checksum = "8f8e5351")]
#[test_action("account", "DisableRegion", checksum = "36702371")]
#[test_action("account", "EnableRegion", checksum = "0450bf5c")]
#[test_action("account", "GetAccountInformation", checksum = "fa3f9583")]
#[test_action("account", "GetAlternateContact", checksum = "ff5c6fd2")]
#[test_action("account", "GetContactInformation", checksum = "2d4b8256")]
#[test_action("account", "GetGovCloudAccountInformation", checksum = "85e05e5a")]
#[test_action("account", "GetPrimaryEmail", checksum = "ab6ac0a2")]
#[test_action("account", "GetRegionOptStatus", checksum = "5a2bd2e5")]
#[test_action("account", "ListRegions", checksum = "7f7242ac")]
#[test_action("account", "PutAccountName", checksum = "fe6872ea")]
#[test_action("account", "PutAlternateContact", checksum = "661f88b6")]
#[test_action("account", "PutContactInformation", checksum = "012826c9")]
#[test_action("account", "StartPrimaryEmailUpdate", checksum = "4503fb90")]
#[tokio::test]
async fn account_probe() {
    let _server = TestServer::start().await;
}

// GetPrimaryEmailUpdateStatus is newer than the typed aws-sdk-account client,
// so drive it over raw restJson1 (POST /getPrimaryEmailUpdateStatus). It
// tracks the StartPrimaryEmailUpdate -> AcceptPrimaryEmailUpdate lifecycle.
#[test_action("account", "GetPrimaryEmailUpdateStatus", checksum = "3d2f420e")]
#[tokio::test]
async fn account_primary_email_update_status() {
    let server = TestServer::start().await;
    let auth = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/account/aws4_request, SignedHeaders=host, Signature=0";
    let acct = "123456789012";
    let call = |path: &str, body: String| {
        let url = format!("{}/{path}", server.endpoint());
        async move {
            reqwest::Client::new()
                .post(url)
                .header("Authorization", auth)
                .header("Content-Type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap()
        }
    };

    // No update started yet -> 404 NotFound.
    let resp = call(
        "getPrimaryEmailUpdateStatus",
        format!(r#"{{"AccountId":"{acct}"}}"#),
    )
    .await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "expected NotFound before any update"
    );

    // Start an update -> status is PENDING.
    let resp = call(
        "startPrimaryEmailUpdate",
        format!(r#"{{"AccountId":"{acct}","PrimaryEmail":"new@example.com"}}"#),
    )
    .await;
    assert!(resp.status().is_success(), "start: {}", resp.status());
    let resp = call(
        "getPrimaryEmailUpdateStatus",
        format!(r#"{{"AccountId":"{acct}"}}"#),
    )
    .await;
    assert!(resp.status().is_success(), "status: {}", resp.status());
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["Status"].as_str(), Some("PENDING"), "{v}");

    // Accept it -> status is ACCEPTED.
    let resp = call(
        "acceptPrimaryEmailUpdate",
        format!(r#"{{"AccountId":"{acct}","PrimaryEmail":"new@example.com","Otp":"000000"}}"#),
    )
    .await;
    assert!(resp.status().is_success(), "accept: {}", resp.status());
    let resp = call(
        "getPrimaryEmailUpdateStatus",
        format!(r#"{{"AccountId":"{acct}"}}"#),
    )
    .await;
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["Status"].as_str(), Some("ACCEPTED"), "{v}");
}
