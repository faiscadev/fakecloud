//! ACM conformance tests.

mod helpers;

use aws_sdk_acm::primitives::Blob;
use aws_sdk_acm::types::{
    CertificateOptions, CertificateTransparencyLoggingPreference, RevocationReason, Tag,
    ValidationMethod,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

const FAKE_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n";
const FAKE_KEY_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n";

async fn make_cert(server: &TestServer, domain: &str) -> String {
    let acm = server.acm_client().await;
    acm.request_certificate()
        .domain_name(domain)
        .validation_method(ValidationMethod::Dns)
        .send()
        .await
        .unwrap()
        .certificate_arn()
        .unwrap()
        .to_string()
}

#[test_action("acm", "RequestCertificate", checksum = "385cddec")]
#[tokio::test]
async fn acm_request_certificate() {
    let server = TestServer::start().await;
    let acm = server.acm_client().await;
    acm.request_certificate()
        .domain_name("conf.example.com")
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "DescribeCertificate", checksum = "cf0b76ba")]
#[tokio::test]
async fn acm_describe_certificate() {
    let server = TestServer::start().await;
    let arn = make_cert(&server, "conf-describe.example.com").await;
    let acm = server.acm_client().await;
    acm.describe_certificate()
        .certificate_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "ListCertificates", checksum = "715fca42")]
#[tokio::test]
async fn acm_list_certificates() {
    let server = TestServer::start().await;
    let acm = server.acm_client().await;
    acm.list_certificates().send().await.unwrap();
}

#[test_action("acm", "DeleteCertificate", checksum = "5478fa21")]
#[tokio::test]
async fn acm_delete_certificate() {
    let server = TestServer::start().await;
    let arn = make_cert(&server, "conf-delete.example.com").await;
    let acm = server.acm_client().await;
    acm.delete_certificate()
        .certificate_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "ImportCertificate", checksum = "e4238636")]
#[tokio::test]
async fn acm_import_certificate() {
    let server = TestServer::start().await;
    let acm = server.acm_client().await;
    acm.import_certificate()
        .certificate(Blob::new(FAKE_CERT_PEM.as_bytes().to_vec()))
        .private_key(Blob::new(FAKE_KEY_PEM.as_bytes().to_vec()))
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "ExportCertificate", checksum = "b6507262")]
#[tokio::test]
async fn acm_export_certificate() {
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    use rsa::RsaPrivateKey;

    let server = TestServer::start().await;
    let acm = server.acm_client().await;
    // ExportCertificate wraps the private key in a passphrase-encrypted
    // PKCS#8 v2 envelope, so import a real RSA-2048 PKCS#8 PEM (the
    // FAKE_KEY_PEM placeholder is not valid DER and would fail the
    // PKCS#8 parse step on export).
    let mut rng = rand::thread_rng();
    let key_pem = RsaPrivateKey::new(&mut rng, 2048)
        .expect("rsa keygen")
        .to_pkcs8_pem(LineEnding::LF)
        .expect("pkcs8 pem")
        .to_string();
    let arn = acm
        .import_certificate()
        .certificate(Blob::new(FAKE_CERT_PEM.as_bytes().to_vec()))
        .private_key(Blob::new(key_pem.into_bytes()))
        .send()
        .await
        .unwrap()
        .certificate_arn()
        .unwrap()
        .to_string();
    acm.export_certificate()
        .certificate_arn(&arn)
        .passphrase(Blob::new(b"hunter2".to_vec()))
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "GetCertificate", checksum = "bc969a34")]
#[tokio::test]
async fn acm_get_certificate() {
    let server = TestServer::start().await;
    let arn = make_cert(&server, "conf-get.example.com").await;
    let acm = server.acm_client().await;
    acm.get_certificate()
        .certificate_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "RenewCertificate", checksum = "c5e940bd")]
#[tokio::test]
async fn acm_renew_certificate() {
    let server = TestServer::start().await;
    let arn = make_cert(&server, "conf-renew.example.com").await;
    let acm = server.acm_client().await;
    acm.renew_certificate()
        .certificate_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "RevokeCertificate", checksum = "0d126732")]
#[tokio::test]
async fn acm_revoke_certificate() {
    let server = TestServer::start().await;
    let arn = make_cert(&server, "conf-revoke.example.com").await;
    let acm = server.acm_client().await;
    acm.revoke_certificate()
        .certificate_arn(&arn)
        .revocation_reason(RevocationReason::Unspecified)
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "ResendValidationEmail", checksum = "7fc43329")]
#[tokio::test]
async fn acm_resend_validation_email() {
    let server = TestServer::start().await;
    let acm = server.acm_client().await;
    let arn = acm
        .request_certificate()
        .domain_name("conf-resend.example.com")
        .validation_method(ValidationMethod::Email)
        .send()
        .await
        .unwrap()
        .certificate_arn()
        .unwrap()
        .to_string();
    acm.resend_validation_email()
        .certificate_arn(&arn)
        .domain("conf-resend.example.com")
        .validation_domain("example.com")
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "AddTagsToCertificate", checksum = "9e285fa1")]
#[tokio::test]
async fn acm_add_tags() {
    let server = TestServer::start().await;
    let arn = make_cert(&server, "conf-addtag.example.com").await;
    let acm = server.acm_client().await;
    acm.add_tags_to_certificate()
        .certificate_arn(&arn)
        .tags(Tag::builder().key("k").value("v").build().unwrap())
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "RemoveTagsFromCertificate", checksum = "9527abb3")]
#[tokio::test]
async fn acm_remove_tags() {
    let server = TestServer::start().await;
    let arn = make_cert(&server, "conf-rmtag.example.com").await;
    let acm = server.acm_client().await;
    acm.add_tags_to_certificate()
        .certificate_arn(&arn)
        .tags(Tag::builder().key("k").value("v").build().unwrap())
        .send()
        .await
        .unwrap();
    acm.remove_tags_from_certificate()
        .certificate_arn(&arn)
        .tags(Tag::builder().key("k").build().unwrap())
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "ListTagsForCertificate", checksum = "1fb5690b")]
#[tokio::test]
async fn acm_list_tags() {
    let server = TestServer::start().await;
    let arn = make_cert(&server, "conf-listtag.example.com").await;
    let acm = server.acm_client().await;
    acm.list_tags_for_certificate()
        .certificate_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "GetAccountConfiguration", checksum = "40ab0077")]
#[tokio::test]
async fn acm_get_account_config() {
    let server = TestServer::start().await;
    let acm = server.acm_client().await;
    acm.get_account_configuration().send().await.unwrap();
}

#[test_action("acm", "PutAccountConfiguration", checksum = "1f1e8ad9")]
#[tokio::test]
async fn acm_put_account_config() {
    let server = TestServer::start().await;
    let acm = server.acm_client().await;
    acm.put_account_configuration()
        .idempotency_token("conf-put-1")
        .expiry_events(
            aws_sdk_acm::types::ExpiryEventsConfiguration::builder()
                .days_before_expiry(45)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "UpdateCertificateOptions", checksum = "7f299377")]
#[tokio::test]
async fn acm_update_options() {
    let server = TestServer::start().await;
    let arn = make_cert(&server, "conf-opts.example.com").await;
    let acm = server.acm_client().await;
    acm.update_certificate_options()
        .certificate_arn(&arn)
        .options(
            CertificateOptions::builder()
                .certificate_transparency_logging_preference(
                    CertificateTransparencyLoggingPreference::Disabled,
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("acm", "SearchCertificates", checksum = "4d6418b5")]
#[tokio::test]
async fn acm_search_certificates() {
    let server = TestServer::start().await;
    let acm = server.acm_client().await;
    acm.search_certificates().send().await.unwrap();
}

/// The ARN-keyed tagging trio (`TagResource` / `UntagResource` /
/// `ListTagsForResource`). The vendored `aws-sdk-acm` predates these
/// operations, so drive them over raw HTTP.
#[test_action("acm", "TagResource", checksum = "0dd309e2")]
#[test_action("acm", "UntagResource", checksum = "11cc21cb")]
#[test_action("acm", "ListTagsForResource", checksum = "88b17fef")]
#[tokio::test]
async fn acm_resource_tagging_round_trip() {
    let server = TestServer::start().await;
    let arn = make_cert(&server, "conf-resourcetag.example.com").await;

    let auth = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/acm/aws4_request, SignedHeaders=host, Signature=0";
    let http = reqwest::Client::new();
    let call = |target: &str, body: String| {
        let url = server.endpoint().to_string();
        let target = format!("CertificateManager.{target}");
        let http = http.clone();
        async move {
            http.post(&url)
                .header("Authorization", auth)
                .header("X-Amz-Target", target)
                .header("Content-Type", "application/x-amz-json-1.1")
                .body(body)
                .send()
                .await
                .unwrap()
        }
    };

    let resp = call(
        "TagResource",
        serde_json::json!({ "ResourceArn": arn, "Tags": [{ "Key": "team", "Value": "core" }] })
            .to_string(),
    )
    .await;
    assert!(resp.status().is_success(), "TagResource: {}", resp.status());

    let resp = call(
        "ListTagsForResource",
        serde_json::json!({ "ResourceArn": arn }).to_string(),
    )
    .await;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["Tags"][0]["Key"], "team");
    assert_eq!(body["Tags"][0]["Value"], "core");

    let resp = call(
        "UntagResource",
        serde_json::json!({ "ResourceArn": arn, "TagKeys": ["team"] }).to_string(),
    )
    .await;
    assert!(resp.status().is_success());

    let resp = call(
        "ListTagsForResource",
        serde_json::json!({ "ResourceArn": arn }).to_string(),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["Tags"].as_array().unwrap().is_empty());

    // An unknown ARN is ResourceNotFoundException, not a 200.
    let resp = call(
        "ListTagsForResource",
        serde_json::json!({ "ResourceArn": "arn:aws:acm:us-east-1:000000000000:certificate/nope" })
            .to_string(),
    )
    .await;
    assert_eq!(resp.status().as_u16(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[test_action("acm", "CreateAcmeEndpoint", checksum = "abaa76f8")]
#[test_action("acm", "DescribeAcmeEndpoint", checksum = "8136ef22")]
#[test_action("acm", "ListAcmeEndpoints", checksum = "222a44a5")]
#[test_action("acm", "UpdateAcmeEndpoint", checksum = "d18ace62")]
#[test_action("acm", "DeleteAcmeEndpoint", checksum = "3c3bef6a")]
#[test_action("acm", "CreateAcmeExternalAccountBinding", checksum = "cd3e4faa")]
#[test_action("acm", "DescribeAcmeExternalAccountBinding", checksum = "eef71175")]
#[test_action("acm", "ListAcmeExternalAccountBindings", checksum = "7f9c3531")]
#[test_action("acm", "RevokeAcmeExternalAccountBinding", checksum = "942dd969")]
#[test_action("acm", "DeleteAcmeExternalAccountBinding", checksum = "fb81c04f")]
#[test_action(
    "acm",
    "GetAcmeExternalAccountBindingCredentials",
    checksum = "488d780b"
)]
#[test_action("acm", "CreateAcmeDomainValidation", checksum = "56c53e35")]
#[test_action("acm", "DescribeAcmeDomainValidation", checksum = "fe93e5cf")]
#[test_action("acm", "ListAcmeDomainValidations", checksum = "42c056b9")]
#[test_action("acm", "UpdateAcmeDomainValidation", checksum = "fea27f24")]
#[test_action("acm", "DeleteAcmeDomainValidation", checksum = "9f9ffb72")]
#[test_action("acm", "DescribeAcmeAccount", checksum = "099d466b")]
#[test_action("acm", "ListAcmeAccounts", checksum = "41886794")]
#[test_action("acm", "RevokeAcmeAccount", checksum = "9f6841e6")]
#[tokio::test]
async fn acm_acme_probe() {
    // The vendored aws-sdk-acm predates the ACME surface; the probe drives
    // these operations from the model, and the handlers are covered by unit
    // tests in `fakecloud-acm`.
    let _server = TestServer::start().await;
}
