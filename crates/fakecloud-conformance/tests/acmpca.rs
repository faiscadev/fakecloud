//! Conformance coverage for AWS Certificate Manager Private CA (`acm-pca`,
//! awsJson1_1, target prefix `ACMPrivateCA`).
//!
//! There is no typed `aws-sdk-acmpca` in this workspace, so every operation is
//! driven over raw HTTP with the `X-Amz-Target` header, mirroring the redshift
//! suite. One `#[test_action]` per `SUPPORTED_ACTIONS` entry pins the operation
//! to its Smithy checksum so model drift fails the build; the audit
//! cross-checks this list against the service crate.

mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;
use serde_json::{json, Value};

const AUTH: &str = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/acm-pca/aws4_request, SignedHeaders=host, Signature=0";

/// POST an awsJson1_1 ACM PCA action, returning `(status, parsed_body)`.
async fn pca(server: &TestServer, op: &str, body: Value) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(format!("{}/", server.endpoint()))
        .header("content-type", "application/x-amz-json-1.1")
        .header("x-amz-target", format!("ACMPrivateCA.{op}"))
        .header("Authorization", AUTH)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    let parsed = serde_json::from_str(&text).unwrap_or(Value::Null);
    (status, parsed)
}

const ROOT_TEMPLATE: &str = "arn:aws:acm-pca:::template/RootCACertificate/V1";

/// Create a ROOT CA and drive the full activation ceremony to bring it to
/// `ACTIVE`, returning its ARN. Every CA now starts `CREATING` (key generation
/// runs in the background) and settles to `PENDING_CERTIFICATE`; a ROOT CA is
/// then activated by self-signing its own CSR with the `RootCACertificate`
/// template and importing the result, exactly as with real AWS.
async fn make_root_ca(server: &TestServer) -> String {
    let (status, body) = pca(
        server,
        "CreateCertificateAuthority",
        json!({
            "CertificateAuthorityConfiguration": {
                "KeyAlgorithm": "EC_prime256v1",
                "SigningAlgorithm": "SHA256WITHECDSA",
                "Subject": { "CommonName": "conf-root.example.com", "Organization": "Conf" }
            },
            "CertificateAuthorityType": "ROOT"
        }),
    )
    .await;
    assert_eq!(status, 200, "create root CA: {body}");
    let arn = body["CertificateAuthorityArn"]
        .as_str()
        .unwrap()
        .to_string();

    // Wait for background keygen to settle the CA to PENDING_CERTIFICATE.
    let mut csr = None;
    for _ in 0..400 {
        let (_, d) = pca(
            server,
            "GetCertificateAuthorityCsr",
            json!({ "CertificateAuthorityArn": arn }),
        )
        .await;
        if let Some(c) = d["Csr"].as_str() {
            csr = Some(c.to_string());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let csr = csr.expect("CA never produced a CSR (keygen stuck)");

    // Self-sign the root certificate (allowed while PENDING_CERTIFICATE).
    let (status, issued) = pca(
        server,
        "IssueCertificate",
        json!({
            "CertificateAuthorityArn": arn,
            "Csr": csr,
            "SigningAlgorithm": "SHA256WITHECDSA",
            "TemplateArn": ROOT_TEMPLATE,
            "Validity": { "Value": 3650, "Type": "DAYS" }
        }),
    )
    .await;
    assert_eq!(status, 200, "self-sign root: {issued}");
    let cert_arn = issued["CertificateArn"].as_str().unwrap().to_string();
    let (_, got) = pca(
        server,
        "GetCertificate",
        json!({ "CertificateAuthorityArn": arn, "CertificateArn": cert_arn }),
    )
    .await;
    let root_cert = got["Certificate"].as_str().unwrap().to_string();

    // Import the self-signed root -> ACTIVE.
    let (status, imp) = pca(
        server,
        "ImportCertificateAuthorityCertificate",
        json!({ "CertificateAuthorityArn": arn, "Certificate": root_cert }),
    )
    .await;
    assert_eq!(status, 200, "import root: {imp}");
    arn
}

/// Build a real end-entity CSR PEM.
fn client_csr(cn: &str) -> String {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let mut params = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
    let mut dn = rcgen::DistinguishedName::new();
    dn.push(rcgen::DnType::CommonName, cn);
    params.distinguished_name = dn;
    params.serialize_request(&key).unwrap().pem().unwrap()
}

#[test_action("acm-pca", "CreateCertificateAuthority", checksum = "7c2ac163")]
#[tokio::test]
async fn create_certificate_authority() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    assert!(arn.contains(":certificate-authority/"));
}

#[test_action("acm-pca", "DescribeCertificateAuthority", checksum = "07a2f93c")]
#[tokio::test]
async fn describe_certificate_authority() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let (status, body) = pca(
        &server,
        "DescribeCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["CertificateAuthority"]["Status"], "ACTIVE");
}

#[test_action("acm-pca", "ListCertificateAuthorities", checksum = "1bd114fa")]
#[tokio::test]
async fn list_certificate_authorities() {
    let server = TestServer::start().await;
    make_root_ca(&server).await;
    let (status, body) = pca(&server, "ListCertificateAuthorities", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["CertificateAuthorities"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test_action("acm-pca", "UpdateCertificateAuthority", checksum = "f08b8aae")]
#[tokio::test]
async fn update_certificate_authority() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let (status, body) = pca(
        &server,
        "UpdateCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn, "Status": "DISABLED" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("acm-pca", "DeleteCertificateAuthority", checksum = "b8cc7da3")]
#[tokio::test]
async fn delete_certificate_authority() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    // Must be disabled first.
    pca(
        &server,
        "UpdateCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn, "Status": "DISABLED" }),
    )
    .await;
    let (status, body) = pca(
        &server,
        "DeleteCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn, "PermanentDeletionTimeInDays": 7 }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("acm-pca", "RestoreCertificateAuthority", checksum = "47273174")]
#[tokio::test]
async fn restore_certificate_authority() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    pca(
        &server,
        "UpdateCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn, "Status": "DISABLED" }),
    )
    .await;
    pca(
        &server,
        "DeleteCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn }),
    )
    .await;
    let (status, body) = pca(
        &server,
        "RestoreCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("acm-pca", "GetCertificateAuthorityCertificate", checksum = "f1f8ced9")]
#[tokio::test]
async fn get_certificate_authority_certificate() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let (status, body) = pca(
        &server,
        "GetCertificateAuthorityCertificate",
        json!({ "CertificateAuthorityArn": arn }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body["Certificate"]
        .as_str()
        .unwrap()
        .contains("BEGIN CERTIFICATE"));
}

#[test_action("acm-pca", "GetCertificateAuthorityCsr", checksum = "81506c65")]
#[tokio::test]
async fn get_certificate_authority_csr() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let (status, body) = pca(
        &server,
        "GetCertificateAuthorityCsr",
        json!({ "CertificateAuthorityArn": arn }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body["Csr"]
        .as_str()
        .unwrap()
        .contains("CERTIFICATE REQUEST"));
}

#[test_action(
    "acm-pca",
    "ImportCertificateAuthorityCertificate",
    checksum = "41e033a7"
)]
#[tokio::test]
async fn import_certificate_authority_certificate() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    // Re-import the CA's own certificate (round-trips through the import path).
    let (_, cert_body) = pca(
        &server,
        "GetCertificateAuthorityCertificate",
        json!({ "CertificateAuthorityArn": arn }),
    )
    .await;
    let cert = cert_body["Certificate"].as_str().unwrap();
    let (status, body) = pca(
        &server,
        "ImportCertificateAuthorityCertificate",
        json!({ "CertificateAuthorityArn": arn, "Certificate": cert }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("acm-pca", "IssueCertificate", checksum = "04918dc2")]
#[tokio::test]
async fn issue_certificate() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let csr = client_csr("leaf.example.com");
    let (status, body) = pca(
        &server,
        "IssueCertificate",
        json!({
            "CertificateAuthorityArn": arn,
            "Csr": csr,
            "SigningAlgorithm": "SHA256WITHECDSA",
            "Validity": { "Value": 365, "Type": "DAYS" }
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body["CertificateArn"]
        .as_str()
        .unwrap()
        .contains("/certificate/"));
}

#[test_action("acm-pca", "GetCertificate", checksum = "884573b3")]
#[tokio::test]
async fn get_certificate() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let csr = client_csr("leaf2.example.com");
    let (_, issued) = pca(
        &server,
        "IssueCertificate",
        json!({
            "CertificateAuthorityArn": arn,
            "Csr": csr,
            "SigningAlgorithm": "SHA256WITHECDSA",
            "Validity": { "Value": 365, "Type": "DAYS" }
        }),
    )
    .await;
    let cert_arn = issued["CertificateArn"].as_str().unwrap();
    let (status, body) = pca(
        &server,
        "GetCertificate",
        json!({ "CertificateAuthorityArn": arn, "CertificateArn": cert_arn }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body["Certificate"]
        .as_str()
        .unwrap()
        .contains("BEGIN CERTIFICATE"));
}

#[test_action("acm-pca", "RevokeCertificate", checksum = "76e1e1ef")]
#[tokio::test]
async fn revoke_certificate() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let csr = client_csr("leaf3.example.com");
    let (_, issued) = pca(
        &server,
        "IssueCertificate",
        json!({
            "CertificateAuthorityArn": arn,
            "Csr": csr,
            "SigningAlgorithm": "SHA256WITHECDSA",
            "Validity": { "Value": 365, "Type": "DAYS" }
        }),
    )
    .await;
    let cert_arn = issued["CertificateArn"].as_str().unwrap();
    let serial = cert_arn.rsplit_once("/certificate/").unwrap().1;
    let (status, body) = pca(
        &server,
        "RevokeCertificate",
        json!({
            "CertificateAuthorityArn": arn,
            "CertificateSerial": serial,
            "RevocationReason": "KEY_COMPROMISE"
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("acm-pca", "TagCertificateAuthority", checksum = "046d3b57")]
#[tokio::test]
async fn tag_certificate_authority() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let (status, body) = pca(
        &server,
        "TagCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn, "Tags": [{ "Key": "env", "Value": "test" }] }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("acm-pca", "ListTags", checksum = "68ed7cf2")]
#[tokio::test]
async fn list_tags() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    pca(
        &server,
        "TagCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn, "Tags": [{ "Key": "env", "Value": "test" }] }),
    )
    .await;
    let (status, body) = pca(
        &server,
        "ListTags",
        json!({ "CertificateAuthorityArn": arn }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["Tags"][0]["Key"], "env");
}

#[test_action("acm-pca", "UntagCertificateAuthority", checksum = "bd1728c0")]
#[tokio::test]
async fn untag_certificate_authority() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    pca(
        &server,
        "TagCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn, "Tags": [{ "Key": "env", "Value": "test" }] }),
    )
    .await;
    let (status, body) = pca(
        &server,
        "UntagCertificateAuthority",
        json!({ "CertificateAuthorityArn": arn, "Tags": [{ "Key": "env" }] }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("acm-pca", "CreatePermission", checksum = "0c8b34d2")]
#[tokio::test]
async fn create_permission() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let (status, body) = pca(
        &server,
        "CreatePermission",
        json!({
            "CertificateAuthorityArn": arn,
            "Principal": "acm.amazonaws.com",
            "Actions": ["IssueCertificate", "GetCertificate", "ListPermissions"]
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("acm-pca", "ListPermissions", checksum = "64ea11a7")]
#[tokio::test]
async fn list_permissions() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    pca(
        &server,
        "CreatePermission",
        json!({
            "CertificateAuthorityArn": arn,
            "Principal": "acm.amazonaws.com",
            "Actions": ["IssueCertificate"]
        }),
    )
    .await;
    let (status, body) = pca(
        &server,
        "ListPermissions",
        json!({ "CertificateAuthorityArn": arn }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["Permissions"][0]["Principal"], "acm.amazonaws.com");
}

#[test_action("acm-pca", "DeletePermission", checksum = "7c038f64")]
#[tokio::test]
async fn delete_permission() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    pca(
        &server,
        "CreatePermission",
        json!({
            "CertificateAuthorityArn": arn,
            "Principal": "acm.amazonaws.com",
            "Actions": ["IssueCertificate"]
        }),
    )
    .await;
    let (status, body) = pca(
        &server,
        "DeletePermission",
        json!({ "CertificateAuthorityArn": arn, "Principal": "acm.amazonaws.com" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("acm-pca", "PutPolicy", checksum = "655bbc65")]
#[tokio::test]
async fn put_policy() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{ "Effect": "Allow", "Principal": { "AWS": "*" }, "Action": "acm-pca:IssueCertificate", "Resource": arn }]
    })
    .to_string();
    let (status, body) = pca(
        &server,
        "PutPolicy",
        json!({ "ResourceArn": arn, "Policy": policy }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("acm-pca", "GetPolicy", checksum = "e7f2d2c4")]
#[tokio::test]
async fn get_policy() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let policy = json!({ "Version": "2012-10-17", "Statement": [] }).to_string();
    pca(
        &server,
        "PutPolicy",
        json!({ "ResourceArn": arn, "Policy": policy }),
    )
    .await;
    let (status, body) = pca(&server, "GetPolicy", json!({ "ResourceArn": arn })).await;
    assert_eq!(status, 200, "{body}");
    assert!(body["Policy"].as_str().unwrap().contains("2012-10-17"));
}

#[test_action("acm-pca", "DeletePolicy", checksum = "e9fb1277")]
#[tokio::test]
async fn delete_policy() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let policy = json!({ "Version": "2012-10-17", "Statement": [] }).to_string();
    pca(
        &server,
        "PutPolicy",
        json!({ "ResourceArn": arn, "Policy": policy }),
    )
    .await;
    let (status, body) = pca(&server, "DeletePolicy", json!({ "ResourceArn": arn })).await;
    assert_eq!(status, 200, "{body}");
}

#[test_action(
    "acm-pca",
    "CreateCertificateAuthorityAuditReport",
    checksum = "3b085912"
)]
#[tokio::test]
async fn create_certificate_authority_audit_report() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let (status, body) = pca(
        &server,
        "CreateCertificateAuthorityAuditReport",
        json!({
            "CertificateAuthorityArn": arn,
            "S3BucketName": "conf-audit-bucket",
            "AuditReportResponseFormat": "JSON"
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body["AuditReportId"].as_str().is_some());
}

#[test_action(
    "acm-pca",
    "DescribeCertificateAuthorityAuditReport",
    checksum = "ae95a061"
)]
#[tokio::test]
async fn describe_certificate_authority_audit_report() {
    let server = TestServer::start().await;
    let arn = make_root_ca(&server).await;
    let (_, created) = pca(
        &server,
        "CreateCertificateAuthorityAuditReport",
        json!({
            "CertificateAuthorityArn": arn,
            "S3BucketName": "conf-audit-bucket",
            "AuditReportResponseFormat": "JSON"
        }),
    )
    .await;
    let report_id = created["AuditReportId"].as_str().unwrap();
    let (status, body) = pca(
        &server,
        "DescribeCertificateAuthorityAuditReport",
        json!({ "CertificateAuthorityArn": arn, "AuditReportId": report_id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["AuditReportStatus"], "SUCCESS");
}
