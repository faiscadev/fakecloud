//! End-to-end CRUD + lifecycle tests for the Transfer Family service, driving
//! the public `AwsService::handle` entry point with hand-built awsJson1.1
//! requests.

use std::collections::HashMap;

use bytes::Bytes;
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};

use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsService};
use fakecloud_transfer::TransferService;

fn svc() -> TransferService {
    TransferService::new(std::sync::Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    ))))
}

fn request(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "transfer".into(),
        action: action.into(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        request_id: "req".into(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: Mutex::new(None),
        path_segments: vec![],
        raw_path: String::new(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

async fn call(s: &TransferService, action: &str, body: Value) -> Value {
    let resp = s.handle(request(action, body)).await.expect("op ok");
    assert!(resp.status.is_success(), "{action} should succeed");
    let bytes = resp.body.expect_bytes();
    if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(bytes).unwrap()
    }
}

async fn err_code(s: &TransferService, action: &str, body: Value) -> String {
    let e = s
        .handle(request(action, body))
        .await
        .err()
        .expect("expected error");
    e.code().to_string()
}

async fn err_status(s: &TransferService, action: &str, body: Value) -> u16 {
    let e = s
        .handle(request(action, body))
        .await
        .err()
        .expect("expected error");
    e.status().as_u16()
}

async fn new_server(s: &TransferService) -> String {
    call(
        s,
        "CreateServer",
        json!({ "IdentityProviderType": "SERVICE_MANAGED" }),
    )
    .await["ServerId"]
        .as_str()
        .unwrap()
        .to_string()
}

const ROLE: &str = "arn:aws:iam::000000000000:role/transfer-role";

/// A real self-signed ECDSA P-256 certificate (serial 0x3039), used to exercise
/// the PEM-parsing path in `ImportCertificate`.
const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBfzCCASWgAwIBAgICMDkwCgYIKoZIzj0EAwIwHjEcMBoGA1UEAwwTZmFrZWNs\n\
b3VkLXRlc3QtY2VydDAeFw0yNjA3MDQwNDMyNDBaFw0zNjA3MDEwNDMyNDBaMB4x\n\
HDAaBgNVBAMME2Zha2VjbG91ZC10ZXN0LWNlcnQwWTATBgcqhkjOPQIBBggqhkjO\n\
PQMBBwNCAAR8LePc+d6fQ07Gd8HC18k6FdRwW2uBUzceP0iwL2O9Hh7bjacYNJPf\n\
FelbZTDBUUaAjnj7s7Uo4fLUGpa03pADo1MwUTAdBgNVHQ4EFgQUQUA6HL+QbCOx\n\
mRXO4LoqwQMxvSswHwYDVR0jBBgwFoAUQUA6HL+QbCOxmRXO4LoqwQMxvSswDwYD\n\
VR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiEAlpmlaZLCBcJYDs+j3B47\n\
6J3PptcVgoSmAetCad/w1uQCIHFdmHdYR2QBGis74scDSWqLR/DAqhpQtPPFpD9o\n\
Lljq\n\
-----END CERTIFICATE-----\n";

#[tokio::test]
async fn server_crud_and_state_transitions() {
    let s = svc();
    let server_id = new_server(&s).await;
    assert!(server_id.starts_with("s-") && server_id.len() == 19);

    let desc = call(&s, "DescribeServer", json!({ "ServerId": server_id })).await;
    assert_eq!(desc["Server"]["State"], json!("ONLINE"));
    assert_eq!(desc["Server"]["ServerId"], json!(server_id));
    assert!(desc["Server"]["Arn"].as_str().unwrap().contains(":server/"));

    // Update persists.
    call(
        &s,
        "UpdateServer",
        json!({ "ServerId": server_id, "LoggingRole": ROLE }),
    )
    .await;
    let desc = call(&s, "DescribeServer", json!({ "ServerId": server_id })).await;
    assert_eq!(desc["Server"]["LoggingRole"], json!(ROLE));

    // Stop -> OFFLINE, Start -> ONLINE, reflected synchronously.
    call(&s, "StopServer", json!({ "ServerId": server_id })).await;
    let desc = call(&s, "DescribeServer", json!({ "ServerId": server_id })).await;
    assert_eq!(desc["Server"]["State"], json!("OFFLINE"));
    call(&s, "StartServer", json!({ "ServerId": server_id })).await;
    let desc = call(&s, "DescribeServer", json!({ "ServerId": server_id })).await;
    assert_eq!(desc["Server"]["State"], json!("ONLINE"));

    // List shows it.
    let list = call(&s, "ListServers", json!({})).await;
    assert_eq!(list["Servers"].as_array().unwrap().len(), 1);

    // Delete removes it.
    call(&s, "DeleteServer", json!({ "ServerId": server_id })).await;
    assert_eq!(
        err_code(&s, "DescribeServer", json!({ "ServerId": server_id })).await,
        "ResourceNotFoundException"
    );
    let list = call(&s, "ListServers", json!({})).await;
    assert!(list["Servers"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn user_and_ssh_key_lifecycle() {
    let s = svc();
    let server_id = new_server(&s).await;
    let created = call(
        &s,
        "CreateUser",
        json!({ "ServerId": server_id, "UserName": "alice", "Role": ROLE, "HomeDirectory": "/home/alice" }),
    )
    .await;
    assert_eq!(created["UserName"], json!("alice"));

    let desc = call(
        &s,
        "DescribeUser",
        json!({ "ServerId": server_id, "UserName": "alice" }),
    )
    .await;
    assert_eq!(desc["User"]["HomeDirectory"], json!("/home/alice"));
    assert_eq!(desc["ServerId"], json!(server_id));

    // Import an SSH public key; it surfaces in describe + the count in list.
    let key = call(
        &s,
        "ImportSshPublicKey",
        json!({ "ServerId": server_id, "UserName": "alice", "SshPublicKeyBody": "ssh-rsa AAAAB3..." }),
    )
    .await;
    let key_id = key["SshPublicKeyId"].as_str().unwrap().to_string();
    let desc = call(
        &s,
        "DescribeUser",
        json!({ "ServerId": server_id, "UserName": "alice" }),
    )
    .await;
    assert_eq!(desc["User"]["SshPublicKeys"].as_array().unwrap().len(), 1);
    let list = call(&s, "ListUsers", json!({ "ServerId": server_id })).await;
    assert_eq!(list["Users"][0]["SshPublicKeyCount"], json!(1));

    call(
        &s,
        "DeleteSshPublicKey",
        json!({ "ServerId": server_id, "UserName": "alice", "SshPublicKeyId": key_id }),
    )
    .await;
    let desc = call(
        &s,
        "DescribeUser",
        json!({ "ServerId": server_id, "UserName": "alice" }),
    )
    .await;
    assert_eq!(desc["User"]["SshPublicKeys"].as_array().unwrap().len(), 0);

    // DescribeServer reflects the live user count.
    let srv = call(&s, "DescribeServer", json!({ "ServerId": server_id })).await;
    assert_eq!(srv["Server"]["UserCount"], json!(1));

    call(
        &s,
        "DeleteUser",
        json!({ "ServerId": server_id, "UserName": "alice" }),
    )
    .await;
    assert_eq!(
        err_code(
            &s,
            "DescribeUser",
            json!({ "ServerId": server_id, "UserName": "alice" })
        )
        .await,
        "ResourceNotFoundException"
    );
}

#[tokio::test]
async fn host_key_crud() {
    let s = svc();
    let server_id = new_server(&s).await;
    let hk = call(
        &s,
        "ImportHostKey",
        json!({ "ServerId": server_id, "HostKeyBody": "-----BEGIN...", "Description": "primary" }),
    )
    .await;
    let host_key_id = hk["HostKeyId"].as_str().unwrap().to_string();
    assert!(host_key_id.starts_with("hostkey-"));
    call(
        &s,
        "UpdateHostKey",
        json!({ "ServerId": server_id, "HostKeyId": host_key_id, "Description": "rotated" }),
    )
    .await;
    let desc = call(
        &s,
        "DescribeHostKey",
        json!({ "ServerId": server_id, "HostKeyId": host_key_id }),
    )
    .await;
    assert_eq!(desc["HostKey"]["Description"], json!("rotated"));
    let list = call(&s, "ListHostKeys", json!({ "ServerId": server_id })).await;
    assert_eq!(list["HostKeys"].as_array().unwrap().len(), 1);
    call(
        &s,
        "DeleteHostKey",
        json!({ "ServerId": server_id, "HostKeyId": host_key_id }),
    )
    .await;
    let list = call(&s, "ListHostKeys", json!({ "ServerId": server_id })).await;
    assert!(list["HostKeys"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn access_crud() {
    let s = svc();
    let server_id = new_server(&s).await;
    let external_id = "S-1-1-12-1234567890-123456789-1234567890-1234";
    call(
        &s,
        "CreateAccess",
        json!({ "ServerId": server_id, "ExternalId": external_id, "Role": ROLE, "HomeDirectory": "/data" }),
    )
    .await;
    call(
        &s,
        "UpdateAccess",
        json!({ "ServerId": server_id, "ExternalId": external_id, "HomeDirectory": "/data2" }),
    )
    .await;
    let desc = call(
        &s,
        "DescribeAccess",
        json!({ "ServerId": server_id, "ExternalId": external_id }),
    )
    .await;
    assert_eq!(desc["Access"]["HomeDirectory"], json!("/data2"));
    assert_eq!(desc["ServerId"], json!(server_id));
    let list = call(&s, "ListAccesses", json!({ "ServerId": server_id })).await;
    assert_eq!(list["Accesses"][0]["ExternalId"], json!(external_id));
    call(
        &s,
        "DeleteAccess",
        json!({ "ServerId": server_id, "ExternalId": external_id }),
    )
    .await;
    let list = call(&s, "ListAccesses", json!({ "ServerId": server_id })).await;
    assert!(list["Accesses"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn workflow_and_agreement() {
    let s = svc();
    let server_id = new_server(&s).await;
    let wf = call(
        &s,
        "CreateWorkflow",
        json!({ "Description": "copy", "Steps": [{ "Type": "COPY" }] }),
    )
    .await;
    let workflow_id = wf["WorkflowId"].as_str().unwrap().to_string();
    assert!(workflow_id.starts_with("w-"));
    let desc = call(&s, "DescribeWorkflow", json!({ "WorkflowId": workflow_id })).await;
    assert_eq!(desc["Workflow"]["Steps"].as_array().unwrap().len(), 1);
    assert_eq!(
        call(&s, "ListWorkflows", json!({})).await["Workflows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // Execution lookups on a missing execution 404.
    assert_eq!(
        err_code(
            &s,
            "DescribeExecution",
            json!({ "WorkflowId": workflow_id, "ExecutionId": "12345678-1234-1234-1234-123456789012" })
        )
        .await,
        "ResourceNotFoundException"
    );

    let ag = call(
        &s,
        "CreateAgreement",
        json!({ "ServerId": server_id, "LocalProfileId": "p-11111111111111111", "PartnerProfileId": "p-22222222222222222", "AccessRole": ROLE }),
    )
    .await;
    let agreement_id = ag["AgreementId"].as_str().unwrap().to_string();
    assert!(agreement_id.starts_with("a-"));
    let desc = call(
        &s,
        "DescribeAgreement",
        json!({ "ServerId": server_id, "AgreementId": agreement_id }),
    )
    .await;
    assert_eq!(desc["Agreement"]["Status"], json!("ACTIVE"));

    call(&s, "DeleteWorkflow", json!({ "WorkflowId": workflow_id })).await;
    call(
        &s,
        "DeleteAgreement",
        json!({ "ServerId": server_id, "AgreementId": agreement_id }),
    )
    .await;
}

#[tokio::test]
async fn connector_and_transfers() {
    let s = svc();
    let c = call(
        &s,
        "CreateConnector",
        json!({ "Url": "sftp://partner.example.com", "AccessRole": ROLE, "SftpConfig": { "UserSecretId": "secret" } }),
    )
    .await;
    let connector_id = c["ConnectorId"].as_str().unwrap().to_string();
    assert!(connector_id.starts_with("c-"));

    let desc = call(
        &s,
        "DescribeConnector",
        json!({ "ConnectorId": connector_id }),
    )
    .await;
    assert_eq!(desc["Connector"]["Status"], json!("ACTIVE"));
    assert_eq!(desc["Connector"]["EgressType"], json!("SERVICE_MANAGED"));
    assert_eq!(
        desc["Connector"]["Url"],
        json!("sftp://partner.example.com")
    );

    let tc = call(&s, "TestConnection", json!({ "ConnectorId": connector_id })).await;
    assert_eq!(tc["Status"], json!("OK"));

    let ft = call(
        &s,
        "StartFileTransfer",
        json!({ "ConnectorId": connector_id, "SendFilePaths": ["/local/a.txt"] }),
    )
    .await;
    let transfer_id = ft["TransferId"].as_str().unwrap().to_string();
    let results = call(
        &s,
        "ListFileTransferResults",
        json!({ "ConnectorId": connector_id, "TransferId": transfer_id }),
    )
    .await;
    let arr = results["FileTransferResults"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["FilePath"], json!("/local/a.txt"));
    assert_eq!(arr[0]["StatusCode"], json!("COMPLETED"));

    // Remote actions return ids against an existing connector.
    assert!(call(
        &s,
        "StartRemoteDelete",
        json!({ "ConnectorId": connector_id, "DeletePath": "/remote/x" })
    )
    .await["DeleteId"]
        .is_string());
    assert!(call(
        &s,
        "StartRemoteMove",
        json!({ "ConnectorId": connector_id, "SourcePath": "/a", "TargetPath": "/b" })
    )
    .await["MoveId"]
        .is_string());
    assert!(call(&s, "StartDirectoryListing", json!({ "ConnectorId": connector_id, "RemoteDirectoryPath": "/", "OutputDirectoryPath": "/out" })).await["ListingId"].is_string());

    call(
        &s,
        "DeleteConnector",
        json!({ "ConnectorId": connector_id }),
    )
    .await;
}

#[tokio::test]
async fn profiles_and_certificates() {
    let s = svc();
    let p = call(
        &s,
        "CreateProfile",
        json!({ "As2Id": "MYID", "ProfileType": "LOCAL" }),
    )
    .await;
    let profile_id = p["ProfileId"].as_str().unwrap().to_string();
    assert!(profile_id.starts_with("p-"));
    let desc = call(&s, "DescribeProfile", json!({ "ProfileId": profile_id })).await;
    assert_eq!(desc["Profile"]["As2Id"], json!("MYID"));
    let list = call(&s, "ListProfiles", json!({ "ProfileType": "LOCAL" })).await;
    assert_eq!(list["Profiles"].as_array().unwrap().len(), 1);
    // Filtering by the other type excludes it.
    let list = call(&s, "ListProfiles", json!({ "ProfileType": "PARTNER" })).await;
    assert!(list["Profiles"].as_array().unwrap().is_empty());

    let cert = call(
        &s,
        "ImportCertificate",
        json!({ "Usage": "SIGNING", "Certificate": "-----BEGIN CERTIFICATE-----", "Description": "signer" }),
    )
    .await;
    let certificate_id = cert["CertificateId"].as_str().unwrap().to_string();
    assert!(certificate_id.starts_with("cert-"));
    let desc = call(
        &s,
        "DescribeCertificate",
        json!({ "CertificateId": certificate_id }),
    )
    .await;
    assert_eq!(desc["Certificate"]["Usage"], json!("SIGNING"));
    assert_eq!(
        call(&s, "ListCertificates", json!({})).await["Certificates"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    call(&s, "DeleteProfile", json!({ "ProfileId": profile_id })).await;
    call(
        &s,
        "DeleteCertificate",
        json!({ "CertificateId": certificate_id }),
    )
    .await;
}

#[tokio::test]
async fn security_policies_and_web_apps() {
    let s = svc();
    let list = call(&s, "ListSecurityPolicies", json!({})).await;
    let names = list["SecurityPolicyNames"].as_array().unwrap();
    assert!(names.iter().any(|n| n == "TransferSecurityPolicy-2018-11"));
    let desc = call(
        &s,
        "DescribeSecurityPolicy",
        json!({ "SecurityPolicyName": "TransferSecurityPolicy-FIPS-2023-05" }),
    )
    .await;
    assert_eq!(desc["SecurityPolicy"]["Fips"], json!(true));
    assert_eq!(
        err_code(
            &s,
            "DescribeSecurityPolicy",
            json!({ "SecurityPolicyName": "TransferSecurityPolicy-9999-99" })
        )
        .await,
        "ResourceNotFoundException"
    );

    let w = call(
        &s,
        "CreateWebApp",
        json!({ "IdentityProviderDetails": { "IdentityCenterConfig": { "InstanceArn": "arn:aws:sso:::instance/ssoins-1" } } }),
    )
    .await;
    let web_app_id = w["WebAppId"].as_str().unwrap().to_string();
    assert!(web_app_id.starts_with("webapp-"));
    let desc = call(&s, "DescribeWebApp", json!({ "WebAppId": web_app_id })).await;
    assert_eq!(desc["WebApp"]["WebAppId"], json!(web_app_id));
    call(
        &s,
        "UpdateWebAppCustomization",
        json!({ "WebAppId": web_app_id, "Title": "My Portal" }),
    )
    .await;
    let cust = call(
        &s,
        "DescribeWebAppCustomization",
        json!({ "WebAppId": web_app_id }),
    )
    .await;
    assert_eq!(cust["WebAppCustomization"]["Title"], json!("My Portal"));
    call(&s, "DeleteWebApp", json!({ "WebAppId": web_app_id })).await;
}

#[tokio::test]
async fn tagging_round_trip() {
    let s = svc();
    let server_id = new_server(&s).await;
    let arn = call(&s, "DescribeServer", json!({ "ServerId": server_id })).await["Server"]["Arn"]
        .as_str()
        .unwrap()
        .to_string();
    call(
        &s,
        "TagResource",
        json!({ "Arn": arn, "Tags": [{ "Key": "env", "Value": "prod" }] }),
    )
    .await;
    let tags = call(&s, "ListTagsForResource", json!({ "Arn": arn })).await;
    assert_eq!(tags["Tags"][0]["Key"], json!("env"));
    call(
        &s,
        "UntagResource",
        json!({ "Arn": arn, "TagKeys": ["env"] }),
    )
    .await;
    let tags = call(&s, "ListTagsForResource", json!({ "Arn": arn })).await;
    assert!(tags["Tags"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn error_codes() {
    let s = svc();
    // Missing parent server -> ResourceNotFoundException.
    assert_eq!(
        err_code(
            &s,
            "CreateUser",
            json!({ "ServerId": "s-0000000000000000a", "UserName": "bob", "Role": ROLE })
        )
        .await,
        "ResourceNotFoundException"
    );
    // Missing required field -> InvalidRequestException.
    assert_eq!(
        err_code(&s, "CreateConnector", json!({})).await,
        "InvalidRequestException"
    );
    // Invalid enum value -> InvalidRequestException (model-derived validation).
    assert_eq!(
        err_code(&s, "CreateServer", json!({ "Domain": "GLACIER" })).await,
        "InvalidRequestException"
    );
    // Too-long string -> InvalidRequestException.
    let long = "x".repeat(300);
    assert_eq!(
        err_code(
            &s,
            "CreateConnector",
            json!({ "AccessRole": ROLE, "Url": long })
        )
        .await,
        "InvalidRequestException"
    );
}

#[tokio::test]
async fn duplicate_server_children_conflict() {
    let s = svc();
    let server_id = new_server(&s).await;
    call(
        &s,
        "CreateUser",
        json!({ "ServerId": server_id, "UserName": "carol", "Role": ROLE }),
    )
    .await;
    assert_eq!(
        err_code(
            &s,
            "CreateUser",
            json!({ "ServerId": server_id, "UserName": "carol", "Role": ROLE })
        )
        .await,
        "ResourceExistsException"
    );
}

// ===================== regression tests (post-merge bug-hunt) =====================

/// Fix 1: a duplicate create returns HTTP 409 (ResourceExistsException maps to
/// `httpError: 409` in the model), not 400.
#[tokio::test]
async fn resource_exists_returns_409() {
    let s = svc();
    let server_id = new_server(&s).await;
    call(
        &s,
        "CreateUser",
        json!({ "ServerId": server_id, "UserName": "dup", "Role": ROLE }),
    )
    .await;
    let status = err_status(
        &s,
        "CreateUser",
        json!({ "ServerId": server_id, "UserName": "dup", "Role": ROLE }),
    )
    .await;
    assert_eq!(status, 409);
}

/// Fix 2: a server created without `Protocols` still reports the AWS default
/// `["SFTP"]` on describe, avoiding terraform drift.
#[tokio::test]
async fn server_protocols_default_to_sftp() {
    let s = svc();
    let server_id = new_server(&s).await;
    let desc = call(&s, "DescribeServer", json!({ "ServerId": server_id })).await;
    assert_eq!(desc["Server"]["Protocols"], json!(["SFTP"]));

    // An explicit value is preserved.
    let sid2 = call(
        &s,
        "CreateServer",
        json!({ "IdentityProviderType": "SERVICE_MANAGED", "Protocols": ["FTP", "FTPS"] }),
    )
    .await["ServerId"]
        .as_str()
        .unwrap()
        .to_string();
    let desc2 = call(&s, "DescribeServer", json!({ "ServerId": sid2 })).await;
    assert_eq!(desc2["Server"]["Protocols"], json!(["FTP", "FTPS"]));
}

/// Fix 3: the host-key `Type` is derived from the key material, not hardcoded.
#[tokio::test]
async fn host_key_type_derived_from_body() {
    let s = svc();
    let server_id = new_server(&s).await;

    let cases = [
        (
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI... user@host",
            "ssh-ed25519",
        ),
        (
            "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTY... user@host",
            "ecdsa-sha2-nistp256",
        ),
        ("ssh-rsa AAAAB3NzaC1yc2EAAAADAQAB... user@host", "ssh-rsa"),
    ];
    for (body, expected) in cases {
        let hk = call(
            &s,
            "ImportHostKey",
            json!({ "ServerId": server_id, "HostKeyBody": body }),
        )
        .await;
        let host_key_id = hk["HostKeyId"].as_str().unwrap().to_string();
        let desc = call(
            &s,
            "DescribeHostKey",
            json!({ "ServerId": server_id, "HostKeyId": host_key_id }),
        )
        .await;
        assert_eq!(desc["HostKey"]["Type"], json!(expected), "body: {body}");
    }
}

/// Fix 4: deleting a server also removes the tag entries of its cascaded
/// children so no orphaned tags linger.
#[tokio::test]
async fn delete_server_removes_child_tags() {
    let s = svc();
    let server_id = new_server(&s).await;
    call(
        &s,
        "CreateUser",
        json!({ "ServerId": server_id, "UserName": "tagged", "Role": ROLE,
                "Tags": [{ "Key": "team", "Value": "ops" }] }),
    )
    .await;
    let user_arn = call(
        &s,
        "DescribeUser",
        json!({ "ServerId": server_id, "UserName": "tagged" }),
    )
    .await["User"]["Arn"]
        .as_str()
        .unwrap()
        .to_string();
    // Tags are visible before deletion.
    let tags = call(&s, "ListTagsForResource", json!({ "Arn": user_arn })).await;
    assert_eq!(tags["Tags"].as_array().unwrap().len(), 1);

    call(&s, "DeleteServer", json!({ "ServerId": server_id })).await;

    // After cascade the child's tags are gone.
    let tags = call(&s, "ListTagsForResource", json!({ "Arn": user_arn })).await;
    assert!(tags["Tags"].as_array().unwrap().is_empty());
}

/// Fix 5: UpdateServer must ignore the create-only `Domain` member.
#[tokio::test]
async fn update_server_ignores_domain() {
    let s = svc();
    let server_id = new_server(&s).await;
    let before = call(&s, "DescribeServer", json!({ "ServerId": server_id })).await;
    assert_eq!(before["Server"]["Domain"], json!("S3"));

    call(
        &s,
        "UpdateServer",
        json!({ "ServerId": server_id, "Domain": "EFS", "LoggingRole": ROLE }),
    )
    .await;
    let after = call(&s, "DescribeServer", json!({ "ServerId": server_id })).await;
    // Domain is unchanged; the legitimate LoggingRole update still applied.
    assert_eq!(after["Server"]["Domain"], json!("S3"));
    assert_eq!(after["Server"]["LoggingRole"], json!(ROLE));
}

/// Fix 6: ListTagsForResource honours MaxResults/NextToken.
#[tokio::test]
async fn list_tags_paginates() {
    let s = svc();
    let server_id = new_server(&s).await;
    let arn = call(&s, "DescribeServer", json!({ "ServerId": server_id })).await["Server"]["Arn"]
        .as_str()
        .unwrap()
        .to_string();
    call(
        &s,
        "TagResource",
        json!({ "Arn": arn, "Tags": [
            { "Key": "a", "Value": "1" },
            { "Key": "b", "Value": "2" },
            { "Key": "c", "Value": "3" }
        ] }),
    )
    .await;

    let page = call(
        &s,
        "ListTagsForResource",
        json!({ "Arn": arn, "MaxResults": 2 }),
    )
    .await;
    assert_eq!(page["Tags"].as_array().unwrap().len(), 2);
    let next = page["NextToken"]
        .as_str()
        .expect("NextToken present")
        .to_string();

    let page2 = call(
        &s,
        "ListTagsForResource",
        json!({ "Arn": arn, "MaxResults": 2, "NextToken": next }),
    )
    .await;
    assert_eq!(page2["Tags"].as_array().unwrap().len(), 1);
    assert!(page2.get("NextToken").is_none());
}

/// Fix 8: importing a real PEM certificate populates the computed
/// Serial/NotBeforeDate/NotAfterDate fields; a non-PEM body leaves them unset.
#[tokio::test]
async fn certificate_dates_populated_from_pem() {
    let s = svc();
    let pem = TEST_CERT_PEM;
    let cert = call(
        &s,
        "ImportCertificate",
        json!({ "Usage": "SIGNING", "Certificate": pem }),
    )
    .await;
    let certificate_id = cert["CertificateId"].as_str().unwrap().to_string();
    let desc = call(
        &s,
        "DescribeCertificate",
        json!({ "CertificateId": certificate_id }),
    )
    .await["Certificate"]
        .clone();
    assert!(
        desc.get("Serial").is_some(),
        "Serial should be populated: {desc}"
    );
    assert!(desc.get("NotBeforeDate").is_some(), "NotBeforeDate missing");
    assert!(desc.get("NotAfterDate").is_some(), "NotAfterDate missing");
    // Dates are epoch-seconds numbers; NotAfter is after NotBefore.
    let nb = desc["NotBeforeDate"].as_f64().unwrap();
    let na = desc["NotAfterDate"].as_f64().unwrap();
    assert!(na > nb);

    // A non-PEM placeholder body leaves the computed fields unset (no panic).
    let cert2 = call(
        &s,
        "ImportCertificate",
        json!({ "Usage": "SIGNING", "Certificate": "-----BEGIN CERTIFICATE-----" }),
    )
    .await;
    let cid2 = cert2["CertificateId"].as_str().unwrap().to_string();
    let desc2 = call(&s, "DescribeCertificate", json!({ "CertificateId": cid2 })).await
        ["Certificate"]
        .clone();
    assert!(desc2.get("Serial").is_none());
}
