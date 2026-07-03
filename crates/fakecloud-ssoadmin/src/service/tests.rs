use super::*;
use bytes::Bytes;
use fakecloud_core::multi_account::MultiAccountState;
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;

fn svc() -> SsoAdminService {
    SsoAdminService::new(Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    ))))
}

fn req(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "sso".into(),
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

fn call(s: &SsoAdminService, action: &str, body: Value) -> Value {
    let resp = dispatch(s, &req(action, body)).expect("op ok");
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn err(s: &SsoAdminService, action: &str, body: Value) -> AwsServiceError {
    dispatch(s, &req(action, body))
        .err()
        .expect("expected error")
}

fn new_instance(s: &SsoAdminService) -> String {
    call(s, "CreateInstance", json!({ "Name": "corp" }))["InstanceArn"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn instance_lifecycle() {
    let s = svc();
    let arn = new_instance(&s);
    assert!(arn.contains(":instance/ssoins-"));
    let desc = call(&s, "DescribeInstance", json!({ "InstanceArn": arn }));
    assert_eq!(desc["Status"], json!("ACTIVE"));
    assert_eq!(desc["Name"], json!("corp"));
    let list = call(&s, "ListInstances", json!({}));
    assert_eq!(list["Instances"].as_array().unwrap().len(), 1);
    // InstanceMetadata carries Regions; DescribeInstanceResponse does not.
    assert!(list["Instances"][0].get("Regions").is_some());
    assert!(desc.get("Regions").is_none());
}

#[test]
fn describe_unknown_instance_auto_provisions() {
    // DescribeInstance declares no ResourceNotFoundException.
    let s = svc();
    let arn = "arn:aws:sso:::instance/ssoins-1234567890abcdef";
    let desc = call(&s, "DescribeInstance", json!({ "InstanceArn": arn }));
    assert_eq!(desc["InstanceArn"], json!(arn));
    assert_eq!(desc["Status"], json!("ACTIVE"));
}

#[test]
fn permission_set_crud() {
    let s = svc();
    let inst = new_instance(&s);
    let created = call(
        &s,
        "CreatePermissionSet",
        json!({ "InstanceArn": inst, "Name": "Admins", "SessionDuration": "PT1H" }),
    );
    let ps_arn = created["PermissionSet"]["PermissionSetArn"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(created["PermissionSet"]["Name"], json!("Admins"));
    assert_eq!(created["PermissionSet"]["SessionDuration"], json!("PT1H"));

    call(
        &s,
        "UpdatePermissionSet",
        json!({ "InstanceArn": inst, "PermissionSetArn": ps_arn, "RelayState": "https://x" }),
    );
    let desc = call(
        &s,
        "DescribePermissionSet",
        json!({ "InstanceArn": inst, "PermissionSetArn": ps_arn }),
    );
    assert_eq!(desc["PermissionSet"]["RelayState"], json!("https://x"));

    let list = call(&s, "ListPermissionSets", json!({ "InstanceArn": inst }));
    assert_eq!(list["PermissionSets"].as_array().unwrap().len(), 1);

    call(
        &s,
        "DeletePermissionSet",
        json!({ "InstanceArn": inst, "PermissionSetArn": ps_arn }),
    );
    let after = call(&s, "ListPermissionSets", json!({ "InstanceArn": inst }));
    assert_eq!(after["PermissionSets"].as_array().unwrap().len(), 0);
}

#[test]
fn inline_and_managed_policies() {
    let s = svc();
    let inst = new_instance(&s);
    let ps_arn = call(
        &s,
        "CreatePermissionSet",
        json!({ "InstanceArn": inst, "Name": "PS" }),
    )["PermissionSet"]["PermissionSetArn"]
        .as_str()
        .unwrap()
        .to_string();

    // Inline policy: unset resolves to empty string.
    let empty = call(
        &s,
        "GetInlinePolicyForPermissionSet",
        json!({ "InstanceArn": inst, "PermissionSetArn": ps_arn }),
    );
    assert_eq!(empty["InlinePolicy"], json!(""));
    call(
        &s,
        "PutInlinePolicyToPermissionSet",
        json!({ "InstanceArn": inst, "PermissionSetArn": ps_arn, "InlinePolicy": "{\"Version\":\"2012-10-17\"}" }),
    );
    let got = call(
        &s,
        "GetInlinePolicyForPermissionSet",
        json!({ "InstanceArn": inst, "PermissionSetArn": ps_arn }),
    );
    assert_eq!(got["InlinePolicy"], json!("{\"Version\":\"2012-10-17\"}"));

    // Managed policy attach + read.
    call(
        &s,
        "AttachManagedPolicyToPermissionSet",
        json!({ "InstanceArn": inst, "PermissionSetArn": ps_arn, "ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess" }),
    );
    let mp = call(
        &s,
        "ListManagedPoliciesInPermissionSet",
        json!({ "InstanceArn": inst, "PermissionSetArn": ps_arn }),
    );
    let arr = mp["AttachedManagedPolicies"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["Name"], json!("ReadOnlyAccess"));
}

#[test]
fn account_assignment_async_status_settles() {
    let s = svc();
    let inst = new_instance(&s);
    let ps_arn = call(
        &s,
        "CreatePermissionSet",
        json!({ "InstanceArn": inst, "Name": "PS" }),
    )["PermissionSet"]["PermissionSetArn"]
        .as_str()
        .unwrap()
        .to_string();
    let created = call(
        &s,
        "CreateAccountAssignment",
        json!({
            "InstanceArn": inst, "TargetId": "111122223333", "TargetType": "AWS_ACCOUNT",
            "PermissionSetArn": ps_arn, "PrincipalType": "USER",
            "PrincipalId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        }),
    );
    let status = &created["AccountAssignmentCreationStatus"];
    assert_eq!(status["Status"], json!("IN_PROGRESS"));
    let request_id = status["RequestId"].as_str().unwrap().to_string();

    let described = call(
        &s,
        "DescribeAccountAssignmentCreationStatus",
        json!({ "InstanceArn": inst, "AccountAssignmentCreationRequestId": request_id }),
    );
    assert_eq!(
        described["AccountAssignmentCreationStatus"]["Status"],
        json!("SUCCEEDED")
    );

    let listed = call(
        &s,
        "ListAccountAssignments",
        json!({ "InstanceArn": inst, "AccountId": "111122223333", "PermissionSetArn": ps_arn }),
    );
    assert_eq!(listed["AccountAssignments"].as_array().unwrap().len(), 1);
}

#[test]
fn provision_permission_set_settles() {
    let s = svc();
    let inst = new_instance(&s);
    let ps_arn = call(
        &s,
        "CreatePermissionSet",
        json!({ "InstanceArn": inst, "Name": "PS" }),
    )["PermissionSet"]["PermissionSetArn"]
        .as_str()
        .unwrap()
        .to_string();
    let prov = call(
        &s,
        "ProvisionPermissionSet",
        json!({ "InstanceArn": inst, "PermissionSetArn": ps_arn, "TargetType": "ALL_PROVISIONED_ACCOUNTS" }),
    );
    let rid = prov["PermissionSetProvisioningStatus"]["RequestId"]
        .as_str()
        .unwrap()
        .to_string();
    let desc = call(
        &s,
        "DescribePermissionSetProvisioningStatus",
        json!({ "InstanceArn": inst, "ProvisionPermissionSetRequestId": rid }),
    );
    assert_eq!(
        desc["PermissionSetProvisioningStatus"]["Status"],
        json!("SUCCEEDED")
    );
}

#[test]
fn application_crud_and_assignment() {
    let s = svc();
    let inst = new_instance(&s);
    let created = call(
        &s,
        "CreateApplication",
        json!({
            "InstanceArn": inst,
            "ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/sso",
            "Name": "my-app"
        }),
    );
    let app_arn = created["ApplicationArn"].as_str().unwrap().to_string();
    assert!(created.get("IdentityStoreArn").is_some());

    let desc = call(
        &s,
        "DescribeApplication",
        json!({ "ApplicationArn": app_arn }),
    );
    assert_eq!(desc["Name"], json!("my-app"));

    call(
        &s,
        "CreateApplicationAssignment",
        json!({ "ApplicationArn": app_arn, "PrincipalId": "u-1", "PrincipalType": "USER" }),
    );
    let assigns = call(
        &s,
        "ListApplicationAssignments",
        json!({ "ApplicationArn": app_arn }),
    );
    assert_eq!(
        assigns["ApplicationAssignments"].as_array().unwrap().len(),
        1
    );
}

#[test]
fn application_access_scope_round_trip() {
    let s = svc();
    let inst = new_instance(&s);
    let app_arn = call(
        &s,
        "CreateApplication",
        json!({
            "InstanceArn": inst,
            "ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/sso",
            "Name": "app"
        }),
    )["ApplicationArn"]
        .as_str()
        .unwrap()
        .to_string();
    call(
        &s,
        "PutApplicationAccessScope",
        json!({ "ApplicationArn": app_arn, "Scope": "sso:account:access", "AuthorizedTargets": ["t1"] }),
    );
    let got = call(
        &s,
        "GetApplicationAccessScope",
        json!({ "ApplicationArn": app_arn, "Scope": "sso:account:access" }),
    );
    assert_eq!(got["Scope"], json!("sso:account:access"));
    assert_eq!(got["AuthorizedTargets"], json!(["t1"]));
}

#[test]
fn trusted_token_issuer_lifecycle() {
    let s = svc();
    let inst = new_instance(&s);
    let cfg = json!({ "OidcJwtConfiguration": {
        "IssuerUrl": "https://issuer.example.com",
        "ClaimAttributePath": "email",
        "IdentityStoreAttributePath": "emails.value",
        "JwksRetrievalOption": "OPEN_ID_DISCOVERY"
    }});
    let created = call(
        &s,
        "CreateTrustedTokenIssuer",
        json!({
            "InstanceArn": inst, "Name": "issuer", "TrustedTokenIssuerType": "OIDC_JWT",
            "TrustedTokenIssuerConfiguration": cfg
        }),
    );
    let arn = created["TrustedTokenIssuerArn"]
        .as_str()
        .unwrap()
        .to_string();
    let desc = call(
        &s,
        "DescribeTrustedTokenIssuer",
        json!({ "TrustedTokenIssuerArn": arn }),
    );
    assert_eq!(desc["Name"], json!("issuer"));
    assert_eq!(desc["TrustedTokenIssuerType"], json!("OIDC_JWT"));
    // Config echoes back verbatim.
    assert_eq!(
        desc["TrustedTokenIssuerConfiguration"]["OidcJwtConfiguration"]["ClaimAttributePath"],
        json!("email")
    );
    let list = call(
        &s,
        "ListTrustedTokenIssuers",
        json!({ "InstanceArn": inst }),
    );
    assert_eq!(list["TrustedTokenIssuers"].as_array().unwrap().len(), 1);
}

#[test]
fn regions_lifecycle() {
    let s = svc();
    let inst = new_instance(&s);
    let added = call(
        &s,
        "AddRegion",
        json!({ "InstanceArn": inst, "RegionName": "us-west-2" }),
    );
    assert_eq!(added["Status"], json!("ACTIVE"));
    let desc = call(
        &s,
        "DescribeRegion",
        json!({ "InstanceArn": inst, "RegionName": "us-west-2" }),
    );
    assert_eq!(desc["IsPrimaryRegion"], json!(true));
    let list = call(&s, "ListRegions", json!({ "InstanceArn": inst }));
    assert_eq!(list["Regions"].as_array().unwrap().len(), 1);
}

#[test]
fn tagging() {
    let s = svc();
    let arn = "arn:aws:sso:::instance/ssoins-1234567890abcdef";
    call(
        &s,
        "TagResource",
        json!({ "ResourceArn": arn, "Tags": [{ "Key": "team", "Value": "sec" }] }),
    );
    let tags = call(&s, "ListTagsForResource", json!({ "ResourceArn": arn }));
    assert_eq!(tags["Tags"].as_array().unwrap().len(), 1);
    call(
        &s,
        "UntagResource",
        json!({ "ResourceArn": arn, "TagKeys": ["team"] }),
    );
    let after = call(&s, "ListTagsForResource", json!({ "ResourceArn": arn }));
    assert_eq!(after["Tags"].as_array().unwrap().len(), 0);
}

#[test]
fn application_providers_catalogue() {
    let s = svc();
    let list = call(&s, "ListApplicationProviders", json!({}));
    assert!(!list["ApplicationProviders"].as_array().unwrap().is_empty());
    let desc = call(
        &s,
        "DescribeApplicationProvider",
        json!({ "ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/sso" }),
    );
    assert_eq!(
        desc["ApplicationProviderArn"],
        json!("arn:aws:sso::aws:applicationProvider/sso")
    );
}

#[test]
fn pagination_windows_results() {
    let s = svc();
    let inst = new_instance(&s);
    for i in 0..3 {
        call(
            &s,
            "CreatePermissionSet",
            json!({ "InstanceArn": inst, "Name": format!("PS{i}") }),
        );
    }
    let page1 = call(
        &s,
        "ListPermissionSets",
        json!({ "InstanceArn": inst, "MaxResults": 2 }),
    );
    assert_eq!(page1["PermissionSets"].as_array().unwrap().len(), 2);
    let token = page1["NextToken"].as_str().unwrap().to_string();
    let page2 = call(
        &s,
        "ListPermissionSets",
        json!({ "InstanceArn": inst, "MaxResults": 2, "NextToken": token }),
    );
    assert_eq!(page2["PermissionSets"].as_array().unwrap().len(), 1);
    assert!(page2.get("NextToken").is_none());
}

#[test]
fn create_time_tags_round_trip() {
    let s = svc();
    let inst = new_instance(&s);
    let ps_arn = call(
        &s,
        "CreatePermissionSet",
        json!({ "InstanceArn": inst, "Name": "Tagged", "Tags": [{ "Key": "env", "Value": "prod" }] }),
    )["PermissionSet"]["PermissionSetArn"]
        .as_str()
        .unwrap()
        .to_string();
    let tags = call(&s, "ListTagsForResource", json!({ "ResourceArn": ps_arn }));
    let arr = tags["Tags"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["Key"], json!("env"));
    assert_eq!(arr[0]["Value"], json!("prod"));
}

#[test]
fn permissions_boundary_rejects_both_policy_kinds() {
    let s = svc();
    let inst = new_instance(&s);
    let ps_arn = call(
        &s,
        "CreatePermissionSet",
        json!({ "InstanceArn": inst, "Name": "PS" }),
    )["PermissionSet"]["PermissionSetArn"]
        .as_str()
        .unwrap()
        .to_string();
    let e = err(
        &s,
        "PutPermissionsBoundaryToPermissionSet",
        json!({
            "InstanceArn": inst, "PermissionSetArn": ps_arn,
            "PermissionsBoundary": {
                "ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
                "CustomerManagedPolicyReference": { "Name": "cmp", "Path": "/" }
            }
        }),
    );
    assert_eq!(e.code(), "ValidationException");
    assert!(e
        .message()
        .contains("Only ManagedPolicyArn or CustomerManagedPolicyReference should be given"));
}

#[test]
fn application_portal_options_default_visibility() {
    let s = svc();
    let inst = new_instance(&s);
    let app_arn = call(
        &s,
        "CreateApplication",
        json!({
            "InstanceArn": inst,
            "ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/sso",
            "Name": "app",
            "PortalOptions": { "SignInOptions": { "Origin": "IDENTITY_CENTER" } }
        }),
    )["ApplicationArn"]
        .as_str()
        .unwrap()
        .to_string();
    let desc = call(
        &s,
        "DescribeApplication",
        json!({ "ApplicationArn": app_arn }),
    );
    assert_eq!(desc["PortalOptions"]["Visibility"], json!("ENABLED"));
}

#[test]
fn update_trusted_token_issuer_preserves_immutable_issuer_url() {
    let s = svc();
    let inst = new_instance(&s);
    let arn = call(
        &s,
        "CreateTrustedTokenIssuer",
        json!({
            "InstanceArn": inst, "Name": "tti", "TrustedTokenIssuerType": "OIDC_JWT",
            "TrustedTokenIssuerConfiguration": { "OidcJwtConfiguration": {
                "IssuerUrl": "https://issuer.example.com", "ClaimAttributePath": "email",
                "IdentityStoreAttributePath": "emails.value", "JwksRetrievalOption": "OPEN_ID_DISCOVERY"
            }}
        }),
    )["TrustedTokenIssuerArn"]
        .as_str()
        .unwrap()
        .to_string();
    // Update config omits the immutable IssuerUrl (as the real provider does).
    call(
        &s,
        "UpdateTrustedTokenIssuer",
        json!({
            "TrustedTokenIssuerArn": arn,
            "TrustedTokenIssuerConfiguration": { "OidcJwtConfiguration": { "ClaimAttributePath": "sub" } }
        }),
    );
    let desc = call(
        &s,
        "DescribeTrustedTokenIssuer",
        json!({ "TrustedTokenIssuerArn": arn }),
    );
    let cfg = &desc["TrustedTokenIssuerConfiguration"]["OidcJwtConfiguration"];
    assert_eq!(cfg["IssuerUrl"], json!("https://issuer.example.com"));
    assert_eq!(cfg["ClaimAttributePath"], json!("sub"));
}

#[test]
fn not_found_and_validation() {
    let s = svc();
    // Describe of an unknown permission set -> ResourceNotFoundException.
    let e = err(
        &s,
        "DescribePermissionSet",
        json!({ "InstanceArn": "arn:aws:sso:::instance/ssoins-1234567890abcdef", "PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-1234567890abcdef/ps-1234567890abcdef" }),
    );
    assert_eq!(e.code(), "ResourceNotFoundException");

    // Missing required field -> ValidationException.
    let e = err(&s, "DescribeInstance", json!({}));
    assert_eq!(e.code(), "ValidationException");

    // Too-short InstanceArn -> ValidationException.
    let e = err(&s, "DescribeInstance", json!({ "InstanceArn": "short" }));
    assert_eq!(e.code(), "ValidationException");

    // Invalid enum -> ValidationException.
    let e = err(
        &s,
        "ProvisionPermissionSet",
        json!({ "InstanceArn": "arn:aws:sso:::instance/ssoins-1234567890abcdef", "PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-1234567890abcdef/ps-1234567890abcdef", "TargetType": "BOGUS" }),
    );
    assert_eq!(e.code(), "ValidationException");

    // MaxResults out of range -> ValidationException.
    let e = err(&s, "ListInstances", json!({ "MaxResults": 500 }));
    assert_eq!(e.code(), "ValidationException");
}
