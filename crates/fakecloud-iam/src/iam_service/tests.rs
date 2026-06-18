use super::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

fn make_service() -> IamService {
    let state: SharedIamState = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    IamService::new(state)
}

fn make_request(action: &str, params: Vec<(&str, &str)>) -> AwsRequest {
    let mut query_params = HashMap::new();
    query_params.insert("Action".to_string(), action.to_string());
    for (k, v) in params {
        query_params.insert(k.to_string(), v.to_string());
    }
    AwsRequest {
        service: "iam".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-id".to_string(),
        headers: http::HeaderMap::new(),
        query_params,
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

/// Pull the text inside the first ``<tag>...</tag>`` element of ``body``.
/// Used by IAM tests to fish an ARN out of a create-* response without
/// pulling in a real XML parser.
fn extract_xml_tag<'a>(body: &'a str, tag: &str) -> &'a str {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body
        .find(&open)
        .unwrap_or_else(|| panic!("tag <{tag}> not found"))
        + open.len();
    let end = body
        .find(&close)
        .unwrap_or_else(|| panic!("tag </{tag}> not found"));
    &body[start..end]
}

#[test]
fn list_access_keys_max_items_zero_returns_error() {
    let svc = make_service();

    // Create a user first
    let req = make_request("CreateUser", vec![("UserName", "testuser")]);
    svc.create_user(&req).unwrap();

    // Try listing access keys with MaxItems=0
    let req = make_request(
        "ListAccessKeys",
        vec![("UserName", "testuser"), ("MaxItems", "0")],
    );
    let result = svc.list_access_keys(&req);
    assert!(result.is_err(), "MaxItems=0 should return an error");
}

#[test]
fn list_users_rejects_non_numeric_max_items() {
    let svc = make_service();
    let req = make_request("ListUsers", vec![("MaxItems", "abc")]);
    let result = svc.list_users(&req);
    assert!(
        result.is_err(),
        "non-numeric MaxItems should return an error"
    );
}

#[test]
fn list_roles_rejects_non_numeric_max_items() {
    let svc = make_service();
    let req = make_request("ListRoles", vec![("MaxItems", "xyz")]);
    let result = svc.list_roles(&req);
    assert!(
        result.is_err(),
        "non-numeric MaxItems should return an error"
    );
}

#[test]
fn list_policies_rejects_non_numeric_max_items() {
    let svc = make_service();
    let req = make_request("ListPolicies", vec![("MaxItems", "notanumber")]);
    let result = svc.list_policies(&req);
    assert!(
        result.is_err(),
        "non-numeric MaxItems should return an error"
    );
}

// ---- Group inline policy tests ----

#[test]
fn put_and_get_group_policy() {
    let svc = make_service();
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;

    // Create group
    svc.handle_sync("CreateGroup", vec![("GroupName", "devs")]);

    // Put inline policy
    svc.handle_sync(
        "PutGroupPolicy",
        vec![
            ("GroupName", "devs"),
            ("PolicyName", "s3-access"),
            ("PolicyDocument", policy_doc),
        ],
    );

    // Get inline policy
    let resp = svc.handle_sync(
        "GetGroupPolicy",
        vec![("GroupName", "devs"), ("PolicyName", "s3-access")],
    );
    assert!(resp.contains("s3-access"));
    assert!(resp.contains("devs"));
}

#[test]
fn list_group_policies() {
    let svc = make_service();
    let doc =
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

    svc.handle_sync("CreateGroup", vec![("GroupName", "ops")]);
    svc.handle_sync(
        "PutGroupPolicy",
        vec![
            ("GroupName", "ops"),
            ("PolicyName", "pol-a"),
            ("PolicyDocument", doc),
        ],
    );
    svc.handle_sync(
        "PutGroupPolicy",
        vec![
            ("GroupName", "ops"),
            ("PolicyName", "pol-b"),
            ("PolicyDocument", doc),
        ],
    );

    let resp = svc.handle_sync("ListGroupPolicies", vec![("GroupName", "ops")]);
    assert!(resp.contains("pol-a"));
    assert!(resp.contains("pol-b"));
}

#[test]
fn delete_group_policy() {
    let svc = make_service();
    let doc =
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

    svc.handle_sync("CreateGroup", vec![("GroupName", "team")]);
    svc.handle_sync(
        "PutGroupPolicy",
        vec![
            ("GroupName", "team"),
            ("PolicyName", "temp"),
            ("PolicyDocument", doc),
        ],
    );

    // Delete
    svc.handle_sync(
        "DeleteGroupPolicy",
        vec![("GroupName", "team"), ("PolicyName", "temp")],
    );

    // List should be empty
    let resp = svc.handle_sync("ListGroupPolicies", vec![("GroupName", "team")]);
    assert!(!resp.contains("temp"));
}

#[test]
fn get_group_policy_not_found() {
    let svc = make_service();
    svc.handle_sync("CreateGroup", vec![("GroupName", "g1")]);

    let req = make_request(
        "GetGroupPolicy",
        vec![("GroupName", "g1"), ("PolicyName", "nope")],
    );
    let result = svc.get_group_policy(&req);
    assert!(result.is_err());
}

// ---- Group managed policy attachment tests ----

#[test]
fn attach_and_list_group_policies_managed() {
    let svc = make_service();
    let doc =
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

    svc.handle_sync("CreateGroup", vec![("GroupName", "eng")]);
    svc.handle_sync(
        "CreatePolicy",
        vec![("PolicyName", "read-policy"), ("PolicyDocument", doc)],
    );

    let create_resp = svc.handle_sync(
        "CreatePolicy",
        vec![("PolicyName", "write-policy"), ("PolicyDocument", doc)],
    );
    // Extract the ARN from the second policy
    let arn_start = create_resp.find("<Arn>").unwrap() + 5;
    let arn_end = create_resp.find("</Arn>").unwrap();
    let write_arn = &create_resp[arn_start..arn_end];

    // Attach both policies - for the first one, extract its ARN too
    // Just use the write_arn which we already have
    svc.handle_sync(
        "AttachGroupPolicy",
        vec![("GroupName", "eng"), ("PolicyArn", write_arn)],
    );

    let list = svc.handle_sync("ListAttachedGroupPolicies", vec![("GroupName", "eng")]);
    assert!(list.contains("write-policy"));
}

#[test]
fn detach_group_policy() {
    let svc = make_service();
    let doc =
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

    svc.handle_sync("CreateGroup", vec![("GroupName", "detach-grp")]);
    let resp = svc.handle_sync(
        "CreatePolicy",
        vec![("PolicyName", "detach-pol"), ("PolicyDocument", doc)],
    );
    let arn = extract_xml_value(&resp, "Arn");

    svc.handle_sync(
        "AttachGroupPolicy",
        vec![("GroupName", "detach-grp"), ("PolicyArn", &arn)],
    );

    // Detach
    svc.handle_sync(
        "DetachGroupPolicy",
        vec![("GroupName", "detach-grp"), ("PolicyArn", &arn)],
    );

    let list = svc.handle_sync(
        "ListAttachedGroupPolicies",
        vec![("GroupName", "detach-grp")],
    );
    assert!(!list.contains("detach-pol"));
}

#[test]
fn detach_group_policy_not_attached_fails() {
    let svc = make_service();
    svc.handle_sync("CreateGroup", vec![("GroupName", "grp-err")]);

    let req = make_request(
        "DetachGroupPolicy",
        vec![
            ("GroupName", "grp-err"),
            ("PolicyArn", "arn:aws:iam::123456789012:policy/nope"),
        ],
    );
    let result = svc.detach_group_policy(&req);
    assert!(result.is_err());
}

// ---- User inline policy tests ----

#[test]
fn put_get_delete_user_inline_policy() {
    let svc = make_service();
    let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*","Resource":"*"}]}"#;

    svc.handle_sync("CreateUser", vec![("UserName", "alice")]);

    // Put
    svc.handle_sync(
        "PutUserPolicy",
        vec![
            ("UserName", "alice"),
            ("PolicyName", "sqs-access"),
            ("PolicyDocument", doc),
        ],
    );

    // Get
    let resp = svc.handle_sync(
        "GetUserPolicy",
        vec![("UserName", "alice"), ("PolicyName", "sqs-access")],
    );
    assert!(resp.contains("sqs-access"));
    assert!(resp.contains("alice"));

    // List
    let list = svc.handle_sync("ListUserPolicies", vec![("UserName", "alice")]);
    assert!(list.contains("sqs-access"));

    // Delete
    svc.handle_sync(
        "DeleteUserPolicy",
        vec![("UserName", "alice"), ("PolicyName", "sqs-access")],
    );

    let list = svc.handle_sync("ListUserPolicies", vec![("UserName", "alice")]);
    assert!(!list.contains("sqs-access"));
}

#[test]
fn get_user_policy_not_found() {
    let svc = make_service();
    svc.handle_sync("CreateUser", vec![("UserName", "bob")]);

    let req = make_request(
        "GetUserPolicy",
        vec![("UserName", "bob"), ("PolicyName", "ghost")],
    );
    let result = svc.get_user_policy(&req);
    assert!(result.is_err());
}

// ---- User managed policy attachment tests ----

#[test]
fn attach_detach_list_user_policies_managed() {
    let svc = make_service();
    let doc =
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

    svc.handle_sync("CreateUser", vec![("UserName", "carol")]);
    let resp = svc.handle_sync(
        "CreatePolicy",
        vec![("PolicyName", "user-pol"), ("PolicyDocument", doc)],
    );
    let arn = extract_xml_value(&resp, "Arn");

    // Attach
    svc.handle_sync(
        "AttachUserPolicy",
        vec![("UserName", "carol"), ("PolicyArn", &arn)],
    );

    // List attached
    let list = svc.handle_sync("ListAttachedUserPolicies", vec![("UserName", "carol")]);
    assert!(list.contains("user-pol"));

    // Detach
    svc.handle_sync(
        "DetachUserPolicy",
        vec![("UserName", "carol"), ("PolicyArn", &arn)],
    );

    let list = svc.handle_sync("ListAttachedUserPolicies", vec![("UserName", "carol")]);
    assert!(!list.contains("user-pol"));
}

#[test]
fn attach_user_policy_nonexistent_user_fails() {
    let svc = make_service();
    let req = make_request(
        "AttachUserPolicy",
        vec![
            ("UserName", "nobody"),
            ("PolicyArn", "arn:aws:iam::123456789012:policy/x"),
        ],
    );
    let result = svc.attach_user_policy(&req);
    assert!(result.is_err());
}

#[test]
fn detach_user_policy_not_attached_fails() {
    let svc = make_service();
    svc.handle_sync("CreateUser", vec![("UserName", "dave")]);

    let req = make_request(
        "DetachUserPolicy",
        vec![
            ("UserName", "dave"),
            ("PolicyArn", "arn:aws:iam::123456789012:policy/nope"),
        ],
    );
    let result = svc.detach_user_policy(&req);
    assert!(result.is_err());
}

// ---- Login profile tests ----

#[test]
fn login_profile_lifecycle() {
    let svc = make_service();
    svc.handle_sync("CreateUser", vec![("UserName", "loginuser")]);

    // Create login profile
    let resp = svc.handle_sync(
        "CreateLoginProfile",
        vec![
            ("UserName", "loginuser"),
            ("Password", "S3cureP@ss!"),
            ("PasswordResetRequired", "true"),
        ],
    );
    assert!(resp.contains("loginuser"));
    assert!(resp.contains("<PasswordResetRequired>true</PasswordResetRequired>"));

    // Get login profile
    let resp = svc.handle_sync("GetLoginProfile", vec![("UserName", "loginuser")]);
    assert!(resp.contains("loginuser"));
    assert!(resp.contains("<PasswordResetRequired>true</PasswordResetRequired>"));

    // Update login profile
    svc.handle_sync(
        "UpdateLoginProfile",
        vec![
            ("UserName", "loginuser"),
            ("PasswordResetRequired", "false"),
        ],
    );

    let resp = svc.handle_sync("GetLoginProfile", vec![("UserName", "loginuser")]);
    assert!(resp.contains("<PasswordResetRequired>false</PasswordResetRequired>"));

    // Delete login profile
    svc.handle_sync("DeleteLoginProfile", vec![("UserName", "loginuser")]);

    // Should fail now
    let req = make_request("GetLoginProfile", vec![("UserName", "loginuser")]);
    assert!(svc.get_login_profile(&req).is_err());
}

#[test]
fn create_login_profile_duplicate_fails() {
    let svc = make_service();
    svc.handle_sync("CreateUser", vec![("UserName", "dupuser")]);
    svc.handle_sync(
        "CreateLoginProfile",
        vec![("UserName", "dupuser"), ("Password", "pass1")],
    );

    let req = make_request(
        "CreateLoginProfile",
        vec![("UserName", "dupuser"), ("Password", "pass2")],
    );
    assert!(svc.create_login_profile(&req).is_err());
}

#[test]
fn delete_login_profile_nonexistent_fails() {
    let svc = make_service();
    svc.handle_sync("CreateUser", vec![("UserName", "nologin")]);

    let req = make_request("DeleteLoginProfile", vec![("UserName", "nologin")]);
    assert!(svc.delete_login_profile(&req).is_err());
}

// ---- MFA tests ----

#[test]
fn virtual_mfa_device_lifecycle() {
    let svc = make_service();

    // Create virtual MFA device
    let resp = svc.handle_sync(
        "CreateVirtualMFADevice",
        vec![("VirtualMFADeviceName", "my-mfa")],
    );
    assert!(resp.contains("my-mfa"));
    assert!(resp.contains("<Base32StringSeed>"));
    assert!(resp.contains("<QRCodePNG>"));
    let serial = extract_xml_value(&resp, "SerialNumber");

    // List should include it
    let list = svc.handle_sync("ListVirtualMFADevices", vec![]);
    assert!(list.contains("my-mfa"));

    // Delete
    svc.handle_sync("DeleteVirtualMFADevice", vec![("SerialNumber", &serial)]);

    // List should be empty
    let list = svc.handle_sync("ListVirtualMFADevices", vec![]);
    assert!(!list.contains("my-mfa"));
}

#[test]
fn delete_virtual_mfa_device_not_found() {
    let svc = make_service();
    let req = make_request(
        "DeleteVirtualMFADevice",
        vec![("SerialNumber", "arn:aws:iam::123456789012:mfa/ghost")],
    );
    assert!(svc.delete_virtual_mfa_device(&req).is_err());
}

#[test]
fn enable_and_list_mfa_devices() {
    let svc = make_service();
    svc.handle_sync("CreateUser", vec![("UserName", "mfauser")]);

    // Create virtual MFA
    let resp = svc.handle_sync(
        "CreateVirtualMFADevice",
        vec![("VirtualMFADeviceName", "dev-mfa")],
    );
    let serial = extract_xml_value(&resp, "SerialNumber");

    // Enable MFA device for user
    svc.handle_sync(
        "EnableMFADevice",
        vec![
            ("UserName", "mfauser"),
            ("SerialNumber", &serial),
            ("AuthenticationCode1", "123456"),
            ("AuthenticationCode2", "654321"),
        ],
    );

    // List MFA devices for user
    let list = svc.handle_sync("ListMFADevices", vec![("UserName", "mfauser")]);
    assert!(list.contains(&serial));
    assert!(list.contains("mfauser"));
}

#[test]
fn deactivate_mfa_device() {
    let svc = make_service();
    svc.handle_sync("CreateUser", vec![("UserName", "deactuser")]);

    let resp = svc.handle_sync(
        "CreateVirtualMFADevice",
        vec![("VirtualMFADeviceName", "deact-mfa")],
    );
    let serial = extract_xml_value(&resp, "SerialNumber");

    svc.handle_sync(
        "EnableMFADevice",
        vec![
            ("UserName", "deactuser"),
            ("SerialNumber", &serial),
            ("AuthenticationCode1", "111111"),
            ("AuthenticationCode2", "222222"),
        ],
    );

    // Deactivate
    svc.handle_sync(
        "DeactivateMFADevice",
        vec![("UserName", "deactuser"), ("SerialNumber", &serial)],
    );

    // Should no longer appear in user's MFA device list
    let list = svc.handle_sync("ListMFADevices", vec![("UserName", "deactuser")]);
    assert!(!list.contains(&serial));
}

#[test]
fn list_virtual_mfa_devices_assignment_filter() {
    let svc = make_service();
    svc.handle_sync("CreateUser", vec![("UserName", "filteruser")]);

    // Create two MFA devices with distinct names
    let resp1 = svc.handle_sync(
        "CreateVirtualMFADevice",
        vec![("VirtualMFADeviceName", "enabled-device")],
    );
    let serial1 = extract_xml_value(&resp1, "SerialNumber");
    svc.handle_sync(
        "CreateVirtualMFADevice",
        vec![("VirtualMFADeviceName", "spare-device")],
    );

    // Enable only the first
    svc.handle_sync(
        "EnableMFADevice",
        vec![
            ("UserName", "filteruser"),
            ("SerialNumber", &serial1),
            ("AuthenticationCode1", "123456"),
            ("AuthenticationCode2", "654321"),
        ],
    );

    // Filter by Assigned
    let assigned = svc.handle_sync(
        "ListVirtualMFADevices",
        vec![("AssignmentStatus", "Assigned")],
    );
    assert!(assigned.contains("enabled-device"));
    assert!(!assigned.contains("spare-device"));

    // Filter by Unassigned
    let unassigned = svc.handle_sync(
        "ListVirtualMFADevices",
        vec![("AssignmentStatus", "Unassigned")],
    );
    assert!(!unassigned.contains("enabled-device"));
    assert!(unassigned.contains("spare-device"));
}

// ---- Account tests ----

#[test]
fn get_account_summary() {
    let svc = make_service();

    // Create some resources to verify counts
    svc.handle_sync("CreateUser", vec![("UserName", "u1")]);
    svc.handle_sync("CreateUser", vec![("UserName", "u2")]);
    svc.handle_sync("CreateGroup", vec![("GroupName", "g1")]);

    let resp = svc.handle_sync("GetAccountSummary", vec![]);
    assert!(resp.contains("<key>Users</key><value>2</value>"));
    assert!(resp.contains("<key>Groups</key><value>1</value>"));
    assert!(resp.contains("<key>UsersQuota</key><value>5000</value>"));
}

#[test]
fn account_alias_lifecycle() {
    let svc = make_service();

    // Create alias
    svc.handle_sync("CreateAccountAlias", vec![("AccountAlias", "my-org")]);

    // List aliases
    let list = svc.handle_sync("ListAccountAliases", vec![]);
    assert!(list.contains("my-org"));

    // Delete alias
    svc.handle_sync("DeleteAccountAlias", vec![("AccountAlias", "my-org")]);

    let list = svc.handle_sync("ListAccountAliases", vec![]);
    assert!(!list.contains("my-org"));
}

#[test]
fn create_account_alias_idempotent() {
    let svc = make_service();
    svc.handle_sync("CreateAccountAlias", vec![("AccountAlias", "test-alias")]);
    svc.handle_sync("CreateAccountAlias", vec![("AccountAlias", "test-alias")]);

    let list = svc.handle_sync("ListAccountAliases", vec![]);
    // Should only appear once
    let count = list.matches("test-alias").count();
    assert_eq!(count, 1, "alias should appear exactly once");
}

// ---- Helper methods for tests ----

fn extract_xml_value(xml: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open).unwrap() + open.len();
    let end = xml.find(&close).unwrap();
    xml[start..end].to_string()
}

impl IamService {
    /// Synchronous helper for unit tests: dispatches to the correct method
    fn handle_sync(&self, action: &str, params: Vec<(&str, &str)>) -> String {
        let req = make_request(action, params);
        let resp = match action {
            "CreateUser" => self.create_user(&req),
            "CreateGroup" => self.create_group(&req),
            "CreatePolicy" => self.create_policy(&req),
            "ListPolicies" => self.list_policies(&req),
            "PutGroupPolicy" => self.put_group_policy(&req),
            "GetGroupPolicy" => self.get_group_policy(&req),
            "DeleteGroupPolicy" => self.delete_group_policy(&req),
            "ListGroupPolicies" => self.list_group_policies(&req),
            "AttachGroupPolicy" => self.attach_group_policy(&req),
            "DetachGroupPolicy" => self.detach_group_policy(&req),
            "ListAttachedGroupPolicies" => self.list_attached_group_policies(&req),
            "PutUserPolicy" => self.put_user_policy(&req),
            "GetUserPolicy" => self.get_user_policy(&req),
            "DeleteUserPolicy" => self.delete_user_policy(&req),
            "ListUserPolicies" => self.list_user_policies(&req),
            "AttachUserPolicy" => self.attach_user_policy(&req),
            "DetachUserPolicy" => self.detach_user_policy(&req),
            "ListAttachedUserPolicies" => self.list_attached_user_policies(&req),
            "CreateLoginProfile" => self.create_login_profile(&req),
            "GetLoginProfile" => self.get_login_profile(&req),
            "UpdateLoginProfile" => self.update_login_profile(&req),
            "DeleteLoginProfile" => self.delete_login_profile(&req),
            "CreateVirtualMFADevice" => self.create_virtual_mfa_device(&req),
            "DeleteVirtualMFADevice" => self.delete_virtual_mfa_device(&req),
            "ListVirtualMFADevices" => self.list_virtual_mfa_devices(&req),
            "EnableMFADevice" => self.enable_mfa_device(&req),
            "DeactivateMFADevice" => self.deactivate_mfa_device(&req),
            "ListMFADevices" => self.list_mfa_devices(&req),
            "GetAccountSummary" => self.get_account_summary(&req),
            "CreateAccountAlias" => self.create_account_alias(&req),
            "DeleteAccountAlias" => self.delete_account_alias(&req),
            "ListAccountAliases" => self.list_account_aliases(&req),
            other => panic!("handle_sync: unhandled action {other}"),
        }
        .unwrap();
        String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
    }
}

// ---- Policy Version Tests ----

#[test]
fn create_and_get_policy_version() {
    let svc = make_service();
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
    let req = make_request(
        "CreatePolicy",
        vec![("PolicyName", "test-pol"), ("PolicyDocument", policy_doc)],
    );
    let resp = svc.create_policy(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    // Extract policy ARN
    let policy_arn = extract_xml_tag(&body, "Arn");

    // Create v2
    let new_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"*"}]}"#;
    let req = make_request(
        "CreatePolicyVersion",
        vec![
            ("PolicyArn", policy_arn),
            ("PolicyDocument", new_doc),
            ("SetAsDefault", "true"),
        ],
    );
    let resp = svc.create_policy_version(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<VersionId>v2</VersionId>"));
    assert!(body.contains("<IsDefaultVersion>true</IsDefaultVersion>"));

    // Get v2
    let req = make_request(
        "GetPolicyVersion",
        vec![("PolicyArn", policy_arn), ("VersionId", "v2")],
    );
    let resp = svc.get_policy_version(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<VersionId>v2</VersionId>"));
    assert!(body.contains("<IsDefaultVersion>true</IsDefaultVersion>"));
}

#[test]
fn list_policy_versions() {
    let svc = make_service();
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let req = make_request(
        "CreatePolicy",
        vec![("PolicyName", "ver-pol"), ("PolicyDocument", policy_doc)],
    );
    let resp = svc.create_policy(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    let policy_arn = extract_xml_tag(&body, "Arn");

    // Create v2
    let req = make_request(
        "CreatePolicyVersion",
        vec![("PolicyArn", policy_arn), ("PolicyDocument", policy_doc)],
    );
    svc.create_policy_version(&req).unwrap();

    let req = make_request("ListPolicyVersions", vec![("PolicyArn", policy_arn)]);
    let resp = svc.list_policy_versions(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    // Should have v1 and v2
    assert!(body.contains("<VersionId>v1</VersionId>"));
    assert!(body.contains("<VersionId>v2</VersionId>"));
}

#[test]
fn delete_default_policy_version_fails() {
    let svc = make_service();
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let req = make_request(
        "CreatePolicy",
        vec![("PolicyName", "def-pol"), ("PolicyDocument", policy_doc)],
    );
    let resp = svc.create_policy(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    let policy_arn = extract_xml_tag(&body, "Arn");

    // v1 is the default; deleting it should fail
    let req = make_request(
        "DeletePolicyVersion",
        vec![("PolicyArn", policy_arn), ("VersionId", "v1")],
    );
    let result = svc.delete_policy_version(&req);
    assert!(result.is_err(), "deleting default version should fail");
}

#[test]
fn set_default_policy_version() {
    let svc = make_service();
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let req = make_request(
        "CreatePolicy",
        vec![("PolicyName", "sd-pol"), ("PolicyDocument", policy_doc)],
    );
    let resp = svc.create_policy(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    let policy_arn = extract_xml_tag(&body, "Arn");

    // Create v2
    let req = make_request(
        "CreatePolicyVersion",
        vec![("PolicyArn", policy_arn), ("PolicyDocument", policy_doc)],
    );
    svc.create_policy_version(&req).unwrap();

    // Set v2 as default
    let req = make_request(
        "SetDefaultPolicyVersion",
        vec![("PolicyArn", policy_arn), ("VersionId", "v2")],
    );
    svc.set_default_policy_version(&req).unwrap();

    // Verify v2 is now default
    let req = make_request(
        "GetPolicyVersion",
        vec![("PolicyArn", policy_arn), ("VersionId", "v2")],
    );
    let resp = svc.get_policy_version(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<IsDefaultVersion>true</IsDefaultVersion>"));

    // v1 should no longer be default
    let req = make_request(
        "GetPolicyVersion",
        vec![("PolicyArn", policy_arn), ("VersionId", "v1")],
    );
    let resp = svc.get_policy_version(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<IsDefaultVersion>false</IsDefaultVersion>"));
}

// ---- Server Certificate Tests ----

#[test]
fn server_certificate_lifecycle() {
    let svc = make_service();

    // Upload
    let req = make_request(
        "UploadServerCertificate",
        vec![
            ("ServerCertificateName", "my-cert"),
            (
                "CertificateBody",
                "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
            ),
            (
                "PrivateKey",
                "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----",
            ),
        ],
    );
    let resp = svc.upload_server_certificate(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<ServerCertificateName>my-cert</ServerCertificateName>"));
    assert!(body.contains("ASCA"));

    // Get
    let req = make_request(
        "GetServerCertificate",
        vec![("ServerCertificateName", "my-cert")],
    );
    let resp = svc.get_server_certificate(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<ServerCertificateName>my-cert</ServerCertificateName>"));

    // List
    let req = make_request("ListServerCertificates", vec![]);
    let resp = svc.list_server_certificates(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("my-cert"));

    // Delete
    let req = make_request(
        "DeleteServerCertificate",
        vec![("ServerCertificateName", "my-cert")],
    );
    svc.delete_server_certificate(&req).unwrap();

    // Should be gone
    let req = make_request(
        "GetServerCertificate",
        vec![("ServerCertificateName", "my-cert")],
    );
    assert!(svc.get_server_certificate(&req).is_err());
}

#[test]
fn server_certificate_duplicate_fails() {
    let svc = make_service();
    let req = make_request(
        "UploadServerCertificate",
        vec![
            ("ServerCertificateName", "dup-cert"),
            ("CertificateBody", "cert-body"),
            ("PrivateKey", "key-body"),
        ],
    );
    svc.upload_server_certificate(&req).unwrap();

    let req = make_request(
        "UploadServerCertificate",
        vec![
            ("ServerCertificateName", "dup-cert"),
            ("CertificateBody", "cert-body"),
            ("PrivateKey", "key-body"),
        ],
    );
    assert!(svc.upload_server_certificate(&req).is_err());
}

// ---- SSH Public Key Tests ----

#[test]
fn ssh_public_key_lifecycle() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "sshuser")]))
        .unwrap();

    // Upload
    let req = make_request(
        "UploadSSHPublicKey",
        vec![
            ("UserName", "sshuser"),
            (
                "SSHPublicKeyBody",
                "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQ test@example",
            ),
        ],
    );
    let resp = svc.upload_ssh_public_key(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<Status>Active</Status>"));
    assert!(body.contains("APKA"));
    // Extract key ID
    let kid_start = body.find("<SSHPublicKeyId>").unwrap() + 16;
    let kid_end = body.find("</SSHPublicKeyId>").unwrap();
    let key_id = &body[kid_start..kid_end];

    // Get
    let req = make_request(
        "GetSSHPublicKey",
        vec![("UserName", "sshuser"), ("SSHPublicKeyId", key_id)],
    );
    let resp = svc.get_ssh_public_key(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains(key_id));
    assert!(body.contains("<Status>Active</Status>"));

    // List
    let req = make_request("ListSSHPublicKeys", vec![("UserName", "sshuser")]);
    let resp = svc.list_ssh_public_keys(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains(key_id));

    // Update status to Inactive
    let req = make_request(
        "UpdateSSHPublicKey",
        vec![
            ("UserName", "sshuser"),
            ("SSHPublicKeyId", key_id),
            ("Status", "Inactive"),
        ],
    );
    svc.update_ssh_public_key(&req).unwrap();

    // Verify status changed
    let req = make_request(
        "GetSSHPublicKey",
        vec![("UserName", "sshuser"), ("SSHPublicKeyId", key_id)],
    );
    let resp = svc.get_ssh_public_key(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<Status>Inactive</Status>"));

    // Delete
    let req = make_request(
        "DeleteSSHPublicKey",
        vec![("UserName", "sshuser"), ("SSHPublicKeyId", key_id)],
    );
    svc.delete_ssh_public_key(&req).unwrap();

    // Should be empty now
    let req = make_request("ListSSHPublicKeys", vec![("UserName", "sshuser")]);
    let resp = svc.list_ssh_public_keys(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(!body.contains(key_id));
}

// ---- Signing Certificate Tests ----

#[test]
fn signing_certificate_lifecycle() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "certuser")]))
        .unwrap();

    let pem = "-----BEGIN CERTIFICATE-----\nMIIBxTCCAW4=\n-----END CERTIFICATE-----";

    // Upload
    let req = make_request(
        "UploadSigningCertificate",
        vec![("UserName", "certuser"), ("CertificateBody", pem)],
    );
    let resp = svc.upload_signing_certificate(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<Status>Active</Status>"));
    assert!(body.contains("<UserName>certuser</UserName>"));
    let cid_start = body.find("<CertificateId>").unwrap() + 15;
    let cid_end = body.find("</CertificateId>").unwrap();
    let cert_id = &body[cid_start..cid_end];

    // List
    let req = make_request("ListSigningCertificates", vec![("UserName", "certuser")]);
    let resp = svc.list_signing_certificates(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains(cert_id));

    // Update to Inactive
    let req = make_request(
        "UpdateSigningCertificate",
        vec![
            ("UserName", "certuser"),
            ("CertificateId", cert_id),
            ("Status", "Inactive"),
        ],
    );
    svc.update_signing_certificate(&req).unwrap();

    // Delete
    let req = make_request(
        "DeleteSigningCertificate",
        vec![("UserName", "certuser"), ("CertificateId", cert_id)],
    );
    svc.delete_signing_certificate(&req).unwrap();

    // Should be empty
    let req = make_request("ListSigningCertificates", vec![("UserName", "certuser")]);
    let resp = svc.list_signing_certificates(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(!body.contains(cert_id));
}

#[test]
fn signing_certificate_malformed_pem_fails() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "badcert")]))
        .unwrap();

    let req = make_request(
        "UploadSigningCertificate",
        vec![
            ("UserName", "badcert"),
            ("CertificateBody", "not-a-pem-cert"),
        ],
    );
    assert!(svc.upload_signing_certificate(&req).is_err());
}

// ---- Credential Report Tests ----

#[test]
fn credential_report_lifecycle() {
    let svc = make_service();

    // GetCredentialReport without generating first should fail
    let req = make_request("GetCredentialReport", vec![]);
    assert!(svc.get_credential_report(&req).is_err());

    // Generate
    let req = make_request("GenerateCredentialReport", vec![]);
    let resp = svc.generate_credential_report(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<State>STARTED</State>"));

    // Generate again returns COMPLETE
    let req = make_request("GenerateCredentialReport", vec![]);
    let resp = svc.generate_credential_report(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<State>COMPLETE</State>"));

    // Get credential report
    let req = make_request("GetCredentialReport", vec![]);
    let resp = svc.get_credential_report(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<ReportFormat>text/csv</ReportFormat>"));
    assert!(body.contains("<Content>"));
}

// ---- Service Linked Role Tests ----

#[test]
fn service_linked_role_lifecycle() {
    let svc = make_service();

    // Create
    let req = make_request(
        "CreateServiceLinkedRole",
        vec![("AWSServiceName", "elasticloadbalancing.amazonaws.com")],
    );
    let resp = svc.create_service_linked_role(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("AWSServiceRoleForElasticLoadBalancing"));
    assert!(body.contains("/aws-service-role/elasticloadbalancing.amazonaws.com/"));

    // Delete
    let req = make_request(
        "DeleteServiceLinkedRole",
        vec![("RoleName", "AWSServiceRoleForElasticLoadBalancing")],
    );
    let resp = svc.delete_service_linked_role(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<DeletionTaskId>"));
    let tid_start = body.find("<DeletionTaskId>").unwrap() + 16;
    let tid_end = body.find("</DeletionTaskId>").unwrap();
    let task_id = &body[tid_start..tid_end];

    // Check deletion status
    let req = make_request(
        "GetServiceLinkedRoleDeletionStatus",
        vec![("DeletionTaskId", task_id)],
    );
    let resp = svc.get_service_linked_role_deletion_status(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<Status>SUCCEEDED</Status>"));
}

// ---- Permission Boundary Tests ----

#[test]
fn role_permissions_boundary() {
    let svc = make_service();
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![
            ("RoleName", "bound-role"),
            ("AssumeRolePolicyDocument", trust),
        ],
    ))
    .unwrap();

    // Put boundary
    let boundary_arn = "arn:aws:iam::123456789012:policy/boundary-policy";
    let req = make_request(
        "PutRolePermissionsBoundary",
        vec![
            ("RoleName", "bound-role"),
            ("PermissionsBoundary", boundary_arn),
        ],
    );
    svc.put_role_permissions_boundary(&req).unwrap();

    // Verify via GetRole
    let req = make_request("GetRole", vec![("RoleName", "bound-role")]);
    let resp = svc.get_role(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains(boundary_arn));

    // Delete boundary
    let req = make_request(
        "DeleteRolePermissionsBoundary",
        vec![("RoleName", "bound-role")],
    );
    svc.delete_role_permissions_boundary(&req).unwrap();

    // Verify removed
    let req = make_request("GetRole", vec![("RoleName", "bound-role")]);
    let resp = svc.get_role(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(!body.contains(boundary_arn));
}

// ---- Tag Role Tests ----

#[test]
fn tag_untag_role() {
    let svc = make_service();
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![
            ("RoleName", "tag-role"),
            ("AssumeRolePolicyDocument", trust),
        ],
    ))
    .unwrap();

    // Tag
    let req = make_request(
        "TagRole",
        vec![
            ("RoleName", "tag-role"),
            ("Tags.member.1.Key", "env"),
            ("Tags.member.1.Value", "prod"),
        ],
    );
    svc.tag_role(&req).unwrap();

    // List tags
    let req = make_request("ListRoleTags", vec![("RoleName", "tag-role")]);
    let resp = svc.list_role_tags(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<Key>env</Key>"));
    assert!(body.contains("<Value>prod</Value>"));

    // Untag
    let req = make_request(
        "UntagRole",
        vec![("RoleName", "tag-role"), ("TagKeys.member.1", "env")],
    );
    svc.untag_role(&req).unwrap();

    let req = make_request("ListRoleTags", vec![("RoleName", "tag-role")]);
    let resp = svc.list_role_tags(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(!body.contains("<Key>env</Key>"));
}

#[test]
fn list_role_tags_rejects_invalid_marker() {
    // bug-audit 2026-05-28, 1.7: a Marker that does not parse as a valid offset
    // must be rejected with "Invalid Marker." (the same ValidationError the
    // other IAM list ops return) rather than silently falling back to the first
    // page.
    let svc = make_service();
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![
            ("RoleName", "marker-role"),
            ("AssumeRolePolicyDocument", trust),
        ],
    ))
    .unwrap();

    let req = make_request(
        "ListRoleTags",
        vec![("RoleName", "marker-role"), ("Marker", "not-a-number")],
    );
    match svc.list_role_tags(&req) {
        Ok(_) => panic!("malformed Marker must be rejected"),
        Err(err) => {
            assert_eq!(err.code(), "ValidationError");
            assert!(err.message().contains("Invalid Marker"));
        }
    }
}

// ---- Tag Policy Tests ----

#[test]
fn tag_untag_policy() {
    let svc = make_service();
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let req = make_request(
        "CreatePolicy",
        vec![("PolicyName", "tag-pol"), ("PolicyDocument", policy_doc)],
    );
    let resp = svc.create_policy(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    let policy_arn = extract_xml_tag(&body, "Arn").to_string();

    let req = make_request(
        "TagPolicy",
        vec![
            ("PolicyArn", &policy_arn),
            ("Tags.member.1.Key", "team"),
            ("Tags.member.1.Value", "platform"),
        ],
    );
    svc.tag_policy(&req).unwrap();

    let req = make_request("ListPolicyTags", vec![("PolicyArn", &policy_arn)]);
    let resp = svc.list_policy_tags(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<Key>team</Key>"));

    let req = make_request(
        "UntagPolicy",
        vec![("PolicyArn", &policy_arn), ("TagKeys.member.1", "team")],
    );
    svc.untag_policy(&req).unwrap();

    let req = make_request("ListPolicyTags", vec![("PolicyArn", &policy_arn)]);
    let resp = svc.list_policy_tags(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(!body.contains("<Key>team</Key>"));
}

// ---- Tag Instance Profile Tests ----

#[test]
fn tag_untag_instance_profile() {
    let svc = make_service();
    svc.create_instance_profile(&make_request(
        "CreateInstanceProfile",
        vec![("InstanceProfileName", "tag-ip")],
    ))
    .unwrap();

    let req = make_request(
        "TagInstanceProfile",
        vec![
            ("InstanceProfileName", "tag-ip"),
            ("Tags.member.1.Key", "dept"),
            ("Tags.member.1.Value", "eng"),
        ],
    );
    svc.tag_instance_profile(&req).unwrap();

    let req = make_request(
        "ListInstanceProfileTags",
        vec![("InstanceProfileName", "tag-ip")],
    );
    let resp = svc.list_instance_profile_tags(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<Key>dept</Key>"));

    let req = make_request(
        "UntagInstanceProfile",
        vec![
            ("InstanceProfileName", "tag-ip"),
            ("TagKeys.member.1", "dept"),
        ],
    );
    svc.untag_instance_profile(&req).unwrap();

    let req = make_request(
        "ListInstanceProfileTags",
        vec![("InstanceProfileName", "tag-ip")],
    );
    let resp = svc.list_instance_profile_tags(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(!body.contains("<Key>dept</Key>"));
}

// ---- Tag OIDC Provider Tests ----

#[test]
fn tag_untag_oidc_provider() {
    let svc = make_service();
    let req = make_request(
        "CreateOpenIDConnectProvider",
        vec![
            ("Url", "https://oidc.example.com"),
            (
                "ThumbprintList.member.1",
                "abcdef1234567890abcdef1234567890abcdef12",
            ),
        ],
    );
    let resp = svc.create_oidc_provider(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    let arn_start =
        body.find("<OpenIDConnectProviderArn>").unwrap() + "<OpenIDConnectProviderArn>".len();
    let arn_end = body.find("</OpenIDConnectProviderArn>").unwrap();
    let oidc_arn = body[arn_start..arn_end].to_string();

    let req = make_request(
        "TagOpenIDConnectProvider",
        vec![
            ("OpenIDConnectProviderArn", &oidc_arn),
            ("Tags.member.1.Key", "stage"),
            ("Tags.member.1.Value", "dev"),
        ],
    );
    svc.tag_oidc_provider(&req).unwrap();

    let req = make_request(
        "ListOpenIDConnectProviderTags",
        vec![("OpenIDConnectProviderArn", &oidc_arn)],
    );
    let resp = svc.list_oidc_provider_tags(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<Key>stage</Key>"));

    let req = make_request(
        "UntagOpenIDConnectProvider",
        vec![
            ("OpenIDConnectProviderArn", &oidc_arn),
            ("TagKeys.member.1", "stage"),
        ],
    );
    svc.untag_oidc_provider(&req).unwrap();

    let req = make_request(
        "ListOpenIDConnectProviderTags",
        vec![("OpenIDConnectProviderArn", &oidc_arn)],
    );
    let resp = svc.list_oidc_provider_tags(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(!body.contains("<Key>stage</Key>"));
}

// ---- Update Role Tests ----

#[test]
fn update_role_description_and_max_session() {
    let svc = make_service();
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![
            ("RoleName", "upd-role"),
            ("AssumeRolePolicyDocument", trust),
        ],
    ))
    .unwrap();

    // UpdateRole with Description and MaxSessionDuration
    let req = make_request(
        "UpdateRole",
        vec![
            ("RoleName", "upd-role"),
            ("Description", "new description"),
            ("MaxSessionDuration", "7200"),
        ],
    );
    svc.update_role(&req).unwrap();

    // UpdateRoleDescription
    let req = make_request(
        "UpdateRoleDescription",
        vec![("RoleName", "upd-role"), ("Description", "updated desc")],
    );
    let resp = svc.update_role_description(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<Description>updated desc</Description>"));
}

// ---- UpdateAssumeRolePolicy Tests ----

#[test]
fn update_assume_role_policy() {
    let svc = make_service();
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![
            ("RoleName", "arp-role"),
            ("AssumeRolePolicyDocument", trust),
        ],
    ))
    .unwrap();

    let new_trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    let req = make_request(
        "UpdateAssumeRolePolicy",
        vec![("RoleName", "arp-role"), ("PolicyDocument", new_trust)],
    );
    svc.update_assume_role_policy(&req).unwrap();

    // Verify by GetRole
    let req = make_request("GetRole", vec![("RoleName", "arp-role")]);
    let resp = svc.get_role(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("lambda.amazonaws.com"));
}

// ---- UpdateGroup Tests ----

#[test]
fn update_group_rename() {
    let svc = make_service();
    svc.create_group(&make_request("CreateGroup", vec![("GroupName", "old-grp")]))
        .unwrap();

    let req = make_request(
        "UpdateGroup",
        vec![("GroupName", "old-grp"), ("NewGroupName", "new-grp")],
    );
    svc.update_group(&req).unwrap();

    // Old name should not exist
    assert!(svc
        .get_group(&make_request("GetGroup", vec![("GroupName", "old-grp")]))
        .is_err());

    // New name should exist
    let resp = svc
        .get_group(&make_request("GetGroup", vec![("GroupName", "new-grp")]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<GroupName>new-grp</GroupName>"));
}

// ---- UpdateUser Tests ----

#[test]
fn update_user_rename() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "old-user")]))
        .unwrap();

    let req = make_request(
        "UpdateUser",
        vec![("UserName", "old-user"), ("NewUserName", "new-user")],
    );
    svc.update_user(&req).unwrap();

    assert!(svc
        .get_user(&make_request("GetUser", vec![("UserName", "old-user")]))
        .is_err());

    let resp = svc
        .get_user(&make_request("GetUser", vec![("UserName", "new-user")]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<UserName>new-user</UserName>"));
}

/// 1.29: UpdateUser rename must preserve the ARN partition (aws-cn here)
/// rather than hardcoding `arn:aws:`.
#[test]
fn update_user_rename_preserves_partition() {
    let svc = make_service();
    let mut create = make_request("CreateUser", vec![("UserName", "old-cn")]);
    create.region = "cn-north-1".to_string();
    let resp = svc.create_user(&create).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(
        extract_xml_tag(&body, "Arn").starts_with("arn:aws-cn:iam::"),
        "create should mint an aws-cn ARN"
    );

    let mut rename = make_request(
        "UpdateUser",
        vec![("UserName", "old-cn"), ("NewUserName", "new-cn")],
    );
    rename.region = "cn-north-1".to_string();
    svc.update_user(&rename).unwrap();

    let mut get = make_request("GetUser", vec![("UserName", "new-cn")]);
    get.region = "cn-north-1".to_string();
    let resp = svc.get_user(&get).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    let arn = extract_xml_tag(&body, "Arn");
    assert!(
        arn.starts_with("arn:aws-cn:iam::"),
        "rename must keep the aws-cn partition, got {arn}"
    );
    assert!(arn.ends_with(":user/new-cn"));
}

// ---- Account Password Policy Tests ----

#[test]
fn account_password_policy_lifecycle() {
    let svc = make_service();

    // Get before setting returns error
    let req = make_request("GetAccountPasswordPolicy", vec![]);
    assert!(svc.get_account_password_policy(&req).is_err());

    // Update (creates the policy)
    let req = make_request(
        "UpdateAccountPasswordPolicy",
        vec![
            ("MinimumPasswordLength", "12"),
            ("RequireSymbols", "true"),
            ("RequireNumbers", "true"),
        ],
    );
    svc.update_account_password_policy(&req).unwrap();

    // Get
    let req = make_request("GetAccountPasswordPolicy", vec![]);
    let resp = svc.get_account_password_policy(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<MinimumPasswordLength>12</MinimumPasswordLength>"));
    assert!(body.contains("<RequireSymbols>true</RequireSymbols>"));

    // Delete
    let req = make_request("DeleteAccountPasswordPolicy", vec![]);
    svc.delete_account_password_policy(&req).unwrap();

    // Should be gone
    let req = make_request("GetAccountPasswordPolicy", vec![]);
    assert!(svc.get_account_password_policy(&req).is_err());
}

// ---- GetAccountAuthorizationDetails Tests ----

#[test]
fn get_account_authorization_details() {
    let svc = make_service();

    // Create a user and role
    svc.create_user(&make_request("CreateUser", vec![("UserName", "auth-user")]))
        .unwrap();
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![
            ("RoleName", "auth-role"),
            ("AssumeRolePolicyDocument", trust),
        ],
    ))
    .unwrap();

    let req = make_request("GetAccountAuthorizationDetails", vec![]);
    let resp = svc.get_account_authorization_details(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<UserName>auth-user</UserName>"));
    assert!(body.contains("<RoleName>auth-role</RoleName>"));
}

// ---- ListEntitiesForPolicy Tests ----

#[test]
fn list_entities_for_policy() {
    let svc = make_service();

    // Create policy
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let req = make_request(
        "CreatePolicy",
        vec![("PolicyName", "ent-pol"), ("PolicyDocument", policy_doc)],
    );
    let resp = svc.create_policy(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    let policy_arn = extract_xml_tag(&body, "Arn").to_string();

    // Create role and attach policy
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![
            ("RoleName", "ent-role"),
            ("AssumeRolePolicyDocument", trust),
        ],
    ))
    .unwrap();
    svc.attach_role_policy(&make_request(
        "AttachRolePolicy",
        vec![("RoleName", "ent-role"), ("PolicyArn", &policy_arn)],
    ))
    .unwrap();

    // Create user and attach policy
    svc.create_user(&make_request("CreateUser", vec![("UserName", "ent-user")]))
        .unwrap();
    svc.attach_user_policy(&make_request(
        "AttachUserPolicy",
        vec![("UserName", "ent-user"), ("PolicyArn", &policy_arn)],
    ))
    .unwrap();

    let req = make_request("ListEntitiesForPolicy", vec![("PolicyArn", &policy_arn)]);
    let resp = svc.list_entities_for_policy(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<RoleName>ent-role</RoleName>"));
    assert!(body.contains("<UserName>ent-user</UserName>"));
}

// ---- GetAccessKeyLastUsed Tests ----

#[test]
fn get_access_key_last_used() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "keyuser")]))
        .unwrap();

    let req = make_request("CreateAccessKey", vec![("UserName", "keyuser")]);
    let resp = svc.create_access_key(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    let kid_start = body.find("<AccessKeyId>").unwrap() + 13;
    let kid_end = body.find("</AccessKeyId>").unwrap();
    let key_id = body[kid_start..kid_end].to_string();

    let req = make_request("GetAccessKeyLastUsed", vec![("AccessKeyId", &key_id)]);
    let resp = svc.get_access_key_last_used(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes());
    assert!(body.contains("<UserName>keyuser</UserName>"));
    // No last used info yet -- should show N/A
    assert!(body.contains("<ServiceName>N/A</ServiceName>"));
}

fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Err(e) => e,
        Ok(_) => panic!("expected error, got Ok"),
    }
}

// ── User CRUD ──

#[test]
fn create_get_delete_user() {
    let svc = make_service();

    let req = make_request("CreateUser", vec![("UserName", "alice")]);
    let resp = svc.create_user(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<UserName>alice</UserName>"));
    assert!(body.contains("<Arn>"));

    // Get
    let req = make_request("GetUser", vec![("UserName", "alice")]);
    let resp = svc.get_user(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<UserName>alice</UserName>"));

    // Delete
    let req = make_request("DeleteUser", vec![("UserName", "alice")]);
    svc.delete_user(&req).unwrap();

    // Get should fail
    let req = make_request("GetUser", vec![("UserName", "alice")]);
    let err = expect_err(svc.get_user(&req));
    assert!(err.to_string().contains("NoSuchEntity"));
}

#[test]
fn create_user_duplicate_fails() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "dup")]))
        .unwrap();

    let err = expect_err(svc.create_user(&make_request("CreateUser", vec![("UserName", "dup")])));
    assert!(err.to_string().contains("EntityAlreadyExists"));
}

#[test]
fn list_users_basic() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "u1")]))
        .unwrap();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "u2")]))
        .unwrap();

    let req = make_request("ListUsers", vec![]);
    let resp = svc.list_users(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<UserName>u1</UserName>"));
    assert!(body.contains("<UserName>u2</UserName>"));
}

fn count_occurrences(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

fn extract_marker(body: &str) -> Option<String> {
    let start = body.find("<Marker>")? + "<Marker>".len();
    let end = body[start..].find("</Marker>")? + start;
    Some(body[start..end].to_string())
}

#[test]
fn list_users_paginates_via_marker() {
    let svc = make_service();
    for i in 0..5 {
        svc.create_user(&make_request(
            "CreateUser",
            vec![("UserName", &format!("user-{i:02}"))],
        ))
        .unwrap();
    }

    // Page 1: MaxItems=2 -> 2 users, IsTruncated true, a Marker.
    let body1 = String::from_utf8_lossy(
        svc.list_users(&make_request("ListUsers", vec![("MaxItems", "2")]))
            .unwrap()
            .body
            .expect_bytes(),
    )
    .to_string();
    assert_eq!(count_occurrences(&body1, "<member>"), 2);
    assert!(body1.contains("<IsTruncated>true</IsTruncated>"));
    let marker = extract_marker(&body1).expect("Marker on page 1");

    // Page 2: feed the Marker back -> next 2 users.
    let body2 = String::from_utf8_lossy(
        svc.list_users(&make_request(
            "ListUsers",
            vec![("MaxItems", "2"), ("Marker", &marker)],
        ))
        .unwrap()
        .body
        .expect_bytes(),
    )
    .to_string();
    assert_eq!(count_occurrences(&body2, "<member>"), 2);
    assert!(body2.contains("<IsTruncated>true</IsTruncated>"));
    let marker = extract_marker(&body2).expect("Marker on page 2");

    // Page 3: last user, not truncated, no Marker.
    let body3 = String::from_utf8_lossy(
        svc.list_users(&make_request(
            "ListUsers",
            vec![("MaxItems", "2"), ("Marker", &marker)],
        ))
        .unwrap()
        .body
        .expect_bytes(),
    )
    .to_string();
    assert_eq!(count_occurrences(&body3, "<member>"), 1);
    assert!(body3.contains("<IsTruncated>false</IsTruncated>"));
    assert!(extract_marker(&body3).is_none());
}

#[test]
fn list_groups_paginates_via_marker() {
    let svc = make_service();
    for i in 0..3 {
        svc.create_group(&make_request(
            "CreateGroup",
            vec![("GroupName", &format!("grp-{i}"))],
        ))
        .unwrap();
    }
    let body1 = String::from_utf8_lossy(
        svc.list_groups(&make_request("ListGroups", vec![("MaxItems", "2")]))
            .unwrap()
            .body
            .expect_bytes(),
    )
    .to_string();
    assert_eq!(count_occurrences(&body1, "<member>"), 2);
    assert!(body1.contains("<IsTruncated>true</IsTruncated>"));
    let marker = extract_marker(&body1).expect("Marker");

    let body2 = String::from_utf8_lossy(
        svc.list_groups(&make_request(
            "ListGroups",
            vec![("MaxItems", "2"), ("Marker", &marker)],
        ))
        .unwrap()
        .body
        .expect_bytes(),
    )
    .to_string();
    assert_eq!(count_occurrences(&body2, "<member>"), 1);
    assert!(body2.contains("<IsTruncated>false</IsTruncated>"));
}

#[test]
fn list_policies_paginates_via_marker() {
    let svc = make_service();
    let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    for i in 0..3 {
        svc.create_policy(&make_request(
            "CreatePolicy",
            vec![("PolicyName", &format!("pol-{i}")), ("PolicyDocument", doc)],
        ))
        .unwrap();
    }
    let body1 = String::from_utf8_lossy(
        svc.list_policies(&make_request("ListPolicies", vec![("MaxItems", "2")]))
            .unwrap()
            .body
            .expect_bytes(),
    )
    .to_string();
    assert_eq!(count_occurrences(&body1, "<member>"), 2);
    assert!(body1.contains("<IsTruncated>true</IsTruncated>"));
    let marker = extract_marker(&body1).expect("Marker");

    let body2 = String::from_utf8_lossy(
        svc.list_policies(&make_request(
            "ListPolicies",
            vec![("MaxItems", "2"), ("Marker", &marker)],
        ))
        .unwrap()
        .body
        .expect_bytes(),
    )
    .to_string();
    assert_eq!(count_occurrences(&body2, "<member>"), 1);
    assert!(body2.contains("<IsTruncated>false</IsTruncated>"));
}

#[test]
fn create_user_with_path_and_tags() {
    let svc = make_service();
    let req = make_request(
        "CreateUser",
        vec![
            ("UserName", "tagged-user"),
            ("Path", "/engineering/"),
            ("Tags.member.1.Key", "team"),
            ("Tags.member.1.Value", "backend"),
        ],
    );
    let resp = svc.create_user(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("/engineering/"));
}

#[test]
fn user_permissions_boundary() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "bounded")]))
        .unwrap();

    let policy_arn = "arn:aws:iam::123456789012:policy/boundary";
    let req = make_request(
        "PutUserPermissionsBoundary",
        vec![("UserName", "bounded"), ("PermissionsBoundary", policy_arn)],
    );
    svc.put_user_permissions_boundary(&req).unwrap();

    let req = make_request("GetUser", vec![("UserName", "bounded")]);
    let resp = svc.get_user(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains(policy_arn));

    let req = make_request(
        "DeleteUserPermissionsBoundary",
        vec![("UserName", "bounded")],
    );
    svc.delete_user_permissions_boundary(&req).unwrap();
}

// ── Role CRUD ──

#[test]
fn create_get_delete_role() {
    let svc = make_service();

    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    let req = make_request(
        "CreateRole",
        vec![
            ("RoleName", "test-role"),
            ("AssumeRolePolicyDocument", trust),
        ],
    );
    let resp = svc.create_role(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<RoleName>test-role</RoleName>"));

    // Get
    let req = make_request("GetRole", vec![("RoleName", "test-role")]);
    let resp = svc.get_role(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<RoleName>test-role</RoleName>"));

    // Delete
    let req = make_request("DeleteRole", vec![("RoleName", "test-role")]);
    svc.delete_role(&req).unwrap();

    let err = expect_err(svc.get_role(&make_request("GetRole", vec![("RoleName", "test-role")])));
    assert!(err.to_string().contains("NoSuchEntity"));
}

#[test]
fn list_roles_basic() {
    let svc = make_service();
    let trust = r#"{"Version":"2012-10-17"}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![("RoleName", "r1"), ("AssumeRolePolicyDocument", trust)],
    ))
    .unwrap();
    svc.create_role(&make_request(
        "CreateRole",
        vec![("RoleName", "r2"), ("AssumeRolePolicyDocument", trust)],
    ))
    .unwrap();

    let resp = svc.list_roles(&make_request("ListRoles", vec![])).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<RoleName>r1</RoleName>"));
    assert!(body.contains("<RoleName>r2</RoleName>"));
}

#[test]
fn role_inline_policy_lifecycle() {
    let svc = make_service();
    let trust = r#"{"Version":"2012-10-17"}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![("RoleName", "ip-role"), ("AssumeRolePolicyDocument", trust)],
    ))
    .unwrap();

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    svc.put_role_policy(&make_request(
        "PutRolePolicy",
        vec![
            ("RoleName", "ip-role"),
            ("PolicyName", "s3-access"),
            ("PolicyDocument", policy),
        ],
    ))
    .unwrap();

    let resp = svc
        .get_role_policy(&make_request(
            "GetRolePolicy",
            vec![("RoleName", "ip-role"), ("PolicyName", "s3-access")],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<PolicyName>s3-access</PolicyName>"));

    let resp = svc
        .list_role_policies(&make_request(
            "ListRolePolicies",
            vec![("RoleName", "ip-role")],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("s3-access"));

    svc.delete_role_policy(&make_request(
        "DeleteRolePolicy",
        vec![("RoleName", "ip-role"), ("PolicyName", "s3-access")],
    ))
    .unwrap();
}

// ── Policy CRUD ──

#[test]
fn create_get_delete_policy() {
    let svc = make_service();

    let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
    let req = make_request(
        "CreatePolicy",
        vec![("PolicyName", "test-pol"), ("PolicyDocument", doc)],
    );
    let resp = svc.create_policy(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<PolicyName>test-pol</PolicyName>"));
    let arn = extract_xml_tag(&body, "Arn");

    // Get
    let resp = svc
        .get_policy(&make_request("GetPolicy", vec![("PolicyArn", arn)]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<PolicyName>test-pol</PolicyName>"));

    // Delete
    svc.delete_policy(&make_request("DeletePolicy", vec![("PolicyArn", arn)]))
        .unwrap();

    let err = expect_err(svc.get_policy(&make_request("GetPolicy", vec![("PolicyArn", arn)])));
    assert!(err.to_string().contains("NoSuchEntity"));
}

#[test]
fn list_policies_basic() {
    let svc = make_service();
    let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
    svc.create_policy(&make_request(
        "CreatePolicy",
        vec![("PolicyName", "p1"), ("PolicyDocument", doc)],
    ))
    .unwrap();

    let resp = svc
        .list_policies(&make_request("ListPolicies", vec![("Scope", "Local")]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<PolicyName>p1</PolicyName>"));
}

// ── Group CRUD ──

#[test]
fn create_get_delete_group() {
    let svc = make_service();

    svc.create_group(&make_request("CreateGroup", vec![("GroupName", "devs")]))
        .unwrap();

    let resp = svc
        .get_group(&make_request("GetGroup", vec![("GroupName", "devs")]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<GroupName>devs</GroupName>"));

    svc.delete_group(&make_request("DeleteGroup", vec![("GroupName", "devs")]))
        .unwrap();

    let err = expect_err(svc.get_group(&make_request("GetGroup", vec![("GroupName", "devs")])));
    assert!(err.to_string().contains("NoSuchEntity"));
}

#[test]
fn group_user_membership() {
    let svc = make_service();
    svc.create_group(&make_request("CreateGroup", vec![("GroupName", "team")]))
        .unwrap();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "member")]))
        .unwrap();

    svc.add_user_to_group(&make_request(
        "AddUserToGroup",
        vec![("GroupName", "team"), ("UserName", "member")],
    ))
    .unwrap();

    let resp = svc
        .get_group(&make_request("GetGroup", vec![("GroupName", "team")]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<UserName>member</UserName>"));

    svc.remove_user_from_group(&make_request(
        "RemoveUserFromGroup",
        vec![("GroupName", "team"), ("UserName", "member")],
    ))
    .unwrap();
}

#[test]
fn list_groups_basic() {
    let svc = make_service();
    svc.create_group(&make_request("CreateGroup", vec![("GroupName", "g1")]))
        .unwrap();
    svc.create_group(&make_request("CreateGroup", vec![("GroupName", "g2")]))
        .unwrap();

    let resp = svc
        .list_groups(&make_request("ListGroups", vec![]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("g1"));
    assert!(body.contains("g2"));
}

// ── Instance Profile CRUD ──

#[test]
fn instance_profile_lifecycle() {
    let svc = make_service();

    svc.create_instance_profile(&make_request(
        "CreateInstanceProfile",
        vec![("InstanceProfileName", "web-ip")],
    ))
    .unwrap();

    let resp = svc
        .get_instance_profile(&make_request(
            "GetInstanceProfile",
            vec![("InstanceProfileName", "web-ip")],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<InstanceProfileName>web-ip</InstanceProfileName>"));

    // Add role
    let trust = r#"{"Version":"2012-10-17"}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![
            ("RoleName", "ip-role2"),
            ("AssumeRolePolicyDocument", trust),
        ],
    ))
    .unwrap();
    svc.add_role_to_instance_profile(&make_request(
        "AddRoleToInstanceProfile",
        vec![("InstanceProfileName", "web-ip"), ("RoleName", "ip-role2")],
    ))
    .unwrap();

    // List
    let resp = svc
        .list_instance_profiles(&make_request("ListInstanceProfiles", vec![]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("web-ip"));

    // Remove role and delete
    svc.remove_role_from_instance_profile(&make_request(
        "RemoveRoleFromInstanceProfile",
        vec![("InstanceProfileName", "web-ip"), ("RoleName", "ip-role2")],
    ))
    .unwrap();
    svc.delete_instance_profile(&make_request(
        "DeleteInstanceProfile",
        vec![("InstanceProfileName", "web-ip")],
    ))
    .unwrap();
}

// ── OIDC Provider CRUD ──

#[test]
fn oidc_provider_lifecycle() {
    let svc = make_service();

    let req = make_request(
        "CreateOpenIDConnectProvider",
        vec![
            ("Url", "https://oidc.example.com"),
            (
                "ThumbprintList.member.1",
                "aabbccddeeff00112233aabbccddeeff00112233",
            ),
            ("ClientIDList.member.1", "my-client"),
        ],
    );
    let resp = svc.create_oidc_provider(&req).unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    let arn = extract_xml_tag(&body, "OpenIDConnectProviderArn");

    let resp = svc
        .get_oidc_provider(&make_request(
            "GetOpenIDConnectProvider",
            vec![("OpenIDConnectProviderArn", arn)],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("oidc.example.com"));
    assert!(body.contains("my-client"));

    let resp = svc
        .list_oidc_providers(&make_request("ListOpenIDConnectProviders", vec![]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains(arn));

    svc.delete_oidc_provider(&make_request(
        "DeleteOpenIDConnectProvider",
        vec![("OpenIDConnectProviderArn", arn)],
    ))
    .unwrap();
}

// ── Access Key lifecycle ──

#[test]
fn access_key_create_update_delete() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "akuser")]))
        .unwrap();

    let resp = svc
        .create_access_key(&make_request(
            "CreateAccessKey",
            vec![("UserName", "akuser")],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    let key_id = extract_xml_tag(&body, "AccessKeyId");
    assert!(body.contains("<Status>Active</Status>"));

    // Update to Inactive
    svc.update_access_key(&make_request(
        "UpdateAccessKey",
        vec![
            ("UserName", "akuser"),
            ("AccessKeyId", key_id),
            ("Status", "Inactive"),
        ],
    ))
    .unwrap();

    // List and verify
    let resp = svc
        .list_access_keys(&make_request(
            "ListAccessKeys",
            vec![("UserName", "akuser")],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<Status>Inactive</Status>"));

    // Delete
    svc.delete_access_key(&make_request(
        "DeleteAccessKey",
        vec![("UserName", "akuser"), ("AccessKeyId", key_id)],
    ))
    .unwrap();

    let resp = svc
        .list_access_keys(&make_request(
            "ListAccessKeys",
            vec![("UserName", "akuser")],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(!body.contains("<AccessKeyId>"));
}

// ── Tag/Untag User ──

#[test]
fn tag_untag_list_user_tags() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "tuser")]))
        .unwrap();

    svc.tag_user(&make_request(
        "TagUser",
        vec![
            ("UserName", "tuser"),
            ("Tags.member.1.Key", "dept"),
            ("Tags.member.1.Value", "eng"),
        ],
    ))
    .unwrap();

    let resp = svc
        .list_user_tags(&make_request("ListUserTags", vec![("UserName", "tuser")]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("<Key>dept</Key>"));
    assert!(body.contains("<Value>eng</Value>"));

    svc.untag_user(&make_request(
        "UntagUser",
        vec![("UserName", "tuser"), ("TagKeys.member.1", "dept")],
    ))
    .unwrap();

    let resp = svc
        .list_user_tags(&make_request("ListUserTags", vec![("UserName", "tuser")]))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(!body.contains("<Key>dept</Key>"));
}

// ── Attach/Detach role policy ──

#[test]
fn attach_detach_list_role_policies_managed() {
    let svc = make_service();
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![
            ("RoleName", "pol-role"),
            ("AssumeRolePolicyDocument", trust),
        ],
    ))
    .unwrap();

    // Create a local policy to attach
    let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let resp = svc
        .create_policy(&make_request(
            "CreatePolicy",
            vec![("PolicyName", "role-pol"), ("PolicyDocument", doc)],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    let policy_arn = extract_xml_tag(&body, "Arn").to_string();

    svc.attach_role_policy(&make_request(
        "AttachRolePolicy",
        vec![("RoleName", "pol-role"), ("PolicyArn", &policy_arn)],
    ))
    .unwrap();

    let resp = svc
        .list_attached_role_policies(&make_request(
            "ListAttachedRolePolicies",
            vec![("RoleName", "pol-role")],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(body.contains("role-pol"));

    svc.detach_role_policy(&make_request(
        "DetachRolePolicy",
        vec![("RoleName", "pol-role"), ("PolicyArn", &policy_arn)],
    ))
    .unwrap();
}

#[test]
fn list_attached_role_policies_includes_aws_managed() {
    let svc = make_service();
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    svc.create_role(&make_request(
        "CreateRole",
        vec![
            ("RoleName", "task-exec"),
            ("AssumeRolePolicyDocument", trust),
        ],
    ))
    .unwrap();

    let managed_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy";
    svc.attach_role_policy(&make_request(
        "AttachRolePolicy",
        vec![("RoleName", "task-exec"), ("PolicyArn", managed_arn)],
    ))
    .unwrap();

    let resp = svc
        .list_attached_role_policies(&make_request(
            "ListAttachedRolePolicies",
            vec![("RoleName", "task-exec")],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(
        body.contains(managed_arn),
        "managed policy ARN missing from response: {body}"
    );
    assert!(
        body.contains("<PolicyName>AmazonECSTaskExecutionRolePolicy</PolicyName>"),
        "managed policy name missing from response: {body}"
    );
}

#[test]
fn list_attached_user_policies_includes_aws_managed() {
    let svc = make_service();
    svc.create_user(&make_request(
        "CreateUser",
        vec![("UserName", "managed-user")],
    ))
    .unwrap();

    let managed_arn = "arn:aws:iam::aws:policy/AdministratorAccess";
    svc.attach_user_policy(&make_request(
        "AttachUserPolicy",
        vec![("UserName", "managed-user"), ("PolicyArn", managed_arn)],
    ))
    .unwrap();

    let resp = svc
        .list_attached_user_policies(&make_request(
            "ListAttachedUserPolicies",
            vec![("UserName", "managed-user")],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(
        body.contains(managed_arn),
        "managed policy ARN missing from response: {body}"
    );
    assert!(
        body.contains("<PolicyName>AdministratorAccess</PolicyName>"),
        "managed policy name missing from response: {body}"
    );
}

#[test]
fn list_attached_group_policies_includes_aws_managed() {
    let svc = make_service();
    svc.create_group(&make_request(
        "CreateGroup",
        vec![("GroupName", "managed-grp")],
    ))
    .unwrap();

    let managed_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess";
    svc.attach_group_policy(&make_request(
        "AttachGroupPolicy",
        vec![("GroupName", "managed-grp"), ("PolicyArn", managed_arn)],
    ))
    .unwrap();

    let resp = svc
        .list_attached_group_policies(&make_request(
            "ListAttachedGroupPolicies",
            vec![("GroupName", "managed-grp")],
        ))
        .unwrap();
    let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
    assert!(
        body.contains(managed_arn),
        "managed policy ARN missing from response: {body}"
    );
    assert!(
        body.contains("<PolicyName>ReadOnlyAccess</PolicyName>"),
        "managed policy name missing from response: {body}"
    );
}

// ── users.rs additional coverage ──

#[test]
fn create_user_missing_name_errors() {
    let svc = make_service();
    let req = make_request("CreateUser", vec![]);
    assert!(svc.create_user(&req).is_err());
}

#[test]
fn create_user_duplicate_errors() {
    let svc = make_service();
    let req = make_request("CreateUser", vec![("UserName", "dup")]);
    svc.create_user(&req).unwrap();
    assert!(svc.create_user(&req).is_err());
}

#[test]
fn get_user_unknown_errors() {
    let svc = make_service();
    let req = make_request("GetUser", vec![("UserName", "ghost")]);
    assert!(svc.get_user(&req).is_err());
}

#[test]
fn delete_user_unknown_errors() {
    let svc = make_service();
    let req = make_request("DeleteUser", vec![("UserName", "ghost")]);
    assert!(svc.delete_user(&req).is_err());
}

#[test]
fn update_user_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "UpdateUser",
        vec![("UserName", "ghost"), ("NewUserName", "new")],
    );
    assert!(svc.update_user(&req).is_err());
}

#[test]
fn tag_user_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "TagUser",
        vec![
            ("UserName", "ghost"),
            ("Tags.member.1.Key", "k"),
            ("Tags.member.1.Value", "v"),
        ],
    );
    assert!(svc.tag_user(&req).is_err());
}

#[test]
fn list_user_tags_unknown_errors() {
    let svc = make_service();
    let req = make_request("ListUserTags", vec![("UserName", "ghost")]);
    assert!(svc.list_user_tags(&req).is_err());
}

#[test]
fn create_access_key_unknown_user_errors() {
    let svc = make_service();
    let req = make_request("CreateAccessKey", vec![("UserName", "ghost")]);
    assert!(svc.create_access_key(&req).is_err());
}

#[test]
fn delete_access_key_unknown_user_errors() {
    let svc = make_service();
    let req = make_request(
        "DeleteAccessKey",
        vec![("UserName", "ghost"), ("AccessKeyId", "AKIAEXAMPLE")],
    );
    assert!(svc.delete_access_key(&req).is_err());
}

#[test]
fn create_login_profile_unknown_user_errors() {
    let svc = make_service();
    let req = make_request(
        "CreateLoginProfile",
        vec![("UserName", "ghost"), ("Password", "Pass123!")],
    );
    assert!(svc.create_login_profile(&req).is_err());
}

#[test]
fn get_login_profile_not_found() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "lp")]))
        .unwrap();
    let req = make_request("GetLoginProfile", vec![("UserName", "lp")]);
    assert!(svc.get_login_profile(&req).is_err());
}

#[test]
fn delete_login_profile_not_found() {
    let svc = make_service();
    let req = make_request("DeleteLoginProfile", vec![("UserName", "ghost")]);
    assert!(svc.delete_login_profile(&req).is_err());
}

#[test]
fn upload_signing_cert_unknown_user_errors() {
    let svc = make_service();
    let req = make_request(
        "UploadSigningCertificate",
        vec![
            ("UserName", "ghost"),
            ("CertificateBody", "-----BEGIN CERT-----"),
        ],
    );
    assert!(svc.upload_signing_certificate(&req).is_err());
}

#[test]
fn upload_ssh_public_key_unknown_user_errors() {
    let svc = make_service();
    let req = make_request(
        "UploadSSHPublicKey",
        vec![("UserName", "ghost"), ("SSHPublicKeyBody", "ssh-rsa AAA")],
    );
    assert!(svc.upload_ssh_public_key(&req).is_err());
}

#[test]
fn get_ssh_public_key_not_found() {
    let svc = make_service();
    let req = make_request(
        "GetSSHPublicKey",
        vec![
            ("UserName", "ghost"),
            ("SSHPublicKeyId", "ASDF"),
            ("Encoding", "SSH"),
        ],
    );
    assert!(svc.get_ssh_public_key(&req).is_err());
}

#[test]
fn delete_ssh_public_key_not_found() {
    let svc = make_service();
    let req = make_request(
        "DeleteSSHPublicKey",
        vec![("UserName", "ghost"), ("SSHPublicKeyId", "ASDF")],
    );
    assert!(svc.delete_ssh_public_key(&req).is_err());
}

#[test]
fn attach_user_policy_unknown_user_errors() {
    let svc = make_service();
    let req = make_request(
        "AttachUserPolicy",
        vec![
            ("UserName", "ghost"),
            ("PolicyArn", "arn:aws:iam::aws:policy/ReadOnlyAccess"),
        ],
    );
    assert!(svc.attach_user_policy(&req).is_err());
}

#[test]
fn detach_user_policy_unknown_user_errors() {
    let svc = make_service();
    let req = make_request(
        "DetachUserPolicy",
        vec![
            ("UserName", "ghost"),
            ("PolicyArn", "arn:aws:iam::aws:policy/ReadOnlyAccess"),
        ],
    );
    assert!(svc.detach_user_policy(&req).is_err());
}

#[test]
fn list_attached_user_policies_unknown_user_errors() {
    let svc = make_service();
    let req = make_request("ListAttachedUserPolicies", vec![("UserName", "ghost")]);
    assert!(svc.list_attached_user_policies(&req).is_err());
}

// ── roles.rs additional ──

#[test]
fn create_role_missing_trust_errors() {
    let svc = make_service();
    let req = make_request("CreateRole", vec![("RoleName", "r1")]);
    assert!(svc.create_role(&req).is_err());
}

#[test]
fn create_role_duplicate_errors() {
    let svc = make_service();
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    let req = make_request(
        "CreateRole",
        vec![("RoleName", "rd"), ("AssumeRolePolicyDocument", trust)],
    );
    svc.create_role(&req).unwrap();
    assert!(svc.create_role(&req).is_err());
}

#[test]
fn get_role_unknown_errors() {
    let svc = make_service();
    let req = make_request("GetRole", vec![("RoleName", "ghost")]);
    assert!(svc.get_role(&req).is_err());
}

#[test]
fn delete_role_unknown_errors() {
    let svc = make_service();
    let req = make_request("DeleteRole", vec![("RoleName", "ghost")]);
    assert!(svc.delete_role(&req).is_err());
}

#[test]
fn update_assume_role_policy_unknown_role_errors() {
    let svc = make_service();
    let req = make_request(
        "UpdateAssumeRolePolicy",
        vec![("RoleName", "ghost"), ("PolicyDocument", "{}")],
    );
    assert!(svc.update_assume_role_policy(&req).is_err());
}

#[test]
fn tag_role_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "TagRole",
        vec![
            ("RoleName", "ghost"),
            ("Tags.member.1.Key", "k"),
            ("Tags.member.1.Value", "v"),
        ],
    );
    assert!(svc.tag_role(&req).is_err());
}

// ── groups.rs additional ──

#[test]
fn create_group_duplicate_errors() {
    let svc = make_service();
    let req = make_request("CreateGroup", vec![("GroupName", "g1")]);
    svc.create_group(&req).unwrap();
    assert!(svc.create_group(&req).is_err());
}

#[test]
fn get_group_unknown_errors() {
    let svc = make_service();
    let req = make_request("GetGroup", vec![("GroupName", "ghost")]);
    assert!(svc.get_group(&req).is_err());
}

#[test]
fn delete_group_unknown_errors() {
    let svc = make_service();
    let req = make_request("DeleteGroup", vec![("GroupName", "ghost")]);
    assert!(svc.delete_group(&req).is_err());
}

#[test]
fn add_user_to_unknown_group_errors() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "u1")]))
        .unwrap();
    let req = make_request(
        "AddUserToGroup",
        vec![("GroupName", "ghost"), ("UserName", "u1")],
    );
    assert!(svc.add_user_to_group(&req).is_err());
}

#[test]
fn add_unknown_user_to_group_errors() {
    let svc = make_service();
    svc.create_group(&make_request("CreateGroup", vec![("GroupName", "ug")]))
        .unwrap();
    let req = make_request(
        "AddUserToGroup",
        vec![("GroupName", "ug"), ("UserName", "ghost")],
    );
    assert!(svc.add_user_to_group(&req).is_err());
}

// ── policies.rs additional ──

#[test]
fn create_policy_duplicate_errors() {
    let svc = make_service();
    let doc =
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;
    let req = make_request(
        "CreatePolicy",
        vec![("PolicyName", "dp"), ("PolicyDocument", doc)],
    );
    svc.create_policy(&req).unwrap();
    assert!(svc.create_policy(&req).is_err());
}

#[test]
fn delete_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "DeletePolicy",
        vec![("PolicyArn", "arn:aws:iam::123456789012:policy/ghost")],
    );
    assert!(svc.delete_policy(&req).is_err());
}

#[test]
fn get_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "GetPolicy",
        vec![("PolicyArn", "arn:aws:iam::123456789012:policy/ghost")],
    );
    assert!(svc.get_policy(&req).is_err());
}

// ── instance profiles ──

#[test]
fn create_instance_profile_duplicate() {
    let svc = make_service();
    let req = make_request(
        "CreateInstanceProfile",
        vec![("InstanceProfileName", "ip1")],
    );
    svc.create_instance_profile(&req).unwrap();
    assert!(svc.create_instance_profile(&req).is_err());
}

#[test]
fn delete_instance_profile_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "DeleteInstanceProfile",
        vec![("InstanceProfileName", "ghost")],
    );
    assert!(svc.delete_instance_profile(&req).is_err());
}

// ── instance_profiles additional ──

#[test]
fn get_instance_profile_unknown_errors() {
    let svc = make_service();
    let req = make_request("GetInstanceProfile", vec![("InstanceProfileName", "ghost")]);
    assert!(svc.get_instance_profile(&req).is_err());
}

#[test]
fn list_instance_profiles_empty_returns_ok() {
    let svc = make_service();
    let req = make_request("ListInstanceProfiles", vec![]);
    let resp = svc.list_instance_profiles(&req).unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[test]
fn add_role_to_instance_profile_unknown_profile_errors() {
    let svc = make_service();
    let req = make_request(
        "AddRoleToInstanceProfile",
        vec![("InstanceProfileName", "ghost"), ("RoleName", "r")],
    );
    assert!(svc.add_role_to_instance_profile(&req).is_err());
}

#[test]
fn remove_role_from_instance_profile_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "RemoveRoleFromInstanceProfile",
        vec![("InstanceProfileName", "ghost"), ("RoleName", "r")],
    );
    assert!(svc.remove_role_from_instance_profile(&req).is_err());
}

#[test]
fn list_instance_profiles_for_role_unknown_errors() {
    let svc = make_service();
    let req = make_request("ListInstanceProfilesForRole", vec![("RoleName", "ghost")]);
    assert!(svc.list_instance_profiles_for_role(&req).is_err());
}

#[test]
fn list_instance_profile_tags_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "ListInstanceProfileTags",
        vec![("InstanceProfileName", "ghost")],
    );
    assert!(svc.list_instance_profile_tags(&req).is_err());
}

// ── OIDC/SAML error branches ──

#[test]
fn get_saml_provider_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "GetSAMLProvider",
        vec![(
            "SAMLProviderArn",
            "arn:aws:iam::123456789012:saml-provider/ghost",
        )],
    );
    assert!(svc.get_saml_provider(&req).is_err());
}

#[test]
fn delete_saml_provider_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "DeleteSAMLProvider",
        vec![(
            "SAMLProviderArn",
            "arn:aws:iam::123456789012:saml-provider/ghost",
        )],
    );
    assert!(svc.delete_saml_provider(&req).is_err());
}

#[test]
fn update_saml_provider_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "UpdateSAMLProvider",
        vec![
            (
                "SAMLProviderArn",
                "arn:aws:iam::123456789012:saml-provider/ghost",
            ),
            ("SAMLMetadataDocument", "<EntityDescriptor>xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</EntityDescriptor>"),
        ],
    );
    assert!(svc.update_saml_provider(&req).is_err());
}

#[test]
fn get_oidc_provider_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "GetOpenIDConnectProvider",
        vec![(
            "OpenIDConnectProviderArn",
            "arn:aws:iam::123456789012:oidc-provider/ghost",
        )],
    );
    assert!(svc.get_oidc_provider(&req).is_err());
}

// ── policies: version operations ──

#[test]
fn create_policy_version_unknown_errors() {
    let svc = make_service();
    let doc =
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;
    let req = make_request(
        "CreatePolicyVersion",
        vec![
            ("PolicyArn", "arn:aws:iam::123456789012:policy/ghost"),
            ("PolicyDocument", doc),
        ],
    );
    assert!(svc.create_policy_version(&req).is_err());
}

#[test]
fn list_policy_versions_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "ListPolicyVersions",
        vec![("PolicyArn", "arn:aws:iam::123456789012:policy/ghost")],
    );
    assert!(svc.list_policy_versions(&req).is_err());
}

#[test]
fn get_policy_version_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "GetPolicyVersion",
        vec![
            ("PolicyArn", "arn:aws:iam::123456789012:policy/ghost"),
            ("VersionId", "v1"),
        ],
    );
    assert!(svc.get_policy_version(&req).is_err());
}

#[test]
fn delete_policy_version_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "DeletePolicyVersion",
        vec![
            ("PolicyArn", "arn:aws:iam::123456789012:policy/ghost"),
            ("VersionId", "v1"),
        ],
    );
    assert!(svc.delete_policy_version(&req).is_err());
}

// ── roles: put/get/delete_role_policy inline ──

#[test]
fn put_role_policy_unknown_role_errors() {
    let svc = make_service();
    let req = make_request(
        "PutRolePolicy",
        vec![
            ("RoleName", "ghost"),
            ("PolicyName", "p1"),
            ("PolicyDocument", "{}"),
        ],
    );
    assert!(svc.put_role_policy(&req).is_err());
}

#[test]
fn get_role_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "GetRolePolicy",
        vec![("RoleName", "ghost"), ("PolicyName", "p1")],
    );
    assert!(svc.get_role_policy(&req).is_err());
}

#[test]
fn delete_role_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "DeleteRolePolicy",
        vec![("RoleName", "ghost"), ("PolicyName", "p1")],
    );
    assert!(svc.delete_role_policy(&req).is_err());
}

#[test]
fn list_role_policies_unknown_errors() {
    let svc = make_service();
    let req = make_request("ListRolePolicies", vec![("RoleName", "ghost")]);
    assert!(svc.list_role_policies(&req).is_err());
}

// ── user inline policies ──

#[test]
fn put_user_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "PutUserPolicy",
        vec![
            ("UserName", "ghost"),
            ("PolicyName", "p"),
            ("PolicyDocument", "{}"),
        ],
    );
    assert!(svc.put_user_policy(&req).is_err());
}

#[test]
fn list_user_policies_unknown_errors() {
    let svc = make_service();
    let req = make_request("ListUserPolicies", vec![("UserName", "ghost")]);
    assert!(svc.list_user_policies(&req).is_err());
}

#[test]
fn get_user_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "GetUserPolicy",
        vec![("UserName", "ghost"), ("PolicyName", "p")],
    );
    assert!(svc.get_user_policy(&req).is_err());
}

#[test]
fn delete_user_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "DeleteUserPolicy",
        vec![("UserName", "ghost"), ("PolicyName", "p")],
    );
    assert!(svc.delete_user_policy(&req).is_err());
}

#[test]
fn put_group_policy_unknown_group_errors() {
    let svc = make_service();
    let req = make_request(
        "PutGroupPolicy",
        vec![
            ("GroupName", "ghost"),
            ("PolicyName", "p"),
            ("PolicyDocument", "{}"),
        ],
    );
    assert!(svc.put_group_policy(&req).is_err());
}

#[test]
fn get_group_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "GetGroupPolicy",
        vec![("GroupName", "ghost"), ("PolicyName", "p")],
    );
    assert!(svc.get_group_policy(&req).is_err());
}

#[test]
fn delete_group_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "DeleteGroupPolicy",
        vec![("GroupName", "ghost"), ("PolicyName", "p")],
    );
    assert!(svc.delete_group_policy(&req).is_err());
}

#[test]
fn list_group_policies_unknown_errors() {
    let svc = make_service();
    let req = make_request("ListGroupPolicies", vec![("GroupName", "ghost")]);
    assert!(svc.list_group_policies(&req).is_err());
}

#[test]
fn attach_group_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "AttachGroupPolicy",
        vec![
            ("GroupName", "ghost"),
            ("PolicyArn", "arn:aws:iam::aws:policy/ReadOnlyAccess"),
        ],
    );
    assert!(svc.attach_group_policy(&req).is_err());
}

#[test]
fn detach_group_policy_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "DetachGroupPolicy",
        vec![
            ("GroupName", "ghost"),
            ("PolicyArn", "arn:aws:iam::aws:policy/ReadOnlyAccess"),
        ],
    );
    assert!(svc.detach_group_policy(&req).is_err());
}

#[test]
fn list_attached_group_policies_unknown_errors() {
    let svc = make_service();
    let req = make_request("ListAttachedGroupPolicies", vec![("GroupName", "ghost")]);
    assert!(svc.list_attached_group_policies(&req).is_err());
}

#[test]
fn list_groups_for_user_unknown_errors() {
    let svc = make_service();
    let req = make_request("ListGroupsForUser", vec![("UserName", "ghost")]);
    assert!(svc.list_groups_for_user(&req).is_err());
}

#[test]
fn get_account_summary_returns_ok() {
    let svc = make_service();
    let req = make_request("GetAccountSummary", vec![]);
    let resp = svc.get_account_summary(&req).unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[test]
fn list_policies_empty_ok() {
    let svc = make_service();
    let req = make_request("ListPolicies", vec![]);
    let resp = svc.list_policies(&req).unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[test]
fn list_users_empty_ok() {
    let svc = make_service();
    let req = make_request("ListUsers", vec![]);
    let resp = svc.list_users(&req).unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[test]
fn list_roles_empty_ok() {
    let svc = make_service();
    let req = make_request("ListRoles", vec![]);
    let resp = svc.list_roles(&req).unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[test]
fn list_groups_empty_ok() {
    let svc = make_service();
    let req = make_request("ListGroups", vec![]);
    let resp = svc.list_groups(&req).unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[test]
fn list_attached_role_policies_unknown_errors() {
    let svc = make_service();
    let req = make_request("ListAttachedRolePolicies", vec![("RoleName", "ghost")]);
    assert!(svc.list_attached_role_policies(&req).is_err());
}

#[test]
fn attach_role_policy_unknown_role_errors() {
    let svc = make_service();
    let req = make_request(
        "AttachRolePolicy",
        vec![
            ("RoleName", "ghost"),
            ("PolicyArn", "arn:aws:iam::aws:policy/ReadOnlyAccess"),
        ],
    );
    assert!(svc.attach_role_policy(&req).is_err());
}

#[test]
fn detach_role_policy_unknown_role_errors() {
    let svc = make_service();
    let req = make_request(
        "DetachRolePolicy",
        vec![
            ("RoleName", "ghost"),
            ("PolicyArn", "arn:aws:iam::aws:policy/ReadOnlyAccess"),
        ],
    );
    assert!(svc.detach_role_policy(&req).is_err());
}

#[test]
fn update_role_description_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "UpdateRole",
        vec![("RoleName", "ghost"), ("Description", "new")],
    );
    assert!(svc.update_role(&req).is_err());
}

// ── SAML / OIDC providers + update paths (oidc.rs) ────────────────

#[test]
fn saml_provider_full_lifecycle() {
    let svc = make_service();

    let create = svc
        .create_saml_provider(&make_request(
            "CreateSAMLProvider",
            vec![
                ("Name", "my-saml"),
                ("SAMLMetadataDocument", "<EntityDescriptor>xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</EntityDescriptor>"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(create.body.expect_bytes()).unwrap();
    let arn = extract_xml_tag(body, "SAMLProviderArn").to_string();
    assert!(arn.ends_with(":saml-provider/my-saml"));

    let get = svc
        .get_saml_provider(&make_request(
            "GetSAMLProvider",
            vec![("SAMLProviderArn", &arn)],
        ))
        .unwrap();
    let get_body = std::str::from_utf8(get.body.expect_bytes()).unwrap();
    assert!(get_body.contains("&lt;EntityDescriptor&gt;"));

    let list = svc
        .list_saml_providers(&make_request("ListSAMLProviders", vec![]))
        .unwrap();
    let list_body = std::str::from_utf8(list.body.expect_bytes()).unwrap();
    assert!(list_body.contains(&arn));

    let update = svc
        .update_saml_provider(&make_request(
            "UpdateSAMLProvider",
            vec![
                ("SAMLProviderArn", &arn),
                ("SAMLMetadataDocument", "<EntityDescriptor>xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</EntityDescriptor>"),
            ],
        ))
        .unwrap();
    let update_body = std::str::from_utf8(update.body.expect_bytes()).unwrap();
    assert!(update_body.contains(&arn));

    svc.delete_saml_provider(&make_request(
        "DeleteSAMLProvider",
        vec![("SAMLProviderArn", &arn)],
    ))
    .unwrap();
    assert!(svc
        .get_saml_provider(&make_request(
            "GetSAMLProvider",
            vec![("SAMLProviderArn", &arn)]
        ))
        .is_err());
}

#[test]
fn oidc_provider_duplicate_rejected() {
    let svc = make_service();
    let params = vec![
        ("Url", "https://example.com"),
        (
            "ThumbprintList.member.1",
            "abcdef1234567890abcdef1234567890abcdef12",
        ),
        ("ClientIDList.member.1", "client-1"),
    ];
    svc.create_oidc_provider(&make_request("CreateOpenIDConnectProvider", params.clone()))
        .unwrap();
    let err = svc
        .create_oidc_provider(&make_request("CreateOpenIDConnectProvider", params))
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
}

#[test]
fn oidc_add_and_remove_client_id() {
    let svc = make_service();
    let create = svc
        .create_oidc_provider(&make_request(
            "CreateOpenIDConnectProvider",
            vec![
                ("Url", "https://client-ops.example.com"),
                (
                    "ThumbprintList.member.1",
                    "abcdef1234567890abcdef1234567890abcdef12",
                ),
                ("ClientIDList.member.1", "original-client"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(create.body.expect_bytes()).unwrap();
    let arn = extract_xml_tag(body, "OpenIDConnectProviderArn").to_string();

    svc.add_client_id_to_oidc(&make_request(
        "AddClientIDToOpenIDConnectProvider",
        vec![
            ("OpenIDConnectProviderArn", &arn),
            ("ClientID", "new-client"),
        ],
    ))
    .unwrap();

    let get = svc
        .get_oidc_provider(&make_request(
            "GetOpenIDConnectProvider",
            vec![("OpenIDConnectProviderArn", &arn)],
        ))
        .unwrap();
    let get_body = std::str::from_utf8(get.body.expect_bytes()).unwrap();
    assert!(get_body.contains("new-client"));

    svc.remove_client_id_from_oidc(&make_request(
        "RemoveClientIDFromOpenIDConnectProvider",
        vec![
            ("OpenIDConnectProviderArn", &arn),
            ("ClientID", "new-client"),
        ],
    ))
    .unwrap();
    let get2 = svc
        .get_oidc_provider(&make_request(
            "GetOpenIDConnectProvider",
            vec![("OpenIDConnectProviderArn", &arn)],
        ))
        .unwrap();
    let get2_body = std::str::from_utf8(get2.body.expect_bytes()).unwrap();
    assert!(!get2_body.contains("new-client"));
}

#[test]
fn oidc_add_client_id_unknown_arn_errors() {
    let svc = make_service();
    let err = svc
        .add_client_id_to_oidc(&make_request(
            "AddClientIDToOpenIDConnectProvider",
            vec![
                (
                    "OpenIDConnectProviderArn",
                    "arn:aws:iam::123:oidc-provider/ghost",
                ),
                ("ClientID", "c"),
            ],
        ))
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[test]
fn oidc_remove_client_id_unknown_arn_errors() {
    let svc = make_service();
    let err = svc
        .remove_client_id_from_oidc(&make_request(
            "RemoveClientIDFromOpenIDConnectProvider",
            vec![
                (
                    "OpenIDConnectProviderArn",
                    "arn:aws:iam::123:oidc-provider/ghost",
                ),
                ("ClientID", "c"),
            ],
        ))
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[test]
fn oidc_update_thumbprint_unknown_arn_errors() {
    let svc = make_service();
    let err = svc
        .update_oidc_thumbprint(&make_request(
            "UpdateOpenIDConnectProviderThumbprint",
            vec![
                (
                    "OpenIDConnectProviderArn",
                    "arn:aws:iam::123:oidc-provider/ghost",
                ),
                (
                    "ThumbprintList.member.1",
                    "abcdef1234567890abcdef1234567890abcdef12",
                ),
            ],
        ))
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[test]
fn oidc_update_thumbprint_succeeds() {
    let svc = make_service();
    let create = svc
        .create_oidc_provider(&make_request(
            "CreateOpenIDConnectProvider",
            vec![
                ("Url", "https://thumb.example.com"),
                (
                    "ThumbprintList.member.1",
                    "abcdef1234567890abcdef1234567890abcdef12",
                ),
                ("ClientIDList.member.1", "c"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(create.body.expect_bytes()).unwrap();
    let arn = extract_xml_tag(body, "OpenIDConnectProviderArn").to_string();

    svc.update_oidc_thumbprint(&make_request(
        "UpdateOpenIDConnectProviderThumbprint",
        vec![
            ("OpenIDConnectProviderArn", &arn),
            (
                "ThumbprintList.member.1",
                "fedcba0987654321fedcba0987654321fedcba09",
            ),
        ],
    ))
    .unwrap();

    let get = svc
        .get_oidc_provider(&make_request(
            "GetOpenIDConnectProvider",
            vec![("OpenIDConnectProviderArn", &arn)],
        ))
        .unwrap();
    let get_body = std::str::from_utf8(get.body.expect_bytes()).unwrap();
    assert!(get_body.contains("fedcba0987654321"));
}

#[test]
fn list_oidc_providers_includes_created() {
    let svc = make_service();
    svc.create_oidc_provider(&make_request(
        "CreateOpenIDConnectProvider",
        vec![
            ("Url", "https://list.example.com"),
            (
                "ThumbprintList.member.1",
                "abcdef1234567890abcdef1234567890abcdef12",
            ),
            ("ClientIDList.member.1", "cx"),
        ],
    ))
    .unwrap();

    let list = svc
        .list_oidc_providers(&make_request("ListOpenIDConnectProviders", vec![]))
        .unwrap();
    let list_body = std::str::from_utf8(list.body.expect_bytes()).unwrap();
    assert!(list_body.contains("list.example.com"));
}

// ── Tests for extras handlers (new ops added to close conformance gap) ──

#[test]
fn service_specific_credentials_lifecycle() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "u1")]))
        .unwrap();
    svc.create_service_specific_credential(&make_request(
        "CreateServiceSpecificCredential",
        vec![
            ("UserName", "u1"),
            ("ServiceName", "codecommit.amazonaws.com"),
        ],
    ))
    .unwrap();
    svc.list_service_specific_credentials(&make_request(
        "ListServiceSpecificCredentials",
        vec![("UserName", "u1")],
    ))
    .unwrap();
}

#[test]
fn organizations_root_creds_and_sessions() {
    let svc = make_service();
    svc.enable_organizations_root_credentials_management(&make_request(
        "EnableOrganizationsRootCredentialsManagement",
        vec![],
    ))
    .unwrap();
    svc.disable_organizations_root_credentials_management(&make_request(
        "DisableOrganizationsRootCredentialsManagement",
        vec![],
    ))
    .unwrap();
    svc.enable_organizations_root_sessions(&make_request(
        "EnableOrganizationsRootSessions",
        vec![],
    ))
    .unwrap();
    svc.disable_organizations_root_sessions(&make_request(
        "DisableOrganizationsRootSessions",
        vec![],
    ))
    .unwrap();
    svc.list_organizations_features(&make_request("ListOrganizationsFeatures", vec![]))
        .unwrap();
}

#[test]
fn service_last_accessed_jobs() {
    let svc = make_service();
    let resp = svc
        .generate_service_last_accessed_details(&make_request(
            "GenerateServiceLastAccessedDetails",
            vec![("Arn", "arn:aws:iam::123456789012:user/u1")],
        ))
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    let job_id = extract_xml_tag(&body, "JobId");
    svc.get_service_last_accessed_details(&make_request(
        "GetServiceLastAccessedDetails",
        vec![("JobId", job_id)],
    ))
    .unwrap();
    svc.get_service_last_accessed_details_with_entities(&make_request(
        "GetServiceLastAccessedDetailsWithEntities",
        vec![("JobId", job_id), ("ServiceNamespace", "s3")],
    ))
    .unwrap();
}

#[test]
fn extra_tagging_surfaces() {
    let svc = make_service();
    // Pre-create entities the tag handlers expect
    svc.create_saml_provider(&make_request(
        "CreateSAMLProvider",
        vec![
            ("Name", "sp1"),
            ("SAMLMetadataDocument", "<EntityDescriptor>xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</EntityDescriptor>"),
        ],
    ))
    .unwrap();
    svc.tag_saml_provider(&make_request(
        "TagSAMLProvider",
        vec![
            (
                "SAMLProviderArn",
                "arn:aws:iam::123456789012:saml-provider/sp1",
            ),
            ("Tags.member.1.Key", "k"),
            ("Tags.member.1.Value", "v"),
        ],
    ))
    .unwrap();
    svc.list_saml_provider_tags(&make_request(
        "ListSAMLProviderTags",
        vec![(
            "SAMLProviderArn",
            "arn:aws:iam::123456789012:saml-provider/sp1",
        )],
    ))
    .unwrap();
    svc.untag_saml_provider(&make_request(
        "UntagSAMLProvider",
        vec![
            (
                "SAMLProviderArn",
                "arn:aws:iam::123456789012:saml-provider/sp1",
            ),
            ("TagKeys.member.1", "k"),
        ],
    ))
    .unwrap();
}

#[test]
fn policy_simulation_smoke() {
    let svc = make_service();
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
    svc.simulate_custom_policy(&make_request(
        "SimulateCustomPolicy",
        vec![
            ("PolicyInputList.member.1", policy),
            ("ActionNames.member.1", "s3:GetObject"),
            ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
        ],
    ))
    .unwrap();
    svc.get_context_keys_for_custom_policy(&make_request(
        "GetContextKeysForCustomPolicy",
        vec![("PolicyInputList.member.1", policy)],
    ))
    .unwrap();
}

#[test]
fn simulate_custom_policy_returns_allow_when_action_matches() {
    let svc = make_service();
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
    let resp = svc
        .simulate_custom_policy(&make_request(
            "SimulateCustomPolicy",
            vec![
                ("PolicyInputList.member.1", policy),
                ("ActionNames.member.1", "s3:GetObject"),
                ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EvalDecision>allowed</EvalDecision>"),
        "expected allowed, body: {body}"
    );
    assert!(body.contains("<EvalActionName>s3:GetObject</EvalActionName>"));
    assert!(body.contains("<EvalResourceName>arn:aws:s3:::b/k</EvalResourceName>"));
}

#[test]
fn simulate_custom_policy_returns_implicit_deny_when_action_unrelated() {
    let svc = make_service();
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
    let resp = svc
        .simulate_custom_policy(&make_request(
            "SimulateCustomPolicy",
            vec![
                ("PolicyInputList.member.1", policy),
                ("ActionNames.member.1", "ec2:RunInstances"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EvalDecision>implicitDeny</EvalDecision>"),
        "expected implicitDeny, body: {body}"
    );
}

#[test]
fn simulate_custom_policy_returns_explicit_deny_for_matching_deny_statement() {
    let svc = make_service();
    let policy = r#"{"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":"s3:*","Resource":"*"},
        {"Effect":"Deny","Action":"s3:DeleteObject","Resource":"*"}
    ]}"#;
    let resp = svc
        .simulate_custom_policy(&make_request(
            "SimulateCustomPolicy",
            vec![
                ("PolicyInputList.member.1", policy),
                ("ActionNames.member.1", "s3:DeleteObject"),
                ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EvalDecision>explicitDeny</EvalDecision>"),
        "expected explicitDeny, body: {body}"
    );
}

#[test]
fn simulate_custom_policy_boundary_caps_allow() {
    let svc = make_service();
    let identity =
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;
    let boundary = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
    let resp = svc
        .simulate_custom_policy(&make_request(
            "SimulateCustomPolicy",
            vec![
                ("PolicyInputList.member.1", identity),
                ("PermissionsBoundaryPolicyInputList.member.1", boundary),
                ("ActionNames.member.1", "ec2:RunInstances"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    // Boundary doesn't allow ec2:RunInstances, so result is implicitDeny.
    assert!(
        body.contains("<EvalDecision>implicitDeny</EvalDecision>"),
        "expected implicitDeny under boundary, body: {body}"
    );
}

#[test]
fn simulate_principal_policy_uses_principals_attached_managed_policy() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "alice")]))
        .unwrap();
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
    svc.create_policy(&make_request(
        "CreatePolicy",
        vec![("PolicyName", "P"), ("PolicyDocument", policy_doc)],
    ))
    .unwrap();
    let policy_arn = "arn:aws:iam::123456789012:policy/P";
    svc.attach_user_policy(&make_request(
        "AttachUserPolicy",
        vec![("UserName", "alice"), ("PolicyArn", policy_arn)],
    ))
    .unwrap();
    let user_arn = "arn:aws:iam::123456789012:user/alice";
    let resp = svc
        .simulate_principal_policy(&make_request(
            "SimulatePrincipalPolicy",
            vec![
                ("PolicySourceArn", user_arn),
                ("ActionNames.member.1", "s3:GetObject"),
                ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EvalDecision>allowed</EvalDecision>"),
        "expected allowed via attached policy, body: {body}"
    );
}

#[test]
fn simulate_principal_policy_unknown_principal_returns_implicit_deny() {
    let svc = make_service();
    let resp = svc
        .simulate_principal_policy(&make_request(
            "SimulatePrincipalPolicy",
            vec![
                ("PolicySourceArn", "arn:aws:iam::123456789012:user/ghost"),
                ("ActionNames.member.1", "s3:GetObject"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EvalDecision>implicitDeny</EvalDecision>"),
        "expected implicitDeny for unknown principal, body: {body}"
    );
}

#[test]
fn simulate_custom_policy_requires_action_names() {
    let svc = make_service();
    let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
    let err = match svc.simulate_custom_policy(&make_request(
        "SimulateCustomPolicy",
        vec![("PolicyInputList.member.1", policy)],
    )) {
        Err(e) => e,
        Ok(_) => panic!("expected InvalidInput"),
    };
    assert_eq!(err.code(), "InvalidInput");
}

#[test]
fn simulate_custom_policy_evaluates_condition_keys() {
    let svc = make_service();
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*","Condition":{"StringEquals":{"aws:RequestTag/foo":"bar"}}}]}"#;

    // Matching tag value -> allowed.
    let resp = svc
        .simulate_custom_policy(&make_request(
            "SimulateCustomPolicy",
            vec![
                ("PolicyInputList.member.1", policy),
                ("ActionNames.member.1", "s3:PutObject"),
                ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
                (
                    "ContextEntries.member.1.ContextKeyName",
                    "aws:RequestTag/foo",
                ),
                ("ContextEntries.member.1.ContextKeyValues.member.1", "bar"),
                ("ContextEntries.member.1.ContextKeyType", "string"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EvalDecision>allowed</EvalDecision>"),
        "expected allowed when condition matches, body: {body}"
    );

    // Different tag value -> implicit deny (Condition false, no Allow matches).
    let resp = svc
        .simulate_custom_policy(&make_request(
            "SimulateCustomPolicy",
            vec![
                ("PolicyInputList.member.1", policy),
                ("ActionNames.member.1", "s3:PutObject"),
                ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
                (
                    "ContextEntries.member.1.ContextKeyName",
                    "aws:RequestTag/foo",
                ),
                ("ContextEntries.member.1.ContextKeyValues.member.1", "baz"),
                ("ContextEntries.member.1.ContextKeyType", "string"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EvalDecision>implicitDeny</EvalDecision>"),
        "expected implicitDeny when condition fails, body: {body}"
    );
}

#[test]
fn simulate_principal_policy_blocked_by_permission_boundary() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "bob")]))
        .unwrap();

    // Identity policy grants s3:GetObject.
    let identity_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
    svc.create_policy(&make_request(
        "CreatePolicy",
        vec![
            ("PolicyName", "GrantS3Get"),
            ("PolicyDocument", identity_doc),
        ],
    ))
    .unwrap();
    let identity_arn = "arn:aws:iam::123456789012:policy/GrantS3Get";
    svc.attach_user_policy(&make_request(
        "AttachUserPolicy",
        vec![("UserName", "bob"), ("PolicyArn", identity_arn)],
    ))
    .unwrap();

    // Boundary only allows ec2 actions, so it does not allow s3:GetObject.
    let boundary_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ec2:Describe*","Resource":"*"}]}"#;
    svc.create_policy(&make_request(
        "CreatePolicy",
        vec![
            ("PolicyName", "BoundaryEc2Only"),
            ("PolicyDocument", boundary_doc),
        ],
    ))
    .unwrap();
    let boundary_arn = "arn:aws:iam::123456789012:policy/BoundaryEc2Only";
    svc.put_user_permissions_boundary(&make_request(
        "PutUserPermissionsBoundary",
        vec![("UserName", "bob"), ("PermissionsBoundary", boundary_arn)],
    ))
    .unwrap();

    let user_arn = "arn:aws:iam::123456789012:user/bob";
    let resp = svc
        .simulate_principal_policy(&make_request(
            "SimulatePrincipalPolicy",
            vec![
                ("PolicySourceArn", user_arn),
                ("ActionNames.member.1", "s3:GetObject"),
                ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EvalDecision>implicitDeny</EvalDecision>"),
        "expected implicitDeny when boundary blocks, body: {body}"
    );
}

#[test]
fn simulate_principal_policy_unions_group_policies() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "carol")]))
        .unwrap();
    svc.create_group(&make_request("CreateGroup", vec![("GroupName", "ops")]))
        .unwrap();
    svc.add_user_to_group(&make_request(
        "AddUserToGroup",
        vec![("UserName", "carol"), ("GroupName", "ops")],
    ))
    .unwrap();
    // Group inline policy grants s3:GetObject — the user has no
    // identity-side policies of their own.
    let inline_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
    svc.put_group_policy(&make_request(
        "PutGroupPolicy",
        vec![
            ("GroupName", "ops"),
            ("PolicyName", "InlineRead"),
            ("PolicyDocument", inline_doc),
        ],
    ))
    .unwrap();

    let resp = svc
        .simulate_principal_policy(&make_request(
            "SimulatePrincipalPolicy",
            vec![
                ("PolicySourceArn", "arn:aws:iam::123456789012:user/carol"),
                ("ActionNames.member.1", "s3:GetObject"),
                ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EvalDecision>allowed</EvalDecision>"),
        "expected allowed via group inline policy, body: {body}"
    );
}

#[test]
fn simulate_principal_policy_unions_attached_group_managed_policy() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "dave")]))
        .unwrap();
    svc.create_group(&make_request("CreateGroup", vec![("GroupName", "admins")]))
        .unwrap();
    svc.add_user_to_group(&make_request(
        "AddUserToGroup",
        vec![("UserName", "dave"), ("GroupName", "admins")],
    ))
    .unwrap();
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}"#;
    svc.create_policy(&make_request(
        "CreatePolicy",
        vec![("PolicyName", "GroupPut"), ("PolicyDocument", policy_doc)],
    ))
    .unwrap();
    svc.attach_group_policy(&make_request(
        "AttachGroupPolicy",
        vec![
            ("GroupName", "admins"),
            ("PolicyArn", "arn:aws:iam::123456789012:policy/GroupPut"),
        ],
    ))
    .unwrap();

    let resp = svc
        .simulate_principal_policy(&make_request(
            "SimulatePrincipalPolicy",
            vec![
                ("PolicySourceArn", "arn:aws:iam::123456789012:user/dave"),
                ("ActionNames.member.1", "s3:PutObject"),
                ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<EvalDecision>allowed</EvalDecision>"),
        "expected allowed via attached group managed policy, body: {body}"
    );
}

#[test]
fn simulate_principal_policy_reports_missing_context_from_resolved_policies() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "ed")]))
        .unwrap();
    // Attach a managed policy whose Condition references a key the
    // request never supplies. The simulator must walk the *resolved*
    // policy set, not just PolicyInputList, when reporting missing
    // context values.
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*","Condition":{"StringEquals":{"aws:RequestedRegion":"us-east-1"}}}]}"#;
    svc.create_policy(&make_request(
        "CreatePolicy",
        vec![("PolicyName", "RegionGate"), ("PolicyDocument", policy_doc)],
    ))
    .unwrap();
    svc.attach_user_policy(&make_request(
        "AttachUserPolicy",
        vec![
            ("UserName", "ed"),
            ("PolicyArn", "arn:aws:iam::123456789012:policy/RegionGate"),
        ],
    ))
    .unwrap();

    let resp = svc
        .simulate_principal_policy(&make_request(
            "SimulatePrincipalPolicy",
            vec![
                ("PolicySourceArn", "arn:aws:iam::123456789012:user/ed"),
                ("ActionNames.member.1", "s3:PutObject"),
                ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains(
            "<MissingContextValues><member>aws:RequestedRegion</member></MissingContextValues>"
        ),
        "expected resolved-policy condition key reported, body: {body}"
    );
}

#[test]
fn simulate_custom_policy_reports_missing_context_values() {
    let svc = make_service();
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":{"aws:RequestTag/team":"red"}}}]}"#;
    let resp = svc
        .simulate_custom_policy(&make_request(
            "SimulateCustomPolicy",
            vec![
                ("PolicyInputList.member.1", policy),
                ("ActionNames.member.1", "s3:GetObject"),
                ("ResourceArns.member.1", "arn:aws:s3:::b/k"),
            ],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    // Caller didn't pass aws:RequestTag/team — should appear under
    // MissingContextValues, and the eval should implicitly deny.
    assert!(
        body.contains(
            "<MissingContextValues><member>aws:RequestTag/team</member></MissingContextValues>"
        ),
        "expected missing context key reported, body: {body}"
    );
    assert!(body.contains("<EvalDecision>implicitDeny</EvalDecision>"));
}

#[test]
fn misc_extras_smoke() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "u1")]))
        .unwrap();
    svc.create_login_profile(&make_request(
        "CreateLoginProfile",
        vec![("UserName", "u1"), ("Password", "p")],
    ))
    .unwrap();
    let mut req = make_request(
        "ChangePassword",
        vec![("OldPassword", "p"), ("NewPassword", "q")],
    );
    req.principal = Some(fakecloud_core::auth::Principal {
        arn: "arn:aws:iam::123456789012:user/u1".to_string(),
        user_id: "AIDAEXAMPLE".to_string(),
        account_id: "123456789012".to_string(),
        principal_type: fakecloud_core::auth::PrincipalType::User,
        source_identity: None,
        tags: None,
    });
    svc.change_password(&req).unwrap();
    svc.set_security_token_service_preferences(&make_request(
        "SetSecurityTokenServicePreferences",
        vec![("GlobalEndpointTokenVersion", "v2Token")],
    ))
    .unwrap();
}

fn change_password_principal(user_name: &str) -> fakecloud_core::auth::Principal {
    fakecloud_core::auth::Principal {
        arn: format!("arn:aws:iam::123456789012:user/{user_name}"),
        user_id: "AIDAEXAMPLE".to_string(),
        account_id: "123456789012".to_string(),
        principal_type: fakecloud_core::auth::PrincipalType::User,
        source_identity: None,
        tags: None,
    }
}

#[test]
fn change_password_rejects_wrong_old_password() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "u1")]))
        .unwrap();
    svc.create_login_profile(&make_request(
        "CreateLoginProfile",
        vec![("UserName", "u1"), ("Password", "correct-old")],
    ))
    .unwrap();
    let mut req = make_request(
        "ChangePassword",
        vec![("OldPassword", "wrong-old"), ("NewPassword", "fresh-new")],
    );
    req.principal = Some(change_password_principal("u1"));
    let err = match svc.change_password(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected mismatch to fail"),
    };
    assert_eq!(err.status(), http::StatusCode::FORBIDDEN);
    assert!(format!("{:?}", err).contains("InvalidUserType"));
}

#[test]
fn change_password_rejects_user_without_login_profile() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "noprofile")]))
        .unwrap();
    let mut req = make_request(
        "ChangePassword",
        vec![("OldPassword", "a"), ("NewPassword", "b")],
    );
    req.principal = Some(change_password_principal("noprofile"));
    let err = match svc.change_password(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected user-without-profile to fail"),
    };
    assert!(format!("{:?}", err).contains("InvalidUserType"));
}

#[test]
fn change_password_rejects_deleted_user() {
    let svc = make_service();
    let mut req = make_request(
        "ChangePassword",
        vec![("OldPassword", "a"), ("NewPassword", "b")],
    );
    req.principal = Some(change_password_principal("ghost"));
    let err = match svc.change_password(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected deleted-user to fail"),
    };
    assert!(format!("{:?}", err).contains("InvalidUserType"));
}

#[test]
fn change_password_writes_new_password() {
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "u1")]))
        .unwrap();
    svc.create_login_profile(&make_request(
        "CreateLoginProfile",
        vec![("UserName", "u1"), ("Password", "old")],
    ))
    .unwrap();
    let mut first = make_request(
        "ChangePassword",
        vec![("OldPassword", "old"), ("NewPassword", "fresh")],
    );
    first.principal = Some(change_password_principal("u1"));
    svc.change_password(&first).unwrap();

    // Subsequent ChangePassword with the previous password fails —
    // the rotation actually wrote the new value to state.
    let mut second = make_request(
        "ChangePassword",
        vec![("OldPassword", "old"), ("NewPassword", "another")],
    );
    second.principal = Some(change_password_principal("u1"));
    assert!(svc.change_password(&second).is_err());

    // Same call with the new password succeeds.
    let mut third = make_request(
        "ChangePassword",
        vec![("OldPassword", "fresh"), ("NewPassword", "another")],
    );
    third.principal = Some(change_password_principal("u1"));
    svc.change_password(&third).unwrap();
}

#[test]
fn set_sts_prefs_rejects_invalid_version() {
    let svc = make_service();
    let err = match svc.set_security_token_service_preferences(&make_request(
        "SetSecurityTokenServicePreferences",
        vec![("GlobalEndpointTokenVersion", "v9Token")],
    )) {
        Err(e) => e,
        Ok(_) => panic!("expected invalid version to be rejected"),
    };
    assert!(format!("{:?}", err).contains("InvalidParameterValue"));
}

#[test]
fn get_sts_prefs_returns_stored_version() {
    let svc = make_service();
    svc.set_security_token_service_preferences(&make_request(
        "SetSecurityTokenServicePreferences",
        vec![("GlobalEndpointTokenVersion", "v2Token")],
    ))
    .unwrap();
    let resp = svc
        .get_security_token_service_preferences(&make_request(
            "GetSecurityTokenServicePreferences",
            vec![],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<GlobalEndpointTokenVersion>v2Token</GlobalEndpointTokenVersion>"),
        "expected stored version in response, got: {body}"
    );
}

#[test]
fn get_sts_prefs_defaults_to_v1token_when_unset() {
    let svc = make_service();
    let resp = svc
        .get_security_token_service_preferences(&make_request(
            "GetSecurityTokenServicePreferences",
            vec![],
        ))
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<GlobalEndpointTokenVersion>v1Token</GlobalEndpointTokenVersion>"),
        "expected default v1Token, got: {body}"
    );
}

#[test]
fn resync_mfa_device_updates_enable_date() {
    use chrono::Utc;
    let svc = make_service();
    svc.create_user(&make_request("CreateUser", vec![("UserName", "u1")]))
        .unwrap();
    let serial = "arn:aws:iam::123456789012:mfa/u1";
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("123456789012");
        let stale = Utc::now() - chrono::Duration::days(30);
        state.virtual_mfa_devices.insert(
            serial.to_string(),
            crate::state::VirtualMfaDevice {
                serial_number: serial.to_string(),
                user: Some("u1".to_string()),
                enable_date: Some(stale),
                base32_string_seed: String::new(),
                qr_code_png: String::new(),
                tags: Vec::new(),
            },
        );
    }

    svc.resync_mfa_device(&make_request(
        "ResyncMFADevice",
        vec![
            ("UserName", "u1"),
            ("SerialNumber", serial),
            ("AuthenticationCode1", "111111"),
            ("AuthenticationCode2", "222222"),
        ],
    ))
    .unwrap();

    let accounts = svc.state.read();
    let state = accounts.get("123456789012").unwrap();
    let dev = state.virtual_mfa_devices.get(serial).unwrap();
    let enabled = dev.enable_date.expect("enable_date should be set");
    let age = (Utc::now() - enabled).num_seconds();
    assert!(
        (0..60).contains(&age),
        "ResyncMFADevice should freshen EnableDate, got age {age}s"
    );
}

// ---- Smithy-declared wire error code tests ----
//
// Strict-mode conformance probes require every error returned by an op to be
// one of the Smithy-declared shapes. These regression tests pin the wire codes
// for input-validation failures on ops where we previously leaked the framework
// codes `MissingParameter`, `ValidationError`, or `ValidationException`.

fn assert_error_code(err: AwsServiceError, expected: &str) {
    match err {
        AwsServiceError::AwsError { code, .. } => {
            assert_eq!(code, expected, "wrong wire error code");
        }
        other => panic!("expected AwsError, got {other:?}"),
    }
}

#[test]
fn create_role_short_permissions_boundary_emits_invalid_input() {
    let svc = make_service();
    let err = svc
        .create_role(&make_request(
            "CreateRole",
            vec![
                ("RoleName", "r"),
                ("AssumeRolePolicyDocument", "{}"),
                ("PermissionsBoundary", "short"),
            ],
        ))
        .err()
        .unwrap();
    assert_error_code(err, "InvalidInput");
}

#[test]
fn put_role_permissions_boundary_short_emits_invalid_input() {
    let svc = make_service();
    let err = svc
        .put_role_permissions_boundary(&make_request(
            "PutRolePermissionsBoundary",
            vec![("RoleName", "r"), ("PermissionsBoundary", "short")],
        ))
        .err()
        .unwrap();
    assert_error_code(err, "InvalidInput");
}

#[test]
fn put_user_permissions_boundary_short_emits_invalid_input() {
    let svc = make_service();
    let err = svc
        .put_user_permissions_boundary(&make_request(
            "PutUserPermissionsBoundary",
            vec![("UserName", "u"), ("PermissionsBoundary", "short")],
        ))
        .err()
        .unwrap();
    assert_error_code(err, "InvalidInput");
}

#[test]
fn set_default_policy_version_short_arn_emits_invalid_input() {
    let svc = make_service();
    let err = svc
        .set_default_policy_version(&make_request(
            "SetDefaultPolicyVersion",
            vec![("PolicyArn", "short"), ("VersionId", "v1")],
        ))
        .err()
        .unwrap();
    assert_error_code(err, "InvalidInput");
}

#[test]
fn create_login_profile_missing_password_emits_password_policy_violation() {
    let svc = make_service();
    let err = svc
        .create_login_profile(&make_request("CreateLoginProfile", vec![("UserName", "u")]))
        .err()
        .unwrap();
    assert_error_code(err, "PasswordPolicyViolation");
}

#[test]
fn create_access_key_unresolved_user_emits_no_such_entity() {
    let svc = make_service();
    let err = svc
        .create_access_key(&make_request("CreateAccessKey", vec![]))
        .err()
        .unwrap();
    assert_error_code(err, "NoSuchEntity");
}

#[test]
fn delete_user_oversize_name_emits_no_such_entity() {
    let svc = make_service();
    let long = "a".repeat(129);
    let err = svc
        .delete_user(&make_request("DeleteUser", vec![("UserName", &long)]))
        .err()
        .unwrap();
    assert_error_code(err, "NoSuchEntity");
}

#[test]
fn tag_user_oversize_name_emits_invalid_input() {
    let svc = make_service();
    let long = "a".repeat(129);
    let err = svc
        .tag_user(&make_request("TagUser", vec![("UserName", &long)]))
        .err()
        .unwrap();
    assert_error_code(err, "InvalidInput");
}

#[test]
fn list_mfa_devices_no_username_does_not_emit_missing_parameter() {
    // ListMFADevices declares NoSuchEntity + ServiceFailure only. Sending no
    // UserName must not leak `MissingParameter` from the shared helper.
    let svc = make_service();
    let resp = svc.list_mfa_devices(&make_request("ListMFADevices", vec![]));
    match resp {
        Ok(_) => {}
        Err(AwsServiceError::AwsError { code, .. }) => {
            assert_ne!(
                code, "MissingParameter",
                "ListMFADevices leaked MissingParameter"
            );
        }
        Err(other) => panic!("unexpected error variant: {other:?}"),
    }
}

#[test]
fn create_open_id_connect_provider_missing_url_emits_invalid_input() {
    let svc = make_service();
    let err = svc
        .create_oidc_provider(&make_request("CreateOpenIDConnectProvider", vec![]))
        .err()
        .unwrap();
    assert_error_code(err, "InvalidInput");
}

#[test]
fn set_security_token_service_preferences_is_iam_enforceable() {
    // The real AWS action `SetSecurityTokenServicePreferences` is mapped and
    // IAM-enforced. Its read-back sibling `GetSecurityTokenServicePreferences`
    // is a fakecloud-only extension (absent from AWS's iam.json), so it is
    // intentionally NOT in SUPPORTED_ACTIONS — there is no AWS IAM action to
    // write a policy against, and listing it would fail the handwritten-test
    // audit (no Smithy model to checksum).
    let svc = make_service();
    let set_req = make_request("SetSecurityTokenServicePreferences", vec![]);
    assert_eq!(
        svc.iam_action_for(&set_req).map(|a| a.action),
        Some("SetSecurityTokenServicePreferences")
    );

    let get_req = make_request("GetSecurityTokenServicePreferences", vec![]);
    assert!(
        svc.iam_action_for(&get_req).is_none(),
        "Get... is a fakecloud extension, not a mapped AWS IAM action"
    );
}

#[test]
fn create_virtual_mfa_device_bad_path_emits_invalid_input() {
    let svc = make_service();
    let err = svc
        .create_virtual_mfa_device(&make_request(
            "CreateVirtualMFADevice",
            vec![("VirtualMFADeviceName", "d"), ("Path", "x")],
        ))
        .err()
        .unwrap();
    assert_error_code(err, "InvalidInput");
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = make_service();
    assert!(svc.snapshot_hook().is_none());
}

#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = make_service().with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    hook().await;
}
