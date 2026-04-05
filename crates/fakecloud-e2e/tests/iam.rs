mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn sts_get_caller_identity() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;

    let resp = client.get_caller_identity().send().await.unwrap();
    assert_eq!(resp.account().unwrap(), "123456789012");
    assert!(resp.arn().unwrap().contains(":root"));
}

#[tokio::test]
async fn iam_create_get_delete_user() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    // Create
    let resp = client
        .create_user()
        .user_name("testuser")
        .send()
        .await
        .unwrap();
    let user = resp.user().unwrap();
    assert_eq!(user.user_name(), "testuser");
    assert!(user.arn().contains("testuser"));
    assert!(user.user_id().starts_with("AIDA"));

    // Get
    let resp = client
        .get_user()
        .user_name("testuser")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.user().unwrap().user_name(), "testuser");

    // Delete
    client
        .delete_user()
        .user_name("testuser")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client.get_user().user_name("testuser").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn iam_list_users() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("alice")
        .send()
        .await
        .unwrap();
    client.create_user().user_name("bob").send().await.unwrap();

    let resp = client.list_users().send().await.unwrap();
    let users = resp.users();
    assert_eq!(users.len(), 2);

    let names: Vec<&str> = users.iter().map(|u| u.user_name()).collect();
    assert!(names.contains(&"alice"));
    assert!(names.contains(&"bob"));
}

#[tokio::test]
async fn iam_create_user_duplicate_fails() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client.create_user().user_name("dupe").send().await.unwrap();
    let result = client.create_user().user_name("dupe").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn iam_access_keys() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("keyuser")
        .send()
        .await
        .unwrap();

    // Create access key
    let resp = client
        .create_access_key()
        .user_name("keyuser")
        .send()
        .await
        .unwrap();
    let key = resp.access_key().unwrap();
    assert!(key.access_key_id().starts_with("FKIA"));
    assert_eq!(key.user_name(), "keyuser");
    let key_id = key.access_key_id().to_string();

    // List access keys
    let resp = client
        .list_access_keys()
        .user_name("keyuser")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.access_key_metadata().len(), 1);

    // Delete access key
    client
        .delete_access_key()
        .user_name("keyuser")
        .access_key_id(&key_id)
        .send()
        .await
        .unwrap();

    let resp = client
        .list_access_keys()
        .user_name("keyuser")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.access_key_metadata().len(), 0);
}

#[tokio::test]
async fn iam_roles() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let trust_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;

    // Create
    let resp = client
        .create_role()
        .role_name("test-role")
        .assume_role_policy_document(trust_policy)
        .send()
        .await
        .unwrap();
    let role = resp.role().unwrap();
    assert_eq!(role.role_name(), "test-role");
    assert!(role.role_id().starts_with("AROA"));

    // Get
    let resp = client
        .get_role()
        .role_name("test-role")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.role().unwrap().role_name(), "test-role");

    // List
    let resp = client.list_roles().send().await.unwrap();
    assert_eq!(resp.roles().len(), 1);

    // Delete
    client
        .delete_role()
        .role_name("test-role")
        .send()
        .await
        .unwrap();

    let resp = client.list_roles().send().await.unwrap();
    assert_eq!(resp.roles().len(), 0);
}

#[tokio::test]
async fn sts_assume_role_unique_credentials() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;

    let resp1 = client
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/role-a")
        .role_session_name("session-a")
        .send()
        .await
        .unwrap();
    let creds1 = resp1.credentials().unwrap();

    let resp2 = client
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/role-b")
        .role_session_name("session-b")
        .send()
        .await
        .unwrap();
    let creds2 = resp2.credentials().unwrap();

    // Access key IDs should be different
    assert_ne!(creds1.access_key_id(), creds2.access_key_id());
    // Secret access keys should be different
    assert_ne!(creds1.secret_access_key(), creds2.secret_access_key());
    // Session tokens should be different
    assert_ne!(creds1.session_token(), creds2.session_token());

    // Temporary STS credentials start with FSIA
    assert!(creds1.access_key_id().starts_with("FSIA"));
    assert!(creds2.access_key_id().starts_with("FSIA"));

    // Session token should be realistic length (>100 chars)
    assert!(creds1.session_token().len() > 100);
}

#[tokio::test]
async fn sts_get_session_token() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;

    let resp = client.get_session_token().send().await.unwrap();
    let creds = resp.credentials().unwrap();
    assert!(creds.access_key_id().starts_with("FSIA"));
    assert!(!creds.secret_access_key().is_empty());
    assert!(creds.session_token().starts_with("AQoEXAMPLEH4"));
}

#[tokio::test]
async fn sts_get_federation_token() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;

    let resp = client
        .get_federation_token()
        .name("Bob")
        .send()
        .await
        .unwrap();
    let creds = resp.credentials().unwrap();
    assert!(creds.access_key_id().starts_with("FSIA"));
    let fed_user = resp.federated_user().unwrap();
    assert!(fed_user.arn().contains("federated-user/Bob"));
    assert!(fed_user.federated_user_id().contains("Bob"));
}

#[tokio::test]
async fn sts_get_access_key_info() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;

    let resp = client
        .get_access_key_info()
        .access_key_id("AKIAIOSFODNN7EXAMPLE")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.account().unwrap(), "123456789012");
}

#[tokio::test]
async fn sts_assume_role_with_web_identity() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;

    let resp = client
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .role_session_name("test-session")
        .web_identity_token("fake-token")
        .send()
        .await
        .unwrap();
    let creds = resp.credentials().unwrap();
    assert!(creds.access_key_id().starts_with("FSIA"));
    let user = resp.assumed_role_user().unwrap();
    assert!(user.arn().contains("assumed-role/test-role/test-session"));
}

#[tokio::test]
async fn sts_assume_role_returns_correct_arn() {
    let server = TestServer::start().await;
    let sts = server.sts_client().await;
    let iam = server.iam_client().await;

    // Create a role first
    let trust_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"sts:AssumeRole"}]}"#;
    let role = iam
        .create_role()
        .role_name("my-role")
        .assume_role_policy_document(trust_policy)
        .send()
        .await
        .unwrap();
    let role_arn = role.role().unwrap().arn();
    let role_id = role.role().unwrap().role_id();

    // Assume the role
    let resp = sts
        .assume_role()
        .role_arn(role_arn)
        .role_session_name("my-session")
        .send()
        .await
        .unwrap();
    let assumed = resp.assumed_role_user().unwrap();
    assert!(
        assumed.arn().contains("assumed-role/my-role/my-session"),
        "ARN should contain assumed-role: {}",
        assumed.arn()
    );
    // AssumedRoleId should be roleId:sessionName
    assert!(
        assumed.assumed_role_id().starts_with(role_id),
        "AssumedRoleId should start with role ID: {}",
        assumed.assumed_role_id()
    );
    assert!(
        assumed.assumed_role_id().ends_with(":my-session"),
        "AssumedRoleId should end with session name: {}",
        assumed.assumed_role_id()
    );
}

#[tokio::test]
async fn sts_get_caller_identity_cli() {
    let server = TestServer::start().await;
    let output = server.aws_cli(&["sts", "get-caller-identity"]).await;
    assert!(
        output.success(),
        "CLI should succeed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(json["Account"], "123456789012");
}

// ---- IAM Group Tests ----

#[tokio::test]
async fn iam_group_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    // Create group
    let resp = client
        .create_group()
        .group_name("developers")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.group().unwrap().group_name(), "developers");

    // List groups
    let list = client.list_groups().send().await.unwrap();
    assert_eq!(list.groups().len(), 1);

    // Create user and add to group
    client
        .create_user()
        .user_name("dev-alice")
        .send()
        .await
        .unwrap();

    client
        .add_user_to_group()
        .group_name("developers")
        .user_name("dev-alice")
        .send()
        .await
        .unwrap();

    // Get group (lists members)
    let group = client
        .get_group()
        .group_name("developers")
        .send()
        .await
        .unwrap();
    assert_eq!(group.users().len(), 1);
    assert_eq!(group.users()[0].user_name(), "dev-alice");

    // List groups for user
    let user_groups = client
        .list_groups_for_user()
        .user_name("dev-alice")
        .send()
        .await
        .unwrap();
    assert_eq!(user_groups.groups().len(), 1);

    // Remove user from group
    client
        .remove_user_from_group()
        .group_name("developers")
        .user_name("dev-alice")
        .send()
        .await
        .unwrap();

    // Delete group
    client
        .delete_group()
        .group_name("developers")
        .send()
        .await
        .unwrap();

    let list = client.list_groups().send().await.unwrap();
    assert!(list.groups().is_empty());
}

// ---- IAM Instance Profile Tests ----

#[tokio::test]
async fn iam_instance_profile_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    // Create instance profile
    let resp = client
        .create_instance_profile()
        .instance_profile_name("web-profile")
        .send()
        .await
        .unwrap();
    let profile = resp.instance_profile().unwrap();
    assert_eq!(profile.instance_profile_name(), "web-profile");

    // Create a role to attach
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    client
        .create_role()
        .role_name("web-role")
        .assume_role_policy_document(trust)
        .send()
        .await
        .unwrap();

    // Add role to instance profile
    client
        .add_role_to_instance_profile()
        .instance_profile_name("web-profile")
        .role_name("web-role")
        .send()
        .await
        .unwrap();

    // Get instance profile and verify role
    let get = client
        .get_instance_profile()
        .instance_profile_name("web-profile")
        .send()
        .await
        .unwrap();
    assert_eq!(get.instance_profile().unwrap().roles().len(), 1);

    // List instance profiles
    let list = client.list_instance_profiles().send().await.unwrap();
    assert_eq!(list.instance_profiles().len(), 1);

    // List instance profiles for role
    let for_role = client
        .list_instance_profiles_for_role()
        .role_name("web-role")
        .send()
        .await
        .unwrap();
    assert_eq!(for_role.instance_profiles().len(), 1);

    // Remove role from instance profile
    client
        .remove_role_from_instance_profile()
        .instance_profile_name("web-profile")
        .role_name("web-role")
        .send()
        .await
        .unwrap();

    // Delete
    client
        .delete_instance_profile()
        .instance_profile_name("web-profile")
        .send()
        .await
        .unwrap();
}

// ---- IAM Policy Tests ----

#[tokio::test]
async fn iam_policy_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;

    // Create policy
    let resp = client
        .create_policy()
        .policy_name("s3-read")
        .policy_document(policy_doc)
        .send()
        .await
        .unwrap();
    let policy_arn = resp.policy().unwrap().arn().unwrap().to_string();

    // Get policy
    let get = client
        .get_policy()
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get.policy().unwrap().policy_name().unwrap(), "s3-read");

    // List policies
    let list = client.list_policies().send().await.unwrap();
    assert!(list
        .policies()
        .iter()
        .any(|p| p.policy_name().unwrap() == "s3-read"));

    // Create a policy version
    let new_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"*"}]}"#;
    let ver = client
        .create_policy_version()
        .policy_arn(&policy_arn)
        .policy_document(new_doc)
        .set_as_default(true)
        .send()
        .await
        .unwrap();
    assert_eq!(ver.policy_version().unwrap().version_id().unwrap(), "v2");

    // List policy versions
    let versions = client
        .list_policy_versions()
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(versions.versions().len(), 2);

    // Delete non-default version first
    client
        .delete_policy_version()
        .policy_arn(&policy_arn)
        .version_id("v1")
        .send()
        .await
        .unwrap();

    // Delete policy
    client
        .delete_policy()
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();
}

// ---- IAM Role Policy Attachment Tests ----

#[tokio::test]
async fn iam_attach_detach_role_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    client
        .create_role()
        .role_name("attach-role")
        .assume_role_policy_document(trust)
        .send()
        .await
        .unwrap();

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"logs:*","Resource":"*"}]}"#;
    let policy = client
        .create_policy()
        .policy_name("logs-full")
        .policy_document(policy_doc)
        .send()
        .await
        .unwrap();
    let policy_arn = policy.policy().unwrap().arn().unwrap().to_string();

    // Attach
    client
        .attach_role_policy()
        .role_name("attach-role")
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();

    // List attached
    let attached = client
        .list_attached_role_policies()
        .role_name("attach-role")
        .send()
        .await
        .unwrap();
    assert_eq!(attached.attached_policies().len(), 1);

    // Detach
    client
        .detach_role_policy()
        .role_name("attach-role")
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();

    let attached = client
        .list_attached_role_policies()
        .role_name("attach-role")
        .send()
        .await
        .unwrap();
    assert!(attached.attached_policies().is_empty());
}

// ---- IAM Inline Policy Tests ----

#[tokio::test]
async fn iam_role_inline_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    client
        .create_role()
        .role_name("inline-role")
        .assume_role_policy_document(trust)
        .send()
        .await
        .unwrap();

    let inline_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*","Resource":"*"}]}"#;
    client
        .put_role_policy()
        .role_name("inline-role")
        .policy_name("sqs-access")
        .policy_document(inline_doc)
        .send()
        .await
        .unwrap();

    // List inline policies
    let policies = client
        .list_role_policies()
        .role_name("inline-role")
        .send()
        .await
        .unwrap();
    assert_eq!(policies.policy_names().len(), 1);
    assert_eq!(policies.policy_names()[0], "sqs-access");

    // Get inline policy
    let get = client
        .get_role_policy()
        .role_name("inline-role")
        .policy_name("sqs-access")
        .send()
        .await
        .unwrap();
    assert_eq!(get.policy_name(), "sqs-access");

    // Delete inline policy
    client
        .delete_role_policy()
        .role_name("inline-role")
        .policy_name("sqs-access")
        .send()
        .await
        .unwrap();

    let policies = client
        .list_role_policies()
        .role_name("inline-role")
        .send()
        .await
        .unwrap();
    assert!(policies.policy_names().is_empty());
}

// ---- IAM OIDC Provider Tests ----

#[tokio::test]
async fn iam_oidc_provider_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let resp = client
        .create_open_id_connect_provider()
        .url("https://accounts.google.com")
        .thumbprint_list("1234567890abcdef1234567890abcdef12345678")
        .client_id_list("my-app-id")
        .send()
        .await
        .unwrap();
    let arn = resp.open_id_connect_provider_arn().unwrap().to_string();

    // List
    let list = client
        .list_open_id_connect_providers()
        .send()
        .await
        .unwrap();
    assert_eq!(list.open_id_connect_provider_list().len(), 1);

    // Get
    let get = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get.client_id_list().len(), 1);

    // Delete
    client
        .delete_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
}

// ---- IAM SAML Provider Tests ----

#[tokio::test]
async fn iam_saml_provider_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let saml_metadata = "<EntityDescriptor>fake-saml</EntityDescriptor>";

    let resp = client
        .create_saml_provider()
        .name("my-idp")
        .saml_metadata_document(saml_metadata)
        .send()
        .await
        .unwrap();
    let arn = resp.saml_provider_arn().unwrap().to_string();

    // List
    let list = client.list_saml_providers().send().await.unwrap();
    assert_eq!(list.saml_provider_list().len(), 1);

    // Get
    let get = client
        .get_saml_provider()
        .saml_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(get.saml_metadata_document().is_some());

    // Delete
    client
        .delete_saml_provider()
        .saml_provider_arn(&arn)
        .send()
        .await
        .unwrap();
}

// ---- IAM Error Cases ----

#[tokio::test]
async fn iam_get_nonexistent_user_fails() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let result = client.get_user().user_name("nobody").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn iam_delete_nonexistent_role_fails() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let result = client.delete_role().role_name("ghost-role").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn iam_tag_user() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("tagged-user")
        .send()
        .await
        .unwrap();

    use aws_sdk_iam::types::Tag;
    client
        .tag_user()
        .user_name("tagged-user")
        .tags(Tag::builder().key("dept").value("eng").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_user_tags()
        .user_name("tagged-user")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), "dept");

    client
        .untag_user()
        .user_name("tagged-user")
        .tag_keys("dept")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_user_tags()
        .user_name("tagged-user")
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

/// Regression: ListVirtualMFADevices should only return virtual MFA devices,
/// excluding hardware MFA devices created via EnableMFADevice with a non-virtual serial.
#[tokio::test]
async fn iam_list_virtual_mfa_excludes_hardware() {
    let server = TestServer::start().await;

    // Create a virtual MFA device via CLI (SDK doesn't have this API directly)
    let tmp_dir = std::env::temp_dir();
    let outfile = tmp_dir.join("mfa-bootstrap.png");
    let output = server
        .aws_cli(&[
            "iam",
            "create-virtual-mfa-device",
            "--virtual-mfa-device-name",
            "my-virtual-mfa",
            "--outfile",
            outfile.to_str().unwrap(),
            "--bootstrap-method",
            "QRCodePNG",
        ])
        .await;
    let _ = std::fs::remove_file(&outfile);
    assert!(
        output.success(),
        "create-virtual-mfa-device failed: {}",
        output.stderr_text()
    );

    // Create a user and enable a hardware MFA device on that user
    let iam = server.iam_client().await;
    iam.create_user()
        .user_name("mfa-user")
        .send()
        .await
        .unwrap();

    // Enable a "hardware" MFA by providing a non-virtual serial number
    let output = server
        .aws_cli(&[
            "iam",
            "enable-mfa-device",
            "--user-name",
            "mfa-user",
            "--serial-number",
            "arn:aws:iam::123456789012:mfa/hardware-token",
            "--authentication-code1",
            "123456",
            "--authentication-code2",
            "654321",
        ])
        .await;
    assert!(
        output.success(),
        "enable-mfa-device failed: {}",
        output.stderr_text()
    );

    // List virtual MFA devices - should only include the virtual one
    let output = server.aws_cli(&["iam", "list-virtual-mfa-devices"]).await;
    assert!(
        output.success(),
        "list-virtual-mfa-devices failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let devices = json["VirtualMFADevices"].as_array().unwrap();

    // Should contain only the virtual MFA device, not the hardware one
    assert_eq!(
        devices.len(),
        1,
        "expected 1 virtual MFA device, got {}",
        devices.len()
    );
    let serial = devices[0]["SerialNumber"].as_str().unwrap();
    assert!(
        serial.contains("my-virtual-mfa"),
        "expected virtual MFA serial containing 'my-virtual-mfa', got: {serial}"
    );
}
