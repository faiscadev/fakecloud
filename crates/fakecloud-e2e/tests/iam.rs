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

#[tokio::test]
async fn sts_decode_authorization_message() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;

    let resp = client
        .decode_authorization_message()
        .encoded_message("some-encoded-authorization-message")
        .send()
        .await
        .unwrap();
    let decoded = resp.decoded_message().unwrap();
    assert!(
        decoded.contains("allowed"),
        "decoded message should contain 'allowed', got: {decoded}"
    );
    assert!(
        decoded.contains("matchedStatements"),
        "decoded message should contain 'matchedStatements', got: {decoded}"
    );
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

// ---- Input Validation Tests ----

#[tokio::test]
async fn iam_list_policies_validates_max_items() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;
    // MaxItems=0 should fail validation (range is 1-1000)
    let result = client.list_policies().max_items(0).send().await;
    let err = result.expect_err("MaxItems=0 should fail validation");
    let msg = format!("{:?}", err);
    assert!(
        msg.contains("ValidationException") || msg.contains("validation"),
        "Expected validation error, got: {}",
        msg
    );
}

#[tokio::test]
async fn iam_list_users_validates_path_prefix() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;
    // Empty PathPrefix should fail validation (min length 1)
    let result = client.list_users().path_prefix("").send().await;
    let err = result.expect_err("Empty PathPrefix should fail validation");
    let msg = format!("{:?}", err);
    assert!(
        msg.contains("ValidationException") || msg.contains("validation"),
        "Expected validation error, got: {}",
        msg
    );
}

#[tokio::test]
async fn sts_assume_role_validates_external_id() {
    let server = TestServer::start().await;
    let output = server
        .aws_cli(&[
            "sts",
            "assume-role",
            "--role-arn",
            "arn:aws:iam::123456789012:role/test-role",
            "--role-session-name",
            "test-session",
            "--external-id",
            "x",
        ])
        .await;
    assert!(
        !output.success(),
        "1-char ExternalId should fail validation but got success"
    );
}

// ---- Group Managed Policy Tests ----

#[tokio::test]
async fn iam_attach_detach_group_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    // Create group
    client
        .create_group()
        .group_name("policy-group")
        .send()
        .await
        .unwrap();

    // Create managed policy
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let policy = client
        .create_policy()
        .policy_name("s3-full")
        .policy_document(policy_doc)
        .send()
        .await
        .unwrap();
    let policy_arn = policy.policy().unwrap().arn().unwrap().to_string();

    // Attach
    client
        .attach_group_policy()
        .group_name("policy-group")
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();

    // List attached
    let attached = client
        .list_attached_group_policies()
        .group_name("policy-group")
        .send()
        .await
        .unwrap();
    assert_eq!(attached.attached_policies().len(), 1);
    assert_eq!(
        attached.attached_policies()[0].policy_name().unwrap(),
        "s3-full"
    );

    // Detach
    client
        .detach_group_policy()
        .group_name("policy-group")
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();

    let attached = client
        .list_attached_group_policies()
        .group_name("policy-group")
        .send()
        .await
        .unwrap();
    assert!(attached.attached_policies().is_empty());
}

// ---- Group Inline Policy Tests ----

#[tokio::test]
async fn iam_group_inline_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_group()
        .group_name("inline-group")
        .send()
        .await
        .unwrap();

    let inline_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*","Resource":"*"}]}"#;
    client
        .put_group_policy()
        .group_name("inline-group")
        .policy_name("sqs-access")
        .policy_document(inline_doc)
        .send()
        .await
        .unwrap();

    // List inline policies
    let policies = client
        .list_group_policies()
        .group_name("inline-group")
        .send()
        .await
        .unwrap();
    assert_eq!(policies.policy_names().len(), 1);
    assert_eq!(policies.policy_names()[0], "sqs-access");

    // Get inline policy
    let get = client
        .get_group_policy()
        .group_name("inline-group")
        .policy_name("sqs-access")
        .send()
        .await
        .unwrap();
    assert_eq!(get.policy_name(), "sqs-access");
    assert_eq!(get.group_name(), "inline-group");

    // Delete inline policy
    client
        .delete_group_policy()
        .group_name("inline-group")
        .policy_name("sqs-access")
        .send()
        .await
        .unwrap();

    let policies = client
        .list_group_policies()
        .group_name("inline-group")
        .send()
        .await
        .unwrap();
    assert!(policies.policy_names().is_empty());
}

// ---- User Managed Policy Attachment Tests ----

#[tokio::test]
async fn iam_attach_detach_user_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("policy-user")
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
        .attach_user_policy()
        .user_name("policy-user")
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();

    // List attached
    let attached = client
        .list_attached_user_policies()
        .user_name("policy-user")
        .send()
        .await
        .unwrap();
    assert_eq!(attached.attached_policies().len(), 1);
    assert_eq!(
        attached.attached_policies()[0].policy_name().unwrap(),
        "logs-full"
    );

    // Detach
    client
        .detach_user_policy()
        .user_name("policy-user")
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();

    let attached = client
        .list_attached_user_policies()
        .user_name("policy-user")
        .send()
        .await
        .unwrap();
    assert!(attached.attached_policies().is_empty());
}

// ---- User Inline Policy Tests ----

#[tokio::test]
async fn iam_user_inline_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("inline-user")
        .send()
        .await
        .unwrap();

    let inline_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}"#;
    client
        .put_user_policy()
        .user_name("inline-user")
        .policy_name("ddb-access")
        .policy_document(inline_doc)
        .send()
        .await
        .unwrap();

    // List
    let policies = client
        .list_user_policies()
        .user_name("inline-user")
        .send()
        .await
        .unwrap();
    assert_eq!(policies.policy_names().len(), 1);
    assert_eq!(policies.policy_names()[0], "ddb-access");

    // Get
    let get = client
        .get_user_policy()
        .user_name("inline-user")
        .policy_name("ddb-access")
        .send()
        .await
        .unwrap();
    assert_eq!(get.policy_name(), "ddb-access");
    assert_eq!(get.user_name(), "inline-user");

    // Delete
    client
        .delete_user_policy()
        .user_name("inline-user")
        .policy_name("ddb-access")
        .send()
        .await
        .unwrap();

    let policies = client
        .list_user_policies()
        .user_name("inline-user")
        .send()
        .await
        .unwrap();
    assert!(policies.policy_names().is_empty());
}

// ---- Login Profile Tests ----

#[tokio::test]
async fn iam_login_profile_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("login-user")
        .send()
        .await
        .unwrap();

    // Create login profile
    let resp = client
        .create_login_profile()
        .user_name("login-user")
        .password("S3cureP@ss!")
        .password_reset_required(true)
        .send()
        .await
        .unwrap();
    let profile = resp.login_profile().unwrap();
    assert_eq!(profile.user_name(), "login-user");
    assert!(profile.password_reset_required());

    // Get login profile
    let get = client
        .get_login_profile()
        .user_name("login-user")
        .send()
        .await
        .unwrap();
    assert_eq!(get.login_profile().unwrap().user_name(), "login-user");
    assert!(get.login_profile().unwrap().password_reset_required());

    // Update login profile
    client
        .update_login_profile()
        .user_name("login-user")
        .password_reset_required(false)
        .send()
        .await
        .unwrap();

    let get = client
        .get_login_profile()
        .user_name("login-user")
        .send()
        .await
        .unwrap();
    assert!(!get.login_profile().unwrap().password_reset_required());

    // Delete login profile
    client
        .delete_login_profile()
        .user_name("login-user")
        .send()
        .await
        .unwrap();

    // Get should fail after delete
    let result = client
        .get_login_profile()
        .user_name("login-user")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn iam_login_profile_duplicate_fails() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("dup-login")
        .send()
        .await
        .unwrap();
    client
        .create_login_profile()
        .user_name("dup-login")
        .password("pass1")
        .send()
        .await
        .unwrap();

    let result = client
        .create_login_profile()
        .user_name("dup-login")
        .password("pass2")
        .send()
        .await;
    assert!(result.is_err());
}

// ---- MFA Tests ----

#[tokio::test]
async fn iam_virtual_mfa_lifecycle_cli() {
    let server = TestServer::start().await;
    let tmp_dir = std::env::temp_dir();
    let outfile = tmp_dir.join("test-mfa-lifecycle.png");

    // Create virtual MFA device
    let output = server
        .aws_cli(&[
            "iam",
            "create-virtual-mfa-device",
            "--virtual-mfa-device-name",
            "lifecycle-mfa",
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
    let json = output.stdout_json();
    let serial = json["VirtualMFADevice"]["SerialNumber"]
        .as_str()
        .unwrap()
        .to_string();

    // List virtual MFA devices
    let output = server.aws_cli(&["iam", "list-virtual-mfa-devices"]).await;
    assert!(output.success());
    let json = output.stdout_json();
    let devices = json["VirtualMFADevices"].as_array().unwrap();
    assert_eq!(devices.len(), 1);

    // Delete virtual MFA device
    let output = server
        .aws_cli(&[
            "iam",
            "delete-virtual-mfa-device",
            "--serial-number",
            &serial,
        ])
        .await;
    assert!(
        output.success(),
        "delete-virtual-mfa-device failed: {}",
        output.stderr_text()
    );

    // List should be empty
    let output = server.aws_cli(&["iam", "list-virtual-mfa-devices"]).await;
    assert!(output.success());
    let json = output.stdout_json();
    assert!(json["VirtualMFADevices"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn iam_enable_deactivate_list_mfa_cli() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;
    let tmp_dir = std::env::temp_dir();
    let outfile = tmp_dir.join("test-enable-mfa.png");

    // Create user
    iam.create_user()
        .user_name("mfa-cli-user")
        .send()
        .await
        .unwrap();

    // Create virtual MFA device
    let output = server
        .aws_cli(&[
            "iam",
            "create-virtual-mfa-device",
            "--virtual-mfa-device-name",
            "cli-mfa",
            "--outfile",
            outfile.to_str().unwrap(),
            "--bootstrap-method",
            "QRCodePNG",
        ])
        .await;
    let _ = std::fs::remove_file(&outfile);
    assert!(output.success());
    let serial = output.stdout_json()["VirtualMFADevice"]["SerialNumber"]
        .as_str()
        .unwrap()
        .to_string();

    // Enable MFA device
    let output = server
        .aws_cli(&[
            "iam",
            "enable-mfa-device",
            "--user-name",
            "mfa-cli-user",
            "--serial-number",
            &serial,
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

    // List MFA devices for user
    let output = server
        .aws_cli(&["iam", "list-mfa-devices", "--user-name", "mfa-cli-user"])
        .await;
    assert!(output.success());
    let json = output.stdout_json();
    let devices = json["MFADevices"].as_array().unwrap();
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0]["UserName"].as_str().unwrap(), "mfa-cli-user");

    // Deactivate MFA device
    let output = server
        .aws_cli(&[
            "iam",
            "deactivate-mfa-device",
            "--user-name",
            "mfa-cli-user",
            "--serial-number",
            &serial,
        ])
        .await;
    assert!(
        output.success(),
        "deactivate-mfa-device failed: {}",
        output.stderr_text()
    );

    // List MFA devices - should be empty for the user now
    let output = server
        .aws_cli(&["iam", "list-mfa-devices", "--user-name", "mfa-cli-user"])
        .await;
    assert!(output.success());
    let json = output.stdout_json();
    assert!(json["MFADevices"].as_array().unwrap().is_empty());
}

// ---- Account Tests ----

#[tokio::test]
async fn iam_get_account_summary_cli() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    // Create some resources
    iam.create_user()
        .user_name("summary-user")
        .send()
        .await
        .unwrap();
    iam.create_group()
        .group_name("summary-group")
        .send()
        .await
        .unwrap();

    let output = server.aws_cli(&["iam", "get-account-summary"]).await;
    assert!(
        output.success(),
        "get-account-summary failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let summary = &json["SummaryMap"];
    assert_eq!(summary["Users"].as_i64().unwrap(), 1);
    assert_eq!(summary["Groups"].as_i64().unwrap(), 1);
    assert_eq!(summary["UsersQuota"].as_i64().unwrap(), 5000);
}

#[tokio::test]
async fn iam_account_alias_lifecycle_cli() {
    let server = TestServer::start().await;

    // Create alias
    let output = server
        .aws_cli(&["iam", "create-account-alias", "--account-alias", "test-org"])
        .await;
    assert!(
        output.success(),
        "create-account-alias failed: {}",
        output.stderr_text()
    );

    // List aliases
    let output = server.aws_cli(&["iam", "list-account-aliases"]).await;
    assert!(output.success());
    let json = output.stdout_json();
    let aliases = json["AccountAliases"].as_array().unwrap();
    assert_eq!(aliases.len(), 1);
    assert_eq!(aliases[0].as_str().unwrap(), "test-org");

    // Delete alias
    let output = server
        .aws_cli(&["iam", "delete-account-alias", "--account-alias", "test-org"])
        .await;
    assert!(
        output.success(),
        "delete-account-alias failed: {}",
        output.stderr_text()
    );

    // List should be empty
    let output = server.aws_cli(&["iam", "list-account-aliases"]).await;
    assert!(output.success());
    let json = output.stdout_json();
    assert!(json["AccountAliases"].as_array().unwrap().is_empty());
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

// ---- Policy Version Tests ----

#[tokio::test]
async fn iam_policy_version_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;

    let policy = client
        .create_policy()
        .policy_name("ver-test-pol")
        .policy_document(policy_doc)
        .send()
        .await
        .unwrap();
    let policy_arn = policy.policy().unwrap().arn().unwrap().to_string();

    // Create v2 (non-default)
    let v2_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"*"}]}"#;
    let ver = client
        .create_policy_version()
        .policy_arn(&policy_arn)
        .policy_document(v2_doc)
        .set_as_default(false)
        .send()
        .await
        .unwrap();
    assert_eq!(ver.policy_version().unwrap().version_id().unwrap(), "v2");
    assert!(!ver.policy_version().unwrap().is_default_version());

    // Create v3 as default
    let ver = client
        .create_policy_version()
        .policy_arn(&policy_arn)
        .policy_document(v2_doc)
        .set_as_default(true)
        .send()
        .await
        .unwrap();
    assert_eq!(ver.policy_version().unwrap().version_id().unwrap(), "v3");
    assert!(ver.policy_version().unwrap().is_default_version());

    // GetPolicyVersion for v2
    let get = client
        .get_policy_version()
        .policy_arn(&policy_arn)
        .version_id("v2")
        .send()
        .await
        .unwrap();
    assert_eq!(get.policy_version().unwrap().version_id().unwrap(), "v2");

    // ListPolicyVersions
    let list = client
        .list_policy_versions()
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(list.versions().len(), 3);

    // SetDefaultPolicyVersion to v2
    client
        .set_default_policy_version()
        .policy_arn(&policy_arn)
        .version_id("v2")
        .send()
        .await
        .unwrap();

    // Verify v2 is now default
    let get = client
        .get_policy_version()
        .policy_arn(&policy_arn)
        .version_id("v2")
        .send()
        .await
        .unwrap();
    assert!(get.policy_version().unwrap().is_default_version());

    // DeletePolicyVersion v1 (non-default)
    client
        .delete_policy_version()
        .policy_arn(&policy_arn)
        .version_id("v1")
        .send()
        .await
        .unwrap();

    // Deleting default version should fail
    let result = client
        .delete_policy_version()
        .policy_arn(&policy_arn)
        .version_id("v2")
        .send()
        .await;
    assert!(result.is_err(), "deleting default version should fail");

    let list = client
        .list_policy_versions()
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(list.versions().len(), 2);
}

// ---- Server Certificate Tests (CLI) ----

#[tokio::test]
async fn iam_server_certificate_lifecycle() {
    let server = TestServer::start().await;

    // Upload
    let output = server
        .aws_cli(&[
            "iam",
            "upload-server-certificate",
            "--server-certificate-name",
            "test-cert",
            "--certificate-body",
            "-----BEGIN CERTIFICATE-----\nMIIBxTCCAW4=\n-----END CERTIFICATE-----",
            "--private-key",
            "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----",
        ])
        .await;
    assert!(
        output.success(),
        "upload-server-certificate failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(
        json["ServerCertificateMetadata"]["ServerCertificateName"],
        "test-cert"
    );

    // Get
    let output = server
        .aws_cli(&[
            "iam",
            "get-server-certificate",
            "--server-certificate-name",
            "test-cert",
        ])
        .await;
    assert!(output.success(), "get failed: {}", output.stderr_text());
    let json = output.stdout_json();
    assert_eq!(
        json["ServerCertificate"]["ServerCertificateMetadata"]["ServerCertificateName"],
        "test-cert"
    );

    // List
    let output = server.aws_cli(&["iam", "list-server-certificates"]).await;
    assert!(output.success(), "list failed: {}", output.stderr_text());
    let json = output.stdout_json();
    assert_eq!(
        json["ServerCertificateMetadataList"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // Delete
    let output = server
        .aws_cli(&[
            "iam",
            "delete-server-certificate",
            "--server-certificate-name",
            "test-cert",
        ])
        .await;
    assert!(output.success(), "delete failed: {}", output.stderr_text());

    // Verify deleted
    let output = server
        .aws_cli(&[
            "iam",
            "get-server-certificate",
            "--server-certificate-name",
            "test-cert",
        ])
        .await;
    assert!(!output.success(), "get should fail after delete");
}

// ---- SSH Public Key Tests (CLI) ----

#[tokio::test]
async fn iam_ssh_public_key_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("ssh-user")
        .send()
        .await
        .unwrap();

    // Upload via CLI
    let output = server
        .aws_cli(&[
            "iam",
            "upload-ssh-public-key",
            "--user-name",
            "ssh-user",
            "--ssh-public-key-body",
            "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQ test@example",
        ])
        .await;
    assert!(
        output.success(),
        "upload-ssh-public-key failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let key_id = json["SSHPublicKey"]["SSHPublicKeyId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(json["SSHPublicKey"]["Status"], "Active");

    // List
    let output = server
        .aws_cli(&["iam", "list-ssh-public-keys", "--user-name", "ssh-user"])
        .await;
    assert!(output.success(), "list failed: {}", output.stderr_text());
    let json = output.stdout_json();
    assert_eq!(json["SSHPublicKeys"].as_array().unwrap().len(), 1);

    // Update status
    let output = server
        .aws_cli(&[
            "iam",
            "update-ssh-public-key",
            "--user-name",
            "ssh-user",
            "--ssh-public-key-id",
            &key_id,
            "--status",
            "Inactive",
        ])
        .await;
    assert!(output.success(), "update failed: {}", output.stderr_text());

    // Get and verify status
    let output = server
        .aws_cli(&[
            "iam",
            "get-ssh-public-key",
            "--user-name",
            "ssh-user",
            "--ssh-public-key-id",
            &key_id,
            "--encoding",
            "SSH",
        ])
        .await;
    assert!(output.success(), "get failed: {}", output.stderr_text());
    let json = output.stdout_json();
    assert_eq!(json["SSHPublicKey"]["Status"], "Inactive");

    // Delete
    let output = server
        .aws_cli(&[
            "iam",
            "delete-ssh-public-key",
            "--user-name",
            "ssh-user",
            "--ssh-public-key-id",
            &key_id,
        ])
        .await;
    assert!(output.success(), "delete failed: {}", output.stderr_text());
}

// ---- Signing Certificate Tests (CLI) ----

#[tokio::test]
async fn iam_signing_certificate_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("sign-user")
        .send()
        .await
        .unwrap();

    let pem = "-----BEGIN CERTIFICATE-----\nMIIBxTCCAW4=\n-----END CERTIFICATE-----";

    // Upload
    let output = server
        .aws_cli(&[
            "iam",
            "upload-signing-certificate",
            "--user-name",
            "sign-user",
            "--certificate-body",
            pem,
        ])
        .await;
    assert!(
        output.success(),
        "upload-signing-certificate failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let cert_id = json["Certificate"]["CertificateId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(json["Certificate"]["Status"], "Active");

    // List
    let output = server
        .aws_cli(&[
            "iam",
            "list-signing-certificates",
            "--user-name",
            "sign-user",
        ])
        .await;
    assert!(output.success(), "list failed: {}", output.stderr_text());
    let json = output.stdout_json();
    assert_eq!(json["Certificates"].as_array().unwrap().len(), 1);

    // Update to Inactive
    let output = server
        .aws_cli(&[
            "iam",
            "update-signing-certificate",
            "--user-name",
            "sign-user",
            "--certificate-id",
            &cert_id,
            "--status",
            "Inactive",
        ])
        .await;
    assert!(output.success(), "update failed: {}", output.stderr_text());

    // Delete
    let output = server
        .aws_cli(&[
            "iam",
            "delete-signing-certificate",
            "--user-name",
            "sign-user",
            "--certificate-id",
            &cert_id,
        ])
        .await;
    assert!(output.success(), "delete failed: {}", output.stderr_text());
}

// ---- Credential Report Tests (CLI) ----

#[tokio::test]
async fn iam_credential_report() {
    let server = TestServer::start().await;

    // Generate
    let output = server.aws_cli(&["iam", "generate-credential-report"]).await;
    assert!(
        output.success(),
        "generate-credential-report failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let state = json["State"].as_str().unwrap();
    assert!(
        state == "STARTED" || state == "COMPLETE",
        "unexpected state: {state}"
    );

    // Generate again to ensure COMPLETE
    let output = server.aws_cli(&["iam", "generate-credential-report"]).await;
    assert!(output.success());
    let json = output.stdout_json();
    assert_eq!(json["State"], "COMPLETE");

    // Get
    let output = server.aws_cli(&["iam", "get-credential-report"]).await;
    assert!(
        output.success(),
        "get-credential-report failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(json["ReportFormat"], "text/csv");
    assert!(json["Content"].as_str().is_some());
}

// ---- Service Linked Role Tests ----

#[tokio::test]
async fn iam_service_linked_role_lifecycle() {
    let server = TestServer::start().await;

    // Create
    let output = server
        .aws_cli(&[
            "iam",
            "create-service-linked-role",
            "--aws-service-name",
            "autoscaling.amazonaws.com",
        ])
        .await;
    assert!(
        output.success(),
        "create-service-linked-role failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let role_name = json["Role"]["RoleName"].as_str().unwrap();
    assert!(
        role_name.contains("AWSServiceRoleFor"),
        "role name should contain AWSServiceRoleFor, got: {role_name}"
    );
    let path = json["Role"]["Path"].as_str().unwrap();
    assert!(path.contains("/aws-service-role/"));

    // Delete
    let output = server
        .aws_cli(&[
            "iam",
            "delete-service-linked-role",
            "--role-name",
            role_name,
        ])
        .await;
    assert!(
        output.success(),
        "delete-service-linked-role failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let task_id = json["DeletionTaskId"].as_str().unwrap();

    // Get deletion status
    let output = server
        .aws_cli(&[
            "iam",
            "get-service-linked-role-deletion-status",
            "--deletion-task-id",
            task_id,
        ])
        .await;
    assert!(
        output.success(),
        "get-service-linked-role-deletion-status failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(json["Status"], "SUCCEEDED");
}

// ---- Permission Boundary Tests ----

#[tokio::test]
async fn iam_role_permissions_boundary() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    client
        .create_role()
        .role_name("boundary-role")
        .assume_role_policy_document(trust)
        .send()
        .await
        .unwrap();

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let policy = client
        .create_policy()
        .policy_name("boundary-pol")
        .policy_document(policy_doc)
        .send()
        .await
        .unwrap();
    let boundary_arn = policy.policy().unwrap().arn().unwrap().to_string();

    // Put boundary
    client
        .put_role_permissions_boundary()
        .role_name("boundary-role")
        .permissions_boundary(&boundary_arn)
        .send()
        .await
        .unwrap();

    // Get and check
    let role = client
        .get_role()
        .role_name("boundary-role")
        .send()
        .await
        .unwrap();
    let pb = role
        .role()
        .unwrap()
        .permissions_boundary()
        .expect("boundary should be set");
    assert_eq!(pb.permissions_boundary_arn().unwrap(), boundary_arn);

    // Delete boundary
    client
        .delete_role_permissions_boundary()
        .role_name("boundary-role")
        .send()
        .await
        .unwrap();

    let role = client
        .get_role()
        .role_name("boundary-role")
        .send()
        .await
        .unwrap();
    assert!(
        role.role().unwrap().permissions_boundary().is_none(),
        "boundary should be removed"
    );
}

// ---- Tag Role Tests ----

#[tokio::test]
async fn iam_tag_role() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    client
        .create_role()
        .role_name("tag-test-role")
        .assume_role_policy_document(trust)
        .send()
        .await
        .unwrap();

    use aws_sdk_iam::types::Tag;
    client
        .tag_role()
        .role_name("tag-test-role")
        .tags(Tag::builder().key("env").value("prod").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_role_tags()
        .role_name("tag-test-role")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), "env");
    assert_eq!(tags.tags()[0].value(), "prod");

    client
        .untag_role()
        .role_name("tag-test-role")
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_role_tags()
        .role_name("tag-test-role")
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

// ---- Tag Policy Tests ----

#[tokio::test]
async fn iam_tag_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let policy = client
        .create_policy()
        .policy_name("tag-test-pol")
        .policy_document(policy_doc)
        .send()
        .await
        .unwrap();
    let policy_arn = policy.policy().unwrap().arn().unwrap().to_string();

    use aws_sdk_iam::types::Tag;
    client
        .tag_policy()
        .policy_arn(&policy_arn)
        .tags(Tag::builder().key("team").value("infra").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_policy_tags()
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), "team");

    client
        .untag_policy()
        .policy_arn(&policy_arn)
        .tag_keys("team")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_policy_tags()
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

// ---- Tag Instance Profile Tests ----

#[tokio::test]
async fn iam_tag_instance_profile() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_instance_profile()
        .instance_profile_name("tag-test-ip")
        .send()
        .await
        .unwrap();

    use aws_sdk_iam::types::Tag;
    client
        .tag_instance_profile()
        .instance_profile_name("tag-test-ip")
        .tags(Tag::builder().key("zone").value("us-east").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_instance_profile_tags()
        .instance_profile_name("tag-test-ip")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), "zone");

    client
        .untag_instance_profile()
        .instance_profile_name("tag-test-ip")
        .tag_keys("zone")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_instance_profile_tags()
        .instance_profile_name("tag-test-ip")
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

// ---- Tag OIDC Provider Tests ----

#[tokio::test]
async fn iam_tag_oidc_provider() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let resp = client
        .create_open_id_connect_provider()
        .url("https://tag-oidc.example.com")
        .thumbprint_list("abcdef1234567890abcdef1234567890abcdef12")
        .send()
        .await
        .unwrap();
    let oidc_arn = resp.open_id_connect_provider_arn().unwrap().to_string();

    use aws_sdk_iam::types::Tag;
    client
        .tag_open_id_connect_provider()
        .open_id_connect_provider_arn(&oidc_arn)
        .tags(Tag::builder().key("stage").value("dev").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_open_id_connect_provider_tags()
        .open_id_connect_provider_arn(&oidc_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), "stage");

    client
        .untag_open_id_connect_provider()
        .open_id_connect_provider_arn(&oidc_arn)
        .tag_keys("stage")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_open_id_connect_provider_tags()
        .open_id_connect_provider_arn(&oidc_arn)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

// ---- Update Role / UpdateRoleDescription / UpdateAssumeRolePolicy Tests ----

#[tokio::test]
async fn iam_update_role_and_assume_role_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    client
        .create_role()
        .role_name("upd-role")
        .assume_role_policy_document(trust)
        .send()
        .await
        .unwrap();

    // UpdateRole: set description and max session duration
    client
        .update_role()
        .role_name("upd-role")
        .description("updated description")
        .max_session_duration(7200)
        .send()
        .await
        .unwrap();

    let role = client
        .get_role()
        .role_name("upd-role")
        .send()
        .await
        .unwrap();
    assert_eq!(
        role.role().unwrap().description().unwrap(),
        "updated description"
    );
    assert_eq!(role.role().unwrap().max_session_duration().unwrap(), 7200);

    // UpdateRoleDescription
    let resp = client
        .update_role_description()
        .role_name("upd-role")
        .description("new desc")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.role().unwrap().description().unwrap(), "new desc");

    // UpdateAssumeRolePolicy
    let new_trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    client
        .update_assume_role_policy()
        .role_name("upd-role")
        .policy_document(new_trust)
        .send()
        .await
        .unwrap();

    let role = client
        .get_role()
        .role_name("upd-role")
        .send()
        .await
        .unwrap();
    let doc = role.role().unwrap().assume_role_policy_document().unwrap();
    assert!(
        doc.contains("lambda.amazonaws.com"),
        "trust policy should be updated"
    );
}

// ---- UpdateGroup Tests ----

#[tokio::test]
async fn iam_update_group() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_group()
        .group_name("old-group")
        .send()
        .await
        .unwrap();

    // Rename
    client
        .update_group()
        .group_name("old-group")
        .new_group_name("new-group")
        .send()
        .await
        .unwrap();

    // Old name should fail
    let result = client.get_group().group_name("old-group").send().await;
    assert!(result.is_err());

    // New name should work
    let resp = client
        .get_group()
        .group_name("new-group")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.group().unwrap().group_name(), "new-group");
}

// ---- UpdateUser Tests ----

#[tokio::test]
async fn iam_update_user() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("rename-me")
        .send()
        .await
        .unwrap();

    client
        .update_user()
        .user_name("rename-me")
        .new_user_name("renamed-user")
        .send()
        .await
        .unwrap();

    let result = client.get_user().user_name("rename-me").send().await;
    assert!(result.is_err());

    let resp = client
        .get_user()
        .user_name("renamed-user")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.user().unwrap().user_name(), "renamed-user");
}

// ---- Account Password Policy Tests ----

#[tokio::test]
async fn iam_account_password_policy() {
    let server = TestServer::start().await;

    // Get before set should fail
    let output = server
        .aws_cli(&["iam", "get-account-password-policy"])
        .await;
    assert!(!output.success());

    // Update
    let output = server
        .aws_cli(&[
            "iam",
            "update-account-password-policy",
            "--minimum-password-length",
            "14",
            "--require-symbols",
        ])
        .await;
    assert!(
        output.success(),
        "update-account-password-policy failed: {}",
        output.stderr_text()
    );

    // Get
    let output = server
        .aws_cli(&["iam", "get-account-password-policy"])
        .await;
    assert!(output.success(), "get failed: {}", output.stderr_text());
    let json = output.stdout_json();
    assert_eq!(json["PasswordPolicy"]["MinimumPasswordLength"], 14);
    assert_eq!(json["PasswordPolicy"]["RequireSymbols"], true);

    // Delete
    let output = server
        .aws_cli(&["iam", "delete-account-password-policy"])
        .await;
    assert!(output.success(), "delete failed: {}", output.stderr_text());

    // Should be gone
    let output = server
        .aws_cli(&["iam", "get-account-password-policy"])
        .await;
    assert!(!output.success());
}

// ---- GetAccountAuthorizationDetails Tests ----

#[tokio::test]
async fn iam_get_account_authorization_details() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("authz-user")
        .send()
        .await
        .unwrap();

    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    client
        .create_role()
        .role_name("authz-role")
        .assume_role_policy_document(trust)
        .send()
        .await
        .unwrap();

    let output = server
        .aws_cli(&["iam", "get-account-authorization-details"])
        .await;
    assert!(
        output.success(),
        "get-account-authorization-details failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();

    let users = json["UserDetailList"].as_array().unwrap();
    assert!(
        users.iter().any(|u| u["UserName"] == "authz-user"),
        "should contain authz-user"
    );

    let roles = json["RoleDetailList"].as_array().unwrap();
    assert!(
        roles.iter().any(|r| r["RoleName"] == "authz-role"),
        "should contain authz-role"
    );
}

// ---- ListEntitiesForPolicy Tests ----

#[tokio::test]
async fn iam_list_entities_for_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let policy = client
        .create_policy()
        .policy_name("entities-pol")
        .policy_document(policy_doc)
        .send()
        .await
        .unwrap();
    let policy_arn = policy.policy().unwrap().arn().unwrap().to_string();

    // Create and attach to role
    let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
    client
        .create_role()
        .role_name("ent-test-role")
        .assume_role_policy_document(trust)
        .send()
        .await
        .unwrap();
    client
        .attach_role_policy()
        .role_name("ent-test-role")
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();

    let output = server
        .aws_cli(&[
            "iam",
            "list-entities-for-policy",
            "--policy-arn",
            &policy_arn,
        ])
        .await;
    assert!(
        output.success(),
        "list-entities-for-policy failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let roles = json["PolicyRoles"].as_array().unwrap();
    assert!(
        roles.iter().any(|r| r["RoleName"] == "ent-test-role"),
        "should list attached role"
    );
}

// ---- GetAccessKeyLastUsed Tests ----

#[tokio::test]
async fn iam_get_access_key_last_used() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("lastused-user")
        .send()
        .await
        .unwrap();

    let key = client
        .create_access_key()
        .user_name("lastused-user")
        .send()
        .await
        .unwrap();
    let key_id = key.access_key().unwrap().access_key_id().to_string();

    let output = server
        .aws_cli(&[
            "iam",
            "get-access-key-last-used",
            "--access-key-id",
            &key_id,
        ])
        .await;
    assert!(
        output.success(),
        "get-access-key-last-used failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(json["UserName"], "lastused-user");
}
