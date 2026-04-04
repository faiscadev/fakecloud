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
    assert!(key.access_key_id().starts_with("AKIA"));
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

    // Access key IDs should start with ASIA
    assert!(creds1.access_key_id().starts_with("ASIA"));
    assert!(creds2.access_key_id().starts_with("ASIA"));

    // Session token should be realistic length (>100 chars)
    assert!(creds1.session_token().len() > 100);
}

#[tokio::test]
async fn sts_get_session_token() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;

    let resp = client.get_session_token().send().await.unwrap();
    let creds = resp.credentials().unwrap();
    assert!(creds.access_key_id().starts_with("ASIA"));
    assert_eq!(creds.access_key_id().len(), 20);
    assert_eq!(creds.secret_access_key().len(), 40);
    assert_eq!(creds.session_token().len(), 356);
    assert!(creds.session_token().starts_with("FQoGZXIvYXdzE"));
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
    assert!(creds.access_key_id().starts_with("ASIA"));
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
    assert!(creds.access_key_id().starts_with("ASIA"));
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
