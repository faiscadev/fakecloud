mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn sts_get_caller_identity() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;

    let resp = client.get_caller_identity().send().await.unwrap();
    assert_eq!(resp.account().unwrap(), "000000000000");
    assert!(resp.arn().unwrap().contains("root"));
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
async fn sts_get_caller_identity_cli() {
    let server = TestServer::start().await;
    let output = server.aws_cli(&["sts", "get-caller-identity"]).await;
    assert!(
        output.success(),
        "CLI should succeed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(json["Account"], "000000000000");
}
