mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// ---------------------------------------------------------------------------
// User Pool lifecycle
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "CreateUserPool", checksum = "42068aef")]
#[tokio::test]
async fn cognito_create_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let resp = client
        .create_user_pool()
        .pool_name("conformance-pool")
        .send()
        .await
        .unwrap();
    let pool = resp.user_pool().unwrap();
    assert!(!pool.id().unwrap().is_empty());
    assert!(pool.arn().unwrap().contains(":userpool/"));
    assert_eq!(pool.name().unwrap(), "conformance-pool");
}

#[test_action("cognito-idp", "DescribeUserPool", checksum = "974e2ffd")]
#[tokio::test]
async fn cognito_describe_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let create = client
        .create_user_pool()
        .pool_name("desc-pool")
        .send()
        .await
        .unwrap();
    let pool_id = create.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.user_pool().unwrap().name().unwrap(), "desc-pool");
}

#[test_action("cognito-idp", "UpdateUserPool", checksum = "556fb3e5")]
#[tokio::test]
async fn cognito_update_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let create = client
        .create_user_pool()
        .pool_name("update-pool")
        .send()
        .await
        .unwrap();
    let pool_id = create.user_pool().unwrap().id().unwrap().to_string();

    client
        .update_user_pool()
        .user_pool_id(&pool_id)
        .mfa_configuration(aws_sdk_cognitoidentityprovider::types::UserPoolMfaType::On)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.user_pool().unwrap().mfa_configuration(),
        Some(&aws_sdk_cognitoidentityprovider::types::UserPoolMfaType::On),
    );
}

#[test_action("cognito-idp", "DeleteUserPool", checksum = "b1e5f200")]
#[tokio::test]
async fn cognito_delete_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let create = client
        .create_user_pool()
        .pool_name("del-pool")
        .send()
        .await
        .unwrap();
    let pool_id = create.user_pool().unwrap().id().unwrap().to_string();

    client
        .delete_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();

    let err = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap_err();
    let svc_err = err.into_service_error();
    assert!(svc_err.is_resource_not_found_exception());
}

#[test_action("cognito-idp", "ListUserPools", checksum = "f67b8722")]
#[tokio::test]
async fn cognito_list_user_pools() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    client
        .create_user_pool()
        .pool_name("list-pool-1")
        .send()
        .await
        .unwrap();
    client
        .create_user_pool()
        .pool_name("list-pool-2")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_user_pools()
        .max_results(10)
        .send()
        .await
        .unwrap();
    assert!(resp.user_pools().len() >= 2);
}

// ---------------------------------------------------------------------------
// User Pool Client lifecycle
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "CreateUserPoolClient", checksum = "74d65959")]
#[tokio::test]
async fn cognito_create_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("client-test-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("test-client")
        .send()
        .await
        .unwrap();
    let upc = resp.user_pool_client().unwrap();
    assert!(!upc.client_id().unwrap().is_empty());
    assert_eq!(upc.client_name().unwrap(), "test-client");
}

#[test_action("cognito-idp", "DescribeUserPoolClient", checksum = "7dc2fb48")]
#[tokio::test]
async fn cognito_describe_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("desc-client-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let created = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("desc-client")
        .send()
        .await
        .unwrap();
    let client_id = created
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    let resp = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.user_pool_client().unwrap().client_name().unwrap(),
        "desc-client"
    );
}

#[test_action("cognito-idp", "UpdateUserPoolClient", checksum = "8ba26c73")]
#[tokio::test]
async fn cognito_update_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("upd-client-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let created = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("upd-client")
        .send()
        .await
        .unwrap();
    let client_id = created
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .update_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .client_name("renamed-client")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.user_pool_client().unwrap().client_name().unwrap(),
        "renamed-client"
    );
}

#[test_action("cognito-idp", "DeleteUserPoolClient", checksum = "954e5fa3")]
#[tokio::test]
async fn cognito_delete_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("del-client-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let created = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("del-client")
        .send()
        .await
        .unwrap();
    let client_id = created
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .delete_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();

    let err = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_resource_not_found_exception());
}

#[test_action("cognito-idp", "ListUserPoolClients", checksum = "3946c12e")]
#[tokio::test]
async fn cognito_list_user_pool_clients() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("list-client-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("c1")
        .send()
        .await
        .unwrap();
    client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("c2")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_user_pool_clients()
        .user_pool_id(&pool_id)
        .max_results(10)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.user_pool_clients().len(), 2);
}
