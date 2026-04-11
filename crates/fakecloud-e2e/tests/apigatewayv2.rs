mod helpers;
use helpers::TestServer;

#[tokio::test]
async fn test_create_api() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let result = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    assert_eq!(result.name(), Some("test-api"));
    assert_eq!(
        result.protocol_type(),
        Some(&aws_sdk_apigatewayv2::types::ProtocolType::Http)
    );
    assert!(result.api_id().is_some());
    assert!(result.api_endpoint().is_some());
}

#[tokio::test]
async fn test_get_api() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let created = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = created.api_id().unwrap();

    let result = client.get_api().api_id(api_id).send().await.unwrap();

    assert_eq!(result.api_id(), Some(api_id));
    assert_eq!(result.name(), Some("test-api"));
}

#[tokio::test]
async fn test_get_apis() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    client
        .create_api()
        .name("api-1")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    client
        .create_api()
        .name("api-2")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let result = client.get_apis().send().await.unwrap();

    let items = result.items();
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn test_update_api() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let created = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = created.api_id().unwrap();

    let result = client
        .update_api()
        .api_id(api_id)
        .name("updated-api")
        .description("Updated description")
        .send()
        .await
        .unwrap();

    assert_eq!(result.name(), Some("updated-api"));
    assert_eq!(result.description(), Some("Updated description"));
}

#[tokio::test]
async fn test_delete_api() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let created = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = created.api_id().unwrap();

    client.delete_api().api_id(api_id).send().await.unwrap();

    let result = client.get_api().api_id(api_id).send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_api_with_tags() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let result = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .tags("env", "test")
        .tags("team", "platform")
        .send()
        .await
        .unwrap();

    assert!(result.tags().is_some());
    let tags = result.tags().unwrap();
    assert_eq!(tags.get("env"), Some(&"test".to_string()));
    assert_eq!(tags.get("team"), Some(&"platform".to_string()));
}
