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

#[tokio::test]
async fn test_create_route() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let route = client
        .create_route()
        .api_id(api_id)
        .route_key("GET /pets")
        .send()
        .await
        .unwrap();

    assert!(route.route_id().is_some());
    assert_eq!(route.route_key(), Some("GET /pets"));
}

#[tokio::test]
async fn test_get_route() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let created = client
        .create_route()
        .api_id(api_id)
        .route_key("GET /pets")
        .send()
        .await
        .unwrap();

    let route_id = created.route_id().unwrap();

    let result = client
        .get_route()
        .api_id(api_id)
        .route_id(route_id)
        .send()
        .await
        .unwrap();

    assert_eq!(result.route_id(), Some(route_id));
    assert_eq!(result.route_key(), Some("GET /pets"));
}

#[tokio::test]
async fn test_get_routes() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    client
        .create_route()
        .api_id(api_id)
        .route_key("GET /pets")
        .send()
        .await
        .unwrap();

    client
        .create_route()
        .api_id(api_id)
        .route_key("POST /pets")
        .send()
        .await
        .unwrap();

    let result = client.get_routes().api_id(api_id).send().await.unwrap();

    let items = result.items();
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn test_update_route() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let created = client
        .create_route()
        .api_id(api_id)
        .route_key("GET /pets")
        .send()
        .await
        .unwrap();

    let route_id = created.route_id().unwrap();

    let result = client
        .update_route()
        .api_id(api_id)
        .route_id(route_id)
        .route_key("GET /pets/{id}")
        .send()
        .await
        .unwrap();

    assert_eq!(result.route_key(), Some("GET /pets/{id}"));
}

#[tokio::test]
async fn test_delete_route() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let created = client
        .create_route()
        .api_id(api_id)
        .route_key("GET /pets")
        .send()
        .await
        .unwrap();

    let route_id = created.route_id().unwrap();

    client
        .delete_route()
        .api_id(api_id)
        .route_id(route_id)
        .send()
        .await
        .unwrap();

    let result = client
        .get_route()
        .api_id(api_id)
        .route_id(route_id)
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_integration() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let integration = client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
        .integration_uri("arn:aws:lambda:us-east-1:123456789012:function:my-function")
        .payload_format_version("2.0")
        .send()
        .await
        .unwrap();

    assert!(integration.integration_id().is_some());
    assert_eq!(
        integration.integration_type(),
        Some(&aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
    );
    assert_eq!(
        integration.integration_uri(),
        Some("arn:aws:lambda:us-east-1:123456789012:function:my-function")
    );
}

#[tokio::test]
async fn test_get_integration() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let created = client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
        .integration_uri("arn:aws:lambda:us-east-1:123456789012:function:my-function")
        .send()
        .await
        .unwrap();

    let integration_id = created.integration_id().unwrap();

    let result = client
        .get_integration()
        .api_id(api_id)
        .integration_id(integration_id)
        .send()
        .await
        .unwrap();

    assert_eq!(result.integration_id(), Some(integration_id));
    assert_eq!(
        result.integration_type(),
        Some(&aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
    );
}

#[tokio::test]
async fn test_get_integrations() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
        .integration_uri("arn:aws:lambda:us-east-1:123456789012:function:fn1")
        .send()
        .await
        .unwrap();

    client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::HttpProxy)
        .integration_uri("https://example.com")
        .send()
        .await
        .unwrap();

    let result = client
        .get_integrations()
        .api_id(api_id)
        .send()
        .await
        .unwrap();

    let items = result.items();
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn test_update_integration() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let created = client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
        .integration_uri("arn:aws:lambda:us-east-1:123456789012:function:fn1")
        .send()
        .await
        .unwrap();

    let integration_id = created.integration_id().unwrap();

    let result = client
        .update_integration()
        .api_id(api_id)
        .integration_id(integration_id)
        .integration_uri("arn:aws:lambda:us-east-1:123456789012:function:fn2")
        .payload_format_version("2.0")
        .send()
        .await
        .unwrap();

    assert_eq!(
        result.integration_uri(),
        Some("arn:aws:lambda:us-east-1:123456789012:function:fn2")
    );
    assert_eq!(result.payload_format_version(), Some("2.0"));
}

#[tokio::test]
async fn test_delete_integration() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let created = client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
        .integration_uri("arn:aws:lambda:us-east-1:123456789012:function:my-function")
        .send()
        .await
        .unwrap();

    let integration_id = created.integration_id().unwrap();

    client
        .delete_integration()
        .api_id(api_id)
        .integration_id(integration_id)
        .send()
        .await
        .unwrap();

    let result = client
        .get_integration()
        .api_id(api_id)
        .integration_id(integration_id)
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_route_with_target_integration() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let integration = client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
        .integration_uri("arn:aws:lambda:us-east-1:123456789012:function:my-function")
        .send()
        .await
        .unwrap();

    let integration_id = integration.integration_id().unwrap();

    let route = client
        .create_route()
        .api_id(api_id)
        .route_key("GET /pets")
        .target(format!("integrations/{}", integration_id))
        .send()
        .await
        .unwrap();

    assert_eq!(
        route.target(),
        Some(format!("integrations/{}", integration_id).as_str())
    );
}

#[tokio::test]
async fn test_create_stage() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let stage = client
        .create_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    assert_eq!(stage.stage_name(), Some("prod"));
    assert!(stage.created_date().is_some());
}

#[tokio::test]
async fn test_get_stage() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    client
        .create_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    let result = client
        .get_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    assert_eq!(result.stage_name(), Some("prod"));
}

#[tokio::test]
async fn test_get_stages() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    client
        .create_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    client
        .create_stage()
        .api_id(api_id)
        .stage_name("dev")
        .send()
        .await
        .unwrap();

    let result = client.get_stages().api_id(api_id).send().await.unwrap();

    let items = result.items();
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn test_update_stage() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    client
        .create_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    let result = client
        .update_stage()
        .api_id(api_id)
        .stage_name("prod")
        .description("Production stage")
        .send()
        .await
        .unwrap();

    assert_eq!(result.description(), Some("Production stage"));
}

#[tokio::test]
async fn test_delete_stage() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    client
        .create_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    client
        .delete_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    let result = client
        .get_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_deployment() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let deployment = client
        .create_deployment()
        .api_id(api_id)
        .description("Initial deployment")
        .send()
        .await
        .unwrap();

    assert!(deployment.deployment_id().is_some());
    assert_eq!(deployment.description(), Some("Initial deployment"));
}

#[tokio::test]
async fn test_get_deployment() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let created = client
        .create_deployment()
        .api_id(api_id)
        .send()
        .await
        .unwrap();

    let deployment_id = created.deployment_id().unwrap();

    let result = client
        .get_deployment()
        .api_id(api_id)
        .deployment_id(deployment_id)
        .send()
        .await
        .unwrap();

    assert_eq!(result.deployment_id(), Some(deployment_id));
}

#[tokio::test]
async fn test_get_deployments() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    client
        .create_deployment()
        .api_id(api_id)
        .description("Deployment 1")
        .send()
        .await
        .unwrap();

    client
        .create_deployment()
        .api_id(api_id)
        .description("Deployment 2")
        .send()
        .await
        .unwrap();

    let result = client
        .get_deployments()
        .api_id(api_id)
        .send()
        .await
        .unwrap();

    let items = result.items();
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn test_deployment_with_stage() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    let api = client
        .create_api()
        .name("test-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    let stage = client
        .create_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    assert_eq!(stage.stage_name(), Some("prod"));
    assert_eq!(stage.deployment_id(), None);

    let deployment = client
        .create_deployment()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    let deployment_id = deployment.deployment_id().unwrap();

    let updated_stage = client
        .get_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    assert_eq!(updated_stage.deployment_id(), Some(deployment_id));
}
