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

#[tokio::test]
async fn test_lambda_proxy_integration() {
    use aws_sdk_lambda::primitives::Blob;
    use std::io::Write;

    let server = TestServer::start().await;
    let lambda_client = server.lambda_client().await;
    let apigw_client = server.apigatewayv2_client().await;

    // Create a Lambda function
    let function_code = r#"
exports.handler = async (event) => {
    return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            message: "Hello from Lambda",
            routeKey: event.routeKey,
            path: event.rawPath,
            pathParams: event.pathParameters,
            queryParams: event.queryStringParameters
        })
    };
};
    "#;

    // Create zip file
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    let options = zip::write::SimpleFileOptions::default();
    writer.start_file("index.js", options).unwrap();
    writer.write_all(function_code.as_bytes()).unwrap();
    let cursor = writer.finish().unwrap();
    let zip_bytes = cursor.into_inner();

    lambda_client
        .create_function()
        .function_name("test-apigw-function")
        .runtime(aws_sdk_lambda::types::Runtime::Nodejs20x)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(zip_bytes))
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Create API Gateway API
    let api = apigw_client
        .create_api()
        .name("test-lambda-api")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();

    let api_id = api.api_id().unwrap();

    // Create integration with Lambda
    let integration = apigw_client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
        .integration_uri("arn:aws:lambda:us-east-1:123456789012:function:test-apigw-function")
        .payload_format_version("2.0")
        .send()
        .await
        .unwrap();

    let integration_id = integration.integration_id().unwrap();

    // Create route
    apigw_client
        .create_route()
        .api_id(api_id)
        .route_key("GET /hello/{name}")
        .target(format!("integrations/{}", integration_id))
        .send()
        .await
        .unwrap();

    // Create stage
    apigw_client
        .create_stage()
        .api_id(api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    // Invoke the API via HTTP
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(format!(
            "{}/prod/hello/world?greeting=hi",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["message"], "Hello from Lambda");
    assert_eq!(body["routeKey"], "GET /hello/{name}");
    assert_eq!(body["path"], "/prod/hello/world");
    assert_eq!(body["pathParams"]["name"], "world");
    assert_eq!(body["queryParams"]["greeting"], "hi");
}

#[tokio::test]
async fn test_cors_actual_request() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    // Create API with CORS configuration
    let api = client
        .create_api()
        .name("test-api-cors-actual")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .cors_configuration(
            aws_sdk_apigatewayv2::types::Cors::builder()
                .allow_origins("*")
                .expose_headers("X-Custom-Header")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let api_id = api.api_id.as_ref().unwrap();

    // Create integration
    let integration = client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::Mock)
        .send()
        .await
        .unwrap();
    let integration_id = integration.integration_id.as_ref().unwrap();

    // Create route
    client
        .create_route()
        .api_id(api_id)
        .route_key("GET /test")
        .target(format!("integrations/{}", integration_id))
        .send()
        .await
        .unwrap();

    // Create stage
    client
        .create_stage()
        .api_id(api_id)
        .stage_name("prod")
        .auto_deploy(true)
        .send()
        .await
        .unwrap();

    // Send actual request
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(format!("{}/prod/test", server.endpoint()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .unwrap(),
        "*"
    );
    assert_eq!(
        response
            .headers()
            .get("access-control-expose-headers")
            .unwrap(),
        "X-Custom-Header"
    );
}

#[tokio::test]
async fn test_mock_integration() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    // Create API
    let api = client
        .create_api()
        .name("test-api-mock")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();
    let api_id = api.api_id.as_ref().unwrap();

    // Create mock integration
    let integration = client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::Mock)
        .send()
        .await
        .unwrap();
    let integration_id = integration.integration_id.as_ref().unwrap();

    // Create route
    client
        .create_route()
        .api_id(api_id)
        .route_key("GET /mock")
        .target(format!("integrations/{}", integration_id))
        .send()
        .await
        .unwrap();

    // Create stage
    client
        .create_stage()
        .api_id(api_id)
        .stage_name("prod")
        .auto_deploy(true)
        .send()
        .await
        .unwrap();

    // Invoke the mock endpoint
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(format!("{}/prod/mock", server.endpoint()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["message"], "This is a mock response");
}

#[tokio::test]
async fn test_authorizer_crud() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    // Create API
    let api = client
        .create_api()
        .name("test-api-authorizers")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();
    let api_id = api.api_id.as_ref().unwrap();

    // Create JWT authorizer
    let jwt_auth = client
        .create_authorizer()
        .api_id(api_id)
        .authorizer_type(aws_sdk_apigatewayv2::types::AuthorizerType::Jwt)
        .name("jwt-authorizer")
        .identity_source("$request.header.Authorization")
        .jwt_configuration(
            aws_sdk_apigatewayv2::types::JwtConfiguration::builder()
                .audience("https://api.example.com")
                .issuer("https://auth.example.com")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let jwt_auth_id = jwt_auth.authorizer_id.as_ref().unwrap();

    assert_eq!(jwt_auth.name.as_ref().unwrap(), "jwt-authorizer");
    assert_eq!(
        jwt_auth.authorizer_type.as_ref().unwrap(),
        &aws_sdk_apigatewayv2::types::AuthorizerType::Jwt
    );

    // Create REQUEST authorizer
    let request_auth = client
        .create_authorizer()
        .api_id(api_id)
        .authorizer_type(aws_sdk_apigatewayv2::types::AuthorizerType::Request)
        .name("lambda-authorizer")
        .authorizer_uri("arn:aws:lambda:us-east-1:000000000000:function:my-authorizer")
        .identity_source("$request.header.Authorization")
        .send()
        .await
        .unwrap();
    let request_auth_id = request_auth.authorizer_id.as_ref().unwrap();

    assert_eq!(request_auth.name.as_ref().unwrap(), "lambda-authorizer");
    assert_eq!(
        request_auth.authorizer_type.as_ref().unwrap(),
        &aws_sdk_apigatewayv2::types::AuthorizerType::Request
    );

    // Get authorizer
    let get_result = client
        .get_authorizer()
        .api_id(api_id)
        .authorizer_id(jwt_auth_id)
        .send()
        .await
        .unwrap();

    assert_eq!(get_result.name.as_ref().unwrap(), "jwt-authorizer");

    // List authorizers
    let list_result = client
        .get_authorizers()
        .api_id(api_id)
        .send()
        .await
        .unwrap();

    assert_eq!(list_result.items.as_ref().unwrap().len(), 2);

    // Update authorizer
    let update_result = client
        .update_authorizer()
        .api_id(api_id)
        .authorizer_id(jwt_auth_id)
        .name("updated-jwt-authorizer")
        .send()
        .await
        .unwrap();

    assert_eq!(
        update_result.name.as_ref().unwrap(),
        "updated-jwt-authorizer"
    );

    // Delete authorizer
    client
        .delete_authorizer()
        .api_id(api_id)
        .authorizer_id(request_auth_id)
        .send()
        .await
        .unwrap();

    // Verify deletion
    let list_after_delete = client
        .get_authorizers()
        .api_id(api_id)
        .send()
        .await
        .unwrap();

    assert_eq!(list_after_delete.items.as_ref().unwrap().len(), 1);
}

#[tokio::test]
async fn test_simulation_endpoint() {
    let server = TestServer::start().await;
    let client = server.apigatewayv2_client().await;

    // Create API with Lambda integration
    let api = client
        .create_api()
        .name("test-api-simulation")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap();
    let api_id = api.api_id.as_ref().unwrap();

    // Create mock integration
    let integration = client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::Mock)
        .send()
        .await
        .unwrap();
    let integration_id = integration.integration_id.as_ref().unwrap();

    // Create route
    client
        .create_route()
        .api_id(api_id)
        .route_key("GET /test")
        .target(format!("integrations/{}", integration_id))
        .send()
        .await
        .unwrap();

    // Create stage
    client
        .create_stage()
        .api_id(api_id)
        .stage_name("prod")
        .auto_deploy(true)
        .send()
        .await
        .unwrap();

    // Make a request to the API
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(format!("{}/prod/test?param1=value1", server.endpoint()))
        .header("X-Custom-Header", "test-value")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    // Check simulation endpoint
    let history_response = http_client
        .get(format!(
            "{}/_fakecloud/apigatewayv2/requests",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(history_response.status(), 200);
    let history: serde_json::Value = history_response.json().await.unwrap();
    let requests = history["requests"].as_array().unwrap();

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["method"].as_str().unwrap(), "GET");
    assert_eq!(requests[0]["path"].as_str().unwrap(), "/test");
    assert_eq!(requests[0]["stage"].as_str().unwrap(), "prod");
    assert_eq!(requests[0]["apiId"].as_str().unwrap(), api_id);
    assert_eq!(
        requests[0]["headers"]["x-custom-header"].as_str().unwrap(),
        "test-value"
    );
    assert_eq!(
        requests[0]["queryParams"]["param1"].as_str().unwrap(),
        "value1"
    );
    assert_eq!(requests[0]["statusCode"].as_u64().unwrap(), 200);
}
