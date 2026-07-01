use super::*;
use bytes::Bytes;
use http::{HeaderMap, Method};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

fn make_state() -> SharedApiGatewayV2State {
    Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ))
}

fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
    let raw_path = path.to_string();
    let segs: Vec<String> = raw_path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    AwsRequest {
        service: "apigateway".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-id".to_string(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(body.to_string()),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: segs,
        raw_path,
        raw_query: String::new(),
        method,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn body_json(resp: &AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Err(e) => e,
        Ok(_) => panic!("expected error, got Ok"),
    }
}

/// Create an API and return its ID.
fn create_api(svc: &ApiGatewayV2Service) -> String {
    let body = serde_json::json!({"name": "test-api", "protocolType": "HTTP"});
    let req = make_request(Method::POST, "/v2/apis", &body.to_string());
    let resp = svc.create_api(&req).unwrap();
    let b = body_json(&resp);
    b["apiId"].as_str().unwrap().to_string()
}

// ── resolve_action routing ──

#[test]
fn resolve_action_api_crud() {
    let req = make_request(Method::POST, "/v2/apis", "{}");
    let (action, _, _) = ApiGatewayV2Service::resolve_action(&req).unwrap();
    assert_eq!(action, "CreateApi");

    let req = make_request(Method::GET, "/v2/apis", "");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "GetApis"
    );

    let req = make_request(Method::GET, "/v2/apis/abc", "");
    let (action, api_id, _) = ApiGatewayV2Service::resolve_action(&req).unwrap();
    assert_eq!(action, "GetApi");
    assert_eq!(api_id.unwrap(), "abc");

    let req = make_request(Method::PATCH, "/v2/apis/abc", "{}");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "UpdateApi"
    );

    let req = make_request(Method::DELETE, "/v2/apis/abc", "");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "DeleteApi"
    );
}

#[test]
fn resolve_action_routes() {
    let req = make_request(Method::POST, "/v2/apis/a1/routes", "{}");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "CreateRoute"
    );

    let req = make_request(Method::GET, "/v2/apis/a1/routes", "");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "GetRoutes"
    );

    let req = make_request(Method::GET, "/v2/apis/a1/routes/r1", "");
    let (action, _, rid) = ApiGatewayV2Service::resolve_action(&req).unwrap();
    assert_eq!(action, "GetRoute");
    assert_eq!(rid.unwrap(), "r1");
}

#[test]
fn resolve_action_integrations() {
    let req = make_request(Method::POST, "/v2/apis/a1/integrations", "{}");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "CreateIntegration"
    );

    let req = make_request(Method::GET, "/v2/apis/a1/integrations", "");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "GetIntegrations"
    );

    let req = make_request(Method::DELETE, "/v2/apis/a1/integrations/i1", "");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "DeleteIntegration"
    );
}

#[test]
fn resolve_action_stages() {
    let req = make_request(Method::POST, "/v2/apis/a1/stages", "{}");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "CreateStage"
    );

    let req = make_request(Method::GET, "/v2/apis/a1/stages/prod", "");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "GetStage"
    );
}

#[test]
fn resolve_action_deployments() {
    let req = make_request(Method::POST, "/v2/apis/a1/deployments", "{}");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "CreateDeployment"
    );

    let req = make_request(Method::GET, "/v2/apis/a1/deployments/d1", "");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "GetDeployment"
    );
}

#[test]
fn resolve_action_authorizers() {
    let req = make_request(Method::POST, "/v2/apis/a1/authorizers", "{}");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "CreateAuthorizer"
    );

    let req = make_request(Method::PATCH, "/v2/apis/a1/authorizers/au1", "{}");
    assert_eq!(
        ApiGatewayV2Service::resolve_action(&req).unwrap().0,
        "UpdateAuthorizer"
    );
}

#[test]
fn resolve_action_unknown_returns_none() {
    let req = make_request(Method::POST, "/v1/something", "{}");
    assert!(ApiGatewayV2Service::resolve_action(&req).is_none());

    let req = make_request(Method::PUT, "/v2/apis/a1/routes/r1", "{}");
    assert!(ApiGatewayV2Service::resolve_action(&req).is_none());
}

// ── API CRUD ──

#[tokio::test]
async fn api_create_get_list_update_delete() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);

    // Create
    let body = serde_json::json!({"name": "my-api", "protocolType": "HTTP"});
    let req = make_request(Method::POST, "/v2/apis", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    let api_id = b["apiId"].as_str().unwrap().to_string();
    assert_eq!(b["name"], "my-api");
    assert_eq!(b["protocolType"], "HTTP");
    assert!(b["apiEndpoint"].as_str().is_some());

    // Get
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}"), "");
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["name"], "my-api");

    // List
    let req = make_request(Method::GET, "/v2/apis", "");
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["items"].as_array().unwrap().len(), 1);

    // Update
    let body = serde_json::json!({"name": "updated-api", "description": "desc"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["name"], "updated-api");
    assert_eq!(b["description"], "desc");

    // Delete
    let req = make_request(Method::DELETE, &format!("/v2/apis/{api_id}"), "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    // Get should fail
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}"), "");
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("NotFoundException"));
}

#[tokio::test]
async fn create_api_requires_name_and_protocol() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);

    let req = make_request(Method::POST, "/v2/apis", r#"{"protocolType": "HTTP"}"#);
    assert!(svc.handle(req).await.is_err());

    let req = make_request(Method::POST, "/v2/apis", r#"{"name": "test"}"#);
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn create_api_unsupported_protocol() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);

    let body = serde_json::json!({"name": "grpc", "protocolType": "GRPC"});
    let req = make_request(Method::POST, "/v2/apis", &body.to_string());
    let err = expect_err(svc.handle(req).await);
    assert!(err.to_string().contains("BadRequestException"));
}

#[tokio::test]
async fn create_api_websocket_protocol_succeeds() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);

    let body = serde_json::json!({"name": "ws-api", "protocolType": "WEBSOCKET"});
    let req = make_request(Method::POST, "/v2/apis", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["name"], "ws-api");
    assert_eq!(b["protocolType"], "WEBSOCKET");
    // Default WS route selection expression follows AWS docs.
    assert_eq!(b["routeSelectionExpression"], "$request.body.action");
}

// ── Route CRUD ──

#[tokio::test]
async fn route_crud_lifecycle() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Create route
    let body = serde_json::json!({"routeKey": "GET /items"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    let route_id = b["routeId"].as_str().unwrap().to_string();
    assert_eq!(b["routeKey"], "GET /items");

    // Get route
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/routes/{route_id}"),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["routeKey"], "GET /items");

    // List routes
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/routes"), "");
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["items"].as_array().unwrap().len(), 1);

    // Update route
    let body = serde_json::json!({"routeKey": "POST /items"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/routes/{route_id}"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["routeKey"], "POST /items");

    // Delete route
    let req = make_request(
        Method::DELETE,
        &format!("/v2/apis/{api_id}/routes/{route_id}"),
        "",
    );
    svc.handle(req).await.unwrap();

    // Get should fail
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/routes/{route_id}"),
        "",
    );
    assert!(svc.handle(req).await.is_err());
}

// ── Integration CRUD ──

#[tokio::test]
async fn integration_crud_lifecycle() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Create integration
    let body = serde_json::json!({
        "integrationType": "HTTP_PROXY",
        "integrationUri": "https://example.com",
        "integrationMethod": "GET",
        "payloadFormatVersion": "1.0",
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    let int_id = b["integrationId"].as_str().unwrap().to_string();
    assert_eq!(b["integrationType"], "HTTP_PROXY");
    // AWS always reports connectionType, defaulting to INTERNET, which the
    // Terraform provider re-plans an integration without.
    assert_eq!(b["connectionType"], "INTERNET");

    // Get
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/integrations/{int_id}"),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["integrationUri"], "https://example.com");

    // List
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/integrations"), "");
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["items"].as_array().unwrap().len(), 1);

    // Update
    let body = serde_json::json!({"integrationUri": "https://updated.com"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/integrations/{int_id}"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["integrationUri"], "https://updated.com");

    // Delete
    let req = make_request(
        Method::DELETE,
        &format!("/v2/apis/{api_id}/integrations/{int_id}"),
        "",
    );
    svc.handle(req).await.unwrap();
}

// ── Stage CRUD ──

#[tokio::test]
async fn stage_crud_lifecycle() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Create stage
    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["stageName"], "prod");

    // Get stage
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/stages/prod"), "");
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["stageName"], "prod");

    // List stages
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/stages"), "");
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["items"].as_array().unwrap().len(), 1);

    // Update stage
    let body = serde_json::json!({"description": "production"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/stages/prod"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["description"], "production");

    // Delete stage
    let req = make_request(
        Method::DELETE,
        &format!("/v2/apis/{api_id}/stages/prod"),
        "",
    );
    svc.handle(req).await.unwrap();

    // Get should fail
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/stages/prod"), "");
    assert!(svc.handle(req).await.is_err());
}

// ── Deployment CRUD ──

#[tokio::test]
async fn deployment_crud() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Create deployment
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/deployments"),
        "{}",
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    let dep_id = b["deploymentId"].as_str().unwrap().to_string();
    assert!(dep_id.starts_with("deployment"));
    // Deployments apply synchronously, so they report DEPLOYED immediately —
    // the Terraform provider's create-waiter polls for this.
    assert_eq!(b["deploymentStatus"], "DEPLOYED");

    // Get deployment
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/deployments/{dep_id}"),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["deploymentId"], dep_id);

    // List deployments
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/deployments"), "");
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["items"].as_array().unwrap().len(), 1);
}

// ── Authorizer CRUD ──

#[tokio::test]
async fn authorizer_crud_lifecycle() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Create authorizer
    let body = serde_json::json!({
        "authorizerType": "JWT",
        "name": "jwt-auth",
        "identitySource": "$request.header.Authorization",
        "jwtConfiguration": {
            "issuer": "https://auth.example.com",
            "audience": ["my-api"],
        },
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    let auth_id = b["authorizerId"].as_str().unwrap().to_string();
    assert_eq!(b["authorizerType"], "JWT");
    assert_eq!(b["name"], "jwt-auth");

    // Get
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/authorizers/{auth_id}"),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["name"], "jwt-auth");

    // List
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/authorizers"), "");
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["items"].as_array().unwrap().len(), 1);

    // Update
    let body = serde_json::json!({"name": "updated-auth"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/authorizers/{auth_id}"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["name"], "updated-auth");

    // Delete
    let req = make_request(
        Method::DELETE,
        &format!("/v2/apis/{api_id}/authorizers/{auth_id}"),
        "",
    );
    svc.handle(req).await.unwrap();

    // Get should fail
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/authorizers/{auth_id}"),
        "",
    );
    assert!(svc.handle(req).await.is_err());
}

// ── API not found errors ──

#[tokio::test]
async fn operations_on_nonexistent_api() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);

    let req = make_request(Method::GET, "/v2/apis/nonexistent", "");
    assert!(svc.handle(req).await.is_err());

    let req = make_request(
        Method::POST,
        "/v2/apis/nonexistent/routes",
        r#"{"routeKey":"GET /"}"#,
    );
    assert!(svc.handle(req).await.is_err());

    let req = make_request(Method::GET, "/v2/apis/nonexistent/routes", "");
    assert!(svc.handle(req).await.is_err());
}

// ── CORS configuration ──

#[tokio::test]
async fn create_api_with_cors() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);

    let body = serde_json::json!({
        "name": "cors-api",
        "protocolType": "HTTP",
        "corsConfiguration": {
            "allowOrigins": ["https://example.com"],
            "allowMethods": ["GET", "POST"],
            "allowHeaders": ["Content-Type"],
        },
    });
    let req = make_request(Method::POST, "/v2/apis", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert!(b["corsConfiguration"].is_object());
}

// ── Unknown route ──

#[tokio::test]
async fn unknown_management_route() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);

    let req = make_request(Method::POST, "/v2/apis/a1/unknown-collection", "{}");
    assert!(svc.handle(req).await.is_err());
}

// ── generate_id format ──

#[test]
fn generate_id_format() {
    let id = generate_id("api");
    assert!(id.starts_with("api"));
    assert_eq!(id.len(), 13); // "api" + 10 hex chars
}

// ── execute_api coverage ──

#[tokio::test]
async fn execute_api_stage_not_found() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let req = make_request(Method::GET, "/prod/pets", "");
    let err = svc.handle(req).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn execute_api_mock_integration_returns_ok() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Create integration
    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create route
    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}")
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create stage
    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Execute
    let req = make_request(Method::GET, "/prod/pets", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
}

#[tokio::test]
async fn execute_api_no_route_matches_returns_not_found() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Create stage but no routes
    let body = serde_json::json!({"stageName": "dev"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/dev/nowhere", "");
    let err = svc.handle(req).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn execute_api_route_no_integration_fails() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Create route with no target
    let body = serde_json::json!({"routeKey": "GET /x"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create stage
    let body = serde_json::json!({"stageName": "st"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/st/x", "");
    let err = svc.handle(req).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn execute_api_empty_path_errors() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let req = make_request(Method::GET, "/", "");
    // No path segments -> NotFoundException
    let err = svc.handle(req).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn execute_api_unsupported_integration_type() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({"integrationType": "UNKNOWN_TYPE"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /x",
        "target": format!("integrations/{integration_id}")
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "unsup"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/unsup/x", "");
    let err = svc.handle(req).await;
    assert!(err.is_err());
}

// ── Update operations ──

#[tokio::test]
async fn update_api_updates_name_and_description() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({"name": "new-name", "description": "updated"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    assert_eq!(b["name"], "new-name");
    assert_eq!(b["description"], "updated");
}

#[tokio::test]
async fn update_route_updates_fields() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({"routeKey": "GET /a"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let route_id = body_json(&resp)["routeId"].as_str().unwrap().to_string();

    let body = serde_json::json!({"routeKey": "GET /b"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/routes/{route_id}"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(body_json(&resp)["routeKey"], "GET /b");
}

#[tokio::test]
async fn update_integration_updates_timeout() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let iid = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"timeoutInMillis": 5000});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/integrations/{iid}"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(body_json(&resp)["timeoutInMillis"], 5000);
}

#[tokio::test]
async fn update_stage_updates_description() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({"stageName": "st1"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"description": "updated"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/stages/st1"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(body_json(&resp)["description"], "updated");
}

#[tokio::test]
async fn update_authorizer_updates_name() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "authorizerType": "JWT",
        "name": "original",
        "identitySource": ["$request.header.Authorization"],
        "jwtConfiguration": {
            "audience": ["client-id"],
            "issuer": "https://example.com"
        }
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let aid = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"name": "renamed"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/authorizers/{aid}"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(body_json(&resp)["name"], "renamed");
}

// ── Error branches on update of nonexistent resources ──

#[tokio::test]
async fn update_nonexistent_api_errors() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let body = serde_json::json!({"name": "new"});
    let req = make_request(Method::PATCH, "/v2/apis/abc123", &body.to_string());
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn delete_nonexistent_integration_errors() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(
        Method::DELETE,
        &format!("/v2/apis/{api_id}/integrations/ghost"),
        "",
    );
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn delete_nonexistent_route_errors() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(
        Method::DELETE,
        &format!("/v2/apis/{api_id}/routes/ghost"),
        "",
    );
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn get_nonexistent_stage_errors() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/stages/ghost"), "");
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn create_deployment_for_nonexistent_api_errors() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let body = serde_json::json!({});
    let req = make_request(
        Method::POST,
        "/v2/apis/ghost/deployments",
        &body.to_string(),
    );
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn deployment_crud_lifecycle() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/deployments"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let deployment_id = body_json(&resp)["deploymentId"]
        .as_str()
        .unwrap()
        .to_string();

    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/deployments/{deployment_id}"),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(
        body_json(&resp)["deploymentId"].as_str().unwrap(),
        deployment_id
    );

    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/deployments"), "");
    let resp = svc.handle(req).await.unwrap();
    let body = body_json(&resp);
    assert!(body["items"].is_array());
}

#[tokio::test]
async fn get_nonexistent_deployment_errors() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/deployments/ghost"),
        "",
    );
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn delete_api_removes_it() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let req = make_request(Method::DELETE, &format!("/v2/apis/{api_id}"), "");
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}"), "");
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn delete_authorizer_not_found() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(
        Method::DELETE,
        &format!("/v2/apis/{api_id}/authorizers/ghost"),
        "",
    );
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn list_authorizers_empty() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/authorizers"), "");
    let resp = svc.handle(req).await.unwrap();
    let body = body_json(&resp);
    assert!(body["items"].is_array());
}

#[tokio::test]
async fn list_stages_empty() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/stages"), "");
    let resp = svc.handle(req).await.unwrap();
    let body = body_json(&resp);
    assert!(body["items"].is_array());
}

#[tokio::test]
async fn get_apis_lists_created() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    create_api(&svc);
    create_api(&svc);
    let req = make_request(Method::GET, "/v2/apis", "");
    let resp = svc.handle(req).await.unwrap();
    let body = body_json(&resp);
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn delete_stage_removes() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let body = serde_json::json!({"stageName": "todel"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::DELETE,
        &format!("/v2/apis/{api_id}/stages/todel"),
        "",
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/stages/todel"), "");
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn get_integrations_empty() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/integrations"), "");
    let resp = svc.handle(req).await.unwrap();
    let body = body_json(&resp);
    assert!(body["items"].is_array());
}

#[tokio::test]
async fn get_routes_empty() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/routes"), "");
    let resp = svc.handle(req).await.unwrap();
    let body = body_json(&resp);
    assert!(body["items"].is_array());
}

#[tokio::test]
async fn get_authorizer_not_found() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/authorizers/ghost"),
        "",
    );
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn get_integration_not_found() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/integrations/ghost"),
        "",
    );
    assert!(svc.handle(req).await.is_err());
}

#[tokio::test]
async fn get_route_not_found() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/routes/ghost"), "");
    assert!(svc.handle(req).await.is_err());
}

// ── JWT authorizer enforcement ──

#[test]
fn extract_identity_source_value_header() {
    let mut req = make_request(Method::GET, "/prod/pets", "");
    req.headers
        .insert("Authorization", "Bearer tok".parse().unwrap());
    assert_eq!(
        super::extract_identity_source_value(&req, "$request.header.Authorization"),
        Some("Bearer tok".to_string())
    );
}

#[test]
fn extract_identity_source_value_querystring() {
    let mut req = make_request(Method::GET, "/prod/pets", "");
    req.query_params
        .insert("token".to_string(), "abc".to_string());
    assert_eq!(
        super::extract_identity_source_value(&req, "$request.querystring.token"),
        Some("abc".to_string())
    );
}

#[test]
fn extract_identity_source_value_unknown_prefix() {
    let req = make_request(Method::GET, "/prod/pets", "");
    assert_eq!(
        super::extract_identity_source_value(&req, "$request.body.action"),
        None
    );
}

#[test]
fn issuer_to_pool_arn_parses_cognito_url() {
    let arn = super::issuer_to_pool_arn(
        "123456789012",
        "us-east-1",
        "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc123",
    );
    assert_eq!(
        arn,
        Some("arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_abc123".to_string())
    );
}

#[test]
fn issuer_to_pool_arn_bad_format_returns_none() {
    assert_eq!(
        super::issuer_to_pool_arn("123456789012", "us-east-1", "not-a-url"),
        None
    );
}

#[tokio::test]
async fn execute_api_jwt_authorizer_no_token_returns_401() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Create JWT authorizer
    let body = serde_json::json!({
        "name": "jwt-auth",
        "authorizerType": "JWT",
        "identitySource": ["$request.header.Authorization"],
        "jwtConfiguration": {
            "audience": ["my-client-id"],
            "issuer": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc123"
        }
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create integration
    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create route with authorizer
    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create stage
    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Execute without token -> 401
    let req = make_request(Method::GET, "/prod/pets", "");
    let err = expect_err(svc.handle(req).await);
    assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn execute_api_jwt_authorizer_no_delivery_returns_500() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Create JWT authorizer
    let body = serde_json::json!({
        "name": "jwt-auth",
        "authorizerType": "JWT",
        "identitySource": ["$request.header.Authorization"],
        "jwtConfiguration": {
            "audience": ["my-client-id"],
            "issuer": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc123"
        }
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create integration
    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create route with authorizer
    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create stage
    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Execute with token but no delivery -> 500
    let mut req = make_request(Method::GET, "/prod/pets", "");
    req.headers
        .insert("Authorization", "Bearer dummy".parse().unwrap());
    let err = expect_err(svc.handle(req).await);
    assert_eq!(err.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ─── Lambda authorizer tests ───────────────────────────────────────

struct MockLambdaInvoker {
    response: Result<Vec<u8>, String>,
}

impl fakecloud_core::delivery::LambdaDelivery for MockLambdaInvoker {
    fn invoke_lambda(
        &self,
        _function_arn: &str,
        _payload: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>> {
        let res = self.response.clone();
        Box::pin(async move { res })
    }
}

fn make_svc_with_lambda(response: Result<Vec<u8>, String>) -> ApiGatewayV2Service {
    let state = make_state();
    let delivery = Arc::new(
        fakecloud_core::delivery::DeliveryBus::new()
            .with_lambda(Arc::new(MockLambdaInvoker { response })),
    );
    ApiGatewayV2Service::new(state).with_delivery(delivery)
}

#[tokio::test]
async fn execute_api_lambda_authorizer_simple_response_allow() {
    let svc = make_svc_with_lambda(Ok(serde_json::json!({
        "isAuthorized": true,
        "context": {"role": "admin"}
    })
    .to_string()
    .into_bytes()));
    let api_id = create_api(&svc);

    // Create REQUEST authorizer
    let body = serde_json::json!({
        "name": "lambda-auth",
        "authorizerType": "REQUEST",
        "identitySource": ["$request.header.Authorization"],
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:authorizer/invocations"
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create MOCK integration
    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create route with authorizer
    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create stage
    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Execute with token -> 200 (authorizer allows)
    let mut req = make_request(Method::GET, "/prod/pets", "");
    req.headers
        .insert("Authorization", "Bearer tok".parse().unwrap());
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn execute_api_lambda_authorizer_simple_response_deny() {
    let svc = make_svc_with_lambda(Ok(serde_json::json!({"isAuthorized": false})
        .to_string()
        .into_bytes()));
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "name": "lambda-auth",
        "authorizerType": "REQUEST",
        "identitySource": ["$request.header.Authorization"],
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:authorizer/invocations"
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let mut req = make_request(Method::GET, "/prod/pets", "");
    req.headers
        .insert("Authorization", "Bearer tok".parse().unwrap());
    let err = expect_err(svc.handle(req).await);
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn execute_api_lambda_authorizer_policy_allow() {
    let svc = make_svc_with_lambda(Ok(serde_json::json!({
        "principalId": "user-1",
        "policyDocument": {
            "Statement": [{"Effect": "Allow", "Resource": "*"}]
        },
        "context": {"role": "admin"}
    })
    .to_string()
    .into_bytes()));
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "name": "lambda-auth",
        "authorizerType": "REQUEST",
        "identitySource": ["$request.header.Authorization"],
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:authorizer/invocations"
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let mut req = make_request(Method::GET, "/prod/pets", "");
    req.headers
        .insert("Authorization", "Bearer tok".parse().unwrap());
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn execute_api_lambda_authorizer_policy_deny() {
    let svc = make_svc_with_lambda(Ok(serde_json::json!({
        "principalId": "user-1",
        "policyDocument": {
            "Statement": [{"Effect": "Deny", "Resource": "*"}]
        }
    })
    .to_string()
    .into_bytes()));
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "name": "lambda-auth",
        "authorizerType": "REQUEST",
        "identitySource": ["$request.header.Authorization"],
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:authorizer/invocations"
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let mut req = make_request(Method::GET, "/prod/pets", "");
    req.headers
        .insert("Authorization", "Bearer tok".parse().unwrap());
    let err = expect_err(svc.handle(req).await);
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn execute_api_lambda_authorizer_no_identity_source_returns_401() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "name": "lambda-auth",
        "authorizerType": "REQUEST",
        "identitySource": ["$request.header.Authorization"],
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:authorizer/invocations"
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // No Authorization header -> 401
    let req = make_request(Method::GET, "/prod/pets", "");
    let err = expect_err(svc.handle(req).await);
    assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn execute_api_lambda_authorizer_optional_identity_source() {
    // REQUEST authorizer with no identitySource configured still invokes Lambda.
    let svc = make_svc_with_lambda(Ok(serde_json::json!({
        "isAuthorized": true,
        "context": {"role": "admin"}
    })
    .to_string()
    .into_bytes()));
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "name": "lambda-auth",
        "authorizerType": "REQUEST",
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:authorizer/invocations"
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // No identity sources configured -> request proceeds to Lambda and is allowed.
    let req = make_request(Method::GET, "/prod/pets", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn execute_api_lambda_authorizer_partial_identity_source_returns_401() {
    // Two identity sources configured, only one present -> 401.
    let svc = make_svc_with_lambda(Ok(serde_json::json!({"isAuthorized": true})
        .to_string()
        .into_bytes()));
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "name": "lambda-auth",
        "authorizerType": "REQUEST",
        "identitySource": ["$request.header.Authorization", "$request.header.X-Custom"],
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:authorizer/invocations"
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Only Authorization header present, missing X-Custom -> 401.
    let mut req = make_request(Method::GET, "/prod/pets", "");
    req.headers
        .insert("Authorization", "Bearer tok".parse().unwrap());
    let err = expect_err(svc.handle(req).await);
    assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn execute_api_lambda_authorizer_empty_identity_source_returns_401() {
    // Identity source header present but empty -> 401.
    let svc = make_svc_with_lambda(Ok(serde_json::json!({"isAuthorized": true})
        .to_string()
        .into_bytes()));
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "name": "lambda-auth",
        "authorizerType": "REQUEST",
        "identitySource": ["$request.header.Authorization"],
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:authorizer/invocations"
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Authorization header present but empty -> 401.
    let mut req = make_request(Method::GET, "/prod/pets", "");
    req.headers.insert("Authorization", "".parse().unwrap());
    let err = expect_err(svc.handle(req).await);
    assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn execute_api_lambda_authorizer_malformed_policy_no_resource() {
    // Policy statement missing Resource field should default to Deny.
    let svc = make_svc_with_lambda(Ok(serde_json::json!({
        "principalId": "user-1",
        "policyDocument": {
            "Statement": [{"Effect": "Allow"}]
        }
    })
    .to_string()
    .into_bytes()));
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "name": "lambda-auth",
        "authorizerType": "REQUEST",
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:authorizer/invocations"
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let auth_id = body_json(&resp)["authorizerId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
        "authorizerId": auth_id
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/prod/pets", "");
    let err = expect_err(svc.handle(req).await);
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
}

#[test]
fn build_method_arn_includes_method_and_no_double_slash() {
    let req = make_request(Method::GET, "/prod/pets", "");
    let arn = build_method_arn(&req, "abc123", "prod");
    assert_eq!(
        arn,
        "arn:aws:execute-api:us-east-1:123456789012:abc123/prod/GET/pets"
    );

    // Empty path after stage should not produce trailing slash.
    let req2 = make_request(Method::GET, "/prod", "");
    let arn2 = build_method_arn(&req2, "abc123", "prod");
    assert_eq!(
        arn2,
        "arn:aws:execute-api:us-east-1:123456789012:abc123/prod/GET/"
    );
}

// ── stage variables ──

#[tokio::test]
async fn execute_api_stage_variable_substitution_in_lambda_arn() {
    let svc = make_svc_with_lambda(Ok(serde_json::json!({"statusCode": 200})
        .to_string()
        .into_bytes()));
    let api_id = create_api(&svc);

    // Create integration with stage variable placeholder
    let body = serde_json::json!({
        "integrationType": "AWS_PROXY",
        "integrationUri": "arn:aws:lambda:us-east-1:123456789012:function:my-fn:${stageVariables.env}",
        "payloadFormatVersion": "2.0",
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create route
    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create stage with stage variable
    let body = serde_json::json!({
        "stageName": "prod",
        "stageVariables": {"env": "prod"},
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Execute - should succeed because stage variable substitutes to a valid Lambda ARN
    let req = make_request(Method::GET, "/prod/pets", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn execute_api_stage_variable_passthrough_when_key_missing() {
    let svc = make_svc_with_lambda(Ok(serde_json::json!({"statusCode": 200})
        .to_string()
        .into_bytes()));
    let api_id = create_api(&svc);

    // Create integration with stage variable placeholder but no matching stage variable
    let body = serde_json::json!({
        "integrationType": "AWS_PROXY",
        "integrationUri": "arn:aws:lambda:us-east-1:123456789012:function:my-fn:${stageVariables.env}",
        "payloadFormatVersion": "2.0",
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Stage without the required stage variable
    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Execute - placeholder remains in URI but Lambda ARN check still matches,
    // so mock lambda is invoked and request succeeds.
    let req = make_request(Method::GET, "/prod/pets", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

// ── custom domain + api mapping ──

#[tokio::test]
async fn execute_api_custom_domain_base_path_mapping() {
    let state = make_state();
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let svc = ApiGatewayV2Service::new(state).with_delivery(delivery);
    let api_id = create_api(&svc);

    // Create MOCK integration
    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create route GET /pets
    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create stage
    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create custom domain
    let body = serde_json::json!({"domainName": "example.com"});
    let req = make_request(Method::POST, "/v2/domainnames", &body.to_string());
    svc.handle(req).await.unwrap();

    // Create api mapping with base path "v1"
    let body = serde_json::json!({"apiId": api_id, "stage": "prod", "apiMappingKey": "v1"});
    let req = make_request(
        Method::POST,
        "/v2/domainnames/example.com/apimappings",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Execute with custom domain host and base path - should strip "/v1" and match route
    let mut req = make_request(Method::GET, "/v1/pets", "");
    req.headers.insert("host", "example.com".parse().unwrap());
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn execute_api_custom_domain_empty_base_path() {
    let state = make_state();
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let svc = ApiGatewayV2Service::new(state).with_delivery(delivery);
    let api_id = create_api(&svc);

    // Create MOCK integration
    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    // Create route GET /pets
    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create stage
    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Create custom domain
    let body = serde_json::json!({"domainName": "example.com"});
    let req = make_request(Method::POST, "/v2/domainnames", &body.to_string());
    svc.handle(req).await.unwrap();

    // Create api mapping with empty base path
    let body = serde_json::json!({"apiId": api_id, "stage": "prod"});
    let req = make_request(
        Method::POST,
        "/v2/domainnames/example.com/apimappings",
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // Execute with custom domain host - full path used for route matching
    let mut req = make_request(Method::GET, "/pets", "");
    req.headers.insert("host", "example.com".parse().unwrap());
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

// ── AWS service integrations ──

#[tokio::test]
async fn execute_api_aws_proxy_sqs_integration() {
    let state = make_state();
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let svc = ApiGatewayV2Service::new(state).with_delivery(delivery);
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "integrationType": "AWS_PROXY",
        "integrationUri": "arn:aws:sqs:us-east-1:123456789012:my-queue",
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "POST /messages",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::POST, "/prod/messages", "hello sqs");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let b = body_json(&resp);
    assert_eq!(b["statusCode"], 200);
    // MessageId is present in the inner JSON body string
    let inner: serde_json::Value = serde_json::from_str(b["body"].as_str().unwrap()).unwrap();
    assert!(!inner["MessageId"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn execute_api_aws_proxy_sns_integration() {
    let state = make_state();
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let svc = ApiGatewayV2Service::new(state).with_delivery(delivery);
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "integrationType": "AWS_PROXY",
        "integrationUri": "arn:aws:sns:us-east-1:123456789012:my-topic",
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "POST /notify",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::POST, "/prod/notify", "hello sns");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let b = body_json(&resp);
    assert_eq!(b["statusCode"], 200);
    let inner: serde_json::Value = serde_json::from_str(b["body"].as_str().unwrap()).unwrap();
    assert!(!inner["MessageId"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn execute_api_aws_proxy_stepfunctions_integration() {
    let state = make_state();
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let svc = ApiGatewayV2Service::new(state).with_delivery(delivery);
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "integrationType": "AWS_PROXY",
        "integrationUri": "arn:aws:states:us-east-1:123456789012:stateMachine:my-sm",
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "POST /workflow",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::POST, "/prod/workflow", "{\"name\":\"test\"}");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let b = body_json(&resp);
    assert_eq!(b["statusCode"], 200);
    let inner: serde_json::Value = serde_json::from_str(b["body"].as_str().unwrap()).unwrap();
    assert!(inner["executionArn"]
        .as_str()
        .unwrap()
        .contains("stateMachine:my-sm"));
    assert!(inner["startDate"].as_str().is_some());
}

#[tokio::test]
async fn execute_api_aws_proxy_unsupported_arn_returns_not_implemented() {
    let state = make_state();
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let svc = ApiGatewayV2Service::new(state).with_delivery(delivery);
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "integrationType": "AWS_PROXY",
        "integrationUri": "arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /items",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/prod/items", "");
    let err = match svc.handle(req).await {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("NotImplemented"),
        "expected NotImplemented but got: {}",
        msg
    );
}

// ─── Access log tests ────────────────────────────────────────────────

#[derive(Default)]
#[allow(clippy::type_complexity)]
struct MockCloudwatchLogsDelivery {
    events: parking_lot::Mutex<Vec<(String, String, String, Vec<(i64, String)>)>>,
}

impl fakecloud_core::delivery::CloudwatchLogsDelivery for MockCloudwatchLogsDelivery {
    fn put_log_events(
        &self,
        account_id: &str,
        log_group_name: &str,
        log_stream_name: &str,
        events: &[(i64, String)],
    ) {
        self.events.lock().push((
            account_id.to_string(),
            log_group_name.to_string(),
            log_stream_name.to_string(),
            events.to_vec(),
        ));
    }
}

fn make_svc_with_access_logs(mock: Arc<MockCloudwatchLogsDelivery>) -> ApiGatewayV2Service {
    let state = make_state();
    let delivery =
        Arc::new(fakecloud_core::delivery::DeliveryBus::new().with_cloudwatch_logs(mock));
    ApiGatewayV2Service::new(state).with_delivery(delivery)
}

#[tokio::test]
async fn execute_api_emits_access_log_when_configured() {
    let mock = Arc::new(MockCloudwatchLogsDelivery::default());
    let svc = make_svc_with_access_logs(mock.clone());
    let api_id = create_api(&svc);

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({
        "stageName": "prod",
        "accessLogSettings": {
            "destinationArn": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/apigateway/access-logs",
        },
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/prod/pets", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let events = mock.events.lock();
    assert_eq!(events.len(), 1, "expected exactly one access log event");
    let (account_id, log_group, log_stream, log_events) = &events[0];
    assert_eq!(account_id, "123456789012");
    assert_eq!(log_group, "/aws/apigateway/access-logs");
    assert_eq!(log_stream, &format!("{api_id}/prod"));
    assert_eq!(log_events.len(), 1);
    let log_line = &log_events[0].1;
    assert!(log_line.contains("\"requestId\":\"test-id\""));
    assert!(log_line.contains("\"httpMethod\":\"GET\""));
    assert!(log_line.contains("\"status\":\"200\""));
}

#[tokio::test]
async fn execute_api_no_access_log_when_not_configured() {
    let mock = Arc::new(MockCloudwatchLogsDelivery::default());
    let svc = make_svc_with_access_logs(mock.clone());
    let api_id = create_api(&svc);

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let body = serde_json::json!({"stageName": "prod"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/prod/pets", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let events = mock.events.lock();
    assert!(events.is_empty(), "expected no access log events");
}

#[tokio::test]
async fn execute_api_access_log_honors_custom_format() {
    let mock = Arc::new(MockCloudwatchLogsDelivery::default());
    let svc = make_svc_with_access_logs(mock.clone());
    let api_id = create_api(&svc);

    let body = serde_json::json!({"integrationType": "MOCK"});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/integrations"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let integration_id = body_json(&resp)["integrationId"]
        .as_str()
        .unwrap()
        .to_string();

    let body = serde_json::json!({
        "routeKey": "GET /pets",
        "target": format!("integrations/{integration_id}"),
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let custom_format = "$context.apiId $context.stage $context.status";
    let body = serde_json::json!({
        "stageName": "prod",
        "accessLogSettings": {
            "destinationArn": "arn:aws:logs:us-east-1:123456789012:log-group:my-logs:*",
            "format": custom_format,
        },
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/prod/pets", "");
    svc.handle(req).await.unwrap();

    let events = mock.events.lock();
    assert_eq!(events.len(), 1);
    let log_line = &events[0].3[0].1;
    let expected = format!("{api_id} prod 200");
    assert_eq!(log_line, &expected);
}

#[tokio::test]
async fn execute_api_access_log_emitted_on_error_paths() {
    let mock = Arc::new(MockCloudwatchLogsDelivery::default());
    let svc = make_svc_with_access_logs(mock.clone());
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "stageName": "prod",
        "accessLogSettings": {
            "destinationArn": "arn:aws:logs:us-east-1:123456789012:log-group:err-logs",
        },
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    // No route configured - should 404 and still log
    let req = make_request(Method::GET, "/prod/unknown", "");
    let err = match svc.handle(req).await {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    assert_eq!(err.status(), StatusCode::NOT_FOUND);

    let events = mock.events.lock();
    assert_eq!(events.len(), 1);
    let log_line = &events[0].3[0].1;
    assert!(log_line.contains("\"status\":\"404\""));
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    assert!(svc.snapshot_hook().is_none());
}

/// With a snapshot store set, the hook is present and runs to completion.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state).with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    hook().await;
}

#[tokio::test]
async fn create_domain_name_reports_available_status() {
    // AWS provisions custom domains synchronously, so each configuration comes
    // back AVAILABLE with a regional endpoint and ipv4 — the Terraform
    // provider's create-waiter polls GetDomainName for DomainNameStatus.
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let body = serde_json::json!({ "domainName": "tfacc.example.com" });
    let req = make_request(Method::POST, "/v2/domainnames", &body.to_string());
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    let cfg0 = &b["domainNameConfigurations"][0];
    assert_eq!(cfg0["domainNameStatus"], "AVAILABLE");
    assert_eq!(cfg0["ipAddressType"], "ipv4");
    assert!(cfg0["apiGatewayDomainName"].as_str().is_some());
}

// ── Batch 6: field-drop + stub fixes ──

/// Route Create/Update round-trips the previously dropped optional members.
#[tokio::test]
async fn route_persists_extended_fields() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "routeKey": "GET /items",
        "apiKeyRequired": true,
        "authorizationScopes": ["scope.a", "scope.b"],
        "modelSelectionExpression": "$request.body.action",
        "operationName": "ListItems",
        "requestModels": {"application/json": "MyModel"},
        "requestParameters": {"route.request.header.x": {"required": true}},
        "routeResponseSelectionExpression": "$default",
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/routes"),
        &body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    let b = body_json(&resp);
    let route_id = b["routeId"].as_str().unwrap().to_string();
    assert_eq!(b["apiKeyRequired"], true);

    // Read back via GetRoute — the fields must survive the store.
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/routes/{route_id}"),
        "",
    );
    let b = body_json(&svc.handle(req).await.unwrap());
    assert_eq!(b["apiKeyRequired"], true);
    assert_eq!(b["authorizationScopes"][1], "scope.b");
    assert_eq!(b["modelSelectionExpression"], "$request.body.action");
    assert_eq!(b["operationName"], "ListItems");
    assert_eq!(b["requestModels"]["application/json"], "MyModel");
    assert_eq!(
        b["requestParameters"]["route.request.header.x"]["required"],
        true
    );
    assert_eq!(b["routeResponseSelectionExpression"], "$default");

    // Update one field; the rest must stay.
    let body = serde_json::json!({"operationName": "Renamed"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/routes/{route_id}"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/routes/{route_id}"),
        "",
    );
    let b = body_json(&svc.handle(req).await.unwrap());
    assert_eq!(b["operationName"], "Renamed");
    assert_eq!(b["apiKeyRequired"], true);
}

/// Stage Create/Update round-trips ClientCertificateId / DefaultRouteSettings /
/// RouteSettings / Tags.
#[tokio::test]
async fn stage_persists_extended_fields() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "stageName": "prod",
        "clientCertificateId": "cert-123",
        "defaultRouteSettings": {"throttlingBurstLimit": 100, "throttlingRateLimit": 50.0},
        "routeSettings": {"GET /items": {"throttlingBurstLimit": 5}},
        "tags": {"team": "platform"},
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/stages/prod"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    assert_eq!(b["clientCertificateId"], "cert-123");
    assert_eq!(b["defaultRouteSettings"]["throttlingBurstLimit"], 100);
    assert_eq!(b["routeSettings"]["GET /items"]["throttlingBurstLimit"], 5);
    assert_eq!(b["tags"]["team"], "platform");

    // Update tags + client cert.
    let body = serde_json::json!({
        "clientCertificateId": "cert-999",
        "tags": {"env": "staging"},
    });
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/stages/prod"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/stages/prod"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    assert_eq!(b["clientCertificateId"], "cert-999");
    assert_eq!(b["tags"]["env"], "staging");
    // Untouched field preserved.
    assert_eq!(b["defaultRouteSettings"]["throttlingBurstLimit"], 100);
}

/// Authorizer Create/Update round-trips AuthorizerCredentialsArn /
/// IdentityValidationExpression.
#[tokio::test]
async fn authorizer_persists_extended_fields() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "name": "my-auth",
        "authorizerType": "REQUEST",
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:auth/invocations",
        "identitySource": ["$request.header.Authorization"],
        "authorizerCredentialsArn": "arn:aws:iam::123456789012:role/invoke",
        "identityValidationExpression": "^Bearer .+$",
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/authorizers"),
        &body.to_string(),
    );
    let b = body_json(&svc.handle(req).await.unwrap());
    let auth_id = b["authorizerId"].as_str().unwrap().to_string();

    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/authorizers/{auth_id}"),
        "",
    );
    let b = body_json(&svc.handle(req).await.unwrap());
    assert_eq!(
        b["authorizerCredentialsArn"],
        "arn:aws:iam::123456789012:role/invoke"
    );
    assert_eq!(b["identityValidationExpression"], "^Bearer .+$");

    let body = serde_json::json!({"identityValidationExpression": "^Token .+$"});
    let req = make_request(
        Method::PATCH,
        &format!("/v2/apis/{api_id}/authorizers/{auth_id}"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();
    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/authorizers/{auth_id}"),
        "",
    );
    let b = body_json(&svc.handle(req).await.unwrap());
    assert_eq!(b["identityValidationExpression"], "^Token .+$");
    assert_eq!(
        b["authorizerCredentialsArn"],
        "arn:aws:iam::123456789012:role/invoke"
    );
}

fn petstore_spec() -> String {
    serde_json::json!({
        "openapi": "3.0.1",
        "info": {"title": "petstore", "version": "2.5", "description": "pets"},
        "paths": {
            "/pets": {
                "get": {"responses": {"200": {"description": "ok"}}},
                "post": {"responses": {"200": {"description": "ok"}}}
            },
            "/pets/{id}": {
                "get": {
                    "x-amazon-apigateway-integration": {
                        "type": "http_proxy",
                        "uri": "https://backend.example.com/pets",
                        "httpMethod": "GET",
                        "payloadFormatVersion": "1.0"
                    },
                    "responses": {"200": {"description": "ok"}}
                }
            }
        }
    })
    .to_string()
}

/// ImportApi parses the spec, creates a real API, and synthesizes routes +
/// integrations that GetApi/GetRoutes read back.
#[tokio::test]
async fn import_api_creates_real_api_and_routes() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);

    let body = serde_json::json!({"body": petstore_spec()});
    let req = make_request(Method::PUT, "/v2/apis", &body.to_string());
    let b = body_json(&svc.handle(req).await.unwrap());
    let api_id = b["apiId"].as_str().unwrap().to_string();
    assert_eq!(b["name"], "petstore");
    assert_eq!(b["version"], "2.5");
    assert_eq!(b["protocolType"], "HTTP");

    // GetApi must find it (not a discarded constant).
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    assert_eq!(b["name"], "petstore");

    // Three routes: GET/POST /pets and GET /pets/{id}.
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/routes"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    let keys: Vec<String> = b["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["routeKey"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"GET /pets".to_string()));
    assert!(keys.contains(&"POST /pets".to_string()));
    assert!(keys.contains(&"GET /pets/{id}".to_string()));

    // The integration on GET /pets/{id} is wired as the route target.
    let integrated = b["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["routeKey"] == "GET /pets/{id}")
        .unwrap();
    assert!(integrated["target"]
        .as_str()
        .unwrap()
        .starts_with("integrations/"));

    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/integrations"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    assert_eq!(b["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        b["items"][0]["integrationUri"],
        "https://backend.example.com/pets"
    );
}

/// ImportApi accepts a YAML document too.
#[tokio::test]
async fn import_api_accepts_yaml() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let yaml = "openapi: 3.0.1\ninfo:\n  title: yamlapi\n  version: '1.0'\npaths:\n  /ping:\n    get:\n      responses:\n        '200':\n          description: ok\n";
    let body = serde_json::json!({"body": yaml});
    let req = make_request(Method::PUT, "/v2/apis", &body.to_string());
    let b = body_json(&svc.handle(req).await.unwrap());
    let api_id = b["apiId"].as_str().unwrap().to_string();
    assert_eq!(b["name"], "yamlapi");
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/routes"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    assert_eq!(b["items"][0]["routeKey"], "GET /ping");
}

/// ReimportApi replaces the routes of an existing API and 404s on unknown ids.
#[tokio::test]
async fn reimport_api_replaces_routes() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);

    // Seed via import.
    let body = serde_json::json!({"body": petstore_spec()});
    let req = make_request(Method::PUT, "/v2/apis", &body.to_string());
    let api_id = body_json(&svc.handle(req).await.unwrap())["apiId"]
        .as_str()
        .unwrap()
        .to_string();

    // Reimport a smaller spec.
    let spec2 = serde_json::json!({
        "openapi": "3.0.1",
        "info": {"title": "petstore-v2", "version": "3.0"},
        "paths": {"/health": {"get": {"responses": {"200": {"description": "ok"}}}}}
    })
    .to_string();
    let body = serde_json::json!({"body": spec2});
    let req = make_request(
        Method::PUT,
        &format!("/v2/apis/{api_id}"),
        &body.to_string(),
    );
    let b = body_json(&svc.handle(req).await.unwrap());
    assert_eq!(b["name"], "petstore-v2");
    assert_eq!(b["version"], "3.0");

    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/routes"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    let items = b["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["routeKey"], "GET /health");

    // Unknown API id 404s.
    let body = serde_json::json!({"body": petstore_spec()});
    let req = make_request(Method::PUT, "/v2/apis/does-not-exist", &body.to_string());
    assert!(svc.handle(req).await.is_err());
}

/// ExportApi emits a real OpenAPI document derived from the API's routes.
#[tokio::test]
async fn export_api_generates_openapi_from_routes() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let body = serde_json::json!({"body": petstore_spec()});
    let req = make_request(Method::PUT, "/v2/apis", &body.to_string());
    let api_id = body_json(&svc.handle(req).await.unwrap())["apiId"]
        .as_str()
        .unwrap()
        .to_string();

    let mut req = make_request(Method::GET, &format!("/v2/apis/{api_id}/exports/OAS30"), "");
    req.query_params
        .insert("outputType".to_string(), "JSON".to_string());
    let b = body_json(&svc.handle(req).await.unwrap());
    let doc: Value = serde_json::from_str(b["body"].as_str().unwrap()).unwrap();
    assert_eq!(doc["openapi"], "3.0.1");
    assert_eq!(doc["info"]["title"], "petstore");
    assert!(doc["paths"]["/pets"].get("get").is_some());
    assert!(doc["paths"]["/pets"].get("post").is_some());
    assert!(doc["paths"]["/pets/{id}"].get("get").is_some());
}

/// ExportApi 404s on an unknown API and requires OutputType.
#[tokio::test]
async fn export_api_validates() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    // Missing OutputType.
    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/exports/OAS30"), "");
    assert!(svc.handle(req).await.is_err());

    // Unknown API.
    let mut req = make_request(Method::GET, "/v2/apis/nope/exports/OAS30", "");
    req.query_params
        .insert("outputType".to_string(), "JSON".to_string());
    assert!(svc.handle(req).await.is_err());
}

/// DeleteCorsConfiguration clears the stored CORS config and persists it.
#[tokio::test]
async fn delete_cors_configuration_clears_config() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let body = serde_json::json!({
        "name": "cors-api",
        "protocolType": "HTTP",
        "corsConfiguration": {"allowOrigins": ["*"], "allowMethods": ["GET"]},
    });
    let req = make_request(Method::POST, "/v2/apis", &body.to_string());
    let api_id = body_json(&svc.create_api(&req).unwrap())["apiId"]
        .as_str()
        .unwrap()
        .to_string();

    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    assert!(b.get("corsConfiguration").is_some());

    let req = make_request(Method::DELETE, &format!("/v2/apis/{api_id}/cors"), "");
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    assert!(b.get("corsConfiguration").is_none());

    // Unknown API 404s.
    let req = make_request(Method::DELETE, "/v2/apis/missing/cors", "");
    assert!(svc.handle(req).await.is_err());
}

/// DeleteAccessLogSettings clears the stage's access log settings.
#[tokio::test]
async fn delete_access_log_settings_clears_settings() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let body = serde_json::json!({
        "stageName": "prod",
        "accessLogSettings": {
            "destinationArn": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/apigw:*",
            "format": "$context.requestId"
        }
    });
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/stages"),
        &body.to_string(),
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/stages/prod"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    assert!(b.get("accessLogSettings").is_some());

    let req = make_request(
        Method::DELETE,
        &format!("/v2/apis/{api_id}/stages/prod/accesslogsettings"),
        "",
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, &format!("/v2/apis/{api_id}/stages/prod"), "");
    let b = body_json(&svc.handle(req).await.unwrap());
    assert!(b.get("accessLogSettings").is_none());
}

/// GetModelTemplate derives a template from the model schema instead of a
/// fixed constant.
#[tokio::test]
async fn get_model_template_derives_from_schema() {
    let state = make_state();
    let svc = ApiGatewayV2Service::new(state);
    let api_id = create_api(&svc);

    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "count": {"type": "integer"},
            "active": {"type": "boolean"}
        }
    })
    .to_string();
    let body = serde_json::json!({"name": "Widget", "schema": schema});
    let req = make_request(
        Method::POST,
        &format!("/v2/apis/{api_id}/models"),
        &body.to_string(),
    );
    let b = body_json(&svc.handle(req).await.unwrap());
    let model_id = b["modelId"].as_str().unwrap().to_string();

    let req = make_request(
        Method::GET,
        &format!("/v2/apis/{api_id}/models/{model_id}/template"),
        "",
    );
    let b = body_json(&svc.handle(req).await.unwrap());
    // Not the old {"value":"{}"} constant — reflects the schema shape.
    let tmpl: Value = serde_json::from_str(b["value"].as_str().unwrap()).unwrap();
    assert_eq!(tmpl["id"], "");
    assert_eq!(tmpl["count"], 0);
    assert_eq!(tmpl["active"], false);
}
