use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{Deployment, HttpApi, Integration, Route, SharedApiGatewayV2State, Stage};
use crate::{cors, http_proxy, lambda_proxy, mock, router::Router};

const SUPPORTED: &[&str] = &[
    "CreateApi",
    "GetApi",
    "GetApis",
    "UpdateApi",
    "DeleteApi",
    "CreateRoute",
    "GetRoute",
    "GetRoutes",
    "UpdateRoute",
    "DeleteRoute",
    "CreateIntegration",
    "GetIntegration",
    "GetIntegrations",
    "UpdateIntegration",
    "DeleteIntegration",
    "CreateStage",
    "GetStage",
    "GetStages",
    "UpdateStage",
    "DeleteStage",
    "CreateDeployment",
    "GetDeployment",
    "GetDeployments",
];

pub struct ApiGatewayV2Service {
    state: SharedApiGatewayV2State,
    delivery: Option<Arc<DeliveryBus>>,
}

impl ApiGatewayV2Service {
    pub fn new(state: SharedApiGatewayV2State) -> Self {
        Self {
            state,
            delivery: None,
        }
    }

    pub fn with_delivery(mut self, delivery: Arc<DeliveryBus>) -> Self {
        self.delivery = Some(delivery);
        self
    }

    /// Determine the action from the HTTP method and path segments.
    /// API Gateway v2 uses REST-style routing:
    ///   POST   /v2/apis              -> CreateApi
    ///   GET    /v2/apis              -> GetApis
    ///   GET    /v2/apis/{api-id}     -> GetApi
    ///   PATCH  /v2/apis/{api-id}     -> UpdateApi
    ///   DELETE /v2/apis/{api-id}     -> DeleteApi
    ///   POST   /v2/apis/{api-id}/routes -> CreateRoute
    ///   GET    /v2/apis/{api-id}/routes -> GetRoutes
    ///   GET    /v2/apis/{api-id}/routes/{route-id} -> GetRoute
    ///   PATCH  /v2/apis/{api-id}/routes/{route-id} -> UpdateRoute
    ///   DELETE /v2/apis/{api-id}/routes/{route-id} -> DeleteRoute
    ///   POST   /v2/apis/{api-id}/integrations -> CreateIntegration
    ///   GET    /v2/apis/{api-id}/integrations -> GetIntegrations
    ///   GET    /v2/apis/{api-id}/integrations/{int-id} -> GetIntegration
    ///   PATCH  /v2/apis/{api-id}/integrations/{int-id} -> UpdateIntegration
    ///   DELETE /v2/apis/{api-id}/integrations/{int-id} -> DeleteIntegration
    ///   POST   /v2/apis/{api-id}/stages -> CreateStage
    ///   GET    /v2/apis/{api-id}/stages -> GetStages
    ///   GET    /v2/apis/{api-id}/stages/{stage-name} -> GetStage
    ///   PATCH  /v2/apis/{api-id}/stages/{stage-name} -> UpdateStage
    ///   DELETE /v2/apis/{api-id}/stages/{stage-name} -> DeleteStage
    ///   POST   /v2/apis/{api-id}/deployments -> CreateDeployment
    ///   GET    /v2/apis/{api-id}/deployments -> GetDeployments
    ///   GET    /v2/apis/{api-id}/deployments/{deployment-id} -> GetDeployment
    fn resolve_action(req: &AwsRequest) -> Option<(&str, Option<String>, Option<String>)> {
        let segs = &req.path_segments;
        if segs.len() < 2 {
            return None;
        }

        // Expect /v2/apis
        if segs[0] != "v2" || segs[1] != "apis" {
            return None;
        }

        match (req.method.clone(), segs.len()) {
            // /v2/apis
            (Method::POST, 2) => Some(("CreateApi", None, None)),
            (Method::GET, 2) => Some(("GetApis", None, None)),
            // /v2/apis/{api-id}
            (Method::GET, 3) => Some(("GetApi", Some(segs[2].clone()), None)),
            (Method::PATCH, 3) => Some(("UpdateApi", Some(segs[2].clone()), None)),
            (Method::DELETE, 3) => Some(("DeleteApi", Some(segs[2].clone()), None)),
            // /v2/apis/{api-id}/routes or /v2/apis/{api-id}/integrations
            (Method::POST, 4) if segs[3] == "routes" => {
                Some(("CreateRoute", Some(segs[2].clone()), None))
            }
            (Method::GET, 4) if segs[3] == "routes" => {
                Some(("GetRoutes", Some(segs[2].clone()), None))
            }
            (Method::POST, 4) if segs[3] == "integrations" => {
                Some(("CreateIntegration", Some(segs[2].clone()), None))
            }
            (Method::GET, 4) if segs[3] == "integrations" => {
                Some(("GetIntegrations", Some(segs[2].clone()), None))
            }
            (Method::POST, 4) if segs[3] == "stages" => {
                Some(("CreateStage", Some(segs[2].clone()), None))
            }
            (Method::GET, 4) if segs[3] == "stages" => {
                Some(("GetStages", Some(segs[2].clone()), None))
            }
            (Method::POST, 4) if segs[3] == "deployments" => {
                Some(("CreateDeployment", Some(segs[2].clone()), None))
            }
            (Method::GET, 4) if segs[3] == "deployments" => {
                Some(("GetDeployments", Some(segs[2].clone()), None))
            }
            // /v2/apis/{api-id}/routes/{route-id} or /v2/apis/{api-id}/integrations/{int-id}
            (Method::GET, 5) if segs[3] == "routes" => {
                Some(("GetRoute", Some(segs[2].clone()), Some(segs[4].clone())))
            }
            (Method::PATCH, 5) if segs[3] == "routes" => {
                Some(("UpdateRoute", Some(segs[2].clone()), Some(segs[4].clone())))
            }
            (Method::DELETE, 5) if segs[3] == "routes" => {
                Some(("DeleteRoute", Some(segs[2].clone()), Some(segs[4].clone())))
            }
            (Method::GET, 5) if segs[3] == "integrations" => Some((
                "GetIntegration",
                Some(segs[2].clone()),
                Some(segs[4].clone()),
            )),
            (Method::PATCH, 5) if segs[3] == "integrations" => Some((
                "UpdateIntegration",
                Some(segs[2].clone()),
                Some(segs[4].clone()),
            )),
            (Method::DELETE, 5) if segs[3] == "integrations" => Some((
                "DeleteIntegration",
                Some(segs[2].clone()),
                Some(segs[4].clone()),
            )),
            (Method::GET, 5) if segs[3] == "stages" => {
                Some(("GetStage", Some(segs[2].clone()), Some(segs[4].clone())))
            }
            (Method::PATCH, 5) if segs[3] == "stages" => {
                Some(("UpdateStage", Some(segs[2].clone()), Some(segs[4].clone())))
            }
            (Method::DELETE, 5) if segs[3] == "stages" => {
                Some(("DeleteStage", Some(segs[2].clone()), Some(segs[4].clone())))
            }
            (Method::GET, 5) if segs[3] == "deployments" => Some((
                "GetDeployment",
                Some(segs[2].clone()),
                Some(segs[4].clone()),
            )),
            _ => None,
        }
    }
}

#[async_trait]
impl AwsService for ApiGatewayV2Service {
    fn service_name(&self) -> &str {
        "apigateway"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Check if this is a management API request or an execute API request
        // Management API: /v2/apis/*
        // Execute API: /{stage}/{path}
        if req.path_segments.first().map(|s| s.as_str()) == Some("v2") {
            // Management API
            return self.handle_management_api(req).await;
        }

        // Execute API
        self.handle_execute_api(req).await
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED
    }
}

impl ApiGatewayV2Service {
    async fn handle_management_api(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, api_id, resource_id) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Unknown path: {}", req.raw_path),
            )
        })?;

        match action {
            "CreateApi" => self.create_api(&req),
            "GetApi" => self.get_api(&req, api_id.as_deref()),
            "GetApis" => self.get_apis(&req),
            "UpdateApi" => self.update_api(&req, api_id.as_deref()),
            "DeleteApi" => self.delete_api(&req, api_id.as_deref()),
            "CreateRoute" => self.create_route(&req, api_id.as_deref()),
            "GetRoute" => self.get_route(&req, api_id.as_deref(), resource_id.as_deref()),
            "GetRoutes" => self.get_routes(&req, api_id.as_deref()),
            "UpdateRoute" => self.update_route(&req, api_id.as_deref(), resource_id.as_deref()),
            "DeleteRoute" => self.delete_route(&req, api_id.as_deref(), resource_id.as_deref()),
            "CreateIntegration" => self.create_integration(&req, api_id.as_deref()),
            "GetIntegration" => {
                self.get_integration(&req, api_id.as_deref(), resource_id.as_deref())
            }
            "GetIntegrations" => self.get_integrations(&req, api_id.as_deref()),
            "UpdateIntegration" => {
                self.update_integration(&req, api_id.as_deref(), resource_id.as_deref())
            }
            "DeleteIntegration" => {
                self.delete_integration(&req, api_id.as_deref(), resource_id.as_deref())
            }
            "CreateStage" => self.create_stage(&req, api_id.as_deref()),
            "GetStage" => self.get_stage(&req, api_id.as_deref(), resource_id.as_deref()),
            "GetStages" => self.get_stages(&req, api_id.as_deref()),
            "UpdateStage" => self.update_stage(&req, api_id.as_deref(), resource_id.as_deref()),
            "DeleteStage" => self.delete_stage(&req, api_id.as_deref(), resource_id.as_deref()),
            "CreateDeployment" => self.create_deployment(&req, api_id.as_deref()),
            "GetDeployment" => self.get_deployment(&req, api_id.as_deref(), resource_id.as_deref()),
            "GetDeployments" => self.get_deployments(&req, api_id.as_deref()),
            _ => Err(AwsServiceError::action_not_implemented(
                "apigateway",
                action,
            )),
        }
    }

    // ─── API CRUD ───────────────────────────────────────────────────────

    fn create_api(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        // API Gateway v2 REST API uses lowercase field names
        validate_required("name", &body["name"])?;
        let name = body["name"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "name is required",
                )
            })?
            .to_string();

        validate_required("protocolType", &body["protocolType"])?;
        let protocol_type = body["protocolType"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "protocolType is required",
            )
        })?;

        if protocol_type != "HTTP" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                format!("Unsupported protocol type: {}", protocol_type),
            ));
        }

        let description = body["description"].as_str().map(|s| s.to_string());
        let tags = body["tags"].as_object().map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        });

        // Parse CORS configuration if provided
        let cors_configuration = if let Some(cors) = body.get("corsConfiguration") {
            Some(serde_json::from_value(cors.clone()).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("Invalid corsConfiguration: {}", e),
                )
            })?)
        } else {
            None
        };

        let api_id = generate_id("api");
        let region = &req.region;

        let mut api = HttpApi::new(api_id, name, description, tags, region);
        api.cors_configuration = cors_configuration;

        let mut state = self.state.write();
        let api_clone = api.clone();
        state.apis.insert(api.api_id.clone(), api);

        Ok(AwsResponse::ok_json(json!(api_clone)))
    }

    fn get_api(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let state = self.state.read();
        let api = state.apis.get(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        Ok(AwsResponse::ok_json(json!(api)))
    }

    fn get_apis(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let apis: Vec<&HttpApi> = state.apis.values().collect();

        Ok(AwsResponse::ok_json(json!({
            "items": apis,
        })))
    }

    fn update_api(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let body = req.json_body();
        let mut state = self.state.write();

        let api = state.apis.get_mut(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        if let Some(name) = body["name"].as_str() {
            api.name = name.to_string();
        }

        if let Some(description) = body["description"].as_str() {
            api.description = Some(description.to_string());
        }

        if let Some(cors) = body.get("corsConfiguration") {
            api.cors_configuration = Some(serde_json::from_value(cors.clone()).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("Invalid corsConfiguration: {}", e),
                )
            })?);
        }

        Ok(AwsResponse::ok_json(json!(api)))
    }

    fn delete_api(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let mut state = self.state.write();

        state.apis.remove(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, vec![]))
    }

    // ─── ROUTE CRUD ─────────────────────────────────────────────────────

    fn create_route(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let body = req.json_body();

        validate_required("routeKey", &body["routeKey"])?;
        let route_key = body["routeKey"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "routeKey is required",
                )
            })?
            .to_string();

        let target = body["target"].as_str().map(|s| s.to_string());
        let authorization_type = body["authorizationType"].as_str().map(|s| s.to_string());
        let authorizer_id = body["authorizerId"].as_str().map(|s| s.to_string());

        let route_id = generate_id("route");

        let route = Route {
            route_id: route_id.clone(),
            route_key,
            target,
            authorization_type,
            authorizer_id,
        };

        let mut state = self.state.write();

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        state
            .routes
            .entry(api_id.to_string())
            .or_default()
            .insert(route_id, route.clone());

        Ok(AwsResponse::ok_json(json!(route)))
    }

    fn get_route(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
        route_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let route_id = route_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Route ID is required",
            )
        })?;

        let state = self.state.read();

        let routes = state.routes.get(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        let route = routes.get(route_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Route not found: {}", route_id),
            )
        })?;

        Ok(AwsResponse::ok_json(json!(route)))
    }

    fn get_routes(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let state = self.state.read();

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        let routes: Vec<&Route> = state
            .routes
            .get(api_id)
            .map(|r| r.values().collect())
            .unwrap_or_default();

        Ok(AwsResponse::ok_json(json!({
            "items": routes,
        })))
    }

    fn update_route(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
        route_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let route_id = route_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Route ID is required",
            )
        })?;

        let body = req.json_body();
        let mut state = self.state.write();

        let routes = state.routes.get_mut(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        let route = routes.get_mut(route_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Route not found: {}", route_id),
            )
        })?;

        if let Some(route_key) = body["routeKey"].as_str() {
            route.route_key = route_key.to_string();
        }

        if let Some(target) = body["target"].as_str() {
            route.target = Some(target.to_string());
        }

        if let Some(authorization_type) = body["authorizationType"].as_str() {
            route.authorization_type = Some(authorization_type.to_string());
        }

        if let Some(authorizer_id) = body["authorizerId"].as_str() {
            route.authorizer_id = Some(authorizer_id.to_string());
        }

        Ok(AwsResponse::ok_json(json!(route)))
    }

    fn delete_route(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
        route_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let route_id = route_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Route ID is required",
            )
        })?;

        let mut state = self.state.write();

        let routes = state.routes.get_mut(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        routes.remove(route_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Route not found: {}", route_id),
            )
        })?;

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, vec![]))
    }

    // ─── INTEGRATION CRUD ───────────────────────────────────────────────

    fn create_integration(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let body = req.json_body();

        validate_required("integrationType", &body["integrationType"])?;
        let integration_type = body["integrationType"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "integrationType is required",
                )
            })?
            .to_string();

        let integration_uri = body["integrationUri"].as_str().map(|s| s.to_string());
        let payload_format_version = body["payloadFormatVersion"].as_str().map(|s| s.to_string());
        let timeout_in_millis = body["timeoutInMillis"].as_i64();

        let integration_id = generate_id("integration");

        let integration = Integration {
            integration_id: integration_id.clone(),
            integration_type,
            integration_uri,
            payload_format_version,
            timeout_in_millis,
        };

        let mut state = self.state.write();

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        state
            .integrations
            .entry(api_id.to_string())
            .or_default()
            .insert(integration_id, integration.clone());

        Ok(AwsResponse::ok_json(json!(integration)))
    }

    fn get_integration(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
        integration_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let integration_id = integration_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Integration ID is required",
            )
        })?;

        let state = self.state.read();

        let integrations = state.integrations.get(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        let integration = integrations.get(integration_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Integration not found: {}", integration_id),
            )
        })?;

        Ok(AwsResponse::ok_json(json!(integration)))
    }

    fn get_integrations(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let state = self.state.read();

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        let integrations: Vec<&Integration> = state
            .integrations
            .get(api_id)
            .map(|i| i.values().collect())
            .unwrap_or_default();

        Ok(AwsResponse::ok_json(json!({
            "items": integrations,
        })))
    }

    fn update_integration(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
        integration_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let integration_id = integration_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Integration ID is required",
            )
        })?;

        let body = req.json_body();
        let mut state = self.state.write();

        let integrations = state.integrations.get_mut(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        let integration = integrations.get_mut(integration_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Integration not found: {}", integration_id),
            )
        })?;

        if let Some(integration_type) = body["integrationType"].as_str() {
            integration.integration_type = integration_type.to_string();
        }

        if let Some(integration_uri) = body["integrationUri"].as_str() {
            integration.integration_uri = Some(integration_uri.to_string());
        }

        if let Some(payload_format_version) = body["payloadFormatVersion"].as_str() {
            integration.payload_format_version = Some(payload_format_version.to_string());
        }

        if let Some(timeout_in_millis) = body["timeoutInMillis"].as_i64() {
            integration.timeout_in_millis = Some(timeout_in_millis);
        }

        Ok(AwsResponse::ok_json(json!(integration)))
    }

    fn delete_integration(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
        integration_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let integration_id = integration_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Integration ID is required",
            )
        })?;

        let mut state = self.state.write();

        let integrations = state.integrations.get_mut(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        integrations.remove(integration_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Integration not found: {}", integration_id),
            )
        })?;

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, vec![]))
    }

    // ─── STAGE CRUD ─────────────────────────────────────────────────────

    fn create_stage(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let body = req.json_body();

        validate_required("stageName", &body["stageName"])?;
        let stage_name = body["stageName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "stageName is required",
                )
            })?
            .to_string();

        let description = body["description"].as_str().map(|s| s.to_string());
        let auto_deploy = body["autoDeploy"].as_bool().unwrap_or(false);
        let deployment_id = body["deploymentId"].as_str().map(|s| s.to_string());

        let created_date = chrono::Utc::now();

        let stage = Stage {
            stage_name: stage_name.clone(),
            description,
            deployment_id,
            auto_deploy,
            created_date,
            last_updated_date: None,
        };

        let mut state = self.state.write();

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        state
            .stages
            .entry(api_id.to_string())
            .or_default()
            .insert(stage_name, stage.clone());

        Ok(AwsResponse::ok_json(json!(stage)))
    }

    fn get_stage(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
        stage_name: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let stage_name = stage_name.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Stage name is required",
            )
        })?;

        let state = self.state.read();

        let stages = state.stages.get(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        let stage = stages.get(stage_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Stage not found: {}", stage_name),
            )
        })?;

        Ok(AwsResponse::ok_json(json!(stage)))
    }

    fn get_stages(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let state = self.state.read();

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        let stages: Vec<&Stage> = state
            .stages
            .get(api_id)
            .map(|s| s.values().collect())
            .unwrap_or_default();

        Ok(AwsResponse::ok_json(json!({
            "items": stages,
        })))
    }

    fn update_stage(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
        stage_name: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let stage_name = stage_name.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Stage name is required",
            )
        })?;

        let body = req.json_body();
        let mut state = self.state.write();

        let stages = state.stages.get_mut(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        let stage = stages.get_mut(stage_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Stage not found: {}", stage_name),
            )
        })?;

        if let Some(description) = body["description"].as_str() {
            stage.description = Some(description.to_string());
        }

        if let Some(auto_deploy) = body["autoDeploy"].as_bool() {
            stage.auto_deploy = auto_deploy;
        }

        if let Some(deployment_id) = body["deploymentId"].as_str() {
            stage.deployment_id = Some(deployment_id.to_string());
        }

        stage.last_updated_date = Some(chrono::Utc::now());

        Ok(AwsResponse::ok_json(json!(stage)))
    }

    fn delete_stage(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
        stage_name: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let stage_name = stage_name.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Stage name is required",
            )
        })?;

        let mut state = self.state.write();

        let stages = state.stages.get_mut(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        stages.remove(stage_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Stage not found: {}", stage_name),
            )
        })?;

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, vec![]))
    }

    // ─── DEPLOYMENT CRUD ────────────────────────────────────────────────

    fn create_deployment(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let body = req.json_body();
        let description = body["description"].as_str().map(|s| s.to_string());
        let stage_name = body["stageName"].as_str();

        let deployment_id = generate_id("deployment");
        let created_date = chrono::Utc::now();

        let deployment = Deployment {
            deployment_id: deployment_id.clone(),
            description,
            created_date,
            auto_deployed: false,
        };

        let mut state = self.state.write();

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        state
            .deployments
            .entry(api_id.to_string())
            .or_default()
            .insert(deployment_id.clone(), deployment.clone());

        // If stage_name is provided, update the stage's deployment_id
        if let Some(stage_name) = stage_name {
            if let Some(stages) = state.stages.get_mut(api_id) {
                if let Some(stage) = stages.get_mut(stage_name) {
                    stage.deployment_id = Some(deployment_id);
                    stage.last_updated_date = Some(chrono::Utc::now());
                }
            }
        }

        Ok(AwsResponse::ok_json(json!(deployment)))
    }

    fn get_deployment(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
        deployment_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let deployment_id = deployment_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Deployment ID is required",
            )
        })?;

        let state = self.state.read();

        let deployments = state.deployments.get(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        let deployment = deployments.get(deployment_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Deployment not found: {}", deployment_id),
            )
        })?;

        Ok(AwsResponse::ok_json(json!(deployment)))
    }

    fn get_deployments(
        &self,
        _req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let state = self.state.read();

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        let deployments: Vec<&Deployment> = state
            .deployments
            .get(api_id)
            .map(|d| d.values().collect())
            .unwrap_or_default();

        Ok(AwsResponse::ok_json(json!({
            "items": deployments,
        })))
    }

    // ─── EXECUTE API ────────────────────────────────────────────────────

    async fn handle_execute_api(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Execute API format: /{stage}/{path...}
        if req.path_segments.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "Stage not specified",
            ));
        }

        let stage_name = &req.path_segments[0];
        let resource_path = format!("/{}", req.path_segments[1..].join("/"));

        // Find the API for this stage and get CORS configuration
        let (api_id, routes, cors_config) = {
            let state = self.state.read();

            // Find which API has this stage
            let (api_id, _stage) = state
                .stages
                .iter()
                .find_map(|(api_id, stages)| {
                    stages
                        .get(stage_name)
                        .map(|stage| (api_id.clone(), stage.clone()))
                })
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "NotFoundException",
                        format!("Stage not found: {}", stage_name),
                    )
                })?;

            // Get routes for this API
            let routes = state
                .routes
                .get(&api_id)
                .map(|r| r.values().cloned().collect())
                .unwrap_or_default();

            // Get CORS configuration from API
            let cors_config = state
                .apis
                .get(&api_id)
                .and_then(|api| api.cors_configuration.clone());

            Ok::<_, AwsServiceError>((api_id, routes, cors_config))
        }?;

        // Handle CORS preflight requests
        if let Some(ref cors_cfg) = cors_config {
            if cors::is_preflight_request(&req) {
                return Ok(cors::handle_preflight(cors_cfg, &req));
            }
        }

        // Match the request against routes
        let router = Router::new(routes);
        let route_match = router
            .match_route(req.method.as_str(), &resource_path)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    format!("No route matches: {} {}", req.method, resource_path),
                )
            })?;

        // Get the integration for this route
        let integration_id = route_match
            .route
            .target
            .as_ref()
            .and_then(|target| target.strip_prefix("integrations/"))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    "Route has no integration",
                )
            })?;

        let integration = {
            let state = self.state.read();
            state
                .integrations
                .get(&api_id)
                .and_then(|integrations| integrations.get(integration_id))
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        format!("Integration not found: {}", integration_id),
                    )
                })?
                .clone()
        };

        // Handle based on integration type
        let mut response = match integration.integration_type.as_str() {
            "AWS_PROXY" => {
                // Lambda proxy integration
                let delivery = self.delivery.as_ref().ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        "Lambda delivery not configured",
                    )
                })?;

                let function_arn = integration.integration_uri.as_ref().ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        "Integration has no URI",
                    )
                })?;

                let event = lambda_proxy::construct_event(
                    &req,
                    &route_match.route.route_key,
                    stage_name,
                    route_match.path_parameters,
                );

                lambda_proxy::invoke_lambda(delivery, function_arn, event).await?
            }
            "HTTP_PROXY" => {
                // HTTP proxy integration
                let target_url = integration.integration_uri.as_ref().ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        "Integration has no URI",
                    )
                })?;

                http_proxy::forward_request(target_url, &req, integration.timeout_in_millis).await?
            }
            "MOCK" => {
                // Mock integration
                mock::create_mock_response()
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_IMPLEMENTED,
                    "NotImplemented",
                    format!(
                        "Integration type not supported: {}",
                        integration.integration_type
                    ),
                ));
            }
        };

        // Add CORS headers if CORS is configured
        if let Some(ref cors_cfg) = cors_config {
            response = cors::add_cors_headers(response, cors_cfg);
        }

        Ok(response)
    }
}

fn generate_id(prefix: &str) -> String {
    let uuid = uuid::Uuid::new_v4().to_string().replace('-', "");
    format!("{}{}", prefix, &uuid[..10])
}
