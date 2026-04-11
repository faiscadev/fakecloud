use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{HttpApi, Integration, Route, SharedApiGatewayV2State};

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
];

pub struct ApiGatewayV2Service {
    state: SharedApiGatewayV2State,
}

impl ApiGatewayV2Service {
    pub fn new(state: SharedApiGatewayV2State) -> Self {
        Self { state }
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
            _ => Err(AwsServiceError::action_not_implemented(
                "apigateway",
                action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED
    }
}

impl ApiGatewayV2Service {
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

        let api_id = generate_id("api");
        let region = &req.region;

        let api = HttpApi::new(api_id, name, description, tags, region);

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
}

fn generate_id(prefix: &str) -> String {
    let uuid = uuid::Uuid::new_v4().to_string().replace('-', "");
    format!("{}{}", prefix, &uuid[..10])
}
