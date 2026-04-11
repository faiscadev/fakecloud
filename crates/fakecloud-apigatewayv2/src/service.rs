use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{HttpApi, SharedApiGatewayV2State};

const SUPPORTED: &[&str] = &["CreateApi", "GetApi", "GetApis", "UpdateApi", "DeleteApi"];

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
    fn resolve_action(req: &AwsRequest) -> Option<(&str, Option<String>)> {
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
            (Method::POST, 2) => Some(("CreateApi", None)),
            (Method::GET, 2) => Some(("GetApis", None)),
            // /v2/apis/{api-id}
            (Method::GET, 3) => Some(("GetApi", Some(segs[2].clone()))),
            (Method::PATCH, 3) => Some(("UpdateApi", Some(segs[2].clone()))),
            (Method::DELETE, 3) => Some(("DeleteApi", Some(segs[2].clone()))),
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
        let (action, resource_id) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Unknown path: {}", req.raw_path),
            )
        })?;

        match action {
            "CreateApi" => self.create_api(&req),
            "GetApi" => self.get_api(&req, resource_id.as_deref()),
            "GetApis" => self.get_apis(&req),
            "UpdateApi" => self.update_api(&req, resource_id.as_deref()),
            "DeleteApi" => self.delete_api(&req, resource_id.as_deref()),
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
}

fn generate_id(prefix: &str) -> String {
    let uuid = uuid::Uuid::new_v4().to_string().replace('-', "");
    format!("{}{}", prefix, &uuid[..10])
}
