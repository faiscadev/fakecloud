//! `ApiGatewayV2Service` `apis` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl ApiGatewayV2Service {
    // ─── API CRUD ───────────────────────────────────────────────────────

    pub(super) fn create_api(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        // API Gateway v2 REST API uses lowercase field names
        validate_required("name", &body["name"])?;
        let name = body["name"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "name is required",
                )
            })?
            .to_string();

        validate_required("protocolType", &body["protocolType"])?;
        let protocol_type = body["protocolType"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "protocolType is required",
            )
        })?;

        if protocol_type != "HTTP" && protocol_type != "WEBSOCKET" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                format!("Unsupported protocol type: {}", protocol_type),
            ));
        }
        let protocol_type = protocol_type.to_string();

        // IpAddressType is an optional enum: ipv4 | dualstack. Capture
        // it now so the persisted HttpApi reflects the request — the
        // prior code validated and dropped it on the floor.
        let requested_ip_type = match body.get("ipAddressType").and_then(|v| v.as_str()) {
            Some(ip) if ip != "ipv4" && ip != "dualstack" => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    format!("Invalid ipAddressType: {}", ip),
                ));
            }
            Some(ip) => Some(ip.to_string()),
            None => None,
        };

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
                    "BadRequestException",
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
        api.protocol_type = protocol_type.clone();
        api.version = body["version"].as_str().map(|s| s.to_string());
        if let Some(ip) = requested_ip_type {
            api.ip_address_type = ip;
        }
        if let Some(disabled) = body["disableExecuteApiEndpoint"].as_bool() {
            api.disable_execute_api_endpoint = disabled;
        }
        if protocol_type == "WEBSOCKET" {
            // WebSocket APIs use a body-based selection expression by default
            // and have no implicit api-key header selector.
            api.route_selection_expression = "$request.body.action".to_string();
            api.api_key_selection_expression = "$request.header.x-api-key".to_string();
            if let Some(rse) = body
                .get("routeSelectionExpression")
                .and_then(|v| v.as_str())
            {
                api.route_selection_expression = rse.to_string();
            }
            if let Some(akse) = body
                .get("apiKeySelectionExpression")
                .and_then(|v| v.as_str())
            {
                api.api_key_selection_expression = akse.to_string();
            }
        }

        let arn = api_resource_arn(region, &api.api_id);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // Unify create-time inline tags into the ARN-keyed tag store that the
        // tag verbs (TagResource/UntagResource/GetTags) operate on. Previously
        // create-time tags lived only on `HttpApi.tags` and were invisible to
        // GetTags, while TagResource tags lived only in `state.tags` and were
        // invisible to GetApi — a permanent Terraform tag drift.
        if let Some(t) = api.tags.clone() {
            if !t.is_empty() {
                state.tags.insert(arn.clone(), t);
            }
        }
        let api_clone = api.clone();
        state.apis.insert(api.api_id.clone(), api);
        let resp = overlay_resource_tags(json!(api_clone), state.tags.get(&arn));

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_api(
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

        let accounts = self.state.read();
        let empty = ApiGatewayV2State::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let api = state.apis.get(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        let arn = api_resource_arn(&req.region, api_id);
        let resp = overlay_resource_tags(json!(api), state.tags.get(&arn));
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_apis(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = ApiGatewayV2State::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let apis: Vec<serde_json::Value> = state
            .apis
            .values()
            .map(|api| {
                let arn = api_resource_arn(&req.region, &api.api_id);
                overlay_resource_tags(json!(api), state.tags.get(&arn))
            })
            .collect();

        Ok(AwsResponse::ok_json(json!({
            "items": apis,
        })))
    }

    pub(super) fn delete_api(
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        state.apis.remove(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;
        // Drop the ARN-keyed tags alongside the API so a re-created API with the
        // same id doesn't inherit stale tags.
        state.tags.remove(&api_resource_arn(&req.region, api_id));

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, vec![]))
    }
}
