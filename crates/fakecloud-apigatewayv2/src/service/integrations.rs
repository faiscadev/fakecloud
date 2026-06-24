//! `ApiGatewayV2Service` `integrations` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl ApiGatewayV2Service {
    // ─── INTEGRATION CRUD ───────────────────────────────────────────────

    pub(super) fn create_integration(
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
                    "BadRequestException",
                    "integrationType is required",
                )
            })?
            .to_string();

        let integration_uri = body["integrationUri"].as_str().map(|s| s.to_string());
        let integration_method = body["integrationMethod"].as_str().map(|s| s.to_string());
        let payload_format_version = body["payloadFormatVersion"].as_str().map(|s| s.to_string());
        let timeout_in_millis = body["timeoutInMillis"].as_i64();
        let connection_type = body["connectionType"]
            .as_str()
            .unwrap_or("INTERNET")
            .to_string();

        let integration_id = generate_id("integration");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Verify API exists
        let protocol_type = match state.apis.get(api_id) {
            Some(api) => api.protocol_type.clone(),
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    format!("API not found: {}", api_id),
                ));
            }
        };

        // AWS applies a protocol-specific default integration timeout when the
        // request omits one: 29000ms for WEBSOCKET APIs, 30000ms for HTTP APIs.
        let is_websocket = protocol_type.eq_ignore_ascii_case("WEBSOCKET");

        let timeout_in_millis =
            Some(timeout_in_millis.unwrap_or(if is_websocket { 29000 } else { 30000 }));

        // WEBSOCKET integrations report a default response selection expression;
        // an explicit request value overrides it. HTTP APIs leave it unset.
        let integration_response_selection_expression = body
            ["integrationResponseSelectionExpression"]
            .as_str()
            .map(|s| s.to_string())
            .or_else(|| is_websocket.then(|| "${integration.response.statuscode}".to_string()));

        // WEBSOCKET integrations default passthrough_behavior to WHEN_NO_MATCH;
        // HTTP APIs leave it unset. An explicit request value overrides.
        let passthrough_behavior = body["passthroughBehavior"]
            .as_str()
            .map(|s| s.to_string())
            .or_else(|| is_websocket.then(|| "WHEN_NO_MATCH".to_string()));

        let integration = Integration {
            integration_id: integration_id.clone(),
            integration_type,
            integration_uri,
            integration_method,
            integration_response_selection_expression,
            passthrough_behavior,
            payload_format_version,
            timeout_in_millis,
            connection_type,
        };

        state
            .integrations
            .entry(api_id.to_string())
            .or_default()
            .insert(integration_id, integration.clone());

        Ok(AwsResponse::ok_json(json!(integration)))
    }

    pub(super) fn get_integration(
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

        let accounts = self.state.read();
        let empty = ApiGatewayV2State::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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

    pub(super) fn get_integrations(
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

    pub(super) fn update_integration(
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
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

        if let Some(integration_method) = body["integrationMethod"].as_str() {
            integration.integration_method = Some(integration_method.to_string());
        }

        if let Some(passthrough_behavior) = body["passthroughBehavior"].as_str() {
            integration.passthrough_behavior = Some(passthrough_behavior.to_string());
        }

        if let Some(payload_format_version) = body["payloadFormatVersion"].as_str() {
            integration.payload_format_version = Some(payload_format_version.to_string());
        }

        if let Some(timeout_in_millis) = body["timeoutInMillis"].as_i64() {
            integration.timeout_in_millis = Some(timeout_in_millis);
        }

        // Previously dropped (bug-hunt 2026-06-24, 1.11): VPC-link connection
        // type and the integration-response selection expression.
        if let Some(connection_type) = body["connectionType"].as_str() {
            integration.connection_type = connection_type.to_string();
        }
        if let Some(expr) = body["integrationResponseSelectionExpression"].as_str() {
            integration.integration_response_selection_expression = Some(expr.to_string());
        }

        Ok(AwsResponse::ok_json(json!(integration)))
    }

    pub(super) fn delete_integration(
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
