//! `ApiGatewayV2Service` `authorizers` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl ApiGatewayV2Service {
    // ─── AUTHORIZER CRUD ────────────────────────────────────────────────

    pub(super) fn create_authorizer(
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

        validate_required("authorizerType", &body["authorizerType"])?;
        let authorizer_type = body["authorizerType"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "authorizerType is required",
                )
            })?
            .to_string();

        let authorizer_uri = body["authorizerUri"].as_str().map(|s| s.to_string());
        let identity_source = body["identitySource"].as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        });

        let jwt_configuration = if let Some(jwt) = body.get("jwtConfiguration") {
            Some(serde_json::from_value(jwt.clone()).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    format!("Invalid jwtConfiguration: {}", e),
                )
            })?)
        } else {
            None
        };

        let authorizer_id = generate_id("auth");

        let authorizer = Authorizer {
            authorizer_id: authorizer_id.clone(),
            name,
            authorizer_type,
            authorizer_uri,
            identity_source,
            jwt_configuration,
            authorizer_payload_format_version: body["authorizerPayloadFormatVersion"]
                .as_str()
                .map(|s| s.to_string()),
            authorizer_result_ttl_in_seconds: body["authorizerResultTtlInSeconds"].as_i64(),
            enable_simple_responses: body["enableSimpleResponses"].as_bool(),
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        state
            .authorizers
            .entry(api_id.to_string())
            .or_default()
            .insert(authorizer_id, authorizer.clone());

        Ok(AwsResponse::ok_json(json!(authorizer)))
    }

    pub(super) fn get_authorizer(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
        authorizer_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let authorizer_id = authorizer_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Authorizer ID is required",
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

        let authorizer = state
            .authorizers
            .get(api_id)
            .and_then(|auths| auths.get(authorizer_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    format!("Authorizer not found: {}", authorizer_id),
                )
            })?;

        Ok(AwsResponse::ok_json(json!(authorizer)))
    }

    pub(super) fn get_authorizers(
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

        let authorizers: Vec<&Authorizer> = state
            .authorizers
            .get(api_id)
            .map(|auths| auths.values().collect())
            .unwrap_or_default();

        Ok(AwsResponse::ok_json(json!({
            "items": authorizers,
        })))
    }

    pub(super) fn update_authorizer(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
        authorizer_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let authorizer_id = authorizer_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Authorizer ID is required",
            )
        })?;

        let body = req.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        let authorizer = state
            .authorizers
            .get_mut(api_id)
            .and_then(|auths| auths.get_mut(authorizer_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    format!("Authorizer not found: {}", authorizer_id),
                )
            })?;

        if let Some(name) = body["name"].as_str() {
            authorizer.name = name.to_string();
        }

        if let Some(authorizer_uri) = body["authorizerUri"].as_str() {
            authorizer.authorizer_uri = Some(authorizer_uri.to_string());
        }

        if let Some(identity_source) = body["identitySource"].as_array() {
            authorizer.identity_source = Some(
                identity_source
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect(),
            );
        }

        if let Some(jwt) = body.get("jwtConfiguration") {
            authorizer.jwt_configuration =
                Some(serde_json::from_value(jwt.clone()).map_err(|e| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "BadRequestException",
                        format!("Invalid jwtConfiguration: {}", e),
                    )
                })?);
        }

        if let Some(v) = body["authorizerPayloadFormatVersion"].as_str() {
            authorizer.authorizer_payload_format_version = Some(v.to_string());
        }

        if let Some(v) = body["authorizerResultTtlInSeconds"].as_i64() {
            authorizer.authorizer_result_ttl_in_seconds = Some(v);
        }

        if let Some(v) = body["enableSimpleResponses"].as_bool() {
            authorizer.enable_simple_responses = Some(v);
        }

        Ok(AwsResponse::ok_json(json!(authorizer)))
    }

    pub(super) fn delete_authorizer(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
        authorizer_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let authorizer_id = authorizer_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Authorizer ID is required",
            )
        })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Verify API exists
        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }

        state
            .authorizers
            .get_mut(api_id)
            .and_then(|auths| auths.remove(authorizer_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    format!("Authorizer not found: {}", authorizer_id),
                )
            })?;

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, vec![]))
    }

    /// Enforce the authorizer configured on a route. Returns an
    /// `AuthorizerInfo` when validation succeeds, or `None` when the route
    /// has no authorizer. Propagates `401 Unauthorized` on failure.
    pub(super) async fn enforce_authorizer(
        &self,
        req: &AwsRequest,
        api_id: &str,
        stage: &str,
        route: &Route,
    ) -> Result<Option<AuthorizerInfo>, AwsServiceError> {
        let authorizer_id = match &route.authorizer_id {
            Some(id) => id,
            None => return Ok(None),
        };

        let authorizer = {
            let accounts = self.state.read();
            let empty = ApiGatewayV2State::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            state
                .authorizers
                .get(api_id)
                .and_then(|a| a.get(authorizer_id))
                .cloned()
        };

        let Some(authorizer) = authorizer else {
            return Err(AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                format!("Authorizer not found: {}", authorizer_id),
            ));
        };

        match authorizer.authorizer_type.as_str() {
            "JWT" => self.enforce_jwt_authorizer(req, &authorizer).await,
            "REQUEST" => {
                self.enforce_lambda_authorizer(req, api_id, stage, &authorizer)
                    .await
            }
            _ => Err(AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                format!(
                    "Unsupported authorizer type: {}",
                    authorizer.authorizer_type
                ),
            )),
        }
    }

    /// Validate a JWT token against the configured issuer and audience.
    pub(super) async fn enforce_jwt_authorizer(
        &self,
        req: &AwsRequest,
        authorizer: &Authorizer,
    ) -> Result<Option<AuthorizerInfo>, AwsServiceError> {
        let identity_sources = authorizer.identity_source.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::UNAUTHORIZED,
                "UnauthorizedException",
                "Authorizer has no identity source",
            )
        })?;

        let token_value = identity_sources
            .iter()
            .find_map(|source| extract_identity_source_value(req, source))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::UNAUTHORIZED,
                    "UnauthorizedException",
                    "Missing required JWT",
                )
            })?;

        let token = token_value
            .strip_prefix("Bearer ")
            .or_else(|| token_value.strip_prefix("bearer "))
            .unwrap_or(&token_value)
            .trim();

        if token.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::UNAUTHORIZED,
                "UnauthorizedException",
                "Empty Authorization header",
            ));
        }

        let jwt_config = authorizer.jwt_configuration.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "JWT authorizer has no configuration",
            )
        })?;

        let issuer = jwt_config.issuer.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "JWT authorizer has no issuer",
            )
        })?;

        let delivery = self.delivery.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "JWT verifier not configured",
            )
        })?;

        let pool_arn =
            issuer_to_pool_arn(&req.account_id, &req.region, issuer).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    "Invalid JWT issuer format",
                )
            })?;

        let claims = delivery
            .verify_cognito_jwt(&req.account_id, &pool_arn, token)
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::UNAUTHORIZED,
                    "UnauthorizedException",
                    format!("Invalid JWT: {e}"),
                )
            })?;

        // Validate audience
        if let Some(audiences) = &jwt_config.audience {
            let token_aud = claims.get("aud").and_then(|v| v.as_str());
            let token_aud_array = claims.get("aud").and_then(|v| v.as_array());
            let matches = token_aud
                .map(|a| audiences.contains(&a.to_string()))
                .unwrap_or(false)
                || token_aud_array
                    .map(|arr| {
                        arr.iter().any(|v| {
                            v.as_str()
                                .map(|s| audiences.contains(&s.to_string()))
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false);
            if !matches {
                return Err(AwsServiceError::aws_error(
                    StatusCode::UNAUTHORIZED,
                    "UnauthorizedException",
                    "Invalid audience",
                ));
            }
        }

        Ok(Some(AuthorizerInfo::Jwt { claims }))
    }

    /// Invoke a Lambda authorizer (REQUEST type) and interpret the
    /// response. Supports both IAM-policy and simple-response formats.
    pub(super) async fn enforce_lambda_authorizer(
        &self,
        req: &AwsRequest,
        api_id: &str,
        stage: &str,
        authorizer: &Authorizer,
    ) -> Result<Option<AuthorizerInfo>, AwsServiceError> {
        // Identity sources are optional for REQUEST authorizers.
        // When configured, every listed source must be present.
        if let Some(sources) = &authorizer.identity_source {
            for source in sources {
                if extract_identity_source_value(req, source)
                    .map(|v| v.is_empty())
                    .unwrap_or(true)
                {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::UNAUTHORIZED,
                        "UnauthorizedException",
                        "Missing required identity source",
                    ));
                }
            }
        }

        let auth_uri = authorizer.authorizer_uri.as_deref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Authorizer is missing authorizerUri; cannot invoke Lambda",
            )
        })?;
        let function_arn = extract_lambda_arn(auth_uri).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "authorizerUri must reference a Lambda function ARN",
            )
        })?;

        let method_arn = build_method_arn(req, api_id, stage);

        let mut headers = serde_json::Map::new();
        for (k, v) in req.headers.iter() {
            if let Ok(s) = v.to_str() {
                headers.insert(
                    k.as_str().to_string(),
                    serde_json::Value::String(s.to_string()),
                );
            }
        }
        let mut query = serde_json::Map::new();
        for (k, v) in &req.query_params {
            query.insert(k.clone(), serde_json::Value::String(v.clone()));
        }

        let event = json!({
            "type": "REQUEST",
            "methodArn": method_arn,
            "resource": req.raw_path,
            "path": req.raw_path,
            "httpMethod": req.method.as_str(),
            "headers": headers,
            "queryStringParameters": query,
            "requestContext": {
                "apiId": api_id,
                "stage": req.path_segments.first().map(|s| s.as_str()).unwrap_or("$default"),
                "path": req.raw_path,
                "httpMethod": req.method.as_str(),
            },
        });

        let delivery = self.delivery.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Lambda delivery not configured",
            )
        })?;
        let response_bytes = delivery
            .invoke_lambda(&function_arn, &event.to_string())
            .await
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    "Lambda delivery not configured",
                )
            })?
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    "ForbiddenException",
                    format!("Authorizer Lambda failed: {e}"),
                )
            })?;
        let response: serde_json::Value = serde_json::from_slice(&response_bytes).map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "ForbiddenException",
                format!("Authorizer returned invalid JSON: {e}"),
            )
        })?;

        // v2 simple response: { "isAuthorized": true/false, "context": {...} }
        if let Some(is_authorized) = response.get("isAuthorized").and_then(|v| v.as_bool()) {
            if !is_authorized {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    "ForbiddenException",
                    "User is not authorized to access this resource",
                ));
            }
            let mut ctx = response
                .get("context")
                .cloned()
                .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()))
                .as_object()
                .cloned()
                .unwrap_or_default();
            ctx.insert(
                "principalId".to_string(),
                response
                    .get("principalId")
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::String("user".to_string())),
            );
            return Ok(Some(AuthorizerInfo::Lambda {
                context: serde_json::Value::Object(ctx),
            }));
        }

        // IAM-policy format (same as v1 TOKEN/REQUEST)
        let effect = parse_policy_effect(&response, &method_arn);
        let principal_id = response
            .get("principalId")
            .and_then(|v| v.as_str())
            .unwrap_or("user")
            .to_string();
        let context = response
            .get("context")
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));

        match effect {
            crate::state::AuthEffect::Allow => {
                let mut ctx = context.as_object().cloned().unwrap_or_default();
                ctx.insert(
                    "principalId".to_string(),
                    serde_json::Value::String(principal_id),
                );
                Ok(Some(AuthorizerInfo::Lambda {
                    context: serde_json::Value::Object(ctx),
                }))
            }
            crate::state::AuthEffect::Deny => Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "ForbiddenException",
                "User is not authorized to access this resource",
            )),
        }
    }
}
