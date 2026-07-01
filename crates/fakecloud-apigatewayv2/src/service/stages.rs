//! `ApiGatewayV2Service` `stages` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl ApiGatewayV2Service {
    // ─── STAGE CRUD ─────────────────────────────────────────────────────

    pub(super) fn create_stage(
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
                    "BadRequestException",
                    "stageName is required",
                )
            })?
            .to_string();

        let description = body["description"].as_str().map(|s| s.to_string());
        let auto_deploy = body["autoDeploy"].as_bool().unwrap_or(false);
        let deployment_id = body["deploymentId"].as_str().map(|s| s.to_string());
        let stage_variables = body["stageVariables"].as_object().map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<BTreeMap<String, String>>()
        });

        let access_log_settings = body.get("accessLogSettings").and_then(|v| {
            if v.is_null() {
                return None;
            }
            let destination_arn = v.get("destinationArn")?.as_str()?.to_string();
            let format = v.get("format").and_then(|f| f.as_str().map(String::from));
            Some(crate::state::AccessLogSettings {
                destination_arn,
                format,
            })
        });

        let created_date = chrono::Utc::now();

        let client_certificate_id = body["clientCertificateId"].as_str().map(|s| s.to_string());
        let default_route_settings = body
            .get("defaultRouteSettings")
            .filter(|v| !v.is_null())
            .cloned();
        let route_settings = body
            .get("routeSettings")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<BTreeMap<String, serde_json::Value>>()
            });
        let tags = parse_string_map(&body["tags"]);

        let stage = Stage {
            stage_name: stage_name.clone(),
            description,
            deployment_id,
            auto_deploy,
            created_date,
            last_updated_date: None,
            web_acl_arn: None,
            stage_variables,
            access_log_settings,
            client_certificate_id,
            default_route_settings,
            route_settings,
            tags,
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

        // Check for duplicate stage
        if state
            .stages
            .get(api_id)
            .is_some_and(|stages| stages.contains_key(&stage_name))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ConflictException",
                format!("Stage already exists: {}", stage_name),
            ));
        }

        state
            .stages
            .entry(api_id.to_string())
            .or_default()
            .insert(stage_name, stage.clone());

        Ok(AwsResponse::ok_json(json!(stage)))
    }

    pub(super) fn get_stage(
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

        let accounts = self.state.read();
        let empty = ApiGatewayV2State::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }
        let stage = state
            .stages
            .get(api_id)
            .and_then(|s| s.get(stage_name))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    format!("Stage not found: {}", stage_name),
                )
            })?;

        Ok(AwsResponse::ok_json(json!(stage)))
    }

    pub(super) fn get_stages(
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

        let stages: Vec<&Stage> = state
            .stages
            .get(api_id)
            .map(|s| s.values().collect())
            .unwrap_or_default();

        Ok(AwsResponse::ok_json(json!({
            "items": stages,
        })))
    }

    pub(super) fn update_stage(
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
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }
        let stage = state
            .stages
            .get_mut(api_id)
            .and_then(|s| s.get_mut(stage_name))
            .ok_or_else(|| {
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

        if let Some(vars) = body["stageVariables"].as_object() {
            let mut map = BTreeMap::new();
            for (k, v) in vars.iter() {
                if let Some(s) = v.as_str() {
                    map.insert(k.clone(), s.to_string());
                }
            }
            stage.stage_variables = Some(map);
        }

        if let Some(settings) = body.get("accessLogSettings") {
            if settings.is_null() {
                stage.access_log_settings = None;
            } else if let Some(arn) = settings["destinationArn"].as_str() {
                let format = settings["format"].as_str().map(String::from);
                stage.access_log_settings = Some(crate::state::AccessLogSettings {
                    destination_arn: arn.to_string(),
                    format,
                });
            }
            // If accessLogSettings is present but destinationArn is missing,
            // preserve the existing settings (Cubic finding).
        }

        if let Some(cert) = body["clientCertificateId"].as_str() {
            stage.client_certificate_id = Some(cert.to_string());
        }

        if let Some(settings) = body.get("defaultRouteSettings") {
            if settings.is_null() {
                stage.default_route_settings = None;
            } else {
                stage.default_route_settings = Some(settings.clone());
            }
        }

        if let Some(settings) = body.get("routeSettings") {
            // Distinguish an explicit null (clear) from an absent field
            // (preserve), mirroring defaultRouteSettings above.
            if settings.is_null() {
                stage.route_settings = None;
            } else if let Some(obj) = settings.as_object() {
                stage.route_settings =
                    Some(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            }
        }

        if let Some(tags) = parse_string_map(&body["tags"]) {
            stage.tags = Some(tags);
        }

        stage.last_updated_date = Some(chrono::Utc::now());

        Ok(AwsResponse::ok_json(json!(stage)))
    }

    pub(super) fn delete_stage(
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if !state.apis.contains_key(api_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            ));
        }
        let stages = state.stages.entry(api_id.to_string()).or_default();

        stages.remove(stage_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Stage not found: {}", stage_name),
            )
        })?;

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, vec![]))
    }

    /// Build the resource ARN that callers use when associating a
    /// WebACL with an API Gateway v2 stage:
    /// `arn:aws:apigateway:<region>::/apis/<api>/stages/<stage>`.
    pub(super) fn stage_resource_arn(&self, region: &str, api_id: &str, stage: &str) -> String {
        format!("arn:aws:apigateway:{region}::/apis/{api_id}/stages/{stage}")
    }

    pub(super) fn emit_access_log(
        &self,
        req: &AwsRequest,
        api_id: &str,
        stage: &str,
        route_key: &str,
        status_code: u16,
    ) {
        let Some(delivery) = self.delivery.as_ref() else {
            return;
        };

        let access_log_settings = {
            let accounts = self.state.read();
            let empty = ApiGatewayV2State::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            state
                .stages
                .get(api_id)
                .and_then(|stages| stages.get(stage))
                .and_then(|s| s.access_log_settings.clone())
        };

        let Some(settings) = access_log_settings else {
            return;
        };

        let log_group_name = settings
            .destination_arn
            .split(":log-group:")
            .nth(1)
            .map(|s| {
                if let Some(prefix) = s.strip_suffix(":*") {
                    prefix.to_string()
                } else {
                    s.to_string()
                }
            });

        let Some(log_group_name) = log_group_name else {
            return;
        };

        let request_time = chrono::Utc::now()
            .format("%d/%b/%Y:%H:%M:%S %z")
            .to_string();
        let source_ip = req
            .headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next().map(str::trim))
            .unwrap_or("-")
            .to_string();

        let format = settings.format.as_deref().unwrap_or(
            r#"{"requestId":"$context.requestId","ip":"$context.identity.sourceIp","requestTime":"$context.requestTime","httpMethod":"$context.httpMethod","routeKey":"$context.routeKey","status":"$context.status","protocol":"$context.protocol","responseLength":"$context.responseLength"}"#,
        );

        // Token-aware single-pass substitution to avoid overlapping corruption.
        let mut log_line = String::new();
        let mut rest = format;
        while let Some(pos) = rest.find("$context.") {
            log_line.push_str(&rest[..pos]);
            rest = &rest[pos..];
            let end = rest[9..]
                .find(|c: char| !c.is_alphanumeric() && c != '.' && c != '_')
                .map(|i| i + 9)
                .unwrap_or(rest.len());
            let token = &rest[..end];
            let value = match token {
                "$context.requestId" => req.request_id.as_str(),
                "$context.apiId" => api_id,
                "$context.stage" => stage,
                "$context.identity.sourceIp" => &source_ip,
                "$context.requestTime" => &request_time,
                "$context.httpMethod" => req.method.as_str(),
                "$context.routeKey" => route_key,
                "$context.status" => {
                    log_line.push_str(&status_code.to_string());
                    rest = &rest[end..];
                    continue;
                }
                "$context.protocol" => "HTTP/1.1",
                "$context.responseLength" => "0",
                _ => token,
            };
            log_line.push_str(value);
            rest = &rest[end..];
        }
        log_line.push_str(rest);

        let timestamp = chrono::Utc::now().timestamp_millis();
        let log_stream_name = format!("{}/{}", api_id, stage);

        delivery.put_log_events(
            &req.account_id,
            &log_group_name,
            &log_stream_name,
            &[(timestamp, log_line)],
        );
    }
}
