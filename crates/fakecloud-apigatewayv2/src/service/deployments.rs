//! `ApiGatewayV2Service` `deployments` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl ApiGatewayV2Service {
    // ─── DEPLOYMENT CRUD ────────────────────────────────────────────────

    pub(super) fn create_deployment(
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
            deployment_status: "DEPLOYED".to_string(),
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

    pub(super) fn get_deployment(
        &self,
        req: &AwsRequest,
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

        let accounts = self.state.read();
        let empty = ApiGatewayV2State::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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

    pub(super) fn get_deployments(
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

        let deployments: Vec<&Deployment> = state
            .deployments
            .get(api_id)
            .map(|d| d.values().collect())
            .unwrap_or_default();

        Ok(AwsResponse::ok_json(json!({
            "items": deployments,
        })))
    }
}
