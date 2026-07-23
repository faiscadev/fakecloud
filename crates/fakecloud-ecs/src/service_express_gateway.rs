// 2026 Express Gateway service ops. Express Gateway is a serverless
// container service with built-in load balancing and autoscaling. The
// CRUD ops here record the spec; runtime semantics (scheduling tasks,
// load-balancing requests) are intentionally not modeled — the
// fakecloud control plane only round-trips configuration.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;
use crate::state::ExpressGatewayService;

impl EcsService {
    pub(super) fn create_express_gateway_service(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let execution_role_arn = req_str(&body, "executionRoleArn")?.to_string();
        let infrastructure_role_arn = req_str(&body, "infrastructureRoleArn")?.to_string();
        let primary_container = body.get("primaryContainer").cloned().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ClientException",
                "primaryContainer is required",
            )
        })?;
        let service_name = body
            .get("serviceName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("eg-{}", uuid::Uuid::new_v4()));

        let task_role_arn = body
            .get("taskRoleArn")
            .and_then(|v| v.as_str())
            .map(String::from);
        let network_configuration = body.get("networkConfiguration").cloned();
        let health_check_path = body
            .get("healthCheckPath")
            .and_then(|v| v.as_str())
            .map(String::from);
        let cpu = body.get("cpu").and_then(|v| v.as_str()).map(String::from);
        let memory = body
            .get("memory")
            .and_then(|v| v.as_str())
            .map(String::from);
        let scaling_target = body.get("scalingTarget").cloned();
        let tags = parse_tags(&body);
        let cluster_input = body.get("cluster").and_then(|v| v.as_str());

        let now = Utc::now();
        let mut accounts = self.state.write();
        let account_id = request.account_id.clone();
        let s = accounts.get_or_create(&account_id);
        let cluster_name = cluster_arn_to_name(cluster_input.unwrap_or("default"));

        if !s.clusters.contains_key(&cluster_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ClusterNotFoundException",
                format!("Cluster {} not found", cluster_name),
            ));
        }

        let key = EcsState::express_gateway_key(&cluster_name, &service_name);
        if s.express_gateway_services.contains_key(&key) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ClientException",
                format!(
                    "Express Gateway service {} already exists in cluster {}",
                    service_name, cluster_name
                ),
            ));
        }

        // ARN carries the request's credential-scope region (req.region), not the
        // frozen server default.
        let service_arn =
            s.express_gateway_arn(request.region.as_str(), &cluster_name, &service_name);
        let cluster_arn = s.cluster_arn(request.region.as_str(), &cluster_name);

        let svc = ExpressGatewayService {
            service_name: service_name.clone(),
            service_arn: service_arn.clone(),
            cluster_arn,
            cluster_name,
            status: "ACTIVE".to_string(),
            execution_role_arn,
            infrastructure_role_arn,
            task_role_arn,
            primary_container,
            network_configuration,
            health_check_path,
            cpu,
            memory,
            scaling_target,
            created_at: now,
            updated_at: now,
            tags,
        };
        s.express_gateway_services.insert(key, svc.clone());

        Ok(AwsResponse::ok_json(json!({
            "service": express_gateway_service_json(&svc),
        })))
    }

    pub(super) fn describe_express_gateway_service(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let identifier = req_str(&body, "serviceArn")?.to_string();

        let accounts = self.state.read();
        let s = accounts
            .get(&request.account_id)
            .cloned()
            .unwrap_or_else(|| accounts.default_ref().clone());
        let svc = lookup_express_gateway_by_arn(&s, &identifier).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ClientException",
                format!("Express Gateway service {} not found", identifier),
            )
        })?;
        Ok(AwsResponse::ok_json(json!({
            "service": express_gateway_service_json(&svc),
        })))
    }

    pub(super) fn update_express_gateway_service(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let identifier = req_str(&body, "serviceArn")?.to_string();

        let now = Utc::now();

        let mut accounts = self.state.write();
        let account_id = request.account_id.clone();
        let s = accounts.get_or_create(&account_id);
        let key = lookup_express_gateway_key_by_arn(s, &identifier).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ClientException",
                format!("Express Gateway service {} not found", identifier),
            )
        })?;

        let svc = s.express_gateway_services.get_mut(&key).unwrap();
        if let Some(v) = body.get("primaryContainer").cloned() {
            svc.primary_container = v;
        }
        if let Some(v) = body.get("taskRoleArn").and_then(|v| v.as_str()) {
            svc.task_role_arn = Some(v.to_string());
        }
        if let Some(v) = body.get("networkConfiguration").cloned() {
            svc.network_configuration = Some(v);
        }
        if let Some(v) = body.get("healthCheckPath").and_then(|v| v.as_str()) {
            svc.health_check_path = Some(v.to_string());
        }
        if let Some(v) = body.get("cpu").and_then(|v| v.as_str()) {
            svc.cpu = Some(v.to_string());
        }
        if let Some(v) = body.get("memory").and_then(|v| v.as_str()) {
            svc.memory = Some(v.to_string());
        }
        if let Some(v) = body.get("scalingTarget").cloned() {
            svc.scaling_target = Some(v);
        }
        svc.updated_at = now;

        let snapshot = svc.clone();
        Ok(AwsResponse::ok_json(json!({
            "service": express_gateway_service_json(&snapshot),
        })))
    }

    pub(super) fn delete_express_gateway_service(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let identifier = req_str(&body, "serviceArn")?.to_string();

        let mut accounts = self.state.write();
        let account_id = request.account_id.clone();
        let s = accounts.get_or_create(&account_id);
        let key = lookup_express_gateway_key_by_arn(s, &identifier).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ClientException",
                format!("Express Gateway service {} not found", identifier),
            )
        })?;
        let mut svc = s.express_gateway_services.remove(&key).unwrap();
        svc.status = "DRAINING".to_string();
        svc.updated_at = Utc::now();
        Ok(AwsResponse::ok_json(json!({
            "service": express_gateway_service_json(&svc),
        })))
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn cluster_arn_to_name(arn_or_name: &str) -> String {
    if let Some(idx) = arn_or_name.rfind('/') {
        arn_or_name[idx + 1..].to_string()
    } else {
        arn_or_name.to_string()
    }
}

fn lookup_express_gateway_by_arn(
    s: &crate::state::EcsState,
    arn: &str,
) -> Option<ExpressGatewayService> {
    s.express_gateway_services
        .values()
        .find(|svc| svc.service_arn == arn)
        .cloned()
}

fn lookup_express_gateway_key_by_arn(s: &crate::state::EcsState, arn: &str) -> Option<String> {
    for (k, svc) in &s.express_gateway_services {
        if svc.service_arn == arn {
            return Some(k.clone());
        }
    }
    None
}

fn express_gateway_service_json(s: &ExpressGatewayService) -> Value {
    let active_configuration = json!({
        "serviceRevisionArn": format!("{}:1", s.service_arn),
        "executionRoleArn": s.execution_role_arn,
        "taskRoleArn": s.task_role_arn,
        "cpu": s.cpu,
        "memory": s.memory,
        "networkConfiguration": s.network_configuration,
        "healthCheckPath": s.health_check_path,
        "primaryContainer": s.primary_container,
        "scalingTarget": s.scaling_target,
    });
    json!({
        "cluster": s.cluster_name,
        "serviceName": s.service_name,
        "serviceArn": s.service_arn,
        "infrastructureRoleArn": s.infrastructure_role_arn,
        "status": {
            "statusCode": s.status,
            "statusReason": Value::Null,
        },
        "currentDeployment": Value::Null,
        "activeConfigurations": [active_configuration],
        "createdAt": s.created_at.timestamp() as f64
            + s.created_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
        "updatedAt": s.updated_at.timestamp() as f64
            + s.updated_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
        "tags": s.tags.iter().map(|t| json!({"key": t.key, "value": t.value})).collect::<Vec<_>>(),
    })
}
