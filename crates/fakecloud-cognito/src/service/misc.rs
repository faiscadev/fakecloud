use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::json;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{CustomDomainConfig, Device, UserImportJob, UserPoolDomain};

use super::{
    device_to_json, domain_description_to_json, import_job_to_json, require_str, CognitoService,
};

impl CognitoService {
    pub(super) fn create_user_pool_domain(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let domain = require_str(&body, "Domain")?;

        let custom_domain_config =
            body["CustomDomainConfig"]["CertificateArn"]
                .as_str()
                .map(|arn| CustomDomainConfig {
                    certificate_arn: arn.to_string(),
                });

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        if state.domains.contains_key(domain) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Domain {domain} is already associated with a user pool."),
            ));
        }

        let domain_obj = UserPoolDomain {
            user_pool_id: pool_id.to_string(),
            domain: domain.to_string(),
            status: "ACTIVE".to_string(),
            custom_domain_config,
            creation_date: Utc::now(),
        };

        state.domains.insert(domain.to_string(), domain_obj);

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_user_pool_domain(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let domain = require_str(&body, "Domain")?;

        let state = self.state.read();

        // AWS returns empty DomainDescription if not found (no error)
        let description = match state.domains.get(domain) {
            Some(d) => domain_description_to_json(d, &state.account_id),
            None => json!({}),
        };

        Ok(AwsResponse::ok_json(json!({
            "DomainDescription": description
        })))
    }

    pub(super) fn update_user_pool_domain(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let domain = require_str(&body, "Domain")?;

        let custom_domain_config =
            body["CustomDomainConfig"]["CertificateArn"]
                .as_str()
                .map(|arn| CustomDomainConfig {
                    certificate_arn: arn.to_string(),
                });

        let mut state = self.state.write();

        let d = state.domains.get_mut(domain).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Domain {domain} does not exist."),
            )
        })?;

        if d.user_pool_id != pool_id {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Domain {domain} does not exist."),
            ));
        }

        d.custom_domain_config = custom_domain_config;

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_user_pool_domain(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let domain = require_str(&body, "Domain")?;

        let mut state = self.state.write();

        match state.domains.get(domain) {
            Some(d) if d.user_pool_id == pool_id => {
                state.domains.remove(domain);
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Domain {domain} does not exist."),
                ));
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ── Device Management ───────────────────────────────────────────────

    pub(super) fn admin_get_device(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let device_key = require_str(&body, "DeviceKey")?;

        let state = self.state.read();

        let users = state.users.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let user = users.get(username).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User {username} does not exist."),
            )
        })?;

        let device = user.devices.get(device_key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Device {device_key} does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "Device": device_to_json(device)
        })))
    }

    pub(super) fn admin_list_devices(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let limit = body["Limit"].as_i64().unwrap_or(10).clamp(1, 60) as usize;
        let pagination_token = body["PaginationToken"].as_str();

        let state = self.state.read();

        let users = state.users.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let user = users.get(username).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User {username} does not exist."),
            )
        })?;

        let mut devices: Vec<&Device> = user.devices.values().collect();
        devices.sort_by(|a, b| a.device_create_date.cmp(&b.device_create_date));

        let start = pagination_token
            .and_then(|t| devices.iter().position(|d| d.device_key == t))
            .unwrap_or(0);

        let page = &devices[start..devices.len().min(start + limit)];
        let next_token = if start + limit < devices.len() {
            devices.get(start + limit).map(|d| d.device_key.clone())
        } else {
            None
        };

        let mut result = json!({
            "Devices": page.iter().map(|d| device_to_json(d)).collect::<Vec<_>>()
        });
        if let Some(token) = next_token {
            result["PaginationToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(result))
    }

    pub(super) fn admin_forget_device(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let device_key = require_str(&body, "DeviceKey")?;

        let mut state = self.state.write();

        let users = state.users.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let user = users.get_mut(username).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User {username} does not exist."),
            )
        })?;

        if user.devices.remove(device_key).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Device {device_key} does not exist."),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn admin_update_device_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let device_key = require_str(&body, "DeviceKey")?;
        let status = body["DeviceRememberedStatus"]
            .as_str()
            .map(|s| s.to_string());

        let mut state = self.state.write();

        let users = state.users.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let user = users.get_mut(username).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User {username} does not exist."),
            )
        })?;

        let device = user.devices.get_mut(device_key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Device {device_key} does not exist."),
            )
        })?;

        device.device_remembered_status = status;
        device.device_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn confirm_device(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let device_key = require_str(&body, "DeviceKey")?;
        let device_name = body["DeviceName"].as_str();

        let mut state = self.state.write();

        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;
        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

        let user = state
            .users
            .get_mut(&pool_id)
            .and_then(|users| users.get_mut(&username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid access token.",
                )
            })?;

        let now = Utc::now();
        let mut device_attributes = HashMap::new();
        if let Some(name) = device_name {
            device_attributes.insert("device_name".to_string(), name.to_string());
        }

        user.devices.insert(
            device_key.to_string(),
            Device {
                device_key: device_key.to_string(),
                device_attributes,
                device_create_date: now,
                device_last_modified_date: now,
                device_last_authenticated_date: Some(now),
                device_remembered_status: None,
            },
        );

        Ok(AwsResponse::ok_json(json!({
            "UserConfirmationNecessary": false
        })))
    }

    // ── Tags ────────────────────────────────────────────────────────────

    pub(super) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;

        let tags: HashMap<String, String> = body["Tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();

        // Validate that the ARN matches a known user pool
        let pool_exists = state.user_pools.values().any(|p| p.arn == resource_arn);
        if !pool_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Resource {resource_arn} not found."),
            ));
        }

        let existing = state.tags.entry(resource_arn.to_string()).or_default();
        for (k, v) in tags {
            existing.insert(k, v);
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;

        let tag_keys: Vec<String> = body["TagKeys"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();

        let pool_exists = state.user_pools.values().any(|p| p.arn == resource_arn);
        if !pool_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Resource {resource_arn} not found."),
            ));
        }

        if let Some(tags) = state.tags.get_mut(resource_arn) {
            for key in &tag_keys {
                tags.remove(key);
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;

        let state = self.state.read();

        let pool_exists = state.user_pools.values().any(|p| p.arn == resource_arn);
        if !pool_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Resource {resource_arn} not found."),
            ));
        }

        let tags = state.tags.get(resource_arn).cloned().unwrap_or_default();

        Ok(AwsResponse::ok_json(json!({ "Tags": tags })))
    }

    // ── Import Jobs ─────────────────────────────────────────────────────

    pub(super) fn get_csv_header(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;

        let state = self.state.read();

        let pool = state.user_pools.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let csv_header: Vec<String> = pool
            .schema_attributes
            .iter()
            .map(|a| a.name.clone())
            .collect();

        Ok(AwsResponse::ok_json(json!({
            "UserPoolId": pool_id,
            "CSVHeader": csv_header
        })))
    }

    pub(super) fn create_user_import_job(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let job_name = require_str(&body, "JobName")?;
        let cw_role_arn = require_str(&body, "CloudWatchLogsRoleArn")?;

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let job_id = format!("import-{}", Uuid::new_v4());
        let now = Utc::now();

        let job = UserImportJob {
            job_id: job_id.clone(),
            job_name: job_name.to_string(),
            user_pool_id: pool_id.to_string(),
            cloud_watch_logs_role_arn: cw_role_arn.to_string(),
            status: "Created".to_string(),
            creation_date: now,
            start_date: None,
            completion_date: None,
            pre_signed_url: Some(format!(
                "https://fakecloud-import.s3.amazonaws.com/{pool_id}/{job_id}/upload.csv"
            )),
        };

        let resp = import_job_to_json(&job);

        state
            .import_jobs
            .entry(pool_id.to_string())
            .or_default()
            .insert(job_id, job);

        Ok(AwsResponse::ok_json(json!({
            "UserImportJob": resp
        })))
    }

    pub(super) fn describe_user_import_job(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let job_id = require_str(&body, "JobId")?;

        let state = self.state.read();

        let job = state
            .import_jobs
            .get(pool_id)
            .and_then(|jobs| jobs.get(job_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Import job {job_id} does not exist."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "UserImportJob": import_job_to_json(job)
        })))
    }

    pub(super) fn list_user_import_jobs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(10).clamp(1, 60) as usize;
        let pagination_token = body["PaginationToken"].as_str();

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let mut jobs: Vec<&UserImportJob> = state
            .import_jobs
            .get(pool_id)
            .map(|m| m.values().collect())
            .unwrap_or_default();
        jobs.sort_by(|a, b| a.creation_date.cmp(&b.creation_date));

        let start = pagination_token
            .and_then(|t| jobs.iter().position(|j| j.job_id == t))
            .unwrap_or(0);

        let page = &jobs[start..jobs.len().min(start + max_results)];
        let next_token = if start + max_results < jobs.len() {
            jobs.get(start + max_results).map(|j| j.job_id.clone())
        } else {
            None
        };

        let mut result = json!({
            "UserImportJobs": page.iter().map(|j| import_job_to_json(j)).collect::<Vec<_>>()
        });
        if let Some(token) = next_token {
            result["PaginationToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(result))
    }
}
