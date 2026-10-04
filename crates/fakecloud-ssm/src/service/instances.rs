use std::collections::BTreeMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{SsmActivation, SsmState};

use super::{missing, SsmService};

impl SsmService {
    pub(super) fn create_activation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("IamRole", body["IamRole"].as_str(), 0, 64)?;
        validate_optional_string_length("Description", body["Description"].as_str(), 0, 256)?;
        validate_optional_string_length(
            "DefaultInstanceName",
            body["DefaultInstanceName"].as_str(),
            0,
            256,
        )?;
        validate_optional_range_i64(
            "RegistrationLimit",
            body["RegistrationLimit"].as_i64(),
            1,
            1000,
        )?;
        let iam_role = body["IamRole"]
            .as_str()
            .ok_or_else(|| missing("IamRole"))?
            .to_string();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let default_instance_name = body["DefaultInstanceName"].as_str().map(|s| s.to_string());
        let registration_limit = body["RegistrationLimit"].as_i64().unwrap_or(1);
        let tags: BTreeMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["Key"].as_str()?;
                        let v = t["Value"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        state.activation_counter += 1;
        let activation_id = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            state.activation_counter, 0, 0, 0, state.activation_counter
        );
        let activation_code = format!("code-{}", activation_id);

        let activation = SsmActivation {
            activation_id: activation_id.clone(),
            iam_role,
            registration_limit,
            registrations_count: 0,
            expiration_date: None,
            description,
            default_instance_name,
            created_date: now,
            expired: false,
            tags,
        };
        state.activations.insert(activation_id.clone(), activation);

        Ok(AwsResponse::ok_json(json!({
            "ActivationId": activation_id,
            "ActivationCode": activation_code,
        })))
    }

    pub(super) fn delete_activation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let activation_id = body["ActivationId"]
            .as_str()
            .ok_or_else(|| missing("ActivationId"))?;

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        if state.activations.remove(activation_id).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidActivationId",
                format!("Activation ID {activation_id} not found"),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_activations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.regional(&req.account_id, &req.region).unwrap_or(&empty);
        let activations: Vec<Value> = state
            .activations
            .values()
            .map(|a| {
                let mut v = json!({
                    "ActivationId": a.activation_id,
                    "IamRole": a.iam_role,
                    "RegistrationLimit": a.registration_limit,
                    "RegistrationsCount": a.registrations_count,
                    "CreatedDate": a.created_date.timestamp_millis() as f64 / 1000.0,
                    "Expired": a.expired,
                });
                if let Some(ref d) = a.description {
                    v["Description"] = json!(d);
                }
                if let Some(ref n) = a.default_instance_name {
                    v["DefaultInstanceName"] = json!(n);
                }
                if let Some(ref e) = a.expiration_date {
                    v["ExpirationDate"] = json!(e.timestamp_millis() as f64 / 1000.0);
                }
                if !a.tags.is_empty() {
                    v["Tags"] = json!(a
                        .tags
                        .iter()
                        .map(|(k, v)| json!({"Key": k, "Value": v}))
                        .collect::<Vec<_>>());
                }
                v
            })
            .collect();

        Ok(AwsResponse::ok_json(
            json!({ "ActivationList": activations }),
        ))
    }

    pub(super) fn deregister_managed_instance(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("InstanceId", body["InstanceId"].as_str(), 20, 124)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        state.managed_instances.remove(instance_id);
        // AWS doesn't error on non-existent instances

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_instance_information(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 5, 50)?;
        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.regional(&req.account_id, &req.region).unwrap_or(&empty);
        let instances: Vec<Value> = state
            .managed_instances
            .values()
            .map(|i| {
                json!({
                    "InstanceId": i.instance_id,
                    "PingStatus": i.ping_status,
                    "LastPingDateTime": i.last_ping_date_time.timestamp_millis() as f64 / 1000.0,
                    "AgentVersion": i.agent_version,
                    "IsLatestVersion": i.is_latest_version,
                    "PlatformType": i.platform_type,
                    "PlatformName": i.platform_name,
                    "PlatformVersion": i.platform_version,
                    "ResourceType": i.resource_type,
                    "IPAddress": i.ip_address,
                    "ComputerName": i.computer_name,
                    "IamRole": i.iam_role,
                    "RegistrationDate": i.registration_date.timestamp_millis() as f64 / 1000.0,
                })
            })
            .collect();

        Ok(AwsResponse::ok_json(
            json!({ "InstanceInformationList": instances }),
        ))
    }

    pub(super) fn describe_instance_properties(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 5, 1000)?;
        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.regional(&req.account_id, &req.region).unwrap_or(&empty);
        let instances: Vec<Value> = state
            .managed_instances
            .values()
            .map(|i| {
                json!({
                    "InstanceId": i.instance_id,
                    "PingStatus": i.ping_status,
                    "LastPingDateTime": i.last_ping_date_time.timestamp_millis() as f64 / 1000.0,
                    "AgentVersion": i.agent_version,
                    "PlatformType": i.platform_type,
                    "PlatformName": i.platform_name,
                    "PlatformVersion": i.platform_version,
                    "ResourceType": i.resource_type,
                    "IPAddress": i.ip_address,
                    "ComputerName": i.computer_name,
                })
            })
            .collect();

        Ok(AwsResponse::ok_json(
            json!({ "InstanceProperties": instances }),
        ))
    }

    pub(super) fn update_managed_instance_role(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        let iam_role = body["IamRole"]
            .as_str()
            .ok_or_else(|| missing("IamRole"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let instance = state
            .managed_instances
            .get_mut(instance_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInstanceId",
                    format!("Instance {instance_id} not found"),
                )
            })?;
        instance.iam_role = iam_role;

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ── Other ─────────────────────────────────────────────────────

    pub(super) fn list_nodes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("SyncName", body["SyncName"].as_str(), 1, 64)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        Ok(AwsResponse::ok_json(json!({ "Nodes": [] })))
    }

    pub(super) fn list_nodes_summary(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("SyncName", body["SyncName"].as_str(), 1, 64)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let _aggregators = body["Aggregators"]
            .as_array()
            .ok_or_else(|| missing("Aggregators"))?;
        Ok(AwsResponse::ok_json(json!({ "Summary": [] })))
    }
}
