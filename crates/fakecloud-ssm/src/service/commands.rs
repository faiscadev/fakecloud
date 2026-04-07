use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::SsmCommand;

use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn send_command(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let document_name = body["DocumentName"]
            .as_str()
            .ok_or_else(|| missing("DocumentName"))?
            .to_string();

        // Validate optional fields
        validate_optional_string_length("DocumentHash", body["DocumentHash"].as_str(), 0, 256)?;
        validate_optional_enum(
            "DocumentHashType",
            body["DocumentHashType"].as_str(),
            &["Sha256", "Sha1"],
        )?;
        validate_optional_range_i64(
            "TimeoutSeconds",
            body["TimeoutSeconds"].as_i64(),
            30,
            2592000,
        )?;
        validate_optional_string_length("Comment", body["Comment"].as_str(), 0, 100)?;
        validate_optional_string_length("OutputS3Region", body["OutputS3Region"].as_str(), 3, 20)?;
        validate_optional_string_length(
            "OutputS3BucketName",
            body["OutputS3BucketName"].as_str(),
            3,
            63,
        )?;
        validate_optional_string_length(
            "OutputS3KeyPrefix",
            body["OutputS3KeyPrefix"].as_str(),
            0,
            500,
        )?;
        validate_optional_string_length("MaxConcurrency", body["MaxConcurrency"].as_str(), 1, 7)?;
        validate_optional_string_length("MaxErrors", body["MaxErrors"].as_str(), 1, 7)?;

        let instance_ids: Vec<String> = body["InstanceIds"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let targets: Vec<Value> = body["Targets"].as_array().cloned().unwrap_or_default();
        let parameters: HashMap<String, Vec<String>> = body["Parameters"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| {
                        let vals = v
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        (k.clone(), vals)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let comment = body["Comment"].as_str().map(|s| s.to_string());
        let output_s3_bucket = body["OutputS3BucketName"].as_str().map(|s| s.to_string());
        let output_s3_prefix = body["OutputS3KeyPrefix"].as_str().map(|s| s.to_string());
        let output_s3_region = body["OutputS3Region"].as_str().map(|s| s.to_string());
        let timeout = body["TimeoutSeconds"].as_i64();
        let max_concurrency = body["MaxConcurrency"].as_str().map(|s| s.to_string());
        let max_errors = body["MaxErrors"].as_str().map(|s| s.to_string());
        let service_role = body["ServiceRoleArn"].as_str().map(|s| s.to_string());
        let notification = body.get("NotificationConfig").cloned();
        let document_hash = body["DocumentHash"].as_str().map(|s| s.to_string());
        let document_hash_type = body["DocumentHashType"].as_str().map(|s| s.to_string());

        let command_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        // Resolve targets to instance IDs (for tag-based targets, just use the instance_ids)
        let effective_instance_ids = if instance_ids.is_empty() && !targets.is_empty() {
            // For tag-based targets, we'll simulate with some dummy instance IDs
            vec!["i-placeholder".to_string()]
        } else {
            instance_ids.clone()
        };

        let cmd = SsmCommand {
            command_id: command_id.clone(),
            document_name: document_name.clone(),
            instance_ids: effective_instance_ids.clone(),
            parameters: parameters.clone(),
            status: "Success".to_string(),
            requested_date_time: now,
            comment: comment.clone(),
            output_s3_bucket_name: output_s3_bucket.clone(),
            output_s3_key_prefix: output_s3_prefix.clone(),
            output_s3_region: output_s3_region.clone(),
            timeout_seconds: timeout,
            service_role_arn: service_role.clone(),
            notification_config: notification.clone(),
            targets: targets.clone(),
            document_hash: document_hash.clone(),
            document_hash_type: document_hash_type.clone(),
        };

        let mut state = self.state.write();
        state.commands.push(cmd);

        let expires = now + chrono::Duration::seconds(timeout.unwrap_or(3600));
        let mut cmd_obj = json!({
            "CommandId": command_id,
            "DocumentName": document_name,
            "InstanceIds": effective_instance_ids,
            "Targets": targets,
            "Parameters": parameters,
            "Status": "Success",
            "StatusDetails": "Details placeholder",
            "RequestedDateTime": now.timestamp_millis() as f64 / 1000.0,
            "ExpiresAfter": expires.timestamp_millis() as f64 / 1000.0,
            "MaxConcurrency": max_concurrency.unwrap_or_default(),
            "MaxErrors": max_errors.unwrap_or_default(),
            "DeliveryTimedOutCount": 0,
        });
        if let Some(ref c) = comment {
            cmd_obj["Comment"] = json!(c);
        }
        if let Some(ref r) = output_s3_region {
            cmd_obj["OutputS3Region"] = json!(r);
        }
        if let Some(ref b) = output_s3_bucket {
            cmd_obj["OutputS3BucketName"] = json!(b);
        }
        if let Some(ref p) = output_s3_prefix {
            cmd_obj["OutputS3KeyPrefix"] = json!(p);
        }
        if let Some(t) = timeout {
            cmd_obj["TimeoutSeconds"] = json!(t);
        }

        Ok(json_resp(json!({ "Command": cmd_obj })))
    }

    pub(super) fn list_commands(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("CommandId", body["CommandId"].as_str(), 36, 36)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let command_id = body["CommandId"].as_str();
        let instance_id = body["InstanceId"].as_str();

        let state = self.state.read();
        let all_commands: Vec<Value> = state
            .commands
            .iter()
            .filter(|c| {
                if let Some(cid) = command_id {
                    if c.command_id != cid {
                        return false;
                    }
                }
                if let Some(iid) = instance_id {
                    if !c.instance_ids.contains(&iid.to_string()) {
                        return false;
                    }
                }
                true
            })
            .map(|c| {
                let expires = c.requested_date_time
                    + chrono::Duration::seconds(c.timeout_seconds.unwrap_or(3600));
                let v = json!({
                    "CommandId": c.command_id,
                    "DocumentName": c.document_name,
                    "InstanceIds": c.instance_ids,
                    "Targets": c.targets,
                    "Parameters": c.parameters,
                    "Status": c.status,
                    "StatusDetails": "Details placeholder",
                    "RequestedDateTime": c.requested_date_time.timestamp_millis() as f64 / 1000.0,
                    "ExpiresAfter": expires.timestamp_millis() as f64 / 1000.0,
                    "Comment": c.comment,
                    "OutputS3Region": c.output_s3_region,
                    "OutputS3BucketName": c.output_s3_bucket_name,
                    "OutputS3KeyPrefix": c.output_s3_key_prefix,
                    "DeliveryTimedOutCount": 0,
                });
                v
            })
            .collect();

        // If a specific CommandId was requested and not found, return an error
        if let Some(cid) = command_id {
            if all_commands.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidCommandId",
                    format!("Command with id {cid} does not exist."),
                ));
            }
        }

        let page = if next_token_offset < all_commands.len() {
            &all_commands[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let commands: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "Commands": commands });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    pub(super) fn get_command_invocation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let command_id = body["CommandId"]
            .as_str()
            .ok_or_else(|| missing("CommandId"))?;
        validate_string_length("CommandId", command_id, 36, 36)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        let plugin_name = body["PluginName"].as_str();
        validate_optional_string_length("PluginName", plugin_name, 4, 500)?;

        let state = self.state.read();
        let cmd = state
            .commands
            .iter()
            .find(|c| c.command_id == command_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvocationDoesNotExist",
                    format!("Command {command_id} not found"),
                )
            })?;

        // Check instance is part of the command
        if !cmd.instance_ids.contains(&instance_id.to_string()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvocationDoesNotExist",
                "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation",
            ));
        }

        // Validate plugin name if provided
        if let Some(pn) = plugin_name {
            let known_plugins = ["aws:runShellScript", "aws:runPowerShellScript"];
            if !known_plugins.contains(&pn) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvocationDoesNotExist",
                    "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation",
                ));
            }
        }

        Ok(json_resp(json!({
            "CommandId": cmd.command_id,
            "InstanceId": instance_id,
            "DocumentName": cmd.document_name,
            "Status": "Success",
            "StatusDetails": "Success",
            "ResponseCode": 0,
            "StandardOutputContent": "",
            "StandardOutputUrl": "",
            "StandardErrorContent": "",
            "StandardErrorUrl": "",
        })))
    }

    pub(super) fn list_command_invocations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("CommandId", body["CommandId"].as_str(), 36, 36)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let command_id = body["CommandId"].as_str();

        let state = self.state.read();
        let all_invocations: Vec<Value> = state
            .commands
            .iter()
            .filter(|c| {
                if let Some(cid) = command_id {
                    c.command_id == cid
                } else {
                    true
                }
            })
            .flat_map(|c| {
                c.instance_ids.iter().map(|iid| {
                    json!({
                        "CommandId": c.command_id,
                        "InstanceId": iid,
                        "DocumentName": c.document_name,
                        "Status": "Success",
                        "StatusDetails": "Success",
                        "RequestedDateTime": c.requested_date_time.timestamp_millis() as f64 / 1000.0,
                        "Comment": c.comment,
                    })
                })
            })
            .collect();

        let page = if next_token_offset < all_invocations.len() {
            &all_invocations[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let invocations: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "CommandInvocations": invocations });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    pub(super) fn cancel_command(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let command_id = body["CommandId"]
            .as_str()
            .ok_or_else(|| missing("CommandId"))?;
        validate_string_length("CommandId", command_id, 36, 36)?;

        let mut state = self.state.write();
        if let Some(cmd) = state
            .commands
            .iter_mut()
            .find(|c| c.command_id == command_id)
        {
            cmd.status = "Cancelled".to_string();
        }

        Ok(json_resp(json!({})))
    }

    // ===== Maintenance Window operations =====
}
