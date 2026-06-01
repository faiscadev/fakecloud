use std::collections::BTreeMap;
use std::time::Duration;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{SsmCommand, SsmCommandInvocation, SsmState};

use super::{missing, missing_with_code, remap_validation_to, SsmService};

/// Delay before a freshly submitted command flips from `Pending` to
/// `InProgress`. Real SSM takes a few seconds; we keep this small so
/// E2E tests don't drag.
const PENDING_TO_IN_PROGRESS: Duration = Duration::from_millis(500);

/// Delay between `InProgress` and the terminal `Success` state.
const IN_PROGRESS_TO_SUCCESS: Duration = Duration::from_millis(1500);

/// Map a machine-readable status to the friendlier `StatusDetails`
/// string AWS returns. Anything unrecognised passes through unchanged.
pub(crate) fn friendly_status_details(status: &str) -> String {
    match status {
        "Pending" => "Pending",
        "InProgress" => "In Progress",
        "Delayed" => "Delayed",
        "Success" => "Success",
        "Cancelled" => "Cancelled",
        "Cancelling" => "Cancelling",
        "Failed" => "Failed",
        "TimedOut" => "Timed Out",
        "AccessDenied" => "Access Denied",
        "DeliveryTimedOut" => "Delivery Timed Out",
        "ExecutionTimedOut" => "Execution Timed Out",
        "Incomplete" => "Incomplete",
        "NoInstancesInTag" => "No Instances In Tag",
        "LimitExceeded" => "Limit Exceeded",
        other => other,
    }
    .to_string()
}

/// Aggregate per-invocation statuses into a single command-level
/// status. Mirrors the rule AWS uses: any failure dominates a success,
/// any in-progress dominates pending. This keeps `ListCommands`
/// consistent with `ListCommandInvocations` even after admin force-fail.
pub(super) fn aggregate_command_status(invocations: &[SsmCommandInvocation]) -> String {
    if invocations.is_empty() {
        return "Pending".to_string();
    }
    let mut has_pending = false;
    let mut has_in_progress = false;
    let mut has_success = false;
    let mut failure: Option<&str> = None;
    for inv in invocations {
        match inv.status.as_str() {
            "Pending" => has_pending = true,
            "InProgress" | "Delayed" => has_in_progress = true,
            "Success" => has_success = true,
            // Any non-success terminal state is a "failure" for the
            // parent command. First one wins so we expose the most
            // informative reason.
            "Failed" | "TimedOut" | "Cancelled" | "Cancelling" | "AccessDenied"
            | "DeliveryTimedOut" | "ExecutionTimedOut" | "Incomplete" | "NoInstancesInTag"
            | "LimitExceeded"
                if failure.is_none() =>
            {
                failure = Some(inv.status.as_str());
            }
            _ => {}
        }
    }
    if let Some(f) = failure {
        return f.to_string();
    }
    if has_in_progress || (has_pending && has_success) {
        return "InProgress".to_string();
    }
    if has_pending {
        return "Pending".to_string();
    }
    if has_success {
        return "Success".to_string();
    }
    "Pending".to_string()
}

/// All fields of a `SendCommand` request, parsed and validated.
struct SendCommandInput {
    document_name: String,
    instance_ids: Vec<String>,
    targets: Vec<Value>,
    parameters: BTreeMap<String, Vec<String>>,
    comment: Option<String>,
    output_s3_bucket: Option<String>,
    output_s3_prefix: Option<String>,
    output_s3_region: Option<String>,
    timeout: Option<i64>,
    max_concurrency: Option<String>,
    max_errors: Option<String>,
    service_role: Option<String>,
    notification: Option<Value>,
    document_hash: Option<String>,
    document_hash_type: Option<String>,
}

impl SendCommandInput {
    fn from_body(body: &Value) -> Result<Self, AwsServiceError> {
        let document_name = body["DocumentName"]
            .as_str()
            .ok_or_else(|| missing("DocumentName"))?
            .to_string();

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
        let parameters: BTreeMap<String, Vec<String>> = body["Parameters"]
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

        Ok(Self {
            document_name,
            instance_ids,
            targets,
            parameters,
            comment: body["Comment"].as_str().map(|s| s.to_string()),
            output_s3_bucket: body["OutputS3BucketName"].as_str().map(|s| s.to_string()),
            output_s3_prefix: body["OutputS3KeyPrefix"].as_str().map(|s| s.to_string()),
            output_s3_region: body["OutputS3Region"].as_str().map(|s| s.to_string()),
            timeout: body["TimeoutSeconds"].as_i64(),
            max_concurrency: body["MaxConcurrency"].as_str().map(|s| s.to_string()),
            max_errors: body["MaxErrors"].as_str().map(|s| s.to_string()),
            service_role: body["ServiceRoleArn"].as_str().map(|s| s.to_string()),
            notification: body.get("NotificationConfig").cloned(),
            document_hash: body["DocumentHash"].as_str().map(|s| s.to_string()),
            document_hash_type: body["DocumentHashType"].as_str().map(|s| s.to_string()),
        })
    }
}

impl SsmService {
    pub(super) fn send_command(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let input = SendCommandInput::from_body(&req.json_body())?;

        let command_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        // Tag-based targets are not modelled in fakecloud, so we surface a
        // single placeholder instance for them. Direct InstanceIds requests
        // pass through unchanged.
        let effective_instance_ids = if input.instance_ids.is_empty() && !input.targets.is_empty() {
            vec!["i-placeholder".to_string()]
        } else {
            input.instance_ids.clone()
        };

        let expires = now + chrono::Duration::seconds(input.timeout.unwrap_or(3600));
        let invocations: Vec<SsmCommandInvocation> = effective_instance_ids
            .iter()
            .map(|iid| SsmCommandInvocation {
                instance_id: iid.clone(),
                status: "Pending".to_string(),
                status_details: friendly_status_details("Pending"),
                standard_output_content: String::new(),
                standard_error_content: String::new(),
                response_code: -1,
                requested_date_time: now,
                last_update_at: now,
            })
            .collect();

        let cmd = SsmCommand {
            command_id: command_id.clone(),
            document_name: input.document_name.clone(),
            instance_ids: effective_instance_ids.clone(),
            parameters: input.parameters.clone(),
            // Real SSM returns `Pending` on submit; a background task
            // flips this to `InProgress` and then `Success` after a
            // short delay so polling clients see the natural lifecycle.
            status: "Pending".to_string(),
            requested_date_time: now,
            expires_after: expires,
            comment: input.comment.clone(),
            output_s3_bucket_name: input.output_s3_bucket.clone(),
            output_s3_key_prefix: input.output_s3_prefix.clone(),
            output_s3_region: input.output_s3_region.clone(),
            timeout_seconds: input.timeout,
            service_role_arn: input.service_role.clone(),
            notification_config: input.notification.clone(),
            targets: input.targets.clone(),
            document_hash: input.document_hash.clone(),
            document_hash_type: input.document_hash_type.clone(),
            invocations,
        };

        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            state.commands.push(cmd);
        }

        // Spawn the lifecycle transition. Detached on purpose — clients
        // poll `GetCommandInvocation`; we don't await completion here.
        // When the test harness runs a `send_command` outside a tokio
        // runtime (e.g. plain `#[test]`), `try_current` returns `Err`
        // and the command stays `Pending` forever, which the unit tests
        // assert against directly.
        if tokio::runtime::Handle::try_current().is_ok() {
            let state_handle = self.state.clone();
            let account_id = req.account_id.clone();
            let cid = command_id.clone();
            tokio::spawn(async move {
                tokio::time::sleep(PENDING_TO_IN_PROGRESS).await;
                advance_pending_to_in_progress(&state_handle, &account_id, &cid);
                tokio::time::sleep(IN_PROGRESS_TO_SUCCESS).await;
                advance_in_progress_to_success(&state_handle, &account_id, &cid);
            });
        }

        let mut cmd_obj = json!({
            "CommandId": command_id,
            "DocumentName": input.document_name,
            "InstanceIds": effective_instance_ids,
            "Targets": input.targets,
            "Parameters": input.parameters,
            "Status": "Pending",
            "StatusDetails": friendly_status_details("Pending"),
            "RequestedDateTime": now.timestamp_millis() as f64 / 1000.0,
            "ExpiresAfter": expires.timestamp_millis() as f64 / 1000.0,
            "MaxConcurrency": input.max_concurrency.clone().unwrap_or_default(),
            "MaxErrors": input.max_errors.clone().unwrap_or_default(),
            "DeliveryTimedOutCount": 0,
        });
        if let Some(ref c) = input.comment {
            cmd_obj["Comment"] = json!(c);
        }
        if let Some(ref r) = input.output_s3_region {
            cmd_obj["OutputS3Region"] = json!(r);
        }
        if let Some(ref b) = input.output_s3_bucket {
            cmd_obj["OutputS3BucketName"] = json!(b);
        }
        if let Some(ref p) = input.output_s3_prefix {
            cmd_obj["OutputS3KeyPrefix"] = json!(p);
        }
        if let Some(t) = input.timeout {
            cmd_obj["TimeoutSeconds"] = json!(t);
        }

        Ok(AwsResponse::ok_json(json!({ "Command": cmd_obj })))
    }

    pub(super) fn list_commands(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // ListCommands declares InvalidCommandId, not ValidationException.
        validate_optional_string_length("CommandId", body["CommandId"].as_str(), 36, 36)
            .map_err(|e| remap_validation_to(e, "InvalidCommandId"))?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)
            .map_err(|e| remap_validation_to(e, "InvalidCommandId"))?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let command_id = body["CommandId"].as_str();
        let instance_id = body["InstanceId"].as_str();

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
                json!({
                    "CommandId": c.command_id,
                    "DocumentName": c.document_name,
                    "InstanceIds": c.instance_ids,
                    "Targets": c.targets,
                    "Parameters": c.parameters,
                    "Status": c.status,
                    "StatusDetails": friendly_status_details(&c.status),
                    "RequestedDateTime": c.requested_date_time.timestamp_millis() as f64 / 1000.0,
                    "ExpiresAfter": c.expires_after.timestamp_millis() as f64 / 1000.0,
                    "Comment": c.comment,
                    "OutputS3Region": c.output_s3_region,
                    "OutputS3BucketName": c.output_s3_bucket_name,
                    "OutputS3KeyPrefix": c.output_s3_key_prefix,
                    "DeliveryTimedOutCount": 0,
                })
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

        let (commands, next_token) =
            paginate_checked(&all_commands, body["NextToken"].as_str(), max_results)
                .map_err(|_| super::invalid_next_token())?;
        let mut resp = json!({ "Commands": commands });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_command_invocation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // GetCommandInvocation declares InvalidCommandId / InvalidInstanceId /
        // InvalidPluginName but not ValidationException.
        let command_id = body["CommandId"]
            .as_str()
            .ok_or_else(|| missing_with_code("CommandId", "InvalidCommandId"))?;
        validate_string_length("CommandId", command_id, 36, 36)
            .map_err(|e| remap_validation_to(e, "InvalidCommandId"))?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing_with_code("InstanceId", "InvalidInstanceId"))?;
        let plugin_name = body["PluginName"].as_str();
        validate_optional_string_length("PluginName", plugin_name, 4, 500)
            .map_err(|e| remap_validation_to(e, "InvalidPluginName"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let inv = cmd
            .invocations
            .iter()
            .find(|i| i.instance_id == instance_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvocationDoesNotExist",
                    "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation",
                )
            })?;

        let mut resp = json!({
            "CommandId": cmd.command_id,
            "InstanceId": instance_id,
            "DocumentName": cmd.document_name,
            "Status": inv.status,
            "StatusDetails": inv.status_details,
            "ResponseCode": inv.response_code,
            "StandardOutputContent": inv.standard_output_content,
            "StandardOutputUrl": "",
            "StandardErrorContent": inv.standard_error_content,
            "StandardErrorUrl": "",
        });
        if let Some(pn) = plugin_name {
            resp["PluginName"] = json!(pn);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn list_command_invocations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // ListCommandInvocations declares InvalidCommandId, not ValidationException.
        validate_optional_string_length("CommandId", body["CommandId"].as_str(), 36, 36)
            .map_err(|e| remap_validation_to(e, "InvalidCommandId"))?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)
            .map_err(|e| remap_validation_to(e, "InvalidCommandId"))?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let command_id = body["CommandId"].as_str();

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
                c.invocations.iter().map(|inv| {
                    json!({
                        "CommandId": c.command_id,
                        "InstanceId": inv.instance_id,
                        "DocumentName": c.document_name,
                        "Status": inv.status,
                        "StatusDetails": inv.status_details,
                        "RequestedDateTime": inv.requested_date_time.timestamp_millis() as f64 / 1000.0,
                        "Comment": c.comment,
                    })
                })
            })
            .collect();

        let (invocations, next_token) =
            paginate_checked(&all_invocations, body["NextToken"].as_str(), max_results)
                .map_err(|_| super::invalid_next_token())?;
        let mut resp = json!({ "CommandInvocations": invocations });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn cancel_command(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // CancelCommand declares InvalidCommandId / InvalidInstanceId, no
        // ValidationException.
        let command_id = body["CommandId"]
            .as_str()
            .ok_or_else(|| missing_with_code("CommandId", "InvalidCommandId"))?;
        validate_string_length("CommandId", command_id, 36, 36)
            .map_err(|e| remap_validation_to(e, "InvalidCommandId"))?;
        let instance_filter: Option<Vec<String>> = body["InstanceIds"].as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        });

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(cmd) = state
            .commands
            .iter_mut()
            .find(|c| c.command_id == command_id)
        {
            let now = Utc::now();
            for inv in cmd.invocations.iter_mut() {
                if let Some(filter) = &instance_filter {
                    if !filter.is_empty() && !filter.contains(&inv.instance_id) {
                        continue;
                    }
                }
                // Cancellation only applies before terminal states.
                if matches!(inv.status.as_str(), "Pending" | "InProgress" | "Delayed") {
                    inv.status = "Cancelled".to_string();
                    inv.status_details = friendly_status_details("Cancelled");
                    inv.last_update_at = now;
                }
            }
            cmd.status = aggregate_command_status(&cmd.invocations);
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ===== Maintenance Window operations =====
}

/// Flip pending invocations + parent command to `InProgress`. Skips any
/// invocation that has already moved into a terminal state via the
/// admin force-fail endpoint or `CancelCommand`.
fn advance_pending_to_in_progress(
    state: &crate::state::SharedSsmState,
    account_id: &str,
    command_id: &str,
) {
    let mut accounts = state.write();
    let st = accounts.get_or_create(account_id);
    let Some(cmd) = st.commands.iter_mut().find(|c| c.command_id == command_id) else {
        return;
    };
    let now = Utc::now();
    for inv in cmd.invocations.iter_mut() {
        if inv.status == "Pending" {
            inv.status = "InProgress".to_string();
            inv.status_details = friendly_status_details("InProgress");
            inv.last_update_at = now;
        }
    }
    cmd.status = aggregate_command_status(&cmd.invocations);
}

/// Flip in-flight invocations + parent command to `Success`. Same
/// terminal-state guard as the pending->in-progress hop.
fn advance_in_progress_to_success(
    state: &crate::state::SharedSsmState,
    account_id: &str,
    command_id: &str,
) {
    let mut accounts = state.write();
    let st = accounts.get_or_create(account_id);
    let Some(cmd) = st.commands.iter_mut().find(|c| c.command_id == command_id) else {
        return;
    };
    let now = Utc::now();
    for inv in cmd.invocations.iter_mut() {
        if matches!(inv.status.as_str(), "Pending" | "InProgress" | "Delayed") {
            inv.status = "Success".to_string();
            inv.status_details = friendly_status_details("Success");
            inv.response_code = 0;
            inv.last_update_at = now;
        }
    }
    cmd.status = aggregate_command_status(&cmd.invocations);
}
