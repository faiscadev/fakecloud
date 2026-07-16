use std::collections::BTreeMap;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{MaintenanceWindow, MaintenanceWindowTarget, MaintenanceWindowTask, SsmState};

use super::{aws_400, missing, missing_with_code, SsmService};

/// Append the optional invocation/logging fields of a maintenance-window task
/// to a response object, so Get/Describe faithfully echo what Register stored.
fn append_task_invocation_fields(resp: &mut Value, task: &MaintenanceWindowTask) {
    if let Some(ref tip) = task.task_invocation_parameters {
        resp["TaskInvocationParameters"] = tip.clone();
    }
    if let Some(ref tp) = task.task_parameters {
        resp["TaskParameters"] = tp.clone();
    }
    if let Some(ref li) = task.logging_info {
        resp["LoggingInfo"] = li.clone();
    }
    if let Some(ref cb) = task.cutoff_behavior {
        resp["CutoffBehavior"] = json!(cb);
    }
}

/// All fields of a `CreateMaintenanceWindow` request, parsed and validated.
struct CreateMaintenanceWindowInput {
    name: String,
    schedule: String,
    duration: i64,
    cutoff: i64,
    allow_unassociated_targets: bool,
    description: Option<String>,
    schedule_timezone: Option<String>,
    schedule_offset: Option<i64>,
    start_date: Option<String>,
    end_date: Option<String>,
    client_token: Option<String>,
    tags: BTreeMap<String, String>,
}

impl CreateMaintenanceWindowInput {
    fn from_body(body: &Value) -> Result<Self, AwsServiceError> {
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("Name", &name, 3, 128)?;

        let schedule = body["Schedule"]
            .as_str()
            .ok_or_else(|| missing("Schedule"))?
            .to_string();
        validate_string_length("Schedule", &schedule, 1, 256)?;

        let duration = body["Duration"]
            .as_i64()
            .ok_or_else(|| missing("Duration"))?;
        validate_range_i64("Duration", duration, 1, 24)?;

        let cutoff = body["Cutoff"].as_i64().ok_or_else(|| missing("Cutoff"))?;
        validate_range_i64("Cutoff", cutoff, 0, 23)?;

        validate_required(
            "AllowUnassociatedTargets",
            &body["AllowUnassociatedTargets"],
        )?;
        let allow_unassociated_targets =
            body["AllowUnassociatedTargets"].as_bool().unwrap_or(false);

        validate_optional_string_length("Description", body["Description"].as_str(), 1, 128)?;
        validate_optional_string_length("ClientToken", body["ClientToken"].as_str(), 1, 64)?;
        let schedule_offset = body["ScheduleOffset"].as_i64();
        validate_optional_range_i64("ScheduleOffset", schedule_offset, 1, 6)?;

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

        Ok(Self {
            name,
            schedule,
            duration,
            cutoff,
            allow_unassociated_targets,
            description: body["Description"].as_str().map(|s| s.to_string()),
            schedule_timezone: body["ScheduleTimezone"].as_str().map(|s| s.to_string()),
            schedule_offset,
            start_date: body["StartDate"].as_str().map(|s| s.to_string()),
            end_date: body["EndDate"].as_str().map(|s| s.to_string()),
            client_token: body["ClientToken"].as_str().map(|s| s.to_string()),
            tags,
        })
    }
}

impl SsmService {
    pub(super) fn create_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let input = CreateMaintenanceWindowInput::from_body(&req.json_body())?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Idempotency: if a window with the same ClientToken already exists,
        // hand back its id without creating a new window.
        if let Some(ref token) = input.client_token {
            if let Some(existing) = state
                .maintenance_windows
                .values()
                .find(|mw| mw.client_token.as_deref() == Some(token))
            {
                return Ok(AwsResponse::ok_json(json!({ "WindowId": existing.id })));
            }
        }

        let window_id = format!("mw-{}", &uuid::Uuid::new_v4().to_string()[..17]);

        let mw = MaintenanceWindow {
            id: window_id.clone(),
            name: input.name,
            schedule: input.schedule,
            duration: input.duration,
            cutoff: input.cutoff,
            allow_unassociated_targets: input.allow_unassociated_targets,
            enabled: true,
            description: input.description,
            tags: input.tags,
            targets: Vec::new(),
            tasks: Vec::new(),
            schedule_timezone: input.schedule_timezone,
            schedule_offset: input.schedule_offset,
            start_date: input.start_date,
            end_date: input.end_date,
            client_token: input.client_token,
        };

        state.maintenance_windows.insert(window_id.clone(), mw);

        Ok(AwsResponse::ok_json(json!({ "WindowId": window_id })))
    }

    pub(super) fn describe_maintenance_windows(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let filters = body["Filters"].as_array();

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all_windows: Vec<Value> = state
            .maintenance_windows
            .values()
            .filter(|mw| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "Name" if !values.iter().any(|v| *v == mw.name) => {
                                return false;
                            }
                            "Enabled" => {
                                let enabled_str = if mw.enabled { "true" } else { "false" };
                                if !values.contains(&enabled_str) {
                                    return false;
                                }
                            }
                            // Unknown filter keys: AWS silently ignores them.
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|mw| {
                let mut v = json!({
                    "WindowId": mw.id,
                    "Name": mw.name,
                    "Schedule": mw.schedule,
                    "Duration": mw.duration,
                    "Cutoff": mw.cutoff,
                    "Enabled": mw.enabled,
                });
                if let Some(ref desc) = mw.description {
                    v["Description"] = json!(desc);
                }
                if let Some(ref tz) = mw.schedule_timezone {
                    v["ScheduleTimezone"] = json!(tz);
                }
                if let Some(offset) = mw.schedule_offset {
                    v["ScheduleOffset"] = json!(offset);
                }
                if let Some(ref sd) = mw.start_date {
                    v["StartDate"] = json!(sd);
                }
                if let Some(ref ed) = mw.end_date {
                    v["EndDate"] = json!(ed);
                }
                v
            })
            .collect();

        let (windows, next_token) =
            paginate_checked(&all_windows, body["NextToken"].as_str(), max_results)
                .map_err(|_| super::invalid_next_token())?;
        let mut resp = json!({ "WindowIdentities": windows });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let mut resp = json!({
            "WindowId": mw.id,
            "Name": mw.name,
            "Schedule": mw.schedule,
            "Duration": mw.duration,
            "Cutoff": mw.cutoff,
            "AllowUnassociatedTargets": mw.allow_unassociated_targets,
            "Enabled": mw.enabled,
        });
        if let Some(ref desc) = mw.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref tz) = mw.schedule_timezone {
            resp["ScheduleTimezone"] = json!(tz);
        }
        if let Some(offset) = mw.schedule_offset {
            resp["ScheduleOffset"] = json!(offset);
        }
        if let Some(ref sd) = mw.start_date {
            resp["StartDate"] = json!(sd);
        }
        if let Some(ref ed) = mw.end_date {
            resp["EndDate"] = json!(ed);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn delete_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // DeleteMaintenanceWindow is idempotent in real AWS for an unknown
        // (but well-formed) WindowId: "If the value passed for WindowId
        // doesn't have a maintenance window associated with it, you will
        // not see an error." Malformed inputs — missing field, wrong
        // length — are *not* covered by that idempotency: AWS validates
        // the shape's @length(min:20, max:20) at the wire layer and
        // rejects them. We mirror that: validate the input shape first,
        // then no-op-remove a well-formed but unknown id.
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        validate_string_length("WindowId", window_id, 20, 20)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.maintenance_windows.remove(window_id);
        Ok(AwsResponse::ok_json(json!({ "WindowId": window_id })))
    }

    pub(super) fn update_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing_with_code("WindowId", "DoesNotExistException"))?;
        // UpdateMaintenanceWindow declares `DoesNotExistException` +
        // `InternalServerError` — there is no ValidationException in its
        // Smithy errors list. So we don't pre-validate WindowId's @length;
        // any well-formed-but-unknown id (and any malformed id, since AWS
        // tags both as "not found" here) routes through the declared
        // DoesNotExistException below. This is what real AWS returns for
        // wrong-shape WindowIds on this op.
        validate_optional_string_length("Name", body["Name"].as_str(), 3, 128)?;
        validate_optional_string_length("Description", body["Description"].as_str(), 1, 128)?;
        validate_optional_string_length("Schedule", body["Schedule"].as_str(), 1, 256)?;
        validate_optional_range_i64("ScheduleOffset", body["ScheduleOffset"].as_i64(), 1, 6)?;
        validate_optional_range_i64("Duration", body["Duration"].as_i64(), 1, 24)?;
        validate_optional_range_i64("Cutoff", body["Cutoff"].as_i64(), 0, 23)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        if let Some(name) = body["Name"].as_str() {
            mw.name = name.to_string();
        }
        if let Some(schedule) = body["Schedule"].as_str() {
            mw.schedule = schedule.to_string();
        }
        if let Some(duration) = body["Duration"].as_i64() {
            mw.duration = duration;
        }
        if let Some(cutoff) = body["Cutoff"].as_i64() {
            mw.cutoff = cutoff;
        }
        if let Some(enabled) = body["Enabled"].as_bool() {
            mw.enabled = enabled;
        }
        if let Some(allow) = body["AllowUnassociatedTargets"].as_bool() {
            mw.allow_unassociated_targets = allow;
        }
        if body.get("Description").is_some() {
            mw.description = body["Description"].as_str().map(|s| s.to_string());
        }
        // ScheduleTimezone / ScheduleOffset / StartDate / EndDate were accepted
        // (ScheduleOffset even validated) but dropped on update (bug-audit
        // 2026-06-20, 1.24).
        if let Some(tz) = body["ScheduleTimezone"].as_str() {
            mw.schedule_timezone = Some(tz.to_string());
        }
        if let Some(offset) = body["ScheduleOffset"].as_i64() {
            mw.schedule_offset = Some(offset);
        }
        if let Some(sd) = body["StartDate"].as_str() {
            mw.start_date = Some(sd.to_string());
        }
        if let Some(ed) = body["EndDate"].as_str() {
            mw.end_date = Some(ed.to_string());
        }

        let mut resp = json!({
            "WindowId": mw.id,
            "Name": mw.name,
            "Schedule": mw.schedule,
            "Duration": mw.duration,
            "Cutoff": mw.cutoff,
            "AllowUnassociatedTargets": mw.allow_unassociated_targets,
            "Enabled": mw.enabled,
        });
        if let Some(ref desc) = mw.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref tz) = mw.schedule_timezone {
            resp["ScheduleTimezone"] = json!(tz);
        }
        if let Some(offset) = mw.schedule_offset {
            resp["ScheduleOffset"] = json!(offset);
        }
        if let Some(ref sd) = mw.start_date {
            resp["StartDate"] = json!(sd);
        }
        if let Some(ref ed) = mw.end_date {
            resp["EndDate"] = json!(ed);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn register_target_with_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let resource_type = body["ResourceType"]
            .as_str()
            .ok_or_else(|| missing("ResourceType"))?
            .to_string();
        let targets = body["Targets"]
            .as_array()
            .cloned()
            .ok_or_else(|| missing("Targets"))?;
        let name = body["Name"].as_str().map(|s| s.to_string());
        let description = body["Description"].as_str().map(|s| s.to_string());
        let owner_information = body["OwnerInformation"].as_str().map(|s| s.to_string());

        let target_id = format!(
            "{}-{}",
            window_id,
            uuid::Uuid::new_v4().to_string().replace('-', "")
        );

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let target = MaintenanceWindowTarget {
            window_target_id: target_id.clone(),
            window_id: window_id.to_string(),
            resource_type,
            targets,
            name,
            description,
            owner_information,
        };
        mw.targets.push(target);

        Ok(AwsResponse::ok_json(json!({ "WindowTargetId": target_id })))
    }

    pub(super) fn deregister_target_from_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let target_id = body["WindowTargetId"]
            .as_str()
            .ok_or_else(|| missing("WindowTargetId"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        mw.targets.retain(|t| t.window_target_id != target_id);

        Ok(AwsResponse::ok_json(json!({
            "WindowId": window_id,
            "WindowTargetId": target_id,
        })))
    }

    pub(super) fn describe_maintenance_window_targets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let targets: Vec<Value> = mw
            .targets
            .iter()
            .map(|t| {
                let mut v = json!({
                    "WindowId": t.window_id,
                    "WindowTargetId": t.window_target_id,
                    "ResourceType": t.resource_type,
                    "Targets": t.targets,
                });
                if let Some(ref name) = t.name {
                    v["Name"] = json!(name);
                }
                if let Some(ref desc) = t.description {
                    v["Description"] = json!(desc);
                }
                if let Some(ref oi) = t.owner_information {
                    v["OwnerInformation"] = json!(oi);
                }
                v
            })
            .collect();

        Ok(AwsResponse::ok_json(json!({ "Targets": targets })))
    }

    pub(super) fn register_task_with_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_arn = body["TaskArn"]
            .as_str()
            .ok_or_else(|| missing("TaskArn"))?
            .to_string();
        let task_type = body["TaskType"]
            .as_str()
            .ok_or_else(|| missing("TaskType"))?
            .to_string();
        let targets = body["Targets"].as_array().cloned().unwrap_or_default();
        let max_concurrency = body["MaxConcurrency"].as_str().map(|s| s.to_string());
        let max_errors = body["MaxErrors"].as_str().map(|s| s.to_string());
        let priority = body["Priority"].as_i64().unwrap_or(1);
        let service_role_arn = body["ServiceRoleArn"].as_str().map(|s| s.to_string());
        let name = body["Name"].as_str().map(|s| s.to_string());
        let description = body["Description"].as_str().map(|s| s.to_string());
        let task_invocation_parameters = body
            .get("TaskInvocationParameters")
            .filter(|v| !v.is_null())
            .cloned();
        let task_parameters = body.get("TaskParameters").filter(|v| !v.is_null()).cloned();
        let logging_info = body.get("LoggingInfo").filter(|v| !v.is_null()).cloned();
        let cutoff_behavior = body["CutoffBehavior"].as_str().map(|s| s.to_string());

        let task_id = format!(
            "{}-{}",
            window_id,
            uuid::Uuid::new_v4().to_string().replace('-', "")
        );

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let task = MaintenanceWindowTask {
            window_task_id: task_id.clone(),
            window_id: window_id.to_string(),
            task_arn,
            task_type,
            targets,
            max_concurrency,
            max_errors,
            priority,
            service_role_arn,
            name,
            description,
            task_invocation_parameters,
            task_parameters,
            logging_info,
            cutoff_behavior,
        };
        mw.tasks.push(task);

        Ok(AwsResponse::ok_json(json!({ "WindowTaskId": task_id })))
    }

    pub(super) fn deregister_task_from_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_id = body["WindowTaskId"]
            .as_str()
            .ok_or_else(|| missing("WindowTaskId"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        mw.tasks.retain(|t| t.window_task_id != task_id);

        Ok(AwsResponse::ok_json(json!({
            "WindowId": window_id,
            "WindowTaskId": task_id,
        })))
    }

    pub(super) fn describe_maintenance_window_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let tasks: Vec<Value> = mw
            .tasks
            .iter()
            .map(|t| {
                let mut v = json!({
                    "WindowId": t.window_id,
                    "WindowTaskId": t.window_task_id,
                    "TaskArn": t.task_arn,
                    "Type": t.task_type,
                    "Targets": t.targets,
                    "Priority": t.priority,
                });
                if let Some(ref mc) = t.max_concurrency {
                    v["MaxConcurrency"] = json!(mc);
                }
                if let Some(ref me) = t.max_errors {
                    v["MaxErrors"] = json!(me);
                }
                if let Some(ref sr) = t.service_role_arn {
                    v["ServiceRoleArn"] = json!(sr);
                }
                if let Some(ref name) = t.name {
                    v["Name"] = json!(name);
                }
                if let Some(ref desc) = t.description {
                    v["Description"] = json!(desc);
                }
                append_task_invocation_fields(&mut v, t);
                v
            })
            .collect();

        Ok(AwsResponse::ok_json(json!({ "Tasks": tasks })))
    }

    // ===== Patch Baseline operations =====

    pub(super) fn update_maintenance_window_target(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let target_id = body["WindowTargetId"]
            .as_str()
            .ok_or_else(|| missing("WindowTargetId"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let target = mw
            .targets
            .iter_mut()
            .find(|t| t.window_target_id == target_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Target {target_id} does not exist in window {window_id}"),
                )
            })?;

        if let Some(name) = body["Name"].as_str() {
            target.name = Some(name.to_string());
        }
        if body.get("Description").is_some() {
            target.description = body["Description"].as_str().map(|s| s.to_string());
        }
        if let Some(targets) = body["Targets"].as_array() {
            target.targets = targets.clone();
        }
        if body.get("OwnerInformation").is_some() {
            target.owner_information = body["OwnerInformation"].as_str().map(|s| s.to_string());
        }

        let mut resp = json!({
            "WindowId": window_id,
            "WindowTargetId": target_id,
            "Targets": target.targets,
        });
        if let Some(ref name) = target.name {
            resp["Name"] = json!(name);
        }
        if let Some(ref desc) = target.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref oi) = target.owner_information {
            resp["OwnerInformation"] = json!(oi);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn update_maintenance_window_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_id = body["WindowTaskId"]
            .as_str()
            .ok_or_else(|| missing("WindowTaskId"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let task = mw
            .tasks
            .iter_mut()
            .find(|t| t.window_task_id == task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Task {task_id} does not exist in window {window_id}"),
                )
            })?;

        if let Some(name) = body["Name"].as_str() {
            task.name = Some(name.to_string());
        }
        if body.get("Description").is_some() {
            task.description = body["Description"].as_str().map(|s| s.to_string());
        }
        if let Some(targets) = body["Targets"].as_array() {
            task.targets = targets.clone();
        }
        if let Some(task_arn) = body["TaskArn"].as_str() {
            task.task_arn = task_arn.to_string();
        }
        if let Some(mc) = body["MaxConcurrency"].as_str() {
            task.max_concurrency = Some(mc.to_string());
        }
        if let Some(me) = body["MaxErrors"].as_str() {
            task.max_errors = Some(me.to_string());
        }
        if let Some(p) = body["Priority"].as_i64() {
            task.priority = p;
        }
        // Previously dropped, so the old service role persisted (bug-hunt
        // 2026-06-24, 1.13).
        if let Some(role) = body["ServiceRoleArn"].as_str() {
            task.service_role_arn = Some(role.to_string());
        }
        if body.get("TaskInvocationParameters").is_some() {
            task.task_invocation_parameters = body
                .get("TaskInvocationParameters")
                .filter(|v| !v.is_null())
                .cloned();
        }
        if body.get("TaskParameters").is_some() {
            task.task_parameters = body.get("TaskParameters").filter(|v| !v.is_null()).cloned();
        }
        if body.get("LoggingInfo").is_some() {
            task.logging_info = body.get("LoggingInfo").filter(|v| !v.is_null()).cloned();
        }
        if let Some(cb) = body["CutoffBehavior"].as_str() {
            task.cutoff_behavior = Some(cb.to_string());
        }

        let mut resp = json!({
            "WindowId": window_id,
            "WindowTaskId": task_id,
            "TaskArn": task.task_arn,
            "TaskType": task.task_type,
            "Targets": task.targets,
            "Priority": task.priority,
        });
        if let Some(ref name) = task.name {
            resp["Name"] = json!(name);
        }
        if let Some(ref desc) = task.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref mc) = task.max_concurrency {
            resp["MaxConcurrency"] = json!(mc);
        }
        if let Some(ref me) = task.max_errors {
            resp["MaxErrors"] = json!(me);
        }
        if let Some(ref sra) = task.service_role_arn {
            resp["ServiceRoleArn"] = json!(sra);
        }
        append_task_invocation_fields(&mut resp, task);

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_maintenance_window_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_id = body["WindowTaskId"]
            .as_str()
            .ok_or_else(|| missing("WindowTaskId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let task = mw
            .tasks
            .iter()
            .find(|t| t.window_task_id == task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Task {task_id} does not exist in window {window_id}"),
                )
            })?;

        let mut resp = json!({
            "WindowId": window_id,
            "WindowTaskId": task_id,
            "TaskArn": task.task_arn,
            "TaskType": task.task_type,
            "Targets": task.targets,
            "Priority": task.priority,
        });
        if let Some(ref name) = task.name {
            resp["Name"] = json!(name);
        }
        if let Some(ref desc) = task.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref mc) = task.max_concurrency {
            resp["MaxConcurrency"] = json!(mc);
        }
        if let Some(ref me) = task.max_errors {
            resp["MaxErrors"] = json!(me);
        }
        if let Some(ref sra) = task.service_role_arn {
            resp["ServiceRoleArn"] = json!(sra);
        }
        append_task_invocation_fields(&mut resp, task);

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_maintenance_window_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let exec = state
            .maintenance_window_executions
            .iter()
            .find(|e| e.window_execution_id == execution_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Execution {execution_id} does not exist"),
                )
            })?;

        let mut resp = json!({
            "WindowExecutionId": exec.window_execution_id,
            "WindowId": exec.window_id,
            "Status": exec.status,
            "StartTime": exec.start_time.timestamp_millis() as f64 / 1000.0,
            "TaskIds": exec.tasks.iter().map(|t| &t.task_execution_id).collect::<Vec<_>>(),
        });
        if let Some(ref end) = exec.end_time {
            resp["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_maintenance_window_execution_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;
        let task_id = body["TaskId"].as_str().ok_or_else(|| missing("TaskId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let exec = state
            .maintenance_window_executions
            .iter()
            .find(|e| e.window_execution_id == execution_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Execution {execution_id} does not exist"),
                )
            })?;

        let task = exec
            .tasks
            .iter()
            .find(|t| t.task_execution_id == task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Task {task_id} does not exist in execution {execution_id}"),
                )
            })?;

        let mut resp = json!({
            "WindowExecutionId": execution_id,
            "TaskExecutionId": task.task_execution_id,
            "TaskArn": task.task_arn,
            "Type": task.task_type,
            "Status": task.status,
            "StartTime": task.start_time.timestamp_millis() as f64 / 1000.0,
        });
        if let Some(ref end) = task.end_time {
            resp["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_maintenance_window_execution_task_invocation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;
        let task_id = body["TaskId"].as_str().ok_or_else(|| missing("TaskId"))?;
        let invocation_id = body["InvocationId"]
            .as_str()
            .ok_or_else(|| missing("InvocationId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let exec = state
            .maintenance_window_executions
            .iter()
            .find(|e| e.window_execution_id == execution_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Execution {execution_id} does not exist"),
                )
            })?;

        let task = exec
            .tasks
            .iter()
            .find(|t| t.task_execution_id == task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Task {task_id} does not exist"),
                )
            })?;

        let inv = task
            .invocations
            .iter()
            .find(|i| i.invocation_id == invocation_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Invocation {invocation_id} does not exist"),
                )
            })?;

        let mut resp = json!({
            "WindowExecutionId": execution_id,
            "TaskExecutionId": task_id,
            "InvocationId": invocation_id,
            "Status": inv.status,
            "StartTime": inv.start_time.timestamp_millis() as f64 / 1000.0,
        });
        if let Some(ref end) = inv.end_time {
            resp["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
        }
        if let Some(ref eid) = inv.execution_id {
            resp["ExecutionId"] = json!(eid);
        }
        if let Some(ref p) = inv.parameters {
            resp["Parameters"] = json!(p);
        }
        if let Some(ref oi) = inv.owner_information {
            resp["OwnerInformation"] = json!(oi);
        }
        if let Some(ref wtid) = inv.window_target_id {
            resp["WindowTargetId"] = json!(wtid);
        }
        if let Some(ref sd) = inv.status_details {
            resp["StatusDetails"] = json!(sd);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn describe_maintenance_window_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("WindowId", body["WindowId"].as_str(), 20, 20)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all: Vec<Value> = state
            .maintenance_window_executions
            .iter()
            .filter(|e| e.window_id == window_id)
            .map(|e| {
                let mut v = json!({
                    "WindowId": e.window_id,
                    "WindowExecutionId": e.window_execution_id,
                    "Status": e.status,
                    "StartTime": e.start_time.timestamp_millis() as f64 / 1000.0,
                });
                if let Some(ref end) = e.end_time {
                    v["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
                }
                v
            })
            .collect();

        let (items, next_token) = paginate_checked(&all, body["NextToken"].as_str(), max_results)
            .map_err(|_| super::invalid_next_token())?;
        let mut resp = json!({ "WindowExecutions": items });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn describe_maintenance_window_execution_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // The op only declares DoesNotExistException + InternalServerError,
        // so missing-input / not-found cases all surface as
        // DoesNotExistException (the generic ValidationException is
        // not in this op's Smithy errors list).
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| aws_400("DoesNotExistException", "WindowExecutionId is required"))?;
        // WindowExecutionId is @length(36, 36) in the Smithy model. Anything
        // outside that range can't refer to a real execution; route it
        // through the declared DoesNotExistException rather than letting it
        // silently match no executions and return 200 OK with an empty list.
        if !(36..=36).contains(&execution_id.len()) {
            return Err(aws_400(
                "DoesNotExistException",
                format!("WindowExecutionId '{execution_id}' is not a valid execution id"),
            ));
        }
        // @range(10, 100) on MaintenanceWindowMaxResults — out-of-range
        // values can't refer to a real page boundary.
        if let Some(max) = body["MaxResults"].as_i64() {
            if !(10..=100).contains(&max) {
                return Err(aws_400(
                    "DoesNotExistException",
                    format!("MaxResults {max} is out of the allowed [10, 100] range"),
                ));
            }
        }

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let tasks: Vec<Value> = state
            .maintenance_window_executions
            .iter()
            .find(|e| e.window_execution_id == execution_id)
            .map(|e| {
                e.tasks
                    .iter()
                    .map(|t| {
                        let mut v = json!({
                            "WindowExecutionId": execution_id,
                            "TaskExecutionId": t.task_execution_id,
                            "TaskArn": t.task_arn,
                            "Type": t.task_type,
                            "Status": t.status,
                            "StartTime": t.start_time.timestamp_millis() as f64 / 1000.0,
                        });
                        if let Some(ref end) = t.end_time {
                            v["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
                        }
                        v
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(AwsResponse::ok_json(
            json!({ "WindowExecutionTaskIdentities": tasks }),
        ))
    }

    pub(super) fn describe_maintenance_window_execution_task_invocations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // See describe_maintenance_window_execution_tasks: only
        // DoesNotExistException is declared, so all missing-input cases
        // are routed through it.
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| aws_400("DoesNotExistException", "WindowExecutionId is required"))?;
        // WindowExecutionId is @length(36, 36) — anything else can't refer
        // to a real execution.
        if !(36..=36).contains(&execution_id.len()) {
            return Err(aws_400(
                "DoesNotExistException",
                format!("WindowExecutionId '{execution_id}' is not a valid execution id"),
            ));
        }
        let task_id = body["TaskId"]
            .as_str()
            .ok_or_else(|| aws_400("DoesNotExistException", "TaskId is required"))?;
        // MaintenanceWindowExecutionTaskId is @length(36, 36).
        if !(36..=36).contains(&task_id.len()) {
            return Err(aws_400(
                "DoesNotExistException",
                format!("TaskId '{task_id}' is not a valid task execution id"),
            ));
        }
        // @range(10, 100) on MaintenanceWindowMaxResults.
        if let Some(max) = body["MaxResults"].as_i64() {
            if !(10..=100).contains(&max) {
                return Err(aws_400(
                    "DoesNotExistException",
                    format!("MaxResults {max} is out of the allowed [10, 100] range"),
                ));
            }
        }

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let invocations: Vec<Value> = state
            .maintenance_window_executions
            .iter()
            .find(|e| e.window_execution_id == execution_id)
            .and_then(|e| e.tasks.iter().find(|t| t.task_execution_id == task_id))
            .map(|t| {
                t.invocations
                    .iter()
                    .map(|i| {
                        let mut v = json!({
                            "WindowExecutionId": execution_id,
                            "TaskExecutionId": task_id,
                            "InvocationId": i.invocation_id,
                            "Status": i.status,
                            "StartTime": i.start_time.timestamp_millis() as f64 / 1000.0,
                        });
                        if let Some(ref end) = i.end_time {
                            v["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
                        }
                        if let Some(ref eid) = i.execution_id {
                            v["ExecutionId"] = json!(eid);
                        }
                        v
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(AwsResponse::ok_json(
            json!({ "WindowExecutionTaskInvocationIdentities": invocations }),
        ))
    }

    pub(super) fn describe_maintenance_window_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("WindowId", body["WindowId"].as_str(), 20, 20)?;
        validate_optional_enum(
            "ResourceType",
            body["ResourceType"].as_str(),
            &["INSTANCE", "RESOURCE_GROUP"],
        )?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, i64::MAX)?;
        Ok(AwsResponse::ok_json(
            json!({ "ScheduledWindowExecutions": [] }),
        ))
    }

    pub(super) fn describe_maintenance_windows_for_target(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_enum(
            "ResourceType",
            body["ResourceType"].as_str(),
            &["INSTANCE", "RESOURCE_GROUP"],
        )?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, i64::MAX)?;
        let _resource_type = body["ResourceType"]
            .as_str()
            .ok_or_else(|| missing("ResourceType"))?;
        let targets = body["Targets"]
            .as_array()
            .ok_or_else(|| missing("Targets"))?;

        // Extract instance IDs from targets
        let target_instance_ids: Vec<&str> = targets
            .iter()
            .filter(|t| t["Key"].as_str() == Some("InstanceIds"))
            .flat_map(|t| {
                t["Values"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect();

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let windows: Vec<Value> = state
            .maintenance_windows
            .values()
            .filter(|mw| {
                if target_instance_ids.is_empty() {
                    return true;
                }
                mw.targets.iter().any(|t| {
                    t.targets.iter().any(|tgt| {
                        tgt["Key"].as_str() == Some("InstanceIds")
                            && tgt["Values"]
                                .as_array()
                                .map(|a| {
                                    a.iter().any(|v| {
                                        target_instance_ids.contains(&v.as_str().unwrap_or(""))
                                    })
                                })
                                .unwrap_or(false)
                    })
                })
            })
            .map(|mw| {
                json!({
                    "WindowId": mw.id,
                    "Name": mw.name,
                })
            })
            .collect();

        Ok(AwsResponse::ok_json(json!({ "WindowIdentities": windows })))
    }

    pub(super) fn cancel_maintenance_window_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let exec = state
            .maintenance_window_executions
            .iter_mut()
            .find(|e| e.window_execution_id == execution_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Execution {execution_id} does not exist"),
                )
            })?;

        exec.status = "CANCELLING".to_string();

        Ok(AwsResponse::ok_json(
            json!({ "WindowExecutionId": execution_id }),
        ))
    }

    // ── Patch Management Details ──────────────────────────────────
}

pub(super) fn mw_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "DoesNotExistException",
        format!("Maintenance window {id} does not exist"),
    )
}
