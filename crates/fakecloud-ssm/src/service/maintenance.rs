use std::collections::HashMap;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{MaintenanceWindow, MaintenanceWindowTarget, MaintenanceWindowTask};

use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn create_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
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
        let description = body["Description"].as_str().map(|s| s.to_string());
        let schedule_timezone = body["ScheduleTimezone"].as_str().map(|s| s.to_string());
        let schedule_offset = body["ScheduleOffset"].as_i64();
        validate_optional_range_i64("ScheduleOffset", schedule_offset, 1, 6)?;
        let start_date = body["StartDate"].as_str().map(|s| s.to_string());
        let end_date = body["EndDate"].as_str().map(|s| s.to_string());

        let client_token = body["ClientToken"].as_str().map(|s| s.to_string());
        let tags: HashMap<String, String> = body["Tags"]
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

        let mut state = self.state.write();

        // Idempotency: if a window with the same ClientToken already exists, return it
        if let Some(ref token) = client_token {
            if let Some(existing) = state
                .maintenance_windows
                .values()
                .find(|mw| mw.client_token.as_deref() == Some(token))
            {
                return Ok(json_resp(json!({ "WindowId": existing.id })));
            }
        }

        let window_id = format!("mw-{}", &uuid::Uuid::new_v4().to_string()[..17]);

        let mw = MaintenanceWindow {
            id: window_id.clone(),
            name,
            schedule,
            duration,
            cutoff,
            allow_unassociated_targets,
            enabled: true,
            description,
            tags,
            targets: Vec::new(),
            tasks: Vec::new(),
            schedule_timezone,
            schedule_offset,
            start_date,
            end_date,
            client_token,
        };

        state.maintenance_windows.insert(window_id.clone(), mw);

        Ok(json_resp(json!({ "WindowId": window_id })))
    }

    pub(super) fn describe_maintenance_windows(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
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
                            "Name" => {
                                if !values.iter().any(|v| *v == mw.name) {
                                    return false;
                                }
                            }
                            "Enabled" => {
                                let enabled_str = if mw.enabled { "true" } else { "false" };
                                if !values.contains(&enabled_str) {
                                    return false;
                                }
                            }
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

        let page = if next_token_offset < all_windows.len() {
            &all_windows[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let windows: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "WindowIdentities": windows });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    pub(super) fn get_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let state = self.state.read();
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

        Ok(json_resp(resp))
    }

    pub(super) fn delete_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        validate_string_length("WindowId", window_id, 20, 20)?;

        let mut state = self.state.write();
        if state.maintenance_windows.remove(window_id).is_none() {
            return Err(mw_not_found(window_id));
        }

        Ok(json_resp(json!({ "WindowId": window_id })))
    }

    pub(super) fn update_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        validate_string_length("WindowId", window_id, 20, 20)?;
        validate_optional_string_length("Name", body["Name"].as_str(), 3, 128)?;
        validate_optional_string_length("Description", body["Description"].as_str(), 1, 128)?;
        validate_optional_string_length("Schedule", body["Schedule"].as_str(), 1, 256)?;
        validate_optional_range_i64("ScheduleOffset", body["ScheduleOffset"].as_i64(), 1, 6)?;
        validate_optional_range_i64("Duration", body["Duration"].as_i64(), 1, 24)?;
        validate_optional_range_i64("Cutoff", body["Cutoff"].as_i64(), 0, 23)?;

        let mut state = self.state.write();
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

        Ok(json_resp(resp))
    }

    pub(super) fn register_target_with_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
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
            &uuid::Uuid::new_v4().to_string().replace('-', "")
        );

        let mut state = self.state.write();
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

        Ok(json_resp(json!({ "WindowTargetId": target_id })))
    }

    pub(super) fn deregister_target_from_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let target_id = body["WindowTargetId"]
            .as_str()
            .ok_or_else(|| missing("WindowTargetId"))?;

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        mw.targets.retain(|t| t.window_target_id != target_id);

        Ok(json_resp(json!({
            "WindowId": window_id,
            "WindowTargetId": target_id,
        })))
    }

    pub(super) fn describe_maintenance_window_targets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let state = self.state.read();
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

        Ok(json_resp(json!({ "Targets": targets })))
    }

    pub(super) fn register_task_with_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
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

        let task_id = format!(
            "{}-{}",
            window_id,
            &uuid::Uuid::new_v4().to_string().replace('-', "")
        );

        let mut state = self.state.write();
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
        };
        mw.tasks.push(task);

        Ok(json_resp(json!({ "WindowTaskId": task_id })))
    }

    pub(super) fn deregister_task_from_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_id = body["WindowTaskId"]
            .as_str()
            .ok_or_else(|| missing("WindowTaskId"))?;

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        mw.tasks.retain(|t| t.window_task_id != task_id);

        Ok(json_resp(json!({
            "WindowId": window_id,
            "WindowTaskId": task_id,
        })))
    }

    pub(super) fn describe_maintenance_window_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let state = self.state.read();
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
                v
            })
            .collect();

        Ok(json_resp(json!({ "Tasks": tasks })))
    }

    // ===== Patch Baseline operations =====

    pub(super) fn update_maintenance_window_target(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let target_id = body["WindowTargetId"]
            .as_str()
            .ok_or_else(|| missing("WindowTargetId"))?;

        let mut state = self.state.write();
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

        Ok(json_resp(resp))
    }

    pub(super) fn update_maintenance_window_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_id = body["WindowTaskId"]
            .as_str()
            .ok_or_else(|| missing("WindowTaskId"))?;

        let mut state = self.state.write();
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

        Ok(json_resp(resp))
    }

    pub(super) fn get_maintenance_window_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_id = body["WindowTaskId"]
            .as_str()
            .ok_or_else(|| missing("WindowTaskId"))?;

        let state = self.state.read();
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

        Ok(json_resp(resp))
    }

    pub(super) fn get_maintenance_window_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;

        let state = self.state.read();
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

        Ok(json_resp(resp))
    }

    pub(super) fn get_maintenance_window_execution_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;
        let task_id = body["TaskId"].as_str().ok_or_else(|| missing("TaskId"))?;

        let state = self.state.read();
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

        Ok(json_resp(resp))
    }

    pub(super) fn get_maintenance_window_execution_task_invocation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;
        let task_id = body["TaskId"].as_str().ok_or_else(|| missing("TaskId"))?;
        let invocation_id = body["InvocationId"]
            .as_str()
            .ok_or_else(|| missing("InvocationId"))?;

        let state = self.state.read();
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

        Ok(json_resp(resp))
    }

    pub(super) fn describe_maintenance_window_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("WindowId", body["WindowId"].as_str(), 20, 20)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let state = self.state.read();
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

        let page = if next_token_offset < all.len() {
            &all[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let items: Vec<Value> = page.iter().take(max_results).cloned().collect();
        let mut resp = json!({ "WindowExecutions": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    pub(super) fn describe_maintenance_window_execution_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length(
            "WindowExecutionId",
            body["WindowExecutionId"].as_str(),
            36,
            36,
        )?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;

        let state = self.state.read();
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

        Ok(json_resp(json!({ "WindowExecutionTaskIdentities": tasks })))
    }

    pub(super) fn describe_maintenance_window_execution_task_invocations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length(
            "WindowExecutionId",
            body["WindowExecutionId"].as_str(),
            36,
            36,
        )?;
        validate_optional_string_length("TaskId", body["TaskId"].as_str(), 36, 36)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;
        let task_id = body["TaskId"].as_str().ok_or_else(|| missing("TaskId"))?;

        let state = self.state.read();
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

        Ok(json_resp(
            json!({ "WindowExecutionTaskInvocationIdentities": invocations }),
        ))
    }

    pub(super) fn describe_maintenance_window_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("WindowId", body["WindowId"].as_str(), 20, 20)?;
        validate_optional_enum(
            "ResourceType",
            body["ResourceType"].as_str(),
            &["INSTANCE", "RESOURCE_GROUP"],
        )?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, i64::MAX)?;
        Ok(json_resp(json!({ "ScheduledWindowExecutions": [] })))
    }

    pub(super) fn describe_maintenance_windows_for_target(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
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

        let state = self.state.read();
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

        Ok(json_resp(json!({ "WindowIdentities": windows })))
    }

    pub(super) fn cancel_maintenance_window_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;

        let mut state = self.state.write();
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

        Ok(json_resp(json!({ "WindowExecutionId": execution_id })))
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
