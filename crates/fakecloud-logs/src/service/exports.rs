use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{body_json, LogsService};
use crate::state::ExportTask;

impl LogsService {
    // ---- Export Tasks ----

    pub(crate) fn create_export_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupName is required",
                )
            })?
            .to_string();
        let from_time = body["from"].as_i64().unwrap_or(0);
        let to_time = body["to"].as_i64().unwrap_or(0);
        let destination = body["destination"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "destination is required",
                )
            })?
            .to_string();
        let destination_prefix = body["destinationPrefix"]
            .as_str()
            .unwrap_or("exportedlogs")
            .to_string();

        validate_string_length("logGroupName", &log_group_name, 1, 512)?;
        validate_optional_string_length("taskName", body["taskName"].as_str(), 1, 512)?;
        validate_optional_string_length(
            "logStreamNamePrefix",
            body["logStreamNamePrefix"].as_str(),
            1,
            512,
        )?;
        validate_string_length("destination", &destination, 1, 512)?;

        let state = self.state.read();
        if !state.log_groups.contains_key(&log_group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            ));
        }
        drop(state);

        let task_name = body["taskName"].as_str().map(|s| s.to_string());
        let log_stream_name_prefix = body["logStreamNamePrefix"].as_str().map(|s| s.to_string());

        let task_id = uuid::Uuid::new_v4().to_string();
        let (status_code, status_message) = if from_time < to_time {
            (
                "COMPLETED".to_string(),
                "Completed successfully".to_string(),
            )
        } else {
            ("active".to_string(), "Task is active".to_string())
        };

        // Collect matching events and write to export storage
        let mut state = self.state.write();
        if from_time < to_time {
            if let Some(group) = state.log_groups.get(&log_group_name) {
                let mut exported_lines: Vec<String> = Vec::new();
                for (stream_name, stream) in &group.log_streams {
                    // Apply stream name prefix filter if provided
                    if let Some(ref prefix) = log_stream_name_prefix {
                        if !stream_name.starts_with(prefix.as_str()) {
                            continue;
                        }
                    }
                    for event in &stream.events {
                        if event.timestamp >= from_time && event.timestamp < to_time {
                            exported_lines.push(event.message.clone());
                        }
                    }
                }
                if !exported_lines.is_empty() {
                    let export_key = format!(
                        "{}/{}/{}/{}",
                        destination, destination_prefix, log_group_name, task_id
                    );
                    let data = exported_lines.join("\n");
                    state.export_storage.insert(export_key, data.into_bytes());
                }
            }
        }

        state.export_tasks.push(ExportTask {
            task_id: task_id.clone(),
            task_name,
            log_group_name,
            log_stream_name_prefix,
            from_time,
            to_time,
            destination,
            destination_prefix,
            status_code,
            status_message,
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "taskId": task_id })).unwrap(),
        ))
    }

    pub(crate) fn describe_export_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let task_id_filter = body["taskId"].as_str();

        validate_optional_string_length("taskId", task_id_filter, 1, 512)?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "statusCode",
            &body["statusCode"],
            &[
                "CANCELLED",
                "COMPLETED",
                "FAILED",
                "PENDING",
                "PENDING_CANCEL",
                "RUNNING",
            ],
        )?;

        let state = self.state.read();

        if let Some(task_id) = task_id_filter {
            let task = state.export_tasks.iter().find(|t| t.task_id == task_id);
            if task.is_none() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The specified export task does not exist.",
                ));
            }
        }

        let tasks: Vec<Value> = state
            .export_tasks
            .iter()
            .filter(|t| {
                if let Some(tid) = task_id_filter {
                    t.task_id == tid
                } else {
                    true
                }
            })
            .map(|t| {
                let mut obj = json!({
                    "taskId": t.task_id,
                    "logGroupName": t.log_group_name,
                    "from": t.from_time,
                    "to": t.to_time,
                    "destination": t.destination,
                    "destinationPrefix": t.destination_prefix,
                    "status": {
                        "code": t.status_code,
                        "message": t.status_message,
                    },
                });
                if let Some(ref name) = t.task_name {
                    obj["taskName"] = json!(name);
                }
                if let Some(ref prefix) = t.log_stream_name_prefix {
                    obj["logStreamNamePrefix"] = json!(prefix);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "exportTasks": tasks })).unwrap(),
        ))
    }

    pub(crate) fn cancel_export_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let task_id = body["taskId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "taskId is required",
            )
        })?;

        validate_string_length("taskId", task_id, 1, 512)?;

        let mut state = self.state.write();
        let task = state
            .export_tasks
            .iter_mut()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The specified export task does not exist.",
                )
            })?;

        task.status_code = "CANCELLED".to_string();
        task.status_message = "Task was cancelled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    /// Internal action: returns data from the export storage for testing.
    /// Request body: `{"keyPrefix": "bucket/prefix"}` — returns all matching entries.
    pub(crate) fn get_exported_data(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_prefix = body["keyPrefix"].as_str().unwrap_or("");

        let state = self.state.read();
        let entries: Vec<Value> = state
            .export_storage
            .iter()
            .filter(|(k, _)| k.starts_with(key_prefix))
            .map(|(k, v)| {
                json!({
                    "key": k,
                    "data": String::from_utf8_lossy(v).to_string(),
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "entries": entries })).unwrap(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- create_export_task: taskName + logStreamNamePrefix stored ----

    #[test]
    fn create_export_task_stores_task_name_and_stream_prefix() {
        let svc = make_service();
        create_group(&svc, "grp");

        let req = make_request(
            "CreateExportTask",
            json!({
                "logGroupName": "grp",
                "from": 0,
                "to": 1000,
                "destination": "my-bucket",
                "taskName": "my-export",
                "logStreamNamePrefix": "web-",
            }),
        );
        let resp = svc.create_export_task(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let task_id = body["taskId"].as_str().unwrap();

        let req = make_request("DescribeExportTasks", json!({ "taskId": task_id }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let task = &body["exportTasks"][0];
        assert_eq!(task["taskName"].as_str().unwrap(), "my-export");
        assert_eq!(task["logStreamNamePrefix"].as_str().unwrap(), "web-");
    }

    #[test]
    fn create_export_task_omits_optional_fields_when_not_provided() {
        let svc = make_service();
        create_group(&svc, "grp");

        let req = make_request(
            "CreateExportTask",
            json!({
                "logGroupName": "grp",
                "from": 0,
                "to": 1000,
                "destination": "my-bucket",
            }),
        );
        let resp = svc.create_export_task(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let task_id = body["taskId"].as_str().unwrap();

        let req = make_request("DescribeExportTasks", json!({ "taskId": task_id }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let task = &body["exportTasks"][0];
        assert!(task.get("taskName").is_none() || task["taskName"].is_null());
        assert!(task.get("logStreamNamePrefix").is_none() || task["logStreamNamePrefix"].is_null());
    }

    // ---- Export task writes to storage ----

    #[test]
    fn logs_export_task_writes_to_s3() {
        let svc = make_service();
        create_group(&svc, "/export/test");
        create_stream(&svc, "/export/test", "stream-1");

        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/export/test",
                "logStreamName": "stream-1",
                "logEvents": [
                    { "timestamp": now, "message": "export event 1" },
                    { "timestamp": now + 1, "message": "export event 2" },
                    { "timestamp": now + 2, "message": "export event 3" },
                ],
            }),
        );
        svc.put_log_events(&req).unwrap();

        // Create export task
        let req = make_request(
            "CreateExportTask",
            json!({
                "logGroupName": "/export/test",
                "from": now - 1000,
                "to": now + 10000,
                "destination": "my-export-bucket",
                "destinationPrefix": "logs",
            }),
        );
        let resp = svc.create_export_task(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let task_id = body["taskId"].as_str().unwrap();

        // Verify task is COMPLETED
        let req = make_request("DescribeExportTasks", json!({ "taskId": task_id }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["exportTasks"][0]["status"]["code"].as_str().unwrap(),
            "COMPLETED"
        );

        // Verify data was written to export storage
        let req = make_request(
            "GetExportedData",
            json!({ "keyPrefix": "my-export-bucket/logs" }),
        );
        let resp = svc.get_exported_data(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let entries = body["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "Should have one export entry");
        let data = entries[0]["data"].as_str().unwrap();
        assert!(data.contains("export event 1"));
        assert!(data.contains("export event 2"));
        assert!(data.contains("export event 3"));
    }

    #[test]
    fn logs_export_task_applies_stream_prefix_filter() {
        let svc = make_service();
        create_group(&svc, "/export-filter/test");
        create_stream(&svc, "/export-filter/test", "web-server");
        create_stream(&svc, "/export-filter/test", "api-server");

        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/export-filter/test",
                "logStreamName": "web-server",
                "logEvents": [{ "timestamp": now, "message": "web event" }],
            }),
        );
        svc.put_log_events(&req).unwrap();

        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/export-filter/test",
                "logStreamName": "api-server",
                "logEvents": [{ "timestamp": now + 1, "message": "api event" }],
            }),
        );
        svc.put_log_events(&req).unwrap();

        let req = make_request(
            "CreateExportTask",
            json!({
                "logGroupName": "/export-filter/test",
                "from": now - 1000,
                "to": now + 10000,
                "destination": "filtered-bucket",
                "destinationPrefix": "prefix",
                "logStreamNamePrefix": "web-",
            }),
        );
        svc.create_export_task(&req).unwrap();

        let req = make_request(
            "GetExportedData",
            json!({ "keyPrefix": "filtered-bucket/prefix" }),
        );
        let resp = svc.get_exported_data(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let entries = body["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        let data = entries[0]["data"].as_str().unwrap();
        assert!(data.contains("web event"));
        assert!(!data.contains("api event"));
    }
}
