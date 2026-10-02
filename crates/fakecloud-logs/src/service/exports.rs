use http::StatusCode;
use serde_json::{json, Value};

use crate::validation::*;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

use super::LogsService;
use crate::state::ExportTask;

/// Gzip-encode a JSONL payload. CloudWatch Logs writes export task and
/// delivery output as gzip-compressed objects (`.gz`); downstream
/// consumers (Athena, S3 SELECT, custom readers) expect that wire shape.
fn gzip_jsonl(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).expect("gzip write");
    encoder.finish().expect("gzip finish")
}

/// Build the S3 key real CloudWatch uses for export task output:
/// `<destinationPrefix>/<exportTaskId>/<32-hex-hash>/000000.gz`. The
/// hash segment is unique per file so reruns of the same task don't
/// collide; we derive it from the stream name + completion timestamp.
fn export_object_key(prefix: &str, task_id: &str, stream_name: &str, ts: i64) -> String {
    use std::hash::{Hash, Hasher};
    let mut h1 = std::collections::hash_map::DefaultHasher::new();
    let mut h2 = std::collections::hash_map::DefaultHasher::new();
    stream_name.hash(&mut h1);
    ts.hash(&mut h1);
    task_id.hash(&mut h2);
    stream_name.hash(&mut h2);
    let hash = format!("{:016x}{:016x}", h1.finish(), h2.finish());
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        format!("{task_id}/{hash}/000000.gz")
    } else {
        format!("{prefix}/{task_id}/{hash}/000000.gz")
    }
}

/// Matching events of one log stream, captured for an export.
type StreamEvents = (String, Vec<crate::state::LogEvent>);

/// Set the status of `task_id` if it still holds `expected`. Returns false
/// when the task is gone or moved on (e.g. cancelled), so the job stops.
fn transition_export(
    state: &crate::state::SharedLogsState,
    account_id: &str,
    task_id: &str,
    expected: &str,
    code: &str,
    message: &str,
    completed: bool,
) -> bool {
    let mut accounts = state.write();
    let Some(st) = accounts.get_mut(account_id) else {
        return false;
    };
    let Some(task) = st.export_tasks.iter_mut().find(|t| t.task_id == task_id) else {
        return false;
    };
    if task.status_code != expected {
        return false;
    }
    task.status_code = code.to_string();
    task.status_message = message.to_string();
    if completed {
        task.completion_time = Some(chrono::Utc::now().timestamp_millis());
    }
    true
}

fn export_still_running(
    state: &crate::state::SharedLogsState,
    account_id: &str,
    task_id: &str,
) -> bool {
    state
        .read()
        .get(account_id)
        .and_then(|st| st.export_tasks.iter().find(|t| t.task_id == task_id))
        .is_some_and(|t| t.status_code == "RUNNING")
}

/// Execute one export task: PENDING -> RUNNING, copy the matching events
/// under a short read lock, then render, gzip and write one S3 object per
/// stream with no Logs lock held, and finish COMPLETED (or stop if the task
/// was cancelled meanwhile).
pub(crate) fn run_export_task(
    state: &crate::state::SharedLogsState,
    bus: &fakecloud_core::delivery::DeliveryBus,
    account_id: &str,
    task_id: &str,
) {
    if !transition_export(
        state,
        account_id,
        task_id,
        "PENDING",
        "RUNNING",
        "Task is running",
        false,
    ) {
        return;
    }

    // Snapshot the task parameters and matching events. Events are cloned
    // stream by stream under the read guard; nothing slow happens here.
    let (task, per_stream): (ExportTask, Vec<StreamEvents>) = {
        let accounts = state.read();
        let Some(st) = accounts.get(account_id) else {
            return;
        };
        let Some(task) = st
            .export_tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .cloned()
        else {
            return;
        };
        let mut per_stream = Vec::new();
        if task.from_time < task.to_time {
            if let Some(group) = st.log_groups.get(&task.log_group_name) {
                for (stream_name, stream) in &group.log_streams {
                    if let Some(ref prefix) = task.log_stream_name_prefix {
                        if !stream_name.starts_with(prefix.as_str()) {
                            continue;
                        }
                    }
                    let matches: Vec<crate::state::LogEvent> = stream
                        .events
                        .iter()
                        .filter(|e| e.timestamp >= task.from_time && e.timestamp < task.to_time)
                        .cloned()
                        .collect();
                    if !matches.is_empty() {
                        per_stream.push((stream_name.clone(), matches));
                    }
                }
            }
        }
        (task, per_stream)
    };

    let written_at = chrono::Utc::now().timestamp_millis();
    for (stream_name, events) in &per_stream {
        if !export_still_running(state, account_id, task_id) {
            return;
        }
        let mut data = String::new();
        for event in events {
            let line = serde_json::to_string(&json!({
                "timestamp": event.timestamp,
                "message": event.message,
            }))
            .unwrap();
            data.push_str(&line);
            data.push('\n');
        }
        // CloudWatch Logs export tasks deliver gzip-compressed JSONL under
        // `<prefix>/<taskId>/<hash>/000000.gz`. We mirror that shape so
        // downstream readers (Athena, S3 SELECT, custom decompressors) work
        // without changes.
        let s3_key = export_object_key(&task.destination_prefix, task_id, stream_name, written_at);
        let body = gzip_jsonl(data.as_bytes());
        if bus
            .put_object_to_s3(
                account_id,
                &task.destination,
                &s3_key,
                body.clone(),
                Some("application/x-gzip"),
            )
            .is_err()
        {
            let fallback_key = format!("{}/{s3_key}", task.destination);
            let mut accounts = state.write();
            if let Some(st) = accounts.get_mut(account_id) {
                st.export_storage.insert(fallback_key, body);
            }
        }
    }

    transition_export(
        state,
        account_id,
        task_id,
        "RUNNING",
        "COMPLETED",
        "Completed successfully",
        true,
    );
}

impl LogsService {
    // ---- Export Tasks ----

    pub(crate) fn create_export_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        if !state.log_groups.contains_key(&log_group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            ));
        }
        drop(accounts);

        let task_name = body["taskName"].as_str().map(|s| s.to_string());
        let log_stream_name_prefix = body["logStreamNamePrefix"].as_str().map(|s| s.to_string());

        let task_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp_millis();

        // Record the task as PENDING and answer straight away, as AWS does:
        // the export itself (event copy, gzip, S3 writes) runs as a job that
        // DescribeExportTasks reports moving PENDING -> RUNNING -> COMPLETED.
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            state.export_tasks.push(ExportTask {
                task_id: task_id.clone(),
                task_name,
                log_group_name,
                log_stream_name_prefix,
                from_time,
                to_time,
                destination,
                destination_prefix,
                status_code: "PENDING".to_string(),
                status_message: "Task is pending".to_string(),
                creation_time: now,
                completion_time: None,
            });
        }
        self.spawn_export_task(req.account_id.clone(), task_id.clone());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "taskId": task_id })).unwrap(),
        ))
    }

    /// Run export task `task_id` off the request path. On a Tokio runtime the
    /// job runs on the blocking pool and the result is persisted when it
    /// finishes; with no runtime (synchronous callers) it runs inline.
    pub(crate) fn spawn_export_task(&self, account_id: String, task_id: String) {
        let state = self.state.clone();
        let bus = self.delivery_bus.clone();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            run_export_task(&state, &bus, &account_id, &task_id);
            return;
        };
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        handle.spawn(async move {
            let job_state = state.clone();
            let joined = tokio::task::spawn_blocking(move || {
                run_export_task(&job_state, &bus, &account_id, &task_id)
            })
            .await;
            if let Err(error) = joined {
                tracing::error!(%error, "logs export task panicked");
            }
            if let Err(error) = super::save_logs_state(&state, store, &lock).await {
                tracing::error!(%error, "failed to persist Logs state after export task");
            }
        });
    }

    /// Restart the export tasks a previous process left PENDING or RUNNING
    /// (it stopped before they finished). Without this they would report an
    /// in-flight status forever after a restart.
    pub fn resume_interrupted_export_tasks(&self) {
        let interrupted: Vec<(String, String)> = {
            let mut accounts = self.state.write();
            let mut out = Vec::new();
            for (account_id, state) in accounts.iter_mut() {
                for task in &mut state.export_tasks {
                    if task.status_code == "PENDING" || task.status_code == "RUNNING" {
                        task.status_code = "PENDING".to_string();
                        task.status_message = "Task is pending".to_string();
                        out.push((account_id.to_string(), task.task_id.clone()));
                    }
                }
            }
            out
        };
        for (account_id, task_id) in interrupted {
            self.spawn_export_task(account_id, task_id);
        }
    }

    pub(crate) fn describe_export_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Real CloudWatch Logs returns an empty `exportTasks` array for an
        // unknown taskId — `DescribeExportTasks` doesn't declare
        // `ResourceNotFoundException` in its Smithy error union. The filter
        // below narrows by taskId.

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
                let mut exec_info = json!({ "creationTime": t.creation_time });
                if let Some(completion) = t.completion_time {
                    exec_info["completionTime"] = json!(completion);
                }
                obj["executionInfo"] = exec_info;
                obj
            })
            .collect();

        // Apply the `limit` (AWS caps DescribeExportTasks at 50 per page) and
        // round-trip a `nextToken` — previously both were validated then
        // ignored, so every task was returned on one page.
        let (page, next_token) = super::paginate_offset(
            &tasks,
            body["limit"].as_i64(),
            50,
            body["nextToken"].as_str(),
        );
        let mut result = json!({ "exportTasks": page });
        if let Some(token) = next_token {
            result["nextToken"] = json!(token);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    pub(crate) fn cancel_export_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let task_id = body["taskId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "taskId is required",
            )
        })?;

        validate_string_length("taskId", task_id, 1, 512)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        // Only an export that has not finished can be cancelled; AWS rejects
        // the rest with InvalidOperationException. A running job notices the
        // CANCELLED status and stops without overwriting it.
        if task.status_code != "PENDING" && task.status_code != "RUNNING" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidOperationException",
                format!(
                    "The specified export task has already finished (status {}).",
                    task.status_code
                ),
            ));
        }
        task.status_code = "CANCELLED".to_string();
        task.status_message = "Cancelled by user".to_string();
        task.completion_time = Some(chrono::Utc::now().timestamp_millis());

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    /// Internal action: returns data from the export storage for testing.
    /// Request body: `{"keyPrefix": "bucket/prefix"}` — returns all matching entries.
    pub(crate) fn get_exported_data(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_prefix = body["keyPrefix"].as_str().unwrap_or("");

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let entries: Vec<Value> = state
            .export_storage
            .iter()
            .filter(|(k, _)| k.starts_with(key_prefix))
            .map(|(k, v)| {
                // Stored payloads are gzipped JSONL on the AWS path; for
                // this introspection endpoint we transparently decompress
                // when we recognize the gzip magic so callers keep getting
                // raw text back.
                let data = if v.len() >= 2 && v[0] == 0x1f && v[1] == 0x8b {
                    use flate2::read::GzDecoder;
                    use std::io::Read;
                    let mut out = String::new();
                    GzDecoder::new(&v[..])
                        .read_to_string(&mut out)
                        .unwrap_or_default();
                    out
                } else {
                    String::from_utf8_lossy(v).to_string()
                };
                json!({
                    "key": k,
                    "data": data,
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
    use crate::state::ExportTask;
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let task_id = body["taskId"].as_str().unwrap();

        let req = make_request("DescribeExportTasks", json!({ "taskId": task_id }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let task_id = body["taskId"].as_str().unwrap();

        let req = make_request("DescribeExportTasks", json!({ "taskId": task_id }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let task_id = body["taskId"].as_str().unwrap();

        // Verify task is COMPLETED
        let req = make_request("DescribeExportTasks", json!({ "taskId": task_id }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let entries = body["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "Should have one export entry");
        let data = entries[0]["data"].as_str().unwrap();
        assert!(data.contains("export event 1"));
        assert!(data.contains("export event 2"));
        assert!(data.contains("export event 3"));
    }

    // ---- Z2: real S3 writes via DeliveryBus ----

    type S3PutRecord = (String, String, String, Vec<u8>, Option<String>);

    #[derive(Default)]
    struct S3Recorder {
        // (account, bucket, key, body, content_type)
        objects: parking_lot::Mutex<Vec<S3PutRecord>>,
    }

    impl fakecloud_core::delivery::S3Delivery for S3Recorder {
        fn put_object(
            &self,
            account_id: &str,
            bucket: &str,
            key: &str,
            body: Vec<u8>,
            content_type: Option<&str>,
        ) -> Result<(), String> {
            self.objects.lock().push((
                account_id.to_string(),
                bucket.to_string(),
                key.to_string(),
                body,
                content_type.map(|s| s.to_string()),
            ));
            Ok(())
        }

        fn get_object(
            &self,
            _account_id: &str,
            bucket: &str,
            key: &str,
        ) -> Result<Vec<u8>, String> {
            self.objects
                .lock()
                .iter()
                .find(|(_, b, k, _, _)| b == bucket && k == key)
                .map(|(_, _, _, body, _)| body.clone())
                .ok_or_else(|| format!("not found: {bucket}/{key}"))
        }
    }

    fn make_service_with_s3(recorder: std::sync::Arc<S3Recorder>) -> crate::service::LogsService {
        use fakecloud_core::delivery::DeliveryBus;
        let state = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let bus = DeliveryBus::new().with_s3(recorder);
        crate::service::LogsService::new(state, std::sync::Arc::new(bus))
    }

    #[test]
    fn create_export_task_writes_events_to_s3() {
        let recorder = std::sync::Arc::new(S3Recorder::default());
        let svc = make_service_with_s3(recorder.clone());
        create_group(&svc, "g");
        create_stream(&svc, "g", "s1");

        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "g",
                "logStreamName": "s1",
                "logEvents": [
                    { "timestamp": now, "message": "evt-a" },
                    { "timestamp": now + 1, "message": "evt-b" },
                    { "timestamp": now + 2, "message": "evt-c" },
                ],
            }),
        );
        svc.put_log_events(&req).unwrap();

        let req = make_request(
            "CreateExportTask",
            json!({
                "logGroupName": "g",
                "from": now - 1,
                "to": now + 100,
                "destination": "exp-bucket",
                "destinationPrefix": "p",
            }),
        );
        svc.create_export_task(&req).unwrap();

        let objects = recorder.objects.lock();
        assert_eq!(objects.len(), 1, "expected one S3 object");
        let (_, bucket, key, body, content_type) = &objects[0];
        assert_eq!(bucket, "exp-bucket");
        // AWS-format key: <prefix>/<taskId>/<hash>/000000.gz
        assert!(key.starts_with("p/"), "key should start with prefix: {key}");
        assert!(
            key.ends_with("/000000.gz"),
            "key should end with .gz: {key}"
        );
        let segments: Vec<&str> = key.split('/').collect();
        assert_eq!(segments.len(), 4, "expected 4 segments in {key}");
        assert_eq!(content_type.as_deref(), Some("application/x-gzip"));
        let text = decompress_gzip(body);
        let lines: Vec<&str> = text.trim().lines().collect();
        assert_eq!(lines.len(), 3);
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["timestamp"].as_i64().unwrap(), now);
        assert_eq!(first["message"].as_str().unwrap(), "evt-a");
    }

    fn decompress_gzip(body: &[u8]) -> String {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut out = String::new();
        GzDecoder::new(body).read_to_string(&mut out).unwrap();
        out
    }

    #[test]
    fn create_export_task_filters_by_time_range() {
        let recorder = std::sync::Arc::new(S3Recorder::default());
        let svc = make_service_with_s3(recorder.clone());
        create_group(&svc, "g");
        create_stream(&svc, "g", "s1");

        // Use real-clock timestamps so PutLogEvents doesn't reject them
        // as too old; offsets simulate t=1/5/10.
        let base = chrono::Utc::now().timestamp_millis();
        let t_low = base + 1;
        let t_mid = base + 5;
        let t_high = base + 10;
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "g",
                "logStreamName": "s1",
                "logEvents": [
                    { "timestamp": t_low, "message": "low" },
                    { "timestamp": t_mid, "message": "mid" },
                    { "timestamp": t_high, "message": "high" },
                ],
            }),
        );
        svc.put_log_events(&req).unwrap();

        let req = make_request(
            "CreateExportTask",
            json!({
                "logGroupName": "g",
                "from": base + 3,
                "to": base + 8,
                "destination": "tr-bucket",
                "destinationPrefix": "p",
            }),
        );
        svc.create_export_task(&req).unwrap();

        let objects = recorder.objects.lock();
        assert_eq!(objects.len(), 1);
        let body = decompress_gzip(&objects[0].3);
        assert!(body.contains("\"mid\""));
        assert!(!body.contains("\"low\""));
        assert!(!body.contains("\"high\""));
    }

    #[test]
    fn create_export_task_marks_task_completed() {
        let recorder = std::sync::Arc::new(S3Recorder::default());
        let svc = make_service_with_s3(recorder);
        create_group(&svc, "g");
        create_stream(&svc, "g", "s1");

        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "g",
                "logStreamName": "s1",
                "logEvents": [{ "timestamp": now, "message": "x" }],
            }),
        );
        svc.put_log_events(&req).unwrap();

        let req = make_request(
            "CreateExportTask",
            json!({
                "logGroupName": "g",
                "from": now - 1,
                "to": now + 1000,
                "destination": "tc-bucket",
                "destinationPrefix": "p",
            }),
        );
        let resp = svc.create_export_task(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let task_id = body["taskId"].as_str().unwrap();

        let req = make_request("DescribeExportTasks", json!({ "taskId": task_id }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let task = &body["exportTasks"][0];
        assert_eq!(task["status"]["code"].as_str().unwrap(), "COMPLETED");
        let exec = &task["executionInfo"];
        assert!(exec["creationTime"].as_i64().unwrap() > 0);
        assert!(exec["completionTime"].as_i64().unwrap() > 0);
    }

    #[test]
    fn create_delivery_to_s3_destination_records_target() {
        let recorder = std::sync::Arc::new(S3Recorder::default());
        let svc = make_service_with_s3(recorder);
        create_group(&svc, "del-grp");

        let req = make_request(
            "DescribeLogGroups",
            json!({ "logGroupNamePrefix": "del-grp" }),
        );
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let group_arn = body["logGroups"][0]["arn"].as_str().unwrap().to_string();

        let req = make_request(
            "PutDeliverySource",
            json!({
                "name": "ds-src",
                "resourceArn": group_arn,
                "logType": "APPLICATION_LOGS",
            }),
        );
        svc.put_delivery_source(&req).unwrap();

        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "ds-dest",
                "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::my-delivery-bucket"
                }
            }),
        );
        svc.put_delivery_destination(&req).unwrap();

        let req = make_request("GetDeliveryDestination", json!({ "name": "ds-dest" }));
        let resp = svc.get_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let dest_arn = body["deliveryDestination"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        let req = make_request(
            "CreateDelivery",
            json!({
                "deliverySourceName": "ds-src",
                "deliveryDestinationArn": dest_arn,
            }),
        );
        let resp = svc.create_delivery(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["delivery"]["deliveryDestinationType"], "S3");
    }

    #[test]
    fn put_log_events_after_delivery_creates_s3_objects() {
        let recorder = std::sync::Arc::new(S3Recorder::default());
        let svc = make_service_with_s3(recorder.clone());
        create_group(&svc, "live-grp");
        create_stream(&svc, "live-grp", "s1");

        let req = make_request(
            "DescribeLogGroups",
            json!({ "logGroupNamePrefix": "live-grp" }),
        );
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let group_arn = body["logGroups"][0]["arn"].as_str().unwrap().to_string();

        let req = make_request(
            "PutDeliverySource",
            json!({
                "name": "live-src",
                "resourceArn": group_arn,
                "logType": "APPLICATION_LOGS",
            }),
        );
        svc.put_delivery_source(&req).unwrap();

        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "live-dest",
                "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::live-bucket"
                }
            }),
        );
        svc.put_delivery_destination(&req).unwrap();

        let req = make_request("GetDeliveryDestination", json!({ "name": "live-dest" }));
        let resp = svc.get_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let dest_arn = body["deliveryDestination"]["arn"]
            .as_str()
            .unwrap()
            .to_string();

        let req = make_request(
            "CreateDelivery",
            json!({
                "deliverySourceName": "live-src",
                "deliveryDestinationArn": dest_arn,
            }),
        );
        svc.create_delivery(&req).unwrap();

        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "live-grp",
                "logStreamName": "s1",
                "logEvents": [
                    { "timestamp": now, "message": "live-a" },
                    { "timestamp": now + 1, "message": "live-b" },
                ],
            }),
        );
        svc.put_log_events(&req).unwrap();

        let objects = recorder.objects.lock();
        assert!(!objects.is_empty(), "expected delivery to write S3 objects");
        let (_, bucket, key, body, content_type) = &objects[0];
        assert_eq!(bucket, "live-bucket");
        assert!(key.ends_with(".gz"), "delivery key should be .gz: {key}");
        assert_eq!(content_type.as_deref(), Some("application/x-gzip"));
        let text = decompress_gzip(body);
        assert!(text.contains("live-a"));
        assert!(text.contains("live-b"));
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let entries = body["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        let data = entries[0]["data"].as_str().unwrap();
        assert!(data.contains("web event"));
        assert!(!data.contains("api event"));
    }

    #[test]
    fn describe_export_tasks_applies_limit_and_next_token() {
        let svc = make_service();
        create_group(&svc, "/export/paged");
        let now = chrono::Utc::now().timestamp_millis();
        for i in 0..3 {
            let req = make_request(
                "CreateExportTask",
                json!({
                    "logGroupName": "/export/paged",
                    "from": now - 1000,
                    "to": now + 10000,
                    "destination": format!("bucket-{i}"),
                }),
            );
            svc.create_export_task(&req).unwrap();
        }

        // Page 1: limit 2 -> two tasks plus a nextToken.
        let req = make_request("DescribeExportTasks", json!({ "limit": 2 }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["exportTasks"].as_array().unwrap().len(), 2);
        let token = body["nextToken"].as_str().unwrap().to_string();

        // Page 2: the remaining task, no further token.
        let req = make_request(
            "DescribeExportTasks",
            json!({ "limit": 2, "nextToken": token }),
        );
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["exportTasks"].as_array().unwrap().len(), 1);
        assert!(body["nextToken"].as_str().is_none());
    }

    #[test]
    fn describe_export_tasks_unknown_id_returns_empty_list() {
        // Real CloudWatch Logs returns an empty `exportTasks` array for an
        // unknown taskId; `DescribeExportTasks` doesn't declare
        // `ResourceNotFoundException` in its Smithy model.
        let svc = make_service();
        let req = make_request("DescribeExportTasks", json!({ "taskId": "does-not-exist" }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["exportTasks"].as_array().unwrap().len(), 0);
    }

    // ---- export task runs as a job ----

    /// S3 sink that checks, at the moment of each write, whether the Logs
    /// state lock is free. The export used to hold the Logs write lock across
    /// gzip + S3 IO; the job must hold no Logs lock while delivering.
    #[derive(Default)]
    struct LockProbe {
        state: std::sync::OnceLock<crate::state::SharedLogsState>,
        writes: parking_lot::Mutex<Vec<bool>>,
    }

    impl fakecloud_core::delivery::S3Delivery for LockProbe {
        fn put_object(
            &self,
            _account_id: &str,
            _bucket: &str,
            _key: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<(), String> {
            let free = self
                .state
                .get()
                .map(|s| s.try_write().is_some())
                .unwrap_or(false);
            self.writes.lock().push(free);
            Ok(())
        }

        fn get_object(&self, _: &str, _: &str, _: &str) -> Result<Vec<u8>, String> {
            Err("unused".into())
        }
    }

    fn put_one_event(svc: &crate::service::LogsService, group: &str, stream: &str) -> i64 {
        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": group,
                "logStreamName": stream,
                "logEvents": [{ "timestamp": now, "message": "m" }],
            }),
        );
        svc.put_log_events(&req).unwrap();
        now
    }

    fn task_status(svc: &crate::service::LogsService, task_id: &str) -> Value {
        let req = make_request("DescribeExportTasks", json!({ "taskId": task_id }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        body["exportTasks"][0].clone()
    }

    #[test]
    fn export_job_holds_no_logs_lock_while_writing_to_s3() {
        use fakecloud_core::delivery::DeliveryBus;
        let probe = std::sync::Arc::new(LockProbe::default());
        let state = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        probe.state.set(state.clone()).ok();
        let svc = crate::service::LogsService::new(
            state,
            std::sync::Arc::new(DeliveryBus::new().with_s3(probe.clone())),
        );
        create_group(&svc, "g");
        create_stream(&svc, "g", "s1");
        create_stream(&svc, "g", "s2");
        let now = put_one_event(&svc, "g", "s1");
        put_one_event(&svc, "g", "s2");
        let req = make_request(
            "CreateExportTask",
            json!({ "logGroupName": "g", "from": now - 1000, "to": now + 1000, "destination": "b" }),
        );
        svc.create_export_task(&req).unwrap();
        let writes = probe.writes.lock().clone();
        assert_eq!(writes.len(), 2);
        assert!(
            writes.iter().all(|free| *free),
            "Logs lock held during S3 write"
        );
    }

    #[test]
    fn cancelled_export_job_is_not_completed_by_the_worker() {
        let recorder = std::sync::Arc::new(S3Recorder::default());
        let svc = make_service_with_s3(recorder.clone());
        create_group(&svc, "g");
        create_stream(&svc, "g", "s1");
        let now = put_one_event(&svc, "g", "s1");
        // Stage a PENDING task the way CreateExportTask does, without
        // running it, then cancel before the worker picks it up.
        {
            let mut accounts = svc.state.write();
            let st = accounts.get_or_create("123456789012");
            st.export_tasks.push(ExportTask {
                task_id: "t-1".into(),
                task_name: None,
                log_group_name: "g".into(),
                log_stream_name_prefix: None,
                from_time: now - 1000,
                to_time: now + 1000,
                destination: "b".into(),
                destination_prefix: "p".into(),
                status_code: "PENDING".into(),
                status_message: String::new(),
                creation_time: now,
                completion_time: None,
            });
        }
        let req = make_request("CancelExportTask", json!({ "taskId": "t-1" }));
        svc.cancel_export_task(&req).unwrap();
        super::run_export_task(&svc.state, &svc.delivery_bus, "123456789012", "t-1");
        assert_eq!(task_status(&svc, "t-1")["status"]["code"], "CANCELLED");
        assert!(
            recorder.objects.lock().is_empty(),
            "cancelled task wrote output"
        );

        // A finished task can no longer be cancelled.
        let req = make_request("CancelExportTask", json!({ "taskId": "t-1" }));
        let Err(err) = svc.cancel_export_task(&req) else {
            panic!("cancel of a finished task must fail");
        };
        assert!(format!("{err:?}").contains("InvalidOperationException"));
    }

    #[test]
    fn completed_export_cannot_be_cancelled() {
        let svc = make_service();
        create_group(&svc, "g");
        let req = make_request(
            "CreateExportTask",
            json!({ "logGroupName": "g", "from": 0, "to": 10, "destination": "b" }),
        );
        let resp = svc.create_export_task(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let id = body["taskId"].as_str().unwrap().to_string();
        assert_eq!(task_status(&svc, &id)["status"]["code"], "COMPLETED");
        let req = make_request("CancelExportTask", json!({ "taskId": id }));
        let Err(err) = svc.cancel_export_task(&req) else {
            panic!("cancel of a finished task must fail");
        };
        assert!(format!("{err:?}").contains("InvalidOperationException"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn export_task_runs_in_background_and_completes() {
        let recorder = std::sync::Arc::new(S3Recorder::default());
        let svc = make_service_with_s3(recorder.clone());
        create_group(&svc, "g");
        create_stream(&svc, "g", "s1");
        let now = put_one_event(&svc, "g", "s1");
        let req = make_request(
            "CreateExportTask",
            json!({ "logGroupName": "g", "from": now - 1000, "to": now + 1000, "destination": "b" }),
        );
        let resp = svc.create_export_task(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let id = body["taskId"].as_str().unwrap().to_string();
        let mut code = String::new();
        for _ in 0..200 {
            code = task_status(&svc, &id)["status"]["code"]
                .as_str()
                .unwrap()
                .to_string();
            if code == "COMPLETED" {
                break;
            }
            assert!(
                code == "PENDING" || code == "RUNNING",
                "unexpected transitional status {code}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(code, "COMPLETED");
        assert!(task_status(&svc, &id)["executionInfo"]["completionTime"].is_i64());
        assert_eq!(recorder.objects.lock().len(), 1);
    }

    #[test]
    fn interrupted_export_tasks_are_resumed() {
        let recorder = std::sync::Arc::new(S3Recorder::default());
        let svc = make_service_with_s3(recorder.clone());
        create_group(&svc, "g");
        create_stream(&svc, "g", "s1");
        let now = put_one_event(&svc, "g", "s1");
        {
            let mut accounts = svc.state.write();
            let st = accounts.get_or_create("123456789012");
            st.export_tasks.push(ExportTask {
                task_id: "t-run".into(),
                task_name: None,
                log_group_name: "g".into(),
                log_stream_name_prefix: None,
                from_time: now - 1000,
                to_time: now + 1000,
                destination: "b".into(),
                destination_prefix: "p".into(),
                status_code: "RUNNING".into(),
                status_message: String::new(),
                creation_time: now,
                completion_time: None,
            });
        }
        svc.resume_interrupted_export_tasks();
        assert_eq!(task_status(&svc, "t-run")["status"]["code"], "COMPLETED");
        assert_eq!(recorder.objects.lock().len(), 1);
    }
}
