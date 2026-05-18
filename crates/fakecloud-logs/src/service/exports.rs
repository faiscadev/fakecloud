use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use crate::validation::*;

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

        // Collect matching events per stream. We render one S3 object per
        // log stream as JSONL ({"timestamp": ..., "message": ...}) so a
        // downstream reader sees each stream's events grouped together,
        // which mirrors how real CloudWatch export tasks emit objects.
        let mut per_stream: Vec<(String, Vec<crate::state::LogEvent>)> = Vec::new();
        {
            let accounts = self.state.read();
            let empty = crate::state::LogsState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            if from_time < to_time {
                if let Some(group) = state.log_groups.get(&log_group_name) {
                    for (stream_name, stream) in &group.log_streams {
                        if let Some(ref prefix) = log_stream_name_prefix {
                            if !stream_name.starts_with(prefix.as_str()) {
                                continue;
                            }
                        }
                        let matches: Vec<crate::state::LogEvent> = stream
                            .events
                            .iter()
                            .filter(|e| e.timestamp >= from_time && e.timestamp < to_time)
                            .cloned()
                            .collect();
                        if !matches.is_empty() {
                            per_stream.push((stream_name.clone(), matches));
                        }
                    }
                }
            }
        }

        let (status_code, status_message, completion_time) = if from_time < to_time {
            (
                "COMPLETED".to_string(),
                "Completed successfully".to_string(),
                Some(now),
            )
        } else {
            ("RUNNING".to_string(), "Task is running".to_string(), None)
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if from_time < to_time {
            for (stream_name, events) in &per_stream {
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
                // CloudWatch Logs export tasks deliver gzip-compressed
                // JSONL under `<prefix>/<taskId>/<hash>/000000.gz`. We
                // mirror that shape so downstream readers (Athena, S3
                // SELECT, custom decompressors) work without changes.
                let s3_key = export_object_key(&destination_prefix, &task_id, stream_name, now);
                let body = gzip_jsonl(data.as_bytes());
                if self
                    .delivery_bus
                    .put_object_to_s3(
                        &req.account_id,
                        &destination,
                        &s3_key,
                        body.clone(),
                        Some("application/x-gzip"),
                    )
                    .is_err()
                {
                    let fallback_key = format!("{destination}/{s3_key}");
                    state.export_storage.insert(fallback_key, body);
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
            creation_time: now,
            completion_time,
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

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "exportTasks": tasks })).unwrap(),
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
}
