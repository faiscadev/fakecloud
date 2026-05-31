// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl EventBridgeService {
    pub(super) fn create_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ArchiveName", &body["ArchiveName"])?;
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?
            .to_string();
        validate_string_length("archiveName", &name, 1, 48)?;
        validate_required("EventSourceArn", &body["EventSourceArn"])?;
        let event_source_arn = body["EventSourceArn"]
            .as_str()
            .ok_or_else(|| missing("EventSourceArn"))?
            .to_string();
        validate_string_length("eventSourceArn", &event_source_arn, 1, 1600)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_string_length("eventPattern", body["EventPattern"].as_str(), 0, 4096)?;
        if let Some(rd) = body["RetentionDays"].as_i64() {
            validate_range_i64("retentionDays", rd, 0, i64::MAX)?;
        }
        let description = body["Description"].as_str().map(|s| s.to_string());
        let event_pattern = body["EventPattern"].as_str().map(|s| s.to_string());
        let retention_days = body["RetentionDays"].as_i64().unwrap_or(0);

        // Validate event pattern if provided
        if let Some(ref pattern) = event_pattern {
            validate_event_pattern(pattern)?;
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Validate event bus exists
        let bus_name = state.resolve_bus_name(&event_source_arn);
        if !state.buses.contains_key(&bus_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Event bus {bus_name} does not exist."),
            ));
        }

        // Check duplicate
        if state.archives.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceAlreadyExistsException",
                format!("Archive {name} already exists."),
            ));
        }

        let now = Utc::now();
        let arn = format!(
            "arn:aws:events:{}:{}:archive/{}",
            req.region, state.account_id, name
        );

        let archive = Archive {
            name: name.clone(),
            arn: arn.clone(),
            event_source_arn: event_source_arn.clone(),
            description,
            event_pattern: event_pattern.clone(),
            retention_days,
            state: "ENABLED".to_string(),
            creation_time: now,
            event_count: 0,
            size_bytes: 0,
            events: Vec::new(),
        };
        state.archives.insert(name.clone(), archive);

        // Create the archive rule
        let rule_name = format!("Events-Archive-{name}");
        let rule_arn = format!(
            "arn:aws:events:{}:{}:rule/{}",
            req.region, state.account_id, rule_name
        );
        // Merge archive event pattern with replay-name filter
        let rule_event_pattern = {
            let mut merged = if let Some(ref ep) = event_pattern {
                serde_json::from_str::<Value>(ep).unwrap_or_else(|_| json!({}))
            } else {
                json!({})
            };
            if let Some(obj) = merged.as_object_mut() {
                obj.insert("replay-name".to_string(), json!([{"exists": false}]));
            }
            serde_json::to_string(&merged).unwrap_or_default()
        };

        // Build the archive target with InputTransformer
        let archive_target = EventTarget {
            id: name.clone(),
            arn: Arn::new("events", &req.region, "", "").to_string(),
            input: None,
            input_path: None,
            input_transformer: Some(json!({
                "InputPathsMap": {},
                "InputTemplate": format!(
                    "{{\"archive-arn\": \"{}\", \"event\": <aws.events.event.json>, \"ingestion-time\": <aws.events.event.ingestion-time>}}",
                    arn
                )
            })),
            sqs_parameters: None,
            role_arn: None,
            dead_letter_config: None,
            retry_policy: None,
            ecs_parameters: None,
            batch_parameters: None,
            kinesis_parameters: None,
            redshift_data_parameters: None,
            http_parameters: None,
            sage_maker_pipeline_parameters: None,
            app_sync_parameters: None,
            run_command_parameters: None,
        };

        let archive_rule = EventRule {
            name: rule_name.clone(),
            arn: rule_arn,
            event_bus_name: bus_name.clone(),
            event_pattern: Some(rule_event_pattern),
            schedule_expression: None,
            state: "ENABLED".to_string(),
            description: None,
            role_arn: None,
            managed_by: Some("prod.vhs.events.aws.internal".to_string()),
            created_by: Some(state.account_id.clone()),
            targets: vec![archive_target],
            tags: BTreeMap::new(),
            last_fired: None,
        };
        let key = (bus_name, rule_name);
        state.rules.insert(key, archive_rule);

        Ok(AwsResponse::ok_json(json!({
            "ArchiveArn": arn,
            "CreationTime": now.timestamp() as f64,
            "State": "ENABLED",
        })))
    }

    pub(super) fn describe_archive(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ArchiveName", &body["ArchiveName"])?;
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?;
        validate_string_length("archiveName", name, 1, 48)?;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let archive = state.archives.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Archive {name} does not exist."),
            )
        })?;

        let mut resp = json!({
            "ArchiveArn": archive.arn,
            "ArchiveName": archive.name,
            "CreationTime": archive.creation_time.timestamp() as f64,
            "EventCount": archive.event_count,
            "EventSourceArn": archive.event_source_arn,
            "RetentionDays": archive.retention_days,
            "SizeBytes": archive.size_bytes,
            "State": archive.state,
        });
        if let Some(ref desc) = archive.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref ep) = archive.event_pattern {
            resp["EventPattern"] = json!(ep);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn list_archives(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy ListArchivesRequest constraints:
        //   NamePrefix: ArchiveName length 1..=48
        //   EventSourceArn: EventBusArn length 1..=1600
        //   NextToken: length 1..=2048
        //   Limit: LimitMax100 range 1..=100
        //   State: ArchiveState enum {ENABLED, DISABLED, CREATING, UPDATING,
        //          CREATE_FAILED, UPDATE_FAILED}
        validate_optional_string_length_value("NamePrefix", &body["NamePrefix"], 1, 48)?;
        validate_optional_string_length_value("EventSourceArn", &body["EventSourceArn"], 1, 1600)?;
        validate_optional_string_length_value("NextToken", &body["NextToken"], 1, 2048)?;
        validate_optional_json_range("Limit", &body["Limit"], 1, 100)?;
        validate_optional_enum_value(
            "State",
            &body["State"],
            &[
                "ENABLED",
                "DISABLED",
                "CREATING",
                "UPDATING",
                "CREATE_FAILED",
                "UPDATE_FAILED",
            ],
        )?;
        let name_prefix = body["NamePrefix"].as_str();
        let source_arn = body["EventSourceArn"].as_str();
        let archive_state = body["State"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100).clamp(1, 100) as usize;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all: Vec<Value> = state
            .archives
            .values()
            .filter(|a| {
                if let Some(prefix) = name_prefix {
                    a.name.starts_with(prefix)
                } else if let Some(arn) = source_arn {
                    a.event_source_arn == arn
                } else if let Some(s) = archive_state {
                    a.state == s
                } else {
                    true
                }
            })
            .map(|a| {
                json!({
                    "ArchiveName": a.name,
                    "CreationTime": a.creation_time.timestamp() as f64,
                    "EventCount": a.event_count,
                    "EventSourceArn": a.event_source_arn,
                    "RetentionDays": a.retention_days,
                    "SizeBytes": a.size_bytes,
                    "State": a.state,
                })
            })
            .collect();

        let (archives, next_token) = paginate_checked(&all, body["NextToken"].as_str(), limit)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Invalid NextToken".to_string(),
                )
            })?;
        let mut resp = json!({ "Archives": archives });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn update_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ArchiveName", &body["ArchiveName"])?;
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?;
        validate_string_length("archiveName", name, 1, 48)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_string_length("eventPattern", body["EventPattern"].as_str(), 0, 4096)?;
        if let Some(rd) = body["RetentionDays"].as_i64() {
            validate_range_i64("retentionDays", rd, 0, i64::MAX)?;
        }

        // Validate event pattern if provided
        if let Some(pattern) = body["EventPattern"].as_str() {
            validate_event_pattern(pattern)?;
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let archive = state.archives.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Archive {name} does not exist."),
            )
        })?;

        if let Some(desc) = body["Description"].as_str() {
            archive.description = Some(desc.to_string());
        }
        if let Some(pattern) = body["EventPattern"].as_str() {
            archive.event_pattern = Some(pattern.to_string());
        }
        if let Some(days) = body["RetentionDays"].as_i64() {
            archive.retention_days = days;
        }

        Ok(AwsResponse::ok_json(json!({
            "ArchiveArn": archive.arn,
            "CreationTime": archive.creation_time.timestamp() as f64,
            "State": archive.state,
        })))
    }

    pub(super) fn delete_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ArchiveName", &body["ArchiveName"])?;
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?;
        validate_string_length("archiveName", name, 1, 48)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.archives.contains_key(name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Archive {name} does not exist."),
            ));
        }

        state.archives.remove(name);

        // Remove the archive rule
        let rule_name = format!("Events-Archive-{name}");
        state.rules.retain(|k, _| k.1 != rule_name);

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ─── Connection Operations ──────────────────────────────────────────

    pub(super) fn start_replay(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let input = StartReplayInput::from_body(&req.json_body())?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Validate event bus + archive, in the order the real service validates them.
        let bus_name = state.resolve_bus_name(&input.destination_arn);
        if !state.buses.contains_key(&bus_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Event bus {bus_name} does not exist."),
            ));
        }

        let archive_name = input
            .event_source_arn
            .rsplit_once("archive/")
            .map(|(_, n)| n.to_string())
            .unwrap_or_default();
        // StartReplay's Smithy model declares ResourceNotFound,
        // ResourceAlreadyExists, InvalidEventPattern, LimitExceeded, and
        // Internal — but not ValidationException. We surface the
        // archive-missing case as ResourceNotFound and treat
        // cross-bus / inverted-window cases the same way (the archive is
        // effectively unusable as a replay source for that destination).
        let archive = state.archives.get(&archive_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Archive {archive_name} does not exist."),
            )
        })?;
        let archive_bus = state.resolve_bus_name(&archive.event_source_arn);
        if archive_bus != bus_name {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "Cross event bus replay is not permitted.",
            ));
        }

        if input.event_end_time <= input.event_start_time {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "EventStartTime must be before EventEndTime.",
            ));
        }

        if state.replays.contains_key(&input.name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceAlreadyExistsException",
                format!("Replay {} already exists.", input.name),
            ));
        }

        let now = Utc::now();
        let arn = format!(
            "arn:aws:events:{}:{}:replay/{}",
            req.region, state.account_id, input.name
        );

        let events_to_deliver = collect_replay_events_with_targets(
            state,
            &archive_name,
            &bus_name,
            input.event_start_time,
            input.event_end_time,
            &req.account_id,
            &req.region,
        );

        let replay = Replay {
            name: input.name.clone(),
            arn: arn.clone(),
            description: input.description,
            event_source_arn: input.event_source_arn,
            destination: input.destination,
            event_start_time: input.event_start_time,
            event_end_time: input.event_end_time,
            state: "COMPLETED".to_string(),
            replay_start_time: now,
            replay_end_time: Some(now),
        };
        state.replays.insert(input.name, replay);

        drop(accounts);

        for (event, targets) in events_to_deliver {
            let detail_value: Value = serde_json::from_str(&event.detail).unwrap_or(json!({}));
            let event_json = json!({
                "version": "0",
                "id": event.event_id,
                "source": event.source,
                "account": req.account_id,
                "detail-type": event.detail_type,
                "detail": detail_value,
                "time": event.time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                "region": req.region,
                "resources": event.resources,
                "replay-name": arn,
            });
            let event_str = event_json.to_string();

            for target in targets {
                self.deliver_replay_event_to_target(
                    &target,
                    &event,
                    &event_json,
                    &event_str,
                    &req.account_id,
                );
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "ReplayArn": arn,
            "ReplayStartTime": now.timestamp() as f64,
            "State": "STARTING",
        })))
    }

    pub(super) fn deliver_replay_event_to_target(
        &self,
        target: &EventTarget,
        event: &PutEvent,
        event_json: &Value,
        event_str: &str,
        account_id: &str,
    ) {
        let target_arn = &target.arn;
        let body_str = if let Some(ref transformer) = target.input_transformer {
            apply_input_transformer(transformer, event_json)
        } else if let Some(ref input) = target.input {
            input.clone()
        } else if let Some(ref input_path) = target.input_path {
            resolve_json_path(event_json, input_path)
                .map(|v| v.to_string())
                .unwrap_or_else(|| event_str.to_string())
        } else {
            event_str.to_string()
        };

        if target_arn.contains(":sqs:") {
            let group_id = target
                .sqs_parameters
                .as_ref()
                .and_then(|p| p["MessageGroupId"].as_str())
                .map(|s| s.to_string());
            if group_id.is_some() {
                self.delivery.send_to_sqs_with_attrs(
                    target_arn,
                    &body_str,
                    &HashMap::new(),
                    group_id.as_deref(),
                    None,
                );
            } else {
                self.delivery
                    .send_to_sqs(target_arn, &body_str, &HashMap::new());
            }
        } else if target_arn.contains(":sns:") {
            self.delivery
                .publish_to_sns(target_arn, &body_str, Some(&event.detail_type));
        } else if target_arn.contains(":lambda:") {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(account_id);
            state
                .lambda_invocations
                .push(crate::state::LambdaInvocation {
                    function_arn: target_arn.clone(),
                    payload: body_str.clone(),
                    timestamp: Utc::now(),
                });
            drop(accounts);
            if let Some(ref ls) = self.lambda_state {
                ls.write()
                    .get_or_create(account_id)
                    .invocations
                    .push(LambdaInvocation {
                        function_arn: target_arn.clone(),
                        payload: body_str.clone(),
                        timestamp: Utc::now(),
                        source: "aws:events".to_string(),
                    });
            }
            invoke_lambda_async(
                &self.container_runtime,
                &self.lambda_state,
                target_arn,
                &body_str,
            );
        } else if target_arn.contains(":logs:") {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(account_id);
            state.log_deliveries.push(crate::state::LogDelivery {
                log_group_arn: target_arn.clone(),
                payload: body_str.clone(),
                timestamp: Utc::now(),
            });
            drop(accounts);
            if let Some(ref log_state) = self.logs_state {
                deliver_to_logs(log_state, target_arn, &body_str, Utc::now());
            }
        } else if target_arn.contains(":states:") {
            self.delivery
                .start_stepfunctions_execution(target_arn, &body_str);
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(account_id);
            state
                .step_function_executions
                .push(crate::state::StepFunctionExecution {
                    state_machine_arn: target_arn.clone(),
                    payload: body_str.clone(),
                    timestamp: Utc::now(),
                });
        } else if target_arn.starts_with("https://") || target_arn.starts_with("http://") {
            let url = target_arn.clone();
            let payload = body_str.clone();
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                let _ = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body(payload)
                    .send()
                    .await;
            });
        }
    }

    pub(super) fn describe_replay(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ReplayName", &body["ReplayName"])?;
        let name = body["ReplayName"]
            .as_str()
            .ok_or_else(|| missing("ReplayName"))?;
        validate_string_length("replayName", name, 1, 64)?;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let replay = state.replays.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Replay {name} does not exist."),
            )
        })?;

        let mut resp = json!({
            "Destination": replay.destination,
            "EventSourceArn": replay.event_source_arn,
            "EventStartTime": replay.event_start_time.timestamp() as f64,
            "EventEndTime": replay.event_end_time.timestamp() as f64,
            "ReplayArn": replay.arn,
            "ReplayName": replay.name,
            "ReplayStartTime": replay.replay_start_time.timestamp() as f64,
            "State": replay.state,
        });
        if let Some(ref desc) = replay.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref end) = replay.replay_end_time {
            resp["ReplayEndTime"] = json!(end.timestamp() as f64);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn list_replays(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy ListReplaysRequest constraints:
        //   NamePrefix: ReplayName length 1..=64
        //   EventSourceArn: ArchiveArn length 1..=1600
        //   NextToken: length 1..=2048
        //   Limit: LimitMax100 range 1..=100
        //   State: ReplayState enum {STARTING, RUNNING, CANCELLING, COMPLETED,
        //          CANCELLED, FAILED}
        validate_optional_string_length_value("NamePrefix", &body["NamePrefix"], 1, 64)?;
        validate_optional_string_length_value("EventSourceArn", &body["EventSourceArn"], 1, 1600)?;
        validate_optional_string_length_value("NextToken", &body["NextToken"], 1, 2048)?;
        validate_optional_json_range("Limit", &body["Limit"], 1, 100)?;
        validate_optional_enum_value(
            "State",
            &body["State"],
            &[
                "STARTING",
                "RUNNING",
                "CANCELLING",
                "COMPLETED",
                "CANCELLED",
                "FAILED",
            ],
        )?;
        let name_prefix = body["NamePrefix"].as_str();
        let source_arn = body["EventSourceArn"].as_str();
        let replay_state = body["State"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100).clamp(1, 100) as usize;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all: Vec<Value> = state
            .replays
            .values()
            .filter(|r| {
                if let Some(prefix) = name_prefix {
                    r.name.starts_with(prefix)
                } else if let Some(arn) = source_arn {
                    r.event_source_arn == arn
                } else if let Some(s) = replay_state {
                    r.state == s
                } else {
                    true
                }
            })
            .map(|r| {
                let mut obj = json!({
                    "EventSourceArn": r.event_source_arn,
                    "EventStartTime": r.event_start_time.timestamp() as f64,
                    "EventEndTime": r.event_end_time.timestamp() as f64,
                    "ReplayName": r.name,
                    "ReplayStartTime": r.replay_start_time.timestamp() as f64,
                    "State": r.state,
                });
                if let Some(ref end) = r.replay_end_time {
                    obj["ReplayEndTime"] = json!(end.timestamp() as f64);
                }
                obj
            })
            .collect();

        let (replays, next_token) = paginate_checked(&all, body["NextToken"].as_str(), limit)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Invalid NextToken".to_string(),
                )
            })?;
        let mut resp = json!({ "Replays": replays });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn cancel_replay(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ReplayName", &body["ReplayName"])?;
        let name = body["ReplayName"]
            .as_str()
            .ok_or_else(|| missing("ReplayName"))?;
        validate_string_length("replayName", name, 1, 64)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let replay = state.replays.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Replay {name} does not exist."),
            )
        })?;

        // Can only cancel STARTING or RUNNING replays (or COMPLETED in our mock)
        if replay.state == "CANCELLED" || replay.state == "CANCELLING" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "IllegalStatusException",
                format!("Replay {name} is not in a valid state for this operation."),
            ));
        }

        let arn = replay.arn.clone();
        replay.state = "CANCELLED".to_string();

        Ok(AwsResponse::ok_json(json!({
            "ReplayArn": arn,
            "State": "CANCELLING",
        })))
    }
}
