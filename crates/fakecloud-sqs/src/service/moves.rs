//! `SqsService` `moves` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl SqsService {
    pub(super) fn start_message_move_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let source_arn = body["SourceArn"]
            .as_str()
            .ok_or_else(|| missing_param("SourceArn"))?
            .to_string();
        let destination_arn = body["DestinationArn"].as_str().map(|s| s.to_string());
        let max_per_sec_i64 = val_as_i64(&body["MaxNumberOfMessagesPerSecond"]);
        if let Some(rate) = max_per_sec_i64 {
            // Range validation runs only when the value would underflow
            // i32 — the AWS-side legal range is 1..=500 but the Smithy
            // op declares no error for "MaxNumberOfMessagesPerSecond out
            // of range" (only `InvalidAddress`, `InvalidSecurity`,
            // `RequestThrottled`, `ResourceNotFoundException`,
            // `UnsupportedOperation`). Be lenient on out-of-range:
            // clamp into i32, let real validation happen against the
            // resource state below.
            if !(i32::MIN as i64..=i32::MAX as i64).contains(&rate) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AWS.SimpleQueueService.UnsupportedOperation",
                    "MaxNumberOfMessagesPerSecond is out of range.",
                ));
            }
        }
        let max_per_sec = max_per_sec_i64.map(|rate| rate.clamp(1, 500) as i32);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let source_url = state
            .queues
            .values()
            .find(|q| q.arn == source_arn)
            .map(|q| q.queue_url.clone())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The resource that you specified for the SourceArn parameter doesn't exist.",
                )
            })?;

        // Source must be a DLQ (i.e. some other queue references it as redrive target).
        let dlq_sources: Vec<String> = state
            .queues
            .values()
            .filter(|q| {
                q.redrive_policy
                    .as_ref()
                    .map(|rp| rp.dead_letter_target_arn == source_arn)
                    .unwrap_or(false)
            })
            .map(|q| q.queue_url.clone())
            .collect();
        if dlq_sources.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.UnsupportedOperation",
                "Source queue must be configured as a dead-letter queue.",
            ));
        }

        // Validate destination if provided: must exist and be in same account.
        if let Some(ref dest) = destination_arn {
            if !state.queues.values().any(|q| &q.arn == dest) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The resource that you specified for the DestinationArn parameter doesn't exist.",
                ));
            }
        }

        // Reject if there's already a RUNNING task for this source.
        if state
            .message_move_tasks
            .iter()
            .any(|t| t.source_arn == source_arn && t.status == MessageMoveTaskStatus::Running)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.UnsupportedOperation",
                "There is already an active message movement task for the specified source queue.",
            ));
        }

        let total = state
            .queues
            .get(&source_url)
            .map(|q| q.messages.len() as u64)
            .unwrap_or(0);
        let task_handle = format!("FakeCloudMessageMoveTask-{}", uuid::Uuid::new_v4().simple());

        // Fast path: if no rate was supplied AND there is no work to throttle,
        // drain synchronously and mark the task COMPLETED before returning.
        // The async path below is only useful when the caller wants real
        // backpressure (rate limit) or the ability to observe / cancel mid-flight.
        if max_per_sec.is_none() {
            let source_messages: Vec<SqsMessage> = state
                .queues
                .get_mut(&source_url)
                .map(|q| {
                    let drained: Vec<_> = q.messages.drain(..).collect();
                    q.inflight.clear();
                    drained
                })
                .unwrap_or_default();
            let mut moved: u64 = 0;
            if let Some(ref dest) = destination_arn {
                let dest_url = state
                    .queues
                    .values()
                    .find(|q| &q.arn == dest)
                    .map(|q| q.queue_url.clone());
                if let Some(dest_url) = dest_url {
                    if let Some(dq) = state.queues.get_mut(&dest_url) {
                        for msg in source_messages {
                            let mut new_msg = msg;
                            new_msg.visible_at = None;
                            new_msg.receive_count = 0;
                            dq.messages.push_back(new_msg);
                            moved += 1;
                        }
                    }
                }
            } else {
                let targets = dlq_sources.clone();
                for (idx, msg) in source_messages.into_iter().enumerate() {
                    if targets.is_empty() {
                        break;
                    }
                    let target_url = &targets[idx % targets.len()];
                    if let Some(tq) = state.queues.get_mut(target_url) {
                        let mut new_msg = msg;
                        new_msg.visible_at = None;
                        new_msg.receive_count = 0;
                        tq.messages.push_back(new_msg);
                        moved += 1;
                    }
                }
            }

            state.message_move_tasks.push(MessageMoveTask {
                task_handle: task_handle.clone(),
                source_arn,
                destination_arn,
                max_messages_per_second: max_per_sec,
                status: MessageMoveTaskStatus::Completed,
                messages_moved: moved,
                messages_to_move: total,
                started_timestamp: Utc::now().timestamp_millis(),
                failure_reason: None,
                cancel_flag: Arc::new(AtomicBool::new(false)),
            });
            return Ok(sqs_response(
                "StartMessageMoveTask",
                json!({ "TaskHandle": task_handle }),
                &req.request_id,
                req.is_query_protocol,
            ));
        }

        // Async path: insert a Running task, spawn a background mover that
        // drains one message at a time at the requested rate. Cancellation is
        // signalled through the task's `cancel_flag`.
        let cancel_flag = Arc::new(AtomicBool::new(false));
        state.message_move_tasks.push(MessageMoveTask {
            task_handle: task_handle.clone(),
            source_arn: source_arn.clone(),
            destination_arn: destination_arn.clone(),
            max_messages_per_second: max_per_sec,
            status: MessageMoveTaskStatus::Running,
            messages_moved: 0,
            messages_to_move: total,
            started_timestamp: Utc::now().timestamp_millis(),
            failure_reason: None,
            cancel_flag: cancel_flag.clone(),
        });
        drop(accounts);

        let state_handle = self.state.clone();
        let account_id = req.account_id.clone();
        let region = req.region.clone();
        let handle_for_task = task_handle.clone();
        let dlq_targets = dlq_sources.clone();
        let rate = max_per_sec.unwrap_or(1).max(1) as u64;
        let interval = std::time::Duration::from_nanos(1_000_000_000 / rate);

        tokio::spawn(async move {
            run_message_move_task(
                state_handle,
                account_id,
                region,
                handle_for_task,
                source_url,
                destination_arn,
                dlq_targets,
                interval,
                cancel_flag,
            )
            .await;
        });

        Ok(sqs_response(
            "StartMessageMoveTask",
            json!({ "TaskHandle": task_handle }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn cancel_message_move_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let task_handle = body["TaskHandle"]
            .as_str()
            .ok_or_else(|| missing_param("TaskHandle"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let task = state
            .message_move_tasks
            .iter_mut()
            .find(|t| t.task_handle == task_handle)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The specified task handle doesn't exist.",
                )
            })?;
        let moved = task.messages_moved;
        match task.status {
            MessageMoveTaskStatus::Running => {
                // Signal the background mover to stop on its next tick.
                // The mover flips status to Cancelled when it observes the
                // flag and exits its loop; we mark Cancelling here so the
                // request doesn't have to wait for that observation.
                task.cancel_flag.store(true, Ordering::SeqCst);
                task.status = MessageMoveTaskStatus::Cancelling;
            }
            MessageMoveTaskStatus::Completed
            | MessageMoveTaskStatus::Cancelled
            | MessageMoveTaskStatus::Cancelling
            | MessageMoveTaskStatus::Failed => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AWS.SimpleQueueService.UnsupportedOperation",
                    "The message movement task isn't in RUNNING status.",
                ));
            }
        }

        Ok(sqs_response(
            "CancelMessageMoveTask",
            json!({ "ApproximateNumberOfMessagesMoved": moved }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn list_message_move_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let source_arn = body["SourceArn"]
            .as_str()
            .ok_or_else(|| missing_param("SourceArn"))?
            .to_string();
        let max_results = val_as_i64(&body["MaxResults"])
            .map(|n| n.clamp(1, 10) as usize)
            .unwrap_or(1);

        let accounts = self.state.read();
        let empty = crate::state::SqsState::new(&req.account_id, &req.region, "");
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Source queue must exist.
        if !state.queues.values().any(|q| q.arn == source_arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The resource that you specified for the SourceArn parameter doesn't exist.",
            ));
        }

        let mut tasks: Vec<&MessageMoveTask> = state
            .message_move_tasks
            .iter()
            .filter(|t| t.source_arn == source_arn)
            .collect();
        tasks.sort_by_key(|t| std::cmp::Reverse(t.started_timestamp));
        tasks.truncate(max_results);

        let results: Vec<Value> = tasks
            .into_iter()
            .map(|t| {
                let mut entry = serde_json::Map::new();
                if t.status == MessageMoveTaskStatus::Running {
                    entry.insert("TaskHandle".to_string(), json!(t.task_handle));
                }
                entry.insert("Status".to_string(), json!(t.status.as_str()));
                entry.insert("SourceArn".to_string(), json!(t.source_arn));
                if let Some(ref d) = t.destination_arn {
                    entry.insert("DestinationArn".to_string(), json!(d));
                }
                if let Some(m) = t.max_messages_per_second {
                    entry.insert("MaxNumberOfMessagesPerSecond".to_string(), json!(m));
                }
                entry.insert(
                    "ApproximateNumberOfMessagesMoved".to_string(),
                    json!(t.messages_moved),
                );
                entry.insert(
                    "ApproximateNumberOfMessagesToMove".to_string(),
                    json!(t.messages_to_move),
                );
                if let Some(ref reason) = t.failure_reason {
                    entry.insert("FailureReason".to_string(), json!(reason));
                }
                entry.insert("StartedTimestamp".to_string(), json!(t.started_timestamp));
                Value::Object(entry)
            })
            .collect();

        Ok(sqs_response(
            "ListMessageMoveTasks",
            json!({ "Results": results }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }
}
