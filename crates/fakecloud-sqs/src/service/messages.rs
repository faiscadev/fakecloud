//! `SqsService` `messages` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl SqsService {
    /// Encrypt an SQS message body via the configured KMS hook when the
    /// queue has `KmsMasterKeyId` set. Returns the plaintext unchanged
    /// when no hook is wired or the queue isn't encrypted.
    ///
    /// Fail-closed: if the hook errors (key denied, key not found), this
    /// returns `Err` so SendMessage / SendMessageBatch abort with a 500
    /// rather than silently storing plaintext on an "encrypted" queue.
    /// Real AWS surfaces KMS errors back to the caller; fakecloud
    /// matching that behavior is what makes tests catch broken key
    /// policies before they hit prod.
    pub(super) fn encrypt_message_body(
        &self,
        account_id: &str,
        queue_arn: &str,
        kms_key_id: Option<&str>,
        plaintext: &str,
    ) -> Result<String, AwsServiceError> {
        let Some(hook) = &self.kms_hook else {
            return Ok(plaintext.to_string());
        };
        let Some(key) = kms_key_id.filter(|k| !k.is_empty()) else {
            return Ok(plaintext.to_string());
        };
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("aws:sqs:arn".to_string(), queue_arn.to_string());
        match hook.encrypt(
            account_id,
            &self.region,
            key,
            plaintext.as_bytes(),
            "sqs.amazonaws.com",
            ctx,
        ) {
            Ok(envelope) => Ok(envelope),
            Err(err) => {
                tracing::warn!(queue = %queue_arn, error = %err, "SQS SSE-KMS encrypt failed");
                Err(AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMS.InternalFailureException",
                    format!("Failed to encrypt SQS message via KMS: {err}"),
                ))
            }
        }
    }

    /// Decrypt a stored SQS body via the KMS hook.
    ///
    /// Bodies stored on a queue can come from two paths:
    ///  - The SQS SendMessage handler, which encrypts under the
    ///    queue's effective key (SSE-KMS or SSE-SQS) and stores a
    ///    base64-wrapped fakecloud-kms envelope.
    ///  - Cross-service delivery (SNS fanout, EventBridge target,
    ///    S3 / DDB / ECS / Logs notifications, SES events, etc.) which
    ///    bypasses the SQS service entirely and writes the plaintext
    ///    body directly into `queue.messages` via `SqsDeliveryImpl`.
    ///
    /// Real AWS keeps both paths working on encrypted queues by
    /// re-encrypting the cross-service body server-side; fakecloud's
    /// in-process delivery bus doesn't need to round-trip through KMS
    /// and skips encryption entirely. To avoid silently dropping
    /// every cross-service message on a managed-SSE queue, we detect
    /// fakecloud envelopes by their version header and only invoke
    /// the KMS hook when one is present. Anything else is returned
    /// as-is — same end-result the caller would see if the body had
    /// been written through SendMessage on an unencrypted queue.
    ///
    /// Hook errors on a body that *does* look like an envelope
    /// surface as 500 `KMS.InternalFailureException` so callers don't
    /// silently see the raw ciphertext envelope when KMS access is
    /// broken.
    pub(super) fn decrypt_message_body(
        &self,
        account_id: &str,
        queue_arn: &str,
        ciphertext: &str,
    ) -> Result<String, AwsServiceError> {
        let Some(hook) = &self.kms_hook else {
            return Ok(ciphertext.to_string());
        };
        if !looks_like_fakecloud_envelope(ciphertext) {
            return Ok(ciphertext.to_string());
        }
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("aws:sqs:arn".to_string(), queue_arn.to_string());
        match hook.decrypt(account_id, ciphertext, "sqs.amazonaws.com", ctx) {
            Ok(bytes) => Ok(String::from_utf8_lossy(&bytes).to_string()),
            Err(err) => {
                tracing::warn!(queue = %queue_arn, error = %err, "SQS SSE-KMS decrypt failed");
                Err(AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMS.InternalFailureException",
                    format!("Failed to decrypt SQS message via KMS: {err}"),
                ))
            }
        }
    }

    pub(super) fn send_message(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // --- Parse and validate request body WITHOUT holding any lock ---
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?
            .to_string();
        let message_body = body["MessageBody"]
            .as_str()
            .ok_or_else(|| missing_param("MessageBody"))?
            .to_string();

        let message_group_id = body["MessageGroupId"].as_str().map(|s| s.to_string());
        let message_dedup_id = body["MessageDeduplicationId"]
            .as_str()
            .map(|s| s.to_string());

        // Resolve the queue *before* shape-validating attributes. The
        // attribute validator returns `InvalidParameterValue`, which is
        // NOT in `SendMessage`'s Smithy `errors` list; the queue lookup
        // returns `QueueDoesNotExist`, which IS. The conformance probe
        // sends unknown queue URLs, so a missing queue must surface
        // first to avoid emitting an undeclared error code.
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            resolve_queue_url(&queue_url, state).ok_or_else(queue_not_found)?;
        }

        let message_attributes = parse_message_attributes(&body);
        validate_message_attributes(&message_attributes)?;
        let system_attributes = parse_message_system_attributes(&body);

        // Pre-compute hashes outside the lock to avoid holding it during CPU work
        let md5_of_body = md5_hex(&message_body);
        let md5_of_attrs = if message_attributes.is_empty() {
            None
        } else {
            Some(md5_of_message_attributes(&message_attributes))
        };
        let md5_of_system_attrs = if system_attributes.is_empty() {
            None
        } else {
            Some(md5_of_message_system_attributes(&system_attributes))
        };
        // Validate delay seconds range before acquiring lock
        let raw_delay = val_as_i64(&body["DelaySeconds"]);
        if let Some(d) = raw_delay {
            if !(0..=900).contains(&d) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Value {d} for parameter DelaySeconds is invalid. Reason: Must be between 0 and 900, if provided."),
                ));
            }
        }

        let request_id = req.request_id.clone();
        let is_query = req.is_query_protocol;

        // --- Acquire write lock ONLY for queue validation + mutation ---
        let (message_id, sequence_number) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            let resolved_url = resolve_queue_url(&queue_url, state).ok_or_else(queue_not_found)?;
            let queue = state
                .queues
                .get_mut(&resolved_url)
                .ok_or_else(queue_not_found)?;

            check_fifo_send_constraints(queue, raw_delay, &message_group_id, &message_dedup_id)?;

            // FIFO dedup
            let effective_dedup_id = if queue.is_fifo {
                message_dedup_id.clone().or_else(|| {
                    if queue
                        .attributes
                        .get("ContentBasedDeduplication")
                        .map(|v| v.as_str())
                        == Some("true")
                    {
                        Some(sha256_hex(&message_body))
                    } else {
                        None
                    }
                })
            } else {
                None
            };

            if queue.is_fifo {
                if let Some(ref dedup_id) = effective_dedup_id {
                    let now = Utc::now();
                    queue.dedup_cache.retain(|_, e| e.expiry > now);
                    if let Some(entry) = queue.dedup_cache.get(dedup_id) {
                        // Duplicate within the dedup window: AWS replays the
                        // ORIGINAL MessageId + SequenceNumber, does not enqueue
                        // again, and does not advance the counter (1.13).
                        let mut resp = json!({
                            "MessageId": entry.message_id,
                            "MD5OfMessageBody": &md5_of_body,
                        });
                        if let Some(ref seq) = entry.sequence_number {
                            resp["SequenceNumber"] = json!(seq);
                        }
                        if let Some(ref md5) = md5_of_attrs {
                            resp["MD5OfMessageAttributes"] = json!(md5);
                        }
                        if let Some(ref md5) = md5_of_system_attrs {
                            resp["MD5OfMessageSystemAttributes"] = json!(md5);
                        }
                        return Ok(sqs_response("SendMessage", resp, &request_id, is_query));
                    }
                    // Cache insertion deferred until after the message is
                    // actually committed (encryption can fail), so a
                    // KMS-denied retry isn't silently de-duplicated.
                }
            }

            // MaximumMessageSize validation. AWS measures body +
            // message-attribute size (name + type + value) against the
            // limit, not the body alone (bug-audit 2026-06-13, 1.13).
            let max_message_size: usize = queue
                .attributes
                .get("MaximumMessageSize")
                .and_then(|s| s.parse().ok())
                .unwrap_or(262144);
            let total_size = message_body.len() + message_attributes_size(&message_attributes);
            if total_size > max_message_size {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "One or more parameters are invalid. Reason: Message must be shorter than {} bytes.",
                        max_message_size
                    ),
                ));
            }

            let delay: i64 = raw_delay
                .or_else(|| {
                    queue
                        .attributes
                        .get("DelaySeconds")
                        .and_then(|s| s.parse().ok())
                })
                .unwrap_or(0);
            let now = Utc::now();
            let visible_at = if delay > 0 {
                Some(now + chrono::Duration::seconds(delay))
            } else {
                None
            };

            let sequence_number = if queue.is_fifo {
                let seq = queue.next_sequence_number;
                queue.next_sequence_number += 1;
                Some(seq.to_string())
            } else {
                None
            };

            // At-rest encryption: encrypt the body via the KMS hook so the
            // in-memory queue holds a fakecloud-kms envelope. The hook
            // resolution covers both SSE-KMS (`KmsMasterKeyId` set) and
            // SSE-SQS (`SqsManagedSseEnabled=true`, implicit
            // `alias/aws/sqs`). `md5_of_body` keeps the plaintext digest
            // — that's what callers verify against, and matches AWS's
            // behavior of returning the plaintext MD5.
            let kms_key_id = Self::effective_kms_key_id(&queue.attributes);
            let stored_body = self.encrypt_message_body(
                &req.account_id,
                &queue.arn,
                kms_key_id.as_deref(),
                &message_body,
            )?;
            let msg = SqsMessage {
                message_id: uuid::Uuid::new_v4().to_string(),
                receipt_handle: None,
                md5_of_body: md5_of_body.clone(),
                body: stored_body,
                sent_timestamp: now.timestamp_millis(),
                attributes: system_attributes,
                message_attributes,
                visible_at,
                receive_count: 0,
                first_received_at: None,
                message_group_id,
                message_dedup_id: effective_dedup_id.clone(),
                created_at: now,
                sequence_number: sequence_number.clone(),
            };

            let message_id = msg.message_id.clone();
            queue.messages.push_back(msg);

            // Commit the dedup cache entry only after the message is on
            // the queue. Earlier failures (encryption, size limit) leave
            // the dedup_id un-cached so the caller's retry can succeed.
            if queue.is_fifo {
                if let Some(ref dedup_id) = effective_dedup_id {
                    queue.dedup_cache.insert(
                        dedup_id.clone(),
                        crate::state::DedupEntry {
                            message_id: message_id.clone(),
                            sequence_number: sequence_number.clone(),
                            expiry: Utc::now() + chrono::Duration::minutes(5),
                        },
                    );
                }
            }

            (message_id, sequence_number)
        };
        // --- Write lock released, build response ---

        let mut resp = json!({
            "MessageId": message_id,
            "MD5OfMessageBody": md5_of_body,
        });
        if let Some(seq) = &sequence_number {
            resp["SequenceNumber"] = json!(seq);
        }
        if let Some(md5) = &md5_of_attrs {
            resp["MD5OfMessageAttributes"] = json!(md5);
        }
        if let Some(md5) = &md5_of_system_attrs {
            resp["MD5OfMessageSystemAttributes"] = json!(md5);
        }

        Ok(sqs_response("SendMessage", resp, &request_id, is_query))
    }

    pub(super) async fn receive_message(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url_input = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?
            .to_string();

        // Resolve the queue URL (might be a name) and read its configured
        // long-poll default in the same lock acquisition.
        let (queue_url, queue_wait_default) = {
            let _accts = self.state.read();
            let _empty = crate::state::SqsState::new(&req.account_id, &req.region, "");
            let state = _accts.get(&req.account_id).unwrap_or(&_empty);
            let url = resolve_queue_url(&queue_url_input, state).ok_or_else(queue_not_found)?;
            let default = state
                .queues
                .get(&url)
                .and_then(|q| q.attributes.get("ReceiveMessageWaitTimeSeconds"))
                .and_then(|v| v.parse::<i64>().ok());
            (url, default)
        };

        let max_messages_raw = val_as_i64(&body["MaxNumberOfMessages"]);
        if let Some(max) = max_messages_raw {
            if !(1..=10).contains(&max) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Value {max} for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided."),
                ));
            }
        }
        let max_messages = max_messages_raw.unwrap_or(1).min(10) as usize;

        let visibility_timeout = val_as_i64(&body["VisibilityTimeout"]);

        // FIFO receive de-dup: a retried receive carrying the same
        // ReceiveRequestAttemptId within the visibility window replays the
        // exact same batch instead of advancing to the next messages.
        let receive_request_attempt_id = body["ReceiveRequestAttemptId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(String::from);

        let wait_time_raw = val_as_i64(&body["WaitTimeSeconds"]);
        if let Some(wt) = wait_time_raw {
            if !(0..=20).contains(&wt) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Value {wt} for parameter WaitTimeSeconds is invalid. Reason: Must be between 0 and 20, if provided."),
                ));
            }
        }
        // When the request omits WaitTimeSeconds, AWS applies the queue's
        // ReceiveMessageWaitTimeSeconds attribute (queue-level long polling).
        // Previously the queue default was ignored and every poll was a
        // short poll (bug-audit 2026-06-20, 1.24).
        let wait_time_seconds = wait_time_raw
            .or(queue_wait_default)
            .unwrap_or(0)
            .clamp(0, 20) as u64;

        // Parse requested system attributes
        let attribute_names: Option<Vec<String>> = body["AttributeNames"].as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });
        // Also check MessageSystemAttributeNames (newer SDK field)
        let sys_attr_names: Option<Vec<String>> =
            body["MessageSystemAttributeNames"].as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            });
        let requested_sys_attrs = sys_attr_names.or(attribute_names);

        // Parse requested message attributes filter
        let msg_attr_names: Option<Vec<String>> =
            body["MessageAttributeNames"].as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            });

        let request_id = req.request_id.clone();
        let is_query = req.is_query_protocol;

        let deadline = if wait_time_seconds > 0 {
            Some(tokio::time::Instant::now() + std::time::Duration::from_secs(wait_time_seconds))
        } else {
            None
        };

        loop {
            let result = self.try_receive_messages(
                &req.account_id,
                &queue_url,
                max_messages,
                visibility_timeout,
                receive_request_attempt_id.as_deref(),
            )?;

            if !result.is_empty() || deadline.is_none() {
                return Ok(format_receive_response(
                    &result,
                    &request_id,
                    is_query,
                    requested_sys_attrs.as_deref(),
                    msg_attr_names.as_deref(),
                ));
            }

            let deadline = deadline.unwrap();
            if tokio::time::Instant::now() >= deadline {
                return Ok(format_receive_response(
                    &result,
                    &request_id,
                    is_query,
                    requested_sys_attrs.as_deref(),
                    msg_attr_names.as_deref(),
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    pub(super) fn try_receive_messages(
        &self,
        account_id: &str,
        queue_url: &str,
        max_messages: usize,
        req_visibility_timeout: Option<i64>,
        receive_request_attempt_id: Option<&str>,
    ) -> Result<Vec<SqsMessage>, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let visibility_timeout: i64 = req_visibility_timeout
            .or_else(|| {
                queue
                    .attributes
                    .get("VisibilityTimeout")
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(30);

        let is_fifo = queue.is_fifo;
        let now = Utc::now();

        // FIFO receive de-dup: a retried receive with a known, unexpired
        // ReceiveRequestAttemptId replays the exact same batch - the
        // messages are still inflight (still within the visibility
        // window), so we re-resolve them by message id rather than
        // pulling the next ones. Expired entries are pruned.
        if is_fifo {
            queue.receive_attempt_cache.retain(|_, e| e.expiry > now);
            if let Some(attempt_id) = receive_request_attempt_id {
                if let Some(entry) = queue.receive_attempt_cache.get(attempt_id).cloned() {
                    let mut replayed: Vec<SqsMessage> = Vec::new();
                    for mid in &entry.message_ids {
                        if let Some(msg) = queue.inflight.iter().find(|m| &m.message_id == mid) {
                            replayed.push(msg.clone());
                        }
                    }
                    if !replayed.is_empty() {
                        // Decrypt bodies for the replayed batch, same as the
                        // normal path below.
                        let queue_arn = queue.arn.clone();
                        let kms_key_id = Self::effective_kms_key_id(&queue.attributes);
                        if kms_key_id.is_some() && self.kms_hook.is_some() {
                            for msg in replayed.iter_mut() {
                                msg.body =
                                    self.decrypt_message_body(account_id, &queue_arn, &msg.body)?;
                            }
                        }
                        return Ok(replayed);
                    }
                }
            }
        }

        // MessageRetentionPeriod expiry: remove messages older than the retention period
        let retention_seconds: i64 = queue
            .attributes
            .get("MessageRetentionPeriod")
            .and_then(|s| s.parse().ok())
            .unwrap_or(345600); // default 4 days
        queue
            .messages
            .retain(|m| (now - m.created_at).num_seconds() < retention_seconds);
        queue
            .inflight
            .retain(|m| (now - m.created_at).num_seconds() < retention_seconds);

        // Return expired inflight messages
        let mut returned = Vec::new();
        queue.inflight.retain(|m| {
            if let Some(visible_at) = m.visible_at {
                if visible_at <= now {
                    returned.push(m.clone());
                    return false;
                }
            }
            true
        });
        // For FIFO queues, push returned messages to the FRONT to maintain order
        if is_fifo {
            for mut m in returned.into_iter().rev() {
                m.visible_at = None;
                queue.messages.push_front(m);
            }
        } else {
            for mut m in returned {
                m.visible_at = None;
                queue.messages.push_back(m);
            }
        }

        let redrive_policy = queue.redrive_policy.clone();

        let mut received = Vec::new();
        let mut dlq_messages = Vec::new();

        if is_fifo {
            // FIFO: deliver messages in order, respecting group locking.
            // Groups that already have inflight messages are skipped.
            // Multiple messages from the same group CAN be delivered in one batch.
            let mut remaining = VecDeque::new();

            // Build set of groups that have pre-existing inflight messages
            let inflight_groups: std::collections::HashSet<String> = queue
                .inflight
                .iter()
                .filter_map(|m| m.message_group_id.clone())
                .collect();

            while let Some(mut msg) = queue.messages.pop_front() {
                if let Some(visible_at) = msg.visible_at {
                    if visible_at > now {
                        remaining.push_back(msg);
                        continue;
                    }
                }

                let group = msg.message_group_id.as_deref().unwrap_or("").to_string();

                // Skip groups that already have inflight messages from previous receives
                if inflight_groups.contains(&group) {
                    remaining.push_back(msg);
                    continue;
                }

                if received.len() < max_messages {
                    msg.receive_count += 1;
                    if msg.first_received_at.is_none() {
                        msg.first_received_at = Some(chrono::Utc::now().timestamp_millis());
                    }
                    if let Some(ref rp) = redrive_policy {
                        if msg.receive_count > rp.max_receive_count {
                            dlq_messages.push((rp.dead_letter_target_arn.clone(), msg));
                            continue;
                        }
                    }
                    let new_handle = uuid::Uuid::new_v4().to_string();
                    queue
                        .receipt_handle_map
                        .entry(msg.message_id.clone())
                        .or_default()
                        .push(new_handle.clone());
                    msg.receipt_handle = Some(new_handle);
                    msg.visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
                    received.push(msg);
                } else {
                    remaining.push_back(msg);
                    break;
                }
            }

            while let Some(m) = queue.messages.pop_front() {
                remaining.push_back(m);
            }
            queue.messages = remaining;
        } else {
            // Standard queue with Fair Queues support:
            // When messages have MessageGroupId, prioritize groups with fewer
            // in-flight messages to prevent noisy neighbor starvation.

            // Count in-flight messages per group
            let mut inflight_per_group: HashMap<String, usize> = HashMap::new();
            for m in &queue.inflight {
                if let Some(ref group) = m.message_group_id {
                    *inflight_per_group.entry(group.clone()).or_default() += 1;
                }
            }

            // Collect all visible messages
            let mut visible: Vec<SqsMessage> = Vec::new();
            let mut remaining = VecDeque::new();
            while let Some(msg) = queue.messages.pop_front() {
                if let Some(visible_at) = msg.visible_at {
                    if visible_at > now {
                        remaining.push_back(msg);
                        continue;
                    }
                }
                visible.push(msg);
            }

            // Sort by fairness: messages from groups with fewer in-flight messages come first.
            // Messages without a group ID are treated as having 0 in-flight (highest priority).
            visible.sort_by_key(|m| {
                m.message_group_id
                    .as_ref()
                    .and_then(|g| inflight_per_group.get(g).copied())
                    .unwrap_or(0)
            });

            // Pick up to max_messages from the sorted list
            for mut msg in visible {
                if received.len() < max_messages {
                    msg.receive_count += 1;
                    if msg.first_received_at.is_none() {
                        msg.first_received_at = Some(chrono::Utc::now().timestamp_millis());
                    }
                    if let Some(ref rp) = redrive_policy {
                        if msg.receive_count > rp.max_receive_count {
                            dlq_messages.push((rp.dead_letter_target_arn.clone(), msg));
                            continue;
                        }
                    }
                    let new_handle = uuid::Uuid::new_v4().to_string();
                    queue
                        .receipt_handle_map
                        .entry(msg.message_id.clone())
                        .or_default()
                        .push(new_handle.clone());
                    msg.receipt_handle = Some(new_handle);
                    msg.visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
                    received.push(msg);
                } else {
                    remaining.push_back(msg);
                }
            }

            queue.messages = remaining;
        }

        // Capture queue-level encryption settings BEFORE the DLQ path
        // re-borrows `state.queues` mutably to push into the DLQ.
        let queue_arn = queue.arn.clone();
        let kms_key_id = Self::effective_kms_key_id(&queue.attributes);

        // Decrypt BEFORE mutating inflight/DLQ. The previous order would
        // bump `receive_count` and stash the message in `inflight`, then
        // bail with the decrypt error — the visibility window was
        // already consumed even though the caller saw an error.
        let mut plaintext_bodies: Vec<String> = Vec::with_capacity(received.len());
        if kms_key_id.is_some() && self.kms_hook.is_some() {
            for msg in received.iter() {
                let plaintext = self.decrypt_message_body(account_id, &queue_arn, &msg.body)?;
                plaintext_bodies.push(plaintext);
            }
        }

        // Push the popped messages onto inflight with their original
        // at-rest bodies (ciphertext for SendMessage-encrypted msgs,
        // plaintext for cross-service deliveries). Inflight has to
        // mirror the at-rest form so a later ChangeMessageVisibility
        // / inflight-expiry round-trip can re-decrypt without
        // corrupting anything.
        for msg in &received {
            queue.inflight.push(msg.clone());
        }

        // Move messages to DLQ — also with at-rest bodies untouched,
        // since the DLQ may have its own SSE settings and a downstream
        // receive call decrypts what's there. If the redrive target ARN
        // doesn't resolve to a known queue (DLQ deleted, ARN typo), the
        // message is returned to the source queue instead of being
        // dropped — real SQS never destroys a message over a
        // misconfigured redrive target.
        for (dlq_arn, mut msg) in dlq_messages.into_iter() {
            msg.receipt_handle = None;
            msg.visible_at = None;
            if let Some(dlq) = state.queues.values_mut().find(|q| q.arn == dlq_arn) {
                dlq.messages.push_back(msg);
            } else {
                tracing::warn!(
                    queue = %queue_arn,
                    dlq_arn = %dlq_arn,
                    "redrive target queue not found; returning message to source queue"
                );
                if let Some(source) = state.queues.values_mut().find(|q| q.arn == queue_arn) {
                    source.messages.push_back(msg);
                }
            }
        }

        // Now swap response bodies in to plaintext.
        if !plaintext_bodies.is_empty() {
            for (msg, plaintext) in received.iter_mut().zip(plaintext_bodies) {
                msg.body = plaintext;
            }
        }

        // Record the FIFO receive attempt so a retry with the same
        // ReceiveRequestAttemptId within the visibility window replays
        // this exact batch. The entry expires when the messages become
        // visible again.
        if is_fifo && !received.is_empty() {
            if let Some(attempt_id) = receive_request_attempt_id {
                if let Some(queue) = state.queues.get_mut(&resolved_url) {
                    queue.receive_attempt_cache.insert(
                        attempt_id.to_string(),
                        crate::state::ReceiveAttemptEntry {
                            message_ids: received.iter().map(|m| m.message_id.clone()).collect(),
                            expiry: now + chrono::Duration::seconds(visibility_timeout),
                        },
                    );
                }
            }
        }

        Ok(received)
    }

    pub(super) fn delete_message(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let receipt_handle = body["ReceiptHandle"]
            .as_str()
            .ok_or_else(|| missing_param("ReceiptHandle"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        // Find the message_id associated with this receipt handle
        let message_id = find_message_id_for_receipt(queue, receipt_handle);

        if let Some(msg_id) = message_id {
            // Delete by message_id (any receipt handle for this message works)
            // Keep the receipt_handle_map entry so subsequent deletes are idempotent
            queue.inflight.retain(|m| m.message_id != msg_id);
            queue.messages.retain(|m| m.message_id != msg_id);
        } else {
            // Receipt handle not found - error
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ReceiptHandleIsInvalid",
                "The input receipt handle is invalid.",
            ));
        }

        Ok(sqs_response(
            "DeleteMessage",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn change_message_visibility(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let receipt_handle = body["ReceiptHandle"]
            .as_str()
            .ok_or_else(|| missing_param("ReceiptHandle"))?;
        let visibility_timeout = val_as_i64(&body["VisibilityTimeout"])
            .ok_or_else(|| missing_param("VisibilityTimeout"))?;
        // AWS limits VisibilityTimeout to 0..=43200 seconds (12 h).
        // Anything outside that range is InvalidParameterValue.
        if !(0..=43200).contains(&visibility_timeout) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "VisibilityTimeout {visibility_timeout} is invalid. Reason: Must be between 0 and 43200 seconds."
                ),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let now = Utc::now();

        // Find the message_id associated with this receipt handle
        let message_id = find_message_id_for_receipt(queue, receipt_handle);

        if let Some(msg_id) = message_id {
            let new_visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
            let mut found = false;

            // Check inflight messages
            for msg in &mut queue.inflight {
                if msg.message_id == msg_id {
                    msg.visible_at = new_visible_at;
                    found = true;
                    break;
                }
            }

            // Also check messages queue (message may have become visible again)
            if !found {
                for msg in &mut queue.messages {
                    if msg.message_id == msg_id {
                        msg.visible_at = new_visible_at;
                        found = true;
                        break;
                    }
                }
            }

            if !found {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ReceiptHandleIsInvalid",
                    "The input receipt handle is invalid.",
                ));
            }
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ReceiptHandleIsInvalid",
                "The input receipt handle is invalid.",
            ));
        }

        Ok(sqs_response(
            "ChangeMessageVisibility",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn change_message_visibility_batch(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        // Entries omitted -> `EmptyBatchRequest`, which is declared on the
        // op. Returning `MissingParameter` here was undeclared and got
        // flagged by strict-mode conformance.
        let entries = body["Entries"].as_array().cloned().unwrap_or_default();
        if entries.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.EmptyBatchRequest",
                "There should be at least one entry in the request.",
            ));
        }

        // AWS caps a batch at 10 entries; over that it returns
        // TooManyEntriesInBatchRequest (matches SendMessageBatch).
        if entries.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
                format!(
                    "Maximum number of entries per request are 10. You have sent {}.",
                    entries.len()
                ),
            ));
        }

        // Reject duplicate batch entry IDs (matches the other batch ops).
        let mut seen_ids = std::collections::HashSet::new();
        for entry in &entries {
            if let Some(id) = entry["Id"].as_str() {
                if !seen_ids.insert(id.to_string()) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                        "Two or more batch entries in the operation have the same Id.",
                    ));
                }
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let now = Utc::now();
        let mut successful = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        for entry in &entries {
            let id = match entry["Id"].as_str() {
                Some(id) => id.to_string(),
                None => continue,
            };
            let receipt_handle = match entry["ReceiptHandle"].as_str() {
                Some(rh) => rh,
                None => {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "MissingParameter",
                        "Message": "ReceiptHandle is required",
                    }));
                    continue;
                }
            };
            let visibility_timeout = match val_as_i64(&entry["VisibilityTimeout"]) {
                Some(vt) => vt,
                None => {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "MissingParameter",
                        "Message": "VisibilityTimeout is required",
                    }));
                    continue;
                }
            };
            // AWS limits VisibilityTimeout to 0..=43200 seconds (12 h). Out-of-range values
            // are a per-entry InvalidParameterValue, mirroring the single-op path; without
            // this guard a huge value overflows `now + Duration::seconds(..)` and panics the
            // request thread.
            if !(0..=43200).contains(&visibility_timeout) {
                failed.push(json!({
                    "Id": id,
                    "SenderFault": true,
                    "Code": "InvalidParameterValue",
                    "Message": format!(
                        "VisibilityTimeout {visibility_timeout} is invalid. Reason: Must be between 0 and 43200 seconds."
                    ),
                }));
                continue;
            }

            let message_id = find_message_id_for_receipt(queue, receipt_handle);

            if let Some(msg_id) = message_id {
                let new_visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
                let mut found = false;

                for msg in &mut queue.inflight {
                    if msg.message_id == msg_id {
                        msg.visible_at = new_visible_at;
                        found = true;
                        break;
                    }
                }

                if !found {
                    for msg in &mut queue.messages {
                        if msg.message_id == msg_id {
                            msg.visible_at = new_visible_at;
                            found = true;
                            break;
                        }
                    }
                }

                if found {
                    successful.push(json!({ "Id": id }));
                } else {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "ReceiptHandleIsInvalid",
                        "Message": "The input receipt handle is invalid.",
                    }));
                }
            } else {
                failed.push(json!({
                    "Id": id,
                    "SenderFault": true,
                    "Code": "ReceiptHandleIsInvalid",
                    "Message": "The input receipt handle is invalid.",
                }));
            }
        }

        Ok(sqs_response(
            "ChangeMessageVisibilityBatch",
            json!({
                "Successful": successful,
                "Failed": failed,
            }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn send_message_batch(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?
            .to_string();

        // Entries omitted -> `EmptyBatchRequest` (declared) instead of
        // the undeclared `MissingParameter`.
        let entries = body["Entries"].as_array().cloned().unwrap_or_default();

        // Validate batch is not empty
        if entries.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.EmptyBatchRequest",
                "There should be at least one SendMessageBatchRequestEntry in the request.",
            ));
        }

        // Max 10 entries
        if entries.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
                format!(
                    "Maximum number of entries per request are 10. You have sent {}.",
                    entries.len()
                ),
            ));
        }

        // Validate entry IDs and check for duplicates
        let mut seen_ids: Vec<String> = Vec::new();
        for entry in &entries {
            if let Some(id) = entry["Id"].as_str() {
                // Validate ID format
                if !is_valid_batch_id(id) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AWS.SimpleQueueService.InvalidBatchEntryId",
                        "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.",
                    ));
                }
                if seen_ids.contains(&id.to_string()) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                        format!("Id {} repeated.", id),
                    ));
                }
                seen_ids.push(id.to_string());
            }
        }

        // Validate total batch size. AWS counts each entry's body plus
        // its message attributes (name + type + value) toward the
        // 256 KiB batch ceiling, not the bodies alone (bug-audit
        // 2026-06-13, 1.13).
        let total_size: usize = entries
            .iter()
            .map(|e| {
                let body_len = e["MessageBody"].as_str().map(str::len).unwrap_or(0);
                let attrs = parse_message_attributes(e);
                body_len + message_attributes_size(&attrs)
            })
            .sum();
        if total_size > 262_144 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.BatchRequestTooLong",
                format!(
                    "Batch requests cannot be longer than 262144 bytes. You have sent {} bytes.",
                    total_size
                ),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(&queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let now = Utc::now();
        let mut successful = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        let cfg = BatchSendConfig::from_queue(queue, now);

        // At-rest encryption: pre-encrypt every entry's body before
        // handing it to the per-entry helper. Encryption is queue-wide
        // and covers both SSE-KMS and SSE-SQS modes, so a KMS failure
        // here aborts the whole batch (matches AWS behavior: when the
        // queue's CMK is unavailable, no entry can succeed). Skip
        // encryption for entries that won't pass per-entry validation
        // — otherwise an invalid entry would still record a KMS
        // GenerateDataKey call and inflate the audit trail.
        let kms_key_id = Self::effective_kms_key_id(&queue.attributes);
        let queue_arn = queue.arn.clone();
        let mut stored_overrides: Vec<Option<String>> = Vec::with_capacity(entries.len());
        if kms_key_id.is_some() && self.kms_hook.is_some() {
            for entry in &entries {
                let body = entry["MessageBody"].as_str();
                let attrs_size = message_attributes_size(&parse_message_attributes(entry));
                let body_size_ok =
                    body.is_some_and(|b| b.len() + attrs_size <= cfg.max_message_size);
                let id_ok = entry["Id"].as_str().is_some_and(is_valid_batch_id);
                let delay_ok = match val_as_i64(&entry["DelaySeconds"]) {
                    Some(d) => (0..=900).contains(&d),
                    None => true,
                };
                if body_size_ok && id_ok && delay_ok {
                    let stored = self.encrypt_message_body(
                        &req.account_id,
                        &queue_arn,
                        kms_key_id.as_deref(),
                        body.unwrap(),
                    )?;
                    stored_overrides.push(Some(stored));
                } else {
                    stored_overrides.push(None);
                }
            }
        } else {
            stored_overrides.resize(entries.len(), None);
        }

        for (entry, override_body) in entries.iter().zip(stored_overrides) {
            match process_batch_send_entry(queue, entry, &cfg, override_body)? {
                BatchEntryOutcome::Success(v) => successful.push(v),
                BatchEntryOutcome::Failure(v) => failed.push(v),
            }
        }

        Ok(sqs_response(
            "SendMessageBatch",
            json!({
                "Successful": successful,
                "Failed": failed,
            }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn delete_message_batch(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        // Entries omitted -> `EmptyBatchRequest` (declared) instead of
        // the undeclared `MissingParameter`.
        let entries = body["Entries"].as_array().cloned().unwrap_or_default();

        // Validate batch is not empty
        if entries.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.EmptyBatchRequest",
                "There should be at least one DeleteMessageBatchRequestEntry in the request.",
            ));
        }

        // AWS caps a batch at 10 entries; over that it returns
        // TooManyEntriesInBatchRequest (matches SendMessageBatch).
        if entries.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
                format!(
                    "Maximum number of entries per request are 10. You have sent {}.",
                    entries.len()
                ),
            ));
        }

        // Check for duplicate IDs
        let mut seen_ids = std::collections::HashSet::new();
        for entry in &entries {
            if let Some(id) = entry["Id"].as_str() {
                if !seen_ids.insert(id.to_string()) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                        "Two or more batch entries in the operation have the same Id.",
                    ));
                }
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let mut successful = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        for entry in &entries {
            let id = match entry["Id"].as_str() {
                Some(id) => id.to_string(),
                None => continue,
            };
            let receipt_handle = match entry["ReceiptHandle"].as_str() {
                Some(rh) => rh,
                None => {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "MissingParameter",
                        "Message": "ReceiptHandle is required",
                    }));
                    continue;
                }
            };

            let message_id = find_message_id_for_receipt(queue, receipt_handle);

            if let Some(msg_id) = message_id {
                queue.inflight.retain(|m| m.message_id != msg_id);
                queue.messages.retain(|m| m.message_id != msg_id);
                successful.push(json!({ "Id": id }));
            } else {
                failed.push(json!({
                    "Id": id,
                    "SenderFault": true,
                    "Code": "ReceiptHandleIsInvalid",
                    "Message": format!(
                        "The input receipt handle \"{}\" is not a valid receipt handle.",
                        receipt_handle
                    ),
                }));
            }
        }

        Ok(sqs_response(
            "DeleteMessageBatch",
            json!({
                "Successful": successful,
                "Failed": failed,
            }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }
}
