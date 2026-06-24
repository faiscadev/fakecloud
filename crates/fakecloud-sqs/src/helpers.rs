use super::*;

/// Heuristic: does this body look like a fakecloud-kms envelope produced
/// by `KmsServiceHook::encrypt`?
///
/// Two encodings are recognized:
///  - The current AWS-shaped binary blob: base64 of bytes starting with
///    the four-byte version header `0x01 0x02 0x02 0x00`.
///  - The legacy textual envelope `fakecloud-kms:<key>:<base64>` (kept
///    around for back-compat with older snapshots).
///
/// Any other body is treated as plaintext and the SQS receive path skips
/// the KMS hook entirely. This is what lets cross-service deliveries
/// (SNS fanout, EventBridge target, S3 / DDB notifications) survive on a
/// queue with `SqsManagedSseEnabled=true` — they bypass the SQS service
/// and write plaintext directly, so insisting on a successful decrypt
/// would silently drop every notification.
pub(crate) fn looks_like_fakecloud_envelope(body: &str) -> bool {
    use base64::Engine;
    if body.starts_with("fakecloud-kms:") {
        return true;
    }
    match base64::engine::general_purpose::STANDARD.decode(body) {
        Ok(bytes) => bytes.starts_with(&[0x01, 0x02, 0x02, 0x00]),
        Err(_) => false,
    }
}

/// Validate DelaySeconds (0–900) and MaximumMessageSize (1024–1 MiB) if
/// present in the caller-supplied queue attributes. Both match AWS's
/// documented ranges; we return the same error code/message the real
/// service does.
pub(crate) fn validate_create_queue_attributes(
    attrs: &BTreeMap<String, String>,
) -> Result<(), AwsServiceError> {
    if let Some(ds) = attrs.get("DelaySeconds") {
        match ds.parse::<i64>() {
            Ok(d) if (0..=900).contains(&d) => {}
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidAttributeValue",
                    "Invalid value for the parameter DelaySeconds.".to_string(),
                ));
            }
        }
    }

    if let Some(mms) = attrs.get("MaximumMessageSize") {
        if let Ok(size) = mms.parse::<u64>() {
            if !(1024..=1_048_576).contains(&size) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidAttributeValue",
                    "Invalid value for the parameter MaximumMessageSize.",
                ));
            }
        }
    }

    Ok(())
}

/// If the attribute is JSON-valued, parse + re-emit as compact JSON.
/// Otherwise the value is passed through unchanged. Invalid JSON is also
/// passed through; downstream validators reject it with proper errors.
pub(crate) fn canonicalize_json_attr(name: &str, value: String) -> String {
    if !JSON_VALUED_ATTRS.contains(&name) {
        return value;
    }
    match serde_json::from_str::<Value>(&value) {
        Ok(parsed) => serde_json::to_string(&parsed).unwrap_or(value),
        Err(_) => value,
    }
}

/// Parse the JSON stored under the `RedrivePolicy` queue attribute into
/// a typed `RedrivePolicy`. AWS accepts both string and integer encodings
/// of `maxReceiveCount`, so we tolerate both. Returns `None` for any
/// parse failure (the caller treats it as "no redrive policy").
pub(crate) fn parse_redrive_policy(attr_str: &str) -> Option<RedrivePolicy> {
    let rp: Value = serde_json::from_str(attr_str).ok()?;
    let dead_letter_target_arn = rp["deadLetterTargetArn"].as_str()?.to_string();
    let max_receive_count = rp["maxReceiveCount"]
        .as_u64()
        .or_else(|| rp["maxReceiveCount"].as_str()?.parse().ok())?
        as u32;
    Some(RedrivePolicy {
        dead_letter_target_arn,
        max_receive_count,
    })
}

/// Validate FIFO-specific send-message constraints: delay must be 0,
/// MessageGroupId is required, and either MessageDeduplicationId must be
/// supplied or the queue must have ContentBasedDeduplication enabled.
/// No-op for non-FIFO queues.
pub(crate) fn check_fifo_send_constraints(
    queue: &SqsQueue,
    raw_delay: Option<i64>,
    message_group_id: &Option<String>,
    message_dedup_id: &Option<String>,
) -> Result<(), AwsServiceError> {
    if !queue.is_fifo {
        return Ok(());
    }

    let delay = raw_delay.unwrap_or(0);
    if delay != 0 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!(
                "Value {delay} for parameter DelaySeconds is invalid. Reason: \
                 The request include parameter that is not valid for this queue type."
            ),
        ));
    }

    if message_group_id.is_none() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MissingParameter",
            "The request must contain the parameter MessageGroupId.",
        ));
    }

    if message_dedup_id.is_none()
        && queue
            .attributes
            .get("ContentBasedDeduplication")
            .map(|v| v.as_str())
            != Some("true")
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "The queue should either have ContentBasedDeduplication enabled \
             or MessageDeduplicationId provided explicitly",
        ));
    }

    Ok(())
}

pub(crate) fn batch_failure(id: &str, code: &str, message: impl Into<String>) -> Value {
    json!({
        "Id": id,
        "SenderFault": true,
        "Code": code,
        "Message": message.into(),
    })
}

/// Process a single SendMessageBatch entry: validate per-entry constraints,
/// enqueue the message, and return either a successful or failed response
/// fragment. Returns `Err` only for errors that AWS signals at the
/// *batch* level (missing MessageGroupId / dedup on FIFO queues).
///
/// `stored_body_override` lets the caller pre-encrypt the message body
/// for SSE-KMS queues without losing the plaintext MD5 the response
/// must return (AWS surfaces the plaintext MD5 even for encrypted
/// queues).
pub(crate) fn process_batch_send_entry(
    queue: &mut SqsQueue,
    entry: &Value,
    cfg: &BatchSendConfig,
    stored_body_override: Option<String>,
) -> Result<BatchEntryOutcome, AwsServiceError> {
    let id = match entry["Id"].as_str() {
        Some(id) => id.to_string(),
        None => {
            return Ok(BatchEntryOutcome::Failure(batch_failure(
                "",
                "MissingParameter",
                "Id is required",
            )))
        }
    };

    let Some(message_body) = entry["MessageBody"].as_str().map(|s| s.to_string()) else {
        return Ok(BatchEntryOutcome::Failure(batch_failure(
            &id,
            "MissingParameter",
            "MessageBody is required",
        )));
    };

    // AWS measures body + message-attribute size (name + type + value)
    // against MaximumMessageSize, not the body alone (bug-audit
    // 2026-06-13, 1.13).
    let entry_attributes = parse_message_attributes(entry);
    let entry_size = message_body.len() + message_attributes_size(&entry_attributes);
    if entry_size > cfg.max_message_size {
        return Ok(BatchEntryOutcome::Failure(batch_failure(
            &id,
            "InvalidParameterValue",
            format!(
                "One or more parameters are invalid. Reason: Message must be shorter than {} bytes.",
                cfg.max_message_size
            ),
        )));
    }

    if let Some(d) = val_as_i64(&entry["DelaySeconds"]) {
        if !(0..=900).contains(&d) {
            return Ok(BatchEntryOutcome::Failure(batch_failure(
                &id,
                "InvalidParameterValue",
                format!(
                    "Value {d} for parameter DelaySeconds is invalid. Reason: \
                     Must be between 0 and 900, if provided."
                ),
            )));
        }
    }

    let message_group_id = entry["MessageGroupId"].as_str().map(|s| s.to_string());
    let message_dedup_id = entry["MessageDeduplicationId"]
        .as_str()
        .map(|s| s.to_string());

    if cfg.is_fifo {
        if message_group_id.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter MessageGroupId.",
            ));
        }
        if message_dedup_id.is_none() && !cfg.content_based_dedup {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "The queue should either have ContentBasedDeduplication enabled \
                 or MessageDeduplicationId provided explicitly",
            ));
        }
    }

    let delay: i64 = val_as_i64(&entry["DelaySeconds"])
        .or(cfg.queue_delay)
        .unwrap_or(0);
    let visible_at = if delay > 0 {
        Some(cfg.now + chrono::Duration::seconds(delay))
    } else {
        None
    };

    let message_attributes = entry_attributes;
    // System attributes (e.g. AWSTraceHeader for X-Ray) must be stored, not
    // dropped — the single-send path stores them, and the batch response
    // already advertises their MD5, so dropping them silently lost the
    // X-Ray trace context for batched sends.
    let system_attributes = parse_message_system_attributes(entry);

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

    // FIFO content-based deduplication. The single-send path computes an
    // effective dedup id (explicit id, else the body SHA-256 when
    // ContentBasedDeduplication is on) and replays the original
    // MessageId/SequenceNumber for a duplicate within the 5-minute window
    // without enqueuing again or advancing the sequence counter. The batch
    // path skipped all of this, so duplicate-body batched messages all
    // enqueued.
    let effective_dedup_id = if cfg.is_fifo {
        message_dedup_id
            .clone()
            .or_else(|| cfg.content_based_dedup.then(|| sha256_hex(&message_body)))
    } else {
        None
    };
    if cfg.is_fifo {
        if let Some(ref dedup_id) = effective_dedup_id {
            queue.dedup_cache.retain(|_, e| e.expiry > cfg.now);
            if let Some(existing) = queue.dedup_cache.get(dedup_id) {
                let mut entry_resp = json!({
                    "Id": id,
                    "MessageId": existing.message_id,
                    "MD5OfMessageBody": md5_of_body,
                });
                if let Some(ref seq) = existing.sequence_number {
                    entry_resp["SequenceNumber"] = json!(seq);
                }
                if let Some(md5) = &md5_of_attrs {
                    entry_resp["MD5OfMessageAttributes"] = json!(md5);
                }
                if let Some(md5) = &md5_of_system_attrs {
                    entry_resp["MD5OfMessageSystemAttributes"] = json!(md5);
                }
                return Ok(BatchEntryOutcome::Success(entry_resp));
            }
        }
    }

    let sequence_number = if cfg.is_fifo {
        let seq = queue.next_sequence_number;
        queue.next_sequence_number += 1;
        Some(seq.to_string())
    } else {
        None
    };

    let msg = SqsMessage {
        message_id: uuid::Uuid::new_v4().to_string(),
        receipt_handle: None,
        md5_of_body: md5_of_body.clone(),
        body: stored_body_override.unwrap_or(message_body),
        sent_timestamp: cfg.now.timestamp_millis(),
        attributes: system_attributes,
        message_attributes,
        visible_at,
        receive_count: 0,
        message_group_id,
        message_dedup_id: effective_dedup_id.clone(),
        created_at: cfg.now,
        sequence_number: sequence_number.clone(),
    };
    let message_id = msg.message_id.clone();

    let mut entry_resp = json!({
        "Id": id,
        "MessageId": message_id,
        "MD5OfMessageBody": md5_of_body,
    });
    if let Some(seq) = &sequence_number {
        entry_resp["SequenceNumber"] = json!(seq);
    }
    if let Some(md5) = &md5_of_attrs {
        entry_resp["MD5OfMessageAttributes"] = json!(md5);
    }
    if let Some(md5) = &md5_of_system_attrs {
        entry_resp["MD5OfMessageSystemAttributes"] = json!(md5);
    }
    queue.messages.push_back(msg);

    if cfg.is_fifo {
        if let Some(dedup_id) = effective_dedup_id {
            queue.dedup_cache.insert(
                dedup_id,
                crate::state::DedupEntry {
                    message_id,
                    sequence_number,
                    expiry: cfg.now + chrono::Duration::minutes(5),
                },
            );
        }
    }
    Ok(BatchEntryOutcome::Success(entry_resp))
}

/// Verify that the DLQ referenced by `rp` actually exists, and — when
/// the source queue is FIFO — that the DLQ is itself a FIFO queue.
/// Mirrors AWS's constraint that FIFO and standard queues cannot be
/// paired across a redrive boundary.
pub(crate) fn validate_redrive_policy_target(
    state: &SqsState,
    rp: &RedrivePolicy,
    is_fifo: bool,
) -> Result<(), AwsServiceError> {
    let dlq = state
        .queues
        .values()
        .find(|q| q.arn == rp.dead_letter_target_arn);

    let Some(dlq) = dlq else {
        return Err(AwsServiceError::aws_error_with_headers(
            StatusCode::BAD_REQUEST,
            "AWS.SimpleQueueService.NonExistentQueue",
            format!(
                "Dead letter target does not exist: {}",
                rp.dead_letter_target_arn
            ),
            vec![(
                "x-amzn-query-error".to_string(),
                "AWS.SimpleQueueService.NonExistentQueue;Sender".to_string(),
            )],
        ));
    };

    if is_fifo && !dlq.is_fifo {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "Dead-letter queue must be the same type of queue as the source.",
        ));
    }

    Ok(())
}

/// Actions that mutate SQS state. Kept in sync with the dispatch table.
pub(crate) fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "CreateQueue"
            | "DeleteQueue"
            | "SetQueueAttributes"
            | "SendMessage"
            | "SendMessageBatch"
            | "ReceiveMessage"
            | "DeleteMessage"
            | "DeleteMessageBatch"
            | "PurgeQueue"
            | "ChangeMessageVisibility"
            | "ChangeMessageVisibilityBatch"
            | "TagQueue"
            | "UntagQueue"
            | "AddPermission"
            | "RemovePermission"
            | "StartMessageMoveTask"
            | "CancelMessageMoveTask"
    )
}

/// Build the SQS resource ARN for an incoming action. Returns `*` for
/// account-level actions or when the queue can't be identified from the
/// request.
pub(crate) fn sqs_resource_for(action: &str, account: &str, region: &str, body: &Value) -> String {
    let partition = if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else {
        "aws"
    };
    let queue_arn = |name: &str| format!("arn:{}:sqs:{}:{}:{}", partition, region, account, name);
    match action {
        "ListQueues" => "*".to_string(),
        "CreateQueue" | "GetQueueUrl" => body
            .get("QueueName")
            .and_then(|v| v.as_str())
            .map(queue_arn)
            .unwrap_or_else(|| "*".to_string()),
        "StartMessageMoveTask" | "ListMessageMoveTasks" => body
            .get("SourceArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "*".to_string()),
        "CancelMessageMoveTask" => "*".to_string(),
        _ => body
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .and_then(|url| url.rsplit('/').next())
            .map(queue_arn)
            .unwrap_or_else(|| "*".to_string()),
    }
}

pub(crate) fn finalize_move_task(
    state_handle: &SharedSqsState,
    account_id: &str,
    region: &str,
    task_handle: &str,
    final_status: MessageMoveTaskStatus,
) {
    let _ = region; // currently only used for symmetry with constructor
    let mut accounts = state_handle.write();
    let state = accounts.get_or_create(account_id);
    if let Some(task) = state
        .message_move_tasks
        .iter_mut()
        .find(|t| t.task_handle == task_handle)
    {
        task.status = final_status;
    }
}

/// Parse the request body. SQS supports both JSON protocol (modern SDKs like aws-sdk-rust)
/// and Query protocol (boto3, older SDKs). For Query protocol, params are in query_params.
pub(crate) fn parse_body(req: &AwsRequest) -> Value {
    // Try JSON first
    if let Ok(v) = serde_json::from_slice::<Value>(&req.body) {
        if v.is_object() && !v.as_object().unwrap().is_empty() {
            return v;
        }
    }
    // Fall back to query params (Query protocol / form-encoded)
    if !req.query_params.is_empty() {
        let mut map = serde_json::Map::new();
        for (k, v) in &req.query_params {
            map.insert(k.clone(), Value::String(v.clone()));
        }
        // Handle nested Attribute.N.Name/Value patterns
        let mut attrs = serde_json::Map::new();
        for i in 1..=20 {
            let name_key = format!("Attribute.{i}.Name");
            let value_key = format!("Attribute.{i}.Value");
            if let (Some(name), Some(value)) = (
                req.query_params.get(&name_key),
                req.query_params.get(&value_key),
            ) {
                attrs.insert(name.clone(), Value::String(value.clone()));
            }
        }
        if !attrs.is_empty() {
            map.insert("Attributes".to_string(), Value::Object(attrs));
        }
        // Handle AttributeName.N patterns (used by GetQueueAttributes in query protocol)
        let mut attr_names = Vec::new();
        for i in 1..=20 {
            let key = format!("AttributeName.{i}");
            if let Some(val) = req.query_params.get(&key) {
                attr_names.push(Value::String(val.clone()));
            }
        }
        if !attr_names.is_empty() {
            map.insert("AttributeNames".to_string(), Value::Array(attr_names));
        }
        // Handle batch entry patterns: *Entry.N.Field or *.N.Field
        // e.g. SendMessageBatchRequestEntry.1.Id=foo&SendMessageBatchRequestEntry.1.MessageBody=bar
        // Also: DeleteMessageBatchRequestEntry.1.Id=...&DeleteMessageBatchRequestEntry.1.ReceiptHandle=...
        // Also: ChangeMessageVisibilityBatchRequestEntry.1.Id=...
        let entries = parse_batch_entries(&req.query_params);
        if !entries.is_empty() {
            map.insert("Entries".to_string(), Value::Array(entries));
        }
        return Value::Object(map);
    }
    Value::Object(Default::default())
}

/// Parse batch entry parameters like `SendMessageBatchRequestEntry.1.Id=foo`.
/// Returns a Vec of JSON objects, one per entry index.
pub(crate) fn parse_batch_entries(params: &HashMap<String, String>) -> Vec<Value> {
    use std::collections::BTreeMap;

    // Find all entry-like keys: anything matching *.N.Field pattern
    let mut entries_map: BTreeMap<u32, serde_json::Map<String, Value>> = BTreeMap::new();

    for (key, value) in params {
        // Match patterns like "SomethingEntry.N.Field" or "Entries.member.N.Field"
        let parts: Vec<&str> = key.split('.').collect();
        if parts.len() >= 3 {
            // Try to find the numeric index
            for (i, part) in parts.iter().enumerate() {
                if let Ok(idx) = part.parse::<u32>() {
                    // Everything after the index is the field name
                    let field = parts[i + 1..].join(".");
                    if !field.is_empty() {
                        entries_map
                            .entry(idx)
                            .or_default()
                            .insert(field, Value::String(value.clone()));
                    }
                    break;
                }
            }
        }
    }

    entries_map.into_values().map(Value::Object).collect()
}

/// Extract an i64 from a Value that might be a number or a string (Query protocol sends strings).
pub(crate) fn val_as_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

pub(crate) fn xml_metadata_only(action: &str, request_id: &str) -> AwsResponse {
    let xml = query_metadata_only_xml(action, SQS_NS, request_id);
    AwsResponse::xml(StatusCode::OK, xml)
}

pub(crate) fn sqs_response(
    action: &str,
    body: Value,
    request_id: &str,
    is_query: bool,
) -> AwsResponse {
    if !is_query {
        return AwsResponse::ok_json(body);
    }
    let inner = match action {
        "CreateQueue" | "GetQueueUrl" => xml_queue_url_only(&body),
        "ListQueues" => xml_list_queues(&body),
        "SendMessage" => xml_send_message(&body),
        "ReceiveMessage" => xml_receive_message(&body),
        "GetQueueAttributes" => xml_attributes_list(&body),
        "SendMessageBatch" => xml_batch_result(
            &body,
            "SendMessageBatchResultEntry",
            xml_send_message_batch_success,
        ),
        "DeleteMessageBatch" => {
            xml_batch_result(&body, "DeleteMessageBatchResultEntry", xml_id_only_success)
        }
        "ChangeMessageVisibilityBatch" => xml_batch_result(
            &body,
            "ChangeMessageVisibilityBatchResultEntry",
            xml_id_only_success,
        ),
        "ListDeadLetterSourceQueues" => xml_dlq_sources(&body),
        "StartMessageMoveTask" => xml_start_message_move_task(&body),
        "CancelMessageMoveTask" => xml_cancel_message_move_task(&body),
        "ListMessageMoveTasks" => xml_list_message_move_tasks(&body),
        // DeleteQueue, DeleteMessage, PurgeQueue, SetQueueAttributes, ChangeMessageVisibility
        _ => return xml_metadata_only(action, request_id),
    };
    AwsResponse::xml(
        StatusCode::OK,
        query_response_xml(action, SQS_NS, &inner, request_id),
    )
}

pub(crate) fn xml_queue_url_only(body: &Value) -> String {
    let url = body["QueueUrl"].as_str().unwrap_or("");
    format!("<QueueUrl>{}</QueueUrl>", xml_escape(url))
}

pub(crate) fn xml_list_queues(body: &Value) -> String {
    let mut inner = String::new();
    if let Some(urls) = body["QueueUrls"].as_array() {
        for url in urls {
            if let Some(u) = url.as_str() {
                inner.push_str(&format!("<QueueUrl>{}</QueueUrl>", xml_escape(u)));
            }
        }
    }
    if let Some(token) = body["NextToken"].as_str() {
        inner.push_str(&format!("<NextToken>{}</NextToken>", xml_escape(token)));
    }
    inner
}

pub(crate) fn xml_send_message(body: &Value) -> String {
    let msg_id = body["MessageId"].as_str().unwrap_or("");
    let md5 = body["MD5OfMessageBody"].as_str().unwrap_or("");
    format!(
        "<MessageId>{}</MessageId><MD5OfMessageBody>{}</MD5OfMessageBody>",
        xml_escape(msg_id),
        xml_escape(md5)
    )
}

pub(crate) fn xml_receive_message(body: &Value) -> String {
    let Some(messages) = body["Messages"].as_array() else {
        return String::new();
    };
    let mut inner = String::new();
    for msg in messages {
        inner.push_str("<Message>");
        if let Some(id) = msg["MessageId"].as_str() {
            inner.push_str(&format!("<MessageId>{}</MessageId>", xml_escape(id)));
        }
        if let Some(rh) = msg["ReceiptHandle"].as_str() {
            inner.push_str(&format!(
                "<ReceiptHandle>{}</ReceiptHandle>",
                xml_escape(rh)
            ));
        }
        if let Some(md5) = msg["MD5OfBody"].as_str() {
            inner.push_str(&format!("<MD5OfBody>{}</MD5OfBody>", xml_escape(md5)));
        }
        if let Some(body_str) = msg["Body"].as_str() {
            inner.push_str(&format!("<Body>{}</Body>", xml_escape(body_str)));
        }
        if let Some(attrs) = msg["Attributes"].as_object() {
            for (k, v) in attrs {
                if let Some(val) = v.as_str() {
                    inner.push_str(&format!(
                        "<Attribute><Name>{}</Name><Value>{}</Value></Attribute>",
                        xml_escape(k),
                        xml_escape(val)
                    ));
                }
            }
        }
        if let Some(msg_attrs) = msg["MessageAttributes"].as_object() {
            for (name, attr) in msg_attrs {
                inner.push_str("<MessageAttribute>");
                inner.push_str(&format!("<Name>{}</Name>", xml_escape(name)));
                inner.push_str("<Value>");
                if let Some(dt) = attr["DataType"].as_str() {
                    inner.push_str(&format!("<DataType>{}</DataType>", xml_escape(dt)));
                }
                if let Some(sv) = attr["StringValue"].as_str() {
                    inner.push_str(&format!("<StringValue>{}</StringValue>", xml_escape(sv)));
                }
                inner.push_str("</Value>");
                inner.push_str("</MessageAttribute>");
            }
        }
        inner.push_str("</Message>");
    }
    inner
}

pub(crate) fn xml_attributes_list(body: &Value) -> String {
    let Some(attrs) = body["Attributes"].as_object() else {
        return String::new();
    };
    let mut inner = String::new();
    for (k, v) in attrs {
        let val = v.as_str().unwrap_or("");
        inner.push_str(&format!(
            "<Attribute><Name>{}</Name><Value>{}</Value></Attribute>",
            xml_escape(k),
            xml_escape(val)
        ));
    }
    inner
}

/// Serialize the Successful + Failed arrays for a `*Batch` response, using
/// `success_tag` as the wrapper tag for each successful entry and
/// `success_body` to write the per-entry body. Failed entries always use
/// the shared `BatchResultErrorEntry` shape.
pub(crate) fn xml_batch_result(
    body: &Value,
    success_tag: &str,
    success_body: fn(&Value) -> String,
) -> String {
    let mut inner = String::new();
    if let Some(successful) = body["Successful"].as_array() {
        for entry in successful {
            inner.push_str(&format!("<{success_tag}>"));
            inner.push_str(&success_body(entry));
            inner.push_str(&format!("</{success_tag}>"));
        }
    }
    if let Some(failed) = body["Failed"].as_array() {
        for entry in failed {
            inner.push_str(&xml_batch_error_entry(entry));
        }
    }
    inner
}

pub(crate) fn xml_id_only_success(entry: &Value) -> String {
    if let Some(id) = entry["Id"].as_str() {
        format!("<Id>{}</Id>", xml_escape(id))
    } else {
        String::new()
    }
}

pub(crate) fn xml_send_message_batch_success(entry: &Value) -> String {
    let mut out = String::new();
    if let Some(id) = entry["Id"].as_str() {
        out.push_str(&format!("<Id>{}</Id>", xml_escape(id)));
    }
    if let Some(msg_id) = entry["MessageId"].as_str() {
        out.push_str(&format!("<MessageId>{}</MessageId>", xml_escape(msg_id)));
    }
    if let Some(md5) = entry["MD5OfMessageBody"].as_str() {
        out.push_str(&format!(
            "<MD5OfMessageBody>{}</MD5OfMessageBody>",
            xml_escape(md5)
        ));
    }
    out
}

pub(crate) fn xml_batch_error_entry(entry: &Value) -> String {
    let mut out = String::from("<BatchResultErrorEntry>");
    if let Some(id) = entry["Id"].as_str() {
        out.push_str(&format!("<Id>{}</Id>", xml_escape(id)));
    }
    if let Some(code) = entry["Code"].as_str() {
        out.push_str(&format!("<Code>{}</Code>", xml_escape(code)));
    }
    if let Some(msg) = entry["Message"].as_str() {
        out.push_str(&format!("<Message>{}</Message>", xml_escape(msg)));
    }
    if let Some(sf) = entry["SenderFault"].as_bool() {
        out.push_str(&format!("<SenderFault>{sf}</SenderFault>"));
    }
    out.push_str("</BatchResultErrorEntry>");
    out
}

pub(crate) fn xml_dlq_sources(body: &Value) -> String {
    let Some(urls) = body["queueUrls"].as_array() else {
        return String::new();
    };
    let mut inner = String::new();
    for url in urls {
        if let Some(u) = url.as_str() {
            inner.push_str(&format!("<QueueUrl>{}</QueueUrl>", xml_escape(u)));
        }
    }
    inner
}

pub(crate) fn xml_start_message_move_task(body: &Value) -> String {
    let handle = body["TaskHandle"].as_str().unwrap_or("");
    format!("<TaskHandle>{}</TaskHandle>", xml_escape(handle))
}

pub(crate) fn xml_cancel_message_move_task(body: &Value) -> String {
    let n = body["ApproximateNumberOfMessagesMoved"]
        .as_u64()
        .unwrap_or(0);
    format!("<ApproximateNumberOfMessagesMoved>{n}</ApproximateNumberOfMessagesMoved>")
}

pub(crate) fn xml_list_message_move_tasks(body: &Value) -> String {
    let Some(results) = body["Results"].as_array() else {
        return String::new();
    };
    let mut inner = String::new();
    for r in results {
        inner.push_str("<ListMessageMoveTasksResultEntry>");
        if let Some(h) = r["TaskHandle"].as_str() {
            inner.push_str(&format!("<TaskHandle>{}</TaskHandle>", xml_escape(h)));
        }
        if let Some(s) = r["Status"].as_str() {
            inner.push_str(&format!("<Status>{}</Status>", xml_escape(s)));
        }
        if let Some(s) = r["SourceArn"].as_str() {
            inner.push_str(&format!("<SourceArn>{}</SourceArn>", xml_escape(s)));
        }
        if let Some(d) = r["DestinationArn"].as_str() {
            inner.push_str(&format!(
                "<DestinationArn>{}</DestinationArn>",
                xml_escape(d)
            ));
        }
        if let Some(m) = r["MaxNumberOfMessagesPerSecond"].as_i64() {
            inner.push_str(&format!(
                "<MaxNumberOfMessagesPerSecond>{m}</MaxNumberOfMessagesPerSecond>"
            ));
        }
        let moved = r["ApproximateNumberOfMessagesMoved"].as_u64().unwrap_or(0);
        inner.push_str(&format!(
            "<ApproximateNumberOfMessagesMoved>{moved}</ApproximateNumberOfMessagesMoved>"
        ));
        if let Some(t) = r["ApproximateNumberOfMessagesToMove"].as_u64() {
            inner.push_str(&format!(
                "<ApproximateNumberOfMessagesToMove>{t}</ApproximateNumberOfMessagesToMove>"
            ));
        }
        if let Some(reason) = r["FailureReason"].as_str() {
            inner.push_str(&format!(
                "<FailureReason>{}</FailureReason>",
                xml_escape(reason)
            ));
        }
        let started = r["StartedTimestamp"].as_i64().unwrap_or(0);
        inner.push_str(&format!("<StartedTimestamp>{started}</StartedTimestamp>"));
        inner.push_str("</ListMessageMoveTasksResultEntry>");
    }
    inner
}

pub(crate) fn format_receive_response(
    received: &[SqsMessage],
    request_id: &str,
    is_query: bool,
    requested_sys_attrs: Option<&[String]>,
    msg_attr_names: Option<&[String]>,
) -> AwsResponse {
    let now_millis = Utc::now().timestamp_millis();

    let messages: Vec<Value> = received
        .iter()
        .map(|m| {
            let mut msg_json = json!({
                "MessageId": m.message_id,
                "ReceiptHandle": m.receipt_handle,
                "MD5OfBody": m.md5_of_body,
                "Body": m.body,
            });

            // Only include system attributes if requested
            if let Some(names) = requested_sys_attrs {
                if !names.is_empty() {
                    let want_all = names.iter().any(|n| n == "All");
                    let mut sys_attrs = serde_json::Map::new();

                    if want_all || names.iter().any(|n| n == "ApproximateReceiveCount") {
                        sys_attrs.insert(
                            "ApproximateReceiveCount".to_string(),
                            json!(m.receive_count.to_string()),
                        );
                    }
                    if want_all || names.iter().any(|n| n == "SentTimestamp") {
                        sys_attrs.insert(
                            "SentTimestamp".to_string(),
                            json!(m.sent_timestamp.to_string()),
                        );
                    }
                    if want_all
                        || names
                            .iter()
                            .any(|n| n == "ApproximateFirstReceiveTimestamp")
                    {
                        sys_attrs.insert(
                            "ApproximateFirstReceiveTimestamp".to_string(),
                            json!(now_millis.to_string()),
                        );
                    }
                    if want_all || names.iter().any(|n| n == "SenderId") {
                        sys_attrs.insert("SenderId".to_string(), json!("AIDAIT2UOQQY3AUEKVGXU"));
                    }
                    if want_all || names.iter().any(|n| n == "MessageGroupId") {
                        if let Some(ref group_id) = m.message_group_id {
                            sys_attrs.insert("MessageGroupId".to_string(), json!(group_id));
                        }
                    }
                    if want_all || names.iter().any(|n| n == "MessageDeduplicationId") {
                        if let Some(ref dedup_id) = m.message_dedup_id {
                            sys_attrs.insert("MessageDeduplicationId".to_string(), json!(dedup_id));
                        }
                    }
                    if want_all || names.iter().any(|n| n == "SequenceNumber") {
                        if let Some(ref seq) = m.sequence_number {
                            sys_attrs.insert("SequenceNumber".to_string(), json!(seq));
                        }
                    }
                    if want_all || names.iter().any(|n| n == "AWSTraceHeader") {
                        // Include AWSTraceHeader if message has it in system attributes
                        if let Some(trace) = m.attributes.get("AWSTraceHeader") {
                            sys_attrs.insert("AWSTraceHeader".to_string(), json!(trace));
                        }
                    }

                    if !sys_attrs.is_empty() {
                        msg_json["Attributes"] = Value::Object(sys_attrs);
                    }
                }
            }

            // Filter message attributes
            let filtered_attrs: BTreeMap<String, &MessageAttribute> =
                if let Some(names) = msg_attr_names {
                    if names.is_empty() {
                        BTreeMap::new()
                    } else if names.iter().any(|n| n == "All" || n == ".*") {
                        m.message_attributes
                            .iter()
                            .map(|(k, v)| (k.clone(), v))
                            .collect()
                    } else {
                        m.message_attributes
                            .iter()
                            .filter(|(k, _)| {
                                names.iter().any(|n| {
                                    if n.ends_with(".*") {
                                        k.starts_with(n.trim_end_matches(".*"))
                                    } else {
                                        k.as_str() == n.as_str()
                                    }
                                })
                            })
                            .map(|(k, v)| (k.clone(), v))
                            .collect()
                    }
                } else {
                    BTreeMap::new()
                };

            if !filtered_attrs.is_empty() {
                let attrs: serde_json::Map<String, Value> = filtered_attrs
                    .iter()
                    .map(|(k, v)| {
                        let mut attr = json!({ "DataType": v.data_type });
                        if let Some(ref sv) = v.string_value {
                            attr["StringValue"] = json!(sv);
                        }
                        if let Some(ref bv) = v.binary_value {
                            use base64::Engine;
                            attr["BinaryValue"] =
                                json!(base64::engine::general_purpose::STANDARD.encode(bv));
                        }
                        (k.clone(), attr)
                    })
                    .collect();
                msg_json["MessageAttributes"] = Value::Object(attrs);
                msg_json["MD5OfMessageAttributes"] =
                    json!(md5_of_message_attributes_from_refs(&filtered_attrs));
            }

            msg_json
        })
        .collect();

    let body = if messages.is_empty() && !is_query {
        // For JSON protocol, omit Messages key when empty
        json!({})
    } else {
        json!({ "Messages": messages })
    };

    sqs_response("ReceiveMessage", body, request_id, is_query)
}

pub(crate) fn parse_message_attributes(body: &Value) -> BTreeMap<String, MessageAttribute> {
    let mut result = BTreeMap::new();
    if let Some(attrs) = body["MessageAttributes"].as_object() {
        for (name, val) in attrs {
            let data_type = val["DataType"].as_str().unwrap_or("String").to_string();
            let string_value = val["StringValue"].as_str().map(|s| s.to_string());
            let binary_value = val["BinaryValue"].as_str().and_then(|s| {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.decode(s).ok()
            });
            result.insert(
                name.clone(),
                MessageAttribute {
                    data_type,
                    string_value,
                    binary_value,
                },
            );
        }
    }

    // Handle Query protocol MessageAttribute.N.Name/Value patterns
    if let Some(body_obj) = body.as_object() {
        for i in 1..=20 {
            let name_key = format!("MessageAttribute.{i}.Name");
            let type_key = format!("MessageAttribute.{i}.Value.DataType");
            let str_key = format!("MessageAttribute.{i}.Value.StringValue");
            let bin_key = format!("MessageAttribute.{i}.Value.BinaryValue");

            if let Some(name) = body_obj.get(&name_key).and_then(|v| v.as_str()) {
                let data_type = body_obj
                    .get(&type_key)
                    .and_then(|v| v.as_str())
                    .unwrap_or("String")
                    .to_string();
                let string_value = body_obj
                    .get(&str_key)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let binary_value = body_obj
                    .get(&bin_key)
                    .and_then(|v| v.as_str())
                    .and_then(|s| {
                        use base64::Engine;
                        base64::engine::general_purpose::STANDARD.decode(s).ok()
                    });
                result.insert(
                    name.to_string(),
                    MessageAttribute {
                        data_type,
                        string_value,
                        binary_value,
                    },
                );
            }
        }
    }

    result
}

/// Validate message attribute names and data types
pub(crate) fn validate_message_attributes(
    attrs: &BTreeMap<String, MessageAttribute>,
) -> Result<(), AwsServiceError> {
    for (name, attr) in attrs {
        // Validate attribute name
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "The message attribute name '{}' is invalid. Attribute name can contain A-Z, a-z, 0-9, underscore (_), hyphen (-), and period (.) characters.",
                    name
                ),
            ));
        }

        // Validate data type
        let dt = &attr.data_type;
        let base_type = dt.split('.').next().unwrap_or(dt);
        let valid_prefixes = ["String", "Number", "Binary"];
        if !valid_prefixes.contains(&base_type) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "The message attribute '{name}' has an invalid message attribute type, the set of supported type prefixes is Binary, Number, and String."
                ),
            ));
        }
    }
    Ok(())
}

pub(crate) fn is_valid_queue_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 80 {
        return false;
    }
    name.chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
}

/// Compute MD5 of message attributes per AWS specification.
/// AWS sorts attributes by name, then for each: encode name length (4 bytes BE),
/// name bytes, data type length (4 bytes BE), data type bytes,
/// then transport type (1=String, 2=Binary) and value length + value bytes.
pub(crate) fn md5_of_message_attributes(attrs: &BTreeMap<String, MessageAttribute>) -> String {
    use md5::Digest;
    let mut sorted: Vec<(&String, &MessageAttribute)> = attrs.iter().collect();
    sorted.sort_by_key(|(k, _)| k.as_str());

    let mut hasher = Md5::new();
    for (name, attr) in sorted {
        // Name
        hasher.update((name.len() as u32).to_be_bytes());
        hasher.update(name.as_bytes());
        // Data type
        hasher.update((attr.data_type.len() as u32).to_be_bytes());
        hasher.update(attr.data_type.as_bytes());

        // Transport type and value
        if attr.data_type.starts_with("String") || attr.data_type.starts_with("Number") {
            hasher.update([1u8]); // STRING transport type
            if let Some(ref sv) = attr.string_value {
                hasher.update((sv.len() as u32).to_be_bytes());
                hasher.update(sv.as_bytes());
            } else {
                hasher.update(0u32.to_be_bytes());
            }
        } else if attr.data_type.starts_with("Binary") {
            hasher.update([2u8]); // BINARY transport type
            if let Some(ref bv) = attr.binary_value {
                hasher.update((bv.len() as u32).to_be_bytes());
                hasher.update(bv);
            } else {
                hasher.update(0u32.to_be_bytes());
            }
        }
    }
    format!("{:032x}", hasher.finalize())
}

/// Total wire size, in bytes, of a set of message attributes as AWS
/// accounts it against `MaximumMessageSize` and the batch ceiling. AWS
/// counts, per attribute: the UTF-8 byte length of the attribute name,
/// the UTF-8 byte length of the data type, and the byte length of the
/// value (UTF-8 for String/Number, raw bytes for Binary). Used so the
/// size check matches AWS, which measures body + attributes together
/// rather than the body alone (bug-audit 2026-06-13, 1.13).
pub(crate) fn message_attributes_size(attrs: &BTreeMap<String, MessageAttribute>) -> usize {
    attrs
        .iter()
        .map(|(name, attr)| {
            let value_len = if let Some(ref sv) = attr.string_value {
                sv.len()
            } else if let Some(ref bv) = attr.binary_value {
                bv.len()
            } else {
                0
            };
            name.len() + attr.data_type.len() + value_len
        })
        .sum()
}

/// Same as md5_of_message_attributes but works with borrowed references
pub(crate) fn md5_of_message_attributes_from_refs(
    attrs: &BTreeMap<String, &MessageAttribute>,
) -> String {
    use md5::Digest;
    let mut sorted: Vec<(&String, &&MessageAttribute)> = attrs.iter().collect();
    sorted.sort_by_key(|(k, _)| k.as_str());

    let mut hasher = Md5::new();
    for (name, attr) in sorted {
        hasher.update((name.len() as u32).to_be_bytes());
        hasher.update(name.as_bytes());
        hasher.update((attr.data_type.len() as u32).to_be_bytes());
        hasher.update(attr.data_type.as_bytes());

        if attr.data_type.starts_with("String") || attr.data_type.starts_with("Number") {
            hasher.update([1u8]);
            if let Some(ref sv) = attr.string_value {
                hasher.update((sv.len() as u32).to_be_bytes());
                hasher.update(sv.as_bytes());
            } else {
                hasher.update(0u32.to_be_bytes());
            }
        } else if attr.data_type.starts_with("Binary") {
            hasher.update([2u8]);
            if let Some(ref bv) = attr.binary_value {
                hasher.update((bv.len() as u32).to_be_bytes());
                hasher.update(bv);
            } else {
                hasher.update(0u32.to_be_bytes());
            }
        }
    }
    format!("{:032x}", hasher.finalize())
}

/// Resolve a QueueUrl that might be a queue name, a path, a full URL, or
/// an SQS ARN (`arn:aws:sqs:REGION:ACCOUNT:queue-name`). Real SQS accepts
/// all four interchangeably; SDKs in cross-account or IaC configs often
/// hand us the ARN.
pub(crate) fn resolve_queue_url(input: &str, state: &crate::state::SqsState) -> Option<String> {
    // Direct match
    if state.queues.contains_key(input) {
        return Some(input.to_string());
    }
    // ARN form: extract queue name (last segment after the colon-prefix)
    if let Some(rest) = input.strip_prefix("arn:aws:sqs:") {
        // rest = "REGION:ACCOUNT:queue-name"
        if let Some(name) = rest.rsplit(':').next() {
            if !name.is_empty() {
                if let Some(url) = state.name_to_url.get(name) {
                    return Some(url.clone());
                }
            }
        }
    }
    // Try as queue name
    if let Some(url) = state.name_to_url.get(input) {
        return Some(url.clone());
    }
    // Try extracting queue name from URL path (e.g., /123456789012/my-queue)
    let name = input.rsplit('/').next().unwrap_or("");
    if let Some(url) = state.name_to_url.get(name) {
        return Some(url.clone());
    }
    None
}

pub(crate) fn missing_param(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MissingParameter",
        format!("The request must contain the parameter {name}"),
    )
}

pub(crate) fn queue_not_found() -> AwsServiceError {
    // AWS SQS uses the awsQueryError-renamed wire code
    // `AWS.SimpleQueueService.NonExistentQueue` for the `QueueDoesNotExist`
    // shape (see `aws.protocols#awsQueryError.code` on the shape in the
    // upstream Smithy model). Real AWS puts this string in `<Code>`, and
    // it's also what's listed in each op's declared `errors`. Returning the
    // short shape name `QueueDoesNotExist` instead would be undeclared by
    // every operation per the model.
    AwsServiceError::aws_error_with_headers(
        StatusCode::BAD_REQUEST,
        "AWS.SimpleQueueService.NonExistentQueue",
        "The specified queue does not exist.",
        vec![(
            "x-amzn-query-error".to_string(),
            "AWS.SimpleQueueService.NonExistentQueue;Sender".to_string(),
        )],
    )
}

pub(crate) fn md5_hex(input: &str) -> String {
    use md5::Digest;
    let mut hasher = Md5::new();
    hasher.update(input.as_bytes());
    format!("{:032x}", hasher.finalize())
}

pub(crate) fn sha256_hex(input: &str) -> String {
    use sha2::Digest;
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:064x}", hasher.finalize())
}

/// Find the message_id associated with a receipt handle by checking both the
/// receipt_handle_map (historical handles) and current message receipt handles.
pub(crate) fn find_message_id_for_receipt(
    queue: &crate::state::SqsQueue,
    receipt_handle: &str,
) -> Option<String> {
    // Check the receipt handle map for any historical handle
    for (msg_id, handles) in &queue.receipt_handle_map {
        if handles.iter().any(|h| h == receipt_handle) {
            return Some(msg_id.clone());
        }
    }
    // Also check current messages/inflight directly
    for msg in &queue.inflight {
        if msg.receipt_handle.as_deref() == Some(receipt_handle) {
            return Some(msg.message_id.clone());
        }
    }
    for msg in &queue.messages {
        if msg.receipt_handle.as_deref() == Some(receipt_handle) {
            return Some(msg.message_id.clone());
        }
    }
    None
}

/// MD5 of message system attributes per AWS specification. System
/// attributes (e.g. `AWSTraceHeader`) are always typed as `String`, so
/// the wire format is the same shape as `md5_of_message_attributes`
/// with transport type 1 and `String` data type for each entry.
pub(crate) fn md5_of_message_system_attributes(attrs: &BTreeMap<String, String>) -> String {
    use md5::Digest;
    let mut sorted: Vec<(&String, &String)> = attrs.iter().collect();
    sorted.sort_by_key(|(k, _)| k.as_str());

    let mut hasher = Md5::new();
    for (name, value) in sorted {
        hasher.update((name.len() as u32).to_be_bytes());
        hasher.update(name.as_bytes());
        let data_type = "String";
        hasher.update((data_type.len() as u32).to_be_bytes());
        hasher.update(data_type.as_bytes());
        hasher.update([1u8]); // STRING transport type
        hasher.update((value.len() as u32).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    format!("{:032x}", hasher.finalize())
}

/// Parse MessageSystemAttributes (e.g., AWSTraceHeader) from the request body.
pub(crate) fn parse_message_system_attributes(body: &Value) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();

    // JSON protocol
    if let Some(attrs) = body["MessageSystemAttributes"].as_object() {
        for (name, val) in attrs {
            if let Some(sv) = val["StringValue"].as_str() {
                result.insert(name.clone(), sv.to_string());
            }
        }
    }

    // Query protocol: MessageSystemAttribute.N.Name/Value
    if let Some(body_obj) = body.as_object() {
        for i in 1..=20 {
            let name_key = format!("MessageSystemAttribute.{i}.Name");
            let str_key = format!("MessageSystemAttribute.{i}.Value.StringValue");

            if let Some(name) = body_obj.get(&name_key).and_then(|v| v.as_str()) {
                if let Some(value) = body_obj.get(&str_key).and_then(|v| v.as_str()) {
                    result.insert(name.to_string(), value.to_string());
                }
            }
        }
    }

    result
}

pub(crate) fn is_valid_batch_id(id: &str) -> bool {
    if id.is_empty() || id.len() > 80 {
        return false;
    }
    id.chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
}

pub(crate) fn parse_numbered_params(body: &Value, prefix: &str) -> Vec<String> {
    let mut result = Vec::new();
    if let Some(obj) = body.as_object() {
        for i in 1..=20 {
            let key = format!("{prefix}.{i}");
            if let Some(v) = obj.get(&key).and_then(|v| v.as_str()) {
                result.push(v.to_string());
            }
        }
    }
    result
}

#[cfg(test)]
mod envelope_tests {
    use super::looks_like_fakecloud_envelope;
    use base64::Engine;

    #[test]
    fn plaintext_body_is_not_an_envelope() {
        assert!(!looks_like_fakecloud_envelope("hello world"));
        assert!(!looks_like_fakecloud_envelope(
            r#"{"Message":"msg-0","Type":"Notification"}"#
        ));
        assert!(!looks_like_fakecloud_envelope(""));
    }

    #[test]
    fn legacy_textual_envelope_is_detected() {
        assert!(looks_like_fakecloud_envelope("fakecloud-kms:abc:dGVzdA=="));
    }

    #[test]
    fn aws_shaped_binary_envelope_is_detected() {
        let mut blob = vec![0x01, 0x02, 0x02, 0x00];
        blob.extend_from_slice(&[0u8; 32]);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&blob);
        assert!(looks_like_fakecloud_envelope(&b64));
    }

    #[test]
    fn random_base64_with_wrong_header_is_not_an_envelope() {
        let blob = vec![0xff, 0xff, 0xff, 0xff, 0u8, 0u8, 0u8, 0u8];
        let b64 = base64::engine::general_purpose::STANDARD.encode(&blob);
        assert!(!looks_like_fakecloud_envelope(&b64));
    }
}
