//! `SqsService` `queues` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl SqsService {
    pub(super) fn create_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_name = body["QueueName"]
            .as_str()
            .ok_or_else(|| missing_param("QueueName"))?
            .to_string();

        let is_fifo = queue_name.ends_with(".fifo");

        // Validate FIFO queue attributes
        if let Some(attrs) = body["Attributes"].as_object() {
            if let Some(fifo_val) = attrs.get("FifoQueue").and_then(|v| v.as_str()) {
                if fifo_val == "true" && !is_fifo {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        "The queue name must end with the .fifo suffix for a FIFO queue.",
                    ));
                }
            }
        }

        // Validate queue name
        let base_name = if is_fifo {
            queue_name.trim_end_matches(".fifo")
        } else {
            &queue_name
        };
        if !is_valid_queue_name(base_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length",
            ));
        }

        let mut new_attributes = BTreeMap::new();
        if let Some(attrs) = body["Attributes"].as_object() {
            for (k, v) in attrs {
                if let Some(s) = v.as_str() {
                    new_attributes.insert(k.trim().to_string(), s.to_string());
                }
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if let Some(url) = state.name_to_url.get(&queue_name) {
            // Queue exists - check if attributes match
            if let Some(existing) = state.queues.get(url) {
                // If caller passed attributes, check for conflicts
                if !new_attributes.is_empty() {
                    for (k, v) in &new_attributes {
                        if let Some(existing_val) = existing.attributes.get(k.trim()) {
                            // Normalize JSON values for comparison (e.g. RedrivePolicy)
                            let val_matches = if let (Ok(a), Ok(b)) = (
                                serde_json::from_str::<Value>(existing_val),
                                serde_json::from_str::<Value>(v),
                            ) {
                                a == b
                            } else {
                                existing_val == v
                            };
                            if !val_matches {
                                return Err(AwsServiceError::aws_error(
                                    StatusCode::BAD_REQUEST,
                                    "QueueAlreadyExists",
                                    "A queue already exists with the same name and a different value for attribute VisibilityTimeout.",
                                ));
                            }
                        }
                    }
                }
            }
            return Ok(sqs_response(
                "CreateQueue",
                json!({ "QueueUrl": url }),
                &req.request_id,
                req.is_query_protocol,
            ));
        }

        let queue_url = format!("{}/{}/{}", state.endpoint, state.account_id, queue_name);

        let mut attributes = BTreeMap::new();
        // Default attributes (real AWS SQS defaults).
        attributes.insert("VisibilityTimeout".to_string(), "30".to_string());
        attributes.insert("DelaySeconds".to_string(), "0".to_string());
        // AWS default MaximumMessageSize is 256 KiB (262144), not 1 MiB.
        attributes.insert("MaximumMessageSize".to_string(), "262144".to_string());
        attributes.insert("MessageRetentionPeriod".to_string(), "345600".to_string());
        attributes.insert("ReceiveMessageWaitTimeSeconds".to_string(), "0".to_string());
        // Encryption defaults. Since May 2023, real AWS SQS enables SSE-SQS
        // (managed server-side encryption) automatically on every new queue,
        // so `SqsManagedSseEnabled` defaults to "true" when no KMS key is
        // configured. `KmsDataKeyReusePeriodSeconds` always defaults to 300
        // (5 minutes) regardless of which encryption mode is active;
        // Terraform's provider refreshes both on every plan and enforces
        // the default when the resource config doesn't specify one.
        attributes.insert(
            "KmsDataKeyReusePeriodSeconds".to_string(),
            "300".to_string(),
        );
        attributes.insert("SqsManagedSseEnabled".to_string(), "true".to_string());
        if is_fifo {
            attributes.insert("FifoQueue".to_string(), "true".to_string());
            attributes.insert("ContentBasedDeduplication".to_string(), "false".to_string());
            attributes.insert("DeduplicationScope".to_string(), "queue".to_string());
            attributes.insert("FifoThroughputLimit".to_string(), "perQueue".to_string());
        }

        validate_create_queue_attributes(&new_attributes)?;

        // Override with provided attributes (trim keys to handle trailing whitespace).
        // For JSON-valued attributes, canonicalize to compact form so the
        // round-trip through GetQueueAttributes matches what Terraform's
        // `jsonencode` produces. Real AWS SQS does the same canonicalization
        // server-side, and skipping it surfaces as `whitespace changes` drift
        // in the upstream `aws_sqs_queue` acceptance suite.
        for (k, v) in new_attributes {
            let key = k.trim().to_string();
            let value = canonicalize_json_attr(&key, v);
            attributes.insert(key, value);
        }

        // Apply the same SSE-mode mutual-exclusion that SetQueueAttributes
        // does: a KMS key at create time implies SSE-KMS mode, so managed
        // SSE must be disabled. And an explicit `SqsManagedSseEnabled=true`
        // at create time clears any KMS key, matching real AWS behaviour.
        if attributes
            .get("KmsMasterKeyId")
            .is_some_and(|k| !k.is_empty())
        {
            attributes.insert("SqsManagedSseEnabled".to_string(), "false".to_string());
        }
        if attributes.get("SqsManagedSseEnabled").map(String::as_str) == Some("true") {
            attributes.remove("KmsMasterKeyId");
        }

        let redrive_policy = attributes
            .get("RedrivePolicy")
            .and_then(|s| parse_redrive_policy(s));

        if let Some(ref rp) = redrive_policy {
            validate_redrive_policy_target(state, rp, is_fifo)?;
        }

        // RedrivePolicy is already canonicalized to compact JSON above by
        // `canonicalize_json_attr`. We only need the typed `RedrivePolicy`
        // value for runtime DLQ routing decisions; the stored attribute
        // string is what GetQueueAttributes returns and round-trips clean.

        // Parse tags
        let mut tags = BTreeMap::new();
        if let Some(tags_obj) = body["tags"].as_object() {
            for (k, v) in tags_obj {
                if let Some(s) = v.as_str() {
                    tags.insert(k.clone(), s.to_string());
                }
            }
        }
        // Also check Tags (JSON protocol)
        if let Some(tags_obj) = body["Tags"].as_object() {
            for (k, v) in tags_obj {
                if let Some(s) = v.as_str() {
                    tags.insert(k.clone(), s.to_string());
                }
            }
        }

        let now = Utc::now();
        let created_ts = now.timestamp();

        attributes.insert("CreatedTimestamp".to_string(), created_ts.to_string());
        attributes.insert("LastModifiedTimestamp".to_string(), created_ts.to_string());

        let queue = SqsQueue {
            arn: format!(
                "arn:{}:sqs:{}:{}:{}",
                fakecloud_aws::arn::partition_for(&state.region),
                state.region,
                state.account_id,
                queue_name
            ),
            queue_name: queue_name.clone(),
            queue_url: queue_url.clone(),
            created_at: now,
            messages: VecDeque::new(),
            inflight: Vec::new(),
            attributes,
            is_fifo,
            dedup_cache: BTreeMap::new(),
            redrive_policy,
            tags,
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: BTreeMap::new(),
            receive_attempt_cache: BTreeMap::new(),
        };

        state.name_to_url.insert(queue_name, queue_url.clone());
        state.queues.insert(queue_url.clone(), queue);

        Ok(sqs_response(
            "CreateQueue",
            json!({ "QueueUrl": queue_url }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn delete_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(&queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .remove(&resolved_url)
            .ok_or_else(queue_not_found)?;
        state.name_to_url.remove(&queue.queue_name);

        Ok(sqs_response(
            "DeleteQueue",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn list_queues(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let prefix = body["QueueNamePrefix"].as_str();
        let _accts = self.state.read();
        let _empty = crate::state::SqsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);

        let max_results = body["MaxResults"]
            .as_u64()
            .or_else(|| body["MaxResults"].as_str().and_then(|s| s.parse().ok()))
            .map(|n| n.clamp(1, 1000) as usize)
            .unwrap_or(1000);

        // The pagination cursor carries the last queue URL emitted, not a
        // positional offset. A positional offset shifts when a queue is
        // created or deleted between page fetches, so the next page would
        // skip or duplicate queues; a URL marker is stable across those
        // mutations because the list is deterministically sorted.
        let after_marker = body["NextToken"].as_str().map(decode_list_queues_token);

        let mut all_urls: Vec<String> = state
            .queues
            .values()
            .filter(|q| prefix.map(|p| q.queue_name.starts_with(p)).unwrap_or(true))
            .map(|q| q.queue_url.clone())
            .collect();
        all_urls.sort();

        // Resume just past the marker. `partition_point` finds the first URL
        // strictly greater than the marker in O(log n).
        let start_idx = match &after_marker {
            Some(marker) => all_urls.partition_point(|u| u.as_str() <= marker.as_str()),
            None => 0,
        };

        let page: Vec<String> = all_urls
            .iter()
            .skip(start_idx)
            .take(max_results)
            .cloned()
            .collect();
        let has_more = start_idx + page.len() < all_urls.len();

        let mut result = json!({ "QueueUrls": page.clone() });
        if has_more {
            if let Some(last) = page.last() {
                result["NextToken"] = json!(encode_list_queues_token(last));
            }
        }

        Ok(sqs_response(
            "ListQueues",
            result,
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn get_queue_url(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_name = body["QueueName"]
            .as_str()
            .ok_or_else(|| missing_param("QueueName"))?;

        let _accts = self.state.read();
        let _empty = crate::state::SqsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let url = state
            .name_to_url
            .get(queue_name)
            .ok_or_else(queue_not_found)?;

        Ok(sqs_response(
            "GetQueueUrl",
            json!({ "QueueUrl": url }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn get_queue_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let _accts = self.state.read();
        let _empty = crate::state::SqsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get(&resolved_url)
            .ok_or_else(queue_not_found)?;

        // Check what attributes were requested
        let requested_names = body["AttributeNames"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>());

        // If no AttributeNames specified, return empty (per AWS behavior for JSON protocol)
        if requested_names.is_none() || requested_names.as_ref().map(|n| n.is_empty()) == Some(true)
        {
            // For query protocol, some clients don't pass AttributeNames and expect empty
            return Ok(sqs_response(
                "GetQueueAttributes",
                json!({}),
                &req.request_id,
                req.is_query_protocol,
            ));
        }

        let names = requested_names.unwrap();
        let want_all = names.contains(&"All");

        // Validate attribute names
        let valid_attrs = [
            "All",
            "Policy",
            "VisibilityTimeout",
            "MaximumMessageSize",
            "MessageRetentionPeriod",
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "CreatedTimestamp",
            "LastModifiedTimestamp",
            "QueueArn",
            "ApproximateNumberOfMessagesDelayed",
            "DelaySeconds",
            "ReceiveMessageWaitTimeSeconds",
            "RedrivePolicy",
            "FifoQueue",
            "ContentBasedDeduplication",
            "KmsMasterKeyId",
            "KmsDataKeyReusePeriodSeconds",
            "DeduplicationScope",
            "FifoThroughputLimit",
            "RedriveAllowPolicy",
            "SqsManagedSseEnabled",
        ];
        for name in &names {
            if !valid_attrs.contains(name) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidAttributeName",
                    format!("Unknown Attribute {name}."),
                ));
            }
        }

        let now = Utc::now();
        let mut attrs = queue.attributes.clone();
        attrs.insert("QueueArn".to_string(), queue.arn.clone());

        // Count visible messages (not delayed)
        let visible_count = queue
            .messages
            .iter()
            .filter(|m| m.visible_at.map(|v| v <= now).unwrap_or(true))
            .count();
        // Count expired inflight as visible
        let expired_inflight = queue
            .inflight
            .iter()
            .filter(|m| m.visible_at.map(|v| v <= now).unwrap_or(false))
            .count();
        let still_inflight = queue.inflight.len() - expired_inflight;
        attrs.insert(
            "ApproximateNumberOfMessages".to_string(),
            (visible_count + expired_inflight).to_string(),
        );
        attrs.insert(
            "ApproximateNumberOfMessagesNotVisible".to_string(),
            still_inflight.to_string(),
        );
        // Count delayed messages
        let delayed_count = queue
            .messages
            .iter()
            .filter(|m| m.visible_at.map(|v| v > now).unwrap_or(false))
            .count();
        attrs.insert(
            "ApproximateNumberOfMessagesDelayed".to_string(),
            delayed_count.to_string(),
        );

        if !want_all {
            attrs.retain(|k, _| names.contains(&k.as_str()));
        }

        if attrs.is_empty() {
            Ok(sqs_response(
                "GetQueueAttributes",
                json!({}),
                &req.request_id,
                req.is_query_protocol,
            ))
        } else {
            Ok(sqs_response(
                "GetQueueAttributes",
                json!({ "Attributes": attrs }),
                &req.request_id,
                req.is_query_protocol,
            ))
        }
    }

    pub(super) fn purge_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        queue.messages.clear();
        queue.inflight.clear();

        Ok(sqs_response(
            "PurgeQueue",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn set_queue_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;

        // Validate the redrive DLQ target the same way CreateQueue does,
        // before storing it. Terraform's `aws_sqs_redrive_policy` resource
        // configures the DLQ via SetQueueAttributes, and an unchecked
        // bad/nonexistent DLQ ARN makes failed messages churn on the source
        // forever instead of being dead-lettered. Done with immutable borrows
        // before the mutable `get_mut` below.
        if let Some(rp_str) = body["Attributes"]
            .as_object()
            .and_then(|a| a.get("RedrivePolicy"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            if let Some(rp) = parse_redrive_policy(rp_str) {
                let is_fifo = state
                    .queues
                    .get(&resolved_url)
                    .map(|q| q.is_fifo)
                    .unwrap_or(false);
                validate_redrive_policy_target(state, &rp, is_fifo)?;
            }
        }

        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        // Validate DelaySeconds (0–900 inclusive) before applying
        if let Some(attrs) = body["Attributes"].as_object() {
            if let Some(ds) = attrs.get("DelaySeconds").and_then(|v| v.as_str()) {
                if let Ok(d) = ds.parse::<i64>() {
                    if !(0..=900).contains(&d) {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidAttributeValue",
                            "Invalid value for the parameter DelaySeconds.".to_string(),
                        ));
                    }
                }
            }
        }

        // Validate VisibilityTimeout (0–43200 inclusive) before applying.
        // An unchecked value is stored and later overflows the receive-path
        // arithmetic (`now + Duration::seconds(v)`), panicking the worker
        // thread — a DoS. AWS returns InvalidAttributeValue for the queue
        // attribute (as opposed to InvalidParameterValue for the per-request
        // VisibilityTimeout on ReceiveMessage/ChangeMessageVisibility).
        if let Some(attrs) = body["Attributes"].as_object() {
            validate_visibility_timeout_attr(
                attrs.get("VisibilityTimeout").and_then(|v| v.as_str()),
            )?;
        }

        if let Some(attrs) = body["Attributes"].as_object() {
            for (k, v) in attrs {
                if let Some(s) = v.as_str() {
                    // Setting an empty value clears an existing value. For
                    // JSON-valued policies and for the KMS master key alias,
                    // "" means "remove" (Terraform's provider sends "" to
                    // switch encryption modes). Other attributes are stored
                    // with their empty string so the round-trip is faithful.
                    let is_clearable = matches!(
                        k.as_str(),
                        "Policy" | "RedrivePolicy" | "RedriveAllowPolicy" | "KmsMasterKeyId"
                    );
                    if s.is_empty() && is_clearable {
                        queue.attributes.remove(k);
                        if k == "RedrivePolicy" {
                            queue.redrive_policy = None;
                        }
                    } else {
                        queue
                            .attributes
                            .insert(k.clone(), canonicalize_json_attr(k, s.to_string()));
                    }
                }
            }

            // Encryption-mode mutual exclusion. Switching *to* SSE-SQS
            // (`SqsManagedSseEnabled=true`) clears the KMS key and resets
            // the reuse period to 300 — real AWS SQS uses a new data key
            // on the managed mode, and the upstream `aws_sqs_queue`
            // provider's refresh expects the default value back.
            // Switching to SSE-KMS (`KmsMasterKeyId` set to non-empty)
            // turns off the managed-SSE flag. A mode switch that doesn't
            // specify `KmsDataKeyReusePeriodSeconds` also resets it to
            // 300, matching real-AWS behaviour.
            let sse_mode_switch =
                attrs.get("SqsManagedSseEnabled").and_then(|v| v.as_str()) == Some("true");
            let kms_key_switch = attrs
                .get("KmsMasterKeyId")
                .and_then(|v| v.as_str())
                .is_some_and(|k| !k.is_empty());
            let reuse_period_explicit = attrs.contains_key("KmsDataKeyReusePeriodSeconds");

            if sse_mode_switch {
                queue.attributes.remove("KmsMasterKeyId");
                if !reuse_period_explicit {
                    queue.attributes.insert(
                        "KmsDataKeyReusePeriodSeconds".to_string(),
                        "300".to_string(),
                    );
                }
            }
            if kms_key_switch {
                queue
                    .attributes
                    .insert("SqsManagedSseEnabled".to_string(), "false".to_string());
            }

            // Update typed redrive_policy used for runtime DLQ routing.
            // The stored attribute string is already canonicalized above.
            if let Some(rp_str) = attrs.get("RedrivePolicy").and_then(|v| v.as_str()) {
                if !rp_str.is_empty() {
                    if let Some(rp) = parse_redrive_policy(rp_str) {
                        queue.redrive_policy = Some(rp);
                    }
                }
            }
        }

        Ok(sqs_response(
            "SetQueueAttributes",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }
}
