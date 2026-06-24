//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `sqs`.

use super::*;

impl ResourceProvisioner {
    pub(super) fn get_att_sqs_queue(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.sqs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let queue = state.queues.get(physical_id)?;
        match attribute {
            "Arn" => Some(queue.arn.clone()),
            "QueueName" => Some(queue.queue_name.clone()),
            "QueueUrl" => Some(queue.queue_url.clone()),
            _ => None,
        }
    }

    // --- SQS ---

    pub(super) fn create_sqs_queue(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let queue_name = props
            .get("QueueName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut __sqs_mas = self.sqs_state.write();
        let state = __sqs_mas.get_or_create(&self.account_id);
        let queue_url = format!("{}/{}/{}", state.endpoint, state.account_id, queue_name);
        let arn = format!(
            "arn:aws:sqs:{}:{}:{}",
            state.region, state.account_id, queue_name
        );

        let is_fifo = queue_name.ends_with(".fifo");
        let mut attributes = std::collections::BTreeMap::new();
        // Seed the real AWS SQS defaults so a CFN-created queue matches one
        // created via the API (Terraform refreshes these on every plan and
        // reports drift if they are absent).
        attributes.insert("VisibilityTimeout".to_string(), "30".to_string());
        attributes.insert("DelaySeconds".to_string(), "0".to_string());
        attributes.insert("MaximumMessageSize".to_string(), "262144".to_string());
        attributes.insert("MessageRetentionPeriod".to_string(), "345600".to_string());
        attributes.insert("ReceiveMessageWaitTimeSeconds".to_string(), "0".to_string());
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
        // Override with provided properties. Object-valued attributes
        // (RedrivePolicy, RedriveAllowPolicy, Policy) are serialized to compact
        // JSON and boolean attributes (ContentBasedDeduplication) to their
        // string form — the previous copy kept only string/integer values, so
        // these were silently dropped.
        if let Some(obj) = props.as_object() {
            for (k, v) in obj {
                if k == "QueueName" || k == "Tags" {
                    continue;
                }
                let value = match v {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        serde_json::to_string(v).unwrap_or_default()
                    }
                    serde_json::Value::Null => continue,
                };
                attributes.insert(k.clone(), value);
            }
        }
        // A KMS key implies SSE-KMS, so managed SSE is off (mirrors the native
        // create_queue mutual-exclusion).
        if attributes
            .get("KmsMasterKeyId")
            .is_some_and(|k| !k.is_empty())
        {
            attributes.insert("SqsManagedSseEnabled".to_string(), "false".to_string());
        }

        // Typed RedrivePolicy used for runtime DLQ routing — without it, CFN
        // queues never route to their dead-letter queue.
        let redrive_policy = attributes
            .get("RedrivePolicy")
            .and_then(|s| fakecloud_sqs::parse_redrive_policy(s));

        let queue = SqsQueue {
            queue_name: queue_name.to_string(),
            queue_url: queue_url.clone(),
            arn: arn.clone(),
            created_at: Utc::now(),
            messages: std::collections::VecDeque::new(),
            inflight: Vec::new(),
            attributes,
            is_fifo,
            dedup_cache: std::collections::BTreeMap::new(),
            redrive_policy,
            tags: std::collections::BTreeMap::new(),
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: std::collections::BTreeMap::new(),
            receive_attempt_cache: std::collections::BTreeMap::new(),
        };

        state
            .name_to_url
            .insert(queue_name.to_string(), queue_url.clone());
        state.queues.insert(queue_url.clone(), queue);

        Ok(ProvisionResult::new(queue_url.clone())
            .with("Arn", arn)
            .with("QueueName", queue_name)
            .with("QueueUrl", queue_url))
    }

    pub(super) fn delete_sqs_queue(&self, physical_id: &str) -> Result<(), String> {
        let mut __sqs_mas = self.sqs_state.write();
        let state = __sqs_mas.get_or_create(&self.account_id);
        if let Some(queue) = state.queues.remove(physical_id) {
            state.name_to_url.remove(&queue.queue_name);
        }
        Ok(())
    }

    // --- SQS QueuePolicy ---
    //
    // AWS::SQS::QueuePolicy doesn't create a standalone object; it stores the
    // PolicyDocument as the `Policy` attribute on each referenced queue, so a
    // subsequent GetQueueAttributes round-trips it. The `Queues` property is a
    // list of queue URLs (Refs are already resolved to physical ids by the
    // time we run). The physical id encodes those URLs (newline-joined, a char
    // that can't appear in a URL) so delete can locate and clear each queue.

    pub(super) fn create_sqs_queue_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let queue_urls = sqs_policy_queue_urls(&resource.properties)?;
        let policy = policy_document_string(&resource.properties)?;

        let mut __sqs_mas = self.sqs_state.write();
        let state = __sqs_mas.get_or_create(&self.account_id);
        for url in &queue_urls {
            let queue = state
                .queues
                .get_mut(url)
                .ok_or_else(|| format!("Queue {url} not yet provisioned"))?;
            queue
                .attributes
                .insert("Policy".to_string(), policy.clone());
        }
        Ok(ProvisionResult::new(queue_urls.join("\n")))
    }

    pub(super) fn update_sqs_queue_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Clear the policy from queues that are no longer referenced, then
        // (re)apply to the current set.
        let old_urls: Vec<String> = existing
            .physical_id
            .split('\n')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        let new_urls = sqs_policy_queue_urls(&resource.properties)?;
        let policy = policy_document_string(&resource.properties)?;

        let mut __sqs_mas = self.sqs_state.write();
        let state = __sqs_mas.get_or_create(&self.account_id);
        for url in &old_urls {
            if !new_urls.contains(url) {
                if let Some(queue) = state.queues.get_mut(url) {
                    queue.attributes.remove("Policy");
                }
            }
        }
        for url in &new_urls {
            let queue = state
                .queues
                .get_mut(url)
                .ok_or_else(|| format!("Queue {url} not yet provisioned"))?;
            queue
                .attributes
                .insert("Policy".to_string(), policy.clone());
        }
        Ok(ProvisionResult::new(new_urls.join("\n")))
    }

    pub(super) fn delete_sqs_queue_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut __sqs_mas = self.sqs_state.write();
        let state = __sqs_mas.get_or_create(&self.account_id);
        for url in physical_id.split('\n').filter(|s| !s.is_empty()) {
            if let Some(queue) = state.queues.get_mut(url) {
                queue.attributes.remove("Policy");
            }
        }
        Ok(())
    }
}

/// Resolve the `Queues` property (a list of Refs already resolved to queue
/// URLs) into a list of queue URLs.
fn sqs_policy_queue_urls(props: &serde_json::Value) -> Result<Vec<String>, String> {
    let queues = props
        .get("Queues")
        .and_then(|v| v.as_array())
        .ok_or("Queues is required")?;
    let urls: Vec<String> = queues
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    if urls.is_empty() {
        return Err("Queues must contain at least one queue".to_string());
    }
    Ok(urls)
}
