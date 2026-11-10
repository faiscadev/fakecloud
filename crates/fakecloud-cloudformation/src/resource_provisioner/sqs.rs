//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `sqs`.

use super::*;

impl ResourceProvisioner {
    pub(super) fn get_att_sqs_queue(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.sqs_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let queue = state.queues.get(physical_id)?;
        match attribute {
            "Arn" => Some(queue.arn.clone()),
            "QueueName" => Some(queue.queue_name.clone()),
            "QueueUrl" => Some(queue.queue_url.clone()),
            _ => None,
        }
    }

    // --- SQS ---

    /// Apply a CFN property update to an existing queue in place, preserving
    /// its enqueued messages. CloudFormation sends the full desired property
    /// set, so the attribute map is rebuilt from the SQS defaults plus the new
    /// properties: a property removed from the template goes back to its
    /// default (as AWS does on a stack update), the typed redrive policy used
    /// for DLQ routing is refreshed alongside the attribute string (the same
    /// pairing SetQueueAttributes maintains), and LastModifiedTimestamp moves.
    pub(super) fn update_sqs_queue(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let url = &existing.physical_id;
        let mut __sqs_mas = self.sqs_state.write();
        let state = __sqs_mas.regional_mut(&self.account_id, &self.region);
        let queue = state
            .queues
            .get_mut(url)
            .ok_or_else(|| format!("Queue {url} not yet provisioned"))?;
        let mut attributes = cfn_queue_attributes(props, queue.is_fifo);
        // Only the attributes the Queue resource owns follow its template.
        // Everything else on the queue -- the `Policy` an
        // AWS::SQS::QueuePolicy (or SetQueueAttributes) wrote, and any other
        // attribute set outside this resource -- is carried over unchanged.
        for (k, v) in &queue.attributes {
            if !SQS_QUEUE_OWNED_ATTRIBUTES.contains(&k.as_str()) {
                attributes.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        let created = queue
            .attributes
            .get("CreatedTimestamp")
            .cloned()
            .unwrap_or_else(|| queue.created_at.timestamp().to_string());
        attributes.insert("CreatedTimestamp".to_string(), created);
        attributes.insert(
            "LastModifiedTimestamp".to_string(),
            Utc::now().timestamp().to_string(),
        );
        // Validate the new set the way SetQueueAttributes does, so an
        // out-of-range attribute fails the update instead of being stored.
        fakecloud_sqs::validate_create_queue_attributes(&attributes).map_err(|e| e.to_string())?;
        queue.redrive_policy = attributes
            .get("RedrivePolicy")
            .and_then(|s| fakecloud_sqs::parse_redrive_policy(s));
        queue.attributes = attributes;
        // Tags are part of the desired state too: an update that drops the
        // Tags property clears them.
        queue.tags = parse_cfn_queue_tags(props.get("Tags"));
        Ok(ProvisionResult::new(url.clone()))
    }

    pub(super) fn create_sqs_queue(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        // An unnamed FIFO queue still needs the `.fifo` ending SQS requires.
        let fifo_requested = props
            .get("FifoQueue")
            .is_some_and(|v| v.as_bool() == Some(true) || v.as_str() == Some("true"));
        let generated_name = if fifo_requested {
            self.physical_name_ending(resource, ".fifo")
        } else {
            self.physical_name(resource)
        };
        let queue_name = props
            .get("QueueName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name);
        if !fakecloud_sqs::is_valid_queue_name(
            queue_name.strip_suffix(".fifo").unwrap_or(queue_name),
        ) {
            return Err(
                "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length"
                    .to_string(),
            );
        }

        let mut __sqs_mas = self.sqs_state.write();
        let state = __sqs_mas.regional_mut(&self.account_id, &self.region);
        if let Some(existing_url) = state.name_to_url.get(queue_name) {
            return Err(resource_already_exists("AWS::SQS::Queue", existing_url));
        }
        let queue_url = format!("{}/{}/{}", state.endpoint, state.account_id, queue_name);
        let arn = self.regional_arn("sqs", queue_name);

        let is_fifo = queue_name.ends_with(".fifo");
        let now = Utc::now();
        let mut attributes = cfn_queue_attributes(props, is_fifo);
        // Validated like CreateQueue, so an out-of-range attribute fails the
        // create instead of being stored.
        fakecloud_sqs::validate_create_queue_attributes(&attributes).map_err(|e| e.to_string())?;
        attributes.insert("CreatedTimestamp".to_string(), now.timestamp().to_string());
        attributes.insert(
            "LastModifiedTimestamp".to_string(),
            now.timestamp().to_string(),
        );

        // Typed RedrivePolicy used for runtime DLQ routing — without it, CFN
        // queues never route to their dead-letter queue.
        let redrive_policy = attributes
            .get("RedrivePolicy")
            .and_then(|s| fakecloud_sqs::parse_redrive_policy(s));

        // Translate the CFN `Tags` property into the queue's tag map. Every
        // other property is applied above; tags were skipped, so ListQueueTags
        // on a CFN-created queue came back empty (Terraform then reported tag
        // drift on every plan).
        let tags = parse_cfn_queue_tags(props.get("Tags"));

        let queue = SqsQueue {
            queue_name: queue_name.to_string(),
            queue_url: queue_url.clone(),
            arn: arn.clone(),
            created_at: now,
            messages: std::collections::VecDeque::new(),
            inflight: Vec::new(),
            attributes,
            is_fifo,
            dedup_cache: std::collections::BTreeMap::new(),
            redrive_policy,
            tags,
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
        let state = __sqs_mas.regional_mut(&self.account_id, &self.region);
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
        let state = __sqs_mas.regional_mut(&self.account_id, &self.region);
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
        let state = __sqs_mas.regional_mut(&self.account_id, &self.region);
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
        let state = __sqs_mas.regional_mut(&self.account_id, &self.region);
        for url in physical_id.split('\n').filter(|s| !s.is_empty()) {
            if let Some(queue) = state.queues.get_mut(url) {
                queue.attributes.remove("Policy");
            }
        }
        Ok(())
    }
}

/// Translate the CFN `AWS::SQS::Queue` `Tags` property into the queue's tag
/// map. CFN models tags as a list of `{ "Key": .., "Value": .. }` objects; a
/// resolved intrinsic can also produce a plain `{key: value}` map, so both
/// shapes are accepted.
/// The queue attribute map a CFN `AWS::SQS::Queue` with `props` describes:
/// the real SQS defaults overlaid with every provided property.
/// Object-valued attributes (RedrivePolicy, RedriveAllowPolicy, Policy) are
/// serialized to compact JSON and booleans to their string form. A KMS key
/// implies SSE-KMS, so managed SSE is off (the native create_queue
/// mutual-exclusion).
/// The queue attributes an `AWS::SQS::Queue` resource owns (its properties,
/// plus `SqsManagedSseEnabled`, which its KMS setting determines). An update
/// resets these to the SQS defaults when the template drops them.
const SQS_QUEUE_OWNED_ATTRIBUTES: &[&str] = &[
    "ContentBasedDeduplication",
    "DeduplicationScope",
    "DelaySeconds",
    "FifoQueue",
    "FifoThroughputLimit",
    "KmsDataKeyReusePeriodSeconds",
    "KmsMasterKeyId",
    "MaximumMessageSize",
    "MessageRetentionPeriod",
    "ReceiveMessageWaitTimeSeconds",
    "RedriveAllowPolicy",
    "RedrivePolicy",
    "SqsManagedSseEnabled",
    "VisibilityTimeout",
];

fn cfn_queue_attributes(
    props: &serde_json::Value,
    is_fifo: bool,
) -> std::collections::BTreeMap<String, String> {
    let mut attributes = std::collections::BTreeMap::new();
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
    if attributes
        .get("KmsMasterKeyId")
        .is_some_and(|k| !k.is_empty())
    {
        attributes.insert("SqsManagedSseEnabled".to_string(), "false".to_string());
    }
    attributes
}

fn parse_cfn_queue_tags(
    tags: Option<&serde_json::Value>,
) -> std::collections::BTreeMap<String, String> {
    let mut map = std::collections::BTreeMap::new();
    match tags {
        Some(serde_json::Value::Array(arr)) => {
            for t in arr {
                let key = t.get("Key").and_then(|v| v.as_str());
                let value = t.get("Value").and_then(|v| v.as_str());
                if let (Some(k), Some(v)) = (key, value) {
                    map.insert(k.to_string(), v.to_string());
                }
            }
        }
        Some(serde_json::Value::Object(obj)) => {
            for (k, v) in obj {
                if let Some(v) = v.as_str() {
                    map.insert(k.clone(), v.to_string());
                }
            }
        }
        _ => {}
    }
    map
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
