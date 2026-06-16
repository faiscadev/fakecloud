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
        if let Some(obj) = props.as_object() {
            for (k, v) in obj {
                if k != "QueueName" {
                    if let Some(s) = v.as_str() {
                        attributes.insert(k.clone(), s.to_string());
                    } else if let Some(n) = v.as_i64() {
                        attributes.insert(k.clone(), n.to_string());
                    }
                }
            }
        }

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
            redrive_policy: None,
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
