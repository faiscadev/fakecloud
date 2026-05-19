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
}
