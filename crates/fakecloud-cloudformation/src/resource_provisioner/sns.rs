//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `sns`.

use super::*;

impl ResourceProvisioner {
    pub(super) fn get_att_sns_topic(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.sns_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let topic = state.topics.get(physical_id)?;
        match attribute {
            "TopicArn" => Some(topic.topic_arn.clone()),
            "TopicName" => Some(topic.name.clone()),
            _ => None,
        }
    }

    // --- SNS ---

    pub(super) fn create_sns_topic(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let topic_name = props
            .get("TopicName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        let topic_arn = format!(
            "arn:aws:sns:{}:{}:{}",
            state.region, state.account_id, topic_name
        );

        let topic = SnsTopic {
            topic_arn: topic_arn.clone(),
            name: topic_name.to_string(),
            attributes: BTreeMap::new(),
            tags: Vec::new(),
            is_fifo: topic_name.ends_with(".fifo"),
            created_at: Utc::now(),
            subscriptions_deleted: 0,
        };

        state.topics.insert(topic_arn.clone(), topic);
        Ok(ProvisionResult::new(topic_arn.clone())
            .with("TopicArn", topic_arn)
            .with("TopicName", topic_name))
    }

    pub(super) fn delete_sns_topic(&self, physical_id: &str) -> Result<(), String> {
        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        state.topics.remove(physical_id);
        // Also remove subscriptions for this topic
        state
            .subscriptions
            .retain(|_, sub| sub.topic_arn != physical_id);
        Ok(())
    }

    // --- SNS Subscription ---

    pub(super) fn create_sns_subscription(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let topic_arn = props
            .get("TopicArn")
            .and_then(|v| v.as_str())
            .ok_or("SNS Subscription requires TopicArn")?;
        let protocol = props
            .get("Protocol")
            .and_then(|v| v.as_str())
            .ok_or("SNS Subscription requires Protocol")?;
        let endpoint = props
            .get("Endpoint")
            .and_then(|v| v.as_str())
            .ok_or("SNS Subscription requires Endpoint")?;

        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);

        // Validate that the topic exists
        if !state.topics.contains_key(topic_arn) {
            return Err(format!("Topic ARN does not exist: {topic_arn}"));
        }

        let sub_arn = format!("{}:{}", topic_arn, Uuid::new_v4());

        let subscription = SnsSubscription {
            subscription_arn: sub_arn.clone(),
            topic_arn: topic_arn.to_string(),
            protocol: protocol.to_string(),
            endpoint: endpoint.to_string(),
            owner: state.account_id.clone(),
            attributes: BTreeMap::new(),
            confirmed: true,
            confirmation_token: None,
        };

        state.subscriptions.insert(sub_arn.clone(), subscription);
        Ok(ProvisionResult::new(sub_arn.clone()).with("Arn", sub_arn))
    }

    pub(super) fn delete_sns_subscription(&self, physical_id: &str) -> Result<(), String> {
        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        state.subscriptions.remove(physical_id);
        Ok(())
    }
}
