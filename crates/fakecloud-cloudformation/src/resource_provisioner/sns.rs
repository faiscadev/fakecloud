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
            fifo_sequence: 0,
            dedup_cache: BTreeMap::new(),
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

    // --- SNS TopicPolicy ---
    //
    // AWS::SNS::TopicPolicy stores the PolicyDocument as the `Policy` attribute
    // on each referenced topic, so a subsequent GetTopicAttributes round-trips
    // it. The `Topics` property is a list of topic ARNs (Refs are resolved to
    // physical ids before we run). The physical id encodes those ARNs
    // (newline-joined) so delete can locate and clear each topic.

    pub(super) fn create_sns_topic_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let topic_arns = sns_policy_topic_arns(&resource.properties)?;
        let policy = policy_document_string(&resource.properties)?;

        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        for arn in &topic_arns {
            let topic = state
                .topics
                .get_mut(arn)
                .ok_or_else(|| format!("Topic {arn} not yet provisioned"))?;
            topic
                .attributes
                .insert("Policy".to_string(), policy.clone());
        }
        Ok(ProvisionResult::new(topic_arns.join("\n")))
    }

    pub(super) fn update_sns_topic_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let old_arns: Vec<String> = existing
            .physical_id
            .split('\n')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        let new_arns = sns_policy_topic_arns(&resource.properties)?;
        let policy = policy_document_string(&resource.properties)?;

        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        for arn in &old_arns {
            if !new_arns.contains(arn) {
                if let Some(topic) = state.topics.get_mut(arn) {
                    topic.attributes.remove("Policy");
                }
            }
        }
        for arn in &new_arns {
            let topic = state
                .topics
                .get_mut(arn)
                .ok_or_else(|| format!("Topic {arn} not yet provisioned"))?;
            topic
                .attributes
                .insert("Policy".to_string(), policy.clone());
        }
        Ok(ProvisionResult::new(new_arns.join("\n")))
    }

    pub(super) fn delete_sns_topic_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        for arn in physical_id.split('\n').filter(|s| !s.is_empty()) {
            if let Some(topic) = state.topics.get_mut(arn) {
                topic.attributes.remove("Policy");
            }
        }
        Ok(())
    }
}

/// Resolve the `Topics` property (a list of Refs already resolved to topic
/// ARNs) into a list of topic ARNs.
fn sns_policy_topic_arns(props: &serde_json::Value) -> Result<Vec<String>, String> {
    let topics = props
        .get("Topics")
        .and_then(|v| v.as_array())
        .ok_or("Topics is required")?;
    let arns: Vec<String> = topics
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    if arns.is_empty() {
        return Err("Topics must contain at least one topic".to_string());
    }
    Ok(arns)
}
