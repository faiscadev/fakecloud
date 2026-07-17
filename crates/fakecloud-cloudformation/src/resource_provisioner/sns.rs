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

    /// Apply a CFN property update to an existing SNS topic in place,
    /// preserving its subscriptions/tags. Mirrors the attribute extraction in
    /// `create_sns_topic` so a stack update re-applies DisplayName / KMS /
    /// FIFO config instead of being silently dropped.
    pub(super) fn update_sns_topic(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = &existing.physical_id;
        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        let topic = state
            .topics
            .get_mut(arn)
            .ok_or_else(|| format!("SNS topic {arn} not yet provisioned"))?;
        for key in [
            "DisplayName",
            "KmsMasterKeyId",
            "SignatureVersion",
            "TracingConfig",
            "ArchivePolicy",
            "FifoThroughputScope",
        ] {
            if let Some(s) = props.get(key).and_then(|v| v.as_str()) {
                topic.attributes.insert(key.to_string(), s.to_string());
            }
        }
        for key in ["FifoTopic", "ContentBasedDeduplication"] {
            if let Some(b) = props
                .get(key)
                .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
            {
                topic.attributes.insert(key.to_string(), b.to_string());
            }
        }
        Ok(ProvisionResult::new(arn.clone()))
    }

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

        // Carry the topic configuration attributes a CFN topic can set, so
        // GetTopicAttributes round-trips them instead of returning defaults.
        let mut attributes = BTreeMap::new();
        for key in [
            "DisplayName",
            "KmsMasterKeyId",
            "SignatureVersion",
            "TracingConfig",
            "ArchivePolicy",
            "FifoThroughputScope",
        ] {
            if let Some(s) = props.get(key).and_then(|v| v.as_str()) {
                attributes.insert(key.to_string(), s.to_string());
            }
        }
        for key in ["FifoTopic", "ContentBasedDeduplication"] {
            if let Some(b) = props
                .get(key)
                .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
            {
                attributes.insert(key.to_string(), b.to_string());
            }
        }
        let tags: Vec<(String, String)> = props
            .get("Tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        Some((
                            t.get("Key").and_then(|v| v.as_str())?.to_string(),
                            t.get("Value").and_then(|v| v.as_str())?.to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let topic = SnsTopic {
            topic_arn: topic_arn.clone(),
            name: topic_name.to_string(),
            attributes,
            tags,
            is_fifo: topic_name.ends_with(".fifo"),
            created_at: Utc::now(),
            subscriptions_deleted: 0,
            fifo_sequence: 0,
            dedup_cache: BTreeMap::new(),
        };

        state.topics.insert(topic_arn.clone(), topic);

        // Inline `Subscription` list — SAM/CFN's shorthand for attaching
        // subscriptions at topic-create time. Without this the topic is
        // created with no subscribers and fan-out is silently broken.
        if let Some(subs) = props.get("Subscription").and_then(|v| v.as_array()) {
            for sub in subs {
                let (Some(protocol), Some(endpoint)) = (
                    sub.get("Protocol").and_then(|v| v.as_str()),
                    sub.get("Endpoint").and_then(|v| v.as_str()),
                ) else {
                    continue;
                };
                let sub_arn = format!("{}:{}", topic_arn, Uuid::new_v4());
                state.subscriptions.insert(
                    sub_arn.clone(),
                    SnsSubscription {
                        subscription_arn: sub_arn,
                        topic_arn: topic_arn.clone(),
                        protocol: protocol.to_string(),
                        endpoint: endpoint.to_string(),
                        owner: state.account_id.clone(),
                        attributes: BTreeMap::new(),
                        confirmed: true,
                        confirmation_token: None,
                    },
                );
            }
        }

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

    /// Apply a CFN property update to an existing SNS subscription in place.
    /// `Protocol`, `Endpoint` and `TopicArn` require replacement in real
    /// CloudFormation and are left untouched; the mutable delivery/filter
    /// attributes (`FilterPolicy`, `FilterPolicyScope`, `RawMessageDelivery`,
    /// `RedrivePolicy`, `DeliveryPolicy`, `SubscriptionRoleArn`) are re-applied
    /// so a stack update reaches the subscription and `GetSubscriptionAttributes`
    /// reflects the new values instead of the stale ones.
    pub(super) fn update_sns_subscription(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let sub_arn = &existing.physical_id;

        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.get_or_create(&self.account_id);
        let subscription = state
            .subscriptions
            .get_mut(sub_arn)
            .ok_or_else(|| format!("SNS subscription {sub_arn} not yet provisioned"))?;

        // JSON-document attributes: accept either an inline object or a string.
        for key in ["FilterPolicy", "RedrivePolicy", "DeliveryPolicy"] {
            if let Some(v) = props.get(key) {
                if !v.is_null() {
                    let doc = if let Some(s) = v.as_str() {
                        s.to_string()
                    } else {
                        serde_json::to_string(v).unwrap_or_default()
                    };
                    subscription.attributes.insert(key.to_string(), doc);
                }
            }
        }
        for key in ["FilterPolicyScope", "SubscriptionRoleArn"] {
            if let Some(s) = props.get(key).and_then(|v| v.as_str()) {
                subscription
                    .attributes
                    .insert(key.to_string(), s.to_string());
            }
        }
        if let Some(b) = props
            .get("RawMessageDelivery")
            .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
        {
            subscription
                .attributes
                .insert("RawMessageDelivery".to_string(), b.to_string());
        }

        Ok(ProvisionResult::new(sub_arn.clone()).with("Arn", sub_arn.clone()))
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
