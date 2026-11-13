//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `sns`.

use super::*;

/// Parse the mutable `AWS::SNS::Subscription` delivery/filter properties into
/// the stored attribute map. Shared by create and update so a CFN-created
/// subscription honors its FilterPolicy/RawMessageDelivery/RedrivePolicy/etc.
/// immediately (create previously stored an empty map). JSON-document members
/// accept an inline object or a string; RawMessageDelivery accepts bool or
/// "true"/"false".
fn sns_subscription_attributes(props: &serde_json::Value) -> BTreeMap<String, String> {
    let mut attributes = BTreeMap::new();
    for key in ["FilterPolicy", "RedrivePolicy", "DeliveryPolicy"] {
        if let Some(v) = props.get(key) {
            if !v.is_null() {
                let doc = if let Some(s) = v.as_str() {
                    s.to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                };
                attributes.insert(key.to_string(), doc);
            }
        }
    }
    for key in ["FilterPolicyScope", "SubscriptionRoleArn"] {
        if let Some(s) = props.get(key).and_then(|v| v.as_str()) {
            attributes.insert(key.to_string(), s.to_string());
        }
    }
    if let Some(b) = props
        .get("RawMessageDelivery")
        .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
    {
        attributes.insert("RawMessageDelivery".to_string(), b.to_string());
    }
    attributes
}

/// Where a topic's stack record keeps the ARNs of the subscriptions its
/// inline `Subscription` list created, so a later update can tell them apart
/// from subscriptions made outside the template. Not a real `GetAtt` name.
const SNS_INLINE_SUBSCRIPTIONS_ATTR: &str = "__fakecloud_inline_subscriptions__";

/// The topic attributes a CFN `AWS::SNS::Topic` manages (FifoTopic is
/// immutable and left out).
const SNS_TOPIC_MUTABLE_ATTRIBUTES: &[&str] = &[
    "DisplayName",
    "KmsMasterKeyId",
    "SignatureVersion",
    "TracingConfig",
    "ArchivePolicy",
    "FifoThroughputScope",
    "ContentBasedDeduplication",
];

/// The topic configuration attributes a CFN topic's properties set.
fn sns_topic_attributes(props: &serde_json::Value) -> BTreeMap<String, String> {
    let mut attributes = BTreeMap::new();
    for key in [
        "DisplayName",
        "KmsMasterKeyId",
        "SignatureVersion",
        "TracingConfig",
        "FifoThroughputScope",
    ] {
        if let Some(s) = props.get(key).and_then(|v| v.as_str()) {
            attributes.insert(key.to_string(), s.to_string());
        }
    }
    // ArchivePolicy is a JSON document: an inline object or a string.
    if let Some(v) = props.get("ArchivePolicy").filter(|v| !v.is_null()) {
        let doc = v
            .as_str()
            .map(String::from)
            .unwrap_or_else(|| serde_json::to_string(v).unwrap_or_default());
        attributes.insert("ArchivePolicy".to_string(), doc);
    }
    for key in ["FifoTopic", "ContentBasedDeduplication"] {
        if let Some(b) = props
            .get(key)
            .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
        {
            attributes.insert(key.to_string(), b.to_string());
        }
    }
    attributes
}

/// CFN `Tags` (`[{Key, Value}]`) as the topic's tag list.
fn sns_topic_tags(props: &serde_json::Value) -> Vec<(String, String)> {
    props
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
        .unwrap_or_default()
}

/// The inline `Subscription` list as (protocol, endpoint) pairs, duplicates
/// dropped (SNS has one subscription per topic/protocol/endpoint).
fn sns_inline_subscriptions(props: &serde_json::Value) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for sub in props
        .get("Subscription")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
    {
        let (Some(protocol), Some(endpoint)) = (
            sub.get("Protocol").and_then(|v| v.as_str()),
            sub.get("Endpoint").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let pair = (protocol.to_string(), endpoint.to_string());
        if !out.contains(&pair) {
            out.push(pair);
        }
    }
    out
}

/// The account a topic or subscription ARN belongs to (the topic owner's),
/// or `caller` when the ARN carries none.
fn sns_owning_account<'a>(arn: &'a str, caller: &'a str) -> &'a str {
    fakecloud_aws::arn::account_of(arn).unwrap_or(caller)
}

/// Whether a topic policy lets `account_id` (its root, which a stack runs
/// as) call `sns:Subscribe` on `topic_arn`.
fn sns_policy_allows_subscribe(
    policy: &str,
    topic_arn: &str,
    account_id: &str,
    region: &str,
) -> bool {
    let doc = fakecloud_iam::evaluator::PolicyDocument::parse(policy);
    let root = fakecloud_aws::arn::Arn::global_in(region, "iam", account_id, "root").to_string();
    let principal = fakecloud_core::auth::Principal {
        arn: root.clone(),
        user_id: account_id.to_string(),
        account_id: account_id.to_string(),
        principal_type: fakecloud_core::auth::PrincipalType::Root,
        source_identity: None,
        tags: None,
    };
    let request = fakecloud_iam::evaluator::EvalRequest {
        principal: &principal,
        action: "sns:Subscribe".to_string(),
        resource: topic_arn.to_string(),
        context: fakecloud_iam::evaluator::RequestContext {
            aws_principal_arn: Some(root),
            aws_principal_account: Some(account_id.to_string()),
            ..Default::default()
        },
    };
    matches!(
        fakecloud_iam::evaluator::evaluate_resource_policy_only(&doc, &request),
        fakecloud_iam::evaluator::Decision::Allow
    )
}

impl ResourceProvisioner {
    pub(super) fn get_att_sns_topic(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.sns_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let topic = state.topics.get(physical_id)?;
        match attribute {
            "TopicArn" => Some(topic.topic_arn.clone()),
            "TopicName" => Some(topic.name.clone()),
            _ => None,
        }
    }

    // --- SNS ---

    /// Apply a CFN property update to an existing SNS topic in place,
    /// preserving its subscriptions. The template is the desired state: the
    /// configuration attributes and Tags are re-applied (a dropped property
    /// is cleared), and the inline `Subscription` list is diffed against the
    /// subscriptions the previous template created inline -- removed entries
    /// are unsubscribed and new ones subscribed, as CloudFormation does.
    /// Subscriptions made outside the inline list are left alone.
    pub(super) fn update_sns_topic(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = &existing.physical_id;
        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.regional_mut(&self.account_id, &self.region);
        let topic = state
            .topics
            .get_mut(arn)
            .ok_or_else(|| format!("SNS topic {arn} not yet provisioned"))?;
        for key in SNS_TOPIC_MUTABLE_ATTRIBUTES {
            topic.attributes.remove(*key);
        }
        topic.attributes.extend(sns_topic_attributes(props));
        topic.tags = sns_topic_tags(props);
        let topic_name = topic.name.clone();

        // Inline subscriptions: keep the ones still listed, unsubscribe the
        // rest of what the previous template created inline, subscribe the
        // new entries.
        let desired = sns_inline_subscriptions(props);
        let previous: Vec<String> = existing
            .attributes
            .get(SNS_INLINE_SUBSCRIPTIONS_ATTR)
            .map(|s| s.lines().map(String::from).collect())
            .unwrap_or_default();
        let mut kept: Vec<String> = Vec::new();
        let mut covered: Vec<(String, String)> = Vec::new();
        for sub_arn in previous {
            let Some(sub) = state.subscriptions.get(&sub_arn) else {
                continue;
            };
            let key = (sub.protocol.clone(), sub.endpoint.clone());
            if desired.contains(&key) && !covered.contains(&key) {
                covered.push(key);
                kept.push(sub_arn);
            } else {
                state.subscriptions.remove(&sub_arn);
            }
        }
        for (protocol, endpoint) in desired {
            if covered.contains(&(protocol.clone(), endpoint.clone())) {
                continue;
            }
            // Subscribe is idempotent on (topic, protocol, endpoint): an
            // existing identical subscription is reused, not duplicated.
            let existing_sub = state
                .subscriptions
                .values()
                .find(|s| s.topic_arn == *arn && s.protocol == protocol && s.endpoint == endpoint)
                .map(|s| s.subscription_arn.clone());
            let sub_arn = match existing_sub {
                Some(sub_arn) => sub_arn,
                None => {
                    let sub_arn = format!("{}:{}", arn, Uuid::new_v4());
                    let owner = state.account_id.clone();
                    state.subscriptions.insert(
                        sub_arn.clone(),
                        SnsSubscription {
                            subscription_arn: sub_arn.clone(),
                            topic_arn: arn.clone(),
                            protocol: protocol.clone(),
                            endpoint: endpoint.clone(),
                            owner,
                            attributes: BTreeMap::new(),
                            confirmed: true,
                            confirmation_token: None,
                            confirmation_auth: Default::default(),
                        },
                    );
                    sub_arn
                }
            };
            covered.push((protocol, endpoint));
            kept.push(sub_arn);
        }
        Ok(ProvisionResult::new(arn.clone())
            .with("TopicArn", arn.clone())
            .with("TopicName", topic_name)
            .with(SNS_INLINE_SUBSCRIPTIONS_ATTR, kept.join("\n")))
    }

    pub(super) fn create_sns_topic(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        // An unnamed FIFO topic still needs the `.fifo` ending SNS requires.
        let fifo_requested = props
            .get("FifoTopic")
            .is_some_and(|v| v.as_bool() == Some(true) || v.as_str() == Some("true"));
        let generated_name = if fifo_requested {
            self.physical_name_ending(resource, ".fifo")
        } else {
            self.physical_name(resource)
        };
        let topic_name = props
            .get("TopicName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name);

        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.regional_mut(&self.account_id, &self.region);
        let topic_arn = fakecloud_sns::topic_arn(&self.region, &self.account_id, topic_name);
        if state.topics.contains_key(&topic_arn) {
            return Err(resource_already_exists("AWS::SNS::Topic", &topic_arn));
        }

        // Carry the topic configuration attributes a CFN topic can set, so
        // GetTopicAttributes round-trips them instead of returning defaults.
        let attributes = sns_topic_attributes(props);
        let tags = sns_topic_tags(props);

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
        let mut inline_arns: Vec<String> = Vec::new();
        for (protocol, endpoint) in sns_inline_subscriptions(props) {
            let sub_arn = format!("{}:{}", topic_arn, Uuid::new_v4());
            state.subscriptions.insert(
                sub_arn.clone(),
                SnsSubscription {
                    subscription_arn: sub_arn.clone(),
                    topic_arn: topic_arn.clone(),
                    protocol,
                    endpoint,
                    owner: state.account_id.clone(),
                    attributes: BTreeMap::new(),
                    confirmed: true,
                    confirmation_token: None,
                    confirmation_auth: Default::default(),
                },
            );
            inline_arns.push(sub_arn);
        }

        Ok(ProvisionResult::new(topic_arn.clone())
            .with("TopicArn", topic_arn)
            .with("TopicName", topic_name)
            .with(SNS_INLINE_SUBSCRIPTIONS_ATTR, inline_arns.join("\n")))
    }

    pub(super) fn delete_sns_topic(&self, physical_id: &str) -> Result<(), String> {
        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.regional_mut(&self.account_id, &self.region);
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

        // The subscription lives with its topic, in the topic owner's account
        // (where Publish fans out from), owned by the stack's account. A
        // topic in another account is subscribable when its policy allows
        // it, as Subscribe is.
        let topic_account = sns_owning_account(topic_arn, &self.account_id).to_string();
        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.regional_mut(
            &topic_account,
            fakecloud_aws::arn::region_of(topic_arn).unwrap_or(&self.region),
        );

        let topic = state
            .topics
            .get(topic_arn)
            .ok_or_else(|| format!("Topic ARN does not exist: {topic_arn}"))?;
        if topic_account != self.account_id && self.iam_mode.is_strict() {
            let allowed = topic.attributes.get("Policy").is_some_and(|policy| {
                sns_policy_allows_subscribe(policy, topic_arn, &self.account_id, &self.region)
            });
            if !allowed {
                return Err(format!(
                    "AuthorizationError: User: arn:{}:iam::{}:root is not authorized to perform: SNS:Subscribe on resource: {topic_arn}",
                    fakecloud_aws::arn::partition_for(&self.region),
                    self.account_id
                ));
            }
        }
        // Subscribe is idempotent on (topic, protocol, endpoint).
        if let Some(existing) = state
            .subscriptions
            .values()
            .find(|s| s.topic_arn == topic_arn && s.protocol == protocol && s.endpoint == endpoint)
        {
            let sub_arn = existing.subscription_arn.clone();
            return Ok(ProvisionResult::new(sub_arn.clone()).with("Arn", sub_arn));
        }

        let sub_arn = format!("{}:{}", topic_arn, Uuid::new_v4());

        let subscription = SnsSubscription {
            subscription_arn: sub_arn.clone(),
            topic_arn: topic_arn.to_string(),
            protocol: protocol.to_string(),
            endpoint: endpoint.to_string(),
            owner: self.account_id.clone(),
            // Parse the delivery/filter attributes at create time (previously
            // hardcoded empty, so a CFN-created subscription silently ignored
            // its FilterPolicy/RawMessageDelivery until an unrelated stack
            // update happened to run the update path). Same set update applies.
            attributes: sns_subscription_attributes(props),
            confirmed: true,
            confirmation_token: None,
            confirmation_auth: Default::default(),
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
        let state = __sns_mas.regional_mut(
            sns_owning_account(sub_arn, &self.account_id),
            fakecloud_aws::arn::region_of(sub_arn).unwrap_or(&self.region),
        );
        let subscription = state
            .subscriptions
            .get_mut(sub_arn)
            .ok_or_else(|| format!("SNS subscription {sub_arn} not yet provisioned"))?;

        // The template is the desired state: re-apply the delivery/filter
        // attributes create parses, clearing any the template dropped.
        subscription.attributes = sns_subscription_attributes(props);

        Ok(ProvisionResult::new(sub_arn.clone()).with("Arn", sub_arn.clone()))
    }

    pub(super) fn delete_sns_subscription(&self, physical_id: &str) -> Result<(), String> {
        let mut __sns_mas = self.sns_state.write();
        let state = __sns_mas.regional_mut(
            sns_owning_account(physical_id, &self.account_id),
            fakecloud_aws::arn::region_of(physical_id).unwrap_or(&self.region),
        );
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
        let state = __sns_mas.regional_mut(&self.account_id, &self.region);
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
        let state = __sns_mas.regional_mut(&self.account_id, &self.region);
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
        let state = __sns_mas.regional_mut(&self.account_id, &self.region);
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
