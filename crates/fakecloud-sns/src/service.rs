use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_aws::arn::Arn;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    MessageAttribute, PlatformApplication, PlatformEndpoint, PublishedMessage, SharedSnsState,
    SnsSnapshot, SnsSubscription, SnsTopic, SNS_SNAPSHOT_SCHEMA_VERSION,
};

pub struct SnsService {
    state: SharedSnsState,
    delivery: Arc<DeliveryBus>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    pub(crate) kms_hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
    pub(crate) region: String,
}

impl SnsService {
    pub fn new(state: SharedSnsState, delivery: Arc<DeliveryBus>) -> Self {
        Self {
            state,
            delivery,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            kms_hook: None,
            region: "us-east-1".to_string(),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn fakecloud_core::delivery::KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    /// Record `GenerateDataKey` + `Decrypt` for an encrypted SNS topic.
    /// SNS encrypts each message at rest with the topic's KMS key, then
    /// decrypts to fan-out to subscribers. fakecloud's fan-out happens
    /// in-process so we don't actually round-trip ciphertext through the
    /// envelope; we just emit the audit-trail records the AWS API would
    /// produce, so callers can assert KMS usage via
    /// `/_fakecloud/kms/usage`.
    fn record_topic_kms_usage(
        &self,
        account_id: &str,
        topic_arn: &str,
        kms_key_id: Option<&str>,
        message: &str,
    ) -> Result<(), AwsServiceError> {
        let Some(hook) = &self.kms_hook else {
            return Ok(());
        };
        let Some(key) = kms_key_id.filter(|k| !k.is_empty()) else {
            return Ok(());
        };
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("aws:sns:arn".to_string(), topic_arn.to_string());
        let envelope = match hook.encrypt(
            account_id,
            &self.region,
            key,
            message.as_bytes(),
            "sns.amazonaws.com",
            ctx.clone(),
        ) {
            Ok(env) => env,
            Err(err) => {
                tracing::warn!(topic = %topic_arn, error = %err, "SNS SSE-KMS encrypt failed");
                return Err(AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMS.InternalFailureException",
                    format!("Failed to encrypt SNS message via KMS: {err}"),
                ));
            }
        };
        // Mirror the GenerateDataKey with the matching Decrypt the
        // service would emit at fan-out time. A decrypt failure here
        // means the topic's CMK is unusable for delivery — also
        // fail-closed so callers don't silently lose audit records.
        if let Err(err) = hook.decrypt(account_id, &envelope, "sns.amazonaws.com", ctx) {
            tracing::warn!(topic = %topic_arn, error = %err, "SNS SSE-KMS decrypt failed");
            return Err(AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMS.InternalFailureException",
                format!("Failed to decrypt SNS message via KMS: {err}"),
            ));
        }
        Ok(())
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes,
    /// with serde + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_sns_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current SNS state when invoked, or
    /// `None` in memory mode (no snapshot store). The CloudFormation
    /// provisioner mutates `state` directly and uses this to write a
    /// CFN-provisioned topic through to disk, the same way a direct mutating
    /// API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_sns_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current SNS state as a snapshot. Offloads the serde + blocking
/// file write to the Tokio blocking pool. Noop when `store` is `None` (memory
/// mode). Shared by `SnsService::save_snapshot` and the CloudFormation
/// provisioner's post-provision persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_sns_snapshot(
    state: &SharedSnsState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = SnsSnapshot {
        schema_version: SNS_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
        state: None,
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write sns snapshot"),
        Err(err) => tracing::error!(%err, "sns snapshot task panicked"),
    }
}

use fakecloud_aws::xml::xml_escape;

const DEFAULT_PAGE_SIZE: usize = 100;

/// Topic-level default DeliveryPolicy. Real SNS wraps the retry knobs
/// inside an `http` object because topic-level policies only apply to
/// HTTP/HTTPS subscribers (other protocols ignore them). Includes
/// `disableSubscriptionOverrides`, `defaultThrottlePolicy`, and
/// `defaultRequestPolicy` keys that real SNS surfaces.
const DEFAULT_TOPIC_DELIVERY_POLICY: &str = r#"{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false,"defaultThrottlePolicy":null,"defaultRequestPolicy":{"headerContentType":"text/plain; charset=UTF-8"}}}"#;

/// Subscription-level default DeliveryPolicy. Subscription policies
/// are flat (no `http` wrapper) and include `guaranteed` /
/// `sicklyRetryPolicy` / `throttlePolicy` keys.
const DEFAULT_SUBSCRIPTION_DELIVERY_POLICY: &str = r#"{"defaultHealthyRetryPolicy":{"numNoDelayRetries":0,"numMinDelayRetries":0,"minDelayTarget":20,"maxDelayTarget":20,"numMaxDelayRetries":0,"numRetries":3,"backoffFunction":"linear"},"sicklyRetryPolicy":null,"throttlePolicy":null,"guaranteed":false}"#;

/// Merge a user-supplied topic-level DeliveryPolicy over the SNS
/// default to produce the EffectiveDeliveryPolicy real SNS surfaces.
/// Falls back to the default when the user policy is missing, empty,
/// or unparseable (matching real SNS behaviour).
fn compute_effective_topic_delivery_policy(user: Option<&str>) -> String {
    compute_effective_policy(user, DEFAULT_TOPIC_DELIVERY_POLICY)
}

/// Merge a user-supplied subscription-level DeliveryPolicy over the
/// SNS default to produce the EffectiveDeliveryPolicy real SNS
/// surfaces. Subscription policies use a flat shape (no `http`
/// wrapper); HTTP/S subscriptions inherit from the topic policy when
/// no override is set.
fn compute_effective_subscription_delivery_policy(user: Option<&str>) -> String {
    compute_effective_policy(user, DEFAULT_SUBSCRIPTION_DELIVERY_POLICY)
}

fn compute_effective_policy(user: Option<&str>, default_json: &str) -> String {
    let default: serde_json::Value = serde_json::from_str(default_json).unwrap();
    let Some(user) = user else {
        return default_json.to_string();
    };
    let user_trim = user.trim();
    if user_trim.is_empty() {
        return default_json.to_string();
    }
    let Ok(user_val) = serde_json::from_str::<serde_json::Value>(user_trim) else {
        return default_json.to_string();
    };
    let merged = merge_json(default, user_val);
    serde_json::to_string(&merged).unwrap_or_else(|_| default_json.to_string())
}

fn merge_json(mut base: serde_json::Value, overlay: serde_json::Value) -> serde_json::Value {
    if let (Some(b), Some(o)) = (base.as_object_mut(), overlay.as_object()) {
        for (k, v) in o {
            match b.get_mut(k) {
                Some(existing @ serde_json::Value::Object(_)) if v.is_object() => {
                    let merged = merge_json(existing.take(), v.clone());
                    *existing = merged;
                }
                _ => {
                    b.insert(k.clone(), v.clone());
                }
            }
        }
        base
    } else {
        overlay
    }
}

/// Apply a dotted-path override (AWS-style) onto a JSON document.
/// Real SNS accepts `SetTopicAttributes` calls with attribute names
/// like `DeliveryPolicy.http.defaultHealthyRetryPolicy.numRetries`
/// (after the leading `DeliveryPolicy.` prefix this is the suffix
/// passed to this function) and patches the corresponding nested key.
/// Numeric-looking values become JSON numbers; `true`/`false`/`null`
/// become JSON literals; everything else stays a string.
fn apply_dotted_override(doc: &mut serde_json::Value, path: &str, raw_value: &str) {
    let segments: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return;
    }
    let parsed = parse_dotted_value(raw_value);
    let mut cursor = doc;
    for (i, seg) in segments.iter().enumerate() {
        if i == segments.len() - 1 {
            if let Some(obj) = cursor.as_object_mut() {
                obj.insert((*seg).to_string(), parsed);
            }
            return;
        }
        // Replace non-objects with an empty object so we can drill in.
        if !cursor.get(*seg).map(|v| v.is_object()).unwrap_or(false) {
            if let Some(obj) = cursor.as_object_mut() {
                obj.insert((*seg).to_string(), serde_json::json!({}));
            } else {
                return;
            }
        }
        cursor = cursor.get_mut(*seg).expect("just inserted");
    }
}

fn parse_dotted_value(raw: &str) -> serde_json::Value {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        return serde_json::Value::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return serde_json::Value::Bool(false);
    }
    if trimmed.eq_ignore_ascii_case("null") {
        return serde_json::Value::Null;
    }
    if let Ok(n) = trimmed.parse::<i64>() {
        return serde_json::Value::from(n);
    }
    if let Ok(n) = trimmed.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(n) {
            return serde_json::Value::Number(num);
        }
    }
    serde_json::Value::String(raw.to_string())
}

const VALID_SNS_ACTIONS: &[&str] = &[
    "GetTopicAttributes",
    "SetTopicAttributes",
    "AddPermission",
    "RemovePermission",
    "DeleteTopic",
    "Subscribe",
    "ListSubscriptionsByTopic",
    "Publish",
    "Receive",
];

const VALID_SUBSCRIPTION_ATTRS: &[&str] = &[
    "RawMessageDelivery",
    "DeliveryPolicy",
    "FilterPolicy",
    "FilterPolicyScope",
    "RedrivePolicy",
    "SubscriptionRoleArn",
];

#[async_trait]
impl AwsService for SnsService {
    fn service_name(&self) -> &str {
        "sns"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(req.action.as_str());
        let result = match req.action.as_str() {
            "CreateTopic" => self.create_topic(&req),
            "DeleteTopic" => self.delete_topic(&req),
            "ListTopics" => self.list_topics(&req),
            "GetTopicAttributes" => self.get_topic_attributes(&req),
            "SetTopicAttributes" => self.set_topic_attributes(&req),
            "Subscribe" => self.subscribe(&req),
            "ConfirmSubscription" => self.confirm_subscription(&req),
            "Unsubscribe" => self.unsubscribe(&req),
            "Publish" => self.publish(&req),
            "PublishBatch" => self.publish_batch(&req),
            "ListSubscriptions" => self.list_subscriptions(&req),
            "ListSubscriptionsByTopic" => self.list_subscriptions_by_topic(&req),
            "GetSubscriptionAttributes" => self.get_subscription_attributes(&req),
            "SetSubscriptionAttributes" => self.set_subscription_attributes(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "AddPermission" => self.add_permission(&req),
            "RemovePermission" => self.remove_permission(&req),
            // Platform application actions
            "CreatePlatformApplication" => self.create_platform_application(&req),
            "DeletePlatformApplication" => self.delete_platform_application(&req),
            "GetPlatformApplicationAttributes" => self.get_platform_application_attributes(&req),
            "SetPlatformApplicationAttributes" => self.set_platform_application_attributes(&req),
            "ListPlatformApplications" => self.list_platform_applications(&req),
            "CreatePlatformEndpoint" => self.create_platform_endpoint(&req),
            "DeleteEndpoint" => self.delete_endpoint(&req),
            "GetEndpointAttributes" => self.get_endpoint_attributes(&req),
            "SetEndpointAttributes" => self.set_endpoint_attributes(&req),
            "ListEndpointsByPlatformApplication" => {
                self.list_endpoints_by_platform_application(&req)
            }
            // SMS actions
            "SetSMSAttributes" => self.set_sms_attributes(&req),
            "GetSMSAttributes" => self.get_sms_attributes(&req),
            "CheckIfPhoneNumberIsOptedOut" => self.check_if_phone_number_is_opted_out(&req),
            "ListPhoneNumbersOptedOut" => self.list_phone_numbers_opted_out(&req),
            "OptInPhoneNumber" => self.opt_in_phone_number(&req),
            "CreateSMSSandboxPhoneNumber" => self.create_sms_sandbox_phone_number(&req),
            "DeleteSMSSandboxPhoneNumber" => self.delete_sms_sandbox_phone_number(&req),
            "VerifySMSSandboxPhoneNumber" => self.verify_sms_sandbox_phone_number(&req),
            "ListSMSSandboxPhoneNumbers" => self.list_sms_sandbox_phone_numbers(&req),
            "GetSMSSandboxAccountStatus" => self.get_sms_sandbox_account_status(&req),
            "ListOriginationNumbers" => self.list_origination_numbers(&req),
            "GetDataProtectionPolicy" => self.get_data_protection_policy(&req),
            "PutDataProtectionPolicy" => self.put_data_protection_policy(&req),
            _ => Err(AwsServiceError::action_not_implemented("sns", &req.action)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SNS_SUPPORTED_ACTIONS
    }

    fn iam_enforceable(&self) -> bool {
        true
    }

    /// SNS resources are topic / subscription / platform-app / endpoint
    /// ARNs. Topic-targeted actions carry a `TopicArn`; subscription
    /// actions carry a `SubscriptionArn`; platform-app actions carry
    /// `PlatformApplicationArn`; endpoint actions carry `EndpointArn`.
    /// Listing and account-scoped actions target `*`.
    fn iam_action_for(&self, request: &AwsRequest) -> Option<fakecloud_core::auth::IamAction> {
        let action = SNS_SUPPORTED_ACTIONS
            .iter()
            .copied()
            .find(|a| *a == request.action)?;
        let resource = match action {
            "CreateTopic" => {
                // The to-be-created topic ARN is built from the same
                // account id the handler uses (state.account_id via
                // `Arn::new`), not `principal.account_id`, so policy
                // evaluation and the actual ARN can't diverge even if
                // the two sources ever drift (identified by cubic on
                // PR #399).
                let _accts = self.state.read(); let _empty = crate::state::SnsState::new(&request.account_id, &request.region, ""); let state = _accts.get(&request.account_id).unwrap_or(&_empty);
                let partition = if request.region.starts_with("cn-") {
                    "aws-cn"
                } else if request.region.starts_with("us-gov-") {
                    "aws-us-gov"
                } else {
                    "aws"
                };
                param(request, "Name")
                    .map(|n| {
                        format!(
                            "arn:{}:sns:{}:{}:{}",
                            partition, request.region, state.account_id, n
                        )
                    })
                    .unwrap_or_else(|| "*".to_string())
            }
            "DeleteTopic"
            | "GetTopicAttributes"
            | "SetTopicAttributes"
            | "Subscribe"
            | "Publish"
            | "PublishBatch"
            | "ListSubscriptionsByTopic"
            | "AddPermission"
            | "RemovePermission"
            // ConfirmSubscription is keyed by TopicArn (identified by
            // cubic on PR #399) — the Token in the request confirms a
            // subscription to that topic; SubscriptionArn only exists
            // after confirmation.
            | "ConfirmSubscription" => {
                param(request, "TopicArn").unwrap_or_else(|| "*".to_string())
            }
            "Unsubscribe" | "GetSubscriptionAttributes" | "SetSubscriptionAttributes" => {
                param(request, "SubscriptionArn").unwrap_or_else(|| "*".to_string())
            }
            "TagResource" | "UntagResource" | "ListTagsForResource" => {
                param(request, "ResourceArn").unwrap_or_else(|| "*".to_string())
            }
            "DeletePlatformApplication"
            | "GetPlatformApplicationAttributes"
            | "SetPlatformApplicationAttributes"
            | "CreatePlatformEndpoint"
            | "ListEndpointsByPlatformApplication" => {
                param(request, "PlatformApplicationArn").unwrap_or_else(|| "*".to_string())
            }
            "DeleteEndpoint" | "GetEndpointAttributes" | "SetEndpointAttributes" => {
                param(request, "EndpointArn").unwrap_or_else(|| "*".to_string())
            }
            "GetDataProtectionPolicy" | "PutDataProtectionPolicy" => {
                param(request, "ResourceArn").unwrap_or_else(|| "*".to_string())
            }
            _ => "*".to_string(),
        };
        Some(fakecloud_core::auth::IamAction {
            service: "sns",
            action,
            resource,
        })
    }

    fn iam_condition_keys_for(
        &self,
        request: &AwsRequest,
        action: &fakecloud_core::auth::IamAction,
    ) -> std::collections::BTreeMap<String, Vec<String>> {
        let mut out = std::collections::BTreeMap::new();
        if action.action == "Subscribe" {
            if let Some(protocol) = param(request, "Protocol") {
                out.insert("sns:protocol".to_string(), vec![protocol]);
            }
            if let Some(endpoint) = param(request, "Endpoint") {
                out.insert("sns:endpoint".to_string(), vec![endpoint]);
            }
        }
        out
    }

    fn resource_tags_for(
        &self,
        resource_arn: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        if resource_arn == "*" {
            return Some(std::collections::HashMap::new());
        }
        let account_id = resource_arn.split(':').nth(4).unwrap_or("");
        let _accts = self.state.read();
        let state = _accts.get(account_id)?;
        let topic = state.topics.get(resource_arn)?;
        Some(topic.tags.iter().cloned().collect())
    }

    fn request_tags_from(
        &self,
        request: &AwsRequest,
        action: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        match action {
            "CreateTopic" | "TagResource" => {
                let tags = parse_tags(request);
                Some(tags.into_iter().collect())
            }
            _ => Some(std::collections::HashMap::new()),
        }
    }
}

const SNS_SUPPORTED_ACTIONS: &[&str] = &[
    "CreateTopic",
    "DeleteTopic",
    "ListTopics",
    "GetTopicAttributes",
    "SetTopicAttributes",
    "Subscribe",
    "ConfirmSubscription",
    "Unsubscribe",
    "Publish",
    "PublishBatch",
    "ListSubscriptions",
    "ListSubscriptionsByTopic",
    "GetSubscriptionAttributes",
    "SetSubscriptionAttributes",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
    "AddPermission",
    "RemovePermission",
    "CreatePlatformApplication",
    "DeletePlatformApplication",
    "GetPlatformApplicationAttributes",
    "SetPlatformApplicationAttributes",
    "ListPlatformApplications",
    "CreatePlatformEndpoint",
    "DeleteEndpoint",
    "GetEndpointAttributes",
    "SetEndpointAttributes",
    "ListEndpointsByPlatformApplication",
    "SetSMSAttributes",
    "GetSMSAttributes",
    "CheckIfPhoneNumberIsOptedOut",
    "ListPhoneNumbersOptedOut",
    "OptInPhoneNumber",
    "CreateSMSSandboxPhoneNumber",
    "DeleteSMSSandboxPhoneNumber",
    "VerifySMSSandboxPhoneNumber",
    "ListSMSSandboxPhoneNumbers",
    "GetSMSSandboxAccountStatus",
    "ListOriginationNumbers",
    "GetDataProtectionPolicy",
    "PutDataProtectionPolicy",
];

const FIFO_NAME_ERROR: &str = "Fifo Topic names must end with .fifo and must be made up of only uppercase and lowercase ASCII letters, numbers, underscores, and hyphens, and must be between 1 and 256 characters long.";
const STANDARD_NAME_ERROR: &str = "Topic names must be made up of only uppercase and lowercase ASCII letters, numbers, underscores, and hyphens, and must be between 1 and 256 characters long.";

impl SnsService {
    fn create_topic(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required(req, "Name")?;

        // Parse attributes from Attributes.entry.N.key / Attributes.entry.N.value
        let topic_attrs = parse_entries(req, "Attributes");
        let is_fifo_attr = topic_attrs
            .get("FifoTopic")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let is_fifo = name.ends_with(".fifo");

        validate_topic_name(&name, is_fifo_attr)?;

        // Parse tags from request
        let tags = parse_tags(req);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let topic_arn = Arn::new("sns", &req.region, &state.account_id, &name).to_string();

        if !state.topics.contains_key(&topic_arn) {
            let mut attributes = BTreeMap::new();
            // Set default policy
            attributes.insert(
                "Policy".to_string(),
                default_policy(&topic_arn, &state.account_id),
            );
            attributes.insert("DisplayName".to_string(), String::new());
            attributes.insert("DeliveryPolicy".to_string(), String::new());

            if is_fifo {
                attributes.insert("FifoTopic".to_string(), "true".to_string());
                attributes.insert("ContentBasedDeduplication".to_string(), "false".to_string());
            }

            // Apply topic attributes from the request
            for (k, v) in &topic_attrs {
                // Normalize boolean-like values for FifoTopic and ContentBasedDeduplication
                if k == "FifoTopic" || k == "ContentBasedDeduplication" {
                    let normalized = if v.eq_ignore_ascii_case("true") {
                        "true"
                    } else {
                        "false"
                    };
                    if k == "FifoTopic" && normalized == "false" {
                        attributes.remove("FifoTopic");
                        attributes.remove("ContentBasedDeduplication");
                        continue;
                    }
                    attributes.insert(k.clone(), normalized.to_string());
                    continue;
                }
                attributes.insert(k.clone(), v.clone());
            }

            let topic = SnsTopic {
                topic_arn: topic_arn.clone(),
                name,
                attributes,
                tags,
                is_fifo,
                created_at: Utc::now(),
                subscriptions_deleted: 0,
                fifo_sequence: 0,
                dedup_cache: std::collections::BTreeMap::new(),
            };
            state.topics.insert(topic_arn.clone(), topic);
        }

        Ok(xml_resp(
            &format!(
                r#"<CreateTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CreateTopicResult>
    <TopicArn>{topic_arn}</TopicArn>
  </CreateTopicResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateTopicResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn delete_topic(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.topics.remove(&topic_arn);
        state
            .subscriptions
            .retain(|_, sub| sub.topic_arn != topic_arn);

        Ok(xml_resp(
            &format!(
                r#"<DeleteTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</DeleteTopicResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn list_topics(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);

        // Filter topics by region
        let all_topics: Vec<&SnsTopic> = state
            .topics
            .values()
            .filter(|t| {
                // Extract region from ARN
                let parts: Vec<&str> = t.topic_arn.split(':').collect();
                parts.len() >= 4 && parts[3] == req.region
            })
            .collect();

        let next_token = param(req, "NextToken")
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let next_token = next_token.min(all_topics.len());

        let page = &all_topics[next_token..];
        let has_more = page.len() > DEFAULT_PAGE_SIZE;
        let page = if has_more {
            &page[..DEFAULT_PAGE_SIZE]
        } else {
            page
        };

        let members: String = page
            .iter()
            .map(|t| {
                format!(
                    "      <member><TopicArn>{}</TopicArn></member>",
                    t.topic_arn
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let next_token_xml = if has_more {
            format!(
                "\n    <NextToken>{}</NextToken>",
                next_token + DEFAULT_PAGE_SIZE
            )
        } else {
            String::new()
        };

        Ok(xml_resp(
            &format!(
                r#"<ListTopicsResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListTopicsResult>
    <Topics>
{members}
    </Topics>{next_token_xml}
  </ListTopicsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListTopicsResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn get_topic_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;

        // Check region: topic must belong to the request's region
        if let Some(topic_region) = arn_region(&topic_arn) {
            if topic_region != req.region {
                return Err(not_found("Topic"));
            }
        }

        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let topic = state
            .topics
            .get(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;

        let subs_confirmed = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn && s.confirmed)
            .count();
        let subs_pending = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn && !s.confirmed)
            .count();

        let mut entries = vec![
            format_attr("TopicArn", &topic.topic_arn),
            format_attr("Owner", &state.account_id),
            format_attr("SubscriptionsConfirmed", &subs_confirmed.to_string()),
            format_attr("SubscriptionsPending", &subs_pending.to_string()),
            format_attr(
                "SubscriptionsDeleted",
                &topic.subscriptions_deleted.to_string(),
            ),
        ];

        // EffectiveDeliveryPolicy: merge any user-set DeliveryPolicy
        // over the SNS topic-level default so callers see their
        // tuning reflected (matching real SNS).
        let user_policy = topic.attributes.get("DeliveryPolicy").map(|s| s.as_str());
        let effective = compute_effective_topic_delivery_policy(user_policy);
        entries.push(format_attr("EffectiveDeliveryPolicy", &effective));

        // Add all stored attributes
        for (k, v) in &topic.attributes {
            entries.push(format_attr(k, v));
        }

        let attrs = entries.join("\n");
        Ok(xml_resp(
            &format!(
                r#"<GetTopicAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetTopicAttributesResult>
    <Attributes>
{attrs}
    </Attributes>
  </GetTopicAttributesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetTopicAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn set_topic_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let attr_name = required(req, "AttributeName")?;
        let attr_value = param(req, "AttributeValue").unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let topic = state
            .topics
            .get_mut(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;

        // AWS accepts dotted-path attribute names like
        // `DeliveryPolicy.http.defaultHealthyRetryPolicy.numRetries` to
        // patch a single nested key without re-sending the full policy
        // document. Detect that form and merge into the stored
        // DeliveryPolicy JSON.
        if let Some(suffix) = attr_name.strip_prefix("DeliveryPolicy.") {
            let existing = topic
                .attributes
                .get("DeliveryPolicy")
                .map(|s| s.as_str())
                .unwrap_or("");
            let mut doc: serde_json::Value = if existing.trim().is_empty() {
                serde_json::from_str(DEFAULT_TOPIC_DELIVERY_POLICY).unwrap()
            } else {
                serde_json::from_str(existing).unwrap_or_else(|_| {
                    serde_json::from_str(DEFAULT_TOPIC_DELIVERY_POLICY).unwrap()
                })
            };
            apply_dotted_override(&mut doc, suffix, &attr_value);
            let serialized = serde_json::to_string(&doc).unwrap_or_default();
            topic
                .attributes
                .insert("DeliveryPolicy".to_string(), serialized);
        } else if attr_name == "Policy" {
            // If setting Policy, compact the JSON
            if let Ok(parsed) = serde_json::from_str::<Value>(&attr_value) {
                if let Ok(compact) = serde_json::to_string(&parsed) {
                    topic.attributes.insert(attr_name, compact);
                } else {
                    topic.attributes.insert(attr_name, attr_value);
                }
            } else {
                topic.attributes.insert(attr_name, attr_value);
            }
        } else {
            topic.attributes.insert(attr_name, attr_value);
        }

        Ok(xml_resp(
            &format!(
                r#"<SetTopicAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SetTopicAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn subscribe(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let protocol = required(req, "Protocol")?;
        let endpoint = param(req, "Endpoint").unwrap_or_default();

        let accts = self.state.read();
        let state_r = match accts.get(&req.account_id) {
            Some(s) => s,
            None => return Err(not_found("Topic")),
        };
        let topic = state_r
            .topics
            .get(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;
        let is_fifo_topic = topic.is_fifo;
        let account_id = req.account_id.clone();

        // Validate application endpoint exists
        if protocol == "application" {
            let endpoint_exists = state_r
                .platform_applications
                .values()
                .any(|app| app.endpoints.contains_key(&endpoint));
            if !endpoint_exists {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    format!(
                        "Invalid parameter: Endpoint Reason: Endpoint does not exist for endpoint {endpoint}"
                    ),
                ));
            }
        }
        drop(accts);

        // Validate SMS endpoint
        if protocol == "sms" {
            validate_sms_endpoint(&endpoint)?;
        }

        // Validate SQS endpoint (must be an ARN)
        if protocol == "sqs" && !endpoint.starts_with("arn:aws:sqs:") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: SQS endpoint ARN",
            ));
        }

        // Validate: FIFO SQS queues can only be subscribed to FIFO topics
        if protocol == "sqs" && endpoint.ends_with(".fifo") && !is_fifo_topic {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: Invalid parameter: Endpoint Reason: FIFO SQS Queues can not be subscribed to standard SNS topics",
            ));
        }

        // Parse subscription attributes
        let sub_attrs = parse_entries(req, "Attributes");

        // Validate subscription attribute names
        for key in sub_attrs.keys() {
            if !VALID_SUBSCRIPTION_ATTRS.contains(&key.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    format!("Invalid parameter: Attributes Reason: Unknown attribute: {key}"),
                ));
            }
        }

        // Validate and auto-set FilterPolicy
        let mut attributes = sub_attrs;
        if let Some(fp) = attributes.get("FilterPolicy") {
            if !fp.is_empty() {
                validate_filter_policy(fp)?;
            }
            if !attributes.contains_key("FilterPolicyScope") {
                attributes.insert(
                    "FilterPolicyScope".to_string(),
                    "MessageAttributes".to_string(),
                );
            }
        }

        // Check for duplicate subscription (same topic, protocol, endpoint)
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for sub in state.subscriptions.values() {
            if sub.topic_arn == topic_arn && sub.protocol == protocol && sub.endpoint == endpoint {
                return Ok(xml_resp(
                    &format!(
                        r#"<SubscribeResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <SubscribeResult>
    <SubscriptionArn>{}</SubscriptionArn>
  </SubscribeResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SubscribeResponse>"#,
                        sub.subscription_arn, req.request_id
                    ),
                    &req.request_id,
                ));
            }
        }

        let sub_arn = format!("{}:{}", topic_arn, uuid::Uuid::new_v4());

        // HTTP/HTTPS subscriptions start as pending (require confirmation)
        let confirmed = protocol != "http" && protocol != "https";
        // AWS returns the literal "pending confirmation" for an unconfirmed
        // subscription unless the caller sets ReturnSubscriptionArn=true, in
        // which case the real ARN is returned even while pending.
        let return_arn = param(req, "ReturnSubscriptionArn")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let response_arn = if confirmed || return_arn {
            sub_arn.clone()
        } else {
            "pending confirmation".to_string()
        };

        // Generate a confirmation token for pending subscriptions. AWS
        // issues a 256-character opaque token; matching the length keeps
        // clients that validate it happy.
        let confirmation_token = if !confirmed {
            Some(self::helpers::generate_confirmation_token())
        } else {
            None
        };
        // Snapshot the server endpoint while we still hold the lock so
        // the spawned POST can build a SubscribeURL pointing at *this*
        // fakecloud instance, not the public AWS endpoint.
        let server_endpoint = state.endpoint.clone();

        let sub = SnsSubscription {
            subscription_arn: sub_arn.clone(),
            topic_arn: topic_arn.clone(),
            protocol: protocol.clone(),
            endpoint: endpoint.clone(),
            owner: account_id,
            attributes,
            confirmed,
            confirmation_token: confirmation_token.clone(),
        };

        state.subscriptions.insert(sub_arn.clone(), sub);
        // Drop the write lock before any async work.
        drop(accounts);

        // For HTTP/HTTPS protocols, AWS POSTs a SubscriptionConfirmation
        // envelope to the endpoint and the subscriber must call back with
        // ConfirmSubscription using the embedded Token. Without this POST
        // any subscriber following AWS docs sits forever unconfirmed.
        if !confirmed && (protocol == "http" || protocol == "https") {
            // Skip the spawn when no Tokio runtime is in scope (sync unit
            // tests). Production callers always run inside the server
            // runtime.
            if tokio::runtime::Handle::try_current().is_err() {
                // fall through; nothing to spawn
            } else if let Some(token) = confirmation_token {
                let endpoint_url = endpoint.clone();
                let topic = topic_arn.clone();
                let sub_arn_for_header = sub_arn.clone();
                let server_url = server_endpoint.clone();
                tokio::spawn(async move {
                    let body =
                        build_subscription_confirmation_envelope(&topic, &token, &server_url);
                    let client = reqwest::Client::new();
                    let result = client
                        .post(&endpoint_url)
                        // AWS sends SNS notifications with text/plain content
                        // type even though the body is JSON. Subscriber SDKs
                        // (e.g. notification handlers) match on this exactly.
                        .header("Content-Type", "text/plain; charset=UTF-8")
                        .header("x-amz-sns-message-type", "SubscriptionConfirmation")
                        .header("x-amz-sns-topic-arn", &topic)
                        .header("x-amz-sns-subscription-arn", &sub_arn_for_header)
                        .header("User-Agent", "Amazon Simple Notification Service Agent")
                        .body(body)
                        .send()
                        .await;
                    if let Err(e) = result {
                        tracing::warn!(
                            endpoint = %endpoint_url,
                            error = %e,
                            "SNS SubscriptionConfirmation POST failed"
                        );
                    }
                });
            }
        }

        Ok(xml_resp(
            &format!(
                r#"<SubscribeResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <SubscribeResult>
    <SubscriptionArn>{response_arn}</SubscriptionArn>
  </SubscribeResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SubscribeResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn confirm_subscription(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let token = required(req, "Token")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // AWS accepts both the confirmation token and the subscription ARN as the Token parameter.
        // Confirming an already-confirmed subscription is a no-op (idempotent).
        let sub_arn = state
            .subscriptions
            .values()
            .find(|s| {
                s.topic_arn == topic_arn
                    && (s.confirmation_token.as_deref() == Some(&token)
                        || s.subscription_arn == token)
            })
            .map(|s| s.subscription_arn.clone())
            .ok_or_else(|| {
                // AWS returns InvalidParameterException for unknown or
                // expired confirmation tokens; matching the wire format
                // lets client SDKs surface the right error type.
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    format!("Invalid token: {token}"),
                )
            })?;

        // Mark the subscription as confirmed
        if let Some(sub) = state.subscriptions.get_mut(&sub_arn) {
            sub.confirmed = true;
        }

        Ok(xml_resp(
            &format!(
                r#"<ConfirmSubscriptionResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ConfirmSubscriptionResult>
    <SubscriptionArn>{sub_arn}</SubscriptionArn>
  </ConfirmSubscriptionResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ConfirmSubscriptionResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn unsubscribe(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let sub_arn = required(req, "SubscriptionArn")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id);
        // Snapshot the parent topic ARN before removing the subscription
        // so we can bump SubscriptionsDeleted on the right topic. Real
        // SNS exposes this counter on GetTopicAttributes.
        let parent_topic = state
            .subscriptions
            .get(&sub_arn)
            .map(|s| s.topic_arn.clone());
        if state.subscriptions.remove(&sub_arn).is_some() {
            if let Some(arn) = parent_topic {
                if let Some(topic) = state.topics.get_mut(&arn) {
                    topic.subscriptions_deleted = topic.subscriptions_deleted.saturating_add(1);
                }
            }
        }

        Ok(xml_resp(
            &format!(
                r#"<UnsubscribeResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</UnsubscribeResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn list_subscriptions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);

        let all_subs: Vec<&SnsSubscription> = state.subscriptions.values().collect();
        let next_token = param(req, "NextToken")
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let next_token = next_token.min(all_subs.len());

        let page = &all_subs[next_token..];
        let has_more = page.len() > DEFAULT_PAGE_SIZE;
        let page = if has_more {
            &page[..DEFAULT_PAGE_SIZE]
        } else {
            page
        };

        let members: String = page
            .iter()
            .map(|s| format_sub_member(s))
            .collect::<Vec<_>>()
            .join("\n");

        let next_token_xml = if has_more {
            format!(
                "\n    <NextToken>{}</NextToken>",
                next_token + DEFAULT_PAGE_SIZE
            )
        } else {
            String::new()
        };

        Ok(xml_resp(
            &format!(
                r#"<ListSubscriptionsResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListSubscriptionsResult>
    <Subscriptions>
{members}
    </Subscriptions>{next_token_xml}
  </ListSubscriptionsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListSubscriptionsResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn list_subscriptions_by_topic(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);

        let all_subs: Vec<&SnsSubscription> = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn)
            .collect();

        let next_token = param(req, "NextToken")
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let next_token = next_token.min(all_subs.len());

        let page = &all_subs[next_token..];
        let has_more = page.len() > DEFAULT_PAGE_SIZE;
        let page = if has_more {
            &page[..DEFAULT_PAGE_SIZE]
        } else {
            page
        };

        let members: String = page
            .iter()
            .map(|s| format_sub_member(s))
            .collect::<Vec<_>>()
            .join("\n");

        let next_token_xml = if has_more {
            format!(
                "\n    <NextToken>{}</NextToken>",
                next_token + DEFAULT_PAGE_SIZE
            )
        } else {
            String::new()
        };

        Ok(xml_resp(
            &format!(
                r#"<ListSubscriptionsByTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListSubscriptionsByTopicResult>
    <Subscriptions>
{members}
    </Subscriptions>{next_token_xml}
  </ListSubscriptionsByTopicResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListSubscriptionsByTopicResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn get_subscription_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sub_arn = required(req, "SubscriptionArn")?;
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let sub = state
            .subscriptions
            .get(&sub_arn)
            .ok_or_else(|| not_found("Subscription"))?;

        // PendingConfirmation reflects the real subscription state: an
        // http/https subscriber that hasn't confirmed its token is still
        // pending. Previously hardcoded "false", which hid unconfirmed
        // subscriptions (bug-audit 2026-06-20, 1.24).
        let pending = if sub.confirmed { "false" } else { "true" };
        let mut entries = vec![
            format_attr("SubscriptionArn", &sub.subscription_arn),
            format_attr("TopicArn", &sub.topic_arn),
            format_attr("Protocol", &sub.protocol),
            format_attr("Endpoint", &sub.endpoint),
            format_attr("Owner", &sub.owner),
            format_attr("ConfirmationWasAuthenticated", "true"),
            format_attr("PendingConfirmation", pending),
        ];

        // Add RawMessageDelivery from attributes or default
        if !sub.attributes.contains_key("RawMessageDelivery") {
            entries.push(format_attr("RawMessageDelivery", "false"));
        }

        // EffectiveDeliveryPolicy: merge any subscription-level
        // DeliveryPolicy over the SNS subscription default so callers
        // see their tuning reflected (matching real SNS).
        let user_policy = sub.attributes.get("DeliveryPolicy").map(|s| s.as_str());
        let effective = compute_effective_subscription_delivery_policy(user_policy);
        entries.push(format_attr("EffectiveDeliveryPolicy", &effective));

        for (k, v) in &sub.attributes {
            // Skip empty FilterPolicy (unsetting it removes it)
            if k == "FilterPolicy" && v.is_empty() {
                continue;
            }
            // If FilterPolicy is unset, also skip FilterPolicyScope
            if k == "FilterPolicyScope" {
                let has_filter = sub
                    .attributes
                    .get("FilterPolicy")
                    .map(|v| !v.is_empty())
                    .unwrap_or(false);
                if !has_filter {
                    continue;
                }
            }
            entries.push(format_attr(k, v));
        }
        let attrs = entries.join("\n");

        Ok(xml_resp(
            &format!(
                r#"<GetSubscriptionAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetSubscriptionAttributesResult>
    <Attributes>
{attrs}
    </Attributes>
  </GetSubscriptionAttributesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetSubscriptionAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn set_subscription_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sub_arn = required(req, "SubscriptionArn")?;
        let attr_name = required(req, "AttributeName")?;
        let attr_value = param(req, "AttributeValue").unwrap_or_default();

        // Validate attribute name
        if !VALID_SUBSCRIPTION_ATTRS.contains(&attr_name.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: AttributeName".to_string(),
            ));
        }

        // Validate filter policy
        if attr_name == "FilterPolicy" && !attr_value.is_empty() {
            validate_filter_policy(&attr_value)?;
        }

        // Validate DeliveryPolicy JSON. AWS rejects malformed JSON at set time;
        // previously it was stored unchecked and silently fell back to the
        // default at delivery, discarding the user's intended policy.
        if attr_name == "DeliveryPolicy"
            && !attr_value.is_empty()
            && serde_json::from_str::<serde_json::Value>(&attr_value).is_err()
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: DeliveryPolicy: failed to parse JSON".to_string(),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let sub = state
            .subscriptions
            .get_mut(&sub_arn)
            .ok_or_else(|| not_found("Subscription"))?;

        sub.attributes.insert(attr_name.clone(), attr_value.clone());

        // Setting FilterPolicy auto-sets FilterPolicyScope
        if attr_name == "FilterPolicy" && !attr_value.is_empty() {
            sub.attributes
                .entry("FilterPolicyScope".to_string())
                .or_insert_with(|| "MessageAttributes".to_string());
        }

        Ok(xml_resp(
            &format!(
                r#"<SetSubscriptionAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SetSubscriptionAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = required(req, "ResourceArn")?;
        let new_tags = parse_tags(req);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let topic = state.topics.get_mut(&resource_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFound",
                "Resource does not exist",
            )
        })?;

        // Check tag quota: existing + new (after dedup) must not exceed 50
        let mut merged = topic.tags.clone();
        for (k, v) in &new_tags {
            // Update existing or add
            if let Some(pos) = merged.iter().position(|(ek, _)| ek == k) {
                merged[pos] = (k.clone(), v.clone());
            } else {
                merged.push((k.clone(), v.clone()));
            }
        }
        if merged.len() > 50 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "TagLimitExceeded",
                "Could not complete request: tag quota of per resource exceeded",
            ));
        }

        topic.tags = merged;

        Ok(xml_resp(
            &format!(
                r#"<TagResourceResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <TagResourceResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</TagResourceResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = required(req, "ResourceArn")?;
        let tag_keys = parse_tag_keys(req);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let topic = state.topics.get_mut(&resource_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFound",
                "Resource does not exist",
            )
        })?;
        topic.tags.retain(|(k, _)| !tag_keys.contains(k));

        Ok(xml_resp(
            &format!(
                r#"<UntagResourceResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <UntagResourceResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</UntagResourceResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = required(req, "ResourceArn")?;
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let topic = state.topics.get(&resource_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFound",
                "Resource does not exist",
            )
        })?;

        let members: String = topic
            .tags
            .iter()
            .map(|(k, v)| {
                format!(
                    "      <member><Key>{}</Key><Value>{}</Value></member>",
                    xml_escape(k),
                    xml_escape(v)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<ListTagsForResourceResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListTagsForResourceResult>
    <Tags>
{members}
    </Tags>
  </ListTagsForResourceResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListTagsForResourceResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn add_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let label = required(req, "Label")?;

        // Parse AWSAccountId.member.N and ActionName.member.N
        let mut account_ids = Vec::new();
        for n in 1..=20 {
            let key = format!("AWSAccountId.member.{n}");
            if let Some(v) = req.query_params.get(&key) {
                account_ids.push(v.clone());
            } else {
                break;
            }
        }

        let mut action_names = Vec::new();
        for n in 1..=20 {
            let key = format!("ActionName.member.{n}");
            if let Some(v) = req.query_params.get(&key) {
                action_names.push(v.clone());
            } else {
                break;
            }
        }

        // Validate action names
        for action in &action_names {
            if !VALID_SNS_ACTIONS.contains(&action.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Policy statement action out of service scope!",
                ));
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let account_id = state.account_id.clone();
        let topic = state
            .topics
            .get_mut(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;

        // Get or create policy
        let policy_str = topic
            .attributes
            .get("Policy")
            .cloned()
            .unwrap_or_else(|| default_policy(&topic_arn, &account_id));

        let mut policy: Value = serde_json::from_str(&policy_str)
            .or_else(|_| serde_json::from_str(&default_policy(&topic_arn, &account_id)))
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    "Failed to parse topic policy",
                )
            })?;

        // Check if statement with this label already exists
        if let Some(statements) = policy["Statement"].as_array() {
            for stmt in statements {
                if stmt["Sid"].as_str() == Some(&label) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameter",
                        "Statement already exists",
                    ));
                }
            }
        }

        // Build principal
        let principal = if account_ids.len() == 1 {
            Value::String(Arn::global("iam", &account_ids[0], "root").to_string())
        } else {
            Value::Array(
                account_ids
                    .iter()
                    .map(|id| Value::String(Arn::global("iam", id, "root").to_string()))
                    .collect(),
            )
        };

        // Build action
        let action = if action_names.len() == 1 {
            Value::String(format!("SNS:{}", action_names[0]))
        } else {
            Value::Array(
                action_names
                    .iter()
                    .map(|a| Value::String(format!("SNS:{}", a)))
                    .collect(),
            )
        };

        let new_statement = serde_json::json!({
            "Sid": label,
            "Effect": "Allow",
            "Principal": {"AWS": principal},
            "Action": action,
            "Resource": topic_arn,
        });

        if let Some(statements) = policy["Statement"].as_array_mut() {
            statements.push(new_statement);
        }

        topic
            .attributes
            .insert("Policy".to_string(), policy.to_string());

        Ok(xml_resp(
            &format!(
                r#"<AddPermissionResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</AddPermissionResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn remove_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let label = required(req, "Label")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let topic = state
            .topics
            .get_mut(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;

        if let Some(policy_str) = topic.attributes.get("Policy").cloned() {
            if let Ok(mut policy) = serde_json::from_str::<Value>(&policy_str) {
                if let Some(statements) = policy["Statement"].as_array_mut() {
                    statements.retain(|s| s["Sid"].as_str() != Some(&label));
                }
                topic
                    .attributes
                    .insert("Policy".to_string(), policy.to_string());
            }
        }

        Ok(xml_resp(
            &format!(
                r#"<RemovePermissionResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</RemovePermissionResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    // ===== Platform Application actions =====

    fn get_data_protection_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = required(req, "ResourceArn")?;
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        if !state.topics.contains_key(&resource_arn) {
            return Err(not_found("Topic"));
        }
        let policy = state
            .data_protection_policies
            .get(&resource_arn)
            .cloned()
            .unwrap_or_default();
        Ok(xml_resp(
            &format!(
                r#"<GetDataProtectionPolicyResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetDataProtectionPolicyResult>
    <DataProtectionPolicy>{}</DataProtectionPolicy>
  </GetDataProtectionPolicyResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetDataProtectionPolicyResponse>"#,
                xml_escape(&policy),
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn put_data_protection_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = required(req, "ResourceArn")?;
        let policy = required(req, "DataProtectionPolicy")?;
        // AWS accepts an empty DataProtectionPolicy to clear the topic's
        // policy (this is how the Terraform resource deletes it). Only a
        // non-empty value must be valid JSON.
        let clearing = policy.trim().is_empty();
        if !clearing && serde_json::from_str::<Value>(&policy).is_err() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "DataProtectionPolicy must be valid JSON.",
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.topics.contains_key(&resource_arn) {
            return Err(not_found("Topic"));
        }
        if clearing {
            state.data_protection_policies.remove(&resource_arn);
        } else {
            state.data_protection_policies.insert(resource_arn, policy);
        }
        Ok(xml_resp(
            &format!(
                r#"<PutDataProtectionPolicyResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <PutDataProtectionPolicyResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</PutDataProtectionPolicyResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }
}

/// Sandbox languages SNS will send the verification code in. Mirrors the
/// `LanguageCodeString` enum in `aws-models/sns.json` — keep these two
/// lists in lockstep.
const SUPPORTED_SANDBOX_LANGUAGES: &[&str] = &[
    "en-US", "en-GB", "es-419", "es-ES", "de-DE", "fr-CA", "fr-FR", "it-IT", "ja-JP", "pt-BR",
    "kr-KR", "zh-CN", "zh-TW",
];

/// Inputs to `build_sns_lambda_event` — one SNS message delivered to one Lambda subscription.
pub(crate) struct SnsLambdaEventInput<'a> {
    pub message_id: &'a str,
    pub topic_arn: &'a str,
    pub subscription_arn: &'a str,
    pub message: &'a str,
    pub subject: Option<&'a str>,
    pub message_attributes: &'a serde_json::Map<String, Value>,
    pub timestamp: &'a chrono::DateTime<Utc>,
    pub endpoint: &'a str,
}

/// Parse MessageAttributes from query params.
/// Format: MessageAttributes.entry.N.Name, MessageAttributes.entry.N.Value.DataType,
///         MessageAttributes.entry.N.Value.StringValue
/// Subscribers of a topic, grouped by protocol. Returned by
/// `collect_topic_subscribers` so the fan-out loop doesn't have to
/// re-filter the subscriptions map five times inline.
pub(crate) struct TopicSubscribers {
    /// (queue_arn, raw_message_delivery, subscription_arn, redrive_policy)
    pub(crate) sqs: Vec<(String, bool, String, Option<String>)>,
    pub(crate) http: Vec<HttpSubscriber>,
    /// (function_arn, subscription_arn, redrive_policy)
    pub(crate) lambda: Vec<(String, String, Option<String>)>,
    /// (email_address, protocol) where protocol is `email` or `email-json`
    pub(crate) email: Vec<(String, String)>,
    pub(crate) sms: Vec<String>,
}

/// HTTP/HTTPS subscriber with the per-subscription DeliveryPolicy +
/// RedrivePolicy needed for retry/DLQ routing.
#[derive(Debug, Clone)]
pub(crate) struct HttpSubscriber {
    pub(crate) endpoint: String,
    pub(crate) subscription_arn: String,
    /// `http` or `https` — used to resolve the per-protocol body when
    /// `MessageStructure=json`.
    pub(crate) protocol: String,
    pub(crate) delivery_policy: Option<String>,
    pub(crate) redrive_policy: Option<String>,
}

/// Read-only state passed down the fan-out helpers so each helper has
/// the same data the monolithic publish() used to reference inline.
/// Visible to `delivery.rs` so the cross-service `SnsDelivery`
/// implementation can build the same context the direct `Publish` op
/// uses.
pub(crate) struct TopicFanoutContext<'a> {
    pub(crate) msg_id: &'a str,
    pub(crate) topic_arn: &'a str,
    pub(crate) subject: Option<&'a str>,
    pub(crate) endpoint: &'a str,
    pub(crate) default_message: &'a str,
    /// When `MessageStructure=json`, the parsed protocol->body map. Each
    /// fan-out helper resolves its own protocol key from this (falling back
    /// to the `default` key) so every subscriber gets a protocol-specific
    /// body, not just SQS. `None` for a plain (non-json) message.
    pub(crate) structure: Option<&'a Value>,
    pub(crate) envelope_attrs: &'a serde_json::Map<String, Value>,
    pub(crate) message_attributes: &'a BTreeMap<String, MessageAttribute>,
    pub(crate) message_group_id: Option<&'a str>,
    pub(crate) message_dedup_id: Option<&'a str>,
}

impl TopicFanoutContext<'_> {
    /// Resolve the body for a specific subscriber protocol when
    /// `MessageStructure=json`. AWS picks the protocol-specific key
    /// (`http`/`https`/`lambda`/`email`/`email-json`/`sms`/`application`/
    /// `sqs`/`firehose`) and falls back to the required `default` key when
    /// the protocol key is absent. With no JSON structure the resolved body
    /// is just `default_message`.
    pub(crate) fn body_for_protocol(&self, protocol: &str) -> String {
        match self.structure {
            Some(structure) => structure
                .get(protocol)
                .or_else(|| structure.get("default"))
                .and_then(|v| v.as_str())
                .unwrap_or(self.default_message)
                .to_string(),
            None => self.default_message.to_string(),
        }
    }
}

/// Known match type keys for filter policy objects.
const VALID_FILTER_MATCH_TYPES: &[&str] = &[
    "exists",
    "prefix",
    "suffix",
    "anything-but",
    "numeric",
    "equals-ignore-case",
];

const VALID_NUMERIC_OPS: &[&str] = &["=", "<", "<=", ">", ">="];
const LOWER_OPS: &[&str] = &[">", ">="];
const UPPER_OPS: &[&str] = &["<", "<="];

#[path = "service_platform.rs"]
mod service_platform;
#[path = "service_publish.rs"]
mod service_publish;
pub(crate) use service_publish::fan_out_to_subscribers;
#[path = "service_sms.rs"]
mod service_sms;

#[path = "helpers.rs"]
mod helpers;
pub(crate) use helpers::*;

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;

#[cfg(test)]
mod effective_policy_tests {
    use super::{
        apply_dotted_override, compute_effective_subscription_delivery_policy,
        compute_effective_topic_delivery_policy,
    };
    use serde_json::Value;

    #[test]
    fn topic_default_uses_http_wrapper() {
        let v: Value =
            serde_json::from_str(&compute_effective_topic_delivery_policy(None)).unwrap();
        assert_eq!(v["http"]["defaultHealthyRetryPolicy"]["numRetries"], 3);
        assert_eq!(v["http"]["disableSubscriptionOverrides"], false);
        assert!(v["http"]["defaultThrottlePolicy"].is_null());
        assert_eq!(
            v["http"]["defaultRequestPolicy"]["headerContentType"],
            "text/plain; charset=UTF-8"
        );
    }

    #[test]
    fn topic_falls_back_to_default_for_empty_policy() {
        let v: Value =
            serde_json::from_str(&compute_effective_topic_delivery_policy(Some(""))).unwrap();
        assert_eq!(v["http"]["defaultHealthyRetryPolicy"]["numRetries"], 3);
    }

    #[test]
    fn topic_falls_back_to_default_for_unparseable_policy() {
        let v: Value =
            serde_json::from_str(&compute_effective_topic_delivery_policy(Some("not json")))
                .unwrap();
        assert_eq!(v["http"]["defaultHealthyRetryPolicy"]["numRetries"], 3);
    }

    #[test]
    fn topic_merges_user_set_retry_count() {
        let user = r#"{"http":{"defaultHealthyRetryPolicy":{"numRetries":7}}}"#;
        let v: Value =
            serde_json::from_str(&compute_effective_topic_delivery_policy(Some(user))).unwrap();
        assert_eq!(v["http"]["defaultHealthyRetryPolicy"]["numRetries"], 7);
        // Default fields the user didn't override stay intact.
        assert_eq!(v["http"]["defaultHealthyRetryPolicy"]["minDelayTarget"], 20);
        assert_eq!(
            v["http"]["defaultHealthyRetryPolicy"]["backoffFunction"],
            "linear"
        );
        assert_eq!(v["http"]["disableSubscriptionOverrides"], false);
    }

    #[test]
    fn topic_user_can_override_disable_subscription_overrides() {
        let user = r#"{"http":{"disableSubscriptionOverrides":true}}"#;
        let v: Value =
            serde_json::from_str(&compute_effective_topic_delivery_policy(Some(user))).unwrap();
        assert_eq!(v["http"]["disableSubscriptionOverrides"], true);
        // Untouched defaults remain.
        assert_eq!(v["http"]["defaultHealthyRetryPolicy"]["numRetries"], 3);
    }

    #[test]
    fn subscription_default_is_flat() {
        let v: Value =
            serde_json::from_str(&compute_effective_subscription_delivery_policy(None)).unwrap();
        assert_eq!(v["defaultHealthyRetryPolicy"]["numRetries"], 3);
        assert_eq!(v["guaranteed"], false);
        // No `http` wrapper at subscription level.
        assert!(v.get("http").is_none());
    }

    #[test]
    fn subscription_merges_user_set_retry_count() {
        let user = r#"{"defaultHealthyRetryPolicy":{"numRetries":5}}"#;
        let v: Value =
            serde_json::from_str(&compute_effective_subscription_delivery_policy(Some(user)))
                .unwrap();
        assert_eq!(v["defaultHealthyRetryPolicy"]["numRetries"], 5);
        assert_eq!(v["defaultHealthyRetryPolicy"]["minDelayTarget"], 20);
        assert_eq!(v["guaranteed"], false);
    }

    #[test]
    fn subscription_user_can_override_top_level_keys() {
        let user = r#"{"guaranteed":true}"#;
        let v: Value =
            serde_json::from_str(&compute_effective_subscription_delivery_policy(Some(user)))
                .unwrap();
        assert_eq!(v["guaranteed"], true);
        assert_eq!(v["defaultHealthyRetryPolicy"]["numRetries"], 3);
    }

    #[test]
    fn dotted_override_patches_nested_int() {
        let mut doc: Value =
            serde_json::json!({"http": {"defaultHealthyRetryPolicy": {"numRetries": 3}}});
        apply_dotted_override(&mut doc, "http.defaultHealthyRetryPolicy.numRetries", "5");
        assert_eq!(doc["http"]["defaultHealthyRetryPolicy"]["numRetries"], 5);
    }

    #[test]
    fn dotted_override_patches_nested_bool() {
        let mut doc: Value = serde_json::json!({"http": {}});
        apply_dotted_override(&mut doc, "http.disableSubscriptionOverrides", "true");
        assert_eq!(doc["http"]["disableSubscriptionOverrides"], true);
    }

    #[test]
    fn dotted_override_creates_missing_path() {
        let mut doc: Value = serde_json::json!({});
        apply_dotted_override(
            &mut doc,
            "http.defaultRequestPolicy.headerContentType",
            "application/json",
        );
        assert_eq!(
            doc["http"]["defaultRequestPolicy"]["headerContentType"],
            "application/json"
        );
    }

    #[test]
    fn dotted_override_handles_null_literal() {
        let mut doc: Value =
            serde_json::json!({"http": {"defaultThrottlePolicy": {"maxReceivesPerSecond": 10}}});
        apply_dotted_override(&mut doc, "http.defaultThrottlePolicy", "null");
        assert!(doc["http"]["defaultThrottlePolicy"].is_null());
    }

    #[test]
    fn dotted_override_keeps_string_for_non_numeric() {
        let mut doc: Value = serde_json::json!({"http": {}});
        apply_dotted_override(
            &mut doc,
            "http.defaultHealthyRetryPolicy.backoffFunction",
            "exponential",
        );
        assert_eq!(
            doc["http"]["defaultHealthyRetryPolicy"]["backoffFunction"],
            "exponential"
        );
    }
}
