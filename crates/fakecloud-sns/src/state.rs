use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnsTopic {
    pub topic_arn: String,
    pub name: String,
    pub attributes: BTreeMap<String, String>,
    pub tags: Vec<(String, String)>,
    pub is_fifo: bool,
    pub created_at: DateTime<Utc>,
    /// Cumulative count of subscriptions that have been deleted from this
    /// topic. AWS exposes this as `SubscriptionsDeleted` on
    /// `GetTopicAttributes`; it never decreases. Bumped on every
    /// successful `Unsubscribe`. Cascade deletions from `DeleteTopic`
    /// reset along with the topic itself, so we don't bump there.
    #[serde(default)]
    pub subscriptions_deleted: u64,
    /// Monotonic per-topic counter for FIFO `SequenceNumber`s. AWS returns
    /// a strictly-increasing opaque decimal on every Publish/PublishBatch
    /// to a FIFO topic; non-FIFO topics emit none. (bug-audit 2026-05-28, 1.6)
    #[serde(default)]
    pub fifo_sequence: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnsSubscription {
    pub subscription_arn: String,
    pub topic_arn: String,
    pub protocol: String,
    pub endpoint: String,
    pub owner: String,
    pub attributes: BTreeMap<String, String>,
    pub confirmed: bool,
    /// Token used for HTTP/HTTPS subscription confirmation.
    pub confirmation_token: Option<String>,
}

/// An SNS message attribute (key-value with a data type).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MessageAttribute {
    pub data_type: String,
    pub string_value: Option<String>,
    pub binary_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublishedMessage {
    pub message_id: String,
    pub topic_arn: String,
    pub message: String,
    pub subject: Option<String>,
    pub message_attributes: BTreeMap<String, MessageAttribute>,
    pub message_group_id: Option<String>,
    pub message_dedup_id: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PlatformApplication {
    pub arn: String,
    pub name: String,
    pub platform: String,
    pub attributes: BTreeMap<String, String>,
    pub endpoints: BTreeMap<String, PlatformEndpoint>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PlatformEndpoint {
    pub arn: String,
    pub token: String,
    pub attributes: BTreeMap<String, String>,
    pub enabled: bool,
    pub messages: Vec<PublishedMessage>,
}

/// A recorded Lambda invocation from SNS delivery.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LambdaInvocation {
    pub function_arn: String,
    pub message: String,
    pub subject: Option<String>,
    pub timestamp: DateTime<Utc>,
}

/// A recorded email delivery from SNS (stub).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SentEmail {
    pub email_address: String,
    pub message: String,
    pub subject: Option<String>,
    pub topic_arn: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SmsSandboxPhoneStatus {
    Pending,
    Verified,
}

impl SmsSandboxPhoneStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SmsSandboxPhoneStatus::Pending => "Pending",
            SmsSandboxPhoneStatus::Verified => "Verified",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SmsSandboxPhoneNumber {
    pub phone_number: String,
    pub language_code: String,
    pub status: SmsSandboxPhoneStatus,
    pub one_time_password: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OriginationNumber {
    pub phone_number: String,
    pub iso_country_code: String,
    pub status: String,
    pub number_capabilities: Vec<String>,
    pub route_type: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnsState {
    pub account_id: String,
    pub region: String,
    pub endpoint: String,
    pub topics: BTreeMap<String, SnsTopic>, // arn -> topic (ordered for predictable iteration)
    pub subscriptions: BTreeMap<String, SnsSubscription>, // sub_arn -> subscription
    pub published: Vec<PublishedMessage>,
    pub platform_applications: BTreeMap<String, PlatformApplication>,
    pub sms_attributes: BTreeMap<String, String>,
    pub opted_out_numbers: Vec<String>,
    pub sms_messages: Vec<(String, String)>, // (phone_number, message)
    /// Recorded Lambda invocations (stub deliveries).
    pub lambda_invocations: Vec<LambdaInvocation>,
    /// Recorded email deliveries (stub — not actually sent).
    pub sent_emails: Vec<SentEmail>,
    #[serde(default)]
    pub sms_sandbox_phone_numbers: BTreeMap<String, SmsSandboxPhoneNumber>,
    #[serde(default)]
    pub origination_numbers: Vec<OriginationNumber>,
    /// Per-resource (topic ARN) data protection policy JSON.
    #[serde(default)]
    pub data_protection_policies: BTreeMap<String, String>,
}

impl SnsState {
    pub fn new(account_id: &str, region: &str, endpoint: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            endpoint: endpoint.to_string(),
            topics: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
            published: Vec::new(),
            platform_applications: BTreeMap::new(),
            sms_attributes: BTreeMap::new(),
            opted_out_numbers: Vec::new(),
            sms_messages: Vec::new(),
            lambda_invocations: Vec::new(),
            sent_emails: Vec::new(),
            sms_sandbox_phone_numbers: BTreeMap::new(),
            origination_numbers: Vec::new(),
            data_protection_policies: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.topics.clear();
        self.subscriptions.clear();
        self.published.clear();
        self.platform_applications.clear();
        self.sms_attributes.clear();
        self.opted_out_numbers.clear();
        self.sms_messages.clear();
        self.lambda_invocations.clear();
        self.sent_emails.clear();
        self.sms_sandbox_phone_numbers.clear();
        self.origination_numbers.clear();
        self.data_protection_policies.clear();
    }

    /// Whether the SNS account is in the SMS sandbox: no destination
    /// number has been verified yet.
    pub fn is_sms_sandboxed(&self) -> bool {
        !self
            .sms_sandbox_phone_numbers
            .values()
            .any(|n| n.status == SmsSandboxPhoneStatus::Verified)
    }

    /// Lazy-seed a default origination number so listing returns something
    /// realistic without forcing tests to set one up.
    pub fn seed_default_origination_numbers(&mut self) {
        if self.origination_numbers.is_empty() {
            self.origination_numbers.push(OriginationNumber {
                phone_number: "+18005550100".to_string(),
                iso_country_code: "US".to_string(),
                status: "ACTIVE".to_string(),
                number_capabilities: vec!["SMS".to_string()],
                route_type: "Transactional".to_string(),
                created_at: Utc::now(),
            });
        }
    }

    /// Seed default opt-out phone numbers.
    pub fn seed_default_opted_out(&mut self) {
        if self.opted_out_numbers.is_empty() {
            self.opted_out_numbers.push("+15005550099".to_string());
            self.opted_out_numbers.push("+447428545399".to_string());
        }
    }
}

pub type SharedSnsState = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<SnsState>>>;

/// On-disk snapshot envelope for SNS state. Versioned so format
/// changes fail loudly on upgrade.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnsSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<SnsState>>,
    #[serde(default)]
    pub state: Option<SnsState>,
}

pub const SNS_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

impl fakecloud_core::multi_account::AccountState for SnsState {
    fn new_for_account(account_id: &str, region: &str, endpoint: &str) -> Self {
        Self::new(account_id, region, endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes_empty() {
        let state = SnsState::new("123456789012", "us-east-1", "http://localhost:4566");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert_eq!(state.endpoint, "http://localhost:4566");
        assert!(state.topics.is_empty());
    }

    #[test]
    fn reset_clears_state() {
        let mut state = SnsState::new("123456789012", "us-east-1", "http://localhost:4566");
        state.opted_out_numbers.push("+15551234567".to_string());
        state.reset();
        assert!(state.opted_out_numbers.is_empty());
    }

    #[test]
    fn seed_default_opted_out_adds_phones() {
        let mut state = SnsState::new("123456789012", "us-east-1", "http://localhost:4566");
        state.seed_default_opted_out();
        assert!(!state.opted_out_numbers.is_empty());
    }

    #[test]
    fn account_state_trait() {
        use fakecloud_core::multi_account::AccountState;
        let state = SnsState::new_for_account("x", "us-east-1", "http://x");
        assert_eq!(state.account_id, "x");
    }
}
