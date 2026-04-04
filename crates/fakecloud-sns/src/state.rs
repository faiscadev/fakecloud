use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SnsTopic {
    pub topic_arn: String,
    pub name: String,
    pub attributes: HashMap<String, String>,
    pub tags: Vec<(String, String)>,
    pub is_fifo: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SnsSubscription {
    pub subscription_arn: String,
    pub topic_arn: String,
    pub protocol: String,
    pub endpoint: String,
    pub owner: String,
    pub attributes: HashMap<String, String>,
    pub confirmed: bool,
}

/// An SNS message attribute (key-value with a data type).
#[derive(Debug, Clone)]
pub struct MessageAttribute {
    pub data_type: String,
    pub string_value: Option<String>,
    pub binary_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct PublishedMessage {
    pub message_id: String,
    pub topic_arn: String,
    pub message: String,
    pub subject: Option<String>,
    pub message_attributes: HashMap<String, MessageAttribute>,
    pub message_group_id: Option<String>,
    pub message_dedup_id: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PlatformApplication {
    pub arn: String,
    pub name: String,
    pub platform: String,
    pub attributes: HashMap<String, String>,
    pub endpoints: HashMap<String, PlatformEndpoint>,
}

#[derive(Debug, Clone)]
pub struct PlatformEndpoint {
    pub arn: String,
    pub token: String,
    pub attributes: HashMap<String, String>,
    pub enabled: bool,
    pub messages: Vec<PublishedMessage>,
}

pub struct SnsState {
    pub account_id: String,
    pub region: String,
    pub topics: BTreeMap<String, SnsTopic>, // arn -> topic (ordered for predictable iteration)
    pub subscriptions: BTreeMap<String, SnsSubscription>, // sub_arn -> subscription
    pub published: Vec<PublishedMessage>,
    pub platform_applications: BTreeMap<String, PlatformApplication>,
    pub sms_attributes: HashMap<String, String>,
    pub opted_out_numbers: Vec<String>,
    pub sms_messages: Vec<(String, String)>, // (phone_number, message)
}

impl SnsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            topics: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
            published: Vec::new(),
            platform_applications: BTreeMap::new(),
            sms_attributes: HashMap::new(),
            opted_out_numbers: Vec::new(),
            sms_messages: Vec::new(),
        }
    }
}

pub type SharedSnsState = Arc<RwLock<SnsState>>;
