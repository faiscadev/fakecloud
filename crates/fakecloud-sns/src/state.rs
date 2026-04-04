use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SnsTopic {
    pub topic_arn: String,
    pub name: String,
    pub attributes: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SnsSubscription {
    pub subscription_arn: String,
    pub topic_arn: String,
    pub protocol: String,
    pub endpoint: String,
    pub attributes: HashMap<String, String>,
    pub confirmed: bool,
}

#[derive(Debug, Clone)]
pub struct PublishedMessage {
    pub message_id: String,
    pub topic_arn: String,
    pub message: String,
    pub subject: Option<String>,
    pub timestamp: DateTime<Utc>,
}

pub struct SnsState {
    pub account_id: String,
    pub region: String,
    pub topics: HashMap<String, SnsTopic>, // arn -> topic
    pub subscriptions: HashMap<String, SnsSubscription>, // sub_arn -> subscription
    pub published: Vec<PublishedMessage>,
}

impl SnsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            topics: HashMap::new(),
            subscriptions: HashMap::new(),
            published: Vec::new(),
        }
    }
}

pub type SharedSnsState = Arc<RwLock<SnsState>>;
