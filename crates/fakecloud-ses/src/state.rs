use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
pub struct EmailIdentity {
    pub identity_name: String,
    pub identity_type: String,
    pub verified: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmailTemplate {
    pub template_name: String,
    pub subject: Option<String>,
    pub html_body: Option<String>,
    pub text_body: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigurationSet {
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SentEmail {
    pub message_id: String,
    pub from: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: Option<String>,
    pub html_body: Option<String>,
    pub text_body: Option<String>,
    pub raw_data: Option<String>,
    pub template_name: Option<String>,
    pub template_data: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContactList {
    pub contact_list_name: String,
    pub description: Option<String>,
    pub topics: Vec<Topic>,
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    pub topic_name: String,
    pub display_name: String,
    pub description: String,
    pub default_subscription_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Contact {
    pub email_address: String,
    pub topic_preferences: Vec<TopicPreference>,
    pub unsubscribe_all: bool,
    pub attributes_data: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPreference {
    pub topic_name: String,
    pub subscription_status: String,
}

pub struct SesState {
    pub account_id: String,
    pub region: String,
    pub identities: HashMap<String, EmailIdentity>,
    pub configuration_sets: HashMap<String, ConfigurationSet>,
    pub templates: HashMap<String, EmailTemplate>,
    pub sent_emails: Vec<SentEmail>,
    pub contact_lists: HashMap<String, ContactList>,
    pub contacts: HashMap<String, HashMap<String, Contact>>,
    /// Tags keyed by resource ARN, value is key→value tag map.
    pub tags: HashMap<String, HashMap<String, String>>,
}

impl SesState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            identities: HashMap::new(),
            configuration_sets: HashMap::new(),
            templates: HashMap::new(),
            sent_emails: Vec::new(),
            contact_lists: HashMap::new(),
            contacts: HashMap::new(),
            tags: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.identities.clear();
        self.configuration_sets.clear();
        self.templates.clear();
        self.sent_emails.clear();
        self.contact_lists.clear();
        self.contacts.clear();
        self.tags.clear();
    }
}

pub type SharedSesState = Arc<RwLock<SesState>>;
