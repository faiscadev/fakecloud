use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::Serialize;
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

pub struct SesState {
    pub account_id: String,
    pub region: String,
    pub identities: HashMap<String, EmailIdentity>,
    pub configuration_sets: HashMap<String, ConfigurationSet>,
    pub templates: HashMap<String, EmailTemplate>,
    pub sent_emails: Vec<SentEmail>,
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
        }
    }

    pub fn reset(&mut self) {
        self.identities.clear();
        self.configuration_sets.clear();
        self.templates.clear();
        self.sent_emails.clear();
    }
}

pub type SharedSesState = Arc<RwLock<SesState>>;
