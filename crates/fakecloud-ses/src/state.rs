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
    // DKIM attributes
    pub dkim_signing_enabled: bool,
    pub dkim_signing_attributes_origin: String,
    pub dkim_domain_signing_private_key: Option<String>,
    pub dkim_domain_signing_selector: Option<String>,
    pub dkim_next_signing_key_length: Option<String>,
    // Feedback attributes
    pub email_forwarding_enabled: bool,
    // Mail-from attributes
    pub mail_from_domain: Option<String>,
    pub mail_from_behavior_on_mx_failure: String,
    // Configuration set association
    pub configuration_set_name: Option<String>,
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
    // Sending options
    pub sending_enabled: bool,
    // Delivery options
    pub tls_policy: String,
    pub sending_pool_name: Option<String>,
    // Tracking options
    pub custom_redirect_domain: Option<String>,
    pub https_policy: Option<String>,
    // Suppression options
    pub suppressed_reasons: Vec<String>,
    // Reputation options
    pub reputation_metrics_enabled: bool,
    // VDM options
    pub vdm_options: Option<serde_json::Value>,
    // Archiving options
    pub archive_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CustomVerificationEmailTemplate {
    pub template_name: String,
    pub from_email_address: String,
    pub template_subject: String,
    pub template_content: String,
    pub success_redirection_url: String,
    pub failure_redirection_url: String,
    pub created_at: DateTime<Utc>,
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

#[derive(Debug, Clone, Serialize)]
pub struct SuppressedDestination {
    pub email_address: String,
    pub reason: String,
    pub last_update_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDestination {
    pub name: String,
    pub enabled: bool,
    pub matching_event_types: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kinesis_firehose_destination: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloud_watch_destination: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sns_destination: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_bridge_destination: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pinpoint_destination: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DedicatedIpPool {
    pub pool_name: String,
    pub scaling_mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DedicatedIp {
    pub ip: String,
    pub warmup_status: String,
    pub warmup_percentage: i32,
    pub pool_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MultiRegionEndpoint {
    pub endpoint_name: String,
    pub endpoint_id: String,
    pub status: String,
    pub regions: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct AccountDetails {
    pub mail_type: Option<String>,
    pub website_url: Option<String>,
    pub contact_language: Option<String>,
    pub use_case_description: Option<String>,
    pub additional_contact_email_addresses: Vec<String>,
    pub production_access_enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct AccountSettings {
    pub sending_enabled: bool,
    pub dedicated_ip_auto_warmup_enabled: bool,
    pub suppressed_reasons: Vec<String>,
    pub vdm_attributes: Option<serde_json::Value>,
    pub details: Option<AccountDetails>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImportJob {
    pub job_id: String,
    pub import_destination: serde_json::Value,
    pub import_data_source: serde_json::Value,
    pub job_status: String,
    pub created_timestamp: DateTime<Utc>,
    pub completed_timestamp: Option<DateTime<Utc>>,
    pub processed_records_count: i32,
    pub failed_records_count: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExportJob {
    pub job_id: String,
    pub export_source_type: String,
    pub export_destination: serde_json::Value,
    pub export_data_source: serde_json::Value,
    pub job_status: String,
    pub created_timestamp: DateTime<Utc>,
    pub completed_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Tenant {
    pub tenant_name: String,
    pub tenant_id: String,
    pub tenant_arn: String,
    pub created_timestamp: DateTime<Utc>,
    pub sending_status: String,
    pub tags: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TenantResourceAssociation {
    pub resource_arn: String,
    pub associated_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReputationEntityState {
    pub reputation_entity_reference: String,
    pub reputation_entity_type: String,
    pub reputation_management_policy: Option<String>,
    pub customer_managed_status: String,
    pub sending_status_aggregate: String,
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
    /// Suppression list: email → suppressed destination info.
    pub suppressed_destinations: HashMap<String, SuppressedDestination>,
    /// Event destinations: config set name → list of event destinations.
    pub event_destinations: HashMap<String, Vec<EventDestination>>,
    /// Identity policies: identity name → policy name → policy JSON document.
    pub identity_policies: HashMap<String, HashMap<String, String>>,
    /// Custom verification email templates: template name → template.
    pub custom_verification_email_templates: HashMap<String, CustomVerificationEmailTemplate>,
    /// Dedicated IP pools: pool name → pool.
    pub dedicated_ip_pools: HashMap<String, DedicatedIpPool>,
    /// Dedicated IPs: IP address → dedicated IP info.
    pub dedicated_ips: HashMap<String, DedicatedIp>,
    /// Multi-region endpoints: endpoint name → endpoint.
    pub multi_region_endpoints: HashMap<String, MultiRegionEndpoint>,
    /// Account-level settings (sending, suppression, VDM, details).
    pub account_settings: AccountSettings,
    /// Import jobs: job_id → ImportJob.
    pub import_jobs: HashMap<String, ImportJob>,
    /// Export jobs: job_id → ExportJob.
    pub export_jobs: HashMap<String, ExportJob>,
    /// Tenants: tenant_name → Tenant.
    pub tenants: HashMap<String, Tenant>,
    /// Tenant resource associations: tenant_name → Vec<resource_arn>.
    pub tenant_resource_associations: HashMap<String, Vec<TenantResourceAssociation>>,
    /// Reputation entities: "type/reference" → ReputationEntity.
    pub reputation_entities: HashMap<String, ReputationEntityState>,
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
            suppressed_destinations: HashMap::new(),
            event_destinations: HashMap::new(),
            identity_policies: HashMap::new(),
            custom_verification_email_templates: HashMap::new(),
            dedicated_ip_pools: HashMap::new(),
            dedicated_ips: HashMap::new(),
            multi_region_endpoints: HashMap::new(),
            account_settings: AccountSettings {
                sending_enabled: true,
                dedicated_ip_auto_warmup_enabled: false,
                suppressed_reasons: Vec::new(),
                vdm_attributes: None,
                details: None,
            },
            import_jobs: HashMap::new(),
            export_jobs: HashMap::new(),
            tenants: HashMap::new(),
            tenant_resource_associations: HashMap::new(),
            reputation_entities: HashMap::new(),
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
        self.suppressed_destinations.clear();
        self.event_destinations.clear();
        self.identity_policies.clear();
        self.custom_verification_email_templates.clear();
        self.dedicated_ip_pools.clear();
        self.dedicated_ips.clear();
        self.multi_region_endpoints.clear();
        self.account_settings = AccountSettings {
            sending_enabled: true,
            dedicated_ip_auto_warmup_enabled: false,
            suppressed_reasons: Vec::new(),
            vdm_attributes: None,
            details: None,
        };
        self.import_jobs.clear();
        self.export_jobs.clear();
        self.tenants.clear();
        self.tenant_resource_associations.clear();
        self.reputation_entities.clear();
    }
}

pub type SharedSesState = Arc<RwLock<SesState>>;
