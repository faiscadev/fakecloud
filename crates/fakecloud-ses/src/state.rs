use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// SubjectPublicKeyInfo DER, base64-encoded. Populated when Easy DKIM
    /// generates the keypair. BYODKIM imports leave this empty (the user
    /// publishes their own public record).
    #[serde(default)]
    pub dkim_public_key_b64: Option<String>,
    // Feedback attributes
    pub email_forwarding_enabled: bool,
    // Mail-from attributes
    pub mail_from_domain: Option<String>,
    pub mail_from_behavior_on_mx_failure: String,
    /// Real SES walks PENDING -> SUCCESS once it observes MX/TXT records.
    /// Default `NotStarted`; set to `Pending` on first PutMailFromAttributes;
    /// auto-advances to `Success` on next read or via admin endpoint.
    #[serde(default)]
    pub mail_from_domain_status: String,
    // Configuration set association
    pub configuration_set_name: Option<String>,
    // SNS notification topics per type, set by SetIdentityNotificationTopic and
    // surfaced by GetIdentityNotificationAttributes (bug-audit 2026-06-20, 1.19).
    #[serde(default)]
    pub bounce_topic: Option<String>,
    #[serde(default)]
    pub complaint_topic: Option<String>,
    #[serde(default)]
    pub delivery_topic: Option<String>,
    /// Domain-verification TXT token. Deterministic per identity and stored so
    /// VerifyDomainIdentity and GetIdentityVerificationAttributes report the
    /// same value (the Terraform data source asserts they match).
    #[serde(default)]
    pub verification_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailTemplate {
    pub template_name: String,
    pub subject: Option<String>,
    pub html_body: Option<String>,
    pub text_body: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationSet {
    pub name: String,
    // Sending options
    pub sending_enabled: bool,
    // Delivery options
    pub tls_policy: String,
    pub sending_pool_name: Option<String>,
    /// Max seconds SES retries delivery before giving up. Optional; only
    /// reported on GetConfigurationSet when explicitly configured.
    #[serde(default)]
    pub max_delivery_seconds: Option<i64>,
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
    /// `MessageSecurityOptions` (the S/MIME `SigningScheme` union) as
    /// supplied by CreateConfigurationSet or UpdateConfigurationSet.
    /// `None` until configured; GetConfigurationSet only reports the
    /// block once it has been set, the same way DeliveryOptions and
    /// TrackingOptions are only echoed when non-default.
    #[serde(default)]
    pub message_security_options: Option<serde_json::Value>,
    /// Tracks whether `ArchivingOptions` was set on the configuration set
    /// (via Create or PutConfigurationSetArchivingOptions). AWS surfaces
    /// the structure on GetConfigurationSet even when only `ArchiveArn`
    /// is empty, so a missing field on Create round-trips would be a
    /// silent input drop. Defaults to `false` for snapshots created
    /// before the field existed.
    #[serde(default)]
    pub archiving_options_present: bool,
}

/// One S/MIME certificate association between an email identity and an
/// ACM certificate, created by `AssociateEmailIdentityCertificate`.
/// Real SES allows a single association per from-address.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityCertificate {
    /// Address the certificate signs for. Always populated: for an
    /// email-address identity it defaults to the identity itself, for a
    /// domain identity the caller must name an address in that domain.
    pub from_address: String,
    /// PROVISIONING | ACTIVE | INACTIVE | DEPROVISIONING | FAILED.
    /// Starts at PROVISIONING and advances to ACTIVE on the next read,
    /// the same way `mail_from_domain_status` walks Pending -> Success.
    pub status: String,
    /// ARN of the ACM certificate, exactly as supplied by the caller.
    pub certificate_arn: String,
    pub associated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomVerificationEmailTemplate {
    pub template_name: String,
    pub from_email_address: String,
    pub template_subject: String,
    pub template_content: String,
    pub success_redirection_url: String,
    pub failure_redirection_url: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Computed `DKIM-Signature` header value when the sender's identity
    /// has DKIM signing enabled. Empty string when sender unverified or
    /// DKIM not configured.
    #[serde(default)]
    pub dkim_signature: Option<String>,
    /// Synthesized RFC 5322-style headers for the stored message. When
    /// DKIM signing is active the `DKIM-Signature` header is the first
    /// entry, ahead of the `From`/`To`/`Subject`/`Date`/`Message-ID`
    /// headers covered by the signature. Empty for messages stored
    /// before DKIM was wired up.
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    pub timestamp: DateTime<Utc>,
    /// Tags applied to the email at send time (EmailTags from v2 SendEmail).
    #[serde(default)]
    pub email_tags: Vec<(String, String)>,
    /// Per-destination delivery insights populated by the event fanout.
    #[serde(default)]
    pub delivery_insights: Vec<EmailRecipientInsight>,
}

/// Per-recipient delivery insights for MessageInsights.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailRecipientInsight {
    pub destination: String,
    pub isp: String,
    pub events: Vec<DeliveryInsightEvent>,
}

/// A single event within an EmailRecipientInsight.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DeliveryInsightEvent {
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounce_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounce_sub_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complaint_sub_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complaint_feedback_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentBounce {
    pub bounce_message_id: String,
    pub original_message_id: String,
    pub bounce_sender: String,
    pub bounced_recipients: Vec<String>,
    pub timestamp: DateTime<Utc>,
    /// Per-recipient bounce details captured from
    /// `BouncedRecipientInfoList`. Empty for bounces queued before the
    /// field was added (preserved via `#[serde(default)]`).
    #[serde(default)]
    pub bounced_recipient_info: Vec<BouncedRecipientInfo>,
    /// Optional explanation extracted from the `Explanation` parameter
    /// of `SendBounce`.
    #[serde(default)]
    pub explanation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BouncedRecipientInfo {
    pub recipient: String,
    pub bounce_type: String,
    pub action: String,
    pub status: String,
    pub diagnostic_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Optional in the model; absent when the caller sent none. Snapshots
    /// written before this was optional stored `""` for "no description".
    #[serde(default)]
    pub description: Option<String>,
    pub default_subscription_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl EventDestination {
    /// The Firehose destination's `(IAM role ARN, delivery stream ARN)`.
    /// Destinations are stored as created, so v1 (`IAMRoleARN`,
    /// `DeliveryStreamARN`) and v2 (`IamRoleArn`, `DeliveryStreamArn`)
    /// member spellings both appear; each API renders its own. Empty or
    /// null members (v1 create stores "" for an omitted field) are absent.
    pub(crate) fn firehose_arns(&self) -> Option<(Option<&str>, Option<&str>)> {
        let k = self.kinesis_firehose_destination.as_ref()?;
        Some((
            spelled_member(k, ["IamRoleArn", "IAMRoleARN"]),
            spelled_member(k, ["DeliveryStreamArn", "DeliveryStreamARN"]),
        ))
    }

    /// The SNS destination's topic ARN. The APIs store `TopicArn`; a
    /// CloudFormation-provisioned destination keeps the template's `TopicARN`.
    pub(crate) fn sns_topic_arn(&self) -> Option<&str> {
        spelled_member(self.sns_destination.as_ref()?, ["TopicArn", "TopicARN"])
    }
}

/// First non-empty string value among the alternative member spellings.
fn spelled_member<'a>(value: &'a serde_json::Value, names: [&str; 2]) -> Option<&'a str> {
    names
        .iter()
        .find_map(|n| value.get(*n)?.as_str().filter(|s| !s.is_empty()))
}

/// `(v1, v2)` spellings of every SES event type.
const EVENT_TYPE_NAMES: &[(&str, &str)] = &[
    ("send", "SEND"),
    ("reject", "REJECT"),
    ("bounce", "BOUNCE"),
    ("complaint", "COMPLAINT"),
    ("delivery", "DELIVERY"),
    ("open", "OPEN"),
    ("click", "CLICK"),
    ("renderingFailure", "RENDERING_FAILURE"),
    ("deliveryDelay", "DELIVERY_DELAY"),
    ("subscription", "SUBSCRIPTION"),
];

/// `(v1, v2)` spellings of every CloudWatch `DimensionValueSource`.
const DIMENSION_VALUE_SOURCES: &[(&str, &str)] = &[
    ("messageTag", "MESSAGE_TAG"),
    ("emailHeader", "EMAIL_HEADER"),
    ("linkTag", "LINK_TAG"),
];

fn v1_spelling<'a>(table: &[(&'static str, &'static str)], value: &'a str) -> &'a str {
    table
        .iter()
        .find(|(v1, v2)| value == *v1 || value == *v2)
        .map_or(value, |(v1, _)| v1)
}

fn v2_spelling<'a>(table: &[(&'static str, &'static str)], value: &'a str) -> &'a str {
    table
        .iter()
        .find(|(v1, v2)| value == *v1 || value == *v2)
        .map_or(value, |(_, v2)| v2)
}

/// An event type in SES v1 spelling (`renderingFailure`), whichever API
/// stored it. Unknown values pass through unchanged.
pub(crate) fn v1_event_type_name(event_type: &str) -> &str {
    v1_spelling(EVENT_TYPE_NAMES, event_type)
}

/// An event type in SES v2 spelling (`RENDERING_FAILURE`), whichever API
/// stored it. Unknown values pass through unchanged.
pub(crate) fn v2_event_type_name(event_type: &str) -> &str {
    v2_spelling(EVENT_TYPE_NAMES, event_type)
}

/// A `DimensionValueSource` in SES v1 spelling (`messageTag`).
pub(crate) fn v1_dimension_value_source(source: &str) -> &str {
    v1_spelling(DIMENSION_VALUE_SOURCES, source)
}

/// A `DimensionValueSource` in SES v2 spelling (`MESSAGE_TAG`).
pub(crate) fn v2_dimension_value_source(source: &str) -> &str {
    v2_spelling(DIMENSION_VALUE_SOURCES, source)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedicatedIpPool {
    pub pool_name: String,
    pub scaling_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedicatedIp {
    pub ip: String,
    pub warmup_status: String,
    pub warmup_percentage: i32,
    pub pool_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiRegionEndpoint {
    pub endpoint_name: String,
    pub endpoint_id: String,
    pub status: String,
    pub regions: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AccountDetails {
    pub mail_type: Option<String>,
    pub website_url: Option<String>,
    pub contact_language: Option<String>,
    pub use_case_description: Option<String>,
    pub additional_contact_email_addresses: Vec<String>,
    pub production_access_enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AccountSettings {
    pub sending_enabled: bool,
    pub dedicated_ip_auto_warmup_enabled: bool,
    pub suppressed_reasons: Vec<String>,
    pub vdm_attributes: Option<serde_json::Value>,
    pub details: Option<AccountDetails>,
    /// Sandbox vs production. New SES accounts default to sandbox
    /// (`false`), which gates SendEmail on having every recipient also
    /// verified. Flipped via PutAccountDetails or the admin endpoint.
    #[serde(default)]
    pub production_access_enabled: bool,
    /// Account pricing plan (NONE | ESSENTIALS | PRO | ENTERPRISE) set via
    /// PutAccountPricingAttributes and reported by GetAccount's
    /// PricingAttributes. `None` reads back as the `NONE` plan.
    #[serde(default)]
    pub pricing_plan: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportJob {
    pub job_id: String,
    pub export_source_type: String,
    pub export_destination: serde_json::Value,
    pub export_data_source: serde_json::Value,
    pub job_status: String,
    pub created_timestamp: DateTime<Utc>,
    pub completed_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub tenant_name: String,
    pub tenant_id: String,
    pub tenant_arn: String,
    pub created_timestamp: DateTime<Utc>,
    pub sending_status: String,
    pub tags: Vec<serde_json::Value>,
    /// Suppression-list preferences for the tenant: `{ SuppressedReasons,
    /// ValidationAttributes }`. Set on CreateTenant or via
    /// PutTenantSuppressionAttributes. `None` until configured.
    #[serde(default)]
    pub suppression_attributes: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantResourceAssociation {
    pub resource_arn: String,
    pub associated_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReputationEntityState {
    pub reputation_entity_reference: String,
    pub reputation_entity_type: String,
    pub reputation_management_policy: Option<String>,
    pub customer_managed_status: String,
    pub sending_status_aggregate: String,
}

// ── SES v1 Receipt Rule types ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiptRuleSet {
    pub name: String,
    pub rules: Vec<ReceiptRule>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiptRule {
    pub name: String,
    pub enabled: bool,
    pub scan_enabled: bool,
    pub tls_policy: String,
    pub recipients: Vec<String>,
    pub actions: Vec<ReceiptAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReceiptAction {
    S3 {
        bucket_name: String,
        object_key_prefix: Option<String>,
        topic_arn: Option<String>,
        kms_key_arn: Option<String>,
    },
    Sns {
        topic_arn: String,
        encoding: Option<String>,
    },
    Lambda {
        function_arn: String,
        invocation_type: Option<String>,
        topic_arn: Option<String>,
    },
    Bounce {
        smtp_reply_code: String,
        message: String,
        sender: String,
        status_code: Option<String>,
        topic_arn: Option<String>,
    },
    AddHeader {
        header_name: String,
        header_value: String,
    },
    Stop {
        scope: String,
        topic_arn: Option<String>,
    },
    Workmail {
        organization_arn: String,
        topic_arn: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiptFilter {
    pub name: String,
    pub ip_filter: IpFilter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpFilter {
    pub cidr: String,
    pub policy: String, // "Allow" or "Block"
}

/// One email accepted by the inbound SMTP listener
/// (`ses_smtp.rs::store_email`). Captured alongside the
/// matching `SentEmail` so tests can assert SMTP-specific facts
/// (auth user, raw size) without re-deriving them from `raw_data`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmtpSubmission {
    pub message_id: String,
    pub from: String,
    pub to: Vec<String>,
    pub subject: Option<String>,
    pub raw_size_bytes: usize,
    pub received_at: DateTime<Utc>,
    pub auth_user: String,
}

/// One event-destination dispatch logged by the SES fanout. Captured
/// every time `fanout::deliver_event` actually hands an event off to
/// SNS/EventBridge/Kinesis/Firehose/CloudWatch so tests can assert the
/// downstream wiring without scraping the target service's introspection
/// state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDestinationDispatch {
    pub destination_name: String,
    /// One of `sns` | `eventbridge` | `kinesis` | `firehose` | `cloudwatch`.
    pub destination_type: String,
    pub event_type: String,
    pub message_id: String,
    pub dispatched_at: DateTime<Utc>,
    /// ARN / target identifier of the downstream resource the event was
    /// sent to. Empty for CloudWatch (uses metric namespace, not ARN).
    pub target_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundEmail {
    pub message_id: String,
    pub from: String,
    pub to: Vec<String>,
    pub subject: String,
    pub body: String,
    pub matched_rules: Vec<String>,
    pub actions_executed: Vec<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SesState {
    pub account_id: String,
    pub region: String,
    #[serde(default)]
    pub identities: BTreeMap<String, EmailIdentity>,
    #[serde(default)]
    pub configuration_sets: BTreeMap<String, ConfigurationSet>,
    #[serde(default)]
    pub templates: BTreeMap<String, EmailTemplate>,
    #[serde(default, skip_serializing)]
    pub sent_emails: Vec<SentEmail>,
    #[serde(default, skip_serializing)]
    pub bounces: Vec<SentBounce>,
    pub contact_lists: BTreeMap<String, ContactList>,
    pub contacts: BTreeMap<String, BTreeMap<String, Contact>>,
    /// Tags keyed by resource ARN, value is key→value tag map.
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Suppression list: email → suppressed destination info.
    pub suppressed_destinations: BTreeMap<String, SuppressedDestination>,
    /// Tenant-level suppression lists: tenant name -> email -> entry.
    /// Applies to sends made on behalf of that tenant (`TenantName`).
    #[serde(default)]
    pub tenant_suppressed_destinations: BTreeMap<String, BTreeMap<String, SuppressedDestination>>,
    /// Event destinations: config set name → list of event destinations.
    pub event_destinations: BTreeMap<String, Vec<EventDestination>>,
    /// Identity policies: identity name → policy name → policy JSON document.
    pub identity_policies: BTreeMap<String, BTreeMap<String, String>>,
    /// S/MIME certificate associations: identity name -> associations,
    /// kept sorted by from-address so pagination is stable.
    #[serde(default)]
    pub identity_certificates: BTreeMap<String, Vec<IdentityCertificate>>,
    /// Custom verification email templates: template name → template.
    pub custom_verification_email_templates: BTreeMap<String, CustomVerificationEmailTemplate>,
    /// Dedicated IP pools: pool name → pool.
    pub dedicated_ip_pools: BTreeMap<String, DedicatedIpPool>,
    /// Dedicated IPs: IP address → dedicated IP info.
    pub dedicated_ips: BTreeMap<String, DedicatedIp>,
    /// Multi-region endpoints: endpoint name → endpoint.
    pub multi_region_endpoints: BTreeMap<String, MultiRegionEndpoint>,
    /// Account-level settings (sending, suppression, VDM, details).
    pub account_settings: AccountSettings,
    /// Import jobs: job_id → ImportJob.
    pub import_jobs: BTreeMap<String, ImportJob>,
    /// Export jobs: job_id → ExportJob.
    pub export_jobs: BTreeMap<String, ExportJob>,
    /// Tenants: tenant_name → Tenant.
    pub tenants: BTreeMap<String, Tenant>,
    /// Tenant resource associations: tenant_name → Vec<resource_arn>.
    pub tenant_resource_associations: BTreeMap<String, Vec<TenantResourceAssociation>>,
    /// Reputation entities: "type/reference" → ReputationEntity.
    pub reputation_entities: BTreeMap<String, ReputationEntityState>,
    // ── SES v1 Receipt Rule state ──
    /// Receipt rule sets: name → rule set.
    pub receipt_rule_sets: BTreeMap<String, ReceiptRuleSet>,
    /// Which rule set is active (by name).
    pub active_receipt_rule_set: Option<String>,
    /// Receipt filters: name → filter.
    pub receipt_filters: BTreeMap<String, ReceiptFilter>,
    /// Inbound emails processed by the introspection endpoint.
    #[serde(default, skip_serializing)]
    pub inbound_emails: Vec<InboundEmail>,
    /// Emails accepted via the SMTP submission listener
    /// (`FAKECLOUD_SES_SMTP_PORT`).
    #[serde(default, skip_serializing)]
    pub smtp_submissions: Vec<SmtpSubmission>,
    /// Log of every event-destination dispatch performed by the SES
    /// fanout. Used by the
    /// `/_fakecloud/ses/event-destinations/deliveries` introspection
    /// endpoint to prove kinesis/firehose/cloudwatch wiring works without
    /// having to peek into the downstream services' state.
    #[serde(default, skip_serializing)]
    pub event_destination_dispatches: Vec<EventDestinationDispatch>,
    /// Deliverability dashboard subscription state.
    #[serde(default)]
    pub deliverability_dashboard: DeliverabilityDashboard,
    /// Deliverability test reports keyed by ReportId.
    #[serde(default)]
    pub deliverability_test_reports: BTreeMap<String, DeliverabilityTestReport>,
    /// VDM recommendations (read-only, lazily seeded once on first read).
    #[serde(default)]
    pub vdm_recommendations: Vec<VdmRecommendation>,
    /// Running count of recipients dropped because they were on the
    /// suppression list (gated by the effective `SuppressedReasons`
    /// filter). Surfaced through the introspection endpoint so tests can
    /// assert the gate fired.
    #[serde(default)]
    pub suppressed_drops_total: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeliverabilityDashboard {
    pub enabled: bool,
    pub subscribed_domains: Vec<SubscribedDomain>,
    pub subscription_expiry_date: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribedDomain {
    pub domain: String,
    pub subscription_start_date: DateTime<Utc>,
    pub inbox_placement_tracking_option_global: bool,
    pub inbox_placement_tracking_option_tracked_isps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliverabilityTestReport {
    pub report_id: String,
    pub report_name: String,
    pub subject: String,
    pub from_email: String,
    pub create_date: DateTime<Utc>,
    pub deliverability_test_status: String, // IN_PROGRESS | COMPLETED
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VdmRecommendation {
    pub resource_arn: String,
    pub recommendation_type: String,
    pub description: String,
    pub status: String,
    pub created_timestamp: DateTime<Utc>,
    pub last_updated_timestamp: DateTime<Utc>,
    pub impact: String,
}

/// Version 3: `SentEmail::raw_data` is always plain MIME (API raw sends
/// were stored as the caller's base64 before).
pub const SES_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Serialize, Deserialize)]
pub struct SesSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<SesState>>,
    #[serde(default)]
    pub state: Option<SesState>,
}

impl SesSnapshot {
    /// Bring an older snapshot's data up to the current schema.
    pub fn upgrade(mut self) -> Self {
        if self.schema_version < 3 {
            let states = self
                .accounts
                .iter_mut()
                .flat_map(|accounts| accounts.iter_mut().map(|(_, state)| state))
                .chain(self.state.iter_mut());
            for state in states {
                state
                    .sent_emails
                    .iter_mut()
                    .for_each(decode_legacy_raw_data);
            }
        }
        self.schema_version = SES_SNAPSHOT_SCHEMA_VERSION;
        self
    }
}

/// Before schema 3 an API raw send kept the caller's base64 in `raw_data`;
/// SMTP submissions (`smtp-` message IDs) were already plain MIME. Decode it
/// and fill in the bodies.
fn decode_legacy_raw_data(email: &mut SentEmail) {
    use base64::Engine;
    if email.message_id.starts_with("smtp-") {
        return;
    }
    let Some(bytes) = email
        .raw_data
        .as_deref()
        .and_then(|raw| base64::engine::general_purpose::STANDARD.decode(raw).ok())
    else {
        return;
    };
    let (text, html) = crate::mime::message_bodies(&bytes);
    email.text_body = email.text_body.take().or(text);
    email.html_body = email.html_body.take().or(html);
    email.raw_data = Some(String::from_utf8_lossy(&bytes).into_owned());
}

impl SesState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            identities: BTreeMap::new(),
            configuration_sets: BTreeMap::new(),
            templates: BTreeMap::new(),
            sent_emails: Vec::new(),
            bounces: Vec::new(),
            contact_lists: BTreeMap::new(),
            contacts: BTreeMap::new(),
            tags: BTreeMap::new(),
            suppressed_destinations: BTreeMap::new(),
            tenant_suppressed_destinations: BTreeMap::new(),
            event_destinations: BTreeMap::new(),
            identity_policies: BTreeMap::new(),
            identity_certificates: BTreeMap::new(),
            custom_verification_email_templates: BTreeMap::new(),
            dedicated_ip_pools: BTreeMap::new(),
            dedicated_ips: BTreeMap::new(),
            multi_region_endpoints: BTreeMap::new(),
            // production_access_enabled defaults to true: fakecloud is a
            // testing tool, not real AWS, and most users want to send to
            // arbitrary recipients without first jumping the sandbox-only
            // verified-recipient gate. Users who want to test sandbox
            // semantics can flip the flag back via the
            // /_fakecloud/ses/account/sandbox admin endpoint.
            account_settings: AccountSettings {
                sending_enabled: true,
                dedicated_ip_auto_warmup_enabled: false,
                suppressed_reasons: Vec::new(),
                vdm_attributes: None,
                details: None,
                production_access_enabled: true,
                pricing_plan: None,
            },
            import_jobs: BTreeMap::new(),
            export_jobs: BTreeMap::new(),
            tenants: BTreeMap::new(),
            tenant_resource_associations: BTreeMap::new(),
            reputation_entities: BTreeMap::new(),
            receipt_rule_sets: BTreeMap::new(),
            active_receipt_rule_set: None,
            receipt_filters: BTreeMap::new(),
            inbound_emails: Vec::new(),
            smtp_submissions: Vec::new(),
            event_destination_dispatches: Vec::new(),
            deliverability_dashboard: DeliverabilityDashboard::default(),
            deliverability_test_reports: BTreeMap::new(),
            vdm_recommendations: Vec::new(),
            suppressed_drops_total: 0,
        }
    }

    /// Reinitialize every field except ``account_id`` / ``region``.
    pub fn reset(&mut self) {
        let account_id = std::mem::take(&mut self.account_id);
        let region = std::mem::take(&mut self.region);
        *self = Self::new(&account_id, &region);
    }

    /// Effective `SuppressedReasons` for a send. Configuration-set scope
    /// wins when populated; otherwise we fall back to the account-level
    /// list. An empty list at both scopes is treated as "enforce both
    /// reasons" (BOUNCE + COMPLAINT) — that matches the historical
    /// fakecloud contract, and AWS callers who never call
    /// PutAccountSuppressionAttributes still expect the suppression list
    /// they explicitly populated to take effect.
    pub fn effective_suppressed_reasons(&self, config_set_name: Option<&str>) -> Vec<String> {
        if let Some(name) = config_set_name {
            if let Some(cs) = self.configuration_sets.get(name) {
                if !cs.suppressed_reasons.is_empty() {
                    return cs.suppressed_reasons.clone();
                }
            }
        }
        if !self.account_settings.suppressed_reasons.is_empty() {
            return self.account_settings.suppressed_reasons.clone();
        }
        vec!["BOUNCE".to_string(), "COMPLAINT".to_string()]
    }

    /// Like [`Self::suppressed_match`], but for a send made on behalf of a
    /// tenant (`tenant_name`) an entry on that tenant's suppression list
    /// blocks the address too. Tenant entries are subject to the same
    /// effective `SuppressedReasons` filter as account-level ones.
    pub fn suppressed_match_for_tenant(
        &self,
        address: &str,
        config_set_name: Option<&str>,
        tenant_name: Option<&str>,
    ) -> Option<&SuppressedDestination> {
        if let Some(hit) = self.suppressed_match(address, config_set_name) {
            return Some(hit);
        }
        let list = self.tenant_suppressed_destinations.get(tenant_name?)?;
        let key = address.trim();
        let (_, entry) = list
            .iter()
            .find(|(k, _)| k.trim().eq_ignore_ascii_case(key))?;
        let reasons = self.effective_suppressed_reasons(config_set_name);
        reasons.iter().any(|r| r == &entry.reason).then_some(entry)
    }

    /// Look up `address` against the suppression list (case-insensitive,
    /// trimmed). Returns the matching `SuppressedDestination` only when
    /// the stored reason is enforced under the effective filter for the
    /// supplied configuration-set scope.
    pub fn suppressed_match(
        &self,
        address: &str,
        config_set_name: Option<&str>,
    ) -> Option<&SuppressedDestination> {
        let key = address.trim().to_ascii_lowercase();
        let entry = self.suppressed_destinations.iter().find_map(|(k, v)| {
            if k.trim().eq_ignore_ascii_case(&key) {
                Some(v)
            } else {
                None
            }
        })?;
        let reasons = self.effective_suppressed_reasons(config_set_name);
        if reasons.iter().any(|r| r == &entry.reason) {
            Some(entry)
        } else {
            None
        }
    }
}

pub type SharedSesState = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<SesState>>>;

impl fakecloud_core::multi_account::AccountState for SesState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_type_names_convert_between_api_spellings() {
        for (v1, v2) in EVENT_TYPE_NAMES {
            assert_eq!(v1_event_type_name(v1), *v1);
            assert_eq!(v1_event_type_name(v2), *v1);
            assert_eq!(v2_event_type_name(v1), *v2);
            assert_eq!(v2_event_type_name(v2), *v2);
        }
        // Unknown values pass through untouched in both directions.
        assert_eq!(v1_event_type_name("SOME_NEW_TYPE"), "SOME_NEW_TYPE");
        assert_eq!(v2_event_type_name("someNewType"), "someNewType");
    }

    #[test]
    fn dimension_value_sources_convert_between_api_spellings() {
        for (v1, v2) in DIMENSION_VALUE_SOURCES {
            assert_eq!(v1_dimension_value_source(v2), *v1);
            assert_eq!(v2_dimension_value_source(v1), *v2);
            assert_eq!(v1_dimension_value_source(v1), *v1);
            assert_eq!(v2_dimension_value_source(v2), *v2);
        }
        assert_eq!(v2_dimension_value_source("custom"), "custom");
    }

    fn destination(firehose: serde_json::Value, sns: serde_json::Value) -> EventDestination {
        EventDestination {
            name: "d".to_string(),
            enabled: true,
            matching_event_types: Vec::new(),
            kinesis_firehose_destination: Some(firehose),
            cloud_watch_destination: None,
            sns_destination: Some(sns),
            event_bridge_destination: None,
            pinpoint_destination: None,
        }
    }

    #[test]
    fn destination_members_accept_both_spellings_and_skip_empty() {
        let d = destination(
            serde_json::json!({"IAMRoleARN": "", "DeliveryStreamARN": "arn:s"}),
            serde_json::json!({"TopicARN": "arn:t"}),
        );
        assert_eq!(d.firehose_arns(), Some((None, Some("arn:s"))));
        assert_eq!(d.sns_topic_arn(), Some("arn:t"));
        let d = destination(
            serde_json::json!({"IamRoleArn": "arn:r", "DeliveryStreamArn": null}),
            serde_json::json!({"TopicArn": ""}),
        );
        assert_eq!(d.firehose_arns(), Some((Some("arn:r"), None)));
        assert_eq!(d.sns_topic_arn(), None);
    }

    #[test]
    fn new_initializes_defaults() {
        let state = SesState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.identities.is_empty());
        assert!(state.configuration_sets.is_empty());
        assert!(state.account_settings.sending_enabled);
    }

    #[test]
    fn new_initializes_introspection_buffers_empty() {
        let state = SesState::new("123456789012", "us-east-1");
        assert!(state.smtp_submissions.is_empty());
        assert!(state.event_destination_dispatches.is_empty());
    }

    #[test]
    fn smtp_submission_round_trips_through_state() {
        let mut state = SesState::new("123456789012", "us-east-1");
        state.smtp_submissions.push(SmtpSubmission {
            message_id: "smtp-1".to_string(),
            from: "src@example.com".to_string(),
            to: vec!["dst@example.com".to_string()],
            subject: Some("hi".to_string()),
            raw_size_bytes: 42,
            received_at: Utc::now(),
            auth_user: "user".to_string(),
        });
        assert_eq!(state.smtp_submissions.len(), 1);
        assert_eq!(state.smtp_submissions[0].auth_user, "user");
        state.reset();
        assert!(state.smtp_submissions.is_empty());
    }

    #[test]
    fn event_destination_dispatch_round_trips() {
        let mut state = SesState::new("123456789012", "us-east-1");
        state
            .event_destination_dispatches
            .push(EventDestinationDispatch {
                destination_name: "fh".to_string(),
                destination_type: "firehose".to_string(),
                event_type: "SEND".to_string(),
                message_id: "msg-1".to_string(),
                dispatched_at: Utc::now(),
                target_arn: "arn:aws:firehose:us-east-1:123456789012:deliverystream/ds1"
                    .to_string(),
            });
        assert_eq!(state.event_destination_dispatches.len(), 1);
        assert_eq!(
            state.event_destination_dispatches[0].destination_type,
            "firehose"
        );
    }

    #[test]
    fn identity_certificate_round_trips_through_state() {
        let mut state = SesState::new("123456789012", "us-east-1");
        state.identity_certificates.insert(
            "smime@example.com".to_string(),
            vec![IdentityCertificate {
                from_address: "smime@example.com".to_string(),
                status: "PROVISIONING".to_string(),
                certificate_arn: "arn:aws:acm:us-east-1:123456789012:certificate/abc-123"
                    .to_string(),
                associated_at: Utc::now(),
            }],
        );
        let encoded = serde_json::to_string(&state).unwrap();
        let decoded: SesState = serde_json::from_str(&encoded).unwrap();
        let certs = decoded
            .identity_certificates
            .get("smime@example.com")
            .unwrap();
        assert_eq!(certs[0].status, "PROVISIONING");
        state.reset();
        assert!(state.identity_certificates.is_empty());
    }

    #[test]
    fn reset_preserves_account_region() {
        let mut state = SesState::new("123456789012", "eu-west-1");
        state.account_settings.sending_enabled = false;
        state.reset();
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "eu-west-1");
        assert!(state.account_settings.sending_enabled);
    }

    #[test]
    fn snapshot_upgrade_decodes_legacy_base64_raw_data() {
        use base64::Engine;
        let mime = "Subject: Hi\r\nContent-Type: text/html\r\n\r\n<p>hi</p>";
        let email = |raw: &str| SentEmail {
            message_id: "m".to_string(),
            from: "a@example.com".to_string(),
            to: vec!["b@example.com".to_string()],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: Some("Hi".to_string()),
            html_body: None,
            text_body: None,
            raw_data: Some(raw.to_string()),
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let mut state = SesState::new("123456789012", "us-east-1");
        // An API raw send (base64) and SMTP submissions (already plain, even
        // when the text happens to be valid base64).
        state.sent_emails.push(email(
            &base64::engine::general_purpose::STANDARD.encode(mime),
        ));
        state.sent_emails.push(SentEmail {
            message_id: "smtp-1".to_string(),
            ..email(mime)
        });
        state.sent_emails.push(SentEmail {
            message_id: "smtp-2".to_string(),
            ..email("QUJD")
        });
        let snapshot = SesSnapshot {
            schema_version: 2,
            accounts: None,
            state: Some(state),
        }
        .upgrade();
        assert_eq!(snapshot.schema_version, SES_SNAPSHOT_SCHEMA_VERSION);
        let sent = snapshot.state.unwrap().sent_emails;
        assert_eq!(sent[0].raw_data.as_deref(), Some(mime));
        assert_eq!(sent[0].html_body.as_deref(), Some("<p>hi</p>"));
        assert_eq!(sent[1].raw_data.as_deref(), Some(mime));
        assert_eq!(sent[2].raw_data.as_deref(), Some("QUJD"));

        // A current snapshot is left as-is, even when a body looks like base64.
        let mut state = SesState::new("123456789012", "us-east-1");
        state.sent_emails.push(email("QUJD"));
        let snapshot = SesSnapshot {
            schema_version: SES_SNAPSHOT_SCHEMA_VERSION,
            accounts: None,
            state: Some(state),
        }
        .upgrade();
        assert_eq!(
            snapshot.state.unwrap().sent_emails[0].raw_data.as_deref(),
            Some("QUJD")
        );
    }
}
