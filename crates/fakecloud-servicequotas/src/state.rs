//! Account-partitioned, serializable Service Quotas state.
//!
//! Quota *definitions* (codes, names, AWS defaults) live in
//! [`crate::catalog`]; this state holds only what an account changed: applied
//! values raised by approved increase requests, the request history, the
//! Organizations quota request template, tags on applied quotas, automatic
//! management settings, quota utilization reports and per-quota enforcement
//! overrides.
//!
//! Map keys are plain strings (never tuples) so the snapshot round-trips
//! through JSON.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

/// Version 2: a quota's applied value, tags and requests are keyed by the
/// quota's scope (global or regional) as the AWS-dumped catalog has it.
/// Version 1 snapshots are migrated on load, see
/// [`crate::persistence::migrate_quota_scopes`].
pub const SERVICEQUOTAS_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

/// A quota increase request (`RequestedServiceQuotaChange`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaRequest {
    pub id: String,
    /// The region the request was made in. Empty for a global quota.
    pub region: String,
    pub service_code: String,
    pub quota_code: String,
    pub desired_value: f64,
    pub status: String,
    #[serde(default)]
    pub case_id: Option<String>,
    pub created: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
    /// JSON `{"accountId": ..., "callerArn": ...}`, as AWS reports it.
    pub requester: String,
    pub quota_arn: String,
}

/// One entry of the Organizations quota request template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateEntry {
    pub service_code: String,
    pub quota_code: String,
    pub aws_region: String,
    pub desired_value: f64,
}

/// Automatic quota management settings for one region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoManagement {
    pub opt_in_level: String,
    pub opt_in_type: String,
    #[serde(default)]
    pub notification_arn: Option<String>,
    /// Excluded quota codes keyed by service code.
    #[serde(default)]
    pub exclusion_list: BTreeMap<String, Vec<String>>,
    /// `ENABLED` or `DISABLED`.
    pub status: String,
}

/// One quota line of a utilization report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UtilizationEntry {
    pub service_code: String,
    pub quota_code: String,
    pub usage: f64,
    pub applied_value: f64,
}

/// A quota utilization report, computed when it is started.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UtilizationReport {
    pub report_id: String,
    pub region: String,
    pub generated_at: DateTime<Utc>,
    pub quotas: Vec<UtilizationEntry>,
}

/// Per-account Service Quotas state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceQuotasData {
    /// Applied values that differ from the AWS default, keyed by
    /// [`applied_key`].
    #[serde(default)]
    pub applied: BTreeMap<String, f64>,
    /// Quota increase requests keyed by request id.
    #[serde(default)]
    pub requests: BTreeMap<String, QuotaRequest>,
    /// The quota request template (management account only), keyed by
    /// [`template_key`].
    #[serde(default)]
    pub template: BTreeMap<String, TemplateEntry>,
    /// When the template was associated with the organization; `None` while
    /// it is not associated.
    #[serde(default)]
    pub template_associated_at: Option<DateTime<Utc>>,
    /// Whether the template was ever associated (distinguishes
    /// `DISASSOCIATED` from never used).
    #[serde(default)]
    pub template_ever_associated: bool,
    /// The template association this (member) account was last checked
    /// against, so a template is applied to a new account exactly once.
    #[serde(default)]
    pub template_checked: Option<String>,
    /// Tags on applied quotas, keyed by quota ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Automatic management settings keyed by region.
    #[serde(default)]
    pub auto_management: BTreeMap<String, AutoManagement>,
    /// Utilization reports keyed by report id.
    #[serde(default)]
    pub reports: BTreeMap<String, UtilizationReport>,
    /// Per-quota enforcement overrides for this account, keyed by
    /// [`crate::settings::quota_ref`]: `true` enforces, `false` ignores. They
    /// take precedence over the server-wide settings.
    #[serde(default)]
    pub enforcement: BTreeMap<String, bool>,
}

impl AccountState for ServiceQuotasData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

/// Key of an applied value. Global quotas apply in every region, so their key
/// carries no region.
pub fn applied_key(region: &str, global: bool, service_code: &str, quota_code: &str) -> String {
    let mut key = String::new();
    write_applied_key(&mut key, region, global, service_code, quota_code);
    key
}

/// [`applied_key`] written over `buf`, so repeated lookups reuse one buffer.
pub(crate) fn write_applied_key(
    buf: &mut String,
    region: &str,
    global: bool,
    service_code: &str,
    quota_code: &str,
) {
    let region = if global { "" } else { region };
    buf.clear();
    buf.extend([region, "|", service_code, "|", quota_code]);
}

pub fn template_key(service_code: &str, quota_code: &str, aws_region: &str) -> String {
    format!("{service_code}|{quota_code}|{aws_region}")
}

pub type SharedServiceQuotasState = Arc<RwLock<MultiAccountState<ServiceQuotasData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceQuotasSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<ServiceQuotasData>,
}
