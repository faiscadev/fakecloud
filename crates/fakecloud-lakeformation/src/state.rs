//! Account-partitioned, serializable state for AWS Lake Formation.
//!
//! Models the Lake Formation governance control plane over Glue: LF-tags and
//! LF-tag expressions, fine-grained permission grants, registered data-lake
//! resources, data-lake settings, data-cell filters, opt-ins, governed-table
//! transactions and their object lists, Identity Center configuration, storage
//! optimizers, resource-attached LF-tags, and synthetic query-planning state.
//! There is no backing Glue catalogue or query engine, so credential vending
//! and query planning return coherent synthetic values while everything else is
//! real, persisted CRUD.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const LAKEFORMATION_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// An LF-tag: a key with an ordered set of allowed values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LFTagRecord {
    pub catalog_id: String,
    pub tag_key: String,
    pub tag_values: Vec<String>,
}

/// A named LF-tag expression (a list of `{TagKey, TagValues}` pairs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LFTagExpressionRecord {
    pub catalog_id: String,
    pub name: String,
    pub description: Option<String>,
    /// The `Expression` (list of LFTag pairs), stored verbatim.
    pub expression: Value,
}

/// A fine-grained permission grant record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantRecord {
    pub catalog_id: String,
    pub principal: Value,
    pub resource: Value,
    pub permissions: Vec<String>,
    pub permissions_with_grant_option: Vec<String>,
    pub condition: Option<Value>,
    pub last_updated: DateTime<Utc>,
}

/// A registered data-lake resource (an S3 location managed by Lake Formation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceInfoRecord {
    pub resource_arn: String,
    pub role_arn: Option<String>,
    pub last_modified: DateTime<Utc>,
    pub with_federation: Option<bool>,
    pub hybrid_access_enabled: Option<bool>,
    pub with_privileged_access: Option<bool>,
    pub expected_resource_owner_account: Option<String>,
}

/// A governed-table transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRecord {
    pub transaction_id: String,
    pub transaction_type: String,
    pub status: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
}

/// A data-lake opt-in (a principal opted in on a resource).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptInRecord {
    pub principal: Value,
    pub resource: Value,
    pub condition: Option<Value>,
    pub last_modified: DateTime<Utc>,
}

/// A Lake Formation Identity Center configuration (one per catalog).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityCenterRecord {
    pub catalog_id: String,
    pub instance_arn: Option<String>,
    pub application_arn: String,
    pub application_status: String,
    pub external_filtering: Option<Value>,
    pub share_recipients: Option<Value>,
    pub service_integrations: Option<Value>,
    pub resource_share: Option<String>,
}

/// A synthetic query-planning session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRecord {
    pub query_id: String,
    pub state: String,
    pub submission_time: DateTime<Utc>,
}

/// The account-scoped Lake Formation state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LakeFormationState {
    /// LF-tags keyed by `catalog\u{1}tagkey`.
    #[serde(default)]
    pub lf_tags: BTreeMap<String, LFTagRecord>,
    /// LF-tag expressions keyed by `catalog\u{1}name`.
    #[serde(default)]
    pub lf_tag_expressions: BTreeMap<String, LFTagExpressionRecord>,
    #[serde(default)]
    pub grants: Vec<GrantRecord>,
    /// Registered resources keyed by resource ARN.
    #[serde(default)]
    pub resources: BTreeMap<String, ResourceInfoRecord>,
    /// Data-lake settings keyed by catalog id (stored verbatim).
    #[serde(default)]
    pub data_lake_settings: BTreeMap<String, Value>,
    /// Data-cell filters keyed by `catalog\u{1}db\u{1}table\u{1}name` (stored verbatim).
    #[serde(default)]
    pub data_cells_filters: BTreeMap<String, Value>,
    #[serde(default)]
    pub opt_ins: Vec<OptInRecord>,
    /// Transactions keyed by transaction id.
    #[serde(default)]
    pub transactions: BTreeMap<String, TransactionRecord>,
    /// Identity Center config keyed by catalog id.
    #[serde(default)]
    pub identity_center: BTreeMap<String, IdentityCenterRecord>,
    /// Resource-attached LF-tags keyed by a resource signature; value is the
    /// attached `LFTagPair` list (verbatim JSON values).
    #[serde(default)]
    pub resource_lf_tags: BTreeMap<String, Vec<Value>>,
    /// Storage-optimizer config keyed by `catalog\u{1}db\u{1}table`; value is a
    /// map of optimizer type -> config map.
    #[serde(default)]
    pub storage_optimizers: BTreeMap<String, Value>,
    /// Query-planning sessions keyed by query id.
    #[serde(default)]
    pub queries: BTreeMap<String, QueryRecord>,
    /// Governed-table object lists keyed by `catalog\u{1}db\u{1}table`.
    #[serde(default)]
    pub table_objects: BTreeMap<String, Vec<Value>>,
    /// Row-level security policies keyed by name (unique within the
    /// account's Data Catalog).
    #[serde(default)]
    pub rls_policies: BTreeMap<String, RLSPolicyRecord>,
    /// Row-level security policy attachments, in attach order.
    #[serde(default)]
    pub rls_policy_attachments: Vec<RLSPolicyAttachmentRecord>,
}

/// A row-level security policy (CreateRLSPolicy).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RLSPolicyRecord {
    pub name: String,
    /// The `AttachmentColumns` list of `Column` structures, stored verbatim.
    pub attachment_columns: Vec<Value>,
    pub filter_expression: String,
}

/// The attachment of an RLS policy to a table for a principal (AttachRLSPolicy).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RLSPolicyAttachmentRecord {
    pub policy_name: String,
    pub resource: Value,
    pub principal: Option<Value>,
    pub condition: Option<Value>,
    /// Whether this attachment is the resource's default RLS policy.
    pub default: bool,
}

impl LakeFormationState {
    /// The default row-level security policy attached to a Glue table (the
    /// attachment made with `SetDefault`), read by Glue's
    /// `Table.RLSProperties`. A resource's catalog defaults to `account_id`.
    pub fn default_rls_policy(
        &self,
        account_id: &str,
        catalog_id: &str,
        database: &str,
        table: &str,
    ) -> Option<String> {
        self.rls_policy_attachments
            .iter()
            .filter(|a| a.default)
            .find(|a| {
                let Some(t) = a
                    .resource
                    .get("Table")
                    .or_else(|| a.resource.get("TableWithColumns"))
                else {
                    return false;
                };
                let s = |k: &str| t.get(k).and_then(Value::as_str);
                s("CatalogId").unwrap_or(account_id) == catalog_id
                    && s("DatabaseName") == Some(database)
                    && s("Name") == Some(table)
            })
            .map(|a| a.policy_name.clone())
    }

    /// Whether an `s3://bucket/prefix` location is covered by a registered
    /// data-lake resource (`arn:<partition>:s3:::bucket[/prefix]`).
    pub fn location_registered(&self, location: &str) -> bool {
        let Some(path) = location.strip_prefix("s3://") else {
            return false;
        };
        let path = path.trim_end_matches('/');
        self.resources.keys().any(|arn| {
            let Some((_, registered)) = arn.split_once(":s3:::") else {
                return false;
            };
            let registered = registered.trim_end_matches('/');
            path == registered || path.starts_with(&format!("{registered}/"))
        })
    }
}

impl AccountState for LakeFormationState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedLakeFormationState = Arc<RwLock<MultiAccountState<LakeFormationState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct LakeFormationSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<LakeFormationState>,
}

/// Composite key joiner using a control char that cannot appear in identifiers.
pub fn ckey(parts: &[&str]) -> String {
    parts.join("\u{1}")
}
