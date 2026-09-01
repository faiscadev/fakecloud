use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackResource {
    pub logical_id: String,
    pub physical_id: String,
    pub resource_type: String,
    pub status: String,
    /// For custom resources, the Lambda ARN (ServiceToken) used for invocation.
    pub service_token: Option<String>,
    /// Per-resource attributes resolvable via `Fn::GetAtt`. Populated at
    /// provisioning time by each resource type's create handler.
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
    /// The resource's `DeletionPolicy` (`Retain`/`Snapshot`/`Delete`/
    /// `RetainExceptOnCreate`), captured from the template at provision time so
    /// `DeleteStack` and the update-remove path can honor Retain/Snapshot
    /// instead of unconditionally destroying the physical resource. `None` =
    /// the CFN default (`Delete`).
    #[serde(default)]
    pub deletion_policy: Option<String>,
    /// The resource's `UpdateReplacePolicy`, honored when an update replaces
    /// this resource. `None` = default (`Delete`).
    #[serde(default)]
    pub update_replace_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackOutput {
    pub key: String,
    pub value: String,
    pub description: Option<String>,
    pub export_name: Option<String>,
}

/// Cross-stack export entry, keyed by `Export.Name` in `state.exports`.
/// Tracks the resolved value plus the stack that owns it so `ListExports`
/// can return a stable `ExportingStackId` and `DeleteStack` can attribute
/// the export back to its source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackExport {
    pub value: String,
    pub exporting_stack_id: String,
    pub exporting_stack_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stack {
    pub name: String,
    pub stack_id: String,
    pub template: String,
    pub status: String,
    /// Why the stack is in `status`, when the status is a failure. Surfaced as
    /// `StackStatusReason` on DescribeStacks/ListStacks; `None` for a healthy
    /// stack. Without it a CREATE_FAILED stack gives the caller no clue what
    /// went wrong.
    #[serde(default)]
    pub status_reason: Option<String>,
    pub resources: Vec<StackResource>,
    pub parameters: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub description: Option<String>,
    pub notification_arns: Vec<String>,
    #[serde(default)]
    pub outputs: Vec<StackOutput>,
    /// Whether `TerminationProtection` is enabled for this stack. Set from the
    /// `EnableTerminationProtection` parameter on `CreateStack` and toggled by
    /// `UpdateTerminationProtection`. `DeleteStack` refuses a protected stack,
    /// and `DescribeStacks` reports the flag. The single source of truth (the
    /// previous write-only `termination_protection` map is gone).
    #[serde(default)]
    pub enable_termination_protection: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudFormationState {
    pub account_id: String,
    pub region: String,
    #[serde(default)]
    pub stacks: BTreeMap<String, Stack>,
    /// Generic stores keyed by `category` (change_sets, stack_sets, types,
    /// generated_templates, resource_scans, refactors, etc.) so the
    /// extras handlers can keep state alive without proliferating
    /// per-category fields.
    #[serde(default)]
    pub extras: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    #[serde(default)]
    pub events: BTreeMap<String, Vec<serde_json::Value>>,
    #[serde(default)]
    pub stack_policies: BTreeMap<String, String>,
    #[serde(default)]
    pub orgs_access_enabled: bool,
    /// Account-wide exports keyed by `Export.Name`. Populated whenever a
    /// stack's outputs include `Export.Name` and removed on stack delete.
    #[serde(default)]
    pub exports: BTreeMap<String, StackExport>,
    /// Reverse-ref map: `imports[export_name]` lists the stack names that
    /// have consumed the export via `Fn::ImportValue`. CloudFormation
    /// blocks deleting a stack whose exports still appear here.
    #[serde(default)]
    pub imports: BTreeMap<String, Vec<String>>,
}

impl CloudFormationState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            stacks: BTreeMap::new(),
            extras: BTreeMap::new(),
            events: BTreeMap::new(),
            stack_policies: BTreeMap::new(),
            orgs_access_enabled: false,
            exports: BTreeMap::new(),
            imports: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.stacks.clear();
        self.extras.clear();
        self.events.clear();
        self.stack_policies.clear();
        self.orgs_access_enabled = false;
        self.exports.clear();
        self.imports.clear();
    }
}

pub type SharedCloudFormationState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<CloudFormationState>>>;

impl fakecloud_core::multi_account::AccountState for CloudFormationState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

pub const CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Serialize, Deserialize)]
pub struct CloudFormationSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<CloudFormationState>>,
    #[serde(default)]
    pub state: Option<CloudFormationState>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes_empty() {
        let state = CloudFormationState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.stacks.is_empty());
    }

    #[test]
    fn reset_clears_stacks() {
        let mut state = CloudFormationState::new("123456789012", "us-east-1");
        state.stacks.insert(
            "s1".to_string(),
            Stack {
                name: "s1".to_string(),
                stack_id: "id".to_string(),
                template: "{}".to_string(),
                status: "CREATE_COMPLETE".to_string(),
                status_reason: None,
                resources: vec![],
                parameters: BTreeMap::new(),
                tags: BTreeMap::new(),
                created_at: Utc::now(),
                updated_at: None,
                description: None,
                notification_arns: vec![],
                outputs: vec![],
                enable_termination_protection: false,
            },
        );
        state.reset();
        assert!(state.stacks.is_empty());
    }
}
