use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

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
    /// The undo log of an operation waiting on a later call: after
    /// `DisableRollback` held the stack at `UPDATE_FAILED`, after
    /// `UPDATE_ROLLBACK_FAILED`, or on a nested stack whose parent has not
    /// finished. Normally absent. (An older snapshot may instead carry a
    /// `reapply_logical_ids` field; updates roll back for real now, so that field
    /// has no successor and is ignored on load.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_update: Option<crate::stack_update::UpdateJournal>,
    /// `CancelUpdateStack` asked the running update to stop. Read at the next
    /// resource boundary; only true while an update runs.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cancel_requested: bool,
}

/// One account's CloudFormation state in one region.
///
/// CloudFormation is a regional service: a stack, its change sets, events and
/// policy, the exports it publishes, the stack sets administered from a region
/// and the registry types activated there all live in the region they were
/// created in. The same stack name can exist independently in two regions, and
/// a request only ever sees the state of the region it is sent to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudFormationState {
    pub account_id: String,
    pub region: String,
    #[serde(default)]
    pub stacks: BTreeMap<String, Stack>,
    /// Generic stores keyed by `category` (change_sets, types,
    /// generated_templates, resource_scans, refactors, etc.) so the
    /// extras handlers can keep state alive without proliferating
    /// per-category fields.
    #[serde(default)]
    pub extras: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    #[serde(default)]
    pub events: BTreeMap<String, Vec<serde_json::Value>>,
    #[serde(default)]
    pub stack_policies: BTreeMap<String, String>,
    /// Exports of this account and region keyed by `Export.Name`. Populated
    /// whenever a stack's outputs include `Export.Name` and removed on stack
    /// delete. Export names are unique per account and region, and
    /// `Fn::ImportValue` only resolves exports of the importing stack's
    /// region.
    #[serde(default)]
    pub exports: BTreeMap<String, StackExport>,
    /// Reverse-ref map: `imports[export_name]` lists the stack names that
    /// have consumed the export via `Fn::ImportValue`. CloudFormation
    /// blocks deleting a stack whose exports still appear here.
    #[serde(default)]
    pub imports: BTreeMap<String, Vec<String>>,
    /// Stack sets administered from this account and region, keyed by
    /// `StackSetId`. Deleted stack sets stay here with status `DELETED`.
    #[serde(default)]
    pub stack_sets: BTreeMap<String, crate::stack_sets::StackSet>,
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
            exports: BTreeMap::new(),
            imports: BTreeMap::new(),
            stack_sets: BTreeMap::new(),
        }
    }

    /// The live stack a `StackName` parameter addresses: a stack id (ARN)
    /// matches the stack with that id, anything else the stack of that name.
    /// The one lookup every handler uses, under its own lock, so a stack
    /// deleted and re-created under the same name is never reached through
    /// the old stack's id. The dispatcher has already refused ids of other
    /// regions and accounts (`service::resolve_stack_ref`).
    pub fn live_stack(&self, stack_ref: &str) -> Option<&Stack> {
        let found = if stack_ref.starts_with("arn:") {
            self.stacks.values().find(|s| s.stack_id == stack_ref)
        } else {
            self.stacks.get(stack_ref)
        };
        found.filter(|s| s.status != "DELETE_COMPLETE")
    }

    /// Mutable [`Self::live_stack`].
    pub fn live_stack_mut(&mut self, stack_ref: &str) -> Option<&mut Stack> {
        let found = if stack_ref.starts_with("arn:") {
            self.stacks.values_mut().find(|s| s.stack_id == stack_ref)
        } else {
            self.stacks.get_mut(stack_ref)
        };
        found.filter(|s| s.status != "DELETE_COMPLETE")
    }

    pub fn reset(&mut self) {
        self.stacks.clear();
        self.extras.clear();
        self.events.clear();
        self.stack_policies.clear();
        self.exports.clear();
        self.imports.clear();
        self.stack_sets.clear();
    }
}

/// Everything one account holds in CloudFormation, partitioned by region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudFormationAccountState {
    pub account_id: String,
    /// The server's configured region.
    pub default_region: String,
    /// Per-region state, created the first time a request targets the region.
    #[serde(default)]
    pub regions: BTreeMap<String, CloudFormationState>,
    /// Trusted access between StackSets and Organizations. It is enabled on
    /// the organization (the StackSets service principal), not per region.
    #[serde(default)]
    pub orgs_access_enabled: bool,
}

impl CloudFormationAccountState {
    pub fn new(account_id: &str, default_region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            default_region: default_region.to_string(),
            regions: BTreeMap::new(),
            orgs_access_enabled: false,
        }
    }

    /// The account's state in `region`, `None` when nothing has touched it.
    pub fn region(&self, region: &str) -> Option<&CloudFormationState> {
        self.regions.get(region)
    }

    /// The account's state in `region`, created empty on first use.
    pub fn region_mut(&mut self, region: &str) -> &mut CloudFormationState {
        let account_id = &self.account_id;
        self.regions
            .entry(region.to_string())
            .or_insert_with(|| CloudFormationState::new(account_id, region))
    }

    /// Every stack the account holds, in every region.
    pub fn all_stacks(&self) -> impl Iterator<Item = &Stack> {
        self.regions.values().flat_map(|r| r.stacks.values())
    }

    /// A stack set this account administers, by `StackSetId`, in whichever
    /// region it was created. Stack set ids carry a UUID, so one names a
    /// single stack set across all regions. Request handlers resolve a
    /// caller's name or id in the request region first (see
    /// `stack_sets::find_active`); this is for following an already-resolved
    /// id.
    pub fn stack_set(&self, set_id: &str) -> Option<&crate::stack_sets::StackSet> {
        self.regions.values().find_map(|r| r.stack_sets.get(set_id))
    }

    /// Mutable [`Self::stack_set`].
    pub fn stack_set_mut(&mut self, set_id: &str) -> Option<&mut crate::stack_sets::StackSet> {
        self.regions
            .values_mut()
            .find_map(|r| r.stack_sets.get_mut(set_id))
    }

    pub fn reset(&mut self) {
        self.regions.clear();
        self.orgs_access_enabled = false;
    }
}

pub type SharedCloudFormationState = Arc<RwLock<MultiAccountState<CloudFormationAccountState>>>;

impl AccountState for CloudFormationAccountState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

/// (account, region) addressing over the account-partitioned container.
pub trait RegionalAccounts {
    /// The state of `account_id` in `region`, `None` when neither has been
    /// touched.
    fn regional(&self, account_id: &str, region: &str) -> Option<&CloudFormationState>;
    /// The state of `account_id` in `region`, creating both on first use.
    fn regional_mut(&mut self, account_id: &str, region: &str) -> &mut CloudFormationState;
    /// The state of `account_id` in `region` without creating either.
    fn regional_get_mut(
        &mut self,
        account_id: &str,
        region: &str,
    ) -> Option<&mut CloudFormationState>;
}

impl RegionalAccounts for MultiAccountState<CloudFormationAccountState> {
    fn regional(&self, account_id: &str, region: &str) -> Option<&CloudFormationState> {
        self.get(account_id).and_then(|a| a.region(region))
    }

    fn regional_mut(&mut self, account_id: &str, region: &str) -> &mut CloudFormationState {
        self.get_or_create(account_id).region_mut(region)
    }

    fn regional_get_mut(
        &mut self,
        account_id: &str,
        region: &str,
    ) -> Option<&mut CloudFormationState> {
        self.get_mut(account_id)
            .and_then(|a| a.regions.get_mut(region))
    }
}

/// v3: state partitioned by (account, region). v2 kept one account-wide state
/// per account, every stack in one map keyed by name; v1 a single account's.
pub const CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Serialize, Deserialize)]
pub struct CloudFormationSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<MultiAccountState<CloudFormationAccountState>>,
    /// Only set when a v1 (single-account) snapshot is migrated: that one
    /// account's state, for the caller to merge into its own container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<CloudFormationAccountState>,
}

/// The account-wide state v1 and v2 snapshots stored: the fields that are now
/// regional, plus the organizations-access flag.
#[derive(Debug, Deserialize)]
struct LegacyCloudFormationState {
    #[serde(flatten)]
    state: CloudFormationState,
    #[serde(default)]
    orgs_access_enabled: bool,
}

impl AccountState for LegacyCloudFormationState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self {
            state: CloudFormationState::new(account_id, region),
            orgs_access_enabled: false,
        }
    }
}

#[derive(Debug, Deserialize)]
struct LegacyCloudFormationSnapshot {
    #[serde(default)]
    accounts: Option<MultiAccountState<LegacyCloudFormationState>>,
    #[serde(default)]
    state: Option<LegacyCloudFormationState>,
}

#[derive(Deserialize)]
struct SnapshotVersion {
    schema_version: u32,
}

/// Parse a persisted CloudFormation snapshot, migrating older schemas to the
/// current one. A snapshot newer than this build comes back with its on-disk
/// `schema_version` and no state, for the caller to refuse.
pub fn parse_cloudformation_snapshot(
    bytes: &[u8],
) -> Result<CloudFormationSnapshot, serde_json::Error> {
    let SnapshotVersion { schema_version } = serde_json::from_slice(bytes)?;
    if schema_version > CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION {
        return Ok(CloudFormationSnapshot {
            schema_version,
            accounts: None,
            state: None,
        });
    }
    if schema_version == CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION {
        return serde_json::from_slice(bytes);
    }
    let legacy: LegacyCloudFormationSnapshot = serde_json::from_slice(bytes)?;
    Ok(CloudFormationSnapshot {
        schema_version: CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION,
        accounts: legacy
            .accounts
            .map(|accounts| accounts.map(split_into_regions)),
        state: legacy.state.as_ref().map(split_into_regions),
    })
}

/// The region a CloudFormation ARN (stack, change set, stack set, type, ...)
/// was minted in.
fn cloudformation_arn_region(arn: &str) -> Option<String> {
    fakecloud_aws::arn::arn_resource(arn, "cloudformation")?;
    fakecloud_aws::arn::region_of(arn).map(str::to_string)
}

/// Fields an `extras` record names its owner by, most specific first: the
/// stack a record belongs to decides its region before the record's own id.
const EXTRAS_ARN_FIELDS: &[&str] = &[
    "StackId",
    "ChangeSetId",
    "Id",
    "StackSetARN",
    "Arn",
    "TypeArn",
    "GeneratedTemplateId",
    "ResourceScanId",
];

/// The region a generic `extras` record belongs to. A record is attributed to
/// a single region: the one its first CloudFormation ARN names, taken in the
/// stable order of [`EXTRAS_ARN_FIELDS`] and then any other field by name, so
/// the result never depends on how the record's keys happen to be ordered.
fn extras_record_region(record: &serde_json::Value) -> Option<String> {
    let fields = record.as_object()?;
    let field_region = |key: &str| {
        fields
            .get(key)
            .and_then(serde_json::Value::as_str)
            .and_then(cloudformation_arn_region)
    };
    EXTRAS_ARN_FIELDS
        .iter()
        .find_map(|key| field_region(key))
        .or_else(|| {
            let mut keys: Vec<&String> = fields.keys().collect();
            keys.sort();
            keys.into_iter().find_map(|key| field_region(key))
        })
}

/// Split one account's pre-regional state into per-region states. Every record
/// goes to the region its own ARN names (a stack's id, an export's exporting
/// stack, a stack set's ARN, a change set's id); records that carry no ARN
/// follow the stack they belong to, and anything left goes to the region the
/// state was created under, the server's configured region.
fn split_into_regions(legacy: &LegacyCloudFormationState) -> CloudFormationAccountState {
    let state = &legacy.state;
    let default_region = state.region.clone();
    let mut account = CloudFormationAccountState::new(&state.account_id, &default_region);
    account.orgs_access_enabled = legacy.orgs_access_enabled;

    // Stack name -> region, for the records keyed by stack name.
    let mut stack_regions: BTreeMap<&str, String> = BTreeMap::new();
    for (name, stack) in &state.stacks {
        let region =
            cloudformation_arn_region(&stack.stack_id).unwrap_or_else(|| default_region.clone());
        stack_regions.insert(name.as_str(), region.clone());
        account
            .region_mut(&region)
            .stacks
            .insert(name.clone(), stack.clone());
    }
    let region_of_stack_ref = |key: &str| -> String {
        cloudformation_arn_region(key)
            .or_else(|| stack_regions.get(key).cloned())
            .unwrap_or_else(|| default_region.clone())
    };

    for (key, events) in &state.events {
        account
            .region_mut(&region_of_stack_ref(key))
            .events
            .insert(key.clone(), events.clone());
    }
    // Policies are keyed by the stack's name. Older builds stored one under
    // whatever `StackName` the caller passed (name or id) and never dropped it
    // on delete, so: a deleted stack's policies are discarded (a later stack
    // of that name starts without one), an id-keyed policy moves to its
    // stack's name, and where a stack has both, the name-keyed one (what
    // `GetStackPolicy` by name returned) wins.
    let mut policies: Vec<(String, &String, bool)> = Vec::new();
    for (key, policy) in &state.stack_policies {
        let by_id = state.stacks.values().find(|s| &s.stack_id == key);
        let stack = by_id.or_else(|| state.stacks.get(key));
        if stack.is_some_and(|s| s.status == "DELETE_COMPLETE") {
            continue;
        }
        let name = by_id.map_or_else(|| key.clone(), |s| s.name.clone());
        policies.push((name, policy, by_id.is_some()));
    }
    // Name-keyed entries first, so an id-keyed one never replaces them.
    policies.sort_by_key(|(_, _, by_id)| *by_id);
    for (name, policy, _) in policies {
        account
            .region_mut(&region_of_stack_ref(&name))
            .stack_policies
            .entry(name)
            .or_insert_with(|| policy.clone());
    }
    let mut export_regions: BTreeMap<&str, String> = BTreeMap::new();
    for (name, export) in &state.exports {
        let region = cloudformation_arn_region(&export.exporting_stack_id)
            .unwrap_or_else(|| region_of_stack_ref(&export.exporting_stack_name));
        export_regions.insert(name.as_str(), region.clone());
        account
            .region_mut(&region)
            .exports
            .insert(name.clone(), export.clone());
    }
    for (name, consumers) in &state.imports {
        let region = export_regions
            .get(name.as_str())
            .cloned()
            .unwrap_or_else(|| {
                consumers
                    .first()
                    .map(|c| region_of_stack_ref(c))
                    .unwrap_or_else(|| default_region.clone())
            });
        account
            .region_mut(&region)
            .imports
            .insert(name.clone(), consumers.clone());
    }
    for (id, set) in &state.stack_sets {
        let region = cloudformation_arn_region(&set.arn).unwrap_or_else(|| default_region.clone());
        account
            .region_mut(&region)
            .stack_sets
            .insert(id.clone(), set.clone());
    }
    for (category, records) in &state.extras {
        for (id, record) in records {
            // A record without an ARN follows the stack it names.
            let region = extras_record_region(record)
                .or_else(|| {
                    ["StackName", "StackId"].into_iter().find_map(|field| {
                        record
                            .get(field)
                            .and_then(serde_json::Value::as_str)
                            .and_then(|stack| stack_regions.get(stack).cloned())
                    })
                })
                .unwrap_or_else(|| default_region.clone());
            account
                .region_mut(&region)
                .extras
                .entry(category.clone())
                .or_default()
                .insert(id.clone(), record.clone());
        }
    }
    account
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

    fn stack(name: &str, stack_id: &str) -> Stack {
        Stack {
            name: name.to_string(),
            stack_id: stack_id.to_string(),
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
            pending_update: None,
            cancel_requested: false,
        }
    }

    #[test]
    fn reset_clears_stacks() {
        let mut state = CloudFormationState::new("123456789012", "us-east-1");
        state.stacks.insert("s1".to_string(), stack("s1", "id"));
        state.reset();
        assert!(state.stacks.is_empty());
    }

    #[test]
    fn regions_are_created_on_first_use_and_isolated() {
        let mut accounts =
            MultiAccountState::<CloudFormationAccountState>::new("111111111111", "us-east-1", "");
        assert!(accounts.regional("111111111111", "eu-west-1").is_none());
        for region in ["us-east-1", "eu-west-1"] {
            let state = accounts.regional_mut("111111111111", region);
            assert_eq!(state.region, region);
            assert_eq!(state.account_id, "111111111111");
            state.stacks.insert(
                "app".to_string(),
                stack(
                    "app",
                    &format!("arn:aws:cloudformation:{region}:111111111111:stack/app/{region}"),
                ),
            );
        }
        for region in ["us-east-1", "eu-west-1"] {
            let state = accounts.regional("111111111111", region).unwrap();
            assert_eq!(state.stacks.len(), 1);
            assert!(state.stacks["app"].stack_id.contains(region));
        }
        let account = accounts.get("111111111111").unwrap();
        assert_eq!(account.all_stacks().count(), 2);
        assert!(accounts
            .regional_get_mut("222222222222", "us-east-1")
            .is_none());
    }

    /// A v2 snapshot kept one account-wide state; loading it splits every
    /// record into the region its ARN names.
    #[test]
    fn v2_snapshot_migrates_records_into_their_regions() {
        let east_id = "arn:aws:cloudformation:us-east-1:111111111111:stack/east/1";
        let west_id = "arn:aws:cloudformation:eu-west-1:111111111111:stack/west/2";
        let gone_id = "arn:aws:cloudformation:us-east-1:111111111111:stack/gone/3";
        let legacy_stack = |name: &str, id: &str| serde_json::to_value(stack(name, id)).unwrap();
        let mut gone = stack("gone", gone_id);
        gone.status = "DELETE_COMPLETE".to_string();
        let account = serde_json::json!({
            "account_id": "111111111111",
            "region": "us-east-1",
            "stacks": {
                "east": legacy_stack("east", east_id),
                "west": legacy_stack("west", west_id),
                "unparseable": legacy_stack("unparseable", "not-an-arn"),
                "gone": serde_json::to_value(&gone).unwrap(),
            },
            "events": {
                east_id: [{"EventId": "e1"}],
                west_id: [{"EventId": "w1"}],
            },
            // By name and by stack id as older builds stored them: east has
            // both (the name-keyed one wins), a deleted stack's are dropped.
            "stack_policies": {
                "west": "{}",
                east_id: "{\"by-id\":1}",
                "east": "{\"by-name\":1}",
                "gone": "{}",
                gone_id: "{}",
            },
            "exports": {
                "WestExport": {
                    "value": "v",
                    "exporting_stack_id": west_id,
                    "exporting_stack_name": "west",
                },
            },
            "imports": {"WestExport": ["west-consumer"]},
            "extras": {
                "change_sets": {
                    "cs": {
                        "ChangeSetId": "arn:aws:cloudformation:eu-west-1:111111111111:changeSet/cs/3",
                        "StackId": west_id,
                    },
                },
                "hooks": {"My::Hook::Hook": {"TypeName": "My::Hook::Hook"}},
                // No ARN: follows the stack it names.
                "drift_detection": {"d1": {"StackName": "west", "Status": "DETECTION_COMPLETE"}},
                // ARNs naming two regions: the stack's decides, whatever the
                // key order.
                "hook_results": {
                    "h1": {
                        "Arn": "arn:aws:cloudformation:us-east-1:111111111111:hook/1",
                        "StackId": west_id,
                    },
                },
            },
            "stack_sets": {
                "set:4": {
                    "stack_set_id": "set:4",
                    "name": "set",
                    "arn": "arn:aws:cloudformation:ap-south-1:111111111111:stackset/set:4",
                    "status": "ACTIVE",
                    "permission_model": "SELF_MANAGED",
                    "created_at": "2026-01-01T00:00:00Z",
                },
            },
            "orgs_access_enabled": true,
        });
        let snapshot = serde_json::json!({
            "schema_version": 2,
            "accounts": {
                "default_account_id": "111111111111",
                "region": "us-east-1",
                "endpoint": "",
                "accounts": {"111111111111": account},
            },
        });
        let parsed = parse_cloudformation_snapshot(&serde_json::to_vec(&snapshot).unwrap())
            .expect("v2 parses");
        assert_eq!(
            parsed.schema_version,
            CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION
        );
        let accounts = parsed.accounts.expect("accounts");
        let account = accounts.get("111111111111").unwrap();
        assert!(account.orgs_access_enabled);
        assert_eq!(account.default_region, "us-east-1");

        let east = account.region("us-east-1").unwrap();
        let mut east_stacks: Vec<&str> = east.stacks.keys().map(String::as_str).collect();
        east_stacks.sort_unstable();
        // A stack id that names no region falls back to the configured one.
        assert_eq!(east_stacks, ["east", "gone", "unparseable"]);
        assert!(east.events.contains_key(east_id));
        assert!(east.exports.is_empty() && east.imports.is_empty());
        // A record with no ARN goes to the configured region.
        assert!(east.extras["hooks"].contains_key("My::Hook::Hook"));
        assert_eq!(east.stack_policies["east"], "{\"by-name\":1}");
        assert!(!east.stack_policies.contains_key(east_id));
        assert!(!east.stack_policies.contains_key("gone"));
        assert!(!east.stack_policies.contains_key(gone_id));

        let west = account.region("eu-west-1").unwrap();
        assert_eq!(west.region, "eu-west-1");
        assert_eq!(west.stacks.keys().collect::<Vec<_>>(), ["west"]);
        assert!(west.events.contains_key(west_id));
        assert!(west.stack_policies.contains_key("west"));
        assert_eq!(west.exports["WestExport"].value, "v");
        assert_eq!(west.imports["WestExport"], ["west-consumer"]);
        assert!(west.extras["change_sets"].contains_key("cs"));
        assert!(west.extras["drift_detection"].contains_key("d1"));
        assert!(west.extras["hook_results"].contains_key("h1"));

        let south = account.region("ap-south-1").unwrap();
        assert!(south.stack_sets.contains_key("set:4"));
        assert!(south.stacks.is_empty());
    }

    #[test]
    fn current_snapshot_round_trips_and_newer_is_reported() {
        let mut accounts =
            MultiAccountState::<CloudFormationAccountState>::new("111111111111", "us-east-1", "");
        accounts
            .regional_mut("111111111111", "eu-west-1")
            .stacks
            .insert(
                "app".to_string(),
                stack(
                    "app",
                    "arn:aws:cloudformation:eu-west-1:111111111111:stack/app/1",
                ),
            );
        let bytes = serde_json::to_vec(&CloudFormationSnapshot {
            schema_version: CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(accounts),
            state: None,
        })
        .unwrap();
        let parsed = parse_cloudformation_snapshot(&bytes).unwrap();
        let accounts = parsed.accounts.unwrap();
        assert!(accounts
            .regional("111111111111", "eu-west-1")
            .unwrap()
            .stacks
            .contains_key("app"));
        assert!(accounts.regional("111111111111", "us-east-1").is_none());

        let newer =
            serde_json::json!({"schema_version": CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION + 1});
        let parsed = parse_cloudformation_snapshot(&serde_json::to_vec(&newer).unwrap()).unwrap();
        assert_eq!(
            parsed.schema_version,
            CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION + 1
        );
        assert!(parsed.accounts.is_none());
    }

    /// v1 held a single account's state, outside any multi-account container.
    #[test]
    fn v1_snapshot_migrates_into_regions() {
        let id = "arn:aws:cloudformation:eu-west-1:111111111111:stack/app/1";
        let snapshot = serde_json::json!({
            "schema_version": 1,
            "state": {
                "account_id": "111111111111",
                "region": "us-east-1",
                "stacks": {"app": serde_json::to_value(stack("app", id)).unwrap()},
            },
        });
        let parsed = parse_cloudformation_snapshot(&serde_json::to_vec(&snapshot).unwrap())
            .expect("v1 parses");
        assert!(parsed.accounts.is_none());
        let account = parsed.state.expect("single account");
        assert_eq!(account.account_id, "111111111111");
        assert!(account
            .region("eu-west-1")
            .unwrap()
            .stacks
            .contains_key("app"));
    }
}
