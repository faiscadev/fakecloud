//! CloudFormation StackSets.
//!
//! A stack set is a template plus parameters that is deployed as one stack per
//! (account, region) pair, a *stack instance*. Every call that changes what is
//! deployed runs as a stack set *operation*. The instances an operation touches
//! are provisioned by driving the ordinary CreateStack / UpdateStack /
//! DeleteStack paths in the target account and region, so an instance's stack
//! is a real stack whose resources exist in the backing services. Each
//! target's outcome is recorded on the operation, which is what
//! DescribeStackSetOperation and ListStackSetOperationResults report.
//!
//! Operations run to completion inside the call that starts them, except for
//! stacks that provision asynchronously (templates with custom resources).
//! Those leave the instance `RUNNING`, and every later read of the stack set
//! folds the stack's current status back into the instance and the operation.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_aws::arn::{partition_for, partition_of};
use fakecloud_aws::xml::xml_escape;
use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::extras::{looks_like_url, xml_response, xml_response_no_result};
use crate::service::{AutoDeploymentClaim, CloudFormationService};
use crate::state::{CloudFormationAccountState, CloudFormationState, RegionalAccounts};

/// Service principal Organizations uses for StackSets trusted access and
/// delegated administration.
const STACKSETS_PRINCIPAL: &str = "member.org.stacksets.cloudformation.amazonaws.com";
/// Name of the optional per-account Lambda that gates deployments.
const ACCOUNT_GATE_FUNCTION: &str = "AWSCloudFormationStackSetAccountGate";
const DEFAULT_ADMIN_ROLE: &str = "AWSCloudFormationStackSetAdministrationRole";
const DEFAULT_EXECUTION_ROLE: &str = "AWSCloudFormationStackSetExecutionRole";
/// ImportStacksToStackSet accepts at most this many stacks per call.
const MAX_IMPORT_STACKS: usize = 10;
const DEFAULT_PAGE_SIZE: usize = 100;
/// How long an operation waits on one background-provisioning stack.
const STACK_WAIT_LIMIT: std::time::Duration = std::time::Duration::from_secs(3600);

const TOLERANCE_EXCEEDED: &str = "Cancelled since failure tolerance has exceeded";
const OPERATION_STOPPED: &str = "Cancelled since the operation was stopped";

// ── State ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackSet {
    pub stack_set_id: String,
    pub name: String,
    pub arn: String,
    /// `ACTIVE`, or `DELETED` once DeleteStackSet has run. Deleted stack sets
    /// stay listable and describable by id, as in AWS.
    pub status: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub template_body: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub tags: Vec<(String, String)>,
    #[serde(default)]
    pub administration_role_arn: Option<String>,
    #[serde(default)]
    pub execution_role_name: Option<String>,
    pub permission_model: String,
    #[serde(default)]
    pub auto_deployment: Option<AutoDeployment>,
    /// The OUs a service-managed stack set is deployed to, each with the
    /// regions it is deployed to there. Kept on the stack set rather than
    /// derived from its instances, so an OU that momentarily has no accounts
    /// stays a target and an account created in it later is still deployed
    /// to. This is what `ListStackSetAutoDeploymentTargets` reports.
    #[serde(default)]
    pub auto_deployment_targets: BTreeMap<String, BTreeSet<String>>,
    /// `(organizational unit, account, region)` triples that must not be
    /// deployed to: ones an `AccountFilterType` left out when the instances
    /// were created, and ones whose instance was deleted by hand. Without
    /// this, reconciling against the organization would undo an explicit
    /// `DeleteStackInstances` on the next membership change.
    ///
    /// Keyed by the OU the decision was made under, so it lasts exactly as
    /// long as the account stays there: moving to another target OU (or out
    /// of the targets altogether and back) deploys to it again, as AWS does.
    #[serde(default)]
    pub auto_deployment_excluded: BTreeSet<(String, String, String)>,
    #[serde(default)]
    pub managed_execution_active: bool,
    #[serde(default)]
    pub instances: Vec<StackInstance>,
    #[serde(default)]
    pub operations: Vec<StackSetOperation>,
    /// Result of the most recent DetectStackSetDrift, if any.
    #[serde(default)]
    pub drift: Option<DriftDetectionDetails>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoDeployment {
    pub enabled: bool,
    pub retain_stacks_on_account_removal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackInstance {
    pub account: String,
    pub region: String,
    #[serde(default)]
    pub stack_id: Option<String>,
    /// `CURRENT`, `OUTDATED` or `INOPERABLE`.
    pub status: String,
    /// `StackInstanceStatus.DetailedStatus`.
    pub detailed_status: String,
    #[serde(default)]
    pub status_reason: Option<String>,
    #[serde(default)]
    pub parameter_overrides: BTreeMap<String, String>,
    #[serde(default)]
    pub organizational_unit_id: Option<String>,
    pub drift_status: String,
    #[serde(default)]
    pub last_drift_check_timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_operation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackSetOperation {
    pub operation_id: String,
    /// `CREATE`, `UPDATE`, `DELETE` or `DETECT_DRIFT`.
    pub action: String,
    /// `RUNNING`, `SUCCEEDED`, `FAILED`, `STOPPING` or `STOPPED`.
    pub status: String,
    #[serde(default)]
    pub status_reason: Option<String>,
    #[serde(default)]
    pub retain_stacks: Option<bool>,
    #[serde(default)]
    pub preferences: OperationPreferences,
    #[serde(default)]
    pub deployment_targets: Option<DeploymentTargets>,
    #[serde(default)]
    pub administration_role_arn: Option<String>,
    #[serde(default)]
    pub execution_role_name: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub ended_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub results: Vec<OperationResult>,
    #[serde(default)]
    pub drift: Option<DriftDetectionDetails>,
    #[serde(default)]
    pub resource_drifts: Vec<InstanceResourceDrift>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationResult {
    pub account: String,
    pub region: String,
    /// `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED` or `CANCELLED`.
    pub status: String,
    #[serde(default)]
    pub status_reason: Option<String>,
    #[serde(default)]
    pub organizational_unit_id: Option<String>,
    #[serde(default)]
    pub account_gate_status: Option<String>,
    #[serde(default)]
    pub account_gate_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperationPreferences {
    #[serde(default)]
    pub region_concurrency_type: Option<String>,
    #[serde(default)]
    pub region_order: Vec<String>,
    #[serde(default)]
    pub failure_tolerance_count: Option<u32>,
    #[serde(default)]
    pub failure_tolerance_percentage: Option<u32>,
    #[serde(default)]
    pub max_concurrent_count: Option<u32>,
    #[serde(default)]
    pub max_concurrent_percentage: Option<u32>,
    #[serde(default)]
    pub concurrency_mode: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeploymentTargets {
    #[serde(default)]
    pub accounts: Vec<String>,
    #[serde(default)]
    pub accounts_url: Option<String>,
    #[serde(default)]
    pub organizational_unit_ids: Vec<String>,
    #[serde(default)]
    pub account_filter_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftDetectionDetails {
    pub drift_status: String,
    pub detection_status: String,
    pub last_drift_check_timestamp: DateTime<Utc>,
    pub total: usize,
    pub drifted: usize,
    pub in_sync: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceResourceDrift {
    pub account: String,
    pub region: String,
    pub stack_id: String,
    pub logical_id: String,
    pub physical_id: String,
    pub resource_type: String,
    /// `IN_SYNC`, `DELETED` or `NOT_CHECKED`.
    pub status: String,
    pub timestamp: DateTime<Utc>,
}

const OPERATION_INTERRUPTED: &str = "The operation was interrupted by a restart";

/// Bring persisted stack sets into a loadable state: migrate records from
/// older builds, and settle operations that were still deploying when the
/// process stopped. Nothing resumes those, so left RUNNING they would block
/// every later operation on their stack set.
pub fn restore_stack_sets(accounts: &mut MultiAccountState<CloudFormationAccountState>) {
    migrate_legacy_stack_sets(accounts);
    let sets: Vec<(String, String)> = accounts
        .iter()
        .flat_map(|(account, state)| {
            state
                .regions
                .values()
                .flat_map(|r| r.stack_sets.keys())
                .map(move |id| (account.to_string(), id.clone()))
        })
        .collect();
    for (account, set_id) in sets {
        // A stack that finished after the last snapshot of its stack set
        // counts as finished, not as interrupted.
        CloudFormationService::refresh_stack_set(accounts, &account, &set_id);
        if let Some(set) = accounts
            .get_mut(&account)
            .and_then(|s| s.stack_set_mut(&set_id))
        {
            backfill_auto_deployment_targets(set);
            settle_interrupted_operations(set);
        }
    }
}

/// A stack set persisted before auto-deployment targets were kept on the set
/// itself: recover them from where it is deployed, which is what the previous
/// build derived them from. A stack set from that build with no instances
/// left has nothing to recover them from — the previous build had already
/// stopped following its OUs in that state — so it restores with none, and
/// the next CreateStackInstances sets them again.
fn backfill_auto_deployment_targets(set: &mut StackSet) {
    if set.permission_model != "SERVICE_MANAGED" || !set.auto_deployment_targets.is_empty() {
        return;
    }
    let targets: Vec<(String, String)> = set
        .instances
        .iter()
        // A refused import never deployed anything, so its OU was never a
        // target — the same rule ImportStacksToStackSet applies.
        .filter(|i| i.detailed_status != "FAILED_IMPORT")
        .filter_map(|i| {
            i.organizational_unit_id
                .as_ref()
                .map(|ou| (ou.clone(), i.region.clone()))
        })
        .collect();
    for (ou, region) in targets {
        set.auto_deployment_targets
            .entry(ou)
            .or_default()
            .insert(region);
    }
}

fn settle_interrupted_operations(set: &mut StackSet) {
    for op in &mut set.operations {
        if !matches!(op.status.as_str(), "RUNNING" | "STOPPING") {
            continue;
        }
        for result in &mut op.results {
            let status = match result.status.as_str() {
                "PENDING" => "CANCELLED",
                "RUNNING" => "FAILED",
                _ => continue,
            };
            result.status = status.to_string();
            result.status_reason = Some(OPERATION_INTERRUPTED.to_string());
        }
        // An instance ends up as its result did: cancelled if its target
        // never started, failed if it was deploying.
        for instance in &mut set.instances {
            if instance.last_operation_id.as_deref() != Some(op.operation_id.as_str())
                || !matches!(instance.detailed_status.as_str(), "RUNNING" | "PENDING")
            {
                continue;
            }
            let cancelled = op.results.iter().any(|r| {
                r.account == instance.account
                    && r.region == instance.region
                    && r.status == "CANCELLED"
            });
            let reason = OPERATION_INTERRUPTED.to_string();
            let outcome = if cancelled {
                Outcome::Cancelled(reason)
            } else {
                Outcome::Failed(reason)
            };
            apply_to_instance(instance, &outcome);
        }
        // Whatever the tolerance, an operation that never got to finish did
        // not succeed.
        op.status = if op.status == "STOPPING" {
            "STOPPED"
        } else {
            "FAILED"
        }
        .to_string();
        op.status_reason = Some(OPERATION_INTERRUPTED.to_string());
        op.ended_at = Some(Utc::now());
    }
}

/// Move stack sets persisted by older builds, which kept a
/// `{StackSetId, StackSetName, Status, TemplateBody}` JSON record in the
/// generic `extras` store, into the typed store.
fn migrate_legacy_stack_sets(accounts: &mut MultiAccountState<CloudFormationAccountState>) {
    let regional = accounts.iter_mut().flat_map(|(account_id, account)| {
        account
            .regions
            .values_mut()
            .map(move |state| (account_id, state))
    });
    for (account_id, state) in regional {
        let Some(legacy) = state.extras.remove("stack_sets") else {
            continue;
        };
        for (name, record) in legacy {
            let id = record["StackSetId"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| format!("{name}:{}", uuid::Uuid::new_v4()));
            if state.stack_sets.contains_key(&id) {
                continue;
            }
            let arn = format!(
                "arn:{}:cloudformation:{}:{account_id}:stackset/{id}",
                partition_for(&state.region),
                state.region
            );
            state.stack_sets.insert(
                id.clone(),
                StackSet {
                    stack_set_id: id,
                    name: record["StackSetName"].as_str().unwrap_or(&name).to_string(),
                    arn,
                    status: record["Status"].as_str().unwrap_or("ACTIVE").to_string(),
                    description: None,
                    template_body: record["TemplateBody"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    parameters: BTreeMap::new(),
                    capabilities: Vec::new(),
                    tags: Vec::new(),
                    administration_role_arn: None,
                    execution_role_name: None,
                    permission_model: "SELF_MANAGED".to_string(),
                    auto_deployment: None,
                    auto_deployment_targets: BTreeMap::new(),
                    auto_deployment_excluded: BTreeSet::new(),
                    managed_execution_active: false,
                    instances: Vec::new(),
                    operations: Vec::new(),
                    drift: None,
                    created_at: Utc::now(),
                },
            );
        }
    }
}

pub(crate) fn is_stack_set_action(action: &str) -> bool {
    matches!(
        action,
        "CreateStackSet"
            | "DescribeStackSet"
            | "ListStackSets"
            | "UpdateStackSet"
            | "DeleteStackSet"
            | "CreateStackInstances"
            | "UpdateStackInstances"
            | "DeleteStackInstances"
            | "DescribeStackInstance"
            | "ListStackInstances"
            | "DescribeStackSetOperation"
            | "ListStackSetOperations"
            | "ListStackSetOperationResults"
            | "StopStackSetOperation"
            | "ImportStacksToStackSet"
            | "ListStackSetAutoDeploymentTargets"
            | "DetectStackSetDrift"
            | "ListStackInstanceResourceDrifts"
    )
}

/// Which stack sets a caller can address. A StackSets delegated administrator
/// acts on the management account's stack sets, but only the service-managed
/// ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    Own,
    DelegatedAdmin,
}

impl Scope {
    pub(crate) fn of(params: &BTreeMap<String, String>) -> Self {
        if params.get("CallAs").map(String::as_str) == Some("DELEGATED_ADMIN") {
            Scope::DelegatedAdmin
        } else {
            Scope::Own
        }
    }

    fn sees(self, set: &StackSet) -> bool {
        self == Scope::Own || set.permission_model == "SERVICE_MANAGED"
    }
}

/// Find an ACTIVE stack set by name or id.
pub(crate) fn find_active<'a>(
    state: &'a CloudFormationState,
    name_or_id: &str,
    scope: Scope,
) -> Option<&'a StackSet> {
    state.stack_sets.values().find(|s| {
        s.status == "ACTIVE"
            && (s.name == name_or_id || s.stack_set_id == name_or_id)
            && scope.sees(s)
    })
}

/// Find a stack set for a read: an active one by name or id, or a deleted one
/// by its (unique) id. A deleted stack set's name is free for reuse, so a name
/// never resolves to one.
fn find_for_read<'a>(
    state: &'a CloudFormationState,
    name_or_id: &str,
    scope: Scope,
) -> Option<&'a StackSet> {
    find_active(state, name_or_id, scope)
        .or_else(|| state.stack_sets.get(name_or_id).filter(|s| scope.sees(s)))
}

fn active_key(state: &CloudFormationState, name_or_id: &str, scope: Scope) -> Option<String> {
    find_active(state, name_or_id, scope).map(|s| s.stack_set_id.clone())
}

// ── Errors ──

fn aws_err(status: StatusCode, code: &str, message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(status, code, message)
}

fn validation(message: impl Into<String>) -> AwsServiceError {
    aws_err(StatusCode::BAD_REQUEST, "ValidationError", message)
}

fn stack_set_not_found(name: &str) -> AwsServiceError {
    aws_err(
        StatusCode::NOT_FOUND,
        "StackSetNotFoundException",
        format!("StackSet {name} not found"),
    )
}

fn operation_not_found(op_id: &str) -> AwsServiceError {
    aws_err(
        StatusCode::NOT_FOUND,
        "OperationNotFoundException",
        format!("Operation {op_id} not found"),
    )
}

fn instance_not_found(set: &str, account: &str, region: &str) -> AwsServiceError {
    aws_err(
        StatusCode::NOT_FOUND,
        "StackInstanceNotFoundException",
        format!("Stack instance with account {account} and region {region} not found for stack set {set}"),
    )
}

fn required(params: &BTreeMap<String, String>, field: &str) -> Result<String, AwsServiceError> {
    params
        .get(field)
        .cloned()
        .ok_or_else(|| validation(format!("{field} is required")))
}

// ── Request parsing ──

/// `Prefix.member.N` scalar list.
fn member_list(params: &BTreeMap<String, String>, prefix: &str) -> Vec<String> {
    (1..)
        .map_while(|i| params.get(&format!("{prefix}.member.{i}")).cloned())
        .collect()
}

/// Whether the request carries the list `prefix` at all, including the empty
/// form (`Prefix=`) the CLI sends for an explicitly empty list.
fn list_present(params: &BTreeMap<String, String>, prefix: &str) -> bool {
    let dotted = format!("{prefix}.");
    params.contains_key(prefix) || params.keys().any(|k| k.starts_with(&dotted))
}

struct ParameterEntry {
    key: String,
    value: Option<String>,
    use_previous: bool,
}

fn parameter_list(params: &BTreeMap<String, String>, prefix: &str) -> Vec<ParameterEntry> {
    (1..)
        .map_while(|i| {
            let key = params.get(&format!("{prefix}.member.{i}.ParameterKey"))?;
            Some(ParameterEntry {
                key: key.clone(),
                value: params
                    .get(&format!("{prefix}.member.{i}.ParameterValue"))
                    .cloned(),
                use_previous: params
                    .get(&format!("{prefix}.member.{i}.UsePreviousValue"))
                    .is_some_and(|v| v.eq_ignore_ascii_case("true")),
            })
        })
        .collect()
}

/// Resolve a parameter list against `previous`: explicit values win, and
/// `UsePreviousValue` entries carry the previous value forward.
fn resolve_parameters(
    entries: &[ParameterEntry],
    previous: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, AwsServiceError> {
    let mut out = BTreeMap::new();
    for entry in entries {
        match (&entry.value, entry.use_previous) {
            (Some(_), true) => {
                return Err(validation(format!(
                    "Invalid input for parameter key {}. Cannot specify usePreviousValue as true and a parameter value at the same time",
                    entry.key
                )))
            }
            (Some(value), false) => {
                out.insert(entry.key.clone(), value.clone());
            }
            (None, true) => match previous.get(&entry.key) {
                Some(value) => {
                    out.insert(entry.key.clone(), value.clone());
                }
                None => {
                    return Err(validation(format!(
                        "Parameter {} does not have a previous value",
                        entry.key
                    )))
                }
            },
            (None, false) => {
                return Err(validation(format!(
                    "Invalid input for parameter key {}. Need to specify either usePreviousValue as true or a value for the parameter",
                    entry.key
                )))
            }
        }
    }
    Ok(out)
}

fn tag_list(params: &BTreeMap<String, String>) -> Vec<(String, String)> {
    (1..)
        .map_while(|i| {
            let key = params.get(&format!("Tags.member.{i}.Key"))?;
            let value = params.get(&format!("Tags.member.{i}.Value"))?;
            Some((key.clone(), value.clone()))
        })
        .collect()
}

fn parse_bool(
    params: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<bool>, AwsServiceError> {
    match params.get(key) {
        None => Ok(None),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(Some(true)),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(Some(false)),
        Some(v) => Err(validation(format!("Invalid value {v} for {key}"))),
    }
}

fn parse_u32(params: &BTreeMap<String, String>, key: &str) -> Result<Option<u32>, AwsServiceError> {
    params
        .get(key)
        .map(|v| {
            v.parse::<u32>()
                .map_err(|_| validation(format!("Invalid value {v} for {key}")))
        })
        .transpose()
}

fn parse_preferences(
    params: &BTreeMap<String, String>,
) -> Result<OperationPreferences, AwsServiceError> {
    let p = "OperationPreferences";
    let prefs = OperationPreferences {
        region_concurrency_type: params.get(&format!("{p}.RegionConcurrencyType")).cloned(),
        region_order: member_list(params, &format!("{p}.RegionOrder")),
        failure_tolerance_count: parse_u32(params, &format!("{p}.FailureToleranceCount"))?,
        failure_tolerance_percentage: parse_u32(
            params,
            &format!("{p}.FailureTolerancePercentage"),
        )?,
        max_concurrent_count: parse_u32(params, &format!("{p}.MaxConcurrentCount"))?,
        max_concurrent_percentage: parse_u32(params, &format!("{p}.MaxConcurrentPercentage"))?,
        concurrency_mode: params.get(&format!("{p}.ConcurrencyMode")).cloned(),
    };
    if prefs.failure_tolerance_count.is_some() && prefs.failure_tolerance_percentage.is_some() {
        return Err(validation(
            "FailureToleranceCount and FailureTolerancePercentage cannot both be specified",
        ));
    }
    if prefs.max_concurrent_count.is_some() && prefs.max_concurrent_percentage.is_some() {
        return Err(validation(
            "MaxConcurrentCount and MaxConcurrentPercentage cannot both be specified",
        ));
    }
    if prefs.failure_tolerance_percentage.is_some_and(|v| v > 100)
        || prefs.max_concurrent_percentage.is_some_and(|v| v > 100)
    {
        return Err(validation("Percentage values must be between 0 and 100"));
    }
    Ok(prefs)
}

fn parse_deployment_targets(params: &BTreeMap<String, String>) -> Option<DeploymentTargets> {
    let p = "DeploymentTargets";
    if !list_present(params, p) {
        return None;
    }
    Some(DeploymentTargets {
        accounts: member_list(params, &format!("{p}.Accounts")),
        accounts_url: params.get(&format!("{p}.AccountsUrl")).cloned(),
        organizational_unit_ids: member_list(params, &format!("{p}.OrganizationalUnitIds")),
        account_filter_type: params.get(&format!("{p}.AccountFilterType")).cloned(),
    })
}

/// The deployment targets an operation records, in `DeploymentTargets` form
/// whether the request used top-level `Accounts` or `DeploymentTargets`.
fn targets_record(
    accounts: &[String],
    deployment_targets: Option<&DeploymentTargets>,
) -> DeploymentTargets {
    match deployment_targets {
        Some(dt) => dt.clone(),
        None => DeploymentTargets {
            accounts: accounts.to_vec(),
            ..DeploymentTargets::default()
        },
    }
}

fn is_account_id(value: &str) -> bool {
    value.len() == 12 && value.bytes().all(|b| b.is_ascii_digit())
}

/// Parse `arn:aws:cloudformation:{region}:{account}:stack/{name}/{id}` into
/// `(account, region)`.
fn stack_arn_location(arn: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() != 6 || parts[0] != "arn" || parts[2] != "cloudformation" {
        return None;
    }
    if !parts[5].starts_with("stack/") || !is_account_id(parts[4]) || parts[3].is_empty() {
        return None;
    }
    Some((parts[4].to_string(), parts[3].to_string()))
}

fn paginate<T>(
    items: Vec<T>,
    params: &BTreeMap<String, String>,
) -> Result<(Vec<T>, Option<String>), AwsServiceError> {
    let start = match params.get("NextToken") {
        Some(token) => token
            .parse::<usize>()
            .map_err(|_| validation("Invalid NextToken"))?,
        None => 0,
    };
    let size = params
        .get("MaxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_PAGE_SIZE);
    let total = items.len();
    if start > total {
        return Err(validation("Invalid NextToken"));
    }
    let end = start.saturating_add(size);
    let page: Vec<T> = items.into_iter().skip(start).take(size).collect();
    let next = (end < total).then(|| end.to_string());
    Ok((page, next))
}

// ── XML ──

fn ts(t: &DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn el(name: &str, value: &str) -> String {
    format!("<{name}>{}</{name}>", xml_escape(value))
}

fn opt_el(name: &str, value: Option<&str>) -> String {
    value.map(|v| el(name, v)).unwrap_or_default()
}

fn list_el(name: &str, members: impl IntoIterator<Item = String>) -> String {
    let inner: String = members
        .into_iter()
        .map(|m| format!("<member>{m}</member>"))
        .collect();
    if inner.is_empty() {
        format!("<{name}/>")
    } else {
        format!("<{name}>{inner}</{name}>")
    }
}

fn scalar_list_el<'a>(name: &str, values: impl IntoIterator<Item = &'a String>) -> String {
    list_el(name, values.into_iter().map(|v| xml_escape(v)))
}

fn parameters_el(name: &str, params: &BTreeMap<String, String>) -> String {
    list_el(
        name,
        params
            .iter()
            .map(|(k, v)| format!("{}{}", el("ParameterKey", k), el("ParameterValue", v))),
    )
}

fn next_token_el(next: Option<String>) -> String {
    next.map(|t| el("NextToken", &t)).unwrap_or_default()
}

fn preferences_el(p: &OperationPreferences) -> String {
    let mut out = String::new();
    out.push_str(&opt_el(
        "RegionConcurrencyType",
        p.region_concurrency_type.as_deref(),
    ));
    if !p.region_order.is_empty() {
        out.push_str(&scalar_list_el("RegionOrder", &p.region_order));
    }
    for (name, value) in [
        ("FailureToleranceCount", p.failure_tolerance_count),
        ("FailureTolerancePercentage", p.failure_tolerance_percentage),
        ("MaxConcurrentCount", p.max_concurrent_count),
        ("MaxConcurrentPercentage", p.max_concurrent_percentage),
    ] {
        if let Some(v) = value {
            out.push_str(&el(name, &v.to_string()));
        }
    }
    out.push_str(&opt_el("ConcurrencyMode", p.concurrency_mode.as_deref()));
    format!("<OperationPreferences>{out}</OperationPreferences>")
}

fn drift_details_el(d: Option<&DriftDetectionDetails>) -> String {
    match d {
        Some(d) => format!(
            "<StackSetDriftDetectionDetails>{}{}{}{}{}{}{}{}</StackSetDriftDetectionDetails>",
            el("DriftStatus", &d.drift_status),
            el("DriftDetectionStatus", &d.detection_status),
            el("LastDriftCheckTimestamp", &ts(&d.last_drift_check_timestamp)),
            el("TotalStackInstancesCount", &d.total.to_string()),
            el("DriftedStackInstancesCount", &d.drifted.to_string()),
            el("InSyncStackInstancesCount", &d.in_sync.to_string()),
            el("InProgressStackInstancesCount", "0"),
            el("FailedStackInstancesCount", &d.failed.to_string()),
        ),
        None => "<StackSetDriftDetectionDetails><DriftStatus>NOT_CHECKED</DriftStatus><TotalStackInstancesCount>0</TotalStackInstancesCount><DriftedStackInstancesCount>0</DriftedStackInstancesCount><InSyncStackInstancesCount>0</InSyncStackInstancesCount><InProgressStackInstancesCount>0</InProgressStackInstancesCount><FailedStackInstancesCount>0</FailedStackInstancesCount></StackSetDriftDetectionDetails>".to_string(),
    }
}

fn auto_deployment_el(a: Option<&AutoDeployment>) -> String {
    a.map(|a| {
        format!(
            "<AutoDeployment>{}{}</AutoDeployment>",
            el("Enabled", &a.enabled.to_string()),
            el(
                "RetainStacksOnAccountRemoval",
                &a.retain_stacks_on_account_removal.to_string()
            ),
        )
    })
    .unwrap_or_default()
}

fn managed_execution_el(active: bool) -> String {
    format!(
        "<ManagedExecution>{}</ManagedExecution>",
        el("Active", &active.to_string())
    )
}

fn deployment_targets_el(t: &DeploymentTargets) -> String {
    let mut out = String::new();
    if !t.accounts.is_empty() {
        out.push_str(&scalar_list_el("Accounts", &t.accounts));
    }
    out.push_str(&opt_el("AccountsUrl", t.accounts_url.as_deref()));
    if !t.organizational_unit_ids.is_empty() {
        out.push_str(&scalar_list_el(
            "OrganizationalUnitIds",
            &t.organizational_unit_ids,
        ));
    }
    out.push_str(&opt_el(
        "AccountFilterType",
        t.account_filter_type.as_deref(),
    ));
    format!("<DeploymentTargets>{out}</DeploymentTargets>")
}

fn status_details_el(op: &StackSetOperation) -> String {
    let failed = op.results.iter().filter(|r| r.status == "FAILED").count();
    format!(
        "<StatusDetails>{}</StatusDetails>",
        el("FailedStackInstancesCount", &failed.to_string())
    )
}

fn stack_set_regions(set: &StackSet) -> Vec<String> {
    let regions: BTreeSet<&String> = set.instances.iter().map(|i| &i.region).collect();
    regions.into_iter().cloned().collect()
}

fn stack_set_ous(set: &StackSet) -> Vec<String> {
    let ous: BTreeSet<&String> = set
        .instances
        .iter()
        .filter_map(|i| i.organizational_unit_id.as_ref())
        .collect();
    ous.into_iter().cloned().collect()
}

fn stack_set_el(set: &StackSet) -> String {
    let mut out = String::new();
    out.push_str(&el("StackSetName", &set.name));
    out.push_str(&el("StackSetId", &set.stack_set_id));
    out.push_str(&opt_el("Description", set.description.as_deref()));
    out.push_str(&el("Status", &set.status));
    out.push_str(&el("TemplateBody", &set.template_body));
    out.push_str(&parameters_el("Parameters", &set.parameters));
    out.push_str(&scalar_list_el("Capabilities", &set.capabilities));
    out.push_str(&list_el(
        "Tags",
        set.tags
            .iter()
            .map(|(k, v)| format!("{}{}", el("Key", k), el("Value", v))),
    ));
    out.push_str(&el("StackSetARN", &set.arn));
    out.push_str(&opt_el(
        "AdministrationRoleARN",
        set.administration_role_arn.as_deref(),
    ));
    out.push_str(&opt_el(
        "ExecutionRoleName",
        set.execution_role_name.as_deref(),
    ));
    out.push_str(&drift_details_el(set.drift.as_ref()));
    out.push_str(&auto_deployment_el(set.auto_deployment.as_ref()));
    out.push_str(&el("PermissionModel", &set.permission_model));
    out.push_str(&scalar_list_el(
        "OrganizationalUnitIds",
        &stack_set_ous(set),
    ));
    out.push_str(&managed_execution_el(set.managed_execution_active));
    out.push_str(&scalar_list_el("Regions", &stack_set_regions(set)));
    format!("<StackSet>{out}</StackSet>")
}

fn stack_set_summary_el(set: &StackSet) -> String {
    let mut out = String::new();
    out.push_str(&el("StackSetName", &set.name));
    out.push_str(&el("StackSetId", &set.stack_set_id));
    out.push_str(&opt_el("Description", set.description.as_deref()));
    out.push_str(&el("Status", &set.status));
    out.push_str(&auto_deployment_el(set.auto_deployment.as_ref()));
    out.push_str(&el("PermissionModel", &set.permission_model));
    out.push_str(&el(
        "DriftStatus",
        set.drift
            .as_ref()
            .map_or("NOT_CHECKED", |d| d.drift_status.as_str()),
    ));
    if let Some(d) = &set.drift {
        out.push_str(&el(
            "LastDriftCheckTimestamp",
            &ts(&d.last_drift_check_timestamp),
        ));
    }
    out.push_str(&managed_execution_el(set.managed_execution_active));
    out
}

fn instance_fields(set: &StackSet, i: &StackInstance, with_overrides: bool) -> String {
    let mut out = String::new();
    out.push_str(&el("StackSetId", &set.stack_set_id));
    out.push_str(&el("Region", &i.region));
    out.push_str(&el("Account", &i.account));
    out.push_str(&opt_el("StackId", i.stack_id.as_deref()));
    if with_overrides {
        out.push_str(&parameters_el("ParameterOverrides", &i.parameter_overrides));
    }
    out.push_str(&el("Status", &i.status));
    out.push_str(&format!(
        "<StackInstanceStatus>{}</StackInstanceStatus>",
        el("DetailedStatus", &i.detailed_status)
    ));
    out.push_str(&opt_el("StatusReason", i.status_reason.as_deref()));
    out.push_str(&opt_el(
        "OrganizationalUnitId",
        i.organizational_unit_id.as_deref(),
    ));
    out.push_str(&el("DriftStatus", &i.drift_status));
    if let Some(t) = &i.last_drift_check_timestamp {
        out.push_str(&el("LastDriftCheckTimestamp", &ts(t)));
    }
    out.push_str(&opt_el("LastOperationId", i.last_operation_id.as_deref()));
    out
}

fn operation_el(set: &StackSet, op: &StackSetOperation) -> String {
    let mut out = String::new();
    out.push_str(&el("OperationId", &op.operation_id));
    out.push_str(&el("StackSetId", &set.stack_set_id));
    out.push_str(&el("Action", &op.action));
    out.push_str(&el("Status", &op.status));
    out.push_str(&preferences_el(&op.preferences));
    if let Some(retain) = op.retain_stacks {
        out.push_str(&el("RetainStacks", &retain.to_string()));
    }
    out.push_str(&opt_el(
        "AdministrationRoleARN",
        op.administration_role_arn.as_deref(),
    ));
    out.push_str(&opt_el(
        "ExecutionRoleName",
        op.execution_role_name.as_deref(),
    ));
    out.push_str(&el("CreationTimestamp", &ts(&op.created_at)));
    if let Some(t) = &op.ended_at {
        out.push_str(&el("EndTimestamp", &ts(t)));
    }
    if let Some(t) = &op.deployment_targets {
        out.push_str(&deployment_targets_el(t));
    }
    if op.drift.is_some() {
        out.push_str(&drift_details_el(op.drift.as_ref()));
    }
    out.push_str(&opt_el("StatusReason", op.status_reason.as_deref()));
    out.push_str(&status_details_el(op));
    format!("<StackSetOperation>{out}</StackSetOperation>")
}

fn operation_summary_el(op: &StackSetOperation) -> String {
    let mut out = String::new();
    out.push_str(&el("OperationId", &op.operation_id));
    out.push_str(&el("Action", &op.action));
    out.push_str(&el("Status", &op.status));
    out.push_str(&el("CreationTimestamp", &ts(&op.created_at)));
    if let Some(t) = &op.ended_at {
        out.push_str(&el("EndTimestamp", &ts(t)));
    }
    out.push_str(&opt_el("StatusReason", op.status_reason.as_deref()));
    out.push_str(&status_details_el(op));
    out.push_str(&preferences_el(&op.preferences));
    out
}

fn operation_result_el(r: &OperationResult) -> String {
    let mut out = String::new();
    out.push_str(&el("Account", &r.account));
    out.push_str(&el("Region", &r.region));
    out.push_str(&el("Status", &r.status));
    out.push_str(&opt_el("StatusReason", r.status_reason.as_deref()));
    if let Some(gate) = &r.account_gate_status {
        out.push_str(&format!(
            "<AccountGateResult>{}{}</AccountGateResult>",
            el("Status", gate),
            opt_el("StatusReason", r.account_gate_reason.as_deref())
        ));
    }
    out.push_str(&opt_el(
        "OrganizationalUnitId",
        r.organizational_unit_id.as_deref(),
    ));
    out
}

// ── Operation engine ──

/// One (account, region) an operation acts on.
#[derive(Debug, Clone)]
struct Target {
    account: String,
    region: String,
    ou: Option<String>,
    suspended: bool,
}

/// Clears a stack set's waiting-for-idle marker once its task is done, even
/// if that task is dropped.
struct RetryGuard {
    retries: std::sync::Arc<parking_lot::Mutex<BTreeSet<(String, String)>>>,
    key: (String, String),
}

impl Drop for RetryGuard {
    fn drop(&mut self) {
        self.retries.lock().remove(&self.key);
    }
}

/// Whether a stack set follows the organization: service-managed, active,
/// with auto-deployment on and somewhere to deploy to.
fn auto_deploys(set: &StackSet) -> bool {
    set.status == "ACTIVE"
        && set.permission_model == "SERVICE_MANAGED"
        && set.auto_deployment.as_ref().is_some_and(|a| a.enabled)
        && !set.auto_deployment_targets.is_empty()
}

/// How a stack set's planned auto-deployment ended.
#[derive(Debug, PartialEq)]
enum PlanOutcome {
    /// Nothing left to do for this stack set in this pass.
    Done,
    /// The stack set changed under the plan; re-derive it.
    Stale,
    /// The plan could not be finished now — another operation owns the stack
    /// set, it is still deploying, or its task died. Re-plan once the stack
    /// set is idle.
    Retry,
}

/// Preferences an auto-deployment operation runs with. Every account it
/// touches got there on its own, so one account's stack failing must not
/// cancel the accounts queued behind it the way an operator-issued operation
/// with the default zero tolerance would.
///
/// As in AWS, a tolerance that is never exceeded also means the operation
/// settles as `SUCCEEDED` even when some of its targets failed; the failures
/// are on the per-target results, which is where `ListStackSetOperationResults`
/// and each instance's status report them.
fn auto_deployment_preferences() -> OperationPreferences {
    OperationPreferences {
        failure_tolerance_percentage: Some(100),
        ..OperationPreferences::default()
    }
}

/// One operation auto-deployment has decided to run on a stack set.
struct PlannedDeployment {
    action_name: &'static str,
    action: TargetAction,
    retain_stacks: Option<bool>,
    targets: Vec<Target>,
}

/// How far up the OU tree a lookup walks before giving up on a cycle. Shared
/// by `accounts_under` and `account_coverage`, which have to agree: one
/// decides what a deployment covers, the other what auto-deployment tears
/// down, so a walk that reached different ancestors would delete instances it
/// had just created.
const MAX_OU_DEPTH: usize = 16;

/// What an operation does to each target.
#[derive(Debug, Clone)]
enum TargetAction {
    /// Create the instance (or re-deploy it, when it already exists) with
    /// these overrides.
    Create { overrides: BTreeMap<String, String> },
    /// Re-deploy the instance's stack. `None` keeps the instance's overrides.
    Update {
        overrides: Option<Vec<OverrideSpec>>,
    },
    Delete {
        retain_stacks: bool,
        /// A DeleteStackInstances the operator asked for, as opposed to one
        /// auto-deployment ran because the account left the OU. The former is
        /// a decision to remember (`auto_deployment_excluded`); the latter is
        /// just the organization changing.
        user_requested: bool,
    },
}

#[derive(Debug, Clone)]
enum OverrideSpec {
    Value(String, String),
    UsePrevious(String),
}

#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Succeeded,
    Running,
    Failed(String),
    Cancelled(String),
    SkippedSuspended,
}

struct GateResult {
    status: &'static str,
    reason: Option<String>,
}

/// The stack-set fields an operation deploys, captured when it starts.
#[derive(Clone)]
struct DeploySpec {
    name: String,
    template_body: String,
    parameters: BTreeMap<String, String>,
    capabilities: Vec<String>,
    tags: Vec<(String, String)>,
}

impl DeploySpec {
    fn of(set: &StackSet) -> Self {
        Self {
            name: set.name.clone(),
            template_body: set.template_body.clone(),
            parameters: set.parameters.clone(),
            capabilities: set.capabilities.clone(),
            tags: set.tags.clone(),
        }
    }

    fn stack_params(&self, overrides: &BTreeMap<String, String>) -> Vec<(String, String)> {
        let mut merged = self.parameters.clone();
        merged.extend(overrides.iter().map(|(k, v)| (k.clone(), v.clone())));
        let mut out = vec![("TemplateBody".to_string(), self.template_body.clone())];
        for (i, (k, v)) in merged.iter().enumerate() {
            out.push((
                format!("Parameters.member.{}.ParameterKey", i + 1),
                k.clone(),
            ));
            out.push((
                format!("Parameters.member.{}.ParameterValue", i + 1),
                v.clone(),
            ));
        }
        for (i, cap) in self.capabilities.iter().enumerate() {
            out.push((format!("Capabilities.member.{}", i + 1), cap.clone()));
        }
        for (i, (k, v)) in self.tags.iter().enumerate() {
            out.push((format!("Tags.member.{}.Key", i + 1), k.clone()));
            out.push((format!("Tags.member.{}.Value", i + 1), v.clone()));
        }
        out
    }
}

/// Map a stack's status after an instance operation to that target's outcome.
fn stack_outcome(action: &str, status: &str, reason: Option<&str>) -> Outcome {
    if status.ends_with("_IN_PROGRESS") {
        return Outcome::Running;
    }
    let ok = match action {
        "DELETE" => status == "DELETE_COMPLETE",
        _ => matches!(
            status,
            "CREATE_COMPLETE" | "UPDATE_COMPLETE" | "IMPORT_COMPLETE"
        ),
    };
    if ok {
        Outcome::Succeeded
    } else {
        Outcome::Failed(
            reason
                .map(str::to_string)
                .unwrap_or_else(|| format!("Stack is in {status} state")),
        )
    }
}

fn result_status(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Succeeded => "SUCCEEDED",
        Outcome::Running => "RUNNING",
        Outcome::Failed(_) => "FAILED",
        Outcome::Cancelled(_) | Outcome::SkippedSuspended => "CANCELLED",
    }
}

fn outcome_reason(outcome: &Outcome) -> Option<String> {
    match outcome {
        Outcome::Failed(r) | Outcome::Cancelled(r) => Some(r.clone()),
        Outcome::SkippedSuspended => Some("Account is suspended".to_string()),
        _ => None,
    }
}

/// Apply an outcome to the instance it concerns.
fn apply_to_instance(instance: &mut StackInstance, outcome: &Outcome) {
    let (status, detailed) = match outcome {
        Outcome::Succeeded => ("CURRENT", "SUCCEEDED"),
        Outcome::Running => ("OUTDATED", "RUNNING"),
        Outcome::Failed(_) => ("OUTDATED", "FAILED"),
        Outcome::Cancelled(_) => ("OUTDATED", "CANCELLED"),
        Outcome::SkippedSuspended => ("OUTDATED", "SKIPPED_SUSPENDED_ACCOUNT"),
    };
    instance.status = status.to_string();
    instance.detailed_status = detailed.to_string();
    instance.status_reason = outcome_reason(outcome);
}

/// Allowed failures per region before an operation stops.
fn region_tolerance(prefs: &OperationPreferences, accounts_in_region: usize) -> usize {
    match (
        prefs.failure_tolerance_count,
        prefs.failure_tolerance_percentage,
    ) {
        (Some(count), _) => count as usize,
        (None, Some(pct)) => accounts_in_region * pct as usize / 100,
        (None, None) => 0,
    }
}

/// Final status of an operation none of whose targets are still running.
fn settled_status(op: &StackSetOperation) -> &'static str {
    if op.status == "STOPPING" || op.status == "STOPPED" {
        return "STOPPED";
    }
    let mut per_region: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for r in &op.results {
        let entry = per_region.entry(r.region.as_str()).or_default();
        entry.0 += 1;
        if r.status == "FAILED" {
            entry.1 += 1;
        }
    }
    let exceeded = per_region
        .values()
        .any(|(total, failed)| *failed > region_tolerance(&op.preferences, *total));
    if exceeded {
        "FAILED"
    } else {
        "SUCCEEDED"
    }
}

fn settle_operation(op: &mut StackSetOperation) {
    if !matches!(op.status.as_str(), "RUNNING" | "STOPPING") {
        return;
    }
    if op
        .results
        .iter()
        .any(|r| matches!(r.status.as_str(), "RUNNING" | "PENDING"))
    {
        return;
    }
    op.status = settled_status(op).to_string();
    op.ended_at = Some(Utc::now());
}

/// Order targets the way the operation deploys them: `RegionOrder` first,
/// then the remaining regions in request order.
fn order_targets(
    mut targets: Vec<Target>,
    regions: &[String],
    prefs: &OperationPreferences,
) -> Vec<Target> {
    let mut order: Vec<&String> = prefs.region_order.iter().collect();
    for r in regions {
        if !order.contains(&r) {
            order.push(r);
        }
    }
    targets.sort_by_key(|t| {
        order
            .iter()
            .position(|r| **r == t.region)
            .unwrap_or(usize::MAX)
    });
    targets
}

/// Show the instances an operation is about to deploy as `OUTDATED` /
/// `PENDING` from the moment it is recorded, creating the records of new
/// ones, so they are listable while the deployment runs.
fn mark_instances_pending(set: &mut StackSet, targets: &[Target], op_id: &str, create: bool) {
    for target in targets {
        let position = set
            .instances
            .iter()
            .position(|i| i.account == target.account && i.region == target.region);
        let idx = match position {
            Some(idx) => idx,
            None if create => {
                set.instances.push(StackInstance {
                    account: target.account.clone(),
                    region: target.region.clone(),
                    stack_id: None,
                    status: "OUTDATED".to_string(),
                    detailed_status: "PENDING".to_string(),
                    status_reason: None,
                    parameter_overrides: BTreeMap::new(),
                    organizational_unit_id: target.ou.clone(),
                    drift_status: "NOT_CHECKED".to_string(),
                    last_drift_check_timestamp: None,
                    last_operation_id: None,
                });
                set.instances.len() - 1
            }
            None => continue,
        };
        let instance = &mut set.instances[idx];
        instance.status = "OUTDATED".to_string();
        instance.detailed_status = "PENDING".to_string();
        instance.status_reason = None;
        instance.last_operation_id = Some(op_id.to_string());
    }
}

/// The overrides an instance carries after an update: explicit values, plus
/// `UsePreviousValue` entries carried over from the instance. `None` keeps the
/// instance's overrides as they are.
fn resolve_update_overrides(
    specs: Option<&[OverrideSpec]>,
    existing: Option<&StackInstance>,
) -> BTreeMap<String, String> {
    let previous = existing
        .map(|i| i.parameter_overrides.clone())
        .unwrap_or_default();
    match specs {
        None => previous,
        Some(specs) => specs
            .iter()
            .filter_map(|s| match s {
                OverrideSpec::Value(k, v) => Some((k.clone(), v.clone())),
                OverrideSpec::UsePrevious(k) => previous.get(k).map(|v| (k.clone(), v.clone())),
            })
            .collect(),
    }
}

/// A PENDING result for every target an operation will act on.
fn pending_results(targets: &[Target]) -> Vec<OperationResult> {
    targets
        .iter()
        .map(|t| OperationResult {
            account: t.account.clone(),
            region: t.region.clone(),
            status: "PENDING".to_string(),
            status_reason: None,
            organizational_unit_id: t.ou.clone(),
            account_gate_status: None,
            account_gate_reason: None,
        })
        .collect()
}

fn synthetic_request(
    account: &str,
    region: &str,
    action: &str,
    request_id: &str,
    params: Vec<(String, String)>,
) -> AwsRequest {
    let mut query: std::collections::HashMap<String, String> = params.into_iter().collect();
    query.insert("Action".to_string(), action.to_string());
    AwsRequest {
        service: "cloudformation".to_string(),
        action: action.to_string(),
        region: region.to_string(),
        account_id: account.to_string(),
        request_id: request_id.to_string(),
        headers: http::HeaderMap::new(),
        query_params: query,
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

/// The accounts in (or below) an OU, or the whole organization for the root.
fn accounts_under(
    org: &fakecloud_organizations::OrganizationState,
    ou: &str,
) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    for account in org.accounts.values() {
        if account.id == org.management_account_id {
            // Service-managed stack sets never deploy to the management account.
            continue;
        }
        let mut parent = account.parent_id.clone();
        let mut depth = 0;
        loop {
            if parent == ou {
                out.push((account.id.clone(), account.status != "ACTIVE"));
                break;
            }
            match org.ous.get(&parent) {
                Some(p) if depth < MAX_OU_DEPTH => {
                    parent = p.parent_id.clone();
                    depth += 1;
                }
                _ => break,
            }
        }
    }
    out
}

impl CloudFormationService {
    pub(crate) async fn handle_stack_set_action(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let params = Self::get_all_params(req);
        match req.action.as_str() {
            "CreateStackSet" => self.create_stack_set(req, &params),
            "DescribeStackSet" => self.describe_stack_set(req, &params),
            "ListStackSets" => self.list_stack_sets(req, &params),
            "UpdateStackSet" => self.update_stack_set(req, &params).await,
            "DeleteStackSet" => self.delete_stack_set(req, &params),
            "CreateStackInstances" => self.create_stack_instances(req, &params).await,
            "UpdateStackInstances" => self.update_stack_instances(req, &params).await,
            "DeleteStackInstances" => self.delete_stack_instances(req, &params).await,
            "DescribeStackInstance" => self.describe_stack_instance(req, &params),
            "ListStackInstances" => self.list_stack_instances(req, &params),
            "DescribeStackSetOperation" => self.describe_stack_set_operation(req, &params),
            "ListStackSetOperations" => self.list_stack_set_operations(req, &params),
            "ListStackSetOperationResults" => self.list_stack_set_operation_results(req, &params),
            "StopStackSetOperation" => self.stop_stack_set_operation(req, &params),
            "ImportStacksToStackSet" => self.import_stacks_to_stack_set(req, &params),
            "ListStackSetAutoDeploymentTargets" => {
                self.list_stack_set_auto_deployment_targets(req, &params)
            }
            "DetectStackSetDrift" => self.detect_stack_set_drift(req, &params),
            "ListStackInstanceResourceDrifts" => {
                self.list_stack_instance_resource_drifts(req, &params)
            }
            other => Err(validation(format!("Unsupported stack set action {other}"))),
        }
    }

    /// The account whose stack sets a call addresses. `CallAs=DELEGATED_ADMIN`
    /// lets a registered StackSets delegated administrator act on the
    /// organization's service-managed stack sets, which live in the
    /// management account.
    fn stack_set_admin_account(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<String, AwsServiceError> {
        self.stack_set_admin_account_of(&req.account_id, params)
    }

    pub(crate) fn stack_set_admin_account_of(
        &self,
        caller: &str,
        params: &BTreeMap<String, String>,
    ) -> Result<String, AwsServiceError> {
        match params.get("CallAs").map(String::as_str) {
            None | Some("SELF") => Ok(caller.to_string()),
            Some("DELEGATED_ADMIN") => {
                let orgs = self.deps.organizations.read();
                let org = orgs.org_of_account(caller).ok_or_else(|| {
                    validation("AWS Organizations is not enabled for this account")
                })?;
                let registered = org
                    .delegated_administrators
                    .get(STACKSETS_PRINCIPAL)
                    .is_some_and(|admins| admins.contains_key(caller));
                if !registered {
                    return Err(validation(format!(
                        "Account {} is not registered as a delegated administrator for {STACKSETS_PRINCIPAL}",
                        caller
                    )));
                }
                Ok(org.management_account_id.clone())
            }
            Some(other) => Err(validation(format!("Invalid value {other} for CallAs"))),
        }
    }

    /// Service-managed stack sets need an organization with StackSets trusted
    /// access, administered from its management account.
    fn check_service_managed_allowed(&self, admin: &str) -> Result<(), AwsServiceError> {
        let trusted_in_org = {
            let orgs = self.deps.organizations.read();
            let org = orgs
                .org_of_account(admin)
                .ok_or_else(|| validation("AWS Organizations is not enabled for this account"))?;
            if org.management_account_id != admin {
                return Err(validation(
                    "Service managed stack sets can only be administered from the organization's management account or a delegated administrator",
                ));
            }
            org.trusted_services.contains_key(STACKSETS_PRINCIPAL)
        };
        let activated = self
            .state
            .read()
            .get(admin)
            .is_some_and(|s| s.orgs_access_enabled);
        if trusted_in_org || activated {
            Ok(())
        } else {
            Err(validation(
                "You must enable organizations access to operate a service managed stack set",
            ))
        }
    }

    // ── Stack sets ──

    fn create_stack_set(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let permission_model = params
            .get("PermissionModel")
            .cloned()
            .unwrap_or_else(|| "SELF_MANAGED".to_string());
        let service_managed = permission_model == "SERVICE_MANAGED";
        if Scope::of(params) == Scope::DelegatedAdmin && !service_managed {
            return Err(validation(
                "A delegated administrator can only create stack sets with SERVICE_MANAGED permission model",
            ));
        }
        if service_managed {
            self.check_service_managed_allowed(&admin)?;
        }

        // A stack set can be created from an existing stack, which is how
        // ImportStacksToStackSet adoption starts.
        let (template_body, mut parameters) = match params.get("StackId") {
            Some(stack_id) => {
                crate::service::resolve_stack_ref(stack_id, &admin, &req.region)?;
                let accounts = self.state.read();
                let stack = accounts
                    .regional(&admin, &req.region)
                    .and_then(|s| {
                        s.stacks
                            .values()
                            .find(|st| &st.stack_id == stack_id && st.status != "DELETE_COMPLETE")
                    })
                    .ok_or_else(|| {
                        validation(format!("Stack with id {stack_id} does not exist"))
                    })?;
                let params: BTreeMap<String, String> = stack
                    .parameters
                    .iter()
                    .filter(|(k, _)| !k.starts_with("AWS::"))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                (stack.template.clone(), params)
            }
            None => (
                self.stack_set_template_body(&req.account_id, params)
                    .map_err(validation)?
                    .unwrap_or_default(),
                BTreeMap::new(),
            ),
        };
        parameters.extend(resolve_parameters(
            &parameter_list(params, "Parameters"),
            &BTreeMap::new(),
        )?);

        let auto_deployment = Self::parse_auto_deployment(params, service_managed, None)?;
        let managed_execution_active =
            parse_bool(params, "ManagedExecution.Active")?.unwrap_or(false);
        let (administration_role_arn, execution_role_name) = if service_managed {
            (None, None)
        } else {
            (
                Some(
                    params
                        .get("AdministrationRoleARN")
                        .cloned()
                        .unwrap_or_else(|| {
                            format!(
                                "arn:{}:iam::{admin}:role/{DEFAULT_ADMIN_ROLE}",
                                partition_for(&req.region)
                            )
                        }),
                ),
                Some(
                    params
                        .get("ExecutionRoleName")
                        .cloned()
                        .unwrap_or_else(|| DEFAULT_EXECUTION_ROLE.to_string()),
                ),
            )
        };

        let id = format!("{name}:{}", uuid::Uuid::new_v4());
        let arn = format!(
            "arn:{}:cloudformation:{}:{admin}:stackset/{id}",
            partition_for(&req.region),
            req.region
        );
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&admin, &req.region);
        if find_active(state, &name, Scope::Own).is_some() {
            return Err(aws_err(
                StatusCode::CONFLICT,
                "NameAlreadyExistsException",
                format!("StackSet {name} already exists"),
            ));
        }
        state.stack_sets.insert(
            id.clone(),
            StackSet {
                stack_set_id: id.clone(),
                name,
                arn,
                status: "ACTIVE".to_string(),
                description: params.get("Description").cloned(),
                template_body,
                parameters,
                capabilities: member_list(params, "Capabilities"),
                tags: tag_list(params),
                administration_role_arn,
                execution_role_name,
                permission_model,
                auto_deployment,
                auto_deployment_targets: BTreeMap::new(),
                auto_deployment_excluded: BTreeSet::new(),
                managed_execution_active,
                instances: Vec::new(),
                operations: Vec::new(),
                drift: None,
                created_at: Utc::now(),
            },
        );
        Ok(xml_response(
            "CreateStackSet",
            el("StackSetId", &id),
            &req.request_id,
        ))
    }

    fn parse_auto_deployment(
        params: &BTreeMap<String, String>,
        service_managed: bool,
        previous: Option<&AutoDeployment>,
    ) -> Result<Option<AutoDeployment>, AwsServiceError> {
        let enabled = parse_bool(params, "AutoDeployment.Enabled")?;
        let retain = parse_bool(params, "AutoDeployment.RetainStacksOnAccountRemoval")?;
        if enabled.is_none() && retain.is_none() {
            // Auto-deployment only exists for service-managed stack sets, so a
            // switch to SELF_MANAGED drops it.
            return Ok(previous.filter(|_| service_managed).cloned());
        }
        if !service_managed {
            return Err(validation(
                "AutoDeployment is only supported for stack sets with SERVICE_MANAGED permission model",
            ));
        }
        let enabled = enabled.or(previous.map(|p| p.enabled)).unwrap_or(false);
        // Retention only means something while auto-deployment is on, so
        // turning it off does not carry the previous setting along.
        let retain = retain
            .or(previous
                .filter(|_| enabled)
                .map(|p| p.retain_stacks_on_account_removal))
            .unwrap_or(false);
        if retain && !enabled {
            return Err(validation(
                "RetainStacksOnAccountRemoval can only be set when AutoDeployment is enabled",
            ));
        }
        Ok(Some(AutoDeployment {
            enabled,
            retain_stacks_on_account_removal: retain,
        }))
    }

    /// Fold asynchronously-provisioning stacks' current status into the
    /// instances and operations of one stack set.
    fn refresh_stack_set(
        accounts: &mut MultiAccountState<CloudFormationAccountState>,
        admin: &str,
        set_id: &str,
    ) {
        let running: Vec<(usize, String, String, String, Option<String>)> =
            match accounts.get(admin).and_then(|s| s.stack_set(set_id)) {
                Some(set) => set
                    .instances
                    .iter()
                    .enumerate()
                    .filter(|(_, i)| i.detailed_status == "RUNNING")
                    .filter_map(|(idx, i)| {
                        Some((
                            idx,
                            i.account.clone(),
                            i.region.clone(),
                            i.stack_id.clone()?,
                            i.last_operation_id.clone(),
                        ))
                    })
                    .collect(),
                None => return,
            };
        let mut outcomes = Vec::new();
        for (idx, account, region, stack_id, op_id) in running {
            let Some((status, reason)) = accounts.regional(&account, &region).and_then(|s| {
                s.stacks
                    .values()
                    .find(|st| st.stack_id == stack_id)
                    .map(|st| (st.status.clone(), st.status_reason.clone()))
            }) else {
                outcomes.push((
                    idx,
                    op_id,
                    Outcome::Failed(format!("Stack {stack_id} does not exist")),
                ));
                continue;
            };
            let action = if status.starts_with("UPDATE") {
                "UPDATE"
            } else {
                "CREATE"
            };
            let outcome = stack_outcome(action, &status, reason.as_deref());
            if outcome != Outcome::Running {
                outcomes.push((idx, op_id, outcome));
            }
        }
        let Some(set) = accounts
            .get_mut(admin)
            .and_then(|s| s.stack_set_mut(set_id))
        else {
            return;
        };
        for (idx, op_id, outcome) in outcomes {
            let (account, region) = {
                let instance = &mut set.instances[idx];
                apply_to_instance(instance, &outcome);
                (instance.account.clone(), instance.region.clone())
            };
            if let Some(op) = op_id
                .as_deref()
                .and_then(|id| set.operations.iter_mut().find(|o| o.operation_id == id))
            {
                if let Some(result) = op
                    .results
                    .iter_mut()
                    .find(|r| r.account == account && r.region == region)
                {
                    result.status = result_status(&outcome).to_string();
                    result.status_reason = outcome_reason(&outcome);
                }
            }
        }
        for op in &mut set.operations {
            settle_operation(op);
        }
    }

    /// Resolve the stack set for a read, after folding in async progress.
    fn read_stack_set(
        &self,
        admin: &str,
        region: &str,
        name_or_id: &str,
        scope: Scope,
    ) -> Result<StackSet, AwsServiceError> {
        let mut accounts = self.state.write();
        let id = accounts
            .regional(admin, region)
            .and_then(|s| find_for_read(s, name_or_id, scope))
            .map(|s| s.stack_set_id.clone())
            .ok_or_else(|| stack_set_not_found(name_or_id))?;
        Self::refresh_stack_set(&mut accounts, admin, &id);
        accounts
            .get(admin)
            .and_then(|s| s.stack_set(&id))
            .cloned()
            .ok_or_else(|| stack_set_not_found(name_or_id))
    }

    fn describe_stack_set(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let set = self.read_stack_set(&admin, &req.region, &name, Scope::of(params))?;
        Ok(xml_response(
            "DescribeStackSet",
            stack_set_el(&set),
            &req.request_id,
        ))
    }

    fn list_stack_sets(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let admin = self.stack_set_admin_account(req, params)?;
        let wanted = params.get("Status");
        let mut sets: Vec<StackSet> = self
            .state
            .read()
            .regional(&admin, &req.region)
            .map(|s| {
                s.stack_sets
                    .values()
                    .filter(|set| wanted.is_none_or(|w| &set.status == w))
                    .filter(|set| Scope::of(params).sees(set))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        sets.sort_by_key(|a| a.created_at);
        let (page, next) = paginate(sets, params)?;
        let inner = format!(
            "{}{}",
            list_el("Summaries", page.iter().map(stack_set_summary_el)),
            next_token_el(next)
        );
        Ok(xml_response("ListStackSets", inner, &req.request_id))
    }

    /// Reject a new operation on a stack set that already has one running, and
    /// a caller-supplied operation id that was used before.
    /// `check_can_start_operation` for DetectStackSetDrift, which models no
    /// OperationIdAlreadyExistsException: a reused id is an invalid operation.
    fn check_can_start_drift(set: &StackSet, op_id: &str) -> Result<(), AwsServiceError> {
        Self::check_can_start_operation(set, op_id).map_err(|e| {
            if e.code() == "OperationIdAlreadyExistsException" {
                aws_err(
                    StatusCode::BAD_REQUEST,
                    "InvalidOperationException",
                    e.message(),
                )
            } else {
                e
            }
        })
    }

    fn check_can_start_operation(set: &StackSet, op_id: &str) -> Result<(), AwsServiceError> {
        if set
            .operations
            .iter()
            .any(|o| matches!(o.status.as_str(), "RUNNING" | "STOPPING"))
        {
            return Err(aws_err(
                StatusCode::CONFLICT,
                "OperationInProgressException",
                format!(
                    "Another Operation on StackSet {} is in progress",
                    set.stack_set_id
                ),
            ));
        }
        if set.operations.iter().any(|o| o.operation_id == op_id) {
            return Err(aws_err(
                StatusCode::CONFLICT,
                "OperationIdAlreadyExistsException",
                format!("Operation {op_id} already exists"),
            ));
        }
        Ok(())
    }

    fn new_operation(
        set: &StackSet,
        op_id: &str,
        action: &str,
        preferences: OperationPreferences,
        deployment_targets: Option<DeploymentTargets>,
        retain_stacks: Option<bool>,
    ) -> StackSetOperation {
        StackSetOperation {
            operation_id: op_id.to_string(),
            action: action.to_string(),
            status: "RUNNING".to_string(),
            status_reason: None,
            retain_stacks,
            preferences,
            deployment_targets,
            administration_role_arn: set.administration_role_arn.clone(),
            execution_role_name: set.execution_role_name.clone(),
            created_at: Utc::now(),
            ended_at: None,
            results: Vec::new(),
            drift: None,
            resource_drifts: Vec::new(),
        }
    }

    async fn update_stack_set(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let preferences = parse_preferences(params)?;
        let op_id = params
            .get("OperationId")
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        // Resolved before the state lock: a TemplateURL read takes the S3 lock.
        let new_template = self
            .stack_set_template_body(&req.account_id, params)
            .map_err(validation)?;
        let use_previous_template = parse_bool(params, "UsePreviousTemplate")?.unwrap_or(false);
        if use_previous_template && new_template.is_some() {
            return Err(validation(
                "UsePreviousTemplate cannot be specified together with TemplateBody or TemplateURL",
            ));
        }
        let explicit_regions = member_list(params, "Regions");
        let explicit_accounts = member_list(params, "Accounts");
        let deployment_targets = parse_deployment_targets(params);

        // Build the updated definition and resolve targets against a snapshot,
        // without holding the CloudFormation lock while Organizations and S3
        // are read. The snapshot is re-validated under the lock below.
        let snapshot = self.active_snapshot(&admin, &req.region, &name, Scope::of(params))?;
        Self::check_can_start_operation(&snapshot, &op_id)?;
        let mut updated = snapshot.clone();
        if let Some(body) = new_template {
            updated.template_body = body;
        }
        let entries = parameter_list(params, "Parameters");
        if !entries.is_empty() {
            updated.parameters = resolve_parameters(&entries, &snapshot.parameters)?;
        }
        if let Some(description) = params.get("Description") {
            updated.description = Some(description.clone());
        }
        if list_present(params, "Capabilities") {
            updated.capabilities = member_list(params, "Capabilities");
        }
        if list_present(params, "Tags") {
            updated.tags = tag_list(params);
        }
        if let Some(model) = params.get("PermissionModel") {
            if *model != snapshot.permission_model {
                if !snapshot.instances.is_empty() {
                    return Err(validation(
                        "PermissionModel cannot be changed for a stack set that has stack instances",
                    ));
                }
                updated.permission_model = model.clone();
            }
        }
        let service_managed = updated.permission_model == "SERVICE_MANAGED";
        if Scope::of(params) == Scope::DelegatedAdmin && !service_managed {
            return Err(validation(
                "A delegated administrator can only manage stack sets with SERVICE_MANAGED permission model",
            ));
        }
        if service_managed && snapshot.permission_model != "SERVICE_MANAGED" {
            self.check_service_managed_allowed(&admin)?;
        }
        if service_managed {
            updated.administration_role_arn = None;
            updated.execution_role_name = None;
        } else {
            if let Some(role) = params.get("AdministrationRoleARN") {
                updated.administration_role_arn = Some(role.clone());
            }
            if let Some(role) = params.get("ExecutionRoleName") {
                updated.execution_role_name = Some(role.clone());
            }
            updated.administration_role_arn.get_or_insert_with(|| {
                format!(
                    "arn:{}:iam::{admin}:role/{DEFAULT_ADMIN_ROLE}",
                    partition_of(&snapshot.arn)
                )
            });
            updated
                .execution_role_name
                .get_or_insert_with(|| DEFAULT_EXECUTION_ROLE.to_string());
        }
        updated.auto_deployment = Self::parse_auto_deployment(
            params,
            service_managed,
            snapshot.auto_deployment.as_ref(),
        )?;
        if !service_managed {
            // A self-managed stack set has no OU targets to follow.
            updated.auto_deployment_targets.clear();
            updated.auto_deployment_excluded.clear();
        }
        if let Some(active) = parse_bool(params, "ManagedExecution.Active")? {
            updated.managed_execution_active = active;
        }

        // Which instances this update re-deploys: those named by
        // Accounts/DeploymentTargets + Regions, or all of them.
        let targeted = !explicit_accounts.is_empty() || deployment_targets.is_some();
        if targeted && explicit_regions.is_empty() {
            return Err(validation(
                "Regions must be specified when Accounts or DeploymentTargets are specified",
            ));
        }
        if !targeted && !explicit_regions.is_empty() {
            return Err(validation(
                "Accounts or DeploymentTargets must be specified when Regions are specified",
            ));
        }
        let (targets, regions) = if targeted {
            let targets = self.existing_instance_targets(
                &updated,
                &req.account_id,
                &explicit_accounts,
                deployment_targets.as_ref(),
                &explicit_regions,
                true,
            )?;
            (targets, explicit_regions.clone())
        } else {
            let suspended = self.suspended_accounts_for(&updated, &admin);
            let targets = updated
                .instances
                .iter()
                .map(|i| Target {
                    account: i.account.clone(),
                    region: i.region.clone(),
                    ou: i.organizational_unit_id.clone(),
                    suspended: suspended.contains(&i.account),
                })
                .collect();
            (targets, stack_set_regions(&updated))
        };
        // Instances left out of a partial update fall behind the new stack
        // set definition.
        for instance in &mut updated.instances {
            if !targets
                .iter()
                .any(|t| t.account == instance.account && t.region == instance.region)
            {
                instance.status = "OUTDATED".to_string();
            }
        }
        let targets = order_targets(targets, &regions, &preferences);
        let record =
            targeted.then(|| targets_record(&explicit_accounts, deployment_targets.as_ref()));
        let mut op = Self::new_operation(&updated, &op_id, "UPDATE", preferences, record, None);
        op.results = pending_results(&targets);
        updated.operations.push(op);
        mark_instances_pending(&mut updated, &targets, &op_id, false);
        let spec = DeploySpec::of(&updated);
        let set_id = updated.stack_set_id.clone();
        {
            let mut accounts = self.state.write();
            Self::refresh_stack_set(&mut accounts, &admin, &set_id);
            let current = accounts
                .get(&admin)
                .and_then(|s| s.stack_set(&set_id))
                .filter(|s| s.status == "ACTIVE")
                .ok_or_else(|| stack_set_not_found(&name))?;
            Self::check_not_stale(current, &snapshot)?;
            Self::check_can_start_operation(current, &op_id)?;
            // Auto-deployment bookkeeping is written without recording an
            // operation (an instance re-attributed to the OU it moved into, a
            // stale exclusion dropped), so `check_not_stale` cannot see it.
            // Carry the live values over the snapshot this update was built
            // from, unless the update itself cleared them.
            if updated.permission_model == "SERVICE_MANAGED" {
                updated
                    .auto_deployment_targets
                    .clone_from(&current.auto_deployment_targets);
                updated
                    .auto_deployment_excluded
                    .clone_from(&current.auto_deployment_excluded);
                for instance in &mut updated.instances {
                    if let Some(live) = current
                        .instances
                        .iter()
                        .find(|i| i.account == instance.account && i.region == instance.region)
                    {
                        instance
                            .organizational_unit_id
                            .clone_from(&live.organizational_unit_id);
                    }
                }
            }
            if let Some(slot) = accounts
                .get_mut(&admin)
                .and_then(|s| s.stack_set_mut(&set_id))
            {
                *slot = updated;
            }
        }

        self.launch_operation(
            req,
            &admin,
            &set_id,
            &op_id,
            &spec,
            targets,
            TargetAction::Update { overrides: None },
        )
        .await;
        Ok(xml_response(
            "UpdateStackSet",
            el("OperationId", &op_id),
            &req.request_id,
        ))
    }

    /// A clone of the ACTIVE stack set `name`, with async progress folded in.
    fn active_snapshot(
        &self,
        admin: &str,
        region: &str,
        name: &str,
        scope: Scope,
    ) -> Result<StackSet, AwsServiceError> {
        let mut accounts = self.state.write();
        let set_id = accounts
            .regional(admin, region)
            .and_then(|s| active_key(s, name, scope))
            .ok_or_else(|| stack_set_not_found(name))?;
        Self::refresh_stack_set(&mut accounts, admin, &set_id);
        accounts
            .get(admin)
            .and_then(|s| s.stack_set(&set_id))
            .cloned()
            .ok_or_else(|| stack_set_not_found(name))
    }

    /// Reject a request planned against a snapshot that another operation has
    /// since changed.
    fn check_not_stale(current: &StackSet, snapshot: &StackSet) -> Result<(), AwsServiceError> {
        if current.operations.len() != snapshot.operations.len() {
            return Err(aws_err(
                StatusCode::CONFLICT,
                "StaleRequestException",
                format!(
                    "Another operation has been performed on StackSet {} since this request was made",
                    current.stack_set_id
                ),
            ));
        }
        Ok(())
    }

    fn delete_stack_set(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let mut accounts = self.state.write();
        // DeleteStackSet declares no not-found error; deleting a stack set that
        // does not exist is a no-op.
        let Some(set_id) = accounts
            .regional(&admin, &req.region)
            .and_then(|s| active_key(s, &name, Scope::of(params)))
        else {
            return Ok(xml_response_no_result("DeleteStackSet", &req.request_id));
        };
        Self::refresh_stack_set(&mut accounts, &admin, &set_id);
        let Some(set) = accounts
            .get_mut(&admin)
            .and_then(|s| s.stack_set_mut(&set_id))
        else {
            return Ok(xml_response_no_result("DeleteStackSet", &req.request_id));
        };
        if set
            .operations
            .iter()
            .any(|o| matches!(o.status.as_str(), "RUNNING" | "STOPPING"))
        {
            return Err(aws_err(
                StatusCode::CONFLICT,
                "OperationInProgressException",
                format!("Another Operation on StackSet {set_id} is in progress"),
            ));
        }
        if !set.instances.is_empty() {
            return Err(aws_err(
                StatusCode::CONFLICT,
                "StackSetNotEmptyException",
                format!("StackSet {name} is not empty"),
            ));
        }
        set.status = "DELETED".to_string();
        Ok(xml_response_no_result("DeleteStackSet", &req.request_id))
    }

    // ── Stack instances ──

    /// Resolve the targets of CreateStackInstances from the request.
    fn resolve_new_targets(
        &self,
        set: &StackSet,
        caller: &str,
        accounts: &[String],
        deployment_targets: Option<&DeploymentTargets>,
        regions: &[String],
    ) -> Result<Vec<Target>, AwsServiceError> {
        if regions.is_empty() {
            return Err(validation("Regions is required"));
        }
        if !accounts.is_empty() && deployment_targets.is_some() {
            return Err(validation(
                "Only one of Accounts or DeploymentTargets can be specified",
            ));
        }
        let mut resolved: Vec<(String, Option<String>, bool)> = Vec::new();
        if set.permission_model == "SERVICE_MANAGED" {
            if !accounts.is_empty() {
                return Err(validation(
                    "StackSets with SERVICE_MANAGED permission model can only have OrganizationalUnit as target",
                ));
            }
            let dt = deployment_targets
                .filter(|t| !t.organizational_unit_ids.is_empty())
                .ok_or_else(|| {
                    validation("DeploymentTargets.OrganizationalUnitIds is required for SERVICE_MANAGED stack sets")
                })?;
            let filter_accounts = self.target_accounts_list(caller, dt)?;
            let filter = self.account_filter_type(dt, &filter_accounts)?;
            let orgs = self.deps.organizations.read();
            let org = orgs
                .org_of_account(caller)
                .ok_or_else(|| validation("AWS Organizations is not enabled for this account"))?;
            let mut seen = BTreeSet::new();
            for ou in &dt.organizational_unit_ids {
                if *ou != org.root_id && !org.ous.contains_key(ou) {
                    return Err(validation(format!(
                        "OrganizationalUnit {ou} does not exist"
                    )));
                }
                for (account, suspended) in accounts_under(org, ou) {
                    let listed = filter_accounts.contains(&account);
                    let keep = match filter.as_str() {
                        "INTERSECTION" => listed,
                        "DIFFERENCE" => !listed,
                        _ => true,
                    };
                    if keep && seen.insert(account.clone()) {
                        resolved.push((account, Some(ou.clone()), suspended));
                    }
                }
            }
            if filter == "UNION" {
                for account in filter_accounts {
                    if account != org.management_account_id && seen.insert(account.clone()) {
                        let suspended = org
                            .accounts
                            .get(&account)
                            .is_some_and(|a| a.status != "ACTIVE");
                        resolved.push((account, None, suspended));
                    }
                }
            }
        } else {
            let list = match deployment_targets {
                Some(dt) => {
                    if !dt.organizational_unit_ids.is_empty() {
                        return Err(validation(
                            "OrganizationalUnitIds are only supported for stack sets with SERVICE_MANAGED permission model",
                        ));
                    }
                    self.target_accounts_list(caller, dt)?
                }
                None => accounts.to_vec(),
            };
            if list.is_empty() {
                return Err(validation(
                    "Accounts or DeploymentTargets must be specified",
                ));
            }
            if let Some(bad) = list.iter().find(|a| !is_account_id(a)) {
                return Err(validation(format!(
                    "Account {bad} is not a valid AWS account id"
                )));
            }
            let mut seen = BTreeSet::new();
            for account in list {
                if seen.insert(account.clone()) {
                    resolved.push((account, None, false));
                }
            }
        }
        let mut targets = Vec::new();
        let mut seen_regions = BTreeSet::new();
        for region in regions.iter().filter(|r| seen_regions.insert(*r)) {
            for (account, ou, suspended) in &resolved {
                targets.push(Target {
                    account: account.clone(),
                    region: region.clone(),
                    ou: ou.clone(),
                    suspended: *suspended,
                });
            }
        }
        Ok(targets)
    }

    /// Organization member accounts that are not ACTIVE. Existing instances in
    /// them are skipped again rather than failed. Scoped to `admin`'s own
    /// organization — another organization's suspended accounts are none of
    /// this stack set's business.
    fn suspended_accounts_for(&self, set: &StackSet, admin: &str) -> BTreeSet<String> {
        // Only service-managed stack sets deploy through the organization;
        // a self-managed one targets accounts directly.
        if set.permission_model != "SERVICE_MANAGED" {
            return BTreeSet::new();
        }
        self.deps
            .organizations
            .read()
            .org_of_account(admin)
            .map(|org| {
                org.accounts
                    .values()
                    .filter(|a| a.status != "ACTIVE")
                    .map(|a| a.id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `DeploymentTargets.Accounts` plus the accounts listed in
    /// `DeploymentTargets.AccountsUrl`, validated.
    fn target_accounts_list(
        &self,
        caller: &str,
        dt: &DeploymentTargets,
    ) -> Result<Vec<String>, AwsServiceError> {
        let mut list = dt.accounts.clone();
        if let Some(url) = &dt.accounts_url {
            if !looks_like_url(url) {
                return Err(validation(format!(
                    "AccountsUrl {url} is not a valid S3 URL"
                )));
            }
            let body = self
                .resolve_template_url(caller, url)
                .map_err(|_| validation(format!("Unable to read the accounts file at {url}")))?;
            list.extend(
                body.split([',', '\n', '\r'])
                    .map(str::trim)
                    .filter(|a| !a.is_empty())
                    .map(str::to_string),
            );
        }
        if let Some(bad) = list.iter().find(|a| !is_account_id(a)) {
            return Err(validation(format!(
                "Account {bad} is not a valid AWS account id"
            )));
        }
        Ok(list)
    }

    fn account_filter_type(
        &self,
        dt: &DeploymentTargets,
        filter_accounts: &[String],
    ) -> Result<String, AwsServiceError> {
        let filter = match (&dt.account_filter_type, filter_accounts.is_empty()) {
            (Some(f), _) => f.clone(),
            // Accounts next to OUs without an explicit filter narrow the OUs.
            (None, false) => "INTERSECTION".to_string(),
            (None, true) => "NONE".to_string(),
        };
        match filter.as_str() {
            "NONE" if !filter_accounts.is_empty() => Err(validation(
                "AccountFilterType NONE cannot be used together with Accounts",
            )),
            "INTERSECTION" | "DIFFERENCE" | "UNION" if filter_accounts.is_empty() => {
                Err(validation(format!(
                    "Accounts must be specified when AccountFilterType is {filter}"
                )))
            }
            "NONE" | "INTERSECTION" | "DIFFERENCE" | "UNION" => Ok(filter),
            other => Err(validation(format!("Invalid AccountFilterType {other}"))),
        }
    }

    /// Resolve targets that must name existing instances (UpdateStackInstances,
    /// DeleteStackInstances, a partial UpdateStackSet).
    fn existing_instance_targets(
        &self,
        set: &StackSet,
        caller: &str,
        accounts: &[String],
        deployment_targets: Option<&DeploymentTargets>,
        regions: &[String],
        must_exist: bool,
    ) -> Result<Vec<Target>, AwsServiceError> {
        if regions.is_empty() {
            return Err(validation("Regions is required"));
        }
        if !accounts.is_empty() && deployment_targets.is_some() {
            return Err(validation(
                "Only one of Accounts or DeploymentTargets can be specified",
            ));
        }
        let suspended = self.suspended_accounts_for(set, caller);
        let mut targets = Vec::new();
        let mut push = |instance: &StackInstance| {
            if !targets
                .iter()
                .any(|t: &Target| t.account == instance.account && t.region == instance.region)
            {
                targets.push(Target {
                    account: instance.account.clone(),
                    region: instance.region.clone(),
                    ou: instance.organizational_unit_id.clone(),
                    suspended: suspended.contains(&instance.account),
                });
            }
        };
        if set.permission_model == "SERVICE_MANAGED" {
            if !accounts.is_empty() {
                return Err(validation(
                    "StackSets with SERVICE_MANAGED permission model can only have OrganizationalUnit as target",
                ));
            }
            let dt = deployment_targets
                .filter(|t| !t.organizational_unit_ids.is_empty())
                .ok_or_else(|| {
                    validation("DeploymentTargets.OrganizationalUnitIds is required for SERVICE_MANAGED stack sets")
                })?;
            let filter_accounts = self.target_accounts_list(caller, dt)?;
            let filter = self.account_filter_type(dt, &filter_accounts)?;
            // An OU covers every OU nested below it, so an instance deployed
            // through a child OU is reached through its parent or the root,
            // and one deployed through a parent is reached through a child.
            // The OU recorded on the instance still counts, for an account
            // that has since moved out.
            let accounts_in_ous: BTreeSet<String> = self
                .deps
                .organizations
                .read()
                .org_of_account(caller)
                .map(|org| {
                    dt.organizational_unit_ids
                        .iter()
                        .flat_map(|ou| accounts_under(org, ou))
                        .map(|(account, _)| account)
                        .collect()
                })
                .unwrap_or_default();
            for region in regions {
                for instance in set.instances.iter().filter(|i| &i.region == region) {
                    let in_ou = accounts_in_ous.contains(&instance.account)
                        || instance
                            .organizational_unit_id
                            .as_ref()
                            .is_some_and(|ou| dt.organizational_unit_ids.contains(ou));
                    let listed = filter_accounts.contains(&instance.account);
                    let keep = match filter.as_str() {
                        "INTERSECTION" => in_ou && listed,
                        "DIFFERENCE" => in_ou && !listed,
                        "UNION" => in_ou || listed,
                        _ => in_ou,
                    };
                    if keep {
                        push(instance);
                    }
                }
            }
        } else {
            let list = match deployment_targets {
                Some(dt) => {
                    if !dt.organizational_unit_ids.is_empty() {
                        return Err(validation(
                            "OrganizationalUnitIds are only supported for stack sets with SERVICE_MANAGED permission model",
                        ));
                    }
                    self.target_accounts_list(caller, dt)?
                }
                None => accounts.to_vec(),
            };
            if list.is_empty() {
                return Err(validation(
                    "Accounts or DeploymentTargets must be specified",
                ));
            }
            if let Some(bad) = list.iter().find(|a| !is_account_id(a)) {
                return Err(validation(format!(
                    "Account {bad} is not a valid AWS account id"
                )));
            }
            for region in regions {
                for account in &list {
                    match set
                        .instances
                        .iter()
                        .find(|i| &i.account == account && &i.region == region)
                    {
                        Some(instance) => push(instance),
                        None if must_exist => {
                            return Err(instance_not_found(&set.name, account, region))
                        }
                        None => {}
                    }
                }
            }
        }
        Ok(targets)
    }

    /// Validate that overrides only touch parameters the template declares.
    fn check_overrides_declared(
        set: &StackSet,
        keys: impl Iterator<Item = String>,
    ) -> Result<(), AwsServiceError> {
        let Ok(template) = fakecloud_core::cfn_template::parse_template_body(&set.template_body)
        else {
            return Ok(());
        };
        let declared: BTreeSet<String> = template
            .get("Parameters")
            .and_then(Value::as_object)
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();
        for key in keys {
            if !declared.contains(&key) && !set.parameters.contains_key(&key) {
                return Err(validation(format!(
                    "Parameter {key} is not declared in the stack set template"
                )));
            }
        }
        Ok(())
    }

    /// Record a new instance operation, re-validating under the lock the
    /// snapshot its targets were resolved against.
    #[allow(clippy::too_many_arguments)]
    fn start_instance_operation(
        &self,
        admin: &str,
        snapshot: &StackSet,
        targets: &[Target],
        op_id: &str,
        action: &str,
        preferences: OperationPreferences,
        deployment_targets: Option<DeploymentTargets>,
        retain_stacks: Option<bool>,
    ) -> Result<DeploySpec, AwsServiceError> {
        let mut accounts = self.state.write();
        Self::refresh_stack_set(&mut accounts, admin, &snapshot.stack_set_id);
        let set = accounts
            .get_mut(admin)
            .and_then(|s| s.stack_set_mut(&snapshot.stack_set_id))
            .filter(|s| s.status == "ACTIVE")
            .ok_or_else(|| stack_set_not_found(&snapshot.name))?;
        Self::check_not_stale(set, snapshot)?;
        Self::check_can_start_operation(set, op_id)?;
        let mut op = Self::new_operation(
            set,
            op_id,
            action,
            preferences,
            deployment_targets,
            retain_stacks,
        );
        // Seeded in the same locked step that records the operation: a read
        // before the deployment task starts must not see a RUNNING operation
        // with nothing left to run and settle it.
        op.results = pending_results(targets);
        set.operations.push(op);
        match action {
            "CREATE" => mark_instances_pending(set, targets, op_id, true),
            "UPDATE" => mark_instances_pending(set, targets, op_id, false),
            _ => {}
        }
        Ok(DeploySpec::of(set))
    }

    async fn create_stack_instances(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let regions = member_list(params, "Regions");
        let accounts = member_list(params, "Accounts");
        let deployment_targets = parse_deployment_targets(params);
        let preferences = parse_preferences(params)?;
        let op_id = params
            .get("OperationId")
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let overrides = resolve_parameters(
            &parameter_list(params, "ParameterOverrides"),
            &BTreeMap::new(),
        )?;

        // Target resolution reads Organizations (and possibly S3) state, so it
        // runs against a snapshot of the stack set before the operation is
        // recorded under the CloudFormation lock.
        let snapshot = self.active_snapshot(&admin, &req.region, &name, Scope::of(params))?;
        Self::check_overrides_declared(&snapshot, overrides.keys().cloned())?;
        let targets = self.resolve_new_targets(
            &snapshot,
            &req.account_id,
            &accounts,
            deployment_targets.as_ref(),
            &regions,
        )?;
        let targets = order_targets(targets, &regions, &preferences);
        let record = Some(targets_record(&accounts, deployment_targets.as_ref()));
        let spec = self.start_instance_operation(
            &admin,
            &snapshot,
            &targets,
            &op_id,
            "CREATE",
            preferences,
            record,
            None,
        )?;
        let set_id = snapshot.stack_set_id.clone();
        self.note_created_targets(&snapshot, &admin, deployment_targets.as_ref(), &regions);
        self.note_create_exclusions(
            &snapshot,
            &admin,
            deployment_targets.as_ref(),
            &targets,
            &regions,
        );
        self.launch_operation(
            req,
            &admin,
            &set_id,
            &op_id,
            &spec,
            targets,
            TargetAction::Create { overrides },
        )
        .await;
        Ok(xml_response(
            "CreateStackInstances",
            el("OperationId", &op_id),
            &req.request_id,
        ))
    }

    async fn update_stack_instances(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let regions = member_list(params, "Regions");
        let accounts = member_list(params, "Accounts");
        let deployment_targets = parse_deployment_targets(params);
        let preferences = parse_preferences(params)?;
        let op_id = params
            .get("OperationId")
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let overrides = list_present(params, "ParameterOverrides").then(|| {
            parameter_list(params, "ParameterOverrides")
                .into_iter()
                .map(|e| match (e.value, e.use_previous) {
                    (Some(v), false) => Ok(OverrideSpec::Value(e.key, v)),
                    (None, true) => Ok(OverrideSpec::UsePrevious(e.key)),
                    (Some(_), true) => Err(validation(format!(
                        "Invalid input for parameter key {}. Cannot specify usePreviousValue as true and a parameter value at the same time",
                        e.key
                    ))),
                    (None, false) => Err(validation(format!(
                        "Invalid input for parameter key {}. Need to specify either usePreviousValue as true or a value for the parameter",
                        e.key
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()
        });
        let overrides = overrides.transpose()?;

        let snapshot = self.active_snapshot(&admin, &req.region, &name, Scope::of(params))?;
        if let Some(specs) = &overrides {
            Self::check_overrides_declared(
                &snapshot,
                specs.iter().map(|s| match s {
                    OverrideSpec::Value(k, _) | OverrideSpec::UsePrevious(k) => k.clone(),
                }),
            )?;
        }
        let targets = self.existing_instance_targets(
            &snapshot,
            &req.account_id,
            &accounts,
            deployment_targets.as_ref(),
            &regions,
            true,
        )?;
        let targets = order_targets(targets, &regions, &preferences);
        let record = Some(targets_record(&accounts, deployment_targets.as_ref()));
        let spec = self.start_instance_operation(
            &admin,
            &snapshot,
            &targets,
            &op_id,
            "UPDATE",
            preferences,
            record,
            None,
        )?;
        let set_id = snapshot.stack_set_id.clone();
        self.launch_operation(
            req,
            &admin,
            &set_id,
            &op_id,
            &spec,
            targets,
            TargetAction::Update { overrides },
        )
        .await;
        Ok(xml_response(
            "UpdateStackInstances",
            el("OperationId", &op_id),
            &req.request_id,
        ))
    }

    async fn delete_stack_instances(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let retain_stacks = parse_bool(params, "RetainStacks")?
            .ok_or_else(|| validation("RetainStacks is required"))?;
        let regions = member_list(params, "Regions");
        let accounts = member_list(params, "Accounts");
        let deployment_targets = parse_deployment_targets(params);
        let preferences = parse_preferences(params)?;
        let op_id = params
            .get("OperationId")
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let snapshot = self.active_snapshot(&admin, &req.region, &name, Scope::of(params))?;
        let targets = self.existing_instance_targets(
            &snapshot,
            &req.account_id,
            &accounts,
            deployment_targets.as_ref(),
            &regions,
            false,
        )?;
        let targets = order_targets(targets, &regions, &preferences);
        let record = Some(targets_record(&accounts, deployment_targets.as_ref()));
        let spec = self.start_instance_operation(
            &admin,
            &snapshot,
            &targets,
            &op_id,
            "DELETE",
            preferences,
            record,
            Some(retain_stacks),
        )?;
        let set_id = snapshot.stack_set_id.clone();
        self.launch_operation(
            req,
            &admin,
            &set_id,
            &op_id,
            &spec,
            targets,
            TargetAction::Delete {
                retain_stacks,
                user_requested: true,
            },
        )
        .await;
        Ok(xml_response(
            "DeleteStackInstances",
            el("OperationId", &op_id),
            &req.request_id,
        ))
    }

    /// Start a recorded operation's deployment.
    ///
    /// On the server (a multi-thread runtime) it runs as a detached task and
    /// the call returns the OperationId straight away, as AWS does: callers
    /// poll DescribeStackSetOperation. That also means a client that gives up
    /// on its request (a read timeout behind a slow account gate or a
    /// container-backed stack) cannot abandon the operation half way and leave
    /// the stack set blocked behind it. Current-thread runtimes (unit tests)
    /// run it inline.
    #[allow(clippy::too_many_arguments)]
    async fn launch_operation(
        &self,
        req: &AwsRequest,
        admin: &str,
        set_id: &str,
        op_id: &str,
        spec: &DeploySpec,
        targets: Vec<Target>,
        action: TargetAction,
    ) {
        let multi_thread = matches!(
            tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()),
            Ok(tokio::runtime::RuntimeFlavor::MultiThread)
        );
        if !multi_thread {
            self.run_operation(&req.request_id, admin, set_id, op_id, spec, targets, action)
                .await;
            return;
        }
        let svc = self.clone();
        let (request_id, admin, set_id, op_id, spec) = (
            req.request_id.clone(),
            admin.to_string(),
            set_id.to_string(),
            op_id.to_string(),
            spec.clone(),
        );
        tokio::spawn(async move {
            svc.run_operation(&request_id, &admin, &set_id, &op_id, &spec, targets, action)
                .await;
            // The request that started the operation has long since persisted
            // its snapshot; persist the finished operation too.
            svc.save_snapshot().await;
        });
    }

    /// Run an operation's targets in order, recording each outcome as it
    /// lands, honoring the failure tolerance and StopStackSetOperation.
    #[allow(clippy::too_many_arguments)]
    async fn run_operation(
        &self,
        request_id: &str,
        admin: &str,
        set_id: &str,
        op_id: &str,
        spec: &DeploySpec,
        targets: Vec<Target>,
        action: TargetAction,
    ) {
        let prefs = {
            let accounts = self.state.read();
            accounts
                .get(admin)
                .and_then(|s| s.stack_set(set_id))
                .and_then(|set| set.operations.iter().find(|o| o.operation_id == op_id))
                .map(|o| o.preferences.clone())
                .unwrap_or_default()
        };
        let mut region_sizes: BTreeMap<String, usize> = BTreeMap::new();
        for t in &targets {
            *region_sizes.entry(t.region.clone()).or_default() += 1;
        }
        let mut region_failures: BTreeMap<String, usize> = BTreeMap::new();
        let mut abort: Option<&'static str> = None;

        for target in targets {
            // Claiming the target marks it RUNNING under the lock, so a
            // StopStackSetOperation that lands while it deploys cannot settle
            // the operation as STOPPED underneath it.
            if abort.is_none() && !self.claim_target(admin, set_id, op_id, &target) {
                abort = Some(OPERATION_STOPPED);
            }
            let mut gate = None;
            let mut stack_id = None;
            let mut overrides = None;
            let outcome = if let Some(reason) = abort {
                Outcome::Cancelled(reason.to_string())
            } else if target.suspended && !matches!(action, TargetAction::Delete { .. }) {
                // A suspended account is skipped for deployments, but its
                // instance can still be removed.
                Outcome::SkippedSuspended
            } else {
                let g = self.account_gate(&target.account, &target.region).await;
                let passed = g.status != "FAILED";
                let gate_reason = g.reason.clone();
                gate = Some(g);
                if passed {
                    let (outcome, id, applied) = self
                        .apply_target(request_id, admin, set_id, spec, &target, &action)
                        .await;
                    // A stack still provisioning in the background (custom
                    // resources) is waited on, so its failure counts against
                    // the tolerance before the next target deploys.
                    let outcome = match (&outcome, &id) {
                        (Outcome::Running, Some(stack_id)) => {
                            // Record the stack first, so the instance points
                            // at it while it provisions and across a restart.
                            self.record_outcome(
                                admin,
                                set_id,
                                op_id,
                                &target,
                                &action,
                                &Outcome::Running,
                                None,
                                id.clone(),
                                applied.clone(),
                            );
                            self.await_stack(&target.account, &target.region, stack_id)
                                .await
                        }
                        _ => outcome,
                    };
                    stack_id = id;
                    overrides = applied;
                    outcome
                } else {
                    Outcome::Failed(
                        gate_reason.unwrap_or_else(|| "Account gate check failed".to_string()),
                    )
                }
            };
            // A target that did not deploy still takes on the overrides it was
            // asked for, so a later redeploy uses them.
            if overrides.is_none() {
                overrides = match &action {
                    TargetAction::Create { overrides } => Some(overrides.clone()),
                    TargetAction::Update { overrides } => Some(resolve_update_overrides(
                        overrides.as_deref(),
                        self.instance_stack(admin, set_id, &target).as_ref(),
                    )),
                    TargetAction::Delete { .. } => None,
                };
            }
            if matches!(outcome, Outcome::Failed(_)) {
                let failures = region_failures.entry(target.region.clone()).or_default();
                *failures += 1;
                let size = region_sizes.get(&target.region).copied().unwrap_or(0);
                if *failures > region_tolerance(&prefs, size) {
                    abort = Some(TOLERANCE_EXCEEDED);
                }
            }
            self.record_outcome(
                admin, set_id, op_id, &target, &action, &outcome, gate, stack_id, overrides,
            );
        }

        let mut accounts = self.state.write();
        if let Some(op) = accounts
            .get_mut(admin)
            .and_then(|s| s.stack_set_mut(set_id))
            .and_then(|set| set.operations.iter_mut().find(|o| o.operation_id == op_id))
        {
            settle_operation(op);
        }
    }

    /// Wait for a stack that provisions in the background to reach a terminal
    /// status. Gives up after `STACK_WAIT_LIMIT`, leaving the target RUNNING
    /// for a later read of the stack set to settle.
    async fn await_stack(&self, account: &str, region: &str, stack_id: &str) -> Outcome {
        let deadline = tokio::time::Instant::now() + STACK_WAIT_LIMIT;
        loop {
            let outcome = match self.stack_status(account, region, stack_id) {
                Some((_, status, reason)) => {
                    let action = if status.starts_with("UPDATE") {
                        "UPDATE"
                    } else {
                        "CREATE"
                    };
                    stack_outcome(action, &status, reason.as_deref())
                }
                None => Outcome::Failed(format!("Stack [{stack_id}] does not exist")),
            };
            if outcome != Outcome::Running || tokio::time::Instant::now() >= deadline {
                return outcome;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    /// Mark a target's result RUNNING before it deploys. Returns false when
    /// the operation has been stopped, in which case the target must not run.
    fn claim_target(&self, admin: &str, set_id: &str, op_id: &str, target: &Target) -> bool {
        let mut accounts = self.state.write();
        let Some(op) = accounts
            .get_mut(admin)
            .and_then(|s| s.stack_set_mut(set_id))
            .and_then(|set| set.operations.iter_mut().find(|o| o.operation_id == op_id))
        else {
            return false;
        };
        if op.status != "RUNNING" {
            return false;
        }
        if let Some(result) = op
            .results
            .iter_mut()
            .find(|r| r.account == target.account && r.region == target.region)
        {
            result.status = "RUNNING".to_string();
        }
        true
    }

    /// Run the account's `AWSCloudFormationStackSetAccountGate` Lambda, if it
    /// has one. A deployment proceeds only when the function answers
    /// `SUCCEEDED`; an account without the function is not gated.
    async fn account_gate(&self, account: &str, region: &str) -> GateResult {
        let arn = format!(
            "arn:{}:lambda:{region}:{account}:function:{ACCOUNT_GATE_FUNCTION}",
            partition_for(region)
        );
        let exists = self
            .deps
            .lambda
            .read()
            .get(account)
            .is_some_and(|s| s.functions.contains_key(ACCOUNT_GATE_FUNCTION));
        if !exists {
            return GateResult {
                status: "SKIPPED",
                reason: Some(format!("Function not found: {arn}")),
            };
        }
        match self.deps.delivery.invoke_lambda(&arn, "{}").await {
            None => GateResult {
                status: "SKIPPED",
                reason: Some("Lambda invocation is not available".to_string()),
            },
            Some(Err(e)) => GateResult {
                status: "FAILED",
                reason: Some(format!("Account gate function invocation failed: {e}")),
            },
            Some(Ok(bytes)) => {
                let status = serde_json::from_slice::<Value>(&bytes)
                    .ok()
                    .and_then(|v| v.get("Status").and_then(Value::as_str).map(str::to_string));
                match status.as_deref() {
                    Some("SUCCEEDED") => GateResult {
                        status: "SUCCEEDED",
                        reason: None,
                    },
                    other => GateResult {
                        status: "FAILED",
                        reason: Some(format!(
                            "Account gate function returned {}",
                            other.unwrap_or("an invalid response")
                        )),
                    },
                }
            }
        }
    }

    fn instance_stack(&self, admin: &str, set_id: &str, target: &Target) -> Option<StackInstance> {
        self.state
            .read()
            .get(admin)
            .and_then(|s| s.stack_set(set_id))
            .and_then(|set| {
                set.instances
                    .iter()
                    .find(|i| i.account == target.account && i.region == target.region)
                    .cloned()
            })
    }

    fn stack_status(
        &self,
        account: &str,
        region: &str,
        stack_id_or_name: &str,
    ) -> Option<(String, String, Option<String>)> {
        self.state.read().regional(account, region).and_then(|s| {
            s.stacks
                .values()
                .filter(|st| st.stack_id == stack_id_or_name || st.name == stack_id_or_name)
                .max_by_key(|st| st.created_at)
                .map(|st| {
                    (
                        st.stack_id.clone(),
                        st.status.clone(),
                        st.status_reason.clone(),
                    )
                })
        })
    }

    /// Deploy one target. Returns its outcome, the instance's stack id, and
    /// the overrides the instance now carries.
    async fn apply_target(
        &self,
        request_id: &str,
        admin: &str,
        set_id: &str,
        spec: &DeploySpec,
        target: &Target,
        action: &TargetAction,
    ) -> (Outcome, Option<String>, Option<BTreeMap<String, String>>) {
        let existing = self.instance_stack(admin, set_id, target);
        let live_stack = existing
            .as_ref()
            .and_then(|i| i.stack_id.as_deref())
            .and_then(|id| self.stack_status(&target.account, &target.region, id))
            .filter(|(_, status, _)| status != "DELETE_COMPLETE");

        match action {
            TargetAction::Delete { retain_stacks, .. } => {
                let Some((stack_id, _, _)) = live_stack else {
                    return (Outcome::Succeeded, None, None);
                };
                if *retain_stacks {
                    return (Outcome::Succeeded, Some(stack_id), None);
                }
                let request = synthetic_request(
                    &target.account,
                    &target.region,
                    "DeleteStack",
                    request_id,
                    vec![("StackName".to_string(), stack_id.clone())],
                );
                if let Err(e) = self.delete_stack(&request).await {
                    return (Outcome::Failed(e.message()), Some(stack_id), None);
                }
                let outcome = match self.stack_status(&target.account, &target.region, &stack_id) {
                    Some((_, status, reason)) => {
                        stack_outcome("DELETE", &status, reason.as_deref())
                    }
                    None => Outcome::Succeeded,
                };
                (outcome, Some(stack_id), None)
            }
            TargetAction::Create { overrides } => match live_stack {
                Some((stack_id, _, _)) => {
                    let (outcome, id) = self
                        .update_instance_stack(request_id, spec, target, &stack_id, overrides)
                        .await;
                    (outcome, id, Some(overrides.clone()))
                }
                None => {
                    let (outcome, id) = self
                        .create_instance_stack(request_id, spec, target, overrides)
                        .await;
                    (outcome, id, Some(overrides.clone()))
                }
            },
            TargetAction::Update { overrides } => {
                let resolved = resolve_update_overrides(overrides.as_deref(), existing.as_ref());
                let Some((stack_id, _, _)) = live_stack else {
                    // An instance that never got a stack (skipped, cancelled,
                    // or its create failed) is deployed now. One whose stack
                    // was deleted out from under it fails, as in AWS.
                    return match existing.and_then(|i| i.stack_id) {
                        Some(missing) => (
                            Outcome::Failed(format!("Stack [{missing}] does not exist")),
                            None,
                            Some(resolved),
                        ),
                        None => {
                            let (outcome, id) = self
                                .create_instance_stack(request_id, spec, target, &resolved)
                                .await;
                            (outcome, id, Some(resolved))
                        }
                    };
                };
                let (outcome, id) = self
                    .update_instance_stack(request_id, spec, target, &stack_id, &resolved)
                    .await;
                (outcome, id, Some(resolved))
            }
        }
    }

    async fn create_instance_stack(
        &self,
        request_id: &str,
        spec: &DeploySpec,
        target: &Target,
        overrides: &BTreeMap<String, String>,
    ) -> (Outcome, Option<String>) {
        let stack_name = format!(
            "StackSet-{}-{}",
            spec.name.replace(':', "-"),
            uuid::Uuid::new_v4()
        );
        let mut stack_params = spec.stack_params(overrides);
        stack_params.push(("StackName".to_string(), stack_name.clone()));
        let request = synthetic_request(
            &target.account,
            &target.region,
            "CreateStack",
            request_id,
            stack_params,
        );
        if let Err(e) = self.create_stack(&request).await {
            return (Outcome::Failed(e.message()), None);
        }
        match self.stack_status(&target.account, &target.region, &stack_name) {
            Some((stack_id, status, reason)) => (
                stack_outcome("CREATE", &status, reason.as_deref()),
                Some(stack_id),
            ),
            None => (
                Outcome::Failed(format!("Stack {stack_name} was not created")),
                None,
            ),
        }
    }

    async fn update_instance_stack(
        &self,
        request_id: &str,
        spec: &DeploySpec,
        target: &Target,
        stack_id: &str,
        overrides: &BTreeMap<String, String>,
    ) -> (Outcome, Option<String>) {
        let mut stack_params = spec.stack_params(overrides);
        stack_params.push(("StackName".to_string(), stack_id.to_string()));
        let request = synthetic_request(
            &target.account,
            &target.region,
            "UpdateStack",
            request_id,
            stack_params,
        );
        if let Err(e) = self.update_stack(&request).await {
            return (Outcome::Failed(e.message()), Some(stack_id.to_string()));
        }
        let outcome = match self.stack_status(&target.account, &target.region, stack_id) {
            Some((_, status, reason)) => stack_outcome("UPDATE", &status, reason.as_deref()),
            None => Outcome::Failed(format!("Stack [{stack_id}] does not exist")),
        };
        (outcome, Some(stack_id.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    fn record_outcome(
        &self,
        admin: &str,
        set_id: &str,
        op_id: &str,
        target: &Target,
        action: &TargetAction,
        outcome: &Outcome,
        gate: Option<GateResult>,
        stack_id: Option<String>,
        overrides: Option<BTreeMap<String, String>>,
    ) {
        let mut accounts = self.state.write();
        let Some(set) = accounts
            .get_mut(admin)
            .and_then(|s| s.stack_set_mut(set_id))
        else {
            return;
        };
        // Once the operation has settled (stopped, and settled by a read
        // while this loop was still going) a newer operation may own the
        // instances; a late outcome must not write over them.
        if !set
            .operations
            .iter()
            .any(|o| o.operation_id == op_id && matches!(o.status.as_str(), "RUNNING" | "STOPPING"))
        {
            return;
        }
        if let Some(result) = set
            .operations
            .iter_mut()
            .find(|o| o.operation_id == op_id)
            .and_then(|op| {
                op.results
                    .iter_mut()
                    .find(|r| r.account == target.account && r.region == target.region)
            })
        {
            result.status = result_status(outcome).to_string();
            result.status_reason = outcome_reason(outcome);
            if let Some(gate) = gate {
                result.account_gate_status = Some(gate.status.to_string());
                result.account_gate_reason = gate.reason;
            }
        }

        let position = set
            .instances
            .iter()
            .position(|i| i.account == target.account && i.region == target.region);
        match (action, position) {
            (TargetAction::Delete { user_requested, .. }, Some(idx)) => {
                if *outcome == Outcome::Succeeded {
                    // An instance the operator removed stays removed: record
                    // it so auto-deployment does not put it back while the
                    // account is still in the target OU, and stop following
                    // the OU in that region once it holds nothing. Both are
                    // recorded here, on the instance that actually went away,
                    // so a delete that failed changes neither.
                    let removed = set.instances.remove(idx);
                    if *user_requested && set.permission_model == "SERVICE_MANAGED" {
                        if let Some(ou) = &removed.organizational_unit_id {
                            set.auto_deployment_excluded.insert((
                                ou.clone(),
                                removed.account.clone(),
                                removed.region.clone(),
                            ));
                            let still_deployed = set.instances.iter().any(|i| {
                                i.organizational_unit_id.as_deref() == Some(ou.as_str())
                                    && i.region == removed.region
                            });
                            if !still_deployed {
                                if let Some(regions) = set.auto_deployment_targets.get_mut(ou) {
                                    regions.remove(&removed.region);
                                    if regions.is_empty() {
                                        set.auto_deployment_targets.remove(ou);
                                    }
                                }
                            }
                        }
                    }
                } else if !matches!(outcome, Outcome::Cancelled(_)) {
                    let instance = &mut set.instances[idx];
                    apply_to_instance(instance, outcome);
                    // A stack that could not be deleted leaves the instance
                    // INOPERABLE, as in AWS.
                    if matches!(outcome, Outcome::Failed(_)) {
                        instance.status = "INOPERABLE".to_string();
                    }
                    instance.last_operation_id = Some(op_id.to_string());
                }
            }
            (TargetAction::Delete { .. }, None) => {}
            (_, position) => {
                let idx = match position {
                    Some(idx) => idx,
                    None => {
                        set.instances.push(StackInstance {
                            account: target.account.clone(),
                            region: target.region.clone(),
                            stack_id: None,
                            status: "OUTDATED".to_string(),
                            detailed_status: "PENDING".to_string(),
                            status_reason: None,
                            parameter_overrides: BTreeMap::new(),
                            organizational_unit_id: target.ou.clone(),
                            drift_status: "NOT_CHECKED".to_string(),
                            last_drift_check_timestamp: None,
                            last_operation_id: None,
                        });
                        set.instances.len() - 1
                    }
                };
                let instance = &mut set.instances[idx];
                apply_to_instance(instance, outcome);
                instance.last_operation_id = Some(op_id.to_string());
                if stack_id.is_some() {
                    instance.stack_id = stack_id;
                }
                if let Some(overrides) = overrides {
                    instance.parameter_overrides = overrides;
                }
                if target.ou.is_some() {
                    instance.organizational_unit_id = target.ou.clone();
                }
            }
        }
    }

    fn describe_stack_instance(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let account = required(params, "StackInstanceAccount")?;
        let region = required(params, "StackInstanceRegion")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let set = self.read_stack_set(&admin, &req.region, &name, Scope::of(params))?;
        let instance = set
            .instances
            .iter()
            .find(|i| i.account == account && i.region == region)
            .ok_or_else(|| instance_not_found(&name, &account, &region))?;
        let inner = format!(
            "<StackInstance>{}</StackInstance>",
            instance_fields(&set, instance, true)
        );
        Ok(xml_response(
            "DescribeStackInstance",
            inner,
            &req.request_id,
        ))
    }

    fn list_stack_instances(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let set = self.read_stack_set(&admin, &req.region, &name, Scope::of(params))?;
        let mut filters: Vec<(String, String)> = Vec::new();
        for i in 1.. {
            let Some(filter_name) = params.get(&format!("Filters.member.{i}.Name")) else {
                break;
            };
            let value = params
                .get(&format!("Filters.member.{i}.Values"))
                .cloned()
                .unwrap_or_default();
            if !matches!(
                filter_name.as_str(),
                "DETAILED_STATUS" | "LAST_OPERATION_ID" | "DRIFT_STATUS"
            ) {
                return Err(validation(format!("Invalid filter name {filter_name}")));
            }
            filters.push((filter_name.clone(), value));
        }
        let account = params.get("StackInstanceAccount");
        let region = params.get("StackInstanceRegion");
        let matching: Vec<&StackInstance> = set
            .instances
            .iter()
            .filter(|i| account.is_none_or(|a| &i.account == a))
            .filter(|i| region.is_none_or(|r| &i.region == r))
            .filter(|i| {
                filters.iter().all(|(name, value)| match name.as_str() {
                    "DETAILED_STATUS" => &i.detailed_status == value,
                    "LAST_OPERATION_ID" => i.last_operation_id.as_ref() == Some(value),
                    _ => &i.drift_status == value,
                })
            })
            .collect();
        let (page, next) = paginate(matching, params)?;
        let inner = format!(
            "{}{}",
            list_el(
                "Summaries",
                page.iter().map(|i| instance_fields(&set, i, false))
            ),
            next_token_el(next)
        );
        Ok(xml_response("ListStackInstances", inner, &req.request_id))
    }

    // ── Operations ──

    fn describe_stack_set_operation(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let op_id = required(params, "OperationId")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let set = self.read_stack_set(&admin, &req.region, &name, Scope::of(params))?;
        let op = set
            .operations
            .iter()
            .find(|o| o.operation_id == op_id)
            .ok_or_else(|| operation_not_found(&op_id))?;
        Ok(xml_response(
            "DescribeStackSetOperation",
            operation_el(&set, op),
            &req.request_id,
        ))
    }

    fn list_stack_set_operations(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let set = self.read_stack_set(&admin, &req.region, &name, Scope::of(params))?;
        // Most recent first.
        let ops: Vec<&StackSetOperation> = set.operations.iter().rev().collect();
        let (page, next) = paginate(ops, params)?;
        let inner = format!(
            "{}{}",
            list_el("Summaries", page.into_iter().map(operation_summary_el)),
            next_token_el(next)
        );
        Ok(xml_response(
            "ListStackSetOperations",
            inner,
            &req.request_id,
        ))
    }

    fn list_stack_set_operation_results(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let op_id = required(params, "OperationId")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let set = self.read_stack_set(&admin, &req.region, &name, Scope::of(params))?;
        let op = set
            .operations
            .iter()
            .find(|o| o.operation_id == op_id)
            .ok_or_else(|| operation_not_found(&op_id))?;
        let mut wanted_status: Option<String> = None;
        for i in 1.. {
            let Some(filter_name) = params.get(&format!("Filters.member.{i}.Name")) else {
                break;
            };
            if filter_name != "OPERATION_RESULT_STATUS" {
                return Err(validation(format!("Invalid filter name {filter_name}")));
            }
            wanted_status = params.get(&format!("Filters.member.{i}.Values")).cloned();
        }
        let results: Vec<&OperationResult> = op
            .results
            .iter()
            .filter(|r| wanted_status.as_ref().is_none_or(|s| &r.status == s))
            .collect();
        let (page, next) = paginate(results, params)?;
        let inner = format!(
            "{}{}",
            list_el("Summaries", page.into_iter().map(operation_result_el)),
            next_token_el(next)
        );
        Ok(xml_response(
            "ListStackSetOperationResults",
            inner,
            &req.request_id,
        ))
    }

    fn stop_stack_set_operation(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let op_id = required(params, "OperationId")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let mut accounts = self.state.write();
        let set_id = accounts
            .regional(&admin, &req.region)
            .and_then(|s| find_for_read(s, &name, Scope::of(params)))
            .map(|s| s.stack_set_id.clone())
            .ok_or_else(|| stack_set_not_found(&name))?;
        Self::refresh_stack_set(&mut accounts, &admin, &set_id);
        let op = accounts
            .get_mut(&admin)
            .and_then(|s| s.stack_set_mut(&set_id))
            .and_then(|set| set.operations.iter_mut().find(|o| o.operation_id == op_id))
            .ok_or_else(|| operation_not_found(&op_id))?;
        if op.status != "RUNNING" {
            return Err(aws_err(
                StatusCode::BAD_REQUEST,
                "InvalidOperationException",
                format!(
                    "Operation {op_id} is in {} state and cannot be stopped",
                    op.status
                ),
            ));
        }
        // Targets not yet started are cancelled; ones already deploying run to
        // completion, and the operation settles as STOPPED once they do.
        op.status = "STOPPING".to_string();
        let mut cancelled = Vec::new();
        for result in &mut op.results {
            if result.status == "PENDING" {
                result.status = "CANCELLED".to_string();
                result.status_reason = Some(OPERATION_STOPPED.to_string());
                cancelled.push((result.account.clone(), result.region.clone()));
            }
        }
        settle_operation(op);
        if let Some(set) = accounts
            .get_mut(&admin)
            .and_then(|s| s.stack_set_mut(&set_id))
        {
            for instance in &mut set.instances {
                if instance.last_operation_id.as_deref() == Some(op_id.as_str())
                    && instance.detailed_status == "PENDING"
                    && cancelled
                        .iter()
                        .any(|(a, r)| *a == instance.account && *r == instance.region)
                {
                    apply_to_instance(instance, &Outcome::Cancelled(OPERATION_STOPPED.to_string()));
                }
            }
        }
        Ok(xml_response(
            "StopStackSetOperation",
            String::new(),
            &req.request_id,
        ))
    }

    // ── Import ──

    fn import_stacks_to_stack_set(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let preferences = parse_preferences(params)?;
        let op_id = params
            .get("OperationId")
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let ous = member_list(params, "OrganizationalUnitIds");
        let mut stack_ids = member_list(params, "StackIds");
        if let Some(url) = params.get("StackIdsUrl") {
            if !stack_ids.is_empty() {
                return Err(validation(
                    "Only one of StackIds or StackIdsUrl can be specified",
                ));
            }
            if !looks_like_url(url) {
                return Err(validation(format!(
                    "StackIdsUrl {url} is not a valid S3 URL"
                )));
            }
            let body = self
                .resolve_template_url(&req.account_id, url)
                .map_err(|_| validation(format!("Unable to read the stack ids file at {url}")))?;
            stack_ids = body
                .split([',', '\n', '\r'])
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
        }
        if stack_ids.is_empty() {
            return Err(validation("StackIds or StackIdsUrl must be specified"));
        }
        if stack_ids.len() > MAX_IMPORT_STACKS {
            return Err(aws_err(
                StatusCode::BAD_REQUEST,
                "LimitExceededException",
                format!("A maximum of {MAX_IMPORT_STACKS} stacks can be imported in one operation"),
            ));
        }

        // Where each stack lives, and which OU its account sits in.
        let mut located = Vec::new();
        for stack_id in &stack_ids {
            let (account, region) = stack_arn_location(stack_id)
                .ok_or_else(|| validation(format!("Invalid stack id {stack_id}")))?;
            located.push((stack_id.clone(), account, region));
        }

        // Which targeted OU each account sits in, read before the
        // CloudFormation lock is taken.
        let mut ou_of_account: BTreeMap<String, String> = BTreeMap::new();
        if !ous.is_empty() {
            let orgs = self.deps.organizations.read();
            let org = orgs
                .org_of_account(&admin)
                .ok_or_else(|| validation("AWS Organizations is not enabled for this account"))?;
            for ou in &ous {
                for (account, _) in accounts_under(org, ou) {
                    ou_of_account.entry(account).or_insert_with(|| ou.clone());
                }
            }
        }

        // What the call asked for, by OU and region, whatever each stack's
        // outcome turns out to be.
        let requested_pairs: BTreeSet<(String, String)> = located
            .iter()
            .map(|(_, account, region)| (account.clone(), region.clone()))
            .collect();
        let mut requested_by_ou: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (_, account, region) in &located {
            if let Some(ou) = ou_of_account.get(account) {
                requested_by_ou
                    .entry(ou.clone())
                    .or_default()
                    .insert(region.clone());
            }
        }

        let mut accounts = self.state.write();
        let set_id = accounts
            .regional(&admin, &req.region)
            .and_then(|s| active_key(s, &name, Scope::of(params)))
            .ok_or_else(|| stack_set_not_found(&name))?;
        Self::refresh_stack_set(&mut accounts, &admin, &set_id);
        let set = accounts
            .get(&admin)
            .and_then(|s| s.stack_set(&set_id))
            .cloned()
            .ok_or_else(|| stack_set_not_found(&name))?;
        Self::check_can_start_operation(&set, &op_id)?;
        let service_managed = set.permission_model == "SERVICE_MANAGED";
        if service_managed && ous.is_empty() {
            return Err(validation(
                "OrganizationalUnitIds is required when importing into a SERVICE_MANAGED stack set",
            ));
        }
        if !service_managed && !ous.is_empty() {
            return Err(validation(
                "OrganizationalUnitIds are only supported for stack sets with SERVICE_MANAGED permission model",
            ));
        }

        let set_template =
            fakecloud_core::cfn_template::parse_template_body(&set.template_body).ok();
        let mut op = Self::new_operation(&set, &op_id, "CREATE", preferences, None, None);
        let mut new_instances = Vec::new();
        for (stack_id, account, region) in located {
            let stack = accounts
                .regional(&account, &region)
                .and_then(|s| {
                    s.stacks
                        .values()
                        .find(|st| st.stack_id == stack_id && st.status != "DELETE_COMPLETE")
                })
                .cloned()
                .ok_or_else(|| {
                    aws_err(
                        StatusCode::NOT_FOUND,
                        "StackNotFoundException",
                        format!("Stack with id {stack_id} does not exist"),
                    )
                })?;
            let ou = if service_managed {
                let ou = ou_of_account.get(&account).cloned().ok_or_else(|| {
                    validation(format!(
                        "Account {account} of stack {stack_id} is not in the specified OrganizationalUnitIds"
                    ))
                })?;
                Some(ou)
            } else {
                None
            };
            let already_managed = accounts.iter().any(|(_, s)| {
                s.regions
                    .values()
                    .flat_map(|r| r.stack_sets.values())
                    .any(|other| {
                        other.status == "ACTIVE"
                            && other
                                .instances
                                .iter()
                                .any(|i| i.stack_id.as_deref() == Some(stack_id.as_str()))
                    })
            });
            let duplicate = set
                .instances
                .iter()
                .chain(new_instances.iter())
                .any(|i: &StackInstance| i.account == account && i.region == region);
            let template_matches = match (
                &set_template,
                fakecloud_core::cfn_template::parse_template_body(&stack.template),
            ) {
                (Some(a), Ok(b)) => *a == b,
                _ => set.template_body.trim() == stack.template.trim(),
            };
            let (result_status, reason, instance_status) = if already_managed {
                (
                    "FAILED",
                    Some(format!(
                        "Stack {stack_id} is already managed by a stack set"
                    )),
                    None,
                )
            } else if duplicate {
                (
                    "FAILED",
                    Some(format!(
                        "Stack instance for account {account} and region {region} already exists"
                    )),
                    None,
                )
            } else if !template_matches {
                (
                    "FAILED",
                    Some("The stack's template does not match the stack set template".to_string()),
                    Some(("OUTDATED", "FAILED_IMPORT")),
                )
            } else {
                ("SUCCEEDED", None, Some(("CURRENT", "SUCCEEDED")))
            };
            op.results.push(OperationResult {
                account: account.clone(),
                region: region.clone(),
                status: result_status.to_string(),
                status_reason: reason.clone(),
                organizational_unit_id: ou.clone(),
                account_gate_status: None,
                account_gate_reason: None,
            });
            if let Some((status, detailed)) = instance_status {
                let overrides = stack
                    .parameters
                    .iter()
                    .filter(|(k, v)| !k.starts_with("AWS::") && set.parameters.get(*k) != Some(*v))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                new_instances.push(StackInstance {
                    account,
                    region,
                    stack_id: Some(stack_id),
                    status: status.to_string(),
                    detailed_status: detailed.to_string(),
                    status_reason: reason,
                    parameter_overrides: overrides,
                    organizational_unit_id: ou,
                    drift_status: "NOT_CHECKED".to_string(),
                    last_drift_check_timestamp: None,
                    last_operation_id: Some(op_id.clone()),
                });
            }
        }
        op.status = settled_status(&op).to_string();
        op.ended_at = Some(Utc::now());
        let set = accounts
            .get_mut(&admin)
            .and_then(|s| s.stack_set_mut(&set_id))
            .ok_or_else(|| stack_set_not_found(&name))?;
        // Adopted instances are deployments like any other: record the OUs
        // and regions they landed in, or auto-deployment would see them as
        // outside the stack set's targets and tear them down. Only stacks
        // that were really adopted count — one refused as FAILED_IMPORT is
        // not deployed anywhere — and the accounts of the OU the operator did
        // not adopt a stack from are left out, exactly as an
        // `AccountFilterType` leaves them out of CreateStackInstances.
        if set.permission_model == "SERVICE_MANAGED" {
            let adopted: Vec<&StackInstance> = new_instances
                .iter()
                .filter(|i| i.detailed_status == "SUCCEEDED")
                .collect();
            let mut regions_by_ou: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            for instance in &adopted {
                if let Some(ou) = &instance.organizational_unit_id {
                    regions_by_ou
                        .entry(ou.clone())
                        .or_default()
                        .insert(instance.region.clone());
                }
            }
            for (ou, regions) in &regions_by_ou {
                set.auto_deployment_targets
                    .entry(ou.clone())
                    .or_default()
                    .extend(regions.iter().cloned());
            }
            // The accounts of a targeted OU the operator named no stack from
            // are the ones left out, exactly as an `AccountFilterType` leaves
            // accounts out of CreateStackInstances. Derived from what was
            // asked for rather than from what was adopted: a stack refused
            // (already managed elsewhere, or a template mismatch) was still
            // asked for, and whether any one succeeded says nothing about the
            // accounts that were never mentioned.
            let live: BTreeSet<(String, String)> = set
                .instances
                .iter()
                .map(|i| (i.account.clone(), i.region.clone()))
                .collect();
            for (ou, regions) in &requested_by_ou {
                for (account, account_ou) in &ou_of_account {
                    if account_ou != ou {
                        continue;
                    }
                    for region in regions {
                        let key = (account.clone(), region.clone());
                        if !requested_pairs.contains(&key) && !live.contains(&key) {
                            set.auto_deployment_excluded.insert((
                                ou.clone(),
                                account.clone(),
                                region.clone(),
                            ));
                        }
                    }
                }
            }
        }
        set.instances.extend(new_instances);
        set.operations.push(op);
        Ok(xml_response(
            "ImportStacksToStackSet",
            el("OperationId", &op_id),
            &req.request_id,
        ))
    }

    fn list_stack_set_auto_deployment_targets(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let set = self.read_stack_set(&admin, &req.region, &name, Scope::of(params))?;
        let by_ou = if set.permission_model == "SERVICE_MANAGED" {
            set.auto_deployment_targets.clone()
        } else {
            BTreeMap::new()
        };
        let entries: Vec<(String, BTreeSet<String>)> = by_ou.into_iter().collect();
        let (page, next) = paginate(entries, params)?;
        let inner = format!(
            "{}{}",
            list_el(
                "Summaries",
                page.iter().map(|(ou, regions)| {
                    format!(
                        "{}{}",
                        el("OrganizationalUnitId", ou),
                        scalar_list_el("Regions", regions)
                    )
                })
            ),
            next_token_el(next)
        );
        Ok(xml_response(
            "ListStackSetAutoDeploymentTargets",
            inner,
            &req.request_id,
        ))
    }

    // ── Auto-deployment ──

    /// How long a deployment deferred behind another operation keeps waiting
    /// for the stack set to go idle before giving up on this round. Outlasts
    /// `STACK_WAIT_LIMIT`, so an operation waiting on a stack that provisions
    /// in the background is waited out rather than abandoned.
    const AUTO_DEPLOYMENT_MAX_WAIT: std::time::Duration =
        std::time::Duration::from_secs(STACK_WAIT_LIMIT.as_secs() + 60);
    /// How soon it first re-checks while waiting, and the longest it lets
    /// that interval grow to.
    const AUTO_DEPLOYMENT_POLL: std::time::Duration = std::time::Duration::from_millis(250);
    const AUTO_DEPLOYMENT_MAX_POLL: std::time::Duration = std::time::Duration::from_secs(5);
    /// How long the Organizations call that triggered a deployment waits for
    /// it before handing the rest over to the background. Deployments are
    /// near-instant unless a template provisions something real, so in
    /// practice the caller sees the whole thing done; this only stops a slow
    /// template holding an API request open past a client's read timeout.
    const AUTO_DEPLOYMENT_INLINE_BUDGET: std::time::Duration = std::time::Duration::from_secs(20);
    /// How many times one pass re-plans a stack set whose state moved under it
    /// (a concurrent UpdateStackSet) before leaving it to the next pass.
    const AUTO_DEPLOYMENT_REPLANS: usize = 8;

    /// Reconcile every service-managed stack set that has `AutoDeployment`
    /// enabled against the organization as it stands now.
    ///
    /// Organizations calls this after a mutation (an account created, invited,
    /// moved between OUs, removed or closed), as does a CloudFormation stack
    /// that provisions organization resources itself. An account that has
    /// joined an OU a stack set is deployed to gains that stack set's
    /// instances; one that has left loses them, keeping its stacks when
    /// `RetainStacksOnAccountRemoval` is set. That is what makes
    /// `AutoDeployment` mean anything: without it a stack set only ever covers
    /// the accounts that were in the OU at CreateStackInstances time.
    ///
    /// Reconciling against current state rather than a membership diff keeps
    /// this idempotent and independent of which call triggered it: every pass
    /// re-derives what is missing, so a pass that could not run is simply
    /// repeated. The cost is that a change and its exact reversal inside one
    /// pass (an account that leaves a target OU and re-joins it before the
    /// pass reads the organization) is indistinguishable from nothing having
    /// happened, because the end state is the same.
    ///
    /// Instances the operator deliberately left out — filtered out by
    /// `AccountFilterType`, or removed with DeleteStackInstances — are
    /// remembered on the stack set (`auto_deployment_excluded`) so reconciling
    /// never undoes that decision.
    ///
    /// Deployment runs inline, so the caller that triggered it sees it done,
    /// but never waits on a stack set that is busy: that one is re-planned by
    /// a background pass once the operation in its way finishes.
    pub async fn reconcile_auto_deployments(&self) {
        // One reconciliation at a time: a stack deployed by this one can
        // itself change the organization and trigger another, and concurrent
        // organization mutations would otherwise plan against each other's
        // half-applied state. A trigger that arrives while one is running is
        // recorded under the same lock that releases the run, so it is always
        // served by a further pass rather than lost. The claim is released on
        // drop, so a cancelled request cannot wedge auto-deployment.
        let Some(mut claim) = AutoDeploymentClaim::take(self) else {
            return;
        };
        // One budget for the whole call: the caller is an API request, and
        // what matters to it is how long *it* is held, not how long any one
        // stack set takes. Past it every remaining deployment carries on in
        // the background.
        let deadline = tokio::time::Instant::now() + Self::AUTO_DEPLOYMENT_INLINE_BUDGET;
        loop {
            // Consume the trigger this pass serves. One that lands while it
            // runs sets the flag again and is served by another lap.
            self.auto_deployment_gate.lock().pending = false;
            self.reconcile_auto_deployments_once(deadline).await;
            if !claim.another_pass_owed() {
                return;
            }
        }
    }

    async fn reconcile_auto_deployments_once(&self, deadline: tokio::time::Instant) {
        if self.deps.organizations.read().is_empty() {
            return;
        }
        let candidates: Vec<(String, String)> = {
            let accounts = self.state.read();
            accounts
                .iter()
                .flat_map(|(admin, state)| {
                    let admin = admin.to_string();
                    state
                        .regions
                        .values()
                        .flat_map(|r| r.stack_sets.values())
                        .filter(|set| auto_deploys(set))
                        .map(move |set| (admin.clone(), set.stack_set_id.clone()))
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        // Each stack set reconciles against ITS OWN administrator's
        // organization. Reconciling every stack set in the process against
        // one organization would deploy an administrator's stack set to
        // accounts belonging to a different organization.
        //
        // `candidates` is grouped by administrator, and one administrator
        // usually owns several stack sets, so the organization is cloned
        // once per administrator rather than once per stack set -- the
        // clone carries every account, OU and policy.
        let mut current: Option<(String, fakecloud_organizations::OrganizationState)> = None;
        for (admin, set_id) in candidates {
            if current.as_ref().is_none_or(|(owner, _)| owner != &admin) {
                current = self
                    .deps
                    .organizations
                    .read()
                    .org_of_account(&admin)
                    .cloned()
                    .map(|org| (admin.clone(), org));
            }
            let Some((_, org)) = &current else {
                continue;
            };
            self.reconcile_stack_set_auto_deployment(org, &admin, &set_id, deadline)
                .await;
        }
    }

    /// The stack set as it stands, with any background-provisioning stack it
    /// is waiting on folded in first.
    fn refreshed_stack_set(&self, admin: &str, set_id: &str) -> Option<StackSet> {
        let mut accounts = self.state.write();
        Self::refresh_stack_set(&mut accounts, admin, set_id);
        accounts
            .get(admin)
            .and_then(|s| s.stack_set(set_id))
            .cloned()
    }

    fn with_stack_set<T>(
        &self,
        admin: &str,
        set_id: &str,
        f: impl FnOnce(&mut StackSet) -> T,
    ) -> Option<T> {
        let mut accounts = self.state.write();
        accounts
            .get_mut(admin)
            .and_then(|s| s.stack_set_mut(set_id))
            .map(f)
    }

    /// Record the OUs and regions a CreateStackInstances call deploys to, so
    /// auto-deployment keeps following them even after every instance in one
    /// of them is gone.
    fn note_created_targets(
        &self,
        set: &StackSet,
        admin: &str,
        deployment_targets: Option<&DeploymentTargets>,
        regions: &[String],
    ) {
        if set.permission_model != "SERVICE_MANAGED" {
            return;
        }
        let Some(dt) = deployment_targets.filter(|d| !d.organizational_unit_ids.is_empty()) else {
            return;
        };
        self.with_stack_set(admin, &set.stack_set_id, |set| {
            for ou in &dt.organizational_unit_ids {
                set.auto_deployment_targets
                    .entry(ou.clone())
                    .or_default()
                    .extend(regions.iter().cloned());
            }
        });
    }

    /// Record the accounts a CreateStackInstances call deliberately leaves
    /// out of its target OUs, so auto-deployment does not add them later, and
    /// clear the exclusion on the instances it does deploy.
    fn note_create_exclusions(
        &self,
        set: &StackSet,
        admin: &str,
        deployment_targets: Option<&DeploymentTargets>,
        targets: &[Target],
        regions: &[String],
    ) {
        if set.permission_model != "SERVICE_MANAGED" {
            return;
        }
        let Some(dt) = deployment_targets.filter(|d| !d.organizational_unit_ids.is_empty()) else {
            return;
        };
        let deployed: BTreeSet<(String, String)> = targets
            .iter()
            .map(|t| (t.account.clone(), t.region.clone()))
            .collect();
        // An instance that already exists was not left out by this call.
        let live: BTreeSet<(String, String)> = set
            .instances
            .iter()
            .map(|i| (i.account.clone(), i.region.clone()))
            .collect();
        let mut excluded: BTreeSet<(String, String, String)> = BTreeSet::new();
        {
            let orgs = self.deps.organizations.read();
            // Without the organization there is nothing to work out who was
            // left out, but what this call deployed is still known, and the
            // exclusions on those are cleared below either way.
            if let Some(org) = orgs.org_of_account(admin) {
                for ou in &dt.organizational_unit_ids {
                    for (account, _) in accounts_under(org, ou) {
                        for region in regions {
                            let key = (account.clone(), region.clone());
                            if !deployed.contains(&key) && !live.contains(&key) {
                                excluded.insert((ou.clone(), account.clone(), region.clone()));
                            }
                        }
                    }
                }
            }
        }
        self.with_stack_set(admin, &set.stack_set_id, |set| {
            set.auto_deployment_excluded.retain(|(_, account, region)| {
                !deployed.contains(&(account.clone(), region.clone()))
            });
            set.auto_deployment_excluded.extend(excluded);
        });
    }

    /// Where one account stands relative to a stack set's target OUs: every
    /// target OU that contains it, nearest first, paired with that OU's
    /// regions.
    fn account_coverage(
        org: &fakecloud_organizations::OrganizationState,
        targets: &BTreeMap<String, BTreeSet<String>>,
        account: &str,
    ) -> Vec<(String, BTreeSet<String>)> {
        let mut matched: Vec<(String, BTreeSet<String>)> = Vec::new();
        let Some(member) = org.accounts.get(account) else {
            return matched;
        };
        let mut parent = member.parent_id.clone();
        // Walk from the account's own parent up to the root, so the first
        // target OU found is the most specific one containing it.
        for _ in 0..=MAX_OU_DEPTH {
            if let Some(regions) = targets.get(&parent) {
                matched.push((parent.clone(), regions.clone()));
            }
            match org.ous.get(&parent) {
                Some(ou) => parent = ou.parent_id.clone(),
                None => break,
            }
        }
        matched
    }

    async fn reconcile_stack_set_auto_deployment(
        &self,
        org: &fakecloud_organizations::OrganizationState,
        admin: &str,
        set_id: &str,
        deadline: tokio::time::Instant,
    ) {
        for _ in 0..Self::AUTO_DEPLOYMENT_REPLANS {
            let Some(set) = self.refreshed_stack_set(admin, set_id) else {
                return;
            };
            // Re-checked each lap, not just when the candidates were picked:
            // an UpdateStackSet is exactly what sends this loop round again,
            // and it may be the one that turned auto-deployment off.
            if !auto_deploys(&set) {
                return;
            }
            let (plan, bookkeeping_changed) = self.plan_auto_deployment(org, admin, &set);
            if bookkeeping_changed {
                // Re-attribution and dropped exclusions are state changes of
                // their own: there may be no operation to persist behind
                // (an empty plan, or one that could not start), and without
                // this they would be lost on a restart.
                self.save_snapshot().await;
            }
            match self
                .run_auto_deployment_plan(admin, &set, plan, deadline)
                .await
            {
                PlanOutcome::Done => return,
                // The stack set moved under the plan (a concurrent
                // UpdateStackSet): re-derive it from what is there now.
                PlanOutcome::Stale => continue,
                // Let whatever is in the way finish, then plan again from
                // scratch rather than replaying targets that may no longer be
                // the right ones.
                PlanOutcome::Retry => {
                    self.schedule_auto_deployment_retry(admin, set_id);
                    return;
                }
            }
        }
        // The stack set kept changing under every attempt; hand what is left
        // to a later pass rather than dropping it.
        self.schedule_auto_deployment_retry(admin, set_id);
    }

    /// What one stack set needs to match the organization: the instances to
    /// remove, then the ones to create.
    fn plan_auto_deployment(
        &self,
        org: &fakecloud_organizations::OrganizationState,
        admin: &str,
        set: &StackSet,
    ) -> (Vec<PlannedDeployment>, bool) {
        let targets = &set.auto_deployment_targets;
        // Where every member account stands relative to the target OUs. The
        // management account is never a service-managed target.
        let mut coverage: BTreeMap<String, Vec<(String, BTreeSet<String>)>> = BTreeMap::new();
        for account in org.accounts.values() {
            if account.id == org.management_account_id {
                continue;
            }
            let matched = Self::account_coverage(org, targets, &account.id);
            if !matched.is_empty() {
                coverage.insert(account.id.clone(), matched);
            }
        }
        // The OU an instance in this region belongs to: the nearest target OU
        // containing the account that is actually deployed to that region.
        let ou_for = |account: &str, region: &str| -> Option<String> {
            coverage
                .get(account)?
                .iter()
                .find_map(|(ou, regions)| regions.contains(region).then(|| ou.clone()))
        };
        let covers = |account: &str, region: &str| -> bool {
            coverage
                .get(account)
                .is_some_and(|matched| matched.iter().any(|(_, r)| r.contains(region)))
        };
        // An instance whose account has left the target OUs, or whose region
        // they no longer cover, is no longer the stack set's. An excluded
        // instance is not deployed but is not torn down either: an exclusion
        // says "do not deploy here", not "remove what is there".
        let removals: Vec<Target> = set
            .instances
            .iter()
            .filter(|i| i.organizational_unit_id.is_some())
            // A refused import records the operator's own pre-existing stack
            // against the stack set without adopting it. Deleting that stack
            // would destroy something the stack set never deployed.
            .filter(|i| i.detailed_status != "FAILED_IMPORT")
            .filter(|i| !covers(&i.account, &i.region))
            .map(|i| Target {
                account: i.account.clone(),
                region: i.region.clone(),
                ou: i.organizational_unit_id.clone(),
                suspended: false,
            })
            .collect();
        // An instance that exists but never got a stack because its target
        // was cancelled — the failure tolerance of an operator-issued
        // operation gave out before it ran — is not "already deployed": a
        // later pass tries it again. One that was tried and failed (a denying
        // account gate, a template the account rejects) is left alone, or
        // every later organization change would re-run a deploy that is known
        // to fail. One still PENDING belongs to an operation that has not
        // finished with it yet.
        let deployed: BTreeSet<(String, String)> = set
            .instances
            .iter()
            .filter(|i| {
                i.stack_id.is_some()
                    || matches!(i.detailed_status.as_str(), "PENDING" | "FAILED")
                    || i.status == "INOPERABLE"
                    || org
                        .accounts
                        .get(&i.account)
                        .is_some_and(|a| a.status != "ACTIVE")
            })
            .map(|i| (i.account.clone(), i.region.clone()))
            .collect();
        let mut creates: Vec<Target> = Vec::new();
        for (account, matched) in &coverage {
            let suspended = org
                .accounts
                .get(account)
                .is_some_and(|a| a.status != "ACTIVE");
            let regions: BTreeSet<&String> = matched.iter().flat_map(|(_, r)| r.iter()).collect();
            for region in regions {
                if deployed.contains(&(account.clone(), region.clone())) {
                    continue;
                }
                let ou = ou_for(account, region);
                let excluded = ou.as_ref().is_some_and(|ou| {
                    set.auto_deployment_excluded.contains(&(
                        ou.clone(),
                        account.clone(),
                        region.clone(),
                    ))
                });
                if excluded {
                    continue;
                }
                creates.push(Target {
                    account: account.clone(),
                    region: region.clone(),
                    ou,
                    suspended,
                });
            }
        }
        // Stale bookkeeping: an instance that has moved into another target
        // OU is re-attributed to it, and an exclusion stops applying once the
        // target OUs no longer cover it, so re-joining deploys again.
        let reattributed: Vec<(String, String, String)> = set
            .instances
            .iter()
            .filter(|i| i.organizational_unit_id.is_some())
            .filter_map(|i| {
                let ou = ou_for(&i.account, &i.region)?;
                // Keep the OU an instance was deployed through while that OU
                // still covers it, so reconciling never rewrites an
                // attribution CreateStackInstances chose.
                let current = i.organizational_unit_id.as_deref()?;
                let still_valid = coverage
                    .get(&i.account)
                    .is_some_and(|m| m.iter().any(|(o, r)| o == current && r.contains(&i.region)));
                (!still_valid).then(|| (i.account.clone(), i.region.clone(), ou))
            })
            .collect();
        // An exclusion lapses as soon as the account stops resolving to the
        // OU it was made under: it left the targets, or moved into another
        // one, which is a membership change that deploys to it again.
        let stale_exclusions: Vec<(String, String, String)> = set
            .auto_deployment_excluded
            .iter()
            .filter(|(ou, account, region)| ou_for(account, region).as_deref() != Some(ou.as_str()))
            .cloned()
            .collect();
        let bookkeeping_changed = !reattributed.is_empty() || !stale_exclusions.is_empty();
        if bookkeeping_changed {
            self.with_stack_set(admin, &set.stack_set_id, |set| {
                for (account, region, ou) in &reattributed {
                    if let Some(instance) = set
                        .instances
                        .iter_mut()
                        .find(|i| i.account == *account && i.region == *region)
                    {
                        instance.organizational_unit_id = Some(ou.clone());
                    }
                }
                for key in &stale_exclusions {
                    set.auto_deployment_excluded.remove(key);
                }
            });
        }
        // Removals first, so an account that moved to an OU deployed in other
        // regions does not briefly hold both sets of stacks.
        let mut plan: Vec<PlannedDeployment> = Vec::new();
        if !removals.is_empty() {
            let retain = set
                .auto_deployment
                .as_ref()
                .is_some_and(|a| a.retain_stacks_on_account_removal);
            plan.push(PlannedDeployment {
                action_name: "DELETE",
                action: TargetAction::Delete {
                    retain_stacks: retain,
                    user_requested: false,
                },
                retain_stacks: Some(retain),
                targets: removals,
            });
        }
        // A re-deployed instance keeps the overrides it was created with; a
        // brand-new one has none. One operation per distinct override set,
        // since overrides are a property of the operation.
        let mut by_overrides: BTreeMap<BTreeMap<String, String>, Vec<Target>> = BTreeMap::new();
        for target in creates {
            let overrides = set
                .instances
                .iter()
                .find(|i| i.account == target.account && i.region == target.region)
                .map(|i| i.parameter_overrides.clone())
                .unwrap_or_default();
            by_overrides.entry(overrides).or_default().push(target);
        }
        for (overrides, targets) in by_overrides {
            plan.push(PlannedDeployment {
                action_name: "CREATE",
                action: TargetAction::Create { overrides },
                retain_stacks: None,
                targets,
            });
        }
        (plan, bookkeeping_changed)
    }

    /// Run a stack set's planned operations, in order, against `set`.
    async fn run_auto_deployment_plan(
        &self,
        admin: &str,
        set: &StackSet,
        plan: Vec<PlannedDeployment>,
        deadline: tokio::time::Instant,
    ) -> PlanOutcome {
        let mut snapshot = set.clone();
        for step in plan {
            let regions: Vec<String> = step
                .targets
                .iter()
                .map(|t| t.region.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let record = DeploymentTargets {
                organizational_unit_ids: step
                    .targets
                    .iter()
                    .filter_map(|t| t.ou.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
                ..DeploymentTargets::default()
            };
            let targets = order_targets(step.targets, &regions, &auto_deployment_preferences());
            let op_id = uuid::Uuid::new_v4().to_string();
            let spec = match self.start_instance_operation(
                admin,
                &snapshot,
                &targets,
                &op_id,
                step.action_name,
                auto_deployment_preferences(),
                Some(record),
                step.retain_stacks,
            ) {
                Ok(spec) => spec,
                Err(err) if err.code() == "OperationInProgressException" => {
                    return PlanOutcome::Retry;
                }
                Err(err) if err.code() == "StaleRequestException" => return PlanOutcome::Stale,
                // Contention and staleness are handled above; what is left
                // is the stack set having gone (deleted, or no longer active),
                // which no retry recovers. Abandoning the rest of the plan is
                // the point — there is nothing to deploy to any more.
                Err(err) => {
                    tracing::warn!(
                        stack_set = snapshot.stack_set_id,
                        error = %err.message(),
                        "stack set auto-deployment could not start an operation"
                    );
                    return PlanOutcome::Done;
                }
            };
            // The operation runs in a task of its own and this only waits for
            // it, the way `launch_operation` does for an API-driven one: the
            // caller that triggered the deployment (an Organizations request)
            // can be cancelled by a client that hangs up, and an operation
            // abandoned half way would stay RUNNING and block the stack set.
            let svc = self.clone();
            let (request_id, admin_owned, set_id_owned, op) = (
                uuid::Uuid::new_v4().to_string(),
                admin.to_string(),
                snapshot.stack_set_id.clone(),
                op_id.clone(),
            );
            let action = step.action;
            let finished = tokio::spawn(async move {
                svc.run_operation(
                    &request_id,
                    &admin_owned,
                    &set_id_owned,
                    &op,
                    &spec,
                    targets,
                    action,
                )
                .await;
                svc.save_snapshot().await;
            });
            match tokio::time::timeout_at(deadline, finished).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    // The operation's task died. Settle it the way a restart
                    // settles one it interrupted, or it stays RUNNING and
                    // every later operation on this stack set is refused.
                    tracing::warn!(
                        stack_set = snapshot.stack_set_id,
                        "stack set auto-deployment operation did not finish"
                    );
                    self.with_stack_set(admin, &snapshot.stack_set_id, |set| {
                        settle_interrupted_operations(set)
                    });
                    self.save_snapshot().await;
                    // The rest of the plan never ran: re-plan it rather than
                    // leaving the accounts it covered undeployed.
                    return PlanOutcome::Retry;
                }
                // Still deploying. The operation owns the stack set until it
                // is done, so leaving it to run is the same situation as
                // finding the stack set busy: a waiter picks the rest of the
                // work up once it goes idle, and the caller is let go.
                Err(_) => {
                    tracing::info!(
                        stack_set = snapshot.stack_set_id,
                        "stack set auto-deployment still running; leaving it to the background"
                    );
                    return PlanOutcome::Retry;
                }
            }
            // The next step plans against what this one left behind.
            match self.refreshed_stack_set(admin, &snapshot.stack_set_id) {
                Some(current) => snapshot = current,
                None => return PlanOutcome::Done,
            }
        }
        PlanOutcome::Done
    }

    /// Wait out the operation holding a stack set, then reconcile again.
    ///
    /// Detached: the caller that triggered the deployment (an Organizations
    /// mutation, or a stack that provisioned an organization resource — which
    /// may be the very operation in the way) must not be held up by it, and
    /// on a busy stack set waiting inline would deadlock that second case.
    fn schedule_auto_deployment_retry(&self, admin: &str, set_id: &str) {
        let key = (admin.to_string(), set_id.to_string());
        // One waiter per stack set: a second change arriving while the first
        // is still waiting is served by the reconciliation that waiter runs.
        if !self.auto_deployment_retries.lock().insert(key.clone()) {
            return;
        }
        let svc = self.clone();
        let (admin, set_id) = key;
        // Built before the spawn: a task dropped before its first poll would
        // otherwise leave the marker set and suppress every later retry.
        let release = RetryGuard {
            retries: self.auto_deployment_retries.clone(),
            key: (admin.clone(), set_id.clone()),
        };
        tokio::spawn(async move {
            let release = release;
            let deadline = tokio::time::Instant::now() + Self::AUTO_DEPLOYMENT_MAX_WAIT;
            // Starts responsive, then backs off: a stack set stuck behind a
            // long deployment must not take the global CloudFormation lock
            // four times a second for an hour.
            let mut poll = Self::AUTO_DEPLOYMENT_POLL;
            loop {
                // Read through `refreshed_stack_set`: an operation whose last
                // stack has finished provisioning only settles when the stack
                // set is refreshed, and a raw read would see it as forever
                // running.
                let busy = svc.refreshed_stack_set(&admin, &set_id).is_some_and(|set| {
                    set.operations
                        .iter()
                        .any(|o| matches!(o.status.as_str(), "RUNNING" | "STOPPING"))
                });
                if !busy {
                    // Stand down first: the reconciliation this runs may find
                    // the stack set busy again (an operator's operation slipped
                    // in) and need to leave a waiter of its own, which it
                    // could not do while this one still held the marker.
                    drop(release);
                    svc.reconcile_auto_deployments().await;
                    return;
                }
                if tokio::time::Instant::now() >= deadline {
                    tracing::warn!(
                        stack_set = set_id,
                        "stack set auto-deployment gave up: another operation held the stack set"
                    );
                    return;
                }
                tokio::time::sleep(poll).await;
                poll = (poll * 2).min(Self::AUTO_DEPLOYMENT_MAX_POLL);
            }
        });
    }

    // ── Drift ──

    fn detect_stack_set_drift(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let preferences = parse_preferences(params)?;
        let op_id = params
            .get("OperationId")
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let set = {
            let mut accounts = self.state.write();
            let set_id = accounts
                .regional(&admin, &req.region)
                .and_then(|s| active_key(s, &name, Scope::of(params)))
                .ok_or_else(|| stack_set_not_found(&name))?;
            Self::refresh_stack_set(&mut accounts, &admin, &set_id);
            let set = accounts
                .get(&admin)
                .and_then(|s| s.stack_set(&set_id))
                .cloned()
                .ok_or_else(|| stack_set_not_found(&name))?;
            Self::check_can_start_drift(&set, &op_id)?;
            set
        };

        // Check every instance's stack against the live backing resources.
        let now = Utc::now();
        let mut op = Self::new_operation(&set, &op_id, "DETECT_DRIFT", preferences, None, None);
        let mut instance_drift: Vec<(String, String, String)> = Vec::new();
        for instance in &set.instances {
            let stack = instance.stack_id.as_ref().and_then(|id| {
                self.state
                    .read()
                    .regional(&instance.account, &instance.region)
                    .and_then(|s| {
                        s.stacks
                            .values()
                            .find(|st| &st.stack_id == id && st.status != "DELETE_COMPLETE")
                            .cloned()
                    })
            });
            let Some(stack) = stack else {
                instance_drift.push((
                    instance.account.clone(),
                    instance.region.clone(),
                    "UNKNOWN".to_string(),
                ));
                op.results.push(OperationResult {
                    account: instance.account.clone(),
                    region: instance.region.clone(),
                    status: "FAILED".to_string(),
                    status_reason: Some("Stack instance does not have a stack".to_string()),
                    organizational_unit_id: instance.organizational_unit_id.clone(),
                    account_gate_status: None,
                    account_gate_reason: None,
                });
                continue;
            };
            let mut drifted = false;
            for resource in &stack.resources {
                let status =
                    match self.resource_exists(&instance.account, &instance.region, resource) {
                        Some(true) => "IN_SYNC",
                        Some(false) => {
                            drifted = true;
                            "DELETED"
                        }
                        None => "NOT_CHECKED",
                    };
                op.resource_drifts.push(InstanceResourceDrift {
                    account: instance.account.clone(),
                    region: instance.region.clone(),
                    stack_id: stack.stack_id.clone(),
                    logical_id: resource.logical_id.clone(),
                    physical_id: resource.physical_id.clone(),
                    resource_type: resource.resource_type.clone(),
                    status: status.to_string(),
                    timestamp: now,
                });
            }
            let status = if drifted { "DRIFTED" } else { "IN_SYNC" };
            instance_drift.push((
                instance.account.clone(),
                instance.region.clone(),
                status.to_string(),
            ));
            op.results.push(OperationResult {
                account: instance.account.clone(),
                region: instance.region.clone(),
                status: "SUCCEEDED".to_string(),
                status_reason: None,
                organizational_unit_id: instance.organizational_unit_id.clone(),
                account_gate_status: None,
                account_gate_reason: None,
            });
        }
        let drifted = instance_drift
            .iter()
            .filter(|(_, _, s)| s == "DRIFTED")
            .count();
        let in_sync = instance_drift
            .iter()
            .filter(|(_, _, s)| s == "IN_SYNC")
            .count();
        let failed = instance_drift
            .iter()
            .filter(|(_, _, s)| s == "UNKNOWN")
            .count();
        let details = DriftDetectionDetails {
            drift_status: if drifted + in_sync == 0 {
                "NOT_CHECKED".to_string()
            } else if drifted > 0 {
                "DRIFTED".to_string()
            } else {
                "IN_SYNC".to_string()
            },
            detection_status: if failed == 0 {
                "COMPLETED".to_string()
            } else if failed == instance_drift.len() {
                "FAILED".to_string()
            } else {
                "PARTIAL_SUCCESS".to_string()
            },
            last_drift_check_timestamp: now,
            total: set.instances.len(),
            drifted,
            in_sync,
            failed,
        };
        op.drift = Some(details.clone());
        op.status = settled_status(&op).to_string();
        op.ended_at = Some(Utc::now());

        let mut accounts = self.state.write();
        Self::refresh_stack_set(&mut accounts, &admin, &set.stack_set_id);
        if let Some(stored) = accounts
            .get_mut(&admin)
            .and_then(|s| s.stack_set_mut(&set.stack_set_id))
        {
            // Re-validate against what is stored now: another operation may
            // have started, or used this OperationId, while resources were
            // being checked. (DetectStackSetDrift does not model
            // StaleRequestException, and a drift result stays valid after an
            // operation that has already finished.)
            Self::check_can_start_drift(stored, &op_id)?;
            for instance in &mut stored.instances {
                if let Some((_, _, status)) = instance_drift
                    .iter()
                    .find(|(a, r, _)| *a == instance.account && *r == instance.region)
                {
                    instance.drift_status = status.clone();
                    instance.last_drift_check_timestamp = Some(now);
                }
            }
            stored.drift = Some(details);
            stored.operations.push(op);
        }
        Ok(xml_response(
            "DetectStackSetDrift",
            el("OperationId", &op_id),
            &req.request_id,
        ))
    }

    fn list_stack_instance_resource_drifts(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(params, "StackSetName")?;
        let account = required(params, "StackInstanceAccount")?;
        let region = required(params, "StackInstanceRegion")?;
        let op_id = required(params, "OperationId")?;
        let admin = self.stack_set_admin_account(req, params)?;
        let set = self.read_stack_set(&admin, &req.region, &name, Scope::of(params))?;
        let op = set
            .operations
            .iter()
            .find(|o| o.operation_id == op_id)
            .ok_or_else(|| operation_not_found(&op_id))?;
        if !set
            .instances
            .iter()
            .any(|i| i.account == account && i.region == region)
        {
            return Err(instance_not_found(&name, &account, &region));
        }
        let statuses = member_list(params, "StackInstanceResourceDriftStatuses");
        let drifts: Vec<&InstanceResourceDrift> = op
            .resource_drifts
            .iter()
            .filter(|d| d.account == account && d.region == region)
            .filter(|d| statuses.is_empty() || statuses.contains(&d.status))
            .collect();
        let (page, next) = paginate(drifts, params)?;
        let inner = format!(
            "{}{}",
            list_el(
                "Summaries",
                page.into_iter().map(|d| {
                    format!(
                        "{}{}{}{}<PropertyDifferences/>{}{}",
                        el("StackId", &d.stack_id),
                        el("LogicalResourceId", &d.logical_id),
                        el("PhysicalResourceId", &d.physical_id),
                        el("ResourceType", &d.resource_type),
                        el("StackResourceDriftStatus", &d.status),
                        el("Timestamp", &ts(&d.timestamp)),
                    )
                })
            ),
            next_token_el(next)
        );
        Ok(xml_response(
            "ListStackInstanceResourceDrifts",
            inner,
            &req.request_id,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extras::tests::{deps, req};
    use crate::service::CloudFormationDeps;
    use crate::state::SharedCloudFormationState;
    use fakecloud_core::delivery::{DeliveryBus, LambdaDelivery};
    use fakecloud_core::service::AwsService;
    use parking_lot::RwLock;
    use std::sync::Arc;

    const ADMIN: &str = "000000000000";
    const ACCT_B: &str = "111111111111";
    const ACCT_C: &str = "222222222222";
    // The queue is unnamed: CloudFormation generates a distinct name per stack,
    // so instances in one account (one per region) do not collide.
    const QUEUE_TEMPLATE: &str = "Parameters:\n  Env:\n    Type: String\n    Default: dev\nResources:\n  Q:\n    Type: AWS::SQS::Queue\n";
    const TOPIC_TEMPLATE: &str = "Parameters:\n  Env:\n    Type: String\n    Default: dev\nResources:\n  T:\n    Type: AWS::SNS::Topic\n";

    fn service_with(deps: CloudFormationDeps) -> CloudFormationService {
        let state: SharedCloudFormationState =
            Arc::new(RwLock::new(
                MultiAccountState::<CloudFormationAccountState>::new(ADMIN, "us-east-1", ""),
            ));
        CloudFormationService::new(state, deps)
    }

    fn service() -> CloudFormationService {
        service_with(deps())
    }

    async fn call_as(
        svc: &CloudFormationService,
        account: &str,
        action: &str,
        params: &[(&str, &str)],
    ) -> Result<String, AwsServiceError> {
        let mut request = req(action, params);
        request.account_id = account.to_string();
        let resp = svc.handle(request).await?;
        Ok(String::from_utf8(resp.body.expect_bytes().to_vec()).expect("utf8"))
    }

    async fn call(
        svc: &CloudFormationService,
        action: &str,
        params: &[(&str, &str)],
    ) -> Result<String, AwsServiceError> {
        call_as(svc, ADMIN, action, params).await
    }

    async fn ok(svc: &CloudFormationService, action: &str, params: &[(&str, &str)]) -> String {
        match call(svc, action, params).await {
            Ok(xml) => xml,
            Err(e) => panic!("{action} failed: {} {}", e.code(), e.message()),
        }
    }

    async fn err(
        svc: &CloudFormationService,
        action: &str,
        params: &[(&str, &str)],
    ) -> AwsServiceError {
        match call(svc, action, params).await {
            Ok(xml) => panic!("{action} should fail, got {xml}"),
            Err(e) => e,
        }
    }

    fn tag(xml: &str, name: &str) -> String {
        let open = format!("<{name}>");
        xml.split(&open)
            .nth(1)
            .and_then(|rest| rest.split(&format!("</{name}>")).next())
            .unwrap_or_else(|| panic!("no <{name}> in {xml}"))
            .to_string()
    }

    fn stored_set(svc: &CloudFormationService, name: &str) -> StackSet {
        stored_set_in(svc, "us-east-1", name)
    }

    fn stored_set_in(svc: &CloudFormationService, region: &str, name: &str) -> StackSet {
        svc.state
            .read()
            .regional(ADMIN, region)
            .and_then(|s| find_active(s, name, Scope::Own))
            .cloned()
            .expect("stack set")
    }

    fn stack_of(svc: &CloudFormationService, account: &str, stack_id: &str) -> crate::state::Stack {
        // An instance's stack lives in the region its stack id names.
        let region = fakecloud_aws::arn::region_of(stack_id).expect("stack id is an ARN");
        svc.state
            .read()
            .regional(account, region)
            .and_then(|s| {
                s.stacks
                    .values()
                    .find(|st| st.stack_id == stack_id)
                    .cloned()
            })
            .expect("instance stack")
    }

    fn queue_count(svc: &CloudFormationService, account: &str) -> usize {
        svc.deps
            .sqs
            .read()
            .get(account)
            .map_or(0, |s| s.queues.len())
    }

    async fn create_set(svc: &CloudFormationService, name: &str, template: &str) {
        ok(
            svc,
            "CreateStackSet",
            &[("StackSetName", name), ("TemplateBody", template)],
        )
        .await;
    }

    #[tokio::test]
    async fn stack_instances_provision_real_stacks_per_account_and_region() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        let xml = ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Accounts.member.2", ACCT_C),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "eu-west-1"),
                ("ParameterOverrides.member.1.ParameterKey", "Env"),
                ("ParameterOverrides.member.1.ParameterValue", "prod"),
            ],
        )
        .await;
        let op_id = tag(&xml, "OperationId");

        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", &op_id)],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "SUCCEEDED", "{op}");
        assert_eq!(tag(&op, "Action"), "CREATE");
        assert!(op.contains("<EndTimestamp>"), "{op}");

        let set = stored_set(&svc, "app");
        assert_eq!(set.instances.len(), 4);
        for instance in &set.instances {
            assert_eq!(instance.status, "CURRENT");
            assert_eq!(instance.detailed_status, "SUCCEEDED");
            let stack_id = instance.stack_id.as_deref().expect("stack id");
            assert!(
                stack_id.starts_with(&format!(
                    "arn:aws:cloudformation:{}:{}:stack/StackSet-app-",
                    instance.region, instance.account
                )),
                "{stack_id}"
            );
            let stack = stack_of(&svc, &instance.account, stack_id);
            assert_eq!(stack.status, "CREATE_COMPLETE");
            assert_eq!(
                stack.parameters.get("Env").map(String::as_str),
                Some("prod")
            );
            assert_eq!(stack.resources.len(), 1);
            // The stack lives in the instance's region, and only there.
            let accounts = svc.state.read();
            let account = accounts.get(&instance.account).expect("account");
            for (region, state) in &account.regions {
                assert_eq!(
                    state.stacks.values().any(|s| s.stack_id == stack_id),
                    region == &instance.region,
                    "{stack_id} in {region}"
                );
            }
        }
        // Each account got a queue per region, in that account.
        assert_eq!(queue_count(&svc, ACCT_B), 2);
        assert_eq!(queue_count(&svc, ACCT_C), 2);
        assert_eq!(queue_count(&svc, ADMIN), 0);

        let described = ok(
            &svc,
            "DescribeStackInstance",
            &[
                ("StackSetName", "app"),
                ("StackInstanceAccount", ACCT_B),
                ("StackInstanceRegion", "eu-west-1"),
            ],
        )
        .await;
        assert_eq!(tag(&described, "Status"), "CURRENT");
        assert_eq!(tag(&described, "DetailedStatus"), "SUCCEEDED");
        assert_eq!(tag(&described, "LastOperationId"), op_id);
        assert!(
            described.contains("<ParameterValue>prod</ParameterValue>"),
            "{described}"
        );

        let listed = ok(
            &svc,
            "ListStackInstances",
            &[("StackSetName", "app"), ("StackInstanceAccount", ACCT_C)],
        )
        .await;
        assert_eq!(listed.matches("<member>").count(), 2, "{listed}");

        let results = ok(
            &svc,
            "ListStackSetOperationResults",
            &[("StackSetName", "app"), ("OperationId", &op_id)],
        )
        .await;
        assert_eq!(
            results.matches("<Status>SUCCEEDED</Status>").count(),
            4,
            "{results}"
        );
        assert!(
            results.contains("<AccountGateResult><Status>SKIPPED</Status>"),
            "{results}"
        );

        let summary = ok(&svc, "DescribeStackSet", &[("StackSetName", "app")]).await;
        assert!(summary.contains("<member>eu-west-1</member>"), "{summary}");
        assert!(summary.contains("<PermissionModel>SELF_MANAGED</PermissionModel>"));
        assert!(summary.contains(&format!(
            "<AdministrationRoleARN>arn:aws:iam::{ADMIN}:role/AWSCloudFormationStackSetAdministrationRole</AdministrationRoleARN>"
        )));
    }

    #[tokio::test]
    async fn updating_the_stack_set_redeploys_every_instance() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "us-west-2"),
            ],
        )
        .await;
        let xml = ok(
            &svc,
            "UpdateStackSet",
            &[("StackSetName", "app"), ("TemplateBody", TOPIC_TEMPLATE)],
        )
        .await;
        let op_id = tag(&xml, "OperationId");
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", &op_id)],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "SUCCEEDED", "{op}");
        assert_eq!(tag(&op, "Action"), "UPDATE");

        let set = stored_set(&svc, "app");
        for instance in &set.instances {
            let stack = stack_of(&svc, ACCT_B, instance.stack_id.as_deref().unwrap());
            assert_eq!(stack.status, "UPDATE_COMPLETE");
            assert_eq!(stack.resources[0].resource_type, "AWS::SNS::Topic");
            assert_eq!(instance.last_operation_id.as_deref(), Some(op_id.as_str()));
        }
        // The queues the old template made are gone.
        assert_eq!(queue_count(&svc, ACCT_B), 0);

        let ops = ok(&svc, "ListStackSetOperations", &[("StackSetName", "app")]).await;
        assert_eq!(ops.matches("<member>").count(), 2, "{ops}");
        // Most recent first.
        assert_eq!(tag(&ops, "Action"), "UPDATE");
    }

    #[tokio::test]
    async fn a_partial_stack_set_update_leaves_the_rest_outdated() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "us-west-2"),
            ],
        )
        .await;
        ok(
            &svc,
            "UpdateStackSet",
            &[
                ("StackSetName", "app"),
                ("TemplateBody", TOPIC_TEMPLATE),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-west-2"),
            ],
        )
        .await;
        let set = stored_set(&svc, "app");
        let east = set
            .instances
            .iter()
            .find(|i| i.region == "us-east-1")
            .unwrap();
        let west = set
            .instances
            .iter()
            .find(|i| i.region == "us-west-2")
            .unwrap();
        assert_eq!(west.status, "CURRENT");
        assert_eq!(east.status, "OUTDATED");
        assert_eq!(
            stack_of(&svc, ACCT_B, east.stack_id.as_deref().unwrap()).resources[0].resource_type,
            "AWS::SQS::Queue"
        );
    }

    #[tokio::test]
    async fn update_stack_instances_applies_and_keeps_overrides() {
        let template = "Parameters:\n  Env:\n    Type: String\n    Default: dev\n  Size:\n    Type: String\n    Default: s\nResources:\n  Q:\n    Type: AWS::SQS::Queue\n";
        let svc = service();
        create_set(&svc, "app", template).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("ParameterOverrides.member.1.ParameterKey", "Env"),
                ("ParameterOverrides.member.1.ParameterValue", "prod"),
            ],
        )
        .await;
        ok(
            &svc,
            "UpdateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("ParameterOverrides.member.1.ParameterKey", "Env"),
                ("ParameterOverrides.member.1.UsePreviousValue", "true"),
                ("ParameterOverrides.member.2.ParameterKey", "Size"),
                ("ParameterOverrides.member.2.ParameterValue", "xl"),
            ],
        )
        .await;
        let set = stored_set(&svc, "app");
        let instance = &set.instances[0];
        assert_eq!(
            instance.parameter_overrides.get("Env").map(String::as_str),
            Some("prod")
        );
        assert_eq!(
            instance.parameter_overrides.get("Size").map(String::as_str),
            Some("xl")
        );
        let stack = stack_of(&svc, ACCT_B, instance.stack_id.as_deref().unwrap());
        assert_eq!(stack.parameters.get("Size").map(String::as_str), Some("xl"));
        assert_eq!(
            stack.parameters.get("Env").map(String::as_str),
            Some("prod")
        );

        // Leaving a parameter out of the list reverts it to the stack set's value.
        ok(
            &svc,
            "UpdateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("ParameterOverrides.member.1.ParameterKey", "Size"),
                ("ParameterOverrides.member.1.UsePreviousValue", "true"),
            ],
        )
        .await;
        let set = stored_set(&svc, "app");
        let stack = stack_of(&svc, ACCT_B, set.instances[0].stack_id.as_deref().unwrap());
        assert_eq!(stack.parameters.get("Env").map(String::as_str), Some("dev"));
        assert_eq!(stack.parameters.get("Size").map(String::as_str), Some("xl"));

        // Overriding a parameter the template does not declare is rejected.
        let e = err(
            &svc,
            "UpdateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("ParameterOverrides.member.1.ParameterKey", "Nope"),
                ("ParameterOverrides.member.1.ParameterValue", "x"),
            ],
        )
        .await;
        assert_eq!(e.code(), "ValidationError");

        // An instance that does not exist is reported.
        let e = err(
            &svc,
            "UpdateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_C),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        assert_eq!(e.code(), "StackInstanceNotFoundException");
    }

    #[tokio::test]
    async fn deleting_instances_tears_down_or_retains_their_stacks() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "us-west-2"),
            ],
        )
        .await;
        let e = err(&svc, "DeleteStackSet", &[("StackSetName", "app")]).await;
        assert_eq!(e.code(), "StackSetNotEmptyException");

        let set = stored_set(&svc, "app");
        let east_stack = set
            .instances
            .iter()
            .find(|i| i.region == "us-east-1")
            .unwrap()
            .stack_id
            .clone()
            .unwrap();
        let west_stack = set
            .instances
            .iter()
            .find(|i| i.region == "us-west-2")
            .unwrap()
            .stack_id
            .clone()
            .unwrap();

        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("RetainStacks", "false"),
            ],
        )
        .await;
        assert_eq!(
            stack_of(&svc, ACCT_B, &east_stack).status,
            "DELETE_COMPLETE"
        );
        assert_eq!(queue_count(&svc, ACCT_B), 1);

        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-west-2"),
                ("RetainStacks", "true"),
            ],
        )
        .await;
        // Retained: the stack and its queue outlive the instance.
        assert_eq!(
            stack_of(&svc, ACCT_B, &west_stack).status,
            "CREATE_COMPLETE"
        );
        assert_eq!(queue_count(&svc, ACCT_B), 1);
        assert!(stored_set(&svc, "app").instances.is_empty());

        let e = err(
            &svc,
            "DescribeStackInstance",
            &[
                ("StackSetName", "app"),
                ("StackInstanceAccount", ACCT_B),
                ("StackInstanceRegion", "us-east-1"),
            ],
        )
        .await;
        assert_eq!(e.code(), "StackInstanceNotFoundException");

        // Empty now, so it deletes; the name is free and the id still resolves.
        let id = stored_set(&svc, "app").stack_set_id;
        ok(&svc, "DeleteStackSet", &[("StackSetName", "app")]).await;
        let e = err(&svc, "DescribeStackSet", &[("StackSetName", "app")]).await;
        assert_eq!(e.code(), "StackSetNotFoundException");
        let by_id = ok(&svc, "DescribeStackSet", &[("StackSetName", &id)]).await;
        assert_eq!(tag(&by_id, "Status"), "DELETED");
        let deleted = ok(&svc, "ListStackSets", &[("Status", "DELETED")]).await;
        assert!(deleted.contains(&id), "{deleted}");
        let active = ok(&svc, "ListStackSets", &[("Status", "ACTIVE")]).await;
        assert!(!active.contains(&id), "{active}");
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
    }

    #[tokio::test]
    async fn a_failing_target_cancels_the_rest_beyond_the_tolerance() {
        // A template importing an export that does not exist fails to create.
        let broken = "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName:\n        Fn::ImportValue: missing-export\n";
        let svc = service();
        create_set(&svc, "bad", broken).await;
        let xml = ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "bad"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "us-west-2"),
            ],
        )
        .await;
        let op_id = tag(&xml, "OperationId");
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "bad"), ("OperationId", &op_id)],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "FAILED", "{op}");
        assert_eq!(tag(&op, "FailedStackInstancesCount"), "1");

        let results = ok(
            &svc,
            "ListStackSetOperationResults",
            &[
                ("StackSetName", "bad"),
                ("OperationId", &op_id),
                ("Filters.member.1.Name", "OPERATION_RESULT_STATUS"),
                ("Filters.member.1.Values", "CANCELLED"),
            ],
        )
        .await;
        assert!(results.contains("<Region>us-west-2</Region>"), "{results}");
        assert!(results.contains(TOLERANCE_EXCEEDED), "{results}");

        let set = stored_set(&svc, "bad");
        let failed = set
            .instances
            .iter()
            .find(|i| i.region == "us-east-1")
            .unwrap();
        assert_eq!(failed.detailed_status, "FAILED");
        assert!(failed
            .status_reason
            .as_deref()
            .unwrap()
            .contains("missing-export"));
        let cancelled = set
            .instances
            .iter()
            .find(|i| i.region == "us-west-2")
            .unwrap();
        assert_eq!(cancelled.detailed_status, "CANCELLED");

        let filtered = ok(
            &svc,
            "ListStackInstances",
            &[
                ("StackSetName", "bad"),
                ("Filters.member.1.Name", "DETAILED_STATUS"),
                ("Filters.member.1.Values", "FAILED"),
            ],
        )
        .await;
        assert_eq!(filtered.matches("<member>").count(), 1, "{filtered}");

        // With one failure tolerated per region, both regions are attempted
        // and the operation succeeds despite the failures being per-region 1.
        let xml = ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "bad"),
                ("Accounts.member.1", ACCT_C),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "us-west-2"),
                ("OperationPreferences.FailureToleranceCount", "1"),
            ],
        )
        .await;
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[
                ("StackSetName", "bad"),
                ("OperationId", &tag(&xml, "OperationId")),
            ],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "SUCCEEDED", "{op}");
        assert_eq!(tag(&op, "FailedStackInstancesCount"), "2");
    }

    #[tokio::test]
    async fn operations_report_modeled_errors() {
        let svc = service();
        let e = err(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "nope"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        assert_eq!(e.code(), "StackSetNotFoundException");
        assert_eq!(e.status(), StatusCode::NOT_FOUND);

        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        let e = err(
            &svc,
            "CreateStackSet",
            &[("StackSetName", "app"), ("TemplateBody", QUEUE_TEMPLATE)],
        )
        .await;
        assert_eq!(e.code(), "NameAlreadyExistsException");

        let e = err(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "never")],
        )
        .await;
        assert_eq!(e.code(), "OperationNotFoundException");

        let params = [
            ("StackSetName", "app"),
            ("Accounts.member.1", ACCT_B),
            ("Regions.member.1", "us-east-1"),
            ("OperationId", "op-1"),
        ];
        ok(&svc, "CreateStackInstances", &params).await;
        let e = err(&svc, "CreateStackInstances", &params).await;
        assert_eq!(e.code(), "OperationIdAlreadyExistsException");

        let e = err(
            &svc,
            "StopStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "op-1")],
        )
        .await;
        assert_eq!(e.code(), "InvalidOperationException");

        let e = err(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", "not-an-account"),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        assert_eq!(e.code(), "ValidationError");
        let e = err(
            &svc,
            "CreateStackInstances",
            &[("StackSetName", "app"), ("Accounts.member.1", ACCT_B)],
        )
        .await;
        assert_eq!(e.code(), "ValidationError");
    }

    #[tokio::test]
    async fn a_running_operation_blocks_others_and_can_be_stopped() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        {
            let mut accounts = svc.state.write();
            let set = accounts
                .regional_mut(ADMIN, "us-east-1")
                .stack_sets
                .values_mut()
                .next()
                .unwrap();
            let mut op = CloudFormationService::new_operation(
                &set.clone(),
                "running-op",
                "CREATE",
                OperationPreferences::default(),
                None,
                None,
            );
            op.results.push(OperationResult {
                account: ACCT_B.to_string(),
                region: "us-east-1".to_string(),
                status: "PENDING".to_string(),
                status_reason: None,
                organizational_unit_id: None,
                account_gate_status: None,
                account_gate_reason: None,
            });
            set.operations.push(op);
        }
        let e = err(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        assert_eq!(e.code(), "OperationInProgressException");
        let e = err(&svc, "DeleteStackSet", &[("StackSetName", "app")]).await;
        assert_eq!(e.code(), "OperationInProgressException");

        ok(
            &svc,
            "StopStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "running-op")],
        )
        .await;
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "running-op")],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "STOPPED", "{op}");
        let results = ok(
            &svc,
            "ListStackSetOperationResults",
            &[("StackSetName", "app"), ("OperationId", "running-op")],
        )
        .await;
        assert_eq!(tag(&results, "Status"), "CANCELLED", "{results}");
    }

    #[tokio::test]
    async fn an_asynchronously_provisioning_instance_settles_on_read() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("OperationId", "op-async"),
            ],
        )
        .await;
        // Rewind to the moment the stack was still provisioning.
        let stack_id = {
            let mut accounts = svc.state.write();
            let set = accounts
                .regional_mut(ADMIN, "us-east-1")
                .stack_sets
                .values_mut()
                .next()
                .unwrap();
            set.instances[0].detailed_status = "RUNNING".to_string();
            set.instances[0].status = "OUTDATED".to_string();
            let op = set.operations.last_mut().unwrap();
            op.status = "RUNNING".to_string();
            op.ended_at = None;
            op.results[0].status = "RUNNING".to_string();
            let stack_id = set.instances[0].stack_id.clone().unwrap();
            let stack = accounts
                .regional_mut(ACCT_B, "us-east-1")
                .stacks
                .values_mut()
                .find(|s| s.stack_id == stack_id)
                .unwrap();
            stack.status = "CREATE_IN_PROGRESS".to_string();
            stack_id
        };
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "op-async")],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "RUNNING", "{op}");

        // The stack finishes: the next read reflects it.
        svc.state
            .write()
            .regional_mut(ACCT_B, "us-east-1")
            .stacks
            .values_mut()
            .find(|s| s.stack_id == stack_id)
            .unwrap()
            .status = "CREATE_COMPLETE".to_string();
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "op-async")],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "SUCCEEDED", "{op}");
        let instance = ok(
            &svc,
            "DescribeStackInstance",
            &[
                ("StackSetName", "app"),
                ("StackInstanceAccount", ACCT_B),
                ("StackInstanceRegion", "us-east-1"),
            ],
        )
        .await;
        assert_eq!(tag(&instance, "Status"), "CURRENT");
    }

    fn seed_org(svc: &CloudFormationService) -> (String, String) {
        let mut org = fakecloud_organizations::OrganizationState::bootstrap(ADMIN);
        let root = org.root_id.clone();
        let parent = org.create_ou(&root, "workloads").unwrap();
        let child = org.create_ou(&parent.id, "prod").unwrap();
        for (account, dest) in [(ACCT_B, &parent.id), (ACCT_C, &child.id)] {
            org.enroll_account_if_missing(account);
            org.move_account(account, &root, dest).unwrap();
        }
        svc.deps.organizations.write().insert(org);
        (parent.id, child.id)
    }

    const ACCT_D: &str = "333333333333";

    #[tokio::test]
    async fn stack_set_in_china_keeps_the_aws_cn_partition() {
        let svc = service();
        seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        let mut create = req(
            "CreateStackSet",
            &[
                ("StackSetName", "cn-set"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "false"),
            ],
        );
        create.account_id = ADMIN.to_string();
        create.region = "cn-north-1".to_string();
        svc.handle(create).await.expect("CreateStackSet");
        let set = stored_set_in(&svc, "cn-north-1", "cn-set");
        assert!(
            set.arn
                .starts_with("arn:aws-cn:cloudformation:cn-north-1:000000000000:stackset/cn-set:"),
            "{}",
            set.arn
        );

        // A stack set lives in the region it was created in: another region
        // does not see it.
        let e = err(
            &svc,
            "UpdateStackSet",
            &[("StackSetName", "cn-set"), ("UsePreviousTemplate", "true")],
        )
        .await;
        assert_eq!(e.code(), "StackSetNotFoundException");
        let e = err(&svc, "DescribeStackSet", &[("StackSetName", "cn-set")]).await;
        assert_eq!(e.code(), "StackSetNotFoundException");
        let listed = ok(&svc, "ListStackSets", &[]).await;
        assert!(!listed.contains("cn-set"), "{listed}");

        // Switching to SELF_MANAGED defaults the administration role in the
        // stack set's partition.
        let mut update = req(
            "UpdateStackSet",
            &[
                ("StackSetName", "cn-set"),
                ("UsePreviousTemplate", "true"),
                ("PermissionModel", "SELF_MANAGED"),
            ],
        );
        update.account_id = ADMIN.to_string();
        update.region = "cn-north-1".to_string();
        svc.handle(update).await.expect("UpdateStackSet");
        assert_eq!(
            stored_set_in(&svc, "cn-north-1", "cn-set")
                .administration_role_arn
                .as_deref(),
            Some("arn:aws-cn:iam::000000000000:role/AWSCloudFormationStackSetAdministrationRole")
        );
    }

    /// A service-managed stack set with AutoDeployment enabled, deployed to
    /// `workloads` in us-east-1. Returns the OU ids from `seed_org`.
    async fn auto_deployed_set(
        svc: &CloudFormationService,
        name: &str,
        retain_on_removal: bool,
    ) -> (String, String) {
        let (workloads, prod) = seed_org(svc);
        ok(svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            svc,
            "CreateStackSet",
            &[
                ("StackSetName", name),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
                (
                    "AutoDeployment.RetainStacksOnAccountRemoval",
                    if retain_on_removal { "true" } else { "false" },
                ),
            ],
        )
        .await;
        ok(
            svc,
            "CreateStackInstances",
            &[
                ("StackSetName", name),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        (workloads, prod)
    }

    /// Put `account` in the organization under `parent`.
    fn join_ou(svc: &CloudFormationService, account: &str, parent: &str) {
        let mut guard = svc.deps.organizations.write();
        let org = guard.sole_mut().expect("organization");
        let root = org.root_id.clone();
        org.enroll_account_if_missing(account);
        org.move_account(account, &root, parent).unwrap();
    }

    fn instance_of<'a>(set: &'a StackSet, account: &str) -> Option<&'a StackInstance> {
        set.instances.iter().find(|i| i.account == account)
    }

    #[tokio::test]
    async fn auto_deployment_covers_an_account_added_to_a_target_ou() {
        let svc = service();
        let (workloads, prod) = auto_deployed_set(&svc, "org", false).await;
        assert_eq!(stored_set(&svc, "org").instances.len(), 2);

        // A new account joins a nested OU below the target after the stack
        // set was deployed. AWS deploys to it; so do we.
        join_ou(&svc, ACCT_D, &prod);
        svc.reconcile_auto_deployments().await;

        let set = stored_set(&svc, "org");
        let instance = instance_of(&set, ACCT_D).expect("auto-deployed instance");
        assert_eq!(instance.status, "CURRENT", "{instance:?}");
        assert_eq!(
            instance.organizational_unit_id.as_deref(),
            Some(workloads.as_str())
        );
        assert_eq!(instance.region, "us-east-1");
        assert_eq!(queue_count(&svc, ACCT_D), 1);

        // Recorded as a CREATE operation against the target OU, as AWS does.
        let op = set
            .operations
            .iter()
            .find(|o| o.action == "CREATE" && o.results.iter().any(|r| r.account == ACCT_D))
            .expect("auto-deployment operation");
        assert_eq!(op.status, "SUCCEEDED");
        assert_eq!(
            op.deployment_targets
                .as_ref()
                .map(|t| t.organizational_unit_ids.clone()),
            Some(vec![workloads.clone()])
        );

        // Reconciling again with nothing to do records no further operation.
        let before = stored_set(&svc, "org").operations.len();
        svc.reconcile_auto_deployments().await;
        assert_eq!(stored_set(&svc, "org").operations.len(), before);
    }

    #[tokio::test]
    async fn auto_deployment_removes_an_account_that_leaves_the_target_ou() {
        let svc = service();
        auto_deployed_set(&svc, "org", false).await;
        assert_eq!(queue_count(&svc, ACCT_C), 1);

        // ACCT_C moves out of the target OU tree, back to the root.
        {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().expect("organization");
            let root = org.root_id.clone();
            let parent = org.parent_of(ACCT_C).expect("parent").0;
            org.move_account(ACCT_C, &parent, &root).unwrap();
        }
        svc.reconcile_auto_deployments().await;

        let set = stored_set(&svc, "org");
        assert!(instance_of(&set, ACCT_C).is_none(), "{set:?}");
        assert!(instance_of(&set, ACCT_B).is_some());
        // RetainStacksOnAccountRemoval=false: the stack goes with the account.
        assert_eq!(queue_count(&svc, ACCT_C), 0);
        assert_eq!(
            set.operations
                .iter()
                .find(|o| o.action == "DELETE")
                .map(|o| o.retain_stacks),
            Some(Some(false))
        );
    }

    #[tokio::test]
    async fn auto_deployment_retains_stacks_when_configured() {
        let svc = service();
        auto_deployed_set(&svc, "org", true).await;
        assert_eq!(queue_count(&svc, ACCT_C), 1);

        {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().expect("organization");
            let root = org.root_id.clone();
            let parent = org.parent_of(ACCT_C).expect("parent").0;
            org.move_account(ACCT_C, &parent, &root).unwrap();
        }
        svc.reconcile_auto_deployments().await;

        let set = stored_set(&svc, "org");
        assert!(instance_of(&set, ACCT_C).is_none(), "{set:?}");
        // The stack stays behind in the removed account.
        assert_eq!(queue_count(&svc, ACCT_C), 1);
    }

    #[tokio::test]
    async fn auto_deployment_does_not_undo_a_deleted_instance() {
        let svc = service();
        let (workloads, prod) = auto_deployed_set(&svc, "org", false).await;
        // The operator drops one account's instance by hand (everything in
        // the target OU except ACCT_B) while the account stays in that OU.
        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("DeploymentTargets.Accounts.member.1", ACCT_B),
                ("DeploymentTargets.AccountFilterType", "DIFFERENCE"),
                ("Regions.member.1", "us-east-1"),
                ("RetainStacks", "false"),
            ],
        )
        .await;
        assert_eq!(queue_count(&svc, ACCT_C), 0);

        // An unrelated organization change must not bring it back.
        join_ou(&svc, ACCT_D, &prod);
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        assert!(instance_of(&set, ACCT_C).is_none(), "{set:?}");
        assert_eq!(queue_count(&svc, ACCT_C), 0);
        // ...while the account that did join is deployed to.
        assert!(instance_of(&set, ACCT_D).is_some(), "{set:?}");

        // Leaving and re-joining the OU deploys again: the exclusion only
        // covers the account while it is still there.
        let (root, parent) = {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            let parent = org.parent_of(ACCT_C).unwrap().0;
            org.move_account(ACCT_C, &parent, &root).unwrap();
            (root, parent)
        };
        svc.reconcile_auto_deployments().await;
        {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            org.move_account(ACCT_C, &root, &parent).unwrap();
        }
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        assert!(instance_of(&set, ACCT_C).is_some(), "{set:?}");
        assert_eq!(queue_count(&svc, ACCT_C), 1);
    }

    #[tokio::test]
    async fn auto_deployment_honors_the_account_filter() {
        let svc = service();
        let (workloads, _prod) = seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
            ],
        )
        .await;
        // Deploy to the OU, but only to ACCT_B.
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("DeploymentTargets.Accounts.member.1", ACCT_B),
                ("DeploymentTargets.AccountFilterType", "INTERSECTION"),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        let set = stored_set(&svc, "org");
        assert_eq!(set.instances.len(), 1);
        assert_eq!(set.instances[0].account, ACCT_B);

        // Reconciling must not deploy to the account the filter left out.
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        assert_eq!(set.instances.len(), 1, "{set:?}");
        assert_eq!(queue_count(&svc, ACCT_C), 0);
    }

    #[tokio::test]
    async fn auto_deployment_reattributes_an_account_moved_between_target_ous() {
        let svc = service();
        let (workloads, prod) = seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
            ],
        )
        .await;
        // Two sibling OUs, each a target in the same region.
        let other = {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            org.create_ou(&root, "sandbox").unwrap().id
        };
        join_ou(&svc, ACCT_D, &other);
        for ou in [&workloads, &other] {
            ok(
                &svc,
                "CreateStackInstances",
                &[
                    ("StackSetName", "org"),
                    ("DeploymentTargets.OrganizationalUnitIds.member.1", ou),
                    ("Regions.member.1", "us-east-1"),
                ],
            )
            .await;
        }
        assert_eq!(
            instance_of(&stored_set(&svc, "org"), ACCT_D)
                .unwrap()
                .organizational_unit_id
                .as_deref(),
            Some(other.as_str())
        );

        // Moving between two target OUs keeps the stack but re-attributes it.
        {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            org.move_account(ACCT_D, &other, &prod).unwrap();
        }
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        let instance = instance_of(&set, ACCT_D).expect("kept");
        assert_eq!(
            instance.organizational_unit_id.as_deref(),
            Some(workloads.as_str())
        );
        assert_eq!(queue_count(&svc, ACCT_D), 1);
    }

    #[tokio::test]
    async fn auto_deployment_keeps_following_an_ou_that_ran_empty() {
        let svc = service();
        let (_workloads, prod) = seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
            ],
        )
        .await;
        // `prod` holds exactly one account.
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                ("DeploymentTargets.OrganizationalUnitIds.member.1", &prod),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        assert_eq!(stored_set(&svc, "org").instances.len(), 1);

        // It leaves, so the stack set has no instances at all left.
        {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            org.move_account(ACCT_C, &prod, &root).unwrap();
        }
        svc.reconcile_auto_deployments().await;
        assert!(stored_set(&svc, "org").instances.is_empty());

        // The OU is still the stack set's target, so an account created in it
        // afterwards is deployed to.
        join_ou(&svc, ACCT_D, &prod);
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        let instance = instance_of(&set, ACCT_D).expect("deployed into the empty OU");
        assert_eq!(
            instance.organizational_unit_id.as_deref(),
            Some(prod.as_str())
        );
        assert_eq!(queue_count(&svc, ACCT_D), 1);
        let targets = ok(
            &svc,
            "ListStackSetAutoDeploymentTargets",
            &[("StackSetName", "org")],
        )
        .await;
        assert_eq!(tag(&targets, "OrganizationalUnitId"), prod);
    }

    #[tokio::test]
    async fn auto_deployment_moves_stacks_to_the_new_ous_regions() {
        let svc = service();
        let (workloads, prod) = seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
            ],
        )
        .await;
        let sandbox = {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            org.create_ou(&root, "sandbox").unwrap().id
        };
        join_ou(&svc, ACCT_D, &sandbox);
        // Two targets, deployed to different regions.
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                ("DeploymentTargets.OrganizationalUnitIds.member.1", &sandbox),
                ("Regions.member.1", "eu-west-1"),
            ],
        )
        .await;

        // Moving between the two targets moves the stacks with it: the
        // instance in the OU it left must not be orphaned.
        {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            org.move_account(ACCT_D, &sandbox, &prod).unwrap();
        }
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        let mine: Vec<&StackInstance> = set
            .instances
            .iter()
            .filter(|i| i.account == ACCT_D)
            .collect();
        assert_eq!(mine.len(), 1, "{set:?}");
        assert_eq!(mine[0].region, "us-east-1");
        assert_eq!(
            mine[0].organizational_unit_id.as_deref(),
            Some(workloads.as_str())
        );
        assert_eq!(queue_count(&svc, ACCT_D), 1);
    }

    #[tokio::test]
    async fn auto_deployment_keeps_the_ou_an_instance_was_deployed_through() {
        let svc = service();
        let (workloads, prod) = seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
            ],
        )
        .await;
        // ACCT_C sits in `prod`, nested under `workloads`; both are targets,
        // each in its own region.
        for (ou, region) in [(&workloads, "us-east-1"), (&prod, "eu-west-1")] {
            ok(
                &svc,
                "CreateStackInstances",
                &[
                    ("StackSetName", "org"),
                    ("DeploymentTargets.OrganizationalUnitIds.member.1", ou),
                    ("Regions.member.1", region),
                ],
            )
            .await;
        }
        let before: Vec<(String, Option<String>)> = stored_set(&svc, "org")
            .instances
            .iter()
            .filter(|i| i.account == ACCT_C)
            .map(|i| (i.region.clone(), i.organizational_unit_id.clone()))
            .collect();
        assert_eq!(before.len(), 2, "{before:?}");

        // Reconciling must not re-attribute either instance to the other OU.
        svc.reconcile_auto_deployments().await;
        let after: Vec<(String, Option<String>)> = stored_set(&svc, "org")
            .instances
            .iter()
            .filter(|i| i.account == ACCT_C)
            .map(|i| (i.region.clone(), i.organizational_unit_id.clone()))
            .collect();
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn a_failed_delete_does_not_exclude_the_account() {
        let svc = service();
        let (workloads, _prod) = auto_deployed_set(&svc, "org", false).await;
        // Termination protection makes the instance's stack undeletable, so
        // the delete fails and the instance stays INOPERABLE.
        {
            let mut accounts = svc.state.write();
            for stack in accounts
                .regional_mut(ACCT_B, "us-east-1")
                .stacks
                .values_mut()
            {
                stack.enable_termination_protection = true;
            }
        }
        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("DeploymentTargets.Accounts.member.1", ACCT_C),
                ("DeploymentTargets.AccountFilterType", "DIFFERENCE"),
                ("Regions.member.1", "us-east-1"),
                ("RetainStacks", "false"),
            ],
        )
        .await;
        let set = stored_set(&svc, "org");
        assert!(instance_of(&set, ACCT_B).is_some(), "{set:?}");
        assert!(
            set.auto_deployment_excluded.is_empty(),
            "a delete that failed must not exclude the account: {:?}",
            set.auto_deployment_excluded
        );
    }

    #[tokio::test]
    async fn auto_deployment_attributes_each_region_to_the_ou_deployed_there() {
        let svc = service();
        let (workloads, prod) = seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
            ],
        )
        .await;
        // Nested targets, each deployed to its own region.
        for (ou, region) in [(&workloads, "us-east-1"), (&prod, "eu-west-1")] {
            ok(
                &svc,
                "CreateStackInstances",
                &[
                    ("StackSetName", "org"),
                    ("DeploymentTargets.OrganizationalUnitIds.member.1", ou),
                    ("Regions.member.1", region),
                ],
            )
            .await;
        }
        // A new account in the nested OU is covered by both, and each of its
        // instances belongs to the OU that is deployed to that region.
        join_ou(&svc, ACCT_D, &prod);
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        let mut mine: Vec<(&str, &str)> = set
            .instances
            .iter()
            .filter(|i| i.account == ACCT_D)
            .map(|i| {
                (
                    i.region.as_str(),
                    i.organizational_unit_id.as_deref().unwrap_or(""),
                )
            })
            .collect();
        mine.sort();
        assert_eq!(
            mine,
            [
                ("eu-west-1", prod.as_str()),
                ("us-east-1", workloads.as_str())
            ],
            "{set:?}"
        );
    }

    #[tokio::test]
    async fn a_deleted_instance_is_excluded_only_in_its_own_region() {
        let svc = service();
        let (workloads, prod) = seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
            ],
        )
        .await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "eu-west-1"),
            ],
        )
        .await;
        // Drop ACCT_C's us-east-1 instance only.
        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("DeploymentTargets.Accounts.member.1", ACCT_B),
                ("DeploymentTargets.AccountFilterType", "DIFFERENCE"),
                ("Regions.member.1", "us-east-1"),
                ("RetainStacks", "false"),
            ],
        )
        .await;

        join_ou(&svc, ACCT_D, &prod);
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        let regions = |account: &str| -> Vec<String> {
            let mut out: Vec<String> = set
                .instances
                .iter()
                .filter(|i| i.account == account)
                .map(|i| i.region.clone())
                .collect();
            out.sort();
            out
        };
        // The removed one stays removed; the other region is untouched.
        assert_eq!(regions(ACCT_C), ["eu-west-1"], "{set:?}");
        // The OU still deploys both regions to a new account.
        assert_eq!(regions(ACCT_D), ["eu-west-1", "us-east-1"], "{set:?}");
    }

    #[tokio::test]
    async fn a_failed_delete_keeps_the_ou_a_target() {
        let svc = service();
        let (workloads, prod) = auto_deployed_set(&svc, "org", false).await;
        {
            let mut accounts = svc.state.write();
            for account in [ACCT_B, ACCT_C] {
                for stack in accounts
                    .regional_mut(account, "us-east-1")
                    .stacks
                    .values_mut()
                {
                    stack.enable_termination_protection = true;
                }
            }
        }
        // A whole-OU delete that cannot delete any stack.
        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("Regions.member.1", "us-east-1"),
                ("RetainStacks", "false"),
            ],
        )
        .await;
        let set = stored_set(&svc, "org");
        assert_eq!(set.instances.len(), 2, "{set:?}");
        assert_eq!(
            set.auto_deployment_targets
                .get(&workloads)
                .map(BTreeSet::len),
            Some(1),
            "a delete that failed must not stop the OU being a target: {:?}",
            set.auto_deployment_targets
        );

        // Reconciling leaves the still-deployed instances alone.
        join_ou(&svc, ACCT_D, &prod);
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        assert!(instance_of(&set, ACCT_B).is_some(), "{set:?}");
        assert!(instance_of(&set, ACCT_D).is_some(), "{set:?}");
    }

    #[tokio::test]
    async fn an_abandoned_reconciliation_releases_its_claim() {
        let svc = service();
        let gate = svc.auto_deployment_gate.clone();
        let claim = AutoDeploymentClaim::take(&svc).expect("first claim");
        // A second trigger only records that a pass is owed.
        assert!(AutoDeploymentClaim::take(&svc).is_none());
        assert!(gate.lock().pending);
        // Finishing a pass with a trigger owed keeps the claim, and leaves the
        // flag for the next lap to consume: a claim dropped before that lap
        // runs still leaves the trigger recorded for whoever claims next.
        let mut claim = claim;
        assert!(claim.another_pass_owed());
        assert!(gate.lock().pending);
        assert!(gate.lock().running);
        // The lap that serves it consumes the flag, as the reconcile loop does.
        gate.lock().pending = false;
        // With nothing owed it releases, in the same lock that read the flag.
        assert!(!claim.another_pass_owed());
        assert!(!gate.lock().running);
        // Dropping the claim (a cancelled request) must not wedge the gate.
        drop(claim);
        assert!(!gate.lock().running);
        assert!(AutoDeploymentClaim::take(&svc).is_some());
    }

    #[tokio::test]
    async fn auto_deployment_retries_an_instance_that_never_deployed() {
        let svc = service();
        let (workloads, prod) = auto_deployed_set(&svc, "org", false).await;
        join_ou(&svc, ACCT_D, &prod);
        // An earlier attempt left a record behind without ever creating the
        // stack — a target cancelled when a sibling account's deploy failed.
        svc.with_stack_set(ADMIN, &stored_set(&svc, "org").stack_set_id, |set| {
            set.instances.push(StackInstance {
                account: ACCT_D.to_string(),
                region: "us-east-1".to_string(),
                stack_id: None,
                status: "OUTDATED".to_string(),
                detailed_status: "CANCELLED".to_string(),
                status_reason: Some("Cancelled since failure tolerance has exceeded".to_string()),
                parameter_overrides: BTreeMap::new(),
                organizational_unit_id: Some(workloads.clone()),
                drift_status: "NOT_CHECKED".to_string(),
                last_drift_check_timestamp: None,
                last_operation_id: None,
            });
        });

        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        let instance = instance_of(&set, ACCT_D).expect("instance");
        assert_eq!(instance.status, "CURRENT", "{instance:?}");
        assert!(instance.stack_id.is_some(), "{instance:?}");
        assert_eq!(queue_count(&svc, ACCT_D), 1);
    }

    #[tokio::test]
    async fn auto_deployment_does_not_let_one_account_cancel_the_others() {
        let svc = service();
        let (_workloads, prod) = auto_deployed_set(&svc, "org", false).await;
        join_ou(&svc, ACCT_D, &prod);
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        let op = set
            .operations
            .iter()
            .find(|o| o.action == "CREATE" && o.results.iter().any(|r| r.account == ACCT_D))
            .expect("auto-deployment operation");
        // Accounts joined the OU independently, so a failure in one must not
        // cancel the rest of the deployment.
        assert_eq!(op.preferences.failure_tolerance_percentage, Some(100));
        assert_eq!(op.preferences.failure_tolerance_count, None);
    }

    #[tokio::test]
    async fn auto_deployment_leaves_imported_instances_alone() {
        let svc = service();
        let (workloads, _prod) = auto_deployed_set(&svc, "org", false).await;
        // A stack in an account under a *different* OU, adopted into the set.
        let sandbox = {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            org.create_ou(&root, "sandbox").unwrap().id
        };
        join_ou(&svc, ACCT_D, &sandbox);
        let xml = call_as(
            &svc,
            ACCT_D,
            "CreateStack",
            &[("StackName", "legacy"), ("TemplateBody", QUEUE_TEMPLATE)],
        )
        .await
        .unwrap();
        let stack_id = tag(&xml, "StackId");
        let xml = ok(
            &svc,
            "ImportStacksToStackSet",
            &[
                ("StackSetName", "org"),
                ("StackIds.member.1", &stack_id),
                ("OrganizationalUnitIds.member.1", &sandbox),
            ],
        )
        .await;
        let op_id = tag(&xml, "OperationId");
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "org"), ("OperationId", &op_id)],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "SUCCEEDED", "{op}");
        assert_eq!(queue_count(&svc, ACCT_D), 1);

        // Reconciling must not tear the adopted stack down: importing into an
        // OU makes it one of the stack set's targets.
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        assert!(instance_of(&set, ACCT_D).is_some(), "{set:?}");
        assert_eq!(queue_count(&svc, ACCT_D), 1);
        assert!(set.auto_deployment_targets.contains_key(&sandbox));
        assert!(set.auto_deployment_targets.contains_key(&workloads));
    }

    #[tokio::test]
    async fn a_retried_instance_keeps_its_parameter_overrides() {
        let svc = service();
        let (workloads, _prod) = auto_deployed_set(&svc, "org", false).await;
        // ACCT_B's instance was created with an override and then lost its
        // stack before it ever deployed.
        let set_id = stored_set(&svc, "org").stack_set_id;
        svc.with_stack_set(ADMIN, &set_id, |set| {
            let instance = set
                .instances
                .iter_mut()
                .find(|i| i.account == ACCT_B)
                .unwrap();
            instance.stack_id = None;
            instance.detailed_status = "CANCELLED".to_string();
            instance.parameter_overrides =
                BTreeMap::from([("Env".to_string(), "prod".to_string())]);
            instance.organizational_unit_id = Some(workloads.clone());
        });

        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        let instance = instance_of(&set, ACCT_B).expect("instance");
        assert!(instance.stack_id.is_some(), "{instance:?}");
        assert_eq!(
            instance.parameter_overrides.get("Env").map(String::as_str),
            Some("prod"),
            "{instance:?}"
        );
        let stack = stack_of(&svc, ACCT_B, instance.stack_id.as_deref().unwrap());
        assert_eq!(
            stack.parameters.get("Env").map(String::as_str),
            Some("prod")
        );
    }

    #[tokio::test]
    async fn a_second_create_does_not_exclude_a_live_instance() {
        let svc = service();
        let (workloads, _prod) = auto_deployed_set(&svc, "org", false).await;
        // A narrower second call adds a region for ACCT_B only. ACCT_C's
        // existing us-east-1 instance was not "left out" by it.
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("DeploymentTargets.Accounts.member.1", ACCT_B),
                ("DeploymentTargets.AccountFilterType", "INTERSECTION"),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "eu-west-1"),
            ],
        )
        .await;
        let set = stored_set(&svc, "org");
        assert!(
            !set.auto_deployment_excluded.contains(&(
                workloads.clone(),
                ACCT_C.to_string(),
                "us-east-1".to_string()
            )),
            "{:?}",
            set.auto_deployment_excluded
        );
        // ACCT_C was left out of eu-west-1 though, so it is not deployed there.
        assert!(set.auto_deployment_excluded.contains(&(
            workloads.clone(),
            ACCT_C.to_string(),
            "eu-west-1".to_string()
        )));
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        let mut regions: Vec<&str> = set
            .instances
            .iter()
            .filter(|i| i.account == ACCT_C)
            .map(|i| i.region.as_str())
            .collect();
        regions.sort();
        assert_eq!(regions, ["us-east-1"], "{set:?}");
    }

    /// A cancelled trigger (a client that hung up mid-mutation) must not
    /// leave the stack set with an operation nothing will ever finish.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_reconciliation_does_not_strand_an_operation() {
        let svc = service();
        let (_workloads, prod) = auto_deployed_set(&svc, "org", false).await;
        join_ou(&svc, ACCT_D, &prod);
        let running = {
            let svc = svc.clone();
            tokio::spawn(async move { svc.reconcile_auto_deployments().await })
        };
        // Drop the caller while it is deploying.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        running.abort();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let set = stored_set(&svc, "org");
            let stuck = set
                .operations
                .iter()
                .any(|o| matches!(o.status.as_str(), "RUNNING" | "STOPPING"));
            if !stuck {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "operation left RUNNING after the trigger was cancelled: {:?}",
                set.operations
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        // The work still lands: the cancelled pass was carried on, and a
        // later trigger reconciles whatever it did not reach. Polled, since
        // a trigger that arrives while the carried-on pass still holds the
        // gate is served by that pass rather than inline.
        svc.reconcile_auto_deployments().await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while instance_of(&stored_set(&svc, "org"), ACCT_D).is_none_or(|i| i.stack_id.is_none()) {
            assert!(
                std::time::Instant::now() < deadline,
                "the account that joined never got its instance"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn auto_deployment_does_not_retry_a_target_that_failed() {
        let svc = service();
        let (workloads, prod) = auto_deployed_set(&svc, "org", false).await;
        join_ou(&svc, ACCT_D, &prod);
        let set_id = stored_set(&svc, "org").stack_set_id;
        // A deploy that was tried and failed — a denying account gate, say.
        svc.with_stack_set(ADMIN, &set_id, |set| {
            set.instances.push(StackInstance {
                account: ACCT_D.to_string(),
                region: "us-east-1".to_string(),
                stack_id: None,
                status: "OUTDATED".to_string(),
                detailed_status: "FAILED".to_string(),
                status_reason: Some("Account gate check failed".to_string()),
                parameter_overrides: BTreeMap::new(),
                organizational_unit_id: Some(workloads.clone()),
                drift_status: "NOT_CHECKED".to_string(),
                last_drift_check_timestamp: None,
                last_operation_id: None,
            });
        });
        let before = stored_set(&svc, "org").operations.len();

        // Unrelated organization changes must not re-run it every time.
        for _ in 0..3 {
            svc.reconcile_auto_deployments().await;
        }
        let set = stored_set(&svc, "org");
        assert_eq!(set.operations.len(), before, "{:?}", set.operations);
        assert_eq!(instance_of(&set, ACCT_D).unwrap().detailed_status, "FAILED");
    }

    #[tokio::test]
    async fn updating_a_stack_set_keeps_auto_deployment_bookkeeping() {
        let svc = service();
        let (workloads, _prod) = auto_deployed_set(&svc, "org", false).await;
        let set_id = stored_set(&svc, "org").stack_set_id;
        // Bookkeeping a reconciliation wrote without recording an operation.
        svc.with_stack_set(ADMIN, &set_id, |set| {
            set.auto_deployment_excluded.insert((
                workloads.clone(),
                ACCT_D.to_string(),
                "us-east-1".to_string(),
            ));
        });

        ok(
            &svc,
            "UpdateStackSet",
            &[
                ("StackSetName", "org"),
                ("UsePreviousTemplate", "true"),
                ("Description", "second revision"),
            ],
        )
        .await;
        let set = stored_set(&svc, "org");
        assert!(
            set.auto_deployment_excluded.contains(&(
                workloads.clone(),
                ACCT_D.to_string(),
                "us-east-1".to_string()
            )),
            "{:?}",
            set.auto_deployment_excluded
        );
        assert!(set.auto_deployment_targets.contains_key(&workloads));
    }

    #[tokio::test]
    async fn importing_one_stack_does_not_deploy_the_rest_of_its_ou() {
        let svc = service();
        let (_workloads, _prod) = auto_deployed_set(&svc, "org", false).await;
        let sandbox = {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            org.create_ou(&root, "sandbox").unwrap().id
        };
        join_ou(&svc, ACCT_D, &sandbox);
        // A second account in the same OU, with no stack of its own.
        const ACCT_E: &str = "444444444444";
        join_ou(&svc, ACCT_E, &sandbox);
        let xml = call_as(
            &svc,
            ACCT_D,
            "CreateStack",
            &[("StackName", "legacy"), ("TemplateBody", QUEUE_TEMPLATE)],
        )
        .await
        .unwrap();
        let stack_id = tag(&xml, "StackId");
        ok(
            &svc,
            "ImportStacksToStackSet",
            &[
                ("StackSetName", "org"),
                ("StackIds.member.1", &stack_id),
                ("OrganizationalUnitIds.member.1", &sandbox),
            ],
        )
        .await;

        // Adopting one account's stack must not deploy the template into the
        // OU's other accounts.
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        assert!(instance_of(&set, ACCT_D).is_some(), "{set:?}");
        assert!(instance_of(&set, ACCT_E).is_none(), "{set:?}");
        assert_eq!(queue_count(&svc, ACCT_E), 0);
    }

    #[tokio::test]
    async fn a_refused_import_does_not_make_its_ou_a_target() {
        let svc = service();
        auto_deployed_set(&svc, "org", false).await;
        let sandbox = {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            org.create_ou(&root, "sandbox").unwrap().id
        };
        join_ou(&svc, ACCT_D, &sandbox);
        // A stack whose template does not match the stack set's: adopted as
        // FAILED_IMPORT, so nothing is actually deployed in that OU.
        let xml = call_as(
            &svc,
            ACCT_D,
            "CreateStack",
            &[("StackName", "other"), ("TemplateBody", TOPIC_TEMPLATE)],
        )
        .await
        .unwrap();
        let stack_id = tag(&xml, "StackId");
        ok(
            &svc,
            "ImportStacksToStackSet",
            &[
                ("StackSetName", "org"),
                ("StackIds.member.1", &stack_id),
                ("OrganizationalUnitIds.member.1", &sandbox),
            ],
        )
        .await;
        let set = stored_set(&svc, "org");
        assert_eq!(
            instance_of(&set, ACCT_D).map(|i| i.detailed_status.as_str()),
            Some("FAILED_IMPORT"),
            "{set:?}"
        );
        assert!(
            !set.auto_deployment_targets.contains_key(&sandbox),
            "{:?}",
            set.auto_deployment_targets
        );
    }

    /// A reconciliation whose caller is cancelled part way still finishes:
    /// the work moves to a task of its own rather than being forgotten.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_pass_is_carried_on() {
        let svc = service();
        let (_workloads, prod) = auto_deployed_set(&svc, "org", false).await;
        join_ou(&svc, ACCT_D, &prod);
        // A pass is under way — nobody else can claim the gate — and the
        // request running it is cancelled.
        let claim = AutoDeploymentClaim::take(&svc).expect("claim");
        assert!(AutoDeploymentClaim::take(&svc).is_none());
        drop(claim);

        // An instance is recorded before its stack is provisioned, as in
        // AWS, so wait for the stack itself rather than for the record.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while instance_of(&stored_set(&svc, "org"), ACCT_D).is_none_or(|i| i.stack_id.is_none()) {
            assert!(
                std::time::Instant::now() < deadline,
                "the interrupted reconciliation never ran"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert_eq!(queue_count(&svc, ACCT_D), 1);
    }

    #[tokio::test]
    async fn auto_deployment_never_deletes_a_refused_imports_stack() {
        let svc = service();
        let (_workloads, prod) = auto_deployed_set(&svc, "org", false).await;
        let sandbox = {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            org.create_ou(&root, "sandbox").unwrap().id
        };
        join_ou(&svc, ACCT_D, &sandbox);
        // An import refused for a template mismatch records the operator's
        // own stack against the stack set without adopting it.
        let xml = call_as(
            &svc,
            ACCT_D,
            "CreateStack",
            &[("StackName", "mine"), ("TemplateBody", TOPIC_TEMPLATE)],
        )
        .await
        .unwrap();
        let stack_id = tag(&xml, "StackId");
        ok(
            &svc,
            "ImportStacksToStackSet",
            &[
                ("StackSetName", "org"),
                ("StackIds.member.1", &stack_id),
                ("OrganizationalUnitIds.member.1", &sandbox),
            ],
        )
        .await;
        let topics = || {
            svc.deps
                .sns
                .read()
                .get(ACCT_D)
                .map_or(0, |s| s.topics.len())
        };
        assert_eq!(topics(), 1);

        // Reconciling must not delete a stack the stack set never deployed.
        join_ou(&svc, "444444444444", &prod);
        svc.reconcile_auto_deployments().await;
        assert_eq!(topics(), 1, "the operator's own stack was deleted");
        assert_eq!(stack_of(&svc, ACCT_D, &stack_id).status, "CREATE_COMPLETE");
    }

    #[test]
    fn restoring_a_stack_set_ignores_refused_imports() {
        let mut accounts =
            MultiAccountState::<CloudFormationAccountState>::new(ADMIN, "us-east-1", "");
        let set = StackSet {
            stack_set_id: "org:1".to_string(),
            name: "org".to_string(),
            arn: String::new(),
            status: "ACTIVE".to_string(),
            description: None,
            template_body: String::new(),
            parameters: BTreeMap::new(),
            capabilities: Vec::new(),
            tags: Vec::new(),
            administration_role_arn: None,
            execution_role_name: None,
            permission_model: "SERVICE_MANAGED".to_string(),
            auto_deployment: Some(AutoDeployment {
                enabled: true,
                retain_stacks_on_account_removal: false,
            }),
            auto_deployment_targets: BTreeMap::new(),
            auto_deployment_excluded: BTreeSet::new(),
            managed_execution_active: false,
            instances: vec![
                StackInstance {
                    account: ACCT_B.to_string(),
                    region: "us-east-1".to_string(),
                    stack_id: Some("stack-b".to_string()),
                    status: "CURRENT".to_string(),
                    detailed_status: "SUCCEEDED".to_string(),
                    status_reason: None,
                    parameter_overrides: BTreeMap::new(),
                    organizational_unit_id: Some("ou-kept".to_string()),
                    drift_status: "NOT_CHECKED".to_string(),
                    last_drift_check_timestamp: None,
                    last_operation_id: None,
                },
                StackInstance {
                    account: ACCT_C.to_string(),
                    region: "us-east-1".to_string(),
                    stack_id: Some("stack-c".to_string()),
                    status: "OUTDATED".to_string(),
                    detailed_status: "FAILED_IMPORT".to_string(),
                    status_reason: None,
                    parameter_overrides: BTreeMap::new(),
                    organizational_unit_id: Some("ou-refused".to_string()),
                    drift_status: "NOT_CHECKED".to_string(),
                    last_drift_check_timestamp: None,
                    last_operation_id: None,
                },
            ],
            operations: Vec::new(),
            drift: None,
            created_at: Utc::now(),
        };
        accounts
            .regional_mut(ADMIN, "us-east-1")
            .stack_sets
            .insert(set.stack_set_id.clone(), set);

        restore_stack_sets(&mut accounts);
        let set = &accounts.regional(ADMIN, "us-east-1").unwrap().stack_sets["org:1"];
        assert!(set.auto_deployment_targets.contains_key("ou-kept"));
        assert!(
            !set.auto_deployment_targets.contains_key("ou-refused"),
            "{:?}",
            set.auto_deployment_targets
        );
    }

    #[tokio::test]
    async fn bookkeeping_only_reconciliation_is_persisted() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cloudformation.json");
        let svc = service().with_snapshot_store(std::sync::Arc::new(
            fakecloud_persistence::DiskSnapshotStore::new(path.clone()),
        ));
        let (workloads, prod) = seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
            ],
        )
        .await;
        let sandbox = {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            org.create_ou(&root, "sandbox").unwrap().id
        };
        join_ou(&svc, ACCT_D, &sandbox);
        // Two targets covering the same region, so moving between them is a
        // re-attribution and nothing else: no operation to persist behind.
        for ou in [&workloads, &sandbox] {
            ok(
                &svc,
                "CreateStackInstances",
                &[
                    ("StackSetName", "org"),
                    ("DeploymentTargets.OrganizationalUnitIds.member.1", ou),
                    ("Regions.member.1", "us-east-1"),
                ],
            )
            .await;
        }
        std::fs::remove_file(&path).ok();
        {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            org.move_account(ACCT_D, &sandbox, &prod).unwrap();
        }

        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        assert_eq!(
            instance_of(&set, ACCT_D)
                .unwrap()
                .organizational_unit_id
                .as_deref(),
            Some(workloads.as_str())
        );
        // The re-attribution has to reach disk: nothing else will write it.
        let snapshot = std::fs::read_to_string(&path).expect("snapshot written");
        assert!(snapshot.contains(&workloads), "{snapshot}");
        assert!(snapshot.contains(ACCT_D), "{snapshot}");
    }

    #[tokio::test]
    async fn a_busy_stack_set_gets_one_waiter_not_one_per_change() {
        let svc = service();
        auto_deployed_set(&svc, "org", false).await;
        let set_id = stored_set(&svc, "org").stack_set_id;
        // A burst of organization changes against a stack set that is busy
        // must not pile up a poller each.
        for _ in 0..5 {
            svc.schedule_auto_deployment_retry(ADMIN, &set_id);
        }
        assert_eq!(
            svc.auto_deployment_retries.lock().len(),
            1,
            "{:?}",
            svc.auto_deployment_retries.lock()
        );
    }

    #[tokio::test]
    async fn a_deleted_instance_deploys_again_in_another_target_ou() {
        let svc = service();
        let (workloads, _prod) = auto_deployed_set(&svc, "org", false).await;
        let sandbox = {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            org.create_ou(&root, "sandbox").unwrap().id
        };
        join_ou(&svc, ACCT_D, &sandbox);
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                ("DeploymentTargets.OrganizationalUnitIds.member.1", &sandbox),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        // The operator removes ACCT_C's instance while it sits in `workloads`.
        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("DeploymentTargets.Accounts.member.1", ACCT_B),
                ("DeploymentTargets.AccountFilterType", "DIFFERENCE"),
                ("Regions.member.1", "us-east-1"),
                ("RetainStacks", "false"),
            ],
        )
        .await;
        svc.reconcile_auto_deployments().await;
        assert!(instance_of(&stored_set(&svc, "org"), ACCT_C).is_none());

        // Moving it into a different target OU is a membership change like
        // any other: the decision was about the OU it left.
        {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let parent = org.parent_of(ACCT_C).unwrap().0;
            org.move_account(ACCT_C, &parent, &sandbox).unwrap();
        }
        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        let instance = instance_of(&set, ACCT_C).expect("redeployed in the new OU");
        assert_eq!(
            instance.organizational_unit_id.as_deref(),
            Some(sandbox.as_str())
        );
        assert_eq!(queue_count(&svc, ACCT_C), 1);
    }

    #[tokio::test]
    async fn a_waiter_can_leave_another_waiter_behind_it() {
        let svc = service();
        auto_deployed_set(&svc, "org", false).await;
        let set_id = stored_set(&svc, "org").stack_set_id;
        // While one waiter is marked, a second is suppressed...
        svc.schedule_auto_deployment_retry(ADMIN, &set_id);
        assert_eq!(svc.auto_deployment_retries.lock().len(), 1);
        // ...but once it stands down, the reconciliation it runs can leave a
        // fresh one, so a stack set that is busy again is not forgotten.
        svc.auto_deployment_retries.lock().clear();
        svc.schedule_auto_deployment_retry(ADMIN, &set_id);
        assert_eq!(svc.auto_deployment_retries.lock().len(), 1);
    }

    #[tokio::test]
    async fn turning_auto_deployment_off_stops_a_pass_in_its_tracks() {
        let svc = service();
        let (_workloads, prod) = auto_deployed_set(&svc, "org", false).await;
        join_ou(&svc, ACCT_D, &prod);
        // The operator turns auto-deployment off between the scan that picked
        // the stack set and the pass that plans it.
        ok(
            &svc,
            "UpdateStackSet",
            &[
                ("StackSetName", "org"),
                ("UsePreviousTemplate", "true"),
                ("AutoDeployment.Enabled", "false"),
            ],
        )
        .await;

        svc.reconcile_auto_deployments().await;
        let set = stored_set(&svc, "org");
        assert!(instance_of(&set, ACCT_D).is_none(), "{set:?}");
        assert_eq!(queue_count(&svc, ACCT_D), 0);
    }

    #[tokio::test]
    async fn a_wholly_refused_import_still_leaves_the_ous_accounts_out() {
        let svc = service();
        auto_deployed_set(&svc, "org", false).await;
        let sandbox = {
            let mut guard = svc.deps.organizations.write();
            let org = guard.sole_mut().unwrap();
            let root = org.root_id.clone();
            org.create_ou(&root, "sandbox").unwrap().id
        };
        join_ou(&svc, ACCT_D, &sandbox);
        const ACCT_E: &str = "444444444444";
        join_ou(&svc, ACCT_E, &sandbox);
        // The only stack named is refused, so nothing is adopted.
        let xml = call_as(
            &svc,
            ACCT_D,
            "CreateStack",
            &[("StackName", "other"), ("TemplateBody", TOPIC_TEMPLATE)],
        )
        .await
        .unwrap();
        let stack_id = tag(&xml, "StackId");
        ok(
            &svc,
            "ImportStacksToStackSet",
            &[
                ("StackSetName", "org"),
                ("StackIds.member.1", &stack_id),
                ("OrganizationalUnitIds.member.1", &sandbox),
            ],
        )
        .await;
        let set = stored_set(&svc, "org");
        // The account whose stack was refused was asked for, so it is not
        // recorded as deliberately left out...
        assert!(
            !set.auto_deployment_excluded.contains(&(
                sandbox.clone(),
                ACCT_D.to_string(),
                "us-east-1".to_string()
            )),
            "{:?}",
            set.auto_deployment_excluded
        );
        // ...while the account nobody mentioned is.
        assert!(
            set.auto_deployment_excluded.contains(&(
                sandbox.clone(),
                ACCT_E.to_string(),
                "us-east-1".to_string()
            )),
            "{:?}",
            set.auto_deployment_excluded
        );
    }

    #[tokio::test]
    async fn auto_deployment_leaves_other_stack_sets_alone() {
        let svc = service();
        let (workloads, prod) = seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        // Same OU target, but auto-deployment is off.
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "manual"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "false"),
            ],
        )
        .await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "manual"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        // A self-managed set deployed to named accounts is never touched either.
        create_set(&svc, "self", TOPIC_TEMPLATE).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "self"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;

        join_ou(&svc, ACCT_D, &prod);
        svc.reconcile_auto_deployments().await;

        let manual = stored_set(&svc, "manual");
        assert!(instance_of(&manual, ACCT_D).is_none(), "{manual:?}");
        assert_eq!(manual.operations.len(), 1);
        let self_managed = stored_set(&svc, "self");
        assert_eq!(self_managed.instances.len(), 1);
        assert_eq!(self_managed.operations.len(), 1);
    }

    #[tokio::test]
    async fn service_managed_stack_sets_deploy_to_organizational_units() {
        let svc = service();
        let (workloads, prod) = seed_org(&svc);
        let create = [
            ("StackSetName", "org"),
            ("TemplateBody", QUEUE_TEMPLATE),
            ("PermissionModel", "SERVICE_MANAGED"),
            ("AutoDeployment.Enabled", "true"),
            ("AutoDeployment.RetainStacksOnAccountRemoval", "false"),
        ];
        // Trusted access has to be activated first.
        let e = err(&svc, "CreateStackSet", &create).await;
        assert_eq!(e.code(), "ValidationError");
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(&svc, "CreateStackSet", &create).await;

        // Top-level accounts are not a valid target for this model.
        let e = err(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        assert_eq!(e.code(), "ValidationError");

        // The OU covers its nested OU; the management account is never a target.
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        let set = stored_set(&svc, "org");
        let mut accounts: Vec<&str> = set.instances.iter().map(|i| i.account.as_str()).collect();
        accounts.sort();
        assert_eq!(accounts, [ACCT_B, ACCT_C]);
        assert!(set
            .instances
            .iter()
            .all(|i| i.organizational_unit_id.as_deref() == Some(workloads.as_str())));
        assert_eq!(queue_count(&svc, ACCT_C), 1);

        let targets = ok(
            &svc,
            "ListStackSetAutoDeploymentTargets",
            &[("StackSetName", "org")],
        )
        .await;
        assert_eq!(tag(&targets, "OrganizationalUnitId"), workloads);
        assert!(targets.contains("<member>us-east-1</member>"), "{targets}");
        let described = ok(&svc, "DescribeStackSet", &[("StackSetName", "org")]).await;
        assert!(
            described.contains(&format!("<member>{workloads}</member>")),
            "{described}"
        );
        assert!(described.contains("<Enabled>true</Enabled>"), "{described}");

        // DIFFERENCE removes the listed account from the OU's instances.
        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("DeploymentTargets.Accounts.member.1", ACCT_B),
                ("DeploymentTargets.AccountFilterType", "DIFFERENCE"),
                ("Regions.member.1", "us-east-1"),
                ("RetainStacks", "false"),
            ],
        )
        .await;
        let set = stored_set(&svc, "org");
        assert_eq!(set.instances.len(), 1);
        assert_eq!(set.instances[0].account, ACCT_B);
        assert_eq!(queue_count(&svc, ACCT_C), 0);

        // A nested OU targets only its own accounts.
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                ("DeploymentTargets.OrganizationalUnitIds.member.1", &prod),
                ("Regions.member.1", "eu-west-1"),
            ],
        )
        .await;
        let set = stored_set(&svc, "org");
        let eu: Vec<&StackInstance> = set
            .instances
            .iter()
            .filter(|i| i.region == "eu-west-1")
            .collect();
        assert_eq!(eu.len(), 1);
        assert_eq!(eu[0].account, ACCT_C);

        // Instances deployed through a nested OU are reached through a parent.
        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("Regions.member.1", "eu-west-1"),
                ("RetainStacks", "false"),
            ],
        )
        .await;
        assert!(stored_set(&svc, "org")
            .instances
            .iter()
            .all(|i| i.region != "eu-west-1"));

        let e = err(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    "ou-none-00000000",
                ),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        assert_eq!(e.code(), "ValidationError");
    }

    #[tokio::test]
    async fn a_delegated_administrator_acts_on_the_management_accounts_stack_sets() {
        let svc = service();
        seed_org(&svc);
        {
            let mut orgs = svc.deps.organizations.write();
            let org = orgs.sole_mut().unwrap();
            org.enable_aws_service_access(STACKSETS_PRINCIPAL);
            org.register_delegated_administrator(ACCT_B, STACKSETS_PRINCIPAL)
                .unwrap();
        }
        let params = [
            ("StackSetName", "delegated"),
            ("TemplateBody", QUEUE_TEMPLATE),
            ("PermissionModel", "SERVICE_MANAGED"),
            ("CallAs", "DELEGATED_ADMIN"),
        ];
        call_as(&svc, ACCT_B, "CreateStackSet", &params)
            .await
            .unwrap();
        // Stored with the management account, visible to it too.
        assert_eq!(
            stored_set(&svc, "delegated").permission_model,
            "SERVICE_MANAGED"
        );
        let listed = call_as(
            &svc,
            ACCT_B,
            "ListStackSets",
            &[("CallAs", "DELEGATED_ADMIN")],
        )
        .await
        .unwrap();
        assert!(
            listed.contains("<StackSetName>delegated</StackSetName>"),
            "{listed}"
        );
        let summary = call_as(
            &svc,
            ACCT_B,
            "GetTemplateSummary",
            &[("StackSetName", "delegated"), ("CallAs", "DELEGATED_ADMIN")],
        )
        .await
        .unwrap();
        assert!(summary.contains("AWS::SQS::Queue"), "{summary}");

        // An account that is not registered cannot.
        let e = call_as(
            &svc,
            ACCT_C,
            "ListStackSets",
            &[("CallAs", "DELEGATED_ADMIN")],
        )
        .await
        .unwrap_err();
        assert_eq!(e.code(), "ValidationError");
    }

    #[tokio::test]
    async fn existing_stacks_import_into_a_stack_set() {
        let svc = service();
        let xml = call_as(
            &svc,
            ACCT_B,
            "CreateStack",
            &[("StackName", "legacy"), ("TemplateBody", QUEUE_TEMPLATE)],
        )
        .await
        .unwrap();
        let stack_id = tag(&xml, "StackId");
        create_set(&svc, "adopt", QUEUE_TEMPLATE).await;

        let xml = ok(
            &svc,
            "ImportStacksToStackSet",
            &[("StackSetName", "adopt"), ("StackIds.member.1", &stack_id)],
        )
        .await;
        let op_id = tag(&xml, "OperationId");
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "adopt"), ("OperationId", &op_id)],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "SUCCEEDED", "{op}");
        let set = stored_set(&svc, "adopt");
        assert_eq!(set.instances.len(), 1);
        assert_eq!(
            set.instances[0].stack_id.as_deref(),
            Some(stack_id.as_str())
        );
        assert_eq!(set.instances[0].account, ACCT_B);
        assert_eq!(set.instances[0].status, "CURRENT");

        // A stack can belong to one stack set only.
        create_set(&svc, "other", QUEUE_TEMPLATE).await;
        let xml = ok(
            &svc,
            "ImportStacksToStackSet",
            &[("StackSetName", "other"), ("StackIds.member.1", &stack_id)],
        )
        .await;
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[
                ("StackSetName", "other"),
                ("OperationId", &tag(&xml, "OperationId")),
            ],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "FAILED", "{op}");
        assert!(stored_set(&svc, "other").instances.is_empty());

        let e = err(
            &svc,
            "ImportStacksToStackSet",
            &[
                ("StackSetName", "other"),
                (
                    "StackIds.member.1",
                    "arn:aws:cloudformation:us-east-1:111111111111:stack/ghost/1",
                ),
            ],
        )
        .await;
        assert_eq!(e.code(), "StackNotFoundException");

        // A stack set can also be created from a stack's template.
        let xml = call_as(
            &svc,
            ACCT_B,
            "CreateStackSet",
            &[("StackSetName", "from-stack"), ("StackId", &stack_id)],
        )
        .await
        .unwrap();
        assert!(xml.contains("<StackSetId>from-stack:"), "{xml}");
        let described = call_as(
            &svc,
            ACCT_B,
            "DescribeStackSet",
            &[("StackSetName", "from-stack")],
        )
        .await
        .unwrap();
        assert!(described.contains("AWS::SQS::Queue"), "{described}");
    }

    #[tokio::test]
    async fn stack_set_drift_detection_checks_each_instance() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "us-west-2"),
            ],
        )
        .await;
        // Delete one instance's queue behind CloudFormation's back.
        let set = stored_set(&svc, "app");
        let east = set
            .instances
            .iter()
            .find(|i| i.region == "us-east-1")
            .unwrap();
        let queue_url = stack_of(&svc, ACCT_B, east.stack_id.as_deref().unwrap()).resources[0]
            .physical_id
            .clone();
        svc.deps
            .sqs
            .write()
            .get_or_create(ACCT_B)
            .queues
            .remove(&queue_url)
            .expect("queue existed");

        let xml = ok(&svc, "DetectStackSetDrift", &[("StackSetName", "app")]).await;
        let op_id = tag(&xml, "OperationId");
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", &op_id)],
        )
        .await;
        assert_eq!(tag(&op, "Action"), "DETECT_DRIFT");
        assert_eq!(tag(&op, "DriftStatus"), "DRIFTED", "{op}");
        assert_eq!(tag(&op, "DriftedStackInstancesCount"), "1");
        assert_eq!(tag(&op, "InSyncStackInstancesCount"), "1");

        let drifts = ok(
            &svc,
            "ListStackInstanceResourceDrifts",
            &[
                ("StackSetName", "app"),
                ("StackInstanceAccount", ACCT_B),
                ("StackInstanceRegion", "us-east-1"),
                ("OperationId", &op_id),
                ("StackInstanceResourceDriftStatuses.member.1", "DELETED"),
            ],
        )
        .await;
        assert_eq!(
            tag(&drifts, "StackResourceDriftStatus"),
            "DELETED",
            "{drifts}"
        );
        assert_eq!(tag(&drifts, "PhysicalResourceId"), queue_url);

        let in_sync = ok(
            &svc,
            "ListStackInstances",
            &[
                ("StackSetName", "app"),
                ("Filters.member.1.Name", "DRIFT_STATUS"),
                ("Filters.member.1.Values", "IN_SYNC"),
            ],
        )
        .await;
        assert!(in_sync.contains("<Region>us-west-2</Region>"), "{in_sync}");
        assert!(!in_sync.contains("<Region>us-east-1</Region>"), "{in_sync}");
        let summary = ok(&svc, "ListStackSets", &[]).await;
        assert!(
            summary.contains("<DriftStatus>DRIFTED</DriftStatus>"),
            "{summary}"
        );
    }

    /// An account gate that stops the operation it is gating, standing in for
    /// a StopStackSetOperation that lands while a target is deploying.
    struct StoppingGate(std::sync::OnceLock<Arc<CloudFormationService>>);

    impl LambdaDelivery for StoppingGate {
        fn invoke_lambda(
            &self,
            _function_arn: &str,
            _payload: &str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>>
        {
            let svc = self.0.get().cloned();
            Box::pin(async move {
                if let Some(svc) = svc {
                    call(
                        &svc,
                        "StopStackSetOperation",
                        &[("StackSetName", "app"), ("OperationId", "op-stop")],
                    )
                    .await
                    .map_err(|e| e.message())?;
                }
                Ok(br#"{"Status":"SUCCEEDED"}"#.to_vec())
            })
        }
    }

    #[tokio::test]
    async fn stopping_an_operation_mid_deployment_cancels_the_remaining_targets() {
        let gate = Arc::new(StoppingGate(std::sync::OnceLock::new()));
        let mut d = deps();
        d.delivery = Arc::new(DeliveryBus::new().with_lambda(gate.clone()));
        let svc = Arc::new(service_with(d));
        gate.0.set(svc.clone()).ok();
        // Only the first target's account has a gate, so the stop lands while
        // that target is deploying.
        let gate_template = "Resources:\n  Gate:\n    Type: AWS::Lambda::Function\n    Properties:\n      FunctionName: AWSCloudFormationStackSetAccountGate\n      Runtime: python3.12\n      Handler: index.handler\n      Role: arn:aws:iam::111111111111:role/gate\n      Code:\n        ZipFile: \"def handler(e, c): return {}\"\n";
        call_as(
            &svc,
            ACCT_B,
            "CreateStack",
            &[("StackName", "gate"), ("TemplateBody", gate_template)],
        )
        .await
        .unwrap();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Accounts.member.2", ACCT_C),
                ("Regions.member.1", "us-east-1"),
                ("OperationId", "op-stop"),
            ],
        )
        .await;
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "op-stop")],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "STOPPED", "{op}");
        let results = ok(
            &svc,
            "ListStackSetOperationResults",
            &[("StackSetName", "app"), ("OperationId", "op-stop")],
        )
        .await;
        // The target already deploying finishes; the next one never starts.
        assert_eq!(queue_count(&svc, ACCT_B), 1);
        assert_eq!(queue_count(&svc, ACCT_C), 0);
        assert!(results.contains("<Status>SUCCEEDED</Status>"), "{results}");
        assert!(results.contains(OPERATION_STOPPED), "{results}");
    }

    #[tokio::test]
    async fn a_delegated_administrator_cannot_touch_self_managed_stack_sets() {
        let svc = service();
        seed_org(&svc);
        {
            let mut orgs = svc.deps.organizations.write();
            let org = orgs.sole_mut().unwrap();
            org.enable_aws_service_access(STACKSETS_PRINCIPAL);
            org.register_delegated_administrator(ACCT_B, STACKSETS_PRINCIPAL)
                .unwrap();
        }
        create_set(&svc, "mgmt-only", QUEUE_TEMPLATE).await;
        let delegated = [("StackSetName", "mgmt-only"), ("CallAs", "DELEGATED_ADMIN")];
        let e = call_as(&svc, ACCT_B, "DescribeStackSet", &delegated)
            .await
            .unwrap_err();
        assert_eq!(e.code(), "StackSetNotFoundException");
        let mut instances = delegated.to_vec();
        instances.extend([
            ("Accounts.member.1", ACCT_C),
            ("Regions.member.1", "us-east-1"),
        ]);
        let e = call_as(&svc, ACCT_B, "CreateStackInstances", &instances)
            .await
            .unwrap_err();
        assert_eq!(e.code(), "StackSetNotFoundException");
        assert_eq!(queue_count(&svc, ACCT_C), 0);
        let e = call_as(
            &svc,
            ACCT_B,
            "CreateStackSet",
            &[
                ("StackSetName", "sneaky"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("CallAs", "DELEGATED_ADMIN"),
            ],
        )
        .await
        .unwrap_err();
        assert_eq!(e.code(), "ValidationError");
        let listed = call_as(
            &svc,
            ACCT_B,
            "ListStackSets",
            &[("CallAs", "DELEGATED_ADMIN")],
        )
        .await
        .unwrap();
        assert!(!listed.contains("mgmt-only"), "{listed}");
    }

    #[tokio::test]
    async fn an_out_of_range_next_token_is_rejected() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        let e = err(
            &svc,
            "ListStackSets",
            &[("NextToken", "18446744073709551615")],
        )
        .await;
        assert_eq!(e.code(), "ValidationError");
        let page = ok(&svc, "ListStackSets", &[("MaxResults", "1")]).await;
        assert!(!page.contains("<NextToken>"), "{page}");
    }

    #[tokio::test]
    async fn a_stack_set_update_deploys_instances_that_never_got_a_stack() {
        let broken = "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName:\n        Fn::ImportValue: missing-export\n";
        let svc = service();
        create_set(&svc, "fixme", broken).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "fixme"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "us-west-2"),
            ],
        )
        .await;
        assert!(stored_set(&svc, "fixme")
            .instances
            .iter()
            .all(|i| i.stack_id.is_none()));

        let xml = ok(
            &svc,
            "UpdateStackSet",
            &[("StackSetName", "fixme"), ("TemplateBody", QUEUE_TEMPLATE)],
        )
        .await;
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[
                ("StackSetName", "fixme"),
                ("OperationId", &tag(&xml, "OperationId")),
            ],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "SUCCEEDED", "{op}");
        let set = stored_set(&svc, "fixme");
        assert!(set
            .instances
            .iter()
            .all(|i| i.status == "CURRENT" && i.stack_id.is_some()));
        assert_eq!(queue_count(&svc, ACCT_B), 2);
    }

    #[tokio::test]
    async fn suspended_accounts_are_skipped_on_create_and_update() {
        let svc = service();
        let (workloads, _) = seed_org(&svc);
        svc.deps
            .organizations
            .write()
            .sole_mut()
            .unwrap()
            .close_account(ACCT_C)
            .unwrap();
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
            ],
        )
        .await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        let skipped = |svc: &CloudFormationService| {
            stored_set(svc, "org")
                .instances
                .into_iter()
                .find(|i| i.account == ACCT_C)
                .unwrap()
                .detailed_status
        };
        assert_eq!(skipped(&svc), "SKIPPED_SUSPENDED_ACCOUNT");
        assert_eq!(queue_count(&svc, ACCT_C), 0);

        let xml = ok(
            &svc,
            "UpdateStackSet",
            &[("StackSetName", "org"), ("TemplateBody", QUEUE_TEMPLATE)],
        )
        .await;
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[
                ("StackSetName", "org"),
                ("OperationId", &tag(&xml, "OperationId")),
            ],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "SUCCEEDED", "{op}");
        assert_eq!(skipped(&svc), "SKIPPED_SUSPENDED_ACCOUNT");
        assert_eq!(queue_count(&svc, ACCT_B), 1);

        // The suspended account's instance still deletes, so the stack set
        // can be emptied and deleted.
        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("Regions.member.1", "us-east-1"),
                ("RetainStacks", "true"),
            ],
        )
        .await;
        assert!(stored_set(&svc, "org").instances.is_empty());
        ok(&svc, "DeleteStackSet", &[("StackSetName", "org")]).await;
    }

    #[tokio::test]
    async fn a_region_listed_twice_deploys_once() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        let xml = ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("Regions.member.2", "us-east-1"),
            ],
        )
        .await;
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[
                ("StackSetName", "app"),
                ("OperationId", &tag(&xml, "OperationId")),
            ],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "SUCCEEDED", "{op}");
        assert_eq!(stored_set(&svc, "app").instances.len(), 1);
        assert_eq!(queue_count(&svc, ACCT_B), 1);
    }

    #[tokio::test]
    async fn a_delegated_administrator_reads_template_urls_from_its_own_account() {
        let svc = service();
        seed_org(&svc);
        {
            let mut orgs = svc.deps.organizations.write();
            let org = orgs.sole_mut().unwrap();
            org.enable_aws_service_access(STACKSETS_PRINCIPAL);
            org.register_delegated_administrator(ACCT_B, STACKSETS_PRINCIPAL)
                .unwrap();
        }
        {
            let mut s3 = svc.deps.s3.write();
            let state = s3.get_or_create(ACCT_B);
            let mut bucket = fakecloud_s3::S3Bucket::new("templates", "us-east-1", ACCT_B);
            bucket.objects.insert(
                "set.yaml".to_string(),
                fakecloud_s3::S3Object {
                    key: "set.yaml".to_string(),
                    body: fakecloud_s3::memory_body(bytes::Bytes::from_static(
                        QUEUE_TEMPLATE.as_bytes(),
                    )),
                    size: QUEUE_TEMPLATE.len() as u64,
                    ..Default::default()
                },
            );
            state.buckets.insert("templates".to_string(), bucket);
        }
        call_as(
            &svc,
            ACCT_B,
            "CreateStackSet",
            &[
                ("StackSetName", "from-url"),
                ("TemplateURL", "https://templates.s3.amazonaws.com/set.yaml"),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("CallAs", "DELEGATED_ADMIN"),
            ],
        )
        .await
        .unwrap();
        assert_eq!(stored_set(&svc, "from-url").template_body, QUEUE_TEMPLATE);
    }

    #[tokio::test]
    async fn switching_to_self_managed_drops_auto_deployment() {
        let svc = service();
        seed_org(&svc);
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
            ],
        )
        .await;
        ok(
            &svc,
            "UpdateStackSet",
            &[("StackSetName", "org"), ("PermissionModel", "SELF_MANAGED")],
        )
        .await;
        let described = ok(&svc, "DescribeStackSet", &[("StackSetName", "org")]).await;
        assert!(!described.contains("<AutoDeployment>"), "{described}");
        assert!(described.contains("<PermissionModel>SELF_MANAGED</PermissionModel>"));
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "retained"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
                ("AutoDeployment.Enabled", "true"),
                ("AutoDeployment.RetainStacksOnAccountRemoval", "true"),
            ],
        )
        .await;
        // Turning auto-deployment off on its own works.
        ok(
            &svc,
            "UpdateStackSet",
            &[
                ("StackSetName", "retained"),
                ("AutoDeployment.Enabled", "false"),
            ],
        )
        .await;
        let described = ok(&svc, "DescribeStackSet", &[("StackSetName", "retained")]).await;
        assert!(
            described.contains("<Enabled>false</Enabled>"),
            "{described}"
        );
    }

    #[tokio::test]
    async fn a_stack_that_finished_before_a_restart_is_not_counted_as_interrupted() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("OperationId", "op-async"),
            ],
        )
        .await;
        {
            // As persisted while the stack was still provisioning; the stack
            // itself finished before the restart.
            let mut accounts = svc.state.write();
            let set = accounts
                .regional_mut(ADMIN, "us-east-1")
                .stack_sets
                .values_mut()
                .next()
                .unwrap();
            set.instances[0].detailed_status = "RUNNING".to_string();
            let op = set.operations.last_mut().unwrap();
            op.status = "RUNNING".to_string();
            op.results[0].status = "RUNNING".to_string();
            restore_stack_sets(&mut accounts);
        }
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "op-async")],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "SUCCEEDED", "{op}");
        assert_eq!(stored_set(&svc, "app").instances[0].status, "CURRENT");
    }

    #[tokio::test]
    async fn an_operation_that_never_started_before_a_restart_is_not_a_success() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        let snapshot = stored_set(&svc, "app");
        let targets = vec![Target {
            account: ACCT_B.to_string(),
            region: "us-east-1".to_string(),
            ou: None,
            suspended: false,
        }];
        svc.start_instance_operation(
            ADMIN,
            &snapshot,
            &targets,
            "never-ran",
            "CREATE",
            OperationPreferences {
                failure_tolerance_count: Some(5),
                ..OperationPreferences::default()
            },
            None,
            None,
        )
        .unwrap();
        restore_stack_sets(&mut svc.state.write());
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "never-ran")],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "FAILED", "{op}");
        assert!(op.contains(OPERATION_INTERRUPTED), "{op}");
    }

    #[tokio::test]
    async fn operations_interrupted_by_a_restart_are_settled_on_load() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        {
            let mut accounts = svc.state.write();
            let set = accounts
                .regional_mut(ADMIN, "us-east-1")
                .stack_sets
                .values_mut()
                .next()
                .unwrap();
            let mut op = CloudFormationService::new_operation(
                &set.clone(),
                "cut-short",
                "CREATE",
                OperationPreferences::default(),
                None,
                None,
            );
            for (account, status) in [(ACCT_B, "RUNNING"), (ACCT_C, "PENDING")] {
                set.instances.push(StackInstance {
                    account: account.to_string(),
                    region: "us-east-1".to_string(),
                    stack_id: None,
                    status: "OUTDATED".to_string(),
                    detailed_status: "PENDING".to_string(),
                    status_reason: None,
                    parameter_overrides: BTreeMap::new(),
                    organizational_unit_id: None,
                    drift_status: "NOT_CHECKED".to_string(),
                    last_drift_check_timestamp: None,
                    last_operation_id: Some("cut-short".to_string()),
                });
                op.results.push(OperationResult {
                    account: account.to_string(),
                    region: "us-east-1".to_string(),
                    status: status.to_string(),
                    status_reason: None,
                    organizational_unit_id: None,
                    account_gate_status: None,
                    account_gate_reason: None,
                });
            }
            set.operations.push(op);
            restore_stack_sets(&mut accounts);
        }
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "cut-short")],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "FAILED", "{op}");
        // Each instance matches its result: deploying failed, waiting cancelled.
        let set = stored_set(&svc, "app");
        let detailed = |account: &str| {
            set.instances
                .iter()
                .find(|i| i.account == account)
                .unwrap()
                .detailed_status
                .clone()
        };
        assert_eq!(detailed(ACCT_B), "FAILED");
        assert_eq!(detailed(ACCT_C), "CANCELLED");
        // The stack set accepts new operations again.
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_before_deployment_starts_does_not_settle_the_operation() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        let snapshot = stored_set(&svc, "app");
        let targets = vec![Target {
            account: ACCT_B.to_string(),
            region: "us-east-1".to_string(),
            ou: None,
            suspended: false,
        }];
        // Record the operation exactly as CreateStackInstances does, without
        // running it yet, as when the deployment task has not been scheduled.
        svc.start_instance_operation(
            ADMIN,
            &snapshot,
            &targets,
            "queued",
            "CREATE",
            OperationPreferences::default(),
            None,
            None,
        )
        .unwrap();
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "queued")],
        )
        .await;
        assert_eq!(tag(&op, "Status"), "RUNNING", "{op}");
        let e = err(&svc, "DeleteStackSet", &[("StackSetName", "app")]).await;
        assert_eq!(e.code(), "OperationInProgressException");
        // The instance it will create is already listed, pending.
        let describe = [
            ("StackSetName", "app"),
            ("StackInstanceAccount", ACCT_B),
            ("StackInstanceRegion", "us-east-1"),
        ];
        let instance = ok(&svc, "DescribeStackInstance", &describe).await;
        assert_eq!(tag(&instance, "Status"), "OUTDATED", "{instance}");
        assert_eq!(tag(&instance, "DetailedStatus"), "PENDING", "{instance}");
        // Stopped before it started: the pending instance is cancelled.
        ok(
            &svc,
            "StopStackSetOperation",
            &[("StackSetName", "app"), ("OperationId", "queued")],
        )
        .await;
        let instance = ok(&svc, "DescribeStackInstance", &describe).await;
        assert_eq!(tag(&instance, "DetailedStatus"), "CANCELLED", "{instance}");
    }

    #[tokio::test]
    async fn an_untouched_iam_role_is_in_sync() {
        let template = "Resources:\n  R:\n    Type: AWS::IAM::Role\n    Properties:\n      RoleName:\n        Fn::Sub: \"${AWS::StackName}-role\"\n      AssumeRolePolicyDocument:\n        Version: \"2012-10-17\"\n        Statement: []\n";
        let svc = service();
        create_set(&svc, "roles", template).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "roles"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        let xml = ok(&svc, "DetectStackSetDrift", &[("StackSetName", "roles")]).await;
        let op = ok(
            &svc,
            "DescribeStackSetOperation",
            &[
                ("StackSetName", "roles"),
                ("OperationId", &tag(&xml, "OperationId")),
            ],
        )
        .await;
        assert_eq!(tag(&op, "DriftStatus"), "IN_SYNC", "{op}");
    }

    #[tokio::test]
    async fn a_target_that_does_not_deploy_keeps_its_requested_overrides() {
        let svc = service();
        let (workloads, _) = seed_org(&svc);
        svc.deps
            .organizations
            .write()
            .sole_mut()
            .unwrap()
            .close_account(ACCT_C)
            .unwrap();
        ok(&svc, "ActivateOrganizationsAccess", &[]).await;
        ok(
            &svc,
            "CreateStackSet",
            &[
                ("StackSetName", "org"),
                ("TemplateBody", QUEUE_TEMPLATE),
                ("PermissionModel", "SERVICE_MANAGED"),
            ],
        )
        .await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "org"),
                (
                    "DeploymentTargets.OrganizationalUnitIds.member.1",
                    &workloads,
                ),
                ("Regions.member.1", "us-east-1"),
                ("ParameterOverrides.member.1.ParameterKey", "Env"),
                ("ParameterOverrides.member.1.ParameterValue", "prod"),
            ],
        )
        .await;
        let skipped = stored_set(&svc, "org")
            .instances
            .into_iter()
            .find(|i| i.account == ACCT_C)
            .unwrap();
        assert_eq!(skipped.detailed_status, "SKIPPED_SUSPENDED_ACCOUNT");
        assert_eq!(
            skipped.parameter_overrides.get("Env").map(String::as_str),
            Some("prod")
        );
    }

    #[tokio::test]
    async fn drift_detection_reports_model_declared_errors_and_unchecked_sets() {
        let broken = "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName:\n        Fn::ImportValue: missing-export\n";
        let svc = service();
        create_set(&svc, "nostacks", broken).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "nostacks"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        let params = [("StackSetName", "nostacks"), ("OperationId", "drift-1")];
        ok(&svc, "DetectStackSetDrift", &params).await;
        let described = ok(&svc, "DescribeStackSet", &[("StackSetName", "nostacks")]).await;
        assert_eq!(tag(&described, "DriftStatus"), "NOT_CHECKED", "{described}");
        let e = err(&svc, "DetectStackSetDrift", &params).await;
        assert_eq!(e.code(), "InvalidOperationException");
    }

    #[tokio::test]
    async fn a_stack_that_cannot_be_deleted_leaves_its_instance_inoperable() {
        let svc = service();
        create_set(&svc, "app", QUEUE_TEMPLATE).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
            ],
        )
        .await;
        let stack_id = stored_set(&svc, "app").instances[0]
            .stack_id
            .clone()
            .unwrap();
        call_as(
            &svc,
            ACCT_B,
            "UpdateTerminationProtection",
            &[
                ("StackName", &stack_id),
                ("EnableTerminationProtection", "true"),
            ],
        )
        .await
        .unwrap();
        ok(
            &svc,
            "DeleteStackInstances",
            &[
                ("StackSetName", "app"),
                ("Accounts.member.1", ACCT_B),
                ("Regions.member.1", "us-east-1"),
                ("RetainStacks", "false"),
            ],
        )
        .await;
        let instance = stored_set(&svc, "app").instances[0].clone();
        assert_eq!(instance.status, "INOPERABLE");
        assert_eq!(instance.detailed_status, "FAILED");
    }

    struct FailingLambda;

    impl LambdaDelivery for FailingLambda {
        fn invoke_lambda(
            &self,
            _function_arn: &str,
            _payload: &str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>>
        {
            Box::pin(async { Err("custom resource handler failed".to_string()) })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_background_stack_failure_counts_against_the_tolerance() {
        let mut d = deps();
        d.delivery = Arc::new(DeliveryBus::new().with_lambda(Arc::new(FailingLambda)));
        let svc = service_with(d);
        // A custom resource provisions in the background on the server.
        let template = "Resources:\n  C:\n    Type: Custom::Thing\n    Properties:\n      ServiceToken: arn:aws:lambda:us-east-1:111111111111:function:handler\n";
        create_set(&svc, "custom", template).await;
        ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "custom"),
                ("Accounts.member.1", ACCT_B),
                ("Accounts.member.2", ACCT_C),
                ("Regions.member.1", "us-east-1"),
                ("OperationId", "op-custom"),
            ],
        )
        .await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let op = loop {
            let op = ok(
                &svc,
                "DescribeStackSetOperation",
                &[("StackSetName", "custom"), ("OperationId", "op-custom")],
            )
            .await;
            if tag(&op, "Status") != "RUNNING" {
                break op;
            }
            assert!(std::time::Instant::now() < deadline, "{op}");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        };
        assert_eq!(tag(&op, "Status"), "FAILED", "{op}");
        let set = stored_set(&svc, "custom");
        let first = set.instances.iter().find(|i| i.account == ACCT_B).unwrap();
        assert!(first.stack_id.is_some(), "{set:?}");
        let second = set.instances.iter().find(|i| i.account == ACCT_C).unwrap();
        assert_eq!(second.detailed_status, "CANCELLED", "{set:?}");
        assert!(second.stack_id.is_none());
    }

    struct Gate(&'static str);

    impl LambdaDelivery for Gate {
        fn invoke_lambda(
            &self,
            _function_arn: &str,
            _payload: &str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>>
        {
            let body = format!("{{\"Status\":\"{}\"}}", self.0);
            Box::pin(async move { Ok(body.into_bytes()) })
        }
    }

    #[tokio::test]
    async fn an_account_gate_that_fails_blocks_deployment_to_that_account() {
        let mut d = deps();
        d.delivery = Arc::new(DeliveryBus::new().with_lambda(Arc::new(Gate("FAILED"))));
        let svc = service_with(d);
        let gate_template = "Resources:\n  Gate:\n    Type: AWS::Lambda::Function\n    Properties:\n      FunctionName: AWSCloudFormationStackSetAccountGate\n      Runtime: python3.12\n      Handler: index.handler\n      Role: arn:aws:iam::111111111111:role/gate\n      Code:\n        ZipFile: \"def handler(e, c): return {}\"\n";
        call_as(
            &svc,
            ACCT_B,
            "CreateStack",
            &[("StackName", "gate"), ("TemplateBody", gate_template)],
        )
        .await
        .unwrap();
        assert!(svc
            .deps
            .lambda
            .read()
            .get(ACCT_B)
            .is_some_and(|s| s.functions.contains_key(ACCOUNT_GATE_FUNCTION)));

        create_set(&svc, "gated", QUEUE_TEMPLATE).await;
        let xml = ok(
            &svc,
            "CreateStackInstances",
            &[
                ("StackSetName", "gated"),
                ("Accounts.member.1", ACCT_B),
                ("Accounts.member.2", ACCT_C),
                ("Regions.member.1", "us-east-1"),
                ("OperationPreferences.FailureToleranceCount", "1"),
            ],
        )
        .await;
        let results = ok(
            &svc,
            "ListStackSetOperationResults",
            &[
                ("StackSetName", "gated"),
                ("OperationId", &tag(&xml, "OperationId")),
            ],
        )
        .await;
        assert!(
            results.contains("<AccountGateResult><Status>FAILED</Status>"),
            "{results}"
        );
        assert_eq!(queue_count(&svc, ACCT_B), 0);
        // The ungated account deploys.
        assert_eq!(queue_count(&svc, ACCT_C), 1);
    }
}
