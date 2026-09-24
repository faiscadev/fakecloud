use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use fakecloud_core::pagination::paginate_checked;
use http::StatusCode;
use rand::Rng;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    MemberAccount, OrgError, OrganizationState, OrganizationalUnit, OrganizationsRegistry,
    OrganizationsSnapshot, Policy, SharedOrganizationsState, FEATURE_SET_ALL,
    FEATURE_SET_CONSOLIDATED_BILLING, ORGANIZATIONS_SNAPSHOT_SCHEMA_VERSION, POLICY_TYPE_SCP,
};

/// Organizations read actions all start with `Describe` or `List`; every other
/// action is a mutation. The inverse formulation guarantees no mutation is ever
/// missed.
fn is_mutating_action(action: &str) -> bool {
    !(action.starts_with("Describe") || action.starts_with("List"))
}

/// Bounds for the synthetic delay before a `CreateAccount` request
/// flips from `IN_PROGRESS` to `SUCCEEDED`. Real AWS takes minutes; a
/// 1-2s window is enough for SDK callers to observe the IN_PROGRESS
/// phase via at least one poll without making tests slow.
const CREATE_ACCOUNT_MIN_DELAY: Duration = Duration::from_millis(1000);
const CREATE_ACCOUNT_MAX_DELAY: Duration = Duration::from_millis(2000);

/// Single source of truth for supported Organizations actions.
/// Enforcement of attached SCPs ships in Batch 4.
pub static ORGANIZATIONS_ACTIONS: &[&str] = &[
    "CreateOrganization",
    "DescribeOrganization",
    "DeleteOrganization",
    "ListRoots",
    "CreateOrganizationalUnit",
    "UpdateOrganizationalUnit",
    "DeleteOrganizationalUnit",
    "DescribeOrganizationalUnit",
    "ListOrganizationalUnitsForParent",
    "ListAccounts",
    "ListAccountsForParent",
    "DescribeAccount",
    "MoveAccount",
    "CreatePolicy",
    "UpdatePolicy",
    "DeletePolicy",
    "DescribePolicy",
    "ListPolicies",
    "AttachPolicy",
    "DetachPolicy",
    "ListPoliciesForTarget",
    "ListTargetsForPolicy",
    "CreateAccount",
    "CreateGovCloudAccount",
    "DescribeCreateAccountStatus",
    "ListCreateAccountStatus",
    "CloseAccount",
    "RemoveAccountFromOrganization",
    "InviteAccountToOrganization",
    "AcceptHandshake",
    "DeclineHandshake",
    "CancelHandshake",
    "DescribeHandshake",
    "ListHandshakesForAccount",
    "ListHandshakesForOrganization",
    "EnableAWSServiceAccess",
    "DisableAWSServiceAccess",
    "ListAWSServiceAccessForOrganization",
    "RegisterDelegatedAdministrator",
    "DeregisterDelegatedAdministrator",
    "ListDelegatedAdministrators",
    "ListDelegatedServicesForAccount",
    "EnableAllFeatures",
    "EnablePolicyType",
    "DisablePolicyType",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
    "ListParents",
    "ListChildren",
    "DescribeEffectivePolicy",
    "PutResourcePolicy",
    "DeleteResourcePolicy",
    "DescribeResourcePolicy",
    "LeaveOrganization",
    "ListAccountsWithInvalidEffectivePolicy",
    "ListEffectivePolicyValidationErrors",
    "InviteOrganizationToTransferResponsibility",
    "DescribeResponsibilityTransfer",
    "UpdateResponsibilityTransfer",
    "TerminateResponsibilityTransfer",
    "ListInboundResponsibilityTransfers",
    "ListOutboundResponsibilityTransfers",
];

/// Called after a mutation that may have changed which accounts the
/// organization contains, or where they sit in the OU tree. Observers
/// re-read the organization themselves and reconcile against it, so the hook
/// carries no payload and is safe to fire more often than strictly needed.
pub type OrgChangeHook = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// The set of observers notified of organization changes.
///
/// CloudFormation registers one to run StackSets auto-deployment: an account
/// joining (or leaving) an OU a service-managed stack set targets has to gain
/// (or lose) that stack set's instances. The registry is a shared handle so
/// the server can build Organizations first and install the CloudFormation
/// observer once that service exists.
#[derive(Clone, Default)]
pub struct OrgChangeHooks {
    hooks: Arc<parking_lot::RwLock<Vec<OrgChangeHook>>>,
    /// Fingerprint of the organization the observers were last told about, so
    /// a mutation that changed nothing they care about costs nothing.
    seen: Arc<parking_lot::Mutex<Option<u64>>>,
}

/// Everything an observer reacts to: which accounts exist, where they sit and
/// whether they are active, and the shape of the OU tree they sit in.
fn membership_fingerprint(org: &OrganizationState) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    org.org_id.hash(&mut hasher);
    org.management_account_id.hash(&mut hasher);
    for account in org.accounts.values() {
        account.id.hash(&mut hasher);
        account.parent_id.hash(&mut hasher);
        account.status.hash(&mut hasher);
    }
    for ou in org.ous.values() {
        ou.id.hash(&mut hasher);
        ou.parent_id.hash(&mut hasher);
    }
    hasher.finish()
}

/// Fingerprint of membership across EVERY organization. An organization
/// appearing or disappearing has to register as a change too, so this
/// hashes the whole registry rather than tracking one value per org.
fn registry_membership_fingerprint(registry: &OrganizationsRegistry) -> Option<u64> {
    if registry.is_empty() {
        return None;
    }
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    // `iter()` is ordered by organization id, so the digest is stable.
    for org in registry.iter() {
        hasher.write_u64(membership_fingerprint(org));
    }
    Some(hasher.finish())
}

impl OrgChangeHooks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, hook: OrgChangeHook) {
        self.hooks.write().push(hook);
    }

    /// Fire only when the organization's membership actually differs from
    /// what the observers were last told. Most mutations (a tag, a policy, a
    /// handshake that is still open) leave it untouched, and a stack set
    /// reconciliation is far too heavy to run on each of those.
    pub async fn fire_if_membership_changed(&self, state: &SharedOrganizationsState) {
        // Read the organization under the same lock that records it. Reading
        // first would let two concurrent mutations record a fingerprint for a
        // state the organization has already left, after which the change
        // that takes it back there looks like no change at all and is never
        // announced.
        let (previous, claimed) = {
            let mut seen = self.seen.lock();
            let fingerprint = registry_membership_fingerprint(&state.read());
            if *seen == fingerprint {
                return;
            }
            let previous = *seen;
            // Claim it up front, so a mutation racing this one does not
            // announce the same state twice.
            *seen = fingerprint;
            (previous, fingerprint)
        };
        let claim = FingerprintClaim {
            seen: self.seen.clone(),
            previous,
            claimed,
            announced: false,
        };
        self.fire().await;
        claim.announced();
    }

    /// Run every registered observer to completion. Awaited by the mutation
    /// that triggered it, so a caller that has just moved an account sees the
    /// resulting deployment already done when the call returns. A registry
    /// with no observers costs one lock.
    pub async fn fire(&self) {
        let hooks: Vec<OrgChangeHook> = self.hooks.read().clone();
        for hook in hooks {
            hook().await;
        }
    }
}

/// Holds a fingerprint claimed for announcement. If the caller is cancelled
/// before the observers have run, it puts back what was there so the change
/// is announced again rather than being remembered as already handled.
struct FingerprintClaim {
    seen: Arc<parking_lot::Mutex<Option<u64>>>,
    previous: Option<u64>,
    claimed: Option<u64>,
    announced: bool,
}

impl FingerprintClaim {
    fn announced(mut self) {
        self.announced = true;
    }
}

impl Drop for FingerprintClaim {
    fn drop(&mut self) {
        if self.announced {
            return;
        }
        let mut seen = self.seen.lock();
        // Only roll back what is still ours: a later change has its own
        // claim and must keep it.
        if *seen == self.claimed {
            *seen = self.previous;
        }
    }
}

impl std::fmt::Debug for OrgChangeHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrgChangeHooks")
            .field("observers", &self.hooks.read().len())
            .finish()
    }
}

pub struct OrganizationsService {
    state: SharedOrganizationsState,
    pub(crate) snapshot_store: Option<Arc<dyn SnapshotStore>>,
    pub(crate) snapshot_lock: Arc<AsyncMutex<()>>,
    pub(crate) change_hooks: OrgChangeHooks,
}

mod accounts;
mod delegated;
mod handshakes;
mod org;
mod ous;
mod policies;
mod policy_types;
mod responsibility;
mod roots;
mod service_access;
mod tags;

impl OrganizationsService {
    pub fn new(state: SharedOrganizationsState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            change_hooks: OrgChangeHooks::new(),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Share a change-hook registry with the server, which installs observers
    /// into it after the services that react to organization changes exist.
    pub fn with_change_hooks(mut self, hooks: OrgChangeHooks) -> Self {
        self.change_hooks = hooks;
        self
    }

    pub fn change_hooks(&self) -> OrgChangeHooks {
        self.change_hooks.clone()
    }

    pub fn shared() -> (Arc<Self>, SharedOrganizationsState) {
        let state: SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(OrganizationsRegistry::default()));
        (Arc::new(Self::new(state.clone())), state)
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    pub(crate) async fn save_snapshot(&self) {
        save_organizations_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Organizations state when invoked,
    /// or `None` in memory mode. The CloudFormation provisioner mutates `state`
    /// directly and uses this to write a CFN-provisioned resource through to
    /// disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_organizations_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Re-arm the completion tick for any `CreateAccount` request restored as
    /// `IN_PROGRESS`, so it still transitions to `SUCCEEDED` after a restart.
    /// Called by the server after loading a snapshot.
    pub fn rearm_in_progress_account_creations(&self) {
        let pending: Vec<String> = {
            let guard = self.state.read();
            // Request ids are globally unique (`car-` + 20 random chars), so
            // the completion tick can find its own request by scanning every
            // organization — no need to thread the owning org id through.
            guard
                .iter()
                .flat_map(|org| org.create_account_requests.iter())
                .filter(|(_, s)| s.state == "IN_PROGRESS")
                .map(|(id, _)| id.clone())
                .collect()
        };
        for request_id in pending {
            self.spawn_create_account_completion(request_id);
        }
    }

    /// Read-side helper: resolve the organization the caller belongs to.
    /// A caller in no organization gets the same
    /// `AWSOrganizationsNotInUseException` as a caller in a process with
    /// no organizations at all, so another organization's existence is
    /// never observable from outside it.
    fn require_member<'a>(
        &self,
        guard: &'a parking_lot::RwLockReadGuard<'_, OrganizationsRegistry>,
        account_id: &str,
    ) -> Result<&'a OrganizationState, AwsServiceError> {
        guard
            .org_of_account(account_id)
            .ok_or_else(organizations_not_in_use)
    }

    /// Write-side helper for mutating ops: resolve the caller's own
    /// organization and enforce that the caller is its management
    /// account. Returns the organization itself, so a handler never has
    /// to re-resolve it out of the registry.
    fn management_org_mut<'a>(
        &self,
        guard: &'a mut parking_lot::RwLockWriteGuard<'_, OrganizationsRegistry>,
        account_id: &str,
    ) -> Result<&'a mut OrganizationState, AwsServiceError> {
        let org = guard
            .org_of_account_mut(account_id)
            .ok_or_else(organizations_not_in_use)?;
        if !org.is_management(account_id) {
            return Err(not_management());
        }
        Ok(org)
    }

    /// The caller's organization, for the read operations AWS opens to
    /// the management account OR to any member registered as a
    /// delegated administrator (for any service principal).
    ///
    /// Delegated administration exists so a member account can run a
    /// service's org-wide integration on the management account's
    /// behalf, which means reading the organization it administers:
    /// `ListHandshakesForOrganization`, `ListAWSServiceAccessForOrganization`,
    /// `ListDelegatedAdministrators` and `ListDelegatedServicesForAccount`
    /// are all documented as callable by a delegated administrator.
    /// Mutating operations stay management-only via
    /// [`Self::management_org_mut`], which is why there is no read-side
    /// management-only gate left: every management-only op mutates.
    fn management_or_delegated_org<'a>(
        &self,
        guard: &'a parking_lot::RwLockReadGuard<'_, OrganizationsRegistry>,
        account_id: &str,
    ) -> Result<&'a OrganizationState, AwsServiceError> {
        let org = self.require_member(guard, account_id)?;
        if !org.is_management(account_id) && !org.is_delegated_administrator(account_id) {
            return Err(not_management());
        }
        Ok(org)
    }
}

/// AWS's handshake-time answer for "that account already belongs to an
/// organization", carrying the modeled `Reason` discriminator.
fn already_in_an_organization(message: String) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::BAD_REQUEST,
        "HandshakeConstraintViolationException",
        message,
        vec![(
            "Reason".to_string(),
            "ALREADY_IN_AN_ORGANIZATION".to_string(),
        )],
    )
}

/// AWS's error for a management-only operation attempted by a member.
fn not_management() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::FORBIDDEN,
        "AccessDeniedException",
        "This operation can be called only from the organization's management account.",
    )
}

fn parse_tags(value: Option<&Value>) -> Vec<(String, String)> {
    let arr = match value.and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };
    arr.iter()
        .filter_map(|v| {
            let k = v.get("Key")?.as_str()?.to_string();
            let value = v.get("Value")?.as_str()?.to_string();
            Some((k, value))
        })
        .collect()
}

/// Walk from `target_id` up to root (inclusive) via OU/account parents.
/// Used by `DescribeEffectivePolicy` to union policy statements across
/// every level. Keeps the input id at the front so direct attachments
/// take precedence in iteration order.
fn ancestors_for(org: &OrganizationState, target_id: &str) -> Vec<String> {
    let mut chain = vec![target_id.to_string()];
    let mut cursor = target_id.to_string();
    while let Some((parent, _)) = org.parent_of(&cursor) {
        if parent.is_empty() {
            break;
        }
        chain.push(parent.clone());
        if parent.starts_with("r-") {
            break;
        }
        cursor = parent;
    }
    chain
}

#[async_trait]
impl AwsService for OrganizationsService {
    fn service_name(&self) -> &str {
        "organizations"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(&req.action);
        // Expire past-due handshakes before answering anything. AWS
        // expires a handshake 15 days after it is extended whether or not
        // anyone is looking, so the sweep cannot hang off the mutating
        // paths alone: a `DescribeHandshake` has to report `EXPIRED` too,
        // and an `AcceptHandshake` must be refused rather than reviving
        // an offer that lapsed. The read-locked check keeps the write
        // lock for the requests that actually have something to expire.
        let now = Utc::now();
        let expired = if self.state.read().has_stale_handshakes(now) {
            self.state.write().expire_stale_handshakes(now)
        } else {
            0
        };
        let result = match req.action.as_str() {
            "CreateOrganization" => self.create_organization(&req),
            "DescribeOrganization" => self.describe_organization(&req),
            "DeleteOrganization" => self.delete_organization(&req),
            "ListRoots" => self.list_roots(&req),
            "CreateOrganizationalUnit" => self.create_organizational_unit(&req),
            "UpdateOrganizationalUnit" => self.update_organizational_unit(&req),
            "DeleteOrganizationalUnit" => self.delete_organizational_unit(&req),
            "DescribeOrganizationalUnit" => self.describe_organizational_unit(&req),
            "ListOrganizationalUnitsForParent" => self.list_organizational_units_for_parent(&req),
            "ListAccounts" => self.list_accounts(&req),
            "ListAccountsForParent" => self.list_accounts_for_parent(&req),
            "DescribeAccount" => self.describe_account(&req),
            "MoveAccount" => self.move_account(&req),
            "CreatePolicy" => self.create_policy(&req),
            "UpdatePolicy" => self.update_policy(&req),
            "DeletePolicy" => self.delete_policy(&req),
            "DescribePolicy" => self.describe_policy(&req),
            "ListPolicies" => self.list_policies(&req),
            "AttachPolicy" => self.attach_policy(&req),
            "DetachPolicy" => self.detach_policy(&req),
            "ListPoliciesForTarget" => self.list_policies_for_target(&req),
            "ListTargetsForPolicy" => self.list_targets_for_policy(&req),
            "CreateAccount" => self.create_account(&req),
            "CreateGovCloudAccount" => self.create_gov_cloud_account(&req),
            "DescribeCreateAccountStatus" => self.describe_create_account_status(&req),
            "ListCreateAccountStatus" => self.list_create_account_status(&req),
            "CloseAccount" => self.close_account(&req),
            "RemoveAccountFromOrganization" => self.remove_account_from_organization(&req),
            "InviteAccountToOrganization" => self.invite_account_to_organization(&req),
            "AcceptHandshake" => self.accept_handshake(&req),
            "DeclineHandshake" => self.decline_handshake(&req),
            "CancelHandshake" => self.cancel_handshake(&req),
            "DescribeHandshake" => self.describe_handshake(&req),
            "ListHandshakesForAccount" => self.list_handshakes_for_account(&req),
            "ListHandshakesForOrganization" => self.list_handshakes_for_organization(&req),
            "EnableAWSServiceAccess" => self.enable_aws_service_access(&req),
            "DisableAWSServiceAccess" => self.disable_aws_service_access(&req),
            "ListAWSServiceAccessForOrganization" => {
                self.list_aws_service_access_for_organization(&req)
            }
            "RegisterDelegatedAdministrator" => self.register_delegated_administrator(&req),
            "DeregisterDelegatedAdministrator" => self.deregister_delegated_administrator(&req),
            "ListDelegatedAdministrators" => self.list_delegated_administrators(&req),
            "ListDelegatedServicesForAccount" => self.list_delegated_services_for_account(&req),
            "EnableAllFeatures" => self.enable_all_features(&req),
            "EnablePolicyType" => self.enable_policy_type(&req),
            "DisablePolicyType" => self.disable_policy_type(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "ListParents" => self.list_parents(&req),
            "ListChildren" => self.list_children(&req),
            "DescribeEffectivePolicy" => self.describe_effective_policy(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            "DescribeResourcePolicy" => self.describe_resource_policy(&req),
            "LeaveOrganization" => self.leave_organization(&req),
            "ListAccountsWithInvalidEffectivePolicy" => {
                self.list_accounts_with_invalid_effective_policy(&req)
            }
            "ListEffectivePolicyValidationErrors" => {
                self.list_effective_policy_validation_errors(&req)
            }
            "InviteOrganizationToTransferResponsibility" => {
                self.invite_organization_to_transfer_responsibility(&req)
            }
            "DescribeResponsibilityTransfer" => self.describe_responsibility_transfer(&req),
            "UpdateResponsibilityTransfer" => self.update_responsibility_transfer(&req),
            "TerminateResponsibilityTransfer" => self.terminate_responsibility_transfer(&req),
            "ListInboundResponsibilityTransfers" => {
                self.list_inbound_responsibility_transfers(&req)
            }
            "ListOutboundResponsibilityTransfers" => {
                self.list_outbound_responsibility_transfers(&req)
            }
            _ => Err(AwsServiceError::action_not_implemented(
                "organizations",
                &req.action,
            )),
        };
        // A sweep is a mutation like any other, even when it happened on
        // the way into a read: without this the expiry is lost on
        // restart and the handshake comes back OPEN.
        if expired > 0 {
            self.save_snapshot().await;
        }
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
            // Any successful mutation can have moved an account between OUs,
            // added one to the organization or taken one out. Observers
            // reconcile against the organization rather than against a diff,
            // so this cannot miss a placement change (StackSets
            // auto-deployment depends on seeing all of them), and the ones
            // that changed nothing they care about are filtered out here.
            self.change_hooks
                .fire_if_membership_changed(&self.state)
                .await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        ORGANIZATIONS_ACTIONS
    }
}

/// Persist the current Organizations state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `OrganizationsService::save_snapshot`, the
/// CreateAccount completion tick, and the CloudFormation provisioner persist
/// hook so all route through the same serialize-and-write path.
pub async fn save_organizations_snapshot(
    state: &SharedOrganizationsState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = OrganizationsSnapshot {
        schema_version: ORGANIZATIONS_SNAPSHOT_SCHEMA_VERSION,
        // v1's single-organization field is read-only now; v2 always
        // writes the whole registry.
        organization: None,
        organizations: state.read().clone(),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write organizations snapshot"),
        Err(err) => tracing::error!(%err, "organizations snapshot task panicked"),
    }
}

fn organizations_not_in_use() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "AWSOrganizationsNotInUseException",
        "Your account is not a member of an organization.",
    )
}

fn policy_summary(policy: &Policy) -> Value {
    json!({
        "Id": policy.id,
        "Arn": policy.arn,
        "Name": policy.name,
        "Description": policy.description,
        "Type": policy.policy_type,
        "AwsManaged": policy.aws_managed,
    })
}

fn policy_with_content(policy: &Policy) -> Value {
    json!({
        "PolicySummary": policy_summary(policy),
        "Content": policy.content,
    })
}

fn target_arn(org: &OrganizationState, target_id: &str, target_type: &str) -> String {
    match target_type {
        "ROOT" => org.root_arn.clone(),
        "ORGANIZATIONAL_UNIT" => org
            .ous
            .get(target_id)
            .map(|ou| ou.arn.clone())
            .unwrap_or_default(),
        "ACCOUNT" => org
            .accounts
            .get(target_id)
            .map(|a| a.arn.clone())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn ou_payload(ou: &OrganizationalUnit) -> Value {
    json!({
        "Id": ou.id,
        "Arn": ou.arn,
        "Name": ou.name,
    })
}

fn account_payload(account: &MemberAccount) -> Value {
    json!({
        "Id": account.id,
        "Arn": account.arn,
        "Email": account.email,
        "Name": account.name,
        "Status": account.status,
        "JoinedMethod": account.joined_method,
        "JoinedTimestamp": account.joined_timestamp.timestamp() as f64,
    })
}

fn required_str<'a>(body: &'a Value, key: &str) -> Result<&'a str, AwsServiceError> {
    body.get(key).and_then(|v| v.as_str()).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidInputException",
            format!("Missing required parameter: {key}"),
        )
    })
}

fn is_known_policy_type(t: &str) -> bool {
    matches!(
        t,
        POLICY_TYPE_SCP
            | "TAG_POLICY"
            | "BACKUP_POLICY"
            | "AISERVICES_OPT_OUT_POLICY"
            | "RESOURCE_CONTROL_POLICY"
    )
}

/// Every value of the Smithy `PolicyType` enum. The `List*` filter ops accept
/// any of these — a type fakecloud doesn't manage simply yields an empty
/// result set, mirroring AWS, which only rejects out-of-enum values with
/// `InvalidInputException`.
fn is_valid_policy_type(t: &str) -> bool {
    matches!(
        t,
        "SERVICE_CONTROL_POLICY"
            | "RESOURCE_CONTROL_POLICY"
            | "TAG_POLICY"
            | "BACKUP_POLICY"
            | "AISERVICES_OPT_OUT_POLICY"
            | "CHATBOT_POLICY"
            | "DECLARATIVE_POLICY_EC2"
            | "SECURITYHUB_POLICY"
            | "INSPECTOR_POLICY"
            | "UPGRADE_ROLLOUT_POLICY"
            | "BEDROCK_POLICY"
            | "S3_POLICY"
            | "NETWORK_SECURITY_DIRECTOR_POLICY"
    )
}

fn invalid_policy_filter(filter: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidInputException",
        format!("You specified an invalid value for the Filter parameter: {filter}"),
    )
}

pub(super) fn invalid_input(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidInputException", msg)
}

fn org_error_to_aws(err: OrgError) -> AwsServiceError {
    match err {
        OrgError::ParentNotFound(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ParentNotFoundException",
            format!("The parent with id {id} was not found."),
        ),
        OrgError::DuplicateOrganizationalUnit(name) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "DuplicateOrganizationalUnitException",
            format!("An organizational unit named {name} already exists under this parent."),
        ),
        OrgError::OrganizationalUnitNotFound(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "OrganizationalUnitNotFoundException",
            format!("The organizational unit with id {id} was not found."),
        ),
        OrgError::OrganizationalUnitNotEmpty(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "OrganizationalUnitNotEmptyException",
            format!("The organizational unit {id} still contains accounts or child OUs."),
        ),
        OrgError::AccountNotFound(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AccountNotFoundException",
            format!("The account with id {id} was not found."),
        ),
        OrgError::SourceParentNotFound(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "SourceParentNotFoundException",
            format!("The source parent {id} does not contain this account."),
        ),
        OrgError::DestinationParentNotFound(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "DestinationParentNotFoundException",
            format!("The destination parent {id} does not exist."),
        ),
        OrgError::PolicyNotFound(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "PolicyNotFoundException",
            format!("The policy with id {id} was not found."),
        ),
        OrgError::DuplicatePolicy(name) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "DuplicatePolicyException",
            format!("A policy named {name} already exists for this policy type."),
        ),
        OrgError::MalformedPolicyDocument => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MalformedPolicyDocumentException",
            "The policy document is not valid JSON.",
        ),
        OrgError::PolicyTypeNotSupported(t) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "PolicyTypeNotSupportedException",
            format!("fakecloud only supports SERVICE_CONTROL_POLICY; got {t}."),
        ),
        OrgError::PolicyChangesNotAllowed(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "PolicyChangesNotAllowedException",
            format!("Policy {id} is AWS-managed and cannot be modified or deleted."),
        ),
        OrgError::PolicyInUse(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "PolicyInUseException",
            format!("Policy {id} is attached to one or more targets; detach before deleting."),
        ),
        OrgError::PolicyNotAttached(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "PolicyNotAttachedException",
            format!("Policy {id} is not attached to this target."),
        ),
        OrgError::TargetNotFound(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "TargetNotFoundException",
            format!("The target with id {id} was not found."),
        ),
        OrgError::AccountChangesNotAllowed(id) => AwsServiceError::aws_error(
            StatusCode::FORBIDDEN,
            "ConstraintViolationException",
            format!("Account {id} cannot be removed or closed (management account)."),
        ),
        OrgError::CreateAccountStatusNotFound(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "CreateAccountStatusNotFoundException",
            format!("Create account status with id {id} was not found."),
        ),
        OrgError::HandshakeNotFound(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "HandshakeNotFoundException",
            format!("The handshake with id {id} was not found."),
        ),
        OrgError::HandshakeAlreadyResolved(state) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidHandshakeTransitionException",
            format!("Handshake is already in terminal state {state}."),
        ),
        OrgError::InvalidHandshakeState(state) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidHandshakeTransitionException",
            format!("State {state} is not a valid terminal handshake state."),
        ),
        OrgError::InvalidHandshakeParty(account) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AccessDeniedException",
            format!("Account {account} is not party to this handshake's transition."),
        ),
        OrgError::DuplicateHandshakeForAccount(account) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "DuplicateHandshakeException",
            format!("An OPEN handshake already exists for account {account}."),
        ),
        // AWS reports both of these on InviteAccountToOrganization and
        // AcceptHandshake as HandshakeConstraintViolationException with
        // Reason=ALREADY_IN_AN_ORGANIZATION; those operations do not model
        // AccountAlreadyRegisteredException at all, so a typed SDK catching
        // the modeled exception would miss it.
        OrgError::AccountAlreadyMember(account) => already_in_an_organization(format!(
            "Account {account} is already a member of this organization."
        )),
        OrgError::AccountInAnotherOrganization(account) => already_in_an_organization(format!(
            "Account {account} is already a member of an organization."
        )),
        OrgError::AWSServiceAccessNotEnabled(svc) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AWSOrganizationsNotInUseException",
            format!("AWS service access for {svc} is not enabled."),
        ),
        OrgError::DelegatedAdministratorAlreadyRegistered(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AccountAlreadyRegisteredException",
            format!("Account {id} is already registered as a delegated administrator."),
        ),
        OrgError::DelegatedAdministratorNotRegistered(id) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AccountNotRegisteredException",
            format!("Account {id} is not registered as a delegated administrator."),
        ),
    }
}

/// Parsed `Filter` block from a `ListHandshakes*` request. Keeps each
/// AWS-supported filter field as an `Option`; `None` means "don't
/// constrain on this dimension".
#[derive(Default, Debug, Clone)]
struct HandshakeFilter {
    action_type: Option<String>,
    parent_handshake_id: Option<String>,
}

/// Parse `Filter` (HandshakeFilter shape) from a `ListHandshakes*`
/// request body. Unknown keys are ignored to match AWS's forward-compat
/// behavior. Returns an empty filter if the field is absent.
fn parse_handshake_filter(body: &Value) -> Result<HandshakeFilter, AwsServiceError> {
    let Some(filter_val) = body.get("Filter") else {
        return Ok(HandshakeFilter::default());
    };
    if filter_val.is_null() {
        return Ok(HandshakeFilter::default());
    }
    let filter_obj = filter_val.as_object().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidInputException",
            "Filter must be an object.",
        )
    })?;
    let action_type = filter_obj
        .get("ActionType")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    if let Some(action) = &action_type {
        // Reject unknown action types up front so callers see the same
        // error AWS returns for typos, instead of silently getting an
        // empty page.
        const ALLOWED: &[&str] = &[
            "INVITE",
            "ENABLE_ALL_FEATURES",
            "APPROVE_ALL_FEATURES",
            "ADD_ORGANIZATIONS_SERVICE_LINKED_ROLE",
            // A real `ActionType`, and now reachable: a source management
            // account sees its own outbound transfer handshakes, so it can
            // filter for them.
            "TRANSFER_RESPONSIBILITY",
        ];
        if !ALLOWED.contains(&action.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInputException",
                format!("Filter.ActionType {action} is not a recognized handshake action."),
            ));
        }
    }
    let parent_handshake_id = filter_obj
        .get("ParentHandshakeId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    Ok(HandshakeFilter {
        action_type,
        parent_handshake_id,
    })
}

/// Match a stored handshake against the parsed filter. We don't track
/// parent/child handshakes today, so any `ParentHandshakeId` filter
/// excludes every handshake — which matches AWS's behavior for
/// stand-alone INVITE handshakes that have no parent link.
fn handshake_matches_filter(h: &crate::state::Handshake, filter: &HandshakeFilter) -> bool {
    if let Some(ref action) = filter.action_type {
        if &h.action != action {
            return false;
        }
    }
    if filter.parent_handshake_id.is_some() {
        // No handshake we mint has a parent; the filter therefore
        // matches nothing rather than everything.
        return false;
    }
    true
}

/// Parse `MaxResults` (1..=20, default 20) and `NextToken` (string
/// matching what `paginate` mints) from any AWS Organizations
/// `List*` request body. Shared by handshake, AWS-service-access,
/// delegated-administrator and delegated-service listings.
fn parse_list_pagination(body: &Value) -> Result<(usize, Option<String>), AwsServiceError> {
    let max_results = match body.get("MaxResults") {
        None | Some(Value::Null) => 20usize,
        Some(v) => {
            let n = v.as_u64().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInputException",
                    "MaxResults must be a positive integer between 1 and 20.",
                )
            })?;
            if !(1..=20).contains(&n) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInputException",
                    "MaxResults must be between 1 and 20.",
                ));
            }
            n as usize
        }
    };
    let next_token = match body.get("NextToken") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let s = v.as_str().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInputException",
                    "NextToken must be a string.",
                )
            })?;
            if s.parse::<usize>().is_err() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInputException",
                    "NextToken is not a valid pagination token.",
                ));
            }
            Some(s.to_string())
        }
    };
    Ok((max_results, next_token))
}

fn handshake_payload(org: &OrganizationState, h: &crate::state::Handshake) -> Value {
    // Real AWS Organizations encodes the inviter as the org itself
    // (`Type: ORGANIZATION`, `Id` = the org id) and the invitee as the
    // member account (`Type: ACCOUNT`, `Id` = account id) or its email
    // (`Type: EMAIL`, `Id` = email address). The source account id is
    // also exposed as a separate ACCOUNT party so callers can correlate.
    let parties = json!([
        {"Id": h.organization_id, "Type": "ORGANIZATION"},
        {"Id": h.source_account_id, "Type": "ACCOUNT"},
        {
            // Pick the id by the party's own Type. Preferring
            // `target_email` whenever it was recorded rendered an address
            // under `Type: ACCOUNT` for any handshake that stored both.
            "Id": if h.target_kind == "EMAIL" {
                h.target_email.clone().unwrap_or_else(|| h.target_account_id.clone())
            } else {
                h.target_account_id.clone()
            },
            "Type": h.target_kind,
        },
    ]);
    // Same rule as the parties above: the value has to match the type it
    // is labelled with. `HandshakeResourceType` models EMAIL separately,
    // so an email-target invite reports the address as EMAIL rather than
    // as an ACCOUNT id.
    let target_resource = if h.target_kind == "EMAIL" {
        json!({
            "Type": "EMAIL",
            "Value": h.target_email.clone().unwrap_or_else(|| h.target_account_id.clone()),
        })
    } else {
        json!({"Type": "ACCOUNT", "Value": h.target_account_id})
    };
    let mut resources = vec![
        json!({"Type": "ORGANIZATION", "Value": h.organization_id}),
        target_resource,
    ];
    // A TRANSFER_RESPONSIBILITY handshake carries the transfer it is
    // offering, and AWS models exactly that as a nested
    // `RESPONSIBILITY_TRANSFER` resource -- which is how an SDK reading
    // only the handshake learns what is being handed over and by whom.
    // `HandshakeResourceType` defines TRANSFER_TYPE,
    // TRANSFER_START_TIMESTAMP and MANAGEMENT_ACCOUNT for no other
    // purpose.
    if let Some(transfer) = h
        .responsibility_transfer_id
        .as_deref()
        .and_then(|id| org.responsibility_transfers.get(id))
        // A handshake written before the link existed deserializes with
        // no transfer id, and nothing backfills it. Fall back to the
        // transfer's own `ActiveHandshakeId`, which covers every restored
        // handshake still OPEN. A restored handshake that had already
        // resolved is beyond recovery -- resolution is what clears that
        // field, and nothing else ties the two together -- so this is as
        // far back as the stored data reaches.
        .or_else(|| {
            org.responsibility_transfers
                .values()
                .find(|t| t.active_handshake_id.as_deref() == Some(h.id.as_str()))
        })
    {
        resources.push(json!({
            "Type": "RESPONSIBILITY_TRANSFER",
            "Value": transfer.id,
            "Resources": [
                {"Type": "TRANSFER_TYPE", "Value": transfer.transfer_type},
                {
                    "Type": "TRANSFER_START_TIMESTAMP",
                    // A nested resource's Value is a string in the Smithy
                    // model, so the timestamp goes out ISO-8601 rather
                    // than as the epoch number the top-level timestamp
                    // members use.
                    "Value": transfer.start_timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                },
                {
                    "Type": "MANAGEMENT_ACCOUNT",
                    "Value": transfer.source_management_account_id,
                },
                {
                    "Type": "MANAGEMENT_EMAIL",
                    "Value": transfer.source_management_account_email,
                },
            ],
        }));
    }
    let resources = Value::Array(resources);
    let mut obj = json!({
        "Id": h.id,
        "Arn": h.arn,
        "Action": h.action,
        "State": h.state,
        "RequestedTimestamp": h.requested_timestamp.timestamp() as f64,
        "ExpirationTimestamp": h.expiration_timestamp.timestamp() as f64,
        "Parties": parties,
        "Resources": resources,
    });
    if let Some(notes) = &h.notes {
        obj["Notes"] = json!(notes);
    }
    obj
}

fn create_account_status_payload(status: &crate::state::CreateAccountStatus) -> Value {
    let mut obj = json!({
        "Id": status.id,
        "AccountName": status.account_name,
        "State": status.state,
        "RequestedTimestamp": status.requested_timestamp.timestamp() as f64,
    });
    if let Some(account_id) = &status.account_id {
        obj["AccountId"] = json!(account_id);
    }
    if let Some(ts) = status.completed_timestamp {
        obj["CompletedTimestamp"] = json!(ts.timestamp() as f64);
    }
    if let Some(reason) = &status.failure_reason {
        obj["FailureReason"] = json!(reason);
    }
    if let Some(gov_id) = &status.gov_cloud_account_id {
        obj["GovCloudAccountId"] = json!(gov_id);
    }
    obj
}

fn organization_payload(org: &OrganizationState) -> Value {
    json!({
        "Id": org.org_id,
        "Arn": org.org_arn,
        "FeatureSet": org.feature_set,
        "MasterAccountArn": org.management_account_arn,
        "MasterAccountId": org.management_account_id,
        "MasterAccountEmail": org.management_account_email,
        "AvailablePolicyTypes": [
            {"Type": "SERVICE_CONTROL_POLICY", "Status": "ENABLED"}
        ],
    })
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
mod pagination_reject_test {
    #[test]
    fn paginate_checked_rejects_invalid_token() {
        use fakecloud_core::pagination::paginate_checked;
        let items: Vec<i32> = (0..5).collect();
        assert!(paginate_checked(&items, Some("bad"), 3).is_err());
        assert!(paginate_checked(&items, Some("2"), 3).is_ok());
    }
}
