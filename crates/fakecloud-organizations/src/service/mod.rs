use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use fakecloud_core::pagination::paginate;
use http::StatusCode;
use rand::Rng;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    MemberAccount, OrgError, OrganizationState, OrganizationalUnit, Policy,
    SharedOrganizationsState, FEATURE_SET_ALL, FEATURE_SET_CONSOLIDATED_BILLING, POLICY_TYPE_SCP,
};

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

pub struct OrganizationsService {
    state: SharedOrganizationsState,
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
        Self { state }
    }

    pub fn shared() -> (Arc<Self>, SharedOrganizationsState) {
        let state: SharedOrganizationsState = Arc::new(parking_lot::RwLock::new(None));
        (Arc::new(Self::new(state.clone())), state)
    }

    /// Read-side helper: enforce that an org exists and the caller is a
    /// member. Returns the borrowed org on success.
    fn require_member<'a>(
        &self,
        guard: &'a parking_lot::RwLockReadGuard<'_, Option<OrganizationState>>,
        account_id: &str,
    ) -> Result<&'a OrganizationState, AwsServiceError> {
        let org = guard.as_ref().ok_or_else(organizations_not_in_use)?;
        if !org.accounts.contains_key(account_id) {
            return Err(organizations_not_in_use());
        }
        Ok(org)
    }

    /// Write-side helper for mutating ops: caller must be the
    /// management account of an existing organization. Returns the
    /// management-only error rather than an Option, so the caller can
    /// unwrap the guard safely right after.
    fn require_member_management(
        &self,
        guard: &parking_lot::RwLockWriteGuard<'_, Option<OrganizationState>>,
        account_id: &str,
    ) -> Result<(), AwsServiceError> {
        let org = guard.as_ref().ok_or_else(organizations_not_in_use)?;
        if !org.accounts.contains_key(account_id) {
            return Err(organizations_not_in_use());
        }
        if !org.is_management(account_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "AccessDeniedException",
                "This operation can be called only from the organization's management account.",
            ));
        }
        Ok(())
    }
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
        match req.action.as_str() {
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
        }
    }

    fn supported_actions(&self) -> &[&str] {
        ORGANIZATIONS_ACTIONS
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
        OrgError::AccountAlreadyMember(account) => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AccountAlreadyRegisteredException",
            format!("Account {account} is already a member of this organization."),
        ),
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

fn handshake_payload(h: &crate::state::Handshake) -> Value {
    // Real AWS Organizations encodes the inviter as the org itself
    // (`Type: ORGANIZATION`, `Id` = the org id) and the invitee as the
    // member account (`Type: ACCOUNT`, `Id` = account id) or its email
    // (`Type: EMAIL`, `Id` = email address). The source account id is
    // also exposed as a separate ACCOUNT party so callers can correlate.
    let parties = json!([
        {"Id": h.organization_id, "Type": "ORGANIZATION"},
        {"Id": h.source_account_id, "Type": "ACCOUNT"},
        {
            "Id": h.target_email.clone().unwrap_or_else(|| h.target_account_id.clone()),
            "Type": h.target_kind,
        },
    ]);
    let resources = json!([
        {"Type": "ORGANIZATION", "Value": h.organization_id},
        {"Type": "ACCOUNT", "Value": h.target_account_id},
    ]);
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
