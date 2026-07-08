//! AWS Shield awsJson1_1 dispatch + operation handlers.
//!
//! Requests carry `X-Amz-Target: AWSShield_20160616.<Operation>`; dispatch keys
//! off `req.action`. Every operation runs model-driven input validation first
//! (required / length / range / enum), then real, account-partitioned,
//! persisted CRUD. Errors are AWS-shaped and drawn from each operation's
//! declared Smithy error set (`ResourceNotFoundException`,
//! `InvalidParameterException`, `ResourceAlreadyExistsException`,
//! `OptimisticLockException`, `LockedSubscriptionException`,
//! `InvalidResourceException`, `LimitsExceededException`,
//! `NoAssociatedRoleException`, ...).

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{SharedShieldState, ShieldState};

pub const SHIELD_ACTIONS: &[&str] = &[
    "AssociateDRTLogBucket",
    "AssociateDRTRole",
    "AssociateHealthCheck",
    "AssociateProactiveEngagementDetails",
    "CreateProtection",
    "CreateProtectionGroup",
    "CreateSubscription",
    "DeleteProtection",
    "DeleteProtectionGroup",
    "DeleteSubscription",
    "DescribeAttack",
    "DescribeAttackStatistics",
    "DescribeDRTAccess",
    "DescribeEmergencyContactSettings",
    "DescribeProtection",
    "DescribeProtectionGroup",
    "DescribeSubscription",
    "DisableApplicationLayerAutomaticResponse",
    "DisableProactiveEngagement",
    "DisassociateDRTLogBucket",
    "DisassociateDRTRole",
    "DisassociateHealthCheck",
    "EnableApplicationLayerAutomaticResponse",
    "EnableProactiveEngagement",
    "GetSubscriptionState",
    "ListAttacks",
    "ListProtectionGroups",
    "ListProtections",
    "ListResourcesInProtectionGroup",
    "ListTagsForResource",
    "TagResource",
    "UntagResource",
    "UpdateApplicationLayerAutomaticResponse",
    "UpdateEmergencyContactSettings",
    "UpdateProtectionGroup",
    "UpdateSubscription",
];

/// Read-only verbs; any other action mutates persisted state and triggers a
/// snapshot after success. The inverse formulation guarantees no mutation is
/// ever missed if a new op is added.
fn is_mutating_action(action: &str) -> bool {
    const READ_PREFIXES: &[&str] = &["Get", "List", "Describe"];
    !READ_PREFIXES.iter().any(|p| action.starts_with(p))
}

pub struct ShieldService {
    state: SharedShieldState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl ShieldService {
    pub fn new(state: SharedShieldState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

#[async_trait]
impl AwsService for ShieldService {
    fn service_name(&self) -> &str {
        "shield"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        // Model-driven input validation (required/length/range/enum) runs
        // before any handler, mirroring AWS's request-validation phase.
        if let Err(msg) = crate::validate::validate_input(&action, &req.json_body()) {
            return Err(invalid_parameter(msg));
        }
        let result = self.dispatch(&action, &req);
        if is_mutating_action(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SHIELD_ACTIONS
    }
}

impl ShieldService {
    #[allow(clippy::too_many_lines)]
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match action {
            // Protections
            "CreateProtection" => self.create_protection(req),
            "DescribeProtection" => self.describe_protection(req),
            "DeleteProtection" => self.delete_protection(req),
            "ListProtections" => self.list_protections(req),
            // Protection groups
            "CreateProtectionGroup" => self.create_protection_group(req),
            "UpdateProtectionGroup" => self.update_protection_group(req),
            "DeleteProtectionGroup" => self.delete_protection_group(req),
            "DescribeProtectionGroup" => self.describe_protection_group(req),
            "ListProtectionGroups" => self.list_protection_groups(req),
            "ListResourcesInProtectionGroup" => self.list_resources_in_protection_group(req),
            // Subscription
            "CreateSubscription" => self.create_subscription(req),
            "DescribeSubscription" => self.describe_subscription(req),
            "UpdateSubscription" => self.update_subscription(req),
            "DeleteSubscription" => self.delete_subscription(req),
            "GetSubscriptionState" => self.get_subscription_state(req),
            // Attacks (honest empty; no synthetic DDoS records)
            "ListAttacks" => self.list_attacks(req),
            "DescribeAttack" => self.describe_attack(req),
            "DescribeAttackStatistics" => self.describe_attack_statistics(req),
            // DRT access
            "AssociateDRTRole" => self.associate_drt_role(req),
            "DisassociateDRTRole" => self.disassociate_drt_role(req),
            "AssociateDRTLogBucket" => self.associate_drt_log_bucket(req),
            "DisassociateDRTLogBucket" => self.disassociate_drt_log_bucket(req),
            "DescribeDRTAccess" => self.describe_drt_access(req),
            // Health checks
            "AssociateHealthCheck" => self.associate_health_check(req),
            "DisassociateHealthCheck" => self.disassociate_health_check(req),
            // Proactive engagement + emergency contacts
            "AssociateProactiveEngagementDetails" => {
                self.associate_proactive_engagement_details(req)
            }
            "EnableProactiveEngagement" => self.enable_proactive_engagement(req),
            "DisableProactiveEngagement" => self.disable_proactive_engagement(req),
            "UpdateEmergencyContactSettings" => self.update_emergency_contact_settings(req),
            "DescribeEmergencyContactSettings" => self.describe_emergency_contact_settings(req),
            // Application-layer automatic response
            "EnableApplicationLayerAutomaticResponse" => {
                self.enable_application_layer_automatic_response(req)
            }
            "UpdateApplicationLayerAutomaticResponse" => {
                self.update_application_layer_automatic_response(req)
            }
            "DisableApplicationLayerAutomaticResponse" => {
                self.disable_application_layer_automatic_response(req)
            }
            // Tags
            "TagResource" => self.tag_resource(req),
            "UntagResource" => self.untag_resource(req),
            "ListTagsForResource" => self.list_tags_for_resource(req),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                action,
            )),
        }
    }
}

// ---- shared error + helper constructors -----------------------------------

fn err(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg.into())
}

pub(crate) fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    err("InvalidParameterException", msg)
}

pub(crate) fn resource_not_found(msg: impl Into<String>) -> AwsServiceError {
    err("ResourceNotFoundException", msg)
}

pub(crate) fn resource_already_exists(msg: impl Into<String>) -> AwsServiceError {
    err("ResourceAlreadyExistsException", msg)
}

pub(crate) fn invalid_resource(msg: impl Into<String>) -> AwsServiceError {
    err("InvalidResourceException", msg)
}

pub(crate) fn limits_exceeded(msg: impl Into<String>) -> AwsServiceError {
    err("LimitsExceededException", msg)
}

pub(crate) fn no_associated_role(msg: impl Into<String>) -> AwsServiceError {
    err("NoAssociatedRoleException", msg)
}

pub(crate) fn locked_subscription(msg: impl Into<String>) -> AwsServiceError {
    err("LockedSubscriptionException", msg)
}

pub(crate) fn ok(value: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::ok_json(value))
}

/// Current time as epoch seconds (awsJson1_1 default timestamp wire form).
pub(crate) fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// A one-year Shield Advanced commitment, in seconds.
pub(crate) const ONE_YEAR_SECS: f64 = 365.0 * 24.0 * 60.0 * 60.0;

/// Shield is a global service: its ARNs carry an empty region field.
pub(crate) fn protection_arn(account: &str, id: &str) -> String {
    format!("arn:aws:shield::{account}:protection/{id}")
}

pub(crate) fn protection_group_arn(account: &str, id: &str) -> String {
    format!("arn:aws:shield::{account}:protection-group/{id}")
}

pub(crate) fn subscription_arn(account: &str) -> String {
    format!("arn:aws:shield::{account}:subscription")
}

impl ShieldService {
    /// Run `f` against this account's mutable state.
    pub(crate) fn with_account_mut<R>(
        &self,
        req: &AwsRequest,
        f: impl FnOnce(&mut ShieldState) -> R,
    ) -> R {
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        f(acct)
    }

    /// Run `f` against this account's read-only state (empty default if the
    /// account has never been touched).
    pub(crate) fn with_account<R>(&self, req: &AwsRequest, f: impl FnOnce(&ShieldState) -> R) -> R {
        let guard = self.state.read();
        match guard.get(&req.account_id) {
            Some(acct) => f(acct),
            None => f(&ShieldState::default()),
        }
    }
}

include!("handlers.rs");
