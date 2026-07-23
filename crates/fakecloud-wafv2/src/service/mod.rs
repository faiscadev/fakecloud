//! WAF v2 JSON 1.1 service.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::evaluator::RateLimiter;
use crate::state::{
    AccountState, ApiKey, IpSet, RegexPatternSet, RuleGroup, SharedWafv2State, Wafv2Accounts,
    Wafv2Snapshot, WebAcl, WAFV2_SNAPSHOT_SCHEMA_VERSION,
};

/// Actions that mutate persisted WAFv2 control-plane state and therefore must
/// trigger a snapshot write. Read-only actions (Get*/List*/Check*/Describe*)
/// and data-plane sampled-request telemetry (ephemeral, like introspection
/// buffers) are excluded.
const MUTATING_ACTIONS: &[&str] = &[
    "CreateWebACL",
    "UpdateWebACL",
    "DeleteWebACL",
    "CreateRuleGroup",
    "UpdateRuleGroup",
    "DeleteRuleGroup",
    "CreateIPSet",
    "UpdateIPSet",
    "DeleteIPSet",
    "CreateRegexPatternSet",
    "UpdateRegexPatternSet",
    "DeleteRegexPatternSet",
    "AssociateWebACL",
    "DisassociateWebACL",
    "PutLoggingConfiguration",
    "DeleteLoggingConfiguration",
    "PutPermissionPolicy",
    "DeletePermissionPolicy",
    "TagResource",
    "UntagResource",
    "CreateAPIKey",
    "DeleteAPIKey",
    "PutManagedRuleSetVersions",
    "UpdateManagedRuleSetVersionExpiryDate",
    "DeleteFirewallManagerRuleGroups",
];

const SUPPORTED_ACTIONS: &[&str] = &[
    "AssociateWebACL",
    "CheckCapacity",
    "CreateAPIKey",
    "CreateIPSet",
    "CreateRegexPatternSet",
    "CreateRuleGroup",
    "CreateWebACL",
    "DeleteAPIKey",
    "DeleteFirewallManagerRuleGroups",
    "DeleteIPSet",
    "DeleteLoggingConfiguration",
    "DeletePermissionPolicy",
    "DeleteRegexPatternSet",
    "DeleteRuleGroup",
    "DeleteWebACL",
    "DescribeAllManagedProducts",
    "DescribeManagedProductsByVendor",
    "DescribeManagedRuleGroup",
    "DisassociateWebACL",
    "GenerateMobileSdkReleaseUrl",
    "GetDecryptedAPIKey",
    "GetIPSet",
    "GetLoggingConfiguration",
    "GetManagedRuleSet",
    "GetMobileSdkRelease",
    "GetPermissionPolicy",
    "GetRateBasedStatementManagedKeys",
    "GetRegexPatternSet",
    "GetRuleGroup",
    "GetSampledRequests",
    "GetTopPathStatisticsByTraffic",
    "GetWebACL",
    "GetWebACLForResource",
    "ListAPIKeys",
    "ListAvailableManagedRuleGroups",
    "ListAvailableManagedRuleGroupVersions",
    "ListIPSets",
    "ListLoggingConfigurations",
    "ListManagedRuleSets",
    "ListMobileSdkReleases",
    "ListRegexPatternSets",
    "ListResourcesForWebACL",
    "ListRuleGroups",
    "ListTagsForResource",
    "ListWebACLs",
    "PutLoggingConfiguration",
    "PutManagedRuleSetVersions",
    "PutPermissionPolicy",
    "TagResource",
    "UntagResource",
    "UpdateIPSet",
    "UpdateManagedRuleSetVersionExpiryDate",
    "UpdateRegexPatternSet",
    "UpdateRuleGroup",
    "UpdateWebACL",
];

pub struct Wafv2Service {
    state: SharedWafv2State,
    rate_limiter: Arc<RateLimiter>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

mod api_keys;
mod capacity;
mod ip_sets;
mod logging;
mod mobile_sdk;
mod permission_policy;
mod regex_pattern_sets;
mod rule_groups;
mod sampled_requests;
mod tags;
mod web_acls;

impl Wafv2Service {
    pub fn new(state: SharedWafv2State) -> Self {
        Self::with_rate_limiter(state, Arc::new(RateLimiter::new()))
    }

    /// Construct with an externally-owned rate limiter so the server can
    /// share a single `RateLimiter` between this service and the admin
    /// `/_fakecloud/wafv2/evaluate` endpoint.
    pub fn with_rate_limiter(state: SharedWafv2State, rate_limiter: Arc<RateLimiter>) -> Self {
        Self {
            state,
            rate_limiter,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn shared_state(&self) -> SharedWafv2State {
        Arc::clone(&self.state)
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_wafv2_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current WAFv2 state when invoked, or
    /// `None` in memory mode. The CloudFormation provisioner mutates `state`
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
                save_wafv2_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Shared, in-process [`RateLimiter`] used by `RateBasedStatement`
    /// evaluation. Every dataplane caller (ALB, API Gateway, CloudFront) and
    /// the test admin endpoint must use this same instance so all WAFv2
    /// evaluations through this server share their counters.
    pub fn rate_limiter(&self) -> Arc<RateLimiter> {
        Arc::clone(&self.rate_limiter)
    }
}

/// Persist the current WAFv2 state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `Wafv2Service::save_snapshot` and the
/// CloudFormation provisioner persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_wafv2_snapshot(
    state: &SharedWafv2State,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = Wafv2Snapshot {
        schema_version: WAFV2_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write wafv2 snapshot"),
        Err(err) => tracing::error!(%err, "wafv2 snapshot task panicked"),
    }
}

impl Default for Wafv2Service {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(Wafv2Accounts::new())))
    }
}

#[async_trait]
impl AwsService for Wafv2Service {
    fn service_name(&self) -> &str {
        "wafv2"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = MUTATING_ACTIONS.contains(&req.action.as_str());
        let result = match req.action.as_str() {
            "CreateWebACL" => self.create_web_acl(&req),
            "GetWebACL" => self.get_web_acl(&req),
            "ListWebACLs" => self.list_web_acls(&req),
            "UpdateWebACL" => self.update_web_acl(&req),
            "DeleteWebACL" => self.delete_web_acl(&req),
            "CreateRuleGroup" => self.create_rule_group(&req),
            "GetRuleGroup" => self.get_rule_group(&req),
            "ListRuleGroups" => self.list_rule_groups(&req),
            "UpdateRuleGroup" => self.update_rule_group(&req),
            "DeleteRuleGroup" => self.delete_rule_group(&req),
            "CreateIPSet" => self.create_ip_set(&req),
            "GetIPSet" => self.get_ip_set(&req),
            "ListIPSets" => self.list_ip_sets(&req),
            "UpdateIPSet" => self.update_ip_set(&req),
            "DeleteIPSet" => self.delete_ip_set(&req),
            "CreateRegexPatternSet" => self.create_regex_pattern_set(&req),
            "GetRegexPatternSet" => self.get_regex_pattern_set(&req),
            "ListRegexPatternSets" => self.list_regex_pattern_sets(&req),
            "UpdateRegexPatternSet" => self.update_regex_pattern_set(&req),
            "DeleteRegexPatternSet" => self.delete_regex_pattern_set(&req),
            "AssociateWebACL" => self.associate_web_acl(&req),
            "DisassociateWebACL" => self.disassociate_web_acl(&req),
            "GetWebACLForResource" => self.get_web_acl_for_resource(&req),
            "ListResourcesForWebACL" => self.list_resources_for_web_acl(&req),
            "PutLoggingConfiguration" => self.put_logging_configuration(&req),
            "GetLoggingConfiguration" => self.get_logging_configuration(&req),
            "DeleteLoggingConfiguration" => self.delete_logging_configuration(&req),
            "ListLoggingConfigurations" => self.list_logging_configurations(&req),
            "PutPermissionPolicy" => self.put_permission_policy(&req),
            "GetPermissionPolicy" => self.get_permission_policy(&req),
            "DeletePermissionPolicy" => self.delete_permission_policy(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "CreateAPIKey" => self.create_api_key(&req),
            "DeleteAPIKey" => self.delete_api_key(&req),
            "GetDecryptedAPIKey" => self.get_decrypted_api_key(&req),
            "ListAPIKeys" => self.list_api_keys(&req),
            "DescribeAllManagedProducts" => self.describe_all_managed_products(&req),
            "DescribeManagedProductsByVendor" => self.describe_managed_products_by_vendor(&req),
            "DescribeManagedRuleGroup" => self.describe_managed_rule_group(&req),
            "GetManagedRuleSet" => self.get_managed_rule_set(&req),
            "ListAvailableManagedRuleGroups" => self.list_available_managed_rule_groups(&req),
            "ListAvailableManagedRuleGroupVersions" => {
                self.list_available_managed_rule_group_versions(&req)
            }
            "ListManagedRuleSets" => self.list_managed_rule_sets(&req),
            "PutManagedRuleSetVersions" => self.put_managed_rule_set_versions(&req),
            "UpdateManagedRuleSetVersionExpiryDate" => {
                self.update_managed_rule_set_version_expiry_date(&req)
            }
            "GenerateMobileSdkReleaseUrl" => self.generate_mobile_sdk_release_url(&req),
            "GetMobileSdkRelease" => self.get_mobile_sdk_release(&req),
            "ListMobileSdkReleases" => self.list_mobile_sdk_releases(&req),
            "CheckCapacity" => self.check_capacity(&req),
            "GetSampledRequests" => self.get_sampled_requests(&req),
            "GetTopPathStatisticsByTraffic" => self.get_top_path_statistics_by_traffic(&req),
            "GetRateBasedStatementManagedKeys" => self.get_rate_based_statement_managed_keys(&req),
            "DeleteFirewallManagerRuleGroups" => self.delete_firewall_manager_rule_groups(&req),
            other => Err(AwsServiceError::action_not_implemented("wafv2", other)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

// ─── WebACL ─────────────────────────────────────────────────────────

impl Wafv2Service {}

// ─── RuleGroup ─────────────────────────────────────────────────────

impl Wafv2Service {}

// ─── IPSet ─────────────────────────────────────────────────────────

impl Wafv2Service {}

// ─── RegexPatternSet ───────────────────────────────────────────────

impl Wafv2Service {}

// ─── Associations ───────────────────────────────────────────────────

impl Wafv2Service {}

// ─── Logging Config ────────────────────────────────────────────────

impl Wafv2Service {}

// ─── Permission Policy ─────────────────────────────────────────────

impl Wafv2Service {}

// ─── Tags ───────────────────────────────────────────────────────────

impl Wafv2Service {}

// ─── API Keys ───────────────────────────────────────────────────────

impl Wafv2Service {}

// ─── Managed rule sets / products ──────────────────────────────────

impl Wafv2Service {}

// ─── Mobile SDK ─────────────────────────────────────────────────────

impl Wafv2Service {}

// ─── Misc query / capacity ─────────────────────────────────────────

impl Wafv2Service {
    fn get_top_path_statistics_by_traffic(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy member is `WebAclArn` (lowercase l), unlike most other ops
        // which use `WebACLArn`. Match the model exactly.
        let _web_acl_arn = require_str_len(&body, "WebAclArn", 20, 2048)?;
        let _scope = require_scope(&body)?;
        body.get("TimeWindow")
            .ok_or_else(|| invalid_param("TimeWindow is required"))?;
        let _limit = require_int_range(&body, "Limit", 1, 100)?;
        let _bots_per_path = require_int_range(&body, "NumberOfTopTrafficBotsPerPath", 1, 10)?;
        opt_str_len(&body, "UriPathPrefix", 1, 512)?;
        opt_str_len(&body, "BotCategory", 1, 256)?;
        opt_str_len(&body, "BotName", 1, 256)?;
        opt_str_len(&body, "BotOrganization", 1, 256)?;
        validate_opt_next_marker(&body)?;
        Ok(AwsResponse::ok_json(json!({
            "PathStatistics": [],
            "TopCategories": [],
            "TotalRequestCount": 0_u64,
        })))
    }
}

// ─── Helpers ────────────────────────────────────────────────────────

fn account_mut<'a>(state: &'a mut Wafv2Accounts, account_id: &str) -> &'a mut AccountState {
    state.accounts.entry(account_id.to_string()).or_default()
}

fn require_str(body: &Value, field: &str) -> Result<String, AwsServiceError> {
    body.get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| invalid_param(format!("{field} is required")))
}

fn require_scope(body: &Value) -> Result<String, AwsServiceError> {
    let scope = require_str(body, "Scope")?;
    validate_enum(&scope, &["REGIONAL", "CLOUDFRONT"], "Scope")?;
    Ok(scope)
}

/// Validate a string member against Smithy `@length(min, max)` constraints.
fn validate_str_len(
    value: &str,
    min: usize,
    max: usize,
    field: &str,
) -> Result<(), AwsServiceError> {
    if value.len() < min || value.len() > max {
        return Err(invalid_param(format!(
            "{field} must be between {min} and {max} characters"
        )));
    }
    Ok(())
}

/// Validate an integer member against Smithy `@range(min, max)` constraints.
fn validate_int_range(value: i64, min: i64, max: i64, field: &str) -> Result<(), AwsServiceError> {
    if value < min || value > max {
        return Err(invalid_param(format!(
            "{field} must be between {min} and {max}"
        )));
    }
    Ok(())
}

/// Validate that a value is one of the allowed enum values.
fn validate_enum(value: &str, allowed: &[&str], field: &str) -> Result<(), AwsServiceError> {
    if !allowed.contains(&value) {
        return Err(invalid_param(format!("Invalid {field}: {value}")));
    }
    Ok(())
}

/// Optional string with length bounds. Returns None when absent. When present
/// the value is validated against `[min, max]`.
fn opt_str_len(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
) -> Result<Option<String>, AwsServiceError> {
    match body.get(field).and_then(Value::as_str) {
        Some(s) => {
            validate_str_len(s, min, max, field)?;
            Ok(Some(s.to_owned()))
        }
        None => Ok(None),
    }
}

/// Required string with length bounds.
fn require_str_len(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
) -> Result<String, AwsServiceError> {
    let s = require_str(body, field)?;
    validate_str_len(&s, min, max, field)?;
    Ok(s)
}

/// Optional integer with range bounds.
fn opt_int_range(
    body: &Value,
    field: &str,
    min: i64,
    max: i64,
) -> Result<Option<i64>, AwsServiceError> {
    match body.get(field) {
        Some(v) => {
            let n = v
                .as_i64()
                .ok_or_else(|| invalid_param(format!("{field} must be an integer")))?;
            validate_int_range(n, min, max, field)?;
            Ok(Some(n))
        }
        None => Ok(None),
    }
}

/// Required integer with range bounds.
fn require_int_range(
    body: &Value,
    field: &str,
    min: i64,
    max: i64,
) -> Result<i64, AwsServiceError> {
    let v = body
        .get(field)
        .ok_or_else(|| invalid_param(format!("{field} is required")))?;
    let n = v
        .as_i64()
        .ok_or_else(|| invalid_param(format!("{field} must be an integer")))?;
    validate_int_range(n, min, max, field)?;
    Ok(n)
}

/// Validate the standard `Limit` paging parameter when present. AWS WAFv2
/// uses `@range(min=1, max=100)` on every list operation.
fn validate_opt_limit(body: &Value) -> Result<(), AwsServiceError> {
    opt_int_range(body, "Limit", 1, 100)?;
    Ok(())
}

/// Validate the standard `NextMarker` paging token when present.
/// AWS WAFv2 uses `@length(min=1, max=256)`.
fn validate_opt_next_marker(body: &Value) -> Result<(), AwsServiceError> {
    opt_str_len(body, "NextMarker", 1, 256)?;
    Ok(())
}

fn invalid_param(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "WAFInvalidParameterException", msg)
}

/// Normalize an ELBv2-style `ResourceArn` to the canonical
/// load-balancer ARN. Real AWS associates web ACLs with the load
/// balancer (not the listener), but callers regularly pass through
/// listener ARNs - either by accident or because they only have
/// the listener handy in their CloudFormation template. Trim the
/// listener suffix so the persisted association matches the lb
/// arn the data plane looks up.
///
/// Non-ELBv2 ARNs (API Gateway, AppSync, Cognito, Verified Access)
/// are returned unchanged.
fn normalize_resource_arn(arn: &str) -> String {
    // Listener ARN:
    //   arn:aws:elasticloadbalancing:<region>:<acct>:listener/<type>/<name>/<lb-suffix>/<listener-suffix>
    // LoadBalancer ARN:
    //   arn:aws:elasticloadbalancing:<region>:<acct>:loadbalancer/<type>/<name>/<lb-suffix>
    if let Some(rest) = arn.strip_prefix("arn:aws:elasticloadbalancing:") {
        if let Some((before, after)) = rest.split_once(":listener/") {
            // Listener path has 4 segments (<type>/<name>/<lb-suffix>/<listener-suffix>);
            // drop the trailing listener suffix to recover the lb ARN.
            let mut parts = after.splitn(4, '/');
            let ty = parts.next();
            let name = parts.next();
            let lb_suffix = parts.next();
            if let (Some(ty), Some(name), Some(lb_suffix)) = (ty, name, lb_suffix) {
                return format!(
                    "arn:aws:elasticloadbalancing:{before}:loadbalancer/{ty}/{name}/{lb_suffix}"
                );
            }
        }
    }
    arn.to_string()
}

fn not_found(resource: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "WAFNonexistentItemException",
        format!("{resource} not found"),
    )
}

fn already_exists(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "WAFDuplicateItemException", msg)
}

fn stale_lock_token() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "WAFOptimisticLockException",
        "LockToken does not match the current value; refresh and retry",
    )
}

fn synth_uuid() -> String {
    Uuid::new_v4().to_string()
}

fn synth_arn(
    account_id: &str,
    region: &str,
    scope: &str,
    kind: &str,
    name: &str,
    id: &str,
) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    // Real AWS WAF v2 CLOUDFRONT-scope ARNs always use `us-east-1` as the
    // region segment plus a `global/...` resource path. REGIONAL ARNs use the
    // caller's region with the literal `regional` scope prefix in the resource
    // path (e.g. `arn:aws:wafv2:us-east-1:acct:regional/ipset/name/id`).
    let (region_in_arn, scope_seg) = if scope == "CLOUDFRONT" {
        ("us-east-1", "global")
    } else {
        (region, "regional")
    };
    Arn::new(
        "wafv2",
        region_in_arn,
        account_id,
        &format!("{scope_seg}/{kind}/{name}/{id}"),
    )
    .to_string()
}

fn parse_string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|v| {
            v.iter()
                .filter_map(|s| s.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_tags(value: Option<&Value>) -> Result<BTreeMap<String, String>, AwsServiceError> {
    let mut out = BTreeMap::new();
    let Some(arr) = value.and_then(Value::as_array) else {
        return Ok(out);
    };
    for tag in arr {
        let key = tag
            .get("Key")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("Tag.Key is required"))?
            .to_string();
        let value = tag
            .get("Value")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        out.insert(key, value);
    }
    Ok(out)
}

fn parse_custom_response_bodies(value: Option<&Value>) -> BTreeMap<String, Value> {
    value
        .and_then(Value::as_object)
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

fn resource_exists(account: &AccountState, arn: &str) -> bool {
    account.web_acls.values().any(|w| w.arn == arn)
        || account.rule_groups.values().any(|r| r.arn == arn)
        || account.ip_sets.values().any(|s| s.arn == arn)
        || account.regex_pattern_sets.values().any(|s| s.arn == arn)
}

/// WCU cost is 1 per leaf statement in real WAF. fakecloud uses the
/// recursive count of statement leaves as a stand-in — close enough for
/// CheckCapacity round-tripping and for the WAFLimitsExceeded path.
fn compute_capacity(rules: &[Value]) -> i64 {
    rules
        .iter()
        .map(|r| r.get("Statement").map(count_statement_leaves).unwrap_or(1) as i64)
        .sum()
}

fn count_statement_leaves(stmt: &Value) -> u32 {
    let Some(obj) = stmt.as_object() else {
        return 1;
    };
    let mut total = 0u32;
    for (k, v) in obj {
        match k.as_str() {
            "AndStatement" | "OrStatement" => {
                if let Some(arr) = v.get("Statements").and_then(Value::as_array) {
                    for s in arr {
                        total += count_statement_leaves(s);
                    }
                }
            }
            "NotStatement" => {
                if let Some(s) = v.get("Statement") {
                    total += count_statement_leaves(s);
                }
            }
            _ => {
                total += 1;
            }
        }
    }
    total.max(1)
}

/// A well-known AWS managed rule group. This is the single source of truth
/// behind ListAvailableManagedRuleGroups, DescribeAllManagedProducts, and
/// DescribeManagedRuleGroup so the three ops never disagree about which groups
/// exist, their capacity, or the rules they contain.
pub(super) struct ManagedGroupDef {
    pub vendor: &'static str,
    pub name: &'static str,
    pub product_id: &'static str,
    pub product_title: &'static str,
    pub description: &'static str,
    pub capacity: i64,
    pub rules: &'static [&'static str],
}

/// The AWS baseline managed rule groups fakecloud recognizes. Capacities and
/// rule names mirror the published AWS WAF managed-rule-group reference so a
/// DescribeManagedRuleGroup round-trips real data rather than a fabricated
/// placeholder.
pub(super) fn managed_rule_group_catalog() -> &'static [ManagedGroupDef] {
    &[
        ManagedGroupDef {
            vendor: "AWS",
            name: "AWSManagedRulesCommonRuleSet",
            product_id: "prod-aws-common",
            product_title: "Core rule set",
            description: "OWASP Top 10 baseline rules",
            capacity: 700,
            rules: &[
                "NoUserAgent_HEADER",
                "UserAgent_BadBots_HEADER",
                "SizeRestrictions_QUERYSTRING",
                "SizeRestrictions_Cookie_HEADER",
                "SizeRestrictions_BODY",
                "SizeRestrictions_URIPATH",
                "EC2MetaDataSSRF_BODY",
                "EC2MetaDataSSRF_COOKIE",
                "EC2MetaDataSSRF_URIPATH",
                "EC2MetaDataSSRF_QUERYARGUMENTS",
                "GenericLFI_QUERYARGUMENTS",
                "GenericLFI_URIPATH",
                "GenericLFI_BODY",
                "RestrictedExtensions_URIPATH",
                "RestrictedExtensions_QUERYARGUMENTS",
                "GenericRFI_QUERYARGUMENTS",
                "GenericRFI_BODY",
                "GenericRFI_URIPATH",
                "CrossSiteScripting_COOKIE",
                "CrossSiteScripting_QUERYARGUMENTS",
                "CrossSiteScripting_BODY",
                "CrossSiteScripting_URIPATH",
            ],
        },
        ManagedGroupDef {
            vendor: "AWS",
            name: "AWSManagedRulesKnownBadInputsRuleSet",
            product_id: "prod-aws-known-bad-inputs",
            product_title: "Known bad inputs rule set",
            description: "Block request patterns associated with known exploits",
            capacity: 200,
            rules: &[
                "JavaDeserializationRCE_HEADER",
                "JavaDeserializationRCE_BODY",
                "JavaDeserializationRCE_URIPATH",
                "JavaDeserializationRCE_QUERYSTRING",
                "Host_localhost_HEADER",
                "PROPFIND_METHOD",
                "ExploitablePaths_URIPATH",
                "Log4JRCE_HEADER",
                "Log4JRCE_QUERYSTRING",
                "Log4JRCE_BODY",
                "Log4JRCE_URIPATH",
            ],
        },
        ManagedGroupDef {
            vendor: "AWS",
            name: "AWSManagedRulesSQLiRuleSet",
            product_id: "prod-aws-sqli",
            product_title: "SQL injection rule set",
            description: "Rules that block SQL injection patterns",
            capacity: 200,
            rules: &[
                "SQLi_QUERYARGUMENTS",
                "SQLiExtendedPatterns_QUERYARGUMENTS",
                "SQLi_BODY",
                "SQLiExtendedPatterns_BODY",
                "SQLi_COOKIE",
                "SQLi_URIPATH",
            ],
        },
    ]
}

/// Look up a managed rule group by (vendor, name). Returns `None` for an
/// unknown group so callers can surface WAFInvalidParameterException the way
/// AWS does instead of fabricating a response.
pub(super) fn managed_group_def(vendor: &str, name: &str) -> Option<&'static ManagedGroupDef> {
    managed_rule_group_catalog()
        .iter()
        .find(|d| d.vendor == vendor && d.name == name)
}

fn managed_products() -> Vec<Value> {
    managed_rule_group_catalog()
        .iter()
        .map(|d| {
            json!({
                "VendorName": d.vendor,
                "ManagedRuleSetName": d.name,
                "ProductId": d.product_id,
                "ProductLink": "https://docs.aws.amazon.com/waf/latest/developerguide/aws-managed-rule-groups-list.html",
                "ProductTitle": d.product_title,
                "ProductDescription": d.description,
                "SnsTopicArn": format!("arn:aws:sns:us-east-1::{}-notifications", d.product_id),
                "IsVersioningSupported": true,
                "IsAdvancedManagedRuleSet": false,
            })
        })
        .collect()
}

/// Real rule summaries for a known managed group. Managed groups block by
/// default, so each rule reports a Block action.
pub(super) fn managed_rule_summaries(vendor: &str, name: &str) -> Vec<Value> {
    managed_group_def(vendor, name)
        .map(|d| {
            d.rules
                .iter()
                .map(|r| json!({ "Name": r, "Action": {"Block": {}} }))
                .collect()
        })
        .unwrap_or_default()
}

// ─── JSON shaping ──────────────────────────────────────────────────

fn web_acl_summary_json(
    id: &str,
    name: &str,
    arn: &str,
    description: Option<&str>,
    lock_token: &str,
) -> Value {
    let mut obj = json!({
        "Id": id,
        "Name": name,
        "ARN": arn,
        "LockToken": lock_token,
    });
    if let Some(d) = description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.to_string()));
    }
    obj
}

fn web_acl_detail_json(acl: &WebAcl) -> Value {
    let mut obj = json!({
        "Id": acl.id,
        "Name": acl.name,
        "ARN": acl.arn,
        "DefaultAction": acl.default_action,
        "Rules": acl.rules,
        "VisibilityConfig": acl.visibility_config,
        "Capacity": acl.capacity,
        "ManagedByFirewallManager": acl.managed_by_firewall_manager,
        "RetrofittedByFirewallManager": acl.retrofitted_by_firewall_manager,
        "LabelNamespace": acl.label_namespace,
        "TokenDomains": acl.token_domains,
        "PreProcessFirewallManagerRuleGroups": acl.pre_process_firewall_manager_rule_groups,
        "PostProcessFirewallManagerRuleGroups": acl.post_process_firewall_manager_rule_groups,
    });
    if let Some(d) = &acl.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if !acl.custom_response_bodies.is_empty() {
        obj.as_object_mut().unwrap().insert(
            "CustomResponseBodies".to_string(),
            json!(acl.custom_response_bodies),
        );
    }
    if let Some(c) = &acl.captcha_config {
        obj.as_object_mut()
            .unwrap()
            .insert("CaptchaConfig".to_string(), c.clone());
    }
    if let Some(c) = &acl.challenge_config {
        obj.as_object_mut()
            .unwrap()
            .insert("ChallengeConfig".to_string(), c.clone());
    }
    if let Some(c) = &acl.association_config {
        obj.as_object_mut()
            .unwrap()
            .insert("AssociationConfig".to_string(), c.clone());
    }
    if let Some(c) = &acl.data_protection_config {
        obj.as_object_mut()
            .unwrap()
            .insert("DataProtectionConfig".to_string(), c.clone());
    }
    if let Some(c) = &acl.on_source_d_do_s_protection_config {
        obj.as_object_mut()
            .unwrap()
            .insert("OnSourceDDoSProtectionConfig".to_string(), c.clone());
    }
    if let Some(c) = &acl.application_config {
        obj.as_object_mut()
            .unwrap()
            .insert("ApplicationConfig".to_string(), c.clone());
    }
    obj
}

fn rule_group_summary_json(
    id: &str,
    name: &str,
    arn: &str,
    description: Option<&str>,
    lock_token: &str,
) -> Value {
    let mut obj = json!({
        "Id": id,
        "Name": name,
        "ARN": arn,
        "LockToken": lock_token,
    });
    if let Some(d) = description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.to_string()));
    }
    obj
}

fn rule_group_detail_json(rg: &RuleGroup) -> Value {
    let mut obj = json!({
        "Id": rg.id,
        "Name": rg.name,
        "ARN": rg.arn,
        "Capacity": rg.capacity,
        "Rules": rg.rules,
        "VisibilityConfig": rg.visibility_config,
        "LabelNamespace": rg.label_namespace,
        "AvailableLabels": rg.available_labels,
        "ConsumedLabels": rg.consumed_labels,
    });
    if let Some(d) = &rg.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if !rg.custom_response_bodies.is_empty() {
        obj.as_object_mut().unwrap().insert(
            "CustomResponseBodies".to_string(),
            json!(rg.custom_response_bodies),
        );
    }
    obj
}

fn ip_set_summary_json(
    id: &str,
    name: &str,
    arn: &str,
    description: Option<&str>,
    lock_token: &str,
) -> Value {
    let mut obj = json!({
        "Id": id,
        "Name": name,
        "ARN": arn,
        "LockToken": lock_token,
    });
    if let Some(d) = description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.to_string()));
    }
    obj
}

fn ip_set_detail_json(set: &IpSet) -> Value {
    let mut obj = json!({
        "Id": set.id,
        "Name": set.name,
        "ARN": set.arn,
        "IPAddressVersion": set.ip_address_version,
        "Addresses": set.addresses,
    });
    if let Some(d) = &set.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    obj
}

fn regex_set_summary_json(
    id: &str,
    name: &str,
    arn: &str,
    description: Option<&str>,
    lock_token: &str,
) -> Value {
    let mut obj = json!({
        "Id": id,
        "Name": name,
        "ARN": arn,
        "LockToken": lock_token,
    });
    if let Some(d) = description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.to_string()));
    }
    obj
}

fn regex_set_detail_json(set: &RegexPatternSet) -> Value {
    let mut obj = json!({
        "Id": set.id,
        "Name": set.name,
        "ARN": set.arn,
        "RegularExpressionList": set.regular_expressions,
    });
    if let Some(d) = &set.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    obj
}

#[cfg(test)]
mod arn_norm_tests {
    use super::normalize_resource_arn;

    #[test]
    fn elb_listener_arn_collapses_to_load_balancer_arn() {
        let listener =
            "arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/web/abc/xyz";
        assert_eq!(
            normalize_resource_arn(listener),
            "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/web/abc"
        );
    }

    #[test]
    fn elb_load_balancer_arn_passes_through() {
        let lb = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/web/abc";
        assert_eq!(normalize_resource_arn(lb), lb);
    }

    #[test]
    fn nlb_listener_arn_collapses_to_network_load_balancer_arn() {
        let listener =
            "arn:aws:elasticloadbalancing:eu-west-1:123456789012:listener/net/wire/abc/xyz";
        assert_eq!(
            normalize_resource_arn(listener),
            "arn:aws:elasticloadbalancing:eu-west-1:123456789012:loadbalancer/net/wire/abc"
        );
    }

    #[test]
    fn non_elbv2_arn_passes_through() {
        let apigw = "arn:aws:apigateway:us-east-1::/restapis/abc/stages/prod";
        assert_eq!(normalize_resource_arn(apigw), apigw);
        let cog = "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_xxx";
        assert_eq!(normalize_resource_arn(cog), cog);
    }
}

#[cfg(test)]
mod managed_rule_set_validation_tests {
    use super::*;

    fn make_body(fields: &[(&str, &str)]) -> Value {
        let mut m = serde_json::Map::new();
        for (k, v) in fields {
            m.insert(k.to_string(), Value::String(v.to_string()));
        }
        Value::Object(m)
    }

    #[test]
    fn put_managed_rule_set_versions_rejects_empty_name() {
        let body = make_body(&[
            ("Name", ""),
            ("Id", "id"),
            ("LockToken", "tok"),
            ("Scope", "REGIONAL"),
            ("RecommendedVersion", "1.0"),
        ]);
        let svc = Wafv2Service::default();
        let req = AwsRequest {
            service: "wafv2".into(),
            action: "PutManagedRuleSetVersions".into(),
            method: http::Method::POST,
            raw_path: "/".into(),
            raw_query: String::new(),
            path_segments: Vec::new(),
            query_params: std::collections::HashMap::new(),
            headers: http::HeaderMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            request_id: "r".into(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let res = svc.put_managed_rule_set_versions(&req);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), "ValidationException");
    }

    #[test]
    fn put_managed_rule_set_versions_rejects_long_name() {
        let body = make_body(&[
            ("Name", &"x".repeat(129)),
            ("Id", "id"),
            ("LockToken", "tok"),
            ("Scope", "REGIONAL"),
            ("RecommendedVersion", "1.0"),
        ]);
        let svc = Wafv2Service::default();
        let req = AwsRequest {
            service: "wafv2".into(),
            action: "PutManagedRuleSetVersions".into(),
            method: http::Method::POST,
            raw_path: "/".into(),
            raw_query: String::new(),
            path_segments: Vec::new(),
            query_params: std::collections::HashMap::new(),
            headers: http::HeaderMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            request_id: "r".into(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let res = svc.put_managed_rule_set_versions(&req);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), "ValidationException");
    }

    #[test]
    fn update_managed_rule_set_version_expiry_date_rejects_missing_timestamp() {
        let body = make_body(&[
            ("Name", "name"),
            ("Id", "id"),
            ("LockToken", "tok"),
            ("Scope", "REGIONAL"),
            ("VersionToExpire", "1.0"),
        ]);
        let svc = Wafv2Service::default();
        let req = AwsRequest {
            service: "wafv2".into(),
            action: "UpdateManagedRuleSetVersionExpiryDate".into(),
            method: http::Method::POST,
            raw_path: "/".into(),
            raw_query: String::new(),
            path_segments: Vec::new(),
            query_params: std::collections::HashMap::new(),
            headers: http::HeaderMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            request_id: "r".into(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let res = svc.update_managed_rule_set_version_expiry_date(&req);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), "WAFInvalidParameterException");
    }

    // bug-audit 2026-05-28, 1.7: List* operations reject a malformed NextMarker
    // (paginate_checked -> WAFInvalidParameterException) instead of silently
    // restarting at page 0.
    #[test]
    fn paginate_checked_rejects_invalid_token() {
        use fakecloud_core::pagination::paginate_checked;
        let items: Vec<i32> = (0..5).collect();
        assert!(paginate_checked(&items, Some("not-a-valid-token"), 3).is_err());
        assert!(paginate_checked(&items, Some("2"), 3).is_ok());
        assert!(paginate_checked(&items, None, 3).is_ok());
    }

    fn req_json(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "wafv2".into(),
            action: action.into(),
            method: http::Method::POST,
            raw_path: "/".into(),
            raw_query: String::new(),
            path_segments: Vec::new(),
            query_params: std::collections::HashMap::new(),
            headers: http::HeaderMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            request_id: "r".into(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    /// 1.23: PutManagedRuleSetVersions persists, and ListManagedRuleSets /
    /// ListAvailableManagedRuleGroupVersions return what was published -
    /// real round-trip, not write-only/empty.
    #[test]
    fn managed_rule_set_publishing_round_trips() {
        let svc = Wafv2Service::default();

        // Before publishing, ListManagedRuleSets is empty.
        let resp = svc
            .list_managed_rule_sets(&req_json(
                "ListManagedRuleSets",
                json!({"Scope": "REGIONAL"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["ManagedRuleSets"].as_array().unwrap().len(), 0);

        // Publish two versions with a recommended version.
        svc.put_managed_rule_set_versions(&req_json(
            "PutManagedRuleSetVersions",
            json!({
                "Name": "MyRuleSet",
                "Id": "abc123",
                "Scope": "REGIONAL",
                "LockToken": "tok",
                "RecommendedVersion": "Version_2.0",
                "VersionsToPublish": {
                    "Version_1.0": {"ForecastedLifetime": 30},
                    "Version_2.0": {"ForecastedLifetime": 30},
                },
            }),
        ))
        .unwrap();

        // ListManagedRuleSets now returns the set.
        let resp = svc
            .list_managed_rule_sets(&req_json(
                "ListManagedRuleSets",
                json!({"Scope": "REGIONAL"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let sets = body["ManagedRuleSets"].as_array().unwrap();
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0]["Name"].as_str(), Some("MyRuleSet"));

        // ListAvailableManagedRuleGroupVersions returns the published versions.
        let resp = svc
            .list_available_managed_rule_group_versions(&req_json(
                "ListAvailableManagedRuleGroupVersions",
                json!({"VendorName": "MyVendor", "Name": "MyRuleSet", "Scope": "REGIONAL"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let versions: Vec<&str> = body["Versions"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v["Name"].as_str())
            .collect();
        assert!(versions.contains(&"Version_1.0"));
        assert!(versions.contains(&"Version_2.0"));
        assert_eq!(body["CurrentDefaultVersion"].as_str(), Some("Version_2.0"));

        // A different scope does not see the REGIONAL set.
        let resp = svc
            .list_managed_rule_sets(&req_json(
                "ListManagedRuleSets",
                json!({"Scope": "CLOUDFRONT"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["ManagedRuleSets"].as_array().unwrap().len(), 0);
    }

    // GetManagedRuleSet reads the set published via PutManagedRuleSetVersions
    // rather than fabricating a 200 for a never-published set.
    #[test]
    fn get_managed_rule_set_reads_state() {
        let svc = Wafv2Service::default();

        // Unknown (scope, name) -> WAFNonexistentItemException.
        let res = svc.get_managed_rule_set(&req_json(
            "GetManagedRuleSet",
            json!({"Name": "Nope", "Id": "abc123", "Scope": "REGIONAL"}),
        ));
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), "WAFNonexistentItemException");

        // Publish a set.
        svc.put_managed_rule_set_versions(&req_json(
            "PutManagedRuleSetVersions",
            json!({
                "Name": "MyRuleSet",
                "Id": "abc123",
                "Scope": "REGIONAL",
                "LockToken": "tok",
                "RecommendedVersion": "Version_1.0",
                "VersionsToPublish": {
                    "Version_1.0": {"ForecastedLifetime": 30, "AssociatedRuleGroupArn": "arn:aws:wafv2:us-east-1:123456789012:regional/rulegroup/rg/1"},
                },
            }),
        ))
        .unwrap();

        // Mismatched Id -> still nonexistent.
        let res = svc.get_managed_rule_set(&req_json(
            "GetManagedRuleSet",
            json!({"Name": "MyRuleSet", "Id": "wrong", "Scope": "REGIONAL"}),
        ));
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), "WAFNonexistentItemException");

        // Correct lookup returns the stored set and its published versions.
        let resp = svc
            .get_managed_rule_set(&req_json(
                "GetManagedRuleSet",
                json!({"Name": "MyRuleSet", "Id": "abc123", "Scope": "REGIONAL"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let set = &body["ManagedRuleSet"];
        assert_eq!(set["Name"].as_str(), Some("MyRuleSet"));
        assert_eq!(set["Id"].as_str(), Some("abc123"));
        assert_eq!(set["RecommendedVersion"].as_str(), Some("Version_1.0"));
        let pv = &set["PublishedVersions"]["Version_1.0"];
        assert_eq!(pv["ForecastedLifetime"].as_i64(), Some(30));
        assert_eq!(
            pv["AssociatedRuleGroupArn"].as_str(),
            Some("arn:aws:wafv2:us-east-1:123456789012:regional/rulegroup/rg/1")
        );
    }

    // UpdateManagedRuleSetVersionExpiryDate persisted nothing (validated then
    // returned Ok); it must look up the set + version and store the expiry so
    // GetManagedRuleSet surfaces it (bug-hunt).
    #[test]
    fn update_managed_rule_set_version_expiry_persists() {
        let svc = Wafv2Service::default();

        // A request against a set that was never created still returns the
        // smoke success response (matching AWS's op-level response); nothing is
        // persisted since there is no set to write onto.
        svc.update_managed_rule_set_version_expiry_date(&req_json(
            "UpdateManagedRuleSetVersionExpiryDate",
            json!({
                "Name": "Nope", "Id": "abc123", "Scope": "REGIONAL",
                "LockToken": "tok", "VersionToExpire": "Version_1.0",
                "ExpiryTimestamp": 1_800_000_000.0
            }),
        ))
        .unwrap();

        // Publish a set with one version.
        svc.put_managed_rule_set_versions(&req_json(
            "PutManagedRuleSetVersions",
            json!({
                "Name": "MyRuleSet",
                "Id": "abc123",
                "Scope": "REGIONAL",
                "LockToken": "tok",
                "VersionsToPublish": { "Version_1.0": {"ForecastedLifetime": 30} },
            }),
        ))
        .unwrap();

        // Expiring a version that was never published also succeeds without
        // persisting anything onto a nonexistent version.
        svc.update_managed_rule_set_version_expiry_date(&req_json(
            "UpdateManagedRuleSetVersionExpiryDate",
            json!({
                "Name": "MyRuleSet", "Id": "abc123", "Scope": "REGIONAL",
                "LockToken": "tok", "VersionToExpire": "Version_9.9",
                "ExpiryTimestamp": 1_800_000_000.0
            }),
        ))
        .unwrap();

        // Set the expiry on the published version.
        let resp = svc
            .update_managed_rule_set_version_expiry_date(&req_json(
                "UpdateManagedRuleSetVersionExpiryDate",
                json!({
                    "Name": "MyRuleSet", "Id": "abc123", "Scope": "REGIONAL",
                    "LockToken": "tok", "VersionToExpire": "Version_1.0",
                    "ExpiryTimestamp": 1_800_000_000.0
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["ExpiringVersion"].as_str(), Some("Version_1.0"));
        assert_eq!(body["ExpiryTimestamp"].as_f64(), Some(1_800_000_000.0));

        // GetManagedRuleSet now surfaces the persisted expiry.
        let resp = svc
            .get_managed_rule_set(&req_json(
                "GetManagedRuleSet",
                json!({"Name": "MyRuleSet", "Id": "abc123", "Scope": "REGIONAL"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            body["ManagedRuleSet"]["PublishedVersions"]["Version_1.0"]["ExpiryTimestamp"].as_f64(),
            Some(1_800_000_000.0)
        );
    }

    // UpdateWebACL is full-replace: an optional member omitted from the update
    // request is cleared, not carried over from the previous version.
    #[test]
    fn update_web_acl_clears_omitted_optional_members() {
        let svc = Wafv2Service::default();
        let create = svc
            .create_web_acl(&req_json(
                "CreateWebACL",
                json!({
                    "Name": "acl1",
                    "Scope": "REGIONAL",
                    "DefaultAction": {"Allow": {}},
                    "VisibilityConfig": {
                        "SampledRequestsEnabled": true,
                        "CloudWatchMetricsEnabled": true,
                        "MetricName": "acl1",
                    },
                    "CustomResponseBodies": {
                        "body1": {"ContentType": "TEXT_PLAIN", "Content": "hi"},
                    },
                    "TokenDomains": ["example.com"],
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(create.body.expect_bytes()).unwrap();
        let id = body["Summary"]["Id"].as_str().unwrap().to_string();
        let lock = body["Summary"]["LockToken"].as_str().unwrap().to_string();

        // Update without CustomResponseBodies / TokenDomains -> both cleared.
        svc.update_web_acl(&req_json(
            "UpdateWebACL",
            json!({
                "Name": "acl1",
                "Scope": "REGIONAL",
                "Id": id,
                "LockToken": lock,
                "DefaultAction": {"Allow": {}},
                "VisibilityConfig": {
                    "SampledRequestsEnabled": true,
                    "CloudWatchMetricsEnabled": true,
                    "MetricName": "acl1",
                },
            }),
        ))
        .unwrap();

        let resp = svc
            .get_web_acl(&req_json(
                "GetWebACL",
                json!({"Name": "acl1", "Scope": "REGIONAL"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let acl = &body["WebACL"];
        // CustomResponseBodies is now empty (either absent or {}).
        let crb = acl.get("CustomResponseBodies");
        assert!(
            crb.is_none()
                || crb
                    .unwrap()
                    .as_object()
                    .map(|m| m.is_empty())
                    .unwrap_or(true),
            "CustomResponseBodies should be cleared, got {crb:?}"
        );
        // TokenDomains cleared -> no ApplicationIntegrationURL surfaced.
        assert!(body.get("ApplicationIntegrationURL").is_none());
    }

    // A snapshot written before published_version_details existed carries only
    // the version names; GetManagedRuleSet must still surface those versions.
    #[test]
    fn get_managed_rule_set_synthesizes_detail_for_legacy_versions() {
        let svc = Wafv2Service::default();
        {
            let mut state = svc.state.write();
            let account = account_mut(&mut state, "123456789012");
            account.managed_rule_sets.insert(
                ("REGIONAL".to_string(), "Legacy".to_string()),
                crate::state::ManagedRuleSet {
                    id: "legacy-id".to_string(),
                    name: "Legacy".to_string(),
                    scope: "REGIONAL".to_string(),
                    description: None,
                    lock_token: "tok".to_string(),
                    label_namespace: "awswaf:managed:Legacy".to_string(),
                    recommended_version: Some("Version_1.0".to_string()),
                    published_versions: vec!["Version_1.0".to_string()],
                    // Legacy snapshot: names present, no per-version detail.
                    published_version_details: std::collections::BTreeMap::new(),
                    created_time: Utc::now(),
                },
            );
        }
        let resp = svc
            .get_managed_rule_set(&req_json(
                "GetManagedRuleSet",
                json!({"Name": "Legacy", "Id": "legacy-id", "Scope": "REGIONAL"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let pv = &body["ManagedRuleSet"]["PublishedVersions"];
        assert!(
            pv.get("Version_1.0").is_some(),
            "legacy published version must not be dropped, got {pv:?}"
        );
    }

    // Republishing an existing version keeps its original PublishTimestamp and
    // only advances LastUpdateTimestamp.
    #[test]
    fn republish_preserves_publish_timestamp() {
        let svc = Wafv2Service::default();
        svc.put_managed_rule_set_versions(&req_json(
            "PutManagedRuleSetVersions",
            json!({
                "Name": "Rs", "Id": "id1", "Scope": "REGIONAL", "LockToken": "t",
                "VersionsToPublish": {"Version_1.0": {"ForecastedLifetime": 30}},
            }),
        ))
        .unwrap();
        // Force a distinct, old PublishTimestamp on the stored detail.
        {
            let mut state = svc.state.write();
            let account = account_mut(&mut state, "123456789012");
            let set = account
                .managed_rule_sets
                .get_mut(&("REGIONAL".to_string(), "Rs".to_string()))
                .unwrap();
            let detail = set
                .published_version_details
                .get_mut("Version_1.0")
                .unwrap()
                .as_object_mut()
                .unwrap();
            detail.insert("PublishTimestamp".to_string(), json!(1000.0));
            detail.insert("LastUpdateTimestamp".to_string(), json!(1000.0));
        }
        // Republish the same version.
        svc.put_managed_rule_set_versions(&req_json(
            "PutManagedRuleSetVersions",
            json!({
                "Name": "Rs", "Id": "id1", "Scope": "REGIONAL", "LockToken": "t",
                "VersionsToPublish": {"Version_1.0": {"ForecastedLifetime": 60}},
            }),
        ))
        .unwrap();
        let resp = svc
            .get_managed_rule_set(&req_json(
                "GetManagedRuleSet",
                json!({"Name": "Rs", "Id": "id1", "Scope": "REGIONAL"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let pv = &body["ManagedRuleSet"]["PublishedVersions"]["Version_1.0"];
        assert_eq!(
            pv["PublishTimestamp"].as_f64(),
            Some(1000.0),
            "PublishTimestamp must be preserved on republish"
        );
        assert_ne!(
            pv["LastUpdateTimestamp"].as_f64(),
            Some(1000.0),
            "LastUpdateTimestamp must be refreshed on republish"
        );
    }
}
