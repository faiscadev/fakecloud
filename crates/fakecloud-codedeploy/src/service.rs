//! AWS CodeDeploy awsJson1.1 dispatch + operation handlers.
//!
//! Implements the full CodeDeploy control plane: applications, application
//! revisions, deployment groups, deployment configurations, deployments and
//! their targets/instances, on-premises instances, GitHub account tokens, and
//! resource tagging. There is no real deployment engine, so a `CreateDeployment`
//! mints a deployment in `Created` that deterministically advances through
//! `InProgress` to a terminal `Succeeded` state across successive
//! `GetDeployment`/`BatchGetDeployments` reads (the same lazy-settle pattern
//! CodeBuild, EKS, and Cloud Map use for async state). The predefined
//! `CodeDeployDefault.*` deployment configurations are always resolvable.
//! Everything else is real, persisted, account-partitioned CRUD.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{CodeDeployState, SharedCodeDeployState};
use crate::validate;

pub const CODEDEPLOY_ACTIONS: &[&str] = &[
    "AddTagsToOnPremisesInstances",
    "BatchGetApplicationRevisions",
    "BatchGetApplications",
    "BatchGetDeploymentGroups",
    "BatchGetDeploymentInstances",
    "BatchGetDeploymentTargets",
    "BatchGetDeployments",
    "BatchGetOnPremisesInstances",
    "ContinueDeployment",
    "CreateApplication",
    "CreateDeployment",
    "CreateDeploymentConfig",
    "CreateDeploymentGroup",
    "DeleteApplication",
    "DeleteDeploymentConfig",
    "DeleteDeploymentGroup",
    "DeleteGitHubAccountToken",
    "DeleteResourcesByExternalId",
    "DeregisterOnPremisesInstance",
    "GetApplication",
    "GetApplicationRevision",
    "GetDeployment",
    "GetDeploymentConfig",
    "GetDeploymentGroup",
    "GetDeploymentInstance",
    "GetDeploymentTarget",
    "GetOnPremisesInstance",
    "ListApplicationRevisions",
    "ListApplications",
    "ListDeploymentConfigs",
    "ListDeploymentGroups",
    "ListDeploymentInstances",
    "ListDeploymentTargets",
    "ListDeployments",
    "ListGitHubAccountTokenNames",
    "ListOnPremisesInstances",
    "ListTagsForResource",
    "PutLifecycleEventHookExecutionStatus",
    "RegisterApplicationRevision",
    "RegisterOnPremisesInstance",
    "RemoveTagsFromOnPremisesInstances",
    "SkipWaitTimeForInstanceTermination",
    "StopDeployment",
    "TagResource",
    "UntagResource",
    "UpdateApplication",
    "UpdateDeploymentGroup",
];

pub struct CodeDeployService {
    state: SharedCodeDeployState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl CodeDeployService {
    pub fn new(state: SharedCodeDeployState) -> Self {
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

/// Whether an action mutates persisted state (and therefore triggers a
/// snapshot). `GetDeployment`/`BatchGetDeployments` advance in-progress
/// deployments to a terminal state on read, so they persist too.
fn is_mutating(action: &str) -> bool {
    matches!(
        action,
        "AddTagsToOnPremisesInstances"
            | "BatchGetDeployments"
            | "CreateApplication"
            | "CreateDeployment"
            | "CreateDeploymentConfig"
            | "CreateDeploymentGroup"
            | "DeleteApplication"
            | "DeleteDeploymentConfig"
            | "DeleteDeploymentGroup"
            | "DeleteResourcesByExternalId"
            | "DeregisterOnPremisesInstance"
            | "GetDeployment"
            | "RegisterApplicationRevision"
            | "RegisterOnPremisesInstance"
            | "RemoveTagsFromOnPremisesInstances"
            | "StopDeployment"
            | "TagResource"
            | "UntagResource"
            | "UpdateApplication"
            | "UpdateDeploymentGroup"
    )
}

#[async_trait]
impl AwsService for CodeDeployService {
    fn service_name(&self) -> &str {
        "codedeploy"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        let result = self.dispatch(&action, &req);
        if is_mutating(&action) && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        CODEDEPLOY_ACTIONS
    }
}

impl CodeDeployService {
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match action {
            "CreateApplication" => self.create_application(req),
            "GetApplication" => self.get_application(req),
            "UpdateApplication" => self.update_application(req),
            "DeleteApplication" => self.delete_application(req),
            "ListApplications" => self.list_applications(req),
            "BatchGetApplications" => self.batch_get_applications(req),
            "RegisterApplicationRevision" => self.register_application_revision(req),
            "GetApplicationRevision" => self.get_application_revision(req),
            "ListApplicationRevisions" => self.list_application_revisions(req),
            "BatchGetApplicationRevisions" => self.batch_get_application_revisions(req),
            "CreateDeploymentGroup" => self.create_deployment_group(req),
            "GetDeploymentGroup" => self.get_deployment_group(req),
            "UpdateDeploymentGroup" => self.update_deployment_group(req),
            "DeleteDeploymentGroup" => self.delete_deployment_group(req),
            "ListDeploymentGroups" => self.list_deployment_groups(req),
            "BatchGetDeploymentGroups" => self.batch_get_deployment_groups(req),
            "CreateDeploymentConfig" => self.create_deployment_config(req),
            "GetDeploymentConfig" => self.get_deployment_config(req),
            "DeleteDeploymentConfig" => self.delete_deployment_config(req),
            "ListDeploymentConfigs" => self.list_deployment_configs(req),
            "CreateDeployment" => self.create_deployment(req),
            "GetDeployment" => self.get_deployment(req),
            "BatchGetDeployments" => self.batch_get_deployments(req),
            "ListDeployments" => self.list_deployments(req),
            "StopDeployment" => self.stop_deployment(req),
            "ContinueDeployment" => self.continue_deployment(req),
            "SkipWaitTimeForInstanceTermination" => self.skip_wait_time(req),
            "GetDeploymentTarget" => self.get_deployment_target(req),
            "ListDeploymentTargets" => self.list_deployment_targets(req),
            "BatchGetDeploymentTargets" => self.batch_get_deployment_targets(req),
            "GetDeploymentInstance" => self.get_deployment_instance(req),
            "ListDeploymentInstances" => self.list_deployment_instances(req),
            "BatchGetDeploymentInstances" => self.batch_get_deployment_instances(req),
            "RegisterOnPremisesInstance" => self.register_on_premises_instance(req),
            "DeregisterOnPremisesInstance" => self.deregister_on_premises_instance(req),
            "GetOnPremisesInstance" => self.get_on_premises_instance(req),
            "ListOnPremisesInstances" => self.list_on_premises_instances(req),
            "BatchGetOnPremisesInstances" => self.batch_get_on_premises_instances(req),
            "AddTagsToOnPremisesInstances" => self.add_tags_to_on_premises(req),
            "RemoveTagsFromOnPremisesInstances" => self.remove_tags_from_on_premises(req),
            "ListGitHubAccountTokenNames" => self.list_github_account_token_names(req),
            "DeleteGitHubAccountToken" => self.delete_github_account_token(req),
            "PutLifecycleEventHookExecutionStatus" => self.put_lifecycle_status(req),
            "DeleteResourcesByExternalId" => self.delete_resources_by_external_id(req),
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

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn ts(dt: DateTime<Utc>) -> Value {
    json!(dt.timestamp_millis() as f64 / 1000.0)
}

fn e(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg)
}

fn body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn str_field(b: &Value, key: &str) -> Option<String> {
    b.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn str_list(b: &Value, key: &str) -> Vec<String> {
    b.get(key)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn len_ok(v: &str, min: usize, max: usize) -> bool {
    let n = v.chars().count();
    n >= min && n <= max
}

/// A short random deployment id in AWS `d-XXXXXXXXX` form (9 uppercase
/// base-36 characters).
fn gen_deployment_id() -> String {
    let raw = uuid::Uuid::new_v4().simple().to_string().to_uppercase();
    let suffix: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(9)
        .collect();
    format!("d-{suffix}")
}

fn gen_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

// Field validators. Each returns the validated value or the operation-declared
// error (`*RequiredException` for absence, `Invalid*Exception` for a malformed
// value) so the negative conformance variants land on a declared error.

fn req_application_name(b: &Value) -> Result<String, AwsServiceError> {
    match str_field(b, "applicationName") {
        None => Err(e(
            "ApplicationNameRequiredException",
            "The minimum number of required application inputs was not satisfied.",
        )),
        Some(v) if !len_ok(&v, 1, 100) => Err(e(
            "InvalidApplicationNameException",
            "The application name was specified in an invalid format.",
        )),
        Some(v) => Ok(v),
    }
}

fn check_application_name(v: &str) -> Result<(), AwsServiceError> {
    if len_ok(v, 1, 100) {
        Ok(())
    } else {
        Err(e(
            "InvalidApplicationNameException",
            "The application name was specified in an invalid format.",
        ))
    }
}

fn req_deployment_group_name(b: &Value, key: &str) -> Result<String, AwsServiceError> {
    match str_field(b, key) {
        None => Err(e(
            "DeploymentGroupNameRequiredException",
            "The deployment group name was not specified.",
        )),
        Some(v) if !len_ok(&v, 1, 100) => Err(e(
            "InvalidDeploymentGroupNameException",
            "The deployment group name was specified in an invalid format.",
        )),
        Some(v) => Ok(v),
    }
}

fn check_deployment_group_name(v: &str) -> Result<(), AwsServiceError> {
    if len_ok(v, 1, 100) {
        Ok(())
    } else {
        Err(e(
            "InvalidDeploymentGroupNameException",
            "The deployment group name was specified in an invalid format.",
        ))
    }
}

fn req_deployment_config_name(b: &Value) -> Result<String, AwsServiceError> {
    match str_field(b, "deploymentConfigName") {
        None => Err(e(
            "DeploymentConfigNameRequiredException",
            "The deployment configuration name was not specified.",
        )),
        Some(v) if !len_ok(&v, 1, 100) => Err(e(
            "InvalidDeploymentConfigNameException",
            "The deployment configuration name was specified in an invalid format.",
        )),
        Some(v) => Ok(v),
    }
}

fn check_deployment_config_name(v: &str) -> Result<(), AwsServiceError> {
    if len_ok(v, 1, 100) {
        Ok(())
    } else {
        Err(e(
            "InvalidDeploymentConfigNameException",
            "The deployment configuration name was specified in an invalid format.",
        ))
    }
}

fn req_deployment_id(b: &Value) -> Result<String, AwsServiceError> {
    match str_field(b, "deploymentId") {
        None => Err(e(
            "DeploymentIdRequiredException",
            "The deployment ID was not specified.",
        )),
        Some(v) if v.is_empty() => Err(e(
            "InvalidDeploymentIdException",
            "The deployment ID was specified in an invalid format.",
        )),
        Some(v) => Ok(v),
    }
}

fn req_instance_name(b: &Value) -> Result<String, AwsServiceError> {
    match str_field(b, "instanceName") {
        None => Err(e(
            "InstanceNameRequiredException",
            "An on-premises instance name was not specified.",
        )),
        Some(v) if v.is_empty() => Err(e(
            "InvalidInstanceNameException",
            "The on-premises instance name was specified in an invalid format.",
        )),
        Some(v) => Ok(v),
    }
}

fn req_resource_arn(b: &Value) -> Result<String, AwsServiceError> {
    match str_field(b, "ResourceArn") {
        None => Err(e(
            "ResourceArnRequiredException",
            "The ARN of a resource is required, but was not specified.",
        )),
        Some(v) if !len_ok(&v, 1, 1011) || !is_codedeploy_arn(&v) => Err(e(
            "InvalidArnException",
            "The specified ARN is not in a valid format.",
        )),
        Some(v) => Ok(v),
    }
}

/// Whether `s` is a syntactically valid CodeDeploy resource ARN of a supported
/// type (`application`, `deploymentgroup`, or `deploymentconfig`).
fn is_codedeploy_arn(s: &str) -> bool {
    let parts: Vec<&str> = s.splitn(7, ':').collect();
    if parts.len() < 7 || parts[0] != "arn" || parts[2] != "codedeploy" {
        return false;
    }
    matches!(
        parts[5],
        "application" | "deploymentgroup" | "deploymentconfig"
    )
}

fn application_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:codedeploy:{region}:{account}:application:{name}")
}

fn deployment_group_arn(region: &str, account: &str, app: &str, group: &str) -> String {
    format!("arn:aws:codedeploy:{region}:{account}:deploymentgroup:{app}/{group}")
}

fn deployment_config_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:codedeploy:{region}:{account}:deploymentconfig:{name}")
}

fn opt_enum(
    b: &Value,
    key: &str,
    set: &[&str],
    code: &str,
    msg: &str,
) -> Result<(), AwsServiceError> {
    if let Some(v) = str_field(b, key) {
        if !validate::is_enum(set, &v) {
            return Err(e(code, msg));
        }
    }
    Ok(())
}

/// Merge an incoming `TagList` into `stored`, replacing any existing tag that
/// shares a `Key` (last-write-wins), matching `TagResource` semantics.
fn merge_tags(stored: &mut Vec<Value>, incoming: &[Value]) {
    for t in incoming {
        if let Some(k) = t.get("Key").and_then(Value::as_str).map(str::to_string) {
            stored.retain(|x| x.get("Key").and_then(Value::as_str) != Some(k.as_str()));
        }
        stored.push(t.clone());
    }
}

/// Validate a list of tag filters' `Type` members against `TagFilterType`,
/// returning `err_code` for any value outside the model enum.
fn validate_tag_filters(
    b: &Value,
    key: &str,
    err_code: &str,
    msg: &str,
) -> Result<(), AwsServiceError> {
    if let Some(filters) = b.get(key).and_then(Value::as_array) {
        for f in filters {
            if let Some(t) = f.get("Type").and_then(Value::as_str) {
                if !validate::is_enum(validate::TAG_FILTER_TYPE, t) {
                    return Err(e(err_code, msg));
                }
            }
        }
    }
    Ok(())
}

/// Validate the nested enums shared by CreateDeploymentGroup and
/// UpdateDeploymentGroup: EC2/on-premises tag-filter `Type`, trigger
/// `triggerEvents`, and auto-rollback `events`. Each malformed member maps to
/// the operation-declared `Invalid*Exception`.
fn validate_deployment_group_config(b: &Value) -> Result<(), AwsServiceError> {
    validate_tag_filters(
        b,
        "ec2TagFilters",
        "InvalidEC2TagException",
        "The EC2 tag was specified in an invalid format.",
    )?;
    validate_tag_filters(
        b,
        "onPremisesInstanceTagFilters",
        "InvalidOnPremisesTagCombinationException",
        "The on-premises instance tag was specified in an invalid format.",
    )?;
    if let Some(triggers) = b.get("triggerConfigurations").and_then(Value::as_array) {
        for tc in triggers {
            if let Some(events) = tc.get("triggerEvents").and_then(Value::as_array) {
                for ev in events.iter().filter_map(Value::as_str) {
                    if !validate::is_enum(validate::TRIGGER_EVENT_TYPE, ev) {
                        return Err(e(
                            "InvalidTriggerConfigException",
                            "The trigger was specified in an invalid format.",
                        ));
                    }
                }
            }
        }
    }
    if let Some(events) = b
        .get("autoRollbackConfiguration")
        .and_then(|c| c.get("events"))
        .and_then(Value::as_array)
    {
        for ev in events.iter().filter_map(Value::as_str) {
            if !validate::is_enum(validate::AUTO_ROLLBACK_EVENT, ev) {
                return Err(e(
                    "InvalidAutoRollbackConfigException",
                    "The automatic rollback configuration was specified in an invalid format.",
                ));
            }
        }
    }
    Ok(())
}

/// Validate a `minimumHealthyHosts` structure's `type` enum and `value` range
/// for CreateDeploymentConfig, returning the declared
/// `InvalidMinimumHealthyHostValueException` for a bad type
/// (must be HOST_COUNT|FLEET_PERCENT) or an out-of-range value
/// (FLEET_PERCENT: 0-100; HOST_COUNT: >= 0).
fn validate_minimum_healthy_hosts(b: &Value) -> Result<(), AwsServiceError> {
    let Some(mhh) = b.get("minimumHealthyHosts").filter(|v| !v.is_null()) else {
        return Ok(());
    };
    let bad = || {
        e(
            "InvalidMinimumHealthyHostValueException",
            "The minimum healthy instances value was specified in an invalid format.",
        )
    };
    let mhh_type = mhh.get("type").and_then(Value::as_str);
    if let Some(t) = mhh_type {
        if !validate::is_enum(validate::MINIMUM_HEALTHY_HOSTS_TYPE, t) {
            return Err(bad());
        }
    }
    if let Some(value) = mhh.get("value").and_then(Value::as_i64) {
        let out_of_range = match mhh_type {
            Some("FLEET_PERCENT") => !(0..=100).contains(&value),
            _ => value < 0,
        };
        if out_of_range {
            return Err(bad());
        }
    }
    Ok(())
}

/// Pull `nextToken` from the body and paginate a list of JSON values into
/// `{list_key: [...], nextToken?}`. CodeDeploy list ops have no `maxResults`
/// member, so a fixed page size of 100 matches the service default.
fn paginate_body(items: Vec<Value>, list_key: &str, b: &Value) -> Result<Value, AwsServiceError> {
    let token = str_field(b, "nextToken");
    let (page, next) = paginate_checked(&items, token.as_deref(), 100).map_err(|_| {
        e(
            "InvalidNextTokenException",
            "The next token was specified in an invalid format.",
        )
    })?;
    let mut out = Map::new();
    out.insert(list_key.to_string(), Value::Array(page));
    if let Some(n) = next {
        out.insert("nextToken".to_string(), Value::String(n));
    }
    Ok(Value::Object(out))
}

impl CodeDeployService {
    fn region_account(&self, req: &AwsRequest) -> (String, String) {
        (req.region.clone(), req.account_id.clone())
    }
}

// ---------------------------------------------------------------------------
// Predefined deployment configurations
// ---------------------------------------------------------------------------

/// Return the synthesized `DeploymentConfigInfo` for a predefined
/// `CodeDeployDefault.*` configuration, or `None` if `name` is not predefined.
fn predefined_config(name: &str) -> Option<Value> {
    let server = |mhh_type: &str, value: i64| {
        json!({
            "deploymentConfigId": name,
            "deploymentConfigName": name,
            "computePlatform": "Server",
            "minimumHealthyHosts": { "type": mhh_type, "value": value },
        })
    };
    let canary = |platform: &str, pct: i64, mins: i64| {
        json!({
            "deploymentConfigId": name,
            "deploymentConfigName": name,
            "computePlatform": platform,
            "trafficRoutingConfig": {
                "type": "TimeBasedCanary",
                "timeBasedCanary": { "canaryPercentage": pct, "canaryInterval": mins },
            },
        })
    };
    let linear = |platform: &str, pct: i64, mins: i64| {
        json!({
            "deploymentConfigId": name,
            "deploymentConfigName": name,
            "computePlatform": platform,
            "trafficRoutingConfig": {
                "type": "TimeBasedLinear",
                "timeBasedLinear": { "linearPercentage": pct, "linearInterval": mins },
            },
        })
    };
    let all_at_once = |platform: &str| {
        json!({
            "deploymentConfigId": name,
            "deploymentConfigName": name,
            "computePlatform": platform,
            "trafficRoutingConfig": { "type": "AllAtOnce" },
        })
    };
    match name {
        // EC2/On-Premises (Server).
        "CodeDeployDefault.OneAtATime" => Some(server("HOST_COUNT", 99)),
        "CodeDeployDefault.HalfAtATime" => Some(server("FLEET_PERCENT", 50)),
        "CodeDeployDefault.AllAtOnce" => Some(server("HOST_COUNT", 0)),
        // Lambda.
        "CodeDeployDefault.LambdaAllAtOnce" => Some(all_at_once("Lambda")),
        "CodeDeployDefault.LambdaCanary10Percent5Minutes" => Some(canary("Lambda", 10, 5)),
        "CodeDeployDefault.LambdaCanary10Percent10Minutes" => Some(canary("Lambda", 10, 10)),
        "CodeDeployDefault.LambdaCanary10Percent15Minutes" => Some(canary("Lambda", 10, 15)),
        "CodeDeployDefault.LambdaCanary10Percent30Minutes" => Some(canary("Lambda", 10, 30)),
        "CodeDeployDefault.LambdaLinear10PercentEvery1Minute" => Some(linear("Lambda", 10, 1)),
        "CodeDeployDefault.LambdaLinear10PercentEvery2Minutes" => Some(linear("Lambda", 10, 2)),
        "CodeDeployDefault.LambdaLinear10PercentEvery3Minutes" => Some(linear("Lambda", 10, 3)),
        "CodeDeployDefault.LambdaLinear10PercentEvery10Minutes" => Some(linear("Lambda", 10, 10)),
        // ECS.
        "CodeDeployDefault.ECSAllAtOnce" => Some(all_at_once("ECS")),
        "CodeDeployDefault.ECSCanary10Percent5Minutes" => Some(canary("ECS", 10, 5)),
        "CodeDeployDefault.ECSCanary10Percent15Minutes" => Some(canary("ECS", 10, 15)),
        "CodeDeployDefault.ECSLinear10PercentEvery1Minutes" => Some(linear("ECS", 10, 1)),
        "CodeDeployDefault.ECSLinear10PercentEvery3Minutes" => Some(linear("ECS", 10, 3)),
        _ => None,
    }
}

const PREDEFINED_CONFIG_NAMES: &[&str] = &[
    "CodeDeployDefault.OneAtATime",
    "CodeDeployDefault.HalfAtATime",
    "CodeDeployDefault.AllAtOnce",
    "CodeDeployDefault.LambdaAllAtOnce",
    "CodeDeployDefault.LambdaCanary10Percent5Minutes",
    "CodeDeployDefault.LambdaCanary10Percent10Minutes",
    "CodeDeployDefault.LambdaCanary10Percent15Minutes",
    "CodeDeployDefault.LambdaCanary10Percent30Minutes",
    "CodeDeployDefault.LambdaLinear10PercentEvery1Minute",
    "CodeDeployDefault.LambdaLinear10PercentEvery2Minutes",
    "CodeDeployDefault.LambdaLinear10PercentEvery3Minutes",
    "CodeDeployDefault.LambdaLinear10PercentEvery10Minutes",
    "CodeDeployDefault.ECSAllAtOnce",
    "CodeDeployDefault.ECSCanary10Percent5Minutes",
    "CodeDeployDefault.ECSCanary10Percent15Minutes",
    "CodeDeployDefault.ECSLinear10PercentEvery1Minutes",
    "CodeDeployDefault.ECSLinear10PercentEvery3Minutes",
];

// ---------------------------------------------------------------------------
// Applications
// ---------------------------------------------------------------------------

impl CodeDeployService {
    fn create_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_application_name(&b)?;
        opt_enum(
            &b,
            "computePlatform",
            validate::COMPUTE_PLATFORM,
            "InvalidComputePlatformException",
            "The computePlatform is invalid.",
        )?;
        let (region, account) = self.region_account(req);
        let compute = str_field(&b, "computePlatform").unwrap_or_else(|| "Server".into());
        let app_id = gen_uuid();
        let now = Utc::now();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.applications.contains_key(&name) {
            return Err(e(
                "ApplicationAlreadyExistsException",
                format!("An application already exists with the name {name}."),
            ));
        }
        let info = json!({
            "applicationId": app_id,
            "applicationName": name,
            "createTime": ts(now),
            "linkedToGitHub": false,
            "computePlatform": compute,
        });
        st.applications.insert(name.clone(), info);
        st.application_order.push(name.clone());
        // Persist create-time `tags` under the application ARN so
        // ListTagsForResource echoes them, matching TagResource storage.
        if let Some(tags) = b.get("tags").and_then(Value::as_array) {
            if !tags.is_empty() {
                let arn = application_arn(&region, &account, &name);
                merge_tags(st.tags.entry(arn).or_default(), tags);
            }
        }
        ok(json!({ "applicationId": app_id }))
    }

    fn get_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_application_name(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(info) = st.applications.get(&name) else {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {name}"),
            ));
        };
        ok(json!({ "application": info.clone() }))
    }

    fn update_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        // Both names are optional in the model but length-constrained.
        if let Some(v) = str_field(&b, "applicationName") {
            check_application_name(&v)?;
        }
        if let Some(v) = str_field(&b, "newApplicationName") {
            check_application_name(&v)?;
        }
        let Some(current) = str_field(&b, "applicationName") else {
            return Err(e(
                "ApplicationNameRequiredException",
                "The minimum number of required application inputs was not satisfied.",
            ));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&current) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {current}"),
            ));
        }
        if let Some(new_name) = str_field(&b, "newApplicationName") {
            if new_name != current && st.applications.contains_key(&new_name) {
                return Err(e(
                    "ApplicationAlreadyExistsException",
                    format!("An application already exists with the name {new_name}."),
                ));
            }
            if new_name != current {
                let mut info = st.applications.remove(&current).unwrap();
                if let Some(obj) = info.as_object_mut() {
                    obj.insert("applicationName".into(), json!(new_name));
                }
                st.applications.insert(new_name.clone(), info);
                for n in st.application_order.iter_mut() {
                    if *n == current {
                        *n = new_name.clone();
                    }
                }
                if let Some(revs) = st.app_revisions.remove(&current) {
                    st.app_revisions.insert(new_name.clone(), revs);
                }
                // Re-key the deployment groups and rewrite the embedded
                // `applicationName` in each stored `DeploymentGroupInfo` so a
                // later GetDeploymentGroup reports the renamed application.
                if let Some(mut groups) = st.deployment_groups.remove(&current) {
                    for group in groups.values_mut() {
                        if let Some(obj) = group.as_object_mut() {
                            obj.insert("applicationName".into(), json!(new_name));
                        }
                    }
                    st.deployment_groups.insert(new_name.clone(), groups);
                }
                // Rewrite the `applicationName` on every stored deployment that
                // referenced the old name so ListDeployments(new-name) and
                // GetDeployment report the renamed application.
                for deployment in st.deployments.values_mut() {
                    if deployment.get("applicationName").and_then(Value::as_str)
                        == Some(current.as_str())
                    {
                        if let Some(obj) = deployment.as_object_mut() {
                            obj.insert("applicationName".into(), json!(new_name));
                        }
                    }
                }
            }
        }
        ok(json!({}))
    }

    fn delete_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_application_name(&b)?;
        let (region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // DeleteApplication is idempotent: its Smithy contract declares no
        // "does not exist" error, so a missing application is a silent success.
        st.applications.remove(&name);
        st.application_order.retain(|n| n != &name);
        st.app_revisions.remove(&name);
        // Purge tags for the application and every deployment group under it, so
        // a later same-named resource does not resurrect stale tags.
        st.tags.remove(&application_arn(&region, &account, &name));
        if let Some(groups) = st.deployment_groups.remove(&name) {
            for dg in groups.keys() {
                st.tags
                    .remove(&deployment_group_arn(&region, &account, &name, dg));
            }
        }
        ok(json!({}))
    }

    fn list_applications(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let names: Vec<Value> = st.application_order.iter().map(|n| json!(n)).collect();
        ok(paginate_body(names, "applications", &b)?)
    }

    fn batch_get_applications(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let names = str_list(&b, "applicationNames");
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // The output shape carries no per-item error field, so AWS raises a
        // whole-call `ApplicationDoesNotExistException` (declared) when any
        // requested name is unknown rather than silently omitting it.
        let mut infos = Vec::with_capacity(names.len());
        for n in &names {
            match st.applications.get(n) {
                Some(info) => infos.push(info.clone()),
                None => {
                    return Err(e(
                        "ApplicationDoesNotExistException",
                        format!("No application found for name: {n}"),
                    ))
                }
            }
        }
        ok(json!({ "applicationsInfo": infos }))
    }
}

// ---------------------------------------------------------------------------
// Application revisions
// ---------------------------------------------------------------------------

/// Validate a `RevisionLocation` structure's `revisionType` enum, when present.
fn validate_revision(rev: &Value) -> Result<(), AwsServiceError> {
    if let Some(t) = rev.get("revisionType").and_then(Value::as_str) {
        if !validate::is_enum(validate::REVISION_LOCATION_TYPE, t) {
            return Err(e(
                "InvalidRevisionException",
                "The revision was specified in an invalid format.",
            ));
        }
    }
    Ok(())
}

impl CodeDeployService {
    fn register_application_revision(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_application_name(&b)?;
        let Some(revision) = b.get("revision").filter(|v| !v.is_null()).cloned() else {
            return Err(e(
                "RevisionRequiredException",
                "The revision ID was not specified.",
            ));
        };
        validate_revision(&revision)?;
        let now = Utc::now();
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&name) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {name}"),
            ));
        }
        let description = str_field(&b, "description");
        let revs = st.app_revisions.entry(name).or_default();
        // If this revision location was already registered, refresh its last-used
        // time; otherwise append a new record.
        if let Some(existing) = revs
            .iter_mut()
            .find(|r| r.get("revisionLocation") == Some(&revision))
        {
            if let Some(obj) = existing.as_object_mut() {
                let generic = obj
                    .entry("genericRevisionInfo")
                    .or_insert_with(|| json!({}));
                if let Some(g) = generic.as_object_mut() {
                    g.insert("lastUsedTime".into(), ts(now));
                    if let Some(d) = &description {
                        g.insert("description".into(), json!(d));
                    }
                }
            }
        } else {
            let mut generic = Map::new();
            generic.insert("registerTime".into(), ts(now));
            generic.insert("firstUsedTime".into(), ts(now));
            generic.insert("lastUsedTime".into(), ts(now));
            generic.insert("deploymentGroups".into(), json!([]));
            if let Some(d) = &description {
                generic.insert("description".into(), json!(d));
            }
            revs.push(json!({
                "revisionLocation": revision,
                "genericRevisionInfo": Value::Object(generic),
            }));
        }
        ok(json!({}))
    }

    fn get_application_revision(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_application_name(&b)?;
        let Some(revision) = b.get("revision").filter(|v| !v.is_null()).cloned() else {
            return Err(e(
                "RevisionRequiredException",
                "The revision ID was not specified.",
            ));
        };
        validate_revision(&revision)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&name) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {name}"),
            ));
        }
        let record = st.app_revisions.get(&name).and_then(|revs| {
            revs.iter()
                .find(|r| r.get("revisionLocation") == Some(&revision))
        });
        let Some(record) = record else {
            return Err(e(
                "RevisionDoesNotExistException",
                "The named revision does not exist with the applicable IAM user or AWS account.",
            ));
        };
        ok(json!({
            "applicationName": name,
            "revision": revision,
            "revisionInfo": record.get("genericRevisionInfo").cloned().unwrap_or_else(|| json!({})),
        }))
    }

    fn list_application_revisions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_application_name(&b)?;
        opt_enum(&b, "sortBy", validate::APPLICATION_REVISION_SORT_BY, "InvalidSortByException", "The column name to sort by is either not present or was specified in an invalid format.")?;
        opt_enum(
            &b,
            "sortOrder",
            validate::SORT_ORDER,
            "InvalidSortOrderException",
            "The sort order was specified in an invalid format.",
        )?;
        opt_enum(
            &b,
            "deployed",
            validate::LIST_STATE_FILTER_ACTION,
            "InvalidDeployedStateFilterException",
            "The deployed state filter was specified in an invalid format.",
        )?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&name) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {name}"),
            ));
        }
        // A revision counts as "deployed" when it has been used by at least one
        // deployment group (a non-empty `deploymentGroups` in its generic info).
        let is_deployed = |r: &Value| -> bool {
            r.get("genericRevisionInfo")
                .and_then(|g| g.get("deploymentGroups"))
                .and_then(Value::as_array)
                .map(|a| !a.is_empty())
                .unwrap_or(false)
        };
        let deployed_filter = str_field(&b, "deployed").unwrap_or_else(|| "ignore".into());
        let mut records: Vec<Value> = st
            .app_revisions
            .get(&name)
            .map(|revs| {
                revs.iter()
                    .filter(|r| match deployed_filter.as_str() {
                        "include" => is_deployed(r),
                        "exclude" => !is_deployed(r),
                        _ => true,
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        // Order by the requested `sortBy` timestamp (default: registration
        // order) and `sortOrder` (default: ascending).
        if let Some(sort_by) = str_field(&b, "sortBy") {
            let key = |r: &Value| -> f64 {
                r.get("genericRevisionInfo")
                    .and_then(|g| g.get(sort_by.as_str()))
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
            };
            records.sort_by(|x, y| {
                key(x)
                    .partial_cmp(&key(y))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            if str_field(&b, "sortOrder").as_deref() == Some("descending") {
                records.reverse();
            }
        }
        let revisions: Vec<Value> = records
            .iter()
            .filter_map(|r| r.get("revisionLocation").cloned())
            .collect();
        ok(paginate_body(revisions, "revisions", &b)?)
    }

    fn batch_get_application_revisions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_application_name(&b)?;
        let requested = b
            .get("revisions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&name) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {name}"),
            ));
        }
        let stored = st.app_revisions.get(&name);
        let infos: Vec<Value> = requested
            .into_iter()
            .map(|loc| {
                let generic = stored
                    .and_then(|revs| {
                        revs.iter()
                            .find(|r| r.get("revisionLocation") == Some(&loc))
                    })
                    .and_then(|r| r.get("genericRevisionInfo").cloned())
                    .unwrap_or_else(|| json!({}));
                json!({ "revisionLocation": loc, "genericRevisionInfo": generic })
            })
            .collect();
        ok(json!({ "applicationName": name, "revisions": infos }))
    }
}

// ---------------------------------------------------------------------------
// Deployment groups
// ---------------------------------------------------------------------------

/// Resolved identifiers for assembling a `DeploymentGroupInfo`.
struct DeploymentGroupIdent<'a> {
    app: &'a str,
    dg_name: &'a str,
    dg_id: &'a str,
    service_role: &'a str,
    config_name: &'a str,
    compute: &'a str,
}

impl CodeDeployService {
    /// Resolve the compute platform of an application (default `Server`).
    fn app_compute_platform(st: &CodeDeployState, app: &str) -> String {
        st.applications
            .get(app)
            .and_then(|a| a.get("computePlatform").and_then(Value::as_str))
            .unwrap_or("Server")
            .to_string()
    }

    /// Assemble a `DeploymentGroupInfo` from a create input body plus the
    /// resolved identifiers.
    fn assemble_deployment_group(&self, b: &Value, ident: &DeploymentGroupIdent) -> Value {
        let mut g = Map::new();
        g.insert("applicationName".into(), json!(ident.app));
        g.insert("deploymentGroupId".into(), json!(ident.dg_id));
        g.insert("deploymentGroupName".into(), json!(ident.dg_name));
        g.insert("deploymentConfigName".into(), json!(ident.config_name));
        g.insert("computePlatform".into(), json!(ident.compute));
        g.insert("serviceRoleArn".into(), json!(ident.service_role));
        for k in [
            "ec2TagFilters",
            "onPremisesInstanceTagFilters",
            "autoScalingGroups",
            "triggerConfigurations",
            "alarmConfiguration",
            "autoRollbackConfiguration",
            "deploymentStyle",
            "outdatedInstancesStrategy",
            "blueGreenDeploymentConfiguration",
            "loadBalancerInfo",
            "ec2TagSet",
            "onPremisesTagSet",
            "ecsServices",
            "terminationHookEnabled",
        ] {
            if let Some(v) = b.get(k).filter(|v| !v.is_null()) {
                g.insert(k.into(), v.clone());
            }
        }
        Value::Object(g)
    }

    fn create_deployment_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let app = req_application_name(&b)?;
        let dg_name = req_deployment_group_name(&b, "deploymentGroupName")?;
        if let Some(v) = str_field(&b, "deploymentConfigName") {
            check_deployment_config_name(&v)?;
        }
        opt_enum(
            &b,
            "outdatedInstancesStrategy",
            validate::OUTDATED_INSTANCES_STRATEGY,
            "InvalidInputException",
            "The outdated instances strategy is invalid.",
        )?;
        validate_deployment_group_config(&b)?;
        let Some(service_role) = str_field(&b, "serviceRoleArn") else {
            return Err(e("RoleRequiredException", "The role ID was not specified."));
        };
        let (region, account) = self.region_account(req);
        let config_name = str_field(&b, "deploymentConfigName")
            .unwrap_or_else(|| "CodeDeployDefault.OneAtATime".into());
        let dg_id = gen_uuid();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&app) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {app}"),
            ));
        }
        if predefined_config(&config_name).is_none()
            && !st.deployment_configs.contains_key(&config_name)
        {
            return Err(e(
                "DeploymentConfigDoesNotExistException",
                format!("No deployment configuration found for name: {config_name}"),
            ));
        }
        let groups = st.deployment_groups.entry(app.clone()).or_default();
        if groups.contains_key(&dg_name) {
            return Err(e(
                "DeploymentGroupAlreadyExistsException",
                format!("A deployment group with the name {dg_name} already exists."),
            ));
        }
        let compute = Self::app_compute_platform(st, &app);
        let group = self.assemble_deployment_group(
            &b,
            &DeploymentGroupIdent {
                app: &app,
                dg_name: &dg_name,
                dg_id: &dg_id,
                service_role: &service_role,
                config_name: &config_name,
                compute: &compute,
            },
        );
        st.deployment_groups
            .entry(app.clone())
            .or_default()
            .insert(dg_name.clone(), group);
        // Persist create-time `tags` under the deployment group ARN so
        // ListTagsForResource echoes them.
        if let Some(tags) = b.get("tags").and_then(Value::as_array) {
            if !tags.is_empty() {
                let arn = deployment_group_arn(&region, &account, &app, &dg_name);
                merge_tags(st.tags.entry(arn).or_default(), tags);
            }
        }
        ok(json!({ "deploymentGroupId": dg_id }))
    }

    fn get_deployment_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let app = req_application_name(&b)?;
        let dg_name = req_deployment_group_name(&b, "deploymentGroupName")?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&app) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {app}"),
            ));
        }
        let Some(group) = st.deployment_groups.get(&app).and_then(|g| g.get(&dg_name)) else {
            return Err(e(
                "DeploymentGroupDoesNotExistException",
                format!("No deployment group found for name: {dg_name}"),
            ));
        };
        ok(json!({ "deploymentGroupInfo": group.clone() }))
    }

    fn update_deployment_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let app = req_application_name(&b)?;
        let current = req_deployment_group_name(&b, "currentDeploymentGroupName")?;
        if let Some(v) = str_field(&b, "newDeploymentGroupName") {
            check_deployment_group_name(&v)?;
        }
        if let Some(v) = str_field(&b, "deploymentConfigName") {
            check_deployment_config_name(&v)?;
        }
        opt_enum(
            &b,
            "outdatedInstancesStrategy",
            validate::OUTDATED_INSTANCES_STRATEGY,
            "InvalidInputException",
            "The outdated instances strategy is invalid.",
        )?;
        validate_deployment_group_config(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&app) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {app}"),
            ));
        }
        let new_name = str_field(&b, "newDeploymentGroupName");
        if let Some(new) = &new_name {
            if new != &current
                && st
                    .deployment_groups
                    .get(&app)
                    .map(|g| g.contains_key(new))
                    .unwrap_or(false)
            {
                return Err(e(
                    "DeploymentGroupAlreadyExistsException",
                    format!("A deployment group with the name {new} already exists."),
                ));
            }
        }
        let Some(existing) = st
            .deployment_groups
            .get(&app)
            .and_then(|g| g.get(&current))
            .cloned()
        else {
            return Err(e(
                "DeploymentGroupDoesNotExistException",
                format!("No deployment group found for name: {current}"),
            ));
        };
        let mut merged = existing;
        if let Some(obj) = merged.as_object_mut() {
            for k in [
                "deploymentConfigName",
                "ec2TagFilters",
                "onPremisesInstanceTagFilters",
                "autoScalingGroups",
                "serviceRoleArn",
                "triggerConfigurations",
                "alarmConfiguration",
                "autoRollbackConfiguration",
                "deploymentStyle",
                "outdatedInstancesStrategy",
                "blueGreenDeploymentConfiguration",
                "loadBalancerInfo",
                "ec2TagSet",
                "onPremisesTagSet",
                "ecsServices",
                "terminationHookEnabled",
            ] {
                if let Some(v) = b.get(k).filter(|v| !v.is_null()) {
                    obj.insert(k.into(), v.clone());
                }
            }
            if let Some(new) = &new_name {
                obj.insert("deploymentGroupName".into(), json!(new));
            }
        }
        let groups = st.deployment_groups.entry(app).or_default();
        groups.remove(&current);
        let final_name = new_name.unwrap_or(current);
        groups.insert(final_name, merged);
        ok(json!({ "hooksNotCleanedUp": [] }))
    }

    fn delete_deployment_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let app = req_application_name(&b)?;
        let dg_name = req_deployment_group_name(&b, "deploymentGroupName")?;
        let (region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Idempotent: the Smithy contract declares no "does not exist" error.
        if let Some(groups) = st.deployment_groups.get_mut(&app) {
            groups.remove(&dg_name);
        }
        // Purge tags so a later same-named group does not resurrect stale tags.
        st.tags
            .remove(&deployment_group_arn(&region, &account, &app, &dg_name));
        ok(json!({ "hooksNotCleanedUp": [] }))
    }

    fn list_deployment_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let app = req_application_name(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&app) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {app}"),
            ));
        }
        let names: Vec<Value> = st
            .deployment_groups
            .get(&app)
            .map(|g| g.keys().map(|k| json!(k)).collect())
            .unwrap_or_default();
        let out = paginate_body(names, "deploymentGroups", &b)?;
        let mut out = out.as_object().cloned().unwrap_or_default();
        out.insert("applicationName".into(), json!(app));
        ok(Value::Object(out))
    }

    fn batch_get_deployment_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let app = req_application_name(&b)?;
        let names = str_list(&b, "deploymentGroupNames");
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&app) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {app}"),
            ));
        }
        let groups = st.deployment_groups.get(&app);
        let infos: Vec<Value> = names
            .iter()
            .filter_map(|n| groups.and_then(|g| g.get(n)).cloned())
            .collect();
        ok(json!({ "deploymentGroupsInfo": infos }))
    }
}

// ---------------------------------------------------------------------------
// Deployment configurations
// ---------------------------------------------------------------------------

impl CodeDeployService {
    fn create_deployment_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_deployment_config_name(&b)?;
        opt_enum(
            &b,
            "computePlatform",
            validate::COMPUTE_PLATFORM,
            "InvalidComputePlatformException",
            "The computePlatform is invalid.",
        )?;
        validate_minimum_healthy_hosts(&b)?;
        let (_region, account) = self.region_account(req);
        let compute = str_field(&b, "computePlatform").unwrap_or_else(|| "Server".into());
        let config_id = gen_uuid();
        let now = Utc::now();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if predefined_config(&name).is_some() || st.deployment_configs.contains_key(&name) {
            return Err(e(
                "DeploymentConfigAlreadyExistsException",
                format!("A deployment configuration already exists with the name {name}."),
            ));
        }
        let mut info = Map::new();
        info.insert("deploymentConfigId".into(), json!(config_id));
        info.insert("deploymentConfigName".into(), json!(name));
        info.insert("computePlatform".into(), json!(compute));
        info.insert("createTime".into(), ts(now));
        for k in ["minimumHealthyHosts", "trafficRoutingConfig", "zonalConfig"] {
            if let Some(v) = b.get(k).filter(|v| !v.is_null()) {
                info.insert(k.into(), v.clone());
            }
        }
        st.deployment_configs
            .insert(name.clone(), Value::Object(info));
        st.deployment_config_order.push(name);
        ok(json!({ "deploymentConfigId": config_id }))
    }

    fn get_deployment_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_deployment_config_name(&b)?;
        if let Some(info) = predefined_config(&name) {
            return ok(json!({ "deploymentConfigInfo": info }));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(info) = st.deployment_configs.get(&name) else {
            return Err(e(
                "DeploymentConfigDoesNotExistException",
                format!("No deployment configuration found for name: {name}"),
            ));
        };
        ok(json!({ "deploymentConfigInfo": info.clone() }))
    }

    fn delete_deployment_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_deployment_config_name(&b)?;
        if predefined_config(&name).is_some() {
            return Err(e(
                "InvalidOperationException",
                "A predefined deployment configuration cannot be deleted.",
            ));
        }
        let (region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.deployment_configs.remove(&name).is_some() {
            st.deployment_config_order.retain(|n| n != &name);
        }
        // Purge tags so a later same-named config does not resurrect stale tags.
        st.tags
            .remove(&deployment_config_arn(&region, &account, &name));
        ok(json!({}))
    }

    fn list_deployment_configs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut names: Vec<Value> = PREDEFINED_CONFIG_NAMES.iter().map(|n| json!(n)).collect();
        names.extend(st.deployment_config_order.iter().map(|n| json!(n)));
        ok(paginate_body(names, "deploymentConfigsList", &b)?)
    }
}

// ---------------------------------------------------------------------------
// Deployments
// ---------------------------------------------------------------------------

/// Advance an in-progress deployment one lifecycle step on read. The first
/// read moves `Created` -> `InProgress` (stamping `startTime`); the next moves
/// it to a terminal `Succeeded` (stamping `completeTime` and a fully-succeeded
/// `deploymentOverview`). Terminal deployments are left untouched.
fn settle_deployment(info: &mut Value, step: &mut u8) {
    let status = info
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("Created");
    if matches!(status, "Succeeded" | "Failed" | "Stopped") {
        return;
    }
    let now = Utc::now();
    *step = step.saturating_add(1);
    if let Some(obj) = info.as_object_mut() {
        if *step == 1 {
            obj.insert("status".into(), json!("InProgress"));
            obj.entry("startTime").or_insert_with(|| ts(now));
            obj.insert(
                "deploymentOverview".into(),
                json!({ "Pending": 0, "InProgress": 1, "Succeeded": 0, "Failed": 0, "Skipped": 0, "Ready": 0 }),
            );
        } else {
            obj.insert("status".into(), json!("Succeeded"));
            obj.entry("startTime").or_insert_with(|| ts(now));
            obj.insert("completeTime".into(), ts(now));
            obj.insert(
                "deploymentOverview".into(),
                json!({ "Pending": 0, "InProgress": 0, "Succeeded": 1, "Failed": 0, "Skipped": 0, "Ready": 0 }),
            );
        }
    }
}

impl CodeDeployService {
    fn create_deployment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let app = req_application_name(&b)?;
        // A deployment must target a deployment group. The model declares
        // DeploymentGroupNameRequiredException for its absence and
        // InvalidDeploymentGroupNameException for a malformed value.
        let dg_name = req_deployment_group_name(&b, "deploymentGroupName")?;
        if let Some(v) = str_field(&b, "deploymentConfigName") {
            check_deployment_config_name(&v)?;
        }
        opt_enum(
            &b,
            "fileExistsBehavior",
            validate::FILE_EXISTS_BEHAVIOR,
            "InvalidFileExistsBehaviorException",
            "The specified fileExistsBehavior value is invalid.",
        )?;
        if let Some(rev) = b.get("revision").filter(|v| !v.is_null()) {
            validate_revision(rev)?;
        }
        let (_region, account) = self.region_account(req);
        let now = Utc::now();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.applications.contains_key(&app) {
            return Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {app}"),
            ));
        }
        if !st
            .deployment_groups
            .get(&app)
            .map(|g| g.contains_key(&dg_name))
            .unwrap_or(false)
        {
            return Err(e(
                "DeploymentGroupDoesNotExistException",
                format!("No deployment group found for name: {dg_name}"),
            ));
        }
        let config_name = str_field(&b, "deploymentConfigName").unwrap_or_else(|| {
            st.deployment_groups
                .get(&app)
                .and_then(|g| g.get(&dg_name))
                .and_then(|g| g.get("deploymentConfigName").and_then(Value::as_str))
                .unwrap_or("CodeDeployDefault.OneAtATime")
                .to_string()
        });
        if predefined_config(&config_name).is_none()
            && !st.deployment_configs.contains_key(&config_name)
        {
            return Err(e(
                "DeploymentConfigDoesNotExistException",
                format!("No deployment configuration found for name: {config_name}"),
            ));
        }
        let compute = Self::app_compute_platform(st, &app);
        // Inherit the target group's stored deploymentStyle; fall back to the
        // AWS default (in-place, no traffic control) only when the group has none.
        let deployment_style = st
            .deployment_groups
            .get(&app)
            .and_then(|g| g.get(&dg_name))
            .and_then(|g| g.get("deploymentStyle").cloned())
            .unwrap_or_else(|| {
                json!({ "deploymentType": "IN_PLACE", "deploymentOption": "WITHOUT_TRAFFIC_CONTROL" })
            });
        let deployment_id = gen_deployment_id();
        let mut info = Map::new();
        info.insert("deploymentId".into(), json!(deployment_id));
        info.insert("applicationName".into(), json!(app));
        info.insert("deploymentGroupName".into(), json!(dg_name));
        info.insert("deploymentConfigName".into(), json!(config_name));
        info.insert("computePlatform".into(), json!(compute));
        info.insert("status".into(), json!("Created"));
        info.insert("creator".into(), json!("user"));
        info.insert("createTime".into(), ts(now));
        info.insert("deploymentStyle".into(), deployment_style);
        info.insert(
            "ignoreApplicationStopFailures".into(),
            json!(b
                .get("ignoreApplicationStopFailures")
                .and_then(Value::as_bool)
                .unwrap_or(false)),
        );
        info.insert(
            "updateOutdatedInstancesOnly".into(),
            json!(b
                .get("updateOutdatedInstancesOnly")
                .and_then(Value::as_bool)
                .unwrap_or(false)),
        );
        for (in_key, out_key) in [
            ("revision", "revision"),
            ("description", "description"),
            ("autoRollbackConfiguration", "autoRollbackConfiguration"),
            ("fileExistsBehavior", "fileExistsBehavior"),
            ("targetInstances", "targetInstances"),
            ("overrideAlarmConfiguration", "overrideAlarmConfiguration"),
        ] {
            if let Some(v) = b.get(in_key).filter(|v| !v.is_null()) {
                info.insert(out_key.into(), v.clone());
            }
        }
        let info = Value::Object(info);
        st.deployments.insert(deployment_id.clone(), info);
        st.deployment_order.push(deployment_id.clone());
        st.deployment_settle.insert(deployment_id.clone(), 0);
        ok(json!({ "deploymentId": deployment_id }))
    }

    fn get_deployment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = req_deployment_id(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut step = st.deployment_settle.get(&id).copied().unwrap_or(0);
        let Some(info) = st.deployments.get_mut(&id) else {
            return Err(e(
                "DeploymentDoesNotExistException",
                format!("The deployment {id} could not be found."),
            ));
        };
        settle_deployment(info, &mut step);
        let info = info.clone();
        st.deployment_settle.insert(id, step);
        ok(json!({ "deploymentInfo": info }))
    }

    fn batch_get_deployments(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let ids = str_list(&b, "deploymentIds");
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut infos = Vec::new();
        for id in ids {
            let mut step = st.deployment_settle.get(&id).copied().unwrap_or(0);
            if let Some(info) = st.deployments.get_mut(&id) {
                settle_deployment(info, &mut step);
                infos.push(info.clone());
                st.deployment_settle.insert(id, step);
            }
        }
        ok(json!({ "deploymentsInfo": infos }))
    }

    fn list_deployments(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        if let Some(v) = str_field(&b, "applicationName") {
            check_application_name(&v)?;
        }
        if let Some(v) = str_field(&b, "deploymentGroupName") {
            check_deployment_group_name(&v)?;
        }
        // A deploymentGroupName without an applicationName is invalid.
        if str_field(&b, "deploymentGroupName").is_some()
            && str_field(&b, "applicationName").is_none()
        {
            return Err(e(
                "ApplicationNameRequiredException",
                "The minimum number of required application inputs was not satisfied.",
            ));
        }
        // Validate the `includeOnlyStatuses` enum members up front.
        let status_filter: Vec<String> = str_list(&b, "includeOnlyStatuses");
        for s in &status_filter {
            if !validate::is_enum(validate::DEPLOYMENT_STATUS, s) {
                return Err(e(
                    "InvalidDeploymentStatusException",
                    format!("The deployment status is invalid: {s}"),
                ));
            }
        }
        // Validate the `createTimeRange` bounds (start/end epoch seconds).
        let time_range = b.get("createTimeRange").filter(|v| !v.is_null());
        let range_start = time_range
            .and_then(|r| r.get("start"))
            .and_then(Value::as_f64);
        let range_end = time_range
            .and_then(|r| r.get("end"))
            .and_then(Value::as_f64);
        if let (Some(start), Some(end)) = (range_start, range_end) {
            if start > end {
                return Err(e(
                    "InvalidTimeRangeException",
                    "The specified time range is invalid: the start is after the end.",
                ));
            }
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let app_filter = str_field(&b, "applicationName");
        if let Some(app) = &app_filter {
            if !st.applications.contains_key(app) {
                return Err(e(
                    "ApplicationDoesNotExistException",
                    format!("No application found for name: {app}"),
                ));
            }
        }
        let dg_filter = str_field(&b, "deploymentGroupName");
        let ids: Vec<Value> = st
            .deployment_order
            .iter()
            .rev()
            .filter(|id| {
                let info = match st.deployments.get(*id) {
                    Some(i) => i,
                    None => return false,
                };
                if let Some(app) = &app_filter {
                    if info.get("applicationName").and_then(Value::as_str) != Some(app.as_str()) {
                        return false;
                    }
                }
                if let Some(dg) = &dg_filter {
                    if info.get("deploymentGroupName").and_then(Value::as_str) != Some(dg.as_str())
                    {
                        return false;
                    }
                }
                if !status_filter.is_empty() {
                    let status = info
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("Created");
                    if !status_filter.iter().any(|s| s == status) {
                        return false;
                    }
                }
                if range_start.is_some() || range_end.is_some() {
                    let created = info
                        .get("createTime")
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0);
                    if let Some(start) = range_start {
                        if created < start {
                            return false;
                        }
                    }
                    if let Some(end) = range_end {
                        if created > end {
                            return false;
                        }
                    }
                }
                true
            })
            .map(|id| json!(id))
            .collect();
        ok(paginate_body(ids, "deployments", &b)?)
    }

    fn stop_deployment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = req_deployment_id(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(info) = st.deployments.get_mut(&id) else {
            return Err(e(
                "DeploymentDoesNotExistException",
                format!("The deployment {id} could not be found."),
            ));
        };
        let status = info
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("Created");
        if matches!(status, "Succeeded" | "Failed" | "Stopped") {
            return Err(e(
                "DeploymentAlreadyCompletedException",
                "The specified deployment has already completed.",
            ));
        }
        if let Some(obj) = info.as_object_mut() {
            obj.insert("status".into(), json!("Stopped"));
            obj.insert("completeTime".into(), ts(Utc::now()));
        }
        ok(json!({ "status": "Succeeded", "statusMessage": "Deployment was stopped." }))
    }

    fn continue_deployment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        opt_enum(
            &b,
            "deploymentWaitType",
            validate::DEPLOYMENT_WAIT_TYPE,
            "InvalidDeploymentWaitTypeException",
            "The wait type is invalid.",
        )?;
        if let Some(id) = str_field(&b, "deploymentId") {
            let (_region, account) = self.region_account(req);
            let mut guard = self.state.write();
            let st = guard.get_or_create(&account);
            if !st.deployments.contains_key(&id) {
                return Err(e(
                    "DeploymentDoesNotExistException",
                    format!("The deployment {id} could not be found."),
                ));
            }
        }
        ok(json!({}))
    }

    fn skip_wait_time(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        if let Some(id) = str_field(&b, "deploymentId") {
            let (_region, account) = self.region_account(req);
            let mut guard = self.state.write();
            let st = guard.get_or_create(&account);
            if !st.deployments.contains_key(&id) {
                return Err(e(
                    "DeploymentDoesNotExistException",
                    format!("The deployment {id} could not be found."),
                ));
            }
        }
        ok(json!({}))
    }
}

// ---------------------------------------------------------------------------
// Deployment targets + instances (no real deploy engine -> empty target sets)
// ---------------------------------------------------------------------------

impl CodeDeployService {
    fn require_deployment(st: &CodeDeployState, id: &str) -> Result<(), AwsServiceError> {
        if st.deployments.contains_key(id) {
            Ok(())
        } else {
            Err(e(
                "DeploymentDoesNotExistException",
                format!("The deployment {id} could not be found."),
            ))
        }
    }

    fn get_deployment_target(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = req_deployment_id(&b)?;
        let Some(target_id) = str_field(&b, "targetId").filter(|s| !s.is_empty()) else {
            return Err(e(
                "DeploymentTargetIdRequiredException",
                "A deployment target ID must be specified.",
            ));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        Self::require_deployment(st, &id)?;
        // No real instances participate in fakecloud deployments, so no target
        // resolves.
        Err(e(
            "DeploymentTargetDoesNotExistException",
            format!("The deployment target {target_id} could not be found."),
        ))
    }

    fn list_deployment_targets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = req_deployment_id(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        Self::require_deployment(st, &id)?;
        ok(paginate_body(vec![], "targetIds", &b)?)
    }

    fn batch_get_deployment_targets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = req_deployment_id(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        Self::require_deployment(st, &id)?;
        ok(json!({ "deploymentTargets": [] }))
    }

    fn get_deployment_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = req_deployment_id(&b)?;
        let Some(instance_id) = str_field(&b, "instanceId").filter(|s| !s.is_empty()) else {
            return Err(e(
                "InstanceIdRequiredException",
                "The instance ID was not specified.",
            ));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        Self::require_deployment(st, &id)?;
        Err(e(
            "InstanceDoesNotExistException",
            format!("The instance {instance_id} could not be found."),
        ))
    }

    fn list_deployment_instances(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = req_deployment_id(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        Self::require_deployment(st, &id)?;
        ok(paginate_body(vec![], "instancesList", &b)?)
    }

    fn batch_get_deployment_instances(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = req_deployment_id(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        Self::require_deployment(st, &id)?;
        ok(json!({ "instancesSummary": [] }))
    }
}

// ---------------------------------------------------------------------------
// On-premises instances
// ---------------------------------------------------------------------------

impl CodeDeployService {
    fn register_on_premises_instance(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_instance_name(&b)?;
        let iam_user = str_field(&b, "iamUserArn");
        let iam_session = str_field(&b, "iamSessionArn");
        match (&iam_user, &iam_session) {
            (None, None) => return Err(e("IamArnRequiredException", "No IAM ARN was specified.")),
            (Some(_), Some(_)) => {
                return Err(e(
                    "MultipleIamArnsProvidedException",
                    "Both an IAM user ARN and an IAM session ARN were specified.",
                ))
            }
            _ => {}
        }
        let (region, account) = self.region_account(req);
        let now = Utc::now();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Only a currently-registered instance (one without a `deregisterTime`)
        // blocks re-registration; a previously deregistered name may be
        // re-registered, matching AWS.
        let already_registered = st
            .on_premises_instances
            .get(&name)
            .map(|i| i.get("deregisterTime").is_none())
            .unwrap_or(false);
        if already_registered {
            return Err(e(
                "InstanceNameAlreadyRegisteredException",
                format!("The specified on-premises instance name is already registered: {name}"),
            ));
        }
        let instance_arn = format!("arn:aws:codedeploy:{region}:{account}:instance/{name}");
        let mut info = Map::new();
        info.insert("instanceName".into(), json!(name));
        info.insert("instanceArn".into(), json!(instance_arn));
        info.insert("registerTime".into(), ts(now));
        info.insert("tags".into(), json!([]));
        if let Some(u) = iam_user {
            info.insert("iamUserArn".into(), json!(u));
        }
        if let Some(s) = iam_session {
            info.insert("iamSessionArn".into(), json!(s));
        }
        let is_new = !st.on_premises_instances.contains_key(&name);
        st.on_premises_instances
            .insert(name.clone(), Value::Object(info));
        if is_new {
            st.on_premises_order.push(name);
        }
        ok(json!({}))
    }

    fn deregister_on_premises_instance(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_instance_name(&b)?;
        let (_region, account) = self.region_account(req);
        let now = Utc::now();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Deregistration is idempotent (the Smithy contract declares no
        // "not registered" error). A registered instance is retained with a
        // `deregisterTime` stamp so it can still be listed by registration
        // status and re-registered later, matching AWS.
        if let Some(info) = st.on_premises_instances.get_mut(&name) {
            if let Some(obj) = info.as_object_mut() {
                obj.insert("deregisterTime".into(), ts(now));
            }
        }
        ok(json!({}))
    }

    fn get_on_premises_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_instance_name(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(info) = st.on_premises_instances.get(&name) else {
            return Err(e(
                "InstanceNotRegisteredException",
                format!("The on-premises instance is not registered: {name}"),
            ));
        };
        ok(json!({ "instanceInfo": info.clone() }))
    }

    fn list_on_premises_instances(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        opt_enum(
            &b,
            "registrationStatus",
            validate::REGISTRATION_STATUS,
            "InvalidRegistrationStatusException",
            "The registration status was specified in an invalid format.",
        )?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // A `deregisterTime` marks a deregistered instance; filter on it when a
        // `registrationStatus` is supplied.
        let status_filter = str_field(&b, "registrationStatus");
        let names: Vec<Value> = st
            .on_premises_order
            .iter()
            .filter(|n| {
                let deregistered = st
                    .on_premises_instances
                    .get(*n)
                    .map(|i| i.get("deregisterTime").is_some())
                    .unwrap_or(false);
                match status_filter.as_deref() {
                    Some("Registered") => !deregistered,
                    Some("Deregistered") => deregistered,
                    _ => true,
                }
            })
            .map(|n| json!(n))
            .collect();
        ok(paginate_body(names, "instanceNames", &b)?)
    }

    fn batch_get_on_premises_instances(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let names = str_list(&b, "instanceNames");
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let infos: Vec<Value> = names
            .iter()
            .filter_map(|n| st.on_premises_instances.get(n).cloned())
            .collect();
        ok(json!({ "instanceInfos": infos }))
    }

    fn add_tags_to_on_premises(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let tags = b
            .get("tags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if tags.is_empty() {
            return Err(e("TagRequiredException", "A tag was not specified."));
        }
        let names = str_list(&b, "instanceNames");
        if names.is_empty() {
            return Err(e(
                "InstanceNameRequiredException",
                "An on-premises instance name was not specified.",
            ));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Validate every instance is registered *before* mutating any, so a
        // later unregistered name cannot leave earlier instances partially
        // tagged (handle() only snapshots on Ok).
        Self::require_all_registered(st, &names)?;
        for name in &names {
            if let Some(existing) = st
                .on_premises_instances
                .get_mut(name)
                .and_then(|i| i.get_mut("tags"))
                .and_then(Value::as_array_mut)
            {
                for t in &tags {
                    if !existing.contains(t) {
                        existing.push(t.clone());
                    }
                }
            }
        }
        ok(json!({}))
    }

    fn remove_tags_from_on_premises(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let tags = b
            .get("tags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if tags.is_empty() {
            return Err(e("TagRequiredException", "A tag was not specified."));
        }
        let names = str_list(&b, "instanceNames");
        if names.is_empty() {
            return Err(e(
                "InstanceNameRequiredException",
                "An on-premises instance name was not specified.",
            ));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Validate-all-then-apply, so a later unregistered name cannot leave
        // earlier instances partially untagged.
        Self::require_all_registered(st, &names)?;
        for name in &names {
            if let Some(existing) = st
                .on_premises_instances
                .get_mut(name)
                .and_then(|i| i.get_mut("tags"))
                .and_then(Value::as_array_mut)
            {
                existing.retain(|t| !tags.contains(t));
            }
        }
        ok(json!({}))
    }

    /// Ensure every named on-premises instance is currently registered (exists
    /// and has no `deregisterTime`); otherwise return `InstanceNotRegisteredException`
    /// without having mutated any state.
    fn require_all_registered(
        st: &CodeDeployState,
        names: &[String],
    ) -> Result<(), AwsServiceError> {
        for name in names {
            let registered = st
                .on_premises_instances
                .get(name)
                .map(|i| i.get("deregisterTime").is_none())
                .unwrap_or(false);
            if !registered {
                return Err(e(
                    "InstanceNotRegisteredException",
                    format!("The on-premises instance is not registered: {name}"),
                ));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// GitHub account tokens + lifecycle hook status + external-id cleanup
// ---------------------------------------------------------------------------

impl CodeDeployService {
    fn list_github_account_token_names(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        // No GitHub account tokens are provisioned in fakecloud.
        ok(paginate_body(vec![], "tokenNameList", &b)?)
    }

    fn delete_github_account_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(token_name) = str_field(&b, "tokenName") else {
            return Err(e(
                "GitHubAccountTokenNameRequiredException",
                "The GitHub account token name was not specified.",
            ));
        };
        // No tokens exist, so any named token is unknown.
        Err(e(
            "GitHubAccountTokenDoesNotExistException",
            format!("No GitHub account connection exists for token name: {token_name}"),
        ))
    }

    fn put_lifecycle_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        opt_enum(
            &b,
            "status",
            validate::LIFECYCLE_EVENT_STATUS,
            "InvalidLifecycleEventHookExecutionStatusException",
            "The lifecycle event status was specified in an invalid format.",
        )?;
        if let Some(id) = str_field(&b, "deploymentId") {
            let (_region, account) = self.region_account(req);
            let mut guard = self.state.write();
            let st = guard.get_or_create(&account);
            if !st.deployments.contains_key(&id) {
                return Err(e(
                    "DeploymentDoesNotExistException",
                    format!("The deployment {id} could not be found."),
                ));
            }
        }
        let exec_id = str_field(&b, "lifecycleEventHookExecutionId")
            .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
        ok(json!({ "lifecycleEventHookExecutionId": exec_id }))
    }

    fn delete_resources_by_external_id(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(external_id) = str_field(&b, "externalId") else {
            return ok(json!({}));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // CloudFormation cleanup hook: remove every deployment tagged with this
        // external id (and its settle bookkeeping). A deployment carries an
        // `externalId` only when CloudFormation created it, so this typically
        // matches nothing, but the deletion is real when it does.
        let to_remove: Vec<String> = st
            .deployments
            .iter()
            .filter(|(_, info)| {
                info.get("externalId").and_then(Value::as_str) == Some(external_id.as_str())
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in to_remove {
            st.deployments.remove(&id);
            st.deployment_settle.remove(&id);
            st.deployment_order.retain(|x| x != &id);
        }
        ok(json!({}))
    }
}

// ---------------------------------------------------------------------------
// Resource tagging
// ---------------------------------------------------------------------------

impl CodeDeployService {
    /// Verify the resource referenced by a (syntactically valid) CodeDeploy ARN
    /// exists, returning the declared per-resource-type not-found error when it
    /// does not. The ARN resource segment is `application:<name>`,
    /// `deploymentgroup:<app>/<group>`, or `deploymentconfig:<name>`.
    fn require_resource_exists(st: &CodeDeployState, arn: &str) -> Result<(), AwsServiceError> {
        let parts: Vec<&str> = arn.splitn(7, ':').collect();
        // A malformed ARN with fewer than six colon-separated segments must not
        // index-panic; AWS rejects it as an invalid ARN.
        let kind = parts.get(5).copied().ok_or_else(|| {
            e(
                "InvalidArnException",
                format!("Invalid resource ARN: {arn}"),
            )
        })?;
        let rest = parts.get(6).copied().unwrap_or("");
        let group_exists = |rest: &str| {
            let (app, group) = rest.split_once('/').unwrap_or((rest, ""));
            st.deployment_groups
                .get(app)
                .map(|g| g.contains_key(group))
                .unwrap_or(false)
        };
        match kind {
            "application" if !st.applications.contains_key(rest) => Err(e(
                "ApplicationDoesNotExistException",
                format!("No application found for name: {rest}"),
            )),
            "deploymentgroup" if !group_exists(rest) => Err(e(
                "DeploymentGroupDoesNotExistException",
                format!("No deployment group found for name: {rest}"),
            )),
            "deploymentconfig"
                if predefined_config(rest).is_none()
                    && !st.deployment_configs.contains_key(rest) =>
            {
                Err(e(
                    "DeploymentConfigDoesNotExistException",
                    format!("No deployment configuration found for name: {rest}"),
                ))
            }
            _ => Ok(()),
        }
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn = req_resource_arn(&b)?;
        let tags = b
            .get("Tags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if tags.is_empty() {
            return Err(e("TagRequiredException", "A tag was not specified."));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        Self::require_resource_exists(st, &arn)?;
        let stored = st.tags.entry(arn).or_default();
        for t in tags {
            let key = t.get("Key").and_then(Value::as_str).map(str::to_string);
            if let Some(k) = key {
                stored.retain(|x| x.get("Key").and_then(Value::as_str) != Some(k.as_str()));
            }
            stored.push(t);
        }
        ok(json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn = req_resource_arn(&b)?;
        let keys = str_list(&b, "TagKeys");
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        Self::require_resource_exists(st, &arn)?;
        if let Some(stored) = st.tags.get_mut(&arn) {
            stored.retain(|t| {
                t.get("Key")
                    .and_then(Value::as_str)
                    .map(|k| !keys.iter().any(|rk| rk == k))
                    .unwrap_or(true)
            });
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn = req_resource_arn(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let tags = st.tags.get(&arn).cloned().unwrap_or_default();
        let out = paginate_body(tags, "Tags", &b)?;
        ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> CodeDeployService {
        let state = Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        )));
        CodeDeployService::new(state)
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "codedeploy".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_of(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn err_code(r: Result<AwsResponse, AwsServiceError>) -> String {
        match r {
            Ok(_) => panic!("expected an error"),
            Err(e) => e.code().to_string(),
        }
    }

    #[test]
    fn create_get_application_round_trip() {
        let s = svc();
        let created = body_of(
            s.create_application(&req(
                "CreateApplication",
                json!({ "applicationName": "app1", "computePlatform": "Lambda" }),
            ))
            .unwrap(),
        );
        assert!(created["applicationId"].is_string());
        let got = body_of(
            s.get_application(&req("GetApplication", json!({ "applicationName": "app1" })))
                .unwrap(),
        );
        assert_eq!(got["application"]["applicationName"], "app1");
        assert_eq!(got["application"]["computePlatform"], "Lambda");
    }

    #[test]
    fn create_application_rejects_duplicate_and_bad_platform() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "dup" }),
        ))
        .unwrap();
        assert_eq!(
            err_code(s.create_application(&req(
                "CreateApplication",
                json!({ "applicationName": "dup" })
            ))),
            "ApplicationAlreadyExistsException"
        );
        assert_eq!(
            err_code(s.create_application(&req(
                "CreateApplication",
                json!({ "applicationName": "x", "computePlatform": "NOPE" })
            ))),
            "InvalidComputePlatformException"
        );
    }

    #[test]
    fn application_name_required_and_invalid() {
        let s = svc();
        assert_eq!(
            err_code(s.get_application(&req("GetApplication", json!({})))),
            "ApplicationNameRequiredException"
        );
        let long = "a".repeat(101);
        assert_eq!(
            err_code(s.get_application(&req("GetApplication", json!({ "applicationName": long })))),
            "InvalidApplicationNameException"
        );
        assert_eq!(
            err_code(s.get_application(&req(
                "GetApplication",
                json!({ "applicationName": "ghost" })
            ))),
            "ApplicationDoesNotExistException"
        );
    }

    #[test]
    fn predefined_deployment_config_always_present() {
        let s = svc();
        let got = body_of(
            s.get_deployment_config(&req(
                "GetDeploymentConfig",
                json!({ "deploymentConfigName": "CodeDeployDefault.OneAtATime" }),
            ))
            .unwrap(),
        );
        assert_eq!(got["deploymentConfigInfo"]["computePlatform"], "Server");
        assert_eq!(
            got["deploymentConfigInfo"]["minimumHealthyHosts"]["type"],
            "HOST_COUNT"
        );
        let canary = body_of(s.get_deployment_config(&req("GetDeploymentConfig", json!({ "deploymentConfigName": "CodeDeployDefault.LambdaCanary10Percent5Minutes" }))).unwrap());
        assert_eq!(
            canary["deploymentConfigInfo"]["trafficRoutingConfig"]["type"],
            "TimeBasedCanary"
        );
    }

    #[test]
    fn deployment_settles_through_lifecycle() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "da" }),
        ))
        .unwrap();
        s.create_deployment_group(&req(
            "CreateDeploymentGroup",
            json!({ "applicationName": "da", "deploymentGroupName": "dg", "serviceRoleArn": "arn:aws:iam::000000000000:role/cd" }),
        ))
        .unwrap();
        let id = body_of(
            s.create_deployment(&req(
                "CreateDeployment",
                json!({ "applicationName": "da", "deploymentGroupName": "dg" }),
            ))
            .unwrap(),
        )["deploymentId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(id.starts_with("d-"));
        let first = body_of(
            s.get_deployment(&req("GetDeployment", json!({ "deploymentId": id.clone() })))
                .unwrap(),
        );
        assert_eq!(first["deploymentInfo"]["status"], "InProgress");
        let second = body_of(
            s.get_deployment(&req("GetDeployment", json!({ "deploymentId": id })))
                .unwrap(),
        );
        assert_eq!(second["deploymentInfo"]["status"], "Succeeded");
    }

    #[test]
    fn create_deployment_group_requires_app_and_role() {
        let s = svc();
        assert_eq!(
            err_code(s.create_deployment_group(&req(
                "CreateDeploymentGroup",
                json!({ "applicationName": "ga", "deploymentGroupName": "g1" })
            ))),
            "RoleRequiredException"
        );
        assert_eq!(
            err_code(s.create_deployment_group(&req("CreateDeploymentGroup", json!({ "applicationName": "ga", "deploymentGroupName": "g1", "serviceRoleArn": "arn:aws:iam::000000000000:role/cd" })))),
            "ApplicationDoesNotExistException"
        );
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "ga" }),
        ))
        .unwrap();
        let created = body_of(
            s.create_deployment_group(&req("CreateDeploymentGroup", json!({ "applicationName": "ga", "deploymentGroupName": "g1", "serviceRoleArn": "arn:aws:iam::000000000000:role/cd" })))
                .unwrap(),
        );
        assert!(created["deploymentGroupId"].is_string());
        let got = body_of(
            s.get_deployment_group(&req(
                "GetDeploymentGroup",
                json!({ "applicationName": "ga", "deploymentGroupName": "g1" }),
            ))
            .unwrap(),
        );
        assert_eq!(
            got["deploymentGroupInfo"]["deploymentConfigName"],
            "CodeDeployDefault.OneAtATime"
        );
    }

    #[test]
    fn on_premises_register_requires_iam_arn() {
        let s = svc();
        assert_eq!(
            err_code(s.register_on_premises_instance(&req(
                "RegisterOnPremisesInstance",
                json!({ "instanceName": "i1" })
            ))),
            "IamArnRequiredException"
        );
        s.register_on_premises_instance(&req(
            "RegisterOnPremisesInstance",
            json!({ "instanceName": "i1", "iamUserArn": "arn:aws:iam::000000000000:user/u" }),
        ))
        .unwrap();
        let got = body_of(
            s.get_on_premises_instance(&req(
                "GetOnPremisesInstance",
                json!({ "instanceName": "i1" }),
            ))
            .unwrap(),
        );
        assert_eq!(got["instanceInfo"]["instanceName"], "i1");
    }

    #[test]
    fn tag_resource_rejects_non_arn() {
        let s = svc();
        assert_eq!(
            err_code(s.list_tags_for_resource(&req(
                "ListTagsForResource",
                json!({ "ResourceArn": "not-an-arn" })
            ))),
            "InvalidArnException"
        );
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "app1" }),
        ))
        .unwrap();
        let arn = "arn:aws:codedeploy:us-east-1:000000000000:application:app1";
        s.tag_resource(&req(
            "TagResource",
            json!({ "ResourceArn": arn, "Tags": [{ "Key": "env", "Value": "prod" }] }),
        ))
        .unwrap();
        let got = body_of(
            s.list_tags_for_resource(&req("ListTagsForResource", json!({ "ResourceArn": arn })))
                .unwrap(),
        );
        assert_eq!(got["Tags"][0]["Key"], "env");
    }

    #[test]
    fn require_resource_exists_rejects_short_arn() {
        // Regression: a resource ARN with fewer than six colon segments must
        // return InvalidArnException rather than index-panicking on parts[5].
        let s = svc();
        let mut guard = s.state.write();
        let st = guard.get_or_create("000000000000");
        let r = CodeDeployService::require_resource_exists(st, "arn:aws:codedeploy:us-east-1:0");
        assert_eq!(r.unwrap_err().code(), "InvalidArnException");
    }

    // Finding 7: TagResource / UntagResource reject a syntactically valid ARN
    // whose resource does not exist (ListTagsForResource stays lenient: its
    // Smithy contract declares no not-found error).
    #[test]
    fn tag_resource_rejects_nonexistent_resource() {
        let s = svc();
        let ghost = "arn:aws:codedeploy:us-east-1:000000000000:application:ghost";
        assert_eq!(
            err_code(s.tag_resource(&req(
                "TagResource",
                json!({ "ResourceArn": ghost, "Tags": [{ "Key": "k", "Value": "v" }] })
            ))),
            "ApplicationDoesNotExistException"
        );
        let ghost_cfg = "arn:aws:codedeploy:us-east-1:000000000000:deploymentconfig:ghostcfg";
        assert_eq!(
            err_code(s.untag_resource(&req(
                "UntagResource",
                json!({ "ResourceArn": ghost_cfg, "TagKeys": ["k"] })
            ))),
            "DeploymentConfigDoesNotExistException"
        );
        // A predefined deployment config ARN is a real resource.
        let predef =
            "arn:aws:codedeploy:us-east-1:000000000000:deploymentconfig:CodeDeployDefault.OneAtATime";
        s.tag_resource(&req(
            "TagResource",
            json!({ "ResourceArn": predef, "Tags": [{ "Key": "k", "Value": "v" }] }),
        ))
        .unwrap();
    }

    // Finding 1: renaming an application rewrites the embedded applicationName
    // in every stored DeploymentGroupInfo.
    #[test]
    fn rename_application_rewrites_group_application_name() {
        let s = svc();
        s.create_application(&req("CreateApplication", json!({ "applicationName": "a" })))
            .unwrap();
        s.create_deployment_group(&req(
            "CreateDeploymentGroup",
            json!({
                "applicationName": "a",
                "deploymentGroupName": "g",
                "serviceRoleArn": "arn:aws:iam::000000000000:role/cd"
            }),
        ))
        .unwrap();
        s.update_application(&req(
            "UpdateApplication",
            json!({ "applicationName": "a", "newApplicationName": "b" }),
        ))
        .unwrap();
        let got = body_of(
            s.get_deployment_group(&req(
                "GetDeploymentGroup",
                json!({ "applicationName": "b", "deploymentGroupName": "g" }),
            ))
            .unwrap(),
        );
        assert_eq!(got["deploymentGroupInfo"]["applicationName"], "b");
    }

    // Completeness A: renaming an application also rewrites the applicationName
    // on every stored deployment, so ListDeployments(new) finds pre-rename
    // deployments and GetDeployment reports the new name.
    #[test]
    fn rename_application_rewrites_deployment_application_name() {
        let s = svc();
        s.create_application(&req("CreateApplication", json!({ "applicationName": "a" })))
            .unwrap();
        s.create_deployment_group(&req(
            "CreateDeploymentGroup",
            json!({ "applicationName": "a", "deploymentGroupName": "dg", "serviceRoleArn": "arn:aws:iam::000000000000:role/cd" }),
        ))
        .unwrap();
        let id = body_of(
            s.create_deployment(&req(
                "CreateDeployment",
                json!({ "applicationName": "a", "deploymentGroupName": "dg" }),
            ))
            .unwrap(),
        )["deploymentId"]
            .as_str()
            .unwrap()
            .to_string();
        s.update_application(&req(
            "UpdateApplication",
            json!({ "applicationName": "a", "newApplicationName": "b" }),
        ))
        .unwrap();
        // ListDeployments filtered by the new name finds the pre-rename deployment.
        let listed = body_of(
            s.list_deployments(&req("ListDeployments", json!({ "applicationName": "b" })))
                .unwrap(),
        );
        let ids: Vec<&str> = listed["deployments"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(ids, vec![id.as_str()]);
        // GetDeployment reports the renamed application.
        let got = body_of(
            s.get_deployment(&req("GetDeployment", json!({ "deploymentId": id })))
                .unwrap(),
        );
        assert_eq!(got["deploymentInfo"]["applicationName"], "b");
    }

    // Completeness B: ListApplicationRevisions rejects bogus deployed / sortBy /
    // sortOrder enum values with the declared errors.
    #[test]
    fn list_application_revisions_validates_enums() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "ra" }),
        ))
        .unwrap();
        let cases = [
            ("deployed", "bogus", "InvalidDeployedStateFilterException"),
            ("sortBy", "bogus", "InvalidSortByException"),
            ("sortOrder", "bogus", "InvalidSortOrderException"),
        ];
        for (field, value, code) in cases {
            assert_eq!(
                err_code(s.list_application_revisions(&req(
                    "ListApplicationRevisions",
                    json!({ "applicationName": "ra", field: value }),
                ))),
                code,
                "field {field}",
            );
        }
    }

    // Finding 5: BatchGetApplications raises the declared not-found for a
    // missing name rather than silently omitting it.
    #[test]
    fn batch_get_applications_raises_not_found() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "real" }),
        ))
        .unwrap();
        assert_eq!(
            err_code(s.batch_get_applications(&req(
                "BatchGetApplications",
                json!({ "applicationNames": ["real", "ghost"] })
            ))),
            "ApplicationDoesNotExistException"
        );
        let got = body_of(
            s.batch_get_applications(&req(
                "BatchGetApplications",
                json!({ "applicationNames": ["real"] }),
            ))
            .unwrap(),
        );
        assert_eq!(got["applicationsInfo"][0]["applicationName"], "real");
    }

    // Finding 2: ListDeployments honours includeOnlyStatuses + createTimeRange
    // and validates the status enum and time range.
    #[test]
    fn list_deployments_filters_by_status_and_time() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "app" }),
        ))
        .unwrap();
        s.create_deployment_group(&req(
            "CreateDeploymentGroup",
            json!({ "applicationName": "app", "deploymentGroupName": "dg", "serviceRoleArn": "arn:aws:iam::000000000000:role/cd" }),
        ))
        .unwrap();
        let mk = || {
            body_of(
                s.create_deployment(&req(
                    "CreateDeployment",
                    json!({ "applicationName": "app", "deploymentGroupName": "dg" }),
                ))
                .unwrap(),
            )["deploymentId"]
                .as_str()
                .unwrap()
                .to_string()
        };
        let created = mk(); // stays "Created"
        let done = mk();
        // Settle `done` to Succeeded via two reads.
        s.get_deployment(&req(
            "GetDeployment",
            json!({ "deploymentId": done.clone() }),
        ))
        .unwrap();
        s.get_deployment(&req(
            "GetDeployment",
            json!({ "deploymentId": done.clone() }),
        ))
        .unwrap();
        let succeeded = body_of(
            s.list_deployments(&req(
                "ListDeployments",
                json!({ "includeOnlyStatuses": ["Succeeded"] }),
            ))
            .unwrap(),
        );
        let ids: Vec<&str> = succeeded["deployments"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(ids, vec![done.as_str()]);
        assert!(!ids.contains(&created.as_str()));
        // Bad status enum + inverted time range are rejected.
        assert_eq!(
            err_code(s.list_deployments(&req(
                "ListDeployments",
                json!({ "includeOnlyStatuses": ["Bogus"] })
            ))),
            "InvalidDeploymentStatusException"
        );
        assert_eq!(
            err_code(s.list_deployments(&req(
                "ListDeployments",
                json!({ "createTimeRange": { "start": 100.0, "end": 1.0 } })
            ))),
            "InvalidTimeRangeException"
        );
        // A time window that excludes everything (far future) returns nothing.
        let none = body_of(
            s.list_deployments(&req(
                "ListDeployments",
                json!({ "createTimeRange": { "start": 9_000_000_000.0_f64 } }),
            ))
            .unwrap(),
        );
        assert_eq!(none["deployments"].as_array().unwrap().len(), 0);
    }

    // Finding 3: ListApplicationRevisions applies the `deployed` filter and
    // sort order.
    #[test]
    fn list_application_revisions_filters_and_sorts() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "ra" }),
        ))
        .unwrap();
        let reg = |bucket: &str| {
            s.register_application_revision(&req(
                "RegisterApplicationRevision",
                json!({
                    "applicationName": "ra",
                    "revision": { "revisionType": "S3", "s3Location": { "bucket": bucket, "key": "k", "bundleType": "zip" } }
                }),
            ))
            .unwrap();
        };
        reg("bucket-a");
        reg("bucket-b");
        // No revision is deployed, so `include` -> empty, `exclude` -> all.
        let inc = body_of(
            s.list_application_revisions(&req(
                "ListApplicationRevisions",
                json!({ "applicationName": "ra", "deployed": "include" }),
            ))
            .unwrap(),
        );
        assert_eq!(inc["revisions"].as_array().unwrap().len(), 0);
        let exc = body_of(
            s.list_application_revisions(&req(
                "ListApplicationRevisions",
                json!({ "applicationName": "ra", "deployed": "exclude" }),
            ))
            .unwrap(),
        );
        assert_eq!(exc["revisions"].as_array().unwrap().len(), 2);
        // Descending register-time order reverses insertion order.
        let desc = body_of(
            s.list_application_revisions(&req(
                "ListApplicationRevisions",
                json!({ "applicationName": "ra", "sortBy": "registerTime", "sortOrder": "descending" }),
            ))
            .unwrap(),
        );
        assert_eq!(
            desc["revisions"][0]["s3Location"]["bucket"], "bucket-b",
            "descending should list the most-recently-registered first"
        );
    }

    // Finding 4: ListOnPremisesInstances honours registrationStatus, and a
    // deregistered instance is retained as Deregistered.
    #[test]
    fn list_on_premises_filters_by_registration_status() {
        let s = svc();
        for n in ["i1", "i2"] {
            s.register_on_premises_instance(&req(
                "RegisterOnPremisesInstance",
                json!({ "instanceName": n, "iamUserArn": "arn:aws:iam::000000000000:user/u" }),
            ))
            .unwrap();
        }
        s.deregister_on_premises_instance(&req(
            "DeregisterOnPremisesInstance",
            json!({ "instanceName": "i2" }),
        ))
        .unwrap();
        let names = |status: &str| -> Vec<String> {
            body_of(
                s.list_on_premises_instances(&req(
                    "ListOnPremisesInstances",
                    json!({ "registrationStatus": status }),
                ))
                .unwrap(),
            )["instanceNames"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(names("Registered"), vec!["i1".to_string()]);
        assert_eq!(names("Deregistered"), vec!["i2".to_string()]);
    }

    // Finding 6: AddTagsToOnPremisesInstances validates all names before
    // mutating any (no partial tagging on error).
    #[test]
    fn add_tags_on_premises_is_all_or_nothing() {
        let s = svc();
        s.register_on_premises_instance(&req(
            "RegisterOnPremisesInstance",
            json!({ "instanceName": "ok1", "iamUserArn": "arn:aws:iam::000000000000:user/u" }),
        ))
        .unwrap();
        assert_eq!(
            err_code(s.add_tags_to_on_premises(&req(
                "AddTagsToOnPremisesInstances",
                json!({ "tags": [{ "Key": "k", "Value": "v" }], "instanceNames": ["ok1", "missing"] })
            ))),
            "InstanceNotRegisteredException"
        );
        // ok1 must not have been tagged.
        let got = body_of(
            s.get_on_premises_instance(&req(
                "GetOnPremisesInstance",
                json!({ "instanceName": "ok1" }),
            ))
            .unwrap(),
        );
        assert_eq!(got["instanceInfo"]["tags"].as_array().unwrap().len(), 0);
    }

    // Finding 8: the true no-op actions are not in the mutating set, while
    // DeleteResourcesByExternalId performs (and persists) a real deletion.
    #[test]
    fn is_mutating_excludes_true_no_ops() {
        assert!(!is_mutating("ContinueDeployment"));
        assert!(!is_mutating("SkipWaitTimeForInstanceTermination"));
        assert!(!is_mutating("PutLifecycleEventHookExecutionStatus"));
        assert!(is_mutating("DeleteResourcesByExternalId"));
    }

    #[test]
    fn delete_resources_by_external_id_removes_matching_deployments() {
        let s = svc();
        {
            let mut g = s.state.write();
            let st = g.get_or_create("000000000000");
            st.deployments.insert(
                "d-EXT000001".into(),
                json!({ "deploymentId": "d-EXT000001", "externalId": "ext-1" }),
            );
            st.deployment_order.push("d-EXT000001".into());
            st.deployment_settle.insert("d-EXT000001".into(), 0);
        }
        s.delete_resources_by_external_id(&req(
            "DeleteResourcesByExternalId",
            json!({ "externalId": "ext-1" }),
        ))
        .unwrap();
        let mut g = s.state.write();
        let st = g.get_or_create("000000000000");
        assert!(!st.deployments.contains_key("d-EXT000001"));
        assert!(st.deployment_order.is_empty());
    }

    #[test]
    fn delete_application_is_idempotent() {
        let s = svc();
        s.delete_application(&req(
            "DeleteApplication",
            json!({ "applicationName": "ghost" }),
        ))
        .unwrap();
    }

    // Helper: create an application plus a deployment group under it.
    fn make_app_and_group(s: &CodeDeployService, app: &str, group: &str) {
        s.create_application(&req("CreateApplication", json!({ "applicationName": app })))
            .unwrap();
        s.create_deployment_group(&req(
            "CreateDeploymentGroup",
            json!({ "applicationName": app, "deploymentGroupName": group, "serviceRoleArn": "arn:aws:iam::000000000000:role/cd" }),
        ))
        .unwrap();
    }

    fn tags_for(s: &CodeDeployService, arn: &str) -> Vec<Value> {
        body_of(
            s.list_tags_for_resource(&req("ListTagsForResource", json!({ "ResourceArn": arn })))
                .unwrap(),
        )["Tags"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }

    // T1.1: CreateApplication persists its create-time `tags` so
    // ListTagsForResource echoes them.
    #[test]
    fn create_application_persists_tags() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "tagged", "tags": [{ "Key": "env", "Value": "prod" }] }),
        ))
        .unwrap();
        let arn = "arn:aws:codedeploy:us-east-1:000000000000:application:tagged";
        let tags = tags_for(&s, arn);
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "env");
        assert_eq!(tags[0]["Value"], "prod");
    }

    // T1.2: CreateDeploymentGroup persists its create-time `tags`.
    #[test]
    fn create_deployment_group_persists_tags() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "app" }),
        ))
        .unwrap();
        s.create_deployment_group(&req(
            "CreateDeploymentGroup",
            json!({
                "applicationName": "app",
                "deploymentGroupName": "dg",
                "serviceRoleArn": "arn:aws:iam::000000000000:role/cd",
                "tags": [{ "Key": "team", "Value": "core" }]
            }),
        ))
        .unwrap();
        let arn = "arn:aws:codedeploy:us-east-1:000000000000:deploymentgroup:app/dg";
        let tags = tags_for(&s, arn);
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "team");
    }

    // T1.3: CreateDeploymentConfig validates the minimumHealthyHosts type and
    // value range with the declared error.
    #[test]
    fn create_deployment_config_validates_minimum_healthy_hosts() {
        let s = svc();
        // Bad type.
        assert_eq!(
            err_code(s.create_deployment_config(&req(
                "CreateDeploymentConfig",
                json!({ "deploymentConfigName": "c1", "minimumHealthyHosts": { "type": "BOGUS", "value": 1 } })
            ))),
            "InvalidMinimumHealthyHostValueException"
        );
        // FLEET_PERCENT out of range.
        assert_eq!(
            err_code(s.create_deployment_config(&req(
                "CreateDeploymentConfig",
                json!({ "deploymentConfigName": "c2", "minimumHealthyHosts": { "type": "FLEET_PERCENT", "value": 150 } })
            ))),
            "InvalidMinimumHealthyHostValueException"
        );
        // HOST_COUNT negative.
        assert_eq!(
            err_code(s.create_deployment_config(&req(
                "CreateDeploymentConfig",
                json!({ "deploymentConfigName": "c3", "minimumHealthyHosts": { "type": "HOST_COUNT", "value": -1 } })
            ))),
            "InvalidMinimumHealthyHostValueException"
        );
        // A valid combination is accepted.
        s.create_deployment_config(&req(
            "CreateDeploymentConfig",
            json!({ "deploymentConfigName": "ok", "minimumHealthyHosts": { "type": "FLEET_PERCENT", "value": 50 } }),
        ))
        .unwrap();
    }

    // T1.4: CreateDeploymentGroup and UpdateDeploymentGroup reject malformed
    // nested enums with the declared errors.
    #[test]
    fn deployment_group_validates_nested_enums() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "app" }),
        ))
        .unwrap();
        let role = "arn:aws:iam::000000000000:role/cd";
        let base = |extra: Value| -> Value {
            let mut m = json!({
                "applicationName": "app",
                "deploymentGroupName": "dg",
                "serviceRoleArn": role,
            });
            for (k, v) in extra.as_object().unwrap() {
                m.as_object_mut().unwrap().insert(k.clone(), v.clone());
            }
            m
        };
        let cases = [
            (
                json!({ "ec2TagFilters": [{ "Key": "k", "Value": "v", "Type": "BOGUS" }] }),
                "InvalidEC2TagException",
            ),
            (
                json!({ "onPremisesInstanceTagFilters": [{ "Key": "k", "Value": "v", "Type": "BOGUS" }] }),
                "InvalidOnPremisesTagCombinationException",
            ),
            (
                json!({ "triggerConfigurations": [{ "triggerName": "t", "triggerTargetArn": "arn:aws:sns:us-east-1:000000000000:x", "triggerEvents": ["BogusEvent"] }] }),
                "InvalidTriggerConfigException",
            ),
            (
                json!({ "autoRollbackConfiguration": { "enabled": true, "events": ["NOT_AN_EVENT"] } }),
                "InvalidAutoRollbackConfigException",
            ),
        ];
        for (extra, code) in &cases {
            assert_eq!(
                err_code(
                    s.create_deployment_group(&req("CreateDeploymentGroup", base(extra.clone())))
                ),
                *code,
                "create: {extra}"
            );
        }
        // The same validation runs on update. Create a valid group first.
        s.create_deployment_group(&req("CreateDeploymentGroup", base(json!({}))))
            .unwrap();
        assert_eq!(
            err_code(s.update_deployment_group(&req(
                "UpdateDeploymentGroup",
                json!({
                    "applicationName": "app",
                    "currentDeploymentGroupName": "dg",
                    "ec2TagFilters": [{ "Key": "k", "Value": "v", "Type": "BOGUS" }]
                })
            ))),
            "InvalidEC2TagException"
        );
        // Valid nested enums are accepted.
        s.update_deployment_group(&req(
            "UpdateDeploymentGroup",
            json!({
                "applicationName": "app",
                "currentDeploymentGroupName": "dg",
                "autoRollbackConfiguration": { "enabled": true, "events": ["DEPLOYMENT_FAILURE"] },
                "triggerConfigurations": [{ "triggerName": "t", "triggerTargetArn": "arn:aws:sns:us-east-1:000000000000:x", "triggerEvents": ["DeploymentSuccess"] }]
            }),
        ))
        .unwrap();
    }

    // Suspicious: CreateDeployment requires a deployment group name, and the
    // named group must exist.
    #[test]
    fn create_deployment_requires_and_checks_group() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "app" }),
        ))
        .unwrap();
        assert_eq!(
            err_code(s.create_deployment(&req(
                "CreateDeployment",
                json!({ "applicationName": "app" })
            ))),
            "DeploymentGroupNameRequiredException"
        );
        assert_eq!(
            err_code(s.create_deployment(&req(
                "CreateDeployment",
                json!({ "applicationName": "app", "deploymentGroupName": "ghost" })
            ))),
            "DeploymentGroupDoesNotExistException"
        );
    }

    // T2.5: CreateDeployment inherits the target group's deploymentStyle.
    #[test]
    fn create_deployment_inherits_deployment_style() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "app" }),
        ))
        .unwrap();
        s.create_deployment_group(&req(
            "CreateDeploymentGroup",
            json!({
                "applicationName": "app",
                "deploymentGroupName": "dg",
                "serviceRoleArn": "arn:aws:iam::000000000000:role/cd",
                "deploymentStyle": { "deploymentType": "BLUE_GREEN", "deploymentOption": "WITH_TRAFFIC_CONTROL" }
            }),
        ))
        .unwrap();
        let id = body_of(
            s.create_deployment(&req(
                "CreateDeployment",
                json!({ "applicationName": "app", "deploymentGroupName": "dg" }),
            ))
            .unwrap(),
        )["deploymentId"]
            .as_str()
            .unwrap()
            .to_string();
        let got = body_of(
            s.get_deployment(&req("GetDeployment", json!({ "deploymentId": id })))
                .unwrap(),
        );
        assert_eq!(
            got["deploymentInfo"]["deploymentStyle"]["deploymentType"],
            "BLUE_GREEN"
        );
        assert_eq!(
            got["deploymentInfo"]["deploymentStyle"]["deploymentOption"],
            "WITH_TRAFFIC_CONTROL"
        );
    }

    // T2.6: CreateDeployment carries overrideAlarmConfiguration into the stored
    // deployment so GetDeployment echoes it.
    #[test]
    fn create_deployment_carries_override_alarm_configuration() {
        let s = svc();
        make_app_and_group(&s, "app", "dg");
        let id = body_of(
            s.create_deployment(&req(
                "CreateDeployment",
                json!({
                    "applicationName": "app",
                    "deploymentGroupName": "dg",
                    "overrideAlarmConfiguration": { "enabled": true, "alarms": [{ "name": "cpu-high" }] }
                }),
            ))
            .unwrap(),
        )["deploymentId"]
            .as_str()
            .unwrap()
            .to_string();
        let got = body_of(
            s.get_deployment(&req("GetDeployment", json!({ "deploymentId": id })))
                .unwrap(),
        );
        assert_eq!(
            got["deploymentInfo"]["overrideAlarmConfiguration"]["enabled"],
            true
        );
        assert_eq!(
            got["deploymentInfo"]["overrideAlarmConfiguration"]["alarms"][0]["name"],
            "cpu-high"
        );
    }

    // T2.7: deleting a resource purges its tag entry, so a same-named
    // recreation does not resurrect stale tags.
    #[test]
    fn delete_purges_resource_tags() {
        let s = svc();
        // Application.
        s.create_application(&req(
            "CreateApplication",
            json!({ "applicationName": "app", "tags": [{ "Key": "k", "Value": "v" }] }),
        ))
        .unwrap();
        let app_arn = "arn:aws:codedeploy:us-east-1:000000000000:application:app";
        assert_eq!(tags_for(&s, app_arn).len(), 1);
        // Deployment group with tags, under the same app.
        s.create_deployment_group(&req(
            "CreateDeploymentGroup",
            json!({
                "applicationName": "app",
                "deploymentGroupName": "dg",
                "serviceRoleArn": "arn:aws:iam::000000000000:role/cd",
                "tags": [{ "Key": "t", "Value": "core" }]
            }),
        ))
        .unwrap();
        let dg_arn = "arn:aws:codedeploy:us-east-1:000000000000:deploymentgroup:app/dg";
        assert_eq!(tags_for(&s, dg_arn).len(), 1);
        // Deleting the application purges both the app and group tag entries.
        s.delete_application(&req(
            "DeleteApplication",
            json!({ "applicationName": "app" }),
        ))
        .unwrap();
        assert_eq!(tags_for(&s, app_arn).len(), 0);
        assert_eq!(tags_for(&s, dg_arn).len(), 0);

        // Deployment config.
        s.create_deployment_config(&req(
            "CreateDeploymentConfig",
            json!({ "deploymentConfigName": "cfg", "minimumHealthyHosts": { "type": "HOST_COUNT", "value": 1 } }),
        ))
        .unwrap();
        let cfg_arn = "arn:aws:codedeploy:us-east-1:000000000000:deploymentconfig:cfg";
        s.tag_resource(&req(
            "TagResource",
            json!({ "ResourceArn": cfg_arn, "Tags": [{ "Key": "k", "Value": "v" }] }),
        ))
        .unwrap();
        assert_eq!(tags_for(&s, cfg_arn).len(), 1);
        s.delete_deployment_config(&req(
            "DeleteDeploymentConfig",
            json!({ "deploymentConfigName": "cfg" }),
        ))
        .unwrap();
        assert_eq!(tags_for(&s, cfg_arn).len(), 0);

        // Deleting a deployment group directly also purges its tags.
        make_app_and_group(&s, "app2", "dg2");
        let dg2_arn = "arn:aws:codedeploy:us-east-1:000000000000:deploymentgroup:app2/dg2";
        s.tag_resource(&req(
            "TagResource",
            json!({ "ResourceArn": dg2_arn, "Tags": [{ "Key": "k", "Value": "v" }] }),
        ))
        .unwrap();
        assert_eq!(tags_for(&s, dg2_arn).len(), 1);
        s.delete_deployment_group(&req(
            "DeleteDeploymentGroup",
            json!({ "applicationName": "app2", "deploymentGroupName": "dg2" }),
        ))
        .unwrap();
        assert_eq!(tags_for(&s, dg2_arn).len(), 0);
    }
}
