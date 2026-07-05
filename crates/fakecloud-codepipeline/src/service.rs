//! AWS CodePipeline awsJson1.1 dispatch + operation handlers.
//!
//! Implements the full CodePipeline control plane: pipelines and their
//! versions, pipeline/action/rule executions, custom action types, webhooks,
//! job and third-party-job polling, stage transitions, approvals, and resource
//! tagging. There is no real release engine, so a `StartPipelineExecution`
//! mints a pipeline execution in `InProgress` that deterministically advances
//! to a terminal `Succeeded` state across successive `GetPipelineExecution`,
//! `GetPipelineState`, and `ListPipelineExecutions` reads (the same lazy-settle
//! pattern CodeDeploy and CodeBuild use for async state). Everything else is
//! real, persisted, account-partitioned CRUD.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::SharedCodePipelineState;
use crate::validate;

pub const CODEPIPELINE_ACTIONS: &[&str] = &[
    "AcknowledgeJob",
    "AcknowledgeThirdPartyJob",
    "CreateCustomActionType",
    "CreatePipeline",
    "DeleteCustomActionType",
    "DeletePipeline",
    "DeleteWebhook",
    "DeregisterWebhookWithThirdParty",
    "DisableStageTransition",
    "EnableStageTransition",
    "GetActionType",
    "GetJobDetails",
    "GetPipeline",
    "GetPipelineExecution",
    "GetPipelineState",
    "GetThirdPartyJobDetails",
    "ListActionExecutions",
    "ListActionTypes",
    "ListDeployActionExecutionTargets",
    "ListPipelineExecutions",
    "ListPipelines",
    "ListRuleExecutions",
    "ListRuleTypes",
    "ListTagsForResource",
    "ListWebhooks",
    "OverrideStageCondition",
    "PollForJobs",
    "PollForThirdPartyJobs",
    "PutActionRevision",
    "PutApprovalResult",
    "PutJobFailureResult",
    "PutJobSuccessResult",
    "PutThirdPartyJobFailureResult",
    "PutThirdPartyJobSuccessResult",
    "PutWebhook",
    "RegisterWebhookWithThirdParty",
    "RetryStageExecution",
    "RollbackStage",
    "StartPipelineExecution",
    "StopPipelineExecution",
    "TagResource",
    "UntagResource",
    "UpdateActionType",
    "UpdatePipeline",
];

pub struct CodePipelineService {
    state: SharedCodePipelineState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl CodePipelineService {
    pub fn new(state: SharedCodePipelineState) -> Self {
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
/// snapshot). The execution-settling reads advance in-progress executions to a
/// terminal state on read, so they persist too.
/// Actions that always mutate persisted state on a successful response (so a
/// snapshot is taken unconditionally). The settling read ops
/// (`GetPipelineExecution`, `GetPipelineState`, `ListPipelineExecutions`) are
/// deliberately absent: they persist only when a settle actually changed an
/// execution, signalled at runtime via the per-call `changed` flag.
fn unconditional_mutator(action: &str) -> bool {
    matches!(
        action,
        "CreateCustomActionType"
            | "CreatePipeline"
            | "DeleteCustomActionType"
            | "DeletePipeline"
            | "DeleteWebhook"
            | "DisableStageTransition"
            | "EnableStageTransition"
            | "PutWebhook"
            | "StartPipelineExecution"
            | "StopPipelineExecution"
            | "TagResource"
            | "UntagResource"
            | "UpdatePipeline"
    )
}

#[async_trait]
impl AwsService for CodePipelineService {
    fn service_name(&self) -> &str {
        "codepipeline"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        // `changed` is set by settling reads when a settle actually mutated an
        // execution. It never crosses the `.await` boundary below as a live
        // borrow, so `handle` stays `Send`.
        let changed = std::cell::Cell::new(false);
        let result = self.dispatch(&action, &req, &changed);
        let should_save = matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
            && (unconditional_mutator(&action) || changed.get());
        if should_save {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        CODEPIPELINE_ACTIONS
    }
}

impl CodePipelineService {
    fn dispatch(
        &self,
        action: &str,
        req: &AwsRequest,
        changed: &std::cell::Cell<bool>,
    ) -> Result<AwsResponse, AwsServiceError> {
        match action {
            "CreatePipeline" => self.create_pipeline(req),
            "GetPipeline" => self.get_pipeline(req),
            "UpdatePipeline" => self.update_pipeline(req),
            "DeletePipeline" => self.delete_pipeline(req),
            "ListPipelines" => self.list_pipelines(req),
            "GetPipelineState" => self.get_pipeline_state(req, changed),
            "GetPipelineExecution" => self.get_pipeline_execution(req, changed),
            "ListPipelineExecutions" => self.list_pipeline_executions(req, changed),
            "StartPipelineExecution" => self.start_pipeline_execution(req),
            "StopPipelineExecution" => self.stop_pipeline_execution(req),
            "ListActionExecutions" => self.list_action_executions(req),
            "ListDeployActionExecutionTargets" => self.list_deploy_action_execution_targets(req),
            "ListRuleExecutions" => self.list_rule_executions(req),
            "ListRuleTypes" => self.list_rule_types(req),
            "PutActionRevision" => self.put_action_revision(req),
            "PutApprovalResult" => self.put_approval_result(req),
            "EnableStageTransition" => self.enable_stage_transition(req),
            "DisableStageTransition" => self.disable_stage_transition(req),
            "RetryStageExecution" => self.retry_stage_execution(req),
            "RollbackStage" => self.rollback_stage(req),
            "OverrideStageCondition" => self.override_stage_condition(req),
            "CreateCustomActionType" => self.create_custom_action_type(req),
            "DeleteCustomActionType" => self.delete_custom_action_type(req),
            "ListActionTypes" => self.list_action_types(req),
            "GetActionType" => self.get_action_type(req),
            "UpdateActionType" => self.update_action_type(req),
            "PutWebhook" => self.put_webhook(req),
            "DeleteWebhook" => self.delete_webhook(req),
            "ListWebhooks" => self.list_webhooks(req),
            "RegisterWebhookWithThirdParty" => self.register_webhook_with_third_party(req),
            "DeregisterWebhookWithThirdParty" => self.deregister_webhook_with_third_party(req),
            "AcknowledgeJob" => self.acknowledge_job(req),
            "AcknowledgeThirdPartyJob" => self.acknowledge_third_party_job(req),
            "GetJobDetails" => self.get_job_details(req),
            "GetThirdPartyJobDetails" => self.get_third_party_job_details(req),
            "PollForJobs" => self.poll_for_jobs(req),
            "PollForThirdPartyJobs" => self.poll_for_third_party_jobs(req),
            "PutJobSuccessResult" => self.put_job_success_result(req),
            "PutJobFailureResult" => self.put_job_failure_result(req),
            "PutThirdPartyJobSuccessResult" => self.put_third_party_job_success_result(req),
            "PutThirdPartyJobFailureResult" => self.put_third_party_job_failure_result(req),
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

/// Every CodePipeline operation declares `ValidationException`; it is the
/// canonical input-shape error, so absent/malformed inputs land on it.
fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn err(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg)
}

fn body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn str_field(b: &Value, key: &str) -> Option<String> {
    b.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn len_ok(v: &str, min: usize, max: usize) -> bool {
    let n = v.chars().count();
    n >= min && n <= max
}

/// A `PipelineName` / `StageName` / `ActionName`: 1-100 chars matching
/// `^[A-Za-z0-9.@\-_]+$`.
fn valid_name(v: &str) -> bool {
    len_ok(v, 1, 100)
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '@' | '-' | '_'))
}

fn gen_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Require a present, non-empty string member, erroring with
/// `ValidationException` when absent or empty.
fn req_str(b: &Value, key: &str) -> Result<String, AwsServiceError> {
    match str_field(b, key) {
        Some(v) if !v.is_empty() => Ok(v),
        _ => Err(validation(format!(
            "1 validation error detected: Value null at '{key}' failed to satisfy constraint: Member must not be null"
        ))),
    }
}

/// Require a valid pipeline name in member `key`.
fn req_pipeline_name(b: &Value, key: &str) -> Result<String, AwsServiceError> {
    let v = req_str(b, key)?;
    if valid_name(&v) {
        Ok(v)
    } else {
        Err(validation(format!(
            "Pipeline name in member '{key}' was specified in an invalid format."
        )))
    }
}

/// Validate the `@length` constraint of a present string member.
fn check_len(b: &Value, key: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if let Some(v) = str_field(b, key) {
        if !len_ok(&v, min, max) {
            return Err(validation(format!(
                "Value '{v}' at '{key}' failed to satisfy constraint: Member must have length between {min} and {max}."
            )));
        }
    }
    Ok(())
}

/// Validate the `@range` constraint of a present integer member (`max = None`
/// for a lower-bound-only range).
fn check_int_range(
    b: &Value,
    key: &str,
    min: i64,
    max: Option<i64>,
) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(key).and_then(Value::as_i64) {
        if v < min || max.map(|m| v > m).unwrap_or(false) {
            return Err(validation(format!(
                "Value '{v}' at '{key}' failed to satisfy constraint: Member must be within the allowed range."
            )));
        }
    }
    Ok(())
}

fn opt_enum(b: &Value, key: &str, set: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(v) = str_field(b, key) {
        if !validate::is_enum(set, &v) {
            return Err(validation(format!(
                "Value '{v}' at '{key}' failed to satisfy constraint: Member must satisfy enum value set."
            )));
        }
    }
    Ok(())
}

/// Whether `s` is a syntactically valid CodePipeline resource ARN
/// (`arn:aws...:codepipeline:<region>:<12-digit-account>:<resource>`).
fn is_codepipeline_arn(s: &str) -> bool {
    let parts: Vec<&str> = s.splitn(6, ':').collect();
    parts.len() == 6
        && parts[0] == "arn"
        && parts[1].starts_with("aws")
        && parts[2] == "codepipeline"
        && parts[4].len() == 12
        && parts[4].chars().all(|c| c.is_ascii_digit())
        && !parts[5].is_empty()
}

fn tag_list(b: &Value) -> Vec<Value> {
    b.get("tags")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// Pull `nextToken` from the body and paginate a list of JSON values into
/// `{list_key: [...], nextToken?}` with a page size honoring `maxResults`.
fn paginate_body(
    items: Vec<Value>,
    list_key: &str,
    b: &Value,
    token_key: &str,
) -> Result<Value, AwsServiceError> {
    let token = str_field(b, token_key);
    let page_size = b
        .get("maxResults")
        .or_else(|| b.get("MaxResults"))
        .and_then(Value::as_u64)
        .filter(|n| *n > 0)
        .map(|n| n as usize)
        .unwrap_or(100);
    let (page, next) = paginate_checked(&items, token.as_deref(), page_size).map_err(|_| {
        validation("The next token was specified in an invalid format.".to_string())
    })?;
    let mut out = Map::new();
    out.insert(list_key.to_string(), Value::Array(page));
    if let Some(n) = next {
        out.insert(token_key.to_string(), Value::String(n));
    }
    Ok(Value::Object(out))
}

impl CodePipelineService {
    fn account(&self, req: &AwsRequest) -> String {
        req.account_id.clone()
    }

    fn pipeline_arn(&self, req: &AwsRequest, name: &str) -> String {
        format!(
            "arn:aws:codepipeline:{}:{}:{}",
            req.region, req.account_id, name
        )
    }

    fn webhook_arn(&self, req: &AwsRequest, name: &str) -> String {
        format!(
            "arn:aws:codepipeline:{}:{}:webhook:{}",
            req.region, req.account_id, name
        )
    }
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

/// Validate the `PipelineDeclaration` structure, returning the pipeline name.
fn validate_pipeline_declaration(pipeline: &Value) -> Result<String, AwsServiceError> {
    let name = req_pipeline_name(pipeline, "name")?;
    if pipeline.get("roleArn").and_then(Value::as_str).is_none() {
        return Err(validation(
            "Pipeline definition must include a roleArn.".to_string(),
        ));
    }
    // A pipeline must declare exactly one artifact store: either a single
    // `artifactStore` or a per-region `artifactStores` map, never both and
    // never neither.
    let has_store = pipeline
        .get("artifactStore")
        .map(|v| v.is_object())
        .unwrap_or(false);
    let has_stores = pipeline
        .get("artifactStores")
        .map(|v| v.is_object())
        .unwrap_or(false);
    if has_store == has_stores {
        return Err(err(
            "InvalidStructureException",
            "Pipeline definition must include exactly one of artifactStore or artifactStores.",
        ));
    }
    let stages = pipeline
        .get("stages")
        .and_then(Value::as_array)
        .filter(|s| !s.is_empty());
    let Some(stages) = stages else {
        return Err(err(
            "InvalidStructureException",
            "Pipeline definition must include at least one stage.",
        ));
    };
    opt_enum(pipeline, "executionMode", validate::EXECUTION_MODE)?;
    opt_enum(pipeline, "pipelineType", validate::PIPELINE_TYPE)?;
    // Validate every stage carries a name and at least one action, and every
    // action carries a well-formed actionTypeId.
    for stage in stages {
        if stage.get("name").and_then(Value::as_str).is_none() {
            return Err(err(
                "InvalidStageDeclarationException",
                "Each stage must include a name.",
            ));
        }
        let actions = stage.get("actions").and_then(Value::as_array);
        let Some(actions) = actions.filter(|a| !a.is_empty()) else {
            return Err(err(
                "InvalidStageDeclarationException",
                "Each stage must include at least one action.",
            ));
        };
        for action in actions {
            let tid = action.get("actionTypeId");
            let Some(tid) = tid else {
                return Err(err(
                    "InvalidActionDeclarationException",
                    "Each action must include an actionTypeId.",
                ));
            };
            if let Some(cat) = tid.get("category").and_then(Value::as_str) {
                if !validate::is_enum(validate::ACTION_CATEGORY, cat) {
                    return Err(err(
                        "InvalidActionDeclarationException",
                        "The action category is invalid.",
                    ));
                }
            }
            if let Some(owner) = tid.get("owner").and_then(Value::as_str) {
                if !validate::is_enum(validate::ACTION_OWNER, owner) {
                    return Err(err(
                        "InvalidActionDeclarationException",
                        "The action owner is invalid.",
                    ));
                }
            }
        }
    }
    Ok(name)
}

/// Fill the AWS-applied defaults on a `PipelineDeclaration` in place: each
/// action's `runOrder` defaults to 1 when omitted, matching what CodePipeline
/// stores and returns from `GetPipeline`.
fn apply_pipeline_defaults(pipeline: &mut Value) {
    if let Some(stages) = pipeline.get_mut("stages").and_then(Value::as_array_mut) {
        for stage in stages {
            if let Some(actions) = stage.get_mut("actions").and_then(Value::as_array_mut) {
                for action in actions {
                    if let Some(obj) = action.as_object_mut() {
                        obj.entry("runOrder").or_insert_with(|| json!(1));
                    }
                }
            }
        }
    }
}

impl CodePipelineService {
    fn create_pipeline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(pipeline) = b.get("pipeline").filter(|v| v.is_object()).cloned() else {
            return Err(validation("Pipeline definition is required."));
        };
        let name = validate_pipeline_declaration(&pipeline)?;
        let now = Utc::now();
        let account = self.account(req);
        let arn = self.pipeline_arn(req, &name);
        let tags = tag_list(&b);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.pipelines.contains_key(&name) {
            return Err(err(
                "PipelineNameInUseException",
                format!("A pipeline with the name {name} already exists."),
            ));
        }
        let mut decl = pipeline;
        apply_pipeline_defaults(&mut decl);
        if let Some(obj) = decl.as_object_mut() {
            obj.insert("version".into(), json!(1));
        }
        st.pipelines.insert(name.clone(), decl.clone());
        st.pipeline_order.push(name.clone());
        st.pipeline_versions
            .insert(name.clone(), vec![decl.clone()]);
        st.pipeline_meta.insert(
            name.clone(),
            json!({ "pipelineArn": arn, "created": ts(now), "updated": ts(now) }),
        );
        if !tags.is_empty() {
            st.tags.insert(arn, tags.clone());
        }
        drop(guard);
        let mut out = Map::new();
        out.insert("pipeline".into(), decl);
        if !tags.is_empty() {
            out.insert("tags".into(), Value::Array(tags));
        }
        ok(Value::Object(out))
    }

    fn get_pipeline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "name")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.pipelines.contains_key(&name) {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ));
        }
        let decl = if let Some(v) = b.get("version").and_then(Value::as_u64) {
            let versions = st.pipeline_versions.get(&name);
            let idx = (v as usize).checked_sub(1);
            match idx.and_then(|i| versions.and_then(|vs| vs.get(i))) {
                Some(d) => d.clone(),
                None => {
                    return Err(err(
                        "PipelineVersionNotFoundException",
                        format!("Pipeline {name} does not have a version {v}."),
                    ))
                }
            }
        } else {
            st.pipelines.get(&name).cloned().unwrap()
        };
        let meta = st
            .pipeline_meta
            .get(&name)
            .cloned()
            .unwrap_or_else(|| json!({ "pipelineArn": self.pipeline_arn(req, &name) }));
        ok(json!({ "pipeline": decl, "metadata": meta }))
    }

    fn update_pipeline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(pipeline) = b.get("pipeline").filter(|v| v.is_object()).cloned() else {
            return Err(validation("Pipeline definition is required."));
        };
        let name = validate_pipeline_declaration(&pipeline)?;
        let now = Utc::now();
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // UpdatePipeline does not create pipelines; the Smithy contract has no
        // "not found" error, so a missing pipeline is a validation error.
        if !st.pipelines.contains_key(&name) {
            return Err(validation(format!(
                "The pipeline with the name {name} does not exist."
            )));
        }
        let next_version = st
            .pipeline_versions
            .get(&name)
            .map(|v| v.len() as u64 + 1)
            .unwrap_or(1);
        let mut decl = pipeline;
        apply_pipeline_defaults(&mut decl);
        if let Some(obj) = decl.as_object_mut() {
            obj.insert("version".into(), json!(next_version));
        }
        st.pipelines.insert(name.clone(), decl.clone());
        st.pipeline_versions
            .entry(name.clone())
            .or_default()
            .push(decl.clone());
        if let Some(meta) = st.pipeline_meta.get_mut(&name) {
            if let Some(obj) = meta.as_object_mut() {
                obj.insert("updated".into(), ts(now));
            }
        }
        ok(json!({ "pipeline": decl }))
    }

    fn delete_pipeline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "name")?;
        let account = self.account(req);
        let arn = self.pipeline_arn(req, &name);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // DeletePipeline is idempotent: no "does not exist" error is declared.
        st.pipelines.remove(&name);
        st.pipeline_order.retain(|n| n != &name);
        st.pipeline_meta.remove(&name);
        st.pipeline_versions.remove(&name);
        if let Some(ids) = st.execution_order.remove(&name) {
            for id in ids {
                st.executions.remove(&id);
            }
        }
        st.transitions_disabled.remove(&name);
        st.tags.remove(&arn);
        ok(json!({}))
    }

    fn list_pipelines(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        check_int_range(&b, "maxResults", 1, Some(1000))?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let summaries: Vec<Value> = st
            .pipeline_order
            .iter()
            .filter_map(|n| {
                let decl = st.pipelines.get(n)?;
                let meta = st.pipeline_meta.get(n);
                let mut s = Map::new();
                s.insert("name".into(), json!(n));
                if let Some(v) = decl.get("version") {
                    s.insert("version".into(), v.clone());
                }
                for k in ["pipelineType", "executionMode"] {
                    if let Some(v) = decl.get(k) {
                        s.insert(k.into(), v.clone());
                    }
                }
                if let Some(m) = meta {
                    for k in ["created", "updated"] {
                        if let Some(v) = m.get(k) {
                            s.insert(k.into(), v.clone());
                        }
                    }
                }
                Some(Value::Object(s))
            })
            .collect();
        ok(paginate_body(summaries, "pipelines", &b, "nextToken")?)
    }
}

// ---------------------------------------------------------------------------
// Pipeline executions
// ---------------------------------------------------------------------------

/// The terminal `PipelineExecutionStatus` values: an execution in any of these
/// is never re-settled.
const TERMINAL_EXECUTION_STATUSES: &[&str] =
    &["Succeeded", "Failed", "Stopped", "Superseded", "Cancelled"];

/// Advance a non-terminal execution one step toward a terminal state on read:
/// an `InProgress` execution settles to `Succeeded`, a `Stopping` execution
/// settles to `Stopped`. Terminal executions are left untouched. Returns
/// whether the execution was actually mutated (so the caller can gate a
/// snapshot on real change rather than persisting on every poll).
fn settle_execution(exec: &mut Value) -> bool {
    let status = exec
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("InProgress");
    let next = match status {
        s if TERMINAL_EXECUTION_STATUSES.contains(&s) => return false,
        "Stopping" => "Stopped",
        _ => "Succeeded",
    };
    if let Some(obj) = exec.as_object_mut() {
        obj.insert("status".into(), json!(next));
        obj.insert("lastUpdateTime".into(), ts(Utc::now()));
        true
    } else {
        false
    }
}

/// Settle every execution of a pipeline in place, honoring `executionMode`.
/// In `SUPERSEDED` mode, when more than one execution is still non-terminal,
/// all but the most-recent `InProgress` runs become `Superseded` and only the
/// latest advances to `Succeeded`. A `Stopping` execution is left to settle to
/// `Stopped` (an explicit stop wins over supersession), and terminal executions
/// are never touched. In `PARALLEL`/`QUEUED` mode every execution settles
/// independently. `order` is the pipeline's execution ids oldest-first.
/// Returns whether any execution was mutated.
fn settle_pipeline_executions(
    executions: &mut std::collections::BTreeMap<String, Value>,
    order: &[String],
    mode: &str,
) -> bool {
    let mut changed = false;
    if mode == "SUPERSEDED" {
        // Non-terminal executions, oldest-first; all but the newest are
        // superseded by a later run.
        let in_flight: Vec<String> = order
            .iter()
            .filter(|id| {
                executions
                    .get(*id)
                    .and_then(|e| e.get("status").and_then(Value::as_str))
                    .map(|s| !TERMINAL_EXECUTION_STATUSES.contains(&s))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if let Some((newest, older)) = in_flight.split_last() {
            for id in older {
                if let Some(exec) = executions.get_mut(id) {
                    // A user-requested stop (`Stopping`) settles to `Stopped`,
                    // not `Superseded`: the explicit stop intent wins. Only an
                    // `InProgress` run is superseded by a later execution.
                    let is_in_progress =
                        exec.get("status").and_then(Value::as_str) == Some("InProgress");
                    if is_in_progress {
                        if let Some(obj) = exec.as_object_mut() {
                            obj.insert("status".into(), json!("Superseded"));
                            obj.insert("lastUpdateTime".into(), ts(Utc::now()));
                            changed = true;
                        }
                    } else {
                        changed |= settle_execution(exec);
                    }
                }
            }
            if let Some(exec) = executions.get_mut(newest) {
                changed |= settle_execution(exec);
            }
        }
    } else {
        for id in order {
            if let Some(exec) = executions.get_mut(id) {
                changed |= settle_execution(exec);
            }
        }
    }
    changed
}

impl CodePipelineService {
    /// The pipeline's configured `executionMode` (defaults to `SUPERSEDED`,
    /// matching CodePipeline's default for pipelines that don't set it).
    fn execution_mode(&self, st: &crate::state::CodePipelineState, name: &str) -> String {
        st.pipelines
            .get(name)
            .and_then(|d| d.get("executionMode").and_then(Value::as_str))
            .unwrap_or("SUPERSEDED")
            .to_string()
    }

    fn start_pipeline_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "name")?;
        let account = self.account(req);
        let now = Utc::now();
        let exec_id = gen_uuid();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(decl) = st.pipelines.get(&name) else {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ));
        };
        let version = decl.get("version").cloned().unwrap_or(json!(1));
        let mode = decl
            .get("executionMode")
            .cloned()
            .unwrap_or_else(|| json!("SUPERSEDED"));
        let exec = json!({
            "pipelineName": name,
            "pipelineVersion": version,
            "pipelineExecutionId": exec_id,
            "status": "InProgress",
            "executionMode": mode,
            "executionType": "STANDARD",
            "startTime": ts(now),
            "lastUpdateTime": ts(now),
            "trigger": { "triggerType": "StartPipelineExecution", "triggerDetail": "user" },
        });
        st.executions.insert(exec_id.clone(), exec);
        st.execution_order
            .entry(name)
            .or_default()
            .push(exec_id.clone());
        ok(json!({ "pipelineExecutionId": exec_id }))
    }

    fn get_pipeline_execution(
        &self,
        req: &AwsRequest,
        changed: &std::cell::Cell<bool>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        let exec_id = req_str(&b, "pipelineExecutionId")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.pipelines.contains_key(&name) {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ));
        }
        let belongs = st
            .execution_order
            .get(&name)
            .map(|ids| ids.iter().any(|i| i == &exec_id))
            .unwrap_or(false);
        if !belongs || !st.executions.contains_key(&exec_id) {
            return Err(err(
                "PipelineExecutionNotFoundException",
                format!("Pipeline execution {exec_id} not found for pipeline {name}."),
            ));
        }
        let mode = self.execution_mode(st, &name);
        let order = st.execution_order.get(&name).cloned().unwrap_or_default();
        if settle_pipeline_executions(&mut st.executions, &order, &mode) {
            changed.set(true);
        }
        let exec = st.executions.get(&exec_id).cloned().unwrap();
        // Project the stored execution onto the PipelineExecution output shape.
        let mut pe = Map::new();
        for k in [
            "pipelineName",
            "pipelineVersion",
            "pipelineExecutionId",
            "status",
            "statusSummary",
            "executionMode",
            "executionType",
            "trigger",
        ] {
            if let Some(v) = exec.get(k) {
                pe.insert(k.into(), v.clone());
            }
        }
        ok(json!({ "pipelineExecution": Value::Object(pe) }))
    }

    fn list_pipeline_executions(
        &self,
        req: &AwsRequest,
        changed: &std::cell::Cell<bool>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.pipelines.contains_key(&name) {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ));
        }
        let mode = self.execution_mode(st, &name);
        let order = st.execution_order.get(&name).cloned().unwrap_or_default();
        if settle_pipeline_executions(&mut st.executions, &order, &mode) {
            changed.set(true);
        }
        // Summaries are returned most-recent first.
        let summaries: Vec<Value> = order
            .iter()
            .rev()
            .filter_map(|id| st.executions.get(id))
            .map(|exec| {
                let mut s = Map::new();
                for k in [
                    "pipelineExecutionId",
                    "status",
                    "statusSummary",
                    "startTime",
                    "lastUpdateTime",
                    "executionMode",
                    "executionType",
                    "trigger",
                ] {
                    if let Some(v) = exec.get(k) {
                        s.insert(k.into(), v.clone());
                    }
                }
                Value::Object(s)
            })
            .collect();
        ok(paginate_body(
            summaries,
            "pipelineExecutionSummaries",
            &b,
            "nextToken",
        )?)
    }

    fn stop_pipeline_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        let exec_id = req_str(&b, "pipelineExecutionId")?;
        let abandon = b.get("abandon").and_then(Value::as_bool).unwrap_or(false);
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.pipelines.contains_key(&name) {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ));
        }
        let belongs = st
            .execution_order
            .get(&name)
            .map(|ids| ids.iter().any(|i| i == &exec_id))
            .unwrap_or(false);
        let mut exec = st.executions.get(&exec_id).cloned();
        let status = exec
            .as_ref()
            .and_then(|e| e.get("status").and_then(Value::as_str))
            .unwrap_or("");
        if !belongs || exec.is_none() || !matches!(status, "InProgress" | "Stopping") {
            return Err(err(
                "PipelineExecutionNotStoppableException",
                format!("Pipeline execution {exec_id} is not in a stoppable state."),
            ));
        }
        if let Some(e) = exec.as_mut() {
            if let Some(obj) = e.as_object_mut() {
                obj.insert(
                    "status".into(),
                    json!(if abandon { "Stopped" } else { "Stopping" }),
                );
                obj.insert("lastUpdateTime".into(), ts(Utc::now()));
                if let Some(reason) = str_field(&b, "reason") {
                    obj.insert("statusSummary".into(), json!(reason));
                }
            }
            st.executions.insert(exec_id.clone(), e.clone());
        }
        ok(json!({ "pipelineExecutionId": exec_id }))
    }

    fn get_pipeline_state(
        &self,
        req: &AwsRequest,
        changed: &std::cell::Cell<bool>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "name")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(decl) = st.pipelines.get(&name).cloned() else {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ));
        };
        let version = decl.get("version").cloned().unwrap_or(json!(1));
        // Settle the pipeline's executions (honoring executionMode), then read
        // the latest execution's status for the per-stage summary.
        let mode = self.execution_mode(st, &name);
        let order = st.execution_order.get(&name).cloned().unwrap_or_default();
        if settle_pipeline_executions(&mut st.executions, &order, &mode) {
            changed.set(true);
        }
        let latest_id = order.last().cloned();
        let latest_status = latest_id
            .as_ref()
            .and_then(|id| st.executions.get(id))
            .and_then(|e| e.get("status").and_then(Value::as_str))
            .map(str::to_string);
        let disabled = st.transitions_disabled.get(&name).cloned();
        let meta = st.pipeline_meta.get(&name).cloned();
        let stages = decl.get("stages").and_then(Value::as_array).cloned();
        let stage_states: Vec<Value> = stages
            .unwrap_or_default()
            .iter()
            .map(|stage| {
                let stage_name = stage.get("name").cloned().unwrap_or(json!(""));
                let action_states: Vec<Value> = stage
                    .get("actions")
                    .and_then(Value::as_array)
                    .map(|acts| {
                        acts.iter()
                            .filter_map(|a| a.get("name").map(|n| json!({ "actionName": n })))
                            .collect()
                    })
                    .unwrap_or_default();
                let mut ss = Map::new();
                ss.insert("stageName".into(), stage_name.clone());
                ss.insert("actionStates".into(), Value::Array(action_states));
                // The inbound transition is enabled unless DisableStageTransition
                // recorded it as disabled for this stage.
                let stage_key = format!("{}/Inbound", stage_name.as_str().unwrap_or(""));
                let transition_state = disabled
                    .as_ref()
                    .and_then(|d| d.get(&stage_key).cloned())
                    .unwrap_or_else(|| json!({ "enabled": true }));
                ss.insert("inboundTransitionState".into(), transition_state);
                if let (Some(id), Some(status)) = (&latest_id, &latest_status) {
                    ss.insert(
                        "latestExecution".into(),
                        json!({ "pipelineExecutionId": id, "status": stage_status(status) }),
                    );
                }
                Value::Object(ss)
            })
            .collect();
        let mut out = Map::new();
        out.insert("pipelineName".into(), json!(name));
        out.insert("pipelineVersion".into(), version);
        out.insert("stageStates".into(), Value::Array(stage_states));
        if let Some(m) = meta {
            for k in ["created", "updated"] {
                if let Some(v) = m.get(k) {
                    out.insert(k.into(), v.clone());
                }
            }
        }
        ok(Value::Object(out))
    }

    fn list_action_executions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.pipelines.contains_key(&name) {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ));
        }
        ok(json!({ "actionExecutionDetails": [] }))
    }

    fn list_deploy_action_execution_targets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let exec_id = req_str(&b, "actionExecutionId")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if let Some(name) = str_field(&b, "pipelineName") {
            if !st.pipelines.contains_key(&name) {
                return Err(err(
                    "PipelineNotFoundException",
                    format!("Account does not have a pipeline with name {name}."),
                ));
            }
        }
        // No action executions are tracked, so any id is unknown.
        Err(err(
            "ActionExecutionNotFoundException",
            format!("Action execution {exec_id} was not found."),
        ))
    }

    fn list_rule_executions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.pipelines.contains_key(&name) {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ));
        }
        ok(json!({ "ruleExecutionDetails": [] }))
    }
}

/// Map a `PipelineExecutionStatus` onto the closest `StageExecutionStatus`.
fn stage_status(pipeline_status: &str) -> &str {
    match pipeline_status {
        "Succeeded" => "Succeeded",
        "Failed" => "Failed",
        "Stopped" => "Stopped",
        "Stopping" => "Stopping",
        "Cancelled" | "Superseded" => "Cancelled",
        _ => "InProgress",
    }
}

// ---------------------------------------------------------------------------
// Stage operations (transitions, retry, rollback, override, approvals, revisions)
// ---------------------------------------------------------------------------

impl CodePipelineService {
    fn pipeline_must_exist(&self, req: &AwsRequest, name: &str) -> Result<(), AwsServiceError> {
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.pipelines.contains_key(name) {
            Ok(())
        } else {
            Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ))
        }
    }

    fn enable_stage_transition(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        let stage_name = req_str(&b, "stageName")?;
        opt_enum(&b, "transitionType", validate::STAGE_TRANSITION_TYPE)?;
        let Some(transition_type) = str_field(&b, "transitionType") else {
            return Err(validation("transitionType is required."));
        };
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.pipelines.contains_key(&name) {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ));
        }
        // Re-enabling clears any recorded disabled state for this transition.
        let key = format!("{stage_name}/{transition_type}");
        if let Some(map) = st.transitions_disabled.get_mut(&name) {
            map.remove(&key);
            if map.is_empty() {
                st.transitions_disabled.remove(&name);
            }
        }
        ok(json!({}))
    }

    fn disable_stage_transition(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        let stage_name = req_str(&b, "stageName")?;
        let reason = req_str(&b, "reason")?;
        opt_enum(&b, "transitionType", validate::STAGE_TRANSITION_TYPE)?;
        let Some(transition_type) = str_field(&b, "transitionType") else {
            return Err(validation("transitionType is required."));
        };
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.pipelines.contains_key(&name) {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {name}."),
            ));
        }
        let key = format!("{stage_name}/{transition_type}");
        st.transitions_disabled.entry(name).or_default().insert(
            key,
            json!({
                "enabled": false,
                "disabledReason": reason,
                "lastChangedBy": req.account_id,
                "lastChangedAt": ts(Utc::now()),
            }),
        );
        ok(json!({}))
    }

    fn retry_stage_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        req_str(&b, "stageName")?;
        req_str(&b, "pipelineExecutionId")?;
        opt_enum(&b, "retryMode", validate::STAGE_RETRY_MODE)?;
        self.pipeline_must_exist(req, &name)?;
        // No failed stage execution is tracked to retry.
        Err(err(
            "StageNotRetryableException",
            "The stage has no failed actions to retry.",
        ))
    }

    fn rollback_stage(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        req_str(&b, "stageName")?;
        req_str(&b, "targetPipelineExecutionId")?;
        self.pipeline_must_exist(req, &name)?;
        Err(err(
            "PipelineExecutionNotFoundException",
            "The target pipeline execution was not found.",
        ))
    }

    fn override_stage_condition(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        req_str(&b, "stageName")?;
        req_str(&b, "pipelineExecutionId")?;
        opt_enum(&b, "conditionType", validate::CONDITION_TYPE)?;
        if str_field(&b, "conditionType").is_none() {
            return Err(validation("conditionType is required."));
        }
        self.pipeline_must_exist(req, &name)?;
        Err(err(
            "StageNotFoundException",
            "The stage was not found for the pipeline.",
        ))
    }

    fn put_action_revision(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        req_str(&b, "stageName")?;
        req_str(&b, "actionName")?;
        if !b
            .get("actionRevision")
            .map(Value::is_object)
            .unwrap_or(false)
        {
            return Err(validation("actionRevision is required."));
        }
        self.pipeline_must_exist(req, &name)?;
        Err(err(
            "StageNotFoundException",
            "The stage was not found for the pipeline.",
        ))
    }

    fn put_approval_result(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_pipeline_name(&b, "pipelineName")?;
        req_str(&b, "stageName")?;
        req_str(&b, "actionName")?;
        req_str(&b, "token")?;
        let result = b.get("result").filter(|v| v.is_object());
        let Some(result) = result else {
            return Err(validation("result is required."));
        };
        if let Some(status) = result.get("status").and_then(Value::as_str) {
            if !validate::is_enum(validate::APPROVAL_STATUS, status) {
                return Err(validation("The approval status is invalid."));
            }
        }
        self.pipeline_must_exist(req, &name)?;
        // No manual-approval action is awaiting a result.
        Err(err(
            "ActionNotFoundException",
            "The approval action was not found for the pipeline.",
        ))
    }
}

// ---------------------------------------------------------------------------
// Custom action types
// ---------------------------------------------------------------------------

fn action_key(category: &str, provider: &str, version: &str) -> String {
    format!("{category}:{provider}:{version}")
}

/// The canonical AWS-owned action-type catalog returned by `ListActionTypes`
/// (owner `AWS`), transcribed from the providers CodePipeline ships across each
/// category. Every entry carries the required `id`, `inputArtifactDetails`, and
/// `outputArtifactDetails` members. Artifact counts follow each category's
/// contract (sources produce but don't consume artifacts; approvals do
/// neither; build/test/deploy/invoke consume and may produce).
fn aws_owned_action_types() -> Vec<Value> {
    // (category, provider, input_min, input_max, output_min, output_max)
    const CATALOG: &[AwsActionType] = &[
        ("Source", "S3", 0, 0, 1, 1),
        ("Source", "CodeCommit", 0, 0, 1, 1),
        ("Source", "ECR", 0, 0, 1, 1),
        ("Source", "CodeStarSourceConnection", 0, 0, 1, 1),
        ("Build", "CodeBuild", 1, 5, 0, 5),
        ("Build", "CodeBuildBatch", 1, 5, 0, 5),
        ("Test", "CodeBuild", 1, 5, 0, 5),
        ("Test", "DeviceFarm", 1, 1, 0, 0),
        ("Deploy", "CodeDeploy", 1, 1, 0, 0),
        ("Deploy", "CloudFormation", 0, 10, 0, 1),
        ("Deploy", "ECS", 1, 1, 0, 0),
        ("Deploy", "S3", 1, 1, 0, 0),
        ("Deploy", "ElasticBeanstalk", 1, 1, 0, 0),
        ("Deploy", "CodeDeployToECS", 1, 3, 0, 0),
        ("Deploy", "ServiceCatalog", 1, 1, 0, 0),
        ("Deploy", "CloudFormationStackSet", 0, 10, 0, 1),
        ("Deploy", "CloudFormationStackInstances", 0, 10, 0, 1),
        ("Deploy", "AppConfig", 1, 1, 0, 0),
        ("Approval", "Manual", 0, 0, 0, 0),
        ("Invoke", "Lambda", 0, 5, 0, 5),
        ("Invoke", "StepFunctions", 0, 5, 0, 5),
    ];
    CATALOG
        .iter()
        .map(|(category, provider, imin, imax, omin, omax)| {
            json!({
                "id": {
                    "category": category,
                    "owner": "AWS",
                    "provider": provider,
                    "version": "1",
                },
                "inputArtifactDetails": { "minimumCount": imin, "maximumCount": imax },
                "outputArtifactDetails": { "minimumCount": omin, "maximumCount": omax },
            })
        })
        .collect()
}

/// One AWS-owned action type: `(category, provider, input_min, input_max,
/// output_min, output_max)`.
type AwsActionType = (&'static str, &'static str, i64, i64, i64, i64);

impl CodePipelineService {
    fn create_custom_action_type(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let category = req_str(&b, "category")?;
        if !validate::is_enum(validate::ACTION_CATEGORY, &category) {
            return Err(validation("The action category is invalid."));
        }
        check_len(&b, "provider", 1, 35)?;
        check_len(&b, "version", 1, 9)?;
        let provider = req_str(&b, "provider")?;
        let version = req_str(&b, "version")?;
        let input_details = b
            .get("inputArtifactDetails")
            .filter(|v| v.is_object())
            .cloned();
        let output_details = b
            .get("outputArtifactDetails")
            .filter(|v| v.is_object())
            .cloned();
        let (Some(input_details), Some(output_details)) = (input_details, output_details) else {
            return Err(validation(
                "inputArtifactDetails and outputArtifactDetails are required.",
            ));
        };
        let account = self.account(req);
        let tags = tag_list(&b);
        let key = action_key(&category, &provider, &version);
        let arn = format!(
            "arn:aws:codepipeline:{}:{}:actiontype:Custom/{}/{}/{}",
            req.region, req.account_id, category, provider, version
        );
        let mut action_type = Map::new();
        action_type.insert(
            "id".into(),
            json!({ "category": category, "owner": "Custom", "provider": provider, "version": version }),
        );
        if let Some(s) = b.get("settings").filter(|v| v.is_object()) {
            action_type.insert("settings".into(), s.clone());
        }
        if let Some(p) = b.get("configurationProperties").filter(|v| v.is_array()) {
            action_type.insert("actionConfigurationProperties".into(), p.clone());
        }
        action_type.insert("inputArtifactDetails".into(), input_details);
        action_type.insert("outputArtifactDetails".into(), output_details);
        let action_type = Value::Object(action_type);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.custom_actions.contains_key(&key) {
            st.custom_action_order.push(key.clone());
        }
        st.custom_actions.insert(key, action_type.clone());
        if !tags.is_empty() {
            st.tags.insert(arn, tags.clone());
        }
        let mut out = Map::new();
        out.insert("actionType".into(), action_type);
        if !tags.is_empty() {
            out.insert("tags".into(), Value::Array(tags));
        }
        ok(Value::Object(out))
    }

    fn delete_custom_action_type(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let category = req_str(&b, "category")?;
        if !validate::is_enum(validate::ACTION_CATEGORY, &category) {
            return Err(validation("The action category is invalid."));
        }
        check_len(&b, "provider", 1, 35)?;
        check_len(&b, "version", 1, 9)?;
        let provider = req_str(&b, "provider")?;
        let version = req_str(&b, "version")?;
        let account = self.account(req);
        let key = action_key(&category, &provider, &version);
        let arn = format!(
            "arn:aws:codepipeline:{}:{}:actiontype:Custom/{}/{}/{}",
            req.region, req.account_id, category, provider, version
        );
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Idempotent: no "does not exist" error is declared.
        if st.custom_actions.remove(&key).is_some() {
            st.custom_action_order.retain(|k| k != &key);
        }
        st.tags.remove(&arn);
        ok(json!({}))
    }

    fn list_action_types(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        opt_enum(&b, "actionOwnerFilter", validate::ACTION_TYPE_OWNER)?;
        check_len(&b, "regionFilter", 4, 30)?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let owner_filter = str_field(&b, "actionOwnerFilter");
        let want = |owner: &str| owner_filter.as_deref().map(|o| o == owner).unwrap_or(true);
        let mut action_types: Vec<Value> = Vec::new();
        // AWS-owned built-in providers.
        if want("AWS") {
            action_types.extend(aws_owned_action_types());
        }
        // Custom action types registered via CreateCustomActionType.
        if want("Custom") {
            action_types.extend(
                st.custom_action_order
                    .iter()
                    .filter_map(|k| st.custom_actions.get(k).cloned()),
            );
        }
        // No ThirdParty (marketplace) action types are registered.
        ok(paginate_body(action_types, "actionTypes", &b, "nextToken")?)
    }

    fn get_action_type(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let category = req_str(&b, "category")?;
        if !validate::is_enum(validate::ACTION_CATEGORY, &category) {
            return Err(validation("The action category is invalid."));
        }
        let owner = req_str(&b, "owner")?;
        if !validate::is_enum(validate::ACTION_TYPE_OWNER, &owner) {
            return Err(validation("The action owner is invalid."));
        }
        let provider = req_str(&b, "provider")?;
        let version = req_str(&b, "version")?;
        // GetActionType surfaces the newer ActionTypeDeclaration registry, which
        // is not populated by CreateCustomActionType (that uses the legacy
        // ActionType shape), so any lookup is a miss.
        Err(err(
            "ActionTypeNotFoundException",
            format!("The action type {owner}/{category}/{provider}/{version} was not found."),
        ))
    }

    fn update_action_type(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let at = b.get("actionType").filter(|v| v.is_object());
        let Some(at) = at else {
            return Err(validation("actionType is required."));
        };
        if !at.get("id").map(Value::is_object).unwrap_or(false) {
            return Err(validation("actionType.id is required."));
        }
        Err(err(
            "ActionTypeNotFoundException",
            "The action type was not found.",
        ))
    }
}

// ---------------------------------------------------------------------------
// Webhooks
// ---------------------------------------------------------------------------

impl CodePipelineService {
    fn put_webhook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let webhook = b.get("webhook").filter(|v| v.is_object());
        let Some(webhook) = webhook.cloned() else {
            return Err(validation("webhook is required."));
        };
        let name = req_pipeline_name(&webhook, "name")?;
        let target_pipeline = req_str(&webhook, "targetPipeline")?;
        req_str(&webhook, "targetAction")?;
        if !webhook.get("filters").map(Value::is_array).unwrap_or(false) {
            return Err(validation("webhook.filters is required."));
        }
        let auth = webhook.get("authentication").and_then(Value::as_str);
        let Some(auth) = auth else {
            return Err(validation("webhook.authentication is required."));
        };
        if !matches!(auth, "GITHUB_HMAC" | "IP" | "UNAUTHENTICATED") {
            return Err(validation("The webhook authentication type is invalid."));
        }
        let account = self.account(req);
        let arn = self.webhook_arn(req, &name);
        let url = format!(
            "https://webhooks.{}.amazonaws.com/trigger?Component=1&webhookName={}",
            req.region, name
        );
        let tags = tag_list(&b);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.pipelines.contains_key(&target_pipeline) {
            return Err(err(
                "PipelineNotFoundException",
                format!("Account does not have a pipeline with name {target_pipeline}."),
            ));
        }
        // The webhook item is stored WITHOUT an embedded tag copy; `st.tags` is
        // the single source of truth so Tag/UntagResource stay authoritative.
        let mut item = Map::new();
        item.insert("definition".into(), webhook);
        item.insert("url".into(), json!(url));
        item.insert("arn".into(), json!(arn.clone()));
        let item = Value::Object(item);
        if !st.webhooks.contains_key(&name) {
            st.webhook_order.push(name.clone());
        }
        st.webhooks.insert(name, item.clone());
        if !tags.is_empty() {
            st.tags.insert(arn.clone(), tags);
        }
        // Project the response with the webhook's current tags.
        let mut resp = item;
        if let Some(current) = st.tags.get(&arn).filter(|t| !t.is_empty()) {
            if let Some(obj) = resp.as_object_mut() {
                obj.insert("tags".into(), Value::Array(current.clone()));
            }
        }
        ok(json!({ "webhook": resp }))
    }

    fn delete_webhook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        check_len(&b, "name", 1, 100)?;
        let name = req_str(&b, "name")?;
        let account = self.account(req);
        let arn = self.webhook_arn(req, &name);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.webhooks.remove(&name).is_some() {
            st.webhook_order.retain(|n| n != &name);
        }
        st.tags.remove(&arn);
        ok(json!({}))
    }

    fn list_webhooks(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        check_int_range(&b, "MaxResults", 1, Some(100))?;
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Populate each item's tags from `st.tags` (the single source of truth)
        // so Tag/UntagResource changes are always reflected.
        let items: Vec<Value> = st
            .webhook_order
            .iter()
            .filter_map(|n| {
                let mut item = st.webhooks.get(n)?.clone();
                let arn = item.get("arn").and_then(Value::as_str).map(str::to_string);
                if let Some(arn) = arn {
                    if let Some(current) = st.tags.get(&arn).filter(|t| !t.is_empty()) {
                        if let Some(obj) = item.as_object_mut() {
                            obj.insert("tags".into(), Value::Array(current.clone()));
                        }
                    }
                }
                Some(item)
            })
            .collect();
        ok(paginate_body(items, "webhooks", &b, "NextToken")?)
    }

    fn register_webhook_with_third_party(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let account = self.account(req);
        if let Some(name) = str_field(&b, "webhookName") {
            let mut guard = self.state.write();
            let st = guard.get_or_create(&account);
            if !st.webhooks.contains_key(&name) {
                return Err(err(
                    "WebhookNotFoundException",
                    format!("Could not find webhook {name}."),
                ));
            }
        }
        ok(json!({}))
    }

    fn deregister_webhook_with_third_party(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let account = self.account(req);
        if let Some(name) = str_field(&b, "webhookName") {
            let mut guard = self.state.write();
            let st = guard.get_or_create(&account);
            if !st.webhooks.contains_key(&name) {
                return Err(err(
                    "WebhookNotFoundException",
                    format!("Could not find webhook {name}."),
                ));
            }
        }
        ok(json!({}))
    }
}

// ---------------------------------------------------------------------------
// Jobs + third-party jobs
// ---------------------------------------------------------------------------

impl CodePipelineService {
    fn acknowledge_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        req_str(&b, "jobId")?;
        req_str(&b, "nonce")?;
        Err(err(
            "JobNotFoundException",
            "The job was not found or is no longer available.",
        ))
    }

    fn acknowledge_third_party_job(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        req_str(&b, "jobId")?;
        req_str(&b, "nonce")?;
        req_str(&b, "clientToken")?;
        Err(err(
            "JobNotFoundException",
            "The job was not found or is no longer available.",
        ))
    }

    fn get_job_details(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        req_str(&b, "jobId")?;
        Err(err(
            "JobNotFoundException",
            "The job was not found or is no longer available.",
        ))
    }

    fn get_third_party_job_details(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        req_str(&b, "jobId")?;
        req_str(&b, "clientToken")?;
        Err(err(
            "JobNotFoundException",
            "The job was not found or is no longer available.",
        ))
    }

    fn poll_for_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        check_int_range(&b, "maxBatchSize", 1, None)?;
        if !b.get("actionTypeId").map(Value::is_object).unwrap_or(false) {
            return Err(validation("actionTypeId is required."));
        }
        // No job workers are queued; the poll drains empty.
        ok(json!({ "jobs": [] }))
    }

    fn poll_for_third_party_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        check_int_range(&b, "maxBatchSize", 1, None)?;
        if !b.get("actionTypeId").map(Value::is_object).unwrap_or(false) {
            return Err(validation("actionTypeId is required."));
        }
        ok(json!({ "jobs": [] }))
    }

    fn put_job_success_result(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        req_str(&b, "jobId")?;
        Err(err(
            "JobNotFoundException",
            "The job was not found or is no longer available.",
        ))
    }

    fn put_job_failure_result(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        req_str(&b, "jobId")?;
        let fd = b.get("failureDetails").filter(|v| v.is_object());
        let Some(fd) = fd else {
            return Err(validation("failureDetails is required."));
        };
        if let Some(t) = fd.get("type").and_then(Value::as_str) {
            if !validate::is_enum(validate::FAILURE_TYPE, t) {
                return Err(validation("The failure type is invalid."));
            }
        }
        Err(err(
            "JobNotFoundException",
            "The job was not found or is no longer available.",
        ))
    }

    fn put_third_party_job_success_result(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        req_str(&b, "jobId")?;
        req_str(&b, "clientToken")?;
        Err(err(
            "JobNotFoundException",
            "The job was not found or is no longer available.",
        ))
    }

    fn put_third_party_job_failure_result(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        req_str(&b, "jobId")?;
        req_str(&b, "clientToken")?;
        if !b
            .get("failureDetails")
            .map(Value::is_object)
            .unwrap_or(false)
        {
            return Err(validation("failureDetails is required."));
        }
        Err(err(
            "JobNotFoundException",
            "The job was not found or is no longer available.",
        ))
    }
}

// ---------------------------------------------------------------------------
// Rule types
// ---------------------------------------------------------------------------

impl CodePipelineService {
    fn list_rule_types(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        opt_enum(&b, "ruleOwnerFilter", validate::RULE_OWNER)?;
        check_len(&b, "regionFilter", 4, 30)?;
        // The predefined AWS-owned rule types, always resolvable.
        let rule_types = json!([
            {
                "id": { "category": "Rule", "owner": "AWS", "provider": "DeploymentWindow", "version": "1" },
                "inputArtifactDetails": { "minimumCount": 0, "maximumCount": 0 }
            },
            {
                "id": { "category": "Rule", "owner": "AWS", "provider": "LambdaInvoke", "version": "1" },
                "inputArtifactDetails": { "minimumCount": 0, "maximumCount": 5 }
            },
            {
                "id": { "category": "Rule", "owner": "AWS", "provider": "VariableCheck", "version": "1" },
                "inputArtifactDetails": { "minimumCount": 0, "maximumCount": 0 }
            }
        ]);
        ok(json!({ "ruleTypes": rule_types }))
    }
}

// ---------------------------------------------------------------------------
// Tagging
// ---------------------------------------------------------------------------

impl CodePipelineService {
    /// Whether an ARN names a currently-existing pipeline or webhook.
    fn resource_exists(&self, req: &AwsRequest, arn: &str) -> bool {
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // pipeline ARN: arn:aws:codepipeline:region:acct:<name>
        // webhook ARN:  arn:aws:codepipeline:region:acct:webhook:<name>
        let suffix = arn.splitn(6, ':').nth(5);
        match suffix {
            Some(s) if s.starts_with("webhook:") => {
                st.webhooks.contains_key(s.trim_start_matches("webhook:"))
            }
            // actiontype:<owner>/<category>/<provider>/<version>
            Some(s) if s.starts_with("actiontype:") => {
                let parts: Vec<&str> = s.trim_start_matches("actiontype:").split('/').collect();
                if parts.len() == 4 {
                    st.custom_actions
                        .contains_key(&action_key(parts[1], parts[2], parts[3]))
                } else {
                    false
                }
            }
            Some(s) => st.pipelines.contains_key(s),
            None => false,
        }
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn = req_str(&b, "resourceArn")?;
        if !is_codepipeline_arn(&arn) {
            return Err(err(
                "InvalidArnException",
                "The specified resource ARN is not valid.",
            ));
        }
        let tags = tag_list(&b);
        if !self.resource_exists(req, &arn) {
            return Err(err(
                "ResourceNotFoundException",
                format!("The resource with the ARN {arn} was not found."),
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let existing = st.tags.entry(arn).or_default();
        for tag in tags {
            let key = tag.get("key").and_then(Value::as_str).map(str::to_string);
            if let Some(key) = key {
                existing.retain(|t| t.get("key").and_then(Value::as_str) != Some(key.as_str()));
                existing.push(tag);
            }
        }
        ok(json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn = req_str(&b, "resourceArn")?;
        if !is_codepipeline_arn(&arn) {
            return Err(err(
                "InvalidArnException",
                "The specified resource ARN is not valid.",
            ));
        }
        if !self.resource_exists(req, &arn) {
            return Err(err(
                "ResourceNotFoundException",
                format!("The resource with the ARN {arn} was not found."),
            ));
        }
        let keys: Vec<String> = b
            .get("tagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if let Some(existing) = st.tags.get_mut(&arn) {
            existing.retain(|t| {
                t.get("key")
                    .and_then(Value::as_str)
                    .map(|k| !keys.iter().any(|rk| rk == k))
                    .unwrap_or(true)
            });
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn = req_str(&b, "resourceArn")?;
        if !is_codepipeline_arn(&arn) {
            return Err(err(
                "InvalidArnException",
                "The specified resource ARN is not valid.",
            ));
        }
        if !self.resource_exists(req, &arn) {
            return Err(err(
                "ResourceNotFoundException",
                format!("The resource with the ARN {arn} was not found."),
            ));
        }
        let account = self.account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let tags = st.tags.get(&arn).cloned().unwrap_or_default();
        ok(paginate_body(tags, "tags", &b, "nextToken")?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::CodePipelineState;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> CodePipelineService {
        let state = Arc::new(RwLock::new(MultiAccountState::<CodePipelineState>::new(
            "123456789012",
            "us-east-1",
            "",
        )));
        CodePipelineService::new(state)
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "codepipeline".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
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

    fn pipeline_decl(name: &str, mode: Option<&str>) -> Value {
        let mut decl = json!({
            "name": name,
            "roleArn": "arn:aws:iam::123456789012:role/cp",
            "artifactStore": { "type": "S3", "location": "artifact-bucket" },
            "stages": [
                { "name": "Source", "actions": [
                    { "name": "Src", "actionTypeId": { "category": "Source", "owner": "AWS", "provider": "S3", "version": "1" } } ] },
                { "name": "Build", "actions": [
                    { "name": "Bld", "actionTypeId": { "category": "Build", "owner": "AWS", "provider": "CodeBuild", "version": "1" } } ] }
            ]
        });
        if let Some(m) = mode {
            decl.as_object_mut()
                .unwrap()
                .insert("executionMode".into(), json!(m));
        }
        decl
    }

    async fn body_of(svc: &CodePipelineService, action: &str, body: Value) -> Value {
        let resp = svc.handle(req(action, body)).await.unwrap();
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    async fn create(svc: &CodePipelineService, name: &str, mode: Option<&str>) {
        svc.handle(req(
            "CreatePipeline",
            json!({ "pipeline": pipeline_decl(name, mode) }),
        ))
        .await
        .unwrap();
    }

    // Fix 1 + 9: settle state machine + returned changed-bool.
    #[test]
    fn settle_stopping_goes_to_stopped_not_succeeded() {
        let mut e = json!({ "status": "Stopping" });
        assert!(settle_execution(&mut e));
        assert_eq!(e["status"], json!("Stopped"));
    }

    #[test]
    fn settle_in_progress_goes_to_succeeded() {
        let mut e = json!({ "status": "InProgress" });
        assert!(settle_execution(&mut e));
        assert_eq!(e["status"], json!("Succeeded"));
    }

    #[test]
    fn settle_terminal_is_never_resettled() {
        for terminal in TERMINAL_EXECUTION_STATUSES {
            let mut e = json!({ "status": terminal });
            assert!(!settle_execution(&mut e), "{terminal} must stay terminal");
            assert_eq!(e["status"], json!(*terminal));
        }
    }

    // Fix 5: SUPERSEDED mode supersedes older in-flight executions.
    #[test]
    fn superseded_mode_supersedes_all_but_latest() {
        let mut execs = std::collections::BTreeMap::new();
        execs.insert("a".to_string(), json!({ "status": "InProgress" }));
        execs.insert("b".to_string(), json!({ "status": "InProgress" }));
        let order = vec!["a".to_string(), "b".to_string()];
        assert!(settle_pipeline_executions(&mut execs, &order, "SUPERSEDED"));
        assert_eq!(execs["a"]["status"], json!("Superseded"));
        assert_eq!(execs["b"]["status"], json!("Succeeded"));
    }

    #[test]
    fn superseded_mode_leaves_stopping_older_execution_to_stop() {
        // An older execution the user explicitly stopped must settle to
        // `Stopped`, not be force-superseded by the newer run.
        let mut execs = std::collections::BTreeMap::new();
        execs.insert("a".to_string(), json!({ "status": "Stopping" }));
        execs.insert("b".to_string(), json!({ "status": "InProgress" }));
        let order = vec!["a".to_string(), "b".to_string()];
        assert!(settle_pipeline_executions(&mut execs, &order, "SUPERSEDED"));
        assert_eq!(execs["a"]["status"], json!("Stopped"));
        assert_eq!(execs["b"]["status"], json!("Succeeded"));
    }

    #[test]
    fn parallel_mode_settles_each_independently() {
        let mut execs = std::collections::BTreeMap::new();
        execs.insert("a".to_string(), json!({ "status": "InProgress" }));
        execs.insert("b".to_string(), json!({ "status": "InProgress" }));
        let order = vec!["a".to_string(), "b".to_string()];
        assert!(settle_pipeline_executions(&mut execs, &order, "PARALLEL"));
        assert_eq!(execs["a"]["status"], json!("Succeeded"));
        assert_eq!(execs["b"]["status"], json!("Succeeded"));
        // Second pass is a no-op (all terminal) -> gates a snapshot off.
        assert!(!settle_pipeline_executions(&mut execs, &order, "PARALLEL"));
    }

    // Fix 4: artifact-store structural validation.
    #[test]
    fn pipeline_requires_exactly_one_artifact_store() {
        let mut no_store = pipeline_decl("p", None);
        no_store.as_object_mut().unwrap().remove("artifactStore");
        assert_eq!(
            validate_pipeline_declaration(&no_store).unwrap_err().code(),
            "InvalidStructureException"
        );

        let mut both = pipeline_decl("p", None);
        both.as_object_mut().unwrap().insert(
            "artifactStores".into(),
            json!({ "us-east-1": { "type": "S3", "location": "b" } }),
        );
        assert_eq!(
            validate_pipeline_declaration(&both).unwrap_err().code(),
            "InvalidStructureException"
        );

        assert!(validate_pipeline_declaration(&pipeline_decl("p", None)).is_ok());
    }

    // Fix 2: AWS-owned action-type catalog.
    #[tokio::test]
    async fn list_action_types_returns_aws_owned_catalog() {
        let svc = svc();
        let all = body_of(&svc, "ListActionTypes", json!({})).await;
        let types = all["actionTypes"].as_array().unwrap();
        let providers: Vec<&str> = types
            .iter()
            .filter_map(|t| t["id"]["provider"].as_str())
            .collect();
        assert!(providers.contains(&"CodeBuild"));
        assert!(providers.contains(&"CodeDeploy"));
        assert!(providers.contains(&"Lambda"));
        // owner=Custom filter excludes the AWS built-ins.
        let custom = body_of(
            &svc,
            "ListActionTypes",
            json!({ "actionOwnerFilter": "Custom" }),
        )
        .await;
        assert!(custom["actionTypes"].as_array().unwrap().is_empty());
    }

    // Fix 3: transition state persists into GetPipelineState.
    #[tokio::test]
    async fn disable_then_enable_stage_transition_reflected_in_state() {
        let svc = svc();
        create(&svc, "p", None).await;
        svc.handle(req(
            "DisableStageTransition",
            json!({ "pipelineName": "p", "stageName": "Build", "transitionType": "Inbound", "reason": "hold" }),
        ))
        .await
        .unwrap();
        let state = body_of(&svc, "GetPipelineState", json!({ "name": "p" })).await;
        let build = state["stageStates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["stageName"] == json!("Build"))
            .unwrap();
        assert_eq!(build["inboundTransitionState"]["enabled"], json!(false));
        assert_eq!(
            build["inboundTransitionState"]["disabledReason"],
            json!("hold")
        );
        // Source stage stays enabled.
        let source = state["stageStates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["stageName"] == json!("Source"))
            .unwrap();
        assert_eq!(source["inboundTransitionState"]["enabled"], json!(true));

        svc.handle(req(
            "EnableStageTransition",
            json!({ "pipelineName": "p", "stageName": "Build", "transitionType": "Inbound" }),
        ))
        .await
        .unwrap();
        let state = body_of(&svc, "GetPipelineState", json!({ "name": "p" })).await;
        let build = state["stageStates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["stageName"] == json!("Build"))
            .unwrap();
        assert_eq!(build["inboundTransitionState"]["enabled"], json!(true));
    }

    // Fix 6: statusSummary survives into GetPipelineExecution after a stop.
    #[tokio::test]
    async fn get_pipeline_execution_includes_status_summary() {
        let svc = svc();
        create(&svc, "p", None).await;
        let started = body_of(&svc, "StartPipelineExecution", json!({ "name": "p" })).await;
        let exec_id = started["pipelineExecutionId"].as_str().unwrap().to_string();
        svc.handle(req(
            "StopPipelineExecution",
            json!({ "pipelineName": "p", "pipelineExecutionId": exec_id, "reason": "manual stop" }),
        ))
        .await
        .unwrap();
        let got = body_of(
            &svc,
            "GetPipelineExecution",
            json!({ "pipelineName": "p", "pipelineExecutionId": exec_id }),
        )
        .await;
        assert_eq!(got["pipelineExecution"]["status"], json!("Stopped"));
        assert_eq!(
            got["pipelineExecution"]["statusSummary"],
            json!("manual stop")
        );
    }

    // Fix 7: webhook tags read from st.tags (single source of truth).
    #[tokio::test]
    async fn webhook_tags_reflect_tag_resource_changes() {
        let svc = svc();
        create(&svc, "p", None).await;
        svc.handle(req(
            "PutWebhook",
            json!({ "webhook": {
                "name": "wh", "targetPipeline": "p", "targetAction": "Src",
                "filters": [{ "jsonPath": "$.ref" }],
                "authentication": "UNAUTHENTICATED",
                "authenticationConfiguration": {} } }),
        ))
        .await
        .unwrap();
        let arn = "arn:aws:codepipeline:us-east-1:123456789012:webhook:wh";
        svc.handle(req(
            "TagResource",
            json!({ "resourceArn": arn, "tags": [{ "key": "env", "value": "prod" }] }),
        ))
        .await
        .unwrap();
        let listed = body_of(&svc, "ListWebhooks", json!({})).await;
        let wh = &listed["webhooks"][0];
        assert_eq!(wh["tags"], json!([{ "key": "env", "value": "prod" }]));

        svc.handle(req(
            "UntagResource",
            json!({ "resourceArn": arn, "tagKeys": ["env"] }),
        ))
        .await
        .unwrap();
        let listed = body_of(&svc, "ListWebhooks", json!({})).await;
        // With no tags, the field is omitted (single source of truth is empty).
        assert!(listed["webhooks"][0].get("tags").is_none());
    }

    // Fix 8: a settling read that changes nothing must not report a change.
    #[tokio::test]
    async fn settled_read_reports_no_further_change() {
        let svc = svc();
        create(&svc, "p", None).await;
        let started = body_of(&svc, "StartPipelineExecution", json!({ "name": "p" })).await;
        let exec_id = started["pipelineExecutionId"].as_str().unwrap().to_string();
        // First read settles InProgress -> Succeeded (changes state).
        let changed1 = std::cell::Cell::new(false);
        svc.get_pipeline_execution(
            &req(
                "GetPipelineExecution",
                json!({ "pipelineName": "p", "pipelineExecutionId": exec_id }),
            ),
            &changed1,
        )
        .unwrap();
        assert!(changed1.get(), "first read should settle and report change");
        // Second read is a no-op: execution is terminal, nothing to persist.
        let changed2 = std::cell::Cell::new(false);
        svc.get_pipeline_execution(
            &req(
                "GetPipelineExecution",
                json!({ "pipelineName": "p", "pipelineExecutionId": exec_id }),
            ),
            &changed2,
        )
        .unwrap();
        assert!(!changed2.get(), "settled read must not report a change");
    }
}
