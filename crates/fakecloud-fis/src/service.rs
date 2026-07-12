//! AWS FIS (`fis`) restJson1 dispatch + operation handlers.
//!
//! The full 26-operation AWS FIS control plane. Requests are routed to an
//! operation by HTTP method + `@http` URI path; path labels are captured
//! positionally (percent-decoded) and query parameters are read from the raw
//! query string so repeated multi-value keys survive. State is
//! account-partitioned and persisted; each experiment template / experiment is
//! stored as its already-output-valid wire object so `GetExperimentTemplate` /
//! `GetExperiment` echo exactly what was persisted, and the experiment lifecycle
//! (`initiating` -> `running` -> `completed`, `stopping` -> `stopped`) settles on
//! the next read.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{set_experiment_status, FisData, SharedFisState};
use crate::{catalog, shared};

/// Every operation name in the AWS FIS Smithy model (26 operations).
pub const FIS_ACTIONS: &[&str] = &[
    "CreateExperimentTemplate",
    "CreateTargetAccountConfiguration",
    "DeleteExperimentTemplate",
    "DeleteTargetAccountConfiguration",
    "GetAction",
    "GetExperiment",
    "GetExperimentTargetAccountConfiguration",
    "GetExperimentTemplate",
    "GetSafetyLever",
    "GetTargetAccountConfiguration",
    "GetTargetResourceType",
    "ListActions",
    "ListExperimentResolvedTargets",
    "ListExperimentTargetAccountConfigurations",
    "ListExperimentTemplates",
    "ListExperiments",
    "ListTagsForResource",
    "ListTargetAccountConfigurations",
    "ListTargetResourceTypes",
    "StartExperiment",
    "StopExperiment",
    "TagResource",
    "UntagResource",
    "UpdateExperimentTemplate",
    "UpdateSafetyLeverState",
    "UpdateTargetAccountConfiguration",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &[
    "CreateExperimentTemplate",
    "UpdateExperimentTemplate",
    "DeleteExperimentTemplate",
    "StartExperiment",
    "StopExperiment",
    "CreateTargetAccountConfiguration",
    "UpdateTargetAccountConfiguration",
    "DeleteTargetAccountConfiguration",
    "UpdateSafetyLeverState",
    "TagResource",
    "UntagResource",
];

pub struct FisService {
    state: SharedFisState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl FisService {
    pub fn new(state: SharedFisState) -> Self {
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
        save_snapshot(
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

    /// Step any in-flight experiment lifecycle for the account before a handler
    /// reads state. Returns `true` if a transition fired (so the caller persists).
    fn reconcile(&self, account: &str) -> bool {
        let mut guard = self.state.write();
        guard.get_or_create(account).reconcile_experiments()
    }

    /// Route a request to an operation name + captured path labels by HTTP
    /// method + `@http` URI path. Returns `None` when no route matches.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<String>)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let segs: Vec<String> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .split('/')
                .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
                .collect()
        };
        let s: Vec<&str> = segs.iter().map(String::as_str).collect();
        let m = &req.method;
        let get = m == Method::GET;
        let post = m == Method::POST;
        let patch = m == Method::PATCH;
        let del = m == Method::DELETE;
        let l1 = |a: &str| vec![a.to_string()];
        let l2 = |a: &str, b: &str| vec![a.to_string(), b.to_string()];
        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            ["experimentTemplates"] if post => ("CreateExperimentTemplate", vec![]),
            ["experimentTemplates"] if get => ("ListExperimentTemplates", vec![]),
            ["experimentTemplates", id] if get => ("GetExperimentTemplate", l1(id)),
            ["experimentTemplates", id] if patch => ("UpdateExperimentTemplate", l1(id)),
            ["experimentTemplates", id] if del => ("DeleteExperimentTemplate", l1(id)),
            ["experimentTemplates", tid, "targetAccountConfigurations"] if get => {
                ("ListTargetAccountConfigurations", l1(tid))
            }
            ["experimentTemplates", tid, "targetAccountConfigurations", aid] if post => {
                ("CreateTargetAccountConfiguration", l2(tid, aid))
            }
            ["experimentTemplates", tid, "targetAccountConfigurations", aid] if get => {
                ("GetTargetAccountConfiguration", l2(tid, aid))
            }
            ["experimentTemplates", tid, "targetAccountConfigurations", aid] if patch => {
                ("UpdateTargetAccountConfiguration", l2(tid, aid))
            }
            ["experimentTemplates", tid, "targetAccountConfigurations", aid] if del => {
                ("DeleteTargetAccountConfiguration", l2(tid, aid))
            }
            ["experiments"] if post => ("StartExperiment", vec![]),
            ["experiments"] if get => ("ListExperiments", vec![]),
            ["experiments", id] if get => ("GetExperiment", l1(id)),
            ["experiments", id] if del => ("StopExperiment", l1(id)),
            ["experiments", eid, "resolvedTargets"] if get => {
                ("ListExperimentResolvedTargets", l1(eid))
            }
            ["experiments", eid, "targetAccountConfigurations"] if get => {
                ("ListExperimentTargetAccountConfigurations", l1(eid))
            }
            ["experiments", eid, "targetAccountConfigurations", aid] if get => {
                ("GetExperimentTargetAccountConfiguration", l2(eid, aid))
            }
            ["actions"] if get => ("ListActions", vec![]),
            ["actions", id] if get => ("GetAction", l1(id)),
            ["targetResourceTypes"] if get => ("ListTargetResourceTypes", vec![]),
            ["targetResourceTypes", rt] if get => ("GetTargetResourceType", l1(rt)),
            ["safetyLevers", id] if get => ("GetSafetyLever", l1(id)),
            ["safetyLevers", id, "state"] if patch => ("UpdateSafetyLeverState", l1(id)),
            ["tags", arn] if post => ("TagResource", l1(arn)),
            ["tags", arn] if get => ("ListTagsForResource", l1(arn)),
            ["tags", arn] if del => ("UntagResource", l1(arn)),
            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for FisService {
    fn service_name(&self) -> &str {
        "fis"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some((action, labels)) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let (result, settled) = self.dispatch(action, &labels, &req);
        let success = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        if settled || (MUTATING.contains(&action) && success) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        FIS_ACTIONS
    }
}

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

impl FisService {
    fn dispatch(
        &self,
        action: &str,
        labels: &[String],
        req: &AwsRequest,
    ) -> (Result<AwsResponse, AwsServiceError>, bool) {
        let body = match parse_body(req) {
            Ok(b) => b,
            Err(e) => return (Err(e), false),
        };
        if let Err(e) = crate::validate::validate_input(action, &body) {
            return (Err(e), false);
        }
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: req.region.clone(),
        };
        let q = parse_query(&req.raw_query);
        // Settle any in-flight lifecycle transition before the handler reads
        // state, and remember whether it fired so the change is persisted even
        // for a pure read.
        let settled = self.reconcile(&ctx.account);
        let a0 = labels.first().map(String::as_str).unwrap_or_default();
        let a1 = labels.get(1).map(String::as_str).unwrap_or_default();
        let result = match action {
            "CreateExperimentTemplate" => self.create_experiment_template(&ctx, &body),
            "UpdateExperimentTemplate" => self.update_experiment_template(&ctx, a0, &body),
            "DeleteExperimentTemplate" => self.delete_experiment_template(&ctx, a0),
            "GetExperimentTemplate" => self.get_experiment_template(&ctx, a0),
            "ListExperimentTemplates" => self.list_experiment_templates(&ctx, &q),
            "StartExperiment" => self.start_experiment(&ctx, &body),
            "StopExperiment" => self.stop_experiment(&ctx, a0),
            "GetExperiment" => self.get_experiment(&ctx, a0),
            "ListExperiments" => self.list_experiments(&ctx, &q),
            "ListExperimentResolvedTargets" => self.list_resolved_targets(&ctx, a0, &q),
            "ListActions" => self.list_actions(&ctx, &q),
            "GetAction" => self.get_action(&ctx, a0),
            "ListTargetResourceTypes" => self.list_target_resource_types(&q),
            "GetTargetResourceType" => self.get_target_resource_type(a0),
            "CreateTargetAccountConfiguration" => self.create_tac(&ctx, a0, a1, &body),
            "UpdateTargetAccountConfiguration" => self.update_tac(&ctx, a0, a1, &body),
            "DeleteTargetAccountConfiguration" => self.delete_tac(&ctx, a0, a1),
            "GetTargetAccountConfiguration" => self.get_tac(&ctx, a0, a1),
            "ListTargetAccountConfigurations" => self.list_tac(&ctx, a0, &q),
            "GetExperimentTargetAccountConfiguration" => self.get_experiment_tac(&ctx, a0, a1),
            "ListExperimentTargetAccountConfigurations" => self.list_experiment_tac(&ctx, a0, &q),
            "GetSafetyLever" => self.get_safety_lever(&ctx, a0),
            "UpdateSafetyLeverState" => self.update_safety_lever_state(&ctx, a0, &body),
            "TagResource" => self.tag_resource(&ctx, a0, &body),
            "UntagResource" => self.untag_resource(&ctx, a0, &q),
            "ListTagsForResource" => self.list_tags_for_resource(&ctx, a0),
            _ => Err(AwsServiceError::action_not_implemented("fis", action)),
        };
        (result, settled)
    }
}

// ===================== helpers =====================

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| validation(&format!("Request body is malformed: {e}")))
}

fn validation(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFoundException", msg)
}

fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_decode_str(k).decode_utf8_lossy().into_owned(),
                percent_decode_str(v).decode_utf8_lossy().into_owned(),
            )
        })
        .collect()
}

fn query_one<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

fn query_all(q: &[(String, String)], key: &str) -> Vec<String> {
    q.iter()
        .filter(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .collect()
}

/// Paginate a slice of wire objects by the `maxResults` / `nextToken`
/// (numeric-offset) query params. Returns the page plus an optional next token.
fn paginate(items: &[Value], q: &[(String, String)]) -> (Vec<Value>, Option<String>) {
    let max = query_one(q, "maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(usize::MAX);
    let start = query_one(q, "nextToken")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let end = start.saturating_add(max).min(items.len());
    let page = items.get(start..end).unwrap_or(&[]).to_vec();
    let next = (end < items.len()).then(|| end.to_string());
    (page, next)
}

/// A required, non-empty path label; rejects an empty label (from a too-short
/// probe path) with `ValidationException`.
fn require_label(name: &str, what: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() {
        Err(validation(&format!("{what} must not be empty.")))
    } else {
        Ok(())
    }
}

/// Validate the shared `maxResults` (range `[1, 100]`) and `nextToken`
/// (length `[1, 1024]`) pagination query parameters against the model, rejecting
/// out-of-range values with `ValidationException`. A parameter that is absent is
/// unconstrained.
fn validate_pagination(q: &[(String, String)]) -> Result<(), AwsServiceError> {
    if let Some((_, v)) = q.iter().find(|(k, _)| k == "maxResults") {
        match v.parse::<i64>() {
            Ok(n) if (1..=100).contains(&n) => {}
            _ => {
                return Err(validation(
                    "maxResults must be an integer in the range [1, 100].",
                ))
            }
        }
    }
    if let Some((_, v)) = q.iter().find(|(k, _)| k == "nextToken") {
        let len = v.chars().count();
        if !(1..=1024).contains(&len) {
            return Err(validation(
                "nextToken length is outside the valid range [1, 1024].",
            ));
        }
    }
    Ok(())
}

/// Reject a query parameter whose value exceeds `max` characters with
/// `ValidationException` (used for the `experimentTemplateId` list filter).
fn validate_query_len(
    q: &[(String, String)],
    key: &str,
    max: usize,
) -> Result<(), AwsServiceError> {
    if let Some((_, v)) = q.iter().find(|(k, _)| k == key) {
        if v.chars().count() > max {
            return Err(validation(&format!(
                "{key} length exceeds the maximum of {max}."
            )));
        }
    }
    Ok(())
}

/// Validate a tagging operation's `resourceArn` path label against the
/// `ResourceArn` `@length` constraint (`[20, 2048]`). A well-formed but unknown
/// ARN is accepted (the tag operation is then a no-op); only a malformed ARN is
/// rejected, with `ValidationException`.
fn validate_resource_arn(arn: &str) -> Result<(), AwsServiceError> {
    let len = arn.chars().count();
    if !(20..=2048).contains(&len) {
        return Err(validation(
            "resourceArn length is outside the valid range [20, 2048].",
        ));
    }
    Ok(())
}

// ===================== experiment templates =====================

impl FisService {
    fn create_experiment_template(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = shared::new_template_id();
        let arn = shared::experiment_template_arn(&ctx.region, &ctx.account, &id);
        let now = shared::now_epoch();

        let mut tpl = Map::new();
        tpl.insert("id".into(), json!(id));
        tpl.insert("arn".into(), json!(arn));
        tpl.insert(
            "description".into(),
            body.get("description").cloned().unwrap_or(Value::Null),
        );
        tpl.insert(
            "targets".into(),
            body.get("targets").cloned().unwrap_or(json!({})),
        );
        tpl.insert(
            "actions".into(),
            body.get("actions").cloned().unwrap_or(json!({})),
        );
        tpl.insert(
            "stopConditions".into(),
            body.get("stopConditions").cloned().unwrap_or(json!([])),
        );
        tpl.insert("creationTime".into(), json!(now));
        tpl.insert("lastUpdateTime".into(), json!(now));
        tpl.insert(
            "roleArn".into(),
            body.get("roleArn").cloned().unwrap_or(Value::Null),
        );
        tpl.insert(
            "tags".into(),
            body.get("tags").cloned().unwrap_or(json!({})),
        );
        if let Some(lc) = body.get("logConfiguration") {
            tpl.insert("logConfiguration".into(), lc.clone());
        }
        tpl.insert(
            "experimentOptions".into(),
            build_experiment_options(body.get("experimentOptions"), None),
        );
        tpl.insert("targetAccountConfigurationsCount".into(), json!(0));
        if let Some(rc) = body.get("experimentReportConfiguration") {
            tpl.insert("experimentReportConfiguration".into(), rc.clone());
        }

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.templates
            .insert(id.clone(), Value::Object(tpl.clone()));
        ok(json!({ "experimentTemplate": Value::Object(tpl) }))
    }

    fn get_experiment_template(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(id, "id")?;
        let guard = self.state.read();
        let tpl = guard
            .get(&ctx.account)
            .and_then(|d| d.templates.get(id))
            .ok_or_else(|| not_found(&format!("Experiment template {id} does not exist.")))?;
        ok(json!({ "experimentTemplate": tpl }))
    }

    fn update_experiment_template(
        &self,
        ctx: &Ctx,
        id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(id, "id")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(tpl) = data.templates.get_mut(id).and_then(Value::as_object_mut) else {
            return Err(not_found(&format!(
                "Experiment template {id} does not exist."
            )));
        };
        for key in [
            "description",
            "stopConditions",
            "targets",
            "actions",
            "roleArn",
            "logConfiguration",
            "experimentReportConfiguration",
        ] {
            if let Some(v) = body.get(key) {
                tpl.insert(key.to_string(), v.clone());
            }
        }
        if body.get("experimentOptions").is_some() {
            let existing = tpl.get("experimentOptions").cloned();
            tpl.insert(
                "experimentOptions".into(),
                build_experiment_options(body.get("experimentOptions"), existing.as_ref()),
            );
        }
        tpl.insert("lastUpdateTime".into(), json!(shared::now_epoch()));
        ok(json!({ "experimentTemplate": Value::Object(tpl.clone()) }))
    }

    fn delete_experiment_template(
        &self,
        ctx: &Ctx,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(id, "id")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(removed) = data.templates.remove(id) else {
            return Err(not_found(&format!(
                "Experiment template {id} does not exist."
            )));
        };
        data.target_account_configs.remove(id);
        ok(json!({ "experimentTemplate": removed }))
    }

    fn list_experiment_templates(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_pagination(q)?;
        let guard = self.state.read();
        let summaries: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.templates.values().map(template_summary).collect())
            .unwrap_or_default();
        let (page, next) = paginate(&summaries, q);
        let mut out = json!({ "experimentTemplates": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }
}

// ===================== experiments =====================

impl FisService {
    fn start_experiment(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let template_id = body
            .get("experimentTemplateId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(tpl) = data.templates.get(&template_id).and_then(Value::as_object) else {
            return Err(not_found(&format!(
                "Experiment template {template_id} does not exist."
            )));
        };
        let tpl = tpl.clone();

        let id = shared::new_experiment_id();
        let arn = shared::experiment_arn(&ctx.region, &ctx.account, &id);
        let now = shared::now_epoch();

        // Copy the template's actions, giving each a starting action state.
        let mut actions = Map::new();
        if let Some(tpl_actions) = tpl.get("actions").and_then(Value::as_object) {
            for (name, a) in tpl_actions {
                let mut a = a.as_object().cloned().unwrap_or_default();
                a.insert(
                    "state".into(),
                    json!({ "status": "pending", "reason": "Experiment is pending." }),
                );
                actions.insert(name.clone(), Value::Object(a));
            }
        }

        let tac_count = data
            .target_account_configs
            .get(&template_id)
            .map(|m| m.len())
            .unwrap_or(0);

        let mut exp = Map::new();
        exp.insert("id".into(), json!(id));
        exp.insert("arn".into(), json!(arn));
        exp.insert("experimentTemplateId".into(), json!(template_id));
        exp.insert(
            "roleArn".into(),
            tpl.get("roleArn").cloned().unwrap_or(Value::Null),
        );
        exp.insert(
            "state".into(),
            json!({ "status": "initiating", "reason": "Experiment is initiating." }),
        );
        exp.insert(
            "targets".into(),
            tpl.get("targets").cloned().unwrap_or(json!({})),
        );
        exp.insert("actions".into(), Value::Object(actions));
        exp.insert(
            "stopConditions".into(),
            tpl.get("stopConditions").cloned().unwrap_or(json!([])),
        );
        exp.insert("creationTime".into(), json!(now));
        exp.insert("startTime".into(), json!(now));
        exp.insert(
            "tags".into(),
            body.get("tags").cloned().unwrap_or(json!({})),
        );
        if let Some(lc) = tpl.get("logConfiguration") {
            exp.insert("logConfiguration".into(), lc.clone());
        }
        let actions_mode = body
            .get("experimentOptions")
            .and_then(|o| o.get("actionsMode"))
            .and_then(Value::as_str)
            .unwrap_or("run-all");
        exp.insert(
            "experimentOptions".into(),
            build_experiment_options(None, tpl.get("experimentOptions"))
                .as_object()
                .cloned()
                .map(|mut o| {
                    o.insert("actionsMode".into(), json!(actions_mode));
                    Value::Object(o)
                })
                .unwrap_or(Value::Null),
        );
        exp.insert("targetAccountConfigurationsCount".into(), json!(tac_count));
        if let Some(rc) = tpl.get("experimentReportConfiguration") {
            exp.insert("experimentReportConfiguration".into(), rc.clone());
        }

        // Copy per-template target-account configs onto the experiment.
        if let Some(configs) = data.target_account_configs.get(&template_id).cloned() {
            data.experiment_target_account_configs
                .insert(id.clone(), configs);
        }

        data.experiments
            .insert(id.clone(), Value::Object(exp.clone()));
        ok(json!({ "experiment": Value::Object(exp) }))
    }

    fn get_experiment(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(id, "id")?;
        let guard = self.state.read();
        let exp = guard
            .get(&ctx.account)
            .and_then(|d| d.experiments.get(id))
            .ok_or_else(|| not_found(&format!("Experiment {id} does not exist.")))?;
        ok(json!({ "experiment": exp }))
    }

    fn stop_experiment(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(id, "id")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(exp) = data.experiments.get_mut(id).and_then(Value::as_object_mut) else {
            return Err(not_found(&format!("Experiment {id} does not exist.")));
        };
        let status = exp
            .get("state")
            .and_then(|s| s.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("");
        // Only a running / initiating / pending experiment can be asked to stop;
        // a terminal experiment is returned unchanged (StopExperiment is a
        // no-op once the experiment has settled).
        if matches!(status, "pending" | "initiating" | "running") {
            set_experiment_status(exp, "stopping", "Experiment is stopping.");
        }
        ok(json!({ "experiment": Value::Object(exp.clone()) }))
    }

    fn list_experiments(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_pagination(q)?;
        validate_query_len(q, "experimentTemplateId", 64)?;
        let template_filter = query_one(q, "experimentTemplateId");
        let guard = self.state.read();
        let summaries: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.experiments
                    .values()
                    .filter(|e| {
                        template_filter.is_none_or(|t| {
                            e.get("experimentTemplateId").and_then(Value::as_str) == Some(t)
                        })
                    })
                    .map(experiment_summary)
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&summaries, q);
        let mut out = json!({ "experiments": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn list_resolved_targets(
        &self,
        ctx: &Ctx,
        experiment_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(experiment_id, "experimentId")?;
        let target_filter = query_one(q, "targetName");
        let guard = self.state.read();
        let exp = guard
            .get(&ctx.account)
            .and_then(|d| d.experiments.get(experiment_id))
            .ok_or_else(|| not_found(&format!("Experiment {experiment_id} does not exist.")))?;
        let mut resolved: Vec<Value> = Vec::new();
        if let Some(targets) = exp.get("targets").and_then(Value::as_object) {
            for (name, target) in targets {
                if target_filter.is_some_and(|f| f != name) {
                    continue;
                }
                let resource_type = target.get("resourceType").cloned().unwrap_or(Value::Null);
                if let Some(arns) = target.get("resourceArns").and_then(Value::as_array) {
                    for arn in arns {
                        let mut info = Map::new();
                        if let Some(a) = arn.as_str() {
                            info.insert("ResourceArn".into(), json!(a));
                        }
                        resolved.push(json!({
                            "resourceType": resource_type,
                            "targetName": name,
                            "targetInformation": Value::Object(info),
                        }));
                    }
                }
            }
        }
        let (page, next) = paginate(&resolved, q);
        let mut out = json!({ "resolvedTargets": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }
}

// ===================== actions + target resource types catalog =====================

impl FisService {
    fn list_actions(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_pagination(q)?;
        let all = catalog::list_action_summaries(&ctx.region);
        let (page, next) = paginate(&all, q);
        let mut out = json!({ "actions": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn get_action(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(id, "id")?;
        let action = catalog::get_action(&ctx.region, id)
            .ok_or_else(|| not_found(&format!("Action {id} does not exist.")))?;
        ok(json!({ "action": action }))
    }

    fn list_target_resource_types(
        &self,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_pagination(q)?;
        let all = catalog::list_resource_type_summaries();
        let (page, next) = paginate(&all, q);
        let mut out = json!({ "targetResourceTypes": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn get_target_resource_type(
        &self,
        resource_type: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(resource_type, "resourceType")?;
        let rt = catalog::get_resource_type(resource_type).ok_or_else(|| {
            not_found(&format!(
                "Target resource type {resource_type} does not exist."
            ))
        })?;
        ok(json!({ "targetResourceType": rt }))
    }
}

// ===================== target account configurations =====================

impl FisService {
    fn require_template(data: &FisData, template_id: &str) -> Result<(), AwsServiceError> {
        if data.templates.contains_key(template_id) {
            Ok(())
        } else {
            Err(not_found(&format!(
                "Experiment template {template_id} does not exist."
            )))
        }
    }

    fn tac_value(account_id: &str, role_arn: &Value, description: &Value) -> Value {
        let mut m = Map::new();
        m.insert("accountId".into(), json!(account_id));
        m.insert("roleArn".into(), role_arn.clone());
        if !description.is_null() {
            m.insert("description".into(), description.clone());
        }
        Value::Object(m)
    }

    fn refresh_tac_count(data: &mut FisData, template_id: &str) {
        let count = data
            .target_account_configs
            .get(template_id)
            .map(|m| m.len())
            .unwrap_or(0);
        if let Some(tpl) = data
            .templates
            .get_mut(template_id)
            .and_then(Value::as_object_mut)
        {
            tpl.insert("targetAccountConfigurationsCount".into(), json!(count));
        }
    }

    fn create_tac(
        &self,
        ctx: &Ctx,
        template_id: &str,
        account_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(template_id, "experimentTemplateId")?;
        require_label(account_id, "accountId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        Self::require_template(data, template_id)?;
        let entry = data
            .target_account_configs
            .entry(template_id.to_string())
            .or_default();
        if entry.contains_key(account_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ConflictException",
                format!("A target account configuration for account {account_id} already exists."),
            ));
        }
        let value = Self::tac_value(
            account_id,
            body.get("roleArn").unwrap_or(&Value::Null),
            body.get("description").unwrap_or(&Value::Null),
        );
        entry.insert(account_id.to_string(), value.clone());
        Self::refresh_tac_count(data, template_id);
        ok(json!({ "targetAccountConfiguration": value }))
    }

    fn update_tac(
        &self,
        ctx: &Ctx,
        template_id: &str,
        account_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(template_id, "experimentTemplateId")?;
        require_label(account_id, "accountId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        Self::require_template(data, template_id)?;
        let Some(existing) = data
            .target_account_configs
            .get(template_id)
            .and_then(|m| m.get(account_id))
            .cloned()
        else {
            return Err(not_found(&format!(
                "Target account configuration for account {account_id} does not exist."
            )));
        };
        let role_arn = body
            .get("roleArn")
            .cloned()
            .unwrap_or_else(|| existing.get("roleArn").cloned().unwrap_or(Value::Null));
        let description = body
            .get("description")
            .cloned()
            .unwrap_or_else(|| existing.get("description").cloned().unwrap_or(Value::Null));
        let value = Self::tac_value(account_id, &role_arn, &description);
        data.target_account_configs
            .entry(template_id.to_string())
            .or_default()
            .insert(account_id.to_string(), value.clone());
        ok(json!({ "targetAccountConfiguration": value }))
    }

    fn delete_tac(
        &self,
        ctx: &Ctx,
        template_id: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(template_id, "experimentTemplateId")?;
        require_label(account_id, "accountId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        Self::require_template(data, template_id)?;
        let removed = data
            .target_account_configs
            .get_mut(template_id)
            .and_then(|m| m.remove(account_id));
        if removed.is_none() {
            return Err(not_found(&format!(
                "Target account configuration for account {account_id} does not exist."
            )));
        }
        Self::refresh_tac_count(data, template_id);
        ok(json!({ "targetAccountConfiguration": {} }))
    }

    fn get_tac(
        &self,
        ctx: &Ctx,
        template_id: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(template_id, "experimentTemplateId")?;
        require_label(account_id, "accountId")?;
        let guard = self.state.read();
        let data = guard.get(&ctx.account).ok_or_else(|| {
            not_found(&format!(
                "Experiment template {template_id} does not exist."
            ))
        })?;
        Self::require_template(data, template_id)?;
        let value = data
            .target_account_configs
            .get(template_id)
            .and_then(|m| m.get(account_id))
            .ok_or_else(|| {
                not_found(&format!(
                    "Target account configuration for account {account_id} does not exist."
                ))
            })?;
        ok(json!({ "targetAccountConfiguration": value }))
    }

    fn list_tac(
        &self,
        ctx: &Ctx,
        template_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(template_id, "experimentTemplateId")?;
        let guard = self.state.read();
        let data = guard.get(&ctx.account).ok_or_else(|| {
            not_found(&format!(
                "Experiment template {template_id} does not exist."
            ))
        })?;
        Self::require_template(data, template_id)?;
        let all: Vec<Value> = data
            .target_account_configs
            .get(template_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(&all, q);
        let mut out = json!({ "targetAccountConfigurations": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn get_experiment_tac(
        &self,
        ctx: &Ctx,
        experiment_id: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(experiment_id, "experimentId")?;
        require_label(account_id, "accountId")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Experiment {experiment_id} does not exist.")))?;
        if !data.experiments.contains_key(experiment_id) {
            return Err(not_found(&format!(
                "Experiment {experiment_id} does not exist."
            )));
        }
        let value = data
            .experiment_target_account_configs
            .get(experiment_id)
            .and_then(|m| m.get(account_id))
            .ok_or_else(|| {
                not_found(&format!(
                    "Target account configuration for account {account_id} does not exist."
                ))
            })?;
        ok(json!({ "targetAccountConfiguration": value }))
    }

    fn list_experiment_tac(
        &self,
        ctx: &Ctx,
        experiment_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(experiment_id, "experimentId")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Experiment {experiment_id} does not exist.")))?;
        if !data.experiments.contains_key(experiment_id) {
            return Err(not_found(&format!(
                "Experiment {experiment_id} does not exist."
            )));
        }
        let all: Vec<Value> = data
            .experiment_target_account_configs
            .get(experiment_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(&all, q);
        let mut out = json!({ "targetAccountConfigurations": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }
}

// ===================== safety levers =====================

impl FisService {
    /// Whether `id` addresses this account's safety lever. AWS provisions a
    /// single account-scoped safety lever whose id is the account id; any other
    /// id addresses a lever that does not exist.
    fn is_account_lever(ctx: &Ctx, id: &str) -> bool {
        id == ctx.account
    }

    fn get_safety_lever(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        if !Self::is_account_lever(ctx, id) {
            return Err(not_found(&format!("Safety lever {id} does not exist.")));
        }
        let guard = self.state.read();
        let stored = guard
            .get(&ctx.account)
            .and_then(|d| d.safety_levers.get(id))
            .cloned();
        // The account's safety lever reads back as `disengaged` (normal
        // operation, experiments allowed) until its state is updated.
        let lever = stored.unwrap_or_else(|| {
            json!({
                "id": id,
                "arn": shared::safety_lever_arn(&ctx.region, &ctx.account, id),
                "state": { "status": "disengaged", "reason": "Default safety lever state." }
            })
        });
        ok(json!({ "safetyLever": lever }))
    }

    fn update_safety_lever_state(
        &self,
        ctx: &Ctx,
        id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        if !Self::is_account_lever(ctx, id) {
            return Err(not_found(&format!("Safety lever {id} does not exist.")));
        }
        let status = body
            .get("state")
            .and_then(|s| s.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("disengaged");
        let reason = body
            .get("state")
            .and_then(|s| s.get("reason"))
            .cloned()
            .unwrap_or(Value::Null);
        let lever = json!({
            "id": id,
            "arn": shared::safety_lever_arn(&ctx.region, &ctx.account, id),
            "state": { "status": status, "reason": reason }
        });
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.safety_levers.insert(id.to_string(), lever.clone());
        ok(json!({ "safetyLever": lever }))
    }
}

// ===================== tagging =====================

impl FisService {
    /// Resolve a taggable resource (experiment template or experiment) by ARN to
    /// a mutable handle on its stored wire object.
    fn taggable_mut<'a>(data: &'a mut FisData, arn: &str) -> Option<&'a mut Map<String, Value>> {
        if let Some(id) = shared::template_id_from_arn(arn) {
            let id = id.to_string();
            return data
                .templates
                .get_mut(&id)
                .and_then(Value::as_object_mut)
                .filter(|o| o.get("arn").and_then(Value::as_str) == Some(arn));
        }
        if let Some(id) = shared::experiment_id_from_arn(arn) {
            let id = id.to_string();
            return data
                .experiments
                .get_mut(&id)
                .and_then(Value::as_object_mut)
                .filter(|o| o.get("arn").and_then(Value::as_str) == Some(arn));
        }
        None
    }

    fn taggable_ref<'a>(data: &'a FisData, arn: &str) -> Option<&'a Map<String, Value>> {
        if let Some(id) = shared::template_id_from_arn(arn) {
            return data
                .templates
                .get(id)
                .and_then(Value::as_object)
                .filter(|o| o.get("arn").and_then(Value::as_str) == Some(arn));
        }
        if let Some(id) = shared::experiment_id_from_arn(arn) {
            return data
                .experiments
                .get(id)
                .and_then(Value::as_object)
                .filter(|o| o.get("arn").and_then(Value::as_str) == Some(arn));
        }
        None
    }

    fn tag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_resource_arn(arn)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // A well-formed ARN that does not address a live resource is accepted as
        // a no-op (the FIS tagging operations declare no errors in the model).
        if let Some(obj) = Self::taggable_mut(data, arn) {
            let tags = obj.entry("tags").or_insert_with(|| json!({}));
            if let (Some(tags), Some(new)) = (
                tags.as_object_mut(),
                body.get("tags").and_then(Value::as_object),
            ) {
                for (k, v) in new {
                    tags.insert(k.clone(), v.clone());
                }
            }
        }
        ok(json!({}))
    }

    fn untag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_resource_arn(arn)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let keys = query_all(q, "tagKeys");
        if let Some(obj) = Self::taggable_mut(data, arn) {
            if let Some(tags) = obj.get_mut("tags").and_then(Value::as_object_mut) {
                if keys.is_empty() {
                    tags.clear();
                } else {
                    for key in keys {
                        tags.remove(&key);
                    }
                }
            }
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        validate_resource_arn(arn)?;
        let guard = self.state.read();
        let tags = guard
            .get(&ctx.account)
            .and_then(|data| Self::taggable_ref(data, arn))
            .and_then(|o| o.get("tags").cloned())
            .unwrap_or_else(|| json!({}));
        ok(json!({ "tags": tags }))
    }
}

// ===================== wire projections =====================

/// Build the `experimentOptions` output structure, defaulting the two members
/// AWS fills in (single-account targeting, fail-on-empty resolution). `input`
/// is a Create/Update input options object; `existing` is a prior options
/// object to fall back to on update.
fn build_experiment_options(input: Option<&Value>, existing: Option<&Value>) -> Value {
    let pick = |member: &str, default: &str| -> String {
        input
            .and_then(|o| o.get(member))
            .and_then(Value::as_str)
            .or_else(|| existing.and_then(|o| o.get(member)).and_then(Value::as_str))
            .unwrap_or(default)
            .to_string()
    };
    json!({
        "accountTargeting": pick("accountTargeting", "single-account"),
        "emptyTargetResolutionMode": pick("emptyTargetResolutionMode", "fail"),
    })
}

/// Project a stored `ExperimentTemplate` to its `ExperimentTemplateSummary`.
fn template_summary(tpl: &Value) -> Value {
    let mut m = Map::new();
    for key in [
        "id",
        "arn",
        "description",
        "creationTime",
        "lastUpdateTime",
        "tags",
    ] {
        if let Some(v) = tpl.get(key) {
            m.insert(key.to_string(), v.clone());
        }
    }
    Value::Object(m)
}

/// Project a stored `Experiment` to its `ExperimentSummary`.
fn experiment_summary(exp: &Value) -> Value {
    let mut m = Map::new();
    for key in [
        "id",
        "arn",
        "experimentTemplateId",
        "state",
        "creationTime",
        "tags",
        "experimentOptions",
    ] {
        if let Some(v) = exp.get(key) {
            m.insert(key.to_string(), v.clone());
        }
    }
    Value::Object(m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> FisService {
        FisService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn ctx() -> Ctx {
        Ctx {
            account: "000000000000".into(),
            region: "us-east-1".into(),
        }
    }

    fn body_json(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).expect("json response body")
    }

    fn err_of(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match r {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        }
    }

    fn create_body() -> Value {
        json!({
            "clientToken": "tok-1",
            "description": "chaos template",
            "stopConditions": [{ "source": "none" }],
            "targets": {
                "myInstances": {
                    "resourceType": "aws:ec2:instance",
                    "resourceArns": ["arn:aws:ec2:us-east-1:000000000000:instance/i-1234"],
                    "selectionMode": "ALL"
                }
            },
            "actions": {
                "stopIt": { "actionId": "aws:ec2:stop-instances", "targets": { "Instances": "myInstances" } }
            },
            "roleArn": "arn:aws:iam::000000000000:role/fis-role"
        })
    }

    fn create_template(s: &FisService, c: &Ctx) -> String {
        let resp = s.create_experiment_template(c, &create_body()).unwrap();
        body_json(&resp)["experimentTemplate"]["id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn create_get_list_roundtrip() {
        let s = svc();
        let c = ctx();
        let id = create_template(&s, &c);
        assert!(id.starts_with("EXT"));
        let got = body_json(&s.get_experiment_template(&c, &id).unwrap());
        assert_eq!(got["experimentTemplate"]["description"], "chaos template");
        assert_eq!(
            got["experimentTemplate"]["experimentOptions"]["accountTargeting"],
            "single-account"
        );
        let listed = body_json(&s.list_experiment_templates(&c, &[]).unwrap());
        assert_eq!(listed["experimentTemplates"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn get_missing_template_is_not_found() {
        let s = svc();
        let err = err_of(s.get_experiment_template(&ctx(), "EXTmissing"));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn experiment_lifecycle_settles() {
        let s = svc();
        let c = ctx();
        let tid = create_template(&s, &c);
        let start = body_json(
            &s.start_experiment(
                &c,
                &json!({ "clientToken": "t", "experimentTemplateId": tid }),
            )
            .unwrap(),
        );
        let eid = start["experiment"]["id"].as_str().unwrap().to_string();
        assert_eq!(start["experiment"]["state"]["status"], "initiating");

        // First read settles initiating -> running.
        assert!(s.reconcile(&c.account));
        let got = body_json(&s.get_experiment(&c, &eid).unwrap());
        assert_eq!(got["experiment"]["state"]["status"], "running");

        // Next read settles running -> completed.
        assert!(s.reconcile(&c.account));
        let got = body_json(&s.get_experiment(&c, &eid).unwrap());
        assert_eq!(got["experiment"]["state"]["status"], "completed");
        assert!(got["experiment"]["endTime"].is_number());
    }

    #[test]
    fn start_missing_template_is_not_found() {
        let s = svc();
        let err = err_of(s.start_experiment(
            &ctx(),
            &json!({ "clientToken": "t", "experimentTemplateId": "EXTnope" }),
        ));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn stop_experiment_moves_to_stopping() {
        let s = svc();
        let c = ctx();
        let tid = create_template(&s, &c);
        let start = body_json(
            &s.start_experiment(
                &c,
                &json!({ "clientToken": "t", "experimentTemplateId": tid }),
            )
            .unwrap(),
        );
        let eid = start["experiment"]["id"].as_str().unwrap().to_string();
        let stopped = body_json(&s.stop_experiment(&c, &eid).unwrap());
        assert_eq!(stopped["experiment"]["state"]["status"], "stopping");
    }

    #[test]
    fn actions_catalog_lists_and_gets() {
        let s = svc();
        let c = ctx();
        let listed = body_json(&s.list_actions(&c, &[]).unwrap());
        assert!(!listed["actions"].as_array().unwrap().is_empty());
        let got = body_json(&s.get_action(&c, "aws:ec2:stop-instances").unwrap());
        assert_eq!(got["action"]["id"], "aws:ec2:stop-instances");
        assert_eq!(
            err_of(s.get_action(&c, "aws:none")).status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn target_account_config_crud() {
        let s = svc();
        let c = ctx();
        let tid = create_template(&s, &c);
        let created = body_json(
            &s.create_tac(
                &c,
                &tid,
                "111122223333",
                &json!({ "roleArn": "arn:aws:iam::111122223333:role/fis" }),
            )
            .unwrap(),
        );
        assert_eq!(
            created["targetAccountConfiguration"]["accountId"],
            "111122223333"
        );
        // Reflected in the template's count.
        let tpl = body_json(&s.get_experiment_template(&c, &tid).unwrap());
        assert_eq!(
            tpl["experimentTemplate"]["targetAccountConfigurationsCount"],
            1
        );
        // Duplicate -> ConflictException.
        let err = err_of(s.create_tac(&c, &tid, "111122223333", &json!({ "roleArn": "r" })));
        assert_eq!(err.status(), StatusCode::CONFLICT);
        // Delete.
        s.delete_tac(&c, &tid, "111122223333").unwrap();
        assert_eq!(
            err_of(s.get_tac(&c, &tid, "111122223333")).status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn safety_lever_defaults_disengaged_then_updates() {
        let s = svc();
        let c = ctx();
        let lever_id = c.account.clone();
        // An id that is not the account lever does not exist.
        assert_eq!(
            err_of(s.get_safety_lever(&c, "unknown-lever")).status(),
            StatusCode::NOT_FOUND
        );
        let got = body_json(&s.get_safety_lever(&c, &lever_id).unwrap());
        assert_eq!(got["safetyLever"]["state"]["status"], "disengaged");
        s.update_safety_lever_state(
            &c,
            &lever_id,
            &json!({ "state": { "status": "engaged", "reason": "halt" } }),
        )
        .unwrap();
        let got = body_json(&s.get_safety_lever(&c, &lever_id).unwrap());
        assert_eq!(got["safetyLever"]["state"]["status"], "engaged");
    }

    #[test]
    fn tag_untag_by_arn() {
        let s = svc();
        let c = ctx();
        let id = create_template(&s, &c);
        let arn = shared::experiment_template_arn(&c.region, &c.account, &id);
        s.tag_resource(&c, &arn, &json!({ "tags": { "team": "sre" } }))
            .unwrap();
        let listed = body_json(&s.list_tags_for_resource(&c, &arn).unwrap());
        assert_eq!(listed["tags"]["team"], "sre");
        s.untag_resource(&c, &arn, &[("tagKeys".into(), "team".into())])
            .unwrap();
        let listed = body_json(&s.list_tags_for_resource(&c, &arn).unwrap());
        assert!(listed["tags"].as_object().unwrap().is_empty());
    }

    #[test]
    fn resolved_targets_derived_from_experiment() {
        let s = svc();
        let c = ctx();
        let tid = create_template(&s, &c);
        let start = body_json(
            &s.start_experiment(
                &c,
                &json!({ "clientToken": "t", "experimentTemplateId": tid }),
            )
            .unwrap(),
        );
        let eid = start["experiment"]["id"].as_str().unwrap().to_string();
        let rt = body_json(&s.list_resolved_targets(&c, &eid, &[]).unwrap());
        let targets = rt["resolvedTargets"].as_array().unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0]["targetName"], "myInstances");
    }

    fn route_req(method: Method, path: &str) -> AwsRequest {
        AwsRequest {
            service: "fis".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: path.to_string(),
            raw_query: String::new(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn routing_matches_expected_operations() {
        let cases = [
            (
                Method::POST,
                "/experimentTemplates",
                "CreateExperimentTemplate",
            ),
            (
                Method::GET,
                "/experimentTemplates",
                "ListExperimentTemplates",
            ),
            (
                Method::GET,
                "/experimentTemplates/EXT1",
                "GetExperimentTemplate",
            ),
            (
                Method::PATCH,
                "/experimentTemplates/EXT1",
                "UpdateExperimentTemplate",
            ),
            (
                Method::DELETE,
                "/experimentTemplates/EXT1",
                "DeleteExperimentTemplate",
            ),
            (
                Method::POST,
                "/experimentTemplates/EXT1/targetAccountConfigurations/111122223333",
                "CreateTargetAccountConfiguration",
            ),
            (Method::POST, "/experiments", "StartExperiment"),
            (Method::DELETE, "/experiments/EXP1", "StopExperiment"),
            (
                Method::GET,
                "/experiments/EXP1/resolvedTargets",
                "ListExperimentResolvedTargets",
            ),
            (Method::GET, "/actions/aws:ec2:stop-instances", "GetAction"),
            (
                Method::GET,
                "/targetResourceTypes/aws:ec2:instance",
                "GetTargetResourceType",
            ),
            (Method::GET, "/safetyLevers/L1", "GetSafetyLever"),
            (
                Method::PATCH,
                "/safetyLevers/L1/state",
                "UpdateSafetyLeverState",
            ),
            (Method::POST, "/tags/arn", "TagResource"),
        ];
        for (method, path, expected) in cases {
            let req = route_req(method.clone(), path);
            let (action, _) = FisService::resolve_action(&req).expect("route");
            assert_eq!(action, expected, "{method} {path}");
        }
    }
}
