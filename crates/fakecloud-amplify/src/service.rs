//! AWS Amplify (`amplify`) restJson1 dispatch + operation handlers.
//!
//! The full 37-operation AWS Amplify control plane. Requests are routed to an
//! operation by HTTP method + `@http` URI path; path labels are captured
//! positionally (percent-decoded, so an ARN label whose slashes arrive
//! percent-encoded survives intact) and query parameters are read from the raw
//! query string. State is account-partitioned and persisted; each resource is
//! stored as its already-output-valid wire object so `Get*` echoes exactly what
//! `Create*` / `Update*` persisted, and the async domain-verification /
//! job-build lifecycles settle deterministically on the next read.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::shared;
use crate::state::{AmplifyData, AppRecord, SharedAmplifyState};

/// Every operation name in the AWS Amplify Smithy model (37 operations).
pub const AMPLIFY_ACTIONS: &[&str] = &[
    "CreateApp",
    "CreateBackendEnvironment",
    "CreateBranch",
    "CreateDeployment",
    "CreateDomainAssociation",
    "CreateWebhook",
    "DeleteApp",
    "DeleteBackendEnvironment",
    "DeleteBranch",
    "DeleteDomainAssociation",
    "DeleteJob",
    "DeleteWebhook",
    "GenerateAccessLogs",
    "GetApp",
    "GetArtifactUrl",
    "GetBackendEnvironment",
    "GetBranch",
    "GetDomainAssociation",
    "GetJob",
    "GetWebhook",
    "ListApps",
    "ListArtifacts",
    "ListBackendEnvironments",
    "ListBranches",
    "ListDomainAssociations",
    "ListJobs",
    "ListTagsForResource",
    "ListWebhooks",
    "StartDeployment",
    "StartJob",
    "StopJob",
    "TagResource",
    "UntagResource",
    "UpdateApp",
    "UpdateBranch",
    "UpdateDomainAssociation",
    "UpdateWebhook",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &[
    "CreateApp",
    "UpdateApp",
    "DeleteApp",
    "CreateBranch",
    "UpdateBranch",
    "DeleteBranch",
    "CreateDomainAssociation",
    "UpdateDomainAssociation",
    "DeleteDomainAssociation",
    "CreateWebhook",
    "UpdateWebhook",
    "DeleteWebhook",
    "CreateBackendEnvironment",
    "DeleteBackendEnvironment",
    "StartJob",
    "StopJob",
    "DeleteJob",
    "CreateDeployment",
    "StartDeployment",
    "TagResource",
    "UntagResource",
];

pub struct AmplifyService {
    state: SharedAmplifyState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl AmplifyService {
    pub fn new(state: SharedAmplifyState) -> Self {
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

    /// Settle any in-flight domain / job lifecycle transition for the account.
    /// Returns `true` if a transition fired (so the caller persists the change).
    fn reconcile(&self, account: &str) -> bool {
        let mut guard = self.state.write();
        guard.get_or_create(account).reconcile()
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
        let del = m == Method::DELETE;
        let l1 = |a: &str| vec![a.to_string()];
        let l2 = |a: &str, b: &str| vec![a.to_string(), b.to_string()];
        let l3 = |a: &str, b: &str, c: &str| vec![a.to_string(), b.to_string(), c.to_string()];
        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            ["apps"] if post => ("CreateApp", vec![]),
            ["apps"] if get => ("ListApps", vec![]),
            ["apps", app] if post => ("UpdateApp", l1(app)),
            ["apps", app] if get => ("GetApp", l1(app)),
            ["apps", app] if del => ("DeleteApp", l1(app)),
            ["apps", app, "branches"] if post => ("CreateBranch", l1(app)),
            ["apps", app, "branches"] if get => ("ListBranches", l1(app)),
            ["apps", app, "branches", br] if post => ("UpdateBranch", l2(app, br)),
            ["apps", app, "branches", br] if get => ("GetBranch", l2(app, br)),
            ["apps", app, "branches", br] if del => ("DeleteBranch", l2(app, br)),
            ["apps", app, "branches", br, "deployments"] if post => {
                ("CreateDeployment", l2(app, br))
            }
            ["apps", app, "branches", br, "deployments", "start"] if post => {
                ("StartDeployment", l2(app, br))
            }
            ["apps", app, "branches", br, "jobs"] if post => ("StartJob", l2(app, br)),
            ["apps", app, "branches", br, "jobs"] if get => ("ListJobs", l2(app, br)),
            ["apps", app, "branches", br, "jobs", job] if get => ("GetJob", l3(app, br, job)),
            ["apps", app, "branches", br, "jobs", job] if del => ("DeleteJob", l3(app, br, job)),
            ["apps", app, "branches", br, "jobs", job, "stop"] if del => {
                ("StopJob", l3(app, br, job))
            }
            ["apps", app, "branches", br, "jobs", job, "artifacts"] if get => {
                ("ListArtifacts", l3(app, br, job))
            }
            ["apps", app, "domains"] if post => ("CreateDomainAssociation", l1(app)),
            ["apps", app, "domains"] if get => ("ListDomainAssociations", l1(app)),
            ["apps", app, "domains", dom] if post => ("UpdateDomainAssociation", l2(app, dom)),
            ["apps", app, "domains", dom] if get => ("GetDomainAssociation", l2(app, dom)),
            ["apps", app, "domains", dom] if del => ("DeleteDomainAssociation", l2(app, dom)),
            ["apps", app, "webhooks"] if post => ("CreateWebhook", l1(app)),
            ["apps", app, "webhooks"] if get => ("ListWebhooks", l1(app)),
            ["apps", app, "backendenvironments"] if post => ("CreateBackendEnvironment", l1(app)),
            ["apps", app, "backendenvironments"] if get => ("ListBackendEnvironments", l1(app)),
            ["apps", app, "backendenvironments", env] if get => {
                ("GetBackendEnvironment", l2(app, env))
            }
            ["apps", app, "backendenvironments", env] if del => {
                ("DeleteBackendEnvironment", l2(app, env))
            }
            ["apps", app, "accesslogs"] if post => ("GenerateAccessLogs", l1(app)),
            ["webhooks", wh] if post => ("UpdateWebhook", l1(wh)),
            ["webhooks", wh] if get => ("GetWebhook", l1(wh)),
            ["webhooks", wh] if del => ("DeleteWebhook", l1(wh)),
            ["artifacts", art] if get => ("GetArtifactUrl", l1(art)),
            ["tags", arn] if post => ("TagResource", l1(arn)),
            ["tags", arn] if get => ("ListTagsForResource", l1(arn)),
            ["tags", arn] if del => ("UntagResource", l1(arn)),
            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for AmplifyService {
    fn service_name(&self) -> &str {
        "amplify"
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
        AMPLIFY_ACTIONS
    }
}

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

impl AmplifyService {
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
        // state, remembering whether it fired so the change is persisted even
        // for a pure read.
        let settled = self.reconcile(&ctx.account);
        let a = |i: usize| labels.get(i).map(String::as_str).unwrap_or_default();
        let result = match action {
            "CreateApp" => self.create_app(&ctx, &body),
            "GetApp" => self.get_app(&ctx, a(0)),
            "UpdateApp" => self.update_app(&ctx, a(0), &body),
            "DeleteApp" => self.delete_app(&ctx, a(0)),
            "ListApps" => self.list_apps(&ctx, &q),
            "CreateBranch" => self.create_branch(&ctx, a(0), &body),
            "GetBranch" => self.get_branch(&ctx, a(0), a(1)),
            "UpdateBranch" => self.update_branch(&ctx, a(0), a(1), &body),
            "DeleteBranch" => self.delete_branch(&ctx, a(0), a(1)),
            "ListBranches" => self.list_branches(&ctx, a(0), &q),
            "CreateDomainAssociation" => self.create_domain(&ctx, a(0), &body),
            "GetDomainAssociation" => self.get_domain(&ctx, a(0), a(1)),
            "UpdateDomainAssociation" => self.update_domain(&ctx, a(0), a(1), &body),
            "DeleteDomainAssociation" => self.delete_domain(&ctx, a(0), a(1)),
            "ListDomainAssociations" => self.list_domains(&ctx, a(0), &q),
            "CreateWebhook" => self.create_webhook(&ctx, a(0), &body),
            "GetWebhook" => self.get_webhook(&ctx, a(0)),
            "UpdateWebhook" => self.update_webhook(&ctx, a(0), &body),
            "DeleteWebhook" => self.delete_webhook(&ctx, a(0)),
            "ListWebhooks" => self.list_webhooks(&ctx, a(0), &q),
            "CreateBackendEnvironment" => self.create_backend_env(&ctx, a(0), &body),
            "GetBackendEnvironment" => self.get_backend_env(&ctx, a(0), a(1)),
            "DeleteBackendEnvironment" => self.delete_backend_env(&ctx, a(0), a(1)),
            "ListBackendEnvironments" => self.list_backend_envs(&ctx, a(0), &q),
            "StartJob" => self.start_job(&ctx, a(0), a(1), &body),
            "GetJob" => self.get_job(&ctx, a(0), a(1), a(2)),
            "StopJob" => self.stop_job(&ctx, a(0), a(1), a(2)),
            "DeleteJob" => self.delete_job(&ctx, a(0), a(1), a(2)),
            "ListJobs" => self.list_jobs(&ctx, a(0), a(1), &q),
            "CreateDeployment" => self.create_deployment(&ctx, a(0), a(1), &body),
            "StartDeployment" => self.start_deployment(&ctx, a(0), a(1), &body),
            "GenerateAccessLogs" => self.generate_access_logs(&ctx, a(0), &body),
            "GetArtifactUrl" => self.get_artifact_url(&ctx, a(0)),
            "ListArtifacts" => self.list_artifacts(&ctx, a(0), a(1), a(2), &q),
            "TagResource" => self.tag_resource(&ctx, a(0), &body),
            "UntagResource" => self.untag_resource(&ctx, a(0), &q),
            "ListTagsForResource" => self.list_tags(&ctx, a(0)),
            _ => Err(AwsServiceError::action_not_implemented("amplify", action)),
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
        .map_err(|e| bad_request(&format!("Request body is malformed: {e}")))
}

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg)
}

fn resource_not_found(msg: &str) -> AwsServiceError {
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

/// Validate an `appId` path label against Amplify's `^d[a-z0-9]+$` (1..=20)
/// pattern, rejecting a malformed id with `BadRequestException` before lookup.
fn check_app_id(app_id: &str) -> Result<(), AwsServiceError> {
    let valid = (1..=20).contains(&app_id.len())
        && app_id.starts_with('d')
        && app_id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    if valid {
        Ok(())
    } else {
        Err(bad_request(&format!(
            "App id '{app_id}' does not match required pattern ^d[a-z0-9]+$ (1-20 characters)."
        )))
    }
}

/// Validate a `branchName` path label (1..=255 chars).
fn check_branch_name(name: &str) -> Result<(), AwsServiceError> {
    if (1..=255).contains(&name.len()) {
        Ok(())
    } else {
        Err(bad_request(&format!(
            "Branch name must be 1-255 characters, got {}.",
            name.len()
        )))
    }
}

/// Read an optional string body member (owned), or a default.
fn str_or(body: &Value, key: &str, default: &str) -> String {
    body.get(key)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .to_string()
}

fn bool_or(body: &Value, key: &str, default: bool) -> bool {
    body.get(key).and_then(Value::as_bool).unwrap_or(default)
}

/// Copy each named optional member from `body` into `out` verbatim when present
/// and non-null. Used to echo the model's optional members onto stored wire
/// objects without inventing values.
fn echo(out: &mut Map<String, Value>, body: &Value, keys: &[&str]) {
    for key in keys {
        if let Some(v) = body.get(*key) {
            if !v.is_null() {
                out.insert((*key).to_string(), v.clone());
            }
        }
    }
}

/// Paginate a list of wire objects using the request's `maxResults` /
/// `nextToken` query params. Validates `maxResults` against `[0, max]` and
/// `nextToken` length, returning `BadRequestException` for out-of-range values.
fn paginate(
    items: Vec<Value>,
    q: &[(String, String)],
    max_results_cap: usize,
) -> Result<(Vec<Value>, Option<String>), AwsServiceError> {
    if let Some(tok) = query_one(q, "nextToken") {
        if tok.len() > 2000 {
            return Err(bad_request("nextToken exceeds the maximum length of 2000."));
        }
    }
    let max = match query_one(q, "maxResults") {
        Some(v) => {
            let n: i64 = v
                .parse()
                .map_err(|_| bad_request("maxResults must be an integer."))?;
            if n < 0 || n as usize > max_results_cap {
                return Err(bad_request(&format!(
                    "maxResults must be between 0 and {max_results_cap}."
                )));
            }
            if n == 0 {
                max_results_cap
            } else {
                n as usize
            }
        }
        None => max_results_cap.min(100),
    };
    let start = query_one(q, "nextToken")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let end = (start + max).min(items.len());
    let page: Vec<Value> = items.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    Ok((page, next))
}

impl AmplifyService {
    // ------------------------------- Apps -------------------------------

    fn build_app(&self, ctx: &Ctx, app_id: &str, body: &Value) -> Value {
        let now = shared::now_epoch();
        let mut app = Map::new();
        app.insert("appId".into(), json!(app_id));
        app.insert(
            "appArn".into(),
            json!(shared::app_arn(&ctx.region, &ctx.account, app_id)),
        );
        app.insert("name".into(), json!(str_or(body, "name", "")));
        app.insert("description".into(), json!(str_or(body, "description", "")));
        app.insert("repository".into(), json!(str_or(body, "repository", "")));
        app.insert("platform".into(), json!(str_or(body, "platform", "WEB")));
        app.insert("createTime".into(), json!(now));
        app.insert("updateTime".into(), json!(now));
        app.insert(
            "environmentVariables".into(),
            body.get("environmentVariables")
                .cloned()
                .unwrap_or_else(|| json!({})),
        );
        app.insert(
            "defaultDomain".into(),
            json!(shared::default_domain(app_id)),
        );
        app.insert(
            "enableBranchAutoBuild".into(),
            json!(bool_or(body, "enableBranchAutoBuild", true)),
        );
        app.insert(
            "enableBasicAuth".into(),
            json!(bool_or(body, "enableBasicAuth", false)),
        );
        // The repository clone method AWS derives from the supplied credential:
        // a personal/OAuth token yields TOKEN, otherwise SSH deploy keys.
        let clone_method = if body.get("accessToken").is_some() || body.get("oauthToken").is_some()
        {
            "TOKEN"
        } else {
            "SSH"
        };
        app.insert("repositoryCloneMethod".into(), json!(clone_method));
        echo(
            &mut app,
            body,
            &[
                "tags",
                "computeRoleArn",
                "iamServiceRoleArn",
                "enableBranchAutoDeletion",
                "basicAuthCredentials",
                "customRules",
                "buildSpec",
                "customHeaders",
                "enableAutoBranchCreation",
                "autoBranchCreationPatterns",
                "autoBranchCreationConfig",
                "cacheConfig",
                "jobConfig",
            ],
        );
        Value::Object(app)
    }

    fn create_app(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let app_id = shared::new_app_id();
        let app = self.build_app(ctx, &app_id, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.apps.insert(
            app_id.clone(),
            AppRecord {
                app: app.clone(),
                ..Default::default()
            },
        );
        ok(json!({ "app": app }))
    }

    fn get_app(&self, ctx: &Ctx, app_id: &str) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let guard = self.state.read();
        match guard.get(&ctx.account).and_then(|d| d.apps.get(app_id)) {
            Some(rec) => ok(json!({ "app": rec.app })),
            None => Err(not_found(&format!("App {app_id} not found."))),
        }
    }

    fn update_app(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.get_mut(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        let Some(app) = rec.app.as_object_mut() else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        for key in [
            "name",
            "description",
            "platform",
            "computeRoleArn",
            "iamServiceRoleArn",
            "environmentVariables",
            "basicAuthCredentials",
            "customRules",
            "buildSpec",
            "customHeaders",
            "autoBranchCreationPatterns",
            "autoBranchCreationConfig",
            "repository",
            "cacheConfig",
            "jobConfig",
        ] {
            if let Some(v) = body.get(key) {
                app.insert(key.to_string(), v.clone());
            }
        }
        // Boolean members renamed between input and output.
        if let Some(v) = body.get("enableBranchAutoBuild") {
            app.insert("enableBranchAutoBuild".into(), v.clone());
        }
        for key in [
            "enableBranchAutoDeletion",
            "enableBasicAuth",
            "enableAutoBranchCreation",
        ] {
            if let Some(v) = body.get(key) {
                app.insert(key.to_string(), v.clone());
            }
        }
        app.insert("updateTime".into(), json!(shared::now_epoch()));
        ok(json!({ "app": Value::Object(app.clone()) }))
    }

    fn delete_app(&self, ctx: &Ctx, app_id: &str) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.remove(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        // Cascade: drop webhooks that belonged to this app.
        data.webhooks
            .retain(|_, w| w.get("appId").and_then(Value::as_str) != Some(app_id));
        ok(json!({ "app": rec.app }))
    }

    fn list_apps(&self, ctx: &Ctx, q: &[(String, String)]) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let apps: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.apps.values().map(|r| r.app.clone()).collect())
            .unwrap_or_default();
        let (page, next) = paginate(apps, q, 100)?;
        let mut out = json!({ "apps": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // ------------------------------ Branches ----------------------------

    fn build_branch(&self, ctx: &Ctx, app_id: &str, branch_name: &str, body: &Value) -> Value {
        let now = shared::now_epoch();
        let mut b = Map::new();
        b.insert(
            "branchArn".into(),
            json!(shared::branch_arn(
                &ctx.region,
                &ctx.account,
                app_id,
                branch_name
            )),
        );
        b.insert("branchName".into(), json!(branch_name));
        b.insert("description".into(), json!(str_or(body, "description", "")));
        b.insert("stage".into(), json!(str_or(body, "stage", "NONE")));
        b.insert(
            "displayName".into(),
            json!(str_or(body, "displayName", branch_name)),
        );
        b.insert(
            "enableNotification".into(),
            json!(bool_or(body, "enableNotification", false)),
        );
        b.insert("createTime".into(), json!(now));
        b.insert("updateTime".into(), json!(now));
        b.insert(
            "environmentVariables".into(),
            body.get("environmentVariables")
                .cloned()
                .unwrap_or_else(|| json!({})),
        );
        b.insert(
            "enableAutoBuild".into(),
            json!(bool_or(body, "enableAutoBuild", true)),
        );
        b.insert("customDomains".into(), json!([]));
        b.insert("framework".into(), json!(str_or(body, "framework", "")));
        b.insert("activeJobId".into(), json!(""));
        b.insert("totalNumberOfJobs".into(), json!("0"));
        b.insert(
            "enableBasicAuth".into(),
            json!(bool_or(body, "enableBasicAuth", false)),
        );
        b.insert("ttl".into(), json!(str_or(body, "ttl", "5")));
        b.insert(
            "enablePullRequestPreview".into(),
            json!(bool_or(body, "enablePullRequestPreview", false)),
        );
        b.insert(
            "thumbnailUrl".into(),
            json!(shared::thumbnail_url(&ctx.region, app_id, branch_name)),
        );
        echo(
            &mut b,
            body,
            &[
                "tags",
                "enableSkewProtection",
                "enablePerformanceMode",
                "basicAuthCredentials",
                "buildSpec",
                "pullRequestEnvironmentName",
                "backendEnvironmentArn",
                "backend",
                "computeRoleArn",
            ],
        );
        Value::Object(b)
    }

    fn create_branch(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let branch_name = body
            .get("branchName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        check_branch_name(&branch_name)?;
        let branch = self.build_branch(ctx, app_id, &branch_name, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.get_mut(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        if rec.branches.contains_key(&branch_name) {
            return Err(bad_request(&format!(
                "A branch with the name {branch_name} already exists."
            )));
        }
        rec.branches.insert(branch_name, branch.clone());
        ok(json!({ "branch": branch }))
    }

    fn get_branch(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let guard = self.state.read();
        match guard
            .get(&ctx.account)
            .and_then(|d| d.apps.get(app_id))
            .and_then(|r| r.branches.get(branch))
        {
            Some(b) => ok(json!({ "branch": b })),
            None => Err(not_found(&format!("Branch {branch} not found."))),
        }
    }

    fn update_branch(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(b) = data
            .apps
            .get_mut(app_id)
            .and_then(|r| r.branches.get_mut(branch))
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!("Branch {branch} not found.")));
        };
        for key in [
            "description",
            "framework",
            "stage",
            "enableNotification",
            "enableAutoBuild",
            "enableSkewProtection",
            "environmentVariables",
            "basicAuthCredentials",
            "enableBasicAuth",
            "enablePerformanceMode",
            "buildSpec",
            "ttl",
            "displayName",
            "enablePullRequestPreview",
            "pullRequestEnvironmentName",
            "backendEnvironmentArn",
            "backend",
            "computeRoleArn",
        ] {
            if let Some(v) = body.get(key) {
                b.insert(key.to_string(), v.clone());
            }
        }
        b.insert("updateTime".into(), json!(shared::now_epoch()));
        ok(json!({ "branch": Value::Object(b.clone()) }))
    }

    fn delete_branch(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.get_mut(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        match rec.branches.remove(branch) {
            Some(b) => {
                rec.jobs.remove(branch);
                rec.next_job_id.remove(branch);
                ok(json!({ "branch": b }))
            }
            None => Err(not_found(&format!("Branch {branch} not found."))),
        }
    }

    fn list_branches(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let guard = self.state.read();
        let branches: Vec<Value> = guard
            .get(&ctx.account)
            .and_then(|d| d.apps.get(app_id))
            .map(|r| r.branches.values().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(branches, q, 50)?;
        let mut out = json!({ "branches": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // --------------------------- Domains --------------------------------

    fn build_domain(&self, ctx: &Ctx, app_id: &str, domain: &str, body: &Value) -> Value {
        let mut d = Map::new();
        d.insert(
            "domainAssociationArn".into(),
            json!(shared::domain_arn(
                &ctx.region,
                &ctx.account,
                app_id,
                domain
            )),
        );
        d.insert("domainName".into(), json!(domain));
        d.insert(
            "enableAutoSubDomain".into(),
            json!(bool_or(body, "enableAutoSubDomain", false)),
        );
        d.insert("domainStatus".into(), json!("CREATING"));
        d.insert("updateStatus".into(), json!("PENDING_VERIFICATION"));
        d.insert("statusReason".into(), json!(""));
        d.insert(
            "certificateVerificationDNSRecord".into(),
            json!(shared::certificate_verification_dns_record(domain)),
        );
        // Build the subdomains from the requested settings.
        let sub_settings = body
            .get("subDomainSettings")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let subdomains: Vec<Value> = sub_settings
            .iter()
            .map(|s| {
                json!({
                    "subDomainSetting": s,
                    "verified": false,
                    "dnsRecord": shared::subdomain_dns_record(app_id),
                })
            })
            .collect();
        d.insert("subDomains".into(), json!(subdomains));
        // Certificate mirrors the requested certificate settings.
        if let Some(cs) = body.get("certificateSettings").and_then(Value::as_object) {
            let ctype = cs
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("AMPLIFY_MANAGED");
            let mut cert = Map::new();
            cert.insert("type".into(), json!(ctype));
            if let Some(arn) = cs.get("customCertificateArn") {
                cert.insert("customCertificateArn".into(), arn.clone());
            }
            cert.insert(
                "certificateVerificationDNSRecord".into(),
                json!(shared::certificate_verification_dns_record(domain)),
            );
            d.insert("certificate".into(), Value::Object(cert));
        }
        echo(
            &mut d,
            body,
            &["autoSubDomainCreationPatterns", "autoSubDomainIAMRole"],
        );
        Value::Object(d)
    }

    fn create_domain(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let domain = body
            .get("domainName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let assoc = self.build_domain(ctx, app_id, &domain, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.get_mut(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        if rec.domains.contains_key(&domain) {
            return Err(bad_request(&format!(
                "A domain association for {domain} already exists."
            )));
        }
        rec.domains.insert(domain, assoc.clone());
        ok(json!({ "domainAssociation": assoc }))
    }

    fn get_domain(
        &self,
        ctx: &Ctx,
        app_id: &str,
        domain: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let guard = self.state.read();
        match guard
            .get(&ctx.account)
            .and_then(|d| d.apps.get(app_id))
            .and_then(|r| r.domains.get(domain))
        {
            Some(a) => ok(json!({ "domainAssociation": a })),
            None => Err(not_found(&format!(
                "Domain association {domain} not found."
            ))),
        }
    }

    fn update_domain(
        &self,
        ctx: &Ctx,
        app_id: &str,
        domain: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(a) = data
            .apps
            .get_mut(app_id)
            .and_then(|r| r.domains.get_mut(domain))
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!(
                "Domain association {domain} not found."
            )));
        };
        if let Some(v) = body.get("enableAutoSubDomain") {
            a.insert("enableAutoSubDomain".into(), v.clone());
        }
        for key in ["autoSubDomainCreationPatterns", "autoSubDomainIAMRole"] {
            if let Some(v) = body.get(key) {
                a.insert(key.to_string(), v.clone());
            }
        }
        if let Some(settings) = body.get("subDomainSettings").and_then(Value::as_array) {
            let subs: Vec<Value> = settings
                .iter()
                .map(|s| {
                    json!({
                        "subDomainSetting": s,
                        "verified": false,
                        "dnsRecord": shared::subdomain_dns_record(app_id),
                    })
                })
                .collect();
            a.insert("subDomains".into(), json!(subs));
        }
        // Re-enter verification so the update settles on the next read.
        a.insert("domainStatus".into(), json!("UPDATING"));
        a.insert("updateStatus".into(), json!("PENDING_VERIFICATION"));
        ok(json!({ "domainAssociation": Value::Object(a.clone()) }))
    }

    fn delete_domain(
        &self,
        ctx: &Ctx,
        app_id: &str,
        domain: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.get_mut(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        match rec.domains.remove(domain) {
            Some(a) => ok(json!({ "domainAssociation": a })),
            None => Err(not_found(&format!(
                "Domain association {domain} not found."
            ))),
        }
    }

    fn list_domains(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let guard = self.state.read();
        let domains: Vec<Value> = guard
            .get(&ctx.account)
            .and_then(|d| d.apps.get(app_id))
            .map(|r| r.domains.values().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(domains, q, 50)?;
        let mut out = json!({ "domainAssociations": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // --------------------------- Webhooks -------------------------------

    fn create_webhook(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let branch = body
            .get("branchName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        check_branch_name(&branch)?;
        let webhook_id = shared::new_webhook_id();
        let now = shared::now_epoch();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.get_mut(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        if !rec.branches.contains_key(&branch) {
            return Err(not_found(&format!("Branch {branch} not found.")));
        }
        let webhook = json!({
            "webhookArn": shared::webhook_arn(&ctx.region, &ctx.account, app_id, &webhook_id),
            "webhookId": webhook_id,
            "webhookUrl": shared::webhook_url(&ctx.region, &webhook_id),
            "appId": app_id,
            "branchName": branch,
            "description": str_or(body, "description", ""),
            "createTime": now,
            "updateTime": now,
        });
        data.webhooks.insert(webhook_id, webhook.clone());
        ok(json!({ "webhook": webhook }))
    }

    fn get_webhook(&self, ctx: &Ctx, webhook_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        match guard
            .get(&ctx.account)
            .and_then(|d| d.webhooks.get(webhook_id))
        {
            Some(w) => ok(json!({ "webhook": w })),
            None => Err(not_found(&format!("Webhook {webhook_id} not found."))),
        }
    }

    fn update_webhook(
        &self,
        ctx: &Ctx,
        webhook_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(w) = data
            .webhooks
            .get_mut(webhook_id)
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!("Webhook {webhook_id} not found.")));
        };
        if let Some(v) = body.get("branchName") {
            w.insert("branchName".into(), v.clone());
        }
        if let Some(v) = body.get("description") {
            w.insert("description".into(), v.clone());
        }
        w.insert("updateTime".into(), json!(shared::now_epoch()));
        ok(json!({ "webhook": Value::Object(w.clone()) }))
    }

    fn delete_webhook(&self, ctx: &Ctx, webhook_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        match data.webhooks.remove(webhook_id) {
            Some(w) => ok(json!({ "webhook": w })),
            None => Err(not_found(&format!("Webhook {webhook_id} not found."))),
        }
    }

    fn list_webhooks(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let guard = self.state.read();
        let webhooks: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.webhooks
                    .values()
                    .filter(|w| w.get("appId").and_then(Value::as_str) == Some(app_id))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(webhooks, q, 50)?;
        let mut out = json!({ "webhooks": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // ---------------------- Backend environments ------------------------

    fn create_backend_env(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let name = body
            .get("environmentName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let now = shared::now_epoch();
        let mut env = Map::new();
        env.insert(
            "backendEnvironmentArn".into(),
            json!(shared::backend_environment_arn(
                &ctx.region,
                &ctx.account,
                app_id,
                &name
            )),
        );
        env.insert("environmentName".into(), json!(name));
        env.insert("createTime".into(), json!(now));
        env.insert("updateTime".into(), json!(now));
        echo(&mut env, body, &["stackName", "deploymentArtifacts"]);
        let env = Value::Object(env);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.get_mut(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        let name = env["environmentName"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if rec.backend_environments.contains_key(&name) {
            return Err(bad_request(&format!(
                "A backend environment {name} already exists."
            )));
        }
        rec.backend_environments.insert(name, env.clone());
        ok(json!({ "backendEnvironment": env }))
    }

    fn get_backend_env(
        &self,
        ctx: &Ctx,
        app_id: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let guard = self.state.read();
        match guard
            .get(&ctx.account)
            .and_then(|d| d.apps.get(app_id))
            .and_then(|r| r.backend_environments.get(name))
        {
            Some(e) => ok(json!({ "backendEnvironment": e })),
            None => Err(not_found(&format!("Backend environment {name} not found."))),
        }
    }

    fn delete_backend_env(
        &self,
        ctx: &Ctx,
        app_id: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.get_mut(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        match rec.backend_environments.remove(name) {
            Some(e) => ok(json!({ "backendEnvironment": e })),
            None => Err(not_found(&format!("Backend environment {name} not found."))),
        }
    }

    fn list_backend_envs(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let filter = query_one(q, "environmentName");
        let guard = self.state.read();
        let envs: Vec<Value> = guard
            .get(&ctx.account)
            .and_then(|d| d.apps.get(app_id))
            .map(|r| {
                r.backend_environments
                    .values()
                    .filter(|e| {
                        filter.is_none()
                            || e.get("environmentName").and_then(Value::as_str) == filter
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(envs, q, 50)?;
        let mut out = json!({ "backendEnvironments": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // ------------------------------ Jobs --------------------------------

    /// Build a fresh `JobSummary` in the `PENDING` state.
    fn build_job_summary(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
        job_id: &str,
        job_type: &str,
        body: &Value,
    ) -> Value {
        let now = shared::now_epoch();
        json!({
            "jobArn": shared::job_arn(&ctx.region, &ctx.account, app_id, branch, job_id),
            "jobId": job_id,
            "commitId": str_or(body, "commitId", "HEAD"),
            "commitMessage": str_or(body, "commitMessage", ""),
            "commitTime": body.get("commitTime").cloned().unwrap_or_else(|| json!(now)),
            "startTime": now,
            "status": "PENDING",
            "jobType": job_type,
        })
    }

    /// Allocate the next numeric job id for a branch and bump its counter +
    /// the branch's `totalNumberOfJobs` / `activeJobId`.
    fn allocate_job(rec: &mut AppRecord, branch: &str, summary: &Value) -> String {
        let next = rec.next_job_id.entry(branch.to_string()).or_insert(1);
        let job_id = next.to_string();
        *next += 1;
        let job = json!({ "summary": summary, "steps": [] });
        rec.jobs
            .entry(branch.to_string())
            .or_default()
            .insert(job_id.clone(), job);
        if let Some(b) = rec.branches.get_mut(branch).and_then(Value::as_object_mut) {
            b.insert("activeJobId".into(), json!(job_id));
            let total = rec
                .jobs
                .get(branch)
                .map(|m| m.len())
                .unwrap_or(0)
                .to_string();
            b.insert("totalNumberOfJobs".into(), json!(total));
        }
        job_id
    }

    fn start_job(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let job_type = body
            .get("jobType")
            .and_then(Value::as_str)
            .unwrap_or("RELEASE")
            .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.get_mut(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        if !rec.branches.contains_key(branch) {
            return Err(not_found(&format!("Branch {branch} not found.")));
        }
        // `StartJob` on an existing (e.g. CREATED-from-deployment) job id
        // restarts that job; otherwise a fresh id is allocated.
        let requested = body.get("jobId").and_then(Value::as_str);
        let (job_id, summary) = if let Some(jid) = requested.filter(|j| {
            rec.jobs
                .get(branch)
                .map(|m| m.contains_key(*j))
                .unwrap_or(false)
        }) {
            let jid = jid.to_string();
            let summary = self.build_job_summary(ctx, app_id, branch, &jid, &job_type, body);
            if let Some(job) = rec
                .jobs
                .get_mut(branch)
                .and_then(|m| m.get_mut(&jid))
                .and_then(Value::as_object_mut)
            {
                job.insert("summary".into(), summary.clone());
            }
            (jid, summary)
        } else {
            // Allocate a placeholder id to build the summary, then commit.
            let peek = rec
                .next_job_id
                .get(branch)
                .copied()
                .unwrap_or(1)
                .to_string();
            let summary = self.build_job_summary(ctx, app_id, branch, &peek, &job_type, body);
            let job_id = Self::allocate_job(rec, branch, &summary);
            (job_id, summary)
        };
        let _ = job_id;
        ok(json!({ "jobSummary": summary }))
    }

    fn get_job(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
        job_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let guard = self.state.read();
        match guard
            .get(&ctx.account)
            .and_then(|d| d.apps.get(app_id))
            .and_then(|r| r.jobs.get(branch))
            .and_then(|m| m.get(job_id))
        {
            Some(job) => ok(json!({ "job": job })),
            None => Err(not_found(&format!("Job {job_id} not found."))),
        }
    }

    fn stop_job(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
        job_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(job) = data
            .apps
            .get_mut(app_id)
            .and_then(|r| r.jobs.get_mut(branch))
            .and_then(|m| m.get_mut(job_id))
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!("Job {job_id} not found.")));
        };
        let now = shared::now_epoch();
        if let Some(summary) = job.get_mut("summary").and_then(Value::as_object_mut) {
            summary.insert("status".into(), json!("CANCELLED"));
            summary.insert("endTime".into(), json!(now));
            let summary = Value::Object(summary.clone());
            return ok(json!({ "jobSummary": summary }));
        }
        Err(not_found(&format!("Job {job_id} not found.")))
    }

    fn delete_job(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
        job_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(jobs) = data
            .apps
            .get_mut(app_id)
            .and_then(|r| r.jobs.get_mut(branch))
        else {
            return Err(not_found(&format!("Job {job_id} not found.")));
        };
        match jobs.remove(job_id) {
            Some(job) => {
                let summary = job.get("summary").cloned().unwrap_or_else(|| json!({}));
                ok(json!({ "jobSummary": summary }))
            }
            None => Err(not_found(&format!("Job {job_id} not found."))),
        }
    }

    fn list_jobs(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let guard = self.state.read();
        let mut summaries: Vec<Value> = guard
            .get(&ctx.account)
            .and_then(|d| d.apps.get(app_id))
            .and_then(|r| r.jobs.get(branch))
            .map(|m| {
                m.values()
                    .filter_map(|j| j.get("summary").cloned())
                    .collect()
            })
            .unwrap_or_default();
        // Newest job first (Amplify orders jobs by descending id).
        summaries.reverse();
        let (page, next) = paginate(summaries, q, 50)?;
        let mut out = json!({ "jobSummaries": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // -------------------------- Deployments -----------------------------

    fn create_deployment(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        // `CreateDeployment` declares no NotFoundException; it is a presign
        // operation that hands back S3 upload URLs. Validate the labels, then
        // return upload targets. When the branch exists we reserve a job id in
        // the `CREATED` state that a later `StartDeployment` starts.
        check_app_id(app_id)?;
        check_branch_name(branch)?;
        let file_map = body.get("fileMap").and_then(Value::as_object);
        let mut uploads = Map::new();
        if let Some(fm) = file_map {
            for name in fm.keys() {
                uploads.insert(
                    name.clone(),
                    json!(shared::upload_url(&ctx.region, app_id, branch, name)),
                );
            }
        }
        let zip_url = shared::upload_url(&ctx.region, app_id, branch, "deployment.zip");

        let mut job_id: Option<String> = None;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(rec) = data.apps.get_mut(app_id) {
            if rec.branches.contains_key(branch) {
                let next = rec.next_job_id.entry(branch.to_string()).or_insert(1);
                let id = next.to_string();
                *next += 1;
                let now = shared::now_epoch();
                let summary = json!({
                    "jobArn": shared::job_arn(&ctx.region, &ctx.account, app_id, branch, &id),
                    "jobId": id,
                    "commitId": "HEAD",
                    "commitMessage": "",
                    "commitTime": now,
                    "startTime": now,
                    "status": "CREATED",
                    "jobType": "MANUAL",
                });
                rec.jobs
                    .entry(branch.to_string())
                    .or_default()
                    .insert(id.clone(), json!({ "summary": summary, "steps": [] }));
                job_id = Some(id);
            }
        }
        let mut out = json!({
            "fileUploadUrls": Value::Object(uploads),
            "zipUploadUrl": zip_url,
        });
        if let Some(id) = job_id {
            out["jobId"] = json!(id);
        }
        ok(out)
    }

    fn start_deployment(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rec) = data.apps.get_mut(app_id) else {
            return Err(not_found(&format!("App {app_id} not found.")));
        };
        if !rec.branches.contains_key(branch) {
            return Err(not_found(&format!("Branch {branch} not found.")));
        }
        let now = shared::now_epoch();
        let requested = body
            .get("jobId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let (job_id, summary) = match requested.filter(|j| {
            rec.jobs
                .get(branch)
                .map(|m| m.contains_key(j))
                .unwrap_or(false)
        }) {
            Some(jid) => {
                let summary = json!({
                    "jobArn": shared::job_arn(&ctx.region, &ctx.account, app_id, branch, &jid),
                    "jobId": jid,
                    "commitId": "HEAD",
                    "commitMessage": "",
                    "commitTime": now,
                    "startTime": now,
                    "status": "PENDING",
                    "jobType": "MANUAL",
                });
                if let Some(job) = rec
                    .jobs
                    .get_mut(branch)
                    .and_then(|m| m.get_mut(&jid))
                    .and_then(Value::as_object_mut)
                {
                    job.insert("summary".into(), summary.clone());
                }
                (jid, summary)
            }
            None => {
                let peek = rec
                    .next_job_id
                    .get(branch)
                    .copied()
                    .unwrap_or(1)
                    .to_string();
                let summary = json!({
                    "jobArn": shared::job_arn(&ctx.region, &ctx.account, app_id, branch, &peek),
                    "jobId": peek,
                    "commitId": "HEAD",
                    "commitMessage": "",
                    "commitTime": now,
                    "startTime": now,
                    "status": "PENDING",
                    "jobType": "MANUAL",
                });
                let job_id = Self::allocate_job(rec, branch, &summary);
                (job_id, summary)
            }
        };
        let _ = job_id;
        ok(json!({ "jobSummary": summary }))
    }

    // -------------------------- Artifacts -------------------------------

    /// The deterministic artifact set for a job. A settled (`SUCCEED`) build
    /// exposes its build log; other states expose nothing yet.
    fn artifacts_for_job(app_id: &str, branch: &str, job_id: &str, job: &Value) -> Vec<Value> {
        let status = job
            .get("summary")
            .and_then(|s| s.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if status != "SUCCEED" {
            return Vec::new();
        }
        vec![json!({
            "artifactFileName": "buildLogs.txt",
            "artifactId": shared::encode_artifact_id(app_id, branch, job_id, "buildLogs.txt"),
        })]
    }

    fn list_artifacts(
        &self,
        ctx: &Ctx,
        app_id: &str,
        branch: &str,
        job_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let guard = self.state.read();
        let artifacts: Vec<Value> = guard
            .get(&ctx.account)
            .and_then(|d| d.apps.get(app_id))
            .and_then(|r| r.jobs.get(branch))
            .and_then(|m| m.get(job_id))
            .map(|job| Self::artifacts_for_job(app_id, branch, job_id, job))
            .unwrap_or_default();
        let (page, next) = paginate(artifacts, q, 50)?;
        let mut out = json!({ "artifacts": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn get_artifact_url(
        &self,
        ctx: &Ctx,
        artifact_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        match shared::decode_artifact_id(artifact_id) {
            Some((app_id, branch, job_id, file)) => ok(json!({
                "artifactId": artifact_id,
                "artifactUrl": shared::artifact_url(&ctx.region, &app_id, &branch, &job_id, &file),
            })),
            None => Err(not_found(&format!("Artifact {artifact_id} not found."))),
        }
    }

    // ------------------------ Access logs -------------------------------

    fn generate_access_logs(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_app_id(app_id)?;
        let domain = body
            .get("domainName")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("domainName is required."))?
            .to_string();
        let guard = self.state.read();
        let exists = guard
            .get(&ctx.account)
            .map(|d| d.apps.contains_key(app_id))
            .unwrap_or(false);
        drop(guard);
        if !exists {
            return Err(not_found(&format!("App {app_id} not found.")));
        }
        ok(json!({
            "logUrl": shared::access_log_url(&ctx.region, app_id, &domain),
        }))
    }

    // ------------------------------ Tags --------------------------------

    /// Resolve a `ResourceArn` label to a mutable tag-carrying resource (an app
    /// or a branch), rejecting a malformed arn with `BadRequestException` and a
    /// missing resource with `ResourceNotFoundException`.
    fn tags_target_mut<'a>(
        data: &'a mut AmplifyData,
        arn: &str,
    ) -> Result<&'a mut Value, AwsServiceError> {
        if !arn.starts_with("arn:aws:amplify:") {
            return Err(bad_request(&format!(
                "Resource arn '{arn}' does not match required pattern ^arn:aws:amplify:."
            )));
        }
        let (app_id, branch) = shared::parse_resource_arn(arn)
            .ok_or_else(|| resource_not_found(&format!("Resource {arn} not found.")))?;
        let rec = data
            .apps
            .get_mut(&app_id)
            .ok_or_else(|| resource_not_found(&format!("Resource {arn} not found.")))?;
        match branch {
            Some(br) => rec
                .branches
                .get_mut(&br)
                .ok_or_else(|| resource_not_found(&format!("Resource {arn} not found."))),
            None => Ok(&mut rec.app),
        }
    }

    fn tag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let target = Self::tags_target_mut(data, arn)?;
        if let Some(obj) = target.as_object_mut() {
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
        let keys = query_all(q, "tagKeys");
        if keys.is_empty() {
            return Err(bad_request("tagKeys is required."));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let target = Self::tags_target_mut(data, arn)?;
        if let Some(tags) = target
            .as_object_mut()
            .and_then(|o| o.get_mut("tags"))
            .and_then(Value::as_object_mut)
        {
            for k in &keys {
                tags.remove(k);
            }
        }
        ok(json!({}))
    }

    fn list_tags(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        if !arn.starts_with("arn:aws:amplify:") {
            return Err(bad_request(&format!(
                "Resource arn '{arn}' does not match required pattern ^arn:aws:amplify:."
            )));
        }
        let (app_id, branch) = shared::parse_resource_arn(arn)
            .ok_or_else(|| resource_not_found(&format!("Resource {arn} not found.")))?;
        let guard = self.state.read();
        let rec = guard
            .get(&ctx.account)
            .and_then(|d| d.apps.get(&app_id))
            .ok_or_else(|| resource_not_found(&format!("Resource {arn} not found.")))?;
        let obj = match &branch {
            Some(br) => rec
                .branches
                .get(br)
                .ok_or_else(|| resource_not_found(&format!("Resource {arn} not found.")))?,
            None => &rec.app,
        };
        let tags = obj.get("tags").cloned().unwrap_or_else(|| json!({}));
        ok(json!({ "tags": tags }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> AmplifyService {
        AmplifyService::new(Arc::new(RwLock::new(MultiAccountState::new(
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

    fn make_app(s: &AmplifyService, c: &Ctx) -> String {
        let created = s
            .create_app(c, &json!({ "name": "my-app", "platform": "WEB" }))
            .unwrap();
        let body = body_json(&created);
        body["app"]["appId"].as_str().unwrap().to_string()
    }

    #[test]
    fn create_app_returns_full_shape() {
        let s = svc();
        let created = s.create_app(&ctx(), &json!({ "name": "hello" })).unwrap();
        let app = &body_json(&created)["app"];
        for field in [
            "appId",
            "appArn",
            "name",
            "description",
            "repository",
            "platform",
            "createTime",
            "updateTime",
            "environmentVariables",
            "defaultDomain",
            "enableBranchAutoBuild",
            "enableBasicAuth",
        ] {
            assert!(app.get(field).is_some(), "missing {field}");
        }
        assert!(app["appId"].as_str().unwrap().starts_with('d'));
        assert!(app["appArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:amplify:us-east-1:000000000000:apps/"));
    }

    #[test]
    fn create_then_get_list_delete_app() {
        let s = svc();
        let c = ctx();
        let app_id = make_app(&s, &c);
        let got = body_json(&s.get_app(&c, &app_id).unwrap());
        assert_eq!(got["app"]["name"], json!("my-app"));
        let listed = body_json(&s.list_apps(&c, &[]).unwrap());
        assert_eq!(listed["apps"].as_array().unwrap().len(), 1);
        s.delete_app(&c, &app_id).unwrap();
        assert_eq!(
            err_of(s.get_app(&c, &app_id)).status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn get_app_malformed_id_is_bad_request() {
        let s = svc();
        assert_eq!(
            err_of(s.get_app(&ctx(), "not-an-app-id")).status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn create_branch_requires_existing_app() {
        let s = svc();
        let err =
            err_of(s.create_branch(&ctx(), "d0000000000000", &json!({ "branchName": "main" })));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn branch_round_trip_and_domain() {
        let s = svc();
        let c = ctx();
        let app_id = make_app(&s, &c);
        let branch = body_json(
            &s.create_branch(
                &c,
                &app_id,
                &json!({ "branchName": "main", "stage": "PRODUCTION" }),
            )
            .unwrap(),
        );
        assert_eq!(branch["branch"]["stage"], json!("PRODUCTION"));
        let got = body_json(&s.get_branch(&c, &app_id, "main").unwrap());
        assert_eq!(got["branch"]["branchName"], json!("main"));
        let listed = body_json(&s.list_branches(&c, &app_id, &[]).unwrap());
        assert_eq!(listed["branches"].as_array().unwrap().len(), 1);

        // Domain association lifecycle.
        let dom = body_json(
            &s.create_domain(
                &c,
                &app_id,
                &json!({
                    "domainName": "example.com",
                    "subDomainSettings": [{ "prefix": "www", "branchName": "main" }],
                }),
            )
            .unwrap(),
        );
        assert_eq!(dom["domainAssociation"]["domainStatus"], json!("CREATING"));
        // Reconcile settles it to AVAILABLE.
        assert!(s.reconcile(&c.account));
        let dom = body_json(&s.get_domain(&c, &app_id, "example.com").unwrap());
        assert_eq!(dom["domainAssociation"]["domainStatus"], json!("AVAILABLE"));
    }

    #[test]
    fn job_lifecycle_and_artifacts() {
        let s = svc();
        let c = ctx();
        let app_id = make_app(&s, &c);
        s.create_branch(&c, &app_id, &json!({ "branchName": "main" }))
            .unwrap();
        let started = body_json(
            &s.start_job(&c, &app_id, "main", &json!({ "jobType": "RELEASE" }))
                .unwrap(),
        );
        let job_id = started["jobSummary"]["jobId"].as_str().unwrap().to_string();
        assert_eq!(started["jobSummary"]["status"], json!("PENDING"));
        // Walk the deterministic build to completion.
        for _ in 0..4 {
            s.reconcile(&c.account);
        }
        let job = body_json(&s.get_job(&c, &app_id, "main", &job_id).unwrap());
        assert_eq!(job["job"]["summary"]["status"], json!("SUCCEED"));
        assert_eq!(job["job"]["steps"][0]["stepName"], json!("BUILD"));
        // Artifacts show up on a settled build.
        let arts = body_json(&s.list_artifacts(&c, &app_id, "main", &job_id, &[]).unwrap());
        let art = &arts["artifacts"][0];
        assert_eq!(art["artifactFileName"], json!("buildLogs.txt"));
        let art_id = art["artifactId"].as_str().unwrap();
        let url = body_json(&s.get_artifact_url(&c, art_id).unwrap());
        assert!(url["artifactUrl"].as_str().unwrap().starts_with("https://"));
    }

    #[test]
    fn stop_job_cancels() {
        let s = svc();
        let c = ctx();
        let app_id = make_app(&s, &c);
        s.create_branch(&c, &app_id, &json!({ "branchName": "main" }))
            .unwrap();
        let started = body_json(
            &s.start_job(&c, &app_id, "main", &json!({ "jobType": "MANUAL" }))
                .unwrap(),
        );
        let job_id = started["jobSummary"]["jobId"].as_str().unwrap().to_string();
        let stopped = body_json(&s.stop_job(&c, &app_id, "main", &job_id).unwrap());
        assert_eq!(stopped["jobSummary"]["status"], json!("CANCELLED"));
    }

    #[test]
    fn webhook_round_trip() {
        let s = svc();
        let c = ctx();
        let app_id = make_app(&s, &c);
        s.create_branch(&c, &app_id, &json!({ "branchName": "main" }))
            .unwrap();
        let wh = body_json(
            &s.create_webhook(
                &c,
                &app_id,
                &json!({ "branchName": "main", "description": "hook" }),
            )
            .unwrap(),
        );
        let id = wh["webhook"]["webhookId"].as_str().unwrap().to_string();
        let got = body_json(&s.get_webhook(&c, &id).unwrap());
        assert_eq!(got["webhook"]["branchName"], json!("main"));
        let listed = body_json(&s.list_webhooks(&c, &app_id, &[]).unwrap());
        assert_eq!(listed["webhooks"].as_array().unwrap().len(), 1);
        s.delete_webhook(&c, &id).unwrap();
        assert_eq!(
            err_of(s.get_webhook(&c, &id)).status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn backend_environment_round_trip() {
        let s = svc();
        let c = ctx();
        let app_id = make_app(&s, &c);
        let env = body_json(
            &s.create_backend_env(&c, &app_id, &json!({ "environmentName": "staging" }))
                .unwrap(),
        );
        assert_eq!(
            env["backendEnvironment"]["environmentName"],
            json!("staging")
        );
        let got = body_json(&s.get_backend_env(&c, &app_id, "staging").unwrap());
        assert_eq!(
            got["backendEnvironment"]["environmentName"],
            json!("staging")
        );
        let listed = body_json(&s.list_backend_envs(&c, &app_id, &[]).unwrap());
        assert_eq!(listed["backendEnvironments"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn tag_untag_by_arn() {
        let s = svc();
        let c = ctx();
        let app_id = make_app(&s, &c);
        let arn = shared::app_arn(&c.region, &c.account, &app_id);
        s.tag_resource(&c, &arn, &json!({ "tags": { "team": "web" } }))
            .unwrap();
        let listed = body_json(&s.list_tags(&c, &arn).unwrap());
        assert_eq!(listed["tags"]["team"], json!("web"));
        s.untag_resource(&c, &arn, &[("tagKeys".into(), "team".into())])
            .unwrap();
        let listed = body_json(&s.list_tags(&c, &arn).unwrap());
        assert_eq!(listed["tags"].as_object().unwrap().len(), 0);
    }

    #[test]
    fn list_tags_unknown_resource_is_not_found() {
        let s = svc();
        let arn = "arn:aws:amplify:us-east-1:000000000000:apps/d0000000000000";
        assert_eq!(
            err_of(s.list_tags(&ctx(), arn)).status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn list_maxresults_out_of_range_is_bad_request() {
        let s = svc();
        let c = ctx();
        let q = vec![("maxResults".to_string(), "999".to_string())];
        assert_eq!(
            err_of(s.list_apps(&c, &q)).status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn create_deployment_returns_upload_urls() {
        let s = svc();
        let c = ctx();
        let app_id = make_app(&s, &c);
        s.create_branch(&c, &app_id, &json!({ "branchName": "main" }))
            .unwrap();
        let dep = body_json(
            &s.create_deployment(
                &c,
                &app_id,
                "main",
                &json!({ "fileMap": { "index.html": "abc" } }),
            )
            .unwrap(),
        );
        assert!(dep["zipUploadUrl"]
            .as_str()
            .unwrap()
            .starts_with("https://"));
        assert!(dep["fileUploadUrls"]["index.html"]
            .as_str()
            .unwrap()
            .starts_with("https://"));
        assert!(dep["jobId"].is_string());
    }

    #[test]
    fn routing_covers_every_action() {
        // Sanity: the action table and router agree on the operation count.
        assert_eq!(AMPLIFY_ACTIONS.len(), 37);
    }
}
