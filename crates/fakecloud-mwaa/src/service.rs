//! Amazon MWAA (`mwaa`) restJson1 dispatch + operation handlers.
//!
//! The full 12-operation Amazon MWAA control plane. Requests are routed to an
//! operation by HTTP method + `@http` URI path; path labels are captured
//! positionally (percent-decoded) and query parameters are read from the raw
//! query string so repeated multi-value keys survive. State is
//! account-partitioned and persisted; each environment is stored as its
//! already-output-valid `Environment` wire object so `GetEnvironment` echoes
//! exactly what `CreateEnvironment` / `UpdateEnvironment` persisted, and the
//! async CREATING -> AVAILABLE / UPDATING -> AVAILABLE / DELETING lifecycle
//! settles on the next read.

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
use crate::state::{MwaaData, SharedMwaaState};

/// Every operation name in the Amazon MWAA Smithy model (12 operations).
pub const MWAA_ACTIONS: &[&str] = &[
    "CreateCliToken",
    "CreateEnvironment",
    "CreateWebLoginToken",
    "DeleteEnvironment",
    "GetEnvironment",
    "InvokeRestApi",
    "ListEnvironments",
    "ListTagsForResource",
    "PublishMetrics",
    "TagResource",
    "UntagResource",
    "UpdateEnvironment",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &[
    "CreateEnvironment",
    "UpdateEnvironment",
    "DeleteEnvironment",
    "TagResource",
    "UntagResource",
];

pub struct MwaaService {
    state: SharedMwaaState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl MwaaService {
    pub fn new(state: SharedMwaaState) -> Self {
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

    /// Settle any in-flight environment lifecycle transition for the account.
    /// Returns `true` if a transition fired (so the caller persists the change).
    fn reconcile(&self, account: &str) -> bool {
        let mut guard = self.state.write();
        guard.get_or_create(account).reconcile_environments()
    }

    /// Route a request to an operation name + captured path labels by HTTP
    /// method + `@http` URI path. Returns `None` when no route matches.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<String>)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        // NB: a trailing slash is intentionally NOT stripped, so an empty path
        // label (`PUT /environments/`, from a too-short negative probe) yields a
        // trailing empty segment and still routes to its operation, which then
        // rejects the empty label with a `ValidationException` -- rather than
        // collapsing `/environments/` back onto the `ListEnvironments` route.
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
        let put = m == Method::PUT;
        let patch = m == Method::PATCH;
        let del = m == Method::DELETE;
        let l = |v: &str| vec![v.to_string()];
        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            ["environments"] if get => ("ListEnvironments", vec![]),
            ["environments", name] if put => ("CreateEnvironment", l(name)),
            ["environments", name] if get => ("GetEnvironment", l(name)),
            ["environments", name] if patch => ("UpdateEnvironment", l(name)),
            ["environments", name] if del => ("DeleteEnvironment", l(name)),
            ["clitoken", name] if post => ("CreateCliToken", l(name)),
            ["webtoken", name] if post => ("CreateWebLoginToken", l(name)),
            ["restapi", name] if post => ("InvokeRestApi", l(name)),
            ["metrics", "environments", name] if post => ("PublishMetrics", l(name)),
            ["tags", arn] if post => ("TagResource", l(arn)),
            ["tags", arn] if get => ("ListTagsForResource", l(arn)),
            ["tags", arn] if del => ("UntagResource", l(arn)),
            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for MwaaService {
    fn service_name(&self) -> &str {
        "mwaa"
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
        MWAA_ACTIONS
    }
}

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

impl MwaaService {
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
        let result = match action {
            "CreateEnvironment" => self.create_environment(&ctx, a0, &body),
            "GetEnvironment" => self.get_environment(&ctx, a0),
            "UpdateEnvironment" => self.update_environment(&ctx, a0, &body),
            "DeleteEnvironment" => self.delete_environment(&ctx, a0),
            "ListEnvironments" => self.list_environments(&ctx, &q),
            "CreateCliToken" => self.create_cli_token(&ctx, a0),
            "CreateWebLoginToken" => self.create_web_login_token(&ctx, a0),
            "InvokeRestApi" => self.invoke_rest_api(&ctx, a0),
            "PublishMetrics" => self.publish_metrics(&ctx, a0),
            "TagResource" => self.tag_resource(&ctx, a0, &body),
            "UntagResource" => self.untag_resource(&ctx, a0, &q),
            "ListTagsForResource" => self.list_tags_for_resource(&ctx, a0),
            _ => Err(AwsServiceError::action_not_implemented("mwaa", action)),
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

/// Whether a name matches MWAA's `EnvironmentName` pattern
/// (`^[a-zA-Z][0-9a-zA-Z-_]*$`, length 1..=80).
fn valid_env_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 80 {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Reject a `Name` / `EnvironmentName` path label that violates the
/// `EnvironmentName` constraint (empty, over 80 chars, or off-pattern) with
/// `ValidationException`, matching AWS's label validation before lookup.
fn check_name_label(name: &str) -> Result<(), AwsServiceError> {
    if valid_env_name(name) {
        Ok(())
    } else {
        Err(validation(&format!(
            "Environment name '{name}' does not match required pattern \
             ^[a-zA-Z][0-9a-zA-Z-_]*$ (1-80 characters)."
        )))
    }
}

/// Read an integer body member, if present and numeric.
fn int_or(body: &Value, key: &str, default: i64) -> i64 {
    body.get(key).and_then(Value::as_i64).unwrap_or(default)
}

/// The five Airflow log modules and their CloudWatch log-group suffixes.
const LOG_MODULES: &[(&str, &str)] = &[
    ("DagProcessingLogs", "DAGProcessing"),
    ("SchedulerLogs", "Scheduler"),
    ("WebserverLogs", "WebServer"),
    ("WorkerLogs", "Worker"),
    ("TaskLogs", "Task"),
];

/// Build the output `LoggingConfiguration` from the request's
/// `LoggingConfigurationInput`, projecting each module to a
/// `ModuleLoggingConfiguration` (adding the `CloudWatchLogGroupArn` when the
/// module is enabled). Every module is always present in the output, matching
/// `GetEnvironment`.
fn build_logging(ctx: &Ctx, name: &str, input: Option<&Value>) -> Value {
    let mut out = Map::new();
    for (member, suffix) in LOG_MODULES {
        let m = input.and_then(|i| i.get(*member));
        let enabled = m
            .and_then(|x| x.get("Enabled"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let level = m
            .and_then(|x| x.get("LogLevel"))
            .and_then(Value::as_str)
            .unwrap_or("WARNING")
            .to_string();
        let mut module = Map::new();
        module.insert("Enabled".into(), json!(enabled));
        module.insert("LogLevel".into(), json!(level));
        if enabled {
            module.insert(
                "CloudWatchLogGroupArn".into(),
                json!(format!(
                    "arn:aws:logs:{}:{}:log-group:airflow-{}-{}:*",
                    ctx.region, ctx.account, name, suffix
                )),
            );
        }
        out.insert((*member).to_string(), Value::Object(module));
    }
    Value::Object(out)
}

impl MwaaService {
    fn create_environment(
        &self,
        ctx: &Ctx,
        name: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_name_label(name)?;
        let arn = shared::environment_arn(&ctx.region, &ctx.account, name);

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);

        // Environment names are unique within an account + region.
        if data.environments.contains_key(name) {
            return Err(validation(&format!(
                "An environment with the name {name} already exists."
            )));
        }

        let mut env = Map::new();
        env.insert("Name".into(), json!(name));
        env.insert("Arn".into(), json!(arn));
        env.insert("Status".into(), json!("CREATING"));
        env.insert("CreatedAt".into(), json!(shared::now_epoch()));
        env.insert(
            "WebserverUrl".into(),
            json!(shared::webserver_url(&ctx.account, &ctx.region, name)),
        );
        env.insert(
            "ServiceRoleArn".into(),
            json!(shared::service_role_arn(&ctx.account)),
        );
        env.insert(
            "CeleryExecutorQueue".into(),
            json!(shared::celery_executor_queue(
                &ctx.account,
                &ctx.region,
                name
            )),
        );

        // Echo required + optional inputs.
        for key in [
            "ExecutionRoleArn",
            "SourceBucketArn",
            "DagS3Path",
            "NetworkConfiguration",
            "KmsKey",
            "PluginsS3Path",
            "PluginsS3ObjectVersion",
            "RequirementsS3Path",
            "RequirementsS3ObjectVersion",
            "StartupScriptS3Path",
            "StartupScriptS3ObjectVersion",
            "AirflowConfigurationOptions",
        ] {
            if let Some(v) = body.get(key) {
                env.insert(key.to_string(), v.clone());
            }
        }

        // Defaulted scalar members (AWS fills these in when omitted).
        env.insert(
            "EnvironmentClass".into(),
            json!(body
                .get("EnvironmentClass")
                .and_then(Value::as_str)
                .unwrap_or("mw1.small")),
        );
        env.insert(
            "AirflowVersion".into(),
            json!(body
                .get("AirflowVersion")
                .and_then(Value::as_str)
                .unwrap_or("2.10.3")),
        );
        env.insert("MaxWorkers".into(), json!(int_or(body, "MaxWorkers", 10)));
        env.insert("MinWorkers".into(), json!(int_or(body, "MinWorkers", 1)));
        env.insert("Schedulers".into(), json!(int_or(body, "Schedulers", 2)));
        env.insert(
            "MinWebservers".into(),
            json!(int_or(body, "MinWebservers", 2)),
        );
        env.insert(
            "MaxWebservers".into(),
            json!(int_or(body, "MaxWebservers", 2)),
        );
        env.insert(
            "WeeklyMaintenanceWindowStart".into(),
            json!(body
                .get("WeeklyMaintenanceWindowStart")
                .and_then(Value::as_str)
                .unwrap_or("SAT:03:30")),
        );
        env.insert(
            "WebserverAccessMode".into(),
            json!(body
                .get("WebserverAccessMode")
                .and_then(Value::as_str)
                .unwrap_or("PUBLIC_ONLY")),
        );
        let endpoint_mgmt = body
            .get("EndpointManagement")
            .and_then(Value::as_str)
            .unwrap_or("SERVICE")
            .to_string();
        env.insert("EndpointManagement".into(), json!(endpoint_mgmt));
        // When the customer manages the VPC endpoints, AWS surfaces the two
        // endpoint service names to connect to.
        if endpoint_mgmt == "CUSTOMER" {
            env.insert(
                "WebserverVpcEndpointService".into(),
                json!(shared::vpc_endpoint_service(&ctx.region, name, "webserver")),
            );
            env.insert(
                "DatabaseVpcEndpointService".into(),
                json!(shared::vpc_endpoint_service(&ctx.region, name, "database")),
            );
        }

        env.insert(
            "LoggingConfiguration".into(),
            build_logging(ctx, name, body.get("LoggingConfiguration")),
        );

        let tags = body
            .get("Tags")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        env.insert("Tags".into(), Value::Object(tags));

        data.environments
            .insert(name.to_string(), Value::Object(env));
        ok(json!({ "Arn": arn }))
    }

    fn get_environment(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        check_name_label(name)?;
        let guard = self.state.read();
        let Some(data) = guard.get(&ctx.account) else {
            return Err(not_found(&format!("Environment {name} not found.")));
        };
        match data.environments.get(name) {
            Some(env) => ok(json!({ "Environment": env })),
            None => Err(not_found(&format!("Environment {name} not found."))),
        }
    }

    fn update_environment(
        &self,
        ctx: &Ctx,
        name: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_name_label(name)?;
        let arn = shared::environment_arn(&ctx.region, &ctx.account, name);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(env) = data
            .environments
            .get_mut(name)
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!("Environment {name} not found.")));
        };

        // Apply the updatable scalar / structure members verbatim.
        for key in [
            "ExecutionRoleArn",
            "AirflowConfigurationOptions",
            "AirflowVersion",
            "DagS3Path",
            "EnvironmentClass",
            "MaxWorkers",
            "MinWorkers",
            "MaxWebservers",
            "MinWebservers",
            "Schedulers",
            "SourceBucketArn",
            "PluginsS3Path",
            "PluginsS3ObjectVersion",
            "RequirementsS3Path",
            "RequirementsS3ObjectVersion",
            "StartupScriptS3Path",
            "StartupScriptS3ObjectVersion",
            "WebserverAccessMode",
            "WeeklyMaintenanceWindowStart",
        ] {
            if let Some(v) = body.get(key) {
                env.insert(key.to_string(), v.clone());
            }
        }

        // `UpdateNetworkConfigurationInput` only carries `SecurityGroupIds`;
        // merge it into the live network configuration.
        if let Some(sg) = body
            .get("NetworkConfiguration")
            .and_then(|n| n.get("SecurityGroupIds"))
        {
            let nc = env
                .entry("NetworkConfiguration")
                .or_insert_with(|| Value::Object(Map::new()));
            if let Some(nc) = nc.as_object_mut() {
                nc.insert("SecurityGroupIds".into(), sg.clone());
            }
        }

        if body.get("LoggingConfiguration").is_some() {
            env.insert(
                "LoggingConfiguration".into(),
                build_logging(ctx, name, body.get("LoggingConfiguration")),
            );
        }

        env.insert("Status".into(), json!("UPDATING"));
        let mut last_update = Map::new();
        last_update.insert("Status".into(), json!("PENDING"));
        last_update.insert("CreatedAt".into(), json!(shared::now_epoch()));
        last_update.insert("Source".into(), json!("api"));
        if let Some(s) = body.get("WorkerReplacementStrategy") {
            last_update.insert("WorkerReplacementStrategy".into(), s.clone());
        }
        env.insert("LastUpdate".into(), Value::Object(last_update));

        ok(json!({ "Arn": arn }))
    }

    fn delete_environment(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        check_name_label(name)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(env) = data
            .environments
            .get_mut(name)
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!("Environment {name} not found.")));
        };
        env.insert("Status".into(), json!("DELETING"));
        // DeleteEnvironmentOutput is empty.
        ok(json!({}))
    }

    fn list_environments(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let names: Vec<String> = guard
            .get(&ctx.account)
            .map(|data| data.environments.keys().cloned().collect())
            .unwrap_or_default();

        // NextToken carries a 2048-char maximum in the model; reject an
        // over-long token rather than silently ignoring it.
        if let Some(tok) = query_one(q, "NextToken") {
            if tok.len() > 2048 {
                return Err(validation("NextToken exceeds the maximum length of 2048."));
            }
        }

        let max = query_one(q, "MaxResults")
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n >= 1)
            .unwrap_or(25);
        let start = query_one(q, "NextToken")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let end = (start + max).min(names.len());
        let page: Vec<Value> = names
            .get(start..end)
            .unwrap_or(&[])
            .iter()
            .map(|n| json!(n))
            .collect();
        let mut out = json!({ "Environments": page });
        if end < names.len() {
            out["NextToken"] = json!(end.to_string());
        }
        ok(out)
    }

    fn create_cli_token(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        self.require_environment(ctx, name)?;
        ok(json!({
            "CliToken": shared::mint_token(),
            "WebServerHostname": shared::webserver_hostname(&ctx.account, &ctx.region, name),
        }))
    }

    fn create_web_login_token(
        &self,
        ctx: &Ctx,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.require_environment(ctx, name)?;
        ok(json!({
            "WebToken": shared::mint_token(),
            "WebServerHostname": shared::webserver_hostname(&ctx.account, &ctx.region, name),
            "IamIdentity": format!("arn:aws:iam::{}:root", ctx.account),
            "AirflowIdentity": "admin",
        }))
    }

    fn invoke_rest_api(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        self.require_environment(ctx, name)?;
        // The Airflow web server (data plane) is provisioned in a later batch;
        // until it is attached, forwarding to the environment's Airflow REST API
        // cannot succeed. Return the modelled server-side error rather than a
        // fabricated response.
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "RestApiServerException",
            "The Airflow web server for this environment is not reachable.",
        ))
    }

    fn publish_metrics(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        check_name_label(name)?;
        // PublishMetrics is an internal, deprecated sink used by the Airflow
        // environment itself; its `MetricData` is documented as ignored. It
        // takes no persisted action and returns an empty body. (The environment
        // name is not required to exist for this internal call.)
        let _ = ctx;
        ok(json!({}))
    }

    fn tag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let env = Self::env_by_arn_mut(data, arn)
            .ok_or_else(|| not_found(&format!("Resource {arn} not found.")))?;
        let tags = env
            .entry("Tags")
            .or_insert_with(|| Value::Object(Map::new()));
        if let (Some(tags), Some(new)) = (
            tags.as_object_mut(),
            body.get("Tags").and_then(Value::as_object),
        ) {
            for (k, v) in new {
                tags.insert(k.clone(), v.clone());
            }
        }
        // TagResourceOutput is empty.
        ok(json!({}))
    }

    fn untag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let env = Self::env_by_arn_mut(data, arn)
            .ok_or_else(|| not_found(&format!("Resource {arn} not found.")))?;
        if let Some(tags) = env.get_mut("Tags").and_then(Value::as_object_mut) {
            for key in query_all(q, "tagKeys") {
                tags.remove(&key);
            }
        }
        // UntagResourceOutput is empty.
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let tags = guard
            .get(&ctx.account)
            .and_then(|data| Self::env_by_arn(data, arn))
            .ok_or_else(|| not_found(&format!("Resource {arn} not found.")))?
            .get("Tags")
            .cloned()
            .unwrap_or_else(|| Value::Object(Map::new()));
        ok(json!({ "Tags": tags }))
    }

    /// Require an environment to exist, returning `ResourceNotFoundException`
    /// otherwise. Used by the token / invoke operations.
    fn require_environment(&self, ctx: &Ctx, name: &str) -> Result<(), AwsServiceError> {
        // NB: a malformed name label is reported here as `ResourceNotFoundException`
        // (a nonexistent environment) rather than `ValidationException`, because
        // `CreateCliToken` -- which routes through here -- declares ONLY
        // `ResourceNotFoundException` in its Smithy model.
        let guard = self.state.read();
        let exists = guard
            .get(&ctx.account)
            .map(|d| d.environments.contains_key(name))
            .unwrap_or(false);
        if exists {
            Ok(())
        } else {
            Err(not_found(&format!("Environment {name} not found.")))
        }
    }

    /// Look up an environment by its ARN (the tagging operations address the
    /// resource by ARN, not name). The ARN's `Name` segment keys the map; a
    /// region mismatch also fails since the ARN carries the region.
    fn env_by_arn<'a>(data: &'a MwaaData, arn: &str) -> Option<&'a Map<String, Value>> {
        let name = shared::name_from_arn(arn)?;
        let env = data.environments.get(name)?.as_object()?;
        (env.get("Arn").and_then(Value::as_str) == Some(arn)).then_some(env)
    }

    fn env_by_arn_mut<'a>(data: &'a mut MwaaData, arn: &str) -> Option<&'a mut Map<String, Value>> {
        let name = shared::name_from_arn(arn)?.to_string();
        let env = data.environments.get_mut(&name)?.as_object_mut()?;
        if env.get("Arn").and_then(Value::as_str) == Some(arn) {
            Some(env)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> MwaaService {
        MwaaService::new(Arc::new(RwLock::new(MultiAccountState::new(
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
            "ExecutionRoleArn": "arn:aws:iam::000000000000:role/mwaa",
            "SourceBucketArn": "arn:aws:s3:::my-dags-bucket",
            "DagS3Path": "dags",
            "NetworkConfiguration": {
                "SubnetIds": ["subnet-aaaa", "subnet-bbbb"],
                "SecurityGroupIds": ["sg-1111"]
            }
        })
    }

    #[test]
    fn create_then_get_roundtrips() {
        let s = svc();
        let c = ctx();
        let created = s.create_environment(&c, "env-1", &create_body()).unwrap();
        assert!(created.status.is_success());

        // Freshly created -> CREATING, then a read settles to AVAILABLE.
        let got = s.get_environment(&c, "env-1").unwrap();
        let body = body_json(&got);
        assert_eq!(body["Environment"]["Status"], json!("CREATING"));
        assert_eq!(
            body["Environment"]["Arn"],
            json!("arn:aws:airflow:us-east-1:000000000000:environment/env-1")
        );

        assert!(s.reconcile(&c.account));
        let got = s.get_environment(&c, "env-1").unwrap();
        assert_eq!(body_json(&got)["Environment"]["Status"], json!("AVAILABLE"));
    }

    #[test]
    fn duplicate_name_is_validation_error() {
        let s = svc();
        let c = ctx();
        s.create_environment(&c, "dup", &create_body()).unwrap();
        let err = err_of(s.create_environment(&c, "dup", &create_body()));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn invalid_name_rejected() {
        let s = svc();
        let err = err_of(s.create_environment(&ctx(), "1-bad-start", &create_body()));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn get_missing_is_not_found() {
        let s = svc();
        let err = err_of(s.get_environment(&ctx(), "nope"));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn list_returns_names() {
        let s = svc();
        let c = ctx();
        s.create_environment(&c, "a-env", &create_body()).unwrap();
        s.create_environment(&c, "b-env", &create_body()).unwrap();
        let out = body_json(&s.list_environments(&c, &[]).unwrap());
        let envs = out["Environments"].as_array().unwrap();
        assert_eq!(envs.len(), 2);
        assert!(envs.contains(&json!("a-env")));
    }

    #[test]
    fn update_sets_updating_then_settles() {
        let s = svc();
        let c = ctx();
        s.create_environment(&c, "up-env", &create_body()).unwrap();
        let patch = json!({ "MaxWorkers": 25, "EnvironmentClass": "mw1.large" });
        s.update_environment(&c, "up-env", &patch).unwrap();
        let got = body_json(&s.get_environment(&c, "up-env").unwrap());
        assert_eq!(got["Environment"]["Status"], json!("UPDATING"));
        assert_eq!(got["Environment"]["MaxWorkers"], json!(25));

        assert!(s.reconcile(&c.account));
        let got = body_json(&s.get_environment(&c, "up-env").unwrap());
        assert_eq!(got["Environment"]["Status"], json!("AVAILABLE"));
        assert_eq!(got["Environment"]["LastUpdate"]["Status"], json!("SUCCESS"));
    }

    #[test]
    fn tag_untag_roundtrip_by_arn() {
        let s = svc();
        let c = ctx();
        s.create_environment(&c, "tag-env", &create_body()).unwrap();
        let arn = "arn:aws:airflow:us-east-1:000000000000:environment/tag-env";
        s.tag_resource(&c, arn, &json!({ "Tags": { "team": "data" } }))
            .unwrap();
        let listed = body_json(&s.list_tags_for_resource(&c, arn).unwrap());
        assert_eq!(listed["Tags"]["team"], json!("data"));

        s.untag_resource(&c, arn, &[("tagKeys".into(), "team".into())])
            .unwrap();
        let listed = body_json(&s.list_tags_for_resource(&c, arn).unwrap());
        assert_eq!(listed["Tags"].as_object().unwrap().len(), 0);
    }

    #[test]
    fn delete_then_get_gone_after_reconcile() {
        let s = svc();
        let c = ctx();
        s.create_environment(&c, "del-env", &create_body()).unwrap();
        s.delete_environment(&c, "del-env").unwrap();
        // DELETING settles to removed on the next reconcile.
        assert!(s.reconcile(&c.account));
        let err = err_of(s.get_environment(&c, "del-env"));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn cli_token_requires_environment() {
        let s = svc();
        let c = ctx();
        assert!(s.create_cli_token(&c, "ghost").is_err());
        s.create_environment(&c, "tok-env", &create_body()).unwrap();
        let out = body_json(&s.create_cli_token(&c, "tok-env").unwrap());
        assert!(!out["CliToken"].as_str().unwrap().is_empty());
        assert!(out["WebServerHostname"]
            .as_str()
            .unwrap()
            .ends_with("airflow.amazonaws.com"));
    }
}
