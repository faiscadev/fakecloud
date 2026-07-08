//! AWS Serverless Application Repository (`serverlessrepo`) restJson1 dispatch +
//! handlers.
//!
//! The full 14-operation SAR control plane. Requests are routed to an operation
//! by HTTP method + `@http` URI path under the `/applications` prefix; path
//! labels are captured positionally (percent-decoded, so an `applicationId`
//! label that is a full ARN survives with its slashes/colons intact) and query
//! parameters are read from the raw query string. State is account-partitioned
//! and persisted. Each application is stored as one internal JSON record; the
//! handlers project the exact Smithy output shape out of it on read so a
//! response never carries a member the model doesn't declare.

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
use crate::state::{ServerlessRepoData, SharedServerlessRepoState};

/// Every operation name in the AWS Serverless Application Repository Smithy
/// model (14).
pub const SERVERLESSREPO_ACTIONS: &[&str] = &[
    "CreateApplication",
    "CreateApplicationVersion",
    "CreateCloudFormationChangeSet",
    "CreateCloudFormationTemplate",
    "DeleteApplication",
    "GetApplication",
    "GetApplicationPolicy",
    "GetCloudFormationTemplate",
    "ListApplicationDependencies",
    "ListApplicationVersions",
    "ListApplications",
    "PutApplicationPolicy",
    "UnshareApplication",
    "UpdateApplication",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
/// `GetCloudFormationTemplate` is included because it settles a template's
/// `PREPARING` -> `ACTIVE` status on read.
const MUTATING: &[&str] = &[
    "CreateApplication",
    "CreateApplicationVersion",
    "CreateCloudFormationChangeSet",
    "CreateCloudFormationTemplate",
    "DeleteApplication",
    "GetCloudFormationTemplate",
    "PutApplicationPolicy",
    "UnshareApplication",
    "UpdateApplication",
];

pub struct ServerlessRepoService {
    state: SharedServerlessRepoState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl ServerlessRepoService {
    pub fn new(state: SharedServerlessRepoState) -> Self {
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
        let put = m == Method::PUT;
        let del = m == Method::DELETE;
        let patch = m == Method::PATCH;
        let l1 = |a: &str| vec![a.to_string()];
        let l2 = |a: &str, b: &str| vec![a.to_string(), b.to_string()];
        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            ["applications"] if post => ("CreateApplication", vec![]),
            ["applications"] if get => ("ListApplications", vec![]),
            ["applications", id] if get => ("GetApplication", l1(id)),
            ["applications", id] if patch => ("UpdateApplication", l1(id)),
            ["applications", id] if del => ("DeleteApplication", l1(id)),
            ["applications", id, "versions"] if get => ("ListApplicationVersions", l1(id)),
            ["applications", id, "versions", ver] if put => {
                ("CreateApplicationVersion", l2(id, ver))
            }
            ["applications", id, "changesets"] if post => ("CreateCloudFormationChangeSet", l1(id)),
            ["applications", id, "templates"] if post => ("CreateCloudFormationTemplate", l1(id)),
            ["applications", id, "templates", tid] if get => {
                ("GetCloudFormationTemplate", l2(id, tid))
            }
            ["applications", id, "policy"] if get => ("GetApplicationPolicy", l1(id)),
            ["applications", id, "policy"] if put => ("PutApplicationPolicy", l1(id)),
            ["applications", id, "dependencies"] if get => ("ListApplicationDependencies", l1(id)),
            ["applications", id, "unshare"] if post => ("UnshareApplication", l1(id)),
            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for ServerlessRepoService {
    fn service_name(&self) -> &str {
        "serverlessrepo"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some((action, labels)) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let result = self.dispatch(action, &labels, &req);
        let success = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        if MUTATING.contains(&action) && success {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SERVERLESSREPO_ACTIONS
    }
}

/// Per-request account + region + host context.
struct Ctx {
    account: String,
    region: String,
    host: String,
}

impl ServerlessRepoService {
    fn dispatch(
        &self,
        action: &str,
        labels: &[String],
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req)?;
        // Reject a missing `@httpLabel` value: when the client omits a required
        // path parameter the router receives the literal `{Label}` placeholder
        // (or an empty segment). Every label-bearing SAR operation declares
        // `BadRequestException`, so surface it here rather than treating the
        // placeholder as a real identifier.
        for label in labels {
            if label.is_empty() || (label.starts_with('{') && label.ends_with('}')) {
                return Err(bad_request(
                    "The request failed because it is missing a required path parameter.",
                ));
            }
        }
        crate::validate::validate_input(action, &body)?;
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: req.region.clone(),
            host: host_of(req),
        };
        let q = parse_query(&req.raw_query);
        let a = |i: usize| labels.get(i).map(String::as_str).unwrap_or_default();
        match action {
            "CreateApplication" => self.create_application(&ctx, &body),
            "GetApplication" => self.get_application(&ctx, a(0), &q),
            "UpdateApplication" => self.update_application(&ctx, a(0), &body),
            "DeleteApplication" => self.delete_application(&ctx, a(0)),
            "ListApplications" => self.list_applications(&ctx, &q),
            "CreateApplicationVersion" => self.create_application_version(&ctx, a(0), a(1), &body),
            "ListApplicationVersions" => self.list_application_versions(&ctx, a(0), &q),
            "PutApplicationPolicy" => self.put_application_policy(&ctx, a(0), &body),
            "GetApplicationPolicy" => self.get_application_policy(&ctx, a(0)),
            "UnshareApplication" => self.unshare_application(&ctx, a(0), &body),
            "CreateCloudFormationChangeSet" => {
                self.create_cloudformation_change_set(&ctx, a(0), &body)
            }
            "CreateCloudFormationTemplate" => {
                self.create_cloudformation_template(&ctx, a(0), &body)
            }
            "GetCloudFormationTemplate" => self.get_cloudformation_template(&ctx, a(0), a(1)),
            "ListApplicationDependencies" => self.list_application_dependencies(&ctx, a(0), &q),
            _ => Err(AwsServiceError::action_not_implemented(
                "serverlessrepo",
                action,
            )),
        }
    }

    // ---------- applications ----------

    fn create_application(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = str_field(body, "name");
        let app_id = shared::application_arn(&ctx.region, &ctx.account, &name);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.applications.contains_key(&app_id) {
            return Err(conflict(&format!(
                "Application with name '{name}' already exists."
            )));
        }
        let now = shared::now_iso();
        let mut app = new_app_record(&app_id, &name, &now, body);
        // Seed an initial version when a semantic version is supplied.
        if let Some(semver) = body.get("semanticVersion").and_then(Value::as_str) {
            if !semver.is_empty() {
                let version = build_version(ctx, &app_id, semver, &now, body);
                set_version(&mut app, semver, version);
            }
        }
        let out = application_output(&app, latest_version_block(&app));
        data.applications.insert(app_id, app);
        created(out)
    }

    fn get_application(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let app = lookup(&guard, &ctx.account, app_id)?;
        let semver = query_one(q, "semanticVersion");
        let version = version_block_for(app, semver)?;
        ok(application_output(app, version))
    }

    fn update_application(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let app = data
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found_app(app_id))?;
        for (src, dst) in [
            ("author", "author"),
            ("description", "description"),
            ("homePageUrl", "homePageUrl"),
            ("readmeUrl", "readmeUrl"),
            ("readmeBody", "readmeBody"),
        ] {
            if let Some(v) = body.get(src) {
                if !v.is_null() {
                    app[dst] = v.clone();
                }
            }
        }
        if let Some(labels) = body.get("labels") {
            if labels.is_array() {
                app["labels"] = labels.clone();
            }
        }
        let block = latest_version_block(app);
        ok(application_output(app, block))
    }

    fn delete_application(&self, ctx: &Ctx, app_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.applications.remove(app_id).is_none() {
            return Err(not_found_app(app_id));
        }
        no_content()
    }

    fn list_applications(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let items: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.applications.values().map(application_summary).collect())
            .unwrap_or_default();
        let (page, next) = paginate(items, q)?;
        let mut out = Map::new();
        out.insert("applications".into(), json!(page));
        if let Some(n) = next {
            out.insert("nextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    // ---------- versions ----------

    fn create_application_version(
        &self,
        ctx: &Ctx,
        app_id: &str,
        semver: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let now = shared::now_iso();
        // Upsert semantics: create a version even if the parent application was
        // never explicitly created (the create path forwards `applicationId`
        // as a path label, which SAR clients treat as an existing app).
        let app = data
            .applications
            .entry(app_id.to_string())
            .or_insert_with(|| {
                new_app_record(app_id, derive_name(app_id).as_str(), &now, &json!({}))
            });
        let version = build_version(ctx, app_id, semver, &now, body);
        let out = version_output(&version);
        set_version(app, semver, version);
        created(out)
    }

    fn list_application_versions(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let app = lookup(&guard, &ctx.account, app_id)?;
        let mut items: Vec<Value> = app
            .get("versions")
            .and_then(Value::as_object)
            .map(|m| m.values().map(version_summary).collect())
            .unwrap_or_default();
        items.sort_by(|a, b| {
            a.get("semanticVersion")
                .and_then(Value::as_str)
                .cmp(&b.get("semanticVersion").and_then(Value::as_str))
        });
        let (page, next) = paginate(items, q)?;
        let mut out = Map::new();
        out.insert("versions".into(), json!(page));
        if let Some(n) = next {
            out.insert("nextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    // ---------- policy ----------

    fn put_application_policy(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let app = data
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found_app(app_id))?;
        let statements = normalize_statements(body.get("statements"));
        app["policy"] = json!(statements.clone());
        ok(json!({ "statements": statements }))
    }

    fn get_application_policy(
        &self,
        ctx: &Ctx,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let app = lookup(&guard, &ctx.account, app_id)?;
        let statements = app
            .get("policy")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        ok(json!({ "statements": statements }))
    }

    fn unshare_application(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let org = str_field(body, "organizationId");
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let app = data
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found_app(app_id))?;
        if let Some(policy) = app.get_mut("policy").and_then(Value::as_array_mut) {
            for st in policy.iter_mut() {
                if let Some(orgs) = st.get_mut("principalOrgIDs").and_then(Value::as_array_mut) {
                    orgs.retain(|o| o.as_str() != Some(org.as_str()));
                }
            }
        }
        no_content()
    }

    // ---------- CloudFormation integration ----------

    fn create_cloudformation_change_set(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let stack_name = str_field(body, "stackName");
        // The change set is minted with well-formed identifiers. Honest gap:
        // fakecloud's SAR does not drive the CloudFormation service to
        // materialise a real stack/change set -- there is no clean in-process
        // seam and fabricating a stack the CFN service doesn't know about would
        // be worse than an honest, well-shaped identifier.
        let semver = body
            .get("semanticVersion")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                let guard = self.state.read();
                guard
                    .get(&ctx.account)
                    .and_then(|d| d.applications.get(app_id))
                    .and_then(|a| a.get("latestVersion").and_then(Value::as_str))
                    .map(str::to_string)
            });
        let mut out = Map::new();
        out.insert("applicationId".into(), json!(app_id));
        out.insert("changeSetId".into(), json!(shared::new_change_set_id()));
        out.insert(
            "stackId".into(),
            json!(shared::stack_id(&ctx.region, &ctx.account, &stack_name)),
        );
        if let Some(v) = semver {
            out.insert("semanticVersion".into(), json!(v));
        }
        created(Value::Object(out))
    }

    fn create_cloudformation_template(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let app = data
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found_app(app_id))?;
        let semver = body
            .get("semanticVersion")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                app.get("latestVersion")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
        let template_id = shared::new_template_id();
        let now = shared::now_iso();
        let expiration = shared::iso_in_hours(6);
        let url = shared::template_url(&ctx.host, &template_id);
        // Copy the stored version's template body so a later download route
        // could serve it (see honest gap in the service doc).
        let template_body = semver
            .as_deref()
            .and_then(|v| app.get("versions").and_then(|m| m.get(v)))
            .and_then(|ver| ver.get("templateBody"))
            .cloned()
            .unwrap_or(Value::Null);
        let mut record = Map::new();
        record.insert("applicationId".into(), json!(app_id));
        record.insert("templateId".into(), json!(template_id));
        record.insert("creationTime".into(), json!(now));
        record.insert("expirationTime".into(), json!(expiration));
        record.insert("status".into(), json!("PREPARING"));
        record.insert("templateUrl".into(), json!(url));
        record.insert("templateBody".into(), template_body);
        if let Some(v) = &semver {
            record.insert("semanticVersion".into(), json!(v));
        }
        let templates = app
            .as_object_mut()
            .expect("app record is an object")
            .entry("templates")
            .or_insert_with(|| json!({}));
        templates[&template_id] = Value::Object(record.clone());
        created(template_output(&Value::Object(record)))
    }

    fn get_cloudformation_template(
        &self,
        ctx: &Ctx,
        app_id: &str,
        template_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let app = data
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found_app(app_id))?;
        let record = app
            .get_mut("templates")
            .and_then(|t| t.get_mut(template_id))
            .ok_or_else(|| not_found(&format!("Template '{template_id}' does not exist.")))?;
        // Settle PREPARING -> ACTIVE on read.
        if record.get("status").and_then(Value::as_str) == Some("PREPARING") {
            record["status"] = json!("ACTIVE");
        }
        ok(template_output(record))
    }

    fn list_application_dependencies(
        &self,
        ctx: &Ctx,
        app_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let app = lookup(&guard, &ctx.account, app_id)?;
        let semver = query_one(q, "semanticVersion");
        let deps = version_record_for(app, semver)
            .and_then(|ver| ver.get("templateBody").and_then(Value::as_str))
            .and_then(shared::parse_template)
            .map(|t| shared::nested_application_dependencies(&t))
            .unwrap_or_default();
        let (page, next) = paginate(deps, q)?;
        let mut out = Map::new();
        out.insert("dependencies".into(), json!(page));
        if let Some(n) = next {
            out.insert("nextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }
}

// ===================== record construction =====================

/// Build a fresh internal application record from a `CreateApplication` body.
fn new_app_record(app_id: &str, name: &str, now: &str, body: &Value) -> Value {
    let mut app = Map::new();
    app.insert("applicationId".into(), json!(app_id));
    app.insert("name".into(), json!(name));
    app.insert("author".into(), json!(str_field(body, "author")));
    app.insert("description".into(), json!(str_field(body, "description")));
    app.insert("creationTime".into(), json!(now));
    app.insert("isVerifiedAuthor".into(), json!(false));
    for key in [
        "homePageUrl",
        "licenseUrl",
        "licenseBody",
        "readmeUrl",
        "readmeBody",
        "spdxLicenseId",
        "sourceCodeUrl",
    ] {
        if let Some(v) = body.get(key) {
            if !v.is_null() {
                app.insert(key.into(), v.clone());
            }
        }
    }
    if let Some(labels) = body.get("labels").filter(|v| v.is_array()) {
        app.insert("labels".into(), labels.clone());
    }
    app.insert("versions".into(), json!({}));
    app.insert("policy".into(), json!([]));
    app.insert("templates".into(), json!({}));
    app.insert("latestVersion".into(), Value::Null);
    Value::Object(app)
}

/// Build an internal version record, parsing the template for parameter
/// definitions, required capabilities, and resource support.
fn build_version(ctx: &Ctx, app_id: &str, semver: &str, now: &str, body: &Value) -> Value {
    let template_body = body
        .get("templateBody")
        .and_then(Value::as_str)
        .map(str::to_string);
    let parsed = template_body.as_deref().and_then(shared::parse_template);
    let (param_defs, caps) = match &parsed {
        Some(t) => (
            shared::parameter_definitions(t),
            shared::required_capabilities(t),
        ),
        None => (Vec::new(), Vec::new()),
    };
    let template_id = shared::new_template_id();
    let mut v = Map::new();
    v.insert("applicationId".into(), json!(app_id));
    v.insert("creationTime".into(), json!(now));
    v.insert("semanticVersion".into(), json!(semver));
    v.insert("parameterDefinitions".into(), json!(param_defs));
    v.insert("requiredCapabilities".into(), json!(caps));
    v.insert("resourcesSupported".into(), json!(true));
    v.insert(
        "templateUrl".into(),
        json!(shared::template_url(&ctx.host, &template_id)),
    );
    if let Some(tb) = template_body {
        v.insert("templateBody".into(), json!(tb));
    }
    for key in ["sourceCodeUrl", "sourceCodeArchiveUrl"] {
        if let Some(s) = body.get(key).and_then(Value::as_str) {
            if !s.is_empty() {
                v.insert(key.into(), json!(s));
            }
        }
    }
    Value::Object(v)
}

/// Insert a version into an app record and update `latestVersion`.
fn set_version(app: &mut Value, semver: &str, version: Value) {
    if let Some(obj) = app.as_object_mut() {
        obj.entry("versions")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .expect("versions is an object")
            .insert(semver.to_string(), version);
        obj.insert("latestVersion".into(), json!(semver));
    }
}

/// Normalise `PutApplicationPolicy` statements, assigning a `statementId` to
/// any statement that omits one and dropping members the model doesn't declare.
fn normalize_statements(statements: Option<&Value>) -> Vec<Value> {
    let Some(list) = statements.and_then(Value::as_array) else {
        return Vec::new();
    };
    list.iter()
        .map(|st| {
            let mut out = Map::new();
            out.insert(
                "statementId".into(),
                st.get("statementId")
                    .filter(|v| v.is_string())
                    .cloned()
                    .unwrap_or_else(|| json!(uuid::Uuid::new_v4().to_string())),
            );
            out.insert(
                "actions".into(),
                st.get("actions").cloned().unwrap_or_else(|| json!([])),
            );
            out.insert(
                "principals".into(),
                st.get("principals").cloned().unwrap_or_else(|| json!([])),
            );
            if let Some(orgs) = st.get("principalOrgIDs").filter(|v| v.is_array()) {
                out.insert("principalOrgIDs".into(), orgs.clone());
            }
            Value::Object(out)
        })
        .collect()
}

// ===================== output projection =====================

/// Project the internal app record onto the `Application` output shape,
/// attaching the given `Version` block when present. Never emits create-only
/// members (`licenseBody` / `readmeBody` / `sourceCodeUrl`) the shape omits.
fn application_output(app: &Value, version: Option<Value>) -> Value {
    let mut out = Map::new();
    copy_present(&mut out, app, "applicationId");
    copy_present(&mut out, app, "author");
    copy_present(&mut out, app, "creationTime");
    copy_present(&mut out, app, "description");
    copy_present(&mut out, app, "homePageUrl");
    out.insert(
        "isVerifiedAuthor".into(),
        app.get("isVerifiedAuthor").cloned().unwrap_or(json!(false)),
    );
    copy_present(&mut out, app, "labels");
    copy_present(&mut out, app, "licenseUrl");
    copy_present(&mut out, app, "name");
    copy_present(&mut out, app, "readmeUrl");
    copy_present(&mut out, app, "spdxLicenseId");
    if let Some(v) = version {
        out.insert("version".into(), v);
    }
    Value::Object(out)
}

/// Project the internal app record onto the `ApplicationSummary` output shape.
fn application_summary(app: &Value) -> Value {
    let mut out = Map::new();
    copy_present(&mut out, app, "applicationId");
    copy_present(&mut out, app, "author");
    copy_present(&mut out, app, "creationTime");
    copy_present(&mut out, app, "description");
    copy_present(&mut out, app, "homePageUrl");
    copy_present(&mut out, app, "labels");
    copy_present(&mut out, app, "name");
    copy_present(&mut out, app, "spdxLicenseId");
    Value::Object(out)
}

/// Project an internal version record onto the `Version` output shape (the
/// block embedded in `GetApplication` / `CreateApplication`).
fn version_output(version: &Value) -> Value {
    let mut out = Map::new();
    copy_present(&mut out, version, "applicationId");
    copy_present(&mut out, version, "creationTime");
    copy_present(&mut out, version, "semanticVersion");
    out.insert(
        "parameterDefinitions".into(),
        version
            .get("parameterDefinitions")
            .cloned()
            .unwrap_or(json!([])),
    );
    out.insert(
        "requiredCapabilities".into(),
        version
            .get("requiredCapabilities")
            .cloned()
            .unwrap_or(json!([])),
    );
    out.insert(
        "resourcesSupported".into(),
        version
            .get("resourcesSupported")
            .cloned()
            .unwrap_or(json!(true)),
    );
    copy_present(&mut out, version, "templateUrl");
    copy_present(&mut out, version, "sourceCodeUrl");
    copy_present(&mut out, version, "sourceCodeArchiveUrl");
    Value::Object(out)
}

/// Project an internal version record onto the `VersionSummary` output shape.
fn version_summary(version: &Value) -> Value {
    let mut out = Map::new();
    copy_present(&mut out, version, "applicationId");
    copy_present(&mut out, version, "creationTime");
    copy_present(&mut out, version, "semanticVersion");
    copy_present(&mut out, version, "sourceCodeUrl");
    Value::Object(out)
}

/// Project a template record onto the CloudFormation-template output shape.
fn template_output(record: &Value) -> Value {
    let mut out = Map::new();
    copy_present(&mut out, record, "applicationId");
    copy_present(&mut out, record, "creationTime");
    copy_present(&mut out, record, "expirationTime");
    copy_present(&mut out, record, "semanticVersion");
    copy_present(&mut out, record, "status");
    copy_present(&mut out, record, "templateId");
    copy_present(&mut out, record, "templateUrl");
    Value::Object(out)
}

/// The `Version` block for the app's latest version, if any.
fn latest_version_block(app: &Value) -> Option<Value> {
    let semver = app.get("latestVersion").and_then(Value::as_str)?;
    let version = app.get("versions").and_then(|m| m.get(semver))?;
    Some(version_output(version))
}

/// Resolve the internal version record for an explicit semantic version (or the
/// latest when unspecified), erroring `NotFoundException` for an unknown
/// explicit version.
fn version_record_for<'a>(app: &'a Value, semver: Option<&str>) -> Option<&'a Value> {
    match semver {
        Some(v) => app.get("versions").and_then(|m| m.get(v)),
        None => {
            let latest = app.get("latestVersion").and_then(Value::as_str)?;
            app.get("versions").and_then(|m| m.get(latest))
        }
    }
}

/// The `Version` output block for an explicit/latest semantic version. Returns
/// `Ok(None)` when the app has no versions and none was requested; errors when
/// an explicitly-requested version does not exist.
fn version_block_for(app: &Value, semver: Option<&str>) -> Result<Option<Value>, AwsServiceError> {
    if let Some(v) = semver {
        return match app.get("versions").and_then(|m| m.get(v)) {
            Some(ver) => Ok(Some(version_output(ver))),
            None => Err(not_found(&format!("Version '{v}' does not exist."))),
        };
    }
    Ok(latest_version_block(app))
}

// ===================== helpers =====================

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn created(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::CREATED, v))
}

fn no_content() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::NO_CONTENT, ""))
}

fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| bad_request(&format!("The request body is malformed: {e}")))
}

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg)
}

fn not_found_app(app_id: &str) -> AwsServiceError {
    not_found(&format!("Application with id '{app_id}' does not exist."))
}

fn conflict(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg)
}

/// Look up an application read-only, erroring `NotFoundException` when absent.
fn lookup<'a>(
    guard: &'a fakecloud_core::multi_account::MultiAccountState<ServerlessRepoData>,
    account: &str,
    app_id: &str,
) -> Result<&'a Value, AwsServiceError> {
    guard
        .get(account)
        .and_then(|d| d.applications.get(app_id))
        .ok_or_else(|| not_found_app(app_id))
}

fn str_field(body: &Value, key: &str) -> String {
    body.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Derive a display name from an application id that may be an ARN
/// (`...:applications/<name>`) or a bare identifier.
fn derive_name(app_id: &str) -> String {
    app_id
        .rsplit(['/', ':'])
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(app_id)
        .to_string()
}

/// Copy a member from `src` into `out` verbatim when present and non-null.
fn copy_present(out: &mut Map<String, Value>, src: &Value, key: &str) {
    if let Some(v) = src.get(key) {
        if !v.is_null() {
            out.insert(key.to_string(), v.clone());
        }
    }
}

fn host_of(req: &AwsRequest) -> String {
    req.headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
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

/// Paginate wire objects using the request's `maxItems` / `nextToken` query
/// params. SAR list ops cap `maxItems` at `[1, 100]`.
fn paginate(
    items: Vec<Value>,
    q: &[(String, String)],
) -> Result<(Vec<Value>, Option<String>), AwsServiceError> {
    let max = match query_one(q, "maxItems") {
        Some(v) => {
            let n: i64 = v
                .parse()
                .map_err(|_| bad_request("maxItems must be an integer."))?;
            if !(1..=100).contains(&n) {
                return Err(bad_request("maxItems must be between 1 and 100."));
            }
            n as usize
        }
        None => 100,
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

#[cfg(test)]
mod tests;
