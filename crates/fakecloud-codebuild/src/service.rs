//! AWS CodeBuild awsJson1.1 dispatch + operation handlers.
//!
//! Implements the full CodeBuild control plane: build projects, builds and
//! build batches, report groups and reports, fleets, webhooks, source
//! credentials, resource policies, curated environment images, and
//! command-execution sandboxes. There is no real build container engine, so a
//! `StartBuild` mints a Build in `IN_PROGRESS` that deterministically settles
//! to a terminal `SUCCEEDED` state on the first `BatchGetBuilds` read (the same
//! lazy-settle pattern EKS and Cloud Map use for async state). Everything else
//! is real, persisted, account-partitioned CRUD.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{CodeBuildState, SharedCodeBuildState};
use crate::validate;

pub const CODEBUILD_ACTIONS: &[&str] = &[
    "BatchDeleteBuilds",
    "BatchGetBuildBatches",
    "BatchGetBuilds",
    "BatchGetCommandExecutions",
    "BatchGetFleets",
    "BatchGetProjects",
    "BatchGetReportGroups",
    "BatchGetReports",
    "BatchGetSandboxes",
    "CreateFleet",
    "CreateProject",
    "CreateReportGroup",
    "CreateWebhook",
    "DeleteBuildBatch",
    "DeleteFleet",
    "DeleteProject",
    "DeleteReport",
    "DeleteReportGroup",
    "DeleteResourcePolicy",
    "DeleteSourceCredentials",
    "DeleteWebhook",
    "DescribeCodeCoverages",
    "DescribeTestCases",
    "GetReportGroupTrend",
    "GetResourcePolicy",
    "ImportSourceCredentials",
    "InvalidateProjectCache",
    "ListBuildBatches",
    "ListBuildBatchesForProject",
    "ListBuilds",
    "ListBuildsForProject",
    "ListCommandExecutionsForSandbox",
    "ListCuratedEnvironmentImages",
    "ListFleets",
    "ListProjects",
    "ListReportGroups",
    "ListReports",
    "ListReportsForReportGroup",
    "ListSandboxes",
    "ListSandboxesForProject",
    "ListSharedProjects",
    "ListSharedReportGroups",
    "ListSourceCredentials",
    "PutResourcePolicy",
    "RetryBuild",
    "RetryBuildBatch",
    "StartBuild",
    "StartBuildBatch",
    "StartCommandExecution",
    "StartSandbox",
    "StartSandboxConnection",
    "StopBuild",
    "StopBuildBatch",
    "StopSandbox",
    "UpdateFleet",
    "UpdateProject",
    "UpdateProjectVisibility",
    "UpdateReportGroup",
    "UpdateWebhook",
];

pub struct CodeBuildService {
    state: SharedCodeBuildState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl CodeBuildService {
    pub fn new(state: SharedCodeBuildState) -> Self {
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
/// snapshot). `BatchGetBuilds`/`BatchGetBuildBatches` settle in-progress
/// records to a terminal state on read, so they persist too.
fn is_mutating(action: &str) -> bool {
    matches!(
        action,
        "BatchDeleteBuilds"
            | "BatchGetBuilds"
            | "BatchGetBuildBatches"
            | "BatchGetCommandExecutions"
            | "CreateFleet"
            | "CreateProject"
            | "CreateReportGroup"
            | "CreateWebhook"
            | "DeleteBuildBatch"
            | "DeleteFleet"
            | "DeleteProject"
            | "DeleteReport"
            | "DeleteReportGroup"
            | "DeleteResourcePolicy"
            | "DeleteSourceCredentials"
            | "DeleteWebhook"
            | "ImportSourceCredentials"
            | "ListCommandExecutionsForSandbox"
            | "PutResourcePolicy"
            | "RetryBuild"
            | "RetryBuildBatch"
            | "StartBuild"
            | "StartBuildBatch"
            | "StartCommandExecution"
            | "StartSandbox"
            | "StopBuild"
            | "StopBuildBatch"
            | "StopSandbox"
            | "UpdateFleet"
            | "UpdateProject"
            | "UpdateProjectVisibility"
            | "UpdateReportGroup"
            | "UpdateWebhook"
    )
}

#[async_trait]
impl AwsService for CodeBuildService {
    fn service_name(&self) -> &str {
        "codebuild"
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
        CODEBUILD_ACTIONS
    }
}

impl CodeBuildService {
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match action {
            "CreateProject" => self.create_project(req),
            "UpdateProject" => self.update_project(req),
            "DeleteProject" => self.delete_project(req),
            "BatchGetProjects" => self.batch_get_projects(req),
            "ListProjects" => self.list_projects(req),
            "ListSharedProjects" => self.list_shared_projects(req),
            "UpdateProjectVisibility" => self.update_project_visibility(req),
            "InvalidateProjectCache" => self.invalidate_project_cache(req),
            "StartBuild" => self.start_build(req),
            "StopBuild" => self.stop_build(req),
            "RetryBuild" => self.retry_build(req),
            "BatchGetBuilds" => self.batch_get_builds(req),
            "BatchDeleteBuilds" => self.batch_delete_builds(req),
            "ListBuilds" => self.list_builds(req),
            "ListBuildsForProject" => self.list_builds_for_project(req),
            "StartBuildBatch" => self.start_build_batch(req),
            "StopBuildBatch" => self.stop_build_batch(req),
            "RetryBuildBatch" => self.retry_build_batch(req),
            "DeleteBuildBatch" => self.delete_build_batch(req),
            "BatchGetBuildBatches" => self.batch_get_build_batches(req),
            "ListBuildBatches" => self.list_build_batches(req),
            "ListBuildBatchesForProject" => self.list_build_batches_for_project(req),
            "CreateReportGroup" => self.create_report_group(req),
            "UpdateReportGroup" => self.update_report_group(req),
            "DeleteReportGroup" => self.delete_report_group(req),
            "BatchGetReportGroups" => self.batch_get_report_groups(req),
            "ListReportGroups" => self.list_report_groups(req),
            "ListSharedReportGroups" => self.list_shared_report_groups(req),
            "GetReportGroupTrend" => self.get_report_group_trend(req),
            "DeleteReport" => self.delete_report(req),
            "BatchGetReports" => self.batch_get_reports(req),
            "ListReports" => self.list_reports(req),
            "ListReportsForReportGroup" => self.list_reports_for_report_group(req),
            "DescribeTestCases" => self.describe_test_cases(req),
            "DescribeCodeCoverages" => self.describe_code_coverages(req),
            "CreateFleet" => self.create_fleet(req),
            "UpdateFleet" => self.update_fleet(req),
            "DeleteFleet" => self.delete_fleet(req),
            "BatchGetFleets" => self.batch_get_fleets(req),
            "ListFleets" => self.list_fleets(req),
            "CreateWebhook" => self.create_webhook(req),
            "UpdateWebhook" => self.update_webhook(req),
            "DeleteWebhook" => self.delete_webhook(req),
            "ImportSourceCredentials" => self.import_source_credentials(req),
            "DeleteSourceCredentials" => self.delete_source_credentials(req),
            "ListSourceCredentials" => self.list_source_credentials(req),
            "PutResourcePolicy" => self.put_resource_policy(req),
            "GetResourcePolicy" => self.get_resource_policy(req),
            "DeleteResourcePolicy" => self.delete_resource_policy(req),
            "ListCuratedEnvironmentImages" => self.list_curated_environment_images(req),
            "StartSandbox" => self.start_sandbox(req),
            "StopSandbox" => self.stop_sandbox(req),
            "StartSandboxConnection" => self.start_sandbox_connection(req),
            "BatchGetSandboxes" => self.batch_get_sandboxes(req),
            "ListSandboxes" => self.list_sandboxes(req),
            "ListSandboxesForProject" => self.list_sandboxes_for_project(req),
            "StartCommandExecution" => self.start_command_execution(req),
            "BatchGetCommandExecutions" => self.batch_get_command_executions(req),
            "ListCommandExecutionsForSandbox" => self.list_command_executions_for_sandbox(req),
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

fn gen_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn invalid(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidInputException", msg)
}
fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ResourceNotFoundException", msg)
}
fn already_exists(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceAlreadyExistsException",
        msg,
    )
}
fn oauth_provider(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "OAuthProviderException", msg)
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

fn arn(region: &str, account: &str, resource: &str) -> String {
    format!("arn:aws:codebuild:{region}:{account}:{resource}")
}

/// Validate a required string field is present and within `[min, max]`
/// characters (the model's `@length` bound). `max = usize::MAX` means no
/// upper bound (a `NonEmptyString`).
fn req_len(b: &Value, key: &str, min: usize, max: usize) -> Result<String, AwsServiceError> {
    let v = str_field(b, key).ok_or_else(|| invalid(format!("Member {key} must be specified")))?;
    check_len(key, &v, min, max)?;
    Ok(v)
}

/// Validate an optional string field, if present, is within `[min, max]`.
fn opt_len(b: &Value, key: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if let Some(v) = str_field(b, key) {
        check_len(key, &v, min, max)?;
    }
    Ok(())
}

fn check_len(key: &str, v: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    let len = v.chars().count();
    if len < min || len > max {
        return Err(invalid(format!(
            "Member {key} length {len} outside allowed range [{min}, {max}]"
        )));
    }
    Ok(())
}

/// Validate the `sortOrder`/`sortBy` members shared by the list operations.
fn validate_sort(b: &Value, sort_by: Option<&[&str]>) -> Result<(), AwsServiceError> {
    if let Some(so) = str_field(b, "sortOrder") {
        if !validate::is_enum(validate::SORT_ORDER, &so) {
            return Err(invalid(format!("Invalid sortOrder: {so}")));
        }
    }
    if let (Some(set), Some(sb)) = (sort_by, str_field(b, "sortBy")) {
        if !validate::is_enum(set, &sb) {
            return Err(invalid(format!("Invalid sortBy: {sb}")));
        }
    }
    Ok(())
}

/// Validate a report group's `exportConfig`: its `exportConfigType` must be a
/// known enum member, and an `S3` export requires an `s3Destination`.
fn validate_export_config(ec: &Value) -> Result<(), AwsServiceError> {
    match ec.get("exportConfigType").and_then(Value::as_str) {
        Some(t) if validate::is_enum(validate::REPORT_EXPORT_CONFIG_TYPE, t) => {
            if t == "S3" && ec.get("s3Destination").map(Value::is_null).unwrap_or(true) {
                return Err(invalid("An s3Destination must be specified for S3 export"));
            }
            Ok(())
        }
        Some(t) => Err(invalid(format!("Invalid export config type: {t}"))),
        None => Err(invalid("An export config type must be specified")),
    }
}

/// Validate a fleet's `environmentType` / `computeType` / `overflowBehavior`
/// enums. On create (`require`) the first two must be present; on update all
/// are optional but any supplied value must be a known enum member.
fn validate_fleet_enums(b: &Value, require: bool) -> Result<(), AwsServiceError> {
    match str_field(b, "environmentType") {
        Some(t) if validate::is_enum(validate::ENVIRONMENT_TYPE, &t) => {}
        Some(t) => return Err(invalid(format!("Invalid environment type: {t}"))),
        None if require => return Err(invalid("An environment type must be specified")),
        None => {}
    }
    match str_field(b, "computeType") {
        Some(t) if validate::is_enum(validate::COMPUTE_TYPE, &t) => {}
        Some(t) => return Err(invalid(format!("Invalid compute type: {t}"))),
        None if require => return Err(invalid("A compute type must be specified")),
        None => {}
    }
    if let Some(t) = str_field(b, "overflowBehavior") {
        if !validate::is_enum(validate::FLEET_OVERFLOW_BEHAVIOR, &t) {
            return Err(invalid(format!("Invalid overflow behavior: {t}")));
        }
    }
    Ok(())
}

/// A record field as a string (empty when absent), used as a sort key / tiebreak.
fn field_str(r: &Value, field: &str) -> String {
    r.get(field)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// A record's timestamp field (epoch seconds) as an f64, defaulting to 0.
fn field_time(r: &Value, field: &str) -> f64 {
    r.get(field).and_then(Value::as_f64).unwrap_or(0.0)
}

/// Order a set of records by the requested `sortBy` (NAME / CREATED_TIME /
/// LAST_MODIFIED_TIME, defaulting to NAME) and `sortOrder` (defaulting to
/// ASCENDING), then project each record to its list element (`out_field`).
/// `name_field` supplies the NAME sort key and the stable tiebreak.
fn sorted_output(
    mut records: Vec<Value>,
    b: &Value,
    name_field: &str,
    out_field: &str,
) -> Vec<Value> {
    let sort_by = str_field(b, "sortBy").unwrap_or_default();
    let descending = str_field(b, "sortOrder").as_deref() == Some("DESCENDING");
    records.sort_by(|x, y| {
        let primary = match sort_by.as_str() {
            "CREATED_TIME" => field_time(x, "created")
                .partial_cmp(&field_time(y, "created"))
                .unwrap_or(std::cmp::Ordering::Equal),
            "LAST_MODIFIED_TIME" => field_time(x, "lastModified")
                .partial_cmp(&field_time(y, "lastModified"))
                .unwrap_or(std::cmp::Ordering::Equal),
            _ => field_str(x, name_field).cmp(&field_str(y, name_field)),
        };
        primary.then_with(|| field_str(x, name_field).cmp(&field_str(y, name_field)))
    });
    if descending {
        records.reverse();
    }
    records
        .iter()
        .filter_map(|r| r.get(out_field).cloned())
        .collect()
}

/// Pull `nextToken`/`maxResults` from the body and paginate a list of JSON
/// values, returning the assembled `{list_key: [...], nextToken?}` object.
/// A `maxResults` outside the model's `[1, 100]` range is rejected.
fn paginate_body(items: Vec<Value>, list_key: &str, b: &Value) -> Result<Value, AwsServiceError> {
    let max = match b.get("maxResults") {
        None | Some(Value::Null) => 100usize,
        Some(v) => {
            let n = v
                .as_i64()
                .ok_or_else(|| invalid("maxResults must be an integer"))?;
            if !(1..=100).contains(&n) {
                return Err(invalid("maxResults must be in range [1, 100]"));
            }
            n as usize
        }
    };
    let token = str_field(b, "nextToken");
    let (page, next) = paginate_checked(&items, token.as_deref(), max)
        .map_err(|_| invalid("Invalid nextToken"))?;
    let mut out = Map::new();
    out.insert(list_key.to_string(), Value::Array(page));
    if let Some(n) = next {
        out.insert("nextToken".to_string(), Value::String(n));
    }
    Ok(Value::Object(out))
}

/// Order an insertion-order id list per the request `sortOrder` and project to
/// JSON strings. The `*_order` vecs are oldest-first (insertion order). AWS's
/// default (absent) `sortOrder` is DESCENDING (newest-first), so DESCENDING
/// reverses the vec and ASCENDING keeps insertion order.
fn apply_sort_order(mut ids: Vec<String>, b: &Value) -> Vec<Value> {
    if str_field(b, "sortOrder").as_deref() != Some("ASCENDING") {
        ids.reverse();
    }
    ids.into_iter().map(Value::String).collect()
}

/// Extract and validate the optional `BuildBatchFilter.status` filter shared by
/// the ListBuildBatches operations. An unknown status value is rejected.
fn build_batch_filter_status(b: &Value) -> Result<Option<String>, AwsServiceError> {
    let Some(status) = b
        .get("filter")
        .and_then(|f| f.get("status"))
        .and_then(Value::as_str)
    else {
        return Ok(None);
    };
    if !validate::is_enum(validate::STATUS_TYPE, status) {
        return Err(invalid(format!("Invalid build batch status: {status}")));
    }
    Ok(Some(status.to_string()))
}

/// Whether a build batch's current (settled) `buildBatchStatus` matches an
/// optional status filter. A `None` filter matches every batch.
fn batch_matches_status(st: &CodeBuildState, id: &str, filter: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(want) => {
            st.build_batches
                .get(id)
                .and_then(|batch| batch.get("buildBatchStatus"))
                .and_then(Value::as_str)
                == Some(want)
        }
    }
}

/// Enforce a CodeBuild name `@pattern` of the form
/// `^[A-Za-z0-9][A-Za-z0-9\-_]{1,N}$`: the first character must be
/// alphanumeric and every remaining character alphanumeric, hyphen, or
/// underscore. The `@length` bound `N` is validated separately by `req_len`.
fn check_name_pattern(key: &str, v: &str) -> Result<(), AwsServiceError> {
    let ok_first = v
        .chars()
        .next()
        .map(|c| c.is_ascii_alphanumeric())
        .unwrap_or(false);
    let ok_rest = v
        .chars()
        .skip(1)
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !ok_first || !ok_rest {
        return Err(invalid(format!(
            "Member {key} does not satisfy the required pattern"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Projects
// ---------------------------------------------------------------------------

impl CodeBuildService {
    fn region_account(&self, req: &AwsRequest) -> (String, String) {
        (req.region.clone(), req.account_id.clone())
    }

    /// Assemble a `Project` output structure from a create/update input body.
    /// Only model-defined `Project` fields are carried, transforming the input
    /// `badgeEnabled` flag into the output `badge` structure.
    fn assemble_project(
        &self,
        input: &Value,
        name: &str,
        arn_str: &str,
        created: DateTime<Utc>,
        last_modified: DateTime<Utc>,
    ) -> Value {
        let mut p = Map::new();
        p.insert("name".into(), json!(name));
        p.insert("arn".into(), json!(arn_str));
        p.insert("created".into(), ts(created));
        p.insert("lastModified".into(), ts(last_modified));
        // Fields whose input name equals the Project output name.
        for k in [
            "description",
            "source",
            "secondarySources",
            "sourceVersion",
            "secondarySourceVersions",
            "artifacts",
            "secondaryArtifacts",
            "cache",
            "environment",
            "serviceRole",
            "timeoutInMinutes",
            "queuedTimeoutInMinutes",
            "encryptionKey",
            "tags",
            "vpcConfig",
            "logsConfig",
            "fileSystemLocations",
            "buildBatchConfig",
            "concurrentBuildLimit",
            "autoRetryLimit",
        ] {
            if let Some(v) = input.get(k) {
                if !v.is_null() {
                    p.insert(k.into(), v.clone());
                }
            }
        }
        // badgeEnabled (input) -> badge (output). Presence-guarded: only emit a
        // `badge` when the caller supplied `badgeEnabled`, so a partial
        // UpdateProject that omits it preserves the stored badge (rather than
        // silently disabling an enabled badge).
        if let Some(badge_enabled) = input.get("badgeEnabled").and_then(Value::as_bool) {
            p.insert("badge".into(), json!({ "badgeEnabled": badge_enabled }));
        }
        Value::Object(p)
    }

    /// Validate the required + enum-constrained members of a project input.
    fn validate_project_input(&self, b: &Value, require: bool) -> Result<(), AwsServiceError> {
        if require && str_field(b, "name").is_none() {
            return Err(invalid("A project name must be specified"));
        }
        if let Some(src) = b.get("source") {
            match src.get("type").and_then(Value::as_str) {
                Some(t) if validate::is_enum(validate::SOURCE_TYPE, t) => {}
                Some(t) => return Err(invalid(format!("Invalid source type: {t}"))),
                None if require => return Err(invalid("Source type must be specified")),
                None => {}
            }
        } else if require {
            return Err(invalid("A source must be specified"));
        }
        if let Some(art) = b.get("artifacts") {
            match art.get("type").and_then(Value::as_str) {
                Some(t) if validate::is_enum(validate::ARTIFACTS_TYPE, t) => {}
                Some(t) => return Err(invalid(format!("Invalid artifacts type: {t}"))),
                None if require => return Err(invalid("Artifacts type must be specified")),
                None => {}
            }
        } else if require {
            return Err(invalid("Artifacts must be specified"));
        }
        if let Some(env) = b.get("environment") {
            match env.get("type").and_then(Value::as_str) {
                Some(t) if validate::is_enum(validate::ENVIRONMENT_TYPE, t) => {}
                Some(t) => return Err(invalid(format!("Invalid environment type: {t}"))),
                None if require => return Err(invalid("Environment type must be specified")),
                None => {}
            }
            match env.get("computeType").and_then(Value::as_str) {
                Some(t) if validate::is_enum(validate::COMPUTE_TYPE, t) => {}
                Some(t) => return Err(invalid(format!("Invalid compute type: {t}"))),
                None if require => return Err(invalid("Compute type must be specified")),
                None => {}
            }
            if require && env.get("image").and_then(Value::as_str).is_none() {
                return Err(invalid("Environment image must be specified"));
            }
        } else if require {
            return Err(invalid("An environment must be specified"));
        }
        if require && str_field(b, "serviceRole").is_none() {
            return Err(invalid("A service role must be specified"));
        }
        Ok(())
    }

    fn create_project(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        self.validate_project_input(&b, true)?;
        let name = req_len(&b, "name", 2, 150)?;
        check_name_pattern("name", &name)?;
        let (region, account) = self.region_account(req);
        let arn_str = arn(&region, &account, &format!("project/{name}"));
        let now = Utc::now();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.projects.contains_key(&name) {
            return Err(already_exists(format!("Project already exists: {name}")));
        }
        let mut project = self.assemble_project(&b, &name, &arn_str, now, now);
        // On create, AWS always reports a badge (default disabled) even when the
        // caller omitted `badgeEnabled`; a partial UpdateProject leaves the
        // stored badge untouched (assemble_project is presence-guarded).
        if let Some(obj) = project.as_object_mut() {
            obj.entry("badge")
                .or_insert_with(|| json!({ "badgeEnabled": false }));
        }
        st.projects.insert(name, project.clone());
        ok(json!({ "project": project }))
    }

    fn update_project(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(name) = str_field(&b, "name") else {
            return Err(invalid("A project name must be specified"));
        };
        self.validate_project_input(&b, false)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(existing) = st.projects.get(&name).cloned() else {
            return Err(not_found(format!("Project does not exist: {name}")));
        };
        let arn_str = existing
            .get("arn")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let created = existing
            .get("created")
            .cloned()
            .unwrap_or_else(|| ts(Utc::now()));
        // Merge: start from the existing project, overlay any provided members.
        let mut merged = existing.clone();
        let updated = self.assemble_project(&b, &name, &arn_str, Utc::now(), Utc::now());
        if let (Some(m), Some(u)) = (merged.as_object_mut(), updated.as_object()) {
            for (k, v) in u {
                // Keep the original created timestamp; refresh lastModified.
                if k == "created" {
                    continue;
                }
                m.insert(k.clone(), v.clone());
            }
            m.insert("created".into(), created);
        }
        st.projects.insert(name, merged.clone());
        ok(json!({ "project": merged }))
    }

    fn delete_project(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_len(&b, "name", 1, usize::MAX)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Removing the project also removes its webhook (stored on the project
        // object) and its per-project build-number counters, so a same-named
        // project recreated later starts numbering from 1. Build/build-batch
        // history is deliberately retained: AWS keeps prior builds queryable by
        // id after a project is deleted.
        if let Some(project) = st.projects.remove(&name) {
            // Drop any resource policy attached to the deleted project so a
            // later GetResourcePolicy 404s instead of returning an orphan.
            if let Some(project_arn) = project.get("arn").and_then(Value::as_str) {
                let project_arn = project_arn.to_string();
                st.resource_policies.remove(&project_arn);
            }
        }
        st.build_numbers.remove(&name);
        st.build_batch_numbers.remove(&name);
        ok(json!({}))
    }

    fn batch_get_projects(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let names = str_list(&b, "names");
        if names.is_empty() {
            return Err(invalid("At least one project name must be specified"));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut found = Vec::new();
        let mut not_found_names = Vec::new();
        for n in names {
            match st.projects.get(&n) {
                Some(p) => found.push(p.clone()),
                None => not_found_names.push(json!(n)),
            }
        }
        ok(json!({
            "projects": found,
            "projectsNotFound": not_found_names,
        }))
    }

    fn list_projects(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, Some(validate::PROJECT_SORT_BY))?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let records: Vec<Value> = st.projects.values().cloned().collect();
        let names = sorted_output(records, &b, "name", "name");
        let out = paginate_body(names, "projects", &b)?;
        ok(out)
    }

    fn list_shared_projects(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, Some(validate::SHARED_RESOURCE_SORT_BY))?;
        // Nothing is cross-account shared in fakecloud, so no project is shared
        // *with* this account -> an empty list is the honest representation.
        ok(paginate_body(vec![], "projects", &b)?)
    }

    fn update_project_visibility(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(project_arn) = str_field(&b, "projectArn") else {
            return Err(invalid("A project ARN must be specified"));
        };
        let visibility = str_field(&b, "projectVisibility").unwrap_or_default();
        if !validate::is_enum(validate::PROJECT_VISIBILITY, &visibility) {
            return Err(invalid(format!("Invalid project visibility: {visibility}")));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Locate the project by ARN.
        let name = project_arn.rsplit('/').next().unwrap_or("").to_string();
        let Some(project) = st.projects.get_mut(&name) else {
            return Err(not_found(format!("Project does not exist: {project_arn}")));
        };
        // A public alias only exists while the project is publicly readable.
        // Switching to PRIVATE clears any previously-stored alias, and the
        // response only carries an alias for PUBLIC_READ.
        let public_alias = if visibility == "PUBLIC_READ" {
            Some(format!("{}-{}", account, &gen_uuid()[..8]))
        } else {
            None
        };
        if let Some(obj) = project.as_object_mut() {
            obj.insert("projectVisibility".into(), json!(visibility));
            match &public_alias {
                Some(alias) => {
                    obj.insert("publicProjectAlias".into(), json!(alias));
                }
                None => {
                    obj.remove("publicProjectAlias");
                }
            }
        }
        let mut resp = Map::new();
        resp.insert("projectArn".into(), json!(project_arn));
        resp.insert("projectVisibility".into(), json!(visibility));
        if let Some(alias) = public_alias {
            resp.insert("publicProjectAlias".into(), json!(alias));
        }
        ok(Value::Object(resp))
    }

    fn invalidate_project_cache(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(name) = str_field(&b, "projectName") else {
            return Err(invalid("A project name must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.projects.contains_key(&name) {
            return Err(not_found(format!("Project does not exist: {name}")));
        }
        ok(json!({}))
    }
}

// ---------------------------------------------------------------------------
// Builds
// ---------------------------------------------------------------------------

/// Build the ordered phase list of a settled (successful) build.
fn settled_phases(start: DateTime<Utc>, end: DateTime<Utc>) -> Value {
    let phase = |t: &str| {
        json!({
            "phaseType": t,
            "phaseStatus": "SUCCEEDED",
            "startTime": ts(start),
            "endTime": ts(end),
            "durationInSeconds": 0,
            "contexts": [],
        })
    };
    json!([
        phase("SUBMITTED"),
        phase("QUEUED"),
        phase("PROVISIONING"),
        phase("DOWNLOAD_SOURCE"),
        phase("INSTALL"),
        phase("PRE_BUILD"),
        phase("BUILD"),
        phase("POST_BUILD"),
        phase("UPLOAD_ARTIFACTS"),
        phase("FINALIZING"),
        json!({
            "phaseType": "COMPLETED",
            "startTime": ts(end),
        }),
    ])
}

/// Settle an in-progress build/build-batch record to a terminal SUCCEEDED
/// state on read. Returns true if the record was mutated.
fn settle_build(record: &mut Value, status_key: &str) -> bool {
    let in_progress = record
        .get(status_key)
        .and_then(Value::as_str)
        .map(|s| s == "IN_PROGRESS")
        .unwrap_or(false);
    if !in_progress {
        return false;
    }
    let start = record
        .get("startTime")
        .and_then(Value::as_f64)
        .map(|s| DateTime::from_timestamp(s as i64, 0).unwrap_or_else(Utc::now))
        .unwrap_or_else(Utc::now);
    let end = Utc::now();
    if let Some(obj) = record.as_object_mut() {
        obj.insert(status_key.into(), json!("SUCCEEDED"));
        obj.insert("currentPhase".into(), json!("COMPLETED"));
        obj.insert("endTime".into(), ts(end));
        obj.insert("phases".into(), settled_phases(start, end));
    }
    true
}

/// Next per-project `buildNumber`. Monotonic within a project (starts at 1),
/// only ever increments, and is never reused even after `BatchDeleteBuilds`
/// shrinks the build set, matching AWS.
fn next_build_number(st: &mut CodeBuildState, project: &str) -> i64 {
    let n = st.build_numbers.entry(project.to_string()).or_insert(0);
    *n += 1;
    *n
}

/// Settle an in-progress command execution to a terminal `SUCCEEDED` state on
/// read (the same lazy-settle contract builds use). Returns true if mutated.
fn settle_command_execution(record: &mut Value) -> bool {
    if record.get("status").and_then(Value::as_str) != Some("IN_PROGRESS") {
        return false;
    }
    if let Some(obj) = record.as_object_mut() {
        obj.insert("status".into(), json!("SUCCEEDED"));
        obj.insert("endTime".into(), ts(Utc::now()));
        obj.insert("exitCode".into(), json!("0"));
    }
    true
}

/// Next per-project `buildBatchNumber`, with the same monotonic semantics.
fn next_build_batch_number(st: &mut CodeBuildState, project: &str) -> i64 {
    let n = st
        .build_batch_numbers
        .entry(project.to_string())
        .or_insert(0);
    *n += 1;
    *n
}

impl CodeBuildService {
    /// Resolve the project a build/build-batch runs against, applying the
    /// simple overrides the probe and Terraform exercise. Returns the base
    /// build record fields (source/environment/artifacts/etc.).
    fn build_from_project(&self, project: &Value, b: &Value) -> Map<String, Value> {
        let mut fields = Map::new();
        for (out_key, proj_key, override_key) in [
            ("source", "source", "sourceOverride"),
            (
                "secondarySources",
                "secondarySources",
                "secondarySourcesOverride",
            ),
            ("artifacts", "artifacts", "artifactsOverride"),
            (
                "secondaryArtifacts",
                "secondaryArtifacts",
                "secondaryArtifactsOverride",
            ),
            ("cache", "cache", "cacheOverride"),
            ("environment", "environment", "environmentOverride"),
            ("serviceRole", "serviceRole", "serviceRoleOverride"),
            ("encryptionKey", "encryptionKey", "encryptionKeyOverride"),
            ("logs", "logsConfig", "logsConfigOverride"),
            (
                "timeoutInMinutes",
                "timeoutInMinutes",
                "timeoutInMinutesOverride",
            ),
            (
                "queuedTimeoutInMinutes",
                "queuedTimeoutInMinutes",
                "queuedTimeoutInMinutesOverride",
            ),
            ("fileSystemLocations", "fileSystemLocations", "_none"),
            ("vpcConfig", "vpcConfig", "_none"),
        ] {
            if let Some(v) = b.get(override_key).filter(|v| !v.is_null()) {
                fields.insert(out_key.into(), v.clone());
            } else if let Some(v) = project.get(proj_key).filter(|v| !v.is_null()) {
                fields.insert(out_key.into(), v.clone());
            }
        }
        // Scalar environment overrides fold into the resolved environment.
        if let Some(env) = fields.get_mut("environment").and_then(Value::as_object_mut) {
            for (env_key, ov_key) in [
                ("type", "environmentTypeOverride"),
                ("image", "imageOverride"),
                ("computeType", "computeTypeOverride"),
                ("certificate", "certificateOverride"),
                ("privilegedMode", "privilegedModeOverride"),
            ] {
                if let Some(v) = b.get(ov_key).filter(|v| !v.is_null()) {
                    env.insert(env_key.into(), v.clone());
                }
            }
            if let Some(v) = b
                .get("environmentVariablesOverride")
                .filter(|v| !v.is_null())
            {
                env.insert("environmentVariables".into(), v.clone());
            }
        }
        fields
    }

    fn start_build(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(project_name) = str_field(&b, "projectName") else {
            return Err(invalid("A project name must be specified"));
        };
        let (region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(project) = st.projects.get(&project_name).cloned() else {
            return Err(not_found(format!("Project does not exist: {project_name}")));
        };
        let now = Utc::now();
        let uuid = gen_uuid();
        let id = format!("{project_name}:{uuid}");
        let build_arn = arn(&region, &account, &format!("build/{id}"));
        let build_number = next_build_number(st, &project_name);
        let mut build = self.build_from_project(&project, &b);
        build.insert("id".into(), json!(id));
        build.insert("arn".into(), json!(build_arn));
        build.insert("buildNumber".into(), json!(build_number));
        build.insert("projectName".into(), json!(project_name));
        build.insert("startTime".into(), ts(now));
        build.insert("currentPhase".into(), json!("QUEUED"));
        build.insert("buildStatus".into(), json!("IN_PROGRESS"));
        build.insert("buildComplete".into(), json!(false));
        build.insert("initiator".into(), json!("fakecloud"));
        if let Some(sv) = str_field(&b, "sourceVersion") {
            build.insert("sourceVersion".into(), json!(sv));
            build.insert("resolvedSourceVersion".into(), json!(sv));
        }
        build.insert(
            "phases".into(),
            json!([
                {
                    "phaseType": "SUBMITTED",
                    "phaseStatus": "SUCCEEDED",
                    "startTime": ts(now),
                    "endTime": ts(now),
                    "durationInSeconds": 0,
                    "contexts": [],
                },
                { "phaseType": "QUEUED", "startTime": ts(now) },
            ]),
        );
        let build = Value::Object(build);
        st.builds.insert(id.clone(), build.clone());
        st.build_order.push(id);
        ok(json!({ "build": build }))
    }

    fn stop_build(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(id) = str_field(&b, "id") else {
            return Err(invalid("A build id must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(build) = st.builds.get_mut(&id) else {
            return Err(not_found(format!("Build does not exist: {id}")));
        };
        // Only an in-progress build can be stopped; a build that already reached
        // a terminal state cannot be re-stopped (AWS -> InvalidInputException).
        if build.get("buildStatus").and_then(Value::as_str) != Some("IN_PROGRESS") {
            return Err(invalid(format!("Build is not in progress: {id}")));
        }
        if let Some(obj) = build.as_object_mut() {
            obj.insert("buildStatus".into(), json!("STOPPED"));
            obj.insert("currentPhase".into(), json!("STOPPED"));
            obj.insert("buildComplete".into(), json!(true));
            obj.insert("endTime".into(), ts(Utc::now()));
        }
        ok(json!({ "build": build.clone() }))
    }

    fn retry_build(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(id) = str_field(&b, "id") else {
            return Err(invalid("A build id must be specified"));
        };
        let (region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(prev) = st.builds.get(&id).cloned() else {
            return Err(not_found(format!("Build does not exist: {id}")));
        };
        let project_name = prev
            .get("projectName")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let now = Utc::now();
        let uuid = gen_uuid();
        let new_id = format!("{project_name}:{uuid}");
        let build_arn = arn(&region, &account, &format!("build/{new_id}"));
        let build_number = next_build_number(st, &project_name);
        let mut build = prev.as_object().cloned().unwrap_or_default();
        build.insert("id".into(), json!(new_id));
        build.insert("arn".into(), json!(build_arn));
        build.insert("buildNumber".into(), json!(build_number));
        build.insert("startTime".into(), ts(now));
        build.remove("endTime");
        build.insert("currentPhase".into(), json!("QUEUED"));
        build.insert("buildStatus".into(), json!("IN_PROGRESS"));
        build.insert("buildComplete".into(), json!(false));
        build.insert(
            "phases".into(),
            json!([{ "phaseType": "SUBMITTED", "phaseStatus": "SUCCEEDED", "startTime": ts(now), "endTime": ts(now), "durationInSeconds": 0, "contexts": [] }]),
        );
        let build = Value::Object(build);
        st.builds.insert(new_id.clone(), build.clone());
        st.build_order.push(new_id);
        ok(json!({ "build": build }))
    }

    fn batch_get_builds(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let ids = str_list(&b, "ids");
        if ids.is_empty() {
            return Err(invalid("At least one build id must be specified"));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for id in ids {
            match st.builds.get_mut(&id) {
                Some(build) => {
                    settle_build(build, "buildStatus");
                    if let Some(obj) = build.as_object_mut() {
                        obj.insert("buildComplete".into(), json!(true));
                    }
                    found.push(build.clone());
                }
                None => missing.push(json!(id)),
            }
        }
        ok(json!({ "builds": found, "buildsNotFound": missing }))
    }

    fn batch_delete_builds(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let ids = str_list(&b, "ids");
        if ids.is_empty() {
            return Err(invalid("At least one build id must be specified"));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut deleted = Vec::new();
        for id in ids {
            if st.builds.remove(&id).is_some() {
                st.build_order.retain(|x| x != &id);
                deleted.push(json!(id));
            }
        }
        ok(json!({ "buildsDeleted": deleted, "buildsNotDeleted": [] }))
    }

    fn list_builds(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, None)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let ids = apply_sort_order(st.build_order.clone(), &b);
        let out = paginate_body(ids, "ids", &b)?;
        ok(out)
    }

    fn list_builds_for_project(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, None)?;
        let Some(project_name) = str_field(&b, "projectName") else {
            return Err(invalid("A project name must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.projects.contains_key(&project_name) {
            return Err(not_found(format!("Project does not exist: {project_name}")));
        }
        let prefix = format!("{project_name}:");
        let ids: Vec<String> = st
            .build_order
            .iter()
            .filter(|id| id.starts_with(&prefix))
            .cloned()
            .collect();
        let out = paginate_body(apply_sort_order(ids, &b), "ids", &b)?;
        ok(out)
    }
}

// ---------------------------------------------------------------------------
// Build batches
// ---------------------------------------------------------------------------

impl CodeBuildService {
    fn start_build_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(project_name) = str_field(&b, "projectName") else {
            return Err(invalid("A project name must be specified"));
        };
        let (region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(project) = st.projects.get(&project_name).cloned() else {
            return Err(not_found(format!("Project does not exist: {project_name}")));
        };
        let now = Utc::now();
        let uuid = gen_uuid();
        let id = format!("{project_name}:{uuid}");
        let batch_arn = arn(&region, &account, &format!("build-batch/{id}"));
        let number = next_build_batch_number(st, &project_name);
        let mut batch = self.build_from_project(&project, &b);
        // Build batches carry logConfig, not logs.
        if let Some(logs) = batch.remove("logs") {
            batch.insert("logConfig".into(), logs);
        }
        if let Some(t) = batch.remove("timeoutInMinutes") {
            batch.insert("buildTimeoutInMinutes".into(), t);
        }
        batch.insert("id".into(), json!(id));
        batch.insert("arn".into(), json!(batch_arn));
        batch.insert("buildBatchNumber".into(), json!(number));
        batch.insert("projectName".into(), json!(project_name));
        batch.insert("startTime".into(), ts(now));
        batch.insert("currentPhase".into(), json!("QUEUED"));
        batch.insert("buildBatchStatus".into(), json!("IN_PROGRESS"));
        batch.insert("complete".into(), json!(false));
        batch.insert("initiator".into(), json!("fakecloud"));
        batch.insert("buildGroups".into(), json!([]));
        let batch = Value::Object(batch);
        st.build_batches.insert(id.clone(), batch.clone());
        st.build_batch_order.push(id);
        ok(json!({ "buildBatch": batch }))
    }

    fn stop_build_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(id) = str_field(&b, "id") else {
            return Err(invalid("A build batch id must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(batch) = st.build_batches.get_mut(&id) else {
            return Err(not_found(format!("Build batch does not exist: {id}")));
        };
        // Only an in-progress batch can be stopped (AWS -> InvalidInputException
        // for an already-terminal batch).
        if batch.get("buildBatchStatus").and_then(Value::as_str) != Some("IN_PROGRESS") {
            return Err(invalid(format!("Build batch is not in progress: {id}")));
        }
        if let Some(obj) = batch.as_object_mut() {
            obj.insert("buildBatchStatus".into(), json!("STOPPED"));
            obj.insert("currentPhase".into(), json!("STOPPED"));
            obj.insert("complete".into(), json!(true));
            obj.insert("endTime".into(), ts(Utc::now()));
        }
        ok(json!({ "buildBatch": batch.clone() }))
    }

    fn retry_build_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(id) = str_field(&b, "id") else {
            return Err(invalid("A build batch id must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(batch) = st.build_batches.get_mut(&id) else {
            return Err(not_found(format!("Build batch does not exist: {id}")));
        };
        // Only a terminal (non-in-progress) batch can be retried; AWS rejects
        // retrying an in-progress batch (the model documents "Only batch builds
        // that have failed can be retried" and declares InvalidInputException).
        if batch.get("buildBatchStatus").and_then(Value::as_str) == Some("IN_PROGRESS") {
            return Err(invalid(format!("Build batch is in progress: {id}")));
        }
        if let Some(obj) = batch.as_object_mut() {
            obj.insert("buildBatchStatus".into(), json!("IN_PROGRESS"));
            obj.insert("currentPhase".into(), json!("QUEUED"));
            obj.insert("complete".into(), json!(false));
            obj.remove("endTime");
        }
        ok(json!({ "buildBatch": batch.clone() }))
    }

    fn delete_build_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let id = req_len(&b, "id", 1, usize::MAX)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut deleted = Vec::new();
        if st.build_batches.remove(&id).is_some() {
            st.build_batch_order.retain(|x| x != &id);
            deleted.push(json!(id));
        }
        ok(json!({
            "statusCode": "SUCCESS",
            "buildsDeleted": deleted,
            "buildsNotDeleted": [],
        }))
    }

    fn batch_get_build_batches(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let ids = str_list(&b, "ids");
        if ids.is_empty() {
            return Err(invalid("At least one build batch id must be specified"));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for id in ids {
            match st.build_batches.get_mut(&id) {
                Some(batch) => {
                    settle_build(batch, "buildBatchStatus");
                    if let Some(obj) = batch.as_object_mut() {
                        obj.insert("complete".into(), json!(true));
                    }
                    found.push(batch.clone());
                }
                None => missing.push(json!(id)),
            }
        }
        ok(json!({ "buildBatches": found, "buildBatchesNotFound": missing }))
    }

    fn list_build_batches(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, None)?;
        let filter_status = build_batch_filter_status(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let ids: Vec<String> = st
            .build_batch_order
            .iter()
            .filter(|id| batch_matches_status(st, id, filter_status.as_deref()))
            .cloned()
            .collect();
        let out = paginate_body(apply_sort_order(ids, &b), "ids", &b)?;
        ok(out)
    }

    fn list_build_batches_for_project(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, None)?;
        let filter_status = build_batch_filter_status(&b)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let prefix = if let Some(project_name) = str_field(&b, "projectName") {
            if !st.projects.contains_key(&project_name) {
                return Err(not_found(format!("Project does not exist: {project_name}")));
            }
            Some(format!("{project_name}:"))
        } else {
            None
        };
        let ids: Vec<String> = st
            .build_batch_order
            .iter()
            .filter(|id| prefix.as_ref().is_none_or(|p| id.starts_with(p)))
            .filter(|id| batch_matches_status(st, id, filter_status.as_deref()))
            .cloned()
            .collect();
        ok(paginate_body(apply_sort_order(ids, &b), "ids", &b)?)
    }
}

// ---------------------------------------------------------------------------
// Report groups + reports
// ---------------------------------------------------------------------------

impl CodeBuildService {
    fn create_report_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_len(&b, "name", 2, 128)?;
        let rg_type = str_field(&b, "type").unwrap_or_default();
        if !validate::is_enum(validate::REPORT_TYPE, &rg_type) {
            return Err(invalid(format!("Invalid report group type: {rg_type}")));
        }
        let Some(export_config) = b.get("exportConfig").filter(|v| !v.is_null()) else {
            return Err(invalid("An export config must be specified"));
        };
        validate_export_config(export_config)?;
        let (region, account) = self.region_account(req);
        let arn_str = arn(&region, &account, &format!("report-group/{name}"));
        let now = Utc::now();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.report_groups.contains_key(&arn_str) {
            return Err(already_exists(format!(
                "Report group already exists: {name}"
            )));
        }
        let mut rg = Map::new();
        rg.insert("arn".into(), json!(arn_str));
        rg.insert("name".into(), json!(name));
        rg.insert("type".into(), json!(rg_type));
        if let Some(ec) = b.get("exportConfig") {
            rg.insert("exportConfig".into(), ec.clone());
        }
        if let Some(t) = b.get("tags") {
            rg.insert("tags".into(), t.clone());
        }
        rg.insert("created".into(), ts(now));
        rg.insert("lastModified".into(), ts(now));
        rg.insert("status".into(), json!("ACTIVE"));
        let rg = Value::Object(rg);
        st.report_groups.insert(arn_str, rg.clone());
        ok(json!({ "reportGroup": rg }))
    }

    fn update_report_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(arn_str) = str_field(&b, "arn") else {
            return Err(invalid("A report group ARN must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // Existence is checked before input validation: an update against a
        // nonexistent report group is a ResourceNotFoundException regardless of
        // whether the supplied exportConfig is well-formed.
        if !st.report_groups.contains_key(&arn_str) {
            return Err(not_found(format!("Report group does not exist: {arn_str}")));
        }
        if let Some(ec) = b.get("exportConfig").filter(|v| !v.is_null()) {
            validate_export_config(ec)?;
        }
        let Some(rg) = st.report_groups.get_mut(&arn_str) else {
            return Err(not_found(format!("Report group does not exist: {arn_str}")));
        };
        if let Some(obj) = rg.as_object_mut() {
            if let Some(ec) = b.get("exportConfig") {
                obj.insert("exportConfig".into(), ec.clone());
            }
            if let Some(t) = b.get("tags") {
                obj.insert("tags".into(), t.clone());
            }
            obj.insert("lastModified".into(), ts(Utc::now()));
        }
        ok(json!({ "reportGroup": rg.clone() }))
    }

    fn delete_report_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn_str = req_len(&b, "arn", 1, usize::MAX)?;
        let delete_reports = b
            .get("deleteReports")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // A report group that still holds reports cannot be deleted unless
        // `deleteReports` is set; AWS throws InvalidInputException otherwise.
        let report_arns: Vec<String> = st
            .reports
            .iter()
            .filter(|(_, r)| {
                r.get("reportGroupArn").and_then(Value::as_str) == Some(arn_str.as_str())
            })
            .map(|(a, _)| a.clone())
            .collect();
        if !report_arns.is_empty() && !delete_reports {
            return Err(invalid(format!(
                "Report group contains reports and cannot be deleted: {arn_str}"
            )));
        }
        // When deleteReports is set, remove the group's reports first.
        for a in &report_arns {
            st.reports.remove(a);
            st.report_order.retain(|x| x != a);
        }
        st.report_groups.remove(&arn_str);
        // Drop any resource policy attached to the deleted report group so a
        // later GetResourcePolicy 404s instead of returning an orphan.
        st.resource_policies.remove(&arn_str);
        ok(json!({}))
    }

    fn batch_get_report_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arns = str_list(&b, "reportGroupArns");
        if arns.is_empty() {
            return Err(invalid("At least one report group ARN must be specified"));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for a in arns {
            match st.report_groups.get(&a) {
                Some(rg) => found.push(rg.clone()),
                None => missing.push(json!(a)),
            }
        }
        ok(json!({ "reportGroups": found, "reportGroupsNotFound": missing }))
    }

    fn list_report_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, Some(validate::REPORT_GROUP_SORT_BY))?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let records: Vec<Value> = st.report_groups.values().cloned().collect();
        let arns = sorted_output(records, &b, "name", "arn");
        ok(paginate_body(arns, "reportGroups", &b)?)
    }

    fn list_shared_report_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, Some(validate::SHARED_RESOURCE_SORT_BY))?;
        // Nothing is cross-account shared in fakecloud -> empty list.
        ok(paginate_body(vec![], "reportGroups", &b)?)
    }

    fn get_report_group_trend(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(arn_str) = str_field(&b, "reportGroupArn") else {
            return Err(invalid("A report group ARN must be specified"));
        };
        if str_field(&b, "trendField").is_none() {
            return Err(invalid("A trend field must be specified"));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.report_groups.contains_key(&arn_str) {
            return Err(not_found(format!("Report group does not exist: {arn_str}")));
        }
        ok(json!({
            "stats": { "average": "0", "max": "0", "min": "0" },
            "rawData": [],
        }))
    }

    fn delete_report(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn_str = req_len(&b, "arn", 1, usize::MAX)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.reports.remove(&arn_str).is_some() {
            st.report_order.retain(|x| x != &arn_str);
        }
        ok(json!({}))
    }

    fn batch_get_reports(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arns = str_list(&b, "reportArns");
        if arns.is_empty() {
            return Err(invalid("At least one report ARN must be specified"));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for a in arns {
            match st.reports.get(&a) {
                Some(r) => found.push(r.clone()),
                None => missing.push(json!(a)),
            }
        }
        ok(json!({ "reports": found, "reportsNotFound": missing }))
    }

    fn list_reports(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, None)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let arns: Vec<Value> = st.report_order.iter().rev().map(|k| json!(k)).collect();
        ok(paginate_body(arns, "reports", &b)?)
    }

    fn list_reports_for_report_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, None)?;
        let Some(rg_arn) = str_field(&b, "reportGroupArn") else {
            return Err(invalid("A report group ARN must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.report_groups.contains_key(&rg_arn) {
            return Err(not_found(format!("Report group does not exist: {rg_arn}")));
        }
        let arns: Vec<Value> = st
            .reports
            .values()
            .filter(|r| r.get("reportGroupArn").and_then(Value::as_str) == Some(rg_arn.as_str()))
            .filter_map(|r| r.get("arn").cloned())
            .collect();
        ok(paginate_body(arns, "reports", &b)?)
    }

    fn describe_test_cases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        if str_field(&b, "reportArn").is_none() {
            return Err(invalid("A report ARN must be specified"));
        }
        // No test data is generated for synthetic reports; return an empty set.
        ok(paginate_body(vec![], "testCases", &b)?)
    }

    fn describe_code_coverages(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        req_len(&b, "reportArn", 1, usize::MAX)?;
        validate_sort(&b, Some(validate::CODE_COVERAGE_SORT_BY))?;
        ok(paginate_body(vec![], "codeCoverages", &b)?)
    }
}

// ---------------------------------------------------------------------------
// Fleets
// ---------------------------------------------------------------------------

impl CodeBuildService {
    fn assemble_fleet(
        &self,
        b: &Value,
        name: &str,
        arn_str: &str,
        id: &str,
        created: Value,
    ) -> Value {
        let mut f = Map::new();
        f.insert("arn".into(), json!(arn_str));
        f.insert("name".into(), json!(name));
        f.insert("id".into(), json!(id));
        f.insert("created".into(), created);
        f.insert("lastModified".into(), ts(Utc::now()));
        f.insert("status".into(), json!({ "statusCode": "ACTIVE" }));
        for k in [
            "baseCapacity",
            "environmentType",
            "computeType",
            "computeConfiguration",
            "overflowBehavior",
            "vpcConfig",
            "proxyConfiguration",
            "imageId",
            "fleetServiceRole",
            "tags",
        ] {
            if let Some(v) = b.get(k).filter(|v| !v.is_null()) {
                f.insert(k.into(), v.clone());
            }
        }
        // scalingConfiguration input -> ScalingConfigurationOutput echo.
        if let Some(sc) = b.get("scalingConfiguration").filter(|v| !v.is_null()) {
            f.insert("scalingConfiguration".into(), sc.clone());
        }
        Value::Object(f)
    }

    fn create_fleet(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_len(&b, "name", 2, 128)?;
        check_name_pattern("name", &name)?;
        if b.get("baseCapacity").and_then(Value::as_i64).is_none() {
            return Err(invalid("A base capacity must be specified"));
        }
        validate_fleet_enums(&b, true)?;
        let (region, account) = self.region_account(req);
        let id = format!("{name}:{}", gen_uuid());
        let arn_str = arn(&region, &account, &format!("fleet/{id}"));
        let now = ts(Utc::now());
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st
            .fleets
            .values()
            .any(|f| f.get("name").and_then(Value::as_str) == Some(name.as_str()))
        {
            return Err(already_exists(format!("Fleet already exists: {name}")));
        }
        let fleet = self.assemble_fleet(&b, &name, &arn_str, &id, now);
        st.fleets.insert(arn_str, fleet.clone());
        ok(json!({ "fleet": fleet }))
    }

    fn update_fleet(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(arn_str) = str_field(&b, "arn") else {
            return Err(invalid("A fleet ARN must be specified"));
        };
        validate_fleet_enums(&b, false)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(fleet) = st.fleets.get_mut(&arn_str) else {
            return Err(not_found(format!("Fleet does not exist: {arn_str}")));
        };
        if let Some(obj) = fleet.as_object_mut() {
            for k in [
                "baseCapacity",
                "environmentType",
                "computeType",
                "computeConfiguration",
                "scalingConfiguration",
                "overflowBehavior",
                "vpcConfig",
                "proxyConfiguration",
                "imageId",
                "fleetServiceRole",
                "tags",
            ] {
                if let Some(v) = b.get(k).filter(|v| !v.is_null()) {
                    obj.insert(k.into(), v.clone());
                }
            }
            obj.insert("lastModified".into(), ts(Utc::now()));
        }
        ok(json!({ "fleet": fleet.clone() }))
    }

    fn delete_fleet(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let arn_str = req_len(&b, "arn", 1, usize::MAX)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        st.fleets.remove(&arn_str);
        ok(json!({}))
    }

    fn batch_get_fleets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let names = str_list(&b, "names");
        if names.is_empty() {
            return Err(invalid("At least one fleet name or ARN must be specified"));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for n in names {
            // Accept either a fleet ARN or a bare fleet name.
            let hit = st.fleets.get(&n).cloned().or_else(|| {
                st.fleets
                    .values()
                    .find(|f| f.get("name").and_then(Value::as_str) == Some(n.as_str()))
                    .cloned()
            });
            match hit {
                Some(f) => found.push(f),
                None => missing.push(json!(n)),
            }
        }
        ok(json!({ "fleets": found, "fleetsNotFound": missing }))
    }

    fn list_fleets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, Some(validate::FLEET_SORT_BY))?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let records: Vec<Value> = st.fleets.values().cloned().collect();
        let arns = sorted_output(records, &b, "name", "arn");
        ok(paginate_body(arns, "fleets", &b)?)
    }
}

// ---------------------------------------------------------------------------
// Webhooks
// ---------------------------------------------------------------------------

impl CodeBuildService {
    fn build_webhook(&self, b: &Value, project_name: &str, account: &str) -> Value {
        let mut w = Map::new();
        w.insert(
            "url".into(),
            json!(format!(
                "https://codebuild.amazonaws.com/webhooks?t={}",
                gen_uuid()
            )),
        );
        w.insert(
            "payloadUrl".into(),
            json!(format!(
                "https://codebuild.amazonaws.com/webhooks/payload/{account}/{project_name}"
            )),
        );
        w.insert("secret".into(), json!(gen_uuid().replace('-', "")));
        if let Some(bf) = b.get("branchFilter").filter(|v| !v.is_null()) {
            w.insert("branchFilter".into(), bf.clone());
        }
        if let Some(fg) = b.get("filterGroups").filter(|v| !v.is_null()) {
            w.insert("filterGroups".into(), fg.clone());
        }
        if let Some(bt) = b.get("buildType").filter(|v| !v.is_null()) {
            w.insert("buildType".into(), bt.clone());
        }
        if let Some(mc) = b.get("manualCreation").filter(|v| !v.is_null()) {
            w.insert("manualCreation".into(), mc.clone());
        }
        if let Some(sc) = b.get("scopeConfiguration").filter(|v| !v.is_null()) {
            w.insert("scopeConfiguration".into(), sc.clone());
        }
        if let Some(pr) = b.get("pullRequestBuildPolicy").filter(|v| !v.is_null()) {
            w.insert("pullRequestBuildPolicy".into(), pr.clone());
        }
        w.insert("lastModifiedSecret".into(), ts(Utc::now()));
        Value::Object(w)
    }

    fn create_webhook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let project_name = req_len(&b, "projectName", 2, 150)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(project) = st.projects.get_mut(&project_name) else {
            return Err(not_found(format!("Project does not exist: {project_name}")));
        };
        // A webhook can only be created for a project whose source is a
        // provider-hosted Git backend; NO_SOURCE / S3 / CODECOMMIT / CODEPIPELINE
        // have no webhook to register (AWS -> OAuthProviderException).
        let source_type = project
            .get("source")
            .and_then(|s| s.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if !validate::is_enum(validate::WEBHOOK_SOURCE_TYPE, source_type) {
            return Err(oauth_provider(format!(
                "Project source type {source_type} does not support webhooks"
            )));
        }
        if project
            .get("webhook")
            .map(|w| !w.is_null())
            .unwrap_or(false)
        {
            return Err(already_exists(format!(
                "A webhook already exists for project: {project_name}"
            )));
        }
        let webhook = self.build_webhook(&b, &project_name, &account);
        if let Some(obj) = project.as_object_mut() {
            obj.insert("webhook".into(), webhook.clone());
        }
        ok(json!({ "webhook": webhook }))
    }

    fn update_webhook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let project_name = req_len(&b, "projectName", 2, 150)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(project) = st.projects.get_mut(&project_name) else {
            return Err(not_found(format!("Project does not exist: {project_name}")));
        };
        let existing = project.get("webhook").filter(|w| !w.is_null()).cloned();
        let Some(mut webhook) = existing else {
            return Err(not_found(format!(
                "No webhook exists for project: {project_name}"
            )));
        };
        if let Some(obj) = webhook.as_object_mut() {
            for k in ["branchFilter", "filterGroups", "buildType"] {
                if let Some(v) = b.get(k).filter(|v| !v.is_null()) {
                    obj.insert(k.into(), v.clone());
                }
            }
            if b.get("rotateSecret")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                obj.insert("secret".into(), json!(gen_uuid().replace('-', "")));
                obj.insert("lastModifiedSecret".into(), ts(Utc::now()));
            }
        }
        if let Some(obj) = project.as_object_mut() {
            obj.insert("webhook".into(), webhook.clone());
        }
        ok(json!({ "webhook": webhook }))
    }

    fn delete_webhook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let project_name = req_len(&b, "projectName", 2, 150)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(project) = st.projects.get_mut(&project_name) else {
            return Err(not_found(format!("Project does not exist: {project_name}")));
        };
        let had = project
            .get("webhook")
            .map(|w| !w.is_null())
            .unwrap_or(false);
        if !had {
            return Err(not_found(format!(
                "No webhook exists for project: {project_name}"
            )));
        }
        if let Some(obj) = project.as_object_mut() {
            obj.remove("webhook");
        }
        ok(json!({}))
    }
}

// ---------------------------------------------------------------------------
// Source credentials + resource policy
// ---------------------------------------------------------------------------

impl CodeBuildService {
    fn import_source_credentials(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let server_type = str_field(&b, "serverType").unwrap_or_default();
        if !validate::is_enum(validate::SERVER_TYPE, &server_type) {
            return Err(invalid(format!("Invalid server type: {server_type}")));
        }
        let auth_type = str_field(&b, "authType").unwrap_or_default();
        if !validate::is_enum(validate::AUTH_TYPE, &auth_type) {
            return Err(invalid(format!("Invalid auth type: {auth_type}")));
        }
        req_len(&b, "token", 1, usize::MAX)?;
        opt_len(&b, "username", 1, usize::MAX)?;
        let (region, account) = self.region_account(req);
        // AWS renders the server type lowercase in the token ARN
        // (`arn:aws:codebuild:...:token/github`) even though the `serverType`
        // field echoes the uppercase enum value.
        let arn_str = arn(
            &region,
            &account,
            &format!("token/{}", server_type.to_lowercase()),
        );
        let overwrite = b
            .get("shouldOverwrite")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.source_credentials.contains_key(&arn_str) && !overwrite {
            return Err(already_exists(format!(
                "Source credentials already exist for: {server_type}"
            )));
        }
        let record = json!({
            "arn": arn_str,
            "serverType": server_type,
            "authType": auth_type,
            "resource": server_type,
        });
        st.source_credentials.insert(arn_str.clone(), record);
        ok(json!({ "arn": arn_str }))
    }

    fn delete_source_credentials(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(arn_str) = str_field(&b, "arn") else {
            return Err(invalid("A source credentials ARN must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if st.source_credentials.remove(&arn_str).is_none() {
            return Err(not_found(format!(
                "Source credentials do not exist: {arn_str}"
            )));
        }
        ok(json!({ "arn": arn_str }))
    }

    fn list_source_credentials(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let infos: Vec<Value> = st.source_credentials.values().cloned().collect();
        ok(json!({ "sourceCredentialsInfos": infos }))
    }

    fn put_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(policy) = str_field(&b, "policy") else {
            return Err(invalid("A policy must be specified"));
        };
        let Some(resource_arn) = str_field(&b, "resourceArn") else {
            return Err(invalid("A resource ARN must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // The resource (project or report group) must exist.
        let exists = st
            .projects
            .values()
            .any(|p| p.get("arn").and_then(Value::as_str) == Some(resource_arn.as_str()))
            || st.report_groups.contains_key(&resource_arn);
        if !exists {
            return Err(not_found(format!(
                "Resource does not exist: {resource_arn}"
            )));
        }
        st.resource_policies.insert(resource_arn.clone(), policy);
        ok(json!({ "resourceArn": resource_arn }))
    }

    fn get_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(resource_arn) = str_field(&b, "resourceArn") else {
            return Err(invalid("A resource ARN must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(policy) = st.resource_policies.get(&resource_arn) else {
            return Err(not_found(format!(
                "No resource policy exists for: {resource_arn}"
            )));
        };
        ok(json!({ "policy": policy }))
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let resource_arn = req_len(&b, "resourceArn", 1, usize::MAX)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        st.resource_policies.remove(&resource_arn);
        ok(json!({}))
    }

    fn list_curated_environment_images(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // A representative slice of the CodeBuild-curated managed images.
        ok(json!({
            "platforms": [
                {
                    "platform": "AMAZON_LINUX",
                    "languages": [
                        {
                            "language": "STANDARD",
                            "images": [
                                {
                                    "name": "aws/codebuild/amazonlinux2-x86_64-standard:5.0",
                                    "description": "AL2 x86_64 standard 5.0",
                                    "versions": ["aws/codebuild/amazonlinux2-x86_64-standard:5.0"]
                                }
                            ]
                        }
                    ]
                },
                {
                    "platform": "UBUNTU",
                    "languages": [
                        {
                            "language": "STANDARD",
                            "images": [
                                {
                                    "name": "aws/codebuild/standard:7.0",
                                    "description": "Ubuntu standard 7.0",
                                    "versions": ["aws/codebuild/standard:7.0"]
                                }
                            ]
                        }
                    ]
                }
            ]
        }))
    }
}

// ---------------------------------------------------------------------------
// Sandboxes + command executions
// ---------------------------------------------------------------------------

impl CodeBuildService {
    fn start_sandbox(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let (region, account) = self.region_account(req);
        let now = Utc::now();
        let id = gen_uuid();
        let sandbox_arn = arn(&region, &account, &format!("sandbox/{id}"));
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        // If a project name is given it must exist.
        if let Some(project_name) = str_field(&b, "projectName") {
            if !st.projects.contains_key(&project_name) {
                return Err(not_found(format!("Project does not exist: {project_name}")));
            }
        }
        let mut sandbox = Map::new();
        sandbox.insert("id".into(), json!(id));
        sandbox.insert("arn".into(), json!(sandbox_arn));
        if let Some(p) = str_field(&b, "projectName") {
            sandbox.insert("projectName".into(), json!(p));
        }
        sandbox.insert("requestTime".into(), ts(now));
        sandbox.insert("startTime".into(), ts(now));
        sandbox.insert("status".into(), json!("RUNNING"));
        sandbox.insert(
            "currentSession".into(),
            json!({ "id": gen_uuid(), "status": "RUNNING", "startTime": ts(now) }),
        );
        let sandbox = Value::Object(sandbox);
        st.sandboxes.insert(id.clone(), sandbox.clone());
        st.sandbox_order.push(id);
        ok(json!({ "sandbox": sandbox }))
    }

    fn stop_sandbox(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(id) = str_field(&b, "id") else {
            return Err(invalid("A sandbox id must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(sandbox) = st.sandboxes.get_mut(&id) else {
            return Err(not_found(format!("Sandbox does not exist: {id}")));
        };
        if let Some(obj) = sandbox.as_object_mut() {
            obj.insert("status".into(), json!("STOPPED"));
            obj.insert("endTime".into(), ts(Utc::now()));
        }
        ok(json!({ "sandbox": sandbox.clone() }))
    }

    fn start_sandbox_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(id) = str_field(&b, "sandboxId") else {
            return Err(invalid("A sandbox id must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.sandboxes.contains_key(&id) {
            return Err(not_found(format!("Sandbox does not exist: {id}")));
        }
        ok(json!({
            "ssmSession": {
                "sessionId": gen_uuid(),
                "tokenValue": gen_uuid().replace('-', ""),
                "streamUrl": format!("wss://ssmmessages.{}.amazonaws.com/v1/data-channel/{}", account, gen_uuid()),
            }
        }))
    }

    fn batch_get_sandboxes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let ids = str_list(&b, "ids");
        if ids.is_empty() {
            return Err(invalid("At least one sandbox id must be specified"));
        }
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for id in ids {
            match st.sandboxes.get(&id) {
                Some(s) => found.push(s.clone()),
                None => missing.push(json!(id)),
            }
        }
        ok(json!({ "sandboxes": found, "sandboxesNotFound": missing }))
    }

    fn list_sandboxes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, None)?;
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let ids = apply_sort_order(st.sandbox_order.clone(), &b);
        ok(paginate_body(ids, "ids", &b)?)
    }

    fn list_sandboxes_for_project(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, None)?;
        let Some(project_name) = str_field(&b, "projectName") else {
            return Err(invalid("A project name must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.projects.contains_key(&project_name) {
            return Err(not_found(format!("Project does not exist: {project_name}")));
        }
        // Iterate sandbox insertion order (not BTreeMap/uuid order) so the
        // result honors sortOrder consistently with the other list ops.
        let ids: Vec<String> = st
            .sandbox_order
            .iter()
            .filter(|id| {
                st.sandboxes
                    .get(*id)
                    .and_then(|s| s.get("projectName").and_then(Value::as_str))
                    == Some(project_name.as_str())
            })
            .cloned()
            .collect();
        ok(paginate_body(apply_sort_order(ids, &b), "ids", &b)?)
    }

    fn start_command_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let Some(sandbox_id) = str_field(&b, "sandboxId") else {
            return Err(invalid("A sandbox id must be specified"));
        };
        let Some(command) = str_field(&b, "command") else {
            return Err(invalid("A command must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let now = Utc::now();
        let id = gen_uuid();
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let Some(sandbox) = st.sandboxes.get(&sandbox_id) else {
            return Err(not_found(format!("Sandbox does not exist: {sandbox_id}")));
        };
        let sandbox_arn = sandbox
            .get("arn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let exec = json!({
            "id": id,
            "sandboxId": sandbox_id,
            "sandboxArn": sandbox_arn,
            "submitTime": ts(now),
            "startTime": ts(now),
            "status": "IN_PROGRESS",
            "command": command,
            "type": str_field(&b, "type").unwrap_or_else(|| "SHELL".into()),
        });
        st.command_executions.insert(id.clone(), exec.clone());
        st.command_execution_order.push(id);
        ok(json!({ "commandExecution": exec }))
    }

    fn batch_get_command_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        // Requires a sandboxId plus a list of command execution ids. An
        // execution only counts as found if it belongs to the given sandbox.
        let sandbox_id = req_len(&b, "sandboxId", 1, usize::MAX)?;
        let ids = str_list(&b, "commandExecutionIds");
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for id in ids {
            match st.command_executions.get_mut(&id) {
                Some(e)
                    if e.get("sandboxId").and_then(Value::as_str) == Some(sandbox_id.as_str()) =>
                {
                    settle_command_execution(e);
                    found.push(e.clone());
                }
                _ => missing.push(json!(id)),
            }
        }
        ok(json!({
            "commandExecutions": found,
            "commandExecutionsNotFound": missing,
        }))
    }

    fn list_command_executions_for_sandbox(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        validate_sort(&b, None)?;
        let Some(sandbox_id) = str_field(&b, "sandboxId") else {
            return Err(invalid("A sandbox id must be specified"));
        };
        let (_region, account) = self.region_account(req);
        let mut guard = self.state.write();
        let st = guard.get_or_create(&account);
        if !st.sandboxes.contains_key(&sandbox_id) {
            return Err(not_found(format!("Sandbox does not exist: {sandbox_id}")));
        }
        // Iterate in insertion order (not BTreeMap/uuid order), settling each
        // in-progress execution on read, consistent with BatchGetCommandExecutions.
        let order = st.command_execution_order.clone();
        let mut execs = Vec::new();
        for id in &order {
            if let Some(e) = st.command_executions.get_mut(id) {
                if e.get("sandboxId").and_then(Value::as_str) == Some(sandbox_id.as_str()) {
                    settle_command_execution(e);
                    execs.push(e.clone());
                }
            }
        }
        ok(paginate_body(execs, "commandExecutions", &b)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> CodeBuildService {
        let state = Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        )));
        CodeBuildService::new(state)
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "codebuild".to_string(),
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

    fn err_of(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match r {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        }
    }

    fn minimal_project(name: &str) -> Value {
        json!({
            "name": name,
            "source": { "type": "NO_SOURCE" },
            "artifacts": { "type": "NO_ARTIFACTS" },
            "environment": {
                "type": "LINUX_CONTAINER",
                "image": "aws/codebuild/standard:7.0",
                "computeType": "BUILD_GENERAL1_SMALL"
            },
            "serviceRole": "arn:aws:iam::000000000000:role/service-role/codebuild"
        })
    }

    #[test]
    fn create_project_mints_arn_and_badge() {
        let s = svc();
        let resp = s
            .create_project(&req("CreateProject", minimal_project("demo")))
            .unwrap();
        let v = body_of(resp);
        let p = &v["project"];
        assert_eq!(p["name"], "demo");
        assert_eq!(
            p["arn"],
            "arn:aws:codebuild:us-east-1:000000000000:project/demo"
        );
        // badgeEnabled input is transformed into a badge structure.
        assert_eq!(p["badge"]["badgeEnabled"], false);
        // The input-only field must not leak into the Project shape.
        assert!(p.get("badgeEnabled").is_none());
    }

    #[test]
    fn create_project_rejects_duplicate() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("dup")))
            .unwrap();
        let err = err_of(s.create_project(&req("CreateProject", minimal_project("dup"))));
        assert!(err.to_string().contains("already exists"), "{err}");
    }

    #[test]
    fn create_project_rejects_bad_enum_and_short_name() {
        let s = svc();
        let mut bad = minimal_project("x"); // name too short (min 2)
        bad["environment"]["type"] = json!("NOT_A_TYPE");
        assert!(s.create_project(&req("CreateProject", bad)).is_err());
    }

    #[test]
    fn start_build_settles_on_read() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("bld")))
            .unwrap();
        let start = body_of(
            s.start_build(&req("StartBuild", json!({ "projectName": "bld" })))
                .unwrap(),
        );
        let id = start["build"]["id"].as_str().unwrap().to_string();
        assert_eq!(start["build"]["buildStatus"], "IN_PROGRESS");
        let got = body_of(
            s.batch_get_builds(&req("BatchGetBuilds", json!({ "ids": [id] })))
                .unwrap(),
        );
        assert_eq!(got["builds"][0]["buildStatus"], "SUCCEEDED");
        assert_eq!(got["builds"][0]["buildComplete"], true);
    }

    #[test]
    fn start_build_unknown_project_not_found() {
        let s = svc();
        let err = err_of(s.start_build(&req("StartBuild", json!({ "projectName": "ghost" }))));
        assert!(err.to_string().contains("does not exist"), "{err}");
    }

    #[test]
    fn list_projects_rejects_bad_sort_order() {
        let s = svc();
        let err = err_of(s.list_projects(&req("ListProjects", json!({ "sortOrder": "SIDEWAYS" }))));
        assert!(err.to_string().contains("sortOrder"), "{err}");
    }

    #[test]
    fn paginate_rejects_out_of_range_max_results() {
        let s = svc();
        let err =
            err_of(s.list_report_groups(&req("ListReportGroups", json!({ "maxResults": 500 }))));
        assert!(err.to_string().contains("range"), "{err}");
    }

    #[test]
    fn report_group_crud_round_trip() {
        let s = svc();
        let created = body_of(
            s.create_report_group(&req(
                "CreateReportGroup",
                json!({
                    "name": "rg1",
                    "type": "TEST",
                    "exportConfig": { "exportConfigType": "NO_EXPORT" }
                }),
            ))
            .unwrap(),
        );
        let arn = created["reportGroup"]["arn"].as_str().unwrap().to_string();
        let got = body_of(
            s.batch_get_report_groups(&req(
                "BatchGetReportGroups",
                json!({ "reportGroupArns": [arn] }),
            ))
            .unwrap(),
        );
        assert_eq!(got["reportGroups"][0]["name"], "rg1");
        assert_eq!(got["reportGroups"][0]["status"], "ACTIVE");
    }

    fn github_project(name: &str) -> Value {
        let mut p = minimal_project(name);
        p["source"] = json!({ "type": "GITHUB", "location": "https://github.com/o/r" });
        p
    }

    // Fix 1: a partial UpdateProject that omits badgeEnabled preserves the badge.
    #[test]
    fn partial_update_preserves_enabled_badge() {
        let s = svc();
        let mut create = minimal_project("bp");
        create["badgeEnabled"] = json!(true);
        s.create_project(&req("CreateProject", create)).unwrap();
        // Update only the description; badge must stay enabled.
        let updated = body_of(
            s.update_project(&req(
                "UpdateProject",
                json!({ "name": "bp", "description": "d" }),
            ))
            .unwrap(),
        );
        assert_eq!(updated["project"]["badge"]["badgeEnabled"], true);
        assert_eq!(updated["project"]["description"], "d");
    }

    // Fix 2: buildNumber is per-project, monotonic, never reused after a delete.
    #[test]
    fn build_number_is_per_project_and_monotonic() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("pp")))
            .unwrap();
        s.create_project(&req("CreateProject", minimal_project("qq")))
            .unwrap();
        let n = |resp: AwsResponse| body_of(resp)["build"]["buildNumber"].as_i64().unwrap();
        let id1 = {
            let r = s
                .start_build(&req("StartBuild", json!({ "projectName": "pp" })))
                .unwrap();
            let v = body_of(r);
            assert_eq!(v["build"]["buildNumber"], 1);
            v["build"]["id"].as_str().unwrap().to_string()
        };
        assert_eq!(
            n(
                s.start_build(&req("StartBuild", json!({ "projectName": "pp" })))
                    .unwrap()
            ),
            2
        );
        // A second project numbers independently, starting at 1.
        assert_eq!(
            n(
                s.start_build(&req("StartBuild", json!({ "projectName": "qq" })))
                    .unwrap()
            ),
            1
        );
        // Deleting a build does not free its number: the next build for p is 3.
        s.batch_delete_builds(&req("BatchDeleteBuilds", json!({ "ids": [id1] })))
            .unwrap();
        assert_eq!(
            n(
                s.start_build(&req("StartBuild", json!({ "projectName": "pp" })))
                    .unwrap()
            ),
            3
        );
    }

    // Fix 3: a completed build cannot be stopped again.
    #[test]
    fn stop_build_rejects_terminal_build() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("sp")))
            .unwrap();
        let id = body_of(
            s.start_build(&req("StartBuild", json!({ "projectName": "sp" })))
                .unwrap(),
        )["build"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        // Settle it to SUCCEEDED via a read.
        s.batch_get_builds(&req("BatchGetBuilds", json!({ "ids": [id.clone()] })))
            .unwrap();
        let err = err_of(s.stop_build(&req("StopBuild", json!({ "id": id }))));
        assert!(err.to_string().contains("not in progress"), "{err}");
    }

    #[test]
    fn stop_build_allows_in_progress_build() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("sp2")))
            .unwrap();
        let id = body_of(
            s.start_build(&req("StartBuild", json!({ "projectName": "sp2" })))
                .unwrap(),
        )["build"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let stopped = body_of(
            s.stop_build(&req("StopBuild", json!({ "id": id })))
                .unwrap(),
        );
        assert_eq!(stopped["build"]["buildStatus"], "STOPPED");
    }

    // Fix 4: webhooks require a provider-hosted Git source type.
    #[test]
    fn create_webhook_rejects_non_git_source() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("nw")))
            .unwrap();
        let err = err_of(s.create_webhook(&req("CreateWebhook", json!({ "projectName": "nw" }))));
        assert!(
            err.to_string().contains("does not support webhooks"),
            "{err}"
        );
    }

    #[test]
    fn create_webhook_accepts_github_source() {
        let s = svc();
        s.create_project(&req("CreateProject", github_project("gw")))
            .unwrap();
        let resp = body_of(
            s.create_webhook(&req("CreateWebhook", json!({ "projectName": "gw" })))
                .unwrap(),
        );
        assert!(resp["webhook"]["payloadUrl"].is_string());
    }

    // Fix 5 + 6: PRIVATE clears the stored alias and omits it from the response.
    #[test]
    fn visibility_private_clears_public_alias() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("vp")))
            .unwrap();
        let a = arn("us-east-1", "000000000000", "project/vp");
        let pub_resp = body_of(
            s.update_project_visibility(&req(
                "UpdateProjectVisibility",
                json!({ "projectArn": a, "projectVisibility": "PUBLIC_READ" }),
            ))
            .unwrap(),
        );
        assert!(pub_resp["publicProjectAlias"].is_string());
        let priv_resp = body_of(
            s.update_project_visibility(&req(
                "UpdateProjectVisibility",
                json!({ "projectArn": a, "projectVisibility": "PRIVATE" }),
            ))
            .unwrap(),
        );
        // Response omits the alias, and the stored project no longer carries it.
        assert!(priv_resp.get("publicProjectAlias").is_none());
        let got = body_of(
            s.batch_get_projects(&req("BatchGetProjects", json!({ "names": ["vp"] })))
                .unwrap(),
        );
        assert!(got["projects"][0].get("publicProjectAlias").is_none());
    }

    // Fix 7: exportConfigType must be a known enum member.
    #[test]
    fn create_report_group_rejects_bad_export_type() {
        let s = svc();
        let err = err_of(s.create_report_group(&req(
            "CreateReportGroup",
            json!({ "name": "rgx", "type": "TEST", "exportConfig": { "exportConfigType": "FTP" } }),
        )));
        assert!(err.to_string().contains("export config type"), "{err}");
    }

    // Fix 8: UpdateFleet validates enums.
    #[test]
    fn update_fleet_rejects_bad_enum() {
        let s = svc();
        let created = body_of(
            s.create_fleet(&req(
                "CreateFleet",
                json!({
                    "name": "flt",
                    "baseCapacity": 1,
                    "environmentType": "LINUX_CONTAINER",
                    "computeType": "BUILD_GENERAL1_SMALL"
                }),
            ))
            .unwrap(),
        );
        let a = created["fleet"]["arn"].as_str().unwrap().to_string();
        let err = err_of(s.update_fleet(&req(
            "UpdateFleet",
            json!({ "arn": a, "computeType": "BOGUS" }),
        )));
        assert!(err.to_string().contains("compute type"), "{err}");
    }

    // Fix 9: sortBy/sortOrder actually reorder results.
    #[test]
    fn list_projects_applies_sort() {
        let s = svc();
        for n in ["alpha", "bravo", "charlie"] {
            s.create_project(&req("CreateProject", minimal_project(n)))
                .unwrap();
        }
        let desc = body_of(
            s.list_projects(&req(
                "ListProjects",
                json!({ "sortBy": "NAME", "sortOrder": "DESCENDING" }),
            ))
            .unwrap(),
        );
        let projects: Vec<&str> = desc["projects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(projects, vec!["charlie", "bravo", "alpha"]);
    }

    // Fix 10 + 13: BatchGetCommandExecutions scopes by sandbox and settles.
    #[test]
    fn command_executions_scoped_to_sandbox_and_settle() {
        let s = svc();
        let sb_a = body_of(s.start_sandbox(&req("StartSandbox", json!({}))).unwrap())["sandbox"]
            ["id"]
            .as_str()
            .unwrap()
            .to_string();
        let sb_b = body_of(s.start_sandbox(&req("StartSandbox", json!({}))).unwrap())["sandbox"]
            ["id"]
            .as_str()
            .unwrap()
            .to_string();
        let exec_id = body_of(
            s.start_command_execution(&req(
                "StartCommandExecution",
                json!({ "sandboxId": sb_a, "command": "echo hi" }),
            ))
            .unwrap(),
        )["commandExecution"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        // Looked up under the wrong sandbox -> not found.
        let wrong = body_of(
            s.batch_get_command_executions(&req(
                "BatchGetCommandExecutions",
                json!({ "sandboxId": sb_b, "commandExecutionIds": [exec_id.clone()] }),
            ))
            .unwrap(),
        );
        assert_eq!(wrong["commandExecutions"].as_array().unwrap().len(), 0);
        assert_eq!(wrong["commandExecutionsNotFound"][0], exec_id);
        // Correct sandbox -> found and settled to SUCCEEDED.
        let right = body_of(
            s.batch_get_command_executions(&req(
                "BatchGetCommandExecutions",
                json!({ "sandboxId": sb_a, "commandExecutionIds": [exec_id] }),
            ))
            .unwrap(),
        );
        assert_eq!(right["commandExecutions"][0]["status"], "SUCCEEDED");
    }

    // Fix 11: shared lists are empty (nothing is cross-account shared).
    #[test]
    fn shared_lists_are_empty() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("sh")))
            .unwrap();
        let sp = body_of(
            s.list_shared_projects(&req("ListSharedProjects", json!({})))
                .unwrap(),
        );
        assert_eq!(sp["projects"].as_array().unwrap().len(), 0);
        let sr = body_of(
            s.list_shared_report_groups(&req("ListSharedReportGroups", json!({})))
                .unwrap(),
        );
        assert_eq!(sr["reportGroups"].as_array().unwrap().len(), 0);
    }

    // Fix 12: deleting a project removes its webhook.
    #[test]
    fn delete_project_removes_webhook() {
        let s = svc();
        s.create_project(&req("CreateProject", github_project("dw")))
            .unwrap();
        s.create_webhook(&req("CreateWebhook", json!({ "projectName": "dw" })))
            .unwrap();
        s.delete_project(&req("DeleteProject", json!({ "name": "dw" })))
            .unwrap();
        // Recreate and confirm no webhook lingers.
        s.create_project(&req("CreateProject", github_project("dw")))
            .unwrap();
        let got = body_of(
            s.batch_get_projects(&req("BatchGetProjects", json!({ "names": ["dw"] })))
                .unwrap(),
        );
        assert!(got["projects"][0].get("webhook").is_none());
    }

    // Fix 14: ListCommandExecutionsForSandbox preserves insertion order.
    #[test]
    fn list_command_executions_insertion_order() {
        let s = svc();
        let sb = body_of(s.start_sandbox(&req("StartSandbox", json!({}))).unwrap())["sandbox"]
            ["id"]
            .as_str()
            .unwrap()
            .to_string();
        let mut ids = Vec::new();
        for c in ["first", "second", "third"] {
            let id = body_of(
                s.start_command_execution(&req(
                    "StartCommandExecution",
                    json!({ "sandboxId": sb, "command": c }),
                ))
                .unwrap(),
            )["commandExecution"]["id"]
                .as_str()
                .unwrap()
                .to_string();
            ids.push(id);
        }
        let listed = body_of(
            s.list_command_executions_for_sandbox(&req(
                "ListCommandExecutionsForSandbox",
                json!({ "sandboxId": sb }),
            ))
            .unwrap(),
        );
        let got: Vec<&str> = listed["commandExecutions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["id"].as_str().unwrap())
            .collect();
        assert_eq!(got, ids.iter().map(String::as_str).collect::<Vec<_>>());
    }

    // Helper: create a project and start `n` builds, returning their ids in
    // creation (oldest-first) order.
    fn start_n_builds(s: &CodeBuildService, project: &str, n: usize) -> Vec<String> {
        s.create_project(&req("CreateProject", minimal_project(project)))
            .unwrap();
        (0..n)
            .map(|_| {
                body_of(
                    s.start_build(&req("StartBuild", json!({ "projectName": project })))
                        .unwrap(),
                )["build"]["id"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    fn ids_of(v: &Value) -> Vec<String> {
        v["ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().to_string())
            .collect()
    }

    // T1.1: sortOrder is honored on id-list ops (default DESCENDING, ASCENDING
    // returns oldest-first insertion order).
    #[test]
    fn list_builds_honors_sort_order() {
        let s = svc();
        let created = start_n_builds(&s, "so", 3);
        // Default -> newest first (reverse of creation order).
        let mut desc = created.clone();
        desc.reverse();
        let default = ids_of(&body_of(
            s.list_builds(&req("ListBuilds", json!({}))).unwrap(),
        ));
        assert_eq!(default, desc);
        let descending = ids_of(&body_of(
            s.list_builds(&req("ListBuilds", json!({ "sortOrder": "DESCENDING" })))
                .unwrap(),
        ));
        assert_eq!(descending, desc);
        // ASCENDING -> creation (oldest-first) order.
        let ascending = ids_of(&body_of(
            s.list_builds(&req("ListBuilds", json!({ "sortOrder": "ASCENDING" })))
                .unwrap(),
        ));
        assert_eq!(ascending, created);
    }

    #[test]
    fn list_builds_for_project_honors_sort_order() {
        let s = svc();
        let created = start_n_builds(&s, "sop", 3);
        let ascending = ids_of(&body_of(
            s.list_builds_for_project(&req(
                "ListBuildsForProject",
                json!({ "projectName": "sop", "sortOrder": "ASCENDING" }),
            ))
            .unwrap(),
        ));
        assert_eq!(ascending, created);
    }

    // T1.2: ListSandboxesForProject follows insertion order and sortOrder,
    // not arbitrary uuid (BTreeMap) order.
    #[test]
    fn list_sandboxes_for_project_honors_insertion_and_sort_order() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("sbp")))
            .unwrap();
        let mut created = Vec::new();
        for _ in 0..3 {
            created.push(
                body_of(
                    s.start_sandbox(&req("StartSandbox", json!({ "projectName": "sbp" })))
                        .unwrap(),
                )["sandbox"]["id"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
        let ascending = ids_of(&body_of(
            s.list_sandboxes_for_project(&req(
                "ListSandboxesForProject",
                json!({ "projectName": "sbp", "sortOrder": "ASCENDING" }),
            ))
            .unwrap(),
        ));
        assert_eq!(ascending, created);
        let mut desc = created.clone();
        desc.reverse();
        let default = ids_of(&body_of(
            s.list_sandboxes_for_project(&req(
                "ListSandboxesForProject",
                json!({ "projectName": "sbp" }),
            ))
            .unwrap(),
        ));
        assert_eq!(default, desc);
    }

    // T1.3: ListBuildBatches applies filter.status and validates the enum.
    #[test]
    fn list_build_batches_filters_by_status() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("bf")))
            .unwrap();
        // Two batches: one left IN_PROGRESS, one stopped -> terminal.
        let running = body_of(
            s.start_build_batch(&req("StartBuildBatch", json!({ "projectName": "bf" })))
                .unwrap(),
        )["buildBatch"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let stopped = body_of(
            s.start_build_batch(&req("StartBuildBatch", json!({ "projectName": "bf" })))
                .unwrap(),
        )["buildBatch"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        s.stop_build_batch(&req("StopBuildBatch", json!({ "id": stopped })))
            .unwrap();
        let in_progress = ids_of(&body_of(
            s.list_build_batches(&req(
                "ListBuildBatches",
                json!({ "filter": { "status": "IN_PROGRESS" } }),
            ))
            .unwrap(),
        ));
        assert_eq!(in_progress, vec![running]);
        let stopped_only = ids_of(&body_of(
            s.list_build_batches(&req(
                "ListBuildBatches",
                json!({ "filter": { "status": "STOPPED" } }),
            ))
            .unwrap(),
        ));
        assert_eq!(stopped_only.len(), 1);
        // An unknown status is rejected.
        let err = err_of(s.list_build_batches(&req(
            "ListBuildBatches",
            json!({ "filter": { "status": "NOPE" } }),
        )));
        assert!(err.to_string().contains("status"), "{err}");
    }

    #[test]
    fn list_build_batches_for_project_filters_by_status() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("bfp")))
            .unwrap();
        let stopped = body_of(
            s.start_build_batch(&req("StartBuildBatch", json!({ "projectName": "bfp" })))
                .unwrap(),
        )["buildBatch"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        s.stop_build_batch(&req("StopBuildBatch", json!({ "id": stopped })))
            .unwrap();
        let in_progress = ids_of(&body_of(
            s.list_build_batches_for_project(&req(
                "ListBuildBatchesForProject",
                json!({ "projectName": "bfp", "filter": { "status": "IN_PROGRESS" } }),
            ))
            .unwrap(),
        ));
        assert!(in_progress.is_empty());
    }

    // T2.4: deleting a project/report group drops its resource policy.
    #[test]
    fn delete_project_removes_resource_policy() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("rp")))
            .unwrap();
        let arn = "arn:aws:codebuild:us-east-1:000000000000:project/rp";
        s.put_resource_policy(&req(
            "PutResourcePolicy",
            json!({ "resourceArn": arn, "policy": "{}" }),
        ))
        .unwrap();
        s.delete_project(&req("DeleteProject", json!({ "name": "rp" })))
            .unwrap();
        let err =
            err_of(s.get_resource_policy(&req("GetResourcePolicy", json!({ "resourceArn": arn }))));
        assert!(err.to_string().contains("No resource policy"), "{err}");
    }

    #[test]
    fn delete_report_group_removes_resource_policy() {
        let s = svc();
        let arn = body_of(
            s.create_report_group(&req(
                "CreateReportGroup",
                json!({
                    "name": "rgp",
                    "type": "TEST",
                    "exportConfig": { "exportConfigType": "NO_EXPORT" }
                }),
            ))
            .unwrap(),
        )["reportGroup"]["arn"]
            .as_str()
            .unwrap()
            .to_string();
        s.put_resource_policy(&req(
            "PutResourcePolicy",
            json!({ "resourceArn": arn, "policy": "{}" }),
        ))
        .unwrap();
        s.delete_report_group(&req("DeleteReportGroup", json!({ "arn": arn })))
            .unwrap();
        let err =
            err_of(s.get_resource_policy(&req("GetResourcePolicy", json!({ "resourceArn": arn }))));
        assert!(err.to_string().contains("No resource policy"), "{err}");
    }

    // T2.5: DeleteReportGroup rejects a group that still holds reports unless
    // deleteReports=true.
    #[test]
    fn delete_report_group_guards_on_reports() {
        let s = svc();
        let arn = body_of(
            s.create_report_group(&req(
                "CreateReportGroup",
                json!({
                    "name": "rgr",
                    "type": "TEST",
                    "exportConfig": { "exportConfigType": "NO_EXPORT" }
                }),
            ))
            .unwrap(),
        )["reportGroup"]["arn"]
            .as_str()
            .unwrap()
            .to_string();
        // Inject a report belonging to the group directly into state.
        let report_arn = format!("{arn}:report-1");
        {
            let mut guard = s.state.write();
            let st = guard.get_or_create("000000000000");
            st.reports.insert(
                report_arn.clone(),
                json!({ "arn": report_arn, "reportGroupArn": arn }),
            );
            st.report_order.push(report_arn.clone());
        }
        // Without deleteReports -> InvalidInputException, group survives.
        let err = err_of(s.delete_report_group(&req("DeleteReportGroup", json!({ "arn": arn }))));
        assert!(err.to_string().contains("cannot be deleted"), "{err}");
        assert!(s
            .state
            .write()
            .get_or_create("000000000000")
            .report_groups
            .contains_key(&arn));
        // With deleteReports -> succeeds and clears the reports.
        s.delete_report_group(&req(
            "DeleteReportGroup",
            json!({ "arn": arn, "deleteReports": true }),
        ))
        .unwrap();
        let mut guard = s.state.write();
        let st = guard.get_or_create("000000000000");
        assert!(!st.report_groups.contains_key(&arn));
        assert!(!st.reports.contains_key(&report_arn));
    }

    // T2.6: create_project / create_fleet enforce the name @pattern.
    #[test]
    fn create_project_rejects_bad_name_pattern() {
        let s = svc();
        let err = err_of(s.create_project(&req("CreateProject", minimal_project("bad name"))));
        assert!(err.to_string().contains("pattern"), "{err}");
        // A leading hyphen violates the first-character class.
        let err = err_of(s.create_project(&req("CreateProject", minimal_project("-lead"))));
        assert!(err.to_string().contains("pattern"), "{err}");
        // A valid name with hyphen/underscore still passes.
        s.create_project(&req("CreateProject", minimal_project("ok-name_1")))
            .unwrap();
    }

    #[test]
    fn create_fleet_rejects_bad_name_pattern() {
        let s = svc();
        let err = err_of(s.create_fleet(&req(
            "CreateFleet",
            json!({
                "name": "bad name",
                "baseCapacity": 1,
                "environmentType": "LINUX_CONTAINER",
                "computeType": "BUILD_GENERAL1_SMALL"
            }),
        )));
        assert!(err.to_string().contains("pattern"), "{err}");
    }

    // retry_build_batch: an in-progress batch cannot be retried; a terminal one can.
    #[test]
    fn retry_build_batch_rejects_in_progress() {
        let s = svc();
        s.create_project(&req("CreateProject", minimal_project("rbb")))
            .unwrap();
        let id = body_of(
            s.start_build_batch(&req("StartBuildBatch", json!({ "projectName": "rbb" })))
                .unwrap(),
        )["buildBatch"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let err = err_of(s.retry_build_batch(&req("RetryBuildBatch", json!({ "id": id.clone() }))));
        assert!(err.to_string().contains("in progress"), "{err}");
        // Settle the batch to a terminal state, then retry succeeds.
        s.batch_get_build_batches(&req("BatchGetBuildBatches", json!({ "ids": [id.clone()] })))
            .unwrap();
        let retried = body_of(
            s.retry_build_batch(&req("RetryBuildBatch", json!({ "id": id })))
                .unwrap(),
        );
        assert_eq!(retried["buildBatch"]["buildBatchStatus"], "IN_PROGRESS");
    }

    // update_report_group checks existence before validating exportConfig.
    #[test]
    fn update_report_group_existence_before_validation() {
        let s = svc();
        let err = err_of(s.update_report_group(&req(
            "UpdateReportGroup",
            json!({
                "arn": "arn:aws:codebuild:us-east-1:000000000000:report-group/ghost",
                "exportConfig": { "exportConfigType": "NOT_A_TYPE" }
            }),
        )));
        // ResourceNotFound wins over the malformed exportConfig.
        assert!(err.to_string().contains("does not exist"), "{err}");
    }
}
