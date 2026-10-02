//! Amazon Managed Service for Apache Flink (`kinesisanalyticsv2`) awsJson1.1
//! dispatch + operation handlers.
//!
//! Full 33-operation control plane. Applications are a faithful,
//! persisted state machine: `StartApplication` moves `READY -> STARTING ->
//! RUNNING` (settling on the next describe), `StopApplication` moves
//! `RUNNING -> STOPPING -> READY`, mirroring how other FakeCloud services
//! auto-settle async state. Every config-changing operation increments the
//! application version and records an async operation. The real Flink-job
//! data plane (a running Docker container) is a separate later batch.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::delivery::S3Delivery;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::runtime::{
    flink_state_to_app_status, is_terminal_flink_state, FlinkBackend, FlinkRuntime, RunningFlink,
    READY_ATTEMPTS, READY_POLL_INTERVAL,
};
use crate::state::{
    Application, FlinkBinding, OperationRecord, SharedKa2State, Snapshot, TagMap, VersionRecord,
};

/// Every operation name in the kinesisanalyticsv2 Smithy model.
pub const KA2_ACTIONS: &[&str] = &[
    "AddApplicationCloudWatchLoggingOption",
    "AddApplicationInput",
    "AddApplicationInputProcessingConfiguration",
    "AddApplicationOutput",
    "AddApplicationReferenceDataSource",
    "AddApplicationVpcConfiguration",
    "CreateApplication",
    "CreateApplicationPresignedUrl",
    "CreateApplicationSnapshot",
    "DeleteApplication",
    "DeleteApplicationCloudWatchLoggingOption",
    "DeleteApplicationInputProcessingConfiguration",
    "DeleteApplicationOutput",
    "DeleteApplicationReferenceDataSource",
    "DeleteApplicationSnapshot",
    "DeleteApplicationVpcConfiguration",
    "DescribeApplication",
    "DescribeApplicationOperation",
    "DescribeApplicationSnapshot",
    "DescribeApplicationVersion",
    "DiscoverInputSchema",
    "ListApplicationOperations",
    "ListApplicationSnapshots",
    "ListApplicationVersions",
    "ListApplications",
    "ListTagsForResource",
    "RollbackApplication",
    "StartApplication",
    "StopApplication",
    "TagResource",
    "UntagResource",
    "UpdateApplication",
    "UpdateApplicationMaintenanceConfiguration",
];

// The AWS wire values for RuntimeEnvironment are hyphenated (`FLINK-1_20`,
// `SQL-1_0`, `ZEPPELIN-FLINK-1_0`) — the Smithy enum NAME is `FLINK-1_20` but
// its serialized `value` is `FLINK-1_20`, which is what the AWS SDKs and the
// terraform provider send. Validate against the wire values, not the names.
const RUNTIME_ENVIRONMENTS: &[&str] = &[
    "SQL-1_0",
    "FLINK-1_6",
    "FLINK-1_8",
    "ZEPPELIN-FLINK-1_0",
    "FLINK-1_11",
    "FLINK-1_13",
    "ZEPPELIN-FLINK-2_0",
    "FLINK-1_15",
    "ZEPPELIN-FLINK-3_0",
    "FLINK-1_18",
    "FLINK-1_19",
    "FLINK-1_20",
    "FLINK-2_2",
];
const APPLICATION_MODES: &[&str] = &["STREAMING", "INTERACTIVE"];
const URL_TYPES: &[&str] = &["FLINK_DASHBOARD_URL", "ZEPPELIN_UI_URL"];
const OPERATION_STATUSES: &[&str] = &["IN_PROGRESS", "CANCELLED", "SUCCESSFUL", "FAILED"];

pub(crate) const DEFAULT_MAINTENANCE_START: &str = "06:00";
pub(crate) const DEFAULT_MAINTENANCE_END: &str = "14:00";

pub struct Ka2Service {
    state: SharedKa2State,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    /// The backing-container runtime that spawns REAL Apache Flink session
    /// clusters and submits jobs. `None` in the degraded / control-plane-only
    /// path (no container CLI, or the backend explicitly disabled): Flink apps
    /// then behave as the pure state machine (`STARTING` settles to `RUNNING`
    /// on the next describe).
    runtime: Option<Arc<dyn FlinkBackend>>,
    /// Per-application epoch + container-operation lock for the real data
    /// plane (see [`Lifecycles`]).
    lifecycles: Lifecycles,
    /// In-process S3 reader used to fetch an application's code JAR from a
    /// fakecloud S3 bucket for submission to the Flink cluster.
    s3: Option<Arc<dyn S3Delivery>>,
    /// Shared EC2 state, used to resolve the owning VPC of an application's VPC
    /// configuration subnets (AWS derives `VpcId` from the subnets). `None` when
    /// EC2 is not wired -> a synthesized placeholder VPC id is used.
    ec2_state: Option<fakecloud_ec2::SharedEc2State>,
}

impl Ka2Service {
    pub fn new(state: SharedKa2State) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            runtime: None,
            lifecycles: Lifecycles::default(),
            s3: None,
            ec2_state: None,
        }
    }

    /// Attach shared EC2 state so a VPC configuration's `VpcId` is resolved from
    /// the owning VPC of its subnets, exactly as AWS derives it.
    pub fn with_ec2_state(mut self, ec2_state: fakecloud_ec2::SharedEc2State) -> Self {
        self.ec2_state = Some(ec2_state);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Attach the backing-container runtime that spawns real Flink session
    /// clusters. With it present, a **Flink-flavor** application with a JAR in
    /// S3 runs as a genuine Flink job in Docker.
    pub fn with_runtime(mut self, runtime: Arc<FlinkRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Attach any [`FlinkBackend`] implementation (the real runtime, or a
    /// deterministic test double).
    pub fn with_backend(mut self, backend: Arc<dyn FlinkBackend>) -> Self {
        self.runtime = Some(backend);
        self
    }

    /// Attach the in-process S3 reader used to fetch application code JARs.
    pub fn with_s3(mut self, s3: Arc<dyn S3Delivery>) -> Self {
        self.s3 = Some(s3);
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

    /// Persist hook for the CloudFormation provisioner (registered by name in
    /// the server). The CFN `AWS::KinesisAnalyticsV2::*` provisioner mutates the
    /// shared `Ka2State` directly and never triggers this service's own save
    /// path, so CloudFormation invokes this hook after a stack op to write the
    /// state through to disk -- the same persistence a direct mutating API call
    /// gets. `None` in memory mode (no snapshot store configured).
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
impl AwsService for Ka2Service {
    fn service_name(&self) -> &str {
        "kinesisanalyticsv2"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating(request.action.as_str());
        let result = dispatch(self, &request);
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        KA2_ACTIONS
    }
}

fn is_mutating(action: &str) -> bool {
    action.starts_with("Add")
        || action.starts_with("Create")
        || action.starts_with("Delete")
        || action.starts_with("Update")
        || action.starts_with("Start")
        || action.starts_with("Stop")
        || action.starts_with("Rollback")
        || action.starts_with("Tag")
        || action.starts_with("Untag")
    // NOTE: DescribeApplication is deliberately NOT listed here. It settles
    // transient lifecycle state (STARTING -> RUNNING, UPDATING -> RUNNING, ...)
    // in memory, but that settle is deterministic and idempotent -- the next
    // describe re-derives it from `settle_status`, so it never needs to be
    // persisted. Terraform's status waiters poll DescribeApplication rapidly
    // while an application settles; if every poll triggered `save()` (clone the
    // whole account state, JSON-serialize a Flink app's multi-MB code payload
    // under `spawn_blocking`, write the snapshot to disk, and AWAIT it, all
    // serialized behind the snapshot mutex), the awaited saves back up faster
    // than they drain on a 2-core CI runner. Describe then stops responding and
    // the provider's waiter wedges to the job timeout -- a stall that never
    // reproduces on a many-core dev box where the saves drain between polls.
    // A real mutation (Start/Stop/Update/...) still persists the settled state.
}

fn dispatch(s: &Ka2Service, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    match req.action.as_str() {
        "CreateApplication" => s.create_application(req),
        "DescribeApplication" => s.describe_application(req),
        "UpdateApplication" => s.update_application(req),
        "DeleteApplication" => s.delete_application(req),
        "ListApplications" => s.list_applications(req),
        "StartApplication" => s.start_application(req),
        "StopApplication" => s.stop_application(req),
        "RollbackApplication" => s.rollback_application(req),
        "CreateApplicationSnapshot" => s.create_application_snapshot(req),
        "DeleteApplicationSnapshot" => s.delete_application_snapshot(req),
        "DescribeApplicationSnapshot" => s.describe_application_snapshot(req),
        "ListApplicationSnapshots" => s.list_application_snapshots(req),
        "DescribeApplicationVersion" => s.describe_application_version(req),
        "ListApplicationVersions" => s.list_application_versions(req),
        "DescribeApplicationOperation" => s.describe_application_operation(req),
        "ListApplicationOperations" => s.list_application_operations(req),
        "AddApplicationCloudWatchLoggingOption" => s.add_cloudwatch_logging_option(req),
        "DeleteApplicationCloudWatchLoggingOption" => s.delete_cloudwatch_logging_option(req),
        "AddApplicationVpcConfiguration" => s.add_vpc_configuration(req),
        "DeleteApplicationVpcConfiguration" => s.delete_vpc_configuration(req),
        "AddApplicationInput" => s.add_input(req),
        "AddApplicationInputProcessingConfiguration" => s.add_input_processing_configuration(req),
        "DeleteApplicationInputProcessingConfiguration" => {
            s.delete_input_processing_configuration(req)
        }
        "AddApplicationOutput" => s.add_output(req),
        "DeleteApplicationOutput" => s.delete_output(req),
        "AddApplicationReferenceDataSource" => s.add_reference_data_source(req),
        "DeleteApplicationReferenceDataSource" => s.delete_reference_data_source(req),
        "DiscoverInputSchema" => s.discover_input_schema(req),
        "CreateApplicationPresignedUrl" => s.create_application_presigned_url(req),
        "UpdateApplicationMaintenanceConfiguration" => s.update_maintenance_configuration(req),
        "TagResource" => s.tag_resource(req),
        "UntagResource" => s.untag_resource(req),
        "ListTagsForResource" => s.list_tags_for_resource(req),
        _ => Err(AwsServiceError::action_not_implemented(
            s.service_name(),
            &req.action,
        )),
    }
}

// ===== helpers =====

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn ok_empty() -> Result<AwsResponse, AwsServiceError> {
    ok(json!({}))
}

fn parse(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| fault("InvalidArgumentException", &format!("malformed body: {e}")))
}

fn fault(code: &str, msg: &str) -> AwsServiceError {
    let status = if code == "ResourceNotFoundException" {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_REQUEST
    };
    AwsServiceError::aws_error(status, code, msg)
}

fn not_found(name: &str) -> AwsServiceError {
    fault(
        "ResourceNotFoundException",
        &format!("Application {name} not found."),
    )
}

/// Read + length-validate a required string field.
fn req_str_len(
    b: &Value,
    field: &str,
    min: usize,
    max: usize,
    ec: &str,
) -> Result<String, AwsServiceError> {
    let s = b
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| fault(ec, &format!("{field} is required.")))?;
    check_len(s, field, min, max, ec)?;
    Ok(s.to_string())
}

/// Read + length-validate an optional string field.
fn opt_str_len(
    b: &Value,
    field: &str,
    min: usize,
    max: usize,
    ec: &str,
) -> Result<Option<String>, AwsServiceError> {
    match b.get(field).and_then(Value::as_str) {
        Some(s) => {
            check_len(s, field, min, max, ec)?;
            Ok(Some(s.to_string()))
        }
        None => Ok(None),
    }
}

fn check_len(
    s: &str,
    field: &str,
    min: usize,
    max: usize,
    ec: &str,
) -> Result<(), AwsServiceError> {
    let len = s.chars().count();
    if len < min || len > max {
        return Err(fault(
            ec,
            &format!("{field} must be between {min} and {max} characters."),
        ));
    }
    Ok(())
}

/// Read + validate an enum-valued string field.
fn enum_field(
    b: &Value,
    field: &str,
    allowed: &[&str],
    required: bool,
    ec: &str,
) -> Result<Option<String>, AwsServiceError> {
    match b.get(field) {
        Some(Value::String(s)) => {
            if !allowed.contains(&s.as_str()) {
                return Err(fault(ec, &format!("{field} has an invalid value: {s}.")));
            }
            Ok(Some(s.clone()))
        }
        Some(Value::Null) | None => {
            if required {
                Err(fault(ec, &format!("{field} is required.")))
            } else {
                Ok(None)
            }
        }
        Some(_) => Err(fault(ec, &format!("{field} must be a string."))),
    }
}

/// Read + range-validate an integer field.
fn int_range(
    b: &Value,
    field: &str,
    min: i64,
    max: i64,
    required: bool,
    ec: &str,
) -> Result<Option<i64>, AwsServiceError> {
    match b.get(field) {
        Some(Value::Number(n)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| fault(ec, &format!("{field} must be an integer.")))?;
            if v < min || v > max {
                return Err(fault(
                    ec,
                    &format!("{field} must be between {min} and {max}."),
                ));
            }
            Ok(Some(v))
        }
        Some(Value::Null) | None => {
            if required {
                Err(fault(ec, &format!("{field} is required.")))
            } else {
                Ok(None)
            }
        }
        Some(_) => Err(fault(ec, &format!("{field} must be an integer."))),
    }
}

pub(crate) fn arn(region: &str, account: &str, name: &str) -> String {
    let partition = fakecloud_aws::arn::partition_for(region);
    format!("arn:{partition}:kinesisanalytics:{region}:{account}:application/{name}")
}

pub(crate) fn new_token() -> String {
    Uuid::new_v4().simple().to_string()
}

fn ts(t: DateTime<Utc>) -> Value {
    json!(t.timestamp())
}

fn parse_tags(b: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(arr) = b.get("Tags").and_then(Value::as_array) {
        for t in arr {
            if let Some(k) = t.get("Key").and_then(Value::as_str) {
                let v = t.get("Value").and_then(Value::as_str).unwrap_or("");
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

fn tags_json(tags: &TagMap) -> Vec<Value> {
    tags.iter()
        .map(|(k, v)| json!({ "Key": k, "Value": v }))
        .collect()
}

/// Decode a numeric next-token; empty/absent -> page start 0.
fn page_start(b: &Value) -> Option<usize> {
    match b.get("NextToken").and_then(Value::as_str) {
        None | Some("") => Some(0),
        Some(t) => t.parse::<usize>().ok(),
    }
}

fn limit(b: &Value, default: usize) -> usize {
    b.get("Limit")
        .and_then(Value::as_i64)
        .map(|n| n.max(1) as usize)
        .unwrap_or(default)
}

fn paginate(items: Vec<Value>, b: &Value, default_max: usize) -> (Vec<Value>, Option<String>) {
    let Some(start) = page_start(b) else {
        return (Vec::new(), None);
    };
    let max = limit(b, default_max);
    let total = items.len();
    if start >= total {
        return (Vec::new(), None);
    }
    let end = start.saturating_add(max).min(total);
    let next = (end < total).then(|| end.to_string());
    (items[start..end].to_vec(), next)
}

// ===== lifecycle settle =====

/// Settle a transient application status to its steady-state target. Called on
/// every describe so reads reflect the natural async progression without a
/// background timer.
fn settle_status(status: &str) -> &'static str {
    match status {
        "STARTING" => "RUNNING",
        "STOPPING" | "FORCE_STOPPING" => "READY",
        "UPDATING" | "AUTOSCALING" => "RUNNING",
        "ROLLING_BACK" => "RUNNING",
        "DELETING" => "DELETING",
        "MAINTENANCE" => "READY",
        other => match other {
            "RUNNING" => "RUNNING",
            "ROLLED_BACK" => "READY",
            _ => "READY",
        },
    }
}

// ===== detail builders =====

fn build_application_detail(app: &Application) -> Value {
    let mut detail = json!({
        "ApplicationARN": app.arn,
        "ApplicationName": app.name,
        "RuntimeEnvironment": app.runtime_environment,
        "ApplicationStatus": app.status,
        "ApplicationVersionId": app.version_id,
        "CreateTimestamp": ts(app.create_timestamp),
        "LastUpdateTimestamp": ts(app.last_update_timestamp),
        "ConditionalToken": app.conditional_token,
        "ApplicationMaintenanceConfigurationDescription": {
            "ApplicationMaintenanceWindowStartTime": app.maintenance_start,
            "ApplicationMaintenanceWindowEndTime": app.maintenance_end,
        },
    });
    let obj = detail.as_object_mut().unwrap();
    if let Some(d) = &app.description {
        obj.insert("ApplicationDescription".into(), json!(d));
    }
    if let Some(r) = &app.service_execution_role {
        obj.insert("ServiceExecutionRole".into(), json!(r));
    }
    if let Some(m) = &app.application_mode {
        obj.insert("ApplicationMode".into(), json!(m));
    }
    if app.config_description.is_object() && !app.config_description.as_object().unwrap().is_empty()
    {
        obj.insert(
            "ApplicationConfigurationDescription".into(),
            app.config_description.clone(),
        );
    }
    if !app.cloudwatch_logging_options.is_empty() {
        obj.insert(
            "CloudWatchLoggingOptionDescriptions".into(),
            json!(app.cloudwatch_logging_options),
        );
    }
    if let Some(v) = app.version_updated_from {
        obj.insert("ApplicationVersionUpdatedFrom".into(), json!(v));
    }
    if let Some(v) = app.version_rolled_back_from {
        obj.insert("ApplicationVersionRolledBackFrom".into(), json!(v));
    }
    if let Some(v) = app.version_rolled_back_to {
        obj.insert("ApplicationVersionRolledBackTo".into(), json!(v));
    }
    detail
}

fn application_summary(app: &Application) -> Value {
    let mut s = json!({
        "ApplicationName": app.name,
        "ApplicationARN": app.arn,
        "ApplicationStatus": app.status,
        "ApplicationVersionId": app.version_id,
        "RuntimeEnvironment": app.runtime_environment,
    });
    if let Some(m) = &app.application_mode {
        s.as_object_mut()
            .unwrap()
            .insert("ApplicationMode".into(), json!(m));
    }
    s
}

fn operation_info(op: &OperationRecord) -> Value {
    json!({
        "Operation": op.operation,
        "OperationId": op.operation_id,
        "StartTime": ts(op.start_time),
        "EndTime": ts(op.end_time),
        "OperationStatus": op.status,
    })
}

fn snapshot_details(s: &Snapshot) -> Value {
    json!({
        "SnapshotName": s.name,
        "SnapshotStatus": s.status,
        "ApplicationVersionId": s.application_version_id,
        "SnapshotCreationTimestamp": ts(s.create_timestamp),
        "RuntimeEnvironment": s.runtime_environment,
    })
}

// ===== config input -> description transform =====

fn str_of(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(str::to_string)
}

/// Build the `ApplicationConfigurationDescription` object from an input
/// `ApplicationConfiguration`, assigning ids to inputs/outputs/reference data
/// sources/VPC configurations.
///
/// For a **Flink** runtime (`FLINK-1_x`) AWS always materializes a full
/// `FlinkApplicationConfigurationDescription` (checkpoint / monitoring /
/// parallelism) and an `ApplicationSnapshotConfigurationDescription`, filling
/// the documented DEFAULT values for anything the caller omitted -- so a bare
/// Flink app still reads back a populated configuration. SQL / Zeppelin apps
/// only echo the members that were supplied.
pub(crate) fn config_to_description(
    cfg: &Value,
    role: Option<&str>,
    runtime: &str,
    id: &mut u64,
) -> Value {
    let is_flink = runtime.starts_with("FLINK-");
    let mut out = serde_json::Map::new();
    let mut next = || {
        *id += 1;
        id.to_string()
    };

    if let Some(sql) = cfg.get("SqlApplicationConfiguration") {
        let mut sqld = serde_json::Map::new();
        if let Some(inputs) = sql.get("Inputs").and_then(Value::as_array) {
            let descs: Vec<Value> = inputs
                .iter()
                .map(|i| input_desc(i, &mut next, role))
                .collect();
            sqld.insert("InputDescriptions".into(), json!(descs));
        }
        if let Some(outputs) = sql.get("Outputs").and_then(Value::as_array) {
            let descs: Vec<Value> = outputs
                .iter()
                .map(|o| output_desc(o, &mut next, role))
                .collect();
            sqld.insert("OutputDescriptions".into(), json!(descs));
        }
        if let Some(refs) = sql.get("ReferenceDataSources").and_then(Value::as_array) {
            let descs: Vec<Value> = refs
                .iter()
                .map(|r| reference_desc(r, &mut next, role))
                .collect();
            sqld.insert("ReferenceDataSourceDescriptions".into(), json!(descs));
        }
        out.insert(
            "SqlApplicationConfigurationDescription".into(),
            Value::Object(sqld),
        );
    }

    if is_flink {
        // AWS always returns a fully-populated FlinkApplicationConfiguration
        // description for a Flink app, defaulting any sub-config the caller
        // omitted to its documented DEFAULT values.
        out.insert(
            "FlinkApplicationConfigurationDescription".into(),
            flink_config_description(cfg.get("FlinkApplicationConfiguration")),
        );
    }

    if let Some(env) = cfg.get("EnvironmentProperties") {
        if let Some(pg) = env.get("PropertyGroups") {
            out.insert(
                "EnvironmentPropertyDescriptions".into(),
                json!({ "PropertyGroupDescriptions": pg.clone() }),
            );
        }
    }

    if let Some(code) = cfg.get("ApplicationCodeConfiguration") {
        let mut cd = serde_json::Map::new();
        if let Some(t) = str_of(code, "CodeContentType") {
            cd.insert("CodeContentType".into(), json!(t));
        } else {
            cd.insert("CodeContentType".into(), json!("ZIPFILE"));
        }
        if let Some(cc) = code.get("CodeContent") {
            let mut ccd = serde_json::Map::new();
            if let Some(t) = str_of(cc, "TextContent") {
                ccd.insert("TextContent".into(), json!(t));
            }
            if let Some(s3) = cc.get("S3ContentLocation") {
                ccd.insert(
                    "S3ApplicationCodeLocationDescription".into(),
                    s3_code_location(s3),
                );
            }
            cd.insert("CodeContentDescription".into(), Value::Object(ccd));
        }
        out.insert(
            "ApplicationCodeConfigurationDescription".into(),
            Value::Object(cd),
        );
    }

    if is_flink {
        // A Flink app always reports a snapshot configuration; when the caller
        // did not supply one AWS defaults SnapshotsEnabled to `true`.
        let enabled = cfg
            .get("ApplicationSnapshotConfiguration")
            .and_then(|s| s.get("SnapshotsEnabled"))
            .and_then(Value::as_bool)
            .unwrap_or(true);
        out.insert(
            "ApplicationSnapshotConfigurationDescription".into(),
            json!({ "SnapshotsEnabled": enabled }),
        );
    } else if let Some(snap) = cfg.get("ApplicationSnapshotConfiguration") {
        let enabled = snap
            .get("SnapshotsEnabled")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        out.insert(
            "ApplicationSnapshotConfigurationDescription".into(),
            json!({ "SnapshotsEnabled": enabled }),
        );
    }

    if let Some(rb) = cfg.get("ApplicationSystemRollbackConfiguration") {
        let enabled = rb
            .get("RollbackEnabled")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        out.insert(
            "ApplicationSystemRollbackConfigurationDescription".into(),
            json!({ "RollbackEnabled": enabled }),
        );
    }

    if let Some(vpcs) = cfg.get("VpcConfigurations").and_then(Value::as_array) {
        let descs: Vec<Value> = vpcs.iter().map(|v| vpc_desc(v, &mut next)).collect();
        out.insert("VpcConfigurationDescriptions".into(), json!(descs));
    }

    if let Some(z) = cfg.get("ZeppelinApplicationConfiguration") {
        let mut zd = serde_json::Map::new();
        let log_level = z
            .get("MonitoringConfiguration")
            .and_then(|m| str_of(m, "LogLevel"))
            .unwrap_or_else(|| "INFO".to_string());
        zd.insert(
            "MonitoringConfigurationDescription".into(),
            json!({ "LogLevel": log_level }),
        );
        out.insert(
            "ZeppelinApplicationConfigurationDescription".into(),
            Value::Object(zd),
        );
    }

    if let Some(enc) = cfg.get("ApplicationEncryptionConfiguration") {
        let mut ed = serde_json::Map::new();
        if let Some(k) = str_of(enc, "KeyId") {
            ed.insert("KeyId".into(), json!(k));
        }
        ed.insert(
            "KeyType".into(),
            json!(str_of(enc, "KeyType").unwrap_or_else(|| "AWS_OWNED_KEY".to_string())),
        );
        out.insert(
            "ApplicationEncryptionConfigurationDescription".into(),
            Value::Object(ed),
        );
    }

    Value::Object(out)
}

// ===== Flink application-configuration defaults =====
//
// AWS auto-populates a Flink app's FlinkApplicationConfiguration description on
// create, and returns it (plus its DEFAULT-vs-CUSTOM per-sub-config values) on
// every describe. When a sub-config's ConfigurationType is DEFAULT (or the
// sub-config was omitted entirely) AWS reports the documented default values;
// when CUSTOM it echoes the caller's values (defaulting any individual field
// the caller left unset).

const DEFAULT_CHECKPOINT_INTERVAL: i64 = 60_000;
const DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS: i64 = 5_000;
const DEFAULT_METRICS_LEVEL: &str = "APPLICATION";
const DEFAULT_LOG_LEVEL: &str = "INFO";

/// Build the full `FlinkApplicationConfigurationDescription` (all three
/// sub-descriptions) from an optional input `FlinkApplicationConfiguration`.
fn flink_config_description(flink: Option<&Value>) -> Value {
    json!({
        "CheckpointConfigurationDescription":
            checkpoint_description(flink.and_then(|f| f.get("CheckpointConfiguration"))),
        "MonitoringConfigurationDescription":
            monitoring_description(flink.and_then(|f| f.get("MonitoringConfiguration"))),
        "ParallelismConfigurationDescription":
            parallelism_description(flink.and_then(|f| f.get("ParallelismConfiguration"))),
    })
}

/// The `ConfigurationType` of an input sub-config, defaulting to `DEFAULT` when
/// the sub-config is absent or did not specify one.
fn configuration_type(input: Option<&Value>) -> String {
    input
        .and_then(|v| str_of(v, "ConfigurationType"))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "DEFAULT".to_string())
}

fn checkpoint_description(input: Option<&Value>) -> Value {
    let ctype = configuration_type(input);
    let custom = ctype == "CUSTOM";
    let get_bool = |k: &str, d: bool| {
        input
            .filter(|_| custom)
            .and_then(|v| v.get(k))
            .and_then(Value::as_bool)
            .unwrap_or(d)
    };
    let get_int = |k: &str, d: i64| {
        input
            .filter(|_| custom)
            .and_then(|v| v.get(k))
            .and_then(Value::as_i64)
            .unwrap_or(d)
    };
    json!({
        "ConfigurationType": ctype,
        "CheckpointingEnabled": get_bool("CheckpointingEnabled", true),
        "CheckpointInterval": get_int("CheckpointInterval", DEFAULT_CHECKPOINT_INTERVAL),
        "MinPauseBetweenCheckpoints":
            get_int("MinPauseBetweenCheckpoints", DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS),
    })
}

fn monitoring_description(input: Option<&Value>) -> Value {
    let ctype = configuration_type(input);
    let custom = ctype == "CUSTOM";
    let get_str = |k: &str, d: &str| {
        input
            .filter(|_| custom)
            .and_then(|v| str_of(v, k))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| d.to_string())
    };
    json!({
        "ConfigurationType": ctype,
        "LogLevel": get_str("LogLevel", DEFAULT_LOG_LEVEL),
        "MetricsLevel": get_str("MetricsLevel", DEFAULT_METRICS_LEVEL),
    })
}

fn parallelism_description(input: Option<&Value>) -> Value {
    let ctype = configuration_type(input);
    let custom = ctype == "CUSTOM";
    let get_int = |k: &str, d: i64| {
        input
            .filter(|_| custom)
            .and_then(|v| v.get(k))
            .and_then(Value::as_i64)
            .unwrap_or(d)
    };
    let parallelism = get_int("Parallelism", 1);
    let auto_scaling = input
        .filter(|_| custom)
        .and_then(|v| v.get("AutoScalingEnabled"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    json!({
        "ConfigurationType": ctype,
        "Parallelism": parallelism,
        "ParallelismPerKPU": get_int("ParallelismPerKPU", 1),
        "AutoScalingEnabled": auto_scaling,
        "CurrentParallelism": parallelism,
    })
}

fn s3_code_location(s3: &Value) -> Value {
    let mut m = serde_json::Map::new();
    m.insert(
        "BucketARN".into(),
        json!(str_of(s3, "BucketARN").unwrap_or_default()),
    );
    m.insert(
        "FileKey".into(),
        json!(str_of(s3, "FileKey").unwrap_or_default()),
    );
    if let Some(v) = str_of(s3, "ObjectVersion") {
        m.insert("ObjectVersion".into(), json!(v));
    }
    Value::Object(m)
}

/// Build the ordered list of in-app stream names AWS mints for a SQL input:
/// one `<NamePrefix>_NNN` (1-based, zero-padded to 3 digits) per unit of the
/// input's parallelism `Count`. So a `Count=1` input reports a single stream
/// and a `Count=42` input reports 42.
fn in_app_stream_names(name_prefix: &str, count: i64) -> Vec<String> {
    (1..=count.max(1))
        .map(|i| format!("{name_prefix}_{i:03}"))
        .collect()
}

fn input_desc(input: &Value, next: &mut impl FnMut() -> String, role: Option<&str>) -> Value {
    let mut d = serde_json::Map::new();
    d.insert("InputId".into(), json!(next()));
    // AWS auto-populates InputParallelism.Count to 1 when the caller omits it.
    let count = input
        .get("InputParallelism")
        .and_then(|p| p.get("Count"))
        .and_then(Value::as_i64)
        .unwrap_or(1);
    if let Some(np) = str_of(input, "NamePrefix") {
        d.insert(
            "InAppStreamNames".into(),
            json!(in_app_stream_names(&np, count)),
        );
        d.insert("NamePrefix".into(), json!(np));
    }
    if let Some(ipc) = input.get("InputProcessingConfiguration") {
        if let Some(lp) = ipc.get("InputLambdaProcessor") {
            d.insert(
                "InputProcessingConfigurationDescription".into(),
                json!({
                    "InputLambdaProcessorDescription": resource_role_desc(lp, role)
                }),
            );
        }
    }
    if let Some(k) = input.get("KinesisStreamsInput") {
        d.insert(
            "KinesisStreamsInputDescription".into(),
            resource_role_desc(k, role),
        );
    }
    if let Some(k) = input.get("KinesisFirehoseInput") {
        d.insert(
            "KinesisFirehoseInputDescription".into(),
            resource_role_desc(k, role),
        );
    }
    d.insert("InputParallelism".into(), json!({ "Count": count }));
    if let Some(s) = input.get("InputSchema") {
        d.insert("InputSchema".into(), s.clone());
    }
    // AWS always reports an InputStartingPositionConfiguration on the input; its
    // InputStartingPosition stays unset (reads back empty) until the application
    // is started with a run configuration that pins one.
    d.insert("InputStartingPositionConfiguration".into(), json!({}));
    Value::Object(d)
}

pub(crate) fn output_desc(
    output: &Value,
    next: &mut impl FnMut() -> String,
    role: Option<&str>,
) -> Value {
    let mut d = serde_json::Map::new();
    d.insert("OutputId".into(), json!(next()));
    if let Some(n) = str_of(output, "Name") {
        d.insert("Name".into(), json!(n));
    }
    if let Some(k) = output.get("KinesisStreamsOutput") {
        d.insert(
            "KinesisStreamsOutputDescription".into(),
            resource_role_desc(k, role),
        );
    }
    if let Some(k) = output.get("KinesisFirehoseOutput") {
        d.insert(
            "KinesisFirehoseOutputDescription".into(),
            resource_role_desc(k, role),
        );
    }
    if let Some(k) = output.get("LambdaOutput") {
        d.insert(
            "LambdaOutputDescription".into(),
            resource_role_desc(k, role),
        );
    }
    if let Some(ds) = output.get("DestinationSchema") {
        d.insert("DestinationSchema".into(), ds.clone());
    }
    Value::Object(d)
}

pub(crate) fn reference_desc(
    r: &Value,
    next: &mut impl FnMut() -> String,
    role: Option<&str>,
) -> Value {
    let mut d = serde_json::Map::new();
    d.insert("ReferenceId".into(), json!(next()));
    d.insert(
        "TableName".into(),
        json!(str_of(r, "TableName").unwrap_or_default()),
    );
    let s3 = r.get("S3ReferenceDataSource");
    let mut s3d = serde_json::Map::new();
    s3d.insert(
        "BucketARN".into(),
        json!(s3.and_then(|v| str_of(v, "BucketARN")).unwrap_or_default()),
    );
    s3d.insert(
        "FileKey".into(),
        json!(s3.and_then(|v| str_of(v, "FileKey")).unwrap_or_default()),
    );
    if let Some(r) = role {
        s3d.insert("ReferenceRoleARN".into(), json!(r));
    }
    d.insert(
        "S3ReferenceDataSourceDescription".into(),
        Value::Object(s3d),
    );
    if let Some(s) = r.get("ReferenceSchema") {
        d.insert("ReferenceSchema".into(), s.clone());
    }
    Value::Object(d)
}

fn resource_role_desc(v: &Value, role: Option<&str>) -> Value {
    let mut m = serde_json::Map::new();
    m.insert(
        "ResourceARN".into(),
        json!(str_of(v, "ResourceARN").unwrap_or_default()),
    );
    if let Some(r) = role {
        m.insert("RoleARN".into(), json!(r));
    }
    Value::Object(m)
}

fn vpc_desc(v: &Value, next: &mut impl FnMut() -> String) -> Value {
    let subnets = v.get("SubnetIds").cloned().unwrap_or_else(|| json!([]));
    let sgs = v
        .get("SecurityGroupIds")
        .cloned()
        .unwrap_or_else(|| json!([]));
    json!({
        "VpcConfigurationId": next(),
        "VpcId": "vpc-0a1b2c3d4e5f6a7b8",
        "SubnetIds": subnets,
        "SecurityGroupIds": sgs,
    })
}

pub(crate) fn cloudwatch_desc(opt: &Value, id: &str, role: Option<&str>) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("CloudWatchLoggingOptionId".into(), json!(id));
    m.insert(
        "LogStreamARN".into(),
        json!(str_of(opt, "LogStreamARN").unwrap_or_default()),
    );
    if let Some(r) = role {
        m.insert("RoleARN".into(), json!(r));
    }
    Value::Object(m)
}

// ===== version + operation recording =====

pub(crate) fn record_version(app: &mut Application) {
    let detail = build_application_detail(app);
    app.versions.insert(
        app.version_id,
        VersionRecord {
            version_id: app.version_id,
            status: app.status.clone(),
            create_timestamp: Utc::now(),
            detail,
        },
    );
}

fn record_operation(
    app: &mut Application,
    operation: &str,
    from: Option<i64>,
    to: Option<i64>,
) -> String {
    let now = Utc::now();
    let id = Uuid::new_v4().simple().to_string();
    app.operations.push(OperationRecord {
        operation_id: id.clone(),
        operation: operation.to_string(),
        start_time: now,
        end_time: now,
        status: "SUCCESSFUL".to_string(),
        version_from: from,
        version_to: to,
    });
    id
}

/// Bump the application version for a config-changing operation, refresh the
/// conditional token, and record the new version snapshot.
pub(crate) fn bump_version(app: &mut Application) {
    let from = app.version_id;
    app.version_id += 1;
    app.version_updated_from = Some(from);
    app.version_rolled_back_from = None;
    app.version_rolled_back_to = None;
    app.conditional_token = new_token();
    app.last_update_timestamp = Utc::now();
    record_version(app);
}

/// Optimistic-concurrency check for `CurrentApplicationVersionId` /
/// `ConditionalToken`. Only enforced when the caller supplied one.
fn check_concurrency(app: &Application, b: &Value) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get("CurrentApplicationVersionId").and_then(Value::as_i64) {
        if v != app.version_id {
            return Err(fault(
                "ConcurrentModificationException",
                &format!(
                    "The current application version id is {}, not {v}.",
                    app.version_id
                ),
            ));
        }
    }
    if let Some(t) = b.get("ConditionalToken").and_then(Value::as_str) {
        if !t.is_empty() && t != app.conditional_token {
            return Err(fault(
                "ConcurrentModificationException",
                "The supplied ConditionalToken does not match the current application state.",
            ));
        }
    }
    Ok(())
}

// ===== handlers =====

impl Ka2Service {
    /// Overwrite each `VpcConfigurationDescription.VpcId` in a config
    /// description with the real owning VPC of its subnets, resolved from EC2
    /// state (AWS derives `VpcId` from the configuration's subnets). A no-op
    /// when EC2 state is not wired or a subnet is unknown -- the synthesized
    /// placeholder id set by `vpc_desc` is then left in place.
    /// Snapshot the EC2 `subnet_id -> owning vpc_id` map for an account, taken
    /// WITHOUT holding the ka2 state lock. Callers gather this *before*
    /// acquiring `self.state.write()` so the EC2 read lock is never nested
    /// inside the ka2 write lock. That nesting (ka2.write -> ec2.read) is a
    /// cross-service lock ordering: under `parking_lot`'s writer-priority, a
    /// concurrent EC2 writer (terraform provisions the VPC/subnets/security
    /// groups in parallel with the application) makes the nested `ec2.read()`
    /// block while the ka2 write lock is held, stalling every other ka2 request
    /// -- on a 2-core CI runner this wedged the provider's status waiters to the
    /// job timeout even though it never reproduced on a many-core dev box.
    /// Returns an empty map when EC2 is not wired.
    fn ec2_subnet_vpc_map(&self, account: &str) -> BTreeMap<String, String> {
        let Some(ec2) = self.ec2_state.as_ref() else {
            return BTreeMap::new();
        };
        let guard = ec2.read();
        guard
            .get(account)
            .map(|st| {
                st.subnets
                    .iter()
                    .map(|(sid, sub)| (sid.clone(), sub.vpc_id.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Overwrite each VPC configuration description's `VpcId` from a
    /// pre-snapshotted `subnet_id -> vpc_id` map (see `ec2_subnet_vpc_map`).
    /// Pure: takes no lock, so it is safe to call while holding `self.state`.
    fn resolve_vpc_ids(cfg: &mut Value, subnet_vpc: &BTreeMap<String, String>) {
        let Some(arr) = cfg
            .get_mut("VpcConfigurationDescriptions")
            .and_then(Value::as_array_mut)
        else {
            return;
        };
        for d in arr.iter_mut() {
            let vpc = d
                .get("SubnetIds")
                .and_then(Value::as_array)
                .and_then(|s| s.iter().filter_map(Value::as_str).next())
                .and_then(|sid| subnet_vpc.get(sid).cloned());
            if let (Some(v), Some(o)) = (vpc, d.as_object_mut()) {
                o.insert("VpcId".into(), json!(v));
            }
        }
    }

    fn create_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let description = opt_str_len(&b, "ApplicationDescription", 0, 1024, ec)?;
        let runtime = enum_field(&b, "RuntimeEnvironment", RUNTIME_ENVIRONMENTS, true, ec)?
            .ok_or_else(|| fault(ec, "RuntimeEnvironment is required."))?;
        let role = req_str_len(&b, "ServiceExecutionRole", 1, 2048, ec)?;
        let mode = enum_field(&b, "ApplicationMode", APPLICATION_MODES, false, ec)?;

        // Snapshot the EC2 subnet->vpc map BEFORE taking the ka2 write lock so
        // the EC2 read lock is never nested inside it (see `ec2_subnet_vpc_map`).
        let vpc_map = self.ec2_subnet_vpc_map(&req.account_id);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // Build + insert through the shared builder so a CFN-provisioned
        // `AWS::KinesisAnalyticsV2::Application` stores byte-identical wire state
        // and cannot diverge from this path (#1766).
        crate::builders::insert_application(
            st,
            &req.region,
            &req.account_id,
            &name,
            description,
            &runtime,
            &role,
            mode,
            b.get("ApplicationConfiguration"),
            b.get("CloudWatchLoggingOptions"),
            parse_tags(&b),
        )
        .map_err(|m| fault("ResourceInUseException", &m))?;
        let app = st.applications.get_mut(&name).expect("just inserted");
        // Resolve each VPC configuration's owning VpcId from EC2 state, then
        // refresh the version-1 detail so the persisted version reflects it.
        Self::resolve_vpc_ids(&mut app.config_description, &vpc_map);
        record_version(app);
        let detail = build_application_detail(app);
        ok(json!({ "ApplicationDetail": detail }))
    }

    fn describe_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        // A Flink-flavor app backed by the REAL runtime has ALL its lifecycle
        // transitions driven by the background container/job tasks -- never
        // auto-settle those in place (that would report RUNNING before the real
        // Flink job is actually up). The pure control-plane path (SQL apps, no
        // runtime, or a Flink app with no JAR) keeps the in-memory settle.
        let real = self.runtime.is_some() && is_real_flink(app);
        if !real {
            let settled = settle_status(&app.status);
            if settled != app.status {
                app.status = settled.to_string();
            }
        }
        let detail = build_application_detail(app);
        // If a real job is RUNNING, reconcile it against the live cluster in the
        // background so a later describe reflects a job that finished/failed on
        // its own (the "a read fires a transition" pattern).
        let reconcile = if real && app.status == "RUNNING" {
            app.flink_binding.as_ref().and_then(|b| {
                b.job_id.as_ref().and_then(|j| {
                    let epoch = self.lifecycles.current(&app.arn);
                    self.task_ctx(&req.account_id, &name, &app.arn, epoch)
                        .map(|ctx| (ctx, b.clone(), j.clone()))
                })
            })
        } else {
            None
        };
        drop(accounts);
        if let Some((ctx, binding, job_id)) = reconcile {
            self.spawn_flink_reconcile(ctx, binding, job_id);
        }
        ok(json!({ "ApplicationDetail": detail }))
    }

    fn update_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, false, ec)?;
        opt_str_len(&b, "ConditionalToken", 1, 512, ec)?;
        let runtime_update = enum_field(
            &b,
            "RuntimeEnvironmentUpdate",
            RUNTIME_ENVIRONMENTS,
            false,
            ec,
        )?;
        let role_update = opt_str_len(&b, "ServiceExecutionRoleUpdate", 1, 2048, ec)?;

        // Snapshot EC2 subnet->vpc BEFORE the ka2 write lock (no nested locks).
        let vpc_map = self.ec2_subnet_vpc_map(&req.account_id);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        check_concurrency(app, &b)?;

        if let Some(r) = runtime_update {
            app.runtime_environment = r;
        }
        if let Some(r) = role_update {
            app.service_execution_role = Some(r);
        }
        if let Some(cfg) = b.get("ApplicationConfigurationUpdate") {
            if cfg.is_object() {
                apply_config_update(app, cfg);
                // A VpcConfigurationUpdate may change the subnets; re-resolve the
                // owning VpcId from EC2 state.
                Self::resolve_vpc_ids(&mut app.config_description, &vpc_map);
            }
        }
        // A `RunConfigurationUpdate` (top-level on UpdateApplication) revises the
        // run configuration a started Flink app reports.
        if app.runtime_environment.starts_with("FLINK-") {
            if let Some(rc) = b.get("RunConfigurationUpdate") {
                let desc = run_config_description(Some(rc));
                ensure_object(&mut app.config_description)
                    .insert("RunConfigurationDescription".into(), desc);
            }
        }
        if let Some(updates) = b
            .get("CloudWatchLoggingOptionUpdates")
            .and_then(Value::as_array)
        {
            let role = app.service_execution_role.clone();
            for u in updates {
                if let Some(oid) = str_of(u, "CloudWatchLoggingOptionId") {
                    for opt in app.cloudwatch_logging_options.iter_mut() {
                        if opt.get("CloudWatchLoggingOptionId").and_then(Value::as_str)
                            == Some(oid.as_str())
                        {
                            if let Some(ls) = str_of(u, "LogStreamARNUpdate") {
                                opt.as_object_mut()
                                    .unwrap()
                                    .insert("LogStreamARN".into(), json!(ls));
                            }
                            if let Some(r) = &role {
                                opt.as_object_mut()
                                    .unwrap()
                                    .insert("RoleARN".into(), json!(r));
                            }
                        }
                    }
                }
            }
        }
        let from = app.version_id;
        // A real Flink app that is running (or on its way there) redeploys its
        // job with the new configuration -- UPDATING until Flink reports the new
        // job RUNNING. Superseding the epoch cancels any in-flight bring-up or
        // earlier redeploy so it can't settle the app on the stale config.
        let redeploy = self.runtime.is_some()
            && is_real_flink(app)
            && ACTIVE_STATUSES.contains(&app.status.as_str());
        if app.status == "RUNNING" || (redeploy && app.status != "STARTING") {
            app.status = "UPDATING".to_string();
        }
        bump_version(app);
        let op_id = record_operation(app, "UpdateApplication", Some(from), Some(app.version_id));
        let detail = build_application_detail(app);
        let deploy = if redeploy {
            let epoch = self.lifecycles.bump(&app.arn);
            DeploySpec::of(app).and_then(|spec| {
                Some((
                    self.task_ctx(&req.account_id, &name, &app.arn, epoch)?,
                    spec,
                ))
            })
        } else {
            None
        };
        drop(accounts);
        if let Some((ctx, spec)) = deploy {
            self.spawn_flink_deploy(ctx, spec);
        }
        ok(json!({ "ApplicationDetail": detail, "OperationId": op_id }))
    }

    fn delete_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        if b.get("CreateTimestamp").is_none() {
            return Err(fault(ec, "CreateTimestamp is required."));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .remove(&name)
            .ok_or_else(|| not_found(&name))?;
        st.tags.remove(&app.arn);
        // Supersede any in-flight bring-up (so it can't resurrect state or keep
        // its container) and tear down every container of the app, dropping the
        // runtime's tracking entry so a same-name re-create starts clean.
        let teardown =
            if self.runtime.is_some() && (is_real_flink(&app) || app.flink_binding.is_some()) {
                let epoch = self.lifecycles.bump(&app.arn);
                self.task_ctx(&req.account_id, &name, &app.arn, epoch)
            } else {
                None
            };
        drop(accounts);
        if let Some(ctx) = teardown {
            self.spawn_flink_teardown(ctx, app.flink_binding.clone(), false);
        }
        ok_empty()
    }

    fn list_applications(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidRequestException";
        int_range(&b, "Limit", 1, 50, false, ec)?;
        // NextToken is typed `ApplicationName` (len 1..=128) in this op.
        opt_str_len(&b, "NextToken", 1, 128, ec)?;
        let accounts = self.state.read();
        let summaries: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| st.applications.values().map(application_summary).collect())
            .unwrap_or_default();
        // This op paginates by application name in the token; emulate with a
        // numeric offset for determinism.
        let start = b
            .get("NextToken")
            .and_then(Value::as_str)
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let max = limit(&b, 50);
        let total = summaries.len();
        let end = start.saturating_add(max).min(total);
        let page = if start < total {
            summaries[start..end].to_vec()
        } else {
            Vec::new()
        };
        let mut out = json!({ "ApplicationSummaries": page });
        if end < total {
            out.as_object_mut()
                .unwrap()
                .insert("NextToken".into(), json!(end.to_string()));
        }
        ok(out)
    }

    fn start_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        if app.runtime_environment.starts_with("FLINK-") {
            // Once a Flink app is started, AWS records a RunConfiguration
            // description on it -- defaulting to RESTORE_FROM_LATEST_SNAPSHOT /
            // AllowNonRestoredState=false when the caller supplied nothing, and
            // echoing any provided RunConfiguration otherwise.
            let desc = run_config_description(b.get("RunConfiguration"));
            ensure_object(&mut app.config_description)
                .insert("RunConfigurationDescription".into(), desc);
        } else if let Some(rc) = b.get("RunConfiguration") {
            // A SQL app's RunConfiguration pins each input's starting position
            // via SqlRunConfigurations; unlike Flink it records no
            // RunConfigurationDescription.
            apply_sql_run_configuration(app, rc);
        }
        app.status = "STARTING".to_string();
        let op_id = record_operation(app, "StartApplication", None, None);
        // Real path: a Flink-flavor app with a JAR in S3 and a runtime attached
        // spawns a genuine Flink session cluster and submits the job in the
        // background (never block the handler on a container pull/start/submit,
        // issues #1539/#1730). The app settles to RUNNING only once Flink reports
        // the job RUNNING. Otherwise the pure state machine settles on describe.
        let real = self.runtime.is_some() && is_real_flink(app);
        let deploy = if real {
            let epoch = self.lifecycles.bump(&app.arn);
            DeploySpec::of(app).and_then(|spec| {
                Some((
                    self.task_ctx(&req.account_id, &name, &app.arn, epoch)?,
                    spec,
                ))
            })
        } else {
            None
        };
        drop(accounts);
        if let Some((ctx, spec)) = deploy {
            self.spawn_flink_deploy(ctx, spec);
        }
        ok(json!({ "OperationId": op_id }))
    }

    fn stop_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let force = b.get("Force").and_then(Value::as_bool).unwrap_or(false);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        // AWS drops the RunConfiguration description once an application is
        // stopped; it is re-established on the next StartApplication.
        if let Some(obj) = app.config_description.as_object_mut() {
            obj.remove("RunConfigurationDescription");
        }
        let real = self.runtime.is_some() && is_real_flink(app);
        let binding = app.flink_binding.clone();
        // Anything to tear down: a bound cluster, or a lifecycle task in flight
        // (a bring-up/redeploy may be creating a container right now).
        let needs_teardown = real
            && (binding.is_some()
                || ACTIVE_STATUSES.contains(&app.status.as_str())
                || matches!(app.status.as_str(), "STOPPING" | "FORCE_STOPPING"));
        if real && !needs_teardown {
            // Nothing running: settle straight to READY (describe skips the
            // in-memory settle for real apps, so a STOPPING with no background
            // canceller would strand forever).
            app.status = "READY".to_string();
        } else {
            app.status = if force {
                "FORCE_STOPPING".to_string()
            } else {
                "STOPPING".to_string()
            };
        }
        let op_id = record_operation(app, "StopApplication", None, None);
        // Supersede any in-flight start/redeploy so it can't flip the app back
        // to RUNNING after this stop.
        let teardown = if real {
            let epoch = self.lifecycles.bump(&app.arn);
            if needs_teardown {
                self.task_ctx(&req.account_id, &name, &app.arn, epoch)
            } else {
                None
            }
        } else {
            None
        };
        drop(accounts);
        if let Some(ctx) = teardown {
            self.spawn_flink_teardown(ctx, binding, true);
        }
        ok(json!({ "OperationId": op_id }))
    }

    fn rollback_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let current = int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, true, ec)?
            .ok_or_else(|| fault(ec, "CurrentApplicationVersionId is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        if current != app.version_id {
            return Err(fault(
                "ConcurrentModificationException",
                &format!(
                    "The current application version id is {}, not {current}.",
                    app.version_id
                ),
            ));
        }
        let target = app.version_id.saturating_sub(1).max(1);
        // Restore the target version's configuration (code, Flink config, run
        // configuration, ...), runtime and execution role into a new version.
        if let Some(prev) = app.versions.get(&target).cloned() {
            if let Some(mut cfg) = prev
                .detail
                .get("ApplicationConfigurationDescription")
                .cloned()
            {
                // The run configuration belongs to the current run, not to the
                // version: a rolled-back app keeps running with it.
                let run_cfg = app
                    .config_description
                    .get("RunConfigurationDescription")
                    .cloned();
                match (run_cfg, cfg.as_object_mut()) {
                    (Some(rc), Some(obj)) => {
                        obj.insert("RunConfigurationDescription".into(), rc);
                    }
                    (None, Some(obj)) => {
                        obj.remove("RunConfigurationDescription");
                    }
                    _ => {}
                }
                app.config_description = cfg;
            }
            if let Some(rt) = prev
                .detail
                .get("RuntimeEnvironment")
                .and_then(Value::as_str)
            {
                app.runtime_environment = rt.to_string();
            }
            if let Some(role) = prev
                .detail
                .get("ServiceExecutionRole")
                .and_then(Value::as_str)
            {
                app.service_execution_role = Some(role.to_string());
            }
        }
        // AWS rolls a running (or stuck-updating) application back to the
        // previous version and keeps it RUNNING on that configuration:
        // ROLLING_BACK while the previous job is redeployed, then RUNNING. An
        // application that was not running just takes the previous config.
        let was_active = ACTIVE_STATUSES.contains(&app.status.as_str());
        let from = app.version_id;
        app.version_id += 1;
        app.conditional_token = new_token();
        app.last_update_timestamp = Utc::now();
        app.version_rolled_back_from = Some(from);
        app.version_rolled_back_to = Some(target);
        app.version_updated_from = Some(from);
        app.status = if was_active { "ROLLING_BACK" } else { "READY" }.to_string();
        record_version(app);
        let op_id = record_operation(app, "RollbackApplication", Some(from), Some(app.version_id));
        let detail = build_application_detail(app);
        let real = self.runtime.is_some() && is_real_flink(app);
        let deploy = if real && was_active {
            let epoch = self.lifecycles.bump(&app.arn);
            DeploySpec::of(app).and_then(|spec| {
                Some((
                    self.task_ctx(&req.account_id, &name, &app.arn, epoch)?,
                    spec,
                ))
            })
        } else {
            None
        };
        drop(accounts);
        if let Some((ctx, spec)) = deploy {
            self.spawn_flink_deploy(ctx, spec);
        }
        ok(json!({ "ApplicationDetail": detail, "OperationId": op_id }))
    }

    fn create_application_snapshot(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let snap_name = req_str_len(&b, "SnapshotName", 1, 256, ec)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        if app.snapshots.contains_key(&snap_name) {
            return Err(fault(
                "ResourceInUseException",
                &format!("Snapshot {snap_name} already exists."),
            ));
        }
        app.snapshots.insert(
            snap_name.clone(),
            Snapshot {
                name: snap_name,
                status: "READY".to_string(),
                application_version_id: app.version_id,
                create_timestamp: Utc::now(),
                runtime_environment: app.runtime_environment.clone(),
            },
        );
        ok_empty()
    }

    fn delete_application_snapshot(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let snap_name = req_str_len(&b, "SnapshotName", 1, 256, ec)?;
        if b.get("SnapshotCreationTimestamp").is_none() {
            return Err(fault(ec, "SnapshotCreationTimestamp is required."));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        if app.snapshots.remove(&snap_name).is_none() {
            return Err(fault(
                "ResourceNotFoundException",
                &format!("Snapshot {snap_name} not found."),
            ));
        }
        ok_empty()
    }

    fn describe_application_snapshot(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let snap_name = req_str_len(&b, "SnapshotName", 1, 256, ec)?;
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(&name))
            .ok_or_else(|| not_found(&name))?;
        let snap = app.snapshots.get(&snap_name).ok_or_else(|| {
            fault(
                "ResourceNotFoundException",
                &format!("Snapshot {snap_name} not found."),
            )
        })?;
        ok(json!({ "SnapshotDetails": snapshot_details(snap) }))
    }

    fn list_application_snapshots(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "Limit", 1, 50, false, ec)?;
        opt_str_len(&b, "NextToken", 1, 512, ec)?;
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(&name));
        let items: Vec<Value> = app
            .map(|a| a.snapshots.values().map(snapshot_details).collect())
            .unwrap_or_default();
        let (page, next) = paginate(items, &b, 50);
        let mut out = json!({ "SnapshotSummaries": page });
        if let Some(n) = next {
            out.as_object_mut()
                .unwrap()
                .insert("NextToken".into(), json!(n));
        }
        ok(out)
    }

    fn describe_application_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let version = int_range(&b, "ApplicationVersionId", 1, 999_999_999, true, ec)?
            .ok_or_else(|| fault(ec, "ApplicationVersionId is required."))?;
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(&name))
            .ok_or_else(|| not_found(&name))?;
        match app.versions.get(&version) {
            Some(v) => ok(json!({ "ApplicationVersionDetail": v.detail })),
            None => ok(json!({})),
        }
    }

    fn list_application_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "Limit", 1, 50, false, ec)?;
        opt_str_len(&b, "NextToken", 1, 512, ec)?;
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(&name))
            .ok_or_else(|| not_found(&name))?;
        let items: Vec<Value> = app
            .versions
            .values()
            .map(|v| json!({ "ApplicationVersionId": v.version_id, "ApplicationStatus": v.status }))
            .collect();
        let (page, next) = paginate(items, &b, 50);
        let mut out = json!({ "ApplicationVersionSummaries": page });
        if let Some(n) = next {
            out.as_object_mut()
                .unwrap()
                .insert("NextToken".into(), json!(n));
        }
        ok(out)
    }

    fn describe_application_operation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let op_id = req_str_len(&b, "OperationId", 1, 64, ec)?;
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(&name))
            .ok_or_else(|| not_found(&name))?;
        let op = app
            .operations
            .iter()
            .find(|o| o.operation_id == op_id)
            .ok_or_else(|| {
                fault(
                    "ResourceNotFoundException",
                    &format!("Operation {op_id} not found."),
                )
            })?;
        let mut details = json!({
            "Operation": op.operation,
            "StartTime": ts(op.start_time),
            "EndTime": ts(op.end_time),
            "OperationStatus": op.status,
        });
        if let (Some(from), Some(to)) = (op.version_from, op.version_to) {
            details.as_object_mut().unwrap().insert(
                "ApplicationVersionChangeDetails".into(),
                json!({
                    "ApplicationVersionUpdatedFrom": from,
                    "ApplicationVersionUpdatedTo": to,
                }),
            );
        }
        ok(json!({ "ApplicationOperationInfoDetails": details }))
    }

    fn list_application_operations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "Limit", 1, 50, false, ec)?;
        opt_str_len(&b, "NextToken", 1, 512, ec)?;
        opt_str_len(&b, "Operation", 1, 64, ec)?;
        enum_field(&b, "OperationStatus", OPERATION_STATUSES, false, ec)?;
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(&name))
            .ok_or_else(|| not_found(&name))?;
        let op_filter = b.get("Operation").and_then(Value::as_str);
        let status_filter = b.get("OperationStatus").and_then(Value::as_str);
        let items: Vec<Value> = app
            .operations
            .iter()
            .filter(|o| op_filter.is_none_or(|f| o.operation == f))
            .filter(|o| status_filter.is_none_or(|f| o.status == f))
            .map(operation_info)
            .collect();
        let (page, next) = paginate(items, &b, 50);
        let mut out = json!({ "ApplicationOperationInfoList": page });
        if let Some(n) = next {
            out.as_object_mut()
                .unwrap()
                .insert("NextToken".into(), json!(n));
        }
        ok(out)
    }

    fn add_cloudwatch_logging_option(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, false, ec)?;
        opt_str_len(&b, "ConditionalToken", 1, 512, ec)?;
        let opt = b
            .get("CloudWatchLoggingOption")
            .cloned()
            .ok_or_else(|| fault(ec, "CloudWatchLoggingOption is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        {
            let app = st.applications.get(&name).ok_or_else(|| not_found(&name))?;
            check_concurrency(app, &b)?;
        }
        crate::builders::insert_cloudwatch_logging_option(st, &name, &opt)
            .map_err(|_| not_found(&name))?;
        let app = st.applications.get_mut(&name).expect("just updated");
        let op_id = record_operation(
            app,
            "UpdateApplication",
            app.version_updated_from,
            Some(app.version_id),
        );
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "CloudWatchLoggingOptionDescriptions": app.cloudwatch_logging_options,
            "OperationId": op_id,
        }))
    }

    fn delete_cloudwatch_logging_option(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, false, ec)?;
        let opt_id = req_str_len(&b, "CloudWatchLoggingOptionId", 1, 50, ec)?;
        opt_str_len(&b, "ConditionalToken", 1, 512, ec)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        check_concurrency(app, &b)?;
        app.cloudwatch_logging_options.retain(|o| {
            o.get("CloudWatchLoggingOptionId").and_then(Value::as_str) != Some(opt_id.as_str())
        });
        bump_version(app);
        let op_id = record_operation(
            app,
            "UpdateApplication",
            app.version_updated_from,
            Some(app.version_id),
        );
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "CloudWatchLoggingOptionDescriptions": app.cloudwatch_logging_options,
            "OperationId": op_id,
        }))
    }

    fn add_vpc_configuration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, false, ec)?;
        opt_str_len(&b, "ConditionalToken", 1, 512, ec)?;
        let vpc = b
            .get("VpcConfiguration")
            .cloned()
            .ok_or_else(|| fault(ec, "VpcConfiguration is required."))?;
        // Snapshot EC2 subnet->vpc BEFORE the ka2 write lock (no nested locks).
        let vpc_map = self.ec2_subnet_vpc_map(&req.account_id);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        check_concurrency(app, &b)?;
        let id = app.next_id();
        let desc = vpc_desc(&vpc, &mut || id.clone());
        config_array_push(
            &mut app.config_description,
            "VpcConfigurationDescriptions",
            desc,
        );
        // Resolve the owning VpcId from EC2 state, then read the stored
        // description back so the response carries the resolved id.
        Self::resolve_vpc_ids(&mut app.config_description, &vpc_map);
        let stored = app
            .config_description
            .get("VpcConfigurationDescriptions")
            .and_then(Value::as_array)
            .and_then(|arr| {
                arr.iter()
                    .find(|d| {
                        d.get("VpcConfigurationId").and_then(Value::as_str) == Some(id.as_str())
                    })
                    .cloned()
            })
            .unwrap_or(Value::Null);
        bump_version(app);
        let op_id = record_operation(
            app,
            "UpdateApplication",
            app.version_updated_from,
            Some(app.version_id),
        );
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "VpcConfigurationDescription": stored,
            "OperationId": op_id,
        }))
    }

    fn delete_vpc_configuration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, false, ec)?;
        let vpc_id = req_str_len(&b, "VpcConfigurationId", 1, 50, ec)?;
        opt_str_len(&b, "ConditionalToken", 1, 512, ec)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        check_concurrency(app, &b)?;
        config_array_retain(
            &mut app.config_description,
            "VpcConfigurationDescriptions",
            "VpcConfigurationId",
            &vpc_id,
        );
        bump_version(app);
        let op_id = record_operation(
            app,
            "UpdateApplication",
            app.version_updated_from,
            Some(app.version_id),
        );
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "OperationId": op_id,
        }))
    }

    fn add_input(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, true, ec)?;
        let input = b
            .get("Input")
            .cloned()
            .ok_or_else(|| fault(ec, "Input is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        let id = app.next_id();
        let role = app.service_execution_role.clone();
        let desc = input_desc(&input, &mut || id.clone(), role.as_deref());
        sql_array_push(&mut app.config_description, "InputDescriptions", desc);
        bump_version(app);
        let descs = sql_array_get(&app.config_description, "InputDescriptions");
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "InputDescriptions": descs,
        }))
    }

    fn add_input_processing_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, true, ec)?;
        let input_id = req_str_len(&b, "InputId", 1, 50, ec)?;
        let ipc = b
            .get("InputProcessingConfiguration")
            .cloned()
            .ok_or_else(|| fault(ec, "InputProcessingConfiguration is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        let role = app.service_execution_role.clone();
        let ipcd = ipc
            .get("InputLambdaProcessor")
            .map(|lp| json!({ "InputLambdaProcessorDescription": resource_role_desc(lp, role.as_deref()) }))
            .unwrap_or_else(|| json!({}));
        let mut found = false;
        if let Some(list) = sql_array_mut(&mut app.config_description, "InputDescriptions") {
            for i in list.iter_mut() {
                if i.get("InputId").and_then(Value::as_str) == Some(input_id.as_str()) {
                    i.as_object_mut().unwrap().insert(
                        "InputProcessingConfigurationDescription".into(),
                        ipcd.clone(),
                    );
                    found = true;
                }
            }
        }
        if !found {
            return Err(fault(
                "ResourceNotFoundException",
                &format!("Input {input_id} not found."),
            ));
        }
        bump_version(app);
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "InputId": input_id,
            "InputProcessingConfigurationDescription": ipcd,
        }))
    }

    fn delete_input_processing_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, true, ec)?;
        let input_id = req_str_len(&b, "InputId", 1, 50, ec)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        if let Some(list) = sql_array_mut(&mut app.config_description, "InputDescriptions") {
            for i in list.iter_mut() {
                if i.get("InputId").and_then(Value::as_str) == Some(input_id.as_str()) {
                    i.as_object_mut()
                        .unwrap()
                        .remove("InputProcessingConfigurationDescription");
                }
            }
        }
        bump_version(app);
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
        }))
    }

    fn add_output(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, true, ec)?;
        let output = b
            .get("Output")
            .cloned()
            .ok_or_else(|| fault(ec, "Output is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        crate::builders::insert_output(st, &name, &output).map_err(|_| not_found(&name))?;
        let app = st.applications.get(&name).expect("just updated");
        let descs = sql_array_get(&app.config_description, "OutputDescriptions");
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "OutputDescriptions": descs,
        }))
    }

    fn delete_output(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, true, ec)?;
        let output_id = req_str_len(&b, "OutputId", 1, 50, ec)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        sql_array_retain(
            &mut app.config_description,
            "OutputDescriptions",
            "OutputId",
            &output_id,
        );
        bump_version(app);
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
        }))
    }

    fn add_reference_data_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, true, ec)?;
        let rds = b
            .get("ReferenceDataSource")
            .cloned()
            .ok_or_else(|| fault(ec, "ReferenceDataSource is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        crate::builders::insert_reference_data_source(st, &name, &rds)
            .map_err(|_| not_found(&name))?;
        let app = st.applications.get(&name).expect("just updated");
        let descs = sql_array_get(&app.config_description, "ReferenceDataSourceDescriptions");
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
            "ReferenceDataSourceDescriptions": descs,
        }))
    }

    fn delete_reference_data_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        int_range(&b, "CurrentApplicationVersionId", 1, 999_999_999, true, ec)?;
        let ref_id = req_str_len(&b, "ReferenceId", 1, 50, ec)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        sql_array_retain(
            &mut app.config_description,
            "ReferenceDataSourceDescriptions",
            "ReferenceId",
            &ref_id,
        );
        bump_version(app);
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationVersionId": app.version_id,
        }))
    }

    fn discover_input_schema(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        opt_str_len(&b, "ResourceARN", 1, 2048, ec)?;
        req_str_len(&b, "ServiceExecutionRole", 1, 2048, ec)?;
        // Infer a plausible single-column JSON schema, echoing any sample rows
        // provided via `S3Configuration`/streaming source as raw records.
        let raw: Vec<Value> = vec![json!("{\"COL1\":\"value\"}")];
        ok(json!({
            "InputSchema": {
                "RecordFormat": {
                    "RecordFormatType": "JSON",
                    "MappingParameters": {
                        "JSONMappingParameters": { "RecordRowPath": "$" }
                    }
                },
                "RecordEncoding": "UTF-8",
                "RecordColumns": [
                    { "Name": "COL1", "SqlType": "VARCHAR(16)", "Mapping": "$.COL1" }
                ]
            },
            "RawInputRecords": raw,
            "ParsedInputRecords": [["value"]],
            "ProcessedInputRecords": ["{\"COL1\":\"value\"}"],
        }))
    }

    fn create_application_presigned_url(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        enum_field(&b, "UrlType", URL_TYPES, true, ec)?;
        int_range(
            &b,
            "SessionExpirationDurationInSeconds",
            1800,
            43200,
            false,
            ec,
        )?;
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(&name))
            .ok_or_else(|| not_found(&name))?;
        // When a REAL Flink cluster is running for this app, return its actually
        // reachable dashboard/REST base URL so a client (or the E2E) can hit the
        // live JobManager. Otherwise fall back to the synthesized AWS-shaped URL.
        let url = match &app.flink_binding {
            Some(bind) if app.status == "RUNNING" => bind.dashboard_url(),
            _ => {
                let token = new_token();
                format!(
                    "https://{}.kinesisanalytics.{}.amazonaws.com/dashboard?authToken={token}",
                    name, req.region
                )
            }
        };
        ok(json!({ "AuthorizedUrl": url }))
    }

    fn update_maintenance_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let name = req_str_len(&b, "ApplicationName", 1, 128, ec)?;
        let update = b
            .get("ApplicationMaintenanceConfigurationUpdate")
            .cloned()
            .ok_or_else(|| fault(ec, "ApplicationMaintenanceConfigurationUpdate is required."))?;
        let start = update
            .get("ApplicationMaintenanceWindowStartTimeUpdate")
            .and_then(Value::as_str);
        if let Some(s) = start {
            check_len(s, "ApplicationMaintenanceWindowStartTimeUpdate", 5, 5, ec)?;
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(&name)
            .ok_or_else(|| not_found(&name))?;
        if let Some(s) = start {
            app.maintenance_start = s.to_string();
            // AWS derives the 8-hour window end from the start time.
            app.maintenance_end = derive_window_end(s);
        }
        ok(json!({
            "ApplicationARN": app.arn,
            "ApplicationMaintenanceConfigurationDescription": {
                "ApplicationMaintenanceWindowStartTime": app.maintenance_start,
                "ApplicationMaintenanceWindowEndTime": app.maintenance_end,
            }
        }))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let resource_arn = req_str_len(&b, "ResourceARN", 1, 2048, ec)?;
        let new_tags = parse_tags(&b);
        if new_tags.is_empty() {
            return Err(fault(ec, "Tags is required."));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !application_arn_exists(st, &resource_arn) {
            return Err(fault(
                "ResourceNotFoundException",
                &format!("Resource {resource_arn} not found."),
            ));
        }
        let entry = st.tags.entry(resource_arn).or_default();
        for (k, v) in new_tags {
            entry.insert(k, v);
        }
        if entry.len() > 200 {
            return Err(fault(
                "TooManyTagsException",
                "A resource can have at most 200 tags.",
            ));
        }
        ok_empty()
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let resource_arn = req_str_len(&b, "ResourceARN", 1, 2048, ec)?;
        let keys: Vec<String> = b
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|k| k.as_str().map(str::to_string))
                    .collect()
            })
            .ok_or_else(|| fault(ec, "TagKeys is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !application_arn_exists(st, &resource_arn) {
            return Err(fault(
                "ResourceNotFoundException",
                &format!("Resource {resource_arn} not found."),
            ));
        }
        if let Some(entry) = st.tags.get_mut(&resource_arn) {
            for k in keys {
                entry.remove(&k);
            }
        }
        ok_empty()
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let ec = "InvalidArgumentException";
        let resource_arn = req_str_len(&b, "ResourceARN", 1, 2048, ec)?;
        let accounts = self.state.read();
        let st = accounts
            .get(&req.account_id)
            .ok_or_else(|| fault("ResourceNotFoundException", "Resource not found."))?;
        if !application_arn_exists(st, &resource_arn) {
            return Err(fault(
                "ResourceNotFoundException",
                &format!("Resource {resource_arn} not found."),
            ));
        }
        let tags = st.tags.get(&resource_arn).cloned().unwrap_or_default();
        ok(json!({ "Tags": tags_json(&tags) }))
    }
}

// ===== real Flink data-plane background tasks =====

/// Bounded poll for a job to reach RUNNING (or a terminal state) after
/// submission: 120 * 500ms = ~60s.
const JOB_POLL_ATTEMPTS: u32 = 120;
/// Bounded poll for a canceled job to reach a terminal state: ~30s.
const CANCEL_POLL_ATTEMPTS: u32 = 60;
const JOB_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Statuses in which an application is (or is about to be) running a job: an
/// update or rollback in one of these redeploys the real job.
const ACTIVE_STATUSES: &[&str] = &[
    "STARTING",
    "RUNNING",
    "UPDATING",
    "ROLLING_BACK",
    "AUTOSCALING",
];

/// Per-application lifecycle coordination for the real Flink data plane.
///
/// * `epoch` is bumped (under the ka2 state write lock) by every lifecycle
///   operation -- Start, Stop, Update/Rollback that redeploys, Delete, restart
///   recovery. A background task captures the epoch it was spawned for and only
///   applies a status/binding write while that epoch is still current, so a
///   superseded bring-up can never overwrite a newer Stop/Delete (or resurrect a
///   deleted application).
/// * `lock` serializes every container operation for an application ARN, so a
///   teardown always runs after a superseded bring-up has reached a checkpoint
///   and a same-name re-create queues behind the old application's teardown.
#[derive(Default)]
struct LifecycleSlot {
    epoch: AtomicU64,
    lock: AsyncMutex<()>,
}

#[derive(Clone, Default)]
struct Lifecycles(Arc<parking_lot::Mutex<HashMap<String, Arc<LifecycleSlot>>>>);

impl Lifecycles {
    fn slot(&self, arn: &str) -> Arc<LifecycleSlot> {
        self.0.lock().entry(arn.to_string()).or_default().clone()
    }

    /// Supersede every in-flight task for `arn`; returns the new epoch.
    fn bump(&self, arn: &str) -> u64 {
        self.slot(arn).epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn current(&self, arn: &str) -> u64 {
        self.slot(arn).epoch.load(Ordering::SeqCst)
    }
}

/// What to submit for a real Flink application: the code JAR location plus the
/// job parameters, captured from the configuration the deploy is for.
#[derive(Debug, Clone)]
struct DeploySpec {
    bucket: String,
    key: String,
    parallelism: Option<i64>,
    program_args: Option<String>,
}

impl DeploySpec {
    fn of(app: &Application) -> Option<Self> {
        let (bucket, key) = flink_code_location(app)?;
        Some(Self {
            bucket,
            key,
            parallelism: flink_parallelism(app),
            program_args: flink_program_args(app),
        })
    }
}

/// Everything a background lifecycle task needs, bound to the application and
/// the epoch it was spawned for.
#[derive(Clone)]
struct TaskCtx {
    state: SharedKa2State,
    store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    backend: Arc<dyn FlinkBackend>,
    s3: Option<Arc<dyn S3Delivery>>,
    lifecycles: Lifecycles,
    account: String,
    name: String,
    arn: String,
    epoch: u64,
}

fn running_of(b: &FlinkBinding) -> RunningFlink {
    RunningFlink {
        container_id: b.container_id.clone(),
        host: b.host.clone(),
        rest_port: b.rest_port,
    }
}

enum Ready {
    Yes,
    Superseded,
    TimedOut(String),
}

impl TaskCtx {
    fn is_current(&self) -> bool {
        self.lifecycles.current(&self.arn) == self.epoch
    }

    /// Apply `f` to the application only while this task's epoch is current
    /// (checked under the state write lock, the same lock the handlers bump the
    /// epoch under) and the application with this ARN still exists.
    fn mutate_if_current<F: FnOnce(&mut Application)>(&self, f: F) -> bool {
        let mut accounts = self.state.write();
        if !self.is_current() {
            return false;
        }
        let st = accounts.get_or_create(&self.account);
        match st.applications.get_mut(&self.name) {
            Some(app) if app.arn == self.arn => {
                f(app);
                true
            }
            _ => false,
        }
    }

    /// The application's current binding (regardless of epoch).
    fn binding(&self) -> Option<FlinkBinding> {
        self.state
            .read()
            .get(&self.account)
            .and_then(|st| st.applications.get(&self.name))
            .filter(|app| app.arn == self.arn)
            .and_then(|app| app.flink_binding.clone())
    }

    async fn save(&self) {
        save_snapshot(&self.state, self.store.clone(), &self.snapshot_lock).await;
    }

    /// Settle to `status`, replacing the binding when `binding` is `Some`.
    async fn settle(&self, status: &str, binding: Option<Option<FlinkBinding>>) -> bool {
        let applied = self.mutate_if_current(|app| {
            app.status = status.to_string();
            if let Some(b) = binding {
                app.flink_binding = b;
            }
        });
        if applied {
            self.save().await;
        }
        applied
    }

    /// A bring-up/redeploy failed: tear the cluster down and settle READY. When
    /// the task has been superseded the successor owns the cluster, so leave it.
    async fn fail(&self, container_id: Option<&str>) {
        if !self.is_current() {
            return;
        }
        self.backend.remove_cluster(&self.arn, container_id).await;
        self.settle("READY", Some(None)).await;
    }

    /// Poll the cluster until it can take a job, abandoning the wait as soon as
    /// the task is superseded (stop/delete during STARTING).
    async fn wait_ready(&self, running: &RunningFlink) -> Ready {
        let started = std::time::Instant::now();
        for _ in 0..READY_ATTEMPTS {
            if !self.is_current() {
                return Ready::Superseded;
            }
            if self.backend.cluster_ready(running).await {
                return Ready::Yes;
            }
            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
        Ready::TimedOut(
            self.backend
                .readiness_failure(&running.container_id, started)
                .await,
        )
    }

    /// Cancel a job and wait (bounded) for it to reach a terminal state.
    async fn cancel_and_wait(&self, running: &RunningFlink, job_id: &str) {
        if let Err(error) = self.backend.cancel_job(running, job_id).await {
            tracing::warn!(%error, app_arn = %self.arn, job_id, "cancel_job failed");
        }
        for _ in 0..CANCEL_POLL_ATTEMPTS {
            match self.backend.job_state(running, job_id).await {
                Ok(s) if is_terminal_flink_state(&s) => break,
                Err(_) => break,
                _ => tokio::time::sleep(JOB_POLL_INTERVAL).await,
            }
        }
    }

    /// Bring the application's job up with `spec`: reuse the tracked cluster or
    /// create one, cancel any previous job (update/rollback redeploy), submit
    /// the JAR, and settle RUNNING once Flink reports the job RUNNING. The caller
    /// holds the application's lifecycle lock. Every step re-checks the epoch: a
    /// superseded task stops touching state and leaves the tracked cluster to
    /// its successor (a job it already submitted is canceled first so the
    /// successor never inherits an untracked job).
    async fn deploy_locked(&self, spec: DeploySpec) {
        if !self.is_current() {
            return;
        }
        // 1. Reuse the tracked cluster or bring up a new one.
        let running = match self.backend.tracked(&self.arn) {
            Some(r) => r,
            None => match self.backend.create_cluster(&self.arn).await {
                Ok(r) => r,
                Err(error) => {
                    tracing::error!(%error, app_arn = %self.arn, "failed to bring up Flink cluster; app back to READY");
                    self.fail(None).await;
                    return;
                }
            },
        };
        // The previous job only matters when it lives on this same cluster.
        let previous = self
            .binding()
            .filter(|b| b.container_id == running.container_id);
        let prev_job = previous.as_ref().and_then(|b| b.job_id.clone());
        // Persist the container binding immediately so a restart can re-attach
        // or clean it up (no RUNNING-with-dead-container, #1338).
        let bound = self.mutate_if_current(|app| {
            app.flink_binding = Some(FlinkBinding {
                container_id: running.container_id.clone(),
                host: running.host.clone(),
                rest_port: running.rest_port,
                jar_id: previous.as_ref().and_then(|b| b.jar_id.clone()),
                job_id: prev_job.clone(),
            });
        });
        if !bound {
            return;
        }
        self.save().await;

        // 2. Wait for a free task slot (cancellable).
        match self.wait_ready(&running).await {
            Ready::Yes => {}
            Ready::Superseded => return,
            Ready::TimedOut(reason) => {
                tracing::error!(app_arn = %self.arn, %reason, "Flink cluster never became ready; app back to READY");
                self.fail(Some(&running.container_id)).await;
                return;
            }
        }

        // 3. A redeploy replaces the running job.
        if let Some(job) = &prev_job {
            self.cancel_and_wait(&running, job).await;
            if !self.mutate_if_current(|app| {
                if let Some(b) = app.flink_binding.as_mut() {
                    b.job_id = None;
                }
            }) {
                return;
            }
        }

        // 4. Fetch the code JAR from fakecloud S3.
        let Some(s3) = self.s3.clone() else {
            tracing::error!(app_arn = %self.arn, "no S3 reader attached; cannot submit Flink job");
            self.fail(Some(&running.container_id)).await;
            return;
        };
        let jar_bytes = match s3.get_object(&self.account, &spec.bucket, &spec.key) {
            Ok(b) => b,
            Err(error) => {
                tracing::error!(%error, app_arn = %self.arn, bucket = %spec.bucket, key = %spec.key, "failed to read Flink code JAR from S3; app back to READY");
                self.fail(Some(&running.container_id)).await;
                return;
            }
        };
        if !self.is_current() {
            return;
        }
        let file_name = spec.key.rsplit('/').next().unwrap_or("application.jar");

        // 5. Upload + run the job.
        let jar_id = match self
            .backend
            .upload_jar(&running, jar_bytes, file_name)
            .await
        {
            Ok(id) => id,
            Err(error) => {
                tracing::error!(%error, app_arn = %self.arn, "Flink jar upload failed; app back to READY");
                self.fail(Some(&running.container_id)).await;
                return;
            }
        };
        if !self.is_current() {
            return;
        }
        let job_id = match self
            .backend
            .run_job(
                &running,
                &jar_id,
                spec.parallelism,
                spec.program_args.as_deref(),
                None,
            )
            .await
        {
            Ok(id) => id,
            Err(error) => {
                tracing::error!(%error, app_arn = %self.arn, "Flink job submission failed; app back to READY");
                self.fail(Some(&running.container_id)).await;
                return;
            }
        };
        let recorded = self.mutate_if_current(|app| {
            app.flink_binding = Some(FlinkBinding {
                container_id: running.container_id.clone(),
                host: running.host.clone(),
                rest_port: running.rest_port,
                jar_id: Some(jar_id.clone()),
                job_id: Some(job_id.clone()),
            });
        });
        if !recorded {
            // Superseded between submit and record: nobody else knows this job.
            let _ = self.backend.cancel_job(&running, &job_id).await;
            return;
        }
        self.save().await;

        // 6. Poll until the real Flink job is RUNNING (or terminal).
        for _ in 0..JOB_POLL_ATTEMPTS {
            if !self.is_current() {
                return;
            }
            match self.backend.job_state(&running, &job_id).await {
                Ok(s) if flink_state_to_app_status(&s) == "RUNNING" => {
                    if self.settle("RUNNING", None).await {
                        tracing::info!(app_arn = %self.arn, job_id = %job_id, "Flink job is RUNNING");
                    }
                    return;
                }
                Ok(s) if is_terminal_flink_state(&s) => {
                    tracing::error!(app_arn = %self.arn, job_id = %job_id, flink_state = %s, "Flink job reached a terminal state before RUNNING; app back to READY");
                    self.fail(Some(&running.container_id)).await;
                    return;
                }
                _ => tokio::time::sleep(JOB_POLL_INTERVAL).await,
            }
        }
        tracing::error!(app_arn = %self.arn, job_id = %job_id, "Flink job did not reach RUNNING within the deadline; app back to READY");
        self.fail(Some(&running.container_id)).await;
    }

    /// Cancel the job (if any) and remove every container of the application.
    /// The caller holds the lifecycle lock. Teardown always completes, even when
    /// superseded: a successor deploy simply brings up a fresh cluster.
    async fn teardown_locked(&self, binding: Option<FlinkBinding>) {
        if let Some(b) = &binding {
            if let Some(job_id) = &b.job_id {
                self.cancel_and_wait(&running_of(b), job_id).await;
            }
        }
        self.backend
            .remove_cluster(&self.arn, binding.as_ref().map(|b| b.container_id.as_str()))
            .await;
    }
}

impl Ka2Service {
    /// Whether a real container runtime is attached.
    pub fn runtime_present(&self) -> bool {
        self.runtime.is_some()
    }

    fn task_ctx(&self, account: &str, name: &str, arn: &str, epoch: u64) -> Option<TaskCtx> {
        Some(TaskCtx {
            state: self.state.clone(),
            store: self.snapshot_store.clone(),
            snapshot_lock: self.snapshot_lock.clone(),
            backend: self.runtime.clone()?,
            s3: self.s3.clone(),
            lifecycles: self.lifecycles.clone(),
            account: account.to_string(),
            name: name.to_string(),
            arn: arn.to_string(),
            epoch,
        })
    }

    /// Start / update / rollback: (re)deploy the application's job in the
    /// background. Fire-and-forget: never blocks the handler on a container
    /// pull/start/submit (issues #1539/#1730).
    fn spawn_flink_deploy(&self, ctx: TaskCtx, spec: DeploySpec) {
        tokio::spawn(async move {
            let slot = ctx.lifecycles.slot(&ctx.arn);
            let _guard = slot.lock.lock().await;
            ctx.deploy_locked(spec).await;
        });
    }

    /// Stop / delete: cancel the job, tear the cluster down, and (for a stop)
    /// settle the application to READY. Queues behind any in-flight bring-up,
    /// which abandons its work at its next checkpoint once superseded.
    fn spawn_flink_teardown(
        &self,
        ctx: TaskCtx,
        binding: Option<FlinkBinding>,
        settle_ready: bool,
    ) {
        tokio::spawn(async move {
            let slot = ctx.lifecycles.slot(&ctx.arn);
            let _guard = slot.lock.lock().await;
            // A bring-up that ran before us may have bound a newer container.
            let binding = ctx.binding().or(binding);
            ctx.teardown_locked(binding).await;
            if settle_ready && ctx.settle("READY", Some(None)).await {
                tracing::info!(app_arn = %ctx.arn, "Flink app stopped and cluster torn down");
            }
        });
    }

    /// Poll a RUNNING app's real job once; if it is no longer running, settle
    /// the app to READY. Only acts while the describe's epoch is current and the
    /// app is still RUNNING with the same job id.
    fn spawn_flink_reconcile(&self, ctx: TaskCtx, binding: FlinkBinding, job_id: String) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        tokio::spawn(async move {
            let terminal = match ctx.backend.job_state(&running_of(&binding), &job_id).await {
                Ok(s) => flink_state_to_app_status(&s) != "RUNNING",
                // Job vanished from the cluster -> no longer running.
                Err(crate::runtime::JobError::NotFound(_)) => true,
                // Transient REST hiccup: leave the app as-is.
                Err(_) => return,
            };
            if !terminal {
                return;
            }
            let mut changed = false;
            ctx.mutate_if_current(|app| {
                let same_job = app.flink_binding.as_ref().and_then(|b| b.job_id.as_deref())
                    == Some(job_id.as_str());
                if app.status == "RUNNING" && same_job {
                    app.status = "READY".to_string();
                    changed = true;
                }
            });
            if changed {
                ctx.save().await;
                tracing::info!(app = %ctx.name, "real Flink job is no longer RUNNING; app settled to READY");
            }
        });
    }

    /// Restart recovery for real Flink applications, mirroring the contract of
    /// MSK / MQ / RDS (no RUNNING-with-dead-container, #1338/#914). Every
    /// transitional state is resolved instead of being stranded:
    ///
    /// * `RUNNING` with a binding: re-attach the persisted container; if it (or
    ///   its job) is gone, or re-attach fails, remove the container BY ID (it is
    ///   not tracked yet) and settle READY.
    /// * `STARTING` / `UPDATING` / `ROLLING_BACK` / `AUTOSCALING`: the operation
    ///   was interrupted, so discard any half-built container and resume it with
    ///   a fresh deploy that settles RUNNING (or READY on failure).
    /// * `STOPPING` / `FORCE_STOPPING`: finish the stop -- remove the container
    ///   and settle READY.
    ///
    /// Fire-and-forget per app so a slow re-attach doesn't block startup.
    pub async fn recover_persisted_containers(&self) {
        if self.runtime.is_none() {
            return;
        }
        enum Plan {
            Reattach(FlinkBinding),
            Resume(Option<FlinkBinding>, DeploySpec),
            FinishStop(Option<FlinkBinding>),
        }
        let mut plans: Vec<(TaskCtx, Plan)> = Vec::new();
        {
            let accounts = self.state.write();
            for (account, st) in accounts.iter() {
                for (name, app) in &st.applications {
                    let binding = app.flink_binding.clone();
                    let plan = match app.status.as_str() {
                        "RUNNING" => binding.map(Plan::Reattach),
                        "STARTING" | "UPDATING" | "ROLLING_BACK" | "AUTOSCALING"
                            if is_real_flink(app) =>
                        {
                            DeploySpec::of(app).map(|spec| Plan::Resume(binding, spec))
                        }
                        "STOPPING" | "FORCE_STOPPING"
                            if is_real_flink(app) || binding.is_some() =>
                        {
                            Some(Plan::FinishStop(binding))
                        }
                        _ => None,
                    };
                    if let Some(plan) = plan {
                        let epoch = self.lifecycles.bump(&app.arn);
                        if let Some(ctx) = self.task_ctx(account, name, &app.arn, epoch) {
                            plans.push((ctx, plan));
                        }
                    }
                }
            }
        }
        for (ctx, plan) in plans {
            tokio::spawn(async move {
                let slot = ctx.lifecycles.slot(&ctx.arn);
                let _guard = slot.lock.lock().await;
                match plan {
                    Plan::Reattach(binding) => {
                        let reattached = ctx
                            .backend
                            .reattach_cluster(&ctx.arn, &binding.container_id)
                            .await;
                        let recovered = match reattached {
                            Ok(Some(running)) => match &binding.job_id {
                                Some(job_id) => match ctx.backend.job_state(&running, job_id).await
                                {
                                    Ok(s) if flink_state_to_app_status(&s) == "RUNNING" => {
                                        Some(running)
                                    }
                                    _ => None,
                                },
                                None => None,
                            },
                            Ok(None) => None,
                            Err(error) => {
                                tracing::warn!(%error, app = %ctx.name, "re-attaching the Flink container failed");
                                None
                            }
                        };
                        match recovered {
                            Some(running) => {
                                // Refresh host/port in case the published port moved.
                                ctx.settle(
                                    "RUNNING",
                                    Some(Some(FlinkBinding {
                                        container_id: running.container_id,
                                        host: running.host,
                                        rest_port: running.rest_port,
                                        jar_id: binding.jar_id.clone(),
                                        job_id: binding.job_id.clone(),
                                    })),
                                )
                                .await;
                                tracing::info!(app = %ctx.name, "recovered RUNNING Flink app after restart");
                            }
                            None => {
                                // The container may be untracked (reattach never
                                // completed): remove it by id, not by map entry.
                                ctx.backend
                                    .remove_cluster(&ctx.arn, Some(&binding.container_id))
                                    .await;
                                ctx.settle("READY", Some(None)).await;
                                tracing::warn!(app = %ctx.name, "could not recover Flink job after restart; app settled to READY");
                            }
                        }
                    }
                    Plan::Resume(binding, spec) => {
                        if let Some(b) = &binding {
                            ctx.backend
                                .remove_cluster(&ctx.arn, Some(&b.container_id))
                                .await;
                        }
                        if ctx.mutate_if_current(|app| app.flink_binding = None) {
                            tracing::info!(app = %ctx.name, "resuming interrupted Flink deploy after restart");
                            ctx.deploy_locked(spec).await;
                        }
                    }
                    Plan::FinishStop(binding) => {
                        if let Some(b) = &binding {
                            ctx.backend
                                .remove_cluster(&ctx.arn, Some(&b.container_id))
                                .await;
                        }
                        ctx.settle("READY", Some(None)).await;
                        tracing::info!(app = %ctx.name, "finished interrupted Flink stop after restart");
                    }
                }
            });
        }
    }
}

/// Whether an application is a REAL-runtime-managed Flink app: a Flink-flavor
/// runtime environment (`FLINK-1_x`, excluding SQL and Zeppelin) AND a code JAR
/// in S3 to submit. Only these get the Docker-backed Flink job; everything else
/// stays on the control-plane state machine.
fn is_real_flink(app: &Application) -> bool {
    app.runtime_environment.starts_with("FLINK-") && flink_code_location(app).is_some()
}

/// Extract the `(bucket, key)` of an application's code JAR from its
/// `ApplicationCodeConfigurationDescription`, or `None` when it has no S3 code
/// location. The `BucketARN` (`arn:aws:s3:::bucket`) is reduced to the name.
fn flink_code_location(app: &Application) -> Option<(String, String)> {
    let s3 = app
        .config_description
        .get("ApplicationCodeConfigurationDescription")?
        .get("CodeContentDescription")?
        .get("S3ApplicationCodeLocationDescription")?;
    let bucket_arn = s3.get("BucketARN").and_then(Value::as_str)?;
    let key = s3.get("FileKey").and_then(Value::as_str)?;
    let bucket = bucket_arn.rsplit(":::").next().unwrap_or(bucket_arn);
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((bucket.to_string(), key.to_string()))
}

/// Read the configured job parallelism from a Flink app's config description.
fn flink_parallelism(app: &Application) -> Option<i64> {
    app.config_description
        .get("FlinkApplicationConfigurationDescription")?
        .get("ParallelismConfigurationDescription")?
        .get("Parallelism")
        .and_then(Value::as_i64)
}

/// Derive program arguments from the app's environment property groups. KDA has
/// no first-class "program args" field; a `PropertyGroupId` of
/// `kinesis.analytics.flink.run.options` with a `ProgramArgs` key is the
/// convention we honor, joining nothing else.
fn flink_program_args(app: &Application) -> Option<String> {
    let groups = app
        .config_description
        .get("EnvironmentPropertyDescriptions")?
        .get("PropertyGroupDescriptions")?
        .as_array()?;
    for g in groups {
        if g.get("PropertyGroupId").and_then(Value::as_str)
            == Some("kinesis.analytics.flink.run.options")
        {
            if let Some(args) = g
                .get("PropertyMap")
                .and_then(|m| m.get("ProgramArgs"))
                .and_then(Value::as_str)
            {
                return Some(args.to_string());
            }
        }
    }
    None
}

// ===== config-description manipulation helpers =====

fn ensure_object(v: &mut Value) -> &mut serde_json::Map<String, Value> {
    if !v.is_object() {
        *v = Value::Object(serde_json::Map::new());
    }
    v.as_object_mut().unwrap()
}

/// Push an element into a top-level array member of the config description.
fn config_array_push(cfg: &mut Value, key: &str, item: Value) {
    let obj = ensure_object(cfg);
    let arr = obj.entry(key).or_insert_with(|| json!([]));
    if let Some(a) = arr.as_array_mut() {
        a.push(item);
    }
}

fn config_array_retain(cfg: &mut Value, key: &str, id_field: &str, id: &str) {
    if let Some(arr) = cfg.get_mut(key).and_then(Value::as_array_mut) {
        arr.retain(|e| e.get(id_field).and_then(Value::as_str) != Some(id));
    }
}

/// Push into `SqlApplicationConfigurationDescription.<key>`, creating the
/// nested structure as needed.
pub(crate) fn sql_array_push(cfg: &mut Value, key: &str, item: Value) {
    let obj = ensure_object(cfg);
    let sql = obj
        .entry("SqlApplicationConfigurationDescription")
        .or_insert_with(|| json!({}));
    let sql_obj = ensure_object(sql);
    let arr = sql_obj.entry(key).or_insert_with(|| json!([]));
    if let Some(a) = arr.as_array_mut() {
        a.push(item);
    }
}

pub(crate) fn sql_array_retain(cfg: &mut Value, key: &str, id_field: &str, id: &str) {
    if let Some(arr) = cfg
        .get_mut("SqlApplicationConfigurationDescription")
        .and_then(|s| s.get_mut(key))
        .and_then(Value::as_array_mut)
    {
        arr.retain(|e| e.get(id_field).and_then(Value::as_str) != Some(id));
    }
    collapse_empty_sql_config(cfg);
}

/// Drop the whole `SqlApplicationConfigurationDescription` once its inputs,
/// outputs and reference data sources are all gone. AWS reports no
/// `SqlApplicationConfiguration` (rather than an empty one) after the last SQL
/// resource is removed, so terraform reads `sql_application_configuration.# = 0`.
fn collapse_empty_sql_config(cfg: &mut Value) {
    let Some(obj) = cfg.as_object_mut() else {
        return;
    };
    let empty = obj
        .get("SqlApplicationConfigurationDescription")
        .map(|sql| {
            [
                "InputDescriptions",
                "OutputDescriptions",
                "ReferenceDataSourceDescriptions",
            ]
            .iter()
            .all(|k| {
                sql.get(k)
                    .and_then(Value::as_array)
                    .map(|a| a.is_empty())
                    .unwrap_or(true)
            })
        })
        .unwrap_or(false);
    if empty {
        obj.remove("SqlApplicationConfigurationDescription");
    }
}

fn sql_array_mut<'a>(cfg: &'a mut Value, key: &str) -> Option<&'a mut Vec<Value>> {
    cfg.get_mut("SqlApplicationConfigurationDescription")
        .and_then(|s| s.get_mut(key))
        .and_then(Value::as_array_mut)
}

fn sql_array_get(cfg: &Value, key: &str) -> Value {
    cfg.get("SqlApplicationConfigurationDescription")
        .and_then(|s| s.get(key))
        .cloned()
        .unwrap_or_else(|| json!([]))
}

/// Apply an `ApplicationConfigurationUpdate` to an application's stored
/// `ApplicationConfigurationDescription` in place. Each member present in the
/// update (which carries `*Update`-suffixed shapes) rebuilds the corresponding
/// description member; members the update omits are preserved. This mirrors how
/// AWS re-derives the description after `UpdateApplication`, including
/// re-defaulting a Flink sub-config whose `ConfigurationType` reverted to
/// `DEFAULT`.
fn apply_config_update(app: &mut Application, update: &Value) {
    let is_flink = app.runtime_environment.starts_with("FLINK-");

    if let Some(code) = update.get("ApplicationCodeConfigurationUpdate") {
        let existing = app
            .config_description
            .get("ApplicationCodeConfigurationDescription")
            .cloned();
        let desc = code_config_update_description(code, existing.as_ref());
        ensure_object(&mut app.config_description)
            .insert("ApplicationCodeConfigurationDescription".into(), desc);
    }

    if is_flink {
        if let Some(flink) = update.get("FlinkApplicationConfigurationUpdate") {
            let existing = app
                .config_description
                .get("FlinkApplicationConfigurationDescription")
                .cloned();
            let desc = flink_config_update_description(flink, existing.as_ref());
            ensure_object(&mut app.config_description)
                .insert("FlinkApplicationConfigurationDescription".into(), desc);
        }
    } else if let Some(sql) = update.get("SqlApplicationConfigurationUpdate") {
        let role = app.service_execution_role.clone();
        apply_sql_config_update(&mut app.config_description, role.as_deref(), sql);
    }

    if let Some(snap) = update.get("ApplicationSnapshotConfigurationUpdate") {
        if let Some(enabled) = snap.get("SnapshotsEnabledUpdate").and_then(Value::as_bool) {
            ensure_object(&mut app.config_description).insert(
                "ApplicationSnapshotConfigurationDescription".into(),
                json!({ "SnapshotsEnabled": enabled }),
            );
        }
    }

    if let Some(env) = update.get("EnvironmentPropertyUpdates") {
        // An empty PropertyGroups list removes all groups.
        let groups = env.get("PropertyGroups").and_then(Value::as_array);
        match groups {
            Some(pg) if !pg.is_empty() => {
                ensure_object(&mut app.config_description).insert(
                    "EnvironmentPropertyDescriptions".into(),
                    json!({ "PropertyGroupDescriptions": pg.clone() }),
                );
            }
            Some(_) => {
                ensure_object(&mut app.config_description)
                    .remove("EnvironmentPropertyDescriptions");
            }
            None => {}
        }
    }

    if let Some(vpcs) = update
        .get("VpcConfigurationUpdates")
        .and_then(Value::as_array)
    {
        for vu in vpcs {
            let id = str_of(vu, "VpcConfigurationIdUpdate");
            if let Some(arr) = app
                .config_description
                .get_mut("VpcConfigurationDescriptions")
                .and_then(Value::as_array_mut)
            {
                for d in arr.iter_mut() {
                    let matches = id.is_none()
                        || d.get("VpcConfigurationId").and_then(Value::as_str) == id.as_deref();
                    if !matches {
                        continue;
                    }
                    if let Some(s) = vu.get("SubnetIdUpdates") {
                        d.as_object_mut()
                            .unwrap()
                            .insert("SubnetIds".into(), s.clone());
                    }
                    if let Some(s) = vu.get("SecurityGroupIdUpdates") {
                        d.as_object_mut()
                            .unwrap()
                            .insert("SecurityGroupIds".into(), s.clone());
                    }
                }
            }
        }
    }
}

/// Map a `*Update`-suffixed input object to its base description-shaped object
/// (e.g. `ConfigurationTypeUpdate` -> `ConfigurationType`). Values are cloned.
fn strip_update_suffix(update: &Value) -> Value {
    let mut m = serde_json::Map::new();
    if let Some(obj) = update.as_object() {
        for (k, v) in obj {
            let base = k.strip_suffix("Update").unwrap_or(k);
            m.insert(base.to_string(), v.clone());
        }
    }
    Value::Object(m)
}

/// Rebuild a `FlinkApplicationConfigurationDescription` from a
/// `FlinkApplicationConfigurationUpdate`, preserving any sub-description the
/// update does not touch.
fn flink_config_update_description(flink_update: &Value, existing: Option<&Value>) -> Value {
    let mut out = existing
        .and_then(|e| e.as_object())
        .cloned()
        .unwrap_or_default();
    if let Some(c) = flink_update.get("CheckpointConfigurationUpdate") {
        out.insert(
            "CheckpointConfigurationDescription".into(),
            checkpoint_description(Some(&strip_update_suffix(c))),
        );
    }
    if let Some(m) = flink_update.get("MonitoringConfigurationUpdate") {
        out.insert(
            "MonitoringConfigurationDescription".into(),
            monitoring_description(Some(&strip_update_suffix(m))),
        );
    }
    if let Some(p) = flink_update.get("ParallelismConfigurationUpdate") {
        out.insert(
            "ParallelismConfigurationDescription".into(),
            parallelism_description(Some(&strip_update_suffix(p))),
        );
    }
    Value::Object(out)
}

/// Rebuild an `ApplicationCodeConfigurationDescription` from an
/// `ApplicationCodeConfigurationUpdate`, preserving fields the update omits.
fn code_config_update_description(code_update: &Value, existing: Option<&Value>) -> Value {
    let mut out = existing
        .and_then(|e| e.as_object())
        .cloned()
        .unwrap_or_default();
    if let Some(t) = str_of(code_update, "CodeContentTypeUpdate") {
        out.insert("CodeContentType".into(), json!(t));
    }
    out.entry("CodeContentType")
        .or_insert_with(|| json!("ZIPFILE"));
    if let Some(cc) = code_update.get("CodeContentUpdate") {
        let mut ccd = out
            .get("CodeContentDescription")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        if let Some(t) = str_of(cc, "TextContentUpdate") {
            ccd.insert("TextContent".into(), json!(t));
        }
        if let Some(s3u) = cc.get("S3ContentLocationUpdate") {
            ccd.insert(
                "S3ApplicationCodeLocationDescription".into(),
                s3_code_location(&strip_update_suffix(s3u)),
            );
        }
        out.insert("CodeContentDescription".into(), Value::Object(ccd));
    }
    Value::Object(out)
}

/// Build the `RunConfigurationDescription` AWS records on a started Flink app,
/// from an optional input `RunConfiguration` / `RunConfigurationUpdate` (both
/// carry an `ApplicationRestoreConfiguration` + `FlinkRunConfiguration`). Unset
/// fields default to AWS's documented values: `RESTORE_FROM_LATEST_SNAPSHOT`
/// and `AllowNonRestoredState=false`.
fn run_config_description(rc: Option<&Value>) -> Value {
    let restore = rc.and_then(|r| r.get("ApplicationRestoreConfiguration"));
    let restore_type = restore
        .and_then(|v| str_of(v, "ApplicationRestoreType"))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "RESTORE_FROM_LATEST_SNAPSHOT".to_string());
    let mut restore_desc = serde_json::Map::new();
    restore_desc.insert("ApplicationRestoreType".into(), json!(restore_type));
    if let Some(sn) = restore.and_then(|v| str_of(v, "SnapshotName")) {
        restore_desc.insert("SnapshotName".into(), json!(sn));
    }
    let allow = rc
        .and_then(|r| r.get("FlinkRunConfiguration"))
        .and_then(|v| v.get("AllowNonRestoredState"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    json!({
        "ApplicationRestoreConfigurationDescription": Value::Object(restore_desc),
        "FlinkRunConfigurationDescription": { "AllowNonRestoredState": allow },
    })
}

/// Build a `{ ResourceARN, RoleARN }` description from a bare resource ARN and
/// the application's service-execution role, used when an `*Update` shape
/// carries only a `ResourceARNUpdate`.
fn arn_role_desc(arn: &str, role: Option<&str>) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("ResourceARN".into(), json!(arn));
    if let Some(r) = role {
        m.insert("RoleARN".into(), json!(r));
    }
    Value::Object(m)
}

/// Apply a `SqlApplicationConfigurationUpdate` (sent by `UpdateApplication` for
/// a SQL app) in place: input / output / reference-data-source updates each
/// match an existing description by its minted id and revise it, mirroring how
/// AWS re-derives the `SqlApplicationConfigurationDescription`.
fn apply_sql_config_update(cfg: &mut Value, role: Option<&str>, sql_update: &Value) {
    if let Some(updates) = sql_update.get("InputUpdates").and_then(Value::as_array) {
        for iu in updates {
            apply_input_update(cfg, role, iu);
        }
    }
    if let Some(updates) = sql_update.get("OutputUpdates").and_then(Value::as_array) {
        for ou in updates {
            apply_output_update(cfg, role, ou);
        }
    }
    if let Some(updates) = sql_update
        .get("ReferenceDataSourceUpdates")
        .and_then(Value::as_array)
    {
        for ru in updates {
            apply_reference_update(cfg, ru);
        }
    }
}

/// Revise an existing `InputDescription` from an `InputUpdate` matched by
/// `InputId`. Regenerates `InAppStreamNames` whenever the name prefix or
/// parallelism count changes, and switches the input source type (a Kinesis
/// streams / Firehose source is mutually exclusive).
fn apply_input_update(cfg: &mut Value, role: Option<&str>, iu: &Value) {
    let input_id = str_of(iu, "InputId");
    let Some(list) = sql_array_mut(cfg, "InputDescriptions") else {
        return;
    };
    for i in list.iter_mut() {
        let matches =
            input_id.is_none() || i.get("InputId").and_then(Value::as_str) == input_id.as_deref();
        if !matches {
            continue;
        }
        let Some(o) = i.as_object_mut() else { continue };
        if let Some(np) = str_of(iu, "NamePrefixUpdate") {
            o.insert("NamePrefix".into(), json!(np));
        }
        if let Some(c) = iu
            .get("InputParallelismUpdate")
            .and_then(|p| p.get("CountUpdate"))
            .and_then(Value::as_i64)
        {
            o.insert("InputParallelism".into(), json!({ "Count": c }));
        }
        // Re-mint the in-app stream names from the (possibly updated) prefix and
        // count so their number tracks the parallelism.
        let count = o
            .get("InputParallelism")
            .and_then(|p| p.get("Count"))
            .and_then(Value::as_i64)
            .unwrap_or(1);
        if let Some(np) = o
            .get("NamePrefix")
            .and_then(Value::as_str)
            .map(String::from)
        {
            o.insert(
                "InAppStreamNames".into(),
                json!(in_app_stream_names(&np, count)),
            );
        }
        if let Some(su) = iu.get("InputSchemaUpdate") {
            let mut schema = o
                .get("InputSchema")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            if let Some(rf) = su.get("RecordFormatUpdate") {
                schema.insert("RecordFormat".into(), rf.clone());
            }
            if let Some(re) = su.get("RecordEncodingUpdate") {
                schema.insert("RecordEncoding".into(), re.clone());
            }
            if let Some(rc) = su.get("RecordColumnUpdates") {
                schema.insert("RecordColumns".into(), rc.clone());
            }
            o.insert("InputSchema".into(), Value::Object(schema));
        }
        if let Some(k) = iu.get("KinesisStreamsInputUpdate") {
            let arn = str_of(k, "ResourceARNUpdate").unwrap_or_default();
            o.insert(
                "KinesisStreamsInputDescription".into(),
                arn_role_desc(&arn, role),
            );
            o.remove("KinesisFirehoseInputDescription");
        }
        if let Some(k) = iu.get("KinesisFirehoseInputUpdate") {
            let arn = str_of(k, "ResourceARNUpdate").unwrap_or_default();
            o.insert(
                "KinesisFirehoseInputDescription".into(),
                arn_role_desc(&arn, role),
            );
            o.remove("KinesisStreamsInputDescription");
        }
        if let Some(ipc) = iu.get("InputProcessingConfigurationUpdate") {
            if let Some(lp) = ipc.get("InputLambdaProcessorUpdate") {
                let arn = str_of(lp, "ResourceARNUpdate").unwrap_or_default();
                o.insert(
                    "InputProcessingConfigurationDescription".into(),
                    json!({ "InputLambdaProcessorDescription": arn_role_desc(&arn, role) }),
                );
            }
        }
    }
}

/// Revise an existing `OutputDescription` from an `OutputUpdate` matched by
/// `OutputId`, switching the (mutually exclusive) destination type as needed.
fn apply_output_update(cfg: &mut Value, role: Option<&str>, ou: &Value) {
    let output_id = str_of(ou, "OutputId");
    let Some(list) = sql_array_mut(cfg, "OutputDescriptions") else {
        return;
    };
    for out in list.iter_mut() {
        let matches = output_id.is_none()
            || out.get("OutputId").and_then(Value::as_str) == output_id.as_deref();
        if !matches {
            continue;
        }
        let Some(o) = out.as_object_mut() else {
            continue;
        };
        if let Some(n) = str_of(ou, "NameUpdate") {
            o.insert("Name".into(), json!(n));
        }
        if let Some(ds) = ou.get("DestinationSchemaUpdate") {
            o.insert("DestinationSchema".into(), ds.clone());
        }
        let sink_keys = [
            (
                "KinesisStreamsOutputUpdate",
                "KinesisStreamsOutputDescription",
            ),
            (
                "KinesisFirehoseOutputUpdate",
                "KinesisFirehoseOutputDescription",
            ),
            ("LambdaOutputUpdate", "LambdaOutputDescription"),
        ];
        for (update_key, desc_key) in sink_keys {
            if let Some(k) = ou.get(update_key) {
                let arn = str_of(k, "ResourceARNUpdate").unwrap_or_default();
                for (_, dk) in sink_keys {
                    o.remove(dk);
                }
                o.insert(desc_key.into(), arn_role_desc(&arn, role));
                break;
            }
        }
    }
}

/// Revise an existing `ReferenceDataSourceDescription` from a
/// `ReferenceDataSourceUpdate` matched by `ReferenceId`.
fn apply_reference_update(cfg: &mut Value, ru: &Value) {
    let ref_id = str_of(ru, "ReferenceId");
    let Some(list) = sql_array_mut(cfg, "ReferenceDataSourceDescriptions") else {
        return;
    };
    for r in list.iter_mut() {
        let matches =
            ref_id.is_none() || r.get("ReferenceId").and_then(Value::as_str) == ref_id.as_deref();
        if !matches {
            continue;
        }
        let Some(o) = r.as_object_mut() else { continue };
        if let Some(tn) = str_of(ru, "TableNameUpdate") {
            o.insert("TableName".into(), json!(tn));
        }
        if let Some(s3u) = ru.get("S3ReferenceDataSourceUpdate") {
            let mut s3 = o
                .get("S3ReferenceDataSourceDescription")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            if let Some(b) = str_of(s3u, "BucketARNUpdate") {
                s3.insert("BucketARN".into(), json!(b));
            }
            if let Some(f) = str_of(s3u, "FileKeyUpdate") {
                s3.insert("FileKey".into(), json!(f));
            }
            o.insert("S3ReferenceDataSourceDescription".into(), Value::Object(s3));
        }
        if let Some(rs) = ru.get("ReferenceSchemaUpdate") {
            o.insert("ReferenceSchema".into(), rs.clone());
        }
    }
}

/// Apply a SQL app's `RunConfiguration.SqlRunConfigurations` (supplied to
/// `StartApplication`) by pinning each targeted input's
/// `InputStartingPositionConfiguration.InputStartingPosition`.
fn apply_sql_run_configuration(app: &mut Application, rc: &Value) {
    let Some(runs) = rc.get("SqlRunConfigurations").and_then(Value::as_array) else {
        return;
    };
    for run in runs {
        let input_id = str_of(run, "InputId");
        let Some(pos) = run
            .get("InputStartingPositionConfiguration")
            .and_then(|c| str_of(c, "InputStartingPosition"))
        else {
            continue;
        };
        if let Some(list) = sql_array_mut(&mut app.config_description, "InputDescriptions") {
            for i in list.iter_mut() {
                let matches = input_id.is_none()
                    || i.get("InputId").and_then(Value::as_str) == input_id.as_deref();
                if matches {
                    if let Some(o) = i.as_object_mut() {
                        o.insert(
                            "InputStartingPositionConfiguration".into(),
                            json!({ "InputStartingPosition": pos }),
                        );
                    }
                }
            }
        }
    }
}

fn derive_window_end(start: &str) -> String {
    // Parse HH:MM, add 8 hours, wrap at 24h.
    let parts: Vec<&str> = start.split(':').collect();
    if parts.len() == 2 {
        if let (Ok(h), Ok(m)) = (parts[0].parse::<u32>(), parts[1].parse::<u32>()) {
            let end_h = (h + 8) % 24;
            return format!("{end_h:02}:{m:02}");
        }
    }
    DEFAULT_MAINTENANCE_END.to_string()
}

fn application_arn_exists(st: &crate::state::Ka2State, resource_arn: &str) -> bool {
    st.applications.values().any(|a| a.arn == resource_arn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> Ka2Service {
        Ka2Service::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "kinesisanalyticsv2".to_string(),
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

    fn err_msg(r: Result<AwsResponse, AwsServiceError>) -> String {
        match r {
            Ok(_) => panic!("expected error"),
            Err(e) => format!("{e:?}"),
        }
    }

    #[test]
    fn arn_format() {
        assert_eq!(
            arn("us-east-1", "123456789012", "app1"),
            "arn:aws:kinesisanalytics:us-east-1:123456789012:application/app1"
        );
    }

    #[test]
    fn create_describe_roundtrip() {
        let s = svc();
        let resp = s
            .create_application(&req(
                "CreateApplication",
                json!({
                    "ApplicationName": "app1",
                    "RuntimeEnvironment": "FLINK-1_20",
                    "ServiceExecutionRole": "arn:aws:iam::000000000000:role/r",
                    "ApplicationDescription": "hello"
                }),
            ))
            .unwrap();
        let v = body_of(resp);
        let d = &v["ApplicationDetail"];
        assert_eq!(d["ApplicationStatus"], "READY");
        assert_eq!(d["ApplicationVersionId"], 1);
        assert_eq!(d["ApplicationDescription"], "hello");

        // Duplicate -> ResourceInUseException
        let err = err_msg(s.create_application(&req(
            "CreateApplication",
            json!({
                "ApplicationName": "app1",
                "RuntimeEnvironment": "FLINK-1_20",
                "ServiceExecutionRole": "arn:aws:iam::000000000000:role/r"
            }),
        )));
        assert!(err.contains("ResourceInUseException"));
    }

    #[test]
    fn lifecycle_settles() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({
                "ApplicationName": "app1",
                "RuntimeEnvironment": "FLINK-1_20",
                "ServiceExecutionRole": "arn:aws:iam::000000000000:role/r"
            }),
        ))
        .unwrap();
        s.start_application(&req("StartApplication", json!({"ApplicationName":"app1"})))
            .unwrap();
        let v = body_of(
            s.describe_application(&req(
                "DescribeApplication",
                json!({"ApplicationName":"app1"}),
            ))
            .unwrap(),
        );
        assert_eq!(v["ApplicationDetail"]["ApplicationStatus"], "RUNNING");
        s.stop_application(&req("StopApplication", json!({"ApplicationName":"app1"})))
            .unwrap();
        let v = body_of(
            s.describe_application(&req(
                "DescribeApplication",
                json!({"ApplicationName":"app1"}),
            ))
            .unwrap(),
        );
        assert_eq!(v["ApplicationDetail"]["ApplicationStatus"], "READY");
    }

    #[test]
    fn version_bumps_on_update() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({
                "ApplicationName": "app1",
                "RuntimeEnvironment": "FLINK-1_20",
                "ServiceExecutionRole": "arn:aws:iam::000000000000:role/r"
            }),
        ))
        .unwrap();
        let v = body_of(
            s.update_application(&req(
                "UpdateApplication",
                json!({"ApplicationName":"app1","ServiceExecutionRoleUpdate":"arn:aws:iam::000000000000:role/r2"}),
            ))
            .unwrap(),
        );
        assert_eq!(v["ApplicationDetail"]["ApplicationVersionId"], 2);
    }

    #[test]
    fn invalid_runtime_rejected() {
        let s = svc();
        let err = err_msg(s.create_application(&req(
            "CreateApplication",
            json!({
                "ApplicationName": "app1",
                "RuntimeEnvironment": "__INVALID_ENUM_VALUE__",
                "ServiceExecutionRole": "arn:aws:iam::000000000000:role/r"
            }),
        )));
        assert!(err.contains("InvalidArgumentException"));
    }

    #[test]
    fn snapshot_lifecycle() {
        let s = svc();
        s.create_application(&req(
            "CreateApplication",
            json!({
                "ApplicationName": "app1",
                "RuntimeEnvironment": "FLINK-1_20",
                "ServiceExecutionRole": "arn:aws:iam::000000000000:role/r"
            }),
        ))
        .unwrap();
        s.create_application_snapshot(&req(
            "CreateApplicationSnapshot",
            json!({"ApplicationName":"app1","SnapshotName":"s1"}),
        ))
        .unwrap();
        let v = body_of(
            s.describe_application_snapshot(&req(
                "DescribeApplicationSnapshot",
                json!({"ApplicationName":"app1","SnapshotName":"s1"}),
            ))
            .unwrap(),
        );
        assert_eq!(v["SnapshotDetails"]["SnapshotStatus"], "READY");
    }

    fn flink_app_with_jar() -> Application {
        let cfg = config_to_description(
            &json!({
                "FlinkApplicationConfiguration": {
                    "ParallelismConfiguration": {
                        "ConfigurationType": "CUSTOM",
                        "Parallelism": 3
                    }
                },
                "ApplicationCodeConfiguration": {
                    "CodeContentType": "ZIPFILE",
                    "CodeContent": {
                        "S3ContentLocation": {
                            "BucketARN": "arn:aws:s3:::my-code-bucket",
                            "FileKey": "jobs/app.jar"
                        }
                    }
                },
                "EnvironmentProperties": {
                    "PropertyGroups": [
                        { "PropertyGroupId": "kinesis.analytics.flink.run.options",
                          "PropertyMap": { "ProgramArgs": "--error-rate 0.1" } }
                    ]
                }
            }),
            Some("arn:aws:iam::000000000000:role/r"),
            "FLINK-1_19",
            &mut 0,
        );
        Application {
            name: "flinkapp".into(),
            arn: arn("us-east-1", "000000000000", "flinkapp"),
            description: None,
            runtime_environment: "FLINK-1_19".into(),
            service_execution_role: Some("arn:aws:iam::000000000000:role/r".into()),
            application_mode: Some("STREAMING".into()),
            status: "READY".into(),
            version_id: 1,
            create_timestamp: Utc::now(),
            last_update_timestamp: Utc::now(),
            conditional_token: "t".into(),
            config_description: cfg,
            cloudwatch_logging_options: Vec::new(),
            maintenance_start: DEFAULT_MAINTENANCE_START.into(),
            maintenance_end: DEFAULT_MAINTENANCE_END.into(),
            snapshots: BTreeMap::new(),
            operations: Vec::new(),
            versions: BTreeMap::new(),
            id_counter: 0,
            version_updated_from: None,
            version_rolled_back_from: None,
            version_rolled_back_to: None,
            flink_binding: None,
        }
    }

    #[test]
    fn flink_app_with_jar_is_real() {
        let app = flink_app_with_jar();
        assert!(is_real_flink(&app));
        assert_eq!(
            flink_code_location(&app),
            Some(("my-code-bucket".into(), "jobs/app.jar".into()))
        );
        assert_eq!(flink_parallelism(&app), Some(3));
        assert_eq!(
            flink_program_args(&app).as_deref(),
            Some("--error-rate 0.1")
        );
    }

    #[test]
    fn sql_app_is_not_real_flink() {
        let mut app = flink_app_with_jar();
        app.runtime_environment = "SQL-1_0".into();
        // SQL flavor -> control-plane only, never the real Docker Flink job.
        assert!(!is_real_flink(&app));
    }

    #[test]
    fn flink_app_without_jar_is_not_real() {
        // A Flink runtime with no S3 code location stays on the control plane.
        let mut app = flink_app_with_jar();
        app.config_description = json!({});
        assert!(!is_real_flink(&app));
        assert_eq!(flink_code_location(&app), None);
    }

    #[test]
    fn degraded_fallback_no_runtime_settles_and_synthesizes_url() {
        // With no runtime attached (the degraded / control-plane path), a Flink
        // app with a JAR still starts as the pure state machine and the
        // presigned URL is the synthesized AWS-shaped URL, not a live endpoint.
        let s = svc();
        assert!(!s.runtime_present());
        s.create_application(&req(
            "CreateApplication",
            json!({
                "ApplicationName": "app1",
                "RuntimeEnvironment": "FLINK-1_19",
                "ServiceExecutionRole": "arn:aws:iam::000000000000:role/r",
                "ApplicationConfiguration": {
                    "ApplicationCodeConfiguration": {
                        "CodeContentType": "ZIPFILE",
                        "CodeContent": { "S3ContentLocation": {
                            "BucketARN": "arn:aws:s3:::b", "FileKey": "app.jar" } }
                    }
                }
            }),
        ))
        .unwrap();
        s.start_application(&req("StartApplication", json!({"ApplicationName":"app1"})))
            .unwrap();
        let v = body_of(
            s.describe_application(&req(
                "DescribeApplication",
                json!({"ApplicationName":"app1"}),
            ))
            .unwrap(),
        );
        // No runtime -> the state machine settles STARTING -> RUNNING in place.
        assert_eq!(v["ApplicationDetail"]["ApplicationStatus"], "RUNNING");
        let u = body_of(
            s.create_application_presigned_url(&req(
                "CreateApplicationPresignedUrl",
                json!({"ApplicationName":"app1","UrlType":"FLINK_DASHBOARD_URL"}),
            ))
            .unwrap(),
        );
        assert!(
            u["AuthorizedUrl"]
                .as_str()
                .unwrap()
                .contains("kinesisanalytics.us-east-1.amazonaws.com"),
            "degraded path returns the synthesized dashboard URL: {}",
            u["AuthorizedUrl"]
        );
    }

    #[test]
    fn pagination_lists_applications() {
        let s = svc();
        for i in 0..3 {
            s.create_application(&req(
                "CreateApplication",
                json!({
                    "ApplicationName": format!("app{i}"),
                    "RuntimeEnvironment": "FLINK-1_20",
                    "ServiceExecutionRole": "arn:aws:iam::000000000000:role/r"
                }),
            ))
            .unwrap();
        }
        let v = body_of(
            s.list_applications(&req("ListApplications", json!({"Limit":2})))
                .unwrap(),
        );
        assert_eq!(v["ApplicationSummaries"].as_array().unwrap().len(), 2);
        assert!(v.get("NextToken").is_some());
    }

    #[test]
    fn china_region_application_arn_uses_the_aws_cn_partition() {
        let s = svc();
        let in_china = |action: &str, body: Value| AwsRequest {
            region: "cn-north-1".to_string(),
            ..req(action, body)
        };
        let v = body_of(
            s.create_application(&in_china(
                "CreateApplication",
                json!({
                    "ApplicationName": "cn-app",
                    "RuntimeEnvironment": "FLINK-1_20",
                    "ServiceExecutionRole": "arn:aws-cn:iam::000000000000:role/r"
                }),
            ))
            .unwrap(),
        );
        let arn = "arn:aws-cn:kinesisanalytics:cn-north-1:000000000000:application/cn-app";
        assert_eq!(v["ApplicationDetail"]["ApplicationARN"], arn);
        s.tag_resource(&in_china(
            "TagResource",
            json!({ "ResourceARN": arn, "Tags": [{ "Key": "env", "Value": "cn" }] }),
        ))
        .unwrap();
        let tags = body_of(
            s.list_tags_for_resource(&in_china(
                "ListTagsForResource",
                json!({ "ResourceARN": arn }),
            ))
            .unwrap(),
        );
        assert_eq!(tags["Tags"][0]["Value"], "cn");
    }

    // ===== real data-plane lifecycle races (deterministic test backend) =====

    mod lifecycle {
        use super::*;
        use crate::runtime::{JobError, RuntimeError};
        use std::sync::atomic::AtomicU64;

        /// Deterministic in-memory `FlinkBackend`: containers and jobs are map
        /// entries, `create_cluster` can be held at a gate so a test can land a
        /// Stop/Delete/Update while a bring-up is in flight.
        #[derive(Default)]
        pub(super) struct FakeBackend {
            next: AtomicU64,
            /// app arn -> tracked cluster (mirrors FlinkRuntime.containers).
            tracked: parking_lot::Mutex<HashMap<String, RunningFlink>>,
            /// live container id -> owning app arn (the label).
            live: parking_lot::Mutex<HashMap<String, String>>,
            pub removed: parking_lot::Mutex<Vec<String>>,
            /// job id -> Flink state.
            pub jobs: parking_lot::Mutex<HashMap<String, String>>,
            /// (container id, parallelism) per submitted job.
            pub runs: parking_lot::Mutex<Vec<(String, Option<i64>)>>,
            gate: parking_lot::Mutex<Option<Arc<tokio::sync::Semaphore>>>,
            pub entered_create: tokio::sync::Notify,
            /// Reattach outcome: None => Ok(None), Some(true) => Ok(Some),
            /// Some(false) => Err (transient daemon failure).
            pub reattach: parking_lot::Mutex<Option<bool>>,
        }

        impl FakeBackend {
            pub fn gated() -> (Arc<Self>, Arc<tokio::sync::Semaphore>) {
                let b = Arc::new(Self::default());
                let sem = Arc::new(tokio::sync::Semaphore::new(0));
                *b.gate.lock() = Some(sem.clone());
                (b, sem)
            }
            pub fn live_count(&self) -> usize {
                self.live.lock().len()
            }
            pub fn is_live(&self, id: &str) -> bool {
                self.live.lock().contains_key(id)
            }
            pub fn tracked_count(&self) -> usize {
                self.tracked.lock().len()
            }
            pub fn running_jobs(&self) -> usize {
                self.jobs
                    .lock()
                    .values()
                    .filter(|s| *s == "RUNNING")
                    .count()
            }
            fn kill(&self, id: &str) {
                if self.live.lock().remove(id).is_some() {
                    self.removed.lock().push(id.to_string());
                }
            }
        }

        #[async_trait::async_trait]
        impl FlinkBackend for FakeBackend {
            fn tracked(&self, app_arn: &str) -> Option<RunningFlink> {
                self.tracked.lock().get(app_arn).cloned()
            }
            async fn create_cluster(&self, app_arn: &str) -> Result<RunningFlink, RuntimeError> {
                self.entered_create.notify_one();
                let gate = self.gate.lock().clone();
                if let Some(g) = gate {
                    g.acquire().await.unwrap().forget();
                }
                let id = format!("c{}", self.next.fetch_add(1, Ordering::SeqCst));
                let r = RunningFlink {
                    container_id: id.clone(),
                    host: "127.0.0.1".into(),
                    rest_port: 1,
                };
                self.live.lock().insert(id, app_arn.to_string());
                self.tracked.lock().insert(app_arn.to_string(), r.clone());
                Ok(r)
            }
            async fn cluster_ready(&self, _running: &RunningFlink) -> bool {
                true
            }
            async fn readiness_failure(
                &self,
                _container_id: &str,
                _started: std::time::Instant,
            ) -> String {
                "timeout".into()
            }
            async fn reattach_cluster(
                &self,
                app_arn: &str,
                container_id: &str,
            ) -> Result<Option<RunningFlink>, RuntimeError> {
                match *self.reattach.lock() {
                    None => Ok(None),
                    Some(false) => Err(RuntimeError::ContainerStartFailed("daemon".into())),
                    Some(true) => {
                        let r = RunningFlink {
                            container_id: container_id.to_string(),
                            host: "127.0.0.1".into(),
                            rest_port: 2,
                        };
                        self.live
                            .lock()
                            .insert(container_id.to_string(), app_arn.to_string());
                        self.tracked.lock().insert(app_arn.to_string(), r.clone());
                        Ok(Some(r))
                    }
                }
            }
            async fn remove_cluster(&self, app_arn: &str, container_id: Option<&str>) {
                if let Some(t) = self.tracked.lock().remove(app_arn) {
                    self.kill(&t.container_id);
                }
                if let Some(id) = container_id {
                    self.kill(id);
                    // A persisted id is removed even when never tracked/live.
                    if !self.removed.lock().iter().any(|r| r == id) {
                        self.removed.lock().push(id.to_string());
                    }
                }
                let labeled: Vec<String> = self
                    .live
                    .lock()
                    .iter()
                    .filter(|(_, a)| *a == app_arn)
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in labeled {
                    self.kill(&id);
                }
            }
            async fn upload_jar(
                &self,
                _running: &RunningFlink,
                _jar: Vec<u8>,
                _file_name: &str,
            ) -> Result<String, JobError> {
                Ok("jar".into())
            }
            async fn run_job(
                &self,
                running: &RunningFlink,
                _jar_id: &str,
                parallelism: Option<i64>,
                _program_args: Option<&str>,
                _entry_class: Option<&str>,
            ) -> Result<String, JobError> {
                let id = format!("j{}", self.next.fetch_add(1, Ordering::SeqCst));
                self.jobs.lock().insert(id.clone(), "RUNNING".into());
                self.runs
                    .lock()
                    .push((running.container_id.clone(), parallelism));
                Ok(id)
            }
            async fn job_state(
                &self,
                _running: &RunningFlink,
                job_id: &str,
            ) -> Result<String, JobError> {
                self.jobs
                    .lock()
                    .get(job_id)
                    .cloned()
                    .ok_or_else(|| JobError::NotFound(job_id.to_string()))
            }
            async fn cancel_job(
                &self,
                _running: &RunningFlink,
                job_id: &str,
            ) -> Result<(), JobError> {
                if let Some(s) = self.jobs.lock().get_mut(job_id) {
                    *s = "CANCELED".into();
                }
                Ok(())
            }
        }

        struct FakeS3;
        impl S3Delivery for FakeS3 {
            fn put_object(
                &self,
                _a: &str,
                _b: &str,
                _k: &str,
                _body: Vec<u8>,
                _ct: Option<&str>,
            ) -> Result<(), String> {
                Ok(())
            }
            fn get_object(&self, _a: &str, _b: &str, _k: &str) -> Result<Vec<u8>, String> {
                Ok(b"PK-jar".to_vec())
            }
        }

        fn real_svc(backend: Arc<FakeBackend>) -> Ka2Service {
            svc().with_backend(backend).with_s3(Arc::new(FakeS3))
        }

        fn create(s: &Ka2Service, name: &str) {
            s.create_application(&req(
                "CreateApplication",
                json!({
                    "ApplicationName": name,
                    "RuntimeEnvironment": "FLINK-1_19",
                    "ServiceExecutionRole": "arn:aws:iam::000000000000:role/r",
                    "ApplicationConfiguration": {
                        "FlinkApplicationConfiguration": {
                            "ParallelismConfiguration": {
                                "ConfigurationType": "CUSTOM", "Parallelism": 3 }
                        },
                        "ApplicationCodeConfiguration": {
                            "CodeContentType": "ZIPFILE",
                            "CodeContent": { "S3ContentLocation": {
                                "BucketARN": "arn:aws:s3:::b", "FileKey": "app.jar" } }
                        }
                    }
                }),
            ))
            .unwrap();
        }

        fn app(s: &Ka2Service, name: &str) -> Option<Application> {
            s.state
                .read()
                .get("000000000000")
                .and_then(|st| st.applications.get(name).cloned())
        }

        fn status(s: &Ka2Service, name: &str) -> String {
            app(s, name).map(|a| a.status).unwrap_or_default()
        }

        async fn until(mut cond: impl FnMut() -> bool, what: &str) {
            for _ in 0..500 {
                if cond() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("timed out waiting for: {what}");
        }

        /// Let any stray background task run to completion.
        async fn quiesce() {
            for _ in 0..20 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        fn start(s: &Ka2Service, name: &str) {
            s.start_application(&req("StartApplication", json!({"ApplicationName": name})))
                .unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn start_settles_running_once_the_job_runs() {
            let b = Arc::new(FakeBackend::default());
            let s = real_svc(b.clone());
            create(&s, "a");
            start(&s, "a");
            until(|| status(&s, "a") == "RUNNING", "RUNNING").await;
            let bind = app(&s, "a").unwrap().flink_binding.unwrap();
            assert!(b.is_live(&bind.container_id));
            assert_eq!(b.running_jobs(), 1);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn stop_during_starting_is_not_undone_by_the_start_task() {
            let (b, gate) = FakeBackend::gated();
            let s = real_svc(b.clone());
            create(&s, "a");
            start(&s, "a");
            b.entered_create.notified().await;
            s.stop_application(&req("StopApplication", json!({"ApplicationName": "a"})))
                .unwrap();
            assert_eq!(status(&s, "a"), "STOPPING");
            // The bring-up now completes its container creation.
            gate.add_permits(1);
            until(|| status(&s, "a") == "READY", "READY after stop").await;
            quiesce().await;
            let a = app(&s, "a").unwrap();
            assert_eq!(
                a.status, "READY",
                "start task must not flip a stopped app to RUNNING"
            );
            assert!(a.flink_binding.is_none());
            assert_eq!(
                b.live_count(),
                0,
                "the superseded bring-up's cluster is torn down"
            );
            assert_eq!(b.tracked_count(), 0);
            assert_eq!(
                b.running_jobs(),
                0,
                "no job was submitted for a stopped app"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn force_stop_during_starting_settles_ready() {
            let (b, gate) = FakeBackend::gated();
            let s = real_svc(b.clone());
            create(&s, "a");
            start(&s, "a");
            b.entered_create.notified().await;
            s.stop_application(&req(
                "StopApplication",
                json!({"ApplicationName": "a", "Force": true}),
            ))
            .unwrap();
            assert_eq!(status(&s, "a"), "FORCE_STOPPING");
            gate.add_permits(1);
            until(|| status(&s, "a") == "READY", "READY after force stop").await;
            quiesce().await;
            assert_eq!(status(&s, "a"), "READY");
            assert_eq!(b.live_count(), 0);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn delete_during_starting_leaks_nothing_and_recreate_starts_clean() {
            let (b, gate) = FakeBackend::gated();
            let s = real_svc(b.clone());
            create(&s, "a");
            let created = app(&s, "a").unwrap().create_timestamp;
            start(&s, "a");
            b.entered_create.notified().await;
            s.delete_application(&req(
                "DeleteApplication",
                json!({"ApplicationName": "a", "CreateTimestamp": created.timestamp()}),
            ))
            .unwrap();
            gate.add_permits(1);
            until(|| b.live_count() == 0 && b.tracked_count() == 0, "teardown").await;
            quiesce().await;
            assert!(app(&s, "a").is_none(), "a deleted app is never resurrected");
            assert_eq!(b.live_count(), 0, "no leaked container");
            assert_eq!(b.tracked_count(), 0, "no stale tracking entry");

            // Same-name re-create + start gets a fresh, live cluster.
            create(&s, "a");
            gate.add_permits(1);
            start(&s, "a");
            until(|| status(&s, "a") == "RUNNING", "recreated RUNNING").await;
            let bind = app(&s, "a").unwrap().flink_binding.unwrap();
            assert!(
                b.is_live(&bind.container_id),
                "recreated app runs on a live cluster"
            );
            assert_eq!(b.live_count(), 1);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn delete_running_app_removes_cluster_and_tracking() {
            let b = Arc::new(FakeBackend::default());
            let s = real_svc(b.clone());
            create(&s, "a");
            start(&s, "a");
            until(|| status(&s, "a") == "RUNNING", "RUNNING").await;
            let created = app(&s, "a").unwrap().create_timestamp;
            s.delete_application(&req(
                "DeleteApplication",
                json!({"ApplicationName": "a", "CreateTimestamp": created.timestamp()}),
            ))
            .unwrap();
            until(|| b.live_count() == 0 && b.tracked_count() == 0, "teardown").await;
            assert_eq!(b.running_jobs(), 0, "job canceled on delete");
        }

        fn update_parallelism(s: &Ka2Service, name: &str, p: i64) -> Value {
            let v = app(s, name).unwrap().version_id;
            body_of(
                s.update_application(&req(
                    "UpdateApplication",
                    json!({
                        "ApplicationName": name,
                        "CurrentApplicationVersionId": v,
                        "ApplicationConfigurationUpdate": {
                            "FlinkApplicationConfigurationUpdate": {
                                "ParallelismConfigurationUpdate": {
                                    "ConfigurationTypeUpdate": "CUSTOM",
                                    "ParallelismUpdate": p
                                }
                            }
                        }
                    }),
                ))
                .unwrap(),
            )
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn update_running_app_redeploys_the_job_then_runs() {
            let b = Arc::new(FakeBackend::default());
            let s = real_svc(b.clone());
            create(&s, "a");
            start(&s, "a");
            until(|| status(&s, "a") == "RUNNING", "RUNNING").await;
            let first = app(&s, "a").unwrap().flink_binding.unwrap();
            let v = update_parallelism(&s, "a", 5);
            assert_eq!(v["ApplicationDetail"]["ApplicationStatus"], "UPDATING");
            assert_eq!(v["ApplicationDetail"]["ApplicationVersionId"], 2);
            until(|| status(&s, "a") == "RUNNING", "RUNNING after update").await;
            let second = app(&s, "a").unwrap().flink_binding.unwrap();
            assert_eq!(second.container_id, first.container_id, "cluster reused");
            assert_ne!(second.job_id, first.job_id, "job redeployed");
            assert_eq!(
                b.jobs.lock().get(first.job_id.as_ref().unwrap()).unwrap(),
                "CANCELED"
            );
            assert_eq!(
                b.runs.lock().last().unwrap().1,
                Some(5),
                "new config applied"
            );
            assert_eq!(b.running_jobs(), 1);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn rollback_running_app_redeploys_previous_config_and_stays_running() {
            let b = Arc::new(FakeBackend::default());
            let s = real_svc(b.clone());
            create(&s, "a");
            start(&s, "a");
            until(|| status(&s, "a") == "RUNNING", "RUNNING").await;
            update_parallelism(&s, "a", 5);
            until(|| status(&s, "a") == "RUNNING", "RUNNING after update").await;
            let v = body_of(
                s.rollback_application(&req(
                    "RollbackApplication",
                    json!({"ApplicationName": "a", "CurrentApplicationVersionId": 2}),
                ))
                .unwrap(),
            );
            assert_eq!(v["ApplicationDetail"]["ApplicationStatus"], "ROLLING_BACK");
            assert_eq!(v["ApplicationDetail"]["ApplicationVersionRolledBackTo"], 1);
            until(|| status(&s, "a") == "RUNNING", "RUNNING after rollback").await;
            assert_eq!(
                b.runs.lock().last().unwrap().1,
                Some(3),
                "previous config redeployed"
            );
            assert_eq!(b.running_jobs(), 1, "exactly one job runs after rollback");
            assert_eq!(flink_parallelism(&app(&s, "a").unwrap()), Some(3));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn stop_during_update_wins_over_the_redeploy() {
            let b = Arc::new(FakeBackend::default());
            let s = real_svc(b.clone());
            create(&s, "a");
            start(&s, "a");
            until(|| status(&s, "a") == "RUNNING", "RUNNING").await;
            update_parallelism(&s, "a", 5);
            s.stop_application(&req("StopApplication", json!({"ApplicationName": "a"})))
                .unwrap();
            until(|| status(&s, "a") == "READY", "READY").await;
            quiesce().await;
            assert_eq!(status(&s, "a"), "READY");
            assert_eq!(b.live_count(), 0);
            assert_eq!(b.running_jobs(), 0);
        }

        fn seed(s: &Ka2Service, name: &str, status: &str, binding: Option<FlinkBinding>) {
            let mut a = super::flink_app_with_jar();
            a.name = name.into();
            a.arn = arn("us-east-1", "000000000000", name);
            a.status = status.into();
            a.flink_binding = binding;
            s.state
                .write()
                .get_or_create("000000000000")
                .applications
                .insert(name.into(), a);
        }

        fn bound(id: &str, job: Option<&str>) -> Option<FlinkBinding> {
            Some(FlinkBinding {
                container_id: id.into(),
                host: "127.0.0.1".into(),
                rest_port: 9,
                jar_id: Some("jar".into()),
                job_id: job.map(str::to_string),
            })
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn recovery_resumes_starting_without_binding() {
            let b = Arc::new(FakeBackend::default());
            let s = real_svc(b.clone());
            seed(&s, "a", "STARTING", None);
            s.recover_persisted_containers().await;
            until(|| status(&s, "a") == "RUNNING", "resumed RUNNING").await;
            assert!(app(&s, "a").unwrap().flink_binding.is_some());
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn recovery_resumes_interrupted_update_on_a_fresh_cluster() {
            let b = Arc::new(FakeBackend::default());
            let s = real_svc(b.clone());
            seed(&s, "a", "UPDATING", bound("old", Some("jold")));
            s.recover_persisted_containers().await;
            until(|| status(&s, "a") == "RUNNING", "resumed RUNNING").await;
            assert!(b.removed.lock().contains(&"old".to_string()));
            assert_ne!(
                app(&s, "a").unwrap().flink_binding.unwrap().container_id,
                "old"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn recovery_finishes_stopping_and_force_stopping() {
            let b = Arc::new(FakeBackend::default());
            let s = real_svc(b.clone());
            seed(&s, "a", "STOPPING", bound("c-a", Some("j")));
            seed(&s, "f", "FORCE_STOPPING", None);
            s.recover_persisted_containers().await;
            until(
                || status(&s, "a") == "READY" && status(&s, "f") == "READY",
                "stops settled",
            )
            .await;
            assert!(b.removed.lock().contains(&"c-a".to_string()));
            assert!(app(&s, "a").unwrap().flink_binding.is_none());
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn recovery_reattach_error_removes_the_container_by_id() {
            let b = Arc::new(FakeBackend::default());
            *b.reattach.lock() = Some(false);
            let s = real_svc(b.clone());
            seed(&s, "a", "RUNNING", bound("persisted", Some("j")));
            s.recover_persisted_containers().await;
            until(|| status(&s, "a") == "READY", "READY").await;
            assert!(
                b.removed.lock().contains(&"persisted".to_string()),
                "untracked persisted container removed by id"
            );
            assert!(app(&s, "a").unwrap().flink_binding.is_none());
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn recovery_reattaches_running_job() {
            let b = Arc::new(FakeBackend::default());
            *b.reattach.lock() = Some(true);
            b.jobs.lock().insert("j".into(), "RUNNING".into());
            let s = real_svc(b.clone());
            seed(&s, "a", "RUNNING", bound("persisted", Some("j")));
            s.recover_persisted_containers().await;
            until(
                || app(&s, "a").unwrap().flink_binding.map(|b| b.rest_port) == Some(2),
                "binding refreshed",
            )
            .await;
            assert_eq!(status(&s, "a"), "RUNNING");
            assert!(b.is_live("persisted"));
        }

        #[test]
        fn control_plane_rollback_of_running_app_settles_running() {
            let s = svc();
            create(&s, "a");
            start(&s, "a");
            let v = body_of(
                s.describe_application(&req("DescribeApplication", json!({"ApplicationName":"a"})))
                    .unwrap(),
            );
            assert_eq!(v["ApplicationDetail"]["ApplicationStatus"], "RUNNING");
            update_parallelism(&s, "a", 5);
            let v = body_of(
                s.rollback_application(&req(
                    "RollbackApplication",
                    json!({"ApplicationName": "a", "CurrentApplicationVersionId": 2}),
                ))
                .unwrap(),
            );
            assert_eq!(v["ApplicationDetail"]["ApplicationStatus"], "ROLLING_BACK");
            let v = body_of(
                s.describe_application(&req("DescribeApplication", json!({"ApplicationName":"a"})))
                    .unwrap(),
            );
            assert_eq!(v["ApplicationDetail"]["ApplicationStatus"], "RUNNING");
            assert_eq!(flink_parallelism(&app(&s, "a").unwrap()), Some(3));
        }

        #[test]
        fn rollback_of_ready_app_stays_ready() {
            let s = svc();
            create(&s, "a");
            update_parallelism(&s, "a", 5);
            let v = body_of(
                s.rollback_application(&req(
                    "RollbackApplication",
                    json!({"ApplicationName": "a", "CurrentApplicationVersionId": 2}),
                ))
                .unwrap(),
            );
            assert_eq!(v["ApplicationDetail"]["ApplicationStatus"], "READY");
        }
    }
}
