//! Resource-specific SageMaker handlers that fall outside the generic resource
//! engine:
//!
//! * tagging (`AddTags` / `ListTags` / `DeleteTags`), which persists tags keyed
//!   by resource ARN; and
//! * a small set of **Action verbs that are the only creation path for their
//!   resource family** (`StartPipelineExecution`, `ImportHubContent`,
//!   `AddAssociation`). The generic Action arm is a stateless no-op, so without
//!   these handlers the family's Describe / List / Update / Delete siblings
//!   would 404 / return empty. Each mints the family's identifier, builds a
//!   record carrying the required output members, and inserts it into the
//!   resource family so those siblings resolve it. (`DeleteAssociation` is also
//!   claimed here so the composite source+destination edge is removed exactly.)
//!
//! These are distinct from the intentional Start/Stop *lifecycle* no-ops
//! (`StartNotebookInstance`, `StopTrainingJob`, `StopPipelineExecution`, ...)
//! which advance state on a resource that already exists — a documented
//! emulation limit left to the generic Action arm.

use serde_json::{Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::generated::OpMeta;

use super::{engine, now_epoch, ok_json, Ctx, SageMakerService};

/// Dispatch an operation to a resource-specific handler. Returns `Ok(None)` if
/// the operation is not claimed here (the caller then falls through to the
/// generic engine).
pub(super) fn dispatch(
    svc: &SageMakerService,
    meta: &OpMeta,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> Result<Option<(AwsResponse, bool)>, AwsServiceError> {
    match meta.op {
        "AddTags" => Ok(Some(add_tags(svc, ctx, body))),
        "ListTags" => Ok(Some(list_tags(svc, ctx, body))),
        "DeleteTags" => Ok(Some(delete_tags(svc, ctx, body))),
        "StartPipelineExecution" => Ok(Some(start_pipeline_execution(svc, ctx, meta, body))),
        "ImportHubContent" => Ok(Some(import_hub_content(svc, ctx, meta, body))),
        "AddAssociation" => Ok(Some(add_association(svc, ctx, meta, body))),
        "DeleteAssociation" => Ok(Some(delete_association(svc, ctx, body))),
        "PutModelPackageGroupPolicy" => Ok(Some(put_mpg_policy(svc, ctx, meta, body))),
        "GetModelPackageGroupPolicy" => Ok(get_mpg_policy(svc, ctx, body)),
        "DeleteModelPackageGroupPolicy" => Ok(Some(delete_mpg_policy(svc, ctx, meta, body))),
        "RegisterDevices" => Ok(Some(register_devices(svc, ctx, meta, body))),
        "DeregisterDevices" => Ok(Some(deregister_devices(svc, ctx, meta, body))),
        "UpdateDevices" => Ok(Some(update_devices(svc, ctx, meta, body))),
        op if is_lifecycle_transition(op) => Ok(lifecycle_transition(svc, ctx, meta, body)),
        _ => Ok(None),
    }
}

// ── Model package group resource policy ──────────────────────────────────
//
// `PutModelPackageGroupPolicy` / `GetModelPackageGroupPolicy` /
// `DeleteModelPackageGroupPolicy` are all Action verbs. The generic Action arm
// discarded the `ResourcePolicy` on Put and synthesised a placeholder `"a"` on
// Get, so the policy never round-tripped. Persist it in account-scoped state
// keyed by the group name (bug-hunt 2026-07-16, 1.24).

fn mpg_policy_singleton(name: &str) -> String {
    format!("ModelPackageGroupPolicy:{name}")
}

fn put_mpg_policy(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let name = str_member(body, "ModelPackageGroupName");
    let policy = body.get("ResourcePolicy").cloned().unwrap_or(Value::Null);
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        data.singletons.insert(mpg_policy_singleton(&name), policy);
    }
    (engine::action(ctx, meta, body), true)
}

fn get_mpg_policy(
    svc: &SageMakerService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> Option<(AwsResponse, bool)> {
    let name = str_member(body, "ModelPackageGroupName");
    let g = svc.state.read();
    let stored = g
        .get(&ctx.account)
        .and_then(|data| data.singletons.get(&mpg_policy_singleton(&name)))
        .filter(|v| !v.is_null())
        .cloned();
    // Return the stored policy when present; otherwise return None so the caller
    // falls through to the generic Action arm, keeping a valid output shape for a
    // stateless probe that never called Put.
    let policy = stored?;
    let mut out = Map::new();
    out.insert("ResourcePolicy".to_string(), policy);
    Some((ok_json(Value::Object(out)), false))
}

fn delete_mpg_policy(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let name = str_member(body, "ModelPackageGroupName");
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        data.singletons.remove(&mpg_policy_singleton(&name));
    }
    (engine::action(ctx, meta, body), true)
}

// ── Edge devices ─────────────────────────────────────────────────────────
//
// `RegisterDevices` wrote nothing (generic Action no-op) and the read siblings
// `ListDevices` / `DescribeDevice` read the `Device` (singular) family, so
// registration was invisible. Persist each device under the `Device` family
// keyed by its `DeviceName` so the read siblings resolve it, and route
// `DeregisterDevices` / `UpdateDevices` to the same family (bug-hunt
// 2026-07-16, 1.24).

const DEVICE_FAMILY: &str = "Device";

fn register_devices(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let fleet = str_member(body, "DeviceFleetName");
    let devices = body.get("Devices").and_then(Value::as_array).cloned();
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        for dev in devices.into_iter().flatten() {
            let Some(obj) = dev.as_object() else { continue };
            let Some(name) = obj.get("DeviceName").and_then(Value::as_str) else {
                continue;
            };
            let mut record = obj.clone();
            record
                .entry("DeviceFleetName".to_string())
                .or_insert_with(|| Value::String(fleet.clone()));
            record.insert(
                "DeviceArn".to_string(),
                Value::String(super::mint_arn(ctx, "device", name)),
            );
            record
                .entry("RegistrationTime".to_string())
                .or_insert_with(now_epoch);
            data.put_resource(DEVICE_FAMILY, name, Value::Object(record));
        }
    }
    (engine::action(ctx, meta, body), true)
}

fn deregister_devices(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let names = body.get("DeviceNames").and_then(Value::as_array).cloned();
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        for name in names.into_iter().flatten() {
            if let Some(n) = name.as_str() {
                data.remove_resource(DEVICE_FAMILY, n);
            }
        }
    }
    (engine::action(ctx, meta, body), true)
}

fn update_devices(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let devices = body.get("Devices").and_then(Value::as_array).cloned();
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        for dev in devices.into_iter().flatten() {
            let Some(obj) = dev.as_object() else { continue };
            let Some(name) = obj.get("DeviceName").and_then(Value::as_str) else {
                continue;
            };
            // Merge the update onto the existing record so registration-time
            // fields (DeviceArn, DeviceFleetName, RegistrationTime) survive.
            let mut record = data
                .get_resource(DEVICE_FAMILY, name)
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            for (k, v) in obj {
                record.insert(k.clone(), v.clone());
            }
            record
                .entry("DeviceArn".to_string())
                .or_insert_with(|| Value::String(super::mint_arn(ctx, "device", name)));
            data.put_resource(DEVICE_FAMILY, name, Value::Object(record));
        }
    }
    (engine::action(ctx, meta, body), true)
}

/// `Start*` / `Stop*` operations that transition a stored resource's status.
/// `StartPipelineExecution` is handled separately above (it *creates* a record),
/// and `StartSession` opens a session rather than transitioning a resource, so
/// both are excluded here.
fn is_lifecycle_transition(op: &str) -> bool {
    (op.starts_with("Start") || op.starts_with("Stop"))
        && op != "StartPipelineExecution"
        && op != "StartSession"
}

/// The settled status a resource reports after a `Start*` (`started=true`) or
/// `Stop*` transition. Values are real members of each family's status enum;
/// families not listed fall back to the generic job lifecycle
/// (`InService` / `Stopped`).
fn transition_status(family: &str, started: bool) -> &'static str {
    match (family, started) {
        ("NotebookInstance", true) => "InService",
        ("NotebookInstance", false) => "Stopped",
        ("MonitoringSchedule", true) => "Scheduled",
        ("MonitoringSchedule", false) => "Stopped",
        ("InferenceExperiment", true) => "Running",
        ("InferenceExperiment", false) => "Cancelled",
        ("MlflowTrackingServer", true) => "Created",
        ("MlflowTrackingServer", false) => "Stopped",
        // Batch/async jobs and everything else: a Stop settles to Stopped; a
        // Start (rare for jobs) returns the resource to service.
        (_, true) => "InService",
        (_, false) => "Stopped",
    }
}

/// Apply a `Start*` / `Stop*` transition to the target resource's `{Family}Status`
/// member (or the single `*Status` member it carries) so a subsequent Describe
/// reflects the new state instead of the stale one. Returns the standard action
/// output. If no matching record exists, returns `None` so the caller falls back
/// to the generic no-op action response (matching AWS, which 4xx's only when the
/// resource is absent — but our engine has no such record to reject against).
fn lifecycle_transition(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Option<(AwsResponse, bool)> {
    let started = meta.op.starts_with("Start");
    let new_status = transition_status(meta.family, started);
    let ident = super::engine::action_key(body);

    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let key = data.resolve_key(meta.family, &ident)?;
    let rec = data.get_resource_mut(meta.family, &key)?;
    let obj = rec.as_object_mut()?;

    // Prefer an existing `*Status` member; otherwise write the canonical
    // `{Family}Status`. A freshly-created record often carries no status member
    // at all (status is server-derived), so we must insert one — otherwise a
    // Describe would keep synthesising the default "healthy" status and the
    // Stop/Start would appear to do nothing.
    let canonical = format!("{}Status", meta.family);
    let status_key = if obj.contains_key(&canonical) {
        canonical
    } else {
        obj.keys()
            .find(|k| k.ends_with("Status"))
            .filter(|_| obj.keys().filter(|k| k.ends_with("Status")).count() == 1)
            .cloned()
            .unwrap_or(canonical)
    };
    obj.insert(status_key, Value::String(new_status.to_string()));
    Some((super::engine::action(ctx, meta, body), true))
}

fn str_member(body: &Map<String, Value>, key: &str) -> String {
    body.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// `StartPipelineExecution` — persist a pipeline-execution record so
/// DescribePipelineExecution / ListPipelineExecutions / UpdatePipelineExecution
/// / StopPipelineExecution operate on the minted `PipelineExecutionArn`.
fn start_pipeline_execution(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let pipeline_name = str_member(body, "PipelineName");
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let exec_id = super::mint_id(
        &ctx.account,
        "PipelineExecution",
        &data.next_seq().to_string(),
    );
    let arn = format!(
        "arn:aws:sagemaker:{}:{}:pipeline/{}/execution/{}",
        ctx.region, ctx.account, pipeline_name, exec_id
    );
    let mut seed = body.clone();
    seed.insert(
        "PipelineExecutionArn".to_string(),
        Value::String(arn.clone()),
    );
    seed.insert(
        "PipelineArn".to_string(),
        Value::String(format!(
            "arn:aws:sagemaker:{}:{}:pipeline/{}",
            ctx.region, ctx.account, pipeline_name
        )),
    );
    seed.entry("PipelineExecutionStatus".to_string())
        .or_insert_with(|| Value::String("Executing".to_string()));
    seed.insert("StartTime".to_string(), now_epoch());
    (engine::action_create(data, ctx, meta, &arn, &seed), true)
}

/// `ImportHubContent` — persist a hub-content record so DescribeHubContent /
/// ListHubContents / DeleteHubContent operate on it. Keyed by `HubContentName`
/// (the family's Describe/Delete key member).
fn import_hub_content(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let hub_content_name = str_member(body, "HubContentName");
    let hub_name = str_member(body, "HubName");
    let mut seed = body.clone();
    seed.insert(
        "HubArn".to_string(),
        Value::String(format!(
            "arn:aws:sagemaker:{}:{}:hub/{}",
            ctx.region, ctx.account, hub_name
        )),
    );
    seed.entry("HubContentStatus".to_string())
        .or_insert_with(|| Value::String("Available".to_string()));
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    (
        engine::action_create(data, ctx, meta, &hub_content_name, &seed),
        true,
    )
}

/// `AddAssociation` — persist the lineage edge so ListAssociations returns it.
/// Keyed by the composite `SourceArn|DestinationArn` so multiple destinations
/// from one source coexist (each is a distinct edge).
fn add_association(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let key = association_key(body);
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    (engine::action_create(data, ctx, meta, &key, body), true)
}

/// `DeleteAssociation` — remove exactly the (source, destination) edge and echo
/// the two ARNs. Idempotent: deleting an absent edge is a success.
fn delete_association(
    svc: &SageMakerService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let key = association_key(body);
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.remove_resource("Association", &key);
    let mut out = Map::new();
    out.insert(
        "SourceArn".to_string(),
        Value::String(str_member(body, "SourceArn")),
    );
    out.insert(
        "DestinationArn".to_string(),
        Value::String(str_member(body, "DestinationArn")),
    );
    (ok_json(Value::Object(out)), true)
}

/// The composite storage key for an association edge.
fn association_key(body: &Map<String, Value>) -> String {
    format!(
        "{}|{}",
        str_member(body, "SourceArn"),
        str_member(body, "DestinationArn")
    )
}

fn tags_to_array(tags: &std::collections::BTreeMap<String, String>) -> Value {
    Value::Array(
        tags.iter()
            .map(|(k, v)| {
                let mut m = Map::new();
                m.insert("Key".to_string(), Value::String(k.clone()));
                m.insert("Value".to_string(), Value::String(v.clone()));
                Value::Object(m)
            })
            .collect(),
    )
}

fn add_tags(svc: &SageMakerService, ctx: &Ctx, body: &Map<String, Value>) -> (AwsResponse, bool) {
    let arn = body
        .get("ResourceArn")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let entry = data.tags.entry(arn).or_default();
    if let Some(list) = body.get("Tags").and_then(Value::as_array) {
        for t in list {
            let key = t.get("Key").and_then(Value::as_str);
            let val = t.get("Value").and_then(Value::as_str).unwrap_or_default();
            if let Some(key) = key {
                entry.insert(key.to_string(), val.to_string());
            }
        }
    }
    let out = tags_to_array(entry);
    let mut resp = Map::new();
    resp.insert("Tags".to_string(), out);
    (ok_json(Value::Object(resp)), true)
}

fn list_tags(svc: &SageMakerService, ctx: &Ctx, body: &Map<String, Value>) -> (AwsResponse, bool) {
    let arn = body
        .get("ResourceArn")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let g = svc.state.read();
    let tags = g
        .get(&ctx.account)
        .and_then(|d| d.tags.get(&arn).cloned())
        .unwrap_or_default();
    let mut resp = Map::new();
    resp.insert("Tags".to_string(), tags_to_array(&tags));
    (ok_json(Value::Object(resp)), false)
}

fn delete_tags(
    svc: &SageMakerService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let arn = body
        .get("ResourceArn")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(entry) = data.tags.get_mut(&arn) {
        if let Some(keys) = body.get("TagKeys").and_then(Value::as_array) {
            for k in keys {
                if let Some(k) = k.as_str() {
                    entry.remove(k);
                }
            }
        }
        if entry.is_empty() {
            data.tags.remove(&arn);
        }
    }
    (ok_json(Value::Object(Map::new())), true)
}
