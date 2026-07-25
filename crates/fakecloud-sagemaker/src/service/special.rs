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
        "EnableSagemakerServicecatalogPortfolio" => {
            Ok(Some(set_portfolio_status(svc, ctx, meta, body, "Enabled")))
        }
        "DisableSagemakerServicecatalogPortfolio" => {
            Ok(Some(set_portfolio_status(svc, ctx, meta, body, "Disabled")))
        }
        "GetSagemakerServicecatalogPortfolioStatus" => {
            Ok(Some(get_portfolio_status(svc, ctx, body)))
        }
        "RetryPipelineExecution" => Ok(Some(retry_pipeline_execution(svc, ctx, meta, body))),
        "BatchAddClusterNodes" => Ok(Some(batch_add_cluster_nodes(svc, ctx, meta, body))),
        "BatchDeleteClusterNodes" => Ok(Some(batch_delete_cluster_nodes(svc, ctx, meta, body))),
        "BatchRebootClusterNodes" => Ok(Some(batch_reboot_cluster_nodes(svc, ctx, meta, body))),
        "BatchReplaceClusterNodes" => Ok(Some(batch_replace_cluster_nodes(svc, ctx, meta, body))),
        "AssociateTrialComponent" => Ok(Some(associate_trial_component(svc, ctx, meta, body))),
        "DisassociateTrialComponent" => {
            Ok(Some(disassociate_trial_component(svc, ctx, meta, body)))
        }
        "ListTrialComponents" => Ok(list_trial_components(svc, ctx, meta, body)),
        "ListClusterNodes" => Ok(Some(list_cluster_nodes(svc, ctx, meta, body))),
        "ListPipelineExecutionSteps" => Ok(list_pipeline_execution_steps(svc, ctx, meta, body)),
        "ListPipelineExecutions" => Ok(list_pipeline_executions(svc, ctx, meta, body)),
        "SendPipelineExecutionStepSuccess" => Ok(Some(send_pipeline_execution_step(
            svc, ctx, meta, body, true,
        ))),
        "SendPipelineExecutionStepFailure" => Ok(Some(send_pipeline_execution_step(
            svc, ctx, meta, body, false,
        ))),
        // Six modelled ops the generator names under a family slug taken from the
        // op's own shape (`EndpointWeightsAndCapacities`, `ClusterSoftware`,
        // `FeatureMetadata`, `InferenceComponentRuntimeConfig`, `MonitoringAlert`,
        // `PipelineVersion`) that no `Create*` ever populates — so the generic
        // engine's `resolve_key` always missed and hard-404'd against a resource
        // that provably exists. Resolve each against the PARENT family its real
        // `Create*` stores under (Endpoint / Cluster / FeatureGroup /
        // InferenceComponent / MonitoringSchedule / Pipeline) and project the op's
        // modelled output shape (bug-hunt 2026-07-25 Tier-1).
        "UpdateEndpointWeightsAndCapacities" => {
            Ok(Some(update_endpoint_weights(svc, ctx, meta, body)?))
        }
        "UpdateClusterSoftware" => Ok(Some(update_cluster_software(svc, ctx, meta, body)?)),
        "DescribeFeatureMetadata" => Ok(Some(describe_feature_metadata(svc, ctx, meta, body)?)),
        "UpdateFeatureMetadata" => Ok(Some(update_feature_metadata(svc, ctx, meta, body)?)),
        "UpdateInferenceComponentRuntimeConfig" => Ok(Some(
            update_inference_component_runtime_config(svc, ctx, meta, body)?,
        )),
        "UpdateMonitoringAlert" => Ok(Some(update_monitoring_alert(svc, ctx, meta, body)?)),
        "UpdatePipelineVersion" => Ok(Some(update_pipeline_version(svc, ctx, meta, body)?)),
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

// ── Service Catalog portfolio status ─────────────────────────────────────
//
// `EnableSagemakerServicecatalogPortfolio` / `DisableSagemakerServicecatalogPortfolio`
// / `GetSagemakerServicecatalogPortfolioStatus` are all Action verbs. The
// generic Action arm discarded the Enable/Disable and returned an empty Get
// (no `Status`), so the portfolio status never round-tripped and Terraform saw a
// perpetual diff. Persist the account-scoped status singleton on Enable/Disable
// and read it back in Get, defaulting to `Disabled` (the live-AWS default before
// any Enable) (bug-hunt 2026-07-19).

const PORTFOLIO_STATUS_SINGLETON: &str = "SagemakerServicecatalogPortfolioStatus";

fn set_portfolio_status(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
    status: &str,
) -> (AwsResponse, bool) {
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        data.singletons.insert(
            PORTFOLIO_STATUS_SINGLETON.to_string(),
            Value::String(status.to_string()),
        );
    }
    (engine::action(ctx, meta, body), true)
}

fn get_portfolio_status(
    svc: &SageMakerService,
    ctx: &Ctx,
    _body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let g = svc.state.read();
    let status = g
        .get(&ctx.account)
        .and_then(|data| data.singletons.get(PORTFOLIO_STATUS_SINGLETON))
        .and_then(Value::as_str)
        .unwrap_or("Disabled")
        .to_string();
    let mut out = Map::new();
    out.insert("Status".to_string(), Value::String(status));
    (ok_json(Value::Object(out)), false)
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

/// `RetryPipelineExecution` — re-run a stopped/failed pipeline execution by
/// transitioning its stored `PipelineExecutionStatus` back to `Executing`, so a
/// subsequent DescribePipelineExecution reflects the retry instead of the stale
/// terminal status. The execution is keyed by its `PipelineExecutionArn` (as
/// minted by `StartPipelineExecution`). If no such record exists the response
/// still echoes the ARN (idempotent, matching the generic Action arm).
fn retry_pipeline_execution(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let arn = str_member(body, "PipelineExecutionArn");
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(key) = data.resolve_key("PipelineExecution", &arn) {
        if let Some(rec) = data.get_resource_mut("PipelineExecution", &key) {
            if let Some(obj) = rec.as_object_mut() {
                obj.insert(
                    "PipelineExecutionStatus".to_string(),
                    Value::String("Executing".to_string()),
                );
                obj.insert("LastModifiedTime".to_string(), now_epoch());
            }
        }
    }
    (engine::action(ctx, meta, body), true)
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

// ── HyperPod cluster nodes ───────────────────────────────────────────────
//
// `BatchAddClusterNodes` / `BatchDeleteClusterNodes` / `BatchRebootClusterNodes`
// / `BatchReplaceClusterNodes` are Action verbs the generic arm accepted and
// discarded, so a node added by one call was invisible to `ListClusterNodes`.
// Persist each node under the `ClusterNode` family (the family the read
// siblings project) keyed by a minted `NodeLogicalId`, carrying its owning
// `ClusterName` for in-cluster scoping. Only scalar members that are real
// `ClusterNodeSummary` fields are stored, so the generic list projection stays
// shape-valid; `ClusterName` is an internal scoping member the projection drops
// (it is not a `ClusterNodeSummary` field).

const CLUSTER_NODE_FAMILY: &str = "ClusterNode";

/// The set of caller-supplied node identifiers (`NodeIds` are instance ids,
/// `NodeLogicalIds` are logical ids) a batch node operation targets.
fn requested_node_ids(body: &Map<String, Value>) -> Vec<String> {
    let mut ids = Vec::new();
    for key in ["NodeIds", "NodeLogicalIds"] {
        if let Some(arr) = body.get(key).and_then(Value::as_array) {
            ids.extend(arr.iter().filter_map(Value::as_str).map(str::to_string));
        }
    }
    ids
}

/// The stored node's logical id and instance id (either may match a request).
fn node_identifiers(rec: &Value) -> (Option<&str>, Option<&str>) {
    let obj = rec.as_object();
    (
        obj.and_then(|o| o.get("NodeLogicalId"))
            .and_then(Value::as_str),
        obj.and_then(|o| o.get("InstanceId"))
            .and_then(Value::as_str),
    )
}

/// Whether a stored node belongs to `cluster`.
fn node_in_cluster(rec: &Value, cluster: &str) -> bool {
    rec.as_object()
        .and_then(|o| o.get("ClusterName"))
        .and_then(Value::as_str)
        == Some(cluster)
}

fn batch_add_cluster_nodes(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let cluster = str_member(body, "ClusterName");
    let mut successful: Vec<Value> = Vec::new();
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        if let Some(specs) = body.get("NodesToAdd").and_then(Value::as_array) {
            for spec in specs {
                let obj = spec.as_object();
                let group = obj
                    .and_then(|o| o.get("InstanceGroupName"))
                    .and_then(Value::as_str)
                    .unwrap_or("default")
                    .to_string();
                let count = obj
                    .and_then(|o| o.get("IncrementTargetCountBy"))
                    .and_then(Value::as_i64)
                    .unwrap_or(1)
                    .max(1);
                for _ in 0..count {
                    let seq = data.next_seq();
                    let node_logical_id =
                        super::mint_id(&ctx.account, "ClusterNode", &seq.to_string());
                    let instance_id = format!("i-{seq:017x}");
                    let mut record = Map::new();
                    record.insert(
                        "NodeLogicalId".to_string(),
                        Value::String(node_logical_id.clone()),
                    );
                    record.insert("InstanceId".to_string(), Value::String(instance_id));
                    record.insert(
                        "InstanceGroupName".to_string(),
                        Value::String(group.clone()),
                    );
                    record.insert("ClusterName".to_string(), Value::String(cluster.clone()));
                    record.insert("LaunchTime".to_string(), now_epoch());
                    data.put_resource(CLUSTER_NODE_FAMILY, &node_logical_id, Value::Object(record));

                    let mut summary = Map::new();
                    summary.insert("NodeLogicalId".to_string(), Value::String(node_logical_id));
                    summary.insert(
                        "InstanceGroupName".to_string(),
                        Value::String(group.clone()),
                    );
                    summary.insert("Status".to_string(), Value::String("Running".to_string()));
                    successful.push(Value::Object(summary));
                }
            }
        }
    }
    // Return the real minted nodes in `Successful`, completed to a shape-valid
    // response by the generic output builder.
    let mut aug = body.clone();
    aug.insert("Successful".to_string(), Value::Array(successful));
    (engine::action(ctx, meta, &aug), true)
}

fn batch_delete_cluster_nodes(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let cluster = str_member(body, "ClusterName");
    let ids = requested_node_ids(body);
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        let victims: Vec<String> = data
            .list_resource_entries(CLUSTER_NODE_FAMILY)
            .into_iter()
            .filter(|(_k, rec)| {
                node_in_cluster(rec, &cluster) && {
                    let (nlid, iid) = node_identifiers(rec);
                    ids.iter()
                        .any(|id| Some(id.as_str()) == nlid || Some(id.as_str()) == iid)
                }
            })
            .map(|(k, _)| k)
            .collect();
        for key in victims {
            data.remove_resource(CLUSTER_NODE_FAMILY, &key);
        }
    }
    (engine::action(ctx, meta, body), true)
}

fn batch_reboot_cluster_nodes(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let cluster = str_member(body, "ClusterName");
    let ids = requested_node_ids(body);
    // A reboot changes no queryable node attribute (the node keeps its id and
    // group), so this is a read: `Successful` reports exactly the requested ids
    // that resolve to a node in the cluster, reflecting real persisted state.
    let successful: Vec<Value> = {
        let g = svc.state.read();
        let entries = g
            .get(&ctx.account)
            .map(|d| d.list_resource_entries(CLUSTER_NODE_FAMILY))
            .unwrap_or_default();
        ids.iter()
            .filter(|id| {
                entries.iter().any(|(_k, rec)| {
                    node_in_cluster(rec, &cluster) && {
                        let (nlid, iid) = node_identifiers(rec);
                        Some(id.as_str()) == nlid || Some(id.as_str()) == iid
                    }
                })
            })
            .map(|id| Value::String(id.clone()))
            .collect()
    };
    let mut aug = body.clone();
    aug.insert("Successful".to_string(), Value::Array(successful));
    (engine::action(ctx, meta, &aug), false)
}

fn batch_replace_cluster_nodes(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let cluster = str_member(body, "ClusterName");
    let ids = requested_node_ids(body);
    let mut successful: Vec<Value> = Vec::new();
    let mut mutated = false;
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        // Resolve each requested id to a stored node key, then replace the node's
        // underlying instance (a fresh InstanceId + boot time), keeping its
        // logical id so `ListClusterNodes` reflects the replacement.
        let matches: Vec<(String, String)> = data
            .list_resource_entries(CLUSTER_NODE_FAMILY)
            .into_iter()
            .filter(|(_k, rec)| node_in_cluster(rec, &cluster))
            .filter_map(|(k, rec)| {
                let (nlid, iid) = node_identifiers(&rec);
                ids.iter()
                    .find(|id| Some(id.as_str()) == nlid || Some(id.as_str()) == iid)
                    .map(|id| (k.clone(), id.clone()))
            })
            .collect();
        for (key, requested) in matches {
            let seq = data.next_seq();
            if let Some(rec) = data.get_resource_mut(CLUSTER_NODE_FAMILY, &key) {
                if let Some(obj) = rec.as_object_mut() {
                    obj.insert(
                        "InstanceId".to_string(),
                        Value::String(format!("i-{seq:017x}")),
                    );
                    obj.insert("LaunchTime".to_string(), now_epoch());
                    mutated = true;
                }
            }
            successful.push(Value::String(requested));
        }
    }
    let mut aug = body.clone();
    aug.insert("Successful".to_string(), Value::Array(successful));
    (engine::action(ctx, meta, &aug), mutated)
}

/// Scoped `ListClusterNodes`: the operation carries a **required** `ClusterName`,
/// but the generic list engine ignores it and would return every account node
/// across all clusters. Filter the `ClusterNode` family to the requested
/// cluster (reusing the `node_in_cluster` scoping the batch mutators apply),
/// then project via the shared list response builder for shape parity.
fn list_cluster_nodes(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let cluster = str_member(body, "ClusterName");
    let g = svc.state.read();
    let entries = g
        .get(&ctx.account)
        .map(|d| d.list_resource_entries(CLUSTER_NODE_FAMILY))
        .unwrap_or_default();
    let filtered: Vec<(String, Value)> = entries
        .into_iter()
        .filter(|(_k, rec)| node_in_cluster(rec, &cluster))
        .collect();
    (
        engine::list_entries_response(ctx, meta, body, filtered),
        false,
    )
}

// ── Trial ⇄ trial-component association ───────────────────────────────────
//
// `AssociateTrialComponent` / `DisassociateTrialComponent` are Action verbs the
// generic arm discarded, so the association never round-tripped. Record the
// association on the trial-component record as an internal set of associated
// trial names (`__AssociatedTrials`, dropped by every output projection), and
// serve the scoped `ListTrialComponents(TrialName=…)` read from it.

const TRIAL_COMPONENT_FAMILY: &str = "TrialComponent";
const ASSOCIATED_TRIALS: &str = "__AssociatedTrials";

fn associate_trial_component(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let component = str_member(body, "TrialComponentName");
    let trial = str_member(body, "TrialName");
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        if let Some(key) = data.resolve_key(TRIAL_COMPONENT_FAMILY, &component) {
            if let Some(rec) = data.get_resource_mut(TRIAL_COMPONENT_FAMILY, &key) {
                if let Some(obj) = rec.as_object_mut() {
                    let arr = obj
                        .entry(ASSOCIATED_TRIALS.to_string())
                        .or_insert_with(|| Value::Array(Vec::new()));
                    if let Some(list) = arr.as_array_mut() {
                        if !list.iter().any(|v| v.as_str() == Some(trial.as_str())) {
                            list.push(Value::String(trial.clone()));
                        }
                    }
                }
            }
        }
    }
    (engine::action(ctx, meta, body), true)
}

fn disassociate_trial_component(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let component = str_member(body, "TrialComponentName");
    let trial = str_member(body, "TrialName");
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        if let Some(key) = data.resolve_key(TRIAL_COMPONENT_FAMILY, &component) {
            if let Some(rec) = data.get_resource_mut(TRIAL_COMPONENT_FAMILY, &key) {
                if let Some(list) = rec
                    .as_object_mut()
                    .and_then(|o| o.get_mut(ASSOCIATED_TRIALS))
                    .and_then(Value::as_array_mut)
                {
                    list.retain(|v| v.as_str() != Some(trial.as_str()));
                }
            }
        }
    }
    (engine::action(ctx, meta, body), true)
}

/// Scoped `ListTrialComponents`: when the request carries a `TrialName`, return
/// only the components associated with that trial (the `AssociateTrialComponent`
/// edge). Returns `None` when no `TrialName` filter is present so the caller
/// falls through to the generic, unfiltered list engine (unchanged behaviour).
fn list_trial_components(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Option<(AwsResponse, bool)> {
    let trial = body.get("TrialName").and_then(Value::as_str)?;
    let g = svc.state.read();
    let entries = g
        .get(&ctx.account)
        .map(|d| d.list_resource_entries(TRIAL_COMPONENT_FAMILY))
        .unwrap_or_default();
    let filtered: Vec<(String, Value)> = entries
        .into_iter()
        .filter(|(_k, rec)| {
            rec.as_object()
                .and_then(|o| o.get(ASSOCIATED_TRIALS))
                .and_then(Value::as_array)
                .is_some_and(|a| a.iter().any(|v| v.as_str() == Some(trial)))
        })
        .collect();
    Some((
        engine::list_entries_response(ctx, meta, body, filtered),
        false,
    ))
}

// ── Pipeline callback steps ──────────────────────────────────────────────
//
// `SendPipelineExecutionStepSuccess` / `SendPipelineExecutionStepFailure`
// resolve a pipeline execution's waiting callback step (identified by its
// `CallbackToken`). The generic arm discarded them, so the step never advanced.
// Persist / advance the step under the `PipelineExecutionStep` family keyed by
// the callback token: an existing waiting step transitions to Succeeded/Failed,
// and one is upserted otherwise. `ListPipelineExecutionSteps` projects the
// resulting `StepStatus` / `FailureReason` (both real `PipelineExecutionStep`
// fields); the token is the storage key, not a projected member.

const PIPELINE_STEP_FAMILY: &str = "PipelineExecutionStep";

fn send_pipeline_execution_step(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
    success: bool,
) -> (AwsResponse, bool) {
    let token = str_member(body, "CallbackToken");
    // The callback token is the only handle the caller holds (the execution arn
    // is embedded in it on real AWS); with no minted-token registry we derive a
    // stable execution arn from the token. It is stored on the step record so
    // `ListPipelineExecutionSteps(PipelineExecutionArn=…)` can scope to it, and
    // echoed in the response's required `PipelineExecutionArn`.
    let exec_arn = format!(
        "arn:aws:sagemaker:{}:{}:pipeline/callback/execution/{}",
        ctx.region, ctx.account, token
    );
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        let mut record = data
            .get_resource(PIPELINE_STEP_FAMILY, &token)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        record
            .entry("StepName".to_string())
            .or_insert_with(|| Value::String("Callback".to_string()));
        record.insert(
            "StepStatus".to_string(),
            Value::String(if success { "Succeeded" } else { "Failed" }.to_string()),
        );
        // `PipelineExecutionArn` is an internal scoping member (not a
        // `PipelineExecutionStep` output field, so the list projection drops it).
        record.insert(
            "PipelineExecutionArn".to_string(),
            Value::String(exec_arn.clone()),
        );
        if success {
            record.remove("FailureReason");
        } else if let Some(reason) = body.get("FailureReason") {
            record.insert("FailureReason".to_string(), reason.clone());
        }
        data.put_resource(PIPELINE_STEP_FAMILY, &token, Value::Object(record));
    }
    let mut aug = body.clone();
    aug.insert("PipelineExecutionArn".to_string(), Value::String(exec_arn));
    (engine::action(ctx, meta, &aug), true)
}

/// Scoped `ListPipelineExecutionSteps`: when the request carries a
/// `PipelineExecutionArn`, return only the callback steps stored for that
/// execution (the internal `PipelineExecutionArn` member
/// `send_pipeline_execution_step` records). Returns `None` when the filter is
/// absent so the caller falls through to the generic, unscoped list engine
/// (unchanged behaviour).
fn list_pipeline_execution_steps(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Option<(AwsResponse, bool)> {
    let arn = body.get("PipelineExecutionArn").and_then(Value::as_str)?;
    let g = svc.state.read();
    let entries = g
        .get(&ctx.account)
        .map(|d| d.list_resource_entries(PIPELINE_STEP_FAMILY))
        .unwrap_or_default();
    let filtered: Vec<(String, Value)> = entries
        .into_iter()
        .filter(|(_k, rec)| {
            rec.as_object()
                .and_then(|o| o.get("PipelineExecutionArn"))
                .and_then(Value::as_str)
                == Some(arn)
        })
        .collect();
    Some((
        engine::list_entries_response(ctx, meta, body, filtered),
        false,
    ))
}

/// Scoped `ListPipelineExecutions`: the operation carries a **required**
/// `PipelineName`, but the generic list engine ignores it and would return
/// every execution across all pipelines. Filter the `PipelineExecution` family
/// to the stored `PipelineName` (`StartPipelineExecution` records it). Returns
/// `None` when the filter is absent so the caller falls through to the generic
/// list engine (unchanged behaviour).
fn list_pipeline_executions(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Option<(AwsResponse, bool)> {
    let name = body.get("PipelineName").and_then(Value::as_str)?;
    let g = svc.state.read();
    let entries = g
        .get(&ctx.account)
        .map(|d| d.list_resource_entries("PipelineExecution"))
        .unwrap_or_default();
    let filtered: Vec<(String, Value)> = entries
        .into_iter()
        .filter(|(_k, rec)| {
            rec.as_object()
                .and_then(|o| o.get("PipelineName"))
                .and_then(Value::as_str)
                == Some(name)
        })
        .collect();
    Some((
        engine::list_entries_response(ctx, meta, body, filtered),
        false,
    ))
}

// ── Ops mis-mapped to a synthetic child family (parent-scoped) ────────────
//
// See the dispatch note above: the generator derived each of these ops'
// `family` slug from the op's own shape name, which no `Create*` populates, so
// the generic engine 404'd. The handlers below resolve the real PARENT family
// and project the op's own modelled output shape. When the parent is absent they
// return `ResourceNotFound` — matching the live API, and a conformance PASS
// (SageMaker's service-wide common error).

/// Feature-level metadata (`Description` / `Parameters`) is stored on the parent
/// `FeatureGroup` record under this internal member, keyed by feature name. It is
/// not a modelled `FeatureGroup` output member, so every projection drops it.
const FEATURE_META: &str = "__FeatureMetadata";
/// Per-schedule monitoring alerts, keyed by alert name, on the parent
/// `MonitoringSchedule` record (internal, dropped by projections).
const MONITORING_ALERTS: &str = "__MonitoringAlerts";
/// Per-pipeline version metadata, keyed by version id, on the parent `Pipeline`
/// record (internal, dropped by projections).
const PIPELINE_VERSIONS: &str = "__PipelineVersions";

/// Resolve `parent_family` by the caller's `ident_member`, apply `mutate` to the
/// existing parent record (bumping `LastModifiedTime`), and return the
/// operation's modelled output projected from the record — overlaying any output
/// member the request echoes that the parent record does not itself store (e.g.
/// `PipelineVersionId`, `MonitoringAlertName`). Returns a 404 `ResourceNotFound`
/// when the parent does not exist, matching the live API (these child-scoped ops
/// reject a missing parent) and keeping the conformance probe a PASS.
fn update_via_parent(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
    parent_family: &str,
    ident_member: &str,
    mutate: impl FnOnce(&mut Map<String, Value>),
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let ident = str_member(body, ident_member);
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let Some(key) = data.resolve_key(parent_family, &ident) else {
        return Err(super::not_found(format!(
            "Resource '{ident}' does not exist."
        )));
    };
    if let Some(obj) = data
        .get_resource_mut(parent_family, &key)
        .and_then(Value::as_object_mut)
    {
        mutate(obj);
        obj.insert("LastModifiedTime".to_string(), now_epoch());
    }
    let mut record = data
        .get_resource(parent_family, &key)
        .cloned()
        .unwrap_or(Value::Null);
    if let Some(obj) = record.as_object_mut() {
        for (wire, _kind) in meta.omembers {
            if !obj.contains_key(*wire) {
                if let Some(v) = body.get(*wire) {
                    obj.insert((*wire).to_string(), v.clone());
                }
            }
        }
    }
    let out = engine::build_output(ctx, meta, &key, &record);
    Ok((ok_json(out), true))
}

/// `UpdateEndpointWeightsAndCapacities` — merge each requested
/// `DesiredWeightsAndCapacities` entry into the parent `Endpoint` record's
/// `ProductionVariants` summary list (keyed by `VariantName`) so a subsequent
/// `DescribeEndpoint` reflects the new desired weight / instance count. Returns
/// the endpoint's `EndpointArn`.
fn update_endpoint_weights(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let desired = body
        .get("DesiredWeightsAndCapacities")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    update_via_parent(
        svc,
        ctx,
        meta,
        body,
        "Endpoint",
        "EndpointName",
        move |obj| {
            let mut variants = obj
                .get("ProductionVariants")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            for spec in desired.iter().filter_map(Value::as_object) {
                let vname = spec
                    .get("VariantName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let weight = spec.get("DesiredWeight").cloned();
                let count = spec.get("DesiredInstanceCount").cloned();
                let apply = |summary: &mut Map<String, Value>| {
                    if let Some(w) = weight.clone() {
                        summary.insert("CurrentWeight".to_string(), w.clone());
                        summary.insert("DesiredWeight".to_string(), w);
                    }
                    if let Some(c) = count.clone() {
                        summary.insert("CurrentInstanceCount".to_string(), c.clone());
                        summary.insert("DesiredInstanceCount".to_string(), c);
                    }
                };
                match variants.iter_mut().find(|v| {
                    v.as_object()
                        .and_then(|o| o.get("VariantName"))
                        .and_then(Value::as_str)
                        == Some(vname)
                }) {
                    Some(existing) => {
                        if let Some(o) = existing.as_object_mut() {
                            apply(o);
                        }
                    }
                    None => {
                        let mut summary = Map::new();
                        summary.insert("VariantName".to_string(), Value::String(vname.to_string()));
                        apply(&mut summary);
                        variants.push(Value::Object(summary));
                    }
                }
            }
            obj.insert("ProductionVariants".to_string(), Value::Array(variants));
        },
    )
}

/// `UpdateClusterSoftware` — a control-plane software update on the parent
/// `Cluster`. No queryable cluster attribute changes (the request carries only
/// the image / instance groups to patch), so the effect is the bumped
/// `LastModifiedTime`; returns the cluster's `ClusterArn`.
fn update_cluster_software(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    update_via_parent(svc, ctx, meta, body, "Cluster", "ClusterName", |_obj| {})
}

/// `UpdateInferenceComponentRuntimeConfig` — apply the requested
/// `DesiredRuntimeConfig.CopyCount` to the parent `InferenceComponent` record's
/// `RuntimeConfig` so a subsequent `DescribeInferenceComponent` reflects it.
/// Returns the component's `InferenceComponentArn`.
fn update_inference_component_runtime_config(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let copies = body
        .get("DesiredRuntimeConfig")
        .and_then(Value::as_object)
        .and_then(|o| o.get("CopyCount"))
        .and_then(Value::as_i64);
    update_via_parent(
        svc,
        ctx,
        meta,
        body,
        "InferenceComponent",
        "InferenceComponentName",
        move |obj| {
            if let Some(c) = copies {
                let mut rc = obj
                    .get("RuntimeConfig")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                rc.insert("DesiredCopyCount".to_string(), Value::from(c));
                rc.insert("CurrentCopyCount".to_string(), Value::from(c));
                obj.insert("RuntimeConfig".to_string(), Value::Object(rc));
            }
        },
    )
}

/// `UpdateMonitoringAlert` — upsert the named alert's `DatapointsToAlert` /
/// `EvaluationPeriod` onto the parent `MonitoringSchedule` record. Returns the
/// schedule's `MonitoringScheduleArn` plus the echoed `MonitoringAlertName`.
fn update_monitoring_alert(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let alert_name = str_member(body, "MonitoringAlertName");
    let datapoints = body.get("DatapointsToAlert").cloned();
    let period = body.get("EvaluationPeriod").cloned();
    update_via_parent(
        svc,
        ctx,
        meta,
        body,
        "MonitoringSchedule",
        "MonitoringScheduleName",
        move |obj| {
            let mut alerts = obj
                .get(MONITORING_ALERTS)
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let mut alert = alerts
                .get(&alert_name)
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            if let Some(d) = datapoints {
                alert.insert("DatapointsToAlert".to_string(), d);
            }
            if let Some(p) = period {
                alert.insert("EvaluationPeriod".to_string(), p);
            }
            alerts.insert(alert_name.clone(), Value::Object(alert));
            obj.insert(MONITORING_ALERTS.to_string(), Value::Object(alerts));
        },
    )
}

/// `UpdatePipelineVersion` — upsert the version's display name / description onto
/// the parent `Pipeline` record (resolved by `PipelineArn`), keyed by
/// `PipelineVersionId`. Returns the echoed `PipelineArn` + `PipelineVersionId`.
fn update_pipeline_version(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let version_key = body
        .get("PipelineVersionId")
        .map(ToString::to_string)
        .unwrap_or_default();
    let display = body.get("PipelineVersionDisplayName").cloned();
    let description = body.get("PipelineVersionDescription").cloned();
    update_via_parent(
        svc,
        ctx,
        meta,
        body,
        "Pipeline",
        "PipelineArn",
        move |obj| {
            let mut versions = obj
                .get(PIPELINE_VERSIONS)
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let mut version = versions
                .get(&version_key)
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            if let Some(d) = display {
                version.insert("PipelineVersionDisplayName".to_string(), d);
            }
            if let Some(d) = description {
                version.insert("PipelineVersionDescription".to_string(), d);
            }
            versions.insert(version_key.clone(), Value::Object(version));
            obj.insert(PIPELINE_VERSIONS.to_string(), Value::Object(versions));
        },
    )
}

/// The `FeatureType` a feature carries in its parent group's `FeatureDefinitions`.
fn feature_type_of(group: Option<&Map<String, Value>>, feature_name: &str) -> Option<String> {
    group?
        .get("FeatureDefinitions")?
        .as_array()?
        .iter()
        .find_map(|d| {
            let o = d.as_object()?;
            if o.get("FeatureName").and_then(Value::as_str) == Some(feature_name) {
                o.get("FeatureType")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            } else {
                None
            }
        })
}

/// `UpdateFeatureMetadata` — set a feature's `Description` and apply its
/// `ParameterAdditions` / `ParameterRemovals` on the parent `FeatureGroup`
/// record, so `DescribeFeatureMetadata` round-trips them. Output shape is empty.
fn update_feature_metadata(
    svc: &SageMakerService,
    ctx: &Ctx,
    _meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let group_name = str_member(body, "FeatureGroupName");
    let feature_name = str_member(body, "FeatureName");
    let description = body.get("Description").cloned();
    let additions = body
        .get("ParameterAdditions")
        .and_then(Value::as_array)
        .cloned();
    let removals = body
        .get("ParameterRemovals")
        .and_then(Value::as_array)
        .cloned();

    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let Some(key) = data.resolve_key("FeatureGroup", &group_name) else {
        return Err(super::not_found(format!(
            "Resource '{group_name}' does not exist."
        )));
    };
    if let Some(obj) = data
        .get_resource_mut("FeatureGroup", &key)
        .and_then(Value::as_object_mut)
    {
        let mut all = obj
            .get(FEATURE_META)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let mut fm = all
            .get(&feature_name)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Some(d) = description {
            fm.insert("Description".to_string(), d);
        }
        let mut params = fm
            .get("Parameters")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        // A key helper: a stored `{Key,Value}` parameter's key.
        let param_key = |p: &Value| -> Option<String> {
            p.as_object()
                .and_then(|o| o.get("Key"))
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        for add in additions.into_iter().flatten() {
            let Some(k) = param_key(&add) else { continue };
            params.retain(|p| param_key(p).as_deref() != Some(k.as_str()));
            params.push(add);
        }
        // `ParameterRemovals` is modelled as a list of parameter keys (strings);
        // tolerate a `{Key}` object form too.
        for rem in removals.into_iter().flatten() {
            let k = rem.as_str().map(str::to_string).or_else(|| param_key(&rem));
            if let Some(k) = k {
                params.retain(|p| param_key(p).as_deref() != Some(k.as_str()));
            }
        }
        fm.insert("Parameters".to_string(), Value::Array(params));
        all.insert(feature_name, Value::Object(fm));
        obj.insert(FEATURE_META.to_string(), Value::Object(all));
    }
    Ok((ok_json(Value::Object(Map::new())), true))
}

/// `DescribeFeatureMetadata` — read a feature's metadata back off the parent
/// `FeatureGroup` record: its `FeatureType` (from the group's
/// `FeatureDefinitions`) plus any `Description` / `Parameters`
/// `UpdateFeatureMetadata` stored, projected onto the op's output shape.
fn describe_feature_metadata(
    svc: &SageMakerService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let group_name = str_member(body, "FeatureGroupName");
    let feature_name = str_member(body, "FeatureName");
    let g = svc.state.read();
    let group = g
        .get(&ctx.account)
        .and_then(|d| d.resolve_key("FeatureGroup", &group_name).map(|k| (d, k)));
    let Some((data, key)) = group else {
        return Err(super::not_found(format!(
            "Resource '{group_name}' does not exist."
        )));
    };
    let record = data
        .get_resource("FeatureGroup", &key)
        .cloned()
        .unwrap_or(Value::Null);
    let gobj = record.as_object();

    let mut out = Map::new();
    if let Some(arn) = gobj.and_then(|o| o.get("FeatureGroupArn")).cloned() {
        out.insert("FeatureGroupArn".to_string(), arn);
    }
    out.insert(
        "FeatureGroupName".to_string(),
        Value::String(group_name.clone()),
    );
    out.insert(
        "FeatureName".to_string(),
        Value::String(feature_name.clone()),
    );
    if let Some(ft) = feature_type_of(gobj, &feature_name) {
        out.insert("FeatureType".to_string(), Value::String(ft));
    }
    if let Some(ct) = gobj.and_then(|o| o.get("CreationTime")).cloned() {
        out.insert("CreationTime".to_string(), ct);
    }
    if let Some(fm) = gobj
        .and_then(|o| o.get(FEATURE_META))
        .and_then(Value::as_object)
        .and_then(|m| m.get(&feature_name))
        .and_then(Value::as_object)
    {
        if let Some(d) = fm.get("Description") {
            out.insert("Description".to_string(), d.clone());
        }
        if let Some(p) = fm.get("Parameters") {
            out.insert("Parameters".to_string(), p.clone());
        }
    }
    // Complete any still-missing required output member (FeatureType default,
    // LastModifiedTime, ...) so the response is shape-valid.
    let out = engine::build_output(ctx, meta, &feature_name, &Value::Object(out));
    Ok((ok_json(out), false))
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
