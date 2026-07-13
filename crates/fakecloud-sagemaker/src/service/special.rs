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
        _ => Ok(None),
    }
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
