//! Resource-specific SageMaker handlers that fall outside the generic resource
//! engine: tagging (`AddTags` / `ListTags` / `DeleteTags`), which persists tags
//! keyed by resource ARN.

use serde_json::{Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::generated::OpMeta;

use super::{ok_json, Ctx, SageMakerService};

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
        _ => Ok(None),
    }
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
