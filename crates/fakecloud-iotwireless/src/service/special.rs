//! Resource-specific handlers that the generic engine cannot express: ARN-keyed
//! tagging, position configurations (a `Put`/`Get` pair keyed by resource
//! identifier), resource positions (a raw `@httpPayload` GeoJSON blob stored
//! per resource), and the wireless-device import-task lookup (which falls back
//! to the wireless-device store for the model's cross-resource read pairing).

use std::collections::HashMap;

use http::{HeaderMap, StatusCode};
use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::generated::OpMeta;

use super::{
    build_output, ok_json, query_get, resource_type, storage_key, Ctx, IotWirelessService,
};

type Handled = Result<Option<(AwsResponse, bool)>, AwsServiceError>;

#[allow(clippy::too_many_arguments)]
pub(super) fn dispatch(
    svc: &IotWirelessService,
    meta: &'static OpMeta,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    _headers: &HeaderMap,
    raw_body: &[u8],
    body: &Map<String, Value>,
) -> Handled {
    match meta.op {
        "TagResource" => Ok(Some(tag_resource(svc, ctx, query, body))),
        "UntagResource" => Ok(Some(untag_resource(svc, ctx, query))),
        "ListTagsForResource" => Ok(Some(list_tags(svc, ctx, query))),

        "PutPositionConfiguration" => Ok(Some(put_position_configuration(svc, ctx, labels, body))),
        "UpdateResourcePosition" => Ok(Some(update_resource_position(svc, ctx, labels, raw_body))),
        "GetResourcePosition" => Ok(Some(get_resource_position(svc, ctx, labels))),

        "GetWirelessDeviceImportTask" => Ok(Some(get_wireless_device_import_task(
            svc, meta, ctx, labels,
        )?)),

        _ => Ok(None),
    }
}

// ---------- tags ----------

fn tag_resource(
    svc: &IotWirelessService,
    ctx: &Ctx,
    query: &[(String, String)],
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let arn = query_get(query, "resourceArn").unwrap_or("").to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let entry = data.tags.entry(arn).or_default();
    if let Some(Value::Array(tags)) = body.get("Tags") {
        for t in tags {
            let k = t
                .get("Key")
                .or_else(|| t.get("key"))
                .and_then(Value::as_str);
            let v = t
                .get("Value")
                .or_else(|| t.get("value"))
                .and_then(Value::as_str);
            if let Some(k) = k {
                entry.insert(k.to_string(), v.unwrap_or("").to_string());
            }
        }
    }
    (ok_json(Value::Object(Map::new())), true)
}

fn untag_resource(
    svc: &IotWirelessService,
    ctx: &Ctx,
    query: &[(String, String)],
) -> (AwsResponse, bool) {
    let arn = query_get(query, "resourceArn").unwrap_or("").to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(entry) = data.tags.get_mut(&arn) {
        // `TagKeys` is a `@httpQuery` list: each key arrives as a repeated
        // query parameter of the same name.
        for (k, v) in query {
            if k == "tagKeys" {
                entry.remove(v);
            }
        }
    }
    (ok_json(Value::Object(Map::new())), true)
}

fn list_tags(
    svc: &IotWirelessService,
    ctx: &Ctx,
    query: &[(String, String)],
) -> (AwsResponse, bool) {
    let arn = query_get(query, "resourceArn").unwrap_or("");
    let g = svc.state.read();
    let tags: Vec<Value> = g
        .get(&ctx.account)
        .and_then(|d| d.tags.get(arn))
        .map(|m| {
            m.iter()
                .map(|(k, v)| json!({ "Key": k, "Value": v }))
                .collect()
        })
        .unwrap_or_default();
    (ok_json(json!({ "Tags": tags })), false)
}

// ---------- position configurations ----------

fn put_position_configuration(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let key = labels
        .get("ResourceIdentifier")
        .cloned()
        .unwrap_or_default();
    let mut record = body.clone();
    record.insert("ResourceIdentifier".to_string(), Value::String(key.clone()));
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.put_resource("position-configurations", &key, Value::Object(record));
    (ok_json(Value::Object(Map::new())), true)
}

// ---------- resource positions (raw @httpPayload GeoJSON blob) ----------

fn resource_position_key(labels: &HashMap<String, String>) -> String {
    format!(
        "resource-position:{}",
        labels
            .get("ResourceIdentifier")
            .map(String::as_str)
            .unwrap_or_default()
    )
}

fn update_resource_position(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    raw_body: &[u8],
) -> (AwsResponse, bool) {
    let key = resource_position_key(labels);
    let payload = String::from_utf8_lossy(raw_body).into_owned();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.blobs.insert(key, payload);
    (ok_json(Value::Object(Map::new())), true)
}

fn get_resource_position(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let key = resource_position_key(labels);
    let g = svc.state.read();
    let payload = g
        .get(&ctx.account)
        .and_then(|d| d.blobs.get(&key))
        .cloned()
        .unwrap_or_default();
    // `GeoJsonPayload` is an `@httpPayload` blob: the response body IS the raw
    // stored GeoJSON, not a JSON envelope.
    (
        AwsResponse::json(StatusCode::OK, payload.into_bytes()),
        false,
    )
}

// ---------- wireless-device import task ----------

/// The Smithy round-trip heuristic pairs `UpdateWirelessDevice` (which writes a
/// `wireless-devices` record) with `GetWirelessDeviceImportTask` by name
/// overlap. Read the import-task store first, then fall back to the
/// wireless-device store so that cross-resource pairing resolves.
fn get_wireless_device_import_task(
    svc: &IotWirelessService,
    meta: &OpMeta,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let key = storage_key(meta, labels);
    let rtype = resource_type(meta);
    let g = svc.state.read();
    let record = g.get(&ctx.account).and_then(|d| {
        d.get_resource(&rtype, &key)
            .or_else(|| d.get_resource("wireless-devices", &key))
            .cloned()
    });
    match record {
        Some(record) => Ok((ok_json(build_output(meta, &record)), false)),
        None => Err(super::engine::not_found(meta, &key)),
    }
}
