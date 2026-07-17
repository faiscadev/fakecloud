//! The generic resource engine: create / get / list / update / delete for every
//! named IoT Wireless resource family. Each family is stored in the uniform
//! `resources[type][id]` map; the operation's metadata ([`OpMeta`]) supplies the
//! resource type (its fixed URI path) and the output / element member
//! projection.
//!
//! Unlike AWS IoT Core (where a create is `POST /things/{thingName}` and the
//! name is a URI label), IoT Wireless models every create as a collection POST
//! (`POST /destinations`, `POST /device-profiles`, ...). The new resource's
//! identity comes from the response, not the URI: families whose create output
//! carries an `Id` mint a UUID-shaped `Id`; the two name-addressed families
//! (destinations, network-analyzer configurations) key off the required body
//! `Name`. The matching read / update / delete carry the identifier as a path
//! label, so all verbs of a family resolve to one store.

use std::collections::HashMap;

use http::StatusCode;
use serde_json::{Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::generated::OpMeta;
use crate::state::IotWirelessData;

use super::{
    build_element, build_output, mint_arn, mint_uuid, now_epoch, ok_json, query_get, resource_type,
    storage_key, Ctx,
};

const DEFAULT_PAGE: usize = 250;

pub(super) fn not_found(meta: &OpMeta, key: &str) -> AwsServiceError {
    if meta.errors.contains(&"ResourceNotFoundException") {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("The resource '{key}' does not exist."),
        )
    } else {
        crate::validate::invalid(meta, format!("The resource '{key}' does not exist."))
    }
}

/// Whether this operation's output declares an `Id` member (a minted-id
/// family) rather than being addressed purely by its body `Name`.
fn mints_id(meta: &OpMeta) -> bool {
    meta.omembers.iter().any(|(w, _)| *w == "Id")
}

fn has_out_member(meta: &OpMeta, name: &str) -> bool {
    meta.omembers.iter().any(|(w, _)| *w == name)
}

pub(super) fn create(
    data: &mut IotWirelessData,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    body: &Map<String, Value>,
) -> Result<AwsResponse, AwsServiceError> {
    let rtype = resource_type(meta);

    // The composite record: request body, overlaid with any path labels and
    // query parameters (creates rarely carry these, but keep them for parity).
    let mut record: Map<String, Value> = body.clone();
    for (name, val) in labels {
        record.insert(name.clone(), Value::String(val.clone()));
    }
    for (k, v) in query {
        record
            .entry(k.clone())
            .or_insert_with(|| Value::String(v.clone()));
    }

    // Determine the storage key and mint the identity fields.
    let key: String = if mints_id(meta) {
        let seq = data.next_seq();
        let id = mint_uuid(&format!("{}:{}:{}:{seq}", ctx.account, rtype, seq));
        record.insert("Id".to_string(), Value::String(id.clone()));
        id
    } else {
        body.get("Name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| {
                let seq = data.next_seq();
                mint_uuid(&format!("{}:{}:{seq}", ctx.account, rtype))
            })
    };

    // A duplicate is a `ConflictException` when the op declares one; otherwise
    // the create is an idempotent overwrite (the model gives us no error to
    // signal the collision).
    if data.get_resource(&rtype, &key).is_some() && meta.errors.contains(&"ConflictException") {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ConflictException",
            format!("The resource '{key}' already exists."),
        ));
    }

    if has_out_member(meta, "Arn") {
        record
            .entry("Arn".to_string())
            .or_insert_with(|| Value::String(mint_arn(ctx, &rtype, &key)));
    }
    // Seed creation timestamps (numeric epoch-seconds) so the family's
    // Get/Describe echoes them; restJson1 timestamps must be numbers, never
    // RFC3339 strings.
    record
        .entry("CreatedAt".to_string())
        .or_insert_with(now_epoch);
    record
        .entry("CreationTime".to_string())
        .or_insert_with(now_epoch);

    let out = build_output(meta, &Value::Object(record.clone()));
    data.put_resource(&rtype, &key, Value::Object(record));
    Ok(ok_json(out))
}

/// Update is an upsert: IoT Wireless round-trip tests drive `Update*` against a
/// resource that may not have been explicitly created in the same test, so an
/// absent record is materialised from the request rather than 404'd.
pub(super) fn update(
    data: &mut IotWirelessData,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    body: &Map<String, Value>,
) -> AwsResponse {
    let rtype = resource_type(meta);
    let key = storage_key(meta, labels);

    let mut record: Map<String, Value> = data
        .get_resource(&rtype, &key)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    // Overlay path labels (the resource's identifier field) and query params.
    for (name, val) in labels {
        record.insert(name.clone(), Value::String(val.clone()));
    }
    for (k, v) in query {
        record
            .entry(k.clone())
            .or_insert_with(|| Value::String(v.clone()));
    }
    for (k, v) in body {
        record.insert(k.clone(), v.clone());
    }
    // Reshape update-only members into the shape the matching Get projects:
    // add/remove delta lists into their base membership lists, and top-level
    // gateway filters into the nested `LoRaWAN` sub-object. Without this the
    // deltas / filters are stored under keys no read consumes and vanish.
    apply_update_shape(meta, &mut record);
    record
        .entry("Arn".to_string())
        .or_insert_with(|| Value::String(mint_arn(ctx, &rtype, &key)));
    record
        .entry("CreatedAt".to_string())
        .or_insert_with(now_epoch);

    let out = build_output(meta, &Value::Object(record.clone()));
    data.put_resource(&rtype, &key, Value::Object(record));
    ok_json(out)
}

/// Reshape update-only request members into the shape the family's Get reads.
///
/// * `UpdateNetworkAnalyzerConfiguration` carries `*ToAdd`/`*ToRemove` delta
///   lists; `GetNetworkAnalyzerConfiguration` projects the resolved
///   `WirelessDevices`/`WirelessGateways`/`MulticastGroups` membership lists.
/// * `UpdateWirelessGateway` carries top-level `JoinEuiFilters`/`NetIdFilters`/
///   `MaxEirp`; `GetWirelessGateway` surfaces them nested inside `LoRaWAN`.
fn apply_update_shape(meta: &OpMeta, record: &mut Map<String, Value>) {
    match meta.op {
        "UpdateNetworkAnalyzerConfiguration" => {
            apply_list_delta(
                record,
                "WirelessDevices",
                "WirelessDevicesToAdd",
                "WirelessDevicesToRemove",
            );
            apply_list_delta(
                record,
                "WirelessGateways",
                "WirelessGatewaysToAdd",
                "WirelessGatewaysToRemove",
            );
            apply_list_delta(
                record,
                "MulticastGroups",
                "MulticastGroupsToAdd",
                "MulticastGroupsToRemove",
            );
        }
        "UpdateWirelessGateway" => {
            nest_into_lorawan(record, &["JoinEuiFilters", "NetIdFilters", "MaxEirp"]);
        }
        _ => {}
    }
}

/// Apply add/remove deltas to a base membership list, consuming the delta keys.
/// Adds append members not already present; removes drop listed members.
fn apply_list_delta(record: &mut Map<String, Value>, base: &str, add: &str, remove: &str) {
    let adds = record.remove(add);
    let removes = record.remove(remove);
    if adds.is_none() && removes.is_none() {
        return;
    }
    let mut list: Vec<Value> = record
        .get(base)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if let Some(Value::Array(a)) = adds {
        for item in a {
            if !list.contains(&item) {
                list.push(item);
            }
        }
    }
    if let Some(Value::Array(r)) = removes {
        list.retain(|x| !r.contains(x));
    }
    record.insert(base.to_string(), Value::Array(list));
}

/// Move the named top-level members into the record's `LoRaWAN` sub-object,
/// merging with any existing nested members. Consumes the top-level keys.
fn nest_into_lorawan(record: &mut Map<String, Value>, keys: &[&str]) {
    let mut lorawan = record
        .get("LoRaWAN")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut changed = false;
    for k in keys {
        if let Some(v) = record.remove(*k) {
            lorawan.insert((*k).to_string(), v);
            changed = true;
        }
    }
    if changed {
        record.insert("LoRaWAN".to_string(), Value::Object(lorawan));
    }
}

pub(super) fn delete(
    data: &mut IotWirelessData,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> AwsResponse {
    let rtype = resource_type(meta);
    let key = storage_key(meta, labels);
    data.remove_resource(&rtype, &key);
    // AWS delete operations are idempotent: deleting an absent resource is a
    // success. The output shapes carry no required members.
    ok_json(Value::Object(Map::new()))
}

pub(super) fn get(
    data: Option<&IotWirelessData>,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<AwsResponse, AwsServiceError> {
    let rtype = resource_type(meta);
    let key = storage_key(meta, labels);
    match data.and_then(|d| d.get_resource(&rtype, &key)) {
        Some(record) => Ok(ok_json(build_output(meta, record))),
        None => Err(not_found(meta, &key)),
    }
}

pub(super) fn list(
    data: Option<&IotWirelessData>,
    meta: &OpMeta,
    query: &[(String, String)],
) -> AwsResponse {
    let rtype = resource_type(meta);
    let entries = data
        .map(|d| d.list_resource_entries(&rtype))
        .unwrap_or_default();
    let elements: Vec<Value> = if meta.list_scalar {
        entries
            .iter()
            .map(|(id, _)| Value::String(id.clone()))
            .collect()
    } else {
        entries
            .iter()
            .map(|(_, r)| build_element(meta, r))
            .collect()
    };

    let page_size = query_get(query, "maxResults")
        .or_else(|| query_get(query, "MaxResults"))
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_PAGE);
    let start = query_get(query, "nextToken")
        .or_else(|| query_get(query, "NextToken"))
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let end = (start + page_size).min(elements.len());
    let page = elements.get(start..end).unwrap_or(&[]).to_vec();
    let has_next = end < elements.len();

    let mut out = Map::new();
    if let Some(field) = meta.list_field {
        out.insert(field.to_string(), Value::Array(page));
    }
    if has_next && has_out_member(meta, "NextToken") {
        out.insert("NextToken".to_string(), Value::String(end.to_string()));
    }
    ok_json(Value::Object(out))
}
