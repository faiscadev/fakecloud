//! The generic resource engine: create / describe / list / update / delete for
//! every named IoT resource family. Each family is stored in the uniform
//! `resources[type][id]` map; the operation's metadata ([`OpMeta`]) supplies
//! the resource type (its fixed URI path), the storage key (its label values),
//! and the output/element member projection. Records persist their input
//! attributes plus any minted ARN / id / timestamps and are echoed on read.

use std::collections::HashMap;

use http::StatusCode;
use serde_json::{Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::generated::{OpMeta, K};
use crate::state::IotData;

use super::{
    build_element, build_output, mint_arn, mint_uuid, now_iso, ok_json, query_get, resource_type,
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

/// Build the stored record for a create: the request body, overlaid with label
/// values and query parameters, plus minted ARN / id / timestamp output fields.
fn build_record(
    ctx: &Ctx,
    meta: &OpMeta,
    rtype: &str,
    key: &str,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    body: &Map<String, Value>,
) -> Value {
    let mut record: Map<String, Value> = body.clone();
    for (name, val) in labels {
        record.insert(name.clone(), Value::String(val.clone()));
    }
    for (k, v) in query {
        record
            .entry(k.clone())
            .or_insert_with(|| Value::String(v.clone()));
    }
    // The primary name is the last label component of the key.
    let primary = key.rsplit('/').next().unwrap_or(key);
    for (wire, kind) in meta.omembers {
        if record.contains_key(*wire) {
            continue;
        }
        match kind {
            K::Str if wire.ends_with("Arn") => {
                record.insert(
                    (*wire).to_string(),
                    Value::String(mint_arn(ctx, rtype, primary)),
                );
            }
            K::Str if wire.ends_with("Id") => {
                record.insert(
                    (*wire).to_string(),
                    Value::String(mint_uuid(&format!("{}:{}:{}", ctx.account, rtype, key))),
                );
            }
            K::Ts => {
                record.insert((*wire).to_string(), Value::String(now_iso()));
            }
            _ => {}
        }
    }
    Value::Object(record)
}

pub(super) fn create(
    data: &mut IotData,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    body: &Map<String, Value>,
) -> Result<AwsResponse, AwsServiceError> {
    let rtype = resource_type(meta);
    let key = storage_key(meta, labels);
    if data.get_resource(&rtype, &key).is_some() {
        if meta.errors.contains(&"ResourceAlreadyExistsException") {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceAlreadyExistsException",
                format!("The resource '{key}' already exists."),
            ));
        }
        if meta.errors.contains(&"ConflictException") {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ConflictException",
                format!("The resource '{key}' already exists."),
            ));
        }
        // No declared conflict error: treat create as idempotent overwrite.
    }
    let record = build_record(ctx, meta, &rtype, &key, labels, query, body);
    let out = build_output(meta, &record);
    data.put_resource(&rtype, &key, record);
    Ok(ok_json(out))
}

pub(super) fn update(
    data: &mut IotData,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
    body: &Map<String, Value>,
) -> Result<AwsResponse, AwsServiceError> {
    let rtype = resource_type(meta);
    let key = storage_key(meta, labels);
    let Some(mut record) = data.get_resource(&rtype, &key).cloned() else {
        return Err(not_found(meta, &key));
    };
    if let Some(obj) = record.as_object_mut() {
        for (k, v) in body {
            obj.insert(k.clone(), v.clone());
        }
        obj.insert("lastModifiedDate".to_string(), Value::String(now_iso()));
    }
    let out = build_output(meta, &record);
    data.put_resource(&rtype, &key, record);
    Ok(ok_json(out))
}

pub(super) fn delete(
    data: &mut IotData,
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

/// A handful of `Describe`/`Get` operations wrap the resource in a single
/// description struct (e.g. `DescribeCertificate` -> `certificateDescription`)
/// rather than returning its members at the top level. The stored record's
/// fields map onto that description shape.
fn wrapper_field(op: &str) -> Option<&'static str> {
    Some(match op {
        "DescribeCertificate" | "DescribeCACertificate" => "certificateDescription",
        "DescribeAuthorizer" => "authorizerDescription",
        "DescribeRoleAlias" => "roleAliasDescription",
        "DescribeStream" => "streamInfo",
        "DescribeJob" => "job",
        "DescribeCertificateProvider" | "DescribeManagedJobTemplate" => return None,
        _ => return None,
    })
}

pub(super) fn get(
    data: Option<&IotData>,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<AwsResponse, AwsServiceError> {
    let rtype = resource_type(meta);
    let key = storage_key(meta, labels);
    match data.and_then(|d| d.get_resource(&rtype, &key)) {
        Some(record) => {
            if let Some(field) = wrapper_field(meta.op) {
                let mut out = Map::new();
                out.insert(field.to_string(), record.clone());
                Ok(ok_json(Value::Object(out)))
            } else {
                Ok(ok_json(build_output(meta, record)))
            }
        }
        None => Err(not_found(meta, &key)),
    }
}

pub(super) fn list(
    data: Option<&IotData>,
    meta: &OpMeta,
    query: &[(String, String)],
) -> AwsResponse {
    let rtype = resource_type(meta);
    let records = data.map(|d| d.list_resources(&rtype)).unwrap_or_default();
    let elements: Vec<Value> = records.iter().map(|r| build_element(meta, r)).collect();

    let page_size = query_get(query, "maxResults")
        .or_else(|| query_get(query, "pageSize"))
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_PAGE);
    let start = query_get(query, "nextToken")
        .or_else(|| query_get(query, "marker"))
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let end = (start + page_size).min(elements.len());
    let page = elements.get(start..end).unwrap_or(&[]).to_vec();
    let has_next = end < elements.len();

    let mut out = Map::new();
    if let Some(field) = meta.list_field {
        out.insert(field.to_string(), Value::Array(page));
    }
    if has_next {
        if meta.omembers.iter().any(|(w, _)| *w == "nextToken") {
            out.insert("nextToken".to_string(), Value::String(end.to_string()));
        } else if meta.omembers.iter().any(|(w, _)| *w == "nextMarker") {
            out.insert("nextMarker".to_string(), Value::String(end.to_string()));
        }
    }
    ok_json(Value::Object(out))
}
