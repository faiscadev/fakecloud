//! The generic resource engine: create / describe / list / update / delete for
//! every named SageMaker resource family. Each family is stored in the uniform
//! `resources[family][id]` map; the operation's metadata ([`OpMeta`]) supplies
//! the resource family, the identifier member (its storage key), the ARN path,
//! and the output / element member projection. Records persist their input
//! attributes plus any minted ARN / id / timestamps and are echoed on read.

use serde_json::{Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::generated::{OpMeta, K};
use crate::state::SageMakerData;

use super::{in_use, mint_arn, not_found, now_epoch, ok_json, Ctx};

const DEFAULT_PAGE: usize = 100;

/// Whether a JSON value's type is compatible with a modelled member kind.
pub(crate) fn kind_matches(kind: K, v: &Value) -> bool {
    match kind {
        K::Str | K::Blob => v.is_string(),
        // awsJson1.1 timestamps wire-encode as epoch-second JSON numbers; accept
        // a string too so any legacy/ISO stored value still projects.
        K::Ts => v.is_string() || v.is_number(),
        K::Int | K::Num => v.is_number(),
        K::Bool => v.is_boolean(),
        K::List => v.is_array(),
        K::Map | K::Struct => v.is_object(),
    }
}

/// Project a stored record onto an operation's output members: keep only
/// members present in the record whose JSON type matches the modelled kind.
pub(crate) fn build_output(meta: &OpMeta, record: &Value) -> Value {
    let mut out = Map::new();
    if let Some(obj) = record.as_object() {
        for (wire, kind) in meta.omembers {
            if let Some(v) = obj.get(*wire) {
                if !v.is_null() && kind_matches(*kind, v) {
                    out.insert((*wire).to_string(), v.clone());
                }
            }
        }
    }
    Value::Object(out)
}

/// Project a record onto a list operation's element members.
fn build_element(meta: &OpMeta, record: &Value) -> Value {
    let mut out = Map::new();
    if let Some(obj) = record.as_object() {
        for (wire, kind) in meta.list_elems {
            if let Some(v) = obj.get(*wire) {
                if !v.is_null() && kind_matches(*kind, v) {
                    out.insert((*wire).to_string(), v.clone());
                }
            }
        }
    }
    Value::Object(out)
}

/// The caller-supplied identifier value for the operation's key member.
fn key_value(meta: &OpMeta, body: &Map<String, Value>) -> Option<String> {
    if meta.key_member.is_empty() {
        return None;
    }
    body.get(meta.key_member)
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Build the stored record for a create: the request body, plus a minted ARN
/// for the family, minted id / arn output members, and creation / last-modified
/// timestamps.
fn build_record(ctx: &Ctx, meta: &OpMeta, key: &str, body: &Map<String, Value>) -> Value {
    let mut record: Map<String, Value> = body.clone();

    // Mint minted-looking output members (ARNs, ids, timestamps) that the
    // request did not supply.
    for (wire, kind) in meta.omembers {
        if record.contains_key(*wire) {
            continue;
        }
        match kind {
            K::Str if wire.ends_with("Arn") => {
                record.insert(
                    (*wire).to_string(),
                    Value::String(mint_arn(ctx, meta.arn_path, key)),
                );
            }
            K::Str if wire.ends_with("Id") => {
                record.insert(
                    (*wire).to_string(),
                    Value::String(super::mint_id(&ctx.account, meta.family, key)),
                );
            }
            K::Ts => {
                record.insert((*wire).to_string(), now_epoch());
            }
            _ => {}
        }
    }

    // The primary ARN and standard timestamps are present on every Describe*
    // output even when the create output only returns the ARN.
    let primary_arn = format!("{}Arn", meta.family);
    record
        .entry(primary_arn)
        .or_insert_with(|| Value::String(mint_arn(ctx, meta.arn_path, key)));
    let now = now_epoch();
    record
        .entry("CreationTime".to_string())
        .or_insert_with(|| now.clone());
    record.entry("LastModifiedTime".to_string()).or_insert(now);

    Value::Object(record)
}

pub(super) fn create(
    data: &mut SageMakerData,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<AwsResponse, AwsServiceError> {
    let key = match key_value(meta, body) {
        Some(k) => k,
        // No caller-supplied identifier (e.g. an optional name): mint one.
        None => super::mint_id(&ctx.account, meta.family, &data.next_seq().to_string()),
    };
    if data.get_resource(meta.family, &key).is_some() {
        if meta.errors.contains(&"ResourceInUse") {
            return Err(in_use(format!("Resource '{key}' already exists.")));
        }
        if meta.errors.contains(&"ConflictException") {
            return Err(AwsServiceError::aws_error(
                http::StatusCode::CONFLICT,
                "ConflictException",
                format!("Resource '{key}' already exists."),
            ));
        }
        // No declared conflict error: treat create as idempotent overwrite.
    }
    let record = build_record(ctx, meta, &key, body);
    let out = build_output(meta, &record);
    data.put_resource(meta.family, &key, record);
    Ok(ok_json(out))
}

pub(super) fn update(
    data: &mut SageMakerData,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<AwsResponse, AwsServiceError> {
    let value = key_value(meta, body).unwrap_or_default();
    let Some(key) = data.resolve_key(meta.family, &value) else {
        return Err(not_found(format!("Resource '{value}' does not exist.")));
    };
    let mut record = data
        .get_resource(meta.family, &key)
        .cloned()
        .unwrap_or(Value::Null);
    if let Some(obj) = record.as_object_mut() {
        for (k, v) in body {
            obj.insert(k.clone(), v.clone());
        }
        obj.insert("LastModifiedTime".to_string(), now_epoch());
    }
    let out = build_output(meta, &record);
    data.put_resource(meta.family, &key, record);
    Ok(ok_json(out))
}

pub(super) fn delete(
    data: &mut SageMakerData,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> AwsResponse {
    let value = key_value(meta, body).unwrap_or_default();
    if let Some(key) = data.resolve_key(meta.family, &value) {
        data.remove_resource(meta.family, &key);
    }
    // AWS delete operations are idempotent: deleting an absent resource is a
    // success. The output shapes carry no required members.
    ok_json(build_output(meta, &Value::Object(Map::new())))
}

pub(super) fn get(
    data: Option<&SageMakerData>,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<AwsResponse, AwsServiceError> {
    let value = key_value(meta, body).unwrap_or_default();
    let record = data.and_then(|d| {
        d.resolve_key(meta.family, &value)
            .and_then(|k| d.get_resource(meta.family, &k).cloned())
    });
    match record {
        Some(record) => Ok(ok_json(build_output(meta, &record))),
        None => Err(not_found(format!("Resource '{value}' does not exist."))),
    }
}

pub(super) fn list(
    data: Option<&SageMakerData>,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> AwsResponse {
    let entries = data
        .map(|d| d.list_resource_entries(meta.family))
        .unwrap_or_default();

    // A `list<string>` element serialises as the resource's identifier string
    // (its storage key); otherwise each element is the projected object.
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

    let page_size = body
        .get("MaxResults")
        .and_then(Value::as_u64)
        .map(|n| n as usize)
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_PAGE);
    let start = body
        .get("NextToken")
        .and_then(Value::as_str)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let end = (start + page_size).min(elements.len());
    let page = elements.get(start..end).unwrap_or(&[]).to_vec();
    let has_next = end < elements.len();

    let mut out = Map::new();
    if let Some(field) = meta.list_field {
        out.insert(field.to_string(), Value::Array(page));
    }
    if has_next && meta.omembers.iter().any(|(w, _)| *w == "NextToken") {
        out.insert("NextToken".to_string(), Value::String(end.to_string()));
    }
    ok_json(Value::Object(out))
}
