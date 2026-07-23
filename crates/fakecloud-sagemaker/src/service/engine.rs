//! The generic resource engine: create / describe / list / update / delete for
//! every named SageMaker resource family. Each family is stored in the uniform
//! `resources[family][id]` map; the operation's metadata ([`OpMeta`]) supplies
//! the resource family, the identifier member (its storage key), the ARN path,
//! and the output / element member projection. Records persist their input
//! attributes plus any minted ARN / id / timestamps and are echoed on read.

use serde_json::{Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::generated::{Field, OpMeta, K};
use crate::state::SageMakerData;

use super::{in_use, mint_arn, not_found, now_epoch, ok_json, Ctx};

const DEFAULT_PAGE: usize = 100;

/// Enum values a terminal / healthy resource would plausibly report, preferred
/// over the raw first enum member when populating a server-derived status so a
/// synthesised Describe reads like a settled resource. Any listed value is a
/// valid enum member on the shapes that carry it; the fallback is the first
/// modelled value (always a valid member).
const PREFERRED_ENUMS: &[&str] = &[
    "Completed",
    "InService",
    "Available",
    "Active",
    "Succeeded",
    "Enabled",
    "Success",
    "Stopped",
    "Deleted",
];

/// Choose a valid value for an enum member: a preferred terminal/healthy state
/// if the enum offers one, else its first modelled value.
fn pick_enum(enums: &[&str]) -> String {
    for p in PREFERRED_ENUMS {
        if enums.contains(p) {
            return (*p).to_string();
        }
    }
    enums.first().copied().unwrap_or("Unknown").to_string()
}

/// Synthesise a shape-valid value for a required output member the stored record
/// does not carry. Strings prefer a derived ARN / name / id; enums a valid
/// member; numbers/timestamps a valid minimum; structures and non-empty lists
/// are built recursively from their own required members.
fn synth_field(ctx: &Ctx, meta: &OpMeta, key: &str, f: &Field) -> Value {
    match f.kind {
        K::Str | K::Blob => Value::String(synth_string(ctx, meta, key, f)),
        K::Ts => now_epoch(),
        K::Int => Value::from(f.min_val.unwrap_or(1)),
        K::Num => Value::from(f.min_val.unwrap_or(1) as f64),
        K::Bool => Value::Bool(true),
        K::Map => Value::Object(Map::new()),
        K::Struct if f.is_union => {
            // A union carries exactly one member: `f.children` holds that single
            // first member, synthesised (recursively) into a one-key object.
            let mut m = Map::new();
            if let Some(child) = f.children.first() {
                m.insert(child.wire.to_string(), synth_field(ctx, meta, key, child));
            }
            Value::Object(m)
        }
        K::Struct => {
            let mut m = Map::new();
            fill_required(ctx, meta, key, &mut m, f.children);
            Value::Object(m)
        }
        K::List => {
            // Emit one element only when the model requires a non-empty list
            // (`@length` min >= 1); otherwise a present-but-empty list already
            // satisfies the required-member check.
            if f.list_min.unwrap_or(0) >= 1 {
                Value::Array(vec![synth_element(ctx, meta, key, f)])
            } else {
                Value::Array(Vec::new())
            }
        }
    }
}

/// A single element for a required non-empty list.
fn synth_element(ctx: &Ctx, meta: &OpMeta, key: &str, f: &Field) -> Value {
    match f.elem_kind {
        K::Struct => {
            let mut m = Map::new();
            fill_required(ctx, meta, key, &mut m, f.children);
            Value::Object(m)
        }
        K::Str | K::Blob => {
            // `f.enums` carries the element's valid enum values (if any).
            if f.enums.is_empty() {
                Value::String("placeholder".to_string())
            } else {
                Value::String(pick_enum(f.enums))
            }
        }
        K::Ts => now_epoch(),
        K::Int => Value::from(1),
        K::Num => Value::from(1.0),
        K::Bool => Value::Bool(true),
        K::Map => Value::Object(Map::new()),
        K::List => Value::Array(Vec::new()),
    }
}

/// A plausible string for a required string member.
fn synth_string(ctx: &Ctx, meta: &OpMeta, key: &str, f: &Field) -> String {
    if !f.enums.is_empty() {
        return pick_enum(f.enums);
    }
    if f.wire.ends_with("Arn") {
        return mint_arn(ctx, meta.arn_path, key);
    }
    if f.wire.ends_with("Name") && !key.is_empty() {
        return key.to_string();
    }
    if f.wire.ends_with("Id") && !key.is_empty() {
        return key.to_string();
    }
    // A short placeholder honouring any `@length` minimum.
    let min = f.min_len.unwrap_or(1).max(1) as usize;
    "a".repeat(min.max(1))
}

/// Recursively complete `out` so every required member is present and of the
/// right type: a missing / null / wrong-typed member is replaced with a
/// synthesised default; a present structure or list element is descended into so
/// its own required sub-members are completed too (an echoed-but-incomplete
/// stored struct still projects a shape-valid response).
fn fill_required(
    ctx: &Ctx,
    meta: &OpMeta,
    key: &str,
    out: &mut Map<String, Value>,
    fields: &[Field],
) {
    for f in fields {
        let needs_default = match out.get(f.wire) {
            None => true,
            Some(v) => v.is_null() || !kind_matches(f.kind, v),
        };
        if needs_default {
            out.insert(f.wire.to_string(), synth_field(ctx, meta, key, f));
            continue;
        }
        // Present and correctly typed: descend to complete nested required
        // members. A present union is left as-is (it already carries a valid
        // member; descending could add a second and make it ambiguous).
        if let Some(v) = out.get_mut(f.wire) {
            match f.kind {
                K::Struct if f.is_union => {}
                K::Struct => {
                    if let Some(m) = v.as_object_mut() {
                        fill_required(ctx, meta, key, m, f.children);
                    }
                }
                K::List if f.elem_kind == K::Struct => {
                    if let Some(arr) = v.as_array_mut() {
                        for el in arr.iter_mut() {
                            if let Some(m) = el.as_object_mut() {
                                fill_required(ctx, meta, key, m, f.children);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// Whether a JSON value's type is compatible with a modelled member kind.
pub(crate) fn kind_matches(kind: K, v: &Value) -> bool {
    match kind {
        K::Str | K::Blob => v.is_string(),
        // awsJson1.1 timestamps wire-encode as epoch-second JSON numbers only; a
        // string (e.g. an ISO-8601 value a raw client posted into a timestamp
        // input member) is *not* a valid on-wire timestamp and would be rejected
        // by the SDK deserialiser. Such values are coerced to a number at
        // projection time (see `coerce_ts`), never echoed as a string.
        K::Ts => v.is_number(),
        K::Int | K::Num => v.is_number(),
        K::Bool => v.is_boolean(),
        K::List => v.is_array(),
        K::Map | K::Struct => v.is_object(),
    }
}

/// Coerce a stored timestamp value into the awsJson1.1 wire form (epoch-second
/// JSON number). A number passes through; a string is parsed as epoch seconds or
/// RFC3339 and converted; anything else yields `None` so the member is dropped
/// rather than emitted as an SDK-rejecting string.
fn coerce_ts(v: &Value) -> Option<Value> {
    if v.is_number() {
        return Some(v.clone());
    }
    let s = v.as_str()?;
    if let Ok(n) = s.parse::<f64>() {
        return Some(Value::from(n));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(Value::from(dt.timestamp_millis() as f64 / 1000.0));
    }
    None
}

/// Project a stored record's members onto the given `(wire, kind)` projection,
/// keeping members whose JSON type matches the modelled kind and coercing
/// timestamps to their numeric wire form.
fn project(obj: Option<&Map<String, Value>>, members: &[(&str, K)], out: &mut Map<String, Value>) {
    let Some(obj) = obj else { return };
    for (wire, kind) in members {
        let Some(v) = obj.get(*wire) else { continue };
        if v.is_null() {
            continue;
        }
        if *kind == K::Ts {
            if let Some(n) = coerce_ts(v) {
                out.insert((*wire).to_string(), n);
            }
        } else if kind_matches(*kind, v) {
            out.insert((*wire).to_string(), v.clone());
        }
    }
}

/// Project a stored record onto an operation's output members: keep members
/// present in the record whose JSON type matches the modelled kind, then fill
/// any still-missing `@required` output member with a synthesised default so the
/// response is valid against the model's output shape.
pub(crate) fn build_output(ctx: &Ctx, meta: &OpMeta, key: &str, record: &Value) -> Value {
    let mut out = Map::new();
    project(record.as_object(), meta.omembers, &mut out);
    fill_required(ctx, meta, key, &mut out, meta.req_out);
    Value::Object(out)
}

/// Project a record onto a list operation's element members, then fill any
/// still-missing `@required` element member (list elements carry required
/// fields the shape validator checks, e.g. a summary's status).
fn build_element(ctx: &Ctx, meta: &OpMeta, key: &str, record: &Value) -> Value {
    let mut out = Map::new();
    project(record.as_object(), meta.list_elems, &mut out);
    fill_required(ctx, meta, key, &mut out, meta.req_elem);
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
    let out = build_output(ctx, meta, &key, &record);
    data.put_resource(meta.family, &key, record);
    Ok(ok_json(out))
}

pub(super) fn update(
    data: &mut SageMakerData,
    ctx: &Ctx,
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
    let out = build_output(ctx, meta, &key, &record);
    data.put_resource(meta.family, &key, record);
    Ok(ok_json(out))
}

pub(super) fn delete(
    data: &mut SageMakerData,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> AwsResponse {
    let value = key_value(meta, body).unwrap_or_default();
    if let Some(key) = data.resolve_key(meta.family, &value) {
        data.remove_resource(meta.family, &key);
    }
    // AWS delete operations are idempotent: deleting an absent resource is a
    // success. Any required output members (e.g. an acknowledgement ARN or a
    // `Success` boolean) are synthesised from the caller-supplied identifier.
    ok_json(build_output(ctx, meta, &value, &Value::Object(Map::new())))
}

pub(super) fn get(
    data: Option<&SageMakerData>,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<AwsResponse, AwsServiceError> {
    let value = key_value(meta, body).unwrap_or_default();
    let resolved = data.and_then(|d| {
        d.resolve_key(meta.family, &value)
            .map(|k| (k.clone(), d.get_resource(meta.family, &k).cloned()))
    });
    match resolved {
        Some((key, Some(record))) => Ok(ok_json(build_output(ctx, meta, &key, &record))),
        _ => Err(not_found(format!("Resource '{value}' does not exist."))),
    }
}

/// A best-effort resource identifier from a request body, used to derive ARNs /
/// names for an action's synthesised output members: the first `*Arn`, else the
/// first `*Name` / `*Id`, else the first string member.
pub(super) fn action_key(body: &Map<String, Value>) -> String {
    let str_members: Vec<(&String, &str)> = body
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k, s)))
        .collect();
    for suffix in ["Arn", "Name", "Id"] {
        if let Some((_, s)) = str_members.iter().find(|(k, _)| k.ends_with(suffix)) {
            return (*s).to_string();
        }
    }
    str_members
        .first()
        .map(|(_, s)| (*s).to_string())
        .unwrap_or_default()
}

/// Handle any action operation not claimed by a resource-specific handler:
/// echo body members that match output members, then fill every required output
/// member (top-level or nested) with a synthesised default so the response is
/// valid against the model's output shape.
pub(super) fn action(ctx: &Ctx, meta: &OpMeta, body: &Map<String, Value>) -> AwsResponse {
    let key = action_key(body);
    let out = build_output(ctx, meta, &key, &Value::Object(body.clone()));
    ok_json(out)
}

/// Persist a resource whose *only* creation path is an Action verb (e.g. a
/// pipeline execution started by `StartPipelineExecution`), so the sibling
/// Describe / List / Update / Delete operations resolve it. Builds the standard
/// record — the caller-provided `seed` (request body plus any pre-minted ARNs /
/// status the handler set), completed with minted ARNs/ids and creation /
/// last-modified timestamps — stores it under `key` in the family, and returns
/// the action's declared output projection.
pub(super) fn action_create(
    data: &mut SageMakerData,
    ctx: &Ctx,
    meta: &OpMeta,
    key: &str,
    seed: &Map<String, Value>,
) -> AwsResponse {
    let record = build_record(ctx, meta, key, seed);
    let out = build_output(ctx, meta, key, &record);
    data.put_resource(meta.family, key, record);
    ok_json(out)
}

/// Apply the common request-side List filters an operation carries generically,
/// comparing against the stored record. Filters not present in `body` are
/// ignored; a record is kept only if it satisfies every present filter:
///
/// * `NameContains` — substring of the storage key or any `*Name` member.
/// * `CreationTimeAfter` / `CreationTimeBefore` — numeric bounds on the record's
///   `CreationTime` (a record without one fails a present bound).
/// * `StatusEquals` — equality against any `*Status` member the record carries
///   (a record that stores no status member is excluded when this filter is set,
///   since its state is not persisted).
///
/// Deliberately not mapped generically (skipped): sort (`SortBy`/`SortOrder`),
/// type/label filters keyed to a specific member name (`SourceType`,
/// `LabelingJobStatus`-style per-op enums already covered by `*Status`), and
/// resource-scoping members (e.g. `PipelineName` on `ListPipelineExecutions`)
/// whose semantics vary per op.
fn passes_filters(id: &str, record: &Value, body: &Map<String, Value>) -> bool {
    let obj = record.as_object();
    if let Some(needle) = body.get("NameContains").and_then(Value::as_str) {
        let hit = id.contains(needle)
            || obj
                .map(|o| {
                    o.iter().any(|(k, v)| {
                        k.ends_with("Name") && v.as_str().is_some_and(|s| s.contains(needle))
                    })
                })
                .unwrap_or(false);
        if !hit {
            return false;
        }
    }
    let creation = obj
        .and_then(|o| o.get("CreationTime"))
        .and_then(Value::as_f64);
    if let Some(after) = body.get("CreationTimeAfter").and_then(Value::as_f64) {
        if creation.map(|c| c >= after) != Some(true) {
            return false;
        }
    }
    if let Some(before) = body.get("CreationTimeBefore").and_then(Value::as_f64) {
        if creation.map(|c| c <= before) != Some(true) {
            return false;
        }
    }
    if let Some(status) = body.get("StatusEquals").and_then(Value::as_str) {
        let hit = obj
            .map(|o| {
                o.iter()
                    .any(|(k, v)| k.ends_with("Status") && v.as_str() == Some(status))
            })
            .unwrap_or(false);
        if !hit {
            return false;
        }
    }
    true
}

pub(super) fn list(
    data: Option<&SageMakerData>,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> AwsResponse {
    let mut entries = data
        .map(|d| d.list_resource_entries(meta.family))
        .unwrap_or_default();
    entries.retain(|(id, r)| passes_filters(id, r, body));
    list_entries_response(ctx, meta, body, entries)
}

/// Project a pre-filtered `(id, record)` entry set onto a list operation's
/// output: build each element (scalar id or projected object), apply the
/// request's `MaxResults` / `NextToken` pagination, and wrap in the op's list
/// field. Shared by the generic [`list`] path and resource-specific handlers
/// that scope the entry set themselves (e.g. `ListTrialComponents` filtered to
/// a single trial's associated components).
pub(super) fn list_entries_response(
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
    entries: Vec<(String, Value)>,
) -> AwsResponse {
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
            .map(|(id, r)| build_element(ctx, meta, id, r))
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
