//! AWS IoT Data Plane (`iotdata`) restJson1 dispatch + handlers.
//!
//! The full 11-operation IoT Data Plane model. Requests are routed to an
//! operation by HTTP method + `@http` URI path; path labels are captured
//! positionally (percent-decoded) and query parameters are read from the raw
//! query string. Device-shadow and retained-message state is
//! account-partitioned and persisted.

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::shared;
use crate::state::{SharedIotDataState, CLASSIC_SHADOW_KEY};
use crate::validate;

/// Every operation name in the AWS IoT Data Plane Smithy model (11).
pub const IOTDATA_ACTIONS: &[&str] = &[
    "DeleteConnection",
    "DeleteThingShadow",
    "GetConnection",
    "GetRetainedMessage",
    "GetThingShadow",
    "ListNamedShadowsForThing",
    "ListRetainedMessages",
    "ListSubscriptions",
    "Publish",
    "SendDirectMessage",
    "UpdateThingShadow",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &["DeleteThingShadow", "Publish", "UpdateThingShadow"];

pub struct IotDataService {
    state: SharedIotDataState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl IotDataService {
    pub fn new(state: SharedIotDataState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
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

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
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

    /// Route a request to an operation name + captured path labels by HTTP
    /// method + `@http` URI path. Returns `None` when no route matches.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<String>)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let segs: Vec<String> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .split('/')
                .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
                .collect()
        };
        let s: Vec<&str> = segs.iter().map(String::as_str).collect();
        let m = &req.method;
        let get = m == Method::GET;
        let post = m == Method::POST;
        let del = m == Method::DELETE;
        let l1 = |a: &str| vec![a.to_string()];
        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            ["things", name, "shadow"] if get => ("GetThingShadow", l1(name)),
            ["things", name, "shadow"] if post => ("UpdateThingShadow", l1(name)),
            ["things", name, "shadow"] if del => ("DeleteThingShadow", l1(name)),
            ["api", "things", "shadow", "ListNamedShadowsForThing", name] if get => {
                ("ListNamedShadowsForThing", l1(name))
            }
            ["topics", topic] if post => ("Publish", l1(topic)),
            ["retainedMessage"] if get => ("ListRetainedMessages", vec![]),
            ["retainedMessage", topic] if get => ("GetRetainedMessage", l1(topic)),
            ["connections", client] if get => ("GetConnection", l1(client)),
            ["connections", client] if del => ("DeleteConnection", l1(client)),
            ["connections", client, "subscriptions"] if get => ("ListSubscriptions", l1(client)),
            ["connections", client, "messages"] if post => ("SendDirectMessage", l1(client)),
            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for IotDataService {
    fn service_name(&self) -> &str {
        "iotdata"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some((action, labels)) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let result = self.dispatch(action, &labels, &req);
        let success = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        if MUTATING.contains(&action) && success {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        IOTDATA_ACTIONS
    }
}

/// Per-request account context.
struct Ctx {
    account: String,
}

impl IotDataService {
    fn dispatch(
        &self,
        action: &str,
        labels: &[String],
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Reject a missing `@httpLabel`: an omitted required path parameter
        // arrives as the literal `{Label}` placeholder or an empty segment.
        for label in labels {
            if label.is_empty() || (label.starts_with('{') && label.ends_with('}')) {
                return Err(invalid_request(
                    "The request is missing a required path parameter.",
                ));
            }
        }
        let ctx = Ctx {
            account: req.account_id.clone(),
        };
        let q = parse_query(&req.raw_query);
        let a = |i: usize| labels.get(i).map(String::as_str).unwrap_or_default();
        match action {
            "GetThingShadow" => self.get_thing_shadow(&ctx, a(0), &q),
            "UpdateThingShadow" => self.update_thing_shadow(&ctx, a(0), &q, &req.body),
            "DeleteThingShadow" => self.delete_thing_shadow(&ctx, a(0), &q),
            "ListNamedShadowsForThing" => self.list_named_shadows(&ctx, a(0), &q),
            "Publish" => {
                let pfi = header_value(req, "x-amz-mqtt5-payload-format-indicator");
                self.publish(&ctx, a(0), &q, pfi.as_deref(), &req.body)
            }
            "GetRetainedMessage" => self.get_retained_message(&ctx, a(0)),
            "ListRetainedMessages" => self.list_retained_messages(&ctx, &q),
            // Connection-introspection operations. fakecloud runs no MQTT
            // broker, so no client is ever connected: every client id
            // faithfully resolves to `ResourceNotFoundException` (the
            // AWS-correct response for an unknown connection) rather than a
            // fabricated connection record.
            "GetConnection" | "DeleteConnection" | "ListSubscriptions" | "SendDirectMessage" => {
                Err(no_connection(a(0)))
            }
            _ => Err(AwsServiceError::action_not_implemented("iotdata", action)),
        }
    }

    // ---------- device shadows ----------

    fn get_thing_shadow(
        &self,
        ctx: &Ctx,
        thing: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate::validate_thing_name(thing)?;
        let shadow_name = shadow_key(q)?;
        let guard = self.state.read();
        let stored = guard
            .get(&ctx.account)
            .and_then(|d| d.shadows.get(thing))
            .and_then(|m| m.get(&shadow_name))
            .ok_or_else(no_shadow)?;
        payload_ok(&project_shadow(stored))
    }

    fn update_thing_shadow(
        &self,
        ctx: &Ctx,
        thing: &str,
        q: &[(String, String)],
        body: &[u8],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate::validate_thing_name(thing)?;
        let doc = validate::validate_shadow_update(body)?;
        let shadow_name = shadow_key(q)?;
        let requested_version = doc.get("version").and_then(Value::as_i64);
        let update_state = doc.get("state").cloned().unwrap_or_else(|| json!({}));

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let thing_shadows = data.shadows.entry(thing.to_string()).or_default();
        let existing = thing_shadows.get(&shadow_name).cloned();

        // Optimistic-locking: a supplied `version` must match the stored one.
        if let (Some(req_ver), Some(current)) = (requested_version, existing.as_ref()) {
            let cur_ver = current.get("version").and_then(Value::as_i64).unwrap_or(0);
            if req_ver != cur_ver {
                return Err(conflict(&format!(
                    "Version conflict: supplied {req_ver}, current {cur_ver}."
                )));
            }
        }

        let ts = shared::now_secs();
        let mut stored = existing.unwrap_or_else(|| {
            json!({
                "state": { "desired": {}, "reported": {} },
                "metadata": { "desired": {}, "reported": {} },
                "version": 0,
            })
        });

        for section in ["desired", "reported"] {
            if let Some(patch) = update_state.get(section) {
                // Merge state.
                let state_section = stored["state"]
                    .as_object_mut()
                    .expect("state object")
                    .entry(section.to_string())
                    .or_insert_with(|| json!({}));
                shared::merge_into(state_section, patch);
                // Stamp metadata.
                let meta_section = stored["metadata"]
                    .as_object_mut()
                    .expect("metadata object")
                    .entry(section.to_string())
                    .or_insert_with(|| json!({}));
                shared::stamp_metadata(meta_section, patch, ts);
            }
        }

        let new_version = stored.get("version").and_then(Value::as_i64).unwrap_or(0) + 1;
        stored["version"] = json!(new_version);
        stored["timestamp"] = json!(ts);

        let response = project_shadow(&stored);
        thing_shadows.insert(shadow_name, stored);
        payload_ok(&response)
    }

    fn delete_thing_shadow(
        &self,
        ctx: &Ctx,
        thing: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate::validate_thing_name(thing)?;
        let shadow_name = shadow_key(q)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(thing_shadows) = data.shadows.get_mut(thing) else {
            return Err(no_shadow());
        };
        let Some(removed) = thing_shadows.remove(&shadow_name) else {
            return Err(no_shadow());
        };
        if thing_shadows.is_empty() {
            data.shadows.remove(thing);
        }
        // AWS returns a small deletion payload carrying the deleted version and
        // a timestamp.
        let version = removed.get("version").and_then(Value::as_i64).unwrap_or(0);
        payload_ok(&json!({ "version": version, "timestamp": shared::now_secs() }))
    }

    fn list_named_shadows(
        &self,
        ctx: &Ctx,
        thing: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate::validate_thing_name(thing)?;
        let guard = self.state.read();
        let mut names: Vec<String> = guard
            .get(&ctx.account)
            .and_then(|d| d.shadows.get(thing))
            .map(|m| {
                m.keys()
                    .filter(|k| k.as_str() != CLASSIC_SHADOW_KEY)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        let page_size = parse_bounded(q, "pageSize", 1, 100, 100)?;
        let (page, next) = paginate(names, q, "nextToken", page_size)?;
        let mut out = Map::new();
        out.insert(
            "results".into(),
            Value::Array(page.into_iter().map(Value::String).collect()),
        );
        if let Some(n) = next {
            out.insert("nextToken".into(), json!(n));
        }
        out.insert("timestamp".into(), json!(shared::now_secs()));
        ok_json(Value::Object(out))
    }

    // ---------- publish + retained messages ----------

    fn publish(
        &self,
        ctx: &Ctx,
        topic: &str,
        q: &[(String, String)],
        payload_format_indicator: Option<&str>,
        body: &[u8],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate::validate_payload_format_indicator(payload_format_indicator)?;
        let qos = parse_bounded(q, "qos", 0, 1, 0)?;
        let retain = matches!(query_one(q, "retain"), Some("true"));
        if retain {
            let mut guard = self.state.write();
            let data = guard.get_or_create(&ctx.account);
            if body.is_empty() {
                // An empty retained payload clears the retained message for the
                // topic (AWS semantics).
                data.retained.remove(topic);
            } else {
                let encoded = base64::engine::general_purpose::STANDARD.encode(body);
                data.retained.insert(
                    topic.to_string(),
                    json!({
                        "topic": topic,
                        "payload": encoded,
                        "payloadSize": body.len() as i64,
                        "qos": qos,
                        "lastModifiedTime": shared::now_millis(),
                    }),
                );
            }
        }
        // Publish returns an empty body (`Unit` output). No live broker fan-out.
        Ok(AwsResponse::json(StatusCode::OK, ""))
    }

    fn get_retained_message(&self, ctx: &Ctx, topic: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let msg = guard
            .get(&ctx.account)
            .and_then(|d| d.retained.get(topic))
            .ok_or_else(|| no_retained(topic))?;
        let mut out = Map::new();
        out.insert("topic".into(), json!(topic));
        copy_present(&mut out, msg, "payload");
        copy_present(&mut out, msg, "qos");
        copy_present(&mut out, msg, "lastModifiedTime");
        ok_json(Value::Object(out))
    }

    fn list_retained_messages(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let mut items: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.retained
                    .values()
                    .map(|m| {
                        let mut summary = Map::new();
                        copy_present(&mut summary, m, "topic");
                        copy_present(&mut summary, m, "payloadSize");
                        copy_present(&mut summary, m, "qos");
                        copy_present(&mut summary, m, "lastModifiedTime");
                        Value::Object(summary)
                    })
                    .collect()
            })
            .unwrap_or_default();
        items.sort_by(|a, b| {
            a.get("topic")
                .and_then(Value::as_str)
                .cmp(&b.get("topic").and_then(Value::as_str))
        });
        let max = parse_bounded(q, "maxResults", 1, 200, 200)?;
        let (page, next) = paginate(items, q, "nextToken", max)?;
        let mut out = Map::new();
        out.insert("retainedTopics".into(), json!(page));
        if let Some(n) = next {
            out.insert("nextToken".into(), json!(n));
        }
        ok_json(Value::Object(out))
    }
}

// ===================== shadow projection =====================

/// Project the stored shadow record onto the full shadow-document wire shape:
/// `state` (desired / reported, plus a computed `delta`), `metadata`,
/// `version`, and `timestamp`. Empty `desired` / `reported` sections are
/// omitted, matching AWS.
fn project_shadow(stored: &Value) -> Value {
    let empty = Map::new();
    let state_in = stored
        .get("state")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let desired = state_in
        .get("desired")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let reported = state_in
        .get("reported")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let mut state_out = Map::new();
    if desired.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
        state_out.insert("desired".into(), desired.clone());
    }
    if reported.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
        state_out.insert("reported".into(), reported.clone());
    }
    if let Some(delta) = shared::compute_delta(&desired, &reported) {
        state_out.insert("delta".into(), delta);
    }

    let mut out = Map::new();
    out.insert("state".into(), Value::Object(state_out));
    if let Some(meta) = stored.get("metadata") {
        out.insert("metadata".into(), meta.clone());
    }
    if let Some(v) = stored.get("version") {
        out.insert("version".into(), v.clone());
    }
    if let Some(t) = stored.get("timestamp") {
        out.insert("timestamp".into(), t.clone());
    }
    Value::Object(out)
}

// ===================== helpers =====================

fn ok_json(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

/// A shadow / deletion payload response: the raw JSON document is the HTTP
/// body (the operation's `@httpPayload` blob).
fn payload_ok(v: &Value) -> Result<AwsResponse, AwsServiceError> {
    let bytes = serde_json::to_vec(v).expect("serde_json::Value serialization is infallible");
    Ok(AwsResponse::json(StatusCode::OK, bytes))
}

fn invalid_request(msg: &str) -> AwsServiceError {
    validate::invalid_request(msg)
}

fn conflict(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg)
}

fn no_shadow() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        "No shadow exists with name: ",
    )
}

fn no_retained(topic: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("No retained message found for topic '{topic}'."),
    )
}

fn no_connection(client: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("No connection found for client id '{client}'."),
    )
}

/// The shadow-name key from the `name` query param (validated when present),
/// or the classic-shadow key when absent.
fn shadow_key(q: &[(String, String)]) -> Result<String, AwsServiceError> {
    match query_one(q, "name") {
        Some(name) => {
            validate::validate_shadow_name(name)?;
            Ok(name.to_string())
        }
        None => Ok(CLASSIC_SHADOW_KEY.to_string()),
    }
}

/// Copy a member from `src` into `out` verbatim when present and non-null.
fn copy_present(out: &mut Map<String, Value>, src: &Value, key: &str) {
    if let Some(v) = src.get(key) {
        if !v.is_null() {
            out.insert(key.to_string(), v.clone());
        }
    }
}

/// Read a request header value as a `String`, if present and valid UTF-8.
fn header_value(req: &AwsRequest, name: &str) -> Option<String> {
    req.headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_decode_str(k).decode_utf8_lossy().into_owned(),
                percent_decode_str(v).decode_utf8_lossy().into_owned(),
            )
        })
        .collect()
}

fn query_one<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

/// Parse a bounded integer query parameter, defaulting when absent and
/// rejecting out-of-range values with `InvalidRequestException`.
fn parse_bounded(
    q: &[(String, String)],
    key: &str,
    min: i64,
    max: i64,
    default: i64,
) -> Result<i64, AwsServiceError> {
    match query_one(q, key) {
        None => Ok(default),
        Some(v) => {
            let n: i64 = v
                .parse()
                .map_err(|_| invalid_request(&format!("{key} must be an integer.")))?;
            if !(min..=max).contains(&n) {
                return Err(invalid_request(&format!(
                    "{key} must be between {min} and {max}."
                )));
            }
            Ok(n)
        }
    }
}

/// Paginate a list using a numeric-offset `nextToken` that round-trips.
fn paginate<T: Clone>(
    items: Vec<T>,
    q: &[(String, String)],
    token_key: &str,
    max: i64,
) -> Result<(Vec<T>, Option<String>), AwsServiceError> {
    let start = query_one(q, token_key)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let max = max as usize;
    let end = (start + max).min(items.len());
    let page: Vec<T> = items.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    Ok((page, next))
}

#[cfg(test)]
mod tests;
