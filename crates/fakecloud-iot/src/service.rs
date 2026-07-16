//! AWS IoT Core (`iot`) restJson1 dispatch.
//!
//! Requests are routed to one of the 272 modelled operations by HTTP method +
//! `@http` URI template (the route table in [`crate::generated`]). Path labels
//! are captured positionally (percent-decoded). Input is validated against the
//! model-derived constraints ([`crate::validate`]), then handled either by the
//! generic resource engine ([`engine`], for the create / describe / list /
//! update / delete verb of every named resource family) or by a resource-
//! specific handler ([`special`], for certificate minting, endpoints,
//! singleton configurations, tagging, and relationship operations). State is
//! account-partitioned and persisted.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::generated::{OpMeta, Seg, Verb, K, OPS};
use crate::persistence::save_snapshot;
use crate::state::SharedIotState;
use crate::validate::{self, Inputs};

mod engine;
mod special;
#[cfg(test)]
mod tests;

/// Every operation name in the AWS IoT Core Smithy model (272).
pub use crate::generated::ACTIONS as IOT_ACTIONS;

pub struct IotService {
    state: SharedIotState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl IotService {
    pub fn new(state: SharedIotState) -> Self {
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
}

#[async_trait]
impl AwsService for IotService {
    fn service_name(&self) -> &str {
        "iot"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some((meta, labels)) = match_route(&req.method, &req.raw_path) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let result = self.dispatch(meta, &labels, &req);
        match result {
            Ok((resp, mutated)) => {
                if mutated && resp.status.is_success() {
                    self.save().await;
                }
                Ok(resp)
            }
            Err(e) => Err(e),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        IOT_ACTIONS
    }
}

/// Per-request context.
pub(crate) struct Ctx {
    pub account: String,
    pub region: String,
}

impl IotService {
    fn dispatch(
        &self,
        meta: &'static OpMeta,
        labels: &HashMap<String, String>,
        req: &AwsRequest,
    ) -> Result<(AwsResponse, bool), AwsServiceError> {
        // A missing required label arrives as the literal `{Name}` placeholder.
        for v in labels.values() {
            if v.is_empty() || (v.starts_with('{') && v.ends_with('}')) {
                return Err(validate::invalid(
                    meta,
                    "The request is missing a required path parameter.",
                ));
            }
        }
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: if req.region.is_empty() {
                "us-east-1".to_string()
            } else {
                req.region.clone()
            },
        };
        let query = parse_query(&req.raw_query);
        let body = parse_body(&req.body);
        let inputs = Inputs {
            labels,
            query: &query,
            headers: &req.headers,
            body: &body,
        };
        validate::validate(meta, &inputs)?;

        // Resource-specific handlers take precedence over the generic engine.
        if let Some(res) = special::dispatch(self, meta, &ctx, labels, &query, &req.headers, &body)?
        {
            return Ok(res);
        }

        // Generic resource engine by verb.
        match meta.verb {
            Verb::Create => {
                let mut g = self.state.write();
                let data = g.get_or_create(&ctx.account);
                Ok((
                    engine::create(data, &ctx, meta, labels, &query, &body)?,
                    true,
                ))
            }
            Verb::Update => {
                let mut g = self.state.write();
                let data = g.get_or_create(&ctx.account);
                Ok((engine::update(data, meta, labels, &body)?, true))
            }
            Verb::Delete => {
                let mut g = self.state.write();
                let data = g.get_or_create(&ctx.account);
                Ok((engine::delete(data, meta, labels), true))
            }
            Verb::Get => {
                let g = self.state.read();
                let data = g.get(&ctx.account);
                Ok((engine::get(data, meta, labels)?, false))
            }
            Verb::List => {
                let g = self.state.read();
                let data = g.get(&ctx.account);
                Ok((engine::list(data, meta, &query), false))
            }
            Verb::Action => {
                // Read-only actions not claimed by `special` return an empty
                // (shape-valid) body. An unclaimed *mutating* action must fail
                // loud rather than silently accept-and-discard, so future model
                // gaps surface instead of appearing to persist.
                if is_read_only_action(meta) {
                    Ok((ok_json(Value::Object(Map::new())), false))
                } else {
                    Err(AwsServiceError::action_not_implemented("iot", meta.op))
                }
            }
        }
    }
}

// ===================== routing =====================

/// Match a request `(method, path)` against the generated route table,
/// returning the operation metadata and captured labels (member name -> value).
/// Ties (a fixed segment vs a label segment at the same position) are broken in
/// favour of the route with more fixed segments — the more specific route.
pub(crate) fn match_route(
    method: &Method,
    raw_path: &str,
) -> Option<(&'static OpMeta, HashMap<String, String>)> {
    let raw = raw_path.split('?').next().unwrap_or(raw_path);
    let raw = raw.strip_prefix('/').unwrap_or(raw);
    // Strip a single trailing slash so `/things/` routes to the 1-segment
    // ListThings rather than the 2-segment DescribeThing with an empty
    // thingName (which 404s). Mirrors the sibling routers (EKS/opensearch/
    // scheduler). A bare `/` collapses to the empty path handled below.
    let trimmed = raw.strip_suffix('/').unwrap_or(raw);
    let segs: Vec<String> = if trimmed.is_empty() {
        Vec::new()
    } else {
        trimmed
            .split('/')
            .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
            .collect()
    };
    let method_str = method.as_str();

    let mut best: Option<(&'static OpMeta, HashMap<String, String>, usize)> = None;
    for meta in OPS {
        if meta.method != method_str {
            continue;
        }
        if let Some((labels, fixed)) = match_segs(meta.segs, &segs) {
            if best.as_ref().map(|(_, _, f)| fixed > *f).unwrap_or(true) {
                best = Some((meta, labels, fixed));
            }
        }
    }
    best.map(|(m, l, _)| (m, l))
}

/// Try to match a route's segment pattern against the request segments,
/// returning captured labels and the fixed-segment count on success.
fn match_segs(pattern: &[Seg], segs: &[String]) -> Option<(HashMap<String, String>, usize)> {
    let has_greedy = matches!(pattern.last(), Some(Seg::Greedy(_)));
    if has_greedy {
        if segs.len() < pattern.len() {
            return None;
        }
    } else if segs.len() != pattern.len() {
        return None;
    }

    let mut labels = HashMap::new();
    let mut fixed = 0usize;
    let mut i = 0usize;
    for (p, seg) in pattern.iter().enumerate() {
        match seg {
            Seg::Fixed(f) => {
                if segs.get(i)?.as_str() != *f {
                    return None;
                }
                fixed += 1;
                i += 1;
            }
            Seg::Label(name) => {
                labels.insert((*name).to_string(), segs.get(i)?.clone());
                i += 1;
            }
            Seg::Greedy(name) => {
                // Greedy is always the last segment; capture the rest.
                debug_assert_eq!(p, pattern.len() - 1);
                let rest = segs[i..].join("/");
                labels.insert((*name).to_string(), rest);
                i = segs.len();
            }
        }
    }
    if i != segs.len() {
        return None;
    }
    Some((labels, fixed))
}

// ===================== shared helpers =====================

pub(crate) fn ok_json(v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, v)
}

/// Whether an unclaimed `Verb::Action` operation is safe to accept as an empty
/// no-op. Read operations (HTTP GET, or a `Get`/`List`/`Describe`/`Test`/
/// `Validate` name — e.g. the index-analytics and authorizer-test actions) have
/// no persisted side effect, so an empty body is shape-valid; anything else
/// mutates and must fail loud instead of silently discarding the request.
pub(crate) fn is_read_only_action(meta: &OpMeta) -> bool {
    meta.method == "GET"
        || ["Get", "List", "Describe", "Search", "Test", "Validate"]
            .iter()
            .any(|p| meta.op.starts_with(p))
}

pub(crate) fn parse_query(raw: &str) -> Vec<(String, String)> {
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

pub(crate) fn parse_body(body: &[u8]) -> Map<String, Value> {
    if body.is_empty() {
        return Map::new();
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(m)) => m,
        _ => Map::new(),
    }
}

/// Current time as a restJson1 timestamp: Unix epoch **seconds** encoded as a
/// JSON number, with fractional milliseconds preserved (e.g. `1752324947.041`).
/// This is the default `timestamp` wire format for the `restJson1` protocol
/// (which AWS IoT Core uses), and is what the aws-sdk / aws-smithy timestamp
/// deserializer expects — an RFC3339 string would be rejected.
pub(crate) fn now_epoch() -> Value {
    let millis = chrono::Utc::now().timestamp_millis();
    Value::from(millis as f64 / 1000.0)
}

pub(crate) fn query_get<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

/// Canonical resource type for a named-resource operation: the operation's
/// fixed URI segments joined by `/`, with a small set of singular/plural
/// aliases collapsed so a create and its describe/list share one store.
pub(crate) fn resource_type(meta: &OpMeta) -> String {
    let joined = meta
        .segs
        .iter()
        .filter_map(|s| match s {
            Seg::Fixed(f) => Some(*f),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    match joined.as_str() {
        "authorizer" => "authorizers".to_string(),
        "custom-metric" => "custom-metrics".to_string(),
        "fleet-metric" => "fleet-metrics".to_string(),
        "cacertificate" => "cacertificates".to_string(),
        other => other.to_string(),
    }
}

/// The composite storage key for a request: its label values in URI order.
pub(crate) fn storage_key(meta: &OpMeta, labels: &HashMap<String, String>) -> String {
    let mut parts = Vec::new();
    for s in meta.segs {
        match s {
            Seg::Label(name) | Seg::Greedy(name) => {
                if let Some(v) = labels.get(*name) {
                    parts.push(v.clone());
                }
            }
            _ => {}
        }
    }
    parts.join("/")
}

pub(crate) fn arn_path(rtype: &str) -> &'static str {
    match rtype {
        "things" => "thing/",
        "thing-groups" | "dynamic-thing-groups" => "thinggroup/",
        "billing-groups" => "billinggroup/",
        "thing-types" => "thingtype/",
        "policies" => "policy/",
        "certificates" => "cert/",
        "cacertificates" => "cacert/",
        "jobs" => "job/",
        "job-templates" => "jobtemplate/",
        "rules" => "rule/",
        "authorizers" => "authorizer/",
        "role-aliases" => "rolealias/",
        "streams" => "stream/",
        "packages" => "package/",
        "packages/versions" => "package/",
        "security-profiles" => "securityprofile/",
        "mitigationactions/actions" => "mitigationaction/",
        "custom-metrics" => "custommetric/",
        "dimensions" => "dimension/",
        "audit/scheduledaudits" => "scheduledaudit/",
        "domainConfigurations" => "domainconfiguration/",
        "fleet-metrics" => "fleetmetric/",
        "provisioning-templates" => "provisioningtemplate/",
        "otaUpdates" => "otaupdate/",
        "certificate-providers" => "certificateprovider/",
        "commands" => "command/",
        _ => "",
    }
}

pub(crate) fn mint_arn(ctx: &Ctx, rtype: &str, name: &str) -> String {
    format!(
        "arn:aws:iot:{}:{}:{}{}",
        ctx.region,
        ctx.account,
        arn_path(rtype),
        name
    )
}

/// Deterministic UUID-shaped id derived from a seed string.
pub(crate) fn mint_uuid(seed: &str) -> String {
    let h = fnv(seed);
    let h2 = fnv(&format!("{seed}:2"));
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (h >> 32) as u32,
        (h >> 16) as u16,
        h as u16,
        (h2 >> 48) as u16,
        h2 & 0xffff_ffff_ffff
    )
}

/// Deterministic 64-hex certificate id derived from a seed string.
pub(crate) fn mint_hex64(seed: &str) -> String {
    let mut out = String::with_capacity(64);
    for i in 0..8 {
        out.push_str(&format!("{:016x}", fnv(&format!("{seed}:{i}"))));
    }
    out.truncate(64);
    out
}

fn fnv(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Whether a JSON value's type is compatible with a modelled member kind.
pub(crate) fn kind_matches(kind: K, v: &Value) -> bool {
    match kind {
        K::Str | K::Blob => v.is_string(),
        // restJson1 timestamps wire-encode as epoch-seconds JSON numbers; accept
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
pub(crate) fn build_element(meta: &OpMeta, record: &Value) -> Value {
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
