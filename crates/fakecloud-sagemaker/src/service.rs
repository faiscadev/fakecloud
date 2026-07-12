//! Amazon SageMaker (`sagemaker`) awsJson1.1 dispatch.
//!
//! Requests are routed to one of the ~403 modelled operations by the
//! `X-Amz-Target: SageMaker.<Operation>` header (surfaced as
//! [`AwsRequest::action`]). All inputs arrive in the JSON body. Input is
//! validated against the model-derived constraints ([`crate::validate`]), then
//! handled by the generic resource engine ([`engine`], for the create /
//! describe / list / update / delete verb of every named resource family) or by
//! a resource-specific handler ([`special`], for tagging). State is
//! account-partitioned and persisted.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::generated::{OpMeta, Verb, OPS};
use crate::persistence::save_snapshot;
use crate::state::SharedSageMakerState;

mod engine;
mod special;
#[cfg(test)]
mod tests;

/// Every operation name in the Amazon SageMaker Smithy model.
pub use crate::generated::ACTIONS as SAGEMAKER_ACTIONS;

/// SageMaker's service-wide "shared error responses": codes the live API can
/// return from essentially any operation for a missing / duplicate / in-use /
/// malformed resource, even where the per-operation Smithy `errors:` list omits
/// them (the SageMaker model declares only four error shapes total). These are
/// accepted alongside each operation's declared errors.
pub const COMMON_ERRORS: &[&str] = &[
    "ResourceNotFound",
    "ResourceInUse",
    "ResourceLimitExceeded",
    "ConflictException",
    "ValidationException",
];

pub struct SageMakerService {
    state: SharedSageMakerState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl SageMakerService {
    pub fn new(state: SharedSageMakerState) -> Self {
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
impl AwsService for SageMakerService {
    fn service_name(&self) -> &str {
        "sagemaker"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some(meta) = OPS.iter().find(|m| m.op == req.action) else {
            return Err(AwsServiceError::action_not_implemented(
                "sagemaker",
                &req.action,
            ));
        };
        let (resp, mutated) = self.dispatch(meta, &req)?;
        if mutated && resp.status.is_success() {
            self.save().await;
        }
        Ok(resp)
    }

    fn supported_actions(&self) -> &[&str] {
        SAGEMAKER_ACTIONS
    }
}

/// Per-request context.
pub(crate) struct Ctx {
    pub account: String,
    pub region: String,
}

impl SageMakerService {
    fn dispatch(
        &self,
        meta: &'static OpMeta,
        req: &AwsRequest,
    ) -> Result<(AwsResponse, bool), AwsServiceError> {
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: if req.region.is_empty() {
                "us-east-1".to_string()
            } else {
                req.region.clone()
            },
        };
        let body = parse_body(&req.body);
        crate::validate::validate(meta, &body)?;

        // Resource-specific handlers (tagging) take precedence over the engine.
        if let Some(res) = special::dispatch(self, meta, &ctx, &body)? {
            return Ok(res);
        }

        match meta.verb {
            Verb::Create => {
                let mut g = self.state.write();
                let data = g.get_or_create(&ctx.account);
                Ok((engine::create(data, &ctx, meta, &body)?, true))
            }
            Verb::Update => {
                let mut g = self.state.write();
                let data = g.get_or_create(&ctx.account);
                Ok((engine::update(data, meta, &body)?, true))
            }
            Verb::Delete => {
                let mut g = self.state.write();
                let data = g.get_or_create(&ctx.account);
                Ok((engine::delete(data, meta, &body), true))
            }
            Verb::Get => {
                let g = self.state.read();
                let data = g.get(&ctx.account);
                Ok((engine::get(data, meta, &body)?, false))
            }
            Verb::List => {
                let g = self.state.read();
                let data = g.get(&ctx.account);
                Ok((engine::list(data, meta, &body), false))
            }
            Verb::Action => {
                // Any action not claimed by `special` is accepted as a
                // control-plane no-op returning an empty (shape-valid) body.
                Ok((ok_json(Value::Object(Map::new())), false))
            }
        }
    }
}

// ===================== shared helpers =====================

pub(crate) fn ok_json(v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, v)
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

/// Current time as an awsJson1.1 timestamp: Unix epoch **seconds** encoded as a
/// JSON number, with fractional milliseconds preserved (e.g. `1752324947.041`).
/// This is the default `timestamp` wire format for the awsJson1.1 protocol, and
/// is what the aws-sdk / aws-smithy timestamp deserializer expects — an RFC3339
/// string would be rejected.
pub(crate) fn now_epoch() -> Value {
    let millis = chrono::Utc::now().timestamp_millis();
    Value::from(millis as f64 / 1000.0)
}

pub(crate) fn mint_arn(ctx: &Ctx, arn_path: &str, name: &str) -> String {
    format!(
        "arn:aws:sagemaker:{}:{}:{}/{}",
        ctx.region, ctx.account, arn_path, name
    )
}

/// Deterministic UUID-shaped id derived from account + family + seed.
pub(crate) fn mint_id(account: &str, family: &str, seed: &str) -> String {
    let base = format!("{account}:{family}:{seed}");
    let h = fnv(&base);
    let h2 = fnv(&format!("{base}:2"));
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (h >> 32) as u32,
        (h >> 16) as u16,
        h as u16,
        (h2 >> 48) as u16,
        h2 & 0xffff_ffff_ffff
    )
}

fn fnv(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

pub(crate) fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFound", msg)
}

pub(crate) fn in_use(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ResourceInUse", msg)
}
