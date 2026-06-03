//! Internal HTTP endpoint that serves a snapshot RDB to a restoring
//! ElastiCache Redis Pod (Kubernetes backend).
//!
//! When a cache cluster is created from a snapshot, the runtime stages
//! the snapshot bytes in a per-process map keyed by Pod name; the
//! restored Pod's container `wget`s them into `/data/dump.rdb` before
//! launching `redis-server`. Guarded by the same process-wide bearer
//! token as the Lambda artifact endpoints — never exposed without auth.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

use crate::admin_lambda_artifacts::check_bearer;
use fakecloud_elasticache::runtime::PendingRdb;

/// Routes mounted under `/_fakecloud/elasticache/_internal/*`.
#[derive(Clone)]
pub struct RdbRoutesContext {
    pub pending_rdb: PendingRdb,
    pub bearer_token: Arc<String>,
}

pub fn router(ctx: RdbRoutesContext) -> Router {
    Router::new()
        .route(
            "/_fakecloud/elasticache/_internal/rdb/{pod}",
            get(serve_rdb),
        )
        .with_state(ctx)
}

async fn serve_rdb(
    State(ctx): State<RdbRoutesContext>,
    Path(pod): Path<String>,
    headers: HeaderMap,
) -> axum::response::Response {
    if check_bearer(&headers, &ctx.bearer_token).is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match ctx.pending_rdb.read().get(&pod).cloned() {
        Some(bytes) => {
            ([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
