//! Routes Lambda Runtime API requests to the runtime that serves them.
//!
//! Every Lambda instance's runtime reaches fakecloud's main listener at its
//! own host name (`fakecloud-runtime-<token>`, see
//! `fakecloud_lambda::runtime::api`); requests carrying such a `Host` are the
//! Runtime API, never the AWS API, and are answered before anything else
//! sees them (no SigV4, no IAM: on Lambda the API is local to the sandbox).

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use fakecloud_lambda::runtime::RuntimeApi;

pub async fn middleware(
    State(api): State<Option<Arc<RuntimeApi>>>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(api) = api.as_ref() {
        if RuntimeApi::is_runtime_request(req.headers()) {
            return api.serve(req).await;
        }
    }
    next.run(req).await
}
