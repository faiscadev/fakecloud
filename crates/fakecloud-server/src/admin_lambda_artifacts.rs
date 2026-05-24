//! Internal HTTP endpoints that serve Lambda function code + layer
//! archives to in-cluster Pod init containers for the Kubernetes
//! backend.
//!
//! Guarded by a process-wide bearer token (generated at fakecloud
//! startup, kept in memory only) so requests from outside the
//! init-container side channel are rejected. The token is templated
//! into the Pod spec at launch time and never logged or persisted.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use fakecloud_lambda::extras::parse_layer_version_arn;
use fakecloud_lambda::SharedLambdaState;

/// Routes mounted under `/_fakecloud/lambda/_internal/*`.
#[derive(Clone)]
pub struct ArtifactRoutesContext {
    pub lambda_state: SharedLambdaState,
    pub bearer_token: Arc<String>,
}

pub fn router(ctx: ArtifactRoutesContext) -> Router {
    Router::new()
        .route(
            "/_fakecloud/lambda/_internal/code/{account_id}/{function_name}/{deploy}",
            get(serve_code),
        )
        .route(
            "/_fakecloud/lambda/_internal/layers/{account_id}/{function_name}/{deploy}",
            get(serve_layers),
        )
        .with_state(ctx)
}

fn check_bearer(headers: &HeaderMap, expected: &str) -> Result<(), StatusCode> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let supplied = header.strip_prefix("Bearer ").unwrap_or("");
    if supplied.is_empty() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if !constant_time_eq(supplied.as_bytes(), expected.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

/// Constant-time bytes-equal so the token check doesn't leak via
/// timing differences. Both args must be the same length for the
/// `==` comparison to be valid.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

async fn serve_code(
    Path((account_id, function_name, _deploy)): Path<(String, String, String)>,
    headers: HeaderMap,
    State(ctx): State<ArtifactRoutesContext>,
) -> impl IntoResponse {
    if let Err(s) = check_bearer(&headers, &ctx.bearer_token) {
        return s.into_response();
    }
    let bytes = {
        let accounts = ctx.lambda_state.read();
        accounts
            .get(&account_id)
            .and_then(|s| s.functions.get(&function_name))
            .and_then(|f| f.code_zip.clone())
    };
    match bytes {
        Some(b) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/zip")],
            b,
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "function not found or no code zip available",
        )
            .into_response(),
    }
}

async fn serve_layers(
    Path((account_id, function_name, deploy)): Path<(String, String, String)>,
    headers: HeaderMap,
    State(ctx): State<ArtifactRoutesContext>,
) -> impl IntoResponse {
    if let Err(s) = check_bearer(&headers, &ctx.bearer_token) {
        return s.into_response();
    }
    // The deploy id is encoded into the path so we can later add
    // staleness rejection without changing the contract; today we
    // serve the function's current attached layer set unconditionally
    // and rely on the facade to spawn a fresh Pod when the deploy
    // fingerprint drifts.
    let _ = deploy;

    let layer_zips: Vec<Vec<u8>> = {
        let accounts = ctx.lambda_state.read();
        let func = match accounts
            .get(&account_id)
            .and_then(|s| s.functions.get(&function_name))
        {
            Some(f) => f.clone(),
            None => {
                return (StatusCode::NOT_FOUND, "function not found").into_response();
            }
        };
        let mut out: Vec<Vec<u8>> = Vec::with_capacity(func.layers.len());
        for attached in &func.layers {
            if let Some((acct, name, ver)) = parse_layer_version_arn(&attached.arn) {
                if let Some(bytes) = accounts
                    .get(&acct)
                    .and_then(|s| s.layers.get(&name))
                    .and_then(|l| l.versions.iter().find(|v| v.version == ver))
                    .and_then(|v| v.code_zip.clone())
                {
                    out.push(bytes);
                }
            }
        }
        out
    };

    match build_layers_tar(&layer_zips) {
        Ok(tar_bytes) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/x-tar")],
            tar_bytes,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to build layers tar: {e}"),
        )
            .into_response(),
    }
}

/// Pack the supplied layer ZIPs into a single tar archive whose
/// entries the init container unzips into `/opt`. The tar holds the
/// raw ZIP bytes as files named `layer-0.zip`, `layer-1.zip`, …; the
/// init container loops + unzips each.
///
/// Returning a tar means one HTTP round-trip per Pod boot regardless
/// of layer count, which matters for cold-start latency in CI.
fn build_layers_tar(layer_zips: &[Vec<u8>]) -> Result<Vec<u8>, std::io::Error> {
    let mut builder = tar::Builder::new(Vec::new());
    for (i, bytes) in layer_zips.iter().enumerate() {
        let mut header = tar::Header::new_gnu();
        header.set_path(format!("layer-{i}.zip"))?;
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, &bytes[..])?;
    }
    builder.into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_lambda::{LambdaFunction, LambdaState};
    use tower::ServiceExt; // for `.oneshot`

    fn mk_state(code: Option<Vec<u8>>) -> SharedLambdaState {
        let mut mas: MultiAccountState<LambdaState> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let acct = mas.get_or_create("000000000000");
        let mut f = LambdaFunction {
            function_name: "my-fn".into(),
            function_arn: "arn:aws:lambda:us-east-1:000000000000:function:my-fn".into(),
            runtime: "python3.12".into(),
            role: "arn:aws:iam::000000000000:role/r".into(),
            handler: "h".into(),
            description: String::new(),
            timeout: 3,
            memory_size: 128,
            code_sha256: String::new(),
            code_size: 0,
            version: "$LATEST".into(),
            last_modified: chrono::Utc::now(),
            tags: Default::default(),
            environment: Default::default(),
            architectures: Vec::new(),
            package_type: "Zip".into(),
            code_zip: code,
            image_uri: None,
            policy: None,
            layers: Vec::new(),
            revision_id: "r".into(),
            tracing_mode: None,
            kms_key_arn: None,
            ephemeral_storage_size: None,
            vpc_config: None,
            snap_start: None,
            dead_letter_config_arn: None,
            file_system_configs: Vec::new(),
            logging_config: None,
            image_config: None,
            durable_config: None,
            signing_profile_version_arn: None,
            signing_job_arn: None,
            runtime_version_config: None,
            master_arn: None,
            state_reason: None,
            state_reason_code: None,
            last_update_status_reason: None,
            last_update_status_reason_code: None,
        };
        f.environment = Default::default();
        acct.functions.insert("my-fn".into(), f);
        Arc::new(parking_lot::RwLock::new(mas))
    }

    fn app(state: SharedLambdaState, token: &str) -> Router {
        router(ArtifactRoutesContext {
            lambda_state: state,
            bearer_token: Arc::new(token.to_string()),
        })
    }

    async fn get_with_auth(app: &Router, path: &str, token: Option<&str>) -> (StatusCode, Vec<u8>) {
        let mut req = Request::builder().uri(path).method("GET");
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let resp = app
            .clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        (status, body)
    }

    #[tokio::test]
    async fn code_endpoint_requires_bearer() {
        let state = mk_state(Some(b"zipbytes".to_vec()));
        let app = app(state, "tok");
        let (status, _) = get_with_auth(
            &app,
            "/_fakecloud/lambda/_internal/code/000000000000/my-fn/d.zip",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn code_endpoint_rejects_wrong_bearer() {
        let state = mk_state(Some(b"zipbytes".to_vec()));
        let app = app(state, "tok");
        let (status, _) = get_with_auth(
            &app,
            "/_fakecloud/lambda/_internal/code/000000000000/my-fn/d.zip",
            Some("nope"),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn code_endpoint_returns_zip_bytes() {
        let state = mk_state(Some(b"zipbytes".to_vec()));
        let app = app(state, "tok");
        let (status, body) = get_with_auth(
            &app,
            "/_fakecloud/lambda/_internal/code/000000000000/my-fn/d.zip",
            Some("tok"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"zipbytes".to_vec());
    }

    #[tokio::test]
    async fn code_endpoint_404_when_no_zip() {
        let state = mk_state(None);
        let app = app(state, "tok");
        let (status, _) = get_with_auth(
            &app,
            "/_fakecloud/lambda/_internal/code/000000000000/my-fn/d.zip",
            Some("tok"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn code_endpoint_404_unknown_function() {
        let state = mk_state(Some(b"x".to_vec()));
        let app = app(state, "tok");
        let (status, _) = get_with_auth(
            &app,
            "/_fakecloud/lambda/_internal/code/000000000000/missing/d.zip",
            Some("tok"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn layers_endpoint_empty_for_no_layers_returns_empty_tar() {
        let state = mk_state(Some(b"x".to_vec()));
        let app = app(state, "tok");
        let (status, body) = get_with_auth(
            &app,
            "/_fakecloud/lambda/_internal/layers/000000000000/my-fn/d.tar",
            Some("tok"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // Empty tar = two 512-byte all-zero blocks (the EOF marker).
        assert_eq!(body, vec![0u8; 1024]);
    }
}
