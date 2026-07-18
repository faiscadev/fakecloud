//! Link-local IMDS / ECS-credentials listeners.
//!
//! Some apps hardcode the AWS metadata addresses instead of honoring
//! `AWS_EC2_METADATA_SERVICE_ENDPOINT` / `AWS_CONTAINER_CREDENTIALS_*`:
//! - IMDS at `http://169.254.169.254`
//! - the ECS container-credentials relative-URI base `http://169.254.170.2`
//!
//! With `--imds-link-local`, fakecloud binds those link-local addresses (port
//! 80) so such apps resolve credentials unmodified:
//! - `169.254.169.254:80` serves **only** the IMDS `/latest/*` surface (not the
//!   rest of the app), returning 404 for anything else.
//! - `169.254.170.2:80` serves container credentials at a single fixed path
//!   (`/creds`); set `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI=/creds`.
//!
//! This needs privileged host setup that fakecloud deliberately does NOT perform
//! itself: the operator assigns the addresses to the loopback interface up front
//! (`sudo ip addr add 169.254.169.254/32 dev lo`, etc.), and fakecloud then binds
//! them (binding port 80 also needs root). fakecloud never creates or deletes the
//! loopback alias, so it leaves no host-networking state behind to leak or clean
//! up; the alias is the operator's to add and remove. If binding fails (address
//! not aliased, or no privilege) it logs the exact alias command and the main
//! server is unaffected. Setup runs as a detached task so it never delays the
//! main server's startup. A container reaching these host addresses is a separate,
//! platform-specific networking step (documented in the guide), not something
//! fakecloud can arrange from inside the process.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{extract::State, routing::get, Json, Router};

use crate::imds::ImdsContext;

/// IMDS link-local address.
const IMDS_IP: &str = "169.254.169.254";
/// ECS container-credentials relative-URI base address.
const ECS_CREDS_IP: &str = "169.254.170.2";
/// The fixed path the ECS-credentials listener serves (set
/// `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` to this).
const ECS_CREDS_PATH: &str = "/creds";
/// Both services listen on port 80, as on real AWS.
const PORT: u16 = 80;

/// The manual command the operator runs to assign the link-local address to the
/// loopback interface, shown in the log when a bind fails. fakecloud never runs
/// this itself; it is purely a hint for the human doing the privileged setup.
fn alias_hint(ip: &str) -> String {
    if cfg!(target_os = "macos") {
        format!("sudo ifconfig lo0 alias {ip}")
    } else {
        format!("sudo ip addr add {ip}/32 dev lo")
    }
}

/// Bind `ip:PORT` and, on success, serve `router` on it. On failure, log the
/// manual setup command and return; never propagates an error so the main server
/// is unaffected. fakecloud does not create the loopback alias -- it binds an
/// address the operator has already provisioned -- so this both stays safe under
/// test (a reserved address simply fails to bind) and leaves no host state to
/// clean up.
async fn bind_and_serve(ip: &str, router: Router, label: &str) {
    let addr = format!("{ip}:{PORT}");
    match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => {
            tracing::info!("fakecloud link-local {label} listening on http://{addr}");
            let label = label.to_string();
            tokio::spawn(async move {
                if let Err(e) = axum::serve(
                    listener,
                    router.into_make_service_with_connect_info::<SocketAddr>(),
                )
                .await
                {
                    tracing::warn!("fakecloud link-local {label} server error: {e}");
                }
            });
        }
        Err(e) => {
            tracing::warn!(
                "fakecloud could not bind link-local {label} on {addr}: {e}. \
                 This needs root (to bind port 80) and the address assigned to the \
                 loopback interface; assign it with: {}. The main server is unaffected.",
                alias_hint(ip)
            );
        }
    }
}

/// Vend container credentials at the fixed `/creds` path on `169.254.170.2`,
/// reusing [`ImdsContext::credentials`] so this surface stays consistent with the
/// IMDS `security-credentials` path (same cache, same IAM-registered creds).
async fn ecs_credentials(State(ctx): State<Arc<ImdsContext>>) -> Json<serde_json::Value> {
    Json(ctx.credentials().to_container_json())
}

/// Bring up the link-local listeners. Runs to completion in a detached task
/// spawned by `main`, so it never delays startup. `ctx` is the IMDS context; the
/// IMDS IP serves an IMDS-only router built from it, the ECS IP the `/creds`
/// surface built from the same context.
pub async fn run(ctx: Arc<ImdsContext>) {
    // Both routers hold a refcounted clone of the one shared context rather than
    // a second copy of its string fields.
    let creds_router = Router::new()
        .route(ECS_CREDS_PATH, get(ecs_credentials))
        .with_state(ctx.clone());

    // Bring both addresses up concurrently; the two binds share no state.
    tokio::join!(
        bind_and_serve(
            IMDS_IP,
            crate::imds::link_local_router(ctx.clone()),
            "IMDS (169.254.169.254)",
        ),
        bind_and_serve(
            ECS_CREDS_IP,
            creds_router,
            "ECS credentials (169.254.170.2)",
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alias_hint_matches_platform() {
        let hint = alias_hint("169.254.169.254");
        if cfg!(target_os = "macos") {
            assert_eq!(hint, "sudo ifconfig lo0 alias 169.254.169.254");
        } else {
            assert_eq!(hint, "sudo ip addr add 169.254.169.254/32 dev lo");
        }
    }

    #[tokio::test]
    async fn bind_failure_logs_and_returns_without_panic() {
        // 192.0.2.1 (RFC 5737 TEST-NET-1) is not assigned to any interface, so
        // the bind fails deterministically regardless of privilege. fakecloud
        // creates no alias, so this never mutates host networking even as root:
        // bind_and_serve must log and return, not panic.
        bind_and_serve("192.0.2.1", Router::new(), "test").await;
    }

    /// Drive the ECS `/creds` route directly (no privilege / real bind needed) to
    /// pin the path constant, method, and container-JSON shape apps depend on.
    #[tokio::test]
    async fn ecs_creds_route_serves_container_json() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use fakecloud_core::multi_account::MultiAccountState;
        use parking_lot::RwLock;
        use tower::ServiceExt;

        let ctx = Arc::new(ImdsContext {
            iam: Arc::new(RwLock::new(MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "",
            ))),
            cache: Arc::new(
                fakecloud_iam::sts_service::container_creds::ContainerCredentialCache::new(),
            ),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            role_arn: "arn:aws:iam::123456789012:role/fakecloud".to_string(),
            instance_id: "i-0123456789abcdef0".to_string(),
        });
        let router = Router::new()
            .route(ECS_CREDS_PATH, get(ecs_credentials))
            .with_state(ctx);

        let resp = router
            .oneshot(
                Request::builder()
                    .uri(ECS_CREDS_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        // The AWS container-credentials shape apps parse (note `Token`, not
        // `SessionToken`).
        assert!(json["AccessKeyId"]
            .as_str()
            .is_some_and(|s| s.starts_with("FSIA")));
        assert!(json["SecretAccessKey"].is_string());
        assert!(json["Token"].is_string());
        assert_eq!(
            json["RoleArn"].as_str(),
            Some("arn:aws:iam::123456789012:role/fakecloud")
        );
    }
}
