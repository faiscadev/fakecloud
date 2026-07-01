// crates/fakecloud-cloudfront/src/dataplane.rs
//! In-process CloudFront data plane.
//!
//! Mirrors the ELBv2 data plane (`fakecloud-elbv2/src/dataplane.rs`): a
//! supervisor loop binds one `TcpListener` on `127.0.0.1:0` per *enabled*
//! distribution, records the OS-allocated port back into distribution state as
//! `bound_port`, and serves viewer requests via `hyper`. The AWS-shaped
//! `*.cloudfront.net` domain stays cosmetic; clients discover the real address
//! via `/_fakecloud/cloudfront/distributions` and connect to
//! `http://127.0.0.1:{bound_port}/...`.
//!
//! This module lands in stages: Task 2 wires the listener lifecycle and serves
//! a fixed `200`. Path-pattern origin routing (Task 3) and CustomErrorResponses
//! (Task 4) extend `handle_request`. There is no global edge network — this is a
//! single local origin-serving node, matching the ALB/API Gateway precedent.

use std::collections::{BTreeMap, HashSet};
use std::convert::Infallible;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use crate::state::SharedCloudFrontState;

const ENV_DISABLE: &str = "FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE";
const SUPERVISOR_TICK_SECS: u64 = 1;

/// Whether the data plane should run. Disabled by setting
/// `FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE` to a truthy value (mirrors the
/// ELBv2 flag), for environments that only exercise the control plane.
pub fn dataplane_enabled() -> bool {
    !matches!(
        std::env::var(ENV_DISABLE).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

/// Per-distribution listener handle. Dropping it aborts the accept loop and
/// frees the OS port, so a disabled/deleted distribution stops serving.
struct BoundListener {
    handle: JoinHandle<()>,
}

impl Drop for BoundListener {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// State shared across the supervisor and per-connection handlers.
#[derive(Clone)]
struct DataPlane {
    state: SharedCloudFrontState,
}

/// Spawn the CloudFront data-plane supervisor. No-op (returns without spawning)
/// when disabled via the env flag.
pub fn spawn_dataplane(state: SharedCloudFrontState) {
    if !dataplane_enabled() {
        debug!("CloudFront data plane disabled via {ENV_DISABLE}");
        return;
    }
    let dp = DataPlane { state };
    tokio::spawn(supervisor_loop(dp));
}

async fn supervisor_loop(dp: DataPlane) {
    let mut bindings: BTreeMap<String, BoundListener> = BTreeMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(SUPERVISOR_TICK_SECS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        reconcile(&dp, &mut bindings).await;
    }
}

/// Reconcile bound listeners against the set of enabled distributions. Binds
/// newly-enabled distributions, tears down listeners for ones that were
/// disabled or deleted, and keeps each distribution's `bound_port` in sync with
/// whether the supervisor currently holds its listener. Because the want-set is
/// derived purely from persisted state, enabled distributions loaded from a
/// snapshot on startup are re-bound on the first tick (startup rebind).
async fn reconcile(dp: &DataPlane, bindings: &mut BTreeMap<String, BoundListener>) {
    // 1. Snapshot the (distribution id, owning account) pairs that want a listener.
    let want: Vec<(String, String)> = {
        let accs = dp.state.read();
        accs.all_distributions()
            .filter(|(_acct, d)| d.config.enabled)
            .map(|(acct, d)| (d.id.clone(), acct.clone()))
            .collect()
    };
    let want_set: HashSet<&String> = want.iter().map(|(id, _)| id).collect();

    // 2. Drop bindings for distributions no longer wanted (disabled/deleted).
    bindings.retain(|id, _| want_set.contains(id));

    // 3. Bind any newly-enabled distribution.
    for (dist_id, account_id) in want.iter() {
        if bindings.contains_key(dist_id) {
            continue;
        }
        match TcpListener::bind(("127.0.0.1", 0)).await {
            Ok(listener) => {
                let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
                if port == 0 {
                    warn!("CloudFront data plane: bind returned port 0 for {dist_id}; skipping");
                    continue;
                }
                {
                    let mut accs = dp.state.write();
                    if let Some(st) = accs.accounts.get_mut(account_id) {
                        if let Some(d) = st.distributions.get_mut(dist_id) {
                            d.bound_port = Some(port);
                        }
                    }
                }
                let dp2 = dp.clone();
                let id2 = dist_id.clone();
                let handle = tokio::spawn(async move {
                    accept_loop(dp2, id2, listener).await;
                });
                bindings.insert(dist_id.clone(), BoundListener { handle });
                trace!(dist = %dist_id, port, "CloudFront data plane: bound listener");
            }
            Err(e) => {
                warn!("CloudFront data plane: failed to bind for {dist_id}: {e}");
            }
        }
    }

    // 4. Clear bound_port for any distribution the supervisor no longer holds.
    let mut accs = dp.state.write();
    let account_ids: Vec<String> = accs.accounts.keys().cloned().collect();
    for acct in account_ids {
        if let Some(st) = accs.accounts.get_mut(&acct) {
            for d in st.distributions.values_mut() {
                if !bindings.contains_key(&d.id) {
                    d.bound_port = None;
                }
            }
        }
    }
}

async fn accept_loop(dp: DataPlane, dist_id: String, listener: TcpListener) {
    loop {
        let (sock, _peer) = match listener.accept().await {
            Ok(p) => p,
            Err(e) => {
                debug!(dist = %dist_id, "accept error: {e}");
                continue;
            }
        };
        let dp2 = dp.clone();
        let id2 = dist_id.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(sock);
            let svc = service_fn(move |req| {
                let dp3 = dp2.clone();
                let id3 = id2.clone();
                async move { Ok::<_, Infallible>(handle_request(&dp3, &id3, req).await) }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                debug!("CloudFront data plane: connection error: {e}");
            }
        });
    }
}

/// Serve one viewer request. Task 2 returns a fixed `200` to prove the listener
/// lifecycle and discovery end to end; Task 3 adds path-pattern origin routing
/// and origin fetch, Task 4 adds CustomErrorResponses.
async fn handle_request(
    _dp: &DataPlane,
    dist_id: &str,
    _req: Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    trace!(dist = %dist_id, "CloudFront data plane: serving request");
    Response::builder()
        .status(200)
        .body(Full::new(Bytes::from_static(b"ok")))
        .expect("static response builds")
}
