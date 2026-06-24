//! In-process HTTP routing for ELBv2 ALBs.
//!
//! For each ALB whose `state_code == "active"`, the supervisor binds
//! a TCP listener on `127.0.0.1:0` (OS-allocated). Connections are
//! parsed as HTTP/1.1, matched against the listener rules belonging
//! to that LB, and dispatched to the action: `forward` (round-robin
//! over registered targets in the chosen target group),
//! `fixed-response`, `redirect`, or `authenticate-oidc` /
//! `authenticate-cognito` (501 — declared next-batch).
//!
//! TLS termination, mTLS, and raw NLB TCP are intentionally next-
//! batch work items. The data-plane doc page enumerates the gap.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::Utc;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_wafv2::evaluator::{
    evaluate_detailed as waf_evaluate_detailed, WafAction, WafRequest,
};
use fakecloud_wafv2::{IpSet, RegexPatternSet, SharedWafv2State, WebAcl};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::accesslogs::{AccessLogRecord, AccessLogger, LogType};
use crate::router::select_actions;
use crate::state::{Action, Listener, LoadBalancer, Rule, SharedElbv2State, TargetGroup};

const ENV_DISABLE: &str = "FAKECLOUD_ELBV2_DISABLE_DATAPLANE";
const SUPERVISOR_TICK_SECS: u64 = 1;
const STICKY_COOKIE: &str = "AWSALB";

pub fn dataplane_enabled() -> bool {
    !matches!(
        std::env::var(ENV_DISABLE).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

/// Per-LB listener handle. Held in the supervisor map so that a LB
/// removal can drop the JoinHandle and free the OS port.
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
    state: SharedElbv2State,
    /// Optional WAFv2 state. When set, every ALB request is evaluated
    /// against the WebACL associated with the load balancer (if any)
    /// before the listener-rule router runs.
    waf_state: Option<SharedWafv2State>,
    /// Optional access-log buffer. When set, every served request
    /// emits one access-log line (and one connection-log line if
    /// connection_logs are enabled) into this in-memory buffer; the
    /// flusher background task ships gzipped batches to S3 every
    /// ~60 seconds or when the buffer fills.
    access_logger: Option<Arc<AccessLogger>>,
    /// Count of WAF Count-action matches, keyed by `WebACL ARN | rule
    /// name`. Exposed for tests and future metrics scraping.
    waf_count_metrics: Arc<Mutex<BTreeMap<String, u64>>>,
    /// Round-robin index per target group ARN (mod target count).
    rr_counters: Arc<Mutex<BTreeMap<String, usize>>>,
    /// Sticky-session map: `AWSALB` cookie value -> `tg_arn|target_id|target_port`.
    sticky_targets: Arc<Mutex<BTreeMap<String, String>>>,
    upstream: reqwest::Client,
    /// Address used to reach loopback-published sibling targets (EC2 instances
    /// and ECS bridge-mode tasks publish their ports on the host daemon).
    /// `127.0.0.1` when fakecloud runs on the host; the host alias
    /// (`host.docker.internal` / `host.containers.internal`) when fakecloud is
    /// itself containerized — otherwise the forward would hit fakecloud's own
    /// loopback and 502 (#1539-class, bug-hunt 2026-06-24 0.B).
    sibling_host: String,
}

pub fn spawn_dataplane(
    state: SharedElbv2State,
    waf_state: Option<SharedWafv2State>,
    waf_count_metrics: Option<Arc<Mutex<BTreeMap<String, u64>>>>,
) {
    spawn_dataplane_with_delivery(state, waf_state, None, waf_count_metrics);
}

/// Spawn the supervisor and (when a `delivery_bus` is wired) the
/// access-log flusher. The flusher is a separate task so the per-
/// request hot path only buffers; S3 puts run off the request thread.
pub fn spawn_dataplane_with_delivery(
    state: SharedElbv2State,
    waf_state: Option<SharedWafv2State>,
    delivery_bus: Option<Arc<DeliveryBus>>,
    waf_count_metrics: Option<Arc<Mutex<BTreeMap<String, u64>>>>,
) -> Option<Arc<AccessLogger>> {
    if !dataplane_enabled() {
        debug!("ELBv2 data plane disabled via {ENV_DISABLE}");
        return None;
    }
    let upstream = match reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        // Disable redirects so we surface them as the upstream sees them.
        .redirect(reqwest::redirect::Policy::none())
        // Per-request timeout — keep tight; tests want fast feedback.
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("ELBv2 data plane: failed to build reqwest client: {e}");
            return None;
        }
    };
    let access_logger = delivery_bus.map(|bus| {
        let logger = Arc::new(AccessLogger::new(Arc::clone(&state), bus));
        crate::accesslogs::spawn_flusher(Arc::clone(&logger));
        logger
    });
    let logger_for_caller = access_logger.as_ref().map(Arc::clone);
    let dp = DataPlane {
        state,
        waf_state,
        access_logger,
        waf_count_metrics: waf_count_metrics
            .unwrap_or_else(|| Arc::new(Mutex::new(BTreeMap::new()))),
        rr_counters: Arc::new(Mutex::new(BTreeMap::new())),
        sticky_targets: Arc::new(Mutex::new(BTreeMap::new())),
        upstream,
        // Detect once: shelling the container CLI per request would be wasteful.
        sibling_host: fakecloud_core::container_net::detect_container_cli()
            .map(|cli| fakecloud_core::container_net::HostNetworking::detect(&cli).sibling_host)
            .unwrap_or_else(|| "127.0.0.1".to_string()),
    };
    tokio::spawn(supervisor_loop(dp));
    logger_for_caller
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

async fn reconcile(dp: &DataPlane, bindings: &mut BTreeMap<String, BoundListener>) {
    // 1. Snapshot the set of ALBs that need a listener and their schemes.
    let want: Vec<(String, String)> = {
        let accs = dp.state.read();
        let mut out = Vec::new();
        for (_acct, st) in accs.iter() {
            for lb in st.load_balancers.values() {
                if !lb_should_bind(lb) {
                    continue;
                }
                out.push((lb.arn.clone(), lb.lb_type.clone()));
            }
        }
        out
    };
    let want_set: std::collections::HashSet<&String> = want.iter().map(|(arn, _)| arn).collect();

    // 2. Drop bindings for LBs that no longer exist or are not active.
    bindings.retain(|arn, _| want_set.contains(arn));

    // 3. Bind any new ALB.
    for (arn, lb_type) in want.into_iter() {
        if bindings.contains_key(&arn) {
            continue;
        }
        if !lb_type.eq_ignore_ascii_case("application") {
            // Only ALBs get the in-process HTTP data plane in this
            // batch. NLB raw-TCP and GWLB are explicit next-batch
            // items per the data-plane doc.
            continue;
        }
        match TcpListener::bind(("127.0.0.1", 0)).await {
            Ok(listener) => {
                let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
                if port == 0 {
                    warn!("ELBv2 data plane: bind returned port 0 for {arn}; skipping");
                    continue;
                }
                {
                    let mut accs = dp.state.write();
                    let owning_account: Option<String> = accs
                        .iter()
                        .find(|(_, s)| s.load_balancers.contains_key(&arn))
                        .map(|(a, _)| a.clone());
                    if let Some(acct) = owning_account {
                        if let Some(st) = accs.get_mut(&acct) {
                            if let Some(lb) = st.load_balancers.get_mut(&arn) {
                                lb.bound_port = Some(port);
                            }
                        }
                    }
                }
                let dp2 = dp.clone();
                let arn2 = arn.clone();
                let handle = tokio::spawn(async move {
                    accept_loop(dp2, arn2, listener).await;
                });
                bindings.insert(arn.clone(), BoundListener { handle });
                trace!(arn = %arn, port, "ELBv2 data plane: bound listener");
            }
            Err(e) => {
                warn!("ELBv2 data plane: failed to bind for {arn}: {e}");
            }
        }
    }

    // 4. Clear bound_port for any LB the supervisor is no longer holding.
    let mut accs = dp.state.write();
    let acct_ids: Vec<String> = accs.iter().map(|(a, _)| a.clone()).collect();
    for acct in acct_ids {
        if let Some(st) = accs.get_mut(&acct) {
            for lb in st.load_balancers.values_mut() {
                if !bindings.contains_key(&lb.arn) {
                    lb.bound_port = None;
                }
            }
        }
    }
}

fn lb_should_bind(lb: &LoadBalancer) -> bool {
    lb.state_code == "active"
}

async fn accept_loop(dp: DataPlane, lb_arn: String, listener: TcpListener) {
    loop {
        let (sock, peer_addr) = match listener.accept().await {
            Ok(p) => p,
            Err(e) => {
                debug!(arn = %lb_arn, "accept error: {e}");
                continue;
            }
        };
        // Capture the local addr (i.e. the LB's bound listener address)
        // before moving the socket into the connection task. Used for
        // the `destination_ip:port` field of the connection log.
        let local_addr = sock.local_addr().ok();
        let dp2 = dp.clone();
        let lb_arn2 = lb_arn.clone();
        tokio::spawn(async move {
            let connection_start = Instant::now();
            let connection_creation_time = Utc::now();
            let io = TokioIo::new(sock);
            // Clone before the move into the service closure so we
            // still hold an owned reference for the post-serve
            // connection-log emission below.
            let dp_for_log = dp2.clone();
            let lb_for_log = lb_arn2.clone();
            let svc = service_fn(move |req| {
                let dp3 = dp2.clone();
                let lb3 = lb_arn2.clone();
                let peer = peer_addr;
                async move { Ok::<_, Infallible>(handle_request(&dp3, &lb3, peer, req).await) }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                debug!("ELBv2 data plane: connection error: {e}");
            }
            // One connection log per established connection, not per
            // request. Keep-alive connections that serve N requests
            // still produce exactly one connection-log entry covering
            // the lifetime of the TCP connection.
            emit_connection_log(
                &dp_for_log,
                &lb_for_log,
                peer_addr,
                local_addr,
                connection_creation_time,
                connection_start,
            );
        });
    }
}

/// Buffer one connection-log record for the LB. Called once per
/// accepted TCP connection after `serve_connection` returns; covers
/// the lifetime of the connection regardless of how many HTTP requests
/// were served on it.
fn emit_connection_log(
    dp: &DataPlane,
    lb_arn: &str,
    peer_addr: SocketAddr,
    local_addr: Option<SocketAddr>,
    creation_time: chrono::DateTime<chrono::Utc>,
    connection_start: Instant,
) {
    let Some(logger) = dp.access_logger.as_ref() else {
        return;
    };
    let elapsed = connection_start.elapsed().as_secs_f64();
    let elb_id = parse_lb_id(lb_arn).unwrap_or_else(|| lb_arn.to_string());
    let (target_ip, target_port) = match local_addr {
        Some(a) => (Some(a.ip().to_string()), Some(a.port())),
        None => (None, None),
    };
    let target_port_list = match (target_ip.as_deref(), target_port) {
        (Some(ip), Some(p)) => format!("{ip}:{p}"),
        _ => "-".to_string(),
    };
    let record = AccessLogRecord {
        log_type: LogType::Connection,
        timestamp: chrono::Utc::now(),
        elb_id,
        client_ip: peer_addr.ip().to_string(),
        client_port: peer_addr.port(),
        target_ip,
        target_port,
        request_processing_time: elapsed,
        target_processing_time: 0.0,
        response_processing_time: 0.0,
        elb_status_code: 0,
        target_status_code: None,
        received_bytes: 0,
        sent_bytes: 0,
        request_method: "-".to_string(),
        request_url: "-".to_string(),
        request_protocol: "-".to_string(),
        user_agent: "-".to_string(),
        ssl_cipher: "-".to_string(),
        ssl_protocol: "-".to_string(),
        target_group_arn: "-".to_string(),
        trace_id: format!("Root=1-{:x}-{}", chrono::Utc::now().timestamp(), short_id()),
        domain_name: "-".to_string(),
        chosen_cert_arn: "-".to_string(),
        matched_rule_priority: "-".to_string(),
        request_creation_time: creation_time,
        actions_executed: "-".to_string(),
        redirect_url: "-".to_string(),
        error_reason: "-".to_string(),
        target_port_list,
        target_status_code_list: "-".to_string(),
        classification: "-".to_string(),
        classification_reason: "-".to_string(),
    };
    logger.record(lb_arn, record);
}

async fn handle_request(
    dp: &DataPlane,
    lb_arn: &str,
    peer_addr: SocketAddr,
    req: Request<Incoming>,
) -> Response<Full<Bytes>> {
    let request_start = Instant::now();
    let request_creation_time = Utc::now();
    // Read request fully into memory. Body sizes that matter for ALB
    // tests are small; the streaming-body refactor is its own batch.
    let (parts, body) = req.into_parts();
    let body_bytes = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => return canned(StatusCode::BAD_REQUEST, "Bad Request"),
    };
    let received_bytes = body_bytes.len() as u64;
    let user_agent = parts
        .headers
        .get(http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let request_method = parts.method.to_string();
    let request_uri_str = parts.uri.to_string();
    let request_protocol = format!("{:?}", parts.version);

    let host = parts
        .headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let mut log_ctx = RequestLogContext {
        request_creation_time,
        request_start,
        received_bytes,
        user_agent,
        request_method,
        request_url: request_uri_str.clone(),
        request_protocol,
        host: host.clone(),
        actions_executed: String::from("waf,forward"),
        action_taken: ActionTaken::Unknown,
        target_ip: None,
        target_port: None,
        target_group_arn: String::from("-"),
        matched_rule_priority: String::from("-"),
        target_status_code: None,
        redirect_url: String::from("-"),
        error_reason: String::from("-"),
    };

    // Pick the listener for this connection. ALBs may have multiple
    // listeners (e.g. 80 + 443); we pick the first listener belonging
    // to this LB, since the in-process bind is single-port for now.
    let snap = match snapshot(dp, lb_arn) {
        Some(s) => s,
        None => {
            let resp = canned(StatusCode::SERVICE_UNAVAILABLE, "LB removed");
            return finalize_response(dp, lb_arn, peer_addr, &mut log_ctx, resp);
        }
    };

    let listener = match snap.listener_for_request(&parts.headers) {
        Some(l) => l,
        None => {
            let resp = canned(StatusCode::BAD_GATEWAY, "No listener");
            return finalize_response(dp, lb_arn, peer_addr, &mut log_ctx, resp);
        }
    };

    let listener_port: u16 = listener
        .port
        .and_then(|p| u16::try_from(p).ok())
        .unwrap_or(80);

    // WAF v2 evaluation: if a WebACL is associated with this LB, run
    // each request through the evaluator before the listener-rule
    // router. Block / Captcha / Challenge short-circuit; Count is
    // recorded but lets the request through.
    if let Some(waf_resp) = evaluate_waf_for_request(
        dp,
        lb_arn,
        &parts.method,
        &parts.uri,
        &parts.headers,
        &body_bytes,
        peer_addr,
    ) {
        log_ctx.actions_executed = String::from("waf");
        log_ctx.action_taken = ActionTaken::WafBlock;
        return finalize_response(dp, lb_arn, peer_addr, &mut log_ctx, waf_resp);
    }

    let rules: Vec<&Rule> = snap.rules_for_listener(&listener.arn);
    let actions = select_actions(
        &rules,
        &listener.default_actions,
        &parts.method,
        &parts.uri,
        &host,
        &parts.headers,
        Some(peer_addr.ip()),
    );

    let action = match actions.iter().min_by_key(|a| a.order.unwrap_or(0)) {
        Some(a) => a.clone(),
        None => {
            let resp = canned(StatusCode::BAD_GATEWAY, "No action");
            return finalize_response(dp, lb_arn, peer_addr, &mut log_ctx, resp);
        }
    };
    log_ctx.matched_rule_priority = "0".to_string();

    let resp = match action.action_type.to_lowercase().as_str() {
        "forward" => {
            log_ctx.actions_executed = "forward".to_string();
            log_ctx.action_taken = ActionTaken::Forward;
            forward_action(
                dp,
                &snap,
                &action,
                parts.method,
                parts.uri,
                parts.headers,
                body_bytes,
                listener_port,
                &mut log_ctx,
            )
            .await
        }
        "fixed-response" => {
            log_ctx.actions_executed = "fixed-response".to_string();
            log_ctx.action_taken = ActionTaken::FixedResponse;
            fixed_response_action(&action)
        }
        "redirect" => {
            log_ctx.actions_executed = "redirect".to_string();
            log_ctx.action_taken = ActionTaken::Redirect;
            let r = redirect_action(&action, &host, &parts.uri);
            log_ctx.redirect_url = r
                .headers()
                .get(http::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_string();
            r
        }
        "authenticate-oidc" | "authenticate-cognito" => {
            log_ctx.actions_executed = "authenticate".to_string();
            canned(
                StatusCode::NOT_IMPLEMENTED,
                "OIDC/Cognito authenticate-action is on the next-batch ELBv2 list",
            )
        }
        other => {
            log_ctx.actions_executed = other.to_string();
            canned(
                StatusCode::BAD_GATEWAY,
                &format!("Unsupported action: {other}"),
            )
        }
    };
    finalize_response(dp, lb_arn, peer_addr, &mut log_ctx, resp)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActionTaken {
    Unknown,
    Forward,
    FixedResponse,
    Redirect,
    WafBlock,
}

/// Captured per-request fields used to build the access-log line.
struct RequestLogContext {
    request_creation_time: chrono::DateTime<chrono::Utc>,
    request_start: Instant,
    received_bytes: u64,
    user_agent: String,
    request_method: String,
    request_url: String,
    request_protocol: String,
    host: String,
    actions_executed: String,
    action_taken: ActionTaken,
    target_ip: Option<String>,
    target_port: Option<u16>,
    target_group_arn: String,
    matched_rule_priority: String,
    target_status_code: Option<u16>,
    redirect_url: String,
    error_reason: String,
}

/// Common tail of every code path: emit the response back to the
/// caller and (when access logs are configured for the LB) buffer one
/// access-log line and one connection-log line.
fn finalize_response(
    dp: &DataPlane,
    lb_arn: &str,
    peer_addr: SocketAddr,
    ctx: &mut RequestLogContext,
    resp: Response<Full<Bytes>>,
) -> Response<Full<Bytes>> {
    let Some(logger) = dp.access_logger.as_ref() else {
        return resp;
    };
    // Cheap snapshot of response details before we hand the response
    // back to hyper. `body.size_hint().exact()` is set because we
    // construct every response with `Full<Bytes>`.
    use hyper::body::Body as _;
    let elb_status_code = resp.status().as_u16();
    let sent_bytes = resp.body().size_hint().exact().unwrap_or(0);
    let total_elapsed = ctx.request_start.elapsed().as_secs_f64();
    let target_processing_time = match ctx.action_taken {
        ActionTaken::Forward => total_elapsed,
        _ => 0.0,
    };
    let request_processing_time = if matches!(ctx.action_taken, ActionTaken::Forward) {
        0.0
    } else {
        total_elapsed
    };

    let elb_id = parse_lb_id(lb_arn).unwrap_or_else(|| lb_arn.to_string());
    let target_port_list = match (ctx.target_ip.as_deref(), ctx.target_port) {
        (Some(ip), Some(port)) => format!("{ip}:{port}"),
        _ => "-".to_string(),
    };
    let target_status_code_list = ctx
        .target_status_code
        .map(|c| c.to_string())
        .unwrap_or_else(|| "-".to_string());

    let access_record = AccessLogRecord {
        log_type: LogType::Access,
        timestamp: chrono::Utc::now(),
        elb_id: elb_id.clone(),
        client_ip: peer_addr.ip().to_string(),
        client_port: peer_addr.port(),
        target_ip: ctx.target_ip.clone(),
        target_port: ctx.target_port,
        request_processing_time,
        target_processing_time,
        response_processing_time: 0.0,
        elb_status_code,
        target_status_code: ctx.target_status_code,
        received_bytes: ctx.received_bytes,
        sent_bytes,
        request_method: ctx.request_method.clone(),
        request_url: ctx.request_url.clone(),
        request_protocol: ctx.request_protocol.clone(),
        user_agent: ctx.user_agent.clone(),
        ssl_cipher: "-".to_string(),
        ssl_protocol: "-".to_string(),
        target_group_arn: ctx.target_group_arn.clone(),
        trace_id: format!("Root=1-{:x}-{}", chrono::Utc::now().timestamp(), short_id()),
        domain_name: ctx.host.clone(),
        chosen_cert_arn: "-".to_string(),
        matched_rule_priority: ctx.matched_rule_priority.clone(),
        request_creation_time: ctx.request_creation_time,
        actions_executed: ctx.actions_executed.clone(),
        redirect_url: ctx.redirect_url.clone(),
        error_reason: ctx.error_reason.clone(),
        target_port_list,
        target_status_code_list,
        classification: "-".to_string(),
        classification_reason: "-".to_string(),
    };
    logger.record(lb_arn, access_record);
    // The connection log is emitted once per accepted TCP connection
    // by `accept_loop` after `serve_connection` returns. Keep-alive
    // connections that serve multiple requests therefore produce
    // exactly one connection-log entry, not one per request.
    resp
}

/// Pull the `app/<name>/<suffix>` path out of an LB ARN — that's the
/// `elb` field AWS uses in the access-log line.
fn parse_lb_id(arn: &str) -> Option<String> {
    let after_loadbalancer = arn.split(":loadbalancer/").nth(1)?;
    Some(after_loadbalancer.to_string())
}

/// Snapshot of the rules/listeners/target-groups for one LB taken
/// under the read lock so the per-request handler doesn't re-lock.
struct LbSnapshot {
    listeners: Vec<Listener>,
    rules: Vec<Rule>,
    target_groups: BTreeMap<String, TargetGroup>,
}

impl LbSnapshot {
    fn listener_for_request(&self, _headers: &HeaderMap) -> Option<&Listener> {
        // The dataplane is single-port per LB in this batch — every
        // request lands on the same hyper bind, so we pick the first
        // HTTP-protocol listener as the active one. HTTPS termination
        // is the next-batch item.
        self.listeners
            .iter()
            .find(|l| {
                l.protocol
                    .as_deref()
                    .map(|p| p.eq_ignore_ascii_case("HTTP"))
                    .unwrap_or(false)
            })
            .or_else(|| self.listeners.first())
    }

    fn rules_for_listener(&self, listener_arn: &str) -> Vec<&Rule> {
        self.rules
            .iter()
            .filter(|r| r.listener_arn == listener_arn)
            .collect()
    }
}

fn snapshot(dp: &DataPlane, lb_arn: &str) -> Option<LbSnapshot> {
    let accs = dp.state.read();
    for (_acct, st) in accs.iter() {
        if st.load_balancers.contains_key(lb_arn) {
            let listeners: Vec<Listener> = st
                .listeners
                .values()
                .filter(|l| l.load_balancer_arn == lb_arn)
                .cloned()
                .collect();
            let listener_arns: std::collections::HashSet<String> =
                listeners.iter().map(|l| l.arn.clone()).collect();
            let rules: Vec<Rule> = st
                .rules
                .values()
                .filter(|r| listener_arns.contains(&r.listener_arn))
                .cloned()
                .collect();
            let target_groups: BTreeMap<String, TargetGroup> = st
                .target_groups
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            return Some(LbSnapshot {
                listeners,
                rules,
                target_groups,
            });
        }
    }
    None
}

/// Outcome of running the WAFv2 evaluator for one ALB request.
/// `Allow` means fall through to the listener-rule router; the other
/// variants short-circuit with a synthetic response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WafEvalOutcome {
    NoAcl,
    Allow,
    Count,
    Block,
    Captcha,
    Challenge,
}

/// Run the WebACL associated with `lb_arn` (if any) against the
/// incoming request. Returns `Some(response)` when the resolved
/// [`WafAction`] is terminal (`Block`, `Captcha`, `Challenge`); returns
/// `None` when the request should fall through to the listener-rule
/// router. Count-action matches are recorded in
/// `dp.waf_count_metrics` and do not short-circuit.
fn evaluate_waf_for_request(
    dp: &DataPlane,
    lb_arn: &str,
    method: &Method,
    uri: &http::Uri,
    headers: &HeaderMap,
    body: &Bytes,
    peer_addr: SocketAddr,
) -> Option<Response<Full<Bytes>>> {
    let waf_state = dp.waf_state.as_ref()?;
    let outcome = evaluate_waf_outcome(
        waf_state,
        lb_arn,
        method.as_str(),
        uri,
        headers,
        body.as_ref(),
        peer_addr,
        Some(&dp.waf_count_metrics),
    );
    waf_outcome_to_response(outcome)
}

/// Pure-function form of [`evaluate_waf_for_request`] that resolves
/// the WAF state for one ALB and returns the resolved
/// [`WafEvalOutcome`]. Split out so unit tests can drive it without
/// constructing a full hyper data plane.
#[allow(clippy::too_many_arguments)]
pub(crate) fn evaluate_waf_outcome(
    waf_state: &SharedWafv2State,
    lb_arn: &str,
    method: &str,
    uri: &http::Uri,
    headers: &HeaderMap,
    body: &[u8],
    peer_addr: SocketAddr,
    count_metrics: Option<&Arc<Mutex<BTreeMap<String, u64>>>>,
) -> WafEvalOutcome {
    let Some(snap) = waf_snapshot_for_lb(waf_state, lb_arn) else {
        return WafEvalOutcome::NoAcl;
    };
    let header_pairs: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|s| (k.as_str().to_lowercase(), s.to_string()))
        })
        .collect();
    let path = uri.path();
    let query = uri.query().unwrap_or("");
    let req = WafRequest {
        method,
        uri: path,
        headers: &header_pairs,
        body,
        query,
        source_ip: peer_addr.ip(),
        country: None,
        body_size_bytes: body.len() as u64,
    };
    let detailed = waf_evaluate_detailed(&req, &snap.web_acl, &snap.ipsets, &snap.regex_sets);
    if !detailed.count_rules.is_empty() {
        if let Some(metrics) = count_metrics {
            let mut m = metrics.lock();
            for rule_name in &detailed.count_rules {
                let key = format!("{}|{}", snap.web_acl.arn, rule_name);
                *m.entry(key).or_insert(0) += 1;
            }
        }
    }
    match detailed.action {
        WafAction::Allow => {
            if !detailed.count_rules.is_empty() {
                WafEvalOutcome::Count
            } else {
                WafEvalOutcome::Allow
            }
        }
        WafAction::Count => WafEvalOutcome::Count,
        WafAction::Block => WafEvalOutcome::Block,
        WafAction::Captcha => WafEvalOutcome::Captcha,
        WafAction::Challenge => WafEvalOutcome::Challenge,
    }
}

fn waf_outcome_to_response(outcome: WafEvalOutcome) -> Option<Response<Full<Bytes>>> {
    match outcome {
        WafEvalOutcome::NoAcl | WafEvalOutcome::Allow | WafEvalOutcome::Count => None,
        WafEvalOutcome::Block => {
            let body = Bytes::from_static(br#"{"message":"Forbidden"}"#);
            let mut resp = Response::new(Full::new(body));
            *resp.status_mut() = StatusCode::FORBIDDEN;
            resp.headers_mut().insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            Some(resp)
        }
        WafEvalOutcome::Captcha => {
            // AWS WAF emits a 405 Method Not Allowed pseudo-response
            // when a CAPTCHA challenge fires for an unsupported
            // request method. The body content is not stable across
            // versions; an empty JSON object matches the wire
            // observation.
            let body = Bytes::from_static(br#"{"message":"Captcha"}"#);
            let mut resp = Response::new(Full::new(body));
            *resp.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
            resp.headers_mut().insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            Some(resp)
        }
        WafEvalOutcome::Challenge => {
            let body = Bytes::from_static(br#"{"message":"Challenge"}"#);
            let mut resp = Response::new(Full::new(body));
            *resp.status_mut() = StatusCode::ACCEPTED;
            resp.headers_mut().insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            Some(resp)
        }
    }
}

/// Snapshot of the WAFv2 state needed to evaluate the WebACL
/// associated with one load balancer. Cloning under the read lock
/// keeps the per-request handler from re-locking.
struct WafSnapshot {
    web_acl: WebAcl,
    ipsets: std::collections::HashMap<String, IpSet>,
    regex_sets: std::collections::HashMap<String, RegexPatternSet>,
}

fn waf_snapshot_for_lb(waf_state: &SharedWafv2State, lb_arn: &str) -> Option<WafSnapshot> {
    let st = waf_state.read();
    for account in st.accounts.values() {
        let Some(acl_arn) = account.associations.get(lb_arn) else {
            continue;
        };
        let Some(web_acl) = account.web_acls.values().find(|a| &a.arn == acl_arn) else {
            continue;
        };
        let ipsets: std::collections::HashMap<String, IpSet> = account
            .ip_sets
            .values()
            .map(|s| (s.arn.clone(), s.clone()))
            .collect();
        let regex_sets: std::collections::HashMap<String, RegexPatternSet> = account
            .regex_pattern_sets
            .values()
            .map(|s| (s.arn.clone(), s.clone()))
            .collect();
        return Some(WafSnapshot {
            web_acl: web_acl.clone(),
            ipsets,
            regex_sets,
        });
    }
    None
}

fn fixed_response_action(action: &Action) -> Response<Full<Bytes>> {
    let cfg = match action.fixed_response.as_ref() {
        Some(c) => c,
        None => return canned(StatusCode::BAD_GATEWAY, "fixed-response missing config"),
    };
    let status = cfg
        .status_code
        .parse::<u16>()
        .ok()
        .and_then(|c| StatusCode::from_u16(c).ok())
        .unwrap_or(StatusCode::OK);
    let body = cfg.message_body.clone().unwrap_or_default();
    let content_type = cfg
        .content_type
        .clone()
        .unwrap_or_else(|| "text/plain".into());
    let mut resp = Response::new(Full::new(Bytes::from(body)));
    *resp.status_mut() = status;
    if let Ok(v) = HeaderValue::from_str(&content_type) {
        resp.headers_mut().insert(http::header::CONTENT_TYPE, v);
    }
    resp
}

fn redirect_action(
    action: &Action,
    request_host: &str,
    request_uri: &http::Uri,
) -> Response<Full<Bytes>> {
    let cfg = match action.redirect.as_ref() {
        Some(c) => c,
        None => return canned(StatusCode::BAD_GATEWAY, "redirect missing config"),
    };
    let status = match cfg.status_code.as_str() {
        "HTTP_301" => StatusCode::MOVED_PERMANENTLY,
        "HTTP_302" => StatusCode::FOUND,
        _ => StatusCode::FOUND,
    };
    let proto = cfg.protocol.clone().unwrap_or_else(|| "HTTPS".into());
    let host = cfg.host.clone().unwrap_or_else(|| {
        request_host
            .split(':')
            .next()
            .unwrap_or(request_host)
            .to_string()
    });
    let path = cfg
        .path
        .clone()
        .unwrap_or_else(|| request_uri.path().to_string());
    let query = match (cfg.query.clone(), request_uri.query()) {
        (Some(q), _) if !q.is_empty() => format!("?{q}"),
        (_, Some(q)) if !q.is_empty() => format!("?{q}"),
        _ => String::new(),
    };
    let port = cfg.port.clone();
    let port_seg = port
        .and_then(|p| if p == "#{port}" { None } else { Some(p) })
        .map(|p| format!(":{p}"))
        .unwrap_or_default();
    // Only the scheme and host are case-insensitive; path and query
    // must preserve original casing (RFC 3986).
    let location = format!(
        "{proto}://{host}{port_seg}{path}{query}",
        proto = proto.to_lowercase(),
        host = host.to_lowercase(),
    );
    let mut resp = Response::new(Full::new(Bytes::new()));
    *resp.status_mut() = status;
    if let Ok(v) = HeaderValue::from_str(&location) {
        resp.headers_mut().insert(http::header::LOCATION, v);
    }
    resp
}

/// Resolve the host to forward to for a chosen target. EC2-instance targets
/// (`i-*`) and ECS bridge-mode tasks (registered with id `127.0.0.1`) publish
/// their ports on the host daemon's loopback, so they're reached via
/// `sibling_host` — `127.0.0.1` on the host, or the host alias when fakecloud
/// is itself containerized. Any other id (a real container IP/DNS) is used
/// verbatim.
fn resolve_upstream_host(target_id: &str, sibling_host: &str) -> String {
    if target_id.starts_with("i-") || target_id == "127.0.0.1" {
        sibling_host.to_string()
    } else {
        target_id.to_string()
    }
}

#[allow(clippy::too_many_arguments)]
async fn forward_action(
    dp: &DataPlane,
    snap: &LbSnapshot,
    action: &Action,
    method: Method,
    uri: http::Uri,
    headers: HeaderMap,
    body: Bytes,
    listener_port: u16,
    log_ctx: &mut RequestLogContext,
) -> Response<Full<Bytes>> {
    // Prefer `forward.target_groups` (weighted), fall back to the
    // single `target_group_arn` field for legacy actions.
    let target_groups: Vec<(String, i32)> = if let Some(fwd) = action.forward.as_ref() {
        fwd.target_groups
            .iter()
            .map(|t| (t.target_group_arn.clone(), t.weight.unwrap_or(1).max(1)))
            .collect()
    } else if let Some(tg) = action.target_group_arn.as_ref() {
        vec![(tg.clone(), 1)]
    } else {
        return canned(
            StatusCode::BAD_GATEWAY,
            "forward action without target groups",
        );
    };

    let total_weight: i32 = target_groups.iter().map(|(_, w)| *w).sum();
    if total_weight <= 0 {
        return canned(StatusCode::BAD_GATEWAY, "no positive target-group weight");
    }
    // Pick a target group (weighted round-robin). Lock is taken
    // and released within the helper — never across the upstream
    // .await, which would make the future !Send.
    let pick_idx = pick_weighted(
        &dp.rr_counters,
        &format!("tg:{}", action_key(action)),
        &target_groups,
        total_weight as usize,
    );

    let (tg_arn, _w) = target_groups[pick_idx].clone();
    log_ctx.target_group_arn = tg_arn.clone();
    let tg = match snap.target_groups.get(&tg_arn) {
        Some(t) => t,
        None => return canned(StatusCode::SERVICE_UNAVAILABLE, "Target group not found"),
    };

    // Sticky session: if the request carries an AWSALB cookie that
    // maps to a registered target in this TG, prefer that target.
    let sticky_pick: Option<String> = if action
        .forward
        .as_ref()
        .and_then(|f| f.stickiness.as_ref())
        .and_then(|s| s.enabled)
        .unwrap_or(false)
    {
        let cookie = headers
            .get(http::header::COOKIE)
            .and_then(|v| v.to_str().ok())
            .and_then(|c| extract_cookie(c, STICKY_COOKIE).map(str::to_string));
        match cookie {
            Some(c) => {
                let map = dp.sticky_targets.lock();
                map.get(&c).cloned()
            }
            None => None,
        }
    } else {
        None
    };

    let healthy: Vec<&crate::state::TargetDescription> = tg
        .targets
        .iter()
        .filter(|t| t.health.state != "unhealthy" && t.health.state != "unused")
        .collect();
    if healthy.is_empty() {
        return canned(StatusCode::SERVICE_UNAVAILABLE, "No healthy targets");
    }

    let chosen: &crate::state::TargetDescription = if let Some(sticky_key) = sticky_pick.as_ref() {
        // sticky_key format: tg_arn|target_id|target_port
        let parts: Vec<&str> = sticky_key.split('|').collect();
        if parts.len() == 3 && parts[0] == tg_arn {
            let port: Option<i32> = parts[2].parse().ok();
            healthy
                .iter()
                .copied()
                .find(|t| t.id == parts[1] && t.port == port)
                .unwrap_or_else(|| {
                    healthy[round_robin_pick(&dp.rr_counters, &tg_arn, healthy.len())]
                })
        } else {
            healthy[round_robin_pick(&dp.rr_counters, &tg_arn, healthy.len())]
        }
    } else {
        healthy[round_robin_pick(&dp.rr_counters, &tg_arn, healthy.len())]
    };

    // Build upstream URL.
    let scheme = "http";
    let upstream_host = resolve_upstream_host(&chosen.id, &dp.sibling_host);
    let upstream_port = chosen.port.or(tg.port).unwrap_or(80);
    log_ctx.target_ip = Some(upstream_host.clone());
    log_ctx.target_port = u16::try_from(upstream_port).ok();
    let path_and_query = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let upstream_url = format!("{scheme}://{upstream_host}:{upstream_port}{path_and_query}");

    // Forward via reqwest. Strip hop-by-hop headers; AWS adds
    // X-Forwarded-* headers.
    let mut req_builder = dp.upstream.request(reqwest_method(&method), &upstream_url);
    let mut forwarded_headers = headers.clone();
    strip_hop_by_hop(&mut forwarded_headers);
    let xff = match forwarded_headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
    {
        Some(existing) => format!("{existing}, 127.0.0.1"),
        None => "127.0.0.1".to_string(),
    };
    forwarded_headers.insert(
        HeaderName::from_static("x-forwarded-for"),
        HeaderValue::from_str(&xff).unwrap(),
    );
    forwarded_headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static("http"),
    );
    forwarded_headers.insert(
        HeaderName::from_static("x-forwarded-port"),
        HeaderValue::from_str(&listener_port.to_string()).unwrap(),
    );
    let trace_id = format!("Root=1-{:x}-{}", chrono::Utc::now().timestamp(), short_id());
    forwarded_headers.insert(
        HeaderName::from_static("x-amzn-trace-id"),
        HeaderValue::from_str(&trace_id).unwrap(),
    );
    for (k, v) in forwarded_headers.iter() {
        req_builder = req_builder.header(k.as_str(), v.as_bytes());
    }
    if !body.is_empty() {
        req_builder = req_builder.body(body.to_vec());
    }

    let upstream_resp = match req_builder.send().await {
        Ok(r) => r,
        Err(e) => {
            log_ctx.error_reason = format!("UpstreamError: {e}");
            return canned(StatusCode::BAD_GATEWAY, &format!("Upstream error: {e}"));
        }
    };

    let mut resp = Response::new(Full::new(Bytes::new()));
    *resp.status_mut() = upstream_resp.status();
    log_ctx.target_status_code = Some(upstream_resp.status().as_u16());
    let upstream_headers = upstream_resp.headers().clone();
    for (k, v) in upstream_headers.iter() {
        if !is_hop_by_hop(k.as_str()) {
            resp.headers_mut().append(k.clone(), v.clone());
        }
    }

    // Issue / refresh sticky cookie if stickiness is enabled.
    if action
        .forward
        .as_ref()
        .and_then(|f| f.stickiness.as_ref())
        .and_then(|s| s.enabled)
        .unwrap_or(false)
    {
        let key = format!("{tg_arn}|{}|{}", chosen.id, chosen.port.unwrap_or(0));
        let cookie_val = Uuid::new_v4().simple().to_string();
        dp.sticky_targets.lock().insert(cookie_val.clone(), key);
        let duration = action
            .forward
            .as_ref()
            .and_then(|f| f.stickiness.as_ref())
            .and_then(|s| s.duration_seconds)
            .unwrap_or(86400);
        let cookie_header = format!("{STICKY_COOKIE}={cookie_val}; Max-Age={duration}; Path=/");
        if let Ok(v) = HeaderValue::from_str(&cookie_header) {
            resp.headers_mut().append(http::header::SET_COOKIE, v);
        }
    }

    let resp_body = match upstream_resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return canned(
                StatusCode::BAD_GATEWAY,
                &format!("Upstream body error: {e}"),
            );
        }
    };
    *resp.body_mut() = Full::new(resp_body);
    resp
}

fn round_robin_pick(
    counters: &Arc<Mutex<BTreeMap<String, usize>>>,
    tg_arn: &str,
    n: usize,
) -> usize {
    if n == 0 {
        return 0;
    }
    let mut map = counters.lock();
    let c = map.entry(tg_arn.to_string()).or_insert(0);
    let pick = *c % n;
    *c = c.wrapping_add(1);
    pick
}

fn pick_weighted(
    counters: &Arc<Mutex<BTreeMap<String, usize>>>,
    key: &str,
    target_groups: &[(String, i32)],
    total_weight: usize,
) -> usize {
    if total_weight == 0 {
        return 0;
    }
    let mut map = counters.lock();
    let c = map.entry(key.to_string()).or_insert(0);
    *c = c.wrapping_add(1);
    let mod_n = (*c) % total_weight;
    drop(map);
    let mut chosen = 0usize;
    let mut acc: i32 = 0;
    for (i, (_, w)) in target_groups.iter().enumerate() {
        acc += *w;
        if (mod_n as i32) < acc {
            chosen = i;
            break;
        }
    }
    chosen
}

fn reqwest_method(m: &Method) -> reqwest::Method {
    match m.as_str() {
        "GET" => reqwest::Method::GET,
        "POST" => reqwest::Method::POST,
        "PUT" => reqwest::Method::PUT,
        "DELETE" => reqwest::Method::DELETE,
        "HEAD" => reqwest::Method::HEAD,
        "OPTIONS" => reqwest::Method::OPTIONS,
        "PATCH" => reqwest::Method::PATCH,
        "TRACE" => reqwest::Method::TRACE,
        "CONNECT" => reqwest::Method::CONNECT,
        other => reqwest::Method::from_bytes(other.as_bytes()).unwrap_or(reqwest::Method::GET),
    }
}

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

fn is_hop_by_hop(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    HOP_BY_HOP.iter().any(|&h| h == n)
}

fn strip_hop_by_hop(h: &mut HeaderMap) {
    let to_remove: Vec<HeaderName> = h
        .keys()
        .filter(|n| is_hop_by_hop(n.as_str()))
        .cloned()
        .collect();
    for n in to_remove {
        h.remove(n);
    }
}

fn canned(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(Bytes::from(msg.to_string())));
    *r.status_mut() = status;
    r.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain"),
    );
    r
}

fn action_key(a: &Action) -> String {
    if let Some(fwd) = a.forward.as_ref() {
        fwd.target_groups
            .iter()
            .map(|t| t.target_group_arn.as_str())
            .collect::<Vec<_>>()
            .join(",")
    } else if let Some(tg) = a.target_group_arn.as_ref() {
        tg.clone()
    } else {
        a.action_type.clone()
    }
}

fn extract_cookie<'a>(cookies: &'a str, name: &str) -> Option<&'a str> {
    for piece in cookies.split(';') {
        let piece = piece.trim_start();
        if let Some(rest) = piece.strip_prefix(&format!("{name}=")) {
            return Some(rest.trim_end());
        }
    }
    None
}

fn short_id() -> String {
    let id = Uuid::new_v4();
    id.simple().to_string()[0..16].to_string()
}

#[cfg(test)]
mod upstream_host_tests {
    use super::resolve_upstream_host;

    #[test]
    fn loopback_published_targets_use_sibling_host() {
        // EC2 instance + ECS bridge-mode (id "127.0.0.1") go via sibling_host.
        assert_eq!(
            resolve_upstream_host("i-0123456789abcdef0", "host.docker.internal"),
            "host.docker.internal"
        );
        assert_eq!(
            resolve_upstream_host("127.0.0.1", "host.containers.internal"),
            "host.containers.internal"
        );
        // On the host, sibling_host is 127.0.0.1 — unchanged behavior.
        assert_eq!(resolve_upstream_host("i-abc", "127.0.0.1"), "127.0.0.1");
    }

    #[test]
    fn real_container_ip_used_verbatim() {
        assert_eq!(
            resolve_upstream_host("10.0.4.7", "host.docker.internal"),
            "10.0.4.7"
        );
    }
}

#[cfg(test)]
mod waf_tests {
    use super::*;
    use chrono::Utc;
    use fakecloud_wafv2::{AccountState, Wafv2Accounts, WebAcl};
    use http::Uri;
    use parking_lot::RwLock;
    use serde_json::{json, Value};
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr};

    const ACCOUNT: &str = "123456789012";
    const LB_ARN: &str =
        "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test/abc";
    const ACL_ARN: &str = "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/xyz";

    fn web_acl_with(default: Value, rules: Vec<Value>) -> WebAcl {
        WebAcl {
            id: "xyz".into(),
            name: "test".into(),
            arn: ACL_ARN.into(),
            scope: "REGIONAL".into(),
            default_action: default,
            description: None,
            rules,
            visibility_config: json!({}),
            capacity: 0,
            lock_token: "lt".into(),
            label_namespace: "awswaf:123456789012:webacl:test:".into(),
            custom_response_bodies: BTreeMap::new(),
            captcha_config: None,
            challenge_config: None,
            token_domains: Vec::new(),
            association_config: None,
            data_protection_config: None,
            on_source_d_do_s_protection_config: None,
            application_config: None,
            retrofitted_by_firewall_manager: false,
            pre_process_firewall_manager_rule_groups: Vec::new(),
            post_process_firewall_manager_rule_groups: Vec::new(),
            managed_by_firewall_manager: false,
            created_time: Utc::now(),
        }
    }

    fn waf_state_with(acl: WebAcl, association: Option<&str>) -> SharedWafv2State {
        let mut accounts = Wafv2Accounts::new();
        let mut acct = AccountState::default();
        acct.web_acls
            .insert(("REGIONAL".into(), acl.name.clone()), acl);
        if let Some(resource) = association {
            acct.associations.insert(resource.into(), ACL_ARN.into());
        }
        accounts.accounts.insert(ACCOUNT.into(), acct);
        Arc::new(RwLock::new(accounts))
    }

    fn block_path_rule(needle: &str) -> Value {
        json!({
            "Name": "block-admin",
            "Priority": 0,
            "Action": {"Block": {}},
            "VisibilityConfig": {},
            "Statement": {
                "ByteMatchStatement": {
                    "SearchString": needle,
                    "FieldToMatch": {"UriPath": {}},
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                    "PositionalConstraint": "STARTS_WITH",
                }
            }
        })
    }

    fn count_path_rule(needle: &str) -> Value {
        json!({
            "Name": "count-admin",
            "Priority": 0,
            "Action": {"Count": {}},
            "VisibilityConfig": {},
            "Statement": {
                "ByteMatchStatement": {
                    "SearchString": needle,
                    "FieldToMatch": {"UriPath": {}},
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                    "PositionalConstraint": "STARTS_WITH",
                }
            }
        })
    }

    fn ip() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0)
    }

    #[test]
    fn request_blocked_by_associated_webacl() {
        let acl = web_acl_with(json!({"Allow": {}}), vec![block_path_rule("/admin")]);
        let waf = waf_state_with(acl, Some(LB_ARN));
        let outcome = evaluate_waf_outcome(
            &waf,
            LB_ARN,
            "GET",
            &"/admin/x".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            b"",
            ip(),
            None,
        );
        assert_eq!(outcome, WafEvalOutcome::Block);
        let resp = waf_outcome_to_response(outcome).expect("expected synthetic response");
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn request_allowed_when_no_rules_match() {
        let acl = web_acl_with(json!({"Allow": {}}), vec![block_path_rule("/admin")]);
        let waf = waf_state_with(acl, Some(LB_ARN));
        let outcome = evaluate_waf_outcome(
            &waf,
            LB_ARN,
            "GET",
            &"/public".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            b"",
            ip(),
            None,
        );
        assert_eq!(outcome, WafEvalOutcome::Allow);
        assert!(waf_outcome_to_response(outcome).is_none());
    }

    #[test]
    fn request_allowed_when_no_webacl_associated() {
        let acl = web_acl_with(json!({"Block": {}}), vec![]);
        // Note: we deliberately do NOT associate the ACL with the LB.
        let waf = waf_state_with(acl, None);
        let outcome = evaluate_waf_outcome(
            &waf,
            LB_ARN,
            "GET",
            &"/anything".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            b"",
            ip(),
            None,
        );
        assert_eq!(outcome, WafEvalOutcome::NoAcl);
        assert!(waf_outcome_to_response(outcome).is_none());
    }

    #[test]
    fn count_action_does_not_block() {
        let acl = web_acl_with(json!({"Allow": {}}), vec![count_path_rule("/admin")]);
        let waf = waf_state_with(acl, Some(LB_ARN));
        let metrics = Arc::new(Mutex::new(BTreeMap::new()));
        let outcome = evaluate_waf_outcome(
            &waf,
            LB_ARN,
            "GET",
            &"/admin/x".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            b"",
            ip(),
            Some(&metrics),
        );
        assert_eq!(outcome, WafEvalOutcome::Count);
        assert!(waf_outcome_to_response(outcome).is_none());
        let snap = metrics.lock();
        let key = format!("{ACL_ARN}|count-admin");
        assert_eq!(snap.get(&key).copied(), Some(1));
    }

    #[test]
    fn webacl_default_block_returns_403() {
        let acl = web_acl_with(json!({"Block": {}}), vec![]);
        let waf = waf_state_with(acl, Some(LB_ARN));
        let outcome = evaluate_waf_outcome(
            &waf,
            LB_ARN,
            "GET",
            &"/anything".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            b"",
            ip(),
            None,
        );
        assert_eq!(outcome, WafEvalOutcome::Block);
        let resp = waf_outcome_to_response(outcome).expect("expected synthetic response");
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }
}
