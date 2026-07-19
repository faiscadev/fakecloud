//! Server runtime helpers extracted from `main.rs` by fc-quality-audit
//! (2026-06-13): process lifecycle (panic hook, fatal exit, shutdown signal),
//! the container healthcheck probe, listener binding + port handshake, the
//! basic-auth parser, and the `POST /_fakecloud/wafv2/evaluate` admin handler.
//! `main()` keeps only bootstrap + routing; these are the self-contained
//! pieces it calls into.

use std::sync::Arc;

use tokio::net::TcpListener;

pub(crate) async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutting down");
}
/// Adapter that exposes the `fakecloud-kms` hook through the
/// `fakecloud-core::delivery::KmsHook` trait so non-KMS services can
/// call into KMS without a direct crate dependency.
/// the raw decoded form as the canonical pair.
pub(crate) fn parse_basic_auth(headers: &axum::http::HeaderMap) -> Option<(String, String)> {
    use base64::Engine as _;
    let value = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let token = value
        .strip_prefix("Basic ")
        .or_else(|| value.strip_prefix("basic "))?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(token.trim())
        .ok()?;
    let raw = String::from_utf8(decoded).ok()?;
    let (id, secret) = raw.split_once(':')?;
    Some((id.to_string(), secret.to_string()))
}
/// Emit a fatal error through the tracing pipeline, flush stderr so the
/// message survives `process::exit`, and terminate with code 1.
pub(crate) fn fatal_exit(args: std::fmt::Arguments<'_>) -> ! {
    use std::io::Write;
    tracing::error!("{args}");
    let _ = std::io::stderr().flush();
    std::process::exit(1);
}

/// Probe `<loopback>:<port>/_fakecloud/health` over a raw TCP request (the
/// published image ships no curl/wget) and return a process exit code: 0 when
/// the server answers `200 OK`, 1 otherwise. The port is taken from the same
/// `--addr`/`FAKECLOUD_ADDR` the server binds, so the container HEALTHCHECK and
/// the server agree without extra configuration.
///
/// The bind host decides which loopback to probe: a specific IP is used as-is,
/// while a wildcard / unspecified bind (`0.0.0.0`, `::`, empty) is probed on
/// both IPv4 and IPv6 loopback so a server listening only on `::` is still
/// reported healthy.
pub(crate) fn run_healthcheck(addr: &str) -> i32 {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    let (host, port) = match split_host_port(addr) {
        Some(hp) => hp,
        None => {
            eprintln!("healthcheck: could not parse host:port from addr {addr:?}");
            return 1;
        }
    };
    let candidates: Vec<IpAddr> = match host.parse::<IpAddr>() {
        // Wildcard / unspecified bind: try both loopback families.
        Ok(ip) if ip.is_unspecified() => {
            vec![
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(Ipv6Addr::LOCALHOST),
            ]
        }
        Ok(ip) => vec![ip],
        // Non-IP host (e.g. "localhost" or empty): try both loopbacks.
        Err(_) => vec![
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
        ],
    };
    for ip in &candidates {
        if healthcheck_probe(*ip, port) {
            return 0;
        }
    }
    eprintln!("healthcheck: no healthy response on port {port} (tried {candidates:?})");
    1
}

/// Split a bind address into its host and port. Handles bracketed IPv6
/// (`[::1]:4566`), plain `host:port`, and `0.0.0.0:4566`.
pub(crate) fn split_host_port(addr: &str) -> Option<(String, u16)> {
    if let Some(rest) = addr.strip_prefix('[') {
        // [ipv6]:port
        let (host, tail) = rest.split_once(']')?;
        let port = tail.strip_prefix(':')?.parse().ok()?;
        return Some((host.to_string(), port));
    }
    let (host, port) = addr.rsplit_once(':')?;
    Some((host.to_string(), port.parse().ok()?))
}

/// Open one probe to `ip:port`, send the health request, and return whether the
/// server answered with a 2xx status line.
pub(crate) fn healthcheck_probe(ip: std::net::IpAddr, port: u16) -> bool {
    use std::io::{Read, Write};
    let timeout = std::time::Duration::from_secs(2);
    let Ok(mut socket) = std::net::TcpStream::connect((ip, port)) else {
        return false;
    };
    let _ = socket.set_read_timeout(Some(timeout));
    let _ = socket.set_write_timeout(Some(timeout));
    let request = "GET /_fakecloud/health HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    if socket.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = String::new();
    if socket.read_to_string(&mut response).is_err() {
        return false;
    }
    // Status line looks like `HTTP/1.0 200 OK`. Accept any 2xx.
    response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .is_some_and(|code| code.starts_with('2'))
}
/// Route panics through `tracing::error!` so they show up in CI logs with
/// the same formatting as regular errors. Runs the default hook afterwards
/// so the process keeps its usual backtrace behaviour for developers
/// running locally with `RUST_BACKTRACE=1`.
pub(crate) fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let payload = info
            .payload()
            .downcast_ref::<&'static str>()
            .copied()
            .map(|s| s.to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic>".to_string());
        tracing::error!(location = %location, payload = %payload, "panic");
        default(info);
    }));
}
/// Prefix used to announce the bound port on stdout. `fakecloud-testkit`
/// scans stdout for the first line starting with this prefix to discover
/// the OS-assigned port when the server was launched with `--addr :0`.
pub(crate) const PORT_HANDSHAKE_PREFIX: &str = "FAKECLOUD_PORT=";
/// Body shape accepted by `POST /_fakecloud/wafv2/evaluate`:
///
/// ```json
/// {
///   "webAclArn": "...",
///   "accountId": "000000000000",      // optional, defaults to server account
///   "request": {
///     "method": "POST",                 // optional, default "GET"
///     "uri": "/admin/users",            // optional, default "/"
///     "query": "id=1",                  // optional
///     "sourceIp": "10.1.2.3",           // optional, default 127.0.0.1
///     "headers": [["X-Forwarded-For", "..."]], // optional
///     "body": "...",                    // optional, base64-encoded if `bodyB64`
///     "bodyB64": "Li4u",                // optional, alternative to `body`
///     "bodySize": 12345,                // optional, defaults to body.len()
///     "country": "DE",                  // optional, also overridable via header
///     "nowEpochSecs": 1700000000        // optional, default = system clock
///   }
/// }
/// ```
///
/// The `country` header `x-fakecloud-geo-country` on the synthetic request
/// is honored as an override, matching the dataplane behavior that lands
/// in W2 (real GeoIP lookup is out of scope for W1).
pub(crate) fn wafv2_evaluate_admin(
    waf_state: &fakecloud_wafv2::SharedWafv2State,
    rate_limiter: &Arc<fakecloud_wafv2::RateLimiter>,
    default_account: &str,
    body: &serde_json::Value,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    use axum::http::StatusCode;
    let bad = |msg: &str| -> (StatusCode, axum::Json<serde_json::Value>) {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({"error": msg})),
        )
    };
    let Some(arn) = body.get("webAclArn").and_then(|v| v.as_str()) else {
        return bad("`webAclArn` is required");
    };
    let account_id = body
        .get("accountId")
        .and_then(|v| v.as_str())
        .unwrap_or(default_account);
    let req_obj = body
        .get("request")
        .cloned()
        .unwrap_or(serde_json::json!({}));
    let method = req_obj
        .get("method")
        .and_then(|v| v.as_str())
        .unwrap_or("GET")
        .to_string();
    let uri = req_obj
        .get("uri")
        .and_then(|v| v.as_str())
        .unwrap_or("/")
        .to_string();
    let query = req_obj
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let source_ip_str = req_obj
        .get("sourceIp")
        .and_then(|v| v.as_str())
        .unwrap_or("127.0.0.1");
    let Ok(source_ip) = source_ip_str.parse::<std::net::IpAddr>() else {
        return bad("`request.sourceIp` is not a valid IP address");
    };
    // Headers come in as either [[name, val], ...] or {name: val} for ergonomics.
    let mut headers: Vec<(String, String)> = Vec::new();
    if let Some(arr) = req_obj.get("headers").and_then(|v| v.as_array()) {
        for item in arr {
            if let Some(pair) = item.as_array() {
                if pair.len() == 2 {
                    if let (Some(k), Some(v)) = (pair[0].as_str(), pair[1].as_str()) {
                        headers.push((k.to_string(), v.to_string()));
                    }
                }
            } else if let Some(obj) = item.as_object() {
                if let (Some(k), Some(v)) = (
                    obj.get("name").and_then(|x| x.as_str()),
                    obj.get("value").and_then(|x| x.as_str()),
                ) {
                    headers.push((k.to_string(), v.to_string()));
                }
            }
        }
    } else if let Some(obj) = req_obj.get("headers").and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(v) = v.as_str() {
                headers.push((k.clone(), v.to_string()));
            }
        }
    }
    // Body: prefer raw `body` string, fall back to base64-encoded `bodyB64`.
    let body_bytes: Vec<u8> = if let Some(b64) = req_obj.get("bodyB64").and_then(|v| v.as_str()) {
        match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64) {
            Ok(b) => b,
            Err(_) => return bad("`request.bodyB64` is not valid base64"),
        }
    } else if let Some(s) = req_obj.get("body").and_then(|v| v.as_str()) {
        s.as_bytes().to_vec()
    } else {
        Vec::new()
    };
    let body_size = req_obj
        .get("bodySize")
        .and_then(|v| v.as_u64())
        .unwrap_or(body_bytes.len() as u64);
    // Country override precedence: explicit `country` field, else the
    // `x-fakecloud-geo-country` header on the synthetic request.
    let country_explicit = req_obj
        .get("country")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let country_header = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(fakecloud_wafv2::FAKECLOUD_GEO_COUNTRY_HEADER))
        .map(|(_, v)| v.clone());
    let country = country_explicit.or(country_header);
    let now_epoch_secs = req_obj
        .get("nowEpochSecs")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
        });
    // Snapshot the WebACL plus the IpSets and RegexPatternSets it might
    // reference. We hold the read lock only long enough to clone what we
    // need — evaluation itself does not need the lock.
    let snapshot = {
        let state = waf_state.read();
        let Some(account) = state.accounts.get(account_id) else {
            return bad("account not found");
        };
        let acl = account.web_acls.values().find(|a| a.arn == arn).cloned();
        let ip_sets: std::collections::HashMap<String, fakecloud_wafv2::IpSet> = account
            .ip_sets
            .values()
            .map(|s| (s.arn.clone(), s.clone()))
            .collect();
        let regex_sets: std::collections::HashMap<String, fakecloud_wafv2::RegexPatternSet> =
            account
                .regex_pattern_sets
                .values()
                .map(|s| (s.arn.clone(), s.clone()))
                .collect();
        let rule_groups: std::collections::HashMap<String, fakecloud_wafv2::RuleGroup> = account
            .rule_groups
            .values()
            .map(|g| (g.arn.clone(), g.clone()))
            .collect();
        (acl, ip_sets, regex_sets, rule_groups)
    };
    let (acl, ip_sets, regex_sets, rule_groups) = snapshot;
    let Some(acl) = acl else {
        return bad("WebACL not found");
    };
    let request = fakecloud_wafv2::WafRequest {
        method: &method,
        uri: &uri,
        headers: &headers,
        body: &body_bytes,
        query: &query,
        source_ip,
        country: country.as_deref(),
        body_size_bytes: body_size,
    };
    let verdict = fakecloud_wafv2::evaluate_web_acl(
        &acl,
        &request,
        &ip_sets,
        &regex_sets,
        &rule_groups,
        rate_limiter,
        now_epoch_secs,
    );
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "action": verdict.action.as_str(),
            "blocked": verdict.blocked,
            "terminatingRuleId": verdict.terminating_rule_id,
            "labels": verdict.labels,
            "countRules": verdict.count_rules,
            "customResponseStatus": verdict.custom_response_status,
            "customResponseBodyKey": verdict.custom_response_body_key,
        })),
    )
}
/// Bind a `TcpListener` and return the listener together with the address
/// Email dispatcher used by Cognito's verification flow: append a
/// `SentEmail` to the SES state for the right account so the email is
/// observable through the standard `/_fakecloud/ses/sent` introspection.
pub(crate) async fn bind_listener(
    addr: &str,
) -> std::io::Result<(TcpListener, std::net::SocketAddr)> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    Ok((listener, bound))
}
/// Emit the port-handshake line used by test harnesses. Taking a generic
/// writer keeps this testable without capturing process stdout.
pub(crate) fn announce_bound_port<W: std::io::Write>(
    port: u16,
    writer: &mut W,
) -> std::io::Result<()> {
    writeln!(writer, "{PORT_HANDSHAKE_PREFIX}{port}")
}
/// Generate the per-process bearer token used by the K8s backend's
/// internal artifact endpoints. 32 random bytes, hex-encoded, never
/// persisted. Pod specs embed this value at launch; teardown
/// invalidates it by virtue of the next fakecloud process minting a
/// fresh one.
pub(crate) fn generate_k8s_internal_token() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
/// Build a public-facing endpoint URL from the address the server actually
/// bound to. Wildcard hosts (``0.0.0.0`` / ``[::]``) are rewritten to
/// ``localhost`` so the URL is useful when embedded in resource identifiers
/// such as SQS queue URLs or SNS ARNs.
pub(crate) fn endpoint_url_from_addr(addr: std::net::SocketAddr) -> String {
    let port = addr.port();
    let host_str = if addr.ip().is_unspecified() {
        "localhost".to_string()
    } else {
        match addr.ip() {
            std::net::IpAddr::V4(ip) => ip.to_string(),
            std::net::IpAddr::V6(ip) => format!("[{ip}]"),
        }
    };
    format!("http://{host_str}:{port}")
}
#[cfg(test)]
mod endpoint_url_tests {
    use super::*;
    #[test]
    fn wildcard_v4_resolves_to_localhost() {
        let addr: std::net::SocketAddr = "0.0.0.0:4566".parse().unwrap();
        assert_eq!(endpoint_url_from_addr(addr), "http://localhost:4566");
    }
    #[test]
    fn wildcard_v6_resolves_to_localhost() {
        let addr: std::net::SocketAddr = "[::]:4566".parse().unwrap();
        assert_eq!(endpoint_url_from_addr(addr), "http://localhost:4566");
    }
    #[test]
    fn explicit_loopback_is_preserved() {
        let addr: std::net::SocketAddr = "127.0.0.1:9000".parse().unwrap();
        assert_eq!(endpoint_url_from_addr(addr), "http://127.0.0.1:9000");
    }
    #[test]
    fn explicit_ipv6_loopback_is_bracketed() {
        let addr: std::net::SocketAddr = "[::1]:9000".parse().unwrap();
        assert_eq!(endpoint_url_from_addr(addr), "http://[::1]:9000");
    }
    #[test]
    fn os_assigned_port_is_reflected() {
        // Simulate the common test-harness case: bind on :0 and check that
        // the returned URL contains the OS-assigned port, not zero.
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let bound = listener.local_addr().unwrap();
        let url = endpoint_url_from_addr(bound);
        assert!(url.starts_with("http://127.0.0.1:"));
        let port_str = url.trim_start_matches("http://127.0.0.1:");
        let port: u16 = port_str.parse().unwrap();
        assert!(port > 0);
    }
}
#[cfg(test)]
mod startup_tests {
    use super::*;

    // Spawn a one-shot TCP server that replies with `status_line`, then probe
    // it via run_healthcheck. Returns the exit code.
    fn probe_with_status(status_line: &'static str) -> i32 {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                let mut buf = [0u8; 256];
                let _ = sock.read(&mut buf);
                let _ = sock.write_all(
                    format!("{status_line}\r\nConnection: close\r\n\r\nbody").as_bytes(),
                );
            }
        });
        let code = run_healthcheck(&format!("0.0.0.0:{port}"));
        handle.join().unwrap();
        code
    }

    #[test]
    fn healthcheck_zero_on_2xx() {
        assert_eq!(probe_with_status("HTTP/1.0 200 OK"), 0);
    }

    #[test]
    fn healthcheck_one_on_5xx() {
        assert_eq!(probe_with_status("HTTP/1.0 500 Internal Server Error"), 1);
    }

    #[test]
    fn healthcheck_one_when_unreachable() {
        // Grab a port then drop the listener so nothing is accepting on it.
        let port = {
            let l = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            l.local_addr().unwrap().port()
        };
        assert_eq!(run_healthcheck(&format!("0.0.0.0:{port}")), 1);
    }

    #[test]
    fn healthcheck_one_on_unparseable_addr() {
        assert_eq!(run_healthcheck("not-an-addr"), 1);
    }

    #[test]
    fn split_host_port_handles_ipv4_ipv6_and_wildcard() {
        assert_eq!(
            split_host_port("0.0.0.0:4566"),
            Some(("0.0.0.0".to_string(), 4566))
        );
        assert_eq!(
            split_host_port("[::1]:4566"),
            Some(("::1".to_string(), 4566))
        );
        assert_eq!(split_host_port("[::]:80"), Some(("::".to_string(), 80)));
        assert_eq!(split_host_port("no-colon"), None);
    }

    #[test]
    fn healthcheck_probes_ipv6_loopback_when_bound_on_ipv6() {
        // Cubic: a server bound on IPv6 must still be reported healthy. Bind an
        // IPv6 loopback listener and probe it via a bracketed IPv6 addr.
        use std::io::{Read, Write};
        let listener = match std::net::TcpListener::bind(("::1", 0)) {
            Ok(l) => l,
            // Some CI sandboxes lack IPv6 loopback; skip rather than fail.
            Err(_) => return,
        };
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                let mut buf = [0u8; 256];
                let _ = sock.read(&mut buf);
                let _ = sock.write_all(b"HTTP/1.0 200 OK\r\nConnection: close\r\n\r\nok");
            }
        });
        let code = run_healthcheck(&format!("[::1]:{port}"));
        handle.join().unwrap();
        assert_eq!(code, 0);
    }

    #[test]
    fn announce_bound_port_uses_tagged_prefix() {
        let mut buf: Vec<u8> = Vec::new();
        announce_bound_port(4566, &mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "FAKECLOUD_PORT=4566\n",);
    }
    #[test]
    fn announce_bound_port_prefix_matches_constant() {
        // Guard against accidental drift between the constant and the
        // literal parser in fakecloud-testkit.
        assert_eq!(PORT_HANDSHAKE_PREFIX, "FAKECLOUD_PORT=");
    }
    #[tokio::test]
    async fn bind_listener_reports_os_assigned_port() {
        let (_listener, bound) = bind_listener("127.0.0.1:0").await.unwrap();
        assert!(bound.port() > 0);
        assert_eq!(bound.ip().to_string(), "127.0.0.1");
    }
    #[tokio::test]
    async fn bind_listener_errors_on_invalid_addr() {
        assert!(bind_listener("not-a-socket-addr").await.is_err());
    }
}
