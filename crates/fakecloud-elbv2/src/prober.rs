use std::time::Duration;

use chrono::Utc;
use reqwest::Client;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, trace};

use crate::state::{SharedElbv2State, TargetGroup};

const TICK_SECONDS: u64 = 1;
const ENV_DISABLE: &str = "FAKECLOUD_ELBV2_DISABLE_HEALTH_PROBES";

pub fn probes_enabled() -> bool {
    !matches!(
        std::env::var(ENV_DISABLE).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

pub fn spawn_prober(state: SharedElbv2State) {
    if !probes_enabled() {
        debug!("ELBv2 health probes disabled via {ENV_DISABLE}");
        return;
    }
    // Detect once: EC2-instance (`i-*`) and ECS bridge-mode (`127.0.0.1`)
    // targets publish their ports on the host daemon's loopback, reached via
    // `sibling_host` (`127.0.0.1` on the host, or the host alias when fakecloud
    // is itself containerized). This mirrors the data plane's
    // `resolve_upstream_host` so probes succeed under FAKECLOUD_IN_CONTAINER=1
    // instead of hitting fakecloud's own loopback and 503'ing every target.
    let sibling_host = fakecloud_core::container_net::detect_container_cli()
        .map(|cli| fakecloud_core::container_net::HostNetworking::detect(&cli).sibling_host)
        .unwrap_or_else(|| "127.0.0.1".to_string());
    tokio::spawn(async move {
        // No client-level timeout: each probe wraps the request in a
        // `tokio::time::timeout` keyed off the target group's
        // `HealthCheckTimeoutSeconds` so that knob is authoritative.
        let client = match Client::builder().danger_accept_invalid_certs(true).build() {
            Ok(c) => c,
            Err(e) => {
                debug!("ELBv2 prober: failed to build HTTP client: {e}");
                return;
            }
        };
        let mut tick = tokio::time::interval(Duration::from_secs(TICK_SECONDS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            run_one_pass(&state, &client, &sibling_host).await;
        }
    });
}

#[derive(Clone)]
struct ProbeJob {
    account_id: String,
    tg_arn: String,
    target_id: String,
    target_port: Option<i32>,
    protocol: String,
    port: i32,
    path: String,
    matcher: String,
    timeout_secs: u64,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
}

async fn run_one_pass(state: &SharedElbv2State, client: &Client, sibling_host: &str) {
    let now = Utc::now();
    let jobs: Vec<ProbeJob> = {
        let accounts = state.read();
        let mut out = Vec::new();
        for (account_id, st) in accounts.iter() {
            for tg in st.target_groups.values() {
                if !tg.health_check_enabled {
                    continue;
                }
                let interval = tg.health_check_interval_seconds.max(1) as i64;
                for t in &tg.targets {
                    if let Some(last) = t.last_probe_at {
                        if (now - last).num_seconds() < interval {
                            continue;
                        }
                    }
                    let Some(job) = build_job(account_id, tg, t) else {
                        continue;
                    };
                    out.push(job);
                }
            }
        }
        out
    };

    if jobs.is_empty() {
        return;
    }

    let results = futures_concurrent(jobs, client, sibling_host).await;

    let mut accounts = state.write();
    for (job, ok) in results {
        let Some(st) = accounts.get_mut(&job.account_id) else {
            continue;
        };
        let Some(tg) = st.target_groups.get_mut(&job.tg_arn) else {
            continue;
        };
        let Some(t) = tg
            .targets
            .iter_mut()
            .find(|t| t.id == job.target_id && t.port == job.target_port)
        else {
            continue;
        };
        t.last_probe_at = Some(now);
        if ok {
            t.consecutive_success = t.consecutive_success.saturating_add(1);
            t.consecutive_failure = 0;
            if t.consecutive_success >= job.healthy_threshold && t.health.state != "healthy" {
                t.health.state = "healthy".into();
                t.health.reason = None;
                t.health.description = None;
                trace!(target_id = %t.id, "ELBv2 prober: target -> healthy");
            }
        } else {
            t.consecutive_failure = t.consecutive_failure.saturating_add(1);
            t.consecutive_success = 0;
            if t.consecutive_failure >= job.unhealthy_threshold && t.health.state != "unhealthy" {
                t.health.state = "unhealthy".into();
                t.health.reason = Some("Target.FailedHealthChecks".into());
                t.health.description = Some(format!(
                    "Health checks failed with these codes after {} consecutive failures",
                    t.consecutive_failure
                ));
                trace!(target_id = %t.id, "ELBv2 prober: target -> unhealthy");
            }
        }
    }
}

fn build_job(
    account_id: &str,
    tg: &TargetGroup,
    target: &crate::state::TargetDescription,
) -> Option<ProbeJob> {
    let tg_protocol = tg
        .health_check_protocol
        .as_deref()
        .or(tg.protocol.as_deref())
        .unwrap_or("HTTP")
        .to_uppercase();
    let port: i32 = match tg.health_check_port.as_deref() {
        Some("traffic-port") | None => target.port?,
        Some(s) => s.parse().ok()?,
    };
    if !(1..=65535).contains(&port) {
        return None;
    }
    let path = tg
        .health_check_path
        .clone()
        .unwrap_or_else(|| "/".to_string());
    let matcher = tg
        .matcher_http_code
        .clone()
        .unwrap_or_else(|| "200".to_string());
    let timeout_secs = tg.health_check_timeout_seconds.max(1) as u64;
    Some(ProbeJob {
        account_id: account_id.to_string(),
        tg_arn: tg.arn.clone(),
        target_id: target.id.clone(),
        target_port: target.port,
        protocol: tg_protocol,
        port,
        path,
        matcher,
        timeout_secs,
        healthy_threshold: tg.healthy_threshold_count.max(1) as u32,
        unhealthy_threshold: tg.unhealthy_threshold_count.max(1) as u32,
    })
}

async fn futures_concurrent(
    jobs: Vec<ProbeJob>,
    client: &Client,
    sibling_host: &str,
) -> Vec<(ProbeJob, bool)> {
    let mut handles = Vec::with_capacity(jobs.len());
    for job in jobs {
        let client = client.clone();
        let sibling_host = sibling_host.to_string();
        handles.push(tokio::spawn(async move {
            let ok = probe(&client, &job, &sibling_host).await;
            (job, ok)
        }));
    }
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        if let Ok(pair) = h.await {
            out.push(pair);
        }
    }
    out
}

/// Resolve the host to probe for a target. EC2-instance (`i-*`) and ECS
/// bridge-mode (`127.0.0.1`) targets publish on the host daemon's loopback and
/// are reached via `sibling_host`; any other id (a real container IP/DNS) is
/// used verbatim. Mirrors the data plane's `resolve_upstream_host`.
fn resolve_probe_host(target_id: &str, sibling_host: &str) -> String {
    if target_id.starts_with("i-") || target_id == "127.0.0.1" {
        sibling_host.to_string()
    } else {
        target_id.to_string()
    }
}

async fn probe(client: &Client, job: &ProbeJob, sibling_host: &str) -> bool {
    let host = resolve_probe_host(&job.target_id, sibling_host);
    let probe_timeout = Duration::from_secs(job.timeout_secs);

    match job.protocol.as_str() {
        "HTTP" | "HTTPS" => {
            let scheme = if job.protocol == "HTTPS" {
                "https"
            } else {
                "http"
            };
            let url = format!(
                "{scheme}://{host}:{port}{path}",
                port = job.port,
                path = job.path
            );
            match timeout(probe_timeout, client.get(&url).send()).await {
                Ok(Ok(resp)) => matcher_matches(&job.matcher, resp.status().as_u16()),
                _ => false,
            }
        }
        "TCP" | "TLS" => {
            let Ok(port) = u16::try_from(job.port) else {
                return false;
            };
            matches!(
                timeout(probe_timeout, TcpStream::connect((host.as_str(), port))).await,
                Ok(Ok(_))
            )
        }
        // UDP / GENEVE / unknown — AWS marks UDP healthy without active probing
        _ => true,
    }
}

fn matcher_matches(spec: &str, code: u16) -> bool {
    for token in spec.split(',') {
        let t = token.trim();
        if t.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = t.split_once('-') {
            let lo: u16 = lo.trim().parse().unwrap_or(0);
            let hi: u16 = hi.trim().parse().unwrap_or(0);
            if code >= lo && code <= hi {
                return true;
            }
        } else if let Ok(want) = t.parse::<u16>() {
            if code == want {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matcher_matches_single_code() {
        assert!(matcher_matches("200", 200));
        assert!(!matcher_matches("200", 201));
    }

    #[test]
    fn matcher_matches_range() {
        assert!(matcher_matches("200-299", 200));
        assert!(matcher_matches("200-299", 250));
        assert!(matcher_matches("200-299", 299));
        assert!(!matcher_matches("200-299", 300));
    }

    #[test]
    fn matcher_matches_list() {
        assert!(matcher_matches("200,301,404", 200));
        assert!(matcher_matches("200,301,404", 301));
        assert!(matcher_matches("200,301,404", 404));
        assert!(!matcher_matches("200,301,404", 500));
    }

    #[test]
    fn matcher_matches_mixed() {
        assert!(matcher_matches("200,300-399", 350));
        assert!(matcher_matches("200,300-399", 200));
        assert!(!matcher_matches("200,300-399", 400));
    }

    #[test]
    fn resolve_probe_host_uses_sibling_for_ec2_and_bridge() {
        // EC2 instance ids resolve to the sibling host (host alias when
        // fakecloud is itself containerized), not fakecloud's own loopback.
        assert_eq!(
            resolve_probe_host("i-0123456789abcdef0", "host.docker.internal"),
            "host.docker.internal"
        );
        // ECS bridge-mode tasks register as 127.0.0.1 and resolve the same way.
        assert_eq!(
            resolve_probe_host("127.0.0.1", "host.docker.internal"),
            "host.docker.internal"
        );
        // On the host, sibling_host is plain loopback.
        assert_eq!(resolve_probe_host("i-abc", "127.0.0.1"), "127.0.0.1");
    }

    #[test]
    fn resolve_probe_host_passes_real_container_ip_verbatim() {
        assert_eq!(
            resolve_probe_host("10.0.1.5", "host.docker.internal"),
            "10.0.1.5"
        );
        assert_eq!(
            resolve_probe_host("ip-10-0-1-5.ec2.internal", "host.docker.internal"),
            "ip-10-0-1-5.ec2.internal"
        );
    }
}
