//! Re-exports of `fakecloud_testkit` items used by the e2e suite, plus a
//! couple of crate-local helpers (`gunzip`) that don't belong in testkit.
//!
//! `TestServer`, including every per-service SDK client factory and the
//! `aws_cli` wrapper, lives in `fakecloud_testkit` under the `sdk-clients`
//! feature which this crate enables in its `Cargo.toml`.

#![allow(dead_code, unused_imports)]

pub use fakecloud_testkit::{data_path_for, run_until_exit, CliOutput, TestServer};

/// The container CLI the e2e server drives (`FAKECLOUD_CONTAINER_CLI`, else
/// the same docker-then-podman detection the test server uses).
pub fn container_cli() -> String {
    std::env::var("FAKECLOUD_CONTAINER_CLI")
        .ok()
        .filter(|v| !v.is_empty() && v != "false")
        .unwrap_or_else(fakecloud_testkit::detect_container_cli)
}

/// Run the container CLI, or `None` when it can't run or fails, so callers
/// never mistake an unreachable daemon for "no such volume".
fn cli_stdout(args: &[String]) -> Option<String> {
    let out = std::process::Command::new(container_cli())
        .args(args)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Removes, on drop, every container data volume fakecloud created for a
/// persistent `--data-path` (they carry a `fakecloud-data-path=<dir>` label),
/// plus any extra volume a test made by hand. Running on drop means a failing
/// test cleans up too, so durable volumes never pile up on the developer's
/// daemon. Declare it after the `TempDir` and before the `TestServer`, so the
/// server (and the containers mounting the volumes) is gone first.
pub struct DataVolumeGuard {
    data_path: String,
    extra: Vec<String>,
}

impl DataVolumeGuard {
    pub fn new(data_path: &std::path::Path) -> Self {
        // The server labels volumes with the canonical path.
        let canonical = std::fs::canonicalize(data_path).unwrap_or_else(|_| data_path.into());
        Self {
            data_path: canonical.to_string_lossy().into_owned(),
            extra: Vec::new(),
        }
    }

    /// Also remove `name` on drop.
    pub fn also_remove(&mut self, name: &str) {
        self.extra.push(name.to_string());
    }

    /// Data volumes fakecloud created for this data dir, narrowed by extra
    /// `key=value` label filters (e.g. `fakecloud-rds=<id>`).
    /// Panics if the container CLI fails, so an assertion never passes on
    /// an unreachable daemon.
    pub fn volumes(&self, labels: &[&str]) -> Vec<String> {
        self.try_volumes(labels)
            .expect("container CLI failed to list volumes")
    }

    fn try_volumes(&self, labels: &[&str]) -> Option<Vec<String>> {
        let mut args = vec![
            "volume".to_string(),
            "ls".to_string(),
            "-q".to_string(),
            "--filter".to_string(),
            format!("label=fakecloud-data-path={}", self.data_path),
        ];
        for label in labels {
            args.push("--filter".to_string());
            args.push(format!("label={label}"));
        }
        Some(
            cli_stdout(&args)?
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_string)
                .collect(),
        )
    }
}

/// Whether the daemon has a volume named `name`. Panics if the container CLI
/// fails, so a "volume is gone" poll never passes on an unreachable daemon.
pub fn volume_exists(name: &str) -> bool {
    let listing = cli_stdout(&[
        "volume".to_string(),
        "ls".to_string(),
        "-q".to_string(),
        "--filter".to_string(),
        format!("name={name}"),
    ])
    .expect("container CLI failed to list volumes");
    // `name=` is a substring filter: match exactly.
    listing.lines().any(|l| l.trim() == name)
}

impl Drop for DataVolumeGuard {
    fn drop(&mut self) {
        let cli = container_cli();
        // Known names are removed even if listing fails; a failed listing is
        // reported rather than silently leaking the data dir's volumes.
        let mut names = match self.try_volumes(&[]) {
            Some(names) => names,
            None => {
                eprintln!(
                    "DataVolumeGuard: could not list volumes for {}; they may be left behind",
                    self.data_path
                );
                Vec::new()
            }
        };
        names.append(&mut self.extra);
        for name in names {
            // A container a crashed server left behind still mounts it.
            if let Ok(out) = std::process::Command::new(&cli)
                .args(["ps", "-aq", "--filter", &format!("volume={name}")])
                .output()
            {
                for id in String::from_utf8_lossy(&out.stdout).split_whitespace() {
                    let _ = std::process::Command::new(&cli)
                        .args(["rm", "-f", id])
                        .output();
                }
            }
            let _ = std::process::Command::new(&cli)
                .args(["volume", "rm", "-f", &name])
                .output();
        }
    }
}

/// Poll SQS ReceiveMessage until at least `n` messages have been collected
/// across one or more calls, or the deadline elapses. Returns whatever was
/// gathered so the caller's assertion can produce a useful failure message.
///
/// Useful when an upstream system (SES → SNS → SQS, EventBridge → SQS, etc.)
/// publishes asynchronously and a fixed sleep would either be too short under
/// CI load or wastefully long under local development.
pub async fn sqs_receive_at_least(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    n: usize,
    deadline: std::time::Duration,
) -> Vec<aws_sdk_sqs::types::Message> {
    let until = std::time::Instant::now() + deadline;
    let mut all: Vec<aws_sdk_sqs::types::Message> = Vec::new();
    while std::time::Instant::now() < until {
        let resp = sqs
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(10)
            .send()
            .await
            .unwrap();
        all.extend(resp.messages().to_vec());
        if all.len() >= n {
            return all;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    all
}

/// Poll `probe` every 50ms until it returns `Some(value)` or the deadline
/// elapses. Returns `Some(value)` on first match or `None` on timeout. The
/// caller asserts on the returned `Option` so the failure message reflects
/// the test's intent rather than a generic timeout panic.
///
/// Use this in place of `tokio::time::sleep(fixed)` followed by a one-shot
/// assertion: a fixed sleep is either too short (flake under CI load) or too
/// long (wastes wall clock). Polling adapts to actual readiness.
pub async fn wait_until<F, Fut, T>(deadline: std::time::Duration, mut probe: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let until = std::time::Instant::now() + deadline;
    loop {
        if let Some(v) = probe().await {
            return Some(v);
        }
        if std::time::Instant::now() >= until {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Decompress gzipped data.
pub fn gunzip(data: &[u8]) -> Vec<u8> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(data);
    let mut result = Vec::new();
    decoder.read_to_end(&mut result).unwrap();
    result
}

/// Poll DescribeDBInstances until the instance reports
/// `db_instance_status = "available"`, then return the populated
/// `DbInstance`. CreateDBInstance returns a `creating` placeholder
/// immediately; this helper bridges tests that need the endpoint.
pub async fn wait_for_db_available(
    rds: &aws_sdk_rds::Client,
    db_instance_identifier: &str,
    max_secs: u64,
) -> aws_sdk_rds::types::DbInstance {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    while std::time::Instant::now() < deadline {
        if let Ok(resp) = rds
            .describe_db_instances()
            .db_instance_identifier(db_instance_identifier)
            .send()
            .await
        {
            for inst in resp.db_instances() {
                if inst.db_instance_status() == Some("available") {
                    return inst.clone();
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!(
        "DB instance {} did not reach 'available' within {}s",
        db_instance_identifier, max_secs
    );
}

/// Poll DescribeCacheClusters until the cluster reaches "available" and return
/// it. CreateCacheCluster now returns "creating" and starts the backing
/// container in the background (bug-audit 3.2), so tests that read the endpoint
/// or assert availability must wait first.
pub async fn wait_for_cache_cluster_available(
    client: &aws_sdk_elasticache::Client,
    cache_cluster_id: &str,
    max_secs: u64,
) -> aws_sdk_elasticache::types::CacheCluster {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    while std::time::Instant::now() < deadline {
        if let Ok(resp) = client
            .describe_cache_clusters()
            .cache_cluster_id(cache_cluster_id)
            .show_cache_node_info(true)
            .send()
            .await
        {
            for c in resp.cache_clusters() {
                if c.cache_cluster_status() == Some("available") {
                    return c.clone();
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("cache cluster {cache_cluster_id} did not reach 'available' within {max_secs}s");
}

/// Poll DescribeReplicationGroups until the group reaches "available".
pub async fn wait_for_replication_group_available(
    client: &aws_sdk_elasticache::Client,
    replication_group_id: &str,
    max_secs: u64,
) -> aws_sdk_elasticache::types::ReplicationGroup {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    while std::time::Instant::now() < deadline {
        if let Ok(resp) = client
            .describe_replication_groups()
            .replication_group_id(replication_group_id)
            .send()
            .await
        {
            for g in resp.replication_groups() {
                if g.status() == Some("available") {
                    return g.clone();
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("replication group {replication_group_id} did not reach 'available' within {max_secs}s");
}

/// Poll DescribeServerlessCaches until the cache reaches "available".
pub async fn wait_for_serverless_cache_available(
    client: &aws_sdk_elasticache::Client,
    serverless_cache_name: &str,
    max_secs: u64,
) -> aws_sdk_elasticache::types::ServerlessCache {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    while std::time::Instant::now() < deadline {
        if let Ok(resp) = client
            .describe_serverless_caches()
            .serverless_cache_name(serverless_cache_name)
            .send()
            .await
        {
            for c in resp.serverless_caches() {
                if c.status() == Some("available") {
                    return c.clone();
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("serverless cache {serverless_cache_name} did not reach 'available' within {max_secs}s");
}

/// Print Docker diagnostics for an MQ broker's backing container to the TEST's
/// stdout (which nextest captures and shows on failure). fakecloud tags each
/// broker container `fakecloud-mq=<broker_id>`, so we can find it by label even
/// though the runtime's own `tracing` logs go to the server's stderr (which CI
/// does not surface).
///
/// This exists so a broker that fails to become ready in CI is DIAGNOSABLE: the
/// container's recent logs reveal the real reason (OOM kill, Erlang node/cookie
/// error, slow boot, crash loop) instead of a bare "CREATION_FAILED".
pub async fn dump_mq_broker_diagnostics(server: &TestServer, broker_id: &str) {
    // The broker's persisted `statusReason` carries the REAL cause a bring-up
    // failed (docker stderr + the failed container's logs), captured by the mq
    // runtime and stored on the CREATION_FAILED record. The typed AWS SDK drops
    // this non-modeled field, so read it straight off the raw restJson1
    // DescribeBroker body (`GET /v1/brokers/{id}`). Best-effort: a fetch failure
    // just prints a note rather than masking the docker diagnostics below.
    println!("\n===== MQ broker statusReason for {broker_id} =====");
    // The broker lives under the ACCOUNT the SDK's access key resolves to, so
    // this raw read MUST be authenticated with the same credential the test
    // client uses (`AKIAIOSFODNN7EXAMPLE`, per TestServer::aws_config). An
    // unauthenticated GET resolves to a different account and never finds the
    // broker -- which is exactly why this dump used to print `<none set>` even
    // though the server had recorded a real statusReason. fakecloud resolves the
    // account from the Credential's access key (the signature is not verified in
    // the default single-account mode), so a minimal SigV4 authorization header
    // is enough to land on the right account.
    match reqwest::Client::new()
        .get(format!("{}/v1/brokers/{broker_id}", server.endpoint()))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 \
             Credential=AKIAIOSFODNN7EXAMPLE/20250101/us-east-1/mq/aws4_request, \
             SignedHeaders=host, Signature=fakecloud",
        )
        .send()
        .await
    {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => println!(
                "statusReason: {}",
                body.get("statusReason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("<none set>")
            ),
            Err(e) => println!("<could not parse DescribeBroker body: {e}>"),
        },
        Err(e) => println!("<could not fetch DescribeBroker: {e}>"),
    }

    fn docker(args: &[&str]) -> String {
        match std::process::Command::new("docker").args(args).output() {
            Ok(o) => {
                let mut s = String::from_utf8_lossy(&o.stdout).into_owned();
                let err = String::from_utf8_lossy(&o.stderr);
                if !err.trim().is_empty() {
                    s.push_str("\n[stderr] ");
                    s.push_str(err.trim());
                }
                s
            }
            Err(e) => format!("<`docker {}` failed: {e}>", args.join(" ")),
        }
    }

    println!("\n===== MQ broker diagnostics for {broker_id} =====");
    let label = format!("label=fakecloud-mq={broker_id}");
    let ids_raw = docker(&["ps", "-aq", "--filter", &label]);
    let ids: Vec<&str> = ids_raw.split_whitespace().collect();
    println!(
        "container ids (filter {label}): {}",
        if ids.is_empty() {
            "<none found>".to_string()
        } else {
            ids.join(", ")
        }
    );
    // Broad `docker ps -a` too, in case the label filter misses (e.g. the
    // container was removed) -- it shows exit status / restart state at a glance.
    println!("--- docker ps -a ---\n{}", docker(&["ps", "-a"]));
    for id in ids {
        println!(
            "--- docker inspect {id} (state) ---\n{}",
            docker(&[
                "inspect",
                "--format",
                "status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} error={{.State.Error}}",
                id,
            ])
        );
        println!(
            "--- docker logs --tail 120 {id} ---\n{}",
            docker(&["logs", "--tail", "120", id])
        );
    }
    println!("===== end MQ broker diagnostics for {broker_id} =====\n");
}

/// The current RFC 6238 TOTP code (HMAC-SHA1, 30 s, 6 digits) for a virtual
/// MFA device, computed from the `Base32StringSeed` bytes
/// `CreateVirtualMFADevice` returned (the base32 text, as the SDK decodes the
/// blob). Independent of fakecloud's own implementation so the e2e suite
/// checks it like an authenticator app would.
pub fn totp_now(base32_seed: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut secret = Vec::new();
    let (mut buffer, mut bits) = (0u64, 0u32);
    for &c in base32_seed.iter().filter(|c| **c != b'=') {
        let v = ALPHABET
            .iter()
            .position(|a| *a == c.to_ascii_uppercase())
            .expect("seed must be base32") as u64;
        buffer = (buffer << 5) | v;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            secret.push((buffer >> bits) as u8);
        }
    }
    let step = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 30)
        .to_be_bytes();
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(&secret).unwrap();
    mac.update(&step);
    let d = mac.finalize().into_bytes();
    let o = (d[19] & 0x0f) as usize;
    let n = (u32::from(d[o] & 0x7f) << 24)
        | (u32::from(d[o + 1]) << 16)
        | (u32::from(d[o + 2]) << 8)
        | u32::from(d[o + 3]);
    format!("{:06}", n % 1_000_000)
}

/// SDK config signed with the reserved `test` root identity, which IAM
/// enforcement exempts. Under `--iam strict` the default test client's
/// example key resolves to no identity and is rejected, so strict tests use
/// this to act as the account root.
pub async fn root_config(server: &TestServer) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await
}

/// A path-style S3 client signed as the `test` root identity.
pub async fn root_s3_client(server: &TestServer) -> aws_sdk_s3::Client {
    let config = root_config(server).await;
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build(),
    )
}
