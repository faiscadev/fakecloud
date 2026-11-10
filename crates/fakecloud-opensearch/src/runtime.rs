//! Backing-container runtime for OpenSearch / Elasticsearch domains.
//!
//! A domain in fakecloud is backed by a REAL single-node search cluster so a
//! client genuinely indexes and searches against it -- the bar Amazon MQ,
//! MSK, RDS and ElastiCache meet -- rather than a formatted-but-dead
//! `search-*.es.amazonaws.com` name.
//!
//! - `OpenSearch_X.Y` runs `opensearchproject/opensearch:X.Y.z` (ECR Public
//!   mirror), the security plugin disabled so the cluster answers plain HTTP
//!   without credentials (see [`DomainSpec`] for why).
//! - `Elasticsearch_6.x` / `Elasticsearch_7.x` run Elastic's Apache-2.0
//!   `elasticsearch-oss` image, the distribution Amazon Elasticsearch Service
//!   was built from. Older engines (1.5, 2.3, 5.x) have no maintained image
//!   that boots as a single node here, so those domains stay control-plane
//!   only ([`engine_image`] returns `None`).
//!
//! Each cluster is single-node (`discovery.type=single-node`) with a 512 MiB
//! heap. Its data directory is a named volume scoped like the other data
//! planes' ([`fakecloud_core::data_volume`]): replacing the container for a
//! configuration change or an engine upgrade keeps every index, and a
//! persistent-mode restart brings the domain back on it. Every CLI call is time-bounded, so a
//! wedged daemon fails a bring-up instead of hanging it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::process::Command;

use fakecloud_core::container_net::{output_within, HostNetworking};

use crate::state::Domain;

/// Env var that disables the backend entirely (control-plane-only domains),
/// e.g. for the terraform-provider acceptance harness, which asserts response
/// formats and would otherwise boot a JVM per domain.
pub const DISABLE_ENV: &str = "FAKECLOUD_OPENSEARCH_DISABLE_BACKEND";
/// Overrides the repository OpenSearch images are pulled from.
pub const OPENSEARCH_REPO_ENV: &str = "FAKECLOUD_OPENSEARCH_IMAGE_REPO";
/// Overrides the repository Elasticsearch images are pulled from.
pub const ELASTICSEARCH_REPO_ENV: &str = "FAKECLOUD_ELASTICSEARCH_IMAGE_REPO";

const OPENSEARCH_REPO: &str = "public.ecr.aws/opensearchproject/opensearch";
const ELASTICSEARCH_REPO: &str = "docker.elastic.co/elasticsearch/elasticsearch-oss";
/// The cluster's HTTP port inside the container.
const HTTP_PORT: u16 = 9200;
/// Heap for the single node: small enough for a laptop or CI runner, enough
/// for test-sized indices.
const HEAP: &str = "-Xms512m -Xmx512m";

/// How long [`OpenSearchRuntime::stop_all`] waits for in-flight bring-ups to
/// remove their own containers.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(3);
/// Budget for a container-CLI call that only touches the local daemon.
const CLI_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a freshly started node gets to answer `_cluster/health`. A JVM
/// boot is ~10-30s natively; an amd64-only image under emulation is far
/// slower.
const READY_TIMEOUT: Duration = Duration::from_secs(300);

/// Which engine family a domain runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineFamily {
    OpenSearch,
    Elasticsearch,
}

impl EngineFamily {
    fn data_dir(self) -> &'static str {
        match self {
            Self::OpenSearch => "/usr/share/opensearch/data",
            Self::Elasticsearch => "/usr/share/elasticsearch/data",
        }
    }

    /// Container env common to every node of this family.
    fn base_env(self) -> Vec<String> {
        let mut env = vec!["discovery.type=single-node".to_string()];
        match self {
            Self::OpenSearch => {
                env.push(format!("OPENSEARCH_JAVA_OPTS={HEAP}"));
                env.push("DISABLE_SECURITY_PLUGIN=true".to_string());
                env.push("DISABLE_INSTALL_DEMO_CONFIG=true".to_string());
            }
            Self::Elasticsearch => env.push(format!("ES_JAVA_OPTS={HEAP}")),
        }
        env
    }
}

/// The image a domain's engine version runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineImage {
    pub family: EngineFamily,
    pub image: String,
}

/// The newest patch release of each engine minor version, as published.
fn patch_release(family: EngineFamily, minor: &str) -> Option<&'static str> {
    let known: &[(&str, &str)] = match family {
        EngineFamily::OpenSearch => &[
            ("1.0", "1.0.1"),
            ("1.1", "1.1.0"),
            ("1.2", "1.2.4"),
            ("1.3", "1.3.20"),
            ("2.3", "2.3.0"),
            ("2.5", "2.5.0"),
            ("2.7", "2.7.0"),
            ("2.9", "2.9.0"),
            ("2.11", "2.11.1"),
            ("2.13", "2.13.0"),
            ("2.15", "2.15.0"),
            ("2.17", "2.17.1"),
            ("2.19", "2.19.2"),
            ("3.1", "3.1.0"),
        ],
        EngineFamily::Elasticsearch => &[
            ("6.0", "6.0.1"),
            ("6.2", "6.2.4"),
            ("6.3", "6.3.2"),
            ("6.4", "6.4.3"),
            ("6.5", "6.5.4"),
            ("6.7", "6.7.2"),
            ("6.8", "6.8.23"),
            ("7.1", "7.1.1"),
            ("7.4", "7.4.2"),
            ("7.7", "7.7.1"),
            ("7.8", "7.8.1"),
            ("7.9", "7.9.3"),
            ("7.10", "7.10.2"),
        ],
    };
    known.iter().find(|(m, _)| *m == minor).map(|(_, p)| *p)
}

fn repo(var: &str, default: &str) -> String {
    std::env::var(var)
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// The image for a domain's engine version (`OpenSearch_2.11`,
/// `Elasticsearch_7.10`), or `None` when the version has no image (the domain
/// then stays control-plane only).
///
/// A known minor maps to its newest patch release. An OpenSearch minor not in
/// the table (a release newer than this build) runs `X.Y.0`, the tag every
/// OpenSearch release publishes. Elasticsearch before 6.0 is refused: Elastic
/// never published an OSS image for it, and the default 5.x image needs
/// X-Pack disabled and host kernel settings a single-node container can't
/// assume.
pub fn engine_image(engine_version: &str) -> Option<EngineImage> {
    let (prefix, version) = engine_version.split_once('_')?;
    let family = if prefix.eq_ignore_ascii_case("OpenSearch") {
        EngineFamily::OpenSearch
    } else if prefix.eq_ignore_ascii_case("Elasticsearch") {
        EngineFamily::Elasticsearch
    } else {
        return None;
    };
    let mut parts = version.split('.');
    let major: u32 = parts.next()?.parse().ok()?;
    let minor_n: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let minor = format!("{major}.{minor_n}");
    let tag = match (family, patch_release(family, &minor)) {
        (_, Some(patch)) => patch.to_string(),
        (EngineFamily::OpenSearch, None) if major >= 1 => format!("{minor}.0"),
        (EngineFamily::Elasticsearch, None) => return None,
        _ => return None,
    };
    let repository = match family {
        EngineFamily::OpenSearch => repo(OPENSEARCH_REPO_ENV, OPENSEARCH_REPO),
        EngineFamily::Elasticsearch => repo(ELASTICSEARCH_REPO_ENV, ELASTICSEARCH_REPO),
    };
    Some(EngineImage {
        family,
        image: format!("{repository}:{tag}"),
    })
}

/// `AdvancedOptions` keys that are node settings, and how a value maps onto
/// the setting. Other keys (`override_main_response_version`, which only
/// changes the version string AWS reports) don't configure the node.
fn advanced_setting(key: &str, value: &str) -> Option<(String, String)> {
    match key {
        "rest.action.multi.allow_explicit_index" => {
            let v = value.trim().to_ascii_lowercase();
            matches!(v.as_str(), "true" | "false").then(|| (key.to_string(), v))
        }
        "indices.query.bool.max_clause_count" => value
            .trim()
            .parse::<u32>()
            .ok()
            .map(|n| (key.to_string(), n.to_string())),
        // AWS takes a percentage of the heap; an empty value means unbounded,
        // the engine's default.
        "indices.fielddata.cache.size" => value
            .trim()
            .trim_end_matches('%')
            .parse::<u32>()
            .ok()
            .filter(|n| (1..=100).contains(n))
            .map(|n| (key.to_string(), format!("{n}%"))),
        _ => None,
    }
}

/// Everything a domain's container is started from. Two specs that differ
/// need a new container; a domain's data volume is part of the spec but
/// survives the replacement.
///
/// The security plugin is always disabled: the local endpoint answers plain
/// HTTP with no credentials, whatever `AccessPolicies` or
/// `AdvancedSecurityOptions` the domain stores (those are kept and echoed,
/// not enforced on the data plane).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainSpec {
    pub family: EngineFamily,
    pub image: String,
    /// Node settings from `AdvancedOptions`, sorted.
    pub settings: Vec<(String, String)>,
    pub volume: String,
}

impl DomainSpec {
    /// The spec for `domain`, or `None` when its engine version has no image.
    pub fn for_domain(domain: &Domain, scope_tag: &str) -> Option<Self> {
        let EngineImage { family, image } = engine_image(&domain.engine_version)?;
        let mut settings: Vec<(String, String)> = domain
            .config
            .get("AdvancedOptions")
            .and_then(|v| v.as_object())
            .map(|opts| {
                opts.iter()
                    .filter_map(|(k, v)| {
                        // AWS takes string values, but SDKs and templates
                        // also send booleans and numbers.
                        let value = match v {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            serde_json::Value::Number(n) => n.to_string(),
                            _ => return None,
                        };
                        advanced_setting(k, &value)
                    })
                    .collect()
            })
            .unwrap_or_default();
        settings.sort();
        Some(Self {
            family,
            image,
            settings,
            volume: domain.data_volume_name(scope_tag),
        })
    }

    fn container_env(&self) -> Vec<String> {
        let mut env = self.family.base_env();
        env.extend(self.settings.iter().map(|(k, v)| format!("{k}={v}")));
        env
    }
}

/// A running domain container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningDomain {
    pub container_id: String,
    /// Where fakecloud reaches the published HTTP port: `127.0.0.1`, or the
    /// sibling host alias when fakecloud itself runs in a container.
    pub host: String,
    pub port: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("search cluster container failed to start: {0}")]
    StartFailed(String),
}

#[derive(Debug, Clone)]
struct Tracked {
    running: RunningDomain,
    spec: DomainSpec,
}

/// Docker/Podman-backed search cluster runtime. Containers are keyed by the
/// domain ARN.
#[derive(Debug, Clone)]
pub struct OpenSearchRuntime {
    cli: String,
    net: HostNetworking,
    /// `fakecloud-<pid>`: the ownership label the startup reaper uses.
    instance_id: String,
    /// Random per-process id. Two containerized fakecloud processes can share
    /// a pid (both pid 1) and a Docker socket, so shutdown's reap of this
    /// process's containers matches this, never the pid label.
    owner_id: String,
    http: reqwest::Client,
    containers: Arc<Mutex<HashMap<String, Tracked>>>,
    /// The spec a bring-up of each domain is running right now.
    inflight: Arc<Mutex<HashMap<String, DomainSpec>>>,
    /// Set by [`Self::stop_all`]: no new container is started, and one being
    /// started is removed instead of handed out.
    closed: Arc<std::sync::atomic::AtomicBool>,
    /// One async lock per domain, so two bring-ups of one domain (a create
    /// racing a config change) never start two containers.
    locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

impl OpenSearchRuntime {
    /// The runtime, or `None` when no container CLI answers or the backend is
    /// disabled via [`DISABLE_ENV`]. Domains then stay control-plane only.
    pub fn new() -> Option<Self> {
        if std::env::var(DISABLE_ENV).is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true")) {
            return None;
        }
        let cli = fakecloud_core::container_net::detect_container_cli()?;
        let net = HostNetworking::detect(&cli);
        Self::with_cli(cli, net)
    }

    /// The runtime driving `cli` with `net`.
    fn with_cli(cli: String, net: HostNetworking) -> Option<Self> {
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .ok()?;
        Some(Self {
            cli,
            net,
            instance_id: format!("fakecloud-{}", std::process::id()),
            owner_id: uuid::Uuid::new_v4().simple().to_string(),
            http,
            containers: Arc::new(Mutex::new(HashMap::new())),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Record that a bring-up of `key` from `spec` is starting. Called by the
    /// converger while it holds the state lock it read `spec` under, so a
    /// configuration change made under that lock sees it.
    pub fn begin_inflight(&self, key: &str, spec: &DomainSpec) {
        self.inflight.lock().insert(key.to_string(), spec.clone());
    }

    /// The bring-up of `key` is done (settled, failed or abandoned).
    pub fn end_inflight(&self, key: &str) {
        self.inflight.lock().remove(key);
    }

    /// The spec an in-flight bring-up of `key` is starting from.
    pub fn inflight_spec(&self, key: &str) -> Option<DomainSpec> {
        self.inflight.lock().get(key).cloned()
    }

    pub fn cli_name(&self) -> &str {
        &self.cli
    }

    fn lock_for(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .entry(key.to_string())
            .or_default()
            .clone()
    }

    /// Bring `key`'s cluster up from `spec` and wait until it answers
    /// `_cluster/health`. A tracked container started from the same spec is
    /// reused unless it is definitively gone; one from another spec is
    /// replaced (the data volume stays).
    pub async fn ensure_domain(
        &self,
        key: &str,
        spec: &DomainSpec,
    ) -> Result<RunningDomain, RuntimeError> {
        let lock = self.lock_for(key);
        let _held = lock.lock().await;
        let tracked = self.containers.lock().get(key).cloned();
        if let Some(t) = tracked {
            if t.spec == *spec && !self.container_gone(&t.running.container_id).await {
                return Ok(t.running);
            }
            self.containers.lock().remove(key);
            self.remove_container(&t.running.container_id).await;
        }
        let running = self.spawn(key, spec).await?;
        self.track(key, &running, spec);
        Ok(running)
    }

    fn track(&self, key: &str, running: &RunningDomain, spec: &DomainSpec) {
        self.containers.lock().insert(
            key.to_string(),
            Tracked {
                running: running.clone(),
                spec: spec.clone(),
            },
        );
    }

    /// Stop tracking `key` and return its container id, so a caller holding
    /// the state lock can hand the removal to a background task.
    pub fn untrack(&self, key: &str) -> Option<String> {
        self.containers
            .lock()
            .remove(key)
            .map(|t| t.running.container_id)
    }

    /// Whether `key`'s tracked container is definitively gone: the daemon
    /// says it no longer exists, or that it has exited. Any other answer --
    /// including a CLI failure or a daemon outage -- is not proof, so `false`
    /// (also when nothing is tracked).
    pub async fn tracked_container_gone(&self, key: &str) -> bool {
        let Some(cid) = self
            .containers
            .lock()
            .get(key)
            .map(|t| t.running.container_id.clone())
        else {
            return false;
        };
        self.container_gone(&cid).await
    }

    async fn container_gone(&self, container_id: &str) -> bool {
        let Ok(out) = self
            .cli_call(&["inspect", "--format", "{{.State.Status}}", container_id])
            .await
        else {
            return false;
        };
        container_gone_verdict(
            out.status.success(),
            &String::from_utf8_lossy(&out.stdout),
            &String::from_utf8_lossy(&out.stderr),
        )
    }

    /// The spec `key`'s tracked container was started from.
    pub fn tracked_spec(&self, key: &str) -> Option<DomainSpec> {
        self.containers.lock().get(key).map(|t| t.spec.clone())
    }

    /// The domain ARNs whose containers this runtime tracks.
    pub fn tracked_domains(&self) -> Vec<String> {
        self.containers.lock().keys().cloned().collect()
    }

    /// Remove `key`'s container (if tracked) and the data volume `volume`.
    pub async fn destroy_domain(&self, key: &str, volume: Option<&str>) {
        let lock = self.lock_for(key);
        let _held = lock.lock().await;
        let tracked = self.containers.lock().remove(key);
        if let Some(t) = &tracked {
            self.remove_container(&t.running.container_id).await;
        }
        // Also the volume the tracked spec named, in case the caller's row
        // was already gone. Any container still mounting a volume (one this
        // process doesn't track, or a previous process's) is removed first,
        // or the volume removal would fail.
        let volumes = volume
            .map(str::to_string)
            .into_iter()
            .chain(tracked.map(|t| t.spec.volume));
        let mut seen = std::collections::BTreeSet::new();
        for v in volumes {
            if seen.insert(v.clone()) {
                self.reap_volume_users(key, &v).await;
                fakecloud_core::data_volume::remove_volume(&self.cli, &v).await;
            }
        }
    }

    /// Remove what a bring-up from `spec` left for a domain incarnation that
    /// no longer exists: the tracked container only if it still runs that
    /// spec (a recreated domain under the same ARN may already run its own),
    /// and the spec's data volume, which only that incarnation used.
    pub async fn retire(&self, key: &str, spec: &DomainSpec) {
        let lock = self.lock_for(key);
        let _held = lock.lock().await;
        let stale = {
            let mut map = self.containers.lock();
            match map.get(key) {
                Some(t) if t.spec == *spec => map.remove(key),
                _ => None,
            }
        };
        if let Some(t) = stale {
            self.remove_container(&t.running.container_id).await;
        }
        // Leftovers still mounting the volume hold it too. The volume names
        // this incarnation only, so a recreated domain's container (on its
        // own volume) is never matched.
        self.reap_volume_users(key, &spec.volume).await;
        fakecloud_core::data_volume::remove_volume(&self.cli, &spec.volume).await;
    }

    /// Tear down a removed domain: `container_id` (already untracked by the
    /// caller), any other container still mounting its data volume, then the
    /// volume. Serialized with bring-ups of the same ARN.
    pub async fn teardown(&self, key: &str, container_id: Option<&str>, volume: Option<&str>) {
        let lock = self.lock_for(key);
        let _held = lock.lock().await;
        if let Some(cid) = container_id {
            self.remove_container(cid).await;
        }
        if let Some(v) = volume {
            self.reap_volume_users(key, v).await;
            fakecloud_core::data_volume::remove_volume(&self.cli, v).await;
        }
    }

    /// Remove one container by id (a removal handed off by [`Self::untrack`]).
    pub async fn remove_container(&self, container_id: &str) {
        let _ = self.cli_call(&["rm", "-f", container_id]).await;
    }

    /// Remove a data volume by name.
    pub async fn remove_volume(&self, volume: &str) {
        fakecloud_core::data_volume::remove_volume(&self.cli, volume).await;
    }

    /// Remove every tracked container (graceful shutdown). Data volumes stay:
    /// persistent mode respawns on them, and memory mode's are removed with
    /// the process's other volumes.
    ///
    /// A bring-up still in flight is told to stop and given a few seconds to
    /// remove its own container; whatever this process started that is still
    /// around (tracked or not) is then removed by its owner label. A `create`
    /// the CLI is still running when the process exits can outlive it; the
    /// startup reaper removes that one.
    pub async fn stop_all(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        let all: Vec<Tracked> = self.containers.lock().drain().map(|(_, t)| t).collect();
        for t in all {
            self.remove_container(&t.running.container_id).await;
        }
        let deadline = Instant::now() + SHUTDOWN_GRACE;
        while !self.inflight.lock().is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.reap_owned().await;
    }

    fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn cli_call(&self, args: &[&str]) -> Result<std::process::Output, String> {
        let mut cmd = Command::new(&self.cli);
        cmd.args(args);
        output_within(&mut cmd, CLI_TIMEOUT)
            .await
            .map_err(|e| format!("{} {}: {e}", self.cli, args.first().unwrap_or(&"")))
    }

    async fn spawn(&self, key: &str, spec: &DomainSpec) -> Result<RunningDomain, RuntimeError> {
        if self.is_closed() {
            return Err(RuntimeError::StartFailed(
                "fakecloud is shutting down".into(),
            ));
        }
        // Whatever still mounts the volume -- a failed bring-up's leftover, or
        // the node of a previous fakecloud process the startup reaper skipped
        // (a containerized restart has pid 1 again) -- holds the node lock on
        // its data and would keep the new node from starting.
        self.reap_volume_users(key, &spec.volume).await;
        if let Err(e) =
            fakecloud_core::container_image::ensure_image(&self.cli, None, &spec.image).await
        {
            return Err(RuntimeError::StartFailed(format!(
                "could not pull {}: {e}",
                spec.image
            )));
        }
        // A node respawned onto existing data may come up with red indices
        // and still serve; a fresh one must reach yellow or green.
        let existing_data =
            fakecloud_core::data_volume::volume_exists(&self.cli, &spec.volume).await;
        fakecloud_core::data_volume::ensure_volume(
            &self.cli,
            &spec.volume,
            fakecloud_core::data_volume::current_scope(),
            &[format!("fakecloud-opensearch={key}")],
        )
        .await;
        let mut args: Vec<String> = vec![
            "create".into(),
            "-p".into(),
            format!(":{HTTP_PORT}"),
            "--label".into(),
            format!("fakecloud-opensearch={key}"),
            "--label".into(),
            format!("fakecloud-opensearch-owner={}", self.owner_id),
            "--label".into(),
            format!("fakecloud-instance={}", self.instance_id),
            "-v".into(),
            format!("{}:{}", spec.volume, spec.family.data_dir()),
        ];
        self.net.push_add_host_args(&mut args);
        for e in spec.container_env() {
            args.push("-e".into());
            args.push(e);
        }
        args.push(spec.image.clone());
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        let created = self
            .cli_call(&argv)
            .await
            .map_err(RuntimeError::StartFailed)?;
        if !created.status.success() {
            return Err(RuntimeError::StartFailed(
                String::from_utf8_lossy(&created.stderr).trim().to_string(),
            ));
        }
        let container_id = String::from_utf8_lossy(&created.stdout).trim().to_string();
        if self.is_closed() {
            self.remove_container(&container_id).await;
            return Err(RuntimeError::StartFailed(
                "fakecloud is shutting down".into(),
            ));
        }
        match self.start_created(&container_id, existing_data).await {
            Ok(running) => {
                tracing::info!(domain = %key, container_id = %container_id, image = %spec.image, "search cluster container started");
                Ok(running)
            }
            Err(why) => {
                // Keep the reason and the node's last logs in the error, then
                // remove the container: an untracked one would keep the data
                // volume mounted and outlive every teardown.
                let diag = self.diagnostics(&container_id).await;
                self.remove_container(&container_id).await;
                Err(RuntimeError::StartFailed(format!("{why}; {diag}")))
            }
        }
    }

    /// Start a created container and wait until its node is ready
    /// (`accept_red`: see [`Self::wait_ready`]).
    async fn start_created(
        &self,
        container_id: &str,
        accept_red: bool,
    ) -> Result<RunningDomain, String> {
        let started = self.cli_call(&["start", container_id]).await?;
        if !started.status.success() {
            return Err(format!(
                "container start failed: {}",
                String::from_utf8_lossy(&started.stderr).trim()
            ));
        }
        let port = self.lookup_port(container_id).await?;
        let running = RunningDomain {
            container_id: container_id.to_string(),
            host: self.net.sibling_host.clone(),
            port,
        };
        self.wait_ready(&running, accept_red).await?;
        Ok(running)
    }

    async fn lookup_port(&self, container_id: &str) -> Result<u16, String> {
        let out = self
            .cli_call(&["port", container_id, &HTTP_PORT.to_string()])
            .await?;
        if !out.status.success() {
            return Err(format!(
                "port lookup failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        let text = String::from_utf8_lossy(&out.stdout);
        fakecloud_core::container_net::parse_published_port(&text)
            .ok_or_else(|| format!("no published port for {HTTP_PORT} in '{}'", text.trim()))
    }

    /// The `status` of a well-formed `_cluster/health` reply, `None` when the
    /// node doesn't answer one.
    async fn health_status(&self, running: &RunningDomain) -> Option<String> {
        let url = format!("http://{}:{}/_cluster/health", running.host, running.port);
        let resp = self.http.get(&url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<serde_json::Value>()
            .await
            .ok()?
            .get("status")?
            .as_str()
            .map(str::to_string)
    }

    /// Wait until the node answers health as ready: `green`/`yellow`, or with
    /// `accept_red` also `red` -- a node on existing data whose indices are
    /// red still serves, and rejecting it would leave the domain control-plane
    /// only for good.
    async fn wait_ready(&self, running: &RunningDomain, accept_red: bool) -> Result<(), String> {
        let start = Instant::now();
        while start.elapsed() < READY_TIMEOUT {
            if self.is_closed() {
                return Err("fakecloud is shutting down".to_string());
            }
            if self
                .health_status(running)
                .await
                .is_some_and(|s| health_is_ready(&s, accept_red))
            {
                return Ok(());
            }
            if self.exited(&running.container_id).await {
                return Err("the search node exited during boot".to_string());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(format!(
            "the search node did not answer _cluster/health within {}s",
            READY_TIMEOUT.as_secs()
        ))
    }

    async fn exited(&self, container_id: &str) -> bool {
        self.cli_call(&["inspect", "--format", "{{.State.Status}}", container_id])
            .await
            .is_ok_and(|o| {
                let s = String::from_utf8_lossy(&o.stdout);
                matches!(s.trim(), "exited" | "dead")
            })
    }

    /// Remove every container of domain `key` that mounts `volume`, whichever
    /// process started it. The volume is scoped to this data dir and domain
    /// incarnation, so only this domain's nodes ever mount it.
    async fn reap_volume_users(&self, key: &str, volume: &str) {
        self.reap_matching(&[
            format!("label=fakecloud-opensearch={key}"),
            format!("volume={volume}"),
        ])
        .await;
    }

    /// Remove every container this process started.
    async fn reap_owned(&self) {
        self.reap_matching(&[format!(
            "label=fakecloud-opensearch-owner={}",
            self.owner_id
        )])
        .await;
    }

    /// Remove every container matching all of `filters` (`ps --filter`).
    async fn reap_matching(&self, filters: &[String]) {
        let mut args: Vec<&str> = vec!["ps", "-aq"];
        for f in filters {
            args.extend(["--filter", f.as_str()]);
        }
        let Ok(out) = self.cli_call(&args).await else {
            return;
        };
        if !out.status.success() {
            return;
        }
        for id in String::from_utf8_lossy(&out.stdout).split_whitespace() {
            self.remove_container(id).await;
        }
    }

    async fn diagnostics(&self, container_id: &str) -> String {
        let state = self
            .cli_call(&[
                "inspect",
                "--format",
                "status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}",
                container_id,
            ])
            .await
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        let logs = self
            .cli_call(&["logs", "--tail", "40", container_id])
            .await
            .map(|o| {
                let mut s = String::from_utf8_lossy(&o.stdout).into_owned();
                s.push_str(&String::from_utf8_lossy(&o.stderr));
                s.trim().to_string()
            })
            .unwrap_or_else(|e| format!("<logs failed: {e}>"));
        format!("container state [{state}]; recent logs:\n{logs}")
    }
}

/// Whether an `inspect --format {{.State.Status}}` answer proves the
/// container is gone: the daemon says no such container, or that it exited.
fn container_gone_verdict(success: bool, stdout: &str, stderr: &str) -> bool {
    if success {
        return matches!(stdout.trim(), "exited" | "dead");
    }
    let err = stderr.to_ascii_lowercase();
    err.contains("no such container") || err.contains("no such object")
}

/// Whether a `_cluster/health` status counts as ready.
pub(crate) fn health_is_ready(status: &str, accept_red: bool) -> bool {
    matches!(status, "green" | "yellow") || (accept_red && status == "red")
}

/// A fake container engine for tests that drive the runtime without Docker:
/// a shell-script CLI that logs every call and a tiny HTTP server standing
/// in for every node's `_cluster/health`.
#[doc(hidden)]
pub mod fake {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::OpenSearchRuntime;
    use fakecloud_core::container_net::HostNetworking;

    pub struct FakeEngine {
        pub dir: PathBuf,
        pub healthy: Arc<AtomicBool>,
        /// While set (and healthy), nodes answer health with status `red`.
        pub red: Arc<AtomicBool>,
        pub runtime: Arc<OpenSearchRuntime>,
    }

    impl FakeEngine {
        /// Start the health server and write the CLI. Every node answers
        /// health while [`Self::healthy`] is set.
        pub async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let healthy = Arc::new(AtomicBool::new(true));
            let red = Arc::new(AtomicBool::new(false));
            let flag = healthy.clone();
            let red_flag = red.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                while let Ok((mut sock, _)) = listener.accept().await {
                    let flag = flag.clone();
                    let red_flag = red_flag.clone();
                    tokio::spawn(async move {
                        let mut buf = [0u8; 2048];
                        let _ = sock.read(&mut buf).await;
                        let (code, body) = if flag.load(Ordering::SeqCst) {
                            if red_flag.load(Ordering::SeqCst) {
                                ("200 OK", r#"{"status":"red"}"#)
                            } else {
                                ("200 OK", r#"{"status":"green"}"#)
                            }
                        } else {
                            ("503 Service Unavailable", r#"{"status":"red"}"#)
                        };
                        let resp = format!(
                            "HTTP/1.1 {code}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                    });
                }
            });
            let dir = std::env::temp_dir().join(format!(
                "fakecloud-opensearch-fake-{}",
                uuid::Uuid::new_v4().simple()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let d = dir.display();
            // `create` mints a fresh id per call; `start` fails while
            // `fail-start` exists; `inspect` reports `exited` while `dead`
            // exists, "No such container" while `gone` exists, and fails like
            // a daemon outage while `daemon-down` exists; `volume inspect`
            // fails while `no-volume` exists; `ps` prints `ps-out`.
            let script = format!(
                r#"#!/bin/sh
echo "$*" >> "{d}/log"
case "$1" in
  create) n=$(($(cat "{d}/n" 2>/dev/null || echo 0) + 1)); echo "$n" > "{d}/n"; echo "cid$n" ;;
  start) if [ -f "{d}/fail-start" ]; then echo "boom" >&2; exit 1; fi ;;
  port) echo "0.0.0.0:{port}" ;;
  inspect)
    if [ -f "{d}/daemon-down" ]; then echo "Cannot connect to the Docker daemon" >&2; exit 1; fi
    if [ -f "{d}/gone" ]; then echo "Error: No such container: $4" >&2; exit 1; fi
    if [ -f "{d}/dead" ]; then echo exited; else echo running; fi ;;
  volume) if [ "$2" = inspect ] && [ -f "{d}/no-volume" ]; then exit 1; fi ;;
  ps) cat "{d}/ps-out" 2>/dev/null ;;
  logs) echo "fake node log" ;;
esac
exit 0
"#
            );
            let cli = dir.join("cli");
            std::fs::write(&cli, script).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&cli, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            let net = HostNetworking {
                host_alias: "host.docker.internal".into(),
                add_host_arg: None,
                sibling_host: "127.0.0.1".into(),
            };
            let runtime =
                Arc::new(OpenSearchRuntime::with_cli(cli.display().to_string(), net).unwrap());
            Self {
                dir,
                healthy,
                red,
                runtime,
            }
        }

        /// Every CLI call so far, one line each.
        pub fn calls(&self) -> Vec<String> {
            std::fs::read_to_string(self.dir.join("log"))
                .unwrap_or_default()
                .lines()
                .map(str::to_string)
                .collect()
        }

        /// Create or remove a flag file (`fail-start`, `dead`, `gone`,
        /// `daemon-down`, `no-volume`).
        pub fn flag(&self, name: &str, on: bool) {
            let path = self.dir.join(name);
            if on {
                std::fs::write(path, "").unwrap();
            } else {
                let _ = std::fs::remove_file(path);
            }
        }

        /// What `ps` lists.
        pub fn set_ps(&self, ids: &str) {
            std::fs::write(self.dir.join("ps-out"), ids).unwrap();
        }

        /// The owner label value this runtime's containers carry.
        pub fn owner_id(&self) -> String {
            self.runtime.owner_id.clone()
        }
    }

    impl Drop for FakeEngine {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opensearch_versions_map_to_their_newest_patch_image() {
        let img = engine_image("OpenSearch_2.11").unwrap();
        assert_eq!(img.family, EngineFamily::OpenSearch);
        assert_eq!(
            img.image,
            "public.ecr.aws/opensearchproject/opensearch:2.11.1"
        );
        assert_eq!(
            engine_image("OpenSearch_1.3").unwrap().image,
            "public.ecr.aws/opensearchproject/opensearch:1.3.20"
        );
        // A release newer than the table runs its `.0` tag.
        assert_eq!(
            engine_image("OpenSearch_3.3").unwrap().image,
            "public.ecr.aws/opensearchproject/opensearch:3.3.0"
        );
    }

    #[test]
    fn elasticsearch_versions_map_to_the_oss_image_and_old_ones_have_none() {
        let img = engine_image("Elasticsearch_7.10").unwrap();
        assert_eq!(img.family, EngineFamily::Elasticsearch);
        assert_eq!(
            img.image,
            "docker.elastic.co/elasticsearch/elasticsearch-oss:7.10.2"
        );
        assert_eq!(
            engine_image("Elasticsearch_6.8").unwrap().image,
            "docker.elastic.co/elasticsearch/elasticsearch-oss:6.8.23"
        );
        for v in [
            "Elasticsearch_5.6",
            "Elasticsearch_2.3",
            "Elasticsearch_1.5",
            "Elasticsearch_7.11",
            "OpenSearch_2",
            "OpenSearch_x.y",
            "Solr_9.1",
            "2.11",
        ] {
            assert_eq!(engine_image(v), None, "{v}");
        }
    }

    #[test]
    fn advanced_options_become_node_settings() {
        assert_eq!(
            advanced_setting("rest.action.multi.allow_explicit_index", "TRUE"),
            Some((
                "rest.action.multi.allow_explicit_index".into(),
                "true".into()
            ))
        );
        assert_eq!(
            advanced_setting("indices.fielddata.cache.size", "20"),
            Some(("indices.fielddata.cache.size".into(), "20%".into()))
        );
        assert_eq!(advanced_setting("indices.fielddata.cache.size", ""), None);
        assert_eq!(
            advanced_setting("indices.query.bool.max_clause_count", "2048"),
            Some(("indices.query.bool.max_clause_count".into(), "2048".into()))
        );
        assert_eq!(
            advanced_setting("override_main_response_version", "true"),
            None
        );
        assert_eq!(
            advanced_setting("rest.action.multi.allow_explicit_index", "maybe"),
            None
        );
    }

    fn domain(engine: &str, advanced: serde_json::Value) -> Domain {
        serde_json::from_value(serde_json::json!({
            "name": "logs",
            "domain_id": "123456789012/logs",
            "arn": "arn:aws:es:us-east-1:123456789012:domain/logs",
            "engine_version": engine,
            "created_via_es": false,
            "endpoint": "search-logs-x.us-east-1.es.amazonaws.com",
            "created": true,
            "deleted": false,
            "config": {"AdvancedOptions": advanced},
            "tags": {},
            "created_at": "2026-01-01T00:00:00Z",
        }))
        .unwrap()
    }

    #[test]
    fn a_spec_changes_with_its_image_and_settings_but_not_with_other_config() {
        let base = domain("OpenSearch_2.11", serde_json::json!({}));
        let spec = DomainSpec::for_domain(&base, "d0").unwrap();
        assert_eq!(spec.family, EngineFamily::OpenSearch);
        assert!(spec.volume.starts_with("fakecloud-opensearch-data-d0-"));
        let mut other = base.clone();
        other.config.insert(
            "ClusterConfig".into(),
            serde_json::json!({"InstanceCount": 3}),
        );
        assert_eq!(DomainSpec::for_domain(&other, "d0").unwrap(), spec);

        let tuned = domain(
            "OpenSearch_2.11",
            serde_json::json!({"indices.fielddata.cache.size": "30"}),
        );
        let tuned_spec = DomainSpec::for_domain(&tuned, "d0").unwrap();
        assert_ne!(tuned_spec, spec);
        // Booleans and numbers count like their string forms.
        let typed = domain(
            "OpenSearch_2.11",
            serde_json::json!({
                "indices.fielddata.cache.size": 30,
                "rest.action.multi.allow_explicit_index": false,
            }),
        );
        let typed_spec = DomainSpec::for_domain(&typed, "d0").unwrap();
        assert_eq!(
            typed_spec.settings,
            vec![
                (
                    "indices.fielddata.cache.size".to_string(),
                    "30%".to_string()
                ),
                (
                    "rest.action.multi.allow_explicit_index".to_string(),
                    "false".to_string()
                ),
            ]
        );
        assert!(tuned_spec
            .container_env()
            .contains(&"indices.fielddata.cache.size=30%".to_string()));

        let upgraded = domain("OpenSearch_2.13", serde_json::json!({}));
        let up_spec = DomainSpec::for_domain(&upgraded, "d0").unwrap();
        assert_ne!(up_spec.image, spec.image);
        // An upgrade keeps the data volume.
        assert_eq!(up_spec.volume, spec.volume);

        assert_eq!(
            DomainSpec::for_domain(&domain("Elasticsearch_5.6", serde_json::json!({})), "d0"),
            None
        );
    }

    #[test]
    fn opensearch_nodes_run_without_the_security_plugin() {
        let env = EngineFamily::OpenSearch.base_env();
        assert!(env.contains(&"DISABLE_SECURITY_PLUGIN=true".to_string()));
        assert!(env.contains(&"discovery.type=single-node".to_string()));
        let es = EngineFamily::Elasticsearch.base_env();
        assert!(es.contains(&"ES_JAVA_OPTS=-Xms512m -Xmx512m".to_string()));
    }
}
