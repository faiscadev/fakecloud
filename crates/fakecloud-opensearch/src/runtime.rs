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
//! persistent-mode restart reattaches it. Every CLI call is time-bounded, so a
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
                    .filter_map(|(k, v)| advanced_setting(k, v.as_str()?))
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

    /// A short digest of the spec, carried as a container label so a
    /// reattached container is reused only if it was started from this spec.
    pub fn digest(&self) -> String {
        let mut parts: Vec<String> = vec![
            format!("{:?}", self.family),
            self.image.clone(),
            self.volume.clone(),
        ];
        parts.extend(self.settings.iter().map(|(k, v)| format!("{k}={v}")));
        let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
        fakecloud_core::data_volume::incarnation_id(&refs)
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
    instance_id: String,
    http: reqwest::Client,
    containers: Arc<Mutex<HashMap<String, Tracked>>>,
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
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .ok()?;
        Some(Self {
            cli,
            net,
            instance_id: format!("fakecloud-{}", std::process::id()),
            http,
            containers: Arc::new(Mutex::new(HashMap::new())),
            locks: Arc::new(Mutex::new(HashMap::new())),
        })
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
    /// reused; one from another spec is replaced (the data volume stays).
    /// `adopt` names a container a previous fakecloud process ran for the
    /// domain: it is restarted and reused when it still exists and carries
    /// this spec's digest.
    pub async fn ensure_domain(
        &self,
        key: &str,
        spec: &DomainSpec,
        adopt: Option<&str>,
    ) -> Result<RunningDomain, RuntimeError> {
        let lock = self.lock_for(key);
        let _held = lock.lock().await;
        let tracked = self.containers.lock().get(key).cloned();
        if let Some(t) = tracked {
            if t.spec == *spec && self.healthy(&t.running).await {
                return Ok(t.running);
            }
            self.containers.lock().remove(key);
            self.remove_container(&t.running.container_id).await;
        }
        if let Some(cid) = adopt {
            if let Some(running) = self.adopt(cid, spec).await {
                self.track(key, &running, spec);
                tracing::info!(domain = %key, container_id = %cid, "re-attached search cluster container");
                return Ok(running);
            }
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
        // was already gone.
        let volumes = volume
            .map(str::to_string)
            .into_iter()
            .chain(tracked.map(|t| t.spec.volume));
        let mut seen = std::collections::BTreeSet::new();
        for v in volumes {
            if seen.insert(v.clone()) {
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
        fakecloud_core::data_volume::remove_volume(&self.cli, &spec.volume).await;
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
    /// persistent mode reattaches them, and memory mode's are removed with
    /// the process's other volumes.
    pub async fn stop_all(&self) {
        let all: Vec<Tracked> = self.containers.lock().drain().map(|(_, t)| t).collect();
        for t in all {
            self.remove_container(&t.running.container_id).await;
        }
    }

    async fn cli_call(&self, args: &[&str]) -> Result<std::process::Output, String> {
        let mut cmd = Command::new(&self.cli);
        cmd.args(args);
        output_within(&mut cmd, CLI_TIMEOUT)
            .await
            .map_err(|e| format!("{} {}: {e}", self.cli, args.first().unwrap_or(&"")))
    }

    async fn adopt(&self, container_id: &str, spec: &DomainSpec) -> Option<RunningDomain> {
        let out = self
            .cli_call(&[
                "inspect",
                "--format",
                "{{index .Config.Labels \"fakecloud-opensearch-spec\"}}",
                container_id,
            ])
            .await
            .ok()?;
        if !out.status.success() {
            return None;
        }
        if String::from_utf8_lossy(&out.stdout).trim() != spec.digest() {
            // Started from an older spec (a config change the previous
            // process never applied): replace it.
            self.remove_container(container_id).await;
            return None;
        }
        let started = self.cli_call(&["start", container_id]).await.ok()?;
        if !started.status.success() {
            self.remove_container(container_id).await;
            return None;
        }
        let port = self.lookup_port(container_id).await.ok()?;
        let running = RunningDomain {
            container_id: container_id.to_string(),
            host: self.net.sibling_host.clone(),
            port,
        };
        match self.wait_ready(&running).await {
            Ok(()) => Some(running),
            Err(_) => {
                self.remove_container(container_id).await;
                None
            }
        }
    }

    async fn spawn(&self, key: &str, spec: &DomainSpec) -> Result<RunningDomain, RuntimeError> {
        self.reap_stale(key).await;
        if let Err(e) =
            fakecloud_core::container_image::ensure_image(&self.cli, None, &spec.image).await
        {
            return Err(RuntimeError::StartFailed(format!(
                "could not pull {}: {e}",
                spec.image
            )));
        }
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
            format!("fakecloud-opensearch-spec={}", spec.digest()),
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
        let started = self
            .cli_call(&["start", &container_id])
            .await
            .map_err(RuntimeError::StartFailed)?;
        if !started.status.success() {
            let diag = self.diagnostics(&container_id).await;
            return Err(RuntimeError::StartFailed(format!(
                "container start failed: {}; {diag}",
                String::from_utf8_lossy(&started.stderr).trim()
            )));
        }
        let port = match self.lookup_port(&container_id).await {
            Ok(p) => p,
            Err(e) => {
                let diag = self.diagnostics(&container_id).await;
                return Err(RuntimeError::StartFailed(format!("{e}; {diag}")));
            }
        };
        let running = RunningDomain {
            container_id: container_id.clone(),
            host: self.net.sibling_host.clone(),
            port,
        };
        if let Err(why) = self.wait_ready(&running).await {
            // Left in place for post-mortem; the next bring-up's stale reap and
            // the shutdown sweep remove it.
            let diag = self.diagnostics(&container_id).await;
            return Err(RuntimeError::StartFailed(format!("{why}; {diag}")));
        }
        tracing::info!(domain = %key, container_id = %container_id, image = %spec.image, "search cluster container started");
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

    /// Whether the node answers `_cluster/health` with a non-red status.
    async fn healthy(&self, running: &RunningDomain) -> bool {
        let url = format!("http://{}:{}/_cluster/health", running.host, running.port);
        let Ok(resp) = self.http.get(&url).send().await else {
            return false;
        };
        if !resp.status().is_success() {
            return false;
        }
        resp.json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|v| v.get("status").and_then(|s| s.as_str()).map(str::to_string))
            .is_some_and(|s| s == "green" || s == "yellow")
    }

    async fn wait_ready(&self, running: &RunningDomain) -> Result<(), String> {
        let start = Instant::now();
        while start.elapsed() < READY_TIMEOUT {
            if self.healthy(running).await {
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

    /// Remove containers a previous failed bring-up of this domain left
    /// behind (in this fakecloud process only).
    async fn reap_stale(&self, key: &str) {
        let Ok(out) = self
            .cli_call(&[
                "ps",
                "-aq",
                "--filter",
                &format!("label=fakecloud-opensearch={key}"),
                "--filter",
                &format!("label=fakecloud-instance={}", self.instance_id),
            ])
            .await
        else {
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
        assert_ne!(tuned_spec.digest(), spec.digest());
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
