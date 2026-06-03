//! Kubernetes backend for ElastiCache backing instances.
//!
//! Runs each cache as a native Pod (one `redis`/`memcached` container)
//! instead of a Docker container. Shared client/lifecycle/exec/reaping
//! plumbing comes from the `fakecloud-k8s` crate; this module only builds
//! the cache Pod spec and maps the ElastiCache runtime operations onto
//! Pod operations.
//!
//! Snapshot restore: a restored Redis Pod's container `wget`s the
//! snapshot RDB from a per-process, bearer-token-guarded fakecloud
//! endpoint into `/data/dump.rdb` before `exec`ing `redis-server`, so the
//! engine loads it at startup — the k8s analogue of the Docker path's
//! `docker cp` into the created container.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, LocalObjectReference, Pod, PodSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use parking_lot::RwLock;

use fakecloud_k8s::{labels, names, K8sClient, K8sEnv};

use super::{BackendInitError, CacheEngineKind, CacheExec, RunningCacheContainer, RuntimeError};

/// Which `fakecloud-service` label cache Pods carry.
const SERVICE: &str = "elasticache";
/// Pod-name prefix for cache Pods (kept short so the resource slug + hash
/// suffix fit the 63-char DNS label limit).
const POD_PREFIX: &str = "fakecloud-ec";
/// Container name inside each cache Pod.
const CONTAINER: &str = "cache";

/// Snapshot RDB payloads staged for in-flight restores, keyed by Pod
/// name. The server's internal endpoint serves bytes from here; the
/// restored Pod's container fetches them at startup. Shared (cloneable
/// `Arc`) so the server route and the runtime see the same map.
pub type PendingRdb = Arc<RwLock<HashMap<String, Vec<u8>>>>;

#[derive(Clone)]
pub(super) struct K8sCache {
    client: K8sClient,
    /// In-cluster fakecloud base URL, e.g.
    /// `http://fakecloud.fakecloud.svc.cluster.local:4566`.
    self_url: String,
    /// Bearer token the restore fetch presents to the RDB endpoint.
    internal_token: String,
    /// Optional `imagePullSecrets` name for private registries.
    pull_secret: Option<String>,
    pending_rdb: PendingRdb,
}

impl std::fmt::Debug for K8sCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sCache")
            .field("namespace", &self.client.namespace())
            .field("self_url", &self.self_url)
            .finish_non_exhaustive()
    }
}

impl K8sCache {
    pub(super) async fn from_env(
        server_port: u16,
        internal_token: String,
    ) -> Result<Self, BackendInitError> {
        let env = K8sEnv::from_env(server_port)?;
        let client = K8sClient::connect(env.namespace.clone())
            .await
            .map_err(|e| BackendInitError::Connect(e.to_string()))?;
        tracing::info!(
            namespace = %env.namespace,
            self_url = %env.self_url,
            "K8s ElastiCache backend initialized"
        );
        Ok(Self {
            client,
            self_url: env.self_url,
            internal_token,
            pull_secret: env.pull_secret,
            pending_rdb: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub(super) fn pending_rdb(&self) -> PendingRdb {
        self.pending_rdb.clone()
    }

    /// Spawn a cache Pod, reading the snapshot RDB from `rdb_path` (if
    /// any) into memory first.
    pub(super) async fn spawn_pod(
        &self,
        resource_id: &str,
        engine: CacheEngineKind,
        rdb_path: Option<&str>,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        let rdb = match rdb_path {
            Some(path) => Some(tokio::fs::read(path).await.map_err(|e| {
                RuntimeError::ContainerStartFailed(format!("reading snapshot rdb {path}: {e}"))
            })?),
            None => None,
        };
        self.spawn_pod_bytes(resource_id, engine, rdb).await
    }

    async fn spawn_pod_bytes(
        &self,
        resource_id: &str,
        engine: CacheEngineKind,
        rdb: Option<Vec<u8>>,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        let pod_name = names::pod_name(POD_PREFIX, resource_id, resource_id);
        let port = engine.port();

        // Stage snapshot bytes (Redis only) for the Pod to fetch, then
        // build the spec referencing the RDB endpoint.
        let rdb_url = if matches!(engine, CacheEngineKind::Redis) {
            if let Some(bytes) = rdb {
                self.pending_rdb.write().insert(pod_name.clone(), bytes);
                Some(format!(
                    "{}/_fakecloud/elasticache/_internal/rdb/{}",
                    self.self_url.trim_end_matches('/'),
                    pod_name
                ))
            } else {
                None
            }
        } else {
            None
        };

        let pod = build_cache_pod(CachePodContext {
            pod_name: &pod_name,
            namespace: self.client.namespace(),
            instance_id: self.client.instance_id(),
            resource_id,
            image: engine.image(),
            port,
            rdb_url: rdb_url.as_deref(),
            internal_token: &self.internal_token,
            pull_secret: self.pull_secret.as_deref(),
        });

        let result = self.launch(&pod, &pod_name, port, engine).await;
        // Whatever happened, the staged RDB is no longer needed: a
        // successful Pod already fetched it; a failed one won't retry.
        self.pending_rdb.write().remove(&pod_name);
        result
    }

    async fn launch(
        &self,
        pod: &Pod,
        pod_name: &str,
        port: u16,
        engine: CacheEngineKind,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        self.client
            .create_pod(pod)
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("create cache pod: {e}")))?;

        let pod_ip = match self
            .client
            .wait_for_pod_ip(pod_name, Duration::from_secs(90))
            .await
        {
            Ok(ip) => ip,
            Err(e) => {
                self.client.delete_pod(pod_name).await;
                return Err(RuntimeError::ContainerStartFailed(e.to_string()));
            }
        };
        if let Err(e) = K8sClient::wait_for_tcp(&pod_ip, port, Duration::from_secs(30)).await {
            self.client.delete_pod(pod_name).await;
            return Err(RuntimeError::ContainerStartFailed(format!(
                "cache pod {pod_name} ({pod_ip}:{port}) not ready: {e}"
            )));
        }

        Ok(RunningCacheContainer {
            container_id: pod_name.to_string(),
            host_port: port,
            endpoint_address: pod_ip,
            endpoint_port: port,
            engine,
        })
    }

    pub(super) async fn delete_pod(&self, pod_name: &str) {
        self.client.delete_pod(pod_name).await;
    }

    pub(super) async fn exec_redis(
        &self,
        pod_name: &str,
        redis_args: &[String],
    ) -> Result<CacheExec, RuntimeError> {
        let mut cmd: Vec<&str> = vec!["redis-cli"];
        cmd.extend(redis_args.iter().map(String::as_str));
        let out = self
            .client
            .exec(pod_name, Some(CONTAINER), &cmd)
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("exec redis-cli: {e}")))?;
        Ok(CacheExec {
            success: out.success(),
            stdout: out.stdout,
            stderr: out.stderr.into_bytes(),
        })
    }

    pub(super) async fn dump_rdb(
        &self,
        pod_name: &str,
        dest_path: &str,
    ) -> Result<(), RuntimeError> {
        let save = self
            .client
            .exec(pod_name, Some(CONTAINER), &["redis-cli", "SAVE"])
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("exec SAVE: {e}")))?;
        if !save.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "redis SAVE failed: {}",
                save.stderr.trim()
            )));
        }
        let cat = self
            .client
            .exec(pod_name, Some(CONTAINER), &["cat", "/data/dump.rdb"])
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("exec cat rdb: {e}")))?;
        if !cat.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "reading dump.rdb from pod failed: {}",
                cat.stderr.trim()
            )));
        }
        tokio::fs::write(dest_path, &cat.stdout)
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("writing {dest_path}: {e}")))?;
        Ok(())
    }

    /// Reboot = recreate the Pod. A Pod can't be restarted in place, so
    /// for Redis we snapshot the live data and reload it into the fresh
    /// Pod, preserving the dataset across the reboot (matching the Docker
    /// backend's in-place `docker restart`). Memcached has no
    /// persistence, so its reboot is a plain recreate (a flush — which is
    /// also what a real memcached reboot does).
    pub(super) async fn reboot_pod(
        &self,
        resource_id: &str,
        running: &RunningCacheContainer,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        let preserved = if matches!(running.engine, CacheEngineKind::Redis) {
            self.snapshot_live_rdb(&running.container_id).await
        } else {
            None
        };
        self.client.delete_pod(&running.container_id).await;
        self.spawn_pod_bytes(resource_id, running.engine, preserved)
            .await
    }

    /// Best-effort `SAVE` + read of the current `dump.rdb` out of a Pod,
    /// returning the bytes. Returns `None` (start empty) if the snapshot
    /// can't be captured rather than failing the reboot.
    async fn snapshot_live_rdb(&self, pod_name: &str) -> Option<Vec<u8>> {
        let _ = self
            .client
            .exec(pod_name, Some(CONTAINER), &["redis-cli", "SAVE"])
            .await;
        let cat = self
            .client
            .exec(pod_name, Some(CONTAINER), &["cat", "/data/dump.rdb"])
            .await
            .ok()?;
        if cat.success() && !cat.stdout.is_empty() {
            Some(cat.stdout)
        } else {
            None
        }
    }

    pub(super) async fn reap_stale(&self) {
        self.client.reap_stale(SERVICE).await;
    }
}

/// Inputs for [`build_cache_pod`].
struct CachePodContext<'a> {
    pod_name: &'a str,
    namespace: &'a str,
    instance_id: &'a str,
    resource_id: &'a str,
    image: &'a str,
    port: u16,
    /// When set (Redis restore), the container fetches this URL into
    /// `/data/dump.rdb` before starting `redis-server`.
    rdb_url: Option<&'a str>,
    internal_token: &'a str,
    pull_secret: Option<&'a str>,
}

/// Build the Pod spec for one cache backing instance. Pure — no cluster
/// I/O — so it's unit-testable.
fn build_cache_pod(ctx: CachePodContext<'_>) -> Pod {
    let mut pod_labels = std::collections::BTreeMap::new();
    pod_labels.insert(
        labels::MANAGED_BY.to_string(),
        labels::MANAGED_BY_VALUE.to_string(),
    );
    pod_labels.insert(labels::INSTANCE.to_string(), ctx.instance_id.to_string());
    pod_labels.insert(labels::SERVICE.to_string(), SERVICE.to_string());
    pod_labels.insert(
        "fakecloud-elasticache".to_string(),
        names::label_safe(ctx.resource_id),
    );

    // When restoring a snapshot, override the command to fetch the RDB
    // into /data before launching redis-server. Otherwise use the image's
    // default entrypoint.
    let (command, env) = match ctx.rdb_url {
        Some(url) => {
            let script = "set -e; \
                 wget -q --header=\"authorization: Bearer $FAKECLOUD_RDB_TOKEN\" \
                      -O /data/dump.rdb \"$FAKECLOUD_RDB_URL\"; \
                 exec redis-server"
                .to_string();
            (
                Some(vec!["sh".to_string(), "-c".to_string(), script]),
                Some(vec![
                    EnvVar {
                        name: "FAKECLOUD_RDB_URL".to_string(),
                        value: Some(url.to_string()),
                        value_from: None,
                    },
                    EnvVar {
                        name: "FAKECLOUD_RDB_TOKEN".to_string(),
                        value: Some(ctx.internal_token.to_string()),
                        value_from: None,
                    },
                ]),
            )
        }
        None => (None, None),
    };

    let container = Container {
        name: CONTAINER.to_string(),
        image: Some(ctx.image.to_string()),
        command,
        env,
        ports: Some(vec![ContainerPort {
            container_port: ctx.port as i32,
            ..ContainerPort::default()
        }]),
        ..Container::default()
    };

    let pull_secrets = ctx.pull_secret.map(|name| {
        vec![LocalObjectReference {
            name: name.to_string(),
        }]
    });

    Pod {
        metadata: ObjectMeta {
            name: Some(ctx.pod_name.to_string()),
            namespace: Some(ctx.namespace.to_string()),
            labels: Some(pod_labels),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            // We manage lifecycle explicitly (reboot recreates the Pod),
            // so the kubelet shouldn't restart the container itself.
            restart_policy: Some("Never".to_string()),
            containers: vec![container],
            image_pull_secrets: pull_secrets,
            ..PodSpec::default()
        }),
        ..Pod::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(rdb_url: Option<&'a str>) -> CachePodContext<'a> {
        CachePodContext {
            pod_name: "fakecloud-ec-mycache-abc123",
            namespace: "fakecloud",
            instance_id: "fakecloud-1234",
            resource_id: "My_Cache",
            image: "redis:7-alpine",
            port: 6379,
            rdb_url,
            internal_token: "secret-token",
            pull_secret: None,
        }
    }

    #[test]
    fn pod_has_ownership_labels() {
        let pod = build_cache_pod(ctx(None));
        let l = pod.metadata.labels.unwrap();
        assert_eq!(l.get(labels::MANAGED_BY).unwrap(), labels::MANAGED_BY_VALUE);
        assert_eq!(l.get(labels::SERVICE).unwrap(), "elasticache");
        assert_eq!(l.get(labels::INSTANCE).unwrap(), "fakecloud-1234");
        // resource id slug is DNS-safe (underscores -> dashes, lowercased).
        assert_eq!(l.get("fakecloud-elasticache").unwrap(), "my-cache");
    }

    #[test]
    fn container_exposes_engine_port_and_image() {
        let pod = build_cache_pod(ctx(None));
        let c = &pod.spec.unwrap().containers[0];
        assert_eq!(c.image.as_deref(), Some("redis:7-alpine"));
        assert_eq!(c.ports.as_ref().unwrap()[0].container_port, 6379);
    }

    #[test]
    fn no_rdb_uses_default_entrypoint() {
        let pod = build_cache_pod(ctx(None));
        let c = &pod.spec.unwrap().containers[0];
        assert!(c.command.is_none());
        assert!(c.env.is_none());
    }

    #[test]
    fn rdb_restore_overrides_command_and_sets_env() {
        let pod = build_cache_pod(ctx(Some(
            "http://fc:4566/_fakecloud/elasticache/_internal/rdb/p",
        )));
        let c = &pod.spec.unwrap().containers[0];
        let script = c.command.as_ref().unwrap().last().unwrap();
        assert!(script.contains("wget"), "should fetch rdb: {script}");
        assert!(script.contains("/data/dump.rdb"));
        assert!(script.contains("exec redis-server"));
        // The token is referenced via env, never inlined into the script.
        assert!(script.contains("$FAKECLOUD_RDB_TOKEN"));
        assert!(!script.contains("secret-token"));
        let env = c.env.as_ref().unwrap();
        assert!(env.iter().any(|e| e.name == "FAKECLOUD_RDB_URL"));
        assert!(
            env.iter()
                .any(|e| e.name == "FAKECLOUD_RDB_TOKEN"
                    && e.value.as_deref() == Some("secret-token"))
        );
    }

    #[test]
    fn restart_policy_never() {
        let pod = build_cache_pod(ctx(None));
        assert_eq!(pod.spec.unwrap().restart_policy.as_deref(), Some("Never"));
    }

    #[test]
    fn pull_secret_attached_when_set() {
        let mut c = ctx(None);
        c.pull_secret = Some("reg-secret");
        let pod = build_cache_pod(c);
        let secrets = pod.spec.unwrap().image_pull_secrets.unwrap();
        assert_eq!(secrets[0].name, "reg-secret");
    }
}
