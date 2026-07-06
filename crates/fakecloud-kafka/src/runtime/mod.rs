//! Backing-container runtime for Amazon MSK (`kafka`) clusters.
//!
//! An MSK cluster in fakecloud is backed by a REAL single-node Apache Kafka
//! broker container (KRaft combined mode) so a client application genuinely
//! produces and consumes through it -- the same bar Amazon MQ / RDS /
//! ElastiCache / Lambda meet, not a formatted-but-dead `*.amazonaws.com`
//! bootstrap string. Topic create/delete/describe/alter operations are driven
//! against the live broker with its own `/opt/kafka/bin/*.sh` tools, and
//! `GetBootstrapBrokers` returns the reachable `host:port`.
//!
//! Image: `apache/kafka:3.8.0` (the official Apache image; KRaft combined mode,
//! broker tools under `/opt/kafka/bin/`), overridable via `FAKECLOUD_KAFKA_IMAGE`.
//!
//! The hard correctness detail is the **advertised listener**. A client that
//! connects to a listener is redirected to whatever that listener advertises,
//! and follows the advertised address for all subsequent work. A single
//! listener advertised as the host port (`{advertise_host}:{P}`) is unreachable
//! from INSIDE the container (only `9092` is open there), so the in-container
//! admin tools -- which follow the advertised address after bootstrapping --
//! cannot talk to the broker. The faithful, actually-working shape is therefore
//! TWO plaintext listeners:
//!
//! - `INTERNAL://:9092` advertised as `INTERNAL://localhost:9092` -- what the
//!   in-container admin tools (topic CRUD, readiness) target; reachable from
//!   inside the container. Also the inter-broker listener.
//! - `EXTERNAL://:19092` advertised as `EXTERNAL://{advertise_host}:{P}` and
//!   published to a fixed free host port (`-p {P}:19092`) -- what external
//!   clients (the E2E producer/consumer, `GetBootstrapBrokers`) connect to.
//!
//! We allocate the free host TCP port `P` FIRST (unlike MQ's ephemeral
//! publish-then-read) so we can both publish AND advertise it. `advertise_host`
//! is the shared [`fakecloud_core::container_net`] sibling host (`127.0.0.1`
//! normally, `host.docker.internal` / `host.containers.internal` when fakecloud
//! is itself containerized), the exact value MQ records in its data-plane
//! binding, so Kafka can't drift from the issue #1539 portability fixes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

/// A running cluster's backing Kafka broker container binding.
#[derive(Debug, Clone)]
pub struct RunningBroker {
    pub container_id: String,
    /// Address clients reach the published PLAINTEXT port at (`127.0.0.1` or the
    /// sibling host alias when fakecloud is containerized).
    pub host: String,
    /// Protocol label -> published host port (`plaintext` -> P).
    pub ports: BTreeMap<String, u16>,
}

impl RunningBroker {
    /// The reachable `host:port` PLAINTEXT bootstrap string for external clients.
    pub fn bootstrap_string(&self) -> Option<String> {
        self.ports
            .get("plaintext")
            .map(|p| format!("{}:{}", self.host, p))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("container runtime is unavailable")]
    Unavailable,
    #[error("broker container failed to start: {0}")]
    ContainerStartFailed(String),
}

/// A topic-operation failure, mapped by the service to the right MSK error shape.
#[derive(Debug, thiserror::Error)]
pub enum TopicError {
    #[error("topic already exists")]
    AlreadyExists,
    #[error("topic does not exist")]
    NotFound,
    #[error("{0}")]
    Broker(String),
}

/// Per-topic summary parsed from `kafka-topics.sh --describe`.
#[derive(Debug, Clone)]
pub struct TopicMeta {
    pub name: String,
    pub partition_count: i64,
    pub replication_factor: i64,
    pub configs: String,
    pub partitions: Vec<PartitionMeta>,
}

/// A single partition's placement parsed from the broker.
#[derive(Debug, Clone)]
pub struct PartitionMeta {
    pub partition: i64,
    pub leader: i64,
    pub replicas: Vec<i64>,
    pub isr: Vec<i64>,
}

/// Docker/Podman-backed single-node Kafka broker runtime for MSK clusters.
#[derive(Debug, Clone)]
pub struct KafkaRuntime {
    cli: String,
    net: fakecloud_core::container_net::HostNetworking,
    instance_id: String,
    /// Cluster ARN -> running broker, for reboot/stop/exec lookups.
    containers: Arc<RwLock<HashMap<String, RunningBroker>>>,
}

impl KafkaRuntime {
    /// Construct the Docker/Podman runtime. Returns `None` when no container CLI
    /// is available (fakecloud then degrades to control-plane-only MSK), or when
    /// the real backend is explicitly disabled via `FAKECLOUD_KAFKA_DISABLE_BACKEND`
    /// (the terraform-provider acceptance harness sets this: tfacc asserts the
    /// AWS response *format* -- the cosmetic `*.amazonaws.com` bootstrap strings
    /// -- and the data plane is proven separately by the E2E suite, so spawning a
    /// real broker there would make MSK return real `127.0.0.1:<port>` endpoints
    /// that fail the provider's AWS-format assertions).
    pub fn new() -> Option<Self> {
        if std::env::var("FAKECLOUD_KAFKA_DISABLE_BACKEND")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        {
            return None;
        }
        let cli = fakecloud_core::container_net::detect_container_cli()?;
        let net = fakecloud_core::container_net::HostNetworking::detect(&cli);
        Some(Self {
            cli,
            net,
            instance_id: format!("fakecloud-{}", std::process::id()),
            containers: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Name of the active container CLI, for logging.
    pub fn cli_name(&self) -> &str {
        &self.cli
    }

    /// The Kafka broker image, overridable via `FAKECLOUD_KAFKA_IMAGE`.
    fn image(&self) -> String {
        std::env::var("FAKECLOUD_KAFKA_IMAGE")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "apache/kafka:3.8.0".to_string())
    }

    /// Spawn (or, if one is already tracked, return) the backing Kafka broker
    /// container for a cluster and block until its broker API answers.
    pub async fn ensure_broker(&self, cluster_arn: &str) -> Result<RunningBroker, RuntimeError> {
        if let Some(existing) = self.containers.read().get(cluster_arn).cloned() {
            return Ok(existing);
        }
        let running = self.spawn_container(cluster_arn).await?;
        self.containers
            .write()
            .insert(cluster_arn.to_string(), running.clone());
        Ok(running)
    }

    /// Reboot a cluster's broker, mirroring AWS: a reboot RESTARTS the broker in
    /// place and preserves the topic log. The SAME backing container is stopped
    /// and started again -- its log dir is never touched, so topics/messages
    /// survive. The published host port is re-read (it is fixed at `-p P:19092`,
    /// so it stays stable, but reading it keeps the advertised binding honest).
    /// Only if the tracked container is truly gone does this spawn a fresh one.
    pub async fn reboot_broker(&self, cluster_arn: &str) -> Result<RunningBroker, RuntimeError> {
        let existing = self.containers.read().get(cluster_arn).cloned();
        if let Some(existing) = existing {
            match self.restart_container(&existing.container_id).await? {
                Some(running) => {
                    self.containers
                        .write()
                        .insert(cluster_arn.to_string(), running.clone());
                    return Ok(running);
                }
                None => {
                    self.containers.write().remove(cluster_arn);
                }
            }
        }
        let running = self.spawn_container(cluster_arn).await?;
        self.containers
            .write()
            .insert(cluster_arn.to_string(), running.clone());
        Ok(running)
    }

    /// Restart an EXISTING backing container in place, preserving its log dir.
    /// `Ok(None)` when the container is gone (caller spawns fresh), `Err` on a
    /// real daemon/readiness failure, `Ok(Some(..))` once it is running again.
    async fn restart_container(
        &self,
        container_id: &str,
    ) -> Result<Option<RunningBroker>, RuntimeError> {
        let inspect = tokio::process::Command::new(&self.cli)
            .args(["inspect", "--format", "{{.State.Status}}", container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !inspect.status.success() {
            return Ok(None);
        }
        let _ = tokio::process::Command::new(&self.cli)
            .args(["restart", container_id])
            .output()
            .await;
        let port = self.lookup_port(container_id, 19092).await?;
        let ports = BTreeMap::from([("plaintext".to_string(), port)]);
        self.wait_for_broker_ready(container_id).await?;
        Ok(Some(RunningBroker {
            container_id: container_id.to_string(),
            host: self.net.sibling_host.clone(),
            ports,
        }))
    }

    /// Re-attach to a cluster's PERSISTED backing container after a fakecloud
    /// restart, preserving its topic log, rather than spawning a fresh empty one.
    /// `Ok(None)` when the container is truly gone (the caller creates fresh),
    /// `Err` on a transient daemon failure, `Ok(Some(..))` once it is running.
    pub async fn reattach_broker(
        &self,
        cluster_arn: &str,
        container_id: &str,
    ) -> Result<Option<RunningBroker>, RuntimeError> {
        let inspect = tokio::process::Command::new(&self.cli)
            .args(["inspect", "--format", "{{.State.Status}}", container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !inspect.status.success() {
            return Ok(None);
        }
        let start = tokio::process::Command::new(&self.cli)
            .args(["start", container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !start.status.success() {
            let stderr = String::from_utf8_lossy(&start.stderr);
            if stderr.contains("No such container") || stderr.contains("no such container") {
                return Ok(None);
            }
            return Err(RuntimeError::ContainerStartFailed(format!(
                "reattach start failed: {}",
                stderr.trim()
            )));
        }
        let port = self.lookup_port(container_id, 19092).await?;
        let ports = BTreeMap::from([("plaintext".to_string(), port)]);
        self.wait_for_broker_ready(container_id).await?;
        let running = RunningBroker {
            container_id: container_id.to_string(),
            host: self.net.sibling_host.clone(),
            ports,
        };
        self.containers
            .write()
            .insert(cluster_arn.to_string(), running.clone());
        tracing::info!(
            cluster_arn = %cluster_arn,
            container_id = %container_id,
            "re-attached persisted MSK Kafka broker container",
        );
        Ok(Some(running))
    }

    /// Stop + remove a cluster's backing container and drop its tracking entry.
    pub async fn stop_broker(&self, cluster_arn: &str) {
        let running = self.containers.write().remove(cluster_arn);
        if let Some(running) = running {
            self.remove_container(&running.container_id).await;
        }
    }

    /// Stop every tracked broker container (graceful shutdown / reset).
    pub async fn stop_all(&self) {
        let containers: Vec<RunningBroker> = {
            let mut map = self.containers.write();
            map.drain().map(|(_, c)| c).collect()
        };
        for c in containers {
            self.remove_container(&c.container_id).await;
        }
    }

    async fn spawn_container(&self, cluster_arn: &str) -> Result<RunningBroker, RuntimeError> {
        // Reap a leaked container left by a PRIOR failed bring-up of THIS cluster,
        // scoped to this cluster AND this fakecloud instance.
        self.reap_stale_cluster_containers(cluster_arn).await;

        // Allocate a fixed free host port FIRST so we can BOTH publish and
        // advertise it (the advertised listener must name a port the client can
        // actually reach; ephemeral publishing would not let us advertise it).
        let port = alloc_free_port()?;
        let advertise = self.net.sibling_host.clone();

        let mut args: Vec<String> = vec!["create".to_string()];
        args.push("-p".to_string());
        // Publish the EXTERNAL listener (container port 19092) to the fixed host
        // port; the INTERNAL listener (9092) stays container-private.
        args.push(format!("{port}:19092"));
        args.push("--label".to_string());
        args.push(format!("fakecloud-kafka={cluster_arn}"));
        args.push("--label".to_string());
        args.push(format!("fakecloud-instance={}", self.instance_id));
        self.net.push_add_host_args(&mut args);
        for (k, v) in kafka_env(&advertise, port) {
            args.push("-e".to_string());
            args.push(format!("{k}={v}"));
        }
        args.push(self.image());

        let output = tokio::process::Command::new(&self.cli)
            .args(&args)
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(
                String::from_utf8_lossy(&output.stderr).trim().to_string(),
            ));
        }
        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();

        let start = tokio::process::Command::new(&self.cli)
            .args(["start", &container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !start.status.success() {
            // Leave the container un-reaped for post-mortem; the next bring-up's
            // stale-reap and the server-shutdown sweep clean it up.
            let diag = self.capture_container_diagnostics(&container_id).await;
            return Err(RuntimeError::ContainerStartFailed(format!(
                "container start failed: {}; {diag}",
                String::from_utf8_lossy(&start.stderr).trim()
            )));
        }

        self.wait_for_broker_ready(&container_id).await?;

        tracing::info!(
            cluster_arn = %cluster_arn,
            container_id = %container_id,
            host = %advertise,
            port = port,
            "MSK Kafka broker container started",
        );

        Ok(RunningBroker {
            container_id,
            host: advertise,
            ports: BTreeMap::from([("plaintext".to_string(), port)]),
        })
    }

    /// Read the host port `container_port` (9092) is published on.
    async fn lookup_port(
        &self,
        container_id: &str,
        container_port: u16,
    ) -> Result<u16, RuntimeError> {
        let out = tokio::process::Command::new(&self.cli)
            .args(["port", container_id, &container_port.to_string()])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !out.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "port lookup for {container_port} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        let s = String::from_utf8_lossy(&out.stdout);
        s.lines()
            .next()
            .and_then(|l| l.trim().rsplit(':').next())
            .and_then(|p| p.parse::<u16>().ok())
            .ok_or_else(|| {
                RuntimeError::ContainerStartFailed(format!(
                    "could not determine host port for {container_port} from '{}'",
                    s.trim()
                ))
            })
    }

    /// Block until the broker answers its admin API. A bare TCP connect races
    /// the KRaft startup (the listener binds before the metadata quorum is
    /// ready), so poll `kafka-topics.sh --list`, which only succeeds once the
    /// broker is genuinely serving. On timeout the container's recent logs and
    /// terminal state are surfaced so a CI failure is diagnosable.
    async fn wait_for_broker_ready(&self, container_id: &str) -> Result<(), RuntimeError> {
        // 240 * 500ms = ~120s (plus per-probe time): generous for a cold KRaft
        // format + boot on a constrained CI runner.
        const ATTEMPTS: u32 = 240;
        let start = std::time::Instant::now();
        for _ in 0..ATTEMPTS {
            if self
                .exec_ok(
                    container_id,
                    &[
                        "/opt/kafka/bin/kafka-topics.sh",
                        "--bootstrap-server",
                        "localhost:9092",
                        "--list",
                    ],
                )
                .await
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(RuntimeError::ContainerStartFailed(
            self.readiness_timeout_reason(container_id, start).await,
        ))
    }

    // ===================== topic operations =====================

    /// Create a topic on the live broker. `replication_factor` MUST be `1` on a
    /// single-node broker; the caller clamps it. Broker "already exists" maps to
    /// [`TopicError::AlreadyExists`].
    pub async fn create_topic(
        &self,
        container_id: &str,
        name: &str,
        partitions: i64,
        replication_factor: i64,
        configs: &[(String, String)],
    ) -> Result<(), TopicError> {
        let partitions = partitions.max(1).to_string();
        let rf = replication_factor.max(1).to_string();
        let mut argv: Vec<String> = vec![
            "/opt/kafka/bin/kafka-topics.sh".into(),
            "--bootstrap-server".into(),
            "localhost:9092".into(),
            "--create".into(),
            "--topic".into(),
            name.into(),
            "--partitions".into(),
            partitions,
            "--replication-factor".into(),
            rf,
        ];
        for (k, v) in configs {
            argv.push("--config".into());
            argv.push(format!("{k}={v}"));
        }
        let out = self.exec(container_id, &str_args(&argv)).await?;
        if out.ok {
            return Ok(());
        }
        if out.combined.contains("already exists") {
            return Err(TopicError::AlreadyExists);
        }
        Err(TopicError::Broker(sanitize(&out.combined)))
    }

    /// Delete a topic on the live broker. Broker "does not exist" maps to
    /// [`TopicError::NotFound`].
    pub async fn delete_topic(&self, container_id: &str, name: &str) -> Result<(), TopicError> {
        let argv = [
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--delete",
            "--topic",
            name,
        ];
        let out = self.exec(container_id, &argv).await?;
        if out.ok {
            return Ok(());
        }
        if out.combined.contains("does not exist")
            || out.combined.contains("UnknownTopicOrPartition")
        {
            return Err(TopicError::NotFound);
        }
        Err(TopicError::Broker(sanitize(&out.combined)))
    }

    /// List every topic on the broker with its partition/replica metadata,
    /// parsed from a single bulk `--describe`.
    pub async fn list_topics(&self, container_id: &str) -> Result<Vec<TopicMeta>, TopicError> {
        let argv = [
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
        ];
        let out = self.exec(container_id, &argv).await?;
        if !out.ok {
            return Err(TopicError::Broker(sanitize(&out.combined)));
        }
        Ok(parse_describe(&out.stdout))
    }

    /// Describe a single topic on the broker; `NotFound` when it does not exist.
    pub async fn describe_topic(
        &self,
        container_id: &str,
        name: &str,
    ) -> Result<TopicMeta, TopicError> {
        let argv = [
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--topic",
            name,
        ];
        let out = self.exec(container_id, &argv).await?;
        if !out.ok {
            if out.combined.contains("does not exist")
                || out.combined.contains("UnknownTopicOrPartition")
            {
                return Err(TopicError::NotFound);
            }
            return Err(TopicError::Broker(sanitize(&out.combined)));
        }
        parse_describe(&out.stdout)
            .into_iter()
            .find(|t| t.name == name)
            .ok_or(TopicError::NotFound)
    }

    /// Increase a topic's partition count (`--alter --partitions`). Kafka only
    /// permits growing the count; a decrease surfaces as a broker error.
    pub async fn alter_partitions(
        &self,
        container_id: &str,
        name: &str,
        partitions: i64,
    ) -> Result<(), TopicError> {
        let p = partitions.max(1).to_string();
        let argv = [
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--alter",
            "--topic",
            name,
            "--partitions",
            &p,
        ];
        let out = self.exec(container_id, &argv).await?;
        if out.ok {
            return Ok(());
        }
        if out.combined.contains("does not exist")
            || out.combined.contains("UnknownTopicOrPartition")
        {
            return Err(TopicError::NotFound);
        }
        Err(TopicError::Broker(sanitize(&out.combined)))
    }

    /// Apply topic-level config overrides via `kafka-configs.sh --alter`.
    pub async fn alter_configs(
        &self,
        container_id: &str,
        name: &str,
        configs: &[(String, String)],
    ) -> Result<(), TopicError> {
        if configs.is_empty() {
            return Ok(());
        }
        let joined = configs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        let argv = [
            "/opt/kafka/bin/kafka-configs.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            name,
            "--add-config",
            &joined,
        ];
        let out = self.exec(container_id, &argv).await?;
        if out.ok {
            return Ok(());
        }
        if out.combined.contains("does not exist")
            || out.combined.contains("UnknownTopicOrPartition")
        {
            return Err(TopicError::NotFound);
        }
        Err(TopicError::Broker(sanitize(&out.combined)))
    }

    // ===================== exec plumbing =====================

    /// Run `<cli> exec <id> <argv...>` and capture stdout/stderr + success.
    async fn exec(&self, container_id: &str, argv: &[&str]) -> Result<ExecOut, TopicError> {
        let mut args = vec!["exec", container_id];
        args.extend_from_slice(argv);
        let out = tokio::process::Command::new(&self.cli)
            .args(&args)
            .output()
            .await
            .map_err(|e| TopicError::Broker(format!("container exec failed: {e}")))?;
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        let combined = format!("{stdout}\n{stderr}");
        Ok(ExecOut {
            ok: out.status.success(),
            stdout,
            combined,
        })
    }

    /// Run `<cli> exec <id> <argv...>` and report whether it exited 0.
    async fn exec_ok(&self, container_id: &str, argv: &[&str]) -> bool {
        let mut args = vec!["exec", container_id];
        args.extend_from_slice(argv);
        tokio::process::Command::new(&self.cli)
            .args(&args)
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    async fn remove_container(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }

    /// Remove any container labeled for THIS cluster AND this fakecloud instance.
    async fn reap_stale_cluster_containers(&self, cluster_arn: &str) {
        let Ok(out) = tokio::process::Command::new(&self.cli)
            .args([
                "ps",
                "-aq",
                "--filter",
                &format!("label=fakecloud-kafka={cluster_arn}"),
                "--filter",
                &format!("label=fakecloud-instance={}", self.instance_id),
            ])
            .output()
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

    /// Build a self-describing `stateInfo` message for a readiness timeout AND
    /// log it: whether the container CRASHED during boot (vs. booting slowly) and
    /// the tail of its logs, so a FAILED cluster is diagnosable from Describe.
    async fn readiness_timeout_reason(
        &self,
        container_id: &str,
        started: std::time::Instant,
    ) -> String {
        let state = tokio::process::Command::new(&self.cli)
            .args([
                "inspect",
                "--format",
                "status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}",
                container_id,
            ])
            .output()
            .await
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        let exited = state.contains("status=exited");
        let logs = self.container_logs(container_id, 60).await;
        let headline = if exited {
            "Kafka broker application exited during boot before becoming ready".to_string()
        } else {
            "Kafka broker did not become ready within 120 seconds (kafka-topics.sh --list never succeeded); the container is still running".to_string()
        };
        let reason =
            format!("{headline}; container state [{state}]; recent container logs:\n{logs}");
        tracing::error!(
            container_id = %container_id,
            elapsed_secs = started.elapsed().as_secs(),
            container_state = %state,
            reason = %headline,
            container_logs = %logs,
            "MSK Kafka broker readiness timed out",
        );
        reason
    }

    async fn capture_container_diagnostics(&self, container_id: &str) -> String {
        let state = tokio::process::Command::new(&self.cli)
            .args([
                "inspect",
                "--format",
                "status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} error={{.State.Error}}",
                container_id,
            ])
            .output()
            .await
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        let logs = self.container_logs(container_id, 80).await;
        format!("container state [{state}]; recent container logs:\n{logs}")
    }

    async fn container_logs(&self, container_id: &str, tail: u32) -> String {
        tokio::process::Command::new(&self.cli)
            .args(["logs", "--tail", &tail.to_string(), container_id])
            .output()
            .await
            .map(|o| {
                let mut s = String::from_utf8_lossy(&o.stdout).into_owned();
                s.push_str(&String::from_utf8_lossy(&o.stderr));
                s.trim().to_string()
            })
            .unwrap_or_else(|e| format!("<container logs failed: {e}>"))
    }
}

struct ExecOut {
    ok: bool,
    stdout: String,
    combined: String,
}

/// Borrow a `Vec<String>` as `&[&str]` for the exec argv.
fn str_args(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

/// The KRaft single-node broker env: all replication factors 1, and TWO
/// plaintext listeners. INTERNAL (`:9092`) is advertised as `localhost:9092`
/// for the in-container admin tools (and inter-broker traffic); EXTERNAL
/// (`:19092`) is advertised as `{advertise_host}:{port}` -- the fixed published
/// host port -- for external clients. Both are needed: the in-container tools
/// follow the advertised address, so a single host-port-advertised listener
/// would be unreachable from inside the container.
fn kafka_env(advertise_host: &str, port: u16) -> Vec<(&'static str, String)> {
    vec![
        ("KAFKA_NODE_ID", "1".to_string()),
        ("KAFKA_PROCESS_ROLES", "broker,controller".to_string()),
        (
            "KAFKA_LISTENERS",
            "INTERNAL://:9092,EXTERNAL://:19092,CONTROLLER://:9093".to_string(),
        ),
        (
            "KAFKA_ADVERTISED_LISTENERS",
            format!("INTERNAL://localhost:9092,EXTERNAL://{advertise_host}:{port}"),
        ),
        (
            "KAFKA_CONTROLLER_QUORUM_VOTERS",
            "1@localhost:9093".to_string(),
        ),
        ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER".to_string()),
        (
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT".to_string(),
        ),
        ("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL".to_string()),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1".to_string()),
        (
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
            "1".to_string(),
        ),
        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1".to_string()),
        ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0".to_string()),
    ]
}

/// Bind a `127.0.0.1:0` listener, read the assigned port, and drop it -- the
/// standard free-port trick. The port is then BOTH published and advertised.
fn alloc_free_port() -> Result<u16, RuntimeError> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).map_err(|e| {
        RuntimeError::ContainerStartFailed(format!("could not allocate a host port: {e}"))
    })?;
    let port = listener
        .local_addr()
        .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?
        .port();
    drop(listener);
    Ok(port)
}

/// Sanitize multi-line broker output for embedding in an error string: take the
/// most informative line, strip control chars, and bound the length. NEVER put
/// raw broker stderr into a response header (issue #1539 Bug 2).
fn sanitize(s: &str) -> String {
    let line = s
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with("Error while executing topic command"))
        .or_else(|| s.lines().map(str::trim).find(|l| !l.is_empty()))
        .unwrap_or("")
        .replace(|c: char| c.is_control(), " ");
    let line = line.trim();
    if line.len() > 300 {
        format!("{}...", &line[..300])
    } else {
        line.to_string()
    }
}

/// Parse `kafka-topics.sh --describe` output into per-topic metadata. Header
/// lines carry `PartitionCount`/`ReplicationFactor`/`Configs`; indented lines
/// carry per-`Partition` `Leader`/`Replicas`/`Isr`. Tolerant of field-order and
/// version differences by scanning `Key:`-`Value` token pairs.
fn parse_describe(stdout: &str) -> Vec<TopicMeta> {
    let mut topics: Vec<TopicMeta> = Vec::new();
    for line in stdout.lines() {
        let fields = kv_fields(line);
        let Some(name) = fields.get("Topic").cloned() else {
            continue;
        };
        if fields.contains_key("Partition") {
            // A per-partition line for the current (last-seen) topic.
            if let Some(t) = topics.iter_mut().rev().find(|t| t.name == name) {
                t.partitions.push(PartitionMeta {
                    partition: fields
                        .get("Partition")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0),
                    leader: fields
                        .get("Leader")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(-1),
                    replicas: parse_int_list(fields.get("Replicas")),
                    isr: parse_int_list(fields.get("Isr")),
                });
            }
        } else {
            // A topic header line.
            topics.push(TopicMeta {
                name,
                partition_count: fields
                    .get("PartitionCount")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0),
                replication_factor: fields
                    .get("ReplicationFactor")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(1),
                configs: fields.get("Configs").cloned().unwrap_or_default(),
                partitions: Vec::new(),
            });
        }
    }
    // Fill partition_count from the parsed partition lines if the header lacked it.
    for t in &mut topics {
        if t.partition_count == 0 && !t.partitions.is_empty() {
            t.partition_count = t.partitions.len() as i64;
        }
    }
    topics
}

/// Scan a describe line into `Key -> Value` pairs. Tokens are tab/space
/// separated; a token ending in `:` names the next token's value. `Configs:` is
/// special-cased to absorb the remainder of the line (it can hold `k=v,k=v`).
fn kv_fields(line: &str) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    // Split on tabs first (kafka uses tab-separated fields), falling back to
    // whitespace within each field.
    let tokens: Vec<&str> = line
        .split(['\t'])
        .flat_map(|f| f.split_whitespace())
        .collect();
    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i];
        if let Some(key) = tok.strip_suffix(':') {
            if key == "Configs" {
                out.insert(key.to_string(), tokens[i + 1..].join(" "));
                break;
            }
            if let Some(val) = tokens.get(i + 1) {
                out.insert(key.to_string(), (*val).to_string());
                i += 2;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// Parse a comma-separated broker id list (`Replicas: 1,2,3`).
fn parse_int_list(v: Option<&String>) -> Vec<i64> {
    v.map(|s| s.split(',').filter_map(|p| p.trim().parse().ok()).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_free_port_returns_a_usable_port() {
        let p = alloc_free_port().expect("port");
        assert!(p >= 1024, "expected an ephemeral port, got {p}");
        // A second allocation should (almost always) differ or at least succeed.
        let _ = alloc_free_port().expect("second port");
    }

    #[test]
    fn kafka_env_advertises_the_allocated_host_port() {
        let env = kafka_env("127.0.0.1", 34567);
        let adv = env
            .iter()
            .find(|(k, _)| *k == "KAFKA_ADVERTISED_LISTENERS")
            .map(|(_, v)| v.clone())
            .unwrap();
        // INTERNAL is advertised for in-container tools; EXTERNAL carries the
        // reachable host port for external clients.
        assert_eq!(adv, "INTERNAL://localhost:9092,EXTERNAL://127.0.0.1:34567");
        // Single-node broker: every replication factor pinned to 1.
        for key in [
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR",
        ] {
            assert_eq!(
                env.iter().find(|(k, _)| *k == key).map(|(_, v)| v.as_str()),
                Some("1"),
                "{key} must be 1 on a single-node broker"
            );
        }
        // KRaft combined mode.
        assert_eq!(
            env.iter()
                .find(|(k, _)| *k == "KAFKA_PROCESS_ROLES")
                .map(|(_, v)| v.as_str()),
            Some("broker,controller")
        );
    }

    #[test]
    fn kafka_env_uses_sibling_host_when_containerized() {
        let env = kafka_env("host.docker.internal", 5000);
        let adv = env
            .iter()
            .find(|(k, _)| *k == "KAFKA_ADVERTISED_LISTENERS")
            .map(|(_, v)| v.clone())
            .unwrap();
        assert_eq!(
            adv,
            "INTERNAL://localhost:9092,EXTERNAL://host.docker.internal:5000"
        );
    }

    #[test]
    fn running_broker_bootstrap_string_is_host_port() {
        let rb = RunningBroker {
            container_id: "abc".into(),
            host: "127.0.0.1".into(),
            ports: BTreeMap::from([("plaintext".to_string(), 9099)]),
        };
        assert_eq!(rb.bootstrap_string().as_deref(), Some("127.0.0.1:9099"));
    }

    #[test]
    fn parse_describe_reads_header_and_partitions() {
        let out = "Topic: orders\tTopicId: abcDEF\tPartitionCount: 2\tReplicationFactor: 1\tConfigs: cleanup.policy=compact\n\
                   \tTopic: orders\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\n\
                   \tTopic: orders\tPartition: 1\tLeader: 1\tReplicas: 1\tIsr: 1\n";
        let topics = parse_describe(out);
        assert_eq!(topics.len(), 1);
        let t = &topics[0];
        assert_eq!(t.name, "orders");
        assert_eq!(t.partition_count, 2);
        assert_eq!(t.replication_factor, 1);
        assert!(t.configs.contains("cleanup.policy=compact"));
        assert_eq!(t.partitions.len(), 2);
        assert_eq!(t.partitions[0].leader, 1);
        assert_eq!(t.partitions[1].partition, 1);
        assert_eq!(t.partitions[0].replicas, vec![1]);
        assert_eq!(t.partitions[0].isr, vec![1]);
    }

    #[test]
    fn parse_describe_handles_multiple_topics() {
        let out = "Topic: a\tPartitionCount: 1\tReplicationFactor: 1\tConfigs:\n\
                   \tTopic: a\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\n\
                   Topic: b\tPartitionCount: 1\tReplicationFactor: 1\tConfigs:\n\
                   \tTopic: b\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\n";
        let topics = parse_describe(out);
        let names: Vec<&str> = topics.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn sanitize_strips_control_chars_and_picks_informative_line() {
        let raw = "Error while executing topic command : Topic 'x' already exists.\n[2024] ERROR org.apache.kafka ... \tTopicExistsException: Topic 'x' already exists.\n";
        let s = sanitize(raw);
        assert!(!s.contains('\n'));
        assert!(!s.contains('\t'));
        assert!(s.contains("already exists"));
    }
}
