//! Backing-container runtime for Amazon MQ brokers.
//!
//! An Amazon MQ broker in fakecloud is backed by a REAL message-broker
//! container so a client application genuinely connects and exchanges
//! messages -- the same bar RDS/ElastiCache/Lambda meet, not a
//! formatted-but-dead `*.amazonaws.com` endpoint.
//!
//! - `ACTIVEMQ` -> `apache/activemq-classic` (Apache ActiveMQ "Classic"),
//!   publishing OpenWire/AMQP/STOMP/MQTT/WS plus the web console. The
//!   broker's users are injected as a `simpleAuthenticationPlugin` (with a
//!   permissive `authorizationPlugin`) inside a generated `activemq.xml`
//!   that is `docker cp`-ed into the container's conf dir, so authentication
//!   is genuinely enforced against the broker's users. A user-supplied
//!   configuration revision that is already a complete standalone broker
//!   config (carries its own `<transportConnector>`) is applied verbatim
//!   instead -- the highest-fidelity win where the uploaded config actually
//!   configures the live broker.
//! - `RABBITMQ` -> `rabbitmq:3.13-alpine` (the lightweight image WITHOUT the
//!   heavy management-UI plugin bundle, which boots fast enough to settle
//!   reliably on a constrained CI runner), publishing AMQP. The create-time
//!   user is provisioned via `RABBITMQ_DEFAULT_USER`/`_PASS`; further
//!   `CreateUser`/`UpdateUser`/`DeleteUser` take effect immediately via
//!   `rabbitmqctl` (matching AWS, which applies RabbitMQ user changes without
//!   a reboot). The image is overridable via `FAKECLOUD_MQ_RABBITMQ_IMAGE`
//!   (e.g. pin `rabbitmq:3.13-management-alpine` to get the console back).
//!
//! Container-to-host networking (CLI detection, host alias, `--add-host`
//! injection, and the in-container sibling address) all come from the shared
//! [`fakecloud_core::container_net`] helper, so MQ can't drift apart from the
//! other container-spawning runtimes on the issue #1539 portability fixes
//! (podman rejects `--add-host …:host-gateway`; `127.0.0.1` is unreachable
//! when fakecloud is itself containerized). Code and config are staged with
//! `docker cp` into the created-but-not-started container, never a bind mount
//! (a host bind source is invisible to the daemon when fakecloud runs in a
//! container).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

/// Which broker engine a resource runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqEngine {
    ActiveMq,
    RabbitMq,
}

impl MqEngine {
    /// Parse the wire `engineType` (case-insensitive).
    pub fn from_wire(engine: &str) -> Self {
        if engine.eq_ignore_ascii_case("RABBITMQ") {
            MqEngine::RabbitMq
        } else {
            MqEngine::ActiveMq
        }
    }

    /// Container image for this engine.
    ///
    /// The RabbitMQ default is the `-alpine` (NOT `-management`) image: the
    /// management-UI plugin bundle is heavy and materially slows the Erlang boot
    /// on a constrained CI runner, which is exactly what pushed brokers past the
    /// readiness window. The alpine image boots fast and only serves AMQP, which
    /// is all the data plane needs. Each image is overridable via env
    /// (`FAKECLOUD_MQ_ACTIVEMQ_IMAGE` / `FAKECLOUD_MQ_RABBITMQ_IMAGE`) so a
    /// different tag can be pinned without a code change.
    fn image(self) -> String {
        let (var, default) = match self {
            MqEngine::ActiveMq => ("FAKECLOUD_MQ_ACTIVEMQ_IMAGE", "apache/activemq-classic"),
            MqEngine::RabbitMq => ("FAKECLOUD_MQ_RABBITMQ_IMAGE", "rabbitmq:3.13-alpine"),
        };
        std::env::var(var)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| default.to_string())
    }

    /// The `(label, container_port)` pairs published to ephemeral host ports.
    /// The label is what `BrokerDataPlane::ports` is keyed by and what the
    /// describe endpoints project from.
    ///
    /// RabbitMQ publishes only AMQP: the default image omits the management
    /// plugin, so there is no console listener on 15672 to publish (the describe
    /// endpoints project a console URL only when the port is actually mapped, so
    /// dropping it here simply means no dead console endpoint is advertised).
    fn published_ports(self) -> &'static [(&'static str, u16)] {
        match self {
            MqEngine::ActiveMq => &[
                ("openwire", 61616),
                ("amqp", 5672),
                ("stomp", 61613),
                ("mqtt", 1883),
                ("ws", 61614),
                ("console", 8161),
            ],
            MqEngine::RabbitMq => &[("amqp", 5672)],
        }
    }
}

/// A broker user to provision into the running broker.
#[derive(Debug, Clone)]
pub struct BrokerUser {
    pub username: String,
    pub password: String,
    pub groups: Vec<String>,
    pub console_access: bool,
}

/// A running broker's backing container binding.
#[derive(Debug, Clone)]
pub struct RunningBroker {
    pub container_id: String,
    /// Address clients reach the published ports at (`127.0.0.1` or the
    /// sibling host alias when fakecloud is containerized).
    pub host: String,
    /// Protocol label -> published host port.
    pub ports: BTreeMap<String, u16>,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("container runtime is unavailable")]
    Unavailable,
    #[error("broker container failed to start: {0}")]
    ContainerStartFailed(String),
}

/// Docker/Podman-backed Amazon MQ broker runtime.
#[derive(Debug, Clone)]
pub struct MqRuntime {
    cli: String,
    net: fakecloud_core::container_net::HostNetworking,
    instance_id: String,
    /// Broker id -> running container, for reboot/stop/exec lookups.
    containers: Arc<RwLock<HashMap<String, RunningBroker>>>,
}

impl MqRuntime {
    /// Construct the Docker/Podman runtime. Returns `None` when no container
    /// CLI is available (fakecloud then degrades to control-plane-only MQ), or
    /// when the real backend is explicitly disabled via
    /// `FAKECLOUD_MQ_DISABLE_BACKEND` (the terraform-provider acceptance
    /// harness sets this: tfacc asserts the AWS response *format* -- the
    /// cosmetic `*.amazonaws.com` endpoints -- and the data plane is proven
    /// separately by the E2E suite, so spawning a real broker there would make
    /// MQ return real `127.0.0.1:<port>` endpoints that fail the provider's
    /// AWS-format assertions).
    pub fn new() -> Option<Self> {
        if std::env::var("FAKECLOUD_MQ_DISABLE_BACKEND")
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

    /// Spawn (or, if one is already tracked, return) the backing container for
    /// a broker and block until its primary wire port accepts connections.
    /// `user_config` is the decoded configuration revision data (the
    /// `activemq.xml` for ActiveMQ, `rabbitmq.conf` for RabbitMQ).
    pub async fn ensure_broker(
        &self,
        broker_id: &str,
        engine: MqEngine,
        users: &[BrokerUser],
        user_config: Option<&str>,
    ) -> Result<RunningBroker, RuntimeError> {
        // If a live container is already tracked for this broker, reuse it
        // (idempotent create/recover).
        if let Some(existing) = self.containers.read().get(broker_id).cloned() {
            return Ok(existing);
        }
        let running = self
            .spawn_container(broker_id, engine, users, user_config)
            .await?;
        self.containers
            .write()
            .insert(broker_id.to_string(), running.clone());
        Ok(running)
    }

    /// Reboot a broker: tear the container down and bring a fresh one up with
    /// the (post-pending) users + configuration, mirroring AWS applying
    /// staged changes on `RebootBroker`.
    pub async fn reboot_broker(
        &self,
        broker_id: &str,
        engine: MqEngine,
        users: &[BrokerUser],
        user_config: Option<&str>,
    ) -> Result<RunningBroker, RuntimeError> {
        self.stop_broker(broker_id).await;
        let running = self
            .spawn_container(broker_id, engine, users, user_config)
            .await?;
        self.containers
            .write()
            .insert(broker_id.to_string(), running.clone());
        Ok(running)
    }

    /// Re-attach to a broker's PERSISTED backing container after a fakecloud
    /// restart, preserving its message data (KahaDB / Mnesia), rather than
    /// spawning a fresh empty one. Returns `Ok(None)` when the container is
    /// truly gone (the caller then creates fresh), `Err` on a transient daemon
    /// failure (the caller retries), and `Ok(Some(..))` once it is running and
    /// its (possibly re-assigned) published host ports have been re-read.
    pub async fn reattach_broker(
        &self,
        broker_id: &str,
        engine: MqEngine,
        container_id: &str,
    ) -> Result<Option<RunningBroker>, RuntimeError> {
        // Does the container still exist? `inspect` exits non-zero (no such
        // object) when it is gone -- distinct from a daemon error, which we
        // can't observe separately here, so a gone-or-broken container falls to
        // a fresh create. A truly transient daemon outage surfaces on `start`.
        let inspect = tokio::process::Command::new(&self.cli)
            .args(["inspect", "--format", "{{.State.Status}}", container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !inspect.status.success() {
            return Ok(None);
        }
        // `start` is idempotent for an already-running container.
        let start = tokio::process::Command::new(&self.cli)
            .args(["start", container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !start.status.success() {
            let stderr = String::from_utf8_lossy(&start.stderr);
            // The container vanished between inspect and start -> create fresh.
            if stderr.contains("No such container") || stderr.contains("no such container") {
                return Ok(None);
            }
            return Err(RuntimeError::ContainerStartFailed(format!(
                "reattach start failed: {}",
                stderr.trim()
            )));
        }
        // Re-read the host->container port mapping: a restarted container with
        // ephemeral publishing may be assigned different host ports.
        let mut ports = BTreeMap::new();
        for (label, cport) in engine.published_ports() {
            let hp = self.lookup_port(container_id, *cport).await?;
            ports.insert((*label).to_string(), hp);
        }
        self.wait_for_broker_ready(engine, container_id, &ports)
            .await?;
        let running = RunningBroker {
            container_id: container_id.to_string(),
            host: self.net.sibling_host.clone(),
            ports,
        };
        self.containers
            .write()
            .insert(broker_id.to_string(), running.clone());
        tracing::info!(
            broker_id = %broker_id,
            container_id = %container_id,
            "re-attached persisted MQ broker container",
        );
        Ok(Some(running))
    }

    /// Stop + remove a broker's backing container and drop its tracking entry.
    pub async fn stop_broker(&self, broker_id: &str) {
        let running = self.containers.write().remove(broker_id);
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

    async fn spawn_container(
        &self,
        broker_id: &str,
        engine: MqEngine,
        users: &[BrokerUser],
        user_config: Option<&str>,
    ) -> Result<RunningBroker, RuntimeError> {
        let mut args: Vec<String> = vec!["create".to_string()];
        for (_, cport) in engine.published_ports() {
            args.push("-p".to_string());
            args.push(format!(":{cport}"));
        }
        args.push("--label".to_string());
        args.push(format!("fakecloud-mq={broker_id}"));
        args.push("--label".to_string());
        args.push(format!("fakecloud-instance={}", self.instance_id));
        self.net.push_add_host_args(&mut args);

        if engine == MqEngine::RabbitMq {
            // Pin a STABLE container hostname: RabbitMQ derives its Erlang node
            // name (rabbit@<hostname>) and its mnesia data directory from the
            // hostname. Docker otherwise assigns a fresh random hostname, so a
            // restarted/reattached container gets a different node name and its
            // mnesia dir no longer matches -- the node never finishes starting.
            // A hostname fixed at create time survives `docker start` (reattach)
            // and reboot, keeping the node name and data dir stable. (We set
            // only --hostname, not RABBITMQ_NODENAME, so RabbitMQ derives a
            // consistent short node name and we avoid the long-vs-short-name
            // resolution trap.)
            args.push("--hostname".to_string());
            args.push(rabbit_hostname(broker_id));
            // Cap the memory high-watermark at an absolute value so a
            // constrained CI runner (where the default 0.4 * detected-RAM can be
            // miscomputed under a cgroup memory limit) doesn't trip a memory
            // alarm that refuses connections.
            args.push("-e".to_string());
            args.push("RABBITMQ_VM_MEMORY_HIGH_WATERMARK_ABSOLUTE=384MiB".to_string());
            // Provision the create-time user via env (the env-created user is
            // tagged `administrator` with full `/` permissions), available the
            // instant the broker boots -- no post-start exec race.
            if let Some(first) = users.first() {
                args.push("-e".to_string());
                args.push(format!("RABBITMQ_DEFAULT_USER={}", first.username));
                args.push("-e".to_string());
                args.push(format!("RABBITMQ_DEFAULT_PASS={}", first.password));
            }
        }
        args.push(engine.image());

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

        // Stage engine configuration into the created-but-not-started
        // container via `docker cp` (never a bind mount) so it takes effect on
        // boot and works whether fakecloud runs on the host or in a container.
        if let Err(e) = self
            .stage_config(&container_id, engine, users, user_config)
            .await
        {
            self.remove_container(&container_id).await;
            return Err(e);
        }

        let start = tokio::process::Command::new(&self.cli)
            .args(["start", &container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !start.status.success() {
            self.remove_container(&container_id).await;
            return Err(RuntimeError::ContainerStartFailed(format!(
                "container start failed: {}",
                String::from_utf8_lossy(&start.stderr).trim()
            )));
        }

        let mut ports = BTreeMap::new();
        for (label, cport) in engine.published_ports() {
            match self.lookup_port(&container_id, *cport).await {
                Ok(hp) => {
                    ports.insert((*label).to_string(), hp);
                }
                Err(e) => {
                    self.remove_container(&container_id).await;
                    return Err(e);
                }
            }
        }

        if let Err(e) = self
            .wait_for_broker_ready(engine, &container_id, &ports)
            .await
        {
            self.remove_container(&container_id).await;
            return Err(e);
        }

        // RabbitMQ users beyond the create-time env user are applied live,
        // straight against the just-created container id (its tracking entry
        // does not exist yet).
        if engine == MqEngine::RabbitMq {
            for u in users.iter().skip(1) {
                let _ = self.rabbit_apply_user_to_container(&container_id, u).await;
            }
        }

        tracing::info!(
            broker_id = %broker_id,
            container_id = %container_id,
            engine = ?engine,
            "MQ broker container started",
        );

        Ok(RunningBroker {
            container_id,
            host: self.net.sibling_host.clone(),
            ports,
        })
    }

    /// `docker cp` the engine configuration into the container's conf dir.
    async fn stage_config(
        &self,
        container_id: &str,
        engine: MqEngine,
        users: &[BrokerUser],
        user_config: Option<&str>,
    ) -> Result<(), RuntimeError> {
        let (dest, contents) = match engine {
            MqEngine::ActiveMq => (
                "/opt/apache-activemq/conf/activemq.xml",
                activemq_config(users, user_config),
            ),
            MqEngine::RabbitMq => {
                // The RabbitMQ configuration revision data is already valid
                // `rabbitmq.conf` (key = value lines); apply it verbatim.
                let Some(conf) = user_config.filter(|c| !c.trim().is_empty()) else {
                    return Ok(());
                };
                ("/etc/rabbitmq/rabbitmq.conf", conf.to_string())
            }
        };
        self.cp_string_into(container_id, dest, &contents).await
    }

    /// Write `contents` to a temp file and `docker cp` it to `dest` inside the
    /// container.
    async fn cp_string_into(
        &self,
        container_id: &str,
        dest: &str,
        contents: &str,
    ) -> Result<(), RuntimeError> {
        let dir = tempfile::TempDir::new()
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        let file = dir.path().join("staged");
        let contents_owned = contents.to_string();
        let file_for_write = file.clone();
        tokio::task::spawn_blocking(move || std::fs::write(&file_for_write, contents_owned))
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        let cp = tokio::process::Command::new(&self.cli)
            .arg("cp")
            .arg(&file)
            .arg(format!("{container_id}:{dest}"))
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !cp.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "docker cp to {dest} failed: {}",
                String::from_utf8_lossy(&cp.stderr).trim()
            )));
        }
        Ok(())
    }

    /// Apply a single RabbitMQ user live via `rabbitmqctl`, resolving the
    /// broker's tracked container. Returns `Err(Unavailable)` when no live
    /// container is tracked for the broker (the caller retries -- e.g. a
    /// `CreateUser` that raced the broker still booting), and `Err` if any
    /// `rabbitmqctl` step fails, so the caller can retry until the user really
    /// exists on the broker (a user the API claims exists MUST be able to log
    /// in).
    pub async fn rabbit_apply_user(
        &self,
        broker_id: &str,
        user: &BrokerUser,
    ) -> Result<(), RuntimeError> {
        let container = self.resolve_container(broker_id)?;
        self.rabbit_apply_user_to_container(&container, user).await
    }

    /// The low-level `rabbitmqctl` apply against a known container id. Used both
    /// by the create-time extra-user loop (container id known, not yet tracked)
    /// and by `rabbit_apply_user` after resolving the tracked container.
    async fn rabbit_apply_user_to_container(
        &self,
        container_id: &str,
        user: &BrokerUser,
    ) -> Result<(), RuntimeError> {
        // add_user fails if the user already exists; fall back to
        // change_password so an UpdateUser is deterministic.
        let added = self
            .rabbitmqctl(container_id, &["add_user", &user.username, &user.password])
            .await;
        if !added
            && !self
                .rabbitmqctl(
                    container_id,
                    &["change_password", &user.username, &user.password],
                )
                .await
        {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "rabbitmqctl could not create or update user {}",
                user.username
            )));
        }
        let tag = if user.console_access {
            "administrator"
        } else {
            "management"
        };
        if !self
            .rabbitmqctl(container_id, &["set_user_tags", &user.username, tag])
            .await
        {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "rabbitmqctl set_user_tags failed for {}",
                user.username
            )));
        }
        if !self
            .rabbitmqctl(
                container_id,
                &[
                    "set_permissions",
                    "-p",
                    "/",
                    &user.username,
                    ".*",
                    ".*",
                    ".*",
                ],
            )
            .await
        {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "rabbitmqctl set_permissions failed for {}",
                user.username
            )));
        }
        Ok(())
    }

    /// Delete a RabbitMQ user live via `rabbitmqctl`. `Err(Unavailable)` when no
    /// live container is tracked (the caller retries best-effort).
    pub async fn rabbit_delete_user(
        &self,
        broker_id: &str,
        username: &str,
    ) -> Result<(), RuntimeError> {
        let container = self.resolve_container(broker_id)?;
        if !self
            .rabbitmqctl(&container, &["delete_user", username])
            .await
        {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "rabbitmqctl delete_user failed for {username}"
            )));
        }
        Ok(())
    }

    /// Resolve a broker id to its tracked container id, or `Err(Unavailable)`
    /// when no live container is tracked (never fabricates a bogus id from the
    /// broker id -- that would exec against a nonexistent container).
    fn resolve_container(&self, broker_id: &str) -> Result<String, RuntimeError> {
        self.containers
            .read()
            .get(broker_id)
            .map(|c| c.container_id.clone())
            .ok_or(RuntimeError::Unavailable)
    }

    async fn rabbitmqctl(&self, container_id: &str, ctl_args: &[&str]) -> bool {
        let mut args = vec!["exec", container_id, "rabbitmqctl"];
        args.extend_from_slice(ctl_args);
        tokio::process::Command::new(&self.cli)
            .args(&args)
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

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

    /// Block until the broker is PROTOCOL-ready, not merely TCP-open. A bare
    /// TCP connect races the broker's JVM/Erlang startup: the ServerSocket
    /// binds early but the broker resets connections until it is fully
    /// initialized (the "Connection reset by peer" the E2E hit). So:
    ///
    /// - **ActiveMQ**: poll the web console (Jetty on 8161), which starts LAST
    ///   in the ActiveMQ boot sequence -- once it answers HTTP, every transport
    ///   connector (OpenWire/AMQP/STOMP/MQTT/WS) is bound and accepting.
    /// - **RabbitMQ**: require BOTH `rabbitmq-diagnostics check_running` (the
    ///   broker application has finished booting) AND a real AMQP protocol
    ///   handshake on the mapped port (the AMQP listener actually accepts) --
    ///   the app can report running a beat before the listener accepts, and a
    ///   listener can accept before the app is ready.
    ///
    /// The window is generous (~180s): a constrained CI runner boots Erlang +
    /// the management plugin slowly. On timeout the container's recent
    /// `docker logs` and which check failed are logged so a CI failure is
    /// diagnosable rather than a black box.
    async fn wait_for_broker_ready(
        &self,
        engine: MqEngine,
        container_id: &str,
        ports: &BTreeMap<String, u16>,
    ) -> Result<(), RuntimeError> {
        // 360 * 500ms = ~180s (plus per-probe time).
        const ATTEMPTS: u32 = 360;
        let start = std::time::Instant::now();
        match engine {
            MqEngine::ActiveMq => {
                let console = ports.get("console").copied().ok_or_else(|| {
                    RuntimeError::ContainerStartFailed("console port was not published".to_string())
                })?;
                for _ in 0..ATTEMPTS {
                    if self.http_responds(console).await {
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                self.log_readiness_timeout(
                    container_id,
                    "ActiveMQ web console never answered HTTP",
                    start,
                )
                .await;
                Err(RuntimeError::ContainerStartFailed(
                    "ActiveMQ broker did not become protocol-ready within 180 seconds".to_string(),
                ))
            }
            MqEngine::RabbitMq => {
                let amqp = ports.get("amqp").copied().ok_or_else(|| {
                    RuntimeError::ContainerStartFailed("amqp port was not published".to_string())
                })?;
                let mut app_ok = false;
                for _ in 0..ATTEMPTS {
                    // `check_running` exits 0 once the RabbitMQ application has
                    // finished booting; no fragile flags (unlike await_startup,
                    // whose --timeout handling differs across CLI versions).
                    if !app_ok {
                        app_ok = self
                            .exec_ok(
                                container_id,
                                &["rabbitmq-diagnostics", "-q", "check_running"],
                            )
                            .await;
                    }
                    if app_ok && self.amqp_accepts(amqp).await {
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                let why = if app_ok {
                    "RabbitMQ app is running but the AMQP listener never accepted a handshake"
                } else {
                    "RabbitMQ application never reported running (check_running)"
                };
                self.log_readiness_timeout(container_id, why, start).await;
                Err(RuntimeError::ContainerStartFailed(
                    "RabbitMQ broker did not become protocol-ready within 180 seconds".to_string(),
                ))
            }
        }
    }

    /// True when an HTTP server answers on `host_port` (the response begins with
    /// an `HTTP/` status line -- a `401` from the auth-protected console counts).
    async fn http_responds(&self, host_port: u16) -> bool {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let host = &self.net.sibling_host;
        let Ok(mut stream) = tokio::net::TcpStream::connect(format!("{host}:{host_port}")).await
        else {
            return false;
        };
        if stream
            .write_all(b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
            .await
            .is_err()
        {
            return false;
        }
        let mut buf = [0u8; 16];
        match tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await {
            Ok(Ok(n)) if n >= 5 => buf.starts_with(b"HTTP/"),
            _ => false,
        }
    }

    /// True when the mapped AMQP port completes the AMQP 0-9-1 protocol header
    /// handshake: after we send the protocol header a live RabbitMQ listener
    /// replies with a `Connection.Start` method frame (type `0x01`), or its own
    /// protocol header on a version mismatch. A not-yet-ready listener resets or
    /// stays silent.
    async fn amqp_accepts(&self, host_port: u16) -> bool {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let host = &self.net.sibling_host;
        let Ok(mut stream) = tokio::net::TcpStream::connect(format!("{host}:{host_port}")).await
        else {
            return false;
        };
        if stream.write_all(b"AMQP\x00\x00\x09\x01").await.is_err() {
            return false;
        }
        let mut buf = [0u8; 8];
        match tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await {
            Ok(Ok(n)) if n >= 1 => buf[0] == 0x01 || buf.starts_with(b"AMQP"),
            _ => false,
        }
    }

    /// Run `docker exec <id> <argv...>` and report whether it exited 0.
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

    /// Log a readiness timeout with the failing check, elapsed time, and the
    /// container's recent `docker logs`, so a CI failure is diagnosable instead
    /// of a black box.
    async fn log_readiness_timeout(
        &self,
        container_id: &str,
        reason: &str,
        started: std::time::Instant,
    ) {
        let logs = tokio::process::Command::new(&self.cli)
            .args(["logs", "--tail", "60", container_id])
            .output()
            .await
            .map(|o| {
                let mut s = String::from_utf8_lossy(&o.stdout).into_owned();
                s.push_str(&String::from_utf8_lossy(&o.stderr));
                s
            })
            .unwrap_or_else(|e| format!("<docker logs failed: {e}>"));
        tracing::error!(
            container_id = %container_id,
            elapsed_secs = started.elapsed().as_secs(),
            reason = %reason,
            container_logs = %logs,
            "MQ broker readiness timed out",
        );
    }

    async fn remove_container(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }
}

/// A STABLE, valid DNS-label container hostname for a RabbitMQ broker.
///
/// RabbitMQ derives its Erlang node name (`rabbit@<hostname>`) from the
/// container hostname. If the hostname changes across restarts (Docker assigns
/// a random one by default), the node cannot find its old Mnesia data directory
/// (`/var/lib/rabbitmq/mnesia/rabbit@<hostname>`) and boot can stall or fail.
/// Pinning a deterministic hostname derived from the broker id keeps the node
/// name stable across reattach/reboot. The result is lowercased and reduced to
/// `[a-z0-9-]`, truncated to a safe length, and prefixed so it always starts
/// with a letter.
fn rabbit_hostname(broker_id: &str) -> String {
    let mut label = String::from("mq-");
    for c in broker_id.chars().flat_map(char::to_lowercase) {
        if c.is_ascii_alphanumeric() {
            label.push(c);
        } else if c == '-' || c == '_' {
            label.push('-');
        }
    }
    label.truncate(60);
    while label.ends_with('-') {
        label.pop();
    }
    label
}

/// XML-escape a value for an attribute.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Build the `activemq.xml` that configures an ActiveMQ container.
///
/// When `user_config` is already a complete standalone broker configuration
/// (it declares its own `<transportConnector>`), it is used verbatim -- the
/// user's uploaded config genuinely configures the live broker. Otherwise a
/// known-good config is generated with the standard transport connectors and
/// the broker's users injected as an enforced `simpleAuthenticationPlugin`
/// (every user is placed in the `mqusers` group, which a permissive
/// `authorizationPlugin` grants full access, so authenticated clients can
/// produce/consume without per-destination authorization surprises).
pub fn activemq_config(users: &[BrokerUser], user_config: Option<&str>) -> String {
    if let Some(cfg) = user_config {
        if cfg.contains("<transportConnector") {
            return cfg.to_string();
        }
    }
    let mut auth_users = String::new();
    for u in users {
        let mut groups: Vec<String> = vec!["mqusers".to_string()];
        for g in &u.groups {
            if !g.is_empty() && g != "mqusers" {
                groups.push(g.clone());
            }
        }
        if u.console_access && !groups.iter().any(|g| g == "admins") {
            groups.push("admins".to_string());
        }
        auth_users.push_str(&format!(
            "                    <authenticationUser username=\"{}\" password=\"{}\" groups=\"{}\"/>\n",
            xml_escape(&u.username),
            xml_escape(&u.password),
            xml_escape(&groups.join(","))
        ));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<beans
  xmlns="http://www.springframework.org/schema/beans"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
  http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">

    <broker xmlns="http://activemq.apache.org/schema/core" brokerName="localhost" dataDirectory="${{activemq.data}}" useJmx="true">

        <destinationPolicy>
            <policyMap>
              <policyEntries>
                <policyEntry topic="&gt;">
                    <pendingMessageLimitStrategy>
                      <constantPendingMessageLimitStrategy limit="1000"/>
                    </pendingMessageLimitStrategy>
                </policyEntry>
              </policyEntries>
            </policyMap>
        </destinationPolicy>

        <managementContext>
            <managementContext createConnector="false"/>
        </managementContext>

        <plugins>
            <simpleAuthenticationPlugin anonymousAccessAllowed="false">
                <users>
{auth_users}                </users>
            </simpleAuthenticationPlugin>
            <authorizationPlugin>
                <map>
                    <authorizationMap>
                        <authorizationEntries>
                            <authorizationEntry queue="&gt;" read="mqusers" write="mqusers" admin="mqusers"/>
                            <authorizationEntry topic="&gt;" read="mqusers" write="mqusers" admin="mqusers"/>
                            <authorizationEntry topic="ActiveMQ.Advisory.&gt;" read="mqusers" write="mqusers" admin="mqusers"/>
                        </authorizationEntries>
                        <tempDestinationAuthorizationEntry>
                            <tempDestinationAuthorizationEntry read="mqusers" write="mqusers" admin="mqusers"/>
                        </tempDestinationAuthorizationEntry>
                    </authorizationMap>
                </map>
            </authorizationPlugin>
        </plugins>

        <persistenceAdapter>
            <kahaDB directory="${{activemq.data}}/kahadb"/>
        </persistenceAdapter>

        <transportConnectors>
            <transportConnector name="openwire" uri="tcp://0.0.0.0:61616"/>
            <transportConnector name="amqp" uri="amqp://0.0.0.0:5672"/>
            <transportConnector name="stomp" uri="stomp://0.0.0.0:61613"/>
            <transportConnector name="mqtt" uri="mqtt://0.0.0.0:1883"/>
            <transportConnector name="ws" uri="ws://0.0.0.0:61614"/>
        </transportConnectors>

        <shutdownHooks>
            <bean xmlns="http://www.springframework.org/schema/beans" class="org.apache.activemq.hooks.SpringContextHook" />
        </shutdownHooks>

    </broker>
</beans>
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(name: &str, pw: &str, console: bool) -> BrokerUser {
        BrokerUser {
            username: name.to_string(),
            password: pw.to_string(),
            groups: vec![],
            console_access: console,
        }
    }

    #[test]
    fn engine_from_wire_is_case_insensitive() {
        assert_eq!(MqEngine::from_wire("RabbitMQ"), MqEngine::RabbitMq);
        assert_eq!(MqEngine::from_wire("rabbitmq"), MqEngine::RabbitMq);
        assert_eq!(MqEngine::from_wire("ACTIVEMQ"), MqEngine::ActiveMq);
        assert_eq!(MqEngine::from_wire("anything"), MqEngine::ActiveMq);
    }

    #[test]
    fn activemq_config_injects_users_and_connectors() {
        let xml = activemq_config(&[user("app", "s3cr3t", true)], None);
        assert!(xml.contains("simpleAuthenticationPlugin anonymousAccessAllowed=\"false\""));
        assert!(xml.contains("username=\"app\""));
        assert!(xml.contains("password=\"s3cr3t\""));
        // Every user is in mqusers (granted full access) plus admins for
        // console access.
        assert!(xml.contains("groups=\"mqusers,admins\""));
        // Transport connectors on all published protocols must be present.
        assert!(xml.contains("tcp://0.0.0.0:61616"));
        assert!(xml.contains("stomp://0.0.0.0:61613"));
        assert!(xml.contains("amqp://0.0.0.0:5672"));
    }

    #[test]
    fn activemq_config_escapes_xml_special_chars() {
        let xml = activemq_config(&[user("a<b", "p&\"w", false)], None);
        assert!(xml.contains("username=\"a&lt;b\""));
        assert!(xml.contains("password=\"p&amp;&quot;w\""));
        assert!(!xml.contains("password=\"p&\""));
    }

    #[test]
    fn activemq_config_uses_full_user_config_verbatim() {
        // A complete standalone config (declares its own transportConnector)
        // is applied verbatim -- the uploaded config configures the broker.
        let user_cfg = "<broker><transportConnectors><transportConnector uri=\"tcp://0.0.0.0:61616\"/></transportConnectors></broker>";
        let xml = activemq_config(&[user("app", "pw", true)], Some(user_cfg));
        assert_eq!(xml, user_cfg);
    }

    #[test]
    fn activemq_config_generates_when_config_is_placeholder() {
        // The auto-generated default (no transportConnector) falls through to
        // the generated auth config.
        let placeholder = "<broker start=\"false\"></broker>";
        let xml = activemq_config(&[user("app", "pw", true)], Some(placeholder));
        assert!(xml.contains("simpleAuthenticationPlugin"));
        assert!(xml.contains("tcp://0.0.0.0:61616"));
    }

    #[test]
    fn published_ports_cover_all_activemq_protocols() {
        let labels: Vec<&str> = MqEngine::ActiveMq
            .published_ports()
            .iter()
            .map(|(l, _)| *l)
            .collect();
        for expected in ["openwire", "amqp", "stomp", "mqtt", "ws", "console"] {
            assert!(labels.contains(&expected), "missing {expected}");
        }
        // ActiveMQ readiness is gated on the web console; it must be published.
        assert!(labels.contains(&"console"));
        let rabbit: Vec<&str> = MqEngine::RabbitMq
            .published_ports()
            .iter()
            .map(|(l, _)| *l)
            .collect();
        assert!(rabbit.contains(&"amqp"));
    }
}
