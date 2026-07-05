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
//! - `RABBITMQ` -> `rabbitmq:3-management`, publishing AMQP + the management
//!   console. The create-time user is provisioned via
//!   `RABBITMQ_DEFAULT_USER`/`_PASS`; further `CreateUser`/`UpdateUser`/
//!   `DeleteUser` take effect immediately via `rabbitmqctl` (matching AWS,
//!   which applies RabbitMQ user changes without a reboot).
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
    fn image(self) -> &'static str {
        match self {
            MqEngine::ActiveMq => "apache/activemq-classic",
            MqEngine::RabbitMq => "rabbitmq:3-management",
        }
    }

    /// The `(label, container_port)` pairs published to ephemeral host ports.
    /// The label is what `BrokerDataPlane::ports` is keyed by and what the
    /// describe endpoints project from.
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
            MqEngine::RabbitMq => &[("amqp", 5672), ("console", 15672)],
        }
    }

    /// The label whose mapped port readiness is probed before the broker is
    /// reported `RUNNING` (the primary wire protocol).
    fn ready_label(self) -> &'static str {
        match self {
            MqEngine::ActiveMq => "openwire",
            MqEngine::RabbitMq => "amqp",
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
    /// CLI is available (fakecloud then degrades to control-plane-only MQ).
    pub fn new() -> Option<Self> {
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

    /// Address fakecloud advertises for clients to reach a spawned broker and
    /// uses for readiness probes.
    pub fn endpoint_host(&self) -> &str {
        &self.net.sibling_host
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

        // RabbitMQ provisions its create-time user via env (the env-created
        // user is tagged `administrator` with full `/` permissions), which is
        // available the instant the broker boots -- no post-start exec race.
        if engine == MqEngine::RabbitMq {
            if let Some(first) = users.first() {
                args.push("-e".to_string());
                args.push(format!("RABBITMQ_DEFAULT_USER={}", first.username));
                args.push("-e".to_string());
                args.push(format!("RABBITMQ_DEFAULT_PASS={}", first.password));
            }
        }
        args.push(engine.image().to_string());

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

        let ready_port = ports.get(engine.ready_label()).copied().ok_or_else(|| {
            RuntimeError::ContainerStartFailed("primary wire port was not published".to_string())
        })?;
        if let Err(e) = self.wait_for_ready(ready_port).await {
            self.remove_container(&container_id).await;
            return Err(e);
        }

        // RabbitMQ users beyond the create-time env user are applied live.
        if engine == MqEngine::RabbitMq {
            for u in users.iter().skip(1) {
                let _ = self.rabbit_apply_user(&container_id, u).await;
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

    /// Apply a single RabbitMQ user live via `rabbitmqctl` (add-or-update,
    /// permissions, and tags). Used for the extra create-time users and for
    /// the immediate `CreateUser`/`UpdateUser` path.
    pub async fn rabbit_apply_user(
        &self,
        broker_id_or_container: &str,
        user: &BrokerUser,
    ) -> Result<(), RuntimeError> {
        let container = self.resolve_container(broker_id_or_container)?;
        // add_user fails if the user exists; fall back to change_password.
        let added = self
            .rabbitmqctl(&container, &["add_user", &user.username, &user.password])
            .await;
        if !added {
            let _ = self
                .rabbitmqctl(
                    &container,
                    &["change_password", &user.username, &user.password],
                )
                .await;
        }
        let tag = if user.console_access {
            "administrator"
        } else {
            "management"
        };
        let _ = self
            .rabbitmqctl(&container, &["set_user_tags", &user.username, tag])
            .await;
        let _ = self
            .rabbitmqctl(
                &container,
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
            .await;
        Ok(())
    }

    /// Delete a RabbitMQ user live via `rabbitmqctl`.
    pub async fn rabbit_delete_user(
        &self,
        broker_id: &str,
        username: &str,
    ) -> Result<(), RuntimeError> {
        let container = self.resolve_container(broker_id)?;
        let _ = self
            .rabbitmqctl(&container, &["delete_user", username])
            .await;
        Ok(())
    }

    /// True if the broker currently has a tracked live container.
    pub fn is_running(&self, broker_id: &str) -> bool {
        self.containers.read().contains_key(broker_id)
    }

    fn resolve_container(&self, broker_id_or_container: &str) -> Result<String, RuntimeError> {
        if let Some(c) = self.containers.read().get(broker_id_or_container) {
            return Ok(c.container_id.clone());
        }
        // Allow passing a container id directly (create path, before the
        // tracking entry exists).
        Ok(broker_id_or_container.to_string())
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

    async fn wait_for_ready(&self, host_port: u16) -> Result<(), RuntimeError> {
        let host = &self.net.sibling_host;
        // Brokers (JVM ActiveMQ, Erlang RabbitMQ) take longer to boot than a
        // cache, so allow up to ~60s.
        for _ in 0..120 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if tokio::net::TcpStream::connect(format!("{host}:{host_port}"))
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
        Err(RuntimeError::ContainerStartFailed(
            "broker container did not become ready within 60 seconds".to_string(),
        ))
    }

    async fn remove_container(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }
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
            <transportConnector name="openwire" uri="tcp://0.0.0.0:61616?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
            <transportConnector name="amqp" uri="amqp://0.0.0.0:5672?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
            <transportConnector name="stomp" uri="stomp://0.0.0.0:61613?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
            <transportConnector name="mqtt" uri="mqtt://0.0.0.0:1883?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
            <transportConnector name="ws" uri="ws://0.0.0.0:61614?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
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
        assert_eq!(MqEngine::ActiveMq.ready_label(), "openwire");
        assert_eq!(MqEngine::RabbitMq.ready_label(), "amqp");
    }
}
