use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio_postgres::NoTls;

mod k8s;

const POSTGRES_DOCKERFILE: &str = include_str!("../../assets/postgres/Dockerfile");
const AWS_COMMONS_CONTROL: &str = include_str!("../../assets/postgres/aws_commons.control");
const AWS_COMMONS_SQL: &str = include_str!("../../assets/postgres/aws_commons--1.1.sql");
const AWS_COMMONS_UPGRADE_SQL: &str =
    include_str!("../../assets/postgres/aws_commons--1.0--1.1.sql");
const AWS_LAMBDA_CONTROL: &str = include_str!("../../assets/postgres/aws_lambda.control");
const AWS_LAMBDA_SQL: &str = include_str!("../../assets/postgres/aws_lambda--1.0.sql");
const AWS_S3_CONTROL: &str = include_str!("../../assets/postgres/aws_s3.control");
const AWS_S3_SQL: &str = include_str!("../../assets/postgres/aws_s3--1.0.sql");

const MYSQL_DOCKERFILE: &str = include_str!("../../assets/mysql/Dockerfile");
const MYSQL_UDF_C: &str = include_str!("../../assets/mysql/fakecloud_udf.c");
const MYSQL_BOOTSTRAP_SH: &str = include_str!("../../assets/mysql/fakecloud-bootstrap.sh");
const MYSQL_BOOTSTRAP_SQL: &str =
    include_str!("../../assets/mysql/99-fakecloud-bootstrap.sql.tmpl");

const MARIADB_DOCKERFILE: &str = include_str!("../../assets/mariadb/Dockerfile");
const MARIADB_UDF_C: &str = include_str!("../../assets/mariadb/fakecloud_udf.c");
const MARIADB_BOOTSTRAP_SH: &str = include_str!("../../assets/mariadb/fakecloud-bootstrap.sh");
const MARIADB_BOOTSTRAP_SQL: &str =
    include_str!("../../assets/mariadb/99-fakecloud-bootstrap.sql.tmpl");

/// Default registry that hosts the prebuilt postgres images. CI publishes
/// to `ghcr.io/faiscadev/fakecloud-postgres:<major>-<version>` on each
/// release tag (see `.github/workflows/docker-rds-images.yml`).
/// Override with the `FAKECLOUD_POSTGRES_REGISTRY` env var (e.g. for
/// private mirrors); set `FAKECLOUD_REBUILD_POSTGRES_IMAGE=1` to force
/// a local rebuild even when the published tag is reachable.
const DEFAULT_POSTGRES_REGISTRY: &str = "ghcr.io/faiscadev";

#[derive(Debug, Clone)]
pub struct RunningDbContainer {
    /// Backend-specific handle: a Docker container id, or a Pod name.
    pub container_id: String,
    /// Host-published port (Docker) or the engine's in-Pod port (k8s).
    pub host_port: u16,
    /// Address clients (and fakecloud's own readiness/restore) connect to:
    /// `127.0.0.1` for Docker, the Pod IP for k8s.
    pub endpoint_address: String,
    /// Port clients connect to: the published host port for Docker, the
    /// engine's standard port for k8s.
    pub endpoint_port: u16,
}

pub struct RdsRuntime {
    cli: String,
    containers: RwLock<HashMap<String, RunningDbContainer>>,
    instance_id: String,
    host_ip: String,
    server_port: u16,
    image_cache: RwLock<HashMap<String, Arc<tokio::sync::Mutex<bool>>>>,
    /// `Some` when running on the Kubernetes backend; the public methods
    /// dispatch to it and the Docker fields above go unused. `None` is the
    /// default Docker/Podman backend.
    k8s: Option<k8s::K8sDb>,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("container runtime is unavailable")]
    Unavailable,
    #[error("container failed to start: {0}")]
    ContainerStartFailed(String),
}

/// Error initializing the Kubernetes backend at startup. Surfaced to the
/// operator so a misconfigured cluster fails fast rather than silently
/// falling back to Docker.
#[derive(Debug, thiserror::Error)]
pub enum BackendInitError {
    #[error(transparent)]
    Env(#[from] fakecloud_k8s::K8sEnvError),
    #[error("failed to connect to the Kubernetes cluster: {0}")]
    Connect(String),
}

impl RdsRuntime {
    pub fn new(server_port: u16) -> Option<Self> {
        let cli = if let Ok(cli) = std::env::var("FAKECLOUD_CONTAINER_CLI") {
            if cli_available(&cli) {
                cli
            } else {
                return None;
            }
        } else if cli_available("docker") {
            "docker".to_string()
        } else if cli_available("podman") {
            "podman".to_string()
        } else {
            return None;
        };

        // Match Lambda runtime container-to-host networking: Linux uses the
        // bridge gateway IP directly, macOS/Windows use Docker Desktop's
        // host-gateway alias. Containers reach fakecloud at host.docker.internal.
        let host_ip = if cfg!(target_os = "linux") {
            detect_bridge_gateway(&cli).unwrap_or_else(|| "172.17.0.1".to_string())
        } else {
            "host-gateway".to_string()
        };

        Some(Self {
            cli,
            containers: RwLock::new(HashMap::new()),
            instance_id: format!("fakecloud-{}", std::process::id()),
            host_ip,
            server_port,
            image_cache: RwLock::new(HashMap::new()),
            k8s: None,
        })
    }

    /// Construct the Kubernetes backend. `server_port` is fakecloud's
    /// bound port (used when `FAKECLOUD_K8S_SELF_URL` omits one). Fails
    /// fast on misconfiguration — never silently degrades to Docker.
    pub async fn new_k8s(server_port: u16) -> Result<Self, BackendInitError> {
        let db = k8s::K8sDb::from_env(server_port).await?;
        Ok(Self {
            cli: String::new(),
            containers: RwLock::new(HashMap::new()),
            instance_id: format!("fakecloud-{}", std::process::id()),
            host_ip: String::new(),
            server_port,
            image_cache: RwLock::new(HashMap::new()),
            k8s: Some(db),
        })
    }

    pub fn cli_name(&self) -> &str {
        if self.k8s.is_some() {
            "kubernetes"
        } else {
            &self.cli
        }
    }

    /// Sweep DB Pods orphaned by a previous process (k8s only; no-op on
    /// the Docker backend, which the shared container reaper handles).
    pub async fn reap_stale(&self) {
        if let Some(k) = &self.k8s {
            k.reap_stale().await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn ensure_postgres(
        &self,
        db_instance_identifier: &str,
        engine: &str,
        engine_version: &str,
        username: &str,
        password: &str,
        db_name: &str,
        account_id: &str,
        region: &str,
    ) -> Result<RunningDbContainer, RuntimeError> {
        if let Some(k) = &self.k8s {
            let running = k
                .ensure(
                    db_instance_identifier,
                    engine,
                    engine_version,
                    username,
                    password,
                    db_name,
                    account_id,
                    region,
                )
                .await?;
            self.containers
                .write()
                .insert(db_instance_identifier.to_string(), running.clone());
            return Ok(running);
        }
        self.stop_container(db_instance_identifier).await;

        // Determine Docker image and port based on engine. Postgres,
        // MySQL, and MariaDB all use prebuilt fakecloud-* images that
        // bake in the bridge UDFs / extensions and call back into the
        // host fakecloud server; the heavier engines (oracle/mssql/db2)
        // stay on upstream images. `bridge_engine_version` is `Some(_)`
        // for the bridge-aware engines and gates the `--add-host`
        // setup below.
        let (image, port, env_vars, bridge_engine_version) = match engine {
            "postgres" => {
                let major_version = engine_version.split('.').next().unwrap_or("16");
                let image = self.ensure_postgres_image(major_version).await?;
                let env_vars = vec![
                    format!("POSTGRES_USER={username}"),
                    format!("POSTGRES_PASSWORD={password}"),
                    format!("POSTGRES_DB={db_name}"),
                    format!(
                        "FAKECLOUD_ENDPOINT=http://host.docker.internal:{}",
                        self.server_port
                    ),
                    format!("FAKECLOUD_ACCOUNT_ID={account_id}"),
                    format!("FAKECLOUD_REGION={region}"),
                ];
                (image, "5432", env_vars, Some(major_version.to_string()))
            }
            "mysql" => {
                // 5.7 was dropped after Oracle community support ended
                // (Oct 2023) — the image base no longer ships the build
                // deps we need for the UDF. Any 5.7.* engine version
                // resolves to 8.0.
                let _ = engine_version;
                let major_version = "8.0";
                let image = self.ensure_mysql_image(major_version).await?;
                let env_vars = vec![
                    format!("MYSQL_ROOT_PASSWORD={password}"),
                    format!("MYSQL_USER={username}"),
                    format!("MYSQL_PASSWORD={password}"),
                    format!("MYSQL_DATABASE={db_name}"),
                    format!(
                        "FAKECLOUD_ENDPOINT=http://host.docker.internal:{}",
                        self.server_port
                    ),
                    format!("FAKECLOUD_ACCOUNT_ID={account_id}"),
                    format!("FAKECLOUD_REGION={region}"),
                ];
                (image, "3306", env_vars, Some(major_version.to_string()))
            }
            "mariadb" => {
                let major_version = if engine_version.starts_with("10.11") {
                    "10.11"
                } else if engine_version.starts_with("11.4") {
                    "11.4"
                } else {
                    "10.6"
                };
                let image = self.ensure_mariadb_image(major_version).await?;
                let env_vars = vec![
                    format!("MARIADB_ROOT_PASSWORD={password}"),
                    format!("MARIADB_USER={username}"),
                    format!("MARIADB_PASSWORD={password}"),
                    format!("MARIADB_DATABASE={db_name}"),
                    format!(
                        "FAKECLOUD_ENDPOINT=http://host.docker.internal:{}",
                        self.server_port
                    ),
                    format!("FAKECLOUD_ACCOUNT_ID={account_id}"),
                    format!("FAKECLOUD_REGION={region}"),
                ];
                (image, "3306", env_vars, Some(major_version.to_string()))
            }
            "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => {
                // Oracle Database Free is the no-cost dev edition shipped by
                // Oracle. The container exposes a "FREEPDB1" pluggable
                // database and creates the SYSTEM user with the password
                // from ORACLE_PASSWORD.
                let image = "gvenzl/oracle-free:23-slim".to_string();
                let env_vars = vec![
                    format!("ORACLE_PASSWORD={password}"),
                    format!("APP_USER={username}"),
                    format!("APP_USER_PASSWORD={password}"),
                    format!("ORACLE_DATABASE={db_name}"),
                ];
                (image, "1521", env_vars, None)
            }
            "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => {
                // SQL Server Express is free for dev/test with no license
                // ceiling. SA password must satisfy MSSQL's complexity
                // requirements (>=8 chars, mix of classes); callers should
                // supply one that does or the container will refuse to
                // start.
                let image = "mcr.microsoft.com/mssql/server:2022-latest".to_string();
                let env_vars = vec![
                    "ACCEPT_EULA=Y".to_string(),
                    format!("MSSQL_SA_PASSWORD={password}"),
                    "MSSQL_PID=Express".to_string(),
                ];
                (image, "1433", env_vars, None)
            }
            "db2-se" | "db2-ae" => {
                // Db2 Community Edition is free under the standard IBM
                // Community License. The container exposes a single
                // database named after DBNAME, owned by db2inst1.
                let image = "icr.io/db2_community/db2:latest".to_string();
                let env_vars = vec![
                    "LICENSE=accept".to_string(),
                    "DB2INSTANCE=db2inst1".to_string(),
                    format!("DB2INST1_PASSWORD={password}"),
                    format!("DBNAME={db_name}"),
                ];
                (image, "50000", env_vars, None)
            }
            _ => {
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "Unsupported engine: {}",
                    engine
                )))
            }
        };

        // Db2 needs --privileged to set kernel parameters during startup.
        let needs_privileged = matches!(engine, "db2-se" | "db2-ae");

        // Build container create args
        let mut args = vec![
            "create".to_string(),
            "-p".to_string(),
            format!(":{}", port),
            "--label".to_string(),
            format!("fakecloud-rds={db_instance_identifier}"),
            "--label".to_string(),
            format!("fakecloud-instance={}", self.instance_id),
        ];

        if needs_privileged {
            args.push("--privileged".to_string());
        }

        // Bridge-aware engines (postgres aws_lambda, mysql/mariadb
        // fakecloud_post UDF) call back into fakecloud over HTTP. Wire
        // the host gateway alias so the in-container code can resolve
        // host.docker.internal on every platform.
        if bridge_engine_version.is_some() {
            args.push("--add-host".to_string());
            args.push(format!("host.docker.internal:{}", self.host_ip));
        }

        for env_var in env_vars {
            args.push("-e".to_string());
            args.push(env_var);
        }

        args.push(image);

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
        let start_result = tokio::process::Command::new(&self.cli)
            .args(["start", &container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !start_result.status.success() {
            self.remove_container(&container_id).await;
            return Err(RuntimeError::ContainerStartFailed(format!(
                "container start failed: {}",
                String::from_utf8_lossy(&start_result.stderr).trim()
            )));
        }

        let host_port = match self.lookup_port(&container_id, port).await {
            Ok(host_port) => host_port,
            Err(error) => {
                self.remove_container(&container_id).await;
                return Err(error);
            }
        };

        // Wait for database to be ready
        let wait_result = match engine {
            "postgres" => {
                self.wait_for_postgres(username, password, db_name, host_port)
                    .await
            }
            "mysql" | "mariadb" => {
                self.wait_for_mysql(username, password, db_name, host_port)
                    .await
            }
            "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => {
                self.wait_for_oracle(&container_id, host_port).await
            }
            "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => {
                self.wait_for_sqlserver(&container_id, host_port).await
            }
            "db2-se" | "db2-ae" => self.wait_for_db2(&container_id, host_port).await,
            _ => unreachable!("engine already validated"),
        };

        if let Err(error) = wait_result {
            self.remove_container(&container_id).await;
            return Err(error);
        }

        let running = RunningDbContainer {
            container_id,
            host_port,
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: host_port,
        };
        self.containers
            .write()
            .insert(db_instance_identifier.to_string(), running.clone());
        Ok(running)
    }

    pub async fn stop_container(&self, db_instance_identifier: &str) {
        let container = self.containers.write().remove(db_instance_identifier);
        if let Some(container) = container {
            if let Some(k) = &self.k8s {
                k.delete_pod(&container.container_id).await;
            } else {
                self.remove_container(&container.container_id).await;
            }
        }
    }

    pub async fn restart_container(
        &self,
        db_instance_identifier: &str,
        engine: &str,
        username: &str,
        password: &str,
        db_name: &str,
    ) -> Result<RunningDbContainer, RuntimeError> {
        if let Some(k) = &self.k8s {
            let running = k
                .restart(db_instance_identifier, engine, username, password, db_name)
                .await?;
            self.containers
                .write()
                .insert(db_instance_identifier.to_string(), running.clone());
            return Ok(running);
        }
        let running = self
            .containers
            .read()
            .get(db_instance_identifier)
            .cloned()
            .ok_or(RuntimeError::Unavailable)?;

        let output = tokio::process::Command::new(&self.cli)
            .args(["restart", &running.container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "container restart failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        let port = match engine {
            "postgres" => "5432",
            "mysql" | "mariadb" => "3306",
            "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => "1521",
            "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => "1433",
            "db2-se" | "db2-ae" => "50000",
            _ => "5432", // fallback
        };

        let host_port = self.lookup_port(&running.container_id, port).await?;

        match engine {
            "postgres" => {
                self.wait_for_postgres(username, password, db_name, host_port)
                    .await?
            }
            "mysql" | "mariadb" => {
                self.wait_for_mysql(username, password, db_name, host_port)
                    .await?
            }
            "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => {
                self.wait_for_oracle(&running.container_id, host_port)
                    .await?
            }
            "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => {
                self.wait_for_sqlserver(&running.container_id, host_port)
                    .await?
            }
            "db2-se" | "db2-ae" => self.wait_for_db2(&running.container_id, host_port).await?,
            _ => {
                self.wait_for_postgres(username, password, db_name, host_port)
                    .await?
            }
        };
        let running = RunningDbContainer {
            container_id: running.container_id,
            host_port,
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: host_port,
        };
        self.containers
            .write()
            .insert(db_instance_identifier.to_string(), running.clone());
        Ok(running)
    }

    pub async fn stop_all(&self) {
        let containers: Vec<String> = {
            let mut containers = self.containers.write();
            containers
                .drain()
                .map(|(_, container)| container.container_id)
                .collect()
        };
        for container_id in containers {
            self.remove_container(&container_id).await;
        }
    }

    async fn lookup_port(&self, container_id: &str, port: &str) -> Result<u16, RuntimeError> {
        let port_output = tokio::process::Command::new(&self.cli)
            .args(["port", container_id, port])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        let port_str = String::from_utf8_lossy(&port_output.stdout);
        port_str
            .trim()
            .rsplit(':')
            .next()
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| {
                RuntimeError::ContainerStartFailed(format!(
                    "could not determine container port from '{}'",
                    port_str.trim()
                ))
            })
    }

    async fn wait_for_postgres(
        &self,
        username: &str,
        password: &str,
        db_name: &str,
        host_port: u16,
    ) -> Result<(), RuntimeError> {
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let connection_string = format!(
                "host=127.0.0.1 port={host_port} user={username} password={password} dbname={db_name}"
            );
            if let Ok((client, connection)) =
                tokio_postgres::connect(&connection_string, NoTls).await
            {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                if client.simple_query("SELECT 1").await.is_ok() {
                    return Ok(());
                }
            }
        }

        Err(RuntimeError::ContainerStartFailed(
            "postgres container did not become ready within 20 seconds".to_string(),
        ))
    }

    async fn wait_for_mysql(
        &self,
        username: &str,
        password: &str,
        db_name: &str,
        host_port: u16,
    ) -> Result<(), RuntimeError> {
        use mysql_async::prelude::*;
        use mysql_async::OptsBuilder;

        for attempt in 1..=40 {
            let opts = OptsBuilder::default()
                .ip_or_hostname("127.0.0.1")
                .tcp_port(host_port)
                .user(Some(username))
                .pass(Some(password))
                .db_name(Some(db_name));

            match mysql_async::Conn::new(opts).await {
                Ok(mut conn) => {
                    if conn.query_drop("SELECT 1").await.is_ok() {
                        let _ = conn.disconnect().await;
                        return Ok(());
                    }
                }
                Err(_) => {
                    if attempt < 40 {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    continue;
                }
            }
        }

        Err(RuntimeError::ContainerStartFailed(
            "MySQL/MariaDB container did not become ready within 20 seconds".to_string(),
        ))
    }

    /// Wait for Oracle Database Free to finish bootstrapping. The
    /// `gvenzl/oracle-free` image prints `DATABASE IS READY TO USE!`
    /// to stdout once the listener accepts connections, so we poll
    /// `docker logs` until that marker appears (or the deadline elapses).
    /// Oracle XE/Free typically takes 30-90 seconds on first start.
    async fn wait_for_oracle(
        &self,
        container_id: &str,
        host_port: u16,
    ) -> Result<(), RuntimeError> {
        self.wait_for_log_marker(container_id, "DATABASE IS READY TO USE!", 240)
            .await?;
        self.wait_for_tcp(host_port, 30).await
    }

    /// Wait for SQL Server to be ready. The official mssql/server image
    /// emits `SQL Server is now ready for client connections.` once it
    /// accepts TCP connections on 1433.
    async fn wait_for_sqlserver(
        &self,
        container_id: &str,
        host_port: u16,
    ) -> Result<(), RuntimeError> {
        self.wait_for_log_marker(
            container_id,
            "SQL Server is now ready for client connections",
            180,
        )
        .await?;
        self.wait_for_tcp(host_port, 30).await
    }

    /// Wait for Db2 Community Edition to finish setup. The
    /// `icr.io/db2_community/db2` image prints `Setup has completed.`
    /// once the instance is up and the database has been created.
    async fn wait_for_db2(&self, container_id: &str, host_port: u16) -> Result<(), RuntimeError> {
        self.wait_for_log_marker(container_id, "Setup has completed", 360)
            .await?;
        self.wait_for_tcp(host_port, 60).await
    }

    /// Poll `docker logs <container>` until the supplied marker appears
    /// in stdout or stderr. `deadline_secs` caps total wait.
    async fn wait_for_log_marker(
        &self,
        container_id: &str,
        marker: &str,
        deadline_secs: u64,
    ) -> Result<(), RuntimeError> {
        let deadline = std::time::Instant::now() + Duration::from_secs(deadline_secs);
        while std::time::Instant::now() < deadline {
            let output = tokio::process::Command::new(&self.cli)
                .args(["logs", container_id])
                .output()
                .await
                .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stdout.contains(marker) || stderr.contains(marker) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        Err(RuntimeError::ContainerStartFailed(format!(
            "container did not log '{}' within {} seconds",
            marker, deadline_secs
        )))
    }

    /// TCP-probe the host port until it accepts a connection or the
    /// deadline elapses. Use after `wait_for_log_marker` since the
    /// listener may bind a moment after the readiness log line.
    async fn wait_for_tcp(&self, host_port: u16, deadline_secs: u64) -> Result<(), RuntimeError> {
        let deadline = std::time::Instant::now() + Duration::from_secs(deadline_secs);
        while std::time::Instant::now() < deadline {
            if tokio::net::TcpStream::connect(("127.0.0.1", host_port))
                .await
                .is_ok()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(RuntimeError::ContainerStartFailed(format!(
            "TCP probe to 127.0.0.1:{} did not succeed within {}s",
            host_port, deadline_secs
        )))
    }

    async fn remove_container(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }

    /// Build (or reuse) the fakecloud-postgres image for a given major
    /// version. The image bakes plpython3u plus the aws_commons and
    /// aws_lambda extension files so users can run
    /// `CREATE EXTENSION aws_lambda CASCADE` inside any database.
    /// Tag includes a content hash so changes to the embedded assets
    /// invalidate the local cache automatically.
    /// Resolve the postgres image tag for a given major version. Tries
    /// (in order): in-process cache, `docker image inspect` for a copy
    /// already on the daemon, `docker pull` of the prebuilt image
    /// published by CI, and finally a local `docker build` from the
    /// embedded Dockerfile + extension assets. The pull path is the
    /// happy path for end users on tagged releases; the build path
    /// covers dev / unreleased versions / airgapped setups.
    ///
    /// Honors:
    /// - `FAKECLOUD_POSTGRES_REGISTRY` — registry prefix (default
    ///   `ghcr.io/faiscadev`); useful for private mirrors.
    /// - `FAKECLOUD_REBUILD_POSTGRES_IMAGE` — when set to a non-empty
    ///   value, skip inspect + pull and force a fresh local build.
    ///   Use after editing the embedded Dockerfile or extension SQL.
    pub(crate) async fn ensure_postgres_image(
        &self,
        major_version: &str,
    ) -> Result<String, RuntimeError> {
        let tag = bridge_image_tag("fakecloud-postgres", major_version);
        self.ensure_bridge_image(&tag, |tag| async move {
            self.build_postgres_image_local(major_version, &tag).await
        })
        .await
    }

    async fn docker_image_exists(&self, tag: &str) -> bool {
        tokio::process::Command::new(&self.cli)
            .args(["image", "inspect", tag])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .map(|status| status.success())
            .unwrap_or(false)
    }

    async fn try_pull_image(&self, tag: &str) -> bool {
        tracing::info!(tag = %tag, "Pulling prebuilt fakecloud-postgres image");
        let output = match tokio::process::Command::new(&self.cli)
            .args(["pull", tag])
            .output()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                tracing::debug!(tag = %tag, error = %e, "docker pull failed to spawn");
                return false;
            }
        };
        if output.status.success() {
            return true;
        }
        tracing::info!(
            tag = %tag,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "Prebuilt postgres image not available, falling back to local build"
        );
        false
    }

    async fn build_postgres_image_local(
        &self,
        major_version: &str,
        tag: &str,
    ) -> Result<(), RuntimeError> {
        let assets: [(&str, &str); 8] = [
            ("Dockerfile", POSTGRES_DOCKERFILE),
            ("aws_commons.control", AWS_COMMONS_CONTROL),
            ("aws_commons--1.1.sql", AWS_COMMONS_SQL),
            ("aws_commons--1.0--1.1.sql", AWS_COMMONS_UPGRADE_SQL),
            ("aws_lambda.control", AWS_LAMBDA_CONTROL),
            ("aws_lambda--1.0.sql", AWS_LAMBDA_SQL),
            ("aws_s3.control", AWS_S3_CONTROL),
            ("aws_s3--1.0.sql", AWS_S3_SQL),
        ];
        self.build_image_local(
            tag,
            &assets,
            &format!("PG_VERSION={major_version}"),
            "fakecloud-postgres",
        )
        .await
    }

    /// Pull-first / build-fallback for the prebuilt fakecloud-mysql
    /// image. Mirrors `ensure_postgres_image`. The image bakes a small
    /// libcurl-backed UDF + Aurora-compatible `mysql.lambda_async` /
    /// `mysql.lambda_sync` stored procedures.
    pub(crate) async fn ensure_mysql_image(
        &self,
        major_version: &str,
    ) -> Result<String, RuntimeError> {
        let tag = bridge_image_tag("fakecloud-mysql", major_version);
        self.ensure_bridge_image(&tag, |tag| async move {
            self.build_mysql_image_local(major_version, &tag).await
        })
        .await
    }

    pub(crate) async fn ensure_mariadb_image(
        &self,
        major_version: &str,
    ) -> Result<String, RuntimeError> {
        let tag = bridge_image_tag("fakecloud-mariadb", major_version);
        self.ensure_bridge_image(&tag, |tag| async move {
            self.build_mariadb_image_local(major_version, &tag).await
        })
        .await
    }

    /// Shared pull-first/build-fallback orchestration used by every
    /// bridge-aware engine. Holds the per-tag mutex, checks the local
    /// daemon first, then tries the prebuilt image, and finally
    /// invokes the supplied local-build closure.
    async fn ensure_bridge_image<F, Fut>(
        &self,
        tag: &str,
        build_local: F,
    ) -> Result<String, RuntimeError>
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<Output = Result<(), RuntimeError>>,
    {
        let lock = {
            let mut cache = self.image_cache.write();
            cache
                .entry(tag.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(false)))
                .clone()
        };
        let mut resolved = lock.lock().await;
        if *resolved {
            return Ok(tag.to_string());
        }

        let force_rebuild = std::env::var("FAKECLOUD_REBUILD_POSTGRES_IMAGE")
            .map(|v| !v.is_empty())
            .unwrap_or(false);

        if !force_rebuild {
            if self.docker_image_exists(tag).await {
                *resolved = true;
                return Ok(tag.to_string());
            }
            if self.try_pull_image(tag).await {
                *resolved = true;
                return Ok(tag.to_string());
            }
        }

        build_local(tag.to_string()).await?;
        *resolved = true;
        Ok(tag.to_string())
    }

    async fn build_mysql_image_local(
        &self,
        major_version: &str,
        tag: &str,
    ) -> Result<(), RuntimeError> {
        let assets: [(&str, &str); 4] = [
            ("Dockerfile", MYSQL_DOCKERFILE),
            ("fakecloud_udf.c", MYSQL_UDF_C),
            ("fakecloud-bootstrap.sh", MYSQL_BOOTSTRAP_SH),
            ("99-fakecloud-bootstrap.sql.tmpl", MYSQL_BOOTSTRAP_SQL),
        ];
        self.build_image_local(
            tag,
            &assets,
            &format!("MYSQL_VERSION={major_version}"),
            "fakecloud-mysql",
        )
        .await
    }

    async fn build_mariadb_image_local(
        &self,
        major_version: &str,
        tag: &str,
    ) -> Result<(), RuntimeError> {
        let assets: [(&str, &str); 4] = [
            ("Dockerfile", MARIADB_DOCKERFILE),
            ("fakecloud_udf.c", MARIADB_UDF_C),
            ("fakecloud-bootstrap.sh", MARIADB_BOOTSTRAP_SH),
            ("99-fakecloud-bootstrap.sql.tmpl", MARIADB_BOOTSTRAP_SQL),
        ];
        self.build_image_local(
            tag,
            &assets,
            &format!("MARIADB_VERSION={major_version}"),
            "fakecloud-mariadb",
        )
        .await
    }

    async fn build_image_local(
        &self,
        tag: &str,
        assets: &[(&str, &str)],
        build_arg: &str,
        image_label: &str,
    ) -> Result<(), RuntimeError> {
        let build_dir =
            tempfile::tempdir().map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        for (name, contents) in assets {
            tokio::fs::write(build_dir.path().join(name), contents)
                .await
                .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        }

        tracing::info!(
            tag = %tag,
            image = %image_label,
            "Building {image_label} image locally (first use can take ~60s)"
        );

        let output = tokio::process::Command::new(&self.cli)
            .args(["build", "--build-arg", build_arg, "-t", tag, "."])
            .current_dir(build_dir.path())
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "docker build for {} failed: {}",
                tag,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        Ok(())
    }

    pub async fn dump_database(
        &self,
        db_instance_identifier: &str,
        engine: &str,
        username: &str,
        password: &str,
        db_name: &str,
    ) -> Result<Vec<u8>, RuntimeError> {
        let container = self
            .containers
            .read()
            .get(db_instance_identifier)
            .cloned()
            .ok_or(RuntimeError::Unavailable)?;

        if let Some(k) = &self.k8s {
            return k
                .dump_database(&container.container_id, engine, username, password, db_name)
                .await;
        }

        let args: Vec<String> = match engine {
            "mysql" | "mariadb" => vec![
                "exec".into(),
                container.container_id.clone(),
                "mysqldump".into(),
                "-u".into(),
                username.into(),
                format!("-p{password}"),
                db_name.into(),
            ],
            "postgres" => vec![
                "exec".into(),
                container.container_id.clone(),
                "pg_dump".into(),
                "-U".into(),
                username.into(),
                "-d".into(),
                db_name.into(),
                "--no-password".into(),
            ],
            // Heavy engines don't ship with a portable dump CLI we can
            // shell out to from the host the same way pg_dump and
            // mysqldump are guaranteed available. Surface a clear
            // error so callers (snapshot/read-replica) don't silently
            // run the wrong tool against an Oracle/SQL Server/Db2
            // container.
            "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" | "sqlserver-ee"
            | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" | "db2-se" | "db2-ae" => {
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "engine {engine} is not yet supported by the snapshot/read-replica path; \
                     emulator stores the API state but cannot dump the underlying database"
                )));
            }
            other => {
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "engine {other} is not supported by dump_database"
                )));
            }
        };

        let output = tokio::process::Command::new(&self.cli)
            .args(&args)
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "dump failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        Ok(output.stdout)
    }

    /// Cat a file inside the running container by absolute path.
    /// Returns `Unavailable` when the runtime/daemon is missing or the
    /// instance has no live container; returns `ContainerStartFailed`
    /// when the file is missing or unreadable. Callers map those to the
    /// AWS-shaped responses.
    pub async fn read_log_file(
        &self,
        db_instance_identifier: &str,
        container_path: &str,
    ) -> Result<Vec<u8>, RuntimeError> {
        let container = self
            .containers
            .read()
            .get(db_instance_identifier)
            .cloned()
            .ok_or(RuntimeError::Unavailable)?;

        if let Some(k) = &self.k8s {
            return k.read_file(&container.container_id, container_path).await;
        }

        let output = tokio::process::Command::new(&self.cli)
            .args(["exec", &container.container_id, "cat", container_path])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "cat {container_path} failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        Ok(output.stdout)
    }

    pub async fn restore_database(
        &self,
        db_instance_identifier: &str,
        engine: &str,
        username: &str,
        password: &str,
        db_name: &str,
        dump_data: &[u8],
    ) -> Result<(), RuntimeError> {
        let container = self
            .containers
            .read()
            .get(db_instance_identifier)
            .cloned()
            .ok_or(RuntimeError::Unavailable)?;

        if let Some(k) = &self.k8s {
            return k
                .restore_database(
                    &container.container_id,
                    engine,
                    username,
                    password,
                    db_name,
                    dump_data,
                )
                .await;
        }

        let args: Vec<String> = match engine {
            "mysql" | "mariadb" => vec![
                "exec".into(),
                "-i".into(),
                container.container_id.clone(),
                "mysql".into(),
                "-u".into(),
                username.into(),
                format!("-p{password}"),
                db_name.into(),
            ],
            "postgres" => vec![
                "exec".into(),
                "-i".into(),
                container.container_id.clone(),
                "psql".into(),
                "-U".into(),
                username.into(),
                "-d".into(),
                db_name.into(),
                "--no-password".into(),
                "-v".into(),
                "ON_ERROR_STOP=1".into(),
            ],
            "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" | "sqlserver-ee"
            | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" | "db2-se" | "db2-ae" => {
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "engine {engine} is not yet supported by the snapshot-restore path"
                )));
            }
            other => {
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "engine {other} is not supported by restore_database"
                )));
            }
        };

        let mut child = tokio::process::Command::new(&self.cli)
            .args(&args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin
                .write_all(dump_data)
                .await
                .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
            drop(stdin);
        }

        let output = child
            .wait_with_output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "restore failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        Ok(())
    }
}

fn cli_available(cli: &str) -> bool {
    std::process::Command::new(cli)
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// On Linux Docker bridge networks, ask the daemon for the bridge
/// gateway IP so containers can reach the host's loopback. macOS and
/// Windows use the magic `host-gateway` alias instead.
fn detect_bridge_gateway(cli: &str) -> Option<String> {
    let output = std::process::Command::new(cli)
        .args([
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{range .IPAM.Config}}{{.Gateway}}{{end}}",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let gateway = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if gateway.is_empty() || !gateway.contains('.') {
        return None;
    }
    Some(gateway)
}

/// Build the prebuilt-image reference for a given engine + major
/// version. Uses `<registry>/<image>:<major>-<fakecloud-version>`,
/// where the registry comes from `FAKECLOUD_POSTGRES_REGISTRY` (kept
/// historical name; defaults to the public `ghcr.io/faiscadev`).
/// The version pin guarantees the runtime asks the daemon for the
/// same image CI publishes for this fakecloud release; mismatched
/// assets force a local rebuild via the fall-through in
/// `ensure_bridge_image`.
pub(crate) fn bridge_image_tag(image: &str, major_version: &str) -> String {
    let registry = std::env::var("FAKECLOUD_POSTGRES_REGISTRY")
        .unwrap_or_else(|_| DEFAULT_POSTGRES_REGISTRY.to_string());
    let registry = registry.trim_end_matches('/');
    format!(
        "{}/{}:{}-{}",
        registry,
        image,
        major_version,
        env!("CARGO_PKG_VERSION")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Single test (rather than three) so the cases run sequentially —
    /// `bridge_image_tag` reads a process-global env var and parallel
    /// `cargo test` workers would race over it otherwise.
    #[test]
    fn bridge_image_tag_resolves_registry_overrides() {
        let prev = std::env::var("FAKECLOUD_POSTGRES_REGISTRY").ok();

        std::env::remove_var("FAKECLOUD_POSTGRES_REGISTRY");
        assert_eq!(
            bridge_image_tag("fakecloud-postgres", "16"),
            format!(
                "ghcr.io/faiscadev/fakecloud-postgres:16-{}",
                env!("CARGO_PKG_VERSION")
            )
        );
        assert_eq!(
            bridge_image_tag("fakecloud-mysql", "8.0"),
            format!(
                "ghcr.io/faiscadev/fakecloud-mysql:8.0-{}",
                env!("CARGO_PKG_VERSION")
            )
        );
        assert_eq!(
            bridge_image_tag("fakecloud-mariadb", "10.11"),
            format!(
                "ghcr.io/faiscadev/fakecloud-mariadb:10.11-{}",
                env!("CARGO_PKG_VERSION")
            )
        );

        std::env::set_var("FAKECLOUD_POSTGRES_REGISTRY", "registry.example.com/team");
        assert_eq!(
            bridge_image_tag("fakecloud-postgres", "15"),
            format!(
                "registry.example.com/team/fakecloud-postgres:15-{}",
                env!("CARGO_PKG_VERSION")
            )
        );

        std::env::set_var("FAKECLOUD_POSTGRES_REGISTRY", "registry.example.com/team/");
        assert_eq!(
            bridge_image_tag("fakecloud-postgres", "13"),
            format!(
                "registry.example.com/team/fakecloud-postgres:13-{}",
                env!("CARGO_PKG_VERSION")
            )
        );

        match prev {
            Some(v) => std::env::set_var("FAKECLOUD_POSTGRES_REGISTRY", v),
            None => std::env::remove_var("FAKECLOUD_POSTGRES_REGISTRY"),
        }
    }
}
