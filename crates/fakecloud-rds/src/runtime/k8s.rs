//! Kubernetes backend for RDS DB instances.
//!
//! Runs each DB instance as a native Pod instead of a Docker container.
//! Shared client / lifecycle / exec / logs / reaping plumbing comes from
//! the `fakecloud-k8s` crate; this module builds the per-engine Pod spec
//! and maps the RDS runtime operations onto Pod operations.
//!
//! Engine coverage matches the Docker backend: the bridge engines
//! (postgres / mysql / mariadb) use the prebuilt `fakecloud-*` images
//! that bake the aws_lambda / aws_s3 / UDF integrations and call back
//! into fakecloud; the heavy engines (oracle / sqlserver / db2) use their
//! upstream images. Unlike Docker there's no local image build — the
//! cluster pulls the published image (override the registry with
//! `FAKECLOUD_POSTGRES_REGISTRY`, or use `FAKECLOUD_K8S_PULL_SECRET` for a
//! private one).
//!
//! Because fakecloud connects to the DB over the Pod IP, this backend
//! only functions when fakecloud itself runs in-cluster (the documented
//! requirement for `FAKECLOUD_K8S_SELF_URL`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, LocalObjectReference, Pod, PodSpec, SecurityContext,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use parking_lot::RwLock;
use tokio_postgres::NoTls;

use fakecloud_k8s::{labels, names, K8sClient, K8sEnv, K8sPodConfig};

use super::{bridge_image_tag, BackendInitError, RunningDbContainer, RuntimeError};

const SERVICE: &str = "rds";
const POD_PREFIX: &str = "fakecloud-rds";
const CONTAINER: &str = "db";

/// How fakecloud decides a freshly-started engine is ready to serve.
#[derive(Clone, Copy)]
enum Readiness {
    Postgres,
    Mysql,
    /// Poll the Pod log for `marker`, then TCP-probe — for engines with
    /// no quick connect-based probe (Oracle / SQL Server / Db2).
    LogMarker {
        marker: &'static str,
        deadline_secs: u64,
    },
}

/// Resolved per-engine Pod configuration.
struct EngineCfg {
    image: String,
    port: u16,
    env: Vec<(String, String)>,
    privileged: bool,
    readiness: Readiness,
}

#[derive(Clone)]
pub(super) struct K8sDb {
    client: K8sClient,
    /// In-cluster fakecloud base URL — wired into bridge engines as
    /// `FAKECLOUD_ENDPOINT` so their aws_lambda/aws_s3/UDF callbacks reach
    /// fakecloud.
    self_url: String,
    pull_secret: Option<String>,
    /// Global + RDS-service node selector / tolerations / annotations
    /// applied to every DB Pod. Per-instance tag overrides are merged over
    /// this when the Pod is built.
    pod_config: K8sPodConfig,
    /// Built Pod spec + port per instance id, so a reboot can recreate the
    /// Pod without re-deriving the engine config.
    specs: Arc<RwLock<HashMap<String, (Pod, u16)>>>,
}

impl std::fmt::Debug for K8sDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sDb")
            .field("namespace", &self.client.namespace())
            .field("self_url", &self.self_url)
            .finish_non_exhaustive()
    }
}

impl K8sDb {
    pub(super) async fn from_env(server_port: u16) -> Result<Self, BackendInitError> {
        let env = K8sEnv::from_env(server_port)?;
        let pod_config = K8sPodConfig::resolved_base("FAKECLOUD_RDS_K8S")?;
        let client = K8sClient::connect(env.namespace.clone())
            .await
            .map_err(|e| BackendInitError::Connect(e.to_string()))?;
        tracing::info!(
            namespace = %env.namespace,
            self_url = %env.self_url,
            "K8s RDS backend initialized"
        );
        Ok(Self {
            client,
            self_url: env.self_url,
            pull_secret: env.pull_secret,
            pod_config,
            specs: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn ensure(
        &self,
        db_instance_identifier: &str,
        engine: &str,
        engine_version: &str,
        username: &str,
        password: &str,
        db_name: &str,
        account_id: &str,
        region: &str,
        tags: &std::collections::BTreeMap<String, String>,
    ) -> Result<RunningDbContainer, RuntimeError> {
        let cfg = engine_config(
            &self.self_url,
            engine,
            engine_version,
            username,
            password,
            db_name,
            account_id,
            region,
        )?;
        let pod_name = names::pod_name(POD_PREFIX, db_instance_identifier, db_instance_identifier);
        let mut pod = build_pod(
            self.client.namespace(),
            self.client.instance_id(),
            self.pull_secret.as_deref(),
            &pod_name,
            db_instance_identifier,
            &cfg,
        );
        // Global + service base with this instance's reserved-tag
        // overrides merged over it (per-instance tags win). Applied before
        // the spec is cached so reboots reuse the same scheduling.
        self.pod_config
            .clone()
            .merge(K8sPodConfig::from_tags(tags))
            .apply(&mut pod);

        self.specs
            .write()
            .insert(db_instance_identifier.to_string(), (pod.clone(), cfg.port));

        self.launch(&pod, &pod_name, &cfg, username, password, db_name)
            .await
    }

    async fn launch(
        &self,
        pod: &Pod,
        pod_name: &str,
        cfg: &EngineCfg,
        username: &str,
        password: &str,
        db_name: &str,
    ) -> Result<RunningDbContainer, RuntimeError> {
        self.client
            .create_pod(pod)
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("create db pod: {e}")))?;

        // Heavy engines take minutes to bootstrap; give the Pod a generous
        // window to reach Running with an IP.
        let pod_ip = match self
            .client
            .wait_for_pod_ip(pod_name, Duration::from_secs(300))
            .await
        {
            Ok(ip) => ip,
            Err(e) => {
                self.client.delete_pod(pod_name).await;
                return Err(RuntimeError::ContainerStartFailed(e.to_string()));
            }
        };

        if let Err(e) = self
            .wait_ready(pod_name, &pod_ip, cfg, username, password, db_name)
            .await
        {
            self.client.delete_pod(pod_name).await;
            return Err(e);
        }

        Ok(RunningDbContainer {
            container_id: pod_name.to_string(),
            host_port: cfg.port,
            endpoint_address: pod_ip,
            endpoint_port: cfg.port,
        })
    }

    async fn wait_ready(
        &self,
        pod_name: &str,
        pod_ip: &str,
        cfg: &EngineCfg,
        username: &str,
        password: &str,
        db_name: &str,
    ) -> Result<(), RuntimeError> {
        match cfg.readiness {
            Readiness::Postgres => {
                for _ in 0..120 {
                    let conn_str = format!(
                        "host={pod_ip} port={} user={username} password={password} dbname={db_name}",
                        cfg.port
                    );
                    if let Ok((client, connection)) =
                        tokio_postgres::connect(&conn_str, NoTls).await
                    {
                        tokio::spawn(async move {
                            let _ = connection.await;
                        });
                        if client.simple_query("SELECT 1").await.is_ok() {
                            return Ok(());
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(RuntimeError::ContainerStartFailed(
                    "postgres pod did not become ready within 60s".into(),
                ))
            }
            Readiness::Mysql => {
                use mysql_async::prelude::Queryable;
                use mysql_async::OptsBuilder;
                for _ in 0..120 {
                    let opts = OptsBuilder::default()
                        .ip_or_hostname(pod_ip.to_string())
                        .tcp_port(cfg.port)
                        .user(Some(username))
                        .pass(Some(password))
                        .db_name(Some(db_name));
                    if let Ok(mut conn) = mysql_async::Conn::new(opts).await {
                        if conn.query_drop("SELECT 1").await.is_ok() {
                            let _ = conn.disconnect().await;
                            return Ok(());
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(RuntimeError::ContainerStartFailed(
                    "mysql/mariadb pod did not become ready within 60s".into(),
                ))
            }
            Readiness::LogMarker {
                marker,
                deadline_secs,
            } => {
                self.wait_for_log_marker(pod_name, marker, deadline_secs)
                    .await?;
                self.wait_for_tcp(pod_ip, cfg.port, 60).await
            }
        }
    }

    async fn wait_for_log_marker(
        &self,
        pod_name: &str,
        marker: &str,
        deadline_secs: u64,
    ) -> Result<(), RuntimeError> {
        let deadline = Instant::now() + Duration::from_secs(deadline_secs);
        while Instant::now() < deadline {
            if let Ok(logs) = self.client.pod_logs(pod_name, Some(CONTAINER)).await {
                if logs.contains(marker) {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        Err(RuntimeError::ContainerStartFailed(format!(
            "pod did not log '{marker}' within {deadline_secs}s"
        )))
    }

    async fn wait_for_tcp(
        &self,
        ip: &str,
        port: u16,
        deadline_secs: u64,
    ) -> Result<(), RuntimeError> {
        K8sClient::wait_for_tcp(ip, port, Duration::from_secs(deadline_secs))
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))
    }

    pub(super) async fn delete_pod(&self, pod_name: &str) {
        self.client.delete_pod(pod_name).await;
    }

    /// Reboot = recreate the Pod from its stored spec. For Postgres /
    /// MySQL / MariaDB the dataset is preserved by dumping it out and
    /// reloading it after the fresh Pod is ready (the data dir is an
    /// ephemeral Pod filesystem, so a plain recreate would otherwise lose
    /// it). Heavy engines recreate without a dump (no portable dump CLI),
    /// matching the Docker backend's snapshot limitation.
    pub(super) async fn restart(
        &self,
        db_instance_identifier: &str,
        engine: &str,
        username: &str,
        password: &str,
        db_name: &str,
    ) -> Result<RunningDbContainer, RuntimeError> {
        let (pod, port) = self
            .specs
            .read()
            .get(db_instance_identifier)
            .cloned()
            .ok_or(RuntimeError::Unavailable)?;
        let pod_name = pod.metadata.name.clone().ok_or(RuntimeError::Unavailable)?;

        // Best-effort capture of the current dataset for the dumpable
        // engines before tearing the Pod down.
        let dump = if matches!(engine, "postgres" | "mysql" | "mariadb") {
            self.dump_database(&pod_name, engine, username, password, db_name)
                .await
                .ok()
        } else {
            None
        };

        self.client.delete_pod(&pod_name).await;

        let cfg_readiness = readiness_for(engine);
        self.client
            .create_pod(&pod)
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("recreate db pod: {e}")))?;
        let pod_ip = match self
            .client
            .wait_for_pod_ip(&pod_name, Duration::from_secs(300))
            .await
        {
            Ok(ip) => ip,
            Err(e) => {
                self.client.delete_pod(&pod_name).await;
                return Err(RuntimeError::ContainerStartFailed(e.to_string()));
            }
        };
        let cfg = EngineCfg {
            image: String::new(),
            port,
            env: Vec::new(),
            privileged: false,
            readiness: cfg_readiness,
        };
        if let Err(e) = self
            .wait_ready(&pod_name, &pod_ip, &cfg, username, password, db_name)
            .await
        {
            self.client.delete_pod(&pod_name).await;
            return Err(e);
        }

        if let Some(data) = dump {
            // Reload preserved data; a failure here shouldn't fail the
            // reboot (the engine is up), so log and continue.
            if let Err(e) = self
                .restore_database(&pod_name, engine, username, password, db_name, &data)
                .await
            {
                tracing::warn!(db = %db_instance_identifier, error = %e, "reboot data reload failed");
            }
        }

        Ok(RunningDbContainer {
            container_id: pod_name,
            host_port: port,
            endpoint_address: pod_ip,
            endpoint_port: port,
        })
    }

    pub(super) async fn dump_database(
        &self,
        pod_name: &str,
        engine: &str,
        username: &str,
        password: &str,
        db_name: &str,
    ) -> Result<Vec<u8>, RuntimeError> {
        let pass_arg;
        let cmd: Vec<&str> = match engine {
            "mysql" | "mariadb" => {
                pass_arg = format!("-p{password}");
                vec!["mysqldump", "-u", username, &pass_arg, db_name]
            }
            "postgres" => vec!["pg_dump", "-U", username, "-d", db_name, "--no-password"],
            other => {
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "engine {other} is not supported by the snapshot/read-replica path on the k8s backend"
                )));
            }
        };
        let out = self
            .client
            .exec(pod_name, Some(CONTAINER), &cmd)
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("exec dump: {e}")))?;
        if !out.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "dump failed: {}",
                out.stderr.trim()
            )));
        }
        Ok(out.stdout)
    }

    pub(super) async fn read_file(
        &self,
        pod_name: &str,
        path: &str,
    ) -> Result<Vec<u8>, RuntimeError> {
        let out = self
            .client
            .exec(pod_name, Some(CONTAINER), &["cat", path])
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("exec cat: {e}")))?;
        if !out.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "cat {path} failed: {}",
                out.stderr.trim()
            )));
        }
        Ok(out.stdout)
    }

    pub(super) async fn restore_database(
        &self,
        pod_name: &str,
        engine: &str,
        username: &str,
        password: &str,
        db_name: &str,
        dump_data: &[u8],
    ) -> Result<(), RuntimeError> {
        let pass_arg;
        let cmd: Vec<&str> = match engine {
            "mysql" | "mariadb" => {
                pass_arg = format!("-p{password}");
                vec!["mysql", "-u", username, &pass_arg, db_name]
            }
            "postgres" => vec![
                "psql",
                "-U",
                username,
                "-d",
                db_name,
                "--no-password",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            other => {
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "engine {other} is not supported by the snapshot-restore path on the k8s backend"
                )));
            }
        };
        let out = self
            .client
            .exec_with_stdin(pod_name, Some(CONTAINER), &cmd, dump_data)
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("exec restore: {e}")))?;
        if !out.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "restore failed: {}",
                out.stderr.trim()
            )));
        }
        Ok(())
    }

    pub(super) async fn reap_stale(&self) {
        self.client.reap_stale(SERVICE).await;
    }

    // engine config + pod spec are pure free functions below.
}

#[allow(clippy::too_many_arguments)]
fn engine_config(
    self_url: &str,
    engine: &str,
    engine_version: &str,
    username: &str,
    password: &str,
    db_name: &str,
    account_id: &str,
    region: &str,
) -> Result<EngineCfg, RuntimeError> {
    // Bridge engines call back into fakecloud at its in-cluster URL.
    let bridge_env = |extra: Vec<(String, String)>| {
        let mut env = extra;
        env.push(("FAKECLOUD_ENDPOINT".into(), self_url.to_string()));
        env.push(("FAKECLOUD_ACCOUNT_ID".into(), account_id.to_string()));
        env.push(("FAKECLOUD_REGION".into(), region.to_string()));
        env
    };
    Ok(match engine {
        "postgres" => {
            let major = engine_version.split('.').next().unwrap_or("16");
            EngineCfg {
                image: bridge_image_tag("fakecloud-postgres", major),
                port: 5432,
                env: bridge_env(vec![
                    ("POSTGRES_USER".into(), username.to_string()),
                    ("POSTGRES_PASSWORD".into(), password.to_string()),
                    ("POSTGRES_DB".into(), db_name.to_string()),
                ]),
                privileged: false,
                readiness: Readiness::Postgres,
            }
        }
        "mysql" => EngineCfg {
            image: bridge_image_tag("fakecloud-mysql", "8.0"),
            port: 3306,
            env: bridge_env(vec![
                ("MYSQL_ROOT_PASSWORD".into(), password.to_string()),
                ("MYSQL_USER".into(), username.to_string()),
                ("MYSQL_PASSWORD".into(), password.to_string()),
                ("MYSQL_DATABASE".into(), db_name.to_string()),
            ]),
            privileged: false,
            readiness: Readiness::Mysql,
        },
        "mariadb" => {
            let major = if engine_version.starts_with("10.11") {
                "10.11"
            } else if engine_version.starts_with("11.4") {
                "11.4"
            } else {
                "10.6"
            };
            EngineCfg {
                image: bridge_image_tag("fakecloud-mariadb", major),
                port: 3306,
                env: bridge_env(vec![
                    ("MARIADB_ROOT_PASSWORD".into(), password.to_string()),
                    ("MARIADB_USER".into(), username.to_string()),
                    ("MARIADB_PASSWORD".into(), password.to_string()),
                    ("MARIADB_DATABASE".into(), db_name.to_string()),
                ]),
                privileged: false,
                readiness: Readiness::Mysql,
            }
        }
        "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => EngineCfg {
            image: "gvenzl/oracle-free:23-slim".into(),
            port: 1521,
            env: vec![
                ("ORACLE_PASSWORD".into(), password.to_string()),
                ("APP_USER".into(), username.to_string()),
                ("APP_USER_PASSWORD".into(), password.to_string()),
                ("ORACLE_DATABASE".into(), db_name.to_string()),
            ],
            privileged: false,
            readiness: Readiness::LogMarker {
                marker: "DATABASE IS READY TO USE!",
                deadline_secs: 240,
            },
        },
        "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => EngineCfg {
            image: "mcr.microsoft.com/mssql/server:2022-latest".into(),
            port: 1433,
            env: vec![
                ("ACCEPT_EULA".into(), "Y".into()),
                ("MSSQL_SA_PASSWORD".into(), password.to_string()),
                ("MSSQL_PID".into(), "Express".into()),
            ],
            privileged: false,
            readiness: Readiness::LogMarker {
                marker: "SQL Server is now ready for client connections",
                deadline_secs: 180,
            },
        },
        "db2-se" | "db2-ae" => EngineCfg {
            image: "icr.io/db2_community/db2:latest".into(),
            port: 50000,
            env: vec![
                ("LICENSE".into(), "accept".into()),
                ("DB2INSTANCE".into(), "db2inst1".into()),
                ("DB2INST1_PASSWORD".into(), password.to_string()),
                ("DBNAME".into(), db_name.to_string()),
            ],
            privileged: true,
            readiness: Readiness::LogMarker {
                marker: "Setup has completed",
                deadline_secs: 360,
            },
        },
        other => {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "Unsupported engine: {other}"
            )))
        }
    })
}

fn build_pod(
    namespace: &str,
    instance_id: &str,
    pull_secret: Option<&str>,
    pod_name: &str,
    db_id: &str,
    cfg: &EngineCfg,
) -> Pod {
    let mut pod_labels = std::collections::BTreeMap::new();
    pod_labels.insert(
        labels::MANAGED_BY.to_string(),
        labels::MANAGED_BY_VALUE.to_string(),
    );
    pod_labels.insert(labels::INSTANCE.to_string(), instance_id.to_string());
    pod_labels.insert(labels::SERVICE.to_string(), SERVICE.to_string());
    pod_labels.insert("fakecloud-rds".to_string(), names::label_safe(db_id));

    let env = cfg
        .env
        .iter()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            value_from: None,
        })
        .collect();

    let security_context = cfg.privileged.then(|| SecurityContext {
        privileged: Some(true),
        ..SecurityContext::default()
    });

    let container = Container {
        name: CONTAINER.to_string(),
        image: Some(cfg.image.clone()),
        env: Some(env),
        ports: Some(vec![ContainerPort {
            container_port: cfg.port as i32,
            ..ContainerPort::default()
        }]),
        security_context,
        ..Container::default()
    };

    let pull_secrets = pull_secret.map(|name| {
        vec![LocalObjectReference {
            name: name.to_string(),
        }]
    });

    Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(pod_labels),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            restart_policy: Some("Never".to_string()),
            containers: vec![container],
            image_pull_secrets: pull_secrets,
            ..PodSpec::default()
        }),
        ..Pod::default()
    }
}

/// Readiness mode for an engine, used by reboot (which doesn't re-derive
/// the full [`EngineCfg`]).
fn readiness_for(engine: &str) -> Readiness {
    match engine {
        "postgres" => Readiness::Postgres,
        "mysql" | "mariadb" => Readiness::Mysql,
        "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => Readiness::LogMarker {
            marker: "DATABASE IS READY TO USE!",
            deadline_secs: 240,
        },
        "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => {
            Readiness::LogMarker {
                marker: "SQL Server is now ready for client connections",
                deadline_secs: 180,
            }
        }
        _ => Readiness::LogMarker {
            marker: "Setup has completed",
            deadline_secs: 360,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // engine_config and build_pod are pure free functions (no cluster
    // I/O), so they're unit-testable directly.

    #[test]
    fn postgres_uses_bridge_image_and_callback_env() {
        std::env::remove_var("FAKECLOUD_POSTGRES_REGISTRY");
        let cfg = engine_config(
            "http://fakecloud.fc.svc:4566",
            "postgres",
            "16.4",
            "admin",
            "secret",
            "appdb",
            "000000000000",
            "us-east-1",
        )
        .unwrap();
        assert!(cfg
            .image
            .starts_with("ghcr.io/faiscadev/fakecloud-postgres:16-"));
        assert_eq!(cfg.port, 5432);
        assert!(matches!(cfg.readiness, Readiness::Postgres));
        // The in-DB extension callback points at the in-cluster URL.
        assert!(cfg
            .env
            .iter()
            .any(|(k, v)| k == "FAKECLOUD_ENDPOINT" && v == "http://fakecloud.fc.svc:4566"));
        assert!(cfg
            .env
            .iter()
            .any(|(k, v)| k == "POSTGRES_PASSWORD" && v == "secret"));
    }

    #[test]
    fn oracle_uses_upstream_image_and_log_marker() {
        let cfg = engine_config(
            "http://fc:4566",
            "oracle-se2",
            "23",
            "u",
            "p",
            "db",
            "0",
            "r",
        )
        .unwrap();
        assert_eq!(cfg.image, "gvenzl/oracle-free:23-slim");
        assert_eq!(cfg.port, 1521);
        assert!(matches!(cfg.readiness, Readiness::LogMarker { .. }));
    }

    #[test]
    fn db2_is_privileged() {
        let cfg =
            engine_config("http://fc:4566", "db2-se", "11.5", "u", "p", "db", "0", "r").unwrap();
        assert!(cfg.privileged);
        assert_eq!(cfg.port, 50000);
    }

    #[test]
    fn unknown_engine_errors() {
        assert!(engine_config("http://fc", "informix", "1", "u", "p", "db", "0", "r").is_err());
    }

    #[test]
    fn build_pod_sets_labels_image_port_and_pull_secret() {
        let cfg =
            engine_config("http://fc:4566", "postgres", "16", "u", "p", "db", "0", "r").unwrap();
        let pod = build_pod(
            "fc-ns",
            "fakecloud-123",
            Some("reg-secret"),
            "fakecloud-rds-x",
            "My_DB",
            &cfg,
        );
        let l = pod.metadata.labels.unwrap();
        assert_eq!(l.get(labels::SERVICE).unwrap(), "rds");
        assert_eq!(l.get(labels::INSTANCE).unwrap(), "fakecloud-123");
        assert_eq!(l.get("fakecloud-rds").unwrap(), "my-db");
        let spec = pod.spec.unwrap();
        assert_eq!(
            spec.containers[0].ports.as_ref().unwrap()[0].container_port,
            5432
        );
        assert_eq!(spec.image_pull_secrets.unwrap()[0].name, "reg-secret");
        assert_eq!(pod.metadata.namespace.as_deref(), Some("fc-ns"));
    }

    #[test]
    fn build_pod_marks_db2_privileged() {
        let cfg = engine_config("http://fc", "db2-se", "11.5", "u", "p", "db", "0", "r").unwrap();
        let pod = build_pod("ns", "i", None, "fakecloud-rds-d", "d", &cfg);
        let sc = pod.spec.unwrap().containers[0]
            .security_context
            .clone()
            .unwrap();
        assert_eq!(sc.privileged, Some(true));
    }

    #[test]
    fn pod_config_overrides_apply_to_built_pod() {
        use std::collections::BTreeMap;
        // Mirrors the `ensure` wiring: service base merged with the
        // instance's reserved-tag overrides, applied to the built Pod.
        let cfg =
            engine_config("http://fc:4566", "postgres", "16", "u", "p", "db", "0", "r").unwrap();
        let mut pod = build_pod("ns", "i", None, "fakecloud-rds-x", "db1", &cfg);
        let base = K8sPodConfig {
            node_selector: BTreeMap::from([("pool".to_string(), "db".to_string())]),
            ..Default::default()
        };
        let tags = BTreeMap::from([
            (
                "fakecloud-k8s/node-selector".to_string(),
                "pool=spot,disktype=ssd".to_string(),
            ),
            (
                "fakecloud-k8s/annotations".to_string(),
                "team=data".to_string(),
            ),
        ]);
        base.merge(K8sPodConfig::from_tags(&tags)).apply(&mut pod);

        let spec = pod.spec.unwrap();
        let sel = spec.node_selector.unwrap();
        // Per-instance tag overrides the base on `pool`; base-only keys survive.
        assert_eq!(sel.get("pool").map(String::as_str), Some("spot"));
        assert_eq!(sel.get("disktype").map(String::as_str), Some("ssd"));
        assert_eq!(
            pod.metadata
                .annotations
                .unwrap()
                .get("team")
                .map(String::as_str),
            Some("data")
        );
    }

    #[test]
    fn readiness_for_maps_engines() {
        assert!(matches!(readiness_for("postgres"), Readiness::Postgres));
        assert!(matches!(readiness_for("mysql"), Readiness::Mysql));
        assert!(matches!(readiness_for("mariadb"), Readiness::Mysql));
        assert!(matches!(
            readiness_for("oracle-ee"),
            Readiness::LogMarker { .. }
        ));
    }
}
