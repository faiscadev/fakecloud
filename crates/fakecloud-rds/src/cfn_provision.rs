//! CloudFormation-driven container backing for RDS instances.
//!
//! When `AWS::RDS::DBInstance` is provisioned through a CloudFormation stack,
//! the CFN provisioner inserts the `DbInstance` record synchronously (so
//! `Ref`/`GetAtt` resolve during provisioning) with status `creating`, then
//! asks RDS to back it with a REAL Postgres/MySQL container — the same
//! container the direct `CreateDBInstance` path spawns. This mirrors that
//! background task for an already-inserted record, so a CFN-provisioned RDS
//! instance is genuinely connectable, not phantom metadata.

use std::sync::Arc;

use crate::runtime::RdsRuntime;
use crate::SharedRdsState;

/// Default logical DB name per engine when the template omits `DBName`,
/// matching what the direct `CreateDBInstance` path uses.
fn default_db_name(engine: &str) -> &'static str {
    match engine {
        "mysql" | "mariadb" => "mysql",
        "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => "ORCL",
        "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => "master",
        "db2-se" | "db2-ae" => "BLUDB",
        _ => "postgres",
    }
}

/// Back an already-inserted (status `creating`) DBInstance with a real
/// container and flip it to `available`. No-op if the record is gone (e.g. the
/// stack was deleted before the container booted). Intended to be `tokio::spawn`ed
/// by the CloudFormation `CreateStack` drain so stack creation never blocks on a
/// container boot/pull (the #1539/#1730 timeout lesson).
pub async fn cfn_ensure_instance_container(
    state: SharedRdsState,
    runtime: Arc<RdsRuntime>,
    identifier: String,
    account_id: String,
    region: String,
) {
    // Snapshot the create parameters from the inserted record.
    let params = {
        let mut accounts = state.write();
        let st = accounts.get_or_create(&account_id);
        match st.instances.get(&identifier) {
            Some(inst) => {
                let db_name = inst
                    .db_name
                    .clone()
                    .unwrap_or_else(|| default_db_name(&inst.engine).to_string());
                (
                    inst.engine.clone(),
                    inst.engine_version.clone(),
                    inst.master_username.clone(),
                    inst.master_user_password.clone(),
                    db_name,
                    inst.tags.clone(),
                )
            }
            None => return,
        }
    };
    let (engine, engine_version, username, password, db_name, tags) = params;

    match runtime
        .ensure_postgres(
            &identifier,
            &engine,
            &engine_version,
            &username,
            &password,
            &db_name,
            &account_id,
            &region,
            &tags,
        )
        .await
    {
        Ok(running) => {
            // Apply the update in a block so the (non-Send) write guard is
            // dropped before any `.await` below.
            let present = {
                let mut accounts = state.write();
                let st = accounts.get_or_create(&account_id);
                if let Some(inst) = st.instances.get_mut(&identifier) {
                    inst.db_instance_status = "available".to_string();
                    inst.endpoint_address = running.endpoint_address;
                    inst.port = i32::from(running.endpoint_port);
                    inst.host_port = running.host_port;
                    inst.container_id = running.container_id;
                    true
                } else {
                    false
                }
            };
            if !present {
                // Deleted mid-boot: reap the orphaned container.
                runtime.stop_container(&identifier).await;
            }
        }
        Err(error) => {
            tracing::error!(
                %error,
                db_instance_identifier = %identifier,
                "CFN-provisioned RDS instance failed to start its container",
            );
            let mut accounts = state.write();
            let st = accounts.get_or_create(&account_id);
            if let Some(inst) = st.instances.get_mut(&identifier) {
                inst.db_instance_status = "failed".to_string();
            }
        }
    }
}

/// Stop and reap the REAL container backing a CFN-provisioned DB instance when
/// its stack is deleted (or the resource is removed by a stack update). Mirrors
/// the direct `DeleteDBInstance` teardown (`stop_container` +
/// `remove_data_volume`) so a stack delete does not leak the running Postgres /
/// MySQL container or its persisted data volume. Intended to be `tokio::spawn`ed
/// by the CloudFormation delete drain after the in-memory record has already
/// been removed.
pub async fn cfn_teardown_instance_container(
    runtime: Arc<RdsRuntime>,
    identifier: String,
    account_id: String,
) {
    runtime.stop_container(&identifier).await;
    runtime.remove_data_volume(&account_id, &identifier).await;
}
