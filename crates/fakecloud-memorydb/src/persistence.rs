//! Snapshot save/load for MemoryDB state.

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_persistence::SnapshotStore;

use crate::state::{
    LegacyMemoryDbSnapshot, MemoryDbSnapshot, SharedMemoryDbState, MEMORYDB_SNAPSHOT_SCHEMA_VERSION,
};

#[derive(Debug, PartialEq, Eq)]
pub enum LoadOutcome {
    Empty,
    Loaded(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("failed to read memorydb persistence snapshot: {0}")]
    Io(String),
    #[error("failed to parse memorydb persistence snapshot: {0}")]
    Parse(String),
    #[error("memorydb persistence schema too new: on-disk={on_disk}, max supported={supported}")]
    SchemaTooNew { on_disk: u32, supported: u32 },
}

pub fn load_into(
    store: &dyn SnapshotStore,
    state: &SharedMemoryDbState,
) -> Result<LoadOutcome, LoadError> {
    let Some(bytes) = store.load().map_err(|e| LoadError::Io(e.to_string()))? else {
        return Ok(LoadOutcome::Empty);
    };
    let snapshot = parse_snapshot(&bytes)?;
    let accounts = snapshot.accounts.account_count();
    *state.write() = snapshot.accounts;
    Ok(LoadOutcome::Loaded(accounts))
}

#[derive(serde::Deserialize)]
struct SnapshotVersion {
    schema_version: u32,
}

/// Parse a persisted snapshot, migrating a v1 one (one state per account for
/// every region) by moving each resource into the region its ARN names.
fn parse_snapshot(bytes: &[u8]) -> Result<MemoryDbSnapshot, LoadError> {
    let parse_err = |e: serde_json::Error| LoadError::Parse(e.to_string());
    let SnapshotVersion { schema_version } = serde_json::from_slice(bytes).map_err(parse_err)?;
    if schema_version > MEMORYDB_SNAPSHOT_SCHEMA_VERSION {
        return Err(LoadError::SchemaTooNew {
            on_disk: schema_version,
            supported: MEMORYDB_SNAPSHOT_SCHEMA_VERSION,
        });
    }
    if schema_version == MEMORYDB_SNAPSHOT_SCHEMA_VERSION {
        return serde_json::from_slice(bytes).map_err(parse_err);
    }
    let legacy: LegacyMemoryDbSnapshot = serde_json::from_slice(bytes).map_err(parse_err)?;
    let region = legacy.accounts.region().to_string();
    let endpoint = legacy.accounts.endpoint().to_string();
    Ok(MemoryDbSnapshot {
        schema_version: MEMORYDB_SNAPSHOT_SCHEMA_VERSION,
        accounts: legacy
            .accounts
            .map_into(|account, st| st.into_account(account, &region, &endpoint)),
    })
}

pub async fn save_snapshot(
    state: &SharedMemoryDbState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = MemoryDbSnapshot {
        schema_version: MEMORYDB_SNAPSHOT_SCHEMA_VERSION,
        accounts: state.read().clone(),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write memorydb snapshot"),
        Err(err) => tracing::error!(%err, "memorydb snapshot task panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{MemoryDbAccount, MemoryDbAccountsExt};
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Mutex;

    struct MemStore(Mutex<Option<Vec<u8>>>);
    impl SnapshotStore for MemStore {
        fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
            *self.0.lock().unwrap() = Some(bytes.to_vec());
            Ok(())
        }
    }

    fn state() -> SharedMemoryDbState {
        Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        )))
    }

    #[test]
    fn empty_store_is_empty() {
        assert_eq!(
            load_into(&MemStore(Mutex::new(None)), &state()).unwrap(),
            LoadOutcome::Empty
        );
    }

    #[test]
    fn round_trip_restores_accounts() {
        let mut accounts: MultiAccountState<MemoryDbAccount> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        accounts.region_state_mut("111122223333", "eu-west-1");
        let snap = MemoryDbSnapshot {
            schema_version: MEMORYDB_SNAPSHOT_SCHEMA_VERSION,
            accounts,
        };
        let store = MemStore(Mutex::new(Some(serde_json::to_vec(&snap).unwrap())));
        assert_eq!(load_into(&store, &state()).unwrap(), LoadOutcome::Loaded(2));
    }

    #[test]
    fn rejects_future_schema() {
        let accounts: MultiAccountState<MemoryDbAccount> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": MEMORYDB_SNAPSHOT_SCHEMA_VERSION + 1,
            "accounts": accounts,
        }))
        .unwrap();
        let store = MemStore(Mutex::new(Some(bytes)));
        assert!(matches!(
            load_into(&store, &state()),
            Err(LoadError::SchemaTooNew { .. })
        ));
    }

    #[test]
    fn v1_snapshot_splits_resources_by_arn_region() {
        let tag = |k: &str| serde_json::json!({ k: "v" });
        let pg = |name: &str, region: &str| {
            serde_json::json!({
                "name": name, "family": "memorydb_redis7", "description": "d",
                "arn": format!("arn:aws:memorydb:{region}:000000000000:parametergroup/{name}"),
                "parameters": {},
            })
        };
        let east_arn = "arn:aws:memorydb:us-east-1:000000000000:parametergroup/east-pg";
        let west_arn = "arn:aws:memorydb:eu-west-1:000000000000:parametergroup/west-pg";
        let mrc_arn = "arn:aws:memorydb:us-east-1:000000000000:multiregioncluster/virxk-m";
        let v1 = serde_json::json!({
            "schema_version": 1,
            "accounts": {
                "default_account_id": "000000000000",
                "region": "us-east-1",
                "endpoint": "",
                "accounts": {
                    "000000000000": {
                        "clusters": {}, "acls": {}, "users": {}, "subnet_groups": {},
                        "snapshots": {}, "reserved_nodes": {},
                        "parameter_groups": {
                            "east-pg": pg("east-pg", "us-east-1"),
                            "west-pg": pg("west-pg", "eu-west-1"),
                        },
                        "multi_region_clusters": {
                            "virxk-m": {
                                "name": "virxk-m", "description": null, "status": "available",
                                "node_type": "db.r7g.large", "engine": "valkey",
                                "engine_version": "7.2", "number_of_shards": 1,
                                "multi_region_parameter_group_name": null,
                                "tls_enabled": true, "arn": mrc_arn, "clusters": [],
                            }
                        },
                        "tags": { east_arn: tag("e"), west_arn: tag("w"), mrc_arn: tag("m") },
                    }
                }
            }
        });
        let store = MemStore(Mutex::new(Some(serde_json::to_vec(&v1).unwrap())));
        let st = state();
        assert_eq!(load_into(&store, &st).unwrap(), LoadOutcome::Loaded(1));
        let accounts = st.read();
        let east = accounts.region_state("000000000000", "us-east-1");
        let west = accounts.region_state("000000000000", "eu-west-1");
        assert!(east.parameter_groups.contains_key("east-pg"));
        assert!(!east.parameter_groups.contains_key("west-pg"));
        assert!(west.parameter_groups.contains_key("west-pg"));
        assert!(!west.parameter_groups.contains_key("east-pg"));
        assert!(east.tags.contains_key(east_arn) && !east.tags.contains_key(west_arn));
        assert!(west.tags.contains_key(west_arn));
        // Both regions carry their own defaults.
        assert_eq!(
            west.users["default"].arn,
            "arn:aws:memorydb:eu-west-1:000000000000:user/default"
        );
        // The multi-region cluster and its tags stay account-wide.
        let account = accounts.get("000000000000").unwrap();
        assert!(account.multi_region_clusters.contains_key("virxk-m"));
        assert!(account.multi_region_cluster_tags.contains_key(mrc_arn));
        assert!(!east.tags.contains_key(mrc_arn));
    }
}
