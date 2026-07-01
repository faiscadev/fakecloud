//! Snapshot save/load helpers for the DSQL control-plane state.

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_persistence::SnapshotStore;

use crate::state::{DsqlSnapshot, SharedDsqlState, DSQL_SNAPSHOT_SCHEMA_VERSION};

#[derive(Debug, PartialEq, Eq)]
pub enum LoadOutcome {
    /// No snapshot file on disk; start with fresh state.
    Empty,
    /// Snapshot loaded successfully; returns the restored account count.
    Loaded(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("failed to read dsql persistence snapshot: {0}")]
    Io(String),
    #[error("failed to parse dsql persistence snapshot: {0}")]
    Parse(String),
    #[error("dsql persistence schema too new: on-disk={on_disk}, max supported={supported}")]
    SchemaTooNew { on_disk: u32, supported: u32 },
}

/// Load a snapshot into `state`. `Empty` when nothing is saved, `Loaded(n)`
/// after a successful restore, or a descriptive error for a fatal startup log.
pub fn load_into(
    store: &dyn SnapshotStore,
    state: &SharedDsqlState,
) -> Result<LoadOutcome, LoadError> {
    let Some(bytes) = store.load().map_err(|e| LoadError::Io(e.to_string()))? else {
        return Ok(LoadOutcome::Empty);
    };
    let snapshot: DsqlSnapshot =
        serde_json::from_slice(&bytes).map_err(|e| LoadError::Parse(e.to_string()))?;
    if snapshot.schema_version > DSQL_SNAPSHOT_SCHEMA_VERSION {
        return Err(LoadError::SchemaTooNew {
            on_disk: snapshot.schema_version,
            supported: DSQL_SNAPSHOT_SCHEMA_VERSION,
        });
    }
    let accounts = snapshot.accounts.account_count();
    *state.write() = snapshot.accounts;
    Ok(LoadOutcome::Loaded(accounts))
}

/// Serialize and persist the current DSQL state. No-op when there is no store
/// (memory mode). Serialized off the async runtime to avoid blocking.
pub async fn save_snapshot(
    state: &SharedDsqlState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = DsqlSnapshot {
        schema_version: DSQL_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write dsql snapshot"),
        Err(err) => tracing::error!(%err, "dsql snapshot task panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{DsqlSnapshot, DsqlState};
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Mutex;

    fn make_state() -> SharedDsqlState {
        Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        )))
    }

    struct MemStore {
        data: Mutex<Option<Vec<u8>>>,
    }
    impl MemStore {
        fn new(data: Option<Vec<u8>>) -> Self {
            Self {
                data: Mutex::new(data),
            }
        }
    }
    impl SnapshotStore for MemStore {
        fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.data.lock().unwrap().clone())
        }
        fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
            *self.data.lock().unwrap() = Some(bytes.to_vec());
            Ok(())
        }
    }

    #[test]
    fn load_into_empty_returns_empty() {
        let state = make_state();
        let store = MemStore::new(None);
        assert_eq!(load_into(&store, &state).unwrap(), LoadOutcome::Empty);
    }

    #[test]
    fn load_into_valid_snapshot_restores_accounts() {
        let mut accounts: MultiAccountState<DsqlState> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        accounts.get_or_create("111122223333");
        let snap = DsqlSnapshot {
            schema_version: DSQL_SNAPSHOT_SCHEMA_VERSION,
            accounts,
        };
        let bytes = serde_json::to_vec(&snap).unwrap();
        let state = make_state();
        let store = MemStore::new(Some(bytes));
        assert_eq!(load_into(&store, &state).unwrap(), LoadOutcome::Loaded(2));
    }

    #[test]
    fn load_into_rejects_future_schema() {
        let mut accounts: MultiAccountState<DsqlState> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        accounts.get_or_create("111122223333");
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": DSQL_SNAPSHOT_SCHEMA_VERSION + 1,
            "accounts": accounts,
        }))
        .unwrap();
        let state = make_state();
        let store = MemStore::new(Some(bytes));
        assert!(matches!(
            load_into(&store, &state),
            Err(LoadError::SchemaTooNew { .. })
        ));
    }
}
