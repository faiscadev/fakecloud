//! Snapshot save/load for AWS FIS state, with experiment-lifecycle
//! reconciliation on restart.

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_persistence::SnapshotStore;

use crate::state::{FisSnapshot, SharedFisState, FIS_SNAPSHOT_SCHEMA_VERSION};

#[derive(Debug, PartialEq, Eq)]
pub enum LoadOutcome {
    Empty,
    Loaded(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("failed to read fis persistence snapshot: {0}")]
    Io(String),
    #[error("failed to parse fis persistence snapshot: {0}")]
    Parse(String),
    #[error("fis persistence schema too new: on-disk={on_disk}, max supported={supported}")]
    SchemaTooNew { on_disk: u32, supported: u32 },
}

pub fn load_into(
    store: &dyn SnapshotStore,
    state: &SharedFisState,
) -> Result<LoadOutcome, LoadError> {
    let Some(bytes) = store.load().map_err(|e| LoadError::Io(e.to_string()))? else {
        return Ok(LoadOutcome::Empty);
    };
    let mut snapshot: FisSnapshot =
        serde_json::from_slice(&bytes).map_err(|e| LoadError::Parse(e.to_string()))?;
    if snapshot.schema_version > FIS_SNAPSHOT_SCHEMA_VERSION {
        return Err(LoadError::SchemaTooNew {
            on_disk: snapshot.schema_version,
            supported: FIS_SNAPSHOT_SCHEMA_VERSION,
        });
    }
    // Settle any experiment lifecycle transition that was in flight when the
    // process stopped: initiating/running experiments advance one stage, a
    // stopping experiment settles to stopped (there is no timer to resume).
    for (_account_id, account) in snapshot.accounts.iter_mut() {
        account.reconcile_experiments();
    }
    let accounts = snapshot.accounts.account_count();
    *state.write() = snapshot.accounts;
    Ok(LoadOutcome::Loaded(accounts))
}

pub async fn save_snapshot(
    state: &SharedFisState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = FisSnapshot {
        schema_version: FIS_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write fis snapshot"),
        Err(err) => tracing::error!(%err, "fis snapshot task panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{FisData, FisSnapshot};
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

    fn state() -> SharedFisState {
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
    fn round_trip_restores_and_reconciles_lifecycle() {
        let mut accounts: MultiAccountState<FisData> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let data = accounts.get_or_create("111122223333");
        data.experiments.insert(
            "EXP1".to_string(),
            serde_json::json!({ "id": "EXP1", "state": { "status": "running" } }),
        );
        let snap = FisSnapshot {
            schema_version: FIS_SNAPSHOT_SCHEMA_VERSION,
            accounts,
        };
        let store = MemStore(Mutex::new(Some(serde_json::to_vec(&snap).unwrap())));
        let restored = state();
        assert_eq!(
            load_into(&store, &restored).unwrap(),
            LoadOutcome::Loaded(2)
        );
        let guard = restored.read();
        let e = guard
            .get("111122223333")
            .unwrap()
            .experiments
            .get("EXP1")
            .unwrap();
        assert_eq!(e["state"]["status"], "completed");
    }

    #[test]
    fn rejects_future_schema() {
        let accounts: MultiAccountState<FisData> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": FIS_SNAPSHOT_SCHEMA_VERSION + 1,
            "accounts": accounts,
        }))
        .unwrap();
        let store = MemStore(Mutex::new(Some(bytes)));
        assert!(matches!(
            load_into(&store, &state()),
            Err(LoadError::SchemaTooNew { .. })
        ));
    }
}
