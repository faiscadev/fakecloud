//! Snapshot save/load for Amazon Comprehend state, with lifecycle
//! reconciliation on restart.

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_persistence::SnapshotStore;

use crate::state::{ComprehendSnapshot, SharedComprehendState, COMPREHEND_SNAPSHOT_SCHEMA_VERSION};

#[derive(Debug, PartialEq, Eq)]
pub enum LoadOutcome {
    Empty,
    Loaded(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("failed to read comprehend persistence snapshot: {0}")]
    Io(String),
    #[error("failed to parse comprehend persistence snapshot: {0}")]
    Parse(String),
    #[error("comprehend persistence schema too new: on-disk={on_disk}, max supported={supported}")]
    SchemaTooNew { on_disk: u32, supported: u32 },
}

pub fn load_into(
    store: &dyn SnapshotStore,
    state: &SharedComprehendState,
) -> Result<LoadOutcome, LoadError> {
    let Some(bytes) = store.load().map_err(|e| LoadError::Io(e.to_string()))? else {
        return Ok(LoadOutcome::Empty);
    };
    let mut snapshot: ComprehendSnapshot =
        serde_json::from_slice(&bytes).map_err(|e| LoadError::Parse(e.to_string()))?;
    if snapshot.schema_version > COMPREHEND_SNAPSHOT_SCHEMA_VERSION {
        return Err(LoadError::SchemaTooNew {
            on_disk: snapshot.schema_version,
            supported: COMPREHEND_SNAPSHOT_SCHEMA_VERSION,
        });
    }
    // Reconcile any lifecycle transition that was in flight when the process
    // stopped: it advances toward its settled state (there is no timer to
    // resume).
    for (_account_id, account) in snapshot.accounts.iter_mut() {
        account.reconcile();
    }
    let accounts = snapshot.accounts.account_count();
    *state.write() = snapshot.accounts;
    Ok(LoadOutcome::Loaded(accounts))
}

pub async fn save_snapshot(
    state: &SharedComprehendState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = ComprehendSnapshot {
        schema_version: COMPREHEND_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write comprehend snapshot"),
        Err(err) => tracing::error!(%err, "comprehend snapshot task panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ComprehendData;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use serde_json::json;
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

    fn state() -> SharedComprehendState {
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
        let mut accounts: MultiAccountState<ComprehendData> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let data = accounts.get_or_create("111122223333");
        data.jobs.insert(
            "j1".into(),
            json!({ "JobId": "j1", "JobStatus": "SUBMITTED" }),
        );
        data.job_families.insert("j1".into(), "sentiment".into());
        let snap = ComprehendSnapshot {
            schema_version: COMPREHEND_SNAPSHOT_SCHEMA_VERSION,
            accounts,
        };
        let store = MemStore(Mutex::new(Some(serde_json::to_vec(&snap).unwrap())));
        let restored = state();
        assert_eq!(
            load_into(&store, &restored).unwrap(),
            LoadOutcome::Loaded(2)
        );
        let guard = restored.read();
        let job = &guard.get("111122223333").unwrap().jobs["j1"];
        assert_eq!(job["JobStatus"], "COMPLETED");
    }

    #[test]
    fn rejects_future_schema() {
        let accounts: MultiAccountState<ComprehendData> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": COMPREHEND_SNAPSHOT_SCHEMA_VERSION + 1,
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
