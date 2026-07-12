//! Snapshot save/load for AWS AppSync state.

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_persistence::SnapshotStore;

use crate::state::{AppSyncSnapshot, SharedAppSyncState, APPSYNC_SNAPSHOT_SCHEMA_VERSION};

#[derive(Debug, PartialEq, Eq)]
pub enum LoadOutcome {
    Empty,
    Loaded(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("failed to read appsync persistence snapshot: {0}")]
    Io(String),
    #[error("failed to parse appsync persistence snapshot: {0}")]
    Parse(String),
    #[error("appsync persistence schema too new: on-disk={on_disk}, max supported={supported}")]
    SchemaTooNew { on_disk: u32, supported: u32 },
}

pub fn load_into(
    store: &dyn SnapshotStore,
    state: &SharedAppSyncState,
) -> Result<LoadOutcome, LoadError> {
    let Some(bytes) = store.load().map_err(|e| LoadError::Io(e.to_string()))? else {
        return Ok(LoadOutcome::Empty);
    };
    let snapshot: AppSyncSnapshot =
        serde_json::from_slice(&bytes).map_err(|e| LoadError::Parse(e.to_string()))?;
    if snapshot.schema_version > APPSYNC_SNAPSHOT_SCHEMA_VERSION {
        return Err(LoadError::SchemaTooNew {
            on_disk: snapshot.schema_version,
            supported: APPSYNC_SNAPSHOT_SCHEMA_VERSION,
        });
    }
    let accounts = snapshot.accounts.account_count();
    *state.write() = snapshot.accounts;
    Ok(LoadOutcome::Loaded(accounts))
}

pub async fn save_snapshot(
    state: &SharedAppSyncState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = AppSyncSnapshot {
        schema_version: APPSYNC_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write appsync snapshot"),
        Err(err) => tracing::error!(%err, "appsync snapshot task panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppSyncData;
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

    fn state() -> SharedAppSyncState {
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
    fn round_trip_restores_apis() {
        let mut accounts: MultiAccountState<AppSyncData> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let data = accounts.get_or_create("111122223333");
        data.graphql_apis
            .insert("abc".to_string(), json!({ "apiId": "abc", "name": "demo" }));
        let snap = AppSyncSnapshot {
            schema_version: APPSYNC_SNAPSHOT_SCHEMA_VERSION,
            accounts,
        };
        let store = MemStore(Mutex::new(Some(serde_json::to_vec(&snap).unwrap())));
        let restored = state();
        assert_eq!(
            load_into(&store, &restored).unwrap(),
            LoadOutcome::Loaded(2)
        );
        let guard = restored.read();
        assert!(guard
            .get("111122223333")
            .unwrap()
            .graphql_apis
            .contains_key("abc"));
    }

    #[test]
    fn rejects_future_schema() {
        let accounts: MultiAccountState<AppSyncData> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": APPSYNC_SNAPSHOT_SCHEMA_VERSION + 1,
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
