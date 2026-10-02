//! Shared IAM snapshot persistence.
//!
//! Both `IamService` and `StsService` operate on the same `SharedIamState`
//! and therefore share a single on-disk snapshot. The save routine and the
//! serializing Mutex live here so both services route through the same
//! critical section.

use std::sync::Arc;

use fakecloud_persistence::SnapshotStore;
use tokio::sync::Mutex as AsyncMutex;

use crate::state::{IamSnapshot, SharedIamState, IAM_SNAPSHOT_SCHEMA_VERSION};

/// Serializes concurrent snapshot writes across both IAM and STS services.
/// Without it, two tasks could clone state under the RwLock, serialize
/// independently, and race on `store.save()`, leaving older bytes as the
/// final on-disk state.
pub type IamSnapshotLock = Arc<AsyncMutex<()>>;

pub fn new_snapshot_lock() -> IamSnapshotLock {
    Arc::new(AsyncMutex::new(()))
}

/// Persist the current IAM state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool so the async runtime
/// stays responsive.
///
/// Noop when `store` is `None` (memory mode).
pub async fn save_iam_snapshot(
    state: &SharedIamState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &IamSnapshotLock,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    // Don't persist STS credentials long past expiry.
    let accounts = {
        let mut accounts = state.read().clone();
        let now = chrono::Utc::now();
        for (_, account_state) in accounts.iter_mut() {
            account_state.prune_expired_sts_credentials(now);
        }
        accounts
    };
    let snapshot = IamSnapshot {
        schema_version: IAM_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(accounts),
        state: None,
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write iam snapshot"),
        Err(err) => tracing::error!(%err, "iam snapshot task panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::IamState;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_persistence::DiskSnapshotStore;
    use parking_lot::RwLock;

    fn shared_state() -> SharedIamState {
        let multi: MultiAccountState<IamState> =
            MultiAccountState::new("123456789012", "us-east-1", "http://localhost:4566");
        std::sync::Arc::new(RwLock::new(multi))
    }

    #[test]
    fn new_snapshot_lock_returns_arc_mutex() {
        let lock: IamSnapshotLock = new_snapshot_lock();
        let _c = lock.clone();
    }

    #[tokio::test]
    async fn save_snapshot_none_store_is_noop() {
        let state = shared_state();
        let lock = new_snapshot_lock();
        save_iam_snapshot(&state, None, &lock).await;
    }

    #[tokio::test]
    async fn save_snapshot_writes_bytes_to_disk_store() {
        let state = shared_state();
        let lock = new_snapshot_lock();
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iam.json");
        let store: std::sync::Arc<dyn SnapshotStore> =
            std::sync::Arc::new(DiskSnapshotStore::new(path.clone()));
        save_iam_snapshot(&state, Some(store), &lock).await;
        let bytes = std::fs::read(&path).unwrap();
        assert!(!bytes.is_empty());
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["schema_version"], IAM_SNAPSHOT_SCHEMA_VERSION);
    }
}
