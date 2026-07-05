//! Snapshot persistence for AWS Config state.

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_persistence::SnapshotStore;

use crate::state::{ConfigSnapshot, SharedConfigState, CONFIG_SNAPSHOT_SCHEMA_VERSION};

/// Persist the current Config state as a snapshot. Offloads serde + the
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by the service's mutating-action hook and the
/// CloudFormation provisioner persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_config_snapshot(
    state: &SharedConfigState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = ConfigSnapshot {
        schema_version: CONFIG_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write config snapshot"),
        Err(err) => tracing::error!(%err, "config snapshot task panicked"),
    }
}
