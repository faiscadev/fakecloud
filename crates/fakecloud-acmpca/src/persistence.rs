//! Snapshot persistence for ACM Private CA state.

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_persistence::SnapshotStore;

use crate::state::{AcmPcaSnapshot, SharedAcmPcaState, ACM_PCA_SNAPSHOT_SCHEMA_VERSION};

/// Persist the current ACM PCA state as a snapshot. Offloads serde + the
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by the service's mutating-action hook and the
/// CloudFormation provisioner persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_acmpca_snapshot(
    state: &SharedAcmPcaState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = AcmPcaSnapshot {
        schema_version: ACM_PCA_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write acm-pca snapshot"),
        Err(err) => tracing::error!(%err, "acm-pca snapshot task panicked"),
    }
}
