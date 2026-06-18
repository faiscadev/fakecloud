use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

/// A type-erased, owned closure that persists one service's whole state as a
/// snapshot when invoked. Built by a service from its own `state` / store /
/// serializing lock (see each service's `snapshot_hook()`), so the
/// serialization stays in the owning crate.
///
/// The CloudFormation resource provisioner mutates services' shared state
/// directly and cannot reach their private `save_snapshot()` paths. After a
/// stack op it invokes the hook for each touched service to write that state
/// through to disk -- the same persistence a direct API mutation would
/// trigger. A `None` hook (memory mode / no store) is simply never collected,
/// so invoking a present hook is always a real persist.
pub type SnapshotHook = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Generic opaque-blob snapshot store used by services that persist their
/// whole state as a single serialized document (DynamoDB tables, SQS queues,
/// etc.). Unlike the fine-grained [`crate::s3::S3Store`] which tracks
/// individual objects and streams bodies to disk, this trait is designed for
/// services whose state is small enough to fit in memory and can be written
/// as one atomic file.
pub trait SnapshotStore: Send + Sync {
    /// Load the latest snapshot, if one exists. Returns `Ok(None)` when
    /// there is nothing on disk yet (first boot).
    fn load(&self) -> io::Result<Option<Vec<u8>>>;

    /// Persist the given bytes as the new snapshot. Implementations must
    /// ensure the write is atomic (crash-safe) and durable.
    fn save(&self, bytes: &[u8]) -> io::Result<()>;
}

/// No-op store used in `StorageMode::Memory`. `load` always returns `None`
/// and `save` is a noop.
pub struct MemorySnapshotStore;

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemorySnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotStore for MemorySnapshotStore {
    fn load(&self) -> io::Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn save(&self, _bytes: &[u8]) -> io::Result<()> {
        Ok(())
    }
}

/// Disk-backed snapshot store. Writes are atomic via the `.tmp` + rename
/// dance in [`crate::atomic::write_atomic_bytes`], with the parent directory
/// fsynced on success.
pub struct DiskSnapshotStore {
    path: PathBuf,
}

impl DiskSnapshotStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl SnapshotStore for DiskSnapshotStore {
    fn load(&self) -> io::Result<Option<Vec<u8>>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn save(&self, bytes: &[u8]) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        crate::atomic::write_atomic_bytes(&self.path, bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_is_noop() {
        let store = MemorySnapshotStore::new();
        assert!(store.load().unwrap().is_none());
        store.save(b"anything").unwrap();
        assert!(store.load().unwrap().is_none());
    }

    #[test]
    fn disk_store_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DiskSnapshotStore::new(tmp.path().join("sub/dir/snapshot.json"));
        assert!(store.load().unwrap().is_none());
        store.save(b"hello world").unwrap();
        assert_eq!(store.load().unwrap().unwrap(), b"hello world");
        store.save(b"second write").unwrap();
        assert_eq!(store.load().unwrap().unwrap(), b"second write");
    }
}
