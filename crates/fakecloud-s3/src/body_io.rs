//! Helpers that keep slow object-body IO out from under the global S3 state
//! lock.
//!
//! The S3 state (`SharedS3State`) is one process-wide `RwLock`. Holding it
//! while reading or writing a multi-megabyte body from disk stalls every
//! other S3 request (and every service that delivers into S3) for the
//! duration of the IO. The two halves of the pattern used here:
//!
//! * **Reads**: resolve the object and open its body under the guard
//!   ([`ObjectBodyHandle::open`]), drop the guard, then read. On unix an open
//!   fd keeps the inode alive even if a concurrent PUT/DELETE renames over or
//!   unlinks the path after the guard is released, so the reader sees exactly
//!   the body that was current when it resolved the key.
//! * **Writes**: spool the payload to a tempfile in the store's spool
//!   directory ([`SpooledBody::spool`]) before taking the guard, so the store
//!   write under the guard is a metadata-only `rename(2)` instead of a full
//!   body copy. Memory-mode stores have no spool directory; the payload stays
//!   in RAM and the store write is a cheap refcount clone.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use bytes::Bytes;
use fakecloud_persistence::{BodyRef, BodySource, S3Store};

/// An object body resolved under the S3 state guard, readable after the
/// guard is dropped.
#[derive(Debug)]
pub enum ObjectBodyHandle {
    Memory(Bytes),
    File { file: std::fs::File, size: u64 },
}

impl ObjectBodyHandle {
    /// Resolve `body` into a handle. Call this while still holding the S3
    /// state guard that produced `body`; it only opens a file descriptor, so
    /// it is cheap.
    pub fn open(body: &BodyRef) -> io::Result<Self> {
        match body {
            BodyRef::Memory(b) => Ok(Self::Memory(b.clone())),
            BodyRef::Disk { path, size, .. } => Ok(Self::File {
                file: std::fs::File::open(path)?,
                size: *size,
            }),
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Self::Memory(b) => b.len() as u64,
            Self::File { size, .. } => *size,
        }
    }

    /// Read the whole body. Call with no S3 lock held.
    pub fn read_all(self) -> io::Result<Bytes> {
        match self {
            Self::Memory(b) => Ok(b),
            Self::File { mut file, size } => {
                let mut buf = Vec::with_capacity(size as usize);
                file.read_to_end(&mut buf)?;
                Ok(Bytes::from(buf))
            }
        }
    }

    /// Read `len` bytes starting at `offset` (clamped to the body). Only the
    /// requested range is read from disk. Call with no S3 lock held.
    pub fn read_range(self, offset: u64, len: u64) -> io::Result<Bytes> {
        let total = self.size();
        let start = offset.min(total);
        let end = start.saturating_add(len).min(total);
        match self {
            Self::Memory(b) => Ok(b.slice(start as usize..end as usize)),
            Self::File { mut file, .. } => {
                file.seek(SeekFrom::Start(start))?;
                let mut buf = vec![0u8; (end - start) as usize];
                file.read_exact(&mut buf)?;
                Ok(Bytes::from(buf))
            }
        }
    }
}

/// A payload staged for a store write. Dropping it before the store consumed
/// the spool file removes the file, so an aborted write leaves nothing behind.
#[derive(Debug)]
pub struct SpooledBody {
    path: Option<PathBuf>,
    bytes: Option<Bytes>,
    size: u64,
}

impl SpooledBody {
    /// Stage `bytes` for a later `store.put_object`. With a disk store the
    /// bytes are written to the store's spool directory now (call with no S3
    /// lock held); with a memory store they are kept as-is.
    pub fn spool(store: Option<&dyn S3Store>, bytes: Bytes) -> io::Result<Self> {
        let size = bytes.len() as u64;
        let dir = store.and_then(|s| s.spool_dir());
        let Some(dir) = dir else {
            return Ok(Self {
                path: None,
                bytes: Some(bytes),
                size,
            });
        };
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("staged-{}.tmp", uuid::Uuid::new_v4().simple()));
        let write = (|| {
            let mut f = std::fs::File::create(&path)?;
            f.write_all(&bytes)?;
            f.sync_data()
        })();
        if let Err(err) = write {
            let _ = std::fs::remove_file(&path);
            return Err(err);
        }
        Ok(Self {
            path: Some(path),
            bytes: None,
            size,
        })
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Hand the staged payload to the store. A disk spool file is moved by
    /// the store (`rename(2)`); if the store fails before moving it, the
    /// file is still cleaned up when `self` drops.
    pub fn source(&mut self) -> BodySource {
        if let Some(path) = &self.path {
            BodySource::File(path.clone())
        } else {
            BodySource::Bytes(self.bytes.clone().unwrap_or_default())
        }
    }
}

impl Drop for SpooledBody {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            // After a successful store write the file was renamed away and
            // this is a no-op; otherwise it removes the orphaned spool file.
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_handle_reads_all_and_ranges() {
        let h =
            ObjectBodyHandle::open(&BodyRef::Memory(Bytes::from_static(b"hello world"))).unwrap();
        assert_eq!(h.size(), 11);
        assert_eq!(&h.read_range(6, 100).unwrap()[..], b"world");
        let h = ObjectBodyHandle::open(&BodyRef::Memory(Bytes::from_static(b"abc"))).unwrap();
        assert_eq!(&h.read_all().unwrap()[..], b"abc");
    }

    #[test]
    fn disk_handle_survives_unlink_after_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("obj.bin");
        std::fs::write(&path, b"0123456789").unwrap();
        let body = BodyRef::Disk {
            bucket: "b".into(),
            key: "k".into(),
            version: None,
            path: path.clone(),
            size: 10,
        };
        let h = ObjectBodyHandle::open(&body).unwrap();
        // A concurrent overwrite after the guard is dropped must not change
        // what this reader sees.
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"XXXXXXXXXX").unwrap();
        assert_eq!(&h.read_range(2, 3).unwrap()[..], b"234");
        let h = ObjectBodyHandle::open(&body).unwrap();
        assert_eq!(&h.read_all().unwrap()[..], b"XXXXXXXXXX");
    }

    #[test]
    fn spool_without_store_keeps_bytes_in_memory() {
        let mut s = SpooledBody::spool(None, Bytes::from_static(b"data")).unwrap();
        assert_eq!(s.size(), 4);
        match s.source() {
            BodySource::Bytes(b) => assert_eq!(&b[..], b"data"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn spool_with_disk_store_writes_file_and_cleans_up_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let store = fakecloud_persistence::s3::DiskS3Store::new(
            dir.path().to_path_buf(),
            std::sync::Arc::new(fakecloud_persistence::cache::BodyCache::new(0)),
        );
        let mut s = SpooledBody::spool(Some(&store), Bytes::from_static(b"payload")).unwrap();
        let path = match s.source() {
            BodySource::File(p) => p,
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(std::fs::read(&path).unwrap(), b"payload");
        drop(s);
        assert!(!path.exists(), "unconsumed spool file must be removed");
    }
}
