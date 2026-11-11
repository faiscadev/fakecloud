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

/// An object's content as another service reads it: the stored body,
/// opened under the S3 state guard, plus what is needed to undo its SSE-KMS
/// encryption once the guard is dropped.
///
/// Every cross-service read of an object (a DynamoDB import, an Athena scan,
/// a CloudFormation template, the S3 delivery bus, ...) goes through this, so
/// an SSE-KMS object reads as its plaintext everywhere, exactly as GetObject
/// returns it, never as the stored envelope.
pub struct ObjectReader {
    handle: ObjectBodyHandle,
    /// `bucket/key`, for error messages.
    location: String,
    /// Set for an SSE-KMS object: the hook decrypting it, and the bucket
    /// owner, bucket and key id it was encrypted under.
    kms: Option<KmsSource>,
    size: u64,
}

struct KmsSource {
    hook: Option<std::sync::Arc<dyn fakecloud_core::delivery::KmsHook>>,
    owner_account: String,
    bucket: String,
    /// The object's key and whether it uses an S3 Bucket Key: together they
    /// name its encryption context.
    object_key: String,
    bucket_key: bool,
    key_id: Option<String>,
}

impl ObjectReader {
    /// Open `obj`, stored in `bucket` owned by `owner_account`. Call while
    /// holding the S3 state guard `accounts` came from; it only opens a file
    /// descriptor.
    pub fn open(
        accounts: &fakecloud_core::multi_account::MultiAccountState<crate::state::S3State>,
        owner_account: &str,
        bucket: &str,
        obj: &crate::state::S3Object,
    ) -> io::Result<Self> {
        let kms =
            crate::side_effects::is_sse_kms(obj.sse_algorithm.as_deref()).then(|| KmsSource {
                hook: crate::side_effects::installed(accounts, owner_account)
                    .and_then(|e| e.kms_hook.clone()),
                owner_account: owner_account.to_string(),
                bucket: bucket.to_string(),
                object_key: obj.key.clone(),
                bucket_key: obj.bucket_key_enabled == Some(true),
                key_id: obj.sse_kms_key_id.clone(),
            });
        Ok(Self {
            handle: ObjectBodyHandle::open(&obj.body)?,
            location: format!("{bucket}/{}", obj.key),
            kms,
            size: obj.size,
        })
    }

    /// The object's size: that of its plaintext.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Read the whole object, decrypting an SSE-KMS body. Call with no S3
    /// lock held. A body that cannot be decrypted is an
    /// [`ObjectReadError::Kms`], never returned as its stored bytes.
    pub fn read_all(self) -> Result<Bytes, ObjectReadError> {
        let stored = self.handle.read_all().map_err(|e| ObjectReadError::Io {
            location: self.location.clone(),
            source: e,
        })?;
        match self.kms {
            None => Ok(stored),
            Some(kms) => crate::side_effects::kms_decrypt(
                kms.hook.as_ref(),
                &kms.owner_account,
                &kms.bucket,
                &kms.object_key,
                kms.bucket_key,
                &stored,
                kms.key_id.as_deref(),
                None,
            )
            .map_err(|reason| ObjectReadError::Kms {
                location: self.location,
                key_id: kms.key_id,
                reason: reason.to_string(),
            }),
        }
    }
}

/// Why an object could not be read for another service.
#[derive(Debug)]
pub enum ObjectReadError {
    /// The stored body could not be read.
    Io { location: String, source: io::Error },
    /// The SSE-KMS body could not be decrypted: the key is missing, disabled
    /// or not usable by the reader. On AWS this is an access failure on the
    /// object (`KMS.*` / `AccessDenied`), not a missing object.
    Kms {
        location: String,
        key_id: Option<String>,
        reason: String,
    },
}

impl ObjectReadError {
    /// Whether this is an SSE-KMS decryption failure.
    pub fn is_kms(&self) -> bool {
        matches!(self, Self::Kms { .. })
    }
}

impl std::fmt::Display for ObjectReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { location, source } => {
                write!(f, "failed to read s3://{location}: {source}")
            }
            Self::Kms {
                location,
                key_id,
                reason,
            } => write!(
                f,
                "access denied to s3://{location}: its SSE-KMS key {} could not decrypt it: \
                 {reason}",
                key_id.as_deref().unwrap_or("aws/s3")
            ),
        }
    }
}

impl std::error::Error for ObjectReadError {}

/// Read `bucket/key` (the current version) in `owner_account` as its
/// plaintext, the way another service reads an object. `None` when the
/// bucket or key does not exist or is a delete marker. The body is read
/// after the S3 guard is released.
pub fn read_object(
    state: &crate::state::SharedS3State,
    owner_account: &str,
    bucket: &str,
    key: &str,
) -> Option<Result<Bytes, ObjectReadError>> {
    read_object_version(state, owner_account, bucket, key, None)
}

/// [`read_object`] of a specific version (`None` = the current one): the
/// current object when it carries that id, else the version history.
pub fn read_object_version(
    state: &crate::state::SharedS3State,
    owner_account: &str,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Option<Result<Bytes, ObjectReadError>> {
    let reader = {
        let accounts = state.read();
        let b = accounts.get(owner_account)?.buckets.get(bucket)?;
        let obj = match version_id {
            None => b.objects.get(key),
            Some(vid) => b
                .objects
                .get(key)
                .filter(|o| o.version_id.as_deref() == Some(vid))
                .or_else(|| {
                    b.object_versions
                        .get(key)?
                        .iter()
                        .find(|o| o.version_id.as_deref() == Some(vid))
                }),
        }
        .filter(|o| !o.is_delete_marker)?;
        ObjectReader::open(&accounts, owner_account, bucket, obj).map_err(|e| ObjectReadError::Io {
            location: format!("{bucket}/{key}"),
            source: e,
        })
    };
    Some(reader.and_then(|r| crate::service::run_blocking_io(|| r.read_all())))
}

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
