use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct S3State {
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub buckets: BTreeMap<String, BucketSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BucketSnapshot {
    pub meta: BucketMeta,
    #[serde(default)]
    pub objects: BTreeMap<String, LoadedObject>,
    #[serde(default)]
    pub object_versions: BTreeMap<String, Vec<LoadedObject>>,
    #[serde(default)]
    pub subresources: BTreeMap<String, String>,
    #[serde(default)]
    pub multipart_uploads: BTreeMap<String, LoadedMpu>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LoadedMpu {
    pub init: MpuInit,
    #[serde(default)]
    pub parts: BTreeMap<u32, LoadedPart>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LoadedPart {
    pub meta: UploadPartMeta,
    #[serde(default)]
    pub body: BodyRef,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LoadedObject {
    pub meta: ObjectMeta,
    #[serde(default)]
    pub body: BodyRef,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BucketMeta {
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_time")]
    pub creation_date: DateTime<Utc>,
    #[serde(default)]
    pub region: String,
    /// Account ID that owns this bucket. Persisted since the disk path structure
    /// includes account scoping to support multi-account namespaces where different
    /// accounts can have buckets with the same name.
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub versioning: Option<String>,
    /// MFA-Delete status ("Enabled"/"Disabled") set via PutBucketVersioning.
    #[serde(default)]
    pub mfa_delete: Option<String>,
    #[serde(default)]
    pub acl: Option<String>,
    #[serde(default)]
    pub acl_owner_id: String,
    #[serde(default)]
    pub accelerate_status: Option<String>,
    #[serde(default)]
    pub eventbridge_enabled: bool,
    #[serde(default)]
    pub lifecycle_transition_default_min_size: Option<String>,
    /// Created with the legacy `EU` location constraint (an eu-west-1 bucket
    /// whose GetBucketLocation still reports `EU`).
    #[serde(default)]
    pub legacy_eu_location: bool,
    /// Created with the defaults S3 gives new buckets (ACLs disabled, Block
    /// Public Access on, SSE-S3 with SSE-C blocked). Absent in metadata
    /// written by older builds, which reads as a bucket from before them.
    #[serde(default)]
    pub new_bucket_defaults: bool,
}

fn default_time() -> DateTime<Utc> {
    DateTime::<Utc>::MIN_UTC
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AclGrantSnapshot {
    pub grantee_type: String,
    #[serde(default)]
    pub grantee_id: Option<String>,
    #[serde(default)]
    pub grantee_display_name: Option<String>,
    #[serde(default)]
    pub grantee_uri: Option<String>,
    pub permission: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TagsSnapshot {
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AclSnapshot {
    #[serde(default)]
    pub owner_id: String,
    #[serde(default)]
    pub grants: Vec<AclGrantSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InventorySnapshot {
    #[serde(default)]
    pub configs: BTreeMap<String, String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub content_type: String,
    #[serde(default)]
    pub etag: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default = "default_time")]
    pub last_modified: DateTime<Utc>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub storage_class: String,
    #[serde(default)]
    pub acl_grants: Vec<AclGrantSnapshot>,
    #[serde(default)]
    pub acl_owner_id: Option<String>,
    #[serde(default)]
    pub parts_count: Option<u32>,
    #[serde(default)]
    pub part_sizes: Option<Vec<(u32, u64)>>,
    #[serde(default)]
    pub sse_algorithm: Option<String>,
    #[serde(default)]
    pub sse_kms_key_id: Option<String>,
    #[serde(default)]
    pub bucket_key_enabled: Option<bool>,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub is_delete_marker: bool,
    #[serde(default)]
    pub restore_ongoing: Option<bool>,
    #[serde(default)]
    pub restore_expiry: Option<String>,
    #[serde(default)]
    pub checksum_algorithm: Option<String>,
    #[serde(default)]
    pub checksum_value: Option<String>,
    /// `COMPOSITE` or `FULL_OBJECT`. Absent in state written before checksum
    /// types were tracked; readers derive it from the object then.
    #[serde(default)]
    pub checksum_type: Option<String>,
    /// Per-part additional checksums of a multipart object (part number,
    /// base64 value in the object's `checksum_algorithm`), in part order.
    #[serde(default)]
    pub part_checksums: Option<Vec<(u32, String)>>,
    #[serde(default)]
    pub lock_mode: Option<String>,
    #[serde(default)]
    pub lock_retain_until: Option<DateTime<Utc>>,
    #[serde(default)]
    pub lock_legal_hold: Option<String>,
    /// Named annotations attached to this object version. Base64 is avoided —
    /// the payload rides as raw bytes so a restart restores it verbatim.
    #[serde(default)]
    pub annotations: Vec<AnnotationSnapshot>,
    #[serde(default)]
    pub content_encoding: Option<String>,
    #[serde(default)]
    pub cache_control: Option<String>,
    #[serde(default)]
    pub content_disposition: Option<String>,
    #[serde(default)]
    pub content_language: Option<String>,
    #[serde(default)]
    pub expires: Option<String>,
    #[serde(default)]
    pub website_redirect_location: Option<String>,
    /// `x-amz-replication-status` (`COMPLETED` / `FAILED` / `REPLICA`).
    #[serde(default)]
    pub replication_status: Option<String>,
    /// The file in the object's directory that holds this version's body,
    /// when it is not the default `<version tag>.bin`: a body replaced in
    /// place ([`S3Store::replace_object_body`]) is written to a fresh
    /// generation file that the sidecar then names, so the switch is the one
    /// atomic sidecar rename. Kept by the disk store itself (a sidecar
    /// rewritten through [`S3Store::put_object_meta`] keeps the one already
    /// recorded); callers leave it `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_file: Option<String>,
}

/// One `PutObjectAnnotation` payload, persisted alongside its object so
/// annotations survive a restart like the object body does.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AnnotationSnapshot {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub payload: Vec<u8>,
    #[serde(default)]
    pub etag: String,
    #[serde(default = "default_time")]
    pub last_modified: DateTime<Utc>,
    #[serde(default)]
    pub checksum_algorithm: Option<String>,
    /// Base64 checksum of the payload in `checksum_algorithm`.
    #[serde(default)]
    pub checksum_value: Option<String>,
}

/// The metadata of one annotation, persisted on its own record so writing or
/// removing one annotation touches nothing else: the record lives beside the
/// object version's sidecar and the payload in a sibling `.bin` file.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AnnotationRecord {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub etag: String,
    #[serde(default = "default_time")]
    pub last_modified: DateTime<Utc>,
    #[serde(default)]
    pub checksum_algorithm: Option<String>,
    #[serde(default)]
    pub checksum_value: Option<String>,
}

impl AnnotationRecord {
    fn into_snapshot(self, payload: Vec<u8>) -> AnnotationSnapshot {
        AnnotationSnapshot {
            name: self.name,
            payload,
            etag: self.etag,
            last_modified: self.last_modified,
            checksum_algorithm: self.checksum_algorithm,
            checksum_value: self.checksum_value,
        }
    }

    fn of(snapshot: &AnnotationSnapshot) -> Self {
        Self {
            name: snapshot.name.clone(),
            etag: snapshot.etag.clone(),
            last_modified: snapshot.last_modified,
            checksum_algorithm: snapshot.checksum_algorithm.clone(),
            checksum_value: snapshot.checksum_value.clone(),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MpuInit {
    pub upload_id: String,
    pub key: String,
    #[serde(default = "default_time")]
    pub initiated: DateTime<Utc>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub content_type: String,
    #[serde(default)]
    pub storage_class: String,
    #[serde(default)]
    pub sse_algorithm: Option<String>,
    #[serde(default)]
    pub sse_kms_key_id: Option<String>,
    /// `x-amz-server-side-encryption-bucket-key-enabled` of the upload.
    #[serde(default)]
    pub bucket_key_enabled: Option<bool>,
    #[serde(default)]
    pub tagging: Option<String>,
    #[serde(default)]
    pub acl_grants: Vec<AclGrantSnapshot>,
    #[serde(default)]
    pub checksum_algorithm: Option<String>,
    /// `x-amz-checksum-type` requested at CreateMultipartUpload.
    #[serde(default)]
    pub checksum_type: Option<String>,
    #[serde(default)]
    pub cache_control: Option<String>,
    #[serde(default)]
    pub content_disposition: Option<String>,
    #[serde(default)]
    pub content_language: Option<String>,
    #[serde(default)]
    pub expires: Option<String>,
    /// Object Lock settings requested at CreateMultipartUpload time.
    #[serde(default)]
    pub lock_mode: Option<String>,
    #[serde(default)]
    pub lock_retain_until: Option<DateTime<Utc>>,
    #[serde(default)]
    pub lock_legal_hold: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UploadPartMeta {
    pub part_number: u32,
    pub etag: String,
    pub size: u64,
    #[serde(default = "default_time")]
    pub last_modified: DateTime<Utc>,
    /// The part's additional checksum (algorithm name, base64 value).
    #[serde(default)]
    pub checksum_algorithm: Option<String>,
    #[serde(default)]
    pub checksum_value: Option<String>,
}

/// A multipart part's additional checksum, handed to
/// [`S3Store::mpu_put_part`] so it is persisted with the part.
#[derive(Debug, Clone, Copy)]
pub struct PartChecksum<'a> {
    pub algorithm: &'a str,
    pub value: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketSubresource {
    Tags,
    Lifecycle,
    Cors,
    Policy,
    Notification,
    Logging,
    Website,
    PublicAccessBlock,
    ObjectLock,
    Replication,
    Ownership,
    Inventory,
    Encryption,
    Versioning,
    Acl,
    Accelerate,
    Analytics,
    IntelligentTiering,
    Metrics,
    RequestPayment,
    Abac,
    MetadataConfiguration,
    MetadataTableConfiguration,
}

pub const ALL_SUBRESOURCES: &[BucketSubresource] = &[
    BucketSubresource::Tags,
    BucketSubresource::Lifecycle,
    BucketSubresource::Cors,
    BucketSubresource::Policy,
    BucketSubresource::Notification,
    BucketSubresource::Logging,
    BucketSubresource::Website,
    BucketSubresource::PublicAccessBlock,
    BucketSubresource::ObjectLock,
    BucketSubresource::Replication,
    BucketSubresource::Ownership,
    BucketSubresource::Inventory,
    BucketSubresource::Encryption,
    BucketSubresource::Versioning,
    BucketSubresource::Acl,
    BucketSubresource::Accelerate,
    BucketSubresource::Analytics,
    BucketSubresource::IntelligentTiering,
    BucketSubresource::Metrics,
    BucketSubresource::RequestPayment,
    BucketSubresource::Abac,
    BucketSubresource::MetadataConfiguration,
    BucketSubresource::MetadataTableConfiguration,
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BodyRef {
    #[serde(skip)]
    Memory(Bytes),
    Disk {
        bucket: String,
        key: String,
        #[serde(default)]
        version: Option<String>,
        path: PathBuf,
        size: u64,
    },
}

impl BodyRef {
    pub fn size(&self) -> u64 {
        match self {
            BodyRef::Memory(b) => b.len() as u64,
            BodyRef::Disk { size, .. } => *size,
        }
    }
}

impl Default for BodyRef {
    fn default() -> Self {
        BodyRef::Memory(Bytes::new())
    }
}

#[derive(Debug)]
pub enum BodySource {
    Bytes(Bytes),
    /// Existing disk path that should be *moved* into the destination via
    /// rename (upload-tmp → final) and consumed.
    File(PathBuf),
    /// Existing disk path that should be *copied* into the destination and
    /// left in place. Used by replication so the source object stays
    /// available while the replica is produced.
    FileCopy(PathBuf),
}

/// Materialize a [`BodySource`] into [`Bytes`]. The `File` variant is
/// consumed (file is unlinked after read); `FileCopy` is left in place.
/// Memory-mode stores call this to absorb tempfiles produced by the
/// streaming dispatch path so streaming and non-streaming services share
/// the same wire-shape regardless of `--storage-mode`.
fn read_body_source(body: BodySource) -> StoreResult<Bytes> {
    match body {
        BodySource::Bytes(b) => Ok(b),
        BodySource::File(p) => {
            let bytes = std::fs::read(&p)?;
            // Best-effort: a leftover tempfile is not a correctness
            // problem so the read result wins over the unlink result.
            let _ = std::fs::remove_file(&p);
            Ok(Bytes::from(bytes))
        }
        BodySource::FileCopy(p) => {
            let bytes = std::fs::read(&p)?;
            Ok(Bytes::from(bytes))
        }
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(String),
    #[error("not supported by this store")]
    NotSupported,
    #[error("{0}")]
    Other(String),
}

pub type StoreResult<T> = Result<T, StoreError>;

/// Delete what [`S3Store::detach_bucket`] moved aside.
pub fn remove_detached(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

pub trait S3Store: Send + Sync {
    /// Load all S3 state organized by account. Returns a map of account_id → S3State.
    /// In persistent mode, loads one entry per account directory on disk.
    /// In memory mode, returns an empty map.
    fn load(&self) -> StoreResult<std::collections::BTreeMap<String, S3State>>;

    fn put_bucket_meta(&self, account_id: &str, bucket: &str, meta: &BucketMeta) -> StoreResult<()>;
    fn put_bucket_subresource(
        &self,
        account_id: &str,
        bucket: &str,
        kind: BucketSubresource,
        payload: &str,
    ) -> StoreResult<()>;
    fn delete_bucket_subresource(&self, account_id: &str, bucket: &str, kind: BucketSubresource) -> StoreResult<()>;
    fn delete_bucket(&self, account_id: &str, bucket: &str) -> StoreResult<()>;

    /// Move `bucket`'s files out of the store in one atomic rename and return
    /// where they went, for the caller to delete off the hot path with
    /// [`remove_detached`]. Once this returns the bucket is gone from the
    /// store, so a bucket created under the name right after is never the
    /// thing the later removal deletes. `None` when there was nothing to move
    /// (or the store holds nothing on disk).
    fn detach_bucket(&self, _account_id: &str, _bucket: &str) -> StoreResult<Option<PathBuf>> {
        Ok(None)
    }

    /// Whether the store holds any persisted state for `bucket`.
    ///
    /// A bucket absent from memory can still have files on disk: from a
    /// create or delete that stopped partway, or a reset whose bucket deletion
    /// failed. Memory-only stores hold nothing, hence the default.
    fn bucket_state_exists(&self, _account_id: &str, _bucket: &str) -> bool {
        false
    }

    /// Whether this bucket was REFUSED at load: data still on disk and
    /// recoverable by repairing the one bad file.
    ///
    /// Covers both layers that can refuse one. [`S3Store::load`] records the
    /// buckets whose OBJECTS it could not read (a corrupt object meta, a missing
    /// part body); the layer that parses a bucket's own files reports the rest
    /// through [`S3Store::mark_bucket_load_refused`], since the store reads those
    /// as opaque text and sees nothing wrong.
    ///
    /// Recorded at load, not probed per call, so a caller cannot confuse "the
    /// loader could not read this" with "this is simply not in memory".
    fn bucket_load_refused(&self, _account_id: &str, _bucket: &str) -> bool {
        false
    }

    /// Record that `bucket` could not be loaded, for a reason found ABOVE the
    /// store: its objects read fine, but a sidecar this layer does not parse
    /// (`tags.toml`, `acl.toml`, `inventory.toml`) did not. The caller hydrating
    /// a snapshot reports each such bucket here so it is refused exactly like a
    /// store-level refusal -- otherwise its name looks free and the next
    /// `CreateBucket` clears the directory, objects included.
    fn mark_bucket_load_refused(&self, _account_id: &str, _bucket: &str) {}

    /// Forget that `bucket` was refused at load, because the name now belongs to
    /// a bucket that loaded.
    ///
    /// The refusal is recorded at load and consulted long afterwards, so it has
    /// to be dropped when it stops being true, or a name whose unreadable data
    /// is gone stays refused for the life of the process.
    fn clear_bucket_load_refusal(&self, _account_id: &str, _bucket: &str) -> StoreResult<()> {
        Ok(())
    }

    fn put_object(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        body: BodySource,
        meta: &ObjectMeta,
    ) -> StoreResult<BodyRef>;
    /// Replace the body and metadata of the object version in a slot that
    /// stays the same object: unlike [`Self::put_object`], which starts a new
    /// object in the slot, its annotations are kept. For a change that
    /// rewrites an existing version's bytes (UpdateObjectEncryption resealing
    /// it under another key).
    fn replace_object_body(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        body: BodySource,
        meta: &ObjectMeta,
    ) -> StoreResult<BodyRef>;
    fn put_object_meta(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        meta: &ObjectMeta,
    ) -> StoreResult<()>;
    fn delete_object(&self, account_id: &str, bucket: &str, key: &str, version: Option<&str>) -> StoreResult<()>;
    fn open_object_body(&self, body: &BodyRef) -> StoreResult<Bytes>;

    /// Persist one annotation of an object version (creating or replacing
    /// it) without rewriting the object's metadata or its other annotations.
    /// Memory-only stores keep annotations in the runtime state alone.
    fn put_annotation(
        &self,
        _account_id: &str,
        _bucket: &str,
        _key: &str,
        _version: Option<&str>,
        _record: &AnnotationRecord,
        _payload: &[u8],
    ) -> StoreResult<()> {
        Ok(())
    }

    /// Remove one annotation of an object version. Removing one that is not
    /// stored succeeds.
    fn delete_annotation(
        &self,
        _account_id: &str,
        _bucket: &str,
        _key: &str,
        _version: Option<&str>,
        _name: &str,
    ) -> StoreResult<()> {
        Ok(())
    }

    fn mpu_create(&self, account_id: &str, bucket: &str, upload_id: &str, init: &MpuInit) -> StoreResult<()>;
    /// Persist a multipart-upload part body. Returns the [`BodyRef`] the
    /// service should keep in its in-memory part map: `BodyRef::Disk`
    /// for disk-backed stores (so subsequent reads stream from the
    /// `.bin` file instead of holding the part in RAM), or
    /// `BodyRef::Memory` for memory-mode stores.
    fn mpu_put_part(
        &self,
        account_id: &str,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        body: BodySource,
        etag: &str,
        checksum: Option<PartChecksum<'_>>,
    ) -> StoreResult<BodyRef>;
    /// Where the streaming dispatch path should spool large request
    /// bodies to disk before handing them to [`Self::put_object`] /
    /// [`Self::mpu_put_part`]. Returning `Some` keeps the spool file on
    /// the same filesystem as the final destination so `rename(2)` is a
    /// metadata-only move; returning `None` means "use the system temp
    /// dir, the store will read the file back into RAM and unlink it".
    fn spool_dir(&self) -> Option<std::path::PathBuf> {
        None
    }
    fn mpu_abort(&self, account_id: &str, bucket: &str, upload_id: &str) -> StoreResult<()>;
    fn mpu_complete(
        &self,
        account_id: &str,
        bucket: &str,
        upload_id: &str,
        final_key: &str,
        version: Option<&str>,
        meta: &ObjectMeta,
    ) -> StoreResult<BodyRef>;
}

pub struct MemoryS3Store;

impl MemoryS3Store {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryS3Store {
    fn default() -> Self {
        Self::new()
    }
}

impl S3Store for MemoryS3Store {
    fn load(&self) -> StoreResult<std::collections::BTreeMap<String, S3State>> {
        Ok(std::collections::BTreeMap::new())
    }

    fn put_bucket_meta(&self, _account_id: &str, _bucket: &str, _meta: &BucketMeta) -> StoreResult<()> {
        Ok(())
    }
    fn put_bucket_subresource(
        &self,
        _account_id: &str,
        _bucket: &str,
        _kind: BucketSubresource,
        _payload: &str,
    ) -> StoreResult<()> {
        Ok(())
    }
    fn delete_bucket_subresource(
        &self,
        _account_id: &str,
        _bucket: &str,
        _kind: BucketSubresource,
    ) -> StoreResult<()> {
        Ok(())
    }
    fn delete_bucket(&self, _account_id: &str, _bucket: &str) -> StoreResult<()> {
        Ok(())
    }

    fn put_object(
        &self,
        _account_id: &str,
        _bucket: &str,
        _key: &str,
        _version: Option<&str>,
        body: BodySource,
        _meta: &ObjectMeta,
    ) -> StoreResult<BodyRef> {
        Ok(BodyRef::Memory(read_body_source(body)?))
    }
    /// Annotations live in the runtime state alone, which the caller keeps.
    fn replace_object_body(
        &self,
        _account_id: &str,
        _bucket: &str,
        _key: &str,
        _version: Option<&str>,
        body: BodySource,
        _meta: &ObjectMeta,
    ) -> StoreResult<BodyRef> {
        Ok(BodyRef::Memory(read_body_source(body)?))
    }
    fn put_object_meta(
        &self,
        _account_id: &str,
        _bucket: &str,
        _key: &str,
        _version: Option<&str>,
        _meta: &ObjectMeta,
    ) -> StoreResult<()> {
        Ok(())
    }
    fn delete_object(&self, _account_id: &str, _bucket: &str, _key: &str, _version: Option<&str>) -> StoreResult<()> {
        Ok(())
    }
    fn open_object_body(&self, body: &BodyRef) -> StoreResult<Bytes> {
        match body {
            BodyRef::Memory(b) => Ok(b.clone()),
            BodyRef::Disk { .. } => {
                panic!("MemoryS3Store cannot open Disk-backed BodyRef")
            }
        }
    }

    fn mpu_create(&self, _account_id: &str, _bucket: &str, _upload_id: &str, _init: &MpuInit) -> StoreResult<()> {
        Ok(())
    }
    fn mpu_put_part(
        &self,
        _account_id: &str,
        _bucket: &str,
        _upload_id: &str,
        _part_number: u32,
        body: BodySource,
        _etag: &str,
        _checksum: Option<PartChecksum<'_>>,
    ) -> StoreResult<BodyRef> {
        // Memory mode keeps all part bodies in RAM. Absorb any tempfile
        // produced by the streaming dispatch path here.
        Ok(BodyRef::Memory(read_body_source(body)?))
    }
    fn mpu_abort(&self, _account_id: &str, _bucket: &str, _upload_id: &str) -> StoreResult<()> {
        Ok(())
    }
    fn mpu_complete(
        &self,
        _account_id: &str,
        _bucket: &str,
        _upload_id: &str,
        _final_key: &str,
        _version: Option<&str>,
        _meta: &ObjectMeta,
    ) -> StoreResult<BodyRef> {
        Ok(BodyRef::Memory(Bytes::new()))
    }
}

pub struct DiskS3Store {
    root: PathBuf,
    cache: std::sync::Arc<crate::cache::BodyCache>,
    /// Escaped directory names `load` could not read, so a caller can tell
    /// recoverable data apart from state the operator already discarded.
    load_refused: parking_lot::RwLock<std::collections::HashSet<String>>,
}

impl DiskS3Store {
    pub fn new(root: PathBuf, cache: std::sync::Arc<crate::cache::BodyCache>) -> Self {
        Self {
            root,
            cache,
            load_refused: parking_lot::RwLock::new(std::collections::HashSet::new()),
        }
    }

    fn accounts_dir(&self) -> PathBuf {
        self.root.join("accounts")
    }

    /// Where [`S3Store::detach_bucket`] moves buckets awaiting removal. A
    /// sibling of the accounts directory, so the loader never sees it and the
    /// rename stays on one filesystem.
    fn trash_dir(&self) -> PathBuf {
        self.root.join(".trash")
    }

    fn bucket_dir(&self, account_id: &str, bucket: &str) -> PathBuf {
        self.accounts_dir()
            .join(account_id)
            .join("buckets")
            .join(crate::key_escape::escape_key_segment(bucket))
    }

    fn object_dir(&self, account_id: &str, bucket: &str, key: &str) -> PathBuf {
        self.bucket_dir(account_id, bucket)
            .join("objects")
            .join(crate::key_escape::escape_key_segment(key))
    }

    fn version_tag(version: Option<&str>) -> String {
        version.unwrap_or("null").to_string()
    }

    fn object_paths(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        version: Option<&str>,
    ) -> (PathBuf, PathBuf, PathBuf) {
        let dir = self.object_dir(account_id, bucket, key);
        let tag = Self::version_tag(version);
        let bin = dir.join(format!("{}.bin", tag));
        let toml = dir.join(format!("{}.toml", tag));
        (dir, bin, toml)
    }

    /// The marker between a version tag and the generation of a body file a
    /// replace wrote: `<version tag>.gen-<generation>.bin`.
    const GENERATION_MARKER: &'static str = ".gen-";

    /// A fresh generation body file name for `tag`, unique in the process
    /// (time and a counter) and across restarts (the time).
    fn generation_body_name(tag: &str) -> String {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!(
            "{tag}{}{nanos:x}-{:x}-{n:x}.bin",
            Self::GENERATION_MARKER,
            std::process::id()
        )
    }

    /// The version tag a generation body file belongs to, `None` for any
    /// other file (the default `<tag>.bin` among them).
    fn generation_body_tag(file_name: &str) -> Option<&str> {
        if file_name.contains(['/', '\\']) {
            return None;
        }
        let stem = file_name.strip_suffix(".bin")?;
        let (tag, generation) = stem.rsplit_once(Self::GENERATION_MARKER)?;
        (!tag.is_empty()
            && !generation.is_empty()
            && generation
                .bytes()
                .all(|b| b.is_ascii_hexdigit() || b == b'-'))
        .then_some(tag)
    }

    /// Where the body of the version with sidecar `meta` and tag `tag` lives
    /// in `dir`: the generation file the sidecar names, else `<tag>.bin`. A
    /// recorded name that is not one of this tag's generation files (it can
    /// only come from a hand-edited sidecar) is ignored.
    fn body_path_in(dir: &std::path::Path, tag: &str, meta: &ObjectMeta) -> PathBuf {
        match meta.body_file.as_deref() {
            Some(name) if Self::generation_body_tag(name) == Some(tag) => dir.join(name),
            _ => dir.join(format!("{tag}.bin")),
        }
    }

    /// The generation body file the sidecar at `toml_path` (`<tag>.toml`)
    /// names, if any: only one of that tag's generation files counts.
    fn recorded_generation_body(toml_path: &std::path::Path) -> Option<String> {
        let tag = toml_path.file_name()?.to_str()?.strip_suffix(".toml")?;
        let text = std::fs::read_to_string(toml_path).ok()?;
        let meta: ObjectMeta = toml::from_str(&text).ok()?;
        meta.body_file
            .filter(|name| Self::generation_body_tag(name) == Some(tag))
    }

    /// Best-effort removal of a body file the slot no longer uses: once the
    /// sidecar no longer names it, a failure only leaves residue that the
    /// next load removes.
    fn remove_stale_body(path: &std::path::Path) {
        if let Err(e) = std::fs::remove_file(path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "replaced object body could not be removed; the next load removes it",
                );
            }
        }
    }

    /// Remove the temp files ([`crate::atomic::is_temp_file_name`]) directly
    /// in `dir`: writes a crash cut short before their rename.
    fn remove_temp_files_in(dir: &std::path::Path) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(crate::atomic::is_temp_file_name)
                && path.is_file()
            {
                Self::remove_stale_body(&path);
            }
        }
    }

    /// Write a slot's body to `bin_path`, then its sidecar (naming
    /// `body_file`) atomically: the sidecar rename is what makes the new body
    /// the slot's. Returns the body ref.
    #[allow(clippy::too_many_arguments)]
    fn write_slot(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        bin_path: PathBuf,
        toml_path: &std::path::Path,
        body: BodySource,
        meta: &ObjectMeta,
        body_file: Option<String>,
    ) -> StoreResult<BodyRef> {
        let mut meta = meta.clone();
        meta.body_file = body_file;
        if meta.is_delete_marker {
            meta.body_file = None;
            crate::atomic::write_atomic_toml(toml_path, &meta)?;
            return Ok(BodyRef::Memory(Bytes::new()));
        }

        let (size, bytes_for_cache): (u64, Option<Bytes>) = match body {
            BodySource::Bytes(b) => {
                crate::atomic::write_atomic_bytes(&bin_path, &b)?;
                (b.len() as u64, Some(b))
            }
            BodySource::File(src) => {
                let src_size = std::fs::metadata(&src)?.len();
                crate::atomic::write_atomic_from_file(&src, &bin_path)?;
                (src_size, None)
            }
            BodySource::FileCopy(src) => {
                let src_size = std::fs::metadata(&src)?.len();
                crate::atomic::write_atomic_copy_from_file(&src, &bin_path)?;
                (src_size, None)
            }
        };

        crate::atomic::write_atomic_toml(toml_path, &meta)?;

        let body_key = crate::cache::BodyKey::new(
            bucket.to_string(),
            key.to_string(),
            version.map(|s| s.to_string()),
        );
        if let Some(b) = bytes_for_cache {
            self.cache.insert(body_key, b);
        } else {
            self.cache.invalidate(&body_key);
        }

        Ok(BodyRef::Disk {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version: version.map(|s| s.to_string()),
            path: bin_path,
            size,
        })
    }

    /// Where an object version's annotations live: one `<id>.toml` record and
    /// `<id>.bin` payload per annotation, `<id>` the SHA-256 of the name (a
    /// name can be 512 bytes, too long for a file name once escaped).
    fn annotation_dir(&self, account_id: &str, bucket: &str, key: &str, version: Option<&str>) -> PathBuf {
        Self::annotation_dir_in(&self.object_dir(account_id, bucket, key), &Self::version_tag(version))
    }

    fn annotation_dir_in(key_dir: &std::path::Path, version_tag: &str) -> PathBuf {
        key_dir.join(format!("{version_tag}.annotations"))
    }

    fn annotation_file_stem(name: &str) -> String {
        use sha2::Digest;
        sha2::Sha256::digest(name.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn write_annotation(
        dir: &std::path::Path,
        record: &AnnotationRecord,
        payload: &[u8],
    ) -> StoreResult<()> {
        std::fs::create_dir_all(dir)?;
        let stem = Self::annotation_file_stem(&record.name);
        // Payload first: `load` reads a record only together with its payload.
        crate::atomic::write_atomic_bytes(&dir.join(format!("{stem}.bin")), payload)?;
        crate::atomic::write_atomic_toml(&dir.join(format!("{stem}.toml")), record)?;
        Ok(())
    }

    /// Read an object version's annotation records, and move any that an
    /// older build embedded in the version's sidecar out into records of
    /// their own (rewriting the sidecar without them), so a later metadata
    /// write -- which no longer carries annotations -- cannot drop them.
    fn load_annotations(
        key_dir: &std::path::Path,
        version_tag: &str,
        sidecar: &std::path::Path,
        meta: &mut ObjectMeta,
    ) -> StoreResult<()> {
        let dir = Self::annotation_dir_in(key_dir, version_tag);
        let mut loaded: BTreeMap<String, AnnotationSnapshot> = BTreeMap::new();
        if dir.is_dir() {
            for entry in std::fs::read_dir(&dir)? {
                let path = entry?.path();
                if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                    continue;
                }
                let record: AnnotationRecord = toml::from_str(&std::fs::read_to_string(&path)?)
                    .map_err(|e| StoreError::Serde(e.to_string()))?;
                let payload = std::fs::read(path.with_extension("bin"))?;
                loaded.insert(record.name.clone(), record.into_snapshot(payload));
            }
        }
        let legacy = std::mem::take(&mut meta.annotations);
        if !legacy.is_empty() {
            for a in legacy {
                if loaded.contains_key(&a.name) {
                    continue;
                }
                Self::write_annotation(&dir, &AnnotationRecord::of(&a), &a.payload)?;
                loaded.insert(a.name.clone(), a);
            }
            crate::atomic::write_atomic_toml(sidecar, &*meta)?;
        }
        meta.annotations = loaded.into_values().collect();
        Ok(())
    }

    fn subresource_filename(kind: BucketSubresource) -> &'static str {
        match kind {
            BucketSubresource::Tags => "tags.toml",
            BucketSubresource::Lifecycle => "lifecycle.toml",
            BucketSubresource::Cors => "cors.toml",
            BucketSubresource::Policy => "policy.toml",
            BucketSubresource::Notification => "notification.toml",
            BucketSubresource::Logging => "logging.toml",
            BucketSubresource::Website => "website.toml",
            BucketSubresource::PublicAccessBlock => "public_access_block.toml",
            BucketSubresource::ObjectLock => "object_lock.toml",
            BucketSubresource::Replication => "replication.toml",
            BucketSubresource::Ownership => "ownership.toml",
            BucketSubresource::Inventory => "inventory.toml",
            BucketSubresource::Encryption => "encryption.toml",
            BucketSubresource::Versioning => "versioning.toml",
            BucketSubresource::Acl => "acl.toml",
            BucketSubresource::Accelerate => "accelerate.toml",
            BucketSubresource::Analytics => "analytics.toml",
            BucketSubresource::IntelligentTiering => "intelligent_tiering.toml",
            BucketSubresource::Metrics => "metrics.toml",
            BucketSubresource::RequestPayment => "request_payment.toml",
            BucketSubresource::Abac => "abac.toml",
            BucketSubresource::MetadataConfiguration => "metadata_configuration.toml",
            BucketSubresource::MetadataTableConfiguration => "metadata_table_configuration.toml",
        }
    }

    fn cleanup_empty(dir: &std::path::Path) {
        let _ = std::fs::remove_dir(dir);
    }

    fn mpu_dir(&self, account_id: &str, bucket: &str, upload_id: &str) -> PathBuf {
        self.bucket_dir(account_id, bucket)
            .join("mpu")
            .join(crate::key_escape::escape_key_segment(upload_id))
    }

    fn mpu_parts_dir(&self, account_id: &str, bucket: &str, upload_id: &str) -> PathBuf {
        self.mpu_dir(account_id, bucket, upload_id).join("parts")
    }

    fn mpu_part_bin(&self, account_id: &str, bucket: &str, upload_id: &str, part_number: u32) -> PathBuf {
        self.mpu_parts_dir(account_id, bucket, upload_id)
            .join(format!("{}.bin", part_number))
    }

    fn mpu_part_toml(&self, account_id: &str, bucket: &str, upload_id: &str, part_number: u32) -> PathBuf {
        self.mpu_parts_dir(account_id, bucket, upload_id)
            .join(format!("{}.toml", part_number))
    }

    fn mpu_body_key(bucket: &str, upload_id: &str, part_number: u32) -> crate::cache::BodyKey {
        crate::cache::BodyKey::new(
            bucket.to_string(),
            format!("__mpu__/{}", upload_id),
            Some(format!("part-{}", part_number)),
        )
    }
}

fn remove_dir_if_present(dir: &std::path::Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

fn io_other(msg: impl Into<String>) -> StoreError {
    StoreError::Other(msg.into())
}

impl S3Store for DiskS3Store {
    fn load(&self) -> StoreResult<std::collections::BTreeMap<String, S3State>> {
        // This load decides which buckets are refused, so start clean: carrying
        // entries over would keep a bucket whose bad file was repaired
        // un-creatable.
        self.load_refused.write().clear();
        // Buckets detached by a reset the previous process did not finish
        // deleting.
        let _ = std::fs::remove_dir_all(self.trash_dir());
        let mut all_states: BTreeMap<String, S3State> = BTreeMap::new();
        let accounts_dir = self.accounts_dir();

        // Handle migration: if accounts/ doesn't exist but buckets/ does,
        // rename buckets/ to accounts/<primary-account>/buckets/
        if !accounts_dir.exists() {
            let old_buckets_dir = self.root.join("buckets");
            if old_buckets_dir.exists() {
                // This is a migration case. Since we don't have the account_id passed to load(),
                // we'll load old buckets to a default account and require the service layer to
                // run migration to move them to the correct account if needed.
                // Use the CLI account ID environment variable or fakecloud's default account ID.
                let primary_account = std::env::var("AWS_ACCOUNT_ID")
                    .unwrap_or_else(|_| "123456789012".to_string());

                tracing::warn!(
                    "S3 store migration: found legacy buckets/ directory; migrating to accounts/{}/buckets structure",
                    primary_account
                );

                let accounts_path = self.accounts_dir();
                std::fs::create_dir_all(&accounts_path)?;
                let account_buckets_dir = accounts_path.join(&primary_account).join("buckets");
                std::fs::create_dir_all(account_buckets_dir.parent().unwrap())?;

                // Move old buckets to new structure: s3/buckets/* -> s3/accounts/<account>/buckets/*
                match std::fs::rename(&old_buckets_dir, &account_buckets_dir) {
                    Ok(_) => {
                        tracing::info!(
                            "S3 store migration: successfully migrated legacy buckets to account {}",
                            primary_account
                        );
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        // Already migrated; fall through to load it
                        tracing::debug!("S3 store migration: destination already exists");
                    }
                    Err(e) => return Err(StoreError::from(e)),
                }
            } else {
                return Ok(all_states);
            }
        }

        // Load each account's buckets
        for account_entry in std::fs::read_dir(&accounts_dir)? {
            let account_entry = account_entry?;
            if !account_entry.file_type()?.is_dir() {
                continue;
            }
            let account_path = account_entry.path();
            let Some(account_id) = account_path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let account_id = account_id.to_string();

            let mut state = S3State::default();
            state.account_id = account_id.clone();

            let buckets_dir = account_path.join("buckets");
            if !buckets_dir.exists() {
                all_states.insert(account_id, state);
                continue;
            }

            for entry in std::fs::read_dir(&buckets_dir)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let bdir = entry.path();
                // Isolate per-bucket load so one corrupt/partial bucket is skipped
                // with a warning instead of aborting the whole store load -- a single
                // truncated file used to make every bucket inaccessible (bug-audit
                // 2026-06-20, 4.3).
                let loaded: StoreResult<Option<BucketSnapshot>> = (|| {
                let meta_path = bdir.join("meta.toml");
                if !meta_path.exists() {
                    return Ok(None);
                }
                let meta_text = std::fs::read_to_string(&meta_path)?;
                let mut meta: BucketMeta =
                    toml::from_str(&meta_text).map_err(|e| StoreError::Serde(e.to_string()))?;
                let mut snap = BucketSnapshot {
                    meta: meta.clone(),
                    objects: BTreeMap::new(),
                    object_versions: BTreeMap::new(),
                    subresources: BTreeMap::new(),
                    multipart_uploads: BTreeMap::new(),
                };

                for kind in ALL_SUBRESOURCES {
                    let fname = Self::subresource_filename(*kind);
                    let path = bdir.join(fname);
                    if path.exists() {
                        let text = std::fs::read_to_string(&path)?;
                        if *kind == BucketSubresource::Versioning && snap.meta.versioning.is_none()
                        {
                            let stripped = text.trim();
                            if !stripped.is_empty() {
                                snap.meta.versioning = Some(stripped.to_string());
                                meta.versioning = snap.meta.versioning.clone();
                            }
                        }
                        snap.subresources.insert(fname.to_string(), text);
                    }
                }

                let objects_root = bdir.join("objects");
                if objects_root.exists() {
                    for okey_entry in std::fs::read_dir(&objects_root)? {
                        let okey_entry = okey_entry?;
                        if !okey_entry.file_type()?.is_dir() {
                            continue;
                        }
                        let key_dir = okey_entry.path();
                        let mut versioned: Vec<LoadedObject> = Vec::new();
                        let mut key_name: Option<String> = None;
                        // Listed up front: loading annotations can rewrite a
                        // sidecar in this directory, and a directory changed
                        // mid-listing may list an entry twice.
                        let version_entries: Vec<std::fs::DirEntry> =
                            std::fs::read_dir(&key_dir)?.collect::<Result<_, _>>()?;
                        // Body files the sidecars name, for clearing what a
                        // replace interrupted or could not remove.
                        let mut referenced: std::collections::HashSet<String> =
                            std::collections::HashSet::new();
                        let mut superseded_defaults: Vec<PathBuf> = Vec::new();
                        for version_entry in &version_entries {
                            let path = version_entry.path();
                            let Some(fname) = path.file_name().and_then(|s| s.to_str()) else {
                                continue;
                            };
                            if !fname.ends_with(".toml") {
                                continue;
                            }
                            let version_tag = &fname[..fname.len() - 5];
                            let toml_text = std::fs::read_to_string(&path)?;
                            let mut obj_meta: ObjectMeta = toml::from_str(&toml_text)
                                .map_err(|e| StoreError::Serde(e.to_string()))?;
                            Self::load_annotations(&key_dir, version_tag, &path, &mut obj_meta)?;
                            let bin_path = Self::body_path_in(&key_dir, version_tag, &obj_meta);
                            if let Some(name) = bin_path.file_name().and_then(|n| n.to_str()) {
                                if Self::generation_body_tag(name).is_some() {
                                    referenced.insert(name.to_string());
                                    // The sidecar names a generation file:
                                    // the default one is no longer this
                                    // version's body.
                                    superseded_defaults
                                        .push(key_dir.join(format!("{version_tag}.bin")));
                                }
                            }
                            let (body, size) = if obj_meta.is_delete_marker {
                                (BodyRef::Memory(Bytes::new()), 0u64)
                            } else if bin_path.exists() {
                                let sz = std::fs::metadata(&bin_path)?.len();
                                (
                                    BodyRef::Disk {
                                        bucket: meta.name.clone(),
                                        key: obj_meta.key.clone(),
                                        version: if version_tag == "null" {
                                            None
                                        } else {
                                            Some(version_tag.to_string())
                                        },
                                        path: bin_path,
                                        size: sz,
                                    },
                                    sz,
                                )
                            } else {
                                // Fail loud: the sidecar says this object has a
                                // body but the .bin file is missing. Returning
                                // silently would hand the caller a truncated
                                // object and hide data loss.
                                return Err(StoreError::Other(format!(
                                    "missing body file: {}",
                                    bin_path.display()
                                )));
                            };
                            let _ = size;
                            key_name.get_or_insert_with(|| obj_meta.key.clone());
                            if version_tag == "null" && obj_meta.version_id.is_none() {
                                snap.objects.insert(
                                    obj_meta.key.clone(),
                                    LoadedObject {
                                        meta: obj_meta,
                                        body,
                                    },
                                );
                            } else {
                                versioned.push(LoadedObject {
                                    meta: obj_meta,
                                    body,
                                });
                            }
                        }
                        // A generation body no sidecar names was written by a
                        // replace that did not reach its sidecar rename, or
                        // was superseded by one that did; a default body
                        // whose sidecar names a generation file was replaced.
                        // Default files of any other version are never
                        // touched.
                        for entry in &version_entries {
                            let path = entry.path();
                            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                                continue;
                            };
                            if Self::generation_body_tag(name).is_some()
                                && !referenced.contains(name)
                            {
                                Self::remove_stale_body(&path);
                            } else if (crate::atomic::is_temp_file_name(name)
                                || name.ends_with(".bin.tmp"))
                                && path.is_file()
                            {
                                // A write that never reached its rename (a
                                // multipart completion's concatenated body
                                // was `<body>.bin.tmp` before it took a
                                // unique temp name).
                                Self::remove_stale_body(&path);
                            } else if name.ends_with(".annotations") && path.is_dir() {
                                Self::remove_temp_files_in(&path);
                            }
                        }
                        for default in &superseded_defaults {
                            Self::remove_stale_body(default);
                        }
                        if !versioned.is_empty() {
                            versioned.sort_by_key(|v| v.meta.last_modified);
                            if let Some(key) = key_name {
                                // Reconcile snap.objects with the newest version:
                                // a trailing delete marker hides any prior null or
                                // live version (remove it); otherwise overwrite
                                // with the newest live version, even if a
                                // pre-versioning null.toml had already been
                                // inserted during the non-versioned scan.
                                match versioned.last() {
                                    Some(newest) if newest.meta.is_delete_marker => {
                                        snap.objects.remove(&key);
                                    }
                                    Some(newest) => {
                                        snap.objects.insert(key.clone(), newest.clone());
                                    }
                                    None => {}
                                }
                                snap.object_versions.insert(key, versioned);
                            }
                        }
                    }
                }

                let mpu_root = bdir.join("mpu");
                if mpu_root.exists() {
                    for upload_entry in std::fs::read_dir(&mpu_root)? {
                        let upload_entry = upload_entry?;
                        if !upload_entry.file_type()?.is_dir() {
                            continue;
                        }
                        let upload_dir = upload_entry.path();
                        let init_path = upload_dir.join("init.toml");
                        if !init_path.exists() {
                            continue;
                        }
                        let init_text = std::fs::read_to_string(&init_path)?;
                        let init: MpuInit = toml::from_str(&init_text)
                            .map_err(|e| StoreError::Serde(e.to_string()))?;
                        let mut loaded_parts: BTreeMap<u32, LoadedPart> = BTreeMap::new();
                        let parts_dir = upload_dir.join("parts");
                        if parts_dir.exists() {
                            for part_entry in std::fs::read_dir(&parts_dir)? {
                                let part_entry = part_entry?;
                                let path = part_entry.path();
                                let Some(fname) = path.file_name().and_then(|s| s.to_str()) else {
                                    continue;
                                };
                                if !fname.ends_with(".toml") {
                                    continue;
                                }
                                let stem = &fname[..fname.len() - 5];
                                let Ok(part_number) = stem.parse::<u32>() else {
                                    continue;
                                };
                                let toml_text = std::fs::read_to_string(&path)?;
                                let part_meta: UploadPartMeta = toml::from_str(&toml_text)
                                    .map_err(|e| StoreError::Serde(e.to_string()))?;
                                let bin_path = parts_dir.join(format!("{}.bin", part_number));
                                if !bin_path.exists() {
                                    return Err(StoreError::Other(format!(
                                        "missing multipart part body file: {}",
                                        bin_path.display()
                                    )));
                                }
                                let sz = std::fs::metadata(&bin_path)?.len();
                                let body = BodyRef::Disk {
                                    bucket: meta.name.clone(),
                                    key: format!("__mpu__/{}", init.upload_id),
                                    version: Some(format!("part-{}", part_number)),
                                    path: bin_path,
                                    size: sz,
                                };
                                loaded_parts.insert(
                                    part_number,
                                    LoadedPart {
                                        meta: part_meta,
                                        body,
                                    },
                                );
                            }
                        }
                        snap.multipart_uploads.insert(
                            init.upload_id.clone(),
                            LoadedMpu {
                                init,
                                parts: loaded_parts,
                            },
                        );
                    }
                }

                Ok(Some(snap))
            })();
                match loaded {
                    Ok(Some(snap)) => {
                        state.buckets.insert(snap.meta.name.clone(), snap);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        // Remembered so a later CreateBucket knows this directory
                        // holds recoverable data rather than state the operator
                        // discarded. Use a composite key of account_id::bucket_name.
                        if let Some(dir_name) = bdir.file_name().and_then(|n| n.to_str()) {
                            self.load_refused
                                .write()
                                .insert(format!("{}::{}", &account_id, dir_name));
                        }
                        tracing::warn!(
                            account = %account_id,
                            bucket = %bdir.display(),
                            error = %e,
                            "skipping unreadable S3 bucket during load; its name is refused until \
                             the bad file is repaired and the server restarted, or the bucket's \
                             directory is removed -- no API call discards it"
                        );
                    }
                }
            }
            all_states.insert(account_id, state);
        }
        Ok(all_states)
    }

    fn put_bucket_meta(&self, account_id: &str, bucket: &str, meta: &BucketMeta) -> StoreResult<()> {
        let dir = self.bucket_dir(account_id, bucket);
        std::fs::create_dir_all(&dir)?;
        crate::atomic::write_atomic_toml(&dir.join("meta.toml"), meta)?;
        Ok(())
    }

    fn put_bucket_subresource(
        &self,
        account_id: &str,
        bucket: &str,
        kind: BucketSubresource,
        payload: &str,
    ) -> StoreResult<()> {
        let dir = self.bucket_dir(account_id, bucket);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(Self::subresource_filename(kind));
        crate::atomic::write_atomic_bytes(&path, payload.as_bytes())?;
        Ok(())
    }

    fn delete_bucket_subresource(&self, account_id: &str, bucket: &str, kind: BucketSubresource) -> StoreResult<()> {
        let path = self
            .bucket_dir(account_id, bucket)
            .join(Self::subresource_filename(kind));
        match std::fs::remove_file(&path) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn bucket_state_exists(&self, account_id: &str, bucket: &str) -> bool {
        self.bucket_dir(account_id, bucket).exists()
    }

    fn bucket_load_refused(&self, account_id: &str, bucket: &str) -> bool {
        let composite_key = format!(
            "{}::{}",
            account_id,
            crate::key_escape::escape_key_segment(bucket)
        );
        self.load_refused.read().contains(&composite_key)
    }

    fn mark_bucket_load_refused(&self, account_id: &str, bucket: &str) {
        let composite_key = format!(
            "{}::{}",
            account_id,
            crate::key_escape::escape_key_segment(bucket)
        );
        self.load_refused.write().insert(composite_key);
    }

    fn clear_bucket_load_refusal(&self, account_id: &str, bucket: &str) -> StoreResult<()> {
        let composite_key = format!(
            "{}::{}",
            account_id,
            crate::key_escape::escape_key_segment(bucket)
        );
        self.load_refused.write().remove(&composite_key);
        Ok(())
    }

    fn detach_bucket(&self, account_id: &str, bucket: &str) -> StoreResult<Option<PathBuf>> {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = self.bucket_dir(account_id, bucket);
        let trash = self.trash_dir();
        std::fs::create_dir_all(&trash)?;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let target = trash.join(format!(
            "{}_{}-{}-{nanos}-{seq}",
            account_id,
            crate::key_escape::escape_key_segment(bucket),
            std::process::id()
        ));
        match std::fs::rename(&dir, &target) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(StoreError::from(e)),
        }
        let composite_key = format!(
            "{}::{}",
            account_id,
            crate::key_escape::escape_key_segment(bucket)
        );
        self.load_refused.write().remove(&composite_key);
        Ok(Some(target))
    }

    fn delete_bucket(&self, account_id: &str, bucket: &str) -> StoreResult<()> {
        let dir = self.bucket_dir(account_id, bucket);
        let outcome = match std::fs::remove_dir_all(&dir) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StoreError::from(e)),
        };
        // Only once the tree is really gone: a removal that stops partway
        // returns Err and the caller reports 500, so dropping the refusal here
        // would leave the next CreateBucket free to discard the remains.
        if outcome.is_ok() {
            let composite_key = format!(
                "{}::{}",
                account_id,
                crate::key_escape::escape_key_segment(bucket)
            );
            self.load_refused.write().remove(&composite_key);
        }
        outcome
    }

    fn put_object(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        body: BodySource,
        meta: &ObjectMeta,
    ) -> StoreResult<BodyRef> {
        let (dir, bin_path, toml_path) = self.object_paths(account_id, bucket, key, version);
        std::fs::create_dir_all(&dir)?;
        // A new object in this slot starts without annotations; the replaced
        // object's go with it.
        remove_dir_if_present(&self.annotation_dir(account_id, bucket, key, version))?;
        // A body a replace left in a generation file stops being the slot's
        // once the new sidecar (naming the default file) is in place.
        let previous = Self::recorded_generation_body(&toml_path);
        let body = self.write_slot(bucket, key, version, bin_path, &toml_path, body, meta, None)?;
        if let Some(previous) = previous {
            Self::remove_stale_body(&dir.join(previous));
        }
        Ok(body)
    }

    /// Replace the version's body with one stored in a fresh generation file
    /// (`<tag>.gen-<generation>.bin`), then atomically replace the sidecar,
    /// which names that file: the sidecar rename is the single commit point.
    /// A crash before it leaves the old sidecar and the old body, which still
    /// match (the unreferenced new file is removed by the next load); a crash
    /// after it leaves the new sidecar and the new body, with the old body
    /// file left for the next load to remove. The previous body file is then
    /// removed, best effort. The slot's annotation records are not touched.
    fn replace_object_body(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        body: BodySource,
        meta: &ObjectMeta,
    ) -> StoreResult<BodyRef> {
        let (dir, default_bin, toml_path) = self.object_paths(account_id, bucket, key, version);
        std::fs::create_dir_all(&dir)?;
        let tag = Self::version_tag(version);
        let previous = match Self::recorded_generation_body(&toml_path) {
            Some(name) => dir.join(name),
            None => default_bin,
        };
        let name = Self::generation_body_name(&tag);
        let body = self.write_slot(
            bucket,
            key,
            version,
            dir.join(&name),
            &toml_path,
            body,
            meta,
            Some(name),
        )?;
        if !meta.is_delete_marker {
            Self::remove_stale_body(&previous);
        }
        Ok(body)
    }

    fn put_object_meta(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        meta: &ObjectMeta,
    ) -> StoreResult<()> {
        let (dir, _bin, toml_path) = self.object_paths(account_id, bucket, key, version);
        std::fs::create_dir_all(&dir)?;
        // A metadata update does not move the body: the sidecar keeps naming
        // the body file it named.
        let mut meta = meta.clone();
        meta.body_file = Self::recorded_generation_body(&toml_path);
        crate::atomic::write_atomic_toml(&toml_path, &meta)?;
        Ok(())
    }

    fn delete_object(&self, account_id: &str, bucket: &str, key: &str, version: Option<&str>) -> StoreResult<()> {
        let (dir, default_bin, toml_path) = self.object_paths(account_id, bucket, key, version);
        // The body is whichever file the sidecar names; a default file left
        // beside a generation one goes too.
        let generation = Self::recorded_generation_body(&toml_path).map(|name| dir.join(name));
        let bin_path = generation.clone().unwrap_or_else(|| default_bin.clone());
        // Sidecar first, body second. `load` iterates `*.toml` and treats a
        // sidecar whose `.bin` is gone as a hard error that costs the whole
        // bucket, while an orphan `.bin` is never looked at. So if this is
        // interrupted -- a signal, or the second `remove_file` failing -- the
        // residue left behind has to be the body, not the sidecar.
        // The sidecar is the durable record, so its removal is the one that has
        // to succeed. Once it is gone the object is deleted as far as every
        // reader is concerned, and failing the call over a body that could not
        // be unlinked would report a delete that did happen as an error --
        // while still leaving the caller no way to reclaim the bytes.
        match std::fs::remove_file(&toml_path) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        if let Err(e) = std::fs::remove_file(&bin_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %bin_path.display(),
                    error = %e,
                    "deleted object's body could not be removed; it is orphaned until the \
                     bucket is deleted",
                );
            }
        }
        if generation.is_some() {
            Self::remove_stale_body(&default_bin);
        }
        // Annotations without their sidecar are never loaded, so like the
        // body a failure here only leaves residue.
        let annotations = self.annotation_dir(account_id, bucket, key, version);
        if let Err(e) = remove_dir_if_present(&annotations) {
            tracing::warn!(
                path = %annotations.display(),
                error = %e,
                "deleted object's annotations could not be removed",
            );
        }
        Self::cleanup_empty(&dir);

        self.cache.invalidate(&crate::cache::BodyKey::new(
            bucket.to_string(),
            key.to_string(),
            version.map(|s| s.to_string()),
        ));
        Ok(())
    }

    fn put_annotation(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        record: &AnnotationRecord,
        payload: &[u8],
    ) -> StoreResult<()> {
        Self::write_annotation(&self.annotation_dir(account_id, bucket, key, version), record, payload)
    }

    fn delete_annotation(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        name: &str,
    ) -> StoreResult<()> {
        let dir = self.annotation_dir(account_id, bucket, key, version);
        let stem = Self::annotation_file_stem(name);
        // Record first: a payload without its record is never loaded.
        for ext in ["toml", "bin"] {
            match std::fs::remove_file(dir.join(format!("{stem}.{ext}"))) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Self::cleanup_empty(&dir);
        Ok(())
    }

    fn open_object_body(&self, body: &BodyRef) -> StoreResult<Bytes> {
        match body {
            BodyRef::Memory(b) => Ok(b.clone()),
            BodyRef::Disk {
                bucket,
                key,
                version,
                path,
                size: _,
            } => {
                let body_key =
                    crate::cache::BodyKey::new(bucket.clone(), key.clone(), version.clone());
                if let Some(bytes) = self.cache.get(&body_key) {
                    return Ok(bytes);
                }
                let bytes = Bytes::from(std::fs::read(path)?);
                self.cache.insert(body_key, bytes.clone());
                Ok(bytes)
            }
        }
    }

    fn mpu_create(&self, account_id: &str, bucket: &str, upload_id: &str, init: &MpuInit) -> StoreResult<()> {
        let parts_dir = self.mpu_parts_dir(account_id, bucket, upload_id);
        std::fs::create_dir_all(&parts_dir)?;
        let init_path = self.mpu_dir(account_id, bucket, upload_id).join("init.toml");
        crate::atomic::write_atomic_toml(&init_path, init)?;
        Ok(())
    }

    fn mpu_put_part(
        &self,
        account_id: &str,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        body: BodySource,
        etag: &str,
        checksum: Option<PartChecksum<'_>>,
    ) -> StoreResult<BodyRef> {
        let parts_dir = self.mpu_parts_dir(account_id, bucket, upload_id);
        std::fs::create_dir_all(&parts_dir)?;
        let bin_path = self.mpu_part_bin(account_id, bucket, upload_id, part_number);
        let toml_path = self.mpu_part_toml(account_id, bucket, upload_id, part_number);

        let size: u64 = match body {
            BodySource::Bytes(b) => {
                let n = b.len() as u64;
                crate::atomic::write_atomic_bytes(&bin_path, &b)?;
                let cache_key = Self::mpu_body_key(bucket, upload_id, part_number);
                self.cache.insert(cache_key, b);
                n
            }
            BodySource::File(src) => {
                let n = std::fs::metadata(&src)?.len();
                crate::atomic::write_atomic_from_file(&src, &bin_path)?;
                self.cache
                    .invalidate(&Self::mpu_body_key(bucket, upload_id, part_number));
                n
            }
            BodySource::FileCopy(src) => {
                let n = std::fs::metadata(&src)?.len();
                crate::atomic::write_atomic_copy_from_file(&src, &bin_path)?;
                self.cache
                    .invalidate(&Self::mpu_body_key(bucket, upload_id, part_number));
                n
            }
        };

        let meta = UploadPartMeta {
            part_number,
            etag: etag.to_string(),
            size,
            last_modified: Utc::now(),
            checksum_algorithm: checksum.map(|c| c.algorithm.to_string()),
            checksum_value: checksum.map(|c| c.value.to_string()),
        };
        crate::atomic::write_atomic_toml(&toml_path, &meta)?;
        // Disk-mode parts live on disk; expose them as BodyRef::Disk so
        // the runtime state never holds the part body in RAM.
        Ok(BodyRef::Disk {
            bucket: bucket.to_string(),
            key: format!("__mpu__/{upload_id}/part-{part_number:05}"),
            version: None,
            path: bin_path,
            size,
        })
    }

    fn spool_dir(&self) -> Option<std::path::PathBuf> {
        let dir = self.root.join(".spool");
        // Best-effort: create lazily; the spool helper also creates if
        // missing. Returning the path even when create fails is safe —
        // the helper handles both create and the actual file open.
        let _ = std::fs::create_dir_all(&dir);
        Some(dir)
    }

    fn mpu_abort(&self, account_id: &str, bucket: &str, upload_id: &str) -> StoreResult<()> {
        let dir = self.mpu_dir(account_id, bucket, upload_id);
        // Invalidate any cached part bodies for this upload.
        if let Ok(entries) = std::fs::read_dir(self.mpu_parts_dir(account_id, bucket, upload_id)) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                    if let Some(stem) = fname.strip_suffix(".bin") {
                        if let Ok(n) = stem.parse::<u32>() {
                            self.cache
                                .invalidate(&Self::mpu_body_key(bucket, upload_id, n));
                        }
                    }
                }
            }
        }
        match std::fs::remove_dir_all(&dir) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn mpu_complete(
        &self,
        account_id: &str,
        bucket: &str,
        upload_id: &str,
        final_key: &str,
        version: Option<&str>,
        meta: &ObjectMeta,
    ) -> StoreResult<BodyRef> {
        let parts_dir = self.mpu_parts_dir(account_id, bucket, upload_id);
        if !parts_dir.exists() {
            return Err(io_other(format!(
                "mpu_complete: no parts dir for upload {}",
                upload_id
            )));
        }

        // Enumerate parts in ascending part-number order.
        let mut part_numbers: Vec<u32> = Vec::new();
        for entry in std::fs::read_dir(&parts_dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(fname) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if let Some(stem) = fname.strip_suffix(".toml") {
                if let Ok(n) = stem.parse::<u32>() {
                    if self.mpu_part_bin(account_id, bucket, upload_id, n).exists() {
                        part_numbers.push(n);
                    }
                }
            }
        }
        part_numbers.sort_unstable();
        if part_numbers.is_empty() {
            return Err(io_other(format!(
                "mpu_complete: upload {} has no parts",
                upload_id
            )));
        }

        let (dir, bin_path, toml_path) = self.object_paths(account_id, bucket, final_key, version);
        std::fs::create_dir_all(&dir)?;
        remove_dir_if_present(&self.annotation_dir(account_id, bucket, final_key, version))?;

        let total_size: u64 = if part_numbers.len() == 1 {
            let only = self.mpu_part_bin(account_id, bucket, upload_id, part_numbers[0]);
            let sz = std::fs::metadata(&only)?.len();
            match std::fs::rename(&only, &bin_path) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc_exdev()) => {
                    // Cross-device rename: fall back to streaming copy then remove source.
                    crate::atomic::write_atomic_copy_from_file(&only, &bin_path)?;
                    let _ = std::fs::remove_file(&only);
                }
                Err(e) => return Err(e.into()),
            }
            // fsync the parent directory so the rename that links the completed
            // object is durable, matching the multi-part branch below. Without
            // it a power-loss could leave a 200-OK'd single-part MPU not durably
            // linked, poisoning the bucket load (bug-audit 2026-06-20, 4.4).
            if let Some(parent) = bin_path.parent() {
                if let Ok(dir_handle) = std::fs::File::open(parent) {
                    let _ = dir_handle.sync_all();
                }
            }
            sz
        } else {
            // A temp name the load recognizes (`crate::atomic::is_temp_file_name`),
            // so a crash before the rename leaves nothing the next load keeps.
            let tmp = crate::atomic::tmp_path(&bin_path);
            let concatenate = || -> std::io::Result<u64> {
                let mut total: u64 = 0;
                let mut out = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&tmp)?;
                for n in &part_numbers {
                    let part_path = self.mpu_part_bin(account_id, bucket, upload_id, *n);
                    let mut input = std::fs::File::open(&part_path)?;
                    total += std::io::copy(&mut input, &mut out)?;
                }
                out.sync_all()?;
                drop(out);
                std::fs::rename(&tmp, &bin_path)?;
                Ok(total)
            };
            let total = match concatenate() {
                Ok(total) => total,
                Err(e) => {
                    let _ = std::fs::remove_file(&tmp);
                    return Err(e.into());
                }
            };
            if let Some(parent) = bin_path.parent() {
                if let Ok(dir_handle) = std::fs::File::open(parent) {
                    let _ = dir_handle.sync_all();
                }
            }
            total
        };

        // The completed object's body is the default file; a generation file
        // an earlier replace left in the slot stops being its body.
        let previous = Self::recorded_generation_body(&toml_path);
        let mut meta = meta.clone();
        meta.body_file = None;
        crate::atomic::write_atomic_toml(&toml_path, &meta)?;
        if let (Some(previous), Some(dir)) = (previous, toml_path.parent()) {
            Self::remove_stale_body(&dir.join(previous));
        }

        // Invalidate per-part cache entries and drop the mpu dir.
        for n in &part_numbers {
            self.cache
                .invalidate(&Self::mpu_body_key(bucket, upload_id, *n));
        }
        let mpu_dir = self.mpu_dir(account_id, bucket, upload_id);
        let _ = std::fs::remove_dir_all(&mpu_dir);

        // The concatenated body is deliberately NOT re-inserted into the cache —
        // large multipart uploads typically exceed the single-object cap, and we
        // must never round-trip the assembled body through RAM. The next
        // open_object_body call will load through the normal cache path.
        self.cache.invalidate(&crate::cache::BodyKey::new(
            bucket.to_string(),
            final_key.to_string(),
            version.map(|s| s.to_string()),
        ));

        Ok(BodyRef::Disk {
            bucket: bucket.to_string(),
            key: final_key.to_string(),
            version: version.map(|s| s.to_string()),
            path: bin_path,
            size: total_size,
        })
    }
}

fn libc_exdev() -> i32 {
    18
}

#[cfg(test)]
mod disk_tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn new_store(tmp: &TempDir) -> DiskS3Store {
        let cache = Arc::new(crate::cache::BodyCache::new(1024 * 1024));
        DiskS3Store::new(tmp.path().to_path_buf(), cache)
    }

    #[test]
    fn detach_bucket_moves_it_out_and_a_recreate_survives_the_removal() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        let meta = BucketMeta {
            name: "b".into(),
            account_id: account_id.to_string(),
            ..Default::default()
        };
        store.put_bucket_meta(account_id, "b", &meta).unwrap();
        let detached = store.detach_bucket(account_id, "b").unwrap().expect("moved");
        assert!(!store.bucket_state_exists(account_id, "b"));
        assert!(detached.exists());
        // Recreated under the same name before the removal runs.
        store.put_bucket_meta(account_id, "b", &meta).unwrap();
        remove_detached(&detached).unwrap();
        assert!(!detached.exists());
        assert!(store.bucket_state_exists(account_id, "b"));
        // Nothing to move the second time round, and removal is idempotent.
        store.delete_bucket(account_id, "b").unwrap();
        assert_eq!(store.detach_bucket(account_id, "b").unwrap(), None);
        remove_detached(&detached).unwrap();
    }

    #[test]
    fn load_clears_leftover_detached_buckets() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        let mut meta = BucketMeta::default();
        meta.account_id = account_id.to_string();
        store.put_bucket_meta(account_id, "b", &meta).unwrap();
        let detached = store.detach_bucket(account_id, "b").unwrap().unwrap();
        let states = store.load().unwrap();
        assert!(states
            .get(account_id)
            .map(|s| s.buckets.is_empty())
            .unwrap_or(true));
        assert!(!detached.exists());
    }

    fn new_store_with_cache(
        tmp: &TempDir,
        cap: u64,
    ) -> (DiskS3Store, Arc<crate::cache::BodyCache>) {
        let cache = Arc::new(crate::cache::BodyCache::new(cap));
        (
            DiskS3Store::new(tmp.path().to_path_buf(), cache.clone()),
            cache,
        )
    }

    fn sample_meta(key: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            key: key.to_string(),
            content_type: "application/octet-stream".to_string(),
            etag: "etag".to_string(),
            size,
            ..Default::default()
        }
    }

    /// The refusal set is written by two layers that name a bucket differently:
    /// `load` inserts the on-disk directory name, while the sidecar layer above
    /// the store hands `mark_bucket_load_refused` a bucket NAME. Both readers
    /// escape. A name that escapes to itself -- which every name in the e2e
    /// suites happens to be -- cannot tell the two key spaces apart, so pin it
    /// with one that does not.
    #[test]
    fn a_refusal_is_found_under_the_escaped_directory_name() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";

        let name = "odd:name";
        let escaped = crate::key_escape::escape_key_segment(name);
        assert_ne!(escaped, name, "pick a name that actually escapes");

        assert!(!store.bucket_load_refused(account_id, name));
        store.mark_bucket_load_refused(account_id, name);
        assert!(
            store.bucket_load_refused(account_id, name),
            "marked by name, read back by name"
        );

        // The directory the store creates for it is the escaped one, so `load`'s
        // raw-directory-name insert lands on the same key this reader uses.
        let meta = BucketMeta {
            name: name.to_string(),
            account_id: account_id.to_string(),
            ..Default::default()
        };
        store.put_bucket_meta(account_id, name, &meta).unwrap();
        assert!(
            tmp.path()
                .join("accounts")
                .join(account_id)
                .join("buckets")
                .join(&escaped)
                .is_dir(),
            "expected the bucket directory under the escaped name"
        );

        store.clear_bucket_load_refusal(account_id, name).unwrap();
        assert!(!store.bucket_load_refused(account_id, name));
    }

    #[test]
    fn put_bucket_meta_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        let meta = BucketMeta {
            name: "b1".to_string(),
            account_id: account_id.to_string(),
            region: "us-east-1".to_string(),
            versioning: Some("Enabled".to_string()),
            ..Default::default()
        };
        store.put_bucket_meta(account_id, "b1", &meta).unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b1").unwrap();
        assert_eq!(snap.meta.name, "b1");
        assert_eq!(snap.meta.region, "us-east-1");
        assert_eq!(snap.meta.versioning.as_deref(), Some("Enabled"));
    }

    #[test]
    fn put_bucket_subresource_each_variant_writes_file() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let variants = [
            BucketSubresource::Tags,
            BucketSubresource::Lifecycle,
            BucketSubresource::Cors,
            BucketSubresource::Policy,
            BucketSubresource::Notification,
            BucketSubresource::Logging,
            BucketSubresource::Website,
            BucketSubresource::PublicAccessBlock,
            BucketSubresource::ObjectLock,
            BucketSubresource::Replication,
            BucketSubresource::Ownership,
            BucketSubresource::Inventory,
            BucketSubresource::Encryption,
            BucketSubresource::Versioning,
            BucketSubresource::Acl,
            BucketSubresource::Accelerate,
        ];
        for v in variants {
            store
                .put_bucket_subresource(account_id, "b", v, "payload=true")
                .unwrap();
            let file = store
                .bucket_dir(account_id, "b")
                .join(DiskS3Store::subresource_filename(v));
            assert!(file.exists(), "{:?}", v);
            assert_eq!(std::fs::read_to_string(&file).unwrap(), "payload=true");
        }
    }

    #[test]
    fn delete_bucket_subresource_removes_file() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .put_bucket_subresource(account_id, "b", BucketSubresource::Tags, "x=1")
            .unwrap();
        store
            .delete_bucket_subresource(account_id, "b", BucketSubresource::Tags)
            .unwrap();
        let file = store.bucket_dir(account_id, "b").join("tags.toml");
        assert!(!file.exists());
        // idempotent
        store
            .delete_bucket_subresource(account_id, "b", BucketSubresource::Tags)
            .unwrap();
    }

    #[test]
    fn put_object_bytes_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from_static(b"hello world");
        let meta = sample_meta("k1", data.len() as u64);
        let body_ref = store
            .put_object(account_id, "b", "k1", None, BodySource::Bytes(data.clone()), &meta)
            .unwrap();
        match &body_ref {
            BodyRef::Disk {
                bucket,
                key,
                size,
                path,
                ..
            } => {
                assert_eq!(bucket, "b");
                assert_eq!(key, "k1");
                assert_eq!(*size, data.len() as u64);
                assert_eq!(std::fs::read(path).unwrap(), data.to_vec());
            }
            _ => panic!("expected Disk"),
        }
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        let obj = snap.objects.get("k1").unwrap();
        assert_eq!(obj.meta.size, data.len() as u64);
    }

    #[test]
    fn put_object_file_copy_source_leaves_src_in_place() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let src_dir = TempDir::new().unwrap();
        let src = src_dir.path().join("src.bin");
        std::fs::write(&src, b"copied-body").unwrap();
        let meta = sample_meta("k", 11);
        let bref = store
            .put_object(account_id, "b", "k", None, BodySource::FileCopy(src.clone()), &meta)
            .unwrap();
        match bref {
            BodyRef::Disk { path, size, .. } => {
                assert_eq!(size, 11);
                assert_eq!(std::fs::read(&path).unwrap(), b"copied-body");
            }
            _ => panic!("expected Disk bodyref"),
        }
        assert!(src.exists(), "source file must not be moved by FileCopy");
        assert_eq!(std::fs::read(&src).unwrap(), b"copied-body");
    }

    #[test]
    fn put_object_file_source() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let src = tmp.path().join("src.bin");
        std::fs::write(&src, b"file-body").unwrap();
        let meta = sample_meta("k", 9);
        let body_ref = store
            .put_object(account_id, "b", "k", None, BodySource::File(src.clone()), &meta)
            .unwrap();
        let path = match body_ref {
            BodyRef::Disk { path, .. } => path,
            _ => panic!(),
        };
        assert_eq!(std::fs::read(&path).unwrap(), b"file-body");
    }

    #[test]
    fn put_object_meta_only_keeps_bin() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from_static(b"abc");
        let mut meta = sample_meta("k", 3);
        store
            .put_object(account_id, "b", "k", None, BodySource::Bytes(data.clone()), &meta)
            .unwrap();
        let (_, bin, _) = store.object_paths(account_id, "b", "k", None);
        let before = std::fs::read(&bin).unwrap();
        meta.tags.insert("x".to_string(), "y".to_string());
        store.put_object_meta(account_id, "b", "k", None, &meta).unwrap();
        assert_eq!(std::fs::read(&bin).unwrap(), before);
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let obj = state.buckets.get("b").unwrap().objects.get("k").unwrap();
        assert_eq!(obj.meta.tags.get("x").map(String::as_str), Some("y"));
    }

    fn annotation(name: &str) -> AnnotationRecord {
        AnnotationRecord {
            name: name.to_string(),
            etag: format!("\"{name}\""),
            checksum_algorithm: Some("CRC64NVME".to_string()),
            checksum_value: Some("AAAAAAAAAAA=".to_string()),
            ..Default::default()
        }
    }

    fn store_with_object(tmp: &TempDir) -> DiskS3Store {
        let store = new_store(tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .put_object(
                account_id,
                "b",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(b"abc")),
                &sample_meta("k", 3),
            )
            .unwrap();
        store
    }

    fn loaded_annotations(store: &DiskS3Store) -> Vec<(String, Vec<u8>)> {
        let account_id = "test-account";
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let obj = &state.buckets["b"].objects["k"];
        obj.meta
            .annotations
            .iter()
            .map(|a| (a.name.clone(), a.payload.clone()))
            .collect()
    }

    #[test]
    fn put_annotation_touches_only_that_annotation() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        let account_id = "test-account";
        store
            .put_annotation(account_id, "b", "k", None, &annotation("a"), b"first")
            .unwrap();
        // Overwrite a's stored payload behind the store's back: if putting b
        // rewrote a, this sentinel would be replaced.
        let a_bin = store
            .annotation_dir(account_id, "b", "k", None)
            .join(format!("{}.bin", DiskS3Store::annotation_file_stem("a")));
        std::fs::write(&a_bin, b"sentinel").unwrap();
        let (_, _, sidecar) = store.object_paths(account_id, "b", "k", None);
        let sidecar_before = std::fs::read(&sidecar).unwrap();

        store
            .put_annotation(account_id, "b", "k", None, &annotation("b"), b"second")
            .unwrap();
        assert_eq!(std::fs::read(&a_bin).unwrap(), b"sentinel");
        assert_eq!(std::fs::read(&sidecar).unwrap(), sidecar_before);
        assert_eq!(
            loaded_annotations(&store),
            vec![
                ("a".to_string(), b"sentinel".to_vec()),
                ("b".to_string(), b"second".to_vec())
            ]
        );

        store.delete_annotation(account_id, "b", "k", None, "a").unwrap();
        store.delete_annotation(account_id, "b", "k", None, "missing").unwrap();
        assert_eq!(
            loaded_annotations(&store),
            vec![("b".to_string(), b"second".to_vec())]
        );
        // A 512-byte name still maps to a short file name.
        let long = "n".repeat(512);
        store
            .put_annotation(account_id, "b", "k", None, &annotation(&long), b"long")
            .unwrap();
        assert!(loaded_annotations(&store).iter().any(|(n, _)| *n == long));
    }

    #[test]
    fn legacy_embedded_annotations_move_to_their_own_records() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        let account_id = "test-account";
        let mut meta = sample_meta("k", 3);
        meta.annotations = vec![AnnotationSnapshot {
            name: "old".to_string(),
            payload: b"legacy".to_vec(),
            etag: "e".to_string(),
            ..Default::default()
        }];
        store.put_object_meta(account_id, "b", "k", None, &meta).unwrap();

        assert_eq!(
            loaded_annotations(&store),
            vec![("old".to_string(), b"legacy".to_vec())]
        );
        // The sidecar no longer embeds it, so deleting the record is final.
        let (_, _, sidecar) = store.object_paths(account_id, "b", "k", None);
        let rewritten: ObjectMeta =
            toml::from_str(&std::fs::read_to_string(&sidecar).unwrap()).unwrap();
        assert!(rewritten.annotations.is_empty());
        store.delete_annotation(account_id, "b", "k", None, "old").unwrap();
        assert!(loaded_annotations(&store).is_empty());
    }

    #[test]
    fn replacing_or_deleting_an_object_drops_its_annotations() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        let account_id = "test-account";
        store
            .put_annotation(account_id, "b", "k", None, &annotation("a"), b"x")
            .unwrap();
        store
            .put_object(
                account_id,
                "b",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(b"new")),
                &sample_meta("k", 3),
            )
            .unwrap();
        assert!(loaded_annotations(&store).is_empty());
        store
            .put_annotation(account_id, "b", "k", None, &annotation("a"), b"x")
            .unwrap();
        store.delete_object(account_id, "b", "k", None).unwrap();
        assert!(!store.annotation_dir(account_id, "b", "k", None).exists());
    }

    /// Replacing a slot's body keeps it the same object: the new bytes and
    /// metadata are stored and read back, and its annotations stay, in an
    /// unversioned slot and a version's own slot alike.
    #[test]
    fn replacing_an_objects_body_keeps_its_annotations() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        let account_id = "test-account";
        store
            .put_annotation(account_id, "b", "k", None, &annotation("a"), b"kept")
            .unwrap();
        let mut meta = sample_meta("k", 5);
        meta.content_type = "text/x-resealed".to_string();
        let body = store
            .replace_object_body(
                account_id,
                "b",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(b"sealx")),
                &meta,
            )
            .unwrap();
        assert_eq!(&store.open_object_body(&body).unwrap()[..], b"sealx");
        assert_eq!(
            loaded_annotations(&store),
            vec![("a".to_string(), b"kept".to_vec())]
        );
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let obj = &state.buckets["b"].objects["k"];
        assert_eq!(obj.meta.content_type, "text/x-resealed");
        assert_eq!(&store.open_object_body(&obj.body).unwrap()[..], b"sealx");

        // A version's slot.
        store
            .put_object(
                account_id,
                "b",
                "v",
                Some("v1"),
                BodySource::Bytes(Bytes::from_static(b"one")),
                &sample_meta("v", 3),
            )
            .unwrap();
        store
            .put_annotation(account_id, "b", "v", Some("v1"), &annotation("n"), b"note")
            .unwrap();
        store
            .replace_object_body(
                account_id,
                "b",
                "v",
                Some("v1"),
                BodySource::Bytes(Bytes::from_static(b"two")),
                &sample_meta("v", 3),
            )
            .unwrap();
        let dir = store.annotation_dir(account_id, "b", "v", Some("v1"));
        assert!(dir
            .join(format!("{}.bin", DiskS3Store::annotation_file_stem("n")))
            .exists());
    }

    /// The files in the object directory of `b/k`, sorted.
    fn slot_files(store: &DiskS3Store) -> Vec<String> {
        let account_id = "test-account";
        let (dir, _, _) = store.object_paths(account_id, "b", "k", None);
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|e| e.path().is_file())
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    /// The body `b/k` loads with, read through a fresh store over the same
    /// directory (a restart).
    fn reloaded_body(tmp: &TempDir) -> Vec<u8> {
        let account_id = "test-account";
        let store = new_store(tmp);
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let obj = &state.buckets["b"].objects["k"];
        store.open_object_body(&obj.body).unwrap().to_vec()
    }

    fn replace(store: &DiskS3Store, body: &'static [u8]) {
        let account_id = "test-account";
        store
            .replace_object_body(
                account_id,
                "b",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(body)),
                &sample_meta("k", body.len() as u64),
            )
            .unwrap();
    }

    /// A replaced body lives in a generation file the sidecar names; the
    /// previous body file is removed, and a restart reads the new body.
    #[test]
    fn a_replaced_body_survives_a_restart() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        replace(&store, b"first");
        replace(&store, b"second");
        let files = slot_files(&store);
        assert_eq!(files.len(), 2, "{files:?}");
        assert!(files.iter().any(|f| f == "null.toml"), "{files:?}");
        let body = files.iter().find(|f| f.ends_with(".bin")).unwrap();
        assert_eq!(DiskS3Store::generation_body_tag(body), Some("null"));
        assert_eq!(reloaded_body(&tmp), b"second");
        // A metadata update keeps the sidecar naming that file.
        let account_id = "test-account";
        store
            .put_object_meta(account_id, "b", "k", None, &sample_meta("k", 6))
            .unwrap();
        assert_eq!(reloaded_body(&tmp), b"second");
        // A new object in the slot is back in the default file, alone.
        store
            .put_object(
                account_id,
                "b",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(b"fresh")),
                &sample_meta("k", 5),
            )
            .unwrap();
        assert_eq!(slot_files(&store), vec!["null.bin", "null.toml"]);
        assert_eq!(reloaded_body(&tmp), b"fresh");
    }

    /// A crash after the new body is written but before the sidecar rename
    /// leaves the old sidecar and body, which read back as they were; the
    /// orphaned generation file is removed by the next load.
    #[test]
    fn a_replace_interrupted_before_its_sidecar_keeps_the_old_object() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        let account_id = "test-account";
        let (dir, _, _) = store.object_paths(account_id, "b", "k", None);
        let orphan = dir.join(DiskS3Store::generation_body_name("null"));
        std::fs::write(&orphan, b"sealed but never committed").unwrap();
        assert_eq!(reloaded_body(&tmp), b"abc");
        assert!(!orphan.exists(), "the orphan is removed on load");
        assert_eq!(slot_files(&store), vec!["null.bin", "null.toml"]);
    }

    /// A crash after the sidecar rename, before the previous body file is
    /// removed, reads the new body; the previous file is removed on load.
    #[test]
    fn a_replace_interrupted_after_its_sidecar_keeps_the_new_object() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        let account_id = "test-account";
        replace(&store, b"resealed");
        let (dir, default_bin, _) = store.object_paths(account_id, "b", "k", None);
        // The removals the crash prevented: the default body and an older
        // generation.
        std::fs::write(&default_bin, b"abc").unwrap();
        let older = dir.join(DiskS3Store::generation_body_name("null"));
        std::fs::write(&older, b"older").unwrap();
        assert_eq!(reloaded_body(&tmp), b"resealed");
        assert!(!default_bin.exists());
        assert!(!older.exists());
        assert_eq!(slot_files(&store).len(), 2);
    }

    /// Temp files a crash left before their rename (`<file>.<pid>.<seq>.tmp`)
    /// are removed by the load, in an object's directory and its annotation
    /// directory; other files are left alone.
    #[test]
    fn load_removes_temp_files_left_by_interrupted_writes() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        let account_id = "test-account";
        store
            .put_annotation(account_id, "b", "k", None, &annotation("a"), b"note")
            .unwrap();
        let (dir, _, _) = store.object_paths(account_id, "b", "k", None);
        let annotations = store.annotation_dir(account_id, "b", "k", None);
        let leftovers = [
            dir.join("null.toml.4242.7.tmp"),
            dir.join("null.bin.4242.8.tmp"),
            annotations.join("deadbeef.toml.4242.9.tmp"),
        ];
        for path in &leftovers {
            std::fs::write(path, b"partial").unwrap();
        }
        let unrelated = dir.join("notes.tmp");
        std::fs::write(&unrelated, b"keep").unwrap();

        assert_eq!(reloaded_body(&tmp), b"abc");
        for path in &leftovers {
            assert!(!path.exists(), "{}", path.display());
        }
        assert!(unrelated.exists());
        assert_eq!(
            loaded_annotations(&store),
            vec![("a".to_string(), b"note".to_vec())]
        );
    }

    /// A multipart completion concatenates its parts into a temp file the
    /// load recognizes, so a crash before its rename leaves nothing behind
    /// after the next load; the fixed `<body>.bin.tmp` name earlier builds
    /// used is cleared too. A completed upload leaves no temp file.
    #[test]
    fn load_removes_multipart_completion_temp_files() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        let account_id = "test-account";
        store
            .mpu_create(account_id, "b", "up", &sample_mpu_init("up", "k2"))
            .unwrap();
        for (n, body) in [(1u32, &b"AAAA"[..]), (2, &b"BB"[..])] {
            store
                .mpu_put_part(
                    account_id,
                    "b",
                    "up",
                    n,
                    BodySource::Bytes(Bytes::copy_from_slice(body)),
                    "e",
                    None,
                )
                .unwrap();
        }
        store
            .mpu_complete(account_id, "b", "up", "k2", None, &sample_meta("k2", 6))
            .unwrap();
        let (dir, bin_path, _) = store.object_paths(account_id, "b", "k2", None);
        let names = |dir: &std::path::Path| -> Vec<String> {
            let mut names: Vec<String> = std::fs::read_dir(dir)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                .collect();
            names.sort();
            names
        };
        assert!(
            names(&dir).iter().all(|n| !n.ends_with(".tmp")),
            "{:?}",
            names(&dir)
        );

        let interrupted = crate::atomic::tmp_path(&bin_path);
        let mut legacy = bin_path.as_os_str().to_owned();
        legacy.push(".tmp");
        let legacy = PathBuf::from(legacy);
        for path in [&interrupted, &legacy] {
            std::fs::write(path, b"AAAAB").unwrap();
        }
        assert_eq!(reloaded_body(&tmp), b"abc");
        assert!(!interrupted.exists(), "{}", interrupted.display());
        assert!(!legacy.exists(), "{}", legacy.display());
        assert_eq!(std::fs::read(&bin_path).unwrap(), b"AAAABB");
    }

    /// Deleting a resealed object removes the file its sidecar names (and a
    /// default file left beside it), leaving nothing.
    #[test]
    fn deleting_a_resealed_object_leaves_no_files() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        let account_id = "test-account";
        replace(&store, b"resealed");
        let (_, default_bin, _) = store.object_paths(account_id, "b", "k", None);
        std::fs::write(&default_bin, b"leftover").unwrap();
        store.delete_object(account_id, "b", "k", None).unwrap();
        assert!(slot_files(&store).is_empty(), "{:?}", slot_files(&store));
    }

    /// A sidecar written before body files were recorded (no `body_file`)
    /// still means the default `<tag>.bin`, and is written back without one.
    #[test]
    fn sidecars_without_a_body_file_read_the_default_body() {
        let tmp = TempDir::new().unwrap();
        let store = store_with_object(&tmp);
        let account_id = "test-account";
        let (_, _, toml_path) = store.object_paths(account_id, "b", "k", None);
        let text = std::fs::read_to_string(&toml_path).unwrap();
        assert!(!text.contains("body_file"), "{text}");
        assert_eq!(reloaded_body(&tmp), b"abc");
        // A name that is not this slot's generation file is not followed.
        let mut meta: ObjectMeta = toml::from_str(&text).unwrap();
        meta.body_file = Some("../../escape.bin".to_string());
        crate::atomic::write_atomic_toml(&toml_path, &meta).unwrap();
        assert_eq!(reloaded_body(&tmp), b"abc");
    }

    /// The memory store keeps annotations in the runtime state only: replacing
    /// a body just hands back the new bytes.
    #[test]
    fn memory_store_replaces_a_body() {
        let store = MemoryS3Store::new();
        let account_id = "test-account";
        let body = store
            .replace_object_body(
                account_id,
                "b",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(b"sealed")),
                &sample_meta("k", 6),
            )
            .unwrap();
        assert_eq!(&store.open_object_body(&body).unwrap()[..], b"sealed");
    }

    #[test]
    fn delete_object_removes_the_sidecar_before_the_body() {
        // `load` iterates `*.toml` and treats a sidecar whose `.bin` is gone as
        // a hard error for the whole bucket, while an orphan `.bin` is never
        // read. So the sidecar has to be the file the delete attempts first.
        //
        // Forces the SIDECAR removal to fail, by making that path a non-empty
        // directory `remove_file` cannot take. Only a sidecar-first delete
        // reports the failure with the body still on disk; a body-first one
        // would have removed the body before reaching it.
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .put_object(
                account_id,
                "b",
                "k.txt",
                None,
                BodySource::Bytes(Bytes::from_static(b"v")),
                &ObjectMeta {
                    key: "k.txt".to_string(),
                    size: 1,
                    ..Default::default()
                },
            )
            .unwrap();

        let (_dir, bin, toml) = store.object_paths(account_id, "b", "k.txt", None);
        std::fs::remove_file(&toml).unwrap();
        std::fs::create_dir(&toml).unwrap();
        std::fs::write(toml.join("blocker"), b"x").unwrap();

        assert!(
            store.delete_object(account_id, "b", "k.txt", None).is_err(),
            "a sidecar that cannot be removed must fail the delete: it is the durable record"
        );
        assert!(
            bin.exists(),
            "the body must still be there, which is only true if the sidecar was attempted first"
        );
    }

    #[test]
    fn delete_object_cleans_up_files_and_cache() {
        let tmp = TempDir::new().unwrap();
        let (store, cache) = new_store_with_cache(&tmp, 1024 * 1024);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from_static(b"bye");
        let meta = sample_meta("k", 3);
        store
            .put_object(account_id, "b", "k", None, BodySource::Bytes(data), &meta)
            .unwrap();
        let body_key = crate::cache::BodyKey::new("b".to_string(), "k".to_string(), None);
        assert!(cache.get(&body_key).is_some());
        store.delete_object(account_id, "b", "k", None).unwrap();
        let (dir, bin, toml_path) = store.object_paths(account_id, "b", "k", None);
        assert!(!bin.exists());
        assert!(!toml_path.exists());
        assert!(!dir.exists());
        assert!(cache.get(&body_key).is_none());
    }

    #[test]
    fn open_object_body_cache_hit_and_refill() {
        let tmp = TempDir::new().unwrap();
        let (store, cache) = new_store_with_cache(&tmp, 1024 * 1024);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from_static(b"payload");
        let meta = sample_meta("k", data.len() as u64);
        let body_ref = store
            .put_object(account_id, "b", "k", None, BodySource::Bytes(data.clone()), &meta)
            .unwrap();
        // Cache hit.
        let got = store.open_object_body(&body_ref).unwrap();
        assert_eq!(got, data);
        // Invalidate and re-read populates cache from disk.
        let body_key = crate::cache::BodyKey::new("b".to_string(), "k".to_string(), None);
        cache.invalidate(&body_key);
        assert!(cache.get(&body_key).is_none());
        let got = store.open_object_body(&body_ref).unwrap();
        assert_eq!(got, data);
        assert!(cache.get(&body_key).is_some());
    }

    #[test]
    fn open_object_body_large_bypasses_cache() {
        let tmp = TempDir::new().unwrap();
        // capacity 1024 → single-object cap 512. Use 800-byte body.
        let (store, cache) = new_store_with_cache(&tmp, 1024);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from(vec![7u8; 800]);
        let meta = sample_meta("big", 800);
        let body_ref = store
            .put_object(account_id, "b", "big", None, BodySource::Bytes(data.clone()), &meta)
            .unwrap();
        let body_key = crate::cache::BodyKey::new("b".to_string(), "big".to_string(), None);
        assert!(cache.get(&body_key).is_none());
        let got = store.open_object_body(&body_ref).unwrap();
        assert_eq!(got, data);
        // Still none — exceeds single-object cap.
        assert!(cache.get(&body_key).is_none());
    }

    #[test]
    fn load_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let s = store.load().unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn load_skips_mpu_without_init() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from_static(b"x");
        let meta = sample_meta("k", 1);
        store
            .put_object(account_id, "b", "k", None, BodySource::Bytes(data), &meta)
            .unwrap();
        // Directory with no init.toml is skipped by load.
        let mpu = store.bucket_dir(account_id, "b").join("mpu").join("upload1");
        std::fs::create_dir_all(&mpu).unwrap();

        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        assert_eq!(snap.objects.len(), 1);
        assert!(snap.multipart_uploads.is_empty());
    }

    #[test]
    fn load_reads_bucket_subresources() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .put_bucket_subresource(account_id, "b", BucketSubresource::Lifecycle, "<Lifecycle/>")
            .unwrap();
        store
            .put_bucket_subresource(account_id, "b", BucketSubresource::Cors, "<Cors/>")
            .unwrap();
        store
            .put_bucket_subresource(account_id, "b", BucketSubresource::Policy, "{}")
            .unwrap();
        store
            .put_bucket_subresource(account_id, "b", BucketSubresource::Tags, "x=1")
            .unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        assert_eq!(
            snap.subresources.get("lifecycle.toml").map(String::as_str),
            Some("<Lifecycle/>"),
        );
        assert_eq!(
            snap.subresources.get("cors.toml").map(String::as_str),
            Some("<Cors/>"),
        );
        assert_eq!(
            snap.subresources.get("policy.toml").map(String::as_str),
            Some("{}"),
        );
        assert_eq!(
            snap.subresources.get("tags.toml").map(String::as_str),
            Some("x=1"),
        );
    }

    #[test]
    fn versioned_put_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    versioning: Some("Enabled".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let base = chrono::Utc::now();
        for (i, (vid, body)) in [("v1", "one"), ("v2", "two"), ("v3", "three")]
            .iter()
            .enumerate()
        {
            let mut m = sample_meta("k", body.len() as u64);
            m.version_id = Some((*vid).to_string());
            m.last_modified = base + chrono::Duration::seconds(i as i64);
            store
                .put_object(
                    account_id,
                    "b",
                    "k",
                    Some(*vid),
                    BodySource::Bytes(Bytes::copy_from_slice(body.as_bytes())),
                    &m,
                )
                .unwrap();
        }
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        let versions = snap.object_versions.get("k").expect("versions present");
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].meta.version_id.as_deref(), Some("v1"));
        assert_eq!(versions[1].meta.version_id.as_deref(), Some("v2"));
        assert_eq!(versions[2].meta.version_id.as_deref(), Some("v3"));
        for v in versions {
            match &v.body {
                BodyRef::Disk { size, .. } => assert!(*size > 0),
                _ => panic!("expected Disk body"),
            }
        }
    }

    #[test]
    fn versioned_load_promotes_latest_live_to_snap_objects() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    versioning: Some("Enabled".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let base = chrono::Utc::now();
        for (i, (vid, body)) in [("v1", "one"), ("v2", "two"), ("v3", "three")]
            .iter()
            .enumerate()
        {
            let mut m = sample_meta("k", body.len() as u64);
            m.version_id = Some((*vid).to_string());
            m.last_modified = base + chrono::Duration::seconds(i as i64);
            store
                .put_object(
                    account_id,
                    "b",
                    "k",
                    Some(*vid),
                    BodySource::Bytes(Bytes::copy_from_slice(body.as_bytes())),
                    &m,
                )
                .unwrap();
        }
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        let current = snap.objects.get("k").expect("current object promoted");
        assert_eq!(current.meta.version_id.as_deref(), Some("v3"));
    }

    #[test]
    fn versioned_load_trailing_delete_marker_hides_current() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    versioning: Some("Enabled".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let base = chrono::Utc::now();
        for (i, (vid, body)) in [("v1", "one"), ("v2", "two"), ("v3", "three")]
            .iter()
            .enumerate()
        {
            let mut m = sample_meta("k", body.len() as u64);
            m.version_id = Some((*vid).to_string());
            m.last_modified = base + chrono::Duration::seconds(i as i64);
            store
                .put_object(
                    account_id,
                    "b",
                    "k",
                    Some(*vid),
                    BodySource::Bytes(Bytes::copy_from_slice(body.as_bytes())),
                    &m,
                )
                .unwrap();
        }
        // Append a delete marker on top.
        let mut dm = sample_meta("k", 0);
        dm.version_id = Some("dm1".to_string());
        dm.is_delete_marker = true;
        dm.last_modified = base + chrono::Duration::seconds(10);
        store
            .put_object(account_id, "b", "k", Some("dm1"), BodySource::Bytes(Bytes::new()), &dm)
            .unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        assert!(
            !snap.objects.contains_key("k"),
            "trailing delete marker must hide current object",
        );
        assert_eq!(snap.object_versions.get("k").unwrap().len(), 4);
    }

    #[test]
    fn legacy_null_object_overridden_by_newer_versions() {
        // A pre-versioning null put followed by versioning-enabled puts must
        // see snap.objects reflect the newest live version, not the stale null.
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let base = chrono::Utc::now();
        let mut null_meta = sample_meta("k", 3);
        null_meta.last_modified = base;
        store
            .put_object(
                account_id,
                "b",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(b"old")),
                &null_meta,
            )
            .unwrap();
        // Enable versioning and put two versions, the second a delete.
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    versioning: Some("Enabled".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut v1 = sample_meta("k", 3);
        v1.version_id = Some("v1".to_string());
        v1.last_modified = base + chrono::Duration::seconds(1);
        store
            .put_object(
                account_id,
                "b",
                "k",
                Some("v1"),
                BodySource::Bytes(Bytes::from_static(b"new")),
                &v1,
            )
            .unwrap();
        let mut v2 = sample_meta("k", 5);
        v2.version_id = Some("v2".to_string());
        v2.last_modified = base + chrono::Duration::seconds(2);
        store
            .put_object(
                account_id,
                "b",
                "k",
                Some("v2"),
                BodySource::Bytes(Bytes::from_static(b"newer")),
                &v2,
            )
            .unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        let current = snap
            .objects
            .get("k")
            .expect("latest live version must override stale null");
        assert_eq!(current.meta.version_id.as_deref(), Some("v2"));
        assert_eq!(current.meta.size, 5);
    }

    #[test]
    fn legacy_null_hidden_by_trailing_delete_marker() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let base = chrono::Utc::now();
        let mut null_meta = sample_meta("k", 3);
        null_meta.last_modified = base;
        store
            .put_object(
                account_id,
                "b",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(b"old")),
                &null_meta,
            )
            .unwrap();
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    versioning: Some("Enabled".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut v1 = sample_meta("k", 3);
        v1.version_id = Some("v1".to_string());
        v1.last_modified = base + chrono::Duration::seconds(1);
        store
            .put_object(
                account_id,
                "b",
                "k",
                Some("v1"),
                BodySource::Bytes(Bytes::from_static(b"new")),
                &v1,
            )
            .unwrap();
        let mut dm = sample_meta("k", 0);
        dm.version_id = Some("dm1".to_string());
        dm.is_delete_marker = true;
        dm.last_modified = base + chrono::Duration::seconds(2);
        store
            .put_object(account_id, "b", "k", Some("dm1"), BodySource::Bytes(Bytes::new()), &dm)
            .unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        assert!(
            !snap.objects.contains_key("k"),
            "trailing delete marker must hide even a legacy null object",
        );
    }

    #[test]
    fn delete_marker_roundtrip_no_body_file() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    versioning: Some("Enabled".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut m = sample_meta("k", 0);
        m.version_id = Some("dm1".to_string());
        m.is_delete_marker = true;
        store
            .put_object(account_id, "b", "k", Some("dm1"), BodySource::Bytes(Bytes::new()), &m)
            .unwrap();
        // No .bin file on disk for delete markers.
        let (_, bin, toml_path) = store.object_paths(account_id, "b", "k", Some("dm1"));
        assert!(!bin.exists(), "delete marker must not have a .bin file");
        assert!(toml_path.exists());

        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let versions = state
            .buckets
            .get("b")
            .unwrap()
            .object_versions
            .get("k")
            .unwrap();
        assert_eq!(versions.len(), 1);
        assert!(versions[0].meta.is_delete_marker);
        match &versions[0].body {
            BodyRef::Memory(b) => assert_eq!(b.len(), 0),
            _ => panic!("delete marker body should be empty Memory"),
        }
    }

    #[test]
    fn mixed_nonversioned_and_versioned_buckets() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "a",
                &BucketMeta {
                    name: "a".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    versioning: Some("Enabled".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let ma = sample_meta("only", 3);
        store
            .put_object(
                account_id,
                "a",
                "only",
                None,
                BodySource::Bytes(Bytes::from_static(b"aaa")),
                &ma,
            )
            .unwrap();
        let base = chrono::Utc::now();
        for (i, vid) in ["v1", "v2"].iter().enumerate() {
            let mut m = sample_meta("twice", 2);
            m.version_id = Some((*vid).to_string());
            m.last_modified = base + chrono::Duration::seconds(i as i64);
            store
                .put_object(
                    account_id,
                    "b",
                    "twice",
                    Some(*vid),
                    BodySource::Bytes(Bytes::from_static(b"xx")),
                    &m,
                )
                .unwrap();
        }
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        assert_eq!(state.buckets.len(), 2);
        let a = state.buckets.get("a").unwrap();
        assert_eq!(a.objects.len(), 1);
        assert!(a.object_versions.is_empty());
        let b = state.buckets.get("b").unwrap();
        // Fix #5: the latest live version is promoted into objects so
        // unversioned GETs see it post-restart.
        assert_eq!(
            b.objects.get("twice").unwrap().meta.version_id.as_deref(),
            Some("v2")
        );
        assert_eq!(b.object_versions.get("twice").unwrap().len(), 2);
    }

    fn sample_mpu_init(upload_id: &str, key: &str) -> MpuInit {
        MpuInit {
            upload_id: upload_id.to_string(),
            key: key.to_string(),
            initiated: chrono::Utc::now(),
            content_type: "application/octet-stream".to_string(),
            storage_class: "STANDARD".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn mpu_create_then_load_empty_parts() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let init = sample_mpu_init("up1", "k1");
        store.mpu_create(account_id, "b", "up1", &init).unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        let m = snap.multipart_uploads.get("up1").expect("upload present");
        assert_eq!(m.init.upload_id, "up1");
        assert_eq!(m.init.key, "k1");
        assert!(m.parts.is_empty());
    }

    #[test]
    fn mpu_create_round_trips_the_requested_object_lock() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let until = chrono::Utc::now() + chrono::Duration::days(2);
        let init = MpuInit {
            lock_mode: Some("COMPLIANCE".to_string()),
            lock_retain_until: Some(until),
            lock_legal_hold: Some("ON".to_string()),
            ..sample_mpu_init("up1", "k1")
        };
        store.mpu_create(account_id, "b", "up1", &init).unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let m = &state.buckets["b"].multipart_uploads["up1"];
        assert_eq!(m.init.lock_mode.as_deref(), Some("COMPLIANCE"));
        assert_eq!(m.init.lock_retain_until, Some(until));
        assert_eq!(m.init.lock_legal_hold.as_deref(), Some("ON"));
    }

    #[test]
    fn mpu_init_written_before_object_lock_fields_still_loads() {
        let init: MpuInit =
            toml::from_str("upload_id = \"up1\"\nkey = \"k1\"\ncontent_type = \"text/plain\"\n")
                .unwrap();
        assert_eq!(init.upload_id, "up1");
        assert!(init.lock_mode.is_none());
        assert!(init.lock_retain_until.is_none());
        assert!(init.lock_legal_hold.is_none());
    }

    #[test]
    fn mpu_put_part_then_load_three_parts() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create(account_id, "b", "up", &sample_mpu_init("up", "k"))
            .unwrap();
        let bodies = [
            Bytes::from_static(b"part-one"),
            Bytes::from_static(b"part-two-longer"),
            Bytes::from_static(b"p3"),
        ];
        for (i, body) in bodies.iter().enumerate() {
            let n = (i + 1) as u32;
            store
                .mpu_put_part(
                    account_id,
                    "b",
                    "up",
                    n,
                    BodySource::Bytes(body.clone()),
                    &format!("et{}", n),
                    Some(PartChecksum {
                        algorithm: "CRC32",
                        value: &format!("ck{n}"),
                    }),
                )
                .unwrap();
        }
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        let m = snap.multipart_uploads.get("up").unwrap();
        assert_eq!(m.parts.len(), 3);
        for (i, body) in bodies.iter().enumerate() {
            let n = (i + 1) as u32;
            let part = m.parts.get(&n).unwrap();
            assert_eq!(part.meta.part_number, n);
            assert_eq!(part.meta.size, body.len() as u64);
            assert_eq!(part.meta.etag, format!("et{}", n));
            assert_eq!(part.meta.checksum_algorithm.as_deref(), Some("CRC32"));
            assert_eq!(part.meta.checksum_value, Some(format!("ck{n}")));
            match &part.body {
                BodyRef::Disk { path, size, .. } => {
                    assert_eq!(*size, body.len() as u64);
                    assert_eq!(std::fs::read(path).unwrap(), body.to_vec());
                }
                _ => panic!("expected Disk"),
            }
        }
    }

    #[test]
    fn mpu_abort_removes_upload() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create(account_id, "b", "up", &sample_mpu_init("up", "k"))
            .unwrap();
        store
            .mpu_put_part(
                account_id,
                "b",
                "up",
                1,
                BodySource::Bytes(Bytes::from_static(b"x")),
                "e",
                None,
            )
            .unwrap();
        store.mpu_abort(account_id, "b", "up").unwrap();
        assert!(!store.mpu_dir(account_id, "b", "up").exists());
        // Idempotent.
        store.mpu_abort(account_id, "b", "up").unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        assert!(snap.multipart_uploads.is_empty());
    }

    #[test]
    fn mpu_complete_single_part_fast_path() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create(account_id, "b", "up", &sample_mpu_init("up", "k"))
            .unwrap();
        let body = Bytes::from_static(b"only-part-bytes");
        store
            .mpu_put_part(account_id, "b", "up", 1, BodySource::Bytes(body.clone()), "et", None)
            .unwrap();
        let meta = sample_meta("k", body.len() as u64);
        let body_ref = store.mpu_complete(account_id, "b", "up", "k", None, &meta).unwrap();
        match &body_ref {
            BodyRef::Disk { path, size, .. } => {
                assert_eq!(*size, body.len() as u64);
                assert_eq!(std::fs::read(path).unwrap(), body.to_vec());
            }
            _ => panic!("expected Disk"),
        }
        assert!(!store.mpu_dir(account_id, "b", "up").exists());
    }

    #[test]
    fn mpu_complete_multi_part_concat() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create(account_id, "b", "up", &sample_mpu_init("up", "k"))
            .unwrap();
        let p1 = Bytes::from_static(b"AAAA");
        let p2 = Bytes::from_static(b"BBBBBB");
        let p3 = Bytes::from_static(b"CC");
        for (n, b) in [(1u32, &p1), (2, &p2), (3, &p3)] {
            store
                .mpu_put_part(account_id, "b", "up", n, BodySource::Bytes(b.clone()), "e", None)
                .unwrap();
        }
        let mut expected = Vec::new();
        expected.extend_from_slice(&p1);
        expected.extend_from_slice(&p2);
        expected.extend_from_slice(&p3);
        let meta = sample_meta("k", expected.len() as u64);
        let body_ref = store.mpu_complete(account_id, "b", "up", "k", None, &meta).unwrap();
        let path = match body_ref {
            BodyRef::Disk { path, size, .. } => {
                assert_eq!(size, expected.len() as u64);
                path
            }
            _ => panic!("expected Disk"),
        };
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert!(!store.mpu_dir(account_id, "b", "up").exists());
    }

    #[test]
    fn mpu_complete_large_streaming_via_file_source() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create(account_id, "b", "up", &sample_mpu_init("up", "k"))
            .unwrap();

        // 3 parts of ~1 MiB each (kept small so tests stay fast but still
        // exercise the BodySource::File + streaming concat path).
        const PART_SIZE: usize = 1024 * 1024;
        let patterns: [u8; 3] = [0x11, 0x22, 0x33];
        let mut expected: Vec<u8> = Vec::with_capacity(PART_SIZE * 3);
        for (i, byte) in patterns.iter().enumerate() {
            let src = tmp.path().join(format!("src-{}.bin", i + 1));
            let data = vec![*byte; PART_SIZE];
            std::fs::write(&src, &data).unwrap();
            expected.extend_from_slice(&data);
            store
                .mpu_put_part(account_id, "b", "up", (i + 1) as u32, BodySource::File(src), "et", None)
                .unwrap();
        }
        let meta = sample_meta("k", expected.len() as u64);
        let body_ref = store.mpu_complete(account_id, "b", "up", "k", None, &meta).unwrap();
        let path = match body_ref {
            BodyRef::Disk { path, size, .. } => {
                assert_eq!(size, expected.len() as u64);
                path
            }
            _ => panic!("expected Disk"),
        };
        let actual = std::fs::read(&path).unwrap();
        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual, expected);
        assert!(!store.mpu_dir(account_id, "b", "up").exists());
    }

    #[test]
    fn mpu_resumable_across_load() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create(account_id, "b", "up", &sample_mpu_init("up", "k"))
            .unwrap();
        let p1 = Bytes::from_static(b"hello-");
        let p2 = Bytes::from_static(b"world-");
        store
            .mpu_put_part(account_id, "b", "up", 1, BodySource::Bytes(p1.clone()), "e1", None)
            .unwrap();
        store
            .mpu_put_part(account_id, "b", "up", 2, BodySource::Bytes(p2.clone()), "e2", None)
            .unwrap();

        // Simulate a restart: re-open the store on the same dir and load.
        let store2 = new_store_with_cache(&tmp, 1024 * 1024).0;
        let loaded = store2.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        let m = snap.multipart_uploads.get("up").unwrap();
        assert_eq!(m.parts.len(), 2);
        assert_eq!(m.parts.get(&1).unwrap().meta.etag, "e1");
        assert_eq!(m.parts.get(&2).unwrap().meta.etag, "e2");

        // Continue the upload on the fresh store.
        let p3 = Bytes::from_static(b"again!");
        store2
            .mpu_put_part(account_id, "b", "up", 3, BodySource::Bytes(p3.clone()), "e3", None)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&p1);
        expected.extend_from_slice(&p2);
        expected.extend_from_slice(&p3);
        let meta = sample_meta("k", expected.len() as u64);
        let body_ref = store2.mpu_complete(account_id, "b", "up", "k", None, &meta).unwrap();
        let path = match body_ref {
            BodyRef::Disk { path, .. } => path,
            _ => panic!(),
        };
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert!(!store2.mpu_dir(account_id, "b", "up").exists());
    }

    #[test]
    fn tags_snapshot_roundtrip_via_store() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut tags = BTreeMap::new();
        tags.insert("env".to_string(), "prod".to_string());
        tags.insert("team".to_string(), "s3".to_string());
        let snap = TagsSnapshot { tags: tags.clone() };
        let payload = toml::to_string(&snap).unwrap();
        store
            .put_bucket_subresource(account_id, "b", BucketSubresource::Tags, &payload)
            .unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let text = state
            .buckets
            .get("b")
            .unwrap()
            .subresources
            .get("tags.toml")
            .cloned()
            .unwrap();
        let decoded: TagsSnapshot = toml::from_str(&text).unwrap();
        assert_eq!(decoded.tags, tags);
    }

    #[test]
    fn acl_snapshot_roundtrip_via_store() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let snap = AclSnapshot {
            owner_id: "owner-abc".to_string(),
            grants: vec![
                AclGrantSnapshot {
                    grantee_type: "CanonicalUser".to_string(),
                    grantee_id: Some("owner-abc".to_string()),
                    grantee_display_name: Some("owner".to_string()),
                    grantee_uri: None,
                    permission: "FULL_CONTROL".to_string(),
                },
                AclGrantSnapshot {
                    grantee_type: "Group".to_string(),
                    grantee_id: None,
                    grantee_display_name: None,
                    grantee_uri: Some(
                        "http://acs.amazonaws.com/groups/global/AllUsers".to_string(),
                    ),
                    permission: "READ".to_string(),
                },
            ],
        };
        let payload = toml::to_string(&snap).unwrap();
        store
            .put_bucket_subresource(account_id, "b", BucketSubresource::Acl, &payload)
            .unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let text = state
            .buckets
            .get("b")
            .unwrap()
            .subresources
            .get("acl.toml")
            .cloned()
            .unwrap();
        let decoded: AclSnapshot = toml::from_str(&text).unwrap();
        assert_eq!(decoded.owner_id, "owner-abc");
        assert_eq!(decoded.grants.len(), 2);
        assert_eq!(decoded.grants[0].permission, "FULL_CONTROL");
        assert_eq!(decoded.grants[1].grantee_type, "Group");
    }

    #[test]
    fn inventory_snapshot_roundtrip_via_store() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut configs = BTreeMap::new();
        configs.insert(
            "inv-1".to_string(),
            "<InventoryConfiguration id=\"inv-1\"/>".to_string(),
        );
        configs.insert(
            "inv-2".to_string(),
            "<InventoryConfiguration id=\"inv-2\"/>".to_string(),
        );
        let snap = InventorySnapshot {
            configs: configs.clone(),
        };
        let payload = toml::to_string(&snap).unwrap();
        store
            .put_bucket_subresource(account_id, "b", BucketSubresource::Inventory, &payload)
            .unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let text = state
            .buckets
            .get("b")
            .unwrap()
            .subresources
            .get("inventory.toml")
            .cloned()
            .unwrap();
        let decoded: InventorySnapshot = toml::from_str(&text).unwrap();
        assert_eq!(decoded.configs, configs);
    }

    #[test]
    fn legacy_versioning_file_is_read() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        // Bucket meta with no versioning field set.
        store
            .put_bucket_meta(
                account_id,
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        // Legacy sidecar: a bare versioning.toml with "Enabled".
        let path = store.bucket_dir(account_id, "b").join("versioning.toml");
        std::fs::write(&path, "Enabled").unwrap();
        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        let snap = state.buckets.get("b").unwrap();
        assert_eq!(snap.meta.versioning.as_deref(), Some("Enabled"));
    }

    #[test]
    fn load_skips_corrupt_bucket_and_keeps_others() {
        // One corrupt bucket must be skipped with a warning, not abort the
        // whole store load (bug-audit 2026-06-20, 4.3).
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let account_id = "test-account";
        store
            .put_bucket_meta(
                account_id,
                "good",
                &BucketMeta {
                    name: "good".to_string(),
                    account_id: account_id.to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .put_object(
                account_id,
                "good",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(b"hi")),
                &sample_meta("k", 2),
            )
            .unwrap();

        let bad_dir = store.accounts_dir().join(account_id).join("buckets").join("bad");
        std::fs::create_dir_all(&bad_dir).unwrap();
        std::fs::write(bad_dir.join("meta.toml"), b"not valid toml = = =").unwrap();

        let loaded = store.load().unwrap();
        let state = loaded.get(account_id).unwrap();
        assert!(
            state.buckets.contains_key("good"),
            "valid bucket must load"
        );
        assert!(
            !state.buckets.contains_key("bad"),
            "corrupt bucket must be skipped, not abort the load"
        );
    }
}
