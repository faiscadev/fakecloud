use std::collections::BTreeMap;
use std::path::PathBuf;

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
    #[serde(default)]
    pub lock_mode: Option<String>,
    #[serde(default)]
    pub lock_retain_until: Option<DateTime<Utc>>,
    #[serde(default)]
    pub lock_legal_hold: Option<String>,
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
    #[serde(default)]
    pub tagging: Option<String>,
    #[serde(default)]
    pub acl_grants: Vec<AclGrantSnapshot>,
    #[serde(default)]
    pub checksum_algorithm: Option<String>,
    #[serde(default)]
    pub cache_control: Option<String>,
    #[serde(default)]
    pub content_disposition: Option<String>,
    #[serde(default)]
    pub content_language: Option<String>,
    #[serde(default)]
    pub expires: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UploadPartMeta {
    pub part_number: u32,
    pub etag: String,
    pub size: u64,
    #[serde(default = "default_time")]
    pub last_modified: DateTime<Utc>,
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

pub trait S3Store: Send + Sync {
    fn load(&self) -> StoreResult<S3State>;

    fn put_bucket_meta(&self, bucket: &str, meta: &BucketMeta) -> StoreResult<()>;
    fn put_bucket_subresource(
        &self,
        bucket: &str,
        kind: BucketSubresource,
        payload: &str,
    ) -> StoreResult<()>;
    fn delete_bucket_subresource(&self, bucket: &str, kind: BucketSubresource) -> StoreResult<()>;
    fn delete_bucket(&self, bucket: &str) -> StoreResult<()>;

    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        body: BodySource,
        meta: &ObjectMeta,
    ) -> StoreResult<BodyRef>;
    fn put_object_meta(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        meta: &ObjectMeta,
    ) -> StoreResult<()>;
    fn delete_object(&self, bucket: &str, key: &str, version: Option<&str>) -> StoreResult<()>;
    fn open_object_body(&self, body: &BodyRef) -> StoreResult<Bytes>;

    fn mpu_create(&self, bucket: &str, upload_id: &str, init: &MpuInit) -> StoreResult<()>;
    /// Persist a multipart-upload part body. Returns the [`BodyRef`] the
    /// service should keep in its in-memory part map: `BodyRef::Disk`
    /// for disk-backed stores (so subsequent reads stream from the
    /// `.bin` file instead of holding the part in RAM), or
    /// `BodyRef::Memory` for memory-mode stores.
    fn mpu_put_part(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        body: BodySource,
        etag: &str,
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
    fn mpu_abort(&self, bucket: &str, upload_id: &str) -> StoreResult<()>;
    fn mpu_complete(
        &self,
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
    fn load(&self) -> StoreResult<S3State> {
        Ok(S3State::default())
    }

    fn put_bucket_meta(&self, _bucket: &str, _meta: &BucketMeta) -> StoreResult<()> {
        Ok(())
    }
    fn put_bucket_subresource(
        &self,
        _bucket: &str,
        _kind: BucketSubresource,
        _payload: &str,
    ) -> StoreResult<()> {
        Ok(())
    }
    fn delete_bucket_subresource(
        &self,
        _bucket: &str,
        _kind: BucketSubresource,
    ) -> StoreResult<()> {
        Ok(())
    }
    fn delete_bucket(&self, _bucket: &str) -> StoreResult<()> {
        Ok(())
    }

    fn put_object(
        &self,
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
        _bucket: &str,
        _key: &str,
        _version: Option<&str>,
        _meta: &ObjectMeta,
    ) -> StoreResult<()> {
        Ok(())
    }
    fn delete_object(&self, _bucket: &str, _key: &str, _version: Option<&str>) -> StoreResult<()> {
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

    fn mpu_create(&self, _bucket: &str, _upload_id: &str, _init: &MpuInit) -> StoreResult<()> {
        Ok(())
    }
    fn mpu_put_part(
        &self,
        _bucket: &str,
        _upload_id: &str,
        _part_number: u32,
        body: BodySource,
        _etag: &str,
    ) -> StoreResult<BodyRef> {
        // Memory mode keeps all part bodies in RAM. Absorb any tempfile
        // produced by the streaming dispatch path here.
        Ok(BodyRef::Memory(read_body_source(body)?))
    }
    fn mpu_abort(&self, _bucket: &str, _upload_id: &str) -> StoreResult<()> {
        Ok(())
    }
    fn mpu_complete(
        &self,
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
}

impl DiskS3Store {
    pub fn new(root: PathBuf, cache: std::sync::Arc<crate::cache::BodyCache>) -> Self {
        Self { root, cache }
    }

    fn buckets_dir(&self) -> PathBuf {
        self.root.join("buckets")
    }

    fn bucket_dir(&self, bucket: &str) -> PathBuf {
        self.buckets_dir()
            .join(crate::key_escape::escape_key_segment(bucket))
    }

    fn object_dir(&self, bucket: &str, key: &str) -> PathBuf {
        self.bucket_dir(bucket)
            .join("objects")
            .join(crate::key_escape::escape_key_segment(key))
    }

    fn version_tag(version: Option<&str>) -> String {
        version.unwrap_or("null").to_string()
    }

    fn object_paths(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
    ) -> (PathBuf, PathBuf, PathBuf) {
        let dir = self.object_dir(bucket, key);
        let tag = Self::version_tag(version);
        let bin = dir.join(format!("{}.bin", tag));
        let toml = dir.join(format!("{}.toml", tag));
        (dir, bin, toml)
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

    fn mpu_dir(&self, bucket: &str, upload_id: &str) -> PathBuf {
        self.bucket_dir(bucket)
            .join("mpu")
            .join(crate::key_escape::escape_key_segment(upload_id))
    }

    fn mpu_parts_dir(&self, bucket: &str, upload_id: &str) -> PathBuf {
        self.mpu_dir(bucket, upload_id).join("parts")
    }

    fn mpu_part_bin(&self, bucket: &str, upload_id: &str, part_number: u32) -> PathBuf {
        self.mpu_parts_dir(bucket, upload_id)
            .join(format!("{}.bin", part_number))
    }

    fn mpu_part_toml(&self, bucket: &str, upload_id: &str, part_number: u32) -> PathBuf {
        self.mpu_parts_dir(bucket, upload_id)
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

fn io_other(msg: impl Into<String>) -> StoreError {
    StoreError::Other(msg.into())
}

impl S3Store for DiskS3Store {
    fn load(&self) -> StoreResult<S3State> {
        let mut state = S3State::default();
        let buckets_dir = self.buckets_dir();
        if !buckets_dir.exists() {
            return Ok(state);
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
                        for version_entry in std::fs::read_dir(&key_dir)? {
                            let version_entry = version_entry?;
                            let path = version_entry.path();
                            let Some(fname) = path.file_name().and_then(|s| s.to_str()) else {
                                continue;
                            };
                            if !fname.ends_with(".toml") {
                                continue;
                            }
                            let version_tag = &fname[..fname.len() - 5];
                            let toml_text = std::fs::read_to_string(&path)?;
                            let obj_meta: ObjectMeta = toml::from_str(&toml_text)
                                .map_err(|e| StoreError::Serde(e.to_string()))?;
                            let bin_path = key_dir.join(format!("{}.bin", version_tag));
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
                Err(e) => tracing::warn!(
                    bucket = %bdir.display(),
                    error = %e,
                    "skipping unreadable S3 bucket during load"
                ),
            }
        }
        Ok(state)
    }

    fn put_bucket_meta(&self, bucket: &str, meta: &BucketMeta) -> StoreResult<()> {
        let dir = self.bucket_dir(bucket);
        std::fs::create_dir_all(&dir)?;
        crate::atomic::write_atomic_toml(&dir.join("meta.toml"), meta)?;
        Ok(())
    }

    fn put_bucket_subresource(
        &self,
        bucket: &str,
        kind: BucketSubresource,
        payload: &str,
    ) -> StoreResult<()> {
        let dir = self.bucket_dir(bucket);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(Self::subresource_filename(kind));
        crate::atomic::write_atomic_bytes(&path, payload.as_bytes())?;
        Ok(())
    }

    fn delete_bucket_subresource(&self, bucket: &str, kind: BucketSubresource) -> StoreResult<()> {
        let path = self
            .bucket_dir(bucket)
            .join(Self::subresource_filename(kind));
        match std::fs::remove_file(&path) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn delete_bucket(&self, bucket: &str) -> StoreResult<()> {
        let dir = self.bucket_dir(bucket);
        match std::fs::remove_dir_all(&dir) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        body: BodySource,
        meta: &ObjectMeta,
    ) -> StoreResult<BodyRef> {
        let (dir, bin_path, toml_path) = self.object_paths(bucket, key, version);
        std::fs::create_dir_all(&dir)?;

        if meta.is_delete_marker {
            crate::atomic::write_atomic_toml(&toml_path, meta)?;
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

        crate::atomic::write_atomic_toml(&toml_path, meta)?;

        let body_key = crate::cache::BodyKey::new(
            bucket.to_string(),
            key.to_string(),
            version.map(|s| s.to_string()),
        );
        if let Some(b) = bytes_for_cache {
            self.cache.insert(body_key, b);
        } else {
            self.cache.invalidate(&crate::cache::BodyKey::new(
                bucket.to_string(),
                key.to_string(),
                version.map(|s| s.to_string()),
            ));
        }

        Ok(BodyRef::Disk {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version: version.map(|s| s.to_string()),
            path: bin_path,
            size,
        })
    }

    fn put_object_meta(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        meta: &ObjectMeta,
    ) -> StoreResult<()> {
        let (dir, _bin, toml_path) = self.object_paths(bucket, key, version);
        std::fs::create_dir_all(&dir)?;
        crate::atomic::write_atomic_toml(&toml_path, meta)?;
        Ok(())
    }

    fn delete_object(&self, bucket: &str, key: &str, version: Option<&str>) -> StoreResult<()> {
        let (dir, bin_path, toml_path) = self.object_paths(bucket, key, version);
        for p in [&bin_path, &toml_path] {
            match std::fs::remove_file(p) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Self::cleanup_empty(&dir);

        self.cache.invalidate(&crate::cache::BodyKey::new(
            bucket.to_string(),
            key.to_string(),
            version.map(|s| s.to_string()),
        ));
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

    fn mpu_create(&self, bucket: &str, upload_id: &str, init: &MpuInit) -> StoreResult<()> {
        let parts_dir = self.mpu_parts_dir(bucket, upload_id);
        std::fs::create_dir_all(&parts_dir)?;
        let init_path = self.mpu_dir(bucket, upload_id).join("init.toml");
        crate::atomic::write_atomic_toml(&init_path, init)?;
        Ok(())
    }

    fn mpu_put_part(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        body: BodySource,
        etag: &str,
    ) -> StoreResult<BodyRef> {
        let parts_dir = self.mpu_parts_dir(bucket, upload_id);
        std::fs::create_dir_all(&parts_dir)?;
        let bin_path = self.mpu_part_bin(bucket, upload_id, part_number);
        let toml_path = self.mpu_part_toml(bucket, upload_id, part_number);

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

    fn mpu_abort(&self, bucket: &str, upload_id: &str) -> StoreResult<()> {
        let dir = self.mpu_dir(bucket, upload_id);
        // Invalidate any cached part bodies for this upload.
        if let Ok(entries) = std::fs::read_dir(self.mpu_parts_dir(bucket, upload_id)) {
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
        bucket: &str,
        upload_id: &str,
        final_key: &str,
        version: Option<&str>,
        meta: &ObjectMeta,
    ) -> StoreResult<BodyRef> {
        let parts_dir = self.mpu_parts_dir(bucket, upload_id);
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
                    if self.mpu_part_bin(bucket, upload_id, n).exists() {
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

        let (dir, bin_path, toml_path) = self.object_paths(bucket, final_key, version);
        std::fs::create_dir_all(&dir)?;

        let total_size: u64 = if part_numbers.len() == 1 {
            let only = self.mpu_part_bin(bucket, upload_id, part_numbers[0]);
            let sz = std::fs::metadata(&only)?.len();
            match std::fs::rename(&only, &bin_path) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc_exdev()) => {
                    // Cross-device rename: fall back to streaming copy then remove source.
                    {
                        let mut input = std::fs::File::open(&only)?;
                        let tmp = {
                            let mut os = bin_path.as_os_str().to_owned();
                            os.push(".tmp");
                            PathBuf::from(os)
                        };
                        let mut out = std::fs::OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .open(&tmp)?;
                        std::io::copy(&mut input, &mut out)?;
                        out.sync_all()?;
                        std::fs::rename(&tmp, &bin_path)?;
                    }
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
            let tmp = {
                let mut os = bin_path.as_os_str().to_owned();
                os.push(".tmp");
                PathBuf::from(os)
            };
            let mut total: u64 = 0;
            {
                let mut out = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&tmp)?;
                for n in &part_numbers {
                    let part_path = self.mpu_part_bin(bucket, upload_id, *n);
                    let mut input = std::fs::File::open(&part_path)?;
                    let copied = std::io::copy(&mut input, &mut out)?;
                    total += copied;
                }
                out.sync_all()?;
            }
            std::fs::rename(&tmp, &bin_path)?;
            if let Some(parent) = bin_path.parent() {
                if let Ok(dir_handle) = std::fs::File::open(parent) {
                    let _ = dir_handle.sync_all();
                }
            }
            total
        };

        crate::atomic::write_atomic_toml(&toml_path, meta)?;

        // Invalidate per-part cache entries and drop the mpu dir.
        for n in &part_numbers {
            self.cache
                .invalidate(&Self::mpu_body_key(bucket, upload_id, *n));
        }
        let mpu_dir = self.mpu_dir(bucket, upload_id);
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

    #[test]
    fn put_bucket_meta_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        let meta = BucketMeta {
            name: "b1".to_string(),
            region: "us-east-1".to_string(),
            versioning: Some("Enabled".to_string()),
            ..Default::default()
        };
        store.put_bucket_meta("b1", &meta).unwrap();
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b1").unwrap();
        assert_eq!(snap.meta.name, "b1");
        assert_eq!(snap.meta.region, "us-east-1");
        assert_eq!(snap.meta.versioning.as_deref(), Some("Enabled"));
    }

    #[test]
    fn put_bucket_subresource_each_variant_writes_file() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
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
                .put_bucket_subresource("b", v, "payload=true")
                .unwrap();
            let file = store
                .bucket_dir("b")
                .join(DiskS3Store::subresource_filename(v));
            assert!(file.exists(), "{:?}", v);
            assert_eq!(std::fs::read_to_string(&file).unwrap(), "payload=true");
        }
    }

    #[test]
    fn delete_bucket_subresource_removes_file() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .put_bucket_subresource("b", BucketSubresource::Tags, "x=1")
            .unwrap();
        store
            .delete_bucket_subresource("b", BucketSubresource::Tags)
            .unwrap();
        let file = store.bucket_dir("b").join("tags.toml");
        assert!(!file.exists());
        // idempotent
        store
            .delete_bucket_subresource("b", BucketSubresource::Tags)
            .unwrap();
    }

    #[test]
    fn put_object_bytes_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from_static(b"hello world");
        let meta = sample_meta("k1", data.len() as u64);
        let body_ref = store
            .put_object("b", "k1", None, BodySource::Bytes(data.clone()), &meta)
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
        let snap = loaded.buckets.get("b").unwrap();
        let obj = snap.objects.get("k1").unwrap();
        assert_eq!(obj.meta.size, data.len() as u64);
    }

    #[test]
    fn put_object_file_copy_source_leaves_src_in_place() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let src_dir = TempDir::new().unwrap();
        let src = src_dir.path().join("src.bin");
        std::fs::write(&src, b"copied-body").unwrap();
        let meta = sample_meta("k", 11);
        let bref = store
            .put_object("b", "k", None, BodySource::FileCopy(src.clone()), &meta)
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let src = tmp.path().join("src.bin");
        std::fs::write(&src, b"file-body").unwrap();
        let meta = sample_meta("k", 9);
        let body_ref = store
            .put_object("b", "k", None, BodySource::File(src.clone()), &meta)
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from_static(b"abc");
        let mut meta = sample_meta("k", 3);
        store
            .put_object("b", "k", None, BodySource::Bytes(data.clone()), &meta)
            .unwrap();
        let (_, bin, _) = store.object_paths("b", "k", None);
        let before = std::fs::read(&bin).unwrap();
        meta.tags.insert("x".to_string(), "y".to_string());
        store.put_object_meta("b", "k", None, &meta).unwrap();
        assert_eq!(std::fs::read(&bin).unwrap(), before);
        let loaded = store.load().unwrap();
        let obj = loaded.buckets.get("b").unwrap().objects.get("k").unwrap();
        assert_eq!(obj.meta.tags.get("x").map(String::as_str), Some("y"));
    }

    #[test]
    fn delete_object_cleans_up_files_and_cache() {
        let tmp = TempDir::new().unwrap();
        let (store, cache) = new_store_with_cache(&tmp, 1024 * 1024);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from_static(b"bye");
        let meta = sample_meta("k", 3);
        store
            .put_object("b", "k", None, BodySource::Bytes(data), &meta)
            .unwrap();
        let body_key = crate::cache::BodyKey::new("b".to_string(), "k".to_string(), None);
        assert!(cache.get(&body_key).is_some());
        store.delete_object("b", "k", None).unwrap();
        let (dir, bin, toml_path) = store.object_paths("b", "k", None);
        assert!(!bin.exists());
        assert!(!toml_path.exists());
        assert!(!dir.exists());
        assert!(cache.get(&body_key).is_none());
    }

    #[test]
    fn open_object_body_cache_hit_and_refill() {
        let tmp = TempDir::new().unwrap();
        let (store, cache) = new_store_with_cache(&tmp, 1024 * 1024);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from_static(b"payload");
        let meta = sample_meta("k", data.len() as u64);
        let body_ref = store
            .put_object("b", "k", None, BodySource::Bytes(data.clone()), &meta)
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from(vec![7u8; 800]);
        let meta = sample_meta("big", 800);
        let body_ref = store
            .put_object("b", "big", None, BodySource::Bytes(data.clone()), &meta)
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
        assert!(s.buckets.is_empty());
    }

    #[test]
    fn load_skips_mpu_without_init() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let data = Bytes::from_static(b"x");
        let meta = sample_meta("k", 1);
        store
            .put_object("b", "k", None, BodySource::Bytes(data), &meta)
            .unwrap();
        // Directory with no init.toml is skipped by load.
        let mpu = store.bucket_dir("b").join("mpu").join("upload1");
        std::fs::create_dir_all(&mpu).unwrap();

        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
        assert_eq!(snap.objects.len(), 1);
        assert!(snap.multipart_uploads.is_empty());
    }

    #[test]
    fn load_reads_bucket_subresources() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .put_bucket_subresource("b", BucketSubresource::Lifecycle, "<Lifecycle/>")
            .unwrap();
        store
            .put_bucket_subresource("b", BucketSubresource::Cors, "<Cors/>")
            .unwrap();
        store
            .put_bucket_subresource("b", BucketSubresource::Policy, "{}")
            .unwrap();
        store
            .put_bucket_subresource("b", BucketSubresource::Tags, "x=1")
            .unwrap();
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
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
                    "b",
                    "k",
                    Some(*vid),
                    BodySource::Bytes(Bytes::copy_from_slice(body.as_bytes())),
                    &m,
                )
                .unwrap();
        }
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
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
                    "b",
                    "k",
                    Some(*vid),
                    BodySource::Bytes(Bytes::copy_from_slice(body.as_bytes())),
                    &m,
                )
                .unwrap();
        }
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
        let current = snap.objects.get("k").expect("current object promoted");
        assert_eq!(current.meta.version_id.as_deref(), Some("v3"));
    }

    #[test]
    fn versioned_load_trailing_delete_marker_hides_current() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
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
            .put_object("b", "k", Some("dm1"), BodySource::Bytes(Bytes::new()), &dm)
            .unwrap();
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let base = chrono::Utc::now();
        let mut null_meta = sample_meta("k", 3);
        null_meta.last_modified = base;
        store
            .put_object(
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
                "b",
                &BucketMeta {
                    name: "b".to_string(),
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
                "b",
                "k",
                Some("v2"),
                BodySource::Bytes(Bytes::from_static(b"newer")),
                &v2,
            )
            .unwrap();
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let base = chrono::Utc::now();
        let mut null_meta = sample_meta("k", 3);
        null_meta.last_modified = base;
        store
            .put_object(
                "b",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(b"old")),
                &null_meta,
            )
            .unwrap();
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
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
            .put_object("b", "k", Some("dm1"), BodySource::Bytes(Bytes::new()), &dm)
            .unwrap();
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
        assert!(
            !snap.objects.contains_key("k"),
            "trailing delete marker must hide even a legacy null object",
        );
    }

    #[test]
    fn delete_marker_roundtrip_no_body_file() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    versioning: Some("Enabled".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut m = sample_meta("k", 0);
        m.version_id = Some("dm1".to_string());
        m.is_delete_marker = true;
        store
            .put_object("b", "k", Some("dm1"), BodySource::Bytes(Bytes::new()), &m)
            .unwrap();
        // No .bin file on disk for delete markers.
        let (_, bin, toml_path) = store.object_paths("b", "k", Some("dm1"));
        assert!(!bin.exists(), "delete marker must not have a .bin file");
        assert!(toml_path.exists());

        let loaded = store.load().unwrap();
        let versions = loaded
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
        store
            .put_bucket_meta(
                "a",
                &BucketMeta {
                    name: "a".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    versioning: Some("Enabled".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let ma = sample_meta("only", 3);
        store
            .put_object(
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
                    "b",
                    "twice",
                    Some(*vid),
                    BodySource::Bytes(Bytes::from_static(b"xx")),
                    &m,
                )
                .unwrap();
        }
        let loaded = store.load().unwrap();
        assert_eq!(loaded.buckets.len(), 2);
        let a = loaded.buckets.get("a").unwrap();
        assert_eq!(a.objects.len(), 1);
        assert!(a.object_versions.is_empty());
        let b = loaded.buckets.get("b").unwrap();
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        let init = sample_mpu_init("up1", "k1");
        store.mpu_create("b", "up1", &init).unwrap();
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
        let m = snap.multipart_uploads.get("up1").expect("upload present");
        assert_eq!(m.init.upload_id, "up1");
        assert_eq!(m.init.key, "k1");
        assert!(m.parts.is_empty());
    }

    #[test]
    fn mpu_put_part_then_load_three_parts() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create("b", "up", &sample_mpu_init("up", "k"))
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
                    "b",
                    "up",
                    n,
                    BodySource::Bytes(body.clone()),
                    &format!("et{}", n),
                )
                .unwrap();
        }
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
        let m = snap.multipart_uploads.get("up").unwrap();
        assert_eq!(m.parts.len(), 3);
        for (i, body) in bodies.iter().enumerate() {
            let n = (i + 1) as u32;
            let part = m.parts.get(&n).unwrap();
            assert_eq!(part.meta.part_number, n);
            assert_eq!(part.meta.size, body.len() as u64);
            assert_eq!(part.meta.etag, format!("et{}", n));
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create("b", "up", &sample_mpu_init("up", "k"))
            .unwrap();
        store
            .mpu_put_part(
                "b",
                "up",
                1,
                BodySource::Bytes(Bytes::from_static(b"x")),
                "e",
            )
            .unwrap();
        store.mpu_abort("b", "up").unwrap();
        assert!(!store.mpu_dir("b", "up").exists());
        // Idempotent.
        store.mpu_abort("b", "up").unwrap();
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
        assert!(snap.multipart_uploads.is_empty());
    }

    #[test]
    fn mpu_complete_single_part_fast_path() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create("b", "up", &sample_mpu_init("up", "k"))
            .unwrap();
        let body = Bytes::from_static(b"only-part-bytes");
        store
            .mpu_put_part("b", "up", 1, BodySource::Bytes(body.clone()), "et")
            .unwrap();
        let meta = sample_meta("k", body.len() as u64);
        let body_ref = store.mpu_complete("b", "up", "k", None, &meta).unwrap();
        match &body_ref {
            BodyRef::Disk { path, size, .. } => {
                assert_eq!(*size, body.len() as u64);
                assert_eq!(std::fs::read(path).unwrap(), body.to_vec());
            }
            _ => panic!("expected Disk"),
        }
        assert!(!store.mpu_dir("b", "up").exists());
    }

    #[test]
    fn mpu_complete_multi_part_concat() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create("b", "up", &sample_mpu_init("up", "k"))
            .unwrap();
        let p1 = Bytes::from_static(b"AAAA");
        let p2 = Bytes::from_static(b"BBBBBB");
        let p3 = Bytes::from_static(b"CC");
        for (n, b) in [(1u32, &p1), (2, &p2), (3, &p3)] {
            store
                .mpu_put_part("b", "up", n, BodySource::Bytes(b.clone()), "e")
                .unwrap();
        }
        let mut expected = Vec::new();
        expected.extend_from_slice(&p1);
        expected.extend_from_slice(&p2);
        expected.extend_from_slice(&p3);
        let meta = sample_meta("k", expected.len() as u64);
        let body_ref = store.mpu_complete("b", "up", "k", None, &meta).unwrap();
        let path = match body_ref {
            BodyRef::Disk { path, size, .. } => {
                assert_eq!(size, expected.len() as u64);
                path
            }
            _ => panic!("expected Disk"),
        };
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert!(!store.mpu_dir("b", "up").exists());
    }

    #[test]
    fn mpu_complete_large_streaming_via_file_source() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create("b", "up", &sample_mpu_init("up", "k"))
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
                .mpu_put_part("b", "up", (i + 1) as u32, BodySource::File(src), "et")
                .unwrap();
        }
        let meta = sample_meta("k", expected.len() as u64);
        let body_ref = store.mpu_complete("b", "up", "k", None, &meta).unwrap();
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
        assert!(!store.mpu_dir("b", "up").exists());
    }

    #[test]
    fn mpu_resumable_across_load() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .mpu_create("b", "up", &sample_mpu_init("up", "k"))
            .unwrap();
        let p1 = Bytes::from_static(b"hello-");
        let p2 = Bytes::from_static(b"world-");
        store
            .mpu_put_part("b", "up", 1, BodySource::Bytes(p1.clone()), "e1")
            .unwrap();
        store
            .mpu_put_part("b", "up", 2, BodySource::Bytes(p2.clone()), "e2")
            .unwrap();

        // Simulate a restart: re-open the store on the same dir and load.
        let store2 = new_store_with_cache(&tmp, 1024 * 1024).0;
        let loaded = store2.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
        let m = snap.multipart_uploads.get("up").unwrap();
        assert_eq!(m.parts.len(), 2);
        assert_eq!(m.parts.get(&1).unwrap().meta.etag, "e1");
        assert_eq!(m.parts.get(&2).unwrap().meta.etag, "e2");

        // Continue the upload on the fresh store.
        let p3 = Bytes::from_static(b"again!");
        store2
            .mpu_put_part("b", "up", 3, BodySource::Bytes(p3.clone()), "e3")
            .unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&p1);
        expected.extend_from_slice(&p2);
        expected.extend_from_slice(&p3);
        let meta = sample_meta("k", expected.len() as u64);
        let body_ref = store2.mpu_complete("b", "up", "k", None, &meta).unwrap();
        let path = match body_ref {
            BodyRef::Disk { path, .. } => path,
            _ => panic!(),
        };
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert!(!store2.mpu_dir("b", "up").exists());
    }

    #[test]
    fn tags_snapshot_roundtrip_via_store() {
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
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
            .put_bucket_subresource("b", BucketSubresource::Tags, &payload)
            .unwrap();
        let loaded = store.load().unwrap();
        let text = loaded
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
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
            .put_bucket_subresource("b", BucketSubresource::Acl, &payload)
            .unwrap();
        let loaded = store.load().unwrap();
        let text = loaded
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
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
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
            .put_bucket_subresource("b", BucketSubresource::Inventory, &payload)
            .unwrap();
        let loaded = store.load().unwrap();
        let text = loaded
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
        // Bucket meta with no versioning field set.
        store
            .put_bucket_meta(
                "b",
                &BucketMeta {
                    name: "b".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        // Legacy sidecar: a bare versioning.toml with "Enabled".
        let path = store.bucket_dir("b").join("versioning.toml");
        std::fs::write(&path, "Enabled").unwrap();
        let loaded = store.load().unwrap();
        let snap = loaded.buckets.get("b").unwrap();
        assert_eq!(snap.meta.versioning.as_deref(), Some("Enabled"));
    }

    #[test]
    fn load_skips_corrupt_bucket_and_keeps_others() {
        // One corrupt bucket must be skipped with a warning, not abort the
        // whole store load (bug-audit 2026-06-20, 4.3).
        let tmp = TempDir::new().unwrap();
        let store = new_store(&tmp);
        store
            .put_bucket_meta(
                "good",
                &BucketMeta {
                    name: "good".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        store
            .put_object(
                "good",
                "k",
                None,
                BodySource::Bytes(Bytes::from_static(b"hi")),
                &sample_meta("k", 2),
            )
            .unwrap();

        let bad_dir = store.buckets_dir().join("bad");
        std::fs::create_dir_all(&bad_dir).unwrap();
        std::fs::write(bad_dir.join("meta.toml"), b"not valid toml = = =").unwrap();

        let loaded = store.load().unwrap();
        assert!(
            loaded.buckets.contains_key("good"),
            "valid bucket must load"
        );
        assert!(
            !loaded.buckets.contains_key("bad"),
            "corrupt bucket must be skipped, not abort the load"
        );
    }
}
