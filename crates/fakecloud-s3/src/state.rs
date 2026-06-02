use bytes::Bytes;
use chrono::{DateTime, Utc};
use fakecloud_persistence::cache::{BodyCache, BodyKey};
use fakecloud_persistence::BodyRef;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;

/// An ACL grant entry.
#[derive(Debug, Clone)]
pub struct AclGrant {
    pub grantee_type: String, // "CanonicalUser" or "Group"
    pub grantee_id: Option<String>,
    pub grantee_display_name: Option<String>,
    pub grantee_uri: Option<String>,
    pub permission: String, // READ, WRITE, READ_ACP, WRITE_ACP, FULL_CONTROL
}

#[derive(Debug, Clone, Default)]
pub struct S3Object {
    pub key: String,
    pub body: BodyRef,
    pub content_type: String,
    pub etag: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    pub metadata: BTreeMap<String, String>,
    pub storage_class: String,
    pub tags: BTreeMap<String, String>,
    pub acl_grants: Vec<AclGrant>,
    pub acl_owner_id: Option<String>,
    /// If created from multipart upload, the number of parts.
    pub parts_count: Option<u32>,
    /// Per-part sizes for multipart objects (part_number, size).
    pub part_sizes: Option<Vec<(u32, u64)>>,
    /// Server-side encryption algorithm.
    pub sse_algorithm: Option<String>,
    /// KMS key ID for SSE-KMS.
    pub sse_kms_key_id: Option<String>,
    /// Whether bucket key is enabled.
    pub bucket_key_enabled: Option<bool>,
    pub version_id: Option<String>,
    pub is_delete_marker: bool,
    pub content_encoding: Option<String>,
    pub website_redirect_location: Option<String>,
    /// Glacier restore: ongoing request status.
    pub restore_ongoing: Option<bool>,
    /// Glacier restore: expiry date string.
    pub restore_expiry: Option<String>,
    /// Checksum algorithm (CRC32, SHA1, SHA256).
    pub checksum_algorithm: Option<String>,
    /// Base64-encoded checksum value.
    pub checksum_value: Option<String>,
    /// Object lock mode (GOVERNANCE or COMPLIANCE).
    pub lock_mode: Option<String>,
    /// Object lock retain-until date (ISO 8601).
    pub lock_retain_until: Option<DateTime<Utc>>,
    /// Legal hold status (ON or OFF).
    pub lock_legal_hold: Option<String>,
}

/// A part uploaded via the multipart upload API.
#[derive(Debug, Clone)]
pub struct UploadPart {
    pub part_number: u32,
    pub body: BodyRef,
    pub etag: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
}

/// An in-progress multipart upload.
#[derive(Debug, Clone)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub key: String,
    pub initiated: DateTime<Utc>,
    /// Parts keyed by part number.
    pub parts: BTreeMap<u32, UploadPart>,
    /// Metadata provided at CreateMultipartUpload time.
    pub metadata: BTreeMap<String, String>,
    pub content_type: String,
    pub storage_class: String,
    pub sse_algorithm: Option<String>,
    pub sse_kms_key_id: Option<String>,
    pub tagging: Option<String>,
    pub acl_grants: Vec<AclGrant>,
    pub checksum_algorithm: Option<String>,
}

#[derive(Debug, Clone)]
pub struct S3Bucket {
    pub name: String,
    pub creation_date: DateTime<Utc>,
    pub region: String,
    /// Objects keyed by their full key path.
    pub objects: BTreeMap<String, S3Object>,
    pub tags: BTreeMap<String, String>,
    pub acl_grants: Vec<AclGrant>,
    pub acl_owner_id: String,
    /// In-progress multipart uploads keyed by upload ID.
    pub multipart_uploads: BTreeMap<String, MultipartUpload>,
    /// Versioning status: None = never enabled, Some("Enabled"), Some("Suspended").
    pub versioning: Option<String>,
    /// Object versions keyed by key, each value is a list of versions.
    pub object_versions: BTreeMap<String, Vec<S3Object>>,
    /// Bucket ACL (canned or XML).
    pub acl: Option<String>,
    pub encryption_config: Option<String>,
    pub lifecycle_config: Option<String>,
    /// Value of the `x-amz-transition-default-minimum-object-size` header
    /// supplied on PutBucketLifecycleConfiguration. Echoed back as a header
    /// on the corresponding GET (and PUT) response. Real AWS defaults to
    /// `all_storage_classes_128K` for general purpose buckets.
    pub lifecycle_transition_default_min_size: Option<String>,
    pub policy: Option<String>,
    pub cors_config: Option<String>,
    pub notification_config: Option<String>,
    pub logging_config: Option<String>,
    pub website_config: Option<String>,
    pub accelerate_status: Option<String>,
    pub public_access_block: Option<String>,
    pub object_lock_config: Option<String>,
    pub replication_config: Option<String>,
    pub ownership_controls: Option<String>,
    pub inventory_configs: BTreeMap<String, String>,
    /// Whether EventBridge notifications are enabled for this bucket.
    pub eventbridge_enabled: bool,
    /// Per-id analytics configurations (XML body).
    pub analytics_configs: BTreeMap<String, String>,
    /// Per-id intelligent-tiering configurations (XML body).
    pub intelligent_tiering_configs: BTreeMap<String, String>,
    /// Per-id metrics configurations (XML body).
    pub metrics_configs: BTreeMap<String, String>,
    /// Request payment configuration (XML body).
    pub request_payment: Option<String>,
    /// Per-bucket ABAC config (XML body) — see PutBucketAbac/GetBucketAbac.
    pub abac_config: Option<String>,
    /// Bucket-level metadata configuration (S3 metadata table v2).
    pub metadata_configuration: Option<String>,
    /// Bucket-level metadata table configuration (S3 metadata table v1).
    pub metadata_table_configuration: Option<String>,
}

impl S3Bucket {
    pub fn new(name: &str, region: &str, owner_id: &str) -> Self {
        Self {
            name: name.to_string(),
            creation_date: Utc::now(),
            region: region.to_string(),
            objects: BTreeMap::new(),
            tags: BTreeMap::new(),
            acl_grants: vec![AclGrant {
                grantee_type: "CanonicalUser".to_string(),
                grantee_id: Some(owner_id.to_string()),
                grantee_display_name: Some(owner_id.to_string()),
                grantee_uri: None,
                permission: "FULL_CONTROL".to_string(),
            }],
            acl_owner_id: owner_id.to_string(),
            multipart_uploads: BTreeMap::new(),
            versioning: None,
            object_versions: BTreeMap::new(),
            acl: None,
            encryption_config: None,
            lifecycle_config: None,
            lifecycle_transition_default_min_size: None,
            policy: None,
            cors_config: None,
            notification_config: None,
            logging_config: None,
            website_config: None,
            accelerate_status: None,
            public_access_block: None,
            object_lock_config: None,
            replication_config: None,
            ownership_controls: None,
            inventory_configs: BTreeMap::new(),
            eventbridge_enabled: false,
            analytics_configs: BTreeMap::new(),
            intelligent_tiering_configs: BTreeMap::new(),
            metrics_configs: BTreeMap::new(),
            request_payment: None,
            abac_config: None,
            metadata_configuration: None,
            metadata_table_configuration: None,
        }
    }
}

/// A recorded S3 notification event for introspection.
#[derive(Debug, Clone)]
pub struct S3NotificationEvent {
    pub bucket: String,
    pub key: String,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
}

/// Stored response from a Lambda function invoked via S3 Object Lambda.
/// Keyed by `request_token` in [`S3State::object_lambda_responses`].
#[derive(Debug, Clone)]
pub struct ObjectLambdaResponse {
    pub route: String,
    pub token: String,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    pub fwd_status: Option<u16>,
    pub fwd_error_message: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub encryption: Option<String>,
    pub kms_key_id: Option<String>,
    pub stored_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct S3AccessPoint {
    pub name: String,
    pub bucket: String,
    pub account_id: String,
    pub network_origin: String,
    pub vpc_configuration: Option<String>,
    pub creation_date: DateTime<Utc>,
    pub public_access_block: Option<String>,
    pub bucket_account_id: Option<String>,
}

pub struct S3State {
    pub account_id: String,
    pub region: String,
    pub buckets: BTreeMap<String, S3Bucket>,
    pub notification_events: Vec<S3NotificationEvent>,
    pub body_cache: Option<Arc<BodyCache>>,
    /// Object Lambda responses keyed by request token.
    pub object_lambda_responses: BTreeMap<String, ObjectLambdaResponse>,
    pub access_points: BTreeMap<String, S3AccessPoint>,
}

impl S3State {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            buckets: BTreeMap::new(),
            notification_events: Vec::new(),
            body_cache: None,
            object_lambda_responses: BTreeMap::new(),
            access_points: BTreeMap::new(),
        }
    }

    pub fn set_body_cache(&mut self, cache: Arc<BodyCache>) {
        self.body_cache = Some(cache);
    }

    pub fn reset(&mut self) {
        self.buckets.clear();
        self.notification_events.clear();
        self.object_lambda_responses.clear();
    }

    /// Read the full body referenced by a [`BodyRef`] without touching the
    /// [`BodyCache`] or any `S3State`. Because it borrows nothing from state,
    /// callers can read part bodies after dropping the global S3 lock — used by
    /// CompleteMultipartUpload to assemble a multi-GB object off-lock instead of
    /// serializing every other S3 operation behind the assembly (bug-audit
    /// 2026-05-28, 4.7). Disk bodies are read straight from their path.
    pub fn read_body_uncached(body: &BodyRef) -> io::Result<Bytes> {
        match body {
            BodyRef::Memory(b) => Ok(b.clone()),
            BodyRef::Disk { path, .. } => Ok(Bytes::from(std::fs::read(path)?)),
        }
    }

    /// Read the full body referenced by a [`BodyRef`], consulting the
    /// persistent [`BodyCache`] when one is configured.
    pub fn read_body(&self, body: &BodyRef) -> io::Result<Bytes> {
        match body {
            BodyRef::Memory(b) => Ok(b.clone()),
            BodyRef::Disk {
                bucket,
                key,
                version,
                path,
                ..
            } => {
                let cache_key = BodyKey::new(bucket.clone(), key.clone(), version.clone());
                if let Some(cache) = &self.body_cache {
                    if let Some(hit) = cache.get(&cache_key) {
                        return Ok(hit);
                    }
                }
                let data = std::fs::read(path)?;
                let bytes = Bytes::from(data);
                if let Some(cache) = &self.body_cache {
                    cache.insert(cache_key, bytes.clone());
                }
                Ok(bytes)
            }
        }
    }

    /// Read a byte range from the body without loading the full object into
    /// memory. Memory bodies are sliced directly; disk bodies are seek+read'd.
    /// Ranges bypass the body cache (the cache stores whole objects only).
    pub fn read_body_range(&self, body: &BodyRef, offset: u64, len: u64) -> io::Result<Bytes> {
        match body {
            BodyRef::Memory(b) => {
                let start = offset as usize;
                let end = start.saturating_add(len as usize).min(b.len());
                if start > b.len() {
                    return Ok(Bytes::new());
                }
                Ok(b.slice(start..end))
            }
            BodyRef::Disk { path, .. } => {
                let mut f = std::fs::File::open(path)?;
                f.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; len as usize];
                f.read_exact(&mut buf)?;
                Ok(Bytes::from(buf))
            }
        }
    }
}

impl fakecloud_core::multi_account::AccountState for S3State {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }

    fn inherit_from(&mut self, sibling: &Self) {
        if let Some(cache) = &sibling.body_cache {
            self.body_cache = Some(cache.clone());
        }
    }
}

pub type SharedS3State = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<S3State>>>;

/// Construct a memory-backed [`BodyRef`] from [`Bytes`].
pub fn memory_body(bytes: Bytes) -> BodyRef {
    BodyRef::Memory(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn new_bucket_seeds_full_control_acl() {
        let b = S3Bucket::new("my-bucket", "us-east-1", "owner-id");
        assert_eq!(b.name, "my-bucket");
        assert_eq!(b.region, "us-east-1");
        assert_eq!(b.acl_owner_id, "owner-id");
        assert_eq!(b.acl_grants.len(), 1);
        assert_eq!(b.acl_grants[0].permission, "FULL_CONTROL");
        assert_eq!(b.acl_grants[0].grantee_type, "CanonicalUser");
        assert!(!b.eventbridge_enabled);
        assert!(b.versioning.is_none());
    }

    #[test]
    fn s3state_new_and_reset_clears_buckets() {
        let mut state = S3State::new("123456789012", "us-east-1");
        assert!(state.buckets.is_empty());
        state
            .buckets
            .insert("b".to_string(), S3Bucket::new("b", "us-east-1", "owner"));
        state.notification_events.push(S3NotificationEvent {
            bucket: "b".to_string(),
            key: "k".to_string(),
            event_type: "s3:ObjectCreated:Put".to_string(),
            timestamp: Utc::now(),
        });
        state.reset();
        assert!(state.buckets.is_empty());
        assert!(state.notification_events.is_empty());
    }

    #[test]
    fn read_body_from_memory_returns_bytes() {
        let state = S3State::new("123", "us-east-1");
        let body = memory_body(Bytes::from_static(b"hello"));
        assert_eq!(state.read_body(&body).unwrap(), &b"hello"[..]);
    }

    #[test]
    fn read_body_from_disk_reads_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.as_file().write_all(b"file-body").unwrap();
        let body = BodyRef::Disk {
            bucket: "b".to_string(),
            key: "k".to_string(),
            version: None,
            path: tmp.path().to_path_buf(),
            size: 9,
        };
        let state = S3State::new("123", "us-east-1");
        assert_eq!(state.read_body(&body).unwrap(), &b"file-body"[..]);
    }

    #[test]
    fn read_body_uncached_reads_memory_and_disk() {
        // bug-audit 4.7: the off-lock multipart assembler relies on this
        // state-free reader for both body kinds.
        let mem = memory_body(Bytes::from_static(b"hello"));
        assert_eq!(S3State::read_body_uncached(&mem).unwrap(), &b"hello"[..]);

        let tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.as_file().write_all(b"file-body").unwrap();
        let disk = BodyRef::Disk {
            bucket: "b".to_string(),
            key: "k".to_string(),
            version: None,
            path: tmp.path().to_path_buf(),
            size: 9,
        };
        assert_eq!(
            S3State::read_body_uncached(&disk).unwrap(),
            &b"file-body"[..]
        );
    }

    #[test]
    fn read_body_range_slices_memory() {
        let state = S3State::new("123", "us-east-1");
        let body = memory_body(Bytes::from_static(b"abcdefghij"));
        assert_eq!(state.read_body_range(&body, 2, 4).unwrap(), &b"cdef"[..]);
    }

    #[test]
    fn read_body_range_memory_beyond_length_returns_empty() {
        let state = S3State::new("123", "us-east-1");
        let body = memory_body(Bytes::from_static(b"abc"));
        assert!(state.read_body_range(&body, 100, 4).unwrap().is_empty());
    }

    #[test]
    fn read_body_range_memory_clamps_to_length() {
        let state = S3State::new("123", "us-east-1");
        let body = memory_body(Bytes::from_static(b"abcdef"));
        assert_eq!(state.read_body_range(&body, 4, 100).unwrap(), &b"ef"[..]);
    }

    #[test]
    fn read_body_range_from_disk() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.as_file().write_all(b"0123456789").unwrap();
        let body = BodyRef::Disk {
            bucket: "b".to_string(),
            key: "k".to_string(),
            version: None,
            path: tmp.path().to_path_buf(),
            size: 10,
        };
        let state = S3State::new("123", "us-east-1");
        assert_eq!(state.read_body_range(&body, 3, 4).unwrap(), &b"3456"[..]);
    }

    #[test]
    fn account_state_impl_new_for_account() {
        use fakecloud_core::multi_account::AccountState;
        let s = S3State::new_for_account("111122223333", "eu-west-1", "http://x");
        assert_eq!(s.account_id, "111122223333");
        assert_eq!(s.region, "eu-west-1");
    }
}
