use bytes::Bytes;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
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

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub data: Bytes,
    pub content_type: String,
    pub etag: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
    pub storage_class: String,
    pub tags: HashMap<String, String>,
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
    pub data: Bytes,
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
    pub metadata: HashMap<String, String>,
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
    pub tags: HashMap<String, String>,
    pub acl_grants: Vec<AclGrant>,
    pub acl_owner_id: String,
    /// In-progress multipart uploads keyed by upload ID.
    pub multipart_uploads: HashMap<String, MultipartUpload>,
    /// Versioning status: None = never enabled, Some("Enabled"), Some("Suspended").
    pub versioning: Option<String>,
    /// Object versions keyed by key, each value is a list of versions.
    pub object_versions: HashMap<String, Vec<S3Object>>,
    /// Bucket ACL (canned or XML).
    pub acl: Option<String>,
    pub encryption_config: Option<String>,
    pub lifecycle_config: Option<String>,
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
    pub inventory_configs: HashMap<String, String>,
    /// Whether EventBridge notifications are enabled for this bucket.
    pub eventbridge_enabled: bool,
}

impl S3Bucket {
    pub fn new(name: &str, region: &str, owner_id: &str) -> Self {
        Self {
            name: name.to_string(),
            creation_date: Utc::now(),
            region: region.to_string(),
            objects: BTreeMap::new(),
            tags: HashMap::new(),
            acl_grants: vec![AclGrant {
                grantee_type: "CanonicalUser".to_string(),
                grantee_id: Some(owner_id.to_string()),
                grantee_display_name: Some(owner_id.to_string()),
                grantee_uri: None,
                permission: "FULL_CONTROL".to_string(),
            }],
            acl_owner_id: owner_id.to_string(),
            multipart_uploads: HashMap::new(),
            versioning: None,
            object_versions: HashMap::new(),
            acl: None,
            encryption_config: None,
            lifecycle_config: None,
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
            inventory_configs: HashMap::new(),
            eventbridge_enabled: false,
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

pub struct S3State {
    pub account_id: String,
    pub region: String,
    pub buckets: HashMap<String, S3Bucket>,
    pub notification_events: Vec<S3NotificationEvent>,
}

impl S3State {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            buckets: HashMap::new(),
            notification_events: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.buckets.clear();
        self.notification_events.clear();
    }
}

pub type SharedS3State = Arc<RwLock<S3State>>;
