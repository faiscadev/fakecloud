use bytes::Bytes;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

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
    pub version_id: Option<String>,
    pub is_delete_marker: bool,
    pub content_encoding: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub parts: HashMap<i32, UploadPart>,
    pub initiated: DateTime<Utc>,
    pub storage_class: Option<String>,
    pub metadata: HashMap<String, String>,
    pub content_type: String,
}

#[derive(Debug, Clone)]
pub struct UploadPart {
    pub part_number: i32,
    pub data: Bytes,
    pub etag: String,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct S3Bucket {
    pub name: String,
    pub creation_date: DateTime<Utc>,
    pub region: String,
    /// Objects keyed by their full key path.
    pub objects: BTreeMap<String, S3Object>,
    pub tags: HashMap<String, String>,
    /// Versioning status: None = never enabled, Some("Enabled"), Some("Suspended")
    pub versioning_status: Option<String>,
    /// Raw XML configs stored as Option<String>
    pub encryption_config: Option<String>,
    pub lifecycle_config: Option<String>,
    pub policy: Option<String>,
    pub cors_config: Option<String>,
    pub acl: Option<String>,
    pub notification_config: Option<String>,
    pub logging_config: Option<String>,
    pub website_config: Option<String>,
    pub accelerate_status: Option<String>,
    pub public_access_block: Option<String>,
    pub object_lock_config: Option<String>,
    /// Version history: key -> list of versions (newest last)
    pub object_versions: HashMap<String, Vec<S3Object>>,
}

impl S3Bucket {
    pub fn new(name: &str, region: &str) -> Self {
        Self {
            name: name.to_string(),
            creation_date: Utc::now(),
            region: region.to_string(),
            objects: BTreeMap::new(),
            tags: HashMap::new(),
            versioning_status: None,
            encryption_config: None,
            lifecycle_config: None,
            policy: None,
            cors_config: None,
            acl: None,
            notification_config: None,
            logging_config: None,
            website_config: None,
            accelerate_status: None,
            public_access_block: None,
            object_lock_config: None,
            object_versions: HashMap::new(),
        }
    }
}

pub struct S3State {
    pub account_id: String,
    pub region: String,
    pub buckets: HashMap<String, S3Bucket>,
    /// In-progress multipart uploads keyed by upload_id
    pub multipart_uploads: HashMap<String, MultipartUpload>,
}

impl S3State {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            buckets: HashMap::new(),
            multipart_uploads: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.buckets.clear();
        self.multipart_uploads.clear();
    }
}

pub type SharedS3State = Arc<RwLock<S3State>>;
