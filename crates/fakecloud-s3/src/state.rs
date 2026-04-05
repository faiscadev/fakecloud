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
}

#[derive(Debug, Clone)]
pub struct S3Bucket {
    pub name: String,
    pub creation_date: DateTime<Utc>,
    pub region: String,
    /// Objects keyed by their full key path.
    pub objects: BTreeMap<String, S3Object>,
    pub tags: HashMap<String, String>,
}

impl S3Bucket {
    pub fn new(name: &str, region: &str) -> Self {
        Self {
            name: name.to_string(),
            creation_date: Utc::now(),
            region: region.to_string(),
            objects: BTreeMap::new(),
            tags: HashMap::new(),
        }
    }
}

pub struct S3State {
    pub account_id: String,
    pub region: String,
    pub buckets: HashMap<String, S3Bucket>,
}

impl S3State {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            buckets: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.buckets.clear();
    }
}

pub type SharedS3State = Arc<RwLock<S3State>>;
