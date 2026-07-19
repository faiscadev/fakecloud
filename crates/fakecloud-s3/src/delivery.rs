use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use fakecloud_core::delivery::S3Delivery;
use fakecloud_persistence::{BodySource, S3Store};
use md5::{Digest, Md5};
use parking_lot::RwLock;

use crate::state::{memory_body, S3Object, SharedS3State};

/// Lets non-S3 services write objects to buckets without taking a
/// direct dep on this crate. Used by CloudWatch Logs export tasks,
/// CloudWatch Logs deliveries, Firehose S3 destinations, ELB access
/// logs, and similar producers.
pub struct S3DeliveryImpl {
    state: SharedS3State,
    /// Durable S3 backing store. S3 durability is write-through to this store
    /// (there is no `s3_state` snapshot; on boot S3 is rebuilt from the store),
    /// so an object written only into `state` is lost on restart. Behind a lock
    /// so callers built before the S3 store exists during startup can wire it up
    /// later via [`Self::set_s3_store`]. `None` => memory mode (no store).
    store: RwLock<Option<Arc<dyn S3Store>>>,
}

impl S3DeliveryImpl {
    pub fn new(state: SharedS3State) -> Self {
        Self {
            state,
            store: RwLock::new(None),
        }
    }

    /// Wire the durable S3 store after construction, so delivered objects are
    /// written through to disk and survive a restart. Several producers (the
    /// CloudWatch-Logs delivery bus, ELB access logs) build this impl before the
    /// S3 store exists during server startup, so the store is set later.
    pub fn set_s3_store(&self, store: Arc<dyn S3Store>) {
        *self.store.write() = Some(store);
    }
}

impl S3Delivery for S3DeliveryImpl {
    fn put_object(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<(), String> {
        // Snapshot the durable store handle before taking the S3 state lock so
        // the two locks are never held nested.
        let store = self.store.read().clone();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let bucket_ref = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| format!("bucket {bucket} not found in account {account_id}"))?;

        let bytes = Bytes::from(body);
        let size = bytes.len() as u64;
        let etag = format!("{:x}", Md5::digest(&bytes));
        let mut obj = S3Object {
            key: key.to_string(),
            body: memory_body(bytes.clone()),
            content_type: content_type
                .unwrap_or("application/octet-stream")
                .to_string(),
            etag,
            size,
            last_modified: Utc::now(),
            ..Default::default()
        };
        // Write through to the durable store so the delivered object survives a
        // restart (S3 has no state snapshot; it is rebuilt from the store on
        // boot). Mirror the S3 replication path: persist via the store, then
        // swap the in-memory body for the canonical ref it returns.
        if let Some(store) = &store {
            let meta = crate::persistence::object_meta_snapshot(&obj);
            let returned = store
                .put_object(bucket, key, None, BodySource::Bytes(bytes), &meta)
                .map_err(|e| format!("failed to persist object {key} to bucket {bucket}: {e}"))?;
            obj.body = returned;
        }
        bucket_ref.objects.insert(key.to_string(), obj);
        Ok(())
    }

    fn get_object(&self, account_id: &str, bucket: &str, key: &str) -> Result<Vec<u8>, String> {
        let accounts = self.state.read();
        let state = accounts
            .get(account_id)
            .ok_or_else(|| format!("account {account_id} has no S3 state"))?;
        let bucket_ref = state
            .buckets
            .get(bucket)
            .ok_or_else(|| format!("bucket {bucket} not found in account {account_id}"))?;
        let object = bucket_ref
            .objects
            .get(key)
            .ok_or_else(|| format!("key {key} not found in bucket {bucket}"))?;
        let body = state
            .read_body(&object.body)
            .map_err(|e| format!("failed to read body: {e}"))?;
        Ok(body.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{S3Bucket, S3State};
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    const ACCOUNT: &str = "123456789012";

    fn shared_state() -> SharedS3State {
        let mut mas: MultiAccountState<S3State> =
            MultiAccountState::new(ACCOUNT, "us-east-1", "http://localhost");
        let st = mas.get_or_create(ACCOUNT);
        st.buckets.insert(
            "logs-bucket".to_string(),
            S3Bucket::new("logs-bucket", "us-east-1", "owner-id"),
        );
        Arc::new(RwLock::new(mas))
    }

    #[test]
    fn put_object_writes_to_bucket() {
        let state = shared_state();
        let delivery = S3DeliveryImpl::new(state.clone());
        delivery
            .put_object(
                ACCOUNT,
                "logs-bucket",
                "exports/run-1.txt",
                b"hello world".to_vec(),
                Some("text/plain"),
            )
            .expect("put should succeed");
        let accounts = state.read();
        let st = accounts.get(ACCOUNT).unwrap();
        let bucket = st.buckets.get("logs-bucket").unwrap();
        let obj = bucket.objects.get("exports/run-1.txt").unwrap();
        assert_eq!(obj.size, 11);
        assert_eq!(obj.content_type, "text/plain");
        assert!(!obj.etag.is_empty());
    }

    #[test]
    fn put_object_rejects_missing_bucket() {
        let state = shared_state();
        let delivery = S3DeliveryImpl::new(state);
        let err = delivery
            .put_object(ACCOUNT, "ghost", "k", b"x".to_vec(), None)
            .expect_err("missing bucket");
        assert!(err.contains("not found"));
    }

    /// Regression (bug-hunt Tier 0 side-channel-persistence): a delivered object
    /// must be written through the durable S3 store, else it is lost on restart
    /// (S3 has no state snapshot; it is rebuilt from the store on boot). With a
    /// store wired, the in-memory body becomes the store's disk-backed ref and
    /// the object reappears after a store reload.
    #[test]
    fn put_object_writes_through_to_durable_store() {
        use fakecloud_persistence::s3::DiskS3Store;
        use fakecloud_persistence::{BodyRef, BucketMeta, S3Store};

        let tmp = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(fakecloud_persistence::cache::BodyCache::new(1024 * 1024));
        let disk: Arc<dyn S3Store> = Arc::new(DiskS3Store::new(tmp.path().join("s3"), cache));
        disk.put_bucket_meta(
            "logs-bucket",
            &BucketMeta {
                name: "logs-bucket".to_string(),
                region: "us-east-1".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        let state = shared_state();
        let delivery = S3DeliveryImpl::new(state.clone());
        delivery.set_s3_store(disk.clone());
        delivery
            .put_object(
                ACCOUNT,
                "logs-bucket",
                "exports/run-1.txt",
                b"durable-bytes".to_vec(),
                Some("text/plain"),
            )
            .expect("put");

        // In-memory body is now the store's disk-backed ref, and the file exists.
        {
            let accounts = state.read();
            let obj = accounts
                .get(ACCOUNT)
                .unwrap()
                .buckets
                .get("logs-bucket")
                .unwrap()
                .objects
                .get("exports/run-1.txt")
                .unwrap();
            match &obj.body {
                BodyRef::Disk { path, .. } => assert!(path.exists()),
                BodyRef::Memory(_) => panic!("delivery was not written through the store"),
            }
        }

        // Reloading the store from disk (as boot does) surfaces the object.
        let reloaded = disk.load().unwrap();
        assert!(reloaded
            .buckets
            .get("logs-bucket")
            .expect("bucket survives reload")
            .objects
            .contains_key("exports/run-1.txt"));
    }

    #[test]
    fn get_object_round_trips_put_object() {
        let state = shared_state();
        let delivery = S3DeliveryImpl::new(state);
        delivery
            .put_object(
                ACCOUNT,
                "logs-bucket",
                "dump.sql",
                b"-- pg dump --".to_vec(),
                Some("application/sql"),
            )
            .expect("put");
        let body = delivery
            .get_object(ACCOUNT, "logs-bucket", "dump.sql")
            .expect("get");
        assert_eq!(body, b"-- pg dump --".to_vec());
    }

    #[test]
    fn get_object_rejects_missing_key() {
        let state = shared_state();
        let delivery = S3DeliveryImpl::new(state);
        let err = delivery
            .get_object(ACCOUNT, "logs-bucket", "ghost")
            .expect_err("missing key");
        assert!(err.contains("not found"));
    }
}
