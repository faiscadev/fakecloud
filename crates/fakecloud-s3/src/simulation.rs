use crate::lifecycle::LifecycleProcessor;
use crate::state::SharedS3State;

/// Result of a lifecycle processor tick.
pub struct LifecycleTickResult {
    pub processed_buckets: u64,
    pub expired_objects: u64,
    pub transitioned_objects: u64,
}

/// Snapshot of a bucket's objects before processing.
struct BucketSnapshot {
    name: String,
    object_count: usize,
    storage_classes: Vec<(String, String)>,
}

/// Run one tick of the S3 lifecycle processor and return statistics.
pub fn tick_lifecycle(state: &SharedS3State) -> LifecycleTickResult {
    // Snapshot object counts and storage classes before processing
    let (buckets_with_lifecycle, before_snapshot) = {
        let s = state.read();
        let mut count = 0u64;
        let mut snapshot: Vec<BucketSnapshot> = Vec::new();
        for bucket in s.buckets.values() {
            let classes: Vec<(String, String)> = bucket
                .objects
                .iter()
                .map(|(k, o)| (k.clone(), o.storage_class.clone()))
                .collect();
            snapshot.push(BucketSnapshot {
                name: bucket.name.clone(),
                object_count: bucket.objects.len(),
                storage_classes: classes,
            });
            if bucket.lifecycle_config.is_some() {
                count += 1;
            }
        }
        (count, snapshot)
    };

    // Run the processor tick
    let processor = LifecycleProcessor::new(state.clone());
    processor.tick();

    // Compute diffs
    let mut expired_objects = 0u64;
    let mut transitioned_objects = 0u64;

    let s = state.read();
    for snap in &before_snapshot {
        let bucket = match s.buckets.get(&snap.name) {
            Some(b) => b,
            None => continue,
        };

        // Count expired (deleted) objects
        let after_count = bucket.objects.len();
        if snap.object_count > after_count {
            expired_objects += (snap.object_count - after_count) as u64;
        }

        // Count transitioned objects (storage class changed)
        for (key, old_class) in &snap.storage_classes {
            if let Some(obj) = bucket.objects.get(key) {
                if &obj.storage_class != old_class {
                    transitioned_objects += 1;
                }
            }
        }
    }

    LifecycleTickResult {
        processed_buckets: buckets_with_lifecycle,
        expired_objects,
        transitioned_objects,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{S3Bucket, S3Object, S3State};
    use bytes::Bytes;
    use chrono::{Duration, Utc};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> SharedS3State {
        Arc::new(RwLock::new(S3State::new("123456789012", "us-east-1")))
    }

    fn make_object(key: &str, age_days: i64) -> S3Object {
        S3Object {
            key: key.to_string(),
            data: Bytes::from("test"),
            content_type: "application/octet-stream".to_string(),
            etag: "\"abc\"".to_string(),
            size: 4,
            last_modified: Utc::now() - Duration::days(age_days),
            metadata: HashMap::new(),
            storage_class: "STANDARD".to_string(),
            tags: HashMap::new(),
            acl_grants: Vec::new(),
            acl_owner_id: None,
            parts_count: None,
            part_sizes: None,
            sse_algorithm: None,
            sse_kms_key_id: None,
            bucket_key_enabled: None,
            version_id: None,
            is_delete_marker: false,
            content_encoding: None,
            website_redirect_location: None,
            restore_ongoing: None,
            restore_expiry: None,
            checksum_algorithm: None,
            checksum_value: None,
            lock_mode: None,
            lock_retain_until: None,
            lock_legal_hold: None,
        }
    }

    #[test]
    fn tick_lifecycle_expires_objects() {
        let state = make_state();

        {
            let mut s = state.write();
            let mut bucket = S3Bucket::new("test-bucket", "us-east-1", "123456789012");
            bucket.lifecycle_config = Some(
                r#"<LifecycleConfiguration>
                    <Rule>
                        <Filter><Prefix></Prefix></Filter>
                        <Status>Enabled</Status>
                        <Expiration><Days>1</Days></Expiration>
                    </Rule>
                </LifecycleConfiguration>"#
                    .to_string(),
            );
            bucket
                .objects
                .insert("old-file.txt".to_string(), make_object("old-file.txt", 5));
            bucket
                .objects
                .insert("new-file.txt".to_string(), make_object("new-file.txt", 0));
            s.buckets.insert("test-bucket".to_string(), bucket);
        }

        let result = tick_lifecycle(&state);
        assert_eq!(result.processed_buckets, 1);
        assert_eq!(result.expired_objects, 1);
        assert_eq!(result.transitioned_objects, 0);

        let s = state.read();
        let bucket = s.buckets.get("test-bucket").unwrap();
        assert_eq!(bucket.objects.len(), 1);
        assert!(bucket.objects.contains_key("new-file.txt"));
    }

    #[test]
    fn tick_lifecycle_transitions_objects() {
        let state = make_state();

        {
            let mut s = state.write();
            let mut bucket = S3Bucket::new("trans-bucket", "us-east-1", "123456789012");
            bucket.lifecycle_config = Some(
                r#"<LifecycleConfiguration>
                    <Rule>
                        <Filter><Prefix></Prefix></Filter>
                        <Status>Enabled</Status>
                        <Transition>
                            <Days>1</Days>
                            <StorageClass>GLACIER</StorageClass>
                        </Transition>
                    </Rule>
                </LifecycleConfiguration>"#
                    .to_string(),
            );
            bucket
                .objects
                .insert("old-file.txt".to_string(), make_object("old-file.txt", 5));
            s.buckets.insert("trans-bucket".to_string(), bucket);
        }

        let result = tick_lifecycle(&state);
        assert_eq!(result.processed_buckets, 1);
        assert_eq!(result.expired_objects, 0);
        assert_eq!(result.transitioned_objects, 1);

        let s = state.read();
        let obj = s.buckets["trans-bucket"]
            .objects
            .get("old-file.txt")
            .unwrap();
        assert_eq!(obj.storage_class, "GLACIER");
    }

    #[test]
    fn tick_lifecycle_no_config_returns_zero() {
        let state = make_state();

        {
            let mut s = state.write();
            let bucket = S3Bucket::new("empty-bucket", "us-east-1", "123456789012");
            s.buckets.insert("empty-bucket".to_string(), bucket);
        }

        let result = tick_lifecycle(&state);
        assert_eq!(result.processed_buckets, 0);
        assert_eq!(result.expired_objects, 0);
        assert_eq!(result.transitioned_objects, 0);
    }
}
