use std::collections::BTreeMap;

use fakecloud_persistence::{
    AclGrantSnapshot, AclSnapshot, BucketMeta, BucketSnapshot, InventorySnapshot, LoadedMpu,
    LoadedObject, LoadedPart, MpuInit, ObjectMeta, S3StateSnapshot, TagsSnapshot, UploadPartMeta,
};

use crate::state::{AclGrant, MultipartUpload, S3Bucket, S3Object, S3State, UploadPart};

impl From<&AclGrant> for AclGrantSnapshot {
    fn from(g: &AclGrant) -> Self {
        Self {
            grantee_type: g.grantee_type.clone(),
            grantee_id: g.grantee_id.clone(),
            grantee_display_name: g.grantee_display_name.clone(),
            grantee_uri: g.grantee_uri.clone(),
            permission: g.permission.clone(),
        }
    }
}

pub fn bucket_meta_snapshot(b: &S3Bucket) -> BucketMeta {
    BucketMeta {
        name: b.name.clone(),
        creation_date: b.creation_date,
        region: b.region.clone(),
        versioning: b.versioning.clone(),
        mfa_delete: b.mfa_delete.clone(),
        acl: b.acl.clone(),
        acl_owner_id: b.acl_owner_id.clone(),
        accelerate_status: b.accelerate_status.clone(),
        eventbridge_enabled: b.eventbridge_enabled,
        lifecycle_transition_default_min_size: b.lifecycle_transition_default_min_size.clone(),
    }
}

pub fn object_meta_snapshot(o: &S3Object) -> ObjectMeta {
    ObjectMeta {
        key: o.key.clone(),
        content_type: o.content_type.clone(),
        etag: o.etag.clone(),
        size: o.size,
        last_modified: o.last_modified,
        metadata: o.metadata.clone(),
        tags: o.tags.clone(),
        storage_class: o.storage_class.clone(),
        acl_grants: o.acl_grants.iter().map(AclGrantSnapshot::from).collect(),
        acl_owner_id: o.acl_owner_id.clone(),
        parts_count: o.parts_count,
        part_sizes: o.part_sizes.clone(),
        sse_algorithm: o.sse_algorithm.clone(),
        sse_kms_key_id: o.sse_kms_key_id.clone(),
        bucket_key_enabled: o.bucket_key_enabled,
        version_id: o.version_id.clone(),
        is_delete_marker: o.is_delete_marker,
        restore_ongoing: o.restore_ongoing,
        restore_expiry: o.restore_expiry.clone(),
        checksum_algorithm: o.checksum_algorithm.clone(),
        checksum_value: o.checksum_value.clone(),
        lock_mode: o.lock_mode.clone(),
        lock_retain_until: o.lock_retain_until,
        lock_legal_hold: o.lock_legal_hold.clone(),
        content_encoding: o.content_encoding.clone(),
        cache_control: o.cache_control.clone(),
        content_disposition: o.content_disposition.clone(),
        content_language: o.content_language.clone(),
        expires: o.expires.clone(),
        website_redirect_location: o.website_redirect_location.clone(),
    }
}

pub fn mpu_init_snapshot(m: &MultipartUpload) -> MpuInit {
    MpuInit {
        upload_id: m.upload_id.clone(),
        key: m.key.clone(),
        initiated: m.initiated,
        metadata: m.metadata.clone(),
        content_type: m.content_type.clone(),
        storage_class: m.storage_class.clone(),
        sse_algorithm: m.sse_algorithm.clone(),
        sse_kms_key_id: m.sse_kms_key_id.clone(),
        tagging: m.tagging.clone(),
        acl_grants: m.acl_grants.iter().map(AclGrantSnapshot::from).collect(),
        checksum_algorithm: m.checksum_algorithm.clone(),
        cache_control: m.cache_control.clone(),
        content_disposition: m.content_disposition.clone(),
        content_language: m.content_language.clone(),
        expires: m.expires.clone(),
    }
}

pub fn upload_part_meta_snapshot(p: &UploadPart) -> UploadPartMeta {
    UploadPartMeta {
        part_number: p.part_number,
        etag: p.etag.clone(),
        size: p.size,
        last_modified: p.last_modified,
    }
}

fn acl_grant_from_snapshot(g: &AclGrantSnapshot) -> AclGrant {
    AclGrant {
        grantee_type: g.grantee_type.clone(),
        grantee_id: g.grantee_id.clone(),
        grantee_display_name: g.grantee_display_name.clone(),
        grantee_uri: g.grantee_uri.clone(),
        permission: g.permission.clone(),
    }
}

pub fn s3_object_from_loaded(lo: LoadedObject) -> S3Object {
    let LoadedObject { meta, body } = lo;
    S3Object {
        key: meta.key,
        body,
        content_type: meta.content_type,
        etag: meta.etag,
        size: meta.size,
        last_modified: meta.last_modified,
        metadata: meta.metadata,
        storage_class: meta.storage_class,
        tags: meta.tags,
        acl_grants: meta
            .acl_grants
            .iter()
            .map(acl_grant_from_snapshot)
            .collect(),
        acl_owner_id: meta.acl_owner_id,
        parts_count: meta.parts_count,
        part_sizes: meta.part_sizes,
        sse_algorithm: meta.sse_algorithm,
        sse_kms_key_id: meta.sse_kms_key_id,
        bucket_key_enabled: meta.bucket_key_enabled,
        version_id: meta.version_id,
        is_delete_marker: meta.is_delete_marker,
        content_encoding: meta.content_encoding,
        cache_control: meta.cache_control,
        content_disposition: meta.content_disposition,
        content_language: meta.content_language,
        expires: meta.expires,
        website_redirect_location: meta.website_redirect_location,
        restore_ongoing: meta.restore_ongoing,
        restore_expiry: meta.restore_expiry,
        checksum_algorithm: meta.checksum_algorithm,
        checksum_value: meta.checksum_value,
        lock_mode: meta.lock_mode,
        lock_retain_until: meta.lock_retain_until,
        lock_legal_hold: meta.lock_legal_hold,
    }
}

pub fn upload_part_from_loaded(lp: LoadedPart) -> UploadPart {
    let LoadedPart { meta, body } = lp;
    UploadPart {
        part_number: meta.part_number,
        body,
        etag: meta.etag,
        size: meta.size,
        last_modified: meta.last_modified,
    }
}

pub fn multipart_upload_from_loaded(lm: LoadedMpu) -> MultipartUpload {
    let LoadedMpu { init, parts } = lm;
    let runtime_parts: BTreeMap<u32, UploadPart> = parts
        .into_iter()
        .map(|(n, lp)| (n, upload_part_from_loaded(lp)))
        .collect();
    MultipartUpload {
        upload_id: init.upload_id,
        key: init.key,
        initiated: init.initiated,
        parts: runtime_parts,
        metadata: init.metadata,
        content_type: init.content_type,
        storage_class: init.storage_class,
        sse_algorithm: init.sse_algorithm,
        sse_kms_key_id: init.sse_kms_key_id,
        tagging: init.tagging,
        acl_grants: init
            .acl_grants
            .iter()
            .map(acl_grant_from_snapshot)
            .collect(),
        checksum_algorithm: init.checksum_algorithm,
        cache_control: init.cache_control,
        content_disposition: init.content_disposition,
        content_language: init.content_language,
        expires: init.expires,
    }
}

pub fn s3_bucket_from_snapshot(
    name: &str,
    snap: BucketSnapshot,
    default_region: &str,
) -> Result<S3Bucket, String> {
    let BucketSnapshot {
        meta,
        objects,
        object_versions,
        subresources,
        multipart_uploads,
    } = snap;
    let region = if meta.region.is_empty() {
        default_region.to_string()
    } else {
        meta.region.clone()
    };
    // Default owner grant, mirroring `S3Bucket::new`. Used when no `acl.toml`
    // sidecar was persisted (bucket created without a custom ACL).
    let owner_id = meta.acl_owner_id.clone();
    let default_owner_grant = AclGrant {
        grantee_type: "CanonicalUser".to_string(),
        grantee_id: Some(owner_id.clone()),
        grantee_display_name: Some(owner_id.clone()),
        grantee_uri: None,
        permission: "FULL_CONTROL".to_string(),
    };
    let has_acl_sidecar = subresources.contains_key("acl.toml");
    let mut b = S3Bucket {
        name: name.to_string(),
        creation_date: meta.creation_date,
        region,
        objects: BTreeMap::new(),
        tags: BTreeMap::new(),
        acl_grants: if has_acl_sidecar {
            Vec::new()
        } else {
            vec![default_owner_grant]
        },
        acl_owner_id: meta.acl_owner_id.clone(),
        multipart_uploads: BTreeMap::new(),
        versioning: meta.versioning.clone(),
        mfa_delete: meta.mfa_delete.clone(),
        object_versions: BTreeMap::new(),
        acl: meta.acl.clone(),
        encryption_config: None,
        lifecycle_config: None,
        lifecycle_transition_default_min_size: meta.lifecycle_transition_default_min_size.clone(),
        policy: None,
        cors_config: None,
        notification_config: None,
        logging_config: None,
        website_config: None,
        accelerate_status: meta.accelerate_status.clone(),
        public_access_block: None,
        object_lock_config: None,
        replication_config: None,
        ownership_controls: None,
        inventory_configs: BTreeMap::new(),
        eventbridge_enabled: meta.eventbridge_enabled,
        analytics_configs: BTreeMap::new(),
        intelligent_tiering_configs: BTreeMap::new(),
        metrics_configs: BTreeMap::new(),
        request_payment: None,
        abac_config: None,
        metadata_configuration: None,
        metadata_table_configuration: None,
    };
    for (key, lo) in objects {
        b.objects.insert(key, s3_object_from_loaded(lo));
    }
    for (key, vs) in object_versions {
        b.object_versions
            .insert(key, vs.into_iter().map(s3_object_from_loaded).collect());
    }
    for (upload_id, lm) in multipart_uploads {
        b.multipart_uploads
            .insert(upload_id, multipart_upload_from_loaded(lm));
    }
    for (fname, text) in subresources {
        match fname.as_str() {
            "lifecycle.toml" => b.lifecycle_config = Some(text),
            "cors.toml" => b.cors_config = Some(text),
            "policy.toml" => b.policy = Some(text),
            "notification.toml" => b.notification_config = Some(text),
            "logging.toml" => b.logging_config = Some(text),
            "website.toml" => b.website_config = Some(text),
            "public_access_block.toml" => b.public_access_block = Some(text),
            "object_lock.toml" => b.object_lock_config = Some(text),
            "replication.toml" => b.replication_config = Some(text),
            "ownership.toml" => b.ownership_controls = Some(text),
            "encryption.toml" => b.encryption_config = Some(text),
            "tags.toml" => {
                if text.trim().is_empty() {
                    continue;
                }
                let snap: TagsSnapshot = toml::from_str(&text)
                    .map_err(|e| format!("failed to parse tags.toml for bucket {name}: {e}"))?;
                b.tags = snap.tags;
            }
            "acl.toml" => {
                if text.trim().is_empty() {
                    continue;
                }
                let snap: AclSnapshot = toml::from_str(&text)
                    .map_err(|e| format!("failed to parse acl.toml for bucket {name}: {e}"))?;
                if !snap.owner_id.is_empty() {
                    b.acl_owner_id = snap.owner_id;
                }
                b.acl_grants = snap.grants.iter().map(acl_grant_from_snapshot).collect();
            }
            "inventory.toml" => {
                if text.trim().is_empty() {
                    continue;
                }
                let snap: InventorySnapshot = toml::from_str(&text).map_err(|e| {
                    format!("failed to parse inventory.toml for bucket {name}: {e}")
                })?;
                b.inventory_configs = snap.configs;
            }
            "analytics.toml" => {
                if text.trim().is_empty() {
                    continue;
                }
                b.analytics_configs = toml::from_str(&text).unwrap_or_default();
            }
            "intelligent_tiering.toml" => {
                if text.trim().is_empty() {
                    continue;
                }
                b.intelligent_tiering_configs = toml::from_str(&text).unwrap_or_default();
            }
            "metrics.toml" => {
                if text.trim().is_empty() {
                    continue;
                }
                b.metrics_configs = toml::from_str(&text).unwrap_or_default();
            }
            "request_payment.toml" => {
                b.request_payment = Some(text);
            }
            "abac.toml" => {
                b.abac_config = Some(text);
            }
            "metadata_configuration.toml" => {
                b.metadata_configuration = Some(text);
            }
            "metadata_table_configuration.toml" => {
                b.metadata_table_configuration = Some(text);
            }
            _ => {}
        }
    }
    Ok(b)
}

pub fn hydrate_s3_state(
    snapshot: S3StateSnapshot,
    account_id: &str,
    region: &str,
) -> Result<S3State, String> {
    let mut state = S3State::new(account_id, region);
    for (name, snap) in snapshot.buckets {
        let bucket = s3_bucket_from_snapshot(&name, snap, region)?;
        state.buckets.insert(name, bucket);
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hydrate_bucket_without_acl_sidecar_gets_default_owner_grant() {
        let snap = BucketSnapshot {
            meta: BucketMeta {
                name: "b".into(),
                acl_owner_id: "owner-xyz".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let b = s3_bucket_from_snapshot("b", snap, "us-east-1").unwrap();
        assert_eq!(b.acl_grants.len(), 1);
        let g = &b.acl_grants[0];
        assert_eq!(g.grantee_type, "CanonicalUser");
        assert_eq!(g.grantee_id.as_deref(), Some("owner-xyz"));
        assert_eq!(g.permission, "FULL_CONTROL");
    }

    #[test]
    fn hydrate_bucket_with_acl_sidecar_uses_sidecar() {
        let acl_toml = r#"
owner_id = "owner-xyz"
[[grants]]
grantee_type = "CanonicalUser"
grantee_id = "grantee-a"
permission = "READ"
"#;
        let mut subresources = BTreeMap::new();
        subresources.insert("acl.toml".to_string(), acl_toml.to_string());
        let snap = BucketSnapshot {
            meta: BucketMeta {
                name: "b".into(),
                acl_owner_id: "owner-xyz".into(),
                ..Default::default()
            },
            subresources,
            ..Default::default()
        };
        let b = s3_bucket_from_snapshot("b", snap, "us-east-1").unwrap();
        assert_eq!(b.acl_grants.len(), 1);
        assert_eq!(b.acl_grants[0].grantee_id.as_deref(), Some("grantee-a"));
        assert_eq!(b.acl_grants[0].permission, "READ");
    }

    #[test]
    fn acl_grant_snapshot_roundtrip() {
        let grant = AclGrant {
            grantee_type: "CanonicalUser".to_string(),
            grantee_id: Some("g1".to_string()),
            grantee_display_name: Some("Alice".to_string()),
            grantee_uri: None,
            permission: "READ".to_string(),
        };
        let snap: AclGrantSnapshot = (&grant).into();
        let round = acl_grant_from_snapshot(&snap);
        assert_eq!(round.grantee_type, "CanonicalUser");
        assert_eq!(round.grantee_id.as_deref(), Some("g1"));
        assert_eq!(round.grantee_display_name.as_deref(), Some("Alice"));
        assert_eq!(round.permission, "READ");
    }

    #[test]
    fn bucket_meta_snapshot_copies_fields() {
        let b = S3Bucket::new("my-bucket", "eu-west-1", "owner");
        let meta = bucket_meta_snapshot(&b);
        assert_eq!(meta.name, "my-bucket");
        assert_eq!(meta.region, "eu-west-1");
        assert_eq!(meta.acl_owner_id, "owner");
        assert!(!meta.eventbridge_enabled);
    }

    #[test]
    fn object_meta_snapshot_preserves_object_fields() {
        let obj = S3Object {
            key: "k".to_string(),
            content_type: "text/plain".to_string(),
            etag: "etag".to_string(),
            size: 42,
            storage_class: "STANDARD".to_string(),
            version_id: Some("v1".to_string()),
            is_delete_marker: false,
            ..Default::default()
        };
        let meta = object_meta_snapshot(&obj);
        assert_eq!(meta.key, "k");
        assert_eq!(meta.content_type, "text/plain");
        assert_eq!(meta.size, 42);
        assert_eq!(meta.version_id.as_deref(), Some("v1"));
    }

    #[test]
    fn mpu_init_snapshot_captures_metadata() {
        use std::collections::BTreeMap;
        let mpu = MultipartUpload {
            upload_id: "up-1".to_string(),
            key: "k".to_string(),
            initiated: chrono::Utc::now(),
            parts: BTreeMap::new(),
            metadata: BTreeMap::new(),
            content_type: "application/octet-stream".to_string(),
            storage_class: "STANDARD".to_string(),
            sse_algorithm: None,
            sse_kms_key_id: None,
            tagging: None,
            acl_grants: Vec::new(),
            checksum_algorithm: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
        };
        let snap = mpu_init_snapshot(&mpu);
        assert_eq!(snap.upload_id, "up-1");
        assert_eq!(snap.content_type, "application/octet-stream");
    }

    #[test]
    fn upload_part_meta_snapshot_copies_identifiers() {
        let part = UploadPart {
            part_number: 3,
            body: crate::state::memory_body(bytes::Bytes::from_static(b"abc")),
            etag: "peeetag".to_string(),
            size: 3,
            last_modified: chrono::Utc::now(),
        };
        let snap = upload_part_meta_snapshot(&part);
        assert_eq!(snap.part_number, 3);
        assert_eq!(snap.etag, "peeetag");
        assert_eq!(snap.size, 3);
    }

    #[test]
    fn hydrate_s3_state_populates_buckets() {
        let mut snapshot = S3StateSnapshot::default();
        snapshot.buckets.insert(
            "b".to_string(),
            BucketSnapshot {
                meta: BucketMeta {
                    name: "b".into(),
                    acl_owner_id: "owner".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let state = hydrate_s3_state(snapshot, "123", "us-east-1").unwrap();
        assert!(state.buckets.contains_key("b"));
    }
}
