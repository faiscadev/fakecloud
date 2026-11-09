use std::collections::BTreeMap;

use fakecloud_persistence::{
    AclGrantSnapshot, AclSnapshot, AnnotationRecord, BucketMeta, BucketSnapshot, InventorySnapshot,
    LoadedMpu, LoadedObject, LoadedPart, MpuInit, ObjectMeta, S3StateSnapshot, TagsSnapshot,
    UploadPartMeta,
};

use crate::checksum::{ChecksumAlgorithm, ChecksumType};
use crate::state::{
    AclGrant, MultipartUpload, ObjectAnnotation, S3Bucket, S3Object, S3State, UploadPart,
};

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
        legacy_eu_location: b.legacy_eu_location,
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
        checksum_algorithm: o.checksum_algorithm.map(|a| a.as_str().to_string()),
        checksum_value: o.checksum_value.clone(),
        checksum_type: o.checksum_type.map(|t| t.as_str().to_string()),
        part_checksums: o.part_checksums.clone(),
        lock_mode: o.lock_mode.clone(),
        lock_retain_until: o.lock_retain_until,
        lock_legal_hold: o.lock_legal_hold.clone(),
        // Annotations are persisted one record each (`S3Store::put_annotation`),
        // never inside the object's metadata.
        annotations: Vec::new(),
        content_encoding: o.content_encoding.clone(),
        cache_control: o.cache_control.clone(),
        content_disposition: o.content_disposition.clone(),
        content_language: o.content_language.clone(),
        expires: o.expires.clone(),
        website_redirect_location: o.website_redirect_location.clone(),
        replication_status: o.replication_status.clone(),
    }
}

/// The persisted record of one annotation (its payload is stored beside it).
pub fn annotation_record(a: &ObjectAnnotation) -> AnnotationRecord {
    AnnotationRecord {
        name: a.name.clone(),
        etag: a.etag.clone(),
        last_modified: a.last_modified,
        checksum_algorithm: a.checksum_algorithm.map(|c| c.as_str().to_string()),
        checksum_value: a.checksum_value.clone(),
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
        bucket_key_enabled: m.bucket_key_enabled,
        tagging: m.tagging.clone(),
        acl_grants: m.acl_grants.iter().map(AclGrantSnapshot::from).collect(),
        checksum_algorithm: m.checksum_algorithm.map(|a| a.as_str().to_string()),
        checksum_type: m.checksum_type.map(|t| t.as_str().to_string()),
        cache_control: m.cache_control.clone(),
        content_disposition: m.content_disposition.clone(),
        content_language: m.content_language.clone(),
        expires: m.expires.clone(),
        lock_mode: m.lock.mode.clone(),
        lock_retain_until: m.lock.retain_until,
        lock_legal_hold: m.lock.legal_hold.clone(),
    }
}

pub fn upload_part_meta_snapshot(p: &UploadPart) -> UploadPartMeta {
    UploadPartMeta {
        part_number: p.part_number,
        etag: p.etag.clone(),
        size: p.size,
        last_modified: p.last_modified,
        checksum_algorithm: p.checksum.as_ref().map(|(a, _)| a.as_str().to_string()),
        checksum_value: p.checksum.as_ref().map(|(_, v)| v.clone()),
    }
}

/// Read a persisted checksum back. The algorithm name is matched
/// case-insensitively, since older builds stored the client's spelling
/// verbatim (`crc32`, `Sha256`). A name outside the enum, or an empty value
/// (the old stub output for algorithms it could not compute), yields no
/// checksum rather than one that can never verify.
fn loaded_checksum(
    algorithm: Option<&str>,
    value: Option<String>,
) -> Option<(ChecksumAlgorithm, String)> {
    let algorithm = ChecksumAlgorithm::parse(algorithm?)?;
    let value = value.filter(|v| !v.trim().is_empty())?;
    Some((algorithm, value))
}

/// Whether a stored grant names someone it could ever match.
///
/// An older build read an email grantee's absent `<ID>` as an empty string, so
/// a legacy `acl.toml` can hold a grant that matches nobody. Dropping those at
/// load keeps memory, disk and `GetBucketAcl` telling the same story -- hiding
/// them only when rendering would make the response disagree with the ACL the
/// bucket actually has, and a read-modify-write would then persist the omission.
fn acl_grant_names_a_grantee(g: &AclGrantSnapshot) -> bool {
    // Trimmed, like the request-side checks: a stored grantee of whitespace
    // names nobody just as surely as an empty one.
    let named = |v: &Option<String>| v.as_deref().is_some_and(|s| !s.trim().is_empty());
    match g.grantee_type.as_str() {
        "Group" => named(&g.grantee_uri),
        "AmazonCustomerByEmail" => named(&g.grantee_display_name),
        _ => named(&g.grantee_id),
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
    let object_checksum = loaded_checksum(
        meta.checksum_algorithm.as_deref(),
        meta.checksum_value.clone(),
    );
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
        replication_status: meta.replication_status,
        expires: meta.expires,
        website_redirect_location: meta.website_redirect_location,
        restore_ongoing: meta.restore_ongoing,
        restore_expiry: meta.restore_expiry,
        checksum_algorithm: object_checksum.as_ref().map(|(a, _)| *a),
        checksum_value: object_checksum.map(|(_, v)| v),
        checksum_type: meta.checksum_type.as_deref().and_then(ChecksumType::parse),
        part_checksums: meta.part_checksums,
        lock_mode: meta.lock_mode,
        lock_retain_until: meta.lock_retain_until,
        lock_legal_hold: meta.lock_legal_hold,
        annotations: meta
            .annotations
            .into_iter()
            .map(|a| {
                // An annotation written before its checksum value was stored
                // keeps its (normalized) declared algorithm; the value is
                // recomputed from the payload, which is held verbatim.
                let checksum_algorithm = a
                    .checksum_algorithm
                    .as_deref()
                    .and_then(ChecksumAlgorithm::parse);
                let checksum_value = checksum_algorithm.map(|algo| {
                    a.checksum_value
                        .clone()
                        .filter(|v| !v.trim().is_empty())
                        .unwrap_or_else(|| crate::checksum::checksum(algo, &a.payload))
                });
                (
                    a.name.clone(),
                    ObjectAnnotation {
                        name: a.name,
                        payload: a.payload,
                        etag: a.etag,
                        last_modified: a.last_modified,
                        checksum_algorithm,
                        checksum_value,
                    },
                )
            })
            .collect(),
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
        checksum: loaded_checksum(meta.checksum_algorithm.as_deref(), meta.checksum_value),
    }
}

pub fn multipart_upload_from_loaded(lm: LoadedMpu) -> MultipartUpload {
    let LoadedMpu { init, parts } = lm;
    let checksum_algorithm = init
        .checksum_algorithm
        .as_deref()
        .and_then(ChecksumAlgorithm::parse);
    // An upload created before types were tracked used the algorithm's
    // default type.
    let checksum_type = checksum_algorithm.map(|a| {
        init.checksum_type
            .as_deref()
            .and_then(ChecksumType::parse)
            .unwrap_or_else(|| a.default_multipart_type())
    });
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
        bucket_key_enabled: init.bucket_key_enabled,
        tagging: init.tagging,
        acl_grants: init
            .acl_grants
            .iter()
            .map(acl_grant_from_snapshot)
            .collect(),
        checksum_algorithm,
        checksum_type,
        cache_control: init.cache_control,
        content_disposition: init.content_disposition,
        content_language: init.content_language,
        expires: init.expires,
        lock: crate::state::ObjectLockSettings {
            mode: init.lock_mode,
            retain_until: init.lock_retain_until,
            legal_hold: init.lock_legal_hold,
        },
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
    // An `acl.toml` written by an older build could be empty (its writer used
    // `unwrap_or_default()` on serialization failure). Treat that as no sidecar
    // at all, so the bucket keeps the default owner grant instead of loading
    // with no grants and silently stripping the owner's FULL_CONTROL.
    let has_acl_sidecar = subresources
        .get("acl.toml")
        .is_some_and(|text| !text.trim().is_empty());
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
        annotation_table_config: None,
        legacy_eu_location: meta.legacy_eu_location,
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
                // Drop grants that name nobody, so the loaded ACL is one the
                // server would also accept back from a client.
                let usable: Vec<AclGrant> = snap
                    .grants
                    .iter()
                    .filter(|g| acl_grant_names_a_grantee(g))
                    .map(acl_grant_from_snapshot)
                    .collect();
                // ...but never leave the bucket with no grants at all. A
                // sidecar whose every grant is unusable is indistinguishable
                // from one that was never written, so fall back to the same
                // default owner grant a bucket with no `acl.toml` gets, rather
                // than loading a bucket whose owner has lost FULL_CONTROL.
                b.acl_grants = if usable.is_empty() {
                    vec![AclGrant {
                        grantee_type: "CanonicalUser".to_string(),
                        grantee_id: Some(b.acl_owner_id.clone()),
                        grantee_display_name: Some(b.acl_owner_id.clone()),
                        grantee_uri: None,
                        permission: "FULL_CONTROL".to_string(),
                    }]
                } else {
                    usable
                };
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
                // Neither `unwrap_or_default` nor `?`. Defaulting would load the
                // bucket with no configurations and no word, and the next Put
                // would overwrite the file and make the loss permanent; failing
                // would hide every object in the bucket behind an unparseable
                // reporting config. Skip the sidecar, keep the bucket, and say so,
                // so the file is still there to look at.
                match toml::from_str(&text) {
                    Ok(parsed) => b.analytics_configs = parsed,
                    Err(e) => tracing::warn!(
                        bucket = %name,
                        error = %e,
                        "ignoring unreadable analytics.toml; its configurations are not loaded",
                    ),
                }
            }
            "intelligent_tiering.toml" => {
                if text.trim().is_empty() {
                    continue;
                }
                // Neither `unwrap_or_default` nor `?`. Defaulting would load the
                // bucket with no configurations and no word, and the next Put
                // would overwrite the file and make the loss permanent; failing
                // would hide every object in the bucket behind an unparseable
                // reporting config. Skip the sidecar, keep the bucket, and say so,
                // so the file is still there to look at.
                match toml::from_str(&text) {
                    Ok(parsed) => b.intelligent_tiering_configs = parsed,
                    Err(e) => tracing::warn!(
                        bucket = %name,
                        error = %e,
                        "ignoring unreadable intelligent_tiering.toml; its configurations are not loaded",
                    ),
                }
            }
            "metrics.toml" => {
                if text.trim().is_empty() {
                    continue;
                }
                // Neither `unwrap_or_default` nor `?`. Defaulting would load the
                // bucket with no configurations and no word, and the next Put
                // would overwrite the file and make the loss permanent; failing
                // would hide every object in the bucket behind an unparseable
                // reporting config. Skip the sidecar, keep the bucket, and say so,
                // so the file is still there to look at.
                match toml::from_str(&text) {
                    Ok(parsed) => b.metrics_configs = parsed,
                    Err(e) => tracing::warn!(
                        bucket = %name,
                        error = %e,
                        "ignoring unreadable metrics.toml; its configurations are not loaded",
                    ),
                }
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

/// Hydrate a loaded snapshot, reporting each bucket that cannot be used instead
/// of failing the whole load.
///
/// The store already isolates a bucket whose objects it cannot read: it warns,
/// records the refusal and carries on, so one bad file costs one bucket rather
/// than the server. Sidecars are parsed here, a layer later, and a single
/// malformed `acl.toml` or `tags.toml` used to abort startup -- every other
/// bucket inaccessible because of one file. Such a bucket is skipped and handed
/// to `refused` so it is reported exactly like a store-level refusal: absent
/// from memory, and its name refused by CreateBucket rather than treated as free
/// -- a create clears the whole stored directory, so a name that is merely
/// unreadable must not look available. Repair the file and restart, or remove
/// the directory, to get the name back.
///
/// Wire this to the store's `mark_bucket_load_refused`, not to a no-op: with the
/// hook dropped on the floor, a bucket skipped here is indistinguishable from a
/// name nobody has used, and the next create deletes its objects.
pub fn hydrate_s3_state_reporting(
    snapshot: S3StateSnapshot,
    account_id: &str,
    region: &str,
    refused: &mut dyn FnMut(&str, &str),
) -> Result<S3State, String> {
    let mut state = S3State::new(account_id, region);
    for (name, snap) in snapshot.buckets {
        match s3_bucket_from_snapshot(&name, snap, region) {
            Ok(bucket) => {
                state.buckets.insert(name, bucket);
            }
            Err(e) => {
                tracing::warn!(
                    bucket = %name,
                    error = %e,
                    "skipping S3 bucket whose stored configuration could not be read; its name is \
                     refused until the bad file is repaired and the server restarted, or the \
                     bucket's directory is removed",
                );
                refused(&name, &e);
            }
        }
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_eu_location_survives_a_snapshot_round_trip() {
        let mut bucket = S3Bucket::new("eu-b", "eu-west-1", "owner");
        bucket.legacy_eu_location = true;
        let snap = BucketSnapshot {
            meta: bucket_meta_snapshot(&bucket),
            ..Default::default()
        };
        let restored = s3_bucket_from_snapshot("eu-b", snap, "us-east-1").unwrap();
        assert!(restored.legacy_eu_location);
        assert_eq!(restored.region, "eu-west-1");
        // Metadata written before the field existed loads as a plain bucket.
        let old: BucketMeta =
            serde_json::from_str(r#"{"name":"old","region":"eu-west-1"}"#).unwrap();
        assert!(!old.legacy_eu_location);
    }

    #[test]
    fn bucket_from_an_older_snapshot_does_not_gain_the_new_bucket_defaults() {
        // A bucket persisted before CreateBucket applied S3's new-bucket
        // defaults has no ownership.toml, public_access_block.toml or
        // encryption.toml. AWS left existing buckets as they were, so it must
        // load with none of the three rather than having them invented.
        let snap = BucketSnapshot {
            meta: BucketMeta {
                name: "legacy".into(),
                acl_owner_id: "owner-xyz".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let b = s3_bucket_from_snapshot("legacy", snap, "us-east-1").unwrap();
        assert!(b.ownership_controls.is_none());
        assert!(b.public_access_block.is_none());
        assert!(b.encryption_config.is_none());

        // ...while a bucket persisted with them gets exactly what was stored.
        let mut fresh = S3Bucket::new("fresh", "us-east-1", "owner-xyz");
        fresh.apply_new_bucket_defaults();
        let mut subresources = BTreeMap::new();
        for (file, text) in [
            ("ownership.toml", fresh.ownership_controls.clone()),
            (
                "public_access_block.toml",
                fresh.public_access_block.clone(),
            ),
            ("encryption.toml", fresh.encryption_config.clone()),
        ] {
            subresources.insert(file.to_string(), text.unwrap());
        }
        let snap = BucketSnapshot {
            meta: bucket_meta_snapshot(&fresh),
            subresources,
            ..Default::default()
        };
        let b = s3_bucket_from_snapshot("fresh", snap, "us-east-1").unwrap();
        assert_eq!(b.ownership_controls, fresh.ownership_controls);
        assert_eq!(b.public_access_block, fresh.public_access_block);
        assert_eq!(b.encryption_config, fresh.encryption_config);
    }

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
        let until = chrono::Utc::now() + chrono::Duration::days(3);
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
            bucket_key_enabled: Some(true),
            tagging: None,
            acl_grants: Vec::new(),
            checksum_algorithm: None,
            checksum_type: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            lock: crate::state::ObjectLockSettings {
                mode: Some("GOVERNANCE".to_string()),
                retain_until: Some(until),
                legal_hold: Some("ON".to_string()),
            },
        };
        let snap = mpu_init_snapshot(&mpu);
        assert_eq!(snap.upload_id, "up-1");
        assert_eq!(snap.bucket_key_enabled, Some(true));
        assert_eq!(snap.content_type, "application/octet-stream");
        assert_eq!(snap.lock_mode.as_deref(), Some("GOVERNANCE"));
        assert_eq!(snap.lock_retain_until, Some(until));
        assert_eq!(snap.lock_legal_hold.as_deref(), Some("ON"));
        // The snapshot round-trips through the loader with the lock intact.
        let loaded = multipart_upload_from_loaded(fakecloud_persistence::LoadedMpu {
            init: snap,
            parts: BTreeMap::new(),
        });
        assert_eq!(loaded.lock, mpu.lock);
    }

    #[test]
    fn upload_part_meta_snapshot_copies_identifiers() {
        let part = UploadPart {
            part_number: 3,
            body: crate::state::memory_body(bytes::Bytes::from_static(b"abc")),
            etag: "peeetag".to_string(),
            size: 3,
            last_modified: chrono::Utc::now(),
            checksum: None,
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
        // The no-op reporter is spelled out here on purpose: production must wire
        // this to the store's `mark_bucket_load_refused`, and a convenience
        // wrapper that defaulted to discarding refusals is what let the
        // sidecar-refusal hole ship in the first place.
        let state =
            hydrate_s3_state_reporting(snapshot, "123", "us-east-1", &mut |_, _| {}).unwrap();
        assert!(state.buckets.contains_key("b"));
    }
}

#[cfg(test)]
mod checksum_persistence_tests {
    use super::*;
    use crate::checksum::{checksum, ChecksumAlgorithm, ChecksumType};
    use fakecloud_persistence::AnnotationSnapshot;

    fn loaded(meta: ObjectMeta) -> S3Object {
        s3_object_from_loaded(LoadedObject {
            meta,
            body: crate::state::memory_body(bytes::Bytes::from_static(b"payload")),
        })
    }

    #[test]
    fn lowercase_persisted_algorithm_names_are_read_back() {
        let crc = checksum(ChecksumAlgorithm::Crc32, b"payload");
        let obj = loaded(ObjectMeta {
            key: "k".into(),
            checksum_algorithm: Some("crc32".into()),
            checksum_value: Some(crc.clone()),
            annotations: vec![AnnotationSnapshot {
                name: "a".into(),
                payload: b"note".to_vec(),
                checksum_algorithm: Some("sha256".into()),
                // Written before annotation checksum values were stored.
                checksum_value: None,
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(obj.checksum_algorithm, Some(ChecksumAlgorithm::Crc32));
        assert_eq!(obj.checksum_value, Some(crc));
        // No stored type: a single-part object is FULL_OBJECT.
        assert_eq!(
            obj.effective_checksum_type(),
            Some(ChecksumType::FullObject)
        );
        let a = &obj.annotations["a"];
        assert_eq!(a.checksum_algorithm, Some(ChecksumAlgorithm::Sha256));
        assert_eq!(
            a.checksum_value,
            Some(checksum(ChecksumAlgorithm::Sha256, b"note"))
        );

        // Round trip writes the canonical uppercase name back.
        let meta = object_meta_snapshot(&obj);
        assert_eq!(meta.checksum_algorithm.as_deref(), Some("CRC32"));
        assert_eq!(
            annotation_record(a).checksum_algorithm.as_deref(),
            Some("SHA256")
        );
    }

    #[test]
    fn legacy_multipart_checksum_reads_as_composite_and_stub_values_are_dropped() {
        let obj = loaded(ObjectMeta {
            key: "k".into(),
            parts_count: Some(2),
            checksum_algorithm: Some("Sha256".into()),
            checksum_value: Some("abc-2".into()),
            ..Default::default()
        });
        assert_eq!(obj.effective_checksum_type(), Some(ChecksumType::Composite));
        // The old stub stored an empty value for algorithms it could not hash.
        let stub = loaded(ObjectMeta {
            key: "k".into(),
            checksum_algorithm: Some("SHA512".into()),
            checksum_value: Some(String::new()),
            ..Default::default()
        });
        assert_eq!(stub.checksum_algorithm, None);
        assert_eq!(stub.checksum_value, None);
        assert_eq!(stub.effective_checksum_type(), None);
    }

    #[test]
    fn multipart_upload_checksum_settings_round_trip() {
        let mut init = MpuInit {
            upload_id: "u".into(),
            key: "k".into(),
            checksum_algorithm: Some("crc32c".into()),
            ..Default::default()
        };
        let part = fakecloud_persistence::UploadPartMeta {
            part_number: 1,
            etag: "e".into(),
            size: 1,
            last_modified: chrono::Utc::now(),
            checksum_algorithm: Some("crc32c".into()),
            checksum_value: Some("AAAAAA==".into()),
        };
        let mut parts = std::collections::BTreeMap::new();
        parts.insert(
            1,
            LoadedPart {
                meta: part,
                body: crate::state::memory_body(bytes::Bytes::from_static(b"x")),
            },
        );
        let m = multipart_upload_from_loaded(LoadedMpu {
            init: init.clone(),
            parts,
        });
        assert_eq!(m.checksum_algorithm, Some(ChecksumAlgorithm::Crc32c));
        // Older uploads carry no type: the algorithm's default applies.
        assert_eq!(m.checksum_type, Some(ChecksumType::Composite));
        assert_eq!(
            m.parts[&1].checksum,
            Some((ChecksumAlgorithm::Crc32c, "AAAAAA==".to_string()))
        );
        init.checksum_type = Some("FULL_OBJECT".into());
        let m = multipart_upload_from_loaded(LoadedMpu {
            init,
            parts: Default::default(),
        });
        assert_eq!(m.checksum_type, Some(ChecksumType::FullObject));
        let snap = mpu_init_snapshot(&m);
        assert_eq!(snap.checksum_algorithm.as_deref(), Some("CRC32C"));
        assert_eq!(snap.checksum_type.as_deref(), Some("FULL_OBJECT"));
    }
}
