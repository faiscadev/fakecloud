use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, StatusCode};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError, ResponseBody};

use fakecloud_persistence::{BodyRef, BodySource};

use crate::persistence::object_meta_snapshot;
use crate::state::{AclGrant, S3Object};

use super::{
    check_get_conditionals, check_head_conditionals, check_object_lock_for_overwrite,
    deliver_notifications, etag_matches, extract_user_metadata, governance_bypass_requested,
    has_object_lock_headers, insert_user_metadata_headers, is_frozen, is_valid_storage_class,
    make_delete_marker, no_such_bucket, no_such_key, parse_delete_objects_quiet,
    parse_delete_objects_xml, parse_range_header, parse_url_encoded_tags, precondition_failed,
    resolve_object, s3_xml, url_encode_s3_key, xml_escape, ObjectLockSettings, RangeResult,
    S3Service,
};

mod delete;
mod list;
mod post_policy;
mod read;
mod website;
mod write;

impl S3Service {}

/// Whether `obj` occupies the bucket's "null" version slot: either it predates
/// versioning (no id at all) or it was written while versioning was suspended.
pub(crate) fn is_null_version(obj: &crate::state::S3Object) -> bool {
    obj.version_id.is_none() || obj.version_id.as_deref() == Some("null")
}

/// The current object, tagged as the `"null"` version, when a new version is
/// about to be stacked on top of it and the history does not already hold a
/// null entry. Pure: the caller persists the returned object's sidecar under
/// the `"null"` slot and only then records it in the history, so a failed
/// write cannot leave a version in memory that disk lacks.
///
/// The sidecar rewrite matters because the loader files a `"null"` slot whose
/// metadata carries no version id as the CURRENT object, where the newer
/// version then replaces it -- without the id, this version is lost on the
/// next restart.
pub(crate) fn null_version_to_preserve(
    b: &crate::state::S3Bucket,
    key: &str,
) -> Option<crate::state::S3Object> {
    let history_has_null = b
        .object_versions
        .get(key)
        .map(|versions| versions.iter().any(is_null_version))
        .unwrap_or(false);
    if history_has_null {
        return None;
    }
    // A suspended-bucket write tags its object `Some("null")`, so match on
    // "occupies the null slot" rather than "has no id at all".
    let existing = b.objects.get(key).filter(|o| is_null_version(o))?;
    let mut preserved = existing.clone();
    preserved.version_id = Some("null".to_string());
    Some(preserved)
}

/// Record a preserved null version in memory: push it into the history and
/// retag the current object with the same `"null"` id. Retagging matters
/// because the object in `objects` and the one now in `object_versions` are
/// the same bytes, and the sidecar on disk carries the id -- leaving the
/// current copy untagged makes `GetObject` report no version while
/// `ListObjectVersions` shows a null one, which is also the state a later
/// failure on the write path would freeze in place.
pub(crate) fn record_preserved_null(
    b: &mut crate::state::S3Bucket,
    key: &str,
    preserved: crate::state::S3Object,
) {
    if let Some(current) = b.objects.get_mut(key) {
        if current.version_id.is_none() {
            current.version_id = Some("null".to_string());
        }
    }
    b.object_versions
        .entry(key.to_string())
        .or_default()
        .push(preserved);
}

/// Record `obj` as the bucket's null version after a write to a
/// versioning-suspended bucket, replacing whatever held that slot (an older
/// null object, or a null delete marker) exactly as AWS does. Only touches
/// the history when the key already has one -- listings read the current
/// object directly otherwise.
pub(crate) fn replace_null_version(
    b: &mut crate::state::S3Bucket,
    key: &str,
    obj: &crate::state::S3Object,
) {
    let Some(versions) = b.object_versions.get_mut(key) else {
        return;
    };
    versions.retain(|o| !is_null_version(o));
    versions.push(obj.clone());
}

/// Commit `obj` as the newest version of its key in `b` (the bucket named
/// `bucket`), following the
/// bucket's versioning state the way every S3 write does:
///
/// - Enabled: the object gets a version id (the caller's, when it already
///   assigned a real one, otherwise a fresh one), a pre-versioning object at
///   the key is preserved as the `"null"` version, and the new version is
///   appended to the history.
/// - Suspended: the object takes over the `"null"` version, replacing
///   whatever held it (an older null object or a null delete marker).
/// - Never enabled: the object replaces the current one and carries no id.
///
/// An object with no explicit retention of its own gets the bucket's default
/// retention, measured from the commit.
///
/// The body is written through `store` (when one is configured) before the
/// new version is recorded in memory, so a failed write returns the error and
/// records no new version. A pre-versioning object relabelled as the null
/// version stays relabelled (disk already says so). With no store, `obj.body`
/// must already hold the payload. A caller-supplied id naming a version
/// already in the history rewrites that version in place; it becomes the
/// current object only if it is the newest.
///
/// Returns the version id to report to the writer: the new id on a
/// versioning-enabled bucket, `None` otherwise.
pub(crate) fn commit_object_version(
    b: &mut crate::state::S3Bucket,
    bucket: &str,
    store: Option<&dyn fakecloud_persistence::S3Store>,
    obj: S3Object,
    body: BodySource,
) -> fakecloud_persistence::StoreResult<Option<String>> {
    commit_object_version_with(b, bucket, store, obj, |store, obj, meta| {
        store.put_object(bucket, &obj.key, meta.version_id.as_deref(), body, meta)
    })
}

/// [`commit_object_version`] with the body write supplied by the caller:
/// `write_body` persists the new version's body (given the store, the object
/// as it will be recorded and its metadata, carrying its final version id) and
/// returns the body ref to keep in memory. CompleteMultipartUpload uses it to assemble
/// the parts in the store instead of writing the object a second time.
pub(crate) fn commit_object_version_with<F>(
    b: &mut crate::state::S3Bucket,
    bucket: &str,
    store: Option<&dyn fakecloud_persistence::S3Store>,
    mut obj: S3Object,
    write_body: F,
) -> fakecloud_persistence::StoreResult<Option<String>>
where
    F: FnOnce(
        &dyn fakecloud_persistence::S3Store,
        &S3Object,
        &fakecloud_persistence::ObjectMeta,
    ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef>,
{
    let key = obj.key.clone();
    let versioning_enabled = b.versioning.as_deref() == Some("Enabled");
    let versioning_suspended = b.versioning.as_deref() == Some("Suspended");

    // The history position of an existing version the caller named again (a
    // replica of a source version that was already replicated). Only a
    // caller-supplied id can already be there; a freshly minted one cannot.
    let mut existing_position = None;
    if versioning_enabled {
        if obj.version_id.as_deref().is_none_or(|v| v == "null") {
            obj.version_id = Some(Uuid::new_v4().to_string());
        } else if let Some(versions) = b.object_versions.get(&key) {
            existing_position = versions.iter().position(|o| o.version_id == obj.version_id);
            // Rewriting a version keeps its place in the history, which the
            // loader rebuilds from LastModified.
            if let Some(pos) = existing_position {
                obj.last_modified = versions[pos].last_modified;
            }
        }
        // The current object becomes the "null" version once a real version
        // is stacked on top of it. Persist the rewritten sidecar BEFORE
        // recording it in memory, so a failed write cannot leave a version in
        // the history that disk lacks; once it is persisted, record it at
        // once. It only relabels the object already there, so it is right
        // whether or not the body write below succeeds, and memory keeps
        // agreeing with disk if that write fails.
        if let Some(preserved) = null_version_to_preserve(b, &key) {
            if let Some(store) = store {
                let preserved_meta = object_meta_snapshot(&preserved);
                run_blocking_io(|| {
                    store.put_object_meta(bucket, &key, Some("null"), &preserved_meta)
                })?;
            }
            record_preserved_null(b, &key, preserved);
        }
    } else if versioning_suspended {
        // Tagging it "null" also keeps the disk sidecar in the versioned set,
        // so a restart-loaded bucket agrees with the live one about which
        // object is current.
        obj.version_id = Some("null".to_string());
    } else {
        obj.version_id = None;
    }

    // The one place a new version falls back to the bucket's default
    // retention: an object that carries no explicit retention (request
    // headers, or a replica's source lock) gets the default, measured from
    // now, the moment the version is committed. A delete marker (a
    // replicated one) holds no data and is never locked.
    if !obj.is_delete_marker {
        let lock = crate::state::ObjectLockSettings {
            mode: obj.lock_mode.take(),
            retain_until: obj.lock_retain_until.take(),
            legal_hold: obj.lock_legal_hold.take(),
        }
        .or_bucket_default(b.object_lock_config.as_deref(), Utc::now());
        obj.lock_mode = lock.mode;
        obj.lock_retain_until = lock.retain_until;
        obj.lock_legal_hold = lock.legal_hold;
    }

    if let Some(store) = store {
        let meta = object_meta_snapshot(&obj);
        obj.body = run_blocking_io(|| write_body(store, &obj, &meta))?;
    }

    // The body is persisted: only now record the new version in memory, so a
    // failed write leaves no version in memory that disk lacks.
    let mut becomes_current = true;
    if versioning_enabled {
        let versions = b.object_versions.entry(key.clone()).or_default();
        match existing_position {
            // A version written again keeps its place; it is the current
            // object only if it is still the newest.
            Some(pos) => {
                becomes_current = pos + 1 == versions.len();
                versions[pos] = obj.clone();
            }
            None => versions.push(obj.clone()),
        }
    } else if versioning_suspended {
        replace_null_version(b, &key, &obj);
    }
    let reported = if versioning_enabled {
        obj.version_id.clone()
    } else {
        None
    };
    if becomes_current {
        b.objects.insert(key, obj);
    }
    Ok(reported)
}

/// Run a blocking closure (synchronous disk IO) without starving the async
/// runtime. On a multi-threaded tokio runtime this uses `block_in_place` so
/// the worker hands its other tasks to a sibling thread for the duration of
/// the IO; on a current-thread runtime (e.g. `#[tokio::test]`) `block_in_place`
/// would panic, so we just run the closure inline. The lock-hold semantics of
/// the caller are unchanged — this only stops a large body copy from blocking
/// unrelated async tasks parked on the same worker.
pub(crate) fn run_blocking_io<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    use tokio::runtime::{Handle, RuntimeFlavor};
    match Handle::try_current() {
        Ok(h) if h.runtime_flavor() == RuntimeFlavor::MultiThread => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// Build the response body for a full GetObject read. For memory-backed
/// bodies this returns `ResponseBody::Bytes`; for disk-backed bodies it
/// opens the file handle eagerly (while the caller still holds the per-state
/// read guard) and returns `ResponseBody::File` so the dispatcher can stream
/// the file directly into the HTTP response without materializing it in RAM.
///
/// Opening the handle inside the read guard is load-bearing: on unix an open
/// fd keeps the old inode alive even after the path is renamed over or
/// unlinked, so a concurrent PUT/DELETE that lands after we drop the guard
/// cannot hand the reader a partial or swapped body.
fn full_body_response(
    state: &crate::state::S3State,
    body: &fakecloud_persistence::BodyRef,
) -> Result<ResponseBody, AwsServiceError> {
    match body {
        fakecloud_persistence::BodyRef::Memory(_) => {
            let bytes = state.read_body(body).map_err(crate::service::io_to_aws)?;
            Ok(ResponseBody::Bytes(bytes))
        }
        fakecloud_persistence::BodyRef::Disk { path, size, .. } => {
            let std_file = std::fs::File::open(path).map_err(crate::service::io_to_aws)?;
            let file = tokio::fs::File::from_std(std_file);
            Ok(ResponseBody::File { file, size: *size })
        }
    }
}

#[cfg(test)]
mod null_version_tests {
    use super::{null_version_to_preserve, record_preserved_null, replace_null_version};
    use crate::state::{S3Bucket, S3Object};

    fn bucket() -> S3Bucket {
        S3Bucket::new("b", "us-east-1", "123456789012")
    }

    fn object(version_id: Option<&str>, etag: &str, is_delete_marker: bool) -> S3Object {
        S3Object {
            key: "k".to_string(),
            version_id: version_id.map(|v| v.to_string()),
            etag: etag.to_string(),
            is_delete_marker,
            ..Default::default()
        }
    }

    #[test]
    fn preserves_a_pre_versioning_object_tagged_null() {
        let mut b = bucket();
        b.objects.insert("k".to_string(), object(None, "e1", false));
        let preserved = null_version_to_preserve(&b, "k").expect("current object is the null slot");
        assert_eq!(preserved.version_id.as_deref(), Some("null"));
        assert_eq!(preserved.etag, "e1");
    }

    #[test]
    fn preserves_an_object_written_while_suspended() {
        // A suspended write tags its object "null", so "has no version id" is
        // the wrong test for the null slot.
        let mut b = bucket();
        b.objects
            .insert("k".to_string(), object(Some("null"), "e1", false));
        assert!(null_version_to_preserve(&b, "k").is_some());
    }

    #[test]
    fn preserves_nothing_when_the_history_already_holds_a_null() {
        let mut b = bucket();
        b.objects.insert("k".to_string(), object(None, "e1", false));
        b.object_versions
            .insert("k".to_string(), vec![object(Some("null"), "e0", false)]);
        assert!(null_version_to_preserve(&b, "k").is_none());
    }

    #[test]
    fn preserves_nothing_for_a_real_version() {
        let mut b = bucket();
        b.objects
            .insert("k".to_string(), object(Some("v1"), "e1", false));
        assert!(null_version_to_preserve(&b, "k").is_none());
    }

    #[test]
    fn preserves_nothing_when_the_key_has_no_current_object() {
        // A key whose current object was removed (a delete marker path, say)
        // has nothing to preserve, even with real versions in the history.
        let mut b = bucket();
        b.object_versions
            .insert("k".to_string(), vec![object(Some("v1"), "e1", false)]);
        assert!(null_version_to_preserve(&b, "k").is_none());
    }

    #[test]
    fn recording_pushes_the_version_and_retags_the_current_object() {
        // The retag keeps memory agreeing with the sidecar, which the caller
        // rewrote to carry the "null" id: without it GetObject reports no
        // version while ListObjectVersions shows one, and a failure on the
        // write that follows freezes that disagreement.
        let mut b = bucket();
        b.objects.insert("k".to_string(), object(None, "e1", false));
        let preserved = null_version_to_preserve(&b, "k").unwrap();
        record_preserved_null(&mut b, "k", preserved);

        assert_eq!(
            b.objects.get("k").unwrap().version_id.as_deref(),
            Some("null")
        );
        let versions = b.object_versions.get("k").expect("history entry");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version_id.as_deref(), Some("null"));
        assert_eq!(versions[0].etag, "e1");
    }

    #[test]
    fn recording_leaves_a_real_current_version_alone() {
        let mut b = bucket();
        b.objects
            .insert("k".to_string(), object(Some("v2"), "e2", false));
        record_preserved_null(&mut b, "k", object(Some("null"), "e1", false));
        assert_eq!(
            b.objects.get("k").unwrap().version_id.as_deref(),
            Some("v2")
        );
    }

    #[test]
    fn replacing_drops_the_previous_null_including_a_marker() {
        let mut b = bucket();
        b.object_versions.insert(
            "k".to_string(),
            vec![
                object(Some("v1"), "e1", false),
                object(Some("null"), "", true),
            ],
        );
        let fresh = object(Some("null"), "e3", false);
        replace_null_version(&mut b, "k", &fresh);

        let versions = b.object_versions.get("k").unwrap();
        assert_eq!(versions.len(), 2, "v1 plus the new null: {versions:?}");
        assert_eq!(versions[0].version_id.as_deref(), Some("v1"));
        assert_eq!(versions[1].etag, "e3");
        assert!(!versions[1].is_delete_marker);
    }

    #[test]
    fn replacing_without_history_touches_nothing() {
        // A key whose null object lives only in `objects` needs no history
        // entry -- listings read the current object directly.
        let mut b = bucket();
        replace_null_version(&mut b, "k", &object(Some("null"), "e1", false));
        assert!(!b.object_versions.contains_key("k"));
    }
}
