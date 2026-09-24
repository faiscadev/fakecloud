mod helpers;

use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, CorsConfiguration, CorsRule,
    CreateBucketConfiguration, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold,
    ObjectLockLegalHoldStatus, ServerSideEncryption, ServerSideEncryptionByDefault,
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule, StorageClass, Tag, Tagging,
    VersioningConfiguration,
};
use helpers::{run_until_exit, TestServer};
use sha2::{Digest, Sha256};

fn pseudo_random_bytes(seed: u64, len: usize) -> Vec<u8> {
    // Deterministic xorshift-based filler — good enough for round-trip tests.
    let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().into()
}

#[tokio::test]
async fn persistence_round_trip_objects_with_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("meta-bucket")
        .send()
        .await
        .unwrap();

    client
        .put_object_lock_configuration()
        .bucket("meta-bucket")
        .object_lock_configuration(
            ObjectLockConfiguration::builder()
                .object_lock_enabled(ObjectLockEnabled::Enabled)
                .build(),
        )
        .send()
        .await
        .ok();

    let objects: Vec<(&str, Vec<u8>, &str)> = vec![
        ("plain.txt", b"hello world".to_vec(), "text/plain"),
        (
            "binary.bin",
            vec![0u8, 1, 2, 3, 255, 254, 253],
            "application/octet-stream",
        ),
        ("doc.json", br#"{"ok":true}"#.to_vec(), "application/json"),
        ("big.txt", vec![b'x'; 64 * 1024], "text/plain"),
        ("tagged.txt", b"tagged".to_vec(), "text/plain"),
    ];

    for (i, (key, body, ct)) in objects.iter().enumerate() {
        let mut put = client
            .put_object()
            .bucket("meta-bucket")
            .key(*key)
            .body(ByteStream::from(body.clone()))
            .content_type(*ct)
            .metadata("custom", format!("value-{}", i))
            .metadata("index", i.to_string())
            .tagging(format!("env=prod&rank={}", i))
            .server_side_encryption(ServerSideEncryption::Aes256);
        if i == 0 {
            put = put.storage_class(StorageClass::StandardIa);
        }
        put.send().await.unwrap();
    }

    // Legal hold on a specific object.
    client
        .put_object_legal_hold()
        .bucket("meta-bucket")
        .key("tagged.txt")
        .legal_hold(
            ObjectLockLegalHold::builder()
                .status(ObjectLockLegalHoldStatus::On)
                .build(),
        )
        .send()
        .await
        .ok();

    server.restart().await;
    let client = server.s3_client().await;

    for (i, (key, body, ct)) in objects.iter().enumerate() {
        let head = client
            .head_object()
            .bucket("meta-bucket")
            .key(*key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_type(), Some(*ct), "content-type for {}", key);
        let meta = head.metadata().unwrap();
        assert_eq!(
            meta.get("custom").map(String::as_str),
            Some(&*format!("value-{}", i))
        );

        let get = client
            .get_object()
            .bucket("meta-bucket")
            .key(*key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, *body, "body mismatch for {}", key);

        let tags = client
            .get_object_tagging()
            .bucket("meta-bucket")
            .key(*key)
            .send()
            .await
            .unwrap();
        let tag_set = tags.tag_set();
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "env" && t.value() == "prod"));
    }

    let resp = client
        .get_object_legal_hold()
        .bucket("meta-bucket")
        .key("tagged.txt")
        .send()
        .await
        .expect("legal hold must round-trip across restart");
    let lh = resp
        .legal_hold()
        .expect("legal hold struct must be present after restart");
    assert_eq!(
        lh.status(),
        Some(&ObjectLockLegalHoldStatus::On),
        "legal hold status must persist across restart",
    );
}

#[tokio::test]
async fn persistence_large_object_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("big-bucket")
        .send()
        .await
        .unwrap();

    const SIZE: usize = 50 * 1024 * 1024;
    let body = pseudo_random_bytes(42, SIZE);
    let expected = sha256(&body);

    client
        .put_object()
        .bucket("big-bucket")
        .key("large.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();

    server.restart().await;
    let client = server.s3_client().await;

    let get = client
        .get_object()
        .bucket("big-bucket")
        .key("large.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(get.content_length(), Some(SIZE as i64));
    let bytes = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(bytes.len(), SIZE);
    assert_eq!(sha256(&bytes), expected);

    // On-disk .bin file size check. Object-dir layout uses an opaque
    // escape of the bucket/key so we walk and pick the largest file.
    let mut largest: u64 = 0;
    for entry in walkdir(tmp.path()) {
        if entry.extension().and_then(|s| s.to_str()) == Some("bin") {
            if let Ok(md) = std::fs::metadata(&entry) {
                largest = largest.max(md.len());
            }
        }
    }
    assert_eq!(largest, SIZE as u64, "on-disk .bin size mismatch");
}

fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(p) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&p) else {
            continue;
        };
        for entry in rd.flatten() {
            let ep = entry.path();
            if ep.is_dir() {
                stack.push(ep);
            } else {
                out.push(ep);
            }
        }
    }
    out
}

#[tokio::test]
async fn persistence_multipart_resume_and_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("mpu-bucket")
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("mpu-bucket")
        .key("resumable.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    const PART_SIZE: usize = 5 * 1024 * 1024 + 1024;
    let parts: Vec<Vec<u8>> = (0..5)
        .map(|i| pseudo_random_bytes(i as u64 + 100, PART_SIZE))
        .collect();

    let mut completed: Vec<CompletedPart> = Vec::new();

    for (idx, body) in parts.iter().enumerate().take(3) {
        let resp = client
            .upload_part()
            .bucket("mpu-bucket")
            .key("resumable.bin")
            .upload_id(&upload_id)
            .part_number((idx + 1) as i32)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .unwrap();
        completed.push(
            CompletedPart::builder()
                .part_number((idx + 1) as i32)
                .e_tag(resp.e_tag().unwrap_or_default())
                .build(),
        );
    }

    server.restart().await;
    let client = server.s3_client().await;

    for (idx, body) in parts.iter().enumerate().take(5).skip(3) {
        let resp = client
            .upload_part()
            .bucket("mpu-bucket")
            .key("resumable.bin")
            .upload_id(&upload_id)
            .part_number((idx + 1) as i32)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .unwrap();
        completed.push(
            CompletedPart::builder()
                .part_number((idx + 1) as i32)
                .e_tag(resp.e_tag().unwrap_or_default())
                .build(),
        );
    }

    client
        .complete_multipart_upload()
        .bucket("mpu-bucket")
        .key("resumable.bin")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let get = client
        .get_object()
        .bucket("mpu-bucket")
        .key("resumable.bin")
        .send()
        .await
        .unwrap();
    let body = get.body.collect().await.unwrap().into_bytes().to_vec();
    let expected: Vec<u8> = parts.iter().flatten().copied().collect();
    assert_eq!(body.len(), expected.len());
    assert_eq!(sha256(&body), sha256(&expected));
}

#[tokio::test]
async fn persistence_multipart_abort_clears_state() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("abort-bucket")
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("abort-bucket")
        .key("aborted.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    for n in 1..=3 {
        client
            .upload_part()
            .bucket("abort-bucket")
            .key("aborted.bin")
            .upload_id(&upload_id)
            .part_number(n)
            .body(ByteStream::from(pseudo_random_bytes(
                n as u64,
                5 * 1024 * 1024 + 32,
            )))
            .send()
            .await
            .unwrap();
    }

    server.restart().await;
    let client = server.s3_client().await;

    client
        .abort_multipart_upload()
        .bucket("abort-bucket")
        .key("aborted.bin")
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap();

    let list = client
        .list_multipart_uploads()
        .bucket("abort-bucket")
        .send()
        .await
        .unwrap();
    assert!(list.uploads().is_empty());
}

#[tokio::test]
async fn persistence_versioning_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("ver-bucket")
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket("ver-bucket")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let bodies: Vec<&[u8]> = vec![b"version-1", b"version-2", b"version-3"];
    for body in &bodies {
        client
            .put_object()
            .bucket("ver-bucket")
            .key("doc.txt")
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .unwrap();
    }
    client
        .delete_object()
        .bucket("ver-bucket")
        .key("doc.txt")
        .send()
        .await
        .unwrap();

    server.restart().await;
    let client = server.s3_client().await;

    let list = client
        .list_object_versions()
        .bucket("ver-bucket")
        .send()
        .await
        .unwrap();
    let versions = list.versions();
    let markers = list.delete_markers();
    assert_eq!(versions.len(), 3, "expected 3 versions");
    assert_eq!(markers.len(), 1, "expected 1 delete marker");

    for (i, body) in bodies.iter().enumerate() {
        let v = &versions[versions.len() - 1 - i];
        let vid = v.version_id().unwrap().to_string();
        let get = client
            .get_object()
            .bucket("ver-bucket")
            .key("doc.txt")
            .version_id(vid)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.as_ref(), *body, "body mismatch for version index {}", i);
    }
}

#[tokio::test]
async fn persistence_batch_delete_marker_survives_restart() {
    // DeleteObjects used to record its delete marker only in memory, so a
    // restart resurrected the deleted key with its newest version current.
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("batch-marker")
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket("batch-marker")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("batch-marker")
        .key("doc.txt")
        .body(ByteStream::from_static(b"v1"))
        .send()
        .await
        .unwrap();

    client
        .delete_objects()
        .bucket("batch-marker")
        .delete(
            aws_sdk_s3::types::Delete::builder()
                .objects(
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key("doc.txt")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    server.restart().await;
    let client = server.s3_client().await;

    let err = client
        .get_object()
        .bucket("batch-marker")
        .key("doc.txt")
        .send()
        .await
        .expect_err("the delete marker must survive the restart");
    assert!(
        format!("{err:?}").contains("NoSuchKey"),
        "expected NoSuchKey after restart, got: {err:?}"
    );

    let list = client
        .list_object_versions()
        .bucket("batch-marker")
        .send()
        .await
        .unwrap();
    assert_eq!(list.delete_markers().len(), 1, "marker must be persisted");
    assert_eq!(list.versions().len(), 1, "the original version survives");
}

#[tokio::test]
async fn persistence_preserved_null_version_survives_delete_marker() {
    // An object written before versioning was enabled becomes the "null"
    // version when a delete marker is stacked on top. Its on-disk sidecar has
    // to record that version id, otherwise the loader files it as the current
    // object and the marker hides it for good.
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("preserve-null")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("preserve-null")
        .key("doc.txt")
        .body(ByteStream::from_static(b"pre-versioning"))
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket("preserve-null")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    client
        .delete_object()
        .bucket("preserve-null")
        .key("doc.txt")
        .send()
        .await
        .unwrap();

    server.restart().await;
    let client = server.s3_client().await;

    let list = client
        .list_object_versions()
        .bucket("preserve-null")
        .send()
        .await
        .unwrap();
    assert_eq!(list.delete_markers().len(), 1, "marker survives");
    assert_eq!(
        list.versions().len(),
        1,
        "the pre-versioning object survives as the null version"
    );

    let got = client
        .get_object()
        .bucket("preserve-null")
        .key("doc.txt")
        .version_id("null")
        .send()
        .await
        .expect("the null version is still readable after the restart");
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(bytes.as_ref(), b"pre-versioning");
}

#[tokio::test]
async fn persistence_preserved_null_version_survives_new_version() {
    // Same sidecar rule as the delete-marker case, on the put path: writing a
    // new version over a pre-versioning object must record the null version id
    // on disk or the loader drops that version on restart.
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("preserve-null-put")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("preserve-null-put")
        .key("doc.txt")
        .body(ByteStream::from_static(b"pre-versioning"))
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket("preserve-null-put")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("preserve-null-put")
        .key("doc.txt")
        .body(ByteStream::from_static(b"v2"))
        .send()
        .await
        .unwrap();

    server.restart().await;
    let client = server.s3_client().await;

    let list = client
        .list_object_versions()
        .bucket("preserve-null-put")
        .send()
        .await
        .unwrap();
    assert_eq!(
        list.versions().len(),
        2,
        "the null version and v2 both survive: {:?}",
        list.versions()
    );

    let got = client
        .get_object()
        .bucket("preserve-null-put")
        .key("doc.txt")
        .version_id("null")
        .send()
        .await
        .expect("the null version is still readable after the restart");
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(bytes.as_ref(), b"pre-versioning");

    let current = client
        .get_object()
        .bucket("preserve-null-put")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    let bytes = current.body.collect().await.unwrap().into_bytes();
    assert_eq!(bytes.as_ref(), b"v2", "v2 is still current");
}

#[tokio::test]
async fn persistence_null_version_preserved_after_suspend_then_reenable() {
    // Enabled -> v1, Suspended -> null current, Enabled -> v2. The history is
    // not empty at the last put, so a guard keyed on "history is empty" would
    // skip preserving the null version and lose it.
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    let set_versioning = |status: BucketVersioningStatus| {
        let client = client.clone();
        async move {
            client
                .put_bucket_versioning()
                .bucket("suspend-cycle")
                .versioning_configuration(VersioningConfiguration::builder().status(status).build())
                .send()
                .await
                .unwrap();
        }
    };

    client
        .create_bucket()
        .bucket("suspend-cycle")
        .send()
        .await
        .unwrap();
    set_versioning(BucketVersioningStatus::Enabled).await;
    client
        .put_object()
        .bucket("suspend-cycle")
        .key("doc.txt")
        .body(ByteStream::from_static(b"v1"))
        .send()
        .await
        .unwrap();

    set_versioning(BucketVersioningStatus::Suspended).await;
    client
        .put_object()
        .bucket("suspend-cycle")
        .key("doc.txt")
        .body(ByteStream::from_static(b"null-body"))
        .send()
        .await
        .unwrap();

    set_versioning(BucketVersioningStatus::Enabled).await;
    client
        .put_object()
        .bucket("suspend-cycle")
        .key("doc.txt")
        .body(ByteStream::from_static(b"v2"))
        .send()
        .await
        .unwrap();

    let got = client
        .get_object()
        .bucket("suspend-cycle")
        .key("doc.txt")
        .version_id("null")
        .send()
        .await
        .expect("null version must exist before the restart");
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(bytes.as_ref(), b"null-body");

    server.restart().await;
    let client = server.s3_client().await;

    let got = client
        .get_object()
        .bucket("suspend-cycle")
        .key("doc.txt")
        .version_id("null")
        .send()
        .await
        .expect("null version must survive the restart");
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(bytes.as_ref(), b"null-body");
}

#[tokio::test]
async fn persistence_suspended_put_null_version_is_current_after_restart() {
    // A suspended write owns the null version. Its sidecar has to record that
    // id, otherwise the loader files it as a bare current object and the
    // newest enabled-era version wins the reconcile, flipping what is current.
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("susp-restart")
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket("susp-restart")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("susp-restart")
        .key("doc.txt")
        .body(ByteStream::from_static(b"v1"))
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket("susp-restart")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("susp-restart")
        .key("doc.txt")
        .body(ByteStream::from_static(b"null-body"))
        .send()
        .await
        .unwrap();

    server.restart().await;
    let client = server.s3_client().await;

    let current = client
        .get_object()
        .bucket("susp-restart")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    let bytes = current.body.collect().await.unwrap().into_bytes();
    assert_eq!(
        bytes.as_ref(),
        b"null-body",
        "the suspended write is still current after the restart"
    );

    let list = client
        .list_object_versions()
        .bucket("susp-restart")
        .send()
        .await
        .unwrap();
    assert_eq!(
        list.versions().len(),
        2,
        "null version + v1: {:?}",
        list.versions()
    );
}

#[tokio::test]
async fn persistence_suspended_mpu_is_current_after_restart() {
    // Same rule as the suspended PutObject: a CompleteMultipartUpload on a
    // suspended bucket owns the null version, and its sidecar has to say so or
    // the restart reconcile hands the key back to the older real version.
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("susp-mpu")
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket("susp-mpu")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("susp-mpu")
        .key("doc.txt")
        .body(ByteStream::from_static(b"v1"))
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket("susp-mpu")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("susp-mpu")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();
    let part_body = vec![b'm'; 5 * 1024 * 1024];
    let part = client
        .upload_part()
        .bucket("susp-mpu")
        .key("doc.txt")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(part_body.clone()))
        .send()
        .await
        .unwrap();
    client
        .complete_multipart_upload()
        .bucket("susp-mpu")
        .key("doc.txt")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    server.restart().await;
    let client = server.s3_client().await;

    let current = client
        .get_object()
        .bucket("susp-mpu")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    let bytes = current.body.collect().await.unwrap().into_bytes();
    assert_eq!(
        bytes.len(),
        part_body.len(),
        "the MPU result is still current after the restart"
    );
}

#[tokio::test]
async fn persistence_bucket_subresources_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("sub-bucket")
        .send()
        .await
        .unwrap();

    // Versioning (required for replication, object lock).
    client
        .put_bucket_versioning()
        .bucket("sub-bucket")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Tags
    client
        .put_bucket_tagging()
        .bucket("sub-bucket")
        .tagging(
            Tagging::builder()
                .tag_set(Tag::builder().key("env").value("prod").build().unwrap())
                .tag_set(Tag::builder().key("team").value("s3").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Lifecycle
    let _ = server
        .aws_cli(&[
            "s3api",
            "put-bucket-lifecycle-configuration",
            "--bucket",
            "sub-bucket",
            "--lifecycle-configuration",
            r#"{"Rules":[{"ID":"expire","Status":"Enabled","Expiration":{"Days":30},"Filter":{}}]}"#,
        ])
        .await;

    // CORS
    client
        .put_bucket_cors()
        .bucket("sub-bucket")
        .cors_configuration(
            CorsConfiguration::builder()
                .cors_rules(
                    CorsRule::builder()
                        .allowed_methods("GET")
                        .allowed_origins("*")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Policy
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::sub-bucket/*"}]}"#;
    client
        .put_bucket_policy()
        .bucket("sub-bucket")
        .policy(policy)
        .send()
        .await
        .unwrap();

    // Object Lock
    client
        .put_object_lock_configuration()
        .bucket("sub-bucket")
        .object_lock_configuration(
            ObjectLockConfiguration::builder()
                .object_lock_enabled(ObjectLockEnabled::Enabled)
                .build(),
        )
        .send()
        .await
        .ok();

    // Encryption
    client
        .put_bucket_encryption()
        .bucket("sub-bucket")
        .server_side_encryption_configuration(
            ServerSideEncryptionConfiguration::builder()
                .rules(
                    ServerSideEncryptionRule::builder()
                        .apply_server_side_encryption_by_default(
                            ServerSideEncryptionByDefault::builder()
                                .sse_algorithm(ServerSideEncryption::Aes256)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Inventory
    let _ = server
        .aws_cli(&[
            "s3api",
            "put-bucket-inventory-configuration",
            "--bucket",
            "sub-bucket",
            "--id",
            "inv-1",
            "--inventory-configuration",
            r#"{"Id":"inv-1","IsEnabled":true,"Destination":{"S3BucketDestination":{"Bucket":"arn:aws:s3:::dest","Format":"CSV"}},"IncludedObjectVersions":"Current","Schedule":{"Frequency":"Daily"}}"#,
        ])
        .await;

    server.restart().await;
    let client = server.s3_client().await;

    let tags = client
        .get_bucket_tagging()
        .bucket("sub-bucket")
        .send()
        .await
        .unwrap();
    let ts = tags.tag_set();
    assert!(ts.iter().any(|t| t.key() == "env" && t.value() == "prod"));
    assert!(ts.iter().any(|t| t.key() == "team" && t.value() == "s3"));

    let lc = client
        .get_bucket_lifecycle_configuration()
        .bucket("sub-bucket")
        .send()
        .await;
    assert!(lc.is_ok(), "lifecycle lost: {:?}", lc.err());

    let cors = client
        .get_bucket_cors()
        .bucket("sub-bucket")
        .send()
        .await
        .unwrap();
    assert!(!cors.cors_rules().is_empty());

    let pol = client
        .get_bucket_policy()
        .bucket("sub-bucket")
        .send()
        .await
        .unwrap();
    assert!(pol.policy().unwrap_or_default().contains("sub-bucket"));

    let enc = client
        .get_bucket_encryption()
        .bucket("sub-bucket")
        .send()
        .await
        .unwrap();
    let _ = enc.server_side_encryption_configuration();

    let inv = client
        .get_bucket_inventory_configuration()
        .bucket("sub-bucket")
        .id("inv-1")
        .send()
        .await;
    assert!(inv.is_ok(), "inventory lost: {:?}", inv.err());
}

#[tokio::test]
async fn persistence_unsupported_services_still_work_in_memory() {
    // Warning emission is covered by a unit test in `fakecloud-persistence`.
    // Here we only verify that a non-S3 service (SQS) keeps working when
    // the server is booted with --storage-mode=persistent.
    let tmp = tempfile::tempdir().unwrap();
    let server = TestServer::start_persistent(tmp.path()).await;
    let sqs = server.sqs_client().await;

    let q = sqs
        .create_queue()
        .queue_name("test-queue")
        .send()
        .await
        .unwrap();
    let url = q.queue_url().unwrap().to_string();

    sqs.send_message()
        .queue_url(&url)
        .message_body("hello")
        .send()
        .await
        .unwrap();

    let recv = sqs
        .receive_message()
        .queue_url(&url)
        .wait_time_seconds(0)
        .send()
        .await
        .unwrap();
    assert_eq!(recv.messages().len(), 1);
    assert_eq!(recv.messages()[0].body(), Some("hello"));
}

#[tokio::test]
async fn persistence_version_file_mismatch_fails_loudly() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("fakecloud.version.toml"),
        "format_version = 99\nfakecloud_version = \"bogus\"\ncreated_at = \"2000-01-01T00:00:00Z\"\n",
    )
    .unwrap();

    let data_arg = tmp.path().display().to_string();
    let (status, stderr) = run_until_exit(
        &["--storage-mode", "persistent", "--data-path", &data_arg],
        &[("FAKECLOUD_CONTAINER_CLI", "false")],
        Duration::from_secs(10),
    );
    assert!(
        !status.success(),
        "expected non-zero exit, got {:?}",
        status
    );
    assert!(
        stderr.contains("fakecloud.version.toml")
            || stderr.contains("format_version")
            || stderr.contains("version file"),
        "stderr did not reference version file: {}",
        stderr
    );
}

#[tokio::test]
async fn persistence_body_cache_small_and_large_objects() {
    let tmp = tempfile::tempdir().unwrap();
    // 4 MiB cache => 2 MiB single-object cap.
    let mut server =
        TestServer::start_persistent_with_cache(tmp.path(), Some(4 * 1024 * 1024)).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("cache-bucket")
        .send()
        .await
        .unwrap();

    let small = pseudo_random_bytes(1, 1024 * 1024);
    let big = pseudo_random_bytes(2, 3 * 1024 * 1024);
    let tiny = pseudo_random_bytes(3, 512 * 1024);

    for (key, body) in [
        ("small.bin", &small),
        ("big.bin", &big),
        ("tiny.bin", &tiny),
    ] {
        client
            .put_object()
            .bucket("cache-bucket")
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .unwrap();
    }

    server.restart().await;
    let client = server.s3_client().await;

    for (key, expected) in [
        ("small.bin", &small),
        ("big.bin", &big),
        ("tiny.bin", &tiny),
    ] {
        let get = client
            .get_object()
            .bucket("cache-bucket")
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(sha256(&got), sha256(expected), "body mismatch for {}", key);
    }
}

#[tokio::test]
async fn persistence_create_bucket_tags_survive_restart() {
    // Tags supplied through CreateBucketConfiguration go to the same Tags
    // subresource PutBucketTagging writes, so they must reload after a restart.
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("create-tag-bucket")
        .create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .tags(Tag::builder().key("team").value("a").build().unwrap())
                .build(),
        )
        .send()
        .await
        .unwrap();

    server.restart().await;
    let client = server.s3_client().await;

    let tags = client
        .get_bucket_tagging()
        .bucket("create-tag-bucket")
        .send()
        .await
        .unwrap();
    let ts = tags.tag_set();
    assert!(
        ts.iter().any(|t| t.key() == "team" && t.value() == "a"),
        "create-time tags lost across restart: {ts:?}"
    );
}
