use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, StatusCode};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use fakecloud_persistence::BodySource;

use crate::persistence::{mpu_init_snapshot, object_meta_snapshot};
use crate::state::{MultipartUpload, S3Object, UploadPart};

use md5::{Digest, Md5};

use super::{
    canned_acl_grants, compute_md5, extract_user_metadata, no_such_bucket, no_such_key,
    no_such_upload, parse_complete_multipart_xml, parse_grant_headers, parse_url_encoded_tags,
    precondition_failed, resolve_object, s3_xml, xml_escape, S3Service,
};

/// Build the `CompleteMultipartUploadResult` XML response for an object that
/// already exists — used by the idempotent re-completion paths (an upload that
/// was already completed by a prior or concurrent request). `checksum_xml` is
/// the optional pre-rendered `<ChecksumX>…</ChecksumX>` element, or `""`.
fn completion_xml_response(bucket: &str, key: &str, etag: &str, checksum_xml: &str) -> AwsResponse {
    let location = format!(
        "https://{bucket_h}.s3.amazonaws.com/{key_h}",
        bucket_h = xml_escape(bucket),
        key_h = xml_escape(key),
    );
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Location>{location}</Location>\
         <Bucket>{}</Bucket>\
         <Key>{}</Key>\
         <ETag>&quot;{}&quot;</ETag>\
         {checksum_xml}\
         </CompleteMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag),
    );
    AwsResponse {
        status: StatusCode::OK,
        content_type: "application/xml".to_string(),
        body: body.into(),
        headers: HeaderMap::new(),
    }
}

impl S3Service {
    pub(super) fn create_multipart_upload(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let upload_id = uuid::Uuid::new_v4().to_string();
        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let metadata = extract_user_metadata(&req.headers);
        let storage_class = req
            .headers
            .get("x-amz-storage-class")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("STANDARD")
            .to_string();
        let sse_algorithm = req
            .headers
            .get("x-amz-server-side-encryption")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let sse_kms_key_id = req
            .headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let tagging = req
            .headers
            .get("x-amz-tagging")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let acl_header = req
            .headers
            .get("x-amz-acl")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let has_grant_headers = req
            .headers
            .keys()
            .any(|k| k.as_str().starts_with("x-amz-grant-"));

        if acl_header.is_some() && has_grant_headers {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Specifying both Canned ACLs and Header Grants is not allowed",
            ));
        }

        let checksum_algorithm = req
            .headers
            .get("x-amz-checksum-algorithm")
            .or_else(|| req.headers.get("x-amz-sdk-checksum-algorithm"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_uppercase());

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let acl_grants = if has_grant_headers {
            parse_grant_headers(&req.headers)
        } else {
            let acl = acl_header.as_deref().unwrap_or("private");
            canned_acl_grants(acl, &b.acl_owner_id)
        };

        let upload = MultipartUpload {
            upload_id: upload_id.clone(),
            key: key.to_string(),
            initiated: Utc::now(),
            parts: std::collections::BTreeMap::new(),
            metadata,
            content_type,
            storage_class,
            sse_algorithm: sse_algorithm.clone(),
            sse_kms_key_id: sse_kms_key_id.clone(),
            tagging,
            acl_grants,
            checksum_algorithm,
        };
        // Store-first: persist the MPU init before touching memory so a disk
        // write failure short-circuits the handler cleanly without leaving an
        // orphaned in-memory upload.
        let init_snapshot = mpu_init_snapshot(&upload);
        self.store
            .mpu_create(bucket, &upload_id, &init_snapshot)
            .map_err(super::persistence_error)?;
        b.multipart_uploads.insert(upload_id.clone(), upload);

        let mut headers = HeaderMap::new();
        if let Some(algo) = &sse_algorithm {
            if let Ok(val) = algo.parse() {
                headers.insert("x-amz-server-side-encryption", val);
            }
        }
        if let Some(kid) = &sse_kms_key_id {
            if let Ok(val) = kid.parse() {
                headers.insert("x-amz-server-side-encryption-aws-kms-key-id", val);
            }
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Bucket>{}</Bucket>\
             <Key>{}</Key>\
             <UploadId>{}</UploadId>\
             </InitiateMultipartUploadResult>",
            xml_escape(bucket),
            xml_escape(key),
            xml_escape(&upload_id),
        );
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: body.into(),
            headers,
        })
    }

    pub(super) async fn upload_part(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Validate part number
        if part_number < 1 {
            return Err(no_such_upload(upload_id));
        }
        if part_number > 10000 {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Part number must be an integer between 1 and 10000, inclusive",
                vec![
                    ("ArgumentName".to_string(), "partNumber".to_string()),
                    ("ArgumentValue".to_string(), part_number.to_string()),
                ],
            ));
        }
        let pn = part_number as u32;

        // Pre-validate the bucket/upload/key under a short read guard
        // so a bogus request doesn't cause us to spool the whole body
        // to disk before returning the error.
        {
            let accts = self.state.read();
            let __empty = crate::state::S3State::new(account_id, "us-east-1");
            let state = accts.get(account_id).unwrap_or(&__empty);
            let b = state
                .buckets
                .get(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            let upload = b
                .multipart_uploads
                .get(upload_id)
                .ok_or_else(|| no_such_upload(upload_id))?;
            if upload.key != key {
                return Err(no_such_upload(upload_id));
            }
        }

        // UploadPart is streaming-only: spool chunks to a tempfile
        // while computing MD5 + size in constant memory, then hand
        // the file to the store as `BodySource::File`.
        let stream = req.take_body_stream().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedRequestBody",
                "UploadPart requires a streaming request body",
            )
        })?;
        let spooled = fakecloud_core::service::spool_request_stream(
            stream,
            self.store.spool_dir().as_deref(),
        )
        .await?;
        let part_size = spooled.size;
        let etag = spooled.md5_hex.clone();
        let body_source = BodySource::File(spooled.path);

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let upload = b
            .multipart_uploads
            .get_mut(upload_id)
            .ok_or_else(|| no_such_upload(upload_id))?;
        if upload.key != key {
            return Err(no_such_upload(upload_id));
        }

        // Store-first: durably write the part body before mutating memory.
        // The store returns the BodyRef the runtime should keep — `Disk`
        // for disk-mode (so the part body never lands in RAM beyond the
        // hyper frame buffer), `Memory` for memory-mode.
        let body_ref = self
            .store
            .mpu_put_part(bucket, upload_id, pn, body_source, &etag)
            .map_err(super::persistence_error)?;
        let part = UploadPart {
            part_number: pn,
            body: body_ref,
            etag: etag.clone(),
            size: part_size,
            last_modified: Utc::now(),
        };
        upload.parts.insert(pn, part);

        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{etag}\"").parse().unwrap());
        if let Some(algo) = &upload.sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        }
        if let Some(kid) = &upload.sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers,
        })
    }

    pub(super) fn upload_part_copy(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<AwsResponse, AwsServiceError> {
        let copy_source = req
            .headers
            .get("x-amz-copy-source")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "x-amz-copy-source header is required",
                )
            })?;

        // Split on '?' BEFORE percent-decoding so keys containing literal '?' are preserved
        let raw_source = copy_source.strip_prefix('/').unwrap_or(copy_source);

        // Parse versionId from ?versionId=X
        let (raw_path, source_version_id) = if let Some(idx) = raw_source.find("?versionId=") {
            let vid = raw_source[idx + 11..].to_string();
            (&raw_source[..idx], Some(vid))
        } else {
            (raw_source, None)
        };
        let decoded_path = percent_encoding::percent_decode_str(raw_path)
            .decode_utf8_lossy()
            .to_string();

        let (src_bucket, src_key) = decoded_path.split_once('/').ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid copy source format",
            )
        })?;

        let copy_range = req
            .headers
            .get("x-amz-copy-source-range")
            .and_then(|v| v.to_str().ok());

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let src_body_ref = {
            let sb = state
                .buckets
                .get(src_bucket)
                .ok_or_else(|| no_such_bucket(src_bucket))?;

            let src_obj = if let Some(ref vid) = source_version_id {
                resolve_object(sb, src_key, Some(vid))?
            } else {
                sb.objects
                    .get(src_key)
                    .ok_or_else(|| no_such_key(src_key))?
            };
            src_obj.body.clone()
        };
        let src_bytes = state.read_body(&src_body_ref).map_err(super::io_to_aws)?;
        let src_data = if let Some(range_str) = copy_range {
            let range_part = range_str.strip_prefix("bytes=").unwrap_or(range_str);
            let invalid_range = || {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    format!(
                        "The x-amz-copy-source-range value \"{range_str}\" is not valid for \
                         the source object of size {}.",
                        src_bytes.len()
                    ),
                )
            };
            if let Some((start_str, end_str)) = range_part.split_once('-') {
                // A copy-source-range over an empty object has no valid offsets, and a
                // malformed/out-of-bounds range must be rejected rather than slicing past
                // the buffer (which would panic the request thread).
                let last = src_bytes.len().checked_sub(1).ok_or_else(invalid_range)?;
                let start: usize = start_str.trim().parse().map_err(|_| invalid_range())?;
                let end: usize = if end_str.trim().is_empty() {
                    last
                } else {
                    end_str.trim().parse().map_err(|_| invalid_range())?
                };
                if start > end || end > last {
                    return Err(invalid_range());
                }
                src_bytes.slice(start..end + 1)
            } else {
                src_bytes.clone()
            }
        } else {
            src_bytes.clone()
        };

        let data_len = src_data.len() as u64;
        let etag = compute_md5(&src_data);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let upload = b
            .multipart_uploads
            .get_mut(upload_id)
            .ok_or_else(|| no_such_upload(upload_id))?;
        if upload.key != key {
            return Err(no_such_upload(upload_id));
        }

        // Store-first: durably write before the in-memory part is visible.
        self.store
            .mpu_put_part(
                bucket,
                upload_id,
                part_number as u32,
                BodySource::Bytes(src_data.clone()),
                &etag,
            )
            .map_err(super::persistence_error)?;
        let part = UploadPart {
            part_number: part_number as u32,
            body: crate::state::memory_body(src_data),
            etag: etag.clone(),
            size: data_len,
            last_modified: Utc::now(),
        };
        upload.parts.insert(part_number as u32, part);

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <CopyPartResult>\
             <ETag>&quot;{etag}&quot;</ETag>\
             <LastModified>{}</LastModified>\
             </CopyPartResult>",
            Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(super) fn complete_multipart_upload(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let submitted_parts = parse_complete_multipart_xml(body_str);

        if submitted_parts.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
            ));
        }

        let if_none_match = req
            .headers
            .get("x-amz-if-none-match")
            .or_else(|| req.headers.get("if-none-match"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Capture the upload + the bucket facts we need under a brief read
        // lock, then release it so the (potentially multi-GB) part assembly +
        // disk persist below runs OFF-LOCK instead of holding the global S3
        // write lock and serializing every other S3 operation server-wide
        // while it runs (bug-audit 2026-05-28, 4.7).
        let (
            upload,
            already_has_object,
            region,
            notification_config,
            versioning_enabled,
            acl_owner_id,
        ) = {
            let accts = self.state.read();
            let empty = crate::state::S3State::new(account_id, "us-east-1");
            let state = accts.get(account_id).unwrap_or(&empty);
            let b = state
                .buckets
                .get(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            let upload = match b.multipart_uploads.get(upload_id) {
                Some(u) => u.clone(),
                None => {
                    // Upload already completed - return existing object if it
                    // exists. IfNoneMatch does NOT apply to re-completions.
                    if let Some(obj) = b.objects.get(key) {
                        return Ok(completion_xml_response(bucket, key, &obj.etag, ""));
                    }
                    return Err(no_such_upload(upload_id));
                }
            };
            (
                upload,
                b.objects.contains_key(key),
                state.region.clone(),
                b.notification_config.clone(),
                b.versioning.as_deref() == Some("Enabled"),
                b.acl_owner_id.clone(),
            )
        };

        if upload.key != key {
            return Err(no_such_upload(upload_id));
        }

        // IfNoneMatch: if "*" and object already exists, reject (only for real completions)
        if let Some(ref inm) = if_none_match {
            if inm == "*" && already_has_object {
                return Err(precondition_failed("If-None-Match"));
            }
        }

        // Validate parts are in ascending order
        for window in submitted_parts.windows(2) {
            if window[0].0 >= window[1].0 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidPartOrder",
                    "The list of parts was not in ascending order. The parts list must be specified in order by part number.",
                ));
            }
        }

        let sorted_parts = submitted_parts;

        // Validate all specified parts exist in the upload
        for (part_num, _) in &sorted_parts {
            if !upload.parts.contains_key(part_num) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidPart",
                    "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not have matched the part's entity tag.",
                ));
            }
        }

        // Validate minimum part size: all non-last parts must be >= 5MB
        const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB
        if sorted_parts.len() > 1 {
            for (i, (part_num, _)) in sorted_parts.iter().enumerate() {
                if i >= sorted_parts.len() - 1 {
                    break; // skip last part
                }
                if let Some(part) = upload.parts.get(part_num) {
                    if part.size < MIN_PART_SIZE as u64 {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "EntityTooSmall",
                            "Your proposed upload is smaller than the minimum allowed object size.",
                        ));
                    }
                }
            }
        }

        // Assemble the object from its parts OFF-LOCK. Bodies are read via the
        // state-free `read_body_uncached`, so no S3 lock is held during the
        // (potentially large) concatenation, hashing, and disk persist below.
        let mut combined_data = Vec::new();
        let mut md5_digests = Vec::new();
        let mut part_sizes = Vec::new();
        // Per-part raw additional-checksum digests, collected in
        // part-number order so we can build the COMPOSITE checksum
        // (hash-of-part-hashes) AWS returns for multipart objects.
        let mut part_checksum_digests: Vec<Vec<u8>> = Vec::new();

        for (part_num, submitted_etag) in &sorted_parts {
            let part = upload.parts.get(part_num).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidPart",
                    "One or more of the specified parts could not be found.",
                )
            })?;
            if submitted_etag != &part.etag {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidPart",
                    "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not have matched the part's entity tag.",
                ));
            }
            let part_bytes =
                crate::state::S3State::read_body_uncached(&part.body).map_err(super::io_to_aws)?;
            let part_md5 = Md5::digest(&part_bytes);
            // Fail closed if the bytes just read no longer hash to the part's
            // recorded etag. We snapshot (clone) the upload — including each
            // part's fixed disk path and etag — then read bodies off-lock. In
            // disk mode a concurrent UploadPart of the same part number
            // rewrites part-NNNNN.bin after the snapshot, so without this
            // check Complete would assemble the *new* bytes while validating
            // them against the *old* cloned etag, producing an object whose
            // content doesn't match the parts the client committed (bug-hunt
            // 2026-06-13, finding 4.1). Memory-mode parts are immutable, so
            // this never trips for them.
            if format!("{:x}", part_md5) != part.etag {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidPart",
                    "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not have matched the part's etag.",
                ));
            }
            if let Some(algo) = upload.checksum_algorithm.as_deref() {
                part_checksum_digests.push(super::compute_checksum_raw(algo, &part_bytes));
            }
            combined_data.extend_from_slice(&part_bytes);
            md5_digests.extend_from_slice(&part_md5);
            part_sizes.push((*part_num, part_bytes.len() as u64));
        }

        // Multipart ETag: MD5(concat(part_md5_digests))-N
        let combined_md5 = Md5::digest(&md5_digests);
        let etag = format!("{:x}-{}", combined_md5, sorted_parts.len());
        // Additional-checksum for a multipart object is COMPOSITE:
        // base64(HASH(concat(raw part digests)))-N - NOT a checksum over
        // the whole reassembled object.
        let checksum_value = upload
            .checksum_algorithm
            .as_deref()
            .and_then(|algo| super::compute_composite_checksum(algo, &part_checksum_digests));
        let data = Bytes::from(combined_data);
        let store_body = data.clone();

        let tags = if let Some(ref tagging) = upload.tagging {
            parse_url_encoded_tags(tagging).into_iter().collect()
        } else {
            std::collections::BTreeMap::new()
        };

        let version_id = if versioning_enabled {
            Some(uuid::Uuid::new_v4().to_string())
        } else {
            None
        };

        let mut obj = S3Object {
            key: key.to_string(),
            size: data.len() as u64,
            body: crate::state::memory_body(data),
            content_type: upload.content_type.clone(),
            etag: etag.clone(),
            last_modified: Utc::now(),
            metadata: upload.metadata.clone(),
            storage_class: upload.storage_class.clone(),
            tags,
            acl_grants: upload.acl_grants.clone(),
            acl_owner_id: Some(acl_owner_id),
            parts_count: Some(sorted_parts.len() as u32),
            part_sizes: Some(part_sizes),
            sse_algorithm: upload.sse_algorithm.clone(),
            sse_kms_key_id: upload.sse_kms_key_id.clone(),
            version_id: version_id.clone(),
            checksum_algorithm: upload.checksum_algorithm.clone(),
            checksum_value: checksum_value.clone(),
            ..Default::default()
        };

        // Commit under the write lock. The expensive work — reading,
        // assembling, and hashing every part — already ran off-lock above; only
        // the validation, the durable persist, and the in-memory insert happen
        // here. Crucially the persist runs *after* the upload + precondition
        // checks pass and within the same lock hold as the insert, so a rejected
        // completion never leaves a half-written object on disk while memory
        // says otherwise (Cubic, 4.7; the off-lock persist could diverge on a
        // concurrent complete). A concurrent Abort/Complete that removed the
        // upload during assembly is honored (idempotent re-completion) rather
        // than resurrected (bug-audit 2026-05-28, 4.3 class).
        let meta = object_meta_snapshot(&obj);
        {
            let mut accts = self.state.write();
            {
                let b = accts
                    .get_or_create(account_id)
                    .buckets
                    .get_mut(bucket)
                    .ok_or_else(|| no_such_bucket(bucket))?;
                if !b.multipart_uploads.contains_key(upload_id) {
                    if let Some(existing) = b.objects.get(key) {
                        return Ok(completion_xml_response(bucket, key, &existing.etag, ""));
                    }
                    return Err(no_such_upload(upload_id));
                }
                // The object may have been created concurrently after the
                // phase-1 snapshot, so revalidate `If-None-Match: *` here rather
                // than trusting the stale `already_has_object`. Leave the upload
                // intact for retry/abort; nothing has been persisted yet.
                if if_none_match.as_deref() == Some("*") && b.objects.contains_key(key) {
                    return Err(precondition_failed("If-None-Match"));
                }
            }
            // Checks passed — persist, then commit to memory, all under the lock.
            self.store
                .mpu_complete(bucket, upload_id, key, meta.version_id.as_deref(), &meta)
                .map_err(super::persistence_error)?;
            let returned_body = self
                .store
                .put_object(
                    bucket,
                    key,
                    meta.version_id.as_deref(),
                    BodySource::Bytes(store_body.clone()),
                    &meta,
                )
                .map_err(super::persistence_error)?;
            obj.body = returned_body.clone();
            let b = accts
                .get_or_create(account_id)
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            b.objects.insert(key.to_string(), obj);
            b.multipart_uploads.remove(upload_id);
            if versioning_enabled {
                if let Some(versions) = b.object_versions.get_mut(key) {
                    if let Some(last) = versions.last_mut() {
                        last.body = returned_body;
                    }
                }
            }
        }

        let mut headers = HeaderMap::new();
        if let Some(vid) = &version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        if let Some(algo) = &upload.sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        }
        if let Some(kid) = &upload.sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }

        let location = format!(
            "https://{bucket_h}.s3.amazonaws.com/{key_h}",
            bucket_h = xml_escape(bucket),
            key_h = xml_escape(key),
        );
        // Surface the per-algorithm checksum element AWS emits so
        // SDK clients that round-trip Complete -> Get can verify
        // integrity end-to-end.
        let checksum_xml = match (upload.checksum_algorithm.as_deref(), &checksum_value) {
            (Some(algo), Some(value)) => format!(
                "<Checksum{algo}>{val}</Checksum{algo}>",
                algo = algo,
                val = xml_escape(value),
            ),
            _ => String::new(),
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Location>{location}</Location>\
             <Bucket>{}</Bucket>\
             <Key>{}</Key>\
             <ETag>&quot;{}&quot;</ETag>\
             {checksum_xml}\
             </CompleteMultipartUploadResult>",
            xml_escape(bucket),
            xml_escape(key),
            xml_escape(&etag),
        );

        // The write lock was already released after committing the object, so
        // event delivery (which may re-enter the S3 service to read state)
        // cannot deadlock against an outstanding write.
        let bucket_name = bucket.to_string();
        let obj_key = key.to_string();
        let obj_size = store_body.len() as u64;
        let obj_etag = etag.clone();
        if let Some(ref config) = notification_config {
            super::deliver_notifications(
                &self.delivery,
                config,
                &super::notifications::ObjectEvent {
                    event_name: "ObjectCreated:CompleteMultipartUpload",
                    bucket_name: &bucket_name,
                    key: &obj_key,
                    size: obj_size,
                    etag: &obj_etag,
                    region: &region,
                },
                Some(&self.state),
            );
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: body.into(),
            headers,
        })
    }

    pub(super) fn abort_multipart_upload(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        // AbortMultipartUpload only declares NoSuchUpload per the Smithy
        // model; collapse missing-bucket into NoSuchUpload for strict
        // conformance (no bucket -> no upload to abort).
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_upload(upload_id))?;

        // Validate upload exists and belongs to the requested key
        match b.multipart_uploads.get(upload_id) {
            Some(upload) if upload.key != key => {
                return Err(no_such_upload(upload_id));
            }
            None => {
                return Err(no_such_upload(upload_id));
            }
            _ => {}
        }
        // Store-first for consistency with other MPU ops.
        self.store
            .mpu_abort(bucket, upload_id)
            .map_err(super::persistence_error)?;
        b.multipart_uploads.remove(upload_id);

        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: HeaderMap::new(),
        })
    }

    pub(super) fn list_multipart_uploads(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let mut uploads_xml = String::new();
        let mut sorted_uploads: Vec<_> = b.multipart_uploads.values().collect();
        sorted_uploads.sort_by_key(|u| &u.key);
        for upload in &sorted_uploads {
            uploads_xml.push_str(&format!(
                "<Upload>\
                 <Key>{}</Key>\
                 <UploadId>{}</UploadId>\
                 <Initiated>{}</Initiated>\
                 <StorageClass>{}</StorageClass>\
                 </Upload>",
                xml_escape(&upload.key),
                xml_escape(&upload.upload_id),
                upload.initiated.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                xml_escape(&upload.storage_class),
            ));
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Bucket>{}</Bucket>\
             <MaxUploads>1000</MaxUploads>\
             <IsTruncated>false</IsTruncated>\
             {uploads_xml}\
             </ListMultipartUploadsResult>",
            xml_escape(bucket),
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(super) fn list_parts(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max_parts: i64 = match req.query_params.get("max-parts") {
            Some(v) => v.parse().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "Provided max-parts not an integer or within integer range",
                )
            })?,
            None => 1000,
        };
        let part_number_marker: i64 = match req.query_params.get("part-number-marker") {
            Some(v) => v.parse().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "Provided part-number-marker not an integer or within integer range",
                )
            })?,
            None => 0,
        };

        // Validate max-parts and part-number-marker
        if max_parts < 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Argument max-parts must be an integer between 0 and 2147483647",
            ));
        }
        if max_parts > 2147483647 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Provided max-parts not an integer or within integer range",
            ));
        }
        if part_number_marker < 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Argument part-number-marker must be an integer between 0 and 2147483647",
            ));
        }
        if part_number_marker > 2147483647 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Provided part-number-marker not an integer or within integer range",
            ));
        }

        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let upload = b
            .multipart_uploads
            .get(upload_id)
            .ok_or_else(|| no_such_upload(upload_id))?;
        if upload.key != key {
            return Err(no_such_upload(upload_id));
        }

        // Filter parts after marker and apply limit
        let all_parts: Vec<_> = upload
            .parts
            .values()
            .filter(|p| p.part_number as i64 > part_number_marker)
            .collect();
        let max = max_parts as usize;
        let is_truncated = all_parts.len() > max;
        let display_parts: Vec<_> = all_parts.into_iter().take(max).collect();

        let mut parts_xml = String::new();
        let mut next_marker: i64 = 0;
        for part in &display_parts {
            next_marker = part.part_number as i64;
            parts_xml.push_str(&format!(
                "<Part>\
                 <PartNumber>{}</PartNumber>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <LastModified>{}</LastModified>\
                 </Part>",
                part.part_number,
                xml_escape(&part.etag),
                part.size,
                part.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            ));
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Bucket>{}</Bucket>\
             <Key>{}</Key>\
             <UploadId>{}</UploadId>\
             <PartNumberMarker>{part_number_marker}</PartNumberMarker>\
             <NextPartNumberMarker>{next_marker}</NextPartNumberMarker>\
             <MaxParts>{max_parts}</MaxParts>\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {parts_xml}\
             </ListPartsResult>",
            xml_escape(bucket),
            xml_escape(key),
            xml_escape(upload_id),
        );
        Ok(s3_xml(StatusCode::OK, body))
    }
}
