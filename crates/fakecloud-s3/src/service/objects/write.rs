//! `S3Service` `write` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    pub(crate) async fn put_object(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Validate key length
        if key.len() > 1024 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "KeyTooLongError",
                "Your key is too long",
            ));
        }

        // Check for If-None-Match conditional on PUT
        let if_none_match = req
            .headers
            .get("if-none-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Check for If-Match conditional on PUT
        let if_match = req
            .headers
            .get("if-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Check for x-amz-tagging header
        let tagging_header = req
            .headers
            .get("x-amz-tagging")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Resolved here, before `take_body_stream` spools the payload to disk:
        // returning after the spool leaks the file, since nothing unlinks it on
        // the error paths.
        let write_acl = self.resolve_write_acl_headers(account_id, bucket, &req.headers)?;

        // Parse tags from header
        let tags = if let Some(tagging) = &tagging_header {
            let parsed = parse_url_encoded_tags(tagging);
            // Validate aws: prefix
            for (k, _) in &parsed {
                if k.starts_with("aws:") {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidTag",
                        "Your TagKey cannot be prefixed with aws:",
                    ));
                }
            }
            parsed.into_iter().collect()
        } else {
            std::collections::BTreeMap::new()
        };

        // --- Read lock phase: validate preconditions and read bucket config ---
        let (acl_owner_id, encryption_config, bucket_region, explicit_lock, checks) = {
            let accts = self.state.read();
            let __empty = crate::state::S3State::new(account_id, "us-east-1");
            let state = accts.get(account_id).unwrap_or(&__empty);
            let b = state
                .buckets
                .get(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;

            // Handle If-Match: check existing object etag
            if let Some(ref if_match_val) = if_match {
                match b.objects.get(key).filter(|o| !o.is_delete_marker) {
                    Some(existing) => {
                        let existing_etag = format!("\"{}\"", existing.etag);
                        if !etag_matches(if_match_val, &existing_etag) {
                            return Err(precondition_failed("If-Match"));
                        }
                    }
                    None => {
                        return Err(no_such_key(key));
                    }
                }
            }

            // Handle If-None-Match: if "*", fail if object already exists
            if let Some(ref inm) = if_none_match {
                if inm.trim() == "*" && b.objects.get(key).is_some_and(|o| !o.is_delete_marker) {
                    return Err(precondition_failed("If-None-Match"));
                }
            }

            // Integrity headers are validated once the bucket and the
            // conditional headers have been, so a request to a missing bucket
            // answers NoSuchBucket and a failed precondition 412, never a
            // digest error.
            let checks = crate::checksum::ChecksumHeaders::parse(&req.headers)?;

            // Object lock: explicit lock headers need a bucket with object lock.
            let explicit_lock =
                ObjectLockSettings::from_request(&req.headers, b.object_lock_config.as_deref())?;
            // A PutObject that sets an object lock must carry a payload
            // integrity check (CopyObject and CreateMultipartUpload need none):
            // Content-MD5, an x-amz-checksum-* value, a checksum trailer, or
            // the SDK's x-amz-sdk-checksum-algorithm.
            if has_object_lock_headers(&req.headers) && !checks.has_integrity_check() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object \
                     requests with Object Lock parameters",
                ));
            }

            (
                b.acl_owner_id.clone(),
                b.encryption_config.clone(),
                b.region.clone(),
                explicit_lock,
                checks,
            )
        }; // read lock dropped

        // The object keeps the algorithm the request's checksum is in; an
        // upload naming none gets S3's default CRC64NVME full-object checksum.
        let checksum_algorithm = checks
            .algorithm()
            .unwrap_or(crate::checksum::DEFAULT_ALGORITHM);

        // Checked before the body is spooled: nothing unlinks the spool file
        // on an error returned after it.
        let storage_class = req
            .headers
            .get("x-amz-storage-class")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("STANDARD")
            .to_string();
        if !is_valid_storage_class(&storage_class) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidStorageClass",
                "The storage class you specified is not valid",
            ));
        }

        // --- Preparation phase: compute object data outside any lock ---
        // PutObject is a streaming-only route: dispatch always wires
        // `body_stream` for PUT-on-bucket-key requests routed through
        // S3, and the test helper does the same for direct service
        // calls. We drain the stream into a tempfile here, computing
        // MD5 + size in constant memory; in disk mode the spool lands
        // on the S3 root so the eventual rename is a same-FS metadata
        // move, in memory mode the store reads the file back and
        // unlinks it.
        let stream = req.take_body_stream().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedRequestBody",
                "PutObject requires a streaming request body",
            )
        })?;
        // Spool the body, folding the checksum digest into the same pass, and
        // verify Content-MD5, the x-amz-checksum-* value or trailing checksum,
        // and a hex x-amz-content-sha256 against the bytes received. A
        // mismatch is BadDigest / XAmzContentSHA256Mismatch, as on S3.
        let crate::checksum::VerifiedSpool { spooled, digests } = crate::checksum::spool_verified(
            &req.headers,
            stream,
            self.store.spool_dir().as_deref(),
            &checks,
            Some(checksum_algorithm),
        )
        .await?;
        let checksum_value = digests.encoded(checksum_algorithm);
        let data_size: u64 = spooled.size;
        let etag: String = spooled.md5_hex.clone();

        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("binary/octet-stream")
            .to_string();
        let content_encoding = fakecloud_core::service::strip_aws_chunked_encoding(
            req.headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok()),
        );
        let website_redirect_location = req
            .headers
            .get("x-amz-website-redirect-location")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let header_str = |name: &str| {
            req.headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        };
        let cache_control = header_str("cache-control");
        let content_disposition = header_str("content-disposition");
        let content_language = header_str("content-language");
        let expires = header_str("expires");

        let metadata = extract_user_metadata(&req.headers);

        // Build ACL grants for object
        let acl_grants = if let Some(grants) = write_acl.grants_for(&acl_owner_id) {
            grants
        } else {
            // Default: owner full control
            vec![AclGrant {
                grantee_type: "CanonicalUser".to_string(),
                grantee_id: Some(acl_owner_id.clone()),
                grantee_display_name: Some(acl_owner_id.clone()),
                grantee_uri: None,
                permission: "FULL_CONTROL".to_string(),
            }]
        };

        // The request's SSE headers, else the bucket's default encryption.
        let crate::side_effects::ObjectEncryption {
            algorithm: sse_algorithm,
            kms_key_id: sse_kms_key_id,
            bucket_key_enabled,
        } = self.object_encryption(
            &req.headers,
            encryption_config.as_deref(),
            account_id,
            &bucket_region,
        );

        // SSE-KMS: read the spool back into memory so the KMS hook can
        // produce an envelope-encrypted blob (AES-GCM needs the whole
        // plaintext at once — chunked AEAD isn't part of the AWS S3
        // wire format). Fail-closed: a KMS error aborts PutObject
        // before any mutation. Non-KMS uploads pass `BodySource::File`
        // straight to the store and never copy the payload into RAM.
        let plaintext_size = data_size;
        let body_source: BodySource = if crate::side_effects::is_sse_kms(sse_algorithm.as_deref()) {
            let bytes = tokio::fs::read(&spooled.path)
                .await
                .map_err(crate::service::io_to_aws)?;
            let _ = tokio::fs::remove_file(&spooled.path).await;
            let cipher = self.encrypt_object_body(
                req,
                account_id,
                &bucket_region,
                bucket,
                &Bytes::from(bytes),
                sse_kms_key_id.as_deref(),
            )?;
            BodySource::Bytes(cipher)
        } else {
            BodySource::File(spooled.path)
        };
        let obj = S3Object {
            key: key.to_string(),
            size: plaintext_size,
            // The runtime body is filled in at commit with the BodyRef
            // the store returns (`commit_object_version`) — for memory mode that
            // is a `Memory` variant, for persistent mode a `Disk`
            // variant pointing at the freshly written .bin file.
            // Constructing with an empty placeholder means streamed
            // uploads never hold the payload in RAM at this point.
            body: BodyRef::default(),
            content_type,
            etag: etag.clone(),
            last_modified: Utc::now(),
            metadata,
            storage_class,
            tags,
            acl_grants,
            acl_owner_id: Some(acl_owner_id),
            sse_algorithm: sse_algorithm.clone(),
            sse_kms_key_id: sse_kms_key_id.clone(),
            bucket_key_enabled,
            // Assigned at commit from the bucket's versioning state.
            version_id: None,
            content_encoding,
            cache_control,
            content_disposition,
            content_language,
            expires,
            website_redirect_location,
            checksum_algorithm: Some(checksum_algorithm),
            checksum_value: checksum_value.clone(),
            checksum_type: Some(crate::checksum::ChecksumType::FullObject),
            // The explicit lock headers; the bucket's default retention is
            // applied at commit when none was asked for.
            lock_mode: explicit_lock.mode,
            lock_retain_until: explicit_lock.retain_until,
            lock_legal_hold: explicit_lock.legal_hold,
            ..Default::default()
        };

        // --- Write lock phase: insert object and handle versioning/replication ---
        let committed = {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;

            // Recheck conditional headers under write lock to prevent TOCTOU race
            if let Some(ref if_match_val) = if_match {
                match b.objects.get(key).filter(|o| !o.is_delete_marker) {
                    Some(existing) => {
                        let existing_etag = format!("\"{}\"", existing.etag);
                        if !etag_matches(if_match_val, &existing_etag) {
                            return Err(precondition_failed("If-Match"));
                        }
                    }
                    None => {
                        return Err(no_such_key(key));
                    }
                }
            }
            if let Some(ref inm) = if_none_match {
                if inm.trim() == "*" && b.objects.get(key).is_some_and(|o| !o.is_delete_marker) {
                    return Err(precondition_failed("If-None-Match"));
                }
            }

            // Write the body through the store, record the new version as the
            // bucket's versioning state dictates, and replicate it.
            crate::side_effects::commit_new_object(
                &mut accts,
                account_id,
                bucket,
                Some(&*self.store),
                Some(&self.side_effects),
                obj,
                |store, obj, meta| {
                    store.put_object(
                        bucket,
                        &obj.key,
                        meta.version_id.as_deref(),
                        body_source,
                        meta,
                    )
                },
            )
            .map_err(crate::service::persistence_error)?
        }; // write lock dropped
        let version_id = committed.version_id.clone();

        // --- Response phase: build response headers ---
        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{etag}\"").parse().unwrap());
        if let Some(vid) = &version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        // Return SSE headers
        crate::side_effects::ObjectEncryption {
            algorithm: sse_algorithm.clone(),
            kms_key_id: sse_kms_key_id.clone(),
            bucket_key_enabled,
        }
        .insert_response_headers(&mut headers);
        // The stored checksum, which S3 returns on every PutObject: a single
        // part upload's checksum is always FULL_OBJECT.
        if let Some(val) = &checksum_value {
            crate::checksum::insert_response_headers(
                &mut headers,
                checksum_algorithm,
                val,
                Some(crate::checksum::ChecksumType::FullObject),
            );
        }

        // Deliver S3 event notifications. A browser form upload reaches this
        // handler through POST Object, which AWS reports as ObjectCreated:Post.
        // S3 routes on method + path rather than `action`, so POST Object's
        // synthesized PutObject request is the only caller that sets it.
        let event_name = if req.action == "PostObject" {
            "ObjectCreated:Post"
        } else {
            "ObjectCreated:Put"
        };
        committed.notify(
            &self.delivery,
            &self.state,
            event_name,
            &req.account_id,
            bucket,
            key,
            data_size,
            &etag,
        );

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: String::new(),
            body: Bytes::new().into(),
            headers,
        })
    }

    pub(crate) fn copy_object(
        &self,
        account_id: &str,
        req: &AwsRequest,
        dest_bucket: &str,
        dest_key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Validate destination key length, matching PutObject (AWS caps keys at
        // 1024 bytes and rejects a longer one with KeyTooLongError).
        if dest_key.len() > 1024 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "KeyTooLongError",
                "Your key is too long",
            ));
        }

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

        // Parsed by the same function the source authorization uses, so the
        // object copied is exactly the object authorized.
        let source = crate::service::parse_copy_source(copy_source).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid copy source format",
            )
        })?;
        let src_version_id = source.version_id.clone();
        let (src_bucket, src_key) = (source.bucket.as_str(), source.key.as_str());

        let metadata_directive = req
            .headers
            .get("x-amz-metadata-directive")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("COPY");

        let storage_class = req
            .headers
            .get("x-amz-storage-class")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Validate storage class if explicitly provided
        if let Some(ref sc) = storage_class {
            if !is_valid_storage_class(sc) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidStorageClass",
                    "The storage class you specified is not valid",
                ));
            }
        }

        let tagging_directive = req
            .headers
            .get("x-amz-tagging-directive")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("COPY");

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

        let bucket_key_enabled = req
            .headers
            .get("x-amz-server-side-encryption-bucket-key-enabled")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("true"));

        let website_redirect = req
            .headers
            .get("x-amz-website-redirect-location")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let if_match = req
            .headers
            .get("x-amz-copy-source-if-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let if_none_match = req
            .headers
            .get("x-amz-copy-source-if-none-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // CopyObject accepts the same ACL headers as PutObject: a canned
        // `x-amz-acl` or the `x-amz-grant-*` pair, never both. Ignoring them --
        // which is what this path used to do -- answered 200 for
        // `copy-object --acl public-read` and produced a private object, so the
        // caller believed it had published the copy.
        // Resolved before the write lock, since the check reads bucket state of
        // its own, and before the copy does any work.
        let copy_acl = self.resolve_write_acl_headers(account_id, dest_bucket, &req.headers)?;

        // Phase 1 (read guard): resolve the source, evaluate preconditions and
        // the destination's attributes, and open the source body. The source
        // body itself (up to 5 GB) is read after the guard drops, so a large
        // copy never holds the global S3 lock across its IO.
        let accts_r = self.state.read();
        let empty_state = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts_r.get(account_id).unwrap_or(&empty_state);
        // The source bucket is read in the account that owns it, which is not
        // the destination's when copying out of another account's bucket
        // (authorized by s3:GetObject on the source at dispatch).
        let src_account = accts_r
            .find_account(|s| s.buckets.contains_key(src_bucket))
            .map_or_else(|| account_id.to_string(), |a| a.to_string());
        let src_state = accts_r.get(&src_account).unwrap_or(state);

        // Resolve source object, possibly a specific version
        let (src_obj, src_version_id_actual) = {
            let sb = src_state
                .buckets
                .get(src_bucket)
                .ok_or_else(|| no_such_bucket(src_bucket))?;
            let obj = resolve_object(sb, src_key, src_version_id.as_ref())?.clone();
            (obj.clone(), obj.version_id.clone())
        };

        // Delete markers cannot be used as copy source
        if src_obj.is_delete_marker {
            return Err(no_such_key(src_key));
        }

        // Glacier/Deep Archive: cannot copy unless restored
        if is_frozen(&src_obj) {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "ObjectNotInActiveTierError",
                "The source object of the COPY action is not in the active tier and is at the \
                 storage class type that does not support the COPY action.",
            ));
        }

        // x-amz-copy-source-if-match: fail with 412 when the source ETag does
        // NOT match (bug-audit — this header was never read, so a mismatched
        // source still copied + 200'd).
        if let Some(ref im) = if_match {
            let src_etag = format!("\"{}\"", src_obj.etag);
            if !etag_matches(im, &src_etag) {
                return Err(AwsServiceError::aws_error_with_fields(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold",
                    vec![(
                        "Condition".to_string(),
                        "x-amz-copy-source-If-Match".to_string(),
                    )],
                ));
            }
        }
        if let Some(ref inm) = if_none_match {
            let src_etag = format!("\"{}\"", src_obj.etag);
            if etag_matches(inm, &src_etag) {
                return Err(AwsServiceError::aws_error_with_fields(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold",
                    vec![(
                        "Condition".to_string(),
                        "x-amz-copy-source-If-None-Match".to_string(),
                    )],
                ));
            }
        }
        // Date-based copy-source preconditions (bug-audit 2026-05-28,
        // 1.10 — these were ignored, so a copy always succeeded). A
        // malformed date header is ignored, matching AWS.
        if let Some(since) = req
            .headers
            .get("x-amz-copy-source-if-unmodified-since")
            .and_then(|v| v.to_str().ok())
            .and_then(crate::service::parse_http_date)
        {
            // Fail if the source WAS modified after the given instant.
            if src_obj.last_modified > since {
                return Err(AwsServiceError::aws_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold",
                ));
            }
        }
        if let Some(since) = req
            .headers
            .get("x-amz-copy-source-if-modified-since")
            .and_then(|v| v.to_str().ok())
            .and_then(crate::service::parse_http_date)
        {
            // Fail if the source was NOT modified after the given instant.
            if src_obj.last_modified <= since {
                return Err(AwsServiceError::aws_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold",
                ));
            }
        }

        // Check copy-in-place validity
        let has_version_id = src_version_id.is_some();
        if src_bucket == dest_bucket
            && src_key == dest_key
            && metadata_directive == "COPY"
            && storage_class.is_none()
            && sse_algorithm.is_none()
            && website_redirect.is_none()
            && !has_version_id
        {
            // Check if bucket encryption would make this a valid copy-in-place
            let sb = state
                .buckets
                .get(src_bucket)
                .ok_or_else(|| no_such_bucket(src_bucket))?;
            let has_bucket_encryption = sb.encryption_config.is_some();
            if !has_bucket_encryption {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    "This copy request is illegal because it is trying to copy an object to itself \
                     without changing the object's metadata, storage class, website redirect location \
                     or encryption attributes.",
                ));
            }
        }

        // Object Lock for the destination: the request's lock headers,
        // validated as PutObject validates them (and refused on a destination
        // without object lock). The destination's default retention is applied
        // at commit. The source object's lock is never carried over -- a copy
        // is a new object version. A missing destination bucket is reported as
        // NoSuchBucket at commit.
        let explicit_lock = match state.buckets.get(dest_bucket) {
            Some(db) => {
                ObjectLockSettings::from_request(&req.headers, db.object_lock_config.as_deref())?
            }
            None => ObjectLockSettings::default(),
        };

        let etag = src_obj.etag.clone();
        let src_obj_size = src_obj.size;
        let last_modified = Utc::now();

        let new_metadata = if metadata_directive == "REPLACE" {
            extract_user_metadata(&req.headers)
        } else {
            src_obj.metadata.clone()
        };

        let new_content_type = if metadata_directive == "REPLACE" {
            req.headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(&src_obj.content_type)
                .to_string()
        } else {
            src_obj.content_type.clone()
        };

        let new_storage_class = storage_class.unwrap_or_else(|| "STANDARD".to_string());

        // System metadata follows the metadata-directive: REPLACE takes the
        // request headers, COPY inherits from the source.
        let (new_cache_control, new_content_disposition, new_content_language, new_expires) =
            if metadata_directive == "REPLACE" {
                let h = |name: &str| {
                    req.headers
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string())
                };
                (
                    h("cache-control"),
                    h("content-disposition"),
                    h("content-language"),
                    h("expires"),
                )
            } else {
                (
                    src_obj.cache_control.clone(),
                    src_obj.content_disposition.clone(),
                    src_obj.content_language.clone(),
                    src_obj.expires.clone(),
                )
            };

        let new_tags = if tagging_directive == "REPLACE" {
            let th = req
                .headers
                .get("x-amz-tagging")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let tags = parse_url_encoded_tags(th);
            // Validate aws: prefix
            for (k, _) in &tags {
                if k.starts_with("aws:") {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidTag",
                        "Your TagKey cannot be prefixed with aws:",
                    ));
                }
            }
            tags.into_iter().collect()
        } else {
            src_obj.tags.clone()
        };

        // Determine bucket default encryption
        let dest_bucket_encryption = state
            .buckets
            .get(dest_bucket)
            .and_then(|b| b.encryption_config.as_ref())
            .and_then(|config| {
                if config.contains("AES256") {
                    Some("AES256".to_string())
                } else if config.contains("aws:kms") {
                    Some("aws:kms".to_string())
                } else {
                    None
                }
            });

        // For SSE: if explicitly set, use new values; if copy-in-place changed SSE, use new;
        // otherwise fall back based on source or bucket default
        let new_sse = if sse_algorithm.is_some() {
            sse_algorithm
        } else if src_bucket == dest_bucket && src_key == dest_key {
            // Copy-in-place without SSE specified: if source had non-AES256 SSE, default to AES256
            if src_obj.sse_algorithm.is_some() && src_obj.sse_algorithm.as_deref() != Some("AES256")
            {
                Some("AES256".to_string())
            } else if src_obj.sse_algorithm.is_some() {
                src_obj.sse_algorithm.clone()
            } else {
                // Use bucket default encryption if available
                dest_bucket_encryption.clone()
            }
        } else {
            // For cross-key copy, use bucket default encryption if no explicit SSE
            dest_bucket_encryption.clone()
        };

        let new_kms = if sse_kms_key_id.is_some() {
            sse_kms_key_id
        } else {
            None
        };
        let new_bke = bucket_key_enabled; // Only set if explicitly provided
        let new_redirect = website_redirect.or_else(|| {
            if metadata_directive == "COPY" {
                src_obj.website_redirect_location.clone()
            } else {
                None
            }
        });

        let dest_region = state
            .buckets
            .get(dest_bucket)
            .map_or_else(|| state.region.clone(), |b| b.region.clone());
        // The requested checksum algorithm is validated once the source and
        // destination buckets are known to exist, so a copy into a missing
        // bucket answers NoSuchBucket rather than a header error.
        if !state.buckets.contains_key(dest_bucket) {
            return Err(no_such_bucket(dest_bucket));
        }
        let checksum_algorithm =
            crate::checksum::parse_algorithm_header(&req.headers, "x-amz-checksum-algorithm")?;
        let src_handle = crate::body_io::ObjectBodyHandle::open(&src_obj.body)
            .map_err(crate::service::io_to_aws)?;
        drop(accts_r);

        // Phase 2 (no lock): read, decrypt, checksum, re-encrypt, and stage
        // the destination body next to the store.
        //
        // Checksum: compute new if algorithm specified, or copy from source.
        // For SSE-KMS sources we work on plaintext so the checksum + the
        // destination's stored body line up with what the caller will get
        // back from a future GetObject. Fail-closed if the source can't
        // be decrypted (revoked key, malformed envelope): copying the
        // ciphertext over to the destination would silently corrupt the
        // copy.
        let raw_src_bytes =
            super::run_blocking_io(|| src_handle.read_all()).map_err(crate::service::io_to_aws)?;
        let src_bytes = if crate::side_effects::is_sse_kms(src_obj.sse_algorithm.as_deref())
            && self.kms_hook.is_some()
        {
            self.decrypt_object_body(
                req,
                &src_account,
                src_bucket,
                &raw_src_bytes,
                src_obj.sse_kms_key_id.as_deref(),
            )?
        } else {
            raw_src_bytes
        };
        // The copy is written as a single object, so its checksum is always a
        // FULL_OBJECT checksum of the whole body (S3 user guide: "With a copy
        // command, the checksum of the object is a direct checksum of the full
        // object"): in the requested algorithm, else the source's, else the
        // CRC64NVME default. A source FULL_OBJECT checksum in that algorithm is
        // already that value; a COMPOSITE (multipart) one is not, so the body
        // is hashed.
        let new_checksum_algo = checksum_algorithm
            .or(src_obj.checksum_algorithm)
            .unwrap_or(crate::checksum::DEFAULT_ALGORITHM);
        let reusable = (src_obj.checksum_algorithm == Some(new_checksum_algo)
            && src_obj.effective_checksum_type()
                == Some(crate::checksum::ChecksumType::FullObject))
        .then(|| src_obj.checksum_value.clone())
        .flatten();
        let new_checksum_val =
            reusable.unwrap_or_else(|| crate::checksum::checksum(new_checksum_algo, &src_bytes));
        // Re-encrypt for the destination if it lands as SSE-KMS, so the
        // stored body is a fresh envelope with the dest's encryption
        // context (bucket arn), under a key in the destination bucket's
        // region.
        let dest_stored_bytes =
            if crate::side_effects::is_sse_kms(new_sse.as_deref()) && self.kms_hook.is_some() {
                self.encrypt_object_body(
                    req,
                    account_id,
                    &dest_region,
                    dest_bucket,
                    &src_bytes,
                    new_kms.as_deref(),
                )?
            } else {
                src_bytes.clone()
            };
        let mut staged_dest = super::run_blocking_io(|| {
            crate::body_io::SpooledBody::spool(Some(&*self.store), dest_stored_bytes.clone())
        })
        .map_err(crate::service::io_to_aws)?;

        // Phase 3 (write guard): commit the destination object. The store
        // write is a rename of the staged file, not a body copy.
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);

        let db = state
            .buckets
            .get_mut(dest_bucket)
            .ok_or_else(|| no_such_bucket(dest_bucket))?;

        // The destination's ACL comes from this request, never from the source:
        // S3 treats a copy as a new object, so an unspecified ACL is the default
        // private one rather than whatever the source carried.
        let dest_acl_grants = copy_acl.grants_for(&db.acl_owner_id).unwrap_or_else(|| {
            vec![AclGrant {
                grantee_type: "CanonicalUser".to_string(),
                grantee_id: Some(db.acl_owner_id.clone()),
                grantee_display_name: Some(db.acl_owner_id.clone()),
                grantee_uri: None,
                permission: "FULL_CONTROL".to_string(),
            }]
        });

        let dest_obj = S3Object {
            key: dest_key.to_string(),
            // Copy is an independent object — never inherit the source's
            // disk-backed BodyRef (which would point at the source's file).
            // The source bytes are read above; the destination's own on-disk
            // file is written at commit by `commit_object_version`.
            body: crate::state::memory_body(dest_stored_bytes.clone()),
            // size tracks plaintext bytes (matches what a future GetObject
            // returns to the caller); see the put_object SSE-KMS path.
            size: src_bytes.len() as u64,
            etag: etag.clone(),
            last_modified,
            content_type: new_content_type,
            metadata: new_metadata,
            storage_class: new_storage_class,
            tags: new_tags,
            acl_grants: dest_acl_grants,
            acl_owner_id: Some(db.acl_owner_id.clone()),
            parts_count: src_obj.parts_count,
            part_sizes: src_obj.part_sizes,
            sse_algorithm: new_sse.clone(),
            sse_kms_key_id: new_kms.clone(),
            bucket_key_enabled: new_bke,
            // Assigned at commit from the destination's versioning state.
            version_id: None,
            content_encoding: src_obj.content_encoding,
            cache_control: new_cache_control,
            content_disposition: new_content_disposition,
            content_language: new_content_language,
            expires: new_expires,
            website_redirect_location: new_redirect,
            checksum_algorithm: Some(new_checksum_algo),
            checksum_value: Some(new_checksum_val.clone()),
            checksum_type: Some(crate::checksum::ChecksumType::FullObject),
            part_checksums: None,
            // Lock status is not copied from the source object.
            // The request's lock headers; the destination's default retention
            // is applied at commit when they ask for none.
            lock_mode: explicit_lock.mode,
            lock_retain_until: explicit_lock.retain_until,
            lock_legal_hold: explicit_lock.legal_hold,
            ..Default::default()
        };

        // Write the copy through the store, record it as the destination's
        // new version and replicate it, exactly as PutObject does. The id it
        // reports is the one assigned at commit.
        let body_source = staged_dest.source();
        let committed = crate::side_effects::commit_new_object(
            &mut accts,
            account_id,
            dest_bucket,
            Some(&*self.store),
            Some(&self.side_effects),
            dest_obj,
            |store, obj, meta| {
                store.put_object(
                    dest_bucket,
                    &obj.key,
                    meta.version_id.as_deref(),
                    body_source,
                    meta,
                )
            },
        )
        .map_err(crate::service::persistence_error)?;
        drop(accts);
        let version_id = committed.version_id.clone();

        let mut response_headers = HeaderMap::new();
        if let Some(vid) = &version_id {
            response_headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        if let Some(ref svid) = src_version_id_actual {
            response_headers.insert("x-amz-copy-source-version-id", svid.parse().unwrap());
        }
        // SSE headers in copy response
        if let Some(ref algo) = new_sse {
            response_headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        } else {
            response_headers.insert("x-amz-server-side-encryption", "AES256".parse().unwrap());
        }
        if let Some(ref kid) = new_kms {
            response_headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }
        if new_bke == Some(true) {
            response_headers.insert(
                "x-amz-server-side-encryption-bucket-key-enabled",
                "true".parse().unwrap(),
            );
        }

        let checksum_xml = format!(
            "<ChecksumType>FULL_OBJECT</ChecksumType>{}",
            crate::checksum::xml_element(new_checksum_algo, &new_checksum_val)
        );

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <CopyObjectResult>\
             <ETag>&quot;{etag}&quot;</ETag>\
             <LastModified>{}</LastModified>\
             {checksum_xml}\
             </CopyObjectResult>",
            last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        );

        // Deliver S3 event notifications
        committed.notify(
            &self.delivery,
            &self.state,
            "ObjectCreated:Copy",
            &req.account_id,
            dest_bucket,
            dest_key,
            src_obj_size,
            &etag,
        );

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: body.into(),
            headers: response_headers,
        })
    }

    pub(crate) fn restore_object(
        &self,
        account_id: &str,
        _req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;
        let glacier_classes = [
            "GLACIER",
            "DEEP_ARCHIVE",
            "GLACIER_IR",
            "INTELLIGENT_TIERING",
        ];
        if !glacier_classes.contains(&obj.storage_class.as_str()) {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::FORBIDDEN,
                "InvalidObjectState",
                "The operation is not valid for the object's storage class",
                vec![("StorageClass".to_string(), obj.storage_class.clone())],
            ));
        }
        let status = if obj.restore_ongoing.is_some() {
            StatusCode::OK
        } else {
            StatusCode::ACCEPTED
        };
        let expiry = (Utc::now() + chrono::Duration::days(30))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        obj.restore_ongoing = Some(false);
        obj.restore_expiry = Some(expiry);
        let meta = object_meta_snapshot(obj);
        self.store
            .put_object_meta(bucket, key, meta.version_id.as_deref(), &meta)
            .map_err(crate::service::persistence_error)?;
        Ok(AwsResponse {
            status,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: HeaderMap::new(),
        })
    }
}
