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

        // Check for ACL header
        let acl_header = req
            .headers
            .get("x-amz-acl")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Check for grant headers alongside canned ACL
        let has_grant_headers = req.headers.keys().any(|k| {
            let name = k.as_str();
            name.starts_with("x-amz-grant-")
        });

        if acl_header.is_some() && has_grant_headers {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Specifying both Canned ACLs and Header Grants is not allowed",
            ));
        }

        // BucketOwnerEnforced disables object ACLs at write-time too:
        // any x-amz-acl or x-amz-grant-* header rejects with
        // AccessControlListNotSupported. Plain PutObject without ACL
        // headers continues to work — only attempts to set a grant
        // are rejected.
        if (acl_header.is_some() || has_grant_headers)
            && self.bucket_owner_enforced(account_id, bucket)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AccessControlListNotSupported",
                "The bucket does not allow ACLs",
            ));
        }

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
        let (
            versioning_enabled,
            acl_owner_id,
            encryption_config,
            object_lock_config,
            notification_config,
            region,
        ) = {
            let accts = self.state.read();
            let __empty = crate::state::S3State::new(account_id, "us-east-1");
            let state = accts.get(account_id).unwrap_or(&__empty);
            let b = state
                .buckets
                .get(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;

            // Handle If-Match: check existing object etag
            if let Some(ref if_match_val) = if_match {
                match b.objects.get(key) {
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
                if inm.trim() == "*" && b.objects.contains_key(key) {
                    return Err(precondition_failed("If-None-Match"));
                }
            }

            // Object lock: validate that bucket has object lock enabled if lock headers present
            let has_lock_headers = req.headers.contains_key("x-amz-object-lock-mode")
                || req
                    .headers
                    .contains_key("x-amz-object-lock-retain-until-date")
                || req.headers.contains_key("x-amz-object-lock-legal-hold");
            if has_lock_headers && b.object_lock_config.is_none() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    "Bucket is missing ObjectLockConfiguration",
                ));
            }

            (
                b.versioning.as_deref() == Some("Enabled"),
                b.acl_owner_id.clone(),
                b.encryption_config.clone(),
                b.object_lock_config.clone(),
                b.notification_config.clone(),
                state.region.clone(),
            )
        }; // read lock dropped

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
        let spooled = fakecloud_core::service::spool_request_stream(
            stream,
            self.store.spool_dir().as_deref(),
            fakecloud_core::service::is_aws_chunked(&req.headers),
        )
        .await?;
        let data_size: u64 = spooled.size;
        let etag: String = spooled.md5_hex.clone();

        // Content-MD5 integrity check: when the client sends the header,
        // AWS verifies it against the body's MD5 and rejects a mismatch
        // with `BadDigest` (bug-audit 2026-05-28, 1.9 — corrupt-digest
        // writes used to be accepted silently). The header is the
        // base64-encoded 128-bit MD5; the spool computed it as hex, so
        // decode the hex to raw bytes and base64-encode to compare.
        if let Some(content_md5) = req.headers.get("content-md5").and_then(|v| v.to_str().ok()) {
            let supplied = content_md5.trim();
            if !supplied.is_empty() {
                let expected_b64 = hex_to_base64(&spooled.md5_hex);
                if expected_b64.as_deref() != Some(supplied) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "BadDigest",
                        "The Content-MD5 you specified did not match what we received.",
                    ));
                }
            }
        }
        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("binary/octet-stream")
            .to_string();
        let version_id = if versioning_enabled {
            Some(uuid::Uuid::new_v4().to_string())
        } else {
            None
        };
        let content_encoding = fakecloud_core::service::strip_aws_chunked_encoding(
            req.headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok()),
        );
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

        // Extract checksum algorithm and value
        let checksum_algorithm = req
            .headers
            .get("x-amz-sdk-checksum-algorithm")
            .or_else(|| req.headers.get("x-amz-checksum-algorithm"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let _checksum_from_header = checksum_algorithm.as_deref().and_then(|algo| {
            let header_name = format!("x-amz-checksum-{}", algo.to_lowercase());
            req.headers
                .get(header_name.as_str())
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        });

        // Build ACL grants for object
        let acl_grants = if has_grant_headers {
            parse_grant_headers(&req.headers)
        } else if let Some(ref acl) = acl_header {
            canned_acl_grants_for_object(acl, &acl_owner_id)
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

        // SSE headers
        let mut sse_algorithm = req
            .headers
            .get("x-amz-server-side-encryption")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let mut sse_kms_key_id = req
            .headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let bucket_key_enabled = req
            .headers
            .get("x-amz-server-side-encryption-bucket-key-enabled")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("true"));

        // Apply bucket default encryption if no explicit SSE headers
        if sse_algorithm.is_none() {
            if let Some(ref enc_config) = encryption_config {
                if let Some(algo) = extract_xml_value(enc_config, "SSEAlgorithm") {
                    if algo == "aws:kms" && sse_kms_key_id.is_none() {
                        sse_kms_key_id = extract_xml_value(enc_config, "KMSMasterKeyID");
                    }
                    sse_algorithm = Some(algo);
                }
            }
        }

        // Validate KMS key exists when using aws:kms encryption
        if sse_algorithm.as_deref() == Some("aws:kms") {
            if let Some(ref kms) = self.kms_state {
                if let Some(ref key_id) = sse_kms_key_id {
                    let kms_accounts = kms.read();
                    let kms_state = kms_accounts
                        .get(&req.account_id)
                        .unwrap_or(kms_accounts.default_ref());
                    let key_exists = kms_state
                        .keys
                        .values()
                        .any(|k| k.key_id == *key_id || k.arn == *key_id)
                        || kms_state
                            .aliases
                            .values()
                            .any(|a| a.alias_name == *key_id || a.alias_arn == *key_id);
                    if !key_exists {
                        // Still allow it — AWS doesn't always reject unknown keys
                        // for emulation purposes, just set the key ID
                        tracing::debug!(
                            key_id = %key_id,
                            "KMS key not found in state, proceeding anyway"
                        );
                    } else {
                        // Resolve alias to key ARN if needed
                        if let Some(alias) = kms_state
                            .aliases
                            .values()
                            .find(|a| a.alias_name == *key_id || a.alias_arn == *key_id)
                        {
                            if let Some(key) = kms_state.keys.get(&alias.target_key_id) {
                                sse_kms_key_id = Some(key.arn.clone());
                            }
                        } else if let Some(key) =
                            kms_state.keys.values().find(|k| k.key_id == *key_id)
                        {
                            sse_kms_key_id = Some(key.arn.clone());
                        }
                    }
                }
            }
        }

        // Checksum: detect algorithm from various headers
        let explicit_checksum_algo = req
            .headers
            .get("x-amz-checksum-algorithm")
            .or_else(|| req.headers.get("x-amz-sdk-checksum-algorithm"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_uppercase());
        let checksum_algorithm = explicit_checksum_algo.clone().or_else(|| {
            // Also detect from checksum value headers
            if req.headers.contains_key("x-amz-checksum-crc32") {
                Some("CRC32".to_string())
            } else if req.headers.contains_key("x-amz-checksum-crc32c") {
                Some("CRC32C".to_string())
            } else if req.headers.contains_key("x-amz-checksum-crc64nvme") {
                Some("CRC64NVME".to_string())
            } else if req.headers.contains_key("x-amz-checksum-sha1") {
                Some("SHA1".to_string())
            } else if req.headers.contains_key("x-amz-checksum-sha256") {
                Some("SHA256".to_string())
            } else {
                None
            }
        });
        // Checksum computation: when the client requested an explicit
        // algorithm, fold the spool through the hasher in 1 MiB chunks.
        // Constant memory; skipped when no algorithm is requested.
        let checksum_value = if let Some(algo) = &checksum_algorithm {
            Some(
                crate::service::compute_checksum_streaming(algo, &spooled.path)
                    .await
                    .map_err(crate::service::io_to_aws)?,
            )
        } else {
            None
        };

        // Object lock - explicit headers or bucket default
        let mut lock_mode = req
            .headers
            .get("x-amz-object-lock-mode")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let mut lock_retain_until = req
            .headers
            .get("x-amz-object-lock-retain-until-date")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<DateTime<Utc>>().ok());
        let lock_legal_hold = req
            .headers
            .get("x-amz-object-lock-legal-hold")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Apply bucket default lock if no explicit lock headers
        if lock_mode.is_none() && lock_retain_until.is_none() {
            if let Some(ref config) = object_lock_config {
                if let Some(mode) = extract_xml_value(config, "Mode") {
                    let days =
                        extract_xml_value(config, "Days").and_then(|d| d.parse::<i64>().ok());
                    let years =
                        extract_xml_value(config, "Years").and_then(|y| y.parse::<i64>().ok());
                    let duration = if let Some(d) = days {
                        Some(chrono::Duration::days(d))
                    } else {
                        years.map(|y| chrono::Duration::days(y * 365))
                    };
                    if let Some(dur) = duration {
                        lock_mode = Some(mode);
                        lock_retain_until = Some(Utc::now() + dur);
                    }
                }
            }
        }

        // SSE-KMS: read the spool back into memory so the KMS hook can
        // produce an envelope-encrypted blob (AES-GCM needs the whole
        // plaintext at once — chunked AEAD isn't part of the AWS S3
        // wire format). Fail-closed: a KMS error aborts PutObject
        // before any mutation. Non-KMS uploads pass `BodySource::File`
        // straight to the store and never copy the payload into RAM.
        let plaintext_size = data_size;
        let body_source: BodySource = if sse_algorithm.as_deref() == Some("aws:kms") {
            let bytes = tokio::fs::read(&spooled.path)
                .await
                .map_err(crate::service::io_to_aws)?;
            let _ = tokio::fs::remove_file(&spooled.path).await;
            let cipher = self.encrypt_object_body(
                account_id,
                &region,
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
            // The runtime body is filled in below with the BodyRef
            // returned from self.store.put_object — for memory mode that
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
            version_id: version_id.clone(),
            content_encoding,
            cache_control,
            content_disposition,
            content_language,
            expires,
            website_redirect_location,
            checksum_algorithm: checksum_algorithm.clone(),
            checksum_value: checksum_value.clone(),
            lock_mode,
            lock_retain_until,
            lock_legal_hold,
            ..Default::default()
        };

        // --- Write lock phase: insert object and handle versioning/replication ---
        {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;

            // Recheck conditional headers under write lock to prevent TOCTOU race
            if let Some(ref if_match_val) = if_match {
                match b.objects.get(key) {
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
                if inm.trim() == "*" && b.objects.contains_key(key) {
                    return Err(precondition_failed("If-None-Match"));
                }
            }

            if versioning_enabled {
                let versions = b.object_versions.entry(key.to_string()).or_default();
                // If the existing current object is a pre-versioning object (no version_id)
                // and not yet tracked in object_versions, preserve it.
                if versions.is_empty() {
                    if let Some(existing) = b.objects.get(key) {
                        if existing.version_id.is_none() {
                            versions.push(existing.clone());
                        }
                    }
                }
                versions.push(obj.clone());
            }
            b.objects.insert(key.to_string(), obj);

            // Snapshot + persist + rewrite runtime body to the returned BodyRef.
            let meta_version = {
                let b2 = state
                    .buckets
                    .get(bucket)
                    .ok_or_else(|| no_such_bucket(bucket))?;
                let o = b2.objects.get(key).ok_or_else(|| no_such_key(key))?;
                object_meta_snapshot(o)
            };
            let returned_body = super::run_blocking_io(|| {
                self.store.put_object(
                    bucket,
                    key,
                    meta_version.version_id.as_deref(),
                    body_source,
                    &meta_version,
                )
            })
            .map_err(crate::service::persistence_error)?;
            if let Some(b2) = state.buckets.get_mut(bucket) {
                if let Some(o) = b2.objects.get_mut(key) {
                    o.body = returned_body.clone();
                }
                if versioning_enabled {
                    if let Some(versions) = b2.object_versions.get_mut(key) {
                        if let Some(last) = versions.last_mut() {
                            last.body = returned_body;
                        }
                    }
                }
            }
            // Replicate (in-memory copy) then persist replicas via the store.
            replicate_through_store(state, &self.store, bucket, key)
                .map_err(crate::service::persistence_error)?;
        } // write lock dropped

        // --- Response phase: build response headers ---
        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{etag}\"").parse().unwrap());
        if let Some(vid) = &version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        // Return SSE headers
        if let Some(algo) = &sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        } else {
            headers.insert("x-amz-server-side-encryption", "AES256".parse().unwrap());
        }
        if let Some(kid) = &sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }
        if bucket_key_enabled == Some(true) {
            headers.insert(
                "x-amz-server-side-encryption-bucket-key-enabled",
                "true".parse().unwrap(),
            );
        }
        // Checksum in response
        if let (Some(algo), Some(val)) = (&checksum_algorithm, &checksum_value) {
            let header_name = format!("x-amz-checksum-{}", algo.to_lowercase());
            if let Ok(name) = header_name.parse::<http::header::HeaderName>() {
                if let Ok(hval) = val.parse() {
                    headers.insert(name, hval);
                }
            }
            // Echo back the checksum algorithm only when explicitly requested
            if explicit_checksum_algo.is_some() {
                headers.insert("x-amz-sdk-checksum-algorithm", algo.parse().unwrap());
            }
        }

        let obj_size = data_size;
        let obj_etag = etag.clone();
        let bucket_name = bucket.to_string();
        let obj_key = key.to_string();

        // Deliver S3 event notifications
        if let Some(ref config) = notification_config {
            deliver_notifications(
                &self.delivery,
                config,
                &crate::service::notifications::ObjectEvent {
                    event_name: "ObjectCreated:Put",
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
        let (raw_path, src_version_id) = if let Some((path, query)) = raw_source.split_once('?') {
            let vid = query
                .split('&')
                .find_map(|p| p.strip_prefix("versionId="))
                .map(|s| s.to_string());
            (path, vid)
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

        let if_none_match = req
            .headers
            .get("x-amz-copy-source-if-none-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let checksum_algorithm = req
            .headers
            .get("x-amz-checksum-algorithm")
            .or_else(|| req.headers.get("x-amz-sdk-checksum-algorithm"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_uppercase());

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);

        // Resolve source object, possibly a specific version
        let (src_obj, src_version_id_actual) = {
            let sb = state
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

        // Checksum: compute new if algorithm specified, or copy from source.
        // For SSE-KMS sources we work on plaintext so the checksum + the
        // destination's stored body line up with what the caller will get
        // back from a future GetObject. Fail-closed if the source can't
        // be decrypted (revoked key, malformed envelope) — copying the
        // ciphertext over to the destination would silently corrupt the
        // copy.
        let raw_src_bytes = state
            .read_body(&src_obj.body)
            .map_err(crate::service::io_to_aws)?;
        let src_bytes =
            if src_obj.sse_algorithm.as_deref() == Some("aws:kms") && self.kms_hook.is_some() {
                self.decrypt_object_body(account_id, src_bucket, &raw_src_bytes)?
            } else {
                raw_src_bytes
            };
        let (new_checksum_algo, new_checksum_val) = if let Some(ref algo) = checksum_algorithm {
            let val = compute_checksum(algo, &src_bytes);
            (Some(algo.clone()), Some(val))
        } else if src_obj.checksum_algorithm.is_some() {
            (
                src_obj.checksum_algorithm.clone(),
                src_obj.checksum_value.clone(),
            )
        } else {
            (None, None)
        };
        // Re-encrypt for the destination if it lands as SSE-KMS, so the
        // stored body is a fresh envelope with the dest's encryption
        // context (bucket arn).
        let dest_region = state.region.clone();
        let dest_stored_bytes = if new_sse.as_deref() == Some("aws:kms") && self.kms_hook.is_some()
        {
            self.encrypt_object_body(
                account_id,
                &dest_region,
                dest_bucket,
                &src_bytes,
                new_kms.as_deref(),
            )?
        } else {
            src_bytes.clone()
        };

        let db = state
            .buckets
            .get_mut(dest_bucket)
            .ok_or_else(|| no_such_bucket(dest_bucket))?;

        let version_id = if db.versioning.as_deref() == Some("Enabled") {
            Some(uuid::Uuid::new_v4().to_string())
        } else {
            None
        };

        // Default ACL for destination (not copied from source)
        let dest_acl_grants = vec![AclGrant {
            grantee_type: "CanonicalUser".to_string(),
            grantee_id: Some(db.acl_owner_id.clone()),
            grantee_display_name: Some(db.acl_owner_id.clone()),
            grantee_uri: None,
            permission: "FULL_CONTROL".to_string(),
        }];

        let dest_obj = S3Object {
            key: dest_key.to_string(),
            // Copy is an independent object — never inherit the source's
            // disk-backed BodyRef (which would point at the source's file).
            // The source bytes are read above; the destination's own on-disk
            // file is written below via `self.store.put_object`.
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
            version_id: version_id.clone(),
            content_encoding: src_obj.content_encoding,
            cache_control: new_cache_control,
            content_disposition: new_content_disposition,
            content_language: new_content_language,
            expires: new_expires,
            website_redirect_location: new_redirect,
            checksum_algorithm: new_checksum_algo.clone(),
            checksum_value: new_checksum_val.clone(),
            // Lock status is not copied from the source object.
            ..Default::default()
        };

        // Store in version history if versioning enabled
        if db.versioning.as_deref() == Some("Enabled") {
            db.object_versions
                .entry(dest_key.to_string())
                .or_default()
                .push(dest_obj.clone());
        }
        db.objects.insert(dest_key.to_string(), dest_obj);
        let dest_meta = {
            let o = db
                .objects
                .get(dest_key)
                .ok_or_else(|| no_such_key(dest_key))?;
            object_meta_snapshot(o)
        };
        // Release the `db` borrow before the store call so we can re-borrow
        // for the BodyRef rewrite and subsequent replication/notification
        // work below.
        let _ = db;
        let dest_body_ref = super::run_blocking_io(|| {
            self.store.put_object(
                dest_bucket,
                dest_key,
                dest_meta.version_id.as_deref(),
                BodySource::Bytes(dest_stored_bytes.clone()),
                &dest_meta,
            )
        })
        .map_err(crate::service::persistence_error)?;
        if let Some(db2) = state.buckets.get_mut(dest_bucket) {
            if let Some(o) = db2.objects.get_mut(dest_key) {
                o.body = dest_body_ref.clone();
            }
            if let Some(versions) = db2.object_versions.get_mut(dest_key) {
                if let Some(last) = versions.last_mut() {
                    last.body = dest_body_ref;
                }
            }
        }
        let db = state
            .buckets
            .get_mut(dest_bucket)
            .ok_or_else(|| no_such_bucket(dest_bucket))?;

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

        // Build checksum XML if present
        let checksum_xml = if let (Some(algo), Some(val)) = (&new_checksum_algo, &new_checksum_val)
        {
            format!("<Checksum{algo}>{val}</Checksum{algo}>")
        } else {
            String::new()
        };

        // Capture notification config before dropping lock
        let notification_config = db.notification_config.clone();
        let copy_size = src_obj_size;
        let copy_etag = etag.clone();
        let copy_bucket = dest_bucket.to_string();
        let copy_key = dest_key.to_string();
        let region = state.region.clone();

        // Replicate object if replication is configured on the destination bucket
        replicate_through_store(state, &self.store, dest_bucket, dest_key)
            .map_err(crate::service::persistence_error)?;

        drop(accts);

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
        if let Some(ref config) = notification_config {
            deliver_notifications(
                &self.delivery,
                config,
                &crate::service::notifications::ObjectEvent {
                    event_name: "ObjectCreated:Copy",
                    bucket_name: &copy_bucket,
                    key: &copy_key,
                    size: copy_size,
                    etag: &copy_etag,
                    region: &region,
                },
                Some(&self.state),
            );
        }

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

/// Convert a lowercase hex MD5 digest to its base64 form, matching the
/// `Content-MD5` header encoding AWS expects. Returns `None` if the input
/// isn't valid hex of the right length.
fn hex_to_base64(hex: &str) -> Option<String> {
    use base64::Engine;
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let h = hex.as_bytes();
    let mut i = 0;
    while i < h.len() {
        let hi = (h[i] as char).to_digit(16)?;
        let lo = (h[i + 1] as char).to_digit(16)?;
        bytes.push((hi * 16 + lo) as u8);
        i += 2;
    }
    Some(base64::engine::general_purpose::STANDARD.encode(bytes))
}

#[cfg(test)]
mod content_md5_tests {
    use super::hex_to_base64;

    // bug-audit 2026-05-28, 1.9: the spool yields a hex MD5; the
    // Content-MD5 header is base64 of the raw digest. Verify the
    // conversion matches the canonical base64 form.
    #[test]
    fn hex_md5_converts_to_base64() {
        // MD5("") = d41d8cd98f00b204e9800998ecf8427e
        assert_eq!(
            hex_to_base64("d41d8cd98f00b204e9800998ecf8427e").as_deref(),
            Some("1B2M2Y8AsgTpgAmY7PhCfg==")
        );
    }

    #[test]
    fn rejects_malformed_hex() {
        assert_eq!(hex_to_base64("xyz"), None); // odd length
        assert_eq!(hex_to_base64("zz"), None); // non-hex digits
    }
}
