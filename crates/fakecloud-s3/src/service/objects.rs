use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::{HeaderMap, StatusCode};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{AclGrant, S3Object};

use super::{
    canned_acl_grants_for_object, check_get_conditionals, check_head_conditionals,
    check_object_lock_for_overwrite, compute_checksum, compute_md5, deliver_notifications,
    etag_matches, extract_user_metadata, extract_xml_value, is_frozen, is_valid_storage_class,
    make_delete_marker, no_such_bucket, no_such_key, parse_delete_objects_xml, parse_grant_headers,
    parse_range_header, parse_url_encoded_tags, precondition_failed, replicate_object,
    resolve_object, s3_xml, url_encode_s3_key, xml_escape, RangeResult, S3Service,
};

impl S3Service {
    #[allow(dead_code)]
    pub(super) fn list_objects_v1(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let delimiter = req.query_params.get("delimiter").cloned();
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        let marker = req.query_params.get("marker").cloned().unwrap_or_default();
        let encoding_type = req.query_params.get("encoding-type").cloned();

        let mut contents = String::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut count = 0;
        let mut is_truncated = false;
        let mut last_key = String::new();

        for (key, obj) in &b.objects {
            if obj.is_delete_marker {
                continue;
            }
            if !key.starts_with(&prefix) {
                continue;
            }
            if !marker.is_empty() && key.as_str() <= marker.as_str() {
                continue;
            }

            // Handle delimiter-based grouping
            if let Some(ref delim) = delimiter {
                if !delim.is_empty() {
                    let suffix = &key[prefix.len()..];
                    if let Some(pos) = suffix.find(delim.as_str()) {
                        let cp = format!("{}{}", prefix, &suffix[..pos + delim.len()]);
                        if !common_prefixes.contains(&cp) {
                            if count >= max_keys {
                                is_truncated = true;
                                break;
                            }
                            common_prefixes.push(cp);
                            last_key = key.clone();
                            count += 1;
                        }
                        continue;
                    }
                }
            }

            if count >= max_keys {
                is_truncated = true;
                break;
            }

            let display_key = if encoding_type.as_deref() == Some("url") {
                url_encode_s3_key(key)
            } else {
                xml_escape(key)
            };

            contents.push_str(&format!(
                "<Contents>\
                 <Key>{}</Key>\
                 <LastModified>{}</LastModified>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <StorageClass>{}</StorageClass>\
                 </Contents>",
                display_key,
                obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                obj.etag,
                obj.size,
                obj.storage_class,
            ));
            last_key = key.clone();
            count += 1;
        }

        let mut common_prefixes_xml = String::new();
        for cp in &common_prefixes {
            let display_cp = if encoding_type.as_deref() == Some("url") {
                url_encode_s3_key(cp)
            } else {
                xml_escape(cp)
            };
            common_prefixes_xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{display_cp}</Prefix></CommonPrefixes>",
            ));
        }

        let next_marker = if is_truncated {
            format!("<NextMarker>{}</NextMarker>", xml_escape(&last_key))
        } else {
            String::new()
        };

        let delimiter_xml = match &delimiter {
            Some(d) if !d.is_empty() => format!("<Delimiter>{}</Delimiter>", xml_escape(d)),
            _ => String::new(),
        };

        let prefix_xml = if prefix.is_empty() {
            String::new()
        } else {
            let display_prefix = if encoding_type.as_deref() == Some("url") {
                url_encode_s3_key(&prefix)
            } else {
                xml_escape(&prefix)
            };
            format!("<Prefix>{display_prefix}</Prefix>")
        };

        let marker_xml = if marker.is_empty() {
            String::new()
        } else {
            format!("<Marker>{}</Marker>", xml_escape(&marker))
        };

        let encoding_xml = if encoding_type.as_deref() == Some("url") {
            "<EncodingType>url</EncodingType>".to_string()
        } else {
            String::new()
        };

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{bucket}</Name>\
             {prefix_xml}\
             {marker_xml}\
             <MaxKeys>{max_keys}</MaxKeys>\
             {delimiter_xml}\
             {encoding_xml}\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {contents}\
             {common_prefixes_xml}\
             {next_marker}\
             </ListBucketResult>",
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(super) fn list_objects_v2(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let delimiter = req
            .query_params
            .get("delimiter")
            .cloned()
            .unwrap_or_default();
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        let start_after = req
            .query_params
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let continuation = req.query_params.get("continuation-token").cloned();
        if let Some(ref ct) = continuation {
            if ct.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "The continuation token provided is incorrect",
                ));
            }
        }
        let fetch_owner = req
            .query_params
            .get("fetch-owner")
            .map(|v| v == "true")
            .unwrap_or(false);

        let effective_start = continuation.as_deref().unwrap_or(&start_after);

        let mut contents = String::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut count = 0;
        let mut is_truncated = false;
        let mut last_key = String::new();

        for (key, obj) in &b.objects {
            if obj.is_delete_marker {
                continue;
            }
            if !key.starts_with(&prefix) {
                continue;
            }
            if !effective_start.is_empty() && key.as_str() <= effective_start {
                continue;
            }

            // Handle delimiter-based grouping
            if !delimiter.is_empty() {
                if prefix.len() > key.len() {
                    continue;
                }
                let suffix = &key[prefix.len()..];
                if let Some(pos) = suffix.find(&delimiter) {
                    let end = (pos + delimiter.len()).min(suffix.len());
                    let cp = format!("{}{}", prefix, &suffix[..end]);
                    if !common_prefixes.contains(&cp) {
                        if count >= max_keys {
                            is_truncated = true;
                            break;
                        }
                        common_prefixes.push(cp);
                        last_key = key.clone();
                        count += 1;
                    }
                    continue;
                }
            }

            if count >= max_keys {
                is_truncated = true;
                break;
            }

            let owner_xml = if fetch_owner {
                let oid = obj.acl_owner_id.as_deref().unwrap_or(&b.acl_owner_id);
                format!(
                    "<Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner>",
                    xml_escape(oid),
                    xml_escape(oid),
                )
            } else {
                String::new()
            };

            let checksum_xml = if let Some(ref algo) = obj.checksum_algorithm {
                format!(
                    "<ChecksumAlgorithm>{}</ChecksumAlgorithm>",
                    xml_escape(algo)
                )
            } else {
                String::new()
            };

            let use_url_enc =
                req.query_params.get("encoding-type").map(|s| s.as_str()) == Some("url");
            let display_key = if use_url_enc {
                url_encode_s3_key(key)
            } else {
                xml_escape(key)
            };

            contents.push_str(&format!(
                "<Contents>\
                 <Key>{}</Key>\
                 <LastModified>{}</LastModified>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <StorageClass>{}</StorageClass>\
                 {owner_xml}{checksum_xml}\
                 </Contents>",
                display_key,
                obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                obj.etag,
                obj.size,
                obj.storage_class,
            ));
            last_key = key.clone();
            count += 1;
        }

        let encoding_type = req.query_params.get("encoding-type").cloned();
        let use_url_encoding = encoding_type.as_deref() == Some("url");

        let mut common_prefixes_xml = String::new();
        for cp in &common_prefixes {
            let display_cp = if use_url_encoding {
                url_encode_s3_key(cp)
            } else {
                xml_escape(cp)
            };
            common_prefixes_xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{display_cp}</Prefix></CommonPrefixes>",
            ));
        }

        let next_token = if is_truncated {
            format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                xml_escape(&last_key)
            )
        } else {
            String::new()
        };

        let cont_token = if let Some(ct) = &continuation {
            format!("<ContinuationToken>{}</ContinuationToken>", xml_escape(ct))
        } else {
            String::new()
        };

        let encoding_xml = if use_url_encoding {
            "<EncodingType>url</EncodingType>".to_string()
        } else {
            String::new()
        };
        let delimiter_xml = if delimiter.is_empty() {
            String::new()
        } else {
            format!("<Delimiter>{}</Delimiter>", xml_escape(&delimiter))
        };
        // StartAfter is only included when no ContinuationToken is present
        let start_after_xml = if start_after.is_empty() || continuation.is_some() {
            String::new()
        } else {
            format!("<StartAfter>{}</StartAfter>", xml_escape(&start_after))
        };

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{bucket}</Name><Prefix>{prefix}</Prefix>{delimiter_xml}{encoding_xml}\
             <KeyCount>{count}</KeyCount>\
             <MaxKeys>{max_keys}</MaxKeys>{start_after_xml}<IsTruncated>{is_truncated}</IsTruncated>\
             {cont_token}{next_token}{contents}{common_prefixes_xml}</ListBucketResult>",
            prefix = if use_url_encoding { url_encode_s3_key(&prefix) } else { xml_escape(&prefix) },
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(super) fn list_object_versions(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let delimiter = req.query_params.get("delimiter").cloned();
        let key_marker = req
            .query_params
            .get("key-marker")
            .cloned()
            .unwrap_or_default();
        let version_id_marker = req.query_params.get("version-id-marker").cloned();
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        let owner_id = &b.acl_owner_id;

        // Build a sorted list of all version entries: (key, obj, is_latest)
        let mut all_entries: Vec<(&str, &S3Object, bool)> = Vec::new();

        if b.object_versions.is_empty() {
            // No versioning history — every object in b.objects is the only version
            for (key, obj) in &b.objects {
                all_entries.push((key.as_str(), obj, true));
            }
        } else {
            // Collect versioned keys
            let mut keys: Vec<&String> = b.object_versions.keys().collect();
            keys.sort();
            for key in &keys {
                if let Some(versions) = b.object_versions.get(key.as_str()) {
                    let len = versions.len();
                    // Latest version is last in the vec; iterate newest-first
                    for (i, obj) in versions.iter().enumerate().rev() {
                        let is_latest = i == len - 1;
                        all_entries.push((key.as_str(), obj, is_latest));
                    }
                }
            }
            // Include non-versioned objects (keys not in object_versions)
            for (key, obj) in &b.objects {
                if !b.object_versions.contains_key(key) {
                    all_entries.push((key.as_str(), obj, true));
                }
            }
            // Sort by key, then newest-first within key (already done by rev above,
            // but we need global sort since we mixed in non-versioned objects)
            all_entries.sort_by(|a, b_entry| a.0.cmp(b_entry.0));
        }

        // Filter by prefix
        all_entries.retain(|(key, _, _)| key.starts_with(prefix.as_str()));

        // Apply key-marker / version-id-marker pagination
        if !key_marker.is_empty() {
            let vid_marker = version_id_marker.as_deref();
            let mut skip = true;
            all_entries.retain(|(key, obj, _)| {
                if !skip {
                    return true;
                }
                if *key < key_marker.as_str() {
                    return false; // before marker, skip
                }
                if *key > key_marker.as_str() {
                    skip = false;
                    return true; // past marker key, include
                }
                // key == key_marker: skip until we find the version_id_marker
                if let Some(vid) = vid_marker {
                    if obj.version_id.as_deref().unwrap_or("null") == vid {
                        // Found the marker version — skip it, include everything after
                        skip = false;
                        return false;
                    }
                    false // still before the version marker
                } else {
                    false // skip entire key_marker key when no version-id-marker
                }
            });
        }

        // Handle delimiter: collect common prefixes
        let mut common_prefixes: Vec<String> = Vec::new();
        if let Some(ref delim) = delimiter {
            let mut filtered_entries = Vec::new();
            let mut seen_prefixes = std::collections::HashSet::new();
            for entry @ (key, _, _) in &all_entries {
                let after_prefix = &key[prefix.len()..];
                if let Some(pos) = after_prefix.find(delim.as_str()) {
                    let cp = format!("{}{}", prefix, &after_prefix[..pos + delim.len()]);
                    if seen_prefixes.insert(cp.clone()) {
                        common_prefixes.push(cp);
                    }
                } else {
                    filtered_entries.push(*entry);
                }
            }
            all_entries = filtered_entries;
        }

        // Pagination: truncate at max_keys (count versions + delete markers + common prefixes)
        let total_items = all_entries.len() + common_prefixes.len();
        let is_truncated = total_items > max_keys;

        // We need to limit versions to max_keys minus common_prefixes already counted
        let version_limit = max_keys.saturating_sub(common_prefixes.len());
        let truncated_entries: Vec<_> = all_entries.iter().take(version_limit).collect();
        let next_markers = if is_truncated && !truncated_entries.is_empty() {
            let last = truncated_entries.last().unwrap();
            Some((
                last.0.to_string(),
                last.1
                    .version_id
                    .clone()
                    .unwrap_or_else(|| "null".to_string()),
            ))
        } else {
            None
        };

        // Build XML
        let mut versions_xml = String::new();
        for (key, obj, is_latest) in &truncated_entries {
            if obj.is_delete_marker {
                versions_xml.push_str(&format!(
                    "<DeleteMarker>\
                     <Key>{}</Key>\
                     <VersionId>{}</VersionId>\
                     <IsLatest>{}</IsLatest>\
                     <LastModified>{}</LastModified>\
                     <Owner><ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName></Owner>\
                     </DeleteMarker>",
                    xml_escape(key),
                    obj.version_id.as_deref().unwrap_or("null"),
                    is_latest,
                    obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                ));
            } else {
                versions_xml.push_str(&format!(
                    "<Version>\
                     <Key>{}</Key>\
                     <VersionId>{}</VersionId>\
                     <IsLatest>{}</IsLatest>\
                     <LastModified>{}</LastModified>\
                     <ETag>&quot;{}&quot;</ETag>\
                     <Size>{}</Size>\
                     <Owner><ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName></Owner>\
                     <StorageClass>{}</StorageClass>\
                     </Version>",
                    xml_escape(key),
                    obj.version_id.as_deref().unwrap_or("null"),
                    is_latest,
                    obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    obj.etag,
                    obj.size,
                    obj.storage_class,
                ));
            }
        }

        // Common prefixes
        let mut cp_xml = String::new();
        for cp in &common_prefixes {
            cp_xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(cp),
            ));
        }

        // Pagination markers
        let marker_xml = if let Some((ref nk, ref nv)) = next_markers {
            format!(
                "<NextKeyMarker>{}</NextKeyMarker>\
                 <NextVersionIdMarker>{}</NextVersionIdMarker>",
                xml_escape(nk),
                xml_escape(nv),
            )
        } else {
            String::new()
        };

        let delimiter_xml = delimiter
            .as_ref()
            .map(|d| format!("<Delimiter>{}</Delimiter>", xml_escape(d)))
            .unwrap_or_default();

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{name}</Name>\
             <Prefix>{pfx}</Prefix>\
             <KeyMarker>{km}</KeyMarker>\
             {delimiter_xml}\
             <MaxKeys>{max_keys}</MaxKeys>\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {marker_xml}\
             {versions_xml}\
             {cp_xml}\
             </ListVersionsResult>",
            name = xml_escape(bucket),
            pfx = xml_escape(&prefix),
            km = xml_escape(&key_marker),
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(super) fn put_object(
        &self,
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
            std::collections::HashMap::new()
        };

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
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

        let data = req.body.clone();
        let data_size = data.len() as u64;
        let etag = compute_md5(&data);
        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("binary/octet-stream")
            .to_string();
        let version_id = if b.versioning.as_deref() == Some("Enabled") {
            Some(uuid::Uuid::new_v4().to_string())
        } else {
            None
        };
        let content_encoding = req
            .headers
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
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
            canned_acl_grants_for_object(acl, &b.acl_owner_id)
        } else {
            // Default: owner full control
            vec![AclGrant {
                grantee_type: "CanonicalUser".to_string(),
                grantee_id: Some(b.acl_owner_id.clone()),
                grantee_display_name: Some(b.acl_owner_id.clone()),
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
            if let Some(ref enc_config) = b.encryption_config {
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
                    let kms_state = kms.read();
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
            } else if req.headers.contains_key("x-amz-checksum-sha1") {
                Some("SHA1".to_string())
            } else if req.headers.contains_key("x-amz-checksum-sha256") {
                Some("SHA256".to_string())
            } else {
                None
            }
        });
        let checksum_value = checksum_algorithm
            .as_deref()
            .map(|algo| compute_checksum(algo, &data));

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
            if let Some(ref config) = b.object_lock_config {
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

        let obj = S3Object {
            key: key.to_string(),
            size: data.len() as u64,
            data,
            content_type,
            etag: etag.clone(),
            last_modified: Utc::now(),
            metadata,
            storage_class,
            tags,
            acl_grants,
            acl_owner_id: Some(b.acl_owner_id.clone()),
            parts_count: None,
            part_sizes: None,
            sse_algorithm: sse_algorithm.clone(),
            sse_kms_key_id: sse_kms_key_id.clone(),
            bucket_key_enabled,
            version_id: version_id.clone(),
            is_delete_marker: false,
            content_encoding,
            website_redirect_location,
            restore_ongoing: None,
            restore_expiry: None,
            checksum_algorithm: checksum_algorithm.clone(),
            checksum_value: checksum_value.clone(),
            lock_mode,
            lock_retain_until,
            lock_legal_hold,
        };
        if b.versioning.as_deref() == Some("Enabled") {
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

        // Capture notification config before dropping state lock
        let notification_config = b.notification_config.clone();
        let obj_size = data_size;
        let obj_etag = etag.clone();
        let bucket_name = bucket.to_string();
        let obj_key = key.to_string();
        let region = state.region.clone();

        // Replicate object if replication is configured on the source bucket
        replicate_object(&mut state, bucket, key);

        drop(state);

        // Deliver S3 event notifications
        if let Some(ref config) = notification_config {
            deliver_notifications(
                &self.delivery,
                config,
                "ObjectCreated:Put",
                &bucket_name,
                &obj_key,
                obj_size,
                &obj_etag,
                &region,
            );
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: String::new(),
            body: Bytes::new(),
            headers,
        })
    }

    pub(super) fn get_object(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = resolve_object(b, key, req.query_params.get("versionId"))?;

        if obj.is_delete_marker {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                "The specified key does not exist.",
                vec![("Key".to_string(), key.to_string())],
            ));
        }

        // Glacier / Deep Archive: cannot GET unless restored
        if is_frozen(obj) {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::FORBIDDEN,
                "InvalidObjectState",
                "The operation is not valid for the object's storage class",
                vec![("StorageClass".to_string(), obj.storage_class.clone())],
            ));
        }

        // Conditional checks
        check_get_conditionals(req, obj)?;
        let total_size = obj.size as usize;
        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{}\"", obj.etag).parse().unwrap());
        headers.insert(
            "last-modified",
            obj.last_modified
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
                .parse()
                .unwrap(),
        );
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        // Always include storage class
        headers.insert("x-amz-storage-class", obj.storage_class.parse().unwrap());
        if let Some(vid) = &obj.version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        if let Some(ref enc) = obj.content_encoding {
            headers.insert("content-encoding", enc.parse().unwrap());
        }
        for (k, v) in &obj.metadata {
            if let (Ok(name), Ok(val)) = (
                format!("x-amz-meta-{k}").parse::<http::header::HeaderName>(),
                v.parse::<http::header::HeaderValue>(),
            ) {
                headers.insert(name, val);
            }
        }
        if let Some(ref redirect) = obj.website_redirect_location {
            headers.insert("x-amz-website-redirect-location", redirect.parse().unwrap());
        }
        if !obj.tags.is_empty() {
            headers.insert(
                "x-amz-tagging-count",
                obj.tags.len().to_string().parse().unwrap(),
            );
        }

        // SSE headers - only when explicitly set
        if let Some(algo) = &obj.sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        }
        if let Some(kid) = &obj.sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }
        if let Some(true) = obj.bucket_key_enabled {
            headers.insert(
                "x-amz-server-side-encryption-bucket-key-enabled",
                "true".parse().unwrap(),
            );
        }

        // Object lock headers
        if let Some(ref mode) = obj.lock_mode {
            headers.insert("x-amz-object-lock-mode", mode.parse().unwrap());
        }
        if let Some(ref until) = obj.lock_retain_until {
            headers.insert(
                "x-amz-object-lock-retain-until-date",
                until.to_rfc3339().parse().unwrap(),
            );
        }
        if let Some(ref hold) = obj.lock_legal_hold {
            headers.insert("x-amz-object-lock-legal-hold", hold.parse().unwrap());
        }
        if let Some(ongoing) = obj.restore_ongoing {
            let rv = if ongoing {
                "ongoing-request=\"true\"".to_string()
            } else if let Some(ref exp) = obj.restore_expiry {
                format!("ongoing-request=\"false\", expiry-date=\"{exp}\"")
            } else {
                "ongoing-request=\"false\"".to_string()
            };
            headers.insert("x-amz-restore", rv.parse().unwrap());
        }
        let mut response_status = StatusCode::OK;
        let response_body;
        let mut is_range_request = false;
        if let Some(range_str) = req.headers.get("range").and_then(|v| v.to_str().ok()) {
            if let Some(rr) = parse_range_header(range_str, total_size) {
                match rr {
                    RangeResult::Satisfiable { start, end } => {
                        headers.insert(
                            "content-range",
                            format!("bytes {start}-{end}/{total_size}").parse().unwrap(),
                        );
                        headers.insert(
                            "content-length",
                            (end - start + 1).to_string().parse().unwrap(),
                        );
                        response_body = obj.data.slice(start..=end);
                        response_status = StatusCode::PARTIAL_CONTENT;
                        is_range_request = true;
                    }
                    RangeResult::NotSatisfiable => {
                        return Err(AwsServiceError::aws_error_with_fields(
                            StatusCode::RANGE_NOT_SATISFIABLE,
                            "InvalidRange",
                            "The requested range is not satisfiable",
                            vec![
                                ("ActualObjectSize".to_string(), total_size.to_string()),
                                ("RangeRequested".to_string(), range_str.to_string()),
                            ],
                        ));
                    }
                    RangeResult::Ignored => {
                        headers.insert("content-length", total_size.to_string().parse().unwrap());
                        response_body = obj.data.clone();
                    }
                }
            } else {
                headers.insert("content-length", total_size.to_string().parse().unwrap());
                response_body = obj.data.clone();
            }
        } else if let Some(part_num_str) = req.query_params.get("partNumber") {
            if let Ok(part_num) = part_num_str.parse::<u32>() {
                // Validate part number
                let max_parts = obj.parts_count.unwrap_or(1) as usize;
                if part_num < 1 || part_num as usize > max_parts {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        "InvalidRange",
                        "The requested range is not satisfiable",
                    ));
                }
                let mut part_start: usize = 0;
                let mut part_size = total_size;
                if let Some(ref part_sizes) = obj.part_sizes {
                    let mut offset: usize = 0;
                    for &(pn, sz) in part_sizes {
                        if pn == part_num {
                            part_start = offset;
                            part_size = sz as usize;
                            break;
                        }
                        offset += sz as usize;
                    }
                }
                if let Some(pc) = obj.parts_count {
                    headers.insert("x-amz-mp-parts-count", pc.to_string().parse().unwrap());
                }
                let part_end = part_start + part_size - 1;
                headers.insert(
                    "content-range",
                    format!("bytes {part_start}-{part_end}/{total_size}")
                        .parse()
                        .unwrap(),
                );
                headers.insert("content-length", part_size.to_string().parse().unwrap());
                response_body = obj.data.slice(part_start..part_start + part_size);
                response_status = StatusCode::PARTIAL_CONTENT;
            } else {
                headers.insert("content-length", total_size.to_string().parse().unwrap());
                response_body = obj.data.clone();
            }
        } else {
            headers.insert("content-length", total_size.to_string().parse().unwrap());
            response_body = obj.data.clone();
        }
        // Only include checksum headers for full (non-range) responses
        if !is_range_request {
            if let Some(algo) = &obj.checksum_algorithm {
                if let Some(val) = &obj.checksum_value {
                    let hn = format!("x-amz-checksum-{}", algo.to_lowercase());
                    if let Ok(name) = hn.parse::<http::header::HeaderName>() {
                        if let Ok(hv) = val.parse() {
                            headers.insert(name, hv);
                        }
                    }
                }
            }
        }
        Ok(AwsResponse {
            status: response_status,
            content_type: obj.content_type.clone(),
            body: response_body,
            headers,
        })
    }

    /// Serve a website object (index or error document) from the bucket.
    pub(super) fn serve_website_object(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        website_config: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let result = self.get_object(req, bucket, key);
        if result.is_err() {
            // If index doc doesn't exist either, try error document
            if let Some(error_key) = extract_xml_value(website_config, "ErrorDocument")
                .and_then(|inner| {
                    let open = "<Key>";
                    let close = "</Key>";
                    let s = inner.find(open)? + open.len();
                    let e = inner.find(close)?;
                    Some(inner[s..e].trim().to_string())
                })
                .or_else(|| extract_xml_value(website_config, "Key"))
            {
                return self.serve_website_error(req, bucket, &error_key);
            }
        }
        result
    }

    /// Serve the website error document with a 404 status.
    pub(super) fn serve_website_error(
        &self,
        req: &AwsRequest,
        bucket: &str,
        error_key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        match self.get_object(req, bucket, error_key) {
            Ok(mut resp) => {
                resp.status = StatusCode::NOT_FOUND;
                Ok(resp)
            }
            Err(e) => Err(e),
        }
    }

    pub(super) fn delete_object(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let if_match = req
            .headers
            .get("if-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let version_id_param = req.query_params.get("versionId").cloned();

        let mut state = self.state.write();
        let region = state.region.clone();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

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

        let mut resp_headers = HeaderMap::new();
        let versioning_enabled = b.versioning.as_deref() == Some("Enabled");

        // Delete a specific version
        if let Some(ref vid) = version_id_param {
            // Check object lock before deleting a specific version
            let locked_obj = {
                let mut found: Option<&S3Object> = None;
                if let Some(versions) = b.object_versions.get(key) {
                    found = versions
                        .iter()
                        .find(|o| o.version_id.as_deref() == Some(vid.as_str()));
                }
                if found.is_none() {
                    if let Some(obj) = b.objects.get(key) {
                        let matches = obj.version_id.as_deref() == Some(vid.as_str())
                            || (vid == "null" && obj.version_id.is_none());
                        if matches {
                            found = Some(obj);
                        }
                    }
                }
                found.and_then(|obj| {
                    if obj.is_delete_marker {
                        return None;
                    }
                    // Legal hold blocks delete
                    if obj.lock_legal_hold.as_deref() == Some("ON") {
                        return Some("AccessDenied");
                    }
                    // Retention check
                    if let (Some(mode), Some(until)) = (&obj.lock_mode, &obj.lock_retain_until) {
                        if *until > Utc::now() {
                            if mode == "COMPLIANCE" {
                                return Some("AccessDenied");
                            }
                            if mode == "GOVERNANCE" {
                                // Check bypass header
                                let bypass = req
                                    .headers
                                    .get("x-amz-bypass-governance-retention")
                                    .and_then(|v| v.to_str().ok())
                                    .map(|s| s.eq_ignore_ascii_case("true"))
                                    .unwrap_or(false);
                                if !bypass {
                                    return Some("AccessDenied");
                                }
                            }
                        }
                    }
                    None
                })
            };
            if let Some(code) = locked_obj {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    code,
                    "Access Denied",
                ));
            }

            let mut is_dm = false;
            if let Some(versions) = b.object_versions.get_mut(key) {
                let vid_matches = |o: &S3Object| {
                    o.version_id.as_deref() == Some(vid.as_str())
                        || (vid == "null" && o.version_id.is_none())
                };
                is_dm = versions
                    .iter()
                    .any(|o| vid_matches(o) && o.is_delete_marker);
                let len_before = versions.len();
                versions.retain(|o| !vid_matches(o));
                let removed = len_before != versions.len();
                // Only update current object if we actually removed a version
                if removed {
                    if let Some(latest) = versions.last() {
                        if latest.is_delete_marker {
                            b.objects.remove(key);
                        } else {
                            b.objects.insert(key.to_string(), latest.clone());
                        }
                    } else {
                        b.objects.remove(key);
                    }
                }
                if versions.is_empty() {
                    b.object_versions.remove(key);
                }
            } else if let Some(obj) = b.objects.get(key) {
                // Match explicit version id, or treat "null" as matching objects with no version
                let matches = obj.version_id.as_deref() == Some(vid.as_str())
                    || (vid == "null" && obj.version_id.is_none());
                if matches {
                    is_dm = obj.is_delete_marker;
                    b.objects.remove(key);
                }
            }
            resp_headers.insert("x-amz-version-id", vid.parse().unwrap());
            if is_dm {
                resp_headers.insert("x-amz-delete-marker", "true".parse().unwrap());
            }
            return Ok(AwsResponse {
                status: StatusCode::NO_CONTENT,
                content_type: "application/xml".to_string(),
                body: Bytes::new(),
                headers: resp_headers,
            });
        }

        // Check object lock for non-version-specific deletes on non-versioned buckets
        if !versioning_enabled {
            if let Some(existing) = b.objects.get(key) {
                if !existing.is_delete_marker {
                    if let Some(code) = check_object_lock_for_overwrite(existing, req) {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::FORBIDDEN,
                            code,
                            "Access Denied",
                        ));
                    }
                }
            }
        }

        // Versioned bucket: create a delete marker
        if versioning_enabled {
            // If the existing object was created before versioning, preserve it
            if !b.object_versions.contains_key(key) {
                if let Some(existing) = b.objects.get(key) {
                    let mut preserved = existing.clone();
                    if preserved.version_id.is_none() {
                        preserved.version_id = Some("null".to_string());
                    }
                    b.object_versions
                        .entry(key.to_string())
                        .or_default()
                        .push(preserved);
                }
            }
            let dm_id = Uuid::new_v4().to_string();
            let marker = make_delete_marker(key, &dm_id);
            b.object_versions
                .entry(key.to_string())
                .or_default()
                .push(marker.clone());
            b.objects.insert(key.to_string(), marker);
            resp_headers.insert("x-amz-version-id", dm_id.parse().unwrap());
            resp_headers.insert("x-amz-delete-marker", "true".parse().unwrap());

            // Notification for delete
            let notification_config = b.notification_config.clone();
            let bucket_name = bucket.to_string();
            let obj_key = key.to_string();
            let region = region.clone();
            drop(state);
            if let Some(ref config) = notification_config {
                deliver_notifications(
                    &self.delivery,
                    config,
                    "ObjectRemoved:DeleteMarkerCreated",
                    &bucket_name,
                    &obj_key,
                    0,
                    "",
                    &region,
                );
            }

            return Ok(AwsResponse {
                status: StatusCode::NO_CONTENT,
                content_type: "application/xml".to_string(),
                body: Bytes::new(),
                headers: resp_headers,
            });
        }

        // Capture notification config before removing
        let notification_config = b.notification_config.clone();
        let bucket_name = bucket.to_string();
        let obj_key = key.to_string();

        b.objects.remove(key);
        drop(state);

        // Deliver S3 event notifications
        if let Some(ref config) = notification_config {
            deliver_notifications(
                &self.delivery,
                config,
                "ObjectRemoved:Delete",
                &bucket_name,
                &obj_key,
                0,
                "",
                &region,
            );
        }

        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    pub(super) fn head_object(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = resolve_object(b, key, req.query_params.get("versionId"))?;
        if obj.is_delete_marker {
            if req.query_params.contains_key("versionId") {
                let mut headers = HeaderMap::new();
                headers.insert("x-amz-delete-marker", "true".parse().unwrap());
                headers.insert("allow", "DELETE".parse().unwrap());
                if let Some(vid) = &obj.version_id {
                    headers.insert("x-amz-version-id", vid.parse().unwrap());
                }
                return Ok(AwsResponse {
                    status: StatusCode::METHOD_NOT_ALLOWED,
                    content_type: "application/xml".to_string(),
                    body: Bytes::new(),
                    headers,
                });
            }
            let mut headers = HeaderMap::new();
            headers.insert("x-amz-delete-marker", "true".parse().unwrap());
            if let Some(vid) = &obj.version_id {
                headers.insert("x-amz-version-id", vid.parse().unwrap());
            }
            return Ok(AwsResponse {
                status: StatusCode::NOT_FOUND,
                content_type: "application/xml".to_string(),
                body: Bytes::new(),
                headers,
            });
        }

        // Conditional checks for HEAD
        check_head_conditionals(req, obj)?;
        let total_size = obj.size;
        let mut response_status = StatusCode::OK;
        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{}\"", obj.etag).parse().unwrap());
        headers.insert(
            "last-modified",
            obj.last_modified
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
                .parse()
                .unwrap(),
        );
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        headers.insert("x-amz-storage-class", obj.storage_class.parse().unwrap());
        if let Some(ref enc) = obj.content_encoding {
            headers.insert("content-encoding", enc.parse().unwrap());
        }
        if let Some(range_str) = req.headers.get("range").and_then(|v| v.to_str().ok()) {
            if let Some(range_result) = parse_range_header(range_str, total_size as usize) {
                match range_result {
                    RangeResult::Satisfiable { start, end } => {
                        headers.insert(
                            "content-range",
                            format!("bytes {start}-{end}/{total_size}").parse().unwrap(),
                        );
                        headers.insert(
                            "content-length",
                            (end - start + 1).to_string().parse().unwrap(),
                        );
                        response_status = StatusCode::PARTIAL_CONTENT;
                    }
                    RangeResult::NotSatisfiable => {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::RANGE_NOT_SATISFIABLE,
                            "InvalidRange",
                            "The requested range is not satisfiable",
                        ));
                    }
                    RangeResult::Ignored => {
                        headers.insert("content-length", total_size.to_string().parse().unwrap());
                    }
                }
            } else {
                headers.insert("content-length", total_size.to_string().parse().unwrap());
            }
        } else if let Some(part_num_str) = req.query_params.get("partNumber") {
            if let Ok(part_num) = part_num_str.parse::<u32>() {
                // Validate part number
                let max_parts = obj.parts_count.unwrap_or(1);
                if part_num < 1 || part_num > max_parts {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        "InvalidRange",
                        "The requested range is not satisfiable",
                    ));
                }
                let mut part_start: u64 = 0;
                let mut part_size = total_size;
                if let Some(ref part_sizes) = obj.part_sizes {
                    let mut offset: u64 = 0;
                    for &(pn, sz) in part_sizes {
                        if pn == part_num {
                            part_start = offset;
                            part_size = sz;
                            break;
                        }
                        offset += sz;
                    }
                }
                if let Some(pc) = obj.parts_count {
                    headers.insert("x-amz-mp-parts-count", pc.to_string().parse().unwrap());
                }
                let part_end = part_start + part_size - 1;
                headers.insert(
                    "content-range",
                    format!("bytes {part_start}-{part_end}/{total_size}")
                        .parse()
                        .unwrap(),
                );
                headers.insert("content-length", part_size.to_string().parse().unwrap());
                response_status = StatusCode::PARTIAL_CONTENT;
            } else {
                headers.insert("content-length", total_size.to_string().parse().unwrap());
            }
        } else {
            headers.insert("content-length", total_size.to_string().parse().unwrap());
        }
        for (k, v) in &obj.metadata {
            if let (Ok(name), Ok(val)) = (
                format!("x-amz-meta-{k}").parse::<http::header::HeaderName>(),
                v.parse::<http::header::HeaderValue>(),
            ) {
                headers.insert(name, val);
            }
        }
        if let Some(ref redirect) = obj.website_redirect_location {
            headers.insert("x-amz-website-redirect-location", redirect.parse().unwrap());
        }

        if let Some(vid) = &obj.version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }

        // SSE headers
        if let Some(algo) = &obj.sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        }
        if let Some(kid) = &obj.sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }
        if let Some(true) = obj.bucket_key_enabled {
            headers.insert(
                "x-amz-server-side-encryption-bucket-key-enabled",
                "true".parse().unwrap(),
            );
        }

        // Object lock headers
        if let Some(ref mode) = obj.lock_mode {
            headers.insert("x-amz-object-lock-mode", mode.parse().unwrap());
        }
        if let Some(ref until) = obj.lock_retain_until {
            headers.insert(
                "x-amz-object-lock-retain-until-date",
                until.to_rfc3339().parse().unwrap(),
            );
        }
        if let Some(ref hold) = obj.lock_legal_hold {
            headers.insert("x-amz-object-lock-legal-hold", hold.parse().unwrap());
        }
        if let Some(ongoing) = obj.restore_ongoing {
            let restore_val = if ongoing {
                "ongoing-request=\"true\"".to_string()
            } else if let Some(ref expiry) = obj.restore_expiry {
                format!("ongoing-request=\"false\", expiry-date=\"{expiry}\"")
            } else {
                "ongoing-request=\"false\"".to_string()
            };
            headers.insert("x-amz-restore", restore_val.parse().unwrap());
        }
        // Checksum headers (returned when ChecksumMode=ENABLED or always if set)
        if let Some(algo) = &obj.checksum_algorithm {
            if let Some(val) = &obj.checksum_value {
                let hn = format!("x-amz-checksum-{}", algo.to_lowercase());
                if let Ok(name) = hn.parse::<http::header::HeaderName>() {
                    if let Ok(hv) = val.parse() {
                        headers.insert(name, hv);
                    }
                }
            }
        }

        Ok(AwsResponse {
            status: response_status,
            content_type: obj.content_type.clone(),
            body: Bytes::new(),
            headers,
        })
    }

    pub(super) fn copy_object(
        &self,
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

        let mut state = self.state.write();

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

        // Checksum: compute new if algorithm specified, or copy from source
        let (new_checksum_algo, new_checksum_val) = if let Some(ref algo) = checksum_algorithm {
            let val = compute_checksum(algo, &src_obj.data);
            (Some(algo.clone()), Some(val))
        } else if src_obj.checksum_algorithm.is_some() {
            (
                src_obj.checksum_algorithm.clone(),
                src_obj.checksum_value.clone(),
            )
        } else {
            (None, None)
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
            data: src_obj.data,
            size: src_obj.size,
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
            is_delete_marker: false,
            content_encoding: src_obj.content_encoding,
            website_redirect_location: new_redirect,
            restore_ongoing: None,
            restore_expiry: None,
            checksum_algorithm: new_checksum_algo.clone(),
            checksum_value: new_checksum_val.clone(),
            // Do not copy lock from source
            lock_mode: None,
            lock_retain_until: None,
            lock_legal_hold: None,
        };

        // Store in version history if versioning enabled
        if db.versioning.as_deref() == Some("Enabled") {
            db.object_versions
                .entry(dest_key.to_string())
                .or_default()
                .push(dest_obj.clone());
        }
        db.objects.insert(dest_key.to_string(), dest_obj);

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
        replicate_object(&mut state, dest_bucket, dest_key);

        drop(state);

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
                "ObjectCreated:Copy",
                &copy_bucket,
                &copy_key,
                copy_size,
                &copy_etag,
                &region,
            );
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: body.into(),
            headers: response_headers,
        })
    }

    pub(super) fn delete_objects(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let entries = parse_delete_objects_xml(body_str);

        if entries.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
            ));
        }

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let bypass = req
            .headers
            .get("x-amz-bypass-governance-retention")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let versioning_enabled = b.versioning.as_deref() == Some("Enabled");
        let mut deleted_xml = String::new();
        let mut error_xml = String::new();
        for entry in &entries {
            let key = &entry.key;
            if let Some(ref vid) = entry.version_id {
                // Check lock before deleting specific version
                let lock_denied = {
                    let obj_opt = b
                        .object_versions
                        .get(key)
                        .and_then(|vs| {
                            vs.iter()
                                .find(|o| o.version_id.as_deref() == Some(vid.as_str()))
                        })
                        .or_else(|| {
                            b.objects.get(key).filter(|o| {
                                o.version_id.as_deref() == Some(vid.as_str())
                                    || (vid == "null" && o.version_id.is_none())
                            })
                        });
                    if let Some(obj) = obj_opt {
                        if obj.is_delete_marker {
                            false
                        } else if obj.lock_legal_hold.as_deref() == Some("ON") {
                            true
                        } else if let (Some(mode), Some(until)) =
                            (&obj.lock_mode, &obj.lock_retain_until)
                        {
                            if *until > Utc::now() {
                                if mode == "COMPLIANCE" {
                                    true
                                } else if mode == "GOVERNANCE" {
                                    !bypass
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if lock_denied {
                    error_xml.push_str(&format!(
                        "<Error><Key>{}</Key><VersionId>{}</VersionId><Code>AccessDenied</Code><Message>Access Denied because object protected by object lock.</Message></Error>",
                        xml_escape(key),
                        xml_escape(vid),
                    ));
                    continue;
                }

                // Delete specific version
                if let Some(versions) = b.object_versions.get_mut(key) {
                    versions.retain(|o| {
                        !(o.version_id.as_deref() == Some(vid)
                            || (vid == "null" && o.version_id.is_none()))
                    });
                    if let Some(latest) = versions.last() {
                        if latest.is_delete_marker {
                            b.objects.remove(key);
                        } else {
                            b.objects.insert(key.to_string(), latest.clone());
                        }
                    } else {
                        b.objects.remove(key);
                    }
                    if versions.is_empty() {
                        b.object_versions.remove(key);
                    }
                }
                deleted_xml.push_str(&format!(
                    "<Deleted><Key>{}</Key><VersionId>{}</VersionId></Deleted>",
                    xml_escape(key),
                    xml_escape(vid),
                ));
            } else if versioning_enabled {
                let dm_id = Uuid::new_v4().to_string();
                let marker = make_delete_marker(key, &dm_id);
                b.object_versions
                    .entry(key.to_string())
                    .or_default()
                    .push(marker.clone());
                b.objects.insert(key.to_string(), marker);
                deleted_xml.push_str(&format!(
                    "<Deleted><Key>{}</Key><DeleteMarker>true</DeleteMarker><DeleteMarkerVersionId>{}</DeleteMarkerVersionId></Deleted>",
                    xml_escape(key), dm_id,
                ));
            } else {
                b.objects.remove(key);
                deleted_xml.push_str(&format!(
                    "<Deleted><Key>{}</Key></Deleted>",
                    xml_escape(key)
                ));
            }
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {deleted_xml}\
             {error_xml}\
             </DeleteResult>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    // ---- Object ACL ----

    pub(super) fn get_object_attributes(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

        let attrs = req
            .headers
            .get("x-amz-object-attributes")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let mut body_parts = Vec::new();

        for attr in attrs.split(',') {
            let attr = attr.trim();
            match attr {
                "ETag" => {
                    body_parts.push(format!("<ETag>{}</ETag>", xml_escape(&obj.etag)));
                }
                "StorageClass" => {
                    body_parts.push(format!(
                        "<StorageClass>{}</StorageClass>",
                        xml_escape(&obj.storage_class)
                    ));
                }
                "ObjectSize" => {
                    body_parts.push(format!("<ObjectSize>{}</ObjectSize>", obj.size));
                }
                "Checksum" => {
                    if let (Some(algo), Some(val)) = (&obj.checksum_algorithm, &obj.checksum_value)
                    {
                        body_parts.push(format!(
                            "<Checksum><Checksum{algo}>{val}</Checksum{algo}></Checksum>"
                        ));
                    }
                }
                "ObjectParts" => {
                    if let Some(pc) = obj.parts_count {
                        let mut parts_inner = format!("<TotalPartsCount>{pc}</TotalPartsCount>");
                        if let Some(ref ps) = obj.part_sizes {
                            for (pn, sz) in ps {
                                parts_inner.push_str(&format!(
                                    "<Part><PartNumber>{pn}</PartNumber><Size>{sz}</Size></Part>"
                                ));
                            }
                        }
                        body_parts.push(format!("<ObjectParts>{parts_inner}</ObjectParts>"));
                    }
                }
                _ => {}
            }
        }

        let mut headers = HeaderMap::new();
        if let Some(vid) = &obj.version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        headers.insert(
            "last-modified",
            obj.last_modified
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
                .parse()
                .unwrap(),
        );

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {}\
             </GetObjectAttributesResponse>",
            body_parts.join("")
        );
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: body.into(),
            headers,
        })
    }

    pub(super) fn restore_object(
        &self,
        _req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
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
        Ok(AwsResponse {
            status,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }
}
