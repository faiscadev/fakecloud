//! `S3Service` `list` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    pub(crate) fn list_objects_v1(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        // Treat empty `delimiter` as absent.
        let delimiter = req
            .query_params
            .get("delimiter")
            .filter(|s| !s.is_empty())
            .cloned();
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

            // ListObjectsV1 always emits Owner per Contents (no fetch-owner toggle).
            contents.push_str(&format!(
                "<Contents>\
                 <Key>{}</Key>\
                 <LastModified>{}</LastModified>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <StorageClass>{}</StorageClass>\
                 <Owner><ID>{owner}</ID><DisplayName>{owner}</DisplayName></Owner>\
                 </Contents>",
                display_key,
                obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                obj.etag,
                obj.size,
                obj.storage_class,
                owner = xml_escape(&b.acl_owner_id),
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

    pub(crate) fn list_objects_v2(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
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

        // continuation token is base64(URL_SAFE_NO_PAD)-encoded key on the way
        // out; decode it on the way back in. Fall back to treating it as a raw
        // key for forward-compat with clients that don't round-trip.
        let decoded_continuation = continuation.as_ref().map(|ct| {
            use base64::Engine;
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(ct.as_bytes())
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
                .unwrap_or_else(|| ct.clone())
        });
        let effective_start = decoded_continuation
            .as_deref()
            .unwrap_or(start_after.as_str());

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

        // NextContinuationToken must be opaque and safe for query-string
        // round-trip. Base64-encode the last_key so keys with `&`/`=`/spaces
        // don't break the next page.
        let next_token = if is_truncated {
            use base64::Engine;
            let encoded =
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(last_key.as_bytes());
            format!("<NextContinuationToken>{encoded}</NextContinuationToken>")
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

    pub(crate) fn list_object_versions(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        // Treat empty `delimiter` as absent.
        let delimiter = req
            .query_params
            .get("delimiter")
            .filter(|s| !s.is_empty())
            .cloned();
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

        // Pagination: truncate at max_keys (count versions + delete markers + common prefixes).
        // Both `all_entries` and `common_prefixes` count against max_keys —
        // truncate each so the combined emit length never exceeds the cap.
        let total_items = all_entries.len() + common_prefixes.len();
        let is_truncated = total_items > max_keys;
        let truncated_entries: Vec<_> = all_entries.iter().take(max_keys).collect();
        let remaining_slots = max_keys.saturating_sub(truncated_entries.len());
        common_prefixes.truncate(remaining_slots);
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
}
