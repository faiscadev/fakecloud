//! `S3Service` `list` family — extracted from service.rs by audit-2026-05-19.

use super::*;

/// Parse the `max-keys` query parameter. Absent -> AWS's default page size of
/// 1000. Present-but-invalid (non-integer, negative, or outside the signed
/// 32-bit range AWS accepts) -> 400 `InvalidArgument`, rather than the silent
/// coercion-to-1000 that used to mask client bugs. A valid value is capped at
/// 1000, the most keys AWS returns in one page.
fn parse_max_keys(req: &AwsRequest) -> Result<usize, AwsServiceError> {
    let raw = match req.query_params.get("max-keys") {
        Some(v) => v,
        None => return Ok(1000),
    };
    match raw.trim().parse::<i64>() {
        Ok(n) if (0..=2_147_483_647).contains(&n) => Ok((n as usize).min(1000)),
        _ => Err(AwsServiceError::aws_error_with_fields(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "Provided max-keys not an integer or within integer range",
            vec![
                ("ArgumentName".to_string(), "max-keys".to_string()),
                ("ArgumentValue".to_string(), raw.clone()),
            ],
        )),
    }
}

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
        // AWS caps max-keys at 1000 regardless of the requested value, so
        // clients that don't paginate still see truncation (bug-audit
        // 2026-05-28, 1.4 — we used the requested value verbatim). An invalid
        // value is a 400, not a silent 1000.
        let max_keys = parse_max_keys(req)?;
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
            if !marker.is_empty() {
                // A marker that is a CommonPrefix (ends with the delimiter) must
                // skip every key under it, or the next page re-emits the prefix.
                let under_resumed_prefix = delimiter.as_deref().is_some_and(|d| {
                    !d.is_empty() && marker.ends_with(d) && key.starts_with(marker.as_str())
                });
                if key.as_str() <= marker.as_str() || under_resumed_prefix {
                    continue;
                }
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
                            // Cursor is the CommonPrefix so the next page resumes
                            // past the whole group, not at its first member.
                            last_key = cp.clone();
                            common_prefixes.push(cp);
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

        // max-keys=0 returns an empty page with IsTruncated=false (no marker),
        // not a truncated empty NextMarker.
        if max_keys == 0 {
            is_truncated = false;
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

        // Per the S3 ListObjects (v1) contract, NextMarker is only returned
        // for a truncated listing when a Delimiter was supplied. Without a
        // delimiter the client must resume from the last returned key itself,
        // and AWS omits NextMarker entirely. Emitting it unconditionally
        // misleads clients that key off its presence.
        let next_marker = if is_truncated && delimiter.is_some() {
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
        // AWS caps max-keys at 1000 (bug-audit 2026-05-28, 1.4); an invalid
        // value is a 400, not a silent 1000.
        let max_keys = parse_max_keys(req)?;
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
            if !effective_start.is_empty() {
                // When the cursor is a CommonPrefix (it ends with the
                // delimiter), skip every key rolled into that prefix — otherwise
                // resuming re-emits the same CommonPrefix on the next page.
                let under_resumed_prefix = !delimiter.is_empty()
                    && effective_start.ends_with(delimiter.as_str())
                    && key.starts_with(effective_start);
                if key.as_str() <= effective_start || under_resumed_prefix {
                    continue;
                }
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
                        // Cursor is the CommonPrefix itself so the next page
                        // resumes past the whole group, not at its first member.
                        last_key = cp.clone();
                        common_prefixes.push(cp);
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

        // max-keys=0 is a valid "give me just the count/metadata" request: AWS
        // returns an empty page with IsTruncated=false, not a truncated empty
        // continuation token (which the next request would reject).
        if max_keys == 0 {
            is_truncated = false;
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
        // AWS caps max-keys at 1000 (bug-audit 2026-05-28, 1.4); an invalid
        // value is a 400, not a silent 1000.
        let max_keys = parse_max_keys(req)?;

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
                // If the marker is itself a delimiter-rolled CommonPrefix (it
                // ends with the delimiter), skip every entry that rolls into it
                // so resuming a page that ended on a CommonPrefix doesn't
                // re-emit that same prefix.
                if let Some(ref delim) = delimiter {
                    if key_marker.ends_with(delim.as_str()) && key.starts_with(key_marker.as_str())
                    {
                        return false;
                    }
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

        // encoding-type=url: AWS url-encodes the key-type fields (Key, Prefix,
        // KeyMarker, NextKeyMarker, Delimiter, CommonPrefixes.Prefix) so keys
        // with XML-illegal control characters are still parseable. Unlike
        // ListObjectsV1/V2, this op previously ignored the parameter.
        let use_url = req.query_params.get("encoding-type").map(|s| s.as_str()) == Some("url");
        let enc = |s: &str| {
            if use_url {
                url_encode_s3_key(s)
            } else {
                xml_escape(s)
            }
        };

        // Single sorted pass interleaving version entries and delimiter-rolled
        // CommonPrefixes, so truncation lands at a consistent point in the
        // sorted stream and a CommonPrefix that sorts before the truncation
        // boundary is never dropped across pages. The previous approach rolled
        // every prefix up front and appended them AFTER the entries, so entries
        // filling max-keys could push a lower-sorting prefix off the page while
        // NextKeyMarker advanced past it — losing that prefix entirely. This
        // mirrors the ListObjectsV1/V2 single-pass model.
        let mut versions_xml = String::new();
        let mut cp_xml = String::new();
        let mut seen_prefixes = std::collections::HashSet::new();
        let mut count = 0usize;
        let mut is_truncated = false;
        // Cursor for the next page: (key, Some(version_id)) for a version
        // entry, (common_prefix, None) for a rolled-up prefix (AWS omits
        // NextVersionIdMarker when the page ends on a CommonPrefix).
        let mut next_markers: Option<(String, Option<String>)> = None;

        for (key, obj, is_latest) in &all_entries {
            let rolled = delimiter.as_ref().and_then(|delim| {
                let after_prefix = &key[prefix.len()..];
                after_prefix
                    .find(delim.as_str())
                    .map(|pos| format!("{}{}", prefix, &after_prefix[..pos + delim.len()]))
            });

            if let Some(cp) = rolled {
                if seen_prefixes.contains(&cp) {
                    continue; // this CommonPrefix was already emitted
                }
                if count >= max_keys {
                    is_truncated = true;
                    break;
                }
                cp_xml.push_str(&format!(
                    "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                    enc(&cp),
                ));
                next_markers = Some((cp.clone(), None));
                seen_prefixes.insert(cp);
                count += 1;
                continue;
            }

            if count >= max_keys {
                is_truncated = true;
                break;
            }
            if obj.is_delete_marker {
                versions_xml.push_str(&format!(
                    "<DeleteMarker>\
                     <Key>{}</Key>\
                     <VersionId>{}</VersionId>\
                     <IsLatest>{}</IsLatest>\
                     <LastModified>{}</LastModified>\
                     <Owner><ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName></Owner>\
                     </DeleteMarker>",
                    enc(key),
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
                    enc(key),
                    obj.version_id.as_deref().unwrap_or("null"),
                    is_latest,
                    obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    obj.etag,
                    obj.size,
                    obj.storage_class,
                ));
            }
            next_markers = Some((
                key.to_string(),
                Some(obj.version_id.as_deref().unwrap_or("null").to_string()),
            ));
            count += 1;
        }

        // Markers are only meaningful for a truncated listing.
        let next_markers = if is_truncated { next_markers } else { None };

        // Pagination markers
        let marker_xml = match &next_markers {
            Some((nk, Some(nv))) => format!(
                "<NextKeyMarker>{}</NextKeyMarker>\
                 <NextVersionIdMarker>{}</NextVersionIdMarker>",
                enc(nk),
                xml_escape(nv),
            ),
            Some((nk, None)) => format!("<NextKeyMarker>{}</NextKeyMarker>", enc(nk)),
            None => String::new(),
        };

        let delimiter_xml = delimiter
            .as_ref()
            .map(|d| format!("<Delimiter>{}</Delimiter>", enc(d)))
            .unwrap_or_default();
        let encoding_xml = if use_url {
            "<EncodingType>url</EncodingType>"
        } else {
            ""
        };

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{name}</Name>\
             <Prefix>{pfx}</Prefix>\
             <KeyMarker>{km}</KeyMarker>\
             {delimiter_xml}\
             <MaxKeys>{max_keys}</MaxKeys>\
             {encoding_xml}\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {marker_xml}\
             {versions_xml}\
             {cp_xml}\
             </ListVersionsResult>",
            name = xml_escape(bucket),
            pfx = enc(&prefix),
            km = enc(&key_marker),
        );
        Ok(s3_xml(StatusCode::OK, body))
    }
}
