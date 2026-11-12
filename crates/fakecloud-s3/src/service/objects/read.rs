//! `S3Service` `read` family — extracted from service.rs by audit-2026-05-19.

use super::*;

/// Insert a response header from a possibly-untrusted string, silently
/// skipping it when the value isn't a legal HTTP header value rather than
/// panicking. Object-lock mode / legal-hold originate from XML request
/// bodies (and persisted state), so a crafted value must never crash the
/// read path. AWS itself only ever stores the validated enum values, so a
/// well-behaved object always round-trips its header.
/// True when the request opted into checksum retrieval via
/// `x-amz-checksum-mode: ENABLED`. AWS only returns a stored object checksum on
/// GetObject/HeadObject when this is set.
fn checksum_mode_enabled(req: &AwsRequest) -> bool {
    req.headers
        .get("x-amz-checksum-mode")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("ENABLED"))
        .unwrap_or(false)
}

/// Insert the stored checksum S3 returns on a GetObject/HeadObject made with
/// checksum mode enabled. A request for one part of a multipart object
/// (`partNumber`) gets that part's checksum; otherwise the object's checksum
/// and its type. Nothing is returned for a part whose checksum is unknown.
fn insert_object_checksum_headers(headers: &mut HeaderMap, obj: &S3Object, part: Option<u32>) {
    let Some(algorithm) = obj.checksum_algorithm else {
        return;
    };
    let checksum_type = obj.effective_checksum_type();
    match (part, obj.parts_count) {
        (Some(pn), Some(_)) => {
            let part_value = obj
                .part_checksums
                .as_ref()
                .and_then(|parts| parts.iter().find(|(n, _)| *n == pn))
                .map(|(_, v)| v.as_str());
            if let Some(value) = part_value {
                crate::checksum::insert_response_headers(headers, algorithm, value, checksum_type);
            }
        }
        _ => {
            if let Some(value) = obj.checksum_value.as_deref() {
                crate::checksum::insert_response_headers(headers, algorithm, value, checksum_type);
            }
        }
    }
}

/// The `ObjectParts` element of GetObjectAttributes for a multipart object:
/// each part's size and, when known, its additional checksum, paged by the
/// `x-amz-max-parts` / `x-amz-part-number-marker` headers.
fn object_parts_xml(
    obj: &S3Object,
    total_parts: u32,
    req: &AwsRequest,
) -> Result<String, AwsServiceError> {
    let header_int = |name: &str, default: i64| -> Result<i64, AwsServiceError> {
        match req.headers.get(name).and_then(|v| v.to_str().ok()) {
            None => Ok(default),
            Some(v) => v
                .trim()
                .parse::<i64>()
                .ok()
                .filter(|n| *n >= 0)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        format!("Provided {name} not an integer or within integer range"),
                    )
                }),
        }
    };
    let max_parts = header_int("x-amz-max-parts", 1000)?;
    let marker = header_int("x-amz-part-number-marker", 0)?;
    let parts: Vec<(u32, u64)> = obj
        .part_sizes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .copied()
        .filter(|(pn, _)| i64::from(*pn) > marker)
        .collect();
    let page = &parts[..parts.len().min(max_parts as usize)];
    let is_truncated = parts.len() > page.len();
    // `TotalPartsCount` is serialized as `PartsCount` (its `@xmlName`).
    let mut out = format!("<PartsCount>{total_parts}</PartsCount>");
    if marker > 0 {
        out.push_str(&format!("<PartNumberMarker>{marker}</PartNumberMarker>"));
    }
    if let Some((last, _)) = page.last() {
        out.push_str(&format!(
            "<NextPartNumberMarker>{last}</NextPartNumberMarker>"
        ));
    }
    out.push_str(&format!(
        "<MaxParts>{max_parts}</MaxParts><IsTruncated>{is_truncated}</IsTruncated>"
    ));
    for (pn, size) in page {
        out.push_str(&format!(
            "<Part><PartNumber>{pn}</PartNumber><Size>{size}</Size>"
        ));
        let part_checksum = obj.checksum_algorithm.zip(
            obj.part_checksums
                .as_ref()
                .and_then(|cs| cs.iter().find(|(n, _)| n == pn))
                .map(|(_, v)| v.as_str()),
        );
        if let Some((algo, value)) = part_checksum {
            out.push_str(&crate::checksum::xml_element(algo, value));
        }
        out.push_str("</Part>");
    }
    Ok(format!("<ObjectParts>{out}</ObjectParts>"))
}

/// The part of a multipart object whose bytes are exactly `start..=end`, if
/// any: S3 returns a part's checksum for a byte range aligned with one part,
/// the same as for a `partNumber` request.
fn part_spanning(obj: &S3Object, start: usize, end: usize) -> Option<u32> {
    let mut offset: u64 = 0;
    for &(pn, size) in obj.part_sizes.as_ref()? {
        if size > 0 && offset == start as u64 && offset + size - 1 == end as u64 {
            return Some(pn);
        }
        offset += size;
    }
    None
}

fn insert_str_header(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(v) = value.parse::<http::header::HeaderValue>() {
        headers.insert(name, v);
    }
}

impl S3Service {
    pub(crate) fn get_object(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        // A missing bucket is `NoSuchBucket` (recorded against us-east-1, signed
        // and anonymous alike), although the Smithy model declares only
        // NoSuchKey + InvalidObjectState: it is one of S3's common errors.
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = resolve_object(b, key, req.query_params.get("versionId"))?;

        // PublicAccessBlock.IgnorePublicAcls: anonymous callers cannot
        // ride a public-read ACL when the bucket has IgnorePublicAcls
        // set. Authenticated callers always pass this gate; the ACL
        // check itself is unchanged for authed paths. A request fakecloud
        // makes as an AWS-owned principal (CloudFront through an origin
        // access control) carries no access key but is not anonymous: it
        // has a principal, and the bucket policy authorized it.
        if req.access_key_id.is_none() && req.principal.is_none() {
            if let Some(xml) = b.public_access_block.as_ref() {
                let flags = crate::service::config::PublicAccessBlockFlags::parse(xml);
                let acl_is_public = obj.acl_grants.iter().chain(b.acl_grants.iter()).any(|g| {
                    g.grantee_type == "Group"
                        && g.grantee_uri
                            .as_deref()
                            .is_some_and(|u| u.contains("acs.amazonaws.com/groups/global/AllUsers"))
                });
                if acl_is_public && flags.ignore_public_acls {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::FORBIDDEN,
                        "AccessDenied",
                        "Access Denied: PublicAccessBlock IgnorePublicAcls is enabled",
                    ));
                }
            }
        }

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
        // SSE-KMS: pre-load and decrypt the full body so we can slice
        // ranged/multi-part reads against plaintext (matching real S3,
        // which transparently decrypts on the read path). Fail-closed —
        // a KMS error surfaces as 500 KMS.InternalFailureException
        // instead of returning the raw envelope to the caller.
        let decrypted_body: Option<Bytes> =
            if crate::side_effects::is_sse_kms(obj.sse_algorithm.as_deref())
                && self.kms_hook.is_some()
            {
                // Offload the (potentially multi-GB) full-object disk read
                // onto a blocking thread so it doesn't stall the tokio worker
                // while we decrypt the envelope, mirroring the multipart
                // assembly path.
                let raw = run_blocking_io(|| state.read_body(&obj.body))
                    .map_err(crate::service::io_to_aws)?;
                Some(self.decrypt_object_body(
                    req,
                    account_id,
                    bucket,
                    &obj.key,
                    obj.bucket_key_enabled == Some(true),
                    &raw,
                    obj.sse_kms_key_id.as_deref(),
                )?)
            } else {
                None
            };
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
        if let Some(status) = obj.replication_status.as_deref() {
            headers.insert("x-amz-replication-status", status.parse().unwrap());
        }
        if let Some(vid) = &obj.version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        // System metadata + response-header overrides. AWS stores these
        // headers verbatim on PutObject and echoes them on GET; the
        // `response-*` query params (presigned-download mechanism) take
        // precedence over the stored value when present.
        let q = |k: &str| req.query_params.get(k).map(|s| s.to_string());
        if let Some(enc) = q("response-content-encoding").or_else(|| obj.content_encoding.clone()) {
            insert_str_header(&mut headers, "content-encoding", &enc);
        }
        if let Some(cc) = q("response-cache-control").or_else(|| obj.cache_control.clone()) {
            insert_str_header(&mut headers, "cache-control", &cc);
        }
        if let Some(cd) =
            q("response-content-disposition").or_else(|| obj.content_disposition.clone())
        {
            insert_str_header(&mut headers, "content-disposition", &cd);
        }
        if let Some(cl) = q("response-content-language").or_else(|| obj.content_language.clone()) {
            insert_str_header(&mut headers, "content-language", &cl);
        }
        if let Some(exp) = q("response-expires").or_else(|| obj.expires.clone()) {
            insert_str_header(&mut headers, "expires", &exp);
        }
        insert_user_metadata_headers(&mut headers, &obj.metadata);
        if let Some(ref redirect) = obj.website_redirect_location {
            insert_str_header(&mut headers, "x-amz-website-redirect-location", redirect);
        }
        if !obj.tags.is_empty() {
            headers.insert(
                "x-amz-tagging-count",
                obj.tags.len().to_string().parse().unwrap(),
            );
        }

        // SSE headers - only when explicitly set. The algorithm / kms key id
        // can originate from a bucket-default-encryption XML body
        // (PutBucketEncryption), so a crafted value must never crash the read
        // path; skip-if-invalid mirrors the object-lock guards below.
        if let Some(algo) = &obj.sse_algorithm {
            insert_str_header(&mut headers, "x-amz-server-side-encryption", algo);
        } else {
            // Every S3 object is encrypted at rest with SSE-S3 (AES256) by
            // default, and AWS reports that on GetObject/HeadObject. The
            // Terraform `aws_s3_object` resource asserts `server_side_encryption
            // = AES256`, so emit it when no other algorithm was set.
            insert_str_header(&mut headers, "x-amz-server-side-encryption", "AES256");
        }
        if let Some(kid) = &obj.sse_kms_key_id {
            insert_str_header(
                &mut headers,
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid,
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
            insert_str_header(&mut headers, "x-amz-object-lock-mode", mode);
        }
        if let Some(ref until) = obj.lock_retain_until {
            headers.insert(
                "x-amz-object-lock-retain-until-date",
                until.to_rfc3339().parse().unwrap(),
            );
        }
        if let Some(ref hold) = obj.lock_legal_hold {
            insert_str_header(&mut headers, "x-amz-object-lock-legal-hold", hold);
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
        let response_body: fakecloud_core::service::ResponseBody;
        let mut is_range_request = false;
        let mut range_span: Option<(usize, usize)> = None;
        if let Some(range_str) = req.headers.get("range").and_then(|v| v.to_str().ok()) {
            if let Some(rr) = parse_range_header(range_str, total_size) {
                match rr {
                    RangeResult::Satisfiable { start, end } => {
                        headers.insert(
                            "content-range",
                            format!("bytes {start}-{end}/{total_size}").parse().unwrap(),
                        );
                        let len = (end - start + 1) as u64;
                        headers.insert("content-length", len.to_string().parse().unwrap());
                        response_body = if let Some(plain) = &decrypted_body {
                            let s = start.min(plain.len());
                            let e = (start + len as usize).min(plain.len());
                            plain.slice(s..e).into()
                        } else {
                            // Seek+read on a blocking thread so a large ranged
                            // read doesn't starve the tokio worker.
                            run_blocking_io(|| state.read_body_range(&obj.body, start as u64, len))
                                .map_err(crate::service::io_to_aws)?
                                .into()
                        };
                        response_status = StatusCode::PARTIAL_CONTENT;
                        is_range_request = true;
                        range_span = Some((start, end));
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
                        response_body = if let Some(plain) = decrypted_body.clone() {
                            plain.into()
                        } else {
                            full_body_response(state, &obj.body)?
                        };
                    }
                }
            } else {
                headers.insert("content-length", total_size.to_string().parse().unwrap());
                response_body = if let Some(plain) = decrypted_body.clone() {
                    plain.into()
                } else {
                    full_body_response(state, &obj.body)?
                };
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
                // A 0-byte part (the single/last part may legitimately be
                // empty) makes `part_start + part_size - 1` underflow — a debug
                // panic / release wrap to a garbage content-range. Saturate so
                // an empty part reports `bytes 0-0/<total>` with length 0.
                let part_end = (part_start + part_size).saturating_sub(1);
                headers.insert(
                    "content-range",
                    format!("bytes {part_start}-{part_end}/{total_size}")
                        .parse()
                        .unwrap(),
                );
                headers.insert("content-length", part_size.to_string().parse().unwrap());
                response_body = if let Some(plain) = &decrypted_body {
                    let s = part_start.min(plain.len());
                    let e = (part_start + part_size).min(plain.len());
                    plain.slice(s..e).into()
                } else {
                    // Seek+read on a blocking thread so a large part read
                    // doesn't starve the tokio worker.
                    run_blocking_io(|| {
                        state.read_body_range(&obj.body, part_start as u64, part_size as u64)
                    })
                    .map_err(crate::service::io_to_aws)?
                    .into()
                };
                response_status = StatusCode::PARTIAL_CONTENT;
            } else {
                headers.insert("content-length", total_size.to_string().parse().unwrap());
                response_body = if let Some(plain) = decrypted_body.clone() {
                    plain.into()
                } else {
                    full_body_response(state, &obj.body)?
                };
            }
        } else {
            headers.insert("content-length", total_size.to_string().parse().unwrap());
            response_body = if let Some(plain) = decrypted_body.clone() {
                plain.into()
            } else {
                full_body_response(state, &obj.body)?
            };
        }
        // Only include checksum headers for full (non-range) responses, and
        // only when the caller opted in with `x-amz-checksum-mode: ENABLED`.
        // AWS does not echo a stored object checksum on a plain GetObject (the
        // Go SDK auto-attaches a CRC32 on PutObject, but Terraform reads the
        // object without enabling checksum mode and expects `checksum_crc32`
        // to be empty).
        if checksum_mode_enabled(req) {
            match range_span {
                // A range returns a checksum only when it is exactly one part.
                Some((start, end)) => {
                    if let Some(pn) = part_spanning(obj, start, end) {
                        insert_object_checksum_headers(&mut headers, obj, Some(pn));
                    }
                }
                None if !is_range_request => {
                    let part = if req.headers.contains_key("range") {
                        None
                    } else {
                        req.query_params
                            .get("partNumber")
                            .and_then(|v| v.parse::<u32>().ok())
                    };
                    insert_object_checksum_headers(&mut headers, obj, part);
                }
                None => {}
            }
        }
        Ok(AwsResponse {
            status: response_status,
            content_type: q("response-content-type").unwrap_or_else(|| obj.content_type.clone()),
            body: response_body,
            headers,
        })
    }

    pub(crate) fn head_object(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        // HeadObject's Smithy model declares only `NotFound` for any
        // missing-target error (com.amazonaws.s3#HeadObject -> errors:
        // [NotFound]). The body is stripped from HEAD responses, so the
        // wire-level signal is the `x-amz-error-code` header. Use the
        // declared code instead of the bucket/object-specific codes so the
        // emitted error matches the model contract.
        let b = state.buckets.get(bucket).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                format!("The specified bucket does not exist: {bucket}"),
            )
        })?;
        let obj = resolve_object(b, key, req.query_params.get("versionId")).map_err(|err| {
            if matches!(err.status(), StatusCode::NOT_FOUND) {
                AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFound", err.message())
            } else {
                err
            }
        })?;
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
                    body: Bytes::new().into(),
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
                body: Bytes::new().into(),
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
        if let Some(status) = obj.replication_status.as_deref() {
            headers.insert("x-amz-replication-status", status.parse().unwrap());
        }
        let hq = |k: &str| req.query_params.get(k).map(|s| s.to_string());
        if let Some(enc) = hq("response-content-encoding").or_else(|| obj.content_encoding.clone())
        {
            insert_str_header(&mut headers, "content-encoding", &enc);
        }
        if let Some(cc) = hq("response-cache-control").or_else(|| obj.cache_control.clone()) {
            insert_str_header(&mut headers, "cache-control", &cc);
        }
        if let Some(cd) =
            hq("response-content-disposition").or_else(|| obj.content_disposition.clone())
        {
            insert_str_header(&mut headers, "content-disposition", &cd);
        }
        if let Some(cl) = hq("response-content-language").or_else(|| obj.content_language.clone()) {
            insert_str_header(&mut headers, "content-language", &cl);
        }
        if let Some(exp) = hq("response-expires").or_else(|| obj.expires.clone()) {
            insert_str_header(&mut headers, "expires", &exp);
        }
        let mut range_span: Option<(usize, usize)> = None;
        if let Some(range_str) = req.headers.get("range").and_then(|v| v.to_str().ok()) {
            if let Some(range_result) = parse_range_header(range_str, total_size as usize) {
                match range_result {
                    RangeResult::Satisfiable { start, end } => {
                        range_span = Some((start, end));
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
                // A 0-byte part (the single/last part may legitimately be
                // empty) makes `part_start + part_size - 1` underflow — a debug
                // panic / release wrap to a garbage content-range. Saturate so
                // an empty part reports `bytes 0-0/<total>` with length 0.
                let part_end = (part_start + part_size).saturating_sub(1);
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
        insert_user_metadata_headers(&mut headers, &obj.metadata);
        if let Some(ref redirect) = obj.website_redirect_location {
            insert_str_header(&mut headers, "x-amz-website-redirect-location", redirect);
        }
        if !obj.tags.is_empty() {
            headers.insert(
                "x-amz-tagging-count",
                obj.tags.len().to_string().parse().unwrap(),
            );
        }

        if let Some(vid) = &obj.version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }

        // SSE headers. As in get_object above, these can carry a
        // bucket-default value sourced from an XML body, so insert through the
        // skip-if-invalid helper rather than .parse().unwrap().
        if let Some(algo) = &obj.sse_algorithm {
            insert_str_header(&mut headers, "x-amz-server-side-encryption", algo);
        } else {
            // Every S3 object is encrypted at rest with SSE-S3 (AES256) by
            // default, and AWS reports that on GetObject/HeadObject. The
            // Terraform `aws_s3_object` resource asserts `server_side_encryption
            // = AES256`, so emit it when no other algorithm was set.
            insert_str_header(&mut headers, "x-amz-server-side-encryption", "AES256");
        }
        if let Some(kid) = &obj.sse_kms_key_id {
            insert_str_header(
                &mut headers,
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid,
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
            insert_str_header(&mut headers, "x-amz-object-lock-mode", mode);
        }
        if let Some(ref until) = obj.lock_retain_until {
            headers.insert(
                "x-amz-object-lock-retain-until-date",
                until.to_rfc3339().parse().unwrap(),
            );
        }
        if let Some(ref hold) = obj.lock_legal_hold {
            insert_str_header(&mut headers, "x-amz-object-lock-legal-hold", hold);
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
        // Checksum headers are only returned when the caller sets
        // `x-amz-checksum-mode: ENABLED`; AWS does not echo them on a plain
        // HeadObject even though one is stored.
        if checksum_mode_enabled(req) {
            match range_span {
                Some((start, end)) => {
                    if let Some(pn) = part_spanning(obj, start, end) {
                        insert_object_checksum_headers(&mut headers, obj, Some(pn));
                    }
                }
                None => {
                    let part = if req.headers.contains_key("range") {
                        None
                    } else {
                        req.query_params
                            .get("partNumber")
                            .and_then(|v| v.parse::<u32>().ok())
                    };
                    insert_object_checksum_headers(&mut headers, obj, part);
                }
            }
        }

        Ok(AwsResponse {
            status: response_status,
            content_type: hq("response-content-type").unwrap_or_else(|| obj.content_type.clone()),
            body: Bytes::new().into(),
            headers,
        })
    }

    // ---- Object ACL ----

    pub(crate) fn get_object_attributes(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        // A missing bucket is `NoSuchBucket`, as for GetObject (S3 common
        // error), though the model declares only NoSuchKey.
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

        // A list header: the SDKs send one `x-amz-object-attributes` line per
        // attribute, other clients one comma-separated line.
        let attrs: Vec<String> = req
            .headers
            .get_all("x-amz-object-attributes")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .map(|a| a.trim().to_string())
            .collect();

        let mut body_parts = Vec::new();

        for attr in &attrs {
            match attr.as_str() {
                "ETag" => {
                    // GetObjectAttributes returns the ETag WITHOUT the
                    // surrounding quotes that GetObject/HeadObject wrap it in
                    // (single-part `md5` and multipart `md5-N` alike).
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
                    if let (Some(algo), Some(val), Some(checksum_type)) = (
                        obj.checksum_algorithm,
                        obj.checksum_value.as_deref(),
                        obj.effective_checksum_type(),
                    ) {
                        body_parts.push(format!(
                            "<Checksum>{}<ChecksumType>{checksum_type}</ChecksumType></Checksum>",
                            crate::checksum::xml_element(algo, val)
                        ));
                    }
                }
                "ObjectParts" => {
                    if let Some(pc) = obj.parts_count {
                        body_parts.push(object_parts_xml(obj, pc, req)?);
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
}
