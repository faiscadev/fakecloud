use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, StatusCode};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{MultipartUpload, S3Object, UploadPart};

use md5::{Digest, Md5};

use super::{
    canned_acl_grants, compute_checksum, compute_md5, extract_user_metadata, no_such_bucket, no_such_key, no_such_upload,
    parse_complete_multipart_xml, parse_grant_headers,
    parse_url_encoded_tags, precondition_failed, resolve_object, s3_xml,
    xml_escape, S3Service,
};

impl S3Service {
    pub(super) fn create_multipart_upload(
        &self,
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

        let mut state = self.state.write();
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
        b.multipart_uploads.insert(upload_id.clone(), upload);

        let mut headers = HeaderMap::new();
        if let Some(algo) = &sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        }
        if let Some(kid) = &sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
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

    pub(super) fn upload_part(
        &self,
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

        let data = req.body.clone();
        let etag = compute_md5(&data);

        let mut state = self.state.write();
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

        let part = UploadPart {
            part_number: pn,
            data: data.clone(),
            etag: etag.clone(),
            size: data.len() as u64,
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
            body: Bytes::new(),
            headers,
        })
    }

    pub(super) fn upload_part_copy(
        &self,
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

        let mut state = self.state.write();
        let src_data = {
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

            if let Some(range_str) = copy_range {
                let range_part = range_str.strip_prefix("bytes=").unwrap_or(range_str);
                if let Some((start_str, end_str)) = range_part.split_once('-') {
                    let start: usize = start_str.parse().unwrap_or(0);
                    let end: usize = end_str.parse().unwrap_or(src_obj.data.len() - 1);
                    let end = std::cmp::min(end + 1, src_obj.data.len());
                    src_obj.data.slice(start..end)
                } else {
                    src_obj.data.clone()
                }
            } else {
                src_obj.data.clone()
            }
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

        let part = UploadPart {
            part_number: part_number as u32,
            data: src_data,
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

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let upload = match b.multipart_uploads.get(upload_id) {
            Some(u) => u.clone(),
            None => {
                // Upload already completed - return existing object if it exists
                // IfNoneMatch does NOT apply to re-completions
                if let Some(obj) = b.objects.get(key) {
                    let etag = obj.etag.clone();
                    let body = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                         <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                         <Bucket>{}</Bucket>\
                         <Key>{}</Key>\
                         <ETag>&quot;{}&quot;</ETag>\
                         </CompleteMultipartUploadResult>",
                        xml_escape(bucket),
                        xml_escape(key),
                        xml_escape(&etag),
                    );
                    return Ok(AwsResponse {
                        status: StatusCode::OK,
                        content_type: "application/xml".to_string(),
                        body: body.into(),
                        headers: HeaderMap::new(),
                    });
                }
                return Err(no_such_upload(upload_id));
            }
        };

        if upload.key != key {
            return Err(no_such_upload(upload_id));
        }

        // IfNoneMatch: if "*" and object already exists, reject (only for real completions)
        if let Some(ref inm) = if_none_match {
            if inm == "*" && b.objects.contains_key(key) {
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
                    if part.data.len() < MIN_PART_SIZE {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "EntityTooSmall",
                            "Your proposed upload is smaller than the minimum allowed object size.",
                        ));
                    }
                }
            }
        }

        // Assemble the object from parts
        let mut combined_data = Vec::new();
        let mut md5_digests = Vec::new();
        let mut part_sizes = Vec::new();

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
            combined_data.extend_from_slice(&part.data);
            let part_md5 = Md5::digest(&part.data);
            md5_digests.extend_from_slice(&part_md5);
            part_sizes.push((*part_num, part.data.len() as u64));
        }

        // Multipart ETag: MD5(concat(part_md5_digests))-N
        let combined_md5 = Md5::digest(&md5_digests);
        let etag = format!("{:x}-{}", combined_md5, sorted_parts.len());
        let checksum_value = upload
            .checksum_algorithm
            .as_deref()
            .map(|algo| compute_checksum(algo, &combined_data));
        let data = Bytes::from(combined_data);

        let tags = if let Some(ref tagging) = upload.tagging {
            parse_url_encoded_tags(tagging).into_iter().collect()
        } else {
            std::collections::HashMap::new()
        };

        let version_id = if b.versioning.as_deref() == Some("Enabled") {
            Some(uuid::Uuid::new_v4().to_string())
        } else {
            None
        };

        let obj = S3Object {
            key: key.to_string(),
            size: data.len() as u64,
            data,
            content_type: upload.content_type.clone(),
            etag: etag.clone(),
            last_modified: Utc::now(),
            metadata: upload.metadata.clone(),
            storage_class: upload.storage_class.clone(),
            tags,
            acl_grants: upload.acl_grants.clone(),
            acl_owner_id: Some(b.acl_owner_id.clone()),
            parts_count: Some(sorted_parts.len() as u32),
            part_sizes: Some(part_sizes),
            sse_algorithm: upload.sse_algorithm.clone(),
            sse_kms_key_id: upload.sse_kms_key_id.clone(),
            bucket_key_enabled: None,
            version_id: version_id.clone(),
            is_delete_marker: false,
            content_encoding: None,
            website_redirect_location: None,
            restore_ongoing: None,
            restore_expiry: None,
            checksum_algorithm: upload.checksum_algorithm.clone(),
            checksum_value,
            lock_mode: None,
            lock_retain_until: None,
            lock_legal_hold: None,
        };
        b.objects.insert(key.to_string(), obj);
        b.multipart_uploads.remove(upload_id);

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

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Bucket>{}</Bucket>\
             <Key>{}</Key>\
             <ETag>&quot;{}&quot;</ETag>\
             </CompleteMultipartUploadResult>",
            xml_escape(bucket),
            xml_escape(key),
            xml_escape(&etag),
        );
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: body.into(),
            headers,
        })
    }

    pub(super) fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

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
        b.multipart_uploads.remove(upload_id);

        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    pub(super) fn list_multipart_uploads(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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

        let state = self.state.read();
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
