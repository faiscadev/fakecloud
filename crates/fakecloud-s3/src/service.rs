use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, Method, StatusCode};
use md5::{Digest, Md5};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{S3Bucket, S3Object, SharedS3State};

pub struct S3Service {
    state: SharedS3State,
}

impl S3Service {
    pub fn new(state: SharedS3State) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for S3Service {
    fn service_name(&self) -> &str {
        "s3"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // S3 REST routing: method + path segments + query params
        let bucket = req.path_segments.first().map(|s| s.as_str());
        let key = if req.path_segments.len() > 1 {
            Some(req.path_segments[1..].join("/"))
        } else {
            None
        };

        match (&req.method, bucket, key.as_deref()) {
            // ListBuckets: GET /
            (&Method::GET, None, None) => self.list_buckets(&req),

            // Bucket-level operations (no key)
            (&Method::PUT, Some(b), None) => {
                if req.query_params.contains_key("tagging") {
                    self.put_bucket_tagging(&req, b)
                } else {
                    self.create_bucket(&req, b)
                }
            }
            (&Method::DELETE, Some(b), None) => {
                if req.query_params.contains_key("tagging") {
                    self.delete_bucket_tagging(&req, b)
                } else {
                    self.delete_bucket(&req, b)
                }
            }
            (&Method::HEAD, Some(b), None) => self.head_bucket(b),
            (&Method::GET, Some(b), None) => {
                if req.query_params.contains_key("tagging") {
                    self.get_bucket_tagging(&req, b)
                } else if req.query_params.contains_key("location") {
                    self.get_bucket_location(b)
                } else {
                    self.list_objects_v2(&req, b)
                }
            }

            // Object-level operations
            (&Method::PUT, Some(b), Some(k)) => {
                if req.headers.contains_key("x-amz-copy-source") {
                    self.copy_object(&req, b, k)
                } else {
                    self.put_object(&req, b, k)
                }
            }
            (&Method::GET, Some(b), Some(k)) => self.get_object(b, k),
            (&Method::DELETE, Some(b), Some(k)) => self.delete_object(b, k),
            (&Method::HEAD, Some(b), Some(k)) => self.head_object(b, k),

            // POST /{bucket}?delete — batch delete
            (&Method::POST, Some(b), None) if req.query_params.contains_key("delete") => {
                self.delete_objects(&req, b)
            }

            _ => Err(AwsServiceError::aws_error(
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowed",
                "The specified method is not allowed against this resource",
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "ListBuckets",
            "CreateBucket",
            "DeleteBucket",
            "HeadBucket",
            "ListObjectsV2",
            "PutObject",
            "GetObject",
            "DeleteObject",
            "HeadObject",
            "CopyObject",
            "DeleteObjects",
            "GetBucketLocation",
            "GetBucketTagging",
            "PutBucketTagging",
            "DeleteBucketTagging",
        ]
    }
}

// ---------------------------------------------------------------------------
// Bucket operations
// ---------------------------------------------------------------------------
impl S3Service {
    fn list_buckets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut buckets_xml = String::new();
        let mut sorted: Vec<_> = state.buckets.values().collect();
        sorted.sort_by_key(|b| &b.name);
        for b in sorted {
            buckets_xml.push_str(&format!(
                "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
                xml_escape(&b.name),
                b.creation_date.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Owner><ID>{account}</ID><DisplayName>{account}</DisplayName></Owner>\
             <Buckets>{buckets_xml}</Buckets>\
             </ListAllMyBucketsResult>",
            account = req.account_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }

    fn create_bucket(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        if !is_valid_bucket_name(bucket) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                format!("The specified bucket is not valid: {bucket}"),
            ));
        }

        let mut state = self.state.write();
        if state.buckets.contains_key(bucket) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "BucketAlreadyOwnedByYou",
                "Your previous request to create the named bucket succeeded and you already own it.",
            ));
        }
        state
            .buckets
            .insert(bucket.to_string(), S3Bucket::new(bucket, &req.region));

        let mut headers = HeaderMap::new();
        headers.insert("location", format!("/{bucket}").parse().unwrap());
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers,
        })
    }

    fn delete_bucket(
        &self,
        _req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        if !b.objects.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "BucketNotEmpty",
                "The bucket you tried to delete is not empty",
            ));
        }
        state.buckets.remove(bucket);
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn head_bucket(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        if !state.buckets.contains_key(bucket) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                format!("The specified bucket does not exist: {bucket}"),
            ));
        }
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn get_bucket_location(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let loc = if b.region == "us-east-1" {
            String::new()
        } else {
            b.region.clone()
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{loc}</LocationConstraint>"
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }

    fn list_objects_v2(
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

        let effective_start = continuation.as_deref().unwrap_or(&start_after);

        let mut contents = String::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut count = 0;
        let mut is_truncated = false;
        let mut last_key = String::new();

        for (key, obj) in &b.objects {
            if !key.starts_with(&prefix) {
                continue;
            }
            if !effective_start.is_empty() && key.as_str() <= effective_start {
                continue;
            }

            // Handle delimiter-based grouping
            if !delimiter.is_empty() {
                let suffix = &key[prefix.len()..];
                if let Some(pos) = suffix.find(&delimiter) {
                    let cp = format!("{}{}", prefix, &suffix[..=pos]);
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

            contents.push_str(&format!(
                "<Contents>\
                 <Key>{}</Key>\
                 <LastModified>{}</LastModified>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <StorageClass>{}</StorageClass>\
                 </Contents>",
                xml_escape(key),
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
            common_prefixes_xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(cp),
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

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{bucket}</Name>\
             <Prefix>{prefix}</Prefix>\
             <KeyCount>{count}</KeyCount>\
             <MaxKeys>{max_keys}</MaxKeys>\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {cont_token}\
             {next_token}\
             {contents}\
             {common_prefixes_xml}\
             </ListBucketResult>",
            prefix = xml_escape(&prefix),
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }

    fn get_bucket_tagging(
        &self,
        _req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        if b.tags.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchTagSet",
                "The TagSet does not exist",
            ));
        }
        let mut tags_xml = String::new();
        for (k, v) in &b.tags {
            tags_xml.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                xml_escape(k),
                xml_escape(v),
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <TagSet>{tags_xml}</TagSet></Tagging>"
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }

    fn put_bucket_tagging(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let tags = parse_tagging_xml(body_str);

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.tags = tags;
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn delete_bucket_tagging(
        &self,
        _req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.tags.clear();
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// Object operations
// ---------------------------------------------------------------------------
impl S3Service {
    fn put_object(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let data = req.body.clone();
        let etag = compute_md5(&data);
        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();

        let metadata = extract_user_metadata(&req.headers);

        let obj = S3Object {
            key: key.to_string(),
            size: data.len() as u64,
            data,
            content_type,
            etag: etag.clone(),
            last_modified: Utc::now(),
            metadata,
            storage_class: "STANDARD".to_string(),
        };
        b.objects.insert(key.to_string(), obj);

        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{etag}\"").parse().unwrap());
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers,
        })
    }

    fn get_object(&self, bucket: &str, key: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

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
        headers.insert("content-length", obj.size.to_string().parse().unwrap());
        for (k, v) in &obj.metadata {
            if let (Ok(name), Ok(val)) = (
                format!("x-amz-meta-{k}").parse::<http::header::HeaderName>(),
                v.parse::<http::header::HeaderValue>(),
            ) {
                headers.insert(name, val);
            }
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: obj.content_type.clone(),
            body: obj.data.clone(),
            headers,
        })
    }

    fn delete_object(&self, bucket: &str, key: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // S3 returns 204 even if the key doesn't exist
        b.objects.remove(key);
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn head_object(&self, bucket: &str, key: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

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
        headers.insert("content-length", obj.size.to_string().parse().unwrap());

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: obj.content_type.clone(),
            body: Bytes::new(),
            headers,
        })
    }

    fn copy_object(
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

        // Decode percent-encoded source and strip leading /
        let decoded = percent_encoding::percent_decode_str(copy_source)
            .decode_utf8_lossy()
            .to_string();
        let source = decoded.strip_prefix('/').unwrap_or(&decoded);
        let (src_bucket, src_key) = source.split_once('/').ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid copy source format",
            )
        })?;

        let mut state = self.state.write();

        // Read source object
        let src_obj = {
            let sb = state
                .buckets
                .get(src_bucket)
                .ok_or_else(|| no_such_bucket(src_bucket))?;
            sb.objects
                .get(src_key)
                .ok_or_else(|| no_such_key(src_key))?
                .clone()
        };

        let etag = src_obj.etag.clone();
        let last_modified = Utc::now();

        // Write to destination
        let db = state
            .buckets
            .get_mut(dest_bucket)
            .ok_or_else(|| no_such_bucket(dest_bucket))?;
        db.objects.insert(
            dest_key.to_string(),
            S3Object {
                key: dest_key.to_string(),
                last_modified,
                ..src_obj
            },
        );

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <CopyObjectResult>\
             <ETag>&quot;{etag}&quot;</ETag>\
             <LastModified>{}</LastModified>\
             </CopyObjectResult>",
            last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }

    fn delete_objects(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let keys = parse_delete_objects_xml(body_str);

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let mut deleted_xml = String::new();
        for key in &keys {
            b.objects.remove(key);
            deleted_xml.push_str(&format!(
                "<Deleted><Key>{}</Key></Deleted>",
                xml_escape(key),
            ));
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {deleted_xml}\
             </DeleteResult>"
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn no_such_bucket(bucket: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        format!("The specified bucket does not exist: {bucket}"),
    )
}

fn no_such_key(key: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        format!("The specified key does not exist: {key}"),
    )
}

fn compute_md5(data: &[u8]) -> String {
    let digest = Md5::digest(data);
    format!("{:x}", digest)
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn extract_user_metadata(headers: &HeaderMap) -> std::collections::HashMap<String, String> {
    let mut meta = std::collections::HashMap::new();
    for (name, value) in headers {
        if let Some(key) = name.as_str().strip_prefix("x-amz-meta-") {
            if let Ok(v) = value.to_str() {
                meta.insert(key.to_string(), v.to_string());
            }
        }
    }
    meta
}

fn is_valid_bucket_name(name: &str) -> bool {
    if name.len() < 3 || name.len() > 63 {
        return false;
    }
    // Must start and end with alphanumeric
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
        return false;
    }
    // Only lowercase letters, digits, hyphens, dots
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
}

/// Minimal XML parser for `<Delete><Object><Key>...</Key></Object>...</Delete>`.
fn parse_delete_objects_xml(xml: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut remaining = xml;
    while let Some(start) = remaining.find("<Key>") {
        let after = &remaining[start + 5..];
        if let Some(end) = after.find("</Key>") {
            keys.push(after[..end].to_string());
            remaining = &after[end + 6..];
        } else {
            break;
        }
    }
    keys
}

/// Minimal XML parser for `<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag>...`.
fn parse_tagging_xml(xml: &str) -> std::collections::HashMap<String, String> {
    let mut tags = std::collections::HashMap::new();
    let mut remaining = xml;
    while let Some(tag_start) = remaining.find("<Tag>") {
        let after = &remaining[tag_start + 5..];
        if let Some(tag_end) = after.find("</Tag>") {
            let tag_body = &after[..tag_end];
            let key = extract_xml_value(tag_body, "Key");
            let value = extract_xml_value(tag_body, "Value");
            if let (Some(k), Some(v)) = (key, value) {
                tags.insert(k, v);
            }
            remaining = &after[tag_end + 6..];
        } else {
            break;
        }
    }
    tags
}

fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml.find(&close)?;
    Some(xml[start..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_bucket_names() {
        assert!(is_valid_bucket_name("my-bucket"));
        assert!(is_valid_bucket_name("my.bucket.name"));
        assert!(is_valid_bucket_name("abc"));
        assert!(!is_valid_bucket_name("ab"));
        assert!(!is_valid_bucket_name("-bucket"));
        assert!(!is_valid_bucket_name("Bucket"));
        assert!(!is_valid_bucket_name("bucket-"));
    }

    #[test]
    fn parse_delete_xml() {
        let xml = r#"<Delete><Object><Key>a.txt</Key></Object><Object><Key>b/c.txt</Key></Object></Delete>"#;
        let keys = parse_delete_objects_xml(xml);
        assert_eq!(keys, vec!["a.txt", "b/c.txt"]);
    }

    #[test]
    fn parse_tags_xml() {
        let xml =
            r#"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"#;
        let tags = parse_tagging_xml(xml);
        assert_eq!(tags.get("env").unwrap(), "prod");
    }

    #[test]
    fn md5_hash() {
        let hash = compute_md5(b"hello");
        assert_eq!(hash, "5d41402abc4b2a76b9719d911017c592");
    }
}
