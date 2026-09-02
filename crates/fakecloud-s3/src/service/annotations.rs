//! Object annotations: named payloads attached to an object version.
//!
//! `PutObjectAnnotation` / `GetObjectAnnotation` / `DeleteObjectAnnotation` /
//! `ListObjectAnnotations` address a side-car keyed by `AnnotationName` on a
//! single object version. The object's own body, ETag and metadata are never
//! touched — annotations live beside it and are listed, paged and filtered
//! independently.

use base64::Engine;
use http::{HeaderMap, StatusCode};
use md5::{Digest, Md5};

use bytes::Bytes;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::ObjectAnnotation;

use super::{no_such_bucket, no_such_key, s3_xml, xml_escape, S3Service};

/// AWS caps an object at 50 annotations.
const MAX_ANNOTATIONS_PER_OBJECT: usize = 50;
/// `AnnotationName` is capped at 255 characters.
const MAX_ANNOTATION_NAME_LEN: usize = 255;

fn invalid_annotation_name(reason: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidAnnotationName", reason)
}

fn no_such_annotation(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchAnnotation",
        format!("The specified annotation does not exist: {name}"),
    )
}

/// `AnnotationName` is required on Put/Get/Delete, must be non-empty, must fit
/// the documented length, and — like an object key — cannot contain a control
/// character, which could not survive the response headers it is echoed in.
fn require_annotation_name(req: &AwsRequest) -> Result<String, AwsServiceError> {
    let name = req
        .query_params
        .get("AnnotationName")
        .cloned()
        .unwrap_or_default();
    if name.is_empty() {
        return Err(invalid_annotation_name("AnnotationName is required"));
    }
    if name.chars().count() > MAX_ANNOTATION_NAME_LEN {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AnnotationNameTooLong",
            format!("AnnotationName exceeds {MAX_ANNOTATION_NAME_LEN} characters"),
        ));
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(invalid_annotation_name(
            "AnnotationName must not contain control characters",
        ));
    }
    Ok(name)
}

fn annotation_etag(payload: &[u8]) -> String {
    format!("\"{:x}\"", Md5::digest(payload))
}

impl S3Service {
    pub(super) fn put_object_annotation(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_annotation_name(req)?;
        let version_id = req.query_params.get("versionId").cloned();
        let payload = req.body.to_vec();
        let checksum_algorithm = req
            .headers
            .get("x-amz-sdk-checksum-algorithm")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = resolve_object_mut(b, key, version_id.as_ref())?;

        // The cap counts distinct names; overwriting an existing annotation is
        // always allowed.
        if !obj.annotations.contains_key(&name)
            && obj.annotations.len() >= MAX_ANNOTATIONS_PER_OBJECT
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AnnotationLimitExceeded",
                format!(
                    "An object cannot carry more than {MAX_ANNOTATIONS_PER_OBJECT} annotations"
                ),
            ));
        }

        let etag = annotation_etag(&payload);
        let object_version_id = obj.version_id.clone();
        obj.annotations.insert(
            name.clone(),
            ObjectAnnotation {
                name: name.clone(),
                payload,
                etag: etag.clone(),
                last_modified: chrono::Utc::now(),
                checksum_algorithm,
            },
        );

        let mut headers = HeaderMap::new();
        if let Ok(v) = etag.parse() {
            headers.insert("ETag", v);
        }
        if let Some(vid) = object_version_id.as_deref().and_then(|v| v.parse().ok()) {
            headers.insert("x-amz-version-id", vid);
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <PutObjectAnnotationResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Key>{}</Key><AnnotationName>{}</AnnotationName></PutObjectAnnotationResult>",
            xml_escape(key),
            xml_escape(&name),
        );
        let mut resp = s3_xml(StatusCode::OK, body);
        resp.headers.extend(headers);
        Ok(resp)
    }

    pub(super) fn get_object_annotation(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_annotation_name(req)?;
        let version_id = req.query_params.get("versionId").cloned();

        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = super::resolve_object(b, key, version_id.as_ref())?;
        let annotation = obj
            .annotations
            .get(&name)
            .ok_or_else(|| no_such_annotation(&name))?;

        let mut headers = HeaderMap::new();
        if let Ok(v) = annotation.etag.parse() {
            headers.insert("ETag", v);
        }
        if let Ok(v) = annotation.payload.len().to_string().parse() {
            headers.insert("Content-Length", v);
        }
        if let Ok(v) = annotation
            .last_modified
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string()
            .parse()
        {
            headers.insert("Last-Modified", v);
        }
        if let Some(vid) = obj.version_id.as_deref().and_then(|v| v.parse().ok()) {
            headers.insert("x-amz-version-id", vid);
        }
        // `ChecksumMode: ENABLED` asks for the stored checksum back.
        if req
            .headers
            .get("x-amz-checksum-mode")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|m| m.eq_ignore_ascii_case("ENABLED"))
        {
            if let Some(algo) = annotation.checksum_algorithm.as_deref() {
                let digest = base64::engine::general_purpose::STANDARD
                    .encode(Md5::digest(&annotation.payload));
                if let (Ok(header), Ok(value)) = (
                    format!("x-amz-checksum-{}", algo.to_lowercase()).parse::<http::HeaderName>(),
                    digest.parse(),
                ) {
                    headers.insert(header, value);
                }
            }
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/octet-stream".to_string(),
            headers,
            body: fakecloud_core::service::ResponseBody::Bytes(Bytes::from(
                annotation.payload.clone(),
            )),
        })
    }

    pub(super) fn delete_object_annotation(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_annotation_name(req)?;
        let version_id = req.query_params.get("versionId").cloned();

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = resolve_object_mut(b, key, version_id.as_ref())?;
        // AWS deletes idempotently: removing an annotation that is not there
        // is still a 204, the same way DeleteObject is.
        obj.annotations.remove(&name);
        let object_version_id = obj.version_id.clone();

        let mut headers = HeaderMap::new();
        if let Some(vid) = object_version_id.as_deref().and_then(|v| v.parse().ok()) {
            headers.insert("x-amz-version-id", vid);
        }
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            headers,
            body: fakecloud_core::service::ResponseBody::Bytes(Bytes::new()),
        })
    }

    pub(super) fn list_object_annotations(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version_id = req.query_params.get("versionId").cloned();
        let prefix = req
            .query_params
            .get("annotation-prefix")
            .or_else(|| req.query_params.get("AnnotationPrefix"))
            .cloned()
            .unwrap_or_default();
        if prefix.chars().any(|c| c.is_control()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidPrefix",
                "AnnotationPrefix must not contain control characters",
            ));
        }
        let max_results = match req
            .query_params
            .get("max-annotation-results")
            .or_else(|| req.query_params.get("MaxAnnotationResults"))
        {
            Some(v) => match v.parse::<i64>() {
                Ok(n) if (1..=1000).contains(&n) => n as usize,
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "MaxAnnotationResults must be between 1 and 1000",
                    ))
                }
            },
            None => 1000,
        };
        let continuation_token = req
            .query_params
            .get("continuation-token")
            .or_else(|| req.query_params.get("ContinuationToken"))
            .cloned();

        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = super::resolve_object(b, key, version_id.as_ref())?;

        // Names sort naturally (BTreeMap), so a continuation token is just the
        // last name returned — the next page starts after it.
        let after = match continuation_token.as_deref() {
            None => None,
            Some(tok) => Some(
                String::from_utf8(
                    base64::engine::general_purpose::STANDARD
                        .decode(tok)
                        .map_err(|_| {
                            AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "InvalidArgument",
                                "The continuation token provided is incorrect",
                            )
                        })?,
                )
                .map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "The continuation token provided is incorrect",
                    )
                })?,
            ),
        };

        let matching: Vec<&ObjectAnnotation> = obj
            .annotations
            .values()
            .filter(|a| a.name.starts_with(&prefix))
            .filter(|a| after.as_deref().is_none_or(|t| a.name.as_str() > t))
            .collect();
        let page: Vec<&ObjectAnnotation> = matching.iter().copied().take(max_results).collect();
        let next_token = if matching.len() > page.len() {
            page.last()
                .map(|a| base64::engine::general_purpose::STANDARD.encode(a.name.as_bytes()))
        } else {
            None
        };

        let mut entries = String::new();
        for a in &page {
            entries.push_str(&format!(
                "<AnnotationEntry><AnnotationName>{}</AnnotationName>\
                 <LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size>",
                xml_escape(&a.name),
                a.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                xml_escape(&a.etag),
                a.payload.len(),
            ));
            if let Some(algo) = &a.checksum_algorithm {
                entries.push_str(&format!(
                    "<ChecksumAlgorithm>{}</ChecksumAlgorithm>",
                    xml_escape(algo)
                ));
            }
            entries.push_str("</AnnotationEntry>");
        }

        let mut body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListObjectAnnotationsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Bucket>{}</Bucket><Key>{}</Key><AnnotationPrefix>{}</AnnotationPrefix>\
             <MaxAnnotationResults>{}</MaxAnnotationResults><AnnotationCount>{}</AnnotationCount>",
            xml_escape(bucket),
            xml_escape(key),
            xml_escape(&prefix),
            max_results,
            page.len(),
        );
        if let Some(tok) = &continuation_token {
            body.push_str(&format!(
                "<ContinuationToken>{}</ContinuationToken>",
                xml_escape(tok)
            ));
        }
        if let Some(tok) = &next_token {
            body.push_str(&format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                xml_escape(tok)
            ));
        }
        body.push_str(&entries);
        body.push_str("</ListObjectAnnotationsResult>");

        let mut resp = s3_xml(StatusCode::OK, body);
        if let Some(vid) = obj.version_id.as_deref().and_then(|v| v.parse().ok()) {
            resp.headers.insert("x-amz-version-id", vid);
        }
        Ok(resp)
    }

    /// `UpdateBucketMetadataAnnotationTableConfiguration` records which
    /// annotations a bucket surfaces in its metadata table. fakecloud stores
    /// the configuration verbatim so it round-trips.
    pub(super) fn update_bucket_metadata_annotation_table_configuration(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = String::from_utf8_lossy(&req.body).into_owned();
        if body.trim().is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "AnnotationTableConfiguration is required",
            ));
        }
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.annotation_table_config = Some(body);
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            headers: HeaderMap::new(),
            body: fakecloud_core::service::ResponseBody::Bytes(Bytes::new()),
        })
    }
}

/// Mutable twin of [`super::resolve_object`]: the annotation write paths need
/// the owning object version, not a copy of it.
fn resolve_object_mut<'a>(
    b: &'a mut crate::state::S3Bucket,
    key: &str,
    version_id: Option<&String>,
) -> Result<&'a mut crate::state::S3Object, AwsServiceError> {
    match version_id {
        None => b.objects.get_mut(key).ok_or_else(|| no_such_key(key)),
        Some(vid) => {
            let wants_null = vid == "null";
            let matches = |o: &crate::state::S3Object| {
                if wants_null {
                    o.version_id.is_none() || o.version_id.as_deref() == Some("null")
                } else {
                    o.version_id.as_deref() == Some(vid.as_str())
                }
            };
            if b.objects.get(key).is_some_and(&matches) {
                return b.objects.get_mut(key).ok_or_else(|| no_such_key(key));
            }
            b.object_versions
                .get_mut(key)
                .and_then(|versions| versions.iter_mut().find(|o| matches(o)))
                .ok_or_else(|| no_such_key(key))
        }
    }
}
