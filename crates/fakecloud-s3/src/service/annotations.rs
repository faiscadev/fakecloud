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

use crate::checksum::{ChecksumHeaders, ChecksumType};
use crate::persistence::annotation_record;
use crate::state::ObjectAnnotation;

use super::{
    etag_matches, no_such_bucket, no_such_key, precondition_failed, s3_xml, xml_escape, S3Object,
    S3Service,
};

/// AWS caps an object at 1,000 annotations (PutObjectAnnotation API reference).
const MAX_ANNOTATIONS_PER_OBJECT: usize = 1000;
/// `AnnotationName` is capped at 512 bytes.
const MAX_ANNOTATION_NAME_BYTES: usize = 512;
/// An annotation payload is 1 byte to 1 MiB.
const MAX_ANNOTATION_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Wire name of the `AnnotationName` `@httpQuery` member. S3 query keys are
/// case-sensitive, so the member name (`AnnotationName`) is not accepted.
pub(super) const ANNOTATION_NAME_QUERY: &str = "annotationName";
/// Wire names of ListObjectAnnotations' `@httpQuery` paging inputs.
pub(super) const MAX_ANNOTATION_RESULTS_QUERY: &str = "max-annotation-results";
pub(super) const ANNOTATION_PREFIX_QUERY: &str = "annotation-prefix";
pub(super) const CONTINUATION_TOKEN_QUERY: &str = "continuation-token";
/// `ObjectIfMatch`: Put/DeleteObjectAnnotation only proceed when the target
/// object version's ETag matches.
const OBJECT_IF_MATCH_HEADER: &str = "x-amz-object-if-match";
/// The annotation ops report the object version under this header, not the
/// `x-amz-version-id` the object ops use.
const OBJECT_VERSION_ID_HEADER: &str = "x-amz-object-version-id";

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
        .get(ANNOTATION_NAME_QUERY)
        .cloned()
        .unwrap_or_default();
    if name.is_empty() {
        return Err(invalid_annotation_name("AnnotationName is required"));
    }
    if name.len() > MAX_ANNOTATION_NAME_BYTES {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AnnotationNameTooLong",
            format!("The annotation name exceeds {MAX_ANNOTATION_NAME_BYTES} bytes."),
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

/// Enforce `x-amz-object-if-match` against the resolved object version.
fn check_object_if_match(req: &AwsRequest, obj: &S3Object) -> Result<(), AwsServiceError> {
    if let Some(condition) = req
        .headers
        .get(OBJECT_IF_MATCH_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        if !etag_matches(condition, &obj.etag) {
            return Err(precondition_failed(OBJECT_IF_MATCH_HEADER));
        }
    }
    Ok(())
}

fn insert_object_version_id(headers: &mut HeaderMap, version_id: Option<&str>) {
    if let Some(vid) = version_id.and_then(|v| v.parse().ok()) {
        headers.insert(OBJECT_VERSION_ID_HEADER, vid);
    }
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

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // NoSuchBucket / NoSuchKey, then the object precondition (412), all
        // before the payload's digests are judged.
        let obj = resolve_object_mut(b, key, version_id.as_ref())?;
        check_object_if_match(req, obj)?;

        // The payload is a streaming blob: an SDK frames it with aws-chunked
        // and sends its checksum as a trailer, both undone before the handler
        // runs (`deframe_buffered`). Verify Content-MD5 / the x-amz-checksum-*
        // value against the payload, computing the stored checksum in the
        // same pass. Without an algorithm the annotation gets S3's default
        // CRC64NVME checksum.
        let checks = ChecksumHeaders::parse(&req.headers)?;
        let algorithm = checks
            .algorithm()
            .unwrap_or(crate::checksum::DEFAULT_ALGORITHM);
        let digests =
            crate::checksum::verify_buffered(&req.headers, &req.body, &checks, Some(algorithm))?;
        let payload = &req.body;
        if payload.is_empty() || payload.len() > MAX_ANNOTATION_PAYLOAD_BYTES {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "The annotation payload must be between 1 byte and 1 MiB.",
            ));
        }
        if std::str::from_utf8(payload).is_err() {
            return Err(AwsServiceError::aws_error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "UnsupportedMediaType",
                "The annotation payload is not valid UTF-8 encoded text.",
            ));
        }

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

        let etag = annotation_etag(payload);
        let checksum_value = digests.encoded(algorithm);
        let annotation = ObjectAnnotation {
            name: name.clone(),
            payload: payload.to_vec(),
            etag: etag.clone(),
            last_modified: chrono::Utc::now(),
            checksum_algorithm: Some(algorithm),
            checksum_value: checksum_value.clone(),
        };
        // Store-first, and only this annotation: its own record and payload,
        // never the object's metadata or its other annotations.
        self.store
            .put_annotation(
                bucket,
                key,
                obj.version_id.as_deref(),
                &annotation_record(&annotation),
                &annotation.payload,
            )
            .map_err(super::persistence_error)?;
        obj.annotations.insert(name.clone(), annotation);

        let mut headers = HeaderMap::new();
        if let Ok(v) = etag.parse() {
            headers.insert("ETag", v);
        }
        insert_object_version_id(&mut headers, obj.version_id.as_deref());
        if let Some(value) = &checksum_value {
            crate::checksum::insert_response_headers(
                &mut headers,
                algorithm,
                value,
                Some(ChecksumType::FullObject),
            );
        }
        insert_sse_header(&mut headers, obj.sse_algorithm.as_deref());
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
        insert_object_version_id(&mut headers, obj.version_id.as_deref());
        insert_sse_header(&mut headers, obj.sse_algorithm.as_deref());
        if let Some(status) = obj
            .replication_status
            .as_deref()
            .and_then(|v| v.parse().ok())
        {
            headers.insert("x-amz-replication-status", status);
        }
        // `ChecksumMode: ENABLED` asks for the stored checksum back: the
        // payload's digest in the algorithm it was written with.
        if req
            .headers
            .get("x-amz-checksum-mode")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|m| m.eq_ignore_ascii_case("ENABLED"))
        {
            if let Some(algo) = annotation.checksum_algorithm {
                let value = annotation
                    .checksum_value
                    .clone()
                    .unwrap_or_else(|| crate::checksum::checksum(algo, &annotation.payload));
                crate::checksum::insert_response_headers(
                    &mut headers,
                    algo,
                    &value,
                    Some(ChecksumType::FullObject),
                );
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
        check_object_if_match(req, obj)?;
        // AWS deletes idempotently: removing an annotation that is not there
        // is still a 204, the same way DeleteObject is.
        if obj.annotations.contains_key(&name) {
            self.store
                .delete_annotation(bucket, key, obj.version_id.as_deref(), &name)
                .map_err(super::persistence_error)?;
            obj.annotations.remove(&name);
        }
        let object_version_id = obj.version_id.clone();

        let mut headers = HeaderMap::new();
        insert_object_version_id(&mut headers, object_version_id.as_deref());
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
            .get(ANNOTATION_PREFIX_QUERY)
            .cloned()
            .unwrap_or_default();
        if prefix.chars().any(|c| c.is_control()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidPrefix",
                "AnnotationPrefix must not contain control characters",
            ));
        }
        let max_results = match req.query_params.get(MAX_ANNOTATION_RESULTS_QUERY) {
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
        let continuation_token = req.query_params.get(CONTINUATION_TOKEN_QUERY).cloned();

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
            if let Some(algo) = a.checksum_algorithm {
                entries.push_str(&format!("<ChecksumAlgorithm>{algo}</ChecksumAlgorithm>"));
            }
            if let Some(status) = &obj.replication_status {
                entries.push_str(&format!(
                    "<ReplicationStatus>{}</ReplicationStatus>",
                    xml_escape(status)
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
        // `Annotations` is a wrapped list: the entries sit inside it.
        if !entries.is_empty() {
            body.push_str("<Annotations>");
            body.push_str(&entries);
            body.push_str("</Annotations>");
        }
        body.push_str("</ListObjectAnnotationsResult>");

        let mut resp = s3_xml(StatusCode::OK, body);
        insert_object_version_id(&mut resp.headers, obj.version_id.as_deref());
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

/// Annotations inherit their object's encryption; an object without
/// server-side encryption is SSE-S3 (`AES256`), the default for new objects.
fn insert_sse_header(headers: &mut HeaderMap, sse_algorithm: Option<&str>) {
    if let Ok(v) = sse_algorithm.unwrap_or("AES256").parse() {
        headers.insert("x-amz-server-side-encryption", v);
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
            if b.objects.get(key).is_some_and(matches) {
                return b.objects.get_mut(key).ok_or_else(|| no_such_key(key));
            }
            b.object_versions
                .get_mut(key)
                .and_then(|versions| versions.iter_mut().find(|o| matches(o)))
                .ok_or_else(|| no_such_key(key))
        }
    }
}

#[cfg(test)]
mod tests {
    use http::Method;

    use super::*;

    fn req_with_query(pairs: &[(&str, &str)]) -> AwsRequest {
        super::super::tests::make_request(Method::DELETE, "/annot/k", pairs, b"")
    }

    fn err_code(res: Result<String, AwsServiceError>) -> String {
        res.expect_err("expected an error").code().to_string()
    }

    #[test]
    fn annotation_name_is_read_from_the_modeled_wire_key() {
        // The Smithy model binds `AnnotationName` to `@httpQuery("annotationName")`.
        let req = req_with_query(&[("annotation", ""), (ANNOTATION_NAME_QUERY, "review")]);
        assert_eq!(require_annotation_name(&req).unwrap(), "review");
    }

    #[test]
    fn member_name_spelling_is_not_the_wire_key() {
        // Query keys are case-sensitive: `AnnotationName=` carries no name.
        let req = req_with_query(&[("annotation", ""), ("AnnotationName", "review")]);
        assert_eq!(
            err_code(require_annotation_name(&req)),
            "InvalidAnnotationName"
        );
    }

    #[test]
    fn list_reads_only_the_modeled_wire_keys() {
        // ListObjectAnnotations binds its paging inputs to `max-annotation-results`,
        // `annotation-prefix` and `continuation-token`; a member-name spelling is
        // an unrelated query parameter and must be ignored, not honored.
        let svc = super::super::tests::make_service();
        let list = |pairs: &[(&str, &str)]| {
            let req = super::super::tests::make_request(Method::GET, "/annot/k", pairs, b"");
            match svc.list_object_annotations("123456789012", &req, "annot", "k") {
                Ok(_) => panic!("bucket does not exist"),
                Err(e) => e.code().to_string(),
            }
        };
        assert_eq!(
            list(&[("annotation", ""), (MAX_ANNOTATION_RESULTS_QUERY, "0")]),
            "InvalidArgument"
        );
        assert_eq!(
            list(&[("annotation", ""), ("MaxAnnotationResults", "0")]),
            "NoSuchBucket"
        );
        assert_eq!(
            list(&[("annotation", ""), (ANNOTATION_PREFIX_QUERY, "a\u{1}")]),
            "InvalidPrefix"
        );
        assert_eq!(
            list(&[("annotation", ""), ("AnnotationPrefix", "a\u{1}")]),
            "NoSuchBucket"
        );
    }

    // --- checksums, digests and the object precondition ---------------------

    use super::super::tests::{make_service, seed_bucket, seed_object};

    const ACCOUNT: &str = "123456789012";

    fn annotation_req(
        method: Method,
        name: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> AwsRequest {
        let mut req = super::super::tests::make_request(
            method,
            "/annot/k",
            &[("annotation", ""), (ANNOTATION_NAME_QUERY, name)],
            body,
        );
        for (k, v) in headers {
            req.headers
                .insert(k.parse::<http::HeaderName>().unwrap(), v.parse().unwrap());
        }
        req
    }

    fn seeded() -> S3Service {
        let svc = make_service();
        seed_bucket(&svc, "annot");
        seed_object(&svc, "annot", "k", b"object body");
        svc
    }

    fn header(resp: &AwsResponse, name: &str) -> Option<String> {
        resp.headers
            .get(name)
            .map(|v| v.to_str().unwrap().to_string())
    }

    fn expect_err(res: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match res {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        }
    }

    fn object_etag(svc: &S3Service) -> String {
        svc.state.read().get(ACCOUNT).unwrap().buckets["annot"].objects["k"]
            .etag
            .clone()
    }

    #[test]
    fn object_if_match_guards_put_and_delete() {
        let svc = seeded();
        let etag = object_etag(&svc);
        let bare = etag.trim_matches('"').to_string();

        // Put: a mismatching ETag is a 412 and stores nothing.
        let put = annotation_req(
            Method::PUT,
            "n",
            &[(OBJECT_IF_MATCH_HEADER, "\"0000\"")],
            b"v1",
        );
        let err = expect_err(svc.put_object_annotation(ACCOUNT, &put, "annot", "k"));
        assert_eq!(err.code(), "PreconditionFailed");
        assert_eq!(err.status(), StatusCode::PRECONDITION_FAILED);
        let get = annotation_req(Method::GET, "n", &[], b"");
        assert_eq!(
            expect_err(svc.get_object_annotation(ACCOUNT, &get, "annot", "k")).code(),
            "NoSuchAnnotation"
        );

        // The object's ETag, quoted or bare, or `*`, lets the write through.
        for cond in [etag.as_str(), bare.as_str(), "*"] {
            let put = annotation_req(Method::PUT, "n", &[(OBJECT_IF_MATCH_HEADER, cond)], b"v1");
            svc.put_object_annotation(ACCOUNT, &put, "annot", "k")
                .unwrap();
        }

        // Delete: a mismatch is a 412 and the annotation survives.
        let del = annotation_req(
            Method::DELETE,
            "n",
            &[(OBJECT_IF_MATCH_HEADER, "\"0000\"")],
            b"",
        );
        let err = expect_err(svc.delete_object_annotation(ACCOUNT, &del, "annot", "k"));
        assert_eq!(err.code(), "PreconditionFailed");
        svc.get_object_annotation(ACCOUNT, &get, "annot", "k")
            .unwrap();

        let del = annotation_req(Method::DELETE, "n", &[(OBJECT_IF_MATCH_HEADER, &etag)], b"");
        let resp = svc
            .delete_object_annotation(ACCOUNT, &del, "annot", "k")
            .unwrap();
        assert_eq!(resp.status, StatusCode::NO_CONTENT);
        assert_eq!(
            expect_err(svc.get_object_annotation(ACCOUNT, &get, "annot", "k")).code(),
            "NoSuchAnnotation"
        );
    }

    #[test]
    fn object_version_is_reported_under_the_modeled_header() {
        // Every annotation op reports the object version as
        // `x-amz-object-version-id`, not the object ops' `x-amz-version-id`.
        let svc = seeded();
        svc.state
            .write()
            .get_or_create(ACCOUNT)
            .buckets
            .get_mut("annot")
            .unwrap()
            .objects
            .get_mut("k")
            .unwrap()
            .version_id = Some("v1".to_string());

        let check = |resp: AwsResponse| {
            assert_eq!(
                header(&resp, OBJECT_VERSION_ID_HEADER).as_deref(),
                Some("v1")
            );
            assert_eq!(header(&resp, "x-amz-version-id"), None);
        };
        let put = annotation_req(Method::PUT, "n", &[], b"v");
        check(
            svc.put_object_annotation(ACCOUNT, &put, "annot", "k")
                .unwrap(),
        );
        let get = annotation_req(Method::GET, "n", &[], b"");
        check(
            svc.get_object_annotation(ACCOUNT, &get, "annot", "k")
                .unwrap(),
        );
        let list =
            super::super::tests::make_request(Method::GET, "/annot/k", &[("annotation", "")], b"");
        check(
            svc.list_object_annotations(ACCOUNT, &list, "annot", "k")
                .unwrap(),
        );
        let del = annotation_req(Method::DELETE, "n", &[], b"");
        check(
            svc.delete_object_annotation(ACCOUNT, &del, "annot", "k")
                .unwrap(),
        );
    }
}
