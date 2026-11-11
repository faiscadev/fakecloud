//! Request-level checks S3 applies before an operation reads or writes
//! anything: header values outside their enum, an object key past the length
//! limit, a multipart query that names only half of an upload part, a
//! configuration request without its body.
//!
//! Every code and message here was recorded against real S3 (us-east-1,
//! 2026-11-09) with raw signed requests, so each refusal matches AWS byte for
//! byte in code and message. Headers S3 does not validate (an unrecognized
//! `x-amz-request-payer` is ignored: PutObject/GetObject/HeadObject/
//! ListObjectsV2/DeleteObject/GetObjectTagging all answered 2xx) are not
//! checked here.

use http::{Method, StatusCode};

use fakecloud_core::service::{AwsRequest, AwsServiceError};

/// S3's object-key limit in UTF-8 bytes.
const MAX_KEY_BYTES: usize = 1024;

/// The `ServerSideEncryption` enum's wire values.
const SSE_VALUES: &[&str] = &["AES256", "aws:fsx", "aws:backup", "aws:kms", "aws:kms:dsse"];

fn header<'a>(req: &'a AwsRequest, name: &str) -> Option<&'a str> {
    req.headers.get(name).and_then(|v| v.to_str().ok())
}

fn bad_request(code: &str, message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, message)
}

/// What a PUT to an object key is. Classified once per request (by
/// [`classify_key_put`]) and shared by dispatch, which streams the body of the
/// uploads instead of buffering it, and by the checks below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyPut {
    /// Not a PUT to an object key.
    None,
    /// PutObject: the body is the object's data.
    PutObject,
    /// CopyObject: no body, the data comes from `x-amz-copy-source`.
    CopyObject,
    /// UploadPart or UploadPartCopy: `partNumber` and `uploadId` together.
    UploadPart,
    /// PutObjectAnnotation: the body is the annotation's payload.
    Annotation,
    /// A sub-resource PUT with a small XML body (tagging, acl, retention,
    /// legal hold, rename, encryption), or half an upload-part address.
    SubResource,
}

/// The object sub-resources a PUT names in its query.
const OBJECT_SUBRESOURCES: &[&str] = &[
    "tagging",
    "acl",
    "retention",
    "legal-hold",
    "renameObject",
    "encryption",
];

/// Classify `req` as a PUT to an object key (`has_key`) or not.
pub(super) fn classify_key_put(req: &AwsRequest, has_key: bool) -> KeyPut {
    if req.method != Method::PUT || !has_key {
        return KeyPut::None;
    }
    let q = |name: &str| req.query_params.contains_key(name);
    if q("annotation") {
        KeyPut::Annotation
    } else if OBJECT_SUBRESOURCES.iter().any(|name| q(name)) {
        KeyPut::SubResource
    } else {
        match (q("partNumber"), q("uploadId")) {
            (true, true) => KeyPut::UploadPart,
            (false, false) if req.headers.contains_key("x-amz-copy-source") => KeyPut::CopyObject,
            (false, false) => KeyPut::PutObject,
            _ => KeyPut::SubResource,
        }
    }
}

/// Which request shape this is, as far as the checks below care.
struct Shape<'a> {
    req: &'a AwsRequest,
    has_key: bool,
    put: KeyPut,
}

impl Shape<'_> {
    fn query(&self, name: &str) -> bool {
        self.req.query_params.contains_key(name)
    }

    /// PutObject or CopyObject: a PUT to a key with no sub-resource.
    fn is_object_write(&self) -> bool {
        matches!(self.put, KeyPut::PutObject | KeyPut::CopyObject)
    }

    fn is_create_multipart_upload(&self) -> bool {
        self.req.method == Method::POST && self.has_key && self.query("uploads")
    }

    /// GetObject or HeadObject.
    fn is_object_read(&self) -> bool {
        (self.req.method == Method::GET || self.req.method == Method::HEAD)
            && self.has_key
            && self.req.query_params.keys().all(|k| {
                // Only response overrides, versionId, partNumber and the SDK's
                // routing hint can accompany a plain object read.
                k.starts_with("response-") || k == "versionId" || k == "partNumber" || k == "x-id"
            })
    }
}

/// Checks that need nothing but the request. `put` is the request's
/// [`classify_key_put`] for `key`.
pub(super) fn check_request(
    req: &AwsRequest,
    key: Option<&str>,
    put: KeyPut,
) -> Result<(), AwsServiceError> {
    let shape = Shape {
        req,
        has_key: key.is_some(),
        put,
    };

    // Recorded on CreateMultipartUpload, DeleteObject and GetObject with a
    // 1025-byte key: 400 KeyTooLongError "Your key is too long".
    if key.is_some_and(|k| k.len() > MAX_KEY_BYTES) {
        return Err(bad_request("KeyTooLongError", "Your key is too long"));
    }

    if shape.is_object_write() || shape.is_create_multipart_upload() {
        // Recorded on PutObject, CopyObject and CreateMultipartUpload.
        if header(req, "x-amz-server-side-encryption").is_some_and(|v| !SSE_VALUES.contains(&v)) {
            return Err(bad_request(
                "InvalidArgument",
                "The encryption method specified is not supported",
            ));
        }
        // Recorded on PutObject and CreateMultipartUpload.
        if header(req, "x-amz-object-lock-event-hold").is_some_and(|v| v != "ON" && v != "OFF") {
            return Err(bad_request(
                "InvalidArgument",
                "Event hold must be either of 'ON' or 'OFF'",
            ));
        }
    }

    if shape.is_create_multipart_upload()
        && header(req, "x-amz-storage-class").is_some_and(|v| !super::is_valid_storage_class(v))
    {
        // Recorded: 400 InvalidStorageClass, as PutObject already answers.
        return Err(bad_request(
            "InvalidStorageClass",
            "The storage class you specified is not valid",
        ));
    }

    if shape.is_object_write()
        && req.headers.contains_key("x-amz-copy-source")
        && header(req, "x-amz-metadata-directive").is_some_and(|v| v != "COPY" && v != "REPLACE")
    {
        // Recorded on CopyObject.
        return Err(bad_request(
            "InvalidArgument",
            "Unknown metadata directive.",
        ));
    }

    if shape.is_object_write() && req.headers.contains_key("x-amz-copy-source") {
        // CopyObject's other two directives are enums the same way: S3 answers
        // a value outside them with the 400 InvalidArgument it gives an
        // unknown metadata directive, naming the directive.
        if header(req, "x-amz-tagging-directive").is_some_and(|v| v != "COPY" && v != "REPLACE") {
            return Err(bad_request("InvalidArgument", "Unknown tagging directive."));
        }
        if header(req, "x-amz-object-annotation-directive")
            .is_some_and(|v| v != "COPY" && v != "EXCLUDE")
        {
            return Err(bad_request(
                "InvalidArgument",
                "Unknown annotation directive.",
            ));
        }
    }

    if shape.is_object_read() && header(req, "x-amz-checksum-mode").is_some_and(|v| v != "ENABLED")
    {
        // Recorded on GetObject (HeadObject answers the same 400 without a
        // body, as for every HEAD error).
        return Err(bad_request(
            "InvalidRequest",
            "Value for x-amz-checksum-mode header is invalid.",
        ));
    }

    // Recorded on ListObjects, ListObjectsV2, ListObjectVersions and
    // ListMultipartUploads: every bucket listing takes `encoding-type`, and
    // `url` is its only value.
    if req.method == Method::GET
        && !shape.has_key
        && req
            .query_params
            .get("encoding-type")
            .is_some_and(|v| v != "url")
    {
        return Err(bad_request(
            "InvalidArgument",
            "Invalid Encoding Method specified in Request",
        ));
    }

    // Recorded on CreateBucket, for a new bucket and for one the caller
    // already owns.
    if req.method == Method::PUT && !shape.has_key && req.query_params.is_empty() {
        if let Some(ns) = header(req, "x-amz-bucket-namespace") {
            if ns != "account-regional" && ns != "global" {
                return Err(bad_request(
                    "InvalidNamespaceHeader",
                    format!(
                        "The x-amz-bucket-namespace header value {ns} is not valid. Only \
                         account-regional or global is supported."
                    ),
                ));
            }
        }
    }

    // Recorded on PutBucketLifecycleConfiguration.
    if req.method == Method::PUT && !shape.has_key && shape.query("lifecycle") {
        if let Some(v) = header(req, "x-amz-transition-default-minimum-object-size") {
            if v != "varies_by_storage_class" && v != "all_storage_classes_128K" {
                return Err(bad_request(
                    "InvalidRequest",
                    format!("Invalid TransitionDefaultMinimumObjectSize found: {v}"),
                ));
            }
        }
    }

    // An upload part is addressed by both `partNumber` and `uploadId`.
    // Recorded: a PUT to a key with `partNumber` alone is 400 InvalidArgument,
    // and one with `uploadId` alone is 405 MethodNotAllowed.
    if req.method == Method::PUT && shape.has_key {
        match (shape.query("partNumber"), shape.query("uploadId")) {
            (true, false) => {
                return Err(bad_request(
                    "InvalidArgument",
                    "This operation does not accept partNumber without uploadId",
                ))
            }
            (false, true) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "MethodNotAllowed",
                    "The specified method is not allowed against this resource.",
                ))
            }
            _ => {}
        }
    }

    Ok(())
}

/// The bucket sub-resources whose PUT/POST must carry a body, with the error
/// S3 returns when it is empty (all recorded with an empty body, and a
/// matching CRC32 where the operation requires a checksum).
fn empty_body_error(req: &AwsRequest) -> Option<AwsServiceError> {
    let missing = || bad_request("MissingRequestBodyError", "Request Body is empty");
    let malformed = || {
        bad_request(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our \
             published schema",
        )
    };
    let q = |name: &str| req.query_params.contains_key(name);
    match req.method {
        Method::PUT => {
            if q("encryption")
                || q("tagging")
                || q("requestPayment")
                || q("metadataInventoryTable")
                || q("metadataJournalTable")
            {
                Some(missing())
            } else if q("analytics") || q("metrics") || q("inventory") || q("intelligent-tiering") {
                Some(malformed())
            } else {
                None
            }
        }
        Method::POST if q("metadataConfiguration") || q("metadataTable") => Some(missing()),
        _ => None,
    }
}

/// Checks that apply once the addressed bucket is known to exist, so a
/// request to a missing bucket still answers NoSuchBucket.
pub(super) fn check_existing_bucket_request(
    req: &AwsRequest,
    key: Option<&str>,
) -> Result<(), AwsServiceError> {
    if !req.body.is_empty() {
        return Ok(());
    }
    // Bucket-level sub-resources, plus PutObjectTagging (recorded: 400
    // MissingRequestBodyError) on a key.
    let applies = match key {
        None => true,
        Some(_) => req.method == Method::PUT && req.query_params.contains_key("tagging"),
    };
    match empty_body_error(req) {
        Some(err) if applies => Err(err),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(method: Method, query: &[(&str, &str)], headers: &[(&str, &str)]) -> AwsRequest {
        let mut r = super::super::tests::make_request(method, "/b", query, b"");
        for (k, v) in headers {
            r.headers.insert(
                http::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        r
    }

    fn check(r: &AwsRequest, key: Option<&str>) -> Result<(), AwsServiceError> {
        check_request(r, key, classify_key_put(r, key.is_some()))
    }

    #[test]
    fn key_puts_are_classified_by_sub_resource_and_copy_source() {
        let classify = |query: &[(&str, &str)], headers: &[(&str, &str)]| {
            classify_key_put(&req(Method::PUT, query, headers), true)
        };
        assert_eq!(classify(&[], &[]), KeyPut::PutObject);
        assert_eq!(
            classify(&[], &[("x-amz-copy-source", "b/k")]),
            KeyPut::CopyObject
        );
        let part = [("partNumber", "1"), ("uploadId", "u")];
        assert_eq!(classify(&part, &[]), KeyPut::UploadPart);
        assert_eq!(
            classify(&part, &[("x-amz-copy-source", "b/k")]),
            KeyPut::UploadPart
        );
        assert_eq!(classify(&[("partNumber", "1")], &[]), KeyPut::SubResource);
        assert_eq!(classify(&[("tagging", "")], &[]), KeyPut::SubResource);
        assert_eq!(classify(&[("annotation", "")], &[]), KeyPut::Annotation);
        assert_eq!(
            classify_key_put(&req(Method::PUT, &[], &[]), false),
            KeyPut::None
        );
        assert_eq!(
            classify_key_put(&req(Method::GET, &[], &[]), true),
            KeyPut::None
        );
    }

    fn code(r: Result<(), AwsServiceError>) -> Option<String> {
        r.err().map(|e| e.code().to_string())
    }

    #[test]
    fn key_past_the_limit_is_too_long() {
        let r = req(Method::DELETE, &[], &[]);
        let long = "k".repeat(1025);
        assert_eq!(
            code(check(&r, Some(&long))).as_deref(),
            Some("KeyTooLongError")
        );
        assert!(check(&r, Some(&"k".repeat(1024))).is_ok());
    }

    #[test]
    fn unknown_encryption_method_is_refused_on_object_writes() {
        let h = [("x-amz-server-side-encryption", "bogus")];
        let put = req(Method::PUT, &[], &h);
        assert_eq!(
            code(check(&put, Some("k"))).as_deref(),
            Some("InvalidArgument")
        );
        let cmu = req(Method::POST, &[("uploads", "")], &h);
        assert_eq!(
            code(check(&cmu, Some("k"))).as_deref(),
            Some("InvalidArgument")
        );
        let ok = req(
            Method::PUT,
            &[],
            &[("x-amz-server-side-encryption", "aws:kms")],
        );
        assert!(check(&ok, Some("k")).is_ok());
    }

    #[test]
    fn event_hold_storage_class_and_metadata_directive_values() {
        let hold = req(Method::PUT, &[], &[("x-amz-object-lock-event-hold", "x")]);
        assert_eq!(
            code(check(&hold, Some("k"))).as_deref(),
            Some("InvalidArgument")
        );
        let sc = req(
            Method::POST,
            &[("uploads", "")],
            &[("x-amz-storage-class", "x")],
        );
        assert_eq!(
            code(check(&sc, Some("k"))).as_deref(),
            Some("InvalidStorageClass")
        );
        let md = req(
            Method::PUT,
            &[],
            &[
                ("x-amz-copy-source", "b/k"),
                ("x-amz-metadata-directive", "x"),
            ],
        );
        assert_eq!(
            code(check(&md, Some("k"))).as_deref(),
            Some("InvalidArgument")
        );
        // Not a copy: the directive is not read.
        let plain = req(Method::PUT, &[], &[("x-amz-metadata-directive", "x")]);
        assert!(check(&plain, Some("k")).is_ok());
        for directive in [
            "x-amz-tagging-directive",
            "x-amz-object-annotation-directive",
        ] {
            let bad = req(
                Method::PUT,
                &[],
                &[("x-amz-copy-source", "b/k"), (directive, "x")],
            );
            assert_eq!(
                code(check(&bad, Some("k"))).as_deref(),
                Some("InvalidArgument"),
                "{directive}"
            );
            let plain = req(Method::PUT, &[], &[(directive, "x")]);
            assert!(check(&plain, Some("k")).is_ok(), "{directive}");
        }
        for (directive, value) in [
            ("x-amz-tagging-directive", "COPY"),
            ("x-amz-tagging-directive", "REPLACE"),
            ("x-amz-object-annotation-directive", "COPY"),
            ("x-amz-object-annotation-directive", "EXCLUDE"),
        ] {
            let ok = req(
                Method::PUT,
                &[],
                &[("x-amz-copy-source", "b/k"), (directive, value)],
            );
            assert!(check(&ok, Some("k")).is_ok(), "{directive}: {value}");
        }
    }

    #[test]
    fn request_payer_value_is_not_validated() {
        let r = req(Method::GET, &[], &[("x-amz-request-payer", "bogus")]);
        assert!(check(&r, Some("k")).is_ok());
    }

    #[test]
    fn checksum_mode_must_be_enabled_on_object_reads() {
        let r = req(Method::HEAD, &[], &[("x-amz-checksum-mode", "x")]);
        assert_eq!(
            code(check(&r, Some("k"))).as_deref(),
            Some("InvalidRequest")
        );
        let ok = req(Method::GET, &[], &[("x-amz-checksum-mode", "ENABLED")]);
        assert!(check(&ok, Some("k")).is_ok());
    }

    #[test]
    fn listing_encoding_type_must_be_url() {
        for q in [
            vec![],
            vec![("list-type", "2")],
            vec![("versions", "")],
            vec![("uploads", "")],
        ] {
            let mut q = q;
            q.push(("encoding-type", "x"));
            let r = req(Method::GET, &q, &[]);
            assert_eq!(code(check(&r, None)).as_deref(), Some("InvalidArgument"));
        }
        let ok = req(Method::GET, &[("encoding-type", "url")], &[]);
        assert!(check(&ok, None).is_ok());
    }

    #[test]
    fn bucket_namespace_and_lifecycle_tdmos_values() {
        let ns = req(Method::PUT, &[], &[("x-amz-bucket-namespace", "x")]);
        assert_eq!(
            code(check(&ns, None)).as_deref(),
            Some("InvalidNamespaceHeader")
        );
        let ok = req(Method::PUT, &[], &[("x-amz-bucket-namespace", "global")]);
        assert!(check(&ok, None).is_ok());
        let lc = req(
            Method::PUT,
            &[("lifecycle", "")],
            &[("x-amz-transition-default-minimum-object-size", "x")],
        );
        let err = check(&lc, None).unwrap_err();
        assert_eq!(err.code(), "InvalidRequest");
        assert!(err
            .message()
            .contains("Invalid TransitionDefaultMinimumObjectSize found: x"));
    }

    #[test]
    fn half_an_upload_part_address_is_refused() {
        let pn = req(Method::PUT, &[("partNumber", "1")], &[]);
        assert_eq!(
            code(check(&pn, Some("k"))).as_deref(),
            Some("InvalidArgument")
        );
        let up = req(Method::PUT, &[("uploadId", "u")], &[]);
        let err = check(&up, Some("k")).unwrap_err();
        assert_eq!(err.status(), StatusCode::METHOD_NOT_ALLOWED);
        let both = req(Method::PUT, &[("partNumber", "1"), ("uploadId", "u")], &[]);
        assert!(check(&both, Some("k")).is_ok());
    }

    #[test]
    fn empty_configuration_bodies() {
        let enc = req(Method::PUT, &[("encryption", "")], &[]);
        assert_eq!(
            code(check_existing_bucket_request(&enc, None)).as_deref(),
            Some("MissingRequestBodyError")
        );
        let tag = req(Method::PUT, &[("tagging", "")], &[]);
        assert_eq!(
            code(check_existing_bucket_request(&tag, Some("k"))).as_deref(),
            Some("MissingRequestBodyError")
        );
        let inv = req(Method::PUT, &[("inventory", ""), ("id", "i")], &[]);
        assert_eq!(
            code(check_existing_bucket_request(&inv, None)).as_deref(),
            Some("MalformedXML")
        );
        let meta = req(Method::POST, &[("metadataTable", "")], &[]);
        assert_eq!(
            code(check_existing_bucket_request(&meta, None)).as_deref(),
            Some("MissingRequestBodyError")
        );
        // A body present is the handler's to judge.
        let mut full = req(Method::PUT, &[("encryption", "")], &[]);
        full.body = Bytes::from_static(b"<x/>");
        assert!(check_existing_bucket_request(&full, None).is_ok());
        // ACL bodies are optional (headers can carry the grant).
        let acl = req(Method::PUT, &[("acl", "")], &[]);
        assert!(check_existing_bucket_request(&acl, None).is_ok());
    }
}
