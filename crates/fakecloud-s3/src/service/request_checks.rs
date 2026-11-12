//! Request-level checks S3 applies before an operation reads or writes
//! anything: header values outside their enum, an object key past the length
//! limit, a configuration request without its body. Each is keyed on the
//! operation the request was classified as ([`super::route::classify`]).
//!
//! Every code and message here was recorded against real S3 (us-east-1,
//! 2026-11-09) with raw signed requests, so each refusal matches AWS byte for
//! byte in code and message. Headers S3 does not validate (an unrecognized
//! `x-amz-request-payer` is ignored: PutObject/GetObject/HeadObject/
//! ListObjectsV2/DeleteObject/GetObjectTagging all answered 2xx) are not
//! checked here.

use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsServiceError};

use super::route::S3Op;

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

/// Checks that need nothing but the request and the operation it was
/// classified as ([`super::route::classify`]), addressing `key`.
pub(super) fn check_request(
    req: &AwsRequest,
    key: Option<&str>,
    op: S3Op,
) -> Result<(), AwsServiceError> {
    // Recorded on CreateMultipartUpload, DeleteObject and GetObject with a
    // 1025-byte key: 400 KeyTooLongError "Your key is too long".
    if key.is_some_and(|k| k.len() > MAX_KEY_BYTES) {
        return Err(bad_request("KeyTooLongError", "Your key is too long"));
    }

    let object_write = matches!(op, S3Op::PutObject | S3Op::CopyObject);
    let create_multipart_upload = op == S3Op::CreateMultipartUpload;
    if object_write || create_multipart_upload {
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

    if create_multipart_upload
        && header(req, "x-amz-storage-class").is_some_and(|v| !super::is_valid_storage_class(v))
    {
        // Recorded: 400 InvalidStorageClass, as PutObject already answers.
        return Err(bad_request(
            "InvalidStorageClass",
            "The storage class you specified is not valid",
        ));
    }

    if op == S3Op::CopyObject
        && header(req, "x-amz-metadata-directive").is_some_and(|v| v != "COPY" && v != "REPLACE")
    {
        // Recorded on CopyObject.
        return Err(bad_request(
            "InvalidArgument",
            "Unknown metadata directive.",
        ));
    }

    if op == S3Op::CopyObject {
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

    // Every GetObject / HeadObject, presigned or with any other parameters.
    if matches!(op, S3Op::GetObject | S3Op::HeadObject)
        && header(req, "x-amz-checksum-mode").is_some_and(|v| v != "ENABLED")
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
    if matches!(
        op,
        S3Op::ListObjects
            | S3Op::ListObjectsV2
            | S3Op::ListObjectVersions
            | S3Op::ListMultipartUploads
    ) && req
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
    if op == S3Op::CreateBucket {
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
    if op == S3Op::PutBucketLifecycleConfiguration {
        if let Some(v) = header(req, "x-amz-transition-default-minimum-object-size") {
            if v != "varies_by_storage_class" && v != "all_storage_classes_128K" {
                return Err(bad_request(
                    "InvalidRequest",
                    format!("Invalid TransitionDefaultMinimumObjectSize found: {v}"),
                ));
            }
        }
    }

    // Half an upload part's address (`partNumber` or `uploadId` alone) is
    // refused by the classification itself.

    Ok(())
}

/// The configuration writes that must carry a body, with the error S3
/// returns when it is empty (all recorded with an empty body, and a matching
/// CRC32 where the operation requires a checksum), plus PutObjectTagging
/// (recorded: 400 MissingRequestBodyError).
fn empty_body_error(op: S3Op) -> Option<AwsServiceError> {
    let missing = || bad_request("MissingRequestBodyError", "Request Body is empty");
    let malformed = || {
        bad_request(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our \
             published schema",
        )
    };
    use S3Op::*;
    match op {
        PutBucketEncryption
        | PutBucketTagging
        | PutObjectTagging
        | PutBucketRequestPayment
        | UpdateBucketMetadataInventoryTableConfiguration
        | UpdateBucketMetadataJournalTableConfiguration
        | CreateBucketMetadataConfiguration
        | CreateBucketMetadataTableConfiguration => Some(missing()),
        PutBucketAnalyticsConfiguration
        | PutBucketMetricsConfiguration
        | PutBucketInventoryConfiguration
        | PutBucketIntelligentTieringConfiguration => Some(malformed()),
        _ => None,
    }
}

/// Checks that apply once the addressed bucket is known to exist, so a
/// request to a missing bucket still answers NoSuchBucket.
pub(super) fn check_existing_bucket_request(
    req: &AwsRequest,
    op: S3Op,
) -> Result<(), AwsServiceError> {
    match empty_body_error(op) {
        Some(err) if req.body.is_empty() => Err(err),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::Method;

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

    /// The operation `r` addresses on bucket `b` and `key`.
    fn op_of(r: &AwsRequest, key: Option<&str>) -> S3Op {
        super::super::route::classify(&r.method, Some("b"), key, &r.query_params, &r.headers)
            .unwrap()
    }

    fn check(r: &AwsRequest, key: Option<&str>) -> Result<(), AwsServiceError> {
        check_request(r, key, op_of(r, key))
    }

    fn check_existing(r: &AwsRequest, key: Option<&str>) -> Result<(), AwsServiceError> {
        check_existing_bucket_request(r, op_of(r, key))
    }

    /// Keyed on the operation, not the query's spelling: a presigned
    /// GetObject (its query full of `X-Amz-*`) is still an object read.
    #[test]
    fn presigned_reads_are_object_reads() {
        let r = req(
            Method::GET,
            &[("X-Amz-Credential", "c"), ("X-Amz-Signature", "s")],
            &[("x-amz-checksum-mode", "x")],
        );
        assert_eq!(
            code(check(&r, Some("k"))).as_deref(),
            Some("InvalidRequest")
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
    fn empty_configuration_bodies() {
        let enc = req(Method::PUT, &[("encryption", "")], &[]);
        assert_eq!(
            code(check_existing(&enc, None)).as_deref(),
            Some("MissingRequestBodyError")
        );
        let tag = req(Method::PUT, &[("tagging", "")], &[]);
        assert_eq!(
            code(check_existing(&tag, Some("k"))).as_deref(),
            Some("MissingRequestBodyError")
        );
        let inv = req(Method::PUT, &[("inventory", ""), ("id", "i")], &[]);
        assert_eq!(
            code(check_existing(&inv, None)).as_deref(),
            Some("MalformedXML")
        );
        let meta = req(Method::POST, &[("metadataTable", "")], &[]);
        assert_eq!(
            code(check_existing(&meta, None)).as_deref(),
            Some("MissingRequestBodyError")
        );
        // A body present is the handler's to judge.
        let mut full = req(Method::PUT, &[("encryption", "")], &[]);
        full.body = Bytes::from_static(b"<x/>");
        assert!(check_existing(&full, None).is_ok());
        // ACL bodies are optional (headers can carry the grant), and an
        // object's encryption update is not a bucket configuration.
        let acl = req(Method::PUT, &[("acl", "")], &[]);
        assert!(check_existing(&acl, None).is_ok());
        let update = req(Method::PUT, &[("encryption", "")], &[]);
        assert!(check_existing(&update, Some("k")).is_ok());
    }
}
