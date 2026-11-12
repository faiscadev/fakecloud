//! The one classification of an S3 REST request into the operation it
//! invokes.
//!
//! Authorization ([`super::iam_actions`]) and the handler's routing both take
//! the operation from [`classify`], on the same bucket and key
//! ([`super::request_key`]), so the operation dispatch authorizes is always
//! the operation the handler runs. Requests S3 refuses before authorizing
//! anything are refused here, for every caller, with S3's recorded errors:
//!
//! - two or more resource-type query parameters (other than `partNumber`
//!   with `uploadId`, an upload part's address): 400 `InvalidArgument`
//!   "Conflicting query string parameters: <names, sorted>";
//! - a resource-type parameter with a method (or target) it does not support:
//!   405 `MethodNotAllowed` naming the method and the resource type;
//! - an upload part whose `partNumber` is not an integer in 1..=10000:
//!   400 `InvalidArgument` "Part number must be an integer between 1 and
//!   10000, inclusive".
//!
//! The resource-type parameters are the literal query keys of the operation
//! URIs in the S3 model (`/{Bucket}?cors`, `/{Bucket}/{Key+}?tagging`, ...,
//! not the SDK's `x-id` hint nor `list-type` / `select-type`), plus
//! `partNumber` and `uploadId`, which S3 lists among them in its conflict
//! errors.

use std::collections::HashMap;

use fakecloud_core::service::AwsServiceError;
use http::{HeaderMap, Method, StatusCode};

macro_rules! s3_ops {
    ($($op:ident),* $(,)?) => {
        /// An S3 API operation (the model's operation names), plus the
        /// browser-form POST Object upload.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) enum S3Op {
            $($op),*
        }

        impl S3Op {
            /// The operation's API name.
            pub(crate) fn name(self) -> &'static str {
                match self {
                    $(S3Op::$op => stringify!($op)),*
                }
            }

            /// Every operation.
            #[cfg(test)]
            pub(crate) const ALL: &'static [S3Op] = &[$(S3Op::$op),*];
        }
    };
}

s3_ops!(
    AbortMultipartUpload,
    CompleteMultipartUpload,
    CopyObject,
    CreateBucket,
    CreateBucketMetadataConfiguration,
    CreateBucketMetadataTableConfiguration,
    CreateMultipartUpload,
    CreateSession,
    DeleteBucket,
    DeleteBucketAnalyticsConfiguration,
    DeleteBucketCors,
    DeleteBucketEncryption,
    DeleteBucketIntelligentTieringConfiguration,
    DeleteBucketInventoryConfiguration,
    DeleteBucketLifecycle,
    DeleteBucketMetadataConfiguration,
    DeleteBucketMetadataTableConfiguration,
    DeleteBucketMetricsConfiguration,
    DeleteBucketOwnershipControls,
    DeleteBucketPolicy,
    DeleteBucketReplication,
    DeleteBucketTagging,
    DeleteBucketWebsite,
    DeleteObject,
    DeleteObjectAnnotation,
    DeleteObjectTagging,
    DeleteObjects,
    DeletePublicAccessBlock,
    GetBucketAbac,
    GetBucketAccelerateConfiguration,
    GetBucketAcl,
    GetBucketAnalyticsConfiguration,
    GetBucketCors,
    GetBucketEncryption,
    GetBucketIntelligentTieringConfiguration,
    GetBucketInventoryConfiguration,
    GetBucketLifecycleConfiguration,
    GetBucketLocation,
    GetBucketLogging,
    GetBucketMetadataConfiguration,
    GetBucketMetadataTableConfiguration,
    GetBucketMetricsConfiguration,
    GetBucketNotificationConfiguration,
    GetBucketOwnershipControls,
    GetBucketPolicy,
    GetBucketPolicyStatus,
    GetBucketReplication,
    GetBucketRequestPayment,
    GetBucketTagging,
    GetBucketVersioning,
    GetBucketWebsite,
    GetObject,
    GetObjectAcl,
    GetObjectAnnotation,
    GetObjectAttributes,
    GetObjectLegalHold,
    GetObjectLockConfiguration,
    GetObjectRetention,
    GetObjectTagging,
    GetObjectTorrent,
    GetPublicAccessBlock,
    HeadBucket,
    HeadObject,
    ListBucketAnalyticsConfigurations,
    ListBucketIntelligentTieringConfigurations,
    ListBucketInventoryConfigurations,
    ListBucketMetricsConfigurations,
    ListBuckets,
    ListDirectoryBuckets,
    ListMultipartUploads,
    ListObjectAnnotations,
    ListObjectVersions,
    ListObjects,
    ListObjectsV2,
    ListParts,
    PostObject,
    PutBucketAbac,
    PutBucketAccelerateConfiguration,
    PutBucketAcl,
    PutBucketAnalyticsConfiguration,
    PutBucketCors,
    PutBucketEncryption,
    PutBucketIntelligentTieringConfiguration,
    PutBucketInventoryConfiguration,
    PutBucketLifecycleConfiguration,
    PutBucketLogging,
    PutBucketMetricsConfiguration,
    PutBucketNotificationConfiguration,
    PutBucketOwnershipControls,
    PutBucketPolicy,
    PutBucketReplication,
    PutBucketRequestPayment,
    PutBucketTagging,
    PutBucketVersioning,
    PutBucketWebsite,
    PutObject,
    PutObjectAcl,
    PutObjectAnnotation,
    PutObjectLegalHold,
    PutObjectLockConfiguration,
    PutObjectRetention,
    PutObjectTagging,
    PutPublicAccessBlock,
    RenameObject,
    RestoreObject,
    SelectObjectContent,
    UpdateBucketMetadataAnnotationTableConfiguration,
    UpdateBucketMetadataInventoryTableConfiguration,
    UpdateBucketMetadataJournalTableConfiguration,
    UpdateObjectEncryption,
    UploadPart,
    UploadPartCopy,
    WriteGetObjectResponse,
);

/// The resource-type query parameters, sorted: the literal query keys of the
/// S3 model's operation URIs, plus `partNumber` and `uploadId`.
pub(crate) const RESOURCE_TYPES: &[&str] = &[
    "abac",
    "accelerate",
    "acl",
    "analytics",
    "annotation",
    "attributes",
    "cors",
    "delete",
    "encryption",
    "intelligent-tiering",
    "inventory",
    "legal-hold",
    "lifecycle",
    "location",
    "logging",
    "metadataAnnotationTable",
    "metadataConfiguration",
    "metadataInventoryTable",
    "metadataJournalTable",
    "metadataTable",
    "metrics",
    "notification",
    "object-lock",
    "ownershipControls",
    "partNumber",
    "policy",
    "policyStatus",
    "publicAccessBlock",
    "renameObject",
    "replication",
    "requestPayment",
    "restore",
    "retention",
    "select",
    "session",
    "tagging",
    "torrent",
    "uploadId",
    "uploads",
    "versioning",
    "versions",
    "website",
];

/// The query keys that address a bucket sub-resource and so need a bucket in
/// the path: every resource type except an upload's address, plus the
/// listing parameter `list-type`.
fn needs_bucket(name: &str) -> bool {
    name == "list-type"
        || (RESOURCE_TYPES.contains(&name)
            && !matches!(name, "partNumber" | "uploadId" | "annotation"))
}

/// What a request addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Service,
    Bucket,
    Object,
}

/// How S3 refuses a request it cannot classify, before authorizing it.
#[derive(Debug)]
pub(crate) struct Refusal {
    pub(crate) error: AwsServiceError,
    /// Whether the refusal is answered like a request S3 got to the bucket
    /// with: CORS headers and an access-log record. Recorded: a conflict of
    /// resource-type parameters gets neither (S3 refuses it before resolving
    /// the request), the other refusals get the bucket's CORS headers.
    pub(crate) reaches_bucket: bool,
}

impl From<AwsServiceError> for Refusal {
    fn from(error: AwsServiceError) -> Self {
        Self {
            error,
            reaches_bucket: true,
        }
    }
}

/// Classify a non-S3-Control request: `method`, the path's `bucket` and
/// [`super::request_key`] `key`, its `query`, and the headers that tell a
/// copy (`x-amz-copy-source`) and a form upload apart. `Err` is how S3
/// answers the request before authorizing it.
pub(crate) fn classify(
    method: &Method,
    bucket: Option<&str>,
    key: Option<&str>,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<S3Op, Refusal> {
    let target = match (bucket, key) {
        (None, _) => Target::Service,
        (Some(_), None) => Target::Bucket,
        (Some(_), Some(_)) => Target::Object,
    };
    if target == Target::Service {
        if query.keys().any(|k| needs_bucket(k)) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                "The specified bucket is not valid: bucket name is required for this operation",
            )
            .into());
        }
        return match *method {
            Method::GET
                if query.get("x-id").map(String::as_str) == Some("ListDirectoryBuckets") =>
            {
                Ok(S3Op::ListDirectoryBuckets)
            }
            Method::GET => Ok(S3Op::ListBuckets),
            _ => Err(method_fallback()),
        };
    }

    let mut present: Vec<&str> = RESOURCE_TYPES
        .iter()
        .copied()
        .filter(|name| query.contains_key(*name))
        .collect();
    present.sort_unstable();
    let upload_part_address = present == ["partNumber", "uploadId"];
    if present.len() > 1 && !upload_part_address {
        return Err(conflicting(&present));
    }

    let has = |name: &str| query.contains_key(name);
    let copy = headers.contains_key("x-amz-copy-source");
    let not_allowed = |resource: &str| method_not_allowed(method, resource);

    if upload_part_address {
        return match *method {
            // Whatever `partNumber` holds: recorded, an invalid one with an
            // upload that does not exist is NoSuchUpload, so the upload is
            // looked up first and the number checked after authorization (by
            // the handler), and the request is an upload part either way.
            Method::PUT if target == Target::Object => Ok(if copy {
                S3Op::UploadPartCopy
            } else {
                S3Op::UploadPart
            }),
            Method::GET if target == Target::Object => Ok(S3Op::ListParts),
            Method::POST if target == Target::Object => Ok(S3Op::CompleteMultipartUpload),
            Method::DELETE if target == Target::Object => Ok(S3Op::AbortMultipartUpload),
            _ => Err(not_allowed("uploadId")),
        };
    }

    let Some(resource) = present.first().copied() else {
        return Ok(match (method.clone(), target) {
            (Method::GET, Target::Bucket) => {
                if query.get("list-type").map(String::as_str) == Some("2") {
                    S3Op::ListObjectsV2
                } else {
                    S3Op::ListObjects
                }
            }
            (Method::PUT, Target::Bucket) => S3Op::CreateBucket,
            (Method::DELETE, Target::Bucket) => S3Op::DeleteBucket,
            (Method::HEAD, Target::Bucket) => S3Op::HeadBucket,
            (Method::POST, Target::Bucket) if bucket == Some("WriteGetObjectResponse") => {
                S3Op::WriteGetObjectResponse
            }
            (Method::POST, Target::Bucket) if is_form_upload(headers) => S3Op::PostObject,
            (Method::GET, Target::Object) => S3Op::GetObject,
            (Method::HEAD, Target::Object) => S3Op::HeadObject,
            (Method::PUT, Target::Object) if copy => S3Op::CopyObject,
            (Method::PUT, Target::Object) => S3Op::PutObject,
            (Method::DELETE, Target::Object) => S3Op::DeleteObject,
            _ => return Err(method_fallback()),
        });
    };

    use S3Op::*;
    let by_id = |get: S3Op, list: S3Op| if has("id") { get } else { list };
    let op = match (target, resource, method.as_str()) {
        // Bucket sub-resources.
        (Target::Bucket, "tagging", "GET") => GetBucketTagging,
        (Target::Bucket, "tagging", "PUT") => PutBucketTagging,
        (Target::Bucket, "tagging", "DELETE") => DeleteBucketTagging,
        (Target::Bucket, "acl", "GET") => GetBucketAcl,
        (Target::Bucket, "acl", "PUT") => PutBucketAcl,
        (Target::Bucket, "versioning", "GET") => GetBucketVersioning,
        (Target::Bucket, "versioning", "PUT") => PutBucketVersioning,
        (Target::Bucket, "cors", "GET") => GetBucketCors,
        (Target::Bucket, "cors", "PUT") => PutBucketCors,
        (Target::Bucket, "cors", "DELETE") => DeleteBucketCors,
        (Target::Bucket, "notification", "GET") => GetBucketNotificationConfiguration,
        (Target::Bucket, "notification", "PUT") => PutBucketNotificationConfiguration,
        (Target::Bucket, "website", "GET") => GetBucketWebsite,
        (Target::Bucket, "website", "PUT") => PutBucketWebsite,
        (Target::Bucket, "website", "DELETE") => DeleteBucketWebsite,
        (Target::Bucket, "accelerate", "GET") => GetBucketAccelerateConfiguration,
        (Target::Bucket, "accelerate", "PUT") => PutBucketAccelerateConfiguration,
        (Target::Bucket, "publicAccessBlock", "GET") => GetPublicAccessBlock,
        (Target::Bucket, "publicAccessBlock", "PUT") => PutPublicAccessBlock,
        (Target::Bucket, "publicAccessBlock", "DELETE") => DeletePublicAccessBlock,
        (Target::Bucket, "encryption", "GET") => GetBucketEncryption,
        (Target::Bucket, "encryption", "PUT") => PutBucketEncryption,
        (Target::Bucket, "encryption", "DELETE") => DeleteBucketEncryption,
        (Target::Bucket, "lifecycle", "GET") => GetBucketLifecycleConfiguration,
        (Target::Bucket, "lifecycle", "PUT") => PutBucketLifecycleConfiguration,
        (Target::Bucket, "lifecycle", "DELETE") => DeleteBucketLifecycle,
        (Target::Bucket, "logging", "GET") => GetBucketLogging,
        (Target::Bucket, "logging", "PUT") => PutBucketLogging,
        (Target::Bucket, "policy", "GET") => GetBucketPolicy,
        (Target::Bucket, "policy", "PUT") => PutBucketPolicy,
        (Target::Bucket, "policy", "DELETE") => DeleteBucketPolicy,
        (Target::Bucket, "policyStatus", "GET") => GetBucketPolicyStatus,
        (Target::Bucket, "object-lock", "GET") => GetObjectLockConfiguration,
        (Target::Bucket, "object-lock", "PUT") => PutObjectLockConfiguration,
        (Target::Bucket, "replication", "GET") => GetBucketReplication,
        (Target::Bucket, "replication", "PUT") => PutBucketReplication,
        (Target::Bucket, "replication", "DELETE") => DeleteBucketReplication,
        (Target::Bucket, "ownershipControls", "GET") => GetBucketOwnershipControls,
        (Target::Bucket, "ownershipControls", "PUT") => PutBucketOwnershipControls,
        (Target::Bucket, "ownershipControls", "DELETE") => DeleteBucketOwnershipControls,
        (Target::Bucket, "inventory", "GET") => by_id(
            GetBucketInventoryConfiguration,
            ListBucketInventoryConfigurations,
        ),
        (Target::Bucket, "inventory", "PUT") => PutBucketInventoryConfiguration,
        (Target::Bucket, "inventory", "DELETE") => DeleteBucketInventoryConfiguration,
        (Target::Bucket, "analytics", "GET") => by_id(
            GetBucketAnalyticsConfiguration,
            ListBucketAnalyticsConfigurations,
        ),
        (Target::Bucket, "analytics", "PUT") => PutBucketAnalyticsConfiguration,
        (Target::Bucket, "analytics", "DELETE") => DeleteBucketAnalyticsConfiguration,
        (Target::Bucket, "intelligent-tiering", "GET") => by_id(
            GetBucketIntelligentTieringConfiguration,
            ListBucketIntelligentTieringConfigurations,
        ),
        (Target::Bucket, "intelligent-tiering", "PUT") => PutBucketIntelligentTieringConfiguration,
        (Target::Bucket, "intelligent-tiering", "DELETE") => {
            DeleteBucketIntelligentTieringConfiguration
        }
        (Target::Bucket, "metrics", "GET") => by_id(
            GetBucketMetricsConfiguration,
            ListBucketMetricsConfigurations,
        ),
        (Target::Bucket, "metrics", "PUT") => PutBucketMetricsConfiguration,
        (Target::Bucket, "metrics", "DELETE") => DeleteBucketMetricsConfiguration,
        (Target::Bucket, "requestPayment", "GET") => GetBucketRequestPayment,
        (Target::Bucket, "requestPayment", "PUT") => PutBucketRequestPayment,
        (Target::Bucket, "abac", "GET") => GetBucketAbac,
        (Target::Bucket, "abac", "PUT") => PutBucketAbac,
        (Target::Bucket, "metadataConfiguration", "GET") => GetBucketMetadataConfiguration,
        (Target::Bucket, "metadataConfiguration", "POST") => CreateBucketMetadataConfiguration,
        (Target::Bucket, "metadataConfiguration", "DELETE") => DeleteBucketMetadataConfiguration,
        (Target::Bucket, "metadataTable", "GET") => GetBucketMetadataTableConfiguration,
        (Target::Bucket, "metadataTable", "POST") => CreateBucketMetadataTableConfiguration,
        (Target::Bucket, "metadataTable", "DELETE") => DeleteBucketMetadataTableConfiguration,
        (Target::Bucket, "metadataInventoryTable", "PUT") => {
            UpdateBucketMetadataInventoryTableConfiguration
        }
        (Target::Bucket, "metadataJournalTable", "PUT") => {
            UpdateBucketMetadataJournalTableConfiguration
        }
        (Target::Bucket, "metadataAnnotationTable", "PUT") => {
            UpdateBucketMetadataAnnotationTableConfiguration
        }
        (Target::Bucket, "session", "GET") => CreateSession,
        (Target::Bucket, "location", "GET") => GetBucketLocation,
        (Target::Bucket, "versions", "GET") => ListObjectVersions,
        (Target::Bucket, "uploads", "GET") => ListMultipartUploads,
        (Target::Bucket, "delete", "POST") => DeleteObjects,

        // Object sub-resources.
        (Target::Object, "tagging", "GET") => GetObjectTagging,
        (Target::Object, "tagging", "PUT") => PutObjectTagging,
        (Target::Object, "tagging", "DELETE") => DeleteObjectTagging,
        (Target::Object, "acl", "GET") => GetObjectAcl,
        (Target::Object, "acl", "PUT") => PutObjectAcl,
        (Target::Object, "retention", "GET") => GetObjectRetention,
        (Target::Object, "retention", "PUT") => PutObjectRetention,
        (Target::Object, "legal-hold", "GET") => GetObjectLegalHold,
        (Target::Object, "legal-hold", "PUT") => PutObjectLegalHold,
        (Target::Object, "attributes", "GET") => GetObjectAttributes,
        (Target::Object, "torrent", "GET") => GetObjectTorrent,
        (Target::Object, "renameObject", "PUT") => RenameObject,
        (Target::Object, "encryption", "PUT") => UpdateObjectEncryption,
        (Target::Object, "restore", "POST") => RestoreObject,
        (Target::Object, "uploads", "POST") => CreateMultipartUpload,
        (Target::Object, "select", "POST")
            if query.get("select-type").map(String::as_str) == Some("2") =>
        {
            SelectObjectContent
        }
        (Target::Object, "annotation", "GET") => {
            if has(super::annotations::ANNOTATION_NAME_QUERY) {
                GetObjectAnnotation
            } else {
                ListObjectAnnotations
            }
        }
        (Target::Object, "annotation", "PUT") => PutObjectAnnotation,
        (Target::Object, "annotation", "DELETE") => DeleteObjectAnnotation,
        // An object read of one part.
        (Target::Object, "partNumber", "GET") => GetObject,
        (Target::Object, "partNumber", "HEAD") => HeadObject,
        // Half an upload part's address (recorded on S3, which names the
        // parameter in both ArgumentName and ArgumentValue).
        (Target::Object, "partNumber", "PUT") => {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "This operation does not accept partNumber without uploadId",
                vec![
                    ("ArgumentName".to_string(), "partNumber".to_string()),
                    ("ArgumentValue".to_string(), "partNumber".to_string()),
                ],
            )
            .into())
        }
        (Target::Object, "uploadId", "GET") => ListParts,
        (Target::Object, "uploadId", "POST") => CompleteMultipartUpload,
        (Target::Object, "uploadId", "DELETE") => AbortMultipartUpload,
        _ => return Err(not_allowed(resource)),
    };
    Ok(op)
}

/// The access-log operation of the upload operations, which method and key
/// alone cannot tell from object reads and writes (logging an
/// `AbortMultipartUpload` as `DELETE.OBJECT` would show a log consumer a live
/// object being deleted). `None` for the rest, logged by method and key.
pub(crate) fn access_log_operation(op: Option<S3Op>) -> Option<&'static str> {
    Some(match op? {
        S3Op::CreateMultipartUpload => "POST.UPLOADS",
        S3Op::RestoreObject => "POST.RESTORE",
        S3Op::CompleteMultipartUpload => "POST.UPLOAD",
        S3Op::UploadPartCopy => "COPY.PART",
        S3Op::UploadPart => "PUT.PART",
        S3Op::AbortMultipartUpload => "DELETE.UPLOAD",
        S3Op::ListMultipartUploads => "GET.UPLOADS",
        S3Op::ListParts => "GET.UPLOAD",
        _ => return None,
    })
}

/// Whether a bucket POST is a browser-form upload (`multipart/form-data`).
fn is_form_upload(headers: &HeaderMap) -> bool {
    headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.to_ascii_lowercase().starts_with("multipart/form-data"))
}

/// Recorded on S3: "Conflicting query string parameters: <names, sorted>",
/// `ArgumentName` `ResourceType`, `ArgumentValue` the first name, with no
/// CORS headers.
fn conflicting(names: &[&str]) -> Refusal {
    Refusal {
        error: AwsServiceError::aws_error_with_fields(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            format!("Conflicting query string parameters: {}", names.join(", ")),
            vec![
                ("ArgumentName".to_string(), "ResourceType".to_string()),
                ("ArgumentValue".to_string(), names[0].to_string()),
            ],
        ),
        reaches_bucket: false,
    }
}

/// The resource type a 405 names: the parameter upper-cased (recorded:
/// `LOCATION`), except an upload's `uploadId`, which S3 calls `UPLOAD`.
fn resource_type_name(resource: &str) -> String {
    match resource {
        "uploadId" => "UPLOAD".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

/// Recorded on S3 (`PUT /b?location`, `PUT /b/k?uploadId=u`): 405 naming the
/// method and the resource type.
fn method_not_allowed(method: &Method, resource: &str) -> Refusal {
    AwsServiceError::aws_error_with_fields(
        StatusCode::METHOD_NOT_ALLOWED,
        "MethodNotAllowed",
        "The specified method is not allowed against this resource.",
        vec![
            ("Method".to_string(), method.as_str().to_string()),
            ("ResourceType".to_string(), resource_type_name(resource)),
        ],
    )
    .into()
}

/// A method no operation takes on its target.
fn method_fallback() -> Refusal {
    AwsServiceError::aws_error(
        StatusCode::METHOD_NOT_ALLOWED,
        "MethodNotAllowed",
        "The specified method is not allowed against this resource",
    )
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn run(
        method: Method,
        target: (Option<&str>, Option<&str>),
        query: &[(&str, &str)],
    ) -> Result<S3Op, AwsServiceError> {
        classify(&method, target.0, target.1, &q(query), &HeaderMap::new()).map_err(|r| r.error)
    }

    /// Recorded: a conflict gets no CORS headers, the other refusals do.
    #[test]
    fn only_conflicts_are_refused_before_reaching_the_bucket() {
        let reach = |method: Method, query: &[(&str, &str)]| {
            classify(&method, Some("b"), Some("k"), &q(query), &HeaderMap::new())
                .unwrap_err()
                .reaches_bucket
        };
        assert!(!reach(Method::GET, &[("acl", ""), ("tagging", "")]));
        assert!(reach(Method::PUT, &[("uploadId", "u")]));
        assert!(reach(Method::PUT, &[("partNumber", "1")]));
        assert!(reach(Method::GET, &[("restore", "")]));
    }

    const BUCKET: (Option<&str>, Option<&str>) = (Some("b"), None);
    const OBJECT: (Option<&str>, Option<&str>) = (Some("b"), Some("k"));

    fn fields(e: &AwsServiceError) -> Vec<(String, String)> {
        e.extra_fields().to_vec()
    }

    #[test]
    fn the_resource_types_are_the_models_uri_literals() {
        let model: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../aws-models/s3.json"
            ))
            .unwrap(),
        )
        .unwrap();
        let mut literals: Vec<String> = Vec::new();
        for shape in model["shapes"].as_object().unwrap().values() {
            let Some(uri) = shape["traits"]["smithy.api#http"]["uri"].as_str() else {
                continue;
            };
            let Some((_, query)) = uri.split_once('?') else {
                continue;
            };
            for part in query.split('&') {
                let name = part.split('=').next().unwrap();
                if !matches!(name, "x-id" | "list-type" | "select-type") {
                    literals.push(name.to_string());
                }
            }
        }
        literals.extend(["partNumber".to_string(), "uploadId".to_string()]);
        literals.sort();
        literals.dedup();
        assert_eq!(literals, RESOURCE_TYPES);
    }

    /// Every operation in the model is routed from its model URI and method.
    #[test]
    fn every_model_operation_classifies_from_its_uri() {
        let model: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../aws-models/s3.json"
            ))
            .unwrap(),
        )
        .unwrap();
        let mut seen = 0;
        for (id, shape) in model["shapes"].as_object().unwrap() {
            let http = &shape["traits"]["smithy.api#http"];
            let (Some(method), Some(uri)) = (http["method"].as_str(), http["uri"].as_str()) else {
                continue;
            };
            let op = id.rsplit('#').next().unwrap();
            let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
            let mut pairs: Vec<(&str, &str)> = query
                .split('&')
                .filter(|p| !p.is_empty())
                .map(|p| p.split_once('=').unwrap_or((p, "")))
                .collect();
            let mut headers = HeaderMap::new();
            // The query members that select these operations.
            match op {
                "UploadPart" | "UploadPartCopy" => {
                    pairs.extend([("partNumber", "1"), ("uploadId", "u")]);
                }
                "AbortMultipartUpload" | "CompleteMultipartUpload" | "ListParts" => {
                    pairs.push(("uploadId", "u"));
                }
                "GetObjectAnnotation" => {
                    pairs.push((super::super::annotations::ANNOTATION_NAME_QUERY, "n"))
                }
                "GetBucketAnalyticsConfiguration"
                | "GetBucketInventoryConfiguration"
                | "GetBucketMetricsConfiguration"
                | "GetBucketIntelligentTieringConfiguration" => pairs.push(("id", "i")),
                _ => {}
            }
            if matches!(op, "CopyObject" | "UploadPartCopy") {
                headers.insert("x-amz-copy-source", "src/k".parse().unwrap());
            }
            let target = if path == "/WriteGetObjectResponse" {
                (Some("WriteGetObjectResponse"), None)
            } else if path.contains("{Key+}") {
                OBJECT
            } else if path.contains("{Bucket}") {
                BUCKET
            } else {
                (None, None)
            };
            let method: Method = method.parse().unwrap();
            let got = classify(&method, target.0, target.1, &q(&pairs), &headers)
                .unwrap_or_else(|e| panic!("{op}: {e:?}"));
            assert_eq!(got.name(), op);
            seen += 1;
        }
        assert_eq!(seen, S3Op::ALL.len() - 1, "every operation but PostObject");
    }

    #[test]
    fn conflicting_resource_types_are_refused_as_recorded() {
        for (method, target, query, names) in [
            (
                Method::GET,
                BUCKET,
                vec![("versions", ""), ("cors", "")],
                "cors, versions",
            ),
            (
                Method::GET,
                BUCKET,
                vec![("versions", ""), ("object-lock", "")],
                "object-lock, versions",
            ),
            (
                Method::GET,
                BUCKET,
                vec![("location", ""), ("versions", "")],
                "location, versions",
            ),
            (
                Method::PUT,
                OBJECT,
                vec![("partNumber", "x"), ("uploadId", "u"), ("tagging", "")],
                "partNumber, tagging, uploadId",
            ),
            (
                Method::PUT,
                OBJECT,
                vec![("partNumber", "1"), ("uploadId", "u"), ("acl", "")],
                "acl, partNumber, uploadId",
            ),
            (
                Method::POST,
                OBJECT,
                vec![("restore", ""), ("uploadId", "u")],
                "restore, uploadId",
            ),
            (
                Method::DELETE,
                BUCKET,
                vec![("publicAccessBlock", ""), ("lifecycle", "")],
                "lifecycle, publicAccessBlock",
            ),
            (
                Method::GET,
                OBJECT,
                vec![("tagging", ""), ("acl", "")],
                "acl, tagging",
            ),
            (
                Method::POST,
                BUCKET,
                vec![("delete", ""), ("uploads", "")],
                "delete, uploads",
            ),
        ] {
            let err = run(method, target, &query).unwrap_err();
            assert_eq!(err.status(), StatusCode::BAD_REQUEST, "{names}");
            assert_eq!(err.code(), "InvalidArgument");
            assert_eq!(
                err.message(),
                format!("Conflicting query string parameters: {names}")
            );
            assert_eq!(
                fields(&err),
                vec![
                    ("ArgumentName".to_string(), "ResourceType".to_string()),
                    (
                        "ArgumentValue".to_string(),
                        names.split(", ").next().unwrap().to_string()
                    ),
                ]
            );
        }
    }

    #[test]
    fn a_resource_type_with_an_unsupported_method_is_405() {
        for (method, target, resource) in [
            (Method::PUT, BUCKET, "location"),
            (Method::DELETE, BUCKET, "location"),
            (Method::DELETE, BUCKET, "versioning"),
            (Method::POST, BUCKET, "cors"),
            (Method::GET, OBJECT, "restore"),
            (Method::PUT, OBJECT, "uploadId"),
            (Method::HEAD, BUCKET, "acl"),
            (Method::GET, OBJECT, "cors"),
            (Method::GET, BUCKET, "retention"),
            (Method::POST, OBJECT, "select"),
        ] {
            let err = run(method.clone(), target, &[(resource, "")]).unwrap_err();
            assert_eq!(
                err.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} {resource}"
            );
            assert_eq!(err.code(), "MethodNotAllowed");
            assert_eq!(
                fields(&err),
                vec![
                    ("Method".to_string(), method.as_str().to_string()),
                    ("ResourceType".to_string(), resource_type_name(resource)),
                ]
            );
        }
    }

    #[test]
    fn an_upload_part_address_is_an_upload_part_whatever_its_number() {
        // The number is checked after the upload lookup and authorization;
        // the request can be nothing but an upload part.
        for good in ["x", "0", "10001", "-1", "", "1", "10000"] {
            assert_eq!(
                run(
                    Method::PUT,
                    OBJECT,
                    &[("partNumber", good), ("uploadId", "u")]
                )
                .unwrap(),
                S3Op::UploadPart
            );
        }
        // Half an address, as recorded.
        let half = run(Method::PUT, OBJECT, &[("partNumber", "1")]).unwrap_err();
        assert_eq!(half.code(), "InvalidArgument");
        assert_eq!(
            half.message(),
            "This operation does not accept partNumber without uploadId"
        );
        assert_eq!(
            fields(&half),
            vec![
                ("ArgumentName".to_string(), "partNumber".to_string()),
                ("ArgumentValue".to_string(), "partNumber".to_string()),
            ]
        );
        let other_half = run(Method::PUT, OBJECT, &[("uploadId", "u")]).unwrap_err();
        assert_eq!(other_half.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            fields(&other_half),
            vec![
                ("Method".to_string(), "PUT".to_string()),
                ("ResourceType".to_string(), "UPLOAD".to_string()),
            ]
        );
    }

    #[test]
    fn modifiers_do_not_count_as_resource_types() {
        assert_eq!(
            run(Method::GET, OBJECT, &[("versionId", "v"), ("tagging", "")]).unwrap(),
            S3Op::GetObjectTagging
        );
        assert_eq!(
            run(
                Method::GET,
                OBJECT,
                &[("partNumber", "2"), ("versionId", "v")]
            )
            .unwrap(),
            S3Op::GetObject
        );
        assert_eq!(
            run(Method::GET, BUCKET, &[("list-type", "2"), ("prefix", "p")]).unwrap(),
            S3Op::ListObjectsV2
        );
        assert_eq!(
            run(
                Method::GET,
                BUCKET,
                &[("uploads", ""), ("upload-id-marker", "u")]
            )
            .unwrap(),
            S3Op::ListMultipartUploads
        );
    }

    #[test]
    fn bucket_sub_resources_need_a_bucket() {
        let err = run(Method::GET, (None, None), &[("cors", "")]).unwrap_err();
        assert_eq!(err.code(), "InvalidBucketName");
        assert_eq!(
            run(Method::GET, (None, None), &[]).unwrap(),
            S3Op::ListBuckets
        );
        assert_eq!(
            run(Method::PUT, (None, None), &[]).unwrap_err().status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
    }
}
