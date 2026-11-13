//! The IAM policy actions an S3 request needs.
//!
//! S3 is authorized under policy action names that often differ from its API
//! operation names: `ListObjectsV2` and `HeadBucket` are `s3:ListBucket`,
//! `ListBuckets` is `s3:ListAllMyBuckets`, `HeadObject` and the multipart
//! writes are `s3:GetObject` / `s3:PutObject`, the lifecycle operations are
//! `s3:*LifecycleConfiguration`, and a request naming a `versionId` needs the
//! `*Version*` action instead. A policy granting `s3:ListObjectsV2` grants
//! nothing, since no such action exists.
//!
//! The mapping follows the user guide's "Required permissions for Amazon S3
//! API operations" (which also says when a conditionally required action
//! applies: an ACL or tagging header, Object Lock headers, a governance
//! bypass) and the API reference's per-operation Permissions sections. The
//! service authorization reference vendored in [`super::iam_reference`]
//! supplies each action's resource type, and its operation table is the
//! oracle the unit tests check every mapping here against.
//!
//! KMS permissions an operation also needs (`kms:Decrypt`,
//! `kms:GenerateDataKey`) are not listed here: they are authorized against the
//! key when S3 uses it, through the KMS hook.

use fakecloud_core::service::AwsRequest;
use http::HeaderMap;

use super::iam_reference::{ResourceKind, ACTION_RESOURCES};
use super::{
    acl_reaches_past_owner, create_bucket_configuration_tags, governance_bypass_requested,
    has_grant_headers, parse_delete_objects_xml, replication_role_arn,
};

/// One authorization a request needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Need {
    /// `service:action` on the request's own bucket or object, whichever the
    /// action's resource type is.
    Own(&'static str, &'static str),
    /// `s3:action` on another object of the request's bucket: each key a
    /// DeleteObjects request names.
    Key(&'static str, String),
    /// Reading the copy source of a CopyObject / UploadPartCopy:
    /// `s3:GetObject`, or `s3:GetObjectVersion` for a versioned source, on the
    /// source object.
    CopySource,
    /// `iam:PassRole` on a role the request hands to S3.
    PassRole(String),
}

/// The resource type `action` is authorized on, per the service
/// authorization reference. `None` for an action S3 does not define.
pub(crate) fn resource_kind(action: &str) -> Option<ResourceKind> {
    ACTION_RESOURCES
        .binary_search_by(|(name, _)| (*name).cmp(action))
        .ok()
        .map(|i| ACTION_RESOURCES[i].1)
}

/// Every authorization the S3 API operation `op` needs for `req`, the
/// operation's own required action first. Empty for an operation with no
/// mapping, which strict enforcement refuses.
pub(crate) fn required(op: &str, req: &AwsRequest) -> Vec<Need> {
    let versioned = names_version(req);
    // `s3:<Unversioned>`, or `s3:<Versioned>` when the request names a version.
    let pick = |plain: &'static str, version: &'static str| {
        Need::Own("s3", if versioned { version } else { plain })
    };
    let s3 = |action: &'static str| Need::Own("s3", action);
    let mut needs = match op {
        "ListBuckets" => vec![s3("ListAllMyBuckets")],
        "ListDirectoryBuckets" => vec![Need::Own("s3express", "ListAllMyDirectoryBuckets")],
        "WriteGetObjectResponse" => vec![Need::Own("s3-object-lambda", "WriteGetObjectResponse")],
        // Directory-bucket (S3 Express One Zone) operations run on the session
        // CreateSession opens, so they are authorized as `s3express:CreateSession`
        // ("CreateSession enables access for all object level operations").
        "CreateSession" | "RenameObject" => vec![Need::Own("s3express", "CreateSession")],

        // Bucket operations.
        "CreateBucket" => create_bucket(req),
        "DeleteBucket" => vec![s3("DeleteBucket")],
        "HeadBucket" | "ListObjects" | "ListObjectsV2" => vec![s3("ListBucket")],
        "ListObjectVersions" => vec![s3("ListBucketVersions")],
        "ListMultipartUploads" => vec![s3("ListBucketMultipartUploads")],
        "GetBucketLocation" => vec![s3("GetBucketLocation")],
        "GetBucketAcl" => vec![s3("GetBucketAcl")],
        "PutBucketAcl" => vec![s3("PutBucketAcl")],
        "GetBucketPolicy" => vec![s3("GetBucketPolicy")],
        "PutBucketPolicy" => vec![s3("PutBucketPolicy")],
        "DeleteBucketPolicy" => vec![s3("DeleteBucketPolicy")],
        "GetBucketPolicyStatus" => vec![s3("GetBucketPolicyStatus")],
        "GetBucketTagging" => vec![s3("GetBucketTagging")],
        "PutBucketTagging" | "DeleteBucketTagging" => vec![s3("PutBucketTagging")],
        "GetBucketVersioning" => vec![s3("GetBucketVersioning")],
        "PutBucketVersioning" => vec![s3("PutBucketVersioning")],
        "GetBucketCors" => vec![s3("GetBucketCORS")],
        "PutBucketCors" | "DeleteBucketCors" => vec![s3("PutBucketCORS")],
        "GetBucketWebsite" => vec![s3("GetBucketWebsite")],
        "PutBucketWebsite" => vec![s3("PutBucketWebsite")],
        "DeleteBucketWebsite" => vec![s3("DeleteBucketWebsite")],
        "GetBucketLifecycleConfiguration" => vec![s3("GetLifecycleConfiguration")],
        "PutBucketLifecycleConfiguration" | "DeleteBucketLifecycle" => {
            vec![s3("PutLifecycleConfiguration")]
        }
        "GetBucketEncryption" => vec![s3("GetEncryptionConfiguration")],
        "PutBucketEncryption" | "DeleteBucketEncryption" => {
            vec![s3("PutEncryptionConfiguration")]
        }
        "GetBucketLogging" => vec![s3("GetBucketLogging")],
        "PutBucketLogging" => vec![s3("PutBucketLogging")],
        "GetBucketNotificationConfiguration" => vec![s3("GetBucketNotification")],
        "PutBucketNotificationConfiguration" => vec![s3("PutBucketNotification")],
        "GetBucketReplication" => vec![s3("GetReplicationConfiguration")],
        // The replication configuration names the role S3 assumes to copy
        // objects, which the caller must be allowed to pass.
        "PutBucketReplication" => {
            let mut needs = vec![s3("PutReplicationConfiguration")];
            needs.extend(replication_role_arn(&req.body).map(Need::PassRole));
            needs
        }
        "DeleteBucketReplication" => vec![s3("PutReplicationConfiguration")],
        "GetBucketOwnershipControls" => vec![s3("GetBucketOwnershipControls")],
        "PutBucketOwnershipControls" | "DeleteBucketOwnershipControls" => {
            vec![s3("PutBucketOwnershipControls")]
        }
        "GetPublicAccessBlock" => vec![s3("GetBucketPublicAccessBlock")],
        "PutPublicAccessBlock" | "DeletePublicAccessBlock" => {
            vec![s3("PutBucketPublicAccessBlock")]
        }
        "GetBucketAccelerateConfiguration" => vec![s3("GetAccelerateConfiguration")],
        "PutBucketAccelerateConfiguration" => vec![s3("PutAccelerateConfiguration")],
        "GetBucketInventoryConfiguration" | "ListBucketInventoryConfigurations" => {
            vec![s3("GetInventoryConfiguration")]
        }
        "PutBucketInventoryConfiguration" | "DeleteBucketInventoryConfiguration" => {
            vec![s3("PutInventoryConfiguration")]
        }
        "GetBucketAnalyticsConfiguration" | "ListBucketAnalyticsConfigurations" => {
            vec![s3("GetAnalyticsConfiguration")]
        }
        "PutBucketAnalyticsConfiguration" | "DeleteBucketAnalyticsConfiguration" => {
            vec![s3("PutAnalyticsConfiguration")]
        }
        "GetBucketIntelligentTieringConfiguration"
        | "ListBucketIntelligentTieringConfigurations" => {
            vec![s3("GetIntelligentTieringConfiguration")]
        }
        "PutBucketIntelligentTieringConfiguration"
        | "DeleteBucketIntelligentTieringConfiguration" => {
            vec![s3("PutIntelligentTieringConfiguration")]
        }
        "GetBucketMetricsConfiguration" | "ListBucketMetricsConfigurations" => {
            vec![s3("GetMetricsConfiguration")]
        }
        "PutBucketMetricsConfiguration" | "DeleteBucketMetricsConfiguration" => {
            vec![s3("PutMetricsConfiguration")]
        }
        "GetBucketRequestPayment" => vec![s3("GetBucketRequestPayment")],
        "PutBucketRequestPayment" => vec![s3("PutBucketRequestPayment")],
        "GetObjectLockConfiguration" => vec![s3("GetBucketObjectLockConfiguration")],
        "PutObjectLockConfiguration" => vec![s3("PutBucketObjectLockConfiguration")],
        "GetBucketAbac" => vec![s3("GetBucketAbac")],
        "PutBucketAbac" => vec![s3("PutBucketAbac")],
        // The V1 and V2 metadata operations share one action name each.
        "GetBucketMetadataConfiguration" | "GetBucketMetadataTableConfiguration" => {
            vec![s3("GetBucketMetadataTableConfiguration")]
        }
        "CreateBucketMetadataConfiguration" | "CreateBucketMetadataTableConfiguration" => {
            vec![s3("CreateBucketMetadataTableConfiguration")]
        }
        "DeleteBucketMetadataConfiguration" | "DeleteBucketMetadataTableConfiguration" => {
            vec![s3("DeleteBucketMetadataTableConfiguration")]
        }
        "UpdateBucketMetadataInventoryTableConfiguration" => {
            vec![s3("UpdateBucketMetadataInventoryTableConfiguration")]
        }
        "UpdateBucketMetadataJournalTableConfiguration" => {
            vec![s3("UpdateBucketMetadataJournalTableConfiguration")]
        }
        "UpdateBucketMetadataAnnotationTableConfiguration" => {
            vec![s3("UpdateBucketMetadataAnnotationTableConfiguration")]
        }

        // Object operations.
        "GetObject" => vec![pick("GetObject", "GetObjectVersion")],
        // "You need the relevant read object (or version) permission".
        "HeadObject" => vec![pick("GetObject", "GetObjectVersion")],
        "GetObjectTorrent" | "SelectObjectContent" => vec![s3("GetObject")],
        // Both actions, per the API reference: GetObject + GetObjectAttributes,
        // or GetObjectVersion + GetObjectVersionAttributes for a version.
        "GetObjectAttributes" => vec![
            pick("GetObject", "GetObjectVersion"),
            pick("GetObjectAttributes", "GetObjectVersionAttributes"),
        ],
        "PutObject" | "CreateMultipartUpload" => {
            let mut needs = vec![s3("PutObject")];
            needs.extend(object_write_extras(&req.headers));
            needs
        }
        // The destination write, then the source read.
        "CopyObject" => {
            let mut needs = vec![s3("PutObject"), Need::CopySource];
            // Copying the source's annotations (the default COPY annotation
            // directive) also needs s3:GetObjectAnnotation on the source and
            // s3:PutObjectAnnotation on the destination, but only when the
            // source has annotations, which the handler checks.
            needs.extend(copy_write_extras(&req.headers));
            needs
        }
        "UploadPart" | "CompleteMultipartUpload" => vec![s3("PutObject")],
        "UploadPartCopy" => vec![s3("PutObject"), Need::CopySource],
        "AbortMultipartUpload" => vec![s3("AbortMultipartUpload")],
        "ListParts" => vec![s3("ListMultipartUploadParts")],
        "DeleteObject" => {
            let mut needs = vec![pick("DeleteObject", "DeleteObjectVersion")];
            if governance_bypass_requested(req) {
                needs.push(s3("BypassGovernanceRetention"));
            }
            needs
        }
        "DeleteObjects" => delete_objects(req),
        "GetObjectAcl" => vec![pick("GetObjectAcl", "GetObjectVersionAcl")],
        "PutObjectAcl" => vec![pick("PutObjectAcl", "PutObjectVersionAcl")],
        "GetObjectTagging" => vec![pick("GetObjectTagging", "GetObjectVersionTagging")],
        "PutObjectTagging" => vec![pick("PutObjectTagging", "PutObjectVersionTagging")],
        "DeleteObjectTagging" => vec![pick("DeleteObjectTagging", "DeleteObjectVersionTagging")],
        "GetObjectRetention" => vec![s3("GetObjectRetention")],
        "PutObjectRetention" => {
            let mut needs = vec![s3("PutObjectRetention")];
            if governance_bypass_requested(req) {
                needs.push(s3("BypassGovernanceRetention"));
            }
            needs
        }
        "GetObjectLegalHold" => vec![s3("GetObjectLegalHold")],
        "PutObjectLegalHold" => vec![s3("PutObjectLegalHold")],
        "RestoreObject" => vec![s3("RestoreObject")],
        "UpdateObjectEncryption" => vec![s3("UpdateObjectEncryption")],
        "GetObjectAnnotation" => vec![pick("GetObjectAnnotation", "GetObjectVersionAnnotation")],
        "ListObjectAnnotations" => {
            vec![pick(
                "ListObjectAnnotations",
                "ListObjectVersionAnnotations",
            )]
        }
        "PutObjectAnnotation" => vec![pick("PutObjectAnnotation", "PutObjectVersionAnnotation")],
        "DeleteObjectAnnotation" => {
            vec![pick(
                "DeleteObjectAnnotation",
                "DeleteObjectVersionAnnotation",
            )]
        }
        _ => Vec::new(),
    };
    needs.dedup();
    needs
}

/// Whether the request addresses a specific object version (`?versionId=`).
pub(crate) fn names_version(req: &AwsRequest) -> bool {
    req.query_params
        .get("versionId")
        .is_some_and(|v| !v.trim().is_empty())
}

/// A non-blank header value. A present-but-empty header asks for nothing, the
/// rule every header-driven permission and condition key here follows.
fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.trim().is_empty())
}

/// The conditionally required actions of an object write (PutObject,
/// CopyObject, CreateMultipartUpload), evaluated on the object written:
/// `s3:PutObjectAcl` for an ACL (`x-amz-acl` or any `x-amz-grant-*`),
/// `s3:PutObjectTagging` for `x-amz-tagging`, `s3:PutObjectRetention` for a
/// retention mode or date and `s3:PutObjectLegalHold` for a legal hold.
pub(crate) fn object_write_extras(headers: &HeaderMap) -> Vec<Need> {
    write_extras(headers, true)
}

/// [`object_write_extras`] for a CopyObject, whose `x-amz-tagging` sets the
/// copy's tags only under `x-amz-tagging-directive: REPLACE`; under COPY (the
/// default) the source's tag set is copied and the header is ignored.
pub(crate) fn copy_write_extras(headers: &HeaderMap) -> Vec<Need> {
    write_extras(headers, copy_replaces_tags(headers))
}

/// Whether a CopyObject replaces the copy's tag set with its `x-amz-tagging`.
pub(crate) fn copy_replaces_tags(headers: &HeaderMap) -> bool {
    headers
        .get("x-amz-tagging-directive")
        .and_then(|v| v.to_str().ok())
        == Some("REPLACE")
}

fn write_extras(headers: &HeaderMap, tagging_applies: bool) -> Vec<Need> {
    let mut needs = Vec::new();
    if header(headers, "x-amz-acl").is_some() || has_grant_headers(headers) {
        needs.push(Need::Own("s3", "PutObjectAcl"));
    }
    if tagging_applies && header(headers, "x-amz-tagging").is_some() {
        needs.push(Need::Own("s3", "PutObjectTagging"));
    }
    // Presence, blank included, as the write itself reads these headers: a
    // blank lock header is not ignored but refused as an invalid argument.
    if headers.contains_key("x-amz-object-lock-mode")
        || headers.contains_key("x-amz-object-lock-retain-until-date")
    {
        needs.push(Need::Own("s3", "PutObjectRetention"));
    }
    if headers.contains_key("x-amz-object-lock-legal-hold") {
        needs.push(Need::Own("s3", "PutObjectLegalHold"));
    }
    needs
}

/// CreateBucket, plus a permission for each thing the create also configures.
/// Dispatch requires every returned action, so a principal holding only
/// `s3:CreateBucket` can still create a plain bucket. These settings persist,
/// so a bucket created `public-read` without `s3:PutBucketAcl` would keep a
/// grant its creator was never allowed to make.
fn create_bucket(req: &AwsRequest) -> Vec<Need> {
    let mut needs = vec![Need::Own("s3", "CreateBucket")];
    let body = std::str::from_utf8(&req.body).unwrap_or("");
    // "You must have the s3:TagResource permission to create a general
    // purpose bucket with tags" (CreateBucketConfiguration$Tags).
    // A configuration that does not read never gets here: it is refused
    // before authorization (`validate_before_authorization`).
    if !create_bucket_configuration_tags(body)
        .unwrap_or_default()
        .is_empty()
    {
        needs.push(Need::Own("s3", "TagResource"));
    }
    // An ACL set to public-read, public-read-write, authenticated-read "or any
    // other custom ACLs" needs s3:PutBucketAcl, while "if you set the ACL to
    // private, or if you don't specify any ACLs, only the s3:CreateBucket
    // permission is required". Judged by the grants the request resolves to,
    // so the two spellings of an owner-only ACL agree.
    let reaches_past_owner = header(&req.headers, "x-amz-acl")
        .is_some_and(|acl| acl_reaches_past_owner(acl, &req.account_id));
    if reaches_past_owner || has_grant_headers(&req.headers) {
        needs.push(Need::Own("s3", "PutBucketAcl"));
    }
    if header(&req.headers, "x-amz-object-ownership").is_some() {
        needs.push(Need::Own("s3", "PutBucketOwnershipControls"));
    }
    if header(&req.headers, "x-amz-bucket-object-lock-enabled")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    {
        // Object Lock forces versioning on, so AWS wants both.
        needs.push(Need::Own("s3", "PutBucketObjectLockConfiguration"));
        needs.push(Need::Own("s3", "PutBucketVersioning"));
    }
    needs
}

/// DeleteObjects is authorized per key: `s3:DeleteObject` on each object it
/// names, `s3:DeleteObjectVersion` for an entry naming a version, plus
/// `s3:BypassGovernanceRetention` on each when the request asks to bypass.
/// A body naming no object is a MalformedXML request; it is authorized as
/// `s3:DeleteObject` on the bucket's objects so the handler can answer it.
fn delete_objects(req: &AwsRequest) -> Vec<Need> {
    let body = std::str::from_utf8(&req.body).unwrap_or("");
    let entries = parse_delete_objects_xml(body);
    let bypass = governance_bypass_requested(req);
    if entries.is_empty() {
        return vec![Need::Key("DeleteObject", String::new())];
    }
    entries
        .into_iter()
        .flat_map(|entry| {
            delete_entry_actions(entry.version_id.is_some(), bypass)
                .into_iter()
                .map(move |action| Need::Key(action, entry.key.clone()))
        })
        .collect()
}

/// The `s3:` actions deleting one DeleteObjects entry needs, on its object.
pub(crate) fn delete_entry_actions(versioned: bool, bypass: bool) -> Vec<&'static str> {
    let delete = if versioned {
        "DeleteObjectVersion"
    } else {
        "DeleteObject"
    };
    if bypass {
        vec![delete, "BypassGovernanceRetention"]
    } else {
        vec![delete]
    }
}

#[cfg(test)]
mod tests {
    use super::super::iam_reference::OPERATION_ACTIONS;
    use super::*;
    use bytes::Bytes;
    use http::Method;
    use std::collections::HashMap;

    fn req(query: &[(&str, &str)], headers: &[(&str, &str)], body: &str) -> AwsRequest {
        let mut h = HeaderMap::new();
        for (k, v) in headers {
            h.append(
                http::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        AwsRequest {
            service: "s3".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "rid".to_string(),
            headers: h,
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
            body: Bytes::from(body.to_string()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec!["b".to_string(), "k".to_string()],
            raw_path: "/b/k".to_string(),
            raw_query: String::new(),
            method: Method::GET,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    /// The `service:action` strings a request needs (source reads as
    /// `s3:GetObject`, the unversioned source).
    fn actions(op: &str, r: &AwsRequest) -> Vec<String> {
        required(op, r)
            .into_iter()
            .map(|n| match n {
                Need::Own(svc, a) => format!("{svc}:{a}"),
                Need::Key(a, _) => format!("s3:{a}"),
                Need::CopySource => "s3:GetObject".to_string(),
                Need::PassRole(_) => "iam:PassRole".to_string(),
            })
            .collect()
    }

    fn plain(op: &str) -> Vec<String> {
        actions(op, &req(&[], &[], ""))
    }

    /// Operations the service authorization reference lists no actions for,
    /// with the action fakecloud authorizes them under and where that comes
    /// from.
    const UNLISTED: &[(&str, &[&str])] = &[
        // Directory-bucket session operations ("Actions for directory buckets").
        ("CreateSession", &["s3express:CreateSession"]),
        ("RenameObject", &["s3express:CreateSession"]),
        (
            "ListDirectoryBuckets",
            &["s3express:ListAllMyDirectoryBuckets"],
        ),
        // Object Lambda's own action (s3-object-lambda reference).
        (
            "WriteGetObjectResponse",
            &["s3-object-lambda:WriteGetObjectResponse"],
        ),
        // The S3 action of the same name; the API reference says so for
        // PutBucketAbac ("You must have s3:PutBucketABAC permission") and
        // UpdateObjectEncryption.
        ("GetBucketAbac", &["s3:GetBucketAbac"]),
        ("PutBucketAbac", &["s3:PutBucketAbac"]),
        ("UpdateObjectEncryption", &["s3:UpdateObjectEncryption"]),
        (
            "UpdateBucketMetadataAnnotationTableConfiguration",
            &["s3:UpdateBucketMetadataAnnotationTableConfiguration"],
        ),
        (
            "GetObjectAnnotation",
            &["s3:GetObjectAnnotation", "s3:GetObjectVersionAnnotation"],
        ),
        (
            "ListObjectAnnotations",
            &[
                "s3:ListObjectAnnotations",
                "s3:ListObjectVersionAnnotations",
            ],
        ),
        (
            "PutObjectAnnotation",
            &["s3:PutObjectAnnotation", "s3:PutObjectVersionAnnotation"],
        ),
        (
            "DeleteObjectAnnotation",
            &[
                "s3:DeleteObjectAnnotation",
                "s3:DeleteObjectVersionAnnotation",
            ],
        ),
    ];

    /// Actions the API reference requires that the operation table leaves
    /// out: GetObjectAttributes needs `s3:GetObjectAttributes` (or
    /// `s3:GetObjectVersionAttributes`) as well as the read, and CreateBucket
    /// with tags needs `s3:TagResource`.
    const EXTRA: &[(&str, &str)] = &[
        ("GetObjectAttributes", "s3:GetObjectAttributes"),
        ("GetObjectAttributes", "s3:GetObjectVersionAttributes"),
        ("CreateBucket", "s3:TagResource"),
        // "You need the relevant read object (or version) permission".
        ("HeadObject", "s3:GetObjectVersion"),
    ];

    /// Every operation fakecloud maps, under every conditional form.
    fn variants(op: &str) -> Vec<AwsRequest> {
        vec![
            req(&[], &[], ""),
            req(&[("versionId", "v1")], &[], ""),
            req(
                &[],
                &[
                    ("x-amz-acl", "public-read"),
                    ("x-amz-grant-read", "id=abc"),
                    ("x-amz-tagging", "a=b"),
                    ("x-amz-object-lock-mode", "GOVERNANCE"),
                    ("x-amz-object-lock-legal-hold", "ON"),
                    ("x-amz-bypass-governance-retention", "true"),
                    ("x-amz-object-ownership", "BucketOwnerEnforced"),
                    ("x-amz-bucket-object-lock-enabled", "true"),
                ],
                if op == "DeleteObjects" {
                    "<Delete><Object><Key>a</Key></Object>\
                     <Object><Key>b</Key><VersionId>v</VersionId></Object></Delete>"
                } else if op == "CreateBucket" {
                    "<CreateBucketConfiguration><Tags><Tag><Key>k</Key><Value>v</Value>\
                     </Tag></Tags></CreateBucketConfiguration>"
                } else if op == "PutBucketReplication" {
                    "<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/r</Role>\
                     </ReplicationConfiguration>"
                } else {
                    ""
                },
            ),
        ]
    }

    /// Every action any fakecloud S3 operation can require is one the
    /// service authorization reference lists for that operation (or a
    /// documented addition), and every action exists in S3's action list.
    #[test]
    fn every_mapping_is_authorized_by_the_reference() {
        let mut mapped = 0;
        for (op, authorized) in OPERATION_ACTIONS {
            for r in variants(op) {
                let got = actions(op, &r);
                if got.is_empty() {
                    continue;
                }
                mapped += 1;
                let unlisted = UNLISTED.iter().find(|(o, _)| o == op).map(|(_, a)| *a);
                for action in got {
                    let ok = authorized.contains(&action.as_str())
                        || unlisted.is_some_and(|a| a.contains(&action.as_str()))
                        || EXTRA.contains(&(op, action.as_str()));
                    assert!(
                        ok,
                        "{op} requires {action}, which the reference does not list"
                    );
                    if let Some(name) = action.strip_prefix("s3:") {
                        assert!(
                            resource_kind(name).is_some(),
                            "{action} is not an S3 action"
                        );
                    }
                }
            }
        }
        assert!(mapped > 0);
        // Every unlisted operation is really unlisted, so the exceptions
        // cannot hide a reference entry.
        for (op, _) in UNLISTED {
            if let Some((_, authorized)) = OPERATION_ACTIONS.iter().find(|(o, _)| o == op) {
                assert!(authorized.is_empty(), "{op} is listed: {authorized:?}");
            }
        }
    }

    /// Every operation fakecloud routes has a mapping.
    #[test]
    fn every_routed_operation_is_mapped() {
        for op in super::super::IAM_MAPPED_OPERATIONS {
            assert!(!plain(op).is_empty(), "{op} has no IAM mapping");
        }
    }

    #[test]
    fn operations_map_to_policy_action_names() {
        for (op, want) in [
            ("ListBuckets", "s3:ListAllMyBuckets"),
            ("ListObjects", "s3:ListBucket"),
            ("ListObjectsV2", "s3:ListBucket"),
            ("HeadBucket", "s3:ListBucket"),
            ("ListObjectVersions", "s3:ListBucketVersions"),
            ("ListMultipartUploads", "s3:ListBucketMultipartUploads"),
            ("ListParts", "s3:ListMultipartUploadParts"),
            ("HeadObject", "s3:GetObject"),
            ("CreateMultipartUpload", "s3:PutObject"),
            ("UploadPart", "s3:PutObject"),
            ("CompleteMultipartUpload", "s3:PutObject"),
            (
                "GetBucketLifecycleConfiguration",
                "s3:GetLifecycleConfiguration",
            ),
            (
                "PutBucketLifecycleConfiguration",
                "s3:PutLifecycleConfiguration",
            ),
            ("DeleteBucketLifecycle", "s3:PutLifecycleConfiguration"),
            ("GetBucketEncryption", "s3:GetEncryptionConfiguration"),
            ("PutBucketEncryption", "s3:PutEncryptionConfiguration"),
            ("DeleteBucketEncryption", "s3:PutEncryptionConfiguration"),
            (
                "PutBucketNotificationConfiguration",
                "s3:PutBucketNotification",
            ),
            (
                "GetBucketNotificationConfiguration",
                "s3:GetBucketNotification",
            ),
            ("GetBucketCors", "s3:GetBucketCORS"),
            ("DeleteBucketCors", "s3:PutBucketCORS"),
            ("DeleteBucketTagging", "s3:PutBucketTagging"),
            ("GetBucketReplication", "s3:GetReplicationConfiguration"),
            ("DeleteBucketReplication", "s3:PutReplicationConfiguration"),
            ("GetPublicAccessBlock", "s3:GetBucketPublicAccessBlock"),
            ("DeletePublicAccessBlock", "s3:PutBucketPublicAccessBlock"),
            (
                "GetObjectLockConfiguration",
                "s3:GetBucketObjectLockConfiguration",
            ),
            (
                "PutObjectLockConfiguration",
                "s3:PutBucketObjectLockConfiguration",
            ),
            (
                "GetBucketAccelerateConfiguration",
                "s3:GetAccelerateConfiguration",
            ),
            (
                "ListBucketMetricsConfigurations",
                "s3:GetMetricsConfiguration",
            ),
            (
                "DeleteBucketAnalyticsConfiguration",
                "s3:PutAnalyticsConfiguration",
            ),
            (
                "DeleteBucketOwnershipControls",
                "s3:PutBucketOwnershipControls",
            ),
            (
                "GetBucketMetadataConfiguration",
                "s3:GetBucketMetadataTableConfiguration",
            ),
            ("SelectObjectContent", "s3:GetObject"),
            ("GetObjectTorrent", "s3:GetObject"),
        ] {
            assert_eq!(plain(op), vec![want.to_string()], "{op}");
        }
    }

    #[test]
    fn a_version_id_selects_the_version_action() {
        let v = req(
            &[("versionId", "3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY")],
            &[],
            "",
        );
        for (op, want) in [
            ("GetObject", vec!["s3:GetObjectVersion"]),
            ("HeadObject", vec!["s3:GetObjectVersion"]),
            ("DeleteObject", vec!["s3:DeleteObjectVersion"]),
            ("GetObjectAcl", vec!["s3:GetObjectVersionAcl"]),
            ("PutObjectAcl", vec!["s3:PutObjectVersionAcl"]),
            ("GetObjectTagging", vec!["s3:GetObjectVersionTagging"]),
            ("PutObjectTagging", vec!["s3:PutObjectVersionTagging"]),
            ("DeleteObjectTagging", vec!["s3:DeleteObjectVersionTagging"]),
            (
                "GetObjectAttributes",
                vec!["s3:GetObjectVersion", "s3:GetObjectVersionAttributes"],
            ),
            ("GetObjectAnnotation", vec!["s3:GetObjectVersionAnnotation"]),
            // No version variant exists for these.
            ("GetObjectRetention", vec!["s3:GetObjectRetention"]),
            ("PutObjectLegalHold", vec!["s3:PutObjectLegalHold"]),
            ("RestoreObject", vec!["s3:RestoreObject"]),
        ] {
            assert_eq!(actions(op, &v), want, "{op}");
        }
        assert_eq!(
            plain("GetObjectAttributes"),
            vec!["s3:GetObject", "s3:GetObjectAttributes"]
        );
        // A blank versionId names no version.
        let blank = req(&[("versionId", "")], &[], "");
        assert_eq!(actions("GetObject", &blank), vec!["s3:GetObject"]);
    }

    #[test]
    fn object_write_headers_add_their_permissions() {
        for op in ["PutObject", "CopyObject", "CreateMultipartUpload"] {
            let base: Vec<String> = plain(op);
            let tagged = actions(
                op,
                &req(
                    &[],
                    &[
                        ("x-amz-tagging", "a=b"),
                        ("x-amz-tagging-directive", "REPLACE"),
                    ],
                    "",
                ),
            );
            assert_eq!(tagged[..base.len()], base[..], "{op}");
            assert_eq!(tagged[base.len()..], ["s3:PutObjectTagging"], "{op}");
            for acl in [("x-amz-acl", "private"), ("x-amz-grant-read", "id=x")] {
                let with_acl = actions(op, &req(&[], &[acl], ""));
                assert_eq!(with_acl[base.len()..], ["s3:PutObjectAcl"], "{op} {acl:?}");
            }
            let locked = actions(
                op,
                &req(
                    &[],
                    &[
                        (
                            "x-amz-object-lock-retain-until-date",
                            "2030-01-01T00:00:00Z",
                        ),
                        ("x-amz-object-lock-legal-hold", "ON"),
                    ],
                    "",
                ),
            );
            assert_eq!(
                locked[base.len()..],
                ["s3:PutObjectRetention", "s3:PutObjectLegalHold"],
                "{op}"
            );
            // Blank headers ask for nothing.
            let blank = actions(
                op,
                &req(&[], &[("x-amz-tagging", ""), ("x-amz-acl", " ")], ""),
            );
            assert_eq!(blank, base, "{op}");
        }
        // Parts and completion take none of those headers' permissions.
        let part = req(&[], &[("x-amz-tagging", "a=b")], "");
        assert_eq!(actions("UploadPart", &part), vec!["s3:PutObject"]);
        assert_eq!(
            actions("CompleteMultipartUpload", &part),
            vec!["s3:PutObject"]
        );
    }

    #[test]
    fn copies_need_the_destination_write_and_the_source_read() {
        // The annotation permissions a copy of an annotated source needs are
        // the handler's to check (only it sees the source's annotations).
        for directive in [None, Some("COPY"), Some("EXCLUDE")] {
            let headers: Vec<_> = directive
                .map(|d| ("x-amz-object-annotation-directive", d))
                .into_iter()
                .collect();
            assert_eq!(
                required("CopyObject", &req(&[], &headers, "")),
                vec![Need::Own("s3", "PutObject"), Need::CopySource]
            );
        }
        assert_eq!(
            required("UploadPartCopy", &req(&[], &[], "")),
            vec![Need::Own("s3", "PutObject"), Need::CopySource]
        );
    }

    /// A copy's `x-amz-tagging` sets its tags only under
    /// `x-amz-tagging-directive: REPLACE`, so only then does it need
    /// `s3:PutObjectTagging`.
    #[test]
    fn copy_tagging_needs_its_permission_only_when_replacing() {
        let base = vec!["s3:PutObject", "s3:GetObject"];
        for directive in [None, Some("COPY")] {
            let mut headers = vec![("x-amz-tagging", "a=b")];
            headers.extend(directive.map(|d| ("x-amz-tagging-directive", d)));
            assert_eq!(actions("CopyObject", &req(&[], &headers, "")), base);
        }
        let replace = req(
            &[],
            &[
                ("x-amz-tagging", "a=b"),
                ("x-amz-tagging-directive", "REPLACE"),
            ],
            "",
        );
        assert_eq!(
            actions("CopyObject", &replace),
            vec!["s3:PutObject", "s3:GetObject", "s3:PutObjectTagging"]
        );
    }

    #[test]
    fn delete_objects_is_authorized_per_key() {
        let body = "<Delete><Object><Key>a</Key></Object>\
                    <Object><Key>b</Key><VersionId>v1</VersionId></Object></Delete>";
        assert_eq!(
            required("DeleteObjects", &req(&[], &[], body)),
            vec![
                Need::Key("DeleteObject", "a".to_string()),
                Need::Key("DeleteObjectVersion", "b".to_string()),
            ]
        );
        assert_eq!(
            required(
                "DeleteObjects",
                &req(&[], &[("x-amz-bypass-governance-retention", "true")], body)
            ),
            vec![
                Need::Key("DeleteObject", "a".to_string()),
                Need::Key("BypassGovernanceRetention", "a".to_string()),
                Need::Key("DeleteObjectVersion", "b".to_string()),
                Need::Key("BypassGovernanceRetention", "b".to_string()),
            ]
        );
    }

    #[test]
    fn governance_bypass_needs_its_permission() {
        let bypass = req(&[], &[("x-amz-bypass-governance-retention", "true")], "");
        assert_eq!(
            actions("DeleteObject", &bypass),
            vec!["s3:DeleteObject", "s3:BypassGovernanceRetention"]
        );
        assert_eq!(
            actions("PutObjectRetention", &bypass),
            vec!["s3:PutObjectRetention", "s3:BypassGovernanceRetention"]
        );
        // Only those operations honor the header.
        assert_eq!(actions("PutObject", &bypass), vec!["s3:PutObject"]);
    }

    #[test]
    fn create_bucket_extras_follow_what_it_configures() {
        assert_eq!(plain("CreateBucket"), vec!["s3:CreateBucket"]);
        let private = req(&[], &[("x-amz-acl", "private")], "");
        assert_eq!(actions("CreateBucket", &private), vec!["s3:CreateBucket"]);
        let public = req(&[], &[("x-amz-acl", "public-read")], "");
        assert_eq!(
            actions("CreateBucket", &public),
            vec!["s3:CreateBucket", "s3:PutBucketAcl"]
        );
        let locked = req(&[], &[("x-amz-bucket-object-lock-enabled", "true")], "");
        assert_eq!(
            actions("CreateBucket", &locked),
            vec![
                "s3:CreateBucket",
                "s3:PutBucketObjectLockConfiguration",
                "s3:PutBucketVersioning"
            ]
        );
    }

    #[test]
    fn resource_kinds_come_from_the_reference() {
        assert_eq!(resource_kind("GetObject"), Some(ResourceKind::Object));
        assert_eq!(resource_kind("ListBucket"), Some(ResourceKind::Bucket));
        assert_eq!(
            resource_kind("ListBucketVersions"),
            Some(ResourceKind::Bucket)
        );
        assert_eq!(
            resource_kind("ListMultipartUploadParts"),
            Some(ResourceKind::Object)
        );
        assert_eq!(
            resource_kind("ListAllMyBuckets"),
            Some(ResourceKind::Account)
        );
        assert_eq!(
            resource_kind("CreateAccessPoint"),
            Some(ResourceKind::Other)
        );
        assert_eq!(resource_kind("ListObjectsV2"), None);
        assert_eq!(resource_kind("HeadObject"), None);
    }
}
