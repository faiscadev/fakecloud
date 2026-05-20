use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::{HeaderMap, StatusCode};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError, ResponseBody};

use fakecloud_persistence::{BodyRef, BodySource};

use crate::persistence::object_meta_snapshot;
use crate::state::{AclGrant, S3Object};

use super::{
    canned_acl_grants_for_object, check_get_conditionals, check_head_conditionals,
    check_object_lock_for_overwrite, compute_checksum, deliver_notifications, etag_matches,
    extract_user_metadata, extract_xml_value, is_frozen, is_valid_storage_class,
    make_delete_marker, no_such_bucket, no_such_key, parse_delete_objects_quiet,
    parse_delete_objects_xml, parse_grant_headers, parse_range_header, parse_url_encoded_tags,
    precondition_failed, replicate_through_store, resolve_object, s3_xml, url_encode_s3_key,
    xml_escape, RangeResult, S3Service,
};

mod delete;
mod list;
mod read;
mod website;
mod write;

impl S3Service {}

/// Build the response body for a full GetObject read. For memory-backed
/// bodies this returns `ResponseBody::Bytes`; for disk-backed bodies it
/// opens the file handle eagerly (while the caller still holds the per-state
/// read guard) and returns `ResponseBody::File` so the dispatcher can stream
/// the file directly into the HTTP response without materializing it in RAM.
///
/// Opening the handle inside the read guard is load-bearing: on unix an open
/// fd keeps the old inode alive even after the path is renamed over or
/// unlinked, so a concurrent PUT/DELETE that lands after we drop the guard
/// cannot hand the reader a partial or swapped body.
fn full_body_response(
    state: &crate::state::S3State,
    body: &fakecloud_persistence::BodyRef,
) -> Result<ResponseBody, AwsServiceError> {
    match body {
        fakecloud_persistence::BodyRef::Memory(_) => {
            let bytes = state.read_body(body).map_err(crate::service::io_to_aws)?;
            Ok(ResponseBody::Bytes(bytes))
        }
        fakecloud_persistence::BodyRef::Disk { path, size, .. } => {
            let std_file = std::fs::File::open(path).map_err(crate::service::io_to_aws)?;
            let file = tokio::fs::File::from_std(std_file);
            Ok(ResponseBody::File { file, size: *size })
        }
    }
}
