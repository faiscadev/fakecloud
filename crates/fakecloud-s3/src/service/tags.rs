use http::{HeaderMap, StatusCode};

use bytes::Bytes;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::persistence::object_meta_snapshot;

use super::{
    no_such_bucket, no_such_key, no_such_key_with_detail, parse_tagging_xml, s3_xml, xml_escape,
    S3Service,
};

impl S3Service {
    pub(super) fn get_object_tagging(
        &self,
        account_id: &str,
        _req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

        let mut tags_xml = String::new();
        for (k, v) in &obj.tags {
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
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(super) fn put_object_tagging(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let tags = parse_tagging_xml(body_str);

        // Validate: no aws: prefix
        for (k, _) in &tags {
            if k.starts_with("aws:") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidTag",
                    "System tags cannot be added/updated by requester",
                ));
            }
        }

        // Validate: max 10 tags
        if tags.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequest",
                "Object tags cannot be greater than 10",
            ));
        }

        let version_id = req.query_params.get("versionId").map(|s| s.to_string());

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let mut response_headers = HeaderMap::new();
        // The sidecar to rewrite: snapshotted from the very object the tags
        // landed on, so persistence can never pick a different version than
        // the in-memory update did (`versionId=null` matches an object with no
        // id, which an equality lookup would miss).
        let tagged: fakecloud_persistence::ObjectMeta;

        if let Some(ref vid) = version_id {
            // Version-specific tagging. `null` names the version of an
            // object written while versioning was off (or never on), the
            // same as `resolve_object` reads it: GetObjectTagging and
            // PutObjectTagging both accept `versionId=null` on an unversioned
            // bucket (recorded against S3).
            let mut snapshot = None;
            let is_version = |o: &crate::state::S3Object| {
                o.version_id.as_deref() == Some(vid.as_str())
                    || (vid == "null" && o.version_id.is_none())
            };

            // Check versioned objects. A live object wins over a delete
            // marker holding the same (null) version id.
            if let Some(versions) = b.object_versions.get_mut(key) {
                let pos = versions
                    .iter()
                    .position(|o| is_version(o) && !o.is_delete_marker)
                    .or_else(|| versions.iter().position(is_version));
                if let Some(obj) = pos.map(|i| &mut versions[i]) {
                    if obj.is_delete_marker {
                        return Err(AwsServiceError::aws_error_with_fields(
                            StatusCode::METHOD_NOT_ALLOWED,
                            "MethodNotAllowed",
                            "The specified method is not allowed against this resource.",
                            vec![
                                ("Method".to_string(), "PUT".to_string()),
                                ("ResourceType".to_string(), "DeleteMarker".to_string()),
                            ],
                        ));
                    }
                    obj.tags = tags.clone().into_iter().collect();
                    response_headers.insert("x-amz-version-id", vid.parse().unwrap());
                    snapshot = Some(object_meta_snapshot(obj));
                }
            }

            // Also check current object
            if snapshot.is_none() {
                if let Some(obj) = b.objects.get_mut(key) {
                    if is_version(obj) {
                        if obj.is_delete_marker {
                            return Err(AwsServiceError::aws_error_with_fields(
                                StatusCode::METHOD_NOT_ALLOWED,
                                "MethodNotAllowed",
                                "The specified method is not allowed against this resource.",
                                vec![
                                    ("Method".to_string(), "PUT".to_string()),
                                    ("ResourceType".to_string(), "DeleteMarker".to_string()),
                                ],
                            ));
                        }
                        obj.tags = tags.into_iter().collect();
                        response_headers.insert("x-amz-version-id", vid.parse().unwrap());
                        snapshot = Some(object_meta_snapshot(obj));
                    }
                }
            }

            // As `resolve_object` reads it: an unknown version id is
            // NoSuchVersion in a bucket that has (or had) versioning, and a
            // malformed one -- every id but `null` -- elsewhere (recorded:
            // PutObjectTagging `versionId=t` on an unversioned bucket is 400
            // InvalidArgument "Invalid version id specified").
            if snapshot.is_none() && b.versioning.is_none() && vid != "null" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "Invalid version id specified",
                ));
            }
            let Some(meta) = snapshot else {
                return Err(AwsServiceError::aws_error_with_fields(
                    StatusCode::NOT_FOUND,
                    "NoSuchVersion",
                    "The specified version does not exist.",
                    vec![
                        ("Key".to_string(), key.to_string()),
                        ("VersionId".to_string(), vid.to_string()),
                    ],
                ));
            };
            tagged = meta;
        } else {
            let obj = b
                .objects
                .get_mut(key)
                .ok_or_else(|| no_such_key_with_detail(key))?;
            if obj.is_delete_marker {
                return Err(no_such_key_with_detail(key));
            }
            obj.tags = tags.into_iter().collect();
            if let Some(ref vid) = obj.version_id {
                response_headers.insert("x-amz-version-id", vid.parse().unwrap());
            }
            tagged = object_meta_snapshot(obj);
        }

        // An object without an id and one with the id `null` share the same
        // sidecar slot, so the snapshot's own id addresses it either way.
        self.store
            .put_object_meta(bucket, key, tagged.version_id.as_deref(), &tagged)
            .map_err(super::persistence_error)?;

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: response_headers,
        })
    }

    // ---- Multipart Upload ----

    pub(super) fn delete_object_tagging(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;
        obj.tags.clear();
        let meta = object_meta_snapshot(obj);
        self.store
            .put_object_meta(bucket, key, meta.version_id.as_deref(), &meta)
            .map_err(super::persistence_error)?;
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: HeaderMap::new(),
        })
    }
}
