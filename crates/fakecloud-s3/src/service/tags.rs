use http::{HeaderMap, StatusCode};

use bytes::Bytes;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::persistence::object_meta_snapshot;

use super::{no_such_bucket, no_such_key, no_such_key_with_detail, s3_xml, xml_escape, S3Service};

fn malformed_xml() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published \
         schema",
    )
}

fn invalid_tag(message: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidTag", message)
}

/// The tag set of a tagging document (`<root><container><Tag><Key/><Value/>`
/// ...), read as XML: comments are ignored, text and CDATA decoded, element
/// names matched whatever their namespace or attributes (`<Tag >`). The one
/// reader of every XML tag set S3 takes (PutObjectTagging, PutBucketTagging,
/// S3 Control TagResource, CreateBucketConfiguration, a POST Object form's
/// `tagging`), and of the request tags IAM evaluates for them, so what is
/// authorized is what is stored. A document that is not well-formed, has
/// another root, or has a `Tag` without a `Key` or `Value` is 400
/// MalformedXML (recorded); an empty key is the validator's to judge.
pub(crate) fn read_tag_set(
    xml: &str,
    root: &str,
    container: &str,
) -> Result<Vec<(String, String)>, AwsServiceError> {
    use quick_xml::events::Event;
    let (root, container) = (root.as_bytes(), container.as_bytes());
    let xml = xml.strip_prefix('\u{feff}').unwrap_or(xml);
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut path: Vec<Vec<u8>> = Vec::new();
    let mut roots = 0usize;
    let mut tags = Vec::new();
    // The tag being read, and the text of its Key / Value so far.
    let mut current: Option<(Option<String>, Option<String>)> = None;
    let mut text = String::new();
    let in_tag = |path: &[Vec<u8>]| {
        path.len() == 3 && path[1].as_slice() == container && path[2].as_slice() == b"Tag"
    };
    loop {
        match reader.read_event().map_err(|_| malformed_xml())? {
            Event::Start(e) => {
                let name = e.local_name().as_ref().to_vec();
                if path.is_empty() {
                    roots += 1;
                    if roots > 1 || name != root {
                        return Err(malformed_xml());
                    }
                }
                path.push(name);
                text.clear();
                if in_tag(&path) {
                    current = Some((None, None));
                }
            }
            Event::Empty(e) => {
                let name = e.local_name().as_ref().to_vec();
                if path.is_empty() {
                    return Err(malformed_xml());
                }
                let mut at = path.clone();
                at.push(name.clone());
                if in_tag(&at) {
                    // `<Tag/>` has no key.
                    return Err(malformed_xml());
                }
                if in_tag(&path) {
                    // `<Key/>` / `<Value/>`: present and empty.
                    if let Some((key, value)) = current.as_mut() {
                        match name.as_slice() {
                            b"Key" => *key = Some(String::new()),
                            b"Value" => *value = Some(String::new()),
                            _ => {}
                        }
                    }
                }
            }
            Event::Text(t) => {
                let decoded = t.unescape().map_err(|_| malformed_xml())?;
                if path.is_empty() && !decoded.trim().is_empty() {
                    return Err(malformed_xml());
                }
                text.push_str(&decoded);
            }
            Event::CData(c) => {
                text.push_str(&String::from_utf8_lossy(&c.into_inner()));
            }
            Event::End(_) => {
                let was_tag = in_tag(&path);
                let Some(name) = path.pop() else {
                    return Err(malformed_xml());
                };
                if was_tag {
                    match current.take() {
                        Some((Some(key), Some(value))) => tags.push((key, value)),
                        _ => return Err(malformed_xml()),
                    }
                } else if in_tag(&path) {
                    if let Some((key, value)) = current.as_mut() {
                        match name.as_slice() {
                            b"Key" => *key = Some(std::mem::take(&mut text)),
                            b"Value" => *value = Some(std::mem::take(&mut text)),
                            _ => {}
                        }
                    }
                }
                text.clear();
            }
            Event::Eof => break,
            // Comments, processing instructions, the declaration.
            _ => {}
        }
    }
    if roots != 1 || !path.is_empty() {
        return Err(malformed_xml());
    }
    Ok(tags)
}

/// An object's tag set from a `Tagging` document (PutObjectTagging's body, a
/// POST Object form's `tagging` field), read by [`read_tag_set`] and checked
/// by [`validate_object_tags`].
pub(crate) fn parse_object_tagging(xml: &str) -> Result<Vec<(String, String)>, AwsServiceError> {
    let tags = read_tag_set(xml, "Tagging", "TagSet")?;
    validate_object_tags(&tags)?;
    Ok(tags)
}

/// Whether a tag key uses only the characters S3 allows: Unicode letters and
/// digits, spaces, and `+ - = . _ : / @` (S3 user guide, object tagging;
/// recorded: `a*b` is refused).
fn tag_key_chars_valid(key: &str) -> bool {
    key.chars().all(|c| {
        c.is_alphanumeric() || (c.is_whitespace() && !c.is_control()) || "+-=._:/@".contains(c)
    })
}

/// The checks every object tag set gets (recorded on S3): a key must be 1-128
/// characters of the allowed set (InvalidTag "The TagKey you have provided
/// is invalid"), not `aws:` prefixed (InvalidTag "Your TagKey cannot be
/// prefixed with aws:"), and appear once (InvalidTag "Cannot provide multiple
/// Tags with the same key"); a value at most 256 characters, empty allowed
/// (InvalidTag "The TagValue you have provided is invalid"); at most 10 tags
/// (BadRequest).
pub(crate) fn validate_object_tags(tags: &[(String, String)]) -> Result<(), AwsServiceError> {
    let mut seen = std::collections::HashSet::new();
    for (key, value) in tags {
        if key.is_empty() || key.chars().count() > 128 || !tag_key_chars_valid(key) {
            return Err(invalid_tag("The TagKey you have provided is invalid"));
        }
        if value.chars().count() > 256 {
            return Err(invalid_tag("The TagValue you have provided is invalid"));
        }
        if key.starts_with("aws:") {
            return Err(invalid_tag("Your TagKey cannot be prefixed with aws:"));
        }
        if !seen.insert(key.as_str()) {
            return Err(invalid_tag(
                "Cannot provide multiple Tags with the same key",
            ));
        }
    }
    if tags.len() > 10 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            "Object tags cannot be greater than 10",
        ));
    }
    Ok(())
}

/// An object's tag set from the `x-amz-tagging` header (PutObject,
/// CopyObject with REPLACE, CreateMultipartUpload): URL-encoded query
/// parameters. Recorded on S3 (PutObject, 2026-11-12): an empty key (`=1`)
/// and a repeated one (`a=1&a=2`) are both InvalidArgument with the message
/// below; otherwise the tags are checked as any object tag set.
pub(crate) fn parse_tagging_header(value: &str) -> Result<Vec<(String, String)>, AwsServiceError> {
    let tags = super::parse_url_encoded_tags(value);
    let mut seen = std::collections::HashSet::new();
    if tags
        .iter()
        .any(|(key, _)| key.is_empty() || !seen.insert(key.as_str()))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query \
             parameters without tag name duplicates.",
        ));
    }
    validate_object_tags(&tags)?;
    Ok(tags)
}

impl S3Service {
    pub(super) fn get_object_tagging(
        &self,
        account_id: &str,
        req: &AwsRequest,
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
        // A specific version reads that version's tags, resolved as every
        // versioned read resolves it (NoSuchVersion in a versioned bucket, 400
        // InvalidArgument for a malformed id elsewhere, `null` naming the
        // version written while versioning was off).
        let version_id = req.query_params.get("versionId");
        let obj = match version_id {
            Some(_) => super::resolve_object(b, key, version_id)?,
            None => b.objects.get(key).ok_or_else(|| no_such_key(key))?,
        };
        if obj.is_delete_marker {
            return Err(if version_id.is_some() {
                delete_marker_not_allowed("GET")
            } else {
                no_such_key(key)
            });
        }

        let mut headers = HeaderMap::new();
        if let Some(vid) = obj.version_id.as_deref().and_then(|v| v.parse().ok()) {
            headers.insert("x-amz-version-id", vid);
        }

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
        let mut resp = s3_xml(StatusCode::OK, body);
        resp.headers.extend(headers);
        Ok(resp)
    }

    pub(super) fn put_object_tagging(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let tags = parse_object_tagging(body_str)?;

        let version_id = req.query_params.get("versionId").map(|s| s.to_string());

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let (tagged, reported_version) =
            replace_object_tags(b, key, version_id.as_deref(), tags, "PUT")?;
        let mut response_headers = HeaderMap::new();
        if let Some(vid) = reported_version.and_then(|v| v.parse().ok()) {
            response_headers.insert("x-amz-version-id", vid);
        }

        // An object without an id and one with the id `null` share the same
        // sidecar slot, so the snapshot's own id addresses it either way.
        self.store
            .put_object_meta(account_id, bucket, key, tagged.version_id.as_deref(), &tagged)
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
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version_id = req.query_params.get("versionId").map(|s| s.to_string());
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let (meta, reported_version) =
            replace_object_tags(b, key, version_id.as_deref(), Vec::new(), "DELETE")?;
        self.store
            .put_object_meta(account_id, bucket, key, meta.version_id.as_deref(), &meta)
            .map_err(super::persistence_error)?;
        let mut headers = HeaderMap::new();
        if let Some(vid) = reported_version.and_then(|v| v.parse().ok()) {
            headers.insert("x-amz-version-id", vid);
        }
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers,
        })
    }
}

/// Replace the tag set of `key` (or of its version `version_id`) and return
/// the snapshot of the object the tags landed on, for its sidecar, with the
/// version id the response reports.
///
/// The sidecar is snapshotted from the very object the tags landed on, so
/// persistence can never pick a different version than the in-memory update
/// did (`versionId=null` matches an object with no id, which an equality
/// lookup would miss).
fn replace_object_tags(
    b: &mut crate::state::S3Bucket,
    key: &str,
    version_id: Option<&str>,
    tags: Vec<(String, String)>,
    method: &str,
) -> Result<(fakecloud_persistence::ObjectMeta, Option<String>), AwsServiceError> {
    let Some(vid) = version_id else {
        // The current object, through its own version so the copy of it in
        // `object_versions` is retagged with it.
        let obj = b
            .objects
            .get(key)
            .ok_or_else(|| no_such_key_with_detail(key))?;
        if obj.is_delete_marker {
            return Err(no_such_key_with_detail(key));
        }
        let current = obj.version_id.clone();
        let vid = current.as_deref().unwrap_or("null").to_string();
        let (meta, _) = replace_object_tags(b, key, Some(&vid), tags, method)?;
        return Ok((meta, current));
    };

    // Version-specific tagging. `null` names the version of an object written
    // while versioning was off (or never on), the same as `resolve_object`
    // reads it: GetObjectTagging and PutObjectTagging both accept
    // `versionId=null` on an unversioned bucket (recorded against S3).
    let is_version = |o: &crate::state::S3Object| {
        o.version_id.as_deref() == Some(vid) || (vid == "null" && o.version_id.is_none())
    };
    let mut snapshot = None;

    // Check versioned objects. A live object wins over a delete marker
    // holding the same (null) version id.
    if let Some(versions) = b.object_versions.get_mut(key) {
        let pos = versions
            .iter()
            .position(|o| is_version(o) && !o.is_delete_marker)
            .or_else(|| versions.iter().position(is_version));
        if let Some(obj) = pos.map(|i| &mut versions[i]) {
            if obj.is_delete_marker {
                return Err(delete_marker_not_allowed(method));
            }
            obj.tags = tags.iter().cloned().collect();
            snapshot = Some(object_meta_snapshot(obj));
        }
    }

    // The current object is its own copy of the newest version (and of the
    // null version on a Suspended bucket), kept beside the one in
    // `object_versions`: retag it too when it is the version named, or reads
    // of the current object keep the old tags.
    if let Some(obj) = b.objects.get_mut(key) {
        if is_version(obj) && !(snapshot.is_some() && obj.is_delete_marker) {
            if obj.is_delete_marker {
                return Err(delete_marker_not_allowed(method));
            }
            obj.tags = tags.into_iter().collect();
            if snapshot.is_none() {
                snapshot = Some(object_meta_snapshot(obj));
            }
        }
    }

    // As `resolve_object` reads it: an unknown version id is NoSuchVersion
    // in a bucket that has (or had) versioning, and a malformed one -- every
    // id but `null` -- elsewhere (recorded: PutObjectTagging `versionId=t` on
    // an unversioned bucket is 400 InvalidArgument "Invalid version id
    // specified").
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
    Ok((meta, Some(vid.to_string())))
}

/// The 405 S3 answers a tagging request that names a delete marker's version.
fn delete_marker_not_allowed(method: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::METHOD_NOT_ALLOWED,
        "MethodNotAllowed",
        "The specified method is not allowed against this resource.",
        vec![
            ("Method".to_string(), method.to_string()),
            ("ResourceType".to_string(), "DeleteMarker".to_string()),
        ],
    )
}
