use http::StatusCode;

use chrono::{DateTime, Utc};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::persistence::object_meta_snapshot;

use super::{
    empty_response, extract_xml_value, no_such_bucket, no_such_key, resolve_object, s3_xml,
    xml_escape, S3Service,
};

impl S3Service {
    pub(super) fn put_object_retention(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version_id = req.query_params.get("versionId").cloned();
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let mode = extract_xml_value(body_str, "Mode");
        // Mode is a closed enum in the S3 Object Lock schema. Reject anything
        // else with MalformedXML (as AWS does) instead of persisting an
        // arbitrary string that would later be round-tripped into the
        // `x-amz-object-lock-mode` response header.
        if let Some(ref m) = mode {
            if m != "GOVERNANCE" && m != "COMPLIANCE" {
                return Err(malformed_object_lock("Mode", m));
            }
        }
        let retain_until = extract_xml_value(body_str, "RetainUntilDate")
            .and_then(|s| s.parse::<DateTime<Utc>>().ok());

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        // Find and update the object (either current or specific version)
        if let Some(ref vid) = version_id {
            let mut found = false;
            if let Some(versions) = b.object_versions.get_mut(key) {
                for obj in versions.iter_mut() {
                    if obj.version_id.as_deref() == Some(vid) {
                        obj.lock_mode = mode.clone();
                        obj.lock_retain_until = retain_until;
                        found = true;
                        break;
                    }
                }
            }
            if let Some(obj) = b.objects.get_mut(key) {
                if obj.version_id.as_deref() == Some(vid) {
                    obj.lock_mode = mode;
                    obj.lock_retain_until = retain_until;
                    found = true;
                }
            }
            if !found {
                return Err(no_such_key(key));
            }
        } else {
            let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;
            obj.lock_mode = mode.clone();
            obj.lock_retain_until = retain_until;
            // Also update in object_versions if the current object has a version_id
            if let Some(ref vid) = obj.version_id {
                let vid = vid.clone();
                if let Some(versions) = b.object_versions.get_mut(key) {
                    for v in versions.iter_mut() {
                        if v.version_id.as_deref() == Some(&vid) {
                            v.lock_mode = mode.clone();
                            v.lock_retain_until = retain_until;
                            break;
                        }
                    }
                }
            }
        }

        // Snapshot the *specific* object that was just mutated — for a
        // versioned put, that's the version in b.object_versions, not the
        // current pointer.
        if let Some(b2) = state.buckets.get(bucket) {
            if let Some(ref vid) = version_id {
                let versioned = b2
                    .object_versions
                    .get(key)
                    .and_then(|vs| vs.iter().find(|o| o.version_id.as_deref() == Some(vid)));
                let target = versioned.or_else(|| {
                    b2.objects
                        .get(key)
                        .filter(|o| o.version_id.as_deref() == Some(vid))
                });
                if let Some(obj) = target {
                    let meta = object_meta_snapshot(obj);
                    self.store
                        .put_object_meta(bucket, key, Some(vid.as_str()), &meta)
                        .map_err(super::persistence_error)?;
                }
            } else if let Some(obj) = b2.objects.get(key) {
                let meta = object_meta_snapshot(obj);
                self.store
                    .put_object_meta(bucket, key, meta.version_id.as_deref(), &meta)
                    .map_err(super::persistence_error)?;
            }
        }
        Ok(empty_response(StatusCode::OK))
    }

    pub(super) fn get_object_retention(
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
        let obj = resolve_object(b, key, req.query_params.get("versionId"))?;

        match (&obj.lock_mode, &obj.lock_retain_until) {
            (Some(mode), Some(until)) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <Retention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <Mode>{}</Mode>\
                     <RetainUntilDate>{}</RetainUntilDate>\
                     </Retention>",
                    xml_escape(mode),
                    until.to_rfc3339(),
                );
                Ok(s3_xml(StatusCode::OK, body))
            }
            _ => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration",
            )),
        }
    }

    pub(super) fn put_object_legal_hold(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version_id = req.query_params.get("versionId").cloned();
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let status = extract_xml_value(body_str, "Status");
        // LegalHold Status is a closed enum (ON | OFF); reject anything else
        // with MalformedXML rather than persisting a header-unsafe string.
        if let Some(ref s) = status {
            if s != "ON" && s != "OFF" {
                return Err(malformed_object_lock("Status", s));
            }
        }

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        if let Some(ref vid) = version_id {
            let mut found = false;
            if let Some(versions) = b.object_versions.get_mut(key) {
                for obj in versions.iter_mut() {
                    if obj.version_id.as_deref() == Some(vid) {
                        obj.lock_legal_hold = status.clone();
                        found = true;
                        break;
                    }
                }
            }
            if let Some(obj) = b.objects.get_mut(key) {
                if obj.version_id.as_deref() == Some(vid) {
                    obj.lock_legal_hold = status;
                    found = true;
                }
            }
            if !found {
                return Err(no_such_key(key));
            }
        } else {
            let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;
            obj.lock_legal_hold = status.clone();
            // Also update in object_versions if the current object has a version_id
            if let Some(ref vid) = obj.version_id {
                let vid = vid.clone();
                if let Some(versions) = b.object_versions.get_mut(key) {
                    for v in versions.iter_mut() {
                        if v.version_id.as_deref() == Some(&vid) {
                            v.lock_legal_hold = status.clone();
                            break;
                        }
                    }
                }
            }
        }

        if let Some(b2) = state.buckets.get(bucket) {
            if let Some(ref vid) = version_id {
                let versioned = b2
                    .object_versions
                    .get(key)
                    .and_then(|vs| vs.iter().find(|o| o.version_id.as_deref() == Some(vid)));
                let target = versioned.or_else(|| {
                    b2.objects
                        .get(key)
                        .filter(|o| o.version_id.as_deref() == Some(vid))
                });
                if let Some(obj) = target {
                    let meta = object_meta_snapshot(obj);
                    self.store
                        .put_object_meta(bucket, key, Some(vid.as_str()), &meta)
                        .map_err(super::persistence_error)?;
                }
            } else if let Some(obj) = b2.objects.get(key) {
                let meta = object_meta_snapshot(obj);
                self.store
                    .put_object_meta(bucket, key, meta.version_id.as_deref(), &meta)
                    .map_err(super::persistence_error)?;
            }
        }
        Ok(empty_response(StatusCode::OK))
    }

    pub(super) fn get_object_legal_hold(
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
        let obj = resolve_object(b, key, req.query_params.get("versionId"))?;

        match &obj.lock_legal_hold {
            Some(hold) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <LegalHold xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <Status>{}</Status>\
                     </LegalHold>",
                    xml_escape(hold),
                );
                Ok(s3_xml(StatusCode::OK, body))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration",
            )),
        }
    }
}

/// Build a `MalformedXML` error for an object-lock field whose value isn't a
/// member of its closed enum (e.g. `Mode` other than GOVERNANCE/COMPLIANCE,
/// `Status` other than ON/OFF). Matches AWS, which rejects such bodies with
/// 400 MalformedXML.
fn malformed_object_lock(field: &str, value: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against \
         our published schema",
        vec![
            ("ArgumentName".to_string(), field.to_string()),
            ("ArgumentValue".to_string(), value.to_string()),
        ],
    )
}
