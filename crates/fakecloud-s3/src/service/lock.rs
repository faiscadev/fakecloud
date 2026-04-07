use http::StatusCode;

use chrono::{DateTime, Utc};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{
    empty_response, extract_xml_value, no_such_bucket, no_such_key, resolve_object, s3_xml,
    xml_escape, S3Service,
};

impl S3Service {
    pub(super) fn put_object_retention(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version_id = req.query_params.get("versionId").cloned();
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let mode = extract_xml_value(body_str, "Mode");
        let retain_until = extract_xml_value(body_str, "RetainUntilDate")
            .and_then(|s| s.parse::<DateTime<Utc>>().ok());

        let mut state = self.state.write();
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

        Ok(empty_response(StatusCode::OK))
    }

    pub(super) fn get_object_retention(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version_id = req.query_params.get("versionId").cloned();
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let status = extract_xml_value(body_str, "Status");

        let mut state = self.state.write();
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

        Ok(empty_response(StatusCode::OK))
    }

    pub(super) fn get_object_legal_hold(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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
