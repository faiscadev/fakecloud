use http::StatusCode;

use chrono::{DateTime, Utc};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::persistence::object_meta_snapshot;

use crate::state::ObjectLockSettings;

use super::{
    empty_response, extract_xml_value, governance_bypass_requested, no_such_bucket, no_such_key,
    resolve_object, s3_xml, xml_escape, S3Service,
};

/// Whether `obj` is the version the request addressed. `"null"` addresses the
/// version written before versioning was enabled, which carries no id -- the
/// same rule the delete paths use. Delete markers never match: AWS rejects
/// lock operations on them, and writing lock metadata against one would
/// overwrite the live null object's sidecar (both map to the same slot).
fn version_matches(obj: &crate::state::S3Object, vid: &str) -> bool {
    if obj.is_delete_marker {
        return false;
    }
    obj.version_id.as_deref() == Some(vid) || (vid == "null" && obj.version_id.is_none())
}

/// Whether replacing `obj`'s retention with `mode` / `retain_until` must be
/// refused. While a retention is unexpired, shortening the retain-until date,
/// changing the mode, or clearing the retention is denied: always under
/// COMPLIANCE, and under GOVERNANCE unless the caller bypasses it. Keeping the
/// mode and extending (or restating) the date is always allowed.
fn retention_change_denied(
    obj: &crate::state::S3Object,
    mode: Option<&str>,
    retain_until: Option<DateTime<Utc>>,
    bypass: bool,
) -> bool {
    let (Some(current_mode), Some(current_until)) =
        (obj.lock_mode.as_deref(), obj.lock_retain_until)
    else {
        return false;
    };
    if current_until <= Utc::now() {
        return false;
    }
    let weakens = mode != Some(current_mode) || retain_until.is_none_or(|u| u < current_until);
    if !weakens {
        return false;
    }
    match current_mode {
        "COMPLIANCE" => true,
        "GOVERNANCE" => !bypass,
        _ => false,
    }
}

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
        let retain_until_raw = extract_xml_value(body_str, "RetainUntilDate");
        let retain_until = retain_until_raw
            .as_deref()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok());
        // A retention with a Mode must carry a valid RetainUntilDate. AWS
        // rejects a Mode-only (or unparseable-date) body with 400 MalformedXML
        // rather than persisting a mode that would later round-trip into the
        // x-amz-object-lock-mode header with no expiry.
        if mode.is_some() && retain_until.is_none() {
            return Err(malformed_object_lock(
                "RetainUntilDate",
                retain_until_raw.as_deref().unwrap_or(""),
            ));
        }

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        // An active retention can only be weakened as Object Lock allows.
        let current = match version_id.as_deref() {
            Some(vid) => b
                .object_versions
                .get(key)
                .and_then(|vs| vs.iter().find(|o| version_matches(o, vid)))
                .or_else(|| b.objects.get(key).filter(|o| version_matches(o, vid))),
            None => b.objects.get(key),
        };
        let bypass = governance_bypass_requested(req);
        if current
            .is_some_and(|obj| retention_change_denied(obj, mode.as_deref(), retain_until, bypass))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "AccessDenied",
                "Access Denied",
            ));
        }

        // Find and update the object (either current or specific version)
        if let Some(ref vid) = version_id {
            let mut found = false;
            if let Some(versions) = b.object_versions.get_mut(key) {
                for obj in versions.iter_mut() {
                    if version_matches(obj, vid) {
                        obj.lock_mode = mode.clone();
                        obj.lock_retain_until = retain_until;
                        found = true;
                        break;
                    }
                }
            }
            if let Some(obj) = b.objects.get_mut(key) {
                if version_matches(obj, vid) {
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
                        if version_matches(v, &vid) {
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
                    .and_then(|vs| vs.iter().find(|o| version_matches(o, vid)));
                let target =
                    versioned.or_else(|| b2.objects.get(key).filter(|o| version_matches(o, vid)));
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
        require_object_lock(b)?;
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
                    if version_matches(obj, vid) {
                        obj.lock_legal_hold = status.clone();
                        found = true;
                        break;
                    }
                }
            }
            if let Some(obj) = b.objects.get_mut(key) {
                if version_matches(obj, vid) {
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
                        if version_matches(v, &vid) {
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
                    .and_then(|vs| vs.iter().find(|o| version_matches(o, vid)));
                let target =
                    versioned.or_else(|| b2.objects.get(key).filter(|o| version_matches(o, vid)));
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
        require_object_lock(b)?;
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

/// GetObjectRetention / GetObjectLegalHold on a bucket created without Object
/// Lock. Recorded against S3: 400 InvalidRequest "Bucket is missing Object
/// Lock Configuration", whether or not the object exists.
fn require_object_lock(b: &crate::state::S3Bucket) -> Result<(), AwsServiceError> {
    if b.object_lock_config.is_some() {
        return Ok(());
    }
    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "Bucket is missing Object Lock Configuration",
    ))
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

/// The `x-amz-object-lock-*` request headers that set an object's lock.
const LOCK_MODE_HEADER: &str = "x-amz-object-lock-mode";
const LOCK_RETAIN_UNTIL_HEADER: &str = "x-amz-object-lock-retain-until-date";
const LOCK_LEGAL_HOLD_HEADER: &str = "x-amz-object-lock-legal-hold";

/// Whether the request carries any Object Lock parameter header.
pub(super) fn has_object_lock_headers(headers: &http::HeaderMap) -> bool {
    [
        LOCK_MODE_HEADER,
        LOCK_RETAIN_UNTIL_HEADER,
        LOCK_LEGAL_HOLD_HEADER,
    ]
    .iter()
    .any(|h| headers.contains_key(*h))
}

fn invalid_lock_argument(message: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgument", message)
}

/// Lock resolution for a new object version. PutObject, CopyObject and
/// CompleteMultipartUpload (with the headers given to CreateMultipartUpload)
/// take the explicit `x-amz-object-lock-*` request headers; a copy never
/// inherits the source object's lock. Every new version -- those, replicas
/// (which carry their source version's lock) and objects other services
/// deliver through `delivery::put_delivered_object` -- then falls back to the
/// bucket's default retention in one place, `commit_object_version_with`,
/// measured from the moment the version is committed.
impl ObjectLockSettings {
    /// The explicit lock a write request asks for, validated as S3 does. Lock
    /// headers are only accepted on a bucket with Object Lock configured; then
    /// mode and retain-until date must come together, the mode must be
    /// GOVERNANCE or COMPLIANCE, the date must be ISO 8601 and in the future,
    /// and the legal hold must be ON or OFF.
    pub(super) fn from_request(
        headers: &http::HeaderMap,
        bucket_lock_config: Option<&str>,
    ) -> Result<Self, AwsServiceError> {
        if has_object_lock_headers(headers) && bucket_lock_config.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Bucket is missing ObjectLockConfiguration",
            ));
        }
        // A value that is not visible ASCII cannot be a valid mode, date or
        // status; keep it (lossily) so it fails the checks below rather than
        // reading as absent.
        let header = |name: &str| {
            headers
                .get(name)
                .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        };
        let mode = header(LOCK_MODE_HEADER);
        let retain_until = header(LOCK_RETAIN_UNTIL_HEADER);
        let legal_hold = header(LOCK_LEGAL_HOLD_HEADER);

        if mode.is_some() != retain_until.is_some() {
            return Err(invalid_lock_argument(
                "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied",
            ));
        }
        let retain_until = match retain_until {
            Some(date) => Some(date.trim().parse::<DateTime<Utc>>().map_err(|_| {
                invalid_lock_argument("The retain until date must be provided in ISO 8601 format")
            })?),
            None => None,
        };
        if let Some(mode) = &mode {
            if mode != "GOVERNANCE" && mode != "COMPLIANCE" {
                return Err(invalid_lock_argument("Unknown wormMode directive."));
            }
        }
        if retain_until.is_some_and(|until| until <= Utc::now()) {
            return Err(invalid_lock_argument(
                "The retain until date must be in the future!",
            ));
        }
        if let Some(status) = &legal_hold {
            if status != "ON" && status != "OFF" {
                return Err(invalid_lock_argument(
                    "Legal Hold must be either of 'ON' or 'OFF'",
                ));
            }
        }
        Ok(Self {
            mode,
            retain_until,
            legal_hold,
        })
    }

    /// These settings, falling back to the bucket's default retention when no
    /// retention was asked for explicitly. The default is measured from `now`,
    /// the moment the version is created.
    pub(super) fn or_bucket_default(
        mut self,
        bucket_lock_config: Option<&str>,
        now: DateTime<Utc>,
    ) -> Self {
        if self.mode.is_some() || self.retain_until.is_some() {
            return self;
        }
        let Some(config) = bucket_lock_config else {
            return self;
        };
        let Some(mode) = extract_xml_value(config, "Mode") else {
            return self;
        };
        let days = extract_xml_value(config, "Days").and_then(|d| d.trim().parse::<i64>().ok());
        let years = extract_xml_value(config, "Years").and_then(|y| y.trim().parse::<i64>().ok());
        // The configuration is range-checked when stored; a value persisted
        // before that check must still not overflow. Years are calendar years
        // (leap days included), not multiples of 365 days.
        let until = match (days, years) {
            (Some(d), _) => chrono::Duration::try_days(d).and_then(|d| now.checked_add_signed(d)),
            (None, Some(y)) => y
                .checked_mul(12)
                .and_then(|m| u32::try_from(m).ok())
                .and_then(|m| now.checked_add_months(chrono::Months::new(m))),
            (None, None) => None,
        };
        if let Some(until) = until {
            self.mode = Some(mode);
            self.retain_until = Some(until);
        }
        self
    }
}
