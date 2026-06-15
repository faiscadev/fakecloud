//! `S3Service` `delete` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    pub(crate) fn delete_object(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let if_match = req
            .headers
            .get("if-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let version_id_param = req.query_params.get("versionId").cloned();

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let region = state.region.clone();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        if let Some(ref if_match_val) = if_match {
            match b.objects.get(key) {
                Some(existing) => {
                    let existing_etag = format!("\"{}\"", existing.etag);
                    if !etag_matches(if_match_val, &existing_etag) {
                        return Err(precondition_failed("If-Match"));
                    }
                }
                None => {
                    return Err(no_such_key(key));
                }
            }
        }

        let mut resp_headers = HeaderMap::new();
        let versioning_enabled = b.versioning.as_deref() == Some("Enabled");

        // Delete a specific version
        if let Some(ref vid) = version_id_param {
            // Check object lock before deleting a specific version
            let locked_obj = {
                let mut found: Option<&S3Object> = None;
                if let Some(versions) = b.object_versions.get(key) {
                    found = versions
                        .iter()
                        .find(|o| o.version_id.as_deref() == Some(vid.as_str()));
                }
                if found.is_none() {
                    if let Some(obj) = b.objects.get(key) {
                        let matches = obj.version_id.as_deref() == Some(vid.as_str())
                            || (vid == "null" && obj.version_id.is_none());
                        if matches {
                            found = Some(obj);
                        }
                    }
                }
                found.and_then(|obj| {
                    if obj.is_delete_marker {
                        return None;
                    }
                    // Legal hold blocks delete
                    if obj.lock_legal_hold.as_deref() == Some("ON") {
                        return Some("AccessDenied");
                    }
                    // Retention check
                    if let (Some(mode), Some(until)) = (&obj.lock_mode, &obj.lock_retain_until) {
                        if *until > Utc::now() {
                            if mode == "COMPLIANCE" {
                                return Some("AccessDenied");
                            }
                            if mode == "GOVERNANCE" {
                                // Check bypass header
                                let bypass = req
                                    .headers
                                    .get("x-amz-bypass-governance-retention")
                                    .and_then(|v| v.to_str().ok())
                                    .map(|s| s.eq_ignore_ascii_case("true"))
                                    .unwrap_or(false);
                                if !bypass {
                                    return Some("AccessDenied");
                                }
                            }
                        }
                    }
                    None
                })
            };
            if let Some(code) = locked_obj {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    code,
                    "Access Denied",
                ));
            }

            let mut is_dm = false;
            if let Some(versions) = b.object_versions.get_mut(key) {
                let vid_matches = |o: &S3Object| {
                    o.version_id.as_deref() == Some(vid.as_str())
                        || (vid == "null" && o.version_id.is_none())
                };
                is_dm = versions
                    .iter()
                    .any(|o| vid_matches(o) && o.is_delete_marker);
                let len_before = versions.len();
                versions.retain(|o| !vid_matches(o));
                let removed = len_before != versions.len();
                // Only update current object if we actually removed a version
                if removed {
                    if let Some(latest) = versions.last() {
                        if latest.is_delete_marker {
                            b.objects.remove(key);
                        } else {
                            b.objects.insert(key.to_string(), latest.clone());
                        }
                    } else {
                        b.objects.remove(key);
                    }
                }
                if versions.is_empty() {
                    b.object_versions.remove(key);
                }
            } else if let Some(obj) = b.objects.get(key) {
                // Match explicit version id, or treat "null" as matching objects with no version
                let matches = obj.version_id.as_deref() == Some(vid.as_str())
                    || (vid == "null" && obj.version_id.is_none());
                if matches {
                    is_dm = obj.is_delete_marker;
                    b.objects.remove(key);
                }
            }
            if let Ok(hv) = vid.parse() {
                resp_headers.insert("x-amz-version-id", hv);
            }
            if is_dm {
                resp_headers.insert("x-amz-delete-marker", "true".parse().unwrap());
            }
            self.store
                .delete_object(bucket, key, Some(vid.as_str()))
                .map_err(crate::service::persistence_error)?;
            return Ok(AwsResponse {
                status: StatusCode::NO_CONTENT,
                content_type: "application/xml".to_string(),
                body: Bytes::new().into(),
                headers: resp_headers,
            });
        }

        // Check object lock for non-version-specific deletes on non-versioned buckets
        if !versioning_enabled {
            if let Some(existing) = b.objects.get(key) {
                if !existing.is_delete_marker {
                    if let Some(code) = check_object_lock_for_overwrite(existing, req) {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::FORBIDDEN,
                            code,
                            "Access Denied",
                        ));
                    }
                }
            }
        }

        // Versioned bucket: create a delete marker
        if versioning_enabled {
            // If the existing object was created before versioning, preserve it
            if !b.object_versions.contains_key(key) {
                if let Some(existing) = b.objects.get(key) {
                    let mut preserved = existing.clone();
                    if preserved.version_id.is_none() {
                        preserved.version_id = Some("null".to_string());
                    }
                    b.object_versions
                        .entry(key.to_string())
                        .or_default()
                        .push(preserved);
                }
            }
            let dm_id = Uuid::new_v4().to_string();
            let marker = make_delete_marker(key, &dm_id);
            let marker_meta = object_meta_snapshot(&marker);
            b.object_versions
                .entry(key.to_string())
                .or_default()
                .push(marker.clone());
            b.objects.insert(key.to_string(), marker);
            resp_headers.insert("x-amz-version-id", dm_id.parse().unwrap());
            resp_headers.insert("x-amz-delete-marker", "true".parse().unwrap());
            self.store
                .delete_object(bucket, key, None)
                .map_err(crate::service::persistence_error)?;
            self.store
                .put_object(
                    bucket,
                    key,
                    Some(dm_id.as_str()),
                    BodySource::Bytes(Bytes::new()),
                    &marker_meta,
                )
                .map_err(crate::service::persistence_error)?;

            // Notification for delete
            let notification_config = b.notification_config.clone();
            let bucket_name = bucket.to_string();
            let obj_key = key.to_string();
            let region = region.clone();
            drop(accts);
            if let Some(ref config) = notification_config {
                deliver_notifications(
                    &self.delivery,
                    config,
                    &crate::service::notifications::ObjectEvent {
                        event_name: "ObjectRemoved:DeleteMarkerCreated",
                        bucket_name: &bucket_name,
                        key: &obj_key,
                        size: 0,
                        etag: "",
                        region: &region,
                    },
                    Some(&self.state),
                );
            }

            return Ok(AwsResponse {
                status: StatusCode::NO_CONTENT,
                content_type: "application/xml".to_string(),
                body: Bytes::new().into(),
                headers: resp_headers,
            });
        }

        // Capture notification config before removing
        let notification_config = b.notification_config.clone();
        let bucket_name = bucket.to_string();
        let obj_key = key.to_string();

        b.objects.remove(key);
        self.store
            .delete_object(bucket, key, None)
            .map_err(crate::service::persistence_error)?;
        drop(accts);

        // Deliver S3 event notifications
        if let Some(ref config) = notification_config {
            deliver_notifications(
                &self.delivery,
                config,
                &crate::service::notifications::ObjectEvent {
                    event_name: "ObjectRemoved:Delete",
                    bucket_name: &bucket_name,
                    key: &obj_key,
                    size: 0,
                    etag: "",
                    region: &region,
                },
                Some(&self.state),
            );
        }

        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: HeaderMap::new(),
        })
    }

    pub(crate) fn delete_objects(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let entries = parse_delete_objects_xml(body_str);
        let quiet = parse_delete_objects_quiet(body_str);

        if entries.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
            ));
        }

        // AWS caps a single DeleteObjects request at 1000 objects and
        // rejects anything larger with a 400 MalformedXML.
        if entries.len() > 1000 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
            ));
        }

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let bypass = req
            .headers
            .get("x-amz-bypass-governance-retention")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let versioning_enabled = b.versioning.as_deref() == Some("Enabled");
        let mut deleted_xml = String::new();
        let mut error_xml = String::new();
        for entry in &entries {
            let key = &entry.key;
            if let Some(ref vid) = entry.version_id {
                // Check lock before deleting specific version
                let lock_denied = {
                    let obj_opt = b
                        .object_versions
                        .get(key)
                        .and_then(|vs| {
                            vs.iter()
                                .find(|o| o.version_id.as_deref() == Some(vid.as_str()))
                        })
                        .or_else(|| {
                            b.objects.get(key).filter(|o| {
                                o.version_id.as_deref() == Some(vid.as_str())
                                    || (vid == "null" && o.version_id.is_none())
                            })
                        });
                    if let Some(obj) = obj_opt {
                        if obj.is_delete_marker {
                            false
                        } else if obj.lock_legal_hold.as_deref() == Some("ON") {
                            true
                        } else if let (Some(mode), Some(until)) =
                            (&obj.lock_mode, &obj.lock_retain_until)
                        {
                            if *until > Utc::now() {
                                if mode == "COMPLIANCE" {
                                    true
                                } else if mode == "GOVERNANCE" {
                                    !bypass
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if lock_denied {
                    error_xml.push_str(&format!(
                        "<Error><Key>{}</Key><VersionId>{}</VersionId><Code>AccessDenied</Code><Message>Access Denied because object protected by object lock.</Message></Error>",
                        xml_escape(key),
                        xml_escape(vid),
                    ));
                    continue;
                }

                // Delete specific version. Look in object_versions first;
                // if absent, treat b.objects as the implicit "null" version
                // slot — otherwise unversioned-bucket batch deletes that
                // target a vid match still report Deleted while leaving
                // the object in place.
                if let Some(versions) = b.object_versions.get_mut(key) {
                    versions.retain(|o| {
                        !(o.version_id.as_deref() == Some(vid)
                            || (vid == "null" && o.version_id.is_none()))
                    });
                    if let Some(latest) = versions.last() {
                        if latest.is_delete_marker {
                            b.objects.remove(key);
                        } else {
                            b.objects.insert(key.to_string(), latest.clone());
                        }
                    } else {
                        b.objects.remove(key);
                    }
                    if versions.is_empty() {
                        b.object_versions.remove(key);
                    }
                } else if let Some(obj) = b.objects.get(key) {
                    let matches = obj.version_id.as_deref() == Some(vid.as_str())
                        || (vid == "null" && obj.version_id.is_none());
                    if matches {
                        b.objects.remove(key);
                    }
                }
                self.store
                    .delete_object(bucket, key, Some(vid.as_str()))
                    .map_err(crate::service::persistence_error)?;
                if !quiet {
                    deleted_xml.push_str(&format!(
                        "<Deleted><Key>{}</Key><VersionId>{}</VersionId></Deleted>",
                        xml_escape(key),
                        xml_escape(vid),
                    ));
                }
            } else if versioning_enabled {
                // Preserve any pre-versioning object as a "null" version
                // before stacking the delete marker on top, otherwise
                // the existing data is shadowed by the marker and lost
                // from the version history.
                if !b.object_versions.contains_key(key.as_str()) {
                    if let Some(existing) = b.objects.get(key.as_str()) {
                        let mut preserved = existing.clone();
                        if preserved.version_id.is_none() {
                            preserved.version_id = Some("null".to_string());
                        }
                        b.object_versions
                            .entry(key.to_string())
                            .or_default()
                            .push(preserved);
                    }
                }
                let dm_id = Uuid::new_v4().to_string();
                let marker = make_delete_marker(key, &dm_id);
                b.object_versions
                    .entry(key.to_string())
                    .or_default()
                    .push(marker.clone());
                b.objects.insert(key.to_string(), marker);
                self.store
                    .delete_object(bucket, key, None)
                    .map_err(crate::service::persistence_error)?;
                if !quiet {
                    deleted_xml.push_str(&format!(
                        "<Deleted><Key>{}</Key><DeleteMarker>true</DeleteMarker><DeleteMarkerVersionId>{}</DeleteMarkerVersionId></Deleted>",
                        xml_escape(key), dm_id,
                    ));
                }
            } else {
                // Mirror single-DeleteObject's lock check: an
                // unversioned-bucket batch delete must respect
                // COMPLIANCE retention and legal-hold per key,
                // otherwise compliance can be sidestepped via the
                // batch endpoint.
                let lock_denied = b
                    .objects
                    .get(key)
                    .filter(|existing| !existing.is_delete_marker)
                    .and_then(|existing| check_object_lock_for_overwrite(existing, req));
                if let Some(code) = lock_denied {
                    error_xml.push_str(&format!(
                        "<Error><Key>{}</Key><Code>{}</Code><Message>Access Denied</Message></Error>",
                        xml_escape(key),
                        code,
                    ));
                    continue;
                }
                b.objects.remove(key);
                self.store
                    .delete_object(bucket, key, None)
                    .map_err(crate::service::persistence_error)?;
                if !quiet {
                    deleted_xml.push_str(&format!(
                        "<Deleted><Key>{}</Key></Deleted>",
                        xml_escape(key)
                    ));
                }
            }
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {deleted_xml}\
             {error_xml}\
             </DeleteResult>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }
}
