//! `S3Service` `delete` family — extracted from service.rs by audit-2026-05-19.

use super::*;

/// The object a no-`versionId` DELETE would destroy, for object-lock
/// purposes. A never-versioned bucket removes the current object; a
/// suspended bucket replaces the null version (which may sit in the version
/// history behind a newer enabled-era version); an enabled bucket only
/// stacks a delete marker and destroys nothing.
fn lock_target<'a>(
    b: &'a crate::state::S3Bucket,
    key: &str,
    versioning_configured: bool,
    versioning_enabled: bool,
) -> Option<&'a S3Object> {
    let is_null = |o: &S3Object| o.version_id.is_none() || o.version_id.as_deref() == Some("null");
    // A null-id delete marker in the history carries no data and no lock, but
    // a live null object can still be current behind it (suspended puts do not
    // append to the history), so fall through to the current object rather
    // than treating the marker as "nothing to check".
    let live_null = |o: &&S3Object| is_null(o) && !o.is_delete_marker;
    if !versioning_configured {
        b.objects.get(key).filter(|o| !o.is_delete_marker)
    } else if !versioning_enabled {
        b.object_versions
            .get(key)
            .and_then(|versions| versions.iter().find(live_null))
            .or_else(|| b.objects.get(key).filter(live_null))
    } else {
        None
    }
}

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
            match b.objects.get(key).filter(|o| !o.is_delete_marker) {
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
        // Enabled *or* Suspended: once a bucket has been versioned, even its
        // pre-versioning object is addressable as the "null" version and AWS
        // reports that id on the event.
        let versioning_configured = b.versioning.is_some();

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
                            if mode == "GOVERNANCE" && !governance_bypass_requested(req) {
                                return Some("AccessDenied");
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
            let mut removed_version = false;
            // The version id to report on the notification: the one the
            // removed object actually carried. An object stored before any
            // versioning was configured has none, and AWS then reports no
            // versionId -- whereas a real version still has one after
            // versioning is Suspended, so the bucket's current status is the
            // wrong thing to gate on.
            let mut removed_vid: Option<String> = None;
            if let Some(versions) = b.object_versions.get_mut(key) {
                let vid_matches = |o: &S3Object| {
                    o.version_id.as_deref() == Some(vid.as_str())
                        || (vid == "null" && o.version_id.is_none())
                };
                is_dm = versions
                    .iter()
                    .any(|o| vid_matches(o) && o.is_delete_marker);
                removed_vid = versions
                    .iter()
                    .find(|o| vid_matches(o))
                    .and_then(|o| o.version_id.clone());
                let len_before = versions.len();
                versions.retain(|o| !vid_matches(o));
                let removed = len_before != versions.len();
                removed_version = removed;
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
                    removed_version = true;
                    removed_vid = obj.version_id.clone();
                    b.objects.remove(key);
                }
            }
            // A matched object with no stored version id is the "null"
            // version; report it as such once the bucket has been versioned.
            if removed_version && removed_vid.is_none() && versioning_configured {
                removed_vid = Some("null".to_string());
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

            // Permanently deleting a version fires ObjectRemoved:Delete
            // carrying that version id, exactly as on real S3.
            let notification_config = b.notification_config.clone();
            let bucket_name = bucket.to_string();
            let obj_key = key.to_string();
            drop(accts);
            if removed_version {
                if let Some(ref config) = notification_config {
                    deliver_notifications(
                        &self.delivery,
                        config,
                        &crate::service::notifications::ObjectEvent {
                            event_name: "ObjectRemoved:Delete",
                            bucket_name: &bucket_name,
                            requester_account: &req.account_id,
                            key: &obj_key,
                            size: 0,
                            etag: "",
                            region: &region,
                            version_id: removed_vid.as_deref(),
                        },
                        Some(&self.state),
                    );
                }
            }
            return Ok(AwsResponse {
                status: StatusCode::NO_CONTENT,
                content_type: "application/xml".to_string(),
                body: Bytes::new().into(),
                headers: resp_headers,
            });
        }

        // Object lock only bites on what this delete actually destroys: a
        // never-versioned bucket loses the current object, and a suspended
        // one loses the null version the marker replaces (which is not
        // necessarily the current object -- an enabled-era version can be
        // current while an older null version sits in the history). An
        // Enabled bucket destroys nothing, so nothing is checked there.
        if let Some(target) = lock_target(b, key, versioning_configured, versioning_enabled) {
            if let Some(code) = check_object_lock_for_overwrite(target, req) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    code,
                    "Access Denied",
                ));
            }
        }

        // Versioned bucket (Enabled or Suspended): create a delete marker.
        // Suspended buckets keep their existing versions and stack a marker
        // whose id is the literal "null", overwriting whatever null version
        // was there, exactly as AWS does.
        if versioning_configured {
            // If the existing object was created before versioning, preserve it
            if !b.object_versions.contains_key(key) {
                if let Some(existing) = b.objects.get(key) {
                    let mut preserved = existing.clone();
                    if preserved.version_id.is_none() {
                        preserved.version_id = Some("null".to_string());
                    }
                    // Rewrite the on-disk sidecar too: the loader routes a
                    // "null" slot whose meta has no version id into the
                    // current-object map, where the delete marker below then
                    // hides it, losing the version after a restart.
                    let preserved_meta = object_meta_snapshot(&preserved);
                    self.store
                        .put_object_meta(bucket, key, Some("null"), &preserved_meta)
                        .map_err(crate::service::persistence_error)?;
                    b.object_versions
                        .entry(key.to_string())
                        .or_default()
                        .push(preserved);
                }
            }
            let dm_id = if versioning_enabled {
                Uuid::new_v4().to_string()
            } else {
                // Suspended: the marker IS the null version, replacing any
                // existing one rather than stacking beside it.
                if let Some(versions) = b.object_versions.get_mut(key) {
                    versions.retain(|o| {
                        !(o.version_id.is_none() || o.version_id.as_deref() == Some("null"))
                    });
                }
                "null".to_string()
            };
            let marker = make_delete_marker(key, &dm_id);
            let marker_meta = object_meta_snapshot(&marker);
            b.object_versions
                .entry(key.to_string())
                .or_default()
                .push(marker.clone());
            b.objects.insert(key.to_string(), marker);
            resp_headers.insert("x-amz-version-id", dm_id.parse().unwrap());
            resp_headers.insert("x-amz-delete-marker", "true".parse().unwrap());
            if !versioning_enabled {
                // Suspended: the marker takes over the "null" slot, so the
                // object that occupied it goes. On an Enabled bucket that
                // slot holds the preserved pre-versioning version, which the
                // marker must NOT destroy (the store keys `None` as "null").
                self.store
                    .delete_object(bucket, key, None)
                    .map_err(crate::service::persistence_error)?;
            }
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
            // A delete marker on a replication source is replicated by the
            // rules that replicate delete markers (only a versioning-enabled
            // bucket can be a source).
            if versioning_enabled {
                crate::service::replication::replicate_delete_marker(
                    &mut accts,
                    account_id,
                    Some(&*self.store),
                    bucket,
                    key,
                    &dm_id,
                    Some(&self.side_effects),
                );
            }
            drop(accts);
            if let Some(ref config) = notification_config {
                deliver_notifications(
                    &self.delivery,
                    config,
                    &crate::service::notifications::ObjectEvent {
                        event_name: "ObjectRemoved:DeleteMarkerCreated",
                        bucket_name: &bucket_name,
                        requester_account: &req.account_id,
                        key: &obj_key,
                        size: 0,
                        etag: "",
                        region: &region,
                        version_id: Some(dm_id.as_str()),
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

        // Deleting a key that was never there is a no-op 204 on AWS and fires
        // no event, so only notify when something was actually removed.
        let existed = b.objects.remove(key).is_some();
        self.store
            .delete_object(bucket, key, None)
            .map_err(crate::service::persistence_error)?;
        drop(accts);

        // Deliver S3 event notifications
        if existed {
            if let Some(ref config) = notification_config {
                deliver_notifications(
                    &self.delivery,
                    config,
                    &crate::service::notifications::ObjectEvent {
                        event_name: "ObjectRemoved:Delete",
                        bucket_name: &bucket_name,
                        requester_account: &req.account_id,
                        key: &obj_key,
                        size: 0,
                        etag: "",
                        region: &region,
                        version_id: None,
                    },
                    Some(&self.state),
                );
            }
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

        let bypass = governance_bypass_requested(req);

        let versioning_enabled = b.versioning.as_deref() == Some("Enabled");
        let versioning_configured = b.versioning.is_some();
        // Delete markers this batch added, replicated once the loop is done.
        let mut new_markers: Vec<(String, String)> = Vec::new();
        let mut deleted_xml = String::new();
        let mut error_xml = String::new();
        // (event name, key, version id) for every object this batch actually
        // removed. Real S3 fires one notification per deleted object, so the
        // batch endpoint must not be a silent hole in the event stream.
        let mut pending_events: Vec<(&'static str, String, Option<String>)> = Vec::new();
        // A persistence failure mid-batch must not swallow the events for the
        // objects already removed: record it, stop, and still deliver what
        // happened before returning the error.
        let mut persist_error: Option<AwsServiceError> = None;
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
                let mut removed_version = false;
                // Report the version id the removed object actually carried
                // (see the single-object path): a Suspended bucket still holds
                // real versions, while a never-versioned object has none.
                let mut removed_vid: Option<String> = None;
                if let Some(versions) = b.object_versions.get_mut(key) {
                    let len_before = versions.len();
                    removed_vid = versions
                        .iter()
                        .find(|o| {
                            o.version_id.as_deref() == Some(vid)
                                || (vid == "null" && o.version_id.is_none())
                        })
                        .and_then(|o| o.version_id.clone());
                    versions.retain(|o| {
                        !(o.version_id.as_deref() == Some(vid)
                            || (vid == "null" && o.version_id.is_none()))
                    });
                    removed_version = versions.len() != len_before;
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
                        removed_version = true;
                        removed_vid = obj.version_id.clone();
                        b.objects.remove(key);
                    }
                }
                if let Err(e) = self.store.delete_object(bucket, key, Some(vid.as_str())) {
                    persist_error = Some(crate::service::persistence_error(e));
                    break;
                }
                if removed_version && removed_vid.is_none() && versioning_configured {
                    removed_vid = Some("null".to_string());
                }
                // Only a version that actually existed produces an event.
                if removed_version {
                    pending_events.push((
                        "ObjectRemoved:Delete",
                        key.to_string(),
                        removed_vid.clone(),
                    ));
                }
                if !quiet {
                    deleted_xml.push_str(&format!(
                        "<Deleted><Key>{}</Key><VersionId>{}</VersionId></Deleted>",
                        xml_escape(key),
                        xml_escape(vid),
                    ));
                }
            } else if versioning_configured {
                // A suspended bucket behaves like an enabled one here except
                // that the marker takes the literal "null" version id and
                // replaces the existing null version (see delete_object).
                //
                // The lock check runs FIRST: a denied entry must leave the
                // bucket untouched, and the preserve step below would
                // otherwise have already written a version-history entry for
                // a request that ends in AccessDenied.
                {
                    let lock_denied =
                        lock_target(b, key, versioning_configured, versioning_enabled)
                            .and_then(|target| check_object_lock_for_overwrite(target, req));
                    if let Some(code) = lock_denied {
                        error_xml.push_str(&format!(
                            "<Error><Key>{}</Key><Code>{}</Code><Message>Access Denied</Message></Error>",
                            xml_escape(key),
                            code,
                        ));
                        continue;
                    }
                }
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
                        // See delete_object: the sidecar needs the "null"
                        // version id or the loader drops this version.
                        let preserved_meta = object_meta_snapshot(&preserved);
                        if let Err(e) =
                            self.store
                                .put_object_meta(bucket, key, Some("null"), &preserved_meta)
                        {
                            persist_error = Some(crate::service::persistence_error(e));
                            break;
                        }
                        b.object_versions
                            .entry(key.to_string())
                            .or_default()
                            .push(preserved);
                    }
                }
                let dm_id = if versioning_enabled {
                    Uuid::new_v4().to_string()
                } else {
                    if let Some(versions) = b.object_versions.get_mut(key.as_str()) {
                        versions.retain(|o| {
                            !(o.version_id.is_none() || o.version_id.as_deref() == Some("null"))
                        });
                    }
                    "null".to_string()
                };
                let marker = make_delete_marker(key, &dm_id);
                let marker_meta = object_meta_snapshot(&marker);
                b.object_versions
                    .entry(key.to_string())
                    .or_default()
                    .push(marker.clone());
                b.objects.insert(key.to_string(), marker);
                // Mirror the single-object path: drop the null slot only when
                // the marker replaces it (suspended), and always persist the
                // marker itself so a restart does not resurrect the object.
                if !versioning_enabled {
                    if let Err(e) = self.store.delete_object(bucket, key, None) {
                        persist_error = Some(crate::service::persistence_error(e));
                        break;
                    }
                }
                if let Err(e) = self.store.put_object(
                    bucket,
                    key,
                    Some(dm_id.as_str()),
                    BodySource::Bytes(Bytes::new()),
                    &marker_meta,
                ) {
                    persist_error = Some(crate::service::persistence_error(e));
                    break;
                }
                if versioning_enabled {
                    new_markers.push((key.to_string(), dm_id.clone()));
                }
                pending_events.push((
                    "ObjectRemoved:DeleteMarkerCreated",
                    key.to_string(),
                    Some(dm_id.clone()),
                ));
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
                let existed = b.objects.remove(key).is_some();
                if let Err(e) = self.store.delete_object(bucket, key, None) {
                    persist_error = Some(crate::service::persistence_error(e));
                    break;
                }
                if existed {
                    pending_events.push(("ObjectRemoved:Delete", key.to_string(), None));
                }
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

        // Deliver after releasing the write lock: delivery re-enters the S3
        // service to read bucket state.
        let notification_config = b.notification_config.clone();
        let region = state.region.clone();
        // Every marker committed before a persistence failure stopped the
        // batch is replicated: those deletes happened.
        for (key, dm_id) in &new_markers {
            crate::service::replication::replicate_delete_marker(
                &mut accts,
                account_id,
                Some(&*self.store),
                bucket,
                key,
                dm_id,
                Some(&self.side_effects),
            );
        }
        drop(accts);
        if let Some(ref config) = notification_config {
            let events: Vec<crate::service::notifications::ObjectEvent<'_>> = pending_events
                .iter()
                .map(
                    |(event_name, key, version_id)| crate::service::notifications::ObjectEvent {
                        event_name,
                        bucket_name: bucket,
                        requester_account: &req.account_id,
                        key,
                        size: 0,
                        etag: "",
                        region: &region,
                        version_id: version_id.as_deref(),
                    },
                )
                .collect();
            // One parse of the config and one state lookup for the whole
            // batch; a 1000-key delete otherwise repeats both per object.
            crate::service::notifications::deliver_notification_batch(
                &self.delivery,
                config,
                &events,
                Some(&self.state),
            );
        }

        if let Some(err) = persist_error {
            return Err(err);
        }

        Ok(s3_xml(StatusCode::OK, body))
    }
}

#[cfg(test)]
mod lock_target_tests {
    use super::lock_target;
    use crate::state::{S3Bucket, S3Object};

    fn bucket(versioning: Option<&str>) -> S3Bucket {
        let mut b = S3Bucket::new("b", "us-east-1", "123456789012");
        b.versioning = versioning.map(|v| v.to_string());
        b
    }

    fn object(version_id: Option<&str>, is_delete_marker: bool) -> S3Object {
        S3Object {
            key: "k".to_string(),
            version_id: version_id.map(|v| v.to_string()),
            is_delete_marker,
            ..Default::default()
        }
    }

    #[test]
    fn never_versioned_bucket_targets_the_current_object() {
        let mut b = bucket(None);
        b.objects.insert("k".to_string(), object(None, false));
        let target = lock_target(&b, "k", false, false).expect("current object is destroyed");
        assert!(target.version_id.is_none());
    }

    #[test]
    fn enabled_bucket_targets_nothing() {
        // An enabled-bucket delete only stacks a marker; no data is destroyed.
        let mut b = bucket(Some("Enabled"));
        b.objects.insert("k".to_string(), object(Some("v1"), false));
        assert!(lock_target(&b, "k", true, true).is_none());
    }

    #[test]
    fn suspended_bucket_targets_the_null_version_in_history() {
        let mut b = bucket(Some("Suspended"));
        b.object_versions
            .insert("k".to_string(), vec![object(Some("null"), false)]);
        // A newer, unlocked version is current; the marker still replaces the
        // null version, so that is what the lock must be checked against.
        b.objects.insert("k".to_string(), object(Some("v2"), false));
        let target = lock_target(&b, "k", true, false).expect("null version is destroyed");
        assert_eq!(target.version_id.as_deref(), Some("null"));
    }

    #[test]
    fn suspended_bucket_looks_past_a_null_delete_marker() {
        // A stale null marker in the history must not hide the live null
        // object that is current -- that object is the one being destroyed.
        let mut b = bucket(Some("Suspended"));
        b.object_versions
            .insert("k".to_string(), vec![object(Some("null"), true)]);
        b.objects.insert("k".to_string(), object(None, false));
        let target = lock_target(&b, "k", true, false).expect("live null object is destroyed");
        assert!(!target.is_delete_marker);
        assert!(target.version_id.is_none());
    }

    #[test]
    fn suspended_bucket_with_only_versioned_objects_targets_nothing() {
        let mut b = bucket(Some("Suspended"));
        b.object_versions
            .insert("k".to_string(), vec![object(Some("v1"), false)]);
        b.objects.insert("k".to_string(), object(Some("v1"), false));
        assert!(lock_target(&b, "k", true, false).is_none());
    }
}
