//! `EcrService` `images` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn put_image_tag_mutability(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let mutability = req_str(&body, "imageTagMutability")?.to_string();
        if mutability != "MUTABLE" && mutability != "IMMUTABLE" {
            return Err(invalid_parameter(format!(
                "Invalid value for imageTagMutability: {mutability}"
            )));
        }
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        repo.image_tag_mutability = mutability.clone();
        let registry_id = repo.registry_id.clone();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "imageTagMutability": mutability,
        })))
    }

    pub(super) fn put_image(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let manifest = req_str(&body, "imageManifest")?.to_string();
        let manifest_media_type = opt_str(&body, "imageManifestMediaType")
            .unwrap_or("application/vnd.docker.distribution.manifest.v2+json")
            .to_string();
        let supplied_tag = opt_str(&body, "imageTag").map(|s| s.to_string());
        let supplied_digest = opt_str(&body, "imageDigest").map(|s| s.to_string());
        let account = target_account_id(request, &body);

        let computed_digest = sha256_digest(manifest.as_bytes());
        if let Some(ref supplied) = supplied_digest {
            if supplied != &computed_digest {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ImageDigestDoesNotMatchException",
                    format!(
                        "The imageDigest '{supplied}' does not match the digest of the uploaded manifest ('{computed_digest}')."
                    ),
                ));
            }
        }
        let digest = supplied_digest.unwrap_or_else(|| computed_digest.clone());

        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let registry_match =
            registry_scan_on_push_matches(&state.registry_scanning_configuration, &name);
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;

        check_repo_policy(
            &account,
            &request.account_id,
            &repo.repository_arn,
            &name,
            repo.policy.as_deref(),
            "ecr:PutImage",
        )?;

        // Immutable tag guard: if a tag is supplied, it already maps to
        // a different digest, and the repo is IMMUTABLE, reject.
        if let Some(ref tag) = supplied_tag {
            if let Some(existing) = repo.image_tags.get(tag) {
                if existing != &digest && repo.image_tag_mutability == "IMMUTABLE" {
                    return Err(image_already_exists(&name, tag));
                }
            }
        }

        let image_entry = repo.images.entry(digest.clone()).or_insert_with(|| Image {
            image_digest: digest.clone(),
            image_manifest: manifest.clone(),
            image_manifest_media_type: manifest_media_type.clone(),
            artifact_media_type: None,
            image_size_in_bytes: manifest.len() as u64,
            image_pushed_at: Utc::now(),
            last_recorded_pull_time: None,
            image_status: "ACTIVE".to_string(),
            last_archived_at: None,
            last_activated_at: None,
            last_in_use_at: None,
            in_use_count: 0,
        });
        // If the caller re-pushes an existing digest with a new manifest
        // payload (shouldn't happen under sha256 addressing but tolerate
        // it), keep the latest manifest.
        image_entry.image_manifest = manifest;
        image_entry.image_manifest_media_type = manifest_media_type.clone();

        if let Some(tag) = supplied_tag.clone() {
            repo.image_tags.insert(tag, digest.clone());
        }

        let snapshot = repo.images.get(&digest).cloned().unwrap();
        let scan_on_push = repo.image_scanning_configuration.scan_on_push;
        // Registry-level fallback: when the per-repo flag is off but a
        // registry scanning rule with frequency=SCAN_ON_PUSH matches the
        // repo's name via its WILDCARD filter, real ECR still scans.
        let should_scan = scan_on_push || registry_match;
        let tag_ref = supplied_tag.as_deref();
        let response = AwsResponse::ok_json(json!({
            "image": {
                "registryId": repo.registry_id,
                "repositoryName": name,
                "imageId": image_id_for(&snapshot, tag_ref),
                "imageManifest": snapshot.image_manifest,
                "imageManifestMediaType": snapshot.image_manifest_media_type,
            }
        }));
        // Drop the lock before kicking the async scan; trigger_scan
        // re-acquires it to mark IN_PROGRESS and spawn the scanner.
        drop(accounts);
        if should_scan {
            self.trigger_scan(&account, &name, &digest);
        }
        self.replicate_image(&account, &name, &digest);
        Ok(response)
    }

    /// Copy a freshly-pushed image to every destination configured by
    /// the registry's replication rules, recording the per-destination
    /// status so DescribeImageReplicationStatus returns real data. The
    /// status flips IN_PROGRESS -> COMPLETE around each copy so a future
    /// async path can surface real progress without reshaping the API.
    /// Cross-account targets get a separate `EcrState`; cross-region
    /// same-account targets share the source state (fakecloud collapses
    /// regions per process) but still get their own status entry.
    pub(crate) fn replicate_image(&self, source_account: &str, repo_name: &str, digest: &str) {
        use crate::state::{ImageReplicationStatus, Repository};

        let (rules, image, layer_blobs, source_registry_id, source_region, source_uri) = {
            let accounts = self.state.read();
            let Some(state) = accounts.get(source_account) else {
                return;
            };
            let Some(cfg) = state.replication_configuration.as_ref() else {
                return;
            };
            let Some(repo) = state.repositories.get(repo_name) else {
                return;
            };
            let Some(image) = repo.images.get(digest).cloned() else {
                return;
            };
            let layers: Vec<crate::state::Layer> = layers_for_image(repo, digest);
            (
                cfg.rules.clone(),
                image,
                layers,
                repo.registry_id.clone(),
                state.region.clone(),
                repo.repository_uri.clone(),
            )
        };

        // Source URI is `<host>/<repo_name>` — strip the trailing repo
        // name to recover the host so the destination repo's URI keeps
        // pointing at the same fakecloud endpoint.
        let endpoint = source_uri
            .strip_suffix(&format!("/{repo_name}"))
            .unwrap_or(&source_uri)
            .to_string();

        let matching: Vec<_> = rules
            .into_iter()
            .filter(|rule| repository_filters_match(&rule.repository_filters, repo_name))
            .flat_map(|rule| rule.destinations.into_iter())
            .collect();
        if matching.is_empty() {
            return;
        }

        // Stage IN_PROGRESS entries before any copying, then flip each
        // to COMPLETE once its copy lands. This mirrors the AWS lifecycle
        // and lets DescribeImageReplicationStatus return meaningful state
        // even mid-flight.
        let mut statuses: Vec<ImageReplicationStatus> = matching
            .iter()
            .filter(|dest| {
                // Same registry + same region is a no-op target — the
                // image is already where the rule points.
                !(dest.registry_id == source_registry_id && dest.region == source_region)
            })
            .map(|dest| ImageReplicationStatus {
                region: dest.region.clone(),
                registry_id: dest.registry_id.clone(),
                status: "IN_PROGRESS".to_string(),
                failure_code: None,
                failure_reason: None,
            })
            .collect();

        if statuses.is_empty() {
            // Still wipe any stale entries for this digest so callers see
            // an honest empty list when no rule actually targets a
            // distinct destination.
            let mut accounts = self.state.write();
            let source_state = accounts.get_or_create(source_account);
            if let Some(repo) = source_state.repositories.get_mut(repo_name) {
                repo.replication_statuses.remove(digest);
            }
            return;
        }

        let mut accounts = self.state.write();
        for (idx, dest) in matching.iter().enumerate() {
            if dest.registry_id == source_registry_id && dest.region == source_region {
                continue;
            }
            // Map the source-side `matching` index to its corresponding
            // status slot (which has the same filter applied).
            let status_idx = matching
                .iter()
                .take(idx + 1)
                .filter(|d| !(d.registry_id == source_registry_id && d.region == source_region))
                .count()
                - 1;

            // Cross-account: hop into the destination registry's state.
            // Cross-region same-account: stay in source state — the same
            // fakecloud process serves the "destination" region too, so a
            // separate copy would duplicate without benefit. The status
            // entry still records the rule fired.
            if dest.registry_id != source_registry_id {
                let target_state = accounts.get_or_create(&dest.registry_id);
                // Provision the mirror repo on demand. Real ECR only
                // replicates into existing repos; create-on-write keeps
                // the fakecloud flow honest by mirroring the source repo
                // metadata when the target hasn't been pre-provisioned.
                if !target_state.repositories.contains_key(repo_name) {
                    let arn = format!(
                        "arn:aws:ecr:{}:{}:repository/{}",
                        dest.region, dest.registry_id, repo_name
                    );
                    let repo = Repository::new(repo_name, arn, &dest.registry_id, &endpoint);
                    target_state
                        .repositories
                        .insert(repo_name.to_string(), repo);
                }
                let target_repo = target_state.repositories.get_mut(repo_name).unwrap();
                target_repo
                    .images
                    .entry(digest.to_string())
                    .or_insert_with(|| image.clone());
                for layer in &layer_blobs {
                    target_repo
                        .layers
                        .entry(layer.digest.clone())
                        .or_insert_with(|| layer.clone());
                }
            }
            statuses[status_idx].status = "COMPLETE".to_string();
        }

        // Record per-destination replication status on the source repo
        // so DescribeImageReplicationStatus surfaces real entries.
        let source_state = accounts.get_or_create(source_account);
        if let Some(repo) = source_state.repositories.get_mut(repo_name) {
            repo.replication_statuses
                .insert(digest.to_string(), statuses);
        }
    }

    pub(super) fn batch_get_image(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let ids = body
            .get("imageIds")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let account = target_account_id(request, &body);
        // Take a write lock so pull-time bookkeeping (`lastInUseAt`,
        // `inUseCount`, `lastRecordedPullTime`) can update in place. Real
        // ECR records pull time at least every 24 h on the data plane;
        // fakecloud bumps unconditionally so tests can observe each call.
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        // Snapshot the registered exclusion ARNs before grabbing the repo
        // mut so `touch_image_pull` can honour the exclusion contract
        // without holding two `&mut`s on `EcrState`.
        let exclusions = pull_time_exclusion_set(state);
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;

        check_repo_policy(
            &account,
            &request.account_id,
            &repo.repository_arn,
            &name,
            repo.policy.as_deref(),
            "ecr:BatchGetImage",
        )?;

        let mut images: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        let mut hit_digests: Vec<String> = Vec::new();
        for id in &ids {
            match resolve_image_digest(repo, id) {
                Some(digest) => {
                    let img = repo.images.get(&digest).unwrap();
                    let tag = id.get("imageTag").and_then(|v| v.as_str());
                    images.push(json!({
                        "registryId": repo.registry_id,
                        "repositoryName": name,
                        "imageId": image_id_for(img, tag),
                        "imageManifest": img.image_manifest,
                        "imageManifestMediaType": img.image_manifest_media_type,
                    }));
                    hit_digests.push(digest);
                }
                None => failures.push(json!({
                    "imageId": id,
                    "failureCode": "ImageNotFound",
                    "failureReason": "Requested image not found",
                })),
            }
        }
        let caller_arn = request.principal.as_ref().map(|p| p.arn.as_str());
        touch_image_pull(repo, &hit_digests, caller_arn, &exclusions);
        Ok(AwsResponse::ok_json(json!({
            "images": images,
            "failures": failures,
        })))
    }

    pub(super) fn batch_delete_image(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let ids = body
            .get("imageIds")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;

        check_repo_policy(
            &account,
            &request.account_id,
            &repo.repository_arn,
            &name,
            repo.policy.as_deref(),
            "ecr:BatchDeleteImage",
        )?;

        let mut deleted: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        for id in &ids {
            if let Some(tag) = id.get("imageTag").and_then(|v| v.as_str()) {
                // Delete by tag: remove only the tag, image stays if
                // other tags still reference the digest.
                if let Some(digest) = repo.image_tags.remove(tag) {
                    deleted.push(json!({ "imageDigest": digest, "imageTag": tag }));
                    let still_tagged = repo.image_tags.values().any(|d| *d == digest);
                    if !still_tagged {
                        repo.images.remove(&digest);
                    }
                    continue;
                }
                failures.push(json!({
                    "imageId": id,
                    "failureCode": "ImageNotFound",
                    "failureReason": "Requested image not found",
                }));
            } else if let Some(digest) = id.get("imageDigest").and_then(|v| v.as_str()) {
                if repo.images.remove(digest).is_some() {
                    repo.image_tags.retain(|_, d| d != digest);
                    deleted.push(json!({ "imageDigest": digest }));
                    continue;
                }
                failures.push(json!({
                    "imageId": id,
                    "failureCode": "ImageNotFound",
                    "failureReason": "Requested image not found",
                }));
            } else {
                failures.push(json!({
                    "imageId": id,
                    "failureCode": "InvalidImageTag",
                    "failureReason": "Either imageDigest or imageTag must be supplied",
                }));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "imageIds": deleted,
            "failures": failures,
        })))
    }

    pub(super) fn describe_images(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        const DEFAULT_PAGE_SIZE: usize = 100;
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let ids = body
            .get("imageIds")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let max_results = match body.get("maxResults").and_then(|v| v.as_i64()) {
            Some(n) => {
                if !(1..=1000).contains(&n) {
                    return Err(invalid_parameter(format!(
                        "Value '{n}' at 'maxResults' failed to satisfy constraint: \
                         Member must have value between 1 and 1000",
                    )));
                }
                n as usize
            }
            None => DEFAULT_PAGE_SIZE,
        };
        let offset = match body.get("nextToken").and_then(|v| v.as_str()) {
            Some(raw) => raw.parse::<usize>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "The specified parameter is invalid: nextToken",
                )
            })?,
            None => 0,
        };
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;

        let mut details: Vec<Value> = Vec::new();
        let mut next_token: Option<String> = None;
        if ids.is_empty() {
            let all: Vec<&Image> = repo.images.values().collect();
            let start = offset.min(all.len());
            let end = (start + max_results).min(all.len());
            for img in &all[start..end] {
                details.push(image_to_details(repo, img, &repo.registry_id));
            }
            if end < all.len() {
                next_token = Some(end.to_string());
            }
        } else {
            for id in &ids {
                let digest =
                    resolve_image_digest(repo, id).ok_or_else(|| image_not_found(&name, id))?;
                let img = repo.images.get(&digest).unwrap();
                details.push(image_to_details(repo, img, &repo.registry_id));
            }
        }
        let mut response = json!({ "imageDetails": details });
        if let Some(token) = next_token {
            response["nextToken"] = json!(token);
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn list_images(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        const DEFAULT_PAGE_SIZE: usize = 100;
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let filter_tag_status = body
            .get("filter")
            .and_then(|v| v.get("tagStatus"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let max_results = match body.get("maxResults").and_then(|v| v.as_i64()) {
            Some(n) => {
                if !(1..=1000).contains(&n) {
                    return Err(invalid_parameter(format!(
                        "Value '{n}' at 'maxResults' failed to satisfy constraint: \
                         Member must have value between 1 and 1000",
                    )));
                }
                n as usize
            }
            None => DEFAULT_PAGE_SIZE,
        };
        let offset = match body.get("nextToken").and_then(|v| v.as_str()) {
            Some(raw) => raw.parse::<usize>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "The specified parameter is invalid: nextToken",
                )
            })?,
            None => 0,
        };
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;

        // Enumerate once per (digest, tag-or-untagged) combination.
        let mut all: Vec<(String, Option<String>)> = Vec::new();
        for (tag, digest) in &repo.image_tags {
            all.push((digest.clone(), Some(tag.clone())));
        }
        let tagged_digests: std::collections::HashSet<&String> = repo.image_tags.values().collect();
        for digest in repo.images.keys() {
            if !tagged_digests.contains(digest) {
                all.push((digest.clone(), None));
            }
        }
        // Apply filter.
        all.retain(|(_, tag)| match filter_tag_status.as_deref() {
            Some("TAGGED") => tag.is_some(),
            Some("UNTAGGED") => tag.is_none(),
            _ => true,
        });
        all.sort();

        let start = offset.min(all.len());
        let end = (start + max_results).min(all.len());
        let ids: Vec<Value> = all[start..end]
            .iter()
            .map(|(d, t)| {
                let mut v = json!({ "imageDigest": d });
                if let Some(tag) = t {
                    v["imageTag"] = json!(tag);
                }
                v
            })
            .collect();
        let mut response = json!({ "imageIds": ids });
        if end < all.len() {
            response["nextToken"] = json!(end.to_string());
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn start_image_scan(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use crate::state::ImageScanFindings;
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let image_id = body
            .get("imageId")
            .cloned()
            .ok_or_else(|| invalid_parameter("Missing imageId"))?;
        let account = target_account_id(request, &body);
        let (digest, layers, registry_id) = {
            let mut accounts = self.state.write();
            let state = accounts
                .get_mut(&account)
                .ok_or_else(|| repository_not_found(&name))?;
            let repo = state
                .repositories
                .get_mut(&name)
                .ok_or_else(|| repository_not_found(&name))?;
            let digest = resolve_image_digest(repo, &image_id)
                .ok_or_else(|| image_not_found(&name, &image_id))?;
            // Mark scan IN_PROGRESS; real findings written by the
            // background scanner task. Caller polls
            // DescribeImageScanFindings to observe completion.
            repo.scan_findings.insert(
                digest.clone(),
                ImageScanFindings {
                    image_digest: digest.clone(),
                    scan_status: "IN_PROGRESS".to_string(),
                    scan_completed_at: None,
                    vulnerability_source_updated_at: None,
                    finding_severity_counts: BTreeMap::new(),
                    findings: Vec::new(),
                },
            );
            // Scope the scan to layers actually referenced by *this*
            // image's manifest. Other images sitting in the same repo
            // (different tag, different digest) must not contaminate
            // the findings — Trivy would otherwise report CVEs from
            // unrelated images.
            let layers = layers_for_image(repo, &digest);
            (digest, layers, repo.registry_id.clone())
        };

        let shared = self.state.clone();
        let store = self.snapshot_store.clone();
        let snap_lock = self.snapshot_lock.clone();
        let account_for_task = account.clone();
        let name_for_task = name.clone();
        let digest_for_task = digest.clone();
        tokio::spawn(async move {
            let result = crate::scanner::scan_layers(&digest_for_task, &layers).await;
            {
                let mut accounts = shared.write();
                let Some(state) = accounts.get_mut(&account_for_task) else {
                    return;
                };
                let Some(repo) = state.repositories.get_mut(&name_for_task) else {
                    return;
                };
                let findings = result.unwrap_or_else(|| ImageScanFindings {
                    image_digest: digest_for_task.clone(),
                    scan_status: "COMPLETE".to_string(),
                    scan_completed_at: Some(Utc::now()),
                    vulnerability_source_updated_at: Some(Utc::now()),
                    finding_severity_counts: BTreeMap::new(),
                    findings: Vec::new(),
                });
                repo.scan_findings.insert(digest_for_task.clone(), findings);
            }
            // Persist the COMPLETE state — without this, restarts
            // restore the snapshot taken at IN_PROGRESS time and the
            // scan appears stuck forever.
            EcrService::save_snapshot_with(shared, store, snap_lock).await;
        });

        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "imageId": image_id,
            "imageScanStatus": {"status": "IN_PROGRESS"},
        })))
    }

    pub(super) fn describe_image_replication_status(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let image_id = body
            .get("imageId")
            .cloned()
            .ok_or_else(|| invalid_parameter("Missing imageId"))?;
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        let Some(digest) = resolve_image_digest(repo, &image_id) else {
            return Err(image_not_found(&name, &image_id));
        };
        let statuses: Vec<Value> = repo
            .replication_statuses
            .get(&digest)
            .map(|entries| {
                entries
                    .iter()
                    .map(|s| {
                        let mut obj = serde_json::Map::new();
                        obj.insert("region".into(), Value::String(s.region.clone()));
                        obj.insert("registryId".into(), Value::String(s.registry_id.clone()));
                        obj.insert("status".into(), Value::String(s.status.clone()));
                        if let Some(ref code) = s.failure_code {
                            obj.insert("failureCode".into(), Value::String(code.clone()));
                        }
                        if let Some(ref reason) = s.failure_reason {
                            obj.insert("failureReason".into(), Value::String(reason.clone()));
                        }
                        Value::Object(obj)
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "repositoryName": name,
            "imageId": image_id,
            "replicationStatuses": statuses,
        })))
    }

    pub(super) fn list_image_referrers(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let subject = body
            .get("subjectId")
            .cloned()
            .ok_or_else(|| invalid_parameter("Missing subjectId"))?;
        let digest = subject
            .get("imageDigest")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("subjectId.imageDigest is required"))?
            .to_string();
        // Optional filter: artifactTypes + artifactStatus. Defaults match
        // AWS: `artifactStatus = ACTIVE` only when no filter is supplied.
        let filter = body.get("filter").cloned().unwrap_or(Value::Null);
        let artifact_type_filter: Option<Vec<String>> = filter
            .get("artifactTypes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            });
        let artifact_status_filter: String = filter
            .get("artifactStatus")
            .and_then(|v| v.as_str())
            .unwrap_or("ACTIVE")
            .to_string();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        if !repo.images.contains_key(&digest) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ImageNotFoundException",
                format!("Subject image {digest} not found in repository '{name}'"),
            ));
        }
        // Walk every image; include those whose manifest has a
        // `subject.digest` matching the requested subject. The subject
        // is the OCI Distribution v1.1 referrer relation.
        let mut referrers: Vec<Value> = Vec::new();
        for image in repo.images.values() {
            let parsed: Value = match serde_json::from_str(&image.image_manifest) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let subject_digest = parsed
                .get("subject")
                .and_then(|s| s.get("digest"))
                .and_then(|d| d.as_str());
            if subject_digest != Some(digest.as_str()) {
                continue;
            }
            // OCI artifact type lives at `artifactType` (image manifest)
            // or `config.mediaType` (image-config-as-artifact).
            let artifact_type = parsed
                .get("artifactType")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    parsed
                        .get("config")
                        .and_then(|c| c.get("mediaType"))
                        .and_then(|v| v.as_str())
                })
                .map(str::to_string);
            if let Some(ref allowed) = artifact_type_filter {
                if !allowed.iter().any(|t| Some(t) == artifact_type.as_ref()) {
                    continue;
                }
            }
            // Status filter: `ANY` matches everything; otherwise the
            // referrer's `image_status` must equal the filter value.
            if artifact_status_filter != "ANY" && image.image_status != artifact_status_filter {
                continue;
            }
            // Annotations live on the manifest under `annotations`.
            let annotations = parsed
                .get("annotations")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let mut referrer = json!({
                "digest": image.image_digest,
                "mediaType": image.image_manifest_media_type,
                "size": image.image_size_in_bytes,
                "annotations": annotations,
                "artifactStatus": image.image_status,
            });
            if let Some(t) = artifact_type {
                referrer["artifactType"] = json!(t);
            }
            referrers.push(referrer);
        }
        Ok(AwsResponse::ok_json(json!({
            "referrers": referrers,
        })))
    }

    pub(super) fn update_image_storage_class(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let image_id = body
            .get("imageId")
            .cloned()
            .ok_or_else(|| invalid_parameter("Missing imageId"))?;
        let target_class = req_str(&body, "targetStorageClass")?.to_string();
        // Smithy `TargetStorageClass` is `STANDARD | ARCHIVE`. Anything
        // else is rejected the way AWS would reject an enum mismatch.
        if target_class != "STANDARD" && target_class != "ARCHIVE" {
            return Err(invalid_parameter(format!(
                "Invalid targetStorageClass '{target_class}'. Must be STANDARD or ARCHIVE."
            )));
        }
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        let digest = resolve_image_digest(repo, &image_id)
            .ok_or_else(|| image_not_found(&name, &image_id))?;
        let registry_id = repo.registry_id.clone();
        let now = Utc::now();
        let image = repo.images.get_mut(&digest).expect("digest resolves");
        let new_status = match target_class.as_str() {
            "ARCHIVE" => {
                image.last_archived_at = Some(now);
                "ARCHIVED"
            }
            // STANDARD = restore from archive.
            _ => {
                image.last_activated_at = Some(now);
                "ACTIVE"
            }
        };
        image.image_status = new_status.to_string();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "imageId": image_id,
            "imageStatus": new_status,
        })))
    }
}
