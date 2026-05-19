use super::*;
use fakecloud_aws::arn::Arn;

/// Actions that mutate persisted state. Only these trigger a snapshot save.
/// Pull-shaped reads (`BatchGetImage`, `GetDownloadUrlForLayer`) are listed
/// because they bump `last_in_use_at`/`in_use_count` on the touched image,
/// which is persisted state.
pub(crate) fn is_mutating(action: &str) -> bool {
    matches!(
        action,
        "CreateRepository"
            | "DeleteRepository"
            | "PutImageTagMutability"
            | "PutImageScanningConfiguration"
            | "SetRepositoryPolicy"
            | "DeleteRepositoryPolicy"
            | "TagResource"
            | "UntagResource"
            | "PutImage"
            | "BatchGetImage"
            | "BatchDeleteImage"
            | "GetDownloadUrlForLayer"
            | "InitiateLayerUpload"
            | "UploadLayerPart"
            | "CompleteLayerUpload"
            | "PutLifecyclePolicy"
            | "DeleteLifecyclePolicy"
            | "StartLifecyclePolicyPreview"
            | "StartImageScan"
            | "PutRegistryPolicy"
            | "DeleteRegistryPolicy"
            | "PutRegistryScanningConfiguration"
            | "PutReplicationConfiguration"
            | "CreatePullThroughCacheRule"
            | "DeletePullThroughCacheRule"
            | "UpdatePullThroughCacheRule"
            | "PutAccountSetting"
            | "CreateRepositoryCreationTemplate"
            | "DeleteRepositoryCreationTemplate"
            | "UpdateRepositoryCreationTemplate"
            | "PutSigningConfiguration"
            | "DeleteSigningConfiguration"
            | "RegisterPullTimeUpdateExclusion"
            | "DeregisterPullTimeUpdateExclusion"
            | "UpdateImageStorageClass"
    )
}

pub(crate) fn req_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body.get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| invalid_parameter(format!("Missing required field: {field}")))
}

pub(crate) fn opt_str<'a>(body: &'a Value, field: &str) -> Option<&'a str> {
    body.get(field).and_then(|v| v.as_str())
}

pub(crate) fn invalid_parameter(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterException",
        message,
    )
}

pub(crate) fn repository_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "RepositoryNotFoundException",
        format!(
            "The repository with name '{name}' does not exist in the registry with id '{registry}'",
            name = name,
            registry = "",
        ),
    )
}

pub(crate) fn repository_already_exists(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "RepositoryAlreadyExistsException",
        format!("The repository with name '{name}' already exists in the registry."),
    )
}

pub(crate) fn repository_policy_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "RepositoryPolicyNotFoundException",
        format!("Repository policy does not exist for the repository with name '{name}'."),
    )
}

/// Validate ECR repository name against AWS pattern:
/// `(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*[a-z0-9]+(?:[._-][a-z0-9]+)*`, length 2–256.
/// Each `/`-separated segment starts and ends with `[a-z0-9]` and uses
/// `[._-]` only between alphanum runs.
pub(crate) fn validate_repository_name(name: &str) -> Result<(), AwsServiceError> {
    let invalid = || {
        invalid_parameter(format!(
            "Invalid parameter at 'repositoryName': '{name}' failed to satisfy constraint: \
             'must satisfy regular expression pattern: (?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*[a-z0-9]+(?:[._-][a-z0-9]+)*'",
        ))
    };
    if name.len() < 2 || name.len() > 256 {
        return Err(invalid());
    }
    // Segments split by `/`. Empty segment (e.g. `foo/`, `foo//bar`,
    // leading/trailing slash) is disallowed.
    for segment in name.split('/') {
        if segment.is_empty() {
            return Err(invalid());
        }
        // Segment := alphanum+ ([._-] alphanum+)*
        let bytes = segment.as_bytes();
        let mut i = 0usize;
        // Leading alphanum run (at least 1 byte).
        if !is_alnum(bytes[0]) {
            return Err(invalid());
        }
        while i < bytes.len() && is_alnum(bytes[i]) {
            i += 1;
        }
        while i < bytes.len() {
            // Separator.
            if !matches!(bytes[i], b'.' | b'_' | b'-') {
                return Err(invalid());
            }
            i += 1;
            // Required alphanum run after each separator.
            if i >= bytes.len() || !is_alnum(bytes[i]) {
                return Err(invalid());
            }
            while i < bytes.len() && is_alnum(bytes[i]) {
                i += 1;
            }
        }
    }
    Ok(())
}

pub(crate) fn is_alnum(b: u8) -> bool {
    b.is_ascii_lowercase() || b.is_ascii_digit()
}

pub(crate) fn parse_tags(body: &Value) -> Vec<(String, String)> {
    body.get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let k = t.get("Key").and_then(|v| v.as_str())?;
                    let v = t.get("Value").and_then(|v| v.as_str()).unwrap_or("");
                    Some((k.to_string(), v.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve the account to scope this request to. ECR inputs use
/// `registryId` to address another account; absent means caller's
/// account. We mirror the cross-service pattern: if `registryId` is
/// present and different, the caller must have cross-account trust —
/// but for CRUD ops we only need to pick the right state entry.
pub(crate) fn target_account_id(request: &AwsRequest, body: &Value) -> String {
    if let Some(id) = body.get("registryId").and_then(|v| v.as_str()) {
        if !id.is_empty() {
            return id.to_string();
        }
    }
    request.account_id.clone()
}

pub(crate) fn repository_to_json(repo: &Repository) -> Value {
    json!({
        "repositoryArn": repo.repository_arn,
        "registryId": repo.registry_id,
        "repositoryName": repo.repository_name,
        "repositoryUri": repo.repository_uri,
        "createdAt": repo.created_at.timestamp(),
        "imageTagMutability": repo.image_tag_mutability,
        "imageScanningConfiguration": {
            "scanOnPush": repo.image_scanning_configuration.scan_on_push,
        },
        "encryptionConfiguration": encryption_config_json(&repo.encryption_configuration),
    })
}

pub(crate) fn encryption_config_json(cfg: &EncryptionConfiguration) -> Value {
    let mut map = Map::new();
    map.insert("encryptionType".into(), json!(cfg.encryption_type));
    if let Some(kms) = &cfg.kms_key {
        map.insert("kmsKey".into(), json!(kms));
    }
    Value::Object(map)
}

/// Decode an ECR resource ARN into `(account_id, repository_name)`.
/// Accepts either a full ARN (`arn:aws:ecr:region:account:repository/name`)
/// or a bare repository name for request bodies that accept both.
pub(crate) fn decode_resource_arn(arn: &str) -> Result<(Option<String>, String), AwsServiceError> {
    if let Some(rest) = arn.strip_prefix("arn:aws:ecr:") {
        let mut parts = rest.splitn(4, ':');
        let _region = parts
            .next()
            .ok_or_else(|| invalid_parameter("Malformed resource ARN"))?;
        let account = parts
            .next()
            .ok_or_else(|| invalid_parameter("Malformed resource ARN"))?;
        let resource = parts
            .next()
            .ok_or_else(|| invalid_parameter("Malformed resource ARN"))?;
        let repo = resource
            .strip_prefix("repository/")
            .ok_or_else(|| invalid_parameter("Resource ARN must reference a repository"))?;
        Ok((Some(account.to_string()), repo.to_string()))
    } else {
        Ok((None, arn.to_string()))
    }
}

pub(crate) fn image_not_found(repo: &str, id: &Value) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ImageNotFoundException",
        format!("The image with imageId {{{id}}} does not exist within the repository with name '{repo}'"),
    )
}

pub(crate) fn repository_policy_denied(repo: &str, action: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::FORBIDDEN,
        "AccessDeniedException",
        format!(
            "User is not authorized to perform: {action} on resource: repository {repo} because no resource-based policy allows the {action} action"
        ),
    )
}

pub(crate) fn layer_not_found(digest: &str, repo: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "LayersNotFoundException",
        format!(
            "The layers with layerDigests '[{digest}]' do not exist in the repository with name '{repo}'"
        ),
    )
}

pub(crate) fn upload_not_found(upload_id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "UploadNotFoundException",
        format!("The upload '{upload_id}' does not exist."),
    )
}

pub(crate) fn image_already_exists(repo: &str, tag: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ImageAlreadyExistsException",
        format!(
            "Image with tag '{tag}' in repository '{repo}' already exists with a different digest and tag mutability is set to IMMUTABLE."
        ),
    )
}

pub(crate) fn invalid_layer(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidLayerException", message)
}

pub(crate) fn sha256_digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

pub(crate) fn image_id_for(image: &Image, tag: Option<&str>) -> Value {
    let mut id = json!({ "imageDigest": image.image_digest });
    if let Some(t) = tag {
        id["imageTag"] = json!(t);
    }
    id
}

pub(crate) fn image_to_details(repo: &Repository, image: &Image, registry_id: &str) -> Value {
    // All tags pointing at this digest.
    let tags: Vec<&str> = repo
        .image_tags
        .iter()
        .filter(|(_, d)| d.as_str() == image.image_digest)
        .map(|(t, _)| t.as_str())
        .collect();
    let mut out = json!({
        "registryId": registry_id,
        "repositoryName": repo.repository_name,
        "imageDigest": image.image_digest,
        "imageTags": tags,
        "imageSizeInBytes": image.image_size_in_bytes,
        "imagePushedAt": image.image_pushed_at.timestamp(),
        "imageManifestMediaType": image.image_manifest_media_type,
    });
    if let Some(a) = &image.artifact_media_type {
        out["artifactMediaType"] = json!(a);
    }
    if let Some(t) = image.last_recorded_pull_time {
        out["lastRecordedPullTime"] = json!(t.timestamp());
    }
    // Storage-class lifecycle fields. `imageStatus` is in the AWS Smithy
    // model; `lastArchivedAt` / `lastActivatedAt` are too. Real ECR omits
    // these until they're set, so do the same.
    out["imageStatus"] = json!(image.image_status);
    if let Some(t) = image.last_archived_at {
        out["lastArchivedAt"] = json!(t.timestamp());
    }
    if let Some(t) = image.last_activated_at {
        out["lastActivatedAt"] = json!(t.timestamp());
    }
    // fakecloud-extension fields (not in AWS Smithy). `lastInUseAt` is
    // bumped by every pull-shaped op (BatchGetImage, GetDownloadUrlForLayer,
    // OCI manifest/blob GET); `inUseCount` is the monotonic counter.
    // Tests can rely on these to assert pull frequency without scraping
    // logs. See P5 ECR polish PR.
    if let Some(t) = image.last_in_use_at {
        out["lastInUseAt"] = json!(t.timestamp());
    }
    out["inUseCount"] = json!(image.in_use_count);
    // If this image is a referrer (its manifest carries a `subject`),
    // surface the subject digest the way AWS does. Parse defensively —
    // not every image is JSON.
    if let Ok(parsed) = serde_json::from_str::<Value>(&image.image_manifest) {
        if let Some(subject_digest) = parsed
            .get("subject")
            .and_then(|s| s.get("digest"))
            .and_then(|d| d.as_str())
        {
            out["subjectManifestDigest"] = json!(subject_digest);
        }
    }
    // Surface scan state on the image itself (real ECR returns these
    // unconditionally; SDK callers that watch scan-on-push completion poll
    // DescribeImages and previously saw nothing).
    if let Some(findings) = repo.scan_findings.get(&image.image_digest) {
        out["imageScanStatus"] = json!({ "status": findings.scan_status });
        let mut summary = serde_json::Map::new();
        if let Some(ts) = findings.scan_completed_at {
            summary.insert("imageScanCompletedAt".into(), json!(ts.timestamp()));
        }
        if let Some(ts) = findings.vulnerability_source_updated_at {
            summary.insert("vulnerabilitySourceUpdatedAt".into(), json!(ts.timestamp()));
        }
        summary.insert(
            "findingSeverityCounts".into(),
            json!(findings.finding_severity_counts),
        );
        out["imageScanFindingsSummary"] = Value::Object(summary);
    } else if repo.image_scanning_configuration.scan_on_push {
        // Scan-on-push configured but the scan hasn't kicked yet.
        out["imageScanStatus"] = json!({ "status": "PENDING" });
    }
    out
}

/// Stamp pull metadata on every image whose digest matches one of `digests`.
/// Used by `BatchGetImage`, `GetDownloadUrlForLayer`, and the OCI manifest /
/// blob GET handlers so DescribeImages reports a fresh `lastInUseAt` and
/// the `inUseCount` matches actual pull traffic.
///
/// When `caller_arn` matches a principal registered via
/// `RegisterPullTimeUpdateExclusion` (passed in `exclusion_arns`), nothing
/// is touched. The exclusion contract has to apply uniformly across the
/// `lastRecordedPullTime`, `lastInUseAt`, and `inUseCount` fields — all
/// three are pull-time bookkeeping.
pub(crate) fn touch_image_pull(
    repo: &mut Repository,
    digests: &[String],
    caller_arn: Option<&str>,
    exclusion_arns: &std::collections::HashSet<String>,
) {
    if digests.is_empty() {
        return;
    }
    if let Some(arn) = caller_arn {
        if exclusion_arns.contains(arn) {
            return;
        }
    }
    let now = chrono::Utc::now();
    for digest in digests {
        if let Some(image) = repo.images.get_mut(digest) {
            image.last_in_use_at = Some(now);
            image.last_recorded_pull_time = Some(now);
            image.in_use_count = image.in_use_count.saturating_add(1);
        }
    }
}

/// Snapshot the registered pull-time-exclusion principal ARNs for the
/// account. Cheap clone (`String`s in a `HashSet`) intended to be taken
/// before grabbing a repo `&mut` so `touch_image_pull` can run without
/// borrowing the surrounding `EcrState`.
pub(crate) fn pull_time_exclusion_set(
    state: &crate::state::EcrState,
) -> std::collections::HashSet<String> {
    state.pull_time_exclusions.keys().cloned().collect()
}

/// Return only the layer blobs referenced by the manifest of `image_digest`.
/// Falls back to all layers if the manifest can't be parsed (e.g. an
/// OCI-spec image that fakecloud stored in an unfamiliar shape) so the
/// scan still runs against something. Layers not stored locally are
/// silently skipped — the scanner only sees what's actually there.
pub(crate) fn layers_for_image(repo: &Repository, image_digest: &str) -> Vec<crate::state::Layer> {
    let Some(image) = repo.images.get(image_digest) else {
        return Vec::new();
    };
    let Ok(manifest): Result<Value, _> = serde_json::from_str(&image.image_manifest) else {
        return repo.layers.values().cloned().collect();
    };
    let mut digests: Vec<String> = Vec::new();
    if let Some(arr) = manifest.get("layers").and_then(|v| v.as_array()) {
        for layer in arr {
            if let Some(d) = layer.get("digest").and_then(|v| v.as_str()) {
                digests.push(d.to_string());
            }
        }
    }
    digests
        .into_iter()
        .filter_map(|d| repo.layers.get(&d).cloned())
        .collect()
}

/// Resolve `imageId` into a stored digest for this repo. Accepts either
/// `{imageDigest}` or `{imageTag}` (or both — digest wins when both set).
pub(crate) fn resolve_image_digest(repo: &Repository, image_id: &Value) -> Option<String> {
    if let Some(d) = image_id.get("imageDigest").and_then(|v| v.as_str()) {
        if repo.images.contains_key(d) {
            return Some(d.to_string());
        }
        return None;
    }
    if let Some(tag) = image_id.get("imageTag").and_then(|v| v.as_str()) {
        return repo.image_tags.get(tag).cloned();
    }
    None
}

pub(crate) fn lifecycle_policy_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "LifecyclePolicyNotFoundException",
        format!("Lifecycle policy does not exist for the repository with name '{name}'."),
    )
}

pub(crate) fn registry_policy_not_found() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "RegistryPolicyNotFoundException",
        "The registry doesn't have an associated registry policy.",
    )
}

/// Apply lifecycle-policy rules to this repo's stored images and
/// return the digests that should be pruned. Covers the four AWS
/// selection dimensions in use today: `tagStatus` (tagged/untagged/any),
/// `tagPrefixList`, `tagPatternList` (wildcard `*`), and `countType`
/// (`imageCountMoreThan` or `sinceImagePushed` with `countUnit=days`).
/// Rules run in ascending `rulePriority` order; later rules can't
/// re-prune images earlier rules already marked.
pub fn evaluate_lifecycle_policy(repo: &crate::state::Repository, policy: &str) -> Vec<String> {
    let Ok(doc) = serde_json::from_str::<Value>(policy) else {
        return Vec::new();
    };
    let Some(rules) = doc.get("rules").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut to_delete: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    // Sort rules by priority ascending (lower priority runs first
    // per AWS semantics).
    let mut sorted: Vec<&Value> = rules.iter().collect();
    sorted.sort_by_key(|r| r.get("rulePriority").and_then(|v| v.as_i64()).unwrap_or(0));
    for rule in sorted {
        let sel = rule.get("selection").cloned().unwrap_or(Value::Null);
        let tag_status = sel
            .get("tagStatus")
            .and_then(|v| v.as_str())
            .unwrap_or("any");
        let count_type = sel.get("countType").and_then(|v| v.as_str()).unwrap_or("");
        let count_number = sel.get("countNumber").and_then(|v| v.as_i64()).unwrap_or(0);
        let count_unit = sel
            .get("countUnit")
            .and_then(|v| v.as_str())
            .unwrap_or("days");
        let tag_prefix_list: Vec<String> = sel
            .get("tagPrefixList")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let tag_pattern_list: Vec<String> = sel
            .get("tagPatternList")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        // Per-image tag lookup: repo stores a tag -> digest map; invert
        // so we can ask "what tags point at this digest".
        let tags_for = |digest: &str| -> Vec<&str> {
            repo.image_tags
                .iter()
                .filter_map(|(t, d)| (d == digest).then_some(t.as_str()))
                .collect()
        };

        // Candidate images, filtered by tagStatus + tagPrefixList +
        // tagPatternList. Per AWS, the tag filters only apply when
        // tagStatus=tagged.
        let mut candidates: Vec<&Image> = repo
            .images
            .values()
            .filter(|img| {
                let tags = tags_for(&img.image_digest);
                let has_tag = !tags.is_empty();
                match tag_status {
                    "tagged" => {
                        if !has_tag {
                            return false;
                        }
                        if !tag_prefix_list.is_empty()
                            && !tags
                                .iter()
                                .any(|t| tag_prefix_list.iter().any(|p| t.starts_with(p.as_str())))
                        {
                            return false;
                        }
                        if !tag_pattern_list.is_empty()
                            && !tags.iter().any(|t| {
                                tag_pattern_list
                                    .iter()
                                    .any(|p| wildcard_match(p.as_str(), t))
                            })
                        {
                            return false;
                        }
                        true
                    }
                    "untagged" => !has_tag,
                    _ => true,
                }
            })
            .filter(|img| !to_delete.contains(&img.image_digest))
            .collect();
        candidates.sort_by_key(|img| img.image_pushed_at);
        match count_type {
            "imageCountMoreThan" => {
                // Keep the newest N, prune the rest.
                let total = candidates.len() as i64;
                if total > count_number {
                    let prune_count = (total - count_number) as usize;
                    for img in candidates.into_iter().take(prune_count) {
                        to_delete.insert(img.image_digest.clone());
                    }
                }
            }
            "sinceImagePushed" => {
                let now = chrono::Utc::now();
                let delta = match count_unit {
                    "days" => chrono::Duration::days(count_number),
                    "hours" => chrono::Duration::hours(count_number),
                    _ => chrono::Duration::days(count_number),
                };
                let threshold = now - delta;
                for img in candidates {
                    if img.image_pushed_at < threshold {
                        to_delete.insert(img.image_digest.clone());
                    }
                }
            }
            _ => {}
        }
    }
    to_delete.into_iter().collect()
}

/// AWS lifecycle `tagPatternList` supports `*` as a shell-style
/// wildcard. No regex metacharacters beyond `*`, no anchoring beyond
/// full-string match.
pub(crate) fn wildcard_match(pattern: &str, text: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return parts[0] == text;
    }
    let mut rest = text;
    // Leading literal must match start if the pattern doesn't start
    // with a `*`.
    if let Some(first) = parts.first() {
        if !first.is_empty() {
            if !rest.starts_with(first) {
                return false;
            }
            rest = &rest[first.len()..];
        }
    }
    // Trailing literal must match end if the pattern doesn't end
    // with a `*`.
    let last_idx = parts.len() - 1;
    for (i, seg) in parts.iter().enumerate().skip(1) {
        if seg.is_empty() {
            continue;
        }
        if i == last_idx {
            if !rest.ends_with(seg) {
                return false;
            }
            rest = &rest[..rest.len() - seg.len()];
        } else if let Some(pos) = rest.find(seg) {
            rest = &rest[pos + seg.len()..];
        } else {
            return false;
        }
    }
    true
}

pub(crate) fn validate_account_setting_name(name: &str) -> Result<(), AwsServiceError> {
    // Smithy `@length(1, 64)` on AccountSettingName.
    if name.is_empty() || name.len() > 64 {
        return Err(invalid_parameter(format!(
            "Invalid parameter at 'name': '{name}' failed to satisfy constraint: \
             Member must have length between 1 and 64"
        )));
    }
    Ok(())
}

pub(crate) fn validate_pullthrough_prefix(prefix: &str) -> Result<(), AwsServiceError> {
    // Smithy @length(2, 30) on PullThroughCacheRuleRepositoryPrefix.
    if prefix.len() < 2 || prefix.len() > 30 {
        return Err(invalid_parameter(format!(
            "Invalid parameter at 'ecrRepositoryPrefix': '{prefix}' failed to satisfy constraint: \
             Member must have length between 2 and 30"
        )));
    }
    Ok(())
}

pub(crate) fn validate_template_prefix(prefix: &str) -> Result<(), AwsServiceError> {
    // Smithy `@length(2, 256)` on CreationTemplatePrefixString, plus
    // AWS's `ROOT` sentinel that's allowed on any-prefix templates.
    if prefix == "ROOT" {
        return Ok(());
    }
    if prefix.len() < 2 || prefix.len() > 256 {
        return Err(invalid_parameter(format!(
            "Invalid parameter at 'prefix': '{prefix}' failed to satisfy constraint: \
             Member must have length between 2 and 256"
        )));
    }
    Ok(())
}

pub(crate) fn validate_max_results(body: &Value) -> Result<(), AwsServiceError> {
    if let Some(n) = body.get("maxResults").and_then(|v| v.as_i64()) {
        if !(1..=1000).contains(&n) {
            return Err(invalid_parameter(format!(
                "Value '{n}' at 'maxResults' failed to satisfy constraint: \
                 Member must have value between 1 and 1000"
            )));
        }
    }
    Ok(())
}

pub(crate) fn pull_through_rule_json(
    registry_id: &str,
    r: &crate::state::PullThroughCacheRule,
) -> Value {
    pull_through_rule_json_with(registry_id, r, false)
}

pub(crate) fn pull_through_rule_json_with_updated(
    registry_id: &str,
    r: &crate::state::PullThroughCacheRule,
) -> Value {
    pull_through_rule_json_with(registry_id, r, true)
}

pub(crate) fn pull_through_rule_json_with(
    registry_id: &str,
    r: &crate::state::PullThroughCacheRule,
    include_updated: bool,
) -> Value {
    let mut out = json!({
        "ecrRepositoryPrefix": r.ecr_repository_prefix,
        "upstreamRegistryUrl": r.upstream_registry_url,
        "createdAt": r.created_at.timestamp(),
        "registryId": registry_id,
    });
    if include_updated {
        out["updatedAt"] = json!(r.updated_at.timestamp());
    }
    if let Some(v) = &r.credential_arn {
        out["credentialArn"] = json!(v);
    }
    if let Some(v) = &r.upstream_registry {
        out["upstreamRegistry"] = json!(v);
    }
    if let Some(v) = &r.custom_role_arn {
        out["customRoleArn"] = json!(v);
    }
    out
}

pub(crate) fn template_to_json(tpl: &crate::state::RepositoryCreationTemplate) -> Value {
    let mut out = json!({
        "prefix": tpl.prefix,
        "imageTagMutability": tpl.image_tag_mutability,
        "appliedFor": tpl.applied_for,
        "resourceTags": tpl.resource_tags,
        "createdAt": tpl.created_at.timestamp(),
        "updatedAt": tpl.updated_at.timestamp(),
    });
    if let Some(desc) = &tpl.description {
        out["description"] = json!(desc);
    }
    if let Some(arn) = &tpl.custom_role_arn {
        out["customRoleArn"] = json!(arn);
    }
    if let Some(p) = &tpl.repository_policy {
        out["repositoryPolicy"] = json!(p);
    }
    if let Some(p) = &tpl.lifecycle_policy {
        out["lifecyclePolicy"] = json!(p);
    }
    if let Some(enc) = &tpl.encryption_configuration {
        let mut e = Map::new();
        e.insert("encryptionType".to_string(), json!(enc.encryption_type));
        if let Some(k) = &enc.kms_key {
            e.insert("kmsKey".to_string(), json!(k));
        }
        out["encryptionConfiguration"] = Value::Object(e);
    }
    out
}

/// Match a registry-level scanning rule's WILDCARD `repository_filters`
/// against a repository name. AWS supports `*` as multi-char wildcard
/// in `WILDCARD` filters; an empty filter list matches everything.
pub(crate) fn registry_filter_matches(filter: &crate::state::RepositoryFilter, repo: &str) -> bool {
    if !filter.filter_type.eq_ignore_ascii_case("WILDCARD") {
        return false;
    }
    wildcard_match(&filter.filter, repo)
}

/// Decide whether a registry scanning configuration would scan-on-push
/// for a given repo name. Returns true when any rule has
/// `scan_frequency=SCAN_ON_PUSH` and at least one of its filters
/// matches (or the filter list is empty, which AWS treats as "all").
pub(crate) fn registry_scan_on_push_matches(
    cfg: &crate::state::RegistryScanningConfiguration,
    repo: &str,
) -> bool {
    cfg.rules.iter().any(|r| {
        r.scan_frequency.eq_ignore_ascii_case("SCAN_ON_PUSH")
            && (r.repository_filters.is_empty()
                || r.repository_filters
                    .iter()
                    .any(|f| registry_filter_matches(f, repo)))
    })
}

/// Replication rule filter check. Empty filter list matches every
/// repository — that's how AWS treats a rule with no filters.
pub(crate) fn repository_filters_match(
    filters: &[crate::state::RepositoryFilter],
    repo_name: &str,
) -> bool {
    if filters.is_empty() {
        return true;
    }
    filters
        .iter()
        .any(|f| registry_filter_matches(f, repo_name))
}

/// Enforce the cross-account repo-policy gate at a handler. Returns
/// Ok(()) when the caller belongs to the same account as the repo
/// (in which case ECR falls back to the caller's IAM perms — out of
/// scope for this batch), or when the repository policy explicitly
/// Allows the requested action for the caller. Otherwise returns the
/// canonical AWS error:
///
/// * `RepositoryPolicyNotFoundException` — the repository has no
///   policy at all (real AWS distinguishes "no policy" from "policy
///   denies" so callers can tell whether they need to ask the
///   repository owner to create a policy or fix an existing one).
/// * `AccessDeniedException` — a policy exists but doesn't grant the
///   requested action to the caller (either implicit or explicit
///   deny).
pub(crate) fn check_repo_policy(
    repo_owner_account: &str,
    caller_account: &str,
    repo_arn: &str,
    repo_name: &str,
    policy_doc: Option<&str>,
    action: &str,
) -> Result<(), AwsServiceError> {
    if caller_account == repo_owner_account {
        return Ok(());
    }
    match repository_policy_decision(policy_doc, caller_account, repo_arn, action) {
        RepoPolicyDecision::Allow => Ok(()),
        RepoPolicyDecision::NoPolicy => Err(repository_policy_not_found(repo_name)),
        RepoPolicyDecision::Deny => Err(repository_policy_denied(repo_name, action)),
    }
}

/// Outcome of evaluating a cross-account ECR request against a
/// repository's resource policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepoPolicyDecision {
    /// Policy exists and explicitly allows the requested action for
    /// the caller. Maps to `Decision::Allow` from the IAM evaluator.
    Allow,
    /// No repository policy exists at all (or it's an empty string).
    /// AWS surfaces this as `RepositoryPolicyNotFoundException` so
    /// callers can distinguish "owner hasn't shared this repo" from
    /// "owner shared it but not with you".
    NoPolicy,
    /// A policy exists but its evaluation produced `ImplicitDeny` or
    /// `ExplicitDeny`. AWS surfaces this as `AccessDeniedException`.
    Deny,
}

/// Cross-account ECR repository policy decision. When the caller's
/// account differs from the repository's owning account, the repo
/// must have a resource policy that explicitly Allows the requested
/// action — empty / missing policies map to [`RepoPolicyDecision::NoPolicy`]
/// so the handler can return the more specific AWS error code.
pub(crate) fn repository_policy_decision(
    policy_doc: Option<&str>,
    caller_account: &str,
    repo_arn: &str,
    action: &str,
) -> RepoPolicyDecision {
    let doc = match policy_doc {
        Some(d) if !d.is_empty() => d,
        _ => return RepoPolicyDecision::NoPolicy,
    };
    use fakecloud_core::auth::{Principal, PrincipalType};
    use fakecloud_iam::evaluator::{evaluate, Decision, EvalRequest, PolicyDocument};
    let parsed = PolicyDocument::parse(doc);
    let principal_arn = Arn::global("iam", caller_account, "root").to_string();
    let principal = Principal {
        arn: principal_arn.clone(),
        user_id: principal_arn,
        account_id: caller_account.to_string(),
        principal_type: PrincipalType::User,
        source_identity: None,
        tags: None,
    };
    let req = EvalRequest {
        principal: &principal,
        action: action.to_string(),
        resource: repo_arn.to_string(),
        context: Default::default(),
    };
    match evaluate(&[parsed], &req) {
        Decision::Allow => RepoPolicyDecision::Allow,
        Decision::ImplicitDeny | Decision::ExplicitDeny => RepoPolicyDecision::Deny,
    }
}

/// Boolean shim retained for the existing unit tests (and any callers
/// that only need a yes/no answer). Returns `true` only when the
/// repository policy explicitly allows the action.
#[cfg(test)]
pub(crate) fn repository_policy_allows(
    policy_doc: Option<&str>,
    caller_account: &str,
    repo_arn: &str,
    action: &str,
) -> bool {
    matches!(
        repository_policy_decision(policy_doc, caller_account, repo_arn, action),
        RepoPolicyDecision::Allow
    )
}
