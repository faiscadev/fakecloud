//! `EcrService` replication backfills: replicate a registry's existing images
//! that match its replication rules to one configured destination.
//!
//! A backfill starts `PREPARING`. After [`PREPARE_DELAY`] it is run: every
//! image and tag of every repository a rule for the destination matches is
//! copied there through the same path push-time replication uses
//! ([`copy_image_to_destination`]), and the backfill ends `COMPLETE` (or
//! `FAILED` when any replication failed) with per-image results. A stopped
//! backfill (StopReplicationBackfill while still preparing) ends `STOPPED`
//! with no progress, as on AWS. The run is driven by a timer task and,
//! idempotently, by every backfill read, so a backfill restored from a
//! snapshot still completes.

use std::time::Duration;

use fakecloud_core::multi_account::MultiAccountState;

use super::*;
use crate::state::{
    EcrState, ReplicationBackfill, ReplicationBackfillProgress, ReplicationBackfillResult,
    ReplicationDestination,
};

/// How long a backfill stays `PREPARING` before it runs.
pub(crate) const PREPARE_DELAY: Duration = Duration::from_secs(2);

/// At most this many backfills may be in progress in a registry at once.
const MAX_IN_PROGRESS: usize = 10;

/// Terminal backfills are listed for 90 days after they end.
const RETENTION_DAYS: i64 = 90;

/// The failure code of a replication whose source image was deleted before
/// the backfill reached it.
const IMAGE_NOT_FOUND: &str = "IMAGE_NOT_FOUND";

fn in_progress(status: &str) -> bool {
    matches!(status, "PREPARING" | "IN_PROGRESS" | "STOPPING")
}

fn ts(dt: chrono::DateTime<Utc>) -> Value {
    json!(dt.timestamp_millis() as f64 / 1000.0)
}

fn destination_json(d: &ReplicationDestination) -> Value {
    json!({ "region": d.region, "registryId": d.registry_id })
}

fn progress_json(p: &ReplicationBackfillProgress) -> Value {
    json!({
        "totalReplicationCount": p.total_replication_count,
        "completedReplicationCount": p.completed_replication_count,
        "failedReplicationCount": p.failed_replication_count,
        "failureCodes": p.failure_codes,
    })
}

fn summary_json(b: &ReplicationBackfill) -> Value {
    let mut v = json!({
        "replicationBackfillId": b.id,
        "destination": destination_json(&b.destination),
        "status": b.status,
        "startedAt": ts(b.started_at),
    });
    if let Some(code) = &b.failure_code {
        v["failureCode"] = json!(code);
    }
    if let Some(p) = &b.progress {
        v["progress"] = progress_json(p);
    }
    if let Some(end) = b.ended_at {
        v["endedAt"] = ts(end);
    }
    v
}

fn result_json(r: &ReplicationBackfillResult) -> Value {
    let mut image_id = json!({ "imageDigest": r.image_digest });
    if let Some(tag) = &r.image_tag {
        image_id["imageTag"] = json!(tag);
    }
    let mut v = json!({
        "repositoryName": r.repository_name,
        "imageId": image_id,
        "status": r.status,
    });
    if let Some(code) = &r.failure_code {
        v["failureCode"] = json!(code);
    }
    v
}

fn backfill_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ReplicationBackfillNotFoundException",
        format!("The replication backfill with ID '{id}' could not be found in the registry."),
    )
}

fn validation(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", message)
}

/// `replicationBackfillId`: required, and a UUID (the model's pattern).
fn backfill_id(body: &Value) -> Result<String, AwsServiceError> {
    let id = req_str(body, "replicationBackfillId")?;
    if Uuid::parse_str(id).is_err() || id.len() != 36 {
        return Err(validation(format!(
            "1 validation error detected: Value '{id}' at 'replicationBackfillId' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[0-9a-fA-F]{{8}}-[0-9a-fA-F]{{4}}-[0-9a-fA-F]{{4}}-[0-9a-fA-F]{{4}}-[0-9a-fA-F]{{12}}$"
        )));
    }
    Ok(id.to_string())
}

/// Numeric-offset pagination of `items` by `maxResults` / `nextToken`.
fn paginate(
    mut items: Vec<Value>,
    body: &Value,
    max_allowed: i64,
) -> Result<(Vec<Value>, Option<String>), AwsServiceError> {
    let max_results = match body.get("maxResults").and_then(Value::as_i64) {
        Some(n) if !(1..=max_allowed).contains(&n) => {
            return Err(invalid_parameter(format!(
                "Value '{n}' at 'maxResults' failed to satisfy constraint: Member must have value between 1 and {max_allowed}"
            )))
        }
        Some(n) => Some(n as usize),
        None => None,
    };
    let offset = match body.get("nextToken").and_then(Value::as_str) {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| invalid_parameter("The specified parameter is invalid: nextToken"))?,
        None => 0,
    };
    let start = offset.min(items.len());
    items.drain(..start);
    let next = match max_results {
        Some(n) if items.len() > n => {
            items.truncate(n);
            Some((start + n).to_string())
        }
        _ => None,
    };
    Ok((items, next))
}

/// Copy `digest` of `repo_name` (its image, layers and the `tags` pointing at
/// it) from `source_account` to `dest`. A cross-account destination gets the
/// copy in its own registry (the repository is created on demand, mirroring
/// the source's); a same-account destination in another Region shares the
/// source registry (fakecloud serves every Region from one registry), so
/// there is nothing to copy. Returns `false` when the source image is gone.
pub(crate) fn copy_image_to_destination(
    accounts: &mut MultiAccountState<EcrState>,
    source_account: &str,
    repo_name: &str,
    digest: &str,
    dest: &ReplicationDestination,
) -> bool {
    let (image, layers, tags, registry_id, endpoint) = {
        let Some(repo) = accounts
            .get(source_account)
            .and_then(|s| s.repositories.get(repo_name))
        else {
            return false;
        };
        let Some(image) = repo.images.get(digest).cloned() else {
            return false;
        };
        let tags: Vec<String> = repo
            .image_tags
            .iter()
            .filter(|(_, d)| d.as_str() == digest)
            .map(|(t, _)| t.clone())
            .collect();
        let endpoint = repo
            .repository_uri
            .strip_suffix(&format!("/{repo_name}"))
            .unwrap_or(&repo.repository_uri)
            .to_string();
        (
            image,
            layers_for_image(repo, digest),
            tags,
            repo.registry_id.clone(),
            endpoint,
        )
    };
    if dest.registry_id == registry_id {
        return true;
    }
    let target_state = accounts.get_or_create(&dest.registry_id);
    let target_repo = target_state
        .repositories
        .entry(repo_name.to_string())
        .or_insert_with(|| {
            let arn = crate::state::repository_arn(&dest.region, &dest.registry_id, repo_name);
            Repository::new(repo_name, arn, &dest.registry_id, &endpoint)
        });
    target_repo
        .images
        .entry(digest.to_string())
        .or_insert(image);
    for layer in layers {
        target_repo
            .layers
            .entry(layer.digest.clone())
            .or_insert(layer);
    }
    for tag in tags {
        target_repo.image_tags.insert(tag, digest.to_string());
    }
    true
}

/// Record a replication of `digest` to `dest` on the source repository, so
/// DescribeImageReplicationStatus reports it.
fn record_replication_status(
    state: &mut EcrState,
    repo_name: &str,
    digest: &str,
    dest: &ReplicationDestination,
    status: &str,
    failure_code: Option<&str>,
) {
    let Some(repo) = state.repositories.get_mut(repo_name) else {
        return;
    };
    let entries = repo
        .replication_statuses
        .entry(digest.to_string())
        .or_default();
    entries.retain(|e| !(e.region == dest.region && e.registry_id == dest.registry_id));
    entries.push(crate::state::ImageReplicationStatus {
        region: dest.region.clone(),
        registry_id: dest.registry_id.clone(),
        status: status.to_string(),
        failure_code: failure_code.map(str::to_string),
        failure_reason: None,
    });
}

/// Run every backfill of `account` still `PREPARING` whose preparation time
/// is up. Returns whether any ran.
pub(crate) fn advance_backfills(accounts: &mut MultiAccountState<EcrState>, account: &str) -> bool {
    let now = Utc::now();
    let due: Vec<String> = accounts
        .get(account)
        .map(|s| {
            s.replication_backfills
                .values()
                .filter(|b| {
                    b.status == "PREPARING"
                        && now - b.started_at >= chrono::Duration::from_std(PREPARE_DELAY).unwrap()
                })
                .map(|b| b.id.clone())
                .collect()
        })
        .unwrap_or_default();
    for id in &due {
        run_backfill(accounts, account, id);
    }
    !due.is_empty()
}

/// Run one backfill: replicate each image and tag of each repository whose
/// name a replication rule for the destination matches.
fn run_backfill(accounts: &mut MultiAccountState<EcrState>, account: &str, id: &str) {
    let Some(state) = accounts.get(account) else {
        return;
    };
    let Some(backfill) = state.replication_backfills.get(id) else {
        return;
    };
    let dest = backfill.destination.clone();
    let filters: Vec<Vec<crate::state::RepositoryFilter>> = state
        .replication_configuration
        .iter()
        .flat_map(|cfg| cfg.rules.iter())
        .filter(|rule| {
            rule.destinations
                .iter()
                .any(|d| d.region == dest.region && d.registry_id == dest.registry_id)
        })
        .map(|rule| rule.repository_filters.clone())
        .collect();
    // (repository, digest, tag) for every replication the backfill performs.
    let mut work: Vec<(String, String, Option<String>)> = Vec::new();
    for (name, repo) in &state.repositories {
        if !filters.iter().any(|f| repository_filters_match(f, name)) {
            continue;
        }
        for digest in repo.images.keys() {
            let tags: Vec<&String> = repo
                .image_tags
                .iter()
                .filter(|(_, d)| *d == digest)
                .map(|(t, _)| t)
                .collect();
            if tags.is_empty() {
                work.push((name.clone(), digest.clone(), None));
            }
            for tag in tags {
                work.push((name.clone(), digest.clone(), Some(tag.clone())));
            }
        }
    }

    let mut results = Vec::with_capacity(work.len());
    let mut progress = ReplicationBackfillProgress {
        total_replication_count: work.len() as u32,
        ..Default::default()
    };
    for (name, digest, tag) in work {
        let copied = copy_image_to_destination(accounts, account, &name, &digest, &dest);
        let (status, failure_code) = if copied {
            progress.completed_replication_count += 1;
            ("COMPLETE", None)
        } else {
            progress.failed_replication_count += 1;
            if !progress.failure_codes.iter().any(|c| c == IMAGE_NOT_FOUND) {
                progress.failure_codes.push(IMAGE_NOT_FOUND.to_string());
            }
            ("FAILED", Some(IMAGE_NOT_FOUND.to_string()))
        };
        if let Some(state) = accounts.get_mut(account) {
            record_replication_status(
                state,
                &name,
                &digest,
                &dest,
                status,
                failure_code.as_deref(),
            );
        }
        results.push(ReplicationBackfillResult {
            repository_name: name,
            image_digest: digest,
            image_tag: tag,
            status: status.to_string(),
            failure_code,
        });
    }
    let Some(backfill) = accounts
        .get_mut(account)
        .and_then(|s| s.replication_backfills.get_mut(id))
    else {
        return;
    };
    backfill.status = if progress.failed_replication_count > 0 {
        "FAILED".to_string()
    } else {
        "COMPLETE".to_string()
    };
    if progress.failed_replication_count > 0 {
        backfill.failure_code = Some("REPLICATION_FAILED".to_string());
    }
    backfill.progress = Some(progress);
    backfill.results = results;
    backfill.ended_at = Some(Utc::now());
}

impl EcrService {
    /// Run the account's due backfills, persisting when any ran.
    fn advance_due_backfills(&self, account: &str) {
        let ran = advance_backfills(&mut self.state.write(), account);
        if ran {
            self.spawn_snapshot();
        }
    }

    fn spawn_snapshot(&self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let shared = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            EcrService::save_snapshot_with(shared, store, lock).await;
        });
    }

    pub(super) fn start_replication_backfill(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let dest_value = body
            .get("destination")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_parameter("Missing required field: destination"))?;
        let dest = ReplicationDestination {
            region: req_str(dest_value, "region")?.to_string(),
            registry_id: req_str(dest_value, "registryId")?.to_string(),
        };
        let account = target_account_id(request, &body);
        self.advance_due_backfills(&account);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let configured = state.replication_configuration.as_ref().is_some_and(|cfg| {
            cfg.rules.iter().any(|rule| {
                rule.destinations
                    .iter()
                    .any(|d| d.region == dest.region && d.registry_id == dest.registry_id)
            })
        });
        if !configured {
            return Err(invalid_parameter(format!(
                "The destination with Region '{}' and registry '{}' is not configured in the registry's replication configuration.",
                dest.region, dest.registry_id
            )));
        }
        let active: Vec<&ReplicationBackfill> = state
            .replication_backfills
            .values()
            .filter(|b| in_progress(&b.status))
            .collect();
        if active.iter().any(|b| b.destination.region == dest.region) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ReplicationBackfillInProgressException",
                format!(
                    "A replication backfill is already in progress for the destination Region '{}'.",
                    dest.region
                ),
            ));
        }
        if active.len() >= MAX_IN_PROGRESS {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "LimitExceededException",
                format!(
                    "The registry already has {MAX_IN_PROGRESS} replication backfills in progress."
                ),
            ));
        }
        let backfill = ReplicationBackfill {
            id: Uuid::new_v4().to_string(),
            destination: dest,
            status: "PREPARING".to_string(),
            failure_code: None,
            progress: None,
            results: Vec::new(),
            started_at: Utc::now(),
            ended_at: None,
        };
        let response = json!({
            "replicationBackfillId": backfill.id,
            "registryId": state.account_id,
            "destination": destination_json(&backfill.destination),
            "status": backfill.status,
            "startedAt": ts(backfill.started_at),
        });
        state
            .replication_backfills
            .insert(backfill.id.clone(), backfill);
        drop(accounts);

        if tokio::runtime::Handle::try_current().is_ok() {
            let shared = self.state.clone();
            let store = self.snapshot_store.clone();
            let lock = self.snapshot_lock.clone();
            tokio::spawn(async move {
                tokio::time::sleep(PREPARE_DELAY).await;
                let ran = advance_backfills(&mut shared.write(), &account);
                if ran {
                    EcrService::save_snapshot_with(shared, store, lock).await;
                }
            });
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn stop_replication_backfill(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let id = backfill_id(&body)?;
        let account = target_account_id(request, &body);
        self.advance_due_backfills(&account);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let registry_id = state.account_id.clone();
        let backfill = state
            .replication_backfills
            .get_mut(&id)
            .ok_or_else(|| backfill_not_found(&id))?;
        if !in_progress(&backfill.status) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ReplicationBackfillInvalidStatusException",
                format!(
                    "The replication backfill '{id}' can't be stopped because its status is {}.",
                    backfill.status
                ),
            ));
        }
        // Nothing has been replicated while preparing, so it stops at once.
        backfill.status = "STOPPED".to_string();
        backfill.ended_at = Some(Utc::now());
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "replicationBackfillId": backfill.id,
            "destination": destination_json(&backfill.destination),
            "status": backfill.status,
            "startedAt": ts(backfill.started_at),
        })))
    }

    pub(super) fn list_replication_backfills(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let account = target_account_id(request, &body);
        self.advance_due_backfills(&account);
        let cutoff = Utc::now() - chrono::Duration::days(RETENTION_DAYS);
        let accounts = self.state.read();
        let state = accounts.get(&account);
        let mut backfills: Vec<&ReplicationBackfill> = state
            .map(|s| {
                s.replication_backfills
                    .values()
                    .filter(|b| b.ended_at.is_none_or(|end| end > cutoff))
                    .collect()
            })
            .unwrap_or_default();
        // Most recently started first.
        backfills.sort_by(|a, b| b.started_at.cmp(&a.started_at).then(a.id.cmp(&b.id)));
        let items: Vec<Value> = backfills.into_iter().map(summary_json).collect();
        let (page, next) = paginate(items, &body, 50)?;
        let mut out = json!({
            "registryId": state.map(|s| s.account_id.clone()).unwrap_or(account),
            "backfills": page,
        });
        if let Some(tok) = next {
            out["nextToken"] = json!(tok);
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn describe_replication_backfill_results(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let id = backfill_id(&body)?;
        let status_filter = opt_str(&body, "resultStatusFilter");
        if let Some(f) = status_filter {
            if !matches!(f, "COMPLETE" | "FAILED") {
                return Err(validation(format!(
                    "1 validation error detected: Value '{f}' at 'resultStatusFilter' failed to satisfy constraint: Member must satisfy enum value set: [COMPLETE, FAILED]"
                )));
            }
        }
        let account = target_account_id(request, &body);
        self.advance_due_backfills(&account);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| backfill_not_found(&id))?;
        let backfill = state
            .replication_backfills
            .get(&id)
            .ok_or_else(|| backfill_not_found(&id))?;
        let mut out = summary_json(backfill);
        out["registryId"] = json!(state.account_id);
        // Per-image results exist only once the backfill is terminal.
        if !in_progress(&backfill.status) {
            let items: Vec<Value> = backfill
                .results
                .iter()
                .filter(|r| status_filter.is_none_or(|f| r.status == f))
                .map(result_json)
                .collect();
            let (page, next) = paginate(items, &body, 1000)?;
            out["results"] = json!(page);
            if let Some(tok) = next {
                out["nextToken"] = json!(tok);
            }
        }
        Ok(AwsResponse::ok_json(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Image, ReplicationConfiguration, ReplicationRule};

    fn image(digest: &str) -> Image {
        serde_json::from_value(json!({
            "image_digest": digest,
            "image_manifest": "{}",
            "image_manifest_media_type": "application/vnd.oci.image.manifest.v1+json",
            "artifact_media_type": null,
            "image_size_in_bytes": 10,
            "image_pushed_at": "2026-01-01T00:00:00Z",
            "last_recorded_pull_time": null,
        }))
        .unwrap()
    }

    fn accounts_with_images() -> MultiAccountState<EcrState> {
        let mut accounts: MultiAccountState<EcrState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        let state = accounts.get_or_create("111111111111");
        for name in ["app/web", "other"] {
            let mut repo = Repository::new(
                name,
                crate::state::repository_arn("us-east-1", "111111111111", name),
                "111111111111",
                "http://localhost:4566",
            );
            repo.images.insert("sha256:aa".into(), image("sha256:aa"));
            repo.images.insert("sha256:bb".into(), image("sha256:bb"));
            repo.image_tags.insert("v1".into(), "sha256:aa".into());
            repo.image_tags.insert("latest".into(), "sha256:aa".into());
            state.repositories.insert(name.to_string(), repo);
        }
        state.replication_configuration = Some(ReplicationConfiguration {
            rules: vec![ReplicationRule {
                destinations: vec![ReplicationDestination {
                    region: "us-west-2".into(),
                    registry_id: "222222222222".into(),
                }],
                repository_filters: vec![crate::state::RepositoryFilter {
                    filter: "app".into(),
                    filter_type: "PREFIX_MATCH".into(),
                }],
            }],
        });
        accounts
    }

    fn preparing(accounts: &mut MultiAccountState<EcrState>, age: chrono::Duration) -> String {
        let id = Uuid::new_v4().to_string();
        accounts
            .get_or_create("111111111111")
            .replication_backfills
            .insert(
                id.clone(),
                ReplicationBackfill {
                    id: id.clone(),
                    destination: ReplicationDestination {
                        region: "us-west-2".into(),
                        registry_id: "222222222222".into(),
                    },
                    status: "PREPARING".into(),
                    failure_code: None,
                    progress: None,
                    results: Vec::new(),
                    started_at: Utc::now() - age,
                    ended_at: None,
                },
            );
        id
    }

    #[test]
    fn backfill_replicates_matching_images_and_tags_once_prepared() {
        let mut accounts = accounts_with_images();
        let id = preparing(&mut accounts, chrono::Duration::zero());
        // Not yet due: nothing runs.
        assert!(!advance_backfills(&mut accounts, "111111111111"));
        accounts
            .get_mut("111111111111")
            .unwrap()
            .replication_backfills
            .get_mut(&id)
            .unwrap()
            .started_at -= chrono::Duration::seconds(10);
        assert!(advance_backfills(&mut accounts, "111111111111"));

        let b = &accounts.get("111111111111").unwrap().replication_backfills[&id];
        assert_eq!(b.status, "COMPLETE");
        assert!(b.ended_at.is_some());
        let p = b.progress.as_ref().unwrap();
        // `app/web` only: sha256:aa twice (two tags) + untagged sha256:bb.
        assert_eq!(p.total_replication_count, 3);
        assert_eq!(p.completed_replication_count, 3);
        assert_eq!(b.results.len(), 3);

        let dest = accounts.get("222222222222").unwrap();
        let repo = &dest.repositories["app/web"];
        assert!(repo.images.contains_key("sha256:aa"));
        assert!(repo.images.contains_key("sha256:bb"));
        assert_eq!(repo.image_tags["v1"], "sha256:aa");
        assert!(!dest.repositories.contains_key("other"));
        // The source records the replication.
        let src = &accounts.get("111111111111").unwrap().repositories["app/web"];
        assert_eq!(src.replication_statuses["sha256:aa"][0].status, "COMPLETE");
    }

    #[test]
    fn stopped_backfill_never_runs() {
        let mut accounts = accounts_with_images();
        let id = preparing(&mut accounts, chrono::Duration::seconds(10));
        accounts
            .get_mut("111111111111")
            .unwrap()
            .replication_backfills
            .get_mut(&id)
            .unwrap()
            .status = "STOPPED".into();
        assert!(!advance_backfills(&mut accounts, "111111111111"));
        assert!(accounts.get("222222222222").is_none());
    }
}
