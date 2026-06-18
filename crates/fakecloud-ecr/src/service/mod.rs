use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::validate_string_length;
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    EcrSnapshot, EncryptionConfiguration, Image, ImageScanningConfiguration, Layer, LayerUpload,
    Repository, SharedEcrState, ECR_SNAPSHOT_SCHEMA_VERSION,
};

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateRepository",
    "DeleteRepository",
    "DescribeRepositories",
    "PutImageTagMutability",
    "PutImageScanningConfiguration",
    "SetRepositoryPolicy",
    "GetRepositoryPolicy",
    "DeleteRepositoryPolicy",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
    "PutImage",
    "BatchGetImage",
    "BatchDeleteImage",
    "BatchCheckLayerAvailability",
    "DescribeImages",
    "ListImages",
    "GetDownloadUrlForLayer",
    "InitiateLayerUpload",
    "UploadLayerPart",
    "CompleteLayerUpload",
    "GetAuthorizationToken",
    "PutLifecyclePolicy",
    "GetLifecyclePolicy",
    "DeleteLifecyclePolicy",
    "StartLifecyclePolicyPreview",
    "GetLifecyclePolicyPreview",
    "StartImageScan",
    "DescribeImageScanFindings",
    "DescribeRegistry",
    "GetRegistryPolicy",
    "PutRegistryPolicy",
    "DeleteRegistryPolicy",
    "GetRegistryScanningConfiguration",
    "PutRegistryScanningConfiguration",
    "BatchGetRepositoryScanningConfiguration",
    "PutReplicationConfiguration",
    "DescribeImageReplicationStatus",
    "CreatePullThroughCacheRule",
    "DeletePullThroughCacheRule",
    "DescribePullThroughCacheRules",
    "UpdatePullThroughCacheRule",
    "ValidatePullThroughCacheRule",
    "GetAccountSetting",
    "PutAccountSetting",
    "CreateRepositoryCreationTemplate",
    "DeleteRepositoryCreationTemplate",
    "DescribeRepositoryCreationTemplates",
    "UpdateRepositoryCreationTemplate",
    "GetSigningConfiguration",
    "PutSigningConfiguration",
    "DeleteSigningConfiguration",
    "DescribeImageSigningStatus",
    "RegisterPullTimeUpdateExclusion",
    "DeregisterPullTimeUpdateExclusion",
    "ListPullTimeUpdateExclusions",
    "ListImageReferrers",
    "UpdateImageStorageClass",
];

pub struct EcrService {
    state: SharedEcrState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    /// KMS state handle — when wired, repositories configured with
    /// `EncryptionConfiguration.encryption_type == "KMS"` store layer
    /// blobs encrypted under the configured CMK via
    /// `fakecloud_kms::api::encrypt_blob` / `decrypt_blob`.
    kms_state: Option<fakecloud_kms::SharedKmsState>,
}

mod images;
mod layers;
mod lifecycle;
mod policies;
mod pull_through;
mod registry;
mod repositories;
mod scanning;
mod settings;
mod signing;
mod tags;
mod templates;

impl EcrService {
    pub fn new(state: SharedEcrState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            kms_state: None,
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_kms(mut self, kms: fakecloud_kms::SharedKmsState) -> Self {
        self.kms_state = Some(kms);
        self
    }

    /// Read-only accessor for the multi-account state. The sibling
    /// `oci` module owns the HTTP-layer adapter for the OCI v2
    /// protocol and needs to reach the same repositories + blobs the
    /// JSON control-plane ops read and write.
    pub fn state_handle(&self) -> &SharedEcrState {
        &self.state
    }

    /// Handle for the shared KMS state when wired. `None` skips the
    /// encrypt/decrypt paths and stores / returns plaintext blobs.
    pub(crate) fn kms_handle(&self) -> Option<&fakecloud_kms::SharedKmsState> {
        self.kms_state.as_ref()
    }

    async fn save_snapshot(&self) {
        Self::save_snapshot_with(
            self.state.clone(),
            self.snapshot_store.clone(),
            self.snapshot_lock.clone(),
        )
        .await;
    }

    /// Build a hook that persists the current state when invoked, or `None` in
    /// memory mode. The CloudFormation provisioner mutates `state` directly and
    /// uses this to write a CFN-provisioned resource through to disk.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                Self::save_snapshot_with(state, Some(store), lock).await;
            })
        }))
    }

    /// Snapshot writer reachable from background tasks (e.g. the async
    /// image-scanner) that don't hold a `&self` reference. Equivalent
    /// to [`save_snapshot`] but takes the components as owned clones.
    pub(crate) async fn save_snapshot_with(
        state: SharedEcrState,
        store: Option<Arc<dyn SnapshotStore>>,
        lock: Arc<AsyncMutex<()>>,
    ) {
        let Some(store) = store else {
            return;
        };
        let _guard = lock.lock().await;
        let snapshot = EcrSnapshot {
            schema_version: ECR_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(state.read().clone()),
        };
        let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let bytes = serde_json::to_vec(&snapshot)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            store.save(&bytes)
        })
        .await;
        match join {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!(%err, "failed to write ecr snapshot"),
            Err(err) => tracing::error!(%err, "ecr snapshot task panicked"),
        }
    }
}

#[async_trait]
impl AwsService for EcrService {
    fn service_name(&self) -> &str {
        "ecr"
    }

    async fn handle(&self, mut request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // OCI v2 Distribution requests come in as path-only REST
        // (`/v2/...` with no `X-Amz-Target`). Dispatch them before the
        // JSON control plane. Blob upload PATCH/PUT consume the raw
        // body stream directly via a per-upload spool file — they
        // never call `drain_request_stream`, so a 1 GiB layer push
        // moves through fakecloud in constant memory. Other OCI routes
        // (manifest PUT, blob HEAD/GET, …) keep using `request.body`
        // so we drain the stream conditionally.
        if request
            .path_segments
            .first()
            .map(|s| s == "v2")
            .unwrap_or(false)
        {
            // Drain unless this is a blob-upload PATCH/PUT — those
            // routes own the streaming consumer.
            let is_blob_upload = matches!(request.method, http::Method::PATCH | http::Method::PUT)
                && request.path_segments.len() >= 5
                && request.path_segments[request.path_segments.len() - 2] == "uploads";
            if !is_blob_upload {
                if let Some(stream) = request.take_body_stream() {
                    request.body = fakecloud_core::service::drain_request_stream(stream).await?;
                }
            }
            let result = crate::oci::dispatch(self, &request).await;
            // POST/PUT/PATCH/DELETE always mutate. GET to a `blobs/<digest>`
            // or `manifests/<reference>` endpoint also mutates because those
            // handlers bump the touched image's `last_in_use_at` /
            // `in_use_count` / `last_recorded_pull_time`. `tags/list` (also
            // GET) is read-only and excluded.
            let is_pull_get = request.method == http::Method::GET
                && request.path_segments.len() >= 3
                && matches!(
                    request.path_segments[request.path_segments.len() - 2].as_str(),
                    "blobs" | "manifests"
                );
            let mutates_oci = is_pull_get
                || matches!(
                    request.method,
                    http::Method::POST
                        | http::Method::PUT
                        | http::Method::PATCH
                        | http::Method::DELETE
                );
            if mutates_oci && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
                self.save_snapshot().await;
            }
            return result;
        }

        // JSON control plane: actions read `request.body`, so drain.
        if let Some(stream) = request.take_body_stream() {
            request.body = fakecloud_core::service::drain_request_stream(stream).await?;
        }

        let mutates = is_mutating(request.action.as_str());
        let result = match request.action.as_str() {
            "CreateRepository" => self.create_repository(&request),
            "DeleteRepository" => self.delete_repository(&request),
            "DescribeRepositories" => self.describe_repositories(&request),
            "PutImageTagMutability" => self.put_image_tag_mutability(&request),
            "PutImageScanningConfiguration" => self.put_image_scanning_configuration(&request),
            "SetRepositoryPolicy" => self.set_repository_policy(&request),
            "GetRepositoryPolicy" => self.get_repository_policy(&request),
            "DeleteRepositoryPolicy" => self.delete_repository_policy(&request),
            "TagResource" => self.tag_resource(&request),
            "UntagResource" => self.untag_resource(&request),
            "ListTagsForResource" => self.list_tags_for_resource(&request),
            "PutImage" => self.put_image(&request),
            "BatchGetImage" => self.batch_get_image(&request),
            "BatchDeleteImage" => self.batch_delete_image(&request),
            "BatchCheckLayerAvailability" => self.batch_check_layer_availability(&request),
            "DescribeImages" => self.describe_images(&request),
            "ListImages" => self.list_images(&request),
            "GetDownloadUrlForLayer" => self.get_download_url_for_layer(&request),
            "InitiateLayerUpload" => self.initiate_layer_upload(&request),
            "UploadLayerPart" => self.upload_layer_part(&request).await,
            "CompleteLayerUpload" => self.complete_layer_upload(&request).await,
            "GetAuthorizationToken" => self.get_authorization_token(&request),
            "PutLifecyclePolicy" => self.put_lifecycle_policy(&request),
            "GetLifecyclePolicy" => self.get_lifecycle_policy(&request),
            "DeleteLifecyclePolicy" => self.delete_lifecycle_policy(&request),
            "StartLifecyclePolicyPreview" => self.start_lifecycle_policy_preview(&request),
            "GetLifecyclePolicyPreview" => self.get_lifecycle_policy_preview(&request),
            "StartImageScan" => self.start_image_scan(&request),
            "DescribeImageScanFindings" => self.describe_image_scan_findings(&request),
            "DescribeRegistry" => self.describe_registry(&request),
            "GetRegistryPolicy" => self.get_registry_policy(&request),
            "PutRegistryPolicy" => self.put_registry_policy(&request),
            "DeleteRegistryPolicy" => self.delete_registry_policy(&request),
            "GetRegistryScanningConfiguration" => {
                self.get_registry_scanning_configuration(&request)
            }
            "PutRegistryScanningConfiguration" => {
                self.put_registry_scanning_configuration(&request)
            }
            "BatchGetRepositoryScanningConfiguration" => {
                self.batch_get_repository_scanning_configuration(&request)
            }
            "PutReplicationConfiguration" => self.put_replication_configuration(&request),
            "DescribeImageReplicationStatus" => self.describe_image_replication_status(&request),
            "CreatePullThroughCacheRule" => self.create_pull_through_cache_rule(&request),
            "DeletePullThroughCacheRule" => self.delete_pull_through_cache_rule(&request),
            "DescribePullThroughCacheRules" => self.describe_pull_through_cache_rules(&request),
            "UpdatePullThroughCacheRule" => self.update_pull_through_cache_rule(&request),
            "ValidatePullThroughCacheRule" => self.validate_pull_through_cache_rule(&request),
            "GetAccountSetting" => self.get_account_setting(&request),
            "PutAccountSetting" => self.put_account_setting(&request),
            "CreateRepositoryCreationTemplate" => {
                self.create_repository_creation_template(&request)
            }
            "DeleteRepositoryCreationTemplate" => {
                self.delete_repository_creation_template(&request)
            }
            "DescribeRepositoryCreationTemplates" => {
                self.describe_repository_creation_templates(&request)
            }
            "UpdateRepositoryCreationTemplate" => {
                self.update_repository_creation_template(&request)
            }
            "GetSigningConfiguration" => self.get_signing_configuration(&request),
            "PutSigningConfiguration" => self.put_signing_configuration(&request),
            "DeleteSigningConfiguration" => self.delete_signing_configuration(&request),
            "DescribeImageSigningStatus" => self.describe_image_signing_status(&request),
            "RegisterPullTimeUpdateExclusion" => self.register_pull_time_update_exclusion(&request),
            "DeregisterPullTimeUpdateExclusion" => {
                self.deregister_pull_time_update_exclusion(&request)
            }
            "ListPullTimeUpdateExclusions" => self.list_pull_time_update_exclusions(&request),
            "ListImageReferrers" => self.list_image_referrers(&request),
            "UpdateImageStorageClass" => self.update_image_storage_class(&request),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                &request.action,
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }
}

// -------- helpers --------

// -------- operations --------

impl EcrService {}

// -------- image + layer helpers --------

// -------- image + layer operations --------

impl EcrService {
    /// Mark `digest` as `IN_PROGRESS` and spawn the scanner task.
    /// Identical wiring to `start_image_scan` minus the request-shaped
    /// response — used both by the user-facing `StartImageScan` and the
    /// `scan_on_push=true` PutImage hook.
    fn trigger_scan(&self, account: &str, name: &str, digest: &str) {
        use crate::state::ImageScanFindings;
        let layers = {
            let mut accounts = self.state.write();
            let Some(state) = accounts.get_mut(account) else {
                return;
            };
            let Some(repo) = state.repositories.get_mut(name) else {
                return;
            };
            repo.scan_findings.insert(
                digest.to_string(),
                ImageScanFindings {
                    image_digest: digest.to_string(),
                    scan_status: "IN_PROGRESS".to_string(),
                    scan_completed_at: None,
                    vulnerability_source_updated_at: None,
                    finding_severity_counts: BTreeMap::new(),
                    findings: Vec::new(),
                },
            );
            layers_for_image(repo, digest)
        };
        let shared = self.state.clone();
        let store = self.snapshot_store.clone();
        let snap_lock = self.snapshot_lock.clone();
        let account = account.to_string();
        let name = name.to_string();
        let digest = digest.to_string();
        tokio::spawn(async move {
            let result = crate::scanner::scan_layers(&digest, &layers).await;
            {
                let mut accounts = shared.write();
                let Some(state) = accounts.get_mut(&account) else {
                    return;
                };
                let Some(repo) = state.repositories.get_mut(&name) else {
                    return;
                };
                let findings = result.unwrap_or_else(|| ImageScanFindings {
                    image_digest: digest.clone(),
                    scan_status: "COMPLETE".to_string(),
                    scan_completed_at: Some(Utc::now()),
                    vulnerability_source_updated_at: Some(Utc::now()),
                    finding_severity_counts: BTreeMap::new(),
                    findings: Vec::new(),
                });
                repo.scan_findings.insert(digest.clone(), findings);
            }
            EcrService::save_snapshot_with(shared, store, snap_lock).await;
        });
    }
}

// -------- lifecycle + scan + registry + polish handlers (Batch 4) --------

impl EcrService {
    fn put_replication_configuration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use crate::state::{
            ReplicationConfiguration, ReplicationDestination, ReplicationRule, RepositoryFilter,
        };
        let body = request.json_body();
        let cfg_value = body
            .get("replicationConfiguration")
            .cloned()
            .ok_or_else(|| invalid_parameter("Missing replicationConfiguration"))?;
        let rules_value = cfg_value
            .get("rules")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let rules: Vec<ReplicationRule> = rules_value
            .iter()
            .map(|r| ReplicationRule {
                destinations: r
                    .get("destinations")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .map(|d| ReplicationDestination {
                                region: d
                                    .get("region")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                registry_id: d
                                    .get("registryId")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                repository_filters: r
                    .get("repositoryFilters")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .map(|f| RepositoryFilter {
                                filter: f
                                    .get("filter")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                filter_type: f
                                    .get("filterType")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("PREFIX_MATCH")
                                    .to_string(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            })
            .collect();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        state.replication_configuration = Some(ReplicationConfiguration { rules });
        let cfg = state.replication_configuration.clone().unwrap();
        Ok(AwsResponse::ok_json(json!({
            "replicationConfiguration": {
                "rules": cfg.rules.iter().map(|r| json!({
                    "destinations": r.destinations.iter().map(|d| json!({
                        "region": d.region,
                        "registryId": d.registry_id,
                    })).collect::<Vec<_>>(),
                    "repositoryFilters": r.repository_filters.iter().map(|f| json!({
                        "filter": f.filter,
                        "filterType": f.filter_type,
                    })).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
            },
        })))
    }
}

#[path = "../service_helpers.rs"]
mod service_helpers;
pub use service_helpers::evaluate_lifecycle_policy;
pub(crate) use service_helpers::*;

#[cfg(test)]
#[path = "../service_tests.rs"]
mod tests;
