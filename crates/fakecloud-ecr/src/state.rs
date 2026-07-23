use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type SharedEcrState = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<EcrState>>>;

impl fakecloud_core::multi_account::AccountState for EcrState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

pub const ECR_SNAPSHOT_SCHEMA_VERSION: u32 = 4;

/// Top-level persisted ECR snapshot. The shape mirrors the convention
/// used by other multi-account services (Kinesis, ElastiCache) so the
/// `main.rs` loader can use the same branching pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EcrSnapshot {
    pub schema_version: u32,
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<EcrState>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EcrState {
    pub account_id: String,
    pub region: String,
    /// Repository name -> repository.
    pub repositories: BTreeMap<String, Repository>,
    /// Registry-level policy JSON document. `None` until the caller
    /// sets one via `PutRegistryPolicy`.
    pub registry_policy: Option<String>,
    /// Registry-level scanning configuration. Defaults to `BASIC` per
    /// AWS behaviour; tracked here so `Get/PutRegistryScanningConfiguration`
    /// round-trips correctly.
    pub registry_scanning_configuration: RegistryScanningConfiguration,
    /// Registry-level replication configuration.
    pub replication_configuration: Option<ReplicationConfiguration>,
    /// Account setting flags keyed by setting name (e.g.,
    /// `BASIC_SCAN_TYPE_VERSION`, `REGISTRY_POLICY_SCOPE`).
    pub account_settings: BTreeMap<String, String>,
    /// Layer upload state machine keyed by `uploadId`. Each entry is
    /// tied to a specific repository.
    ///
    /// NOT persisted: the per-upload spool file lives under the system temp
    /// dir (`spool_path`) and never survives a restart, so persisting the
    /// upload metadata alone would leave orphan entries whose next
    /// `UploadLayerPart` opens a missing spool and 500s. Dropping in-flight
    /// uploads on restart keeps the map consistent with the (gone) spools; a
    /// stale client simply re-initiates, matching ECR's finite upload lifetime.
    #[serde(skip)]
    pub layer_uploads: BTreeMap<String, LayerUpload>,
    /// Pull-time update exclusions keyed by IAM principal ARN. These
    /// are registry-level per the Smithy model.
    #[serde(default)]
    pub pull_time_exclusions: BTreeMap<String, PullTimeExclusion>,
    /// Pull-through cache rules keyed by `ecrRepositoryPrefix`.
    #[serde(default)]
    pub pull_through_cache_rules: BTreeMap<String, PullThroughCacheRule>,
    /// Repository creation templates keyed by prefix.
    #[serde(default)]
    pub repository_creation_templates: BTreeMap<String, RepositoryCreationTemplate>,
    /// Registry-wide signing configuration.
    #[serde(default)]
    pub signing_configuration: Option<SigningConfiguration>,
}

impl EcrState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            repositories: BTreeMap::new(),
            registry_policy: None,
            registry_scanning_configuration: RegistryScanningConfiguration::default(),
            replication_configuration: None,
            account_settings: BTreeMap::new(),
            layer_uploads: BTreeMap::new(),
            pull_time_exclusions: BTreeMap::new(),
            pull_through_cache_rules: BTreeMap::new(),
            repository_creation_templates: BTreeMap::new(),
            signing_configuration: None,
        }
    }

    pub fn reset(&mut self) {
        self.repositories.clear();
        self.registry_policy = None;
        self.registry_scanning_configuration = RegistryScanningConfiguration::default();
        self.replication_configuration = None;
        self.account_settings.clear();
        self.layer_uploads.clear();
        self.pull_time_exclusions.clear();
        self.pull_through_cache_rules.clear();
        self.repository_creation_templates.clear();
        self.signing_configuration = None;
    }

    pub fn repository_arn(&self, repository_name: &str) -> String {
        format!(
            "arn:aws:ecr:{}:{}:repository/{}",
            self.region, self.account_id, repository_name
        )
    }

    pub fn registry_id(&self) -> &str {
        &self.account_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Repository {
    pub repository_name: String,
    pub repository_arn: String,
    pub registry_id: String,
    pub repository_uri: String,
    pub created_at: DateTime<Utc>,
    pub image_tag_mutability: String,
    pub image_scanning_configuration: ImageScanningConfiguration,
    pub encryption_configuration: EncryptionConfiguration,
    pub tags: BTreeMap<String, String>,
    /// Repository-level policy document JSON. `None` until the caller
    /// sets one via `SetRepositoryPolicy`.
    pub policy: Option<String>,
    /// Repository-level lifecycle policy document JSON.
    pub lifecycle_policy: Option<String>,
    /// Last lifecycle-policy document passed to
    /// `StartLifecyclePolicyPreview` — distinct from the active
    /// `lifecycle_policy` so a preview against an alternate document
    /// doesn't corrupt the live policy. `GetLifecyclePolicyPreview`
    /// reads from this field.
    #[serde(default)]
    pub lifecycle_policy_preview: Option<String>,
    /// Last time the lifecycle policy was evaluated against this
    /// repository's images. `None` until the policy has been applied
    /// at least once. Surfaced through `GetLifecyclePolicy`'s
    /// `lastEvaluatedAt` field.
    #[serde(default)]
    pub lifecycle_policy_last_evaluated_at: Option<DateTime<Utc>>,
    /// Per-image scan findings, keyed by manifest digest.
    #[serde(default)]
    pub scan_findings: BTreeMap<String, ImageScanFindings>,
    /// Stored images keyed by manifest digest (sha256). One image can
    /// have many tags (via `image_tags`).
    #[serde(default)]
    pub images: BTreeMap<String, Image>,
    /// Tag name -> image digest. Multiple tags can point to the same
    /// digest.
    #[serde(default)]
    pub image_tags: BTreeMap<String, String>,
    /// Content-addressed layer blobs keyed by their sha256 digest
    /// (e.g. `sha256:deadbeef…`). Stored as base64 to keep JSON
    /// snapshots portable.
    #[serde(default)]
    pub layers: BTreeMap<String, Layer>,
    /// Per-image replication status entries, keyed by image digest.
    /// Populated as PutImage fans out to each registered destination.
    #[serde(default)]
    pub replication_statuses: BTreeMap<String, Vec<ImageReplicationStatus>>,
}

/// Outcome of replicating one image to one destination registry/region.
/// Surfaced through `DescribeImageReplicationStatus`. AWS returns both
/// `failureCode` and `failureReason`; we keep them as Options because
/// they're only set when `status == "FAILED"`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImageReplicationStatus {
    pub region: String,
    pub registry_id: String,
    pub status: String,
    pub failure_code: Option<String>,
    #[serde(default)]
    pub failure_reason: Option<String>,
}

impl Repository {
    pub fn new(
        repository_name: &str,
        repository_arn: String,
        registry_id: &str,
        endpoint: &str,
    ) -> Self {
        // Strip scheme from endpoint for repositoryUri (docker requires host only).
        let host = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .trim_end_matches('/')
            .to_string();
        Self {
            repository_name: repository_name.to_string(),
            repository_arn,
            registry_id: registry_id.to_string(),
            repository_uri: format!("{host}/{repository_name}"),
            created_at: Utc::now(),
            image_tag_mutability: "MUTABLE".to_string(),
            image_scanning_configuration: ImageScanningConfiguration::default(),
            encryption_configuration: EncryptionConfiguration::default(),
            tags: BTreeMap::new(),
            policy: None,
            lifecycle_policy: None,
            lifecycle_policy_preview: None,
            lifecycle_policy_last_evaluated_at: None,
            scan_findings: BTreeMap::new(),
            images: BTreeMap::new(),
            image_tags: BTreeMap::new(),
            layers: BTreeMap::new(),
            replication_statuses: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullTimeExclusion {
    pub principal_arn: String,
    pub registered_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImageScanFindings {
    pub image_digest: String,
    pub scan_status: String,
    pub scan_completed_at: Option<DateTime<Utc>>,
    pub vulnerability_source_updated_at: Option<DateTime<Utc>>,
    pub finding_severity_counts: BTreeMap<String, i64>,
    pub findings: Vec<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullThroughCacheRule {
    pub ecr_repository_prefix: String,
    pub upstream_registry_url: String,
    pub upstream_registry: Option<String>,
    pub credential_arn: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub custom_role_arn: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepositoryCreationTemplate {
    pub prefix: String,
    pub description: Option<String>,
    pub image_tag_mutability: String,
    pub applied_for: Vec<String>,
    pub resource_tags: Vec<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub custom_role_arn: Option<String>,
    pub repository_policy: Option<String>,
    pub lifecycle_policy: Option<String>,
    pub encryption_configuration: Option<EncryptionConfiguration>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SigningConfiguration {
    /// Raw rule payload from `PutSigningConfiguration`. Round-trippable
    /// via `GetSigningConfiguration` even when a rule specifies a
    /// key algorithm we can't verify against.
    pub rules: Vec<Value>,
    /// PEM-parsed public keys that `DescribeImageSigningStatus` will
    /// use to verify companion cosign signatures. Populated lazily
    /// from `rules` at `PutSigningConfiguration` time; unrecognised
    /// rule shapes just leave this empty.
    #[serde(default)]
    pub trusted_keys: Vec<crate::signing::TrustedKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Image {
    pub image_digest: String,
    pub image_manifest: String,
    pub image_manifest_media_type: String,
    pub artifact_media_type: Option<String>,
    pub image_size_in_bytes: u64,
    pub image_pushed_at: DateTime<Utc>,
    pub last_recorded_pull_time: Option<DateTime<Utc>>,
    /// Lifecycle/storage state surfaced through `ImageDetail.imageStatus`
    /// in `DescribeImages`. One of `ACTIVE`, `ARCHIVED`, `ACTIVATING`.
    /// Defaults to `ACTIVE` (the only value AWS exposes for newly pushed
    /// images); transitions are driven by `UpdateImageStorageClass`.
    #[serde(default = "default_image_status")]
    pub image_status: String,
    /// Last time `UpdateImageStorageClass` archived this image. `None`
    /// while the image has never been archived. Surfaced as
    /// `ImageDetail.lastArchivedAt`.
    #[serde(default)]
    pub last_archived_at: Option<DateTime<Utc>>,
    /// Last time `UpdateImageStorageClass` restored this image from
    /// archive. `None` while the image has never been activated from
    /// archive. Surfaced as `ImageDetail.lastActivatedAt`.
    #[serde(default)]
    pub last_activated_at: Option<DateTime<Utc>>,
    /// Last time the image was read by a pull-shaped op (`BatchGetImage`,
    /// `GetDownloadUrlForLayer`, OCI manifest GET, OCI blob GET). Updated
    /// alongside `last_recorded_pull_time` so callers that rely on the
    /// fakecloud-extension `lastInUseAt`/`inUseCount` pair can introspect
    /// pull frequency in tests.
    #[serde(default)]
    pub last_in_use_at: Option<DateTime<Utc>>,
    /// Monotonic counter of pull-shaped accesses. Bumped by the same
    /// touch points that update `last_in_use_at`. Defaults to 0; not
    /// part of the AWS Smithy model — fakecloud surfaces it as
    /// `inUseCount` in `DescribeImages` for parity with the user
    /// expectation.
    #[serde(default)]
    pub in_use_count: u64,
}

fn default_image_status() -> String {
    "ACTIVE".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Layer {
    pub digest: String,
    pub size: u64,
    /// Base64-encoded blob bytes. When the owning repository has
    /// `EncryptionConfiguration.encryption_type == "KMS"`, these bytes
    /// are the envelope produced by `fakecloud_kms::api::encrypt_blob`;
    /// `blob_get` decrypts on the way out. For AES256 (fakecloud's
    /// default) the bytes are the plaintext blob.
    pub blob_b64: String,
    pub media_type: String,
    /// ARN of the KMS key the blob was encrypted under, when stored
    /// encrypted. `None` means the bytes in `blob_b64` are plaintext
    /// — either because the repo used the default AES256 encryption
    /// (fakecloud-internal, no-op) or the layer pre-dates the KMS
    /// wire-up.
    #[serde(default)]
    pub encrypted_with_kms_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LayerUpload {
    pub upload_id: String,
    pub repository_name: String,
    pub created_at: DateTime<Utc>,
    /// Filesystem path to the in-progress upload's spool file. Each
    /// `UploadLayerPart` (JSON control plane) and OCI blob `PATCH`
    /// appends raw bytes to this file; the OCI `PUT` finish step
    /// streams the final chunk in, computes SHA-256 over the file in
    /// constant memory, and then promotes the bytes into a `Layer`.
    /// Storing the path means a 1 GiB push never holds the partial
    /// upload in RAM.
    #[serde(default)]
    pub spool_path: String,
    pub last_byte_received: u64,
    /// True while a layer-part / blob-`PATCH` append for this upload is
    /// in flight (validated + reserved under the state lock, but the
    /// actual file append runs with the lock released so a multi-hundred-
    /// MB chunk doesn't stall a worker). A second concurrent append that
    /// observes this flag set is rejected BEFORE it appends, so two parts
    /// racing on the same start offset can't both write into the spool and
    /// corrupt it. Runtime-only, never persisted.
    #[serde(default, skip)]
    pub append_in_flight: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ImageScanningConfiguration {
    /// Whether images are scanned automatically on push. Defaults to `false`.
    pub scan_on_push: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptionConfiguration {
    /// `AES256` or `KMS`.
    pub encryption_type: String,
    /// KMS key ARN when `encryption_type == "KMS"`.
    pub kms_key: Option<String>,
}

impl Default for EncryptionConfiguration {
    fn default() -> Self {
        Self {
            encryption_type: "AES256".to_string(),
            kms_key: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryScanningConfiguration {
    /// `BASIC` or `ENHANCED`.
    pub scan_type: String,
    pub rules: Vec<RegistryScanningRule>,
}

impl Default for RegistryScanningConfiguration {
    fn default() -> Self {
        Self {
            scan_type: "BASIC".to_string(),
            rules: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryScanningRule {
    pub scan_frequency: String,
    pub repository_filters: Vec<RepositoryFilter>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepositoryFilter {
    pub filter: String,
    pub filter_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationConfiguration {
    pub rules: Vec<ReplicationRule>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationRule {
    pub destinations: Vec<ReplicationDestination>,
    pub repository_filters: Vec<RepositoryFilter>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationDestination {
    pub region: String,
    pub registry_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layer_uploads_are_not_persisted_across_snapshot() {
        // In-progress uploads spool to the system temp dir, which does not
        // survive a restart. Persisting the upload metadata alone would leave
        // an orphan entry whose next `UploadLayerPart` opens a missing spool
        // and 500s (bug-hunt 4.2), so `layer_uploads` is `#[serde(skip)]`.
        let mut state = EcrState::new("123456789012", "us-east-1");
        state.layer_uploads.insert(
            "upload-1".to_string(),
            LayerUpload {
                upload_id: "upload-1".to_string(),
                repository_name: "repo".to_string(),
                created_at: Utc::now(),
                spool_path: "/tmp/fakecloud-ecr-upload-upload-1".to_string(),
                last_byte_received: 42,
                append_in_flight: false,
            },
        );

        let json = serde_json::to_string(&state).unwrap();
        assert!(
            !json.contains("upload-1"),
            "in-progress uploads must not be serialized"
        );

        let restored: EcrState = serde_json::from_str(&json).unwrap();
        assert!(
            restored.layer_uploads.is_empty(),
            "in-progress uploads must not survive a restart"
        );
    }
}
