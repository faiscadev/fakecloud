use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaFunction {
    pub function_name: String,
    pub function_arn: String,
    pub runtime: String,
    pub role: String,
    pub handler: String,
    pub description: String,
    pub timeout: i64,
    pub memory_size: i64,
    pub code_sha256: String,
    pub code_size: i64,
    pub version: String,
    pub last_modified: DateTime<Utc>,
    pub tags: BTreeMap<String, String>,
    pub environment: BTreeMap<String, String>,
    pub architectures: Vec<String>,
    pub package_type: String,
    pub code_zip: Option<Vec<u8>>,
    /// Container image URI for `PackageType=Image` functions. Points at a
    /// private or public ECR image that the runtime pulls at invoke time.
    /// `None` for `PackageType=Zip`.
    #[serde(default)]
    pub image_uri: Option<String>,
    /// Resource-based policy attached to this function via
    /// `AddPermission`, serialized as a full JSON policy document
    /// (`{"Version":"2012-10-17","Statement":[...]}`). `None` means
    /// the function has no resource policy attached, matching the
    /// `ResourceNotFoundException` AWS returns from `GetPolicy` in
    /// that state. `AddPermission` lazily initializes this; every
    /// `RemovePermission` leaves at least `{"Statement":[]}` behind,
    /// matching AWS behavior.
    pub policy: Option<String>,
    /// Layer versions attached to this function, in attach order. AWS
    /// extracts each layer's content into `/opt` of the runtime sandbox at
    /// invoke time; fakecloud's container runtime mirrors that via
    /// `docker cp`. `code_size` is captured at attach time from the
    /// referenced `LayerVersion` so `GetFunctionConfiguration` can echo it
    /// without a second state lookup; layer versions are immutable so the
    /// cached size never goes stale.
    #[serde(default)]
    pub layers: Vec<AttachedLayer>,
    /// `RevisionId` is a stable token AWS expects to round-trip through
    /// optimistic-concurrency calls (`UpdateFunctionConfiguration`,
    /// `UpdateFunctionCode`, `AddPermission`, …). It only changes when
    /// the function config changes; we used to mint a fresh UUID per
    /// `function_config_json` call which broke client-side ETag-style
    /// guards.
    #[serde(default = "default_revision_id")]
    pub revision_id: String,
    /// `TracingConfig.Mode` — `PassThrough` (default) or `Active`.
    #[serde(default)]
    pub tracing_mode: Option<String>,
    /// `KMSKeyArn` for env-var encryption (defaults to AWS-managed
    /// `aws/lambda` when unset, which we represent as `None`).
    #[serde(default)]
    pub kms_key_arn: Option<String>,
    /// `EphemeralStorage.Size` in MiB. AWS default is 512.
    #[serde(default)]
    pub ephemeral_storage_size: Option<i64>,
    /// `VpcConfig` (`SubnetIds`, `SecurityGroupIds`, `Ipv6AllowedForDualStack`).
    /// fakecloud doesn't network-isolate; we just round-trip the shape.
    #[serde(default)]
    pub vpc_config: Option<serde_json::Value>,
    /// `SnapStart` (`ApplyOn`, `OptimizationStatus`).
    #[serde(default)]
    pub snap_start: Option<serde_json::Value>,
    /// `DeadLetterConfig.TargetArn` for async-invoke failures.
    #[serde(default)]
    pub dead_letter_config_arn: Option<String>,
    /// `FileSystemConfigs` (EFS access points). Round-tripped only.
    #[serde(default)]
    pub file_system_configs: Vec<serde_json::Value>,
    /// `LoggingConfig` (LogFormat, ApplicationLogLevel, SystemLogLevel,
    /// LogGroup).
    #[serde(default)]
    pub logging_config: Option<serde_json::Value>,
    /// `ImageConfigResponse.ImageConfig` for container-package functions.
    #[serde(default)]
    pub image_config: Option<serde_json::Value>,
    /// `SigningProfileVersionArn` populated by code signing.
    #[serde(default)]
    pub signing_profile_version_arn: Option<String>,
    /// `SigningJobArn` populated by code signing.
    #[serde(default)]
    pub signing_job_arn: Option<String>,
    /// `RuntimeVersionConfig` (`RuntimeVersionArn`).
    #[serde(default)]
    pub runtime_version_config: Option<serde_json::Value>,
    /// `MasterArn` — only set on numbered versions; points at the parent
    /// `$LATEST` ARN.
    #[serde(default)]
    pub master_arn: Option<String>,
    /// Free-form `StateReason` populated when the function is not in the
    /// happy `Active`/`Successful` path (e.g. image scan failed, KMS key
    /// disabled, code-signing rejection). `None` for normal functions.
    #[serde(default)]
    pub state_reason: Option<String>,
    /// Machine-readable `StateReasonCode` paired with `state_reason`
    /// (`Idle`, `Creating`, `Restoring`, `EniLimitExceeded`, …).
    #[serde(default)]
    pub state_reason_code: Option<String>,
    /// `DurableConfig` for AWS's durable-function feature
    /// (`RetentionPeriodInDays`, `ExecutionTimeout`). Round-tripped
    /// only — there's no execution-history backend in fakecloud.
    #[serde(default)]
    pub durable_config: Option<serde_json::Value>,
    /// Free-form `LastUpdateStatusReason` set on the most recent failed
    /// or in-progress configuration update.
    #[serde(default)]
    pub last_update_status_reason: Option<String>,
    /// Machine-readable code paired with `last_update_status_reason`
    /// (`EniLimitExceeded`, `InsufficientRolePermissions`, …).
    #[serde(default)]
    pub last_update_status_reason_code: Option<String>,
}

fn default_revision_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachedLayer {
    pub arn: String,
    #[serde(default)]
    pub code_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSourceMapping {
    pub uuid: String,
    pub function_arn: String,
    pub event_source_arn: String,
    pub batch_size: i64,
    pub enabled: bool,
    pub state: String,
    pub last_modified: DateTime<Utc>,
    /// Raw `Filters: [{Pattern: "..."}]` array as supplied via
    /// `FilterCriteria`. Each pattern is an EventBridge-style JSON
    /// pattern matched against the record body — non-matching records
    /// are dropped.
    #[serde(default)]
    pub filter_patterns: Vec<String>,
    /// Wait up to N seconds to accumulate `batch_size` records before
    /// invoking. Implemented as a deadline check inside the poller.
    #[serde(default)]
    pub maximum_batching_window_in_seconds: Option<i64>,
    /// `LATEST`, `TRIM_HORIZON`, or `AT_TIMESTAMP`. Honored on the
    /// first poll for stream sources (Kinesis, DDB Streams).
    #[serde(default)]
    pub starting_position: Option<String>,
    /// Optional epoch-second timestamp paired with
    /// `StartingPosition=AT_TIMESTAMP`.
    #[serde(default)]
    pub starting_position_timestamp: Option<f64>,
    /// Kinesis-only: number of concurrent batch invocations per shard.
    #[serde(default)]
    pub parallelization_factor: Option<i64>,
    /// `["ReportBatchItemFailures"]` to opt into partial-batch failure
    /// semantics. Empty / unset = entire batch is retried on error.
    #[serde(default)]
    pub function_response_types: Vec<String>,
    /// KMS key for encrypting the filter-criteria document at rest. AWS
    /// added this in 2024 for Kafka/Kinesis sources whose filters carry
    /// sensitive selectors.
    #[serde(default)]
    pub kms_key_arn: Option<String>,
    /// `MetricsConfig.Metrics` — set of opted-in CloudWatch metrics
    /// (`["EventCount"]`). Round-tripped only; fakecloud doesn't yet
    /// publish these metrics.
    #[serde(default)]
    pub metrics_config: Option<serde_json::Value>,
    /// `DestinationConfig` — `OnFailure.Destination` arn (and rarely
    /// `OnSuccess.Destination` for self-managed Kafka). Round-tripped.
    #[serde(default)]
    pub destination_config: Option<serde_json::Value>,
    /// `MaximumRetryAttempts` for the source. AWS uses `-1` to mean
    /// "infinite" so we keep the int rather than a bool.
    #[serde(default)]
    pub maximum_retry_attempts: Option<i64>,
    /// `MaximumRecordAgeInSeconds`. `-1` = infinite.
    #[serde(default)]
    pub maximum_record_age_in_seconds: Option<i64>,
    /// `BisectBatchOnFunctionError` — split the batch in half and retry
    /// on Lambda invoke failure (Kinesis / DDB streams only).
    #[serde(default)]
    pub bisect_batch_on_function_error: Option<bool>,
    /// `TumblingWindowInSeconds` — Kinesis-only batch aggregation window.
    #[serde(default)]
    pub tumbling_window_in_seconds: Option<i64>,
    /// `Topics` — MSK / self-managed-Kafka topic list.
    #[serde(default)]
    pub topics: Vec<String>,
    /// `Queues` — Amazon MQ broker queue list.
    #[serde(default)]
    pub queues: Vec<String>,
}

/// A recorded Lambda invocation from cross-service delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaInvocation {
    pub function_arn: String,
    pub payload: String,
    pub timestamp: DateTime<Utc>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaState {
    pub account_id: String,
    pub region: String,
    #[serde(default)]
    pub functions: BTreeMap<String, LambdaFunction>,
    #[serde(default)]
    pub event_source_mappings: BTreeMap<String, EventSourceMapping>,
    /// Recorded invocations from cross-service integrations — not persisted.
    #[serde(default, skip)]
    pub invocations: Vec<LambdaInvocation>,
    /// Per-function aliases keyed by `{function}:{alias}`.
    #[serde(default)]
    pub aliases: BTreeMap<String, FunctionAlias>,
    /// Published versions per function (function_name -> Vec<version>).
    #[serde(default)]
    pub function_versions: BTreeMap<String, Vec<String>>,
    /// Immutable per-version snapshot of the function (code + config),
    /// keyed by `function_name -> version -> LambdaFunction`. AWS makes
    /// each numbered version a frozen copy of `$LATEST` at publish time.
    #[serde(default)]
    pub function_version_snapshots: BTreeMap<String, BTreeMap<String, LambdaFunction>>,
    /// Layers keyed by name.
    #[serde(default)]
    pub layers: BTreeMap<String, Layer>,
    /// Function URL configs keyed by function name.
    #[serde(default)]
    pub function_url_configs: BTreeMap<String, FunctionUrlConfig>,
    /// Reserved concurrency configs keyed by function name.
    #[serde(default)]
    pub function_concurrency: BTreeMap<String, i64>,
    /// Provisioned concurrency configs keyed by `{function}:{qualifier}`.
    #[serde(default)]
    pub provisioned_concurrency: BTreeMap<String, ProvisionedConcurrencyConfig>,
    /// Code signing configs keyed by id.
    #[serde(default)]
    pub code_signing_configs: BTreeMap<String, CodeSigningConfig>,
    /// Function-to-code-signing-config association keyed by function name.
    #[serde(default)]
    pub function_code_signing: BTreeMap<String, String>,
    /// Event invoke configs keyed by `{function}:{qualifier}`.
    #[serde(default)]
    pub event_invoke_configs: BTreeMap<String, EventInvokeConfig>,
    /// Runtime management configs keyed by `{function}:{qualifier}`.
    #[serde(default)]
    pub runtime_management: BTreeMap<String, RuntimeManagementConfig>,
    /// Scaling configs keyed by event source mapping uuid.
    #[serde(default)]
    pub scaling_configs: BTreeMap<String, FunctionScalingConfig>,
    /// Recursion configs keyed by function name.
    #[serde(default)]
    pub recursion_configs: BTreeMap<String, String>,
    /// Account settings (single per-account record).
    #[serde(default)]
    pub account_settings: Option<AccountSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionAlias {
    pub alias_arn: String,
    pub name: String,
    pub function_version: String,
    pub description: String,
    pub revision_id: String,
    pub routing_config: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Layer {
    pub layer_name: String,
    pub layer_arn: String,
    pub versions: Vec<LayerVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerVersion {
    pub version: i64,
    pub layer_version_arn: String,
    pub description: String,
    pub created_date: DateTime<Utc>,
    pub compatible_runtimes: Vec<String>,
    pub license_info: String,
    pub policy: Option<String>,
    /// Raw ZIP bytes from `Content.ZipFile` on `PublishLayerVersion`.
    /// `None` only on legacy snapshots predating layer storage.
    #[serde(default)]
    pub code_zip: Option<Vec<u8>>,
    #[serde(default)]
    pub code_sha256: String,
    #[serde(default)]
    pub code_size: i64,
    /// `CompatibleArchitectures` declared at publish time. AWS rejects
    /// `GetFunction` if the function's architecture isn't in this set;
    /// fakecloud round-trips the field but doesn't enforce.
    #[serde(default)]
    pub compatible_architectures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionUrlConfig {
    pub function_arn: String,
    pub function_url: String,
    pub auth_type: String,
    pub cors: Option<serde_json::Value>,
    pub creation_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub invoke_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionedConcurrencyConfig {
    pub requested: i64,
    pub allocated: i64,
    pub status: String,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeSigningConfig {
    pub csc_id: String,
    pub csc_arn: String,
    pub description: String,
    pub allowed_publishers: Vec<String>,
    pub untrusted_artifact_action: String,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventInvokeConfig {
    pub function_arn: String,
    pub maximum_event_age: i64,
    pub maximum_retry_attempts: i64,
    /// `None` -> input omitted `DestinationConfig` entirely; AWS responds
    /// with `{OnSuccess:{}, OnFailure:{}}` (per `@examples` for
    /// `PutFunctionEventInvokeConfig`).
    ///
    /// `Some({})` -> caller explicitly sent `{}`; AWS echoes `{}` verbatim
    /// (round-trip semantics).
    ///
    /// `Some({...})` -> half-populated; AWS backfills the missing half as
    /// `{}` (per `@examples` for `UpdateFunctionEventInvokeConfig`).
    #[serde(default)]
    pub destination_config: Option<serde_json::Value>,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeManagementConfig {
    pub update_runtime_on: String,
    pub runtime_version_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FunctionScalingConfig {
    /// `MinExecutionEnvironments` — the minimum number of execution
    /// environments to maintain for the function. AWS's
    /// `FunctionScalingConfig` shape uses these two members (not the
    /// pre-2025 `MaximumConcurrency`).
    pub min_execution_environments: Option<i64>,
    /// `MaxExecutionEnvironments` — the upper bound on provisioned
    /// execution environments.
    pub max_execution_environments: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AccountSettings {
    pub concurrent_executions: i64,
    pub code_size_zipped: i64,
    pub code_size_unzipped: i64,
    pub total_code_size: i64,
}

impl LambdaState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            functions: BTreeMap::new(),
            event_source_mappings: BTreeMap::new(),
            invocations: Vec::new(),
            aliases: BTreeMap::new(),
            function_versions: BTreeMap::new(),
            function_version_snapshots: BTreeMap::new(),
            layers: BTreeMap::new(),
            function_url_configs: BTreeMap::new(),
            function_concurrency: BTreeMap::new(),
            provisioned_concurrency: BTreeMap::new(),
            code_signing_configs: BTreeMap::new(),
            function_code_signing: BTreeMap::new(),
            event_invoke_configs: BTreeMap::new(),
            runtime_management: BTreeMap::new(),
            scaling_configs: BTreeMap::new(),
            recursion_configs: BTreeMap::new(),
            account_settings: None,
        }
    }

    pub fn reset(&mut self) {
        self.functions.clear();
        self.event_source_mappings.clear();
        self.invocations.clear();
        self.aliases.clear();
        self.function_versions.clear();
        self.function_version_snapshots.clear();
        self.layers.clear();
        self.function_url_configs.clear();
        self.function_concurrency.clear();
        self.provisioned_concurrency.clear();
        self.code_signing_configs.clear();
        self.function_code_signing.clear();
        self.event_invoke_configs.clear();
        self.runtime_management.clear();
        self.scaling_configs.clear();
        self.recursion_configs.clear();
        self.account_settings = None;
    }
}

pub type SharedLambdaState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<LambdaState>>>;

impl fakecloud_core::multi_account::AccountState for LambdaState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

pub const LAMBDA_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Serialize, Deserialize)]
pub struct LambdaSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<LambdaState>>,
    #[serde(default)]
    pub state: Option<LambdaState>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_has_empty_collections() {
        let state = LambdaState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.functions.is_empty());
        assert!(state.event_source_mappings.is_empty());
        assert!(state.invocations.is_empty());
    }

    #[test]
    fn reset_clears_collections() {
        let mut state = LambdaState::new("123456789012", "us-east-1");
        state.invocations.push(LambdaInvocation {
            function_arn: "arn".to_string(),
            payload: "p".to_string(),
            timestamp: Utc::now(),
            source: "s".to_string(),
        });
        state.reset();
        assert!(state.invocations.is_empty());
    }
}
