use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

pub use crate::durable::DurableExecution;

use fakecloud_core::multi_account::{
    parse_regional_snapshot, AccountState, MultiRegionState, RegionalSnapshot, RegionalState,
    SplitByRegion,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// `DurableConfig` (`ExecutionTimeout`, `RetentionPeriodInDays`,
    /// `KMSKeyArn`): set on a durable function, whose invocations start
    /// durable executions (see `crate::durable`).
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
    /// `SourceAccessConfigurations` — VPC/auth config (security groups,
    /// subnets, SASL/SCRAM secrets) for Kafka/MQ/MSK sources. Round-tripped
    /// so Get/List/Update echo back what the caller supplied (1.17).
    #[serde(default)]
    pub source_access_configurations: Vec<serde_json::Value>,
    /// `SelfManagedEventSource` -- the bootstrap servers of a self-managed
    /// Kafka cluster (`{"Endpoints": {"KAFKA_BOOTSTRAP_SERVERS": [...]}}`),
    /// which has no event source ARN.
    #[serde(default)]
    pub self_managed_event_source: Option<serde_json::Value>,
    /// `SelfManagedKafkaEventSourceConfig` (consumer group id).
    #[serde(default)]
    pub self_managed_kafka_event_source_config: Option<serde_json::Value>,
    /// `DocumentDBEventSourceConfig` (database, collection, full document).
    #[serde(default)]
    pub document_db_event_source_config: Option<serde_json::Value>,
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
    /// Scaling configs keyed by function name and qualifier.
    #[serde(default)]
    pub scaling_configs: BTreeMap<String, FunctionScalingConfig>,
    /// Recursion configs keyed by function name.
    #[serde(default)]
    pub recursion_configs: BTreeMap<String, String>,
    /// Account settings (single per-account record).
    #[serde(default)]
    pub account_settings: Option<AccountSettings>,
    /// Capacity providers (Lambda Workflows, 2025-11-30 API) keyed by name.
    #[serde(default)]
    pub capacity_providers: BTreeMap<String, CapacityProvider>,
    /// Durable executions (2025-12-01 API) keyed by ARN. Callbacks live on
    /// their execution: a callback id encodes the execution ARN.
    #[serde(default)]
    pub durable_executions: BTreeMap<String, DurableExecution>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityProvider {
    pub name: String,
    pub arn: String,
    pub state: String,
    pub vpc_config: serde_json::Value,
    pub permissions_config: serde_json::Value,
    pub instance_requirements: Option<serde_json::Value>,
    pub scaling_config: Option<serde_json::Value>,
    pub kms_key_arn: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub last_modified: DateTime<Utc>,
    /// Function versions associated with the capacity provider, as
    /// `function_name:qualifier` strings. Populated implicitly when a
    /// function version references the provider; we don't yet write
    /// from `CreateFunction`, but `ListFunctionVersionsByCapacityProvider`
    /// can read whatever is staged here.
    #[serde(default)]
    pub function_versions: Vec<String>,
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
    /// Highest version number ever published, so a deleted version's number
    /// is never handed out again.
    #[serde(default)]
    pub last_version: i64,
}

impl Layer {
    pub fn new(layer_name: &str, layer_arn: String) -> Self {
        Self {
            layer_name: layer_name.to_string(),
            layer_arn,
            versions: Vec::new(),
            last_version: 0,
        }
    }

    /// Reserve the number of the next published version. Versions are
    /// numbered from 1 and never reused, even after deletions.
    pub fn next_version(&mut self) -> i64 {
        let highest = self
            .versions
            .iter()
            .map(|v| v.version)
            .max()
            .unwrap_or(0)
            .max(self.last_version);
        self.last_version = highest + 1;
        self.last_version
    }
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
    /// `RevisionId` of `policy`: minted whenever the policy changes, so
    /// `AddLayerVersionPermission` / `GetLayerVersionPolicy` report the same
    /// value until then and a stale `RevisionId` precondition is refused.
    /// `None` without a policy (or on snapshots that predate it, see
    /// [`crate::layer_access::policy_revision_id`]).
    #[serde(default)]
    pub policy_revision_id: Option<String>,
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

/// The ARN of an unqualified function, in `region`'s partition.
pub fn function_arn(region: &str, account_id: &str, function_name: &str) -> String {
    fakecloud_aws::arn::Arn::regional(
        "lambda",
        region,
        account_id,
        &format!("function:{function_name}"),
    )
    .to_string()
}

/// The ARN of a function version or alias (`function:<name>:<qualifier>`).
pub fn qualified_function_arn(
    region: &str,
    account_id: &str,
    function_name: &str,
    qualifier: &str,
) -> String {
    format!(
        "{}:{qualifier}",
        function_arn(region, account_id, function_name)
    )
}

/// The ARN of a layer (without a version).
pub fn layer_arn(region: &str, account_id: &str, layer_name: &str) -> String {
    fakecloud_aws::arn::Arn::regional("lambda", region, account_id, &format!("layer:{layer_name}"))
        .to_string()
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
            capacity_providers: BTreeMap::new(),
            durable_executions: BTreeMap::new(),
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
        self.capacity_providers.clear();
        self.durable_executions.clear();
    }
}

/// Lambda state partitioned by account and region: every function, layer,
/// event source mapping, code signing config and account setting lives in
/// exactly one (account, region), like on AWS.
pub type SharedLambdaState = Arc<RwLock<MultiRegionState<LambdaState>>>;

impl AccountState for LambdaState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

/// Where a function reference points: `(account, region, function name)`.
/// A function ARN (`arn:<p>:lambda:REGION:ACCOUNT:function:NAME[:Q]`) names
/// its own account and region; a partial ARN (`ACCOUNT:function:NAME`) its
/// account; a bare name (optionally `NAME:QUALIFIER`) neither, so the
/// caller's account and region apply. Cross-service callers (SNS, SQS
/// pollers, EventBridge, Step Functions, API Gateway, ...) resolve every
/// function through this so a target ARN reaches the region it names.
pub fn function_location<'a>(
    function_ref: &'a str,
    default_account: &'a str,
    default_region: &'a str,
) -> (&'a str, &'a str, &'a str) {
    if let Some(rest) = fakecloud_aws::arn::arn_resource(function_ref, "lambda") {
        let parts: Vec<&str> = rest.splitn(5, ':').collect();
        if parts.len() >= 4 && parts[2] == "function" && !parts[3].is_empty() {
            let region = Some(parts[0])
                .filter(|r| !r.is_empty())
                .unwrap_or(default_region);
            let account = Some(parts[1])
                .filter(|a| !a.is_empty())
                .unwrap_or(default_account);
            return (account, region, parts[3]);
        }
        return (default_account, default_region, function_ref);
    }
    let parts: Vec<&str> = function_ref.splitn(4, ':').collect();
    if parts.len() >= 3
        && parts[1] == "function"
        && !parts[0].is_empty()
        && parts[0].chars().all(|c| c.is_ascii_digit())
        && !parts[2].is_empty()
    {
        return (parts[0], default_region, parts[2]);
    }
    let name = function_ref.split(':').next().unwrap_or(function_ref);
    (default_account, default_region, name)
}

/// The `$LATEST` function a reference points at (see [`function_location`]).
pub fn find_function<'a>(
    accounts: &'a MultiRegionState<LambdaState>,
    function_ref: &str,
    default_account: &str,
    default_region: &str,
) -> Option<&'a LambdaFunction> {
    let (account, region, name) = function_location(function_ref, default_account, default_region);
    accounts.regional(account, region)?.functions.get(name)
}

/// The ZIP bytes of every layer attached to `func`, in attach order. Each
/// layer version ARN names the account and region the layer lives in;
/// unresolvable layers (an external layer fakecloud holds no copy of, or a
/// version deleted since) are skipped with a warning.
pub fn attached_layer_zips(
    accounts: &MultiRegionState<LambdaState>,
    func: &LambdaFunction,
) -> Vec<Vec<u8>> {
    func.layers
        .iter()
        .filter_map(|attached| {
            let zip = crate::layer_access::find_attached_version(accounts, &attached.arn)
                .and_then(|v| v.code_zip.clone());
            if zip.is_none() {
                tracing::warn!(
                    function = %func.function_name,
                    layer_arn = %attached.arn,
                    "attached layer not resolvable; skipping /opt mount for this layer"
                );
            }
            zip
        })
        .collect()
}

/// [`find_function`] plus its attached layers' ZIP bytes, cloned out of the
/// state so the caller can drop the lock before invoking.
pub fn resolve_invocable(
    accounts: &MultiRegionState<LambdaState>,
    function_ref: &str,
    default_account: &str,
    default_region: &str,
) -> Option<(LambdaFunction, Vec<Vec<u8>>)> {
    let func = find_function(accounts, function_ref, default_account, default_region)?.clone();
    let layers = attached_layer_zips(accounts, &func);
    Some((func, layers))
}

/// The region a Lambda ARN names, `None` for anything else.
fn lambda_arn_region(arn: &str) -> Option<&str> {
    fakecloud_aws::arn::arn_resource(arn, "lambda")?;
    fakecloud_aws::arn::region_of(arn)
}

/// The function name a `{function}` or `{function}:{qualifier}` key names.
fn key_function(key: &str) -> &str {
    key.split(':').next().unwrap_or(key)
}

impl SplitByRegion for LambdaState {
    /// Every function goes to the region its ARN names; the records keyed by
    /// function (versions, aliases, URL configs, concurrency, event invoke /
    /// runtime / scaling / recursion / code signing settings) follow their
    /// function. Event source mappings follow their function ARN, layers,
    /// code signing configs, capacity providers and durable executions their
    /// own ARN. Anything naming no region (and the
    /// account settings) lands in the server's default region.
    fn split_by_region(self, into: &mut RegionalState<Self>) {
        let default_region = into.default_region().to_string();
        let mut function_regions: BTreeMap<String, String> = BTreeMap::new();
        for (name, func) in self.functions {
            let region = lambda_arn_region(&func.function_arn)
                .unwrap_or(&default_region)
                .to_string();
            function_regions.insert(name.clone(), region.clone());
            into.region_mut(&region).functions.insert(name, func);
        }
        let region_of_function = |key: &str| -> String {
            function_regions
                .get(key_function(key))
                .cloned()
                .unwrap_or_else(|| default_region.clone())
        };
        macro_rules! follow_function {
            ($field:ident) => {
                for (key, value) in self.$field {
                    let region = region_of_function(&key);
                    into.region_mut(&region).$field.insert(key, value);
                }
            };
        }
        follow_function!(aliases);
        follow_function!(function_versions);
        follow_function!(function_version_snapshots);
        follow_function!(function_url_configs);
        follow_function!(function_concurrency);
        follow_function!(provisioned_concurrency);
        follow_function!(function_code_signing);
        follow_function!(event_invoke_configs);
        follow_function!(runtime_management);
        follow_function!(scaling_configs);
        follow_function!(recursion_configs);

        macro_rules! by_own_arn {
            ($field:ident, $arn_field:ident) => {
                for (key, value) in self.$field {
                    let region = lambda_arn_region(&value.$arn_field)
                        .map(str::to_string)
                        .unwrap_or_else(|| default_region.clone());
                    into.region_mut(&region).$field.insert(key, value);
                }
            };
        }
        by_own_arn!(event_source_mappings, function_arn);
        by_own_arn!(layers, layer_arn);
        by_own_arn!(code_signing_configs, csc_arn);
        by_own_arn!(capacity_providers, arn);

        for (arn, exec) in self.durable_executions {
            let region = lambda_arn_region(&exec.arn)
                .or_else(|| lambda_arn_region(&exec.function_arn))
                .map(str::to_string)
                .unwrap_or_else(|| default_region.clone());
            into.region_mut(&region)
                .durable_executions
                .insert(arn, exec);
        }
        if let Some(settings) = self.account_settings {
            into.region_mut(&default_region).account_settings = Some(settings);
        }
    }
}

/// v3: state partitioned by (account, region). v2 kept one account-wide
/// state per account (every function in one map keyed by name); v1 a single
/// account's state.
pub const LAMBDA_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

/// A persisted Lambda snapshot: the (account, region)-partitioned state, or,
/// for a migrated v1 snapshot, one account's state split by region.
pub type LambdaSnapshot = RegionalSnapshot<LambdaState>;

/// Parse a persisted Lambda snapshot, migrating older schemas (v1: one
/// account's state; v2: one account-wide state per account) by splitting each
/// account by region. A snapshot newer than this build comes back with its
/// on-disk `schema_version` and no state, for the caller to refuse.
pub fn parse_lambda_snapshot(bytes: &[u8]) -> Result<LambdaSnapshot, serde_json::Error> {
    parse_regional_snapshot(
        bytes,
        LAMBDA_SNAPSHOT_SCHEMA_VERSION,
        |state: LambdaState| {
            let account_id = state.account_id.clone();
            let region = state.region.clone();
            RegionalState::from_legacy(&account_id, &region, "", state)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;

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

    fn function(name: &str, region: &str, account: &str) -> LambdaFunction {
        LambdaFunction {
            function_name: name.to_string(),
            function_arn: function_arn(region, account, name),
            ..Default::default()
        }
    }

    /// A v2 snapshot kept every function of an account in one map, so a
    /// function created in eu-west-1 sat next to the us-east-1 ones. Loading
    /// it puts each resource in the region its ARN names, with the records
    /// keyed by function following their function.
    #[test]
    fn v2_snapshot_is_split_by_region() {
        let account = "111111111111";
        let mut legacy = LambdaState::new(account, "us-east-1");
        legacy
            .functions
            .insert("east".into(), function("east", "us-east-1", account));
        legacy
            .functions
            .insert("west".into(), function("west", "eu-west-1", account));
        legacy.aliases.insert(
            "west:live".into(),
            FunctionAlias {
                alias_arn: qualified_function_arn("eu-west-1", account, "west", "live"),
                name: "live".into(),
                function_version: "1".into(),
                description: String::new(),
                revision_id: "r".into(),
                routing_config: None,
            },
        );
        legacy.function_concurrency.insert("west".into(), 5);
        legacy.function_concurrency.insert("east".into(), 7);
        legacy.event_source_mappings.insert(
            "esm-west".into(),
            EventSourceMapping {
                uuid: "esm-west".into(),
                function_arn: function_arn("eu-west-1", account, "west"),
                event_source_arn: "arn:aws:sqs:eu-west-1:111111111111:q".into(),
                batch_size: 10,
                enabled: true,
                state: "Enabled".into(),
                last_modified: Utc::now(),
                filter_patterns: Vec::new(),
                maximum_batching_window_in_seconds: None,
                starting_position: None,
                starting_position_timestamp: None,
                parallelization_factor: None,
                function_response_types: Vec::new(),
                kms_key_arn: None,
                metrics_config: None,
                destination_config: None,
                maximum_retry_attempts: None,
                maximum_record_age_in_seconds: None,
                bisect_batch_on_function_error: None,
                tumbling_window_in_seconds: None,
                topics: Vec::new(),
                queues: Vec::new(),
                source_access_configurations: Vec::new(),
                self_managed_event_source: None,
                self_managed_kafka_event_source_config: None,
                document_db_event_source_config: None,
            },
        );
        legacy.layers.insert(
            "west-layer".into(),
            Layer::new("west-layer", layer_arn("eu-west-1", account, "west-layer")),
        );
        legacy.account_settings = Some(AccountSettings::default());

        let mut accounts: MultiAccountState<LambdaState> =
            MultiAccountState::new(account, "us-east-1", "");
        *accounts.get_or_create(account) = legacy;
        let v2 = serde_json::json!({
            "schema_version": 2,
            "accounts": accounts,
        });

        let parsed = parse_lambda_snapshot(&serde_json::to_vec(&v2).unwrap()).unwrap();
        assert_eq!(parsed.schema_version, LAMBDA_SNAPSHOT_SCHEMA_VERSION);
        let accounts = parsed.accounts.expect("accounts");
        let east = accounts.regional(account, "us-east-1").expect("us-east-1");
        let west = accounts.regional(account, "eu-west-1").expect("eu-west-1");

        assert!(east.functions.contains_key("east"));
        assert!(!east.functions.contains_key("west"));
        assert_eq!(east.function_concurrency.get("east"), Some(&7));
        assert!(east.account_settings.is_some());

        assert!(west.functions.contains_key("west"));
        assert!(west.aliases.contains_key("west:live"));
        assert_eq!(west.function_concurrency.get("west"), Some(&5));
        assert!(west.event_source_mappings.contains_key("esm-west"));
        assert!(west.layers.contains_key("west-layer"));
        assert_eq!(west.region, "eu-west-1");
        assert!(west.account_settings.is_none());

        // The migrated container round-trips as the current schema.
        let current = LambdaSnapshot {
            schema_version: LAMBDA_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(accounts),
            state: None,
        };
        let reparsed = parse_lambda_snapshot(&serde_json::to_vec(&current).unwrap()).unwrap();
        let accounts = reparsed.accounts.unwrap();
        assert!(accounts
            .regional(account, "eu-west-1")
            .unwrap()
            .functions
            .contains_key("west"));
    }

    /// A v1 snapshot held one account's state; it is split by region too.
    #[test]
    fn v1_snapshot_is_split_by_region() {
        let account = "222222222222";
        let mut legacy = LambdaState::new(account, "us-east-1");
        legacy
            .functions
            .insert("west".into(), function("west", "eu-west-1", account));
        let v1 = serde_json::json!({"schema_version": 1, "state": legacy});
        let parsed = parse_lambda_snapshot(&serde_json::to_vec(&v1).unwrap()).unwrap();
        let state = parsed.state.expect("state");
        assert_eq!(state.account_id(), account);
        assert!(state
            .region("eu-west-1")
            .unwrap()
            .functions
            .contains_key("west"));
        assert!(state.region("us-east-1").is_none());
    }

    #[test]
    fn newer_snapshot_is_reported_without_state() {
        let newer = serde_json::json!({"schema_version": LAMBDA_SNAPSHOT_SCHEMA_VERSION + 1});
        let parsed = parse_lambda_snapshot(&serde_json::to_vec(&newer).unwrap()).unwrap();
        assert_eq!(parsed.schema_version, LAMBDA_SNAPSHOT_SCHEMA_VERSION + 1);
        assert!(parsed.accounts.is_none());
    }

    #[test]
    fn function_location_reads_arn_account_and_region() {
        let arn = "arn:aws:lambda:eu-west-1:111111111111:function:f:live";
        assert_eq!(
            function_location(arn, "000000000000", "us-east-1"),
            ("111111111111", "eu-west-1", "f")
        );
        assert_eq!(
            function_location("222222222222:function:f", "000000000000", "us-east-1"),
            ("222222222222", "us-east-1", "f")
        );
        assert_eq!(
            function_location("f:live", "000000000000", "us-east-1"),
            ("000000000000", "us-east-1", "f")
        );
    }
}
