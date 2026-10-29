use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use chrono::Utc;
use fakecloud_aws::arn::Arn;
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::{S3Store, SnapshotStore};
use fakecloud_s3::SharedS3State;

use crate::config::{parse_buffering_hints, validate_destination_strings, validate_file_extension};
use crate::engine::FirehoseEngine;
use crate::state::{
    DeliveryStream, EncryptionConfig, FirehoseAccounts, FirehoseSnapshot, S3Destination,
    SharedFirehoseState, FIREHOSE_SNAPSHOT_SCHEMA_VERSION,
};

/// Actions that mutate persisted Firehose state and therefore must trigger a
/// snapshot write. PutRecord/PutRecordBatch deliver to S3 (persisted by the S3
/// store) but do not change Firehose's own state, so they are excluded.
const MUTATING_ACTIONS: &[&str] = &[
    "CreateDeliveryStream",
    "DeleteDeliveryStream",
    "TagDeliveryStream",
    "UntagDeliveryStream",
    "UpdateDestination",
    "StartDeliveryStreamEncryption",
    "StopDeliveryStreamEncryption",
];

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateDeliveryStream",
    "DescribeDeliveryStream",
    "ListDeliveryStreams",
    "DeleteDeliveryStream",
    "PutRecord",
    "PutRecordBatch",
    "TagDeliveryStream",
    "UntagDeliveryStream",
    "ListTagsForDeliveryStream",
    "UpdateDestination",
    "StartDeliveryStreamEncryption",
    "StopDeliveryStreamEncryption",
];

pub struct FirehoseService {
    state: SharedFirehoseState,
    engine: Arc<FirehoseEngine>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl FirehoseService {
    /// Construct a service with its own buffering engine. Construction never
    /// starts a worker: records are acknowledged into memory and reach S3 only
    /// after [`FirehoseEngine::start`] runs on [`Self::delivery_engine`] inside a
    /// Tokio runtime, or when [`FirehoseEngine::shutdown`] performs the final
    /// drain. The server shares one engine through [`Self::with_engine`].
    pub fn new(state: SharedFirehoseState) -> Self {
        Self {
            engine: Arc::new(FirehoseEngine::new(state.clone())),
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Adopt the engine's configuration state as well as its admission buffer,
    /// so API mutations and snapshot hooks always describe the delivered stream.
    pub fn with_engine(mut self, engine: Arc<FirehoseEngine>) -> Self {
        self.state = engine.shared_state();
        self.engine = engine;
        self
    }

    pub fn delivery_engine(&self) -> Arc<FirehoseEngine> {
        self.engine.clone()
    }

    /// Give the engine its S3 destination. Delivery still needs a started
    /// engine; see [`Self::new`].
    pub fn with_s3(self, s3: SharedS3State) -> Self {
        self.engine.set_s3(s3);
        self
    }

    /// Builder form of [`Self::set_s3_store`], for the public service which is
    /// constructed after the S3 store already exists.
    pub fn with_s3_store(self, store: Arc<dyn S3Store>) -> Self {
        self.set_s3_store(store);
        self
    }

    /// Wire the durable S3 store into the shared engine after construction, so
    /// delivered objects are written through to disk.
    pub fn set_s3_store(&self, store: Arc<dyn S3Store>) {
        self.engine.set_s3_store(store);
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn shared_state(&self) -> SharedFirehoseState {
        Arc::clone(&self.state)
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_firehose_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Firehose state when invoked, or
    /// `None` in memory mode (no snapshot store). The CloudFormation provisioner
    /// mutates `state` directly and uses this to write a CFN-provisioned
    /// resource through to disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_firehose_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current Firehose state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `FirehoseService::save_snapshot` and the
/// CloudFormation provisioner's post-provision persist hook so both route
/// through the same serialize-and-write path.
pub async fn save_firehose_snapshot(
    state: &SharedFirehoseState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = FirehoseSnapshot {
        schema_version: FIREHOSE_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write firehose snapshot"),
        Err(err) => tracing::error!(%err, "firehose snapshot task panicked"),
    }
}

impl Default for FirehoseService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(FirehoseAccounts::new())))
    }
}

#[async_trait]
impl AwsService for FirehoseService {
    fn service_name(&self) -> &str {
        "firehose"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = MUTATING_ACTIONS.contains(&req.action.as_str());
        let result = match req.action.as_str() {
            "CreateDeliveryStream" => self.create_delivery_stream(&req),
            "DescribeDeliveryStream" => self.describe_delivery_stream(&req),
            "ListDeliveryStreams" => self.list_delivery_streams(&req),
            "DeleteDeliveryStream" => self.delete_delivery_stream(&req),
            "PutRecord" => self.put_record(&req),
            "PutRecordBatch" => self.put_record_batch(&req),
            "TagDeliveryStream" => self.tag_delivery_stream(&req),
            "UntagDeliveryStream" => self.untag_delivery_stream(&req),
            "ListTagsForDeliveryStream" => self.list_tags_for_delivery_stream(&req),
            "UpdateDestination" => self.update_destination(&req),
            "StartDeliveryStreamEncryption" => self.start_delivery_stream_encryption(&req),
            "StopDeliveryStreamEncryption" => self.stop_delivery_stream_encryption(&req),
            other => Err(AwsServiceError::action_not_implemented("firehose", other)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

fn missing(field: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidArgumentException",
        format!("Missing required field: {field}"),
    )
}

fn decode_record_data(value: &Value) -> Result<Bytes, AwsServiceError> {
    if value.is_null() {
        return Err(missing("Record.Data"));
    }
    let encoded = value.as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "SerializationException",
            "Record.Data must be a base64-encoded string",
        )
    })?;
    let data = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "SerializationException",
                "Record.Data must be valid base64",
            )
        })?;
    if data.len() > 1_024_000 {
        return Err(invalid_argument(
            "Record.Data exceeds 1,024,000 decoded bytes",
        ));
    }
    Ok(Bytes::from(data))
}

/// Non-S3 destination base names. Firehose names each destination's input
/// `<Base>Configuration` (create) / `<Base>Update` (update) and reports it as
/// `<Base>Description`. fakecloud models S3/ExtendedS3 field-by-field; these
/// are round-tripped as raw JSON so DescribeDeliveryStream reflects them.
const EXTRA_DESTINATION_BASES: &[&str] = &[
    "RedshiftDestination",
    "ElasticsearchDestination",
    "AmazonopensearchserviceDestination",
    "AmazonOpenSearchServerlessDestination",
    "SplunkDestination",
    "HttpEndpointDestination",
    "SnowflakeDestination",
    "IcebergDestination",
];

/// Pull any non-S3 destination config/update blocks out of `body`, keyed by
/// their `<Base>Description` field name. `suffix` is "Configuration" (create)
/// or "Update" (update). The HTTP-endpoint destination redacts its access key
/// in the description, mirroring real Firehose.
fn extract_extra_destinations(body: &Value, suffix: &str) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for base in EXTRA_DESTINATION_BASES {
        let input_key = format!("{base}{suffix}");
        if let Some(cfg) = body.get(&input_key).filter(|v| v.is_object()) {
            out.insert(format!("{base}Description"), cfg.clone());
        }
    }
    out
}

/// Validate the optional CompressionFormat member of an S3 configuration or
/// update object. Absence is allowed;
/// a present value (including null) must be one of the exact AWS enum strings.
/// Non-object containers are left to the caller's existing shape handling.
fn validate_s3_compression(config: &Value) -> Result<(), AwsServiceError> {
    let Some(format) = config.get("CompressionFormat") else {
        return Ok(());
    };
    if matches!(
        format.as_str(),
        Some("UNCOMPRESSED" | "GZIP" | "ZIP" | "Snappy" | "HADOOP_SNAPPY")
    ) {
        return Ok(());
    }
    Err(invalid_argument(
        "CompressionFormat must be one of UNCOMPRESSED, GZIP, ZIP, Snappy, HADOOP_SNAPPY",
    ))
}

/// Redshift loads from its intermediate S3 location with the COPY command,
/// which cannot read Snappy or ZIP objects; the API model documents both as
/// disallowed for `RedshiftDestinationConfiguration.S3Configuration` and
/// `RedshiftDestinationUpdate.S3Update`. (The S3 backup is never read by COPY,
/// so it accepts every format.) Run after `validate_s3_compression`.
fn validate_redshift_intermediate_compression(config: &Value) -> Result<(), AwsServiceError> {
    match config.get("CompressionFormat").and_then(Value::as_str) {
        Some(format @ ("Snappy" | "ZIP")) => Err(invalid_argument(format!(
            "CompressionFormat {format} is not supported for the intermediate Amazon S3 \
             location of an Amazon Redshift destination because the Amazon Redshift COPY \
             operation cannot read it."
        ))),
        _ => Ok(()),
    }
}

/// Validate a classic or Extended S3 destination block plus every backup
/// block stored from it. `parse_s3_destination` keeps `S3BackupConfiguration`
/// and `apply_s3_update` keeps `S3BackupUpdate`, whichever destination shape
/// carried them; an update can take either path (a full parse when the stream
/// has no S3 destination yet), so it checks both. Shared with CloudFormation
/// provisioning (`update == false`).
pub fn validate_s3_destination_compression(
    config: &Value,
    update: bool,
) -> Result<(), AwsServiceError> {
    validate_s3_compression(config)?;
    validate_s3_compression(&config["S3BackupConfiguration"])?;
    if update {
        validate_s3_compression(&config["S3BackupUpdate"])?;
    }
    Ok(())
}

/// Preflight only the S3 destination members in the Firehose input model,
/// including raw-JSON destinations, before any stream mutation. No recursive
/// JSON walk: similarly named fields in processing/HTTP options are unrelated.
/// This is the only CompressionFormat check on the create/update paths, so it
/// covers every S3 block `parse_s3_destination`/`apply_s3_update` store.
fn validate_destination_compression(body: &Value, suffix: &str) -> Result<(), AwsServiceError> {
    let update = suffix == "Update";
    for base in ["S3Destination", "ExtendedS3Destination"] {
        validate_s3_destination_compression(&body[format!("{base}{suffix}")], update)?;
    }
    let backup_member = if update {
        "S3BackupUpdate"
    } else {
        "S3BackupConfiguration"
    };
    for base in EXTRA_DESTINATION_BASES {
        let config = &body[format!("{base}{suffix}")];
        // IcebergDestinationUpdate has S3Configuration, not S3Update.
        let s3_member = if update && *base != "IcebergDestination" {
            "S3Update"
        } else {
            "S3Configuration"
        };
        validate_s3_compression(&config[s3_member])?;
        if *base == "RedshiftDestination" {
            validate_redshift_intermediate_compression(&config[s3_member])?;
            validate_s3_compression(&config[backup_member])?;
        }
    }
    Ok(())
}

pub(crate) fn parse_s3_destination(val: &Value) -> Result<Option<S3Destination>, AwsServiceError> {
    if !val.is_object() {
        return Ok(None);
    }
    let Some(role_arn) = val["RoleARN"].as_str().map(|s| s.to_string()) else {
        return Ok(None);
    };
    let Some(bucket_arn) = val["BucketARN"].as_str().map(|s| s.to_string()) else {
        return Ok(None);
    };
    let (size, interval) = parse_buffering_hints(val)?;
    validate_file_extension(val)?;
    validate_destination_strings(val)?;
    let buffering_size_mb = Some(size.unwrap_or(5));
    let buffering_interval_seconds = Some(interval.unwrap_or(300));
    Ok(Some(S3Destination {
        destination_id: format!("destinationId-{}", Uuid::new_v4()),
        role_arn,
        bucket_arn,
        prefix: val["Prefix"].as_str().map(|s| s.to_string()),
        error_output_prefix: val["ErrorOutputPrefix"].as_str().map(|s| s.to_string()),
        buffering_size_mb,
        buffering_interval_seconds,
        compression_format: val["CompressionFormat"].as_str().map(|s| s.to_string()),
        processing_configuration: val["ProcessingConfiguration"]
            .is_object()
            .then(|| val["ProcessingConfiguration"].clone()),
        data_format_conversion_configuration: val["DataFormatConversionConfiguration"]
            .is_object()
            .then(|| val["DataFormatConversionConfiguration"].clone()),
        cloudwatch_logging_options: val["CloudWatchLoggingOptions"]
            .is_object()
            .then(|| val["CloudWatchLoggingOptions"].clone()),
        custom_time_zone: val["CustomTimeZone"].as_str().map(|s| s.to_string()),
        s3_backup_mode: val["S3BackupMode"].as_str().map(|s| s.to_string()),
        file_extension: val["FileExtension"].as_str().map(|s| s.to_string()),
        dynamic_partitioning_configuration: val["DynamicPartitioningConfiguration"]
            .is_object()
            .then(|| val["DynamicPartitioningConfiguration"].clone()),
        encryption_configuration: val["EncryptionConfiguration"]
            .is_object()
            .then(|| val["EncryptionConfiguration"].clone()),
        // The `S3BackupConfiguration` create block is a full S3 destination
        // config; stored as raw JSON and echoed back as `S3BackupDescription`.
        s3_backup_description: val["S3BackupConfiguration"]
            .is_object()
            .then(|| val["S3BackupConfiguration"].clone()),
    }))
}

fn invalid_argument(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgumentException", msg)
}

/// Apply a partial `*S3DestinationUpdate` block onto an existing destination.
/// Every field in the `*Update` shape is optional, so only the members the
/// caller supplied are changed; the destination's stable `DestinationId` is
/// preserved. Buffering values are validated when present.
fn apply_s3_update(dest: &mut S3Destination, val: &Value) -> Result<(), AwsServiceError> {
    // CompressionFormat (here and in S3BackupUpdate) was already checked by
    // `validate_destination_compression` before the stream was touched.
    let (size, interval) = parse_buffering_hints(val)?;
    validate_file_extension(val)?;
    validate_destination_strings(val)?;

    if let Some(v) = val["RoleARN"].as_str() {
        dest.role_arn = v.to_string();
    }
    if let Some(v) = val["BucketARN"].as_str() {
        dest.bucket_arn = v.to_string();
    }
    if let Some(v) = val["Prefix"].as_str() {
        dest.prefix = Some(v.to_string());
    }
    if let Some(v) = val["ErrorOutputPrefix"].as_str() {
        dest.error_output_prefix = Some(v.to_string());
    }
    if size.is_some() {
        dest.buffering_size_mb = size;
    }
    if interval.is_some() {
        dest.buffering_interval_seconds = interval;
    }
    if let Some(v) = val["CompressionFormat"].as_str() {
        dest.compression_format = Some(v.to_string());
    }
    if val["ProcessingConfiguration"].is_object() {
        dest.processing_configuration = Some(val["ProcessingConfiguration"].clone());
    }
    if val["DataFormatConversionConfiguration"].is_object() {
        dest.data_format_conversion_configuration =
            Some(val["DataFormatConversionConfiguration"].clone());
    }
    if val["CloudWatchLoggingOptions"].is_object() {
        dest.cloudwatch_logging_options = Some(val["CloudWatchLoggingOptions"].clone());
    }
    if let Some(v) = val["CustomTimeZone"].as_str() {
        dest.custom_time_zone = Some(v.to_string());
    }
    if let Some(v) = val["S3BackupMode"].as_str() {
        dest.s3_backup_mode = Some(v.to_string());
    }
    if let Some(v) = val["FileExtension"].as_str() {
        dest.file_extension = Some(v.to_string());
    }
    if val["DynamicPartitioningConfiguration"].is_object() {
        dest.dynamic_partitioning_configuration =
            Some(val["DynamicPartitioningConfiguration"].clone());
    }
    if val["EncryptionConfiguration"].is_object() {
        dest.encryption_configuration = Some(val["EncryptionConfiguration"].clone());
    }
    // `S3BackupUpdate` is an all-optional S3 destination update; merge it field-
    // wise onto the stored backup description rather than replacing it.
    if val["S3BackupUpdate"].is_object() {
        match dest.s3_backup_description.as_mut() {
            Some(existing) => merge_json(existing, &val["S3BackupUpdate"]),
            None => dest.s3_backup_description = Some(val["S3BackupUpdate"].clone()),
        }
    }
    Ok(())
}

/// Recursively merge `update` into `target`: for every key present in `update`,
/// objects recurse and all other values overwrite; keys absent from `update`
/// are left untouched. Firehose's `*Update` shapes are all-optional with merge
/// semantics, so a partial update must not drop the fields the caller omitted.
fn merge_json(target: &mut Value, update: &Value) {
    match (target, update) {
        (Value::Object(tgt), Value::Object(upd)) => {
            for (k, v) in upd {
                merge_json(tgt.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (tgt, upd) => *tgt = upd.clone(),
    }
}

/// Increment a Firehose delivery-stream version id. AWS version ids are
/// numeric strings that advance by one on every successful configuration
/// change (UpdateDestination, Start/StopEncryption, ...).
fn next_version_id(current: &str) -> String {
    current
        .parse::<u64>()
        .map(|n| (n + 1).to_string())
        .unwrap_or_else(|_| current.to_string())
}

/// Validate a delivery-stream name against the Firehose model constraints:
/// length 1..=64 and pattern `^[a-zA-Z0-9_.-]+$`. AWS rejects violations with
/// an InvalidArgumentException before any resource lookup.
fn validate_stream_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 64 {
        return Err(invalid_argument(format!(
            "DeliveryStreamName must be between 1 and 64 characters, got {}",
            name.len()
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    {
        return Err(invalid_argument(
            "DeliveryStreamName may only contain letters, digits, '_', '.' and '-'",
        ));
    }
    Ok(())
}

const DELIVERY_STREAM_TYPES: &[&str] = &[
    "DirectPut",
    "KinesisStreamAsSource",
    "MSKAsSource",
    "DatabaseAsSource",
];

fn s3_destination_json(dest: &S3Destination) -> Value {
    let buf = json!({
        "SizeInMBs": dest.buffering_size_mb.unwrap_or(5),
        "IntervalInSeconds": dest.buffering_interval_seconds.unwrap_or(300),
    });
    let mut s3 = json!({
        "RoleARN": dest.role_arn,
        "BucketARN": dest.bucket_arn,
        "BufferingHints": buf,
        "CompressionFormat": dest.compression_format.clone().unwrap_or_else(|| "UNCOMPRESSED".to_string()),
    });
    if let Some(ref p) = dest.prefix {
        s3["Prefix"] = json!(p);
    }
    if let Some(ref p) = dest.error_output_prefix {
        s3["ErrorOutputPrefix"] = json!(p);
    }
    // ProcessingConfiguration / DataFormatConversionConfiguration are only
    // valid on the ExtendedS3 shape, not the legacy S3DestinationDescription.
    let mut extended = s3.clone();
    if let Some(ref pc) = dest.processing_configuration {
        extended["ProcessingConfiguration"] = pc.clone();
    }
    if let Some(ref dfc) = dest.data_format_conversion_configuration {
        extended["DataFormatConversionConfiguration"] = dfc.clone();
    }
    // AWS always reports these on the ExtendedS3 description, defaulting them
    // when the caller omits the values. The Terraform resource reads the
    // CloudWatch logging block, custom_time_zone, and s3_backup_mode
    // unconditionally; an empty default block leaves them "not found" / "".
    extended["CloudWatchLoggingOptions"] = dest
        .cloudwatch_logging_options
        .clone()
        .unwrap_or_else(|| json!({ "Enabled": false }));
    extended["CustomTimeZone"] = json!(dest.custom_time_zone.as_deref().unwrap_or("UTC"));
    extended["S3BackupMode"] = json!(dest.s3_backup_mode.as_deref().unwrap_or("Disabled"));
    if let Some(ref ext) = dest.file_extension {
        extended["FileExtension"] = json!(ext);
    }
    // ExtendedS3-only descriptions: dynamic partitioning, delivered-object SSE
    // config, and the nested S3 backup destination description.
    if let Some(ref dp) = dest.dynamic_partitioning_configuration {
        extended["DynamicPartitioningConfiguration"] = dp.clone();
    }
    if let Some(ref enc) = dest.encryption_configuration {
        extended["EncryptionConfiguration"] = enc.clone();
    }
    if let Some(ref backup) = dest.s3_backup_description {
        extended["S3BackupDescription"] = backup.clone();
    }
    json!({
        "DestinationId": dest.destination_id,
        "S3DestinationDescription": s3,
        "ExtendedS3DestinationDescription": extended,
    })
}

/// Parse a `DeliveryStreamEncryptionConfigurationInput` (shared by
/// CreateDeliveryStream and StartDeliveryStreamEncryption) into the ENABLED
/// SSE config it requests. `KeyType` defaults to `AWS_OWNED_CMK`; a
/// `CUSTOMER_MANAGED_CMK` requires `KeyARN`.
fn parse_encryption_input(config: &Value) -> Result<EncryptionConfig, AwsServiceError> {
    let key_type = config["KeyType"]
        .as_str()
        .unwrap_or("AWS_OWNED_CMK")
        .to_string();
    if key_type != "AWS_OWNED_CMK" && key_type != "CUSTOMER_MANAGED_CMK" {
        return Err(invalid_argument(format!(
            "KeyType must be AWS_OWNED_CMK or CUSTOMER_MANAGED_CMK, got {key_type}"
        )));
    }
    let key_arn = if key_type == "CUSTOMER_MANAGED_CMK" {
        let arn = config["KeyARN"].as_str().ok_or_else(|| {
            invalid_argument("KeyARN is required when KeyType is CUSTOMER_MANAGED_CMK")
        })?;
        Some(arn.to_string())
    } else {
        None
    };
    Ok(EncryptionConfig {
        status: "ENABLED".to_string(),
        key_type: Some(key_type),
        key_arn,
    })
}

fn encryption_config_json(enc: &EncryptionConfig) -> Value {
    let mut v = json!({ "Status": enc.status });
    if let Some(kt) = &enc.key_type {
        v["KeyType"] = json!(kt);
    }
    if let Some(ka) = &enc.key_arn {
        v["KeyARN"] = json!(ka);
    }
    v
}

fn arn_for(region: &str, account: &str, name: &str) -> String {
    Arn::regional(
        "firehose",
        region,
        account,
        &format!("deliverystream/{name}"),
    )
    .to_string()
}

impl FirehoseService {
    fn create_delivery_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?
            .to_string();
        validate_stream_name(&name)?;
        let stream_type = body["DeliveryStreamType"]
            .as_str()
            .unwrap_or("DirectPut")
            .to_string();
        if !DELIVERY_STREAM_TYPES.contains(&stream_type.as_str()) {
            return Err(invalid_argument(format!(
                "Invalid DeliveryStreamType: {stream_type}"
            )));
        }

        validate_destination_compression(&body, "Configuration")?;

        let s3_dest = match parse_s3_destination(&body["S3DestinationConfiguration"])? {
            Some(d) => Some(d),
            None => parse_s3_destination(&body["ExtendedS3DestinationConfiguration"])?,
        };
        let extra_destinations = extract_extra_destinations(&body, "Configuration");

        // Create-time Tags must be returned by ListTagsForDeliveryStream without
        // a follow-up TagDeliveryStream call (bug-hunt).
        let mut create_tags = BTreeMap::new();
        if let Some(arr) = body["Tags"].as_array() {
            for t in arr {
                if let (Some(k), Some(v)) = (t["Key"].as_str(), t["Value"].as_str()) {
                    create_tags.insert(k.to_string(), v.to_string());
                }
            }
        }

        // Create-time SSE: same semantics as a follow-up
        // StartDeliveryStreamEncryption.
        let encryption = match &body["DeliveryStreamEncryptionConfigurationInput"] {
            Value::Null => None,
            config => Some(parse_encryption_input(config)?),
        };

        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let streams = state.streams_mut(&req.region);
        if streams.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceInUseException",
                format!("DeliveryStream {name} already exists"),
            ));
        }
        let arn = arn_for(&req.region, &req.account_id, &name);
        let stream = DeliveryStream {
            instance_id: Uuid::new_v4(),
            name: name.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            stream_type,
            created_at: now,
            last_update: now,
            version_id: "1".to_string(),
            destination: s3_dest,
            tags: create_tags,
            encryption,
            extra_destinations,
        };
        streams.insert(name, stream);
        Ok(AwsResponse::ok_json(json!({
            "DeliveryStreamARN": arn,
        })))
    }

    fn describe_delivery_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| not_found(name))?;
        let stream = state
            .streams(&req.region)
            .and_then(|s| s.get(name))
            .ok_or_else(|| not_found(name))?;

        // A delivery stream reports a single destination object carrying every
        // configured destination description. Start from the S3 description (if
        // any) and fold in the non-S3 descriptions so a Redshift/OpenSearch/
        // Splunk/HTTP destination round-trips instead of vanishing.
        let destinations: Vec<Value> =
            if stream.destination.is_none() && stream.extra_destinations.is_empty() {
                Vec::new()
            } else {
                let mut dest = stream
                    .destination
                    .as_ref()
                    .map(s3_destination_json)
                    .unwrap_or_else(|| json!({ "DestinationId": "destinationId-000000000001" }));
                for (key, value) in &stream.extra_destinations {
                    dest[key] = value.clone();
                }
                vec![dest]
            };

        let mut description = json!({
            "DeliveryStreamName": stream.name,
            "DeliveryStreamARN": stream.arn,
            "DeliveryStreamStatus": stream.status,
            "DeliveryStreamType": stream.stream_type,
            "VersionId": stream.version_id,
            "CreateTimestamp": stream.created_at.timestamp() as f64,
            "LastUpdateTimestamp": stream.last_update.timestamp() as f64,
            "Destinations": destinations,
            "HasMoreDestinations": false,
        });
        if let Some(enc) = &stream.encryption {
            description["DeliveryStreamEncryptionConfiguration"] = encryption_config_json(enc);
        }

        Ok(AwsResponse::ok_json(json!({
            "DeliveryStreamDescription": description,
        })))
    }

    fn list_delivery_streams(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let limit = match body["Limit"].as_i64() {
            Some(l) => {
                if !(1..=10000).contains(&l) {
                    return Err(invalid_argument(format!(
                        "Limit must be between 1 and 10000, got {l}"
                    )));
                }
                l as usize
            }
            None => 10,
        };
        let exclusive_start = body["ExclusiveStartDeliveryStreamName"]
            .as_str()
            .map(|s| s.to_string());
        if let Some(ref start) = exclusive_start {
            validate_stream_name(start)?;
        }
        let type_filter = body["DeliveryStreamType"].as_str().map(|s| s.to_string());
        if let Some(ref t) = type_filter {
            if !DELIVERY_STREAM_TYPES.contains(&t.as_str()) {
                return Err(invalid_argument(format!("Invalid DeliveryStreamType: {t}")));
            }
        }

        let accounts = self.state.read();
        let names: Vec<String> = accounts
            .get(&req.account_id)
            .and_then(|s| s.streams(&req.region))
            .map(|streams| {
                streams
                    .iter()
                    .filter(|(n, stream)| {
                        if let Some(ref start) = exclusive_start {
                            if n.as_str() <= start.as_str() {
                                return false;
                            }
                        }
                        if let Some(ref t) = type_filter {
                            return &stream.stream_type == t;
                        }
                        true
                    })
                    .map(|(n, _)| n.clone())
                    .collect()
            })
            .unwrap_or_default();
        let truncated = names.len() > limit;
        let names: Vec<String> = names.into_iter().take(limit).collect();
        Ok(AwsResponse::ok_json(json!({
            "DeliveryStreamNames": names,
            "HasMoreDeliveryStreams": truncated,
        })))
    }

    fn delete_delivery_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        validate_stream_name(name)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        if state.streams_mut(&req.region).remove(name).is_none() {
            return Err(not_found(name));
        }
        drop(accounts);
        self.engine.prune_throughput();
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn put_record(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?
            .to_string();
        let data = decode_record_data(&body["Record"]["Data"])?;
        let record_id = format!("{}", Uuid::new_v4());
        let accepted = self
            .engine
            .enqueue_put(&req.account_id, &req.region, &name, vec![data])?;
        if !accepted[0] {
            return Err(crate::engine::throughput_error());
        }
        Ok(AwsResponse::ok_json(json!({
            "RecordId": record_id,
            "Encrypted": false,
        })))
    }

    fn put_record_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?
            .to_string();
        let records = body["Records"]
            .as_array()
            .ok_or_else(|| missing("Records"))?;
        if !(1..=500).contains(&records.len()) {
            return Err(invalid_argument(
                "Records must contain between 1 and 500 entries",
            ));
        }
        let mut datas = Vec::with_capacity(records.len());
        let mut decoded_bytes = 0;
        for r in records {
            let data = decode_record_data(&r["Data"])?;
            decoded_bytes += data.len();
            if decoded_bytes > 4_194_304 {
                return Err(invalid_argument("Records exceed 4,194,304 decoded bytes"));
            }
            datas.push(data);
        }
        // One handoff after all decoding/limits: request-wide rejection cannot
        // partially admit data. Only throughput refusal can fail individual entries.
        let accepted = self
            .engine
            .enqueue_put(&req.account_id, &req.region, &name, datas)?;
        let failed = accepted.iter().filter(|admitted| !**admitted).count();
        let response_records: Vec<_> = accepted
            .into_iter()
            .map(|admitted| {
                if admitted {
                    json!({"RecordId": Uuid::new_v4().to_string()})
                } else {
                    json!({
                        "ErrorCode": crate::throughput::THROUGHPUT_ERROR_CODE,
                        "ErrorMessage": crate::throughput::THROUGHPUT_ERROR_MESSAGE,
                    })
                }
            })
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "FailedPutCount": failed,
            "Encrypted": false,
            "RequestResponses": response_records,
        })))
    }

    fn tag_delivery_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;
        if let Some(arr) = body["Tags"].as_array() {
            for t in arr {
                if let (Some(k), Some(v)) = (t["Key"].as_str(), t["Value"].as_str()) {
                    stream.tags.insert(k.to_string(), v.to_string());
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_delivery_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;
        if let Some(arr) = body["TagKeys"].as_array() {
            for k in arr {
                if let Some(s) = k.as_str() {
                    stream.tags.remove(s);
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_delivery_stream(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        // AWS: Limit @range(1,50) caps the page (default 50);
        // ExclusiveStartTagKey resumes after that key. Tags are ordered by
        // key (BTreeMap iteration). Without this, large tag sets were
        // returned whole with HasMoreTags hardcoded to false (bug-hunt
        // 2026-07-16).
        let limit = match body["Limit"].as_i64() {
            Some(l) => {
                if !(1..=50).contains(&l) {
                    return Err(invalid_argument(format!(
                        "Limit must be between 1 and 50, got {l}"
                    )));
                }
                l as usize
            }
            None => 50,
        };
        let exclusive_start = body["ExclusiveStartTagKey"].as_str();
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| not_found(name))?;
        let stream = state
            .streams(&req.region)
            .and_then(|s| s.get(name))
            .ok_or_else(|| not_found(name))?;
        let remaining: Vec<(&String, &String)> = stream
            .tags
            .iter()
            .filter(|(k, _)| exclusive_start.is_none_or(|start| k.as_str() > start))
            .collect();
        let has_more = remaining.len() > limit;
        let tags: Vec<Value> = remaining
            .into_iter()
            .take(limit)
            .map(|(k, v)| json!({"Key": k, "Value": v}))
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "Tags": tags,
            "HasMoreTags": has_more,
        })))
    }

    fn update_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        // CurrentDeliveryStreamVersionId and DestinationId are both REQUIRED
        // by AWS UpdateDestination.
        let current_version = body["CurrentDeliveryStreamVersionId"]
            .as_str()
            .ok_or_else(|| missing("CurrentDeliveryStreamVersionId"))?;
        let destination_id = body["DestinationId"]
            .as_str()
            .ok_or_else(|| missing("DestinationId"))?;
        validate_destination_compression(&body, "Update")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;

        // AWS requires CurrentDeliveryStreamVersionId to match the stream's
        // current version and rejects a mismatch with a concurrent-modification
        // error (this is the optimistic-concurrency guard).
        if current_version != stream.version_id {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ConcurrentModificationException",
                format!(
                    "The current delivery stream version {} does not match \
                     the supplied version {current_version}.",
                    stream.version_id
                ),
            ));
        }

        // DestinationId must identify the stream's current destination.
        let current_dest_id = stream
            .destination
            .as_ref()
            .map(|d| d.destination_id.clone())
            .unwrap_or_else(|| "destinationId-000000000001".to_string());
        if destination_id != current_dest_id {
            return Err(invalid_argument(format!(
                "DestinationId {destination_id} does not match the current \
                 destination {current_dest_id} for delivery stream {name}."
            )));
        }

        // Apply the S3 / ExtendedS3 update. The `*Update` shapes make every
        // member optional, so a partial update (e.g. only BufferingHints) must
        // merge onto the existing destination rather than being dropped, and
        // the destination keeps its stable DestinationId across updates.
        let s3_update = if body["S3DestinationUpdate"].is_object() {
            Some(&body["S3DestinationUpdate"])
        } else if body["ExtendedS3DestinationUpdate"].is_object() {
            Some(&body["ExtendedS3DestinationUpdate"])
        } else {
            None
        };
        if let Some(update) = s3_update {
            match stream.destination.as_mut() {
                Some(existing) => apply_s3_update(existing, update)?,
                // No existing S3 destination: fall back to a full parse
                // (RoleARN + BucketARN required to materialize one).
                None => {
                    if let Some(d) = parse_s3_destination(update)? {
                        stream.destination = Some(d);
                    }
                }
            }
        }

        // Non-S3 destinations (Redshift/OpenSearch/Splunk/HTTP/...) carry
        // all-optional `*Update` shapes. Merge each supplied update field-wise
        // onto the stored `*DestinationDescription` instead of replacing the
        // whole object, so a partial update (e.g. only RetryOptions) does not
        // drop the other fields the caller omitted.
        for (key, value) in extract_extra_destinations(&body, "Update") {
            match stream.extra_destinations.get_mut(&key) {
                Some(existing) => merge_json(existing, &value),
                None => {
                    stream.extra_destinations.insert(key, value);
                }
            }
        }

        // A successful update advances the stream version.
        stream.version_id = next_version_id(&stream.version_id);
        stream.last_update = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn start_delivery_stream_encryption(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;

        let encryption =
            parse_encryption_input(&body["DeliveryStreamEncryptionConfigurationInput"])?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;
        stream.encryption = Some(encryption);
        stream.last_update = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn stop_delivery_stream_encryption(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;
        stream.encryption = Some(EncryptionConfig {
            status: "DISABLED".to_string(),
            key_type: None,
            key_arn: None,
        });
        stream.last_update = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    /// Internal decoded-record handoff, unmetered like the Logs/SES adapters.
    /// Public Direct PUT APIs use the engine's quota-aware admission instead.
    pub fn deliver_records(
        &self,
        account_id: &str,
        region: &str,
        stream_name: &str,
        datas: Vec<Vec<u8>>,
    ) -> Result<(), AwsServiceError> {
        self.engine.enqueue(
            account_id,
            region,
            stream_name,
            datas.into_iter().map(Bytes::from).collect(),
        )
    }
}

fn not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("DeliveryStream {name} not found"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::buffering_policy;
    use bytes::Bytes;
    use http::{HeaderMap, Method};

    #[test]
    fn unsupported_s3_admission_rejects_before_backend_or_empty_payload() {
        for setting in [
            json!({"CompressionFormat": "ZIP"}),
            json!({"CompressionFormat": "HADOOP_SNAPPY"}),
            json!({"Prefix": "!{timestamp:yyyy}/"}),
            json!({"CustomTimeZone": "America/New_York"}),
            json!({"ProcessingConfiguration": {"Enabled": true}}),
            json!({"ProcessingConfiguration": {"Enabled": true, "Processors": [{"Type": "Lambda"}, {"Type": "AppendDelimiterToRecord"}]}}),
            json!({"ProcessingConfiguration": {"Enabled": true, "Processors": [{"Type": "Decompression"}]}}),
            json!({"ProcessingConfiguration": {"Enabled": true, "Processors": [{"Type": "RecordDeAggregation"}]}}),
            json!({"ProcessingConfiguration": {"Enabled": true, "Processors": [{"Type": "CloudWatchLogProcessing"}]}}),
            json!({"ProcessingConfiguration": {"Enabled": true, "Processors": [{"Type": "MetadataExtraction"}]}}),
            json!({"DataFormatConversionConfiguration": {"Enabled": true}}),
            json!({"DataFormatConversionConfiguration": {"SchemaConfiguration": {}}}),
            json!({"DynamicPartitioningConfiguration": {"Enabled": true}}),
            json!({"S3BackupMode": "Enabled"}),
        ] {
            for (backend, bucket_arn) in [
                (false, "arn:aws:s3:::admission"),
                (true, "arn:aws:s3:::admission"),
                (true, "arn:aws:s3:::missing"),
                (true, "malformed"),
            ] {
                let (mut svc, _s3_state, disk, _tmp) = firehose_with_disk_s3("admission");
                if !backend {
                    svc = FirehoseService::new(svc.shared_state());
                }
                let mut dest = json!({"RoleARN": "role", "BucketARN": bucket_arn});
                dest.as_object_mut()
                    .unwrap()
                    .extend(setting.as_object().unwrap().clone());
                svc.create_delivery_stream(&request("CreateDeliveryStream", json!({
                    "DeliveryStreamName": "unsupported", "ExtendedS3DestinationConfiguration": dest,
                }))).unwrap();
                let field = setting.as_object().unwrap().keys().next().unwrap();
                // Settings survive the control plane, but never earn a record ID.
                let description = describe(&svc, "unsupported");
                assert_eq!(
                    description["Destinations"][0]["ExtendedS3DestinationDescription"][field],
                    setting[field]
                );
                svc.update_destination(&request(
                    "UpdateDestination",
                    json!({
                        "DeliveryStreamName": "unsupported", "CurrentDeliveryStreamVersionId": "1",
                        "DestinationId": description["Destinations"][0]["DestinationId"],
                        "ExtendedS3DestinationUpdate": {"FileExtension": ".log"},
                    }),
                ))
                .unwrap();
                for body in [json!({"Data": "eA=="}), json!({"Data": ""})] {
                    let error = svc
                        .put_record(&request(
                            "PutRecord",
                            json!({"DeliveryStreamName": "unsupported", "Record": body}),
                        ))
                        .err()
                        .expect("unsupported put must fail");
                    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
                    assert_eq!(error.code(), "InvalidArgumentException");
                    assert!(error.message().contains(field), "{}", error.message());
                }
                for records in [
                    json!([]),
                    json!([{"Data": ""}]),
                    json!([{"Data": "eA=="}, {"Data": ""}]),
                ] {
                    let empty = records.as_array().unwrap().is_empty();
                    let error = svc
                        .put_record_batch(&request(
                            "PutRecordBatch",
                            json!({"DeliveryStreamName": "unsupported", "Records": records}),
                        ))
                        .err()
                        .expect("unsupported batch must fail");
                    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
                    assert_eq!(error.code(), "InvalidArgumentException");
                    if !empty {
                        assert!(error.message().contains(field), "{}", error.message());
                    }
                }
                assert!(disk.load().unwrap().buckets["admission"].objects.is_empty());
            }
        }
    }

    #[test]
    fn unsupported_non_s3_record_admission_names_destination() {
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "unsupported", "HttpEndpointDestinationConfiguration": {
                    "RoleARN": "role", "EndpointConfiguration": {"Url": "https://example.invalid"},
                },
            }),
        ))
        .unwrap();
        for (action, records) in [
            ("PutRecord", json!({"Record": {"Data": "eA=="}})),
            ("PutRecordBatch", json!({"Records": [{"Data": "eA=="}]})),
        ] {
            let mut body = records;
            body["DeliveryStreamName"] = json!("unsupported");
            let error = if action == "PutRecord" {
                svc.put_record(&request(action, body))
            } else {
                svc.put_record_batch(&request(action, body))
            }
            .err()
            .expect("non-S3 delivery is not implemented");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.code(), "InvalidArgumentException");
            assert!(error
                .message()
                .contains("HttpEndpointDestinationConfiguration"));
        }
    }

    #[tokio::test]
    async fn disabled_delivery_features_keep_verbatim_supported_output() {
        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("admission");
        svc.create_delivery_stream(&request("CreateDeliveryStream", json!({
            "DeliveryStreamName": "supported", "ExtendedS3DestinationConfiguration": {
                "RoleARN": "role", "BucketARN": "arn:aws:s3:::admission",
                "CompressionFormat": "UNCOMPRESSED", "CustomTimeZone": "UTC", "Prefix": "static/",
                "ProcessingConfiguration": {"Enabled": false, "Processors": [{"Type": "Lambda"}]},
                "DataFormatConversionConfiguration": {"Enabled": false},
                "DynamicPartitioningConfiguration": {"Enabled": false},
                "EncryptionConfiguration": {"NoEncryptionConfig": "NoEncryption"},
                "S3BackupMode": "Disabled",
            },
        }))).unwrap();
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "supported",
            vec![b"abc".to_vec(), b"def".to_vec()],
        )
        .unwrap();
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        assert_eq!(
            delivered_bodies(&s3_state, &disk, "admission"),
            vec![b"abcdef".to_vec()]
        );
    }

    #[test]
    fn mixed_destination_configuration_does_not_bypass_admission() {
        for add_s3_on_update in [false, true] {
            let (svc, _s3_state, disk, _tmp) = firehose_with_disk_s3("admission");
            let s3 = json!({"RoleARN": "role", "BucketARN": "arn:aws:s3:::admission"});
            let mut body = json!({
                "DeliveryStreamName": "mixed", "HttpEndpointDestinationConfiguration": {
                    "RoleARN": "role", "EndpointConfiguration": {"Url": "https://example.invalid"},
                },
            });
            if !add_s3_on_update {
                body["ExtendedS3DestinationConfiguration"] = s3.clone();
            }
            svc.create_delivery_stream(&request("CreateDeliveryStream", body))
                .unwrap();
            if add_s3_on_update {
                let description = describe(&svc, "mixed");
                svc.update_destination(&request(
                    "UpdateDestination",
                    json!({
                        "DeliveryStreamName": "mixed", "CurrentDeliveryStreamVersionId": "1",
                        "DestinationId": description["Destinations"][0]["DestinationId"], "ExtendedS3DestinationUpdate": s3,
                    }),
                ))
                .unwrap();
            }
            for result in [
                svc.put_record(&request(
                    "PutRecord",
                    json!({"DeliveryStreamName": "mixed", "Record": {"Data": "eA=="}}),
                )),
                svc.put_record_batch(&request(
                    "PutRecordBatch",
                    json!({"DeliveryStreamName": "mixed", "Records": [{"Data": "eA=="}]}),
                )),
            ] {
                let error = result
                    .err()
                    .expect("unsupported mixed destination must fail admission");
                assert_eq!(error.status(), StatusCode::BAD_REQUEST);
                assert_eq!(error.code(), "InvalidArgumentException");
                assert!(error
                    .message()
                    .contains("HttpEndpointDestinationConfiguration"));
            }
            assert!(disk.load().unwrap().buckets["admission"].objects.is_empty());
        }
    }

    fn request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "firehose".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "req-1".to_string(),
            headers: HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn service() -> FirehoseService {
        FirehoseService::default()
    }

    #[test]
    fn failed_s3_update_does_not_mutate_configuration_or_version() {
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "atomic",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket", "Prefix": "before/"
                }
            }),
        ))
        .unwrap();
        let before = describe(&svc, "atomic");
        for update in [
            json!({"Prefix": "changed/", "FileExtension": "bad"}),
            json!({"Prefix": "changed/", "BufferingHints": {"SizeInMBs": 1}}),
            json!({"Prefix": "changed/", "BufferingHints": {"SizeInMBs": "1", "IntervalInSeconds": 300}}),
            json!({"Prefix": "changed/", "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 901}}),
        ] {
            let error = svc
                .update_destination(&request(
                    "UpdateDestination",
                    json!({
                        "DeliveryStreamName": "atomic",
                        "CurrentDeliveryStreamVersionId": "1",
                        "DestinationId": before["Destinations"][0]["DestinationId"],
                        "ExtendedS3DestinationUpdate": update
                    }),
                ))
                .err()
                .expect("invalid update must fail");
            assert_eq!(error.code(), "InvalidArgumentException");
            assert_eq!(describe(&svc, "atomic"), before);
        }
        assert_eq!(
            before["Destinations"][0]["ExtendedS3DestinationDescription"]["BufferingHints"],
            json!({"SizeInMBs": 5, "IntervalInSeconds": 300})
        );
    }

    #[test]
    fn unsupported_delivery_configuration_still_round_trips() {
        let svc = service();
        let config = json!({
            "RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket",
            "CompressionFormat": "ZIP", "Prefix": "!{timestamp:yyyy}/",
            "CustomTimeZone": "America/New_York",
            "ProcessingConfiguration": {"Enabled": true},
            "DataFormatConversionConfiguration": {},
            "DynamicPartitioningConfiguration": {"Enabled": true}
        });
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({"DeliveryStreamName": "accepted", "ExtendedS3DestinationConfiguration": config}),
        ))
        .unwrap();
        let before = describe(&svc, "accepted");
        let ext = &before["Destinations"][0]["ExtendedS3DestinationDescription"];
        for (field, value) in config.as_object().unwrap() {
            assert_eq!(&ext[field], value);
        }
        svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "accepted",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": before["Destinations"][0]["DestinationId"],
                "ExtendedS3DestinationUpdate": {"CompressionFormat": "HADOOP_SNAPPY"}
            }),
        ))
        .unwrap();
        assert_eq!(
            describe(&svc, "accepted")["Destinations"][0]["ExtendedS3DestinationDescription"]
                ["CompressionFormat"],
            "HADOOP_SNAPPY"
        );
    }

    #[test]
    fn stream_creations_have_distinct_instance_identity_and_update_preserves_it() {
        let svc = service();
        for name in ["first", "second"] {
            svc.create_delivery_stream(&request(
                "CreateDeliveryStream",
                json!({
                    "DeliveryStreamName": name,
                    "S3DestinationConfiguration": {"RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket"}
                }),
            ))
            .unwrap();
        }
        let first = svc
            .state
            .read()
            .get("123456789012")
            .unwrap()
            .streams("us-east-1")
            .unwrap()["first"]
            .instance_id;
        let second = svc
            .state
            .read()
            .get("123456789012")
            .unwrap()
            .streams("us-east-1")
            .unwrap()["second"]
            .instance_id;
        assert_ne!(first, second);
        let before = describe(&svc, "first");
        svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "first",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": before["Destinations"][0]["DestinationId"],
                "S3DestinationUpdate": {"BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 30}}
            }),
        ))
        .unwrap();
        assert_eq!(
            svc.state
                .read()
                .get("123456789012")
                .unwrap()
                .streams("us-east-1")
                .unwrap()["first"]
                .instance_id,
            first
        );
        assert_eq!(describe(&svc, "first")["VersionId"], "2");
    }

    fn create(svc: &FirehoseService, name: &str) {
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({ "DeliveryStreamName": name }),
        ))
        .unwrap();
    }

    #[test]
    fn create_delivery_stream_persists_tags() {
        // Create-time Tags must surface via ListTagsForDeliveryStream without a
        // follow-up TagDeliveryStream (bug-hunt).
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "tagged-at-create",
                "Tags": [
                    {"Key": "team", "Value": "data"},
                    {"Key": "env", "Value": "prod"}
                ]
            }),
        ))
        .unwrap();

        let resp = svc
            .list_tags_for_delivery_stream(&request(
                "ListTagsForDeliveryStream",
                json!({ "DeliveryStreamName": "tagged-at-create" }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        let mut pairs: Vec<(&str, &str)> = tags
            .iter()
            .map(|t| (t["Key"].as_str().unwrap(), t["Value"].as_str().unwrap()))
            .collect();
        pairs.sort();
        assert_eq!(pairs, vec![("env", "prod"), ("team", "data")]);
    }

    fn describe_encryption(svc: &FirehoseService, name: &str) -> Value {
        let resp = svc
            .describe_delivery_stream(&request(
                "DescribeDeliveryStream",
                json!({ "DeliveryStreamName": name }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        body["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"].clone()
    }

    #[test]
    fn create_with_encryption_input_reflected_in_describe() {
        let svc = service();
        let key = "arn:aws:kms:us-east-1:123456789012:key/abcd";
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "enc-at-create",
                "DeliveryStreamEncryptionConfigurationInput": {
                    "KeyType": "CUSTOMER_MANAGED_CMK",
                    "KeyARN": key
                }
            }),
        ))
        .unwrap();
        assert_eq!(
            describe_encryption(&svc, "enc-at-create"),
            json!({"Status": "ENABLED", "KeyType": "CUSTOMER_MANAGED_CMK", "KeyARN": key})
        );

        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "enc-default",
                "DeliveryStreamEncryptionConfigurationInput": {}
            }),
        ))
        .unwrap();
        assert_eq!(
            describe_encryption(&svc, "enc-default"),
            json!({"Status": "ENABLED", "KeyType": "AWS_OWNED_CMK"})
        );

        let err = svc
            .create_delivery_stream(&request(
                "CreateDeliveryStream",
                json!({
                    "DeliveryStreamName": "enc-bad",
                    "DeliveryStreamEncryptionConfigurationInput": {"KeyType": "CUSTOMER_MANAGED_CMK"}
                }),
            ))
            .err()
            .expect("CUSTOMER_MANAGED_CMK without KeyARN is rejected");
        assert_eq!(err.code(), "InvalidArgumentException");
    }

    #[test]
    fn start_encryption_customer_managed_reflected_in_describe() {
        let svc = service();
        create(&svc, "enc-stream");
        svc.start_delivery_stream_encryption(&request(
            "StartDeliveryStreamEncryption",
            json!({
                "DeliveryStreamName": "enc-stream",
                "DeliveryStreamEncryptionConfigurationInput": {
                    "KeyType": "CUSTOMER_MANAGED_CMK",
                    "KeyARN": "arn:aws:kms:us-east-1:123456789012:key/abc"
                }
            }),
        ))
        .unwrap();

        let enc = describe_encryption(&svc, "enc-stream");
        assert_eq!(enc["Status"], "ENABLED");
        assert_eq!(enc["KeyType"], "CUSTOMER_MANAGED_CMK");
        assert_eq!(enc["KeyARN"], "arn:aws:kms:us-east-1:123456789012:key/abc");
    }

    #[test]
    fn start_encryption_defaults_to_aws_owned_cmk() {
        let svc = service();
        create(&svc, "default-enc");
        svc.start_delivery_stream_encryption(&request(
            "StartDeliveryStreamEncryption",
            json!({ "DeliveryStreamName": "default-enc" }),
        ))
        .unwrap();

        let enc = describe_encryption(&svc, "default-enc");
        assert_eq!(enc["Status"], "ENABLED");
        assert_eq!(enc["KeyType"], "AWS_OWNED_CMK");
        assert!(enc["KeyARN"].is_null());
    }

    #[test]
    fn stop_encryption_sets_disabled() {
        let svc = service();
        create(&svc, "stop-enc");
        svc.start_delivery_stream_encryption(&request(
            "StartDeliveryStreamEncryption",
            json!({ "DeliveryStreamName": "stop-enc" }),
        ))
        .unwrap();
        svc.stop_delivery_stream_encryption(&request(
            "StopDeliveryStreamEncryption",
            json!({ "DeliveryStreamName": "stop-enc" }),
        ))
        .unwrap();

        let enc = describe_encryption(&svc, "stop-enc");
        assert_eq!(enc["Status"], "DISABLED");
        assert!(enc["KeyType"].is_null());
    }

    #[test]
    fn start_encryption_unknown_stream_not_found() {
        let svc = service();
        let result = svc.start_delivery_stream_encryption(&request(
            "StartDeliveryStreamEncryption",
            json!({ "DeliveryStreamName": "ghost" }),
        ));
        let Err(err) = result else {
            panic!("expected ResourceNotFoundException");
        };
        assert!(format!("{err:?}").contains("ResourceNotFoundException"));
    }

    #[test]
    fn stop_encryption_unknown_stream_not_found() {
        let svc = service();
        let result = svc.stop_delivery_stream_encryption(&request(
            "StopDeliveryStreamEncryption",
            json!({ "DeliveryStreamName": "ghost" }),
        ));
        let Err(err) = result else {
            panic!("expected ResourceNotFoundException");
        };
        assert!(format!("{err:?}").contains("ResourceNotFoundException"));
    }

    #[test]
    fn buffering_size_within_range_ok() {
        assert!(buffering_policy(Some(64), Some(300)).is_ok());
    }

    #[test]
    fn buffering_size_too_small_rejected() {
        assert!(buffering_policy(Some(0), Some(300)).is_err());
    }

    #[test]
    fn buffering_size_too_large_rejected() {
        assert!(buffering_policy(Some(200), Some(300)).is_err());
    }

    #[test]
    fn buffering_interval_zero_ok() {
        assert!(buffering_policy(Some(5), Some(0)).is_ok());
    }

    #[test]
    fn buffering_interval_below_60_accepted() {
        assert!(buffering_policy(Some(5), Some(30)).is_ok());
    }

    #[test]
    fn buffering_interval_above_900_rejected() {
        assert!(buffering_policy(Some(5), Some(1200)).is_err());
    }

    #[test]
    fn create_with_processing_configuration_round_trips() {
        // ProcessingConfiguration (Lambda transform) on an ExtendedS3 destination
        // was dropped; it must round-trip through DescribeDeliveryStream
        // (bug-audit 2026-06-20, 1.18).
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "proc-stream",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket",
                    "ProcessingConfiguration": {
                        "Enabled": true,
                        "Processors": [{ "Type": "Lambda" }]
                    }
                }
            }),
        ))
        .unwrap();

        let resp = svc
            .describe_delivery_stream(&request(
                "DescribeDeliveryStream",
                json!({ "DeliveryStreamName": "proc-stream" }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let dests = &body["DeliveryStreamDescription"]["Destinations"];
        let ext = &dests[0]["ExtendedS3DestinationDescription"];
        assert_eq!(ext["ProcessingConfiguration"]["Enabled"], true, "{body}");
        assert_eq!(
            ext["ProcessingConfiguration"]["Processors"][0]["Type"],
            "Lambda"
        );
    }

    fn describe(svc: &FirehoseService, name: &str) -> Value {
        let resp = svc
            .describe_delivery_stream(&request(
                "DescribeDeliveryStream",
                json!({ "DeliveryStreamName": name }),
            ))
            .unwrap();
        serde_json::from_slice::<Value>(resp.body.expect_bytes()).unwrap()
            ["DeliveryStreamDescription"]
            .clone()
    }

    fn compression_s3_config() -> Value {
        json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": "arn:aws:s3:::bucket",
            "Prefix": "before/"
        })
    }

    fn compression_state(svc: &FirehoseService) -> Value {
        // Include full-precision timestamps and every stored field, not just
        // Describe's timestamps rounded to seconds.
        serde_json::to_value(&*svc.state.read()).unwrap()
    }

    #[test]
    fn compression_valid_values_and_omission_preserve_s3_semantics() {
        for base in ["S3Destination", "ExtendedS3Destination"] {
            for format in [
                None,
                Some("UNCOMPRESSED"),
                Some("GZIP"),
                Some("ZIP"),
                Some("Snappy"),
                Some("HADOOP_SNAPPY"),
            ] {
                let svc = service();
                let mut config = compression_s3_config();
                if let Some(format) = format {
                    config["CompressionFormat"] = json!(format);
                }
                let mut body = json!({"DeliveryStreamName": "compression"});
                body[format!("{base}Configuration")] = config;
                svc.create_delivery_stream(&request("CreateDeliveryStream", body))
                    .unwrap();
                let before = describe(&svc, "compression");
                assert_eq!(
                    before["Destinations"][0]["S3DestinationDescription"]["CompressionFormat"],
                    format.unwrap_or("UNCOMPRESSED")
                );

                let mut update = json!({
                    "DeliveryStreamName": "compression",
                    "CurrentDeliveryStreamVersionId": "1",
                    "DestinationId": before["Destinations"][0]["DestinationId"]
                });
                update[format!("{base}Update")] = json!({"Prefix": "after/"});
                svc.update_destination(&request("UpdateDestination", update))
                    .unwrap();
                let after = describe(&svc, "compression");
                assert_eq!(
                    after["Destinations"][0]["S3DestinationDescription"]["CompressionFormat"],
                    format.unwrap_or("UNCOMPRESSED")
                );
                assert_eq!(
                    after["Destinations"][0]["S3DestinationDescription"]["Prefix"],
                    "after/"
                );
            }
        }
    }

    #[test]
    fn compression_invalid_create_leaves_no_stream() {
        for base in ["S3Destination", "ExtendedS3Destination"] {
            for format in [
                json!("BROTLI"),
                json!("gzip"),
                json!("SNAPPY"),
                json!("snappy"),
                json!(""),
                json!(null),
                json!(7),
                json!(true),
                json!([]),
                json!({}),
            ] {
                let svc = service();
                let before = compression_state(&svc);
                let mut config = compression_s3_config();
                config["CompressionFormat"] = format.clone();
                let mut body = json!({"DeliveryStreamName": "bad-compression", "Tags": [{"Key": "env", "Value": "bad"}]});
                body[format!("{base}Configuration")] = config;
                let err = svc
                    .create_delivery_stream(&request("CreateDeliveryStream", body))
                    .err()
                    .expect("invalid compression must fail");
                assert_eq!(err.code(), "InvalidArgumentException", "{base}: {format}");
                assert_eq!(err.status(), StatusCode::BAD_REQUEST);
                assert_eq!(compression_state(&svc), before);
            }
        }
    }

    #[test]
    fn compression_invalid_update_is_atomic() {
        for base in ["S3Destination", "ExtendedS3Destination"] {
            let svc = service();
            let mut body = json!({"DeliveryStreamName": "compression"});
            body[format!("{base}Configuration")] = compression_s3_config();
            svc.create_delivery_stream(&request("CreateDeliveryStream", body))
                .unwrap();
            let description = describe(&svc, "compression");
            let before = compression_state(&svc);
            for format in [
                json!("BROTLI"),
                json!("Gzip"),
                json!(null),
                json!(42),
                json!(["GZIP"]),
            ] {
                let mut update = json!({
                    "DeliveryStreamName": "compression",
                    "CurrentDeliveryStreamVersionId": "1",
                    "DestinationId": description["Destinations"][0]["DestinationId"],
                    "HttpEndpointDestinationUpdate": {"RoleARN": "changed"}
                });
                update[format!("{base}Update")] = json!({
                    "RoleARN": "changed", "BucketARN": "arn:aws:s3:::changed",
                    "Prefix": "changed/", "BufferingHints": {"SizeInMBs": 10},
                    "CompressionFormat": format
                });
                let err = svc
                    .update_destination(&request("UpdateDestination", update))
                    .err()
                    .expect("invalid compression must fail");
                assert_eq!(err.code(), "InvalidArgumentException");
                assert_eq!(err.status(), StatusCode::BAD_REQUEST);
                assert_eq!(compression_state(&svc), before);
            }
        }
    }

    // Literal model paths: Iceberg deliberately uses S3Configuration on
    // update too. These blocks are otherwise stored/merged as raw JSON.
    const COMPRESSION_NESTED_PATHS: &[(&str, &str, &str)] = &[
        (
            "ExtendedS3Destination",
            "S3BackupConfiguration",
            "S3BackupUpdate",
        ),
        ("RedshiftDestination", "S3Configuration", "S3Update"),
        (
            "RedshiftDestination",
            "S3BackupConfiguration",
            "S3BackupUpdate",
        ),
        ("ElasticsearchDestination", "S3Configuration", "S3Update"),
        (
            "AmazonopensearchserviceDestination",
            "S3Configuration",
            "S3Update",
        ),
        (
            "AmazonOpenSearchServerlessDestination",
            "S3Configuration",
            "S3Update",
        ),
        ("SplunkDestination", "S3Configuration", "S3Update"),
        ("HttpEndpointDestination", "S3Configuration", "S3Update"),
        ("SnowflakeDestination", "S3Configuration", "S3Update"),
        ("IcebergDestination", "S3Configuration", "S3Configuration"),
    ];

    #[test]
    fn compression_nested_s3_create_is_validated_atomically() {
        for &(base, create_member, _) in COMPRESSION_NESTED_PATHS {
            let svc = service();
            let mut config = compression_s3_config();
            config[create_member] = compression_s3_config();
            let mut body = json!({"DeliveryStreamName": "nested"});
            body[format!("{base}Configuration")] = config;
            for bad in [json!("BROTLI"), json!(false)] {
                let mut invalid = body.clone();
                invalid[format!("{base}Configuration")][create_member]["CompressionFormat"] = bad;
                let before = compression_state(&svc);
                let err = svc
                    .create_delivery_stream(&request("CreateDeliveryStream", invalid))
                    .err()
                    .expect("invalid nested create must fail");
                assert_eq!(
                    err.code(),
                    "InvalidArgumentException",
                    "{base}.{create_member}"
                );
                assert_eq!(err.status(), StatusCode::BAD_REQUEST);
                assert_eq!(compression_state(&svc), before);
            }
        }
    }

    #[test]
    fn compression_nested_s3_update_is_validated_atomically() {
        for &(base, create_member, update_member) in COMPRESSION_NESTED_PATHS {
            let svc = service();
            let mut config = compression_s3_config();
            config[create_member] = compression_s3_config();
            let mut body = json!({"DeliveryStreamName": "nested"});
            body[format!("{base}Configuration")] = config;
            // HADOOP_SNAPPY is valid on every S3 member, including Redshift's
            // intermediate location (which rejects Snappy and ZIP).
            body[format!("{base}Configuration")][create_member]["CompressionFormat"] =
                json!("HADOOP_SNAPPY");
            svc.create_delivery_stream(&request("CreateDeliveryStream", body))
                .unwrap();
            let description = describe(&svc, "nested");
            let before = compression_state(&svc);
            for bad in [json!("SNAPPY"), json!(null)] {
                let mut update = json!({
                    "DeliveryStreamName": "nested",
                    "CurrentDeliveryStreamVersionId": "1",
                    "DestinationId": description["Destinations"][0]["DestinationId"],
                    "S3DestinationUpdate": {"Prefix": "must-not-change/"}
                });
                update[format!("{base}Update")] =
                    json!({"Prefix": "changed/", "RoleARN": "changed"});
                update[format!("{base}Update")][update_member] =
                    json!({"CompressionFormat": bad, "Prefix": "changed-backup/"});
                let err = svc
                    .update_destination(&request("UpdateDestination", update))
                    .err()
                    .expect("invalid nested update must fail");
                assert_eq!(
                    err.code(),
                    "InvalidArgumentException",
                    "{base}.{update_member}"
                );
                assert_eq!(err.status(), StatusCode::BAD_REQUEST);
                assert_eq!(compression_state(&svc), before);
            }
        }
    }

    fn redshift_config(intermediate: &str, backup: &str) -> Value {
        let mut config = json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "ClusterJDBCURL": "jdbc:redshift://cluster:5439/db",
            "CopyCommand": {"DataTableName": "events"},
            "Username": "user",
            "Password": "password",
            "S3Configuration": compression_s3_config(),
            "S3BackupMode": "Enabled",
            "S3BackupConfiguration": compression_s3_config()
        });
        config["S3Configuration"]["CompressionFormat"] = json!(intermediate);
        config["S3BackupConfiguration"]["CompressionFormat"] = json!(backup);
        config
    }

    #[test]
    fn redshift_intermediate_s3_rejects_snappy_and_zip_on_create() {
        for format in ["Snappy", "ZIP"] {
            let svc = service();
            let before = compression_state(&svc);
            let err = svc
                .create_delivery_stream(&request(
                    "CreateDeliveryStream",
                    json!({
                        "DeliveryStreamName": "redshift",
                        "RedshiftDestinationConfiguration": redshift_config(format, "GZIP")
                    }),
                ))
                .err()
                .expect("COPY-unreadable format must fail");
            assert_eq!(err.code(), "InvalidArgumentException", "{format}");
            assert_eq!(err.status(), StatusCode::BAD_REQUEST);
            assert!(err.message().contains(format), "{}", err.message());
            assert!(err.message().contains("Redshift"), "{}", err.message());
            assert_eq!(compression_state(&svc), before);
        }
        // COPY never reads the backup, and the other enum values are loadable.
        for (intermediate, backup) in [
            ("UNCOMPRESSED", "Snappy"),
            ("GZIP", "ZIP"),
            ("HADOOP_SNAPPY", "Snappy"),
        ] {
            let svc = service();
            svc.create_delivery_stream(&request(
                "CreateDeliveryStream",
                json!({
                    "DeliveryStreamName": "redshift",
                    "RedshiftDestinationConfiguration": redshift_config(intermediate, backup)
                }),
            ))
            .unwrap_or_else(|e| panic!("{intermediate}/{backup}: {}", e.message()));
        }
    }

    #[test]
    fn redshift_intermediate_s3_update_rejects_snappy_and_zip_atomically() {
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "redshift",
                "RedshiftDestinationConfiguration": redshift_config("GZIP", "GZIP")
            }),
        ))
        .unwrap();
        let description = describe(&svc, "redshift");
        let destination_id = description["Destinations"][0]["DestinationId"].clone();
        let before = compression_state(&svc);
        for format in ["Snappy", "ZIP"] {
            let err = svc
                .update_destination(&request(
                    "UpdateDestination",
                    json!({
                        "DeliveryStreamName": "redshift",
                        "CurrentDeliveryStreamVersionId": "1",
                        "DestinationId": destination_id,
                        "RedshiftDestinationUpdate": {
                            "Username": "changed",
                            "S3Update": {"Prefix": "changed/", "CompressionFormat": format}
                        }
                    }),
                ))
                .err()
                .expect("COPY-unreadable format must fail");
            assert_eq!(err.code(), "InvalidArgumentException", "{format}");
            assert_eq!(compression_state(&svc), before);
        }
        // The backup accepts them; the intermediate location accepts the rest.
        svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "redshift",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": destination_id,
                "RedshiftDestinationUpdate": {
                    "S3Update": {"CompressionFormat": "UNCOMPRESSED"},
                    "S3BackupUpdate": {"CompressionFormat": "ZIP"}
                }
            }),
        ))
        .unwrap();
        assert_eq!(describe(&svc, "redshift")["VersionId"], "2");
    }

    #[test]
    fn compression_validation_is_limited_to_s3_member_objects() {
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "opaque",
                "HttpEndpointDestinationConfiguration": {
                    "CompressionFormat": "BROTLI",
                    "RequestConfiguration": {"CompressionFormat": "BROTLI"},
                    "S3Configuration": [{"CompressionFormat": "BROTLI"}]
                }
            }),
        ))
        .unwrap();
        assert_eq!(
            describe(&svc, "opaque")["Destinations"][0]["HttpEndpointDestinationDescription"]
                ["RequestConfiguration"]["CompressionFormat"],
            "BROTLI"
        );
    }

    /// With no S3 destination yet, UpdateDestination fully parses the S3
    /// update, which keeps its `S3BackupConfiguration`: that block must be
    /// validated too.
    #[test]
    fn compression_update_fallback_parse_validates_backup_configuration() {
        for base in ["S3Destination", "ExtendedS3Destination"] {
            let svc = service();
            svc.create_delivery_stream(&request(
                "CreateDeliveryStream",
                json!({
                    "DeliveryStreamName": "fallback",
                    "HttpEndpointDestinationConfiguration": {
                        "EndpointConfiguration": {"Url": "https://example.com"},
                        "S3Configuration": compression_s3_config()
                    }
                }),
            ))
            .unwrap();
            let destination_id =
                describe(&svc, "fallback")["Destinations"][0]["DestinationId"].clone();
            let before = compression_state(&svc);
            let update = |backup: &str| {
                let mut config = compression_s3_config();
                config["S3BackupConfiguration"] = json!({"CompressionFormat": backup});
                let mut body = json!({
                    "DeliveryStreamName": "fallback",
                    "CurrentDeliveryStreamVersionId": "1",
                    "DestinationId": destination_id
                });
                body[format!("{base}Update")] = config;
                request("UpdateDestination", body)
            };
            let err = svc
                .update_destination(&update("BROTLI"))
                .err()
                .expect("invalid backup compression must fail");
            assert_eq!(err.code(), "InvalidArgumentException", "{base}");
            assert_eq!(compression_state(&svc), before, "{base}");
            svc.update_destination(&update("ZIP")).unwrap();
        }
    }

    #[test]
    fn update_destination_keeps_destination_id_stable_and_bumps_version() {
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "upd-stream",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket",
                    "Prefix": "before/"
                }
            }),
        ))
        .unwrap();

        let before = describe(&svc, "upd-stream");
        let original_dest_id = before["Destinations"][0]["DestinationId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(before["VersionId"], "1");

        // Partial update: only change the Prefix (no RoleARN/BucketARN).
        svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "upd-stream",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": original_dest_id,
                "ExtendedS3DestinationUpdate": { "Prefix": "after/" }
            }),
        ))
        .unwrap();

        let after = describe(&svc, "upd-stream");
        // DestinationId is stable across the update.
        assert_eq!(
            after["Destinations"][0]["DestinationId"].as_str().unwrap(),
            original_dest_id
        );
        // Partial update applied: Prefix changed, BucketARN preserved.
        assert_eq!(
            after["Destinations"][0]["ExtendedS3DestinationDescription"]["Prefix"],
            "after/"
        );
        assert_eq!(
            after["Destinations"][0]["ExtendedS3DestinationDescription"]["BucketARN"],
            "arn:aws:s3:::bucket"
        );
        // Version advanced from 1 -> 2.
        assert_eq!(after["VersionId"], "2");
    }

    #[test]
    fn update_non_s3_destination_merges_field_wise() {
        // A partial RedshiftDestinationUpdate must merge onto the stored
        // description instead of replacing it, so untouched fields survive.
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "rs-stream",
                "RedshiftDestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "ClusterJDBCURL": "jdbc:redshift://cluster:5439/db",
                    "Username": "admin",
                    "CopyCommand": { "DataTableName": "events" },
                    "RetryOptions": { "DurationInSeconds": 3600 }
                }
            }),
        ))
        .unwrap();

        let dest_id = describe(&svc, "rs-stream")["Destinations"][0]["DestinationId"]
            .as_str()
            .unwrap()
            .to_string();

        // Update only the RetryOptions sub-field.
        svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "rs-stream",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": dest_id,
                "RedshiftDestinationUpdate": {
                    "RetryOptions": { "DurationInSeconds": 7200 }
                }
            }),
        ))
        .unwrap();

        let after = describe(&svc, "rs-stream");
        let rs = &after["Destinations"][0]["RedshiftDestinationDescription"];
        // Changed field applied.
        assert_eq!(rs["RetryOptions"]["DurationInSeconds"], 7200, "{after}");
        // Untouched fields preserved (not dropped by a whole-object replace).
        assert_eq!(rs["ClusterJDBCURL"], "jdbc:redshift://cluster:5439/db");
        assert_eq!(rs["Username"], "admin");
        assert_eq!(rs["CopyCommand"]["DataTableName"], "events");
        assert_eq!(rs["RoleARN"], "arn:aws:iam::123456789012:role/fh");
    }

    #[test]
    fn extended_s3_dynamic_partition_encryption_backup_round_trip() {
        // DynamicPartitioningConfiguration, EncryptionConfiguration and the
        // nested S3 backup destination were dropped entirely; all three must
        // round-trip through DescribeDeliveryStream.
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "ext-stream",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket",
                    "DynamicPartitioningConfiguration": {
                        "Enabled": true,
                        "RetryOptions": { "DurationInSeconds": 300 }
                    },
                    "EncryptionConfiguration": {
                        "KMSEncryptionConfig": {
                            "AWSKMSKeyARN": "arn:aws:kms:us-east-1:123456789012:key/abc"
                        }
                    },
                    "S3BackupMode": "Enabled",
                    "S3BackupConfiguration": {
                        "RoleARN": "arn:aws:iam::123456789012:role/backup",
                        "BucketARN": "arn:aws:s3:::backup-bucket",
                        "Prefix": "backup/"
                    }
                }
            }),
        ))
        .unwrap();

        let desc = describe(&svc, "ext-stream");
        let ext = &desc["Destinations"][0]["ExtendedS3DestinationDescription"];
        assert_eq!(
            ext["DynamicPartitioningConfiguration"]["Enabled"], true,
            "{desc}"
        );
        assert_eq!(
            ext["EncryptionConfiguration"]["KMSEncryptionConfig"]["AWSKMSKeyARN"],
            "arn:aws:kms:us-east-1:123456789012:key/abc"
        );
        assert_eq!(ext["S3BackupMode"], "Enabled");
        assert_eq!(
            ext["S3BackupDescription"]["BucketARN"],
            "arn:aws:s3:::backup-bucket"
        );
        assert_eq!(ext["S3BackupDescription"]["Prefix"], "backup/");
    }

    fn create_ver_stream(svc: &FirehoseService) -> String {
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "ver-stream",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket"
                }
            }),
        ))
        .unwrap();
        describe(svc, "ver-stream")["Destinations"][0]["DestinationId"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn update_destination_rejects_stale_version() {
        let svc = service();
        let dest_id = create_ver_stream(&svc);

        // Wrong CurrentDeliveryStreamVersionId -> ConcurrentModificationException.
        let err = match svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "ver-stream",
                "CurrentDeliveryStreamVersionId": "99",
                "DestinationId": dest_id,
                "ExtendedS3DestinationUpdate": { "Prefix": "x/" }
            }),
        )) {
            Err(e) => e,
            Ok(_) => panic!("stale version must be rejected"),
        };
        assert_eq!(err.code(), "ConcurrentModificationException");
    }

    #[test]
    fn update_destination_requires_current_version_and_destination_id() {
        let svc = service();
        let dest_id = create_ver_stream(&svc);

        // Missing CurrentDeliveryStreamVersionId -> rejected.
        let err = match svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "ver-stream",
                "DestinationId": dest_id,
                "ExtendedS3DestinationUpdate": { "Prefix": "x/" }
            }),
        )) {
            Err(e) => e,
            Ok(_) => panic!("missing CurrentDeliveryStreamVersionId must be rejected"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);

        // Missing DestinationId -> rejected.
        let err = match svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "ver-stream",
                "CurrentDeliveryStreamVersionId": "1",
                "ExtendedS3DestinationUpdate": { "Prefix": "x/" }
            }),
        )) {
            Err(e) => e,
            Ok(_) => panic!("missing DestinationId must be rejected"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn update_destination_rejects_mismatched_destination_id() {
        let svc = service();
        create_ver_stream(&svc);

        let err = match svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "ver-stream",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": "destinationId-wrong",
                "ExtendedS3DestinationUpdate": { "Prefix": "x/" }
            }),
        )) {
            Err(e) => e,
            Ok(_) => panic!("mismatched DestinationId must be rejected"),
        };
        assert_eq!(err.code(), "InvalidArgumentException");
    }

    #[test]
    fn list_tags_for_delivery_stream_paginates() {
        // Regression: ListTagsForDeliveryStream honors Limit +
        // ExclusiveStartTagKey and sets HasMoreTags when a page is truncated,
        // instead of returning every tag with HasMoreTags hardcoded false.
        let svc = service();
        create(&svc, "tagged");
        svc.tag_delivery_stream(&request(
            "TagDeliveryStream",
            json!({
                "DeliveryStreamName": "tagged",
                "Tags": [
                    {"Key": "a", "Value": "1"},
                    {"Key": "b", "Value": "2"},
                    {"Key": "c", "Value": "3"}
                ]
            }),
        ))
        .unwrap();

        // Page 1: Limit 2 -> first two keys, more remain.
        let resp = svc
            .list_tags_for_delivery_stream(&request(
                "ListTagsForDeliveryStream",
                json!({ "DeliveryStreamName": "tagged", "Limit": 2 }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0]["Key"], "a");
        assert_eq!(tags[1]["Key"], "b");
        assert_eq!(body["HasMoreTags"], true);

        // Page 2: resume after "b".
        let resp = svc
            .list_tags_for_delivery_stream(&request(
                "ListTagsForDeliveryStream",
                json!({
                    "DeliveryStreamName": "tagged",
                    "Limit": 2,
                    "ExclusiveStartTagKey": "b"
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "c");
        assert_eq!(body["HasMoreTags"], false);
    }

    const S3_ACCOUNT: &str = "123456789012";

    fn memory_fixture() -> (FirehoseService, SharedS3State) {
        let s3: SharedS3State = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                S3_ACCOUNT,
                "us-east-1",
                "http://localhost",
            ),
        ));
        s3.write().get_or_create(S3_ACCOUNT).buckets.insert(
            "buffered".into(),
            fakecloud_s3::S3Bucket::new("buffered", "us-east-1", "owner"),
        );
        let svc = service().with_s3(s3.clone());
        create_s3_stream(&svc, "buffered", "buffered");
        (svc, s3)
    }

    fn put(svc: &FirehoseService, data: &[u8]) -> Result<AwsResponse, AwsServiceError> {
        svc.put_record(&request(
            "PutRecord",
            json!({
                "DeliveryStreamName": "buffered",
                "Record": {"Data": base64::engine::general_purpose::STANDARD.encode(data)}
            }),
        ))
    }

    fn batch(svc: &FirehoseService, records: Vec<Value>) -> Result<AwsResponse, AwsServiceError> {
        svc.put_record_batch(&request(
            "PutRecordBatch",
            json!({
                "DeliveryStreamName": "buffered", "Records": records
            }),
        ))
    }

    fn record(data: &[u8]) -> Value {
        json!({"Data": base64::engine::general_purpose::STANDARD.encode(data)})
    }

    #[test]
    fn task5_public_ack_precedes_delivery() {
        let (svc, s3) = memory_fixture();
        let response = put(&svc, b"accepted").unwrap();
        let response: Value = serde_json::from_slice(response.body.expect_bytes()).unwrap();
        assert!(response["RecordId"].as_str().is_some());
        assert!(s3.read().get(S3_ACCOUNT).unwrap().buckets["buffered"]
            .objects
            .is_empty());
    }

    #[test]
    fn task5_record_and_batch_input_limits() {
        let (svc, _) = memory_fixture();
        put(&svc, &vec![1; 1_024_000]).unwrap();
        for result in [
            put(&svc, &vec![2; 1_024_001]),
            batch(&svc, vec![]),
            batch(&svc, vec![record(b"x"); 501]),
            batch(&svc, vec![record(b"before"), record(&vec![2; 1_024_001])]),
            batch(&svc, vec![record(&vec![3; 1_000_000]); 5]),
        ] {
            let error = result
                .err()
                .expect("over-limit request must fail before admission");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.code(), "InvalidArgumentException");
        }
        batch(&svc, vec![record(b"x"); 500]).unwrap();
    }

    #[test]
    fn malformed_batch_is_request_wide_even_after_valid_or_empty_records() {
        let (svc, _) = memory_fixture();
        for (malformed, expected_code) in [
            (json!({"Data": "%%%"}), "SerializationException"),
            (json!({}), "InvalidArgumentException"),
            (json!({"Data": 1}), "SerializationException"),
        ] {
            let error = batch(&svc, vec![record(b""), record(b"valid"), malformed])
                .err()
                .unwrap();
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.code(), expected_code);
        }
        let error = svc
            .put_record_batch(&request(
                "PutRecordBatch",
                json!({
                    "DeliveryStreamName": "absent", "Records": [{"Data": "%%%"}]
                }),
            ))
            .err()
            .unwrap();
        assert_eq!(error.code(), "SerializationException");
    }

    #[tokio::test]
    async fn task5_unsupported_delivery_rejects_before_ack() {
        for setting in [
            json!({"CompressionFormat": "ZIP"}),
            json!({"CompressionFormat": "HADOOP_SNAPPY"}),
            json!({"Prefix": "!{timestamp:yyyy}/"}),
            json!({"CustomTimeZone": "America/New_York"}),
            json!({"ProcessingConfiguration": {"Enabled": true}}),
            json!({"ProcessingConfiguration": {"Enabled": true, "Processors": [{"Type": "Lambda"}, {"Type": "AppendDelimiterToRecord"}]}}),
            json!({"DataFormatConversionConfiguration": {}}),
            json!({"DataFormatConversionConfiguration": {"Enabled": true}}),
            json!({"DynamicPartitioningConfiguration": {"Enabled": true}}),
        ] {
            let svc = service();
            let mut config = json!({"RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket"});
            config
                .as_object_mut()
                .unwrap()
                .extend(setting.as_object().unwrap().clone());
            svc.create_delivery_stream(&request(
                "CreateDeliveryStream",
                json!({
                    "DeliveryStreamName": "buffered", "ExtendedS3DestinationConfiguration": config
                }),
            ))
            .unwrap();
            let description = describe(&svc, "buffered");
            for (field, value) in setting.as_object().unwrap() {
                assert_eq!(
                    &description["Destinations"][0]["ExtendedS3DestinationDescription"][field],
                    value
                );
            }
            for result in [put(&svc, b"x"), batch(&svc, vec![record(b"x")])] {
                let error = result.err().expect("unsupported delivery must not ACK");
                assert_eq!(error.status(), StatusCode::BAD_REQUEST);
                assert_eq!(error.code(), "InvalidArgumentException");
            }
            assert_eq!(
                svc.delivery_engine().shutdown().await,
                crate::DrainReport::default()
            );
        }
    }

    #[tokio::test]
    async fn public_delete_recreate_cannot_deliver_old_buffer() {
        let (svc, s3) = memory_fixture();
        put(&svc, b"old").unwrap();
        svc.delete_delivery_stream(&request(
            "DeleteDeliveryStream",
            json!({
                "DeliveryStreamName": "buffered"
            }),
        ))
        .unwrap();
        create_s3_stream(&svc, "buffered", "buffered");
        put(&svc, b"new").unwrap();
        assert_eq!(
            svc.delivery_engine().shutdown().await,
            crate::DrainReport {
                delivered_batches: 1,
                undelivered_records: 0,
                undelivered_bytes: 0,
            }
        );
        let s3 = s3.read();
        let objects = &s3.get(S3_ACCOUNT).unwrap().buckets["buffered"].objects;
        assert_eq!(objects.len(), 1);
        let fakecloud_persistence::BodyRef::Memory(body) = &objects.values().next().unwrap().body
        else {
            panic!("memory fixture")
        };
        assert_eq!(body.as_ref(), b"new");
    }

    #[tokio::test]
    async fn public_buffered_update_uses_new_destination_at_detachment() {
        use std::io::Read;
        let (svc, s3) = memory_fixture();
        put(&svc, b"before-update").unwrap();
        s3.write().get_or_create(S3_ACCOUNT).buckets.insert(
            "updated".into(),
            fakecloud_s3::S3Bucket::new("updated", "us-east-1", "owner"),
        );
        let before = describe(&svc, "buffered");
        svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "buffered", "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": before["Destinations"][0]["DestinationId"],
                "ExtendedS3DestinationUpdate": {
                    "BucketARN": "arn:aws:s3:::updated", "CompressionFormat": "GZIP",
                    "Prefix": "new/", "FileExtension": ".updated"
                }
            }),
        ))
        .unwrap();
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        let s3 = s3.read();
        assert!(s3.get(S3_ACCOUNT).unwrap().buckets["buffered"]
            .objects
            .is_empty());
        let objects = &s3.get(S3_ACCOUNT).unwrap().buckets["updated"].objects;
        assert_eq!(objects.len(), 1);
        let object = objects.values().next().unwrap();
        assert!(object.key.starts_with("new/"));
        assert!(object.key.contains("/buffered-2-"));
        assert!(object.key.ends_with(".updated"));
        assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
        let fakecloud_persistence::BodyRef::Memory(body) = &object.body else {
            panic!("memory fixture")
        };
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(body.as_ref())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, b"before-update");
    }

    async fn wait_for_object(s3: &SharedS3State) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if !s3.read().get(S3_ACCOUNT).unwrap().buckets["buffered"]
                    .objects
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary worker must publish the threshold batch");
    }

    #[tokio::test]
    async fn api_and_internal_hook_share_buffer() {
        use fakecloud_core::delivery::FirehoseDelivery;
        let (svc, s3) = memory_fixture();
        let before = describe(&svc, "buffered");
        svc.update_destination(&request("UpdateDestination", json!({
            "DeliveryStreamName": "buffered", "CurrentDeliveryStreamVersionId": "1",
            "DestinationId": before["Destinations"][0]["DestinationId"],
            "ExtendedS3DestinationUpdate": {"BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 900}}
        }))).unwrap();
        let engine = svc.delivery_engine();
        let hook = crate::FirehoseDeliveryImpl::with_engine(engine.clone());
        // Construction and admission are inert until explicitly started.
        let response = put(&svc, &vec![b'a'; 524_288]).unwrap();
        assert!(
            serde_json::from_slice::<Value>(response.body.expect_bytes()).unwrap()["RecordId"]
                .is_string()
        );
        assert!(s3.read().get(S3_ACCOUNT).unwrap().buckets["buffered"]
            .objects
            .is_empty());
        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/buffered",
            &vec![b'b'; 524_288],
        );
        assert!(s3.read().get(S3_ACCOUNT).unwrap().buckets["buffered"]
            .objects
            .is_empty());
        engine.start().unwrap();
        wait_for_object(&s3).await;
        let report = engine.shutdown().await;
        assert_eq!(report.undelivered_records, 0);
        {
            let s3 = s3.read();
            let objects = &s3.get(S3_ACCOUNT).unwrap().buckets["buffered"].objects;
            assert_eq!(
                objects.len(),
                1,
                "API and internal sources must aggregate, not use separate engines"
            );
            let object = objects.values().next().unwrap();
            assert_eq!(object.size, 1_048_576);
            let fakecloud_persistence::BodyRef::Memory(body) = &object.body else {
                panic!("memory fixture")
            };
            assert_eq!(&body[..524_288], vec![b'a'; 524_288]);
            assert_eq!(&body[524_288..], vec![b'b'; 524_288]);
        }
        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/buffered",
            b"late",
        );
        assert_eq!(
            put(&svc, b"late").err().unwrap().code(),
            "ServiceUnavailableException"
        );
        assert_eq!(engine.shutdown().await, report);
    }

    #[tokio::test]
    async fn decoded_batch_maximum_and_atomic_rejection() {
        let exact = vec![record(&vec![1; 1_024_000]); 4];
        for records in [
            {
                let mut r = exact.clone();
                r.push(record(&vec![2; 98_305]));
                r
            },
            vec![record(b"first"), record(&vec![3; 1_024_001])],
            vec![record(b"first"); 501],
            vec![],
        ] {
            let (svc, s3) = memory_fixture();
            // Previously accepted data is the only data allowed to drain after
            // the rejected call, even if earlier entries were individually valid.
            put(&svc, b"existing").unwrap();
            let error = batch(&svc, records)
                .err()
                .expect("whole request must be rejected");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.code(), "InvalidArgumentException");
            let report = svc.delivery_engine().shutdown().await;
            assert_eq!(report.delivered_batches, 1);
            assert_eq!(report.undelivered_records, 0);
            let s3 = s3.read();
            let objects = &s3.get(S3_ACCOUNT).unwrap().buckets["buffered"].objects;
            assert_eq!(objects.len(), 1);
            let fakecloud_persistence::BodyRef::Memory(body) =
                &objects.values().next().unwrap().body
            else {
                panic!("memory fixture")
            };
            assert_eq!(
                body.as_ref(),
                b"existing",
                "no partial admission from rejected request"
            );
        }
        let (svc, s3) = memory_fixture();
        let mut exact = exact;
        exact.push(record(&vec![2; 98_304]));
        let response = batch(&svc, exact).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(response.body.expect_bytes()).unwrap()
                ["FailedPutCount"],
            0
        );
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        let s3 = s3.read();
        let objects = &s3.get(S3_ACCOUNT).unwrap().buckets["buffered"].objects;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects.values().next().unwrap().size, 4_194_304);
    }

    #[tokio::test]
    async fn malformed_batch_admits_nothing_and_empty_blobs_create_no_objects() {
        let (svc, s3) = memory_fixture();
        batch(
            &svc,
            vec![
                json!({"Data": "%%%"}),
                record(b""),
                record(b"a"),
                json!({}),
                record(b"b"),
            ],
        )
        .err()
        .expect("malformed batch must reject");
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 0);
        let objects = s3.read().get(S3_ACCOUNT).unwrap().buckets["buffered"]
            .objects
            .clone();
        assert!(objects.is_empty());
        let (svc, s3) = memory_fixture();
        put(&svc, b"").unwrap();
        let response = batch(&svc, vec![record(b""); 500]).unwrap();
        let response: Value = serde_json::from_slice(response.body.expect_bytes()).unwrap();
        assert_eq!(response["FailedPutCount"], 0);
        assert_eq!(response["RequestResponses"].as_array().unwrap().len(), 500);
        assert_eq!(
            svc.delivery_engine().shutdown().await,
            crate::DrainReport::default()
        );
        assert!(s3.read().get(S3_ACCOUNT).unwrap().buckets["buffered"]
            .objects
            .is_empty());
    }

    #[tokio::test]
    async fn no_s3_destination_ack_obeys_shutdown_cutoff() {
        let svc = service();
        create(&svc, "buffered");
        put(&svc, b"configuration-only").unwrap();
        batch(&svc, vec![record(b"configuration-only")]).unwrap();
        assert_eq!(
            svc.delivery_engine().shutdown().await,
            crate::DrainReport::default()
        );
        for result in [
            put(&svc, b"late"),
            batch(&svc, vec![record(b"late")]),
            batch(&svc, vec![record(b"")]),
        ] {
            let error = result.err().unwrap();
            assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(error.code(), "ServiceUnavailableException");
        }
    }

    #[tokio::test]
    async fn injected_engine_state_is_used_by_api_and_snapshot_hooks() {
        let (existing, s3) = memory_fixture();
        let engine = existing.delivery_engine();
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(fakecloud_persistence::DiskSnapshotStore::new(
            tmp.path().join("firehose.json"),
        ));
        let svc = service()
            .with_snapshot_store(store.clone())
            .with_engine(engine.clone());
        assert!(Arc::ptr_eq(&svc.shared_state(), &existing.shared_state()));
        assert!(Arc::ptr_eq(&svc.delivery_engine(), &engine));
        assert_eq!(describe(&svc, "buffered")["DeliveryStreamName"], "buffered");
        put(&svc, b"pending-data-not-in-snapshot").unwrap();
        svc.handle(request(
            "TagDeliveryStream",
            json!({
                "DeliveryStreamName": "buffered", "Tags": [{"Key": "shared", "Value": "yes"}]
            }),
        ))
        .await
        .unwrap();
        svc.snapshot_hook().unwrap()().await;
        let bytes = store.load().unwrap().unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("pending-data-not-in-snapshot"));
        let snapshot: FirehoseSnapshot = serde_json::from_slice(&bytes).unwrap();
        let accounts = snapshot.accounts.unwrap();
        assert_eq!(
            accounts
                .get(S3_ACCOUNT)
                .unwrap()
                .streams("us-east-1")
                .unwrap()["buffered"]
                .tags["shared"],
            "yes"
        );
        assert_eq!(engine.shutdown().await.delivered_batches, 1);
        assert_eq!(
            s3.read().get(S3_ACCOUNT).unwrap().buckets["buffered"]
                .objects
                .len(),
            1
        );
    }

    /// Build an S3 state seeded with `bucket`, plus a matching on-disk
    /// [`DiskS3Store`] (with the bucket's meta so `load()` surfaces it), and a
    /// FirehoseService wired to both.
    fn firehose_with_disk_s3(
        bucket: &str,
    ) -> (
        FirehoseService,
        SharedS3State,
        std::sync::Arc<fakecloud_persistence::s3::DiskS3Store>,
        tempfile::TempDir,
    ) {
        use fakecloud_core::multi_account::MultiAccountState;
        use fakecloud_persistence::{cache::BodyCache, s3::DiskS3Store, BucketMeta, S3Store};
        use fakecloud_s3::{S3Bucket, S3State};

        let tmp = tempfile::TempDir::new().unwrap();
        let cache = std::sync::Arc::new(BodyCache::new(1024 * 1024));
        let disk = std::sync::Arc::new(DiskS3Store::new(tmp.path().join("s3"), cache));
        disk.put_bucket_meta(
            bucket,
            &BucketMeta {
                name: bucket.to_string(),
                region: "us-east-1".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        let mut mas: MultiAccountState<S3State> =
            MultiAccountState::new(S3_ACCOUNT, "us-east-1", "http://localhost");
        mas.get_or_create(S3_ACCOUNT).buckets.insert(
            bucket.to_string(),
            S3Bucket::new(bucket, "us-east-1", "owner-id"),
        );
        let s3_state = std::sync::Arc::new(RwLock::new(mas));

        let svc = FirehoseService::new(Arc::new(RwLock::new(FirehoseAccounts::new())))
            .with_s3(s3_state.clone())
            .with_s3_store(disk.clone() as std::sync::Arc<dyn S3Store>);
        (svc, s3_state, disk, tmp)
    }

    fn create_s3_stream(svc: &FirehoseService, name: &str, bucket: &str) {
        create_s3_stream_with_compression(svc, name, bucket, None);
    }

    fn create_s3_stream_with_compression(
        svc: &FirehoseService,
        name: &str,
        bucket: &str,
        compression: Option<&str>,
    ) {
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": name,
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": format!("arn:aws:s3:::{bucket}"),
                    "CompressionFormat": compression.unwrap_or("UNCOMPRESSED"),
                }
            }),
        ))
        .unwrap();
    }

    #[tokio::test]
    async fn snappy_delivery_uses_the_modeled_enum_and_xerial_metadata() {
        use crate::test_xerial_decoder::{decode, multiblock_input};
        use fakecloud_s3::checksum::{checksum, ChecksumAlgorithm, ChecksumType};

        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-snappy");
        create_s3_stream_with_compression(&svc, "snappy", "fh-snappy", Some("Snappy"));
        let input = multiblock_input();
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "snappy",
            vec![b"abc".to_vec(), b"def\n".to_vec(), input.clone()],
        )
        .unwrap();
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        let s3 = s3_state.read();
        let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-snappy"];
        assert_eq!(bucket.objects.len(), 1);
        let object = bucket.objects.values().next().unwrap();
        assert!(object.key.ends_with(".snappy"));
        assert_eq!(object.content_encoding.as_deref(), Some("snappy-java"));
        assert_eq!(object.content_type, "application/octet-stream");
        let raw = disk.open_object_body(&object.body).unwrap();
        assert_eq!(object.size, raw.len() as u64);
        let expected_checksum = checksum(ChecksumAlgorithm::Crc64Nvme, &raw);
        assert_eq!(
            object.checksum_algorithm,
            Some(ChecksumAlgorithm::Crc64Nvme)
        );
        assert_eq!(object.checksum_type, Some(ChecksumType::FullObject));
        assert_eq!(
            object.checksum_value.as_deref(),
            Some(expected_checksum.as_str())
        );
        let blocks = decode(&raw).unwrap();
        assert_eq!(
            blocks.iter().map(Vec::len).collect::<Vec<_>>(),
            [32768, 32768, 4471]
        );
        let decoded = blocks.concat();
        assert_eq!(&decoded[..7], b"abcdef\n");
        assert_eq!(&decoded[7..], input);
        assert_ne!(
            expected_checksum,
            checksum(ChecksumAlgorithm::Crc64Nvme, &decoded)
        );
        let key = object.key.clone();
        drop(s3);

        let loaded = disk.load().unwrap();
        let persisted = &loaded.buckets["fh-snappy"].objects[&key];
        assert_eq!(
            persisted.meta.checksum_value.as_deref(),
            Some(expected_checksum.as_str())
        );
        assert_eq!(disk.open_object_body(&persisted.body).unwrap(), raw);
    }

    #[tokio::test]
    async fn gzip_delivery_encodes_bytes_and_persists_metadata() {
        use fakecloud_s3::checksum::{checksum, ChecksumAlgorithm, ChecksumType};
        use std::io::Read;

        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-gzip");
        create_s3_stream_with_compression(&svc, "gzip", "fh-gzip", Some("GZIP"));
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "gzip",
            vec![b"abc".to_vec(), b"def\n".to_vec(), vec![0, 255]],
        )
        .unwrap();
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        let s3 = s3_state.read();
        let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-gzip"];
        assert_eq!(bucket.objects.len(), 1);
        let object = bucket.objects.values().next().unwrap();
        assert!(object.key.ends_with(".gz"));
        assert_eq!(object.content_type, "application/octet-stream");
        assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
        let raw = disk.open_object_body(&object.body).unwrap();
        assert!(raw.starts_with(&[0x1f, 0x8b, 8]));
        assert_eq!(object.size, raw.len() as u64);
        let expected_checksum = checksum(ChecksumAlgorithm::Crc64Nvme, &raw);
        assert_eq!(
            object.checksum_algorithm,
            Some(ChecksumAlgorithm::Crc64Nvme)
        );
        assert_eq!(object.checksum_type, Some(ChecksumType::FullObject));
        assert_eq!(
            object.checksum_value.as_deref(),
            Some(expected_checksum.as_str())
        );
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(raw.as_ref())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, b"abcdef\n\x00\xff");
        assert_ne!(
            expected_checksum,
            checksum(ChecksumAlgorithm::Crc64Nvme, &decoded)
        );
        let key = object.key.clone();
        drop(s3);

        let loaded = disk.load().unwrap();
        let persisted = &loaded.buckets["fh-gzip"].objects[&key];
        assert_eq!(persisted.meta.content_type, "application/octet-stream");
        assert_eq!(persisted.meta.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(
            persisted.meta.checksum_value.as_deref(),
            Some(expected_checksum.as_str())
        );
        assert_eq!(disk.open_object_body(&persisted.body).unwrap(), raw);
    }

    #[tokio::test]
    async fn gzip_store_failure_retains_records_without_memory_publication() {
        use fakecloud_persistence::{cache::BodyCache, s3::DiskS3Store};

        for fail_staging in [true, false] {
            let (svc, s3_state, disk, tmp) = firehose_with_disk_s3("fh-fallback");
            create_s3_stream_with_compression(&svc, "fallback", "fh-fallback", Some("GZIP"));
            if fail_staging {
                let blocked = tmp.path().join("not-a-directory");
                std::fs::write(&blocked, b"blocks persistent writes").unwrap();
                svc.set_s3_store(Arc::new(DiskS3Store::new(
                    blocked,
                    Arc::new(BodyCache::new(1024)),
                )));
            } else {
                // Staging can succeed, but the final object directory cannot be created.
                std::fs::write(
                    tmp.path().join("s3/buckets/fh-fallback/objects"),
                    b"blocks commit",
                )
                .unwrap();
            }
            let response = svc.put_record(&request("PutRecord", json!({
            "DeliveryStreamName": "fallback",
            "Record": {"Data": base64::engine::general_purpose::STANDARD.encode(b"\x00\xff")}
        }))).unwrap();
            let response: Value = serde_json::from_slice(response.body.expect_bytes()).unwrap();
            assert!(response["RecordId"].as_str().is_some());

            let report = svc.delivery_engine().shutdown().await;
            assert_eq!(
                report,
                crate::DrainReport {
                    delivered_batches: 0,
                    undelivered_records: 1,
                    undelivered_bytes: 2,
                }
            );
            assert_eq!(svc.delivery_engine().shutdown().await, report);
            let s3 = s3_state.read();
            let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-fallback"];
            assert!(
                bucket.objects.is_empty(),
                "failed persistence cannot publish a memory fallback"
            );
            // The deliberately corrupt objects root makes load skip the bucket;
            // remove only the blocker before checking durable publication.
            if !fail_staging {
                std::fs::remove_file(tmp.path().join("s3/buckets/fh-fallback/objects")).unwrap();
            }
            assert!(disk.load().unwrap().buckets["fh-fallback"]
                .objects
                .is_empty());
        }
    }

    #[tokio::test]
    async fn empty_gzip_delivery_does_not_create_an_object() {
        let (svc, s3_state, _disk, _tmp) = firehose_with_disk_s3("fh-empty-gzip");
        create_s3_stream_with_compression(&svc, "empty-gzip", "fh-empty-gzip", Some("GZIP"));
        svc.deliver_records(S3_ACCOUNT, "us-east-1", "empty-gzip", vec![vec![], vec![]])
            .unwrap();
        assert_eq!(
            svc.delivery_engine().shutdown().await,
            crate::DrainReport::default()
        );
        let s3 = s3_state.read();
        assert!(s3.get(S3_ACCOUNT).unwrap().buckets["fh-empty-gzip"]
            .objects
            .is_empty());
    }

    fn kinesis_source_body(name: &str) -> Value {
        json!({
            "DeliveryStreamName": name,
            "DeliveryStreamType": "KinesisStreamAsSource",
            "KinesisStreamSourceConfiguration": {
                "KinesisStreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/source",
                "RoleARN": "arn:aws:iam::123456789012:role/fh",
            },
            "ExtendedS3DestinationConfiguration": {
                "RoleARN": "arn:aws:iam::123456789012:role/fh",
                "BucketARN": "arn:aws:s3:::fh-bucket",
            },
        })
    }

    fn record_request(action: &str, name: &str) -> AwsRequest {
        let mut body = json!({ "DeliveryStreamName": name });
        if action == "PutRecord" {
            body["Record"] = json!({ "Data": "eA==" });
        } else {
            body["Records"] = json!([{ "Data": "eA==" }]);
        }
        request(action, body)
    }

    fn assert_source_rejected(err: AwsServiceError) {
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.code(), "InvalidArgumentException");
        assert_eq!(
            err.message(),
            "This operation is not permitted on KinesisStreamAsSource delivery stream type."
        );
    }

    fn assert_source_bucket_empty(s3: &SharedS3State, disk: &dyn S3Store) {
        let s3 = s3.read();
        assert!(s3.get(S3_ACCOUNT).unwrap().buckets["fh-bucket"]
            .objects
            .is_empty());
        assert!(disk.load().unwrap().buckets["fh-bucket"].objects.is_empty());
    }

    #[test]
    fn put_record_rejects_kinesis_source_without_s3_side_effects() {
        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-bucket");
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            kinesis_source_body("source-stream"),
        ))
        .unwrap();

        assert_source_rejected(
            svc.put_record(&record_request("PutRecord", "source-stream"))
                .err()
                .expect("source streams must reject PutRecord"),
        );
        assert_source_bucket_empty(&s3_state, disk.as_ref());
    }

    #[test]
    fn put_record_batch_rejects_kinesis_source_without_s3_side_effects() {
        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-bucket");
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            kinesis_source_body("source-stream"),
        ))
        .unwrap();

        for records in [
            json!([{ "Data": "eA==" }]),
            json!([{ "Data": "eA==" }, { "Data": "" }]),
        ] {
            assert_source_rejected(
                svc.put_record_batch(&request(
                    "PutRecordBatch",
                    json!({ "DeliveryStreamName": "source-stream", "Records": records }),
                ))
                .err()
                .expect("source streams must reject PutRecordBatch"),
            );
        }
        assert_source_bucket_empty(&s3_state, disk.as_ref());
    }

    #[test]
    fn put_record_batch_rejects_kinesis_source_without_deliverable_records() {
        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-bucket");
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            kinesis_source_body("source-stream"),
        ))
        .unwrap();

        // Empty arrays fail the request-wide 1-500 entry check; valid empty
        // blobs in a nonempty array must still run source admission.
        let err = svc
            .put_record_batch(&request(
                "PutRecordBatch",
                json!({
                    "DeliveryStreamName": "source-stream", "Records": []
                }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidArgumentException");
        for records in [
            json!([{ "Data": "" }, {"Data": ""}]),
            json!([{ "Data": "" }]),
        ] {
            assert_source_rejected(
                svc.put_record_batch(&request(
                    "PutRecordBatch",
                    json!({ "DeliveryStreamName": "source-stream", "Records": records }),
                ))
                .err()
                .expect("empty/all-invalid batches must not bypass source admission"),
            );
        }
        assert_source_bucket_empty(&s3_state, disk.as_ref());
    }

    #[test]
    fn source_admission_precedes_destination_and_backend_checks() {
        for with_backend in [false, true] {
            for destination in ["s3", "http", "none"] {
                let (mut svc, _s3_state, _disk, _tmp) = firehose_with_disk_s3("fh-bucket");
                if !with_backend {
                    svc = service();
                }
                let mut body = kinesis_source_body("source-stream");
                if destination != "s3" {
                    body["ExtendedS3DestinationConfiguration"] = Value::Null;
                }
                if destination == "http" {
                    body["HttpEndpointDestinationConfiguration"] = json!({
                        "EndpointConfiguration": { "Url": "https://example.com/ingest" },
                        "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    });
                }
                svc.create_delivery_stream(&request("CreateDeliveryStream", body))
                    .unwrap();
                for action in ["PutRecord", "PutRecordBatch"] {
                    let req = record_request(action, "source-stream");
                    let result = if action == "PutRecord" {
                        svc.put_record(&req)
                    } else {
                        svc.put_record_batch(&req)
                    };
                    assert_source_rejected(result.err().expect("source admission is required"));
                }
                for datas in [vec![], vec![b"x".to_vec()]] {
                    assert_source_rejected(
                        svc.deliver_records(S3_ACCOUNT, "us-east-1", "source-stream", datas)
                            .expect_err("shared delivery must reject before backend checks"),
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn internal_delivery_hook_rejects_kinesis_source_without_s3_side_effects() {
        use fakecloud_core::delivery::FirehoseDelivery;

        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-bucket");
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            kinesis_source_body("source-stream"),
        ))
        .unwrap();
        create_s3_stream(&svc, "direct-stream", "fh-bucket");
        let hook = crate::delivery::FirehoseDeliveryImpl::with_engine(svc.delivery_engine());
        hook.set_s3_store(disk.clone());

        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/source-stream",
            b"must not deliver",
        );
        assert_source_bucket_empty(&s3_state, disk.as_ref());

        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/direct-stream",
            b"x",
        );
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        let s3 = s3_state.read();
        let objects = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-bucket"].objects;
        assert_eq!(objects.len(), 1, "DirectPut hook still delivers");
        assert_eq!(objects.values().next().unwrap().size, 1);
        assert_eq!(disk.load().unwrap().buckets["fh-bucket"].objects.len(), 1);
    }

    #[test]
    fn source_admission_is_account_and_region_isolated_for_same_names() {
        let svc = service();
        let scopes = [
            (S3_ACCOUNT, "us-east-1", "KinesisStreamAsSource"),
            (S3_ACCOUNT, "us-west-2", "DirectPut"),
            ("222222222222", "us-east-1", "DirectPut"),
            ("222222222222", "us-west-2", "KinesisStreamAsSource"),
        ];
        for (account, region, stream_type) in scopes {
            let mut req = request(
                "CreateDeliveryStream",
                json!({ "DeliveryStreamName": "same-name", "DeliveryStreamType": stream_type }),
            );
            req.account_id = account.to_string();
            req.region = region.to_string();
            svc.create_delivery_stream(&req).unwrap();
        }
        for (account, region, stream_type) in scopes {
            for action in ["PutRecord", "PutRecordBatch"] {
                let mut req = record_request(action, "same-name");
                req.account_id = account.to_string();
                req.region = region.to_string();
                let result = if action == "PutRecord" {
                    svc.put_record(&req)
                } else {
                    svc.put_record_batch(&req)
                };
                if stream_type == "DirectPut" {
                    assert_eq!(result.unwrap().status, StatusCode::OK);
                } else {
                    assert_source_rejected(result.err().expect("scoped source stream rejects"));
                }
            }
        }
    }

    #[test]
    fn record_admission_preserves_scoped_resource_not_found() {
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            kinesis_source_body("source-stream"),
        ))
        .unwrap();
        for (account, region, name) in [
            ("222222222222", "us-east-1", "source-stream"),
            (S3_ACCOUNT, "us-west-2", "source-stream"),
            (S3_ACCOUNT, "us-east-1", "missing-stream"),
        ] {
            for action in ["PutRecord", "PutRecordBatch"] {
                let mut req = record_request(action, name);
                req.account_id = account.to_string();
                req.region = region.to_string();
                let result = if action == "PutRecord" {
                    svc.put_record(&req)
                } else {
                    svc.put_record_batch(&req)
                };
                let err = result.err().expect("stream is absent in this scope");
                assert_eq!(err.status(), StatusCode::BAD_REQUEST);
                assert_eq!(err.code(), "ResourceNotFoundException");
                assert_eq!(err.message(), format!("DeliveryStream {name} not found"));
            }
            let err = svc
                .deliver_records(account, region, name, vec![])
                .expect_err("shared delivery also checks scoped existence");
            assert_eq!(err.code(), "ResourceNotFoundException");
        }
    }

    #[tokio::test]
    async fn direct_put_record_succeeds_and_malformed_batches_do_not_deliver() {
        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-bucket");
        create_s3_stream(&svc, "direct-stream", "fh-bucket");
        let resp = svc
            .put_record(&record_request("PutRecord", "direct-stream"))
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(!body["RecordId"].as_str().unwrap().is_empty());

        for records in [
            json!([{ "Data": "eA==" }, { "Data": "@@@" }]),
            json!([{ "Data": "@@@" }]),
        ] {
            let error = svc
                .put_record_batch(&request(
                    "PutRecordBatch",
                    json!({ "DeliveryStreamName": "direct-stream", "Records": records }),
                ))
                .err()
                .unwrap();
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.code(), "SerializationException");
        }
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        let s3 = s3_state.read();
        assert_eq!(
            s3.get(S3_ACCOUNT).unwrap().buckets["fh-bucket"]
                .objects
                .len(),
            1
        );
        assert_eq!(disk.load().unwrap().buckets["fh-bucket"].objects.len(), 1);
        assert_eq!(
            s3.get(S3_ACCOUNT).unwrap().buckets["fh-bucket"]
                .objects
                .values()
                .next()
                .unwrap()
                .size,
            1
        );
    }

    /// `AppendDelimiterToRecord` runs before compression: every record of an
    /// API or internal delivery, including an empty one (which alone would not
    /// produce a GZIP object), is followed by `\n` inside the GZIP stream.
    #[tokio::test]
    async fn gzip_compresses_records_after_append_delimiter() {
        use fakecloud_core::delivery::FirehoseDelivery;
        use std::io::Read;

        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-gzip-delim");
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "gzip-delim",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::fh-gzip-delim",
                    "CompressionFormat": "GZIP",
                    // Zero interval: each delivery below detaches as its own object.
                    "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 0},
                    "ProcessingConfiguration": {
                        "Enabled": true,
                        "Processors": [{"Type": "AppendDelimiterToRecord"}]
                    }
                }
            }),
        ))
        .unwrap();
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "gzip-delim",
            vec![b"a".to_vec(), b"b\n".to_vec(), vec![0, 255]],
        )
        .unwrap();
        svc.deliver_records(S3_ACCOUNT, "us-east-1", "gzip-delim", vec![vec![]])
            .unwrap();
        let hook = crate::FirehoseDeliveryImpl::with_engine(svc.delivery_engine());
        hook.set_s3_store(disk.clone());
        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/gzip-delim",
            b"{\"internal\":true}",
        );
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 3);

        let s3 = s3_state.read();
        let mut decoded: Vec<Vec<u8>> = s3.get(S3_ACCOUNT).unwrap().buckets["fh-gzip-delim"]
            .objects
            .values()
            .map(|object| {
                assert!(object.key.ends_with(".gz"), "{}", object.key);
                assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
                let raw = disk.open_object_body(&object.body).unwrap();
                assert_eq!(object.size, raw.len() as u64);
                let mut plain = Vec::new();
                flate2::read::GzDecoder::new(raw.as_ref())
                    .read_to_end(&mut plain)
                    .unwrap();
                plain
            })
            .collect();
        decoded.sort();
        assert_eq!(
            decoded,
            vec![
                b"\n".to_vec(),
                b"a\nb\n\n\x00\xff\n".to_vec(),
                b"{\"internal\":true}\n".to_vec(),
            ]
        );
    }

    #[tokio::test]
    async fn delivery_preserves_binary_bytes_and_producer_delimiters() {
        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-bytes");
        create_s3_stream(&svc, "verbatim", "fh-bytes");
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "verbatim",
            vec![b"abc".to_vec(), b"def\n".to_vec(), vec![0, 255]],
        )
        .unwrap();

        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        let s3 = s3_state.read();
        let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-bytes"];
        assert_eq!(bucket.objects.len(), 1);
        let object = bucket.objects.values().next().unwrap();
        assert_eq!(
            disk.open_object_body(&object.body).unwrap().as_ref(),
            b"abcdef\n\x00\xff"
        );
        assert_eq!(object.size, 9);
        // S3 stores the bare MD5 hex (GetObject/HeadObject/List add the quotes),
        // as for any single-part PutObject.
        assert_eq!(object.etag, "7d2ae660248a023760f924dd991e2533");
    }

    #[tokio::test]
    async fn delivery_names_use_stream_version_utc_and_custom_extension() {
        for (prefix, extension) in [
            (None, None),
            (Some("events/"), Some(".custom.log")),
            (Some("events"), Some("")),
            (Some("../logical/"), Some(".log")),
        ] {
            let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-names");
            create_s3_stream(&svc, "named", "fh-names");
            let destination_id = svc
                .state
                .read()
                .get(S3_ACCOUNT)
                .unwrap()
                .streams("us-east-1")
                .unwrap()["named"]
                .destination
                .as_ref()
                .unwrap()
                .destination_id
                .clone();
            let mut update = json!({});
            if let Some(prefix) = prefix {
                update["Prefix"] = json!(prefix);
            }
            if let Some(extension) = extension {
                update["FileExtension"] = json!(extension);
            }
            svc.update_destination(&request(
                "UpdateDestination",
                json!({
                    "DeliveryStreamName": "named",
                    "CurrentDeliveryStreamVersionId": "1",
                    "DestinationId": destination_id,
                    "ExtendedS3DestinationUpdate": update
                }),
            ))
            .unwrap();
            let before = Utc::now();
            svc.deliver_records(S3_ACCOUNT, "us-east-1", "named", vec![b"x".to_vec()])
                .unwrap();
            assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
            let after = Utc::now();

            let s3 = s3_state.read();
            let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-names"];
            let object = bucket.objects.values().next().unwrap();
            // S3's LastModified is assigned after staging, so it need not share
            // the key's delivery second when disk I/O spans a time boundary.
            let name = object.key.strip_prefix(prefix.unwrap_or_default()).unwrap();
            let (hierarchy, suffix) = name.rsplit_once("/named-2-").unwrap();
            let (timestamp, uuid_and_extension) = suffix.split_at(19);
            let time = chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d-%H-%M-%S")
                .unwrap()
                .and_utc();
            assert!((before.timestamp()..=after.timestamp()).contains(&time.timestamp()));
            assert_eq!(hierarchy, time.format("%Y/%m/%d/%H").to_string());
            let uuid_and_extension = uuid_and_extension.strip_prefix('-').unwrap();
            let uuid = uuid_and_extension
                .strip_suffix(extension.unwrap_or_default())
                .unwrap();
            assert_eq!(uuid.len(), 36);
            assert_eq!(Uuid::parse_str(uuid).unwrap().to_string(), uuid);
            assert_eq!(disk.open_object_body(&object.body).unwrap().as_ref(), b"x");
        }
    }

    #[tokio::test]
    async fn internal_delivery_preserves_exact_bytes() {
        use fakecloud_core::delivery::FirehoseDelivery;

        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-hook");
        create_s3_stream(&svc, "hook", "fh-hook");
        let hook = crate::FirehoseDeliveryImpl::with_engine(svc.delivery_engine());
        hook.set_s3_store(disk.clone());
        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/hook",
            b"\x00\xff",
        );

        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        let s3 = s3_state.read();
        let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-hook"];
        assert_eq!(bucket.objects.len(), 1);
        assert_eq!(
            disk.open_object_body(&bucket.objects.values().next().unwrap().body)
                .unwrap()
                .as_ref(),
            b"\x00\xff"
        );
    }

    #[tokio::test]
    async fn empty_delivery_does_not_create_an_object() {
        let (svc, s3_state, _disk, _tmp) = firehose_with_disk_s3("fh-empty");
        create_s3_stream(&svc, "empty", "fh-empty");
        svc.deliver_records(S3_ACCOUNT, "us-east-1", "empty", vec![vec![], vec![]])
            .unwrap();
        assert_eq!(
            svc.delivery_engine().shutdown().await,
            crate::DrainReport::default()
        );
        let s3 = s3_state.read();
        assert!(s3.get(S3_ACCOUNT).unwrap().buckets["fh-empty"]
            .objects
            .is_empty());
    }

    fn s3_destination_with(extra: Value) -> Value {
        let mut value = json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": "arn:aws:s3:::bucket",
        });
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        value
    }

    #[test]
    fn file_extension_validation_boundaries() {
        let longest = format!(".{}", "a".repeat(127));
        for valid in ["", ".log", ".custom.log", ".a0!-_.*'()", longest.as_str()] {
            let dest = parse_s3_destination(&s3_destination_with(json!({"FileExtension": valid})))
                .unwrap()
                .unwrap();
            assert_eq!(dest.file_extension.as_deref(), Some(valid));
        }
        let too_long = format!(".{}", "a".repeat(128));
        for invalid in [
            json!("log"),
            json!("."),
            json!(".UPPER"),
            json!(".é"),
            json!(".a/b"),
            json!(".a b"),
            json!(too_long),
            json!(1),
            json!(true),
        ] {
            let err = parse_s3_destination(&s3_destination_with(json!({"FileExtension": invalid})))
                .err()
                .unwrap_or_else(|| panic!("FileExtension {invalid} must be rejected"));
            assert_eq!(err.code(), "InvalidArgumentException");
            assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn update_destination_rejects_invalid_file_extension_without_changes() {
        let svc = service();
        let dest_id = create_ver_stream(&svc);
        for invalid in [json!(".UPPER"), json!("log"), json!(7)] {
            let err = svc
                .update_destination(&request(
                    "UpdateDestination",
                    json!({
                        "DeliveryStreamName": "ver-stream",
                        "CurrentDeliveryStreamVersionId": "1",
                        "DestinationId": dest_id,
                        "ExtendedS3DestinationUpdate": {"Prefix": "changed/", "FileExtension": invalid}
                    }),
                ))
                .err()
                .unwrap_or_else(|| panic!("FileExtension {invalid} must be rejected"));
            assert_eq!(err.code(), "InvalidArgumentException");
        }
        let after = describe(&svc, "ver-stream");
        assert_eq!(after["VersionId"], "1");
        let ext = &after["Destinations"][0]["ExtendedS3DestinationDescription"];
        assert!(ext.get("Prefix").is_none(), "{after}");
        assert!(ext.get("FileExtension").is_none(), "{after}");
    }

    /// Zero-interval stream, so every delivery detaches as its own object.
    fn create_stream_with_processing(
        svc: &FirehoseService,
        name: &str,
        bucket: &str,
        processing: Value,
    ) {
        let mut destination = json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": format!("arn:aws:s3:::{bucket}"),
            "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 0},
        });
        if !processing.is_null() {
            destination["ProcessingConfiguration"] = processing;
        }
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({"DeliveryStreamName": name, "ExtendedS3DestinationConfiguration": destination}),
        ))
        .unwrap();
    }

    fn delivered_bodies(
        s3_state: &SharedS3State,
        disk: &fakecloud_persistence::s3::DiskS3Store,
        bucket: &str,
    ) -> Vec<Vec<u8>> {
        let s3 = s3_state.read();
        let mut bodies: Vec<Vec<u8>> = s3.get(S3_ACCOUNT).unwrap().buckets[bucket]
            .objects
            .values()
            .map(|object| disk.open_object_body(&object.body).unwrap().to_vec())
            .collect();
        bodies.sort();
        bodies
    }

    /// AWS: "If you want to add a new line delimiter between records in objects
    /// that are delivered to Amazon S3, choose AppendDelimiterToRecord as a
    /// processor type." Applied per record (after each one), for API and
    /// internal (Logs/SES) deliveries alike. A Lambda processor is not run, so
    /// configuring one alongside it is refused rather than silently skipped.
    #[tokio::test]
    async fn append_delimiter_processor_terminates_every_record() {
        use fakecloud_core::delivery::FirehoseDelivery;

        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-delim");
        create_stream_with_processing(
            &svc,
            "delim",
            "fh-delim",
            json!({"Enabled": true, "Processors": [
                {"Type": "AppendDelimiterToRecord", "Parameters": [{"ParameterName": "Delimiter", "ParameterValue": "\\n"}]}
            ]}),
        );
        create_stream_with_processing(
            &svc,
            "lambda-delim",
            "fh-delim",
            json!({"Enabled": true, "Processors": [
                {"Type": "Lambda", "Parameters": [{"ParameterName": "LambdaArn", "ParameterValue": "arn:aws:lambda:us-east-1:123456789012:function:f"}]},
                {"Type": "AppendDelimiterToRecord", "Parameters": [{"ParameterName": "Delimiter", "ParameterValue": "\\n"}]}
            ]}),
        );
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "delim",
            vec![b"a".to_vec(), b"b\n".to_vec(), vec![0, 255]],
        )
        .unwrap();
        svc.deliver_records(S3_ACCOUNT, "us-east-1", "delim", vec![vec![]])
            .unwrap();
        let error = svc
            .deliver_records(S3_ACCOUNT, "us-east-1", "lambda-delim", vec![b"x".to_vec()])
            .unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.code(), "InvalidArgumentException");
        let hook = crate::FirehoseDeliveryImpl::with_engine(svc.delivery_engine());
        hook.set_s3_store(disk.clone());
        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/delim",
            b"{\"internal\":true}",
        );
        // The internal hook cannot surface the admission error; it drops the record.
        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/lambda-delim",
            b"{\"internal\":\"lambda\"}",
        );

        assert_eq!(
            svc.delivery_engine().shutdown().await,
            crate::DrainReport {
                delivered_batches: 3,
                undelivered_records: 0,
                undelivered_bytes: 0,
            }
        );
        assert_eq!(
            delivered_bodies(&s3_state, &disk, "fh-delim"),
            vec![
                b"\n".to_vec(),
                b"a\nb\n\n\x00\xff\n".to_vec(),
                b"{\"internal\":true}\n".to_vec(),
            ]
        );
    }

    #[tokio::test]
    async fn append_delimiter_requires_an_enabled_processing_configuration() {
        for processing in [
            Value::Null,
            json!({"Enabled": false, "Processors": [{"Type": "AppendDelimiterToRecord"}]}),
            json!({"Processors": [{"Type": "AppendDelimiterToRecord"}]}),
            json!({"Enabled": false, "Processors": [{"Type": "Lambda"}]}),
        ] {
            let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-raw");
            create_stream_with_processing(&svc, "raw", "fh-raw", processing.clone());
            svc.deliver_records(
                S3_ACCOUNT,
                "us-east-1",
                "raw",
                vec![b"a".to_vec(), b"b".to_vec()],
            )
            .unwrap();
            assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
            assert_eq!(
                delivered_bodies(&s3_state, &disk, "fh-raw"),
                vec![b"ab".to_vec()],
                "{processing}"
            );
        }
        // Enabled processing without an AppendDelimiterToRecord processor is
        // not run, and is not silently delivered raw either.
        for processing in [
            json!({"Enabled": true, "Processors": [{"Type": "Lambda"}]}),
            json!({"Enabled": true}),
        ] {
            let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-raw");
            create_stream_with_processing(&svc, "raw", "fh-raw", processing.clone());
            let error = svc
                .deliver_records(
                    S3_ACCOUNT,
                    "us-east-1",
                    "raw",
                    vec![b"a".to_vec(), b"b".to_vec()],
                )
                .unwrap_err();
            assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{processing}");
            assert_eq!(error.code(), "InvalidArgumentException");
            assert_eq!(
                svc.delivery_engine().shutdown().await,
                crate::DrainReport::default()
            );
            assert!(delivered_bodies(&s3_state, &disk, "fh-raw").is_empty());
        }
    }

    #[tokio::test]
    async fn put_record_batch_accepts_empty_records_like_put_record() {
        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-empty");
        create_stream_with_processing(
            &svc,
            "delimited",
            "fh-empty",
            json!({"Enabled": true, "Processors": [{"Type": "AppendDelimiterToRecord"}]}),
        );
        let resp = svc
            .put_record_batch(&request(
                "PutRecordBatch",
                json!({
                    "DeliveryStreamName": "delimited",
                    "Records": [{ "Data": "YQ==" }, { "Data": "" }]
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["FailedPutCount"], 0);
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        assert_eq!(
            delivered_bodies(&s3_state, &disk, "fh-empty"),
            vec![b"a\n\n".to_vec()]
        );
    }

    #[tokio::test]
    async fn put_record_writes_through_to_durable_s3_store() {
        use fakecloud_persistence::{BodyRef, S3Store};

        let (svc, s3_state, disk, _tmp) = firehose_with_disk_s3("fh-bucket");
        create_s3_stream(&svc, "logs-to-s3", "fh-bucket");

        let payload = b"event-line".to_vec();
        let resp = svc
            .put_record(&request(
                "PutRecord",
                json!({
                    "DeliveryStreamName": "logs-to-s3",
                    "Record": {
                        "Data": base64::engine::general_purpose::STANDARD.encode(&payload)
                    }
                }),
            ))
            .expect("PutRecord should succeed when the destination bucket exists");
        assert!(resp
            .body
            .expect_bytes()
            .windows(8)
            .any(|w| w == b"RecordId"));

        // The delivered object's in-memory body must be the store's canonical
        // disk-backed ref (proof the write went through the store, not just RAM),
        // and the backing file must exist on disk.
        assert_eq!(svc.delivery_engine().shutdown().await.delivered_batches, 1);
        let s3 = s3_state.read();
        let acct = s3.get(S3_ACCOUNT).unwrap();
        let bucket = acct.buckets.get("fh-bucket").unwrap();
        assert_eq!(bucket.objects.len(), 1, "exactly one delivered object");
        let obj = bucket.objects.values().next().unwrap();
        match &obj.body {
            BodyRef::Disk { path, .. } => {
                assert!(path.exists(), "delivered object bytes must be on disk");
            }
            BodyRef::Memory(_) => panic!("delivery was not written through to the durable store"),
        }
        drop(s3);

        // Reloading the store from disk (as boot does) must surface the object.
        let reloaded = disk.load().unwrap();
        let rbucket = reloaded
            .buckets
            .get("fh-bucket")
            .expect("bucket survives reload");
        assert_eq!(
            rbucket.objects.len(),
            1,
            "delivered object survives a store reload"
        );
    }

    #[tokio::test]
    async fn put_record_to_missing_bucket_is_accepted_and_retained() {
        // AWS accepts PutRecord into the delivery buffer and reports delivery
        // failures asynchronously (never as an API error), so a stream whose
        // destination bucket is absent still returns 200 + RecordId. The record
        // stays buffered for retry instead of fabricating a synchronous error.
        let (svc, s3_state, _disk, _tmp) = firehose_with_disk_s3("real-bucket");
        create_s3_stream(&svc, "broken-stream", "ghost-bucket");

        let resp = svc
            .put_record(&request(
                "PutRecord",
                json!({
                    "DeliveryStreamName": "broken-stream",
                    "Record": {
                        "Data": base64::engine::general_purpose::STANDARD.encode(b"x")
                    }
                }),
            ))
            .expect("PutRecord to a missing destination bucket must still be accepted (AWS async delivery)");
        assert!(resp
            .body
            .expect_bytes()
            .windows(8)
            .any(|w| w == b"RecordId"));

        // The record could not be delivered, so the existing (different) bucket
        // stays empty — nothing was written anywhere.
        assert_eq!(
            svc.delivery_engine().shutdown().await,
            crate::DrainReport {
                delivered_batches: 0,
                undelivered_records: 1,
                undelivered_bytes: 1,
            }
        );
        let s3 = s3_state.read();
        let real = s3
            .get(S3_ACCOUNT)
            .unwrap()
            .buckets
            .get("real-bucket")
            .unwrap();
        assert!(real.objects.is_empty(), "no object should be written");
    }

    #[test]
    fn china_region_delivery_stream_uses_the_aws_cn_partition() {
        let svc = service();
        let in_china = |action: &str, body: Value| AwsRequest {
            region: "cn-north-1".to_string(),
            ..request(action, body)
        };
        svc.create_delivery_stream(&in_china(
            "CreateDeliveryStream",
            json!({ "DeliveryStreamName": "cn-fh" }),
        ))
        .unwrap();
        let resp = svc
            .describe_delivery_stream(&in_china(
                "DescribeDeliveryStream",
                json!({ "DeliveryStreamName": "cn-fh" }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            body["DeliveryStreamDescription"]["DeliveryStreamARN"],
            "arn:aws-cn:firehose:cn-north-1:123456789012:deliverystream/cn-fh"
        );
        assert_eq!(
            fakecloud_aws::arn::arn_resource("arn:aws-cn:s3:::cn-bucket", "s3")
                .and_then(|r| r.strip_prefix("::")),
            Some("cn-bucket")
        );
    }
}
