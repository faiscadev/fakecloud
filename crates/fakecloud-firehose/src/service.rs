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
use fakecloud_persistence::{BodySource, S3Store, SnapshotStore};
use fakecloud_s3::{memory_body, S3Object, SharedS3State};

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
    s3: Option<SharedS3State>,
    /// Durable S3 backing store. S3 durability is write-through to this store
    /// (there is no `s3_state` snapshot; on boot S3 is rebuilt from the store),
    /// so records delivered to an S3 destination must be routed through it or
    /// they are lost on restart. Behind a lock so the CloudWatch-Logs delivery
    /// hook (built before the S3 store exists) can be wired up after the fact
    /// via `set_s3_store`.
    s3_store: RwLock<Option<Arc<dyn S3Store>>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl FirehoseService {
    pub fn new(state: SharedFirehoseState) -> Self {
        Self {
            state,
            s3: None,
            s3_store: RwLock::new(None),
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_s3(mut self, s3: SharedS3State) -> Self {
        self.s3 = Some(s3);
        self
    }

    /// Builder form of [`Self::set_s3_store`], for the public service which is
    /// constructed after the S3 store already exists.
    pub fn with_s3_store(self, store: Arc<dyn S3Store>) -> Self {
        self.set_s3_store(store);
        self
    }

    /// Wire the durable S3 store after construction. Used by
    /// `FirehoseDeliveryImpl`, whose delivery hook is built before the S3 store
    /// is available during server startup.
    pub fn set_s3_store(&self, store: Arc<dyn S3Store>) {
        *self.s3_store.write() = Some(store);
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

fn parse_s3_destination(val: &Value) -> Result<Option<S3Destination>, AwsServiceError> {
    if !val.is_object() {
        return Ok(None);
    }
    let Some(role_arn) = val["RoleARN"].as_str().map(|s| s.to_string()) else {
        return Ok(None);
    };
    let Some(bucket_arn) = val["BucketARN"].as_str().map(|s| s.to_string()) else {
        return Ok(None);
    };
    let buf = &val["BufferingHints"];
    let buffering_size_mb = buf["SizeInMBs"].as_i64();
    let buffering_interval_seconds = buf["IntervalInSeconds"].as_i64();
    validate_buffering(buffering_size_mb, buffering_interval_seconds)?;
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
    let buf = &val["BufferingHints"];
    let size = buf["SizeInMBs"].as_i64();
    let interval = buf["IntervalInSeconds"].as_i64();
    validate_buffering(size, interval)?;

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
    Ok(())
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

/// AWS limits per Firehose docs: SizeInMBs in [1,128], IntervalInSeconds
/// either 0 (immediate) or in [60,900].
fn validate_buffering(
    size_mb: Option<i64>,
    interval_s: Option<i64>,
) -> Result<(), AwsServiceError> {
    if let Some(s) = size_mb {
        if !(1..=128).contains(&s) {
            return Err(invalid_argument(format!(
                "BufferingHints.SizeInMBs must be between 1 and 128, got {s}"
            )));
        }
    }
    if let Some(i) = interval_s {
        if i != 0 && !(60..=900).contains(&i) {
            return Err(invalid_argument(format!(
                "BufferingHints.IntervalInSeconds must be 0 or between 60 and 900, got {i}"
            )));
        }
    }
    Ok(())
}

fn s3_destination_json(dest: &S3Destination) -> Value {
    let mut buf = json!({});
    if let Some(s) = dest.buffering_size_mb {
        buf["SizeInMBs"] = json!(s);
    }
    if let Some(i) = dest.buffering_interval_seconds {
        buf["IntervalInSeconds"] = json!(i);
    }
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
    json!({
        "DestinationId": dest.destination_id,
        "S3DestinationDescription": s3,
        "ExtendedS3DestinationDescription": extended,
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
    Arn::new(
        "firehose",
        region,
        account,
        &format!("deliverystream/{name}"),
    )
    .to_string()
}

fn bucket_name_from_arn(arn: &str) -> Option<&str> {
    arn.strip_prefix("arn:aws:s3:::")
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

        let s3_dest = match parse_s3_destination(&body["S3DestinationConfiguration"])? {
            Some(d) => Some(d),
            None => parse_s3_destination(&body["ExtendedS3DestinationConfiguration"])?,
        };
        let extra_destinations = extract_extra_destinations(&body, "Configuration");

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
            name: name.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            stream_type,
            created_at: now,
            last_update: now,
            version_id: "1".to_string(),
            destination: s3_dest,
            tags: BTreeMap::new(),
            encryption: None,
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
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn put_record(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?
            .to_string();
        let data_b64 = body["Record"]["Data"]
            .as_str()
            .ok_or_else(|| missing("Record.Data"))?;
        let data = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgumentException",
                    "Record.Data must be valid base64",
                )
            })?;
        let record_id = format!("{}", Uuid::new_v4());
        self.deliver_records(&req.account_id, &req.region, &name, vec![data])?;
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
        let mut datas = Vec::with_capacity(records.len());
        let mut response_records = Vec::with_capacity(records.len());
        let mut failed = 0;
        for r in records {
            let b64 = r["Data"].as_str();
            match b64.map(|s| base64::engine::general_purpose::STANDARD.decode(s)) {
                Some(Ok(data)) if !data.is_empty() => {
                    datas.push(data);
                    response_records.push(json!({
                        "RecordId": Uuid::new_v4().to_string(),
                    }));
                }
                _ => {
                    failed += 1;
                    response_records.push(json!({
                        "ErrorCode": "InvalidArgumentException",
                        "ErrorMessage": "Record.Data must be a non-empty base64-encoded string",
                    }));
                }
            }
        }
        if !datas.is_empty() {
            self.deliver_records(&req.account_id, &req.region, &name, datas)?;
        } else if failed == records.len() && !records.is_empty() {
            // Validate stream exists even when nothing delivers, so callers
            // get a clear ResourceNotFoundException for typos.
            let accounts = self.state.read();
            let state = accounts
                .get(&req.account_id)
                .ok_or_else(|| not_found(&name))?;
            state
                .streams(&req.region)
                .and_then(|s| s.get(&name))
                .ok_or_else(|| not_found(&name))?;
        }
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

        // Non-S3 destinations (Redshift/OpenSearch/Splunk/HTTP/...) were
        // previously dropped on update; merge any supplied ones in.
        for (key, value) in extract_extra_destinations(&body, "Update") {
            stream.extra_destinations.insert(key, value);
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

        let config = &body["DeliveryStreamEncryptionConfigurationInput"];
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;
        stream.encryption = Some(EncryptionConfig {
            status: "ENABLED".to_string(),
            key_type: Some(key_type),
            key_arn,
        });
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

    pub fn deliver_records(
        &self,
        account_id: &str,
        region: &str,
        stream_name: &str,
        datas: Vec<Vec<u8>>,
    ) -> Result<(), AwsServiceError> {
        let dest = {
            let accounts = self.state.read();
            let state = accounts
                .get(account_id)
                .ok_or_else(|| not_found(stream_name))?;
            let stream = state
                .streams(region)
                .and_then(|s| s.get(stream_name))
                .ok_or_else(|| not_found(stream_name))?;
            stream.destination.clone()
        };
        let Some(dest) = dest else {
            return Ok(());
        };
        let Some(s3) = &self.s3 else {
            return Ok(());
        };
        let Some(bucket_name) = bucket_name_from_arn(&dest.bucket_arn) else {
            tracing::warn!(
                stream = %stream_name,
                bucket_arn = %dest.bucket_arn,
                "firehose S3 destination has a malformed bucket ARN; dropping records",
            );
            return Err(delivery_failed(
                stream_name,
                "malformed destination bucket ARN",
            ));
        };
        let mut payload = Vec::new();
        for d in datas {
            payload.extend_from_slice(&d);
            if !d.last().map(|b| *b == b'\n').unwrap_or(false) {
                payload.push(b'\n');
            }
        }
        if payload.is_empty() {
            return Ok(());
        }
        let now = Utc::now();
        let prefix = dest.prefix.clone().unwrap_or_default();
        let key = format!(
            "{prefix}{date}/{stream_name}-{ts}-{rand}",
            date = now.format("%Y/%m/%d/%H"),
            ts = now.timestamp(),
            rand = &Uuid::new_v4().to_string()[..8],
        );
        let size = payload.len() as u64;
        let etag = format!("\"{}\"", Uuid::new_v4().simple());
        let bytes = Bytes::from(payload);
        let body = memory_body(bytes.clone());
        let _ = region;
        // Snapshot the durable store handle before taking the S3 state lock so we
        // never hold two locks at once.
        let store = self.s3_store.read().clone();
        let mut s3_state = s3.write();
        let s3 = s3_state.get_or_create(account_id);
        let Some(bucket) = s3.buckets.get_mut(bucket_name) else {
            tracing::warn!(
                stream = %stream_name,
                bucket = %bucket_name,
                "firehose S3 destination bucket does not exist; dropping records",
            );
            return Err(delivery_failed(
                stream_name,
                format!("destination bucket {bucket_name} does not exist"),
            ));
        };
        let object = S3Object {
            key: key.clone(),
            body,
            content_type: "application/octet-stream".to_string(),
            etag,
            size,
            last_modified: now,
            metadata: BTreeMap::new(),
            storage_class: "STANDARD".to_string(),
            tags: BTreeMap::new(),
            acl_grants: Vec::new(),
            acl_owner_id: None,
            parts_count: None,
            part_sizes: None,
            sse_algorithm: None,
            sse_kms_key_id: None,
            bucket_key_enabled: None,
            version_id: None,
            is_delete_marker: false,
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            website_redirect_location: None,
            restore_ongoing: None,
            restore_expiry: None,
            checksum_algorithm: None,
            checksum_value: None,
            lock_mode: None,
            lock_retain_until: None,
            lock_legal_hold: None,
        };
        // Write through to the durable S3 store so delivered records survive a
        // restart. S3 has no state snapshot: on boot it is rebuilt from this
        // store, so an object that only lives in `s3_state` is lost. Mirror the
        // S3 replication path (service/notifications.rs): persist via the store,
        // then swap the in-memory body for the canonical (possibly disk-backed)
        // ref the store returns.
        if let Some(store) = &store {
            let meta = fakecloud_s3::persistence::object_meta_snapshot(&object);
            match store.put_object(bucket_name, &key, None, BodySource::Bytes(bytes), &meta) {
                Ok(returned) => {
                    let mut object = object;
                    object.body = returned;
                    bucket.objects.insert(key, object);
                }
                Err(err) => {
                    tracing::error!(
                        stream = %stream_name,
                        bucket = %bucket_name,
                        %err,
                        "failed to persist firehose delivery to S3 store",
                    );
                    return Err(delivery_failed(
                        stream_name,
                        format!("failed to persist delivery to bucket {bucket_name}: {err}"),
                    ));
                }
            }
        } else {
            // Memory mode: no durable store, keep the in-memory object only.
            bucket.objects.insert(key, object);
        }
        Ok(())
    }
}

/// A record could not be delivered to its configured S3 destination. Firehose
/// surfaces this as `ServiceUnavailableException`; since this emulator delivers
/// synchronously with no retry buffer, undeliverable data is reported honestly
/// rather than acknowledged with a fabricated RecordId.
fn delivery_failed(stream_name: &str, reason: impl std::fmt::Display) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "ServiceUnavailableException",
        format!("could not deliver records for stream {stream_name}: {reason}"),
    )
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
    use http::{HeaderMap, Method};

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

    fn create(svc: &FirehoseService, name: &str) {
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({ "DeliveryStreamName": name }),
        ))
        .unwrap();
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
        assert!(validate_buffering(Some(64), Some(300)).is_ok());
    }

    #[test]
    fn buffering_size_too_small_rejected() {
        assert!(validate_buffering(Some(0), None).is_err());
    }

    #[test]
    fn buffering_size_too_large_rejected() {
        assert!(validate_buffering(Some(200), None).is_err());
    }

    #[test]
    fn buffering_interval_zero_ok() {
        assert!(validate_buffering(None, Some(0)).is_ok());
    }

    #[test]
    fn buffering_interval_below_60_rejected() {
        assert!(validate_buffering(None, Some(30)).is_err());
    }

    #[test]
    fn buffering_interval_above_900_rejected() {
        assert!(validate_buffering(None, Some(1200)).is_err());
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

    /// Build an S3 state seeded with `bucket`, plus a matching on-disk
    /// [`DiskS3Store`] (with the bucket's meta so `load()` surfaces it), and a
    /// FirehoseService wired to both.
    fn firehose_with_disk_s3(
        bucket: &str,
    ) -> (
        FirehoseService,
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
            .with_s3(s3_state)
            .with_s3_store(disk.clone() as std::sync::Arc<dyn S3Store>);
        (svc, disk, tmp)
    }

    fn create_s3_stream(svc: &FirehoseService, name: &str, bucket: &str) {
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": name,
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": format!("arn:aws:s3:::{bucket}"),
                }
            }),
        ))
        .unwrap();
    }

    #[test]
    fn put_record_writes_through_to_durable_s3_store() {
        use fakecloud_persistence::{BodyRef, S3Store};

        let (svc, disk, _tmp) = firehose_with_disk_s3("fh-bucket");
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
        let s3 = svc.s3.as_ref().unwrap().read();
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

    #[test]
    fn put_record_to_missing_bucket_reports_failure_not_fake_success() {
        // Fix: a delivery to a non-existent destination bucket must not be
        // acknowledged with a fabricated RecordId; it is surfaced honestly.
        let (svc, _disk, _tmp) = firehose_with_disk_s3("real-bucket");
        create_s3_stream(&svc, "broken-stream", "ghost-bucket");

        let result = svc.put_record(&request(
            "PutRecord",
            json!({
                "DeliveryStreamName": "broken-stream",
                "Record": {
                    "Data": base64::engine::general_purpose::STANDARD.encode(b"x")
                }
            }),
        ));
        match result {
            Ok(_) => panic!("delivery to a missing bucket must fail, not fake success"),
            Err(err) => assert_eq!(err.code(), "ServiceUnavailableException"),
        }
    }
}
