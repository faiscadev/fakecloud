use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use md5::{Digest, Md5};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::{
    validate_optional_json_range, validate_optional_string_length, validate_string_length,
};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    KinesisConsumer, KinesisRecord, KinesisShard, KinesisSnapshot, KinesisState, KinesisStream,
    SharedKinesisState, KINESIS_SNAPSHOT_SCHEMA_VERSION,
};

const SUPPORTED_ACTIONS: &[&str] = &[
    "AddTagsToStream",
    "CreateStream",
    "DecreaseStreamRetentionPeriod",
    "DeleteResourcePolicy",
    "DeleteStream",
    "DeregisterStreamConsumer",
    "DescribeAccountSettings",
    "DescribeLimits",
    "DescribeStream",
    "DescribeStreamConsumer",
    "DescribeStreamSummary",
    "DisableEnhancedMonitoring",
    "EnableEnhancedMonitoring",
    "GetRecords",
    "GetResourcePolicy",
    "GetShardIterator",
    "IncreaseStreamRetentionPeriod",
    "ListShards",
    "ListStreamConsumers",
    "ListStreams",
    "ListTagsForResource",
    "ListTagsForStream",
    "MergeShards",
    "PutRecord",
    "PutRecords",
    "PutResourcePolicy",
    "RegisterStreamConsumer",
    "RemoveTagsFromStream",
    "SplitShard",
    "StartStreamEncryption",
    "StopStreamEncryption",
    "SubscribeToShard",
    "TagResource",
    "UntagResource",
    "UpdateAccountSettings",
    "UpdateMaxRecordSize",
    "UpdateShardCount",
    "UpdateStreamMode",
    "UpdateStreamWarmThroughput",
];

pub struct KinesisService {
    state: SharedKinesisState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl KinesisService {
    pub fn new(state: SharedKinesisState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes,
    /// with serde + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_kinesis_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Kinesis state when invoked, or
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
                save_kinesis_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current Kinesis state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `KinesisService::save_snapshot` and the
/// CloudFormation provisioner's post-provision persist hook so both route
/// through the same serialize-and-write path.
pub async fn save_kinesis_snapshot(
    state: &SharedKinesisState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = KinesisSnapshot {
        schema_version: KINESIS_SNAPSHOT_SCHEMA_VERSION,
        state: None,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write kinesis snapshot"),
        Err(err) => tracing::error!(%err, "kinesis snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for KinesisService {
    fn service_name(&self) -> &str {
        "kinesis"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(request.action.as_str());
        let result = match request.action.as_str() {
            "CreateStream" => self.create_stream(&request),
            "DescribeStream" => self.describe_stream(&request),
            "DescribeStreamSummary" => self.describe_stream_summary(&request),
            "ListStreams" => self.list_streams(&request),
            "DeleteStream" => self.delete_stream(&request),
            "GetRecords" => self.get_records(&request),
            "GetShardIterator" => self.get_shard_iterator(&request),
            "PutRecord" => self.put_record(&request),
            "PutRecords" => self.put_records(&request),
            "AddTagsToStream" => self.add_tags_to_stream(&request),
            "ListTagsForStream" => self.list_tags_for_stream(&request),
            "RemoveTagsFromStream" => self.remove_tags_from_stream(&request),
            "IncreaseStreamRetentionPeriod" => self.increase_stream_retention_period(&request),
            "DecreaseStreamRetentionPeriod" => self.decrease_stream_retention_period(&request),
            "TagResource" => self.tag_resource(&request),
            "UntagResource" => self.untag_resource(&request),
            "ListTagsForResource" => self.list_tags_for_resource(&request),
            "GetResourcePolicy" => self.get_resource_policy(&request),
            "PutResourcePolicy" => self.put_resource_policy(&request),
            "DeleteResourcePolicy" => self.delete_resource_policy(&request),
            "StartStreamEncryption" => self.start_stream_encryption(&request),
            "StopStreamEncryption" => self.stop_stream_encryption(&request),
            "EnableEnhancedMonitoring" => self.enable_enhanced_monitoring(&request),
            "DisableEnhancedMonitoring" => self.disable_enhanced_monitoring(&request),
            "DescribeAccountSettings" => self.describe_account_settings(&request),
            "UpdateAccountSettings" => self.update_account_settings(&request),
            "DescribeLimits" => self.describe_limits(&request),
            "UpdateStreamMode" => self.update_stream_mode(&request),
            "UpdateStreamWarmThroughput" => self.update_stream_warm_throughput(&request),
            "UpdateMaxRecordSize" => self.update_max_record_size(&request),
            "RegisterStreamConsumer" => self.register_stream_consumer(&request),
            "DeregisterStreamConsumer" => self.deregister_stream_consumer(&request),
            "DescribeStreamConsumer" => self.describe_stream_consumer(&request),
            "ListStreamConsumers" => self.list_stream_consumers(&request),
            "ListShards" => self.list_shards(&request),
            "MergeShards" => self.merge_shards(&request),
            "SplitShard" => self.split_shard(&request),
            "UpdateShardCount" => self.update_shard_count(&request),
            "SubscribeToShard" => self.subscribe_to_shard(&request),
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

// --- Original operations (CreateStream through DecreaseStreamRetentionPeriod) ---

impl KinesisService {
    fn create_stream(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let stream_name = require_stream_name(&body)?;
        let shard_count = body["ShardCount"].as_i64().unwrap_or(1);
        if shard_count <= 0 {
            return Err(invalid_argument("ShardCount must be greater than zero"));
        }
        let shard_count = i32::try_from(shard_count)
            .map_err(|_| invalid_argument("ShardCount must be less than or equal to 2147483647"))?;

        // Honor StreamModeDetails.StreamMode if supplied. AWS accepts
        // PROVISIONED (default) and ON_DEMAND; ON_DEMAND streams ignore
        // ShardCount entirely. Anything unrecognized is a 400.
        let stream_mode = body["StreamModeDetails"]["StreamMode"]
            .as_str()
            .unwrap_or("PROVISIONED")
            .to_string();
        if stream_mode != "PROVISIONED" && stream_mode != "ON_DEMAND" {
            return Err(invalid_argument(
                "StreamMode must be PROVISIONED or ON_DEMAND",
            ));
        }
        // ON_DEMAND seeds a small fixed shard count; the surface still
        // exposes shards through GetShardIterator/GetRecords so callers
        // can read/write while we no-op the per-shard provisioning.
        let effective_shard_count = if stream_mode == "ON_DEMAND" {
            4
        } else {
            shard_count
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        if state.streams.contains_key(stream_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceInUseException",
                format!(
                    "Stream {stream_name} under account {} already exists.",
                    state.account_id
                ),
            ));
        }

        // CreateStream accepts an initial Tags map. Terraform's
        // aws_kinesis_stream sets tags at create time and treats a missing tag
        // on the next read as drift, so they must be persisted here rather than
        // dropped (callers can still AddTagsToStream later).
        let tags: std::collections::BTreeMap<String, String> = body["Tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let stream = KinesisStream {
            stream_name: stream_name.to_string(),
            stream_arn: state.stream_arn(request.region.as_str(), stream_name),
            stream_status: "ACTIVE".to_string(),
            stream_creation_timestamp: Utc::now(),
            retention_period_hours: 24,
            stream_mode,
            encryption_type: "NONE".to_string(),
            key_id: None,
            shard_count: effective_shard_count,
            open_shard_count: effective_shard_count,
            tags,
            shards: build_stream_shards(effective_shard_count),
            next_shard_index: effective_shard_count,
            enhanced_metrics: Vec::new(),
            warm_throughput_mibps: None,
            max_record_size_kib: None,
        };
        state.streams.insert(stream_name.to_string(), stream);

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_stream(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        // Limit is 1..=10000 in the model but a single DescribeStream call
        // returns at most 100 shards; anything above is capped to 100 and
        // `HasMoreShards` signals the caller to page with ExclusiveStartShardId.
        validate_optional_json_range("Limit", &body["Limit"], 1, 10000)?;
        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let stream = state.lookup_stream(&body)?;

        let enhanced_monitoring = if stream.enhanced_metrics.is_empty() {
            json!([{ "ShardLevelMetrics": Vec::<String>::new() }])
        } else {
            json!([{ "ShardLevelMetrics": stream.enhanced_metrics }])
        };

        let limit = body["Limit"]
            .as_i64()
            .map(|n| n.clamp(1, 100))
            .unwrap_or(100) as usize;
        let exclusive_start = body["ExclusiveStartShardId"].as_str();
        let mut shards: Vec<Value> = stream
            .shards
            .iter()
            .filter(|s| {
                exclusive_start
                    .map(|start| s.shard_id.as_str() > start)
                    .unwrap_or(true)
            })
            .take(limit + 1)
            .map(shard_to_json)
            .collect();
        let has_more = shards.len() > limit;
        shards.truncate(limit);

        Ok(AwsResponse::ok_json(json!({
            "StreamDescription": {
                "EncryptionType": stream.encryption_type,
                "EnhancedMonitoring": enhanced_monitoring,
                "HasMoreShards": has_more,
                "KeyId": stream.key_id,
                "RetentionPeriodHours": stream.retention_period_hours,
                "Shards": shards,
                "StreamARN": stream.stream_arn,
                "StreamCreationTimestamp": stream.stream_creation_timestamp.timestamp_millis() as f64 / 1000.0,
                "StreamModeDetails": {
                    "StreamMode": stream.stream_mode,
                },
                "StreamName": stream.stream_name,
                "StreamStatus": stream.stream_status
            }
        })))
    }

    fn describe_stream_summary(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let stream = state.lookup_stream(&body)?;
        let consumer_count = state
            .consumers
            .values()
            .filter(|c| c.stream_arn == stream.stream_arn)
            .count();

        // StreamDescriptionSummary carries EnhancedMonitoring just like the
        // full DescribeStream output. The Terraform aws_kinesis_stream data
        // source reads `shard_level_metrics` from here (it calls
        // DescribeStreamSummary), so it must be present.
        let enhanced_monitoring = if stream.enhanced_metrics.is_empty() {
            json!([{ "ShardLevelMetrics": Vec::<String>::new() }])
        } else {
            json!([{ "ShardLevelMetrics": stream.enhanced_metrics }])
        };

        let mut summary = json!({
            "ConsumerCount": consumer_count,
            "EncryptionType": stream.encryption_type,
            "EnhancedMonitoring": enhanced_monitoring,
            "KeyId": stream.key_id.as_deref().unwrap_or_default(),
            "OpenShardCount": stream.open_shard_count,
            "RetentionPeriodHours": stream.retention_period_hours,
            "StreamARN": stream.stream_arn,
            "StreamCreationTimestamp": stream.stream_creation_timestamp.timestamp_millis() as f64 / 1000.0,
            "StreamModeDetails": {
                "StreamMode": stream.stream_mode,
            },
            "StreamName": stream.stream_name,
            "StreamStatus": stream.stream_status
        });
        // WarmThroughput / MaxRecordSizeInKiB are only present after the caller
        // configures them via UpdateStreamWarmThroughput / UpdateMaxRecordSize;
        // they were previously omitted, so those updates never read back.
        if let Some(warm) = stream.warm_throughput_mibps {
            summary["WarmThroughput"] = json!({
                "TargetMiBps": warm,
                "CurrentMiBps": warm,
            });
        }
        if let Some(max_size) = stream.max_record_size_kib {
            summary["MaxRecordSizeInKiB"] = json!(max_size);
        }

        Ok(AwsResponse::ok_json(json!({
            "StreamDescriptionSummary": summary
        })))
    }

    fn list_streams(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let exclusive_start = body["ExclusiveStartStreamName"].as_str();
        validate_optional_string_length("ExclusiveStartStreamName", exclusive_start, 1, 128)?;
        validate_optional_string_length("NextToken", body["NextToken"].as_str(), 1, 1048576)?;
        // Smithy: ListStreamsInputLimit is @range(1, 10000). Real Kinesis caps at 100
        // server-side per the operation docs, but the wire-layer accepts up to 10000.
        validate_optional_json_range("Limit", &body["Limit"], 1, 10000)?;
        let limit = body["Limit"].as_i64().unwrap_or(100).min(100);

        // NextToken (an opaque base64 of the last returned stream name) takes
        // precedence over ExclusiveStartStreamName, matching AWS where the two
        // are alternative ways to resume the same listing.
        let resume_after: Option<String> = match body["NextToken"].as_str() {
            Some(tok) => base64::engine::general_purpose::STANDARD
                .decode(tok)
                .ok()
                .and_then(|b| String::from_utf8(b).ok()),
            None => exclusive_start.map(String::from),
        };

        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let mut names: Vec<String> = state.streams.keys().cloned().collect();
        names.sort();

        // Resume at the first name strictly greater than the cursor. The cursor
        // (last stream name of the previous page) is a stable total-ordering key
        // over the sorted names, so if the stream it named was deleted between
        // pages we still resume at the right offset instead of restarting from 0
        // and re-emitting the whole list (which an exact-match lookup did).
        let start = resume_after
            .as_deref()
            .map(|cursor| {
                names
                    .iter()
                    .position(|candidate| candidate.as_str() > cursor)
                    .unwrap_or(names.len())
            })
            .unwrap_or(0);
        let remaining = names.len().saturating_sub(start);
        let page_len = remaining.min(limit as usize);
        let has_more_streams = remaining > page_len;
        let selected: Vec<String> = names.into_iter().skip(start).take(page_len).collect();
        let next_token = if has_more_streams {
            selected
                .last()
                .map(|name| base64::engine::general_purpose::STANDARD.encode(name))
        } else {
            None
        };
        let summaries: Vec<Value> = selected
            .iter()
            .filter_map(|name| state.streams.get(name))
            .map(|s| {
                json!({
                    "StreamName": s.stream_name,
                    "StreamARN": s.stream_arn,
                    "StreamStatus": s.stream_status,
                    "StreamModeDetails": {
                        "StreamMode": s.stream_mode,
                    },
                    "StreamCreationTimestamp": s.stream_creation_timestamp.timestamp_millis() as f64 / 1000.0,
                })
            })
            .collect();

        let mut response = json!({
            "HasMoreStreams": has_more_streams,
            "StreamNames": selected,
            "StreamSummaries": summaries
        });
        if let Some(token) = next_token {
            response["NextToken"] = json!(token);
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn delete_stream(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let stream = state.streams.remove(&stream_name);
        if stream.is_none() {
            return Err(stream_not_found(&state.account_id, &stream_name));
        }
        let stream_arn = state.stream_arn(request.region.as_str(), &stream_name);
        state.consumers.retain(|_, c| c.stream_arn != stream_arn);

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn add_tags_to_stream(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        let tags = body["Tags"]
            .as_object()
            .ok_or_else(|| invalid_argument("Tags must be an object"))?;
        for (key, value) in tags {
            if let Some(value) = value.as_str() {
                stream.tags.insert(key.clone(), value.to_string());
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_shard_iterator(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let shard_id = require_shard_id(&body)?;
        let iterator_type = body["ShardIteratorType"]
            .as_str()
            .ok_or_else(|| invalid_argument("ShardIteratorType is required"))?;

        let stream = state
            .streams
            .get(&stream_name)
            .ok_or_else(|| stream_not_found(&state.account_id, &stream_name))?;
        let shard = stream
            .shards
            .iter()
            .find(|candidate| candidate.shard_id == shard_id)
            .ok_or_else(|| invalid_argument("ShardId is invalid"))?;

        let next_record_index = shard_iterator_start_index(shard, iterator_type, &body)?;
        let iterator = state.insert_iterator(&stream_name, shard_id, next_record_index);

        Ok(AwsResponse::ok_json(json!({
            "ShardIterator": iterator,
        })))
    }

    fn get_records(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let iterator = body["ShardIterator"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_argument("ShardIterator is required"))?
            .to_string();
        // GetRecords Limit targets GetRecordsInputLimit @range(1, 10000).
        // AWS rejects out-of-range values with InvalidArgumentException
        // (not ValidationException — that's why this uses the crate's
        // Kinesis-specific error rather than validate_optional_json_range).
        if !body["Limit"].is_null() {
            let n = body["Limit"]
                .as_i64()
                .ok_or_else(|| invalid_argument("Limit must be an integer between 1 and 10000"))?;
            if !(1..=10_000).contains(&n) {
                return Err(invalid_argument(format!(
                    "1 validation error detected: Value '{n}' at 'limit' failed to \
                     satisfy constraint: Member must have value less than or equal to \
                     10000 and greater than or equal to 1"
                )));
            }
        }
        let limit = body["Limit"].as_u64().unwrap_or(10_000) as usize;

        let lease = state
            .iterators
            .get(&iterator)
            .cloned()
            .ok_or_else(expired_iterator)?;
        if lease.expires_at < Utc::now() {
            state.iterators.remove(&iterator);
            return Err(expired_iterator());
        }

        let stream = state
            .streams
            .get(&lease.stream_name)
            .ok_or_else(|| stream_not_found(&state.account_id, &lease.stream_name))?;
        let shard = stream
            .shards
            .iter()
            .find(|candidate| candidate.shard_id == lease.shard_id)
            .ok_or_else(|| invalid_argument("ShardId is invalid"))?;

        // Drop records past retention. Records are stored in arrival
        // order so we can stop scanning at the first one within window
        // — but the iterator slot might point into the expired prefix,
        // so advance past it before reading.
        let retention_cutoff =
            Utc::now() - chrono::Duration::hours(stream.retention_period_hours as i64);
        let mut start_index = lease.next_record_index;
        while start_index < shard.records.len()
            && shard.records[start_index].approximate_arrival_timestamp < retention_cutoff
        {
            start_index += 1;
        }
        // AWS caps a single GetRecords response at min(Limit records, 10 MiB of
        // data), whichever comes first, and lets NextShardIterator resume after
        // the truncation point. Slicing purely by record COUNT (the old
        // behaviour) could return a batch far larger than 10 MiB. Accumulate
        // record byte sizes and stop before the batch would exceed the cap —
        // but always include at least the first record so a full shard never
        // yields an empty batch with a live iterator (which stalls consumers).
        const MAX_GET_RECORDS_BYTES: usize = 10 * 1024 * 1024;
        let mut end_index = start_index;
        let mut accumulated_bytes: usize = 0;
        while end_index < shard.records.len() && end_index - start_index < limit {
            let record = &shard.records[end_index];
            let record_bytes = record.data.len() + record.partition_key.len();
            if end_index > start_index
                && accumulated_bytes.saturating_add(record_bytes) > MAX_GET_RECORDS_BYTES
            {
                break;
            }
            accumulated_bytes = accumulated_bytes.saturating_add(record_bytes);
            end_index += 1;
        }
        let records: Vec<Value> = shard.records[start_index..end_index]
            .iter()
            .map(|record| {
                json!({
                    "ApproximateArrivalTimestamp": record.approximate_arrival_timestamp.timestamp_millis() as f64 / 1000.0,
                    "Data": base64::engine::general_purpose::STANDARD.encode(&record.data),
                    "PartitionKey": record.partition_key,
                    "SequenceNumber": record.sequence_number,
                })
            })
            .collect();

        // Capture the shard fields before the borrow ends so the mutable
        // `insert_iterator` call below can run.
        let total_records = shard.records.len();
        let shard_closed = !shard.is_open;

        // When a split/merged shard is fully drained, AWS returns `ChildShards`
        // alongside the null NextShardIterator so KCL / Lambda ESM can fan out
        // to the child shard(s) instead of stalling at the parent.
        let child_shards: Vec<Value> = if shard_closed && end_index >= total_records {
            stream
                .shards
                .iter()
                .filter(|c| {
                    c.parent_shard_id.as_deref() == Some(lease.shard_id.as_str())
                        || c.adjacent_parent_shard_id.as_deref() == Some(lease.shard_id.as_str())
                })
                .map(|c| {
                    let mut parents = Vec::new();
                    if let Some(p) = &c.parent_shard_id {
                        parents.push(p.clone());
                    }
                    if let Some(p) = &c.adjacent_parent_shard_id {
                        parents.push(p.clone());
                    }
                    json!({
                        "ShardId": c.shard_id,
                        "ParentShards": parents,
                        "HashKeyRange": {
                            "StartingHashKey": c.starting_hash_key,
                            "EndingHashKey": c.ending_hash_key,
                        },
                    })
                })
                .collect()
        } else {
            Vec::new()
        };

        // MillisBehindLatest is the real time gap between the last record
        // returned in this batch and the newest (tip) record in the shard —
        // not a 0/1 flag. Consumers (Lambda ESM IteratorAge alarms, KCL
        // catch-up detection, Flink watermarks) use it to gauge how far
        // behind they are. Zero when caught up or the batch is empty.
        let millis_behind_latest = if end_index >= total_records || end_index == 0 {
            0
        } else {
            let last_returned = shard.records[end_index - 1].approximate_arrival_timestamp;
            let tip = shard.records[total_records - 1].approximate_arrival_timestamp;
            (tip - last_returned).num_milliseconds().max(0)
        };

        // A shard closed by SplitShard/MergeShards and then fully read returns a
        // null NextShardIterator, signalling the consumer to move on to the
        // child shard(s). Returning a live iterator forever (the old behaviour)
        // makes KCL-style consumers loop on the parent shard and never advance
        // past the split/merge (bug-audit 2026-06-20, 1.7).
        let next_iterator = if shard_closed && end_index >= total_records {
            Value::Null
        } else {
            json!(state.insert_iterator(&lease.stream_name, &lease.shard_id, end_index))
        };

        let mut response = json!({
            "MillisBehindLatest": millis_behind_latest,
            "NextShardIterator": next_iterator,
            "Records": records,
        });
        if !child_shards.is_empty() {
            response["ChildShards"] = Value::Array(child_shards);
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn put_record(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        // Drop records past retention on write so they don't accumulate in
        // memory (and in every snapshot) unbounded.
        state.trim_expired_records();
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        let partition_key = require_partition_key(&body)?;
        let data = decode_record_data(&body["Data"])?;
        // Data + PartitionKey must fit the per-record ceiling (1 MiB, or the
        // stream's configured MaxRecordSizeInKiB). AWS rejects an oversized
        // single record with ValidationException before it is written.
        let max_record_bytes = effective_max_record_bytes(stream);
        if data.len() + partition_key.len() > max_record_bytes {
            return Err(validation_exception(format!(
                "Record size (Data + PartitionKey) exceeds the {max_record_bytes}-byte per-record limit"
            )));
        }
        let encryption_type = stream.encryption_type.clone();
        let explicit_hash_key = body["ExplicitHashKey"].as_str();
        let shard = select_shard_mut(stream, partition_key, explicit_hash_key)?;
        let sequence_number = append_record(shard, partition_key, data);
        let shard_id = shard.shard_id.clone();

        Ok(AwsResponse::ok_json(json!({
            "EncryptionType": encryption_type,
            "SequenceNumber": sequence_number,
            "ShardId": shard_id,
        })))
    }

    fn put_records(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        // Drop records past retention on write so they don't accumulate in
        // memory (and in every snapshot) unbounded.
        state.trim_expired_records();
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        let entries = body["Records"]
            .as_array()
            .ok_or_else(|| invalid_argument("Records must be an array"))?;
        if entries.is_empty() {
            return Err(invalid_argument("Records must not be empty"));
        }
        // Whole-request limits AWS rejects up front with ValidationException,
        // before any record is written: at most 500 records, and at most 5 MiB
        // (or the stream's larger ceiling) of aggregate Data + PartitionKey.
        if entries.len() > MAX_PUT_RECORDS_COUNT {
            return Err(validation_exception(format!(
                "1 validation error detected: Value at 'records' failed to satisfy \
                 constraint: Member must have length less than or equal to {MAX_PUT_RECORDS_COUNT}"
            )));
        }
        let max_batch_bytes = effective_max_batch_bytes(stream);
        let aggregate_bytes: usize = entries.iter().map(put_records_entry_size).sum();
        if aggregate_bytes > max_batch_bytes {
            return Err(validation_exception(format!(
                "PutRecords aggregate payload ({aggregate_bytes} bytes) exceeds the \
                 {max_batch_bytes}-byte per-request limit"
            )));
        }

        // A record that fails validation (e.g. empty PartitionKey) is reported
        // as a PER-RECORD failure with FailedRecordCount incremented — the
        // conformance baseline recorded from real AWS (checksum 27e5bb6b) shows
        // PutRecords returns a per-record InvalidArgumentException rather than
        // rejecting the whole request. (Report finding 1.7 was a false
        // positive, verified against the recorded AWS behavior.)
        let mut failed_record_count = 0;
        let mut records = Vec::with_capacity(entries.len());
        for entry in entries {
            match put_records_entry(stream, entry) {
                Ok((shard_id, sequence_number)) => records.push(json!({
                    "SequenceNumber": sequence_number,
                    "ShardId": shard_id,
                })),
                Err(error_message) => {
                    failed_record_count += 1;
                    records.push(json!({
                        "ErrorCode": "InvalidArgumentException",
                        "ErrorMessage": error_message,
                    }));
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "EncryptionType": stream.encryption_type,
            "FailedRecordCount": failed_record_count,
            "Records": records,
        })))
    }

    fn list_tags_for_stream(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let stream = state.lookup_stream(&body)?;

        let tags: Vec<Value> = stream
            .tags
            .iter()
            .map(|(key, value)| json!({ "Key": key, "Value": value }))
            .collect();

        Ok(AwsResponse::ok_json(json!({
            "HasMoreTags": false,
            "Tags": tags
        })))
    }

    fn remove_tags_from_stream(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        let tag_keys = body["TagKeys"]
            .as_array()
            .ok_or_else(|| invalid_argument("TagKeys must be an array"))?;
        for key in tag_keys.iter().filter_map(|value| value.as_str()) {
            stream.tags.remove(key);
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn increase_stream_retention_period(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.update_retention_period(request, true)
    }

    fn decrease_stream_retention_period(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.update_retention_period(request, false)
    }

    fn update_retention_period(
        &self,
        request: &AwsRequest,
        increasing: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let hours = body["RetentionPeriodHours"]
            .as_i64()
            .ok_or_else(|| invalid_argument("RetentionPeriodHours is required"))?;
        if !(24..=8760).contains(&hours) {
            return Err(invalid_argument(
                "RetentionPeriodHours must be between 24 and 8760",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        // Real AWS Kinesis treats `IncreaseStreamRetentionPeriod` with the
        // current value as a no-op (despite what the API docs say) — the
        // upstream `aws_kinesis_stream` provider unconditionally calls
        // `IncreaseStreamRetentionPeriod(24)` on every create regardless
        // of whether the user changed retention_period, and the test
        // suite passes against real AWS. Strictly less-than is still an
        // error for `IncreaseStreamRetentionPeriod`, and strictly
        // greater-than is still an error for `DecreaseStreamRetentionPeriod`.
        if increasing && hours < stream.retention_period_hours as i64 {
            return Err(invalid_argument(
                "RetentionPeriodHours must be greater than the current retention period",
            ));
        }
        if !increasing && hours > stream.retention_period_hours as i64 {
            return Err(invalid_argument(
                "RetentionPeriodHours must be less than the current retention period",
            ));
        }

        stream.retention_period_hours = hours as i32;
        Ok(AwsResponse::ok_json(json!({})))
    }

    // --- Batch 1: Tags v2 + Resource Policy ---

    fn tag_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let resource_arn = require_resource_arn(&body)?;
        let tags = body["Tags"]
            .as_object()
            .ok_or_else(|| invalid_argument("Tags must be a map"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = state
            .stream_name_from_arn(resource_arn)
            .ok_or_else(|| resource_not_found_arn(resource_arn))?;
        let stream = state.streams.get_mut(&stream_name).unwrap();
        for (key, value) in tags {
            if let Some(value) = value.as_str() {
                stream.tags.insert(key.clone(), value.to_string());
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let resource_arn = require_resource_arn(&body)?;
        let tag_keys = body["TagKeys"]
            .as_array()
            .ok_or_else(|| invalid_argument("TagKeys must be a list"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = state
            .stream_name_from_arn(resource_arn)
            .ok_or_else(|| resource_not_found_arn(resource_arn))?;
        let stream = state.streams.get_mut(&stream_name).unwrap();
        for key in tag_keys.iter().filter_map(|v| v.as_str()) {
            stream.tags.remove(key);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let resource_arn = require_resource_arn(&body)?;

        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let stream_name = state
            .stream_name_from_arn(resource_arn)
            .ok_or_else(|| resource_not_found_arn(resource_arn))?;
        let stream = state.streams.get(&stream_name).unwrap();
        let tags: Vec<Value> = stream
            .tags
            .iter()
            .map(|(key, value)| json!({ "Key": key, "Value": value }))
            .collect();

        Ok(AwsResponse::ok_json(json!({ "Tags": tags })))
    }

    fn get_resource_policy(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let resource_arn = require_resource_arn(&body)?;

        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        state
            .stream_name_from_arn(resource_arn)
            .ok_or_else(|| resource_not_found_arn(resource_arn))?;
        let policy = state
            .resource_policies
            .get(resource_arn)
            .cloned()
            .unwrap_or_default();

        Ok(AwsResponse::ok_json(json!({ "Policy": policy })))
    }

    fn put_resource_policy(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let resource_arn = require_resource_arn(&body)?;
        let policy = body["Policy"]
            .as_str()
            .ok_or_else(|| invalid_argument("Policy is required"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        state
            .stream_name_from_arn(resource_arn)
            .ok_or_else(|| resource_not_found_arn(resource_arn))?;
        state
            .resource_policies
            .insert(resource_arn.to_string(), policy.to_string());

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_resource_policy(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let resource_arn = require_resource_arn(&body)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        state
            .stream_name_from_arn(resource_arn)
            .ok_or_else(|| resource_not_found_arn(resource_arn))?;
        state.resource_policies.remove(resource_arn);

        Ok(AwsResponse::ok_json(json!({})))
    }

    // --- Batch 2: Encryption + Enhanced Monitoring ---

    fn start_stream_encryption(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let encryption_type = body["EncryptionType"]
            .as_str()
            .ok_or_else(|| invalid_argument("EncryptionType is required"))?;
        if encryption_type != "KMS" {
            return Err(invalid_argument(format!(
                "EncryptionType must be KMS for StartStreamEncryption, got {encryption_type}"
            )));
        }
        let key_id = body["KeyId"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("KeyId is required"))?;
        validate_optional_string_length("KeyId", Some(key_id), 1, 2048)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;
        stream.encryption_type = encryption_type.to_string();
        stream.key_id = Some(key_id.to_string());

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn stop_stream_encryption(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let _encryption_type = body["EncryptionType"]
            .as_str()
            .ok_or_else(|| invalid_argument("EncryptionType is required"))?;
        let _key_id = body["KeyId"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("KeyId is required"))?;
        validate_optional_string_length("KeyId", body["KeyId"].as_str(), 1, 2048)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;
        stream.encryption_type = "NONE".to_string();
        stream.key_id = None;

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn enable_enhanced_monitoring(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let metrics = body["ShardLevelMetrics"]
            .as_array()
            .ok_or_else(|| invalid_argument("ShardLevelMetrics is required"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream_arn = state.stream_arn(request.region.as_str(), &stream_name);
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        let current: Vec<String> = stream.enhanced_metrics.clone();
        let desired: Vec<String> = metrics
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        for metric in &desired {
            if !SHARD_LEVEL_METRICS.contains(&metric.as_str()) {
                return Err(invalid_argument(format!(
                    "Invalid ShardLevelMetrics value: {metric}"
                )));
            }
        }

        if desired.contains(&"ALL".to_string()) {
            stream.enhanced_metrics = SHARD_LEVEL_METRICS
                .iter()
                .filter(|m| **m != "ALL")
                .map(|s| s.to_string())
                .collect();
        } else {
            for metric in &desired {
                if !stream.enhanced_metrics.contains(metric) {
                    stream.enhanced_metrics.push(metric.clone());
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "StreamName": stream_name,
            "StreamARN": stream_arn,
            "CurrentShardLevelMetrics": current,
            "DesiredShardLevelMetrics": desired,
        })))
    }

    fn disable_enhanced_monitoring(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let metrics = body["ShardLevelMetrics"]
            .as_array()
            .ok_or_else(|| invalid_argument("ShardLevelMetrics is required"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream_arn = state.stream_arn(request.region.as_str(), &stream_name);
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        let current: Vec<String> = stream.enhanced_metrics.clone();
        let desired: Vec<String> = metrics
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        for metric in &desired {
            if !SHARD_LEVEL_METRICS.contains(&metric.as_str()) {
                return Err(invalid_argument(format!(
                    "Invalid ShardLevelMetrics value: {metric}"
                )));
            }
        }

        if desired.contains(&"ALL".to_string()) {
            stream.enhanced_metrics.clear();
        } else {
            stream.enhanced_metrics.retain(|m| !desired.contains(m));
        }

        Ok(AwsResponse::ok_json(json!({
            "StreamName": stream_name,
            "StreamARN": stream_arn,
            "CurrentShardLevelMetrics": current,
            "DesiredShardLevelMetrics": desired,
        })))
    }

    // --- Batch 3: Account/Limits + Stream Config ---

    fn describe_account_settings(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        Ok(AwsResponse::ok_json(json!({
            "MinimumThroughputBillingCommitment": {
                "Status": state.billing_commitment_status,
            }
        })))
    }

    fn update_account_settings(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let commitment = &body["MinimumThroughputBillingCommitment"];
        let status = commitment["Status"].as_str().ok_or_else(|| {
            invalid_argument("MinimumThroughputBillingCommitment.Status is required")
        })?;
        if status != "ENABLED" && status != "DISABLED" {
            return Err(invalid_argument(format!(
                "Invalid MinimumThroughputBillingCommitment status: {status}"
            )));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        state.billing_commitment_status = status.to_string();

        Ok(AwsResponse::ok_json(json!({
            "MinimumThroughputBillingCommitment": {
                "Status": status,
            }
        })))
    }

    fn describe_limits(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let open_shard_count: i32 = state.streams.values().map(|s| s.open_shard_count).sum();
        let on_demand_count = state
            .streams
            .values()
            .filter(|s| s.stream_mode == "ON_DEMAND")
            .count() as i32;

        Ok(AwsResponse::ok_json(json!({
            "ShardLimit": state.shard_limit,
            "OpenShardCount": open_shard_count,
            "OnDemandStreamCount": on_demand_count,
            "OnDemandStreamCountLimit": state.on_demand_stream_count_limit,
        })))
    }

    fn update_stream_mode(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let stream_arn = body["StreamARN"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("StreamARN is required"))?;
        validate_string_length("StreamARN", stream_arn, 1, 2048)?;

        let stream_mode = body["StreamModeDetails"]["StreamMode"]
            .as_str()
            .ok_or_else(|| invalid_argument("StreamModeDetails.StreamMode is required"))?;
        if stream_mode != "PROVISIONED" && stream_mode != "ON_DEMAND" {
            return Err(invalid_argument(format!(
                "Invalid StreamMode: {stream_mode}"
            )));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = state
            .stream_name_from_arn(stream_arn)
            .ok_or_else(|| resource_not_found_arn(stream_arn))?;
        let stream = state.streams.get_mut(&stream_name).unwrap();
        stream.stream_mode = stream_mode.to_string();

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn update_stream_warm_throughput(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let warm_mibps = body["WarmThroughputMiBps"]
            .as_i64()
            .ok_or_else(|| invalid_argument("WarmThroughputMiBps is required"))?;
        if warm_mibps < 0 {
            return Err(invalid_argument("WarmThroughputMiBps must be >= 0"));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream_arn = state.stream_arn(request.region.as_str(), &stream_name);
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;
        stream.warm_throughput_mibps = Some(warm_mibps);

        Ok(AwsResponse::ok_json(json!({
            "StreamARN": stream_arn,
            "StreamName": stream_name,
            "WarmThroughput": {
                "TargetMiBps": warm_mibps,
                "CurrentMiBps": warm_mibps,
            }
        })))
    }

    fn update_max_record_size(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        validate_optional_json_range(
            "MaxRecordSizeInKiB",
            &body["MaxRecordSizeInKiB"],
            1024,
            10240,
        )?;
        let max_size = body["MaxRecordSizeInKiB"]
            .as_i64()
            .ok_or_else(|| invalid_argument("MaxRecordSizeInKiB is required"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_arn = body["StreamARN"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("StreamARN is required"))?;
        let stream_name = state
            .stream_name_from_arn(stream_arn)
            .ok_or_else(|| resource_not_found_arn(stream_arn))?;
        let stream = state.streams.get_mut(&stream_name).unwrap();
        stream.max_record_size_kib = Some(max_size);

        Ok(AwsResponse::ok_json(json!({})))
    }

    // --- Batch 4: Consumer Management ---

    fn register_stream_consumer(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let stream_arn = body["StreamARN"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("StreamARN is required"))?;
        validate_string_length("StreamARN", stream_arn, 1, 2048)?;
        let consumer_name = body["ConsumerName"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("ConsumerName is required"))?;
        validate_string_length("ConsumerName", consumer_name, 1, 128)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let _stream_name = state
            .stream_name_from_arn(stream_arn)
            .ok_or_else(|| resource_not_found_arn(stream_arn))?;

        let now = Utc::now();
        let consumer_arn = format!(
            "{}/consumer/{}:{}",
            stream_arn,
            consumer_name,
            now.timestamp()
        );

        let exists = state
            .consumers
            .values()
            .any(|c| c.stream_arn == stream_arn && c.consumer_name == consumer_name);
        if exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceInUseException",
                format!("Consumer {consumer_name} already exists on stream."),
            ));
        }

        let consumer = KinesisConsumer {
            consumer_name: consumer_name.to_string(),
            consumer_arn: consumer_arn.clone(),
            consumer_status: "ACTIVE".to_string(),
            consumer_creation_timestamp: now,
            stream_arn: stream_arn.to_string(),
        };
        state.consumers.insert(consumer_arn.clone(), consumer);

        Ok(AwsResponse::ok_json(json!({
            "Consumer": {
                "ConsumerName": consumer_name,
                "ConsumerARN": consumer_arn,
                "ConsumerStatus": "ACTIVE",
                "ConsumerCreationTimestamp": now.timestamp_millis() as f64 / 1000.0,
            }
        })))
    }

    fn deregister_stream_consumer(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        let consumer_arn = if let Some(arn) = body["ConsumerARN"].as_str().filter(|v| !v.is_empty())
        {
            validate_string_length("ConsumerARN", arn, 1, 2048)?;
            arn.to_string()
        } else {
            let stream_arn = body["StreamARN"]
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    invalid_argument("Either ConsumerARN or StreamARN+ConsumerName is required")
                })?;
            let consumer_name = body["ConsumerName"]
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| invalid_argument("ConsumerName is required with StreamARN"))?;
            state
                .consumers
                .values()
                .find(|c| c.stream_arn == stream_arn && c.consumer_name == consumer_name)
                .map(|c| c.consumer_arn.clone())
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ResourceNotFoundException",
                        format!("Consumer {consumer_name} not found."),
                    )
                })?
        };

        state
            .consumers
            .remove(&consumer_arn)
            .ok_or_else(|| resource_not_found_arn(&consumer_arn))?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_stream_consumer(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

        let consumer = if let Some(arn) = body["ConsumerARN"].as_str().filter(|v| !v.is_empty()) {
            validate_string_length("ConsumerARN", arn, 1, 2048)?;
            state
                .consumers
                .get(arn)
                .ok_or_else(|| resource_not_found_arn(arn))?
        } else {
            let stream_arn = body["StreamARN"]
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    invalid_argument("Either ConsumerARN or StreamARN+ConsumerName is required")
                })?;
            let consumer_name = body["ConsumerName"]
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| invalid_argument("ConsumerName is required with StreamARN"))?;
            state
                .consumers
                .values()
                .find(|c| c.stream_arn == stream_arn && c.consumer_name == consumer_name)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ResourceNotFoundException",
                        format!("Consumer {consumer_name} not found."),
                    )
                })?
        };

        Ok(AwsResponse::ok_json(json!({
            "ConsumerDescription": {
                "ConsumerName": consumer.consumer_name,
                "ConsumerARN": consumer.consumer_arn,
                "ConsumerStatus": consumer.consumer_status,
                "ConsumerCreationTimestamp": consumer.consumer_creation_timestamp.timestamp_millis() as f64 / 1000.0,
                "StreamARN": consumer.stream_arn,
            }
        })))
    }

    fn list_stream_consumers(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let stream_arn = body["StreamARN"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("StreamARN is required"))?;
        validate_string_length("StreamARN", stream_arn, 1, 2048)?;
        validate_optional_string_length("NextToken", body["NextToken"].as_str(), 1, 1048576)?;
        validate_optional_json_range("MaxResults", &body["MaxResults"], 1, 10000)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(100) as usize;

        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let mut consumers: Vec<Value> = state
            .consumers
            .values()
            .filter(|c| c.stream_arn == stream_arn)
            .map(|c| {
                json!({
                    "ConsumerName": c.consumer_name,
                    "ConsumerARN": c.consumer_arn,
                    "ConsumerStatus": c.consumer_status,
                    "ConsumerCreationTimestamp": c.consumer_creation_timestamp.timestamp_millis() as f64 / 1000.0,
                })
            })
            .collect();
        consumers.sort_by(|a, b| a["ConsumerName"].as_str().cmp(&b["ConsumerName"].as_str()));

        let next_token = body["NextToken"].as_str();
        if let Some(token) = next_token {
            if let Some(pos) = consumers
                .iter()
                .position(|c| c["ConsumerName"].as_str() == Some(token))
            {
                consumers = consumers.split_off(pos + 1);
            }
        }

        let has_more = consumers.len() > max_results;
        consumers.truncate(max_results);
        let response_token = if has_more {
            consumers
                .last()
                .and_then(|c| c["ConsumerName"].as_str())
                .map(|s| json!(s))
        } else {
            None
        };

        let mut resp = json!({ "Consumers": consumers });
        if let Some(token) = response_token {
            resp["NextToken"] = token;
        }

        Ok(AwsResponse::ok_json(resp))
    }

    // --- Batch 5: Shard Management ---

    fn list_shards(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        validate_optional_string_length("NextToken", body["NextToken"].as_str(), 1, 1048576)?;
        validate_optional_json_range("MaxResults", &body["MaxResults"], 1, 10000)?;
        // AWS returns up to 1000 shards when MaxResults is omitted (the max the
        // caller may request is 10000). Defaulting to 10000 over-returned.
        let max_results = body["MaxResults"].as_i64().unwrap_or(1000) as usize;

        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let stream = state.lookup_stream(&body)?;

        // Resume position: a NextToken (opaque cursor) takes precedence over
        // ExclusiveStartShardId. AWS forbids mixing NextToken with the other
        // filter params; at minimum we decode it back into the shard id to
        // resume from the right place so SDK paginators advance.
        let exclusive_start: Option<String> = match body["NextToken"].as_str() {
            Some(token) => Some(decode_list_shards_token(token)?),
            None => body["ExclusiveStartShardId"].as_str().map(str::to_string),
        };

        // Apply an optional ShardFilter (AT_LATEST / AT_TRIM_HORIZON /
        // AT_TIMESTAMP / AFTER_SHARD_ID / FROM_*) before paginating. KCL
        // relies on this to fan out only to the relevant shards.
        let shard_filter = body.get("ShardFilter").filter(|f| f.is_object());
        let mut filtered_shards: Vec<&KinesisShard> = Vec::new();
        for shard in &stream.shards {
            if let Some(filter) = shard_filter {
                if !shard_matches_filter(shard, filter)? {
                    continue;
                }
            }
            filtered_shards.push(shard);
        }

        let mut shards: Vec<Value> = filtered_shards
            .into_iter()
            .filter(|s| {
                if let Some(start_id) = exclusive_start.as_deref() {
                    s.shard_id.as_str() > start_id
                } else {
                    true
                }
            })
            .take(max_results + 1)
            .map(shard_to_json)
            .collect();

        let has_more = shards.len() > max_results;
        shards.truncate(max_results);

        let mut resp = json!({ "Shards": shards });
        if has_more {
            if let Some(last_id) = shards.last().and_then(|s| s["ShardId"].as_str()) {
                resp["NextToken"] = json!(encode_list_shards_token(last_id));
            }
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn merge_shards(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let shard_to_merge = body["ShardToMerge"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("ShardToMerge is required"))?;
        let adjacent_shard = body["AdjacentShardToMerge"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("AdjacentShardToMerge is required"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        if shard_to_merge == adjacent_shard {
            return Err(invalid_argument(
                "ShardToMerge and AdjacentShardToMerge must be different shards",
            ));
        }
        let shard1_idx = find_open_shard_idx(&stream.shards, shard_to_merge)?;
        let shard2_idx = find_open_shard_idx(&stream.shards, adjacent_shard)?;

        let (start1, end1) = shard_hash_range(&stream.shards[shard1_idx]);
        let (start2, end2) = shard_hash_range(&stream.shards[shard2_idx]);
        let adj1 = end1.checked_add(1) == Some(start2);
        let adj2 = end2.checked_add(1) == Some(start1);
        if !adj1 && !adj2 {
            return Err(invalid_argument(
                "ShardToMerge and AdjacentShardToMerge must be adjacent shards",
            ));
        }

        let starting = start1.min(start2);
        let ending = end1.max(end2);

        stream.shards[shard1_idx].is_open = false;
        stream.shards[shard2_idx].is_open = false;

        let new_id = next_shard_id(stream);
        stream.shards.push(KinesisShard {
            shard_id: new_id,
            starting_hash_key: starting.to_string(),
            ending_hash_key: ending.to_string(),
            parent_shard_id: Some(shard_to_merge.to_string()),
            adjacent_parent_shard_id: Some(adjacent_shard.to_string()),
            is_open: true,
            next_sequence_number: 1,
            records: Vec::new(),
        });

        stream.shard_count = stream.shards.len() as i32;
        stream.open_shard_count = stream.shards.iter().filter(|s| s.is_open).count() as i32;

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn split_shard(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let shard_to_split = body["ShardToSplit"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("ShardToSplit is required"))?;
        let new_starting_hash_key = body["NewStartingHashKey"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("NewStartingHashKey is required"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        let shard_idx = find_open_shard_idx(&stream.shards, shard_to_split)?;

        let (old_starting, old_ending) = shard_hash_range(&stream.shards[shard_idx]);
        let split_point: u128 = new_starting_hash_key
            .parse()
            .map_err(|_| invalid_argument("NewStartingHashKey must be a valid number"))?;

        if split_point <= old_starting || split_point > old_ending {
            return Err(invalid_argument(
                "NewStartingHashKey must be within the shard's hash key range",
            ));
        }

        stream.shards[shard_idx].is_open = false;

        let id1 = next_shard_id(stream);
        let id2 = next_shard_id(stream);

        stream.shards.push(KinesisShard {
            shard_id: id1,
            starting_hash_key: old_starting.to_string(),
            ending_hash_key: (split_point - 1).to_string(),
            parent_shard_id: Some(shard_to_split.to_string()),
            adjacent_parent_shard_id: None,
            is_open: true,
            next_sequence_number: 1,
            records: Vec::new(),
        });
        stream.shards.push(KinesisShard {
            shard_id: id2,
            starting_hash_key: split_point.to_string(),
            ending_hash_key: old_ending.to_string(),
            parent_shard_id: Some(shard_to_split.to_string()),
            adjacent_parent_shard_id: None,
            is_open: true,
            next_sequence_number: 1,
            records: Vec::new(),
        });

        stream.shard_count = stream.shards.len() as i32;
        stream.open_shard_count = stream.shards.iter().filter(|s| s.is_open).count() as i32;

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn update_shard_count(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let target = body["TargetShardCount"]
            .as_i64()
            .ok_or_else(|| invalid_argument("TargetShardCount is required"))?;
        if target <= 0 || target > 10000 {
            return Err(invalid_argument(
                "TargetShardCount must be between 1 and 10000",
            ));
        }
        let _scaling_type = body["ScalingType"]
            .as_str()
            .ok_or_else(|| invalid_argument("ScalingType is required"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let stream_name = resolve_stream_name(state, &body)?;
        let account_id = state.account_id.clone();
        let stream_arn = state.stream_arn(request.region.as_str(), &stream_name);
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        let current = stream.open_shard_count;

        // Real Kinesis UpdateShardCount (uniform scaling) does not simply close
        // the old shards and open `target` new ones. It reshapes the current
        // partition into the target partition through the shards' *common
        // refinement*: every current open shard is split at the target
        // boundaries (closing the originals), then adjacent refinement pieces
        // that fall inside the same target shard are merged (closing those
        // pieces). This is why scaling 2 -> 3 leaves 4 closed shards, not 2 —
        // a fact the `aws_kinesis_stream` data source asserts. We reproduce the
        // same lineage so `closed_shards` / `open_shards` match AWS.

        // Current open shards as inclusive [start, end] hash-key ranges.
        let mut open_ranges: Vec<(u128, u128)> = stream
            .shards
            .iter()
            .filter(|s| s.is_open)
            .map(|s| {
                (
                    s.starting_hash_key.parse::<u128>().unwrap_or(0),
                    s.ending_hash_key.parse::<u128>().unwrap_or(MAX_HASH_KEY),
                )
            })
            .collect();
        open_ranges.sort_unstable();

        // Target uniform partition as inclusive [start, end] ranges.
        let count = target as u128;
        let target_ranges: Vec<(u128, u128)> = (0..count)
            .map(|idx| {
                let start = if idx == 0 {
                    0
                } else {
                    (MAX_HASH_KEY / count) * idx + 1
                };
                let end = if idx == count - 1 {
                    MAX_HASH_KEY
                } else {
                    (MAX_HASH_KEY / count) * (idx + 1)
                };
                (start, end)
            })
            .collect();

        // Cut points = the union of every range's *start* key. Each pair of
        // consecutive starts defines a refinement interval `[start, next-1]`,
        // with the final interval running to `MAX_HASH_KEY`. (Hash keys span
        // the full u128 range, so an exclusive `end+1` upper bound would
        // overflow — starts alone unambiguously define the partition.)
        let mut cuts: Vec<u128> = Vec::with_capacity(open_ranges.len() + target_ranges.len() + 1);
        cuts.push(0);
        for (s, _) in open_ranges.iter().chain(target_ranges.iter()) {
            cuts.push(*s);
        }
        cuts.sort_unstable();
        cuts.dedup();

        let refinement: Vec<(u128, u128)> = cuts
            .iter()
            .enumerate()
            .map(|(i, &start)| {
                let end = cuts.get(i + 1).map(|next| next - 1).unwrap_or(MAX_HASH_KEY);
                (start, end)
            })
            .collect();

        // Capture (id, start, end) of the pre-scale open shards *before*
        // closing them, so the new shards can record them as parents — the
        // lineage a consumer walks via `ChildShards` to discover the post-scale
        // shards. Every refinement interval falls entirely inside exactly one
        // of these (the cut points include every original start key).
        let original_open: Vec<(String, u128, u128)> = stream
            .shards
            .iter()
            .filter(|s| s.is_open)
            .map(|s| {
                (
                    s.shard_id.clone(),
                    s.starting_hash_key.parse::<u128>().unwrap_or(0),
                    s.ending_hash_key.parse::<u128>().unwrap_or(MAX_HASH_KEY),
                )
            })
            .collect();
        let parent_for = |start: u128, end: u128| -> Option<String> {
            original_open
                .iter()
                .find(|(_, ostart, oend)| start >= *ostart && end <= *oend)
                .map(|(id, _, _)| id.clone())
        };

        // Close every current open shard — they are all split away.
        for shard in &mut stream.shards {
            shard.is_open = false;
        }

        // Materialise each refinement interval as a shard whose parent is the
        // pre-scale shard that contained it. Every original shard thus becomes
        // the parent of at least one new shard, so GetRecords on a drained
        // original returns non-empty `ChildShards` instead of stranding the
        // consumer at the closed parent.
        let mut piece_ids: Vec<(u128, u128, String)> = Vec::new();
        for (start, end) in &refinement {
            let new_id = next_shard_id(stream);
            let parent = parent_for(*start, *end);
            stream.shards.push(KinesisShard {
                shard_id: new_id.clone(),
                starting_hash_key: start.to_string(),
                ending_hash_key: end.to_string(),
                parent_shard_id: parent,
                adjacent_parent_shard_id: None,
                is_open: true,
                next_sequence_number: 1,
                records: Vec::new(),
            });
            piece_ids.push((*start, *end, new_id));
        }

        // For each target shard, gather the refinement pieces inside it. A
        // single-piece target keeps that piece open. A multi-piece target folds
        // its pieces left-to-right into pairwise merges, closing each piece and
        // every intermediate merge and leaving one open shard spanning the
        // target range — so every piece has an open descendant and the lineage
        // from each original shard stays connected end to end.
        for (tstart, tend) in &target_ranges {
            let pieces: Vec<(u128, String)> = piece_ids
                .iter()
                .filter(|(ps, pe, _)| ps >= tstart && pe <= tend)
                .map(|(_, pe, id)| (*pe, id.clone()))
                .collect();
            if pieces.len() <= 1 {
                continue;
            }
            // Fold: `acc` is the current open head of the merge chain, `acc_end`
            // its ending hash key. Each step closes `acc` and the next piece and
            // opens a merged shard parented on both.
            let (_, mut acc) = pieces[0].clone();
            close_shard(stream, &acc);
            for (next_end, next_id) in &pieces[1..] {
                close_shard(stream, next_id);
                let merged_id = next_shard_id(stream);
                stream.shards.push(KinesisShard {
                    shard_id: merged_id.clone(),
                    starting_hash_key: tstart.to_string(),
                    ending_hash_key: next_end.to_string(),
                    parent_shard_id: Some(acc.clone()),
                    adjacent_parent_shard_id: Some(next_id.clone()),
                    is_open: true,
                    next_sequence_number: 1,
                    records: Vec::new(),
                });
                // The previous head is now an interior (closed) merge node; the
                // freshly pushed `merged_id` is the new open head.
                close_shard(stream, &acc);
                acc = merged_id;
            }
        }

        stream.shard_count = stream.shards.len() as i32;
        stream.open_shard_count = target as i32;

        Ok(AwsResponse::ok_json(json!({
            "StreamName": stream_name,
            "StreamARN": stream_arn,
            "CurrentShardCount": current,
            "TargetShardCount": target,
        })))
    }

    // --- Batch 6: SubscribeToShard ---

    fn subscribe_to_shard(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_stream_id(&body)?;
        let consumer_arn = body["ConsumerARN"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("ConsumerARN is required"))?;
        validate_string_length("ConsumerARN", consumer_arn, 1, 2048)?;
        let shard_id = body["ShardId"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("ShardId is required"))?;

        let starting_position = &body["StartingPosition"];
        if starting_position.is_null() {
            return Err(invalid_argument("StartingPosition is required"));
        }
        let position_type = starting_position["Type"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid_argument("StartingPosition.Type is required"))?;
        const VALID_TYPES: &[&str] = &[
            "AT_SEQUENCE_NUMBER",
            "AFTER_SEQUENCE_NUMBER",
            "TRIM_HORIZON",
            "LATEST",
            "AT_TIMESTAMP",
        ];
        if !VALID_TYPES.contains(&position_type) {
            return Err(invalid_argument(format!(
                "Invalid StartingPosition.Type: {position_type}"
            )));
        }

        // Enhanced fan-out: consult the registered consumers. Previously this
        // returned ResourceNotFoundException unconditionally — even for a
        // consumer that Register/Describe had populated — so every fan-out read
        // failed (bug-hunt 2026-06-24, 1.9). Resolve the consumer, its stream
        // and shard, the starting position, and stream the records back as a
        // `SubscribeToShardEvent` over the eventstream wire format the SDK
        // expects.
        let accounts = self.state.read();
        let empty = KinesisState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let consumer = state.consumers.get(consumer_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Consumer {consumer_arn} not found."),
            )
        })?;
        let stream_name = consumer
            .stream_arn
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string();
        let stream = state
            .streams
            .get(&stream_name)
            .ok_or_else(|| stream_not_found(&state.account_id, &stream_name))?;
        let shard = stream
            .shards
            .iter()
            .find(|s| s.shard_id == shard_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Shard {shard_id} not found in stream {stream_name}."),
                )
            })?;

        // SubscribeToShard's StartingPosition uses `SequenceNumber`;
        // GetShardIterator (which `shard_iterator_start_index` reads) uses
        // `StartingSequenceNumber`. Bridge the field names.
        let synthetic = json!({
            "StartingSequenceNumber": starting_position.get("SequenceNumber").cloned().unwrap_or(Value::Null),
            "Timestamp": starting_position.get("Timestamp").cloned().unwrap_or(Value::Null),
        });
        let start_index = shard_iterator_start_index(shard, position_type, &synthetic)?;

        // AWS caps a SubscribeToShard event at 1000 records / 10 MiB.
        let end_index = shard.records.len().min(start_index.saturating_add(1000));
        let records: Vec<Value> = shard.records[start_index..end_index]
            .iter()
            .map(|record| {
                json!({
                    "ApproximateArrivalTimestamp": record.approximate_arrival_timestamp.timestamp_millis() as f64 / 1000.0,
                    "Data": base64::engine::general_purpose::STANDARD.encode(&record.data),
                    "PartitionKey": record.partition_key,
                    "SequenceNumber": record.sequence_number,
                })
            })
            .collect();

        let continuation = shard
            .records
            .get(end_index.saturating_sub(1))
            .map(|r| r.sequence_number.clone());
        let total = shard.records.len();
        let millis_behind_latest = if end_index >= total || end_index == 0 {
            0
        } else {
            let last = shard.records[end_index - 1].approximate_arrival_timestamp;
            let tip = shard.records[total - 1].approximate_arrival_timestamp;
            (tip - last).num_milliseconds().max(0)
        };

        let mut event = json!({
            "Records": records,
            "MillisBehindLatest": millis_behind_latest,
        });
        if let Some(c) = continuation {
            event["ContinuationSequenceNumber"] = json!(c);
        }
        let payload = serde_json::to_vec(&event).unwrap_or_default();
        let frame = crate::eventstream::subscribe_to_shard_event_frame(&payload);

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/vnd.amazon.eventstream".to_string(),
            body: frame.into(),
            headers: http::HeaderMap::new(),
        })
    }
}

// --- State helper ---

impl crate::state::KinesisState {
    fn lookup_stream(&self, body: &Value) -> Result<&KinesisStream, AwsServiceError> {
        let stream_name = resolve_stream_name(self, body)?;
        self.streams
            .get(&stream_name)
            .ok_or_else(|| stream_not_found(&self.account_id, &stream_name))
    }
}

// --- Free functions ---

/// The maximum hash key value (2^128 - 1).
const MAX_HASH_KEY: u128 = u128::MAX;

const SHARD_LEVEL_METRICS: &[&str] = &[
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds",
    "ALL",
];

#[path = "service_helpers.rs"]
mod service_helpers;
pub use service_helpers::build_stream_shards;
pub(crate) use service_helpers::*;

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
