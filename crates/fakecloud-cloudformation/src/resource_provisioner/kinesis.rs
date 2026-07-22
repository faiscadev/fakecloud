//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `kinesis`.

use super::*;

impl ResourceProvisioner {
    // --- Kinesis ---

    pub(super) fn create_kinesis_stream(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let stream_name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let shard_count = props
            .get("ShardCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(1) as i32;
        if shard_count <= 0 {
            return Err("ShardCount must be greater than zero".to_string());
        }
        let stream_mode = props
            .get("StreamModeDetails")
            .and_then(|v| v.get("StreamMode"))
            .and_then(|v| v.as_str())
            .unwrap_or("PROVISIONED")
            .to_string();
        let retention_period_hours = props
            .get("RetentionPeriodHours")
            .and_then(|v| v.as_i64())
            .unwrap_or(24) as i32;

        let mut accounts = self.kinesis_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.streams.contains_key(&stream_name) {
            return Err(format!("Stream {stream_name} already exists"));
        }
        let stream_arn = format!(
            "arn:aws:kinesis:{}:{}:stream/{}",
            self.region, state.account_id, stream_name
        );
        let stream = KinesisStream {
            stream_name: stream_name.clone(),
            stream_arn: stream_arn.clone(),
            stream_status: "ACTIVE".to_string(),
            stream_creation_timestamp: Utc::now(),
            retention_period_hours,
            stream_mode,
            encryption_type: "NONE".to_string(),
            key_id: None,
            shard_count,
            open_shard_count: shard_count,
            tags: BTreeMap::new(),
            shards: build_stream_shards(shard_count),
            next_shard_index: shard_count,
            enhanced_metrics: Vec::new(),
            warm_throughput_mibps: None,
            max_record_size_kib: None,
        };
        state.streams.insert(stream_name.clone(), stream);

        Ok(ProvisionResult::new(stream_name).with("Arn", stream_arn))
    }

    /// Apply a CFN property update to an existing Kinesis stream in place.
    /// `RetentionPeriodHours`, `ShardCount` and `StreamModeDetails.StreamMode`
    /// are update-without-replacement in real CloudFormation, so a stack update
    /// must reach the stream and be reflected by `DescribeStreamSummary` instead
    /// of being silently dropped. Retention and stream mode are applied exactly;
    /// a `ShardCount` change reshapes the stream to the target uniform partition
    /// (the fine-grained closed-shard lineage produced by the native
    /// `UpdateShardCount` common-refinement is not reproduced here — only the
    /// resulting open-shard partition, which is what Describe reports).
    pub(super) fn update_kinesis_stream(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        // The Kinesis stream's CFN physical id is the stream name (create returns
        // `ProvisionResult::new(stream_name)`), so look it up by name.
        let stream_name = &existing.physical_id;

        let mut accounts = self.kinesis_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let stream = state
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| format!("Kinesis stream {stream_name} not yet provisioned"))?;
        let stream_arn = stream.stream_arn.clone();

        if let Some(hours) = props.get("RetentionPeriodHours").and_then(|v| v.as_i64()) {
            stream.retention_period_hours = hours as i32;
        }
        if let Some(mode) = props
            .get("StreamModeDetails")
            .and_then(|v| v.get("StreamMode"))
            .and_then(|v| v.as_str())
        {
            stream.stream_mode = mode.to_string();
        }
        if let Some(target) = props.get("ShardCount").and_then(|v| v.as_i64()) {
            if target <= 0 {
                return Err("ShardCount must be greater than zero".to_string());
            }
            let target = target as i32;
            if target != stream.shard_count {
                stream.shards = build_stream_shards(target);
                stream.shard_count = target;
                stream.open_shard_count = target;
                stream.next_shard_index = target;
            }
        }

        Ok(ProvisionResult::new(existing.physical_id.clone()).with("Arn", stream_arn))
    }

    pub(super) fn delete_kinesis_stream(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kinesis_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.streams.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_kinesis_stream_consumer(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let stream_arn = props
            .get("StreamARN")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "StreamARN is required".to_string())?
            .to_string();
        let consumer_name = props
            .get("ConsumerName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ConsumerName is required".to_string())?
            .to_string();

        let mut accounts = self.kinesis_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state
            .consumers
            .values()
            .any(|c| c.stream_arn == stream_arn && c.consumer_name == consumer_name)
        {
            return Err(format!(
                "Consumer {consumer_name} already exists on stream {stream_arn}"
            ));
        }
        let now = Utc::now();
        let consumer_arn = format!(
            "{}/consumer/{}:{}",
            stream_arn,
            consumer_name,
            now.timestamp()
        );
        let consumer = KinesisConsumer {
            consumer_name: consumer_name.clone(),
            consumer_arn: consumer_arn.clone(),
            consumer_status: "ACTIVE".to_string(),
            consumer_creation_timestamp: now,
            stream_arn: stream_arn.clone(),
        };
        state.consumers.insert(consumer_arn.clone(), consumer);

        Ok(ProvisionResult::new(consumer_arn.clone())
            .with("ConsumerARN", consumer_arn)
            .with("ConsumerName", consumer_name)
            .with("ConsumerStatus", "ACTIVE")
            .with("ConsumerCreationTimestamp", now.timestamp().to_string())
            .with("StreamARN", stream_arn))
    }

    pub(super) fn delete_kinesis_stream_consumer(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kinesis_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.consumers.remove(physical_id);
        Ok(())
    }
}
