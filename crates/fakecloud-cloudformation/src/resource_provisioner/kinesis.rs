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
        let generated_name = self.physical_name(resource);
        let stream_name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
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

        let (encryption_type, key_id) = cfn_kinesis_encryption(props)?;

        let mut accounts = self.kinesis_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if state.streams.contains_key(&stream_name) {
            return Err(format!("Stream {stream_name} already exists"));
        }
        let stream_arn = state.stream_arn(&self.region, &stream_name);
        let stream = KinesisStream {
            stream_name: stream_name.clone(),
            stream_arn: stream_arn.clone(),
            stream_status: "ACTIVE".to_string(),
            stream_creation_timestamp: Utc::now(),
            retention_period_hours,
            stream_mode,
            encryption_type,
            key_id,
            shard_count,
            open_shard_count: shard_count,
            tags: cfn_kinesis_tags(props),
            shards: build_stream_shards(shard_count),
            next_shard_index: shard_count,
            enhanced_metrics: Vec::new(),
            warm_throughput_mibps: None,
            max_record_size_kib: None,
            record_distribution_strategy: fakecloud_kinesis::default_record_distribution_strategy(),
            auto_distribution_cursor: 0,
        };
        state.streams.insert(stream_name.clone(), stream);

        Ok(ProvisionResult::new(stream_name).with("Arn", stream_arn))
    }

    /// Apply a CFN property update to an existing Kinesis stream in place.
    /// `RetentionPeriodHours`, `ShardCount`, `StreamModeDetails`,
    /// `StreamEncryption` and `Tags` are update-without-replacement in real
    /// CloudFormation. A `ShardCount` change goes through UpdateShardCount's
    /// uniform resharding: the old shards are closed (keeping their records)
    /// and the new ones record their parents, so no data is lost.
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
        let state = accounts.regional_mut(&self.account_id, &self.region);
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
            if !(1..=10000).contains(&target) {
                return Err("ShardCount must be between 1 and 10000".to_string());
            }
            let target = target as i32;
            if target != stream.open_shard_count {
                fakecloud_kinesis::reshard_uniform(stream, target);
            }
        }
        // StreamEncryption and Tags are desired state: dropping either from
        // the template stops encryption / removes the tags.
        let (encryption_type, key_id) = cfn_kinesis_encryption(props)?;
        stream.encryption_type = encryption_type;
        stream.key_id = key_id;
        stream.tags = cfn_kinesis_tags(props);

        Ok(ProvisionResult::new(existing.physical_id.clone()).with("Arn", stream_arn))
    }

    pub(super) fn delete_kinesis_stream(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.kinesis_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if let Some(stream) = state.streams.remove(physical_id) {
            state.forget_stream_checkpoints(&stream.stream_arn);
        }
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
        let state = accounts.regional_mut(&self.account_id, &self.region);
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
            tags: BTreeMap::new(),
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
        let state = accounts.regional_mut(&self.account_id, &self.region);
        state.consumers.remove(physical_id);
        Ok(())
    }
}

/// `StreamEncryption` as the stream's (EncryptionType, KeyId): KMS with the
/// given key, or NONE when the property is absent.
fn cfn_kinesis_encryption(props: &serde_json::Value) -> Result<(String, Option<String>), String> {
    let Some(enc) = props.get("StreamEncryption").filter(|v| v.is_object()) else {
        return Ok(("NONE".to_string(), None));
    };
    let encryption_type = enc
        .get("EncryptionType")
        .and_then(|v| v.as_str())
        .unwrap_or("KMS");
    if encryption_type != "KMS" {
        return Err(format!(
            "StreamEncryption.EncryptionType must be KMS, got {encryption_type}"
        ));
    }
    let key_id = enc
        .get("KeyId")
        .and_then(|v| v.as_str())
        .filter(|k| !k.is_empty())
        .ok_or_else(|| "StreamEncryption.KeyId is required".to_string())?;
    Ok(("KMS".to_string(), Some(key_id.to_string())))
}

/// CFN `Tags` (`[{Key, Value}]`) as the stream's tag map.
fn cfn_kinesis_tags(props: &serde_json::Value) -> BTreeMap<String, String> {
    props
        .get("Tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    Some((
                        t.get("Key")?.as_str()?.to_string(),
                        t.get("Value")?.as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}
