use super::*;

/// Locate the index of an open shard by id, returning the caller-friendly
/// `"Shard X not found or not open"` error when it's missing.
pub(crate) fn find_open_shard_idx(
    shards: &[KinesisShard],
    shard_id: &str,
) -> Result<usize, AwsServiceError> {
    shards
        .iter()
        .position(|s| s.shard_id == shard_id && s.is_open)
        .ok_or_else(|| invalid_argument(format!("Shard {shard_id} not found or not open")))
}

/// Parse a shard's hash key range into `(start, end)` as `u128`. We
/// silently fall back to 0 on parse errors to match the pre-split
/// behaviour of the shard-management operations.
pub(crate) fn shard_hash_range(shard: &KinesisShard) -> (u128, u128) {
    let start = shard.starting_hash_key.parse().unwrap_or(0);
    let end = shard.ending_hash_key.parse().unwrap_or(0);
    (start, end)
}

/// Allocate the next `shardId-NNNNNNNNNNNN` in this stream's monotonic
/// counter. Advances `next_shard_index`, so only call it when you are
/// about to push a new shard.
pub(crate) fn next_shard_id(stream: &mut KinesisStream) -> String {
    let id = format!("shardId-{:012}", stream.next_shard_index);
    stream.next_shard_index += 1;
    id
}

/// Mark the shard with `shard_id` closed, if it is present. Idempotent, so
/// callers can close an already-closed shard without a membership check.
pub(crate) fn close_shard(stream: &mut KinesisStream, shard_id: &str) {
    if let Some(shard) = stream.shards.iter_mut().find(|s| s.shard_id == shard_id) {
        shard.is_open = false;
    }
}

/// Actions that mutate *durable* Kinesis state and therefore warrant a
/// snapshot save. GetShardIterator / GetRecords are deliberately excluded:
/// the only thing they touch is the shard-iterator lease map, which is
/// `#[serde(skip)]` (ephemeral, never persisted). Treating those read polls
/// as mutating meant every GetRecords cloned + serialized + disk-wrote the
/// whole multi-account state for zero durable effect — a per-read cost that
/// turns a read-heavy consumer into a serialization bottleneck.
pub(crate) fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "CreateChannel"
            | "CreateStream"
            | "DeleteChannel"
            | "DeleteStream"
            | "UpdateChannel"
            | "PutRecord"
            | "PutRecords"
            | "AddTagsToStream"
            | "RemoveTagsFromStream"
            | "IncreaseStreamRetentionPeriod"
            | "DecreaseStreamRetentionPeriod"
            | "TagResource"
            | "UntagResource"
            | "PutResourcePolicy"
            | "DeleteResourcePolicy"
            | "StartStreamEncryption"
            | "StopStreamEncryption"
            | "EnableEnhancedMonitoring"
            | "DisableEnhancedMonitoring"
            | "UpdateAccountSettings"
            | "UpdateStreamMode"
            | "UpdateStreamRecordDistributionStrategy"
            | "UpdateStreamWarmThroughput"
            | "UpdateMaxRecordSize"
            | "RegisterStreamConsumer"
            | "DeregisterStreamConsumer"
            | "MergeShards"
            | "SplitShard"
            | "UpdateShardCount"
    )
}

/// PutRecord: default single-record payload limit (Data + PartitionKey) is
/// 1 MiB. AWS raises the ceiling to `MaxRecordSizeInKiB` when the stream
/// opts into larger records.
pub(crate) const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;
/// PutRecords: a single call carries at most 500 records.
pub(crate) const MAX_PUT_RECORDS_COUNT: usize = 500;
/// PutRecords: a single call carries at most 5 MiB of aggregate payload
/// (default). Streams that raise `MaxRecordSizeInKiB` above 5 MiB get the
/// larger ceiling so a single legal record can never exceed the batch limit.
pub(crate) const DEFAULT_MAX_PUT_RECORDS_BYTES: usize = 5 * 1024 * 1024;
/// PartitionKey is a Unicode string of 1..=256 characters.
pub(crate) const MAX_PARTITION_KEY_CHARS: usize = 256;

/// Effective per-record payload ceiling for a stream: `MaxRecordSizeInKiB`
/// when configured, otherwise the 1 MiB default.
pub(crate) fn effective_max_record_bytes(stream: &KinesisStream) -> usize {
    stream
        .max_record_size_kib
        .filter(|kib| *kib > 0)
        .map(|kib| (kib as usize) * 1024)
        .unwrap_or(DEFAULT_MAX_RECORD_BYTES)
}

/// Aggregate payload ceiling for a PutRecords call: the larger of the 5 MiB
/// default and the stream's per-record ceiling.
pub(crate) fn effective_max_batch_bytes(stream: &KinesisStream) -> usize {
    effective_max_record_bytes(stream).max(DEFAULT_MAX_PUT_RECORDS_BYTES)
}

pub(crate) fn require_stream_name(body: &Value) -> Result<&str, AwsServiceError> {
    let name = body["StreamName"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("StreamName is required"))?;
    validate_string_length("StreamName", name, 1, 128)?;
    Ok(name)
}

pub(crate) fn resolve_stream_name(
    state: &crate::state::KinesisState,
    body: &Value,
) -> Result<String, AwsServiceError> {
    if let Some(stream_name) = body["StreamName"]
        .as_str()
        .filter(|value| !value.is_empty())
    {
        validate_string_length("StreamName", stream_name, 1, 128)?;
        return Ok(stream_name.to_string());
    }

    if let Some(stream_arn) = body["StreamARN"].as_str().filter(|value| !value.is_empty()) {
        if let Some(stream_name) = stream_arn.rsplit('/').next() {
            if state.arn_in_region(stream_arn) && state.streams.contains_key(stream_name) {
                return Ok(stream_name.to_string());
            }
            return Err(stream_not_found(&state.account_id, stream_name));
        }
    }

    Err(invalid_argument("StreamName or StreamARN is required"))
}

pub(crate) fn shard_to_json(shard: &KinesisShard) -> Value {
    // The advertised StartingSequenceNumber must match the format records are
    // actually minted with — a per-shard discriminator packed into the low
    // digits (see `append_record`). Advertising the bare `00…01` for every
    // shard meant GetShardIterator(AT_SEQUENCE_NUMBER, <advertised>) on shard
    // N>0 looked up a sequence no record in that shard has, returning
    // InvalidArgumentException (bug-hunt 2026-06-24, 1.19).
    let disc = shard_discriminator(&shard.shard_id);
    let mut obj = json!({
        "ShardId": shard.shard_id,
        "HashKeyRange": {
            "StartingHashKey": shard.starting_hash_key,
            "EndingHashKey": shard.ending_hash_key,
        },
        "SequenceNumberRange": {
            "StartingSequenceNumber": format!("{:05}{:051}", disc, 1),
        },
    });
    if let Some(ref parent) = shard.parent_shard_id {
        obj["ParentShardId"] = json!(parent);
    }
    if let Some(ref adj) = shard.adjacent_parent_shard_id {
        obj["AdjacentParentShardId"] = json!(adj);
    }
    if !shard.is_open {
        obj["SequenceNumberRange"]["EndingSequenceNumber"] = json!(format!(
            "{:05}{:051}",
            disc,
            shard.next_sequence_number.saturating_sub(1).max(1)
        ));
    }
    obj
}

pub fn build_stream_shards(shard_count: i32) -> Vec<KinesisShard> {
    let count = shard_count as u128;
    (0..shard_count)
        .map(|index| {
            let i = index as u128;
            let starting = if i == 0 {
                0u128
            } else {
                (MAX_HASH_KEY / count) * i + 1
            };
            let ending = if i == count - 1 {
                MAX_HASH_KEY
            } else {
                (MAX_HASH_KEY / count) * (i + 1)
            };
            KinesisShard {
                shard_id: format!("shardId-{:012}", index),
                starting_hash_key: starting.to_string(),
                ending_hash_key: ending.to_string(),
                parent_shard_id: None,
                adjacent_parent_shard_id: None,
                is_open: true,
                next_sequence_number: 1,
                records: Vec::new(),
            }
        })
        .collect()
}

/// PartitionKey for a PutRecord call. It is required under the default
/// `USER_PARTITION_KEY` strategy; under `AUTO` it is optional (an omitted key
/// is recorded as empty and never returned) and ignored for placement.
pub(crate) fn require_partition_key<'a>(
    body: &'a Value,
    stream: &KinesisStream,
) -> Result<&'a str, AwsServiceError> {
    let auto = stream.record_distribution_strategy == RECORD_DISTRIBUTION_AUTO;
    let partition_key = match body["PartitionKey"].as_str() {
        // Optional under AUTO means omitted, not empty: the member still
        // carries its length >= 1 constraint.
        None if auto => return Ok(""),
        Some("") if auto => return Err(empty_partition_key()),
        Some(key) if !key.is_empty() => key,
        _ => return Err(invalid_argument("PartitionKey is required")),
    };
    if partition_key.chars().count() > MAX_PARTITION_KEY_CHARS {
        return Err(validation_exception(format!(
            "1 validation error detected: Value at 'partitionKey' failed to satisfy \
             constraint: Member must have length less than or equal to {MAX_PARTITION_KEY_CHARS}"
        )));
    }
    Ok(partition_key)
}

fn empty_partition_key() -> AwsServiceError {
    validation_exception(
        "1 validation error detected: Value at 'partitionKey' failed to satisfy \
         constraint: Member must have length greater than or equal to 1",
    )
}

pub(crate) fn require_shard_id(body: &Value) -> Result<&str, AwsServiceError> {
    body["ShardId"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("ShardId is required"))
}

pub(crate) fn require_resource_arn(body: &Value) -> Result<&str, AwsServiceError> {
    let arn = body["ResourceARN"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("ResourceARN is required"))?;
    validate_string_length("ResourceARN", arn, 1, 2048)?;
    Ok(arn)
}

/// The resource a Tags v2 `ResourceARN` names. The model constrains it to
/// `^arn:aws.*:kinesis:.*:\d{12}:.*(stream|channel)/\S+$`, so a delivery
/// channel is as valid a tag target as a stream.
pub(crate) enum TaggedResource {
    Stream(String),
    Channel(String),
    /// Keyed by the stored consumer ARN.
    Consumer(String),
}

/// Resolve a Tags v2 `ResourceARN` to the resource it names. Both arms key off
/// the ARN's resource segment, so a caller whose credential scope names a
/// different region than the stored ARN still resolves the resource.
pub(crate) fn resolve_tagged_resource(
    state: &crate::state::KinesisState,
    resource_arn: &str,
) -> Result<TaggedResource, AwsServiceError> {
    if resource_arn.contains("/consumer/") {
        // Consumers are keyed by their full ARN; fall back to matching the
        // resource segment so a caller whose credential scope names a
        // different region still resolves it (as the stream arm does).
        if state.consumers.contains_key(resource_arn) {
            return Ok(TaggedResource::Consumer(resource_arn.to_string()));
        }
        let resource_segment = |arn: &str| arn.splitn(6, ':').nth(5).map(str::to_string);
        let wanted = resource_segment(resource_arn);
        return state
            .consumers
            .keys()
            .find(|k| wanted.is_some() && resource_segment(k) == wanted)
            .cloned()
            .map(TaggedResource::Consumer)
            .ok_or_else(|| resource_not_found_arn(resource_arn));
    }
    if resource_arn.contains(":channel/") {
        return state
            .channel_name_from_arn(resource_arn)
            .map(TaggedResource::Channel)
            .ok_or_else(|| resource_not_found_arn(resource_arn));
    }
    state
        .stream_name_from_arn(resource_arn)
        .map(TaggedResource::Stream)
        .ok_or_else(|| resource_not_found_arn(resource_arn))
}

/// The tag map `resource_arn` names, for the two mutating tag operations.
pub(crate) fn resource_tags_mut<'a>(
    state: &'a mut crate::state::KinesisState,
    resource_arn: &str,
) -> Result<&'a mut std::collections::BTreeMap<String, String>, AwsServiceError> {
    // Resolved before the mutable borrows below, and infallible from here:
    // `resolve_tagged_resource` only names a resource it found in these very
    // maps.
    let resource = resolve_tagged_resource(state, resource_arn)?;
    match resource {
        TaggedResource::Stream(name) => Ok(&mut state.streams.get_mut(&name).unwrap().tags),
        TaggedResource::Channel(name) => Ok(&mut state.channels.get_mut(&name).unwrap().tags),
        TaggedResource::Consumer(arn) => Ok(&mut state.consumers.get_mut(&arn).unwrap().tags),
    }
}

/// The tag map `resource_arn` names, for `ListTagsForResource`.
pub(crate) fn resource_tags<'a>(
    state: &'a crate::state::KinesisState,
    resource_arn: &str,
) -> Result<&'a std::collections::BTreeMap<String, String>, AwsServiceError> {
    let resource = resolve_tagged_resource(state, resource_arn)?;
    match resource {
        TaggedResource::Stream(name) => Ok(&state.streams[&name].tags),
        TaggedResource::Channel(name) => Ok(&state.channels[&name].tags),
        TaggedResource::Consumer(arn) => Ok(&state.consumers[&arn].tags),
    }
}

pub(crate) fn decode_record_data(value: &Value) -> Result<Vec<u8>, AwsServiceError> {
    let encoded = value
        .as_str()
        .ok_or_else(|| invalid_argument("Data must be a base64 string"))?;
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| invalid_argument("Data must be valid base64"))
}

/// Compute the 128-bit hash key for a partition key exactly as AWS does:
/// the big-endian unsigned integer value of `MD5(partitionKey)`.
pub(crate) fn partition_key_hash(partition_key: &str) -> u128 {
    let digest = Md5::digest(partition_key.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    u128::from_be_bytes(bytes)
}

/// Resolve the routing hash for a record. When `ExplicitHashKey` is supplied
/// it overrides the partition-key hash (AWS allows a base-10 string in
/// `[0, 2^128-1]`); otherwise we hash the partition key with MD5.
pub(crate) fn routing_hash(
    partition_key: &str,
    explicit_hash_key: Option<&str>,
) -> Result<u128, AwsServiceError> {
    if let Some(raw) = explicit_hash_key.filter(|value| !value.is_empty()) {
        return raw
            .parse::<u128>()
            .map_err(|_| invalid_argument("ExplicitHashKey must be a valid 128-bit integer"));
    }
    Ok(partition_key_hash(partition_key))
}

/// Locate the open shard whose `[StartingHashKey, EndingHashKey]` range
/// contains `hash`. Falls back to the last open shard (or the last shard
/// overall when none are open) so a routing hash can never fail to land.
pub(crate) fn select_shard_index_for_hash(stream: &KinesisStream, hash: u128) -> usize {
    let mut fallback: Option<usize> = None;
    for (idx, shard) in stream.shards.iter().enumerate() {
        if !shard.is_open {
            continue;
        }
        fallback = Some(idx);
        let (start, end) = shard_hash_range(shard);
        if hash >= start && hash <= end {
            return idx;
        }
    }
    fallback.unwrap_or_else(|| stream.shards.len().saturating_sub(1))
}

pub(crate) fn select_shard_mut<'a>(
    stream: &'a mut KinesisStream,
    partition_key: &str,
    explicit_hash_key: Option<&str>,
) -> Result<&'a mut KinesisShard, AwsServiceError> {
    // `select_shard_index_for_hash` falls back to `shards.len() - 1`, which
    // wraps to index 0 on an empty shard list — an out-of-bounds index panic.
    // No create path leaves a stream shard-less today, but guard it the same
    // way the fan-in delivery path already does rather than risk a panic.
    if stream.shards.is_empty() {
        return Err(invalid_argument("Stream has no shards to route to"));
    }
    // Under the AUTO record distribution strategy the service places records
    // itself and ignores both PartitionKey and ExplicitHashKey; spread them
    // evenly by rotating through the open shards.
    if stream.record_distribution_strategy == RECORD_DISTRIBUTION_AUTO {
        // Ignored for placement, but a malformed hash key is still rejected.
        routing_hash(partition_key, explicit_hash_key)?;
        let open: Vec<usize> = stream
            .shards
            .iter()
            .enumerate()
            .filter(|(_, shard)| shard.is_open)
            .map(|(idx, _)| idx)
            .collect();
        let idx = if open.is_empty() {
            stream.shards.len() - 1
        } else {
            open[(stream.auto_distribution_cursor % open.len() as u64) as usize]
        };
        stream.auto_distribution_cursor = stream.auto_distribution_cursor.wrapping_add(1);
        return Ok(&mut stream.shards[idx]);
    }
    let hash = routing_hash(partition_key, explicit_hash_key)?;
    let idx = select_shard_index_for_hash(stream, hash);
    Ok(&mut stream.shards[idx])
}

/// Wire shape of a stored record for GetRecords / SubscribeToShard.
/// `PartitionKey` is omitted when an AUTO-strategy producer supplied none.
pub(crate) fn record_to_json(record: &KinesisRecord) -> Value {
    let mut value = json!({
        "ApproximateArrivalTimestamp": record.approximate_arrival_timestamp.timestamp_millis() as f64 / 1000.0,
        "Data": base64::engine::general_purpose::STANDARD.encode(&record.data),
        "SequenceNumber": record.sequence_number,
    });
    if !record.partition_key.is_empty() {
        value["PartitionKey"] = json!(record.partition_key);
    }
    value
}

pub(crate) fn append_record(
    shard: &mut KinesisShard,
    partition_key: &str,
    data: Vec<u8>,
) -> String {
    // Stream-unique sequence number: pack a per-shard discriminator into
    // the low 5 digits so two shards can never mint the same value, while
    // the high digits keep per-shard monotonic ordering (bug-audit
    // 2026-05-28, 1.12 — values were only per-shard unique).
    let disc = shard_discriminator(&shard.shard_id);
    let sequence_number = format!("{:05}{:051}", disc, shard.next_sequence_number);
    shard.next_sequence_number += 1;
    shard.records.push(KinesisRecord {
        sequence_number: sequence_number.clone(),
        partition_key: partition_key.to_string(),
        data,
        approximate_arrival_timestamp: Utc::now(),
    });
    sequence_number
}

pub(crate) fn put_records_entry(
    stream: &mut KinesisStream,
    entry: &Value,
) -> Result<(String, String), String> {
    let auto = stream.record_distribution_strategy == RECORD_DISTRIBUTION_AUTO;
    let partition_key = match entry["PartitionKey"].as_str() {
        None if auto => "",
        Some("") if auto => return Err(empty_partition_key().message()),
        Some(key) if !key.is_empty() => key,
        _ => return Err("PartitionKey is required".to_string()),
    };
    if partition_key.chars().count() > MAX_PARTITION_KEY_CHARS {
        return Err(format!(
            "PartitionKey must have length less than or equal to {MAX_PARTITION_KEY_CHARS}"
        ));
    }
    let data = decode_record_data(&entry["Data"]).map_err(|error| error.message())?;
    let max_record_bytes = effective_max_record_bytes(stream);
    if data.len() + partition_key.len() > max_record_bytes {
        return Err(format!(
            "Record size (Data + PartitionKey) exceeds the {max_record_bytes}-byte per-record limit"
        ));
    }
    let explicit_hash_key = entry["ExplicitHashKey"].as_str();
    let shard = select_shard_mut(stream, partition_key, explicit_hash_key)
        .map_err(|error| error.message())?;
    let sequence_number = append_record(shard, partition_key, data);
    Ok((shard.shard_id.clone(), sequence_number))
}

/// Decoded byte size of a PutRecords entry's payload (Data + PartitionKey),
/// used for the aggregate-size pre-check. Undecodable data counts as 0 here;
/// the per-record loop reports it as a per-record failure.
pub(crate) fn put_records_entry_size(entry: &Value) -> usize {
    let data_len = decode_record_data(&entry["Data"])
        .map(|bytes| bytes.len())
        .unwrap_or(0);
    let key_len = entry["PartitionKey"].as_str().map(str::len).unwrap_or(0);
    data_len + key_len
}

pub(crate) fn shard_iterator_start_index(
    shard: &KinesisShard,
    iterator_type: &str,
    body: &Value,
) -> Result<usize, AwsServiceError> {
    match iterator_type {
        "TRIM_HORIZON" => Ok(0),
        "LATEST" => Ok(shard.records.len()),
        "AT_SEQUENCE_NUMBER" => {
            let sequence_number = require_starting_sequence_number(body)?;
            resolve_sequence_number_index(shard, sequence_number, false)
        }
        "AFTER_SEQUENCE_NUMBER" => {
            let sequence_number = require_starting_sequence_number(body)?;
            resolve_sequence_number_index(shard, sequence_number, true)
        }
        "AT_TIMESTAMP" => {
            // AWS encodes Timestamp as epoch seconds (float, with optional
            // fractional millis). Find the first record whose
            // approximate_arrival_timestamp is at or after that mark; fall
            // through to past-the-end when no record qualifies, so the
            // following GetRecords returns an empty page rather than 400.
            let ts_value = body["Timestamp"]
                .as_f64()
                .ok_or_else(|| invalid_argument("Timestamp is required"))?;
            if !ts_value.is_finite() || ts_value < 0.0 {
                return Err(invalid_argument("Timestamp must be a non-negative epoch"));
            }
            let secs = ts_value.trunc() as i64;
            let nanos = ((ts_value - ts_value.trunc()) * 1_000_000_000.0) as u32;
            let target = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
                .ok_or_else(|| invalid_argument("Timestamp is invalid"))?;
            let idx = shard
                .records
                .iter()
                .position(|r| r.approximate_arrival_timestamp >= target)
                .unwrap_or(shard.records.len());
            Ok(idx)
        }
        _ => Err(invalid_argument("Unsupported ShardIteratorType")),
    }
}

pub(crate) fn require_starting_sequence_number(body: &Value) -> Result<&str, AwsServiceError> {
    body["StartingSequenceNumber"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("StartingSequenceNumber is required"))
}

/// Extract the 5-digit shard discriminator packed into the front of a
/// sequence number by [`append_record`]. Returns `None` for anything that is
/// not a well-formed 56-digit sequence number, so a malformed or foreign token
/// can never be mistaken for a value this shard minted.
fn sequence_shard_discriminator(sequence_number: &str) -> Option<u32> {
    if sequence_number.len() != 56 || !sequence_number.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    sequence_number[..5].parse::<u32>().ok()
}

/// Resolve an `AT_/AFTER_SEQUENCE_NUMBER` iterator start index.
///
/// When the exact sequence number is still present we return its index (or
/// the next one for `AFTER`). When it is *not* present but was minted by this
/// shard and sorts before the earliest retained record, the record it named
/// was aged out by the retention window, so — like real Kinesis — we resolve
/// to the trim horizon (the earliest available record). A sequence number that
/// belongs to a different shard, is malformed, or is otherwise not valid for
/// this shard raises InvalidArgumentException, matching AWS.
pub(crate) fn resolve_sequence_number_index(
    shard: &KinesisShard,
    sequence_number: &str,
    after: bool,
) -> Result<usize, AwsServiceError> {
    if let Some(pos) = shard
        .records
        .iter()
        .position(|record| record.sequence_number == sequence_number)
    {
        return Ok(if after { pos + 1 } else { pos });
    }
    // A below-horizon token is only "trimmed off the front" if it could have
    // been minted by THIS shard — its packed discriminator must match. A
    // cross-shard or malformed token that merely happens to sort low is an
    // invalid argument, not a trim-horizon resolution.
    if let Some(first) = shard.records.first() {
        if sequence_number < first.sequence_number.as_str()
            && sequence_shard_discriminator(sequence_number)
                == Some(shard_discriminator(&shard.shard_id))
        {
            return Ok(0);
        }
    }
    Err(invalid_argument("StartingSequenceNumber is invalid"))
}

/// Encode a `ListShards` continuation token. AWS returns an opaque base64
/// cursor; we wrap the last-returned shard id so that a paginator feeding the
/// token back resumes immediately after it.
pub(crate) fn encode_list_shards_token(last_shard_id: &str) -> String {
    let payload = json!({ "ExclusiveStartShardId": last_shard_id });
    base64::engine::general_purpose::STANDARD.encode(payload.to_string().as_bytes())
}

/// Decode a `ListShards` continuation token back into the exclusive-start
/// shard id. Rejects malformed tokens with `InvalidArgumentException`, the
/// same shape AWS uses for an expired/garbage `NextToken`.
pub(crate) fn decode_list_shards_token(token: &str) -> Result<String, AwsServiceError> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(token)
        .map_err(|_| invalid_argument("Invalid NextToken"))?;
    let parsed: Value =
        serde_json::from_slice(&raw).map_err(|_| invalid_argument("Invalid NextToken"))?;
    parsed["ExclusiveStartShardId"]
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| invalid_argument("Invalid NextToken"))
}

pub(crate) fn validate_stream_id(body: &Value) -> Result<(), AwsServiceError> {
    validate_optional_string_length("StreamId", body["StreamId"].as_str(), 1, 24)
}

/// Evaluate a `ListShards` `ShardFilter` against a single shard. Returns
/// whether the shard should be included.
///
/// - `AT_LATEST`: only currently-open shards.
/// - `AT_TRIM_HORIZON` / `FROM_TRIM_HORIZON`: every shard (data is still
///   within the retention window in this model).
/// - `AFTER_SHARD_ID`: shards whose id sorts after the filter's `ShardId`.
/// - `AT_TIMESTAMP` / `FROM_TIMESTAMP`: shards that are still open or that
///   hold at least one record at/after the filter `Timestamp`.
///
/// Required companion fields (`ShardId` / `Timestamp`) are validated, matching
/// the `InvalidArgumentException` AWS raises when they're missing.
pub(crate) fn shard_matches_filter(
    shard: &KinesisShard,
    filter: &Value,
) -> Result<bool, AwsServiceError> {
    let filter_type = filter["Type"]
        .as_str()
        .filter(|t| !t.is_empty())
        .ok_or_else(|| invalid_argument("ShardFilter.Type is required"))?;

    match filter_type {
        "AT_LATEST" => Ok(shard.is_open),
        "AT_TRIM_HORIZON" | "FROM_TRIM_HORIZON" => Ok(true),
        "AFTER_SHARD_ID" => {
            let after = filter["ShardId"]
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    invalid_argument("ShardFilter.ShardId is required for AFTER_SHARD_ID")
                })?;
            Ok(shard.shard_id.as_str() > after)
        }
        "AT_TIMESTAMP" | "FROM_TIMESTAMP" => {
            let ts_value = filter["Timestamp"].as_f64().ok_or_else(|| {
                invalid_argument(
                    "ShardFilter.Timestamp is required for AT_TIMESTAMP/FROM_TIMESTAMP",
                )
            })?;
            if !ts_value.is_finite() || ts_value < 0.0 {
                return Err(invalid_argument(
                    "ShardFilter.Timestamp must be a non-negative epoch",
                ));
            }
            let secs = ts_value.trunc() as i64;
            let nanos = ((ts_value - ts_value.trunc()) * 1_000_000_000.0) as u32;
            let target = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
                .ok_or_else(|| invalid_argument("ShardFilter.Timestamp is invalid"))?;
            if shard.is_open {
                return Ok(true);
            }
            Ok(shard
                .records
                .iter()
                .any(|r| r.approximate_arrival_timestamp >= target))
        }
        other => Err(invalid_argument(format!(
            "Unsupported ShardFilter.Type: {other}"
        ))),
    }
}

pub(crate) fn resource_not_found_arn(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("Resource {arn} not found."),
    )
}

pub(crate) fn invalid_argument(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgumentException", message)
}

/// AWS returns `ValidationException` (HTTP 400) for constraint violations the
/// front end rejects before the operation runs — an oversized record payload,
/// too-long partition key, or a PutRecords batch over its count/size limits.
/// Validate a `RecordDistributionStrategy` value against the model enum.
pub(crate) fn parse_record_distribution_strategy(
    value: &Value,
) -> Result<Option<String>, AwsServiceError> {
    let Some(raw) = value.as_str() else {
        return Ok(None);
    };
    if raw != RECORD_DISTRIBUTION_AUTO && raw != RECORD_DISTRIBUTION_USER_PARTITION_KEY {
        return Err(validation_exception(format!(
            "1 validation error detected: Value '{raw}' at 'recordDistributionStrategy' failed \
             to satisfy constraint: Member must satisfy enum value set: [AUTO, USER_PARTITION_KEY]"
        )));
    }
    Ok(Some(raw.to_string()))
}

pub(crate) fn auto_requires_on_demand() -> AwsServiceError {
    invalid_argument(
        "The AUTO record distribution strategy is only supported for streams in ON_DEMAND capacity mode",
    )
}

pub(crate) fn validation_exception(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", message)
}

pub(crate) fn stream_not_found(account_id: &str, stream_name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("Stream {stream_name} under account {account_id} not found."),
    )
}

pub(crate) fn expired_iterator() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ExpiredIteratorException",
        "Shard iterator is expired or invalid.",
    )
}

/// Numeric discriminator from a shard id like `shardId-000000000003` -> 3,
/// clamped to 5 digits; 0 when there is no numeric suffix. Packed into the
/// low digits of each sequence number so they are unique stream-wide.
pub(crate) fn shard_discriminator(shard_id: &str) -> u32 {
    let digits: String = shard_id
        .rsplit('-')
        .next()
        .unwrap_or("")
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect();
    digits.parse::<u64>().unwrap_or(0).min(99_999) as u32
}

// --- Channels ---

/// `DataFreshnessInSeconds` when the caller omits it, and the range AWS
/// accepts for it (5 to 15 minutes).
pub(crate) const DEFAULT_CHANNEL_DATA_FRESHNESS_SECONDS: i64 = 300;
pub(crate) const MIN_CHANNEL_DATA_FRESHNESS_SECONDS: i64 = 300;
pub(crate) const MAX_CHANNEL_DATA_FRESHNESS_SECONDS: i64 = 900;

/// `S3StorageConfiguration.OutputKeyTemplate` when the caller omits it.
pub(crate) const DEFAULT_CHANNEL_OUTPUT_KEY_TEMPLATE: &str =
    "kinesis-channel/!{channel-name}/!{channel-id}/!{yyyy}/!{MM}/!{dd}/!{HH}/\
     !{channel-name}-!{channel-id}-!{yyyy}-!{MM}-!{dd}-!{HH}-!{mm}!{extension}";

/// `CloudWatchLogs.LogStreamName` when the caller omits it.
pub(crate) const DEFAULT_CHANNEL_LOG_STREAM_NAME: &str = "DestinationDelivery";

/// `ListChannels` returns at most 100 channels per page; a larger
/// `MaxResults` is clamped rather than rejected.
pub(crate) const MAX_LIST_CHANNELS_PAGE: usize = 100;

pub(crate) fn require_channel_name(body: &Value) -> Result<&str, AwsServiceError> {
    let name = body["ChannelName"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("ChannelName is required"))?;
    validate_string_length("ChannelName", name, 1, 128)?;
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    {
        return Err(validation_exception(
            "Value at 'channelName' failed to satisfy constraint: \
             Member must satisfy regular expression pattern: ^[a-zA-Z0-9_.-]+$",
        ));
    }
    Ok(name)
}

pub(crate) fn require_channel_arn(body: &Value) -> Result<&str, AwsServiceError> {
    let arn = body["ChannelARN"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("ChannelARN is required"))?;
    validate_string_length("ChannelARN", arn, 1, 2048)?;
    Ok(arn)
}

/// A required string member of a channel sub-structure.
pub(crate) fn require_channel_member<'a>(
    value: &'a Value,
    field: &str,
    max_len: usize,
) -> Result<&'a str, AwsServiceError> {
    let found = value[field]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument(format!("{field} is required")))?;
    validate_string_length(field, found, 1, max_len)?;
    Ok(found)
}

/// An optional enum member, defaulted when absent and rejected when it is not
/// one of the values the Smithy model lists.
fn channel_enum_member(
    value: &Value,
    field: &str,
    allowed: &[&str],
    default: Option<&str>,
) -> Result<String, AwsServiceError> {
    let found = match value[field].as_str().filter(|value| !value.is_empty()) {
        Some(found) => found,
        None => {
            return default
                .map(str::to_string)
                .ok_or_else(|| invalid_argument(format!("{field} is required")))
        }
    };
    if !allowed.contains(&found) {
        return Err(invalid_argument(format!(
            "{field} must be one of {}",
            allowed.join(", ")
        )));
    }
    Ok(found.to_string())
}

/// `DataFreshnessInSeconds` of an `UpdateChannel` destination update, where
/// the member is required rather than defaulted.
pub(crate) fn require_channel_data_freshness(value: &Value) -> Result<i32, AwsServiceError> {
    if value["DataFreshnessInSeconds"].is_null() {
        return Err(invalid_argument("DataFreshnessInSeconds is required"));
    }
    channel_data_freshness(&value["DataFreshnessInSeconds"])
}

/// `DataFreshnessInSeconds`, defaulted to 300 and range-checked.
fn channel_data_freshness(value: &Value) -> Result<i32, AwsServiceError> {
    validate_optional_json_range(
        "DataFreshnessInSeconds",
        value,
        MIN_CHANNEL_DATA_FRESHNESS_SECONDS,
        MAX_CHANNEL_DATA_FRESHNESS_SECONDS,
    )?;
    Ok(value
        .as_i64()
        .unwrap_or(DEFAULT_CHANNEL_DATA_FRESHNESS_SECONDS) as i32)
}

/// Parse `StreamConfigurationList`. Every `StreamARN` must name a stream that
/// exists in this account: AWS reports an unknown source stream as
/// `ResourceNotFoundException`.
pub(crate) fn parse_channel_streams(
    state: &crate::state::KinesisState,
    body: &Value,
) -> Result<Vec<KinesisChannelStream>, AwsServiceError> {
    let entries = body["StreamConfigurationList"]
        .as_array()
        .ok_or_else(|| invalid_argument("StreamConfigurationList is required"))?;
    if entries.is_empty() || entries.len() > 10000 {
        return Err(validation_exception(
            "Value at 'streamConfigurationList' failed to satisfy constraint: \
             Member must have length between 1 and 10000",
        ));
    }

    let mut streams = Vec::with_capacity(entries.len());
    for entry in entries {
        let stream_arn = require_channel_member(entry, "StreamARN", 2048)?;
        let stream_name = state
            .stream_name_from_arn(stream_arn)
            .ok_or_else(|| resource_not_found_arn(stream_arn))?;
        let stream = state
            .streams
            .get(&stream_name)
            .ok_or_else(|| resource_not_found_arn(stream_arn))?;

        let record_configuration = &entry["RecordConfiguration"];
        if !record_configuration.is_object() {
            return Err(invalid_argument("RecordConfiguration is required"));
        }
        let record_format_type = channel_enum_member(
            record_configuration,
            "RecordFormatType",
            &["GSR_JSON", "JSON", "STRING", "BYTE_ARRAY"],
            None,
        )?;
        let gsr_schema_arn = match record_configuration["GSRSchemaARN"]
            .as_str()
            .filter(|value| !value.is_empty())
        {
            Some(arn) => {
                // `GSRSchemaARN` is capped at 512, not the 2048 the other ARN
                // members share.
                validate_string_length("GSRSchemaARN", arn, 1, 512)?;
                Some(arn.to_string())
            }
            None => None,
        };

        streams.push(KinesisChannelStream {
            stream_arn: stream.stream_arn.clone(),
            stream_creation_timestamp: stream.stream_creation_timestamp,
            record_format_type,
            gsr_schema_arn,
        });
    }
    Ok(streams)
}

/// Parse the channel destination. Exactly one of the two destination shapes
/// must be supplied.
pub(crate) fn parse_channel_destination(
    body: &Value,
    channel_name: &str,
    channel_id: &str,
) -> Result<KinesisChannelDestination, AwsServiceError> {
    let s3 = &body["S3DestinationConfiguration"];
    let s3_tables = &body["S3TablesDestinationConfiguration"];
    match (s3.is_null(), s3_tables.is_null()) {
        (false, false) => Err(invalid_argument(
            "Specify either S3DestinationConfiguration or \
             S3TablesDestinationConfiguration, but not both",
        )),
        (true, true) => Err(invalid_argument(
            "Either S3DestinationConfiguration or S3TablesDestinationConfiguration is required",
        )),
        (false, true) => {
            let storage = parse_channel_storage(&s3["StorageConfiguration"])?;
            let dead_letter_queue = parse_channel_dead_letter_queue(
                &s3["DeadLetterQueueS3Configuration"],
                // A general purpose S3 destination may omit the dead-letter
                // queue; it then defaults to the destination bucket under an
                // error prefix.
                Some(&storage),
                channel_name,
                channel_id,
            )?;
            Ok(KinesisChannelDestination::S3 {
                data_freshness_in_seconds: channel_data_freshness(&s3["DataFreshnessInSeconds"])?,
                dead_letter_queue,
                storage,
            })
        }
        (true, false) => {
            let dead_letter_queue = parse_channel_dead_letter_queue(
                &s3_tables["DeadLetterQueueS3Configuration"],
                // Required for streaming tables: there is no destination
                // bucket to fall back to.
                None,
                channel_name,
                channel_id,
            )?;
            Ok(KinesisChannelDestination::S3Tables {
                data_freshness_in_seconds: channel_data_freshness(
                    &s3_tables["DataFreshnessInSeconds"],
                )?,
                dead_letter_queue,
                tables: parse_channel_tables(&s3_tables["S3TablesConfigurationList"])?,
            })
        }
    }
}

fn parse_channel_storage(value: &Value) -> Result<KinesisChannelS3Storage, AwsServiceError> {
    if !value.is_object() {
        return Err(invalid_argument("StorageConfiguration is required"));
    }
    let output_key_template = match value["OutputKeyTemplate"]
        .as_str()
        .filter(|value| !value.is_empty())
    {
        Some(template) => {
            validate_string_length("OutputKeyTemplate", template, 1, 1024)?;
            template.to_string()
        }
        None => DEFAULT_CHANNEL_OUTPUT_KEY_TEMPLATE.to_string(),
    };
    Ok(KinesisChannelS3Storage {
        bucket_arn: require_channel_member(value, "BucketARN", 2048)?.to_string(),
        expected_bucket_owner: require_expected_bucket_owner(value)?,
        output_key_template,
        storage_class: channel_enum_member(
            value,
            "StorageClass",
            &["STANDARD", "INTELLIGENT_TIERING", "GLACIER_IR"],
            Some("STANDARD"),
        )?,
        compression_type: channel_enum_member(
            value,
            "CompressionType",
            &["NONE", "GZIP", "ZSTD"],
            None,
        )?,
    })
}

fn require_expected_bucket_owner(value: &Value) -> Result<String, AwsServiceError> {
    let owner = value["ExpectedBucketOwner"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("ExpectedBucketOwner is required"))?;
    validate_string_length("ExpectedBucketOwner", owner, 12, 12)?;
    Ok(owner.to_string())
}

/// Parse `DeadLetterQueueS3Configuration`. When `fallback` is `Some` an absent
/// configuration defaults to that bucket under the channel's error prefix;
/// when it is `None` the configuration is required.
fn parse_channel_dead_letter_queue(
    value: &Value,
    fallback: Option<&KinesisChannelS3Storage>,
    channel_name: &str,
    channel_id: &str,
) -> Result<KinesisChannelDeadLetterQueue, AwsServiceError> {
    let default_prefix = format!("kinesis-channel/errors/{channel_name}/{channel_id}/");
    if !value.is_object() {
        let fallback = fallback.ok_or_else(|| {
            invalid_argument(
                "DeadLetterQueueS3Configuration is required for streaming table destinations",
            )
        })?;
        return Ok(KinesisChannelDeadLetterQueue {
            bucket_arn: fallback.bucket_arn.clone(),
            expected_bucket_owner: fallback.expected_bucket_owner.clone(),
            error_output_prefix: default_prefix,
        });
    }
    let error_output_prefix = match value["ErrorOutputPrefix"]
        .as_str()
        .filter(|value| !value.is_empty())
    {
        Some(prefix) => {
            validate_string_length("ErrorOutputPrefix", prefix, 1, 512)?;
            prefix.to_string()
        }
        None => default_prefix,
    };
    Ok(KinesisChannelDeadLetterQueue {
        bucket_arn: require_channel_member(value, "BucketARN", 2048)?.to_string(),
        expected_bucket_owner: require_expected_bucket_owner(value)?,
        error_output_prefix,
    })
}

fn parse_channel_tables(value: &Value) -> Result<Vec<KinesisChannelS3Table>, AwsServiceError> {
    let entries = value
        .as_array()
        .ok_or_else(|| invalid_argument("S3TablesConfigurationList is required"))?;
    if entries.is_empty() || entries.len() > 10000 {
        return Err(validation_exception(
            "Value at 's3TablesConfigurationList' failed to satisfy constraint: \
             Member must have length between 1 and 10000",
        ));
    }

    let mut tables = Vec::with_capacity(entries.len());
    for entry in entries {
        let partition_fields = match &entry["PartitionSpec"] {
            Value::Null => Vec::new(),
            spec => parse_channel_partition_fields(&spec["PartitionFields"])?,
        };
        tables.push(KinesisChannelS3Table {
            table_bucket_arn: require_channel_member(entry, "TableBucketARN", 2048)?.to_string(),
            namespace: require_channel_member(entry, "Namespace", 255)?.to_string(),
            table_name: require_channel_member(entry, "TableName", 255)?.to_string(),
            compression_type: channel_enum_member(
                entry,
                "CompressionType",
                &["NONE", "ZSTD", "SNAPPY"],
                None,
            )?,
            partition_fields,
        });
    }
    Ok(tables)
}

fn parse_channel_partition_fields(
    value: &Value,
) -> Result<Vec<KinesisChannelPartitionField>, AwsServiceError> {
    let entries = value
        .as_array()
        .ok_or_else(|| invalid_argument("PartitionFields is required"))?;
    if entries.is_empty() || entries.len() > 10 {
        return Err(validation_exception(
            "Value at 'partitionFields' failed to satisfy constraint: \
             Member must have length between 1 and 10",
        ));
    }
    entries
        .iter()
        .map(|entry| {
            Ok(KinesisChannelPartitionField {
                transform: channel_enum_member(entry, "Transform", &["TIME_HOUR"], None)?,
                source_name: require_channel_member(entry, "SourceName", 255)?.to_string(),
            })
        })
        .collect()
}

/// Parse `ChannelEncryptionConfiguration`. Absent means the channel uses no
/// customer managed key.
pub(crate) fn parse_channel_encryption(
    value: &Value,
) -> Result<Option<KinesisChannelEncryption>, AwsServiceError> {
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(KinesisChannelEncryption {
        encryption_type: channel_enum_member(value, "EncryptionType", &["KMS"], None)?,
        key_id: require_channel_member(value, "KeyId", 2048)?.to_string(),
    }))
}

/// Parse `ChannelLoggingConfiguration`. It is optional on `CreateChannel` but
/// required in `ChannelDescription`, so an absent one resolves to logging
/// disabled under the default log group and stream names.
pub(crate) fn parse_channel_logging(
    value: &Value,
    channel_name: &str,
    channel_id: &str,
) -> Result<KinesisChannelLogging, AwsServiceError> {
    let default_group = format!("/aws/kinesis/{channel_name}/{channel_id}");
    if value.is_null() {
        return Ok(KinesisChannelLogging {
            enabled: false,
            log_group_name: default_group,
            log_stream_name: DEFAULT_CHANNEL_LOG_STREAM_NAME.to_string(),
        });
    }
    let logs = &value["CloudWatchLogs"];
    if !logs.is_object() {
        return Err(invalid_argument("CloudWatchLogs is required"));
    }
    let enabled = logs["Enabled"]
        .as_bool()
        .ok_or_else(|| invalid_argument("Enabled is required"))?;
    let log_group_name = match logs["LogGroupName"]
        .as_str()
        .filter(|value| !value.is_empty())
    {
        Some(name) => {
            validate_string_length("LogGroupName", name, 1, 512)?;
            name.to_string()
        }
        None => default_group,
    };
    let log_stream_name = match logs["LogStreamName"]
        .as_str()
        .filter(|value| !value.is_empty())
    {
        Some(name) => {
            validate_string_length("LogStreamName", name, 1, 512)?;
            name.to_string()
        }
        None => DEFAULT_CHANNEL_LOG_STREAM_NAME.to_string(),
    };
    Ok(KinesisChannelLogging {
        enabled,
        log_group_name,
        log_stream_name,
    })
}

/// Render a channel as the `ChannelDescription` returned by CreateChannel,
/// DescribeChannel and UpdateChannel.
pub(crate) fn channel_description_json(channel: &KinesisChannel) -> Value {
    let streams: Vec<Value> = channel
        .streams
        .iter()
        .map(|source| {
            let mut record_configuration = json!({ "RecordFormatType": source.record_format_type });
            if let Some(ref arn) = source.gsr_schema_arn {
                record_configuration["GSRSchemaARN"] = json!(arn);
            }
            json!({
                "StreamARN": source.stream_arn,
                "StreamCreationTimestamp": epoch_seconds(source.stream_creation_timestamp),
                "RecordConfiguration": record_configuration,
            })
        })
        .collect();

    let mut description = json!({
        "ChannelName": channel.channel_name,
        "ChannelARN": channel.channel_arn,
        "ChannelId": channel.channel_id,
        "ChannelStatus": channel.channel_status,
        "ChannelCreationTimestamp": epoch_seconds(channel.channel_creation_timestamp),
        "ServiceExecutionRoleARN": channel.service_execution_role_arn,
        "StreamConfigurationList": streams,
        "LoggingConfiguration": {
            "CloudWatchLogs": {
                "Enabled": channel.logging.enabled,
                "LogGroupName": channel.logging.log_group_name,
                "LogStreamName": channel.logging.log_stream_name,
            }
        },
    });

    match &channel.destination {
        KinesisChannelDestination::S3 {
            data_freshness_in_seconds,
            dead_letter_queue,
            storage,
        } => {
            description["S3DestinationConfiguration"] = json!({
                "DataFreshnessInSeconds": data_freshness_in_seconds,
                "DeadLetterQueueS3Configuration": dead_letter_queue_json(dead_letter_queue),
                "StorageConfiguration": {
                    "BucketARN": storage.bucket_arn,
                    "ExpectedBucketOwner": storage.expected_bucket_owner,
                    "OutputKeyTemplate": storage.output_key_template,
                    "StorageClass": storage.storage_class,
                    "CompressionType": storage.compression_type,
                },
            });
        }
        KinesisChannelDestination::S3Tables {
            data_freshness_in_seconds,
            dead_letter_queue,
            tables,
        } => {
            let tables: Vec<Value> = tables
                .iter()
                .map(|table| {
                    let mut entry = json!({
                        "TableBucketARN": table.table_bucket_arn,
                        "Namespace": table.namespace,
                        "TableName": table.table_name,
                        "CompressionType": table.compression_type,
                    });
                    if !table.partition_fields.is_empty() {
                        entry["PartitionSpec"] = json!({
                            "PartitionFields": table
                                .partition_fields
                                .iter()
                                .map(|field| json!({
                                    "Transform": field.transform,
                                    "SourceName": field.source_name,
                                }))
                                .collect::<Vec<Value>>(),
                        });
                    }
                    entry
                })
                .collect();
            description["S3TablesDestinationConfiguration"] = json!({
                "DataFreshnessInSeconds": data_freshness_in_seconds,
                "DeadLetterQueueS3Configuration": dead_letter_queue_json(dead_letter_queue),
                "S3TablesConfigurationList": tables,
            });
        }
    }

    if let Some(ref encryption) = channel.encryption {
        description["EncryptionConfiguration"] = json!({
            "EncryptionType": encryption.encryption_type,
            "KeyId": encryption.key_id,
        });
    }
    description
}

fn dead_letter_queue_json(dead_letter_queue: &KinesisChannelDeadLetterQueue) -> Value {
    json!({
        "BucketARN": dead_letter_queue.bucket_arn,
        "ExpectedBucketOwner": dead_letter_queue.expected_bucket_owner,
        "ErrorOutputPrefix": dead_letter_queue.error_output_prefix,
    })
}

/// Render a channel as the `ChannelSummary` returned by ListChannels.
pub(crate) fn channel_summary_json(channel: &KinesisChannel) -> Value {
    json!({
        "ChannelName": channel.channel_name,
        "ChannelARN": channel.channel_arn,
        "ChannelId": channel.channel_id,
        "ChannelStatus": channel.channel_status,
        "ChannelCreationTimestamp": epoch_seconds(channel.channel_creation_timestamp),
        "ChannelDestinationType": channel.destination.destination_type(),
        "Streams": channel
            .streams
            .iter()
            .map(|source| json!({
                "StreamARN": source.stream_arn,
                "StreamCreationTimestamp": epoch_seconds(source.stream_creation_timestamp),
            }))
            .collect::<Vec<Value>>(),
    })
}

/// Timestamps go on the wire as epoch seconds with millisecond precision,
/// the same encoding the stream and consumer responses use.
fn epoch_seconds(timestamp: chrono::DateTime<Utc>) -> f64 {
    timestamp.timestamp_millis() as f64 / 1000.0
}

/// One parsed `ListChannels` `StreamFilter` entry.
pub(crate) struct ChannelStreamFilter {
    stream_arn: String,
    creation_timestamp_millis: Option<i64>,
}

/// Parse the `StreamFilter` list. Parsing up front (rather than per candidate
/// channel) means a malformed filter is rejected even when the account holds
/// no channels to evaluate it against.
///
/// Each `StreamARN` is resolved against `state` the same region-tolerant way
/// `CreateChannel` resolves a source ARN, then stored in the stream's own
/// canonical form, which is what a channel holds. Without that, a caller
/// whose credential scope names a different region than the stream's stored
/// ARN filters against a string no channel can ever carry. An ARN that names
/// no existing stream is kept verbatim: it simply matches nothing.
pub(crate) fn parse_channel_stream_filters(
    state: &crate::state::KinesisState,
    value: &Value,
) -> Result<Vec<ChannelStreamFilter>, AwsServiceError> {
    if value.is_null() {
        return Ok(Vec::new());
    }
    let entries = value
        .as_array()
        .ok_or_else(|| invalid_argument("StreamFilter must be a list"))?;
    if entries.is_empty() || entries.len() > 10000 {
        return Err(validation_exception(
            "Value at 'streamFilter' failed to satisfy constraint: \
             Member must have length between 1 and 10000",
        ));
    }
    entries
        .iter()
        .map(|entry| {
            let creation_timestamp_millis = match &entry["StreamCreationTimestamp"] {
                Value::Null => None,
                value => {
                    let seconds = value.as_f64().ok_or_else(|| {
                        invalid_argument("StreamCreationTimestamp must be an epoch timestamp")
                    })?;
                    Some((seconds * 1000.0).round() as i64)
                }
            };
            let stream_arn = require_channel_member(entry, "StreamARN", 2048)?;
            let stream_arn = state
                .stream_name_from_arn(stream_arn)
                .and_then(|name| state.streams.get(&name))
                .map_or_else(
                    || stream_arn.to_string(),
                    |stream| stream.stream_arn.clone(),
                );
            Ok(ChannelStreamFilter {
                stream_arn,
                creation_timestamp_millis,
            })
        })
        .collect()
}

/// A channel matches the filter list when any of its source streams matches
/// any filter entry.
pub(crate) fn channel_matches_stream_filters(
    channel: &KinesisChannel,
    filters: &[ChannelStreamFilter],
) -> bool {
    filters.iter().any(|filter| {
        channel.streams.iter().any(|source| {
            source.stream_arn == filter.stream_arn
                && filter.creation_timestamp_millis.is_none_or(|wanted| {
                    wanted == source.stream_creation_timestamp.timestamp_millis()
                })
        })
    })
}

/// Encode a `ListChannels` continuation token. Like ListStreams, the opaque
/// cursor wraps the last returned channel name so the next page resumes
/// strictly after it.
pub(crate) fn encode_list_channels_token(last_channel_name: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(last_channel_name)
}

/// Decode a `ListChannels` continuation token. A garbage token is an
/// `InvalidArgumentException`, matching the ListShards cursor.
pub(crate) fn decode_list_channels_token(token: &str) -> Result<String, AwsServiceError> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(token)
        .map_err(|_| invalid_argument("Invalid NextToken"))?;
    String::from_utf8(raw).map_err(|_| invalid_argument("Invalid NextToken"))
}

#[cfg(test)]
mod sequence_discriminator_tests {
    use super::shard_discriminator;

    #[test]
    fn parses_numeric_suffix() {
        assert_eq!(shard_discriminator("shardId-000000000000"), 0);
        assert_eq!(shard_discriminator("shardId-000000000003"), 3);
        assert_eq!(shard_discriminator("shardId-000000000042"), 42);
    }

    #[test]
    fn distinct_shards_differ() {
        assert_ne!(
            shard_discriminator("shardId-000000000001"),
            shard_discriminator("shardId-000000000002")
        );
    }

    #[test]
    fn handles_missing_suffix() {
        assert_eq!(shard_discriminator("weird"), 0);
        assert_eq!(shard_discriminator(""), 0);
    }
}
