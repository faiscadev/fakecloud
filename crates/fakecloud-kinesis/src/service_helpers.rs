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

/// Actions that mutate Kinesis state. Note: GetShardIterator also
/// writes (it records a lease in `iterators`), and GetRecords advances
/// that lease, so both are treated as mutating.
pub(crate) fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "CreateStream"
            | "DeleteStream"
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
            | "UpdateStreamWarmThroughput"
            | "UpdateMaxRecordSize"
            | "RegisterStreamConsumer"
            | "DeregisterStreamConsumer"
            | "MergeShards"
            | "SplitShard"
            | "UpdateShardCount"
            | "GetShardIterator"
            | "GetRecords"
    )
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
            if state.streams.contains_key(stream_name) {
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

pub(crate) fn require_partition_key(body: &Value) -> Result<&str, AwsServiceError> {
    body["PartitionKey"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("PartitionKey is required"))
}

pub(crate) fn require_shard_id(body: &Value) -> Result<&str, AwsServiceError> {
    body["ShardId"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("ShardId is required"))
}

pub(crate) fn require_resource_arn(body: &Value) -> Result<&str, AwsServiceError> {
    body["ResourceARN"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("ResourceARN is required"))
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
    let hash = routing_hash(partition_key, explicit_hash_key)?;
    let idx = select_shard_index_for_hash(stream, hash);
    Ok(&mut stream.shards[idx])
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
    let partition_key = entry["PartitionKey"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "PartitionKey is required".to_string())?;
    let data = decode_record_data(&entry["Data"]).map_err(|error| error.message())?;
    let explicit_hash_key = entry["ExplicitHashKey"].as_str();
    let shard = select_shard_mut(stream, partition_key, explicit_hash_key)
        .map_err(|error| error.message())?;
    let sequence_number = append_record(shard, partition_key, data);
    Ok((shard.shard_id.clone(), sequence_number))
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
            find_record_index_by_sequence_number(shard, sequence_number)
        }
        "AFTER_SEQUENCE_NUMBER" => {
            let sequence_number = require_starting_sequence_number(body)?;
            Ok(find_record_index_by_sequence_number(shard, sequence_number)? + 1)
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

pub(crate) fn find_record_index_by_sequence_number(
    shard: &KinesisShard,
    sequence_number: &str,
) -> Result<usize, AwsServiceError> {
    shard
        .records
        .iter()
        .position(|record| record.sequence_number == sequence_number)
        .ok_or_else(|| invalid_argument("StartingSequenceNumber is invalid"))
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
