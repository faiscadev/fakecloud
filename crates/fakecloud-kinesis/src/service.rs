use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use md5::{Digest, Md5};
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::{
    validate_optional_json_range, validate_optional_string_length, validate_string_length,
};

use crate::state::{
    KinesisConsumer, KinesisRecord, KinesisShard, KinesisStream, SharedKinesisState,
};

const SUPPORTED_ACTIONS: &[&str] = &[
    "AddTagsToStream",
    "CreateStream",
    "DecreaseStreamRetentionPeriod",
    "DeleteStream",
    "DeregisterStreamConsumer",
    "DescribeStream",
    "DescribeStreamConsumer",
    "DescribeStreamSummary",
    "GetRecords",
    "GetShardIterator",
    "IncreaseStreamRetentionPeriod",
    "ListStreamConsumers",
    "ListStreams",
    "ListTagsForStream",
    "PutRecord",
    "PutRecords",
    "RegisterStreamConsumer",
    "RemoveTagsFromStream",
];

pub struct KinesisService {
    state: SharedKinesisState,
}

impl KinesisService {
    pub fn new(state: SharedKinesisState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for KinesisService {
    fn service_name(&self) -> &str {
        "kinesis"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match request.action.as_str() {
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
            "RegisterStreamConsumer" => self.register_stream_consumer(&request),
            "DeregisterStreamConsumer" => self.deregister_stream_consumer(&request),
            "DescribeStreamConsumer" => self.describe_stream_consumer(&request),
            "ListStreamConsumers" => self.list_stream_consumers(&request),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                &request.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }
}

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

        let mut state = self.state.write();
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

        let stream = KinesisStream {
            stream_name: stream_name.to_string(),
            stream_arn: state.stream_arn(stream_name),
            stream_status: "ACTIVE".to_string(),
            stream_creation_timestamp: Utc::now(),
            retention_period_hours: 24,
            stream_mode: "PROVISIONED".to_string(),
            encryption_type: "NONE".to_string(),
            shard_count,
            open_shard_count: shard_count,
            tags: std::collections::HashMap::new(),
            shards: build_stream_shards(shard_count),
        };
        state.streams.insert(stream_name.to_string(), stream);

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_stream(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let state = self.state.read();
        let stream = state.lookup_stream(&body)?;

        Ok(AwsResponse::ok_json(json!({
            "StreamDescription": {
                "EncryptionType": stream.encryption_type,
                "HasMoreShards": false,
                "RetentionPeriodHours": stream.retention_period_hours,
                "Shards": build_shards(stream.shard_count),
                "StreamARN": stream.stream_arn,
                "StreamCreationTimestamp": stream.stream_creation_timestamp.timestamp_millis() as f64 / 1000.0,
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
        let state = self.state.read();
        let stream = state.lookup_stream(&body)?;

        Ok(AwsResponse::ok_json(json!({
            "StreamDescriptionSummary": {
                "ConsumerCount": 0,
                "EncryptionType": stream.encryption_type,
                "KeyId": Value::Null,
                "OpenShardCount": stream.open_shard_count,
                "RetentionPeriodHours": stream.retention_period_hours,
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

    fn list_streams(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let exclusive_start = body["ExclusiveStartStreamName"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(i64::MAX);
        if limit <= 0 {
            return Err(invalid_argument("Limit must be greater than zero"));
        }

        let state = self.state.read();
        let mut names: Vec<String> = state.streams.keys().cloned().collect();
        names.sort();

        let start = exclusive_start
            .and_then(|name| {
                names
                    .iter()
                    .position(|candidate| candidate == name)
                    .map(|idx| idx + 1)
            })
            .unwrap_or(0);
        let remaining = names.len().saturating_sub(start);
        let page_len = remaining.min(limit as usize);
        let has_more_streams = remaining > page_len;
        let selected: Vec<String> = names.into_iter().skip(start).take(page_len).collect();

        Ok(AwsResponse::ok_json(json!({
            "HasMoreStreams": has_more_streams,
            "StreamNames": selected,
            "StreamSummaries": []
        })))
    }

    fn delete_stream(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let stream_name = resolve_stream_name(&self.state.read(), &body)?;

        let mut state = self.state.write();
        let stream = state.streams.remove(&stream_name);
        if stream.is_none() {
            return Err(stream_not_found(&state.account_id, &stream_name));
        }
        // Clean up consumers associated with this stream
        let stream_arn = state.stream_arn(&stream_name);
        state.consumers.retain(|_, c| c.stream_arn != stream_arn);

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn add_tags_to_stream(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let mut state = self.state.write();
        let stream_name = resolve_stream_name(&state, &body)?;
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
        let mut state = self.state.write();
        let stream_name = resolve_stream_name(&state, &body)?;
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
        let mut state = self.state.write();
        let iterator = body["ShardIterator"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_argument("ShardIterator is required"))?
            .to_string();
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

        let end_index = shard
            .records
            .len()
            .min(lease.next_record_index.saturating_add(limit));
        let records: Vec<Value> = shard.records[lease.next_record_index..end_index]
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

        let millis_behind_latest = if end_index < shard.records.len() {
            1
        } else {
            0
        };
        let next_iterator = state.insert_iterator(&lease.stream_name, &lease.shard_id, end_index);

        Ok(AwsResponse::ok_json(json!({
            "MillisBehindLatest": millis_behind_latest,
            "NextShardIterator": next_iterator,
            "Records": records,
        })))
    }

    fn put_record(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let mut state = self.state.write();
        let stream_name = resolve_stream_name(&state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        let partition_key = require_partition_key(&body)?;
        let data = decode_record_data(&body["Data"])?;
        let encryption_type = stream.encryption_type.clone();
        let shard = select_shard_mut(stream, partition_key);
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
        let mut state = self.state.write();
        let stream_name = resolve_stream_name(&state, &body)?;
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
        let state = self.state.read();
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
        let mut state = self.state.write();
        let stream_name = resolve_stream_name(&state, &body)?;
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

        let mut state = self.state.write();
        let stream_name = resolve_stream_name(&state, &body)?;
        let account_id = state.account_id.clone();
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&account_id, &stream_name))?;

        if increasing && hours <= stream.retention_period_hours as i64 {
            return Err(invalid_argument(
                "RetentionPeriodHours must be greater than the current retention period",
            ));
        }
        if !increasing && hours >= stream.retention_period_hours as i64 {
            return Err(invalid_argument(
                "RetentionPeriodHours must be less than the current retention period",
            ));
        }

        stream.retention_period_hours = hours as i32;
        Ok(AwsResponse::ok_json(json!({})))
    }

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

        let mut state = self.state.write();
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

        // Check for duplicate consumer name on this stream
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
        let mut state = self.state.write();

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
        let state = self.state.read();

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

        let state = self.state.read();
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

        // Handle NextToken-based pagination
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
}

impl crate::state::KinesisState {
    fn lookup_stream(&self, body: &Value) -> Result<&KinesisStream, AwsServiceError> {
        let stream_name = resolve_stream_name(self, body)?;
        self.streams
            .get(&stream_name)
            .ok_or_else(|| stream_not_found(&self.account_id, &stream_name))
    }
}

fn require_stream_name(body: &Value) -> Result<&str, AwsServiceError> {
    body["StreamName"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("StreamName is required"))
}

fn resolve_stream_name(
    state: &crate::state::KinesisState,
    body: &Value,
) -> Result<String, AwsServiceError> {
    if let Some(stream_name) = body["StreamName"]
        .as_str()
        .filter(|value| !value.is_empty())
    {
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

fn build_shards(shard_count: i32) -> Vec<Value> {
    build_stream_shards(shard_count)
        .into_iter()
        .map(|shard| {
            json!({
                "HashKeyRange": {
                    "EndingHashKey": "340282366920938463463374607431768211455",
                    "StartingHashKey": "0"
                },
                "SequenceNumberRange": {
                    "StartingSequenceNumber": format!("{}0000000000000000000", 1)
                },
                "ShardId": shard.shard_id
            })
        })
        .collect()
}

fn build_stream_shards(shard_count: i32) -> Vec<KinesisShard> {
    (0..shard_count)
        .map(|index| KinesisShard {
            shard_id: format!("shardId-{:012}", index),
            next_sequence_number: 1,
            records: Vec::new(),
        })
        .collect()
}

fn require_partition_key(body: &Value) -> Result<&str, AwsServiceError> {
    body["PartitionKey"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("PartitionKey is required"))
}

fn require_shard_id(body: &Value) -> Result<&str, AwsServiceError> {
    body["ShardId"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("ShardId is required"))
}

fn decode_record_data(value: &Value) -> Result<Vec<u8>, AwsServiceError> {
    let encoded = value
        .as_str()
        .ok_or_else(|| invalid_argument("Data must be a base64 string"))?;
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| invalid_argument("Data must be valid base64"))
}

fn select_shard_mut<'a>(
    stream: &'a mut KinesisStream,
    partition_key: &str,
) -> &'a mut KinesisShard {
    let shard_index = partition_key_to_shard_index(partition_key, stream.shards.len());
    &mut stream.shards[shard_index]
}

fn partition_key_to_shard_index(partition_key: &str, shard_count: usize) -> usize {
    let digest = Md5::digest(partition_key.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    (u64::from_be_bytes(bytes) as usize) % shard_count
}

fn append_record(shard: &mut KinesisShard, partition_key: &str, data: Vec<u8>) -> String {
    let sequence_number = format!("{:020}", shard.next_sequence_number);
    shard.next_sequence_number += 1;
    shard.records.push(KinesisRecord {
        sequence_number: sequence_number.clone(),
        partition_key: partition_key.to_string(),
        data,
        approximate_arrival_timestamp: Utc::now(),
    });
    sequence_number
}

fn put_records_entry(
    stream: &mut KinesisStream,
    entry: &Value,
) -> Result<(String, String), String> {
    let partition_key = entry["PartitionKey"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "PartitionKey is required".to_string())?;
    let data = decode_record_data(&entry["Data"]).map_err(|error| error.message())?;
    let shard = select_shard_mut(stream, partition_key);
    let sequence_number = append_record(shard, partition_key, data);
    Ok((shard.shard_id.clone(), sequence_number))
}

fn shard_iterator_start_index(
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
        _ => Err(invalid_argument("Unsupported ShardIteratorType")),
    }
}

fn require_starting_sequence_number(body: &Value) -> Result<&str, AwsServiceError> {
    body["StartingSequenceNumber"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_argument("StartingSequenceNumber is required"))
}

fn find_record_index_by_sequence_number(
    shard: &KinesisShard,
    sequence_number: &str,
) -> Result<usize, AwsServiceError> {
    shard
        .records
        .iter()
        .position(|record| record.sequence_number == sequence_number)
        .ok_or_else(|| invalid_argument("StartingSequenceNumber is invalid"))
}

fn validate_stream_id(body: &Value) -> Result<(), AwsServiceError> {
    validate_optional_string_length("StreamId", body["StreamId"].as_str(), 1, 24)
}

fn resource_not_found_arn(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("Resource {arn} not found."),
    )
}

fn invalid_argument(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgumentException", message)
}

fn stream_not_found(account_id: &str, stream_name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("Stream {stream_name} under account {account_id} not found."),
    )
}

fn expired_iterator() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ExpiredIteratorException",
        "Shard iterator is expired or invalid.",
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;

    use super::*;
    use crate::state::KinesisState;

    fn request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "kinesis".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "req-1".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            path_segments: Vec::new(),
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    #[test]
    fn create_stream_stores_metadata() {
        let state = Arc::new(RwLock::new(KinesisState::new("123456789012", "us-east-1")));
        let service = KinesisService::new(state.clone());

        service
            .create_stream(&request(
                "CreateStream",
                json!({ "StreamName": "orders", "ShardCount": 2 }),
            ))
            .unwrap();

        let state = state.read();
        let stream = state.streams.get("orders").unwrap();
        assert_eq!(stream.stream_status, "ACTIVE");
        assert_eq!(stream.shard_count, 2);
        assert_eq!(stream.retention_period_hours, 24);
        assert!(stream.stream_arn.ends_with(":stream/orders"));
    }

    #[test]
    fn create_stream_rejects_duplicate_names() {
        let state = Arc::new(RwLock::new(KinesisState::new("123456789012", "us-east-1")));
        let service = KinesisService::new(state.clone());

        service
            .create_stream(&request(
                "CreateStream",
                json!({ "StreamName": "orders", "ShardCount": 1 }),
            ))
            .unwrap();

        let error = service
            .create_stream(&request(
                "CreateStream",
                json!({ "StreamName": "orders", "ShardCount": 1 }),
            ))
            .err()
            .expect("duplicate stream should fail");
        assert_eq!(error.code(), "ResourceInUseException");
    }

    #[test]
    fn update_retention_period_validates_direction() {
        let state = Arc::new(RwLock::new(KinesisState::new("123456789012", "us-east-1")));
        let service = KinesisService::new(state.clone());

        service
            .create_stream(&request(
                "CreateStream",
                json!({ "StreamName": "orders", "ShardCount": 1 }),
            ))
            .unwrap();

        let error = service
            .decrease_stream_retention_period(&request(
                "DecreaseStreamRetentionPeriod",
                json!({ "StreamName": "orders", "RetentionPeriodHours": 48 }),
            ))
            .err()
            .expect("invalid retention decrease should fail");
        assert_eq!(error.code(), "InvalidArgumentException");
    }

    #[test]
    fn partition_keys_route_deterministically() {
        let shard_a = partition_key_to_shard_index("customer-1", 4);
        let shard_b = partition_key_to_shard_index("customer-1", 4);
        let shard_c = partition_key_to_shard_index("customer-2", 4);

        assert_eq!(shard_a, shard_b);
        assert!(shard_c < 4);
    }

    #[test]
    fn append_record_advances_sequence_numbers() {
        let mut shard = KinesisShard {
            shard_id: "shardId-000000000000".to_string(),
            next_sequence_number: 1,
            records: Vec::new(),
        };

        let first = append_record(&mut shard, "key", b"first".to_vec());
        let second = append_record(&mut shard, "key", b"second".to_vec());

        assert_eq!(first, "00000000000000000001");
        assert_eq!(second, "00000000000000000002");
        assert_eq!(shard.records.len(), 2);
    }

    #[test]
    fn trim_horizon_iterator_starts_at_zero() {
        let mut shard = KinesisShard {
            shard_id: "shardId-000000000000".to_string(),
            next_sequence_number: 1,
            records: Vec::new(),
        };
        append_record(&mut shard, "key", b"first".to_vec());

        let index = shard_iterator_start_index(&shard, "TRIM_HORIZON", &json!({})).unwrap();
        assert_eq!(index, 0);
    }

    #[test]
    fn latest_iterator_starts_after_existing_records() {
        let mut shard = KinesisShard {
            shard_id: "shardId-000000000000".to_string(),
            next_sequence_number: 1,
            records: Vec::new(),
        };
        append_record(&mut shard, "key", b"first".to_vec());
        append_record(&mut shard, "key", b"second".to_vec());

        let index = shard_iterator_start_index(&shard, "LATEST", &json!({})).unwrap();
        assert_eq!(index, 2);
    }

    #[test]
    fn insert_iterator_purges_expired_leases() {
        let mut state = crate::state::KinesisState::new("123456789012", "us-east-1");
        state.iterators.insert(
            "expired".to_string(),
            crate::state::ShardIteratorLease {
                iterator_token: "expired".to_string(),
                stream_name: "stream".to_string(),
                shard_id: "shardId-000000000000".to_string(),
                next_record_index: 0,
                expires_at: Utc::now() - chrono::Duration::minutes(1),
            },
        );

        let token = state.insert_iterator("stream", "shardId-000000000000", 0);

        assert!(state.iterators.contains_key(&token));
        assert!(!state.iterators.contains_key("expired"));
    }
}
