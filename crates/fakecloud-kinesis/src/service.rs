use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{KinesisStream, SharedKinesisState};

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateStream",
    "DescribeStream",
    "DescribeStreamSummary",
    "ListStreams",
    "DeleteStream",
    "AddTagsToStream",
    "ListTagsForStream",
    "RemoveTagsFromStream",
    "IncreaseStreamRetentionPeriod",
    "DecreaseStreamRetentionPeriod",
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
            "AddTagsToStream" => self.add_tags_to_stream(&request),
            "ListTagsForStream" => self.list_tags_for_stream(&request),
            "RemoveTagsFromStream" => self.remove_tags_from_stream(&request),
            "IncreaseStreamRetentionPeriod" => self.increase_stream_retention_period(&request),
            "DecreaseStreamRetentionPeriod" => self.decrease_stream_retention_period(&request),
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
            shard_count: shard_count as i32,
            open_shard_count: shard_count as i32,
            tags: std::collections::HashMap::new(),
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
        let selected: Vec<String> = names.into_iter().skip(start).take(limit as usize).collect();

        Ok(AwsResponse::ok_json(json!({
            "HasMoreStreams": false,
            "StreamNames": selected,
            "StreamSummaries": []
        })))
    }

    fn delete_stream(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let stream_name = resolve_stream_name(&self.state.read(), &body)?;

        let mut state = self.state.write();
        if state.streams.remove(&stream_name).is_none() {
            return Err(stream_not_found(&stream_name));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn add_tags_to_stream(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let mut state = self.state.write();
        let stream_name = resolve_stream_name(&state, &body)?;
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&stream_name))?;

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
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&stream_name))?;

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
        let stream = state
            .streams
            .get_mut(&stream_name)
            .ok_or_else(|| stream_not_found(&stream_name))?;

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
}

impl crate::state::KinesisState {
    fn lookup_stream(&self, body: &Value) -> Result<&KinesisStream, AwsServiceError> {
        let stream_name = resolve_stream_name(self, body)?;
        self.streams
            .get(&stream_name)
            .ok_or_else(|| stream_not_found(&stream_name))
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
            return Err(stream_not_found(stream_name));
        }
    }

    Err(invalid_argument("StreamName or StreamARN is required"))
}

fn build_shards(shard_count: i32) -> Vec<Value> {
    (0..shard_count)
        .map(|index| {
            json!({
                "HashKeyRange": {
                    "EndingHashKey": "340282366920938463463374607431768211455",
                    "StartingHashKey": "0"
                },
                "SequenceNumberRange": {
                    "StartingSequenceNumber": format!("{}0000000000000000000", index + 1)
                },
                "ShardId": format!("shardId-{:012}", index)
            })
        })
        .collect()
}

fn invalid_argument(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgumentException", message)
}

fn stream_not_found(stream_name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("Stream {stream_name} under account 123456789012 not found."),
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
}
