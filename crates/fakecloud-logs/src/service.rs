use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{LogEvent, LogGroup, LogStream, SharedLogsState};

pub struct LogsService {
    state: SharedLogsState,
}

impl LogsService {
    pub fn new(state: SharedLogsState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for LogsService {
    fn service_name(&self) -> &str {
        "logs"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateLogGroup" => self.create_log_group(&req),
            "DeleteLogGroup" => self.delete_log_group(&req),
            "DescribeLogGroups" => self.describe_log_groups(&req),
            "CreateLogStream" => self.create_log_stream(&req),
            "DeleteLogStream" => self.delete_log_stream(&req),
            "DescribeLogStreams" => self.describe_log_streams(&req),
            "PutLogEvents" => self.put_log_events(&req),
            "GetLogEvents" => self.get_log_events(&req),
            "FilterLogEvents" => self.filter_log_events(&req),
            "TagLogGroup" => self.tag_log_group(&req),
            "UntagLogGroup" => self.untag_log_group(&req),
            "ListTagsLogGroup" => self.list_tags_log_group(&req),
            "PutRetentionPolicy" => self.put_retention_policy(&req),
            "DeleteRetentionPolicy" => self.delete_retention_policy(&req),
            _ => Err(AwsServiceError::action_not_implemented("logs", &req.action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateLogGroup",
            "DeleteLogGroup",
            "DescribeLogGroups",
            "CreateLogStream",
            "DeleteLogStream",
            "DescribeLogStreams",
            "PutLogEvents",
            "GetLogEvents",
            "FilterLogEvents",
            "TagLogGroup",
            "UntagLogGroup",
            "ListTagsLogGroup",
            "PutRetentionPolicy",
            "DeleteRetentionPolicy",
        ]
    }
}

fn body_json(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

impl LogsService {
    fn create_log_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupName is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();
        if state.log_groups.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceAlreadyExistsException",
                format!("The specified log group already exists: {name}"),
            ));
        }

        let arn = format!(
            "arn:aws:logs:{}:{}:log-group:{}:*",
            state.region, state.account_id, name
        );
        let now = Utc::now().timestamp_millis();

        let tags = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        state.log_groups.insert(
            name.clone(),
            LogGroup {
                name,
                arn,
                creation_time: now,
                retention_in_days: None,
                tags,
                log_streams: std::collections::HashMap::new(),
                stored_bytes: 0,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_log_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        let mut state = self.state.write();
        if state.log_groups.remove(name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn describe_log_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let prefix = body["logGroupNamePrefix"].as_str().unwrap_or("");
        let limit = body["limit"].as_i64().unwrap_or(50) as usize;

        let state = self.state.read();
        let mut groups: Vec<&LogGroup> = state
            .log_groups
            .values()
            .filter(|g| prefix.is_empty() || g.name.starts_with(prefix))
            .collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));
        groups.truncate(limit);

        let log_groups: Vec<Value> = groups
            .iter()
            .map(|g| {
                let mut obj = json!({
                    "logGroupName": g.name,
                    "arn": g.arn,
                    "creationTime": g.creation_time,
                    "storedBytes": g.stored_bytes,
                    "metricFilterCount": 0,
                });
                if let Some(days) = g.retention_in_days {
                    obj["retentionInDays"] = json!(days);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logGroups": log_groups })).unwrap(),
        ))
    }

    fn create_log_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let stream_name = body["logStreamName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logStreamName is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();

        // Extract region/account before mutable borrow of log_groups
        let region = state.region.clone();
        let account_id = state.account_id.clone();

        let group = state.log_groups.get_mut(group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        if group.log_streams.contains_key(&stream_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceAlreadyExistsException",
                format!("The specified log stream already exists: {stream_name}"),
            ));
        }

        let arn = format!(
            "arn:aws:logs:{region}:{account_id}:log-group:{group_name}:log-stream:{stream_name}",
        );
        let now = Utc::now().timestamp_millis();

        group.log_streams.insert(
            stream_name.clone(),
            LogStream {
                name: stream_name,
                arn,
                creation_time: now,
                first_event_timestamp: None,
                last_event_timestamp: None,
                last_ingestion_time: None,
                upload_sequence_token: "1".to_string(),
                events: Vec::new(),
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_log_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let stream_name = body["logStreamName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logStreamName is required",
            )
        })?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        if group.log_streams.remove(stream_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log stream does not exist: {stream_name}"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn describe_log_streams(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let prefix = body["logStreamNamePrefix"].as_str().unwrap_or("");
        let limit = body["limit"].as_i64().unwrap_or(50) as usize;

        let state = self.state.read();
        let group = state.log_groups.get(group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        let mut streams: Vec<&LogStream> = group
            .log_streams
            .values()
            .filter(|s| prefix.is_empty() || s.name.starts_with(prefix))
            .collect();
        streams.sort_by(|a, b| a.name.cmp(&b.name));
        streams.truncate(limit);

        let log_streams: Vec<Value> = streams
            .iter()
            .map(|s| {
                let mut obj = json!({
                    "logStreamName": s.name,
                    "arn": s.arn,
                    "creationTime": s.creation_time,
                    "uploadSequenceToken": s.upload_sequence_token,
                });
                if let Some(ts) = s.first_event_timestamp {
                    obj["firstEventTimestamp"] = json!(ts);
                }
                if let Some(ts) = s.last_event_timestamp {
                    obj["lastEventTimestamp"] = json!(ts);
                }
                if let Some(ts) = s.last_ingestion_time {
                    obj["lastIngestionTime"] = json!(ts);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logStreams": log_streams })).unwrap(),
        ))
    }

    fn put_log_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let stream_name = body["logStreamName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logStreamName is required",
            )
        })?;

        let log_events = body["logEvents"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logEvents is required",
            )
        })?;

        let now = Utc::now().timestamp_millis();
        let mut new_events: Vec<LogEvent> = log_events
            .iter()
            .map(|e| LogEvent {
                timestamp: e["timestamp"].as_i64().unwrap_or(now),
                message: e["message"].as_str().unwrap_or("").to_string(),
                ingestion_time: now,
            })
            .collect();

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        let stream = group.log_streams.get_mut(stream_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log stream does not exist: {stream_name}"),
            )
        })?;

        // Update stream metadata
        for event in &new_events {
            if stream.first_event_timestamp.is_none()
                || Some(event.timestamp) < stream.first_event_timestamp
            {
                stream.first_event_timestamp = Some(event.timestamp);
            }
            if stream.last_event_timestamp.is_none()
                || Some(event.timestamp) > stream.last_event_timestamp
            {
                stream.last_event_timestamp = Some(event.timestamp);
            }
            group.stored_bytes += event.message.len() as i64 + 26; // 26 bytes overhead per event
        }
        stream.last_ingestion_time = Some(now);

        // Increment sequence token
        let next_token: u64 = stream.upload_sequence_token.parse().unwrap_or(0) + 1;
        stream.upload_sequence_token = next_token.to_string();

        stream.events.append(&mut new_events);
        // Sort events by timestamp
        stream.events.sort_by_key(|e| e.timestamp);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "nextSequenceToken": stream.upload_sequence_token,
            }))
            .unwrap(),
        ))
    }

    fn get_log_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let stream_name = body["logStreamName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logStreamName is required",
            )
        })?;
        let start_time = body["startTime"].as_i64();
        let end_time = body["endTime"].as_i64();
        let limit = body["limit"].as_i64().unwrap_or(10000) as usize;
        let start_from_head = body["startFromHead"].as_bool().unwrap_or(false);

        let state = self.state.read();
        let group = state.log_groups.get(group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        let stream = group.log_streams.get(stream_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log stream does not exist: {stream_name}"),
            )
        })?;

        let mut events: Vec<&LogEvent> = stream
            .events
            .iter()
            .filter(|e| {
                if let Some(start) = start_time {
                    if e.timestamp < start {
                        return false;
                    }
                }
                if let Some(end) = end_time {
                    if e.timestamp >= end {
                        return false;
                    }
                }
                true
            })
            .collect();

        if !start_from_head {
            events.reverse();
        }
        events.truncate(limit);
        if !start_from_head {
            events.reverse();
        }

        let events_json: Vec<Value> = events
            .iter()
            .map(|e| {
                json!({
                    "timestamp": e.timestamp,
                    "message": e.message,
                    "ingestionTime": e.ingestion_time,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "events": events_json,
                "nextForwardToken": "f/0",
                "nextBackwardToken": "b/0",
            }))
            .unwrap(),
        ))
    }

    fn filter_log_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let filter_pattern = body["filterPattern"].as_str().unwrap_or("");
        let start_time = body["startTime"].as_i64();
        let end_time = body["endTime"].as_i64();
        let limit = body["limit"].as_i64().unwrap_or(10000) as usize;
        let stream_names: Vec<&str> = body["logStreamNames"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let state = self.state.read();
        let group = state.log_groups.get(group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        let mut filtered_events: Vec<Value> = Vec::new();

        let streams: Vec<(&String, &LogStream)> = if stream_names.is_empty() {
            group.log_streams.iter().collect()
        } else {
            group
                .log_streams
                .iter()
                .filter(|(name, _)| stream_names.contains(&name.as_str()))
                .collect()
        };

        for (_, stream) in streams {
            for event in &stream.events {
                if let Some(start) = start_time {
                    if event.timestamp < start {
                        continue;
                    }
                }
                if let Some(end) = end_time {
                    if event.timestamp >= end {
                        continue;
                    }
                }
                // Simple substring matching for filter pattern
                if !filter_pattern.is_empty() && !event.message.contains(filter_pattern) {
                    continue;
                }

                filtered_events.push(json!({
                    "logStreamName": stream.name,
                    "timestamp": event.timestamp,
                    "message": event.message,
                    "ingestionTime": event.ingestion_time,
                    "eventId": format!("{}-{}", stream.name, event.timestamp),
                }));
            }
        }

        filtered_events.sort_by_key(|e| e["timestamp"].as_i64().unwrap_or(0));
        filtered_events.truncate(limit);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "events": filtered_events,
                "searchedLogStreams": [],
            }))
            .unwrap(),
        ))
    }

    fn tag_log_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let tags = body["tags"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "tags is required",
            )
        })?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        for (k, v) in tags {
            group
                .tags
                .insert(k.clone(), v.as_str().unwrap_or("").to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_log_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let keys = body["tags"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "tags is required",
            )
        })?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        for key in keys {
            if let Some(k) = key.as_str() {
                group.tags.remove(k);
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_tags_log_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        let state = self.state.read();
        let group = state.log_groups.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "tags": group.tags })).unwrap(),
        ))
    }

    fn put_retention_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let days = body["retentionInDays"].as_i64().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "retentionInDays is required",
            )
        })?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        group.retention_in_days = Some(days as i32);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_retention_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        group.retention_in_days = None;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}
