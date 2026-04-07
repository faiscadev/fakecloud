use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{body_json, validation_error, LogsService};
use base64::Engine;
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::HashMap;
use std::io::Write;

use super::{extract_log_group_from_arn, generate_sequence_token, matches_filter_pattern};
use crate::state::{LogEvent, LogStream};
use crate::transformer;

impl LogsService {
    // ---- Log Streams ----

    pub(crate) fn create_log_stream(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

        validate_string_length("logGroupName", group_name, 1, 512)?;
        validate_string_length("logStreamName", &stream_name, 1, 512)?;

        let mut state = self.state.write();
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
                upload_sequence_token: generate_sequence_token(),
                events: Vec::new(),
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn delete_log_stream(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

        validate_string_length("logGroupName", group_name, 1, 512)?;
        validate_string_length("logStreamName", stream_name, 1, 512)?;

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

    pub(crate) fn describe_log_streams(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);

        // Support both logGroupName and logGroupIdentifier
        let group_name = if let Some(name) = body["logGroupName"].as_str() {
            name.to_string()
        } else if let Some(identifier) = body["logGroupIdentifier"].as_str() {
            // Validate: must not end with :*
            if identifier.ends_with(":*") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!(
                        "1 validation error detected: Value '{}' at 'logGroupIdentifier' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w#+=/:,.@-]*",
                        identifier
                    ),
                ));
            }
            // If it's an ARN, extract the log group name
            if identifier.starts_with("arn:aws:logs:") {
                extract_log_group_from_arn(identifier).unwrap_or_else(|| identifier.to_string())
            } else {
                identifier.to_string()
            }
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            ));
        };

        let prefix = body["logStreamNamePrefix"].as_str().unwrap_or("");
        let limit = body["limit"].as_i64().unwrap_or(50) as usize;
        let order_by = body["orderBy"].as_str().unwrap_or("LogStreamName");
        let next_token = body["nextToken"].as_str();

        validate_optional_string_length("logGroupName", body["logGroupName"].as_str(), 1, 512)?;
        validate_optional_string_length(
            "logGroupIdentifier",
            body["logGroupIdentifier"].as_str(),
            1,
            2048,
        )?;
        validate_optional_string_length(
            "logStreamNamePrefix",
            body["logStreamNamePrefix"].as_str(),
            1,
            512,
        )?;

        // Validate limit
        if limit > 50 {
            return Err(validation_error(
                "limit",
                &limit.to_string(),
                "Member must have value less than or equal to 50",
            ));
        }

        // Validate orderBy
        if order_by != "LogStreamName" && order_by != "LastEventTime" {
            return Err(validation_error(
                "orderBy",
                order_by,
                "Member must satisfy enum value set: [LogStreamName, LastEventTime]",
            ));
        }

        // Cannot use prefix with LastEventTime ordering
        if order_by == "LastEventTime" && !prefix.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "Cannot order by LastEventTime with a logStreamNamePrefix.",
            ));
        }

        let state = self.state.read();
        let group = state.log_groups.get(group_name.as_str()).ok_or_else(|| {
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

        // Handle pagination with token format: logGroupName@lastStreamName
        let start_idx = if let Some(token) = next_token {
            if let Some((_group, last_stream)) = token.split_once('@') {
                streams
                    .iter()
                    .position(|s| s.name.as_str() > last_stream)
                    .unwrap_or(streams.len())
            } else {
                streams.len() // invalid token -> empty results
            }
        } else {
            0
        };

        let page = &streams[start_idx..];
        let has_more = page.len() > limit;
        let page = if has_more { &page[..limit] } else { page };

        let log_streams: Vec<Value> = page
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

        let mut result = json!({ "logStreams": log_streams });
        if has_more {
            if let Some(last) = page.last() {
                result["nextToken"] = json!(format!("{}@{}", group_name, last.name));
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    // ---- Log Events ----

    pub(crate) fn put_log_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        validate_string_length("logGroupName", group_name, 1, 512)?;
        validate_string_length("logStreamName", stream_name, 1, 512)?;

        let log_events = body["logEvents"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logEvents is required",
            )
        })?;

        let now = Utc::now().timestamp_millis();

        // Check chronological order
        let timestamps: Vec<i64> = log_events
            .iter()
            .map(|e| e["timestamp"].as_i64().unwrap_or(now))
            .collect();
        for i in 1..timestamps.len() {
            if timestamps[i] < timestamps[i - 1] {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Log events in a single PutLogEvents request must be in chronological order.",
                ));
            }
        }

        // Check for too old (14 days) and too new (2 hours) events
        let fourteen_days_ms = 14 * 24 * 60 * 60 * 1000i64;
        let two_hours_ms = 2 * 60 * 60 * 1000i64;
        let mut too_old_end_index: Option<usize> = None;
        let mut too_new_start_index: Option<usize> = None;

        for (i, ts) in timestamps.iter().enumerate() {
            if now.saturating_sub(*ts) > fourteen_days_ms {
                too_old_end_index = Some(i);
            } else if ts.saturating_sub(now) > two_hours_ms && too_new_start_index.is_none() {
                too_new_start_index = Some(i);
            }
        }

        // Build events list (only accepted ones)
        let mut new_events: Vec<LogEvent> = Vec::new();
        let mut rejected_info = json!({});
        let mut has_rejected = false;

        for (i, e) in log_events.iter().enumerate() {
            let ts = e["timestamp"].as_i64().unwrap_or(now);
            let is_too_old = too_old_end_index.is_some() && i <= too_old_end_index.unwrap();
            let is_too_new = too_new_start_index.is_some() && i >= too_new_start_index.unwrap();

            if is_too_old || is_too_new {
                continue;
            }

            new_events.push(LogEvent {
                timestamp: ts,
                message: e["message"].as_str().unwrap_or("").to_string(),
                ingestion_time: now,
            });
        }

        if let Some(idx) = too_old_end_index {
            rejected_info["tooOldLogEventEndIndex"] = json!(idx);
            has_rejected = true;
        }
        if let Some(idx) = too_new_start_index {
            rejected_info["tooNewLogEventStartIndex"] = json!(idx);
            has_rejected = true;
        }

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        // Apply transformer if configured on the log group
        if let Some(ref tx) = group.transformer {
            for event in &mut new_events {
                let transformed =
                    transformer::apply_transformer(&tx.transformer_config, &event.message);
                event.message = serde_json::to_string(&transformed).unwrap();
            }
        }

        let stream = group.log_streams.get_mut(stream_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log stream does not exist.",
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
            group.stored_bytes += event.message.len() as i64 + 26;
        }
        stream.last_ingestion_time = Some(now);

        // Generate new sequence token
        stream.upload_sequence_token = generate_sequence_token();

        let accepted_events: Vec<LogEvent> = new_events.clone();
        stream.events.append(&mut new_events);
        stream.events.sort_by_key(|e| e.timestamp);

        let sequence_token = stream.upload_sequence_token.clone();

        // Collect subscription filter info for delivery (while we hold the lock)
        let filters_to_deliver: Vec<(String, String, String)> = group
            .subscription_filters
            .iter()
            .map(|f| {
                (
                    f.filter_name.clone(),
                    f.filter_pattern.clone(),
                    f.destination_arn.clone(),
                )
            })
            .collect();
        let group_name_owned = group_name.to_string();
        let stream_name_owned = stream_name.to_string();

        // Collect delivery pipeline info: find active deliveries whose source
        // resource ARN matches this log group's ARN.
        let group_arn = group.arn.clone();
        let delivery_targets: Vec<String> = state
            .deliveries
            .values()
            .filter_map(|d| {
                // Check if the delivery source references this log group
                if let Some(source) = state.delivery_sources.get(&d.delivery_source_name) {
                    if source.resource_arns.contains(&group_arn) {
                        // Find the destination's S3 bucket configuration
                        if let Some(dest) = state
                            .delivery_destinations
                            .values()
                            .find(|dd| dd.arn == d.delivery_destination_arn)
                        {
                            if let Some(dest_arn) = dest
                                .delivery_destination_configuration
                                .get("destinationResourceArn")
                            {
                                if dest_arn.contains(":s3:") || dest_arn.starts_with("arn:aws:s3") {
                                    return Some(dest_arn.clone());
                                }
                            }
                        }
                    }
                }
                None
            })
            .collect();

        // Write delivery pipeline events to internal export storage
        if !delivery_targets.is_empty() && !accepted_events.is_empty() {
            let lines: Vec<String> = accepted_events.iter().map(|e| e.message.clone()).collect();
            let data = lines.join("\n");
            let now_str = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
            for dest_arn in &delivery_targets {
                // Extract bucket name from S3 ARN: arn:aws:s3:::bucket-name
                let bucket = dest_arn.strip_prefix("arn:aws:s3:::").unwrap_or(dest_arn);
                let key = format!(
                    "{}/delivery/{}/{}/{}",
                    bucket, group_name_owned, stream_name_owned, now_str
                );
                // Append to existing data if present
                let entry = state.export_storage.entry(key).or_default();
                if !entry.is_empty() {
                    entry.push(b'\n');
                }
                entry.extend_from_slice(data.as_bytes());
            }
        }

        drop(state);

        // Deliver to subscription filter destinations
        if !filters_to_deliver.is_empty() && !accepted_events.is_empty() {
            for (filter_name, filter_pattern, destination_arn) in &filters_to_deliver {
                let matching_events: Vec<&LogEvent> = accepted_events
                    .iter()
                    .filter(|e| matches_filter_pattern(filter_pattern, &e.message))
                    .collect();

                if matching_events.is_empty() {
                    continue;
                }

                let log_events_json: Vec<Value> = matching_events
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        json!({
                            "id": format!("{:032}", i),
                            "timestamp": e.timestamp,
                            "message": e.message,
                        })
                    })
                    .collect();

                let payload = json!({
                    "messageType": "DATA_MESSAGE",
                    "owner": "123456789012",
                    "logGroup": group_name_owned,
                    "logStream": stream_name_owned,
                    "subscriptionFilters": [filter_name],
                    "logEvents": log_events_json,
                });

                let payload_str = serde_json::to_string(&payload).unwrap();
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(payload_str.as_bytes()).unwrap();
                let compressed = encoder.finish().unwrap();
                let encoded = base64::engine::general_purpose::STANDARD.encode(&compressed);

                self.delivery_bus
                    .send_to_sqs(destination_arn, &encoded, &HashMap::new());
            }
        }

        let mut response = json!({
            "nextSequenceToken": sequence_token,
        });
        if has_rejected {
            response["rejectedLogEventsInfo"] = rejected_info;
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&response).unwrap(),
        ))
    }

    pub(crate) fn get_log_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);

        // Support both logGroupName and logGroupIdentifier
        let group_name = if let Some(name) = body["logGroupName"].as_str() {
            name.to_string()
        } else if let Some(identifier) = body["logGroupIdentifier"].as_str() {
            if identifier.ends_with(":*") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!(
                        "1 validation error detected: Value '{}' at 'logGroupIdentifier' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w#+=/:,.@-]*",
                        identifier
                    ),
                ));
            }
            if identifier.starts_with("arn:aws:logs:") {
                extract_log_group_from_arn(identifier).unwrap_or_else(|| identifier.to_string())
            } else {
                identifier.to_string()
            }
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            ));
        };

        let stream_name = body["logStreamName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logStreamName is required",
            )
        })?;

        validate_optional_string_length("logGroupName", body["logGroupName"].as_str(), 1, 512)?;
        validate_optional_string_length(
            "logGroupIdentifier",
            body["logGroupIdentifier"].as_str(),
            1,
            2048,
        )?;
        validate_string_length("logStreamName", stream_name, 1, 512)?;

        let start_time = body["startTime"].as_i64();
        let end_time = body["endTime"].as_i64();
        let limit = body["limit"].as_i64().unwrap_or(10000) as usize;
        let start_from_head = body["startFromHead"].as_bool().unwrap_or(false);
        let next_token = body["nextToken"].as_str();

        // Validate limit
        if limit > 10000 {
            return Err(validation_error(
                "limit",
                &limit.to_string(),
                "Member must have value less than or equal to 10000",
            ));
        }

        // Validate nextToken format
        if let Some(token) = next_token {
            if !token.starts_with("f/") && !token.starts_with("b/") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "The specified nextToken is invalid.",
                ));
            }
            let num_part = &token[2..];
            if num_part.len() != 56 || num_part.parse::<u128>().is_err() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "The specified nextToken is invalid.",
                ));
            }
        }

        let state = self.state.read();
        let group = state.log_groups.get(group_name.as_str()).ok_or_else(|| {
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

        // All events are indexed 0..n
        let all_events: Vec<&LogEvent> = stream
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

        let total = all_events.len();

        // Determine start position from token
        let (start_idx, is_forward) = if let Some(token) = next_token {
            let is_forward = token.starts_with("f/");
            let idx: usize = token[2..].parse().unwrap_or(0);
            if is_forward {
                // Forward token: start from idx+1
                (idx + 1, true)
            } else {
                // Backward token: end at idx (exclusive), so start at max(0, idx-limit)
                (idx, false)
            }
        } else {
            (0, start_from_head)
        };

        let events_slice: Vec<&LogEvent>;
        let next_forward_idx: usize;
        let next_backward_idx: usize;

        if is_forward || start_from_head && next_token.is_none() {
            // Forward: from start_idx, take limit
            let end_idx = std::cmp::min(start_idx + limit, total);
            if start_idx >= total {
                events_slice = Vec::new();
                let last_idx = if total > 0 { total - 1 } else { 0 };
                next_forward_idx = last_idx;
                next_backward_idx = last_idx;
            } else {
                events_slice = all_events[start_idx..end_idx].to_vec();
                next_forward_idx = end_idx - 1;
                next_backward_idx = start_idx;
            }
        } else {
            // Backward (default): from end, take last `limit` events
            if next_token.is_some() {
                // Backward token: start_idx is the position, go backward `limit` from here
                let begin = start_idx.saturating_sub(limit);
                let end_idx = start_idx;
                if begin >= total || end_idx > total || begin >= end_idx {
                    events_slice = Vec::new();
                    next_forward_idx = start_idx;
                    next_backward_idx = start_idx;
                } else {
                    events_slice = all_events[begin..end_idx].to_vec();
                    next_forward_idx = end_idx - 1;
                    next_backward_idx = begin;
                }
            } else {
                // No token, not start_from_head: return last `limit` events
                let begin = total.saturating_sub(limit);
                events_slice = all_events[begin..].to_vec();
                next_forward_idx = if total > 0 { total - 1 } else { 0 };
                next_backward_idx = begin;
            }
        }

        let events_json: Vec<Value> = events_slice
            .iter()
            .map(|e| {
                json!({
                    "timestamp": e.timestamp,
                    "message": e.message,
                    "ingestionTime": e.ingestion_time,
                })
            })
            .collect();

        let forward_token = format!("f/{:056}", next_forward_idx);
        let backward_token = format!("b/{:056}", next_backward_idx);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "events": events_json,
                "nextForwardToken": forward_token,
                "nextBackwardToken": backward_token,
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn filter_log_events(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_identifier = body["logGroupIdentifier"].as_str();
        let log_group_name = body["logGroupName"].as_str();
        let filter_pattern = body["filterPattern"].as_str().unwrap_or("");
        let start_time = body["startTime"].as_i64();
        let end_time = body["endTime"].as_i64();
        let limit = body["limit"].as_i64().unwrap_or(10000) as usize;
        let next_token = body["nextToken"].as_str();
        let stream_names: Vec<&str> = body["logStreamNames"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let stream_name_prefix = body["logStreamNamePrefix"].as_str().unwrap_or("");

        if let Some(name) = log_group_name {
            validate_string_length("logGroupName", name, 1, 512)?;
        }
        validate_optional_string_length("logGroupIdentifier", log_group_identifier, 1, 2048)?;
        validate_optional_string_length(
            "logStreamNamePrefix",
            body["logStreamNamePrefix"].as_str(),
            1,
            512,
        )?;
        validate_optional_string_length("filterPattern", Some(filter_pattern), 0, 1024)?;

        // Resolve the effective log group name: logGroupIdentifier takes precedence,
        // and can be either a name or an ARN.
        let resolved_group_name = if let Some(identifier) = log_group_identifier {
            if identifier.starts_with("arn:") {
                extract_log_group_from_arn(identifier).ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterException",
                        format!("Invalid ARN: {identifier}"),
                    )
                })?
            } else {
                identifier.to_string()
            }
        } else if let Some(name) = log_group_name {
            name.to_string()
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "Either logGroupName or logGroupIdentifier is required",
            ));
        };

        // Validate limit
        if limit > 10000 {
            return Err(validation_error(
                "limit",
                &limit.to_string(),
                "Member must have value less than or equal to 10000",
            ));
        }

        let state = self.state.read();
        let group = state
            .log_groups
            .get(resolved_group_name.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("The specified log group does not exist: {resolved_group_name}"),
                )
            })?;

        let mut filtered_events: Vec<Value> = Vec::new();

        let streams: Vec<(&String, &LogStream)> = if !stream_names.is_empty() {
            group
                .log_streams
                .iter()
                .filter(|(name, _)| stream_names.contains(&name.as_str()))
                .collect()
        } else if !stream_name_prefix.is_empty() {
            group
                .log_streams
                .iter()
                .filter(|(name, _)| name.starts_with(stream_name_prefix))
                .collect()
        } else {
            group.log_streams.iter().collect()
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
                // Filter pattern matching
                if !filter_pattern.is_empty()
                    && !matches_filter_pattern(filter_pattern, &event.message)
                {
                    continue;
                }

                let event_id = format!("{}-{}", stream.name, event.timestamp);

                filtered_events.push(json!({
                    "logStreamName": stream.name,
                    "timestamp": event.timestamp,
                    "message": event.message,
                    "ingestionTime": event.ingestion_time,
                    "eventId": event_id,
                }));
            }
        }

        filtered_events.sort_by_key(|e| e["timestamp"].as_i64().unwrap_or(0));

        // Handle pagination
        // Token format: groupName@streamName@eventId
        let start_idx = if let Some(token) = next_token {
            let parts: Vec<&str> = token.splitn(3, '@').collect();
            if parts.len() == 3 {
                let after_event_id = parts[2];
                // Find the position after this eventId
                filtered_events
                    .iter()
                    .position(|e| e["eventId"].as_str().unwrap_or("") == after_event_id)
                    .map(|pos| pos + 1)
                    .unwrap_or(filtered_events.len())
            } else {
                filtered_events.len() // invalid token -> empty results
            }
        } else {
            0
        };

        let remaining = &filtered_events[start_idx..];
        let has_more = remaining.len() > limit;
        let page: Vec<Value> = if has_more {
            remaining[..limit].to_vec()
        } else {
            remaining.to_vec()
        };

        let mut result = json!({
            "events": page,
            "searchedLogStreams": [],
        });

        if has_more {
            if let Some(last) = page.last() {
                let event_id = last["eventId"].as_str().unwrap_or("");
                result["nextToken"] = json!(format!(
                    "{}@{}@{}",
                    resolved_group_name,
                    last["logStreamName"].as_str().unwrap_or(""),
                    event_id
                ));
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    pub(crate) fn get_log_record(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let _log_record_pointer = body["logRecordPointer"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logRecordPointer is required",
            )
        })?;

        // Stub: return empty log record
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logRecord": {} })).unwrap(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- filter_log_events: logGroupIdentifier ----

    #[test]
    fn filter_log_events_uses_log_group_identifier_as_name() {
        let svc = make_service();
        create_group(&svc, "my-group");
        create_stream(&svc, "my-group", "stream-1");
        put_events(&svc, "my-group", "stream-1", &["hello"]);

        let req = make_request(
            "FilterLogEvents",
            json!({ "logGroupIdentifier": "my-group" }),
        );
        let resp = svc.filter_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn filter_log_events_uses_log_group_identifier_as_arn() {
        let svc = make_service();
        create_group(&svc, "my-group");
        create_stream(&svc, "my-group", "stream-1");
        put_events(&svc, "my-group", "stream-1", &["hello"]);

        let req = make_request(
            "FilterLogEvents",
            json!({ "logGroupIdentifier": "arn:aws:logs:us-east-1:123456789012:log-group:my-group:*" }),
        );
        let resp = svc.filter_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn filter_log_events_errors_without_group_name_or_identifier() {
        let svc = make_service();
        let req = make_request("FilterLogEvents", json!({}));
        assert!(svc.filter_log_events(&req).is_err());
    }

    // ---- filter_log_events: logStreamNamePrefix ----

    #[test]
    fn filter_log_events_filters_by_stream_name_prefix() {
        let svc = make_service();
        create_group(&svc, "grp");
        create_stream(&svc, "grp", "web-1");
        create_stream(&svc, "grp", "web-2");
        create_stream(&svc, "grp", "api-1");
        put_events(&svc, "grp", "web-1", &["a"]);
        put_events(&svc, "grp", "web-2", &["b"]);
        put_events(&svc, "grp", "api-1", &["c"]);

        let req = make_request(
            "FilterLogEvents",
            json!({ "logGroupName": "grp", "logStreamNamePrefix": "web" }),
        );
        let resp = svc.filter_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let events = body["events"].as_array().unwrap();
        assert_eq!(events.len(), 2);
        for e in events {
            assert!(e["logStreamName"].as_str().unwrap().starts_with("web"));
        }
    }

    // ---- FilterLogEvents pattern matching tests ----

    #[test]
    fn filter_pattern_empty_matches_everything() {
        assert!(matches_filter_pattern("", "any message"));
        assert!(matches_filter_pattern("  ", "any message"));
    }

    #[test]
    fn filter_pattern_simple_text_matches() {
        assert!(matches_filter_pattern("ERROR", "This is an ERROR message"));
        assert!(!matches_filter_pattern("ERROR", "This is a warning"));
    }

    #[test]
    fn filter_pattern_multiple_terms_and() {
        assert!(matches_filter_pattern(
            "ERROR Exception",
            "ERROR: NullPointerException occurred"
        ));
        assert!(!matches_filter_pattern(
            "ERROR Exception",
            "ERROR: something broke"
        ));
        assert!(!matches_filter_pattern(
            "ERROR Exception",
            "Exception in thread"
        ));
    }

    #[test]
    fn filter_pattern_quoted_exact_phrase() {
        assert!(matches_filter_pattern(
            "\"error occurred\"",
            "An error occurred in module X"
        ));
        assert!(!matches_filter_pattern(
            "\"error occurred\"",
            "An error has occurred in module X"
        ));
    }

    #[test]
    fn filter_pattern_json_field_equals_string() {
        assert!(matches_filter_pattern(
            "{ $.level = \"ERROR\" }",
            r#"{"level":"ERROR","message":"boom"}"#
        ));
        assert!(!matches_filter_pattern(
            "{ $.level = \"ERROR\" }",
            r#"{"level":"INFO","message":"ok"}"#
        ));
    }

    #[test]
    fn filter_pattern_json_field_not_equals() {
        assert!(matches_filter_pattern(
            "{ $.level != \"INFO\" }",
            r#"{"level":"ERROR","message":"boom"}"#
        ));
        assert!(!matches_filter_pattern(
            "{ $.level != \"INFO\" }",
            r#"{"level":"INFO","message":"ok"}"#
        ));
    }

    #[test]
    fn filter_pattern_json_numeric_comparison() {
        assert!(matches_filter_pattern(
            "{ $.status = 500 }",
            r#"{"status":500,"msg":"error"}"#
        ));
        assert!(!matches_filter_pattern(
            "{ $.status = 500 }",
            r#"{"status":200,"msg":"ok"}"#
        ));
        assert!(matches_filter_pattern(
            "{ $.latency > 100 }",
            r#"{"latency":250}"#
        ));
        assert!(!matches_filter_pattern(
            "{ $.latency > 100 }",
            r#"{"latency":50}"#
        ));
    }

    #[test]
    fn filter_pattern_json_nested_field() {
        assert!(matches_filter_pattern(
            "{ $.request.method = \"POST\" }",
            r#"{"request":{"method":"POST","path":"/api"}}"#
        ));
        assert!(!matches_filter_pattern(
            "{ $.request.method = \"POST\" }",
            r#"{"request":{"method":"GET","path":"/api"}}"#
        ));
    }

    #[test]
    fn filter_pattern_json_non_json_message_no_match() {
        assert!(!matches_filter_pattern(
            "{ $.level = \"ERROR\" }",
            "This is a plain text message"
        ));
    }

    #[test]
    fn filter_log_events_applies_pattern() {
        let svc = make_service();

        // Create log group and stream
        let req = make_request(
            "CreateLogGroup",
            json!({ "logGroupName": "/filter-pattern/test" }),
        );
        svc.create_log_group(&req).unwrap();

        let req = make_request(
            "CreateLogStream",
            json!({
                "logGroupName": "/filter-pattern/test",
                "logStreamName": "stream-1"
            }),
        );
        svc.create_log_stream(&req).unwrap();

        // Put events with mixed content
        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/filter-pattern/test",
                "logStreamName": "stream-1",
                "logEvents": [
                    { "timestamp": now, "message": "ERROR: disk full" },
                    { "timestamp": now + 1000, "message": "INFO: request complete" },
                    { "timestamp": now + 2000, "message": "ERROR: connection timeout" },
                    { "timestamp": now + 3000, "message": "WARN: high latency" }
                ]
            }),
        );
        svc.put_log_events(&req).unwrap();

        // Filter for ERROR
        let req = make_request(
            "FilterLogEvents",
            json!({
                "logGroupName": "/filter-pattern/test",
                "filterPattern": "ERROR"
            }),
        );
        let resp = svc.filter_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let events = body["events"].as_array().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0]["message"].as_str().unwrap().contains("ERROR"));
        assert!(events[1]["message"].as_str().unwrap().contains("ERROR"));

        // Filter for multiple terms (AND)
        let req = make_request(
            "FilterLogEvents",
            json!({
                "logGroupName": "/filter-pattern/test",
                "filterPattern": "ERROR timeout"
            }),
        );
        let resp = svc.filter_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let events = body["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0]["message"].as_str().unwrap().contains("timeout"));

        // Filter for quoted phrase
        let req = make_request(
            "FilterLogEvents",
            json!({
                "logGroupName": "/filter-pattern/test",
                "filterPattern": "\"request complete\""
            }),
        );
        let resp = svc.filter_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let events = body["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0]["message"]
            .as_str()
            .unwrap()
            .contains("request complete"));
    }

    #[test]
    fn filter_log_events_json_pattern() {
        let svc = make_service();

        let req = make_request(
            "CreateLogGroup",
            json!({ "logGroupName": "/json-filter/test" }),
        );
        svc.create_log_group(&req).unwrap();

        let req = make_request(
            "CreateLogStream",
            json!({
                "logGroupName": "/json-filter/test",
                "logStreamName": "s1"
            }),
        );
        svc.create_log_stream(&req).unwrap();

        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/json-filter/test",
                "logStreamName": "s1",
                "logEvents": [
                    { "timestamp": now, "message": r#"{"level":"ERROR","msg":"fail"}"# },
                    { "timestamp": now + 1000, "message": r#"{"level":"INFO","msg":"ok"}"# },
                    { "timestamp": now + 2000, "message": r#"{"level":"ERROR","msg":"crash"}"# },
                    { "timestamp": now + 3000, "message": "not json at all" }
                ]
            }),
        );
        svc.put_log_events(&req).unwrap();

        // Filter with JSON pattern
        let req = make_request(
            "FilterLogEvents",
            json!({
                "logGroupName": "/json-filter/test",
                "filterPattern": "{ $.level = \"ERROR\" }"
            }),
        );
        let resp = svc.filter_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let events = body["events"].as_array().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0]["message"].as_str().unwrap().contains("ERROR"));
        assert!(events[1]["message"].as_str().unwrap().contains("ERROR"));
    }

    // ---- Query language (StartQuery / GetQueryResults) tests ----

    #[test]
    fn logs_query_filters_events() {
        let svc = make_service();
        create_group(&svc, "/query/test");
        create_stream(&svc, "/query/test", "stream-1");

        let now = chrono::Utc::now().timestamp_millis();
        let events: Vec<Value> = vec![
            json!({ "timestamp": now, "message": "ERROR: something broke" }),
            json!({ "timestamp": now + 1, "message": "INFO: all good" }),
            json!({ "timestamp": now + 2, "message": "ERROR: another failure" }),
        ];
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/query/test",
                "logStreamName": "stream-1",
                "logEvents": events,
            }),
        );
        svc.put_log_events(&req).unwrap();

        // Start a query with filter
        let start_secs = (now / 1000) - 1;
        let end_secs = (now / 1000) + 10;
        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "/query/test",
                "startTime": start_secs,
                "endTime": end_secs,
                "queryString": "filter @message like /ERROR/ | limit 10",
            }),
        );
        let resp = svc.start_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let query_id = body["queryId"].as_str().unwrap();

        // Get results
        let req = make_request("GetQueryResults", json!({ "queryId": query_id }));
        let resp = svc.get_query_results(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let results = body["results"].as_array().unwrap();
        assert_eq!(results.len(), 2, "Should only return ERROR events");
        assert_eq!(body["status"].as_str().unwrap(), "Complete");
    }

    #[test]
    fn logs_query_fields_selection() {
        let svc = make_service();
        create_group(&svc, "/qfields/test");
        create_stream(&svc, "/qfields/test", "s1");

        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/qfields/test",
                "logStreamName": "s1",
                "logEvents": [{ "timestamp": now, "message": "hello" }],
            }),
        );
        svc.put_log_events(&req).unwrap();

        let start_secs = (now / 1000) - 1;
        let end_secs = (now / 1000) + 10;
        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "/qfields/test",
                "startTime": start_secs,
                "endTime": end_secs,
                "queryString": "fields @message",
            }),
        );
        let resp = svc.start_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let query_id = body["queryId"].as_str().unwrap();

        let req = make_request("GetQueryResults", json!({ "queryId": query_id }));
        let resp = svc.get_query_results(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let results = body["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);

        let row = results[0].as_array().unwrap();
        let field_names: Vec<&str> = row.iter().map(|f| f["field"].as_str().unwrap()).collect();
        assert!(field_names.contains(&"@message"));
        assert!(field_names.contains(&"@ptr"));
        assert!(!field_names.contains(&"@timestamp"));
    }

    #[test]
    fn logs_query_sort_and_limit() {
        let svc = make_service();
        create_group(&svc, "/qsort/test");
        create_stream(&svc, "/qsort/test", "s1");

        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/qsort/test",
                "logStreamName": "s1",
                "logEvents": [
                    { "timestamp": now, "message": "first" },
                    { "timestamp": now + 1000, "message": "second" },
                    { "timestamp": now + 2000, "message": "third" },
                ],
            }),
        );
        svc.put_log_events(&req).unwrap();

        let start_secs = (now / 1000) - 1;
        let end_secs = (now / 1000) + 10;
        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "/qsort/test",
                "startTime": start_secs,
                "endTime": end_secs,
                "queryString": "sort @timestamp desc | limit 2",
            }),
        );
        let resp = svc.start_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let query_id = body["queryId"].as_str().unwrap();

        let req = make_request("GetQueryResults", json!({ "queryId": query_id }));
        let resp = svc.get_query_results(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let results = body["results"].as_array().unwrap();
        assert_eq!(results.len(), 2, "Should be limited to 2");

        // First result should be the latest (desc sort)
        let first_msg = results[0]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["field"].as_str() == Some("@message"))
            .unwrap();
        assert_eq!(first_msg["value"].as_str().unwrap(), "third");
    }

    #[test]
    fn logs_query_json_field_filter() {
        let svc = make_service();
        create_group(&svc, "/qjson/test");
        create_stream(&svc, "/qjson/test", "s1");

        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/qjson/test",
                "logStreamName": "s1",
                "logEvents": [
                    { "timestamp": now, "message": r#"{"level":"ERROR","msg":"fail"}"# },
                    { "timestamp": now + 1, "message": r#"{"level":"INFO","msg":"ok"}"# },
                    { "timestamp": now + 2, "message": r#"{"level":"ERROR","msg":"crash"}"# },
                ],
            }),
        );
        svc.put_log_events(&req).unwrap();

        let start_secs = (now / 1000) - 1;
        let end_secs = (now / 1000) + 10;
        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "/qjson/test",
                "startTime": start_secs,
                "endTime": end_secs,
                "queryString": r#"filter level = "ERROR""#,
            }),
        );
        let resp = svc.start_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let query_id = body["queryId"].as_str().unwrap();

        let req = make_request("GetQueryResults", json!({ "queryId": query_id }));
        let resp = svc.get_query_results(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let results = body["results"].as_array().unwrap();
        assert_eq!(results.len(), 2, "Should only match ERROR JSON events");
    }
}
