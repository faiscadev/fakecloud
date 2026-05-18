use http::StatusCode;
use serde_json::{json, Value};

use crate::validation::*;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{validation_error, LogsService};
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
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();

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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        // Snapshot metric filters bound to this log group; eval happens
        // after we drop the lock so PutMetricData can re-lock cloudwatch
        // state without blocking other PutLogEvents calls.
        let metric_filters_for_group: Vec<crate::state::MetricFilter> = state
            .metric_filters
            .iter()
            .filter(|f| f.log_group_name == group_name_owned)
            .cloned()
            .collect();
        let region_owned = state.region.clone();
        let account_id_for_metrics = state.account_id.clone();

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

        // Write delivery pipeline events to the destination S3 bucket
        // via the delivery bus so subscribers can read them with real
        // S3 GetObject. Falls back to the internal export_storage map
        // when no S3 writer is wired (in-process tests). Output is
        // gzip-compressed JSONL (`.gz`), matching real CloudWatch Logs
        // delivery to S3.
        if !delivery_targets.is_empty() && !accepted_events.is_empty() {
            let mut data = String::new();
            for event in &accepted_events {
                let line = serde_json::to_string(&json!({
                    "timestamp": event.timestamp,
                    "message": event.message,
                }))
                .unwrap();
                data.push_str(&line);
                data.push('\n');
            }
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(data.as_bytes()).expect("gzip write");
            let gz_body = encoder.finish().expect("gzip finish");
            let now_dt = Utc::now();
            let date_part = now_dt.format("%Y/%m/%d").to_string();
            let now_str = now_dt.format("%Y%m%dT%H%M%SZ").to_string();
            let account_id_owned = state.account_id.clone();
            for dest_arn in &delivery_targets {
                let bucket = dest_arn.strip_prefix("arn:aws:s3:::").unwrap_or(dest_arn);
                // Mirror the CloudWatch Logs delivery key shape:
                // `aws-logs-write/<accountId>/<group>/<date>/<stream>-<ts>.gz`.
                let s3_key = format!(
                    "aws-logs-write/{}/{}/{}/{}-{}.gz",
                    account_id_owned, group_name_owned, date_part, stream_name_owned, now_str,
                );
                if self
                    .delivery_bus
                    .put_object_to_s3(
                        &account_id_owned,
                        bucket,
                        &s3_key,
                        gz_body.clone(),
                        Some("application/x-gzip"),
                    )
                    .is_err()
                {
                    let fallback_key = format!("{bucket}/{s3_key}");
                    state.export_storage.insert(fallback_key, gz_body.clone());
                }
            }
        }

        drop(accounts);

        if !filters_to_deliver.is_empty() && !accepted_events.is_empty() {
            for (filter_name, filter_pattern, destination_arn) in &filters_to_deliver {
                self.deliver_to_subscription_filter(
                    filter_name,
                    filter_pattern,
                    destination_arn,
                    &group_name_owned,
                    &stream_name_owned,
                    &accepted_events,
                );
            }
        }

        // Metric filter eval: per filter, per matching event, publish a
        // CloudWatch metric data point. Real CloudWatch does this on
        // ingestion; downstream alarms read those points.
        if !metric_filters_for_group.is_empty() && !accepted_events.is_empty() {
            self.publish_metric_filter_data(
                &account_id_for_metrics,
                &region_owned,
                &metric_filters_for_group,
                &accepted_events,
            );
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

    /// Apply a subscription filter's pattern to a batch of accepted log
    /// events and deliver the matches to the filter's destination ARN.
    /// CloudWatch wraps the matches in the documented DATA_MESSAGE
    /// envelope, gzip-compresses it, and base64-encodes the result;
    /// every destination flavour (SQS, Lambda, Kinesis) consumes that
    /// same payload but expects a different wire shape.
    fn deliver_to_subscription_filter(
        &self,
        filter_name: &str,
        filter_pattern: &str,
        destination_arn: &str,
        group_name: &str,
        stream_name: &str,
        accepted_events: &[LogEvent],
    ) {
        let matching_events: Vec<&LogEvent> = accepted_events
            .iter()
            .filter(|e| matches_filter_pattern(filter_pattern, &e.message))
            .collect();
        if matching_events.is_empty() {
            return;
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
            "logGroup": group_name,
            "logStream": stream_name,
            "subscriptionFilters": [filter_name],
            "logEvents": log_events_json,
        });

        let payload_str = serde_json::to_string(&payload).unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload_str.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let encoded = base64::engine::general_purpose::STANDARD.encode(&compressed);

        if destination_arn.contains(":sqs:") {
            self.delivery_bus
                .send_to_sqs(destination_arn, &encoded, &HashMap::new());
        } else if destination_arn.contains(":lambda:") {
            let lambda_event = json!({
                "awslogs": {
                    "data": encoded,
                }
            });
            let lambda_payload = serde_json::to_string(&lambda_event).unwrap();
            tokio::spawn({
                let bus = self.delivery_bus.clone();
                let arn = destination_arn.to_string();
                async move {
                    if let Some(result) = bus.invoke_lambda(&arn, &lambda_payload).await {
                        match result {
                            Ok(_) => {
                                tracing::debug!(
                                    function_arn = %arn,
                                    "CloudWatch Logs -> Lambda subscription delivered"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    function_arn = %arn,
                                    error = %e,
                                    "CloudWatch Logs -> Lambda subscription failed"
                                );
                            }
                        }
                    }
                }
            });
        } else if destination_arn.contains(":kinesis:") {
            let partition_key = format!("{}-{}", group_name, stream_name);
            self.delivery_bus
                .send_to_kinesis(destination_arn, &encoded, &partition_key);
        } else if destination_arn.contains(":firehose:") {
            // Firehose subscriptions get the same gzip+base64 envelope
            // Lambda/Kinesis subscribers see, so a downstream consumer
            // can decode with the standard CloudWatch-Logs format.
            self.delivery_bus
                .put_record_to_firehose(destination_arn, encoded.as_bytes());
        }
    }

    /// Run each metric filter against the accepted events and publish
    /// CloudWatch metric data points for matches. Mirrors real
    /// CloudWatch Logs: one datum per matching event, value resolved
    /// from the transformation's `MetricValue` (literal or `$.path`).
    fn publish_metric_filter_data(
        &self,
        account_id: &str,
        region: &str,
        metric_filters: &[crate::state::MetricFilter],
        accepted_events: &[LogEvent],
    ) {
        for filter in metric_filters {
            for event in accepted_events {
                if !crate::filter_pattern::matches(&filter.filter_pattern, &event.message) {
                    continue;
                }
                for tx in &filter.metric_transformations {
                    let value = crate::filter_pattern::resolve_metric_value(
                        &tx.metric_value,
                        tx.default_value,
                        &event.message,
                    );
                    self.delivery_bus.put_cloudwatch_metric(
                        account_id,
                        region,
                        &tx.metric_namespace,
                        &tx.metric_name,
                        value,
                        None,
                        std::collections::BTreeMap::new(),
                        event.timestamp,
                    );
                }
            }
        }
    }

    pub(crate) fn get_log_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let retention_cutoff = group
            .retention_in_days
            .map(|d| Utc::now().timestamp_millis() - (d as i64) * 86_400_000);

        // All events are indexed 0..n
        let all_events: Vec<&LogEvent> = stream
            .events
            .iter()
            .filter(|e| {
                if let Some(cutoff) = retention_cutoff {
                    if e.timestamp < cutoff {
                        return false;
                    }
                }
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
        let body = req.json_body();
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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let retention_cutoff = group
            .retention_in_days
            .map(|d| Utc::now().timestamp_millis() - (d as i64) * 86_400_000);

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
                if let Some(cutoff) = retention_cutoff {
                    if event.timestamp < cutoff {
                        continue;
                    }
                }
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
        let body = req.json_body();
        let pointer = body["logRecordPointer"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logRecordPointer is required",
            )
        })?;

        // fakecloud-issued pointers are base64 of `<group>|<stream>|<index>`,
        // matching the GetLogObject pointer encoding. Real CWL pointers are
        // opaque tokens minted by Insights queries — callers cannot
        // legitimately construct them without first observing them, so we
        // only need to round-trip what other operations produced.
        let (group_name, stream_name, idx) =
            parse_log_record_pointer(pointer).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logRecordPointer is not a fakecloud-issued pointer",
                )
            })?;
        let accounts = self.state.read();
        let state = accounts.get(&req.account_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "log record not found",
            )
        })?;
        let group = state.log_groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("log group '{group_name}' not found"),
            )
        })?;
        let stream = group.log_streams.get(&stream_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("log stream '{stream_name}' not found"),
            )
        })?;
        let ev = stream.events.get(idx).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "log event index out of range",
            )
        })?;
        let mut record = serde_json::Map::new();
        record.insert("@timestamp".into(), Value::String(ev.timestamp.to_string()));
        record.insert(
            "@ingestionTime".into(),
            Value::String(ev.ingestion_time.to_string()),
        );
        record.insert("@logStream".into(), Value::String(stream_name));
        record.insert("@logGroup".into(), Value::String(group_name));
        record.insert("@message".into(), Value::String(ev.message.clone()));
        // If the event message is JSON, expose its top-level keys alongside
        // the synthetic ones — matches what CWL Insights does for
        // discovered fields.
        if let Ok(parsed) = serde_json::from_str::<serde_json::Map<String, Value>>(&ev.message) {
            for (k, v) in parsed {
                if !record.contains_key(&k) {
                    let stringified = match &v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    record.insert(k, Value::String(stringified));
                }
            }
        }
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logRecord": record })).unwrap(),
        ))
    }
}

/// Pointer format mirrors GetLogObject: base64(`<group>|<stream>|<index>`).
fn parse_log_record_pointer(pointer: &str) -> Option<(String, String, usize)> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(pointer)
        .ok()?;
    let raw = String::from_utf8(bytes).ok()?;
    let parts: Vec<&str> = raw.splitn(3, '|').collect();
    if parts.len() != 3 {
        return None;
    }
    let idx = parts[2].parse::<usize>().ok()?;
    Some((parts[0].to_string(), parts[1].to_string(), idx))
}

#[cfg(test)]
pub(crate) fn encode_log_record_pointer(group: &str, stream: &str, idx: usize) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(format!("{group}|{stream}|{idx}").as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- get_log_record ----

    #[test]
    fn get_log_record_rejects_garbage_pointer() {
        let svc = make_service();
        let req = make_request("GetLogRecord", json!({ "logRecordPointer": "not-base64" }));
        match svc.get_log_record(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("expected InvalidParameterException"),
        }
    }

    #[test]
    fn get_log_record_returns_event_for_valid_pointer() {
        let svc = make_service();
        create_group(&svc, "/test/getrec");
        create_stream(&svc, "/test/getrec", "s1");
        put_events(
            &svc,
            "/test/getrec",
            "s1",
            &["{\"level\":\"info\",\"msg\":\"hi\"}"],
        );

        let pointer = super::encode_log_record_pointer("/test/getrec", "s1", 0);
        let req = make_request("GetLogRecord", json!({ "logRecordPointer": pointer }));
        let resp = svc.get_log_record(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["logRecord"]["@logStream"], "s1");
        assert_eq!(body["logRecord"]["@logGroup"], "/test/getrec");
        assert!(body["logRecord"]["@message"]
            .as_str()
            .unwrap()
            .contains("hi"));
        // JSON keys discovered from the message body.
        assert_eq!(body["logRecord"]["level"], "info");
        assert_eq!(body["logRecord"]["msg"], "hi");
    }

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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let query_id = body["queryId"].as_str().unwrap();

        // Get results
        let req = make_request("GetQueryResults", json!({ "queryId": query_id }));
        let resp = svc.get_query_results(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let query_id = body["queryId"].as_str().unwrap();

        let req = make_request("GetQueryResults", json!({ "queryId": query_id }));
        let resp = svc.get_query_results(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let query_id = body["queryId"].as_str().unwrap();

        let req = make_request("GetQueryResults", json!({ "queryId": query_id }));
        let resp = svc.get_query_results(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let query_id = body["queryId"].as_str().unwrap();

        let req = make_request("GetQueryResults", json!({ "queryId": query_id }));
        let resp = svc.get_query_results(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let results = body["results"].as_array().unwrap();
        assert_eq!(results.len(), 2, "Should only match ERROR JSON events");
    }

    // ── create_log_stream validation ──

    #[test]
    fn create_log_stream_missing_group_errors() {
        let svc = make_service();
        let req = make_request("CreateLogStream", json!({"logStreamName": "s"}));
        assert!(svc.create_log_stream(&req).is_err());
    }

    #[test]
    fn create_log_stream_missing_stream_errors() {
        let svc = make_service();
        create_group(&svc, "g1");
        let req = make_request("CreateLogStream", json!({"logGroupName": "g1"}));
        assert!(svc.create_log_stream(&req).is_err());
    }

    #[test]
    fn create_log_stream_nonexistent_group_errors() {
        let svc = make_service();
        let req = make_request(
            "CreateLogStream",
            json!({"logGroupName": "missing", "logStreamName": "s"}),
        );
        assert!(svc.create_log_stream(&req).is_err());
    }

    #[test]
    fn create_log_stream_duplicate_errors() {
        let svc = make_service();
        create_group(&svc, "dup");
        create_stream(&svc, "dup", "s");
        let req = make_request(
            "CreateLogStream",
            json!({"logGroupName": "dup", "logStreamName": "s"}),
        );
        assert!(svc.create_log_stream(&req).is_err());
    }

    // ── delete_log_stream validation ──

    #[test]
    fn delete_log_stream_missing_group_errors() {
        let svc = make_service();
        let req = make_request("DeleteLogStream", json!({"logStreamName": "s"}));
        assert!(svc.delete_log_stream(&req).is_err());
    }

    #[test]
    fn delete_log_stream_missing_stream_errors() {
        let svc = make_service();
        let req = make_request("DeleteLogStream", json!({"logGroupName": "g"}));
        assert!(svc.delete_log_stream(&req).is_err());
    }

    #[test]
    fn delete_log_stream_nonexistent_group() {
        let svc = make_service();
        let req = make_request(
            "DeleteLogStream",
            json!({"logGroupName": "missing", "logStreamName": "s"}),
        );
        assert!(svc.delete_log_stream(&req).is_err());
    }

    #[test]
    fn delete_log_stream_nonexistent_stream() {
        let svc = make_service();
        create_group(&svc, "g2");
        let req = make_request(
            "DeleteLogStream",
            json!({"logGroupName": "g2", "logStreamName": "missing"}),
        );
        assert!(svc.delete_log_stream(&req).is_err());
    }

    #[test]
    fn delete_log_stream_succeeds() {
        let svc = make_service();
        create_group(&svc, "gd");
        create_stream(&svc, "gd", "s");
        let req = make_request(
            "DeleteLogStream",
            json!({"logGroupName": "gd", "logStreamName": "s"}),
        );
        svc.delete_log_stream(&req).unwrap();
    }

    // ── describe_log_streams ──

    #[test]
    fn describe_log_streams_missing_group_errors() {
        let svc = make_service();
        let req = make_request("DescribeLogStreams", json!({}));
        assert!(svc.describe_log_streams(&req).is_err());
    }

    #[test]
    fn describe_log_streams_identifier_ending_in_colon_star_errors() {
        let svc = make_service();
        let req = make_request(
            "DescribeLogStreams",
            json!({"logGroupIdentifier": "arn:aws:logs:us-east-1:123456789012:log-group:x:*"}),
        );
        assert!(svc.describe_log_streams(&req).is_err());
    }

    #[test]
    fn describe_log_streams_identifier_as_arn() {
        let svc = make_service();
        create_group(&svc, "ident");
        create_stream(&svc, "ident", "s1");
        let req = make_request(
            "DescribeLogStreams",
            json!({"logGroupIdentifier": "arn:aws:logs:us-east-1:123456789012:log-group:ident"}),
        );
        let resp = svc.describe_log_streams(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["logStreams"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn describe_log_streams_limit_over_50_errors() {
        let svc = make_service();
        let req = make_request(
            "DescribeLogStreams",
            json!({"logGroupName": "g", "limit": 100}),
        );
        assert!(svc.describe_log_streams(&req).is_err());
    }

    #[test]
    fn describe_log_streams_invalid_order_by_errors() {
        let svc = make_service();
        let req = make_request(
            "DescribeLogStreams",
            json!({"logGroupName": "g", "orderBy": "Bogus"}),
        );
        assert!(svc.describe_log_streams(&req).is_err());
    }

    #[test]
    fn describe_log_streams_last_event_time_with_prefix_errors() {
        let svc = make_service();
        let req = make_request(
            "DescribeLogStreams",
            json!({
                "logGroupName": "g",
                "orderBy": "LastEventTime",
                "logStreamNamePrefix": "abc"
            }),
        );
        assert!(svc.describe_log_streams(&req).is_err());
    }

    #[test]
    fn describe_log_streams_missing_group_resource_error() {
        let svc = make_service();
        let req = make_request("DescribeLogStreams", json!({"logGroupName": "missing"}));
        assert!(svc.describe_log_streams(&req).is_err());
    }

    #[test]
    fn describe_log_streams_with_prefix_filter() {
        let svc = make_service();
        create_group(&svc, "pref");
        create_stream(&svc, "pref", "web-1");
        create_stream(&svc, "pref", "api-1");
        let req = make_request(
            "DescribeLogStreams",
            json!({"logGroupName": "pref", "logStreamNamePrefix": "web"}),
        );
        let resp = svc.describe_log_streams(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["logStreams"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn describe_log_streams_pagination() {
        let svc = make_service();
        create_group(&svc, "pg");
        for i in 0..5 {
            create_stream(&svc, "pg", &format!("s{i}"));
        }
        let req = make_request(
            "DescribeLogStreams",
            json!({"logGroupName": "pg", "limit": 2}),
        );
        let resp = svc.describe_log_streams(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["logStreams"].as_array().unwrap().len(), 2);
        assert!(body["nextToken"].is_string());

        let token = body["nextToken"].as_str().unwrap().to_string();
        let req = make_request(
            "DescribeLogStreams",
            json!({"logGroupName": "pg", "limit": 2, "nextToken": token}),
        );
        let resp = svc.describe_log_streams(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["logStreams"].as_array().unwrap().len(), 2);
    }

    // ── put_log_events ──

    #[test]
    fn put_log_events_missing_group() {
        let svc = make_service();
        let req = make_request("PutLogEvents", json!({"logStreamName": "s"}));
        assert!(svc.put_log_events(&req).is_err());
    }

    #[test]
    fn put_log_events_missing_stream() {
        let svc = make_service();
        let req = make_request("PutLogEvents", json!({"logGroupName": "g"}));
        assert!(svc.put_log_events(&req).is_err());
    }

    #[test]
    fn put_log_events_missing_events() {
        let svc = make_service();
        create_group(&svc, "g");
        create_stream(&svc, "g", "s");
        let req = make_request(
            "PutLogEvents",
            json!({"logGroupName": "g", "logStreamName": "s"}),
        );
        assert!(svc.put_log_events(&req).is_err());
    }

    #[test]
    fn put_log_events_non_chronological_errors() {
        let svc = make_service();
        create_group(&svc, "g");
        create_stream(&svc, "g", "s");
        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "g",
                "logStreamName": "s",
                "logEvents": [
                    {"timestamp": now, "message": "b"},
                    {"timestamp": now - 1000, "message": "a"}
                ]
            }),
        );
        assert!(svc.put_log_events(&req).is_err());
    }

    #[test]
    fn put_log_events_too_old_rejected() {
        let svc = make_service();
        create_group(&svc, "g");
        create_stream(&svc, "g", "s");
        let now = chrono::Utc::now().timestamp_millis();
        let old = now - 20 * 24 * 60 * 60 * 1000;
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "g",
                "logStreamName": "s",
                "logEvents": [
                    {"timestamp": old, "message": "old"},
                    {"timestamp": now, "message": "new"}
                ]
            }),
        );
        let resp = svc.put_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["rejectedLogEventsInfo"].is_object());
    }

    #[test]
    fn put_log_events_too_new_rejected() {
        let svc = make_service();
        create_group(&svc, "g");
        create_stream(&svc, "g", "s");
        let now = chrono::Utc::now().timestamp_millis();
        let future = now + 10 * 60 * 60 * 1000;
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "g",
                "logStreamName": "s",
                "logEvents": [
                    {"timestamp": now, "message": "now"},
                    {"timestamp": future, "message": "future"}
                ]
            }),
        );
        let resp = svc.put_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["rejectedLogEventsInfo"].is_object());
    }

    #[test]
    fn put_log_events_nonexistent_group_errors() {
        let svc = make_service();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "missing",
                "logStreamName": "s",
                "logEvents": [{"timestamp": 1000, "message": "x"}]
            }),
        );
        assert!(svc.put_log_events(&req).is_err());
    }

    #[test]
    fn put_log_events_nonexistent_stream_errors() {
        let svc = make_service();
        create_group(&svc, "g");
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "g",
                "logStreamName": "missing",
                "logEvents": [{"timestamp": 1000, "message": "x"}]
            }),
        );
        assert!(svc.put_log_events(&req).is_err());
    }

    // ── get_log_events ──

    #[test]
    fn get_log_events_basic_returns_events() {
        let svc = make_service();
        create_group(&svc, "g");
        create_stream(&svc, "g", "s");
        put_events(&svc, "g", "s", &["a", "b", "c"]);

        let req = make_request(
            "GetLogEvents",
            json!({"logGroupName": "g", "logStreamName": "s"}),
        );
        let resp = svc.get_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 3);
        assert!(body["nextForwardToken"].is_string());
        assert!(body["nextBackwardToken"].is_string());
    }

    #[test]
    fn get_log_events_start_from_head() {
        let svc = make_service();
        create_group(&svc, "g");
        create_stream(&svc, "g", "s");
        put_events(&svc, "g", "s", &["a", "b", "c"]);

        let req = make_request(
            "GetLogEvents",
            json!({
                "logGroupName": "g",
                "logStreamName": "s",
                "startFromHead": true,
                "limit": 2
            }),
        );
        let resp = svc.get_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn get_log_events_missing_group_errors() {
        let svc = make_service();
        let req = make_request("GetLogEvents", json!({"logStreamName": "s"}));
        assert!(svc.get_log_events(&req).is_err());
    }

    #[test]
    fn get_log_events_missing_stream_errors() {
        let svc = make_service();
        let req = make_request("GetLogEvents", json!({"logGroupName": "g"}));
        assert!(svc.get_log_events(&req).is_err());
    }

    #[test]
    fn get_log_events_limit_over_10000() {
        let svc = make_service();
        let req = make_request(
            "GetLogEvents",
            json!({"logGroupName": "g", "logStreamName": "s", "limit": 20000}),
        );
        assert!(svc.get_log_events(&req).is_err());
    }

    #[test]
    fn get_log_events_invalid_next_token_format() {
        let svc = make_service();
        let req = make_request(
            "GetLogEvents",
            json!({
                "logGroupName": "g",
                "logStreamName": "s",
                "nextToken": "bogus"
            }),
        );
        assert!(svc.get_log_events(&req).is_err());
    }

    #[test]
    fn get_log_events_identifier_colon_star_errors() {
        let svc = make_service();
        let req = make_request(
            "GetLogEvents",
            json!({
                "logGroupIdentifier": "arn:aws:logs:us-east-1:123456789012:log-group:g:*",
                "logStreamName": "s"
            }),
        );
        assert!(svc.get_log_events(&req).is_err());
    }

    #[test]
    fn get_log_events_identifier_as_arn_resolves() {
        let svc = make_service();
        create_group(&svc, "ga");
        create_stream(&svc, "ga", "s");
        put_events(&svc, "ga", "s", &["x"]);
        let req = make_request(
            "GetLogEvents",
            json!({
                "logGroupIdentifier": "arn:aws:logs:us-east-1:123456789012:log-group:ga",
                "logStreamName": "s"
            }),
        );
        let resp = svc.get_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn get_log_events_time_range_filters() {
        let svc = make_service();
        create_group(&svc, "g");
        create_stream(&svc, "g", "s");
        put_events(&svc, "g", "s", &["a", "b", "c"]);

        let req = make_request(
            "GetLogEvents",
            json!({
                "logGroupName": "g",
                "logStreamName": "s",
                "startTime": 0,
                "endTime": i64::MAX
            }),
        );
        let resp = svc.get_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn get_log_events_missing_group_resource_error() {
        let svc = make_service();
        let req = make_request(
            "GetLogEvents",
            json!({"logGroupName": "missing", "logStreamName": "s"}),
        );
        assert!(svc.get_log_events(&req).is_err());
    }

    #[test]
    fn get_log_events_missing_stream_resource_error() {
        let svc = make_service();
        create_group(&svc, "g");
        let req = make_request(
            "GetLogEvents",
            json!({"logGroupName": "g", "logStreamName": "missing"}),
        );
        assert!(svc.get_log_events(&req).is_err());
    }

    // ── retention enforcement ──

    #[test]
    fn get_log_events_drops_events_older_than_retention() {
        let svc = make_service();
        create_group(&svc, "g");
        create_stream(&svc, "g", "s");
        let now = chrono::Utc::now().timestamp_millis();
        // 12 days ago: still inside the 14-day PutLogEvents window,
        // but older than a 1-day retention.
        let twelve_days_ago = now - 12 * 86_400_000;
        put_events_at(&svc, "g", "s", &["old1", "old2"], twelve_days_ago);
        put_events(&svc, "g", "s", &["fresh"]);
        put_retention(&svc, "g", 1);

        let req = make_request(
            "GetLogEvents",
            json!({"logGroupName": "g", "logStreamName": "s", "startFromHead": true}),
        );
        let resp = svc.get_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let events = body["events"].as_array().unwrap();
        assert_eq!(events.len(), 1, "expected only the fresh event");
        assert_eq!(events[0]["message"].as_str().unwrap(), "fresh");
    }

    #[test]
    fn get_log_events_no_retention_returns_all_events() {
        let svc = make_service();
        create_group(&svc, "g");
        create_stream(&svc, "g", "s");
        let now = chrono::Utc::now().timestamp_millis();
        put_events_at(&svc, "g", "s", &["old"], now - 12 * 86_400_000);
        put_events(&svc, "g", "s", &["fresh"]);

        let req = make_request(
            "GetLogEvents",
            json!({"logGroupName": "g", "logStreamName": "s", "startFromHead": true}),
        );
        let resp = svc.get_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn filter_log_events_drops_events_older_than_retention() {
        let svc = make_service();
        create_group(&svc, "g");
        create_stream(&svc, "g", "s");
        let now = chrono::Utc::now().timestamp_millis();
        put_events_at(&svc, "g", "s", &["stale"], now - 12 * 86_400_000);
        put_events(&svc, "g", "s", &["recent"]);
        put_retention(&svc, "g", 1);

        let req = make_request("FilterLogEvents", json!({"logGroupName": "g"}));
        let resp = svc.filter_log_events(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let events = body["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["message"].as_str().unwrap(), "recent");
    }

    // ── deliveries coverage ──

    #[test]
    fn put_delivery_destination_missing_name_errors() {
        let svc = make_service();
        let req = make_request(
            "PutDeliveryDestination",
            json!({"deliveryDestinationType": "S3"}),
        );
        assert!(svc.put_delivery_destination(&req).is_err());
    }

    #[test]
    fn put_delivery_destination_invalid_type_errors() {
        let svc = make_service();
        let req = make_request(
            "PutDeliveryDestination",
            json!({"name": "d1", "deliveryDestinationType": "BOGUS"}),
        );
        assert!(svc.put_delivery_destination(&req).is_err());
    }

    #[test]
    fn put_delivery_destination_invalid_output_format_errors() {
        let svc = make_service();
        let req = make_request(
            "PutDeliveryDestination",
            json!({"name": "d1", "outputFormat": "xml"}),
        );
        assert!(svc.put_delivery_destination(&req).is_err());
    }

    #[test]
    fn get_delivery_destination_unknown_errors() {
        let svc = make_service();
        let req = make_request("GetDeliveryDestination", json!({"name": "ghost"}));
        assert!(svc.get_delivery_destination(&req).is_err());
    }

    #[test]
    fn delete_delivery_destination_unknown_errors() {
        let svc = make_service();
        let req = make_request("DeleteDeliveryDestination", json!({"name": "ghost"}));
        assert!(svc.delete_delivery_destination(&req).is_err());
    }

    #[test]
    fn put_delivery_source_missing_name_errors() {
        let svc = make_service();
        let req = make_request(
            "PutDeliverySource",
            json!({"resourceArn": "arn:aws:logs:us-east-1:123:log-group:g"}),
        );
        assert!(svc.put_delivery_source(&req).is_err());
    }

    #[test]
    fn get_delivery_source_unknown_errors() {
        let svc = make_service();
        let req = make_request("GetDeliverySource", json!({"name": "ghost"}));
        assert!(svc.get_delivery_source(&req).is_err());
    }

    #[test]
    fn delete_delivery_source_unknown_errors() {
        let svc = make_service();
        let req = make_request("DeleteDeliverySource", json!({"name": "ghost"}));
        assert!(svc.delete_delivery_source(&req).is_err());
    }

    #[test]
    fn get_delivery_unknown_errors() {
        let svc = make_service();
        let req = make_request("GetDelivery", json!({"id": "ghost"}));
        assert!(svc.get_delivery(&req).is_err());
    }

    #[test]
    fn delete_delivery_unknown_errors() {
        let svc = make_service();
        let req = make_request("DeleteDelivery", json!({"id": "ghost"}));
        assert!(svc.delete_delivery(&req).is_err());
    }

    #[test]
    fn put_delivery_destination_policy_unknown_errors() {
        let svc = make_service();
        let req = make_request(
            "PutDeliveryDestinationPolicy",
            json!({
                "deliveryDestinationName": "ghost",
                "deliveryDestinationPolicy": "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
            }),
        );
        assert!(svc.put_delivery_destination_policy(&req).is_err());
    }

    #[test]
    fn get_delivery_destination_policy_unknown_errors() {
        let svc = make_service();
        let req = make_request(
            "GetDeliveryDestinationPolicy",
            json!({"deliveryDestinationName": "ghost"}),
        );
        assert!(svc.get_delivery_destination_policy(&req).is_err());
    }

    #[test]
    fn delete_delivery_destination_policy_unknown_errors() {
        let svc = make_service();
        let req = make_request(
            "DeleteDeliveryDestinationPolicy",
            json!({"deliveryDestinationName": "ghost"}),
        );
        assert!(svc.delete_delivery_destination_policy(&req).is_err());
    }

    #[test]
    fn delete_resource_policy_unknown_errors() {
        let svc = make_service();
        let req = make_request("DeleteResourcePolicy", json!({"policyName": "ghost"}));
        assert!(svc.delete_resource_policy(&req).is_err());
    }

    #[test]
    fn delete_account_policy_unknown_errors() {
        let svc = make_service();
        let req = make_request(
            "DeleteAccountPolicy",
            json!({"policyName": "ghost", "policyType": "DATA_PROTECTION_POLICY"}),
        );
        assert!(svc.delete_account_policy(&req).is_err());
    }

    #[test]
    fn get_data_protection_policy_unknown_errors() {
        let svc = make_service();
        let req = make_request(
            "GetDataProtectionPolicy",
            json!({"logGroupIdentifier": "ghost"}),
        );
        assert!(svc.get_data_protection_policy(&req).is_err());
    }

    #[test]
    fn delete_data_protection_policy_unknown_errors() {
        let svc = make_service();
        let req = make_request(
            "DeleteDataProtectionPolicy",
            json!({"logGroupIdentifier": "ghost"}),
        );
        assert!(svc.delete_data_protection_policy(&req).is_err());
    }

    #[test]
    fn list_log_anomaly_detectors_empty_ok() {
        let svc = make_service();
        let req = make_request("ListLogAnomalyDetectors", json!({}));
        let resp = svc.list_log_anomaly_detectors(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["anomalyDetectors"].is_array());
    }

    #[test]
    fn put_destination_missing_name_errors() {
        let svc = make_service();
        let req = make_request(
            "PutDestination",
            json!({"targetArn": "arn:foo", "roleArn": "arn:bar"}),
        );
        assert!(svc.put_destination(&req).is_err());
    }

    #[test]
    fn delete_destination_unknown_errors() {
        let svc = make_service();
        let req = make_request("DeleteDestination", json!({"destinationName": "ghost"}));
        assert!(svc.delete_destination(&req).is_err());
    }

    #[test]
    fn describe_destinations_empty_ok() {
        let svc = make_service();
        let req = make_request("DescribeDestinations", json!({}));
        let resp = svc.describe_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["destinations"].is_array());
    }

    // ---- Metric filter publishes data to CloudWatch ----

    #[derive(Default)]
    struct MetricRecorder {
        calls: parking_lot::Mutex<Vec<(String, String, f64, i64)>>,
    }

    impl fakecloud_core::delivery::CloudwatchDelivery for MetricRecorder {
        fn put_metric(
            &self,
            _account_id: &str,
            _region: &str,
            namespace: &str,
            metric_name: &str,
            value: f64,
            _unit: Option<&str>,
            _dimensions: std::collections::BTreeMap<String, String>,
            timestamp_ms: i64,
        ) {
            self.calls.lock().push((
                namespace.to_string(),
                metric_name.to_string(),
                value,
                timestamp_ms,
            ));
        }
    }

    fn make_service_with_metrics(recorder: std::sync::Arc<MetricRecorder>) -> LogsService {
        use fakecloud_core::delivery::DeliveryBus;
        let state = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let bus = DeliveryBus::new().with_cloudwatch_metrics(recorder);
        LogsService::new(state, std::sync::Arc::new(bus))
    }

    fn put_metric_filter(
        svc: &LogsService,
        group: &str,
        name: &str,
        pattern: &str,
        namespace: &str,
        metric: &str,
        value: &str,
    ) {
        let req = make_request(
            "PutMetricFilter",
            json!({
                "filterName": name,
                "filterPattern": pattern,
                "logGroupName": group,
                "metricTransformations": [
                    {
                        "metricName": metric,
                        "metricNamespace": namespace,
                        "metricValue": value,
                    }
                ],
            }),
        );
        svc.put_metric_filter(&req).unwrap();
    }

    #[test]
    fn put_log_events_publishes_metric_for_matching_filter() {
        let recorder = std::sync::Arc::new(MetricRecorder::default());
        let svc = make_service_with_metrics(recorder.clone());
        create_group(&svc, "mf-publish");
        create_stream(&svc, "mf-publish", "s1");
        put_metric_filter(
            &svc,
            "mf-publish",
            "errors-filter",
            "ERROR",
            "MyApp",
            "ErrorCount",
            "1",
        );

        put_events(&svc, "mf-publish", "s1", &["ERROR: timeout"]);

        let calls = recorder.calls.lock();
        assert_eq!(calls.len(), 1, "expected one metric data point");
        assert_eq!(calls[0].0, "MyApp");
        assert_eq!(calls[0].1, "ErrorCount");
        assert_eq!(calls[0].2, 1.0);
    }

    #[test]
    fn put_log_events_skips_metric_when_pattern_does_not_match() {
        let recorder = std::sync::Arc::new(MetricRecorder::default());
        let svc = make_service_with_metrics(recorder.clone());
        create_group(&svc, "mf-skip");
        create_stream(&svc, "mf-skip", "s1");
        put_metric_filter(
            &svc,
            "mf-skip",
            "errors-filter",
            "ERROR",
            "MyApp",
            "ErrorCount",
            "1",
        );

        put_events(&svc, "mf-skip", "s1", &["INFO: ok"]);

        assert!(
            recorder.calls.lock().is_empty(),
            "non-matching event must not publish a metric"
        );
    }

    #[test]
    fn put_log_events_extracts_metric_value_from_json_path() {
        let recorder = std::sync::Arc::new(MetricRecorder::default());
        let svc = make_service_with_metrics(recorder.clone());
        create_group(&svc, "mf-json");
        create_stream(&svc, "mf-json", "s1");
        put_metric_filter(
            &svc,
            "mf-json",
            "bytes-filter",
            "{ $.bytes > 0 }",
            "MyApp",
            "BytesProcessed",
            "$.bytes",
        );

        put_events(&svc, "mf-json", "s1", &[r#"{"bytes": 2048}"#]);

        let calls = recorder.calls.lock();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].2, 2048.0);
    }

    #[test]
    fn get_query_results_drops_events_older_than_retention() {
        let svc = make_service();
        create_group(&svc, "ret-q");
        create_stream(&svc, "ret-q", "s");
        let now = chrono::Utc::now().timestamp_millis();
        put_events_at(&svc, "ret-q", "s", &["stale"], now - 12 * 86_400_000);
        put_events(&svc, "ret-q", "s", &["recent"]);
        put_retention(&svc, "ret-q", 1);

        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "ret-q",
                "queryString": "fields @message",
                "startTime": (now / 1000) - 30 * 86_400,
                "endTime": now / 1000,
            }),
        );
        let resp = svc.start_query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let qid = body["queryId"].as_str().unwrap().to_string();

        let req = make_request("GetQueryResults", json!({"queryId": qid}));
        let resp = svc.get_query_results(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let scanned = body["statistics"]["recordsScanned"].as_f64().unwrap();
        assert_eq!(scanned, 1.0, "retention should hide the 12-day-old event");
    }
}
