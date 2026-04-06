use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{
    Delivery, DeliveryDestination, DeliverySource, Destination, ExportTask, LogEvent, LogGroup,
    LogStream, MetricFilter, MetricTransformation, QueryDefinition, QueryInfo, ResourcePolicy,
    SharedLogsState, SubscriptionFilter,
};

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
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "PutRetentionPolicy" => self.put_retention_policy(&req),
            "DeleteRetentionPolicy" => self.delete_retention_policy(&req),
            "PutSubscriptionFilter" => self.put_subscription_filter(&req),
            "DescribeSubscriptionFilters" => self.describe_subscription_filters(&req),
            "DeleteSubscriptionFilter" => self.delete_subscription_filter(&req),
            "PutMetricFilter" => self.put_metric_filter(&req),
            "DescribeMetricFilters" => self.describe_metric_filters(&req),
            "DeleteMetricFilter" => self.delete_metric_filter(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "DescribeResourcePolicies" => self.describe_resource_policies(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            "PutDestination" => self.put_destination(&req),
            "DescribeDestinations" => self.describe_destinations(&req),
            "DeleteDestination" => self.delete_destination(&req),
            "PutDestinationPolicy" => self.put_destination_policy(&req),
            "StartQuery" => self.start_query(&req),
            "GetQueryResults" => self.get_query_results(&req),
            "DescribeQueries" => self.describe_queries(&req),
            "CreateExportTask" => self.create_export_task(&req),
            "DescribeExportTasks" => self.describe_export_tasks(&req),
            "CancelExportTask" => self.cancel_export_task(&req),
            "PutDeliveryDestination" => self.put_delivery_destination(&req),
            "GetDeliveryDestination" => self.get_delivery_destination(&req),
            "DescribeDeliveryDestinations" => self.describe_delivery_destinations(&req),
            "DeleteDeliveryDestination" => self.delete_delivery_destination(&req),
            "PutDeliveryDestinationPolicy" => self.put_delivery_destination_policy(&req),
            "GetDeliveryDestinationPolicy" => self.get_delivery_destination_policy(&req),
            "DeleteDeliveryDestinationPolicy" => self.delete_delivery_destination_policy(&req),
            "PutDeliverySource" => self.put_delivery_source(&req),
            "GetDeliverySource" => self.get_delivery_source(&req),
            "DescribeDeliverySources" => self.describe_delivery_sources(&req),
            "DeleteDeliverySource" => self.delete_delivery_source(&req),
            "CreateDelivery" => self.create_delivery(&req),
            "GetDelivery" => self.get_delivery(&req),
            "DescribeDeliveries" => self.describe_deliveries(&req),
            "DeleteDelivery" => self.delete_delivery(&req),
            "AssociateKmsKey" => self.associate_kms_key(&req),
            "DisassociateKmsKey" => self.disassociate_kms_key(&req),
            "PutQueryDefinition" => self.put_query_definition(&req),
            "DescribeQueryDefinitions" => self.describe_query_definitions(&req),
            "DeleteQueryDefinition" => self.delete_query_definition(&req),
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
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
            "PutRetentionPolicy",
            "DeleteRetentionPolicy",
            "PutSubscriptionFilter",
            "DescribeSubscriptionFilters",
            "DeleteSubscriptionFilter",
            "PutMetricFilter",
            "DescribeMetricFilters",
            "DeleteMetricFilter",
            "PutResourcePolicy",
            "DescribeResourcePolicies",
            "DeleteResourcePolicy",
            "PutDestination",
            "DescribeDestinations",
            "DeleteDestination",
            "PutDestinationPolicy",
            "StartQuery",
            "GetQueryResults",
            "DescribeQueries",
            "CreateExportTask",
            "DescribeExportTasks",
            "CancelExportTask",
            "PutDeliveryDestination",
            "GetDeliveryDestination",
            "DescribeDeliveryDestinations",
            "DeleteDeliveryDestination",
            "PutDeliveryDestinationPolicy",
            "GetDeliveryDestinationPolicy",
            "DeleteDeliveryDestinationPolicy",
            "PutDeliverySource",
            "GetDeliverySource",
            "DescribeDeliverySources",
            "DeleteDeliverySource",
            "CreateDelivery",
            "GetDelivery",
            "DescribeDeliveries",
            "DeleteDelivery",
            "AssociateKmsKey",
            "DisassociateKmsKey",
            "PutQueryDefinition",
            "DescribeQueryDefinitions",
            "DeleteQueryDefinition",
        ]
    }
}

fn body_json(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn generate_sequence_token() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    // u128 max is ~3.4e38, so we limit to 38 digits to avoid overflow
    format!("{:038}", nanos % 10u128.pow(38))
}

fn validation_error(field: &str, value: &str, constraint: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterException",
        format!(
            "1 validation error detected: Value '{value}' at '{field}' failed to satisfy constraint: {constraint}"
        ),
    )
}

impl LogsService {
    // ---- Log Groups ----

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

        validate_string_length("logGroupName", &name, 1, 512)?;
        validate_optional_string_length("kmsKeyId", body["kmsKeyId"].as_str(), 0, 256)?;

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

        let kms_key_id = body["kmsKeyId"].as_str().map(|s| s.to_string());

        state.log_groups.insert(
            name.clone(),
            LogGroup {
                name,
                arn,
                creation_time: now,
                retention_in_days: None,
                kms_key_id,
                tags,
                log_streams: std::collections::HashMap::new(),
                stored_bytes: 0,
                subscription_filters: Vec::new(),
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

        validate_string_length("logGroupName", name, 1, 512)?;

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
        let next_token = body["nextToken"].as_str();

        validate_optional_string_length(
            "logGroupNamePrefix",
            body["logGroupNamePrefix"].as_str(),
            1,
            512,
        )?;
        validate_optional_string_length(
            "logGroupNamePattern",
            body["logGroupNamePattern"].as_str(),
            0,
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

        let state = self.state.read();
        let mut groups: Vec<&LogGroup> = state
            .log_groups
            .values()
            .filter(|g| prefix.is_empty() || g.name.starts_with(prefix))
            .collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));

        // Handle pagination
        let start_idx = if let Some(token) = next_token {
            groups
                .iter()
                .position(|g| g.name.as_str() > token)
                .unwrap_or(groups.len())
        } else {
            0
        };

        let page = &groups[start_idx..];
        let has_more = page.len() > limit;
        let page = if has_more { &page[..limit] } else { page };

        let log_groups: Vec<Value> = page
            .iter()
            .map(|g| {
                let log_group_arn = g.arn.trim_end_matches(":*").to_string();
                let mut obj = json!({
                    "logGroupName": g.name,
                    "arn": g.arn,
                    "logGroupArn": log_group_arn,
                    "creationTime": g.creation_time,
                    "storedBytes": g.stored_bytes,
                    "metricFilterCount": 0,
                });
                if let Some(days) = g.retention_in_days {
                    obj["retentionInDays"] = json!(days);
                }
                if let Some(ref kms) = g.kms_key_id {
                    obj["kmsKeyId"] = json!(kms);
                }
                obj
            })
            .collect();

        let mut result = json!({ "logGroups": log_groups });
        if has_more {
            if let Some(last) = page.last() {
                result["nextToken"] = json!(last.name);
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    // ---- Log Streams ----

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

    fn describe_log_streams(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        stream.events.append(&mut new_events);
        stream.events.sort_by_key(|e| e.timestamp);

        let mut response = json!({
            "nextSequenceToken": stream.upload_sequence_token,
        });
        if has_rejected {
            response["rejectedLogEventsInfo"] = rejected_info;
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&response).unwrap(),
        ))
    }

    fn get_log_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
        let next_token = body["nextToken"].as_str();
        let stream_names: Vec<&str> = body["logStreamNames"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        validate_string_length("logGroupName", group_name, 1, 512)?;
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
        validate_optional_string_length("filterPattern", Some(filter_pattern), 0, 1024)?;

        // Validate limit
        if limit > 10000 {
            return Err(validation_error(
                "limit",
                &limit.to_string(),
                "Member must have value less than or equal to 10000",
            ));
        }

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
                    group_name,
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

    // ---- Tags (legacy) ----

    fn tag_log_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;

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

        validate_string_length("logGroupName", name, 1, 512)?;

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

        validate_string_length("logGroupName", name, 1, 512)?;

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

    // ---- Tags (new API: TagResource/UntagResource/ListTagsForResource) ----

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let arn = body["resourceArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "resourceArn is required",
            )
        })?;

        validate_string_length("resourceArn", arn, 1, 1011)?;

        let tags = body["tags"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "tags is required",
            )
        })?;

        let new_tags: std::collections::HashMap<String, String> = tags
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
            .collect();

        let mut state = self.state.write();

        // Try log group
        if let Some(group) = state
            .log_groups
            .values_mut()
            .find(|g| g.arn == arn || g.arn.trim_end_matches(":*") == arn)
        {
            for (k, v) in new_tags {
                group.tags.insert(k, v);
            }
            return Ok(AwsResponse::json(StatusCode::OK, "{}"));
        }

        // Try destination
        if let Some(dest) = state.destinations.values_mut().find(|d| d.arn == arn) {
            for (k, v) in new_tags {
                dest.tags.insert(k, v);
            }
            return Ok(AwsResponse::json(StatusCode::OK, "{}"));
        }

        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("The specified resource does not exist: {arn}"),
        ))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let arn = body["resourceArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "resourceArn is required",
            )
        })?;

        validate_string_length("resourceArn", arn, 1, 1011)?;

        let tag_keys = body["tagKeys"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "tagKeys is required",
            )
        })?;

        let keys: Vec<String> = tag_keys
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        let mut state = self.state.write();

        // Try log group
        if let Some(group) = state
            .log_groups
            .values_mut()
            .find(|g| g.arn == arn || g.arn.trim_end_matches(":*") == arn)
        {
            for k in &keys {
                group.tags.remove(k);
            }
            return Ok(AwsResponse::json(StatusCode::OK, "{}"));
        }

        // Try destination
        if let Some(dest) = state.destinations.values_mut().find(|d| d.arn == arn) {
            for k in &keys {
                dest.tags.remove(k);
            }
            return Ok(AwsResponse::json(StatusCode::OK, "{}"));
        }

        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("The specified resource does not exist: {arn}"),
        ))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let arn = body["resourceArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "resourceArn is required",
            )
        })?;

        validate_string_length("resourceArn", arn, 1, 1011)?;

        let state = self.state.read();

        // Try log group
        if let Some(group) = state
            .log_groups
            .values()
            .find(|g| g.arn == arn || g.arn.trim_end_matches(":*") == arn)
        {
            return Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({ "tags": group.tags })).unwrap(),
            ));
        }

        // Try destination
        if let Some(dest) = state.destinations.values().find(|d| d.arn == arn) {
            return Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({ "tags": dest.tags })).unwrap(),
            ));
        }

        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("The specified resource does not exist: {arn}"),
        ))
    }

    // ---- Retention Policy ----

    fn put_retention_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;

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

        validate_string_length("logGroupName", name, 1, 512)?;

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

    // ---- Subscription Filters ----

    fn put_subscription_filter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let filter_name = body["filterName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "filterName is required",
                )
            })?
            .to_string();
        let filter_pattern = body["filterPattern"].as_str().unwrap_or("").to_string();
        let destination_arn = body["destinationArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "destinationArn is required",
                )
            })?
            .to_string();
        let role_arn = body["roleArn"].as_str().map(|s| s.to_string());
        let distribution = body["distribution"]
            .as_str()
            .unwrap_or("ByLogStream")
            .to_string();

        validate_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_string_length("filterName", &filter_name, 1, 512)?;
        validate_optional_string_length("filterPattern", Some(&filter_pattern), 0, 1024)?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(log_group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            )
        })?;

        // Check if updating existing filter
        if let Some(existing) = group
            .subscription_filters
            .iter_mut()
            .find(|f| f.filter_name == filter_name)
        {
            existing.filter_pattern = filter_pattern;
            existing.destination_arn = destination_arn;
            existing.role_arn = role_arn;
            existing.distribution = distribution;
            return Ok(AwsResponse::json(StatusCode::OK, "{}"));
        }

        // Max 2 subscription filters per log group
        if group.subscription_filters.len() >= 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "LimitExceededException",
                "Resource limit exceeded.",
            ));
        }

        let now = Utc::now().timestamp_millis();
        group.subscription_filters.push(SubscriptionFilter {
            filter_name,
            log_group_name: log_group_name.to_string(),
            filter_pattern,
            destination_arn,
            role_arn,
            distribution,
            creation_time: now,
        });

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn describe_subscription_filters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_optional_string_length(
            "filterNamePrefix",
            body["filterNamePrefix"].as_str(),
            1,
            512,
        )?;

        let state = self.state.read();
        let group = state.log_groups.get(log_group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            )
        })?;

        let filters: Vec<Value> = group
            .subscription_filters
            .iter()
            .map(|f| {
                let mut obj = json!({
                    "filterName": f.filter_name,
                    "logGroupName": f.log_group_name,
                    "filterPattern": f.filter_pattern,
                    "destinationArn": f.destination_arn,
                    "distribution": f.distribution,
                    "creationTime": f.creation_time,
                });
                if let Some(ref arn) = f.role_arn {
                    obj["roleArn"] = json!(arn);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "subscriptionFilters": filters })).unwrap(),
        ))
    }

    fn delete_subscription_filter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let filter_name = body["filterName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "filterName is required",
            )
        })?;

        validate_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_string_length("filterName", filter_name, 1, 512)?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(log_group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            )
        })?;

        let idx = group
            .subscription_filters
            .iter()
            .position(|f| f.filter_name == filter_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The specified subscription filter does not exist.",
                )
            })?;

        group.subscription_filters.remove(idx);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Metric Filters ----

    fn put_metric_filter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let filter_name = body["filterName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "filterName is required",
                )
            })?
            .to_string();
        let filter_pattern = body["filterPattern"].as_str().unwrap_or("").to_string();
        let log_group_name = body["logGroupName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupName is required",
                )
            })?
            .to_string();

        validate_string_length("filterName", &filter_name, 1, 512)?;
        validate_string_length("logGroupName", &log_group_name, 1, 512)?;
        validate_optional_string_length("filterPattern", Some(&filter_pattern), 0, 1024)?;

        let transformations_json = body["metricTransformations"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "metricTransformations is required",
            )
        })?;

        // Validate max 1 transformation
        if transformations_json.len() > 1 {
            return Err(validation_error(
                "metricTransformations",
                &format!("{}", transformations_json.len()),
                "Member must have length less than or equal to 1",
            ));
        }

        let transformations: Vec<MetricTransformation> = transformations_json
            .iter()
            .map(|t| MetricTransformation {
                metric_name: t["metricName"].as_str().unwrap_or("").to_string(),
                metric_namespace: t["metricNamespace"].as_str().unwrap_or("").to_string(),
                metric_value: t["metricValue"].as_str().unwrap_or("").to_string(),
                default_value: t["defaultValue"].as_f64(),
            })
            .collect();

        let now = Utc::now().timestamp_millis();

        let mut state = self.state.write();

        // Update existing or add new
        if let Some(existing) = state
            .metric_filters
            .iter_mut()
            .find(|f| f.filter_name == filter_name && f.log_group_name == log_group_name)
        {
            existing.filter_pattern = filter_pattern;
            existing.metric_transformations = transformations;
        } else {
            state.metric_filters.push(MetricFilter {
                filter_name,
                filter_pattern,
                log_group_name,
                metric_transformations: transformations,
                creation_time: now,
            });
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn describe_metric_filters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let filter_name_prefix = body["filterNamePrefix"].as_str();
        let log_group_name = body["logGroupName"].as_str();
        let metric_name = body["metricName"].as_str();
        let metric_namespace = body["metricNamespace"].as_str();

        validate_optional_string_length("filterNamePrefix", filter_name_prefix, 1, 512)?;
        validate_optional_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_optional_string_length("metricName", metric_name, 0, 255)?;
        validate_optional_string_length("metricNamespace", metric_namespace, 0, 255)?;

        let state = self.state.read();
        let filters: Vec<Value> = state
            .metric_filters
            .iter()
            .filter(|f| {
                if let Some(prefix) = filter_name_prefix {
                    if !f.filter_name.starts_with(prefix) {
                        return false;
                    }
                }
                if let Some(lg) = log_group_name {
                    if f.log_group_name != lg {
                        return false;
                    }
                }
                if let Some(mn) = metric_name {
                    if !f.metric_transformations.iter().any(|t| t.metric_name == mn) {
                        return false;
                    }
                }
                if let Some(ns) = metric_namespace {
                    if !f
                        .metric_transformations
                        .iter()
                        .any(|t| t.metric_namespace == ns)
                    {
                        return false;
                    }
                }
                true
            })
            .map(|f| {
                let transformations: Vec<Value> = f
                    .metric_transformations
                    .iter()
                    .map(|t| {
                        let mut obj = json!({
                            "metricName": t.metric_name,
                            "metricNamespace": t.metric_namespace,
                            "metricValue": t.metric_value,
                        });
                        if let Some(dv) = t.default_value {
                            obj["defaultValue"] = json!(dv);
                        }
                        obj
                    })
                    .collect();

                json!({
                    "filterName": f.filter_name,
                    "filterPattern": f.filter_pattern,
                    "logGroupName": f.log_group_name,
                    "metricTransformations": transformations,
                    "creationTime": f.creation_time,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "metricFilters": filters })).unwrap(),
        ))
    }

    fn delete_metric_filter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let filter_name = body["filterName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "filterName is required",
            )
        })?;
        let log_group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("filterName", filter_name, 1, 512)?;
        validate_string_length("logGroupName", log_group_name, 1, 512)?;

        let mut state = self.state.write();
        let idx = state
            .metric_filters
            .iter()
            .position(|f| f.filter_name == filter_name && f.log_group_name == log_group_name);

        if let Some(i) = idx {
            state.metric_filters.remove(i);
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Resource Policies ----

    fn put_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let policy_name = body["policyName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "policyName is required",
                )
            })?
            .to_string();
        let policy_document = body["policyDocument"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "policyDocument is required",
                )
            })?
            .to_string();

        let now = Utc::now().timestamp_millis();

        let mut state = self.state.write();

        // Check limit (10 per region) only if adding new
        if !state.resource_policies.contains_key(&policy_name)
            && state.resource_policies.len() >= 10
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "LimitExceededException",
                "Resource limit exceeded.",
            ));
        }

        let policy = ResourcePolicy {
            policy_name: policy_name.clone(),
            policy_document: policy_document.clone(),
            last_updated_time: now,
        };

        state.resource_policies.insert(policy_name.clone(), policy);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "resourcePolicy": {
                    "policyName": policy_name,
                    "policyDocument": policy_document,
                    "lastUpdatedTime": now,
                }
            }))
            .unwrap(),
        ))
    }

    fn describe_resource_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let _body = body_json(req);
        let state = self.state.read();

        let mut policies: Vec<Value> = state
            .resource_policies
            .values()
            .map(|p| {
                json!({
                    "policyName": p.policy_name,
                    "policyDocument": p.policy_document,
                    "lastUpdatedTime": p.last_updated_time,
                })
            })
            .collect();
        policies.sort_by(|a, b| {
            a["policyName"]
                .as_str()
                .unwrap()
                .cmp(b["policyName"].as_str().unwrap())
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "resourcePolicies": policies })).unwrap(),
        ))
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let policy_name = body["policyName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyName is required",
            )
        })?;

        let mut state = self.state.write();
        if state.resource_policies.remove(policy_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Policy with name [{policy_name}] does not exist"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Destinations ----

    fn put_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let destination_name = body["destinationName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "destinationName is required",
                )
            })?
            .to_string();
        let target_arn = body["targetArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "targetArn is required",
                )
            })?
            .to_string();
        let role_arn = body["roleArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "roleArn is required",
                )
            })?
            .to_string();

        validate_string_length("destinationName", &destination_name, 1, 512)?;

        let tags: std::collections::HashMap<String, String> = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();
        let arn = format!(
            "arn:aws:logs:{}:{}:destination:{}",
            state.region, state.account_id, destination_name
        );
        let now = Utc::now().timestamp_millis();

        // Update or create
        let access_policy = state
            .destinations
            .get(&destination_name)
            .and_then(|d| d.access_policy.clone());

        let dest = Destination {
            destination_name: destination_name.clone(),
            target_arn: target_arn.clone(),
            role_arn: role_arn.clone(),
            arn: arn.clone(),
            access_policy,
            creation_time: now,
            tags: tags.clone(),
        };

        state.destinations.insert(destination_name.clone(), dest);

        let dest_json = json!({
            "destinationName": destination_name,
            "targetArn": target_arn,
            "roleArn": role_arn,
            "arn": arn,
            "creationTime": now,
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "destination": dest_json })).unwrap(),
        ))
    }

    fn describe_destinations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let prefix = body["DestinationNamePrefix"].as_str().unwrap_or("");

        validate_optional_string_length(
            "DestinationNamePrefix",
            body["DestinationNamePrefix"].as_str(),
            1,
            512,
        )?;

        let state = self.state.read();
        let destinations: Vec<Value> = state
            .destinations
            .values()
            .filter(|d| prefix.is_empty() || d.destination_name.starts_with(prefix))
            .map(|d| {
                let mut obj = json!({
                    "destinationName": d.destination_name,
                    "targetArn": d.target_arn,
                    "roleArn": d.role_arn,
                    "arn": d.arn,
                    "creationTime": d.creation_time,
                });
                if let Some(ref policy) = d.access_policy {
                    obj["accessPolicy"] = json!(policy);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "destinations": destinations })).unwrap(),
        ))
    }

    fn delete_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["destinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "destinationName is required",
            )
        })?;

        validate_string_length("destinationName", name, 1, 512)?;

        let mut state = self.state.write();
        if state.destinations.remove(name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified destination does not exist: {name}"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_destination_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["destinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "destinationName is required",
            )
        })?;

        validate_string_length("destinationName", name, 1, 512)?;

        let policy = body["accessPolicy"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "accessPolicy is required",
            )
        })?;

        let mut state = self.state.write();
        let dest = state.destinations.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified destination does not exist: {name}"),
            )
        })?;

        dest.access_policy = Some(policy.to_string());

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Queries ----

    fn start_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let start_time = body["startTime"].as_i64().unwrap_or(0);
        let end_time = body["endTime"].as_i64().unwrap_or(0);
        let query_string = body["queryString"].as_str().unwrap_or("").to_string();

        validate_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_optional_string_length("queryString", Some(&query_string), 0, 10000)?;

        let mut state = self.state.write();

        // Verify log group exists
        if !state.log_groups.contains_key(log_group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            ));
        }

        let query_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().timestamp_millis();

        state.queries.insert(
            query_id.clone(),
            QueryInfo {
                query_id: query_id.clone(),
                log_group_name: log_group_name.to_string(),
                query_string,
                start_time,
                end_time,
                status: "Complete".to_string(),
                create_time: now,
            },
        );

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "queryId": query_id })).unwrap(),
        ))
    }

    fn get_query_results(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let query_id = body["queryId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "queryId is required",
            )
        })?;

        validate_string_length("queryId", query_id, 1, 256)?;

        let state = self.state.read();
        let query = state.queries.get(query_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified query does not exist.",
            )
        })?;

        // Find matching log events
        let mut results: Vec<Value> = Vec::new();
        if let Some(group) = state.log_groups.get(&query.log_group_name) {
            for stream in group.log_streams.values() {
                for event in &stream.events {
                    // Convert timestamps: query uses seconds, events use milliseconds
                    let event_time_secs = event.timestamp / 1000;
                    if event_time_secs >= query.start_time && event_time_secs < query.end_time {
                        results.push(json!([
                            {"field": "@message", "value": event.message},
                            {"field": "@ptr", "value": format!("{}/{}", stream.name, event.timestamp)},
                        ]));
                    }
                }
            }
        }

        // Sort by @message value
        results.sort_by(|a, b| {
            let a_msg = a[0]["value"].as_str().unwrap_or("");
            let b_msg = b[0]["value"].as_str().unwrap_or("");
            a_msg.cmp(b_msg)
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "status": query.status,
                "results": results,
                "statistics": {
                    "recordsMatched": results.len() as f64,
                    "recordsScanned": results.len() as f64,
                    "bytesScanned": 0.0,
                },
            }))
            .unwrap(),
        ))
    }

    fn describe_queries(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str();
        let status_filter = body["status"].as_str();

        validate_optional_string_length("logGroupName", log_group_name, 1, 512)?;

        let state = self.state.read();
        let queries: Vec<Value> = state
            .queries
            .values()
            .filter(|q| {
                if let Some(lg) = log_group_name {
                    if q.log_group_name != lg {
                        return false;
                    }
                }
                if let Some(status) = status_filter {
                    if q.status != status {
                        return false;
                    }
                }
                true
            })
            .map(|q| {
                json!({
                    "queryId": q.query_id,
                    "queryString": q.query_string,
                    "status": q.status,
                    "createTime": q.create_time,
                    "logGroupName": q.log_group_name,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "queries": queries })).unwrap(),
        ))
    }

    // ---- Export Tasks ----

    fn create_export_task(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupName is required",
                )
            })?
            .to_string();
        let from_time = body["from"].as_i64().unwrap_or(0);
        let to_time = body["to"].as_i64().unwrap_or(0);
        let destination = body["destination"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "destination is required",
                )
            })?
            .to_string();
        let destination_prefix = body["destinationPrefix"]
            .as_str()
            .unwrap_or("exportedlogs")
            .to_string();

        validate_string_length("logGroupName", &log_group_name, 1, 512)?;
        validate_optional_string_length("taskName", body["taskName"].as_str(), 1, 512)?;
        validate_optional_string_length(
            "logStreamNamePrefix",
            body["logStreamNamePrefix"].as_str(),
            1,
            512,
        )?;
        validate_string_length("destination", &destination, 1, 512)?;

        let state = self.state.read();
        if !state.log_groups.contains_key(&log_group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            ));
        }
        drop(state);

        let task_id = uuid::Uuid::new_v4().to_string();
        let (status_code, status_message) = if from_time < to_time {
            (
                "COMPLETED".to_string(),
                "Completed successfully".to_string(),
            )
        } else {
            ("active".to_string(), "Task is active".to_string())
        };

        let mut state = self.state.write();
        state.export_tasks.push(ExportTask {
            task_id: task_id.clone(),
            log_group_name,
            from_time,
            to_time,
            destination,
            destination_prefix,
            status_code,
            status_message,
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "taskId": task_id })).unwrap(),
        ))
    }

    fn describe_export_tasks(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let task_id_filter = body["taskId"].as_str();

        validate_optional_string_length("taskId", task_id_filter, 1, 512)?;

        let state = self.state.read();

        if let Some(task_id) = task_id_filter {
            let task = state.export_tasks.iter().find(|t| t.task_id == task_id);
            if task.is_none() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The specified export task does not exist.",
                ));
            }
        }

        let tasks: Vec<Value> = state
            .export_tasks
            .iter()
            .filter(|t| {
                if let Some(tid) = task_id_filter {
                    t.task_id == tid
                } else {
                    true
                }
            })
            .map(|t| {
                json!({
                    "taskId": t.task_id,
                    "logGroupName": t.log_group_name,
                    "from": t.from_time,
                    "to": t.to_time,
                    "destination": t.destination,
                    "destinationPrefix": t.destination_prefix,
                    "status": {
                        "code": t.status_code,
                        "message": t.status_message,
                    },
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "exportTasks": tasks })).unwrap(),
        ))
    }

    fn cancel_export_task(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let task_id = body["taskId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "taskId is required",
            )
        })?;

        validate_string_length("taskId", task_id, 1, 512)?;

        let mut state = self.state.write();
        let task = state
            .export_tasks
            .iter_mut()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The specified export task does not exist.",
                )
            })?;

        task.status_code = "CANCELLED".to_string();
        task.status_message = "Task was cancelled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Delivery Destinations ----

    fn put_delivery_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "name is required",
                )
            })?
            .to_string();

        validate_string_length("name", &name, 1, 60)?;

        let output_format = body["outputFormat"].as_str().map(|s| s.to_string());

        // Validate output format
        if let Some(ref fmt) = output_format {
            let valid = ["json", "plain", "w3c", "raw", "parquet"];
            if !valid.contains(&fmt.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("1 validation error detected: Value '{fmt}' at 'outputFormat' failed to satisfy constraint: Member must satisfy enum value set: [json, plain, w3c, raw, parquet]"),
                ));
            }
        }

        let config: std::collections::HashMap<String, String> = body
            ["deliveryDestinationConfiguration"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let tags: std::collections::HashMap<String, String> = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();

        // Check if updating - cannot change output format
        if let Some(existing) = state.delivery_destinations.get(&name) {
            if let Some(ref new_fmt) = output_format {
                if let Some(ref existing_fmt) = existing.output_format {
                    if new_fmt != existing_fmt {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "ValidationException",
                            "Cannot update outputFormat for an existing delivery destination.",
                        ));
                    }
                }
            }
        }

        let arn = format!(
            "arn:aws:logs:{}:{}:delivery-destination:{}",
            state.region, state.account_id, name
        );

        let existing_policy = state
            .delivery_destinations
            .get(&name)
            .and_then(|d| d.delivery_destination_policy.clone());

        let dd = DeliveryDestination {
            name: name.clone(),
            arn: arn.clone(),
            output_format: output_format.clone(),
            delivery_destination_configuration: config.clone(),
            tags: tags.clone(),
            delivery_destination_policy: existing_policy,
        };

        state.delivery_destinations.insert(name.clone(), dd);

        // Build the configuration object for the response, preserving existing fields
        // and ensuring destinationResourceArn is always present
        let config_resp = {
            let mut c: serde_json::Map<String, Value> =
                config.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
            c.entry("destinationResourceArn".to_string())
                .or_insert(Value::Null);
            Value::Object(c)
        };

        let mut resp = json!({
            "deliveryDestination": {
                "name": name,
                "arn": arn,
                "deliveryDestinationConfiguration": config_resp,
            }
        });
        if let Some(ref fmt) = output_format {
            resp["deliveryDestination"]["outputFormat"] = json!(fmt);
        }
        if !tags.is_empty() {
            resp["deliveryDestination"]["tags"] = json!(tags);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&resp).unwrap(),
        ))
    }

    fn get_delivery_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let state = self.state.read();
        let dd = state.delivery_destinations.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            )
        })?;

        let mut obj = json!({
            "name": dd.name,
            "arn": dd.arn,
            "deliveryDestinationConfiguration": dd.delivery_destination_configuration,
        });
        if let Some(ref fmt) = dd.output_format {
            obj["outputFormat"] = json!(fmt);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "deliveryDestination": obj })).unwrap(),
        ))
    }

    fn describe_delivery_destinations(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let dds: Vec<Value> = state
            .delivery_destinations
            .values()
            .map(|dd| {
                let mut obj = json!({
                    "name": dd.name,
                    "arn": dd.arn,
                    "deliveryDestinationConfiguration": dd.delivery_destination_configuration,
                });
                if let Some(ref fmt) = dd.output_format {
                    obj["outputFormat"] = json!(fmt);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "deliveryDestinations": dds })).unwrap(),
        ))
    }

    fn delete_delivery_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let mut state = self.state.write();
        if state.delivery_destinations.remove(name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_delivery_destination_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["deliveryDestinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "deliveryDestinationName is required",
            )
        })?;
        let policy = body["deliveryDestinationPolicy"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "deliveryDestinationPolicy is required",
                )
            })?
            .to_string();

        validate_string_length("deliveryDestinationName", name, 1, 60)?;
        validate_string_length("deliveryDestinationPolicy", &policy, 1, 51200)?;

        let mut state = self.state.write();
        let dd = state.delivery_destinations.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            )
        })?;

        dd.delivery_destination_policy = Some(policy.clone());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "policy": {
                    "deliveryDestinationPolicy": policy,
                }
            }))
            .unwrap(),
        ))
    }

    fn get_delivery_destination_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["deliveryDestinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "deliveryDestinationName is required",
            )
        })?;

        validate_string_length("deliveryDestinationName", name, 1, 60)?;

        let state = self.state.read();
        let dd = state.delivery_destinations.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            )
        })?;

        let policy_json = if let Some(ref policy) = dd.delivery_destination_policy {
            json!({
                "deliveryDestinationPolicy": policy,
            })
        } else {
            json!({})
        };

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "policy": policy_json,
            }))
            .unwrap(),
        ))
    }

    fn delete_delivery_destination_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["deliveryDestinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "deliveryDestinationName is required",
            )
        })?;

        validate_string_length("deliveryDestinationName", name, 1, 60)?;

        let mut state = self.state.write();
        let dd = state.delivery_destinations.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery destination '{name}' does not exist."),
            )
        })?;

        dd.delivery_destination_policy = None;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Delivery Sources ----

    fn put_delivery_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "name is required",
                )
            })?
            .to_string();
        let resource_arn = body["resourceArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "resourceArn is required",
                )
            })?
            .to_string();
        let log_type = body["logType"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logType is required",
                )
            })?
            .to_string();

        validate_string_length("name", &name, 1, 60)?;
        validate_string_length("logType", &log_type, 1, 255)?;

        let tags: std::collections::HashMap<String, String> = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        // Extract service from ARN
        let service = resource_arn
            .split(':')
            .nth(2)
            .unwrap_or("unknown")
            .to_string();

        // Validate resource ARN format - must start with arn:aws:
        if !resource_arn.starts_with("arn:aws:") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("Invalid resource ARN: {resource_arn}"),
            ));
        }

        // S3 cannot be a delivery source
        if service == "s3" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The resource ARN '{resource_arn}' is not a valid delivery source."),
            ));
        }

        // Validate log type based on service
        let valid_log_types: &[&str] = match service.as_str() {
            "cloudfront" => &["ACCESS_LOGS"],
            _ => &["ACCESS_LOGS", "APPLICATION_LOGS", "FW_LOGS"],
        };
        if !valid_log_types.contains(&log_type.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("Log type '{log_type}' is not valid for this resource."),
            ));
        }

        let mut state = self.state.write();

        // Cannot update with different resourceArn
        if let Some(existing) = state.delivery_sources.get(&name) {
            if !existing.resource_arns.is_empty() && existing.resource_arns[0] != resource_arn {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ConflictException",
                    "Cannot update delivery source with a different resourceArn.",
                ));
            }
        }

        let arn = format!(
            "arn:aws:logs:{}:{}:delivery-source:{}",
            state.region, state.account_id, name
        );

        let ds = DeliverySource {
            name: name.clone(),
            arn: arn.clone(),
            resource_arns: vec![resource_arn],
            service: service.clone(),
            log_type: log_type.clone(),
            tags: tags.clone(),
        };

        state.delivery_sources.insert(name.clone(), ds);

        let state_ref = state.delivery_sources.get(&name).unwrap();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "deliverySource": {
                    "name": state_ref.name,
                    "arn": state_ref.arn,
                    "resourceArns": state_ref.resource_arns,
                    "service": state_ref.service,
                    "logType": state_ref.log_type,
                    "tags": state_ref.tags,
                }
            }))
            .unwrap(),
        ))
    }

    fn get_delivery_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let state = self.state.read();
        let ds = state.delivery_sources.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery source '{name}' does not exist."),
            )
        })?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "deliverySource": {
                    "name": ds.name,
                    "arn": ds.arn,
                    "resourceArns": ds.resource_arns,
                    "service": ds.service,
                    "logType": ds.log_type,
                    "tags": ds.tags,
                }
            }))
            .unwrap(),
        ))
    }

    fn describe_delivery_sources(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let sources: Vec<Value> = state
            .delivery_sources
            .values()
            .map(|ds| {
                json!({
                    "name": ds.name,
                    "arn": ds.arn,
                    "resourceArns": ds.resource_arns,
                    "service": ds.service,
                    "logType": ds.log_type,
                    "tags": ds.tags,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "deliverySources": sources })).unwrap(),
        ))
    }

    fn delete_delivery_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "name is required",
            )
        })?;

        validate_string_length("name", name, 1, 60)?;

        let mut state = self.state.write();
        if state.delivery_sources.remove(name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery source '{name}' does not exist."),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Deliveries ----

    fn create_delivery(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let delivery_source_name = body["deliverySourceName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "deliverySourceName is required",
                )
            })?
            .to_string();
        let delivery_destination_arn = body["deliveryDestinationArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "deliveryDestinationArn is required",
                )
            })?
            .to_string();

        validate_string_length("deliverySourceName", &delivery_source_name, 1, 60)?;
        validate_optional_string_length("fieldDelimiter", body["fieldDelimiter"].as_str(), 0, 5)?;

        let tags: std::collections::HashMap<String, String> = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let record_fields: Vec<String> = body["recordFields"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let field_delimiter = body["fieldDelimiter"].as_str().map(|s| s.to_string());
        let s3_delivery_config = body["s3DeliveryConfiguration"].clone();

        let mut state = self.state.write();

        // Verify source exists
        if !state.delivery_sources.contains_key(&delivery_source_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery source '{}' does not exist.", delivery_source_name),
            ));
        }

        // Verify destination exists
        let dest_exists = state
            .delivery_destinations
            .values()
            .any(|dd| dd.arn == delivery_destination_arn);
        if !dest_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!(
                    "Delivery destination '{}' does not exist.",
                    delivery_destination_arn
                ),
            ));
        }

        // Check for duplicate delivery (same source + destination)
        let already_exists = state.deliveries.values().any(|d| {
            d.delivery_source_name == delivery_source_name
                && d.delivery_destination_arn == delivery_destination_arn
        });
        if already_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ConflictException",
                "A delivery already exists for this source and destination.",
            ));
        }

        // Determine destination type from ARN
        let dest_type = if delivery_destination_arn.contains(":s3:") {
            "S3"
        } else if delivery_destination_arn.contains(":firehose:") {
            "FH"
        } else {
            "CWL"
        };

        let delivery_id = uuid::Uuid::new_v4().to_string();
        let arn = format!(
            "arn:aws:logs:{}:{}:delivery:{}",
            state.region, state.account_id, delivery_id
        );

        let delivery = Delivery {
            id: delivery_id.clone(),
            delivery_source_name: delivery_source_name.clone(),
            delivery_destination_arn: delivery_destination_arn.clone(),
            delivery_destination_type: dest_type.to_string(),
            arn: arn.clone(),
            tags: tags.clone(),
        };

        state.deliveries.insert(delivery_id.clone(), delivery);

        let mut delivery_json = json!({
            "id": delivery_id,
            "deliverySourceName": delivery_source_name,
            "deliveryDestinationArn": delivery_destination_arn,
            "deliveryDestinationType": dest_type,
            "arn": arn,
            "tags": tags,
        });
        if !record_fields.is_empty() {
            delivery_json["recordFields"] = json!(record_fields);
        }
        if let Some(ref delim) = field_delimiter {
            delivery_json["fieldDelimiter"] = json!(delim);
        }
        if !s3_delivery_config.is_null() {
            delivery_json["s3DeliveryConfiguration"] = s3_delivery_config;
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "delivery": delivery_json,
            }))
            .unwrap(),
        ))
    }

    fn get_delivery(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let delivery_id = body["id"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "id is required",
            )
        })?;

        validate_string_length("id", delivery_id, 1, 64)?;

        let state = self.state.read();
        let d = state.deliveries.get(delivery_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery '{delivery_id}' does not exist."),
            )
        })?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "delivery": {
                    "id": d.id,
                    "deliverySourceName": d.delivery_source_name,
                    "deliveryDestinationArn": d.delivery_destination_arn,
                    "deliveryDestinationType": d.delivery_destination_type,
                    "arn": d.arn,
                    "tags": d.tags,
                }
            }))
            .unwrap(),
        ))
    }

    fn describe_deliveries(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let deliveries: Vec<Value> = state
            .deliveries
            .values()
            .map(|d| {
                json!({
                    "id": d.id,
                    "deliverySourceName": d.delivery_source_name,
                    "deliveryDestinationArn": d.delivery_destination_arn,
                    "deliveryDestinationType": d.delivery_destination_type,
                    "arn": d.arn,
                    "tags": d.tags,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "deliveries": deliveries })).unwrap(),
        ))
    }

    fn delete_delivery(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let delivery_id = body["id"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "id is required",
            )
        })?;

        validate_string_length("id", delivery_id, 1, 64)?;

        let mut state = self.state.write();
        if state.deliveries.remove(delivery_id).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Delivery '{delivery_id}' does not exist."),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- KMS Key ----

    fn associate_kms_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let kms_key_id = body["kmsKeyId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "kmsKeyId is required",
                )
            })?
            .to_string();

        validate_string_length("logGroupName", name, 1, 512)?;
        validate_string_length("kmsKeyId", &kms_key_id, 0, 256)?;
        validate_optional_string_length(
            "resourceIdentifier",
            body["resourceIdentifier"].as_str(),
            1,
            2048,
        )?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        group.kms_key_id = Some(kms_key_id);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn disassociate_kms_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;
        validate_optional_string_length(
            "resourceIdentifier",
            body["resourceIdentifier"].as_str(),
            1,
            2048,
        )?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        group.kms_key_id = None;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Query Definitions ----

    fn put_query_definition(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "name is required",
                )
            })?
            .to_string();
        let query_string = body["queryString"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "queryString is required",
                )
            })?
            .to_string();
        let log_group_names: Vec<String> = body["logGroupNames"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let query_definition_id = body["queryDefinitionId"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        validate_string_length("name", &name, 1, 255)?;
        validate_string_length("queryString", &query_string, 1, 10000)?;
        validate_optional_string_length(
            "queryDefinitionId",
            body["queryDefinitionId"].as_str(),
            1,
            256,
        )?;

        let now = Utc::now().timestamp_millis();

        let mut state = self.state.write();
        state.query_definitions.insert(
            query_definition_id.clone(),
            QueryDefinition {
                query_definition_id: query_definition_id.clone(),
                name,
                query_string,
                log_group_names,
                last_modified: now,
            },
        );

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "queryDefinitionId": query_definition_id,
            }))
            .unwrap(),
        ))
    }

    fn describe_query_definitions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length(
            "queryDefinitionNamePrefix",
            body["queryDefinitionNamePrefix"].as_str(),
            1,
            255,
        )?;

        let state = self.state.read();
        let defs: Vec<Value> = state
            .query_definitions
            .values()
            .map(|qd| {
                json!({
                    "queryDefinitionId": qd.query_definition_id,
                    "name": qd.name,
                    "queryString": qd.query_string,
                    "logGroupNames": qd.log_group_names,
                    "lastModified": qd.last_modified,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "queryDefinitions": defs })).unwrap(),
        ))
    }

    fn delete_query_definition(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let qd_id = body["queryDefinitionId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "queryDefinitionId is required",
            )
        })?;

        validate_string_length("queryDefinitionId", qd_id, 1, 256)?;

        let mut state = self.state.write();
        let success = state.query_definitions.remove(qd_id).is_some();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "success": success })).unwrap(),
        ))
    }
}

/// Extract log group name from ARN like "arn:aws:logs:region:account:log-group:name"
fn extract_log_group_from_arn(arn: &str) -> Option<String> {
    // arn:aws:logs:region:account:log-group:name
    let parts: Vec<&str> = arn.splitn(7, ':').collect();
    if parts.len() >= 7 && parts[5] == "log-group" {
        Some(parts[6].to_string())
    } else {
        None
    }
}

/// CloudWatch Logs filter pattern matching.
///
/// Rules:
/// - Empty pattern or patterns starting with `{` (JSON patterns) match everything
/// - Quoted string `"foo bar"` matches the exact substring
/// - Multiple unquoted words `foo bar` means ALL words must appear anywhere in the message
/// - Single unquoted word `foo` is a simple substring match
fn matches_filter_pattern(pattern: &str, message: &str) -> bool {
    let pattern = pattern.trim();

    // Empty pattern matches everything
    if pattern.is_empty() {
        return true;
    }

    // JSON/metric filter patterns (start with { or [) - we don't parse these, match everything
    if pattern.starts_with('{') || pattern.starts_with('[') {
        return true;
    }

    // Quoted pattern: exact substring match
    if pattern.starts_with('"') && pattern.ends_with('"') && pattern.len() >= 2 {
        let inner = &pattern[1..pattern.len() - 1];
        return message.contains(inner);
    }

    // Multiple words: all must be present
    let words: Vec<&str> = pattern.split_whitespace().collect();
    words.iter().all(|word| message.contains(word))
}
