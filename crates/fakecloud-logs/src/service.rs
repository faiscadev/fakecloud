use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use http::StatusCode;
use serde_json::{json, Value};

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{
    AccountPolicy, AnomalyDetector, DataProtectionPolicy, Delivery, DeliveryDestination,
    DeliverySource, Destination, ExportTask, ImportTask, IndexPolicy, Integration, LogEvent,
    LogGroup, LogStream, LookupTable, MetricFilter, MetricTransformation, QueryDefinition,
    QueryInfo, ResourcePolicy, ScheduledQuery, SharedLogsState, SubscriptionFilter, Transformer,
};

pub struct LogsService {
    state: SharedLogsState,
    delivery_bus: Arc<DeliveryBus>,
}

impl LogsService {
    pub fn new(state: SharedLogsState, delivery_bus: Arc<DeliveryBus>) -> Self {
        Self {
            state,
            delivery_bus,
        }
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
            "PutAccountPolicy" => self.put_account_policy(&req),
            "DescribeAccountPolicies" => self.describe_account_policies(&req),
            "DeleteAccountPolicy" => self.delete_account_policy(&req),
            "PutDataProtectionPolicy" => self.put_data_protection_policy(&req),
            "GetDataProtectionPolicy" => self.get_data_protection_policy(&req),
            "DeleteDataProtectionPolicy" => self.delete_data_protection_policy(&req),
            "PutIndexPolicy" => self.put_index_policy(&req),
            "DescribeIndexPolicies" => self.describe_index_policies(&req),
            "DeleteIndexPolicy" => self.delete_index_policy(&req),
            "DescribeFieldIndexes" => self.describe_field_indexes(&req),
            "PutTransformer" => self.put_transformer(&req),
            "GetTransformer" => self.get_transformer(&req),
            "DeleteTransformer" => self.delete_transformer(&req),
            "TestTransformer" => self.test_transformer(&req),
            "CreateLogAnomalyDetector" => self.create_log_anomaly_detector(&req),
            "GetLogAnomalyDetector" => self.get_log_anomaly_detector(&req),
            "DeleteLogAnomalyDetector" => self.delete_log_anomaly_detector(&req),
            "ListLogAnomalyDetectors" => self.list_log_anomaly_detectors(&req),
            "UpdateLogAnomalyDetector" => self.update_log_anomaly_detector(&req),
            "GetLogGroupFields" => self.get_log_group_fields(&req),
            "TestMetricFilter" => self.test_metric_filter(&req),
            "StopQuery" => self.stop_query(&req),
            "PutLogGroupDeletionProtection" => self.put_log_group_deletion_protection(&req),
            "GetLogRecord" => self.get_log_record(&req),
            "ListAnomalies" => self.list_anomalies(&req),
            "UpdateAnomaly" => self.update_anomaly(&req),
            "CreateImportTask" => self.create_import_task(&req),
            "DescribeImportTasks" => self.describe_import_tasks(&req),
            "DescribeImportTaskBatches" => self.describe_import_task_batches(&req),
            "CancelImportTask" => self.cancel_import_task(&req),
            "PutIntegration" => self.put_integration(&req),
            "GetIntegration" => self.get_integration(&req),
            "DeleteIntegration" => self.delete_integration(&req),
            "ListIntegrations" => self.list_integrations(&req),
            "CreateLookupTable" => self.create_lookup_table(&req),
            "GetLookupTable" => self.get_lookup_table(&req),
            "DescribeLookupTables" => self.describe_lookup_tables(&req),
            "DeleteLookupTable" => self.delete_lookup_table(&req),
            "UpdateLookupTable" => self.update_lookup_table(&req),
            "CreateScheduledQuery" => self.create_scheduled_query(&req),
            "GetScheduledQuery" => self.get_scheduled_query(&req),
            "GetScheduledQueryHistory" => self.get_scheduled_query_history(&req),
            "ListScheduledQueries" => self.list_scheduled_queries(&req),
            "DeleteScheduledQuery" => self.delete_scheduled_query(&req),
            "UpdateScheduledQuery" => self.update_scheduled_query(&req),
            "StartLiveTail" => self.start_live_tail(&req),
            "ListLogGroups" => self.list_log_groups(&req),
            "ListLogGroupsForQuery" => self.list_log_groups_for_query(&req),
            "ListAggregateLogGroupSummaries" => self.list_aggregate_log_group_summaries(&req),
            "PutBearerTokenAuthentication" => self.put_bearer_token_authentication(&req),
            "GetLogObject" => self.get_log_object(&req),
            "GetLogFields" => self.get_log_fields(&req),
            "AssociateSourceToS3TableIntegration" => {
                self.associate_source_to_s3_table_integration(&req)
            }
            "ListSourcesForS3TableIntegration" => self.list_sources_for_s3_table_integration(&req),
            "DisassociateSourceFromS3TableIntegration" => {
                self.disassociate_source_from_s3_table_integration(&req)
            }
            "UpdateDeliveryConfiguration" => self.update_delivery_configuration(&req),
            "DescribeConfigurationTemplates" => self.describe_configuration_templates(&req),
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
            "PutAccountPolicy",
            "DescribeAccountPolicies",
            "DeleteAccountPolicy",
            "PutDataProtectionPolicy",
            "GetDataProtectionPolicy",
            "DeleteDataProtectionPolicy",
            "PutIndexPolicy",
            "DescribeIndexPolicies",
            "DeleteIndexPolicy",
            "DescribeFieldIndexes",
            "PutTransformer",
            "GetTransformer",
            "DeleteTransformer",
            "TestTransformer",
            "CreateLogAnomalyDetector",
            "GetLogAnomalyDetector",
            "DeleteLogAnomalyDetector",
            "ListLogAnomalyDetectors",
            "UpdateLogAnomalyDetector",
            "GetLogGroupFields",
            "TestMetricFilter",
            "StopQuery",
            "PutLogGroupDeletionProtection",
            "GetLogRecord",
            "ListAnomalies",
            "UpdateAnomaly",
            "CreateImportTask",
            "DescribeImportTasks",
            "DescribeImportTaskBatches",
            "CancelImportTask",
            "PutIntegration",
            "GetIntegration",
            "DeleteIntegration",
            "ListIntegrations",
            "CreateLookupTable",
            "GetLookupTable",
            "DescribeLookupTables",
            "DeleteLookupTable",
            "UpdateLookupTable",
            "CreateScheduledQuery",
            "GetScheduledQuery",
            "GetScheduledQueryHistory",
            "ListScheduledQueries",
            "DeleteScheduledQuery",
            "UpdateScheduledQuery",
            "StartLiveTail",
            "ListLogGroups",
            "ListLogGroupsForQuery",
            "ListAggregateLogGroupSummaries",
            "PutBearerTokenAuthentication",
            "GetLogObject",
            "GetLogFields",
            "AssociateSourceToS3TableIntegration",
            "ListSourcesForS3TableIntegration",
            "DisassociateSourceFromS3TableIntegration",
            "UpdateDeliveryConfiguration",
            "DescribeConfigurationTemplates",
        ]
    }
}

fn body_json(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn require_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body[field].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            format!("{field} is required"),
        )
    })
}

/// Build a delivery destination configuration JSON object, ensuring
/// `destinationResourceArn` is always present as a string (Smithy requirement).
fn dd_config_json(config: &std::collections::HashMap<String, String>) -> Value {
    let mut m: serde_json::Map<String, Value> =
        config.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
    m.entry("destinationResourceArn".to_string())
        .or_insert_with(|| json!(""));
    Value::Object(m)
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
        validate_optional_string_length("kmsKeyId", body["kmsKeyId"].as_str(), 1, 256)?;

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
                data_protection_policy: None,
                index_policies: Vec::new(),
                transformer: None,
                deletion_protection: false,
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
        // Check deletion protection
        if let Some(group) = state.log_groups.get(name) {
            if group.deletion_protection {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "OperationAbortedException",
                    format!("Log group {name} has deletion protection enabled"),
                ));
            }
        }
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
        let pattern = body["logGroupNamePattern"].as_str().unwrap_or("");
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
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "logGroupClass",
            &body["logGroupClass"],
            &["STANDARD", "INFREQUENT_ACCESS", "DELIVERY"],
        )?;

        let state = self.state.read();
        let mut groups: Vec<&LogGroup> = state
            .log_groups
            .values()
            .filter(|g| {
                (prefix.is_empty() || g.name.starts_with(prefix))
                    && (pattern.is_empty() || g.name.contains(pattern))
            })
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
        validate_required("filterPattern", &body["filterPattern"])?;
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
        validate_optional_string_length(
            "fieldSelectionCriteria",
            body["fieldSelectionCriteria"].as_str(),
            0,
            2000,
        )?;

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
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;

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
        let body = body_json(req);
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "policyScope",
            &body["policyScope"],
            &["ACCOUNT", "RESOURCE"],
        )?;
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
        validate_string_length("targetArn", &target_arn, 1, 2048)?;
        validate_string_length("roleArn", &role_arn, 1, 2048)?;

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
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;

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

        validate_string_length("accessPolicy", policy, 1, 5120)?;

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
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "status",
            &body["status"],
            &[
                "Scheduled",
                "Running",
                "Complete",
                "Failed",
                "Cancelled",
                "Timeout",
                "Unknown",
            ],
        )?;
        validate_optional_enum_value(
            "queryLanguage",
            &body["queryLanguage"],
            &["CWLI", "SQL", "PPL"],
        )?;

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

        let task_name = body["taskName"].as_str().map(|s| s.to_string());
        let log_stream_name_prefix = body["logStreamNamePrefix"].as_str().map(|s| s.to_string());

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
            task_name,
            log_group_name,
            log_stream_name_prefix,
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
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "statusCode",
            &body["statusCode"],
            &[
                "CANCELLED",
                "COMPLETED",
                "FAILED",
                "PENDING",
                "PENDING_CANCEL",
                "RUNNING",
            ],
        )?;

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
                let mut obj = json!({
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
                });
                if let Some(ref name) = t.task_name {
                    obj["taskName"] = json!(name);
                }
                if let Some(ref prefix) = t.log_stream_name_prefix {
                    obj["logStreamNamePrefix"] = json!(prefix);
                }
                obj
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

        validate_optional_enum_value(
            "deliveryDestinationType",
            &body["deliveryDestinationType"],
            &["S3", "CWL", "FH", "XRAY"],
        )?;

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
        // and always including destinationResourceArn (Smithy shape requires string, not null)
        let config_resp = {
            let mut c: serde_json::Map<String, Value> =
                config.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
            c.entry("destinationResourceArn".to_string())
                .or_insert_with(|| json!(""));
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
            "deliveryDestinationConfiguration": dd_config_json(&dd.delivery_destination_configuration),
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
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;

        let state = self.state.read();
        let dds: Vec<Value> = state
            .delivery_destinations
            .values()
            .map(|dd| {
                let mut obj = json!({
                    "name": dd.name,
                    "arn": dd.arn,
                    "deliveryDestinationConfiguration": dd_config_json(&dd.delivery_destination_configuration),
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

    fn describe_delivery_sources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;

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

    fn describe_deliveries(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;

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
        let log_group_name = body["logGroupName"].as_str();
        let resource_identifier = body["resourceIdentifier"].as_str();
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

        if let Some(name) = log_group_name {
            validate_string_length("logGroupName", name, 1, 512)?;
        }
        validate_string_length("kmsKeyId", &kms_key_id, 1, 256)?;
        validate_optional_string_length("resourceIdentifier", resource_identifier, 1, 2048)?;

        let resolved_name = resolve_log_group_name(log_group_name, resource_identifier)?;

        let mut state = self.state.write();
        let group = state
            .log_groups
            .get_mut(resolved_name.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("The specified log group does not exist: {resolved_name}"),
                )
            })?;

        group.kms_key_id = Some(kms_key_id);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn disassociate_kms_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str();
        let resource_identifier = body["resourceIdentifier"].as_str();

        if let Some(name) = log_group_name {
            validate_string_length("logGroupName", name, 1, 512)?;
        }
        validate_optional_string_length("resourceIdentifier", resource_identifier, 1, 2048)?;

        let resolved_name = resolve_log_group_name(log_group_name, resource_identifier)?;

        let mut state = self.state.write();
        let group = state
            .log_groups
            .get_mut(resolved_name.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("The specified log group does not exist: {resolved_name}"),
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
        validate_optional_string_length("clientToken", body["clientToken"].as_str(), 36, 128)?;
        validate_optional_enum_value(
            "queryLanguage",
            &body["queryLanguage"],
            &["CWLI", "SQL", "PPL"],
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
        let name_prefix = body["queryDefinitionNamePrefix"].as_str().unwrap_or("");
        validate_optional_string_length(
            "queryDefinitionNamePrefix",
            body["queryDefinitionNamePrefix"].as_str(),
            1,
            255,
        )?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "queryLanguage",
            &body["queryLanguage"],
            &["CWLI", "SQL", "PPL"],
        )?;

        let state = self.state.read();
        let defs: Vec<Value> = state
            .query_definitions
            .values()
            .filter(|qd| name_prefix.is_empty() || qd.name.starts_with(name_prefix))
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

    // ---- Account Policies ----

    fn put_account_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_enum_value(
            "policyType",
            &body["policyType"],
            &[
                "DATA_PROTECTION_POLICY",
                "SUBSCRIPTION_FILTER_POLICY",
                "FIELD_INDEX_POLICY",
                "TRANSFORMER_POLICY",
                "METRIC_EXTRACTION_POLICY",
            ],
        )?;
        validate_optional_enum_value("scope", &body["scope"], &["ALL"])?;
        let policy_name = body["policyName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyName is required",
            )
        })?;
        let policy_type = body["policyType"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyType is required",
            )
        })?;
        let policy_document = body["policyDocument"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyDocument is required",
            )
        })?;

        let now = Utc::now().timestamp_millis();
        let mut state = self.state.write();
        let account_id = state.account_id.clone();
        let scope = body["scope"].as_str().map(|s| s.to_string());
        let selection_criteria = body["selectionCriteria"].as_str().map(|s| s.to_string());

        let policy = AccountPolicy {
            policy_name: policy_name.to_string(),
            policy_type: policy_type.to_string(),
            policy_document: policy_document.to_string(),
            scope: scope.clone(),
            selection_criteria: selection_criteria.clone(),
            account_id: account_id.clone(),
            last_updated_time: now,
        };

        let key = (policy_name.to_string(), policy_type.to_string());
        state.account_policies.insert(key, policy);

        let mut result = json!({
            "accountPolicy": {
                "policyName": policy_name,
                "policyType": policy_type,
                "policyDocument": policy_document,
                "accountId": account_id,
                "lastUpdatedTime": now,
            }
        });
        if let Some(s) = scope {
            result["accountPolicy"]["scope"] = json!(s);
        }
        if let Some(s) = selection_criteria {
            result["accountPolicy"]["selectionCriteria"] = json!(s);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    fn describe_account_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_enum_value(
            "policyType",
            &body["policyType"],
            &[
                "DATA_PROTECTION_POLICY",
                "SUBSCRIPTION_FILTER_POLICY",
                "FIELD_INDEX_POLICY",
                "TRANSFORMER_POLICY",
                "METRIC_EXTRACTION_POLICY",
            ],
        )?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        let policy_type = body["policyType"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyType is required",
            )
        })?;
        let policy_name = body["policyName"].as_str();

        let state = self.state.read();
        let policies: Vec<Value> = state
            .account_policies
            .values()
            .filter(|p| {
                p.policy_type == policy_type && policy_name.is_none_or(|n| p.policy_name == n)
            })
            .map(|p| {
                let mut obj = json!({
                    "policyName": p.policy_name,
                    "policyType": p.policy_type,
                    "policyDocument": p.policy_document,
                    "accountId": p.account_id,
                    "lastUpdatedTime": p.last_updated_time,
                });
                if let Some(ref s) = p.scope {
                    obj["scope"] = json!(s);
                }
                if let Some(ref s) = p.selection_criteria {
                    obj["selectionCriteria"] = json!(s);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "accountPolicies": policies })).unwrap(),
        ))
    }

    fn delete_account_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let policy_name = body["policyName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyName is required",
            )
        })?;
        let policy_type = body["policyType"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyType is required",
            )
        })?;

        let key = (policy_name.to_string(), policy_type.to_string());
        let mut state = self.state.write();
        if state.account_policies.remove(&key).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Account policy {policy_name} of type {policy_type} not found"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Data Protection Policies ----

    fn put_data_protection_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
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

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.clone()
        };

        let now = Utc::now().timestamp_millis();
        let mut state = self.state.write();
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;
        let log_group_id_resp = group.arn.clone();

        group.data_protection_policy = Some(DataProtectionPolicy {
            policy_document: policy_document.clone(),
            last_updated_time: now,
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "logGroupIdentifier": log_group_id_resp,
                "policyDocument": policy_document,
                "lastUpdatedTime": now,
            }))
            .unwrap(),
        ))
    }

    fn get_data_protection_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.clone()
        };

        let state = self.state.read();
        let group = state.log_groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        let mut result = json!({
            "logGroupIdentifier": group.arn,
        });
        if let Some(ref dp) = group.data_protection_policy {
            result["policyDocument"] = json!(dp.policy_document);
            result["lastUpdatedTime"] = json!(dp.last_updated_time);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    fn delete_data_protection_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id
        };

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        if group.data_protection_policy.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "No data protection policy found for this log group",
            ));
        }

        group.data_protection_policy = None;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Index Policies ----

    fn put_index_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
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

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.clone()
        };

        let policy_name = body["policyName"].as_str().unwrap_or("default").to_string();

        let now = Utc::now().timestamp_millis();
        let mut state = self.state.write();
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        // Replace existing policy with same name, or add new one
        if let Some(existing) = group
            .index_policies
            .iter_mut()
            .find(|p| p.policy_name == policy_name)
        {
            existing.policy_document = policy_document.clone();
            existing.last_updated_time = now;
        } else {
            group.index_policies.push(IndexPolicy {
                policy_name: policy_name.clone(),
                policy_document: policy_document.clone(),
                last_updated_time: now,
            });
        }

        let result = json!({
            "indexPolicy": {
                "policyName": policy_name,
                "policyDocument": policy_document,
                "logGroupIdentifier": group.arn,
                "lastUpdateTime": now,
            }
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    fn describe_index_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        let log_group_ids = body["logGroupIdentifiers"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupIdentifiers is required",
            )
        })?;

        let state = self.state.read();
        let mut policies = Vec::new();

        for id_val in log_group_ids {
            let id = id_val.as_str().unwrap_or("");
            let group_name = if id.starts_with("arn:") {
                extract_log_group_from_arn(id).unwrap_or_default()
            } else {
                id.to_string()
            };
            if let Some(group) = state.log_groups.get(&group_name) {
                for p in &group.index_policies {
                    policies.push(json!({
                        "policyName": p.policy_name,
                        "policyDocument": p.policy_document,
                        "logGroupIdentifier": group.arn,
                        "lastUpdateTime": p.last_updated_time,
                    }));
                }
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "indexPolicies": policies })).unwrap(),
        ))
    }

    fn delete_index_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id
        };

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        if group.index_policies.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "No index policy found for this log group",
            ));
        }

        group.index_policies.clear();
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn describe_field_indexes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        // Validate that logGroupIdentifiers is provided
        let _log_group_ids = body["logGroupIdentifiers"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupIdentifiers is required",
            )
        })?;

        // Stub: return empty list
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "fieldIndexes": [] })).unwrap(),
        ))
    }

    // ---- Transformers ----

    fn put_transformer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();
        let transformer_config = body.get("transformerConfig").cloned().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "transformerConfig is required",
            )
        })?;

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.clone()
        };

        let now = Utc::now().timestamp_millis();
        let mut state = self.state.write();
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        group.transformer = Some(Transformer {
            transformer_config,
            creation_time: now,
            last_modified_time: now,
        });

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_transformer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.clone()
        };

        let state = self.state.read();
        let group = state.log_groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        let mut result = json!({
            "logGroupIdentifier": group.arn,
        });
        if let Some(ref t) = group.transformer {
            result["transformerConfig"] = t.transformer_config.clone();
            result["creationTime"] = json!(t.creation_time);
            result["lastModifiedTime"] = json!(t.last_modified_time);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    fn delete_transformer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id
        };

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        group.transformer = None;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn test_transformer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let _transformer_config = body.get("transformerConfig").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "transformerConfig is required",
            )
        })?;
        let log_event_messages = body["logEventMessages"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logEventMessages is required",
            )
        })?;

        // Stub: return the input events as transformed output unchanged
        let transformed: Vec<Value> = log_event_messages
            .iter()
            .map(|msg| {
                json!({
                    "eventMessage": msg,
                    "transformedEventMessage": msg,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "transformedLogs": transformed,
            }))
            .unwrap(),
        ))
    }

    // ---- Anomaly Detectors ----

    fn create_log_anomaly_detector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length("detectorName", body["detectorName"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "evaluationFrequency",
            &body["evaluationFrequency"],
            &[
                "ONE_MIN",
                "FIVE_MIN",
                "TEN_MIN",
                "FIFTEEN_MIN",
                "THIRTY_MIN",
                "ONE_HOUR",
            ],
        )?;
        validate_optional_string_length("filterPattern", body["filterPattern"].as_str(), 0, 1024)?;
        validate_optional_string_length("kmsKeyId", body["kmsKeyId"].as_str(), 0, 256)?;
        validate_optional_range_i64(
            "anomalyVisibilityTime",
            body["anomalyVisibilityTime"].as_i64(),
            7,
            90,
        )?;

        let log_group_arn_list = body["logGroupArnList"]
            .as_array()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupArnList is required",
                )
            })?
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>();

        let detector_name = body["detectorName"].as_str().unwrap_or("").to_string();
        let evaluation_frequency = body["evaluationFrequency"].as_str().map(|s| s.to_string());
        let filter_pattern = body["filterPattern"].as_str().map(|s| s.to_string());
        let anomaly_visibility_time = body["anomalyVisibilityTime"].as_i64();

        let now = Utc::now().timestamp_millis();
        let mut state = self.state.write();
        let detector_id = uuid::Uuid::new_v4().to_string();
        let arn = format!(
            "arn:aws:logs:{}:{}:anomaly-detector:{}",
            state.region, state.account_id, detector_id
        );

        let detector = AnomalyDetector {
            detector_name: detector_name.clone(),
            arn: arn.clone(),
            log_group_arn_list,
            evaluation_frequency,
            filter_pattern,
            anomaly_visibility_time,
            creation_time: now,
            last_modified_time: now,
            enabled: true,
        };

        state.anomaly_detectors.insert(arn.clone(), detector);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "anomalyDetectorArn": arn })).unwrap(),
        ))
    }

    fn get_log_anomaly_detector(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let arn = body["anomalyDetectorArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "anomalyDetectorArn is required",
            )
        })?;

        let state = self.state.read();
        let detector = state.anomaly_detectors.get(arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Anomaly detector not found: {arn}"),
            )
        })?;

        let mut result = json!({
            "anomalyDetectorArn": detector.arn,
            "detectorName": detector.detector_name,
            "logGroupArnList": detector.log_group_arn_list,
            "creationTimeStamp": detector.creation_time,
            "lastModifiedTimeStamp": detector.last_modified_time,
            "anomalyDetectorStatus": if detector.enabled { "TRAINING" } else { "PAUSED" },
        });
        if let Some(ref f) = detector.evaluation_frequency {
            result["evaluationFrequency"] = json!(f);
        }
        if let Some(ref f) = detector.filter_pattern {
            result["filterPattern"] = json!(f);
        }
        if let Some(t) = detector.anomaly_visibility_time {
            result["anomalyVisibilityTime"] = json!(t);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    fn delete_log_anomaly_detector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let arn = body["anomalyDetectorArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "anomalyDetectorArn is required",
            )
        })?;

        let mut state = self.state.write();
        if state.anomaly_detectors.remove(arn).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Anomaly detector not found: {arn}"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_log_anomaly_detectors(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length(
            "filterLogGroupArn",
            body["filterLogGroupArn"].as_str(),
            1,
            2048,
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        let filter_log_group_arn = body["filterLogGroupArn"].as_str();
        let _limit = body["limit"].as_i64().unwrap_or(50);

        let state = self.state.read();
        let detectors: Vec<Value> = state
            .anomaly_detectors
            .values()
            .filter(|d| {
                filter_log_group_arn.is_none_or(|arn| d.log_group_arn_list.iter().any(|a| a == arn))
            })
            .map(|d| {
                let mut obj = json!({
                    "anomalyDetectorArn": d.arn,
                    "detectorName": d.detector_name,
                    "logGroupArnList": d.log_group_arn_list,
                    "creationTimeStamp": d.creation_time,
                    "lastModifiedTimeStamp": d.last_modified_time,
                    "anomalyDetectorStatus": if d.enabled { "TRAINING" } else { "PAUSED" },
                });
                if let Some(ref f) = d.evaluation_frequency {
                    obj["evaluationFrequency"] = json!(f);
                }
                if let Some(ref f) = d.filter_pattern {
                    obj["filterPattern"] = json!(f);
                }
                if let Some(t) = d.anomaly_visibility_time {
                    obj["anomalyVisibilityTime"] = json!(t);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "anomalyDetectors": detectors })).unwrap(),
        ))
    }

    fn update_log_anomaly_detector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let arn = body["anomalyDetectorArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "anomalyDetectorArn is required",
            )
        })?;
        let enabled = body["enabled"].as_bool().unwrap_or(true);

        let mut state = self.state.write();
        let detector = state.anomaly_detectors.get_mut(arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Anomaly detector not found: {arn}"),
            )
        })?;

        detector.enabled = enabled;
        if let Some(f) = body["evaluationFrequency"].as_str() {
            detector.evaluation_frequency = Some(f.to_string());
        }
        if let Some(f) = body["filterPattern"].as_str() {
            detector.filter_pattern = Some(f.to_string());
        }
        if let Some(t) = body["anomalyVisibilityTime"].as_i64() {
            detector.anomaly_visibility_time = Some(t);
        }
        detector.last_modified_time = Utc::now().timestamp_millis();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Misc Operations ----

    fn get_log_group_fields(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupName"]
            .as_str()
            .or_else(|| body["logGroupIdentifier"].as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupName or logGroupIdentifier is required",
                )
            })?;

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.to_string()
        };

        let state = self.state.read();
        if !state.log_groups.contains_key(&group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            ));
        }

        // Stub response with common fields
        let fields = json!([
            { "fieldName": "@timestamp", "percent": 100 },
            { "fieldName": "@message", "percent": 100 },
        ]);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logGroupFields": fields })).unwrap(),
        ))
    }

    fn test_metric_filter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let filter_pattern = body["filterPattern"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "filterPattern is required",
            )
        })?;
        validate_string_length("filterPattern", filter_pattern, 0, 1024)?;
        let log_event_messages = body["logEventMessages"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logEventMessages is required",
            )
        })?;

        let matches: Vec<Value> = log_event_messages
            .iter()
            .enumerate()
            .filter(|(_, msg)| {
                let msg_str = msg.as_str().unwrap_or("");
                matches_filter_pattern(filter_pattern, msg_str)
            })
            .map(|(i, msg)| {
                json!({
                    "eventNumber": i + 1,
                    "eventMessage": msg,
                    "extractedValues": {},
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "matches": matches })).unwrap(),
        ))
    }

    fn stop_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let query_id = body["queryId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "queryId is required",
            )
        })?;

        let mut state = self.state.write();
        let query = state.queries.get_mut(query_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Query {query_id} is not in a cancellable state"),
            )
        })?;

        let was_running = query.status == "Running" || query.status == "Scheduled";
        if was_running {
            query.status = "Cancelled".to_string();
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "success": was_running })).unwrap(),
        ))
    }

    fn put_log_group_deletion_protection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();
        let deletion_protection = body["deletionProtectionEnabled"].as_bool().unwrap_or(true);

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id
        };

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        group.deletion_protection = deletion_protection;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_log_record(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn list_anomalies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length(
            "anomalyDetectorArn",
            body["anomalyDetectorArn"].as_str(),
            1,
            2048,
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        validate_optional_enum_value(
            "suppressionState",
            &body["suppressionState"],
            &["SUPPRESSED", "UNSUPPRESSED"],
        )?;
        // Stub: return empty anomalies list
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "anomalies": [] })).unwrap(),
        ))
    }

    fn update_anomaly(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("anomalyDetectorArn", &body["anomalyDetectorArn"])?;
        validate_optional_string_length(
            "anomalyDetectorArn",
            body["anomalyDetectorArn"].as_str(),
            1,
            2048,
        )?;
        validate_optional_string_length("anomalyId", body["anomalyId"].as_str(), 36, 36)?;
        validate_optional_string_length("patternId", body["patternId"].as_str(), 32, 32)?;
        validate_optional_enum_value(
            "suppressionType",
            &body["suppressionType"],
            &["LIMITED", "INFINITE"],
        )?;
        // No-op stub
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // -- Import tasks --

    fn create_import_task(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let import_source_arn = require_str(&body, "importSourceArn")?;
        let import_role_arn = require_str(&body, "importRoleArn")?;
        validate_string_length("importRoleArn", import_role_arn, 1, 2048)?;
        let log_group_name = body["logGroupName"].as_str().map(|s| s.to_string());

        let import_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().timestamp_millis();

        let task = ImportTask {
            import_id: import_id.clone(),
            import_source_arn: import_source_arn.to_string(),
            import_role_arn: import_role_arn.to_string(),
            log_group_name,
            status: "RUNNING".to_string(),
            creation_time: now,
        };

        let mut state = self.state.write();
        state.import_tasks.insert(import_id.clone(), task);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "importId": import_id })).unwrap(),
        ))
    }

    fn describe_import_tasks(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length("importId", body["importId"].as_str(), 1, 256)?;
        validate_optional_enum_value(
            "importStatus",
            &body["importStatus"],
            &["IN_PROGRESS", "CANCELLED", "COMPLETED", "FAILED"],
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;

        let state = self.state.read();
        let tasks: Vec<Value> = state
            .import_tasks
            .values()
            .map(|t| {
                json!({
                    "importId": t.import_id,
                    "importSourceArn": t.import_source_arn,
                    "importStatus": t.status,
                    "creationTime": t.creation_time,
                })
            })
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "imports": tasks })).unwrap(),
        ))
    }

    fn describe_import_task_batches(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let import_id = require_str(&body, "importId")?;
        validate_string_length("importId", import_id, 1, 256)?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        // Stub: return empty batches
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "importBatches": [] })).unwrap(),
        ))
    }

    fn cancel_import_task(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let import_id = require_str(&body, "importId")?;

        let mut state = self.state.write();
        match state.import_tasks.get_mut(import_id) {
            Some(task) => {
                task.status = "CANCELLED".to_string();
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Import task not found: {import_id}"),
            )),
        }
    }

    // -- Integrations --

    fn put_integration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("resourceConfig", &body["resourceConfig"])?;
        let integration_name = require_str(&body, "integrationName")?;
        validate_string_length("integrationName", integration_name, 1, 50)?;
        let integration_type = require_str(&body, "integrationType")?;
        validate_enum("integrationType", integration_type, &["OPENSEARCH"])?;
        let resource_config = body["resourceConfig"].clone();

        let now = Utc::now().timestamp_millis();
        let integration = Integration {
            integration_name: integration_name.to_string(),
            integration_type: integration_type.to_string(),
            resource_config,
            status: "ACTIVE".to_string(),
            creation_time: now,
        };

        let mut state = self.state.write();
        state
            .integrations
            .insert(integration_name.to_string(), integration);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "integrationName": integration_name,
                "integrationStatus": "ACTIVE"
            }))
            .unwrap(),
        ))
    }

    fn get_integration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let integration_name = require_str(&body, "integrationName")?;

        let state = self.state.read();
        match state.integrations.get(integration_name) {
            Some(i) => Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({
                    "integrationName": i.integration_name,
                    "integrationType": i.integration_type,
                    "integrationStatus": i.status,
                }))
                .unwrap(),
            )),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Integration not found: {integration_name}"),
            )),
        }
    }

    fn delete_integration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let integration_name = require_str(&body, "integrationName")?;
        validate_string_length("integrationName", integration_name, 1, 50)?;

        let mut state = self.state.write();
        state.integrations.remove(integration_name);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_integrations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length(
            "integrationNamePrefix",
            body["integrationNamePrefix"].as_str(),
            1,
            50,
        )?;
        validate_optional_enum_value("integrationType", &body["integrationType"], &["OPENSEARCH"])?;
        validate_optional_enum_value(
            "integrationStatus",
            &body["integrationStatus"],
            &["PROVISIONING", "ACTIVE", "FAILED"],
        )?;

        let state = self.state.read();
        let integrations: Vec<Value> = state
            .integrations
            .values()
            .map(|i| {
                json!({
                    "integrationName": i.integration_name,
                    "integrationType": i.integration_type,
                    "integrationStatus": i.status,
                })
            })
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "integrationSummaries": integrations })).unwrap(),
        ))
    }

    // -- Lookup tables --

    fn create_lookup_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let lookup_table_name = require_str(&body, "lookupTableName")?;
        validate_string_length("lookupTableName", lookup_table_name, 1, 256)?;
        let table_body = require_str(&body, "tableBody")?;
        validate_string_length("tableBody", table_body, 1, 10485760)?;
        validate_optional_string_length("description", body["description"].as_str(), 0, 1024)?;
        validate_optional_string_length("kmsKeyId", body["kmsKeyId"].as_str(), 0, 256)?;

        let state_r = self.state.read();
        let account_id = state_r.account_id.clone();
        let region = state_r.region.clone();
        drop(state_r);

        let arn = format!("arn:aws:logs:{region}:{account_id}:lookup-table:{lookup_table_name}");
        let now = Utc::now().timestamp_millis();

        let table = LookupTable {
            lookup_table_name: lookup_table_name.to_string(),
            arn: arn.clone(),
            table_body: table_body.to_string(),
            creation_time: now,
            last_modified_time: now,
        };

        let mut state = self.state.write();
        state.lookup_tables.insert(arn.clone(), table);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "lookupTableArn": arn })).unwrap(),
        ))
    }

    fn get_lookup_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let lookup_table_arn = require_str(&body, "lookupTableArn")?;

        let state = self.state.read();
        match state.lookup_tables.get(lookup_table_arn) {
            Some(t) => Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({
                    "lookupTableName": t.lookup_table_name,
                    "lookupTableArn": t.arn,
                    "tableBody": t.table_body,
                    "creationTime": t.creation_time,
                    "lastModifiedTime": t.last_modified_time,
                }))
                .unwrap(),
            )),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Lookup table not found: {lookup_table_arn}"),
            )),
        }
    }

    fn describe_lookup_tables(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length(
            "lookupTableNamePrefix",
            body["lookupTableNamePrefix"].as_str(),
            1,
            256,
        )?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;

        let state = self.state.read();
        let tables: Vec<Value> = state
            .lookup_tables
            .values()
            .map(|t| {
                json!({
                    "lookupTableName": t.lookup_table_name,
                    "lookupTableArn": t.arn,
                })
            })
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "lookupTables": tables })).unwrap(),
        ))
    }

    fn delete_lookup_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let lookup_table_arn = require_str(&body, "lookupTableArn")?;

        let mut state = self.state.write();
        state.lookup_tables.remove(lookup_table_arn);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn update_lookup_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let lookup_table_arn = require_str(&body, "lookupTableArn")?;
        let table_body = require_str(&body, "tableBody")?;

        let mut state = self.state.write();
        match state.lookup_tables.get_mut(lookup_table_arn) {
            Some(t) => {
                t.table_body = table_body.to_string();
                t.last_modified_time = Utc::now().timestamp_millis();
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Lookup table not found: {lookup_table_arn}"),
            )),
        }
    }

    // -- Scheduled queries --

    fn create_scheduled_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = require_str(&body, "name")?;
        validate_string_length("name", name, 1, 255)?;
        validate_optional_string_length("description", body["description"].as_str(), 0, 1024)?;
        let query_string = require_str(&body, "queryString")?;
        validate_string_length("queryString", query_string, 0, 10000)?;
        let query_language = require_str(&body, "queryLanguage")?;
        validate_enum("queryLanguage", query_language, &["CWLI", "SQL", "PPL"])?;
        let schedule_expression = require_str(&body, "scheduleExpression")?;
        validate_string_length("scheduleExpression", schedule_expression, 0, 256)?;
        let execution_role_arn = require_str(&body, "executionRoleArn")?;
        validate_string_length("executionRoleArn", execution_role_arn, 1, 2048)?;
        validate_optional_string_length("timezone", body["timezone"].as_str(), 1, 2048)?;
        validate_optional_range_i64(
            "scheduleStartTime",
            body["scheduleStartTime"].as_i64(),
            0,
            i64::MAX,
        )?;
        validate_optional_range_i64(
            "scheduleEndTime",
            body["scheduleEndTime"].as_i64(),
            0,
            i64::MAX,
        )?;
        validate_optional_enum_value("state", &body["state"], &["ENABLED", "DISABLED"])?;

        let state_r = self.state.read();
        let account_id = state_r.account_id.clone();
        let region = state_r.region.clone();
        drop(state_r);

        let arn = format!("arn:aws:logs:{region}:{account_id}:scheduled-query:{name}");
        let now = Utc::now().timestamp_millis();

        let sq = ScheduledQuery {
            name: name.to_string(),
            arn: arn.clone(),
            query_string: query_string.to_string(),
            query_language: query_language.to_string(),
            schedule_expression: schedule_expression.to_string(),
            execution_role_arn: execution_role_arn.to_string(),
            status: "ACTIVE".to_string(),
            creation_time: now,
            last_modified_time: now,
        };

        let mut state = self.state.write();
        state.scheduled_queries.insert(arn.clone(), sq);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "scheduledQueryArn": arn })).unwrap(),
        ))
    }

    fn get_scheduled_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let identifier = require_str(&body, "identifier")?;

        let state = self.state.read();
        match state.scheduled_queries.get(identifier) {
            Some(sq) => Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({
                    "scheduledQueryArn": sq.arn,
                    "name": sq.name,
                    "queryString": sq.query_string,
                    "queryLanguage": sq.query_language,
                    "scheduleExpression": sq.schedule_expression,
                    "executionRoleArn": sq.execution_role_arn,
                }))
                .unwrap(),
            )),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Scheduled query not found: {identifier}"),
            )),
        }
    }

    fn get_scheduled_query_history(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let _identifier = require_str(&body, "identifier")?;
        validate_required("startTime", &body["startTime"])?;
        validate_required("endTime", &body["endTime"])?;
        validate_optional_range_i64("startTime", body["startTime"].as_i64(), 0, i64::MAX)?;
        validate_optional_range_i64("endTime", body["endTime"].as_i64(), 0, i64::MAX)?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        // Stub: return empty history
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "triggerHistory": [] })).unwrap(),
        ))
    }

    fn list_scheduled_queries(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        validate_optional_enum_value("state", &body["state"], &["ENABLED", "DISABLED"])?;

        let state = self.state.read();
        let queries: Vec<Value> = state
            .scheduled_queries
            .values()
            .map(|sq| {
                json!({
                    "name": sq.name,
                    "scheduledQueryArn": sq.arn,
                })
            })
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "scheduledQueries": queries })).unwrap(),
        ))
    }

    fn delete_scheduled_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let identifier = require_str(&body, "identifier")?;

        let mut state = self.state.write();
        state.scheduled_queries.remove(identifier);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn update_scheduled_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let identifier = require_str(&body, "identifier")?;
        let query_string = require_str(&body, "queryString")?;
        let query_language = require_str(&body, "queryLanguage")?;
        let schedule_expression = require_str(&body, "scheduleExpression")?;
        let execution_role_arn = require_str(&body, "executionRoleArn")?;

        let mut state = self.state.write();
        match state.scheduled_queries.get_mut(identifier) {
            Some(sq) => {
                sq.query_string = query_string.to_string();
                sq.query_language = query_language.to_string();
                sq.schedule_expression = schedule_expression.to_string();
                sq.execution_role_arn = execution_role_arn.to_string();
                sq.last_modified_time = Utc::now().timestamp_millis();
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Scheduled query not found: {identifier}"),
            )),
        }
    }

    // -- Misc stubs --

    fn start_live_tail(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("logGroupIdentifiers", &body["logGroupIdentifiers"])?;
        validate_optional_string_length(
            "logEventFilterPattern",
            body["logEventFilterPattern"].as_str(),
            0,
            1024,
        )?;
        let session_id = uuid::Uuid::new_v4().to_string();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "responseStream": {
                    "sessionStart": {
                        "sessionId": session_id,
                        "logGroupIdentifiers": [],
                    }
                }
            }))
            .unwrap(),
        ))
    }

    fn list_log_groups_for_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let query_id = require_str(&body, "queryId")?;
        validate_string_length("queryId", query_id, 1, 256)?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 50, 500)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        // Stub: return empty log group names
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logGroupIdentifiers": [] })).unwrap(),
        ))
    }

    fn list_aggregate_log_group_summaries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("groupBy", &body["groupBy"])?;
        validate_optional_enum_value(
            "groupBy",
            &body["groupBy"],
            &[
                "DATA_SOURCE_NAME_TYPE_AND_FORMAT",
                "DATA_SOURCE_NAME_AND_TYPE",
            ],
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_enum_value(
            "logGroupClass",
            &body["logGroupClass"],
            &["STANDARD", "INFREQUENT_ACCESS", "DELIVERY"],
        )?;
        validate_optional_string_length(
            "logGroupNamePattern",
            body["logGroupNamePattern"].as_str(),
            3,
            129,
        )?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        // Stub: return empty summaries
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "aggregateLogGroupSummaries": [] })).unwrap(),
        ))
    }

    fn list_log_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let prefix = body["logGroupNamePrefix"].as_str().unwrap_or("");
        let pattern = body["logGroupNamePattern"].as_str().unwrap_or("");
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
            3,
            129,
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        validate_optional_enum_value(
            "logGroupClass",
            &body["logGroupClass"],
            &["STANDARD", "INFREQUENT_ACCESS", "DELIVERY"],
        )?;

        let state = self.state.read();
        let mut groups: Vec<&LogGroup> = state
            .log_groups
            .values()
            .filter(|g| {
                (prefix.is_empty() || g.name.starts_with(prefix))
                    && (pattern.is_empty() || g.name.contains(pattern))
            })
            .collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));

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

        // ListLogGroups returns LogGroupSummary (logGroupName, logGroupArn, logGroupClass only)
        let log_groups: Vec<Value> = page
            .iter()
            .map(|g| {
                let log_group_arn = g.arn.trim_end_matches(":*").to_string();
                json!({
                    "logGroupName": g.name,
                    "logGroupArn": log_group_arn,
                    "logGroupClass": "STANDARD",
                })
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

    fn put_bearer_token_authentication(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required(
            "bearerTokenAuthenticationEnabled",
            &body["bearerTokenAuthenticationEnabled"],
        )?;
        let log_group_identifier = require_str(&body, "logGroupIdentifier")?;
        validate_string_length("logGroupIdentifier", log_group_identifier, 1, 2048)?;
        let enabled = body["bearerTokenAuthenticationEnabled"]
            .as_bool()
            .unwrap_or(false);

        let mut state = self.state.write();
        state
            .bearer_token_auth
            .insert(log_group_identifier.to_string(), enabled);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_log_object(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("logObjectPointer", &body["logObjectPointer"])?;
        validate_optional_string_length(
            "logObjectPointer",
            body["logObjectPointer"].as_str(),
            1,
            512,
        )?;
        // Stub: return empty (fieldStream is streaming, represented as empty object)
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_log_fields(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("dataSourceName", &body["dataSourceName"])?;
        validate_required("dataSourceType", &body["dataSourceType"])?;
        // Stub: return empty log fields
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logFields": [] })).unwrap(),
        ))
    }

    fn associate_source_to_s3_table_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("dataSource", &body["dataSource"])?;
        let integration_arn = require_str(&body, "integrationArn")?;
        let data_source = body["dataSource"].clone();
        let source_id = data_source
            .as_object()
            .and_then(|o| o.get("resourceArn"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let mut state = self.state.write();
        state
            .s3_table_sources
            .entry(integration_arn.to_string())
            .or_default()
            .push(source_id);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_sources_for_s3_table_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let integration_arn = require_str(&body, "integrationArn")?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;

        let state = self.state.read();
        let sources: Vec<Value> = state
            .s3_table_sources
            .get(integration_arn)
            .map(|sources| {
                sources
                    .iter()
                    .map(|s| {
                        json!({
                            "identifier": s,
                            "status": "ACTIVE",
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "sources": sources })).unwrap(),
        ))
    }

    fn disassociate_source_from_s3_table_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let identifier = require_str(&body, "identifier")?;
        validate_string_length("identifier", identifier, 1, 2048)?;
        // No-op stub (we don't track detailed enough to remove specific sources)
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn update_delivery_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let id = require_str(&body, "id")?;

        let state = self.state.read();
        if !state.deliveries.contains_key(id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Delivery not found: {id}"),
            ));
        }
        drop(state);

        // No-op: delivery configuration update is accepted but not stored
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn describe_configuration_templates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length("service", body["service"].as_str(), 1, 255)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        // Stub: return empty configuration templates
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "configurationTemplates": [] })).unwrap(),
        ))
    }
}

/// Resolve log group name from either logGroupName or resourceIdentifier.
/// resourceIdentifier can be a log group name or an ARN.
fn resolve_log_group_name(
    log_group_name: Option<&str>,
    resource_identifier: Option<&str>,
) -> Result<String, AwsServiceError> {
    if let Some(identifier) = resource_identifier {
        if identifier.starts_with("arn:") {
            extract_log_group_from_arn(identifier).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {identifier}"),
                )
            })
        } else {
            Ok(identifier.to_string())
        }
    } else if let Some(name) = log_group_name {
        Ok(name.to_string())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            "Either logGroupName or resourceIdentifier is required",
        ))
    }
}

/// Extract log group name from ARN like "arn:aws:logs:region:account:log-group:name:*"
fn extract_log_group_from_arn(arn: &str) -> Option<String> {
    // arn:aws:logs:region:account:log-group:name:*
    let parts: Vec<&str> = arn.splitn(7, ':').collect();
    if parts.len() >= 7 && parts[5] == "log-group" {
        let name = parts[6].strip_suffix(":*").unwrap_or(parts[6]);
        Some(name.to_string())
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

    // JSON/metric filter patterns: { $.field = "value" }
    if pattern.starts_with('{') && pattern.ends_with('}') {
        return matches_json_filter_pattern(pattern, message);
    }

    // Array-style metric filter patterns - match everything (not implemented)
    if pattern.starts_with('[') {
        return true;
    }

    // Quoted pattern: exact substring match (handles escaped inner quotes)
    if pattern.starts_with('"') && pattern.ends_with('"') && pattern.len() >= 2 {
        let inner = &pattern[1..pattern.len() - 1];
        // Unescape inner quotes: \"  ->  "
        let unescaped = inner.replace("\\\"", "\"");
        return message.contains(&unescaped);
    }

    // Multiple words: all must be present (AND semantics)
    let terms = parse_filter_terms(pattern);
    terms.iter().all(|term| message.contains(term.as_str()))
}

/// Parse filter pattern terms, respecting quoted strings as single terms.
fn parse_filter_terms(pattern: &str) -> Vec<String> {
    let mut terms = Vec::new();
    let mut chars = pattern.chars().peekable();

    while chars.peek().is_some() {
        // Skip whitespace
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }

        if chars.peek().is_none() {
            break;
        }

        if chars.peek() == Some(&'"') {
            // Quoted term
            chars.next(); // consume opening quote
            let mut term = String::new();
            loop {
                match chars.next() {
                    Some('\\') => {
                        if let Some(c) = chars.next() {
                            term.push(c);
                        }
                    }
                    Some('"') => break,
                    Some(c) => term.push(c),
                    None => break,
                }
            }
            terms.push(term);
        } else {
            // Unquoted term
            let mut term = String::new();
            while chars.peek().is_some_and(|c| !c.is_whitespace()) {
                term.push(chars.next().unwrap());
            }
            if !term.is_empty() {
                terms.push(term);
            }
        }
    }

    terms
}

/// Match a JSON filter pattern like `{ $.level = "ERROR" }` against a message.
fn matches_json_filter_pattern(pattern: &str, message: &str) -> bool {
    // Strip the outer braces
    let inner = pattern
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or("")
        .trim();

    if inner.is_empty() {
        return true;
    }

    // Parse the message as JSON
    let msg_json: serde_json::Value = match serde_json::from_str(message) {
        Ok(v) => v,
        Err(_) => return false, // Non-JSON message cannot match JSON filter
    };

    // Support: $.field = "value", $.field != "value", $.field = number,
    //          $.field > number, $.field < number, $.field >= number, $.field <= number
    // Also support && for multiple conditions
    let conditions: Vec<&str> = inner.split("&&").collect();

    for condition in conditions {
        let condition = condition.trim();
        if !matches_single_json_condition(condition, &msg_json) {
            return false;
        }
    }

    true
}

fn matches_single_json_condition(condition: &str, json: &serde_json::Value) -> bool {
    // Try to parse: $.field op value
    let condition = condition.trim();

    // Find the operator
    let ops = ["!=", ">=", "<=", "=", ">", "<"];
    let mut found_op = None;
    let mut op_pos = 0;
    let mut op_len = 0;

    for op in &ops {
        if let Some(pos) = condition.find(op) {
            // Make sure we're not inside a quoted string
            let before = &condition[..pos];
            let quote_count = before.chars().filter(|&c| c == '"').count();
            if quote_count % 2 == 0 {
                found_op = Some(*op);
                op_pos = pos;
                op_len = op.len();
                break;
            }
        }
    }

    let (op, field_part, value_part) = match found_op {
        Some(op) => (
            op,
            condition[..op_pos].trim(),
            condition[op_pos + op_len..].trim(),
        ),
        None => {
            // No operator: just check if the field exists
            // Pattern like `{ $.field }` means field exists
            if let Some(path) = condition.strip_prefix("$.") {
                return resolve_json_path_simple(json, path).is_some();
            }
            return true;
        }
    };

    // Extract JSON path from field_part (must start with $.)
    let path = match field_part.strip_prefix("$.") {
        Some(p) => p,
        None => return true, // Don't understand this pattern, match everything
    };

    let actual_value = match resolve_json_path_simple(json, path) {
        Some(v) => v,
        None => return op == "!=", // field doesn't exist: only != matches
    };

    // Parse the expected value
    let expected_str = if value_part.starts_with('"') && value_part.ends_with('"') {
        // String comparison
        let s = &value_part[1..value_part.len() - 1];
        match op {
            "=" => actual_value.as_str() == Some(s),
            "!=" => actual_value.as_str() != Some(s),
            _ => false,
        }
    } else if let Ok(expected_num) = value_part.parse::<f64>() {
        // Numeric comparison
        let actual_num = actual_value.as_f64();
        match (op, actual_num) {
            ("=", Some(n)) => (n - expected_num).abs() < f64::EPSILON,
            ("!=", Some(n)) => (n - expected_num).abs() >= f64::EPSILON,
            (">", Some(n)) => n > expected_num,
            ("<", Some(n)) => n < expected_num,
            (">=", Some(n)) => n >= expected_num,
            ("<=", Some(n)) => n <= expected_num,
            _ => false,
        }
    } else if value_part == "true" || value_part == "false" {
        let expected_bool = value_part == "true";
        match op {
            "=" => actual_value.as_bool() == Some(expected_bool),
            "!=" => actual_value.as_bool() != Some(expected_bool),
            _ => false,
        }
    } else {
        true // Unknown value format, match everything
    };

    expected_str
}

/// Resolve a simple dot-separated JSON path (e.g., "level" or "nested.field").
fn resolve_json_path_simple<'a>(
    json: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    let mut current = json;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    if current.is_null() {
        None
    } else {
        Some(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::LogsState;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_service() -> LogsService {
        let state = Arc::new(parking_lot::RwLock::new(LogsState::new(
            "123456789012",
            "us-east-1",
        )));
        let delivery_bus = Arc::new(DeliveryBus::new());
        LogsService::new(state, delivery_bus)
    }

    fn make_request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "logs".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            path_segments: vec![],
            raw_path: "/".to_string(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    fn create_group(svc: &LogsService, name: &str) {
        let req = make_request("CreateLogGroup", json!({ "logGroupName": name }));
        svc.create_log_group(&req).unwrap();
    }

    fn create_stream(svc: &LogsService, group: &str, stream: &str) {
        let req = make_request(
            "CreateLogStream",
            json!({ "logGroupName": group, "logStreamName": stream }),
        );
        svc.create_log_stream(&req).unwrap();
    }

    fn put_events(svc: &LogsService, group: &str, stream: &str, messages: &[&str]) {
        let now = chrono::Utc::now().timestamp_millis();
        let events: Vec<Value> = messages
            .iter()
            .enumerate()
            .map(|(i, msg)| json!({ "timestamp": now + i as i64, "message": msg }))
            .collect();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": group,
                "logStreamName": stream,
                "logEvents": events,
            }),
        );
        svc.put_log_events(&req).unwrap();
    }

    // ---- describe_log_groups: logGroupNamePattern ----

    #[test]
    fn describe_log_groups_pattern_filters_by_substring() {
        let svc = make_service();
        create_group(&svc, "/app/web");
        create_group(&svc, "/app/api");
        create_group(&svc, "/system/metrics");

        let req = make_request("DescribeLogGroups", json!({ "logGroupNamePattern": "app" }));
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let names: Vec<&str> = body["logGroups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g["logGroupName"].as_str().unwrap())
            .collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"/app/web"));
        assert!(names.contains(&"/app/api"));
    }

    #[test]
    fn describe_log_groups_pattern_empty_returns_all() {
        let svc = make_service();
        create_group(&svc, "/app/web");
        create_group(&svc, "/system/metrics");

        let req = make_request("DescribeLogGroups", json!({}));
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["logGroups"].as_array().unwrap().len(), 2);
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

    // ---- create_export_task: taskName + logStreamNamePrefix stored ----

    #[test]
    fn create_export_task_stores_task_name_and_stream_prefix() {
        let svc = make_service();
        create_group(&svc, "grp");

        let req = make_request(
            "CreateExportTask",
            json!({
                "logGroupName": "grp",
                "from": 0,
                "to": 1000,
                "destination": "my-bucket",
                "taskName": "my-export",
                "logStreamNamePrefix": "web-",
            }),
        );
        let resp = svc.create_export_task(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let task_id = body["taskId"].as_str().unwrap();

        let req = make_request("DescribeExportTasks", json!({ "taskId": task_id }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let task = &body["exportTasks"][0];
        assert_eq!(task["taskName"].as_str().unwrap(), "my-export");
        assert_eq!(task["logStreamNamePrefix"].as_str().unwrap(), "web-");
    }

    #[test]
    fn create_export_task_omits_optional_fields_when_not_provided() {
        let svc = make_service();
        create_group(&svc, "grp");

        let req = make_request(
            "CreateExportTask",
            json!({
                "logGroupName": "grp",
                "from": 0,
                "to": 1000,
                "destination": "my-bucket",
            }),
        );
        let resp = svc.create_export_task(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let task_id = body["taskId"].as_str().unwrap();

        let req = make_request("DescribeExportTasks", json!({ "taskId": task_id }));
        let resp = svc.describe_export_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let task = &body["exportTasks"][0];
        assert!(task.get("taskName").is_none() || task["taskName"].is_null());
        assert!(task.get("logStreamNamePrefix").is_none() || task["logStreamNamePrefix"].is_null());
    }

    // ---- associate_kms_key / disassociate_kms_key: resourceIdentifier ----

    #[test]
    fn associate_kms_key_via_resource_identifier_arn() {
        let svc = make_service();
        create_group(&svc, "grp");

        let req = make_request(
            "AssociateKmsKey",
            json!({
                "resourceIdentifier": "arn:aws:logs:us-east-1:123456789012:log-group:grp:*",
                "kmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/abc-123",
            }),
        );
        svc.associate_kms_key(&req).unwrap();

        let state = svc.state.read();
        assert_eq!(
            state.log_groups["grp"].kms_key_id.as_deref(),
            Some("arn:aws:kms:us-east-1:123456789012:key/abc-123")
        );
    }

    #[test]
    fn disassociate_kms_key_via_resource_identifier_name() {
        let svc = make_service();
        create_group(&svc, "grp");

        // First associate
        let req = make_request(
            "AssociateKmsKey",
            json!({ "logGroupName": "grp", "kmsKeyId": "some-key" }),
        );
        svc.associate_kms_key(&req).unwrap();

        // Disassociate via resourceIdentifier (plain name)
        let req = make_request("DisassociateKmsKey", json!({ "resourceIdentifier": "grp" }));
        svc.disassociate_kms_key(&req).unwrap();

        let state = svc.state.read();
        assert!(state.log_groups["grp"].kms_key_id.is_none());
    }

    // ---- describe_query_definitions: queryDefinitionNamePrefix ----

    #[test]
    fn describe_query_definitions_filters_by_name_prefix() {
        let svc = make_service();

        // Create some query definitions
        for name in &["error-queries-1", "error-queries-2", "latency-queries-1"] {
            let req = make_request(
                "PutQueryDefinition",
                json!({
                    "name": name,
                    "queryString": "fields @timestamp | limit 20",
                }),
            );
            svc.put_query_definition(&req).unwrap();
        }

        let req = make_request(
            "DescribeQueryDefinitions",
            json!({ "queryDefinitionNamePrefix": "error" }),
        );
        let resp = svc.describe_query_definitions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let defs = body["queryDefinitions"].as_array().unwrap();
        assert_eq!(defs.len(), 2);
        for d in defs {
            assert!(d["name"].as_str().unwrap().starts_with("error"));
        }
    }

    #[test]
    fn describe_query_definitions_no_prefix_returns_all() {
        let svc = make_service();

        for name in &["a", "b", "c"] {
            let req = make_request(
                "PutQueryDefinition",
                json!({ "name": name, "queryString": "fields @timestamp" }),
            );
            svc.put_query_definition(&req).unwrap();
        }

        let req = make_request("DescribeQueryDefinitions", json!({}));
        let resp = svc.describe_query_definitions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["queryDefinitions"].as_array().unwrap().len(), 3);
    }

    // ---- extract_log_group_from_arn ----

    #[test]
    fn put_delivery_destination_includes_empty_destination_resource_arn() {
        let svc = make_service();
        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "my-dest",
                "deliveryDestinationConfiguration": {}
            }),
        );
        let resp = svc.put_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let config = &body["deliveryDestination"]["deliveryDestinationConfiguration"];
        // destinationResourceArn should always be present as a string (Smithy requirement)
        assert_eq!(
            config["destinationResourceArn"].as_str().unwrap(),
            "",
            "destinationResourceArn should be an empty string when not set"
        );
    }

    #[test]
    fn put_delivery_destination_includes_destination_resource_arn_when_set() {
        let svc = make_service();
        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "my-dest",
                "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::my-bucket"
                }
            }),
        );
        let resp = svc.put_delivery_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let config = &body["deliveryDestination"]["deliveryDestinationConfiguration"];
        assert_eq!(
            config["destinationResourceArn"].as_str().unwrap(),
            "arn:aws:s3:::my-bucket"
        );
    }

    #[test]
    fn extract_log_group_from_arn_strips_wildcard_suffix() {
        let arn = "arn:aws:logs:us-east-1:123456789012:log-group:my-group:*";
        assert_eq!(
            extract_log_group_from_arn(arn),
            Some("my-group".to_string())
        );
    }

    #[test]
    fn extract_log_group_from_arn_without_wildcard() {
        let arn = "arn:aws:logs:us-east-1:123456789012:log-group:my-group";
        assert_eq!(
            extract_log_group_from_arn(arn),
            Some("my-group".to_string())
        );
    }

    #[test]
    fn extract_log_group_from_arn_invalid() {
        assert_eq!(extract_log_group_from_arn("not-an-arn"), None);
    }

    // ---- Account policies ----

    #[test]
    fn account_policy_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "PutAccountPolicy",
            json!({
                "policyName": "test-policy",
                "policyType": "DATA_PROTECTION_POLICY",
                "policyDocument": "{\"Name\":\"test\"}",
            }),
        );
        let resp = svc.put_account_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["accountPolicy"]["policyName"], "test-policy");

        let req = make_request(
            "DescribeAccountPolicies",
            json!({ "policyType": "DATA_PROTECTION_POLICY" }),
        );
        let resp = svc.describe_account_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["accountPolicies"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DeleteAccountPolicy",
            json!({
                "policyName": "test-policy",
                "policyType": "DATA_PROTECTION_POLICY",
            }),
        );
        svc.delete_account_policy(&req).unwrap();

        let req = make_request(
            "DescribeAccountPolicies",
            json!({ "policyType": "DATA_PROTECTION_POLICY" }),
        );
        let resp = svc.describe_account_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["accountPolicies"].as_array().unwrap().is_empty());
    }

    // ---- Data protection policy ----

    #[test]
    fn data_protection_policy_lifecycle() {
        let svc = make_service();
        create_group(&svc, "dp-group");

        let req = make_request(
            "PutDataProtectionPolicy",
            json!({
                "logGroupIdentifier": "dp-group",
                "policyDocument": "{\"Name\":\"dp\"}",
            }),
        );
        svc.put_data_protection_policy(&req).unwrap();

        let req = make_request(
            "GetDataProtectionPolicy",
            json!({ "logGroupIdentifier": "dp-group" }),
        );
        let resp = svc.get_data_protection_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["policyDocument"], "{\"Name\":\"dp\"}");

        let req = make_request(
            "DeleteDataProtectionPolicy",
            json!({ "logGroupIdentifier": "dp-group" }),
        );
        svc.delete_data_protection_policy(&req).unwrap();

        let req = make_request(
            "GetDataProtectionPolicy",
            json!({ "logGroupIdentifier": "dp-group" }),
        );
        let resp = svc.get_data_protection_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body.get("policyDocument").is_none());
    }

    // ---- Index policies ----

    #[test]
    fn index_policy_lifecycle() {
        let svc = make_service();
        create_group(&svc, "idx-group");

        let req = make_request(
            "PutIndexPolicy",
            json!({
                "logGroupIdentifier": "idx-group",
                "policyDocument": "{\"Fields\":[\"field1\"]}",
            }),
        );
        svc.put_index_policy(&req).unwrap();

        let req = make_request(
            "DescribeIndexPolicies",
            json!({ "logGroupIdentifiers": ["idx-group"] }),
        );
        let resp = svc.describe_index_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["indexPolicies"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DeleteIndexPolicy",
            json!({
                "logGroupIdentifier": "idx-group",
            }),
        );
        svc.delete_index_policy(&req).unwrap();
    }

    // ---- Transformers ----

    #[test]
    fn transformer_lifecycle() {
        let svc = make_service();
        create_group(&svc, "tx-group");

        let req = make_request(
            "PutTransformer",
            json!({
                "logGroupIdentifier": "tx-group",
                "transformerConfig": [{"addKeys":{"entries":[{"key":"new","value":"val"}]}}],
            }),
        );
        svc.put_transformer(&req).unwrap();

        let req = make_request(
            "GetTransformer",
            json!({ "logGroupIdentifier": "tx-group" }),
        );
        let resp = svc.get_transformer(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["transformerConfig"].is_array());

        let req = make_request(
            "DeleteTransformer",
            json!({ "logGroupIdentifier": "tx-group" }),
        );
        svc.delete_transformer(&req).unwrap();
    }

    #[test]
    fn test_transformer_returns_transformed_events() {
        let svc = make_service();

        let req = make_request(
            "TestTransformer",
            json!({
                "transformerConfig": [{"addKeys":{"entries":[]}}],
                "logEventMessages": ["hello", "world"],
            }),
        );
        let resp = svc.test_transformer(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["transformedLogs"].as_array().unwrap().len(), 2);
    }

    // ---- Anomaly detectors ----

    #[test]
    fn anomaly_detector_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "CreateLogAnomalyDetector",
            json!({
                "logGroupArnList": ["arn:aws:logs:us-east-1:123456789012:log-group:test:*"],
                "detectorName": "my-detector",
            }),
        );
        let resp = svc.create_log_anomaly_detector(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let arn = body["anomalyDetectorArn"].as_str().unwrap().to_string();

        let req = make_request(
            "GetLogAnomalyDetector",
            json!({ "anomalyDetectorArn": &arn }),
        );
        let resp = svc.get_log_anomaly_detector(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["detectorName"], "my-detector");

        let req = make_request("ListLogAnomalyDetectors", json!({}));
        let resp = svc.list_log_anomaly_detectors(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["anomalyDetectors"].as_array().unwrap().len(), 1);

        let req = make_request(
            "UpdateLogAnomalyDetector",
            json!({ "anomalyDetectorArn": &arn, "enabled": false }),
        );
        svc.update_log_anomaly_detector(&req).unwrap();

        let req = make_request(
            "DeleteLogAnomalyDetector",
            json!({ "anomalyDetectorArn": &arn }),
        );
        svc.delete_log_anomaly_detector(&req).unwrap();
    }

    // ---- Misc operations ----

    #[test]
    fn get_log_group_fields_returns_stub() {
        let svc = make_service();
        create_group(&svc, "fields-group");

        let req = make_request(
            "GetLogGroupFields",
            json!({ "logGroupName": "fields-group" }),
        );
        let resp = svc.get_log_group_fields(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["logGroupFields"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_metric_filter_matches() {
        let svc = make_service();

        let req = make_request(
            "TestMetricFilter",
            json!({
                "filterPattern": "ERROR",
                "logEventMessages": ["ERROR: oops", "INFO: ok", "ERROR: again"],
            }),
        );
        let resp = svc.test_metric_filter(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["matches"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn stop_query_marks_as_cancelled() {
        let svc = make_service();
        create_group(&svc, "sq-group");

        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "sq-group",
                "startTime": 0,
                "endTime": 9999999999i64,
                "queryString": "fields @timestamp",
            }),
        );
        let resp = svc.start_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let qid = body["queryId"].as_str().unwrap().to_string();

        // Manually set query status to Running so we can test cancellation
        {
            let mut state = svc.state.write();
            state.queries.get_mut(&qid).unwrap().status = "Running".to_string();
        }

        let req = make_request("StopQuery", json!({ "queryId": &qid }));
        let resp = svc.stop_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["success"], true);

        let state = svc.state.read();
        assert_eq!(state.queries[&qid].status, "Cancelled");
    }

    #[test]
    fn put_log_group_deletion_protection() {
        let svc = make_service();
        create_group(&svc, "prot-group");

        let req = make_request(
            "PutLogGroupDeletionProtection",
            json!({
                "logGroupIdentifier": "prot-group",
                "deletionProtectionEnabled": true,
            }),
        );
        svc.put_log_group_deletion_protection(&req).unwrap();

        let state = svc.state.read();
        assert!(state.log_groups["prot-group"].deletion_protection);
    }

    #[test]
    fn get_log_record_returns_empty_stub() {
        let svc = make_service();

        let req = make_request(
            "GetLogRecord",
            json!({ "logRecordPointer": "some-pointer" }),
        );
        let resp = svc.get_log_record(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["logRecord"].is_object());
    }

    #[test]
    fn list_anomalies_returns_empty() {
        let svc = make_service();

        let req = make_request("ListAnomalies", json!({}));
        let resp = svc.list_anomalies(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["anomalies"].as_array().unwrap().is_empty());
    }

    #[test]
    fn update_anomaly_noop() {
        let svc = make_service();
        let req = make_request(
            "UpdateAnomaly",
            json!({"anomalyDetectorArn": "arn:aws:logs:us-east-1:123456789012:anomaly-detector:test"}),
        );
        svc.update_anomaly(&req).unwrap();
    }

    // -- Import tasks --

    #[test]
    fn import_task_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "CreateImportTask",
            json!({
                "importSourceArn": "arn:aws:s3:::my-bucket/logs",
                "importRoleArn": "arn:aws:iam::123456789012:role/import-role"
            }),
        );
        let resp = svc.create_import_task(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let import_id = body["importId"].as_str().unwrap().to_string();

        let req = make_request("DescribeImportTasks", json!({}));
        let resp = svc.describe_import_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["imports"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DescribeImportTaskBatches",
            json!({ "importId": import_id }),
        );
        let resp = svc.describe_import_task_batches(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["importBatches"].as_array().unwrap().is_empty());

        let req = make_request("CancelImportTask", json!({ "importId": import_id }));
        svc.cancel_import_task(&req).unwrap();

        let req = make_request("DescribeImportTasks", json!({}));
        let resp = svc.describe_import_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["imports"][0]["importStatus"].as_str().unwrap(),
            "CANCELLED"
        );
    }

    // -- Integrations --

    #[test]
    fn integration_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "PutIntegration",
            json!({
                "integrationName": "test-int",
                "integrationType": "OPENSEARCH",
                "resourceConfig": { "openSearchResourceConfig": {} }
            }),
        );
        svc.put_integration(&req).unwrap();

        let req = make_request("GetIntegration", json!({ "integrationName": "test-int" }));
        let resp = svc.get_integration(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["integrationName"].as_str().unwrap(), "test-int");

        let req = make_request("ListIntegrations", json!({}));
        let resp = svc.list_integrations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["integrationSummaries"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DeleteIntegration",
            json!({ "integrationName": "test-int" }),
        );
        svc.delete_integration(&req).unwrap();

        let req = make_request("ListIntegrations", json!({}));
        let resp = svc.list_integrations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["integrationSummaries"].as_array().unwrap().is_empty());
    }

    // -- Lookup tables --

    #[test]
    fn lookup_table_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "CreateLookupTable",
            json!({
                "lookupTableName": "test-table",
                "tableBody": "key,value\na,b"
            }),
        );
        let resp = svc.create_lookup_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let arn = body["lookupTableArn"].as_str().unwrap().to_string();

        let req = make_request("GetLookupTable", json!({ "lookupTableArn": arn }));
        let resp = svc.get_lookup_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["lookupTableName"].as_str().unwrap(), "test-table");

        let req = make_request("DescribeLookupTables", json!({}));
        let resp = svc.describe_lookup_tables(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["lookupTables"].as_array().unwrap().len(), 1);

        let req = make_request(
            "UpdateLookupTable",
            json!({ "lookupTableArn": arn, "tableBody": "key,value\nc,d" }),
        );
        svc.update_lookup_table(&req).unwrap();

        let req = make_request("DeleteLookupTable", json!({ "lookupTableArn": arn }));
        svc.delete_lookup_table(&req).unwrap();

        let req = make_request("DescribeLookupTables", json!({}));
        let resp = svc.describe_lookup_tables(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["lookupTables"].as_array().unwrap().is_empty());
    }

    // -- Scheduled queries --

    #[test]
    fn scheduled_query_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "CreateScheduledQuery",
            json!({
                "name": "test-sq",
                "queryString": "fields @timestamp | limit 10",
                "queryLanguage": "CWLI",
                "scheduleExpression": "rate(1 hour)",
                "executionRoleArn": "arn:aws:iam::123456789012:role/exec"
            }),
        );
        let resp = svc.create_scheduled_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let arn = body["scheduledQueryArn"].as_str().unwrap().to_string();

        let req = make_request("GetScheduledQuery", json!({ "identifier": arn }));
        let resp = svc.get_scheduled_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["name"].as_str().unwrap(), "test-sq");

        let req = make_request(
            "GetScheduledQueryHistory",
            json!({ "identifier": arn, "startTime": 0_i64, "endTime": 9999999999_i64 }),
        );
        let resp = svc.get_scheduled_query_history(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["triggerHistory"].as_array().unwrap().is_empty());

        let req = make_request("ListScheduledQueries", json!({}));
        let resp = svc.list_scheduled_queries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["scheduledQueries"].as_array().unwrap().len(), 1);

        let req = make_request(
            "UpdateScheduledQuery",
            json!({
                "identifier": arn,
                "queryString": "fields @message | limit 5",
                "queryLanguage": "CWLI",
                "scheduleExpression": "rate(2 hours)",
                "executionRoleArn": "arn:aws:iam::123456789012:role/exec"
            }),
        );
        svc.update_scheduled_query(&req).unwrap();

        let req = make_request("DeleteScheduledQuery", json!({ "identifier": arn }));
        svc.delete_scheduled_query(&req).unwrap();

        let req = make_request("ListScheduledQueries", json!({}));
        let resp = svc.list_scheduled_queries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["scheduledQueries"].as_array().unwrap().is_empty());
    }

    // -- Misc stubs --

    #[test]
    fn start_live_tail_returns_session() {
        let svc = make_service();
        let req = make_request(
            "StartLiveTail",
            json!({ "logGroupIdentifiers": ["/test/group"] }),
        );
        let resp = svc.start_live_tail(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["responseStream"]["sessionStart"]["sessionId"]
            .as_str()
            .is_some());
    }

    #[test]
    fn list_log_groups_delegates_to_describe() {
        let svc = make_service();
        create_group(&svc, "/test/list");
        let req = make_request("DescribeLogGroups", json!({}));
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["logGroups"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn list_log_groups_for_query_returns_empty() {
        let svc = make_service();
        let req = make_request(
            "ListLogGroupsForQuery",
            json!({ "queryId": "some-query-id" }),
        );
        let resp = svc.list_log_groups_for_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["logGroupIdentifiers"].as_array().unwrap().is_empty());
    }

    #[test]
    fn list_aggregate_log_group_summaries_returns_empty() {
        let svc = make_service();
        let req = make_request(
            "ListAggregateLogGroupSummaries",
            json!({ "groupBy": "DATA_SOURCE_NAME_AND_TYPE" }),
        );
        let resp = svc.list_aggregate_log_group_summaries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["aggregateLogGroupSummaries"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn put_bearer_token_authentication_stores_flag() {
        let svc = make_service();
        create_group(&svc, "/test/bearer");
        let req = make_request(
            "PutBearerTokenAuthentication",
            json!({
                "logGroupIdentifier": "/test/bearer",
                "bearerTokenAuthenticationEnabled": true
            }),
        );
        svc.put_bearer_token_authentication(&req).unwrap();
    }

    #[test]
    fn get_log_object_returns_stub() {
        let svc = make_service();
        let req = make_request(
            "GetLogObject",
            json!({ "logObjectPointer": "some-pointer" }),
        );
        let resp = svc.get_log_object(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body.is_object());
    }

    #[test]
    fn get_log_fields_returns_stub() {
        let svc = make_service();
        let req = make_request(
            "GetLogFields",
            json!({ "dataSourceName": "test", "dataSourceType": "CW_LOG" }),
        );
        let resp = svc.get_log_fields(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["logFields"].as_array().unwrap().is_empty());
    }

    #[test]
    fn s3_table_integration_stubs() {
        let svc = make_service();

        let req = make_request(
            "AssociateSourceToS3TableIntegration",
            json!({
                "integrationArn": "arn:aws:logs:us-east-1:123456789012:integration:test",
                "dataSource": { "resourceArn": "arn:aws:logs:us-east-1:123456789012:log-group:test" }
            }),
        );
        svc.associate_source_to_s3_table_integration(&req).unwrap();

        let req = make_request(
            "ListSourcesForS3TableIntegration",
            json!({
                "integrationArn": "arn:aws:logs:us-east-1:123456789012:integration:test"
            }),
        );
        let resp = svc.list_sources_for_s3_table_integration(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["sources"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DisassociateSourceFromS3TableIntegration",
            json!({ "identifier": "arn:aws:logs:us-east-1:123456789012:integration:test" }),
        );
        svc.disassociate_source_from_s3_table_integration(&req)
            .unwrap();
    }

    #[test]
    fn update_delivery_configuration_noop() {
        let svc = make_service();
        // First create a delivery setup
        let req = make_request(
            "PutDeliverySource",
            json!({
                "name": "test-ds",
                "resourceArn": "arn:aws:logs:us-east-1:123456789012:log-group:dummy",
                "logType": "APPLICATION_LOGS"
            }),
        );
        svc.put_delivery_source(&req).unwrap();

        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "test-dd",
                "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::test-bucket"
                }
            }),
        );
        svc.put_delivery_destination(&req).unwrap();

        let req = make_request(
            "CreateDelivery",
            json!({
                "deliverySourceName": "test-ds",
                "deliveryDestinationArn": "arn:aws:logs:us-east-1:123456789012:delivery-destination:test-dd"
            }),
        );
        let resp = svc.create_delivery(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let delivery_id = body["delivery"]["id"].as_str().unwrap().to_string();

        let req = make_request("UpdateDeliveryConfiguration", json!({ "id": delivery_id }));
        svc.update_delivery_configuration(&req).unwrap();
    }

    #[test]
    fn describe_configuration_templates_returns_empty() {
        let svc = make_service();
        let req = make_request("DescribeConfigurationTemplates", json!({}));
        let resp = svc.describe_configuration_templates(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["configurationTemplates"]
            .as_array()
            .unwrap()
            .is_empty());
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
}
