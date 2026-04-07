use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Value};

use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::SharedLogsState;

mod anomaly;
mod deliveries;
mod destinations;
mod exports;
mod filters;
mod groups;
mod misc;
mod policies;
mod queries;
mod streams;
mod tags;

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
            // Internal action for testing export storage
            "GetExportedData" => self.get_exported_data(&req),
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
pub(crate) mod test_helpers {
    use super::*;
    use crate::state::LogsState;
    use bytes::Bytes;
    use fakecloud_core::delivery::DeliveryBus;
    use http::{HeaderMap, Method};
    use std::collections::HashMap;
    use std::sync::Arc;

    pub fn make_service() -> LogsService {
        let state = Arc::new(parking_lot::RwLock::new(LogsState::new(
            "123456789012",
            "us-east-1",
        )));
        let delivery_bus = Arc::new(DeliveryBus::new());
        LogsService::new(state, delivery_bus)
    }

    pub fn make_request(
        action: &str,
        body: serde_json::Value,
    ) -> fakecloud_core::service::AwsRequest {
        fakecloud_core::service::AwsRequest {
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
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    pub fn create_group(svc: &LogsService, name: &str) {
        let req = make_request(
            "CreateLogGroup",
            serde_json::json!({ "logGroupName": name }),
        );
        svc.create_log_group(&req).unwrap();
    }

    pub fn create_stream(svc: &LogsService, group: &str, stream: &str) {
        let req = make_request(
            "CreateLogStream",
            serde_json::json!({ "logGroupName": group, "logStreamName": stream }),
        );
        svc.create_log_stream(&req).unwrap();
    }

    pub fn put_events(svc: &LogsService, group: &str, stream: &str, messages: &[&str]) {
        let now = chrono::Utc::now().timestamp_millis();
        let events: Vec<serde_json::Value> = messages
            .iter()
            .enumerate()
            .map(|(i, msg)| serde_json::json!({ "timestamp": now + i as i64, "message": msg }))
            .collect();
        let req = make_request(
            "PutLogEvents",
            serde_json::json!({
                "logGroupName": group,
                "logStreamName": stream,
                "logEvents": events,
            }),
        );
        svc.put_log_events(&req).unwrap();
    }
}
