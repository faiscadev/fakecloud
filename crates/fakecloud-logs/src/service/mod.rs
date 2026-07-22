use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Value};

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{LogsSnapshot, SharedLogsState, LOGS_SNAPSHOT_SCHEMA_VERSION};

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
mod syslog;
mod tags;

/// CloudWatch Logs actions that do NOT mutate state. Everything else
/// triggers a snapshot save on HTTP 2xx.
fn is_read_only_action(action: &str) -> bool {
    matches!(
        action,
        "DescribeLogGroups"
            | "DescribeLogStreams"
            | "GetLogEvents"
            | "FilterLogEvents"
            | "ListTagsLogGroup"
            | "ListTagsForResource"
            | "DescribeSubscriptionFilters"
            | "DescribeMetricFilters"
            | "DescribeResourcePolicies"
            | "DescribeDestinations"
            | "GetQueryResults"
            | "DescribeQueries"
            | "DescribeExportTasks"
            | "GetDeliveryDestination"
            | "DescribeDeliveryDestinations"
            | "GetDeliveryDestinationPolicy"
            | "GetDeliverySource"
            | "DescribeDeliverySources"
            | "GetDelivery"
            | "DescribeDeliveries"
            | "DescribeQueryDefinitions"
            | "DescribeAccountPolicies"
            | "GetDataProtectionPolicy"
            | "DescribeIndexPolicies"
            | "DescribeFieldIndexes"
            | "GetTransformer"
            | "TestTransformer"
            | "GetLogAnomalyDetector"
            | "ListLogAnomalyDetectors"
            | "GetLogGroupFields"
            | "TestMetricFilter"
            | "GetLogRecord"
            | "ListAnomalies"
            | "DescribeImportTasks"
            | "DescribeImportTaskBatches"
            | "GetIntegration"
            | "ListIntegrations"
            | "GetLookupTable"
            | "DescribeLookupTables"
            | "GetScheduledQuery"
            | "GetScheduledQueryHistory"
            | "ListScheduledQueries"
            | "StartLiveTail"
            | "ListLogGroups"
            | "ListLogGroupsForQuery"
            | "ListAggregateLogGroupSummaries"
            | "GetLogObject"
            | "GetLogFields"
            | "ListSourcesForS3TableIntegration"
            | "DescribeConfigurationTemplates"
            | "ListSyslogConfigurations"
            | "GetExportedData"
            | "GetStorageTierPolicy"
    )
}

pub struct LogsService {
    state: SharedLogsState,
    delivery_bus: Arc<DeliveryBus>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl LogsService {
    pub fn new(state: SharedLogsState, delivery_bus: Arc<DeliveryBus>) -> Self {
        Self {
            state,
            delivery_bus,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Share the snapshot lock with another writer of the Logs state (e.g. the
    /// EventBridge -> Logs delivery persist hook). Serialising both writers on
    /// the same lock prevents a stale-last-write where an older clone-serialize
    /// overwrites a newer one when the two persist paths interleave.
    pub fn with_snapshot_lock(mut self, lock: Arc<AsyncMutex<()>>) -> Self {
        self.snapshot_lock = lock;
        self
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes,
    /// with serde + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_logs_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Logs state when invoked, or `None`
    /// in memory mode (no snapshot store). The CloudFormation provisioner
    /// mutates `state` directly and uses this to write a CFN-provisioned
    /// resource through to disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_logs_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current Logs state as a snapshot. Offloads the serde + blocking
/// file write to the Tokio blocking pool. Noop when `store` is `None` (memory
/// mode). Shared by `LogsService::save_snapshot` and the CloudFormation
/// provisioner's post-provision persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_logs_snapshot(
    state: &SharedLogsState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = LogsSnapshot {
        schema_version: LOGS_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
        state: None,
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write logs snapshot"),
        Err(err) => tracing::error!(%err, "logs snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for LogsService {
    fn service_name(&self) -> &str {
        "logs"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = !is_read_only_action(req.action.as_str());
        let result = match req.action.as_str() {
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
            "PutSyslogConfiguration" => self.put_syslog_configuration(&req),
            "ListSyslogConfigurations" => self.list_syslog_configurations(&req),
            "DeleteSyslogConfiguration" => self.delete_syslog_configuration(&req),
            "GetStorageTierPolicy" => self.get_storage_tier_policy(&req),
            "PutStorageTierPolicy" => self.put_storage_tier_policy(&req),
            // Internal action for testing export storage
            "GetExportedData" => self.get_exported_data(&req),
            _ => Err(AwsServiceError::action_not_implemented("logs", &req.action)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }
}

const SUPPORTED_ACTIONS: &[&str] = &[
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
    "PutSyslogConfiguration",
    "ListSyslogConfigurations",
    "DeleteSyslogConfiguration",
    "GetStorageTierPolicy",
    "PutStorageTierPolicy",
];

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
fn dd_config_json(config: &std::collections::BTreeMap<String, String>) -> Value {
    let mut m: serde_json::Map<String, Value> =
        config.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
    m.entry("destinationResourceArn".to_string())
        .or_insert_with(|| json!(""));
    Value::Object(m)
}

/// Infer a delivery destination's type from its destination resource ARN's
/// service. Defaults to `CWL` (the most common case) when the ARN is absent or
/// unrecognised.
pub fn infer_delivery_destination_type(destination_arn: Option<&String>) -> String {
    let arn = destination_arn.map(String::as_str).unwrap_or("");
    let service = arn.split(':').nth(2).unwrap_or("");
    match service {
        "s3" => "S3",
        "firehose" => "FH",
        "xray" => "XRAY",
        _ => "CWL",
    }
    .to_string()
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
pub(crate) fn extract_log_group_from_arn(arn: &str) -> Option<String> {
    // arn:aws:logs:region:account:log-group:name:*
    let parts: Vec<&str> = arn.splitn(7, ':').collect();
    if parts.len() >= 7 && parts[5] == "log-group" {
        let name = parts[6].strip_suffix(":*").unwrap_or(parts[6]);
        Some(name.to_string())
    } else {
        None
    }
}

/// Encode a pagination offset into an opaque base64 `nextToken`.
pub(crate) fn encode_offset_token(offset: usize) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(format!("offset:{offset}").as_bytes())
}

/// Decode a `nextToken` produced by [`encode_offset_token`]. An absent or
/// unparseable token resolves to the first page (offset 0).
pub(crate) fn decode_offset_token(token: Option<&str>) -> usize {
    use base64::Engine;
    token
        .and_then(|t| base64::engine::general_purpose::STANDARD.decode(t).ok())
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| {
            s.strip_prefix("offset:")
                .and_then(|n| n.parse::<usize>().ok())
        })
        .unwrap_or(0)
}

/// Apply an offset+limit page to `items`, returning the page slice together
/// with the `nextToken` for the following page (if any). `limit` values <= 0
/// fall back to `default_limit`.
pub(crate) fn paginate_offset<T: Clone>(
    items: &[T],
    limit: Option<i64>,
    default_limit: usize,
    next_token: Option<&str>,
) -> (Vec<T>, Option<String>) {
    let limit = limit
        .filter(|n| *n > 0)
        .map(|n| n as usize)
        .unwrap_or(default_limit);
    let offset = decode_offset_token(next_token);
    let page: Vec<T> = items.iter().skip(offset).take(limit).cloned().collect();
    let next = if offset + limit < items.len() {
        Some(encode_offset_token(offset + limit))
    } else {
        None
    };
    (page, next)
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

    // JSON `{ ... }` (incl. `||`) and array `[ ... ]` patterns use the full
    // filter-pattern engine, so FilterLogEvents matches them the same way
    // metric-filter ingest does instead of failing closed.
    if (pattern.starts_with('{') && pattern.ends_with('}')) || pattern.starts_with('[') {
        return crate::filter_pattern::matches(pattern, message);
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

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;
    use bytes::Bytes;
    use fakecloud_core::delivery::DeliveryBus;
    use http::{HeaderMap, Method};
    use std::collections::HashMap;
    use std::sync::Arc;

    pub fn make_service() -> LogsService {
        let state = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
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
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
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

    pub fn put_events_at(
        svc: &LogsService,
        group: &str,
        stream: &str,
        messages: &[&str],
        timestamp: i64,
    ) {
        let events: Vec<serde_json::Value> = messages
            .iter()
            .enumerate()
            .map(
                |(i, msg)| serde_json::json!({ "timestamp": timestamp + i as i64, "message": msg }),
            )
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

    pub fn put_retention(svc: &LogsService, group: &str, days: i32) {
        let req = make_request(
            "PutRetentionPolicy",
            serde_json::json!({ "logGroupName": group, "retentionInDays": days }),
        );
        svc.put_retention_policy(&req).unwrap();
    }

    // bug-audit 2026-06-27, T1.14: FilterLogEvents now evaluates array `[...]`
    // patterns through the full engine (positional token match) instead of
    // failing closed.
    #[test]
    fn array_filter_pattern_matches_positionally() {
        // Bare-name fields match any token in their slot.
        assert!(matches_filter_pattern("[w1, w2, w3]", "some log message"));
        // A literal-equality field that doesn't match the token fails.
        assert!(!matches_filter_pattern(
            "[w1=ERROR, w2, w3]",
            "INFO log message"
        ));
        // Wrong arity (4 tokens vs 3 fields) doesn't match.
        assert!(!matches_filter_pattern("[w1, w2, w3]", "a b c d"));
    }

    // JSON patterns with `||` (OR) now match via the full engine.
    #[test]
    fn json_filter_pattern_supports_or() {
        let msg = r#"{"level":"ERROR","code":500}"#;
        assert!(matches_filter_pattern(
            "{ $.level = \"WARN\" || $.code = 500 }",
            msg
        ));
        assert!(!matches_filter_pattern(
            "{ $.level = \"WARN\" || $.code = 200 }",
            msg
        ));
    }

    /// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
    #[test]
    fn snapshot_hook_is_none_without_store() {
        let svc = make_service();
        assert!(svc.snapshot_hook().is_none());
    }

    /// With a store, the hook is present and invoking it runs the whole-state
    /// persist path the CloudFormation provisioner uses after mutating logs
    /// state directly.
    #[tokio::test]
    async fn snapshot_hook_fires_with_store() {
        let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
            Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
        let svc = make_service().with_snapshot_store(store);
        let hook = svc
            .snapshot_hook()
            .expect("hook present when a store is set");
        // Must not panic; exercises the closure and the snapshot save path.
        hook().await;
    }
}
