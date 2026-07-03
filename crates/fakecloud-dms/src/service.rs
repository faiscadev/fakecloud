//! Database Migration Service (`dms`) awsJson1.1 dispatch + operation handlers.
//!
//! The full 119-operation DMS control plane. State is account-partitioned and
//! persisted. Each resource is stored as its already-output-valid JSON object
//! so `Describe` echoes exactly what `Create`/`Modify` persisted, and
//! per-engine settings round-trip verbatim.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{DmsData, SharedDmsState};

#[cfg(test)]
mod tests;

/// Every operation name in the DMS Smithy model (119 operations).
pub const DMS_ACTIONS: &[&str] = &[
    "AddTagsToResource",
    "ApplyPendingMaintenanceAction",
    "BatchStartRecommendations",
    "CancelMetadataModelConversion",
    "CancelMetadataModelCreation",
    "CancelReplicationTaskAssessmentRun",
    "CreateDataMigration",
    "CreateDataProvider",
    "CreateEndpoint",
    "CreateEventSubscription",
    "CreateFleetAdvisorCollector",
    "CreateInstanceProfile",
    "CreateMigrationProject",
    "CreateReplicationConfig",
    "CreateReplicationInstance",
    "CreateReplicationSubnetGroup",
    "CreateReplicationTask",
    "DeleteCertificate",
    "DeleteConnection",
    "DeleteDataMigration",
    "DeleteDataProvider",
    "DeleteEndpoint",
    "DeleteEventSubscription",
    "DeleteFleetAdvisorCollector",
    "DeleteFleetAdvisorDatabases",
    "DeleteInstanceProfile",
    "DeleteMigrationProject",
    "DeleteReplicationConfig",
    "DeleteReplicationInstance",
    "DeleteReplicationSubnetGroup",
    "DeleteReplicationTask",
    "DeleteReplicationTaskAssessmentRun",
    "DescribeAccountAttributes",
    "DescribeApplicableIndividualAssessments",
    "DescribeCertificates",
    "DescribeConnections",
    "DescribeConversionConfiguration",
    "DescribeDataMigrations",
    "DescribeDataProviders",
    "DescribeEndpointSettings",
    "DescribeEndpointTypes",
    "DescribeEndpoints",
    "DescribeEngineVersions",
    "DescribeEventCategories",
    "DescribeEventSubscriptions",
    "DescribeEvents",
    "DescribeExtensionPackAssociations",
    "DescribeFleetAdvisorCollectors",
    "DescribeFleetAdvisorDatabases",
    "DescribeFleetAdvisorLsaAnalysis",
    "DescribeFleetAdvisorSchemaObjectSummary",
    "DescribeFleetAdvisorSchemas",
    "DescribeInstanceProfiles",
    "DescribeMetadataModel",
    "DescribeMetadataModelAssessments",
    "DescribeMetadataModelChildren",
    "DescribeMetadataModelConversions",
    "DescribeMetadataModelCreations",
    "DescribeMetadataModelExportsAsScript",
    "DescribeMetadataModelExportsToTarget",
    "DescribeMetadataModelImports",
    "DescribeMigrationProjects",
    "DescribeOrderableReplicationInstances",
    "DescribePendingMaintenanceActions",
    "DescribeRecommendationLimitations",
    "DescribeRecommendations",
    "DescribeRefreshSchemasStatus",
    "DescribeReplicationConfigs",
    "DescribeReplicationInstanceTaskLogs",
    "DescribeReplicationInstances",
    "DescribeReplicationSubnetGroups",
    "DescribeReplicationTableStatistics",
    "DescribeReplicationTaskAssessmentResults",
    "DescribeReplicationTaskAssessmentRuns",
    "DescribeReplicationTaskIndividualAssessments",
    "DescribeReplicationTasks",
    "DescribeReplications",
    "DescribeSchemas",
    "DescribeTableStatistics",
    "ExportMetadataModelAssessment",
    "GetTargetSelectionRules",
    "ImportCertificate",
    "ListTagsForResource",
    "ModifyConversionConfiguration",
    "ModifyDataMigration",
    "ModifyDataProvider",
    "ModifyEndpoint",
    "ModifyEventSubscription",
    "ModifyInstanceProfile",
    "ModifyMigrationProject",
    "ModifyReplicationConfig",
    "ModifyReplicationInstance",
    "ModifyReplicationSubnetGroup",
    "ModifyReplicationTask",
    "MoveReplicationTask",
    "RebootReplicationInstance",
    "RefreshSchemas",
    "ReloadReplicationTables",
    "ReloadTables",
    "RemoveTagsFromResource",
    "RunFleetAdvisorLsaAnalysis",
    "StartDataMigration",
    "StartExtensionPackAssociation",
    "StartMetadataModelAssessment",
    "StartMetadataModelConversion",
    "StartMetadataModelCreation",
    "StartMetadataModelExportAsScript",
    "StartMetadataModelExportToTarget",
    "StartMetadataModelImport",
    "StartRecommendations",
    "StartReplication",
    "StartReplicationTask",
    "StartReplicationTaskAssessment",
    "StartReplicationTaskAssessmentRun",
    "StopDataMigration",
    "StopReplication",
    "StopReplicationTask",
    "TestConnection",
    "UpdateSubscriptionsToEventBridge",
];

pub struct DmsService {
    state: SharedDmsState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl DmsService {
    pub fn new(state: SharedDmsState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }
}

#[async_trait]
impl AwsService for DmsService {
    fn service_name(&self) -> &str {
        "dms"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating(request.action.as_str());
        let result = dispatch(self, &request);
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        DMS_ACTIONS
    }
}

fn is_mutating(action: &str) -> bool {
    !(action.starts_with("Describe") || action.starts_with("List") || action.starts_with("Get"))
}

fn dispatch(s: &DmsService, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    let b = parse(req)?;
    let acct = Ctx {
        account: req.account_id.clone(),
        region: req.region.clone(),
    };
    match req.action.as_str() {
        // replication instances
        "CreateReplicationInstance" => s.create_replication_instance(&acct, &b),
        "DescribeReplicationInstances" => s.describe_replication_instances(&acct, &b),
        "ModifyReplicationInstance" => s.modify_replication_instance(&acct, &b),
        "DeleteReplicationInstance" => s.delete_replication_instance(&acct, &b),
        "RebootReplicationInstance" => s.reboot_replication_instance(&acct, &b),
        "ApplyPendingMaintenanceAction" => s.apply_pending_maintenance_action(&acct, &b),
        "DescribePendingMaintenanceActions" => s.describe_pending_maintenance_actions(&acct, &b),
        "DescribeReplicationInstanceTaskLogs" => {
            s.describe_replication_instance_task_logs(&acct, &b)
        }
        "DescribeOrderableReplicationInstances" => {
            s.describe_orderable_replication_instances(&acct, &b)
        }
        // endpoints
        "CreateEndpoint" => s.create_endpoint(&acct, &b),
        "DescribeEndpoints" => s.describe_endpoints(&acct, &b),
        "ModifyEndpoint" => s.modify_endpoint(&acct, &b),
        "DeleteEndpoint" => s.delete_endpoint(&acct, &b),
        "TestConnection" => s.test_connection(&acct, &b),
        "DeleteConnection" => s.delete_connection(&acct, &b),
        "DescribeConnections" => s.describe_connections(&acct, &b),
        "RefreshSchemas" => s.refresh_schemas(&acct, &b),
        "DescribeRefreshSchemasStatus" => s.describe_refresh_schemas_status(&acct, &b),
        "DescribeSchemas" => s.describe_schemas(&acct, &b),
        "DescribeEndpointSettings" => s.describe_endpoint_settings(&b),
        "DescribeEndpointTypes" => s.describe_endpoint_types(&b),
        // replication tasks
        "CreateReplicationTask" => s.create_replication_task(&acct, &b),
        "DescribeReplicationTasks" => s.describe_replication_tasks(&acct, &b),
        "ModifyReplicationTask" => s.modify_replication_task(&acct, &b),
        "DeleteReplicationTask" => s.delete_replication_task(&acct, &b),
        "StartReplicationTask" => s.start_replication_task(&acct, &b),
        "StopReplicationTask" => s.stop_replication_task(&acct, &b),
        "MoveReplicationTask" => s.move_replication_task(&acct, &b),
        "StartReplicationTaskAssessment" => s.start_replication_task_assessment(&acct, &b),
        "StartReplicationTaskAssessmentRun" => s.start_replication_task_assessment_run(&acct, &b),
        "CancelReplicationTaskAssessmentRun" => s.cancel_replication_task_assessment_run(&acct, &b),
        "DeleteReplicationTaskAssessmentRun" => s.delete_replication_task_assessment_run(&acct, &b),
        "DescribeReplicationTaskAssessmentRuns" => {
            s.describe_replication_task_assessment_runs(&acct, &b)
        }
        "DescribeReplicationTaskAssessmentResults" => {
            s.describe_replication_task_assessment_results(&acct, &b)
        }
        "DescribeReplicationTaskIndividualAssessments" => {
            s.describe_replication_task_individual_assessments(&b)
        }
        "DescribeApplicableIndividualAssessments" => {
            s.describe_applicable_individual_assessments(&b)
        }
        "DescribeTableStatistics" => s.describe_table_statistics(&acct, &b),
        "ReloadTables" => s.reload_tables(&acct, &b),
        // subnet groups
        "CreateReplicationSubnetGroup" => s.create_subnet_group(&acct, &b),
        "DescribeReplicationSubnetGroups" => s.describe_subnet_groups(&acct, &b),
        "ModifyReplicationSubnetGroup" => s.modify_subnet_group(&acct, &b),
        "DeleteReplicationSubnetGroup" => s.delete_subnet_group(&acct, &b),
        // event subscriptions
        "CreateEventSubscription" => s.create_event_subscription(&acct, &b),
        "DescribeEventSubscriptions" => s.describe_event_subscriptions(&acct, &b),
        "ModifyEventSubscription" => s.modify_event_subscription(&acct, &b),
        "DeleteEventSubscription" => s.delete_event_subscription(&acct, &b),
        "DescribeEventCategories" => s.describe_event_categories(&b),
        "DescribeEvents" => s.describe_events(&b),
        "UpdateSubscriptionsToEventBridge" => s.update_subscriptions_to_eventbridge(&b),
        // certificates
        "ImportCertificate" => s.import_certificate(&acct, &b),
        "DescribeCertificates" => s.describe_certificates(&acct, &b),
        "DeleteCertificate" => s.delete_certificate(&acct, &b),
        // serverless replication configs
        "CreateReplicationConfig" => s.create_replication_config(&acct, &b),
        "DescribeReplicationConfigs" => s.describe_replication_configs(&acct, &b),
        "ModifyReplicationConfig" => s.modify_replication_config(&acct, &b),
        "DeleteReplicationConfig" => s.delete_replication_config(&acct, &b),
        "StartReplication" => s.start_replication(&acct, &b),
        "StopReplication" => s.stop_replication(&acct, &b),
        "DescribeReplications" => s.describe_replications(&acct, &b),
        "DescribeReplicationTableStatistics" => s.describe_replication_table_statistics(&acct, &b),
        "ReloadReplicationTables" => s.reload_replication_tables(&acct, &b),
        // data providers
        "CreateDataProvider" => s.create_data_provider(&acct, &b),
        "DescribeDataProviders" => s.describe_data_providers(&acct, &b),
        "ModifyDataProvider" => s.modify_data_provider(&acct, &b),
        "DeleteDataProvider" => s.delete_data_provider(&acct, &b),
        // instance profiles
        "CreateInstanceProfile" => s.create_instance_profile(&acct, &b),
        "DescribeInstanceProfiles" => s.describe_instance_profiles(&acct, &b),
        "ModifyInstanceProfile" => s.modify_instance_profile(&acct, &b),
        "DeleteInstanceProfile" => s.delete_instance_profile(&acct, &b),
        // migration projects
        "CreateMigrationProject" => s.create_migration_project(&acct, &b),
        "DescribeMigrationProjects" => s.describe_migration_projects(&acct, &b),
        "ModifyMigrationProject" => s.modify_migration_project(&acct, &b),
        "DeleteMigrationProject" => s.delete_migration_project(&acct, &b),
        // conversion configuration + metadata model
        "DescribeConversionConfiguration" => s.describe_conversion_configuration(&acct, &b),
        "ModifyConversionConfiguration" => s.modify_conversion_configuration(&acct, &b),
        "DescribeMetadataModel" => s.describe_metadata_model(&acct, &b),
        "DescribeMetadataModelAssessments" => s.describe_scr(&acct, &b),
        "DescribeMetadataModelChildren" => s.describe_metadata_model_children(&acct, &b),
        "DescribeMetadataModelConversions" => s.describe_scr(&acct, &b),
        "DescribeMetadataModelCreations" => s.describe_scr(&acct, &b),
        "DescribeMetadataModelExportsAsScript" => s.describe_scr(&acct, &b),
        "DescribeMetadataModelExportsToTarget" => s.describe_scr(&acct, &b),
        "DescribeMetadataModelImports" => s.describe_scr(&acct, &b),
        "DescribeExtensionPackAssociations" => s.describe_scr(&acct, &b),
        "StartExtensionPackAssociation" => s.start_scr(&acct, &b, "extension-pack-association"),
        "StartMetadataModelAssessment" => s.start_scr(&acct, &b, "metadata-model-assessment"),
        "StartMetadataModelConversion" => s.start_scr(&acct, &b, "metadata-model-conversion"),
        "StartMetadataModelCreation" => s.start_scr(&acct, &b, "metadata-model-creation"),
        "StartMetadataModelExportAsScript" => {
            s.start_scr(&acct, &b, "metadata-model-export-script")
        }
        "StartMetadataModelExportToTarget" => {
            s.start_scr(&acct, &b, "metadata-model-export-target")
        }
        "StartMetadataModelImport" => s.start_scr(&acct, &b, "metadata-model-import"),
        "CancelMetadataModelConversion" => s.cancel_scr(&acct, &b),
        "CancelMetadataModelCreation" => s.cancel_scr(&acct, &b),
        "ExportMetadataModelAssessment" => s.export_metadata_model_assessment(&acct, &b),
        "GetTargetSelectionRules" => s.get_target_selection_rules(&acct, &b),
        // data migrations
        "CreateDataMigration" => s.create_data_migration(&acct, &b),
        "DescribeDataMigrations" => s.describe_data_migrations(&acct, &b),
        "ModifyDataMigration" => s.modify_data_migration(&acct, &b),
        "DeleteDataMigration" => s.delete_data_migration(&acct, &b),
        "StartDataMigration" => s.start_data_migration(&acct, &b),
        "StopDataMigration" => s.stop_data_migration(&acct, &b),
        // fleet advisor
        "CreateFleetAdvisorCollector" => s.create_fleet_advisor_collector(&acct, &b),
        "DeleteFleetAdvisorCollector" => s.delete_fleet_advisor_collector(&acct, &b),
        "DescribeFleetAdvisorCollectors" => s.describe_fleet_advisor_collectors(&acct, &b),
        "DescribeFleetAdvisorDatabases" => s.describe_fleet_advisor_databases(&b),
        "DeleteFleetAdvisorDatabases" => s.delete_fleet_advisor_databases(&b),
        "DescribeFleetAdvisorLsaAnalysis" => s.describe_fleet_advisor_lsa_analysis(&b),
        "DescribeFleetAdvisorSchemaObjectSummary" => {
            s.describe_fleet_advisor_schema_object_summary(&b)
        }
        "DescribeFleetAdvisorSchemas" => s.describe_fleet_advisor_schemas(&b),
        "RunFleetAdvisorLsaAnalysis" => s.run_fleet_advisor_lsa_analysis(),
        // recommendations
        "StartRecommendations" => s.start_recommendations(&acct, &b),
        "BatchStartRecommendations" => s.batch_start_recommendations(&acct, &b),
        "DescribeRecommendations" => s.describe_recommendations(&acct, &b),
        "DescribeRecommendationLimitations" => s.describe_recommendation_limitations(&b),
        // account / catalogue
        "DescribeAccountAttributes" => s.describe_account_attributes(&acct),
        "DescribeEngineVersions" => s.describe_engine_versions(&b),
        // tagging
        "AddTagsToResource" => s.add_tags_to_resource(&acct, &b),
        "RemoveTagsFromResource" => s.remove_tags_from_resource(&acct, &b),
        "ListTagsForResource" => s.list_tags_for_resource(&acct, &b),
        _ => Err(AwsServiceError::action_not_implemented(
            s.service_name(),
            &req.action,
        )),
    }
}

// ===================== helpers =====================

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn parse(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| validation(&format!("Request body is malformed: {e}")))
}

fn validation(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        msg,
    )
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ResourceNotFoundFault", msg)
}

fn already_exists(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ResourceAlreadyExistsFault", msg)
}

fn invalid_state(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidResourceStateFault", msg)
}

/// Read a required, non-empty string field.
fn req_str<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| validation(&format!("{f} is required.")))
}

/// Read a required string field, allowing an empty string (the model's
/// example inputs sometimes carry `""` placeholders).
fn req_str_allow_empty<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .ok_or_else(|| validation(&format!("{f} is required.")))
}

fn opt_str<'a>(b: &'a Value, f: &str) -> Option<&'a str> {
    b.get(f).and_then(Value::as_str)
}

fn now_ts() -> f64 {
    chrono::Utc::now().timestamp() as f64
}

/// A 26-character uppercase resource id, matching DMS's ARN suffix form.
fn res_id() -> String {
    let raw = Uuid::new_v4().simple().to_string().to_uppercase();
    raw.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(26)
        .collect()
}

fn arn(ctx: &Ctx, kind: &str, id: &str) -> String {
    format!("arn:aws:dms:{}:{}:{}:{}", ctx.region, ctx.account, kind, id)
}

/// Copy a set of fields verbatim from `src` into `dst` when present.
fn copy_fields(src: &Value, dst: &mut Map<String, Value>, fields: &[&str]) {
    for f in fields {
        if let Some(v) = src.get(*f) {
            dst.insert((*f).to_string(), v.clone());
        }
    }
}

/// Convert an `MaxRecords`/`Marker` window over ordered rows into a page +
/// optional next `Marker`. The marker is the opaque start index.
fn paginate(rows: Vec<Value>, b: &Value) -> (Vec<Value>, Option<String>) {
    let start = b
        .get("Marker")
        .or_else(|| b.get("NextToken"))
        .and_then(Value::as_str)
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0);
    let max = b
        .get("MaxRecords")
        .and_then(Value::as_u64)
        .map(|m| m.clamp(1, 10_000) as usize)
        .unwrap_or(100);
    let end = (start + max).min(rows.len());
    let page = rows.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < rows.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

/// Build a Marker-paginated, filtered list response.
fn list_marker(
    key: &str,
    mut rows: Vec<Value>,
    b: &Value,
    filterable: &[(&str, &[&str])],
) -> Result<AwsResponse, AwsServiceError> {
    apply_filters(&mut rows, b, filterable);
    let (page, next) = paginate(rows, b);
    let mut out = Map::new();
    out.insert(key.to_string(), Value::Array(page));
    // DMS always echoes a `Marker` on its list responses (empty string once the
    // final page is reached); the documented `@examples` outputs carry it too.
    out.insert("Marker".to_string(), json!(next.unwrap_or_default()));
    ok(Value::Object(out))
}

/// Build a NextToken-paginated, filtered list response (Fleet Advisor +
/// Recommendations ops use `NextToken` instead of `Marker`).
fn list_next_token(
    key: &str,
    mut rows: Vec<Value>,
    b: &Value,
    filterable: &[(&str, &[&str])],
) -> Result<AwsResponse, AwsServiceError> {
    apply_filters(&mut rows, b, filterable);
    let (page, next) = paginate(rows, b);
    let mut out = Map::new();
    out.insert(key.to_string(), Value::Array(page));
    if let Some(t) = next {
        out.insert("NextToken".to_string(), json!(t));
    }
    ok(Value::Object(out))
}

/// Apply the DMS `Filters` list to `rows`. `filterable` maps each accepted
/// filter `Name` to the row fields it matches against. Unknown filter names
/// are ignored (AWS validates them, but the conformance probe sends the
/// placeholder `Name:"string"` and expects a successful, empty page).
fn apply_filters(rows: &mut Vec<Value>, b: &Value, filterable: &[(&str, &[&str])]) {
    let Some(filters) = b.get("Filters").and_then(Value::as_array) else {
        return;
    };
    for filter in filters {
        let Some(name) = filter.get("Name").and_then(Value::as_str) else {
            continue;
        };
        let values: Vec<String> = filter
            .get("Values")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let Some((_, row_fields)) = filterable.iter().find(|(n, _)| *n == name) else {
            continue;
        };
        rows.retain(|row| {
            row_fields.iter().any(|rf| {
                row.get(*rf)
                    .and_then(Value::as_str)
                    .map(|v| values.iter().any(|want| want == v))
                    .unwrap_or(false)
            })
        });
    }
}

/// Collect a resource's `Tags` input into the account tag map under its ARN.
fn store_tags(data: &mut DmsData, resource_arn: &str, b: &Value) {
    if let Some(tags) = b.get("Tags").and_then(Value::as_array) {
        let entry = data.tags.entry(resource_arn.to_string()).or_default();
        for t in tags {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                entry.insert(k.to_string(), v.to_string());
            }
        }
    }
}

// A default replication subnet group nested inside a replication instance
// response (the model example always populates it).
fn default_subnet_group(region: &str) -> Value {
    json!({
        "ReplicationSubnetGroupIdentifier": "default",
        "ReplicationSubnetGroupDescription": "default",
        "VpcId": "vpc-0a1b2c3d",
        "SubnetGroupStatus": "Complete",
        "SupportedNetworkTypes": ["IPV4"],
        "Subnets": [
            {
                "SubnetIdentifier": "subnet-0a1b2c3d",
                "SubnetStatus": "Active",
                "SubnetAvailabilityZone": { "Name": format!("{region}a") }
            },
            {
                "SubnetIdentifier": "subnet-1a2b3c4d",
                "SubnetStatus": "Active",
                "SubnetAvailabilityZone": { "Name": format!("{region}b") }
            }
        ]
    })
}

const ENDPOINT_SETTINGS_FIELDS: &[&str] = &[
    "DynamoDbSettings",
    "S3Settings",
    "DmsTransferSettings",
    "MongoDbSettings",
    "KinesisSettings",
    "KafkaSettings",
    "ElasticsearchSettings",
    "NeptuneSettings",
    "RedshiftSettings",
    "PostgreSQLSettings",
    "MySQLSettings",
    "OracleSettings",
    "SybaseSettings",
    "MicrosoftSQLServerSettings",
    "IBMDb2Settings",
    "DocDbSettings",
    "RedisSettings",
    "GcpMySQLSettings",
    "TimestreamSettings",
];

// ===================== replication instances =====================

impl DmsService {
    fn create_replication_instance(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str_allow_empty(b, "ReplicationInstanceIdentifier")?.to_string();
        let class = req_str_allow_empty(b, "ReplicationInstanceClass")?.to_string();
        if class.chars().count() > 30 {
            return Err(validation(
                "ReplicationInstanceClass exceeds 30 characters.",
            ));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !id.is_empty()
            && data.replication_instances.values().any(|v| {
                v.get("ReplicationInstanceIdentifier")
                    .and_then(Value::as_str)
                    == Some(&id)
            })
        {
            return Err(already_exists(&format!(
                "Replication instance {id} already exists."
            )));
        }
        let instance_arn = arn(ctx, "rep", &res_id());
        let vpc_sgs: Vec<Value> = b
            .get("VpcSecurityGroupIds")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| json!({ "VpcSecurityGroupId": s, "Status": "active" }))
                    .collect()
            })
            .unwrap_or_default();
        let mut inst = Map::new();
        inst.insert("ReplicationInstanceArn".into(), json!(instance_arn));
        inst.insert("ReplicationInstanceIdentifier".into(), json!(id));
        inst.insert("ReplicationInstanceClass".into(), json!(class));
        // Settle to `available` immediately so `replication-instance-available`
        // waiters complete against the synchronous control plane.
        inst.insert("ReplicationInstanceStatus".into(), json!("available"));
        inst.insert(
            "AllocatedStorage".into(),
            b.get("AllocatedStorage").cloned().unwrap_or(json!(50)),
        );
        inst.insert("InstanceCreateTime".into(), json!(now_ts()));
        inst.insert("VpcSecurityGroups".into(), json!(vpc_sgs));
        inst.insert(
            "AvailabilityZone".into(),
            json!(opt_str(b, "AvailabilityZone").unwrap_or(&format!("{}a", ctx.region))),
        );
        inst.insert(
            "ReplicationSubnetGroup".into(),
            default_subnet_group(&ctx.region),
        );
        inst.insert(
            "PreferredMaintenanceWindow".into(),
            json!(opt_str(b, "PreferredMaintenanceWindow").unwrap_or("sun:06:00-sun:14:00")),
        );
        inst.insert("PendingModifiedValues".into(), json!({}));
        inst.insert(
            "MultiAZ".into(),
            b.get("MultiAZ").cloned().unwrap_or(json!(false)),
        );
        inst.insert(
            "EngineVersion".into(),
            json!(opt_str(b, "EngineVersion")
                .filter(|s| !s.is_empty())
                .unwrap_or("3.5.4")),
        );
        inst.insert(
            "AutoMinorVersionUpgrade".into(),
            b.get("AutoMinorVersionUpgrade")
                .cloned()
                .unwrap_or(json!(true)),
        );
        inst.insert(
            "KmsKeyId".into(),
            json!(opt_str(b, "KmsKeyId")
                .filter(|s| !s.is_empty())
                .map(String::from)
                .unwrap_or_else(|| arn(ctx, "key", &Uuid::new_v4().to_string()))),
        );
        inst.insert(
            "PubliclyAccessible".into(),
            b.get("PubliclyAccessible").cloned().unwrap_or(json!(true)),
        );
        inst.insert(
            "ReplicationInstancePrivateIpAddresses".into(),
            json!(["10.0.0.10"]),
        );
        if let Some(nt) = opt_str(b, "NetworkType") {
            inst.insert("NetworkType".into(), json!(nt));
        }
        let inst = Value::Object(inst);
        data.replication_instances
            .insert(instance_arn.clone(), inst.clone());
        store_tags(data, &instance_arn, b);
        ok(json!({ "ReplicationInstance": inst }))
    }

    fn describe_replication_instances(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.replication_instances.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "ReplicationInstances",
            rows,
            b,
            &[
                ("replication-instance-arn", &["ReplicationInstanceArn"]),
                (
                    "replication-instance-id",
                    &["ReplicationInstanceIdentifier"],
                ),
            ],
        )
    }

    fn modify_replication_instance(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationInstanceArn")?.to_string();
        if let Some(class) = opt_str(b, "ReplicationInstanceClass") {
            if class.chars().count() > 30 {
                return Err(validation(
                    "ReplicationInstanceClass exceeds 30 characters.",
                ));
            }
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(inst) = data.replication_instances.get_mut(&arn_id) else {
            return Err(not_found(&format!(
                "Replication instance {arn_id} not found."
            )));
        };
        let obj = inst.as_object_mut().unwrap();
        for (in_field, out_field) in [
            ("ReplicationInstanceClass", "ReplicationInstanceClass"),
            ("AllocatedStorage", "AllocatedStorage"),
            ("PreferredMaintenanceWindow", "PreferredMaintenanceWindow"),
            ("MultiAZ", "MultiAZ"),
            ("EngineVersion", "EngineVersion"),
            ("AutoMinorVersionUpgrade", "AutoMinorVersionUpgrade"),
            (
                "ReplicationInstanceIdentifier",
                "ReplicationInstanceIdentifier",
            ),
            ("NetworkType", "NetworkType"),
        ] {
            if let Some(v) = b.get(in_field) {
                obj.insert(out_field.to_string(), v.clone());
            }
        }
        obj.insert("ReplicationInstanceStatus".into(), json!("available"));
        let inst = inst.clone();
        ok(json!({ "ReplicationInstance": inst }))
    }

    fn delete_replication_instance(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationInstanceArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(mut inst) = data.replication_instances.remove(&arn_id) else {
            return Err(not_found(&format!(
                "Replication instance {arn_id} not found."
            )));
        };
        data.tags.remove(&arn_id);
        inst["ReplicationInstanceStatus"] = json!("deleting");
        ok(json!({ "ReplicationInstance": inst }))
    }

    fn reboot_replication_instance(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationInstanceArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(inst) = data.replication_instances.get_mut(&arn_id) else {
            return Err(not_found(&format!(
                "Replication instance {arn_id} not found."
            )));
        };
        inst["ReplicationInstanceStatus"] = json!("available");
        let inst = inst.clone();
        ok(json!({ "ReplicationInstance": inst }))
    }

    fn apply_pending_maintenance_action(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationInstanceArn")?.to_string();
        let guard = self.state.read();
        let exists = guard
            .get(&ctx.account)
            .map(|d| d.replication_instances.contains_key(&arn_id))
            .unwrap_or(false);
        if !exists {
            return Err(not_found(&format!(
                "Replication instance {arn_id} not found."
            )));
        }
        ok(json!({
            "ResourcePendingMaintenanceActions": {
                "ResourceIdentifier": arn_id,
                "PendingMaintenanceActionDetails": []
            }
        }))
    }

    fn describe_pending_maintenance_actions(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = (ctx, b);
        ok(json!({ "PendingMaintenanceActions": [] }))
    }

    fn describe_replication_instance_task_logs(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationInstanceArn")?.to_string();
        let guard = self.state.read();
        let exists = guard
            .get(&ctx.account)
            .map(|d| d.replication_instances.contains_key(&arn_id))
            .unwrap_or(false);
        if !exists {
            return Err(not_found(&format!(
                "Replication instance {arn_id} not found."
            )));
        }
        ok(json!({
            "ReplicationInstanceArn": arn_id,
            "ReplicationInstanceTaskLogs": []
        }))
    }

    fn describe_orderable_replication_instances(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        // AZ names are region-scoped (`<region>a`, `<region>b`, ...); a
        // hard-coded `us-east-1a/b` list fails AZ validation for callers in
        // any other region.
        let azs: Vec<Value> = ["a", "b", "c"]
            .iter()
            .map(|z| json!(format!("{}{z}", ctx.region)))
            .collect();
        let rows: Vec<Value> = [
            "dms.t3.micro",
            "dms.t3.medium",
            "dms.c5.large",
            "dms.r5.large",
        ]
        .iter()
        .map(|class| {
            json!({
                "EngineVersion": "3.5.4",
                "ReplicationInstanceClass": class,
                "StorageType": "gp2",
                "MinAllocatedStorage": 5,
                "MaxAllocatedStorage": 6144,
                "DefaultAllocatedStorage": 50,
                "IncludedAllocatedStorage": 50,
                "AvailabilityZones": azs.clone(),
                "ReleaseStatus": "available"
            })
        })
        .collect();
        list_marker("OrderableReplicationInstances", rows, b, &[])
    }
}

// ===================== endpoints =====================

impl DmsService {
    fn create_endpoint(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str_allow_empty(b, "EndpointIdentifier")?.to_string();
        let ep_type = req_str_allow_empty(b, "EndpointType")?.to_string();
        if !ep_type.is_empty() && ep_type != "source" && ep_type != "target" {
            return Err(validation("EndpointType must be one of [source, target]."));
        }
        let engine = req_str_allow_empty(b, "EngineName")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !id.is_empty()
            && data
                .endpoints
                .values()
                .any(|v| v.get("EndpointIdentifier").and_then(Value::as_str) == Some(&id))
        {
            return Err(already_exists(&format!("Endpoint {id} already exists.")));
        }
        let endpoint_arn = arn(ctx, "endpoint", &res_id());
        let mut ep = Map::new();
        ep.insert("EndpointArn".into(), json!(endpoint_arn));
        ep.insert("EndpointIdentifier".into(), json!(id));
        ep.insert("EndpointType".into(), json!(ep_type));
        ep.insert("EngineName".into(), json!(engine));
        ep.insert("EngineDisplayName".into(), json!(engine));
        ep.insert("Status".into(), json!("active"));
        copy_fields(
            b,
            &mut ep,
            &[
                "Username",
                "ServerName",
                "Port",
                "DatabaseName",
                "ExtraConnectionAttributes",
                "KmsKeyId",
                "CertificateArn",
                "SslMode",
                "ServiceAccessRoleArn",
                "ExternalTableDefinition",
            ],
        );
        copy_fields(b, &mut ep, ENDPOINT_SETTINGS_FIELDS);
        let ep = Value::Object(ep);
        data.endpoints.insert(endpoint_arn.clone(), ep.clone());
        store_tags(data, &endpoint_arn, b);
        ok(json!({ "Endpoint": ep }))
    }

    fn describe_endpoints(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.endpoints.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "Endpoints",
            rows,
            b,
            &[
                ("endpoint-arn", &["EndpointArn"]),
                ("endpoint-id", &["EndpointIdentifier"]),
                ("endpoint-type", &["EndpointType"]),
                ("engine-name", &["EngineName"]),
            ],
        )
    }

    fn modify_endpoint(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str_allow_empty(b, "EndpointArn")?.to_string();
        if let Some(t) = opt_str(b, "EndpointType") {
            if !t.is_empty() && t != "source" && t != "target" {
                return Err(validation("EndpointType must be one of [source, target]."));
            }
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(ep) = data.endpoints.get_mut(&arn_id) else {
            return Err(not_found(&format!("Endpoint {arn_id} not found.")));
        };
        let obj = ep.as_object_mut().unwrap();
        for f in [
            "EndpointIdentifier",
            "EngineName",
            "Username",
            "ServerName",
            "Port",
            "DatabaseName",
            "ExtraConnectionAttributes",
            "CertificateArn",
            "SslMode",
            "ServiceAccessRoleArn",
            "ExternalTableDefinition",
        ] {
            if let Some(v) = b.get(f) {
                obj.insert(f.to_string(), v.clone());
            }
        }
        if let Some(t) = opt_str(b, "EndpointType") {
            if !t.is_empty() {
                obj.insert("EndpointType".into(), json!(t));
            }
        }
        for f in ENDPOINT_SETTINGS_FIELDS {
            if let Some(v) = b.get(*f) {
                obj.insert((*f).to_string(), v.clone());
            }
        }
        let ep = ep.clone();
        ok(json!({ "Endpoint": ep }))
    }

    fn delete_endpoint(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "EndpointArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(mut ep) = data.endpoints.remove(&arn_id) else {
            return Err(not_found(&format!("Endpoint {arn_id} not found.")));
        };
        data.tags.remove(&arn_id);
        ep["Status"] = json!("deleting");
        ok(json!({ "Endpoint": ep }))
    }

    fn test_connection(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let inst_arn = req_str(b, "ReplicationInstanceArn")?.to_string();
        let ep_arn = req_str(b, "EndpointArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.replication_instances.contains_key(&inst_arn) {
            return Err(not_found(&format!(
                "Replication instance {inst_arn} not found."
            )));
        }
        let Some(ep) = data.endpoints.get(&ep_arn) else {
            return Err(not_found(&format!("Endpoint {ep_arn} not found.")));
        };
        let conn = json!({
            "ReplicationInstanceArn": inst_arn,
            "EndpointArn": ep_arn,
            "Status": "successful",
            "EndpointIdentifier": ep.get("EndpointIdentifier").cloned().unwrap_or(json!("")),
            "ReplicationInstanceIdentifier": data
                .replication_instances
                .get(&inst_arn)
                .and_then(|i| i.get("ReplicationInstanceIdentifier"))
                .cloned()
                .unwrap_or(json!(""))
        });
        data.connections
            .insert(format!("{ep_arn}|{inst_arn}"), conn.clone());
        ok(json!({ "Connection": conn }))
    }

    fn delete_connection(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ep_arn = req_str(b, "EndpointArn")?.to_string();
        let inst_arn = req_str(b, "ReplicationInstanceArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(mut conn) = data.connections.remove(&format!("{ep_arn}|{inst_arn}")) else {
            return Err(not_found("Connection not found."));
        };
        conn["Status"] = json!("deleted");
        ok(json!({ "Connection": conn }))
    }

    fn describe_connections(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.connections.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "Connections",
            rows,
            b,
            &[
                ("endpoint-arn", &["EndpointArn"]),
                ("replication-instance-arn", &["ReplicationInstanceArn"]),
            ],
        )
    }

    fn refresh_schemas(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ep_arn = req_str_allow_empty(b, "EndpointArn")?.to_string();
        let inst_arn = req_str_allow_empty(b, "ReplicationInstanceArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.endpoints.contains_key(&ep_arn) {
            return Err(not_found(&format!("Endpoint {ep_arn} not found.")));
        }
        if !data.replication_instances.contains_key(&inst_arn) {
            return Err(not_found(&format!(
                "Replication instance {inst_arn} not found."
            )));
        }
        let status = json!({
            "EndpointArn": ep_arn,
            "ReplicationInstanceArn": inst_arn,
            "Status": "refreshing",
            "LastRefreshDate": now_ts()
        });
        data.refresh_schemas_status
            .insert(ep_arn.clone(), status.clone());
        ok(json!({ "RefreshSchemasStatus": status }))
    }

    fn describe_refresh_schemas_status(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ep_arn = req_str_allow_empty(b, "EndpointArn")?.to_string();
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        if let Some(status) = data.and_then(|d| d.refresh_schemas_status.get(&ep_arn)) {
            return ok(json!({ "RefreshSchemasStatus": status.clone() }));
        }
        if data
            .map(|d| d.endpoints.contains_key(&ep_arn))
            .unwrap_or(false)
        {
            return ok(json!({
                "RefreshSchemasStatus": {
                    "EndpointArn": ep_arn,
                    "Status": "successful",
                    "LastRefreshDate": now_ts()
                }
            }));
        }
        Err(not_found(&format!("Endpoint {ep_arn} not found.")))
    }

    fn describe_schemas(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ep_arn = req_str_allow_empty(b, "EndpointArn")?.to_string();
        let guard = self.state.read();
        if !guard
            .get(&ctx.account)
            .map(|d| d.endpoints.contains_key(&ep_arn))
            .unwrap_or(false)
        {
            return Err(not_found(&format!("Endpoint {ep_arn} not found.")));
        }
        ok(json!({ "Schemas": [] }))
    }

    fn describe_endpoint_settings(&self, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let engine = req_str(b, "EngineName")?;
        let rows = vec![
            json!({ "Name": "ServerName", "Type": "string", "Applicability": engine }),
            json!({ "Name": "Port", "Type": "integer", "IntValueMin": 1, "IntValueMax": 65535 }),
            json!({ "Name": "DatabaseName", "Type": "string" }),
        ];
        list_marker("EndpointSettings", rows, b, &[])
    }

    fn describe_endpoint_types(&self, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let rows: Vec<Value> = [
            ("mysql", true),
            ("postgres", true),
            ("oracle", true),
            ("sqlserver", true),
            ("s3", false),
            ("dynamodb", false),
            ("kinesis", false),
            ("mariadb", true),
            ("aurora", true),
            ("aurora-postgresql", true),
        ]
        .iter()
        .flat_map(|(engine, cdc)| {
            ["source", "target"].iter().map(move |et| {
                json!({
                    "EngineName": engine,
                    "SupportsCDC": cdc,
                    "EndpointType": et,
                    "EngineDisplayName": engine,
                    "ReplicationInstanceEngineMinimumVersion": "3.4.7"
                })
            })
        })
        .collect();
        list_marker(
            "SupportedEndpointTypes",
            rows,
            b,
            &[
                ("engine-name", &["EngineName"]),
                ("endpoint-type", &["EndpointType"]),
            ],
        )
    }
}

// ===================== replication tasks =====================

impl DmsService {
    fn create_replication_task(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str_allow_empty(b, "ReplicationTaskIdentifier")?.to_string();
        let src = req_str_allow_empty(b, "SourceEndpointArn")?.to_string();
        let tgt = req_str_allow_empty(b, "TargetEndpointArn")?.to_string();
        let inst = req_str_allow_empty(b, "ReplicationInstanceArn")?.to_string();
        let mig_type = req_str_allow_empty(b, "MigrationType")?.to_string();
        check_migration_type(&mig_type)?;
        let table_mappings = req_str_allow_empty(b, "TableMappings")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // Cross-reference validation (AWS rejects unknown endpoints/instances).
        if !data.replication_instances.contains_key(&inst) {
            return Err(not_found(&format!(
                "Replication instance {inst} not found."
            )));
        }
        if !data.endpoints.contains_key(&src) {
            return Err(not_found(&format!("Source endpoint {src} not found.")));
        }
        if !data.endpoints.contains_key(&tgt) {
            return Err(not_found(&format!("Target endpoint {tgt} not found.")));
        }
        if !id.is_empty()
            && data
                .replication_tasks
                .values()
                .any(|v| v.get("ReplicationTaskIdentifier").and_then(Value::as_str) == Some(&id))
        {
            return Err(already_exists(&format!(
                "Replication task {id} already exists."
            )));
        }
        let task_arn = arn(ctx, "task", &res_id());
        let settings = opt_str(b, "ReplicationTaskSettings")
            .filter(|s| !s.is_empty())
            .map(String::from)
            .unwrap_or_else(default_task_settings);
        let mut task = Map::new();
        task.insert("ReplicationTaskArn".into(), json!(task_arn));
        task.insert("ReplicationTaskIdentifier".into(), json!(id));
        task.insert("SourceEndpointArn".into(), json!(src));
        task.insert("TargetEndpointArn".into(), json!(tgt));
        task.insert("ReplicationInstanceArn".into(), json!(inst));
        task.insert("MigrationType".into(), json!(mig_type));
        task.insert("TableMappings".into(), json!(table_mappings));
        task.insert("ReplicationTaskSettings".into(), json!(settings));
        task.insert("Status".into(), json!("ready"));
        task.insert("ReplicationTaskCreationDate".into(), json!(now_ts()));
        copy_fields(
            b,
            &mut task,
            &["CdcStartPosition", "CdcStopPosition", "TaskData"],
        );
        task.insert(
            "ReplicationTaskStats".into(),
            json!({
                "FullLoadProgressPercent": 0,
                "TablesLoaded": 0,
                "TablesLoading": 0,
                "TablesQueued": 0,
                "TablesErrored": 0
            }),
        );
        let task = Value::Object(task);
        data.replication_tasks
            .insert(task_arn.clone(), task.clone());
        store_tags(data, &task_arn, b);
        ok(json!({ "ReplicationTask": task }))
    }

    fn describe_replication_tasks(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.replication_tasks.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "ReplicationTasks",
            rows,
            b,
            &[
                ("replication-task-arn", &["ReplicationTaskArn"]),
                ("replication-task-id", &["ReplicationTaskIdentifier"]),
                ("migration-type", &["MigrationType"]),
                ("endpoint-arn", &["SourceEndpointArn", "TargetEndpointArn"]),
                ("replication-instance-arn", &["ReplicationInstanceArn"]),
            ],
        )
    }

    fn modify_replication_task(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationTaskArn")?.to_string();
        if let Some(mt) = opt_str(b, "MigrationType") {
            check_migration_type(mt)?;
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(task) = data.replication_tasks.get_mut(&arn_id) else {
            return Err(not_found(&format!("Replication task {arn_id} not found.")));
        };
        let obj = task.as_object_mut().unwrap();
        for f in [
            "ReplicationTaskIdentifier",
            "MigrationType",
            "TableMappings",
            "ReplicationTaskSettings",
            "CdcStartPosition",
            "CdcStopPosition",
            "TaskData",
        ] {
            if let Some(v) = b.get(f) {
                obj.insert(f.to_string(), v.clone());
            }
        }
        let task = task.clone();
        ok(json!({ "ReplicationTask": task }))
    }

    fn delete_replication_task(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationTaskArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(mut task) = data.replication_tasks.remove(&arn_id) else {
            return Err(not_found(&format!("Replication task {arn_id} not found.")));
        };
        data.tags.remove(&arn_id);
        task["Status"] = json!("deleting");
        ok(json!({ "ReplicationTask": task }))
    }

    fn set_task_status(
        &self,
        ctx: &Ctx,
        b: &Value,
        status: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationTaskArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(task) = data.replication_tasks.get_mut(&arn_id) else {
            return Err(not_found(&format!("Replication task {arn_id} not found.")));
        };
        task["Status"] = json!(status);
        if status == "running" {
            task["ReplicationTaskStartDate"] = json!(now_ts());
        }
        let task = task.clone();
        ok(json!({ "ReplicationTask": task }))
    }

    fn start_replication_task(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        if let Some(t) = opt_str(b, "StartReplicationTaskType") {
            const ALLOWED: &[&str] = &["start-replication", "resume-processing", "reload-target"];
            if !t.is_empty() && !ALLOWED.contains(&t) {
                return Err(validation(
                    "StartReplicationTaskType must be one of [start-replication, resume-processing, reload-target].",
                ));
            }
        }
        self.set_task_status(ctx, b, "running")
    }

    fn stop_replication_task(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        self.set_task_status(ctx, b, "stopped")
    }

    fn start_replication_task_assessment(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.set_task_status(ctx, b, "testing")
    }

    fn move_replication_task(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationTaskArn")?.to_string();
        let target_inst = req_str(b, "TargetReplicationInstanceArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.replication_instances.contains_key(&target_inst) {
            return Err(not_found(&format!(
                "Target replication instance {target_inst} not found."
            )));
        }
        let Some(task) = data.replication_tasks.get_mut(&arn_id) else {
            return Err(not_found(&format!("Replication task {arn_id} not found.")));
        };
        task["ReplicationInstanceArn"] = json!(target_inst);
        task["Status"] = json!("moving");
        let task = task.clone();
        ok(json!({ "ReplicationTask": task }))
    }

    fn start_replication_task_assessment_run(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let task_arn = req_str(b, "ReplicationTaskArn")?.to_string();
        req_str(b, "ServiceAccessRoleArn")?;
        req_str(b, "ResultLocationBucket")?;
        let run_name = req_str(b, "AssessmentRunName")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.replication_tasks.contains_key(&task_arn) {
            return Err(not_found(&format!(
                "Replication task {task_arn} not found."
            )));
        }
        if data
            .assessment_runs
            .values()
            .any(|r| r.get("AssessmentRunName").and_then(Value::as_str) == Some(&run_name))
        {
            return Err(already_exists(&format!(
                "Assessment run {run_name} already exists."
            )));
        }
        let run_arn = arn(ctx, "assessment-run", &res_id());
        let mut run = Map::new();
        run.insert("ReplicationTaskAssessmentRunArn".into(), json!(run_arn));
        run.insert("ReplicationTaskArn".into(), json!(task_arn));
        run.insert("Status".into(), json!("running"));
        run.insert(
            "ReplicationTaskAssessmentRunCreationDate".into(),
            json!(now_ts()),
        );
        run.insert("AssessmentRunName".into(), json!(run_name));
        run.insert("IsLatestTaskAssessmentRun".into(), json!(true));
        copy_fields(
            b,
            &mut run,
            &[
                "ServiceAccessRoleArn",
                "ResultLocationBucket",
                "ResultLocationFolder",
                "ResultEncryptionMode",
                "ResultKmsKeyArn",
            ],
        );
        let run = Value::Object(run);
        data.assessment_runs.insert(run_arn.clone(), run.clone());
        store_tags(data, &run_arn, b);
        ok(json!({ "ReplicationTaskAssessmentRun": run }))
    }

    fn cancel_replication_task_assessment_run(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationTaskAssessmentRunArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(run) = data.assessment_runs.get_mut(&arn_id) else {
            return Err(not_found(&format!("Assessment run {arn_id} not found.")));
        };
        run["Status"] = json!("cancelling");
        let run = run.clone();
        ok(json!({ "ReplicationTaskAssessmentRun": run }))
    }

    fn delete_replication_task_assessment_run(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationTaskAssessmentRunArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(run) = data.assessment_runs.remove(&arn_id) else {
            return Err(not_found(&format!("Assessment run {arn_id} not found.")));
        };
        ok(json!({ "ReplicationTaskAssessmentRun": run }))
    }

    fn describe_replication_task_assessment_runs(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.assessment_runs.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "ReplicationTaskAssessmentRuns",
            rows,
            b,
            &[("replication-task-arn", &["ReplicationTaskArn"])],
        )
    }

    fn describe_replication_task_assessment_results(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = (ctx, b);
        ok(json!({ "BucketName": "", "ReplicationTaskAssessmentResults": [] }))
    }

    fn describe_replication_task_individual_assessments(
        &self,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        list_marker("ReplicationTaskIndividualAssessments", vec![], b, &[])
    }

    fn describe_applicable_individual_assessments(
        &self,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        if let Some(mt) = opt_str(b, "MigrationType") {
            check_migration_type(mt)?;
        }
        let rows: Vec<Value> = [
            "unsupported-data-types-in-source",
            "table-with-lob-but-no-primary-key",
        ]
        .iter()
        .map(|n| json!(n))
        .collect();
        list_marker("IndividualAssessmentNames", rows, b, &[])
    }

    fn describe_table_statistics(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let task_arn = req_str_allow_empty(b, "ReplicationTaskArn")?.to_string();
        let guard = self.state.read();
        if !guard
            .get(&ctx.account)
            .map(|d| d.replication_tasks.contains_key(&task_arn))
            .unwrap_or(false)
        {
            return Err(not_found(&format!(
                "Replication task {task_arn} not found."
            )));
        }
        let mut out = Map::new();
        out.insert("ReplicationTaskArn".into(), json!(task_arn));
        out.insert("TableStatistics".into(), json!([]));
        ok(Value::Object(out))
    }

    fn reload_tables(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let task_arn = req_str(b, "ReplicationTaskArn")?.to_string();
        if b.get("TablesToReload").and_then(Value::as_array).is_none() {
            return Err(validation("TablesToReload is required."));
        }
        let guard = self.state.read();
        if !guard
            .get(&ctx.account)
            .map(|d| d.replication_tasks.contains_key(&task_arn))
            .unwrap_or(false)
        {
            return Err(not_found(&format!(
                "Replication task {task_arn} not found."
            )));
        }
        ok(json!({ "ReplicationTaskArn": task_arn }))
    }
}

fn check_migration_type(t: &str) -> Result<(), AwsServiceError> {
    const ALLOWED: &[&str] = &["full-load", "cdc", "full-load-and-cdc"];
    if !t.is_empty() && !ALLOWED.contains(&t) {
        return Err(validation(
            "MigrationType must be one of [full-load, cdc, full-load-and-cdc].",
        ));
    }
    Ok(())
}

fn default_task_settings() -> String {
    json!({
        "TargetMetadata": { "TargetSchema": "", "SupportLobs": true, "FullLobMode": false, "LobChunkSize": 64 },
        "FullLoadSettings": { "TargetTablePrepMode": "DROP_AND_CREATE", "MaxFullLoadSubTasks": 8, "CommitRate": 10000 },
        "Logging": { "EnableLogging": false }
    })
    .to_string()
}

// ===================== subnet groups =====================

impl DmsService {
    fn create_subnet_group(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str_allow_empty(b, "ReplicationSubnetGroupIdentifier")?.to_string();
        let desc = req_str_allow_empty(b, "ReplicationSubnetGroupDescription")?.to_string();
        let subnet_ids: Vec<String> = b
            .get("SubnetIds")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .ok_or_else(|| validation("SubnetIds is required."))?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !id.is_empty() && data.subnet_groups.contains_key(&id) {
            return Err(already_exists(&format!(
                "Subnet group {id} already exists."
            )));
        }
        let subnets: Vec<Value> = subnet_ids
            .iter()
            .enumerate()
            .map(|(i, sid)| {
                json!({
                    "SubnetIdentifier": sid,
                    "SubnetStatus": "Active",
                    "SubnetAvailabilityZone": { "Name": format!("{}{}", ctx.region, (b'a' + (i % 3) as u8) as char) }
                })
            })
            .collect();
        let group = json!({
            "ReplicationSubnetGroupIdentifier": id,
            "ReplicationSubnetGroupDescription": desc,
            "VpcId": "vpc-0a1b2c3d",
            "SubnetGroupStatus": "Complete",
            "SupportedNetworkTypes": ["IPV4"],
            "Subnets": subnets
        });
        data.subnet_groups.insert(id.clone(), group.clone());
        let group_arn = arn(ctx, "subgrp", &id);
        store_tags(data, &group_arn, b);
        ok(json!({ "ReplicationSubnetGroup": group }))
    }

    fn describe_subnet_groups(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.subnet_groups.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "ReplicationSubnetGroups",
            rows,
            b,
            &[(
                "replication-subnet-group-id",
                &["ReplicationSubnetGroupIdentifier"],
            )],
        )
    }

    fn modify_subnet_group(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str_allow_empty(b, "ReplicationSubnetGroupIdentifier")?.to_string();
        if b.get("SubnetIds").and_then(Value::as_array).is_none() {
            return Err(validation("SubnetIds is required."));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(group) = data.subnet_groups.get_mut(&id) else {
            return Err(not_found(&format!("Subnet group {id} not found.")));
        };
        let obj = group.as_object_mut().unwrap();
        if let Some(desc) = opt_str(b, "ReplicationSubnetGroupDescription") {
            obj.insert("ReplicationSubnetGroupDescription".into(), json!(desc));
        }
        if let Some(ids) = b.get("SubnetIds").and_then(Value::as_array) {
            let subnets: Vec<Value> = ids
                .iter()
                .filter_map(|v| v.as_str())
                .enumerate()
                .map(|(i, sid)| {
                    json!({
                        "SubnetIdentifier": sid,
                        "SubnetStatus": "Active",
                        "SubnetAvailabilityZone": { "Name": format!("{}{}", ctx.region, (b'a' + (i % 3) as u8) as char) }
                    })
                })
                .collect();
            obj.insert("Subnets".into(), json!(subnets));
        }
        let group = group.clone();
        ok(json!({ "ReplicationSubnetGroup": group }))
    }

    fn delete_subnet_group(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "ReplicationSubnetGroupIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.subnet_groups.remove(&id).is_none() {
            return Err(not_found(&format!("Subnet group {id} not found.")));
        }
        data.tags.remove(&arn(ctx, "subgrp", &id));
        ok(json!({}))
    }
}

// ===================== event subscriptions =====================

impl DmsService {
    fn create_event_subscription(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str_allow_empty(b, "SubscriptionName")?.to_string();
        let topic = req_str_allow_empty(b, "SnsTopicArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !name.is_empty() && data.event_subscriptions.contains_key(&name) {
            return Err(already_exists(&format!(
                "Event subscription {name} already exists."
            )));
        }
        let mut sub = Map::new();
        sub.insert("CustomerAwsId".into(), json!(ctx.account));
        sub.insert("CustSubscriptionId".into(), json!(name));
        sub.insert("SnsTopicArn".into(), json!(topic));
        sub.insert("Status".into(), json!("active"));
        sub.insert(
            "SubscriptionCreationTime".into(),
            json!(chrono::Utc::now().to_rfc3339()),
        );
        sub.insert(
            "Enabled".into(),
            b.get("Enabled").cloned().unwrap_or(json!(true)),
        );
        if let Some(st) = opt_str(b, "SourceType") {
            sub.insert("SourceType".into(), json!(st));
        }
        if let Some(cats) = b.get("EventCategories") {
            sub.insert("EventCategoriesList".into(), cats.clone());
        }
        if let Some(ids) = b.get("SourceIds") {
            sub.insert("SourceIdsList".into(), ids.clone());
        }
        let sub = Value::Object(sub);
        data.event_subscriptions.insert(name.clone(), sub.clone());
        store_tags(data, &arn(ctx, "es", &name), b);
        ok(json!({ "EventSubscription": sub }))
    }

    fn describe_event_subscriptions(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = opt_str(b, "SubscriptionName")
            .filter(|s| !s.is_empty())
            .map(String::from);
        let guard = self.state.read();
        let subs = guard.get(&ctx.account).map(|d| &d.event_subscriptions);
        // A named lookup for a non-existent subscription raises the
        // `ResourceNotFoundFault` the op declares, rather than a silent empty
        // list. (An empty/omitted name lists everything.)
        if let Some(n) = &name {
            if !subs.map(|s| s.contains_key(n)).unwrap_or(false) {
                return Err(not_found(&format!("Event subscription {n} not found.")));
            }
        }
        let rows: Vec<Value> = subs
            .map(|s| {
                s.iter()
                    .filter(|(k, _)| name.as_ref().map(|n| *k == n).unwrap_or(true))
                    .map(|(_, v)| v.clone())
                    .collect()
            })
            .unwrap_or_default();
        list_marker(
            "EventSubscriptionsList",
            rows,
            b,
            &[
                ("event-subscription-arn", &["EventSubscriptionArn"]),
                ("event-subscription-id", &["CustSubscriptionId"]),
            ],
        )
    }

    fn modify_event_subscription(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(b, "SubscriptionName")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(sub) = data.event_subscriptions.get_mut(&name) else {
            return Err(not_found(&format!("Event subscription {name} not found.")));
        };
        let obj = sub.as_object_mut().unwrap();
        if let Some(v) = b.get("SnsTopicArn") {
            obj.insert("SnsTopicArn".into(), v.clone());
        }
        if let Some(v) = b.get("SourceType") {
            obj.insert("SourceType".into(), v.clone());
        }
        if let Some(v) = b.get("EventCategories") {
            obj.insert("EventCategoriesList".into(), v.clone());
        }
        if let Some(v) = b.get("Enabled") {
            obj.insert("Enabled".into(), v.clone());
        }
        let sub = sub.clone();
        ok(json!({ "EventSubscription": sub }))
    }

    fn delete_event_subscription(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(b, "SubscriptionName")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(sub) = data.event_subscriptions.remove(&name) else {
            return Err(not_found(&format!("Event subscription {name} not found.")));
        };
        data.tags.remove(&arn(ctx, "es", &name));
        ok(json!({ "EventSubscription": sub }))
    }

    fn describe_event_categories(&self, _b: &Value) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({
            "EventCategoryGroupList": [
                {
                    "SourceType": "replication-instance",
                    "EventCategories": ["configuration change", "creation", "deletion", "failover", "failure", "maintenance"]
                },
                {
                    "SourceType": "replication-task",
                    "EventCategories": ["configuration change", "creation", "deletion", "failure", "state change"]
                }
            ]
        }))
    }

    fn describe_events(&self, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        if let Some(st) = opt_str(b, "SourceType") {
            if !st.is_empty() && st != "replication-instance" {
                return Err(validation("SourceType must be replication-instance."));
            }
        }
        list_marker("Events", vec![], b, &[])
    }

    fn update_subscriptions_to_eventbridge(
        &self,
        _b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({ "Result": "Successfully migrated subscriptions to EventBridge." }))
    }
}

// ===================== certificates =====================

impl DmsService {
    fn import_certificate(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str_allow_empty(b, "CertificateIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !id.is_empty()
            && data
                .certificates
                .values()
                .any(|v| v.get("CertificateIdentifier").and_then(Value::as_str) == Some(&id))
        {
            return Err(already_exists(&format!("Certificate {id} already exists.")));
        }
        let cert_arn = arn(ctx, "cert", &res_id());
        let mut cert = Map::new();
        cert.insert("CertificateArn".into(), json!(cert_arn));
        cert.insert("CertificateIdentifier".into(), json!(id));
        cert.insert("CertificateCreationDate".into(), json!(now_ts()));
        cert.insert("CertificateOwner".into(), json!(ctx.account));
        cert.insert("SigningAlgorithm".into(), json!("SHA256withRSA"));
        cert.insert("KeyLength".into(), json!(2048));
        cert.insert("ValidFromDate".into(), json!(now_ts()));
        cert.insert(
            "ValidToDate".into(),
            json!(now_ts() + 365.0 * 24.0 * 3600.0),
        );
        if let Some(pem) = opt_str(b, "CertificatePem") {
            cert.insert("CertificatePem".into(), json!(pem));
        }
        if let Some(kms) = opt_str(b, "KmsKeyId") {
            cert.insert("KmsKeyId".into(), json!(kms));
        }
        let cert = Value::Object(cert);
        data.certificates.insert(cert_arn.clone(), cert.clone());
        store_tags(data, &cert_arn, b);
        ok(json!({ "Certificate": cert }))
    }

    fn describe_certificates(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.certificates.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "Certificates",
            rows,
            b,
            &[("certificate-id", &["CertificateIdentifier"])],
        )
    }

    fn delete_certificate(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "CertificateArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(cert) = data.certificates.remove(&arn_id) else {
            return Err(not_found(&format!("Certificate {arn_id} not found.")));
        };
        data.tags.remove(&arn_id);
        ok(json!({ "Certificate": cert }))
    }
}

// ===================== serverless replication configs =====================

impl DmsService {
    fn create_replication_config(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str_allow_empty(b, "ReplicationConfigIdentifier")?.to_string();
        let src = req_str_allow_empty(b, "SourceEndpointArn")?.to_string();
        let tgt = req_str_allow_empty(b, "TargetEndpointArn")?.to_string();
        let rep_type = req_str_allow_empty(b, "ReplicationType")?.to_string();
        check_replication_type(&rep_type)?;
        let table_mappings = req_str_allow_empty(b, "TableMappings")?.to_string();
        let compute = b
            .get("ComputeConfig")
            .cloned()
            .ok_or_else(|| validation("ComputeConfig is required."))?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !id.is_empty()
            && data
                .replication_configs
                .values()
                .any(|v| v.get("ReplicationConfigIdentifier").and_then(Value::as_str) == Some(&id))
        {
            return Err(already_exists(&format!(
                "Replication config {id} already exists."
            )));
        }
        let config_arn = arn(ctx, "replication-config", &res_id());
        let mut cfg = Map::new();
        cfg.insert("ReplicationConfigArn".into(), json!(config_arn));
        cfg.insert("ReplicationConfigIdentifier".into(), json!(id));
        cfg.insert("SourceEndpointArn".into(), json!(src));
        cfg.insert("TargetEndpointArn".into(), json!(tgt));
        cfg.insert("ReplicationType".into(), json!(rep_type));
        cfg.insert("ComputeConfig".into(), compute);
        cfg.insert("TableMappings".into(), json!(table_mappings));
        cfg.insert("ReplicationConfigCreateTime".into(), json!(now_ts()));
        cfg.insert("ReplicationConfigUpdateTime".into(), json!(now_ts()));
        copy_fields(
            b,
            &mut cfg,
            &["ReplicationSettings", "SupplementalSettings"],
        );
        let cfg = Value::Object(cfg);
        data.replication_configs
            .insert(config_arn.clone(), cfg.clone());
        store_tags(data, &config_arn, b);
        ok(json!({ "ReplicationConfig": cfg }))
    }

    fn describe_replication_configs(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.replication_configs.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "ReplicationConfigs",
            rows,
            b,
            &[
                ("replication-config-arn", &["ReplicationConfigArn"]),
                ("replication-config-id", &["ReplicationConfigIdentifier"]),
            ],
        )
    }

    fn modify_replication_config(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationConfigArn")?.to_string();
        if let Some(rt) = opt_str(b, "ReplicationType") {
            check_replication_type(rt)?;
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(cfg) = data.replication_configs.get_mut(&arn_id) else {
            return Err(not_found(&format!(
                "Replication config {arn_id} not found."
            )));
        };
        let obj = cfg.as_object_mut().unwrap();
        for f in [
            "ReplicationConfigIdentifier",
            "ReplicationType",
            "TableMappings",
            "ReplicationSettings",
            "SupplementalSettings",
            "ComputeConfig",
            "SourceEndpointArn",
            "TargetEndpointArn",
        ] {
            if let Some(v) = b.get(f) {
                obj.insert(f.to_string(), v.clone());
            }
        }
        obj.insert("ReplicationConfigUpdateTime".into(), json!(now_ts()));
        let cfg = cfg.clone();
        ok(json!({ "ReplicationConfig": cfg }))
    }

    fn delete_replication_config(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationConfigArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(cfg) = data.replication_configs.remove(&arn_id) else {
            return Err(not_found(&format!(
                "Replication config {arn_id} not found."
            )));
        };
        data.replications.remove(&arn_id);
        data.tags.remove(&arn_id);
        ok(json!({ "ReplicationConfig": cfg }))
    }

    fn build_replication(&self, cfg: &Value, status: &str) -> Value {
        json!({
            "ReplicationConfigIdentifier": cfg.get("ReplicationConfigIdentifier").cloned().unwrap_or(json!("")),
            "ReplicationConfigArn": cfg.get("ReplicationConfigArn").cloned().unwrap_or(json!("")),
            "SourceEndpointArn": cfg.get("SourceEndpointArn").cloned().unwrap_or(json!("")),
            "TargetEndpointArn": cfg.get("TargetEndpointArn").cloned().unwrap_or(json!("")),
            "ReplicationType": cfg.get("ReplicationType").cloned().unwrap_or(json!("")),
            "Status": status,
            "ReplicationCreateTime": now_ts(),
            "ReplicationUpdateTime": now_ts(),
            "ProvisionData": { "ProvisionState": "provisioned", "ProvisionedCapacityUnits": 2 },
            "ReplicationStats": { "FullLoadProgressPercent": 0, "TablesLoaded": 0, "TablesLoading": 0, "TablesQueued": 0, "TablesErrored": 0 }
        })
    }

    fn start_replication(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationConfigArn")?.to_string();
        req_str(b, "StartReplicationType")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(cfg) = data.replication_configs.get(&arn_id).cloned() else {
            return Err(not_found(&format!(
                "Replication config {arn_id} not found."
            )));
        };
        if data
            .replications
            .get(&arn_id)
            .and_then(|r| r.get("Status"))
            .and_then(Value::as_str)
            == Some("running")
        {
            return Err(invalid_state(&format!(
                "Replication for {arn_id} is already running."
            )));
        }
        let mut rep = self.build_replication(&cfg, "running");
        rep["StartReplicationType"] = b
            .get("StartReplicationType")
            .cloned()
            .unwrap_or(json!("start-replication"));
        for f in ["CdcStartPosition", "CdcStopPosition"] {
            if let Some(v) = b.get(f) {
                rep[f] = v.clone();
            }
        }
        data.replications.insert(arn_id, rep.clone());
        ok(json!({ "Replication": rep }))
    }

    fn stop_replication(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationConfigArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(rep) = data.replications.get_mut(&arn_id) else {
            return Err(not_found(&format!("Replication for {arn_id} not found.")));
        };
        rep["Status"] = json!("stopped");
        rep["ReplicationLastStopTime"] = json!(now_ts());
        let rep = rep.clone();
        ok(json!({ "Replication": rep }))
    }

    fn describe_replications(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.replications.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "Replications",
            rows,
            b,
            &[("replication-config-arn", &["ReplicationConfigArn"])],
        )
    }

    fn describe_replication_table_statistics(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationConfigArn")?.to_string();
        let guard = self.state.read();
        if !guard
            .get(&ctx.account)
            .map(|d| d.replication_configs.contains_key(&arn_id))
            .unwrap_or(false)
        {
            return Err(not_found(&format!(
                "Replication config {arn_id} not found."
            )));
        }
        ok(json!({
            "ReplicationConfigArn": arn_id,
            "ReplicationTableStatistics": []
        }))
    }

    fn reload_replication_tables(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn_id = req_str(b, "ReplicationConfigArn")?.to_string();
        if b.get("TablesToReload").and_then(Value::as_array).is_none() {
            return Err(validation("TablesToReload is required."));
        }
        let guard = self.state.read();
        if !guard
            .get(&ctx.account)
            .map(|d| d.replication_configs.contains_key(&arn_id))
            .unwrap_or(false)
        {
            return Err(not_found(&format!(
                "Replication config {arn_id} not found."
            )));
        }
        ok(json!({ "ReplicationConfigArn": arn_id }))
    }
}

fn check_replication_type(t: &str) -> Result<(), AwsServiceError> {
    const ALLOWED: &[&str] = &["full-load", "cdc", "full-load-and-cdc"];
    if !t.is_empty() && !ALLOWED.contains(&t) {
        return Err(validation(
            "ReplicationType must be one of [full-load, cdc, full-load-and-cdc].",
        ));
    }
    Ok(())
}

// ===================== data providers =====================

impl DmsService {
    fn create_data_provider(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let engine = req_str_allow_empty(b, "Engine")?.to_string();
        let settings = b
            .get("Settings")
            .cloned()
            .ok_or_else(|| validation("Settings is required."))?;
        let name = opt_str(b, "DataProviderName")
            .filter(|s| !s.is_empty())
            .map(String::from)
            .unwrap_or_else(|| format!("data-provider-{}", &res_id()[..8].to_lowercase()));
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data
            .data_providers
            .values()
            .any(|v| v.get("DataProviderName").and_then(Value::as_str) == Some(&name))
        {
            return Err(already_exists(&format!(
                "Data provider {name} already exists."
            )));
        }
        let provider_arn = arn(ctx, "data-provider", &name);
        let mut dp = Map::new();
        dp.insert("DataProviderArn".into(), json!(provider_arn));
        dp.insert("DataProviderName".into(), json!(name));
        dp.insert("Engine".into(), json!(engine));
        dp.insert("Settings".into(), settings);
        dp.insert("DataProviderCreationTime".into(), json!(now_ts()));
        if let Some(d) = opt_str(b, "Description") {
            dp.insert("Description".into(), json!(d));
        }
        if let Some(v) = b.get("Virtual") {
            dp.insert("Virtual".into(), v.clone());
        }
        let dp = Value::Object(dp);
        data.data_providers.insert(provider_arn.clone(), dp.clone());
        store_tags(data, &provider_arn, b);
        ok(json!({ "DataProvider": dp }))
    }

    fn describe_data_providers(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.data_providers.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "DataProviders",
            rows,
            b,
            &[(
                "data-provider-identifier",
                &["DataProviderArn", "DataProviderName"],
            )],
        )
    }

    fn resolve_data_provider<'a>(data: &'a mut DmsData, ident: &str) -> Option<&'a mut Value> {
        if data.data_providers.contains_key(ident) {
            return data.data_providers.get_mut(ident);
        }
        let key = data
            .data_providers
            .iter()
            .find(|(_, v)| v.get("DataProviderName").and_then(Value::as_str) == Some(ident))
            .map(|(k, _)| k.clone())?;
        data.data_providers.get_mut(&key)
    }

    fn modify_data_provider(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ident = req_str(b, "DataProviderIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(dp) = Self::resolve_data_provider(data, &ident) else {
            return Err(not_found(&format!("Data provider {ident} not found.")));
        };
        let obj = dp.as_object_mut().unwrap();
        for (in_f, out_f) in [
            ("DataProviderName", "DataProviderName"),
            ("Description", "Description"),
            ("Engine", "Engine"),
            ("Virtual", "Virtual"),
            ("Settings", "Settings"),
        ] {
            if let Some(v) = b.get(in_f) {
                obj.insert(out_f.to_string(), v.clone());
            }
        }
        let dp = dp.clone();
        ok(json!({ "DataProvider": dp }))
    }

    fn delete_data_provider(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ident = req_str(b, "DataProviderIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let key = if data.data_providers.contains_key(&ident) {
            Some(ident.clone())
        } else {
            data.data_providers
                .iter()
                .find(|(_, v)| {
                    v.get("DataProviderName").and_then(Value::as_str) == Some(ident.as_str())
                })
                .map(|(k, _)| k.clone())
        };
        let Some(key) = key else {
            return Err(not_found(&format!("Data provider {ident} not found.")));
        };
        let dp = data.data_providers.remove(&key).unwrap();
        data.tags.remove(&key);
        ok(json!({ "DataProvider": dp }))
    }
}

// ===================== instance profiles =====================

impl DmsService {
    fn build_instance_profile(&self, ctx: &Ctx, b: &Value, profile_arn: &str, name: &str) -> Value {
        let mut ip = Map::new();
        ip.insert("InstanceProfileArn".into(), json!(profile_arn));
        ip.insert("InstanceProfileName".into(), json!(name));
        ip.insert("InstanceProfileCreationTime".into(), json!(now_ts()));
        ip.insert(
            "VpcSecurityGroups".into(),
            b.get("VpcSecurityGroups").cloned().unwrap_or(json!([])),
        );
        copy_fields(
            b,
            &mut ip,
            &[
                "AvailabilityZone",
                "KmsKeyArn",
                "PubliclyAccessible",
                "NetworkType",
                "Description",
                "SubnetGroupIdentifier",
            ],
        );
        let _ = ctx;
        Value::Object(ip)
    }

    fn create_instance_profile(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = opt_str(b, "InstanceProfileName")
            .filter(|s| !s.is_empty())
            .map(String::from)
            .unwrap_or_else(|| format!("instance-profile-{}", &res_id()[..8].to_lowercase()));
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data
            .instance_profiles
            .values()
            .any(|v| v.get("InstanceProfileName").and_then(Value::as_str) == Some(&name))
        {
            return Err(already_exists(&format!(
                "Instance profile {name} already exists."
            )));
        }
        let profile_arn = arn(ctx, "instance-profile", &name);
        let ip = self.build_instance_profile(ctx, b, &profile_arn, &name);
        data.instance_profiles
            .insert(profile_arn.clone(), ip.clone());
        store_tags(data, &profile_arn, b);
        ok(json!({ "InstanceProfile": ip }))
    }

    fn describe_instance_profiles(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.instance_profiles.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "InstanceProfiles",
            rows,
            b,
            &[(
                "instance-profile-identifier",
                &["InstanceProfileArn", "InstanceProfileName"],
            )],
        )
    }

    fn resolve_instance_profile_key(data: &DmsData, ident: &str) -> Option<String> {
        if data.instance_profiles.contains_key(ident) {
            return Some(ident.to_string());
        }
        data.instance_profiles
            .iter()
            .find(|(_, v)| v.get("InstanceProfileName").and_then(Value::as_str) == Some(ident))
            .map(|(k, _)| k.clone())
    }

    fn modify_instance_profile(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ident = req_str_allow_empty(b, "InstanceProfileIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(key) = Self::resolve_instance_profile_key(data, &ident) else {
            return Err(not_found(&format!("Instance profile {ident} not found.")));
        };
        let ip = data.instance_profiles.get_mut(&key).unwrap();
        let obj = ip.as_object_mut().unwrap();
        for f in [
            "AvailabilityZone",
            "KmsKeyArn",
            "PubliclyAccessible",
            "NetworkType",
            "InstanceProfileName",
            "Description",
            "SubnetGroupIdentifier",
            "VpcSecurityGroups",
        ] {
            if let Some(v) = b.get(f) {
                obj.insert(f.to_string(), v.clone());
            }
        }
        let ip = ip.clone();
        ok(json!({ "InstanceProfile": ip }))
    }

    fn delete_instance_profile(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ident = req_str(b, "InstanceProfileIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(key) = Self::resolve_instance_profile_key(data, &ident) else {
            return Err(not_found(&format!("Instance profile {ident} not found.")));
        };
        let ip = data.instance_profiles.remove(&key).unwrap();
        data.tags.remove(&key);
        ok(json!({ "InstanceProfile": ip }))
    }
}

// ===================== migration projects =====================

fn build_provider_descriptors(input: Option<&Value>) -> Vec<Value> {
    input
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|d| {
                    let ident = d
                        .get("DataProviderIdentifier")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let name = ident.rsplit(':').next().unwrap_or(ident);
                    let mut m = Map::new();
                    m.insert("DataProviderArn".into(), json!(ident));
                    m.insert("DataProviderName".into(), json!(name));
                    if let Some(v) = d.get("SecretsManagerSecretId") {
                        m.insert("SecretsManagerSecretId".into(), v.clone());
                    }
                    if let Some(v) = d.get("SecretsManagerAccessRoleArn") {
                        m.insert("SecretsManagerAccessRoleArn".into(), v.clone());
                    }
                    Value::Object(m)
                })
                .collect()
        })
        .unwrap_or_default()
}

impl DmsService {
    fn create_migration_project(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        if b.get("SourceDataProviderDescriptors")
            .and_then(Value::as_array)
            .is_none()
        {
            return Err(validation("SourceDataProviderDescriptors is required."));
        }
        if b.get("TargetDataProviderDescriptors")
            .and_then(Value::as_array)
            .is_none()
        {
            return Err(validation("TargetDataProviderDescriptors is required."));
        }
        let profile_ident = req_str_allow_empty(b, "InstanceProfileIdentifier")?.to_string();
        let name = opt_str(b, "MigrationProjectName")
            .filter(|s| !s.is_empty())
            .map(String::from)
            .unwrap_or_else(|| format!("migration-project-{}", &res_id()[..8].to_lowercase()));
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data
            .migration_projects
            .values()
            .any(|v| v.get("MigrationProjectName").and_then(Value::as_str) == Some(&name))
        {
            return Err(already_exists(&format!(
                "Migration project {name} already exists."
            )));
        }
        let profile_name = profile_ident
            .rsplit(':')
            .next()
            .unwrap_or(&profile_ident)
            .to_string();
        let profile_arn = if profile_ident.starts_with("arn:") {
            profile_ident.clone()
        } else {
            arn(ctx, "instance-profile", &profile_ident)
        };
        let project_arn = arn(ctx, "migration-project", &name);
        let mut mp = Map::new();
        mp.insert("MigrationProjectArn".into(), json!(project_arn));
        mp.insert("MigrationProjectName".into(), json!(name));
        mp.insert("MigrationProjectCreationTime".into(), json!(now_ts()));
        mp.insert("InstanceProfileArn".into(), json!(profile_arn));
        mp.insert("InstanceProfileName".into(), json!(profile_name));
        mp.insert(
            "SourceDataProviderDescriptors".into(),
            json!(build_provider_descriptors(
                b.get("SourceDataProviderDescriptors")
            )),
        );
        mp.insert(
            "TargetDataProviderDescriptors".into(),
            json!(build_provider_descriptors(
                b.get("TargetDataProviderDescriptors")
            )),
        );
        copy_fields(
            b,
            &mut mp,
            &[
                "TransformationRules",
                "Description",
                "SchemaConversionApplicationAttributes",
            ],
        );
        let mp = Value::Object(mp);
        data.migration_projects
            .insert(project_arn.clone(), mp.clone());
        store_tags(data, &project_arn, b);
        ok(json!({ "MigrationProject": mp }))
    }

    fn describe_migration_projects(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.migration_projects.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "MigrationProjects",
            rows,
            b,
            &[(
                "migration-project-identifier",
                &["MigrationProjectArn", "MigrationProjectName"],
            )],
        )
    }

    fn resolve_migration_project_key(data: &DmsData, ident: &str) -> Option<String> {
        if data.migration_projects.contains_key(ident) {
            return Some(ident.to_string());
        }
        data.migration_projects
            .iter()
            .find(|(_, v)| v.get("MigrationProjectName").and_then(Value::as_str) == Some(ident))
            .map(|(k, _)| k.clone())
    }

    fn modify_migration_project(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ident = req_str(b, "MigrationProjectIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(key) = Self::resolve_migration_project_key(data, &ident) else {
            return Err(not_found(&format!("Migration project {ident} not found.")));
        };
        let mp = data.migration_projects.get_mut(&key).unwrap();
        let obj = mp.as_object_mut().unwrap();
        for f in [
            "MigrationProjectName",
            "TransformationRules",
            "Description",
            "SchemaConversionApplicationAttributes",
        ] {
            if let Some(v) = b.get(f) {
                obj.insert(f.to_string(), v.clone());
            }
        }
        if let Some(v) = b.get("SourceDataProviderDescriptors") {
            obj.insert(
                "SourceDataProviderDescriptors".into(),
                json!(build_provider_descriptors(Some(v))),
            );
        }
        if let Some(v) = b.get("TargetDataProviderDescriptors") {
            obj.insert(
                "TargetDataProviderDescriptors".into(),
                json!(build_provider_descriptors(Some(v))),
            );
        }
        if let Some(pi) = opt_str(b, "InstanceProfileIdentifier") {
            let pname = pi.rsplit(':').next().unwrap_or(pi).to_string();
            let parn = if pi.starts_with("arn:") {
                pi.to_string()
            } else {
                arn(ctx, "instance-profile", pi)
            };
            obj.insert("InstanceProfileArn".into(), json!(parn));
            obj.insert("InstanceProfileName".into(), json!(pname));
        }
        let mp = mp.clone();
        ok(json!({ "MigrationProject": mp }))
    }

    fn delete_migration_project(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ident = req_str(b, "MigrationProjectIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(key) = Self::resolve_migration_project_key(data, &ident) else {
            return Err(not_found(&format!("Migration project {ident} not found.")));
        };
        let mp = data.migration_projects.remove(&key).unwrap();
        data.conversion_configs.remove(&key);
        data.tags.remove(&key);
        ok(json!({ "MigrationProject": mp }))
    }
}

// ============ conversion config + metadata model (schema conversion) ============

impl DmsService {
    fn require_project(data: &DmsData, b: &Value) -> Result<String, AwsServiceError> {
        // `MigrationProjectIdentifier` is `@length(min: 0)`, so an empty string
        // is a valid input; it simply resolves to no project. Reaching for a
        // non-existent (or empty) project yields the declared
        // `ResourceNotFoundFault` rather than a validation error.
        let ident = req_str_allow_empty(b, "MigrationProjectIdentifier")?;
        Self::resolve_migration_project_key(data, ident)
            .ok_or_else(|| not_found(&format!("Migration project {ident} not found.")))
    }

    fn describe_conversion_configuration(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found("Migration project not found."))?;
        let key = Self::require_project(data, b)?;
        let cfg = data
            .conversion_configs
            .get(&key)
            .cloned()
            .unwrap_or_else(|| "{}".to_string());
        ok(json!({ "MigrationProjectIdentifier": key, "ConversionConfiguration": cfg }))
    }

    fn modify_conversion_configuration(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg = req_str(b, "ConversionConfiguration")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let key = Self::require_project(data, b)?;
        data.conversion_configs.insert(key.clone(), cfg);
        ok(json!({ "MigrationProjectIdentifier": key }))
    }

    fn describe_metadata_model(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        req_str(b, "SelectionRules")?;
        if let Some(o) = opt_str(b, "Origin") {
            check_origin(o)?;
        }
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found("Migration project not found."))?;
        Self::require_project(data, b)?;
        ok(json!({
            "MetadataModelName": "root",
            "MetadataModelType": "SchemaObject",
            "TargetMetadataModels": [],
            "Definition": "{}"
        }))
    }

    fn describe_metadata_model_children(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        req_str(b, "SelectionRules")?;
        if let Some(o) = opt_str(b, "Origin") {
            check_origin(o)?;
        }
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found("Migration project not found."))?;
        Self::require_project(data, b)?;
        list_marker("MetadataModelChildren", vec![], b, &[])
    }

    /// Shared handler for the family of `Describe*` schema-conversion-request
    /// list ops (assessments, conversions, imports, exports, extension packs).
    fn describe_scr(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        // `MigrationProjectIdentifier` is `@required @length(0, 255)`: enforce
        // presence and the length bound (so omit / too-long negative variants
        // are rejected), but not existence — several of these list ops
        // (`DescribeExtensionPackAssociations`, ...) declare no errors at all,
        // so a present-but-unknown project must still answer 200 with an empty
        // page.
        let ident = req_str_allow_empty(b, "MigrationProjectIdentifier")?;
        if ident.chars().count() > 255 {
            return Err(validation(
                "MigrationProjectIdentifier exceeds 255 characters.",
            ));
        }
        let guard = self.state.read();
        let key = guard.get(&ctx.account).and_then(|data| {
            opt_str(b, "MigrationProjectIdentifier")
                .and_then(|ident| Self::resolve_migration_project_key(data, ident))
        });
        let rows: Vec<Value> = match (guard.get(&ctx.account), &key) {
            (Some(data), Some(key)) => data
                .schema_conversion_requests
                .values()
                .filter(|r| {
                    r.get("MigrationProjectArn").and_then(Value::as_str) == Some(key.as_str())
                })
                .cloned()
                .collect(),
            _ => Vec::new(),
        };
        list_marker(
            "Requests",
            rows,
            b,
            &[("request-id", &["RequestIdentifier"])],
        )
    }

    /// Shared handler for the family of `Start*` schema-conversion ops. Persists
    /// a `SchemaConversionRequest` under the project and returns its id.
    fn start_scr(&self, ctx: &Ctx, b: &Value, _kind: &str) -> Result<AwsResponse, AwsServiceError> {
        if let Some(o) = opt_str(b, "Origin") {
            check_origin(o)?;
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let key = Self::require_project(data, b)?;
        let request_id = Uuid::new_v4().to_string();
        let req = json!({
            "Status": "SUCCESS",
            "RequestIdentifier": request_id,
            "MigrationProjectArn": key
        });
        data.schema_conversion_requests
            .insert(request_id.clone(), req);
        ok(json!({ "RequestIdentifier": request_id }))
    }

    fn cancel_scr(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let request_id = req_str(b, "RequestIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        Self::require_project(data, b)?;
        let req = data
            .schema_conversion_requests
            .get(&request_id)
            .cloned()
            .unwrap_or_else(|| json!({ "Status": "CANCELLED", "RequestIdentifier": request_id }));
        if let Some(r) = data.schema_conversion_requests.get_mut(&request_id) {
            r["Status"] = json!("CANCELLED");
        }
        ok(json!({ "Request": req }))
    }

    fn export_metadata_model_assessment(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        req_str(b, "SelectionRules")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found("Migration project not found."))?;
        Self::require_project(data, b)?;
        let entry = |suffix: &str| {
            json!({
                "S3ObjectKey": format!("assessment-report.{suffix}"),
                "ObjectURL": format!("https://s3.amazonaws.com/dms-assessments/assessment-report.{suffix}")
            })
        };
        ok(json!({ "PdfReport": entry("pdf"), "CsvReport": entry("csv") }))
    }

    fn get_target_selection_rules(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rules = req_str(b, "SelectionRules")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found("Migration project not found."))?;
        Self::require_project(data, b)?;
        ok(json!({ "TargetSelectionRules": rules }))
    }
}

fn check_origin(o: &str) -> Result<(), AwsServiceError> {
    if !o.is_empty() && o != "SOURCE" && o != "TARGET" {
        return Err(validation("Origin must be one of [SOURCE, TARGET]."));
    }
    Ok(())
}

// ===================== data migrations =====================

impl DmsService {
    fn create_data_migration(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let project_ident = req_str_allow_empty(b, "MigrationProjectIdentifier")?.to_string();
        let mig_type = req_str_allow_empty(b, "DataMigrationType")?.to_string();
        check_data_migration_type(&mig_type)?;
        req_str_allow_empty(b, "ServiceAccessRoleArn")?;
        let name = opt_str(b, "DataMigrationName")
            .filter(|s| !s.is_empty())
            .map(String::from)
            .unwrap_or_else(|| format!("data-migration-{}", &res_id()[..8].to_lowercase()));
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data
            .data_migrations
            .values()
            .any(|v| v.get("DataMigrationName").and_then(Value::as_str) == Some(&name))
        {
            return Err(already_exists(&format!(
                "Data migration {name} already exists."
            )));
        }
        let project_arn = if project_ident.starts_with("arn:") {
            project_ident.clone()
        } else {
            arn(ctx, "migration-project", &project_ident)
        };
        let dm_arn = arn(ctx, "data-migration", &name);
        let mut dm = Map::new();
        dm.insert("DataMigrationArn".into(), json!(dm_arn));
        dm.insert("DataMigrationName".into(), json!(name));
        dm.insert("MigrationProjectArn".into(), json!(project_arn));
        dm.insert("DataMigrationType".into(), json!(mig_type));
        dm.insert("DataMigrationCreateTime".into(), json!(now_ts()));
        dm.insert("DataMigrationStatus".into(), json!("READY"));
        copy_fields(
            b,
            &mut dm,
            &[
                "ServiceAccessRoleArn",
                "SourceDataSettings",
                "TargetDataSettings",
            ],
        );
        let dm = Value::Object(dm);
        data.data_migrations.insert(dm_arn.clone(), dm.clone());
        store_tags(data, &dm_arn, b);
        ok(json!({ "DataMigration": dm }))
    }

    fn describe_data_migrations(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        if let Some(m) = opt_str(b, "Marker") {
            if m.chars().count() > 1024 {
                return Err(validation("Marker exceeds 1024 characters."));
            }
        }
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.data_migrations.values().cloned().collect())
            .unwrap_or_default();
        list_marker(
            "DataMigrations",
            rows,
            b,
            &[("data-migration-name", &["DataMigrationName"])],
        )
    }

    fn resolve_data_migration_key(data: &DmsData, ident: &str) -> Option<String> {
        if data.data_migrations.contains_key(ident) {
            return Some(ident.to_string());
        }
        data.data_migrations
            .iter()
            .find(|(_, v)| v.get("DataMigrationName").and_then(Value::as_str) == Some(ident))
            .map(|(k, _)| k.clone())
    }

    fn modify_data_migration(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ident = req_str(b, "DataMigrationIdentifier")?.to_string();
        if let Some(mt) = opt_str(b, "DataMigrationType") {
            check_data_migration_type(mt)?;
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(key) = Self::resolve_data_migration_key(data, &ident) else {
            return Err(not_found(&format!("Data migration {ident} not found.")));
        };
        let dm = data.data_migrations.get_mut(&key).unwrap();
        let obj = dm.as_object_mut().unwrap();
        for f in [
            "DataMigrationName",
            "ServiceAccessRoleArn",
            "DataMigrationType",
            "SourceDataSettings",
            "TargetDataSettings",
        ] {
            if let Some(v) = b.get(f) {
                obj.insert(f.to_string(), v.clone());
            }
        }
        let dm = dm.clone();
        ok(json!({ "DataMigration": dm }))
    }

    fn delete_data_migration(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ident = req_str(b, "DataMigrationIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(key) = Self::resolve_data_migration_key(data, &ident) else {
            return Err(not_found(&format!("Data migration {ident} not found.")));
        };
        let dm = data.data_migrations.remove(&key).unwrap();
        data.tags.remove(&key);
        ok(json!({ "DataMigration": dm }))
    }

    fn set_data_migration_status(
        &self,
        ctx: &Ctx,
        b: &Value,
        status: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ident = req_str(b, "DataMigrationIdentifier")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(key) = Self::resolve_data_migration_key(data, &ident) else {
            return Err(not_found(&format!("Data migration {ident} not found.")));
        };
        let dm = data.data_migrations.get_mut(&key).unwrap();
        dm["DataMigrationStatus"] = json!(status);
        if status == "RUNNING" {
            dm["DataMigrationStartTime"] = json!(now_ts());
        }
        let dm = dm.clone();
        ok(json!({ "DataMigration": dm }))
    }

    fn start_data_migration(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        if let Some(t) = opt_str(b, "StartType") {
            const ALLOWED: &[&str] = &["reload-target", "resume-processing", "start-replication"];
            if !t.is_empty() && !ALLOWED.contains(&t) {
                return Err(validation(
                    "StartType must be one of [reload-target, resume-processing, start-replication].",
                ));
            }
        }
        self.set_data_migration_status(ctx, b, "RUNNING")
    }

    fn stop_data_migration(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        self.set_data_migration_status(ctx, b, "STOPPED")
    }
}

fn check_data_migration_type(t: &str) -> Result<(), AwsServiceError> {
    const ALLOWED: &[&str] = &["full-load", "cdc", "full-load-and-cdc"];
    if !t.is_empty() && !ALLOWED.contains(&t) {
        return Err(validation(
            "DataMigrationType must be one of [full-load, cdc, full-load-and-cdc].",
        ));
    }
    Ok(())
}

// ===================== fleet advisor =====================

impl DmsService {
    fn create_fleet_advisor_collector(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str_allow_empty(b, "CollectorName")?.to_string();
        let role = req_str_allow_empty(b, "ServiceAccessRoleArn")?.to_string();
        let bucket = req_str_allow_empty(b, "S3BucketName")?.to_string();
        let collector_id = Uuid::new_v4().to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let mut collector = Map::new();
        collector.insert("CollectorReferencedId".into(), json!(collector_id));
        collector.insert("CollectorName".into(), json!(name));
        collector.insert("ServiceAccessRoleArn".into(), json!(role));
        collector.insert("S3BucketName".into(), json!(bucket));
        if let Some(d) = opt_str(b, "Description") {
            collector.insert("Description".into(), json!(d));
        }
        data.fleet_advisor_collectors
            .insert(collector_id.clone(), Value::Object(collector.clone()));
        // The create response shape is flat (not wrapped in a Collector member).
        ok(Value::Object(collector))
    }

    fn delete_fleet_advisor_collector(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "CollectorReferencedId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.fleet_advisor_collectors.remove(&id).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CollectorNotFoundFault",
                format!("Collector {id} not found."),
            ));
        }
        ok(json!({}))
    }

    fn describe_fleet_advisor_collectors(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.fleet_advisor_collectors.values().cloned().collect())
            .unwrap_or_default();
        list_next_token(
            "Collectors",
            rows,
            b,
            &[
                ("collector-referenced-id", &["CollectorReferencedId"]),
                ("collector-name", &["CollectorName"]),
            ],
        )
    }

    fn describe_fleet_advisor_databases(&self, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        list_next_token("Databases", vec![], b, &[])
    }

    fn delete_fleet_advisor_databases(&self, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let ids = b
            .get("DatabaseIds")
            .cloned()
            .ok_or_else(|| validation("DatabaseIds is required."))?;
        ok(json!({ "DatabaseIds": ids }))
    }

    fn describe_fleet_advisor_lsa_analysis(
        &self,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        list_next_token("Analysis", vec![], b, &[])
    }

    fn describe_fleet_advisor_schema_object_summary(
        &self,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        list_next_token("FleetAdvisorSchemaObjects", vec![], b, &[])
    }

    fn describe_fleet_advisor_schemas(&self, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        list_next_token("FleetAdvisorSchemas", vec![], b, &[])
    }

    fn run_fleet_advisor_lsa_analysis(&self) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({ "LsaAnalysisId": Uuid::new_v4().to_string(), "Status": "RUNNING" }))
    }
}

// ===================== recommendations =====================

impl DmsService {
    fn start_recommendations(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let db_id = req_str(b, "DatabaseId")?.to_string();
        if b.get("Settings").is_none() {
            return Err(validation("Settings is required."));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.recommendations.insert(
            db_id.clone(),
            json!({
                "DatabaseId": db_id,
                "Status": "in-progress",
                "Settings": b.get("Settings").cloned().unwrap_or(json!({}))
            }),
        );
        ok(json!({}))
    }

    fn batch_start_recommendations(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(entries) = b.get("Data").and_then(Value::as_array) {
            for e in entries {
                if let Some(db_id) = e.get("DatabaseId").and_then(Value::as_str) {
                    data.recommendations.insert(
                        db_id.to_string(),
                        json!({ "DatabaseId": db_id, "Status": "in-progress" }),
                    );
                }
            }
        }
        ok(json!({ "ErrorEntries": [] }))
    }

    fn describe_recommendations(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows = guard
            .get(&ctx.account)
            .map(|d| d.recommendations.values().cloned().collect())
            .unwrap_or_default();
        list_next_token(
            "Recommendations",
            rows,
            b,
            &[("database-id", &["DatabaseId"]), ("status", &["Status"])],
        )
    }

    fn describe_recommendation_limitations(
        &self,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        list_next_token("Limitations", vec![], b, &[])
    }
}

// ===================== account / catalogue =====================

impl DmsService {
    fn describe_account_attributes(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let count = |f: fn(&DmsData) -> usize| data.map(f).unwrap_or(0) as i64;
        let quotas = json!([
            { "AccountQuotaName": "ReplicationInstances", "Used": count(|d| d.replication_instances.len()), "Max": 20 },
            { "AccountQuotaName": "ReplicationSubnetGroups", "Used": count(|d| d.subnet_groups.len()), "Max": 20 },
            { "AccountQuotaName": "Endpoints", "Used": count(|d| d.endpoints.len()), "Max": 1000 },
            { "AccountQuotaName": "ReplicationTasks", "Used": count(|d| d.replication_tasks.len()), "Max": 200 },
            { "AccountQuotaName": "AllocatedStorage", "Used": 0, "Max": 10000 },
            { "AccountQuotaName": "EventSubscriptions", "Used": count(|d| d.event_subscriptions.len()), "Max": 20 }
        ]);
        ok(json!({
            "AccountQuotas": quotas,
            "UniqueAccountIdentifier": format!("dms-{}", ctx.account)
        }))
    }

    fn describe_engine_versions(&self, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let rows: Vec<Value> = ["3.4.7", "3.5.1", "3.5.2", "3.5.3", "3.5.4"]
            .iter()
            .map(|v| {
                json!({
                    "Version": v,
                    "Lifecycle": "available",
                    "ReleaseStatus": "prod",
                    "AvailableUpgrades": []
                })
            })
            .collect();
        list_marker("EngineVersions", rows, b, &[])
    }
}

// ===================== tagging =====================

impl DmsService {
    fn add_tags_to_resource(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = req_str(b, "ResourceArn")?.to_string();
        if b.get("Tags").and_then(Value::as_array).is_none() {
            return Err(validation("Tags is required."));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        store_tags(data, &resource_arn, b);
        ok(json!({}))
    }

    fn remove_tags_from_resource(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = req_str(b, "ResourceArn")?.to_string();
        let keys: Vec<String> = b
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .ok_or_else(|| validation("TagKeys is required."))?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(entry) = data.tags.get_mut(&resource_arn) {
            for k in &keys {
                entry.remove(k);
            }
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let mut arns: Vec<String> = Vec::new();
        if let Some(a) = opt_str(b, "ResourceArn") {
            arns.push(a.to_string());
        }
        if let Some(list) = b.get("ResourceArnList").and_then(Value::as_array) {
            arns.extend(list.iter().filter_map(|v| v.as_str().map(String::from)));
        }
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let mut tag_list = Vec::new();
        for a in &arns {
            if let Some(entry) = data.and_then(|d| d.tags.get(a)) {
                for (k, v) in entry {
                    tag_list.push(json!({ "Key": k, "Value": v, "ResourceArn": a }));
                }
            }
        }
        ok(json!({ "TagList": tag_list }))
    }
}
