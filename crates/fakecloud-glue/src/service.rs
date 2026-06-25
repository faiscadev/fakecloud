use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    Column, Database, GlueAccounts, GlueSnapshot, Partition, SerdeInfo, SharedGlueState,
    StorageDescriptor, Table, GLUE_SNAPSHOT_SCHEMA_VERSION,
};

/// Glue read actions all start with one of these verbs; every other action is
/// a mutation (Create/Update/Delete/BatchCreate/BatchDelete/BatchPut/BatchStop/
/// BatchUpdate/Cancel/Import/Modify/Put/Register/Remove/Reset/Resume/Run/Start/
/// Stop/Tag/Untag). Used to decide when a handled request must trigger a
/// persistence snapshot. The inverse formulation guarantees no mutation is
/// ever missed.
fn is_mutating_action(action: &str) -> bool {
    const READ_PREFIXES: &[&str] = &[
        "BatchGet", "Get", "List", "Search", "Query", "Check", "Describe", "Test",
    ];
    !READ_PREFIXES.iter().any(|p| action.starts_with(p))
}

const SUPPORTED_ACTIONS: &[&str] = &[
    "BatchCreatePartition",
    "BatchDeleteConnection",
    "BatchDeletePartition",
    "BatchDeleteTable",
    "BatchDeleteTableVersion",
    "BatchGetBlueprints",
    "BatchGetCrawlers",
    "BatchGetCustomEntityTypes",
    "BatchGetDataQualityResult",
    "BatchGetDevEndpoints",
    "BatchGetJobs",
    "BatchGetPartition",
    "BatchGetTableOptimizer",
    "BatchGetTriggers",
    "BatchGetWorkflows",
    "BatchPutDataQualityStatisticAnnotation",
    "BatchStopJobRun",
    "BatchUpdatePartition",
    "CancelDataQualityRuleRecommendationRun",
    "CancelDataQualityRulesetEvaluationRun",
    "CancelMLTaskRun",
    "CancelStatement",
    "CheckSchemaVersionValidity",
    "CreateBlueprint",
    "CreateCatalog",
    "CreateClassifier",
    "CreateColumnStatisticsTaskSettings",
    "CreateConnection",
    "CreateCrawler",
    "CreateCustomEntityType",
    "CreateDatabase",
    "CreateDataQualityRuleset",
    "CreateDevEndpoint",
    "CreateGlueIdentityCenterConfiguration",
    "CreateIntegration",
    "CreateIntegrationResourceProperty",
    "CreateIntegrationTableProperties",
    "CreateJob",
    "CreateMLTransform",
    "CreatePartition",
    "CreatePartitionIndex",
    "CreateRegistry",
    "CreateSchema",
    "CreateScript",
    "CreateSecurityConfiguration",
    "CreateSession",
    "CreateTable",
    "CreateTableOptimizer",
    "CreateTrigger",
    "CreateUsageProfile",
    "CreateUserDefinedFunction",
    "CreateWorkflow",
    "DeleteBlueprint",
    "DeleteCatalog",
    "DeleteClassifier",
    "DeleteColumnStatisticsForPartition",
    "DeleteColumnStatisticsForTable",
    "DeleteColumnStatisticsTaskSettings",
    "DeleteConnection",
    "DeleteConnectionType",
    "DeleteCrawler",
    "DeleteCustomEntityType",
    "DeleteDatabase",
    "DeleteDataQualityRuleset",
    "DeleteDevEndpoint",
    "DeleteGlueIdentityCenterConfiguration",
    "DeleteIntegration",
    "DeleteIntegrationResourceProperty",
    "DeleteIntegrationTableProperties",
    "DeleteJob",
    "DeleteMLTransform",
    "DeletePartition",
    "DeletePartitionIndex",
    "DeleteRegistry",
    "DeleteResourcePolicy",
    "DeleteSchema",
    "DeleteSchemaVersions",
    "DeleteSecurityConfiguration",
    "DeleteSession",
    "DeleteTable",
    "DeleteTableOptimizer",
    "DeleteTableVersion",
    "DeleteTrigger",
    "DeleteUsageProfile",
    "DeleteUserDefinedFunction",
    "DeleteWorkflow",
    "DescribeConnectionType",
    "DescribeEntity",
    "DescribeInboundIntegrations",
    "DescribeIntegrations",
    "GetBlueprint",
    "GetBlueprintRun",
    "GetBlueprintRuns",
    "GetCatalog",
    "GetCatalogImportStatus",
    "GetCatalogs",
    "GetClassifier",
    "GetClassifiers",
    "GetColumnStatisticsForPartition",
    "GetColumnStatisticsForTable",
    "GetColumnStatisticsTaskRun",
    "GetColumnStatisticsTaskRuns",
    "GetColumnStatisticsTaskSettings",
    "GetConnection",
    "GetConnections",
    "GetCrawler",
    "GetCrawlerMetrics",
    "GetCrawlers",
    "GetCustomEntityType",
    "GetDatabase",
    "GetDatabases",
    "GetDataCatalogEncryptionSettings",
    "GetDashboardUrl",
    "GetDataflowGraph",
    "GetDataQualityModel",
    "GetDataQualityModelResult",
    "GetDataQualityResult",
    "GetDataQualityRuleRecommendationRun",
    "GetDataQualityRuleset",
    "GetDataQualityRulesetEvaluationRun",
    "GetDevEndpoint",
    "GetDevEndpoints",
    "GetEntityRecords",
    "GetGlueIdentityCenterConfiguration",
    "GetIntegrationResourceProperty",
    "GetIntegrationTableProperties",
    "GetJob",
    "GetJobBookmark",
    "GetJobRun",
    "GetJobRuns",
    "GetJobs",
    "GetMapping",
    "GetMaterializedViewRefreshTaskRun",
    "GetMLTaskRun",
    "GetMLTaskRuns",
    "GetMLTransform",
    "GetMLTransforms",
    "GetPartition",
    "GetPartitionIndexes",
    "GetPartitions",
    "GetPlan",
    "GetRegistry",
    "GetResourcePolicies",
    "GetResourcePolicy",
    "GetSchema",
    "GetSchemaByDefinition",
    "GetSchemaVersion",
    "GetSchemaVersionsDiff",
    "GetSecurityConfiguration",
    "GetSecurityConfigurations",
    "GetSession",
    "GetSessionEndpoint",
    "GetStatement",
    "GetTable",
    "GetTableOptimizer",
    "GetTables",
    "GetTableVersion",
    "GetTableVersions",
    "GetTags",
    "GetTrigger",
    "GetTriggers",
    "GetUnfilteredPartitionMetadata",
    "GetUnfilteredPartitionsMetadata",
    "GetUnfilteredTableMetadata",
    "GetUsageProfile",
    "GetUserDefinedFunction",
    "GetUserDefinedFunctions",
    "GetWorkflow",
    "GetWorkflowRun",
    "GetWorkflowRunProperties",
    "GetWorkflowRuns",
    "ImportCatalogToGlue",
    "ListBlueprints",
    "ListColumnStatisticsTaskRuns",
    "ListConnectionTypes",
    "ListCrawlers",
    "ListCrawls",
    "ListCustomEntityTypes",
    "ListDataQualityResults",
    "ListDataQualityRuleRecommendationRuns",
    "ListDataQualityRulesetEvaluationRuns",
    "ListDataQualityRulesets",
    "ListDataQualityStatisticAnnotations",
    "ListDataQualityStatistics",
    "ListDevEndpoints",
    "ListEntities",
    "ListIntegrationResourceProperties",
    "ListJobs",
    "ListMaterializedViewRefreshTaskRuns",
    "ListMLTransforms",
    "ListRegistries",
    "ListSchemas",
    "ListSchemaVersions",
    "ListSessions",
    "ListStatements",
    "ListTableOptimizerRuns",
    "ListTriggers",
    "ListUsageProfiles",
    "ListWorkflows",
    "ModifyIntegration",
    "PutDataCatalogEncryptionSettings",
    "PutDataQualityProfileAnnotation",
    "PutResourcePolicy",
    "PutSchemaVersionMetadata",
    "PutWorkflowRunProperties",
    "QuerySchemaVersionMetadata",
    "RegisterConnectionType",
    "RegisterSchemaVersion",
    "RemoveSchemaVersionMetadata",
    "ResetJobBookmark",
    "ResumeWorkflowRun",
    "RunStatement",
    "SearchTables",
    "StartBlueprintRun",
    "StartColumnStatisticsTaskRun",
    "StartColumnStatisticsTaskRunSchedule",
    "StartCrawler",
    "StartCrawlerSchedule",
    "StartDataQualityRuleRecommendationRun",
    "StartDataQualityRulesetEvaluationRun",
    "StartExportLabelsTaskRun",
    "StartImportLabelsTaskRun",
    "StartJobRun",
    "StartMaterializedViewRefreshTaskRun",
    "StartMLEvaluationTaskRun",
    "StartMLLabelingSetGenerationTaskRun",
    "StartTrigger",
    "StartWorkflowRun",
    "StopColumnStatisticsTaskRun",
    "StopColumnStatisticsTaskRunSchedule",
    "StopCrawler",
    "StopCrawlerSchedule",
    "StopMaterializedViewRefreshTaskRun",
    "StopSession",
    "StopTrigger",
    "StopWorkflowRun",
    "TagResource",
    "TestConnection",
    "UntagResource",
    "UpdateBlueprint",
    "UpdateCatalog",
    "UpdateClassifier",
    "UpdateColumnStatisticsForPartition",
    "UpdateColumnStatisticsForTable",
    "UpdateColumnStatisticsTaskSettings",
    "UpdateConnection",
    "UpdateCrawler",
    "UpdateCrawlerSchedule",
    "UpdateDatabase",
    "UpdateDataQualityRuleset",
    "UpdateDevEndpoint",
    "UpdateGlueIdentityCenterConfiguration",
    "UpdateIntegrationResourceProperty",
    "UpdateIntegrationTableProperties",
    "UpdateJob",
    "UpdateJobFromSourceControl",
    "UpdateMLTransform",
    "UpdatePartition",
    "UpdateRegistry",
    "UpdateSchema",
    "UpdateSourceControlFromJob",
    "UpdateTable",
    "UpdateTableOptimizer",
    "UpdateTrigger",
    "UpdateUsageProfile",
    "UpdateUserDefinedFunction",
    "UpdateWorkflow",
];

pub struct GlueService {
    pub(crate) state: SharedGlueState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl GlueService {
    pub fn new(state: SharedGlueState) -> Self {
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

    pub fn shared_state(&self) -> SharedGlueState {
        Arc::clone(&self.state)
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_glue_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Glue state when invoked, or
    /// `None` in memory mode. The CloudFormation provisioner mutates `state`
    /// directly and uses this to write a CFN-provisioned resource through to
    /// disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_glue_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current Glue state as a snapshot. Offloads the serde + blocking
/// file write to the Tokio blocking pool. Noop when `store` is `None` (memory
/// mode). Shared by `GlueService::save_snapshot` and the CloudFormation
/// provisioner persist hook so both route through the same serialize-and-write
/// path.
pub async fn save_glue_snapshot(
    state: &SharedGlueState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = GlueSnapshot {
        schema_version: GLUE_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write glue snapshot"),
        Err(err) => tracing::error!(%err, "glue snapshot task panicked"),
    }
}

impl Default for GlueService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(GlueAccounts::new())))
    }
}

#[async_trait]
impl AwsService for GlueService {
    fn service_name(&self) -> &str {
        "glue"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Server-side input validation (lengths/ranges/enums) runs before any
        // handler, mirroring AWS's request-validation phase.
        crate::common::validate_constraints(&req.action, &req.json_body())?;
        let mutates = is_mutating_action(&req.action);
        let result = match req.action.as_str() {
            "BatchCreatePartition" => self.batch_create_partition(&req),
            "BatchDeleteConnection" => self.batch_delete_connection(&req),
            "BatchDeletePartition" => self.batch_delete_partition(&req),
            "BatchDeleteTable" => self.batch_delete_table(&req),
            "BatchDeleteTableVersion" => self.batch_delete_table_version(&req),
            "BatchGetBlueprints" => self.batch_get_blueprints(&req),
            "BatchGetCrawlers" => self.batch_get_crawlers(&req),
            "BatchGetCustomEntityTypes" => self.batch_get_custom_entity_types(&req),
            "BatchGetDataQualityResult" => self.batch_get_data_quality_result(&req),
            "BatchGetDevEndpoints" => self.batch_get_dev_endpoints(&req),
            "BatchGetJobs" => self.batch_get_jobs(&req),
            "BatchGetPartition" => self.batch_get_partition(&req),
            "BatchGetTableOptimizer" => self.batch_get_table_optimizer(&req),
            "BatchGetTriggers" => self.batch_get_triggers(&req),
            "BatchGetWorkflows" => self.batch_get_workflows(&req),
            "BatchPutDataQualityStatisticAnnotation" => {
                self.batch_put_data_quality_statistic_annotation(&req)
            }
            "BatchStopJobRun" => self.batch_stop_job_run(&req),
            "BatchUpdatePartition" => self.batch_update_partition(&req),
            "CancelDataQualityRuleRecommendationRun" => {
                self.cancel_data_quality_rule_recommendation_run(&req)
            }
            "CancelDataQualityRulesetEvaluationRun" => {
                self.cancel_data_quality_ruleset_evaluation_run(&req)
            }
            "CancelMLTaskRun" => self.cancel_ml_task_run(&req),
            "CancelStatement" => self.cancel_statement(&req),
            "CheckSchemaVersionValidity" => self.check_schema_version_validity(&req),
            "CreateBlueprint" => self.create_blueprint(&req),
            "CreateCatalog" => self.create_catalog(&req),
            "CreateClassifier" => self.create_classifier(&req),
            "CreateColumnStatisticsTaskSettings" => {
                self.create_column_statistics_task_settings(&req)
            }
            "CreateConnection" => self.create_connection(&req),
            "CreateCrawler" => self.create_crawler(&req),
            "CreateCustomEntityType" => self.create_custom_entity_type(&req),
            "CreateDatabase" => self.create_database(&req),
            "CreateDataQualityRuleset" => self.create_data_quality_ruleset(&req),
            "CreateDevEndpoint" => self.create_dev_endpoint(&req),
            "CreateGlueIdentityCenterConfiguration" => {
                self.create_glue_identity_center_configuration(&req)
            }
            "CreateIntegration" => self.create_integration(&req),
            "CreateIntegrationResourceProperty" => self.create_integration_resource_property(&req),
            "CreateIntegrationTableProperties" => self.create_integration_table_properties(&req),
            "CreateJob" => self.create_job(&req),
            "CreateMLTransform" => self.create_ml_transform(&req),
            "CreatePartition" => self.create_partition(&req),
            "CreatePartitionIndex" => self.create_partition_index(&req),
            "CreateRegistry" => self.create_registry(&req),
            "CreateSchema" => self.create_schema(&req),
            "CreateScript" => self.create_script(&req),
            "CreateSecurityConfiguration" => self.create_security_configuration(&req),
            "CreateSession" => self.create_session(&req),
            "CreateTable" => self.create_table(&req),
            "CreateTableOptimizer" => self.create_table_optimizer(&req),
            "CreateTrigger" => self.create_trigger(&req),
            "CreateUsageProfile" => self.create_usage_profile(&req),
            "CreateUserDefinedFunction" => self.create_user_defined_function(&req),
            "CreateWorkflow" => self.create_workflow(&req),
            "DeleteBlueprint" => self.delete_blueprint(&req),
            "DeleteCatalog" => self.delete_catalog(&req),
            "DeleteClassifier" => self.delete_classifier(&req),
            "DeleteColumnStatisticsForPartition" => {
                self.delete_column_statistics_for_partition(&req)
            }
            "DeleteColumnStatisticsForTable" => self.delete_column_statistics_for_table(&req),
            "DeleteColumnStatisticsTaskSettings" => {
                self.delete_column_statistics_task_settings(&req)
            }
            "DeleteConnection" => self.delete_connection(&req),
            "DeleteConnectionType" => self.delete_connection_type(&req),
            "DeleteCrawler" => self.delete_crawler(&req),
            "DeleteCustomEntityType" => self.delete_custom_entity_type(&req),
            "DeleteDatabase" => self.delete_database(&req),
            "DeleteDataQualityRuleset" => self.delete_data_quality_ruleset(&req),
            "DeleteDevEndpoint" => self.delete_dev_endpoint(&req),
            "DeleteGlueIdentityCenterConfiguration" => {
                self.delete_glue_identity_center_configuration(&req)
            }
            "DeleteIntegration" => self.delete_integration(&req),
            "DeleteIntegrationResourceProperty" => self.delete_integration_resource_property(&req),
            "DeleteIntegrationTableProperties" => self.delete_integration_table_properties(&req),
            "DeleteJob" => self.delete_job(&req),
            "DeleteMLTransform" => self.delete_ml_transform(&req),
            "DeletePartition" => self.delete_partition(&req),
            "DeletePartitionIndex" => self.delete_partition_index(&req),
            "DeleteRegistry" => self.delete_registry(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            "DeleteSchema" => self.delete_schema(&req),
            "DeleteSchemaVersions" => self.delete_schema_versions(&req),
            "DeleteSecurityConfiguration" => self.delete_security_configuration(&req),
            "DeleteSession" => self.delete_session(&req),
            "DeleteTable" => self.delete_table(&req),
            "DeleteTableOptimizer" => self.delete_table_optimizer(&req),
            "DeleteTableVersion" => self.delete_table_version(&req),
            "DeleteTrigger" => self.delete_trigger(&req),
            "DeleteUsageProfile" => self.delete_usage_profile(&req),
            "DeleteUserDefinedFunction" => self.delete_user_defined_function(&req),
            "DeleteWorkflow" => self.delete_workflow(&req),
            "DescribeConnectionType" => self.describe_connection_type(&req),
            "DescribeEntity" => self.describe_entity(&req),
            "DescribeInboundIntegrations" => self.describe_inbound_integrations(&req),
            "DescribeIntegrations" => self.describe_integrations(&req),
            "GetBlueprint" => self.get_blueprint(&req),
            "GetBlueprintRun" => self.get_blueprint_run(&req),
            "GetBlueprintRuns" => self.get_blueprint_runs(&req),
            "GetCatalog" => self.get_catalog(&req),
            "GetCatalogImportStatus" => self.get_catalog_import_status(&req),
            "GetCatalogs" => self.get_catalogs(&req),
            "GetClassifier" => self.get_classifier(&req),
            "GetClassifiers" => self.get_classifiers(&req),
            "GetColumnStatisticsForPartition" => self.get_column_statistics_for_partition(&req),
            "GetColumnStatisticsForTable" => self.get_column_statistics_for_table(&req),
            "GetColumnStatisticsTaskRun" => self.get_column_statistics_task_run(&req),
            "GetColumnStatisticsTaskRuns" => self.get_column_statistics_task_runs(&req),
            "GetColumnStatisticsTaskSettings" => self.get_column_statistics_task_settings(&req),
            "GetConnection" => self.get_connection(&req),
            "GetConnections" => self.get_connections(&req),
            "GetCrawler" => self.get_crawler(&req),
            "GetCrawlerMetrics" => self.get_crawler_metrics(&req),
            "GetCrawlers" => self.get_crawlers(&req),
            "GetCustomEntityType" => self.get_custom_entity_type(&req),
            "GetDatabase" => self.get_database(&req),
            "GetDatabases" => self.get_databases(&req),
            "GetDataCatalogEncryptionSettings" => self.get_data_catalog_encryption_settings(&req),
            "GetDashboardUrl" => self.get_dashboard_url(&req),
            "GetDataflowGraph" => self.get_dataflow_graph(&req),
            "GetDataQualityModel" => self.get_data_quality_model(&req),
            "GetDataQualityModelResult" => self.get_data_quality_model_result(&req),
            "GetDataQualityResult" => self.get_data_quality_result(&req),
            "GetDataQualityRuleRecommendationRun" => {
                self.get_data_quality_rule_recommendation_run(&req)
            }
            "GetDataQualityRuleset" => self.get_data_quality_ruleset(&req),
            "GetDataQualityRulesetEvaluationRun" => {
                self.get_data_quality_ruleset_evaluation_run(&req)
            }
            "GetDevEndpoint" => self.get_dev_endpoint(&req),
            "GetDevEndpoints" => self.get_dev_endpoints(&req),
            "GetEntityRecords" => self.get_entity_records(&req),
            "GetGlueIdentityCenterConfiguration" => {
                self.get_glue_identity_center_configuration(&req)
            }
            "GetIntegrationResourceProperty" => self.get_integration_resource_property(&req),
            "GetIntegrationTableProperties" => self.get_integration_table_properties(&req),
            "GetJob" => self.get_job(&req),
            "GetJobBookmark" => self.get_job_bookmark(&req),
            "GetJobRun" => self.get_job_run(&req),
            "GetJobRuns" => self.get_job_runs(&req),
            "GetJobs" => self.get_jobs(&req),
            "GetMapping" => self.get_mapping(&req),
            "GetMaterializedViewRefreshTaskRun" => {
                self.get_materialized_view_refresh_task_run(&req)
            }
            "GetMLTaskRun" => self.get_ml_task_run(&req),
            "GetMLTaskRuns" => self.get_ml_task_runs(&req),
            "GetMLTransform" => self.get_ml_transform(&req),
            "GetMLTransforms" => self.get_ml_transforms(&req),
            "GetPartition" => self.get_partition(&req),
            "GetPartitionIndexes" => self.get_partition_indexes(&req),
            "GetPartitions" => self.get_partitions(&req),
            "GetPlan" => self.get_plan(&req),
            "GetRegistry" => self.get_registry(&req),
            "GetResourcePolicies" => self.get_resource_policies(&req),
            "GetResourcePolicy" => self.get_resource_policy(&req),
            "GetSchema" => self.get_schema(&req),
            "GetSchemaByDefinition" => self.get_schema_by_definition(&req),
            "GetSchemaVersion" => self.get_schema_version(&req),
            "GetSchemaVersionsDiff" => self.get_schema_versions_diff(&req),
            "GetSecurityConfiguration" => self.get_security_configuration(&req),
            "GetSecurityConfigurations" => self.get_security_configurations(&req),
            "GetSession" => self.get_session(&req),
            "GetSessionEndpoint" => self.get_session_endpoint(&req),
            "GetStatement" => self.get_statement(&req),
            "GetTable" => self.get_table(&req),
            "GetTableOptimizer" => self.get_table_optimizer(&req),
            "GetTables" => self.get_tables(&req),
            "GetTableVersion" => self.get_table_version(&req),
            "GetTableVersions" => self.get_table_versions(&req),
            "GetTags" => self.get_tags(&req),
            "GetTrigger" => self.get_trigger(&req),
            "GetTriggers" => self.get_triggers(&req),
            "GetUnfilteredPartitionMetadata" => self.get_unfiltered_partition_metadata(&req),
            "GetUnfilteredPartitionsMetadata" => self.get_unfiltered_partitions_metadata(&req),
            "GetUnfilteredTableMetadata" => self.get_unfiltered_table_metadata(&req),
            "GetUsageProfile" => self.get_usage_profile(&req),
            "GetUserDefinedFunction" => self.get_user_defined_function(&req),
            "GetUserDefinedFunctions" => self.get_user_defined_functions(&req),
            "GetWorkflow" => self.get_workflow(&req),
            "GetWorkflowRun" => self.get_workflow_run(&req),
            "GetWorkflowRunProperties" => self.get_workflow_run_properties(&req),
            "GetWorkflowRuns" => self.get_workflow_runs(&req),
            "ImportCatalogToGlue" => self.import_catalog_to_glue(&req),
            "ListBlueprints" => self.list_blueprints(&req),
            "ListColumnStatisticsTaskRuns" => self.list_column_statistics_task_runs(&req),
            "ListConnectionTypes" => self.list_connection_types(&req),
            "ListCrawlers" => self.list_crawlers(&req),
            "ListCrawls" => self.list_crawls(&req),
            "ListCustomEntityTypes" => self.list_custom_entity_types(&req),
            "ListDataQualityResults" => self.list_data_quality_results(&req),
            "ListDataQualityRuleRecommendationRuns" => {
                self.list_data_quality_rule_recommendation_runs(&req)
            }
            "ListDataQualityRulesetEvaluationRuns" => {
                self.list_data_quality_ruleset_evaluation_runs(&req)
            }
            "ListDataQualityRulesets" => self.list_data_quality_rulesets(&req),
            "ListDataQualityStatisticAnnotations" => {
                self.list_data_quality_statistic_annotations(&req)
            }
            "ListDataQualityStatistics" => self.list_data_quality_statistics(&req),
            "ListDevEndpoints" => self.list_dev_endpoints(&req),
            "ListEntities" => self.list_entities(&req),
            "ListIntegrationResourceProperties" => self.list_integration_resource_properties(&req),
            "ListJobs" => self.list_jobs(&req),
            "ListMaterializedViewRefreshTaskRuns" => {
                self.list_materialized_view_refresh_task_runs(&req)
            }
            "ListMLTransforms" => self.list_ml_transforms(&req),
            "ListRegistries" => self.list_registries(&req),
            "ListSchemas" => self.list_schemas(&req),
            "ListSchemaVersions" => self.list_schema_versions(&req),
            "ListSessions" => self.list_sessions(&req),
            "ListStatements" => self.list_statements(&req),
            "ListTableOptimizerRuns" => self.list_table_optimizer_runs(&req),
            "ListTriggers" => self.list_triggers(&req),
            "ListUsageProfiles" => self.list_usage_profiles(&req),
            "ListWorkflows" => self.list_workflows(&req),
            "ModifyIntegration" => self.modify_integration(&req),
            "PutDataCatalogEncryptionSettings" => self.put_data_catalog_encryption_settings(&req),
            "PutDataQualityProfileAnnotation" => self.put_data_quality_profile_annotation(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "PutSchemaVersionMetadata" => self.put_schema_version_metadata(&req),
            "PutWorkflowRunProperties" => self.put_workflow_run_properties(&req),
            "QuerySchemaVersionMetadata" => self.query_schema_version_metadata(&req),
            "RegisterConnectionType" => self.register_connection_type(&req),
            "RegisterSchemaVersion" => self.register_schema_version(&req),
            "RemoveSchemaVersionMetadata" => self.remove_schema_version_metadata(&req),
            "ResetJobBookmark" => self.reset_job_bookmark(&req),
            "ResumeWorkflowRun" => self.resume_workflow_run(&req),
            "RunStatement" => self.run_statement(&req),
            "SearchTables" => self.search_tables(&req),
            "StartBlueprintRun" => self.start_blueprint_run(&req),
            "StartColumnStatisticsTaskRun" => self.start_column_statistics_task_run(&req),
            "StartColumnStatisticsTaskRunSchedule" => {
                self.start_column_statistics_task_run_schedule(&req)
            }
            "StartCrawler" => self.start_crawler(&req),
            "StartCrawlerSchedule" => self.start_crawler_schedule(&req),
            "StartDataQualityRuleRecommendationRun" => {
                self.start_data_quality_rule_recommendation_run(&req)
            }
            "StartDataQualityRulesetEvaluationRun" => {
                self.start_data_quality_ruleset_evaluation_run(&req)
            }
            "StartExportLabelsTaskRun" => self.start_export_labels_task_run(&req),
            "StartImportLabelsTaskRun" => self.start_import_labels_task_run(&req),
            "StartJobRun" => self.start_job_run(&req),
            "StartMaterializedViewRefreshTaskRun" => {
                self.start_materialized_view_refresh_task_run(&req)
            }
            "StartMLEvaluationTaskRun" => self.start_ml_evaluation_task_run(&req),
            "StartMLLabelingSetGenerationTaskRun" => {
                self.start_ml_labeling_set_generation_task_run(&req)
            }
            "StartTrigger" => self.start_trigger(&req),
            "StartWorkflowRun" => self.start_workflow_run(&req),
            "StopColumnStatisticsTaskRun" => self.stop_column_statistics_task_run(&req),
            "StopColumnStatisticsTaskRunSchedule" => {
                self.stop_column_statistics_task_run_schedule(&req)
            }
            "StopCrawler" => self.stop_crawler(&req),
            "StopCrawlerSchedule" => self.stop_crawler_schedule(&req),
            "StopMaterializedViewRefreshTaskRun" => {
                self.stop_materialized_view_refresh_task_run(&req)
            }
            "StopSession" => self.stop_session(&req),
            "StopTrigger" => self.stop_trigger(&req),
            "StopWorkflowRun" => self.stop_workflow_run(&req),
            "TagResource" => self.tag_resource(&req),
            "TestConnection" => self.test_connection(&req),
            "UntagResource" => self.untag_resource(&req),
            "UpdateBlueprint" => self.update_blueprint(&req),
            "UpdateCatalog" => self.update_catalog(&req),
            "UpdateClassifier" => self.update_classifier(&req),
            "UpdateColumnStatisticsForPartition" => {
                self.update_column_statistics_for_partition(&req)
            }
            "UpdateColumnStatisticsForTable" => self.update_column_statistics_for_table(&req),
            "UpdateColumnStatisticsTaskSettings" => {
                self.update_column_statistics_task_settings(&req)
            }
            "UpdateConnection" => self.update_connection(&req),
            "UpdateCrawler" => self.update_crawler(&req),
            "UpdateCrawlerSchedule" => self.update_crawler_schedule(&req),
            "UpdateDatabase" => self.update_database(&req),
            "UpdateDataQualityRuleset" => self.update_data_quality_ruleset(&req),
            "UpdateDevEndpoint" => self.update_dev_endpoint(&req),
            "UpdateGlueIdentityCenterConfiguration" => {
                self.update_glue_identity_center_configuration(&req)
            }
            "UpdateIntegrationResourceProperty" => self.update_integration_resource_property(&req),
            "UpdateIntegrationTableProperties" => self.update_integration_table_properties(&req),
            "UpdateJob" => self.update_job(&req),
            "UpdateJobFromSourceControl" => self.update_job_from_source_control(&req),
            "UpdateMLTransform" => self.update_ml_transform(&req),
            "UpdatePartition" => self.update_partition(&req),
            "UpdateRegistry" => self.update_registry(&req),
            "UpdateSchema" => self.update_schema(&req),
            "UpdateSourceControlFromJob" => self.update_source_control_from_job(&req),
            "UpdateTable" => self.update_table(&req),
            "UpdateTableOptimizer" => self.update_table_optimizer(&req),
            "UpdateTrigger" => self.update_trigger(&req),
            "UpdateUsageProfile" => self.update_usage_profile(&req),
            "UpdateUserDefinedFunction" => self.update_user_defined_function(&req),
            "UpdateWorkflow" => self.update_workflow(&req),
            other => Err(AwsServiceError::action_not_implemented("glue", other)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

fn missing(field: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidInputException",
        format!("Missing required field: {field}"),
    )
}

fn entity_not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "EntityNotFoundException",
        msg.into(),
    )
}

fn already_exists(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "AlreadyExistsException",
        msg.into(),
    )
}

pub(crate) fn parse_string_map(val: &Value) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    if let Some(obj) = val.as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                m.insert(k.clone(), s.to_string());
            }
        }
    }
    m
}

fn parse_columns(val: &Value) -> Vec<Column> {
    let Some(arr) = val.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .map(|c| Column {
            name: c["Name"].as_str().unwrap_or_default().to_string(),
            column_type: c["Type"].as_str().unwrap_or_default().to_string(),
            comment: c["Comment"].as_str().map(|s| s.to_string()),
            parameters: parse_string_map(&c["Parameters"]),
        })
        .collect()
}

pub(crate) fn parse_storage_descriptor(val: &Value) -> Option<StorageDescriptor> {
    if !val.is_object() {
        return None;
    }
    let serde_info = if val["SerdeInfo"].is_object() {
        Some(SerdeInfo {
            name: val["SerdeInfo"]["Name"].as_str().map(|s| s.to_string()),
            serialization_library: val["SerdeInfo"]["SerializationLibrary"]
                .as_str()
                .map(|s| s.to_string()),
            parameters: parse_string_map(&val["SerdeInfo"]["Parameters"]),
        })
    } else {
        None
    };
    let bucket_columns = val["BucketColumns"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let sort_columns = val["SortColumns"].as_array().cloned().unwrap_or_default();
    let skewed_info = if val["SkewedInfo"].is_object() {
        Some(val["SkewedInfo"].clone())
    } else {
        None
    };
    Some(StorageDescriptor {
        columns: parse_columns(&val["Columns"]),
        location: val["Location"].as_str().map(|s| s.to_string()),
        input_format: val["InputFormat"].as_str().map(|s| s.to_string()),
        output_format: val["OutputFormat"].as_str().map(|s| s.to_string()),
        compressed: val["Compressed"].as_bool(),
        serde_info,
        parameters: parse_string_map(&val["Parameters"]),
        bucket_columns,
        number_of_buckets: val["NumberOfBuckets"].as_i64(),
        stored_as_sub_directories: val["StoredAsSubDirectories"].as_bool(),
        sort_columns,
        skewed_info,
    })
}

fn columns_json(cols: &[Column]) -> Value {
    Value::Array(
        cols.iter()
            .map(|c| {
                let mut o = json!({"Name": c.name, "Type": c.column_type});
                if let Some(ref cm) = c.comment {
                    o["Comment"] = json!(cm);
                }
                if !c.parameters.is_empty() {
                    o["Parameters"] = json!(c.parameters);
                }
                o
            })
            .collect(),
    )
}

fn storage_descriptor_json(sd: &StorageDescriptor) -> Value {
    let mut o = json!({
        "Columns": columns_json(&sd.columns),
        "Parameters": sd.parameters,
    });
    if let Some(ref l) = sd.location {
        o["Location"] = json!(l);
    }
    if let Some(ref fmt) = sd.input_format {
        o["InputFormat"] = json!(fmt);
    }
    if let Some(ref fmt) = sd.output_format {
        o["OutputFormat"] = json!(fmt);
    }
    if let Some(c) = sd.compressed {
        o["Compressed"] = json!(c);
    }
    if let Some(ref si) = sd.serde_info {
        let mut sj = json!({"Parameters": si.parameters});
        if let Some(ref n) = si.name {
            sj["Name"] = json!(n);
        }
        if let Some(ref l) = si.serialization_library {
            sj["SerializationLibrary"] = json!(l);
        }
        o["SerdeInfo"] = sj;
    }
    if !sd.bucket_columns.is_empty() {
        o["BucketColumns"] = json!(sd.bucket_columns);
    }
    if let Some(n) = sd.number_of_buckets {
        o["NumberOfBuckets"] = json!(n);
    }
    if let Some(b) = sd.stored_as_sub_directories {
        o["StoredAsSubDirectories"] = json!(b);
    }
    if !sd.sort_columns.is_empty() {
        o["SortColumns"] = json!(sd.sort_columns);
    }
    if let Some(ref si) = sd.skewed_info {
        o["SkewedInfo"] = si.clone();
    }
    o
}

fn database_json(db: &Database) -> Value {
    let mut o = json!({
        "Name": db.name,
        "CatalogId": db.catalog_id,
        "Parameters": db.parameters,
        "CreateTime": db.created_at.timestamp() as f64,
    });
    if let Some(ref d) = db.description {
        o["Description"] = json!(d);
    }
    if let Some(ref l) = db.location_uri {
        o["LocationUri"] = json!(l);
    }
    o
}

pub(crate) fn table_json(t: &Table) -> Value {
    let mut o = json!({
        "Name": t.name,
        "DatabaseName": t.database_name,
        "Retention": t.retention,
        "Parameters": t.parameters,
        "PartitionKeys": columns_json(&t.partition_keys),
        "CreateTime": t.create_time.timestamp() as f64,
        "UpdateTime": t.update_time.timestamp() as f64,
    });
    if let Some(ref d) = t.description {
        o["Description"] = json!(d);
    }
    if let Some(ref ow) = t.owner {
        o["Owner"] = json!(ow);
    }
    if let Some(ref tt) = t.table_type {
        o["TableType"] = json!(tt);
    }
    if let Some(ref vot) = t.view_original_text {
        o["ViewOriginalText"] = json!(vot);
    }
    if let Some(ref vet) = t.view_expanded_text {
        o["ViewExpandedText"] = json!(vet);
    }
    if let Some(ref sd) = t.storage_descriptor {
        o["StorageDescriptor"] = storage_descriptor_json(sd);
    }
    if let Some(la) = t.last_access_time {
        o["LastAccessTime"] = json!(la.timestamp() as f64);
    }
    o
}

pub(crate) fn partition_json(p: &Partition) -> Value {
    let mut o = json!({
        "Values": p.values,
        "CatalogId": p.catalog_id,
        "DatabaseName": p.database_name,
        "TableName": p.table_name,
        "Parameters": p.parameters,
        "CreationTime": p.create_time.timestamp() as f64,
    });
    if let Some(la) = p.last_access_time {
        o["LastAccessTime"] = json!(la.timestamp() as f64);
    }
    if let Some(ref sd) = p.storage_descriptor {
        o["StorageDescriptor"] = storage_descriptor_json(sd);
    }
    o
}

pub(crate) fn partition_key(values: &[String]) -> String {
    // Length-prefix each value so partitions whose values contain `/` (or any
    // separator) cannot collide with neighbouring partitions.
    let mut s = String::new();
    for v in values {
        s.push_str(&v.len().to_string());
        s.push(':');
        s.push_str(v);
        s.push('\u{1f}');
    }
    s
}

fn parse_partition_values(json: &Value, field: &str) -> Result<Vec<String>, AwsServiceError> {
    let arr = json.as_array().ok_or_else(|| missing(field))?;
    if arr.is_empty() {
        return Err(missing(field));
    }
    arr.iter()
        .map(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| missing(field))
        })
        .collect()
}

impl GlueService {
    pub(crate) fn create_database(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let input = &body["DatabaseInput"];
        let name = input["Name"]
            .as_str()
            .ok_or_else(|| missing("DatabaseInput.Name"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let dbs = state.dbs_in_mut(&req.region);
        if dbs.contains_key(&name) {
            return Err(already_exists(format!("Database {name} already exists")));
        }
        dbs.insert(
            name.clone(),
            Database {
                name,
                description: input["Description"].as_str().map(|s| s.to_string()),
                location_uri: input["LocationUri"].as_str().map(|s| s.to_string()),
                parameters: parse_string_map(&input["Parameters"]),
                created_at: Utc::now(),
                catalog_id: req.account_id.clone(),
                tables: BTreeMap::new(),
            },
        );
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_database(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| entity_not_found(format!("Database {name} not found")))?;
        let dbs = state
            .dbs_in(&req.region)
            .ok_or_else(|| entity_not_found(format!("Database {name} not found")))?;
        let db = dbs
            .get(name)
            .ok_or_else(|| entity_not_found(format!("Database {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "Database": database_json(db)
        })))
    }

    pub(crate) fn get_databases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let dbs: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .map(|map| map.values().map(database_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({"DatabaseList": dbs})))
    }

    pub(crate) fn update_database(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let input = &body["DatabaseInput"];
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let dbs = state.dbs_in_mut(&req.region);
        let db = dbs
            .get_mut(name)
            .ok_or_else(|| entity_not_found(format!("Database {name} not found")))?;
        if let Some(d) = input["Description"].as_str() {
            db.description = Some(d.to_string());
        }
        if let Some(l) = input["LocationUri"].as_str() {
            db.location_uri = Some(l.to_string());
        }
        if input["Parameters"].is_object() {
            db.parameters = parse_string_map(&input["Parameters"]);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_database(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        if state.dbs_in_mut(&req.region).remove(name).is_none() {
            return Err(entity_not_found(format!("Database {name} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn create_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?
            .to_string();
        let input = &body["TableInput"];
        let name = input["Name"]
            .as_str()
            .ok_or_else(|| missing("TableInput.Name"))?
            .to_string();
        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let dbs = state.dbs_in_mut(&req.region);
        let db = dbs
            .get_mut(&db_name)
            .ok_or_else(|| entity_not_found(format!("Database {db_name} not found")))?;
        if db.tables.contains_key(&name) {
            return Err(already_exists(format!("Table {name} already exists")));
        }
        db.tables.insert(
            name.clone(),
            Table {
                name,
                database_name: db_name,
                description: input["Description"].as_str().map(|s| s.to_string()),
                owner: input["Owner"].as_str().map(|s| s.to_string()),
                create_time: now,
                update_time: now,
                last_access_time: None,
                retention: input["Retention"].as_i64().unwrap_or(0),
                storage_descriptor: parse_storage_descriptor(&input["StorageDescriptor"]),
                partition_keys: parse_columns(&input["PartitionKeys"]),
                view_original_text: input["ViewOriginalText"].as_str().map(|s| s.to_string()),
                view_expanded_text: input["ViewExpandedText"].as_str().map(|s| s.to_string()),
                table_type: input["TableType"].as_str().map(|s| s.to_string()),
                parameters: parse_string_map(&input["Parameters"]),
                partitions: BTreeMap::new(),
            },
        );
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| entity_not_found(format!("Table {name} not found")))?;
        let dbs = state
            .dbs_in(&req.region)
            .ok_or_else(|| entity_not_found(format!("Table {name} not found")))?;
        let db = dbs
            .get(db_name)
            .ok_or_else(|| entity_not_found(format!("Database {db_name} not found")))?;
        let t = db
            .tables
            .get(name)
            .ok_or_else(|| entity_not_found(format!("Table {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({"Table": table_json(t)})))
    }

    pub(crate) fn get_tables(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?;
        let accounts = self.state.read();
        let tables: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .and_then(|dbs| dbs.get(db_name))
            .map(|db| db.tables.values().map(table_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({"TableList": tables})))
    }

    pub(crate) fn update_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?;
        let input = &body["TableInput"];
        let name = input["Name"]
            .as_str()
            .ok_or_else(|| missing("TableInput.Name"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let dbs = state.dbs_in_mut(&req.region);
        let db = dbs
            .get_mut(db_name)
            .ok_or_else(|| entity_not_found(format!("Database {db_name} not found")))?;
        let t = db
            .tables
            .get_mut(name)
            .ok_or_else(|| entity_not_found(format!("Table {name} not found")))?;
        t.update_time = Utc::now();
        if let Some(d) = input["Description"].as_str() {
            t.description = Some(d.to_string());
        }
        if let Some(o) = input["Owner"].as_str() {
            t.owner = Some(o.to_string());
        }
        if let Some(tt) = input["TableType"].as_str() {
            t.table_type = Some(tt.to_string());
        }
        if input["StorageDescriptor"].is_object() {
            t.storage_descriptor = parse_storage_descriptor(&input["StorageDescriptor"]);
        }
        if input["Parameters"].is_object() {
            t.parameters = parse_string_map(&input["Parameters"]);
        }
        if input["PartitionKeys"].is_array() {
            t.partition_keys = parse_columns(&input["PartitionKeys"]);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let dbs = state.dbs_in_mut(&req.region);
        let db = dbs
            .get_mut(db_name)
            .ok_or_else(|| entity_not_found(format!("Database {db_name} not found")))?;
        if db.tables.remove(name).is_none() {
            return Err(entity_not_found(format!("Table {name} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn create_partition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?
            .to_string();
        let table_name = body["TableName"]
            .as_str()
            .ok_or_else(|| missing("TableName"))?
            .to_string();
        let input = &body["PartitionInput"];
        let values = parse_partition_values(&input["Values"], "PartitionInput.Values")?;
        let key = partition_key(&values);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let dbs = state.dbs_in_mut(&req.region);
        let db = dbs
            .get_mut(&db_name)
            .ok_or_else(|| entity_not_found(format!("Database {db_name} not found")))?;
        let table = db
            .tables
            .get_mut(&table_name)
            .ok_or_else(|| entity_not_found(format!("Table {table_name} not found")))?;
        if table.partitions.contains_key(&key) {
            return Err(already_exists(format!("Partition {key} already exists")));
        }
        table.partitions.insert(
            key,
            Partition {
                values,
                catalog_id: body["CatalogId"]
                    .as_str()
                    .unwrap_or(&req.account_id)
                    .to_string(),
                database_name: db_name,
                table_name,
                create_time: Utc::now(),
                last_access_time: None,
                storage_descriptor: parse_storage_descriptor(&input["StorageDescriptor"]),
                parameters: parse_string_map(&input["Parameters"]),
            },
        );
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_partition(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?;
        let table_name = body["TableName"]
            .as_str()
            .ok_or_else(|| missing("TableName"))?;
        let values = parse_partition_values(&body["PartitionValues"], "PartitionValues")?;
        let key = partition_key(&values);
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| entity_not_found("Partition not found"))?;
        let dbs = state
            .dbs_in(&req.region)
            .ok_or_else(|| entity_not_found("Partition not found"))?;
        let db = dbs
            .get(db_name)
            .ok_or_else(|| entity_not_found(format!("Database {db_name} not found")))?;
        let table = db
            .tables
            .get(table_name)
            .ok_or_else(|| entity_not_found(format!("Table {table_name} not found")))?;
        let p = table
            .partitions
            .get(&key)
            .ok_or_else(|| entity_not_found("Partition not found"))?;
        Ok(AwsResponse::ok_json(
            json!({"Partition": partition_json(p)}),
        ))
    }

    pub(crate) fn get_partitions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?;
        let table_name = body["TableName"]
            .as_str()
            .ok_or_else(|| missing("TableName"))?;
        let expression = body["Expression"].as_str().unwrap_or("");
        let accounts = self.state.read();
        let parts: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .and_then(|dbs| dbs.get(db_name))
            .and_then(|db| db.tables.get(table_name))
            .map(|table| {
                table
                    .partitions
                    .values()
                    .filter(|p| {
                        crate::partition_filter::matches(
                            expression,
                            &table.partition_keys,
                            &p.values,
                        )
                    })
                    .map(partition_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({"Partitions": parts})))
    }

    pub(crate) fn update_partition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?;
        let table_name = body["TableName"]
            .as_str()
            .ok_or_else(|| missing("TableName"))?;
        let value_list = parse_partition_values(&body["PartitionValueList"], "PartitionValueList")?;
        let key = partition_key(&value_list);
        let input = &body["PartitionInput"];
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let dbs = state.dbs_in_mut(&req.region);
        let db = dbs
            .get_mut(db_name)
            .ok_or_else(|| entity_not_found(format!("Database {db_name} not found")))?;
        let table = db
            .tables
            .get_mut(table_name)
            .ok_or_else(|| entity_not_found(format!("Table {table_name} not found")))?;
        let part = table
            .partitions
            .get_mut(&key)
            .ok_or_else(|| entity_not_found("Partition not found"))?;
        if input["StorageDescriptor"].is_object() {
            part.storage_descriptor = parse_storage_descriptor(&input["StorageDescriptor"]);
        }
        if input["Parameters"].is_object() {
            part.parameters = parse_string_map(&input["Parameters"]);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_partition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?;
        let table_name = body["TableName"]
            .as_str()
            .ok_or_else(|| missing("TableName"))?;
        let values = parse_partition_values(&body["PartitionValues"], "PartitionValues")?;
        let key = partition_key(&values);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let dbs = state.dbs_in_mut(&req.region);
        let db = dbs
            .get_mut(db_name)
            .ok_or_else(|| entity_not_found(format!("Database {db_name} not found")))?;
        let table = db
            .tables
            .get_mut(table_name)
            .ok_or_else(|| entity_not_found(format!("Table {table_name} not found")))?;
        if table.partitions.remove(&key).is_none() {
            return Err(entity_not_found("Partition not found"));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn batch_get_partition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?;
        let table_name = body["TableName"]
            .as_str()
            .ok_or_else(|| missing("TableName"))?;
        let to_get = body["PartitionsToGet"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let accounts = self.state.read();
        let mut found = Vec::new();
        let mut not_found = Vec::new();
        let table = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .and_then(|dbs| dbs.get(db_name))
            .and_then(|db| db.tables.get(table_name));
        for pv in &to_get {
            let values = parse_partition_values(&pv["Values"], "PartitionsToGet.Values")?;
            let key = partition_key(&values);
            match table.and_then(|t| t.partitions.get(&key)) {
                Some(p) => found.push(partition_json(p)),
                None => not_found.push(json!({"Values": values})),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "Partitions": found,
            "UnprocessedKeys": not_found,
        })))
    }

    pub(crate) fn batch_create_partition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db_name = body["DatabaseName"]
            .as_str()
            .ok_or_else(|| missing("DatabaseName"))?
            .to_string();
        let table_name = body["TableName"]
            .as_str()
            .ok_or_else(|| missing("TableName"))?
            .to_string();
        let inputs = body["PartitionInputList"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let mut errors = Vec::new();
        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let dbs = state.dbs_in_mut(&req.region);
        let db = dbs
            .get_mut(&db_name)
            .ok_or_else(|| entity_not_found(format!("Database {db_name} not found")))?;
        let table = db
            .tables
            .get_mut(&table_name)
            .ok_or_else(|| entity_not_found(format!("Table {table_name} not found")))?;
        for input in inputs {
            let values = match parse_partition_values(&input["Values"], "PartitionInput.Values") {
                Ok(v) => v,
                Err(_) => {
                    errors.push(json!({
                        "PartitionValues": Vec::<String>::new(),
                        "ErrorDetail": {
                            "ErrorCode": "InvalidInputException",
                            "ErrorMessage": "Values must be a non-empty list of strings",
                        },
                    }));
                    continue;
                }
            };
            let key = partition_key(&values);
            if table.partitions.contains_key(&key) {
                errors.push(json!({
                    "PartitionValues": values,
                    "ErrorDetail": {
                        "ErrorCode": "AlreadyExistsException",
                        "ErrorMessage": format!("Partition {key} already exists"),
                    },
                }));
                continue;
            }
            table.partitions.insert(
                key,
                Partition {
                    values,
                    catalog_id: body["CatalogId"]
                        .as_str()
                        .unwrap_or(&req.account_id)
                        .to_string(),
                    database_name: db_name.clone(),
                    table_name: table_name.clone(),
                    create_time: now,
                    last_access_time: None,
                    storage_descriptor: parse_storage_descriptor(&input["StorageDescriptor"]),
                    parameters: parse_string_map(&input["Parameters"]),
                },
            );
        }
        Ok(AwsResponse::ok_json(json!({"Errors": errors})))
    }
}
