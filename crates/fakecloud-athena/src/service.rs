//! Athena JSON 1.1 service.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::pagination::paginate;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_glue::SharedGlueState;
use fakecloud_s3::SharedS3State;

use crate::sql::{self, ExecutedQuery};
use crate::state::{
    AccountState, AthenaAccounts, Calculation, CapacityAssignmentConfiguration,
    CapacityReservation, DataCatalog, NamedQuery, Notebook, PreparedStatement, QueryExecution,
    Session, SharedAthenaState, WorkGroup,
};

const SUPPORTED_ACTIONS: &[&str] = &[
    "BatchGetNamedQuery",
    "BatchGetPreparedStatement",
    "BatchGetQueryExecution",
    "CancelCapacityReservation",
    "CreateCapacityReservation",
    "CreateDataCatalog",
    "CreateNamedQuery",
    "CreateNotebook",
    "CreatePreparedStatement",
    "CreatePresignedNotebookUrl",
    "CreateWorkGroup",
    "DeleteCapacityReservation",
    "DeleteDataCatalog",
    "DeleteNamedQuery",
    "DeleteNotebook",
    "DeletePreparedStatement",
    "DeleteWorkGroup",
    "ExportNotebook",
    "GetCalculationExecution",
    "GetCalculationExecutionCode",
    "GetCalculationExecutionStatus",
    "GetCapacityAssignmentConfiguration",
    "GetCapacityReservation",
    "GetDatabase",
    "GetDataCatalog",
    "GetNamedQuery",
    "GetNotebookMetadata",
    "GetPreparedStatement",
    "GetQueryExecution",
    "GetQueryResults",
    "GetQueryRuntimeStatistics",
    "GetResourceDashboard",
    "GetSession",
    "GetSessionEndpoint",
    "GetSessionStatus",
    "GetTableMetadata",
    "GetWorkGroup",
    "ImportNotebook",
    "ListApplicationDPUSizes",
    "ListCalculationExecutions",
    "ListCapacityReservations",
    "ListDatabases",
    "ListDataCatalogs",
    "ListEngineVersions",
    "ListExecutors",
    "ListNamedQueries",
    "ListNotebookMetadata",
    "ListNotebookSessions",
    "ListPreparedStatements",
    "ListQueryExecutions",
    "ListSessions",
    "ListTableMetadata",
    "ListTagsForResource",
    "ListWorkGroups",
    "PutCapacityAssignmentConfiguration",
    "StartCalculationExecution",
    "StartQueryExecution",
    "StartSession",
    "StopCalculationExecution",
    "StopQueryExecution",
    "TagResource",
    "TerminateSession",
    "UntagResource",
    "UpdateCapacityReservation",
    "UpdateDataCatalog",
    "UpdateNamedQuery",
    "UpdateNotebook",
    "UpdateNotebookMetadata",
    "UpdatePreparedStatement",
    "UpdateWorkGroup",
];

pub struct AthenaService {
    state: SharedAthenaState,
    glue: Option<SharedGlueState>,
    s3: Option<SharedS3State>,
}

impl AthenaService {
    pub fn new(state: SharedAthenaState) -> Self {
        Self {
            state,
            glue: None,
            s3: None,
        }
    }

    pub fn with_glue(mut self, glue: SharedGlueState) -> Self {
        self.glue = Some(glue);
        self
    }

    pub fn with_s3(mut self, s3: SharedS3State) -> Self {
        self.s3 = Some(s3);
        self
    }

    pub fn shared_state(&self) -> SharedAthenaState {
        Arc::clone(&self.state)
    }
}

impl Default for AthenaService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(AthenaAccounts::new())))
    }
}

#[async_trait]
impl AwsService for AthenaService {
    fn service_name(&self) -> &str {
        "athena"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            // Workgroups
            "CreateWorkGroup" => self.create_work_group(&req),
            "GetWorkGroup" => self.get_work_group(&req),
            "ListWorkGroups" => self.list_work_groups(&req),
            "UpdateWorkGroup" => self.update_work_group(&req),
            "DeleteWorkGroup" => self.delete_work_group(&req),

            // Data catalogs
            "CreateDataCatalog" => self.create_data_catalog(&req),
            "GetDataCatalog" => self.get_data_catalog(&req),
            "ListDataCatalogs" => self.list_data_catalogs(&req),
            "UpdateDataCatalog" => self.update_data_catalog(&req),
            "DeleteDataCatalog" => self.delete_data_catalog(&req),
            "GetDatabase" => self.get_database(&req),
            "ListDatabases" => self.list_databases(&req),
            "GetTableMetadata" => self.get_table_metadata(&req),
            "ListTableMetadata" => self.list_table_metadata(&req),

            // Named queries
            "CreateNamedQuery" => self.create_named_query(&req),
            "GetNamedQuery" => self.get_named_query(&req),
            "ListNamedQueries" => self.list_named_queries(&req),
            "BatchGetNamedQuery" => self.batch_get_named_query(&req),
            "UpdateNamedQuery" => self.update_named_query(&req),
            "DeleteNamedQuery" => self.delete_named_query(&req),

            // Prepared statements
            "CreatePreparedStatement" => self.create_prepared_statement(&req),
            "GetPreparedStatement" => self.get_prepared_statement(&req),
            "ListPreparedStatements" => self.list_prepared_statements(&req),
            "BatchGetPreparedStatement" => self.batch_get_prepared_statement(&req),
            "UpdatePreparedStatement" => self.update_prepared_statement(&req),
            "DeletePreparedStatement" => self.delete_prepared_statement(&req),

            // Query executions
            "StartQueryExecution" => self.start_query_execution(&req),
            "StopQueryExecution" => self.stop_query_execution(&req),
            "GetQueryExecution" => self.get_query_execution(&req),
            "ListQueryExecutions" => self.list_query_executions(&req),
            "BatchGetQueryExecution" => self.batch_get_query_execution(&req),
            "GetQueryResults" => self.get_query_results(&req),
            "GetQueryRuntimeStatistics" => self.get_query_runtime_statistics(&req),

            // Notebooks
            "CreateNotebook" => self.create_notebook(&req),
            "ImportNotebook" => self.import_notebook(&req),
            "ExportNotebook" => self.export_notebook(&req),
            "GetNotebookMetadata" => self.get_notebook_metadata(&req),
            "ListNotebookMetadata" => self.list_notebook_metadata(&req),
            "UpdateNotebook" => self.update_notebook(&req),
            "UpdateNotebookMetadata" => self.update_notebook_metadata(&req),
            "DeleteNotebook" => self.delete_notebook(&req),
            "CreatePresignedNotebookUrl" => self.create_presigned_notebook_url(&req),

            // Sessions / calculations
            "StartSession" => self.start_session(&req),
            "GetSession" => self.get_session(&req),
            "GetSessionStatus" => self.get_session_status(&req),
            "GetSessionEndpoint" => self.get_session_endpoint(&req),
            "ListSessions" => self.list_sessions(&req),
            "ListNotebookSessions" => self.list_notebook_sessions(&req),
            "TerminateSession" => self.terminate_session(&req),
            "StartCalculationExecution" => self.start_calculation_execution(&req),
            "StopCalculationExecution" => self.stop_calculation_execution(&req),
            "GetCalculationExecution" => self.get_calculation_execution(&req),
            "GetCalculationExecutionCode" => self.get_calculation_execution_code(&req),
            "GetCalculationExecutionStatus" => self.get_calculation_execution_status(&req),
            "ListCalculationExecutions" => self.list_calculation_executions(&req),

            // Capacity reservations
            "CreateCapacityReservation" => self.create_capacity_reservation(&req),
            "GetCapacityReservation" => self.get_capacity_reservation(&req),
            "ListCapacityReservations" => self.list_capacity_reservations(&req),
            "UpdateCapacityReservation" => self.update_capacity_reservation(&req),
            "CancelCapacityReservation" => self.cancel_capacity_reservation(&req),
            "DeleteCapacityReservation" => self.delete_capacity_reservation(&req),
            "PutCapacityAssignmentConfiguration" => {
                self.put_capacity_assignment_configuration(&req)
            }
            "GetCapacityAssignmentConfiguration" => {
                self.get_capacity_assignment_configuration(&req)
            }

            // Tags
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),

            // Misc / read-only catalog
            "ListEngineVersions" => self.list_engine_versions(&req),
            "ListApplicationDPUSizes" => self.list_application_dpu_sizes(&req),
            "ListExecutors" => self.list_executors(&req),
            "GetResourceDashboard" => self.get_resource_dashboard(&req),

            other => Err(AwsServiceError::action_not_implemented("athena", other)),
        }
    }
}

// ─── Workgroups ────────────────────────────────────────────────────

impl AthenaService {
    fn create_work_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let configuration = body.get("Configuration").cloned();
        let tags = parse_tags(body.get("Tags"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.work_groups.contains_key(&name) {
            return Err(invalid_request(format!("Workgroup {name} already exists")));
        }
        let wg = WorkGroup {
            name: name.clone(),
            state: "ENABLED".to_string(),
            description,
            configuration,
            creation_time: Utc::now(),
            engine_version: Some("AUTO".to_string()),
        };
        let arn = workgroup_arn(&req.account_id, &req.region, &name);
        account.work_groups.insert(name, wg);
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_work_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "WorkGroup")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let wg = account
            .work_groups
            .get(&name)
            .ok_or_else(|| invalid_request(format!("Workgroup {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "WorkGroup": work_group_json(wg),
        })))
    }

    fn list_work_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let max_results = validate_max_results(&body, 1, 50)?;
        // Smithy: NextToken targets Token @length(1,1024).
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<WorkGroup> = account.work_groups.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let summaries: Vec<Value> = page.iter().map(workgroup_summary_json).collect();
        let mut response = json!({ "WorkGroups": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn update_work_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "WorkGroup")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let wg = account
            .work_groups
            .get_mut(&name)
            .ok_or_else(|| invalid_request(format!("Workgroup {name} not found")))?;
        if let Some(d) = body.get("Description").and_then(Value::as_str) {
            wg.description = Some(d.to_string());
        }
        if let Some(s) = body.get("State").and_then(Value::as_str) {
            wg.state = s.to_string();
        }
        if let Some(c) = body.get("ConfigurationUpdates") {
            wg.configuration = Some(c.clone());
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_work_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "WorkGroup")?;
        let recursive = body
            .get("RecursiveDeleteOption")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if name == "primary" {
            return Err(invalid_request("Cannot delete the primary workgroup"));
        }
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let used_by_query = account
            .query_executions
            .values()
            .any(|q| q.work_group == name);
        let used_by_named = account.named_queries.values().any(|q| q.work_group == name);
        let used_by_prepared = account
            .prepared_statements
            .keys()
            .any(|(wg, _)| wg == &name);
        if !recursive && (used_by_query || used_by_named || used_by_prepared) {
            return Err(invalid_request(format!(
                "Workgroup {name} still has resources; pass RecursiveDeleteOption=true"
            )));
        }
        if account.work_groups.remove(&name).is_none() {
            return Err(invalid_request(format!("Workgroup {name} not found")));
        }
        if recursive {
            account.query_executions.retain(|_, q| q.work_group != name);
            account.named_queries.retain(|_, q| q.work_group != name);
            account.prepared_statements.retain(|(wg, _), _| wg != &name);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}

// ─── Data catalogs ─────────────────────────────────────────────────

impl AthenaService {
    fn create_data_catalog(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: Name targets CatalogNameString @length(min:1, max:256).
        let name = validate_required_string_len(&body, "Name", 1, 256)?;
        let cat_type = require_str(&body, "Type")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let parameters = body
            .get("Parameters")
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let connection_type = body
            .get("ConnectionType")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let tags = parse_tags(body.get("Tags"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.data_catalogs.contains_key(&name) {
            return Err(invalid_request(format!(
                "DataCatalog {name} already exists"
            )));
        }
        let cat = DataCatalog {
            name: name.clone(),
            description,
            cat_type,
            parameters,
            status: "CREATE_COMPLETE".to_string(),
            connection_type,
            error: None,
        };
        let arn = datacatalog_arn(&req.account_id, &req.region, &name);
        account.data_catalogs.insert(name, cat);
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_data_catalog(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cat = account
            .data_catalogs
            .get(&name)
            .ok_or_else(|| invalid_request(format!("DataCatalog {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "DataCatalog": data_catalog_json(cat),
        })))
    }

    fn list_data_catalogs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: MaxResults targets MaxDataCatalogsCount @range(2,50);
        // NextToken targets Token @length(1,1024).
        let max_results = validate_max_results(&body, 2, 50)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<DataCatalog> = account.data_catalogs.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let summaries: Vec<Value> = page
            .iter()
            .map(|c| {
                json!({
                    "CatalogName": c.name,
                    "Type": c.cat_type,
                    "Status": c.status,
                })
            })
            .collect();
        let mut response = json!({ "DataCatalogsSummary": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn update_data_catalog(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let cat_type = require_str(&body, "Type")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cat = account
            .data_catalogs
            .get_mut(&name)
            .ok_or_else(|| invalid_request(format!("DataCatalog {name} not found")))?;
        cat.cat_type = cat_type;
        if description.is_some() {
            cat.description = description;
        }
        if let Some(p) = body.get("Parameters").and_then(Value::as_object) {
            cat.parameters = p
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_data_catalog(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: Name targets CatalogNameString @length(1,256).
        let name = validate_required_string_len(&body, "Name", 1, 256)?;
        if name == "AwsDataCatalog" {
            return Err(invalid_request("Cannot delete the default AwsDataCatalog"));
        }
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.data_catalogs.remove(&name).is_none() {
            return Err(invalid_request(format!("DataCatalog {name} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "DataCatalog": {
                "Name": "",
                "Type": "",
                "Status": "DELETE_COMPLETE",
            }
        })))
    }

    fn get_database(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = require_str(&body, "CatalogName")?;
        let database = require_str(&body, "DatabaseName")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.data_catalogs.contains_key(&catalog) {
            return Err(invalid_request(format!("DataCatalog {catalog} not found")));
        }
        // Resolve via Glue for the default catalog.
        if catalog == "AwsDataCatalog" {
            if let Some(ref glue) = self.glue {
                let glue_state = glue.read();
                if let Some(acct) = glue_state.get(&req.account_id) {
                    if let Some(dbs) = acct.dbs_in(&req.region) {
                        if let Some(db) = dbs.get(&database) {
                            return Ok(AwsResponse::ok_json(json!({
                                "Database": glue_database_json(db),
                            })));
                        }
                    }
                }
                return Err(invalid_request(format!("Database {database} not found")));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "Database": {
                "Name": database,
                "Description": format!("synthesized database for {catalog}"),
                "Parameters": {},
            }
        })))
    }

    fn list_databases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = require_str(&body, "CatalogName")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.data_catalogs.contains_key(&catalog) {
            return Err(invalid_request(format!("DataCatalog {catalog} not found")));
        }
        if catalog == "AwsDataCatalog" {
            if let Some(ref glue) = self.glue {
                let glue_state = glue.read();
                let list: Vec<Value> = glue_state
                    .get(&req.account_id)
                    .and_then(|a| a.dbs_in(&req.region))
                    .map(|dbs| dbs.values().map(glue_database_json).collect())
                    .unwrap_or_default();
                return Ok(AwsResponse::ok_json(json!({ "DatabaseList": list })));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "DatabaseList": [{"Name": "default", "Description": "default database"}],
        })))
    }

    fn get_table_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = require_str(&body, "CatalogName")?;
        let database = require_str(&body, "DatabaseName")?;
        let table = require_str(&body, "TableName")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.data_catalogs.contains_key(&catalog) {
            return Err(invalid_request(format!("DataCatalog {catalog} not found")));
        }
        if catalog == "AwsDataCatalog" {
            if let Some(ref glue) = self.glue {
                let glue_state = glue.read();
                if let Some(acct) = glue_state.get(&req.account_id) {
                    if let Some(dbs) = acct.dbs_in(&req.region) {
                        if let Some(db) = dbs.get(&database) {
                            if let Some(tbl) = db.tables.get(&table) {
                                return Ok(AwsResponse::ok_json(json!({
                                    "TableMetadata": glue_table_metadata_json(tbl),
                                })));
                            }
                        }
                    }
                }
                return Err(invalid_request(format!(
                    "Table {database}.{table} not found"
                )));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "TableMetadata": {
                "Name": table,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"database": database},
                "Columns": [],
                "PartitionKeys": [],
            }
        })))
    }

    fn list_table_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = require_str(&body, "CatalogName")?;
        let database = require_str(&body, "DatabaseName")?;
        let expression = body
            .get("Expression")
            .and_then(Value::as_str)
            .unwrap_or("*")
            .to_string();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.data_catalogs.contains_key(&catalog) {
            return Err(invalid_request(format!("DataCatalog {catalog} not found")));
        }
        if catalog == "AwsDataCatalog" {
            if let Some(ref glue) = self.glue {
                let glue_state = glue.read();
                let list: Vec<Value> = glue_state
                    .get(&req.account_id)
                    .and_then(|a| a.dbs_in(&req.region))
                    .and_then(|dbs| dbs.get(&database))
                    .map(|db| {
                        db.tables
                            .values()
                            .filter(|t| match_table_expression(&t.name, &expression))
                            .map(glue_table_metadata_json)
                            .collect()
                    })
                    .unwrap_or_default();
                return Ok(AwsResponse::ok_json(json!({ "TableMetadataList": list })));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "TableMetadataList": [{
                "Name": "sample",
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"database": database},
                "Columns": [],
                "PartitionKeys": [],
            }]
        })))
    }
}

// ─── Named queries ─────────────────────────────────────────────────

impl AthenaService {
    fn create_named_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: Name=NameString(1,128), Database=DatabaseString(1,255),
        // QueryString=QueryString(1,262144), Description=DescriptionString(1,1024).
        let name = validate_required_string_len(&body, "Name", 1, 128)?;
        let database = validate_required_string_len(&body, "Database", 1, 255)?;
        let query_string = validate_required_string_len(&body, "QueryString", 1, 262144)?;
        validate_opt_string_len(&body, "Description", 1, 1024)?;
        // Smithy: IdempotencyToken @length(min: 32, max: 128).
        validate_opt_string_len(&body, "ClientRequestToken", 32, 128)?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let work_group = body
            .get("WorkGroup")
            .and_then(Value::as_str)
            .unwrap_or("primary")
            .to_string();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.work_groups.contains_key(&work_group) {
            return Err(invalid_request(format!("Workgroup {work_group} not found")));
        }
        let id = synth_uuid();
        account.named_queries.insert(
            id.clone(),
            NamedQuery {
                named_query_id: id.clone(),
                name,
                description,
                database,
                query_string,
                work_group,
                last_used_at: None,
            },
        );
        Ok(AwsResponse::ok_json(json!({ "NamedQueryId": id })))
    }

    fn get_named_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NamedQueryId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .named_queries
            .get(&id)
            .ok_or_else(|| invalid_request(format!("NamedQuery {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "NamedQuery": named_query_json(q),
        })))
    }

    fn list_named_queries(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = body
            .get("WorkGroup")
            .and_then(Value::as_str)
            .unwrap_or("primary")
            .to_string();
        // Smithy: MaxResults targets MaxNamedQueriesCount @range(0,50);
        // NextToken targets Token @length(1,1024).
        let max_results = validate_max_results(&body, 0, 50)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut ids: Vec<String> = account
            .named_queries
            .values()
            .filter(|q| q.work_group == work_group)
            .map(|q| q.named_query_id.clone())
            .collect();
        ids.sort();
        let (page, next) = paginate(&ids, next_token.as_deref(), max_results);
        let mut response = json!({ "NamedQueryIds": page });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn batch_get_named_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: @required NamedQueryIds (@length min:1 max:50).
        validate_required_list(&body, "NamedQueryIds")?;
        validate_list_len(&body, "NamedQueryIds", 1, 50)?;
        let ids = parse_string_list(body.get("NamedQueryIds"));
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for id in ids {
            if let Some(q) = account.named_queries.get(&id) {
                found.push(named_query_json(q));
            } else {
                missing.push(json!({ "NamedQueryId": id, "ErrorCode": "NOT_FOUND", "ErrorMessage": "NamedQuery not found" }));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "NamedQueries": found,
            "UnprocessedNamedQueryIds": missing,
        })))
    }

    fn update_named_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NamedQueryId")?;
        let name = require_str(&body, "Name")?;
        let query_string = require_str(&body, "QueryString")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .named_queries
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("NamedQuery {id} not found")))?;
        q.name = name;
        q.query_string = query_string;
        q.description = description;
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_named_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NamedQueryId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.named_queries.remove(&id).is_none() {
            return Err(invalid_request(format!("NamedQuery {id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}

// ─── Prepared statements ───────────────────────────────────────────

impl AthenaService {
    fn create_prepared_statement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let statement_name = require_str(&body, "StatementName")?;
        let query_statement = require_str(&body, "QueryStatement")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.work_groups.contains_key(&work_group) {
            return Err(invalid_request(format!("Workgroup {work_group} not found")));
        }
        let key = (work_group.clone(), statement_name.clone());
        if account.prepared_statements.contains_key(&key) {
            return Err(invalid_request(format!(
                "PreparedStatement {statement_name} already exists in {work_group}"
            )));
        }
        account.prepared_statements.insert(
            key,
            PreparedStatement {
                statement_name,
                work_group_name: work_group,
                query_statement,
                description,
                last_modified_time: Utc::now(),
            },
        );
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_prepared_statement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let statement_name = require_str(&body, "StatementName")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let ps = account
            .prepared_statements
            .get(&(work_group.clone(), statement_name.clone()))
            .ok_or_else(|| {
                invalid_request(format!(
                    "PreparedStatement {statement_name} not found in {work_group}"
                ))
            })?;
        Ok(AwsResponse::ok_json(json!({
            "PreparedStatement": prepared_statement_json(ps),
        })))
    }

    fn list_prepared_statements(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let max_results = validate_max_results(&body, 1, 50)?;
        // Smithy: NextToken targets Token @length(1,1024).
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut ps: Vec<PreparedStatement> = account
            .prepared_statements
            .iter()
            .filter(|((wg, _), _)| wg == &work_group)
            .map(|(_, p)| p.clone())
            .collect();
        ps.sort_by(|a, b| a.statement_name.cmp(&b.statement_name));
        let (page, next) = paginate(&ps, next_token.as_deref(), max_results);
        let summaries: Vec<Value> = page
            .iter()
            .map(|p| {
                json!({
                    "StatementName": p.statement_name,
                    "LastModifiedTime": p.last_modified_time.timestamp() as f64,
                })
            })
            .collect();
        let mut response = json!({ "PreparedStatements": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn batch_get_prepared_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        // Smithy: @required PreparedStatementNames (@length min:1 max:50).
        validate_required_list(&body, "PreparedStatementNames")?;
        validate_list_len(&body, "PreparedStatementNames", 1, 50)?;
        let names = parse_string_list(body.get("PreparedStatementNames"));
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for name in names {
            if let Some(ps) = account
                .prepared_statements
                .get(&(work_group.clone(), name.clone()))
            {
                found.push(prepared_statement_json(ps));
            } else {
                missing.push(json!({ "StatementName": name, "ErrorCode": "NOT_FOUND", "ErrorMessage": "PreparedStatement not found" }));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "PreparedStatements": found,
            "UnprocessedPreparedStatementNames": missing,
        })))
    }

    fn update_prepared_statement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let statement_name = require_str(&body, "StatementName")?;
        let query_statement = require_str(&body, "QueryStatement")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let ps = account
            .prepared_statements
            .get_mut(&(work_group.clone(), statement_name.clone()))
            .ok_or_else(|| {
                invalid_request(format!(
                    "PreparedStatement {statement_name} not found in {work_group}"
                ))
            })?;
        ps.query_statement = query_statement;
        ps.description = description;
        ps.last_modified_time = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_prepared_statement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let statement_name = require_str(&body, "StatementName")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account
            .prepared_statements
            .remove(&(work_group.clone(), statement_name.clone()))
            .is_none()
        {
            return Err(invalid_request(format!(
                "PreparedStatement {statement_name} not found in {work_group}"
            )));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}

// ─── Query executions ──────────────────────────────────────────────

impl AthenaService {
    fn start_query_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: ClientRequestToken targets IdempotencyToken @length(32,128).
        validate_opt_string_len(&body, "ClientRequestToken", 32, 128)?;
        // QueryString @length(1,262144) is enforced below if the caller provides it directly.
        if let Some(Value::String(s)) = body.get("QueryString") {
            let len = s.chars().count();
            if !(1..=262144).contains(&len) {
                return Err(invalid_request(format!(
                    "QueryString length {len} is outside the valid range 1-262144",
                )));
            }
        }
        let work_group = body
            .get("WorkGroup")
            .and_then(Value::as_str)
            .unwrap_or("primary")
            .to_string();
        let context = body.get("QueryExecutionContext").cloned();
        let result_configuration = body.get("ResultConfiguration").cloned();
        let default_database = context
            .as_ref()
            .and_then(|c| c.get("Database"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let output_location = result_configuration
            .as_ref()
            .and_then(|c| c.get("OutputLocation"))
            .and_then(Value::as_str)
            .map(str::to_owned);

        // Resolve query string from NamedQueryId or QueryString.
        let mut query = if let Some(named_query_id) =
            body.get("NamedQueryId").and_then(Value::as_str)
        {
            let mut state = self.state.write();
            let account = state
                .accounts
                .get_mut(&req.account_id)
                .ok_or_else(|| invalid_request(format!("NamedQuery {named_query_id} not found")))?;
            let nq = account
                .named_queries
                .get_mut(named_query_id)
                .ok_or_else(|| invalid_request(format!("NamedQuery {named_query_id} not found")))?;
            nq.last_used_at = Some(Utc::now());
            nq.query_string.clone()
        } else {
            require_str(&body, "QueryString")?
        };

        // Apply ExecutionParameters substitution.
        if let Some(params) = body.get("ExecutionParameters").and_then(Value::as_array) {
            query = substitute_parameters(&query, params)?;
        }

        // Workgroup existence check before kicking off SQL execution so we
        // surface the same error users hit on real Athena.
        {
            let mut state = self.state.write();
            let account = account_mut(&mut state, &req.account_id);
            if !account.work_groups.contains_key(&work_group) {
                return Err(invalid_request(format!("Workgroup {work_group} not found")));
            }
        }

        let id = synth_uuid();
        let now = Utc::now();

        // Try real SQL execution. Only SELECT is implemented today; anything
        // else lands in QueryExecution with a structured failure reason
        // (state=FAILED, state_change_reason=<error>) so callers see the real
        // outcome instead of a fabricated SUCCEEDED.
        let executed = sql::execute(
            &query,
            default_database.as_deref(),
            output_location.as_deref(),
            &req.account_id,
            &req.region,
            self.glue.as_ref(),
            self.s3.as_ref(),
        );

        let (state_str, state_reason, columns, rows, scanned, output) = match executed {
            Ok(ExecutedQuery {
                columns,
                rows,
                data_scanned_bytes,
                output_location,
            }) => (
                "SUCCEEDED".to_string(),
                None,
                columns,
                rows,
                data_scanned_bytes,
                output_location,
            ),
            Err(err) => {
                tracing::debug!(query = %query, error = %err, "athena: query failed");
                (
                    "FAILED".to_string(),
                    Some(err.to_string()),
                    Vec::new(),
                    Vec::new(),
                    0i64,
                    None,
                )
            }
        };

        // Re-merge the executed output_location back into ResultConfiguration
        // so GetQueryExecution echoes the resolved s3:// key back to the
        // caller (real Athena does this).
        let mut effective_result_config = result_configuration.clone();
        if let Some(ref out) = output {
            let cfg = effective_result_config
                .get_or_insert_with(|| json!({}))
                .as_object_mut();
            if let Some(obj) = cfg {
                obj.insert("OutputLocation".to_string(), Value::String(out.clone()));
            }
        }

        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let qe = QueryExecution {
            query_execution_id: id.clone(),
            query: query.clone(),
            statement_type: classify_statement(&query),
            work_group,
            state: state_str,
            state_change_reason: state_reason,
            submission_time: now,
            completion_time: Some(now),
            query_execution_context: context,
            result_configuration: effective_result_config,
            engine_version: Some(json!({
                "SelectedEngineVersion": "AUTO",
                "EffectiveEngineVersion": "Athena engine version 3",
            })),
            data_scanned_bytes: scanned,
            engine_execution_time_ms: 1,
            query_planning_time_ms: 1,
            total_execution_time_ms: 2,
            result_rows: rows,
            result_columns: columns,
        };
        account.query_executions.insert(id.clone(), qe);
        Ok(AwsResponse::ok_json(json!({ "QueryExecutionId": id })))
    }

    fn stop_query_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "QueryExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .query_executions
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("QueryExecution {id} not found")))?;
        q.state = "CANCELLED".to_string();
        q.state_change_reason = Some("Cancelled by user".to_string());
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_query_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "QueryExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .query_executions
            .get(&id)
            .ok_or_else(|| invalid_request(format!("QueryExecution {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "QueryExecution": query_execution_json(q),
        })))
    }

    fn list_query_executions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = body
            .get("WorkGroup")
            .and_then(Value::as_str)
            .unwrap_or("primary")
            .to_string();
        // Smithy: MaxResults targets MaxQueryExecutionsCount @range(0,50);
        // NextToken targets Token @length(1,1024).
        let max_results = validate_max_results(&body, 0, 50)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<QueryExecution> = account
            .query_executions
            .values()
            .filter(|q| q.work_group == work_group)
            .cloned()
            .collect();
        all.sort_by_key(|q| std::cmp::Reverse(q.submission_time));
        let ids: Vec<String> = all.iter().map(|q| q.query_execution_id.clone()).collect();
        let (page, next) = paginate(&ids, next_token.as_deref(), max_results);
        let mut response = json!({ "QueryExecutionIds": page });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn batch_get_query_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: @required QueryExecutionIds (@length min:1 max:50).
        validate_required_list(&body, "QueryExecutionIds")?;
        validate_list_len(&body, "QueryExecutionIds", 1, 50)?;
        let ids = parse_string_list(body.get("QueryExecutionIds"));
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for id in ids {
            if let Some(q) = account.query_executions.get(&id) {
                found.push(query_execution_json(q));
            } else {
                missing.push(json!({
                    "QueryExecutionId": id,
                    "ErrorCode": "NOT_FOUND",
                    "ErrorMessage": "QueryExecution not found",
                }));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "QueryExecutions": found,
            "UnprocessedQueryExecutionIds": missing,
        })))
    }

    fn get_query_results(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "QueryExecutionId")?;
        let max_results = validate_max_results(&body, 1, 1000)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .query_executions
            .get(&id)
            .ok_or_else(|| invalid_request(format!("QueryExecution {id} not found")))?;
        if q.state != "SUCCEEDED" {
            return Err(invalid_request(format!(
                "Query is in state {} — results unavailable",
                q.state
            )));
        }
        let column_info: Vec<Value> = q
            .result_columns
            .iter()
            .map(|(name, ty)| {
                json!({
                    "CatalogName": "AwsDataCatalog",
                    "SchemaName": "default",
                    "TableName": "",
                    "Name": name,
                    "Label": name,
                    "Type": ty,
                    "Precision": 0,
                    "Scale": 0,
                    "Nullable": "NULLABLE",
                    "CaseSensitive": false,
                })
            })
            .collect();
        let header_row = json!({
            "Data": q.result_columns.iter().map(|(n, _)| json!({"VarCharValue": n})).collect::<Vec<_>>(),
        });
        let mut rows = vec![header_row];
        for row in q.result_rows.iter().take(max_results.saturating_sub(1)) {
            rows.push(json!({
                "Data": row.iter().map(|v| json!({"VarCharValue": v})).collect::<Vec<_>>(),
            }));
        }
        Ok(AwsResponse::ok_json(json!({
            "ResultSet": {
                "Rows": rows,
                "ResultSetMetadata": {"ColumnInfo": column_info},
            },
            "UpdateCount": 0,
        })))
    }

    fn get_query_runtime_statistics(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "QueryExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .query_executions
            .get(&id)
            .ok_or_else(|| invalid_request(format!("QueryExecution {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "QueryRuntimeStatistics": {
                "Timeline": {
                    "QueryQueueTimeInMillis": 0,
                    "QueryPlanningTimeInMillis": q.query_planning_time_ms,
                    "EngineExecutionTimeInMillis": q.engine_execution_time_ms,
                    "ServiceProcessingTimeInMillis": 0,
                    "TotalExecutionTimeInMillis": q.total_execution_time_ms,
                },
                "Rows": {
                    "InputRows": q.result_rows.len() as i64,
                    "InputBytes": q.data_scanned_bytes,
                    "OutputRows": q.result_rows.len() as i64,
                    "OutputBytes": q.data_scanned_bytes,
                },
            }
        })))
    }
}

// ─── Notebooks ─────────────────────────────────────────────────────

impl AthenaService {
    fn create_notebook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let name = require_str(&body, "Name")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.work_groups.contains_key(&work_group) {
            return Err(invalid_request(format!("Workgroup {work_group} not found")));
        }
        let id = synth_uuid();
        account.notebooks.insert(
            id.clone(),
            Notebook {
                notebook_id: id.clone(),
                name,
                work_group,
                creation_time: Utc::now(),
                last_modified_time: Utc::now(),
                payload: String::new(),
                notebook_type: "IPYNB".to_string(),
            },
        );
        Ok(AwsResponse::ok_json(json!({ "NotebookId": id })))
    }

    fn import_notebook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let name = require_str(&body, "Name")?;
        let payload = body
            .get("Payload")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let notebook_type = body
            .get("Type")
            .and_then(Value::as_str)
            .unwrap_or("IPYNB")
            .to_string();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.work_groups.contains_key(&work_group) {
            return Err(invalid_request(format!("Workgroup {work_group} not found")));
        }
        let id = synth_uuid();
        account.notebooks.insert(
            id.clone(),
            Notebook {
                notebook_id: id.clone(),
                name,
                work_group,
                creation_time: Utc::now(),
                last_modified_time: Utc::now(),
                payload,
                notebook_type,
            },
        );
        Ok(AwsResponse::ok_json(json!({ "NotebookId": id })))
    }

    fn export_notebook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NotebookId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let n = account
            .notebooks
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Notebook {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "NotebookMetadata": notebook_metadata_json(n),
            "Payload": n.payload,
        })))
    }

    fn get_notebook_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NotebookId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let n = account
            .notebooks
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Notebook {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "NotebookMetadata": notebook_metadata_json(n),
        })))
    }

    fn list_notebook_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let max_results = validate_max_results(&body, 1, 50)?;
        // Smithy: NextToken targets Token @length(1,1024).
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<Notebook> = account
            .notebooks
            .values()
            .filter(|n| n.work_group == work_group)
            .cloned()
            .collect();
        all.sort_by(|a, b| a.notebook_id.cmp(&b.notebook_id));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let metadatas: Vec<Value> = page.iter().map(notebook_metadata_json).collect();
        let mut response = json!({ "NotebookMetadataList": metadatas });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn update_notebook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NotebookId")?;
        let payload = require_str(&body, "Payload")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let n = account
            .notebooks
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("Notebook {id} not found")))?;
        n.payload = payload;
        n.last_modified_time = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn update_notebook_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NotebookId")?;
        let name = require_str(&body, "Name")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let n = account
            .notebooks
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("Notebook {id} not found")))?;
        n.name = name;
        n.last_modified_time = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_notebook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NotebookId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.notebooks.remove(&id).is_none() {
            return Err(invalid_request(format!("Notebook {id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn create_presigned_notebook_url(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.sessions.contains_key(&session_id) {
            return Err(invalid_request(format!("Session {session_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "NotebookUrl": format!("https://athena-notebook.{}.amazonaws.com/{}", req.region, session_id),
            "AuthToken": synth_uuid(),
            "AuthTokenExpirationTime": (Utc::now().timestamp() + 3600) as f64,
        })))
    }
}

// ─── Sessions / calculations ──────────────────────────────────────

impl AthenaService {
    fn start_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let configuration = body.get("EngineConfiguration").cloned();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.work_groups.contains_key(&work_group) {
            return Err(invalid_request(format!("Workgroup {work_group} not found")));
        }
        let id = synth_uuid();
        account.sessions.insert(
            id.clone(),
            Session {
                session_id: id.clone(),
                work_group,
                description,
                engine_version: Some("PySpark engine version 3".to_string()),
                state: "IDLE".to_string(),
                start_date_time: Utc::now(),
                end_date_time: None,
                idle_since_date_time: Some(Utc::now()),
                configuration,
                notebook_version: Some("Athena notebook version 1".to_string()),
            },
        );
        Ok(AwsResponse::ok_json(json!({
            "SessionId": id,
            "State": "IDLE",
        })))
    }

    fn get_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let s = account
            .sessions
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Session {id} not found")))?;
        Ok(AwsResponse::ok_json(session_detail_json(s)))
    }

    fn get_session_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let s = account
            .sessions
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Session {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "SessionId": s.session_id,
            "Status": session_status_json(s),
        })))
    }

    fn get_session_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.sessions.contains_key(&id) {
            return Err(invalid_request(format!("Session {id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "SessionId": id,
            "EndpointUrl": format!("https://athena-session.{}.amazonaws.com/{}", req.region, id),
            "ExpirationDateTime": (Utc::now().timestamp() + 3600) as f64,
        })))
    }

    fn list_sessions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        // Smithy: MaxResults targets MaxSessionsCount @range(1,100);
        // NextToken targets SessionManagerToken @length(0,2048);
        // StateFilter is SessionState enum.
        let max_results = validate_max_results(&body, 1, 100)?;
        validate_opt_string_len(&body, "NextToken", 0, 2048)?;
        validate_opt_enum(
            &body,
            "StateFilter",
            &[
                "CREATING",
                "CREATED",
                "IDLE",
                "BUSY",
                "TERMINATING",
                "TERMINATED",
                "DEGRADED",
                "FAILED",
            ],
        )?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<Session> = account
            .sessions
            .values()
            .filter(|s| s.work_group == work_group)
            .cloned()
            .collect();
        all.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let summaries: Vec<Value> = page.iter().map(session_summary_json).collect();
        let mut response = json!({ "Sessions": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn list_notebook_sessions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let notebook_id = require_str(&body, "NotebookId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.notebooks.contains_key(&notebook_id) {
            return Err(invalid_request(format!("Notebook {notebook_id} not found")));
        }
        let summaries: Vec<Value> = account
            .sessions
            .values()
            .map(session_summary_json)
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "NotebookSessionsList": summaries,
        })))
    }

    fn terminate_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let s = account
            .sessions
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("Session {id} not found")))?;
        s.state = "TERMINATED".to_string();
        s.end_date_time = Some(Utc::now());
        Ok(AwsResponse::ok_json(json!({ "State": "TERMINATED" })))
    }

    fn start_calculation_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = require_str(&body, "SessionId")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let code_block = body
            .get("CodeBlock")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.sessions.contains_key(&session_id) {
            return Err(invalid_request(format!("Session {session_id} not found")));
        }
        let id = synth_uuid();
        account.calculations.insert(
            id.clone(),
            Calculation {
                calculation_execution_id: id.clone(),
                session_id,
                description,
                state: "COMPLETED".to_string(),
                state_change_reason: None,
                working_directory: Some(format!("s3://athena-calc-results/{}", Uuid::new_v4())),
                code_block,
                submission_date_time: Utc::now(),
                completion_date_time: Some(Utc::now()),
            },
        );
        Ok(AwsResponse::ok_json(json!({
            "CalculationExecutionId": id,
            "State": "COMPLETED",
        })))
    }

    fn stop_calculation_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "CalculationExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let c = account
            .calculations
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("Calculation {id} not found")))?;
        c.state = "CANCELED".to_string();
        c.state_change_reason = Some("Cancelled by user".to_string());
        Ok(AwsResponse::ok_json(json!({ "State": "CANCELED" })))
    }

    fn get_calculation_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "CalculationExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let c = account
            .calculations
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Calculation {id} not found")))?;
        Ok(AwsResponse::ok_json(calculation_detail_json(c)))
    }

    fn get_calculation_execution_code(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "CalculationExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let c = account
            .calculations
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Calculation {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "CodeBlock": c.code_block.clone().unwrap_or_default(),
        })))
    }

    fn get_calculation_execution_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "CalculationExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let c = account
            .calculations
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Calculation {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "Status": calculation_status_json(c),
            "Statistics": {
                "DpuExecutionInMillis": 100,
                "Progress": "100%",
            }
        })))
    }

    fn list_calculation_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: SessionId @length(1,256), NextToken @length(0,2048),
        // StateFilter is CalculationExecutionState enum, MaxResults @range(1,100).
        let session_id = validate_required_string_len(&body, "SessionId", 1, 256)?;
        validate_opt_string_len(&body, "NextToken", 0, 2048)?;
        validate_opt_enum(
            &body,
            "StateFilter",
            &[
                "CREATING",
                "CREATED",
                "QUEUED",
                "RUNNING",
                "CANCELING",
                "CANCELED",
                "COMPLETED",
                "FAILED",
            ],
        )?;
        let max_results = validate_max_results(&body, 1, 100)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<Calculation> = account
            .calculations
            .values()
            .filter(|c| c.session_id == session_id)
            .cloned()
            .collect();
        all.sort_by(|a, b| a.calculation_execution_id.cmp(&b.calculation_execution_id));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let summaries: Vec<Value> = page.iter().map(calculation_summary_json).collect();
        let mut response = json!({ "Calculations": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }
}

// ─── Capacity reservations ────────────────────────────────────────

impl AthenaService {
    fn create_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: Name targets CapacityReservationName @length(min:1, max:128).
        let name = validate_required_string_len(&body, "Name", 1, 128)?;
        let target_dpus =
            body.get("TargetDpus")
                .and_then(Value::as_i64)
                .ok_or_else(|| invalid_request("TargetDpus is required"))? as i32;
        let tags = parse_tags(body.get("Tags"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.capacity_reservations.contains_key(&name) {
            return Err(invalid_request(format!(
                "CapacityReservation {name} already exists"
            )));
        }
        let cr = CapacityReservation {
            name: name.clone(),
            status: "ACTIVE".to_string(),
            target_dpus,
            allocated_dpus: target_dpus,
            creation_time: Utc::now(),
            last_allocation: Some(Utc::now()),
            last_successful_allocation_time: Some(Utc::now()),
        };
        let arn = capacity_reservation_arn(&req.account_id, &req.region, &name);
        account.capacity_reservations.insert(name, cr);
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_capacity_reservation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cr = account
            .capacity_reservations
            .get(&name)
            .ok_or_else(|| invalid_request(format!("CapacityReservation {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "CapacityReservation": capacity_reservation_json(cr),
        })))
    }

    fn list_capacity_reservations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let max_results = validate_max_results(&body, 1, 50)?;
        // Smithy: NextToken targets Token @length(1,1024).
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<CapacityReservation> =
            account.capacity_reservations.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let crs: Vec<Value> = page.iter().map(capacity_reservation_json).collect();
        let mut response = json!({ "CapacityReservations": crs });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn update_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let target_dpus =
            body.get("TargetDpus")
                .and_then(Value::as_i64)
                .ok_or_else(|| invalid_request("TargetDpus is required"))? as i32;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cr = account
            .capacity_reservations
            .get_mut(&name)
            .ok_or_else(|| invalid_request(format!("CapacityReservation {name} not found")))?;
        cr.target_dpus = target_dpus;
        cr.allocated_dpus = target_dpus;
        cr.last_allocation = Some(Utc::now());
        cr.last_successful_allocation_time = Some(Utc::now());
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn cancel_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cr = account
            .capacity_reservations
            .get_mut(&name)
            .ok_or_else(|| invalid_request(format!("CapacityReservation {name} not found")))?;
        cr.status = "CANCELLING".to_string();
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: Name targets CapacityReservationName @length(1,128).
        let name = validate_required_string_len(&body, "Name", 1, 128)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.capacity_reservations.remove(&name).is_none() {
            return Err(invalid_request(format!(
                "CapacityReservation {name} not found"
            )));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn put_capacity_assignment_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cr_name = require_str(&body, "CapacityReservationName")?;
        let assignments = body
            .get("CapacityAssignments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.capacity_reservations.contains_key(&cr_name) {
            return Err(invalid_request(format!(
                "CapacityReservation {cr_name} not found"
            )));
        }
        account.capacity_assignment_config = Some(CapacityAssignmentConfiguration {
            capacity_reservation_name: cr_name,
            capacity_assignments: assignments,
        });
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_capacity_assignment_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cr_name = require_str(&body, "CapacityReservationName")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cfg = account
            .capacity_assignment_config
            .clone()
            .filter(|c| c.capacity_reservation_name == cr_name)
            .ok_or_else(|| {
                invalid_request(format!("No CapacityAssignmentConfiguration for {cr_name}"))
            })?;
        Ok(AwsResponse::ok_json(json!({
            "CapacityAssignmentConfiguration": {
                "CapacityReservationName": cfg.capacity_reservation_name,
                "CapacityAssignments": cfg.capacity_assignments,
            }
        })))
    }
}

// ─── Tags / misc ──────────────────────────────────────────────────

impl AthenaService {
    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: ResourceARN @length(1,1011), Tags @required.
        let arn = validate_required_string_len(&body, "ResourceARN", 1, 1011)?;
        validate_required_list(&body, "Tags")?;
        let tags = parse_tags(body.get("Tags"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let entry = account.tags.entry(arn).or_default();
        for (k, v) in tags {
            entry.insert(k, v);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: ResourceARN @length(1,1011), TagKeys @required.
        let arn = validate_required_string_len(&body, "ResourceARN", 1, 1011)?;
        validate_required_list(&body, "TagKeys")?;
        let keys = parse_string_list(body.get("TagKeys"));
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if let Some(t) = account.tags.get_mut(&arn) {
            for k in keys {
                t.remove(&k);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: ResourceARN targets AmazonResourceName @length(1,1011),
        // NextToken targets Token @length(1,1024),
        // MaxResults targets MaxTagsCount @range(min:75).
        let arn = validate_required_string_len(&body, "ResourceARN", 1, 1011)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        if let Some(v) = body.get("MaxResults").and_then(Value::as_i64) {
            if v < 75 {
                return Err(invalid_request(format!(
                    "MaxResults value {v} is below the minimum 75",
                )));
            }
        }
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let tags = account.tags.get(&arn).cloned().unwrap_or_default();
        let tag_list: Vec<Value> = tags
            .into_iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect();
        Ok(AwsResponse::ok_json(json!({ "Tags": tag_list })))
    }

    fn list_engine_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: MaxResults targets MaxEngineVersionsCount @range(1,10);
        // NextToken targets Token @length(1,1024).
        validate_max_results(&body, 1, 10)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        Ok(AwsResponse::ok_json(json!({
            "EngineVersions": [
                {"EffectiveEngineVersion": "Athena engine version 3", "SelectedEngineVersion": "AUTO"},
                {"EffectiveEngineVersion": "Athena engine version 3", "SelectedEngineVersion": "Athena engine version 3"},
            ]
        })))
    }

    fn list_application_dpu_sizes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: MaxResults targets MaxApplicationDPUSizesCount @range(1,100);
        // NextToken targets Token @length(1,1024).
        validate_max_results(&body, 1, 100)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        Ok(AwsResponse::ok_json(json!({
            "ApplicationDPUSizes": [
                {"ApplicationRuntimeId": "Athena-PySpark-3.0", "SupportedDPUSizes": [1, 2, 4, 8, 16, 32]},
            ]
        })))
    }

    fn list_executors(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.sessions.contains_key(&session_id) {
            return Err(invalid_request(format!("Session {session_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "SessionId": session_id,
            "ExecutorsSummary": [],
        })))
    }

    fn get_resource_dashboard(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: ResourceARN @required, targets AmazonResourceName @length(1,1011).
        validate_required_string_len(&body, "ResourceARN", 1, 1011)?;
        // Smithy GetResourceDashboardOutput: { Url: String (required) }.
        Ok(AwsResponse::ok_json(json!({
            "Url": "https://console.aws.amazon.com/athena/home#/dashboard",
        })))
    }
}

// ─── Helpers ───────────────────────────────────────────────────────

fn account_mut<'a>(state: &'a mut AthenaAccounts, account_id: &str) -> &'a mut AccountState {
    let a = state.accounts.entry(account_id.to_string()).or_default();
    a.ensure_initialized();
    a
}

fn require_str(body: &Value, field: &str) -> Result<String, AwsServiceError> {
    body.get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| invalid_request(format!("{field} is required")))
}

/// Validate `MaxResults` is within [min, max] when present. AWS Athena
/// uses range constraints like @range(min: 1, max: 50); the probe sends
/// boundary-violating values and expects InvalidRequestException.
fn validate_max_results(body: &Value, min: i64, max: i64) -> Result<usize, AwsServiceError> {
    if let Some(v) = body.get("MaxResults") {
        if let Some(n) = v.as_i64() {
            if n < min || n > max {
                return Err(invalid_request(format!(
                    "MaxResults value {n} is outside the valid range {min}-{max}",
                )));
            }
            return Ok(n as usize);
        }
    }
    Ok(50)
}

/// Validate a string body field length is within [min, max] when
/// present. Used for `@length`-constrained inputs like `ClientRequestToken`.
fn validate_opt_string_len(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    if let Some(Value::String(s)) = body.get(field) {
        let len = s.chars().count();
        if len < min || len > max {
            return Err(invalid_request(format!(
                "{field} length {len} is outside the valid range {min}-{max}",
            )));
        }
    }
    Ok(())
}

/// Require that a string field be present and within `[min, max]` characters.
/// Combines existence and `@length` enforcement for required `@length`-constrained
/// fields like `Name` / `Database` / `QueryString`.
fn validate_required_string_len(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
) -> Result<String, AwsServiceError> {
    let s = body
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_request(format!("{field} is required")))?;
    let len = s.chars().count();
    if len < min || len > max {
        return Err(invalid_request(format!(
            "{field} length {len} is outside the valid range {min}-{max}",
        )));
    }
    Ok(s.to_string())
}

/// Validate that a list-typed field is present (non-null) and non-empty when
/// the Smithy model marks it `@required` with `@length(min: 1, ...)`. Used for
/// batch operations like `BatchGetNamedQuery` where the list itself is required.
fn validate_required_list(body: &Value, field: &str) -> Result<(), AwsServiceError> {
    match body.get(field) {
        Some(Value::Array(_)) => Ok(()),
        _ => Err(invalid_request(format!("{field} is required"))),
    }
}

/// Validate that a list-typed field, if present, has a length within `[min, max]`.
fn validate_list_len(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    if let Some(Value::Array(a)) = body.get(field) {
        let len = a.len();
        if len < min || len > max {
            return Err(invalid_request(format!(
                "{field} length {len} is outside the valid range {min}-{max}",
            )));
        }
    }
    Ok(())
}

/// Validate an optional enum field's value against the allowed set.
fn validate_opt_enum(body: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(Value::String(s)) = body.get(field) {
        if !allowed.contains(&s.as_str()) {
            return Err(invalid_request(format!(
                "{field} value {s:?} is not a valid enum value",
            )));
        }
    }
    Ok(())
}

/// Replace `?` placeholders in a query string with the provided parameter
/// values, quoted safely for SQL parsing.
fn substitute_parameters(query: &str, params: &[Value]) -> Result<String, AwsServiceError> {
    let mut result = String::with_capacity(query.len());
    let mut param_iter = params.iter().filter_map(Value::as_str);
    let mut chars = query.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            // Skip inside string literals so we don't replace ? in strings.
            result.push('\'');
            while let Some(ch) = chars.next() {
                result.push(ch);
                if ch == '\'' {
                    // Handle escaped quotes ('').
                    if chars.peek() == Some(&'\'') {
                        result.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
            }
        } else if c == '?' {
            let p = param_iter
                .next()
                .ok_or_else(|| invalid_request("More placeholders than ExecutionParameters"))?;
            let escaped = p.replace('\'', "''");
            result.push('\'');
            result.push_str(&escaped);
            result.push('\'');
        } else {
            result.push(c);
        }
    }
    Ok(result)
}

fn invalid_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", msg)
}

fn synth_uuid() -> String {
    Uuid::new_v4().to_string()
}

fn workgroup_arn(account_id: &str, region: &str, name: &str) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    Arn::new("athena", region, account_id, &format!("workgroup/{name}")).to_string()
}

fn datacatalog_arn(account_id: &str, region: &str, name: &str) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    Arn::new("athena", region, account_id, &format!("datacatalog/{name}")).to_string()
}

fn capacity_reservation_arn(account_id: &str, region: &str, name: &str) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    Arn::new(
        "athena",
        region,
        account_id,
        &format!("capacity-reservation/{name}"),
    )
    .to_string()
}

fn parse_string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|v| {
            v.iter()
                .filter_map(|s| s.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_tags(
    value: Option<&Value>,
) -> Result<std::collections::BTreeMap<String, String>, AwsServiceError> {
    let mut out = std::collections::BTreeMap::new();
    let Some(arr) = value.and_then(Value::as_array) else {
        return Ok(out);
    };
    for tag in arr {
        let key = tag
            .get("Key")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_request("Tag.Key is required"))?
            .to_string();
        let value = tag
            .get("Value")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        out.insert(key, value);
    }
    Ok(out)
}

/// Map the leading SQL keyword to one of Athena's documented statement types.
fn classify_statement(query: &str) -> String {
    let trimmed = query.trim_start();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("SELECT")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("EXPLAIN")
    {
        "DML".to_string()
    } else if upper.starts_with("CREATE") || upper.starts_with("ALTER") || upper.starts_with("DROP")
    {
        "DDL".to_string()
    } else if upper.starts_with("UPDATE")
        || upper.starts_with("INSERT")
        || upper.starts_with("DELETE")
    {
        "DML".to_string()
    } else {
        "UTILITY".to_string()
    }
}

// ─── JSON shaping ──────────────────────────────────────────────────

fn work_group_json(wg: &WorkGroup) -> Value {
    let mut obj = json!({
        "Name": wg.name,
        "State": wg.state,
        "CreationTime": wg.creation_time.timestamp() as f64,
    });
    if let Some(d) = &wg.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if let Some(c) = &wg.configuration {
        obj.as_object_mut()
            .unwrap()
            .insert("Configuration".to_string(), c.clone());
    }
    obj
}

fn workgroup_summary_json(wg: &WorkGroup) -> Value {
    let mut obj = json!({
        "Name": wg.name,
        "State": wg.state,
        "CreationTime": wg.creation_time.timestamp() as f64,
    });
    if let Some(d) = &wg.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if let Some(v) = &wg.engine_version {
        obj.as_object_mut().unwrap().insert(
            "EngineVersion".to_string(),
            json!({"SelectedEngineVersion": v}),
        );
    }
    obj
}

fn data_catalog_json(c: &DataCatalog) -> Value {
    let mut obj = json!({
        "Name": c.name,
        "Type": c.cat_type,
        "Status": c.status,
        "Parameters": c.parameters,
    });
    if let Some(d) = &c.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if let Some(t) = &c.connection_type {
        obj.as_object_mut()
            .unwrap()
            .insert("ConnectionType".to_string(), Value::String(t.clone()));
    }
    if let Some(e) = &c.error {
        obj.as_object_mut()
            .unwrap()
            .insert("Error".to_string(), Value::String(e.clone()));
    }
    obj
}

fn named_query_json(q: &NamedQuery) -> Value {
    let mut obj = json!({
        "Name": q.name,
        "Database": q.database,
        "QueryString": q.query_string,
        "NamedQueryId": q.named_query_id,
        "WorkGroup": q.work_group,
    });
    if let Some(d) = &q.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    obj
}

fn prepared_statement_json(p: &PreparedStatement) -> Value {
    let mut obj = json!({
        "StatementName": p.statement_name,
        "QueryStatement": p.query_statement,
        "WorkGroupName": p.work_group_name,
        "LastModifiedTime": p.last_modified_time.timestamp() as f64,
    });
    if let Some(d) = &p.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    obj
}

fn query_execution_json(q: &QueryExecution) -> Value {
    let mut obj = json!({
        "QueryExecutionId": q.query_execution_id,
        "Query": q.query,
        "StatementType": q.statement_type,
        "WorkGroup": q.work_group,
        "Status": {
            "State": q.state,
            "SubmissionDateTime": q.submission_time.timestamp() as f64,
            "CompletionDateTime": q.completion_time.map(|t| t.timestamp() as f64),
        },
        "Statistics": {
            "DataScannedInBytes": q.data_scanned_bytes,
            "EngineExecutionTimeInMillis": q.engine_execution_time_ms,
            "QueryPlanningTimeInMillis": q.query_planning_time_ms,
            "TotalExecutionTimeInMillis": q.total_execution_time_ms,
        },
    });
    if let Some(c) = &q.query_execution_context {
        obj.as_object_mut()
            .unwrap()
            .insert("QueryExecutionContext".to_string(), c.clone());
    }
    if let Some(c) = &q.result_configuration {
        obj.as_object_mut()
            .unwrap()
            .insert("ResultConfiguration".to_string(), c.clone());
    }
    if let Some(v) = &q.engine_version {
        obj.as_object_mut()
            .unwrap()
            .insert("EngineVersion".to_string(), v.clone());
    }
    if let Some(reason) = &q.state_change_reason {
        obj["Status"].as_object_mut().unwrap().insert(
            "StateChangeReason".to_string(),
            Value::String(reason.clone()),
        );
    }
    obj
}

fn notebook_metadata_json(n: &Notebook) -> Value {
    json!({
        "NotebookId": n.notebook_id,
        "Name": n.name,
        "WorkGroup": n.work_group,
        "Type": n.notebook_type,
        "CreationTime": n.creation_time.timestamp() as f64,
        "LastModifiedTime": n.last_modified_time.timestamp() as f64,
    })
}

fn session_summary_json(s: &Session) -> Value {
    json!({
        "SessionId": s.session_id,
        "Description": s.description,
        "Status": session_status_json(s),
        "EngineVersion": s.engine_version,
        "NotebookVersion": s.notebook_version,
    })
}

fn session_detail_json(s: &Session) -> Value {
    let mut obj = json!({
        "SessionId": s.session_id,
        "WorkGroup": s.work_group,
        "Status": session_status_json(s),
        "EngineVersion": s.engine_version,
        "NotebookVersion": s.notebook_version,
    });
    if let Some(d) = &s.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if let Some(c) = &s.configuration {
        obj.as_object_mut()
            .unwrap()
            .insert("EngineConfiguration".to_string(), c.clone());
    }
    obj
}

fn session_status_json(s: &Session) -> Value {
    let mut obj = json!({
        "State": s.state,
        "StartDateTime": s.start_date_time.timestamp() as f64,
    });
    if let Some(t) = s.end_date_time {
        obj.as_object_mut()
            .unwrap()
            .insert("EndDateTime".to_string(), json!(t.timestamp() as f64));
    }
    if let Some(t) = s.idle_since_date_time {
        obj.as_object_mut()
            .unwrap()
            .insert("IdleSinceDateTime".to_string(), json!(t.timestamp() as f64));
    }
    obj
}

fn calculation_summary_json(c: &Calculation) -> Value {
    json!({
        "CalculationExecutionId": c.calculation_execution_id,
        "Description": c.description,
        "Status": calculation_status_json(c),
    })
}

fn calculation_detail_json(c: &Calculation) -> Value {
    let mut obj = json!({
        "CalculationExecutionId": c.calculation_execution_id,
        "SessionId": c.session_id,
        "Status": calculation_status_json(c),
    });
    if let Some(d) = &c.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if let Some(w) = &c.working_directory {
        obj.as_object_mut().unwrap().insert(
            "Result".to_string(),
            json!({ "ResultS3Uri": w, "ResultType": "JSON" }),
        );
    }
    obj
}

fn calculation_status_json(c: &Calculation) -> Value {
    let mut obj = json!({
        "State": c.state,
        "SubmissionDateTime": c.submission_date_time.timestamp() as f64,
    });
    if let Some(t) = c.completion_date_time {
        obj.as_object_mut().unwrap().insert(
            "CompletionDateTime".to_string(),
            json!(t.timestamp() as f64),
        );
    }
    if let Some(reason) = &c.state_change_reason {
        obj.as_object_mut().unwrap().insert(
            "StateChangeReason".to_string(),
            Value::String(reason.clone()),
        );
    }
    obj
}

fn capacity_reservation_json(cr: &CapacityReservation) -> Value {
    let mut obj = json!({
        "Name": cr.name,
        "Status": cr.status,
        "TargetDpus": cr.target_dpus,
        "AllocatedDpus": cr.allocated_dpus,
        "CreationTime": cr.creation_time.timestamp() as f64,
    });
    if let Some(t) = cr.last_allocation {
        obj.as_object_mut().unwrap().insert(
            "LastAllocation".to_string(),
            json!({
                "Status": "SUCCEEDED",
                "RequestTime": t.timestamp() as f64,
            }),
        );
    }
    if let Some(t) = cr.last_successful_allocation_time {
        obj.as_object_mut().unwrap().insert(
            "LastSuccessfulAllocationTime".to_string(),
            json!(t.timestamp() as f64),
        );
    }
    obj
}

// ─── Glue -> Athena helpers ────────────────────────────────────────

fn glue_database_json(db: &fakecloud_glue::Database) -> Value {
    let mut obj = json!({
        "Name": db.name,
        "Description": db.description.as_deref().unwrap_or(""),
        "Parameters": db.parameters,
    });
    if let Some(uri) = &db.location_uri {
        obj.as_object_mut()
            .unwrap()
            .insert("LocationUri".to_string(), Value::String(uri.clone()));
    }
    obj
}

fn glue_table_metadata_json(tbl: &fakecloud_glue::Table) -> Value {
    let columns: Vec<Value> = tbl
        .storage_descriptor
        .as_ref()
        .map(|s| {
            s.columns
                .iter()
                .map(|c| {
                    let mut col = json!({
                        "Name": c.name,
                        "Type": c.column_type,
                    });
                    if let Some(comment) = &c.comment {
                        col.as_object_mut()
                            .unwrap()
                            .insert("Comment".to_string(), Value::String(comment.clone()));
                    }
                    col
                })
                .collect()
        })
        .unwrap_or_default();

    let partition_keys: Vec<Value> = tbl
        .partition_keys
        .iter()
        .map(|c| {
            let mut col = json!({
                "Name": c.name,
                "Type": c.column_type,
            });
            if let Some(comment) = &c.comment {
                col.as_object_mut()
                    .unwrap()
                    .insert("Comment".to_string(), Value::String(comment.clone()));
            }
            col
        })
        .collect();

    let mut obj = json!({
        "Name": tbl.name,
        "TableType": tbl.table_type.as_deref().unwrap_or("EXTERNAL_TABLE"),
        "Columns": columns,
        "PartitionKeys": partition_keys,
    });

    let params = &tbl.parameters;
    if !params.is_empty() {
        obj.as_object_mut().unwrap().insert(
            "Parameters".to_string(),
            serde_json::to_value(params).unwrap(),
        );
    }
    obj
}

fn match_table_expression(name: &str, expression: &str) -> bool {
    if expression == "*" {
        return true;
    }
    regex::Regex::new(&format!("^{expression}$")).is_ok_and(|re| re.is_match(name))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use fakecloud_core::service::AwsRequest;
    use fakecloud_glue::Column;
    use fakecloud_glue::{Database, GlueAccounts, SharedGlueState, StorageDescriptor, Table};
    use parking_lot::RwLock;
    use serde_json::json;

    use crate::service::{substitute_parameters, AthenaService};
    use crate::state::AthenaAccounts;
    use crate::SharedAthenaState;

    fn test_service() -> (AthenaService, SharedGlueState) {
        let glue = Arc::new(RwLock::new(GlueAccounts::new()));
        let svc = AthenaService::new(Arc::new(RwLock::new(AthenaAccounts::new())))
            .with_glue(Arc::clone(&glue));
        (svc, glue)
    }

    fn seed_glue_db(glue: &SharedGlueState, account_id: &str, region: &str, db: Database) {
        let mut g = glue.write();
        let acct = g.get_or_create(account_id, region);
        let dbs = acct.dbs_in_mut(region);
        dbs.insert(db.name.clone(), db);
    }

    fn seed_glue_table(
        glue: &SharedGlueState,
        account_id: &str,
        region: &str,
        db_name: &str,
        table: Table,
    ) {
        let mut g = glue.write();
        let acct = g.get_or_create(account_id, region);
        let dbs = acct.dbs_in_mut(region);
        let db = dbs.get_mut(db_name).unwrap();
        db.tables.insert(table.name.clone(), table);
    }

    fn make_db(name: &str) -> Database {
        Database {
            name: name.to_string(),
            description: Some(format!("db {name}")),
            location_uri: Some(format!("s3://bucket/{name}")),
            parameters: std::collections::BTreeMap::new(),
            created_at: Utc::now(),
            catalog_id: "123456789012".to_string(),
            tables: std::collections::BTreeMap::new(),
        }
    }

    fn make_table(name: &str, db_name: &str) -> Table {
        Table {
            name: name.to_string(),
            database_name: db_name.to_string(),
            description: Some(format!("table {name}")),
            owner: None,
            create_time: Utc::now(),
            update_time: Utc::now(),
            last_access_time: None,
            retention: 0,
            storage_descriptor: Some(StorageDescriptor {
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        column_type: "int".to_string(),
                        comment: Some("pk".to_string()),
                    },
                    Column {
                        name: "name".to_string(),
                        column_type: "string".to_string(),
                        comment: None,
                    },
                ],
                location: Some("s3://bucket/data/".to_string()),
                input_format: None,
                output_format: None,
                compressed: None,
                serde_info: None,
                parameters: std::collections::BTreeMap::new(),
            }),
            partition_keys: vec![],
            view_original_text: None,
            view_expanded_text: None,
            table_type: Some("EXTERNAL_TABLE".to_string()),
            parameters: std::collections::BTreeMap::new(),
            partitions: std::collections::BTreeMap::new(),
        }
    }

    fn req(action: &str, body: serde_json::Value) -> AwsRequest {
        AwsRequest {
            service: "athena".to_string(),
            action: action.to_string(),
            body: serde_json::to_vec(&body).unwrap().into(),
            query_params: Default::default(),
            headers: Default::default(),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "req-1".to_string(),
            principal: None,
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    fn parse_json(resp: &fakecloud_core::service::AwsResponse) -> serde_json::Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    #[test]
    fn substitute_parameters_replaces_placeholders() {
        let out = substitute_parameters(
            "SELECT * FROM t WHERE id = ? AND name = ?",
            &[json!("42"), json!("Alice")],
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE id = '42' AND name = 'Alice'");
    }

    #[test]
    fn substitute_parameters_skips_placeholders_in_string_literals() {
        let out = substitute_parameters(
            "SELECT * FROM t WHERE name = 'foo?bar' AND id = ?",
            &[json!("7")],
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE name = 'foo?bar' AND id = '7'");
    }

    #[test]
    fn substitute_parameters_errors_on_too_few_params() {
        let err =
            substitute_parameters("SELECT * WHERE a = ? AND b = ?", &[json!("1")]).unwrap_err();
        assert!(err
            .message()
            .contains("More placeholders than ExecutionParameters"));
    }

    #[test]
    fn substitute_parameters_escapes_quotes() {
        let out = substitute_parameters("SELECT * WHERE name = ?", &[json!("O'Brien")]).unwrap();
        assert_eq!(out, "SELECT * WHERE name = 'O''Brien'");
    }

    #[test]
    fn start_query_execution_with_named_query_id() {
        let svc = AthenaService::new(SharedAthenaState::default());
        let create_resp = svc
            .create_named_query(&req(
                "CreateNamedQuery",
                json!({
                    "Name": "my-query",
                    "Database": "default",
                    "QueryString": "SELECT 1 AS id",
                    "WorkGroup": "primary"
                }),
            ))
            .unwrap();
        let nq_id = parse_json(&create_resp)
            .get("NamedQueryId")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let exec_resp = svc
            .start_query_execution(&req(
                "StartQueryExecution",
                json!({ "NamedQueryId": nq_id }),
            ))
            .unwrap();
        let body = parse_json(&exec_resp);
        assert!(body.get("QueryExecutionId").is_some());

        let qe_id = body.get("QueryExecutionId").unwrap().as_str().unwrap();
        let qe_resp = svc
            .get_query_execution(&req(
                "GetQueryExecution",
                json!({ "QueryExecutionId": qe_id }),
            ))
            .unwrap();
        let qe = parse_json(&qe_resp).get("QueryExecution").unwrap().clone();
        assert_eq!(qe.get("Query").unwrap(), "SELECT 1 AS id");
    }

    #[test]
    fn start_query_execution_bumps_named_query_last_used_at() {
        let svc = AthenaService::new(SharedAthenaState::default());
        let create_resp = svc
            .create_named_query(&req(
                "CreateNamedQuery",
                json!({
                    "Name": "tracked",
                    "Database": "default",
                    "QueryString": "SELECT 1",
                    "WorkGroup": "primary"
                }),
            ))
            .unwrap();
        let nq_id = parse_json(&create_resp)
            .get("NamedQueryId")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        // Before any StartQueryExecution the named query has no last_used_at.
        {
            let state = svc.state.read();
            let account = state.accounts.values().next().unwrap();
            let nq = account.named_queries.get(&nq_id).unwrap();
            assert!(nq.last_used_at.is_none());
        }

        svc.start_query_execution(&req(
            "StartQueryExecution",
            json!({ "NamedQueryId": nq_id }),
        ))
        .unwrap();

        let state = svc.state.read();
        let account = state.accounts.values().next().unwrap();
        let nq = account.named_queries.get(&nq_id).unwrap();
        assert!(
            nq.last_used_at.is_some(),
            "last_used_at must be populated after StartQueryExecution",
        );
    }

    #[test]
    fn start_query_execution_with_execution_parameters() {
        let svc = AthenaService::new(SharedAthenaState::default());
        let exec_resp = svc
            .start_query_execution(&req(
                "StartQueryExecution",
                json!({
                    "QueryString": "SELECT ? AS id, ? AS name",
                    "ExecutionParameters": ["42", "Alice"]
                }),
            ))
            .unwrap();
        let body = parse_json(&exec_resp);
        let qe_id = body.get("QueryExecutionId").unwrap().as_str().unwrap();
        let qe_resp = svc
            .get_query_execution(&req(
                "GetQueryExecution",
                json!({ "QueryExecutionId": qe_id }),
            ))
            .unwrap();
        let qe = parse_json(&qe_resp).get("QueryExecution").unwrap().clone();
        assert_eq!(
            qe.get("Query").unwrap(),
            "SELECT '42' AS id, 'Alice' AS name"
        );
    }

    #[test]
    fn list_databases_reads_glue() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("inventory"));

        let resp = svc
            .list_databases(&req(
                "ListDatabases",
                json!({ "CatalogName": "AwsDataCatalog" }),
            ))
            .unwrap();
        let body = parse_json(&resp);
        let list = body.get("DatabaseList").unwrap().as_array().unwrap();
        assert_eq!(list.len(), 2);
        let names: Vec<String> = list
            .iter()
            .map(|d| d.get("Name").unwrap().as_str().unwrap().to_string())
            .collect();
        assert!(names.contains(&"sales".to_string()));
        assert!(names.contains(&"inventory".to_string()));
    }

    #[test]
    fn get_database_reads_glue() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));

        let resp = svc
            .get_database(&req(
                "GetDatabase",
                json!({ "CatalogName": "AwsDataCatalog", "DatabaseName": "sales" }),
            ))
            .unwrap();
        let body = parse_json(&resp);
        let db = body.get("Database").unwrap();
        assert_eq!(db.get("Name").unwrap(), "sales");
        assert_eq!(
            db.get("LocationUri").unwrap().as_str().unwrap(),
            "s3://bucket/sales"
        );
    }

    #[test]
    fn start_query_execution_missing_named_query_errors() {
        let svc = AthenaService::new(SharedAthenaState::default());
        svc.create_named_query(&req(
            "CreateNamedQuery",
            json!({
                "Name": "seed",
                "Database": "default",
                "QueryString": "SELECT 1",
                "WorkGroup": "primary"
            }),
        ))
        .unwrap();
        match svc.start_query_execution(&req(
            "StartQueryExecution",
            json!({ "NamedQueryId": "nope" }),
        )) {
            Ok(_) => panic!("expected error"),
            Err(err) => assert!(err.message().contains("NamedQuery nope not found")),
        }
    }

    #[test]
    fn get_database_missing_in_glue_errors() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));

        let err = match svc.get_database(&req(
            "GetDatabase",
            json!({ "CatalogName": "AwsDataCatalog", "DatabaseName": "missing" }),
        )) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(err.message().contains("Database missing not found"));
    }

    #[test]
    fn list_table_metadata_reads_glue() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));
        seed_glue_table(
            &glue,
            "123456789012",
            "us-east-1",
            "sales",
            make_table("orders", "sales"),
        );
        seed_glue_table(
            &glue,
            "123456789012",
            "us-east-1",
            "sales",
            make_table("returns", "sales"),
        );

        let resp = svc
            .list_table_metadata(&req(
                "ListTableMetadata",
                json!({ "CatalogName": "AwsDataCatalog", "DatabaseName": "sales" }),
            ))
            .unwrap();
        let body = parse_json(&resp);
        let list = body.get("TableMetadataList").unwrap().as_array().unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn get_table_metadata_reads_glue() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));
        seed_glue_table(
            &glue,
            "123456789012",
            "us-east-1",
            "sales",
            make_table("orders", "sales"),
        );

        let resp = svc
            .get_table_metadata(&req(
                "GetTableMetadata",
                json!({
                    "CatalogName": "AwsDataCatalog",
                    "DatabaseName": "sales",
                    "TableName": "orders"
                }),
            ))
            .unwrap();
        let body = parse_json(&resp);
        let meta = body.get("TableMetadata").unwrap();
        assert_eq!(meta.get("Name").unwrap(), "orders");
        let cols = meta.get("Columns").unwrap().as_array().unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].get("Name").unwrap(), "id");
        assert_eq!(cols[0].get("Type").unwrap(), "int");
    }

    #[test]
    fn get_table_metadata_missing_errors() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));

        let err = match svc.get_table_metadata(&req(
            "GetTableMetadata",
            json!({
                "CatalogName": "AwsDataCatalog",
                "DatabaseName": "sales",
                "TableName": "missing"
            }),
        )) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(err.message().contains("Table sales.missing not found"));
    }

    #[test]
    fn match_table_expression_wildcard() {
        use super::match_table_expression;
        assert!(match_table_expression("orders", "*"));
        assert!(match_table_expression("orders", "ord.*"));
        assert!(!match_table_expression("orders", "ret.*"));
        assert!(match_table_expression("orders", "ord.rs"));
        assert!(!match_table_expression("orders", "ord.r"));
        // Exact expression must match the name, not every name.
        assert!(match_table_expression("orders", "orders"));
        assert!(!match_table_expression("orders", "sales"));
    }
}
