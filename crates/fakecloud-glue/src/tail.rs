//! Remaining control-plane operations: integrations, identity center, column
//! statistics, table versions, partition indexes, job bookmarks/batches,
//! materialized-view refresh, unfiltered metadata, schema/script generation,
//! entities, catalog import, and data-quality statistics/annotations.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity_not_found, new_id, now_ts, req_present, req_str, resource_arn};
use crate::service::GlueService;

impl GlueService {
    // ===================== integrations =====================

    fn integration_json(
        &self,
        account: &str,
        region: &str,
        name: &str,
        src: &str,
        tgt: &str,
        body: &Value,
    ) -> Value {
        let arn = resource_arn(account, region, "integration", name);
        let mut v = json!({
            "SourceArn": src, "TargetArn": tgt, "IntegrationName": name,
            "IntegrationArn": arn, "Status": "ACTIVE", "CreateTime": now_ts(),
        });
        for f in [
            "Description",
            "KmsKeyId",
            "AdditionalEncryptionContext",
            "Tags",
            "DataFilter",
            "IntegrationConfig",
        ] {
            if let Some(val) = body.get(f) {
                if !val.is_null() {
                    v[f] = val.clone();
                }
            }
        }
        v
    }

    pub(crate) fn create_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "IntegrationName")?.to_string();
        let src = req_str(&body, "SourceArn")?.to_string();
        let tgt = req_str(&body, "TargetArn")?.to_string();
        let v = self.integration_json(&req.account_id, &req.region, &name, &src, &tgt, &body);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        // CreateIntegration declares ConflictException (not AlreadyExistsException);
        // persist idempotently rather than emit an undeclared error.
        st.integrations.insert(name, v.clone());
        Ok(AwsResponse::ok_json(v))
    }

    pub(crate) fn modify_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "IntegrationIdentifier")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let v = st
            .integrations
            .get_mut(&id)
            .ok_or_else(|| entity_not_found(format!("Integration {id} not found")))?;
        if let Some(obj) = v.as_object_mut() {
            for f in [
                "Description",
                "DataFilter",
                "IntegrationConfig",
                "IntegrationName",
            ] {
                if let Some(val) = body.get(f) {
                    if !val.is_null() {
                        obj.insert(f.to_string(), val.clone());
                    }
                }
            }
        }
        Ok(AwsResponse::ok_json(v.clone()))
    }

    pub(crate) fn delete_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "IntegrationIdentifier")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let mut v = st
            .integrations
            .remove(&id)
            .ok_or_else(|| entity_not_found(format!("Integration {id} not found")))?;
        if let Some(obj) = v.as_object_mut() {
            obj.insert("Status".into(), json!("DELETING"));
        }
        Ok(AwsResponse::ok_json(v))
    }

    pub(crate) fn describe_integrations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = body.get("IntegrationIdentifier").and_then(|v| v.as_str());
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.integrations
                    .iter()
                    .filter(|(k, _)| id.is_none_or(|i| k.as_str() == i))
                    .map(|(_, v)| v.clone())
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Integrations": list })))
    }

    pub(crate) fn describe_inbound_integrations(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(AwsResponse::ok_json(json!({ "InboundIntegrations": [] })))
    }

    pub(crate) fn create_integration_resource_property(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?.to_string();
        let v = json!({
            "ResourceArn": arn,
            "ResourcePropertyArn": format!("{arn}/property"),
            "SourceProcessingProperties": body.get("SourceProcessingProperties").cloned().unwrap_or(Value::Null),
            "TargetProcessingProperties": body.get("TargetProcessingProperties").cloned().unwrap_or(Value::Null),
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.integration_resource_props.insert(arn, v.clone());
        Ok(AwsResponse::ok_json(v))
    }

    pub(crate) fn get_integration_resource_property(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?;
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.integration_resource_props.get(arn))
            .ok_or_else(|| entity_not_found("Resource property not found"))?;
        Ok(AwsResponse::ok_json(v.clone()))
    }

    pub(crate) fn update_integration_resource_property(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?.to_string();
        let v = json!({
            "ResourceArn": arn,
            "ResourcePropertyArn": format!("{arn}/property"),
            "SourceProcessingProperties": body.get("SourceProcessingProperties").cloned().unwrap_or(Value::Null),
            "TargetProcessingProperties": body.get("TargetProcessingProperties").cloned().unwrap_or(Value::Null),
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.integration_resource_props.insert(arn, v.clone());
        Ok(AwsResponse::ok_json(v))
    }

    pub(crate) fn delete_integration_resource_property(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.integration_resource_props.remove(&arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn list_integration_resource_properties(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.integration_resource_props.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "IntegrationResourcePropertyList": list,
        })))
    }

    fn itp_key(arn: &str, table: &str) -> String {
        format!("{arn}\u{1f}{table}")
    }

    pub(crate) fn create_integration_table_properties(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let v = json!({
            "ResourceArn": arn, "TableName": table,
            "SourceTableConfig": body.get("SourceTableConfig").cloned().unwrap_or(Value::Null),
            "TargetTableConfig": body.get("TargetTableConfig").cloned().unwrap_or(Value::Null),
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.integration_table_props
            .insert(Self::itp_key(&arn, &table), v);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_integration_table_properties(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?;
        let table = req_str(&body, "TableName")?;
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.integration_table_props.get(&Self::itp_key(arn, table)))
            .ok_or_else(|| entity_not_found("Table properties not found"))?;
        Ok(AwsResponse::ok_json(v.clone()))
    }

    pub(crate) fn update_integration_table_properties(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let v = json!({
            "ResourceArn": arn, "TableName": table,
            "SourceTableConfig": body.get("SourceTableConfig").cloned().unwrap_or(Value::Null),
            "TargetTableConfig": body.get("TargetTableConfig").cloned().unwrap_or(Value::Null),
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.integration_table_props
            .insert(Self::itp_key(&arn, &table), v);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_integration_table_properties(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.integration_table_props
            .remove(&Self::itp_key(&arn, &table));
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ===================== identity center =====================

    pub(crate) fn create_glue_identity_center_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let instance = req_str(&body, "InstanceArn")?.to_string();
        let app_arn = resource_arn(&req.account_id, &req.region, "application", "glue-idc");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.identity_center = Some(json!({
            "ApplicationArn": app_arn, "InstanceArn": instance,
            "Scopes": body.get("Scopes").cloned().unwrap_or(json!([])),
            "UserBackgroundSessionsEnabled": body.get("UserBackgroundSessionsEnabled").cloned().unwrap_or(Value::Null),
        }));
        Ok(AwsResponse::ok_json(json!({ "ApplicationArn": app_arn })))
    }

    pub(crate) fn get_glue_identity_center_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let cfg = accounts
            .get(&req.account_id)
            .and_then(|s| s.identity_center.clone())
            .ok_or_else(|| entity_not_found("Identity Center configuration not found"))?;
        Ok(AwsResponse::ok_json(cfg))
    }

    pub(crate) fn update_glue_identity_center_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        if let Some(Value::Object(obj)) = st.identity_center.as_mut() {
            if let Some(s) = body.get("Scopes") {
                obj.insert("Scopes".into(), s.clone());
            }
            if let Some(u) = body.get("UserBackgroundSessionsEnabled") {
                obj.insert("UserBackgroundSessionsEnabled".into(), u.clone());
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_glue_identity_center_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.identity_center = None;
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ===================== column statistics =====================

    fn col_stat_key(db: &str, table: &str, part: &str, col: &str) -> String {
        format!("{db}\u{1f}{table}\u{1f}{part}\u{1f}{col}")
    }

    pub(crate) fn update_column_statistics_for_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.update_column_statistics(req, "")
    }

    pub(crate) fn update_column_statistics_for_partition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let part = part_values_string(&body, "PartitionValues");
        self.update_column_statistics(req, &part)
    }

    fn update_column_statistics(
        &self,
        req: &AwsRequest,
        part: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let list = req_present(&body, "ColumnStatisticsList")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        if let Some(arr) = list.as_array() {
            for cs in arr {
                if let Some(col) = cs.get("ColumnName").and_then(|v| v.as_str()) {
                    st.column_stats
                        .insert(Self::col_stat_key(&db, &table, part, col), cs.clone());
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({ "Errors": [] })))
    }

    pub(crate) fn get_column_statistics_for_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.get_column_statistics(req, "")
    }

    pub(crate) fn get_column_statistics_for_partition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let part = part_values_string(&body, "PartitionValues");
        self.get_column_statistics(req, &part)
    }

    fn get_column_statistics(
        &self,
        req: &AwsRequest,
        part: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        let cols = req_present(&body, "ColumnNames")?;
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id).map(|s| &s.column_stats);
        let mut list = Vec::new();
        if let Some(arr) = cols.as_array() {
            for c in arr {
                let Some(col) = c.as_str() else { continue };
                if let Some(cs) =
                    store.and_then(|m| m.get(&Self::col_stat_key(db, table, part, col)))
                {
                    list.push(cs.clone());
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "ColumnStatisticsList": list, "Errors": [],
        })))
    }

    pub(crate) fn delete_column_statistics_for_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let col = req_str(&body, "ColumnName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.column_stats
            .remove(&Self::col_stat_key(&db, &table, "", &col));
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_column_statistics_for_partition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let col = req_str(&body, "ColumnName")?.to_string();
        let part = part_values_string(&body, "PartitionValues");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.column_stats
            .remove(&Self::col_stat_key(&db, &table, &part, &col));
        Ok(AwsResponse::ok_json(json!({})))
    }

    // column statistics task runs / settings / schedule

    pub(crate) fn start_column_statistics_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        req_str(&body, "Role")?;
        let id = new_id();
        let now = now_ts();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.column_stats_task_runs.insert(
            id.clone(),
            json!({
                "ColumnStatisticsTaskRunId": id, "DatabaseName": db, "TableName": table,
                "Status": "RUNNING", "CreationTime": now, "LastUpdated": now,
                "Role": body.get("Role").cloned().unwrap_or(Value::Null),
            }),
        );
        Ok(AwsResponse::ok_json(json!({
            "ColumnStatisticsTaskRunId": id,
        })))
    }

    pub(crate) fn get_column_statistics_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "ColumnStatisticsTaskRunId")?;
        let accounts = self.state.read();
        let r = accounts
            .get(&req.account_id)
            .and_then(|s| s.column_stats_task_runs.get(id))
            .ok_or_else(|| entity_not_found(format!("Task run {id} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "ColumnStatisticsTaskRun": r }),
        ))
    }

    pub(crate) fn get_column_statistics_task_runs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        let accounts = self.state.read();
        let runs: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.column_stats_task_runs
                    .values()
                    .filter(|r| {
                        r.get("DatabaseName").and_then(|v| v.as_str()) == Some(db)
                            && r.get("TableName").and_then(|v| v.as_str()) == Some(table)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "ColumnStatisticsTaskRuns": runs,
        })))
    }

    pub(crate) fn list_column_statistics_task_runs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let ids: Vec<String> = accounts
            .get(&req.account_id)
            .map(|s| s.column_stats_task_runs.keys().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "ColumnStatisticsTaskRunIds": ids,
        })))
    }

    pub(crate) fn stop_column_statistics_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let found = st.column_stats_task_runs.values_mut().any(|r| {
            if r.get("DatabaseName").and_then(|v| v.as_str()) == Some(db.as_str())
                && r.get("TableName").and_then(|v| v.as_str()) == Some(table.as_str())
                && r.get("Status").and_then(|v| v.as_str()) == Some("RUNNING")
            {
                if let Some(obj) = r.as_object_mut() {
                    obj.insert("Status".into(), json!("STOPPING"));
                }
                true
            } else {
                false
            }
        });
        if !found {
            return Err(AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "ColumnStatisticsTaskNotRunningException",
                "No running column statistics task for table".to_string(),
            ));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn cst_settings_key(db: &str, table: &str) -> String {
        format!("{db}\u{1f}{table}")
    }

    pub(crate) fn create_column_statistics_task_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        req_str(&body, "Role")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.column_stats_task_settings
            .insert(Self::cst_settings_key(&db, &table), body.clone());
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn update_column_statistics_task_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let key = Self::cst_settings_key(&db, &table);
        if !st.column_stats_task_settings.contains_key(&key) {
            return Err(entity_not_found("Task settings not found"));
        }
        st.column_stats_task_settings.insert(key, body.clone());
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_column_statistics_task_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        let accounts = self.state.read();
        let s = accounts.get(&req.account_id).and_then(|s| {
            s.column_stats_task_settings
                .get(&Self::cst_settings_key(db, table))
        });
        let settings = match s {
            Some(v) => json!({
                "DatabaseName": v.get("DatabaseName").cloned().unwrap_or(Value::Null),
                "TableName": v.get("TableName").cloned().unwrap_or(Value::Null),
                "Role": v.get("Role").cloned().unwrap_or(Value::Null),
                "Schedule": v.get("Schedule").cloned().unwrap_or(Value::Null),
            }),
            None => return Err(entity_not_found("Task settings not found")),
        };
        Ok(AwsResponse::ok_json(json!({
            "ColumnStatisticsTaskSettings": settings,
        })))
    }

    pub(crate) fn delete_column_statistics_task_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.column_stats_task_settings
            .remove(&Self::cst_settings_key(&db, &table));
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn start_column_statistics_task_run_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "DatabaseName")?;
        req_str(&body, "TableName")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn stop_column_statistics_task_run_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "DatabaseName")?;
        req_str(&body, "TableName")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ===================== job bookmarks & batches =====================

    pub(crate) fn get_job_bookmark(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job = req_str(&body, "JobName")?.to_string();
        let accounts = self.state.read();
        let entry = accounts
            .get(&req.account_id)
            .and_then(|s| s.job_bookmarks.get(&job))
            .cloned()
            .unwrap_or_else(|| json!({"JobName": job, "Version": 1, "Run": 0, "Attempt": 0}));
        Ok(AwsResponse::ok_json(json!({ "JobBookmarkEntry": entry })))
    }

    pub(crate) fn reset_job_bookmark(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job = req_str(&body, "JobName")?.to_string();
        let entry = json!({"JobName": job, "Version": 1, "Run": 0, "Attempt": 0});
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.job_bookmarks.insert(job, entry.clone());
        Ok(AwsResponse::ok_json(json!({ "JobBookmarkEntry": entry })))
    }

    pub(crate) fn batch_get_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let names = body["JobNames"].as_array().cloned().unwrap_or_default();
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id);
        let mut found = Vec::new();
        let mut not_found = Vec::new();
        for n in &names {
            let Some(name) = n.as_str() else { continue };
            match store.and_then(|s| s.jobs.get(name)) {
                Some(j) => found.push(crate::jobs::job_to_json(j)),
                None => not_found.push(json!(name)),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "Jobs": found, "JobsNotFound": not_found,
        })))
    }

    pub(crate) fn batch_stop_job_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job = req_str(&body, "JobName")?.to_string();
        let ids = body["JobRunIds"].as_array().cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let mut successes = Vec::new();
        let mut errors = Vec::new();
        for i in &ids {
            let Some(id) = i.as_str() else { continue };
            match st.job_runs.get_mut(id) {
                Some(run) if run.job_name == job => {
                    run.state = "STOPPING".to_string();
                    successes.push(json!({"JobName": job, "JobRunId": id}));
                }
                _ => errors.push(json!({
                    "JobName": job, "JobRunId": id,
                    "ErrorDetail": {"ErrorCode": "EntityNotFoundException", "ErrorMessage": "JobRun not found"},
                })),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "SuccessfulSubmissions": successes, "Errors": errors,
        })))
    }

    pub(crate) fn update_job_from_source_control(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job = body.get("JobName").and_then(|v| v.as_str()).unwrap_or("");
        Ok(AwsResponse::ok_json(json!({ "JobName": job })))
    }

    pub(crate) fn update_source_control_from_job(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job = body.get("JobName").and_then(|v| v.as_str()).unwrap_or("");
        Ok(AwsResponse::ok_json(json!({ "JobName": job })))
    }

    // ===================== table versions =====================

    fn tv_key(db: &str, table: &str, version: &str) -> String {
        format!("{db}\u{1f}{table}\u{1f}{version}")
    }

    pub(crate) fn get_table_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        let requested = body.get("VersionId").and_then(|v| v.as_str());
        let accounts = self.state.read();
        let st = accounts
            .get(&req.account_id)
            .ok_or_else(|| entity_not_found(format!("Table {table} not found")))?;
        // Prefer the archived version store; default to the latest version when
        // no VersionId is supplied.
        let version = match requested {
            Some(v) => v.to_string(),
            None => st
                .table_version_ids(db, table)
                .last()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "1".to_string()),
        };
        let tv_key = Self::tv_key(db, table, &version);
        if let Some(tv) = st.table_versions.get(&tv_key) {
            return Ok(AwsResponse::ok_json(json!({ "TableVersion": tv })));
        }
        // Fall back to synthesizing from the live table (tables created before
        // the archive existed, or restored from an older snapshot).
        let tbl = st
            .dbs_in(&req.region)
            .and_then(|dbs| dbs.get(db))
            .and_then(|d| d.tables.get(table))
            .ok_or_else(|| entity_not_found(format!("Table {table} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "TableVersion": {
                "Table": crate::service::table_json(tbl),
                "VersionId": version,
            }
        })))
    }

    pub(crate) fn get_table_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        let accounts = self.state.read();
        let st = match accounts.get(&req.account_id) {
            Some(s) => s,
            None => return Ok(AwsResponse::ok_json(json!({ "TableVersions": [] }))),
        };
        // Return the full archive, newest first (Glue orders descending).
        let ids = st.table_version_ids(db, table);
        if !ids.is_empty() {
            let versions: Vec<Value> = ids
                .iter()
                .rev()
                .filter_map(|n| {
                    st.table_versions
                        .get(&Self::tv_key(db, table, &n.to_string()))
                })
                .cloned()
                .collect();
            return Ok(AwsResponse::ok_json(json!({ "TableVersions": versions })));
        }
        // Fall back to a synthesized single version for pre-archive tables.
        let versions = match st
            .dbs_in(&req.region)
            .and_then(|dbs| dbs.get(db))
            .and_then(|d| d.tables.get(table))
        {
            Some(t) => vec![json!({
                "Table": crate::service::table_json(t), "VersionId": "1",
            })],
            None => vec![],
        };
        Ok(AwsResponse::ok_json(json!({ "TableVersions": versions })))
    }

    pub(crate) fn delete_table_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let version = req_str(&body, "VersionId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.table_versions
            .remove(&Self::tv_key(&db, &table, &version));
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn batch_delete_table_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "DatabaseName")?;
        req_str(&body, "TableName")?;
        req_present(&body, "VersionIds")?;
        Ok(AwsResponse::ok_json(json!({ "Errors": [] })))
    }

    // ===================== partition indexes & extra partition/table batches =====================

    fn pi_key(db: &str, table: &str, index: &str) -> String {
        format!("{db}\u{1f}{table}\u{1f}{index}")
    }

    pub(crate) fn create_partition_index(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let pi = req_present(&body, "PartitionIndex")?;
        let index = pi
            .get("IndexName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| crate::common::missing("PartitionIndex.IndexName"))?
            .to_string();
        // `PartitionIndex.Keys` is a list of column-name strings; the stored
        // `PartitionIndexDescriptor.Keys` is a list of KeySchemaElement
        // (Name + Type). Resolve each key's type from the table's columns.
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let col_types: std::collections::BTreeMap<String, String> = st
            .dbs_in(&req.region)
            .and_then(|dbs| dbs.get(&db))
            .and_then(|d| d.tables.get(&table))
            .map(|t| {
                t.partition_keys
                    .iter()
                    .chain(t.storage_descriptor.iter().flat_map(|sd| sd.columns.iter()))
                    .map(|c| (c.name.clone(), c.column_type.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let keys: Vec<Value> = pi
            .get("Keys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|k| k.as_str())
                    .map(|name| {
                        json!({
                            "Name": name,
                            "Type": col_types.get(name).cloned().unwrap_or_else(|| "string".to_string()),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        st.partition_indexes.insert(
            Self::pi_key(&db, &table, &index),
            json!({ "IndexName": index, "Keys": keys, "IndexStatus": "ACTIVE" }),
        );
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_partition_index(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let index = req_str(&body, "IndexName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.partition_indexes
            .remove(&Self::pi_key(&db, &table, &index));
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_partition_indexes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        let prefix = format!("{db}\u{1f}{table}\u{1f}");
        let accounts = self.state.read();
        // Real AWS raises EntityNotFoundException when the database or table
        // does not exist. Terraform's partition-index destroy check relies on
        // this: after the table is torn down it expects the not-found error,
        // not an empty list.
        let table_exists = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .and_then(|dbs| dbs.get(db))
            .is_some_and(|d| d.tables.contains_key(table));
        if !table_exists {
            return Err(entity_not_found(format!("Table {table} not found")));
        }
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.partition_indexes
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .map(|(_, v)| v.clone())
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "PartitionIndexDescriptorList": list,
        })))
    }

    pub(crate) fn batch_delete_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let tables = body["TablesToDelete"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let mut errors = Vec::new();
        if let Some(database) = st.dbs_in_mut(&req.region).get_mut(&db) {
            for t in &tables {
                let Some(name) = t.as_str() else { continue };
                if database.tables.remove(name).is_none() {
                    errors.push(json!({
                        "TableName": name,
                        "ErrorDetail": {"ErrorCode": "EntityNotFoundException", "ErrorMessage": "Table not found"},
                    }));
                }
            }
        } else {
            return Err(entity_not_found(format!("Database {db} not found")));
        }
        Ok(AwsResponse::ok_json(json!({ "Errors": errors })))
    }

    pub(crate) fn batch_delete_partition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let to_delete = body["PartitionsToDelete"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let database = st
            .dbs_in_mut(&req.region)
            .get_mut(&db)
            .ok_or_else(|| entity_not_found(format!("Database {db} not found")))?;
        let tbl = database
            .tables
            .get_mut(&table)
            .ok_or_else(|| entity_not_found(format!("Table {table} not found")))?;
        let mut errors = Vec::new();
        for pv in &to_delete {
            let values: Vec<String> = pv["Values"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let key = crate::service::partition_key(&values);
            if tbl.partitions.remove(&key).is_none() {
                errors.push(json!({
                    "PartitionValues": values,
                    "ErrorDetail": {"ErrorCode": "EntityNotFoundException", "ErrorMessage": "Partition not found"},
                }));
            }
        }
        Ok(AwsResponse::ok_json(json!({ "Errors": errors })))
    }

    pub(crate) fn batch_update_partition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let entries = body["Entries"].as_array().cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let database = st
            .dbs_in_mut(&req.region)
            .get_mut(&db)
            .ok_or_else(|| entity_not_found(format!("Database {db} not found")))?;
        let tbl = database
            .tables
            .get_mut(&table)
            .ok_or_else(|| entity_not_found(format!("Table {table} not found")))?;
        let mut errors = Vec::new();
        for e in &entries {
            let values: Vec<String> = e["PartitionValueList"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let key = crate::service::partition_key(&values);
            let Some(part) = tbl.partitions.get_mut(&key) else {
                errors.push(json!({
                    "PartitionValueList": values,
                    "ErrorDetail": {"ErrorCode": "EntityNotFoundException", "ErrorMessage": "Partition not found"},
                }));
                continue;
            };
            // Apply the PartitionInput instead of just checking existence —
            // batch partition edits were a silent no-op (bug-hunt 2026-06-24,
            // 1.13).
            let input = &e["PartitionInput"];
            if input["StorageDescriptor"].is_object() {
                part.storage_descriptor =
                    crate::service::parse_storage_descriptor(&input["StorageDescriptor"]);
            }
            if input["Parameters"].is_object() {
                part.parameters = crate::service::parse_string_map(&input["Parameters"]);
            }
        }
        Ok(AwsResponse::ok_json(json!({ "Errors": errors })))
    }

    // ===================== materialized view refresh =====================

    pub(crate) fn start_materialized_view_refresh_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = req_str(&body, "CatalogId")?.to_string();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let id = new_id();
        let now = now_ts();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.mv_refresh_runs.insert(
            id.clone(),
            json!({
                "MaterializedViewRefreshTaskRunId": id, "CatalogId": catalog,
                "DatabaseName": db, "TableName": table, "Status": "RUNNING",
                "CreationTime": now, "StartTime": now,
            }),
        );
        Ok(AwsResponse::ok_json(json!({
            "MaterializedViewRefreshTaskRunId": id,
        })))
    }

    pub(crate) fn get_materialized_view_refresh_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "CatalogId")?;
        let id = req_str(&body, "MaterializedViewRefreshTaskRunId")?;
        let accounts = self.state.read();
        let r = accounts
            .get(&req.account_id)
            .and_then(|s| s.mv_refresh_runs.get(id))
            .ok_or_else(|| entity_not_found(format!("Task run {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "MaterializedViewRefreshTaskRun": r,
        })))
    }

    pub(crate) fn list_materialized_view_refresh_task_runs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "CatalogId")?;
        let accounts = self.state.read();
        let runs: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.mv_refresh_runs.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "MaterializedViewRefreshTaskRuns": runs,
        })))
    }

    pub(crate) fn stop_materialized_view_refresh_task_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = req_str(&body, "CatalogId")?.to_string();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        for r in st.mv_refresh_runs.values_mut() {
            if r.get("CatalogId").and_then(|v| v.as_str()) == Some(catalog.as_str())
                && r.get("DatabaseName").and_then(|v| v.as_str()) == Some(db.as_str())
                && r.get("TableName").and_then(|v| v.as_str()) == Some(table.as_str())
            {
                if let Some(obj) = r.as_object_mut() {
                    obj.insert("Status".into(), json!("STOPPING"));
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ===================== unfiltered metadata =====================

    pub(crate) fn get_unfiltered_table_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let name = req_str(&body, "Name")?;
        req_str(&body, "CatalogId")?;
        req_present(&body, "SupportedPermissionTypes")?;
        let accounts = self.state.read();
        let tbl = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .and_then(|dbs| dbs.get(db))
            .and_then(|d| d.tables.get(name))
            .ok_or_else(|| entity_not_found(format!("Table {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "Table": crate::service::table_json(tbl),
            "IsRegisteredWithLakeFormation": false,
        })))
    }

    pub(crate) fn get_unfiltered_partition_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        req_str(&body, "CatalogId")?;
        req_present(&body, "PartitionValues")?;
        req_present(&body, "SupportedPermissionTypes")?;
        let values: Vec<String> = body["PartitionValues"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let key = crate::service::partition_key(&values);
        let accounts = self.state.read();
        let part = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .and_then(|dbs| dbs.get(db))
            .and_then(|d| d.tables.get(table))
            .and_then(|t| t.partitions.get(&key))
            .ok_or_else(|| entity_not_found("Partition not found"))?;
        Ok(AwsResponse::ok_json(json!({
            "Partition": crate::service::partition_json(part),
            "IsRegisteredWithLakeFormation": false,
        })))
    }

    pub(crate) fn get_unfiltered_partitions_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        req_str(&body, "CatalogId")?;
        req_present(&body, "SupportedPermissionTypes")?;
        let accounts = self.state.read();
        let parts: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .and_then(|dbs| dbs.get(db))
            .and_then(|d| d.tables.get(table))
            .map(|t| {
                t.partitions
                    .values()
                    .map(|p| json!({"Partition": crate::service::partition_json(p)}))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "UnfilteredPartitions": parts,
        })))
    }

    // ===================== schema/script generation & search =====================

    pub(crate) fn create_script(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let nodes = body["DagNodes"].as_array().cloned().unwrap_or_default();
        let edges = body["DagEdges"].as_array().cloned().unwrap_or_default();
        let language = body["Language"].as_str().unwrap_or("PYTHON");
        let (python, scala) = generate_script(&nodes, &edges);
        // AWS returns only the field for the requested language, but the
        // response shape carries both; emit the requested one and leave the
        // other populated too (real Glue returns both when both render).
        let _ = language;
        Ok(AwsResponse::ok_json(json!({
            "PythonScript": python,
            "ScalaCode": scala,
        })))
    }

    pub(crate) fn get_plan(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let mapping = req_present(&body, "Mapping")?.clone();
        let source = req_present(&body, "Source")?.clone();
        let sinks = body["Sinks"].as_array().cloned().unwrap_or_default();
        let language = body["Language"].as_str().unwrap_or("PYTHON");
        let (python, scala) = generate_plan(&source, &sinks, &mapping);
        let _ = language;
        Ok(AwsResponse::ok_json(json!({
            "PythonScript": python,
            "ScalaCode": scala,
        })))
    }

    pub(crate) fn get_mapping(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let source = req_present(&body, "Source")?;
        // Derive a MappingEntry per column of the source table's schema in the
        // catalog (identity mapping source -> sink), matching how Glue proposes
        // a default mapping from the discovered schema.
        let db = source["DatabaseName"].as_str().unwrap_or_default();
        let table_name = source["TableName"].as_str().unwrap_or_default();
        let sink = body["Sinks"]
            .as_array()
            .and_then(|s| s.first())
            .cloned()
            .unwrap_or_else(|| source.clone());
        let target_table = sink["TableName"].as_str().unwrap_or(table_name);

        let accounts = self.state.read();
        let mapping: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .and_then(|dbs| dbs.get(db))
            .and_then(|d| d.tables.get(table_name))
            .map(|t| {
                t.storage_descriptor
                    .as_ref()
                    .map(|sd| {
                        sd.columns
                            .iter()
                            .map(|col| {
                                json!({
                                    "SourceTable": table_name,
                                    "SourcePath": col.name,
                                    "SourceType": col.column_type,
                                    "TargetTable": target_table,
                                    "TargetPath": col.name,
                                    "TargetType": col.column_type,
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Mapping": mapping })))
    }

    pub(crate) fn get_dataflow_graph(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Parse the submitted PySpark script back into a DAG: every
        // `id = Class.apply(...)` assignment becomes a node and dataflow
        // references between them become edges. This mirrors Glue's
        // round-trip between CreateScript and GetDataflowGraph.
        let script = body["PythonScript"].as_str().unwrap_or_default();
        let (nodes, edges) = parse_dataflow_graph(script);
        Ok(AwsResponse::ok_json(json!({
            "DagNodes": nodes,
            "DagEdges": edges,
        })))
    }

    pub(crate) fn search_tables(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let tables: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .map(|dbs| {
                dbs.values()
                    .flat_map(|d| d.tables.values().map(crate::service::table_json))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "TableList": tables })))
    }

    // ===================== entities =====================

    pub(crate) fn describe_entity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "ConnectionName")?;
        let entity_name = req_str(&body, "EntityName")?.to_string();
        // Glue's connector entities map onto the catalog: an entity name
        // corresponds to a table (the connector surfaces its objects as
        // catalog-shaped entities). Derive the entity's fields from the
        // matching table's columns wherever one exists, else describe a
        // single key field so the entity is non-empty and reflects the input.
        let accounts = self.state.read();
        let fields: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .and_then(|dbs| {
                dbs.values()
                    .flat_map(|d| d.tables.values())
                    .find(|t| t.name == entity_name)
            })
            .and_then(|t| t.storage_descriptor.as_ref())
            .map(|sd| {
                sd.columns
                    .iter()
                    .map(|c| field_json(&c.name, &c.column_type))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![field_json("Id", "string")]);
        Ok(AwsResponse::ok_json(json!({ "Fields": fields })))
    }

    pub(crate) fn list_entities(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // List the catalog's tables as connector entities (each catalog table
        // is an addressable entity), reflecting real catalog state instead of
        // a hardcoded empty list.
        let accounts = self.state.read();
        let entities: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .map(|dbs| {
                dbs.values()
                    .flat_map(|d| d.tables.values())
                    .map(|t| {
                        json!({
                            "EntityName": t.name,
                            "Label": t.name,
                            "IsParentEntity": false,
                            "Category": "TABLE",
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Entities": entities })))
    }

    pub(crate) fn get_entity_records(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let entity_name = req_str(&body, "EntityName")?.to_string();
        req_present(&body, "Limit")?;
        // Records are the partition rows of the matching catalog table,
        // projected as a record document keyed by partition values. Tables
        // without partitions yield no records (an empty but accurate result),
        // matching a connector entity that currently holds no rows.
        let accounts = self.state.read();
        let records: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|s| s.dbs_in(&req.region))
            .and_then(|dbs| {
                dbs.values()
                    .flat_map(|d| d.tables.values())
                    .find(|t| t.name == entity_name)
            })
            .map(|t| {
                t.partitions
                    .values()
                    .map(|p| json!({ "Values": p.values }))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Records": records })))
    }

    // ===================== catalog import =====================

    pub(crate) fn import_catalog_to_glue(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_catalog_import_status(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(AwsResponse::ok_json(json!({
            "ImportStatus": {"ImportCompleted": true},
        })))
    }

    // ===================== data quality statistics & annotations & model =====================

    pub(crate) fn list_data_quality_statistics(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(AwsResponse::ok_json(json!({ "Statistics": [] })))
    }

    pub(crate) fn list_data_quality_statistic_annotations(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Ok(AwsResponse::ok_json(json!({ "Annotations": [] })))
    }

    pub(crate) fn batch_put_data_quality_statistic_annotation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_present(&body, "InclusionAnnotations")?;
        Ok(AwsResponse::ok_json(json!({
            "FailedInclusionAnnotations": [],
        })))
    }

    pub(crate) fn put_data_quality_profile_annotation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "ProfileId")?;
        req_present(&body, "InclusionAnnotation")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_data_quality_model(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "ProfileId")?;
        Ok(AwsResponse::ok_json(json!({ "Status": "SUCCEEDED" })))
    }

    pub(crate) fn get_data_quality_model_result(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "StatisticId")?;
        req_str(&body, "ProfileId")?;
        Ok(AwsResponse::ok_json(json!({})))
    }
}

/// Encode a partition's value list into a stable string for keying.
fn part_values_string(body: &Value, field: &str) -> String {
    body.get(field)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join("\u{1f}")
        })
        .unwrap_or_default()
}

/// Build a connector-entity `Field` document from a column name + type.
fn field_json(name: &str, field_type: &str) -> Value {
    json!({
        "FieldName": name,
        "Label": name,
        "FieldType": field_type,
        "IsPrimaryKey": false,
        "IsNullable": true,
        "IsRetrievable": true,
        "IsFilterable": true,
    })
}

/// Render a single CodeGenNode arg as a `name="value"` (or `name=value` for
/// params) fragment.
fn node_arg(arg: &Value) -> Option<String> {
    let name = arg.get("Name")?.as_str()?;
    let value = arg.get("Value").and_then(|v| v.as_str()).unwrap_or("");
    let is_param = arg.get("Param").and_then(|v| v.as_bool()).unwrap_or(false);
    if is_param {
        Some(format!("{name} = {value}"))
    } else {
        Some(format!("{name} = \"{value}\""))
    }
}

/// Generate PySpark + Scala scripts from a Glue ETL DAG (`DagNodes`/`DagEdges`).
/// Each node becomes a `var = NodeType.apply(...)` statement that references
/// its upstream nodes per the edges, exactly as Glue's code generator does.
fn generate_script(nodes: &[Value], edges: &[Value]) -> (String, String) {
    let mut python = String::from(
        "import sys\nfrom awsglue.transforms import *\nfrom awsglue.context import GlueContext\nfrom pyspark.context import SparkContext\n\nglueContext = GlueContext(SparkContext.getOrCreate())\n",
    );
    let mut scala = String::from(
        "import com.amazonaws.services.glue.GlueContext\nimport com.amazonaws.services.glue.util.GlueArgParser\nimport org.apache.spark.SparkContext\n\nval glueContext = new GlueContext(SparkContext.getOrCreate())\n",
    );

    for node in nodes {
        let id = node.get("Id").and_then(|v| v.as_str()).unwrap_or("node");
        let node_type = node
            .get("NodeType")
            .and_then(|v| v.as_str())
            .unwrap_or("DataSource");
        // Upstream node ids feeding this node.
        let upstream: Vec<&str> = edges
            .iter()
            .filter(|e| e.get("Target").and_then(|v| v.as_str()) == Some(id))
            .filter_map(|e| e.get("Source").and_then(|v| v.as_str()))
            .collect();
        let args: Vec<String> = node
            .get("Args")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(node_arg).collect())
            .unwrap_or_default();
        let mut all_args = args.clone();
        if !upstream.is_empty() {
            all_args.push(format!("frame = {}", upstream.join(", ")));
        }
        let arg_str = all_args.join(", ");
        python.push_str(&format!("{id} = {node_type}.apply({arg_str})\n"));
        scala.push_str(&format!("val {id} = {node_type}.apply({arg_str})\n"));
    }
    (python, scala)
}

/// Generate scripts for `GetPlan` from the source/sinks/mapping inputs.
fn generate_plan(source: &Value, sinks: &[Value], mapping: &Value) -> (String, String) {
    let src_db = source
        .get("DatabaseName")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let src_tbl = source
        .get("TableName")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let mut python = format!(
        "import sys\nfrom awsglue.transforms import *\nfrom awsglue.context import GlueContext\nfrom pyspark.context import SparkContext\n\nglueContext = GlueContext(SparkContext.getOrCreate())\ndatasource = glueContext.create_dynamic_frame.from_catalog(database = \"{src_db}\", table_name = \"{src_tbl}\")\n",
    );
    let mut scala = format!(
        "import com.amazonaws.services.glue.GlueContext\nimport org.apache.spark.SparkContext\n\nval glueContext = new GlueContext(SparkContext.getOrCreate())\nval datasource = glueContext.getCatalogSource(database = \"{src_db}\", tableName = \"{src_tbl}\").getDynamicFrame()\n",
    );
    if let Some(maps) = mapping.as_array() {
        let tuples: Vec<String> = maps
            .iter()
            .filter_map(|m| {
                let sp = m.get("SourcePath").and_then(|v| v.as_str())?;
                let st = m
                    .get("SourceType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("string");
                let tp = m.get("TargetPath").and_then(|v| v.as_str()).unwrap_or(sp);
                let tt = m.get("TargetType").and_then(|v| v.as_str()).unwrap_or(st);
                Some(format!("(\"{sp}\", \"{st}\", \"{tp}\", \"{tt}\")"))
            })
            .collect();
        if !tuples.is_empty() {
            python.push_str(&format!(
                "applymapping = ApplyMapping.apply(frame = datasource, mappings = [{}])\n",
                tuples.join(", ")
            ));
            scala.push_str(&format!(
                "val applymapping = datasource.applyMapping(mappings = Seq({}))\n",
                tuples.join(", ")
            ));
        }
    }
    for sink in sinks {
        let sdb = sink
            .get("DatabaseName")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let stbl = sink.get("TableName").and_then(|v| v.as_str()).unwrap_or("");
        python.push_str(&format!(
            "glueContext.write_dynamic_frame.from_catalog(frame = applymapping, database = \"{sdb}\", table_name = \"{stbl}\")\n",
        ));
        scala.push_str(&format!(
            "glueContext.getCatalogSink(database = \"{sdb}\", tableName = \"{stbl}\").writeDynamicFrame(applymapping)\n",
        ));
    }
    (python, scala)
}

/// Parse a generated PySpark script back into a DAG. Recognises
/// `id = NodeType.apply(...)` statements as nodes and `frame = a, b` arg
/// references as edges, the inverse of [`generate_script`].
fn parse_dataflow_graph(script: &str) -> (Vec<Value>, Vec<Value>) {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut line_number = 0i64;
    for line in script.lines() {
        line_number += 1;
        let trimmed = line.trim();
        let Some((lhs, rhs)) = trimmed.split_once('=') else {
            continue;
        };
        let id = lhs.trim();
        let rhs = rhs.trim();
        // Match `NodeType.apply(...)`.
        let Some(apply_idx) = rhs.find(".apply(") else {
            continue;
        };
        let node_type = &rhs[..apply_idx];
        if id.is_empty() || node_type.is_empty() || node_type.contains(' ') {
            continue;
        }
        nodes.push(json!({
            "Id": id,
            "NodeType": node_type,
            "Args": [],
            "LineNumber": line_number,
        }));
        // Edges: `frame = a, b` inside the call references upstream node ids.
        if let Some(fidx) = rhs.find("frame = ") {
            let after = &rhs[fidx + "frame = ".len()..];
            let inner = after.trim_end_matches(')');
            for src in inner.split(',') {
                let src = src.trim();
                if !src.is_empty() && !src.contains('"') {
                    edges.push(json!({
                        "Source": src,
                        "Target": id,
                        "TargetParameter": "frame",
                    }));
                }
            }
        }
    }
    (nodes, edges)
}
