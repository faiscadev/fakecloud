//! `AthenaService` `metadata` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl AthenaService {
    pub(super) fn get_database(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_databases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn get_table_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_table_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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
