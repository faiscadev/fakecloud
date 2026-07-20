//! Catalogs, custom entity types, user-defined functions, usage profiles, and
//! table optimizers.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity_not_found, missing, now_ts, req_present, req_str};
use crate::generic;
use crate::service::GlueService;

impl GlueService {
    // --- catalogs ---

    pub(crate) fn create_catalog(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let input = req_present(&body, "CatalogInput")?;
        let now = now_ts();
        let catalog = json!({
            "CatalogId": name, "Name": name,
            "Description": input.get("Description").cloned().unwrap_or(Value::Null),
            "Parameters": input.get("Parameters").cloned().unwrap_or(json!({})),
            "CreateTime": now, "UpdateTime": now,
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut st.catalogs, &name, catalog, "Catalog")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_catalog(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "CatalogId")?;
        let accounts = self.state.read();
        let c = accounts
            .get(&req.account_id)
            .and_then(|s| s.catalogs.get(id))
            .ok_or_else(|| entity_not_found(format!("Catalog {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Catalog": c })))
    }

    pub(crate) fn get_catalogs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.catalogs.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "CatalogList": list })))
    }

    pub(crate) fn update_catalog(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "CatalogId")?.to_string();
        let input = req_present(&body, "CatalogInput")?;
        let mut updates: Vec<(&str, Value)> = vec![("UpdateTime", json!(now_ts()))];
        for f in ["Description", "Parameters"] {
            if let Some(v) = input.get(f) {
                if !v.is_null() {
                    updates.push((f, v.clone()));
                }
            }
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut st.catalogs, &id, "Catalog", updates)?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_catalog(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "CatalogId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut st.catalogs, &id, "Catalog")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    // --- custom entity types ---

    pub(crate) fn create_custom_entity_type(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let regex = req_str(&body, "RegexString")?.to_string();
        let stored = json!({
            "Name": name, "RegexString": regex,
            "ContextWords": body.get("ContextWords").cloned().unwrap_or(Value::Null),
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(
            &mut st.custom_entity_types,
            &name,
            stored,
            "CustomEntityType",
        )?;
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn get_custom_entity_type(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let c = accounts
            .get(&req.account_id)
            .and_then(|s| s.custom_entity_types.get(name))
            .ok_or_else(|| entity_not_found(format!("CustomEntityType {name} not found")))?;
        Ok(AwsResponse::ok_json(c.clone()))
    }

    pub(crate) fn delete_custom_entity_type(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut st.custom_entity_types, &name, "CustomEntityType")?;
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn list_custom_entity_types(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.custom_entity_types.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "CustomEntityTypes": list })))
    }

    pub(crate) fn batch_get_custom_entity_types(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let names = body["Names"].as_array().cloned().unwrap_or_default();
        let accounts = self.state.read();
        let store = accounts
            .get(&req.account_id)
            .map(|s| &s.custom_entity_types);
        let mut found = Vec::new();
        let mut not_found = Vec::new();
        for n in &names {
            let Some(name) = n.as_str() else { continue };
            match store.and_then(|m| m.get(name)) {
                Some(c) => found.push(c.clone()),
                None => not_found.push(json!(name)),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "CustomEntityTypes": found, "CustomEntityTypesNotFound": not_found,
        })))
    }

    // --- user-defined functions ---

    fn udf_key(db: &str, name: &str) -> String {
        format!("{db}\u{1f}{name}")
    }

    pub(crate) fn create_user_defined_function(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let input = req_present(&body, "FunctionInput")?;
        let name = input["FunctionName"]
            .as_str()
            .ok_or_else(|| missing("FunctionInput.FunctionName"))?
            .to_string();
        let now = now_ts();
        let stored = json!({
            "FunctionName": name, "DatabaseName": db, "CatalogId": req.account_id,
            "ClassName": input.get("ClassName").cloned().unwrap_or(Value::Null),
            "OwnerName": input.get("OwnerName").cloned().unwrap_or(Value::Null),
            "OwnerType": input.get("OwnerType").cloned().unwrap_or(Value::Null),
            "ResourceUris": input.get("ResourceUris").cloned().unwrap_or(Value::Null),
            "CreateTime": now,
        });
        let key = Self::udf_key(&db, &name);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut st.udfs, &key, stored, "UserDefinedFunction")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_user_defined_function(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let name = req_str(&body, "FunctionName")?;
        let key = Self::udf_key(db, name);
        let accounts = self.state.read();
        let f = accounts
            .get(&req.account_id)
            .and_then(|s| s.udfs.get(&key))
            .ok_or_else(|| entity_not_found(format!("Function {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "UserDefinedFunction": f })))
    }

    pub(crate) fn get_user_defined_functions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "Pattern")?;
        let db = body.get("DatabaseName").and_then(|v| v.as_str());
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.udfs
                    .values()
                    .filter(|f| {
                        db.is_none_or(|d| f.get("DatabaseName").and_then(|v| v.as_str()) == Some(d))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let (page, token) = crate::common::paginate_body(&body, list)?;
        let mut resp = json!({ "UserDefinedFunctions": page });
        if let Some(t) = token {
            resp["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(crate) fn update_user_defined_function(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let name = req_str(&body, "FunctionName")?;
        let input = req_present(&body, "FunctionInput")?;
        let key = Self::udf_key(db, name);
        let mut updates: Vec<(&str, Value)> = Vec::new();
        for f in ["ClassName", "OwnerName", "OwnerType", "ResourceUris"] {
            if let Some(v) = input.get(f) {
                if !v.is_null() {
                    updates.push((f, v.clone()));
                }
            }
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut st.udfs, &key, "UserDefinedFunction", updates)?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_user_defined_function(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let db = req_str(&body, "DatabaseName")?;
        let name = req_str(&body, "FunctionName")?;
        let key = Self::udf_key(db, name);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut st.udfs, &key, "UserDefinedFunction")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    // --- usage profiles ---

    pub(crate) fn create_usage_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        req_present(&body, "Configuration")?;
        let now = now_ts();
        let stored = json!({
            "Name": name,
            "Description": body.get("Description").cloned().unwrap_or(Value::Null),
            "Configuration": body.get("Configuration").cloned().unwrap_or(json!({})),
            "CreatedOn": now, "LastModifiedOn": now,
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut st.usage_profiles, &name, stored, "UsageProfile")?;
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn get_usage_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let p = accounts
            .get(&req.account_id)
            .and_then(|s| s.usage_profiles.get(name))
            .ok_or_else(|| entity_not_found(format!("UsageProfile {name} not found")))?;
        Ok(AwsResponse::ok_json(p.clone()))
    }

    pub(crate) fn update_usage_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        req_present(&body, "Configuration")?;
        let mut updates: Vec<(&str, Value)> = vec![
            ("Configuration", body["Configuration"].clone()),
            ("LastModifiedOn", json!(now_ts())),
        ];
        if let Some(d) = body.get("Description") {
            updates.push(("Description", d.clone()));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut st.usage_profiles, &name, "UsageProfile", updates)?;
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn delete_usage_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        // DeleteUsageProfile does not declare EntityNotFoundException; idempotent.
        st.usage_profiles.remove(&name);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn list_usage_profiles(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.usage_profiles
                    .values()
                    .map(|p| {
                        json!({
                            "Name": p.get("Name").cloned().unwrap_or(Value::Null),
                            "Description": p.get("Description").cloned().unwrap_or(Value::Null),
                            "CreatedOn": p.get("CreatedOn").cloned().unwrap_or(Value::Null),
                            "LastModifiedOn": p.get("LastModifiedOn").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Profiles": list })))
    }

    // --- table optimizers ---

    fn optimizer_key(catalog: &str, db: &str, table: &str, ty: &str) -> String {
        format!("{catalog}\u{1f}{db}\u{1f}{table}\u{1f}{ty}")
    }

    pub(crate) fn create_table_optimizer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = req_str(&body, "CatalogId")?.to_string();
        let db = req_str(&body, "DatabaseName")?.to_string();
        let table = req_str(&body, "TableName")?.to_string();
        let ty = req_str(&body, "Type")?.to_string();
        let config = req_present(&body, "TableOptimizerConfiguration")?.clone();
        let key = Self::optimizer_key(&catalog, &db, &table, &ty);
        let stored = json!({
            "CatalogId": catalog, "DatabaseName": db, "TableName": table,
            "TableOptimizer": {"type": ty, "configuration": config},
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut st.table_optimizers, &key, stored, "TableOptimizer")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_table_optimizer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = req_str(&body, "CatalogId")?;
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        let ty = req_str(&body, "Type")?;
        let key = Self::optimizer_key(catalog, db, table, ty);
        let accounts = self.state.read();
        let o = accounts
            .get(&req.account_id)
            .and_then(|s| s.table_optimizers.get(&key))
            .ok_or_else(|| entity_not_found("TableOptimizer not found"))?;
        Ok(AwsResponse::ok_json(o.clone()))
    }

    pub(crate) fn update_table_optimizer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = req_str(&body, "CatalogId")?;
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        let ty = req_str(&body, "Type")?;
        let config = req_present(&body, "TableOptimizerConfiguration")?.clone();
        let key = Self::optimizer_key(catalog, db, table, ty);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let o = st
            .table_optimizers
            .get_mut(&key)
            .ok_or_else(|| entity_not_found("TableOptimizer not found"))?;
        if let Some(obj) = o.as_object_mut() {
            obj.insert(
                "TableOptimizer".into(),
                json!({"type": ty, "configuration": config}),
            );
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_table_optimizer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = req_str(&body, "CatalogId")?;
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        let ty = req_str(&body, "Type")?;
        let key = Self::optimizer_key(catalog, db, table, ty);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut st.table_optimizers, &key, "TableOptimizer")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn batch_get_table_optimizer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let entries = body["Entries"].as_array().cloned().unwrap_or_default();
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id).map(|s| &s.table_optimizers);
        let mut found = Vec::new();
        let mut failures = Vec::new();
        for e in &entries {
            let catalog = e.get("catalogId").and_then(|v| v.as_str()).unwrap_or("");
            let db = e.get("databaseName").and_then(|v| v.as_str()).unwrap_or("");
            let table = e.get("tableName").and_then(|v| v.as_str()).unwrap_or("");
            let ty = e.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let key = Self::optimizer_key(catalog, db, table, ty);
            match store.and_then(|m| m.get(&key)) {
                Some(o) => found.push(json!({
                    "catalogId": catalog, "databaseName": db, "tableName": table,
                    "tableOptimizer": o.get("TableOptimizer").cloned().unwrap_or(Value::Null),
                })),
                None => failures.push(json!({
                    "catalogId": catalog, "databaseName": db, "tableName": table, "type": ty,
                    "error": {"ErrorCode": "EntityNotFoundException", "ErrorMessage": "TableOptimizer not found"},
                })),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "TableOptimizers": found, "Failures": failures,
        })))
    }

    pub(crate) fn list_table_optimizer_runs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let catalog = req_str(&body, "CatalogId")?;
        let db = req_str(&body, "DatabaseName")?;
        let table = req_str(&body, "TableName")?;
        req_str(&body, "Type")?;
        Ok(AwsResponse::ok_json(json!({
            "CatalogId": catalog, "DatabaseName": db, "TableName": table,
            "TableOptimizerRuns": [],
        })))
    }
}
