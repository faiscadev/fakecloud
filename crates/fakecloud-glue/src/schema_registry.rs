//! Schema Registry: registries, schemas, schema versions, version metadata.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{
    entity_not_found, invalid_input, new_uuid, now_ts, req_present, req_str, resource_arn,
};
use crate::generic;
use crate::service::GlueService;

/// Resolve the registry name from a `RegistryId` input shape (by name or ARN).
fn registry_name(id: &Value) -> Option<String> {
    if let Some(n) = id.get("RegistryName").and_then(|v| v.as_str()) {
        return Some(n.to_string());
    }
    id.get("RegistryArn")
        .and_then(|v| v.as_str())
        .and_then(|arn| arn.rsplit('/').next())
        .map(|s| s.to_string())
}

/// Resolve a schema's storage key (registry/schema) from a `SchemaId`.
///
/// A `SchemaId` identifies a schema either by `RegistryName` + `SchemaName` or
/// by `SchemaArn`. The ARN's resource path is `schema/<registry>/<schema>`, so
/// both the registry and schema name must come out of the ARN — not just the
/// trailing segment (otherwise a by-ARN lookup keys off the wrong registry).
fn schema_key(id: &Value) -> Option<String> {
    if let Some(name) = id.get("SchemaName").and_then(|v| v.as_str()) {
        let reg = id
            .get("RegistryName")
            .and_then(|v| v.as_str())
            .unwrap_or("default-registry");
        return Some(format!("{reg}\u{1f}{name}"));
    }
    let arn = id.get("SchemaArn").and_then(|v| v.as_str())?;
    // `...:schema/<registry>/<schema>` -> ("<registry>", "<schema>")
    let resource = arn.split(":schema/").nth(1).unwrap_or(arn);
    let (reg, name) = resource.split_once('/')?;
    Some(format!("{reg}\u{1f}{name}"))
}

impl GlueService {
    // --- registries ---

    pub(crate) fn create_registry(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "RegistryName")?.to_string();
        let arn = resource_arn(&req.account_id, &req.region, "registry", &name);
        let desc = body.get("Description").cloned().unwrap_or(Value::Null);
        let tags = body.get("Tags").cloned().unwrap_or(json!({}));
        let now = now_ts();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(
            &mut st.registries,
            &name,
            json!({
                "RegistryName": name, "RegistryArn": arn, "Description": desc,
                "Status": "AVAILABLE", "CreatedTime": now.to_string(), "UpdatedTime": now.to_string(),
            }),
            "Registry",
        )?;
        Ok(AwsResponse::ok_json(json!({
            "RegistryArn": arn, "RegistryName": name, "Description": desc, "Tags": tags,
        })))
    }

    pub(crate) fn get_registry(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_present(&body, "RegistryId")?;
        let name = registry_name(id).ok_or_else(|| invalid_input("RegistryId required"))?;
        let accounts = self.state.read();
        let r = accounts
            .get(&req.account_id)
            .and_then(|s| s.registries.get(&name))
            .ok_or_else(|| entity_not_found(format!("Registry {name} not found")))?;
        Ok(AwsResponse::ok_json(r.clone()))
    }

    pub(crate) fn update_registry(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_present(&body, "RegistryId")?;
        let name = registry_name(id).ok_or_else(|| invalid_input("RegistryId required"))?;
        req_str(&body, "Description")?;
        let desc = body["Description"].clone();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(
            &mut st.registries,
            &name,
            "Registry",
            vec![
                ("Description", desc),
                ("UpdatedTime", json!(now_ts().to_string())),
            ],
        )?;
        let arn = resource_arn(&req.account_id, &req.region, "registry", &name);
        Ok(AwsResponse::ok_json(json!({
            "RegistryName": name, "RegistryArn": arn,
        })))
    }

    pub(crate) fn delete_registry(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_present(&body, "RegistryId")?;
        let name = registry_name(id).ok_or_else(|| invalid_input("RegistryId required"))?;
        let arn = resource_arn(&req.account_id, &req.region, "registry", &name);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut st.registries, &name, "Registry")?;
        Ok(AwsResponse::ok_json(json!({
            "RegistryName": name, "RegistryArn": arn, "Status": "DELETING",
        })))
    }

    pub(crate) fn list_registries(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.registries.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Registries": list })))
    }

    // --- schemas ---

    pub(crate) fn create_schema(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "SchemaName")?.to_string();
        let data_format = req_str(&body, "DataFormat")?.to_string();
        let reg = body
            .get("RegistryId")
            .and_then(registry_name)
            .unwrap_or_else(|| "default-registry".to_string());
        let key = format!("{reg}\u{1f}{name}");
        let schema_arn = resource_arn(
            &req.account_id,
            &req.region,
            "schema",
            &format!("{reg}/{name}"),
        );
        let reg_arn = resource_arn(&req.account_id, &req.region, "registry", &reg);
        let compat = body
            .get("Compatibility")
            .and_then(|v| v.as_str())
            .unwrap_or("BACKWARD")
            .to_string();
        let desc = body.get("Description").cloned().unwrap_or(Value::Null);
        let now = now_ts();
        let has_def = body
            .get("SchemaDefinition")
            .and_then(|v| v.as_str())
            .is_some();

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let schema = json!({
            "RegistryName": reg, "RegistryArn": reg_arn, "SchemaName": name, "SchemaArn": schema_arn,
            "Description": desc, "DataFormat": data_format, "Compatibility": compat,
            "SchemaCheckpoint": 1, "LatestSchemaVersion": if has_def {1} else {0},
            "NextSchemaVersion": if has_def {2} else {1},
            "SchemaStatus": "AVAILABLE", "CreatedTime": now.to_string(), "UpdatedTime": now.to_string(),
        });
        generic::create_unique(&mut st.schemas, &key, schema, "Schema")?;

        let mut version_id = Value::Null;
        if has_def {
            let vid = new_uuid();
            version_id = json!(vid);
            st.schema_versions.insert(
                vid.clone(),
                json!({
                    "SchemaVersionId": vid, "SchemaArn": resource_arn(&req.account_id,&req.region,"schema",&format!("{reg}/{name}")),
                    "SchemaName": name, "RegistryName": reg,
                    "SchemaDefinition": body["SchemaDefinition"].clone(),
                    "DataFormat": data_format, "VersionNumber": 1,
                    "Status": "AVAILABLE", "CreatedTime": now.to_string(),
                }),
            );
        }
        let mut out = json!({
            "RegistryName": reg, "RegistryArn": resource_arn(&req.account_id,&req.region,"registry",&reg),
            "SchemaName": name, "SchemaArn": resource_arn(&req.account_id,&req.region,"schema",&format!("{reg}/{name}")),
            "Description": desc, "DataFormat": data_format, "Compatibility": compat,
            "SchemaCheckpoint": 1, "LatestSchemaVersion": if has_def {1} else {0},
            "NextSchemaVersion": if has_def {2} else {1}, "SchemaStatus": "AVAILABLE",
        });
        if !version_id.is_null() {
            out["SchemaVersionId"] = version_id;
            out["SchemaVersionStatus"] = json!("AVAILABLE");
        }
        if let Some(tags) = body.get("Tags") {
            out["Tags"] = tags.clone();
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(crate) fn get_schema(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_present(&body, "SchemaId")?;
        let key = schema_key(id).ok_or_else(|| invalid_input("SchemaId required"))?;
        let accounts = self.state.read();
        let s = accounts
            .get(&req.account_id)
            .and_then(|st| st.schemas.get(&key))
            .ok_or_else(|| entity_not_found("Schema not found"))?;
        Ok(AwsResponse::ok_json(s.clone()))
    }

    pub(crate) fn delete_schema(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_present(&body, "SchemaId")?;
        let key = schema_key(id).ok_or_else(|| invalid_input("SchemaId required"))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let s = st
            .schemas
            .remove(&key)
            .ok_or_else(|| entity_not_found("Schema not found"))?;
        Ok(AwsResponse::ok_json(json!({
            "SchemaArn": s.get("SchemaArn").cloned().unwrap_or(Value::Null),
            "SchemaName": s.get("SchemaName").cloned().unwrap_or(Value::Null),
            "Status": "DELETING",
        })))
    }

    pub(crate) fn update_schema(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_present(&body, "SchemaId")?;
        let key = schema_key(id).ok_or_else(|| invalid_input("SchemaId required"))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let s = st
            .schemas
            .get_mut(&key)
            .ok_or_else(|| entity_not_found("Schema not found"))?;
        if let Some(obj) = s.as_object_mut() {
            if let Some(c) = body.get("Compatibility") {
                obj.insert("Compatibility".into(), c.clone());
            }
            if let Some(d) = body.get("Description") {
                obj.insert("Description".into(), d.clone());
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "SchemaArn": s.get("SchemaArn").cloned().unwrap_or(Value::Null),
            "SchemaName": s.get("SchemaName").cloned().unwrap_or(Value::Null),
            "RegistryName": s.get("RegistryName").cloned().unwrap_or(Value::Null),
        })))
    }

    pub(crate) fn list_schemas(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let reg_filter = body.get("RegistryId").and_then(registry_name);
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.schemas
                    .values()
                    .filter(|sc| {
                        reg_filter.as_ref().is_none_or(|r| {
                            sc.get("RegistryName").and_then(|n| n.as_str()) == Some(r.as_str())
                        })
                    })
                    .map(|sc| {
                        json!({
                            "RegistryName": sc.get("RegistryName").cloned().unwrap_or(Value::Null),
                            "SchemaName": sc.get("SchemaName").cloned().unwrap_or(Value::Null),
                            "SchemaArn": sc.get("SchemaArn").cloned().unwrap_or(Value::Null),
                            "Description": sc.get("Description").cloned().unwrap_or(Value::Null),
                            "SchemaStatus": sc.get("SchemaStatus").cloned().unwrap_or(Value::Null),
                            "CreatedTime": sc.get("CreatedTime").cloned().unwrap_or(Value::Null),
                            "UpdatedTime": sc.get("UpdatedTime").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Schemas": list })))
    }

    // --- schema versions ---

    pub(crate) fn register_schema_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_present(&body, "SchemaId")?;
        let key = schema_key(id).ok_or_else(|| invalid_input("SchemaId required"))?;
        let def = req_str(&body, "SchemaDefinition")?.to_string();
        let now = now_ts();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let schema = st
            .schemas
            .get_mut(&key)
            .ok_or_else(|| entity_not_found("Schema not found"))?;
        let next = schema
            .get("NextSchemaVersion")
            .and_then(|v| v.as_i64())
            .unwrap_or(2);
        let (reg, name, arn) = (
            schema.get("RegistryName").cloned().unwrap_or(Value::Null),
            schema.get("SchemaName").cloned().unwrap_or(Value::Null),
            schema.get("SchemaArn").cloned().unwrap_or(Value::Null),
        );
        let data_format = schema.get("DataFormat").cloned().unwrap_or(Value::Null);
        if let Some(obj) = schema.as_object_mut() {
            obj.insert("LatestSchemaVersion".into(), json!(next));
            obj.insert("NextSchemaVersion".into(), json!(next + 1));
        }
        let vid = new_uuid();
        st.schema_versions.insert(
            vid.clone(),
            json!({
                "SchemaVersionId": vid, "SchemaArn": arn, "SchemaName": name, "RegistryName": reg,
                "SchemaDefinition": def, "DataFormat": data_format,
                "VersionNumber": next, "Status": "AVAILABLE", "CreatedTime": now.to_string(),
            }),
        );
        Ok(AwsResponse::ok_json(json!({
            "SchemaVersionId": vid, "VersionNumber": next, "Status": "AVAILABLE",
        })))
    }

    pub(crate) fn get_schema_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id).map(|s| &s.schema_versions);
        let version = if let Some(vid) = body.get("SchemaVersionId").and_then(|v| v.as_str()) {
            store.and_then(|m| m.get(vid)).cloned()
        } else if let Some(key) = body.get("SchemaId").and_then(schema_key) {
            let want = body
                .get("SchemaVersionNumber")
                .and_then(|v| v.get("VersionNumber"))
                .and_then(|v| v.as_i64());
            store.and_then(|m| {
                m.values()
                    .filter(|v| matches_schema(v, &key))
                    .filter(|v| {
                        want.is_none_or(|n| {
                            v.get("VersionNumber").and_then(|x| x.as_i64()) == Some(n)
                        })
                    })
                    .max_by_key(|v| v.get("VersionNumber").and_then(|x| x.as_i64()).unwrap_or(0))
                    .cloned()
            })
        } else {
            None
        };
        let v = version.ok_or_else(|| entity_not_found("Schema version not found"))?;
        Ok(AwsResponse::ok_json(json!({
            "SchemaVersionId": v.get("SchemaVersionId").cloned().unwrap_or(Value::Null),
            "SchemaDefinition": v.get("SchemaDefinition").cloned().unwrap_or(Value::Null),
            "DataFormat": v.get("DataFormat").cloned().unwrap_or(Value::Null),
            "SchemaArn": v.get("SchemaArn").cloned().unwrap_or(Value::Null),
            "VersionNumber": v.get("VersionNumber").cloned().unwrap_or(Value::Null),
            "Status": v.get("Status").cloned().unwrap_or(Value::Null),
            "CreatedTime": v.get("CreatedTime").cloned().unwrap_or(Value::Null),
        })))
    }

    pub(crate) fn get_schema_by_definition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key = body
            .get("SchemaId")
            .and_then(schema_key)
            .ok_or_else(|| invalid_input("SchemaId required"))?;
        let def = req_str(&body, "SchemaDefinition")?;
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| {
                s.schema_versions
                    .values()
                    .find(|v| {
                        matches_schema(v, &key)
                            && v.get("SchemaDefinition").and_then(|d| d.as_str()) == Some(def)
                    })
                    .cloned()
            })
            .ok_or_else(|| entity_not_found("Schema version not found"))?;
        Ok(AwsResponse::ok_json(json!({
            "SchemaVersionId": v.get("SchemaVersionId").cloned().unwrap_or(Value::Null),
            "SchemaArn": v.get("SchemaArn").cloned().unwrap_or(Value::Null),
            "DataFormat": v.get("DataFormat").cloned().unwrap_or(Value::Null),
            "Status": v.get("Status").cloned().unwrap_or(Value::Null),
            "CreatedTime": v.get("CreatedTime").cloned().unwrap_or(Value::Null),
        })))
    }

    pub(crate) fn list_schema_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key = body
            .get("SchemaId")
            .and_then(schema_key)
            .ok_or_else(|| invalid_input("SchemaId required"))?;
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.schema_versions
                    .values()
                    .filter(|v| matches_schema(v, &key))
                    .map(|v| {
                        json!({
                            "SchemaVersionId": v.get("SchemaVersionId").cloned().unwrap_or(Value::Null),
                            "VersionNumber": v.get("VersionNumber").cloned().unwrap_or(Value::Null),
                            "Status": v.get("Status").cloned().unwrap_or(Value::Null),
                            "CreatedTime": v.get("CreatedTime").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Schemas": list })))
    }

    pub(crate) fn delete_schema_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_present(&body, "SchemaId")?;
        req_str(&body, "Versions")?;
        Ok(AwsResponse::ok_json(json!({ "SchemaVersionErrors": [] })))
    }

    pub(crate) fn check_schema_version_validity(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "DataFormat")?;
        req_str(&body, "SchemaDefinition")?;
        Ok(AwsResponse::ok_json(json!({ "Valid": true })))
    }

    pub(crate) fn get_schema_versions_diff(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_present(&body, "SchemaId")?;
        req_present(&body, "FirstSchemaVersionNumber")?;
        req_present(&body, "SecondSchemaVersionNumber")?;
        req_str(&body, "SchemaDiffType")?;
        Ok(AwsResponse::ok_json(json!({ "Diff": "" })))
    }

    // --- schema version metadata ---

    pub(crate) fn put_schema_version_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kv = req_present(&body, "MetadataKeyValue")?;
        let mk = kv.get("MetadataKey").and_then(|v| v.as_str()).unwrap_or("");
        let mv = kv
            .get("MetadataValue")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let (vid, name, reg, arn) = self.resolve_schema_version_ctx(req, &body)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.schema_version_metadata.insert(
            format!("{vid}\u{1f}{mk}\u{1f}{mv}"),
            json!({"key": mk, "value": mv}),
        );
        Ok(AwsResponse::ok_json(json!({
            "SchemaArn": arn, "SchemaName": name, "RegistryName": reg,
            "LatestVersion": false, "VersionNumber": 1,
            "SchemaVersionId": vid, "MetadataKey": mk, "MetadataValue": mv,
        })))
    }

    pub(crate) fn remove_schema_version_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kv = req_present(&body, "MetadataKeyValue")?;
        let mk = kv.get("MetadataKey").and_then(|v| v.as_str()).unwrap_or("");
        let mv = kv
            .get("MetadataValue")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let (vid, name, reg, arn) = self.resolve_schema_version_ctx(req, &body)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        st.schema_version_metadata
            .remove(&format!("{vid}\u{1f}{mk}\u{1f}{mv}"));
        Ok(AwsResponse::ok_json(json!({
            "SchemaArn": arn, "SchemaName": name, "RegistryName": reg,
            "LatestVersion": false, "VersionNumber": 1,
            "SchemaVersionId": vid, "MetadataKey": mk, "MetadataValue": mv,
        })))
    }

    pub(crate) fn query_schema_version_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let (vid, _, _, _) = self
            .resolve_schema_version_ctx(req, &body)
            .unwrap_or_else(|_| (String::new(), Value::Null, Value::Null, Value::Null));
        let accounts = self.state.read();
        let mut map = serde_json::Map::new();
        if let Some(st) = accounts.get(&req.account_id) {
            for (k, v) in &st.schema_version_metadata {
                if k.starts_with(&format!("{vid}\u{1f}")) {
                    let key = v.get("key").and_then(|x| x.as_str()).unwrap_or("");
                    let val = v.get("value").and_then(|x| x.as_str()).unwrap_or("");
                    map.insert(
                        key.to_string(),
                        json!({"MetadataValue": val, "OtherMetadataValueList": []}),
                    );
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "MetadataInfoMap": Value::Object(map),
            "SchemaVersionId": vid,
        })))
    }

    fn resolve_schema_version_ctx(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<(String, Value, Value, Value), AwsServiceError> {
        if let Some(vid) = body.get("SchemaVersionId").and_then(|v| v.as_str()) {
            let accounts = self.state.read();
            if let Some(v) = accounts
                .get(&req.account_id)
                .and_then(|s| s.schema_versions.get(vid))
            {
                return Ok((
                    vid.to_string(),
                    v.get("SchemaName").cloned().unwrap_or(Value::Null),
                    v.get("RegistryName").cloned().unwrap_or(Value::Null),
                    v.get("SchemaArn").cloned().unwrap_or(Value::Null),
                ));
            }
            return Ok((vid.to_string(), Value::Null, Value::Null, Value::Null));
        }
        if let Some(key) = body.get("SchemaId").and_then(schema_key) {
            let accounts = self.state.read();
            if let Some(v) = accounts
                .get(&req.account_id)
                .and_then(|s| s.schema_versions.values().find(|v| matches_schema(v, &key)))
            {
                return Ok((
                    v.get("SchemaVersionId")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    v.get("SchemaName").cloned().unwrap_or(Value::Null),
                    v.get("RegistryName").cloned().unwrap_or(Value::Null),
                    v.get("SchemaArn").cloned().unwrap_or(Value::Null),
                ));
            }
        }
        Err(invalid_input(
            "SchemaVersionId or SchemaId with version required",
        ))
    }
}

fn matches_schema(version: &Value, key: &str) -> bool {
    let reg = version
        .get("RegistryName")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let name = version
        .get("SchemaName")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    format!("{reg}\u{1f}{name}") == key
}
