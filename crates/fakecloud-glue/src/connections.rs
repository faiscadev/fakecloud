//! Connections, connection types, security configurations, resource policies,
//! data-catalog encryption settings, tags, and TestConnection.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{
    entity, entity_not_found, missing, new_id, now_ts, req_present, req_str, resource_arn,
};
use crate::generic;
use crate::service::GlueService;

/// Connection-output fields whose Smithy targets are identical between
/// `ConnectionInput` and `Connection`, so echoing the stored input value is
/// shape-safe.
const CONNECTION_FIELDS: &[&str] = &[
    "Name",
    "Description",
    "ConnectionType",
    "MatchCriteria",
    "ConnectionProperties",
    "SparkProperties",
    "AthenaProperties",
    "PythonProperties",
    "PhysicalConnectionRequirements",
];

impl GlueService {
    pub(crate) fn create_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let input = req_present(&body, "ConnectionInput")?;
        let name = input["Name"]
            .as_str()
            .ok_or_else(|| missing("ConnectionInput.Name"))?
            .to_string();
        let now = now_ts();
        let stored = entity(
            input,
            CONNECTION_FIELDS,
            vec![
                ("CreationTime", json!(now)),
                ("LastUpdatedTime", json!(now)),
                ("Status", json!("READY")),
            ],
        );
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut state.connections, &name, stored, "Connection")?;
        Ok(AwsResponse::ok_json(
            json!({ "CreateConnectionStatus": "READY" }),
        ))
    }

    pub(crate) fn get_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let c = accounts
            .get(&req.account_id)
            .and_then(|s| s.connections.get(name))
            .ok_or_else(|| entity_not_found(format!("Connection {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Connection": c })))
    }

    pub(crate) fn get_connections(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.connections.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "ConnectionList": list })))
    }

    pub(crate) fn update_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let input = req_present(&body, "ConnectionInput")?;
        let mut updates: Vec<(&str, Value)> = Vec::new();
        for f in CONNECTION_FIELDS {
            if let Some(v) = input.get(*f) {
                if !v.is_null() {
                    updates.push((f, v.clone()));
                }
            }
        }
        updates.push(("LastUpdatedTime", json!(now_ts())));
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut state.connections, &name, "Connection", updates)?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "ConnectionName")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut state.connections, &name, "Connection")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn batch_delete_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let names = body["ConnectionNameList"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let mut succeeded = Vec::new();
        let mut errors = serde_json::Map::new();
        for n in &names {
            let Some(name) = n.as_str() else { continue };
            if state.connections.remove(name).is_some() {
                succeeded.push(json!(name));
            } else {
                errors.insert(
                    name.to_string(),
                    crate::common::error_detail(
                        "EntityNotFoundException",
                        format!("Connection {name} not found"),
                    ),
                );
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "Succeeded": succeeded,
            "Errors": Value::Object(errors),
        })))
    }

    pub(crate) fn test_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // AWS accepts either a named, already-created connection
        // (`ConnectionName`) or an inline `TestConnectionInput`. Validate that
        // one of them is present and resolvable, then return the empty success
        // body AWS sends. A missing named connection is a real error.
        let body = req.json_body();
        if let Some(name) = body.get("ConnectionName").and_then(|v| v.as_str()) {
            let accounts = self.state.read();
            let exists = accounts
                .get(&req.account_id)
                .map(|s| s.connections.contains_key(name))
                .unwrap_or(false);
            if !exists {
                return Err(entity_not_found(format!("Connection {name} not found")));
            }
        } else {
            // Inline test input: require the connection type + properties that
            // a real connection attempt needs, mirroring AWS validation.
            let input = req_present(&body, "TestConnectionInput")?;
            if input.get("ConnectionType").and_then(|v| v.as_str()).is_none() {
                return Err(missing("TestConnectionInput.ConnectionType"));
            }
            req_present(input, "ConnectionProperties")?;
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    // --- connection types ---

    pub(crate) fn register_connection_type(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ct = req_str(&body, "ConnectionType")?.to_string();
        req_str(&body, "IntegrationType")?;
        req_present(&body, "ConnectionProperties")?;
        req_present(&body, "ConnectorAuthenticationConfiguration")?;
        req_present(&body, "RestConfiguration")?;
        let arn = resource_arn(&req.account_id, &req.region, "connectionType", &ct);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        state.connection_types.insert(
            ct.clone(),
            json!({
                "ConnectionType": ct,
                "Description": body.get("Description").cloned().unwrap_or(Value::Null),
                "ConnectionProperties": body.get("ConnectionProperties").cloned().unwrap_or(json!({})),
            }),
        );
        Ok(AwsResponse::ok_json(json!({ "ConnectionTypeArn": arn })))
    }

    pub(crate) fn delete_connection_type(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ct = req_str(&body, "ConnectionType")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut state.connection_types, &ct, "ConnectionType")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn describe_connection_type(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ct = req_str(&body, "ConnectionType")?;
        let accounts = self.state.read();
        let stored = accounts
            .get(&req.account_id)
            .and_then(|s| s.connection_types.get(ct));
        let desc = stored
            .and_then(|v| v.get("Description"))
            .cloned()
            .unwrap_or(Value::Null);
        Ok(AwsResponse::ok_json(json!({
            "ConnectionType": ct,
            "Description": desc,
        })))
    }

    pub(crate) fn list_connection_types(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.connection_types
                    .keys()
                    .map(|ct| json!({"ConnectionType": ct}))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "ConnectionTypes": list })))
    }

    // --- security configurations ---

    pub(crate) fn create_security_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let enc = req_present(&body, "EncryptionConfiguration")?.clone();
        let now = now_ts();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(
            &mut state.security_configs,
            &name,
            json!({
                "Name": name,
                "CreatedTimeStamp": now,
                "EncryptionConfiguration": enc,
            }),
            "SecurityConfiguration",
        )?;
        Ok(AwsResponse::ok_json(json!({
            "Name": name,
            "CreatedTimestamp": now,
        })))
    }

    pub(crate) fn get_security_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let c = accounts
            .get(&req.account_id)
            .and_then(|s| s.security_configs.get(name))
            .ok_or_else(|| entity_not_found(format!("SecurityConfiguration {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "SecurityConfiguration": c })))
    }

    pub(crate) fn get_security_configurations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.security_configs.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "SecurityConfigurations": list }),
        ))
    }

    pub(crate) fn delete_security_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut state.security_configs, &name, "SecurityConfiguration")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    // --- data-catalog encryption settings ---

    pub(crate) fn put_data_catalog_encryption_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let settings = req_present(&body, "DataCatalogEncryptionSettings")?.clone();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        state.encryption_settings = Some(settings);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_data_catalog_encryption_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let settings = accounts
            .get(&req.account_id)
            .and_then(|s| s.encryption_settings.clone())
            .unwrap_or_else(|| json!({}));
        Ok(AwsResponse::ok_json(json!({
            "DataCatalogEncryptionSettings": settings,
        })))
    }

    // --- resource policies ---

    pub(crate) fn put_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy = req_str(&body, "PolicyInJson")?.to_string();
        let arn = body["ResourceArn"].as_str().unwrap_or("").to_string();
        let hash = new_id();
        let now = now_ts();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        state.resource_policies.insert(
            arn,
            json!({
                "PolicyInJson": policy,
                "PolicyHash": hash,
                "CreateTime": now,
                "UpdateTime": now,
            }),
        );
        Ok(AwsResponse::ok_json(json!({ "PolicyHash": hash })))
    }

    pub(crate) fn get_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["ResourceArn"].as_str().unwrap_or("");
        let accounts = self.state.read();
        let p = accounts
            .get(&req.account_id)
            .and_then(|s| s.resource_policies.get(arn))
            .ok_or_else(|| entity_not_found("Resource policy not found"))?;
        Ok(AwsResponse::ok_json(p.clone()))
    }

    pub(crate) fn get_resource_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.resource_policies
                    .values()
                    .map(|p| {
                        json!({
                            "PolicyInJson": p.get("PolicyInJson").cloned().unwrap_or(Value::Null),
                            "PolicyHash": p.get("PolicyHash").cloned().unwrap_or(Value::Null),
                            "CreateTime": p.get("CreateTime").cloned().unwrap_or(Value::Null),
                            "UpdateTime": p.get("UpdateTime").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "GetResourcePoliciesResponseList": list,
        })))
    }

    pub(crate) fn delete_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["ResourceArn"].as_str().unwrap_or("").to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        state.resource_policies.remove(&arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    // --- tags ---

    pub(crate) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?.to_string();
        let add = req_present(&body, "TagsToAdd")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let entry = state.tags.entry(arn).or_default();
        if let Some(obj) = add.as_object() {
            for (k, v) in obj {
                if let Some(s) = v.as_str() {
                    entry.insert(k.clone(), s.to_string());
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?.to_string();
        let remove = req_present(&body, "TagsToRemove")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        if let Some(entry) = state.tags.get_mut(&arn) {
            if let Some(arr) = remove.as_array() {
                for k in arr {
                    if let Some(s) = k.as_str() {
                        entry.remove(s);
                    }
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "ResourceArn")?;
        let accounts = self.state.read();
        let tags = accounts
            .get(&req.account_id)
            .and_then(|s| s.tags.get(arn))
            .cloned()
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Tags": tags })))
    }
}
