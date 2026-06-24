//! Blueprints, blueprint runs, and dev endpoints.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity_not_found, new_id, now_ts, req_str};
use crate::generic;
use crate::service::GlueService;

impl GlueService {
    // --- blueprints ---

    pub(crate) fn create_blueprint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let location = req_str(&body, "BlueprintLocation")?.to_string();
        let now = now_ts();
        let stored = json!({
            "Name": name,
            "Description": body.get("Description").cloned().unwrap_or(Value::Null),
            "BlueprintLocation": location,
            "CreatedOn": now,
            "LastModifiedOn": now,
            "Status": "ACTIVE",
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut st.blueprints, &name, stored, "Blueprint")?;
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn get_blueprint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let b = accounts
            .get(&req.account_id)
            .and_then(|s| s.blueprints.get(name))
            .ok_or_else(|| entity_not_found(format!("Blueprint {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Blueprint": b })))
    }

    pub(crate) fn batch_get_blueprints(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let names = body["Names"].as_array().cloned().unwrap_or_default();
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id).map(|s| &s.blueprints);
        let mut found = Vec::new();
        let mut missing_b = Vec::new();
        for n in &names {
            let Some(name) = n.as_str() else { continue };
            match store.and_then(|m| m.get(name)) {
                Some(b) => found.push(b.clone()),
                None => missing_b.push(json!(name)),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "Blueprints": found,
            "MissingBlueprints": missing_b,
        })))
    }

    pub(crate) fn list_blueprints(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let names: Vec<String> = accounts
            .get(&req.account_id)
            .map(|s| s.blueprints.keys().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Blueprints": names })))
    }

    pub(crate) fn update_blueprint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let location = req_str(&body, "BlueprintLocation")?.to_string();
        let mut updates = vec![
            ("BlueprintLocation", json!(location)),
            ("LastModifiedOn", json!(now_ts())),
        ];
        if let Some(d) = body.get("Description") {
            updates.push(("Description", d.clone()));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut st.blueprints, &name, "Blueprint", updates)?;
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn delete_blueprint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        // DeleteBlueprint does not declare EntityNotFoundException; treat as
        // idempotent so a missing blueprint doesn't surface an undeclared error.
        st.blueprints.remove(&name);
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    // --- blueprint runs ---

    pub(crate) fn start_blueprint_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let bp = req_str(&body, "BlueprintName")?.to_string();
        req_str(&body, "RoleArn")?;
        let run_id = new_id();
        let now = now_ts();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        if !st.blueprints.contains_key(&bp) {
            return Err(entity_not_found(format!("Blueprint {bp} not found")));
        }
        st.blueprint_runs.insert(
            run_id.clone(),
            json!({
                "BlueprintName": bp,
                "RunId": run_id,
                "State": "RUNNING",
                "StartedOn": now,
                "RoleArn": body.get("RoleArn").cloned().unwrap_or(Value::Null),
                "Parameters": body.get("Parameters").cloned().unwrap_or(Value::Null),
            }),
        );
        Ok(AwsResponse::ok_json(json!({ "RunId": run_id })))
    }

    pub(crate) fn get_blueprint_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "BlueprintName")?;
        let run_id = req_str(&body, "RunId")?;
        let accounts = self.state.read();
        let run = accounts
            .get(&req.account_id)
            .and_then(|s| s.blueprint_runs.get(run_id))
            .ok_or_else(|| entity_not_found(format!("BlueprintRun {run_id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "BlueprintRun": run })))
    }

    pub(crate) fn get_blueprint_runs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let bp = req_str(&body, "BlueprintName")?;
        let accounts = self.state.read();
        let runs: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.blueprint_runs
                    .values()
                    .filter(|r| r.get("BlueprintName").and_then(|n| n.as_str()) == Some(bp))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "BlueprintRuns": runs })))
    }

    // --- dev endpoints ---

    pub(crate) fn create_dev_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "EndpointName")?.to_string();
        let role = req_str(&body, "RoleArn")?.to_string();
        let now = now_ts();
        let mut stored = serde_json::Map::new();
        for f in [
            "EndpointName",
            "RoleArn",
            "SecurityGroupIds",
            "SubnetId",
            "PublicKey",
            "PublicKeys",
            "NumberOfNodes",
            "WorkerType",
            "GlueVersion",
            "NumberOfWorkers",
            "ExtraPythonLibsS3Path",
            "ExtraJarsS3Path",
            "SecurityConfiguration",
            "Arguments",
        ] {
            if let Some(v) = body.get(f) {
                if !v.is_null() {
                    stored.insert(f.to_string(), v.clone());
                }
            }
        }
        // A real dev endpoint takes minutes to provision its notebook
        // environment; fakecloud has nothing to stand up, so it is READY at
        // once and the provider's creation waiter completes on its first poll
        // (otherwise it spins for ~15m and times out).
        stored.insert("Status".into(), json!("READY"));
        stored.insert("CreatedTimestamp".into(), json!(now));
        stored.insert("LastModifiedTimestamp".into(), json!(now));
        // AWS defaults a worker-type-less dev endpoint to 5 nodes, which the
        // resource reads back as `number_of_nodes`.
        if !stored.contains_key("NumberOfNodes") && !stored.contains_key("WorkerType") {
            stored.insert("NumberOfNodes".into(), json!(5));
        }
        let stored = Value::Object(stored);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut st.dev_endpoints, &name, stored, "DevEndpoint")?;
        Ok(AwsResponse::ok_json(json!({
            "EndpointName": name,
            "RoleArn": role,
            "Status": "PROVISIONING",
            "CreatedTimestamp": now,
        })))
    }

    pub(crate) fn get_dev_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "EndpointName")?;
        let accounts = self.state.read();
        let e = accounts
            .get(&req.account_id)
            .and_then(|s| s.dev_endpoints.get(name))
            .ok_or_else(|| entity_not_found(format!("DevEndpoint {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "DevEndpoint": e })))
    }

    pub(crate) fn get_dev_endpoints(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.dev_endpoints.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "DevEndpoints": list })))
    }

    pub(crate) fn batch_get_dev_endpoints(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let names = body["DevEndpointNames"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id).map(|s| &s.dev_endpoints);
        let mut found = Vec::new();
        let mut not_found = Vec::new();
        for n in &names {
            let Some(name) = n.as_str() else { continue };
            match store.and_then(|m| m.get(name)) {
                Some(e) => found.push(e.clone()),
                None => not_found.push(json!(name)),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "DevEndpoints": found,
            "DevEndpointsNotFound": not_found,
        })))
    }

    pub(crate) fn list_dev_endpoints(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let names: Vec<String> = accounts
            .get(&req.account_id)
            .map(|s| s.dev_endpoints.keys().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "DevEndpointNames": names })))
    }

    pub(crate) fn update_dev_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "EndpointName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let e = st
            .dev_endpoints
            .get_mut(&name)
            .ok_or_else(|| entity_not_found(format!("DevEndpoint {name} not found")))?;
        if let Some(obj) = e.as_object_mut() {
            if let Some(pk) = body.get("PublicKey") {
                obj.insert("PublicKey".into(), pk.clone());
            }
            // Previously dropped (bug-hunt 2026-06-24, 1.13): the rest of the
            // mutable DevEndpoint fields.
            for scalar in ["CustomLibraries", "Arguments", "RoleArn"] {
                if let Some(v) = body.get(scalar) {
                    obj.insert(scalar.into(), v.clone());
                }
            }
            if let Some(add) = body.get("AddPublicKeys").and_then(|v| v.as_array()) {
                if let Some(keys) = obj
                    .entry("PublicKeys")
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                {
                    for k in add {
                        if !keys.contains(k) {
                            keys.push(k.clone());
                        }
                    }
                }
            }
            if let Some(del) = body.get("DeletePublicKeys").and_then(|v| v.as_array()) {
                if let Some(keys) = obj.get_mut("PublicKeys").and_then(|v| v.as_array_mut()) {
                    keys.retain(|k| !del.contains(k));
                }
            }
            if let Some(add) = body.get("AddArguments").and_then(|v| v.as_object()) {
                if let Some(args) = obj
                    .entry("Arguments")
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                {
                    for (k, v) in add {
                        args.insert(k.clone(), v.clone());
                    }
                }
            }
            if let Some(del) = body.get("DeleteArguments").and_then(|v| v.as_array()) {
                if let Some(args) = obj.get_mut("Arguments").and_then(|v| v.as_object_mut()) {
                    for k in del {
                        if let Some(k) = k.as_str() {
                            args.remove(k);
                        }
                    }
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_dev_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "EndpointName")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut st.dev_endpoints, &name, "DevEndpoint")?;
        Ok(AwsResponse::ok_json(json!({})))
    }
}
