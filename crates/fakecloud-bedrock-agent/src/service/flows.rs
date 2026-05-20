//! `BedrockAgentService` `flows` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl BedrockAgentService {
    pub(super) fn create_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "name")?;
        let id = short_id();
        let now_dt = now();
        // executionRoleArn is required by the Smithy model; synthesize a
        // plausible value when the caller omits one so the response still
        // satisfies the required shape.
        let role_arn = opt_str(&body, "executionRoleArn").unwrap_or_else(|| {
            format!(
                "arn:aws:iam::{}:role/service-role/AmazonBedrockExecutionRoleForFlows_{id}",
                req.account_id
            )
        });
        let arn = flow_arn(&id, &req.region, &req.account_id);
        let definition = opt_json(&body, "definition");
        let flow = Flow {
            flow_id: id.clone(),
            name: name.clone(),
            description: opt_str(&body, "description"),
            execution_role_arn: Some(role_arn.clone()),
            status: "NotPrepared".to_string(),
            created_at: now_dt,
            updated_at: now_dt,
            version: "DRAFT".to_string(),
            definition: definition.clone(),
        };
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state.flows.insert(id.clone(), flow);
        let mut out = json!({
            "name": name,
            "executionRoleArn": role_arn,
            "id": id,
            "arn": arn,
            "status": "NotPrepared",
            "createdAt": now_dt.to_rfc3339(),
            "updatedAt": now_dt.to_rfc3339(),
            "version": "DRAFT",
        });
        if let Some(d) = opt_str(&body, "description") {
            out["description"] = json!(d);
        }
        if let Some(k) = opt_str(&body, "customerEncryptionKeyArn") {
            out["customerEncryptionKeyArn"] = json!(k);
        }
        if let Some(def) = definition {
            out["definition"] = def;
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn get_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "flowId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Flow {id} not found")))?;
        let f = state
            .flows
            .get(&id)
            .ok_or_else(|| not_found(format!("Flow {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "flow": flow_json(f) })))
    }

    pub(super) fn list_flows(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.flows
                    .values()
                    .map(|f| flow_summary_json(f, &req.region, &req.account_id))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "flowSummaries": list })))
    }

    pub(super) fn update_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "flowId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let f = state
            .flows
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Flow {id} not found")))?;
        f.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            f.name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            f.description = Some(d);
        }
        if let Some(r) = opt_str(&body, "executionRoleArn") {
            f.execution_role_arn = Some(r);
        }
        if body.get("definition").is_some() {
            f.definition = opt_json(&body, "definition");
        }
        Ok(AwsResponse::ok_json(json!({ "flow": flow_json(f) })))
    }

    pub(super) fn delete_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "flowId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state
            .flows
            .remove(&id)
            .ok_or_else(|| not_found(format!("Flow {id} not found")))?;
        state.flow_versions.remove(&id);
        state.flow_aliases.retain(|_, a| a.flow_id != id);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn prepare_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "flowId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let f = state
            .flows
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Flow {id} not found")))?;
        f.status = "PREPARED".to_string();
        f.updated_at = now();
        Ok(AwsResponse::ok_json(json!({
            "flowId": id,
            "status": "PREPARED",
        })))
    }

    pub(super) fn create_flow_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let flow = state
            .flows
            .get(&flow_id)
            .ok_or_else(|| not_found(format!("Flow {flow_id} not found")))?;
        let versions = state.flow_versions.entry(flow_id.clone()).or_default();
        let version_num = (versions.len() as u64 + 1).to_string();
        let fv = FlowVersion {
            flow_version: version_num.clone(),
            flow_id: flow_id.clone(),
            description: opt_str(&body, "description"),
            created_at: now_dt,
            updated_at: now_dt,
            definition: flow.definition.clone(),
        };
        versions.push(fv);
        Ok(AwsResponse::ok_json(json!({
            "flowVersion": {
                "flowVersion": version_num,
                "flowId": flow_id,
                "createdAt": now_dt.to_rfc3339(),
            }
        })))
    }

    pub(super) fn get_flow_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let version = req_str(&body, "flowVersion")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Flow version {version} not found")))?;
        let v = state
            .flow_versions
            .get(&flow_id)
            .and_then(|vec| vec.iter().find(|v| v.flow_version == version))
            .ok_or_else(|| not_found(format!("Flow version {version} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "flowVersion": flow_version_json(v) }),
        ))
    }

    pub(super) fn list_flow_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .and_then(|s| s.flow_versions.get(&flow_id))
            .map(|vec| vec.iter().map(flow_version_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "flowVersionSummaries": list }),
        ))
    }

    pub(super) fn delete_flow_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let version = req_str(&body, "flowVersion")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let vec = state
            .flow_versions
            .get_mut(&flow_id)
            .ok_or_else(|| not_found(format!("Flow version {version} not found")))?;
        let pos = vec
            .iter()
            .position(|v| v.flow_version == version)
            .ok_or_else(|| not_found(format!("Flow version {version} not found")))?;
        vec.remove(pos);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn validate_flow_definition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _flow_id = req_str(&body, "flowId")?;
        Ok(AwsResponse::ok_json(json!({
            "isValid": true,
            "validationDetails": [],
        })))
    }

    pub(super) fn create_flow_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let name = req_str(&body, "name")?;
        let alias_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.flows.contains_key(&flow_id) {
            return Err(not_found(format!("Flow {flow_id} not found")));
        }
        let alias = FlowAlias {
            alias_id: alias_id.clone(),
            alias_name: name.clone(),
            flow_id: flow_id.clone(),
            routing_configuration: opt_array(&body, "routingConfiguration"),
            description: opt_str(&body, "description"),
            created_at: now_dt,
            updated_at: now_dt,
        };
        state.flow_aliases.insert(alias_id.clone(), alias);
        Ok(AwsResponse::ok_json(json!({
            "flowAlias": {
                "aliasId": alias_id,
                "aliasName": name,
                "flowId": flow_id,
                "createdAt": now_dt.to_rfc3339(),
            }
        })))
    }

    pub(super) fn get_flow_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let alias_id = req_str(&body, "aliasId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Flow alias {alias_id} not found")))?;
        let a = state
            .flow_aliases
            .get(&alias_id)
            .filter(|a| a.flow_id == flow_id)
            .ok_or_else(|| not_found(format!("Flow alias {alias_id} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "flowAlias": flow_alias_json(a) }),
        ))
    }

    pub(super) fn list_flow_aliases(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.flow_aliases
                    .values()
                    .filter(|a| a.flow_id == flow_id)
                    .map(flow_alias_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "flowAliasSummaries": list })))
    }

    pub(super) fn update_flow_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let alias_id = req_str(&body, "aliasId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let a = state
            .flow_aliases
            .get_mut(&alias_id)
            .filter(|a| a.flow_id == flow_id)
            .ok_or_else(|| not_found(format!("Flow alias {alias_id} not found")))?;
        a.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            a.alias_name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            a.description = Some(d);
        }
        if body.get("routingConfiguration").is_some() {
            a.routing_configuration = opt_array(&body, "routingConfiguration");
        }
        Ok(AwsResponse::ok_json(
            json!({ "flowAlias": flow_alias_json(a) }),
        ))
    }

    pub(super) fn delete_flow_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let alias_id = req_str(&body, "aliasId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        match state.flow_aliases.get(&alias_id) {
            Some(a) if a.flow_id == flow_id => {
                state.flow_aliases.remove(&alias_id);
            }
            _ => return Err(not_found(format!("Flow alias {alias_id} not found"))),
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}
