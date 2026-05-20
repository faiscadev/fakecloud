//! `BedrockAgentService` `data_sources` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl BedrockAgentService {
    pub(super) fn create_data_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let name = req_str(&body, "name")?;
        let id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        let ds = DataSource {
            data_source_id: id.clone(),
            name,
            description: opt_str(&body, "description"),
            knowledge_base_id: kb_id,
            data_source_configuration: opt_json(&body, "dataSourceConfiguration"),
            status: "ACTIVE".to_string(),
            created_at: now_dt,
            updated_at: now_dt,
            failure_reasons: Vec::new(),
        };
        state.data_sources.insert(id.clone(), ds);
        Ok(AwsResponse::ok_json(json!({
            "dataSource": {
                "dataSourceId": id,
                "status": "ACTIVE",
                "createdAt": now_dt.to_rfc3339(),
            }
        })))
    }

    pub(super) fn get_data_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("DataSource {ds_id} not found")))?;
        let ds = state
            .data_sources
            .get(&ds_id)
            .filter(|d| d.knowledge_base_id == kb_id)
            .ok_or_else(|| not_found(format!("DataSource {ds_id} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "dataSource": data_source_json(ds) }),
        ))
    }

    pub(super) fn list_data_sources(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.data_sources
                    .values()
                    .filter(|d| d.knowledge_base_id == kb_id)
                    .map(data_source_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "dataSourceSummaries": list })))
    }

    pub(super) fn update_data_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let ds = state
            .data_sources
            .get_mut(&ds_id)
            .filter(|d| d.knowledge_base_id == kb_id)
            .ok_or_else(|| not_found(format!("DataSource {ds_id} not found")))?;
        ds.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            ds.name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            ds.description = Some(d);
        }
        if body.get("dataSourceConfiguration").is_some() {
            ds.data_source_configuration = opt_json(&body, "dataSourceConfiguration");
        }
        Ok(AwsResponse::ok_json(
            json!({ "dataSource": data_source_json(ds) }),
        ))
    }

    pub(super) fn delete_data_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let removed = state
            .data_sources
            .remove(&ds_id)
            .filter(|d| d.knowledge_base_id == kb_id)
            .is_some();
        if !removed {
            return Err(not_found(format!("DataSource {ds_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}
