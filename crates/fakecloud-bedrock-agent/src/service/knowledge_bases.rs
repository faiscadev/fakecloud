//! `BedrockAgentService` `knowledge_bases` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl BedrockAgentService {
    pub(super) fn create_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "name")?;
        let id = short_id();
        let now_dt = now();
        let kb = KnowledgeBase {
            knowledge_base_id: id.clone(),
            name,
            knowledge_base_arn: format!(
                "arn:aws:bedrock:{}:{}:knowledge-base/{}",
                req.region, req.account_id, id
            ),
            description: opt_str(&body, "description"),
            role_arn: opt_str(&body, "roleArn").unwrap_or_else(|| {
                format!(
                    "arn:aws:iam::{}:role/fakecloud-bedrock-kb-role",
                    req.account_id
                )
            }),
            knowledge_base_configuration: opt_json(&body, "knowledgeBaseConfiguration")
                .unwrap_or_else(|| json!({"type": "VECTOR"})),
            storage_configuration: opt_json(&body, "storageConfiguration"),
            status: "ACTIVE".to_string(),
            created_at: now_dt,
            updated_at: now_dt,
            failure_reasons: Vec::new(),
        };
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state.knowledge_bases.insert(id.clone(), kb);
        let k = state.knowledge_bases.get(&id).unwrap();
        Ok(AwsResponse::ok_json(json!({ "knowledgeBase": kb_json(k) })))
    }

    pub(super) fn get_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "knowledgeBaseId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {id} not found")))?;
        let k = state
            .knowledge_bases
            .get(&id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "knowledgeBase": kb_json(k) })))
    }

    pub(super) fn list_knowledge_bases(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.knowledge_bases
                    .values()
                    .map(knowledge_base_summary_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "knowledgeBaseSummaries": list }),
        ))
    }

    pub(super) fn update_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let k = state
            .knowledge_bases
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {id} not found")))?;
        k.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            k.name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            k.description = Some(d);
        }
        if let Some(r) = opt_str(&body, "roleArn") {
            k.role_arn = r;
        }
        if body.get("knowledgeBaseConfiguration").is_some() {
            k.knowledge_base_configuration =
                opt_json(&body, "knowledgeBaseConfiguration").unwrap_or_else(|| json!({}));
        }
        if body.get("storageConfiguration").is_some() {
            k.storage_configuration = opt_json(&body, "storageConfiguration");
        }
        Ok(AwsResponse::ok_json(json!({ "knowledgeBase": kb_json(k) })))
    }

    pub(super) fn delete_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state
            .knowledge_bases
            .remove(&id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {id} not found")))?;
        state
            .data_sources
            .retain(|_, ds| ds.knowledge_base_id != id);
        state
            .ingestion_jobs
            .retain(|_, jobs| !jobs.iter().any(|j| j.knowledge_base_id == id));
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn start_ingestion_job(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let job_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        let job = IngestionJob {
            ingestion_job_id: job_id.clone(),
            knowledge_base_id: kb_id.clone(),
            data_source_id: ds_id,
            description: opt_str(&body, "description"),
            status: "COMPLETE".to_string(),
            failure_reasons: Vec::new(),
            started_at: now_dt,
            updated_at: now_dt,
        };
        state.ingestion_jobs.entry(kb_id).or_default().push(job);
        Ok(AwsResponse::ok_json(json!({
            "ingestionJob": {
                "ingestionJobId": job_id,
                "status": "COMPLETE",
                "startedAt": now_dt.to_rfc3339(),
                "updatedAt": now_dt.to_rfc3339(),
            }
        })))
    }

    pub(super) fn get_ingestion_job(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let job_id = req_str(&body, "ingestionJobId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("IngestionJob {job_id} not found")))?;
        let job = state
            .ingestion_jobs
            .get(&kb_id)
            .and_then(|jobs| {
                jobs.iter()
                    .find(|j| j.ingestion_job_id == job_id && j.data_source_id == ds_id)
            })
            .ok_or_else(|| not_found(format!("IngestionJob {job_id} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "ingestionJob": ingestion_job_json(job) }),
        ))
    }

    pub(super) fn list_ingestion_jobs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .and_then(|s| s.ingestion_jobs.get(&kb_id))
            .map(|jobs| {
                jobs.iter()
                    .filter(|j| j.data_source_id == ds_id)
                    .map(ingestion_job_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "ingestionJobSummaries": list }),
        ))
    }

    pub(super) fn stop_ingestion_job(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let job_id = req_str(&body, "ingestionJobId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let jobs = state
            .ingestion_jobs
            .get_mut(&kb_id)
            .ok_or_else(|| not_found(format!("IngestionJob {job_id} not found")))?;
        let job = jobs
            .iter_mut()
            .find(|j| j.ingestion_job_id == job_id && j.data_source_id == ds_id)
            .ok_or_else(|| not_found(format!("IngestionJob {job_id} not found")))?;
        job.status = "STOPPED".to_string();
        job.updated_at = now();
        Ok(AwsResponse::ok_json(
            json!({ "ingestionJob": ingestion_job_json(job) }),
        ))
    }

    pub(super) fn ingest_knowledge_base_documents(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_knowledge_base_documents(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_knowledge_base_documents(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {kb_id} not found")))?;
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "documentDetails": [],
        })))
    }

    pub(super) fn list_knowledge_base_documents(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {kb_id} not found")))?;
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "documentDetails": [],
        })))
    }
}
