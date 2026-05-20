//! `BedrockAgentService` `prompts` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl BedrockAgentService {
    pub(super) fn create_prompt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "name")?;
        let id = short_id();
        let now_dt = now();
        let variants = opt_array(&body, "variants");
        let arn = prompt_arn(&id, &req.region, &req.account_id);
        let prompt = Prompt {
            prompt_id: id.clone(),
            name: name.clone(),
            description: opt_str(&body, "description"),
            variants: variants.clone(),
            version: "DRAFT".to_string(),
            created_at: now_dt,
            updated_at: now_dt,
        };
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state.prompts.insert(id.clone(), prompt);
        let mut out = json!({
            "name": name,
            "id": id,
            "arn": arn,
            "version": "DRAFT",
            "createdAt": now_dt.to_rfc3339(),
            "updatedAt": now_dt.to_rfc3339(),
            "variants": variants,
        });
        if let Some(d) = opt_str(&body, "description") {
            out["description"] = json!(d);
        }
        if let Some(k) = opt_str(&body, "customerEncryptionKeyArn") {
            out["customerEncryptionKeyArn"] = json!(k);
        }
        if let Some(dv) = opt_str(&body, "defaultVariant") {
            out["defaultVariant"] = json!(dv);
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn get_prompt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "promptIdentifier")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?;
        let p = state
            .prompts
            .get(&id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "prompt": prompt_json(p) })))
    }

    pub(super) fn create_prompt_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // The routing layer surfaces the prompt identifier (path segment) into
        // the body under `promptIdentifier`, so we read it back here. The
        // resulting version is numbered incrementally per the Smithy contract.
        let body = req.json_body();
        let id = req_str(&body, "promptIdentifier")?;
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let prompt = state
            .prompts
            .get(&id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?
            .clone();
        let versions = state.prompt_versions.entry(id.clone()).or_default();
        let version_num = (versions.len() as u64 + 1).to_string();
        let pv = PromptVersion {
            prompt_version: version_num.clone(),
            prompt_id: id.clone(),
            description: opt_str(&body, "description"),
            created_at: now_dt,
            updated_at: now_dt,
            variants: prompt.variants.clone(),
        };
        versions.push(pv);
        let arn = format!(
            "{}:{version_num}",
            prompt_arn(&id, &req.region, &req.account_id)
        );
        let mut out = json!({
            "name": prompt.name,
            "id": id,
            "arn": arn,
            "version": version_num,
            "createdAt": now_dt.to_rfc3339(),
            "updatedAt": now_dt.to_rfc3339(),
            "variants": prompt.variants,
        });
        if let Some(d) = opt_str(&body, "description").or(prompt.description) {
            out["description"] = json!(d);
        }
        Ok(AwsResponse::json_value(StatusCode::CREATED, out))
    }

    pub(super) fn list_prompts(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.prompts
                    .values()
                    .map(|p| prompt_summary_json(p, &req.region, &req.account_id))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "promptSummaries": list })))
    }

    pub(super) fn update_prompt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "promptIdentifier")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let p = state
            .prompts
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?;
        p.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            p.name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            p.description = Some(d);
        }
        if body.get("variants").is_some() {
            p.variants = opt_array(&body, "variants");
        }
        Ok(AwsResponse::ok_json(json!({ "prompt": prompt_json(p) })))
    }

    pub(super) fn delete_prompt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "promptIdentifier")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state
            .prompts
            .remove(&id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?;
        state.prompt_versions.remove(&id);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_prompt_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "promptIdentifier")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?;
        let versions: Vec<Value> = state
            .prompt_versions
            .get(&id)
            .map(|vs| {
                vs.iter()
                    .map(|v| {
                        let mut o = json!({
                            "id": v.prompt_id,
                            "version": v.prompt_version,
                            "arn": format!("{}:{}", prompt_arn(&v.prompt_id, &req.region, &req.account_id), v.prompt_version),
                            "createdAt": v.created_at.to_rfc3339(),
                            "updatedAt": v.updated_at.to_rfc3339(),
                        });
                        if let Some(ref d) = v.description {
                            o["description"] = json!(d);
                        }
                        o
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "promptSummaries": versions })))
    }

    pub(super) fn get_prompt_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "promptIdentifier")?;
        let version = req_str(&body, "promptVersion")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?;
        let v = state
            .prompt_versions
            .get(&id)
            .and_then(|vs| vs.iter().find(|v| v.prompt_version == version))
            .ok_or_else(|| not_found(format!("Prompt version {version} not found")))?;
        let mut out = json!({
            "id": v.prompt_id,
            "version": v.prompt_version,
            "arn": format!("{}:{}", prompt_arn(&v.prompt_id, &req.region, &req.account_id), v.prompt_version),
            "createdAt": v.created_at.to_rfc3339(),
            "updatedAt": v.updated_at.to_rfc3339(),
            "variants": v.variants,
        });
        if let Some(ref d) = v.description {
            out["description"] = json!(d);
        }
        Ok(AwsResponse::ok_json(out))
    }
}
