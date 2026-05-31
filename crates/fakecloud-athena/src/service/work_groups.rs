//! `AthenaService` `work_groups` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl AthenaService {
    pub(super) fn create_work_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let configuration = body.get("Configuration").cloned();
        let tags = parse_tags(body.get("Tags"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.work_groups.contains_key(&name) {
            return Err(invalid_request(format!("Workgroup {name} already exists")));
        }
        let wg = WorkGroup {
            name: name.clone(),
            state: "ENABLED".to_string(),
            description,
            configuration,
            creation_time: Utc::now(),
            engine_version: Some("AUTO".to_string()),
        };
        let arn = workgroup_arn(&req.account_id, &req.region, &name);
        account.work_groups.insert(name, wg);
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_work_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "WorkGroup")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let wg = account
            .work_groups
            .get(&name)
            .ok_or_else(|| invalid_request(format!("Workgroup {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "WorkGroup": work_group_json(wg),
        })))
    }

    pub(super) fn list_work_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let max_results = validate_max_results(&body, 1, 50)?;
        // Smithy: NextToken targets Token @length(1,1024).
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<WorkGroup> = account.work_groups.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let (page, next) = paginate_checked(&all, next_token.as_deref(), max_results)
            .map_err(|_| invalid_request("Invalid NextToken"))?;
        let summaries: Vec<Value> = page.iter().map(workgroup_summary_json).collect();
        let mut response = json!({ "WorkGroups": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn update_work_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "WorkGroup")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let wg = account
            .work_groups
            .get_mut(&name)
            .ok_or_else(|| invalid_request(format!("Workgroup {name} not found")))?;
        if let Some(d) = body.get("Description").and_then(Value::as_str) {
            wg.description = Some(d.to_string());
        }
        if let Some(s) = body.get("State").and_then(Value::as_str) {
            wg.state = s.to_string();
        }
        if let Some(c) = body.get("ConfigurationUpdates") {
            wg.configuration = Some(c.clone());
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_work_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "WorkGroup")?;
        let recursive = body
            .get("RecursiveDeleteOption")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if name == "primary" {
            return Err(invalid_request("Cannot delete the primary workgroup"));
        }
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let used_by_query = account
            .query_executions
            .values()
            .any(|q| q.work_group == name);
        let used_by_named = account.named_queries.values().any(|q| q.work_group == name);
        let used_by_prepared = account
            .prepared_statements
            .keys()
            .any(|(wg, _)| wg == &name);
        if !recursive && (used_by_query || used_by_named || used_by_prepared) {
            return Err(invalid_request(format!(
                "Workgroup {name} still has resources; pass RecursiveDeleteOption=true"
            )));
        }
        if account.work_groups.remove(&name).is_none() {
            return Err(invalid_request(format!("Workgroup {name} not found")));
        }
        if recursive {
            account.query_executions.retain(|_, q| q.work_group != name);
            account.named_queries.retain(|_, q| q.work_group != name);
            account.prepared_statements.retain(|(wg, _), _| wg != &name);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}
