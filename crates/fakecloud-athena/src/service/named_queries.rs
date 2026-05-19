//! `AthenaService` `named_queries` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl AthenaService {
    pub(super) fn create_named_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: Name=NameString(1,128), Database=DatabaseString(1,255),
        // QueryString=QueryString(1,262144), Description=DescriptionString(1,1024).
        let name = validate_required_string_len(&body, "Name", 1, 128)?;
        let database = validate_required_string_len(&body, "Database", 1, 255)?;
        let query_string = validate_required_string_len(&body, "QueryString", 1, 262144)?;
        validate_opt_string_len(&body, "Description", 1, 1024)?;
        // Smithy: IdempotencyToken @length(min: 32, max: 128).
        validate_opt_string_len(&body, "ClientRequestToken", 32, 128)?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let work_group = body
            .get("WorkGroup")
            .and_then(Value::as_str)
            .unwrap_or("primary")
            .to_string();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.work_groups.contains_key(&work_group) {
            return Err(invalid_request(format!("Workgroup {work_group} not found")));
        }
        let id = synth_uuid();
        account.named_queries.insert(
            id.clone(),
            NamedQuery {
                named_query_id: id.clone(),
                name,
                description,
                database,
                query_string,
                work_group,
                last_used_at: None,
            },
        );
        Ok(AwsResponse::ok_json(json!({ "NamedQueryId": id })))
    }

    pub(super) fn get_named_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NamedQueryId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .named_queries
            .get(&id)
            .ok_or_else(|| invalid_request(format!("NamedQuery {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "NamedQuery": named_query_json(q),
        })))
    }

    pub(super) fn list_named_queries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = body
            .get("WorkGroup")
            .and_then(Value::as_str)
            .unwrap_or("primary")
            .to_string();
        // Smithy: MaxResults targets MaxNamedQueriesCount @range(0,50);
        // NextToken targets Token @length(1,1024).
        let max_results = validate_max_results(&body, 0, 50)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut ids: Vec<String> = account
            .named_queries
            .values()
            .filter(|q| q.work_group == work_group)
            .map(|q| q.named_query_id.clone())
            .collect();
        ids.sort();
        let (page, next) = paginate(&ids, next_token.as_deref(), max_results);
        let mut response = json!({ "NamedQueryIds": page });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn batch_get_named_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: @required NamedQueryIds (@length min:1 max:50).
        validate_required_list(&body, "NamedQueryIds")?;
        validate_list_len(&body, "NamedQueryIds", 1, 50)?;
        let ids = parse_string_list(body.get("NamedQueryIds"));
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for id in ids {
            if let Some(q) = account.named_queries.get(&id) {
                found.push(named_query_json(q));
            } else {
                missing.push(json!({ "NamedQueryId": id, "ErrorCode": "NOT_FOUND", "ErrorMessage": "NamedQuery not found" }));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "NamedQueries": found,
            "UnprocessedNamedQueryIds": missing,
        })))
    }

    pub(super) fn update_named_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NamedQueryId")?;
        let name = require_str(&body, "Name")?;
        let query_string = require_str(&body, "QueryString")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .named_queries
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("NamedQuery {id} not found")))?;
        q.name = name;
        q.query_string = query_string;
        q.description = description;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_named_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NamedQueryId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.named_queries.remove(&id).is_none() {
            return Err(invalid_request(format!("NamedQuery {id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}
