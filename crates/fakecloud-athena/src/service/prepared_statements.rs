//! `AthenaService` `prepared_statements` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl AthenaService {
    pub(super) fn create_prepared_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let statement_name = require_str(&body, "StatementName")?;
        let query_statement = require_str(&body, "QueryStatement")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.work_groups.contains_key(&work_group) {
            return Err(invalid_request(format!("Workgroup {work_group} not found")));
        }
        let key = (work_group.clone(), statement_name.clone());
        if account.prepared_statements.contains_key(&key) {
            return Err(invalid_request(format!(
                "PreparedStatement {statement_name} already exists in {work_group}"
            )));
        }
        account.prepared_statements.insert(
            key,
            PreparedStatement {
                statement_name,
                work_group_name: work_group,
                query_statement,
                description,
                last_modified_time: Utc::now(),
            },
        );
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_prepared_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let statement_name = require_str(&body, "StatementName")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let ps = account
            .prepared_statements
            .get(&(work_group.clone(), statement_name.clone()))
            .ok_or_else(|| {
                invalid_request(format!(
                    "PreparedStatement {statement_name} not found in {work_group}"
                ))
            })?;
        Ok(AwsResponse::ok_json(json!({
            "PreparedStatement": prepared_statement_json(ps),
        })))
    }

    pub(super) fn list_prepared_statements(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let max_results = validate_max_results(&body, 1, 50)?;
        // Smithy: NextToken targets Token @length(1,1024).
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut ps: Vec<PreparedStatement> = account
            .prepared_statements
            .iter()
            .filter(|((wg, _), _)| wg == &work_group)
            .map(|(_, p)| p.clone())
            .collect();
        ps.sort_by(|a, b| a.statement_name.cmp(&b.statement_name));
        let (page, next) = paginate_checked(&ps, next_token.as_deref(), max_results)
            .map_err(|_| invalid_request("Invalid NextToken"))?;
        let summaries: Vec<Value> = page
            .iter()
            .map(|p| {
                json!({
                    "StatementName": p.statement_name,
                    "LastModifiedTime": p.last_modified_time.timestamp() as f64,
                })
            })
            .collect();
        let mut response = json!({ "PreparedStatements": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn batch_get_prepared_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        // Smithy: @required PreparedStatementNames (@length min:1 max:50).
        validate_required_list(&body, "PreparedStatementNames")?;
        validate_list_len(&body, "PreparedStatementNames", 1, 50)?;
        let names = parse_string_list(body.get("PreparedStatementNames"));
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for name in names {
            if let Some(ps) = account
                .prepared_statements
                .get(&(work_group.clone(), name.clone()))
            {
                found.push(prepared_statement_json(ps));
            } else {
                missing.push(json!({ "StatementName": name, "ErrorCode": "NOT_FOUND", "ErrorMessage": "PreparedStatement not found" }));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "PreparedStatements": found,
            "UnprocessedPreparedStatementNames": missing,
        })))
    }

    pub(super) fn update_prepared_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let statement_name = require_str(&body, "StatementName")?;
        let query_statement = require_str(&body, "QueryStatement")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let ps = account
            .prepared_statements
            .get_mut(&(work_group.clone(), statement_name.clone()))
            .ok_or_else(|| {
                invalid_request(format!(
                    "PreparedStatement {statement_name} not found in {work_group}"
                ))
            })?;
        ps.query_statement = query_statement;
        ps.description = description;
        ps.last_modified_time = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_prepared_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let statement_name = require_str(&body, "StatementName")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account
            .prepared_statements
            .remove(&(work_group.clone(), statement_name.clone()))
            .is_none()
        {
            return Err(invalid_request(format!(
                "PreparedStatement {statement_name} not found in {work_group}"
            )));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}
