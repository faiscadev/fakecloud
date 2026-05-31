//! `StepFunctionsService` `map_runs` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StepFunctionsService {
    // ─── Map runs ───────────────────────────────────────────────────────

    pub(super) fn describe_map_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["mapRunArn"]
            .as_str()
            .ok_or_else(|| missing("mapRunArn"))?
            .to_string();
        let accounts = self.state.read();
        let empty = crate::state::StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mr = state
            .map_runs
            .get(&arn)
            .ok_or_else(|| resource_not_found(&arn))?;
        Ok(AwsResponse::ok_json(map_run_to_json(mr)))
    }

    pub(super) fn list_map_runs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // `executionArn` is required + has @length 1..=256.
        let exec_arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?
            .to_string();
        validate_arn_length("executionArn", &exec_arn, 256)?;
        let raw_max_results = body["maxResults"].as_i64();
        if let Some(mr) = raw_max_results {
            validate_max_results(mr)?;
        }
        let next_token = body["nextToken"].as_str();
        if let Some(t) = next_token {
            validate_page_token(t)?;
        }
        let max_results = match raw_max_results.unwrap_or(0) {
            0 => 100,
            n => n as usize,
        };
        let accounts = self.state.read();
        let empty = crate::state::StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut runs: Vec<&crate::state::MapRun> = state
            .map_runs
            .values()
            .filter(|r| r.execution_arn == exec_arn)
            .collect();
        // Stable order so pagination is deterministic.
        runs.sort_by_key(|r| r.start_date);
        let items: Vec<Value> = runs
            .iter()
            .map(|r| {
                json!({
                    "mapRunArn": r.map_run_arn,
                    "executionArn": r.execution_arn,
                    "stateMachineArn": "",
                    "startDate": r.start_date.timestamp(),
                    "stopDate": r.stop_date.map(|d| d.timestamp()),
                })
            })
            .collect();
        let (page, token) =
            paginate_checked(&items, next_token, max_results).map_err(|_| invalid_token())?;
        let mut resp = json!({ "mapRuns": page });
        if let Some(t) = token {
            resp["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn update_map_run(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["mapRunArn"]
            .as_str()
            .ok_or_else(|| missing("mapRunArn"))?
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mr = state
            .map_runs
            .get_mut(&arn)
            .ok_or_else(|| resource_not_found(&arn))?;
        if let Some(c) = body["maxConcurrency"].as_i64() {
            mr.max_concurrency = c as i32;
        }
        if let Some(p) = body["toleratedFailurePercentage"].as_f64() {
            mr.tolerated_failure_percentage = p;
        }
        if let Some(c) = body["toleratedFailureCount"].as_i64() {
            mr.tolerated_failure_count = c;
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}
