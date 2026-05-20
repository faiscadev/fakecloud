//! `StepFunctionsService` `tasks` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StepFunctionsService {
    pub(super) fn send_task_success(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.update_task_token(req, "SUCCEEDED")
    }

    pub(super) fn send_task_failure(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.update_task_token(req, "FAILED")
    }

    pub(super) fn send_task_heartbeat(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Heartbeats only refresh `last_heartbeat_at`; they don't change
        // the task's lifecycle status. The interpreter's heartbeat-timeout
        // check reads `last_heartbeat_at` to decide whether to fail the
        // task with `States.HeartbeatTimeout`.
        let body = req.json_body();
        let token = body["taskToken"]
            .as_str()
            .ok_or_else(|| missing("taskToken"))?
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let entry = state
            .task_tokens
            .get_mut(&token)
            .ok_or_else(|| task_does_not_exist(&token))?;
        entry.last_heartbeat_at = Some(chrono::Utc::now());
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn update_task_token(
        &self,
        req: &AwsRequest,
        new_status: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let token = body["taskToken"]
            .as_str()
            .ok_or_else(|| missing("taskToken"))?
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let entry = state
            .task_tokens
            .get_mut(&token)
            .ok_or_else(|| task_does_not_exist(&token))?;
        entry.status = new_status.to_string();
        if new_status == "SUCCEEDED" {
            entry.output = body["output"].as_str().map(String::from);
        } else if new_status == "FAILED" {
            entry.error = body["error"].as_str().map(String::from);
            entry.cause = body["cause"].as_str().map(String::from);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}
