//! `StepFunctionsService` `activities` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StepFunctionsService {
    // ─── Activities ─────────────────────────────────────────────────────

    pub(super) fn create_activity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["name"].as_str().ok_or_else(|| missing("name"))?;
        validate_name(name)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let arn = format!(
            "arn:aws:states:{}:{}:activity:{}",
            req.region, state.account_id, name
        );
        if state.activities.contains_key(&arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ActivityAlreadyExists",
                format!("Activity already exists: {arn}"),
            ));
        }
        let mut tags = BTreeMap::new();
        // CreateActivity accepts create-time `tags`; the Terraform
        // `aws_sfn_activity` resource reads them back immediately.
        let _ = fakecloud_core::tags::apply_tags(&mut tags, &body, "tags", "key", "value");
        let activity = crate::state::Activity {
            name: name.to_string(),
            arn: arn.clone(),
            creation_date: chrono::Utc::now(),
            tags,
        };
        state.activities.insert(arn.clone(), activity.clone());
        Ok(AwsResponse::ok_json(json!({
            "activityArn": arn,
            "creationDate": activity.creation_date.timestamp(),
        })))
    }

    pub(super) fn delete_activity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["activityArn"]
            .as_str()
            .ok_or_else(|| missing("activityArn"))?
            .to_string();
        validate_arn_length("activityArn", &arn, 256)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.activities.remove(&arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_activity(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["activityArn"]
            .as_str()
            .ok_or_else(|| missing("activityArn"))?
            .to_string();
        let accounts = self.state.read();
        let empty = crate::state::StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let a = state.activities.get(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ActivityDoesNotExist",
                format!("Activity does not exist: {arn}"),
            )
        })?;
        Ok(AwsResponse::ok_json(json!({
            "activityArn": a.arn,
            "name": a.name,
            "creationDate": a.creation_date.timestamp(),
        })))
    }

    pub(super) async fn get_activity_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["activityArn"]
            .as_str()
            .ok_or_else(|| missing("activityArn"))?
            .to_string();
        // Activity must exist before we'll accept long-poll calls.
        {
            let accounts = self.state.read();
            let state = accounts
                .get(&req.account_id)
                .ok_or_else(|| activity_not_found(&arn))?;
            if !state.activities.contains_key(&arn) {
                return Err(activity_not_found(&arn));
            }
        }

        // AWS GetActivityTask blocks up to 60s. fakecloud defaults to 5s
        // so test suites don't stall when no worker is feeding the queue.
        let max_wait_secs: u64 = std::env::var("FAKECLOUD_SFN_GET_ACTIVITY_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_wait_secs);

        loop {
            // Try to dequeue oldest PENDING token for this activity.
            {
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&req.account_id);
                let mut candidates: Vec<(String, chrono::DateTime<chrono::Utc>)> = state
                    .task_tokens
                    .iter()
                    .filter(|(_, t)| t.activity_arn == arn && t.status == "PENDING")
                    .map(|(k, t)| (k.clone(), t.created_at))
                    .collect();
                candidates.sort_by_key(|c| c.1);
                if let Some((token, _)) = candidates.into_iter().next() {
                    let now = chrono::Utc::now();
                    let entry = state.task_tokens.get_mut(&token).expect("just looked up");
                    entry.status = "IN_PROGRESS".to_string();
                    entry.last_heartbeat_at = Some(now);
                    let input = entry.input.clone().unwrap_or_else(|| "{}".to_string());
                    return Ok(AwsResponse::ok_json(json!({
                        "taskToken": token,
                        "input": input,
                    })));
                }
            }
            if std::time::Instant::now() >= deadline {
                // No task available in window — return empty token (matches
                // AWS behavior).
                return Ok(AwsResponse::ok_json(json!({
                    "taskToken": "",
                    "input": "",
                })));
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }
}
