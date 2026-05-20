//! `EcrService` `lifecycle` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn put_lifecycle_policy(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let policy = req_str(&body, "lifecyclePolicyText")?.to_string();
        // Parse sanity-check.
        serde_json::from_str::<Value>(&policy)
            .map_err(|_| invalid_parameter("lifecyclePolicyText is not valid JSON"))?;
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        repo.lifecycle_policy = Some(policy.clone());
        // Apply immediately so the store reflects the policy.
        let prune = evaluate_lifecycle_policy(repo, &policy);
        for digest in &prune {
            repo.images.remove(digest);
            repo.image_tags.retain(|_, d| d != digest);
        }
        repo.lifecycle_policy_last_evaluated_at = Some(Utc::now());
        let registry_id = repo.registry_id.clone();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "lifecyclePolicyText": policy,
        })))
    }

    pub(super) fn get_lifecycle_policy(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        let policy = repo
            .lifecycle_policy
            .clone()
            .ok_or_else(|| lifecycle_policy_not_found(&name))?;
        let last_eval = repo
            .lifecycle_policy_last_evaluated_at
            .map(|t| t.timestamp())
            .unwrap_or(0);
        Ok(AwsResponse::ok_json(json!({
            "registryId": repo.registry_id,
            "repositoryName": name,
            "lifecyclePolicyText": policy,
            "lastEvaluatedAt": last_eval,
        })))
    }

    pub(super) fn delete_lifecycle_policy(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        let policy = repo
            .lifecycle_policy
            .take()
            .ok_or_else(|| lifecycle_policy_not_found(&name))?;
        let last_eval = repo
            .lifecycle_policy_last_evaluated_at
            .take()
            .map(|t| t.timestamp())
            .unwrap_or(0);
        let registry_id = repo.registry_id.clone();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "lifecyclePolicyText": policy,
            "lastEvaluatedAt": last_eval,
        })))
    }

    pub(super) fn start_lifecycle_policy_preview(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let account = target_account_id(request, &body);
        let policy = match opt_str(&body, "lifecyclePolicyText") {
            Some(s) => {
                // Validate the supplied policy is JSON before pretending the
                // preview succeeded; otherwise malformed input lands on
                // `lifecycle_policy_preview` and downstream GetLifecyclePolicyPreview
                // returns a bogus "successful" result.
                if serde_json::from_str::<serde_json::Value>(s).is_err() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterException",
                        "lifecyclePolicyText is not valid JSON",
                    ));
                }
                s.to_string()
            }
            None => {
                let accounts = self.state.read();
                let state = accounts
                    .get(&account)
                    .ok_or_else(|| repository_not_found(&name))?;
                let repo = state
                    .repositories
                    .get(&name)
                    .ok_or_else(|| repository_not_found(&name))?;
                repo.lifecycle_policy
                    .clone()
                    .ok_or_else(|| lifecycle_policy_not_found(&name))?
            }
        };
        // Run the preview eval and stamp `last_evaluated_at` so callers
        // polling `GetLifecyclePolicy` after a preview see the most
        // recent evaluation marker. Preview is non-destructive — we
        // don't apply the prune here, just record that it ran.
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        let _prune = evaluate_lifecycle_policy(repo, &policy);
        repo.lifecycle_policy_preview = Some(policy.clone());
        repo.lifecycle_policy_last_evaluated_at = Some(Utc::now());
        let registry_id = repo.registry_id.clone();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "lifecyclePolicyText": policy,
            "status": "COMPLETE",
        })))
    }

    pub(super) fn get_lifecycle_policy_preview(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        let policy = repo
            .lifecycle_policy_preview
            .clone()
            .or_else(|| repo.lifecycle_policy.clone())
            .ok_or_else(|| lifecycle_policy_not_found(&name))?;
        let prune = evaluate_lifecycle_policy(repo, &policy);
        let results: Vec<Value> = prune
            .iter()
            .map(|digest| {
                json!({
                    "imageDigest": digest,
                    "imagePushedAt": repo.images.get(digest).map(|i| i.image_pushed_at.timestamp()).unwrap_or(0),
                    "action": {"type": "EXPIRE"},
                })
            })
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "registryId": repo.registry_id,
            "repositoryName": name,
            "lifecyclePolicyText": policy,
            "status": "COMPLETE",
            "previewResults": results,
            "summary": {"expiringImageTotalCount": prune.len()},
        })))
    }
}
