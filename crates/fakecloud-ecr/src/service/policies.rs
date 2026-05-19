//! `EcrService` `policies` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn set_repository_policy(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let policy_text = req_str(&body, "policyText")?.to_string();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        repo.policy = Some(policy_text.clone());
        let registry_id = repo.registry_id.clone();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "policyText": policy_text,
        })))
    }

    pub(super) fn get_repository_policy(
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
            .policy
            .clone()
            .ok_or_else(|| repository_policy_not_found(&name))?;
        Ok(AwsResponse::ok_json(json!({
            "registryId": repo.registry_id,
            "repositoryName": name,
            "policyText": policy,
        })))
    }

    pub(super) fn delete_repository_policy(
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
            .policy
            .take()
            .ok_or_else(|| repository_policy_not_found(&name))?;
        let registry_id = repo.registry_id.clone();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "policyText": policy,
        })))
    }

    pub(super) fn get_registry_policy(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(registry_policy_not_found)?;
        let policy = state
            .registry_policy
            .clone()
            .ok_or_else(registry_policy_not_found)?;
        Ok(AwsResponse::ok_json(json!({
            "registryId": state.account_id,
            "policyText": policy,
        })))
    }

    pub(super) fn put_registry_policy(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let policy = req_str(&body, "policyText")?.to_string();
        if policy.len() > 10_240 {
            return Err(invalid_parameter(format!(
                "Value at 'policyText' failed to satisfy constraint: \
                 Member must have length less than or equal to 10240 (got {})",
                policy.len()
            )));
        }
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        state.registry_policy = Some(policy.clone());
        Ok(AwsResponse::ok_json(json!({
            "registryId": state.account_id,
            "policyText": policy,
        })))
    }

    pub(super) fn delete_registry_policy(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(registry_policy_not_found)?;
        let policy = state
            .registry_policy
            .take()
            .ok_or_else(registry_policy_not_found)?;
        Ok(AwsResponse::ok_json(json!({
            "registryId": state.account_id,
            "policyText": policy,
        })))
    }
}
