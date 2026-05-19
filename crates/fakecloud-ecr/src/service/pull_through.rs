//! `EcrService` `pull_through` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn create_pull_through_cache_rule(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use crate::state::PullThroughCacheRule;
        let body = request.json_body();
        let prefix = req_str(&body, "ecrRepositoryPrefix")?.to_string();
        validate_pullthrough_prefix(&prefix)?;
        let upstream_url = req_str(&body, "upstreamRegistryUrl")?.to_string();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        if state.pull_through_cache_rules.contains_key(&prefix) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "PullThroughCacheRuleAlreadyExistsException",
                format!("A pull through cache rule with the prefix '{prefix}' already exists."),
            ));
        }
        let now = Utc::now();
        let rule = PullThroughCacheRule {
            ecr_repository_prefix: prefix.clone(),
            upstream_registry_url: upstream_url.clone(),
            upstream_registry: opt_str(&body, "upstreamRegistry").map(|s| s.to_string()),
            credential_arn: opt_str(&body, "credentialArn").map(|s| s.to_string()),
            created_at: now,
            updated_at: now,
            custom_role_arn: opt_str(&body, "customRoleArn").map(|s| s.to_string()),
        };
        state
            .pull_through_cache_rules
            .insert(prefix.clone(), rule.clone());
        Ok(AwsResponse::ok_json(pull_through_rule_json(
            state.account_id.as_str(),
            &rule,
        )))
    }

    pub(super) fn delete_pull_through_cache_rule(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let prefix = req_str(&body, "ecrRepositoryPrefix")?.to_string();
        validate_pullthrough_prefix(&prefix)?;
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let removed = state
            .pull_through_cache_rules
            .remove(&prefix)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "PullThroughCacheRuleNotFoundException",
                    format!("No pull through cache rule with prefix '{prefix}' exists."),
                )
            })?;
        // DeletePullThroughCacheRuleResponse omits upstreamRegistry per
        // the Smithy model — it only appears on Create/Describe.
        let mut response = pull_through_rule_json(state.account_id.as_str(), &removed);
        if let Value::Object(ref mut map) = response {
            map.remove("upstreamRegistry");
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn describe_pull_through_cache_rules(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_max_results(&body)?;
        let prefixes: Vec<String> = body
            .get("ecrRepositoryPrefixes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts.get(&account);
        let rules: Vec<&crate::state::PullThroughCacheRule> = state
            .map(|s| s.pull_through_cache_rules.values().collect())
            .unwrap_or_default();
        let registry_id = state.map(|s| s.account_id.clone()).unwrap_or_default();
        let filtered: Vec<Value> = rules
            .iter()
            .filter(|r| prefixes.is_empty() || prefixes.contains(&r.ecr_repository_prefix))
            .map(|r| pull_through_rule_json_with_updated(&registry_id, r))
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "pullThroughCacheRules": filtered,
        })))
    }

    pub(super) fn update_pull_through_cache_rule(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let prefix = req_str(&body, "ecrRepositoryPrefix")?.to_string();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let rule = state
            .pull_through_cache_rules
            .get_mut(&prefix)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "PullThroughCacheRuleNotFoundException",
                    format!("No pull through cache rule with prefix '{prefix}' exists."),
                )
            })?;
        if let Some(cred) = opt_str(&body, "credentialArn") {
            rule.credential_arn = Some(cred.to_string());
        }
        if let Some(role) = opt_str(&body, "customRoleArn") {
            rule.custom_role_arn = Some(role.to_string());
        }
        rule.updated_at = Utc::now();
        let response = pull_through_rule_json_with_updated(state.account_id.as_str(), rule);
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn validate_pull_through_cache_rule(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let prefix = req_str(&body, "ecrRepositoryPrefix")?.to_string();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts.get(&account);
        let rule = state
            .and_then(|s| s.pull_through_cache_rules.get(&prefix))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "PullThroughCacheRuleNotFoundException",
                    format!("No pull through cache rule with prefix '{prefix}' exists."),
                )
            })?;
        let registry_id = state.map(|s| s.account_id.clone()).unwrap_or_default();
        let mut base = pull_through_rule_json(&registry_id, rule);
        base["isValid"] = json!(true);
        Ok(AwsResponse::ok_json(base))
    }
}
