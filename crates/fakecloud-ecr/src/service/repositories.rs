//! `EcrService` `repositories` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn create_repository(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        validate_repository_name(&name)?;
        let image_tag_mutability = opt_str(&body, "imageTagMutability")
            .unwrap_or("MUTABLE")
            .to_string();
        if image_tag_mutability != "MUTABLE" && image_tag_mutability != "IMMUTABLE" {
            return Err(invalid_parameter(format!(
                "Invalid value for imageTagMutability: {image_tag_mutability}"
            )));
        }
        let scan_on_push = body
            .get("imageScanningConfiguration")
            .and_then(|v| v.get("scanOnPush"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let encryption = body
            .get("encryptionConfiguration")
            .map(|v| EncryptionConfiguration {
                encryption_type: v
                    .get("encryptionType")
                    .and_then(|x| x.as_str())
                    .unwrap_or("AES256")
                    .to_string(),
                kms_key: v
                    .get("kmsKey")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string()),
            })
            .unwrap_or_default();
        let tags = parse_tags(&body);

        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let endpoint = accounts.endpoint().to_string();
        let state = accounts.get_or_create(&account);
        if state.repositories.contains_key(&name) {
            return Err(repository_already_exists(&name));
        }
        let arn = state.repository_arn(&name);
        let mut repo = Repository::new(&name, arn, state.registry_id(), &endpoint);
        repo.image_tag_mutability = image_tag_mutability;
        repo.image_scanning_configuration = ImageScanningConfiguration { scan_on_push };
        repo.encryption_configuration = encryption;
        for (k, v) in tags {
            repo.tags.insert(k, v);
        }
        let response = repository_to_json(&repo);
        state.repositories.insert(name.clone(), repo);
        Ok(AwsResponse::ok_json(json!({ "repository": response })))
    }

    pub(super) fn delete_repository(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let force = body.get("force").and_then(|v| v.as_bool()).unwrap_or(false);
        let account = target_account_id(request, &body);

        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        // Repository-image state lands in Batch 2; until then, nothing
        // to block the delete, so `force` is accepted but noop-ish.
        let _ = force;
        let snapshot = repository_to_json(repo);
        state.repositories.remove(&name);
        Ok(AwsResponse::ok_json(json!({ "repository": snapshot })))
    }

    pub(super) fn describe_repositories(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // AWS's documented default page size for DescribeRepositories.
        const DEFAULT_PAGE_SIZE: usize = 100;
        let body = request.json_body();
        let max_results = match body.get("maxResults").and_then(|v| v.as_i64()) {
            Some(n) => {
                // Smithy @range(min=1, max=1000) on DescribeRepositories.maxResults.
                if !(1..=1000).contains(&n) {
                    return Err(invalid_parameter(format!(
                        "Value '{n}' at 'maxResults' failed to satisfy constraint: \
                         Member must have value between 1 and 1000",
                    )));
                }
                n as usize
            }
            None => DEFAULT_PAGE_SIZE,
        };
        let offset = match body.get("nextToken").and_then(|v| v.as_str()) {
            Some(raw) => raw.parse::<usize>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "The specified parameter is invalid: nextToken",
                )
            })?,
            None => 0,
        };
        let names: Vec<String> = body
            .get("repositoryNames")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let Some(state) = accounts.get(&account) else {
            return Ok(AwsResponse::ok_json(json!({ "repositories": [] })));
        };
        let mut out: Vec<Value> = Vec::new();
        let mut next_token: Option<String> = None;
        if names.is_empty() {
            let all: Vec<&Repository> = state.repositories.values().collect();
            let start = offset.min(all.len());
            let end = (start + max_results).min(all.len());
            for repo in &all[start..end] {
                out.push(repository_to_json(repo));
            }
            if end < all.len() {
                next_token = Some(end.to_string());
            }
        } else {
            for n in &names {
                let repo = state
                    .repositories
                    .get(n)
                    .ok_or_else(|| repository_not_found(n))?;
                out.push(repository_to_json(repo));
            }
        }
        let mut response = json!({ "repositories": out });
        if let Some(token) = next_token {
            response["nextToken"] = json!(token);
        }
        Ok(AwsResponse::ok_json(response))
    }
}
