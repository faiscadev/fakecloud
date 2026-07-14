//! `EcrService` `scanning` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn put_image_scanning_configuration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let scan_on_push = body
            .get("imageScanningConfiguration")
            .and_then(|v| v.get("scanOnPush"))
            .and_then(|v| v.as_bool())
            .ok_or_else(|| invalid_parameter("Missing imageScanningConfiguration.scanOnPush"))?;
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get_mut(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        repo.image_scanning_configuration = ImageScanningConfiguration { scan_on_push };
        let registry_id = repo.registry_id.clone();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "repositoryName": name,
            "imageScanningConfiguration": { "scanOnPush": scan_on_push },
        })))
    }

    pub(super) fn describe_image_scan_findings(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "repositoryName")?.to_string();
        let image_id = body
            .get("imageId")
            .cloned()
            .ok_or_else(|| invalid_parameter("Missing imageId"))?;
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| repository_not_found(&name))?;
        let repo = state
            .repositories
            .get(&name)
            .ok_or_else(|| repository_not_found(&name))?;
        let digest = resolve_image_digest(repo, &image_id)
            .ok_or_else(|| image_not_found(&name, &image_id))?;
        // A never-scanned image has no findings entry. Real ECR returns
        // ScanNotFoundException here, NOT a fabricated COMPLETE result — the
        // fake success could green-light a CI security gate for an unscanned image.
        let findings = repo.scan_findings.get(&digest).cloned().ok_or_else(|| {
            AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "ScanNotFoundException",
                format!(
                    "Image scan does not exist for the image with '{{imageDigest:{digest}}}' in \
                     the repository with name '{name}' in the registry with id '{account}'"
                ),
            )
        })?;
        Ok(AwsResponse::ok_json(json!({
            "registryId": repo.registry_id,
            "repositoryName": name,
            "imageId": image_id,
            "imageScanStatus": {"status": findings.scan_status},
            "imageScanFindings": {
                "imageScanCompletedAt": findings.scan_completed_at.map(|t| t.timestamp()),
                "vulnerabilitySourceUpdatedAt": findings.vulnerability_source_updated_at.map(|t| t.timestamp()),
                "findings": findings.findings,
                "findingSeverityCounts": findings.finding_severity_counts,
            },
        })))
    }

    pub(super) fn get_registry_scanning_configuration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts.get(&account);
        let cfg = state
            .map(|s| s.registry_scanning_configuration.clone())
            .unwrap_or_default();
        let rules: Vec<Value> = cfg
            .rules
            .iter()
            .map(|r| {
                json!({
                    "scanFrequency": r.scan_frequency,
                    "repositoryFilters": r.repository_filters.iter().map(|f| json!({
                        "filter": f.filter,
                        "filterType": f.filter_type,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "registryId": state.map(|s| s.account_id.clone()).unwrap_or(account),
            "scanningConfiguration": {
                "scanType": cfg.scan_type,
                "rules": rules,
            },
        })))
    }

    pub(super) fn put_registry_scanning_configuration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use crate::state::{RegistryScanningConfiguration, RegistryScanningRule, RepositoryFilter};
        let body = request.json_body();
        let scan_type = opt_str(&body, "scanType").unwrap_or("BASIC").to_string();
        if scan_type != "BASIC" && scan_type != "ENHANCED" {
            return Err(invalid_parameter(format!(
                "Invalid scanType '{scan_type}'. Must be BASIC or ENHANCED."
            )));
        }
        let rules = body
            .get("rules")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let parsed_rules: Vec<RegistryScanningRule> = rules
            .iter()
            .map(|r| RegistryScanningRule {
                scan_frequency: r
                    .get("scanFrequency")
                    .and_then(|v| v.as_str())
                    .unwrap_or("SCAN_ON_PUSH")
                    .to_string(),
                repository_filters: r
                    .get("repositoryFilters")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .map(|f| RepositoryFilter {
                                filter: f
                                    .get("filter")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                filter_type: f
                                    .get("filterType")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("WILDCARD")
                                    .to_string(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            })
            .collect();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        state.registry_scanning_configuration = RegistryScanningConfiguration {
            scan_type: scan_type.clone(),
            rules: parsed_rules,
        };
        let cfg = state.registry_scanning_configuration.clone();
        Ok(AwsResponse::ok_json(json!({
            "registryScanningConfiguration": {
                "scanType": cfg.scan_type,
                "rules": cfg.rules.iter().map(|r| json!({
                    "scanFrequency": r.scan_frequency,
                    "repositoryFilters": r.repository_filters.iter().map(|f| json!({
                        "filter": f.filter,
                        "filterType": f.filter_type,
                    })).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
            },
        })))
    }

    pub(super) fn batch_get_repository_scanning_configuration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let names: Vec<String> = body
            .get("repositoryNames")
            .and_then(|v| v.as_array())
            .ok_or_else(|| invalid_parameter("Missing required field: repositoryNames"))?
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| repository_not_found(&account))?;
        let mut scanning: Vec<Value> = Vec::new();
        let mut failures: Vec<Value> = Vec::new();
        for n in &names {
            match state.repositories.get(n) {
                Some(repo) => scanning.push(json!({
                    "repositoryArn": repo.repository_arn,
                    "repositoryName": n,
                    "scanOnPush": repo.image_scanning_configuration.scan_on_push,
                    // Real ECR derives scanFrequency from the
                    // repository's scanOnPush flag: SCAN_ON_PUSH when
                    // set, MANUAL otherwise. Hardcoding SCAN_ON_PUSH
                    // surfaced inconsistent responses.
                    "scanFrequency": if repo.image_scanning_configuration.scan_on_push {
                        "SCAN_ON_PUSH"
                    } else {
                        "MANUAL"
                    },
                    "appliedScanFilters": [],
                })),
                None => failures.push(json!({
                    "repositoryName": n,
                    "failureCode": "REPOSITORY_NOT_FOUND",
                    "failureReason": format!("Repository '{n}' not found"),
                })),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "scanningConfigurations": scanning,
            "failures": failures,
        })))
    }
}
