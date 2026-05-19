//! `EcrService` `registry` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn get_authorization_token(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let registry_ids: Vec<String> = body
            .get("registryIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let accounts = self.state.read();
        let default_account = accounts.default_account_id().to_string();
        let targets = if registry_ids.is_empty() {
            vec![default_account]
        } else {
            registry_ids
        };
        let endpoint = accounts.endpoint().to_string();
        drop(accounts);
        let expires_at = (Utc::now() + chrono::Duration::hours(12)).timestamp();
        let authorization_data: Vec<Value> = targets
            .into_iter()
            .map(|_registry_id| {
                let token = B64.encode(format!("AWS:{}", Uuid::new_v4()).as_bytes());
                json!({
                    "authorizationToken": token,
                    "expiresAt": expires_at,
                    "proxyEndpoint": endpoint,
                })
            })
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "authorizationData": authorization_data,
        })))
    }

    pub(super) fn describe_registry(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts.get(&account);
        let registry_id = state
            .map(|s| s.account_id.clone())
            .unwrap_or_else(|| account.clone());
        let rules = state
            .and_then(|s| s.replication_configuration.as_ref())
            .map(|cfg| {
                cfg.rules
                    .iter()
                    .map(|r| {
                        json!({
                            "destinations": r.destinations.iter().map(|d| json!({
                                "region": d.region,
                                "registryId": d.registry_id,
                            })).collect::<Vec<_>>(),
                            "repositoryFilters": r.repository_filters.iter().map(|f| json!({
                                "filter": f.filter,
                                "filterType": f.filter_type,
                            })).collect::<Vec<_>>(),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "registryId": registry_id,
            "replicationConfiguration": {"rules": rules},
        })))
    }
}
