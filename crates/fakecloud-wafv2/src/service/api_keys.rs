//! `Wafv2Service` `api_keys` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn create_api_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let scope = require_scope(&body)?;
        let token_domains = parse_string_list(body.get("TokenDomains"));
        if token_domains.is_empty() {
            return Err(invalid_param(
                "TokenDomains must contain at least one entry",
            ));
        }
        // API keys in real WAF are opaque encrypted blobs. fakecloud encodes
        // a deterministic JSON payload so callers can round-trip via
        // GetDecryptedAPIKey without storing extra state.
        let payload = json!({
            "tokenDomains": token_domains,
            "scope": scope,
            "version": 1,
            "id": Uuid::new_v4().to_string(),
        });
        let api_key = base64::engine::general_purpose::STANDARD
            .encode(serde_json::to_vec(&payload).unwrap_or_default());
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        account.api_keys.insert(
            api_key.clone(),
            ApiKey {
                api_key: api_key.clone(),
                scope,
                token_domains,
                version: 1,
                creation_timestamp: Utc::now(),
            },
        );
        Ok(AwsResponse::ok_json(json!({ "APIKey": api_key })))
    }

    pub(super) fn delete_api_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let api_key = require_str(&body, "APIKey")?;
        let _scope = require_scope(&body)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.api_keys.remove(&api_key).is_none() {
            return Err(not_found("APIKey"));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_decrypted_api_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let api_key = require_str(&body, "APIKey")?;
        let _scope = require_scope(&body)?;
        let state = self.state.read();
        let key = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.api_keys.get(&api_key))
            .cloned()
            .ok_or_else(|| not_found("APIKey"))?;
        Ok(AwsResponse::ok_json(json!({
            "TokenDomains": key.token_domains,
            "CreationTimestamp": key.creation_timestamp.timestamp() as f64,
        })))
    }

    pub(super) fn list_api_keys(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let scope = require_scope(&body)?;
        validate_opt_limit(&body)?;
        validate_opt_next_marker(&body)?;
        let limit = body.get("Limit").and_then(Value::as_u64).unwrap_or(100) as usize;
        let next_marker = body
            .get("NextMarker")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let state = self.state.read();
        let mut all: Vec<ApiKey> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.api_keys
                    .values()
                    .filter(|k| k.scope == scope)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        all.sort_by(|a, b| a.api_key.cmp(&b.api_key));
        let (page, next) = paginate(&all, next_marker.as_deref(), limit);
        let summaries: Vec<Value> = page
            .iter()
            .map(|k| {
                json!({
                    "TokenDomains": k.token_domains,
                    "APIKey": k.api_key,
                    "CreationTimestamp": k.creation_timestamp.timestamp() as f64,
                    "Version": k.version,
                })
            })
            .collect();
        let mut response = json!({
            "APIKeySummaries": summaries,
            "ApplicationIntegrationURL": format!("https://wafv2-token.{}.amazonaws.com/", req.region),
        });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextMarker".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }
}
