//! `Wafv2Service` `ip_sets` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn create_ip_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str_len(&body, "Name", 1, 128)?;
        let scope = require_scope(&body)?;
        let ip_address_version = require_str(&body, "IPAddressVersion")?;
        let addresses = parse_string_list(body.get("Addresses"));
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let tags = parse_tags(body.get("Tags"))?;
        let key = (scope.clone(), name.clone());
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.ip_sets.contains_key(&key) {
            return Err(already_exists(&format!("IPSet {name} already exists")));
        }
        let id = synth_uuid();
        let arn = synth_arn(&req.account_id, &req.region, &scope, "ipset", &name, &id);
        let lock_token = synth_uuid();
        let summary = ip_set_summary_json(&id, &name, &arn, description.as_deref(), &lock_token);
        let set = IpSet {
            id,
            name,
            arn: arn.clone(),
            scope,
            description,
            ip_address_version,
            addresses,
            lock_token,
            created_time: Utc::now(),
        };
        account.ip_sets.insert(key, set);
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        Ok(AwsResponse::ok_json(json!({ "Summary": summary })))
    }

    pub(super) fn get_ip_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str_len(&body, "Name", 1, 128)?;
        let scope = require_scope(&body)?;
        // Id is required (Smithy @required) and must be a length(1, 36) string.
        let _id = require_str_len(&body, "Id", 1, 36)?;
        let state = self.state.read();
        let set = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.ip_sets.get(&(scope, name)))
            .ok_or_else(|| not_found("IPSet"))?
            .clone();
        Ok(AwsResponse::ok_json(json!({
            "IPSet": ip_set_detail_json(&set),
            "LockToken": set.lock_token,
        })))
    }

    pub(super) fn list_ip_sets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
        let mut all: Vec<IpSet> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.ip_sets
                    .values()
                    .filter(|x| x.scope == scope)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let (page, next) = paginate(&all, next_marker.as_deref(), limit);
        let summaries: Vec<Value> = page
            .iter()
            .map(|s| {
                ip_set_summary_json(
                    &s.id,
                    &s.name,
                    &s.arn,
                    s.description.as_deref(),
                    &s.lock_token,
                )
            })
            .collect();
        let mut response = json!({ "IPSets": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextMarker".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn update_ip_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let scope = require_scope(&body)?;
        let id_in = require_str(&body, "Id")?;
        let lock_token_in = require_str(&body, "LockToken")?;
        let addresses = parse_string_list(body.get("Addresses"));
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let set = account
            .ip_sets
            .get_mut(&(scope, name))
            .ok_or_else(|| not_found("IPSet"))?;
        if set.id != id_in {
            return Err(invalid_param("Id does not match the named IPSet"));
        }
        if set.lock_token != lock_token_in {
            return Err(stale_lock_token());
        }
        set.addresses = addresses;
        set.description = description;
        set.lock_token = synth_uuid();
        Ok(AwsResponse::ok_json(
            json!({ "NextLockToken": set.lock_token }),
        ))
    }

    pub(super) fn delete_ip_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let scope = require_scope(&body)?;
        let id_in = require_str(&body, "Id")?;
        let lock_token_in = require_str(&body, "LockToken")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let key = (scope, name);
        let set = account
            .ip_sets
            .get(&key)
            .ok_or_else(|| not_found("IPSet"))?;
        if set.id != id_in {
            return Err(invalid_param("Id does not match the named IPSet"));
        }
        if set.lock_token != lock_token_in {
            return Err(stale_lock_token());
        }
        let arn = set.arn.clone();
        account.ip_sets.remove(&key);
        account.tags.remove(&arn);
        Ok(AwsResponse::ok_json(json!({})))
    }
}
