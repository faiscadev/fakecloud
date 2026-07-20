//! `Wafv2Service` `regex_pattern_sets` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn create_regex_pattern_set(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str_len(&body, "Name", 1, 128)?;
        let scope = require_scope(&body)?;
        let regular_expressions = body
            .get("RegularExpressionList")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let tags = parse_tags(body.get("Tags"))?;
        let key = (scope.clone(), name.clone());
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.regex_pattern_sets.contains_key(&key) {
            return Err(already_exists(&format!(
                "RegexPatternSet {name} already exists"
            )));
        }
        let id = synth_uuid();
        let arn = synth_arn(
            &req.account_id,
            &req.region,
            &scope,
            "regexpatternset",
            &name,
            &id,
        );
        let lock_token = synth_uuid();
        let summary = regex_set_summary_json(&id, &name, &arn, description.as_deref(), &lock_token);
        let set = RegexPatternSet {
            id,
            name,
            arn: arn.clone(),
            scope,
            description,
            regular_expressions,
            lock_token,
            created_time: Utc::now(),
        };
        account.regex_pattern_sets.insert(key, set);
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        Ok(AwsResponse::ok_json(json!({ "Summary": summary })))
    }

    pub(super) fn get_regex_pattern_set(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str_len(&body, "Name", 1, 128)?;
        let scope = require_scope(&body)?;
        let id = require_str_len(&body, "Id", 1, 36)?;
        let state = self.state.read();
        let set = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.regex_pattern_sets.get(&(scope, name)))
            .ok_or_else(|| not_found("RegexPatternSet"))?
            .clone();
        if set.id != id {
            return Err(not_found("RegexPatternSet"));
        }
        Ok(AwsResponse::ok_json(json!({
            "RegexPatternSet": regex_set_detail_json(&set),
            "LockToken": set.lock_token,
        })))
    }

    pub(super) fn list_regex_pattern_sets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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
        let mut all: Vec<RegexPatternSet> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.regex_pattern_sets
                    .values()
                    .filter(|x| x.scope == scope)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let (page, next) = paginate_checked(&all, next_marker.as_deref(), limit)
            .map_err(|_| invalid_param("The specified NextMarker is not valid."))?;
        let summaries: Vec<Value> = page
            .iter()
            .map(|s| {
                regex_set_summary_json(
                    &s.id,
                    &s.name,
                    &s.arn,
                    s.description.as_deref(),
                    &s.lock_token,
                )
            })
            .collect();
        let mut response = json!({ "RegexPatternSets": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextMarker".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn update_regex_pattern_set(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let scope = require_scope(&body)?;
        let id_in = require_str(&body, "Id")?;
        let lock_token_in = require_str(&body, "LockToken")?;
        let regular_expressions = body
            .get("RegularExpressionList")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let set = account
            .regex_pattern_sets
            .get_mut(&(scope, name))
            .ok_or_else(|| not_found("RegexPatternSet"))?;
        if set.id != id_in {
            return Err(invalid_param("Id does not match the named RegexPatternSet"));
        }
        if set.lock_token != lock_token_in {
            return Err(stale_lock_token());
        }
        set.regular_expressions = regular_expressions;
        set.description = description;
        set.lock_token = synth_uuid();
        Ok(AwsResponse::ok_json(
            json!({ "NextLockToken": set.lock_token }),
        ))
    }

    pub(super) fn delete_regex_pattern_set(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let scope = require_scope(&body)?;
        let id_in = require_str(&body, "Id")?;
        let lock_token_in = require_str(&body, "LockToken")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let key = (scope, name);
        let set = account
            .regex_pattern_sets
            .get(&key)
            .ok_or_else(|| not_found("RegexPatternSet"))?;
        if set.id != id_in {
            return Err(invalid_param("Id does not match the named RegexPatternSet"));
        }
        if set.lock_token != lock_token_in {
            return Err(stale_lock_token());
        }
        let arn = set.arn.clone();
        account.regex_pattern_sets.remove(&key);
        account.tags.remove(&arn);
        Ok(AwsResponse::ok_json(json!({})))
    }
}
