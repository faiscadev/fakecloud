//! `Wafv2Service` `tags` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require_str(&body, "ResourceARN")?;
        let tags = parse_tags(body.get("Tags"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !resource_exists(account, &arn) {
            return Err(not_found("Resource"));
        }
        let entry = account.tags.entry(arn).or_default();
        for (k, v) in tags {
            entry.insert(k, v);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require_str(&body, "ResourceARN")?;
        let keys = parse_string_list(body.get("TagKeys"));
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !resource_exists(account, &arn) {
            return Err(not_found("Resource"));
        }
        if let Some(t) = account.tags.get_mut(&arn) {
            for k in keys {
                t.remove(&k);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require_str(&body, "ResourceARN")?;
        let state = self.state.read();
        let account = state.accounts.get(&req.account_id);
        let exists = account.is_some_and(|a| resource_exists(a, &arn));
        if !exists {
            return Err(not_found("Resource"));
        }
        let tags = account
            .and_then(|a| a.tags.get(&arn))
            .cloned()
            .unwrap_or_default();
        let tag_list: Vec<Value> = tags
            .into_iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "TagInfoForResource": {
                "ResourceARN": arn,
                "TagList": tag_list,
            },
        })))
    }
}
