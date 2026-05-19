//! `AthenaService` `tags` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl AthenaService {
    pub(super) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: ResourceARN @length(1,1011), Tags @required.
        let arn = validate_required_string_len(&body, "ResourceARN", 1, 1011)?;
        validate_required_list(&body, "Tags")?;
        let tags = parse_tags(body.get("Tags"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let entry = account.tags.entry(arn).or_default();
        for (k, v) in tags {
            entry.insert(k, v);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: ResourceARN @length(1,1011), TagKeys @required.
        let arn = validate_required_string_len(&body, "ResourceARN", 1, 1011)?;
        validate_required_list(&body, "TagKeys")?;
        let keys = parse_string_list(body.get("TagKeys"));
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
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
        // Smithy: ResourceARN targets AmazonResourceName @length(1,1011),
        // NextToken targets Token @length(1,1024),
        // MaxResults targets MaxTagsCount @range(min:75).
        let arn = validate_required_string_len(&body, "ResourceARN", 1, 1011)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        if let Some(v) = body.get("MaxResults").and_then(Value::as_i64) {
            if v < 75 {
                return Err(invalid_request(format!(
                    "MaxResults value {v} is below the minimum 75",
                )));
            }
        }
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let tags = account.tags.get(&arn).cloned().unwrap_or_default();
        let tag_list: Vec<Value> = tags
            .into_iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect();
        Ok(AwsResponse::ok_json(json!({ "Tags": tag_list })))
    }
}
