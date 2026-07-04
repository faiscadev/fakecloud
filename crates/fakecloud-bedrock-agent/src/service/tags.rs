//! `BedrockAgentService` `tags` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl BedrockAgentService {
    pub(super) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "resourceArn")?;
        let tags = body["tags"]
            .as_object()
            .ok_or_else(|| missing("tags"))?
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect::<BTreeMap<_, _>>();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let entry = state.tags.entry(arn).or_default();
        for (k, v) in tags {
            entry.insert(k, v);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "resourceArn")?;
        // `tagKeys` is an `@httpQuery` list sent as repeated `tagKeys=a&tagKeys=b`
        // pairs; `query_params` collapses repeats to the last value, so parse
        // every occurrence out of the raw query string, percent-decoding each.
        // Fall back to a JSON body for clients that send the keys there.
        let keys: Vec<String> = if let Some(v) = body.get("tagKeys").and_then(|v| v.as_array()) {
            v.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        } else {
            req.raw_query
                .split('&')
                .filter_map(|pair| pair.strip_prefix("tagKeys="))
                .map(|v| {
                    percent_encoding::percent_decode_str(v)
                        .decode_utf8_lossy()
                        .into_owned()
                })
                .collect()
        };
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if let Some(entry) = state.tags.get_mut(&arn) {
            for k in keys {
                entry.remove(&k);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "resourceArn")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Resource {arn} not found")))?;
        let tags = state.tags.get(&arn).cloned().unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "tags": tags })))
    }
}
