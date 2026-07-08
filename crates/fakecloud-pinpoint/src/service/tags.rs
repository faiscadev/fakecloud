//! ARN-keyed tag handlers.
//!
//! `TagResource` / `UntagResource` / `ListTagsForResource` declare no errors in
//! the Pinpoint model, so these never fail: an unknown ARN simply has an empty
//! tag set.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::{no_content, ok, Ctx, PinpointService};

impl PinpointService {
    pub(super) fn tag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let incoming = body
            .get("tags")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let entry = data.tags.entry(arn.to_string()).or_default();
        for (k, v) in incoming {
            entry.insert(k, v.as_str().unwrap_or_default().to_string());
        }
        no_content()
    }

    pub(super) fn untag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let keys: Vec<&str> = q
            .iter()
            .filter(|(k, _)| k == "tagKeys" || k == "TagKeys")
            .map(|(_, v)| v.as_str())
            .collect();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(entry) = data.tags.get_mut(arn) {
            for key in &keys {
                entry.remove(*key);
            }
        }
        no_content()
    }

    pub(super) fn list_tags_for_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let tags: BTreeMap<String, String> = guard
            .get(&ctx.account)
            .and_then(|d| d.tags.get(arn))
            .cloned()
            .unwrap_or_default();
        ok(json!({ "tags": tags }))
    }
}
