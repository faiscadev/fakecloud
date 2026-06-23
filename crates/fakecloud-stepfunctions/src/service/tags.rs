//! `StepFunctionsService` `tags` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StepFunctionsService {
    // ─── Tagging ────────────────────────────────────────────────────────

    pub(super) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("resourceArn", &body["resourceArn"])?;
        let arn = body["resourceArn"]
            .as_str()
            .ok_or_else(|| missing("resourceArn"))?;
        validate_arn(arn)?;
        validate_required("tags", &body["tags"])?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let tags_map = resource_tags_mut(state, arn).ok_or_else(|| resource_not_found(arn))?;

        fakecloud_core::tags::apply_tags(tags_map, &body, "tags", "key", "value").map_err(|f| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("{f} must be a list"),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("resourceArn", &body["resourceArn"])?;
        let arn = body["resourceArn"]
            .as_str()
            .ok_or_else(|| missing("resourceArn"))?;
        validate_arn(arn)?;
        validate_required("tagKeys", &body["tagKeys"])?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let tags_map = resource_tags_mut(state, arn).ok_or_else(|| resource_not_found(arn))?;

        fakecloud_core::tags::remove_tags(tags_map, &body, "tagKeys").map_err(|f| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("{f} must be a list"),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("resourceArn", &body["resourceArn"])?;
        let arn = body["resourceArn"]
            .as_str()
            .ok_or_else(|| missing("resourceArn"))?;
        validate_arn(arn)?;

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let tags_map = resource_tags(state, arn).ok_or_else(|| resource_not_found(arn))?;

        let tags = fakecloud_core::tags::tags_to_json(tags_map, "key", "value");

        Ok(AwsResponse::ok_json(json!({ "tags": tags })))
    }
}

/// Resolve the tag map for a taggable Step Functions resource ARN — a state
/// machine or an activity (both carry tags in AWS).
fn resource_tags<'a>(
    state: &'a StepFunctionsState,
    arn: &str,
) -> Option<&'a BTreeMap<String, String>> {
    state
        .state_machines
        .get(arn)
        .map(|sm| &sm.tags)
        .or_else(|| state.activities.get(arn).map(|a| &a.tags))
}

fn resource_tags_mut<'a>(
    state: &'a mut StepFunctionsState,
    arn: &str,
) -> Option<&'a mut BTreeMap<String, String>> {
    if state.state_machines.contains_key(arn) {
        return state.state_machines.get_mut(arn).map(|sm| &mut sm.tags);
    }
    state.activities.get_mut(arn).map(|a| &mut a.tags)
}
