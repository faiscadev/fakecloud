use http::StatusCode;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{body_json, LogsService};

impl LogsService {
    // ---- Tags (legacy) ----

    pub(crate) fn tag_log_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;

        let tags = body["tags"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "tags is required",
            )
        })?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        for (k, v) in tags {
            group
                .tags
                .insert(k.clone(), v.as_str().unwrap_or("").to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn untag_log_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;

        let keys = body["tags"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "tags is required",
            )
        })?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        for key in keys {
            if let Some(k) = key.as_str() {
                group.tags.remove(k);
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn list_tags_log_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;

        let state = self.state.read();
        let group = state.log_groups.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "tags": group.tags })).unwrap(),
        ))
    }

    // ---- Tags (new API: TagResource/UntagResource/ListTagsForResource) ----

    pub(crate) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let arn = body["resourceArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "resourceArn is required",
            )
        })?;

        validate_string_length("resourceArn", arn, 1, 1011)?;

        let tags = body["tags"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "tags is required",
            )
        })?;

        let new_tags: std::collections::HashMap<String, String> = tags
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
            .collect();

        let mut state = self.state.write();

        // Try log group
        if let Some(group) = state
            .log_groups
            .values_mut()
            .find(|g| g.arn == arn || g.arn.trim_end_matches(":*") == arn)
        {
            for (k, v) in new_tags {
                group.tags.insert(k, v);
            }
            return Ok(AwsResponse::json(StatusCode::OK, "{}"));
        }

        // Try destination
        if let Some(dest) = state.destinations.values_mut().find(|d| d.arn == arn) {
            for (k, v) in new_tags {
                dest.tags.insert(k, v);
            }
            return Ok(AwsResponse::json(StatusCode::OK, "{}"));
        }

        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("The specified resource does not exist: {arn}"),
        ))
    }

    pub(crate) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let arn = body["resourceArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "resourceArn is required",
            )
        })?;

        validate_string_length("resourceArn", arn, 1, 1011)?;

        let tag_keys = body["tagKeys"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "tagKeys is required",
            )
        })?;

        let keys: Vec<String> = tag_keys
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        let mut state = self.state.write();

        // Try log group
        if let Some(group) = state
            .log_groups
            .values_mut()
            .find(|g| g.arn == arn || g.arn.trim_end_matches(":*") == arn)
        {
            for k in &keys {
                group.tags.remove(k);
            }
            return Ok(AwsResponse::json(StatusCode::OK, "{}"));
        }

        // Try destination
        if let Some(dest) = state.destinations.values_mut().find(|d| d.arn == arn) {
            for k in &keys {
                dest.tags.remove(k);
            }
            return Ok(AwsResponse::json(StatusCode::OK, "{}"));
        }

        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("The specified resource does not exist: {arn}"),
        ))
    }

    pub(crate) fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let arn = body["resourceArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "resourceArn is required",
            )
        })?;

        validate_string_length("resourceArn", arn, 1, 1011)?;

        let state = self.state.read();

        // Try log group
        if let Some(group) = state
            .log_groups
            .values()
            .find(|g| g.arn == arn || g.arn.trim_end_matches(":*") == arn)
        {
            return Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({ "tags": group.tags })).unwrap(),
            ));
        }

        // Try destination
        if let Some(dest) = state.destinations.values().find(|d| d.arn == arn) {
            return Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({ "tags": dest.tags })).unwrap(),
            ));
        }

        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("The specified resource does not exist: {arn}"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- Tagging (new API: TagResource / UntagResource / ListTagsForResource) ----

    #[test]
    fn tag_resource_lifecycle_on_log_group() {
        let svc = make_service();
        create_group(&svc, "tag-grp");

        // Get the ARN
        let req = make_request(
            "DescribeLogGroups",
            json!({ "logGroupNamePrefix": "tag-grp" }),
        );
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let arn = body["logGroups"][0]["arn"].as_str().unwrap().to_string();

        // Tag
        let req = make_request(
            "TagResource",
            json!({
                "resourceArn": arn,
                "tags": { "env": "prod", "team": "platform" },
            }),
        );
        svc.tag_resource(&req).unwrap();

        // List tags
        let req = make_request("ListTagsForResource", json!({ "resourceArn": arn }));
        let resp = svc.list_tags_for_resource(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["tags"]["env"], "prod");
        assert_eq!(body["tags"]["team"], "platform");

        // Untag
        let req = make_request(
            "UntagResource",
            json!({
                "resourceArn": arn,
                "tagKeys": ["team"],
            }),
        );
        svc.untag_resource(&req).unwrap();

        let req = make_request("ListTagsForResource", json!({ "resourceArn": arn }));
        let resp = svc.list_tags_for_resource(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["tags"].as_object().unwrap().len(), 1);
        assert!(body["tags"].get("team").is_none());
    }

    #[test]
    fn tag_resource_on_destination() {
        let svc = make_service();

        let req = make_request(
            "PutDestination",
            json!({
                "destinationName": "tag-dest",
                "targetArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
                "roleArn": "arn:aws:iam::123456789012:role/r",
            }),
        );
        let resp = svc.put_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let arn = body["destination"]["arn"].as_str().unwrap().to_string();

        let req = make_request(
            "TagResource",
            json!({ "resourceArn": arn, "tags": { "key1": "val1" } }),
        );
        svc.tag_resource(&req).unwrap();

        let req = make_request("ListTagsForResource", json!({ "resourceArn": arn }));
        let resp = svc.list_tags_for_resource(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["tags"]["key1"], "val1");
    }

    #[test]
    fn tag_resource_nonexistent_errors() {
        let svc = make_service();
        let req = make_request(
            "TagResource",
            json!({
                "resourceArn": "arn:aws:logs:us-east-1:123456789012:log-group:nope:*",
                "tags": { "k": "v" },
            }),
        );
        assert!(svc.tag_resource(&req).is_err());
    }
}
