use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{body_json, LogsService};
use chrono::Utc;

use crate::state::Destination;

impl LogsService {
    // ---- Destinations ----

    pub(crate) fn put_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let destination_name = body["destinationName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "destinationName is required",
                )
            })?
            .to_string();
        let target_arn = body["targetArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "targetArn is required",
                )
            })?
            .to_string();
        let role_arn = body["roleArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "roleArn is required",
                )
            })?
            .to_string();

        validate_string_length("destinationName", &destination_name, 1, 512)?;
        validate_string_length("targetArn", &target_arn, 1, 2048)?;
        validate_string_length("roleArn", &role_arn, 1, 2048)?;

        let tags: std::collections::HashMap<String, String> = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();
        let arn = format!(
            "arn:aws:logs:{}:{}:destination:{}",
            state.region, state.account_id, destination_name
        );
        let now = Utc::now().timestamp_millis();

        // Update or create
        let access_policy = state
            .destinations
            .get(&destination_name)
            .and_then(|d| d.access_policy.clone());

        let dest = Destination {
            destination_name: destination_name.clone(),
            target_arn: target_arn.clone(),
            role_arn: role_arn.clone(),
            arn: arn.clone(),
            access_policy,
            creation_time: now,
            tags: tags.clone(),
        };

        state.destinations.insert(destination_name.clone(), dest);

        let dest_json = json!({
            "destinationName": destination_name,
            "targetArn": target_arn,
            "roleArn": role_arn,
            "arn": arn,
            "creationTime": now,
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "destination": dest_json })).unwrap(),
        ))
    }

    pub(crate) fn describe_destinations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let prefix = body["DestinationNamePrefix"].as_str().unwrap_or("");

        validate_optional_string_length(
            "DestinationNamePrefix",
            body["DestinationNamePrefix"].as_str(),
            1,
            512,
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;

        let state = self.state.read();
        let destinations: Vec<Value> = state
            .destinations
            .values()
            .filter(|d| prefix.is_empty() || d.destination_name.starts_with(prefix))
            .map(|d| {
                let mut obj = json!({
                    "destinationName": d.destination_name,
                    "targetArn": d.target_arn,
                    "roleArn": d.role_arn,
                    "arn": d.arn,
                    "creationTime": d.creation_time,
                });
                if let Some(ref policy) = d.access_policy {
                    obj["accessPolicy"] = json!(policy);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "destinations": destinations })).unwrap(),
        ))
    }

    pub(crate) fn delete_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["destinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "destinationName is required",
            )
        })?;

        validate_string_length("destinationName", name, 1, 512)?;

        let mut state = self.state.write();
        if state.destinations.remove(name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified destination does not exist: {name}"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn put_destination_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["destinationName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "destinationName is required",
            )
        })?;

        validate_string_length("destinationName", name, 1, 512)?;

        let policy = body["accessPolicy"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "accessPolicy is required",
            )
        })?;

        validate_string_length("accessPolicy", policy, 1, 5120)?;

        let mut state = self.state.write();
        let dest = state.destinations.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified destination does not exist: {name}"),
            )
        })?;

        dest.access_policy = Some(policy.to_string());

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- Destinations ----

    #[test]
    fn destination_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "PutDestination",
            json!({
                "destinationName": "my-dest",
                "targetArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
                "roleArn": "arn:aws:iam::123456789012:role/logs-role",
            }),
        );
        let resp = svc.put_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["destination"]["destinationName"], "my-dest");
        assert!(body["destination"]["arn"]
            .as_str()
            .unwrap()
            .contains("my-dest"));

        // Set access policy
        let req = make_request(
            "PutDestinationPolicy",
            json!({
                "destinationName": "my-dest",
                "accessPolicy": "{\"Version\":\"2012-10-17\"}",
            }),
        );
        svc.put_destination_policy(&req).unwrap();

        // Describe
        let req = make_request("DescribeDestinations", json!({}));
        let resp = svc.describe_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let dests = body["destinations"].as_array().unwrap();
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0]["accessPolicy"], "{\"Version\":\"2012-10-17\"}");

        // Delete
        let req = make_request("DeleteDestination", json!({ "destinationName": "my-dest" }));
        svc.delete_destination(&req).unwrap();

        let req = make_request("DescribeDestinations", json!({}));
        let resp = svc.describe_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["destinations"].as_array().unwrap().is_empty());
    }

    #[test]
    fn delete_destination_nonexistent_errors() {
        let svc = make_service();
        let req = make_request("DeleteDestination", json!({ "destinationName": "nope" }));
        assert!(svc.delete_destination(&req).is_err());
    }

    #[test]
    fn put_destination_policy_nonexistent_errors() {
        let svc = make_service();
        let req = make_request(
            "PutDestinationPolicy",
            json!({
                "destinationName": "nope",
                "accessPolicy": "{}",
            }),
        );
        assert!(svc.put_destination_policy(&req).is_err());
    }
}
