use http::StatusCode;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{extract_log_group_from_arn, require_str, LogsService};
use crate::state::SyslogConfiguration;
use chrono::Utc;

/// Resolve a `logGroupIdentifier` (a log group name or ARN) to the bare log
/// group name used as the state key.
fn identifier_to_group_name(identifier: &str) -> Result<String, AwsServiceError> {
    if identifier.starts_with("arn:") {
        extract_log_group_from_arn(identifier).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Invalid ARN: {identifier}"),
            )
        })
    } else {
        Ok(identifier.to_string())
    }
}

/// Validate a VPC endpoint id against the Smithy pattern `^vpce-[0-9a-f]{1,64}$`.
fn validate_vpc_endpoint_id(value: &str) -> Result<(), AwsServiceError> {
    let rest = value.strip_prefix("vpce-").filter(|r| {
        !r.is_empty()
            && r.len() <= 64
            && r.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    });
    if rest.is_some() {
        Ok(())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            format!("vpcEndpointId does not match required pattern: {value}"),
        ))
    }
}

impl LogsService {
    // ---- Syslog configurations ----

    pub(crate) fn put_syslog_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identifier = require_str(&body, "logGroupIdentifier")?.to_string();
        let group_name = identifier_to_group_name(&identifier)?;

        let vpc_endpoint_id = body["vpcEndpointId"].as_str().map(|s| s.to_string());
        if let Some(id) = &vpc_endpoint_id {
            validate_vpc_endpoint_id(id)?;
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let group = state.log_groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;
        let log_group_arn = group.arn.clone();

        let created_at = state
            .syslog_configurations
            .get(&group_name)
            .map(|c| c.created_at)
            .unwrap_or_else(|| Utc::now().timestamp_millis());

        state.syslog_configurations.insert(
            group_name,
            SyslogConfiguration {
                log_group_arn,
                source_type: "VPCE".to_string(),
                vpc_endpoint_id,
                created_at,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn list_syslog_configurations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let group_filter = body["logGroupIdentifier"]
            .as_str()
            .map(identifier_to_group_name)
            .transpose()?;
        let vpc_filter = body["vpcEndpointId"].as_str();

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let configs: Vec<_> = state
            .syslog_configurations
            .iter()
            .filter(|(group_name, _)| {
                group_filter
                    .as_ref()
                    .is_none_or(|g| g == group_name.as_str())
            })
            .filter(|(_, c)| vpc_filter.is_none_or(|v| c.vpc_endpoint_id.as_deref() == Some(v)))
            .map(|(_, c)| {
                json!({
                    "logGroupArn": c.log_group_arn,
                    "sourceType": c.source_type,
                    "vpcEndpointId": c.vpc_endpoint_id,
                    "createdAt": c.created_at,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "syslogConfigurations": configs })).unwrap(),
        ))
    }

    pub(crate) fn delete_syslog_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identifier = require_str(&body, "logGroupIdentifier")?.to_string();
        let group_name = identifier_to_group_name(&identifier)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.syslog_configurations.remove(&group_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("No syslog configuration found for log group: {group_name}"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}
