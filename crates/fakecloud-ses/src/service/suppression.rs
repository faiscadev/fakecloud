use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::SuppressedDestination;

use super::SesV2Service;

impl SesV2Service {
    pub(super) fn put_suppressed_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let email = match body["EmailAddress"].as_str() {
            Some(e) => e.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EmailAddress is required",
                ));
            }
        };
        let reason = match body["Reason"].as_str() {
            Some(r) if r == "BOUNCE" || r == "COMPLAINT" => r.to_string(),
            Some(_) => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Reason must be BOUNCE or COMPLAINT",
                ));
            }
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Reason is required",
                ));
            }
        };

        let mut state = self.state.write();
        state.suppressed_destinations.insert(
            email.clone(),
            SuppressedDestination {
                email_address: email,
                reason,
                last_update_time: Utc::now(),
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn get_suppressed_destination(
        &self,
        email: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let dest = match state.suppressed_destinations.get(email) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("{} is not on the suppression list", email),
                ));
            }
        };

        let response = json!({
            "SuppressedDestination": {
                "EmailAddress": dest.email_address,
                "Reason": dest.reason,
                "LastUpdateTime": dest.last_update_time.timestamp() as f64,
            }
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn delete_suppressed_destination(
        &self,
        email: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        if state.suppressed_destinations.remove(email).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("{} is not on the suppression list", email),
            ));
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn list_suppressed_destinations(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let summaries: Vec<Value> = state
            .suppressed_destinations
            .values()
            .map(|d| {
                json!({
                    "EmailAddress": d.email_address,
                    "Reason": d.reason,
                    "LastUpdateTime": d.last_update_time.timestamp() as f64,
                })
            })
            .collect();

        let response = json!({
            "SuppressedDestinationSummaries": summaries,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }
}
