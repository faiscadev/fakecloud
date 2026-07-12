//! Model-driven input validation for Amazon Pinpoint.
//!
//! Pinpoint's Smithy model puts almost no scalar constraints on top-level
//! request members: no `@length` / `@range`, and the only enum-typed inputs are
//! nested inside the request payload structures (so the conformance
//! negative-testing strategy never reaches them from a top-level member). The
//! observable negative cases are therefore:
//!
//! * a missing required `@httpLabel` (handled in `service.rs` by rejecting an
//!   empty / `{Placeholder}` label), and
//! * a missing required member of a required `@httpPayload` structure.
//!
//! This module enforces the second: for every operation whose payload declares
//! required members, each must be present (and non-null) in the request body,
//! or the request is rejected with `BadRequestException` — the code Pinpoint
//! declares for every one of these operations.

use serde_json::Value;

use fakecloud_core::service::AwsServiceError;
use http::StatusCode;

/// The required members of each operation's `@httpPayload` structure. Only
/// operations whose payload has at least one required member appear here; every
/// other operation's body is unconstrained at the contract level.
fn required_payload_members(action: &str) -> &'static [&'static str] {
    match action {
        "CreateApp" => &["Name"],
        "CreateExportJob" => &["RoleArn", "S3UrlPrefix"],
        "CreateImportJob" => &["Format", "RoleArn", "S3Url"],
        "CreateJourney" => &["Name"],
        "CreateRecommenderConfiguration" => {
            &["RecommendationProviderRoleArn", "RecommendationProviderUri"]
        }
        "PutEventStream" => &["DestinationStreamArn", "RoleArn"],
        "PutEvents" => &["BatchItem"],
        "SendMessages" => &["MessageConfiguration"],
        "SendOTPMessage" => &[
            "BrandName",
            "Channel",
            "DestinationIdentity",
            "OriginationIdentity",
            "ReferenceId",
        ],
        "SendUsersMessages" => &["MessageConfiguration", "Users"],
        "TagResource" => &["tags"],
        "UpdateEndpointsBatch" => &["Item"],
        "UpdateAdmChannel" => &["ClientId", "ClientSecret"],
        "UpdateBaiduChannel" => &["ApiKey", "SecretKey"],
        "UpdateEmailChannel" => &["FromAddress", "Identity"],
        "UpdateJourney" => &["Name"],
        "UpdateRecommenderConfiguration" => {
            &["RecommendationProviderRoleArn", "RecommendationProviderUri"]
        }
        "VerifyOTPMessage" => &["DestinationIdentity", "Otp", "ReferenceId"],
        _ => &[],
    }
}

/// Validate the request body for `action`, erroring `BadRequestException` when a
/// required payload member is missing or null.
pub fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    for member in required_payload_members(action) {
        let present = body.get(*member).map(|v| !v.is_null()).unwrap_or(false);
        if !present {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                format!("Missing required parameter: {member}"),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn is_bad_request(err: &AwsServiceError) -> bool {
        matches!(
            err,
            AwsServiceError::AwsError { status, code, .. }
                if *status == StatusCode::BAD_REQUEST && code == "BadRequestException"
        )
    }

    #[test]
    fn missing_required_member_rejected() {
        let err = validate_input("CreateApp", &json!({})).unwrap_err();
        assert!(is_bad_request(&err));
    }

    #[test]
    fn present_member_ok() {
        assert!(validate_input("CreateApp", &json!({ "Name": "demo" })).is_ok());
    }

    #[test]
    fn unconstrained_op_ok_with_empty_body() {
        assert!(validate_input("CreateCampaign", &json!({})).is_ok());
    }

    #[test]
    fn null_member_rejected() {
        let err = validate_input("CreateJourney", &json!({ "Name": null })).unwrap_err();
        assert!(is_bad_request(&err));
    }
}
