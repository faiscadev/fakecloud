//! Model-derived input validation for Amazon Managed Blockchain operations.
//!
//! Rejects requests that violate the Smithy contract (missing required members,
//! out-of-set enum values, out-of-range integers) with `InvalidRequestException`
//! -- a code every Managed Blockchain operation declares -- so the conformance
//! suite's negative variants land on a declared error rather than a routing miss
//! or a 500. Body member names are the restJson1 wire names (PascalCase; the
//! model declares no `@jsonName` overrides). Members bound to `@httpLabel` /
//! `@httpQuery` are validated in the handler (they are not part of the JSON
//! body).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

pub const FRAMEWORK: &[&str] = &["HYPERLEDGER_FABRIC", "ETHEREUM"];
pub const EDITION: &[&str] = &["STARTER", "STANDARD"];
pub const ACCESSOR_TYPE: &[&str] = &["BILLING_TOKEN"];
pub const VOTE_VALUE: &[&str] = &["YES", "NO"];
pub const STATE_DB: &[&str] = &["LevelDB", "CouchDB"];
pub const ACCESSOR_NETWORK_TYPE: &[&str] = &[
    "ETHEREUM_GOERLI",
    "ETHEREUM_MAINNET",
    "ETHEREUM_MAINNET_AND_GOERLI",
    "POLYGON_MAINNET",
    "POLYGON_MUMBAI",
];

fn invalid_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", msg)
}

/// A present, non-null required member.
fn require<'a>(b: &'a Value, field: &str) -> Result<&'a Value, AwsServiceError> {
    match b.get(field) {
        Some(Value::Null) | None => Err(invalid_request(&format!(
            "The request failed because it is missing the required parameter '{field}'."
        ))),
        Some(Value::String(s)) if s.is_empty() => {
            Err(invalid_request(&format!("'{field}' must not be empty.")))
        }
        Some(v) => Ok(v),
    }
}

/// An optional string member is one of `allowed` (case-sensitive), if present.
fn enum_opt(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field).and_then(Value::as_str) {
        if !allowed.contains(&v) {
            return Err(invalid_request(&format!(
                "'{field}' must be one of [{}], got '{v}'.",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

/// A required string member is one of `allowed` (case-sensitive).
fn enum_req(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    require(b, field)?;
    enum_opt(b, field, allowed)
}

/// An optional string member's length is within `[min, max]` (in characters),
/// if present.
fn len_opt(b: &Value, field: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        let n = s.chars().count();
        if n < min || n > max {
            return Err(invalid_request(&format!(
                "'{field}' length must be between {min} and {max}, got {n}."
            )));
        }
    }
    Ok(())
}

/// Validate the required nested `MemberConfiguration` (Fabric admin credentials).
fn validate_member_configuration(b: &Value) -> Result<(), AwsServiceError> {
    let mc = require(b, "MemberConfiguration")?;
    require(mc, "Name")?;
    let fc = require(mc, "FrameworkConfiguration")?;
    // Hyperledger Fabric member configuration carries admin credentials.
    if let Some(fabric) = fc.get("Fabric") {
        if !fabric.is_null() {
            require(fabric, "AdminUsername")?;
            require(fabric, "AdminPassword")?;
        }
    }
    Ok(())
}

/// Validate an operation's already-parsed JSON body against the model.
pub fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    match action {
        "CreateNetwork" => {
            require(body, "ClientRequestToken")?;
            len_opt(body, "ClientRequestToken", 1, 64)?;
            require(body, "Name")?;
            len_opt(body, "Name", 1, 64)?;
            len_opt(body, "Description", 0, 128)?;
            enum_req(body, "Framework", FRAMEWORK)?;
            require(body, "FrameworkVersion")?;
            len_opt(body, "FrameworkVersion", 1, 8)?;
            require(body, "VotingPolicy")?;
            validate_member_configuration(body)?;
            validate_framework_configuration(body)?;
        }
        "CreateMember" => {
            require(body, "ClientRequestToken")?;
            len_opt(body, "ClientRequestToken", 1, 64)?;
            require(body, "InvitationId")?;
            len_opt(body, "InvitationId", 1, 32)?;
            validate_member_configuration(body)?;
        }
        "CreateNode" => {
            require(body, "ClientRequestToken")?;
            len_opt(body, "ClientRequestToken", 1, 64)?;
            len_opt(body, "MemberId", 1, 32)?;
            let nc = require(body, "NodeConfiguration")?;
            require(nc, "InstanceType")?;
            enum_opt(nc, "StateDB", STATE_DB)?;
        }
        "CreateProposal" => {
            require(body, "ClientRequestToken")?;
            len_opt(body, "ClientRequestToken", 1, 64)?;
            require(body, "MemberId")?;
            len_opt(body, "MemberId", 1, 32)?;
            len_opt(body, "Description", 0, 128)?;
            require(body, "Actions")?;
        }
        "CreateAccessor" => {
            require(body, "ClientRequestToken")?;
            len_opt(body, "ClientRequestToken", 1, 64)?;
            enum_req(body, "AccessorType", ACCESSOR_TYPE)?;
            enum_opt(body, "NetworkType", ACCESSOR_NETWORK_TYPE)?;
        }
        "VoteOnProposal" => {
            require(body, "VoterMemberId")?;
            enum_req(body, "Vote", VOTE_VALUE)?;
        }
        "TagResource" => {
            require(body, "Tags")?;
        }
        "UntagResource" => {
            // TagKeys arrive as an `@httpQuery` list, validated in the handler.
        }
        _ => {}
    }
    Ok(())
}

/// Validate the optional `FrameworkConfiguration` (network-level Fabric edition).
fn validate_framework_configuration(b: &Value) -> Result<(), AwsServiceError> {
    if let Some(fc) = b.get("FrameworkConfiguration") {
        if !fc.is_null() {
            if let Some(fabric) = fc.get("Fabric") {
                if !fabric.is_null() {
                    enum_req(fabric, "Edition", EDITION)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_member_config() -> Value {
        json!({
            "Name": "member1",
            "FrameworkConfiguration": {
                "Fabric": { "AdminUsername": "admin", "AdminPassword": "Password123" }
            }
        })
    }

    #[test]
    fn create_network_requires_core_fields() {
        assert!(validate_input("CreateNetwork", &json!({})).is_err());
        let ok = json!({
            "ClientRequestToken": "t",
            "Name": "net",
            "Framework": "HYPERLEDGER_FABRIC",
            "FrameworkVersion": "2.2",
            "VotingPolicy": { "ApprovalThresholdPolicy": { "ThresholdPercentage": 50 } },
            "MemberConfiguration": valid_member_config(),
        });
        assert!(validate_input("CreateNetwork", &ok).is_ok());
    }

    #[test]
    fn create_network_rejects_bad_framework() {
        let bad = json!({
            "ClientRequestToken": "t",
            "Name": "net",
            "Framework": "BOGUS",
            "FrameworkVersion": "2.2",
            "VotingPolicy": {},
            "MemberConfiguration": valid_member_config(),
        });
        assert!(validate_input("CreateNetwork", &bad).is_err());
    }

    #[test]
    fn create_member_requires_admin_credentials() {
        let bad = json!({
            "ClientRequestToken": "t",
            "InvitationId": "i-1",
            "MemberConfiguration": { "Name": "m", "FrameworkConfiguration": { "Fabric": {} } },
        });
        assert!(validate_input("CreateMember", &bad).is_err());
    }

    #[test]
    fn create_accessor_requires_type() {
        assert!(validate_input("CreateAccessor", &json!({ "ClientRequestToken": "t" })).is_err());
        assert!(validate_input(
            "CreateAccessor",
            &json!({ "ClientRequestToken": "t", "AccessorType": "BILLING_TOKEN" })
        )
        .is_ok());
    }

    #[test]
    fn vote_requires_valid_value() {
        assert!(validate_input(
            "VoteOnProposal",
            &json!({ "VoterMemberId": "m-1", "Vote": "MAYBE" })
        )
        .is_err());
        assert!(validate_input(
            "VoteOnProposal",
            &json!({ "VoterMemberId": "m-1", "Vote": "YES" })
        )
        .is_ok());
    }
}
