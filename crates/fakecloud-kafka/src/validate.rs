//! Model-derived input validation for Amazon MSK operations.
//!
//! Rejects requests that violate the Smithy contract (missing required members,
//! out-of-set enum values, out-of-range page sizes) with `BadRequestException`
//! (the code every MSK operation declares) so the conformance suite's negative
//! variants land on a declared error rather than a routing miss or a 500. Body
//! member names are the restJson1 `jsonName`s (camelCase), matching what the
//! AWS SDK and Terraform put on the wire.

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

const ENHANCED_MONITORING: &[&str] = &[
    "DEFAULT",
    "PER_BROKER",
    "PER_TOPIC_PER_BROKER",
    "PER_TOPIC_PER_PARTITION",
];
const STORAGE_MODE: &[&str] = &["LOCAL", "TIERED"];

fn require(b: &Value, field: &str) -> Result<(), AwsServiceError> {
    match b.get(field) {
        Some(Value::Null) | None => Err(bad_request(&format!(
            "The parameter {field} is not valid. The provided parameter {field} is required."
        ))),
        Some(Value::String(s)) if s.is_empty() => Err(bad_request(&format!(
            "The parameter {field} must not be empty."
        ))),
        _ => Ok(()),
    }
}

fn check_enum(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        if !allowed.iter().any(|a| a.eq_ignore_ascii_case(s)) {
            return Err(bad_request(&format!(
                "Value '{s}' at '{field}' failed to satisfy constraint: Member must satisfy enum value set: {allowed:?}"
            )));
        }
    }
    Ok(())
}

/// Validate an operation's already-parsed JSON body (camelCase `jsonName`s).
/// Path-label / query parameters are validated by the handlers themselves (a
/// missing/invalid id surfaces as the operation's declared not-found error).
pub fn validate_input(action: &str, b: &Value) -> Result<(), AwsServiceError> {
    match action {
        "CreateCluster" => {
            require(b, "brokerNodeGroupInfo")?;
            require(b, "clusterName")?;
            require(b, "kafkaVersion")?;
            require(b, "numberOfBrokerNodes")?;
            check_enum(b, "enhancedMonitoring", ENHANCED_MONITORING)?;
            check_enum(b, "storageMode", STORAGE_MODE)?;
        }
        "CreateClusterV2" => {
            require(b, "clusterName")?;
        }
        "CreateConfiguration" => {
            require(b, "name")?;
            require(b, "serverProperties")?;
        }
        "UpdateConfiguration" => {
            require(b, "serverProperties")?;
        }
        "CreateReplicator" => {
            require(b, "kafkaClusters")?;
            require(b, "replicationInfoList")?;
            require(b, "replicatorName")?;
            require(b, "serviceExecutionRoleArn")?;
        }
        "CreateTopic" => {
            require(b, "topicName")?;
            require(b, "partitionCount")?;
            require(b, "replicationFactor")?;
        }
        "CreateVpcConnection" => {
            require(b, "targetClusterArn")?;
            require(b, "authentication")?;
            require(b, "vpcId")?;
            require(b, "clientSubnets")?;
            require(b, "securityGroups")?;
        }
        "BatchAssociateScramSecret" | "BatchDisassociateScramSecret" => {
            require(b, "secretArnList")?;
        }
        "PutClusterPolicy" => {
            require(b, "policy")?;
        }
        "RebootBroker" => {
            require(b, "brokerIds")?;
        }
        "RejectClientVpcConnection" => {
            require(b, "vpcConnectionArn")?;
        }
        "TagResource" => {
            require(b, "tags")?;
        }
        "UpdateBrokerCount" => {
            require(b, "currentVersion")?;
            require(b, "targetNumberOfBrokerNodes")?;
        }
        "UpdateBrokerStorage" => {
            require(b, "currentVersion")?;
            require(b, "targetBrokerEBSVolumeInfo")?;
        }
        "UpdateBrokerType" => {
            require(b, "currentVersion")?;
            require(b, "targetInstanceType")?;
        }
        "UpdateClusterConfiguration" => {
            require(b, "currentVersion")?;
            require(b, "configurationInfo")?;
        }
        "UpdateClusterKafkaVersion" => {
            require(b, "currentVersion")?;
            require(b, "targetKafkaVersion")?;
        }
        "UpdateMonitoring" => {
            require(b, "currentVersion")?;
            check_enum(b, "enhancedMonitoring", ENHANCED_MONITORING)?;
        }
        "UpdateConnectivity" | "UpdateSecurity" | "UpdateReplicationInfo" => {
            require(b, "currentVersion")?;
        }
        "UpdateRebalancing" => {
            require(b, "currentVersion")?;
            require(b, "rebalancing")?;
        }
        "UpdateStorage" => {
            require(b, "currentVersion")?;
            check_enum(b, "storageMode", STORAGE_MODE)?;
        }
        _ => {}
    }
    Ok(())
}

/// Validate the shared constrained `@httpQuery` parameters (page size + opaque
/// pagination token). Rejecting an out-of-range page size with
/// `BadRequestException` (a code every affected operation declares) keeps the
/// conformance suite's negative query-parameter variants on a declared error.
pub fn validate_query(q: &[(String, String)]) -> Result<(), AwsServiceError> {
    for (k, v) in q {
        // MaxResults carries `@range{min:1,max:100}` on every MSK list op.
        if k == "maxResults"
            && !v
                .parse::<i64>()
                .map(|n| (1..=100).contains(&n))
                .unwrap_or(false)
        {
            return Err(bad_request(&format!(
                "Value '{v}' at 'maxResults' failed to satisfy constraint: Member must be between 1 and 100"
            )));
        }
        // NextToken is an opaque offset the service itself minted (a decimal
        // string); a client that echoes a malformed token gets a
        // BadRequestException rather than silently being served page 1.
        if k == "nextToken" && v.parse::<usize>().is_err() {
            return Err(bad_request(&format!(
                "Value '{v}' at 'nextToken' is not a valid pagination token."
            )));
        }
    }
    Ok(())
}
