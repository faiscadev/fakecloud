//! Model-derived input validation for AWS X-Ray operations.
//!
//! Rejects requests that violate the Smithy contract (missing required members,
//! out-of-set enum values) with `InvalidRequestException` -- the error code
//! *every* X-Ray operation declares -- so the conformance suite's negative
//! variants land on a declared error rather than a routing miss or a 500. Body
//! member names are the restJson1 member names (PascalCase; X-Ray declares no
//! `jsonName` overrides).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

pub const ENCRYPTION_TYPE: &[&str] = &["NONE", "KMS"];
pub const TIME_RANGE_TYPE: &[&str] = &["TraceId", "Event", "Service"];
pub const TRACE_FORMAT_TYPE: &[&str] = &["XRAY", "OTEL"];
pub const INSIGHT_STATE: &[&str] = &["ACTIVE", "CLOSED"];
pub const TRACE_SEGMENT_DESTINATION: &[&str] = &["XRay", "CloudWatchLogs"];
pub const SAMPLING_STRATEGY_NAME: &[&str] = &["PartialScan", "FixedRate"];

/// The `SamplingRule` members X-Ray marks `@required`.
const SAMPLING_RULE_REQUIRED: &[&str] = &[
    "ResourceARN",
    "Priority",
    "FixedRate",
    "ReservoirSize",
    "ServiceName",
    "ServiceType",
    "Host",
    "HTTPMethod",
    "URLPath",
    "Version",
];

fn invalid(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", msg)
}

/// A single length constraint on a string member: `(member, min, max)`.
type LengthRule = (&'static str, usize, usize);

/// Per-operation length constraints on top-level string input members, taken
/// from the X-Ray Smithy model. Any present member whose string length falls
/// outside `[min, max]` is rejected with `InvalidRequestException`.
const LENGTH_CONSTRAINTS: &[(&str, &[LengthRule])] = &[
    ("CancelTraceRetrieval", &[("RetrievalToken", 0, 1020)]),
    ("CreateGroup", &[("GroupName", 1, 32)]),
    ("DeleteGroup", &[("GroupName", 1, 32), ("GroupARN", 1, 400)]),
    ("DeleteResourcePolicy", &[("PolicyName", 1, 128)]),
    ("GetGroup", &[("GroupName", 1, 32), ("GroupARN", 1, 400)]),
    ("GetGroups", &[("NextToken", 1, 100)]),
    ("GetInsightEvents", &[("NextToken", 1, 2000)]),
    ("GetInsightImpactGraph", &[("NextToken", 1, 2000)]),
    (
        "GetInsightSummaries",
        &[
            ("GroupARN", 1, 400),
            ("GroupName", 1, 32),
            ("NextToken", 1, 2000),
        ],
    ),
    ("GetRetrievedTracesGraph", &[("RetrievalToken", 0, 1020)]),
    (
        "GetServiceGraph",
        &[("GroupName", 1, 32), ("GroupARN", 1, 400)],
    ),
    (
        "GetTimeSeriesServiceStatistics",
        &[
            ("GroupName", 1, 32),
            ("GroupARN", 1, 400),
            ("EntitySelectorExpression", 1, 500),
        ],
    ),
    ("ListResourcePolicies", &[("NextToken", 1, 100)]),
    ("ListRetrievedTraces", &[("RetrievalToken", 0, 1020)]),
    ("ListTagsForResource", &[("ResourceARN", 1, 1011)]),
    ("PutEncryptionConfig", &[("KeyId", 1, 3000)]),
    ("PutResourcePolicy", &[("PolicyName", 1, 128)]),
    (
        "PutTelemetryRecords",
        &[
            ("EC2InstanceId", 0, 20),
            ("Hostname", 0, 255),
            ("ResourceARN", 0, 500),
        ],
    ),
    ("TagResource", &[("ResourceARN", 1, 1011)]),
    ("UntagResource", &[("ResourceARN", 1, 1011)]),
    ("UpdateGroup", &[("GroupName", 1, 32), ("GroupARN", 1, 400)]),
];

/// Reject an integer member outside its declared `[min, max]` range.
fn range_opt(b: &Value, field: &str, min: i64, max: i64) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field).and_then(Value::as_i64) {
        if v < min || v > max {
            return Err(invalid(&format!(
                "{field} value {v} is outside the allowed range [{min}, {max}]."
            )));
        }
    }
    Ok(())
}

fn validate_lengths(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    let Some((_, rules)) = LENGTH_CONSTRAINTS.iter().find(|(op, _)| *op == action) else {
        return Ok(());
    };
    for (field, min, max) in *rules {
        if let Some(s) = body.get(*field).and_then(Value::as_str) {
            let len = s.chars().count();
            if len < *min || len > *max {
                return Err(invalid(&format!(
                    "{field} length {len} is outside the allowed range [{min}, {max}]."
                )));
            }
        }
    }
    Ok(())
}

/// A present, non-null required member (an empty string still counts as present
/// for X-Ray's contract, matching the service's own laxer required handling).
fn require<'a>(b: &'a Value, field: &str) -> Result<&'a Value, AwsServiceError> {
    match b.get(field) {
        Some(Value::Null) | None => Err(invalid(&format!("{field} is required."))),
        Some(v) => Ok(v),
    }
}

/// An optional string member is one of `allowed` (case-sensitive), when present.
fn enum_opt(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field).and_then(Value::as_str) {
        if !allowed.contains(&v) {
            return Err(invalid(&format!(
                "{field} must be one of [{}], got '{v}'.",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

/// A required enum string member is present and one of `allowed`.
fn enum_req(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    require(b, field)?;
    enum_opt(b, field, allowed)
}

fn validate_sampling_rule(rule: &Value) -> Result<(), AwsServiceError> {
    if !rule.is_object() {
        return Err(invalid("SamplingRule must be an object."));
    }
    for field in SAMPLING_RULE_REQUIRED {
        require(rule, field)?;
    }
    Ok(())
}

/// Validate an operation's already-parsed JSON body against the model.
pub fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    validate_lengths(action, body)?;
    match action {
        "BatchGetTraces" | "GetTraceGraph" => {
            require(body, "TraceIds")?;
        }
        "CancelTraceRetrieval" | "GetRetrievedTracesGraph" => {
            require(body, "RetrievalToken")?;
        }
        "ListRetrievedTraces" => {
            require(body, "RetrievalToken")?;
            enum_opt(body, "TraceFormat", TRACE_FORMAT_TYPE)?;
        }
        "CreateGroup" => {
            require(body, "GroupName")?;
        }
        "CreateSamplingRule" => {
            let rule = require(body, "SamplingRule")?;
            validate_sampling_rule(rule)?;
        }
        "UpdateSamplingRule" => {
            require(body, "SamplingRuleUpdate")?;
        }
        "DeleteResourcePolicy" => {
            require(body, "PolicyName")?;
        }
        "PutResourcePolicy" => {
            require(body, "PolicyName")?;
            require(body, "PolicyDocument")?;
        }
        "GetInsight" => {
            require(body, "InsightId")?;
        }
        "GetInsightEvents" => {
            require(body, "InsightId")?;
            range_opt(body, "MaxResults", 1, 50)?;
        }
        "GetInsightImpactGraph" => {
            require(body, "InsightId")?;
            require(body, "StartTime")?;
            require(body, "EndTime")?;
        }
        "GetInsightSummaries" => {
            require(body, "StartTime")?;
            require(body, "EndTime")?;
            range_opt(body, "MaxResults", 1, 100)?;
            if let Some(states) = body.get("States").and_then(Value::as_array) {
                for state in states {
                    if let Some(s) = state.as_str() {
                        if !INSIGHT_STATE.contains(&s) {
                            return Err(invalid(&format!(
                                "States entries must be one of [{}], got '{s}'.",
                                INSIGHT_STATE.join(", ")
                            )));
                        }
                    }
                }
            }
        }
        "GetServiceGraph" | "GetTimeSeriesServiceStatistics" => {
            require(body, "StartTime")?;
            require(body, "EndTime")?;
        }
        "GetTraceSummaries" => {
            require(body, "StartTime")?;
            require(body, "EndTime")?;
            enum_opt(body, "TimeRangeType", TIME_RANGE_TYPE)?;
            if let Some(strategy) = body.get("SamplingStrategy") {
                enum_opt(strategy, "Name", SAMPLING_STRATEGY_NAME)?;
            }
        }
        "GetSamplingTargets" => {
            require(body, "SamplingStatisticsDocuments")?;
        }
        "StartTraceRetrieval" => {
            require(body, "TraceIds")?;
            require(body, "StartTime")?;
            require(body, "EndTime")?;
        }
        "ListTagsForResource" => {
            require(body, "ResourceARN")?;
        }
        "TagResource" => {
            require(body, "ResourceARN")?;
            require(body, "Tags")?;
        }
        "UntagResource" => {
            require(body, "ResourceARN")?;
            require(body, "TagKeys")?;
        }
        "PutEncryptionConfig" => {
            enum_req(body, "Type", ENCRYPTION_TYPE)?;
        }
        "PutTelemetryRecords" => {
            require(body, "TelemetryRecords")?;
        }
        "PutTraceSegments" => {
            require(body, "TraceSegmentDocuments")?;
        }
        "UpdateIndexingRule" => {
            require(body, "Name")?;
            require(body, "Rule")?;
        }
        "UpdateTraceSegmentDestination" => {
            enum_req(body, "Destination", TRACE_SEGMENT_DESTINATION)?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn put_trace_segments_requires_documents() {
        assert!(validate_input("PutTraceSegments", &json!({})).is_err());
        assert!(
            validate_input("PutTraceSegments", &json!({ "TraceSegmentDocuments": [] })).is_ok()
        );
    }

    #[test]
    fn get_service_graph_requires_time_range() {
        assert!(validate_input("GetServiceGraph", &json!({ "StartTime": 1 })).is_err());
        assert!(
            validate_input("GetServiceGraph", &json!({ "StartTime": 1, "EndTime": 2 })).is_ok()
        );
    }

    #[test]
    fn put_encryption_config_requires_valid_type() {
        assert!(validate_input("PutEncryptionConfig", &json!({})).is_err());
        assert!(validate_input("PutEncryptionConfig", &json!({ "Type": "BOGUS" })).is_err());
        assert!(validate_input("PutEncryptionConfig", &json!({ "Type": "NONE" })).is_ok());
    }

    #[test]
    fn create_sampling_rule_requires_nested_members() {
        assert!(validate_input(
            "CreateSamplingRule",
            &json!({ "SamplingRule": { "ResourceARN": "*" } })
        )
        .is_err());
        let full = json!({ "SamplingRule": {
            "ResourceARN": "*", "Priority": 1, "FixedRate": 0.1, "ReservoirSize": 1,
            "ServiceName": "*", "ServiceType": "*", "Host": "*", "HTTPMethod": "*",
            "URLPath": "*", "Version": 1
        }});
        assert!(validate_input("CreateSamplingRule", &full).is_ok());
    }

    #[test]
    fn tag_resource_requires_arn_and_tags() {
        assert!(validate_input("TagResource", &json!({ "ResourceARN": "a" })).is_err());
        assert!(validate_input("TagResource", &json!({ "ResourceARN": "a", "Tags": [] })).is_ok());
    }
}
