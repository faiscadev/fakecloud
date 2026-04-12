//! Shared helpers for AWS Query protocol services (SQS, SNS, ElastiCache, RDS, SES v1, IAM).

use http::StatusCode;

use crate::service::{AwsRequest, AwsServiceError};

/// Wrap an action result in the standard AWS Query protocol XML envelope.
///
/// Produces the canonical response shape:
/// ```xml
/// <?xml version="1.0" encoding="UTF-8"?>
/// <{Action}Response xmlns="{namespace}">
///   <{Action}Result>{inner}</{Action}Result>
///   <ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata>
/// </{Action}Response>
/// ```
pub fn query_response_xml(action: &str, namespace: &str, inner: &str, request_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <{action}Response xmlns=\"{namespace}\">\
         <{action}Result>{inner}</{action}Result>\
         <ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata>\
         </{action}Response>"
    )
}

/// Produce a Query protocol XML response with only metadata (no result body).
pub fn query_metadata_only_xml(action: &str, namespace: &str, request_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <{action}Response xmlns=\"{namespace}\">\
         <ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata>\
         </{action}Response>"
    )
}

/// Extract an optional query parameter from an `AwsRequest`.
///
/// Returns `None` if the parameter is missing or empty.
pub fn optional_query_param(req: &AwsRequest, name: &str) -> Option<String> {
    req.query_params
        .get(name)
        .cloned()
        .filter(|value| !value.is_empty())
}

/// Extract a required query parameter from an `AwsRequest`.
///
/// Returns `MissingParameter` error if the parameter is missing or empty.
pub fn required_query_param(req: &AwsRequest, name: &str) -> Result<String, AwsServiceError> {
    optional_query_param(req, name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MissingParameter",
            format!("The request must contain the parameter {name}."),
        )
    })
}
