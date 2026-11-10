//! CreateEventSourceMapping's event source checks: the ARN's shape, its
//! region, its service, and whether the source exists.
//!
//! Lambda reaches into SQS, Kinesis and DynamoDB state through
//! [`EventSourceLookup`], which the server installs (the Lambda crate does
//! not depend on those services). Without one -- a bare `LambdaService` in
//! unit tests -- existence is not checked.
//!
//! The refusals are AWS's, recorded against Lambda in us-east-1
//! (2026-11-10, `tests/fixtures/create-event-source-mapping-missing-source.json`).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

/// The `Arn` shape's `@pattern`, quoted in its ValidationException.
const EVENT_SOURCE_ARN_PATTERN: &str = "arn:(aws[a-zA-Z0-9-]*):([a-zA-Z0-9\\-])+:((eusc-)?[a-z]{2}((-gov)|(-iso([a-z]?)))?-[a-z]+-\\d{1})?:(\\d{12})?:(.*)";

/// Whether an event source exists, looked up in its own service's state
/// (the account and region its ARN names).
pub trait EventSourceLookup: Send + Sync {
    /// `Some(exists)` for a source this lookup knows the service of (SQS
    /// queues, Kinesis streams, DynamoDB streams), `None` otherwise.
    fn exists(&self, arn: &str) -> Option<bool>;
}

/// The event source services a mapping can read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceKind {
    Sqs,
    Kinesis,
    DynamoDbStream,
    /// MSK, Amazon MQ, DocumentDB: accepted, existence not looked up.
    Other,
}

fn source_kind(service: &str) -> Option<SourceKind> {
    match service {
        "sqs" => Some(SourceKind::Sqs),
        "kinesis" => Some(SourceKind::Kinesis),
        "dynamodb" => Some(SourceKind::DynamoDbStream),
        "kafka" | "mq" | "rds" => Some(SourceKind::Other),
        _ => None,
    }
}

fn invalid_parameter(message: String) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        message,
    )
}

/// The `(service, region)` of `arn` when it satisfies the `Arn` pattern.
fn parse(arn: &str) -> Option<(&str, &str)> {
    let mut parts = arn.splitn(6, ':');
    let (prefix, partition, service, region, account) = (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    );
    // The resource (`(.*)`) may be empty but its separator may not.
    parts.next()?;
    let partition_ok = partition
        .strip_prefix("aws")
        .is_some_and(|rest| rest.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'));
    let service_ok = !service.is_empty()
        && service
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-');
    let region_ok = region.is_empty() || crate::extras::is_lambda_arn_region(region);
    let account_ok =
        account.is_empty() || (account.len() == 12 && account.bytes().all(|b| b.is_ascii_digit()));
    (prefix == "arn" && partition_ok && service_ok && region_ok && account_ok)
        .then_some((service, region))
}

/// Check `arn` as CreateEventSourceMapping's `EventSourceArn` for a mapping
/// in `region`: its shape (ValidationException), its region and service,
/// and, through `lookup`, that the source exists (each
/// InvalidParameterValueException, worded as AWS words it).
pub(crate) fn validate(
    arn: &str,
    region: &str,
    lookup: Option<&dyn EventSourceLookup>,
) -> Result<(), AwsServiceError> {
    let Some((service, source_region)) = parse(arn) else {
        return Err(crate::extras::constraint_violation(
            arn,
            "eventSourceArn",
            &format!("Member must satisfy regular expression pattern: {EVENT_SOURCE_ARN_PATTERN}"),
        ));
    };
    if !source_region.is_empty() && source_region != region {
        return Err(invalid_parameter(format!(
            "Event source region must match Lambda region {region}"
        )));
    }
    let Some(kind) = source_kind(service) else {
        return Err(invalid_parameter(format!(
            "Unrecognized event source. Unsupported source arn : {arn}"
        )));
    };
    if lookup.and_then(|l| l.exists(arn)) == Some(false) {
        return Err(invalid_parameter(match kind {
            SourceKind::Sqs => "Error occurred while ReceiveMessage. SQS Error Code: \
                                AWS.SimpleQueueService.NonExistentQueue. SQS Error Message: \
                                The specified queue does not exist."
                .to_string(),
            SourceKind::Kinesis | SourceKind::DynamoDbStream | SourceKind::Other => {
                format!("Stream not found: {arn}")
            }
        }));
    }
    Ok(())
}

/// The settings CreateEventSourceMapping and UpdateEventSourceMapping both
/// take, against their `@length` / `@range` / enum constraints.
pub(crate) fn validate_settings(body: &Value) -> Result<(), AwsServiceError> {
    use crate::service::{check_optional_enum, check_optional_int_range};
    crate::extras::check_optional_len("FunctionName", body["FunctionName"].as_str(), 1, 256)?;
    crate::extras::check_optional_len("KMSKeyArn", body["KMSKeyArn"].as_str(), 0, 10000)?;
    // Only CreateEventSourceMapping takes `StartingPosition`.
    check_optional_enum(
        "StartingPosition",
        body["StartingPosition"].as_str(),
        &["TRIM_HORIZON", "LATEST", "AT_TIMESTAMP"],
    )?;
    for (field, min, max) in [
        ("BatchSize", 1, 10000),
        ("MaximumBatchingWindowInSeconds", 0, 300),
        ("ParallelizationFactor", 1, 10),
        ("MaximumRecordAgeInSeconds", -1, 604800),
        ("MaximumRetryAttempts", -1, 10000),
        ("TumblingWindowInSeconds", 0, 900),
    ] {
        check_optional_int_range(field, body[field].as_i64(), min, max)?;
    }
    Ok(())
}

/// Every check CreateEventSourceMapping makes of its request before
/// looking at the function: the settings, the function reference, the event
/// source ([`validate`]) and the filter patterns, which it returns. One path
/// for the API and for CloudFormation's AWS::Lambda::EventSourceMapping, so
/// a stack is refused exactly what a direct call is.
pub fn validate_create(
    body: &Value,
    region: &str,
    lookup: Option<&dyn EventSourceLookup>,
) -> Result<Vec<String>, AwsServiceError> {
    validate_settings(body)?;
    match body["EventSourceArn"].as_str() {
        Some(event_source_arn) => {
            crate::extras::check_len("EventSourceArn", event_source_arn, 0, 10000)?;
            validate(event_source_arn, region, lookup)?;
        }
        // A self-managed Kafka cluster is named by its bootstrap servers,
        // not an ARN.
        None if body["SelfManagedEventSource"].is_object() => {}
        None => return Err(unrecognized()),
    }
    let function_name = body["FunctionName"]
        .as_str()
        .ok_or_else(|| invalid_parameter("FunctionName is required".to_string()))?;
    // An event source mapping invokes a function of its own region; a
    // function ARN from another region is refused like on AWS.
    if let Some(function_region) = crate::service::function_ref_scope(function_name).1 {
        if function_region != region {
            return Err(invalid_parameter(format!(
                "Functions from '{function_region}' are not reachable in this region ('{region}')"
            )));
        }
    }
    filter_patterns(body)
}

/// `FilterCriteria.Filters[].Pattern`, strictly: a missing or non-string
/// pattern, a non-array `Filters`, a non-object `FilterCriteria` and a
/// pattern the filter grammar refuses are each an error, as on AWS, rather
/// than silently dropped.
fn filter_patterns(body: &Value) -> Result<Vec<String>, AwsServiceError> {
    let patterns: Vec<String> = match body.get("FilterCriteria") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Object(criteria)) => match criteria.get("Filters") {
            None => Vec::new(),
            Some(Value::Array(arr)) => arr
                .iter()
                .map(|f| match f.get("Pattern") {
                    Some(Value::String(s)) => Ok(s.clone()),
                    _ => Err(invalid_parameter(
                        "FilterCriteria.Filters[].Pattern must be a string".to_string(),
                    )),
                })
                .collect::<Result<_, _>>()?,
            Some(_) => {
                return Err(invalid_parameter(
                    "FilterCriteria.Filters must be an array".to_string(),
                ))
            }
        },
        Some(_) => {
            return Err(invalid_parameter(
                "FilterCriteria must be an object".to_string(),
            ))
        }
    };
    crate::filter::FilterSet::validate(patterns.iter()).map_err(invalid_parameter)?;
    Ok(patterns)
}

/// The refusal for a mapping with no event source at all.
pub(crate) fn unrecognized() -> AwsServiceError {
    invalid_parameter("Unrecognized event source.".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Known(&'static [&'static str]);

    impl EventSourceLookup for Known {
        fn exists(&self, arn: &str) -> Option<bool> {
            let service = arn.split(':').nth(2)?;
            matches!(service, "sqs" | "kinesis" | "dynamodb").then(|| self.0.contains(&arn))
        }
    }

    #[derive(serde::Deserialize)]
    struct Recording {
        account: String,
        region: String,
        cases: Vec<Case>,
    }

    #[derive(serde::Deserialize)]
    struct Case {
        event_source_arn: Option<String>,
        code: String,
        message: String,
    }

    /// The refusals match the recorded ones, account substituted.
    #[test]
    fn refusals_match_the_aws_recording() {
        let recording: Recording = serde_json::from_str(include_str!(
            "../tests/fixtures/create-event-source-mapping-missing-source.json"
        ))
        .unwrap();
        let lookup = Known(&[]);
        for case in recording.cases {
            let err = match case.event_source_arn.as_deref() {
                Some(arn) => validate(arn, &recording.region, Some(&lookup))
                    .expect_err(&format!("{arn} was accepted")),
                None => unrecognized(),
            };
            assert_eq!(err.code(), case.code, "{:?}", case.event_source_arn);
            assert_eq!(err.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                err.message(),
                case.message,
                "{:?} ({})",
                case.event_source_arn,
                recording.account
            );
        }
    }

    #[test]
    fn existing_sources_and_sources_nothing_looks_up_are_accepted() {
        const QUEUE: &str = "arn:aws:sqs:us-east-1:123456789012:q";
        let queue = QUEUE;
        let lookup = Known(&[QUEUE]);
        assert!(validate(queue, "us-east-1", Some(&lookup)).is_ok());
        // No lookup installed: the shape is still checked, existence is not.
        assert!(validate(
            "arn:aws:sqs:us-east-1:123456789012:other",
            "us-east-1",
            None
        )
        .is_ok());
        assert!(validate("", "us-east-1", None).is_err());
        // MSK and Amazon MQ sources are not looked up.
        assert!(validate(
            "arn:aws:kafka:us-east-1:123456789012:cluster/c/abc",
            "us-east-1",
            Some(&lookup)
        )
        .is_ok());
        // A source in another account of the same region is looked up there.
        assert!(validate(
            "arn:aws:sqs:us-east-1:210987654321:q",
            "us-east-1",
            Some(&lookup)
        )
        .is_err());
    }
}
