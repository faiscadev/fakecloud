//! Model-derived input validation for CloudTrail operations.
//!
//! Each operation's constrained top-level input members (string `@length`,
//! integer `@range`, and `enum` value sets) are validated against the AWS
//! Smithy model. A violation yields an `InvalidParameterException`, matching
//! the real service's client-side validation. Members absent from the request
//! are skipped (required-field presence is enforced by the handlers).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

enum RuleKind {
    Str {
        min: Option<usize>,
        max: Option<usize>,
    },
    Int {
        min: Option<i64>,
        max: Option<i64>,
    },
    Enum(&'static [&'static str]),
}

struct Rule {
    field: &'static str,
    kind: RuleKind,
}

impl Rule {
    const fn str_(field: &'static str, min: Option<usize>, max: Option<usize>) -> Rule {
        Rule {
            field,
            kind: RuleKind::Str { min, max },
        }
    }
    const fn int_(field: &'static str, min: Option<i64>, max: Option<i64>) -> Rule {
        Rule {
            field,
            kind: RuleKind::Int { min, max },
        }
    }
    const fn enum_(field: &'static str, values: &'static [&'static str]) -> Rule {
        Rule {
            field,
            kind: RuleKind::Enum(values),
        }
    }
}

fn invalid(msg: String) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterException", &msg)
}

/// Validate the constrained top-level members of `body` for `action`.
pub(crate) fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    for rule in rules_for(action) {
        let Some(v) = body.get(rule.field) else {
            continue;
        };
        match &rule.kind {
            RuleKind::Str { min, max } => {
                if let Some(s) = v.as_str() {
                    let len = s.chars().count();
                    if let Some(min) = min {
                        if len < *min {
                            return Err(invalid(format!(
                                "{} must have length greater than or equal to {}.",
                                rule.field, min
                            )));
                        }
                    }
                    if let Some(max) = max {
                        if len > *max {
                            return Err(invalid(format!(
                                "{} must have length less than or equal to {}.",
                                rule.field, max
                            )));
                        }
                    }
                }
            }
            RuleKind::Int { min, max } => {
                if let Some(n) = v.as_i64() {
                    if let Some(min) = min {
                        if n < *min {
                            return Err(invalid(format!(
                                "{} must be greater than or equal to {}.",
                                rule.field, min
                            )));
                        }
                    }
                    if let Some(max) = max {
                        if n > *max {
                            return Err(invalid(format!(
                                "{} must be less than or equal to {}.",
                                rule.field, max
                            )));
                        }
                    }
                }
            }
            RuleKind::Enum(values) => {
                if let Some(s) = v.as_str() {
                    if !values.contains(&s) {
                        return Err(invalid(format!(
                            "{} '{}' is not a valid value. Valid values are: {}.",
                            rule.field,
                            s,
                            values.join(", ")
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Per-operation constrained-member rules, derived from the Smithy model.
fn rules_for(action: &str) -> &'static [Rule] {
    match action {
        "CancelQuery" => {
            const R: &[Rule] = &[
                Rule::str_("EventDataStore", Some(3), Some(256)),
                Rule::str_("QueryId", Some(36), Some(36)),
                Rule::str_("EventDataStoreOwnerAccountId", Some(12), Some(16)),
            ];
            R
        }
        "CreateChannel" => {
            const R: &[Rule] = &[
                Rule::str_("Name", Some(3), Some(128)),
                Rule::str_("Source", Some(1), Some(256)),
            ];
            R
        }
        "CreateDashboard" => {
            const R: &[Rule] = &[Rule::str_("Name", Some(3), Some(128))];
            R
        }
        "CreateEventDataStore" => {
            const R: &[Rule] = &[
                Rule::str_("Name", Some(3), Some(128)),
                Rule::int_("RetentionPeriod", Some(7), Some(3653)),
                Rule::str_("KmsKeyId", Some(1), Some(350)),
                Rule::enum_(
                    "BillingMode",
                    &["EXTENDABLE_RETENTION_PRICING", "FIXED_RETENTION_PRICING"],
                ),
            ];
            R
        }
        "DeleteChannel" => {
            const R: &[Rule] = &[Rule::str_("Channel", Some(3), Some(256))];
            R
        }
        "DeleteEventDataStore" => {
            const R: &[Rule] = &[Rule::str_("EventDataStore", Some(3), Some(256))];
            R
        }
        "DeleteResourcePolicy" => {
            const R: &[Rule] = &[Rule::str_("ResourceArn", Some(3), Some(256))];
            R
        }
        "DeregisterOrganizationDelegatedAdmin" => {
            const R: &[Rule] = &[Rule::str_("DelegatedAdminAccountId", Some(12), Some(16))];
            R
        }
        "DescribeQuery" => {
            const R: &[Rule] = &[
                Rule::str_("EventDataStore", Some(3), Some(256)),
                Rule::str_("QueryId", Some(36), Some(36)),
                Rule::str_("QueryAlias", Some(1), Some(256)),
                Rule::str_("RefreshId", Some(10), Some(20)),
                Rule::str_("EventDataStoreOwnerAccountId", Some(12), Some(16)),
            ];
            R
        }
        "DisableFederation" => {
            const R: &[Rule] = &[Rule::str_("EventDataStore", Some(3), Some(256))];
            R
        }
        "EnableFederation" => {
            const R: &[Rule] = &[
                Rule::str_("EventDataStore", Some(3), Some(256)),
                Rule::str_("FederationRoleArn", Some(3), Some(125)),
            ];
            R
        }
        "GenerateQuery" => {
            const R: &[Rule] = &[Rule::str_("Prompt", Some(3), Some(500))];
            R
        }
        "GetChannel" => {
            const R: &[Rule] = &[Rule::str_("Channel", Some(3), Some(256))];
            R
        }
        "GetEventDataStore" => {
            const R: &[Rule] = &[Rule::str_("EventDataStore", Some(3), Some(256))];
            R
        }
        "GetImport" => {
            const R: &[Rule] = &[Rule::str_("ImportId", Some(36), Some(36))];
            R
        }
        "GetInsightSelectors" => {
            const R: &[Rule] = &[Rule::str_("EventDataStore", Some(3), Some(256))];
            R
        }
        "GetQueryResults" => {
            const R: &[Rule] = &[
                Rule::str_("EventDataStore", Some(3), Some(256)),
                Rule::str_("QueryId", Some(36), Some(36)),
                Rule::str_("NextToken", Some(4), Some(1000)),
                Rule::int_("MaxQueryResults", Some(1), Some(1000)),
                Rule::str_("EventDataStoreOwnerAccountId", Some(12), Some(16)),
            ];
            R
        }
        "GetResourcePolicy" => {
            const R: &[Rule] = &[Rule::str_("ResourceArn", Some(3), Some(256))];
            R
        }
        "ListChannels" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(4), Some(1000)),
            ];
            R
        }
        "ListDashboards" => {
            const R: &[Rule] = &[
                Rule::str_("NamePrefix", Some(3), Some(128)),
                Rule::enum_("Type", &["MANAGED", "CUSTOM"]),
                Rule::str_("NextToken", Some(4), Some(1000)),
                Rule::int_("MaxResults", Some(1), Some(1000)),
            ];
            R
        }
        "ListEventDataStores" => {
            const R: &[Rule] = &[
                Rule::str_("NextToken", Some(4), Some(1000)),
                Rule::int_("MaxResults", Some(1), Some(1000)),
            ];
            R
        }
        "ListImportFailures" => {
            const R: &[Rule] = &[
                Rule::str_("ImportId", Some(36), Some(36)),
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(4), Some(1000)),
            ];
            R
        }
        "ListImports" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("Destination", Some(3), Some(256)),
                Rule::enum_(
                    "ImportStatus",
                    &[
                        "INITIALIZING",
                        "IN_PROGRESS",
                        "FAILED",
                        "STOPPED",
                        "COMPLETED",
                    ],
                ),
                Rule::str_("NextToken", Some(4), Some(1000)),
            ];
            R
        }
        "ListInsightsData" => {
            const R: &[Rule] = &[
                Rule::str_("InsightSource", Some(3), Some(256)),
                Rule::enum_("DataType", &["InsightsEvents"]),
                Rule::int_("MaxResults", Some(1), Some(50)),
                Rule::str_("NextToken", Some(4), Some(1000)),
            ];
            R
        }
        "ListInsightsMetricData" => {
            const R: &[Rule] = &[
                Rule::str_("EventSource", Some(0), Some(256)),
                Rule::str_("EventName", Some(0), Some(128)),
                Rule::enum_(
                    "InsightType",
                    &["ApiCallRateInsight", "ApiErrorRateInsight"],
                ),
                Rule::str_("ErrorCode", Some(0), Some(128)),
                Rule::int_("Period", Some(60), Some(3600)),
                Rule::enum_("DataType", &["FillWithZeros", "NonZeroData"]),
                Rule::int_("MaxResults", Some(1), Some(21600)),
                Rule::str_("NextToken", Some(1), Some(5000)),
            ];
            R
        }
        "ListQueries" => {
            const R: &[Rule] = &[
                Rule::str_("EventDataStore", Some(3), Some(256)),
                Rule::str_("NextToken", Some(4), Some(1000)),
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::enum_(
                    "QueryStatus",
                    &[
                        "QUEUED",
                        "RUNNING",
                        "FINISHED",
                        "FAILED",
                        "CANCELLED",
                        "TIMED_OUT",
                    ],
                ),
            ];
            R
        }
        "LookupEvents" => {
            const R: &[Rule] = &[
                Rule::enum_("EventCategory", &["insight"]),
                Rule::int_("MaxResults", Some(1), Some(50)),
            ];
            R
        }
        "PutEventConfiguration" => {
            const R: &[Rule] = &[Rule::enum_("MaxEventSize", &["Standard", "Large"])];
            R
        }
        "PutInsightSelectors" => {
            const R: &[Rule] = &[
                Rule::str_("EventDataStore", Some(3), Some(256)),
                Rule::str_("InsightsDestination", Some(3), Some(256)),
            ];
            R
        }
        "PutResourcePolicy" => {
            const R: &[Rule] = &[
                Rule::str_("ResourceArn", Some(3), Some(256)),
                Rule::str_("ResourcePolicy", Some(1), Some(8192)),
            ];
            R
        }
        "RegisterOrganizationDelegatedAdmin" => {
            const R: &[Rule] = &[Rule::str_("MemberAccountId", Some(12), Some(16))];
            R
        }
        "RestoreEventDataStore" => {
            const R: &[Rule] = &[Rule::str_("EventDataStore", Some(3), Some(256))];
            R
        }
        "SearchSampleQueries" => {
            const R: &[Rule] = &[
                Rule::str_("SearchPhrase", Some(2), Some(1000)),
                Rule::int_("MaxResults", Some(1), Some(50)),
                Rule::str_("NextToken", Some(4), Some(1000)),
            ];
            R
        }
        "StartEventDataStoreIngestion" => {
            const R: &[Rule] = &[Rule::str_("EventDataStore", Some(3), Some(256))];
            R
        }
        "StartImport" => {
            const R: &[Rule] = &[Rule::str_("ImportId", Some(36), Some(36))];
            R
        }
        "StartQuery" => {
            const R: &[Rule] = &[
                Rule::str_("QueryStatement", Some(1), Some(10000)),
                Rule::str_("DeliveryS3Uri", Some(0), Some(1024)),
                Rule::str_("QueryAlias", Some(1), Some(256)),
                Rule::str_("EventDataStoreOwnerAccountId", Some(12), Some(16)),
            ];
            R
        }
        "StopEventDataStoreIngestion" => {
            const R: &[Rule] = &[Rule::str_("EventDataStore", Some(3), Some(256))];
            R
        }
        "StopImport" => {
            const R: &[Rule] = &[Rule::str_("ImportId", Some(36), Some(36))];
            R
        }
        "UpdateChannel" => {
            const R: &[Rule] = &[
                Rule::str_("Channel", Some(3), Some(256)),
                Rule::str_("Name", Some(3), Some(128)),
            ];
            R
        }
        "UpdateEventDataStore" => {
            const R: &[Rule] = &[
                Rule::str_("EventDataStore", Some(3), Some(256)),
                Rule::str_("Name", Some(3), Some(128)),
                Rule::int_("RetentionPeriod", Some(7), Some(3653)),
                Rule::str_("KmsKeyId", Some(1), Some(350)),
                Rule::enum_(
                    "BillingMode",
                    &["EXTENDABLE_RETENTION_PRICING", "FIXED_RETENTION_PRICING"],
                ),
            ];
            R
        }
        _ => &[],
    }
}
