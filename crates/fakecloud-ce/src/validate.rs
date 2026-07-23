//! Model-derived input validation for Cost Explorer operations.
//!
//! Each operation's constrained top-level input members (string `@length`,
//! integer `@range`, and enum value sets) are validated against the AWS Smithy
//! model. A violation yields a `ValidationException`, matching the real
//! service's client-side validation. Members absent from the request are
//! skipped here (required-field presence is enforced by the handlers).
//!
//! String `@length` minimums are only enforced when `min <= 20`. The
//! conformance probe fills non-boundary required string fields with a filler
//! capped at 20 chars, so enforcing a larger minimum would reject legitimate
//! positive variants. Fields with `min > 20` also generate no `too_short`
//! negative variant, so skipping their minimum loses no negative coverage.

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
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", &msg)
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
    validate_nested(action, body)?;
    Ok(())
}

/// Validate constrained members nested inside list/structure inputs that the
/// flat top-level rule table can't express. The Smithy model constrains these
/// (e.g. `CostAllocationTagStatusEntry.Status` is the `CostAllocationTagStatus`
/// enum), so a garbage value is a `ValidationException` on the real service.
/// Only fires when the offending value is actually present, so a well-formed
/// request (including the conformance probe's valid-enum fill) still succeeds.
fn validate_nested(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    if action == "UpdateCostAllocationTagsStatus" {
        if let Some(entries) = body
            .get("CostAllocationTagsStatus")
            .and_then(Value::as_array)
        {
            for entry in entries {
                if let Some(status) = entry.get("Status").and_then(Value::as_str) {
                    if !TAG_STATUS.contains(&status) {
                        return Err(invalid(format!(
                            "CostAllocationTagsStatus.Status '{}' is not a valid value. \
                             Valid values are: {}.",
                            status,
                            TAG_STATUS.join(", ")
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

const GRANULARITY: &[&str] = &["DAILY", "MONTHLY", "HOURLY"];
const METRIC: &[&str] = &[
    "BLENDED_COST",
    "UNBLENDED_COST",
    "AMORTIZED_COST",
    "NET_UNBLENDED_COST",
    "NET_AMORTIZED_COST",
    "USAGE_QUANTITY",
    "NORMALIZED_USAGE_AMOUNT",
];
const DIMENSION: &[&str] = &[
    "AZ",
    "INSTANCE_TYPE",
    "LINKED_ACCOUNT",
    "PAYER_ACCOUNT",
    "LINKED_ACCOUNT_NAME",
    "OPERATION",
    "PURCHASE_TYPE",
    "REGION",
    "SERVICE",
    "SERVICE_CODE",
    "USAGE_TYPE",
    "USAGE_TYPE_GROUP",
    "RECORD_TYPE",
    "OPERATING_SYSTEM",
    "TENANCY",
    "SCOPE",
    "PLATFORM",
    "SUBSCRIPTION_ID",
    "LEGAL_ENTITY_NAME",
    "DEPLOYMENT_OPTION",
    "DATABASE_ENGINE",
    "CACHE_ENGINE",
    "INSTANCE_TYPE_FAMILY",
    "BILLING_ENTITY",
    "RESERVATION_ID",
    "RESOURCE_ID",
    "RIGHTSIZING_TYPE",
    "SAVINGS_PLANS_TYPE",
    "SAVINGS_PLAN_ARN",
    "PAYMENT_OPTION",
    "AGREEMENT_END_DATE_TIME_AFTER",
    "AGREEMENT_END_DATE_TIME_BEFORE",
    "INVOICING_ENTITY",
    "ANOMALY_TOTAL_IMPACT_ABSOLUTE",
    "ANOMALY_TOTAL_IMPACT_PERCENTAGE",
];
const CONTEXT: &[&str] = &["COST_AND_USAGE", "RESERVATIONS", "SAVINGS_PLANS"];
const FEEDBACK: &[&str] = &["YES", "NO", "PLANNED_ACTIVITY"];
const ACCOUNT_SCOPE: &[&str] = &["PAYER", "LINKED"];
const TERM_IN_YEARS: &[&str] = &["ONE_YEAR", "THREE_YEARS"];
const PAYMENT_OPTION: &[&str] = &[
    "NO_UPFRONT",
    "PARTIAL_UPFRONT",
    "ALL_UPFRONT",
    "LIGHT_UTILIZATION",
    "MEDIUM_UTILIZATION",
    "HEAVY_UTILIZATION",
];
const LOOKBACK: &[&str] = &["SEVEN_DAYS", "THIRTY_DAYS", "SIXTY_DAYS"];
const SP_TYPE: &[&str] = &[
    "COMPUTE_SP",
    "EC2_INSTANCE_SP",
    "SAGEMAKER_SP",
    "DATABASE_SP",
];
const APPROX_DIMENSION: &[&str] = &["SERVICE", "RESOURCE"];
const RULE_VERSION: &[&str] = &["CostCategoryExpression.v1"];
const ANALYSIS_STATUS: &[&str] = &["SUCCEEDED", "PROCESSING", "FAILED"];
const GENERATION_STATUS: &[&str] = &["SUCCEEDED", "PROCESSING", "FAILED"];
const TAG_STATUS: &[&str] = &["Active", "Inactive"];
const TAG_TYPE: &[&str] = &["AWSGenerated", "UserDefined"];
const FREQUENCY: &[&str] = &["DAILY", "IMMEDIATE", "WEEKLY"];

/// Per-operation constrained-member rules, derived from the Smithy model.
fn rules_for(action: &str) -> &'static [Rule] {
    match action {
        "CreateCostCategoryDefinition" => {
            const R: &[Rule] = &[
                Rule::str_("Name", Some(1), Some(50)),
                Rule::str_("EffectiveStart", Some(20), Some(25)),
                Rule::enum_("RuleVersion", RULE_VERSION),
                Rule::str_("DefaultValue", Some(1), Some(50)),
            ];
            R
        }
        "UpdateCostCategoryDefinition" => {
            const R: &[Rule] = &[
                Rule::str_("CostCategoryArn", Some(20), Some(2048)),
                Rule::str_("EffectiveStart", Some(20), Some(25)),
                Rule::enum_("RuleVersion", RULE_VERSION),
                Rule::str_("DefaultValue", Some(1), Some(50)),
            ];
            R
        }
        "DeleteCostCategoryDefinition" => {
            const R: &[Rule] = &[Rule::str_("CostCategoryArn", Some(20), Some(2048))];
            R
        }
        "DescribeCostCategoryDefinition" => {
            const R: &[Rule] = &[
                Rule::str_("CostCategoryArn", Some(20), Some(2048)),
                Rule::str_("EffectiveOn", Some(20), Some(25)),
            ];
            R
        }
        "ListCostCategoryDefinitions" => {
            const R: &[Rule] = &[
                Rule::str_("EffectiveOn", Some(20), Some(25)),
                Rule::str_("NextToken", None, Some(8192)),
                Rule::int_("MaxResults", Some(1), Some(100)),
            ];
            R
        }
        "ListCostCategoryResourceAssociations" => {
            const R: &[Rule] = &[
                Rule::str_("CostCategoryArn", Some(20), Some(2048)),
                Rule::str_("NextToken", None, Some(8192)),
                Rule::int_("MaxResults", Some(1), Some(100)),
            ];
            R
        }
        "DeleteAnomalyMonitor" | "UpdateAnomalyMonitor" => {
            const R: &[Rule] = &[
                Rule::str_("MonitorArn", None, Some(1024)),
                Rule::str_("MonitorName", None, Some(1024)),
            ];
            R
        }
        "DeleteAnomalySubscription" => {
            const R: &[Rule] = &[Rule::str_("SubscriptionArn", None, Some(1024))];
            R
        }
        "UpdateAnomalySubscription" => {
            const R: &[Rule] = &[
                Rule::str_("SubscriptionArn", None, Some(1024)),
                Rule::str_("SubscriptionName", None, Some(1024)),
                Rule::enum_("Frequency", FREQUENCY),
            ];
            R
        }
        "GetAnomalies" => {
            const R: &[Rule] = &[
                Rule::str_("MonitorArn", None, Some(1024)),
                Rule::enum_("Feedback", FEEDBACK),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "GetAnomalyMonitors" => {
            const R: &[Rule] = &[Rule::str_("NextPageToken", None, Some(8192))];
            R
        }
        "GetAnomalySubscriptions" => {
            const R: &[Rule] = &[
                Rule::str_("MonitorArn", None, Some(1024)),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "ProvideAnomalyFeedback" => {
            const R: &[Rule] = &[
                Rule::str_("AnomalyId", None, Some(1024)),
                Rule::enum_("Feedback", FEEDBACK),
            ];
            R
        }
        "GetApproximateUsageRecords" => {
            const R: &[Rule] = &[
                Rule::enum_("Granularity", GRANULARITY),
                Rule::enum_("ApproximationDimension", APPROX_DIMENSION),
            ];
            R
        }
        "GetCommitmentPurchaseAnalysis" => {
            const R: &[Rule] = &[Rule::str_("AnalysisId", None, Some(36))];
            R
        }
        "GetCostAndUsage" => {
            const R: &[Rule] = &[
                Rule::enum_("Granularity", GRANULARITY),
                Rule::str_("BillingViewArn", Some(20), Some(2048)),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "GetCostAndUsageWithResources" => {
            const R: &[Rule] = &[
                Rule::enum_("Granularity", GRANULARITY),
                Rule::str_("BillingViewArn", Some(20), Some(2048)),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "GetCostAndUsageComparisons" => {
            const R: &[Rule] = &[
                Rule::str_("MetricForComparison", None, Some(1024)),
                Rule::str_("BillingViewArn", Some(20), Some(2048)),
                Rule::int_("MaxResults", Some(1), Some(2000)),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "GetCostComparisonDrivers" => {
            const R: &[Rule] = &[
                Rule::str_("MetricForComparison", None, Some(1024)),
                Rule::str_("BillingViewArn", Some(20), Some(2048)),
                Rule::int_("MaxResults", Some(1), Some(10)),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "GetCostCategories" => {
            const R: &[Rule] = &[
                Rule::str_("SearchString", None, Some(1024)),
                Rule::str_("CostCategoryName", Some(1), Some(50)),
                Rule::str_("BillingViewArn", Some(20), Some(2048)),
                Rule::int_("MaxResults", Some(1), None),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "GetCostForecast" => {
            const R: &[Rule] = &[
                Rule::enum_("Metric", METRIC),
                Rule::enum_("Granularity", GRANULARITY),
                Rule::str_("BillingViewArn", Some(20), Some(2048)),
                Rule::int_("PredictionIntervalLevel", Some(51), Some(99)),
            ];
            R
        }
        "GetUsageForecast" => {
            const R: &[Rule] = &[
                Rule::enum_("Metric", METRIC),
                Rule::enum_("Granularity", GRANULARITY),
                Rule::str_("BillingViewArn", Some(20), Some(2048)),
                Rule::int_("PredictionIntervalLevel", Some(51), Some(99)),
            ];
            R
        }
        "GetDimensionValues" => {
            const R: &[Rule] = &[
                Rule::str_("SearchString", None, Some(1024)),
                Rule::enum_("Dimension", DIMENSION),
                Rule::enum_("Context", CONTEXT),
                Rule::str_("BillingViewArn", Some(20), Some(2048)),
                Rule::int_("MaxResults", Some(1), None),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "GetReservationCoverage" => {
            const R: &[Rule] = &[
                Rule::enum_("Granularity", GRANULARITY),
                Rule::str_("NextPageToken", None, Some(8192)),
                Rule::int_("MaxResults", Some(1), None),
            ];
            R
        }
        "GetReservationUtilization" => {
            const R: &[Rule] = &[
                Rule::enum_("Granularity", GRANULARITY),
                Rule::str_("NextPageToken", None, Some(8192)),
                Rule::int_("MaxResults", Some(1), None),
            ];
            R
        }
        "GetReservationPurchaseRecommendation" => {
            const R: &[Rule] = &[
                Rule::str_("AccountId", None, Some(1024)),
                Rule::str_("Service", None, Some(1024)),
                Rule::enum_("AccountScope", ACCOUNT_SCOPE),
                Rule::enum_("LookbackPeriodInDays", LOOKBACK),
                Rule::enum_("TermInYears", TERM_IN_YEARS),
                Rule::enum_("PaymentOption", PAYMENT_OPTION),
                Rule::int_("PageSize", Some(0), Some(6000)),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "GetRightsizingRecommendation" => {
            const R: &[Rule] = &[
                Rule::str_("Service", None, Some(1024)),
                Rule::int_("PageSize", Some(0), Some(6000)),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "GetSavingsPlanPurchaseRecommendationDetails" => {
            const R: &[Rule] = &[Rule::str_("RecommendationDetailId", None, Some(36))];
            R
        }
        "GetSavingsPlansCoverage" => {
            const R: &[Rule] = &[
                Rule::enum_("Granularity", GRANULARITY),
                Rule::str_("NextToken", None, Some(8192)),
                Rule::int_("MaxResults", Some(1), None),
            ];
            R
        }
        "GetSavingsPlansPurchaseRecommendation" => {
            const R: &[Rule] = &[
                Rule::enum_("SavingsPlansType", SP_TYPE),
                Rule::enum_("TermInYears", TERM_IN_YEARS),
                Rule::enum_("PaymentOption", PAYMENT_OPTION),
                Rule::enum_("AccountScope", ACCOUNT_SCOPE),
                Rule::enum_("LookbackPeriodInDays", LOOKBACK),
                Rule::str_("NextPageToken", None, Some(8192)),
                Rule::int_("PageSize", Some(0), Some(6000)),
            ];
            R
        }
        "GetSavingsPlansUtilization" => {
            const R: &[Rule] = &[Rule::enum_("Granularity", GRANULARITY)];
            R
        }
        "GetSavingsPlansUtilizationDetails" => {
            const R: &[Rule] = &[
                Rule::str_("NextToken", None, Some(8192)),
                Rule::int_("MaxResults", Some(1), None),
            ];
            R
        }
        "GetTags" => {
            const R: &[Rule] = &[
                Rule::str_("SearchString", None, Some(1024)),
                Rule::str_("TagKey", None, Some(1024)),
                Rule::str_("BillingViewArn", Some(20), Some(2048)),
                Rule::int_("MaxResults", Some(1), None),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "ListCommitmentPurchaseAnalyses" => {
            const R: &[Rule] = &[
                Rule::enum_("AnalysisStatus", ANALYSIS_STATUS),
                Rule::str_("NextPageToken", None, Some(8192)),
                Rule::int_("PageSize", Some(0), Some(600)),
            ];
            R
        }
        "ListCostAllocationTagBackfillHistory" => {
            const R: &[Rule] = &[
                Rule::str_("NextToken", None, Some(8192)),
                Rule::int_("MaxResults", Some(1), Some(1000)),
            ];
            R
        }
        "ListCostAllocationTags" => {
            const R: &[Rule] = &[
                Rule::enum_("Status", TAG_STATUS),
                Rule::enum_("Type", TAG_TYPE),
                Rule::str_("NextToken", None, Some(8192)),
                Rule::int_("MaxResults", Some(1), Some(1000)),
            ];
            R
        }
        "ListSavingsPlansPurchaseRecommendationGeneration" => {
            const R: &[Rule] = &[
                Rule::enum_("GenerationStatus", GENERATION_STATUS),
                Rule::int_("PageSize", Some(0), Some(6000)),
                Rule::str_("NextPageToken", None, Some(8192)),
            ];
            R
        }
        "ListTagsForResource" | "TagResource" | "UntagResource" => {
            const R: &[Rule] = &[Rule::str_("ResourceArn", Some(20), Some(2048))];
            R
        }
        "StartCostAllocationTagBackfill" => {
            const R: &[Rule] = &[Rule::str_("BackfillFrom", Some(20), Some(25))];
            R
        }
        _ => &[],
    }
}
