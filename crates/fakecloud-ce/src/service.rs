//! AWS Cost Explorer (`ce`) awsJson1.1 dispatch + operation handlers.
//!
//! The full 47-operation Cost Explorer control plane. Stored resources are
//! account-partitioned and persisted; each is kept as its already-output-valid
//! JSON object so `Get`/`Describe` echoes exactly what `Create`/`Update`
//! persisted. Reporting operations return the model's exact output shape with
//! empty result series and zeroed-but-present required aggregates — the honest
//! "no usage recorded" state for a fake account that incurs no real cost.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{CeData, SharedCeState};
use crate::validate::validate_input;

#[cfg(test)]
mod tests;

/// Every operation name in the Cost Explorer Smithy model (47 operations).
pub const CE_ACTIONS: &[&str] = &[
    "CreateAnomalyMonitor",
    "CreateAnomalySubscription",
    "CreateCostCategoryDefinition",
    "DeleteAnomalyMonitor",
    "DeleteAnomalySubscription",
    "DeleteCostCategoryDefinition",
    "DescribeCostCategoryDefinition",
    "GetAnomalies",
    "GetAnomalyMonitors",
    "GetAnomalySubscriptions",
    "GetApproximateUsageRecords",
    "GetCommitmentPurchaseAnalysis",
    "GetCostAndUsage",
    "GetCostAndUsageComparisons",
    "GetCostAndUsageWithResources",
    "GetCostCategories",
    "GetCostComparisonDrivers",
    "GetCostForecast",
    "GetDimensionValues",
    "GetReservationCoverage",
    "GetReservationPurchaseRecommendation",
    "GetReservationUtilization",
    "GetRightsizingRecommendation",
    "GetSavingsPlanPurchaseRecommendationDetails",
    "GetSavingsPlansCoverage",
    "GetSavingsPlansPurchaseRecommendation",
    "GetSavingsPlansUtilization",
    "GetSavingsPlansUtilizationDetails",
    "GetTags",
    "GetUsageForecast",
    "ListCommitmentPurchaseAnalyses",
    "ListCostAllocationTagBackfillHistory",
    "ListCostAllocationTags",
    "ListCostCategoryDefinitions",
    "ListCostCategoryResourceAssociations",
    "ListSavingsPlansPurchaseRecommendationGeneration",
    "ListTagsForResource",
    "ProvideAnomalyFeedback",
    "StartCommitmentPurchaseAnalysis",
    "StartCostAllocationTagBackfill",
    "StartSavingsPlansPurchaseRecommendationGeneration",
    "TagResource",
    "UntagResource",
    "UpdateAnomalyMonitor",
    "UpdateAnomalySubscription",
    "UpdateCostAllocationTagsStatus",
    "UpdateCostCategoryDefinition",
];

pub struct CeService {
    state: SharedCeState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl CeService {
    pub fn new(state: SharedCeState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }
}

#[async_trait]
impl AwsService for CeService {
    fn service_name(&self) -> &str {
        "ce"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating(request.action.as_str());
        let result = dispatch(self, &request);
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        CE_ACTIONS
    }
}

/// Read-only operations do not mutate state and skip the persistence save.
fn is_mutating(action: &str) -> bool {
    action.starts_with("Create")
        || action.starts_with("Delete")
        || action.starts_with("Update")
        || action.starts_with("Start")
        || action.starts_with("Tag")
        || action.starts_with("Untag")
        || action == "ProvideAnomalyFeedback"
}

fn dispatch(s: &CeService, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    let b = parse(req)?;
    validate_input(req.action.as_str(), &b)?;
    let ctx = Ctx {
        account: req.account_id.clone(),
    };
    match req.action.as_str() {
        // anomaly monitors
        "CreateAnomalyMonitor" => s.create_anomaly_monitor(&ctx, &b),
        "GetAnomalyMonitors" => s.get_anomaly_monitors(&ctx, &b),
        "UpdateAnomalyMonitor" => s.update_anomaly_monitor(&ctx, &b),
        "DeleteAnomalyMonitor" => s.delete_anomaly_monitor(&ctx, &b),
        // anomaly subscriptions
        "CreateAnomalySubscription" => s.create_anomaly_subscription(&ctx, &b),
        "GetAnomalySubscriptions" => s.get_anomaly_subscriptions(&ctx, &b),
        "UpdateAnomalySubscription" => s.update_anomaly_subscription(&ctx, &b),
        "DeleteAnomalySubscription" => s.delete_anomaly_subscription(&ctx, &b),
        // anomalies + feedback
        "GetAnomalies" => s.get_anomalies(&ctx, &b),
        "ProvideAnomalyFeedback" => s.provide_anomaly_feedback(&ctx, &b),
        // cost categories
        "CreateCostCategoryDefinition" => s.create_cost_category(&ctx, &b),
        "DescribeCostCategoryDefinition" => s.describe_cost_category(&ctx, &b),
        "UpdateCostCategoryDefinition" => s.update_cost_category(&ctx, &b),
        "DeleteCostCategoryDefinition" => s.delete_cost_category(&ctx, &b),
        "ListCostCategoryDefinitions" => s.list_cost_categories(&ctx, &b),
        "ListCostCategoryResourceAssociations" => s.list_cost_category_associations(&ctx, &b),
        // cost allocation tags
        "ListCostAllocationTags" => s.list_cost_allocation_tags(&ctx, &b),
        "UpdateCostAllocationTagsStatus" => s.update_cost_allocation_tags_status(&ctx, &b),
        "StartCostAllocationTagBackfill" => s.start_backfill(&ctx, &b),
        "ListCostAllocationTagBackfillHistory" => s.list_backfill_history(&ctx, &b),
        // tagging
        "TagResource" => s.tag_resource(&ctx, &b),
        "UntagResource" => s.untag_resource(&ctx, &b),
        "ListTagsForResource" => s.list_tags_for_resource(&ctx, &b),
        // commitment / savings-plans analyses + generation
        "StartCommitmentPurchaseAnalysis" => s.start_commitment_analysis(&ctx, &b),
        "GetCommitmentPurchaseAnalysis" => s.get_commitment_analysis(&ctx, &b),
        "ListCommitmentPurchaseAnalyses" => s.list_commitment_analyses(&ctx, &b),
        "StartSavingsPlansPurchaseRecommendationGeneration" => s.start_sp_generation(&ctx, &b),
        "ListSavingsPlansPurchaseRecommendationGeneration" => s.list_sp_generation(&ctx, &b),
        // reporting / analytics (structurally-valid zero state)
        "GetCostAndUsage" => cost_and_usage(&b, false),
        "GetCostAndUsageWithResources" => cost_and_usage(&b, true),
        "GetCostAndUsageComparisons" => cost_and_usage_comparisons(&b),
        "GetCostComparisonDrivers" => cost_comparison_drivers(&b),
        "GetCostCategories" => get_cost_categories_report(&b),
        "GetDimensionValues" => get_dimension_values(&b),
        "GetTags" => get_tags_report(&b),
        "GetCostForecast" => forecast(&b),
        "GetUsageForecast" => forecast(&b),
        "GetApproximateUsageRecords" => approximate_usage_records(&b),
        "GetReservationCoverage" => reservation_coverage(&b),
        "GetReservationUtilization" => reservation_utilization(&b),
        "GetReservationPurchaseRecommendation" => reservation_purchase_recommendation(&b),
        "GetRightsizingRecommendation" => rightsizing_recommendation(&b),
        "GetSavingsPlansCoverage" => savings_plans_coverage(&b),
        "GetSavingsPlansUtilization" => savings_plans_utilization(&b),
        "GetSavingsPlansUtilizationDetails" => savings_plans_utilization_details(&b),
        "GetSavingsPlansPurchaseRecommendation" => savings_plans_purchase_recommendation(&b),
        "GetSavingsPlanPurchaseRecommendationDetails" => {
            savings_plan_purchase_recommendation_details(&b)
        }
        _ => Err(AwsServiceError::action_not_implemented(
            s.service_name(),
            &req.action,
        )),
    }
}

// ===================== helpers =====================

/// Per-request account context. Cost Explorer is a global (partition-scoped)
/// service, so its ARNs carry an empty region segment.
struct Ctx {
    account: String,
}

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn empty_ok() -> Result<AwsResponse, AwsServiceError> {
    ok(json!({}))
}

fn parse(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| validation(&format!("Request body is malformed: {e}")))
}

fn err(code: &str, msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg)
}

fn validation(msg: &str) -> AwsServiceError {
    err("ValidationException", msg)
}

fn unknown_monitor(arn: &str) -> AwsServiceError {
    err(
        "UnknownMonitorException",
        &format!("Cannot find the anomaly monitor: {arn}."),
    )
}

fn unknown_subscription(arn: &str) -> AwsServiceError {
    err(
        "UnknownSubscriptionException",
        &format!("Cannot find the anomaly subscription: {arn}."),
    )
}

fn cost_category_not_found(arn: &str) -> AwsServiceError {
    err(
        "ResourceNotFoundException",
        &format!("Cannot find the cost category: {arn}."),
    )
}

/// Read a required string field. Presence (the key exists and is a string) is
/// enforced here; the empty string is accepted because several Cost Explorer
/// identifiers (`GenericString`, `MetricForComparison`) declare `length_min: 0`,
/// so an empty value is a valid boundary input. Any real minimum length is
/// enforced in `validate::validate_input`.
fn req_str<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .ok_or_else(|| validation(&format!("{f} is a required field.")))
}

/// Assert a required (possibly-structured) member is present and non-null.
fn req_present(b: &Value, f: &str) -> Result<(), AwsServiceError> {
    match b.get(f) {
        Some(v) if !v.is_null() => Ok(()),
        _ => Err(validation(&format!("{f} is a required field."))),
    }
}

fn opt_str<'a>(b: &'a Value, f: &str) -> Option<&'a str> {
    b.get(f).and_then(Value::as_str)
}

/// Current time as an RFC-3339 UTC `ZonedDateTime` (e.g. `2024-01-01T00:00:00Z`,
/// 20 chars — within the model's 20..25 length bound and matching its pattern).
fn now_zoned() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Current date as a `YearMonthDay` string (`2024-01-01`).
fn today() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

fn new_uuid() -> String {
    Uuid::new_v4().to_string()
}

fn monitor_arn(account: &str) -> String {
    format!("arn:aws:ce::{account}:anomalymonitor/{}", new_uuid())
}

fn subscription_arn(account: &str) -> String {
    format!("arn:aws:ce::{account}:anomalysubscription/{}", new_uuid())
}

fn cost_category_arn(account: &str) -> String {
    format!("arn:aws:ce::{account}:costcategory/{}", new_uuid())
}

/// Start/End bounds of an input `DateInterval`, defaulting when absent so the
/// echoed output `DateInterval` always carries its required members.
fn interval_bounds(tp: Option<&Value>) -> (String, String) {
    let start = tp
        .and_then(|v| v.get("Start"))
        .and_then(Value::as_str)
        .unwrap_or("2024-01-01")
        .to_string();
    let end = tp
        .and_then(|v| v.get("End"))
        .and_then(Value::as_str)
        .unwrap_or("2024-01-02")
        .to_string();
    (start, end)
}

fn date_interval(start: &str, end: &str) -> Value {
    json!({ "Start": start, "End": end })
}

/// The `MetricValue.Unit` Cost Explorer reports for a given metric key. Cost
/// metrics are denominated in the account currency (`USD`); the usage metrics
/// (`UsageQuantity`, `NormalizedUsageAmount`, and their enum forms) aggregate
/// heterogeneous usage units, for which real Cost Explorer returns `N/A`.
/// Matches both the `MetricName` string form (`UnblendedCost`) and the
/// `Metric` enum form (`UNBLENDED_COST`).
fn metric_unit(metric: &str) -> &'static str {
    let upper = metric.to_ascii_uppercase();
    if upper.contains("USAGE") || upper.contains("NORMALIZED") {
        "N/A"
    } else {
        "USD"
    }
}

/// Zeroed `MetricValue` for the given unit.
fn metric_value(unit: &str) -> Value {
    json!({ "Amount": "0", "Unit": unit })
}

/// Hard ceiling on emitted `ResultByTime` buckets, high enough that no real or
/// probe input reaches it (274 years of DAILY buckets). When a range would
/// exceed it, the caller emits a `NextPageToken` resume date so the series is
/// never silently truncated.
const MAX_BUCKETS: usize = 100_000;

/// The per-granularity `(start, end)` buckets for a range, plus an optional
/// resume-start date emitted when generation stopped at [`MAX_BUCKETS`].
type BucketSplit = (Vec<(String, String)>, Option<String>);

/// Split `[start, end)` into per-granularity `(start, end)` buckets. Returns
/// `None` when the bounds are not parseable `YYYY-MM-DD` dates (the probe fills
/// synthetic non-date strings) or the granularity is HOURLY, in which case the
/// caller emits a single bucket echoing the requested period verbatim.
/// Otherwise returns every bucket in the range, plus `Some(resume_start)` when
/// generation stopped at [`MAX_BUCKETS`] with more buckets remaining.
fn split_buckets(start: &str, end: &str, granularity: &str) -> Option<BucketSplit> {
    use chrono::{Datelike, Duration, NaiveDate};
    let fmt = "%Y-%m-%d";
    let s = NaiveDate::parse_from_str(start.get(0..10)?, fmt).ok()?;
    let e = NaiveDate::parse_from_str(end.get(0..10)?, fmt).ok()?;
    if e <= s {
        return None;
    }
    let mut out = Vec::new();
    let mut resume: Option<String> = None;
    let mut cur = s;
    while cur < e {
        if out.len() >= MAX_BUCKETS {
            resume = Some(cur.format(fmt).to_string());
            break;
        }
        let next = match granularity {
            "DAILY" => cur + Duration::days(1),
            "MONTHLY" => {
                let (mut y, mut m) = (cur.year(), cur.month());
                if m == 12 {
                    y += 1;
                    m = 1;
                } else {
                    m += 1;
                }
                NaiveDate::from_ymd_opt(y, m, 1).unwrap_or(e).min(e)
            }
            // HOURLY (and anything else): fall back to a single period-echoing
            // bucket rather than inventing hour-resolution timestamps for an
            // account with no recorded usage.
            _ => return None,
        };
        out.push((cur.format(fmt).to_string(), next.format(fmt).to_string()));
        cur = next;
    }
    if out.is_empty() {
        None
    } else {
        Some((out, resume))
    }
}

/// Opaque-index pagination over ordered rows (`NextToken`/`NextPageToken`).
fn paginate(rows: Vec<Value>, b: &Value, default_max: usize) -> (Vec<Value>, Option<String>) {
    let start = b
        .get("NextToken")
        .or_else(|| b.get("NextPageToken"))
        .and_then(Value::as_str)
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0);
    let max = b
        .get("MaxResults")
        .or_else(|| b.get("PageSize"))
        .and_then(Value::as_u64)
        .map(|m| m.clamp(1, 10_000) as usize)
        .unwrap_or(default_max);
    let end = start.saturating_add(max).min(rows.len());
    let page = rows.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < rows.len() {
        Some(format!("{end}"))
    } else {
        None
    };
    (page, next)
}

/// Copy a set of fields verbatim from `src` into `dst` when present.
fn copy_fields(src: &Value, dst: &mut Map<String, Value>, fields: &[&str]) {
    for f in fields {
        if let Some(v) = src.get(*f) {
            dst.insert((*f).to_string(), v.clone());
        }
    }
}

/// Store the request's `ResourceTags` into the account tag map under `key`.
fn store_resource_tags(data: &mut CeData, key: &str, b: &Value) {
    if let Some(tags) = b.get("ResourceTags").and_then(Value::as_array) {
        let entry = data.tags.entry(key.to_string()).or_default();
        for t in tags {
            if let Some(k) = t.get("Key").and_then(Value::as_str) {
                let v = t.get("Value").and_then(Value::as_str).unwrap_or("");
                entry.insert(k.to_string(), v.to_string());
            }
        }
    }
}

// ===================== anomaly monitors =====================

impl CeService {
    fn create_anomaly_monitor(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        req_present(b, "AnomalyMonitor")?;
        let arn = monitor_arn(&ctx.account);
        let mut mon = b
            .get("AnomalyMonitor")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        mon.insert("MonitorArn".into(), json!(arn));
        mon.entry("CreationDate").or_insert(json!(today()));
        mon.insert("LastUpdatedDate".into(), json!(today()));
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        store_resource_tags(data, &arn, b);
        data.anomaly_monitors
            .insert(arn.clone(), Value::Object(mon));
        ok(json!({ "MonitorArn": arn }))
    }

    fn get_anomaly_monitors(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let wanted: Option<Vec<String>> =
            b.get("MonitorArnList").and_then(Value::as_array).map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            });
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.anomaly_monitors
                    .iter()
                    .filter(|(arn, _)| wanted.as_ref().is_none_or(|w| w.contains(arn)))
                    .map(|(_, v)| v.clone())
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, b, 100);
        let mut out = Map::new();
        out.insert("AnomalyMonitors".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("NextPageToken".into(), json!(t));
        }
        ok(Value::Object(out))
    }

    fn update_anomaly_monitor(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "MonitorArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let mon = data
            .anomaly_monitors
            .get_mut(&arn)
            .ok_or_else(|| unknown_monitor(&arn))?;
        if let Some(obj) = mon.as_object_mut() {
            if let Some(name) = opt_str(b, "MonitorName") {
                obj.insert("MonitorName".into(), json!(name));
            }
            obj.insert("LastUpdatedDate".into(), json!(today()));
        }
        ok(json!({ "MonitorArn": arn }))
    }

    fn delete_anomaly_monitor(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "MonitorArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.anomaly_monitors.remove(&arn).is_none() {
            return Err(unknown_monitor(&arn));
        }
        data.tags.remove(&arn);
        empty_ok()
    }
}

// ===================== anomaly subscriptions =====================

impl CeService {
    fn create_anomaly_subscription(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        req_present(b, "AnomalySubscription")?;
        let arn = subscription_arn(&ctx.account);
        let mut sub = b
            .get("AnomalySubscription")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        sub.insert("SubscriptionArn".into(), json!(arn));
        sub.entry("AccountId").or_insert(json!(ctx.account));
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        store_resource_tags(data, &arn, b);
        data.anomaly_subscriptions
            .insert(arn.clone(), Value::Object(sub));
        ok(json!({ "SubscriptionArn": arn }))
    }

    fn get_anomaly_subscriptions(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let wanted: Option<Vec<String>> = b
            .get("SubscriptionArnList")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            });
        let monitor_filter = opt_str(b, "MonitorArn").map(str::to_string);
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.anomaly_subscriptions
                    .iter()
                    .filter(|(arn, _)| wanted.as_ref().is_none_or(|w| w.contains(arn)))
                    .filter(|(_, v)| {
                        monitor_filter.as_deref().is_none_or(|m| {
                            v.get("MonitorArnList")
                                .and_then(Value::as_array)
                                .map(|l| l.iter().any(|x| x.as_str() == Some(m)))
                                .unwrap_or(false)
                        })
                    })
                    .map(|(_, v)| v.clone())
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, b, 100);
        let mut out = Map::new();
        out.insert("AnomalySubscriptions".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("NextPageToken".into(), json!(t));
        }
        ok(Value::Object(out))
    }

    fn update_anomaly_subscription(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "SubscriptionArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let sub = data
            .anomaly_subscriptions
            .get_mut(&arn)
            .ok_or_else(|| unknown_subscription(&arn))?;
        if let Some(obj) = sub.as_object_mut() {
            copy_fields(
                b,
                obj,
                &[
                    "Threshold",
                    "Frequency",
                    "MonitorArnList",
                    "Subscribers",
                    "ThresholdExpression",
                ],
            );
            if let Some(name) = opt_str(b, "SubscriptionName") {
                obj.insert("SubscriptionName".into(), json!(name));
            }
        }
        ok(json!({ "SubscriptionArn": arn }))
    }

    fn delete_anomaly_subscription(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "SubscriptionArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.anomaly_subscriptions.remove(&arn).is_none() {
            return Err(unknown_subscription(&arn));
        }
        data.tags.remove(&arn);
        empty_ok()
    }
}

// ===================== anomalies + feedback =====================

impl CeService {
    fn get_anomalies(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        req_present(b, "DateInterval")?;
        let monitor_filter = opt_str(b, "MonitorArn").map(str::to_string);
        let feedback_filter = opt_str(b, "Feedback").map(str::to_string);
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.anomalies
                    .values()
                    .filter(|a| {
                        monitor_filter
                            .as_deref()
                            .is_none_or(|m| a.get("MonitorArn").and_then(Value::as_str) == Some(m))
                    })
                    .filter(|a| {
                        feedback_filter
                            .as_deref()
                            .is_none_or(|f| a.get("Feedback").and_then(Value::as_str) == Some(f))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, b, 100);
        let mut out = Map::new();
        out.insert("Anomalies".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("NextPageToken".into(), json!(t));
        }
        ok(Value::Object(out))
    }

    fn provide_anomaly_feedback(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "AnomalyId")?.to_string();
        let feedback = req_str(b, "Feedback")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(anomaly) = data.anomalies.get_mut(&id) {
            if let Some(obj) = anomaly.as_object_mut() {
                obj.insert("Feedback".into(), json!(feedback));
            }
        }
        ok(json!({ "AnomalyId": id }))
    }
}

// ===================== cost categories =====================

impl CeService {
    fn build_cost_category(&self, ctx: &Ctx, arn: &str, b: &Value, effective_start: &str) -> Value {
        let mut cc = Map::new();
        cc.insert("CostCategoryArn".into(), json!(arn));
        cc.insert("EffectiveStart".into(), json!(effective_start));
        cc.insert(
            "Name".into(),
            b.get("Name").cloned().unwrap_or(json!("cost-category")),
        );
        cc.insert(
            "RuleVersion".into(),
            b.get("RuleVersion")
                .cloned()
                .unwrap_or(json!("CostCategoryExpression.v1")),
        );
        cc.insert("Rules".into(), b.get("Rules").cloned().unwrap_or(json!([])));
        copy_fields(b, &mut cc, &["SplitChargeRules", "DefaultValue"]);
        let _ = ctx;
        Value::Object(cc)
    }

    fn create_cost_category(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        req_str(b, "Name")?;
        req_str(b, "RuleVersion")?;
        req_present(b, "Rules")?;
        let arn = cost_category_arn(&ctx.account);
        let effective_start = opt_str(b, "EffectiveStart")
            .map(str::to_string)
            .unwrap_or_else(now_zoned);
        let cc = self.build_cost_category(ctx, &arn, b, &effective_start);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        store_resource_tags(data, &arn, b);
        data.cost_categories.insert(arn.clone(), cc);
        ok(json!({ "CostCategoryArn": arn, "EffectiveStart": effective_start }))
    }

    fn describe_cost_category(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "CostCategoryArn")?.to_string();
        let guard = self.state.read();
        match guard
            .get(&ctx.account)
            .and_then(|d| d.cost_categories.get(&arn))
        {
            Some(cc) => ok(json!({ "CostCategory": cc })),
            None => Err(cost_category_not_found(&arn)),
        }
    }

    fn update_cost_category(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "CostCategoryArn")?.to_string();
        req_str(b, "RuleVersion")?;
        req_present(b, "Rules")?;
        let effective_start = opt_str(b, "EffectiveStart")
            .map(str::to_string)
            .unwrap_or_else(now_zoned);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let existing = data
            .cost_categories
            .get(&arn)
            .cloned()
            .ok_or_else(|| cost_category_not_found(&arn))?;
        // Preserve the immutable Name; bump RuleVersion/Rules and effective date.
        let mut merged = b.clone();
        if let (Some(obj), Some(name)) = (merged.as_object_mut(), existing.get("Name")) {
            obj.entry("Name").or_insert(name.clone());
        }
        let cc = self.build_cost_category(ctx, &arn, &merged, &effective_start);
        data.cost_categories.insert(arn.clone(), cc);
        ok(json!({ "CostCategoryArn": arn, "EffectiveStart": effective_start }))
    }

    fn delete_cost_category(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "CostCategoryArn")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.cost_categories.remove(&arn).is_none() {
            return Err(cost_category_not_found(&arn));
        }
        data.cost_category_associations.remove(&arn);
        data.tags.remove(&arn);
        ok(json!({ "CostCategoryArn": arn, "EffectiveEnd": now_zoned() }))
    }

    fn list_cost_categories(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.cost_categories
                    .values()
                    .map(|cc| {
                        let mut r = Map::new();
                        copy_fields(
                            cc,
                            &mut r,
                            &["CostCategoryArn", "Name", "EffectiveStart", "EffectiveEnd"],
                        );
                        let n = cc
                            .get("Rules")
                            .and_then(Value::as_array)
                            .map(|a| a.len())
                            .unwrap_or(0);
                        r.insert("NumberOfRules".into(), json!(n));
                        copy_fields(cc, &mut r, &["DefaultValue"]);
                        Value::Object(r)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, b, 100);
        let mut out = Map::new();
        out.insert("CostCategoryReferences".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("NextToken".into(), json!(t));
        }
        ok(Value::Object(out))
    }

    fn list_cost_category_associations(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let rows: Vec<Value> = if let Some(arn) = opt_str(b, "CostCategoryArn") {
            if !data
                .map(|d| d.cost_categories.contains_key(arn))
                .unwrap_or(false)
            {
                return Err(cost_category_not_found(arn));
            }
            data.and_then(|d| d.cost_category_associations.get(arn))
                .cloned()
                .unwrap_or_default()
        } else {
            data.map(|d| {
                d.cost_category_associations
                    .values()
                    .flatten()
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
        };
        let (page, next) = paginate(rows, b, 100);
        let mut out = Map::new();
        out.insert(
            "CostCategoryResourceAssociations".into(),
            Value::Array(page),
        );
        if let Some(t) = next {
            out.insert("NextToken".into(), json!(t));
        }
        ok(Value::Object(out))
    }
}

// ===================== cost allocation tags =====================

impl CeService {
    fn list_cost_allocation_tags(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let status_filter = opt_str(b, "Status").map(str::to_string);
        let type_filter = opt_str(b, "Type").map(str::to_string);
        let key_filter: Option<Vec<String>> = b.get("TagKeys").and_then(Value::as_array).map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        });
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.cost_allocation_tags
                    .iter()
                    .filter(|(k, v)| {
                        status_filter
                            .as_deref()
                            .is_none_or(|s| v.get("Status").and_then(Value::as_str) == Some(s))
                            && type_filter
                                .as_deref()
                                .is_none_or(|t| v.get("Type").and_then(Value::as_str) == Some(t))
                            && key_filter.as_ref().is_none_or(|ks| ks.contains(k))
                    })
                    .map(|(_, v)| v.clone())
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, b, 100);
        let mut out = Map::new();
        out.insert("CostAllocationTags".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("NextToken".into(), json!(t));
        }
        ok(Value::Object(out))
    }

    fn update_cost_allocation_tags_status(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        req_present(b, "CostAllocationTagsStatus")?;
        let entries = b
            .get("CostAllocationTagsStatus")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        for entry in &entries {
            let (Some(key), Some(status)) = (
                entry.get("TagKey").and_then(Value::as_str),
                entry.get("Status").and_then(Value::as_str),
            ) else {
                continue;
            };
            let tag = data
                .cost_allocation_tags
                .entry(key.to_string())
                .or_insert_with(|| {
                    json!({
                        "TagKey": key,
                        "Type": "UserDefined",
                        "Status": status,
                    })
                });
            if let Some(obj) = tag.as_object_mut() {
                obj.insert("Status".into(), json!(status));
                obj.insert("LastUpdatedDate".into(), json!(now_zoned()));
            }
        }
        // No per-key failures in the emulator: every requested update settles.
        ok(json!({ "Errors": [] }))
    }

    fn start_backfill(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let backfill_from = req_str(b, "BackfillFrom")?.to_string();
        let now = now_zoned();
        let request = json!({
            "BackfillFrom": backfill_from,
            "RequestedAt": now,
            "CompletedAt": now,
            "BackfillStatus": "SUCCEEDED",
            "LastUpdatedAt": now,
        });
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.backfill_requests.push(request.clone());
        ok(json!({ "BackfillRequest": request }))
    }

    fn list_backfill_history(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.backfill_requests.iter().rev().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(rows, b, 100);
        let mut out = Map::new();
        out.insert("BackfillRequests".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("NextToken".into(), json!(t));
        }
        ok(Value::Object(out))
    }
}

// ===================== tagging =====================

impl CeService {
    fn tag_resource(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "ResourceArn")?.to_string();
        req_present(b, "ResourceTags")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        store_resource_tags(data, &arn, b);
        empty_ok()
    }

    fn untag_resource(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "ResourceArn")?.to_string();
        req_present(b, "ResourceTagKeys")?;
        let keys = b
            .get("ResourceTagKeys")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(entry) = data.tags.get_mut(&arn) {
            for k in &keys {
                if let Some(k) = k.as_str() {
                    entry.remove(k);
                }
            }
        }
        empty_ok()
    }

    fn list_tags_for_resource(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(b, "ResourceArn")?.to_string();
        let guard = self.state.read();
        let tags: Vec<Value> = guard
            .get(&ctx.account)
            .and_then(|d| d.tags.get(&arn))
            .map(|m| {
                m.iter()
                    .map(|(k, v)| json!({ "Key": k, "Value": v }))
                    .collect()
            })
            .unwrap_or_default();
        ok(json!({ "ResourceTags": tags }))
    }
}

// ===================== commitment / savings-plans analyses =====================

impl CeService {
    fn start_commitment_analysis(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        req_present(b, "CommitmentPurchaseAnalysisConfiguration")?;
        let id = new_uuid();
        let now = now_zoned();
        let config = b
            .get("CommitmentPurchaseAnalysisConfiguration")
            .cloned()
            .unwrap_or(json!({}));
        let record = json!({
            "AnalysisId": id,
            "AnalysisStartedTime": now,
            "EstimatedCompletionTime": now,
            "AnalysisCompletionTime": now,
            "AnalysisStatus": "SUCCEEDED",
            "CommitmentPurchaseAnalysisConfiguration": config,
        });
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.commitment_analyses.insert(id.clone(), record);
        ok(json!({
            "AnalysisId": id,
            "AnalysisStartedTime": now,
            "EstimatedCompletionTime": now,
        }))
    }

    fn get_commitment_analysis(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = req_str(b, "AnalysisId")?.to_string();
        let guard = self.state.read();
        let rec = guard
            .get(&ctx.account)
            .and_then(|d| d.commitment_analyses.get(&id))
            .cloned()
            .ok_or_else(|| {
                err(
                    "AnalysisNotFoundException",
                    &format!("Cannot find the analysis: {id}."),
                )
            })?;
        ok(rec)
    }

    fn list_commitment_analyses(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let status_filter = opt_str(b, "AnalysisStatus").map(str::to_string);
        let id_filter: Option<Vec<String>> =
            b.get("AnalysisIds").and_then(Value::as_array).map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            });
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.commitment_analyses
                    .iter()
                    .filter(|(id, v)| {
                        status_filter.as_deref().is_none_or(|s| {
                            v.get("AnalysisStatus").and_then(Value::as_str) == Some(s)
                        }) && id_filter.as_ref().is_none_or(|ids| ids.contains(id))
                    })
                    .map(|(_, v)| {
                        let mut summary = Map::new();
                        copy_fields(
                            v,
                            &mut summary,
                            &[
                                "AnalysisId",
                                "AnalysisStatus",
                                "AnalysisStartedTime",
                                "AnalysisCompletionTime",
                                "EstimatedCompletionTime",
                                "CommitmentPurchaseAnalysisConfiguration",
                            ],
                        );
                        Value::Object(summary)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, b, 100);
        let mut out = Map::new();
        out.insert("AnalysisSummaryList".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("NextPageToken".into(), json!(t));
        }
        ok(Value::Object(out))
    }

    fn start_sp_generation(&self, ctx: &Ctx, _b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = new_uuid();
        let now = now_zoned();
        let record = json!({
            "RecommendationId": id,
            "GenerationStatus": "SUCCEEDED",
            "GenerationStartedTime": now,
            "GenerationCompletionTime": now,
            "EstimatedCompletionTime": now,
        });
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.sp_generations.insert(id.clone(), record);
        ok(json!({
            "RecommendationId": id,
            "GenerationStartedTime": now,
            "EstimatedCompletionTime": now,
        }))
    }

    fn list_sp_generation(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let status_filter = opt_str(b, "GenerationStatus").map(str::to_string);
        let id_filter: Option<Vec<String>> = b
            .get("RecommendationIds")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            });
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.sp_generations
                    .iter()
                    .filter(|(id, v)| {
                        status_filter.as_deref().is_none_or(|s| {
                            v.get("GenerationStatus").and_then(Value::as_str) == Some(s)
                        }) && id_filter.as_ref().is_none_or(|ids| ids.contains(id))
                    })
                    .map(|(_, v)| v.clone())
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, b, 100);
        let mut out = Map::new();
        out.insert("GenerationSummaryList".into(), Value::Array(page));
        if let Some(t) = next {
            out.insert("NextPageToken".into(), json!(t));
        }
        ok(Value::Object(out))
    }
}

// ===================== reporting / analytics (zero state) =====================

/// One `ResultByTime` bucket with the requested metrics zeroed (each in its
/// metric-appropriate unit) and no groups.
fn result_bucket(start: &str, end: &str, metrics: &[String]) -> Value {
    let mut total = Map::new();
    for m in metrics {
        total.insert(m.clone(), metric_value(metric_unit(m)));
    }
    json!({
        "TimePeriod": date_interval(start, end),
        "Total": Value::Object(total),
        "Groups": [],
        "Estimated": false,
    })
}

/// `GetCostAndUsage` (`require_filter=false`) and `GetCostAndUsageWithResources`
/// (`require_filter=true`) share this handler. The two ops declare different
/// `@required` input members: `GetCostAndUsage` requires `Metrics`, while
/// `GetCostAndUsageWithResources` requires `Filter` (and `Metrics` is optional).
fn cost_and_usage(b: &Value, require_filter: bool) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "TimePeriod")?;
    req_str(b, "Granularity")?;
    if require_filter {
        req_present(b, "Filter")?;
    } else {
        req_present(b, "Metrics")?;
    }
    let (period_start, end) = interval_bounds(b.get("TimePeriod"));
    // A `NextPageToken` carrying a resume date (emitted only when a prior page
    // hit MAX_BUCKETS) continues the series from there; otherwise start at the
    // requested period start. Synthetic non-date tokens fall back to the start.
    let start = opt_str(b, "NextPageToken")
        .filter(|t| {
            chrono::NaiveDate::parse_from_str(t.get(0..10).unwrap_or(t), "%Y-%m-%d").is_ok()
        })
        .unwrap_or(&period_start)
        .to_string();
    let granularity = opt_str(b, "Granularity").unwrap_or("MONTHLY");
    let metrics: Vec<String> = b
        .get("Metrics")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let (results, next_token): (Vec<Value>, Option<String>) =
        match split_buckets(&start, &end, granularity) {
            Some((buckets, resume)) => (
                buckets
                    .iter()
                    .map(|(s, e)| result_bucket(s, e, &metrics))
                    .collect(),
                resume,
            ),
            None => (vec![result_bucket(&start, &end, &metrics)], None),
        };
    let mut out = Map::new();
    out.insert("ResultsByTime".into(), Value::Array(results));
    if let Some(gb) = b.get("GroupBy") {
        out.insert("GroupDefinitions".into(), gb.clone());
    }
    out.insert("DimensionValueAttributes".into(), json!([]));
    if let Some(token) = next_token {
        out.insert("NextPageToken".into(), json!(token));
    }
    ok(Value::Object(out))
}

fn cost_and_usage_comparisons(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "BaselineTimePeriod")?;
    req_present(b, "ComparisonTimePeriod")?;
    req_str(b, "MetricForComparison")?;
    ok(json!({
        "CostAndUsageComparisons": [],
        "TotalCostAndUsage": {},
    }))
}

fn cost_comparison_drivers(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "BaselineTimePeriod")?;
    req_present(b, "ComparisonTimePeriod")?;
    req_str(b, "MetricForComparison")?;
    ok(json!({ "CostComparisonDrivers": [] }))
}

fn get_cost_categories_report(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "TimePeriod")?;
    ok(json!({
        "CostCategoryNames": [],
        "CostCategoryValues": [],
        "ReturnSize": 0,
        "TotalSize": 0,
    }))
}

fn get_dimension_values(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "TimePeriod")?;
    req_str(b, "Dimension")?;
    ok(json!({
        "DimensionValues": [],
        "ReturnSize": 0,
        "TotalSize": 0,
    }))
}

fn get_tags_report(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "TimePeriod")?;
    ok(json!({
        "Tags": [],
        "ReturnSize": 0,
        "TotalSize": 0,
    }))
}

fn forecast(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "TimePeriod")?;
    let metric = req_str(b, "Metric")?;
    let unit = metric_unit(metric);
    req_str(b, "Granularity")?;
    ok(json!({
        "Total": metric_value(unit),
        "ForecastResultsByTime": [],
    }))
}

fn approximate_usage_records(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_str(b, "Granularity")?;
    req_str(b, "ApproximationDimension")?;
    ok(json!({
        "Services": {},
        "TotalRecords": 0,
        "LookbackPeriod": date_interval("2024-01-01", "2024-01-02"),
    }))
}

fn reservation_coverage(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "TimePeriod")?;
    ok(json!({ "CoveragesByTime": [] }))
}

fn reservation_utilization(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "TimePeriod")?;
    ok(json!({ "UtilizationsByTime": [] }))
}

fn reservation_purchase_recommendation(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_str(b, "Service")?;
    ok(json!({ "Recommendations": [] }))
}

fn rightsizing_recommendation(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_str(b, "Service")?;
    ok(json!({ "RightsizingRecommendations": [] }))
}

fn savings_plans_coverage(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "TimePeriod")?;
    ok(json!({ "SavingsPlansCoverages": [] }))
}

fn savings_plans_utilization(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "TimePeriod")?;
    ok(json!({
        "Total": {
            "Utilization": {
                "TotalCommitment": "0.0",
                "UsedCommitment": "0.0",
                "UnusedCommitment": "0.0",
                "UtilizationPercentage": "0.0",
            }
        }
    }))
}

fn savings_plans_utilization_details(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_present(b, "TimePeriod")?;
    let (start, end) = interval_bounds(b.get("TimePeriod"));
    ok(json!({
        "SavingsPlansUtilizationDetails": [],
        "TimePeriod": date_interval(&start, &end),
    }))
}

fn savings_plans_purchase_recommendation(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    req_str(b, "SavingsPlansType")?;
    req_str(b, "TermInYears")?;
    req_str(b, "PaymentOption")?;
    req_str(b, "LookbackPeriodInDays")?;
    ok(json!({}))
}

fn savings_plan_purchase_recommendation_details(b: &Value) -> Result<AwsResponse, AwsServiceError> {
    let id = req_str(b, "RecommendationDetailId")?.to_string();
    ok(json!({ "RecommendationDetailId": id }))
}
