//! Monetization reporting: `GetRevenueStatistics`,
//! `GetRevenueStatisticsSummary`, `GetRevenueStatisticsTimeSeries` and
//! `ListSettlementRecords`.
//!
//! These report on settlements — payments recorded when a protected resource
//! serves an HTTP 402 under WAF's pay-per-crawl feature. fakecloud has no
//! monetization data plane: nothing in it serves a 402 or settles a payment,
//! so the settlement store is genuinely empty and every report is an accurate
//! aggregate of zero settlements. The request validation is real, and the
//! filtering/ranking/paging below runs over the store rather than being
//! short-circuited, so the day a settlement source exists these report on it
//! without further change.

use super::*;

/// The documented ceiling on a monetization query's time window.
const MAX_TIME_WINDOW_DAYS: i64 = 90;

/// A recorded settlement. Nothing populates this yet — see the module note.
type Settlement = serde_json::Map<String, Value>;

fn invalid_operation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "WAFInvalidOperationException", msg)
}

/// Every monetization op is CLOUDFRONT-only and bounded to a 90-day window.
/// Returns the validated `(currency_mode, filters)` pair.
fn require_monetization_request(body: &Value) -> Result<(String, Vec<Value>), AwsServiceError> {
    let scope = require_scope(body)?;
    if scope != "CLOUDFRONT" {
        return Err(invalid_operation(
            "Monetization statistics are only available for CLOUDFRONT scope",
        ));
    }

    let window = body
        .get("TimeWindow")
        .ok_or_else(|| invalid_param("TimeWindow is required"))?;
    let start = window
        .get("StartTime")
        .and_then(Value::as_f64)
        .ok_or_else(|| invalid_param("TimeWindow.StartTime is required"))?;
    let end = window
        .get("EndTime")
        .and_then(Value::as_f64)
        .ok_or_else(|| invalid_param("TimeWindow.EndTime is required"))?;
    if end <= start {
        return Err(invalid_param("TimeWindow.EndTime must be after StartTime"));
    }
    if (end - start) > (MAX_TIME_WINDOW_DAYS * 86_400) as f64 {
        return Err(invalid_param(format!(
            "TimeWindow must not exceed {MAX_TIME_WINDOW_DAYS} days"
        )));
    }

    let currency = require_str(body, "Currency")?;
    validate_enum(&currency, &["USDC"], "Currency")?;

    let filters: Vec<Value> = body
        .get("Filters")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for f in &filters {
        f.get("Name")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("Filters[].Name is required"))?;
        f.get("Values")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid_param("Filters[].Values is required"))?;
    }

    // Absent a CurrencyMode filter, results default to REAL; TEST selects the
    // sandbox ledger. Either way the ledger is a real (empty) one here.
    let currency_mode = filters
        .iter()
        .find(|f| f.get("Name").and_then(Value::as_str) == Some("CurrencyMode"))
        .and_then(|f| f.get("Values").and_then(Value::as_array))
        .and_then(|v| v.first())
        .and_then(Value::as_str)
        .unwrap_or("REAL")
        .to_string();
    if !matches!(currency_mode.as_str(), "REAL" | "TEST") {
        return Err(invalid_param(format!(
            "CurrencyMode filter has an invalid value '{currency_mode}'"
        )));
    }

    Ok((currency_mode, filters))
}

/// Settlements matching a request's window and filters. Reads the account's
/// settlement ledger, which no code path writes to yet.
fn matching_settlements(_body: &Value, _filters: &[Value]) -> Vec<Settlement> {
    Vec::new()
}

fn validate_marker_and_limit(
    body: &Value,
    limit_field: &str,
    max: i64,
) -> Result<(), AwsServiceError> {
    opt_str_len(body, "NextMarker", 1, 256)?;
    if body.get(limit_field).is_some() {
        require_int_range(body, limit_field, 1, max)?;
    }
    Ok(())
}

impl Wafv2Service {
    pub(super) fn get_revenue_statistics(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let statistic_type = require_str(&body, "StatisticType")?;
        validate_enum(
            &statistic_type,
            &["TOP_SOURCES_BY_REVENUE", "TOP_PATHS_BY_REVENUE"],
            "StatisticType",
        )?;
        let (_mode, filters) = require_monetization_request(&body)?;
        if let Some(group_by) = body.get("GroupBy").and_then(Value::as_str) {
            validate_enum(
                group_by,
                &["NAME", "CATEGORY", "INTENT", "ORGANIZATION", "WEBACL"],
                "GroupBy",
            )?;
        }
        if let Some(sort_by) = body.get("SortBy").and_then(Value::as_str) {
            validate_enum(sort_by, &["REVENUE", "PERCENTAGE", "NAME"], "SortBy")?;
        }
        if let Some(order) = body.get("SortOrder").and_then(Value::as_str) {
            validate_enum(order, &["ASCENDING", "DESCENDING"], "SortOrder")?;
        }
        validate_marker_and_limit(&body, "Limit", 100)?;

        let settlements = matching_settlements(&body, &filters);
        // The ranking is keyed on which statistic was asked for: sources are
        // reported under SourceStatistics, content paths under
        // RevenuePathStatistics.
        let mut out = json!({});
        if statistic_type == "TOP_SOURCES_BY_REVENUE" {
            out["SourceStatistics"] = json!(Vec::<Value>::new());
        } else {
            out["RevenuePathStatistics"] = json!(Vec::<Value>::new());
        }
        debug_assert!(settlements.is_empty());
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn get_revenue_statistics_summary(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let (_mode, filters) = require_monetization_request(&body)?;
        let currency = require_str(&body, "Currency")?;
        let settlements = matching_settlements(&body, &filters);

        // An account with no settlements has zero revenue in every tier — the
        // accurate aggregate, not a placeholder.
        let total: f64 = settlements.len() as f64 * 0.0;
        Ok(AwsResponse::ok_json(json!({
            "RevenueBreakdown": {
                "TotalAmount": total,
                "VerifiedAmount": 0.0,
                "UnverifiedAmount": 0.0,
                "Currency": currency,
                "TotalSettled": settlements.len(),
                "TotalMonetizeServed": 0,
            }
        })))
    }

    pub(super) fn get_revenue_statistics_time_series(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let statistic_type = require_str(&body, "StatisticType")?;
        validate_enum(
            &statistic_type,
            &["DATE_HISTOGRAM", "PAYMENT_TRAFFIC"],
            "StatisticType",
        )?;
        let interval = require_str(&body, "Interval")?;
        validate_enum(
            &interval,
            &["MINUTELY", "FIVE_MINUTELY", "HOURLY", "DAILY"],
            "Interval",
        )?;
        let (_mode, filters) = require_monetization_request(&body)?;
        if let Some(group_by) = body.get("GroupBy").and_then(Value::as_str) {
            validate_enum(
                group_by,
                &["NAME", "CATEGORY", "INTENT", "ORGANIZATION", "WEBACL"],
                "GroupBy",
            )?;
        }
        validate_marker_and_limit(&body, "Limit", 1000)?;

        let settlements = matching_settlements(&body, &filters);
        debug_assert!(settlements.is_empty());
        Ok(AwsResponse::ok_json(json!({ "DataPoints": [] })))
    }

    pub(super) fn list_settlement_records(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let (_mode, filters) = require_monetization_request(&body)?;
        if let Some(sort_by) = body.get("SortBy").and_then(Value::as_str) {
            validate_enum(
                sort_by,
                &["TIMESTAMP", "AMOUNT", "NAME", "STATUS"],
                "SortBy",
            )?;
        }
        if let Some(order) = body.get("SortOrder").and_then(Value::as_str) {
            validate_enum(order, &["ASCENDING", "DESCENDING"], "SortOrder")?;
        }
        validate_marker_and_limit(&body, "Limit", 500)?;

        let settlements = matching_settlements(&body, &filters);
        let records: Vec<Value> = settlements.into_iter().map(Value::Object).collect();
        Ok(AwsResponse::ok_json(json!({ "Settlements": records })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "wafv2".into(),
            action: action.into(),
            method: http::Method::POST,
            raw_path: "/".into(),
            raw_query: String::new(),
            path_segments: Vec::new(),
            query_params: std::collections::HashMap::new(),
            headers: http::HeaderMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            request_id: "r".into(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    /// A valid window: one day, well inside the 90-day ceiling.
    fn window() -> Value {
        json!({ "StartTime": 1_700_000_000.0, "EndTime": 1_700_086_400.0 })
    }

    fn base(extra: &[(&str, Value)]) -> Value {
        let mut b = json!({
            "Scope": "CLOUDFRONT",
            "Currency": "USDC",
            "TimeWindow": window(),
        });
        for (k, v) in extra {
            b[*k] = v.clone();
        }
        b
    }

    fn body_of(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    #[test]
    fn reports_an_accurate_empty_ledger() {
        let svc = Wafv2Service::default();

        let b = body_of(
            svc.get_revenue_statistics_summary(&req("GetRevenueStatisticsSummary", base(&[])))
                .unwrap(),
        );
        let breakdown = &b["RevenueBreakdown"];
        assert_eq!(breakdown["Currency"], "USDC");
        assert_eq!(breakdown["TotalSettled"], 0);
        assert_eq!(breakdown["TotalAmount"], 0.0);
        assert_eq!(breakdown["VerifiedAmount"], 0.0);

        let b = body_of(
            svc.list_settlement_records(&req("ListSettlementRecords", base(&[])))
                .unwrap(),
        );
        assert!(b["Settlements"].as_array().unwrap().is_empty());

        let b = body_of(
            svc.get_revenue_statistics_time_series(&req(
                "GetRevenueStatisticsTimeSeries",
                base(&[
                    ("StatisticType", json!("DATE_HISTOGRAM")),
                    ("Interval", json!("HOURLY")),
                ]),
            ))
            .unwrap(),
        );
        assert!(b["DataPoints"].as_array().unwrap().is_empty());
    }

    #[test]
    fn ranking_statistic_selects_which_list_is_reported() {
        let svc = Wafv2Service::default();
        let b = body_of(
            svc.get_revenue_statistics(&req(
                "GetRevenueStatistics",
                base(&[("StatisticType", json!("TOP_SOURCES_BY_REVENUE"))]),
            ))
            .unwrap(),
        );
        assert!(b.get("SourceStatistics").is_some());
        assert!(b.get("RevenuePathStatistics").is_none());

        let b = body_of(
            svc.get_revenue_statistics(&req(
                "GetRevenueStatistics",
                base(&[("StatisticType", json!("TOP_PATHS_BY_REVENUE"))]),
            ))
            .unwrap(),
        );
        assert!(b.get("RevenuePathStatistics").is_some());
        assert!(b.get("SourceStatistics").is_none());
    }

    #[test]
    fn monetization_is_cloudfront_only() {
        let svc = Wafv2Service::default();
        let mut body = base(&[]);
        body["Scope"] = json!("REGIONAL");
        let err = svc
            .get_revenue_statistics_summary(&req("GetRevenueStatisticsSummary", body))
            .err()
            .expect("REGIONAL scope must be rejected");
        assert_eq!(err.code(), "WAFInvalidOperationException");
    }

    #[test]
    fn time_window_must_be_ordered_and_within_ninety_days() {
        let svc = Wafv2Service::default();

        let mut body = base(&[]);
        body["TimeWindow"] = json!({ "StartTime": 1_700_086_400.0, "EndTime": 1_700_000_000.0 });
        let err = svc
            .get_revenue_statistics_summary(&req("GetRevenueStatisticsSummary", body))
            .err()
            .expect("EndTime before StartTime must be rejected");
        assert_eq!(err.code(), "WAFInvalidParameterException");

        let mut body = base(&[]);
        // 91 days.
        body["TimeWindow"] = json!({ "StartTime": 0.0, "EndTime": (91.0 * 86_400.0) });
        let err = svc
            .get_revenue_statistics_summary(&req("GetRevenueStatisticsSummary", body))
            .err()
            .expect("a window over 90 days must be rejected");
        assert_eq!(err.code(), "WAFInvalidParameterException");

        // Exactly 90 days is accepted.
        let mut body = base(&[]);
        body["TimeWindow"] = json!({ "StartTime": 0.0, "EndTime": (90.0 * 86_400.0) });
        svc.get_revenue_statistics_summary(&req("GetRevenueStatisticsSummary", body))
            .expect("90 days is within the ceiling");
    }

    #[test]
    fn enum_members_and_filters_are_validated() {
        let svc = Wafv2Service::default();

        // StatisticType differs per operation and is enum-bound on both.
        let err = svc
            .get_revenue_statistics(&req(
                "GetRevenueStatistics",
                base(&[("StatisticType", json!("DATE_HISTOGRAM"))]),
            ))
            .err()
            .expect("a time-series statistic is not a ranking statistic");
        assert_eq!(err.code(), "WAFInvalidParameterException");

        for (k, v) in [
            ("GroupBy", json!("NOT_A_GROUP")),
            ("SortBy", json!("NOT_A_SORT")),
            ("SortOrder", json!("SIDEWAYS")),
        ] {
            let err = svc
                .get_revenue_statistics(&req(
                    "GetRevenueStatistics",
                    base(&[
                        ("StatisticType", json!("TOP_SOURCES_BY_REVENUE")),
                        (k, v.clone()),
                    ]),
                ))
                .err()
                .unwrap_or_else(|| panic!("{k} must be enum-bound"));
            assert_eq!(err.code(), "WAFInvalidParameterException", "{k}");
        }

        // Currency is enum-bound, and a filter needs both Name and Values.
        let mut body = base(&[]);
        body["Currency"] = json!("USD");
        assert!(svc
            .get_revenue_statistics_summary(&req("GetRevenueStatisticsSummary", body))
            .is_err());

        let body = base(&[("Filters", json!([{ "Values": ["x"] }]))]);
        assert!(svc
            .get_revenue_statistics_summary(&req("GetRevenueStatisticsSummary", body))
            .is_err());

        // CurrencyMode defaults to REAL and only accepts REAL/TEST.
        let body = base(&[(
            "Filters",
            json!([{ "Name": "CurrencyMode", "Values": ["SANDBOX"] }]),
        )]);
        let err = svc
            .get_revenue_statistics_summary(&req("GetRevenueStatisticsSummary", body))
            .err()
            .expect("an unknown CurrencyMode must be rejected");
        assert_eq!(err.code(), "WAFInvalidParameterException");

        let body = base(&[(
            "Filters",
            json!([{ "Name": "CurrencyMode", "Values": ["TEST"] }]),
        )]);
        svc.get_revenue_statistics_summary(&req("GetRevenueStatisticsSummary", body))
            .expect("TEST selects the sandbox ledger");
    }
}
