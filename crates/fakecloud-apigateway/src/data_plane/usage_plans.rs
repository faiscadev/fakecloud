//! apigateway data_plane `usage_plans` concerns (audit-2026-05-19).

use super::*;

/// Resolve the usage plan that matches `(api_id, stage_name)` for an
/// API key. If no plan matches, the request is unmetered (the key is
/// known but not associated with a plan that targets this stage).
pub(super) fn enforce_usage_plan(
    service: &ApiGatewayService,
    req: &AwsRequest,
    api_id: &str,
    stage_name: &str,
    method_path: &str,
) -> Result<(), AwsServiceError> {
    let presented = req
        .headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let presented = match presented {
        Some(s) if !s.trim().is_empty() => s,
        _ => return Err(api_key_forbidden()),
    };

    // Resolve key + the active usage plan under a single read lock.
    // `plan = None` when the key isn't associated with any plan that
    // targets this stage; in that case the request is unmetered.
    let (key_id, plan): (String, Option<UsagePlanSnapshot>) = {
        let accounts = service.state_handle().read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(api_key_forbidden)?;
        let key = state
            .api_keys
            .values()
            .find(|k| k.value == presented)
            .cloned();
        let key = match key {
            Some(k) if k.enabled => k,
            _ => return Err(api_key_forbidden()),
        };
        let plan = first_matching_plan(state, &key.id, api_id, stage_name, method_path);
        (key.id, plan)
    };
    let Some(plan) = plan else {
        return Ok(());
    };

    let mut meters = service.meters.lock();

    // Throttle. A method-level override (`apiStages[].throttle[<path>]`)
    // takes precedence over the plan-level throttle. The bucket key
    // includes the method override path so each `(plan, key, method)`
    // triple meters independently — matching AWS's documented behavior
    // that method-level limits are evaluated separately from the plan
    // overall throttle.
    if let Some((rate, burst, method_override)) = plan.throttle {
        let bucket_key = (
            req.account_id.clone(),
            plan.id.clone(),
            key_id.clone(),
            method_override.unwrap_or_default(),
        );
        let bucket = meters
            .buckets
            .entry(bucket_key)
            .or_insert_with(|| TokenBucket::new(rate, burst));
        // Keep config fresh in case the plan was updated.
        bucket.rate_per_sec = rate;
        bucket.burst = burst;
        if !bucket.try_acquire(Instant::now()) {
            return Err(limit_exceeded());
        }
    }

    // Quota.
    if let Some((limit, period, offset)) = plan.quota {
        let window = current_quota_window(chrono::Utc::now(), period, offset);
        let counter_key = (
            req.account_id.clone(),
            plan.id.clone(),
            key_id.clone(),
            window,
        );
        let entry = meters.counters.entry(counter_key).or_insert(0);
        if *entry >= limit {
            return Err(limit_exceeded());
        }
        *entry += 1;
    }

    Ok(())
}

pub(super) fn parse_quota_period(s: &str) -> Option<QuotaPeriod> {
    match s {
        "DAY" => Some(QuotaPeriod::Day),
        "WEEK" => Some(QuotaPeriod::Week),
        "MONTH" => Some(QuotaPeriod::Month),
        _ => None,
    }
}

/// AWS picks any matching plan when multiple plans associate the same
/// key; we pick the first per `BTreeMap` iteration order, which is
/// deterministic by `usagePlanId`. `method_path` (e.g. `/items/GET`)
/// drives selection of the per-method throttle override under the
/// matched `apiStages[]` entry.
pub(super) fn first_matching_plan(
    state: &crate::state::ApiGatewayState,
    key_id: &str,
    api_id: &str,
    stage_name: &str,
    method_path: &str,
) -> Option<UsagePlanSnapshot> {
    for (plan_id, keys) in &state.usage_plan_keys {
        if !keys.contains_key(key_id) {
            continue;
        }
        let Some(plan) = state.usage_plans.get(plan_id) else {
            continue;
        };
        let matched_stage = plan.api_stages.iter().find(|stage_entry| {
            let api = stage_entry.get("apiId").and_then(|v| v.as_str());
            let stage = stage_entry.get("stage").and_then(|v| v.as_str());
            matches!((api, stage), (Some(a), Some(s)) if a == api_id && s == stage_name)
        });
        let Some(matched_stage) = matched_stage else {
            continue;
        };
        return Some(snapshot_plan(plan, matched_stage, method_path));
    }
    None
}

pub(super) fn snapshot_plan(
    plan: &crate::state::UsagePlan,
    matched_stage: &serde_json::Value,
    method_path: &str,
) -> UsagePlanSnapshot {
    let plan_throttle = plan.throttle.as_ref().and_then(parse_throttle);
    // Method-level override under the matched apiStage entry. AWS keys
    // these by `<resource_path>/<HTTP_METHOD>`. We try the exact path
    // first, then the AWS catch-all `/*/*` if no exact match.
    let method_throttle = matched_stage
        .get("throttle")
        .and_then(|t| t.as_object())
        .and_then(|map| {
            let exact = map.get(method_path);
            let wildcard = map.get("/*/*");
            exact
                .or(wildcard)
                .and_then(parse_throttle)
                .map(|(rate, burst)| (rate, burst, Some(method_path.to_string())))
        });
    let throttle =
        method_throttle.or_else(|| plan_throttle.map(|(rate, burst)| (rate, burst, None)));
    let quota = plan.quota.as_ref().and_then(|q| {
        let limit = q.get("limit").and_then(|v| v.as_u64())?;
        let period_str = q.get("period").and_then(|v| v.as_str())?;
        let period = parse_quota_period(period_str)?;
        let offset = q.get("offset").and_then(|v| v.as_i64()).unwrap_or(0);
        Some((limit, period, offset))
    });
    UsagePlanSnapshot {
        id: plan.id.clone(),
        throttle,
        quota,
    }
}

/// Parse a `{rateLimit, burstLimit}` JSON object into a `(rate, burst)`
/// pair. Returns `None` for missing fields, non-numeric values, or
/// non-positive limits — AWS treats those as "no throttle configured".
pub(super) fn parse_throttle(t: &serde_json::Value) -> Option<(f64, f64)> {
    let rate = t.get("rateLimit").and_then(|v| v.as_f64())?;
    let burst = t
        .get("burstLimit")
        .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|n| n as f64)))?;
    if rate <= 0.0 || burst <= 0.0 {
        return None;
    }
    Some((rate, burst))
}

pub(super) fn current_quota_window(
    now: chrono::DateTime<chrono::Utc>,
    period: QuotaPeriod,
    offset: i64,
) -> String {
    use chrono::Datelike;
    let date = now.date_naive() - chrono::Duration::days(offset);
    match period {
        QuotaPeriod::Day => format!("D:{date}"),
        QuotaPeriod::Week => {
            // ISO week: Monday-based. AWS docs aren't specific about
            // anchor day; ISO week is stable + locale-free.
            let iso = date.iso_week();
            format!("W:{}-{:02}", iso.year(), iso.week())
        }
        QuotaPeriod::Month => format!("M:{}-{:02}", date.year(), date.month()),
    }
}
