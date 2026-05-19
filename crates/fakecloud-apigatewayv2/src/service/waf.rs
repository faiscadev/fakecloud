//! `ApiGatewayV2Service` `waf` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl ApiGatewayV2Service {
    /// Wire the shared WAFv2 state + rate limiter so the execute-API
    /// data plane can evaluate the WebACL associated with the matched
    /// stage's ARN. Pass the same `Wafv2Service::rate_limiter()`
    /// instance every dataplane uses so `RateBasedStatement` counters
    /// stay consistent.
    pub fn with_waf(
        mut self,
        state: fakecloud_wafv2::SharedWafv2State,
        rate_limiter: Arc<fakecloud_wafv2::RateLimiter>,
    ) -> Self {
        self.waf_state = Some(state);
        self.waf_rate_limiter = Some(rate_limiter);
        self
    }

    /// Snapshot of the WAF Count-action metrics. Keyed by
    /// `"<webacl-arn>|<rule-name>"`. Returns an empty map when WAF
    /// inspection is disabled.
    pub fn waf_count_metrics_snapshot(&self) -> std::collections::BTreeMap<String, u64> {
        self.waf_count_metrics.lock().clone()
    }

    /// Run WAFv2 inspection for one execute-API request. Returns
    /// `Some(response)` for a terminal action; returns `None` for
    /// `Allow` / `Count` / `NoAcl`.
    pub(super) fn evaluate_waf(
        &self,
        req: &AwsRequest,
        api_id: &str,
        stage_name: &str,
    ) -> Option<AwsResponse> {
        let waf_state = self.waf_state.as_ref()?;
        let limiter = self.waf_rate_limiter.as_ref()?;
        let resource_arn = self.stage_resource_arn(&req.region, api_id, stage_name);
        let ctx = build_waf_context(req);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let decision =
            fakecloud_wafv2::evaluate_request(waf_state, &resource_arn, &ctx, limiter, now);
        self.record_count_rules(&decision);
        decision_to_response(decision)
    }

    pub(super) fn record_count_rules(&self, decision: &fakecloud_wafv2::Decision) {
        let rules = decision.count_rules();
        if rules.is_empty() {
            return;
        }
        let Some(arn) = decision.web_acl_arn() else {
            return;
        };
        let mut metrics = self.waf_count_metrics.lock();
        for rule in rules {
            let key = format!("{arn}|{rule}");
            *metrics.entry(key).or_insert(0) += 1;
        }
    }

    pub(super) fn record_request(
        &self,
        req: &AwsRequest,
        api_id: &str,
        stage: &str,
        path: &str,
        status_code: u16,
    ) {
        let headers_map: std::collections::BTreeMap<String, String> = req
            .headers
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|v_str| (k.as_str().to_string(), v_str.to_string()))
            })
            .collect();

        let body_string = if req.body.is_empty() {
            None
        } else {
            String::from_utf8(req.body.to_vec()).ok()
        };

        let request_record = ApiRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            api_id: api_id.to_string(),
            stage: stage.to_string(),
            method: req.method.to_string(),
            path: path.to_string(),
            headers: headers_map,
            query_params: req.query_params.clone().into_iter().collect(),
            body: body_string,
            timestamp: chrono::Utc::now(),
            status_code,
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.request_history.push(request_record);
    }
}
