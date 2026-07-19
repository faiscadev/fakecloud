//! Cross-service inspection helpers for WAFv2.
//!
//! This module exposes a single high-level entry point,
//! [`evaluate_request`], that the upstream dataplanes (ELBv2 ALB, API
//! Gateway v1, API Gateway v2, CloudFront) call before forwarding a
//! request to its real handler.
//!
//! The helper looks up the WebACL associated with `resource_arn` in the
//! shared WAFv2 state, snapshots the IpSets / RegexPatternSets it
//! references, and returns a [`Decision`] that tells the caller
//! whether to:
//!
//! - **Allow** the request through (the rules either matched a non-
//!   terminal rule or the WebACL's default action was Allow).
//! - **Block** the request with a synthetic 403 response (or the
//!   per-rule custom status / body).
//! - **Count** the request — same as Allow but at least one Count rule
//!   matched and was recorded for metrics.
//! - **Captcha** / **Challenge** — short-circuit with a 405-style
//!   response. Full CAPTCHA / interstitial flow is out of scope for
//!   this batch; callers surface the action and short-circuit.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use crate::evaluator::{
    evaluate_web_acl, RateLimiter, WafAction, WafRequest, WafVerdict, FAKECLOUD_GEO_COUNTRY_HEADER,
};
use crate::state::{IpSet, RegexPatternSet, RuleGroup, SharedWafv2State, WebAcl};

/// Default cap on the request-body bytes inspected by
/// [`evaluate_request`]. Matches AWS WAF's free-tier inspection limit
/// for ALB (8 KB).
pub const DEFAULT_BODY_INSPECTION_LIMIT: usize = 8 * 1024;

/// Outcome from [`evaluate_request`]. The caller maps this to an HTTP
/// response (or lets the request fall through to the real handler).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// No WebACL is associated with the resource. Caller proceeds as
    /// if WAF inspection were disabled.
    NoAcl,
    /// Allow the request through. `count_rules` lists rules that
    /// matched with a `Count` action — they are non-terminal but
    /// useful for metrics.
    Allow {
        labels: Vec<String>,
        count_rules: Vec<String>,
        web_acl_arn: String,
    },
    /// Block the request. `status` defaults to 403; rules can override
    /// via `CustomResponse`. `body_key` references a WebACL-level
    /// `CustomResponseBodies` entry (the caller resolves it).
    Block {
        status: u16,
        body_key: Option<String>,
        terminating_rule_id: Option<String>,
        labels: Vec<String>,
        count_rules: Vec<String>,
        web_acl_arn: String,
    },
    /// Captcha challenge. Real AWS would return an interstitial page;
    /// fakecloud surfaces a 405 short-circuit response and lets tests
    /// observe the action via the verdict shape.
    Captcha {
        terminating_rule_id: Option<String>,
        labels: Vec<String>,
        web_acl_arn: String,
    },
    /// Silent challenge (browser fingerprint check). Same short-circuit
    /// shape as `Captcha`.
    Challenge {
        terminating_rule_id: Option<String>,
        labels: Vec<String>,
        web_acl_arn: String,
    },
}

impl Decision {
    /// Whether the caller should short-circuit with a synthetic
    /// response. `Allow` and `NoAcl` return `false`; everything else
    /// returns `true`.
    pub fn is_short_circuit(&self) -> bool {
        !matches!(self, Decision::NoAcl | Decision::Allow { .. })
    }

    /// Names of rules whose matching `Count` action fired during this
    /// evaluation. Empty for `NoAcl`, `Captcha`, and `Challenge`.
    pub fn count_rules(&self) -> &[String] {
        match self {
            Decision::Allow { count_rules, .. } | Decision::Block { count_rules, .. } => {
                count_rules
            }
            _ => &[],
        }
    }

    /// ARN of the WebACL that owns this decision. `None` when no
    /// WebACL is associated with the resource.
    pub fn web_acl_arn(&self) -> Option<&str> {
        match self {
            Decision::Allow { web_acl_arn, .. }
            | Decision::Block { web_acl_arn, .. }
            | Decision::Captcha { web_acl_arn, .. }
            | Decision::Challenge { web_acl_arn, .. } => Some(web_acl_arn.as_str()),
            Decision::NoAcl => None,
        }
    }
}

/// Cross-service request context the dataplane assembles before
/// invoking [`evaluate_request`]. Field semantics match
/// [`WafRequest`] — this struct is a friendlier owned-data wrapper so
/// dataplane callers don't have to re-borrow lifetimes through the
/// inspection step.
#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    pub method: String,
    /// URI path only. Query string is carried separately so caller
    /// dataplanes can pass `uri.path()` directly.
    pub uri_path: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    /// First [`DEFAULT_BODY_INSPECTION_LIMIT`] bytes of the body (or
    /// fewer). Real wire size is preserved in `body_size_bytes`.
    pub body: Vec<u8>,
    pub body_size_bytes: u64,
    pub source_ip: Option<IpAddr>,
    /// 2-letter country code resolved via GeoIP. When `None`, callers
    /// can still set it via the `x-fakecloud-geo-country` header on
    /// the inspected request — the evaluator picks that up.
    pub country: Option<String>,
}

impl RequestContext {
    /// Construct a context with `body` automatically truncated to
    /// [`DEFAULT_BODY_INSPECTION_LIMIT`]. `body_size_bytes` is set to
    /// the *full* original length so SizeConstraint statements still
    /// see the wire size.
    pub fn new(method: &str, uri_path: &str, query: &str) -> Self {
        Self {
            method: method.to_string(),
            uri_path: uri_path.to_string(),
            query: query.to_string(),
            ..Default::default()
        }
    }

    pub fn with_headers(mut self, headers: Vec<(String, String)>) -> Self {
        self.headers = headers;
        self
    }

    pub fn with_body(mut self, body: &[u8]) -> Self {
        self.body_size_bytes = body.len() as u64;
        let take = body.len().min(DEFAULT_BODY_INSPECTION_LIMIT);
        self.body = body[..take].to_vec();
        self
    }

    pub fn with_source_ip(mut self, ip: IpAddr) -> Self {
        self.source_ip = Some(ip);
        self
    }

    pub fn with_country(mut self, country: Option<String>) -> Self {
        self.country = country;
        self
    }

    /// Resolve the country to feed the evaluator. Precedence: explicit
    /// `country` field, then the `x-fakecloud-geo-country` header.
    fn resolved_country(&self) -> Option<String> {
        if let Some(c) = &self.country {
            return Some(c.clone());
        }
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(FAKECLOUD_GEO_COUNTRY_HEADER))
            .map(|(_, v)| v.clone())
    }
}

/// Snapshot of one resource's WAF state, taken under the read lock so
/// the per-request handler doesn't re-lock during evaluation.
struct ResourceSnapshot {
    web_acl: WebAcl,
    ipsets: HashMap<String, IpSet>,
    regex_sets: HashMap<String, RegexPatternSet>,
    rule_groups: HashMap<String, RuleGroup>,
}

fn snapshot_for_resource(state: &SharedWafv2State, resource_arn: &str) -> Option<ResourceSnapshot> {
    let st = state.read();
    for account in st.accounts.values() {
        let Some(acl_arn) = account.associations.get(resource_arn) else {
            continue;
        };
        let Some(web_acl) = account.web_acls.values().find(|a| &a.arn == acl_arn) else {
            continue;
        };
        let ipsets: HashMap<String, IpSet> = account
            .ip_sets
            .values()
            .map(|s| (s.arn.clone(), s.clone()))
            .collect();
        let regex_sets: HashMap<String, RegexPatternSet> = account
            .regex_pattern_sets
            .values()
            .map(|s| (s.arn.clone(), s.clone()))
            .collect();
        let rule_groups: HashMap<String, RuleGroup> = account
            .rule_groups
            .values()
            .map(|g| (g.arn.clone(), g.clone()))
            .collect();
        return Some(ResourceSnapshot {
            web_acl: web_acl.clone(),
            ipsets,
            regex_sets,
            rule_groups,
        });
    }
    None
}

/// Evaluate `ctx` against the WebACL associated with `resource_arn`.
///
/// `resource_arn` is whatever ARN the caller used in
/// `AssociateWebACL` — for ALB it's the load balancer ARN; for API
/// Gateway v1 it's the stage ARN
/// (`arn:aws:apigateway:<region>::/restapis/<api>/stages/<stage>`);
/// for API Gateway v2 it's the analogous `/apis/<api>/stages/<stage>`
/// shape; for CloudFront it's the distribution ARN.
///
/// `rate_limiter` is the shared in-process [`RateLimiter`] the WAFv2
/// service holds (`Wafv2Service::rate_limiter()`). Passing the same
/// instance every call keeps `RateBasedStatement` counters consistent
/// across the admin endpoint and every dataplane.
///
/// `now_epoch_secs` is taken explicitly so tests can drive the
/// rate-limit clock deterministically. Production callers pass the
/// system clock.
pub fn evaluate_request(
    state: &SharedWafv2State,
    resource_arn: &str,
    ctx: &RequestContext,
    rate_limiter: &Arc<RateLimiter>,
    now_epoch_secs: i64,
) -> Decision {
    let Some(snapshot) = snapshot_for_resource(state, resource_arn) else {
        return Decision::NoAcl;
    };
    let country = ctx.resolved_country();
    // Synthesize a default source IP when the caller has none. Real
    // dataplanes always have a peer IP; the fallback keeps unit tests
    // and admin paths from panicking.
    let source_ip = ctx
        .source_ip
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)));
    let req = WafRequest {
        method: &ctx.method,
        uri: &ctx.uri_path,
        headers: &ctx.headers,
        body: &ctx.body,
        query: &ctx.query,
        source_ip,
        country: country.as_deref(),
        body_size_bytes: ctx.body_size_bytes,
    };
    let verdict = evaluate_web_acl(
        &snapshot.web_acl,
        &req,
        &snapshot.ipsets,
        &snapshot.regex_sets,
        &snapshot.rule_groups,
        rate_limiter,
        now_epoch_secs,
    );
    verdict_to_decision(verdict, snapshot.web_acl.arn)
}

fn verdict_to_decision(verdict: WafVerdict, web_acl_arn: String) -> Decision {
    let WafVerdict {
        action,
        terminating_rule_id,
        labels,
        blocked: _,
        count_rules,
        custom_response_body_key,
        custom_response_status,
    } = verdict;
    match action {
        WafAction::Allow | WafAction::Count => Decision::Allow {
            labels,
            count_rules,
            web_acl_arn,
        },
        WafAction::Block => Decision::Block {
            status: custom_response_status.unwrap_or(403),
            body_key: custom_response_body_key,
            terminating_rule_id,
            labels,
            count_rules,
            web_acl_arn,
        },
        WafAction::Captcha => Decision::Captcha {
            terminating_rule_id,
            labels,
            web_acl_arn,
        },
        WafAction::Challenge => Decision::Challenge {
            terminating_rule_id,
            labels,
            web_acl_arn,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{AccountState, Wafv2Accounts};
    use chrono::Utc;
    use parking_lot::RwLock;
    use serde_json::{json, Value};
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;

    const ACCOUNT: &str = "123456789012";
    const ACL_ARN: &str = "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/xyz";
    const RESOURCE: &str = "arn:aws:apigateway:us-east-1::/restapis/abc/stages/prod";

    fn web_acl_with(default: Value, rules: Vec<Value>) -> WebAcl {
        WebAcl {
            id: "xyz".into(),
            name: "test".into(),
            arn: ACL_ARN.into(),
            scope: "REGIONAL".into(),
            default_action: default,
            description: None,
            rules,
            visibility_config: json!({}),
            capacity: 0,
            lock_token: "lt".into(),
            label_namespace: "awswaf:123456789012:webacl:test:".into(),
            custom_response_bodies: BTreeMap::new(),
            captcha_config: None,
            challenge_config: None,
            token_domains: Vec::new(),
            association_config: None,
            data_protection_config: None,
            on_source_d_do_s_protection_config: None,
            application_config: None,
            retrofitted_by_firewall_manager: false,
            pre_process_firewall_manager_rule_groups: Vec::new(),
            post_process_firewall_manager_rule_groups: Vec::new(),
            managed_by_firewall_manager: false,
            created_time: Utc::now(),
        }
    }

    fn shared(acl: WebAcl, association: Option<&str>) -> SharedWafv2State {
        let mut accounts = Wafv2Accounts::new();
        let mut acct = AccountState::default();
        acct.web_acls
            .insert(("REGIONAL".into(), acl.name.clone()), acl);
        if let Some(resource) = association {
            acct.associations.insert(resource.into(), ACL_ARN.into());
        }
        accounts.accounts.insert(ACCOUNT.into(), acct);
        Arc::new(RwLock::new(accounts))
    }

    fn block_path_rule(needle: &str) -> Value {
        json!({
            "Name": "block-admin",
            "Priority": 0,
            "Action": {"Block": {}},
            "VisibilityConfig": {},
            "Statement": {
                "ByteMatchStatement": {
                    "SearchString": needle,
                    "FieldToMatch": {"UriPath": {}},
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                    "PositionalConstraint": "STARTS_WITH",
                }
            }
        })
    }

    fn count_path_rule(needle: &str) -> Value {
        json!({
            "Name": "count-admin",
            "Priority": 0,
            "Action": {"Count": {}},
            "VisibilityConfig": {},
            "Statement": {
                "ByteMatchStatement": {
                    "SearchString": needle,
                    "FieldToMatch": {"UriPath": {}},
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                    "PositionalConstraint": "STARTS_WITH",
                }
            }
        })
    }

    #[test]
    fn no_association_returns_no_acl() {
        let acl = web_acl_with(json!({"Block": {}}), vec![]);
        let state = shared(acl, None);
        let ctx = RequestContext::new("GET", "/anything", "");
        let limiter = Arc::new(RateLimiter::new());
        let d = evaluate_request(&state, RESOURCE, &ctx, &limiter, 0);
        assert!(matches!(d, Decision::NoAcl));
        assert!(!d.is_short_circuit());
    }

    #[test]
    fn associated_rule_blocks_with_default_403() {
        let acl = web_acl_with(json!({"Allow": {}}), vec![block_path_rule("/admin")]);
        let state = shared(acl, Some(RESOURCE));
        let ctx = RequestContext::new("GET", "/admin/x", "")
            .with_source_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        let limiter = Arc::new(RateLimiter::new());
        let d = evaluate_request(&state, RESOURCE, &ctx, &limiter, 0);
        match d {
            Decision::Block {
                status,
                terminating_rule_id,
                ..
            } => {
                assert_eq!(status, 403);
                assert_eq!(terminating_rule_id.as_deref(), Some("block-admin"));
            }
            other => panic!("expected Block, got {other:?}"),
        }
    }

    #[test]
    fn count_rule_returns_allow_with_count_rules_populated() {
        let acl = web_acl_with(json!({"Allow": {}}), vec![count_path_rule("/admin")]);
        let state = shared(acl, Some(RESOURCE));
        let ctx = RequestContext::new("GET", "/admin/x", "");
        let limiter = Arc::new(RateLimiter::new());
        let d = evaluate_request(&state, RESOURCE, &ctx, &limiter, 0);
        match d {
            Decision::Allow { count_rules, .. } => {
                assert_eq!(count_rules, vec!["count-admin"]);
            }
            other => panic!("expected Allow, got {other:?}"),
        }
    }

    #[test]
    fn default_block_returns_block_decision() {
        let acl = web_acl_with(json!({"Block": {}}), vec![]);
        let state = shared(acl, Some(RESOURCE));
        let ctx = RequestContext::new("GET", "/anything", "");
        let limiter = Arc::new(RateLimiter::new());
        let d = evaluate_request(&state, RESOURCE, &ctx, &limiter, 0);
        assert!(matches!(d, Decision::Block { status: 403, .. }));
    }

    #[test]
    fn body_truncates_to_default_inspection_limit() {
        let big = vec![b'x'; DEFAULT_BODY_INSPECTION_LIMIT + 1024];
        let ctx = RequestContext::new("POST", "/", "").with_body(&big);
        assert_eq!(ctx.body.len(), DEFAULT_BODY_INSPECTION_LIMIT);
        assert_eq!(ctx.body_size_bytes, big.len() as u64);
    }
}
