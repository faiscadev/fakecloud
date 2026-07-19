//! WAFv2 statement evaluation engine.
//!
//! Walks the rules in a [`WebAcl`], matches each rule's statement against an
//! incoming request, and returns the resolved [`WafAction`]. Rules are
//! evaluated in `Priority` order (ascending). The first rule whose statement
//! matches and that has a non-`Count` action terminates evaluation; `Count`
//! rules continue past. When no rule matches, the WebACL's `DefaultAction`
//! decides Allow vs Block.
//!
//! # Public surface
//!
//! - [`evaluate`] / [`evaluate_detailed`]: stateless evaluators that ignore
//!   `RateBasedStatement` (treated as a non-match). Useful when no rate
//!   limiter is available.
//! - [`evaluate_web_acl`]: the full evaluator. Returns a [`WafVerdict`] with
//!   the terminating rule id, accumulated labels, and `blocked` boolean.
//!   Caller-side dataplanes (ALB, API Gateway, CloudFront) consume this.
//!
//! # Rate-based rules
//!
//! [`RateLimiter`] is an in-process token-bucket-style counter keyed on the
//! tuple `(rule_id, aggregate_key_value)`. It is not serialized — counters
//! are best-effort and reset across process restarts, matching how AWS
//! describes its own behaviour ("can pause the rule's rate limiting
//! activities for up to a minute").

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

use base64::Engine;
use parking_lot::Mutex;
use percent_encoding::percent_decode_str;
use regex::Regex;
use serde_json::Value;

use crate::state::{IpSet, RegexPatternSet, RuleGroup, WebAcl};

/// HTTP header name (lowercased) the test admin endpoint uses to inject a
/// synthetic geo country code into the request. Real GeoIP lookup is out of
/// scope for the W1 evaluator; callers in the dataplane resolve country
/// codes themselves before constructing [`WafRequest`].
pub const FAKECLOUD_GEO_COUNTRY_HEADER: &str = "x-fakecloud-geo-country";

/// Default rate-based evaluation window (seconds). AWS allows
/// `EvaluationWindowSec` of 60, 120, 300, or 600 with 300 as the default.
pub const DEFAULT_RATE_WINDOW_SECS: u64 = 300;

/// Request fields evaluated against WAF statements. Borrowing keeps the
/// evaluator allocation-free for the common request paths.
#[derive(Debug, Clone)]
pub struct WafRequest<'a> {
    pub method: &'a str,
    pub uri: &'a str,
    pub headers: &'a [(String, String)],
    pub body: &'a [u8],
    pub query: &'a str,
    pub source_ip: IpAddr,
    pub country: Option<&'a str>,
    /// Total request body byte count. May exceed `body.len()` when the
    /// caller truncates the body for inspection but knows the wire size.
    /// Used by [`SizeConstraintStatement`].
    pub body_size_bytes: u64,
}

impl<'a> WafRequest<'a> {
    /// Construct a request with `body_size_bytes` defaulted to `body.len()`.
    pub fn from_parts(
        method: &'a str,
        uri: &'a str,
        headers: &'a [(String, String)],
        body: &'a [u8],
        query: &'a str,
        source_ip: IpAddr,
    ) -> Self {
        Self {
            method,
            uri,
            headers,
            body,
            query,
            source_ip,
            country: None,
            body_size_bytes: body.len() as u64,
        }
    }
}

/// Resolved action returned by [`evaluate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WafAction {
    Allow,
    Block,
    Count,
    Captcha,
    Challenge,
}

impl WafAction {
    /// Human-readable name matching the AWS action enum keys
    /// (`Allow` / `Block` / `Count` / `Captcha` / `Challenge`).
    pub fn as_str(self) -> &'static str {
        match self {
            WafAction::Allow => "Allow",
            WafAction::Block => "Block",
            WafAction::Count => "Count",
            WafAction::Captcha => "Captcha",
            WafAction::Challenge => "Challenge",
        }
    }
}

/// Detailed evaluation outcome. `action` is the final resolved
/// action (same value [`evaluate`] returns); `count_rules` lists the
/// names of rules whose `Count` action matched on the way to the
/// terminal decision. Useful for emitting per-rule metrics without
/// blocking the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WafEvaluation {
    pub action: WafAction,
    pub count_rules: Vec<String>,
}

/// Full evaluator output consumed by upstream dataplanes (ALB, API Gateway,
/// CloudFront). Includes the rule that terminated evaluation (if any), the
/// labels accumulated by all matching rules, and a convenience `blocked`
/// flag. `Captcha` and `Challenge` map to `blocked=true` since the request
/// is short-circuited with a non-2xx response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WafVerdict {
    pub action: WafAction,
    pub terminating_rule_id: Option<String>,
    pub labels: Vec<String>,
    pub blocked: bool,
    /// Names of rules whose matching `Count` action fired.
    pub count_rules: Vec<String>,
    /// Optional response body keyword from a matching `Block` rule's
    /// `CustomResponse`. Callers look this up in `WebAcl.custom_response_bodies`.
    pub custom_response_body_key: Option<String>,
    /// Optional HTTP status code from a matching `Block` rule's `CustomResponse`.
    /// `None` means the caller should use its default (403 for Block, 405 for
    /// Captcha/Challenge per AWS docs).
    pub custom_response_status: Option<u16>,
}

/// In-memory rate-based-rule counter. Tracks request timestamps per
/// `(rule_id, aggregate_key)` and reports the count within the configured
/// evaluation window on each `record` call. Counters are not serialized; a
/// process restart clears all rate-limit state.
#[derive(Debug, Default)]
pub struct RateLimiter {
    inner: Mutex<HashMap<(String, String), Vec<i64>>>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an event for `key` at `now` (unix epoch seconds), prune events
    /// older than `window_secs`, and return the resulting in-window count.
    pub fn record(&self, rule_id: &str, agg_key: &str, window_secs: u64, now: i64) -> u64 {
        let mut guard = self.inner.lock();
        let bucket = guard
            .entry((rule_id.to_string(), agg_key.to_string()))
            .or_default();
        let cutoff = now - window_secs as i64;
        bucket.retain(|t| *t > cutoff);
        bucket.push(now);
        bucket.len() as u64
    }

    /// Drop all counters. Mostly used by tests.
    pub fn clear(&self) {
        self.inner.lock().clear();
    }
}

/// Evaluate `req` against `web_acl`. Rules are processed in ascending
/// `Priority`; matching `Count` rules are recorded but do not terminate
/// evaluation. The first matching non-`Count` rule's action is returned;
/// otherwise the WebACL's `DefaultAction` is used.
///
/// `RateBasedStatement` rules always evaluate as non-matching here because
/// no [`RateLimiter`] is provided. Use [`evaluate_web_acl`] if you need
/// rate-based enforcement.
pub fn evaluate(
    req: &WafRequest,
    web_acl: &WebAcl,
    ipsets: &HashMap<String, IpSet>,
    regex_sets: &HashMap<String, RegexPatternSet>,
    rule_groups: &HashMap<String, RuleGroup>,
) -> WafAction {
    evaluate_detailed(req, web_acl, ipsets, regex_sets, rule_groups).action
}

/// Like [`evaluate`] but also reports the names of rules whose
/// matching `Count` action fired. The returned `action` is the same
/// value [`evaluate`] would return.
pub fn evaluate_detailed(
    req: &WafRequest,
    web_acl: &WebAcl,
    ipsets: &HashMap<String, IpSet>,
    regex_sets: &HashMap<String, RegexPatternSet>,
    rule_groups: &HashMap<String, RuleGroup>,
) -> WafEvaluation {
    let verdict = evaluate_inner(
        req,
        web_acl,
        ipsets,
        regex_sets,
        rule_groups,
        None,
        current_epoch_secs(),
    );
    WafEvaluation {
        action: verdict.action,
        count_rules: verdict.count_rules,
    }
}

/// Full evaluator. Returns a [`WafVerdict`] capturing the terminating rule
/// id, accumulated labels, and `blocked` flag. Callers consume this to
/// shape the HTTP response (e.g. ALB returns 403 on Block, 405 on Captcha).
///
/// `now_epoch_secs` is taken as a parameter so tests can drive the
/// rate-based clock deterministically.
#[allow(clippy::too_many_arguments)]
pub fn evaluate_web_acl(
    web_acl: &WebAcl,
    request: &WafRequest,
    ipsets: &HashMap<String, IpSet>,
    regex_sets: &HashMap<String, RegexPatternSet>,
    rule_groups: &HashMap<String, RuleGroup>,
    rate_limiter: &RateLimiter,
    now_epoch_secs: i64,
) -> WafVerdict {
    evaluate_inner(
        request,
        web_acl,
        ipsets,
        regex_sets,
        rule_groups,
        Some(rate_limiter),
        now_epoch_secs,
    )
}

impl WebAcl {
    /// Convenience wrapper around [`evaluate`] for callers holding a `&WebAcl`.
    pub fn evaluate(
        &self,
        req: &WafRequest,
        ipsets: &HashMap<String, IpSet>,
        regex_sets: &HashMap<String, RegexPatternSet>,
        rule_groups: &HashMap<String, RuleGroup>,
    ) -> WafAction {
        evaluate(req, self, ipsets, regex_sets, rule_groups)
    }

    /// Convenience wrapper around [`evaluate_detailed`] for callers
    /// holding a `&WebAcl`.
    pub fn evaluate_detailed(
        &self,
        req: &WafRequest,
        ipsets: &HashMap<String, IpSet>,
        regex_sets: &HashMap<String, RegexPatternSet>,
        rule_groups: &HashMap<String, RuleGroup>,
    ) -> WafEvaluation {
        evaluate_detailed(req, self, ipsets, regex_sets, rule_groups)
    }
}

fn current_epoch_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[allow(clippy::too_many_arguments)]
fn evaluate_inner(
    req: &WafRequest,
    web_acl: &WebAcl,
    ipsets: &HashMap<String, IpSet>,
    regex_sets: &HashMap<String, RegexPatternSet>,
    rule_groups: &HashMap<String, RuleGroup>,
    rate_limiter: Option<&RateLimiter>,
    now_epoch_secs: i64,
) -> WafVerdict {
    let mut rules: Vec<&Value> = web_acl.rules.iter().collect();
    rules.sort_by_key(|r| r.get("Priority").and_then(Value::as_i64).unwrap_or(0));

    let mut labels: HashSet<String> = HashSet::new();
    let mut emitted_labels: Vec<String> = Vec::new();
    let mut count_rules: Vec<String> = Vec::new();

    for rule in rules {
        let Some(stmt) = rule.get("Statement") else {
            continue;
        };
        // Customer RuleGroupReferenceStatement rules delegate their terminal
        // decision to the referenced rule group's own rules (they carry an
        // `OverrideAction`, not an `Action`). Handle them before the generic
        // statement dispatch so a matching Block rule inside the group blocks.
        if let Some(rg_ref) = stmt.get("RuleGroupReferenceStatement") {
            if let Some(term) = eval_rule_group_reference(
                rule,
                rg_ref,
                req,
                ipsets,
                regex_sets,
                rule_groups,
                rate_limiter,
                now_epoch_secs,
                &mut labels,
                &mut emitted_labels,
                &mut count_rules,
            ) {
                add_rule_labels(rule, &mut labels, &mut emitted_labels);
                return WafVerdict {
                    action: term.action,
                    terminating_rule_id: Some(term.rule_id),
                    labels: emitted_labels,
                    blocked: matches!(
                        term.action,
                        WafAction::Block | WafAction::Captcha | WafAction::Challenge
                    ),
                    count_rules,
                    custom_response_body_key: term.body_key,
                    custom_response_status: term.status,
                };
            }
            // No terminal action produced by the group — it is non-terminal
            // for the WebACL, continue to the next rule.
            continue;
        }
        let ctx = StmtCtx {
            req,
            ipsets,
            regex_sets,
            labels: &labels,
            rule_id: rule
                .get("Name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            rate_limiter,
            now_epoch_secs,
        };
        if !eval_statement(stmt, &ctx) {
            continue;
        }
        // Add labels produced by this rule so subsequent rules can match.
        add_rule_labels(rule, &mut labels, &mut emitted_labels);
        if let Some(action) = rule.get("Action").and_then(rule_action) {
            // Count is non-terminal: keep evaluating subsequent rules but
            // record the rule for metrics.
            if action == WafAction::Count {
                if let Some(name) = rule.get("Name").and_then(Value::as_str) {
                    count_rules.push(name.to_owned());
                }
                continue;
            }
            let (body_key, status) = block_custom_response(rule);
            let blocked = matches!(
                action,
                WafAction::Block | WafAction::Captcha | WafAction::Challenge
            );
            return WafVerdict {
                action,
                terminating_rule_id: rule.get("Name").and_then(Value::as_str).map(str::to_owned),
                labels: emitted_labels,
                blocked,
                count_rules,
                custom_response_body_key: body_key,
                custom_response_status: status,
            };
        }
        // A rule that matched but carries only an OverrideAction (e.g. a
        // managed rule group reference without an explicit terminal Action)
        // does not terminate here; customer rule group references are expanded
        // and terminated above.
    }

    let default = default_action(web_acl);
    WafVerdict {
        action: default,
        terminating_rule_id: None,
        labels: emitted_labels,
        blocked: default == WafAction::Block,
        count_rules,
        custom_response_body_key: None,
        custom_response_status: None,
    }
}

/// Insert every `RuleLabels[].Name` produced by `rule` into the shared label
/// set, tracking newly-added labels in `emitted_labels` (in insertion order).
fn add_rule_labels(rule: &Value, labels: &mut HashSet<String>, emitted_labels: &mut Vec<String>) {
    if let Some(arr) = rule.get("RuleLabels").and_then(Value::as_array) {
        for label in arr {
            if let Some(name) = label.get("Name").and_then(Value::as_str) {
                if labels.insert(name.to_owned()) {
                    emitted_labels.push(name.to_owned());
                }
            }
        }
    }
}

/// Terminal outcome produced by evaluating a referenced customer rule group.
struct GroupTerminal {
    action: WafAction,
    rule_id: String,
    body_key: Option<String>,
    status: Option<u16>,
}

/// Evaluate a customer `RuleGroupReferenceStatement`.
///
/// Looks up the referenced [`RuleGroup`] by ARN and evaluates its rules in
/// `Priority` order against the request, reusing the same statement machinery
/// as inline WebACL rules. Honors:
///
/// - the reference's `OverrideAction`: `Count` converts every inner rule's
///   action to `Count` (test/monitor mode); `None` lets inner actions apply.
/// - `RuleActionOverrides`: per-rule action replacement by rule name.
/// - `ExcludedRules` (legacy): named inner rules are forced to `Count`.
///
/// Returns `Some(GroupTerminal)` when an inner rule matches with a terminal
/// (non-`Count`) action, or `None` when nothing terminal fired (including a
/// missing rule group — AWS validates the reference at association time, so at
/// evaluation a missing group is a no-op rather than a match).
#[allow(clippy::too_many_arguments)]
fn eval_rule_group_reference(
    outer_rule: &Value,
    rg_ref: &Value,
    req: &WafRequest,
    ipsets: &HashMap<String, IpSet>,
    regex_sets: &HashMap<String, RegexPatternSet>,
    rule_groups: &HashMap<String, RuleGroup>,
    rate_limiter: Option<&RateLimiter>,
    now_epoch_secs: i64,
    labels: &mut HashSet<String>,
    emitted_labels: &mut Vec<String>,
    count_rules: &mut Vec<String>,
) -> Option<GroupTerminal> {
    let arn = rg_ref.get("ARN").and_then(Value::as_str)?;
    let group = rule_groups.get(arn)?;

    // OverrideAction {Count:{}} on the reference forces every inner rule to
    // Count (nothing terminates). {None:{}} is the pass-through default.
    let force_count = outer_rule
        .get("OverrideAction")
        .and_then(Value::as_object)
        .map(|o| o.contains_key("Count"))
        .unwrap_or(false);

    let overrides = rg_ref.get("RuleActionOverrides").and_then(Value::as_array);
    let excluded: HashSet<&str> = rg_ref
        .get("ExcludedRules")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|e| e.get("Name").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default();

    let mut inner_rules: Vec<&Value> = group.rules.iter().collect();
    inner_rules.sort_by_key(|r| r.get("Priority").and_then(Value::as_i64).unwrap_or(0));

    for inner in inner_rules {
        let Some(stmt) = inner.get("Statement") else {
            continue;
        };
        let name = inner
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();

        let matched = {
            let ctx = StmtCtx {
                req,
                ipsets,
                regex_sets,
                labels: &*labels,
                rule_id: name.clone(),
                rate_limiter,
                now_epoch_secs,
            };
            eval_statement(stmt, &ctx)
        };
        if !matched {
            continue;
        }

        add_rule_labels(inner, labels, emitted_labels);

        // Resolve the effective action for this inner rule.
        let mut action = if excluded.contains(name.as_str()) {
            WafAction::Count
        } else if let Some(ov) = override_action_for(overrides, &name) {
            ov
        } else {
            inner
                .get("Action")
                .and_then(rule_action)
                .unwrap_or(WafAction::Count)
        };
        if force_count {
            action = WafAction::Count;
        }

        if action == WafAction::Count {
            count_rules.push(name);
            continue;
        }

        let (body_key, status) = block_custom_response(inner);
        return Some(GroupTerminal {
            action,
            rule_id: name,
            body_key,
            status,
        });
    }
    None
}

/// Resolve a `RuleActionOverrides` entry for the inner rule named `name`.
fn override_action_for(overrides: Option<&Vec<Value>>, name: &str) -> Option<WafAction> {
    let overrides = overrides?;
    overrides
        .iter()
        .find(|o| o.get("Name").and_then(Value::as_str) == Some(name))
        .and_then(|o| o.get("ActionToUse"))
        .and_then(rule_action)
}

fn block_custom_response(rule: &Value) -> (Option<String>, Option<u16>) {
    let action = rule.get("Action").and_then(Value::as_object);
    let Some(action) = action else {
        return (None, None);
    };
    // CustomResponse may live under Block or any other terminal action.
    for key in ["Block", "Captcha", "Challenge"] {
        if let Some(spec) = action.get(key).and_then(|v| v.get("CustomResponse")) {
            let body_key = spec
                .get("CustomResponseBodyKey")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let status = spec
                .get("ResponseCode")
                .and_then(Value::as_u64)
                .and_then(|v| u16::try_from(v).ok());
            return (body_key, status);
        }
    }
    (None, None)
}

fn default_action(web_acl: &WebAcl) -> WafAction {
    if web_acl.default_action.get("Block").is_some() {
        WafAction::Block
    } else {
        WafAction::Allow
    }
}

fn rule_action(action: &Value) -> Option<WafAction> {
    if action.get("Allow").is_some() {
        Some(WafAction::Allow)
    } else if action.get("Block").is_some() {
        Some(WafAction::Block)
    } else if action.get("Count").is_some() {
        Some(WafAction::Count)
    } else if action.get("Captcha").is_some() {
        Some(WafAction::Captcha)
    } else if action.get("Challenge").is_some() {
        Some(WafAction::Challenge)
    } else {
        None
    }
}

// --- Statement dispatch -----------------------------------------------------

/// Bag of references threaded through every recursive `eval_*` call. Cheaper
/// to build once at rule entry than to pass eight separate parameters.
struct StmtCtx<'a> {
    req: &'a WafRequest<'a>,
    ipsets: &'a HashMap<String, IpSet>,
    regex_sets: &'a HashMap<String, RegexPatternSet>,
    labels: &'a HashSet<String>,
    rule_id: String,
    rate_limiter: Option<&'a RateLimiter>,
    now_epoch_secs: i64,
}

fn eval_statement(stmt: &Value, ctx: &StmtCtx) -> bool {
    let Some(obj) = stmt.as_object() else {
        return false;
    };

    if let Some(s) = obj.get("ByteMatchStatement") {
        return eval_byte_match(s, ctx.req);
    }
    if let Some(s) = obj.get("SqliMatchStatement") {
        return eval_sqli_match(s, ctx.req);
    }
    if let Some(s) = obj.get("XssMatchStatement") {
        return eval_xss_match(s, ctx.req);
    }
    if let Some(s) = obj.get("GeoMatchStatement") {
        return eval_geo_match(s, ctx.req);
    }
    if let Some(s) = obj.get("IPSetReferenceStatement") {
        return eval_ipset_ref(s, ctx.req, ctx.ipsets);
    }
    if let Some(s) = obj.get("RegexPatternSetReferenceStatement") {
        return eval_regex_set_ref(s, ctx.req, ctx.regex_sets);
    }
    if let Some(s) = obj.get("RegexMatchStatement") {
        return eval_regex_match(s, ctx.req);
    }
    if let Some(s) = obj.get("AndStatement") {
        return eval_and(s, ctx);
    }
    if let Some(s) = obj.get("OrStatement") {
        return eval_or(s, ctx);
    }
    if let Some(s) = obj.get("NotStatement") {
        return eval_not(s, ctx);
    }
    if let Some(s) = obj.get("LabelMatchStatement") {
        return eval_label_match(s, ctx.labels);
    }
    if let Some(s) = obj.get("SizeConstraintStatement") {
        return eval_size_constraint(s, ctx.req);
    }
    if let Some(s) = obj.get("RateBasedStatement") {
        return eval_rate_based(s, ctx);
    }
    if let Some(s) = obj.get("ManagedRuleGroupStatement") {
        return eval_managed_rule_group(s, ctx.req);
    }

    // RuleGroupReferenceStatement is expanded at the top level of the WebACL
    // loop (see `eval_rule_group_reference`), not here — AWS does not allow a
    // rule group reference nested inside a logical statement, so reaching this
    // arm means the reference is malformed; treat it as a non-match.
    false
}

// --- Logical combinators ----------------------------------------------------

fn eval_and(stmt: &Value, ctx: &StmtCtx) -> bool {
    let Some(arr) = stmt.get("Statements").and_then(Value::as_array) else {
        return false;
    };
    !arr.is_empty() && arr.iter().all(|s| eval_statement(s, ctx))
}

fn eval_or(stmt: &Value, ctx: &StmtCtx) -> bool {
    let Some(arr) = stmt.get("Statements").and_then(Value::as_array) else {
        return false;
    };
    arr.iter().any(|s| eval_statement(s, ctx))
}

fn eval_not(stmt: &Value, ctx: &StmtCtx) -> bool {
    let Some(inner) = stmt.get("Statement") else {
        return false;
    };
    !eval_statement(inner, ctx)
}

// --- Leaf statements --------------------------------------------------------

fn eval_byte_match(stmt: &Value, req: &WafRequest) -> bool {
    let Some(needle_b64) = stmt.get("SearchString").and_then(Value::as_str) else {
        return false;
    };
    // SearchString is base64-encoded over the wire; tolerate raw strings too
    // since callers (and tests) frequently pass them unencoded.
    let needle: Vec<u8> = base64::engine::general_purpose::STANDARD
        .decode(needle_b64)
        .unwrap_or_else(|_| needle_b64.as_bytes().to_vec());
    if needle.is_empty() {
        return false;
    }
    let Some(constraint) = stmt.get("PositionalConstraint").and_then(Value::as_str) else {
        return false;
    };
    let transformations = stmt.get("TextTransformations");
    let fields = collect_fields(stmt.get("FieldToMatch"), req);
    fields.iter().any(|raw| {
        let candidate = apply_transformations(raw, transformations);
        positional_match(&candidate, &needle, constraint)
    })
}

fn positional_match(haystack: &[u8], needle: &[u8], constraint: &str) -> bool {
    match constraint {
        "EXACTLY" => haystack == needle,
        "STARTS_WITH" => haystack.starts_with(needle),
        "ENDS_WITH" => haystack.ends_with(needle),
        "CONTAINS" => bytes_contains(haystack, needle),
        "CONTAINS_WORD" => contains_word(haystack, needle),
        _ => false,
    }
}

fn bytes_contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn contains_word(haystack: &[u8], needle: &[u8]) -> bool {
    if !bytes_contains(haystack, needle) {
        return false;
    }
    // Word boundaries: ascii alnum/underscore on either side disqualifies.
    let n = needle.len();
    haystack.windows(n).enumerate().any(|(i, w)| {
        if w != needle {
            return false;
        }
        let left_ok = i == 0 || !is_word_byte(haystack[i - 1]);
        let right_ok = i + n == haystack.len() || !is_word_byte(haystack[i + n]);
        left_ok && right_ok
    })
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn eval_sqli_match(stmt: &Value, req: &WafRequest) -> bool {
    let transformations = stmt.get("TextTransformations");
    let fields = collect_fields(stmt.get("FieldToMatch"), req);
    let tokens: &[&[u8]] = &[
        b"union select",
        b"or 1=1",
        b"' or '1'='1",
        b"'; drop",
        b"--",
        b"/*",
        b"*/",
        b"xp_cmdshell",
    ];
    fields.iter().any(|raw| {
        let lower = lowercase_bytes(&apply_transformations(raw, transformations));
        tokens.iter().any(|t| bytes_contains(&lower, t))
    })
}

fn eval_xss_match(stmt: &Value, req: &WafRequest) -> bool {
    let transformations = stmt.get("TextTransformations");
    let fields = collect_fields(stmt.get("FieldToMatch"), req);
    let tokens: &[&[u8]] = &[
        b"<script",
        b"</script",
        b"javascript:",
        b"onerror=",
        b"onload=",
        b"onclick=",
        b"<iframe",
    ];
    fields.iter().any(|raw| {
        let lower = lowercase_bytes(&apply_transformations(raw, transformations));
        tokens.iter().any(|t| bytes_contains(&lower, t))
    })
}

fn eval_geo_match(stmt: &Value, req: &WafRequest) -> bool {
    let Some(country) = req.country else {
        return false;
    };
    let Some(arr) = stmt.get("CountryCodes").and_then(Value::as_array) else {
        return false;
    };
    arr.iter()
        .filter_map(Value::as_str)
        .any(|c| c.eq_ignore_ascii_case(country))
}

fn eval_ipset_ref(stmt: &Value, req: &WafRequest, ipsets: &HashMap<String, IpSet>) -> bool {
    let Some(arn) = stmt.get("ARN").and_then(Value::as_str) else {
        return false;
    };
    let Some(set) = ipsets.get(arn) else {
        return false;
    };
    set.addresses
        .iter()
        .any(|cidr| cidr_contains(cidr, &req.source_ip))
}

fn eval_regex_set_ref(
    stmt: &Value,
    req: &WafRequest,
    regex_sets: &HashMap<String, RegexPatternSet>,
) -> bool {
    let Some(arn) = stmt.get("ARN").and_then(Value::as_str) else {
        return false;
    };
    let Some(set) = regex_sets.get(arn) else {
        return false;
    };
    let transformations = stmt.get("TextTransformations");
    let fields = collect_fields(stmt.get("FieldToMatch"), req);
    let patterns: Vec<Regex> = set
        .regular_expressions
        .iter()
        .filter_map(|p| p.get("RegexString").and_then(Value::as_str))
        .filter_map(|s| Regex::new(s).ok())
        .collect();
    if patterns.is_empty() {
        return false;
    }
    fields.iter().any(|raw| {
        let candidate = apply_transformations(raw, transformations);
        let Ok(text) = std::str::from_utf8(&candidate) else {
            return false;
        };
        patterns.iter().any(|r| r.is_match(text))
    })
}

fn eval_regex_match(stmt: &Value, req: &WafRequest) -> bool {
    let Some(pattern) = stmt.get("RegexString").and_then(Value::as_str) else {
        return false;
    };
    let Ok(re) = Regex::new(pattern) else {
        return false;
    };
    let transformations = stmt.get("TextTransformations");
    let fields = collect_fields(stmt.get("FieldToMatch"), req);
    fields.iter().any(|raw| {
        let candidate = apply_transformations(raw, transformations);
        std::str::from_utf8(&candidate)
            .map(|t| re.is_match(t))
            .unwrap_or(false)
    })
}

fn eval_label_match(stmt: &Value, labels: &HashSet<String>) -> bool {
    let Some(key) = stmt.get("Key").and_then(Value::as_str) else {
        return false;
    };
    let scope = stmt.get("Scope").and_then(Value::as_str).unwrap_or("LABEL");
    labels.iter().any(|l| match scope {
        // NAMESPACE: match any label whose namespace prefix equals `key`.
        "NAMESPACE" => l.starts_with(key),
        _ => l == key,
    })
}

fn eval_size_constraint(stmt: &Value, req: &WafRequest) -> bool {
    let Some(op) = stmt.get("ComparisonOperator").and_then(Value::as_str) else {
        return false;
    };
    let Some(size) = stmt.get("Size").and_then(Value::as_i64) else {
        return false;
    };
    if size < 0 {
        return false;
    }
    let target_size = field_size(
        stmt.get("FieldToMatch"),
        req,
        stmt.get("TextTransformations"),
    );
    let lhs = target_size as i64;
    match op {
        "EQ" => lhs == size,
        "NE" => lhs != size,
        "LE" => lhs <= size,
        "LT" => lhs < size,
        "GE" => lhs >= size,
        "GT" => lhs > size,
        _ => false,
    }
}

fn field_size(field: Option<&Value>, req: &WafRequest, xforms: Option<&Value>) -> u64 {
    let Some(field) = field else {
        return 0;
    };
    let Some(obj) = field.as_object() else {
        return 0;
    };
    if obj.contains_key("Body") || obj.contains_key("JsonBody") {
        // Use the wire-level body byte count, not the (possibly truncated)
        // inspected body — AWS evaluates the full body length here.
        return req.body_size_bytes;
    }
    // For other fields, evaluate on the transformed bytes returned by
    // `collect_fields` so size comparisons honor `TextTransformations`.
    collect_fields(Some(field), req)
        .iter()
        .map(|raw| apply_transformations(raw, xforms).len() as u64)
        .max()
        .unwrap_or(0)
}

/// Result of resolving the aggregation key for a rate-based rule.
/// `Key` carries the actual aggregate key value; `Fallback(b)` means the
/// configured input (typically a `FORWARDED_IP` header) was missing or
/// malformed and the caller should honor the rule's `FallbackBehavior`.
enum AggregateKey {
    Key(String),
    Fallback(bool),
}

fn eval_rate_based(stmt: &Value, ctx: &StmtCtx) -> bool {
    // No limiter -> we cannot count, treat as non-match. Callers that need
    // rate limiting use `evaluate_web_acl`.
    let Some(limiter) = ctx.rate_limiter else {
        return false;
    };
    let Some(limit) = stmt.get("Limit").and_then(Value::as_u64) else {
        return false;
    };
    if limit == 0 {
        return false;
    }
    let window = stmt
        .get("EvaluationWindowSec")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_RATE_WINDOW_SECS);
    // Optional scope-down statement: only count requests that match it.
    if let Some(scope_down) = stmt.get("ScopeDownStatement") {
        if !eval_statement(scope_down, ctx) {
            return false;
        }
    }
    let agg = stmt
        .get("AggregateKeyType")
        .and_then(Value::as_str)
        .unwrap_or("IP");
    let agg_key = match rate_aggregate_key(agg, ctx.req, stmt) {
        AggregateKey::Key(k) => k,
        // Per AWS docs: when the configured source (FORWARDED_IP header)
        // is missing or contains a malformed IP, `FallbackBehavior=MATCH`
        // counts the request as a match and `NO_MATCH` skips the rule.
        // The aggregation key resolution short-circuits to the boolean.
        AggregateKey::Fallback(b) => return b,
    };
    let count = limiter.record(&ctx.rule_id, &agg_key, window, ctx.now_epoch_secs);
    count > limit
}

fn rate_aggregate_key(agg: &str, req: &WafRequest, stmt: &Value) -> AggregateKey {
    match agg {
        "IP" => AggregateKey::Key(req.source_ip.to_string()),
        "FORWARDED_IP" => {
            let cfg = stmt.get("ForwardedIPConfig");
            let header_name = cfg
                .and_then(|c| c.get("HeaderName"))
                .and_then(Value::as_str)
                .unwrap_or("X-Forwarded-For");
            let fallback_match = cfg
                .and_then(|c| c.get("FallbackBehavior"))
                .and_then(Value::as_str)
                .map(|b| b.eq_ignore_ascii_case("MATCH"))
                .unwrap_or(false);
            let raw = req
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(header_name))
                .map(|(_, v)| {
                    // X-Forwarded-For is a comma-separated chain; the
                    // client-asserted client IP is the leftmost entry.
                    v.split(',').next().unwrap_or(v).trim().to_string()
                });
            match raw {
                None => AggregateKey::Fallback(fallback_match),
                Some(v) if v.parse::<std::net::IpAddr>().is_err() => {
                    AggregateKey::Fallback(fallback_match)
                }
                Some(v) => AggregateKey::Key(v),
            }
        }
        "CONSTANT" => AggregateKey::Key("__constant__".to_string()),
        // CUSTOM_KEYS aggregates on user-defined fields. We join all
        // resolvable key values into a single bucket id; if none of the
        // configured keys can be resolved, fall through to non-match
        // (CustomKeys has no `FallbackBehavior` knob — AWS just skips).
        "CUSTOM_KEYS" => {
            let Some(arr) = stmt.get("CustomKeys").and_then(Value::as_array) else {
                return AggregateKey::Fallback(false);
            };
            let parts: Vec<String> = arr
                .iter()
                .filter_map(|k| custom_key_value(k, req))
                .collect();
            if parts.is_empty() {
                AggregateKey::Fallback(false)
            } else {
                AggregateKey::Key(parts.join("|"))
            }
        }
        _ => AggregateKey::Fallback(false),
    }
}

fn custom_key_value(key: &Value, req: &WafRequest) -> Option<String> {
    if let Some(h) = key.get("Header") {
        let name = h.get("Name").and_then(Value::as_str)?;
        return req
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone());
    }
    if let Some(q) = key.get("QueryArgument") {
        let name = q.get("Name").and_then(Value::as_str)?;
        let vals = query_arg_values(req.query, name);
        if vals.is_empty() {
            return None;
        }
        return String::from_utf8(vals[0].clone()).ok();
    }
    if key.get("HTTPMethod").is_some() {
        return Some(req.method.to_string());
    }
    if key.get("UriPath").is_some() {
        return Some(req.uri.to_string());
    }
    if key.get("IP").is_some() {
        return Some(req.source_ip.to_string());
    }
    None
}

/// Canned managed rule groups. AWSManagedRulesCommonRuleSet is a stub that
/// flags the well-known noisy admin paths so tests can exercise the
/// `ManagedRuleGroupStatement` code path without us redistributing the real
/// AWS rule set.
fn eval_managed_rule_group(stmt: &Value, req: &WafRequest) -> bool {
    let vendor = stmt.get("VendorName").and_then(Value::as_str).unwrap_or("");
    let name = stmt.get("Name").and_then(Value::as_str).unwrap_or("");
    if !vendor.eq_ignore_ascii_case("AWS") {
        return false;
    }
    match name {
        "AWSManagedRulesCommonRuleSet" => {
            // Match a small allowlist of admin-y paths plus an obvious
            // CRS canary header.
            common_ruleset_match(req)
        }
        "AWSManagedRulesSQLiRuleSet" => {
            let fields = collect_fields(Some(&serde_json::json!({"QueryString": {}})), req);
            let tokens: &[&[u8]] = &[b"union select", b"or 1=1", b"' or '1'='1", b"--"];
            fields.iter().any(|raw| {
                let lower = lowercase_bytes(raw);
                tokens.iter().any(|t| bytes_contains(&lower, t))
            })
        }
        "AWSManagedRulesKnownBadInputsRuleSet" => req
            .headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("user-agent") && v.is_empty()),
        _ => false,
    }
}

fn common_ruleset_match(req: &WafRequest) -> bool {
    // Conservative subset; mirrors what the public CRS docs flag as
    // "noisy admin paths" plus a CRS canary header.
    const ADMIN_PREFIXES: &[&str] = &[
        "/admin",
        "/wp-admin",
        "/phpmyadmin",
        "/.env",
        "/.git",
        "/cgi-bin/",
    ];
    let uri = req.uri;
    if ADMIN_PREFIXES.iter().any(|p| uri.starts_with(p)) {
        return true;
    }
    req.headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("x-crs-test") && !v.is_empty())
}

// --- FieldToMatch + TextTransformations -------------------------------------

fn collect_fields(field: Option<&Value>, req: &WafRequest) -> Vec<Vec<u8>> {
    let Some(field) = field else {
        return Vec::new();
    };
    let Some(obj) = field.as_object() else {
        return Vec::new();
    };

    if obj.contains_key("Method") {
        return vec![req.method.as_bytes().to_vec()];
    }
    if obj.contains_key("UriPath") {
        return vec![req.uri.as_bytes().to_vec()];
    }
    if obj.contains_key("QueryString") {
        return vec![req.query.as_bytes().to_vec()];
    }
    if obj.contains_key("Body") || obj.contains_key("JsonBody") {
        return vec![req.body.to_vec()];
    }
    if let Some(sh) = obj.get("SingleHeader") {
        if let Some(name) = sh.get("Name").and_then(Value::as_str) {
            return req
                .headers
                .iter()
                .filter(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_bytes().to_vec())
                .collect();
        }
    }
    if obj.contains_key("AllHeaders") || obj.contains_key("Headers") {
        return req
            .headers
            .iter()
            .map(|(k, v)| format!("{k}:{v}").into_bytes())
            .collect();
    }
    if let Some(sqa) = obj.get("SingleQueryArgument") {
        if let Some(name) = sqa.get("Name").and_then(Value::as_str) {
            return query_arg_values(req.query, name);
        }
    }
    // Cookies, JsonBody match-pattern, HeaderOrder, JA3Fingerprint, etc.
    // are intentionally not modelled in W1 — add as later phases need them.
    Vec::new()
}

fn query_arg_values(query: &str, name: &str) -> Vec<Vec<u8>> {
    query
        .split('&')
        .filter_map(|kv| {
            let mut parts = kv.splitn(2, '=');
            let k = parts.next()?;
            let v = parts.next().unwrap_or("");
            if k.eq_ignore_ascii_case(name) {
                Some(v.as_bytes().to_vec())
            } else {
                None
            }
        })
        .collect()
}

fn apply_transformations(raw: &[u8], xforms: Option<&Value>) -> Vec<u8> {
    let Some(arr) = xforms.and_then(Value::as_array) else {
        return raw.to_vec();
    };
    let mut ordered: Vec<&Value> = arr.iter().collect();
    ordered.sort_by_key(|t| t.get("Priority").and_then(Value::as_i64).unwrap_or(0));
    let mut current = raw.to_vec();
    for t in ordered {
        let Some(kind) = t.get("Type").and_then(Value::as_str) else {
            continue;
        };
        current = match kind {
            "NONE" => current,
            "LOWERCASE" => lowercase_bytes(&current),
            "URL_DECODE" => url_decode_bytes(&current),
            "COMPRESS_WHITE_SPACE" => compress_whitespace(&current),
            // HTML_ENTITY_DECODE, CMD_LINE etc. fall through to NONE; add
            // when a real customer config actually exercises them.
            _ => current,
        };
    }
    current
}

fn lowercase_bytes(input: &[u8]) -> Vec<u8> {
    input.iter().map(|b| b.to_ascii_lowercase()).collect()
}

fn url_decode_bytes(input: &[u8]) -> Vec<u8> {
    let Ok(s) = std::str::from_utf8(input) else {
        return input.to_vec();
    };
    percent_decode_str(s).collect()
}

fn compress_whitespace(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut last_was_ws = false;
    for &b in input {
        let is_ws = matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b);
        if is_ws {
            if !last_was_ws {
                out.push(b' ');
            }
            last_was_ws = true;
        } else {
            out.push(b);
            last_was_ws = false;
        }
    }
    out
}

// --- CIDR matching (no extra deps) -----------------------------------------

fn cidr_contains(cidr: &str, ip: &IpAddr) -> bool {
    let Some((net_str, prefix_str)) = cidr.split_once('/') else {
        // Bare IP — exact match.
        return net_str_eq(cidr, ip);
    };
    let Ok(prefix) = prefix_str.parse::<u8>() else {
        return false;
    };
    match (net_str.parse::<IpAddr>(), ip) {
        (Ok(IpAddr::V4(net)), IpAddr::V4(addr)) if prefix <= 32 => {
            mask_match(&net.octets(), &addr.octets(), prefix)
        }
        (Ok(IpAddr::V6(net)), IpAddr::V6(addr)) if prefix <= 128 => {
            mask_match(&net.octets(), &addr.octets(), prefix)
        }
        _ => false,
    }
}

fn net_str_eq(s: &str, ip: &IpAddr) -> bool {
    s.parse::<IpAddr>().map(|p| p == *ip).unwrap_or(false)
}

fn mask_match(net: &[u8], addr: &[u8], prefix: u8) -> bool {
    let full_bytes = (prefix / 8) as usize;
    let extra_bits = prefix % 8;
    if net.len() != addr.len() || full_bytes > net.len() {
        return false;
    }
    if net[..full_bytes] != addr[..full_bytes] {
        return false;
    }
    if extra_bits == 0 {
        return true;
    }
    let mask = 0xffu8 << (8 - extra_bits);
    (net[full_bytes] & mask) == (addr[full_bytes] & mask)
}

// --- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;

    fn make_acl(default: Value, rules: Vec<Value>) -> WebAcl {
        WebAcl {
            id: "id".into(),
            name: "acl".into(),
            arn: "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/acl/id".into(),
            scope: "REGIONAL".into(),
            default_action: default,
            description: None,
            rules,
            visibility_config: json!({}),
            capacity: 0,
            lock_token: "lt".into(),
            label_namespace: "awswaf:000000000000:webacl:acl:".into(),
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

    fn req(uri: &'static str) -> WafRequest<'static> {
        WafRequest {
            method: "GET",
            uri,
            headers: &[],
            body: b"",
            query: "",
            source_ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            country: None,
            body_size_bytes: 0,
        }
    }

    fn byte_match_uri_contains(needle: &str, action: Value) -> Value {
        json!({
            "Name": "r",
            "Priority": 0,
            "Action": action,
            "VisibilityConfig": {},
            "Statement": {
                "ByteMatchStatement": {
                    "SearchString": needle,
                    "FieldToMatch": {"UriPath": {}},
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                    "PositionalConstraint": "CONTAINS",
                }
            }
        })
    }

    #[test]
    fn byte_match_contains_returns_block_when_default_allow_with_match() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![byte_match_uri_contains("/admin", json!({"Block": {}}))],
        );
        let action = evaluate(
            &req("/admin/users"),
            &acl,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        assert_eq!(action, WafAction::Block);
    }

    #[test]
    fn byte_match_no_match_returns_default_allow() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![byte_match_uri_contains("/admin", json!({"Block": {}}))],
        );
        let action = evaluate(
            &req("/public"),
            &acl,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        assert_eq!(action, WafAction::Allow);
    }

    #[test]
    fn byte_match_exactly_post_admin() {
        // POST /admin matches EXACTLY against the literal string "POST /admin"
        // when the FieldToMatch is SingleHeader synthesised; for this test
        // we synthesise the haystack via a method+uri combo header.
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "exactly",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"ByteMatchStatement": {
                    "SearchString": "/admin",
                    "FieldToMatch": {"UriPath": {}},
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                    "PositionalConstraint": "EXACTLY",
                }},
            })],
        );
        let mut r = req("/admin");
        r.method = "POST";
        assert_eq!(
            evaluate(&r, &acl, &HashMap::new(), &HashMap::new(), &HashMap::new()),
            WafAction::Block,
            "POST /admin EXACTLY should block"
        );
        assert_eq!(
            evaluate(
                &req("/admin/users"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Allow,
            "/admin/users is not EXACTLY /admin"
        );
    }

    #[test]
    fn ip_set_match_blocks_listed_ip() {
        let arn = "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/blocked/abc".to_string();
        let mut sets = HashMap::new();
        sets.insert(
            arn.clone(),
            IpSet {
                id: "abc".into(),
                name: "blocked".into(),
                arn: arn.clone(),
                scope: "REGIONAL".into(),
                description: None,
                ip_address_version: "IPV4".into(),
                addresses: vec!["10.0.0.0/8".into()],
                lock_token: "lt".into(),
                created_time: Utc::now(),
            },
        );
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "r",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"IPSetReferenceStatement": {"ARN": arn}},
            })],
        );
        let mut r = req("/");
        r.source_ip = IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3));
        assert_eq!(
            evaluate(&r, &acl, &sets, &HashMap::new(), &HashMap::new()),
            WafAction::Block
        );
    }

    #[test]
    fn geo_match_country_code_blocks() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "r",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"GeoMatchStatement": {"CountryCodes": ["DE"]}},
            })],
        );
        let mut r = req("/");
        r.country = Some("DE");
        assert_eq!(
            evaluate(&r, &acl, &HashMap::new(), &HashMap::new(), &HashMap::new()),
            WafAction::Block,
            "country=DE matches"
        );
        let mut r2 = req("/");
        r2.country = Some("US");
        assert_eq!(
            evaluate(&r2, &acl, &HashMap::new(), &HashMap::new(), &HashMap::new()),
            WafAction::Allow,
            "country=US does not match"
        );
        // No country -> no match
        assert_eq!(
            evaluate(
                &req("/"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Allow,
            "missing country header -> no match"
        );
    }

    #[test]
    fn regex_match_uri_path() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "r",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"RegexMatchStatement": {
                    "RegexString": "^/api/v[0-9]+/admin$",
                    "FieldToMatch": {"UriPath": {}},
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                }},
            })],
        );
        assert_eq!(
            evaluate(
                &req("/api/v2/admin"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Block
        );
        assert_eq!(
            evaluate(
                &req("/api/v2/admin/x"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Allow
        );
    }

    #[test]
    fn and_statement_requires_all() {
        // Byte match would hit, but geo match needs country=US which the request lacks.
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "r",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"AndStatement": {"Statements": [
                    {"ByteMatchStatement": {
                        "SearchString": "/admin",
                        "FieldToMatch": {"UriPath": {}},
                        "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                        "PositionalConstraint": "CONTAINS",
                    }},
                    {"GeoMatchStatement": {"CountryCodes": ["US"]}},
                ]}},
            })],
        );
        assert_eq!(
            evaluate(
                &req("/admin"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Allow
        );
    }

    #[test]
    fn or_statement_takes_first_match() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "r",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"OrStatement": {"Statements": [
                    {"ByteMatchStatement": {
                        "SearchString": "/admin",
                        "FieldToMatch": {"UriPath": {}},
                        "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                        "PositionalConstraint": "CONTAINS",
                    }},
                    {"GeoMatchStatement": {"CountryCodes": ["US"]}},
                ]}},
            })],
        );
        assert_eq!(
            evaluate(
                &req("/admin/x"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Block
        );
    }

    #[test]
    fn not_statement_inverts() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "r",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"NotStatement": {"Statement": {
                    "ByteMatchStatement": {
                        "SearchString": "/admin",
                        "FieldToMatch": {"UriPath": {}},
                        "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                        "PositionalConstraint": "CONTAINS",
                    }
                }}},
            })],
        );
        // Inner ByteMatch fails, NOT inverts to true -> Block.
        assert_eq!(
            evaluate(
                &req("/public"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Block
        );
    }

    #[test]
    fn regex_pattern_set_reference() {
        let arn =
            "arn:aws:wafv2:us-east-1:000000000000:regional/regexpatternset/rps/abc".to_string();
        let mut sets = HashMap::new();
        sets.insert(
            arn.clone(),
            RegexPatternSet {
                id: "abc".into(),
                name: "rps".into(),
                arn: arn.clone(),
                scope: "REGIONAL".into(),
                description: None,
                regular_expressions: vec![
                    json!({"RegexString": "^/admin"}),
                    json!({"RegexString": "^/internal"}),
                ],
                lock_token: "lt".into(),
                created_time: Utc::now(),
            },
        );
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "r",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"RegexPatternSetReferenceStatement": {
                    "ARN": arn,
                    "FieldToMatch": {"UriPath": {}},
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                }},
            })],
        );
        assert_eq!(
            evaluate(
                &req("/internal/x"),
                &acl,
                &HashMap::new(),
                &sets,
                &HashMap::new()
            ),
            WafAction::Block
        );
    }

    #[test]
    fn default_action_block_when_no_rules_match_and_default_block() {
        let acl = make_acl(json!({"Block": {}}), vec![]);
        assert_eq!(
            evaluate(
                &req("/anything"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Block
        );
    }

    #[test]
    fn count_action_does_not_terminate() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![byte_match_uri_contains("/admin", json!({"Count": {}})), {
                let mut r = byte_match_uri_contains("/admin", json!({"Block": {}}));
                r["Priority"] = json!(1);
                r["Name"] = json!("r2");
                r
            }],
        );
        assert_eq!(
            evaluate(
                &req("/admin/x"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Block
        );
    }

    #[test]
    fn label_match_after_earlier_rule_emits_label() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![
                json!({
                    "Name": "tag",
                    "Priority": 0,
                    "Action": {"Count": {}},
                    "VisibilityConfig": {},
                    "RuleLabels": [{"Name": "awswaf:custom:admin"}],
                    "Statement": {"ByteMatchStatement": {
                        "SearchString": "/admin",
                        "FieldToMatch": {"UriPath": {}},
                        "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                        "PositionalConstraint": "CONTAINS",
                    }},
                }),
                json!({
                    "Name": "block-by-label",
                    "Priority": 1,
                    "Action": {"Block": {}},
                    "VisibilityConfig": {},
                    "Statement": {"LabelMatchStatement": {
                        "Scope": "LABEL",
                        "Key": "awswaf:custom:admin",
                    }},
                }),
            ],
        );
        assert_eq!(
            evaluate(
                &req("/admin/x"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Block
        );
    }

    // --- New W1 tests -----------------------------------------------------

    #[test]
    fn size_constraint_body_too_large_blocks() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "size",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"SizeConstraintStatement": {
                    "FieldToMatch": {"Body": {}},
                    "ComparisonOperator": "GT",
                    "Size": 1024,
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                }},
            })],
        );
        let mut r = req("/upload");
        r.body_size_bytes = 2048;
        assert_eq!(
            evaluate(&r, &acl, &HashMap::new(), &HashMap::new(), &HashMap::new()),
            WafAction::Block
        );
        let mut r_small = req("/upload");
        r_small.body_size_bytes = 512;
        assert_eq!(
            evaluate(
                &r_small,
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Allow
        );
    }

    #[test]
    fn size_constraint_uri_path_le() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "small-uri",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"SizeConstraintStatement": {
                    "FieldToMatch": {"UriPath": {}},
                    "ComparisonOperator": "LT",
                    "Size": 5,
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                }},
            })],
        );
        // /api is 4 bytes -> blocks
        assert_eq!(
            evaluate(
                &req("/api"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Block
        );
        assert_eq!(
            evaluate(
                &req("/admin"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Allow
        );
    }

    #[test]
    fn rate_based_blocks_after_limit() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "rate",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"RateBasedStatement": {
                    "Limit": 1000,
                    "AggregateKeyType": "IP",
                    "EvaluationWindowSec": 300,
                }},
            })],
        );
        let limiter = RateLimiter::new();
        let now = 1_700_000_000;
        let mut r = req("/api");
        r.source_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        // First 1000 requests should be allowed.
        for _ in 0..1000 {
            let v = evaluate_web_acl(
                &acl,
                &r,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
                &limiter,
                now,
            );
            assert_eq!(v.action, WafAction::Allow);
            assert!(!v.blocked);
        }
        // 1001st must trigger the block.
        let v = evaluate_web_acl(
            &acl,
            &r,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            now,
        );
        assert_eq!(v.action, WafAction::Block);
        assert_eq!(v.terminating_rule_id.as_deref(), Some("rate"));
        assert!(v.blocked);
    }

    #[test]
    fn rate_based_window_rolls_over() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "rate",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"RateBasedStatement": {
                    "Limit": 2,
                    "AggregateKeyType": "IP",
                    "EvaluationWindowSec": 300,
                }},
            })],
        );
        let limiter = RateLimiter::new();
        let r = req("/api");
        let t0 = 1_700_000_000;
        evaluate_web_acl(
            &acl,
            &r,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            t0,
        );
        evaluate_web_acl(
            &acl,
            &r,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            t0,
        );
        let v3 = evaluate_web_acl(
            &acl,
            &r,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            t0,
        );
        assert_eq!(v3.action, WafAction::Block, "3rd in window blocks");
        // Roll the clock past the window — counters expire, request is allowed again.
        let later = t0 + 301;
        let v4 = evaluate_web_acl(
            &acl,
            &r,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            later,
        );
        assert_eq!(v4.action, WafAction::Allow, "after window rolls, allowed");
    }

    #[test]
    fn rate_based_per_ip_independent_buckets() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "rate",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"RateBasedStatement": {
                    "Limit": 1,
                    "AggregateKeyType": "IP",
                    "EvaluationWindowSec": 60,
                }},
            })],
        );
        let limiter = RateLimiter::new();
        let now = 1_700_000_000;
        let mut a = req("/api");
        a.source_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let mut b = req("/api");
        b.source_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        // 1st request from each IP allowed.
        assert_eq!(
            evaluate_web_acl(
                &acl,
                &a,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
                &limiter,
                now
            )
            .action,
            WafAction::Allow
        );
        assert_eq!(
            evaluate_web_acl(
                &acl,
                &b,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
                &limiter,
                now
            )
            .action,
            WafAction::Allow
        );
        // 2nd from a blocks, but b's counter is independent.
        assert_eq!(
            evaluate_web_acl(
                &acl,
                &a,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
                &limiter,
                now
            )
            .action,
            WafAction::Block
        );
        // b's second still goes over its own limit too (Limit=1, so 2nd
        // blocks). The check is that it isn't *prematurely* blocked by a's
        // bucket — we already proved the first b succeeded above.
        assert_eq!(
            evaluate_web_acl(
                &acl,
                &b,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
                &limiter,
                now
            )
            .action,
            WafAction::Block
        );
    }

    #[test]
    fn rate_based_forwarded_ip_missing_header_no_match_default() {
        // No FallbackBehavior configured -> defaults to NO_MATCH so the
        // rule does not fire when the header is missing.
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "rate",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"RateBasedStatement": {
                    "Limit": 1,
                    "AggregateKeyType": "FORWARDED_IP",
                    "EvaluationWindowSec": 60,
                    "ForwardedIPConfig": {"HeaderName": "X-Forwarded-For"},
                }},
            })],
        );
        let limiter = RateLimiter::new();
        // No XFF header on the request.
        let v = evaluate_web_acl(
            &acl,
            &req("/"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            0,
        );
        assert_eq!(
            v.action,
            WafAction::Allow,
            "missing XFF defaults to NO_MATCH"
        );
    }

    #[test]
    fn rate_based_forwarded_ip_missing_header_with_match_fallback_blocks() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "rate",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"RateBasedStatement": {
                    "Limit": 1,
                    "AggregateKeyType": "FORWARDED_IP",
                    "EvaluationWindowSec": 60,
                    "ForwardedIPConfig": {
                        "HeaderName": "X-Forwarded-For",
                        "FallbackBehavior": "MATCH"
                    },
                }},
            })],
        );
        let limiter = RateLimiter::new();
        let v = evaluate_web_acl(
            &acl,
            &req("/"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            0,
        );
        assert_eq!(
            v.action,
            WafAction::Block,
            "missing XFF with FallbackBehavior=MATCH blocks regardless of count"
        );
    }

    #[test]
    fn rate_based_forwarded_ip_malformed_value_uses_fallback_not_counter() {
        // Without the malformed-IP fallback fix, a request whose XFF header
        // is garbage would still rate-limit on the garbage string. With the
        // fix, FallbackBehavior=NO_MATCH skips and FallbackBehavior=MATCH
        // blocks unconditionally.
        let mk_acl = |fb: &str| {
            make_acl(
                json!({"Allow": {}}),
                vec![json!({
                    "Name": "rate",
                    "Priority": 0,
                    "Action": {"Block": {}},
                    "VisibilityConfig": {},
                    "Statement": {"RateBasedStatement": {
                        "Limit": 1000,
                        "AggregateKeyType": "FORWARDED_IP",
                        "EvaluationWindowSec": 60,
                        "ForwardedIPConfig": {
                            "HeaderName": "X-Forwarded-For",
                            "FallbackBehavior": fb,
                        },
                    }},
                })],
            )
        };
        let limiter = RateLimiter::new();
        let headers = vec![("X-Forwarded-For".to_string(), "not-an-ip".to_string())];
        let r = WafRequest {
            method: "GET",
            uri: "/",
            headers: &headers,
            body: b"",
            query: "",
            source_ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            country: None,
            body_size_bytes: 0,
        };
        let acl_no = mk_acl("NO_MATCH");
        assert_eq!(
            evaluate_web_acl(
                &acl_no,
                &r,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
                &limiter,
                0
            )
            .action,
            WafAction::Allow,
            "malformed XFF + NO_MATCH falls through"
        );
        let acl_match = mk_acl("MATCH");
        assert_eq!(
            evaluate_web_acl(
                &acl_match,
                &r,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
                &limiter,
                0
            )
            .action,
            WafAction::Block,
            "malformed XFF + MATCH blocks even though limit is 1000"
        );
    }

    #[test]
    fn rate_based_forwarded_ip_aggregate() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "rate",
                "Priority": 0,
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"RateBasedStatement": {
                    "Limit": 1,
                    "AggregateKeyType": "FORWARDED_IP",
                    "EvaluationWindowSec": 60,
                    "ForwardedIPConfig": {"HeaderName": "X-Forwarded-For", "FallbackBehavior": "MATCH"},
                }},
            })],
        );
        let limiter = RateLimiter::new();
        let now = 1_700_000_000;
        let headers = vec![("X-Forwarded-For".to_string(), "203.0.113.5".to_string())];
        let r = WafRequest {
            method: "GET",
            uri: "/",
            headers: &headers,
            body: b"",
            query: "",
            source_ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            country: None,
            body_size_bytes: 0,
        };
        assert_eq!(
            evaluate_web_acl(
                &acl,
                &r,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
                &limiter,
                now
            )
            .action,
            WafAction::Allow
        );
        assert_eq!(
            evaluate_web_acl(
                &acl,
                &r,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
                &limiter,
                now
            )
            .action,
            WafAction::Block
        );
    }

    #[test]
    fn managed_rule_group_common_set_blocks_admin() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "crs",
                "Priority": 0,
                "OverrideAction": {"None": {}},
                "Action": {"Block": {}},
                "VisibilityConfig": {},
                "Statement": {"ManagedRuleGroupStatement": {
                    "VendorName": "AWS",
                    "Name": "AWSManagedRulesCommonRuleSet",
                }},
            })],
        );
        assert_eq!(
            evaluate(
                &req("/admin/users"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Block
        );
        assert_eq!(
            evaluate(
                &req("/wp-admin"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Block
        );
        assert_eq!(
            evaluate(
                &req("/index.html"),
                &acl,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new()
            ),
            WafAction::Allow
        );
    }

    #[test]
    fn captcha_action_marks_blocked() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![byte_match_uri_contains("/admin", json!({"Captcha": {}}))],
        );
        let limiter = RateLimiter::new();
        let v = evaluate_web_acl(
            &acl,
            &req("/admin/x"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            0,
        );
        assert_eq!(v.action, WafAction::Captcha);
        assert!(v.blocked, "Captcha is non-2xx — counts as blocked");
    }

    #[test]
    fn challenge_action_marks_blocked() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![byte_match_uri_contains("/admin", json!({"Challenge": {}}))],
        );
        let limiter = RateLimiter::new();
        let v = evaluate_web_acl(
            &acl,
            &req("/admin/x"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            0,
        );
        assert_eq!(v.action, WafAction::Challenge);
        assert!(v.blocked);
    }

    #[test]
    fn verdict_carries_terminating_rule_id_and_labels() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![
                json!({
                    "Name": "tag",
                    "Priority": 0,
                    "Action": {"Count": {}},
                    "VisibilityConfig": {},
                    "RuleLabels": [{"Name": "awswaf:custom:admin"}],
                    "Statement": {"ByteMatchStatement": {
                        "SearchString": "/admin",
                        "FieldToMatch": {"UriPath": {}},
                        "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                        "PositionalConstraint": "CONTAINS",
                    }},
                }),
                json!({
                    "Name": "block-by-label",
                    "Priority": 1,
                    "Action": {"Block": {}},
                    "VisibilityConfig": {},
                    "Statement": {"LabelMatchStatement": {
                        "Scope": "LABEL",
                        "Key": "awswaf:custom:admin",
                    }},
                }),
            ],
        );
        let limiter = RateLimiter::new();
        let v = evaluate_web_acl(
            &acl,
            &req("/admin/x"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            0,
        );
        assert_eq!(v.action, WafAction::Block);
        assert_eq!(v.terminating_rule_id.as_deref(), Some("block-by-label"));
        assert_eq!(v.labels, vec!["awswaf:custom:admin"]);
        assert_eq!(v.count_rules, vec!["tag"]);
    }

    #[test]
    fn block_with_custom_response_propagates_to_verdict() {
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![json!({
                "Name": "block",
                "Priority": 0,
                "Action": {"Block": {"CustomResponse": {
                    "ResponseCode": 429,
                    "CustomResponseBodyKey": "rate-limited",
                }}},
                "VisibilityConfig": {},
                "Statement": {"ByteMatchStatement": {
                    "SearchString": "/admin",
                    "FieldToMatch": {"UriPath": {}},
                    "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                    "PositionalConstraint": "CONTAINS",
                }},
            })],
        );
        let limiter = RateLimiter::new();
        let v = evaluate_web_acl(
            &acl,
            &req("/admin"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            0,
        );
        assert_eq!(v.action, WafAction::Block);
        assert_eq!(v.custom_response_status, Some(429));
        assert_eq!(v.custom_response_body_key.as_deref(), Some("rate-limited"));
    }

    // --- Customer RuleGroupReferenceStatement expansion ---------------------

    const RG_ARN: &str = "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/rg/rgid";

    fn make_rule_group(rules: Vec<Value>) -> RuleGroup {
        RuleGroup {
            id: "rgid".into(),
            name: "rg".into(),
            arn: RG_ARN.into(),
            scope: "REGIONAL".into(),
            capacity: 10,
            description: None,
            rules,
            visibility_config: json!({}),
            lock_token: "lt".into(),
            label_namespace: "awswaf:000000000000:rulegroup:rg:".into(),
            custom_response_bodies: BTreeMap::new(),
            available_labels: Vec::new(),
            consumed_labels: Vec::new(),
            created_time: Utc::now(),
        }
    }

    fn rule_group_ref_rule(overrides: Value) -> Value {
        let mut stmt = json!({ "ARN": RG_ARN });
        if let Value::Object(extra) = overrides {
            for (k, v) in extra {
                stmt[k] = v;
            }
        }
        json!({
            "Name": "rg-ref",
            "Priority": 0,
            "OverrideAction": {"None": {}},
            "VisibilityConfig": {},
            "Statement": {"RuleGroupReferenceStatement": stmt},
        })
    }

    fn groups_map(g: RuleGroup) -> HashMap<String, RuleGroup> {
        let mut m = HashMap::new();
        m.insert(g.arn.clone(), g);
        m
    }

    #[test]
    fn customer_rule_group_reference_blocks_matching_request() {
        // The referenced group's only rule blocks /admin; the WebACL delegates
        // entirely to the group. A matching request must be blocked.
        let group = make_rule_group(vec![byte_match_uri_contains(
            "/admin",
            json!({"Block": {}}),
        )]);
        let acl = make_acl(json!({"Allow": {}}), vec![rule_group_ref_rule(json!({}))]);
        let groups = groups_map(group);
        let limiter = RateLimiter::new();

        let v = evaluate_web_acl(
            &acl,
            &req("/admin/users"),
            &HashMap::new(),
            &HashMap::new(),
            &groups,
            &limiter,
            0,
        );
        assert_eq!(v.action, WafAction::Block, "group Block rule must block");
        assert!(v.blocked);
        assert_eq!(v.terminating_rule_id.as_deref(), Some("r"));

        // A non-matching request falls through to the WebACL default (Allow).
        let v2 = evaluate_web_acl(
            &acl,
            &req("/public"),
            &HashMap::new(),
            &HashMap::new(),
            &groups,
            &limiter,
            0,
        );
        assert_eq!(v2.action, WafAction::Allow);
    }

    #[test]
    fn customer_rule_group_override_to_count_does_not_block() {
        // OverrideAction {Count:{}} converts every inner action to Count.
        let group = make_rule_group(vec![byte_match_uri_contains(
            "/admin",
            json!({"Block": {}}),
        )]);
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![rule_group_ref_rule(json!({}))
                .as_object()
                .cloned()
                .map(|mut o| {
                    o.insert("OverrideAction".into(), json!({"Count": {}}));
                    Value::Object(o)
                })
                .unwrap()],
        );
        let groups = groups_map(group);
        let limiter = RateLimiter::new();
        let v = evaluate_web_acl(
            &acl,
            &req("/admin/users"),
            &HashMap::new(),
            &HashMap::new(),
            &groups,
            &limiter,
            0,
        );
        assert_eq!(v.action, WafAction::Allow, "Count override must not block");
        assert!(v.count_rules.contains(&"r".to_string()));
    }

    #[test]
    fn customer_rule_group_action_override_flips_block_to_count() {
        // RuleActionOverrides on the reference replaces the inner rule's Block
        // action with Count for the rule named "r".
        let group = make_rule_group(vec![byte_match_uri_contains(
            "/admin",
            json!({"Block": {}}),
        )]);
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![rule_group_ref_rule(json!({
                "RuleActionOverrides": [
                    {"Name": "r", "ActionToUse": {"Count": {}}}
                ]
            }))],
        );
        let groups = groups_map(group);
        let limiter = RateLimiter::new();
        let v = evaluate_web_acl(
            &acl,
            &req("/admin/users"),
            &HashMap::new(),
            &HashMap::new(),
            &groups,
            &limiter,
            0,
        );
        assert_eq!(v.action, WafAction::Allow);
        assert!(v.count_rules.contains(&"r".to_string()));
    }

    #[test]
    fn customer_rule_group_excluded_rule_is_forced_to_count() {
        let group = make_rule_group(vec![byte_match_uri_contains(
            "/admin",
            json!({"Block": {}}),
        )]);
        let acl = make_acl(
            json!({"Allow": {}}),
            vec![rule_group_ref_rule(json!({
                "ExcludedRules": [{"Name": "r"}]
            }))],
        );
        let groups = groups_map(group);
        let limiter = RateLimiter::new();
        let v = evaluate_web_acl(
            &acl,
            &req("/admin/users"),
            &HashMap::new(),
            &HashMap::new(),
            &groups,
            &limiter,
            0,
        );
        assert_eq!(v.action, WafAction::Allow);
        assert!(v.count_rules.contains(&"r".to_string()));
    }

    #[test]
    fn customer_rule_group_missing_reference_is_no_match() {
        // AWS validates the reference at association time; at eval a missing
        // group is a no-op, falling through to the WebACL default action.
        let acl = make_acl(json!({"Allow": {}}), vec![rule_group_ref_rule(json!({}))]);
        let limiter = RateLimiter::new();
        let v = evaluate_web_acl(
            &acl,
            &req("/admin/users"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &limiter,
            0,
        );
        assert_eq!(v.action, WafAction::Allow);
    }
}
