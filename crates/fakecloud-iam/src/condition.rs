//! IAM policy `Condition` block evaluation (Phase 2).
//!
//! Phase 1 of the opt-in IAM enforcement skipped any statement carrying a
//! `Condition` block. This module implements real evaluation of the 28
//! operators AWS defines, with `IfExists` suffix handling and
//! `ForAllValues` / `ForAnyValue` qualifiers, against a
//! [`ConditionContext`] populated at dispatch time.
//!
//! # Scope
//!
//! **Implemented operators** (authoritative list mirrored from
//! [`crate::policy_validation`]):
//!
//! - String: `StringEquals`, `StringNotEquals`, `StringEqualsIgnoreCase`,
//!   `StringNotEqualsIgnoreCase`, `StringLike`, `StringNotLike`
//! - Numeric: `NumericEquals`, `NumericNotEquals`, `NumericLessThan`,
//!   `NumericLessThanEquals`, `NumericGreaterThan`,
//!   `NumericGreaterThanEquals`
//! - Date: `DateEquals`, `DateNotEquals`, `DateLessThan`,
//!   `DateLessThanEquals`, `DateGreaterThan`, `DateGreaterThanEquals`
//! - Boolean: `Bool`
//! - Binary: `BinaryEquals`
//! - IP: `IpAddress`, `NotIpAddress`
//! - ARN: `ArnEquals`, `ArnNotEquals`, `ArnLike`, `ArnNotLike`
//! - Existence: `Null`
//!
//! Plus the `...IfExists` suffix and the `ForAllValues:` / `ForAnyValue:`
//! qualifiers on every operator.
//!
//! **Implemented global keys**:
//!
//! - `aws:username` — populated from the IAM user ARN at dispatch time;
//!   absent for assumed-role / federated-user / root principals,
//!   matching AWS.
//! - `aws:userid` — `<role-id>:<RoleSessionName>` for assumed-role
//!   sessions, the IAM user's `AIDA...` id for IAM users.
//! - `aws:PrincipalArn`, `aws:PrincipalAccount`, `aws:PrincipalType`
//! - `aws:SourceIp` — remote-address of the HTTP connection.
//! - `aws:CurrentTime`, `aws:EpochTime`
//! - `aws:SecureTransport`, `aws:RequestedRegion`
//! - `aws:MultiFactorAuthPresent` — `true` iff the underlying STS
//!   session was minted with `SerialNumber` + `TokenCode`.
//! - `aws:MultiFactorAuthAge` — seconds since the MFA-asserted session
//!   was minted; only present when MFA was supplied at mint time.
//! - `aws:CalledVia` — multi-value chain of service principals that
//!   re-invoked downstream services on the caller's behalf.
//! - `aws:SourceVpc`, `aws:SourceVpce`, `aws:VpcSourceIp` — populated
//!   when the request transited a VPC interface endpoint.
//! - `aws:FederatedProvider` — SAML provider ARN for
//!   `AssumeRoleWithSAML`, OIDC provider ARN (or `ProviderId` host)
//!   for `AssumeRoleWithWebIdentity`. Absent for IAM user keys, plain
//!   `AssumeRole`, `GetSessionToken`, `GetFederationToken`.
//! - `aws:TokenIssueTime` — wall-clock time at which the underlying
//!   STS credential was issued. Absent for IAM user access keys.
//!
//! Service-specific keys (`s3:prefix`, `sqs:MessageAttribute`, …) are
//! deferred to a follow-up batch; the [`ConditionContext::service_keys`]
//! map is pre-wired so they can land without a signature change.
//!
//! # Safe-fail semantics
//!
//! Any unimplemented operator, unknown key, or parse error emits a
//! `tracing::debug!` on the `fakecloud::iam::audit` target and causes the
//! operator to evaluate to `false` — i.e. the statement is treated as
//! *not applicable*. Silently returning `true` would let real policies
//! grant access we can't actually verify, which would defeat the whole
//! opt-in enforcement story.

use std::net::IpAddr;

use chrono::{DateTime, Utc};
use serde_json::Value;

/// Re-export of the data type defined in `fakecloud-core::auth` — see
/// [`fakecloud_core::auth::ConditionContext`] for field documentation.
/// The condition operator framework in this module is implemented
/// against this type.
pub use fakecloud_core::auth::ConditionContext;

/// Base condition operator name (without `IfExists` suffix or
/// `ForAllValues:` / `ForAnyValue:` qualifier).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionOperator {
    StringEquals,
    StringNotEquals,
    StringEqualsIgnoreCase,
    StringNotEqualsIgnoreCase,
    StringLike,
    StringNotLike,
    NumericEquals,
    NumericNotEquals,
    NumericLessThan,
    NumericLessThanEquals,
    NumericGreaterThan,
    NumericGreaterThanEquals,
    DateEquals,
    DateNotEquals,
    DateLessThan,
    DateLessThanEquals,
    DateGreaterThan,
    DateGreaterThanEquals,
    Bool,
    BinaryEquals,
    IpAddress,
    NotIpAddress,
    ArnEquals,
    ArnNotEquals,
    ArnLike,
    ArnNotLike,
    Null,
}

impl ConditionOperator {
    /// Negated matching operators, which evaluate true when the condition
    /// key is absent from the request context.
    pub fn is_negated(self) -> bool {
        matches!(
            self,
            Self::StringNotEquals
                | Self::StringNotEqualsIgnoreCase
                | Self::StringNotLike
                | Self::NumericNotEquals
                | Self::DateNotEquals
                | Self::NotIpAddress
                | Self::ArnNotEquals
                | Self::ArnNotLike
        )
    }

    fn from_str(name: &str) -> Option<Self> {
        Some(match name {
            "StringEquals" => Self::StringEquals,
            "StringNotEquals" => Self::StringNotEquals,
            "StringEqualsIgnoreCase" => Self::StringEqualsIgnoreCase,
            "StringNotEqualsIgnoreCase" => Self::StringNotEqualsIgnoreCase,
            "StringLike" => Self::StringLike,
            "StringNotLike" => Self::StringNotLike,
            "NumericEquals" => Self::NumericEquals,
            "NumericNotEquals" => Self::NumericNotEquals,
            "NumericLessThan" => Self::NumericLessThan,
            "NumericLessThanEquals" => Self::NumericLessThanEquals,
            "NumericGreaterThan" => Self::NumericGreaterThan,
            "NumericGreaterThanEquals" => Self::NumericGreaterThanEquals,
            "DateEquals" => Self::DateEquals,
            "DateNotEquals" => Self::DateNotEquals,
            "DateLessThan" => Self::DateLessThan,
            "DateLessThanEquals" => Self::DateLessThanEquals,
            "DateGreaterThan" => Self::DateGreaterThan,
            "DateGreaterThanEquals" => Self::DateGreaterThanEquals,
            "Bool" => Self::Bool,
            "BinaryEquals" => Self::BinaryEquals,
            "IpAddress" => Self::IpAddress,
            "NotIpAddress" => Self::NotIpAddress,
            "ArnEquals" => Self::ArnEquals,
            "ArnNotEquals" => Self::ArnNotEquals,
            "ArnLike" => Self::ArnLike,
            "ArnNotLike" => Self::ArnNotLike,
            "Null" => Self::Null,
            _ => return None,
        })
    }
}

/// `ForAllValues:` / `ForAnyValue:` qualifier applied to the operator.
///
/// `Single` is the default (no qualifier) and behaves like `ForAnyValue`
/// for single-valued context keys, which is what AWS does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Qualifier {
    Single,
    ForAnyValue,
    ForAllValues,
}

/// Parsed operator name: the base [`ConditionOperator`], the
/// `IfExists` flag, and the `ForAllValues` / `ForAnyValue` qualifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedOperatorName {
    pub op: ConditionOperator,
    pub if_exists: bool,
    pub qualifier: Qualifier,
}

impl ParsedOperatorName {
    /// Parse an operator name as it appears in a policy JSON key, e.g.
    /// `"StringEqualsIfExists"`, `"ForAllValues:StringLike"`,
    /// `"ForAnyValue:DateLessThanIfExists"`.
    ///
    /// Returns `None` if the base operator is not one of the 28 AWS
    /// defines. The caller should safe-fail (statement does not apply)
    /// on `None`.
    pub fn parse(raw: &str) -> Option<Self> {
        let (qualifier, rest) = if let Some(s) = raw.strip_prefix("ForAllValues:") {
            (Qualifier::ForAllValues, s)
        } else if let Some(s) = raw.strip_prefix("ForAnyValue:") {
            (Qualifier::ForAnyValue, s)
        } else {
            (Qualifier::Single, raw)
        };
        let (base, if_exists) = if let Some(s) = rest.strip_suffix("IfExists") {
            (s, true)
        } else {
            (rest, false)
        };
        ConditionOperator::from_str(base).map(|op| Self {
            op,
            if_exists,
            qualifier,
        })
    }
}

/// One parsed entry from a statement's condition block: the operator, a
/// key, and the policy-declared value list.
#[derive(Debug, Clone)]
pub struct ParsedCondition {
    pub operator: ParsedOperatorName,
    pub key: String,
    pub values: Vec<String>,
    /// Whether the policy's language version expands `${...}` policy
    /// variables in `values` (`2012-10-17`).
    pub policy_variables: bool,
}

/// A statement's fully-parsed `Condition` block. Multiple entries are
/// combined with **AND**: every entry must evaluate to `true` for the
/// statement to apply.
#[derive(Debug, Clone, Default)]
pub struct CompiledCondition {
    pub entries: Vec<ParsedCondition>,
}

impl CompiledCondition {
    /// Parse a `Condition` block JSON value into a [`CompiledCondition`].
    ///
    /// AWS's condition block shape is:
    /// ```json
    /// { "OperatorName": { "key1": "val", "key2": ["v1", "v2"] }, ... }
    /// ```
    ///
    /// Operators that fail to parse (unknown base name) become
    /// [`ParsedCondition`] entries with an `Unknown` marker via a
    /// sentinel: we still record them so evaluation can safe-fail. We
    /// model this as "if any entry has an unrecognized operator, the
    /// whole condition block evaluates to `false`" — recorded via a
    /// dedicated `unknown_operators` vec.
    pub fn parse(value: &Value) -> Self {
        let mut out = Self::default();
        let Some(obj) = value.as_object() else {
            return out;
        };
        for (op_name, key_map) in obj {
            let Some(operator) = ParsedOperatorName::parse(op_name) else {
                // Unknown operator — record a poisoned entry that will
                // force the whole block to false on evaluation.
                out.entries.push(ParsedCondition {
                    operator: ParsedOperatorName {
                        op: ConditionOperator::Null,
                        if_exists: false,
                        qualifier: Qualifier::Single,
                    },
                    key: format!("__unknown_operator__:{op_name}"),
                    values: Vec::new(),
                    policy_variables: false,
                });
                continue;
            };
            let Some(inner) = key_map.as_object() else {
                continue;
            };
            for (key, values) in inner {
                let values = coerce_value_list(values);
                out.entries.push(ParsedCondition {
                    operator,
                    key: key.clone(),
                    values,
                    policy_variables: false,
                });
            }
        }
        out
    }

    /// Mark whether the block's policy expands `${...}` policy variables
    /// (only `Version: 2012-10-17` policies do).
    pub fn with_policy_variables(mut self, enabled: bool) -> Self {
        for entry in &mut self.entries {
            entry.policy_variables = enabled;
        }
        self
    }

    /// Evaluate this condition block against a [`ConditionContext`] for an
    /// `Allow` statement. Returns `true` iff every entry matches (AND
    /// semantics).
    pub fn matches(&self, ctx: &ConditionContext) -> bool {
        self.matches_for_effect(ctx, false)
    }

    /// Evaluate this condition block for a statement of the given effect.
    ///
    /// The effect only matters for an operator fakecloud does not recognize
    /// (AWS rejects such a policy at validation time, so the evaluator never
    /// meets one there). It must fail closed in both directions: an `Allow`
    /// it guards does not apply (no grant on a condition we cannot check),
    /// while a `Deny` it guards does (an unreadable restriction still
    /// restricts). The remaining entries are evaluated either way.
    pub fn matches_for_effect(&self, ctx: &ConditionContext, is_deny: bool) -> bool {
        for entry in &self.entries {
            if entry.key.starts_with("__unknown_operator__:") {
                let op_name = entry.key.trim_start_matches("__unknown_operator__:");
                tracing::debug!(
                    target: "fakecloud::iam::audit",
                    operator = %op_name,
                    is_deny,
                    "unknown condition operator; Allow does not apply, Deny applies"
                );
                if is_deny {
                    continue;
                }
                return false;
            }
            if !evaluate_entry(entry, ctx) {
                return false;
            }
        }
        true
    }
}

fn coerce_value_list(value: &Value) -> Vec<String> {
    match value {
        Value::String(s) => vec![s.clone()],
        Value::Bool(b) => vec![b.to_string()],
        Value::Number(n) => vec![n.to_string()],
        Value::Array(arr) => arr.iter().filter_map(value_to_string).collect(),
        _ => Vec::new(),
    }
}

fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Evaluate a single condition entry. Convenience entry point used by
/// the evaluator integration in Batch 2 and by unit tests.
pub fn evaluate_entry(entry: &ParsedCondition, ctx: &ConditionContext) -> bool {
    // Special case: `Null` checks key existence, not value equality.
    if entry.operator.op == ConditionOperator::Null {
        return evaluate_null(entry, ctx);
    }

    let context_values = ctx.lookup(&entry.key);

    // Missing key handling.
    let context_values = match context_values {
        Some(vs) if !vs.is_empty() => vs,
        // The service populated the key and the request carries no values for
        // it. `ForAllValues` is vacuously true then, as AWS documents ("every
        // value matches" holds for none). A key that was never populated is
        // not treated the same way: it may be one fakecloud does not extract,
        // and granting on it would fail open.
        Some(_) if entry.operator.qualifier == Qualifier::ForAllValues => return true,
        _ => {
            // Key absent. `IfExists` -> vacuously true.
            if entry.operator.if_exists {
                return true;
            }
            // A single-valued negated operator (`StringNotEquals`,
            // `ArnNotLike`, `NotIpAddress`, ...) asks "is the value anything
            // other than these?", which AWS answers true when the key is not
            // in the request at all. Returning false here let a
            // `Deny ... StringNotEquals` guard be skipped by simply omitting
            // the key. `ForAnyValue` needs at least one value to match, so it
            // stays false.
            if entry.operator.qualifier == Qualifier::Single && entry.operator.op.is_negated() {
                return true;
            }
            if ctx.lookup(&entry.key).is_none() {
                tracing::debug!(
                    target: "fakecloud::iam::audit",
                    key = %entry.key,
                    operator = ?entry.operator.op,
                    "condition key not populated; treating statement as non-applicable"
                );
            }
            return false;
        }
    };

    if entry.policy_variables
        && supports_policy_variables(entry.operator.op)
        && entry
            .values
            .iter()
            .any(|v| crate::policy_variables::has_variables(v))
    {
        let expanded: Vec<Option<Vec<crate::policy_variables::Piece>>> = entry
            .values
            .iter()
            .map(|v| crate::policy_variables::expand(v, ctx))
            .collect();
        let one = |cv: &String| match_expanded(entry.operator.op, &expanded, cv);
        return match entry.operator.qualifier {
            Qualifier::Single | Qualifier::ForAnyValue => context_values.iter().any(one),
            Qualifier::ForAllValues => context_values.iter().all(one),
        };
    }

    match entry.operator.qualifier {
        Qualifier::Single | Qualifier::ForAnyValue => {
            // ANY context value satisfies the operator against the
            // policy value list (which is itself an OR of values).
            context_values
                .iter()
                .any(|cv| match_values(entry.operator.op, &entry.values, cv))
        }
        Qualifier::ForAllValues => {
            // EVERY context value must match; empty context set is
            // vacuously true (AWS semantics).
            context_values
                .iter()
                .all(|cv| match_values(entry.operator.op, &entry.values, cv))
        }
    }
}

/// Policy variables are expanded only in string and ARN comparisons.
fn supports_policy_variables(op: ConditionOperator) -> bool {
    use ConditionOperator::*;
    matches!(
        op,
        StringEquals
            | StringNotEquals
            | StringEqualsIgnoreCase
            | StringNotEqualsIgnoreCase
            | StringLike
            | StringNotLike
            | ArnEquals
            | ArnLike
            | ArnNotEquals
            | ArnNotLike
    )
}

/// [`match_values`] for policy values with variables expanded. A value whose
/// variable has no value (`None`) is null: no positive operator matches it,
/// and every inverted one does.
fn match_expanded(
    op: ConditionOperator,
    values: &[Option<Vec<crate::policy_variables::Piece>>],
    context_value: &str,
) -> bool {
    use crate::policy_variables::{glob as piece_glob, to_text};
    use ConditionOperator::*;
    let positive = |pred: &dyn Fn(&[crate::policy_variables::Piece]) -> bool| {
        values.iter().any(|v| v.as_deref().is_some_and(pred))
    };
    let inverted = |pred: &dyn Fn(&[crate::policy_variables::Piece]) -> bool| {
        values.iter().all(|v| v.as_deref().is_none_or(|p| !pred(p)))
    };
    match op {
        StringEquals => positive(&|p| to_text(p) == context_value),
        StringNotEquals => inverted(&|p| to_text(p) == context_value),
        StringEqualsIgnoreCase => positive(&|p| to_text(p).eq_ignore_ascii_case(context_value)),
        StringNotEqualsIgnoreCase => inverted(&|p| to_text(p).eq_ignore_ascii_case(context_value)),
        StringLike | ArnEquals | ArnLike => positive(&|p| piece_glob(p, context_value, false)),
        StringNotLike | ArnNotEquals | ArnNotLike => {
            inverted(&|p| piece_glob(p, context_value, false))
        }
        _ => false,
    }
}

/// `Null` operator: `{ "Null": { "aws:username": "true" } }` -> passes
/// iff the key is missing. `"false"` -> passes iff the key is present.
fn evaluate_null(entry: &ParsedCondition, ctx: &ConditionContext) -> bool {
    let key_present = ctx
        .lookup(&entry.key)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    // "true" means "key MUST be null (missing)"; "false" means "key MUST
    // be present". Multiple values in the policy list are OR-combined.
    entry.values.iter().any(|v| match v.as_str() {
        "true" => !key_present,
        "false" => key_present,
        _ => false,
    })
}

/// Match a single policy-value list against a single context value,
/// dispatched by operator. The value-list is treated as OR for positive
/// operators and AND-of-negatives for negative operators — i.e. for
/// `StringNotEquals` every policy value must differ from the context
/// value, matching how AWS evaluates.
fn match_values(op: ConditionOperator, policy_values: &[String], context_value: &str) -> bool {
    use ConditionOperator::*;
    match op {
        StringEquals => policy_values.iter().any(|pv| pv == context_value),
        StringNotEquals => policy_values.iter().all(|pv| pv != context_value),
        StringEqualsIgnoreCase => policy_values
            .iter()
            .any(|pv| pv.eq_ignore_ascii_case(context_value)),
        StringNotEqualsIgnoreCase => policy_values
            .iter()
            .all(|pv| !pv.eq_ignore_ascii_case(context_value)),
        StringLike => policy_values.iter().any(|pv| glob(pv, context_value)),
        StringNotLike => policy_values.iter().all(|pv| !glob(pv, context_value)),
        NumericEquals => numeric_cmp(policy_values, context_value, |p, c| p == c),
        NumericNotEquals => numeric_cmp_all(policy_values, context_value, |p, c| p != c),
        NumericLessThan => numeric_cmp(policy_values, context_value, |p, c| c < p),
        NumericLessThanEquals => numeric_cmp(policy_values, context_value, |p, c| c <= p),
        NumericGreaterThan => numeric_cmp(policy_values, context_value, |p, c| c > p),
        NumericGreaterThanEquals => numeric_cmp(policy_values, context_value, |p, c| c >= p),
        DateEquals => date_cmp(policy_values, context_value, |p, c| p == c),
        DateNotEquals => date_cmp_all(policy_values, context_value, |p, c| p != c),
        DateLessThan => date_cmp(policy_values, context_value, |p, c| c < p),
        DateLessThanEquals => date_cmp(policy_values, context_value, |p, c| c <= p),
        DateGreaterThan => date_cmp(policy_values, context_value, |p, c| c > p),
        DateGreaterThanEquals => date_cmp(policy_values, context_value, |p, c| c >= p),
        Bool => bool_match(policy_values, context_value),
        BinaryEquals => policy_values.iter().any(|pv| pv == context_value),
        IpAddress => policy_values.iter().any(|pv| cidr_match(pv, context_value)),
        NotIpAddress => policy_values
            .iter()
            .all(|pv| !cidr_match(pv, context_value)),
        ArnEquals | ArnLike => policy_values.iter().any(|pv| glob(pv, context_value)),
        ArnNotEquals | ArnNotLike => policy_values.iter().all(|pv| !glob(pv, context_value)),
        Null => false, // handled separately in evaluate_null
    }
}

fn numeric_cmp(
    policy_values: &[String],
    context_value: &str,
    pred: impl Fn(f64, f64) -> bool,
) -> bool {
    let Ok(c) = context_value.parse::<f64>() else {
        tracing::debug!(
            target: "fakecloud::iam::audit",
            context_value = %context_value,
            "non-numeric context value for Numeric* operator; failing closed"
        );
        return false;
    };
    policy_values.iter().any(|pv| {
        pv.parse::<f64>()
            .map(|p| pred(p, c))
            .ok()
            .unwrap_or_else(|| {
                tracing::debug!(
                    target: "fakecloud::iam::audit",
                    policy_value = %pv,
                    "non-numeric policy value for Numeric* operator; failing closed"
                );
                false
            })
    })
}

fn numeric_cmp_all(
    policy_values: &[String],
    context_value: &str,
    pred: impl Fn(f64, f64) -> bool,
) -> bool {
    let Ok(c) = context_value.parse::<f64>() else {
        return false;
    };
    policy_values
        .iter()
        .all(|pv| pv.parse::<f64>().map(|p| pred(p, c)).unwrap_or(false))
}

fn date_cmp(
    policy_values: &[String],
    context_value: &str,
    pred: impl Fn(DateTime<Utc>, DateTime<Utc>) -> bool,
) -> bool {
    let Some(c) = parse_date(context_value) else {
        tracing::debug!(
            target: "fakecloud::iam::audit",
            context_value = %context_value,
            "unparseable context date for Date* operator; failing closed"
        );
        return false;
    };
    policy_values
        .iter()
        .any(|pv| parse_date(pv).map(|p| pred(p, c)).unwrap_or(false))
}

fn date_cmp_all(
    policy_values: &[String],
    context_value: &str,
    pred: impl Fn(DateTime<Utc>, DateTime<Utc>) -> bool,
) -> bool {
    let Some(c) = parse_date(context_value) else {
        return false;
    };
    policy_values
        .iter()
        .all(|pv| parse_date(pv).map(|p| pred(p, c)).unwrap_or(false))
}

fn parse_date(s: &str) -> Option<DateTime<Utc>> {
    // AWS accepts both RFC3339 timestamps and epoch seconds.
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(secs) = s.parse::<i64>() {
        return DateTime::from_timestamp(secs, 0);
    }
    None
}

fn bool_match(policy_values: &[String], context_value: &str) -> bool {
    let cv = context_value.eq_ignore_ascii_case("true");
    policy_values.iter().any(|pv| {
        let pvb = pv.eq_ignore_ascii_case("true");
        let pv_is_bool = pv.eq_ignore_ascii_case("true") || pv.eq_ignore_ascii_case("false");
        pv_is_bool && pvb == cv
    })
}

/// CIDR membership test. Accepts both bare addresses (treated as /32 or
/// /128) and CIDR notation. Supports IPv4 and IPv6.
pub(crate) fn cidr_match(pattern: &str, value: &str) -> bool {
    let Ok(addr) = value.parse::<IpAddr>() else {
        return false;
    };
    let (net_str, prefix_len) = match pattern.split_once('/') {
        Some((n, p)) => {
            let Ok(pl) = p.parse::<u8>() else {
                return false;
            };
            (n, Some(pl))
        }
        None => (pattern, None),
    };
    let Ok(net) = net_str.parse::<IpAddr>() else {
        return false;
    };
    match (net, addr) {
        (IpAddr::V4(n), IpAddr::V4(a)) => {
            let pl = prefix_len.unwrap_or(32);
            if pl > 32 {
                return false;
            }
            let mask: u32 = if pl == 0 { 0 } else { u32::MAX << (32 - pl) };
            (u32::from(n) & mask) == (u32::from(a) & mask)
        }
        (IpAddr::V6(n), IpAddr::V6(a)) => {
            let pl = prefix_len.unwrap_or(128);
            if pl > 128 {
                return false;
            }
            let mask: u128 = if pl == 0 { 0 } else { u128::MAX << (128 - pl) };
            (u128::from(n) & mask) == (u128::from(a) & mask)
        }
        _ => false,
    }
}

/// Glob match with `*` and `?`. Duplicated here rather than re-exported
/// from `evaluator::glob_match` to avoid making that helper `pub(crate)`
/// across modules — keeps the evaluator's public surface stable.
fn glob(pattern: &str, value: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    let mut pi = 0usize;
    let mut vi = 0usize;
    let mut star: Option<usize> = None;
    let mut star_v = 0usize;
    while vi < v.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == v[vi]) {
            pi += 1;
            vi += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            star_v = vi;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            star_v += 1;
            vi = star_v;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Top-level evaluation helper used by the evaluator integration in
/// Batch 2. Returns `true` iff the block is empty or every entry matches.
pub fn evaluate_condition_block(block: &CompiledCondition, ctx: &ConditionContext) -> bool {
    block.matches(ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_aws::arn::Arn;
    use serde_json::json;

    fn ctx_user(name: &str) -> ConditionContext {
        ConditionContext {
            aws_username: Some(name.to_string()),
            aws_principal_arn: Some(
                Arn::global("iam", "123456789012", &format!("user/{name}")).to_string(),
            ),
            aws_principal_account: Some("123456789012".to_string()),
            aws_principal_type: Some("User".to_string()),
            aws_userid: Some("AIDAEXAMPLE".to_string()),
            ..Default::default()
        }
    }

    fn compile(v: serde_json::Value) -> CompiledCondition {
        CompiledCondition::parse(&v)
    }

    // ---- Operator name parsing ----

    #[test]
    fn parse_plain_operator() {
        let p = ParsedOperatorName::parse("StringEquals").unwrap();
        assert_eq!(p.op, ConditionOperator::StringEquals);
        assert!(!p.if_exists);
        assert_eq!(p.qualifier, Qualifier::Single);
    }

    #[test]
    fn parse_if_exists_suffix() {
        let p = ParsedOperatorName::parse("StringEqualsIfExists").unwrap();
        assert_eq!(p.op, ConditionOperator::StringEquals);
        assert!(p.if_exists);
    }

    #[test]
    fn parse_for_all_values_qualifier() {
        let p = ParsedOperatorName::parse("ForAllValues:StringLike").unwrap();
        assert_eq!(p.op, ConditionOperator::StringLike);
        assert_eq!(p.qualifier, Qualifier::ForAllValues);
    }

    #[test]
    fn parse_for_any_value_with_if_exists() {
        let p = ParsedOperatorName::parse("ForAnyValue:DateLessThanIfExists").unwrap();
        assert_eq!(p.op, ConditionOperator::DateLessThan);
        assert!(p.if_exists);
        assert_eq!(p.qualifier, Qualifier::ForAnyValue);
    }

    #[test]
    fn parse_unknown_operator_returns_none() {
        assert!(ParsedOperatorName::parse("NotARealOp").is_none());
    }

    // ---- String operators ----

    #[test]
    fn string_equals_matches_exact() {
        let b = compile(json!({ "StringEquals": { "aws:username": "alice" } }));
        assert!(b.matches(&ctx_user("alice")));
        assert!(!b.matches(&ctx_user("bob")));
    }

    #[test]
    fn string_not_equals_denies_match() {
        let b = compile(json!({ "StringNotEquals": { "aws:username": "alice" } }));
        assert!(!b.matches(&ctx_user("alice")));
        assert!(b.matches(&ctx_user("bob")));
    }

    #[test]
    fn string_equals_ignore_case() {
        let b = compile(json!({ "StringEqualsIgnoreCase": { "aws:username": "ALICE" } }));
        assert!(b.matches(&ctx_user("alice")));
    }

    #[test]
    fn string_like_wildcard() {
        let b = compile(json!({ "StringLike": { "aws:username": "al*" } }));
        assert!(b.matches(&ctx_user("alice")));
        assert!(!b.matches(&ctx_user("bob")));
    }

    #[test]
    fn string_not_like_wildcard() {
        let b = compile(json!({ "StringNotLike": { "aws:username": "al*" } }));
        assert!(!b.matches(&ctx_user("alice")));
        assert!(b.matches(&ctx_user("bob")));
    }

    #[test]
    fn string_equals_list_is_or() {
        let b = compile(json!({
            "StringEquals": { "aws:username": ["alice", "carol"] }
        }));
        assert!(b.matches(&ctx_user("alice")));
        assert!(b.matches(&ctx_user("carol")));
        assert!(!b.matches(&ctx_user("bob")));
    }

    // ---- Numeric ----

    #[test]
    fn numeric_equals() {
        let mut ctx = ctx_user("alice");
        ctx.service_keys
            .insert("s3:maxkeys".to_string(), vec!["42".to_string()]);
        let b = compile(json!({ "NumericEquals": { "s3:maxkeys": "42" } }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn numeric_less_than_epoch() {
        let mut ctx = ctx_user("alice");
        ctx.aws_epoch_time = Some(1_000);
        let b = compile(json!({ "NumericLessThan": { "aws:epochtime": "2000" } }));
        assert!(b.matches(&ctx));
        ctx.aws_epoch_time = Some(3_000);
        assert!(!b.matches(&ctx));
    }

    // ---- Date ----

    #[test]
    fn date_less_than_current_time() {
        let mut ctx = ctx_user("alice");
        ctx.aws_current_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .ok()
            .map(|d| d.with_timezone(&Utc));
        let b = compile(json!({
            "DateLessThan": { "aws:CurrentTime": "2025-01-01T00:00:00Z" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn date_greater_than_blocks_past() {
        let mut ctx = ctx_user("alice");
        ctx.aws_current_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .ok()
            .map(|d| d.with_timezone(&Utc));
        let b = compile(json!({
            "DateGreaterThan": { "aws:CurrentTime": "2025-01-01T00:00:00Z" }
        }));
        assert!(!b.matches(&ctx));
    }

    // ---- Bool ----

    #[test]
    fn bool_secure_transport() {
        let mut ctx = ctx_user("alice");
        ctx.aws_secure_transport = Some(false);
        let b = compile(json!({
            "Bool": { "aws:SecureTransport": "false" }
        }));
        assert!(b.matches(&ctx));
        ctx.aws_secure_transport = Some(true);
        assert!(!b.matches(&ctx));
    }

    // ---- IP address ----

    #[test]
    fn ip_address_cidr_match() {
        let mut ctx = ctx_user("alice");
        ctx.aws_source_ip = Some("10.0.0.5".parse().unwrap());
        let b = compile(json!({ "IpAddress": { "aws:SourceIp": "10.0.0.0/24" } }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn ip_address_cidr_outside() {
        let mut ctx = ctx_user("alice");
        ctx.aws_source_ip = Some("192.168.1.5".parse().unwrap());
        let b = compile(json!({ "IpAddress": { "aws:SourceIp": "10.0.0.0/24" } }));
        assert!(!b.matches(&ctx));
    }

    #[test]
    fn not_ip_address_blocks_cidr() {
        let mut ctx = ctx_user("alice");
        ctx.aws_source_ip = Some("10.0.0.5".parse().unwrap());
        let b = compile(json!({ "NotIpAddress": { "aws:SourceIp": "10.0.0.0/24" } }));
        assert!(!b.matches(&ctx));
    }

    #[test]
    fn ip_address_bare_v4() {
        let mut ctx = ctx_user("alice");
        ctx.aws_source_ip = Some("127.0.0.1".parse().unwrap());
        let b = compile(json!({ "IpAddress": { "aws:SourceIp": "127.0.0.1" } }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn ip_address_v6_cidr() {
        let mut ctx = ctx_user("alice");
        ctx.aws_source_ip = Some("2001:db8::1".parse().unwrap());
        let b = compile(json!({ "IpAddress": { "aws:SourceIp": "2001:db8::/32" } }));
        assert!(b.matches(&ctx));
    }

    // ---- ARN ----

    #[test]
    fn arn_like_wildcard() {
        let b = compile(json!({
            "ArnLike": { "aws:PrincipalArn": "arn:aws:iam::*:user/*" }
        }));
        assert!(b.matches(&ctx_user("alice")));
    }

    #[test]
    fn arn_not_equals_rejects_exact() {
        let b = compile(json!({
            "ArnNotEquals": {
                "aws:PrincipalArn": "arn:aws:iam::123456789012:user/alice"
            }
        }));
        assert!(!b.matches(&ctx_user("alice")));
        assert!(b.matches(&ctx_user("bob")));
    }

    // ---- Null (existence) ----

    #[test]
    fn null_true_requires_missing_key() {
        let b = compile(json!({ "Null": { "aws:username": "true" } }));
        assert!(!b.matches(&ctx_user("alice"))); // key present
        let ctx = ConditionContext::default();
        assert!(b.matches(&ctx)); // key absent
    }

    #[test]
    fn null_false_requires_present_key() {
        let b = compile(json!({ "Null": { "aws:username": "false" } }));
        assert!(b.matches(&ctx_user("alice")));
        let ctx = ConditionContext::default();
        assert!(!b.matches(&ctx));
    }

    // ---- IfExists ----

    #[test]
    fn if_exists_passes_on_missing_key() {
        let b = compile(json!({
            "StringEqualsIfExists": { "aws:username": "alice" }
        }));
        let ctx = ConditionContext::default();
        assert!(b.matches(&ctx));
    }

    #[test]
    fn if_exists_still_checks_present_key() {
        let b = compile(json!({
            "StringEqualsIfExists": { "aws:username": "alice" }
        }));
        assert!(b.matches(&ctx_user("alice")));
        assert!(!b.matches(&ctx_user("bob")));
    }

    // ---- ForAllValues / ForAnyValue ----

    #[test]
    fn for_all_values_every_context_must_match() {
        let mut ctx = ctx_user("alice");
        ctx.request_tags = Some(
            [("env", "dev"), ("team", "platform")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        let b = compile(json!({
            "ForAllValues:StringEquals": {
                "aws:TagKeys": ["env", "team", "owner"]
            }
        }));
        assert!(b.matches(&ctx));
        ctx.request_tags = Some(
            [("env", "dev"), ("rogue", "x")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        assert!(!b.matches(&ctx));
    }

    #[test]
    fn for_any_value_some_context_matches() {
        let mut ctx = ctx_user("alice");
        ctx.request_tags = Some(
            [("env", "dev"), ("rogue", "x")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        let b = compile(json!({
            "ForAnyValue:StringEquals": { "aws:TagKeys": "env" }
        }));
        assert!(b.matches(&ctx));
    }

    // ---- Multi-operator AND semantics ----

    #[test]
    fn multiple_operators_must_all_match() {
        let mut ctx = ctx_user("alice");
        ctx.aws_source_ip = Some("10.0.0.1".parse().unwrap());
        let b = compile(json!({
            "StringEquals": { "aws:username": "alice" },
            "IpAddress":    { "aws:SourceIp": "10.0.0.0/24" }
        }));
        assert!(b.matches(&ctx));

        let mut wrong_ip = ctx.clone();
        wrong_ip.aws_source_ip = Some("192.168.1.1".parse().unwrap());
        assert!(!b.matches(&wrong_ip));

        let wrong_user = ctx_user("bob");
        let mut wu = wrong_user;
        wu.aws_source_ip = Some("10.0.0.1".parse().unwrap());
        assert!(!b.matches(&wu));
    }

    // ---- Safe-fail on unknown operator / key ----

    #[test]
    fn unknown_operator_fails_closed() {
        let b = compile(json!({ "NotARealOp": { "aws:username": "alice" } }));
        assert!(!b.matches(&ctx_user("alice")));
        // Fail closed for Deny too: an unreadable restriction still applies.
        assert!(b.matches_for_effect(&ctx_user("alice"), true));
    }

    #[test]
    fn unknown_operator_in_deny_still_ands_with_other_entries() {
        let b = compile(json!({
            "NotARealOp": { "aws:username": "alice" },
            "StringEquals": { "aws:username": "bob" }
        }));
        assert!(!b.matches_for_effect(&ctx_user("alice"), true));
        assert!(b.matches_for_effect(&ctx_user("bob"), true));
        assert!(!b.matches_for_effect(&ctx_user("bob"), false));
    }

    /// Truth table for a condition key absent from the request context:
    /// positive operators are false, negated operators are true, any
    /// `...IfExists` is true, and `ForAnyValue:` with a negated operator is
    /// false (it needs a value to match).
    #[test]
    fn missing_key_truth_table() {
        let ctx = ConditionContext::default();
        let cases: &[(&str, &str, bool)] = &[
            ("StringEquals", "x", false),
            ("StringNotEquals", "x", true),
            ("StringEqualsIgnoreCase", "x", false),
            ("StringNotEqualsIgnoreCase", "x", true),
            ("StringLike", "x*", false),
            ("StringNotLike", "x*", true),
            ("NumericEquals", "1", false),
            ("NumericNotEquals", "1", true),
            ("NumericLessThan", "1", false),
            ("DateEquals", "2020-01-01T00:00:00Z", false),
            ("DateNotEquals", "2020-01-01T00:00:00Z", true),
            ("DateLessThan", "2020-01-01T00:00:00Z", false),
            ("Bool", "true", false),
            ("IpAddress", "10.0.0.0/8", false),
            ("NotIpAddress", "10.0.0.0/8", true),
            ("ArnEquals", "arn:aws:iam::1:role/r", false),
            ("ArnNotEquals", "arn:aws:iam::1:role/r", true),
            ("ArnLike", "arn:aws:iam::*:role/*", false),
            ("ArnNotLike", "arn:aws:iam::*:role/*", true),
            ("StringEqualsIfExists", "x", true),
            ("StringNotEqualsIfExists", "x", true),
            ("ArnLikeIfExists", "arn:*", true),
            ("ForAnyValue:StringNotEquals", "x", false),
            ("ForAnyValue:StringEquals", "x", false),
        ];
        for (op, value, want) in cases {
            let b = compile(json!({ *op: { "aws:madeupkey": value } }));
            assert_eq!(b.matches(&ctx), *want, "{op} with key absent");
            assert_eq!(
                b.matches_for_effect(&ctx, true),
                *want,
                "{op} with key absent (Deny)"
            );
        }
    }

    #[test]
    fn deny_string_not_equals_applies_when_key_omitted() {
        // `Deny unless aws:RequestedRegion == us-east-1` must still deny a
        // request whose context lacks the key entirely.
        let b = compile(json!({
            "StringNotEquals": { "aws:RequestedRegion": "us-east-1" }
        }));
        assert!(b.matches_for_effect(&ConditionContext::default(), true));
        let ctx = ConditionContext {
            aws_requested_region: Some("us-east-1".into()),
            ..Default::default()
        };
        assert!(!b.matches_for_effect(&ctx, true));
    }

    #[test]
    fn unknown_key_fails_closed() {
        let b = compile(json!({
            "StringEquals": { "aws:madeupkey": "whatever" }
        }));
        assert!(!b.matches(&ctx_user("alice")));
    }

    #[test]
    fn context_lookup_case_insensitive() {
        let ctx = ctx_user("alice");
        assert_eq!(ctx.lookup("AWS:UserName"), Some(vec!["alice".to_string()]));
        assert_eq!(ctx.lookup("aws:username"), Some(vec!["alice".to_string()]));
    }

    #[test]
    fn cidr_match_helper() {
        assert!(cidr_match("10.0.0.0/8", "10.1.2.3"));
        assert!(!cidr_match("10.0.0.0/8", "11.0.0.1"));
        assert!(cidr_match("0.0.0.0/0", "1.2.3.4"));
        assert!(!cidr_match("invalid", "1.2.3.4"));
    }

    // ---- F3 global condition keys ----

    #[test]
    fn mfa_present_bool_true() {
        let mut ctx = ctx_user("alice");
        ctx.aws_mfa_present = Some(true);
        let b = compile(json!({
            "Bool": { "aws:MultiFactorAuthPresent": "true" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn mfa_present_bool_false_when_absent_session() {
        let mut ctx = ctx_user("alice");
        ctx.aws_mfa_present = Some(false);
        let b = compile(json!({
            "Bool": { "aws:MultiFactorAuthPresent": "true" }
        }));
        assert!(!b.matches(&ctx));
    }

    #[test]
    fn mfa_present_missing_key_safe_fails_without_if_exists() {
        // Long-lived IAM user keys never have MFA; a policy that
        // requires it must safe-fail.
        let ctx = ctx_user("alice");
        let b = compile(json!({
            "Bool": { "aws:MultiFactorAuthPresent": "true" }
        }));
        assert!(!b.matches(&ctx));
    }

    #[test]
    fn mfa_present_if_exists_passes_when_absent() {
        let ctx = ctx_user("alice");
        let b = compile(json!({
            "BoolIfExists": { "aws:MultiFactorAuthPresent": "true" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn mfa_age_numeric_less_than() {
        // Session minted 60 seconds ago — policy requires age < 3600.
        let mut ctx = ctx_user("alice");
        ctx.aws_mfa_age_seconds = Some(60);
        let b = compile(json!({
            "NumericLessThan": { "aws:MultiFactorAuthAge": "3600" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn mfa_age_numeric_greater_than_blocks_old_sessions() {
        let mut ctx = ctx_user("alice");
        ctx.aws_mfa_age_seconds = Some(7200);
        let b = compile(json!({
            "NumericLessThan": { "aws:MultiFactorAuthAge": "3600" }
        }));
        assert!(!b.matches(&ctx));
    }

    #[test]
    fn called_via_string_equals_matches_first_hop() {
        let mut ctx = ctx_user("alice");
        ctx.aws_called_via = vec!["cloudformation.amazonaws.com".into()];
        let b = compile(json!({
            "StringEquals": { "aws:CalledVia": "cloudformation.amazonaws.com" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn called_via_for_any_value_matches_in_chain() {
        let mut ctx = ctx_user("alice");
        ctx.aws_called_via = vec![
            "cloudformation.amazonaws.com".into(),
            "athena.amazonaws.com".into(),
        ];
        let b = compile(json!({
            "ForAnyValue:StringEquals": {
                "aws:CalledVia": ["athena.amazonaws.com", "lambda.amazonaws.com"]
            }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn called_via_for_all_values_requires_every_hop_in_set() {
        let mut ctx = ctx_user("alice");
        ctx.aws_called_via = vec![
            "cloudformation.amazonaws.com".into(),
            "athena.amazonaws.com".into(),
        ];
        let b = compile(json!({
            "ForAllValues:StringEquals": {
                "aws:CalledVia": ["cloudformation.amazonaws.com", "athena.amazonaws.com", "lambda.amazonaws.com"]
            }
        }));
        assert!(b.matches(&ctx));
        // Add a hop not in the policy set; ForAllValues now fails.
        ctx.aws_called_via.push("ec2.amazonaws.com".into());
        assert!(!b.matches(&ctx));
    }

    #[test]
    fn source_vpce_string_equals() {
        let mut ctx = ctx_user("alice");
        ctx.aws_source_vpce = Some("vpce-0abcd1234".into());
        let b = compile(json!({
            "StringEquals": { "aws:SourceVpce": "vpce-0abcd1234" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn source_vpce_string_not_equals_blocks_other() {
        let mut ctx = ctx_user("alice");
        ctx.aws_source_vpce = Some("vpce-bad".into());
        let b = compile(json!({
            "StringEquals": { "aws:SourceVpce": "vpce-good" }
        }));
        assert!(!b.matches(&ctx));
    }

    #[test]
    fn source_vpc_string_equals() {
        let mut ctx = ctx_user("alice");
        ctx.aws_source_vpc = Some("vpc-1a2b3c".into());
        let b = compile(json!({
            "StringEquals": { "aws:SourceVpc": "vpc-1a2b3c" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn vpc_source_ip_cidr_match() {
        let mut ctx = ctx_user("alice");
        ctx.aws_vpc_source_ip = Some("172.31.5.10".parse().unwrap());
        let b = compile(json!({
            "IpAddress": { "aws:VpcSourceIp": "172.31.0.0/16" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn vpc_source_ip_cidr_outside() {
        let mut ctx = ctx_user("alice");
        ctx.aws_vpc_source_ip = Some("10.0.0.5".parse().unwrap());
        let b = compile(json!({
            "IpAddress": { "aws:VpcSourceIp": "172.31.0.0/16" }
        }));
        assert!(!b.matches(&ctx));
    }

    #[test]
    fn federated_provider_string_equals_saml_arn() {
        let mut ctx = ctx_user("alice");
        ctx.aws_federated_provider = Some("arn:aws:iam::123456789012:saml-provider/idp".into());
        let b = compile(json!({
            "StringEquals": {
                "aws:FederatedProvider": "arn:aws:iam::123456789012:saml-provider/idp"
            }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn federated_provider_string_like_oidc_host() {
        let mut ctx = ctx_user("alice");
        ctx.aws_federated_provider = Some("accounts.google.com".into());
        let b = compile(json!({
            "StringLike": { "aws:FederatedProvider": "accounts.*" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn token_issue_time_date_less_than() {
        let mut ctx = ctx_user("alice");
        ctx.aws_token_issue_time = DateTime::parse_from_rfc3339("2024-06-01T00:00:00Z")
            .ok()
            .map(|d| d.with_timezone(&Utc));
        let b = compile(json!({
            "DateLessThan": { "aws:TokenIssueTime": "2024-12-31T23:59:59Z" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn token_issue_time_date_greater_than_blocks_old_token() {
        let mut ctx = ctx_user("alice");
        ctx.aws_token_issue_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .ok()
            .map(|d| d.with_timezone(&Utc));
        let b = compile(json!({
            "DateGreaterThan": { "aws:TokenIssueTime": "2024-06-01T00:00:00Z" }
        }));
        assert!(!b.matches(&ctx));
    }

    #[test]
    fn userid_string_equals_assumed_role_format() {
        // AWS userid format for assumed roles: <role-id>:<RoleSessionName>
        let mut ctx = ctx_user("alice");
        ctx.aws_userid = Some("AROAEXAMPLE:bob-session".into());
        let b = compile(json!({
            "StringEquals": { "aws:userid": "AROAEXAMPLE:bob-session" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn userid_string_like_wildcard() {
        let mut ctx = ctx_user("alice");
        ctx.aws_userid = Some("AROAEXAMPLE:bob-session".into());
        let b = compile(json!({
            "StringLike": { "aws:userid": "AROAEXAMPLE:*" }
        }));
        assert!(b.matches(&ctx));
    }

    #[test]
    fn username_lookup_case_insensitive_for_f3_keys() {
        // AWS treats every condition key case-insensitively. Spot-check
        // each F3 key's lookup with mixed case.
        let mut ctx = ctx_user("alice");
        ctx.aws_mfa_present = Some(true);
        ctx.aws_called_via = vec!["lambda.amazonaws.com".into()];
        ctx.aws_source_vpc = Some("vpc-x".into());
        ctx.aws_federated_provider = Some("accounts.google.com".into());
        ctx.aws_token_issue_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .ok()
            .map(|d| d.with_timezone(&Utc));

        assert!(ctx.lookup("AWS:MULTIFACTORAUTHPRESENT").is_some());
        assert!(ctx.lookup("aws:multifactorauthpresent").is_some());
        assert!(ctx.lookup("aws:CalledVia").is_some());
        assert!(ctx.lookup("AWS:SOURCEVPC").is_some());
        assert!(ctx.lookup("aws:FederatedProvider").is_some());
        assert!(ctx.lookup("aws:tokenissuetime").is_some());
    }

    #[test]
    fn combined_mfa_and_token_issue_time() {
        // Realistic policy: only allow when MFA is present AND the
        // session was minted in the last hour. Combined AND semantics.
        let mut ctx = ctx_user("alice");
        ctx.aws_mfa_present = Some(true);
        ctx.aws_mfa_age_seconds = Some(120);
        ctx.aws_token_issue_time = Some(Utc::now() - chrono::Duration::seconds(120));
        let b = compile(json!({
            "Bool": { "aws:MultiFactorAuthPresent": "true" },
            "NumericLessThan": { "aws:MultiFactorAuthAge": "3600" }
        }));
        assert!(b.matches(&ctx));

        // Bump the age past the policy ceiling — denied.
        ctx.aws_mfa_age_seconds = Some(7200);
        assert!(!b.matches(&ctx));
    }
}
