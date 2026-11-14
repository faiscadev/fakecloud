//! Account-level retention policies (`PutAccountPolicy` with
//! `policyType = RETENTION_POLICY`).
//!
//! A retention policy document is `{"retentionInDays": <days | "NEVER_EXPIRE">,
//! "enforcementMode": "NEW_ONLY" | "RETROACTIVE"}`, optionally scoped with a
//! `LogGroupNamePrefix IN [...]` / `NOT IN [...]` selection criteria. When one
//! or more policies apply to a log group, the longest retention wins
//! (`NEVER_EXPIRE` being the longest), ties go to the policy created first, and
//! the winner replaces the group's own retention for both reporting
//! (`DescribeLogGroups` `retentionInDays` + `retentionPolicyName`) and event
//! expiry.
//!
//! AWS applies a new or updated policy 24 hours after the call; the emulator
//! applies it immediately, which is what the dry-run preview models anyway.

use std::collections::BTreeMap;

use crate::state::{AccountPolicy, LogGroup, VALID_RETENTION_DAYS};

pub const RETENTION_POLICY: &str = "RETENTION_POLICY";

/// Retention periods an account-level policy accepts (30 days minimum).
const POLICY_RETENTION_DAYS_MIN: i64 = 30;

/// Each account can have up to 20 retention policies per Region.
pub const MAX_RETENTION_POLICIES: usize = 20;

/// A retention period: `None` means log events never expire.
pub type Retention = Option<i32>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnforcementMode {
    NewOnly,
    Retroactive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionDocument {
    pub retention: Retention,
    pub enforcement: EnforcementMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefixCriteria {
    All,
    In(Vec<String>),
    NotIn(Vec<String>),
}

impl PrefixCriteria {
    fn matches(&self, name: &str) -> bool {
        match self {
            PrefixCriteria::All => true,
            PrefixCriteria::In(prefixes) => prefixes.iter().any(|p| name.starts_with(p.as_str())),
            PrefixCriteria::NotIn(prefixes) => {
                !prefixes.iter().any(|p| name.starts_with(p.as_str()))
            }
        }
    }
}

/// Parse and validate a retention policy document.
pub fn parse_document(doc: &str) -> Result<RetentionDocument, String> {
    let value: serde_json::Value = serde_json::from_str(doc)
        .map_err(|_| "The retention policy document is not valid JSON.".to_string())?;
    let obj = value
        .as_object()
        .ok_or_else(|| "The retention policy document must be a JSON object.".to_string())?;
    let allowed = VALID_RETENTION_DAYS
        .iter()
        .filter(|d| **d >= POLICY_RETENTION_DAYS_MIN)
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let invalid_days = || {
        format!(
            "retentionInDays must be one of [{allowed}] or NEVER_EXPIRE; account-level \
             retention policies require a minimum retention period of 30 days."
        )
    };
    let retention = match obj.get("retentionInDays") {
        None | Some(serde_json::Value::Null) => {
            return Err("The retention policy document must include retentionInDays.".into())
        }
        Some(serde_json::Value::String(s)) if s == "NEVER_EXPIRE" => None,
        Some(v) => {
            let days = v
                .as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                .ok_or_else(invalid_days)?;
            if days < POLICY_RETENTION_DAYS_MIN || !VALID_RETENTION_DAYS.contains(&days) {
                return Err(invalid_days());
            }
            Some(days as i32)
        }
    };
    let enforcement = match obj.get("enforcementMode").and_then(|v| v.as_str()) {
        Some("NEW_ONLY") => EnforcementMode::NewOnly,
        Some("RETROACTIVE") => EnforcementMode::Retroactive,
        Some(other) => {
            return Err(format!(
                "enforcementMode must be NEW_ONLY or RETROACTIVE; got {other}."
            ))
        }
        None => return Err("The retention policy document must include enforcementMode.".into()),
    };
    Ok(RetentionDocument {
        retention,
        enforcement,
    })
}

/// Parse a retention policy's selection criteria. Only `LogGroupNamePrefix`
/// with `IN` / `NOT IN` and up to 50 values is supported; absent criteria
/// select every log group in the account.
pub fn parse_criteria(criteria: Option<&str>) -> Result<PrefixCriteria, String> {
    let Some(raw) = criteria.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(PrefixCriteria::All);
    };
    let unsupported = || {
        "Retention policies support only LogGroupNamePrefix IN [...] or \
         LogGroupNamePrefix NOT IN [...] selection criteria."
            .to_string()
    };
    let rest = raw
        .strip_prefix("LogGroupNamePrefix")
        .ok_or_else(unsupported)?
        .trim_start();
    let (negated, list) = if let Some(l) = rest.strip_prefix("NOT IN") {
        (true, l)
    } else if let Some(l) = rest.strip_prefix("IN") {
        (false, l)
    } else {
        return Err(unsupported());
    };
    let values: Vec<String> = serde_json::from_str(list.trim()).map_err(|_| unsupported())?;
    if values.is_empty() || values.len() > 50 {
        return Err("Selection criteria must list between 1 and 50 values.".into());
    }
    Ok(if negated {
        PrefixCriteria::NotIn(values)
    } else {
        PrefixCriteria::In(values)
    })
}

/// A parsed retention policy, ready to be matched against log groups.
#[derive(Debug, Clone)]
pub struct ParsedPolicy {
    pub name: String,
    pub created_time: i64,
    pub document: RetentionDocument,
    pub criteria: PrefixCriteria,
}

impl ParsedPolicy {
    pub fn from_stored(p: &AccountPolicy) -> Option<Self> {
        if p.policy_type != RETENTION_POLICY {
            return None;
        }
        Some(Self {
            name: p.policy_name.clone(),
            created_time: p.created_time(),
            document: parse_document(&p.policy_document).ok()?,
            criteria: parse_criteria(p.selection_criteria.as_deref()).ok()?,
        })
    }

    pub fn applies_to(&self, group: &LogGroup) -> bool {
        self.criteria.matches(&group.name)
            && match self.document.enforcement {
                EnforcementMode::Retroactive => true,
                EnforcementMode::NewOnly => group.creation_time > self.created_time,
            }
    }
}

/// `true` when retention `a` keeps events longer than `b`.
fn longer(a: Retention, b: Retention) -> bool {
    match (a, b) {
        (None, None) => false,
        (None, Some(_)) => true,
        (Some(_), None) => false,
        (Some(x), Some(y)) => x > y,
    }
}

/// The policy governing `group` among `policies`: the longest retention,
/// ties broken by earliest creation.
pub fn governing<'a>(policies: &'a [ParsedPolicy], group: &LogGroup) -> Option<&'a ParsedPolicy> {
    let mut best: Option<&ParsedPolicy> = None;
    for p in policies.iter().filter(|p| p.applies_to(group)) {
        best = match best {
            None => Some(p),
            Some(b)
                if longer(p.document.retention, b.document.retention)
                    || (p.document.retention == b.document.retention
                        && p.created_time < b.created_time) =>
            {
                Some(p)
            }
            keep => keep,
        };
    }
    best
}

pub fn parsed_policies(policies: &BTreeMap<(String, String), AccountPolicy>) -> Vec<ParsedPolicy> {
    policies
        .values()
        .filter_map(ParsedPolicy::from_stored)
        .collect()
}

/// The retention in effect for `group`: an applicable account policy's, else
/// the group's own.
pub fn effective_with(policies: &[ParsedPolicy], group: &LogGroup) -> Retention {
    match governing(policies, group) {
        Some(p) => p.document.retention,
        None => group.retention_in_days,
    }
}

/// [`effective_with`] straight from the stored account policies.
pub fn effective_retention(
    policies: &BTreeMap<(String, String), AccountPolicy>,
    group: &LogGroup,
) -> Retention {
    effective_with(&parsed_policies(policies), group)
}

/// Wire form of a retention period in a dry-run `LogGroupImpact`.
pub fn retention_value(r: Retention) -> String {
    match r {
        Some(days) => days.to_string(),
        None => "NEVER_EXPIRE".to_string(),
    }
}

/// Estimated impact of replacing the policy set `before` with `after` on the
/// log groups in `scope`, rendered as the model's `DryRunResult`.
pub fn dry_run_result<'a>(
    groups: impl Iterator<Item = &'a LogGroup>,
    in_scope: impl Fn(&LogGroup) -> bool,
    before: &[ParsedPolicy],
    after: &[ParsedPolicy],
) -> serde_json::Value {
    let mut increased = 0u64;
    let mut decreased = 0u64;
    let mut unchanged = 0u64;
    let mut impacts: Vec<(&LogGroup, Retention, Retention)> = Vec::new();
    for g in groups.filter(|g| in_scope(g)) {
        let current = effective_with(before, g);
        let proposed = effective_with(after, g);
        if longer(proposed, current) {
            increased += 1;
        } else if longer(current, proposed) {
            decreased += 1;
        } else {
            unchanged += 1;
        }
        impacts.push((g, current, proposed));
    }
    impacts.sort_by(|a, b| {
        b.0.stored_bytes
            .cmp(&a.0.stored_bytes)
            .then_with(|| a.0.name.cmp(&b.0.name))
    });
    let top: Vec<serde_json::Value> = impacts
        .iter()
        .take(20)
        .map(|(g, current, proposed)| {
            serde_json::json!({
                "logGroupName": g.name,
                "currentRetention": retention_value(*current),
                "proposedRetention": retention_value(*proposed),
                "storedBytes": g.stored_bytes,
            })
        })
        .collect();
    serde_json::json!({
        "totalMatchedLogGroups": increased + decreased + unchanged,
        "increasedRetentionLogGroupCount": increased,
        "decreasedRetentionLogGroupCount": decreased,
        "unchangedRetentionLogGroupCount": unchanged,
        "failedAnalysisCount": 0,
        "topLogGroupsImpacted": top,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_parses_days_and_never_expire() {
        assert_eq!(
            parse_document(r#"{"retentionInDays": 90, "enforcementMode": "RETROACTIVE"}"#),
            Ok(RetentionDocument {
                retention: Some(90),
                enforcement: EnforcementMode::Retroactive
            })
        );
        assert_eq!(
            parse_document(r#"{"retentionInDays": "NEVER_EXPIRE", "enforcementMode": "NEW_ONLY"}"#)
                .unwrap()
                .retention,
            None
        );
    }

    #[test]
    fn document_rejects_short_or_unlisted_retention() {
        assert!(
            parse_document(r#"{"retentionInDays": 14, "enforcementMode": "NEW_ONLY"}"#).is_err()
        );
        assert!(
            parse_document(r#"{"retentionInDays": 31, "enforcementMode": "NEW_ONLY"}"#).is_err()
        );
        assert!(parse_document(r#"{"retentionInDays": 30}"#).is_err());
        assert!(
            parse_document(r#"{"retentionInDays": 30, "enforcementMode": "SOMETIMES"}"#).is_err()
        );
    }

    #[test]
    fn criteria_parse_in_and_not_in() {
        assert_eq!(parse_criteria(None), Ok(PrefixCriteria::All));
        assert_eq!(
            parse_criteria(Some(r#"LogGroupNamePrefix IN ["/aws/lambda"]"#)),
            Ok(PrefixCriteria::In(vec!["/aws/lambda".into()]))
        );
        let not_in = parse_criteria(Some(r#"LogGroupNamePrefix NOT IN ["/a", "/b"]"#)).unwrap();
        assert!(not_in.matches("/c/x"));
        assert!(!not_in.matches("/b/x"));
        assert!(parse_criteria(Some(r#"LogGroupName IN ["x"]"#)).is_err());
    }

    #[test]
    fn longest_retention_wins_and_never_expire_is_longest() {
        assert!(longer(None, Some(3653)));
        assert!(longer(Some(90), Some(30)));
        assert!(!longer(Some(30), Some(30)));
    }
}
