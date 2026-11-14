//! Nested alarm configuration members (`ActionConfiguration`,
//! `WarmUpConfiguration`, `EvaluationCriteria`, `EvaluationWindow`) kept in
//! their awsQuery flat-key form and rendered back as nested XML.
//!
//! Requests over every CloudWatch protocol reach the handlers as flat
//! `A.B.member.N.C` params, so storing the keys under a member's prefix and
//! rebuilding the element tree on describe round-trips the structure exactly
//! (lists keep their order, unions keep their single arm). Each member's
//! constraints are validated against the model before it is stored.

use std::collections::BTreeMap;

use fakecloud_core::service::{AwsRequest, AwsServiceError};
use serde::{Deserialize, Serialize};

use crate::service::{invalid_param, xml_escape};

/// A nested member's flat keys (relative to its prefix) and leaf values.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FlatMembers(pub BTreeMap<String, String>);

impl FlatMembers {
    /// Collect every `<prefix>.*` request param, or `None` when the member
    /// was not supplied.
    pub fn capture(req: &AwsRequest, prefix: &str) -> Option<Self> {
        let needle = format!("{prefix}.");
        let map: BTreeMap<String, String> = req
            .query_params
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix(&needle)
                    .map(|rest| (rest.to_string(), v.clone()))
            })
            .collect();
        (!map.is_empty()).then_some(Self(map))
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// `member.N` values of the list at `key`, in index order.
    pub fn list(&self, key: &str) -> Vec<&str> {
        let needle = format!("{key}.member.");
        let mut items: Vec<(u32, &str)> = self
            .0
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix(&needle)
                    .and_then(|i| i.parse::<u32>().ok())
                    .map(|i| (i, v.as_str()))
            })
            .collect();
        items.sort_by_key(|(i, _)| *i);
        items.into_iter().map(|(_, v)| v).collect()
    }

    fn has_prefix(&self, key: &str) -> bool {
        let needle = format!("{key}.");
        self.0.keys().any(|k| k == key || k.starts_with(&needle))
    }

    /// Render as `<tag>...</tag>`, rebuilding nested elements and `<member>`
    /// list items from the flat keys.
    pub fn render(&self, tag: &str) -> String {
        let mut root = Node::default();
        for (k, v) in &self.0 {
            root.insert(&segments(k), v);
        }
        let mut out = String::new();
        out.push_str(&format!("<{tag}>"));
        root.render_children(&mut out);
        out.push_str(&format!("</{tag}>"));
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Segment {
    /// A `member.N` list item, ordered by N.
    Item(u32),
    Name(String),
}

fn segments(key: &str) -> Vec<Segment> {
    let parts: Vec<&str> = key.split('.').collect();
    let mut out = Vec::with_capacity(parts.len());
    let mut i = 0;
    while i < parts.len() {
        if parts[i] == "member" {
            if let Some(n) = parts.get(i + 1).and_then(|n| n.parse::<u32>().ok()) {
                out.push(Segment::Item(n));
                i += 2;
                continue;
            }
        }
        out.push(Segment::Name(parts[i].to_string()));
        i += 1;
    }
    out
}

#[derive(Debug, Default)]
struct Node {
    value: Option<String>,
    children: BTreeMap<Segment, Node>,
}

impl Node {
    fn insert(&mut self, path: &[Segment], value: &str) {
        match path.split_first() {
            None => self.value = Some(value.to_string()),
            Some((head, rest)) => self
                .children
                .entry(head.clone())
                .or_default()
                .insert(rest, value),
        }
    }

    fn render_children(&self, out: &mut String) {
        for (seg, child) in &self.children {
            let tag = match seg {
                Segment::Item(_) => "member",
                Segment::Name(n) => n.as_str(),
            };
            out.push_str(&format!("<{tag}>"));
            if let Some(v) = &child.value {
                out.push_str(&xml_escape(v));
            }
            child.render_children(out);
            out.push_str(&format!("</{tag}>"));
        }
    }
}

fn check_len(name: &str, v: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    let len = v.chars().count();
    if len < min || len > max {
        return Err(invalid_param(format!(
            "{name} length {len} is outside [{min}, {max}]"
        )));
    }
    Ok(())
}

fn check_int(name: &str, v: &str, min: i64, max: i64) -> Result<(), AwsServiceError> {
    let n = v
        .parse::<i64>()
        .map_err(|_| invalid_param(format!("{name} must be an integer")))?;
    if n < min || n > max {
        return Err(invalid_param(format!(
            "{name} value {n} is outside [{min}, {max}]"
        )));
    }
    Ok(())
}

fn check_bool(name: &str, v: &str) -> Result<(), AwsServiceError> {
    if v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("false") {
        Ok(())
    } else {
        Err(invalid_param(format!("{name} must be a boolean")))
    }
}

/// `ActionConfiguration` (shared by metric, composite and log alarms):
/// `AlarmTagsPropagation.Enabled` is required, each tag-key list holds 1-50
/// keys of 1-128 characters.
pub fn action_configuration(req: &AwsRequest) -> Result<Option<FlatMembers>, AwsServiceError> {
    let Some(m) = FlatMembers::capture(req, "ActionConfiguration") else {
        return Ok(None);
    };
    if m.has_prefix("AlarmTagsPropagation") {
        let enabled = m.get("AlarmTagsPropagation.Enabled").ok_or_else(|| {
            invalid_param("ActionConfiguration.AlarmTagsPropagation.Enabled is required")
        })?;
        check_bool("ActionConfiguration.AlarmTagsPropagation.Enabled", enabled)?;
        for list in ["IncludedTagKeys", "ExcludedTagKeys"] {
            let key = format!("AlarmTagsPropagation.{list}");
            if !m.has_prefix(&key) {
                continue;
            }
            let keys = m.list(&key);
            let name = format!("ActionConfiguration.{key}");
            if keys.is_empty() || keys.len() > 50 {
                return Err(invalid_param(format!(
                    "{name} must contain between 1 and 50 tag keys"
                )));
            }
            for k in keys {
                check_len(&name, k, 1, 128)?;
            }
        }
    }
    Ok(Some(m))
}

/// `WarmUpConfiguration`: required `WarmUpPeriodDurationInMinutes` in
/// 1-2880 plus an optional boolean.
pub fn warm_up_configuration(req: &AwsRequest) -> Result<Option<FlatMembers>, AwsServiceError> {
    let Some(m) = FlatMembers::capture(req, "WarmUpConfiguration") else {
        return Ok(None);
    };
    let minutes = m.get("WarmUpPeriodDurationInMinutes").ok_or_else(|| {
        invalid_param("WarmUpConfiguration.WarmUpPeriodDurationInMinutes is required")
    })?;
    check_int(
        "WarmUpConfiguration.WarmUpPeriodDurationInMinutes",
        minutes,
        1,
        2880,
    )?;
    if let Some(v) = m.get("OnlyStartEvaluatingAfterWarmUpPeriodEnds") {
        check_bool(
            "WarmUpConfiguration.OnlyStartEvaluatingAfterWarmUpPeriodEnds",
            v,
        )?;
    }
    Ok(Some(m))
}

/// `EvaluationCriteria` (union, `PromQLCriteria` arm): required `Query` of
/// 1-10000 characters, `PendingPeriod` / `RecoveryPeriod` in 0-86400.
pub fn evaluation_criteria(req: &AwsRequest) -> Result<Option<FlatMembers>, AwsServiceError> {
    let Some(m) = FlatMembers::capture(req, "EvaluationCriteria") else {
        return Ok(None);
    };
    if m.has_prefix("PromQLCriteria") {
        let query = m
            .get("PromQLCriteria.Query")
            .ok_or_else(|| invalid_param("EvaluationCriteria.PromQLCriteria.Query is required"))?;
        check_len("EvaluationCriteria.PromQLCriteria.Query", query, 1, 10000)?;
        for p in ["PendingPeriod", "RecoveryPeriod"] {
            if let Some(v) = m.get(&format!("PromQLCriteria.{p}")) {
                check_int(
                    &format!("EvaluationCriteria.PromQLCriteria.{p}"),
                    v,
                    0,
                    86400,
                )?;
            }
        }
    }
    Ok(Some(m))
}

/// `EvaluationWindow` (union of `WallClockWindow { Timezone }` and the empty
/// `SlidingWindow`): a `Timezone` is 1-50 characters.
pub fn evaluation_window(req: &AwsRequest) -> Result<Option<FlatMembers>, AwsServiceError> {
    let Some(mut m) = FlatMembers::capture(req, "EvaluationWindow") else {
        return Ok(None);
    };
    if let Some(tz) = m.get("WallClockWindow.Timezone") {
        check_len("EvaluationWindow.WallClockWindow.Timezone", tz, 1, 50)?;
    }
    // An empty `SlidingWindow` structure arrives as a bare
    // `EvaluationWindow.SlidingWindow` key; it renders as an empty element.
    if let Some(v) = m.0.get_mut("SlidingWindow") {
        v.clear();
    }
    Ok(Some(m))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_nested_members_and_ordered_lists() {
        let mut map = BTreeMap::new();
        map.insert(
            "AlarmTagsPropagation.Enabled".to_string(),
            "true".to_string(),
        );
        map.insert(
            "AlarmTagsPropagation.IncludedTagKeys.member.10".to_string(),
            "z".to_string(),
        );
        map.insert(
            "AlarmTagsPropagation.IncludedTagKeys.member.2".to_string(),
            "a&b".to_string(),
        );
        let xml = FlatMembers(map).render("ActionConfiguration");
        assert_eq!(
            xml,
            "<ActionConfiguration><AlarmTagsPropagation><Enabled>true</Enabled>\
             <IncludedTagKeys><member>a&amp;b</member><member>z</member></IncludedTagKeys>\
             </AlarmTagsPropagation></ActionConfiguration>"
        );
    }
}
