//! Parsing and evaluation of the RDS `Filters` request member.
//!
//! RDS Describe* operations take a `FilterList` whose Smithy shape names
//! the list member `Filter` and the value member `Value`, so the
//! query-protocol wire form is:
//!
//! ```text
//! Filters.Filter.1.Name=dbi-resource-id
//! Filters.Filter.1.Values.Value.1=db-5e1b7794bef34787abd8310859cc93a2
//! Filters.Filter.1.Values.Value.2=db-1b8e439e59564e849b38b04191c40b8e
//! ```
//!
//! Older SDKs (and hand-rolled clients) sometimes emit the generic
//! `member` element name instead, so both spellings are accepted.
//!
//! Semantics, per the AWS docs: filters are AND-ed with each other and
//! with the operation's own identifier parameter; the values inside one
//! filter are OR-ed. Names and values are case-sensitive, and wildcards
//! are not supported.
//!
//! Real RDS rejects a filter name an operation doesn't support with
//! `InvalidParameterValue`. We can't: that error is not declared on any
//! of the Describe* operations in the Smithy model, so returning it
//! would emit an undeclared error shape and fail conformance. The
//! closest in-shape behaviour is to treat an unrecognized filter (and a
//! filter carrying no values) as matching no resource, which is what the
//! per-operation matchers below do — a caller filtering on something we
//! don't recognise gets an empty result rather than the whole list.

use fakecloud_core::service::AwsRequest;

/// Look a parameter up by presence, keeping an explicitly-empty value.
///
/// `optional_query_param` treats `Key=` as absent, which would truncate a
/// filter's value list at a blank member (`Values.Value.1=` followed by
/// `Values.Value.2=mysql` would parse as no values at all, and the filter
/// would then match nothing). AWS keeps the non-empty siblings, so the
/// list walk has to distinguish "present but empty" from "absent".
fn present_param(req: &AwsRequest, key: &str) -> Option<String> {
    req.query_params.get(key).cloned()
}

/// One `Filters.Filter.N` entry: a name plus the values it accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RdsFilter {
    pub name: String,
    pub values: Vec<String>,
}

impl RdsFilter {
    /// True when `candidate` is one of the filter's values. A `None`
    /// candidate (the resource doesn't carry the attribute at all) never
    /// matches, which is what AWS does for e.g. `domain` on an instance
    /// with no Active Directory membership.
    pub fn matches(&self, candidate: Option<&str>) -> bool {
        match candidate {
            Some(value) => self.values.iter().any(|v| v == value),
            None => false,
        }
    }

    /// True when any of `candidates` is one of the filter's values. Used
    /// for filters AWS documents as accepting "identifiers and ARNs".
    pub fn matches_any<'a>(&self, candidates: impl IntoIterator<Item = Option<&'a str>>) -> bool {
        candidates.into_iter().any(|c| self.matches(c))
    }
}

/// Rebuild an RDS ARN for a sibling resource of `arn` (same partition,
/// region and account) with a different resource type and id, so filters
/// AWS documents as accepting "identifiers and ARNs" can be matched
/// against the ARN form too. Returns `None` for a malformed ARN.
pub(crate) fn sibling_rds_arn(arn: &str, resource_type: &str, id: &str) -> Option<String> {
    let prefix: Vec<&str> = arn.splitn(7, ':').take(5).collect();
    if prefix.len() < 5 {
        return None;
    }
    Some(format!("{}:{resource_type}:{id}", prefix.join(":")))
}

/// Normalize an identifier request parameter: an explicitly-empty value
/// means "absent" (AWS ignores it rather than matching the empty string),
/// and an ARN is reduced to its resource segment because clients pass
/// either form — the Terraform provider stores full ARNs in
/// `snapshot_identifier`.
///
/// The reduction is guarded on the `arn:` prefix rather than splitting on
/// the last colon of any value: AWS's own automated-snapshot identifiers
/// carry one (`rds:mydb-2026-08-30-06-00`), and blindly trimming would
/// turn a real id into a lookup miss.
///
/// Note this is the opposite of a filter *value*, where an explicit empty
/// string is a legitimate member to match on (see `present_param`).
pub(crate) fn normalized_identifier(param: Option<String>) -> Option<String> {
    param
        .filter(|value| !value.is_empty())
        .map(|value| match value.starts_with("arn:") {
            true => value.rsplit(':').next().unwrap_or(&value).to_string(),
            false => value,
        })
        .filter(|value| !value.is_empty())
}

/// Parse `Filters.Filter.N.Name` + `Filters.Filter.N.Values.Value.M` (and
/// the `member` spelling of either element) into filter entries.
///
/// Indices are 1-based and contiguous; parsing stops at the first gap,
/// matching how the SDKs serialize the list.
pub(crate) fn parse_filters(req: &AwsRequest) -> Vec<RdsFilter> {
    let mut filters = Vec::new();

    for index in 1.. {
        let Some((prefix, name)) = ["Filter", "member"].iter().find_map(|element| {
            let prefix = format!("Filters.{element}.{index}");
            present_param(req, &format!("{prefix}.Name")).map(|name| (prefix, name))
        }) else {
            break;
        };

        let mut values = Vec::new();
        for element in ["Value", "member"] {
            for value_index in 1.. {
                let key = format!("{prefix}.Values.{element}.{value_index}");
                match present_param(req, &key) {
                    Some(value) => values.push(value),
                    None => break,
                }
            }
            if !values.is_empty() {
                break;
            }
        }

        filters.push(RdsFilter { name, values });
    }

    filters
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use std::collections::HashMap;

    fn request(params: &[(&str, &str)]) -> AwsRequest {
        let mut query_params =
            HashMap::from([("Action".to_string(), "DescribeDBInstances".to_string())]);
        for (key, value) in params {
            query_params.insert((*key).to_string(), (*value).to_string());
        }
        AwsRequest {
            service: "rds".to_string(),
            action: "DescribeDBInstances".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn parses_filter_element_spelling() {
        let filters = parse_filters(&request(&[
            ("Filters.Filter.1.Name", "dbi-resource-id"),
            ("Filters.Filter.1.Values.Value.1", "db-a"),
            ("Filters.Filter.1.Values.Value.2", "db-b"),
            ("Filters.Filter.2.Name", "engine"),
            ("Filters.Filter.2.Values.Value.1", "mysql"),
        ]));

        assert_eq!(
            filters,
            vec![
                RdsFilter {
                    name: "dbi-resource-id".to_string(),
                    values: vec!["db-a".to_string(), "db-b".to_string()],
                },
                RdsFilter {
                    name: "engine".to_string(),
                    values: vec!["mysql".to_string()],
                },
            ]
        );
    }

    #[test]
    fn parses_member_element_spelling() {
        let filters = parse_filters(&request(&[
            ("Filters.member.1.Name", "engine"),
            ("Filters.member.1.Values.member.1", "postgres"),
        ]));

        assert_eq!(
            filters,
            vec![RdsFilter {
                name: "engine".to_string(),
                values: vec!["postgres".to_string()],
            }]
        );
    }

    #[test]
    fn keeps_an_explicitly_empty_value() {
        // `Value.1=` is a legitimate member (filtering for a blank
        // attribute); dropping it must not truncate the list and lose
        // `Value.2`.
        let filters = parse_filters(&request(&[
            ("Filters.Filter.1.Name", "engine"),
            ("Filters.Filter.1.Values.Value.1", ""),
            ("Filters.Filter.1.Values.Value.2", "mysql"),
        ]));

        assert_eq!(
            filters,
            vec![RdsFilter {
                name: "engine".to_string(),
                values: vec![String::new(), "mysql".to_string()],
            }]
        );
    }

    #[test]
    fn stops_at_first_index_gap() {
        let filters = parse_filters(&request(&[
            ("Filters.Filter.1.Name", "engine"),
            ("Filters.Filter.1.Values.Value.1", "mysql"),
            ("Filters.Filter.3.Name", "domain"),
            ("Filters.Filter.3.Values.Value.1", "d-1"),
        ]));

        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn absent_filters_parse_to_empty() {
        assert!(parse_filters(&request(&[("MaxRecords", "20")])).is_empty());
    }

    #[test]
    fn filter_without_values_matches_nothing() {
        let filter = RdsFilter {
            name: "engine".to_string(),
            values: Vec::new(),
        };

        assert!(!filter.matches(Some("mysql")));
    }

    #[test]
    fn matching_is_case_sensitive_and_ors_values() {
        let filter = RdsFilter {
            name: "engine".to_string(),
            values: vec!["mysql".to_string(), "postgres".to_string()],
        };

        assert!(filter.matches(Some("mysql")));
        assert!(filter.matches(Some("postgres")));
        assert!(!filter.matches(Some("MySQL")));
        assert!(!filter.matches(None));
    }

    #[test]
    fn matches_any_accepts_alternate_attributes() {
        let filter = RdsFilter {
            name: "db-instance-id".to_string(),
            values: vec!["arn:aws:rds:us-east-1:000000000000:db:mydb".to_string()],
        };

        assert!(filter.matches_any([
            Some("mydb-other"),
            Some("arn:aws:rds:us-east-1:000000000000:db:mydb"),
        ]));
        assert!(!filter.matches_any([Some("mydb-other"), None]));
    }

    #[test]
    fn normalized_identifier_drops_empty_and_reduces_arns() {
        assert_eq!(normalized_identifier(None), None);
        // `Key=` reaches handlers as Some("") and means "not supplied".
        assert_eq!(normalized_identifier(Some(String::new())), None);
        assert_eq!(
            normalized_identifier(Some("snap-1".to_string())),
            Some("snap-1".to_string())
        );
        assert_eq!(
            normalized_identifier(Some(
                "arn:aws:rds:us-east-1:123456789012:cluster-snapshot:snap-1".to_string()
            )),
            Some("snap-1".to_string())
        );
        // AWS's automated-snapshot ids carry a colon and are NOT ARNs;
        // trimming at the last colon would turn a real id into a miss.
        assert_eq!(
            normalized_identifier(Some("rds:mydb-2026-08-30-06-00".to_string())),
            Some("rds:mydb-2026-08-30-06-00".to_string())
        );
    }

    #[test]
    fn sibling_arn_swaps_resource_type_and_id() {
        assert_eq!(
            sibling_rds_arn(
                "arn:aws:rds:us-east-1:123456789012:db:mydb",
                "cluster",
                "myclu"
            ),
            Some("arn:aws:rds:us-east-1:123456789012:cluster:myclu".to_string())
        );
    }

    #[test]
    fn sibling_arn_rejects_malformed_input() {
        assert_eq!(sibling_rds_arn("not-an-arn", "db", "mydb"), None);
    }
}
