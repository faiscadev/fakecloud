//! Shared EC2 request-parsing and error helpers.
//!
//! EC2's query encoding uses 1-based indexed list members (`ResourceId.1`,
//! `Tag.2.Key`) and a uniform `Filter.N.Name` / `Filter.N.Value.M` shape on
//! every `Describe*` operation. These helpers parse those shapes once so every
//! resource-family batch reuses them rather than re-deriving the indexing.

use std::collections::HashMap;

use http::StatusCode;

use fakecloud_core::service::AwsServiceError;

/// An EC2 `Filter.N` entry: a name and one or more accepted values (OR within a
/// filter, AND across filters — AWS semantics).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filter {
    pub name: String,
    pub values: Vec<String>,
}

/// `InvalidParameterValue` — the catch-all 400 for bad EC2 input.
pub fn invalid_parameter_value(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        message.into(),
    )
}

/// `MissingParameter` — a required parameter was absent.
pub fn missing_parameter(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MissingParameter",
        format!("The request must contain the parameter {name}"),
    )
}

/// Collect a 1-based indexed list, e.g. `ResourceId.1`, `ResourceId.2`, ….
///
/// EC2 list members are contiguous from index 1; collection stops at the first
/// missing index. Empty values terminate the list too (matching how the SDKs
/// never emit a gap).
pub fn indexed_list(params: &HashMap<String, String>, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        let key = format!("{prefix}.{i}");
        match params.get(&key) {
            Some(v) if !v.is_empty() => out.push(v.clone()),
            _ => break,
        }
        i += 1;
    }
    out
}

/// Parse `Filter.N.Name` + `Filter.N.Value.M` into [`Filter`] entries.
pub fn parse_filters(params: &HashMap<String, String>) -> Vec<Filter> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        let name_key = format!("Filter.{i}.Name");
        let Some(name) = params.get(&name_key).filter(|v| !v.is_empty()) else {
            break;
        };
        let values = indexed_list(params, &format!("Filter.{i}.Value"));
        out.push(Filter {
            name: name.clone(),
            values,
        });
        i += 1;
    }
    out
}

/// Parse `{prefix}.N.Key` + `{prefix}.N.Value` tag pairs (the request shape for
/// `CreateTags`/`DeleteTags` and `TagSpecification.N.Tag.M`).
///
/// The value is `None` only when the `Value` parameter is *absent* — for
/// `DeleteTags` that means "remove this key regardless of value". A *present*
/// `Value` (including an explicit empty string `Value=`) is preserved as
/// `Some(value)` so DeleteTags can match the empty-value tag specifically
/// rather than collapsing it into a key-only delete.
pub fn parse_tag_pairs(
    params: &HashMap<String, String>,
    prefix: &str,
) -> Vec<(String, Option<String>)> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        let key_param = format!("{prefix}.{i}.Key");
        let Some(key) = params.get(&key_param).filter(|v| !v.is_empty()) else {
            break;
        };
        let value = params.get(&format!("{prefix}.{i}.Value")).cloned();
        out.push((key.clone(), value));
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn indexed_list_collects_contiguous_then_stops() {
        let params = p(&[("ResourceId.1", "vpc-1"), ("ResourceId.2", "vpc-2")]);
        assert_eq!(indexed_list(&params, "ResourceId"), vec!["vpc-1", "vpc-2"]);
    }

    #[test]
    fn indexed_list_stops_at_gap() {
        let params = p(&[("ResourceId.1", "vpc-1"), ("ResourceId.3", "vpc-3")]);
        assert_eq!(indexed_list(&params, "ResourceId"), vec!["vpc-1"]);
    }

    #[test]
    fn parse_filters_groups_name_and_values() {
        let params = p(&[
            ("Filter.1.Name", "resource-id"),
            ("Filter.1.Value.1", "vpc-1"),
            ("Filter.1.Value.2", "vpc-2"),
            ("Filter.2.Name", "key"),
            ("Filter.2.Value.1", "Name"),
        ]);
        let filters = parse_filters(&params);
        assert_eq!(filters.len(), 2);
        assert_eq!(
            filters[0],
            Filter {
                name: "resource-id".into(),
                values: vec!["vpc-1".into(), "vpc-2".into()]
            }
        );
        assert_eq!(
            filters[1],
            Filter {
                name: "key".into(),
                values: vec!["Name".into()]
            }
        );
    }

    #[test]
    fn parse_tag_pairs_handles_optional_value() {
        let params = p(&[
            ("Tag.1.Key", "Name"),
            ("Tag.1.Value", "web"),
            ("Tag.2.Key", "env"),
        ]);
        let tags = parse_tag_pairs(&params, "Tag");
        assert_eq!(
            tags,
            vec![("Name".into(), Some("web".into())), ("env".into(), None)]
        );
    }

    #[test]
    fn parse_tag_pairs_distinguishes_empty_value_from_absent() {
        // Present-but-empty `Value=` -> Some(""), absent `Value` -> None.
        // DeleteTags relies on this: `Value=` deletes only the empty-value tag,
        // while an absent value deletes the key regardless of value.
        let params = p(&[("Tag.1.Key", "a"), ("Tag.1.Value", ""), ("Tag.2.Key", "b")]);
        let tags = parse_tag_pairs(&params, "Tag");
        assert_eq!(
            tags,
            vec![("a".into(), Some("".into())), ("b".into(), None)]
        );
    }
}
