//! Lambda's tag rules, shared by every operation that writes tags
//! (`TagResource`, `UntagResource` and the `Tags` member of
//! `CreateFunction`, `CreateEventSourceMapping`, `CreateCodeSigningConfig`
//! and `CreateCapacityProvider`) and by the CloudFormation provisioner.
//!
//! Two layers, checked in the order Lambda checks them:
//!
//! - The model constraints on `TagKey` (length 1..128) and `TagValue`
//!   (length 0..256), both with the pattern
//!   `^([\p{L}\p{Z}\p{N}_.:/=+\-@]*)$`, refused as Lambda's input
//!   validation `ValidationException`.
//! - The AWS tagging rules: the `aws:` key prefix is reserved for AWS, and a
//!   resource holds at most 50 tags (counting the ones it already has),
//!   refused as `InvalidParameterValueException`.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use fakecloud_core::service::AwsServiceError;
use http::StatusCode;
use regex::Regex;

/// The most tags a Lambda resource can hold.
pub const MAX_TAGS_PER_RESOURCE: usize = 50;

/// The `TagKey` / `TagValue` pattern from the model.
pub const TAG_PATTERN: &str = r"^([\p{L}\p{Z}\p{N}_.:/=+\-@]*)$";

const KEY_MAX: usize = 128;
const VALUE_MAX: usize = 256;

fn tag_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(TAG_PATTERN).expect("tag pattern compiles"))
}

/// The violated member constraints of one `TagKey` / `TagValue`, in the
/// order Lambda lists them.
fn string_violations(s: &str, min: usize, max: usize) -> Vec<String> {
    let len = s.chars().count();
    let mut out = Vec::new();
    if len > max {
        out.push(format!(
            "Member must have length less than or equal to {max}"
        ));
    }
    if len < min {
        out.push(format!(
            "Member must have length greater than or equal to {min}"
        ));
    }
    if !tag_regex().is_match(s) {
        out.push(format!(
            "Member must satisfy regular expression pattern: {TAG_PATTERN}"
        ));
    }
    out
}

/// Union of the violations across `items`, keeping first-seen order.
fn collect_violations<'a>(
    items: impl Iterator<Item = &'a str>,
    min: usize,
    max: usize,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for item in items {
        for v in string_violations(item, min, max) {
            if !out.contains(&v) {
                out.push(v);
            }
        }
    }
    out
}

fn validation_exception(errors: &[String]) -> AwsServiceError {
    let count = errors.len();
    let noun = if count == 1 { "error" } else { "errors" };
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("{count} validation {noun} detected: {}", errors.join("; ")),
    )
}

fn invalid_parameter(message: String) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        message,
    )
}

/// A tag map as Lambda's validator prints it (`{k1=v1, k2=v2}`).
fn display_map(tags: &BTreeMap<String, String>) -> String {
    let inner: Vec<String> = tags.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{{{}}}", inner.join(", "))
}

fn reserved_key(key: &str) -> bool {
    key.get(..4).is_some_and(|p| p.eq_ignore_ascii_case("aws:"))
}

/// Check the model constraints of a `Tags` map (member `tags`).
pub fn validate_tag_shapes(tags: &BTreeMap<String, String>) -> Result<(), AwsServiceError> {
    let shown = display_map(tags);
    let mut errors = Vec::new();
    let keys = collect_violations(tags.keys().map(String::as_str), 1, KEY_MAX);
    if !keys.is_empty() {
        errors.push(format!(
            "Value '{shown}' at 'tags' failed to satisfy constraint: Map keys must satisfy \
             constraint: [{}]",
            keys.join(", ")
        ));
    }
    let values = collect_violations(tags.values().map(String::as_str), 0, VALUE_MAX);
    if !values.is_empty() {
        errors.push(format!(
            "Value '{shown}' at 'tags' failed to satisfy constraint: Map value must satisfy \
             constraint: [{}]",
            values.join(", ")
        ));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(validation_exception(&errors))
    }
}

/// Refuse tag keys in the reserved `aws:` namespace.
fn check_reserved<'a>(mut keys: impl Iterator<Item = &'a String>) -> Result<(), AwsServiceError> {
    match keys.find(|k| reserved_key(k)) {
        Some(key) => Err(invalid_parameter(format!(
            "Tag key '{key}' is invalid: the aws: prefix is reserved for AWS use."
        ))),
        None => Ok(()),
    }
}

/// Check tags a request adds to a resource that already holds `existing`:
/// the model constraints, the reserved prefix, and the per-resource limit on
/// the tags the resource would end up with.
pub fn validate_tags_added(
    existing: &BTreeMap<String, String>,
    added: &BTreeMap<String, String>,
) -> Result<(), AwsServiceError> {
    validate_tag_shapes(added)?;
    check_reserved(added.keys())?;
    let total = existing
        .keys()
        .chain(added.keys().filter(|k| !existing.contains_key(*k)))
        .filter(|k| !reserved_key(k))
        .count();
    if total > MAX_TAGS_PER_RESOURCE {
        return Err(invalid_parameter(format!(
            "Number of tags exceeds resource tag limit. A resource can have at most \
             {MAX_TAGS_PER_RESOURCE} tags."
        )));
    }
    Ok(())
}

/// Check the tags a create request (or a CloudFormation resource, whose
/// tags replace the old ones) sets on a resource.
pub fn validate_tags(tags: &BTreeMap<String, String>) -> Result<(), AwsServiceError> {
    validate_tags_added(&BTreeMap::new(), tags)
}

/// Check an `UntagResource` `TagKeys` list (member `tagKeys`): the model
/// constraints of every `TagKey`, then the reserved prefix, which no caller
/// can remove either.
pub fn validate_tag_keys(keys: &[String]) -> Result<(), AwsServiceError> {
    let violations = collect_violations(keys.iter().map(String::as_str), 1, KEY_MAX);
    if !violations.is_empty() {
        return Err(validation_exception(&[format!(
            "Value '[{}]' at 'tagKeys' failed to satisfy constraint: Member must satisfy \
             constraint: [{}]",
            keys.join(", "),
            violations.join(", ")
        )]));
    }
    check_reserved(keys.iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn code(e: &AwsServiceError) -> &str {
        e.code()
    }

    #[test]
    fn valid_tags_pass() {
        validate_tags(&map(&[
            ("team", "data"),
            ("Name", ""),
            ("a.b:c/d=e+f-g@h", "ünï 1"),
        ]))
        .unwrap();
    }

    #[test]
    fn key_length_bounds_are_validation_exceptions() {
        let long = "k".repeat(129);
        let err = validate_tags(&map(&[(long.as_str(), "v")])).unwrap_err();
        assert_eq!(code(&err), "ValidationException");
        assert!(err.message().contains(
            "Map keys must satisfy constraint: [Member must have length less than or equal to 128]"
        ));
        let err = validate_tags(&map(&[("", "v")])).unwrap_err();
        assert!(err
            .message()
            .contains("Member must have length greater than or equal to 1"));
        validate_tags(&map(&[("k".repeat(128).as_str(), "v")])).unwrap();
    }

    #[test]
    fn value_length_and_pattern_are_validation_exceptions() {
        let long = "v".repeat(257);
        let err = validate_tags(&map(&[("k", long.as_str())])).unwrap_err();
        assert_eq!(code(&err), "ValidationException");
        assert!(err.message().contains("Map value must satisfy constraint"));
        validate_tags(&map(&[("k", "v".repeat(256).as_str())])).unwrap();

        let err = validate_tags(&map(&[("bad#key", "v")])).unwrap_err();
        assert!(err
            .message()
            .contains("Member must satisfy regular expression pattern"));
        let err = validate_tags(&map(&[("k", "bad*value")])).unwrap_err();
        assert!(err.message().contains("Map value must satisfy"));
    }

    #[test]
    fn key_and_value_errors_are_both_reported() {
        let err = validate_tags(&map(&[("bad#", "bad*")])).unwrap_err();
        assert!(err.message().starts_with("2 validation errors detected"));
    }

    #[test]
    fn reserved_prefix_is_invalid_parameter() {
        for key in ["aws:owner", "AWS:owner", "Aws:x"] {
            let err = validate_tags(&map(&[(key, "v")])).unwrap_err();
            assert_eq!(code(&err), "InvalidParameterValueException", "{key}");
        }
        validate_tags(&map(&[("awsx", "v"), ("my:aws:key", "aws:value")])).unwrap();
    }

    #[test]
    fn limit_counts_existing_tags() {
        let fifty: BTreeMap<String, String> = (0..50)
            .map(|i| (format!("k{i}"), "v".to_string()))
            .collect();
        validate_tags(&fifty).unwrap();
        let mut fifty_one = fifty.clone();
        fifty_one.insert("extra".into(), "v".into());
        let err = validate_tags(&fifty_one).unwrap_err();
        assert_eq!(code(&err), "InvalidParameterValueException");

        let existing: BTreeMap<String, String> = (0..49)
            .map(|i| (format!("k{i}"), "v".to_string()))
            .collect();
        validate_tags_added(&existing, &map(&[("new", "v")])).unwrap();
        // Overwriting an existing key adds nothing to the count.
        let full = fifty;
        validate_tags_added(&full, &map(&[("k0", "changed")])).unwrap();
        let err = validate_tags_added(&full, &map(&[("new", "v")])).unwrap_err();
        assert_eq!(code(&err), "InvalidParameterValueException");
    }

    #[test]
    fn tag_keys_are_validated() {
        validate_tag_keys(&["team".into(), "a.b".into()]).unwrap();
        let err = validate_tag_keys(&["k".repeat(129)]).unwrap_err();
        assert_eq!(code(&err), "ValidationException");
        assert!(err.message().contains("at 'tagKeys'"));
        let err = validate_tag_keys(&[String::new()]).unwrap_err();
        assert_eq!(code(&err), "ValidationException");
        let err = validate_tag_keys(&["bad#".into()]).unwrap_err();
        assert_eq!(code(&err), "ValidationException");
        let err = validate_tag_keys(&["aws:x".into()]).unwrap_err();
        assert_eq!(code(&err), "InvalidParameterValueException");
    }
}
