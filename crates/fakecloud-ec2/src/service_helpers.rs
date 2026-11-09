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

/// Generate an EC2 resource id: `<prefix>-<17 lowercase hex>`, matching the
/// modern long-id format (e.g. `vpc-0a1b2c3d4e5f67890`).
pub fn gen_id(prefix: &str) -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &hex[..17])
}

/// A stable id in the same `<prefix>-<17 hex>` shape as [`gen_id`], derived
/// (FNV-1a) from `key`. For catalog entries the caller never creates (AWS
/// managed endpoint services, reserved-instance offerings): their ids must be
/// the same on every describe, or a paging client sees a different catalog on
/// every page.
pub fn stable_id(prefix: &str, key: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{prefix}-{hash:017x}")
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

/// An EC2 `Invalid<Resource>.NotFound`-style error (HTTP 400, matching AWS).
pub fn not_found(code: &str, id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        code,
        format!("The ID '{id}' does not exist"),
    )
}

/// `InvalidInstanceID.NotFound` (HTTP 400) — the requested instance does not
/// exist, matching what AWS returns for state-change/describe ops on a bad id.
pub fn instance_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidInstanceID.NotFound",
        format!("The instance ID '{id}' does not exist"),
    )
}

/// `InstanceLimitExceeded` (HTTP 400) — the requested instance count exceeds
/// the limit, matching what AWS returns when MaxCount is above the per-request
/// (or account) ceiling.
pub fn instance_limit_exceeded(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InstanceLimitExceeded",
        message.into(),
    )
}

/// `IncorrectInstanceState` (HTTP 400) — an instance state-change is illegal
/// from the instance's current state (e.g. starting a terminated instance).
pub fn incorrect_instance_state(id: &str, current: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "IncorrectInstanceState",
        format!("The instance '{id}' is not in a state from which it can be modified (current state: {current})"),
    )
}

/// `IncorrectState` (HTTP 400) — the resource is in the wrong state for the
/// request (e.g. associating a second IAM instance profile with an instance).
pub fn incorrect_state(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "IncorrectState", message.into())
}

/// The `InstanceProfileId` reported for an instance profile. AWS reports the
/// profile's own id, so every association with the same profile reports the
/// same value; EC2 cannot read IAM's store (`fakecloud-ec2` does not depend on
/// `fakecloud-iam`), so both services derive it from the profile ARN through
/// the same shared helper instead of minting it independently. That holds
/// whenever the two agree on the ARN: every profile addressed by ARN, and
/// every name-addressed profile on the default path.
pub fn instance_profile_id_for(arn: &str) -> String {
    fakecloud_aws::arn::unique_id_for("AIPA", arn)
}

/// `InvalidIamInstanceProfileArn.Malformed` (HTTP 400) — the supplied IAM
/// instance-profile ARN is not a well-formed instance-profile ARN.
pub fn malformed_instance_profile_arn(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidIamInstanceProfileArn.Malformed",
        format!("The IAM instance profile ARN '{arn}' is malformed"),
    )
}

/// An EC2 ARN (`arn:<partition>:ec2:<region>:<owner>:<resource>`) in the
/// region's partition.
pub fn ec2_arn(region: &str, owner: &str, resource: &str) -> String {
    fakecloud_aws::arn::Arn::regional("ec2", region, owner, resource).to_string()
}

/// A region-less EC2 ARN (IPAM resources: `arn:<partition>:ec2::<owner>:...`)
/// in the partition of `region`.
pub fn ec2_global_arn(region: &str, owner: &str, resource: &str) -> String {
    fakecloud_aws::arn::Arn::global_in(region, "ec2", owner, resource).to_string()
}

/// Whether `arn` is a well-formed IAM instance-profile ARN:
/// `arn:<partition>:iam::<account>:instance-profile/<path><name>`.
pub fn is_instance_profile_arn(arn: &str) -> bool {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    parts.len() == 6
        && parts[0] == "arn"
        && !parts[1].is_empty()
        && parts[2] == "iam"
        && parts[3].is_empty()
        && parts[5]
            .strip_prefix("instance-profile/")
            .is_some_and(|rest| !rest.is_empty() && !rest.ends_with('/'))
}

/// Whether `name` is a legal IAM instance-profile name: `[\w+=,.@-]{1,128}`,
/// the pattern the IAM API documents for the `InstanceProfileName` member.
pub fn is_instance_profile_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_+=,.@-".contains(c))
}

/// `InvalidAssociationID.NotFound` (HTTP 400) — the requested association id
/// does not exist.
pub fn association_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidAssociationID.NotFound",
        format!("The association ID '{id}' does not exist"),
    )
}

/// Match an EC2 filter value against a candidate, honoring the `*` (any run)
/// and `?` (any single char) wildcards AWS supports in filter values. A value
/// with no wildcard is an exact match.
pub fn filter_value_matches(pattern: &str, candidate: &str) -> bool {
    if !pattern.contains('*') && !pattern.contains('?') {
        return pattern == candidate;
    }
    glob_match(pattern.as_bytes(), candidate.as_bytes())
}

/// Minimal glob matcher for `*`/`?` over bytes (EC2 filter wildcards).
fn glob_match(pat: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0usize, 0usize);
    let (mut star_p, mut star_t): (Option<usize>, usize) = (None, 0);
    while t < text.len() {
        if p < pat.len() && (pat[p] == b'?' || pat[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pat.len() && pat[p] == b'*' {
            star_p = Some(p);
            star_t = t;
            p += 1;
        } else if let Some(sp) = star_p {
            p = sp + 1;
            star_t += 1;
            t = star_t;
        } else {
            return false;
        }
    }
    while p < pat.len() && pat[p] == b'*' {
        p += 1;
    }
    p == pat.len()
}

/// Prefix inside every `NextToken` this server mints, so a token from
/// elsewhere (or a hand-edited one) is recognised as foreign.
const PAGE_TOKEN_PREFIX: &str = "fakecloud-ec2-page:";

/// Mint the opaque `NextToken` that resumes a listing at `offset`.
pub fn encode_page_token(offset: usize) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{PAGE_TOKEN_PREFIX}{offset}"))
}

/// Decode a `NextToken` minted by [`encode_page_token`]. Anything else is
/// rejected with `InvalidParameterValue` (`Invalid value '<t>' for nextToken`)
/// rather than silently restarting the caller at page one.
pub fn decode_page_token(token: &str) -> Result<usize, AwsServiceError> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .ok()
        .and_then(|raw| String::from_utf8(raw).ok())
        .and_then(|s| s.strip_prefix(PAGE_TOKEN_PREFIX)?.parse::<usize>().ok())
        .ok_or_else(|| invalid_parameter_value(format!("Invalid value '{token}' for nextToken")))
}

/// Read a request's `MaxResults` as an integer: absent or empty is `None`,
/// and a value that is not an integer is rejected (taking it as "no limit"
/// would hand back the whole set the caller asked to page). Range checks are
/// per operation.
pub fn parse_page_size(params: &HashMap<String, String>) -> Result<Option<i64>, AwsServiceError> {
    match params.get("MaxResults").filter(|v| !v.is_empty()) {
        None => Ok(None),
        Some(v) => v
            .parse::<i64>()
            .map(Some)
            .map_err(|_| invalid_parameter_value(format!("Invalid value '{v}' for maxResults"))),
    }
}

/// Apply offset-based pagination to an already-sorted item list. Returns the
/// page plus the opaque `NextToken` that fetches the rest, or `None` when the
/// page reaches the end. `max_results` of `None` means "all remaining"; a
/// `next_token` this server did not mint is an error.
pub fn paginate<T: Clone>(
    items: &[T],
    next_token: Option<&str>,
    max_results: Option<usize>,
) -> Result<(Vec<T>, Option<String>), AwsServiceError> {
    let start = match next_token.filter(|t| !t.is_empty()) {
        Some(t) => decode_page_token(t)?.min(items.len()),
        None => 0,
    };
    let end = match max_results {
        Some(n) => (start + n).min(items.len()),
        None => items.len(),
    };
    let page = items[start..end].to_vec();
    let token = (end < items.len()).then(|| encode_page_token(end));
    Ok((page, token))
}

/// Require a non-empty scalar parameter, else `MissingParameter`. Omitting a
/// required scalar is wire-observable, so the conformance harness generates a
/// negative variant for it — handlers must reject it.
pub fn require(params: &HashMap<String, String>, key: &str) -> Result<String, AwsServiceError> {
    params
        .get(key)
        .filter(|v| !v.is_empty())
        .cloned()
        .ok_or_else(|| missing_parameter(key))
}

/// Require a structure member to be present, identified by any wire param
/// under `{prefix}.` (e.g. `InstanceTagAttribute.IncludeAllTagsOfInstance`).
/// Omitting a required *structure* is wire-observable, so the harness emits a
/// `negative_omit_<Struct>` variant — handlers must reject it.
pub fn require_struct(
    params: &HashMap<String, String>,
    prefix: &str,
) -> Result<(), AwsServiceError> {
    let pat = format!("{prefix}.");
    if params.keys().any(|k| k.starts_with(&pat)) {
        Ok(())
    } else {
        Err(missing_parameter(prefix))
    }
}

/// Reject a present-but-invalid enum value (the harness's
/// `negative_invalid_enum_*` variant). Absent is allowed here — required-ness
/// is enforced separately via [`require`].
pub fn validate_enum(
    params: &HashMap<String, String>,
    key: &str,
    allowed: &[&str],
) -> Result<(), AwsServiceError> {
    if let Some(v) = params.get(key).filter(|v| !v.is_empty()) {
        if !allowed.contains(&v.as_str()) {
            return Err(invalid_parameter_value(format!(
                "Invalid value '{v}' for {key}"
            )));
        }
    }
    Ok(())
}

/// Reject an out-of-range `MaxResults` (the harness's `negative_below_min` /
/// `negative_above_max` variants). EC2 describe pages bound MaxResults to
/// [5, 1000] unless documented otherwise.
/// Whether the request is a `DryRun`.
pub(crate) fn is_dry_run(params: &HashMap<String, String>) -> bool {
    params
        .get("DryRun")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

/// `DryRunOperation`: the request would have succeeded without `DryRun`.
pub(crate) fn refuse_dry_run(params: &HashMap<String, String>) -> Result<(), AwsServiceError> {
    if is_dry_run(params) {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::PRECONDITION_FAILED,
            "DryRunOperation",
            "Request would have succeeded, but DryRun flag is set.",
        ));
    }
    Ok(())
}

pub fn validate_max_results(
    params: &HashMap<String, String>,
    min: i64,
    max: i64,
) -> Result<(), AwsServiceError> {
    if let Some(v) = params.get("MaxResults").filter(|v| !v.is_empty()) {
        if let Ok(n) = v.parse::<i64>() {
            if n < min || n > max {
                return Err(invalid_parameter_value(format!(
                    "MaxResults must be between {min} and {max}"
                )));
            }
        }
    }
    Ok(())
}

/// Reject a present integer parameter outside `[min, max]` (the harness's
/// `negative_below_min_*` / `negative_above_max_*` variants for `@range`
/// members like `PrivateIpAddressCount` or `MaxDrainDurationSeconds`).
pub fn validate_int_range(
    params: &HashMap<String, String>,
    key: &str,
    min: i64,
    max: i64,
) -> Result<(), AwsServiceError> {
    if let Some(v) = params.get(key).filter(|v| !v.is_empty()) {
        if let Ok(n) = v.parse::<i64>() {
            if n < min || n > max {
                return Err(invalid_parameter_value(format!(
                    "{key} must be between {min} and {max}"
                )));
            }
        }
    }
    Ok(())
}

/// Reject a present parameter whose length is outside `[min, max]` (the
/// harness's `negative_too_short_*` / `negative_too_long_*` variants for
/// `@length`-constrained members such as a bounded `NextToken`).
pub fn validate_length(
    params: &HashMap<String, String>,
    key: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    if let Some(v) = params.get(key) {
        let n = v.chars().count();
        if n < min || n > max {
            return Err(invalid_parameter_value(format!(
                "{key} length must be between {min} and {max}"
            )));
        }
    }
    Ok(())
}

/// Collect a 1-based indexed list, e.g. `ResourceId.1`, `ResourceId.2`, ….
///
/// EC2 list members are contiguous from index 1; collection stops at the first
/// missing index. A present-but-empty value terminates the list too — for
/// id-lists (`InstanceId.N`, `GroupId.N`, `VolumeId.N`, …) an empty member is
/// meaningless, and treating it as a terminator matches how the SDKs never emit
/// a gap. Filter *values* differ (an empty value is a legitimate member); use
/// [`indexed_list_keep_empty`] there.
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

/// Like [`indexed_list`], but a present-but-empty value (`Filter.1.Value.1=`)
/// is preserved as a legitimate empty-string member rather than terminating the
/// list — the terminator is "next index absent", not "value empty". Used for
/// filter values, where filtering for an absent/empty tag value is valid and an
/// empty value must NOT truncate (or self-referentially loop) the list.
pub fn indexed_list_keep_empty(params: &HashMap<String, String>, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        let key = format!("{prefix}.{i}");
        match params.get(&key) {
            Some(v) => out.push(v.clone()),
            None => break,
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
        let values = indexed_list_keep_empty(params, &format!("Filter.{i}.Value"));
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
    fn indexed_list_empty_value_terminates_for_id_lists() {
        // Plain id-lists keep the original semantics: a present-but-empty member
        // is meaningless and terminates (SDKs never emit an empty id member).
        let params = p(&[("InstanceId.1", ""), ("InstanceId.2", "i-2")]);
        assert_eq!(indexed_list(&params, "InstanceId"), Vec::<String>::new());
    }

    #[test]
    fn indexed_list_keep_empty_preserves_present_but_empty_value() {
        // `Filter.1.Value.1=` (explicit empty string) is a legitimate member,
        // not a terminator: it must be kept, and a following contiguous index
        // must still be collected rather than truncated at the empty one.
        let params = p(&[("Value.1", ""), ("Value.2", "x")]);
        assert_eq!(indexed_list_keep_empty(&params, "Value"), vec!["", "x"]);
    }

    #[test]
    fn indexed_list_keep_empty_then_absent_stops() {
        // A trailing empty value followed by an absent index still terminates
        // (no infinite loop on a genuinely-absent index).
        let params = p(&[("Value.1", "")]);
        assert_eq!(indexed_list_keep_empty(&params, "Value"), vec![""]);
    }

    #[test]
    fn parse_filters_keeps_empty_filter_value() {
        // Filtering for an empty/absent tag value: `Filter.1.Value.1=` must be
        // preserved as a single empty-string value, not dropped.
        let params = p(&[("Filter.1.Name", "tag:env"), ("Filter.1.Value.1", "")]);
        let filters = parse_filters(&params);
        assert_eq!(
            filters,
            vec![Filter {
                name: "tag:env".into(),
                values: vec!["".into()]
            }]
        );
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
    fn filter_wildcards() {
        assert!(filter_value_matches("web", "web"));
        assert!(!filter_value_matches("web", "web1"));
        assert!(filter_value_matches("web*", "web-prod"));
        assert!(filter_value_matches("*prod", "web-prod"));
        assert!(filter_value_matches("web*prod", "web-staging-prod"));
        assert!(filter_value_matches("we?", "web"));
        assert!(!filter_value_matches("we?", "web1"));
        assert!(filter_value_matches("*", "anything"));
        assert!(!filter_value_matches("web?", "web"));
    }

    #[test]
    fn paginate_pages_and_round_trips_token() {
        let items: Vec<i32> = (0..10).collect();
        let (page, token) = paginate(&items, None, Some(4)).unwrap();
        assert_eq!(page, vec![0, 1, 2, 3]);
        let (page2, token2) = paginate(&items, token.as_deref(), Some(4)).unwrap();
        assert_eq!(page2, vec![4, 5, 6, 7]);
        let (page3, token3) = paginate(&items, token2.as_deref(), Some(4)).unwrap();
        assert_eq!(page3, vec![8, 9]);
        assert_eq!(token3, None);
    }

    #[test]
    fn paginate_no_max_returns_all() {
        let items: Vec<i32> = (0..3).collect();
        let (page, token) = paginate(&items, None, None).unwrap();
        assert_eq!(page, items);
        assert_eq!(token, None);
    }

    #[test]
    fn page_tokens_are_opaque_and_foreign_ones_are_rejected() {
        let token = encode_page_token(42);
        assert!(
            !token.contains("42"),
            "token should not expose the offset: {token}"
        );
        assert_eq!(decode_page_token(&token).unwrap(), 42);
        for bad in ["42", "not-a-token", "Zm9v", "!!!"] {
            let err = decode_page_token(bad).unwrap_err();
            assert_eq!(err.code(), "InvalidParameterValue", "{bad}");
        }
        let items = [1, 2, 3];
        assert!(paginate(&items, Some("garbage"), Some(1)).is_err());
    }

    #[test]
    fn stable_ids_repeat_for_a_key_and_differ_across_keys() {
        let a = stable_id("vpce-svc", "com.amazonaws.us-east-1.s3");
        assert_eq!(a, stable_id("vpce-svc", "com.amazonaws.us-east-1.s3"));
        assert_ne!(a, stable_id("vpce-svc", "com.amazonaws.us-east-1.ec2"));
        assert_eq!(a.len(), "vpce-svc-".len() + 17, "{a}");
    }

    #[test]
    fn page_size_must_be_an_integer() {
        assert_eq!(parse_page_size(&p(&[])).unwrap(), None);
        assert_eq!(parse_page_size(&p(&[("MaxResults", "")])).unwrap(), None);
        assert_eq!(
            parse_page_size(&p(&[("MaxResults", "7")])).unwrap(),
            Some(7)
        );
        // Range checks (including whether 0 is allowed) are per operation.
        assert_eq!(
            parse_page_size(&p(&[("MaxResults", "0")])).unwrap(),
            Some(0)
        );
        assert_eq!(
            parse_page_size(&p(&[("MaxResults", "-1")])).unwrap(),
            Some(-1)
        );
        for bad in ["abc", "1.5"] {
            assert!(
                parse_page_size(&p(&[("MaxResults", bad)])).is_err(),
                "{bad}"
            );
        }
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
