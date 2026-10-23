//! Application status checks: an HTTP/HTTPS probe EC2 runs against instances,
//! plus the associations that decide which instances a check applies to and
//! the per-instance suppression windows.
//!
//! Associations are the interesting part: a check reaches an instance either
//! by instance id or by tag, and `DescribeApplicationStatus` resolves both —
//! so tagging an instance after the fact brings it under a tag-associated
//! check without re-associating.
//!
//! `ClientToken` is an idempotency token on every mutating operation here.
//! Only `CreateApplicationStatusCheck` can mint a duplicate on a retry, so
//! that is the one that records the token and replays its original result;
//! the rest converge on the same state when replayed (Modify writes the same
//! fields, Delete tombstones an already-tombstoned check, Associate and the
//! suppression pair are set operations).

use chrono::Utc;

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::tags::{apply_tag_specifications, tag_set_xml, tag_specifications_for};
use crate::service::Ec2Service;
use crate::service_helpers::{
    filter_value_matches, gen_id, indexed_list, instance_not_found, invalid_parameter_value,
    not_found, parse_tag_pairs, require, validate_enum, validate_int_range, validate_length,
    Filter,
};
use crate::state::{ApplicationStatusCheck, ApplicationStatusSuppression, Ec2State, Tag};

/// The `ResourceType` a `TagSpecification.N` block must name to tag a check on
/// create, and the key the shared tag store files those tags under.
const CHECK_RESOURCE_TYPE: &str = "application-status-check";

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn dry_run(req: &AwsRequest) -> bool {
    req.query_params
        .get("DryRun")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

/// `TargetTagAssociation.N.Key` / `.Value` pairs. The member is
/// `TargetTagAssociations`, but its `xmlName` is singular, so that is what
/// official clients put on the wire; the plural spelling is accepted too.
fn tag_associations(req: &AwsRequest) -> Vec<(String, String)> {
    let mut pairs = parse_tag_pairs(&req.query_params, "TargetTagAssociation");
    if pairs.is_empty() {
        pairs = parse_tag_pairs(&req.query_params, "TargetTagAssociations");
    }
    pairs
        .into_iter()
        .map(|(k, v)| (k, v.unwrap_or_default()))
        .collect()
}

/// The `AssociationType` reported on an association result. The result objects
/// are documented with `EC2TAG` and `INSTANCE_ID`, which is a different
/// spelling from the `AssociationTypeEnum` (`tag`, `instance-id`) that the
/// describe-associations response uses.
const RESULT_TYPE_TAG: &str = "EC2TAG";
const RESULT_TYPE_INSTANCE: &str = "INSTANCE_ID";

fn check_xml(c: &ApplicationStatusCheck, tags: &[Tag]) -> String {
    let mut s = String::new();
    s.push_str(&ec2_elem("applicationStatusCheckId", &c.id));
    s.push_str(&ec2_elem("aggregation", &c.aggregation));
    s.push_str(&ec2_elem("protocol", &c.protocol));
    s.push_str(&format!("<port>{}</port>", c.port));
    if let Some(p) = &c.path {
        s.push_str(&ec2_elem("path", p));
    }
    for (tag, v) in [
        ("deviceIndex", c.device_index),
        ("interval", c.interval),
        ("timeout", c.timeout),
        ("failureThreshold", c.failure_threshold),
        ("successThreshold", c.success_threshold),
        (
            "initializationGracePeriodSeconds",
            c.initialization_grace_period_seconds,
        ),
    ] {
        if let Some(v) = v {
            s.push_str(&format!("<{tag}>{v}</{tag}>"));
        }
    }
    for (tag, v) in [
        ("ipVersion", &c.ip_version),
        ("ipScope", &c.ip_scope),
        ("statusCodeMatcher", &c.status_code_matcher),
    ] {
        if let Some(v) = v {
            s.push_str(&ec2_elem(tag, v));
        }
    }
    s.push_str(&health_check_paths_xml(&c.health_check_paths));
    s.push_str(&ec2_elem("creationTime", &c.creation_time));
    s.push_str(&ec2_elem("modifyTime", &c.modify_time));
    // `lastUpdatedAt` is the last time the check object changed, which is
    // exactly what `modify_time` records — create and every Modify/Associate
    // stamp it.
    s.push_str(&ec2_elem("lastUpdatedAt", &c.modify_time));
    if let Some(d) = &c.deletion_time {
        s.push_str(&ec2_elem("deletionTime", d));
    }
    let pairs: Vec<String> = c
        .tag_associations
        .iter()
        .map(|(k, v)| format!("{}{}", ec2_elem("key", k), ec2_elem("value", v)))
        .collect();
    // EC2 renders an empty list as `<set/>` rather than omitting it, so SDKs
    // see an empty list instead of a missing member.
    s.push_str(&ec2_list("targetTagAssociationSet", &pairs));
    s.push_str(&tag_set_xml(tags));
    s
}

/// Validate `StatusCodeMatcher`: a comma-separated list of individual HTTP
/// status codes or `low-high` ranges, where the first value of a range must be
/// less than the second.
fn validate_status_code_matcher(req: &AwsRequest) -> Result<(), AwsServiceError> {
    let Some(raw) = str_param(req, "StatusCodeMatcher") else {
        return Ok(());
    };
    // "Maximum length: 64 characters."
    validate_length(&req.query_params, "StatusCodeMatcher", 0, 64)?;
    let bad = || invalid_parameter_value(format!("Invalid value '{raw}' for StatusCodeMatcher"));
    // A status code is exactly three digits, so parsing one is the whole test.
    let code = |s: &str| {
        (s.len() == 3 && s.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse::<u32>().ok())
            .flatten()
    };
    for part in raw.split(',') {
        let part = part.trim();
        match part.split_once('-') {
            Some((low, high)) => {
                let (Some(low), Some(high)) = (code(low), code(high)) else {
                    return Err(bad());
                };
                if low >= high {
                    return Err(invalid_parameter_value(
                        "StatusCodeMatcher range must start below where it ends",
                    ));
                }
            }
            None => {
                if code(part).is_none() {
                    return Err(bad());
                }
            }
        }
    }
    Ok(())
}

/// Validate the probe settings shared by Create and Modify. The bounds come
/// from the member documentation in the EC2 model — the members are plain
/// `Integer`s with no `@range` trait, so the documented values are the only
/// source for them.
fn validate_probe(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_enum(&req.query_params, "Protocol", &["http", "https"])?;
    validate_enum(&req.query_params, "Aggregation", &["included", "excluded"])?;
    validate_enum(&req.query_params, "IpVersion", &["ipv4", "ipv6"])?;
    validate_enum(&req.query_params, "IpScope", &["private"])?;
    // `PortNumber` and `InitializationGracePeriodSeconds` do carry `@range`.
    validate_int_range(&req.query_params, "Port", 1, 65_535)?;
    validate_int_range(
        &req.query_params,
        "InitializationGracePeriodSeconds",
        -1,
        600,
    )?;
    // "Valid value: 60." — the interval is not tunable.
    if let Some(v) = int_param(req, "Interval") {
        if v != 60 {
            return Err(invalid_parameter_value("Interval must be 60"));
        }
    }
    // "Valid values: 1 to 30."
    validate_int_range(&req.query_params, "Timeout", 1, 30)?;
    // "The value must be greater than 0." — documented with no upper bound.
    for key in ["FailureThreshold", "SuccessThreshold"] {
        if int_param(req, key).is_some_and(|v| v <= 0) {
            return Err(invalid_parameter_value(format!(
                "{key} must be greater than 0"
            )));
        }
    }
    // DeviceIndex names a network interface slot, so it cannot be negative.
    // The model leaves it an unbounded Integer, but no slot below zero exists.
    if int_param(req, "DeviceIndex").is_some_and(|v| v < 0) {
        return Err(invalid_parameter_value("DeviceIndex must not be negative"));
    }
    validate_status_code_matcher(req)?;
    Ok(())
}

/// A probe that times out no sooner than it repeats can never report a result
/// before the next attempt starts. Both values are optional on the wire, so
/// the check runs against the values the check will *hold* after the request
/// is applied, not only against the pair that arrived together.
fn validate_timeout_against_interval(
    interval: Option<i64>,
    timeout: Option<i64>,
) -> Result<(), AwsServiceError> {
    if let (Some(i), Some(t)) = (interval, timeout) {
        if t >= i {
            return Err(invalid_parameter_value(
                "Timeout must be less than Interval",
            ));
        }
    }
    Ok(())
}

fn int_param(req: &AwsRequest, key: &str) -> Option<i64> {
    req.query_params.get(key).and_then(|v| v.parse().ok())
}

/// Reject a present-but-unparseable integer instead of silently treating it as
/// absent, which would let a malformed Port fall back to the default.
fn require_int_params(req: &AwsRequest, keys: &[&str]) -> Result<(), AwsServiceError> {
    for key in keys {
        if let Some(v) = req.query_params.get(*key).filter(|v| !v.is_empty()) {
            if v.parse::<i64>().is_err() {
                return Err(invalid_parameter_value(format!(
                    "Invalid value '{v}' for {key}"
                )));
            }
        }
    }
    Ok(())
}

/// Require an integer parameter to be both present and parseable, so a
/// required int is read exactly once instead of being re-parsed behind a
/// fallback that can never fire.
fn require_int(req: &AwsRequest, key: &str) -> Result<i64, AwsServiceError> {
    let raw = require(&req.query_params, key)?;
    raw.parse::<i64>()
        .map_err(|_| invalid_parameter_value(format!("Invalid value '{raw}' for {key}")))
}

/// The optional probe parameters every Create/Modify validates the same way.
/// `Port` is required on Create and read through `require_int` there, so it is
/// only in this list for Modify, where it is optional.
const PROBE_INT_PARAMS: &[&str] = &[
    "Port",
    "DeviceIndex",
    "Interval",
    "Timeout",
    "FailureThreshold",
    "SuccessThreshold",
    "InitializationGracePeriodSeconds",
];

/// `MaxResults` + `NextToken` as the paginated describes read them. A
/// `NextToken` that is not one this server minted is rejected rather than
/// silently restarting the caller at page one.
fn pagination(req: &AwsRequest) -> Result<(Option<usize>, Option<String>), AwsServiceError> {
    let max_results = req
        .query_params
        .get("MaxResults")
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<usize>().ok());
    let next_token = req
        .query_params
        .get("NextToken")
        .filter(|v| !v.is_empty())
        .cloned();
    if let Some(t) = &next_token {
        crate::service_helpers::decode_page_token(t)?;
    }
    Ok((max_results, next_token))
}

/// Parse the `HealthCheckPath.N` request set: each path has one source and an
/// indexed `Destination.M` set beneath it.
fn health_check_paths(req: &AwsRequest) -> Vec<crate::state::HealthCheckPath> {
    let mut paths = Vec::new();
    for n in 1.. {
        let prefix = format!("HealthCheckPath.{n}");
        let get = |suffix: &str| req.query_params.get(&format!("{prefix}.{suffix}")).cloned();
        let source_subnet_id = get("Source.SubnetId");
        let source_security_group_id = get("Source.SecurityGroupId");
        let mut destinations = Vec::new();
        for m in 1.. {
            let subnet = get(&format!("Destination.{m}.SubnetId"));
            let sg = get(&format!("Destination.{m}.SecurityGroupId"));
            if subnet.is_none() && sg.is_none() {
                break;
            }
            destinations.push((subnet, sg));
        }
        if source_subnet_id.is_none()
            && source_security_group_id.is_none()
            && destinations.is_empty()
        {
            break;
        }
        paths.push(crate::state::HealthCheckPath {
            source_subnet_id,
            source_security_group_id,
            destinations,
        });
    }
    paths
}

fn health_check_paths_xml(paths: &[crate::state::HealthCheckPath]) -> String {
    let items: Vec<String> = paths
        .iter()
        .map(|p| {
            let mut source = String::new();
            if let Some(v) = &p.source_subnet_id {
                source.push_str(&ec2_elem("subnetId", v));
            }
            if let Some(v) = &p.source_security_group_id {
                source.push_str(&ec2_elem("securityGroupId", v));
            }
            let destinations: Vec<String> = p
                .destinations
                .iter()
                .map(|(subnet, sg)| {
                    let mut d = String::new();
                    if let Some(v) = subnet {
                        d.push_str(&ec2_elem("subnetId", v));
                    }
                    if let Some(v) = sg {
                        d.push_str(&ec2_elem("securityGroupId", v));
                    }
                    d
                })
                .collect();
            let mut out = format!("<source>{source}</source>");
            if !destinations.is_empty() {
                out.push_str(&ec2_list("destinationSet", &destinations));
            }
            out
        })
        .collect();
    if items.is_empty() {
        String::new()
    } else {
        ec2_list("healthCheckPathSet", &items)
    }
}

fn str_param(req: &AwsRequest, key: &str) -> Option<String> {
    req.query_params.get(key).filter(|v| !v.is_empty()).cloned()
}

/// A check that has been deleted is tombstoned rather than dropped, so that
/// its deletion time stays describable. Every other operation must treat it as
/// gone.
fn get_check<'a>(
    state: &'a mut Ec2State,
    id: &str,
) -> Result<&'a mut ApplicationStatusCheck, AwsServiceError> {
    state
        .application_status_checks
        .get_mut(id)
        .filter(|c| c.deletion_time.is_none())
        .ok_or_else(|| not_found("InvalidApplicationStatusCheckId.NotFound", id))
}

/// The account's EC2 state, or `None` when it holds none yet. An account with
/// no state cannot hold any of the ids a describe explicitly asked for, so
/// that case is the same not-found the populated path returns; building a
/// throwaway `Ec2State` here would seed a whole default network and AMI
/// catalogue on every read.
fn account_state<'a>(
    accounts: &'a fakecloud_core::multi_account::MultiRegionState<Ec2State>,
    account_id: &str,
    region: &str,
    requested: &[String],
) -> Result<Option<&'a Ec2State>, AwsServiceError> {
    match accounts.regional(account_id, region) {
        Some(state) => Ok(Some(state)),
        None => match requested.first() {
            Some(id) => Err(not_found("InvalidApplicationStatusCheckId.NotFound", id)),
            None => Ok(None),
        },
    }
}

/// Reject an explicitly-requested check id that does not exist. AWS answers a
/// describe naming a missing id with a hard error, not a silently-short list.
fn ensure_checks_exist(
    state: &Ec2State,
    ids: &[String],
    include_tombstoned: bool,
) -> Result<(), AwsServiceError> {
    for id in ids {
        let known = state
            .application_status_checks
            .get(id)
            .is_some_and(|c| include_tombstoned || c.deletion_time.is_none());
        if !known {
            return Err(not_found("InvalidApplicationStatusCheckId.NotFound", id));
        }
    }
    Ok(())
}

/// `DescribeApplicationStatusChecks` filters. The model documents one,
/// `aggregation`; the `tag:`/`tag-key`/`tag-value` family every EC2 describe
/// accepts works too, now that a check's tags live in the shared tag store.
fn check_matches(c: &ApplicationStatusCheck, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "aggregation" => vec![c.aggregation.clone()],
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            "tag-value" => tags.iter().map(|t| t.value.clone()).collect(),
            name => match name.strip_prefix("tag:") {
                Some(key) => tags
                    .iter()
                    .filter(|t| t.key == key)
                    .map(|t| t.value.clone())
                    .collect(),
                // An unknown filter name matches nothing, the same way the
                // rest of the EC2 describes treat one.
                None => return false,
            },
        };
        f.values
            .iter()
            .any(|v| candidates.iter().any(|c| filter_value_matches(v, c)))
    })
}

/// A stable fingerprint of everything a create request determines. It is
/// recorded when the check is minted and never updated, so a Modify or a
/// CreateTags landing in between cannot make a legitimate retry look like a
/// different call.
fn create_fingerprint(
    c: &ApplicationStatusCheck,
    tags: &std::collections::BTreeMap<String, String>,
) -> String {
    format!(
        "{}|{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{}|{:?}",
        c.aggregation,
        c.protocol,
        c.port,
        c.path,
        c.device_index,
        c.ip_version,
        c.ip_scope,
        c.interval,
        c.timeout,
        c.failure_threshold,
        c.success_threshold,
        c.status_code_matcher,
        c.initialization_grace_period_seconds,
        serde_json::to_string(&c.health_check_paths).unwrap_or_default(),
        tags,
    )
}

pub(crate) fn create_application_status_check(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let protocol = require(&req.query_params, "Protocol")?;
    let port = require_int(req, "Port")?;
    require_int_params(req, &PROBE_INT_PARAMS[1..])?;
    validate_probe(req)?;
    validate_timeout_against_interval(int_param(req, "Interval"), int_param(req, "Timeout"))?;
    let client_token = str_param(req, "ClientToken");
    let requested_tags = tag_specifications_for(&req.query_params, CHECK_RESOURCE_TYPE);

    // A DryRun validates the request and mints nothing. It runs ahead of the
    // idempotency replay so that a dry run never answers with a real check.
    if dry_run(req) {
        return Ok(Ec2Service::respond(
            "CreateApplicationStatusCheck",
            &req.request_id,
            "",
        ));
    }

    let now = now_rfc3339();
    let mut check = ApplicationStatusCheck {
        id: gen_id("asc"),
        aggregation: str_param(req, "Aggregation").unwrap_or_else(|| "included".to_string()),
        protocol,
        port,
        path: str_param(req, "Path"),
        device_index: int_param(req, "DeviceIndex"),
        ip_version: str_param(req, "IpVersion"),
        ip_scope: str_param(req, "IpScope"),
        interval: int_param(req, "Interval"),
        timeout: int_param(req, "Timeout"),
        failure_threshold: int_param(req, "FailureThreshold"),
        success_threshold: int_param(req, "SuccessThreshold"),
        status_code_matcher: str_param(req, "StatusCodeMatcher"),
        initialization_grace_period_seconds: int_param(req, "InitializationGracePeriodSeconds"),
        health_check_paths: health_check_paths(req),
        instance_ids: Vec::new(),
        tag_associations: Vec::new(),
        client_token,
        create_fingerprint: None,
        creation_time: now.clone(),
        modify_time: now,
        deletion_time: None,
    };
    let fingerprint = create_fingerprint(&check, &requested_tags);
    check.create_fingerprint = check.client_token.is_some().then(|| fingerprint.clone());

    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);

    // Replaying a create with the same idempotency token must not mint a
    // second check; AWS answers the retry with the original object. A retry
    // that changes the parameters is not a retry, and AWS says so rather than
    // quietly handing back an object that does not match what was asked for.
    if let Some(token) = &check.client_token {
        if let Some(existing) = state.application_status_checks.values().find(|c| {
            c.deletion_time.is_none() && c.client_token.as_deref() == Some(token.as_str())
        }) {
            // A check that carries no recorded fingerprint offers nothing to
            // compare against, so a retry on its token replays rather than
            // failing on evidence that was never recorded.
            if let Some(recorded) = existing.create_fingerprint.as_deref() {
                if recorded != fingerprint {
                    return Err(AwsServiceError::aws_error(
                        http::StatusCode::BAD_REQUEST,
                        "IdempotentParameterMismatch",
                        format!(
                            "The client token '{token}' was already used with different parameters"
                        ),
                    ));
                }
            }
            let body = format!(
                "<applicationStatusCheck>{}</applicationStatusCheck>",
                check_xml(existing, state.tags_for(&existing.id))
            );
            return Ok(Ec2Service::respond(
                "CreateApplicationStatusCheck",
                &req.request_id,
                &body,
            ));
        }
    }

    let id = check.id.clone();
    // Create-time tags go to the shared EC2 tag store, so DescribeTags and
    // CreateTags/DeleteTags see the same tags the check reports.
    apply_tag_specifications(state, &req.query_params, &id, CHECK_RESOURCE_TYPE);
    let body = format!(
        "<applicationStatusCheck>{}</applicationStatusCheck>",
        check_xml(&check, state.tags_for(&id))
    );
    state.application_status_checks.insert(id, check);
    Ok(Ec2Service::respond(
        "CreateApplicationStatusCheck",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_application_status_checks(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_int_params(req, &["MaxResults"])?;
    validate_int_range(&req.query_params, "MaxResults", 5, 100)?;
    let (max_results, next_token) = pagination(req)?;
    let ids = indexed_list(&req.query_params, "ApplicationStatusCheckId");
    let filters = crate::service_helpers::parse_filters(&req.query_params);
    // Deleted checks are tombstoned; only `IncludeAll` surfaces them.
    let include_all = req
        .query_params
        .get("IncludeAll")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let accounts = svc.state.read();
    let state = account_state(&accounts, &req.account_id, &req.region, &ids)?;
    let items: Vec<String> = state
        .map(|s| {
            ensure_checks_exist(s, &ids, include_all)?;
            Ok::<_, AwsServiceError>(
                s.application_status_checks
                    .values()
                    .filter(|c| ids.is_empty() || ids.contains(&c.id))
                    .filter(|c| include_all || c.deletion_time.is_none())
                    .filter(|c| check_matches(c, s.tags_for(&c.id), &filters))
                    .map(|c| check_xml(c, s.tags_for(&c.id)))
                    .collect(),
            )
        })
        .transpose()?
        .unwrap_or_default();
    let (page, token) =
        crate::service_helpers::paginate(&items, next_token.as_deref(), max_results)?;
    Ok(Ec2Service::respond(
        "DescribeApplicationStatusChecks",
        &req.request_id,
        &format!(
            "{}{}",
            ec2_list("applicationStatusCheckSet", &page),
            token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
        ),
    ))
}

pub(crate) fn modify_application_status_check(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ApplicationStatusCheckId")?;
    require_int_params(req, PROBE_INT_PARAMS)?;
    validate_probe(req)?;
    let paths = health_check_paths(req);
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let dry = dry_run(req);
    let check = get_check(state, &id)?;
    // Interval and Timeout are each optional, so the invariant has to hold
    // against the values the check ends up with, not only against a pair that
    // arrived in the same request.
    validate_timeout_against_interval(
        int_param(req, "Interval").or(check.interval),
        int_param(req, "Timeout").or(check.timeout),
    )?;
    // A DryRun validates the request — including that the check exists — and
    // changes nothing.
    if dry {
        return Ok(Ec2Service::respond(
            "ModifyApplicationStatusCheck",
            &req.request_id,
            "",
        ));
    }
    if let Some(v) = str_param(req, "Protocol") {
        check.protocol = v;
    }
    if let Some(v) = int_param(req, "Port") {
        check.port = v;
    }
    if let Some(v) = str_param(req, "Aggregation") {
        check.aggregation = v;
    }
    if let Some(v) = str_param(req, "Path") {
        check.path = Some(v);
    }
    if let Some(v) = str_param(req, "IpVersion") {
        check.ip_version = Some(v);
    }
    if let Some(v) = str_param(req, "IpScope") {
        check.ip_scope = Some(v);
    }
    if let Some(v) = str_param(req, "StatusCodeMatcher") {
        check.status_code_matcher = Some(v);
    }
    for (key, slot) in [
        ("DeviceIndex", &mut check.device_index),
        ("Interval", &mut check.interval),
        ("Timeout", &mut check.timeout),
        ("FailureThreshold", &mut check.failure_threshold),
        ("SuccessThreshold", &mut check.success_threshold),
        (
            "InitializationGracePeriodSeconds",
            &mut check.initialization_grace_period_seconds,
        ),
    ] {
        if let Some(v) = req
            .query_params
            .get(key)
            .and_then(|v| v.parse::<i64>().ok())
        {
            *slot = Some(v);
        }
    }
    // An omitted member and an empty list are the same bytes in an ec2Query
    // request, so an absent `HealthCheckPath.N` set leaves the stored paths
    // alone rather than clearing them. The same is true of every optional
    // scalar above: ec2Query has no way to spell "unset this".
    if !paths.is_empty() {
        check.health_check_paths = paths;
    }
    check.modify_time = now_rfc3339();
    let rendered = check.clone();
    let body = format!(
        "<applicationStatusCheck>{}</applicationStatusCheck>",
        check_xml(&rendered, state.tags_for(&id))
    );
    Ok(Ec2Service::respond(
        "ModifyApplicationStatusCheck",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_application_status_check(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ApplicationStatusCheckId")?;
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let dry = dry_run(req);
    // get_check already treats a tombstoned check as gone, so reaching here
    // means the check is live.
    let check = get_check(state, &id)?;
    // A DryRun validates the request — including that the check exists — and
    // changes nothing.
    if dry {
        return Ok(Ec2Service::respond(
            "DeleteApplicationStatusCheck",
            &req.request_id,
            "",
        ));
    }
    // AWS reports a deletion time on the returned object, so the check is
    // tombstoned rather than dropped; its associations go with it.
    check.deletion_time = Some(now_rfc3339());
    check.instance_ids.clear();
    check.tag_associations.clear();
    let rendered = check.clone();
    // The tombstone stays describable under `IncludeAll`, but its tags go with
    // it the way every other EC2 delete drops a resource's tags — nothing can
    // address a tombstoned id to clean them up later.
    let tags = state.tags.remove(&id).unwrap_or_default();
    let body = format!(
        "<applicationStatusCheck>{}</applicationStatusCheck>",
        check_xml(&rendered, &tags)
    );
    Ok(Ec2Service::respond(
        "DeleteApplicationStatusCheck",
        &req.request_id,
        &body,
    ))
}

/// Shared body for Associate/Disassociate: both take the same inputs and
/// report the same success/failure split.
fn change_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &'static str,
    associate: bool,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ApplicationStatusCheckId")?;
    let instance_ids = indexed_list(&req.query_params, "InstanceId");
    let tags = tag_associations(req);
    if instance_ids.is_empty() && tags.is_empty() {
        return Err(invalid_parameter_value(
            "Either InstanceIds or TargetTagAssociations must be specified",
        ));
    }
    // A check targets instances or tags, never both in one call.
    if !instance_ids.is_empty() && !tags.is_empty() {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "InvalidParameterCombination",
            "InstanceIds and TargetTagAssociations cannot be specified together",
        ));
    }

    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let known_instances: Vec<String> = state.instances.keys().cloned().collect();
    let dry = dry_run(req);
    let check = get_check(state, &id)?;
    // A DryRun validates the request — including that the check exists — and
    // changes nothing.
    if dry {
        return Ok(Ec2Service::respond(action, &req.request_id, ""));
    }

    let mut successful = Vec::new();
    let mut unsuccessful = Vec::new();
    for instance_id in instance_ids {
        // An instance that does not exist is reported per-target rather than
        // failing the whole call, on both associate and disassociate.
        if !known_instances.contains(&instance_id) {
            unsuccessful.push(format!(
                "{}{}{}{}",
                ec2_elem("applicationStatusCheckId", &id),
                ec2_elem("associationType", RESULT_TYPE_INSTANCE),
                ec2_elem("associationValue", &instance_id),
                ec2_elem("reason", "The instance ID does not exist")
            ));
            continue;
        }
        if associate {
            if !check.instance_ids.contains(&instance_id) {
                check.instance_ids.push(instance_id.clone());
            }
        } else {
            check.instance_ids.retain(|i| i != &instance_id);
        }
        successful.push(format!(
            "{}{}{}",
            ec2_elem("applicationStatusCheckId", &id),
            ec2_elem("associationType", RESULT_TYPE_INSTANCE),
            ec2_elem("associationValue", &instance_id)
        ));
    }
    for (k, v) in tags {
        if associate {
            if !check
                .tag_associations
                .iter()
                .any(|(ek, ev)| ek == &k && ev == &v)
            {
                check.tag_associations.push((k.clone(), v.clone()));
            }
        } else {
            check
                .tag_associations
                .retain(|(ek, ev)| !(ek == &k && ev == &v));
        }
        successful.push(format!(
            "{}{}{}",
            ec2_elem("applicationStatusCheckId", &id),
            ec2_elem("associationType", RESULT_TYPE_TAG),
            ec2_elem("associationValue", &format!("{k}={v}"))
        ));
    }
    check.modify_time = now_rfc3339();

    let body = format!(
        "{}{}",
        ec2_list("successfulResultSet", &successful),
        ec2_list("unsuccessfulResultSet", &unsuccessful)
    );
    Ok(Ec2Service::respond(action, &req.request_id, &body))
}

pub(crate) fn associate_application_status_check(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    change_associations(svc, req, "AssociateApplicationStatusCheck", true)
}

pub(crate) fn disassociate_application_status_check(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    change_associations(svc, req, "DisassociateApplicationStatusCheck", false)
}

pub(crate) fn describe_application_status_check_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_int_params(req, &["MaxResults"])?;
    validate_int_range(&req.query_params, "MaxResults", 5, 1_000)?;
    let (max_results, next_token) = pagination(req)?;
    let ids = indexed_list(&req.query_params, "ApplicationStatusCheckId");
    let filters = crate::service_helpers::parse_filters(&req.query_params);
    // The model documents one filter here: `association-type`, whose values
    // are the `AssociationTypeEnum` spellings.
    let type_matches = |association_type: &str| {
        filters.iter().all(|f| match f.name.as_str() {
            "association-type" => f
                .values
                .iter()
                .any(|v| filter_value_matches(v, association_type)),
            _ => false,
        })
    };
    let accounts = svc.state.read();
    let mut items = Vec::new();
    if let Some(state) = account_state(&accounts, &req.account_id, &req.region, &ids)? {
        ensure_checks_exist(state, &ids, true)?;
        for c in state.application_status_checks.values() {
            if !ids.is_empty() && !ids.contains(&c.id) {
                continue;
            }
            if type_matches("instance-id") {
                for instance_id in &c.instance_ids {
                    items.push(format!(
                        "{}{}{}",
                        ec2_elem("applicationStatusCheckId", &c.id),
                        ec2_elem("associationType", "instance-id"),
                        ec2_elem("value", instance_id)
                    ));
                }
            }
            if type_matches("tag") {
                for (k, v) in &c.tag_associations {
                    items.push(format!(
                        "{}{}{}",
                        ec2_elem("applicationStatusCheckId", &c.id),
                        ec2_elem("associationType", "tag"),
                        format_args!("{}{}", ec2_elem("key", k), ec2_elem("value", v))
                    ));
                }
            }
        }
    }
    let (page, token) =
        crate::service_helpers::paginate(&items, next_token.as_deref(), max_results)?;
    Ok(Ec2Service::respond(
        "DescribeApplicationStatusCheckAssociations",
        &req.request_id,
        &format!(
            "{}{}",
            ec2_list("associationSet", &page),
            token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
        ),
    ))
}

pub(crate) fn describe_application_status(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_int_params(req, &["MaxResults"])?;
    validate_int_range(&req.query_params, "MaxResults", 1, 100)?;
    let (max_results, next_token) = pagination(req)?;
    let requested = indexed_list(&req.query_params, "InstanceId");
    let filters = crate::service_helpers::parse_filters(&req.query_params);
    let accounts = svc.state.read();
    let mut items = Vec::new();
    // An explicitly-requested instance that does not exist is a hard error on
    // AWS, not a silently-short list — including when the account holds no
    // state at all.
    let Some(state) = accounts.regional(&req.account_id, &req.region) else {
        if let Some(id) = requested.first() {
            return Err(instance_not_found(id));
        }
        return Ok(empty_application_status(&req.request_id));
    };
    for id in &requested {
        if !state.instances.contains_key(id) {
            return Err(instance_not_found(id));
        }
    }
    let evaluated_at = Utc::now();
    let evaluated_at_str = evaluated_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    for (instance_id, instance) in &state.instances {
        if !requested.is_empty() && !requested.contains(instance_id) {
            continue;
        }
        // A check reaches this instance either by id or by one of its
        // tags, so tagging an instance later brings it under a
        // tag-associated check without re-associating.
        let instance_tags = state.tags_for(instance_id);
        let checks: Vec<&ApplicationStatusCheck> = state
            .application_status_checks
            .values()
            .filter(|c| c.deletion_time.is_none())
            .filter(|c| {
                c.instance_ids.contains(instance_id)
                    || c.tag_associations
                        .iter()
                        .any(|(k, v)| instance_tags.iter().any(|t| &t.key == k && &t.value == v))
            })
            .collect();

        let suppression = state
            .application_status_suppressions
            .get(instance_id)
            .filter(|s| !suppression_expired(s, evaluated_at));

        // With no check associated there is nothing to report on; with
        // one, fakecloud runs no probe, so the status is the honest
        // "insufficient-data" rather than a fabricated "ok".
        // `Aggregation=excluded` keeps a check off the instance's
        // aggregate status while still reporting it in the detail set, so
        // an instance whose only check is excluded has nothing to
        // aggregate over.
        let included = checks
            .iter()
            .filter(|c| c.aggregation != "excluded")
            .count();
        let status = if suppression.is_some() {
            "suppressed"
        } else if included == 0 {
            "not-applicable"
        } else {
            "insufficient-data"
        };
        if !status_matches(instance, state, status, &filters) {
            continue;
        }
        // The status has held since whatever last changed it: the suppression
        // window opening, or the newest association/check edit. With neither,
        // it has held since the instance launched.
        let status_since = match suppression {
            Some(s) => s.suppress_at.clone(),
            None => checks
                .iter()
                .map(|c| c.modify_time.clone())
                .max()
                .unwrap_or_else(|| instance.launch_time.clone()),
        };
        let details: Vec<String> = checks
            .iter()
            .map(|c| {
                format!(
                    "{}{}{}{}{}{}",
                    ec2_elem("applicationStatusCheckId", &c.id),
                    ec2_elem("checkUpdateTime", &c.modify_time),
                    ec2_elem("aggregation", &c.aggregation),
                    ec2_elem("status", "insufficient-data"),
                    ec2_elem("statusTimeStamp", &evaluated_at_str),
                    ec2_elem("statusSince", &c.modify_time),
                )
            })
            .collect();
        let mut app_status = ec2_elem("status", status);
        app_status.push_str(&ec2_elem("statusTimeStamp", &evaluated_at_str));
        app_status.push_str(&ec2_elem("statusSince", &status_since));
        if let Some(resume_at) = suppression.and_then(|s| s.resume_at.as_deref()) {
            app_status.push_str(&ec2_elem("resumeAt", resume_at));
        }
        if !details.is_empty() {
            app_status.push_str(&ec2_list("detailSet", &details));
        }
        items.push(format!(
            "{}{}{}{}{}",
            ec2_elem("instanceId", instance_id),
            ec2_elem("availabilityZone", &instance.az),
            ec2_elem("availabilityZoneId", &availability_zone_id(state, instance)),
            format_args!("<applicationStatus>{app_status}</applicationStatus>"),
            tag_set_xml(instance_tags),
        ));
    }
    let (page, token) =
        crate::service_helpers::paginate(&items, next_token.as_deref(), max_results)?;
    Ok(Ec2Service::respond(
        "DescribeApplicationStatus",
        &req.request_id,
        &format!(
            "<applicationStatusesResponseType>{}</applicationStatusesResponseType>{}",
            ec2_list("instanceSet", &page),
            token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
        ),
    ))
}

/// The `DescribeApplicationStatus` body for an account that holds no state.
fn empty_application_status(request_id: &str) -> AwsResponse {
    Ec2Service::respond(
        "DescribeApplicationStatus",
        request_id,
        &format!(
            "<applicationStatusesResponseType>{}</applicationStatusesResponseType>",
            ec2_list("instanceSet", &[])
        ),
    )
}

/// A suppression window that has already elapsed no longer suppresses
/// anything; the entry is only cleared when the caller disables it.
fn suppression_expired(s: &ApplicationStatusSuppression, now: chrono::DateTime<Utc>) -> bool {
    s.resume_at
        .as_deref()
        .and_then(|r| chrono::DateTime::parse_from_rfc3339(r).ok())
        .is_some_and(|r| r <= now)
}

/// The AZ id of the instance's subnet. An instance carries an AZ name, and the
/// subnet it launched into is what pins that name to a zone id.
fn availability_zone_id(state: &Ec2State, instance: &crate::state::Instance) -> String {
    instance
        .subnet_id
        .as_ref()
        .and_then(|id| state.subnets.get(id))
        .map(|s| s.availability_zone_id.clone())
        .or_else(|| {
            state
                .subnets
                .values()
                .find(|s| s.availability_zone == instance.az)
                .map(|s| s.availability_zone_id.clone())
        })
        .unwrap_or_default()
}

/// `DescribeApplicationStatus` filters. The model documents two:
/// `availability-zone-id` and `status`.
fn status_matches(
    instance: &crate::state::Instance,
    state: &Ec2State,
    status: &str,
    filters: &[Filter],
) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "availability-zone-id" => vec![availability_zone_id(state, instance)],
            "status" => vec![status.to_string()],
            // An unknown filter name matches nothing, the same way the rest of
            // the EC2 describes treat one.
            _ => return false,
        };
        f.values
            .iter()
            .any(|v| candidates.iter().any(|c| filter_value_matches(v, c)))
    })
}

fn change_suppression(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &'static str,
    enable: bool,
) -> Result<AwsResponse, AwsServiceError> {
    let instance_ids = indexed_list(&req.query_params, "InstanceId");
    if instance_ids.is_empty() {
        return Err(invalid_parameter_value("InstanceIds must be specified"));
    }
    let now = Utc::now();
    let suppress_at = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    // `DurationSeconds` is modeled only on Enable — Disable ends the window
    // outright, so there is nothing to time-box and nothing to validate.
    let resume_at = if enable {
        require_int_params(req, &["DurationSeconds"])?;
        match int_param(req, "DurationSeconds") {
            Some(d) if d <= 0 => {
                return Err(invalid_parameter_value(
                    "DurationSeconds must be a positive integer",
                ))
            }
            // A duration far enough out to overflow the timestamp is out of
            // range, not a reason to tear down the connection.
            Some(d) => Some(
                chrono::Duration::try_seconds(d)
                    .and_then(|d| now.checked_add_signed(d))
                    .ok_or_else(|| invalid_parameter_value("DurationSeconds is out of range"))?
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            ),
            None => None,
        }
    } else {
        None
    };
    if dry_run(req) {
        return Ok(Ec2Service::respond(action, &req.request_id, ""));
    }

    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let known: Vec<String> = state.instances.keys().cloned().collect();

    let mut successful = Vec::new();
    let mut unsuccessful = Vec::new();
    for instance_id in instance_ids {
        if !known.contains(&instance_id) {
            unsuccessful.push(format!(
                "{}{}",
                ec2_elem("instanceId", &instance_id),
                ec2_elem("reason", "The instance ID does not exist")
            ));
            continue;
        }
        // Disable reports the window it just ended — the suppressAt that was
        // actually in force, and `now` as the moment reporting resumes, since
        // that is what ending the window does.
        let (entry_suppress_at, entry_resume_at) = if enable {
            state.application_status_suppressions.insert(
                instance_id.clone(),
                ApplicationStatusSuppression {
                    instance_id: instance_id.clone(),
                    suppress_at: suppress_at.clone(),
                    resume_at: resume_at.clone(),
                },
            );
            (suppress_at.clone(), resume_at.clone())
        } else {
            match state
                .application_status_suppressions
                .remove(&instance_id)
                .filter(|s| !suppression_expired(s, now))
            {
                Some(removed) => (removed.suppress_at, Some(suppress_at.clone())),
                // There was no window to end. That is not a success with a
                // zero-length window invented for it — the modeled
                // unsuccessful result carries exactly this case.
                None => {
                    unsuccessful.push(format!(
                        "{}{}",
                        ec2_elem("instanceId", &instance_id),
                        ec2_elem(
                            "reason",
                            "Application status check suppression is not enabled for the instance"
                        )
                    ));
                    continue;
                }
            }
        };
        let mut entry = ec2_elem("instanceId", &instance_id);
        entry.push_str(&ec2_elem("suppressAt", &entry_suppress_at));
        if let Some(r) = &entry_resume_at {
            entry.push_str(&ec2_elem("resumeAt", r));
        }
        successful.push(entry);
    }

    let body = format!(
        "{}{}",
        ec2_list("successfulResultSet", &successful),
        ec2_list("unsuccessfulResultSet", &unsuccessful)
    );
    Ok(Ec2Service::respond(action, &req.request_id, &body))
}

pub(crate) fn enable_application_status_check_suppression(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    change_suppression(svc, req, "EnableApplicationStatusCheckSuppression", true)
}

pub(crate) fn disable_application_status_check_suppression(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    change_suppression(svc, req, "DisableApplicationStatusCheckSuppression", false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    fn make_check(svc: &Ec2Service, extra: &[(&str, &str)]) -> String {
        let mut params: Vec<(&str, &str)> = vec![("Protocol", "http"), ("Port", "8080")];
        params.extend_from_slice(extra);
        let b = body(
            create_application_status_check(svc, &req("CreateApplicationStatusCheck", &params))
                .unwrap(),
        );
        b.split("<applicationStatusCheckId>")
            .nth(1)
            .unwrap()
            .split("</applicationStatusCheckId>")
            .next()
            .unwrap()
            .to_string()
    }

    /// Register an instance directly so association targets exist.
    fn seed_instance(svc: &Ec2Service, id: &str, tags: &[(&str, &str)]) {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut("000000000000", "us-east-1");
        state.instances.insert(
            id.to_string(),
            crate::state::Instance {
                instance_id: id.into(),
                image_id: "ami-1".into(),
                instance_type: "t3.micro".into(),
                state_code: 16,
                state_name: "running".into(),
                private_ip: "10.0.0.5".into(),
                public_ip: None,
                subnet_id: Some("subnet-1".into()),
                vpc_id: Some("vpc-1".into()),
                key_name: None,
                security_group_ids: vec![],
                reservation_id: "r-1".into(),
                ami_launch_index: 0,
                monitoring: false,
                az: "us-east-1a".into(),
                launch_time: "2024-01-01T00:00:00.000Z".into(),
                container_id: None,
                disable_api_termination: false,
                disable_api_stop: false,
                source_dest_check: true,
                ebs_optimized: false,
                instance_initiated_shutdown_behavior: "stop".into(),
                user_data: None,
                metadata_options: Default::default(),
                cpu_options: None,
                bandwidth_weighting: None,
                maintenance_options: Default::default(),
                placement_tenancy: None,
                placement_affinity: None,
                placement_group_name: None,
                private_dns_hostname_type: None,
                enable_resource_name_dns_a_record: false,
                enable_resource_name_dns_aaaa_record: false,
                instance_lifecycle: None,
            },
        );
        if !tags.is_empty() {
            state.tags.insert(
                id.to_string(),
                tags.iter()
                    .map(|(k, v)| crate::state::Tag {
                        key: k.to_string(),
                        value: v.to_string(),
                    })
                    .collect(),
            );
        }
    }

    #[test]
    fn check_create_describe_modify_delete() {
        let svc = Ec2Service::new();
        let id = make_check(
            &svc,
            &[("Path", "/healthz"), ("Interval", "60"), ("Timeout", "5")],
        );
        assert!(id.starts_with("asc-"), "{id}");

        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(d.contains("<path>/healthz</path>"), "{d}");
        assert!(d.contains("<port>8080</port>"), "{d}");
        // Aggregation defaults to `included`.
        assert!(d.contains("<aggregation>included</aggregation>"), "{d}");

        modify_application_status_check(
            &svc,
            &req(
                "ModifyApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &id),
                    ("Path", "/ready"),
                    ("Aggregation", "excluded"),
                ],
            ),
        )
        .unwrap();
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(d.contains("<path>/ready</path>"), "{d}");
        assert!(d.contains("<aggregation>excluded</aggregation>"), "{d}");
        // An unmodified field is left alone.
        assert!(d.contains("<port>8080</port>"), "{d}");

        // Delete tombstones: gone from the default describe, visible under
        // IncludeAll with a deletion time.
        delete_application_status_check(
            &svc,
            &req(
                "DeleteApplicationStatusCheck",
                &[("ApplicationStatusCheckId", &id)],
            ),
        )
        .unwrap();
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(!d.contains(&id), "{d}");
        let d = body(
            describe_application_status_checks(
                &svc,
                &req("DescribeApplicationStatusChecks", &[("IncludeAll", "true")]),
            )
            .unwrap(),
        );
        assert!(d.contains(&id), "{d}");
        assert!(d.contains("<deletionTime>"), "{d}");

        // A second delete is a not-found.
        let err = err_of(delete_application_status_check(
            &svc,
            &req(
                "DeleteApplicationStatusCheck",
                &[("ApplicationStatusCheckId", &id)],
            ),
        ));
        assert_eq!(err.code(), "InvalidApplicationStatusCheckId.NotFound");
    }

    #[test]
    fn associations_report_unknown_instances_per_target() {
        let svc = Ec2Service::new();
        let id = make_check(&svc, &[]);
        seed_instance(&svc, "i-1111111111111111a", &[]);

        let b = body(
            associate_application_status_check(
                &svc,
                &req(
                    "AssociateApplicationStatusCheck",
                    &[
                        ("ApplicationStatusCheckId", &id),
                        ("InstanceId.1", "i-1111111111111111a"),
                        ("InstanceId.2", "i-doesnotexist00000"),
                    ],
                ),
            )
            .unwrap(),
        );
        // The known instance succeeds; the unknown one is reported rather
        // than failing the whole call.
        assert!(b.contains("i-1111111111111111a"), "{b}");
        assert!(b.contains("The instance ID does not exist"), "{b}");

        let assoc = body(
            describe_application_status_check_associations(
                &svc,
                &req("DescribeApplicationStatusCheckAssociations", &[]),
            )
            .unwrap(),
        );
        assert!(
            assoc.contains("<associationType>instance-id</associationType>"),
            "{assoc}"
        );
        assert!(assoc.contains("i-1111111111111111a"), "{assoc}");

        disassociate_application_status_check(
            &svc,
            &req(
                "DisassociateApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &id),
                    ("InstanceId.1", "i-1111111111111111a"),
                ],
            ),
        )
        .unwrap();
        let assoc = body(
            describe_application_status_check_associations(
                &svc,
                &req("DescribeApplicationStatusCheckAssociations", &[]),
            )
            .unwrap(),
        );
        assert!(!assoc.contains("i-1111111111111111a"), "{assoc}");
    }

    #[test]
    fn tag_association_reaches_instances_tagged_later() {
        let svc = Ec2Service::new();
        let id = make_check(&svc, &[]);
        associate_application_status_check(
            &svc,
            &req(
                "AssociateApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &id),
                    ("TargetTagAssociations.1.Key", "app"),
                    ("TargetTagAssociations.1.Value", "web"),
                ],
            ),
        )
        .unwrap();

        // An untagged instance is out of scope.
        seed_instance(&svc, "i-2222222222222222b", &[]);
        let s = body(
            describe_application_status(&svc, &req("DescribeApplicationStatus", &[])).unwrap(),
        );
        assert!(s.contains("<status>not-applicable</status>"), "{s}");

        // Tagging it afterwards brings it under the check without
        // re-associating.
        seed_instance(&svc, "i-2222222222222222b", &[("app", "web")]);
        let s = body(
            describe_application_status(&svc, &req("DescribeApplicationStatus", &[])).unwrap(),
        );
        assert!(s.contains("<status>insufficient-data</status>"), "{s}");
        assert!(s.contains(&id), "{s}");
    }

    #[test]
    fn suppression_overrides_the_reported_status() {
        let svc = Ec2Service::new();
        let id = make_check(&svc, &[]);
        seed_instance(&svc, "i-3333333333333333c", &[]);
        associate_application_status_check(
            &svc,
            &req(
                "AssociateApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &id),
                    ("InstanceId.1", "i-3333333333333333c"),
                ],
            ),
        )
        .unwrap();

        enable_application_status_check_suppression(
            &svc,
            &req(
                "EnableApplicationStatusCheckSuppression",
                &[
                    ("InstanceId.1", "i-3333333333333333c"),
                    ("DurationSeconds", "600"),
                ],
            ),
        )
        .unwrap();
        let s = body(
            describe_application_status(&svc, &req("DescribeApplicationStatus", &[])).unwrap(),
        );
        assert!(s.contains("<status>suppressed</status>"), "{s}");

        disable_application_status_check_suppression(
            &svc,
            &req(
                "DisableApplicationStatusCheckSuppression",
                &[("InstanceId.1", "i-3333333333333333c")],
            ),
        )
        .unwrap();
        let s = body(
            describe_application_status(&svc, &req("DescribeApplicationStatus", &[])).unwrap(),
        );
        assert!(s.contains("<status>insufficient-data</status>"), "{s}");

        // An unknown instance is reported per-target.
        let b = body(
            enable_application_status_check_suppression(
                &svc,
                &req(
                    "EnableApplicationStatusCheckSuppression",
                    &[("InstanceId.1", "i-nope0000000000000")],
                ),
            )
            .unwrap(),
        );
        assert!(b.contains("The instance ID does not exist"), "{b}");
    }

    #[test]
    fn probe_settings_are_validated() {
        let svc = Ec2Service::new();

        // Protocol and Port are required.
        assert_eq!(
            err_of(create_application_status_check(
                &svc,
                &req("CreateApplicationStatusCheck", &[("Port", "80")])
            ))
            .code(),
            "MissingParameter"
        );

        for params in [
            vec![("Protocol", "ftp"), ("Port", "80")],
            vec![("Protocol", "http"), ("Port", "0")],
            vec![
                ("Protocol", "http"),
                ("Port", "80"),
                ("Aggregation", "maybe"),
            ],
            vec![("Protocol", "http"), ("Port", "80"), ("IpVersion", "ipv7")],
            vec![("Protocol", "http"), ("Port", "80"), ("Interval", "1")],
            // "Valid values: 1 to 30." for Timeout.
            vec![("Protocol", "http"), ("Port", "80"), ("Timeout", "31")],
            vec![("Protocol", "http"), ("Port", "80"), ("Timeout", "0")],
            // Thresholds are documented as "greater than 0", with no ceiling.
            vec![
                ("Protocol", "http"),
                ("Port", "80"),
                ("FailureThreshold", "0"),
            ],
            vec![
                ("Protocol", "http"),
                ("Port", "80"),
                ("SuccessThreshold", "0"),
            ],
            // StatusCodeMatcher takes codes and low-high ranges only.
            vec![
                ("Protocol", "http"),
                ("Port", "80"),
                ("StatusCodeMatcher", "2xx"),
            ],
            vec![
                ("Protocol", "http"),
                ("Port", "80"),
                ("StatusCodeMatcher", "399-300"),
            ],
        ] {
            let err = err_of(create_application_status_check(
                &svc,
                &req("CreateApplicationStatusCheck", &params),
            ));
            assert_eq!(err.code(), "InvalidParameterValue", "{params:?}");
        }

        // Associating needs at least one target.
        let id = make_check(&svc, &[]);
        let err = err_of(associate_application_status_check(
            &svc,
            &req(
                "AssociateApplicationStatusCheck",
                &[("ApplicationStatusCheckId", &id)],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn dry_run_changes_nothing() {
        let svc = Ec2Service::new();
        create_application_status_check(
            &svc,
            &req(
                "CreateApplicationStatusCheck",
                &[("Protocol", "http"), ("Port", "80"), ("DryRun", "true")],
            ),
        )
        .unwrap();
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(!d.contains("asc-"), "{d}");
    }

    #[test]
    fn describe_uses_the_modeled_wrapper_names() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1", &[]);
        let id = make_check(&svc, &[]);
        associate_application_status_check(
            &svc,
            &req(
                "AssociateApplicationStatusCheck",
                &[("ApplicationStatusCheckId", &id), ("InstanceId.1", "i-1")],
            ),
        )
        .unwrap();

        let d = body(
            describe_application_status(&svc, &req("DescribeApplicationStatus", &[])).unwrap(),
        );
        assert!(
            d.contains("<applicationStatusesResponseType>"),
            "the modeled wrapper is applicationStatusesResponseType: {d}"
        );

        // Tag associations use the modeled set name on the check response.
        disassociate_application_status_check(
            &svc,
            &req(
                "DisassociateApplicationStatusCheck",
                &[("ApplicationStatusCheckId", &id), ("InstanceId.1", "i-1")],
            ),
        )
        .unwrap();
        associate_application_status_check(
            &svc,
            &req(
                "AssociateApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &id),
                    ("TargetTagAssociation.1.Key", "env"),
                    ("TargetTagAssociation.1.Value", "prod"),
                ],
            ),
        )
        .unwrap();
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(
            d.contains("<targetTagAssociationSet>"),
            "the modeled set name is targetTagAssociationSet: {d}"
        );
    }

    #[test]
    fn association_results_use_the_documented_type_spelling() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1", &[]);
        let id = make_check(&svc, &[]);

        // Result objects are documented with EC2TAG and INSTANCE_ID, which is
        // a different spelling from the describe response's enum.
        let d = body(
            associate_application_status_check(
                &svc,
                &req(
                    "AssociateApplicationStatusCheck",
                    &[("ApplicationStatusCheckId", &id), ("InstanceId.1", "i-1")],
                ),
            )
            .unwrap(),
        );
        assert!(
            d.contains("<associationType>INSTANCE_ID</associationType>"),
            "{d}"
        );

        let d = body(
            associate_application_status_check(
                &svc,
                &req(
                    "AssociateApplicationStatusCheck",
                    &[
                        ("ApplicationStatusCheckId", &id),
                        ("TargetTagAssociation.1.Key", "env"),
                        ("TargetTagAssociation.1.Value", "prod"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(
            d.contains("<associationType>EC2TAG</associationType>"),
            "{d}"
        );

        // The describe-associations response keeps the enum spelling.
        let d = body(
            describe_application_status_check_associations(
                &svc,
                &req("DescribeApplicationStatusCheckAssociations", &[]),
            )
            .unwrap(),
        );
        assert!(d.contains("instance-id") || d.contains("tag"), "{d}");
    }

    #[test]
    fn association_requires_exactly_one_target_type() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1", &[]);
        let id = make_check(&svc, &[]);

        let err = err_of(associate_application_status_check(
            &svc,
            &req(
                "AssociateApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &id),
                    ("InstanceId.1", "i-1"),
                    ("TargetTagAssociation.1.Key", "env"),
                    ("TargetTagAssociation.1.Value", "prod"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterCombination");
    }

    #[test]
    fn disassociating_an_unknown_instance_is_reported_unsuccessful() {
        let svc = Ec2Service::new();
        let id = make_check(&svc, &[]);
        let d = body(
            disassociate_application_status_check(
                &svc,
                &req(
                    "DisassociateApplicationStatusCheck",
                    &[
                        ("ApplicationStatusCheckId", &id),
                        ("InstanceId.1", "i-ghost"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(
            d.contains("<unsuccessfulResultSet>") && d.contains("i-ghost"),
            "a nonexistent instance must not be reported successful: {d}"
        );
        assert!(!d.contains("<successfulResultSet><item>"), "{d}");
    }

    #[test]
    fn health_check_paths_round_trip() {
        let svc = Ec2Service::new();
        let id = make_check(
            &svc,
            &[
                ("HealthCheckPath.1.Source.SubnetId", "subnet-a"),
                ("HealthCheckPath.1.Destination.1.SubnetId", "subnet-b"),
                ("HealthCheckPath.1.Destination.1.SecurityGroupId", "sg-1"),
            ],
        );
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(d.contains("<healthCheckPathSet>"), "{d}");
        assert!(
            d.contains("subnet-a") && d.contains("subnet-b") && d.contains("sg-1"),
            "{d}"
        );

        // Modify replaces the set.
        modify_application_status_check(
            &svc,
            &req(
                "ModifyApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &id),
                    ("HealthCheckPath.1.Source.SubnetId", "subnet-c"),
                ],
            ),
        )
        .unwrap();
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(d.contains("subnet-c") && !d.contains("subnet-a"), "{d}");
    }

    #[test]
    fn an_excluded_check_does_not_drive_the_aggregate_status() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1", &[]);
        let id = make_check(&svc, &[("Aggregation", "excluded")]);
        associate_application_status_check(
            &svc,
            &req(
                "AssociateApplicationStatusCheck",
                &[("ApplicationStatusCheckId", &id), ("InstanceId.1", "i-1")],
            ),
        )
        .unwrap();

        let d = body(
            describe_application_status(&svc, &req("DescribeApplicationStatus", &[])).unwrap(),
        );
        // The check still appears in the detail set, but contributes nothing.
        assert!(d.contains("<detailSet>"), "{d}");
        assert!(
            d.contains("<status>not-applicable</status>"),
            "an excluded-only instance has nothing to aggregate: {d}"
        );
    }

    #[test]
    fn a_deleted_check_is_gone_for_every_other_operation() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1", &[]);
        let id = make_check(&svc, &[]);
        delete_application_status_check(
            &svc,
            &req(
                "DeleteApplicationStatusCheck",
                &[("ApplicationStatusCheckId", &id)],
            ),
        )
        .unwrap();

        for err in [
            err_of(modify_application_status_check(
                &svc,
                &req(
                    "ModifyApplicationStatusCheck",
                    &[("ApplicationStatusCheckId", &id), ("Port", "9090")],
                ),
            )),
            err_of(associate_application_status_check(
                &svc,
                &req(
                    "AssociateApplicationStatusCheck",
                    &[("ApplicationStatusCheckId", &id), ("InstanceId.1", "i-1")],
                ),
            )),
            err_of(delete_application_status_check(
                &svc,
                &req(
                    "DeleteApplicationStatusCheck",
                    &[("ApplicationStatusCheckId", &id)],
                ),
            )),
        ] {
            assert_eq!(err.code(), "InvalidApplicationStatusCheckId.NotFound");
        }
    }

    #[test]
    fn a_malformed_integer_is_rejected_rather_than_defaulted() {
        let svc = Ec2Service::new();
        let err = err_of(create_application_status_check(
            &svc,
            &req(
                "CreateApplicationStatusCheck",
                &[("Protocol", "http"), ("Port", "not-a-number")],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn describe_application_status_paginates() {
        let svc = Ec2Service::new();
        for i in 1..=3 {
            seed_instance(&svc, &format!("i-{i}"), &[]);
        }
        let id = make_check(&svc, &[]);
        for i in 1..=3 {
            associate_application_status_check(
                &svc,
                &req(
                    "AssociateApplicationStatusCheck",
                    &[
                        ("ApplicationStatusCheckId", &id),
                        ("InstanceId.1", &format!("i-{i}")),
                    ],
                ),
            )
            .unwrap();
        }

        // Collect the ids from each page: counting alone would pass even if a
        // page repeated one instance and dropped another.
        let ids_of = |xml: &str| -> Vec<String> {
            xml.split("<instanceId>")
                .skip(1)
                .filter_map(|s| s.split("</instanceId>").next())
                .map(str::to_string)
                .collect()
        };

        let first = body(
            describe_application_status(
                &svc,
                &req("DescribeApplicationStatus", &[("MaxResults", "2")]),
            )
            .unwrap(),
        );
        let first_ids = ids_of(&first);
        assert_eq!(first_ids.len(), 2, "{first}");
        let token = first
            .split("<nextToken>")
            .nth(1)
            .and_then(|s| s.split("</nextToken>").next())
            .expect("a partial page must carry a nextToken")
            .to_string();

        let second = body(
            describe_application_status(
                &svc,
                &req(
                    "DescribeApplicationStatus",
                    &[("MaxResults", "2"), ("NextToken", &token)],
                ),
            )
            .unwrap(),
        );
        let second_ids = ids_of(&second);
        assert_eq!(second_ids.len(), 1, "{second}");
        assert!(
            !second.contains("<nextToken>"),
            "the last page ends: {second}"
        );

        // Every instance appears exactly once across the two pages.
        let mut seen = first_ids;
        seen.extend(second_ids);
        seen.sort();
        assert_eq!(
            seen,
            vec!["i-1".to_string(), "i-2".to_string(), "i-3".to_string()],
            "pages must partition the instances, with no repeat or omission"
        );
    }

    #[test]
    fn malformed_or_negative_integers_are_rejected() {
        let svc = Ec2Service::new();
        // A non-numeric MaxResults must not slip past the range check.
        let err = err_of(describe_application_status(
            &svc,
            &req("DescribeApplicationStatus", &[("MaxResults", "many")]),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");

        // A device index names an interface slot, so it cannot be negative.
        let err = err_of(create_application_status_check(
            &svc,
            &req(
                "CreateApplicationStatusCheck",
                &[("Protocol", "http"), ("Port", "80"), ("DeviceIndex", "-1")],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }
    #[test]
    fn suppression_duration_out_of_range_is_rejected_not_a_panic() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-4444444444444444d", &[]);
        // Both of these overflow a timestamp: the first blows past
        // `TimeDelta`'s own range, the second fits a delta but not a date.
        for duration in ["9223372036854775807", "9000000000000"] {
            let err = err_of(enable_application_status_check_suppression(
                &svc,
                &req(
                    "EnableApplicationStatusCheckSuppression",
                    &[
                        ("InstanceId.1", "i-4444444444444444d"),
                        ("DurationSeconds", duration),
                    ],
                ),
            ));
            assert_eq!(err.code(), "InvalidParameterValue", "{duration}");
        }
        // A malformed duration is rejected rather than silently ignored.
        let err = err_of(enable_application_status_check_suppression(
            &svc,
            &req(
                "EnableApplicationStatusCheckSuppression",
                &[
                    ("InstanceId.1", "i-4444444444444444d"),
                    ("DurationSeconds", "soon"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn disable_reports_the_window_it_ended() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-5555555555555555e", &[]);
        let enabled = body(
            enable_application_status_check_suppression(
                &svc,
                &req(
                    "EnableApplicationStatusCheckSuppression",
                    &[
                        ("InstanceId.1", "i-5555555555555555e"),
                        ("DurationSeconds", "600"),
                    ],
                ),
            )
            .unwrap(),
        );
        let field = |xml: &str, tag: &str| {
            xml.split(&format!("<{tag}>"))
                .nth(1)
                .unwrap_or_default()
                .split(&format!("</{tag}>"))
                .next()
                .unwrap_or_default()
                .to_string()
        };
        let suppress_at = field(&enabled, "suppressAt");
        let resume_at = field(&enabled, "resumeAt");
        assert!(
            !suppress_at.is_empty() && !resume_at.is_empty(),
            "{enabled}"
        );

        let disabled = body(
            disable_application_status_check_suppression(
                &svc,
                &req(
                    "DisableApplicationStatusCheckSuppression",
                    &[("InstanceId.1", "i-5555555555555555e")],
                ),
            )
            .unwrap(),
        );
        // The window that was in force, not one minted at the moment of
        // deletion. Reporting resumes now, so `resumeAt` is not the future
        // time the window would have ended on its own.
        assert_eq!(field(&disabled, "suppressAt"), suppress_at, "{disabled}");
        assert_ne!(field(&disabled, "resumeAt"), resume_at, "{disabled}");
        assert!(!field(&disabled, "resumeAt").is_empty(), "{disabled}");

        // Disabling again has no window to end, so it is reported as
        // unsuccessful rather than as a zero-length window invented for it.
        let again = body(
            disable_application_status_check_suppression(
                &svc,
                &req(
                    "DisableApplicationStatusCheckSuppression",
                    &[("InstanceId.1", "i-5555555555555555e")],
                ),
            )
            .unwrap(),
        );
        assert!(again.contains("<successfulResultSet/>"), "{again}");
        assert!(
            again.contains("suppression is not enabled for the instance"),
            "{again}"
        );

        // `DurationSeconds` is not modeled on Disable, so a value there is not
        // validated against the Enable rules.
        disable_application_status_check_suppression(
            &svc,
            &req(
                "DisableApplicationStatusCheckSuppression",
                &[
                    ("InstanceId.1", "i-5555555555555555e"),
                    ("DurationSeconds", "not-a-number"),
                ],
            ),
        )
        .unwrap();
    }

    #[test]
    fn an_elapsed_suppression_no_longer_suppresses() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-9999999999999999c", &[]);
        {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut("000000000000", "us-east-1");
            state.application_status_suppressions.insert(
                "i-9999999999999999c".to_string(),
                ApplicationStatusSuppression {
                    instance_id: "i-9999999999999999c".to_string(),
                    suppress_at: "2020-01-01T00:00:00.000Z".to_string(),
                    resume_at: Some("2020-01-01T00:10:00.000Z".to_string()),
                },
            );
        }
        let s = body(
            describe_application_status(&svc, &req("DescribeApplicationStatus", &[])).unwrap(),
        );
        assert!(s.contains("<status>not-applicable</status>"), "{s}");

        // And ending a window that already elapsed is not a success.
        let d = body(
            disable_application_status_check_suppression(
                &svc,
                &req(
                    "DisableApplicationStatusCheckSuppression",
                    &[("InstanceId.1", "i-9999999999999999c")],
                ),
            )
            .unwrap(),
        );
        assert!(d.contains("<successfulResultSet/>"), "{d}");
    }

    #[test]
    fn check_tags_carry_the_modeled_resource_type_and_go_with_the_check() {
        let svc = Ec2Service::new();
        let id = make_check(
            &svc,
            &[
                (
                    "TagSpecification.1.ResourceType",
                    "application-status-check",
                ),
                ("TagSpecification.1.Tag.1.Key", "Name"),
                ("TagSpecification.1.Tag.1.Value", "web-health"),
            ],
        );
        let t = body(
            crate::service::tags::describe_tags(
                &svc,
                &req(
                    "DescribeTags",
                    &[
                        ("Filter.1.Name", "resource-type"),
                        ("Filter.1.Value.1", "application-status-check"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(t.contains(&id), "{t}");
        assert!(
            t.contains("<resourceType>application-status-check</resourceType>"),
            "{t}"
        );

        // Deleting the check takes its tags with it — nothing can address a
        // tombstoned id to clean them up afterwards.
        delete_application_status_check(
            &svc,
            &req(
                "DeleteApplicationStatusCheck",
                &[("ApplicationStatusCheckId", &id)],
            ),
        )
        .unwrap();
        let t = body(crate::service::tags::describe_tags(&svc, &req("DescribeTags", &[])).unwrap());
        assert!(!t.contains("web-health"), "{t}");
    }

    #[test]
    fn checks_filter_on_their_tags() {
        let svc = Ec2Service::new();
        let tagged = make_check(
            &svc,
            &[
                (
                    "TagSpecification.1.ResourceType",
                    "application-status-check",
                ),
                ("TagSpecification.1.Tag.1.Key", "env"),
                ("TagSpecification.1.Tag.1.Value", "prod"),
            ],
        );
        let untagged = make_check(&svc, &[]);
        let d = body(
            describe_application_status_checks(
                &svc,
                &req(
                    "DescribeApplicationStatusChecks",
                    &[("Filter.1.Name", "tag:env"), ("Filter.1.Value.1", "prod")],
                ),
            )
            .unwrap(),
        );
        assert!(d.contains(&tagged), "{d}");
        assert!(!d.contains(&untagged), "{d}");
    }

    #[test]
    fn a_reused_client_token_with_new_parameters_is_a_mismatch() {
        let svc = Ec2Service::new();
        make_check(&svc, &[("ClientToken", "token-2"), ("Path", "/healthz")]);
        let err = err_of(create_application_status_check(
            &svc,
            &req(
                "CreateApplicationStatusCheck",
                &[
                    ("Protocol", "http"),
                    ("Port", "8080"),
                    ("ClientToken", "token-2"),
                    ("Path", "/different"),
                ],
            ),
        ));
        assert_eq!(err.code(), "IdempotentParameterMismatch");

        // Tags are part of the request too.
        let err = err_of(create_application_status_check(
            &svc,
            &req(
                "CreateApplicationStatusCheck",
                &[
                    ("Protocol", "http"),
                    ("Port", "8080"),
                    ("ClientToken", "token-2"),
                    ("Path", "/healthz"),
                    (
                        "TagSpecification.1.ResourceType",
                        "application-status-check",
                    ),
                    ("TagSpecification.1.Tag.1.Key", "env"),
                    ("TagSpecification.1.Tag.1.Value", "prod"),
                ],
            ),
        ));
        assert_eq!(err.code(), "IdempotentParameterMismatch");

        // The unchanged retry still replays.
        let d = body(
            create_application_status_check(
                &svc,
                &req(
                    "CreateApplicationStatusCheck",
                    &[
                        ("Protocol", "http"),
                        ("Port", "8080"),
                        ("ClientToken", "token-2"),
                        ("Path", "/healthz"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(d.contains("<path>/healthz</path>"), "{d}");
        let all = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert_eq!(
            all.matches("<applicationStatusCheckId>").count(),
            1,
            "{all}"
        );

        // The retry is compared against the create request, not against the
        // check as it stands now, so editing the check in between does not
        // turn a legitimate retry into a mismatch.
        let id = all
            .split("<applicationStatusCheckId>")
            .nth(1)
            .unwrap()
            .split("</applicationStatusCheckId>")
            .next()
            .unwrap()
            .to_string();
        modify_application_status_check(
            &svc,
            &req(
                "ModifyApplicationStatusCheck",
                &[("ApplicationStatusCheckId", &id), ("Path", "/moved")],
            ),
        )
        .unwrap();
        crate::service::tags::create_tags(
            &svc,
            &req(
                "CreateTags",
                &[
                    ("ResourceId.1", &id),
                    ("Tag.1.Key", "added"),
                    ("Tag.1.Value", "later"),
                ],
            ),
        )
        .unwrap();
        let d = body(
            create_application_status_check(
                &svc,
                &req(
                    "CreateApplicationStatusCheck",
                    &[
                        ("Protocol", "http"),
                        ("Port", "8080"),
                        ("ClientToken", "token-2"),
                        ("Path", "/healthz"),
                    ],
                ),
            )
            .unwrap(),
        );
        // It replays the check as it stands, without minting a second one.
        assert!(d.contains("<path>/moved</path>"), "{d}");
        let all = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert_eq!(
            all.matches("<applicationStatusCheckId>").count(),
            1,
            "{all}"
        );
    }

    #[test]
    fn a_check_without_a_recorded_fingerprint_still_replays() {
        let svc = Ec2Service::new();
        let id = make_check(&svc, &[("ClientToken", "token-legacy")]);
        // A check persisted before the fingerprint was recorded.
        {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut("000000000000", "us-east-1");
            state
                .application_status_checks
                .get_mut(&id)
                .unwrap()
                .create_fingerprint = None;
        }
        let d = body(
            create_application_status_check(
                &svc,
                &req(
                    "CreateApplicationStatusCheck",
                    &[
                        ("Protocol", "http"),
                        ("Port", "8080"),
                        ("ClientToken", "token-legacy"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(d.contains(&id), "{d}");
    }

    #[test]
    fn a_dry_run_create_never_replays_an_existing_check() {
        let svc = Ec2Service::new();
        make_check(&svc, &[("ClientToken", "token-dry")]);
        let d = body(
            create_application_status_check(
                &svc,
                &req(
                    "CreateApplicationStatusCheck",
                    &[
                        ("Protocol", "http"),
                        ("Port", "8080"),
                        ("ClientToken", "token-dry"),
                        ("DryRun", "true"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(!d.contains("<applicationStatusCheck>"), "{d}");
    }

    #[test]
    fn describes_paginate_and_reject_a_foreign_token() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-6666666666666666f", &[]);
        let mut ids = Vec::new();
        for _ in 0..7 {
            ids.push(make_check(&svc, &[]));
        }
        for id in &ids {
            associate_application_status_check(
                &svc,
                &req(
                    "AssociateApplicationStatusCheck",
                    &[
                        ("ApplicationStatusCheckId", id),
                        ("InstanceId.1", "i-6666666666666666f"),
                    ],
                ),
            )
            .unwrap();
        }

        let page = |action: &'static str, params: &[(&str, &str)]| match action {
            "DescribeApplicationStatusChecks" => {
                body(describe_application_status_checks(&svc, &req(action, params)).unwrap())
            }
            _ => body(
                describe_application_status_check_associations(&svc, &req(action, params)).unwrap(),
            ),
        };
        let token_of = |xml: &str| {
            xml.split("<nextToken>").nth(1).map(|t| {
                t.split("</nextToken>")
                    .next()
                    .unwrap_or_default()
                    .to_string()
            })
        };
        let count = |xml: &str| xml.matches("<applicationStatusCheckId>").count();

        for action in [
            "DescribeApplicationStatusChecks",
            "DescribeApplicationStatusCheckAssociations",
        ] {
            let first = page(action, &[("MaxResults", "5")]);
            assert_eq!(count(&first), 5, "{action}: {first}");
            let token = token_of(&first).unwrap_or_else(|| panic!("{action} emitted no token"));
            let second = page(action, &[("MaxResults", "5"), ("NextToken", &token)]);
            assert_eq!(count(&second), 2, "{action}: {second}");
            assert!(token_of(&second).is_none(), "{action}: {second}");
            // The two pages partition the set instead of overlapping.
            for id in &ids {
                assert_eq!(
                    usize::from(first.contains(id.as_str()))
                        + usize::from(second.contains(id.as_str())),
                    1,
                    "{action} lost or duplicated {id}"
                );
            }
        }

        // A token this server never minted is an error, not a silent restart
        // at page one.
        let err = err_of(describe_application_status_checks(
            &svc,
            &req(
                "DescribeApplicationStatusChecks",
                &[("NextToken", "not-a-token")],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn describes_filter_on_the_modeled_names() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-7777777777777777a", &[]);
        let included = make_check(&svc, &[]);
        let excluded = make_check(&svc, &[("Aggregation", "excluded")]);

        let d = body(
            describe_application_status_checks(
                &svc,
                &req(
                    "DescribeApplicationStatusChecks",
                    &[
                        ("Filter.1.Name", "aggregation"),
                        ("Filter.1.Value.1", "excluded"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(d.contains(&excluded), "{d}");
        assert!(!d.contains(&included), "{d}");

        associate_application_status_check(
            &svc,
            &req(
                "AssociateApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &included),
                    ("InstanceId.1", "i-7777777777777777a"),
                ],
            ),
        )
        .unwrap();
        associate_application_status_check(
            &svc,
            &req(
                "AssociateApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &excluded),
                    ("TargetTagAssociation.1.Key", "env"),
                    ("TargetTagAssociation.1.Value", "prod"),
                ],
            ),
        )
        .unwrap();
        let a = body(
            describe_application_status_check_associations(
                &svc,
                &req(
                    "DescribeApplicationStatusCheckAssociations",
                    &[
                        ("Filter.1.Name", "association-type"),
                        ("Filter.1.Value.1", "tag"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(a.contains("<associationType>tag</associationType>"), "{a}");
        assert!(
            !a.contains("<associationType>instance-id</associationType>"),
            "{a}"
        );

        // `status` and `availability-zone-id` are the two documented filters
        // on DescribeApplicationStatus.
        let s = body(
            describe_application_status(
                &svc,
                &req(
                    "DescribeApplicationStatus",
                    &[("Filter.1.Name", "status"), ("Filter.1.Value.1", "ok")],
                ),
            )
            .unwrap(),
        );
        assert!(!s.contains("i-7777777777777777a"), "{s}");
        let s = body(
            describe_application_status(
                &svc,
                &req(
                    "DescribeApplicationStatus",
                    &[
                        ("Filter.1.Name", "status"),
                        ("Filter.1.Value.1", "insufficient-data"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(s.contains("i-7777777777777777a"), "{s}");
    }

    #[test]
    fn describes_reject_an_id_that_does_not_exist() {
        let svc = Ec2Service::new();
        for action in [
            "DescribeApplicationStatusChecks",
            "DescribeApplicationStatusCheckAssociations",
        ] {
            let params = [("ApplicationStatusCheckId.1", "asc-doesnotexist00")];
            let err = match action {
                "DescribeApplicationStatusChecks" => err_of(describe_application_status_checks(
                    &svc,
                    &req(action, &params),
                )),
                _ => err_of(describe_application_status_check_associations(
                    &svc,
                    &req(action, &params),
                )),
            };
            assert_eq!(
                err.code(),
                "InvalidApplicationStatusCheckId.NotFound",
                "{action}"
            );
        }
        let err = err_of(describe_application_status(
            &svc,
            &req(
                "DescribeApplicationStatus",
                &[("InstanceId.1", "i-doesnotexist00000")],
            ),
        ));
        assert_eq!(err.code(), "InvalidInstanceID.NotFound");
    }

    #[test]
    fn create_time_tags_land_in_the_shared_tag_store() {
        let svc = Ec2Service::new();
        let id = make_check(
            &svc,
            &[
                (
                    "TagSpecification.1.ResourceType",
                    "application-status-check",
                ),
                ("TagSpecification.1.Tag.1.Key", "Name"),
                ("TagSpecification.1.Tag.1.Value", "web-health"),
            ],
        );
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(
            d.contains("<key>Name</key><value>web-health</value>"),
            "{d}"
        );

        // The same store CreateTags writes to, so a tag added afterwards shows
        // up on the check.
        crate::service::tags::create_tags(
            &svc,
            &req(
                "CreateTags",
                &[
                    ("ResourceId.1", &id),
                    ("Tag.1.Key", "tier"),
                    ("Tag.1.Value", "gold"),
                ],
            ),
        )
        .unwrap();
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(d.contains("<key>tier</key><value>gold</value>"), "{d}");

        // And CreateTags on a check id that does not exist is rejected.
        let err = err_of(crate::service::tags::create_tags(
            &svc,
            &req(
                "CreateTags",
                &[
                    ("ResourceId.1", "asc-doesnotexist00"),
                    ("Tag.1.Key", "tier"),
                    ("Tag.1.Value", "gold"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidID");
    }

    #[test]
    fn a_replayed_create_returns_the_original_check() {
        let svc = Ec2Service::new();
        let first = make_check(&svc, &[("ClientToken", "token-1")]);
        let second = make_check(&svc, &[("ClientToken", "token-1")]);
        assert_eq!(first, second);
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert_eq!(d.matches("<applicationStatusCheckId>").count(), 1, "{d}");
    }

    #[test]
    fn a_dry_run_validates_that_the_check_exists() {
        let svc = Ec2Service::new();
        let missing = "asc-doesnotexist00";
        let err = err_of(modify_application_status_check(
            &svc,
            &req(
                "ModifyApplicationStatusCheck",
                &[("ApplicationStatusCheckId", missing), ("DryRun", "true")],
            ),
        ));
        assert_eq!(err.code(), "InvalidApplicationStatusCheckId.NotFound");
        let err = err_of(delete_application_status_check(
            &svc,
            &req(
                "DeleteApplicationStatusCheck",
                &[("ApplicationStatusCheckId", missing), ("DryRun", "true")],
            ),
        ));
        assert_eq!(err.code(), "InvalidApplicationStatusCheckId.NotFound");
        // And a dry run against a live check leaves it untouched.
        let id = make_check(&svc, &[("Path", "/healthz")]);
        modify_application_status_check(
            &svc,
            &req(
                "ModifyApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &id),
                    ("Path", "/changed"),
                    ("DryRun", "true"),
                ],
            ),
        )
        .unwrap();
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(d.contains("<path>/healthz</path>"), "{d}");
    }

    #[test]
    fn modify_holds_the_timeout_invariant_against_stored_values() {
        let svc = Ec2Service::new();
        let id = make_check(&svc, &[("Interval", "60"), ("Timeout", "5")]);
        // Timeout alone, checked against the stored Interval.
        let err = err_of(modify_application_status_check(
            &svc,
            &req(
                "ModifyApplicationStatusCheck",
                &[("ApplicationStatusCheckId", &id), ("Timeout", "60")],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
        let d = body(
            describe_application_status_checks(&svc, &req("DescribeApplicationStatusChecks", &[]))
                .unwrap(),
        );
        assert!(d.contains("<timeout>5</timeout>"), "{d}");
    }

    #[test]
    fn instance_status_reports_the_modeled_instance_fields() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-8888888888888888b", &[("env", "prod")]);
        let id = make_check(&svc, &[]);
        associate_application_status_check(
            &svc,
            &req(
                "AssociateApplicationStatusCheck",
                &[
                    ("ApplicationStatusCheckId", &id),
                    ("InstanceId.1", "i-8888888888888888b"),
                ],
            ),
        )
        .unwrap();
        let s = body(
            describe_application_status(&svc, &req("DescribeApplicationStatus", &[])).unwrap(),
        );
        assert!(
            s.contains("<availabilityZone>us-east-1a</availabilityZone>"),
            "{s}"
        );
        assert!(s.contains("<availabilityZoneId>"), "{s}");
        assert!(s.contains("<key>env</key><value>prod</value>"), "{s}");
        assert!(s.contains("<statusTimeStamp>"), "{s}");
        assert!(s.contains("<statusSince>"), "{s}");
        assert!(s.contains("<checkUpdateTime>"), "{s}");

        enable_application_status_check_suppression(
            &svc,
            &req(
                "EnableApplicationStatusCheckSuppression",
                &[
                    ("InstanceId.1", "i-8888888888888888b"),
                    ("DurationSeconds", "600"),
                ],
            ),
        )
        .unwrap();
        let s = body(
            describe_application_status(&svc, &req("DescribeApplicationStatus", &[])).unwrap(),
        );
        // A suppressed instance reports when reporting resumes.
        assert!(s.contains("<resumeAt>"), "{s}");
    }
}
