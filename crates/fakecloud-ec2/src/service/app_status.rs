//! Application status checks: an HTTP/HTTPS probe EC2 runs against instances,
//! plus the associations that decide which instances a check applies to and
//! the per-instance suppression windows.
//!
//! Associations are the interesting part: a check reaches an instance either
//! by instance id or by tag, and `DescribeApplicationStatus` resolves both —
//! so tagging an instance after the fact brings it under a tag-associated
//! check without re-associating.

use chrono::Utc;

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, invalid_parameter_value, not_found, parse_tag_pairs, require,
    validate_enum, validate_int_range,
};
use crate::state::{ApplicationStatusCheck, ApplicationStatusSuppression, Ec2State};

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

fn check_xml(c: &ApplicationStatusCheck) -> String {
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
    if let Some(d) = &c.deletion_time {
        s.push_str(&ec2_elem("deletionTime", d));
    }
    let pairs: Vec<String> = c
        .tag_associations
        .iter()
        .map(|(k, v)| format!("{}{}", ec2_elem("key", k), ec2_elem("value", v)))
        .collect();
    if !pairs.is_empty() {
        s.push_str(&ec2_list("targetTagAssociationSet", &pairs));
    }
    let tags: Vec<String> = c
        .tags
        .iter()
        .map(|(k, v)| format!("{}{}", ec2_elem("key", k), ec2_elem("value", v)))
        .collect();
    if !tags.is_empty() {
        s.push_str(&ec2_list("tagSet", &tags));
    }
    s
}

/// Validate the probe settings shared by Create and Modify.
fn validate_probe(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_enum(&req.query_params, "Protocol", &["http", "https"])?;
    validate_enum(&req.query_params, "Aggregation", &["included", "excluded"])?;
    validate_enum(&req.query_params, "IpVersion", &["ipv4", "ipv6"])?;
    validate_enum(&req.query_params, "IpScope", &["private"])?;
    validate_int_range(&req.query_params, "Port", 1, 65_535)?;
    validate_int_range(&req.query_params, "Interval", 5, 300)?;
    validate_int_range(&req.query_params, "Timeout", 2, 120)?;
    validate_int_range(&req.query_params, "FailureThreshold", 1, 10)?;
    validate_int_range(&req.query_params, "SuccessThreshold", 1, 10)?;
    // -1 disables the grace period, so the modeled floor is below zero.
    validate_int_range(
        &req.query_params,
        "InitializationGracePeriodSeconds",
        -1,
        600,
    )?;
    // A probe that times out no sooner than it repeats can never report a
    // result before the next attempt starts.
    let interval = req
        .query_params
        .get("Interval")
        .and_then(|v| v.parse::<i64>().ok());
    let timeout = req
        .query_params
        .get("Timeout")
        .and_then(|v| v.parse::<i64>().ok());
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

pub(crate) fn create_application_status_check(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let protocol = require(&req.query_params, "Protocol")?;
    require(&req.query_params, "Port")?;
    validate_probe(req)?;
    require_int_params(
        req,
        &[
            "Port",
            "DeviceIndex",
            "Interval",
            "Timeout",
            "FailureThreshold",
            "SuccessThreshold",
            "InitializationGracePeriodSeconds",
        ],
    )?;
    let port = int_param(req, "Port").unwrap_or(80);

    if dry_run(req) {
        return Ok(Ec2Service::respond(
            "CreateApplicationStatusCheck",
            &req.request_id,
            "",
        ));
    }

    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let now = now_rfc3339();
    let check = ApplicationStatusCheck {
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
        tags: parse_tag_pairs(&req.query_params, "TagSpecification.1.Tag")
            .into_iter()
            .map(|(k, v)| (k, v.unwrap_or_default()))
            .collect(),
        creation_time: now.clone(),
        modify_time: now,
        deletion_time: None,
    };
    let body = format!(
        "<applicationStatusCheck>{}</applicationStatusCheck>",
        check_xml(&check)
    );
    state
        .application_status_checks
        .insert(check.id.clone(), check);
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
    validate_int_range(&req.query_params, "MaxResults", 5, 100)?;
    let ids = indexed_list(&req.query_params, "ApplicationStatusCheckId");
    // Deleted checks are tombstoned; only `IncludeAll` surfaces them.
    let include_all = req
        .query_params
        .get("IncludeAll")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .map(|s| {
            s.application_status_checks
                .values()
                .filter(|c| ids.is_empty() || ids.contains(&c.id))
                .filter(|c| include_all || c.deletion_time.is_none())
                .map(check_xml)
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "DescribeApplicationStatusChecks",
        &req.request_id,
        &ec2_list("applicationStatusCheckSet", &items),
    ))
}

pub(crate) fn modify_application_status_check(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ApplicationStatusCheckId")?;
    validate_probe(req)?;
    require_int_params(
        req,
        &[
            "Port",
            "DeviceIndex",
            "Interval",
            "Timeout",
            "FailureThreshold",
            "SuccessThreshold",
            "InitializationGracePeriodSeconds",
        ],
    )?;
    let paths = health_check_paths(req);
    if dry_run(req) {
        return Ok(Ec2Service::respond(
            "ModifyApplicationStatusCheck",
            &req.request_id,
            "",
        ));
    }
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let check = get_check(state, &id)?;
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
    if !paths.is_empty() {
        check.health_check_paths = paths;
    }
    check.modify_time = now_rfc3339();
    let body = format!(
        "<applicationStatusCheck>{}</applicationStatusCheck>",
        check_xml(check)
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
    if dry_run(req) {
        return Ok(Ec2Service::respond(
            "DeleteApplicationStatusCheck",
            &req.request_id,
            "",
        ));
    }
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let check = get_check(state, &id)?;
    if check.deletion_time.is_some() {
        return Err(not_found("InvalidApplicationStatusCheckId.NotFound", &id));
    }
    // AWS reports a deletion time on the returned object, so the check is
    // tombstoned rather than dropped; its associations go with it.
    check.deletion_time = Some(now_rfc3339());
    check.instance_ids.clear();
    check.tag_associations.clear();
    let body = format!(
        "<applicationStatusCheck>{}</applicationStatusCheck>",
        check_xml(check)
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
    if dry_run(req) {
        return Ok(Ec2Service::respond(action, &req.request_id, ""));
    }

    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let known_instances: Vec<String> = state.instances.keys().cloned().collect();
    let check = get_check(state, &id)?;

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
    validate_int_range(&req.query_params, "MaxResults", 5, 1_000)?;
    let ids = indexed_list(&req.query_params, "ApplicationStatusCheckId");
    let accounts = svc.state.read();
    let mut items = Vec::new();
    if let Some(state) = accounts.get(&req.account_id) {
        for c in state.application_status_checks.values() {
            if !ids.is_empty() && !ids.contains(&c.id) {
                continue;
            }
            for instance_id in &c.instance_ids {
                items.push(format!(
                    "{}{}{}",
                    ec2_elem("applicationStatusCheckId", &c.id),
                    ec2_elem("associationType", "instance-id"),
                    ec2_elem("value", instance_id)
                ));
            }
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
    Ok(Ec2Service::respond(
        "DescribeApplicationStatusCheckAssociations",
        &req.request_id,
        &ec2_list("associationSet", &items),
    ))
}

pub(crate) fn describe_application_status(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_int_range(&req.query_params, "MaxResults", 1, 100)?;
    let requested = indexed_list(&req.query_params, "InstanceId");
    let accounts = svc.state.read();
    let mut items = Vec::new();
    if let Some(state) = accounts.get(&req.account_id) {
        let now = Utc::now();
        for instance_id in state.instances.keys() {
            if !requested.is_empty() && !requested.contains(instance_id) {
                continue;
            }
            // A check reaches this instance either by id or by one of its
            // tags, so tagging an instance later brings it under a
            // tag-associated check without re-associating.
            let instance_tags = state.tags.get(instance_id);
            let checks: Vec<&ApplicationStatusCheck> = state
                .application_status_checks
                .values()
                .filter(|c| c.deletion_time.is_none())
                .filter(|c| {
                    c.instance_ids.contains(instance_id)
                        || c.tag_associations.iter().any(|(k, v)| {
                            instance_tags.is_some_and(|tags| {
                                tags.iter().any(|t| &t.key == k && &t.value == v)
                            })
                        })
                })
                .collect();

            let suppressed = state
                .application_status_suppressions
                .get(instance_id)
                .is_some_and(|s| {
                    s.resume_at
                        .as_deref()
                        .and_then(|r| chrono::DateTime::parse_from_rfc3339(r).ok())
                        .is_none_or(|r| r > now)
                });

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
            let status = if suppressed {
                "suppressed"
            } else if included == 0 {
                "not-applicable"
            } else {
                "insufficient-data"
            };
            let details: Vec<String> = checks
                .iter()
                .map(|c| {
                    format!(
                        "{}{}{}",
                        ec2_elem("applicationStatusCheckId", &c.id),
                        ec2_elem("aggregation", &c.aggregation),
                        ec2_elem("status", "insufficient-data")
                    )
                })
                .collect();
            let mut app_status = ec2_elem("status", status);
            if !details.is_empty() {
                app_status.push_str(&ec2_list("detailSet", &details));
            }
            items.push(format!(
                "{}{}",
                ec2_elem("instanceId", instance_id),
                format_args!("<applicationStatus>{app_status}</applicationStatus>")
            ));
        }
    }
    Ok(Ec2Service::respond(
        "DescribeApplicationStatus",
        &req.request_id,
        &format!(
            "<applicationStatusesResponseType>{}</applicationStatusesResponseType>",
            ec2_list("instanceSet", &items)
        ),
    ))
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
    let duration = int_param(req, "DurationSeconds");
    if let Some(d) = duration {
        if d <= 0 {
            return Err(invalid_parameter_value(
                "DurationSeconds must be a positive integer",
            ));
        }
    }
    if dry_run(req) {
        return Ok(Ec2Service::respond(action, &req.request_id, ""));
    }

    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let known: Vec<String> = state.instances.keys().cloned().collect();
    let now = Utc::now();
    let suppress_at = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let resume_at = duration.map(|d| {
        (now + chrono::Duration::seconds(d)).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    });

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
        if enable {
            state.application_status_suppressions.insert(
                instance_id.clone(),
                ApplicationStatusSuppression {
                    instance_id: instance_id.clone(),
                    suppress_at: suppress_at.clone(),
                    resume_at: resume_at.clone(),
                },
            );
        } else {
            state.application_status_suppressions.remove(&instance_id);
        }
        let mut entry = ec2_elem("instanceId", &instance_id);
        entry.push_str(&ec2_elem("suppressAt", &suppress_at));
        if let Some(r) = &resume_at {
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
        let state = accounts.get_or_create("000000000000");
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
            &[("Path", "/healthz"), ("Interval", "30"), ("Timeout", "5")],
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
            // A probe that times out no sooner than it repeats is rejected.
            vec![
                ("Protocol", "http"),
                ("Port", "80"),
                ("Interval", "10"),
                ("Timeout", "10"),
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
}
