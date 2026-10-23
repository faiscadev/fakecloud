//! On-demand capacity reservations, capacity reservation fleets, capacity
//! blocks, billing-owner transfer, and interruptible allocations.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    ec2_arn, filter_value_matches, gen_id, incorrect_state, indexed_list, invalid_parameter_value,
    missing_parameter, not_found, paginate, parse_filters, require, validate_enum,
    validate_int_range, validate_length, validate_max_results, Filter,
};
use crate::state::{
    CapacityReservation, CapacityReservationAdjustment, CapacityReservationCommitment,
    CapacityReservationModificationQuote, Ec2State, InterruptibleAllocation, Tag,
};
use chrono::{DateTime, Duration, Utc};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

/// Cumulative start-date pushout allowed past a reservation's original start.
const MAX_PUSHOUT_DAYS: i64 = 30;
/// Date change quotes are valid for 24 hours...
const QUOTE_VALIDITY_HOURS: i64 = 24;
/// ...and always expire at least one hour before the reservation starts.
const QUOTE_START_MARGIN_HOURS: i64 = 1;

fn fmt_ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn parse_ts(v: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(v)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// Parse an optional ISO 8601 timestamp parameter, rejecting a malformed one.
fn ts_param(req: &AwsRequest, key: &str) -> Result<Option<DateTime<Utc>>, AwsServiceError> {
    match req.query_params.get(key).filter(|v| !v.is_empty()) {
        None => Ok(None),
        Some(v) => parse_ts(v)
            .map(Some)
            .ok_or_else(|| invalid_parameter_value(format!("Invalid value '{v}' for {key}"))),
    }
}

/// The reservation's state as of `now`: a future-dated reservation is
/// `scheduled` until its start date arrives, then it is delivered (`active`).
fn current_state(r: &CapacityReservation, now: DateTime<Utc>) -> String {
    if r.state == "scheduled"
        && r.start_date
            .as_deref()
            .and_then(parse_ts)
            .is_some_and(|s| s <= now)
    {
        return "active".to_string();
    }
    r.state.clone()
}

fn commitment_xml(c: &CapacityReservationCommitment) -> String {
    format!(
        "<commitmentInfo><committedInstanceCount>{}</committedInstanceCount>{}<commitmentDuration>{}</commitmentDuration></commitmentInfo>",
        c.committed_instance_count,
        ec2_elem("commitmentEndDate", &c.commitment_end_date),
        c.commitment_duration,
    )
}

fn adjustment_xml(a: &CapacityReservationAdjustment) -> String {
    let mut out = String::from("<adjustmentDetails>");
    if let Some(v) = &a.start_date {
        out.push_str(&ec2_elem("startDate", v));
    }
    if let Some(v) = &a.end_date {
        out.push_str(&ec2_elem("endDate", v));
    }
    if let Some(v) = &a.commitment_end_date {
        out.push_str(&ec2_elem("commitmentEndDate", v));
    }
    out.push_str(&ec2_elem("endDateType", &a.end_date_type));
    if let Some(v) = a.commitment_duration {
        out.push_str(&format!("<commitmentDuration>{v}</commitmentDuration>"));
    }
    out.push_str("</adjustmentDetails>");
    out
}

/// `adjustmentStatus` + `adjustmentDetails`, shared by the reservation render
/// and the `ModifyCapacityReservation` result.
fn adjustment_fields_xml(r: &CapacityReservation) -> String {
    let mut out = String::new();
    if let Some(s) = &r.adjustment_status {
        out.push_str(&ec2_elem("adjustmentStatus", s));
    }
    if let Some(a) = &r.adjustment_details {
        out.push_str(&adjustment_xml(a));
    }
    out
}

const INSTANCE_PLATFORMS: &[&str] = &[
    "Linux/UNIX",
    "Red Hat Enterprise Linux",
    "SUSE Linux",
    "Windows",
    "Windows with SQL Server",
    "Windows with SQL Server Enterprise",
    "Windows with SQL Server Standard",
    "Windows with SQL Server Web",
    "Linux with SQL Server Standard",
    "Linux with SQL Server Web",
    "Linux with SQL Server Enterprise",
    "RHEL with SQL Server Standard",
    "RHEL with SQL Server Enterprise",
    "RHEL with SQL Server Web",
    "RHEL with HA",
    "RHEL with HA and SQL Server Standard",
    "RHEL with HA and SQL Server Enterprise",
    "Ubuntu Pro",
];

fn cr_xml(r: &CapacityReservation, tags: &[Tag], owner: &str) -> String {
    let mut extra = ec2_elem(
        "reservationType",
        r.reservation_type.as_deref().unwrap_or("default"),
    );
    if let Some(e) = &r.end_date {
        extra.push_str(&ec2_elem("endDate", e));
    }
    if let Some(c) = &r.commitment {
        extra.push_str(&commitment_xml(c));
    }
    if let Some(o) = &r.original_start_date {
        extra.push_str(&ec2_elem("originalStartDate", o));
    }
    extra.push_str(&adjustment_fields_xml(r));
    extra.push_str(&interruptible_fields_xml(r));
    format!(
        "{}{}{}{}{}{}{}<totalInstanceCount>{}</totalInstanceCount><availableInstanceCount>{}</availableInstanceCount>\
         <ebsOptimized>{}</ebsOptimized><ephemeralStorage>{}</ephemeralStorage>{}{}{}{}{}{}{}",
        ec2_elem("capacityReservationId", &r.id),
        ec2_elem("ownerId", owner),
        ec2_elem("capacityReservationArn", &ec2_arn(&region_of(r), owner, &format!("capacity-reservation/{}", r.id))),
        ec2_elem("instanceType", &r.instance_type),
        ec2_elem("instancePlatform", &r.instance_platform),
        ec2_elem("availabilityZone", &r.availability_zone),
        ec2_elem("tenancy", &r.tenancy),
        r.total_instance_count,
        r.available_instance_count,
        r.ebs_optimized,
        r.ephemeral_storage,
        ec2_elem("state", &current_state(r, Utc::now())),
        ec2_elem("endDateType", &r.end_date_type),
        ec2_elem("instanceMatchCriteria", &r.instance_match_criteria),
        ec2_elem("createDate", r.create_date.as_deref().unwrap_or(FIXED_TIME)),
        ec2_elem("startDate", r.start_date.as_deref().unwrap_or(FIXED_TIME)),
        super::tags::tag_set_xml(tags),
        extra,
    )
}

/// `interruptible` plus, for an interruptible reservation, its
/// `interruptionInfo` / `zeroSizePreference`, and for a source reservation its
/// `interruptibleCapacityAllocation`.
fn interruptible_fields_xml(r: &CapacityReservation) -> String {
    let mut out = format!("<interruptible>{}</interruptible>", r.interruptible);
    if let Some(a) = &r.interruptible_allocation {
        out.push_str(&format!(
            "<interruptibleCapacityAllocation><instanceCount>{}</instanceCount><targetInstanceCount>{}</targetInstanceCount>{}{}{}{}</interruptibleCapacityAllocation>",
            a.instance_count,
            a.target_instance_count,
            ec2_elem("status", &a.status),
            ec2_elem("interruptibleCapacityReservationId", &a.interruptible_reservation_id),
            ec2_elem("interruptionType", INTERRUPTION_TYPE),
            ec2_elem("zeroSizePreference", &a.zero_size_preference),
        ));
    }
    if let Some(src) = &r.interruption_source_id {
        out.push_str(&format!(
            "<interruptionInfo>{}{}</interruptionInfo>",
            ec2_elem("sourceCapacityReservationId", src),
            ec2_elem("interruptionType", INTERRUPTION_TYPE),
        ));
    }
    if let Some(p) = &r.zero_size_preference {
        out.push_str(&ec2_elem("zeroSizePreference", p));
    }
    out
}

/// `InvalidCapacityReservationId.NotFound` — the reservation does not exist.
fn cr_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "InvalidCapacityReservationId.NotFound",
        format!("The capacity reservation ID '{id}' does not exist"),
    )
}

fn region_of(r: &CapacityReservation) -> String {
    r.availability_zone
        .trim_end_matches(|c: char| c.is_alphabetic())
        .to_string()
}

pub(crate) fn create_capacity_reservation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let instance_type = require(&req.query_params, "InstanceType")?;
    let platform = require(&req.query_params, "InstancePlatform")?;
    let count: i64 = require(&req.query_params, "InstanceCount")?
        .parse()
        .unwrap_or(1);
    validate_enum(&req.query_params, "InstancePlatform", INSTANCE_PLATFORMS)?;
    validate_enum(&req.query_params, "Tenancy", &["default", "dedicated"])?;
    validate_enum(&req.query_params, "EndDateType", &["unlimited", "limited"])?;
    validate_enum(
        &req.query_params,
        "InstanceMatchCriteria",
        &["open", "targeted"],
    )?;
    validate_enum(
        &req.query_params,
        "DeliveryPreference",
        &["fixed", "incremental"],
    )?;
    validate_int_range(&req.query_params, "CommitmentDuration", 1, 200_000_000)?;
    let now = Utc::now();
    let start = ts_param(req, "StartDate")?;
    let end = ts_param(req, "EndDate")?;
    // A reservation whose StartDate lies in the future is future-dated: it is
    // `scheduled` until delivered, carries its original start date, and any
    // CommitmentDuration runs from that start.
    let future_start = start.filter(|s| *s > now);
    let commitment = match (
        future_start,
        req.query_params
            .get("CommitmentDuration")
            .and_then(|v| v.parse::<i64>().ok()),
    ) {
        (Some(s), Some(secs)) => Some(CapacityReservationCommitment {
            committed_instance_count: count,
            commitment_duration: secs,
            commitment_end_date: fmt_ts(s + Duration::seconds(secs)),
        }),
        _ => None,
    };
    let id = gen_id("cr");
    let r = CapacityReservation {
        id: id.clone(),
        instance_type,
        instance_platform: platform,
        availability_zone: req
            .query_params
            .get("AvailabilityZone")
            .cloned()
            .unwrap_or_else(|| {
                format!(
                    "{}a",
                    if req.region.is_empty() {
                        "us-east-1"
                    } else {
                        &req.region
                    }
                )
            }),
        tenancy: req
            .query_params
            .get("Tenancy")
            .cloned()
            .unwrap_or_else(|| "default".to_string()),
        total_instance_count: count,
        available_instance_count: count,
        state: if future_start.is_some() {
            "scheduled"
        } else {
            "active"
        }
        .to_string(),
        start_date: start.map(fmt_ts),
        end_date: end.map(fmt_ts),
        original_start_date: future_start.map(fmt_ts),
        commitment,
        adjustment_status: None,
        adjustment_details: None,
        reservation_type: None,
        create_date: None,
        ebs_optimized: req
            .query_params
            .get("EbsOptimized")
            .is_some_and(|v| v == "true"),
        ephemeral_storage: req
            .query_params
            .get("EphemeralStorage")
            .is_some_and(|v| v == "true"),
        end_date_type: req
            .query_params
            .get("EndDateType")
            .cloned()
            .unwrap_or_else(|| "unlimited".to_string()),
        instance_match_criteria: req
            .query_params
            .get("InstanceMatchCriteria")
            .cloned()
            .unwrap_or_else(|| "open".to_string()),
        ..Default::default()
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "capacity-reservation",
        );
        let t = state.tags_for(&id).to_vec();
        state.capacity_reservations.insert(id.clone(), r.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateCapacityReservation",
        &req.request_id,
        &format!(
            "<capacityReservation>{}</capacityReservation>",
            cr_xml(&r, &tags, &owner)
        ),
    ))
}

/// Keep an interruptible allocation consistent when either side of it is
/// cancelled with `CancelCapacityReservation`.
///
/// - Cancelling the interruptible reservation ends the allocation the same
///   way reducing it to zero under `default` does ("permanently remove the
///   allocation and return all capacity", EC2 User Guide, "Interruptible
///   Capacity Reservations for capacity owners"): the capacity goes back to
///   the source and the source's allocation is marked `canceled`.
/// - Cancelling the source ends the capacity the interruptible reservation
///   was carved from, so the interruptible reservation is cancelled with it
///   and the allocation is marked `canceled`. (The guide does not spell this
///   case out; the interruptible reservation cannot outlive the reservation
///   that owns its capacity.)
fn cancel_interruptible_links(
    state: &mut Ec2State,
    cancelled_id: &str,
    source_id: Option<String>,
    allocation: Option<InterruptibleAllocation>,
) {
    if let Some(src) = source_id {
        if let Some(source) = state.capacity_reservations.get_mut(&src) {
            if let Some(a) = source.interruptible_allocation.as_mut().filter(|a| {
                a.interruptible_reservation_id == cancelled_id && a.status != "canceled"
            }) {
                let returned = a.instance_count;
                a.instance_count = 0;
                a.target_instance_count = 0;
                a.status = "canceled".to_string();
                source.total_instance_count += returned;
                source.available_instance_count += returned;
            }
        }
    }
    if let Some(a) = allocation.filter(|a| a.status != "canceled") {
        if let Some(icr) = state
            .capacity_reservations
            .get_mut(&a.interruptible_reservation_id)
        {
            icr.state = "cancelled".to_string();
        }
        if let Some(source) = state.capacity_reservations.get_mut(cancelled_id) {
            if let Some(sa) = source.interruptible_allocation.as_mut() {
                sa.status = "canceled".to_string();
            }
        }
    }
}

pub(crate) fn cancel_capacity_reservation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CapacityReservationId")?;
    validate_enum(
        &req.query_params,
        "ApplyCancellationCharges",
        &["commitment-wind-down"],
    )?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let r = state
            .capacity_reservations
            .get_mut(&id)
            .ok_or_else(|| cr_not_found(&id))?;
        r.state = "cancelled".to_string();
        let source_id = r.interruption_source_id.clone();
        let allocation = r.interruptible_allocation.clone();
        cancel_interruptible_links(state, &id, source_id, allocation);
    }
    Ok(Ec2Service::respond(
        "CancelCapacityReservation",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_capacity_reservations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    let wanted = indexed_list(&req.query_params, "CapacityReservationId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let mut items: Vec<String> = state
        .capacity_reservations
        .values()
        .filter(|r| wanted.is_empty() || wanted.contains(&r.id))
        .map(|r| cr_xml(r, state.tags_for(&r.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeCapacityReservations",
        &req.request_id,
        &ec2_list("capacityReservationSet", &items),
    ))
}

pub(crate) fn modify_capacity_reservation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CapacityReservationId")?;
    validate_enum(&req.query_params, "EndDateType", &["unlimited", "limited"])?;
    validate_enum(
        &req.query_params,
        "InstanceMatchCriteria",
        &["open", "targeted"],
    )?;
    let end = ts_param(req, "EndDate")?;
    let start = ts_param(req, "StartDate")?;
    let quote_id = req
        .query_params
        .get("QuoteId")
        .filter(|v| !v.is_empty())
        .cloned();
    let accept_terms = req
        .query_params
        .get("AcceptModificationTerms")
        .is_some_and(|v| v == "true");
    // A start-date change is only ever applied through an accepted quote.
    if start.is_some() && quote_id.is_none() {
        return Err(missing_parameter("QuoteId"));
    }
    if quote_id.is_some() && !accept_terms {
        return Err(invalid_parameter_value(
            "AcceptModificationTerms must be true to apply the modification described by QuoteId",
        ));
    }
    let now = Utc::now();
    let result = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let r = state
            .capacity_reservations
            .get_mut(&id)
            .ok_or_else(|| cr_not_found(&id))?;
        let quoted_start = match &quote_id {
            None => None,
            Some(qid) => {
                let q = state
                    .capacity_reservation_modification_quotes
                    .get(qid)
                    .ok_or_else(|| quote_not_found(qid))?;
                if q.capacity_reservation_id != id {
                    return Err(invalid_parameter_value(format!(
                        "The quote '{qid}' is not for Capacity Reservation '{id}'"
                    )));
                }
                if quote_state(q, now) != "active" {
                    return Err(incorrect_state(format!(
                        "The quote '{qid}' is expired or has already been used"
                    )));
                }
                let new_start = parse_ts(&q.new_start_date).unwrap_or(now);
                // Quote dates are stored at millisecond precision; compare the
                // caller's StartDate at the same precision.
                if start.is_some_and(|s| fmt_ts(s) != q.new_start_date) {
                    return Err(invalid_parameter_value(format!(
                        "StartDate does not match the start date quoted by '{qid}'"
                    )));
                }
                Some(new_start)
            }
        };
        if let Some(new_start) = quoted_start {
            if current_state(r, now) != "scheduled" {
                return Err(incorrect_state(format!(
                    "The start date of Capacity Reservation '{id}' can't be changed because it has already been delivered"
                )));
            }
            r.start_date = Some(fmt_ts(new_start));
            if let Some(c) = r.commitment.as_mut() {
                c.commitment_end_date =
                    fmt_ts(new_start + Duration::seconds(c.commitment_duration));
            }
        }
        if let Some(e) = end {
            r.end_date = Some(fmt_ts(e));
        }
        if let Some(c) = req
            .query_params
            .get("InstanceCount")
            .and_then(|v| v.parse().ok())
        {
            r.total_instance_count = c;
            r.available_instance_count = c;
        }
        // EndDateType and InstanceMatchCriteria are validated above but were
        // never persisted -> DescribeCapacityReservations read back the
        // create-time values and aws_ec2_capacity_reservation drifted. Both are
        // modifiable per AWS; honor them.
        if let Some(t) = req.query_params.get("EndDateType") {
            r.end_date_type = t.clone();
            // An unlimited reservation has no end date.
            if t == "unlimited" {
                r.end_date = None;
            }
        }
        if let Some(m) = req.query_params.get("InstanceMatchCriteria") {
            r.instance_match_criteria = m.clone();
        }
        // Modifications apply synchronously here, so the most recent one is
        // always `applied` and the details are the resulting configuration.
        r.adjustment_status = Some("applied".to_string());
        r.adjustment_details = Some(CapacityReservationAdjustment {
            start_date: r.start_date.clone(),
            end_date: r.end_date.clone(),
            commitment_end_date: r.commitment.as_ref().map(|c| c.commitment_end_date.clone()),
            end_date_type: r.end_date_type.clone(),
            commitment_duration: r.commitment.as_ref().map(|c| c.commitment_duration),
        });
        let result = adjustment_fields_xml(r);
        // Accepting a quote changes the reservation the other outstanding
        // quotes were priced against, so every quote for it is spent.
        if quoted_start.is_some() {
            for q in state
                .capacity_reservation_modification_quotes
                .values_mut()
                .filter(|q| q.capacity_reservation_id == id)
            {
                q.used = true;
            }
        }
        result
    };
    Ok(Ec2Service::respond(
        "ModifyCapacityReservation",
        &req.request_id,
        &format!("{}{result}", ec2_return(true)),
    ))
}

// ---- date change quotes ----

fn quote_not_found(id: &str) -> AwsServiceError {
    not_found("InvalidCapacityReservationModificationQuoteId.NotFound", id)
}

/// `active` until the quote is used or reaches its expiration time.
fn quote_state(q: &CapacityReservationModificationQuote, now: DateTime<Utc>) -> &'static str {
    let expired = q.used || parse_ts(&q.expiration_time).is_none_or(|e| now >= e);
    if expired {
        "expired"
    } else {
        "active"
    }
}

fn quote_xml(q: &CapacityReservationModificationQuote, tags: &[Tag], now: DateTime<Utc>) -> String {
    let mut update = ec2_elem("newStartDate", &q.new_start_date);
    if let Some(v) = &q.new_commitment_end_date {
        update = format!("{}{update}", ec2_elem("newCommitmentEndDate", v));
    }
    if let Some(v) = q.new_commitment_duration {
        update.push_str(&format!(
            "<newCommitmentDuration>{v}</newCommitmentDuration>"
        ));
    }
    format!(
        "{}{}{}{}{}<currentConfiguration><instanceCount>{}</instanceCount>{}{}{}</currentConfiguration>\
         <modificationTerms><reservationUpdate>{update}</reservationUpdate></modificationTerms>{}",
        ec2_elem("capacityReservationModificationQuoteId", &q.id),
        ec2_elem("capacityReservationId", &q.capacity_reservation_id),
        ec2_elem("createTime", &q.create_time),
        ec2_elem("expirationTime", &q.expiration_time),
        ec2_elem("quoteState", quote_state(q, now)),
        q.current_instance_count,
        ec2_elem("reservationState", &q.current_reservation_state),
        ec2_elem("startDate", &q.current_start_date),
        ec2_elem("originalStartDate", &q.original_start_date),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_capacity_reservation_date_change_quote(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CapacityReservationId")?;
    let new_start =
        ts_param(req, "NewStartDate")?.ok_or_else(|| missing_parameter("NewStartDate"))?;
    let client_token = req
        .query_params
        .get("ClientToken")
        .filter(|v| !v.is_empty())
        .cloned();
    let now = Utc::now();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    // Idempotent retry: the same ClientToken returns the original quote.
    if let Some(token) = &client_token {
        if let Some(q) = state
            .capacity_reservation_modification_quotes
            .values()
            .find(|q| q.client_token.as_deref() == Some(token.as_str()))
        {
            if q.capacity_reservation_id != id || q.new_start_date != fmt_ts(new_start) {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "IdempotentParameterMismatch",
                    "The client token has already been used with different parameters",
                ));
            }
            let body = format!(
                "<capacityReservationModificationQuote>{}</capacityReservationModificationQuote>",
                quote_xml(q, state.tags_for(&q.id), now)
            );
            return Ok(Ec2Service::respond(
                "CreateCapacityReservationDateChangeQuote",
                &req.request_id,
                &body,
            ));
        }
    }
    let r = state
        .capacity_reservations
        .get(&id)
        .ok_or_else(|| cr_not_found(&id))?;
    let reservation_state = current_state(r, now);
    let current_start = r.start_date.as_deref().and_then(parse_ts);
    let (Some(current_start), "scheduled") = (current_start, reservation_state.as_str()) else {
        return Err(incorrect_state(format!(
            "Capacity Reservation '{id}' is not a future-dated Capacity Reservation that has not yet been delivered"
        )));
    };
    let original_start = r
        .original_start_date
        .as_deref()
        .and_then(parse_ts)
        .unwrap_or(current_start);
    if new_start <= current_start {
        return Err(invalid_parameter_value(
            "NewStartDate must be later than the current start date of the Capacity Reservation",
        ));
    }
    if new_start > original_start + Duration::days(MAX_PUSHOUT_DAYS) {
        return Err(invalid_parameter_value(format!(
            "NewStartDate exceeds the {MAX_PUSHOUT_DAYS}-day limit on start date pushouts from the original start date"
        )));
    }
    let expiration = (now + Duration::hours(QUOTE_VALIDITY_HOURS))
        .min(current_start - Duration::hours(QUOTE_START_MARGIN_HOURS));
    if expiration <= now {
        return Err(incorrect_state(format!(
            "Capacity Reservation '{id}' starts too soon for its start date to be changed"
        )));
    }
    let quote = CapacityReservationModificationQuote {
        id: gen_id("crmq"),
        capacity_reservation_id: id.clone(),
        create_time: fmt_ts(now),
        expiration_time: fmt_ts(expiration),
        used: false,
        client_token,
        current_instance_count: r.total_instance_count,
        current_reservation_state: reservation_state,
        current_start_date: fmt_ts(current_start),
        original_start_date: fmt_ts(original_start),
        new_start_date: fmt_ts(new_start),
        new_commitment_end_date: r
            .commitment
            .as_ref()
            .map(|c| fmt_ts(new_start + Duration::seconds(c.commitment_duration))),
        new_commitment_duration: r.commitment.as_ref().map(|c| c.commitment_duration),
    };
    crate::service::tags::apply_tag_specifications(
        state,
        &req.query_params,
        &quote.id,
        "capacity-reservation-modification-quote",
    );
    let body = format!(
        "<capacityReservationModificationQuote>{}</capacityReservationModificationQuote>",
        quote_xml(&quote, state.tags_for(&quote.id), now)
    );
    state
        .capacity_reservation_modification_quotes
        .insert(quote.id.clone(), quote);
    Ok(Ec2Service::respond(
        "CreateCapacityReservationDateChangeQuote",
        &req.request_id,
        &body,
    ))
}

fn quote_matches(
    q: &CapacityReservationModificationQuote,
    tags: &[Tag],
    filters: &[Filter],
    now: DateTime<Utc>,
) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "capacity-reservation-id" => vec![q.capacity_reservation_id.clone()],
            "capacity-reservation-modification-quote-id" => vec![q.id.clone()],
            "quote-state" => vec![quote_state(q, now).to_string()],
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            name => match name.strip_prefix("tag:") {
                Some(key) => tags
                    .iter()
                    .filter(|t| t.key == key)
                    .map(|t| t.value.clone())
                    .collect(),
                None => return false,
            },
        };
        f.values
            .iter()
            .any(|v| candidates.iter().any(|c| filter_value_matches(v, c)))
    })
}

pub(crate) fn describe_capacity_reservation_date_change_quotes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    let wanted = indexed_list(&req.query_params, "CapacityReservationModificationQuoteId");
    let filters = parse_filters(&req.query_params);
    let now = Utc::now();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let quotes = &state.capacity_reservation_modification_quotes;
    if let Some(missing) = wanted.iter().find(|id| !quotes.contains_key(*id)) {
        return Err(quote_not_found(missing));
    }
    // BTreeMap iteration is already id-ordered, giving stable pagination.
    let items: Vec<String> = quotes
        .values()
        .filter(|q| wanted.is_empty() || wanted.contains(&q.id))
        .filter(|q| quote_matches(q, state.tags_for(&q.id), &filters, now))
        .map(|q| quote_xml(q, state.tags_for(&q.id), now))
        .collect();
    let max_results = req
        .query_params
        .get("MaxResults")
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<usize>().ok());
    let next_token = req.query_params.get("NextToken").map(String::as_str);
    let (page, token) = paginate(&items, next_token, max_results)?;
    let body = format!(
        "{}{}",
        ec2_list("capacityReservationModificationQuoteSet", &page),
        token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
    );
    Ok(Ec2Service::respond(
        "DescribeCapacityReservationDateChangeQuotes",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_capacity_reservation_usage(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CapacityReservationId")?;
    validate_max_results(&req.query_params, 1, 1000)?;
    let accounts = svc.state.read();
    let r = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.capacity_reservations.get(&id).cloned());
    let (itype, total, avail) = r
        .map(|r| {
            (
                r.instance_type,
                r.total_instance_count,
                r.available_instance_count,
            )
        })
        .unwrap_or_else(|| ("t3.micro".to_string(), 1, 1));
    let body = format!(
        "{}{}<totalInstanceCount>{}</totalInstanceCount><availableInstanceCount>{}</availableInstanceCount>{}{}",
        ec2_elem("capacityReservationId", &id),
        ec2_elem("instanceType", &itype),
        total,
        avail,
        ec2_elem("state", "active"),
        ec2_list("instanceUsageSet", &[]),
    );
    Ok(Ec2Service::respond(
        "GetCapacityReservationUsage",
        &req.request_id,
        &body,
    ))
}

// ---- capacity reservation fleets ----

pub(crate) fn create_capacity_reservation_fleet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TotalTargetCapacity")?;
    validate_enum(&req.query_params, "Tenancy", &["default"])?;
    validate_enum(&req.query_params, "InstanceMatchCriteria", &["open"])?;
    let id = gen_id("crf");
    let cap = req
        .query_params
        .get("TotalTargetCapacity")
        .cloned()
        .unwrap_or_else(|| "1".to_string());
    {
        let mut accounts = svc.state.write();
        // The map value stores the fleet's TotalTargetCapacity so Describe /
        // Modify can round-trip it rather than reporting a hardcoded 1.
        accounts
            .regional_mut(&req.account_id, &req.region)
            .capacity_reservation_fleets
            .insert(id.clone(), cap.clone());
    }
    let body = format!(
        "{}{}<totalTargetCapacity>{}</totalTargetCapacity><totalFulfilledCapacity>{}</totalFulfilledCapacity>{}{}{}{}",
        ec2_elem("capacityReservationFleetId", &id),
        ec2_elem("state", "active"),
        cap, cap,
        ec2_elem("instanceMatchCriteria", "open"),
        ec2_elem("allocationStrategy", "prioritized"),
        ec2_elem("tenancy", "default"),
        ec2_elem("createTime", FIXED_TIME),
    );
    Ok(Ec2Service::respond(
        "CreateCapacityReservationFleet",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_capacity_reservation_fleets(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 100)?;
    let wanted = indexed_list(&req.query_params, "CapacityReservationFleetId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let mut items: Vec<String> = state
        .capacity_reservation_fleets
        .iter()
        .filter(|(id, _)| wanted.is_empty() || wanted.contains(id))
        .map(|(id, cap)| {
            format!(
                "{}{}<totalTargetCapacity>{}</totalTargetCapacity>",
                ec2_elem("capacityReservationFleetId", id),
                ec2_elem("state", "active"),
                cap,
            )
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeCapacityReservationFleets",
        &req.request_id,
        &ec2_list("capacityReservationFleetSet", &items),
    ))
}

pub(crate) fn cancel_capacity_reservation_fleets(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "CapacityReservationFleetId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        for id in &ids {
            state.capacity_reservation_fleets.remove(id);
        }
    }
    let items: Vec<String> = ids
        .iter()
        .map(|id| format!("{}<currentFleetState>cancelled</currentFleetState><previousFleetState>active</previousFleetState>", ec2_elem("capacityReservationFleetId", id)))
        .collect();
    let body = format!(
        "{}{}",
        ec2_list("successfulFleetCancellationSet", &items),
        ec2_list("failedFleetCancellationSet", &[])
    );
    Ok(Ec2Service::respond(
        "CancelCapacityReservationFleets",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_capacity_reservation_fleet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CapacityReservationFleetId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let cap = state
            .capacity_reservation_fleets
            .get_mut(&id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "InvalidCapacityReservationFleetId.NotFound",
                    format!("The capacity reservation fleet ID '{id}' does not exist"),
                )
            })?;
        if let Some(tc) = req.query_params.get("TotalTargetCapacity") {
            *cap = tc.clone();
        }
    }
    Ok(Ec2Service::respond(
        "ModifyCapacityReservationFleet",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn modify_instance_capacity_reservation_attributes(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    Ok(Ec2Service::respond(
        "ModifyInstanceCapacityReservationAttributes",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- split / move ----
//
// `CreateCapacityReservationBySplitting` carves a new reservation out of an
// existing one; `MoveCapacityReservationInstances` shifts capacity between two
// existing reservations. Both follow the EC2 User Guide ("Split off capacity
// from an existing Capacity Reservation" / "Move capacity between Capacity
// Reservations") and the API reference:
//
// - the source (and, for a move, the destination) must be `active` and owned
//   by the caller -- reservations are account-scoped here, so another
//   account's id is simply `InvalidCapacityReservationId.NotFound`;
// - Capacity Blocks can't be split or moved, nor can a future-dated
//   reservation during its commitment period;
// - available capacity is taken first, then used capacity, and used capacity
//   only moves for `open` reservations (instances launched `open` are the
//   only eligible ones);
// - a split may take at most the source's size minus one instance, while a
//   move may take all of it, which cancels the source;
// - a move needs the same instance type, platform, Availability Zone,
//   tenancy and end time on both sides.

/// `UnsupportedOperation` -- the reservation kind does not support the action.
fn unsupported_operation(message: String) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "UnsupportedOperation",
        message,
    )
}

/// `DryRunOperation`: the request would have succeeded without `DryRun`.
fn dry_run_succeeded(req: &AwsRequest) -> Result<(), AwsServiceError> {
    if req
        .query_params
        .get("DryRun")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::PRECONDITION_FAILED,
            "DryRunOperation",
            "Request would have succeeded, but DryRun flag is set.",
        ));
    }
    Ok(())
}

/// The required `InstanceCount` of a split / move: an integer of at least 1.
fn transfer_count(req: &AwsRequest) -> Result<i64, AwsServiceError> {
    let raw = require(&req.query_params, "InstanceCount")?;
    let count: i64 = raw
        .parse()
        .map_err(|_| invalid_parameter_value(format!("Invalid value '{raw}' for InstanceCount")))?;
    if count < 1 {
        return Err(invalid_parameter_value(format!(
            "InstanceCount must be at least 1, got {count}"
        )));
    }
    Ok(count)
}

/// Check that `r` can give up `count` instances, returning how many of them
/// come out of its available (unused) capacity; the rest is used capacity.
/// `max` is the most the action may take (size - 1 for a split, the whole
/// reservation for a move).
fn take_capacity(
    r: &CapacityReservation,
    role: &str,
    count: i64,
    max: i64,
    now: DateTime<Utc>,
) -> Result<i64, AwsServiceError> {
    check_transferable(r, role, now)?;
    if count > max {
        return Err(invalid_parameter_value(format!(
            "InstanceCount {count} exceeds the maximum of {max} instances that can be taken from Capacity Reservation {}",
            r.id
        )));
    }
    let from_available = count.min(r.available_instance_count.max(0));
    if count > from_available && r.instance_match_criteria != "open" {
        return Err(invalid_parameter_value(format!(
            "Capacity Reservation {} has only {} available instances; used capacity can only be taken from a reservation with open instance eligibility",
            r.id, r.available_instance_count
        )));
    }
    Ok(from_available)
}

/// State checks shared by both sides of a split / move.
fn check_transferable(
    r: &CapacityReservation,
    role: &str,
    now: DateTime<Utc>,
) -> Result<(), AwsServiceError> {
    if r.reservation_type.as_deref() == Some("capacity-block") {
        return Err(unsupported_operation(format!(
            "The {role} Capacity Reservation {} is a Capacity Block; splitting or moving capacity of a Capacity Block is not supported",
            r.id
        )));
    }
    let state = current_state(r, now);
    if state != "active" {
        return Err(incorrect_state(format!(
            "The {role} Capacity Reservation {} is in state '{state}'; it must be 'active'",
            r.id
        )));
    }
    if r.commitment
        .as_ref()
        .and_then(|c| parse_ts(&c.commitment_end_date))
        .is_some_and(|end| end > now)
    {
        return Err(incorrect_state(format!(
            "The {role} Capacity Reservation {} is within its commitment period",
            r.id
        )));
    }
    Ok(())
}

/// The `ClientToken` of a split / move, if the caller sent one.
fn client_token(req: &AwsRequest) -> Option<String> {
    req.query_params
        .get("ClientToken")
        .filter(|v| !v.is_empty())
        .cloned()
}

/// Replay of a split / move under an already-spent `ClientToken`: the same
/// request returns the original result (rendered as the reservations stand
/// now); a different one is `IdempotentParameterMismatch`.
fn replay_transfer(
    state: &Ec2State,
    token: Option<&str>,
    action: &str,
    source: &str,
    destination: Option<&str>,
    count: i64,
) -> Result<Option<crate::state::CapacityReservationTokenUse>, AwsServiceError> {
    let Some(used) = token.and_then(|t| state.capacity_reservation_client_tokens.get(t)) else {
        return Ok(None);
    };
    let same = used.action == action
        && used.source_capacity_reservation_id == source
        && destination.is_none_or(|d| used.destination_capacity_reservation_id == d)
        && used.instance_count == count;
    if !same {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "IdempotentParameterMismatch",
            "The client token has already been used with different parameters",
        ));
    }
    Ok(Some(used.clone()))
}

/// `sourceCapacityReservation` + `destinationCapacityReservation` +
/// `instanceCount`, the result of both a split and a move.
fn transfer_result_xml(
    state: &Ec2State,
    owner: &str,
    used: &crate::state::CapacityReservationTokenUse,
) -> String {
    let render = |id: &str| {
        state
            .capacity_reservations
            .get(id)
            .map(|r| cr_xml(r, state.tags_for(id), owner))
            .unwrap_or_default()
    };
    format!(
        "<sourceCapacityReservation>{}</sourceCapacityReservation><destinationCapacityReservation>{}</destinationCapacityReservation><instanceCount>{}</instanceCount>",
        render(&used.source_capacity_reservation_id),
        render(&used.destination_capacity_reservation_id),
        used.instance_count,
    )
}

pub(crate) fn create_capacity_reservation_by_splitting(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    const ACTION: &str = "CreateCapacityReservationBySplitting";
    // EC2 answers a dry run from the permission check, before it looks at
    // the request's parameters or the reservations' state.
    dry_run_succeeded(req)?;
    let src_id = require(&req.query_params, "SourceCapacityReservationId")?;
    let count = transfer_count(req)?;
    let token = client_token(req);
    let owner = req.account_id.clone();
    let now = Utc::now();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    if let Some(used) = replay_transfer(state, token.as_deref(), ACTION, &src_id, None, count)? {
        let body = transfer_result_xml(state, &owner, &used);
        return Ok(Ec2Service::respond(ACTION, &req.request_id, &body));
    }
    let source = state
        .capacity_reservations
        .get(&src_id)
        .ok_or_else(|| cr_not_found(&src_id))?;
    let from_available = take_capacity(
        source,
        "source",
        count,
        source.total_instance_count - 1,
        now,
    )?;
    // The new reservation has the source's configuration but none of its
    // tags, history or commitment: it is a fresh, immediately active
    // On-Demand Capacity Reservation holding the split-off capacity.
    let dest_id = gen_id("cr");
    let dest = CapacityReservation {
        id: dest_id.clone(),
        instance_type: source.instance_type.clone(),
        instance_platform: source.instance_platform.clone(),
        availability_zone: source.availability_zone.clone(),
        tenancy: source.tenancy.clone(),
        total_instance_count: count,
        available_instance_count: from_available,
        state: "active".to_string(),
        end_date_type: source.end_date_type.clone(),
        end_date: source.end_date.clone(),
        instance_match_criteria: source.instance_match_criteria.clone(),
        ebs_optimized: source.ebs_optimized,
        ephemeral_storage: source.ephemeral_storage,
        create_date: Some(fmt_ts(now)),
        start_date: Some(fmt_ts(now)),
        ..Default::default()
    };
    let source = state
        .capacity_reservations
        .get_mut(&src_id)
        .expect("source looked up above");
    source.total_instance_count -= count;
    source.available_instance_count -= from_available;
    state.capacity_reservations.insert(dest_id.clone(), dest);
    crate::service::tags::apply_tag_specifications(
        state,
        &req.query_params,
        &dest_id,
        "capacity-reservation",
    );
    let used = crate::state::CapacityReservationTokenUse {
        action: ACTION.to_string(),
        source_capacity_reservation_id: src_id,
        destination_capacity_reservation_id: dest_id,
        instance_count: count,
    };
    let body = transfer_result_xml(state, &owner, &used);
    if let Some(t) = token {
        state.capacity_reservation_client_tokens.insert(t, used);
    }
    Ok(Ec2Service::respond(ACTION, &req.request_id, &body))
}

pub(crate) fn move_capacity_reservation_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    const ACTION: &str = "MoveCapacityReservationInstances";
    dry_run_succeeded(req)?;
    let src_id = require(&req.query_params, "SourceCapacityReservationId")?;
    let dest_id = require(&req.query_params, "DestinationCapacityReservationId")?;
    let count = transfer_count(req)?;
    let token = client_token(req);
    let owner = req.account_id.clone();
    let now = Utc::now();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    if let Some(used) = replay_transfer(
        state,
        token.as_deref(),
        ACTION,
        &src_id,
        Some(&dest_id),
        count,
    )? {
        let body = transfer_result_xml(state, &owner, &used);
        return Ok(Ec2Service::respond(ACTION, &req.request_id, &body));
    }
    if src_id == dest_id {
        return Err(invalid_parameter_value(
            "The source and destination Capacity Reservations must be different".to_string(),
        ));
    }
    let source = state
        .capacity_reservations
        .get(&src_id)
        .ok_or_else(|| cr_not_found(&src_id))?;
    let dest = state
        .capacity_reservations
        .get(&dest_id)
        .ok_or_else(|| cr_not_found(&dest_id))?;
    let from_available = take_capacity(source, "source", count, source.total_instance_count, now)?;
    check_transferable(dest, "destination", now)?;
    let mismatch = [
        ("instance type", source.instance_type != dest.instance_type),
        (
            "platform",
            source.instance_platform != dest.instance_platform,
        ),
        (
            "Availability Zone",
            source.availability_zone != dest.availability_zone,
        ),
        ("tenancy", source.tenancy != dest.tenancy),
        (
            "end time",
            source.end_date_type != dest.end_date_type || source.end_date != dest.end_date,
        ),
    ]
    .into_iter()
    .find_map(|(what, differs)| differs.then_some(what));
    if let Some(what) = mismatch {
        return Err(invalid_parameter_value(format!(
            "Capacity Reservations {src_id} and {dest_id} must have the same {what}"
        )));
    }
    if count > from_available && dest.instance_match_criteria != "open" {
        return Err(invalid_parameter_value(format!(
            "Used capacity can only be moved into a Capacity Reservation with open instance eligibility, and {dest_id} is targeted"
        )));
    }
    let source = state
        .capacity_reservations
        .get_mut(&src_id)
        .expect("source looked up above");
    source.total_instance_count -= count;
    source.available_instance_count -= from_available;
    // Moving all of a reservation's capacity cancels it.
    if source.total_instance_count == 0 {
        source.state = "cancelled".to_string();
    }
    let dest = state
        .capacity_reservations
        .get_mut(&dest_id)
        .expect("destination looked up above");
    dest.total_instance_count += count;
    dest.available_instance_count += from_available;
    let used = crate::state::CapacityReservationTokenUse {
        action: ACTION.to_string(),
        source_capacity_reservation_id: src_id,
        destination_capacity_reservation_id: dest_id,
        instance_count: count,
    };
    let body = transfer_result_xml(state, &owner, &used);
    if let Some(t) = token {
        state.capacity_reservation_client_tokens.insert(t, used);
    }
    Ok(Ec2Service::respond(ACTION, &req.request_id, &body))
}

// ---- billing owner transfer ----

pub(crate) fn describe_capacity_reservation_billing_requests(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Role")?;
    validate_enum(
        &req.query_params,
        "Role",
        &["odcr-owner", "unused-reservation-billing-owner"],
    )?;
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeCapacityReservationBillingRequests",
        &req.request_id,
        &ec2_list("capacityReservationBillingRequestSet", &[]),
    ))
}

fn cr_ack(req: &AwsRequest, action: &str, extra: &[&str]) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CapacityReservationId")?;
    for k in extra {
        require(&req.query_params, k)?;
    }
    // UnusedReservationBillingOwnerId is a 12-digit account id when present.
    validate_length(&req.query_params, "UnusedReservationBillingOwnerId", 12, 12)?;
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn associate_capacity_reservation_billing_owner(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    cr_ack(
        req,
        "AssociateCapacityReservationBillingOwner",
        &["UnusedReservationBillingOwnerId"],
    )
}
pub(crate) fn disassociate_capacity_reservation_billing_owner(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    cr_ack(
        req,
        "DisassociateCapacityReservationBillingOwner",
        &["UnusedReservationBillingOwnerId"],
    )
}
pub(crate) fn accept_capacity_reservation_billing_ownership(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    cr_ack(req, "AcceptCapacityReservationBillingOwnership", &[])
}
pub(crate) fn reject_capacity_reservation_billing_ownership(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    cr_ack(req, "RejectCapacityReservationBillingOwnership", &[])
}

// ---- capacity blocks ----

/// Instance types sold as Capacity Blocks (EC2 User Guide, "Capacity Blocks
/// for ML": supported instance types).
const CAPACITY_BLOCK_INSTANCE_TYPES: &[&str] = &[
    "p4d.24xlarge",
    "p4de.24xlarge",
    "p5.4xlarge",
    "p5.48xlarge",
    "p5e.48xlarge",
    "p5en.48xlarge",
    "p6-b200.48xlarge",
    "trn1.32xlarge",
    "trn2.48xlarge",
];

/// Capacity Blocks start at 11:30 UTC.
const CAPACITY_BLOCK_START_HOUR: u32 = 11;
const CAPACITY_BLOCK_START_MINUTE: u32 = 30;
/// Capacity Blocks can be reserved up to eight weeks in advance.
const CAPACITY_BLOCK_MAX_ADVANCE_DAYS: i64 = 56;

/// A valid Capacity Block duration: 1-14 days in 1-day increments, or up to
/// 182 days in 7-day increments.
fn valid_capacity_block_hours(h: i64) -> bool {
    (h % 24 == 0 && (1..=14).contains(&(h / 24))) || (h % 168 == 0 && (1..=26).contains(&(h / 168)))
}

/// The first Capacity Block start (11:30 UTC) at or after `t`.
fn next_block_start(t: DateTime<Utc>) -> DateTime<Utc> {
    let day = t.date_naive();
    let at = |d: chrono::NaiveDate| {
        d.and_hms_opt(CAPACITY_BLOCK_START_HOUR, CAPACITY_BLOCK_START_MINUTE, 0)
            .expect("valid time")
            .and_utc()
    };
    let start = at(day);
    if start >= t {
        start
    } else {
        at(day + Duration::days(1))
    }
}

fn offering_xml(o: &crate::state::CapacityBlockOffering) -> String {
    format!(
        "<item>{}{}{}<instanceCount>{}</instanceCount>{}{}<capacityBlockDurationHours>{}</capacityBlockDurationHours>{}<capacityBlockDurationMinutes>{}</capacityBlockDurationMinutes></item>",
        ec2_elem("capacityBlockOfferingId", &o.id),
        ec2_elem("instanceType", &o.instance_type),
        ec2_elem("availabilityZone", &o.availability_zone),
        o.instance_count,
        ec2_elem("startDate", &o.start_date),
        ec2_elem("endDate", &o.end_date),
        o.duration_hours,
        ec2_elem("tenancy", "default"),
        o.duration_hours * 60,
    )
}

/// `DescribeCapacityBlockOfferings`: one offering per supported instance
/// type (or the requested one) at the earliest 11:30 UTC start within the
/// requested date range and the eight-week booking window, in the request
/// Region's first Availability Zone. Each offering is remembered so
/// `PurchaseCapacityBlock` buys exactly what was offered. UltraServer
/// offerings are not available.
pub(crate) fn describe_capacity_block_offerings(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    dry_run_succeeded(req)?;
    let raw = require(&req.query_params, "CapacityDurationHours")?;
    validate_max_results(&req.query_params, 1, 1000)?;
    let hours: i64 = raw
        .parse()
        .ok()
        .filter(|h| valid_capacity_block_hours(*h))
        .ok_or_else(|| {
            invalid_parameter_value(format!(
                "CapacityDurationHours {raw} is not a valid Capacity Block duration: 1-14 days in 1-day increments, or up to 182 days in 7-day increments"
            ))
        })?;
    let count: i64 = match req.query_params.get("InstanceCount") {
        None => 1,
        Some(v) => v
            .parse()
            .ok()
            .filter(|n| (1..=64).contains(n))
            .ok_or_else(|| {
                invalid_parameter_value(format!("Invalid value '{v}' for InstanceCount"))
            })?,
    };
    let now = Utc::now();
    let range_start = ts_param(req, "StartDateRange")?;
    let range_end = ts_param(req, "EndDateRange")?;
    if let (Some(s), Some(e)) = (range_start, range_end) {
        if e <= s {
            return Err(invalid_parameter_value(
                "EndDateRange must be later than StartDateRange".to_string(),
            ));
        }
    }
    let start = next_block_start(range_start.map_or(now, |s| s.max(now)));
    let end = start + Duration::hours(hours);
    let available = start <= now + Duration::days(CAPACITY_BLOCK_MAX_ADVANCE_DAYS)
        && range_end.is_none_or(|e| end <= e)
        && !req.query_params.contains_key("UltraserverType");
    let types: Vec<&str> = match req.query_params.get("InstanceType") {
        Some(t) => CAPACITY_BLOCK_INSTANCE_TYPES
            .iter()
            .copied()
            .filter(|c| c == t)
            .collect(),
        None => CAPACITY_BLOCK_INSTANCE_TYPES.to_vec(),
    };
    let region = if req.region.is_empty() {
        "us-east-1"
    } else {
        req.region.as_str()
    };
    let az = format!("{region}a");
    let mut items = Vec::new();
    if available {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        for t in types {
            let mut offering = crate::state::CapacityBlockOffering {
                id: String::new(),
                instance_type: t.to_string(),
                availability_zone: az.clone(),
                instance_count: count,
                start_date: fmt_ts(start),
                end_date: fmt_ts(end),
                duration_hours: hours,
            };
            // The same offering described again keeps its id.
            let existing = state
                .capacity_block_offerings
                .values()
                .find(|o| {
                    crate::state::CapacityBlockOffering {
                        id: String::new(),
                        ..(*o).clone()
                    } == offering
                })
                .map(|o| o.id.clone());
            offering.id = existing.unwrap_or_else(|| gen_id("cb"));
            items.push(offering_xml(&offering));
            state
                .capacity_block_offerings
                .insert(offering.id.clone(), offering);
        }
    }
    Ok(Ec2Service::respond(
        "DescribeCapacityBlockOfferings",
        &req.request_id,
        &format!(
            "<capacityBlockOfferingSet>{}</capacityBlockOfferingSet>",
            items.concat()
        ),
    ))
}

pub(crate) fn describe_capacity_blocks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeCapacityBlocks",
        &req.request_id,
        &ec2_list("capacityBlockSet", &[]),
    ))
}

/// `PurchaseCapacityBlock`: buy an offering `DescribeCapacityBlockOfferings`
/// returned. The Capacity Block is a `capacity-block` reservation with the
/// offering's instance type, count, Availability Zone and dates, `scheduled`
/// until its start. The offering is consumed by the purchase.
pub(crate) fn purchase_capacity_block(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    dry_run_succeeded(req)?;
    let offering_id = require(&req.query_params, "CapacityBlockOfferingId")?;
    let platform = require(&req.query_params, "InstancePlatform")?;
    validate_enum(&req.query_params, "InstancePlatform", INSTANCE_PLATFORMS)?;
    let owner = req.account_id.clone();
    let now = Utc::now();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let offering = state
        .capacity_block_offerings
        .remove(&offering_id)
        .ok_or_else(|| {
            invalid_parameter_value(format!(
                "The Capacity Block offering '{offering_id}' does not exist or is no longer available"
            ))
        })?;
    let scheduled = parse_ts(&offering.start_date).is_some_and(|s| s > now);
    let id = gen_id("cr");
    state.capacity_reservations.insert(
        id.clone(),
        CapacityReservation {
            id: id.clone(),
            instance_type: offering.instance_type,
            instance_platform: platform,
            availability_zone: offering.availability_zone,
            tenancy: "default".to_string(),
            total_instance_count: offering.instance_count,
            available_instance_count: offering.instance_count,
            state: if scheduled { "scheduled" } else { "active" }.to_string(),
            end_date_type: "limited".to_string(),
            instance_match_criteria: "targeted".to_string(),
            reservation_type: Some("capacity-block".to_string()),
            create_date: Some(fmt_ts(now)),
            start_date: Some(offering.start_date),
            end_date: Some(offering.end_date),
            ..Default::default()
        },
    );
    crate::service::tags::apply_tag_specifications(
        state,
        &req.query_params,
        &id,
        "capacity-reservation",
    );
    // `capacityBlockSet` lists UltraServer Capacity Blocks; an instance
    // Capacity Block purchase has none.
    let body = format!(
        "<capacityReservation>{}</capacityReservation>{}",
        cr_xml(
            &state.capacity_reservations[&id],
            state.tags_for(&id),
            &owner
        ),
        ec2_list("capacityBlockSet", &[])
    );
    Ok(Ec2Service::respond(
        "PurchaseCapacityBlock",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_capacity_block_status(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeCapacityBlockStatus",
        &req.request_id,
        &ec2_list("capacityBlockStatusSet", &[]),
    ))
}

pub(crate) fn describe_capacity_block_extension_history(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeCapacityBlockExtensionHistory",
        &req.request_id,
        &ec2_list("capacityBlockExtensionSet", &[]),
    ))
}

pub(crate) fn describe_capacity_block_extension_offerings(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CapacityBlockExtensionDurationHours")?;
    require(&req.query_params, "CapacityReservationId")?;
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeCapacityBlockExtensionOfferings",
        &req.request_id,
        &ec2_list("capacityBlockExtensionOfferingSet", &[]),
    ))
}

pub(crate) fn purchase_capacity_block_extension(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CapacityBlockExtensionOfferingId")?;
    require(&req.query_params, "CapacityReservationId")?;
    Ok(Ec2Service::respond(
        "PurchaseCapacityBlockExtension",
        &req.request_id,
        &ec2_list("capacityBlockExtensionSet", &[]),
    ))
}

// ---- cancellation quotes ----

pub(crate) fn describe_capacity_reservation_topology(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 10)?;
    let _ = svc;
    Ok(Ec2Service::respond(
        "DescribeCapacityReservationTopology",
        &req.request_id,
        &ec2_list("capacityReservationSet", &[]),
    ))
}

// ---- interruptible allocations ----

const ZERO_SIZE_PREFERENCES: &[&str] = &["retain", "default"];

/// Parse a required non-negative instance count.
fn instance_count_param(req: &AwsRequest, key: &str) -> Result<i64, AwsServiceError> {
    let raw = require(&req.query_params, key)?;
    raw.parse::<i64>()
        .ok()
        .filter(|n| *n >= 0)
        .ok_or_else(|| invalid_parameter_value(format!("Invalid value '{raw}' for {key}")))
}

fn interruptible_result_xml(source_id: &str, a: &InterruptibleAllocation) -> String {
    format!(
        "{}{}<instanceCount>{}</instanceCount><targetInstanceCount>{}</targetInstanceCount>{}{}",
        ec2_elem(
            "interruptibleCapacityReservationId",
            &a.interruptible_reservation_id
        ),
        ec2_elem("sourceCapacityReservationId", source_id),
        a.instance_count,
        a.target_instance_count,
        ec2_elem("status", &a.status),
        ec2_elem("interruptionType", INTERRUPTION_TYPE),
    )
}

/// The only `InterruptionType` EC2 defines: the source owner reclaims ad hoc.
const INTERRUPTION_TYPE: &str = "adhoc";

/// Carve `InstanceCount` unused instances out of an active source reservation
/// into a new interruptible reservation. The source gives up that capacity
/// (both total and available) and records the allocation; the interruptible
/// reservation points back at its source through `InterruptionInfo`.
pub(crate) fn create_interruptible_capacity_reservation_allocation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let source_id = require(&req.query_params, "CapacityReservationId")?;
    let count = instance_count_param(req, "InstanceCount")?;
    validate_enum(
        &req.query_params,
        "ZeroSizePreference",
        ZERO_SIZE_PREFERENCES,
    )?;
    let zero_pref = req
        .query_params
        .get("ZeroSizePreference")
        .cloned()
        .unwrap_or_else(|| "default".to_string());
    if count < 1 {
        return Err(invalid_parameter_value(
            "InstanceCount must be at least 1".to_string(),
        ));
    }
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let source = state
        .capacity_reservations
        .get(&source_id)
        .ok_or_else(|| cr_not_found(&source_id))?;
    if source.interruptible {
        return Err(invalid_parameter_value(format!(
            "Capacity Reservation {source_id} is itself interruptible and cannot be a source"
        )));
    }
    if current_state(source, Utc::now()) != "active" || source.end_date.is_some() {
        return Err(incorrect_state(format!(
            "Capacity Reservation {source_id} must be active with no end date to allocate interruptible capacity"
        )));
    }
    if source
        .interruptible_allocation
        .as_ref()
        .is_some_and(|a| a.status != "canceled")
    {
        return Err(incorrect_state(format!(
            "Capacity Reservation {source_id} already has an interruptible allocation"
        )));
    }
    if count > source.available_instance_count {
        return Err(invalid_parameter_value(format!(
            "InstanceCount {count} exceeds the {} available instances in Capacity Reservation {source_id}",
            source.available_instance_count
        )));
    }
    let id = gen_id("cr");
    let interruptible = CapacityReservation {
        id: id.clone(),
        instance_type: source.instance_type.clone(),
        instance_platform: source.instance_platform.clone(),
        availability_zone: source.availability_zone.clone(),
        tenancy: source.tenancy.clone(),
        total_instance_count: count,
        available_instance_count: count,
        state: "active".to_string(),
        end_date_type: "unlimited".to_string(),
        instance_match_criteria: source.instance_match_criteria.clone(),
        ebs_optimized: source.ebs_optimized,
        ephemeral_storage: source.ephemeral_storage,
        interruptible: true,
        interruption_source_id: Some(source_id.clone()),
        zero_size_preference: Some(zero_pref.clone()),
        ..Default::default()
    };
    let allocation = InterruptibleAllocation {
        interruptible_reservation_id: id.clone(),
        instance_count: count,
        target_instance_count: count,
        status: "active".to_string(),
        zero_size_preference: zero_pref,
    };
    let source = state
        .capacity_reservations
        .get_mut(&source_id)
        .expect("source looked up above");
    source.total_instance_count -= count;
    source.available_instance_count -= count;
    source.interruptible_allocation = Some(allocation.clone());
    crate::service::tags::apply_tag_specifications(
        state,
        &req.query_params,
        &id,
        "capacity-reservation",
    );
    state.capacity_reservations.insert(id, interruptible);
    let body = format!(
        "{}<targetInstanceCount>{}</targetInstanceCount>{}{}",
        ec2_elem("sourceCapacityReservationId", &source_id),
        allocation.target_instance_count,
        ec2_elem("status", &allocation.status),
        ec2_elem("interruptionType", INTERRUPTION_TYPE),
    );
    Ok(Ec2Service::respond(
        "CreateInterruptibleCapacityReservationAllocation",
        &req.request_id,
        &body,
    ))
}

/// Resize (or reconfigure) the interruptible allocation of a source
/// reservation. Growing takes more unused capacity from the source; shrinking
/// returns it. Shrinking to zero keeps the interruptible reservation at zero
/// capacity under `retain`, and cancels it under `default`.
pub(crate) fn update_interruptible_capacity_reservation_allocation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let source_id = require(&req.query_params, "CapacityReservationId")?;
    validate_enum(
        &req.query_params,
        "ZeroSizePreference",
        ZERO_SIZE_PREFERENCES,
    )?;
    let target = match req.query_params.get("TargetInstanceCount") {
        Some(_) => Some(instance_count_param(req, "TargetInstanceCount")?),
        None => None,
    };
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let source = state
        .capacity_reservations
        .get(&source_id)
        .ok_or_else(|| cr_not_found(&source_id))?;
    // Allocations are made from, and modified through, an active source
    // reservation with no end date.
    if current_state(source, Utc::now()) != "active" || source.end_date.is_some() {
        return Err(incorrect_state(format!(
            "Capacity Reservation {source_id} must be active with no end date to modify its interruptible allocation"
        )));
    }
    let mut allocation = source
        .interruptible_allocation
        .clone()
        .filter(|a| a.status != "canceled")
        .ok_or_else(|| {
            incorrect_state(format!(
                "Capacity Reservation {source_id} has no interruptible allocation"
            ))
        })?;
    if let Some(p) = req.query_params.get("ZeroSizePreference") {
        allocation.zero_size_preference = p.clone();
    }
    let current = allocation.instance_count;
    let target = target.unwrap_or(current);
    let delta = target - current;
    if delta > source.available_instance_count {
        return Err(invalid_parameter_value(format!(
            "TargetInstanceCount {target} exceeds the capacity available in Capacity Reservation {source_id}"
        )));
    }
    let cancel = target == 0 && allocation.zero_size_preference == "default";
    allocation.instance_count = target;
    allocation.target_instance_count = target;
    if cancel {
        allocation.status = "canceled".to_string();
    }
    let source = state
        .capacity_reservations
        .get_mut(&source_id)
        .expect("source looked up above");
    source.total_instance_count -= delta;
    source.available_instance_count -= delta;
    source.interruptible_allocation = Some(allocation.clone());
    if let Some(r) = state
        .capacity_reservations
        .get_mut(&allocation.interruptible_reservation_id)
    {
        // Resize the interruptible reservation. Growing adds available
        // capacity. Shrinking reclaims available instances first, then
        // instances in use, until the target is met (EC2 User Guide,
        // "Interruptible Capacity Reservations for capacity owners": "we
        // first reclaim available instances, then running instances");
        // the reclaimed in-use capacity is gone from the reservation.
        let in_use = (r.total_instance_count - r.available_instance_count).max(0);
        let in_use_after = in_use.min(target);
        r.total_instance_count = target;
        r.available_instance_count = target - in_use_after;
        r.zero_size_preference = Some(allocation.zero_size_preference.clone());
        if cancel {
            r.state = "cancelled".to_string();
        }
    }
    Ok(Ec2Service::respond(
        "UpdateInterruptibleCapacityReservationAllocation",
        &req.request_id,
        &interruptible_result_xml(&source_id, &allocation),
    ))
}

#[cfg(test)]
mod crfleet_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    fn new_cr(svc: &Ec2Service, count: &str) -> String {
        let created = body(
            create_capacity_reservation(
                svc,
                &req(
                    "CreateCapacityReservation",
                    &[
                        ("InstanceType", "t3.micro"),
                        ("InstancePlatform", "Linux/UNIX"),
                        ("AvailabilityZone", "us-east-1a"),
                        ("InstanceCount", count),
                    ],
                ),
            )
            .unwrap(),
        );
        created
            .split("<capacityReservationId>")
            .nth(1)
            .unwrap()
            .split("</capacityReservationId>")
            .next()
            .unwrap()
            .to_string()
    }

    fn cr(svc: &Ec2Service, id: &str) -> CapacityReservation {
        svc.state
            .read()
            .regional("000000000000", "us-east-1")
            .unwrap()
            .capacity_reservations[id]
            .clone()
    }

    #[test]
    fn interruptible_allocation_carves_capacity_from_the_source() {
        let svc = Ec2Service::new();
        let src = new_cr(&svc, "10");

        // Validation: unknown source, off-enum preference, over-allocation.
        let e = err_of(create_interruptible_capacity_reservation_allocation(
            &svc,
            &req(
                "CreateInterruptibleCapacityReservationAllocation",
                &[
                    ("CapacityReservationId", "cr-0000000000000000"),
                    ("InstanceCount", "1"),
                ],
            ),
        ));
        assert_eq!(e.code(), "InvalidCapacityReservationId.NotFound");
        let e = err_of(create_interruptible_capacity_reservation_allocation(
            &svc,
            &req(
                "CreateInterruptibleCapacityReservationAllocation",
                &[
                    ("CapacityReservationId", &src),
                    ("InstanceCount", "1"),
                    ("ZeroSizePreference", "keep"),
                ],
            ),
        ));
        assert_eq!(e.code(), "InvalidParameterValue");
        let e = err_of(create_interruptible_capacity_reservation_allocation(
            &svc,
            &req(
                "CreateInterruptibleCapacityReservationAllocation",
                &[("CapacityReservationId", &src), ("InstanceCount", "11")],
            ),
        ));
        assert_eq!(e.code(), "InvalidParameterValue");

        let out = body(
            create_interruptible_capacity_reservation_allocation(
                &svc,
                &req(
                    "CreateInterruptibleCapacityReservationAllocation",
                    &[
                        ("CapacityReservationId", &src),
                        ("InstanceCount", "4"),
                        ("ZeroSizePreference", "retain"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(
            out.contains("<targetInstanceCount>4</targetInstanceCount>"),
            "{out}"
        );
        assert!(
            out.contains("<interruptionType>adhoc</interruptionType>"),
            "{out}"
        );

        let source = cr(&svc, &src);
        assert_eq!(source.total_instance_count, 6);
        assert_eq!(source.available_instance_count, 6);
        let alloc = source.interruptible_allocation.clone().unwrap();
        assert_eq!(alloc.instance_count, 4);
        assert_eq!(alloc.zero_size_preference, "retain");
        let icr = cr(&svc, &alloc.interruptible_reservation_id);
        assert!(icr.interruptible);
        assert_eq!(icr.total_instance_count, 4);
        assert_eq!(icr.interruption_source_id.as_deref(), Some(src.as_str()));

        // A second allocation from the same source is refused.
        let e = err_of(create_interruptible_capacity_reservation_allocation(
            &svc,
            &req(
                "CreateInterruptibleCapacityReservationAllocation",
                &[("CapacityReservationId", &src), ("InstanceCount", "1")],
            ),
        ));
        assert_eq!(e.code(), "IncorrectState");

        // Describe exposes both sides of the allocation.
        let desc = body(
            describe_capacity_reservations(&svc, &req("DescribeCapacityReservations", &[]))
                .unwrap(),
        );
        assert!(desc.contains("<interruptibleCapacityAllocation>"), "{desc}");
        assert!(desc.contains("<interruptionInfo>"), "{desc}");
        assert!(
            desc.contains("<interruptible>true</interruptible>"),
            "{desc}"
        );

        // Grow by 2, then shrink to zero: `retain` keeps the reservation.
        update_interruptible_capacity_reservation_allocation(
            &svc,
            &req(
                "UpdateInterruptibleCapacityReservationAllocation",
                &[
                    ("CapacityReservationId", &src),
                    ("TargetInstanceCount", "6"),
                ],
            ),
        )
        .unwrap();
        assert_eq!(cr(&svc, &src).available_instance_count, 4);
        assert_eq!(
            cr(&svc, &alloc.interruptible_reservation_id).total_instance_count,
            6
        );
        let e = err_of(update_interruptible_capacity_reservation_allocation(
            &svc,
            &req(
                "UpdateInterruptibleCapacityReservationAllocation",
                &[
                    ("CapacityReservationId", &src),
                    ("TargetInstanceCount", "11"),
                ],
            ),
        ));
        assert_eq!(e.code(), "InvalidParameterValue");
        update_interruptible_capacity_reservation_allocation(
            &svc,
            &req(
                "UpdateInterruptibleCapacityReservationAllocation",
                &[
                    ("CapacityReservationId", &src),
                    ("TargetInstanceCount", "0"),
                ],
            ),
        )
        .unwrap();
        assert_eq!(cr(&svc, &src).available_instance_count, 10);
        assert_eq!(
            cr(&svc, &alloc.interruptible_reservation_id).state,
            "active"
        );

        // Switching to `default` at zero cancels the interruptible reservation.
        let out = body(
            update_interruptible_capacity_reservation_allocation(
                &svc,
                &req(
                    "UpdateInterruptibleCapacityReservationAllocation",
                    &[
                        ("CapacityReservationId", &src),
                        ("TargetInstanceCount", "0"),
                        ("ZeroSizePreference", "default"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(out.contains("<status>canceled</status>"), "{out}");
        assert_eq!(
            cr(&svc, &alloc.interruptible_reservation_id).state,
            "cancelled"
        );
    }

    fn allocate(svc: &Ec2Service, src: &str, count: &str) -> String {
        create_interruptible_capacity_reservation_allocation(
            svc,
            &req(
                "CreateInterruptibleCapacityReservationAllocation",
                &[("CapacityReservationId", src), ("InstanceCount", count)],
            ),
        )
        .unwrap();
        cr(svc, src)
            .interruptible_allocation
            .unwrap()
            .interruptible_reservation_id
    }

    fn update_alloc(
        svc: &Ec2Service,
        src: &str,
        target: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        update_interruptible_capacity_reservation_allocation(
            svc,
            &req(
                "UpdateInterruptibleCapacityReservationAllocation",
                &[
                    ("CapacityReservationId", src),
                    ("TargetInstanceCount", target),
                ],
            ),
        )
    }

    #[test]
    fn cancelling_the_interruptible_reservation_returns_capacity_to_the_source() {
        let svc = Ec2Service::new();
        let src = new_cr(&svc, "10");
        let icr = allocate(&svc, &src, "4");
        cancel_capacity_reservation(
            &svc,
            &req(
                "CancelCapacityReservation",
                &[("CapacityReservationId", &icr)],
            ),
        )
        .unwrap();
        assert_eq!(cr(&svc, &icr).state, "cancelled");
        let source = cr(&svc, &src);
        assert_eq!(source.total_instance_count, 10);
        assert_eq!(source.available_instance_count, 10);
        let a = source.interruptible_allocation.unwrap();
        assert_eq!(a.status, "canceled");
        assert_eq!(a.instance_count, 0);
        // The allocation is gone, so a new one can be made.
        allocate(&svc, &src, "2");
    }

    #[test]
    fn cancelling_the_source_cancels_its_interruptible_reservation() {
        let svc = Ec2Service::new();
        let src = new_cr(&svc, "10");
        let icr = allocate(&svc, &src, "4");
        cancel_capacity_reservation(
            &svc,
            &req(
                "CancelCapacityReservation",
                &[("CapacityReservationId", &src)],
            ),
        )
        .unwrap();
        assert_eq!(cr(&svc, &src).state, "cancelled");
        assert_eq!(cr(&svc, &icr).state, "cancelled");
        assert_eq!(
            cr(&svc, &src).interruptible_allocation.unwrap().status,
            "canceled"
        );
        // A cancelled source can no longer be modified.
        let e = err_of(update_alloc(&svc, &src, "2"));
        assert_eq!(e.code(), "IncorrectState");
    }

    #[test]
    fn update_requires_an_active_source_without_end_date() {
        let svc = Ec2Service::new();
        let src = new_cr(&svc, "10");
        allocate(&svc, &src, "4");
        {
            let mut accounts = svc.state.write();
            let r = accounts
                .regional_mut("000000000000", "us-east-1")
                .capacity_reservations
                .get_mut(&src)
                .unwrap();
            r.end_date = Some("2030-01-01T00:00:00.000Z".to_string());
        }
        let e = err_of(update_alloc(&svc, &src, "2"));
        assert_eq!(e.code(), "IncorrectState");
    }

    #[test]
    fn shrinking_reclaims_available_then_in_use_capacity() {
        let svc = Ec2Service::new();
        let src = new_cr(&svc, "10");
        let icr = allocate(&svc, &src, "6");
        // Three of the six interruptible instances are in use.
        {
            let mut accounts = svc.state.write();
            accounts
                .regional_mut("000000000000", "us-east-1")
                .capacity_reservations
                .get_mut(&icr)
                .unwrap()
                .available_instance_count = 3;
        }
        // 6 -> 4 reclaims two available instances only.
        update_alloc(&svc, &src, "4").unwrap();
        let r = cr(&svc, &icr);
        assert_eq!((r.total_instance_count, r.available_instance_count), (4, 1));
        // 4 -> 2 reclaims the last available one, then one in use.
        update_alloc(&svc, &src, "2").unwrap();
        let r = cr(&svc, &icr);
        assert_eq!((r.total_instance_count, r.available_instance_count), (2, 0));
        // The source got every reclaimed instance back exactly once.
        let s = cr(&svc, &src);
        assert_eq!((s.total_instance_count, s.available_instance_count), (8, 8));
        // Growing again adds available capacity only.
        update_alloc(&svc, &src, "5").unwrap();
        let r = cr(&svc, &icr);
        assert_eq!((r.total_instance_count, r.available_instance_count), (5, 3));
        let s = cr(&svc, &src);
        assert_eq!((s.total_instance_count, s.available_instance_count), (5, 5));
    }

    #[test]
    fn splitting_does_not_copy_interruptible_state() {
        let svc = Ec2Service::new();
        let src = new_cr(&svc, "10");
        let icr = allocate(&svc, &src, "4");
        for from in [&src, &icr] {
            let out = body(
                create_capacity_reservation_by_splitting(
                    &svc,
                    &req(
                        "CreateCapacityReservationBySplitting",
                        &[
                            ("SourceCapacityReservationId", from),
                            ("InstanceCount", "1"),
                        ],
                    ),
                )
                .unwrap(),
            );
            let dest = out
                .split("<destinationCapacityReservation><capacityReservationId>")
                .nth(1)
                .unwrap()
                .split('<')
                .next()
                .unwrap()
                .to_string();
            let d = cr(&svc, &dest);
            assert!(!d.interruptible);
            assert!(d.interruption_source_id.is_none());
            assert!(d.zero_size_preference.is_none());
            assert!(d.interruptible_allocation.is_none());
        }
    }

    #[test]
    fn create_capacity_reservation_persists_ebs_optimized_and_ephemeral() {
        // bug-audit 2026-07-29 (cycle 8) E2-3: CreateCapacityReservation dropped
        // EbsOptimized/EphemeralStorage; cr_xml hardcoded both false.
        let svc = Ec2Service::new();
        body(
            create_capacity_reservation(
                &svc,
                &req(
                    "CreateCapacityReservation",
                    &[
                        ("InstanceType", "t3.micro"),
                        ("InstancePlatform", "Linux/UNIX"),
                        ("AvailabilityZone", "us-east-1a"),
                        ("InstanceCount", "1"),
                        ("EbsOptimized", "true"),
                        ("EphemeralStorage", "true"),
                    ],
                ),
            )
            .unwrap(),
        );
        let desc = body(
            describe_capacity_reservations(&svc, &req("DescribeCapacityReservations", &[]))
                .unwrap(),
        );
        assert!(desc.contains("<ebsOptimized>true</ebsOptimized>"), "{desc}");
        assert!(
            desc.contains("<ephemeralStorage>true</ephemeralStorage>"),
            "{desc}"
        );
    }

    #[test]
    fn modify_capacity_reservation_persists_end_date_type_and_match() {
        // bug-audit 2026-07-28 (cycle 7) E3: ModifyCapacityReservation validated
        // EndDateType + InstanceMatchCriteria then persisted only InstanceCount,
        // so DescribeCapacityReservations read back the create-time values.
        let svc = Ec2Service::new();
        let created = body(
            create_capacity_reservation(
                &svc,
                &req(
                    "CreateCapacityReservation",
                    &[
                        ("InstanceType", "t3.micro"),
                        ("InstancePlatform", "Linux/UNIX"),
                        ("AvailabilityZone", "us-east-1a"),
                        ("InstanceCount", "4"),
                        ("InstanceMatchCriteria", "open"),
                    ],
                ),
            )
            .unwrap(),
        );
        let id = created
            .split("<capacityReservationId>")
            .nth(1)
            .unwrap()
            .split("</capacityReservationId>")
            .next()
            .unwrap()
            .to_string();
        modify_capacity_reservation(
            &svc,
            &req(
                "ModifyCapacityReservation",
                &[
                    ("CapacityReservationId", &id),
                    ("EndDateType", "unlimited"),
                    ("InstanceMatchCriteria", "targeted"),
                ],
            ),
        )
        .unwrap();
        let desc = body(
            describe_capacity_reservations(&svc, &req("DescribeCapacityReservations", &[]))
                .unwrap(),
        );
        assert!(
            desc.contains("<instanceMatchCriteria>targeted</instanceMatchCriteria>"),
            "match criteria not persisted: {desc}"
        );
        assert!(
            desc.contains("<endDateType>unlimited</endDateType>"),
            "end date type not persisted: {desc}"
        );
    }

    #[test]
    fn cr_fleet_total_target_capacity_round_trips() {
        let svc = Ec2Service::new();
        let resp = create_capacity_reservation_fleet(
            &svc,
            &req(
                "CreateCapacityReservationFleet",
                &[
                    ("TotalTargetCapacity", "10"),
                    ("Tenancy", "default"),
                    ("InstanceMatchCriteria", "open"),
                ],
            ),
        )
        .unwrap();
        let created = body(resp);
        let id = created
            .split("<capacityReservationFleetId>")
            .nth(1)
            .unwrap()
            .split("</capacityReservationFleetId>")
            .next()
            .unwrap()
            .to_string();
        let desc = body(
            describe_capacity_reservation_fleets(
                &svc,
                &req("DescribeCapacityReservationFleets", &[]),
            )
            .unwrap(),
        );
        assert!(
            desc.contains("<totalTargetCapacity>10</totalTargetCapacity>"),
            "{desc}"
        );

        modify_capacity_reservation_fleet(
            &svc,
            &req(
                "ModifyCapacityReservationFleet",
                &[
                    ("CapacityReservationFleetId", &id),
                    ("TotalTargetCapacity", "20"),
                ],
            ),
        )
        .unwrap();
        let desc2 = body(
            describe_capacity_reservation_fleets(
                &svc,
                &req("DescribeCapacityReservationFleets", &[]),
            )
            .unwrap(),
        );
        assert!(
            desc2.contains("<totalTargetCapacity>20</totalTargetCapacity>"),
            "{desc2}"
        );
    }

    #[test]
    fn modify_cr_fleet_missing_errors() {
        let svc = Ec2Service::new();
        let err = err_of(modify_capacity_reservation_fleet(
            &svc,
            &req(
                "ModifyCapacityReservationFleet",
                &[("CapacityReservationFleetId", "crf-nope")],
            ),
        ));
        assert_eq!(err.code(), "InvalidCapacityReservationFleetId.NotFound");
    }
}

#[cfg(test)]
mod date_change_quote_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    fn between<'a>(s: &'a str, tag: &str) -> &'a str {
        s.split(&format!("<{tag}>"))
            .nth(1)
            .and_then(|r| r.split(&format!("</{tag}>")).next())
            .unwrap_or_else(|| panic!("<{tag}> missing in {s}"))
    }

    /// A future-dated reservation starting in `days`, with a 1-day commitment.
    fn future_cr(svc: &Ec2Service, days: i64) -> (String, DateTime<Utc>) {
        let start = Utc::now() + Duration::days(days);
        let start_s = fmt_ts(start);
        let out = body(
            create_capacity_reservation(
                svc,
                &req(
                    "CreateCapacityReservation",
                    &[
                        ("InstanceType", "m5.large"),
                        ("InstancePlatform", "Linux/UNIX"),
                        ("InstanceCount", "4"),
                        ("StartDate", &start_s),
                        ("CommitmentDuration", "86400"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert_eq!(between(&out, "state"), "scheduled");
        assert_eq!(between(&out, "originalStartDate"), start_s);
        assert_eq!(between(&out, "commitmentDuration"), "86400");
        (between(&out, "capacityReservationId").to_string(), start)
    }

    fn quote(svc: &Ec2Service, cr: &str, new_start: DateTime<Utc>) -> String {
        let out = body(
            create_capacity_reservation_date_change_quote(
                svc,
                &req(
                    "CreateCapacityReservationDateChangeQuote",
                    &[
                        ("CapacityReservationId", cr),
                        ("NewStartDate", &fmt_ts(new_start)),
                        (
                            "TagSpecification.1.ResourceType",
                            "capacity-reservation-modification-quote",
                        ),
                        ("TagSpecification.1.Tag.1.Key", "team"),
                        ("TagSpecification.1.Tag.1.Value", "infra"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert_eq!(between(&out, "quoteState"), "active");
        assert_eq!(between(&out, "newStartDate"), fmt_ts(new_start));
        assert_eq!(
            between(&out, "newCommitmentEndDate"),
            fmt_ts(new_start + Duration::seconds(86400))
        );
        assert_eq!(between(&out, "reservationState"), "scheduled");
        assert!(out.contains("<key>team</key>"), "{out}");
        between(&out, "capacityReservationModificationQuoteId").to_string()
    }

    #[test]
    fn quote_then_modify_pushes_out_start_date() {
        let svc = Ec2Service::new();
        let (cr, start) = future_cr(&svc, 5);
        let new_start = start + Duration::days(3);
        let qid = quote(&svc, &cr, new_start);
        assert!(qid.starts_with("crmq-"));

        let out = body(
            modify_capacity_reservation(
                &svc,
                &req(
                    "ModifyCapacityReservation",
                    &[
                        ("CapacityReservationId", &cr),
                        ("QuoteId", &qid),
                        ("StartDate", &fmt_ts(new_start)),
                        ("AcceptModificationTerms", "true"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert_eq!(between(&out, "adjustmentStatus"), "applied");
        assert_eq!(between(&out, "startDate"), fmt_ts(new_start));
        assert_eq!(
            between(&out, "commitmentEndDate"),
            fmt_ts(new_start + Duration::seconds(86400))
        );

        let desc = body(
            describe_capacity_reservations(&svc, &req("DescribeCapacityReservations", &[]))
                .unwrap(),
        );
        assert_eq!(between(&desc, "startDate"), fmt_ts(new_start));
        // The original start date is the anchor; it doesn't move.
        assert_eq!(between(&desc, "originalStartDate"), fmt_ts(start));
        assert_eq!(between(&desc, "adjustmentStatus"), "applied");

        // A quote is single-use: it now reports expired and can't be reapplied.
        let quotes = body(
            describe_capacity_reservation_date_change_quotes(
                &svc,
                &req(
                    "DescribeCapacityReservationDateChangeQuotes",
                    &[("CapacityReservationModificationQuoteId.1", &qid)],
                ),
            )
            .unwrap(),
        );
        assert_eq!(between(&quotes, "quoteState"), "expired");
        let err = err_of(modify_capacity_reservation(
            &svc,
            &req(
                "ModifyCapacityReservation",
                &[
                    ("CapacityReservationId", &cr),
                    ("QuoteId", &qid),
                    ("AcceptModificationTerms", "true"),
                ],
            ),
        ));
        assert_eq!(err.code(), "IncorrectState");
    }

    #[test]
    fn applying_a_quote_spends_the_other_quotes_for_the_reservation() {
        let svc = Ec2Service::new();
        let (cr, start) = future_cr(&svc, 5);
        let early = quote(&svc, &cr, start + Duration::days(1));
        let late_start = start + Duration::days(3);
        let late = quote(&svc, &cr, late_start);
        // StartDate at microsecond precision (as boto3 sends it) still
        // matches the millisecond-precision quoted date.
        let micro = format!("{}Z", late_start.format("%Y-%m-%dT%H:%M:%S%.6f"));
        modify_capacity_reservation(
            &svc,
            &req(
                "ModifyCapacityReservation",
                &[
                    ("CapacityReservationId", &cr),
                    ("QuoteId", &late),
                    ("StartDate", &micro),
                    ("AcceptModificationTerms", "true"),
                ],
            ),
        )
        .unwrap();
        // The earlier quote can no longer pull the start date back.
        let err = err_of(modify_capacity_reservation(
            &svc,
            &req(
                "ModifyCapacityReservation",
                &[
                    ("CapacityReservationId", &cr),
                    ("QuoteId", &early),
                    ("AcceptModificationTerms", "true"),
                ],
            ),
        ));
        assert_eq!(err.code(), "IncorrectState");
        let desc = body(
            describe_capacity_reservations(&svc, &req("DescribeCapacityReservations", &[]))
                .unwrap(),
        );
        assert_eq!(between(&desc, "startDate"), fmt_ts(late_start));
    }

    #[test]
    fn quote_client_token_reuse_with_a_different_date_is_a_mismatch() {
        let svc = Ec2Service::new();
        let (cr, start) = future_cr(&svc, 5);
        let call = |days: i64| {
            create_capacity_reservation_date_change_quote(
                &svc,
                &req(
                    "CreateCapacityReservationDateChangeQuote",
                    &[
                        ("CapacityReservationId", &cr),
                        ("NewStartDate", &fmt_ts(start + Duration::days(days))),
                        ("ClientToken", "tok-1"),
                    ],
                ),
            )
        };
        let first = between(
            &body(call(1).unwrap()),
            "capacityReservationModificationQuoteId",
        )
        .to_string();
        let again = between(
            &body(call(1).unwrap()),
            "capacityReservationModificationQuoteId",
        )
        .to_string();
        assert_eq!(first, again);
        assert_eq!(err_of(call(2)).code(), "IdempotentParameterMismatch");
    }

    #[test]
    fn switching_to_unlimited_drops_the_end_date() {
        let svc = Ec2Service::new();
        let (cr, start) = future_cr(&svc, 5);
        let end = fmt_ts(start + Duration::days(30));
        let out = body(
            modify_capacity_reservation(
                &svc,
                &req(
                    "ModifyCapacityReservation",
                    &[
                        ("CapacityReservationId", &cr),
                        ("EndDateType", "limited"),
                        ("EndDate", &end),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(out.contains(&format!("<endDate>{end}</endDate>")), "{out}");
        modify_capacity_reservation(
            &svc,
            &req(
                "ModifyCapacityReservation",
                &[("CapacityReservationId", &cr), ("EndDateType", "unlimited")],
            ),
        )
        .unwrap();
        let desc = body(
            describe_capacity_reservations(&svc, &req("DescribeCapacityReservations", &[]))
                .unwrap(),
        );
        assert!(!desc.contains("<endDate>"), "{desc}");
        assert_eq!(between(&desc, "endDateType"), "unlimited");
    }

    #[test]
    fn quote_enforces_pushout_window_and_future_dating() {
        let svc = Ec2Service::new();
        let (cr, start) = future_cr(&svc, 5);
        let call = |new_start: DateTime<Utc>, id: &str| {
            err_of(create_capacity_reservation_date_change_quote(
                &svc,
                &req(
                    "CreateCapacityReservationDateChangeQuote",
                    &[
                        ("CapacityReservationId", id),
                        ("NewStartDate", &fmt_ts(new_start)),
                    ],
                ),
            ))
            .code()
            .to_string()
        };
        assert_eq!(
            call(start - Duration::hours(1), &cr),
            "InvalidParameterValue"
        );
        assert_eq!(
            call(start + Duration::days(31), &cr),
            "InvalidParameterValue"
        );
        assert_eq!(
            call(start, "cr-00000000000000000"),
            "InvalidCapacityReservationId.NotFound"
        );

        // An immediate (already delivered) reservation has no date to change.
        let now_cr = body(
            create_capacity_reservation(
                &svc,
                &req(
                    "CreateCapacityReservation",
                    &[
                        ("InstanceType", "m5.large"),
                        ("InstancePlatform", "Linux/UNIX"),
                        ("InstanceCount", "1"),
                    ],
                ),
            )
            .unwrap(),
        );
        let now_id = between(&now_cr, "capacityReservationId").to_string();
        assert_eq!(
            call(Utc::now() + Duration::days(1), &now_id),
            "IncorrectState"
        );
    }

    #[test]
    fn modify_requires_accepting_quote_terms() {
        let svc = Ec2Service::new();
        let (cr, start) = future_cr(&svc, 5);
        let qid = quote(&svc, &cr, start + Duration::days(1));
        let err = err_of(modify_capacity_reservation(
            &svc,
            &req(
                "ModifyCapacityReservation",
                &[("CapacityReservationId", &cr), ("QuoteId", &qid)],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
        let err = err_of(modify_capacity_reservation(
            &svc,
            &req(
                "ModifyCapacityReservation",
                &[
                    ("CapacityReservationId", &cr),
                    ("StartDate", &fmt_ts(start + Duration::days(1))),
                ],
            ),
        ));
        assert_eq!(err.code(), "MissingParameter");
        let err = err_of(modify_capacity_reservation(
            &svc,
            &req(
                "ModifyCapacityReservation",
                &[
                    ("CapacityReservationId", &cr),
                    ("QuoteId", &qid),
                    ("StartDate", &fmt_ts(start + Duration::days(2))),
                    ("AcceptModificationTerms", "true"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn describe_quotes_filters_paginates_and_rejects_unknown_ids() {
        let svc = Ec2Service::new();
        let (cr_a, start_a) = future_cr(&svc, 5);
        let (cr_b, start_b) = future_cr(&svc, 6);
        quote(&svc, &cr_a, start_a + Duration::days(1));
        quote(&svc, &cr_a, start_a + Duration::days(2));
        let qb = quote(&svc, &cr_b, start_b + Duration::days(1));

        let describe = |q: &[(&str, &str)]| {
            body(
                describe_capacity_reservation_date_change_quotes(
                    &svc,
                    &req("DescribeCapacityReservationDateChangeQuotes", q),
                )
                .unwrap(),
            )
        };
        let only_b = describe(&[
            ("Filter.1.Name", "capacity-reservation-id"),
            ("Filter.1.Value.1", &cr_b),
        ]);
        assert_eq!(
            only_b
                .matches("<capacityReservationModificationQuoteId>")
                .count(),
            1,
            "{only_b}"
        );
        assert!(only_b.contains(&qb));
        let tagged = describe(&[("Filter.1.Name", "tag:team"), ("Filter.1.Value.1", "infra")]);
        assert_eq!(
            tagged
                .matches("<capacityReservationModificationQuoteId>")
                .count(),
            3
        );

        let page1 = describe(&[("MaxResults", "2")]);
        assert_eq!(
            page1
                .matches("<capacityReservationModificationQuoteId>")
                .count(),
            2
        );
        let token = between(&page1, "nextToken").to_string();
        let page2 = describe(&[("MaxResults", "2"), ("NextToken", &token)]);
        assert_eq!(
            page2
                .matches("<capacityReservationModificationQuoteId>")
                .count(),
            1
        );
        assert!(!page2.contains("<nextToken>"));

        let err = err_of(describe_capacity_reservation_date_change_quotes(
            &svc,
            &req(
                "DescribeCapacityReservationDateChangeQuotes",
                &[(
                    "CapacityReservationModificationQuoteId.1",
                    "crmq-00000000000000000",
                )],
            ),
        ));
        assert_eq!(
            err.code(),
            "InvalidCapacityReservationModificationQuoteId.NotFound"
        );
    }
}

#[cfg(test)]
mod split_move_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    fn create(svc: &Ec2Service, extra: &[(&str, &str)]) -> String {
        let mut q = vec![
            ("InstanceType", "m5.large"),
            ("InstancePlatform", "Linux/UNIX"),
            ("AvailabilityZone", "us-east-1a"),
        ];
        q.extend_from_slice(extra);
        let out =
            body(create_capacity_reservation(svc, &req("CreateCapacityReservation", &q)).unwrap());
        between(&out, "capacityReservationId").to_string()
    }

    fn between<'a>(s: &'a str, tag: &str) -> &'a str {
        let open = format!("<{tag}>");
        let start = s.find(&open).unwrap() + open.len();
        let end = s[start..].find(&format!("</{tag}>")).unwrap();
        &s[start..start + end]
    }

    fn cr(svc: &Ec2Service, id: &str) -> CapacityReservation {
        svc.state
            .read()
            .regional("000000000000", "us-east-1")
            .unwrap()
            .capacity_reservations[id]
            .clone()
    }

    fn set_available(svc: &Ec2Service, id: &str, available: i64) {
        svc.state
            .write()
            .regional_mut("000000000000", "us-east-1")
            .capacity_reservations
            .get_mut(id)
            .unwrap()
            .available_instance_count = available;
    }

    fn offering(svc: &Ec2Service, instance_type: &str, count: &str) -> String {
        let out = body(
            describe_capacity_block_offerings(
                svc,
                &req(
                    "DescribeCapacityBlockOfferings",
                    &[
                        ("InstanceType", instance_type),
                        ("InstanceCount", count),
                        ("CapacityDurationHours", "48"),
                    ],
                ),
            )
            .unwrap(),
        );
        between(&out, "capacityBlockOfferingId").to_string()
    }

    fn buy_block(svc: &Ec2Service, instance_type: &str, count: &str) -> String {
        let id = offering(svc, instance_type, count);
        body(
            purchase_capacity_block(
                svc,
                &req(
                    "PurchaseCapacityBlock",
                    &[
                        ("CapacityBlockOfferingId", &id),
                        ("InstancePlatform", "Linux/UNIX"),
                    ],
                ),
            )
            .unwrap(),
        )
    }

    fn split(
        svc: &Ec2Service,
        src: &str,
        count: &str,
        extra: &[(&str, &str)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut q = vec![
            ("SourceCapacityReservationId", src),
            ("InstanceCount", count),
        ];
        q.extend_from_slice(extra);
        create_capacity_reservation_by_splitting(
            svc,
            &req("CreateCapacityReservationBySplitting", &q),
        )
    }

    fn mv(
        svc: &Ec2Service,
        src: &str,
        dest: &str,
        count: &str,
        extra: &[(&str, &str)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut q = vec![
            ("SourceCapacityReservationId", src),
            ("DestinationCapacityReservationId", dest),
            ("InstanceCount", count),
        ];
        q.extend_from_slice(extra);
        move_capacity_reservation_instances(svc, &req("MoveCapacityReservationInstances", &q))
    }

    #[test]
    fn split_moves_capacity_into_a_new_reservation() {
        let svc = Ec2Service::new();
        let src = create(
            &svc,
            &[
                ("InstanceCount", "5"),
                ("EbsOptimized", "true"),
                ("Tenancy", "dedicated"),
                ("TagSpecification.1.ResourceType", "capacity-reservation"),
                ("TagSpecification.1.Tag.1.Key", "team"),
                ("TagSpecification.1.Tag.1.Value", "src"),
            ],
        );
        let out = body(
            split(
                &svc,
                &src,
                "2",
                &[
                    ("TagSpecification.1.ResourceType", "capacity-reservation"),
                    ("TagSpecification.1.Tag.1.Key", "stack"),
                    ("TagSpecification.1.Tag.1.Value", "prod"),
                ],
            )
            .unwrap(),
        );
        let src_xml = between(&out, "sourceCapacityReservation");
        let dest_xml = between(&out, "destinationCapacityReservation");
        assert_eq!(between(&out, "instanceCount"), "2");
        assert_eq!(between(src_xml, "capacityReservationId"), src);
        assert_eq!(between(src_xml, "totalInstanceCount"), "3");
        assert_eq!(between(src_xml, "availableInstanceCount"), "3");
        let dest = between(dest_xml, "capacityReservationId").to_string();
        assert_ne!(dest, src);
        assert_eq!(between(dest_xml, "totalInstanceCount"), "2");
        assert_eq!(between(dest_xml, "availableInstanceCount"), "2");
        assert_eq!(between(dest_xml, "state"), "active");
        // Same configuration as the source...
        assert_eq!(between(dest_xml, "instanceType"), "m5.large");
        assert_eq!(between(dest_xml, "tenancy"), "dedicated");
        assert_eq!(between(dest_xml, "ebsOptimized"), "true");
        // ...except tags: only the requested ones.
        assert!(dest_xml.contains("<key>stack</key>"), "{dest_xml}");
        assert!(!dest_xml.contains("<key>team</key>"), "{dest_xml}");
        // Persisted on both sides.
        let s = cr(&svc, &src);
        assert_eq!((s.total_instance_count, s.available_instance_count), (3, 3));
        let d = cr(&svc, &dest);
        assert_eq!((d.total_instance_count, d.available_instance_count), (2, 2));
        let described = body(
            describe_capacity_reservations(
                &svc,
                &req(
                    "DescribeCapacityReservations",
                    &[("CapacityReservationId.1", &dest)],
                ),
            )
            .unwrap(),
        );
        assert!(described.contains("<key>stack</key>"), "{described}");
    }

    #[test]
    fn split_takes_available_capacity_first_then_used() {
        let svc = Ec2Service::new();
        let src = create(&svc, &[("InstanceCount", "8")]);
        // 5 used, 3 available.
        set_available(&svc, &src, 3);
        let out = body(split(&svc, &src, "4", &[]).unwrap());
        let dest_xml = between(&out, "destinationCapacityReservation");
        assert_eq!(between(dest_xml, "totalInstanceCount"), "4");
        assert_eq!(between(dest_xml, "availableInstanceCount"), "3");
        let s = cr(&svc, &src);
        assert_eq!((s.total_instance_count, s.available_instance_count), (4, 0));
    }

    #[test]
    fn split_validates_source_and_count() {
        let svc = Ec2Service::new();
        assert_eq!(
            err_of(split(&svc, "cr-0123456789abcdef0", "1", &[])).code(),
            "InvalidCapacityReservationId.NotFound"
        );
        let src = create(&svc, &[("InstanceCount", "3")]);
        for bad in ["0", "-1", "abc", "3", "4"] {
            assert_eq!(
                err_of(split(&svc, &src, bad, &[])).code(),
                "InvalidParameterValue",
                "count {bad}"
            );
        }
        // Nothing was taken by the rejected requests.
        assert_eq!(cr(&svc, &src).total_instance_count, 3);
        // The maximum is the size minus one.
        split(&svc, &src, "2", &[]).unwrap();
        assert_eq!(cr(&svc, &src).total_instance_count, 1);
        // A one-instance reservation can't be split at all.
        assert_eq!(
            err_of(split(&svc, &src, "1", &[])).code(),
            "InvalidParameterValue"
        );
    }

    #[test]
    fn split_requires_an_active_on_demand_reservation() {
        let svc = Ec2Service::new();
        let cancelled = create(&svc, &[("InstanceCount", "3")]);
        cancel_capacity_reservation(
            &svc,
            &req(
                "CancelCapacityReservation",
                &[("CapacityReservationId", &cancelled)],
            ),
        )
        .unwrap();
        assert_eq!(
            err_of(split(&svc, &cancelled, "1", &[])).code(),
            "IncorrectState"
        );

        let future = (Utc::now() + Duration::days(10)).to_rfc3339();
        let scheduled = create(
            &svc,
            &[
                ("InstanceCount", "3"),
                ("StartDate", &future),
                ("CommitmentDuration", "1209600"),
            ],
        );
        assert_eq!(
            err_of(split(&svc, &scheduled, "1", &[])).code(),
            "IncorrectState"
        );

        // Delivered but still inside its commitment period.
        {
            let mut accounts = svc.state.write();
            let r = accounts
                .regional_mut("000000000000", "us-east-1")
                .capacity_reservations
                .get_mut(&scheduled)
                .unwrap();
            r.state = "active".to_string();
            r.start_date = Some(fmt_ts(Utc::now() - Duration::days(1)));
        }
        let e = err_of(split(&svc, &scheduled, "1", &[]));
        assert_eq!(e.code(), "IncorrectState");
        assert!(format!("{e:?}").contains("commitment"), "{e:?}");

        let block = buy_block(&svc, "p5.48xlarge", "4");
        assert_eq!(between(&block, "reservationType"), "capacity-block");
        let block_id = between(&block, "capacityReservationId").to_string();
        // Delivered, so only its kind keeps it from being split.
        svc.state
            .write()
            .regional_mut("000000000000", "us-east-1")
            .capacity_reservations
            .get_mut(&block_id)
            .unwrap()
            .state = "active".to_string();
        assert_eq!(
            err_of(split(&svc, &block_id, "1", &[])).code(),
            "UnsupportedOperation"
        );
    }

    #[test]
    fn split_used_capacity_needs_open_eligibility() {
        let svc = Ec2Service::new();
        let src = create(
            &svc,
            &[
                ("InstanceCount", "6"),
                ("InstanceMatchCriteria", "targeted"),
            ],
        );
        set_available(&svc, &src, 2);
        assert_eq!(
            err_of(split(&svc, &src, "3", &[])).code(),
            "InvalidParameterValue"
        );
        // Available capacity alone is fine.
        split(&svc, &src, "2", &[]).unwrap();
        let s = cr(&svc, &src);
        assert_eq!((s.total_instance_count, s.available_instance_count), (4, 0));
    }

    #[test]
    fn split_dry_run_and_client_token() {
        let svc = Ec2Service::new();
        let src = create(&svc, &[("InstanceCount", "6")]);
        assert_eq!(
            err_of(split(&svc, &src, "1", &[("DryRun", "true")])).code(),
            "DryRunOperation"
        );
        assert_eq!(cr(&svc, &src).total_instance_count, 6);
        let first = body(split(&svc, &src, "2", &[("ClientToken", "tok")]).unwrap());
        let again = body(split(&svc, &src, "2", &[("ClientToken", "tok")]).unwrap());
        let dest = between(
            between(&first, "destinationCapacityReservation"),
            "capacityReservationId",
        );
        assert_eq!(
            between(
                between(&again, "destinationCapacityReservation"),
                "capacityReservationId"
            ),
            dest
        );
        // Split once, not twice.
        assert_eq!(cr(&svc, &src).total_instance_count, 4);
        assert_eq!(
            err_of(split(&svc, &src, "1", &[("ClientToken", "tok")])).code(),
            "IdempotentParameterMismatch"
        );
    }

    #[test]
    fn move_shifts_capacity_between_matching_reservations() {
        let svc = Ec2Service::new();
        let src = create(&svc, &[("InstanceCount", "5")]);
        let dest = create(
            &svc,
            &[
                ("InstanceCount", "2"),
                ("InstanceMatchCriteria", "targeted"),
            ],
        );
        let out = body(mv(&svc, &src, &dest, "3", &[]).unwrap());
        assert_eq!(between(&out, "instanceCount"), "3");
        assert_eq!(
            between(
                between(&out, "sourceCapacityReservation"),
                "totalInstanceCount"
            ),
            "2"
        );
        assert_eq!(
            between(
                between(&out, "destinationCapacityReservation"),
                "totalInstanceCount"
            ),
            "5"
        );
        let d = cr(&svc, &dest);
        assert_eq!((d.total_instance_count, d.available_instance_count), (5, 5));
        // Moving everything cancels the source.
        mv(&svc, &src, &dest, "2", &[]).unwrap();
        let s = cr(&svc, &src);
        assert_eq!((s.total_instance_count, s.state.as_str()), (0, "cancelled"));
        assert_eq!(
            err_of(mv(&svc, &src, &dest, "1", &[])).code(),
            "IncorrectState"
        );
    }

    #[test]
    fn move_validates_both_reservations() {
        let svc = Ec2Service::new();
        let src = create(&svc, &[("InstanceCount", "4")]);
        let other_type = {
            let out = body(
                create_capacity_reservation(
                    &svc,
                    &req(
                        "CreateCapacityReservation",
                        &[
                            ("InstanceType", "c5.large"),
                            ("InstancePlatform", "Linux/UNIX"),
                            ("AvailabilityZone", "us-east-1a"),
                            ("InstanceCount", "1"),
                        ],
                    ),
                )
                .unwrap(),
            );
            between(&out, "capacityReservationId").to_string()
        };
        assert_eq!(
            err_of(mv(&svc, &src, "cr-0123456789abcdef0", "1", &[])).code(),
            "InvalidCapacityReservationId.NotFound"
        );
        assert_eq!(
            err_of(mv(&svc, "cr-0123456789abcdef0", &src, "1", &[])).code(),
            "InvalidCapacityReservationId.NotFound"
        );
        assert_eq!(
            err_of(mv(&svc, &src, &src, "1", &[])).code(),
            "InvalidParameterValue"
        );
        assert_eq!(
            err_of(mv(&svc, &src, &other_type, "1", &[])).code(),
            "InvalidParameterValue"
        );
        let dest = create(&svc, &[("InstanceCount", "1")]);
        assert_eq!(
            err_of(mv(&svc, &src, &dest, "5", &[])).code(),
            "InvalidParameterValue"
        );
        assert_eq!(
            err_of(mv(&svc, &src, &dest, "1", &[("DryRun", "true")])).code(),
            "DryRunOperation"
        );
        assert_eq!(cr(&svc, &src).total_instance_count, 4);
        mv(&svc, &src, &dest, "1", &[("ClientToken", "m")]).unwrap();
        mv(&svc, &src, &dest, "1", &[("ClientToken", "m")]).unwrap();
        assert_eq!(cr(&svc, &src).total_instance_count, 3);
        assert_eq!(
            err_of(mv(&svc, &src, &dest, "2", &[("ClientToken", "m")])).code(),
            "IdempotentParameterMismatch"
        );
    }

    #[test]
    fn dry_run_is_answered_before_any_validation() {
        let svc = Ec2Service::new();
        // A source that does not exist and an invalid count: the permission
        // check still answers first.
        assert_eq!(
            err_of(split(
                &svc,
                "cr-0123456789abcdef0",
                "0",
                &[("DryRun", "true")]
            ))
            .code(),
            "DryRunOperation"
        );
        assert_eq!(
            err_of(mv(&svc, "cr-1", "cr-1", "-5", &[("DryRun", "true")])).code(),
            "DryRunOperation"
        );
    }

    #[test]
    fn split_destination_is_created_at_the_split_time() {
        let svc = Ec2Service::new();
        let src = create(&svc, &[("InstanceCount", "3")]);
        let before = Utc::now() - Duration::seconds(1);
        let out = body(split(&svc, &src, "1", &[]).unwrap());
        let dest = between(&out, "destinationCapacityReservation");
        let created = parse_ts(between(dest, "createDate")).unwrap();
        assert!(created >= before && created <= Utc::now(), "{dest}");
        assert_eq!(between(dest, "createDate"), between(dest, "startDate"));
    }

    #[test]
    fn capacity_block_purchase_buys_the_described_offering() {
        let svc = Ec2Service::new();
        let id = offering(&svc, "p5.48xlarge", "2");
        let offered = body(
            describe_capacity_block_offerings(
                &svc,
                &req(
                    "DescribeCapacityBlockOfferings",
                    &[
                        ("InstanceType", "p5.48xlarge"),
                        ("InstanceCount", "2"),
                        ("CapacityDurationHours", "48"),
                    ],
                ),
            )
            .unwrap(),
        );
        // Described again, the same offering keeps its id.
        assert_eq!(between(&offered, "capacityBlockOfferingId"), id);
        assert_eq!(between(&offered, "availabilityZone"), "us-east-1a");
        let start = parse_ts(between(&offered, "startDate")).unwrap();
        let end = parse_ts(between(&offered, "endDate")).unwrap();
        assert_eq!(end - start, Duration::hours(48));
        assert_eq!(start.format("%H:%M").to_string(), "11:30");

        let out = body(
            purchase_capacity_block(
                &svc,
                &req(
                    "PurchaseCapacityBlock",
                    &[
                        ("CapacityBlockOfferingId", &id),
                        ("InstancePlatform", "Linux/UNIX"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert_eq!(between(&out, "instanceType"), "p5.48xlarge");
        assert_eq!(between(&out, "totalInstanceCount"), "2");
        assert_eq!(between(&out, "availabilityZone"), "us-east-1a");
        assert_eq!(between(&out, "startDate"), between(&offered, "startDate"));
        assert_eq!(between(&out, "endDate"), between(&offered, "endDate"));
        assert_eq!(between(&out, "reservationType"), "capacity-block");
        assert!(out.contains("arn:aws:ec2:us-east-1:"), "{out}");
        assert!(!out.contains("<capacityBlockSet><item>"), "{out}");
        // The offering is consumed; unknown offerings are rejected.
        for bad in [id.as_str(), "cb-0123456789abcdef0"] {
            assert_eq!(
                err_of(purchase_capacity_block(
                    &svc,
                    &req(
                        "PurchaseCapacityBlock",
                        &[
                            ("CapacityBlockOfferingId", bad),
                            ("InstancePlatform", "Linux/UNIX")
                        ],
                    ),
                ))
                .code(),
                "InvalidParameterValue"
            );
        }
    }

    #[test]
    fn capacity_block_offerings_follow_the_request_region_and_rules() {
        let svc = Ec2Service::new();
        let mut r = req(
            "DescribeCapacityBlockOfferings",
            &[
                ("InstanceType", "trn1.32xlarge"),
                ("CapacityDurationHours", "24"),
            ],
        );
        r.region = "eu-west-1".into();
        let out = body(describe_capacity_block_offerings(&svc, &r).unwrap());
        assert_eq!(between(&out, "availabilityZone"), "eu-west-1a");
        let id = between(&out, "capacityBlockOfferingId").to_string();
        let mut p = req(
            "PurchaseCapacityBlock",
            &[
                ("CapacityBlockOfferingId", &id),
                ("InstancePlatform", "Linux/UNIX"),
            ],
        );
        p.region = "eu-west-1".into();
        let bought = body(purchase_capacity_block(&svc, &p).unwrap());
        assert!(bought.contains("arn:aws:ec2:eu-west-1:"), "{bought}");
        for bad in ["0", "25", "200", "abc"] {
            assert_eq!(
                err_of(describe_capacity_block_offerings(
                    &svc,
                    &req(
                        "DescribeCapacityBlockOfferings",
                        &[("CapacityDurationHours", bad)]
                    ),
                ))
                .code(),
                "InvalidParameterValue",
                "{bad}"
            );
        }
        // An instance type not sold as Capacity Blocks has no offerings.
        let out = body(
            describe_capacity_block_offerings(
                &svc,
                &req(
                    "DescribeCapacityBlockOfferings",
                    &[
                        ("InstanceType", "t3.micro"),
                        ("CapacityDurationHours", "672"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(!out.contains("<item>"), "{out}");
    }
}
