//! On-demand capacity reservations, capacity reservation fleets, capacity
//! blocks, billing-owner transfer, and interruptible allocations.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, require, validate_enum, validate_int_range, validate_length,
    validate_max_results,
};
use crate::state::{CapacityReservation, Ec2State, Tag};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

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
    format!(
        "{}{}{}{}{}{}{}<totalInstanceCount>{}</totalInstanceCount><availableInstanceCount>{}</availableInstanceCount>\
         <ebsOptimized>{}</ebsOptimized><ephemeralStorage>{}</ephemeralStorage>{}{}{}{}{}{}",
        ec2_elem("capacityReservationId", &r.id),
        ec2_elem("ownerId", owner),
        ec2_elem("capacityReservationArn", &format!("arn:aws:ec2:{}:{owner}:capacity-reservation/{}", region_of(r), r.id)),
        ec2_elem("instanceType", &r.instance_type),
        ec2_elem("instancePlatform", &r.instance_platform),
        ec2_elem("availabilityZone", &r.availability_zone),
        ec2_elem("tenancy", &r.tenancy),
        r.total_instance_count,
        r.available_instance_count,
        r.ebs_optimized,
        r.ephemeral_storage,
        ec2_elem("state", &r.state),
        ec2_elem("endDateType", &r.end_date_type),
        ec2_elem("instanceMatchCriteria", &r.instance_match_criteria),
        ec2_elem("createDate", FIXED_TIME),
        ec2_elem("startDate", FIXED_TIME),
        super::tags::tag_set_xml(tags),
    )
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
        state: "active".to_string(),
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
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let r = accounts
            .get_or_create(&req.account_id)
            .capacity_reservations
            .get_mut(&id)
            .ok_or_else(|| cr_not_found(&id))?;
        r.state = "cancelled".to_string();
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
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
    {
        let mut accounts = svc.state.write();
        let r = accounts
            .get_or_create(&req.account_id)
            .capacity_reservations
            .get_mut(&id)
            .ok_or_else(|| cr_not_found(&id))?;
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
        }
        if let Some(m) = req.query_params.get("InstanceMatchCriteria") {
            r.instance_match_criteria = m.clone();
        }
    }
    Ok(Ec2Service::respond(
        "ModifyCapacityReservation",
        &req.request_id,
        &ec2_return(true),
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
        .get(&req.account_id)
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
            .get_or_create(&req.account_id)
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
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        let state = accounts.get_or_create(&req.account_id);
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
        let state = accounts.get_or_create(&req.account_id);
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

fn synth_cr_xml(id: &str, owner: &str, count: i64) -> String {
    let r = CapacityReservation {
        id: id.to_string(),
        instance_type: "t3.micro".to_string(),
        instance_platform: "Linux/UNIX".to_string(),
        availability_zone: "us-east-1a".to_string(),
        tenancy: "default".to_string(),
        total_instance_count: count,
        available_instance_count: count,
        state: "active".to_string(),
        end_date_type: "unlimited".to_string(),
        instance_match_criteria: "open".to_string(),
        ebs_optimized: false,
        ephemeral_storage: false,
    };
    cr_xml(&r, &[], owner)
}

pub(crate) fn create_capacity_reservation_by_splitting(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let src = require(&req.query_params, "SourceCapacityReservationId")?;
    let count: i64 = require(&req.query_params, "InstanceCount")?
        .parse()
        .unwrap_or(1);
    let owner = req.account_id.clone();
    let dest = gen_id("cr");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(r) = state.capacity_reservations.get(&src).cloned() {
            let mut d = r.clone();
            d.id = dest.clone();
            d.total_instance_count = count;
            d.available_instance_count = count;
            state.capacity_reservations.insert(dest.clone(), d);
        }
    }
    let body = format!(
        "<sourceCapacityReservation>{}</sourceCapacityReservation><destinationCapacityReservation>{}</destinationCapacityReservation><instanceCount>{}</instanceCount>",
        synth_cr_xml(&src, &owner, count),
        synth_cr_xml(&dest, &owner, count),
        count,
    );
    Ok(Ec2Service::respond(
        "CreateCapacityReservationBySplitting",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn move_capacity_reservation_instances(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let src = require(&req.query_params, "SourceCapacityReservationId")?;
    let dest = require(&req.query_params, "DestinationCapacityReservationId")?;
    let count: i64 = require(&req.query_params, "InstanceCount")?
        .parse()
        .unwrap_or(1);
    let owner = req.account_id.clone();
    let body = format!(
        "<sourceCapacityReservation>{}</sourceCapacityReservation><destinationCapacityReservation>{}</destinationCapacityReservation><instanceCount>{}</instanceCount>",
        synth_cr_xml(&src, &owner, count),
        synth_cr_xml(&dest, &owner, count),
        count,
    );
    Ok(Ec2Service::respond(
        "MoveCapacityReservationInstances",
        &req.request_id,
        &body,
    ))
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

pub(crate) fn describe_capacity_block_offerings(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CapacityDurationHours")?;
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeCapacityBlockOfferings",
        &req.request_id,
        &ec2_list("capacityBlockOfferingSet", &[]),
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

pub(crate) fn purchase_capacity_block(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CapacityBlockOfferingId")?;
    require(&req.query_params, "InstancePlatform")?;
    validate_enum(&req.query_params, "InstancePlatform", INSTANCE_PLATFORMS)?;
    let owner = req.account_id.clone();
    let id = gen_id("cr");
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .capacity_reservations
            .insert(
                id.clone(),
                CapacityReservation {
                    id: id.clone(),
                    instance_type: "p5.48xlarge".to_string(),
                    instance_platform: req
                        .query_params
                        .get("InstancePlatform")
                        .cloned()
                        .unwrap_or_else(|| "Linux/UNIX".to_string()),
                    availability_zone: "us-east-1a".to_string(),
                    tenancy: "default".to_string(),
                    total_instance_count: 1,
                    available_instance_count: 1,
                    state: "active".to_string(),
                    end_date_type: "limited".to_string(),
                    instance_match_criteria: "targeted".to_string(),
                    ebs_optimized: false,
                    ephemeral_storage: false,
                },
            );
    }
    let body = format!(
        "<capacityReservation>{}</capacityReservation>{}",
        synth_cr_xml(&id, &owner, 1),
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

pub(crate) fn create_interruptible_capacity_reservation_allocation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CapacityReservationId")?;
    let count = require(&req.query_params, "InstanceCount")?;
    let body = format!(
        "{}<targetInstanceCount>{}</targetInstanceCount><status>active</status><interruptionType>spot</interruptionType>",
        ec2_elem("sourceCapacityReservationId", &id),
        count,
    );
    Ok(Ec2Service::respond(
        "CreateInterruptibleCapacityReservationAllocation",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn update_interruptible_capacity_reservation_allocation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CapacityReservationId")?;
    let count = require(&req.query_params, "TargetInstanceCount")?;
    let body = format!(
        "{}{}<targetInstanceCount>{}</targetInstanceCount><status>active</status><interruptionType>spot</interruptionType>",
        ec2_elem("interruptibleCapacityReservationId", &id),
        ec2_elem("sourceCapacityReservationId", &id),
        count,
    );
    Ok(Ec2Service::respond(
        "UpdateInterruptibleCapacityReservationAllocation",
        &req.request_id,
        &body,
    ))
}

#[cfg(test)]
mod crfleet_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
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
