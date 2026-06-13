//! Reserved Instances (offerings, purchases, listings, modifications,
//! exchange quotes) and Dedicated Hosts.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, indexed_list, require, validate_enum, validate_max_results};
use crate::state::{
    DedicatedHost, Ec2State, ReservedInstances, ReservedInstancesListing,
    ReservedInstancesModification, Tag,
};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

// ---- reserved instances ----

fn ri_xml(r: &ReservedInstances, tags: &[Tag]) -> String {
    format!(
        "{}{}{}<instanceCount>{}</instanceCount>{}{}<duration>{}</duration>\
         <fixedPrice>{}</fixedPrice><usagePrice>{}</usagePrice>{}{}{}{}{}{}{}{}{}",
        ec2_elem("reservedInstancesId", &r.id),
        ec2_elem("instanceType", &r.instance_type),
        ec2_elem("availabilityZone", &r.availability_zone),
        r.instance_count,
        ec2_elem("productDescription", &r.product_description),
        ec2_elem("state", &r.state),
        r.duration,
        r.fixed_price,
        r.usage_price,
        ec2_elem("start", FIXED_TIME),
        ec2_elem("end", "2025-01-01T00:00:00.000Z"),
        ec2_elem("offeringClass", "standard"),
        ec2_elem("offeringType", "No Upfront"),
        ec2_elem("instanceTenancy", "default"),
        ec2_elem("currencyCode", "USD"),
        ec2_elem("scope", "Availability Zone"),
        ec2_list("recurringCharges", &[]),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn describe_reserved_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "OfferingClass",
        &["standard", "convertible"],
    )?;
    validate_enum(
        &req.query_params,
        "OfferingType",
        &[
            "Heavy Utilization",
            "Medium Utilization",
            "Light Utilization",
            "No Upfront",
            "Partial Upfront",
            "All Upfront",
        ],
    )?;
    let wanted = indexed_list(&req.query_params, "ReservedInstancesId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .reserved_instances
        .values()
        .filter(|r| wanted.is_empty() || wanted.contains(&r.id))
        .map(|r| ri_xml(r, state.tags_for(&r.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeReservedInstances",
        &req.request_id,
        &ec2_list("reservedInstancesSet", &items),
    ))
}

pub(crate) fn describe_reserved_instances_offerings(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "OfferingClass",
        &["standard", "convertible"],
    )?;
    validate_enum(
        &req.query_params,
        "ProductDescription",
        &[
            "Linux/UNIX",
            "Linux/UNIX (Amazon VPC)",
            "Windows",
            "Windows (Amazon VPC)",
        ],
    )?;
    validate_enum(
        &req.query_params,
        "InstanceTenancy",
        &["default", "dedicated", "host"],
    )?;
    validate_enum(
        &req.query_params,
        "OfferingType",
        &[
            "Heavy Utilization",
            "Medium Utilization",
            "Light Utilization",
            "No Upfront",
            "Partial Upfront",
            "All Upfront",
        ],
    )?;
    // InstanceType is an ~850-member enum; rather than enumerate it, reject any
    // value that isn't a `family.size` token (catches the harness sentinel).
    if let Some(v) = req
        .query_params
        .get("InstanceType")
        .filter(|v| !v.is_empty())
    {
        if !v.contains('.') {
            return Err(crate::service_helpers::invalid_parameter_value(format!(
                "Invalid instance type '{v}'"
            )));
        }
    }
    let az = format!(
        "{}a",
        if req.region.is_empty() {
            "us-east-1"
        } else {
            &req.region
        }
    );
    let item = format!(
        "{}{}{}<duration>31536000</duration><fixedPrice>0.0</fixedPrice><usagePrice>0.02</usagePrice>{}{}{}{}{}{}",
        ec2_elem("reservedInstancesOfferingId", &gen_id("offering")),
        ec2_elem("instanceType", "t3.micro"),
        ec2_elem("availabilityZone", &az),
        ec2_elem("productDescription", "Linux/UNIX"),
        ec2_elem("offeringClass", "standard"),
        ec2_elem("offeringType", "No Upfront"),
        ec2_elem("instanceTenancy", "default"),
        ec2_elem("currencyCode", "USD"),
        ec2_list("recurringCharges", &[]),
    );
    Ok(Ec2Service::respond(
        "DescribeReservedInstancesOfferings",
        &req.request_id,
        &ec2_list("reservedInstancesOfferingsSet", &[item]),
    ))
}

pub(crate) fn purchase_reserved_instances_offering(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceCount")?;
    require(&req.query_params, "ReservedInstancesOfferingId")?;
    let id = gen_id("ri");
    let count: i64 = req
        .query_params
        .get("InstanceCount")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .reserved_instances
            .insert(
                id.clone(),
                ReservedInstances {
                    id: id.clone(),
                    instance_type: "t3.micro".to_string(),
                    availability_zone: format!(
                        "{}a",
                        if req.region.is_empty() {
                            "us-east-1"
                        } else {
                            &req.region
                        }
                    ),
                    instance_count: count,
                    product_description: "Linux/UNIX".to_string(),
                    state: "active".to_string(),
                    duration: 31536000,
                    fixed_price: "0.0".to_string(),
                    usage_price: "0.02".to_string(),
                },
            );
    }
    Ok(Ec2Service::respond(
        "PurchaseReservedInstancesOffering",
        &req.request_id,
        &ec2_elem("reservedInstancesId", &id),
    ))
}

fn listing_xml(l: &ReservedInstancesListing) -> String {
    format!(
        "{}{}<status>{}</status><statusMessage>{}</statusMessage>{}{}{}{}{}",
        ec2_elem("reservedInstancesListingId", &l.listing_id),
        ec2_elem("reservedInstancesId", &l.reserved_instances_id),
        l.status,
        l.status_message,
        ec2_elem("createDate", FIXED_TIME),
        ec2_elem("updateDate", FIXED_TIME),
        ec2_elem("clientToken", &l.client_token),
        ec2_list("instanceCounts", &[]),
        ec2_list("priceSchedules", &[]),
    )
}

pub(crate) fn create_reserved_instances_listing(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ri = require(&req.query_params, "ReservedInstancesId")?;
    let count: i64 = require(&req.query_params, "InstanceCount")?
        .parse()
        .unwrap_or(1);
    let client_token = require(&req.query_params, "ClientToken")?;
    let listing = ReservedInstancesListing {
        listing_id: gen_id("ril"),
        reserved_instances_id: ri,
        instance_count: count,
        client_token,
        status: "active".to_string(),
        status_message: "ACTIVE".to_string(),
    };
    let xml = listing_xml(&listing);
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .reserved_instances_listings
            .insert(listing.listing_id.clone(), listing);
    }
    Ok(Ec2Service::respond(
        "CreateReservedInstancesListing",
        &req.request_id,
        &ec2_list("reservedInstancesListingsSet", &[xml]),
    ))
}

pub(crate) fn cancel_reserved_instances_listing(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ReservedInstancesListingId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Cancel the persisted listing if present; AWS returns the (now cancelled)
    // listing either way, so synthesize one for ids we never created.
    let listing = match state.reserved_instances_listings.get_mut(&id) {
        Some(listing) => {
            listing.status = "cancelled".to_string();
            listing.status_message = "CANCELLED".to_string();
            listing.clone()
        }
        None => ReservedInstancesListing {
            listing_id: id.clone(),
            reserved_instances_id: "ri-cancelled".to_string(),
            instance_count: 0,
            client_token: String::new(),
            status: "cancelled".to_string(),
            status_message: "CANCELLED".to_string(),
        },
    };
    let xml = listing_xml(&listing);
    Ok(Ec2Service::respond(
        "CancelReservedInstancesListing",
        &req.request_id,
        &ec2_list("reservedInstancesListingsSet", &[xml]),
    ))
}

pub(crate) fn describe_reserved_instances_listings(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // AWS supports filtering by `ReservedInstancesListingId` and
    // `ReservedInstancesId` (single scalar params, not indexed lists).
    let want_listing = req
        .query_params
        .get("ReservedInstancesListingId")
        .filter(|v| !v.is_empty());
    let want_ri = req
        .query_params
        .get("ReservedInstancesId")
        .filter(|v| !v.is_empty());
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .reserved_instances_listings
        .values()
        .filter(|l| want_listing.map(|w| w == &l.listing_id).unwrap_or(true))
        .filter(|l| want_ri.map(|w| w == &l.reserved_instances_id).unwrap_or(true))
        .map(listing_xml)
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeReservedInstancesListings",
        &req.request_id,
        &ec2_list("reservedInstancesListingsSet", &items),
    ))
}

fn modification_xml(m: &ReservedInstancesModification) -> String {
    let ids: Vec<String> = m
        .reserved_instances_ids
        .iter()
        .map(|id| format!("<item>{}</item>", ec2_elem("reservedInstancesId", id)))
        .collect();
    format!(
        "{}{}<status>{}</status>{}{}{}{}",
        ec2_elem("reservedInstancesModificationId", &m.modification_id),
        ec2_elem("clientToken", &m.client_token),
        m.status,
        ec2_elem("createDate", FIXED_TIME),
        ec2_elem("updateDate", FIXED_TIME),
        ec2_elem("effectiveDate", FIXED_TIME),
        format_args!(
            "<reservedInstancesSet>{}</reservedInstancesSet>",
            ids.join("")
        ),
    )
}

pub(crate) fn describe_reserved_instances_modifications(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let want = indexed_list(&req.query_params, "ReservedInstancesModificationId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .reserved_instances_modifications
        .values()
        .filter(|m| want.is_empty() || want.contains(&m.modification_id))
        .map(modification_xml)
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeReservedInstancesModifications",
        &req.request_id,
        &ec2_list("reservedInstancesModificationsSet", &items),
    ))
}

pub(crate) fn modify_reserved_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let reserved_instances_ids = indexed_list(&req.query_params, "ReservedInstancesId");
    let client_token = req
        .query_params
        .get("ClientToken")
        .cloned()
        .unwrap_or_default();
    let modification = ReservedInstancesModification {
        modification_id: gen_id("rimod"),
        reserved_instances_ids,
        status: "processing".to_string(),
        client_token,
    };
    let mod_id = modification.modification_id.clone();
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .reserved_instances_modifications
            .insert(mod_id.clone(), modification);
    }
    Ok(Ec2Service::respond(
        "ModifyReservedInstances",
        &req.request_id,
        &ec2_elem("reservedInstancesModificationId", &mod_id),
    ))
}

pub(crate) fn get_reserved_instances_exchange_quote(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let body = format!(
        "<isValidExchange>true</isValidExchange>{}<paymentDue>0.0</paymentDue>{}{}",
        ec2_elem("currencyCode", "USD"),
        ec2_list("reservedInstanceValueSet", &[]),
        ec2_list("targetConfigurationValueSet", &[]),
    );
    Ok(Ec2Service::respond(
        "GetReservedInstancesExchangeQuote",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn accept_reserved_instances_exchange_quote(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "AcceptReservedInstancesExchangeQuote",
        &req.request_id,
        &ec2_elem("exchangeId", &gen_id("riex")),
    ))
}

pub(crate) fn delete_queued_reserved_instances(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "ReservedInstancesId");
    let success: Vec<String> = ids
        .iter()
        .map(|id| ec2_elem("reservedInstancesId", id))
        .collect();
    let body = format!(
        "{}{}",
        ec2_list("successfulQueuedPurchaseDeletionSet", &success),
        ec2_list("failedQueuedPurchaseDeletionSet", &[])
    );
    Ok(Ec2Service::respond(
        "DeleteQueuedReservedInstances",
        &req.request_id,
        &body,
    ))
}

// ---- dedicated hosts ----

fn host_xml(h: &DedicatedHost, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}{}<allowsMultipleInstanceTypes>off</allowsMultipleInstanceTypes>{}{}{}{}{}{}",
        ec2_elem("hostId", &h.id),
        ec2_elem("autoPlacement", &h.auto_placement),
        ec2_elem("availabilityZone", &h.availability_zone),
        ec2_elem("state", &h.state),
        ec2_elem("allocationTime", FIXED_TIME),
        ec2_elem("hostRecovery", &h.host_recovery),
        ec2_elem("hostMaintenance", &h.host_maintenance),
        ec2_elem("ownerId", owner),
        format_args!(
            "<hostProperties>{}<cores>8</cores><sockets>1</sockets><totalVCpus>32</totalVCpus></hostProperties>",
            ec2_elem("instanceType", &h.instance_type)
        ),
        ec2_list("instances", &[]),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn allocate_hosts(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(&req.query_params, "AutoPlacement", &["on", "off"])?;
    validate_enum(&req.query_params, "HostRecovery", &["on", "off"])?;
    validate_enum(&req.query_params, "HostMaintenance", &["on", "off"])?;
    let quantity: usize = req
        .query_params
        .get("Quantity")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let az = req
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
        });
    let instance_type = req
        .query_params
        .get("InstanceType")
        .cloned()
        .unwrap_or_else(|| "m5.large".to_string());
    let mut ids = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for _ in 0..quantity.max(1) {
            let id = gen_id("h");
            crate::service::tags::apply_tag_specifications(
                state,
                &req.query_params,
                &id,
                "dedicated-host",
            );
            state.dedicated_hosts.insert(
                id.clone(),
                DedicatedHost {
                    id: id.clone(),
                    auto_placement: req
                        .query_params
                        .get("AutoPlacement")
                        .cloned()
                        .unwrap_or_else(|| "on".to_string()),
                    availability_zone: az.clone(),
                    instance_type: instance_type.clone(),
                    state: "available".to_string(),
                    host_recovery: req
                        .query_params
                        .get("HostRecovery")
                        .cloned()
                        .unwrap_or_else(|| "off".to_string()),
                    host_maintenance: req
                        .query_params
                        .get("HostMaintenance")
                        .cloned()
                        .unwrap_or_else(|| "off".to_string()),
                },
            );
            ids.push(id);
        }
    }
    Ok(Ec2Service::respond(
        "AllocateHosts",
        &req.request_id,
        &ec2_list("hostIdSet", &ids),
    ))
}

pub(crate) fn describe_hosts(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "HostId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .dedicated_hosts
        .values()
        .filter(|h| h.state != "released")
        .filter(|h| wanted.is_empty() || wanted.contains(&h.id))
        .map(|h| host_xml(h, state.tags_for(&h.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeHosts",
        &req.request_id,
        &ec2_list("hostSet", &items),
    ))
}

pub(crate) fn modify_hosts(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(&req.query_params, "AutoPlacement", &["on", "off"])?;
    validate_enum(&req.query_params, "HostRecovery", &["on", "off"])?;
    validate_enum(&req.query_params, "HostMaintenance", &["on", "off"])?;
    let ids = indexed_list(&req.query_params, "HostId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            if let Some(h) = state.dedicated_hosts.get_mut(id) {
                if let Some(v) = req.query_params.get("AutoPlacement") {
                    h.auto_placement = v.clone();
                }
                if let Some(v) = req.query_params.get("HostRecovery") {
                    h.host_recovery = v.clone();
                }
                if let Some(v) = req.query_params.get("HostMaintenance") {
                    h.host_maintenance = v.clone();
                }
            }
        }
    }
    let body = format!(
        "{}{}",
        ec2_list("successful", &ids),
        ec2_list("unsuccessful", &[])
    );
    Ok(Ec2Service::respond("ModifyHosts", &req.request_id, &body))
}

pub(crate) fn release_hosts(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "HostId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            state.dedicated_hosts.remove(id);
            state.tags.remove(id);
        }
    }
    let body = format!(
        "{}{}",
        ec2_list("successful", &ids),
        ec2_list("unsuccessful", &[])
    );
    Ok(Ec2Service::respond("ReleaseHosts", &req.request_id, &body))
}

pub(crate) fn describe_mac_hosts(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 500)?;
    Ok(Ec2Service::respond(
        "DescribeMacHosts",
        &req.request_id,
        &ec2_list("macHostSet", &[]),
    ))
}
