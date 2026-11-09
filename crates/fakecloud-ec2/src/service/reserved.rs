//! Reserved Instances (offerings, purchases, listings, modifications,
//! exchange quotes) and Dedicated Hosts.

use chrono::Timelike;
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

fn ri_xml(r: &ReservedInstances, state: &str, tags: &[Tag]) -> String {
    format!(
        "{}{}{}<instanceCount>{}</instanceCount>{}{}<duration>{}</duration>\
         <fixedPrice>{}</fixedPrice><usagePrice>{}</usagePrice>{}{}{}{}{}{}{}{}{}",
        ec2_elem("reservedInstancesId", &r.id),
        ec2_elem("instanceType", &r.instance_type),
        if r.availability_zone.is_empty() {
            String::new()
        } else {
            ec2_elem("availabilityZone", &r.availability_zone)
        },
        r.instance_count,
        ec2_elem("productDescription", &r.product_description),
        ec2_elem("state", state),
        r.duration,
        r.fixed_price,
        r.usage_price,
        ec2_elem("start", &r.start),
        ec2_elem("end", &r.end),
        ec2_elem("offeringClass", &r.offering_class),
        ec2_elem("offeringType", &r.offering_type),
        ec2_elem("instanceTenancy", &r.instance_tenancy),
        ec2_elem("currencyCode", "USD"),
        ec2_elem("scope", &r.scope),
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
    let param = |name: &str| req.query_params.get(name).filter(|v| !v.is_empty());
    let (class, offering_type) = (param("OfferingClass"), param("OfferingType"));
    let now = chrono::Utc::now();
    let accounts = svc.state.read();
    let mut items: Vec<String> = accounts
        .regional(&req.account_id, &req.region)
        .map(|state| {
            state
                .reserved_instances
                .values()
                .filter(|r| wanted.is_empty() || wanted.contains(&r.id))
                .filter(|r| class.is_none_or(|c| *c == r.offering_class))
                .filter(|r| offering_type.is_none_or(|t| *t == r.offering_type))
                .map(|r| ri_xml(r, ri_state(r, now), state.tags_for(&r.id)))
                .collect()
        })
        .unwrap_or_default();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeReservedInstances",
        &req.request_id,
        &ec2_list("reservedInstancesSet", &items),
    ))
}

/// The terms real EC2 offers a reservation for: 1 year and 3 years.
const RI_TERM_SECONDS: [i64; 2] = [31_536_000, 94_608_000];
const PRODUCTS: [&str; 4] = [
    "Linux/UNIX",
    "Linux/UNIX (Amazon VPC)",
    "Windows",
    "Windows (Amazon VPC)",
];
const CLASSES: [&str; 2] = ["standard", "convertible"];
const OFFERING_TYPES: [&str; 6] = [
    "Heavy Utilization",
    "Medium Utilization",
    "Light Utilization",
    "No Upfront",
    "Partial Upfront",
    "All Upfront",
];
const TENANCIES: [&str; 3] = ["default", "dedicated", "host"];
/// What every offering costs here: fixed placeholders, not AWS's prices.
const FIXED_PRICE: &str = "0.0";
const USAGE_PRICE: &str = "0.02";

/// One Reserved Instance offering of a Region, as DescribeReservedInstancesOfferings
/// lists it and PurchaseReservedInstancesOffering buys it.
#[derive(Clone, Debug, PartialEq)]
struct Offering {
    instance_type: &'static str,
    /// `None` for a Region-scoped offering.
    zone: Option<String>,
    product: &'static str,
    class: &'static str,
    offering_type: &'static str,
    tenancy: &'static str,
    duration: i64,
}

fn fnv64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

fn index_of(values: &[&'static str], value: &str) -> Option<u8> {
    values
        .iter()
        .position(|v| *v == value)
        .and_then(|i| u8::try_from(i).ok())
}

impl Offering {
    fn key(&self, region: &str) -> String {
        format!(
            "{region}|{}|{}|{}|{}|{}|{}|{}",
            self.zone.as_deref().unwrap_or(""),
            self.instance_type,
            self.product,
            self.class,
            self.offering_type,
            self.tenancy,
            self.duration
        )
    }

    /// The check half of the id: a hash of the offering, with the UUID version
    /// nibble set.
    fn check(&self, region: &str) -> [u8; 8] {
        let mut check = fnv64(self.key(region).as_bytes()).to_be_bytes();
        check[6] = (check[6] & 0x0f) | 0x40;
        check
    }

    /// What the payload half is masked with, keyed by the check so ids look
    /// unrelated; its first byte carries the UUID variant bits.
    fn mask(check: &[u8; 8]) -> [u8; 8] {
        let mut mask = fnv64(&[&check[..], b"/offering"].concat()).to_be_bytes();
        mask[0] = (mask[0] & 0x3f) | 0x80;
        mask
    }

    /// The offering's id: UUID-shaped like AWS's, and reversible, so a purchase
    /// resolves it without remembering what was described. The first half is a
    /// hash of the offering and its Region; the second encodes the catalog
    /// index of the type, the zone letter and the option indexes, masked.
    fn id(&self, region: &str) -> String {
        let type_index = crate::instance_types::catalog()
            .binary_search_by(|t| t.name.as_str().cmp(self.instance_type))
            .ok()
            .and_then(|i| u16::try_from(i).ok())
            .filter(|i| *i < 0x4000)
            .unwrap_or(0);
        // The zone by its position among the Region's zones, 0 for none.
        let zone_slot = self
            .zone
            .as_deref()
            .and_then(|z| {
                crate::defaults::region_zones(region)
                    .iter()
                    .position(|(name, _)| name == z)
            })
            .and_then(|i| u8::try_from(i + 1).ok())
            .unwrap_or(0);
        let payload = [
            (type_index >> 8) as u8,
            type_index as u8,
            zone_slot,
            index_of(&PRODUCTS, self.product).unwrap_or(0),
            index_of(&CLASSES, self.class).unwrap_or(0),
            index_of(&OFFERING_TYPES, self.offering_type).unwrap_or(0),
            index_of(&TENANCIES, self.tenancy).unwrap_or(0),
            RI_TERM_SECONDS
                .iter()
                .position(|d| *d == self.duration)
                .unwrap_or(0) as u8,
        ];
        let check = self.check(region);
        let mask = Self::mask(&check);
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&check);
        for i in 0..8 {
            bytes[8 + i] = payload[i] ^ mask[i];
        }
        uuid::Uuid::from_bytes(bytes).to_string()
    }

    /// The offering an id of this Region names, `None` for one no
    /// DescribeReservedInstancesOfferings here lists.
    fn from_id(region: &str, id: &str) -> Option<Offering> {
        let bytes = *uuid::Uuid::parse_str(id).ok()?.as_bytes();
        let check: [u8; 8] = bytes[..8].try_into().ok()?;
        let mask = Self::mask(&check);
        let payload: Vec<u8> = (0..8).map(|i| bytes[8 + i] ^ mask[i]).collect();
        let type_index = usize::from(u16::from_be_bytes([payload[0], payload[1]]));
        let offering = Offering {
            instance_type: crate::instance_types::catalog()
                .get(type_index)?
                .name
                .as_str(),
            zone: match payload[2] {
                0 => None,
                slot => Some(
                    crate::defaults::region_zones(region)
                        .into_iter()
                        .nth(usize::from(slot) - 1)?
                        .0,
                ),
            },
            product: PRODUCTS.get(usize::from(payload[3]))?,
            class: CLASSES.get(usize::from(payload[4]))?,
            offering_type: OFFERING_TYPES.get(usize::from(payload[5]))?,
            tenancy: TENANCIES.get(usize::from(payload[6]))?,
            duration: *RI_TERM_SECONDS.get(usize::from(payload[7]))?,
        };
        (offering.check(region) == check && offering.listed(region)).then_some(offering)
    }

    /// Whether the Region lists this offering: Region-scoped where the Region
    /// offers the type, zonal in a zone selling it, and convertible only
    /// Region-scoped.
    fn listed(&self, region: &str) -> bool {
        let Some(spec) = crate::instance_types::lookup(self.instance_type) else {
            return false;
        };
        match &self.zone {
            None => spec.offered_in(region),
            Some(zone) => self.class != "convertible" && spec.zones_selling(region).contains(zone),
        }
    }

    fn xml(&self, region: &str) -> String {
        format!(
            "{}{}{}<duration>{}</duration><fixedPrice>{FIXED_PRICE}</fixedPrice><usagePrice>{USAGE_PRICE}</usagePrice>{}{}{}{}{}{}{}",
            ec2_elem("reservedInstancesOfferingId", &self.id(region)),
            ec2_elem("instanceType", self.instance_type),
            self.zone
                .as_ref()
                .map_or_else(String::new, |z| ec2_elem("availabilityZone", z)),
            self.duration,
            ec2_elem("productDescription", self.product),
            ec2_elem("offeringClass", self.class),
            ec2_elem("offeringType", self.offering_type),
            ec2_elem("instanceTenancy", self.tenancy),
            ec2_elem("currencyCode", "USD"),
            ec2_elem("scope", scope_of(self.zone.as_deref())),
            ec2_list("recurringCharges", &[]),
        )
    }
}

fn scope_of(zone: Option<&str>) -> &'static str {
    if zone.is_some() {
        "Availability Zone"
    } else {
        "Region"
    }
}

/// The value of an enum parameter, or `default`; the caller has validated it
/// is one of `values`.
fn enum_param(
    req: &AwsRequest,
    name: &str,
    values: &[&'static str],
    default: &'static str,
) -> &'static str {
    req.query_params
        .get(name)
        .filter(|v| !v.is_empty())
        .and_then(|v| values.iter().find(|x| *x == v).copied())
        .unwrap_or(default)
}

pub(crate) fn describe_reserved_instances_offerings(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(&req.query_params, "OfferingClass", &CLASSES)?;
    validate_enum(&req.query_params, "ProductDescription", &PRODUCTS)?;
    validate_enum(&req.query_params, "InstanceTenancy", &TENANCIES)?;
    validate_enum(&req.query_params, "OfferingType", &OFFERING_TYPES)?;
    let region = crate::service::instance::request_region(req);
    let instance_type = req
        .query_params
        .get("InstanceType")
        .filter(|v| !v.is_empty())
        .map_or("t3.micro", String::as_str);
    // Recorded from real EC2 for both `m5.bogus` and `nodot`.
    let spec = crate::instance_types::lookup(instance_type).ok_or_else(|| {
        crate::service_helpers::invalid_parameter_value(format!(
            "The following supplied instance types do not exist: [{instance_type}]"
        ))
    })?;
    let product = enum_param(req, "ProductDescription", &PRODUCTS, "Linux/UNIX");
    let class = enum_param(req, "OfferingClass", &CLASSES, "standard");
    let offering_type = enum_param(req, "OfferingType", &OFFERING_TYPES, "No Upfront");
    let tenancy = enum_param(req, "InstanceTenancy", &TENANCIES, "default");
    // A Region-scoped offering where the Region offers the type, and one per
    // zone selling it, as real EC2 lists them (m5.large in us-east-1: the
    // Region plus every zone but 1e). Convertible offerings are Region-scoped
    // only. A requested zone keeps only its own.
    let wanted_zone = req
        .query_params
        .get("AvailabilityZone")
        .filter(|v| !v.is_empty());
    let mut locations: Vec<Option<String>> = vec![None];
    locations.extend(spec.zones_selling(region).into_iter().map(Some));
    if let Some(zone) = wanted_zone {
        locations.retain(|l| l.as_ref() == Some(zone));
    }
    // Each location is offered for a 1-year and a 3-year term, as real EC2
    // lists them, kept to the requested [MinDuration, MaxDuration] (seconds).
    let bound = |name: &str| -> Result<Option<i64>, AwsServiceError> {
        req.query_params
            .get(name)
            .filter(|v| !v.is_empty())
            .map(|v| {
                v.parse::<i64>().map_err(|_| {
                    crate::service_helpers::invalid_parameter_value(format!(
                        "Invalid value '{v}' for {name}."
                    ))
                })
            })
            .transpose()
    };
    let (min_duration, max_duration) = (bound("MinDuration")?, bound("MaxDuration")?);
    let items: Vec<String> = locations
        .into_iter()
        .flat_map(|zone| {
            RI_TERM_SECONDS.into_iter().map(move |duration| Offering {
                instance_type: spec.name.as_str(),
                zone: zone.clone(),
                product,
                class,
                offering_type,
                tenancy,
                duration,
            })
        })
        .filter(|o| {
            min_duration.is_none_or(|m| o.duration >= m)
                && max_duration.is_none_or(|m| o.duration <= m)
                && o.listed(region)
        })
        .map(|o| o.xml(region))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeReservedInstancesOfferings",
        &req.request_id,
        &ec2_list("reservedInstancesOfferingsSet", &items),
    ))
}

/// How long a purchase reads `payment-pending` after it is made, before it is
/// `active`. Inferred: AWS documents the transition but not its length, and
/// payment here never fails.
const PAYMENT_PENDING_SECONDS: i64 = 2;

/// The state of a reservation at `now`, from its stored deletion and its
/// clock alone: `queued-deleted` once its queued purchase is deleted, `queued`
/// until its start, `payment-pending` just after, `active` until its end, then
/// `retired`.
fn ri_state(r: &ReservedInstances, now: chrono::DateTime<chrono::Utc>) -> &'static str {
    if r.state == "queued-deleted" {
        return "queued-deleted";
    }
    let (Some(start), Some(end)) = (parse_time(&r.start), parse_time(&r.end)) else {
        return "active";
    };
    if now < start {
        "queued"
    } else if now < start + chrono::Duration::seconds(PAYMENT_PENDING_SECONDS) {
        "payment-pending"
    } else if now < end {
        "active"
    } else {
        "retired"
    }
}

fn parse_time(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|t| t.with_timezone(&chrono::Utc))
}

/// When a purchase is made: now, or the requested `PurchaseTime` for a queued
/// one (`Some` only for a time still ahead).
///
/// Documented: only Region-scoped reservations can be queued, and at most
/// three years ahead. Inferred, not recorded: the codes and messages, a time
/// in the past being refused rather than bought now, and a time within the
/// current second counting as now.
fn queued_purchase_time(
    raw: Option<&String>,
    zonal: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, AwsServiceError> {
    let Some(raw) = raw.filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let at = parse_time(raw).ok_or_else(|| {
        crate::service_helpers::invalid_parameter_value(format!(
            "Invalid value '{raw}' for PurchaseTime. It must be a UTC timestamp \
             (YYYY-MM-DDTHH:MM:SSZ)."
        ))
    })?;
    let this_second = now.with_nanosecond(0).unwrap_or(now);
    if at < this_second {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "PurchaseTime ({raw}) must not be in the past."
        )));
    }
    if at <= now {
        return Ok(None);
    }
    if zonal {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "InvalidParameterCombination",
            "A purchase can be queued only for a Reserved Instance with a scope of Region.",
        ));
    }
    let horizon = now
        .checked_add_months(chrono::Months::new(36))
        .unwrap_or(now);
    if at > horizon {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "PurchaseTime ({raw}) must be no more than three years in the future."
        )));
    }
    Ok(Some(at))
}

/// Buys `InstanceCount` of an offering DescribeReservedInstancesOfferings
/// lists in this Region. Without a `PurchaseTime` ahead the purchase is made
/// at once, as AWS makes it; with one, it is `queued` until that time. Its
/// state is then read off the clock (see `ri_state`).
pub(crate) fn purchase_reserved_instances_offering(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let params = &req.query_params;
    let count_raw = require(params, "InstanceCount")?;
    let offering_id = require(params, "ReservedInstancesOfferingId")?;
    validate_enum(params, "LimitPrice.CurrencyCode", &["USD"])?;
    let count: i64 = count_raw.parse().ok().filter(|n| *n > 0).ok_or_else(|| {
        crate::service_helpers::invalid_parameter_value(format!(
            "Invalid value '{count_raw}' for InstanceCount. It must be a positive integer."
        ))
    })?;
    let region = crate::service::instance::request_region(req).to_string();
    let offering = Offering::from_id(&region, &offering_id).ok_or_else(|| {
        AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "InvalidReservedInstancesOfferingId",
            format!("The Reserved Instances offering '{offering_id}' does not exist."),
        )
    })?;
    // The total order is the offering's upfront price for every instance.
    let fixed_price: f64 = FIXED_PRICE.parse().unwrap_or(0.0);
    let total = fixed_price * count as f64;
    if let Some(raw) = params.get("LimitPrice.Amount").filter(|v| !v.is_empty()) {
        let limit: f64 = raw.parse().map_err(|_| {
            crate::service_helpers::invalid_parameter_value(format!(
                "Invalid value '{raw}' for LimitPrice.Amount."
            ))
        })?;
        if total > limit {
            return Err(AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "LimitPriceExceeded",
                format!(
                    "The cost of the total order ({total}) is greater than the specified limit \
                     price ({limit})."
                ),
            ));
        }
    }
    let now = chrono::Utc::now();
    let queued_at = queued_purchase_time(params.get("PurchaseTime"), offering.zone.is_some(), now)?;
    crate::service_helpers::refuse_dry_run(params)?;
    let id = uuid::Uuid::new_v4().to_string();
    let start = queued_at.unwrap_or(now);
    let end = start + chrono::Duration::seconds(offering.duration);
    let stamp =
        |t: chrono::DateTime<chrono::Utc>| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    svc.state
        .write()
        .regional_mut(&req.account_id, &req.region)
        .reserved_instances
        .insert(
            id.clone(),
            ReservedInstances {
                id: id.clone(),
                instance_type: offering.instance_type.to_string(),
                availability_zone: offering.zone.clone().unwrap_or_default(),
                instance_count: count,
                product_description: offering.product.to_string(),
                state: if queued_at.is_some() {
                    "queued"
                } else {
                    "payment-pending"
                }
                .to_string(),
                duration: offering.duration,
                fixed_price: FIXED_PRICE.to_string(),
                usage_price: USAGE_PRICE.to_string(),
                scope: scope_of(offering.zone.as_deref()).to_string(),
                offering_class: offering.class.to_string(),
                offering_type: offering.offering_type.to_string(),
                instance_tenancy: offering.tenancy.to_string(),
                start: stamp(start),
                end: stamp(end),
            },
        );
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
            .regional_mut(&req.account_id, &req.region)
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
    let state = accounts.regional_mut(&req.account_id, &req.region);
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let mut items: Vec<String> = state
        .reserved_instances_listings
        .values()
        .filter(|l| want_listing.map(|w| w == &l.listing_id).unwrap_or(true))
        .filter(|l| {
            want_ri
                .map(|w| w == &l.reserved_instances_id)
                .unwrap_or(true)
        })
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
            .regional_mut(&req.account_id, &req.region)
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

/// Cancels queued purchases. Each id is deleted only while still queued: it
/// then reads `queued-deleted` and is never made. Any other is reported back
/// per item with the modelled codes (`reserved-instances-id-invalid` for no
/// such reservation, `reserved-instances-not-in-queued-state` for one already
/// made or deleted); the messages are inferred.
pub(crate) fn delete_queued_reserved_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "ReservedInstancesId");
    if ids.is_empty() {
        return Err(crate::service_helpers::missing_parameter(
            "ReservedInstancesId",
        ));
    }
    crate::service_helpers::refuse_dry_run(&req.query_params)?;
    let now = chrono::Utc::now();
    let mut accounts = svc.state.write();
    let mut state = accounts.regional_get_mut(&req.account_id, &req.region);
    let mut success = Vec::new();
    let mut failed = Vec::new();
    for id in &ids {
        let failure = match state
            .as_mut()
            .and_then(|s| s.reserved_instances.get_mut(id))
        {
            None => Some((
                "reserved-instances-id-invalid",
                format!("The Reserved Instances ID '{id}' does not exist."),
            )),
            Some(r) => match ri_state(r, now) {
                "queued" => {
                    r.state = "queued-deleted".to_string();
                    None
                }
                current => Some((
                    "reserved-instances-not-in-queued-state",
                    format!(
                        "The Reserved Instances '{id}' cannot be deleted because they are in the \
                         {current} state, not queued."
                    ),
                )),
            },
        };
        match failure {
            None => success.push(ec2_elem("reservedInstancesId", id)),
            Some((code, message)) => failed.push(format!(
                "<error>{}{}</error>{}",
                ec2_elem("code", code),
                ec2_elem("message", &message),
                ec2_elem("reservedInstancesId", id)
            )),
        }
    }
    let body = format!(
        "{}{}",
        ec2_list("successfulQueuedPurchaseDeletionSet", &success),
        ec2_list("failedQueuedPurchaseDeletionSet", &failed)
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(r: Result<AwsResponse, AwsServiceError>) -> String {
        match r {
            Ok(resp) => String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap(),
            Err(e) => panic!("expected success, got {}: {}", e.code(), e.message()),
        }
    }

    fn elems(out: &str, name: &str) -> Vec<String> {
        out.split(&format!("<{name}>"))
            .skip(1)
            .map(|s| s.split('<').next().unwrap().to_string())
            .collect()
    }

    fn offering_ids(svc: &Ec2Service, params: &[(&str, &str)]) -> Vec<String> {
        let out = body(describe_reserved_instances_offerings(
            svc,
            &req("DescribeReservedInstancesOfferings", params),
        ));
        elems(&out, "reservedInstancesOfferingId")
    }

    fn purchase(svc: &Ec2Service, params: &[(&str, &str)]) -> Result<AwsResponse, AwsServiceError> {
        purchase_reserved_instances_offering(svc, &req("PurchaseReservedInstancesOffering", params))
    }

    fn describe(svc: &Ec2Service) -> String {
        body(describe_reserved_instances(
            svc,
            &req("DescribeReservedInstances", &[]),
        ))
    }

    #[test]
    fn offering_ids_are_uuids_that_resolve_to_their_offering() {
        let offering = Offering {
            instance_type: "m5.large",
            zone: Some("us-east-1d".into()),
            product: "Windows",
            class: "standard",
            offering_type: "Partial Upfront",
            tenancy: "dedicated",
            duration: 94_608_000,
        };
        let id = offering.id("us-east-1");
        let parsed = uuid::Uuid::parse_str(&id).unwrap();
        assert_eq!(parsed.get_version_num(), 4, "{id}");
        assert_eq!(parsed.get_variant(), uuid::Variant::RFC4122, "{id}");
        assert_eq!(Offering::from_id("us-east-1", &id), Some(offering.clone()));
        // Another Region, a changed character, or no such offering: none.
        assert_eq!(Offering::from_id("eu-west-1", &id), None);
        let tampered = format!("{}{}", &id[..35], if id.ends_with('0') { '1' } else { '0' });
        assert_eq!(Offering::from_id("us-east-1", &tampered), None);
        let unsold = Offering {
            zone: Some("us-east-1e".into()),
            ..offering.clone()
        };
        assert_eq!(
            Offering::from_id("us-east-1", &unsold.id("us-east-1")),
            None
        );
        let zonal_convertible = Offering {
            class: "convertible",
            ..offering
        };
        assert_eq!(
            Offering::from_id("us-east-1", &zonal_convertible.id("us-east-1")),
            None
        );
    }

    #[test]
    fn a_purchase_records_the_offering_it_buys() {
        let svc = Ec2Service::new();
        let ids = offering_ids(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("AvailabilityZone", "us-east-1d"),
                ("ProductDescription", "Windows"),
                ("OfferingType", "Partial Upfront"),
                ("InstanceTenancy", "dedicated"),
                ("MinDuration", "94608000"),
            ],
        );
        assert_eq!(ids.len(), 1, "{ids:?}");
        let ri = body(purchase(
            &svc,
            &[
                ("ReservedInstancesOfferingId", &ids[0]),
                ("InstanceCount", "3"),
                ("LimitPrice.Amount", "0"),
                ("LimitPrice.CurrencyCode", "USD"),
            ],
        ));
        let ri_id = elems(&ri, "reservedInstancesId").remove(0);
        assert!(uuid::Uuid::parse_str(&ri_id).is_ok(), "{ri_id}");

        // Pending until its payment settles, then active.
        let out = describe(&svc);
        assert_eq!(elems(&out, "state"), ["payment-pending"], "{out}");
        for (name, value) in [
            ("reservedInstancesId", ri_id.as_str()),
            ("instanceType", "m5.large"),
            ("availabilityZone", "us-east-1d"),
            ("instanceCount", "3"),
            ("productDescription", "Windows"),
            ("duration", "94608000"),
            ("offeringClass", "standard"),
            ("offeringType", "Partial Upfront"),
            ("instanceTenancy", "dedicated"),
            ("scope", "Availability Zone"),
            ("currencyCode", "USD"),
        ] {
            assert_eq!(elems(&out, name), [value], "{name}: {out}");
        }
        let parse =
            |name: &str| chrono::DateTime::parse_from_rfc3339(&elems(&out, name)[0]).unwrap();
        assert_eq!((parse("end") - parse("start")).num_seconds(), 94_608_000);
        let accounts = svc.state.read();
        let r = &accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .reserved_instances[&ri_id];
        let start = parse_time(&r.start).unwrap();
        assert_eq!(
            ri_state(
                r,
                start + chrono::Duration::seconds(PAYMENT_PENDING_SECONDS)
            ),
            "active"
        );
    }

    #[test]
    fn a_region_scoped_purchase_has_no_zone() {
        let svc = Ec2Service::new();
        let ids = offering_ids(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MaxDuration", "31536000"),
            ],
        );
        assert_eq!(ids.len(), 1, "{ids:?}");
        body(purchase(
            &svc,
            &[
                ("ReservedInstancesOfferingId", &ids[0]),
                ("InstanceCount", "1"),
            ],
        ));
        let out = describe(&svc);
        assert!(!out.contains("<availabilityZone>"), "{out}");
        assert_eq!(elems(&out, "scope"), ["Region"]);
        assert_eq!(elems(&out, "offeringClass"), ["convertible"]);
        assert_eq!(elems(&out, "duration"), ["31536000"]);
    }

    #[test]
    fn a_purchase_refuses_what_it_cannot_buy() {
        let svc = Ec2Service::new();
        let id = offering_ids(&svc, &[("InstanceType", "t3.micro")]).remove(0);
        let refused = |params: &[(&str, &str)]| {
            let e = err_of(purchase(&svc, params));
            (e.code().to_string(), e.message())
        };
        for bogus in ["offering-1", "4b2293b4-5813-4cc8-9ce3-1957f0000000"] {
            assert_eq!(
                refused(&[
                    ("ReservedInstancesOfferingId", bogus),
                    ("InstanceCount", "1")
                ]),
                (
                    "InvalidReservedInstancesOfferingId".to_string(),
                    format!("The Reserved Instances offering '{bogus}' does not exist.")
                )
            );
        }
        assert_eq!(
            refused(&[
                ("ReservedInstancesOfferingId", &id),
                ("InstanceCount", "2"),
                ("LimitPrice.Amount", "-1"),
            ])
            .0,
            "LimitPriceExceeded"
        );
        assert_eq!(
            refused(&[("ReservedInstancesOfferingId", &id), ("InstanceCount", "0")]).0,
            "InvalidParameterValue"
        );
        assert_eq!(
            refused(&[
                ("ReservedInstancesOfferingId", &id),
                ("InstanceCount", "1"),
                ("DryRun", "true"),
            ])
            .0,
            "DryRunOperation"
        );
        // None of them bought anything.
        assert!(!describe(&svc).contains("<item>"));
    }

    fn region_offering(svc: &Ec2Service) -> String {
        offering_ids(
            svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MaxDuration", "31536000"),
            ],
        )
        .remove(0)
    }

    fn stamp(t: chrono::DateTime<chrono::Utc>) -> String {
        t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }

    fn queue(svc: &Ec2Service, offering: &str, at: &str) -> Result<AwsResponse, AwsServiceError> {
        purchase(
            svc,
            &[
                ("ReservedInstancesOfferingId", offering),
                ("InstanceCount", "1"),
                ("PurchaseTime", at),
            ],
        )
    }

    #[test]
    fn a_queued_purchase_is_made_at_its_time() {
        let svc = Ec2Service::new();
        let offering = region_offering(&svc);
        let at = chrono::Utc::now() + chrono::Duration::days(1);
        let ri = body(queue(&svc, &offering, &stamp(at)));
        let id = elems(&ri, "reservedInstancesId").remove(0);
        let out = describe(&svc);
        assert_eq!(elems(&out, "state"), ["queued"], "{out}");
        let start = chrono::DateTime::parse_from_rfc3339(&elems(&out, "start")[0]).unwrap();
        let end = chrono::DateTime::parse_from_rfc3339(&elems(&out, "end")[0]).unwrap();
        assert_eq!(start.timestamp(), at.timestamp());
        assert_eq!((end - start).num_seconds(), 31_536_000);
        // Still queued on another read before its time.
        assert_eq!(elems(&describe(&svc), "state"), ["queued"]);
        // Once the clock reaches it, it is paid for, active, then retired.
        {
            let accounts = svc.state.read();
            let r = &accounts
                .regional("000000000000", "us-east-1")
                .unwrap()
                .reserved_instances[&id];
            let start = parse_time(&r.start).unwrap();
            let end = parse_time(&r.end).unwrap();
            let at = |t| ri_state(r, t);
            assert_eq!(at(start - chrono::Duration::seconds(1)), "queued");
            assert_eq!(at(start), "payment-pending");
            assert_eq!(
                at(start + chrono::Duration::seconds(PAYMENT_PENDING_SECONDS)),
                "active"
            );
            assert_eq!(at(end - chrono::Duration::seconds(1)), "active");
            assert_eq!(at(end), "retired");
        }
        // A time within the current second is a purchase made now.
        let now = chrono::Utc::now();
        assert_eq!(
            queued_purchase_time(Some(&stamp(now)), false, now).unwrap(),
            None
        );
    }

    #[test]
    fn a_purchase_time_is_validated() {
        let svc = Ec2Service::new();
        let offering = region_offering(&svc);
        let code = |r: Result<AwsResponse, AwsServiceError>| err_of(r).code().to_string();
        let now = chrono::Utc::now();
        assert_eq!(
            code(queue(
                &svc,
                &offering,
                &stamp(now - chrono::Duration::hours(1))
            )),
            "InvalidParameterValue"
        );
        assert_eq!(
            code(queue(
                &svc,
                &offering,
                &stamp(now + chrono::Duration::days(3 * 365 + 2))
            )),
            "InvalidParameterValue"
        );
        assert_eq!(
            code(queue(&svc, &offering, "tomorrow")),
            "InvalidParameterValue"
        );
        // Up to three years ahead is fine.
        body(queue(
            &svc,
            &offering,
            &stamp(now + chrono::Duration::days(3 * 365 - 1)),
        ));
        // A zonal reservation cannot be queued.
        let zonal = offering_ids(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("AvailabilityZone", "us-east-1a"),
            ],
        )
        .remove(0);
        assert_eq!(
            code(queue(&svc, &zonal, &stamp(now + chrono::Duration::days(1)))),
            "InvalidParameterCombination"
        );
    }

    #[test]
    fn only_queued_purchases_are_deleted() {
        let svc = Ec2Service::new();
        let offering = region_offering(&svc);
        let at = chrono::Utc::now() + chrono::Duration::days(1);
        let queued = elems(
            &body(queue(&svc, &offering, &stamp(at))),
            "reservedInstancesId",
        )
        .remove(0);
        let made = elems(
            &body(purchase(
                &svc,
                &[
                    ("ReservedInstancesOfferingId", &offering),
                    ("InstanceCount", "1"),
                ],
            )),
            "reservedInstancesId",
        )
        .remove(0);
        let delete = |ids: &[&str]| {
            let params: Vec<(String, &str)> = ids
                .iter()
                .enumerate()
                .map(|(i, id)| (format!("ReservedInstancesId.{}", i + 1), *id))
                .collect();
            let params: Vec<(&str, &str)> = params.iter().map(|(k, v)| (k.as_str(), *v)).collect();
            body(delete_queued_reserved_instances(
                &svc,
                &req("DeleteQueuedReservedInstances", &params),
            ))
        };
        let out = delete(&[&queued, &made, "e5a2ff3b-7d14-494f-90af-0b5d00000000"]);
        let (ok, failed) = out.split_once("<failedQueuedPurchaseDeletionSet>").unwrap();
        assert_eq!(elems(ok, "reservedInstancesId"), [queued.as_str()]);
        assert_eq!(
            elems(failed, "code"),
            [
                "reserved-instances-not-in-queued-state",
                "reserved-instances-id-invalid"
            ]
        );
        assert_eq!(
            elems(failed, "reservedInstancesId"),
            [made.as_str(), "e5a2ff3b-7d14-494f-90af-0b5d00000000"]
        );
        // Deleted, it is never made, and cannot be deleted twice.
        {
            let accounts = svc.state.read();
            let r = &accounts
                .regional("000000000000", "us-east-1")
                .unwrap()
                .reserved_instances[&queued];
            assert_eq!(
                ri_state(r, at + chrono::Duration::days(1)),
                "queued-deleted"
            );
        }
        let again = delete(&[&queued]);
        assert_eq!(
            elems(&again, "code"),
            ["reserved-instances-not-in-queued-state"]
        );
    }

    #[test]
    fn describe_applies_the_offering_class_and_type() {
        let svc = Ec2Service::new();
        let convertible = region_offering(&svc);
        let standard = offering_ids(
            &svc,
            &[
                ("InstanceType", "t3.micro"),
                ("OfferingType", "All Upfront"),
            ],
        )
        .remove(0);
        for offering in [&convertible, &standard] {
            body(purchase(
                &svc,
                &[
                    ("ReservedInstancesOfferingId", offering),
                    ("InstanceCount", "1"),
                ],
            ));
        }
        let classes = |params: &[(&str, &str)]| {
            elems(
                &body(describe_reserved_instances(
                    &svc,
                    &req("DescribeReservedInstances", params),
                )),
                "instanceType",
            )
        };
        assert_eq!(classes(&[("OfferingClass", "convertible")]), ["m5.large"]);
        assert_eq!(classes(&[("OfferingClass", "standard")]), ["t3.micro"]);
        assert_eq!(classes(&[("OfferingType", "All Upfront")]), ["t3.micro"]);
        assert!(classes(&[("OfferingType", "Partial Upfront")]).is_empty());
    }

    #[test]
    fn describe_never_creates_a_region() {
        let svc = Ec2Service::new();
        let mut r = req("DescribeReservedInstances", &[]);
        r.region = "eu-north-1".into();
        let out = body(describe_reserved_instances(&svc, &r));
        assert!(!out.contains("<item>"));
        assert!(svc
            .state
            .read()
            .regional("000000000000", "eu-north-1")
            .is_none());
    }

    #[test]
    fn an_unparseable_duration_bound_is_refused() {
        let svc = Ec2Service::new();
        for name in ["MinDuration", "MaxDuration"] {
            let e = err_of(describe_reserved_instances_offerings(
                &svc,
                &req("DescribeReservedInstancesOfferings", &[(name, "a year")]),
            ));
            assert_eq!(
                (e.code(), e.message()),
                (
                    "InvalidParameterValue",
                    format!("Invalid value 'a year' for {name}.")
                )
            );
        }
    }

    #[test]
    fn every_zone_round_trips_through_an_offering_id() {
        // The catalog's zones are letter-suffixed only (no Local Zones), so the
        // zone is checked by index: every zone of a few Regions, and an index
        // past the last one.
        for region in ["us-east-1", "eu-west-1", "ap-northeast-1", "sa-east-1"] {
            let zones = crate::defaults::region_zones(region);
            assert!(!zones.is_empty(), "{region}");
            let mut checked = 0;
            for (zone, _) in &zones {
                let offering = Offering {
                    instance_type: "t3.micro",
                    zone: Some(zone.clone()),
                    product: "Linux/UNIX",
                    class: "standard",
                    offering_type: "No Upfront",
                    tenancy: "default",
                    duration: 31_536_000,
                };
                if !offering.listed(region) {
                    continue;
                }
                assert_eq!(
                    Offering::from_id(region, &offering.id(region)),
                    Some(offering.clone()),
                    "{zone}"
                );
                checked += 1;
            }
            assert!(checked >= 3, "{region}: {checked}");
            // A slot one past the Region's zones decodes to nothing.
            let past = Offering {
                instance_type: "t3.micro",
                zone: None,
                product: "Linux/UNIX",
                class: "standard",
                offering_type: "No Upfront",
                tenancy: "default",
                duration: 31_536_000,
            };
            let id = past.id(region);
            let mut bytes = *uuid::Uuid::parse_str(&id).unwrap().as_bytes();
            let check: [u8; 8] = bytes[..8].try_into().unwrap();
            let mask = Offering::mask(&check);
            bytes[10] = u8::try_from(zones.len() + 1).unwrap() ^ mask[2];
            let forged = uuid::Uuid::from_bytes(bytes).to_string();
            assert_eq!(Offering::from_id(region, &forged), None, "{region}");
        }
    }
}
