//! Reserved Instances (offerings, purchases, listings, modifications,
//! exchange quotes) and Dedicated Hosts.

use chrono::Timelike;
use fakecloud_aws::ec2query::{ec2_elem, ec2_elem_opt, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, indexed_list, require, validate_enum, validate_max_results};
use crate::state::{
    DedicatedHost, Ec2State, ListingPriceSchedule, ReservedInstances, ReservedInstancesListing,
    ReservedInstancesModification, ReservedInstancesModificationResult, Tag,
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
                .filter(|r| ri_visible(r, now))
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
    if r.retired_at
        .as_deref()
        .and_then(parse_time)
        .is_some_and(|t| now >= t)
    {
        return "retired";
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
    let count: i64 = parse_count(&count_raw).ok_or_else(|| {
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
                retired_at: None,
                available_from: None,
            },
        );
    Ok(Ec2Service::respond(
        "PurchaseReservedInstancesOffering",
        &req.request_id,
        &ec2_elem("reservedInstancesId", &id),
    ))
}

// ---- shared by the secondary operations ----

/// An instance count as the model types it: a positive Smithy `Integer`
/// (32-bit), so nothing larger is ever stored or summed.
fn parse_count(raw: &str) -> Option<i64> {
    raw.parse::<i32>().ok().filter(|n| *n > 0).map(i64::from)
}

/// The sum of instance counts, `None` past what an instance count holds.
fn checked_total(counts: impl IntoIterator<Item = i64>) -> Option<i64> {
    counts
        .into_iter()
        .try_fold(0i64, |acc, n| acc.checked_add(n))
        .filter(|t| *t <= i64::from(i32::MAX))
}

type Now = chrono::DateTime<chrono::Utc>;

fn timestamp(t: Now) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn error(code: &str, message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(http::StatusCode::BAD_REQUEST, code, message.into())
}

/// `InvalidInput`, the code the Reserved Instances backends answer most
/// validation with; their messages carry a leading space, as recorded.
fn invalid_input(message: impl Into<String>) -> AwsServiceError {
    error("InvalidInput", message)
}

/// Whether `id` has the shape real EC2 requires of Reserved Instances and
/// listing ids: `\w{8}(-\w{4}){3}-\w{12}`.
fn uuid_shaped(id: &str) -> bool {
    let parts: Vec<&str> = id.split('-').collect();
    parts.len() == 5
        && parts.iter().zip([8, 4, 4, 4, 12]).all(|(part, len)| {
            part.len() == len && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
}

/// The `InvalidInput` real EC2 answers an id that is not UUID-shaped with,
/// naming the member as it does (`reservedInstanceId`,
/// `reservedInstanceListingId`).
fn malformed_id(id: &str, member: &str) -> AwsServiceError {
    invalid_input(format!(
        "1 validation error detected: Value '{id}' at '{member}' failed to satisfy constraint: \
         Member must satisfy regular expression pattern: \\w{{8}}(-\\w{{4}}){{3}}-\\w{{12}}"
    ))
}

/// A reservation is described once the modification making it is fulfilled.
fn ri_visible(r: &ReservedInstances, now: Now) -> bool {
    r.available_from
        .as_deref()
        .and_then(parse_time)
        .is_none_or(|t| now >= t)
}

/// The indexes `1..` of a list of structures sent as `{prefix}.N.member`,
/// up to the first index with no member.
fn struct_indexes(params: &std::collections::HashMap<String, String>, prefix: &str) -> Vec<usize> {
    (1..)
        .take_while(|i| {
            let p = format!("{prefix}.{i}.");
            params.keys().any(|k| k.starts_with(&p))
        })
        .collect()
}

fn member<'a>(
    params: &'a std::collections::HashMap<String, String>,
    prefix: &str,
    i: usize,
    name: &str,
) -> Option<&'a String> {
    params.get(&format!("{prefix}.{i}.{name}"))
}

/// Filters by name, refusing a name the operation does not document as real
/// EC2 does. Values of one filter match any; filters all match.
fn checked_filters(
    params: &std::collections::HashMap<String, String>,
    names: &[&str],
) -> Result<Vec<crate::service_helpers::Filter>, AwsServiceError> {
    let filters = crate::service_helpers::parse_filters(params);
    if let Some(f) = filters.iter().find(|f| !names.contains(&f.name.as_str())) {
        return Err(crate::service::instance::invalid_catalog_filter(&f.name));
    }
    Ok(filters)
}

/// Whether a filter's values match any of `candidates`; a timestamp value
/// also matches the same instant written another way.
fn filter_matches(values: &[String], candidates: &[String]) -> bool {
    values.iter().any(|v| {
        candidates.iter().any(|c| {
            crate::service_helpers::filter_value_matches(v, c)
                || matches!((parse_time(v), parse_time(c)), (Some(a), Some(b)) if a == b)
        })
    })
}

/// Months left in a reservation at `now`: whole calendar months before its
/// end.
fn months_remaining(r: &ReservedInstances, now: Now) -> i64 {
    let Some(end) = parse_time(&r.end) else {
        return 0;
    };
    let mut months = 0;
    while now
        .checked_add_months(chrono::Months::new(months + 1))
        .is_some_and(|t| t <= end)
    {
        months += 1;
    }
    i64::from(months)
}

// ---- listings ----

/// The status a listing reads at `now`: as stored once cancelled, `closed`
/// once its reservation has ended, else `active`.
fn listing_status(
    l: &ReservedInstancesListing,
    ri: Option<&ReservedInstances>,
    now: Now,
) -> &'static str {
    if l.status == "cancelled" {
        "cancelled"
    } else if ri.is_none_or(|r| ri_state(r, now) == "retired") {
        "closed"
    } else {
        "active"
    }
}

/// A listing's price for each month left, from the most months down to one:
/// each month takes the schedule with the fewest months that still covers it,
/// and the month the reservation is in is the active one.
fn expanded_schedules(l: &ReservedInstancesListing, remaining: i64, active: bool) -> Vec<String> {
    let longest = l.price_schedules.iter().map(|s| s.term).max().unwrap_or(0);
    let top = remaining.max(longest).max(1);
    (1..=top)
        .rev()
        .filter_map(|month| {
            let schedule = l
                .price_schedules
                .iter()
                .filter(|s| s.term >= month)
                .min_by_key(|s| s.term)
                .or_else(|| l.price_schedules.iter().max_by_key(|s| s.term))?;
            Some(format!(
                "<term>{month}</term><price>{:?}</price>{}<active>{}</active>",
                schedule.price,
                ec2_elem("currencyCode", &schedule.currency_code),
                active && month == remaining
            ))
        })
        .collect()
}

fn listing_xml(l: &ReservedInstancesListing, state: &Ec2State, now: Now) -> String {
    let ri = state.reserved_instances.get(&l.reserved_instances_id);
    let status = listing_status(l, ri, now);
    let count = |s: &str| {
        format!(
            "<state>{s}</state><instanceCount>{}</instanceCount>",
            match (status, s) {
                ("active", "available") => l.instance_count,
                ("cancelled" | "closed", "cancelled") => l.instance_count,
                _ => 0,
            }
        )
    };
    let update = match (status, ri) {
        ("closed", Some(r)) => r.end.clone(),
        _ => l.update_date.clone(),
    };
    let remaining = ri.map_or(0, |r| months_remaining(r, now));
    format!(
        "{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("reservedInstancesListingId", &l.listing_id),
        ec2_elem("reservedInstancesId", &l.reserved_instances_id),
        ec2_elem("createDate", &l.create_date),
        ec2_elem("updateDate", &update),
        ec2_elem("status", status),
        ec2_elem("statusMessage", &status.to_ascii_uppercase()),
        ec2_list(
            "instanceCounts",
            &["available", "sold", "cancelled", "pending"].map(count)
        ),
        ec2_list(
            "priceSchedules",
            &expanded_schedules(l, remaining, status == "active")
        ),
        super::tags::tag_set_xml(state.tags_for(&l.listing_id)),
        ec2_elem("clientToken", &l.client_token),
    )
}

/// The requested price schedules, validated as recorded from real EC2.
fn price_schedules(
    params: &std::collections::HashMap<String, String>,
) -> Result<Vec<ListingPriceSchedule>, AwsServiceError> {
    const P: &str = "PriceSchedules";
    let indexes = struct_indexes(params, P);
    if indexes.is_empty() {
        return Err(crate::service_helpers::missing_parameter(P));
    }
    let present = |i: usize, name: &str| member(params, P, i, name).filter(|v| !v.is_empty());
    for name in ["Term", "Price"] {
        if indexes.iter().any(|i| present(*i, name).is_none()) {
            return Err(crate::service_helpers::missing_parameter(name));
        }
    }
    let invalid = |value: &str, name: &str, why: &str| {
        crate::service_helpers::invalid_parameter_value(format!(
            "Value ({value}) for parameter {name} is invalid. {why}"
        ))
    };
    let mut out = Vec::new();
    for i in indexes {
        let (term_raw, price_raw) = (present(i, "Term").unwrap(), present(i, "Price").unwrap());
        let term: i64 = term_raw.parse().map_err(|_| {
            invalid(
                term_raw,
                "Term",
                "Pricing term must be a whole number of months.",
            )
        })?;
        if term < 0 {
            return Err(invalid(
                term_raw,
                "Term",
                "Pricing term cannot be negative.",
            ));
        }
        let price: f64 = price_raw
            .parse()
            .ok()
            .filter(|p: &f64| p.is_finite())
            .ok_or_else(|| invalid(price_raw, "Price", "Pricing price value must be a number."))?;
        if price < 0.0 {
            return Err(invalid(
                price_raw,
                "Price",
                "Pricing price value cannot be negative.",
            ));
        }
        let currency = present(i, "CurrencyCode").map_or("USD", String::as_str);
        if currency != "USD" {
            return Err(invalid(
                currency,
                "CurrencyCode",
                "Invalid currency code specified.",
            ));
        }
        out.push(ListingPriceSchedule {
            term,
            price,
            currency_code: currency.to_string(),
        });
    }
    Ok(out)
}

/// The EC2 code for a client token reused with other arguments.
fn idempotent_mismatch() -> AwsServiceError {
    error(
        "IdempotentParameterMismatch",
        "Arguments on this idempotent request are inconsistent with arguments used in previous \
         request(s).",
    )
}

/// Lists instances of an active Standard reservation in the Reserved
/// Instance Marketplace.
///
/// Recorded from real EC2: the missing-parameter, malformed-id, instance count
/// and price schedule refusals, in this order. What real EC2 checks after its
/// seller registration (the account here is always a registered seller) is
/// inferred: an unknown reservation, one that is not an active Standard one,
/// more instances than are left to list, and a schedule outside the months
/// remaining.
pub(crate) fn create_reserved_instances_listing(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let params = &req.query_params;
    let ri_id = require(params, "ReservedInstancesId")?;
    let count_raw = require(params, "InstanceCount")?;
    let mut schedules = price_schedules(params)?;
    let client_token = params.get("ClientToken").cloned().ok_or_else(|| {
        invalid_input(
            "1 validation error detected: Value null at 'clientToken' failed to satisfy \
             constraint: Member must not be null",
        )
    })?;
    if !uuid_shaped(&ri_id) {
        return Err(malformed_id(&ri_id, "reservedInstanceId"));
    }
    let count: i64 = parse_count(&count_raw).ok_or_else(|| {
        crate::service_helpers::invalid_parameter_value(format!(
            "Value ({count_raw}) for parameter InstanceCount is invalid. InstanceCount must be \
             greater than zero."
        ))
    })?;
    schedules.sort_by_key(|s| std::cmp::Reverse(s.term));
    let now = chrono::Utc::now();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    if !client_token.is_empty() {
        if let Some(existing) = state
            .reserved_instances_listings
            .values()
            .find(|l| l.client_token == client_token)
        {
            if existing.reserved_instances_id != ri_id
                || existing.instance_count != count
                || existing.price_schedules != schedules
            {
                return Err(idempotent_mismatch());
            }
            let xml = listing_xml(existing, state, now);
            return Ok(Ec2Service::respond(
                "CreateReservedInstancesListing",
                &req.request_id,
                &ec2_list("reservedInstancesListingsSet", &[xml]),
            ));
        }
    }
    let ri = state
        .reserved_instances
        .get(&ri_id)
        .filter(|r| ri_visible(r, now))
        .ok_or_else(|| {
            error(
                "InvalidReservedInstancesId",
                format!("The Reserved Instances ID '{ri_id}' does not exist."),
            )
        })?;
    if ri.offering_class != "standard" {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "Reserved Instances '{ri_id}' are Convertible. Only Standard Reserved Instances can \
             be sold in the Reserved Instance Marketplace."
        )));
    }
    let current = ri_state(ri, now);
    if current != "active" {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "Reserved Instances '{ri_id}' are {current}, not active, and cannot be listed."
        )));
    }
    if modification_in_progress(state, &ri_id, now) {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "Reserved Instances '{ri_id}' have a modification in progress and cannot be listed."
        )));
    }
    let listed: i64 = state
        .reserved_instances_listings
        .values()
        .filter(|l| l.reserved_instances_id == ri_id)
        .filter(|l| listing_status(l, Some(ri), now) == "active")
        // Earlier versions stored unvalidated counts; never let one free capacity.
        .map(|l| l.instance_count.max(0))
        .fold(0i64, i64::saturating_add);
    let available = ri.instance_count.saturating_sub(listed);
    if count > available {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "Value ({count}) for parameter InstanceCount is invalid. Reserved Instances \
             '{ri_id}' have {available} instances available to list."
        )));
    }
    let remaining = months_remaining(ri, now);
    if remaining < 1 {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "Reserved Instances '{ri_id}' have less than one month remaining and cannot be listed."
        )));
    }
    if let Some(s) = schedules.iter().find(|s| s.term < 1 || s.term > remaining) {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "Value ({}) for parameter Term is invalid. The term must be between 1 and the \
             {remaining} months remaining.",
            s.term
        )));
    }
    if let Some(pair) = schedules.windows(2).find(|w| w[0].term == w[1].term) {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "Value ({}) for parameter Term is invalid. Each term can be priced once.",
            pair[0].term
        )));
    }
    let listing = ReservedInstancesListing {
        listing_id: uuid::Uuid::new_v4().to_string(),
        reserved_instances_id: ri_id,
        instance_count: count,
        client_token,
        status: "active".to_string(),
        status_message: "ACTIVE".to_string(),
        price_schedules: schedules,
        create_date: timestamp(now),
        update_date: timestamp(now),
    };
    let xml = listing_xml(&listing, state, now);
    state
        .reserved_instances_listings
        .insert(listing.listing_id.clone(), listing);
    Ok(Ec2Service::respond(
        "CreateReservedInstancesListing",
        &req.request_id,
        &ec2_list("reservedInstancesListingsSet", &[xml]),
    ))
}

/// Cancels an active listing. The refusals of a missing, malformed or unknown
/// id are recorded from real EC2; that of a listing no longer active is
/// inferred.
pub(crate) fn cancel_reserved_instances_listing(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = req
        .query_params
        .get("ReservedInstancesListingId")
        .filter(|v| !v.is_empty())
        .cloned()
        .ok_or_else(|| crate::service_helpers::missing_parameter("reservedInstancesListingId"))?;
    // A stored listing is looked up before the id format is checked: listings
    // saved by earlier versions carry `ril-` ids and must stay cancellable.
    let unknown = || {
        if uuid_shaped(&id) {
            invalid_input(" Invalid value for 'listingId'")
        } else {
            malformed_id(&id, "reservedInstanceListingId")
        }
    };
    let now = chrono::Utc::now();
    let mut accounts = svc.state.write();
    let state = accounts
        .regional_get_mut(&req.account_id, &req.region)
        .ok_or_else(unknown)?;
    let listing = state
        .reserved_instances_listings
        .get(&id)
        .ok_or_else(unknown)?;
    let status = listing_status(
        listing,
        state.reserved_instances.get(&listing.reserved_instances_id),
        now,
    );
    if status != "active" {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "The Reserved Instances listing '{id}' is {status} and cannot be cancelled."
        )));
    }
    if let Some(l) = state.reserved_instances_listings.get_mut(&id) {
        l.status = "cancelled".to_string();
        l.status_message = "CANCELLED".to_string();
        l.update_date = timestamp(now);
    }
    let xml = listing_xml(&state.reserved_instances_listings[&id], state, now);
    Ok(Ec2Service::respond(
        "CancelReservedInstancesListing",
        &req.request_id,
        &ec2_list("reservedInstancesListingsSet", &[xml]),
    ))
}

const LISTING_FILTERS: [&str; 4] = [
    "reserved-instances-id",
    "reserved-instances-listing-id",
    "status",
    "status-message",
];

pub(crate) fn describe_reserved_instances_listings(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let filters = checked_filters(&req.query_params, &LISTING_FILTERS)?;
    let scalar = |name: &str| req.query_params.get(name).filter(|v| !v.is_empty());
    let (want_listing, want_ri) = (
        scalar("ReservedInstancesListingId"),
        scalar("ReservedInstancesId"),
    );
    let now = chrono::Utc::now();
    let accounts = svc.state.read();
    let mut items: Vec<(String, String)> = accounts
        .regional(&req.account_id, &req.region)
        .map(|state| {
            state
                .reserved_instances_listings
                .values()
                .filter(|l| want_listing.is_none_or(|w| *w == l.listing_id))
                .filter(|l| want_ri.is_none_or(|w| *w == l.reserved_instances_id))
                .filter(|l| {
                    let status = listing_status(
                        l,
                        state.reserved_instances.get(&l.reserved_instances_id),
                        now,
                    );
                    filters.iter().all(|f| {
                        let candidate = match f.name.as_str() {
                            "reserved-instances-id" => l.reserved_instances_id.clone(),
                            "reserved-instances-listing-id" => l.listing_id.clone(),
                            "status" => status.to_string(),
                            _ => status.to_ascii_uppercase(),
                        };
                        filter_matches(&f.values, &[candidate])
                    })
                })
                .map(|l| (l.create_date.clone(), listing_xml(l, state, now)))
                .collect()
        })
        .unwrap_or_default();
    items.sort();
    let items: Vec<String> = items.into_iter().map(|(_, xml)| xml).collect();
    Ok(Ec2Service::respond(
        "DescribeReservedInstancesListings",
        &req.request_id,
        &ec2_list("reservedInstancesListingsSet", &items),
    ))
}

// ---- modifications ----

/// How long a modification reads `processing` before it is `fulfilled`.
/// Inferred: AWS documents the two states but not how long processing takes.
const MODIFICATION_PROCESSING_SECONDS: i64 = 2;

fn modification_status(m: &ReservedInstancesModification, now: Now) -> &'static str {
    match parse_time(&m.fulfilled_at) {
        Some(at) if now < at => "processing",
        _ => "fulfilled",
    }
}

fn modification_in_progress(state: &Ec2State, ri_id: &str, now: Now) -> bool {
    state.reserved_instances_modifications.values().any(|m| {
        m.reserved_instances_ids.iter().any(|i| i == ri_id)
            && modification_status(m, now) == "processing"
    })
}

fn target_configuration_xml(t: &ReservedInstancesModificationResult) -> String {
    format!(
        "{}<instanceCount>{}</instanceCount>{}{}{}{}",
        ec2_elem_opt("availabilityZone", t.availability_zone.as_deref()),
        t.instance_count,
        ec2_elem("instanceType", &t.instance_type),
        ec2_elem_opt("platform", t.platform.as_deref()),
        ec2_elem("scope", &t.scope),
        ec2_elem_opt("availabilityZoneId", t.availability_zone_id.as_deref()),
    )
}

fn modification_xml(m: &ReservedInstancesModification, now: Now) -> String {
    let status = modification_status(m, now);
    let fulfilled = status == "fulfilled";
    let ids: Vec<String> = m
        .reserved_instances_ids
        .iter()
        .map(|id| ec2_elem("reservedInstancesId", id))
        .collect();
    let results: Vec<String> = m
        .results
        .iter()
        .map(|r| {
            format!(
                "{}<targetConfiguration>{}</targetConfiguration>",
                if fulfilled {
                    ec2_elem("reservedInstancesId", &r.reserved_instances_id)
                } else {
                    String::new()
                },
                target_configuration_xml(r)
            )
        })
        .collect();
    format!(
        "{}{}{}{}{}{}{}{}",
        ec2_elem("reservedInstancesModificationId", &m.modification_id),
        ec2_list("reservedInstancesSet", &ids),
        ec2_list("modificationResultSet", &results),
        ec2_elem("createDate", &m.create_date),
        ec2_elem(
            "updateDate",
            if fulfilled {
                &m.fulfilled_at
            } else {
                &m.create_date
            }
        ),
        ec2_elem("effectiveDate", &m.effective_date),
        ec2_elem("status", status),
        ec2_elem("clientToken", &m.client_token),
    )
}

const MODIFICATION_FILTERS: [&str; 13] = [
    "client-token",
    "create-date",
    "effective-date",
    "modification-result.reserved-instances-id",
    "modification-result.target-configuration.availability-zone",
    "modification-result.target-configuration.availability-zone-id",
    "modification-result.target-configuration.instance-count",
    "modification-result.target-configuration.instance-type",
    "reserved-instances-id",
    "reserved-instances-modification-id",
    "status",
    "status-message",
    "update-date",
];

/// What a modification filter compares against.
fn modification_filter_candidates(
    m: &ReservedInstancesModification,
    name: &str,
    now: Now,
) -> Vec<String> {
    let status = modification_status(m, now);
    let targets = |f: fn(&ReservedInstancesModificationResult) -> Option<String>| {
        m.results.iter().filter_map(f).collect()
    };
    match name {
        "client-token" => vec![m.client_token.clone()],
        "create-date" => vec![m.create_date.clone()],
        "effective-date" => vec![m.effective_date.clone()],
        "update-date" => vec![if status == "fulfilled" {
            m.fulfilled_at.clone()
        } else {
            m.create_date.clone()
        }],
        "modification-result.reserved-instances-id" if status == "fulfilled" => {
            targets(|r| Some(r.reserved_instances_id.clone()))
        }
        "modification-result.reserved-instances-id" => Vec::new(),
        "modification-result.target-configuration.availability-zone" => {
            targets(|r| r.availability_zone.clone())
        }
        "modification-result.target-configuration.availability-zone-id" => {
            targets(|r| r.availability_zone_id.clone())
        }
        "modification-result.target-configuration.instance-count" => {
            targets(|r| Some(r.instance_count.to_string()))
        }
        "modification-result.target-configuration.instance-type" => {
            targets(|r| Some(r.instance_type.clone()))
        }
        "reserved-instances-id" => m.reserved_instances_ids.clone(),
        "reserved-instances-modification-id" => vec![m.modification_id.clone()],
        "status" => vec![status.to_string()],
        // A modification that did not fail has no status message.
        _ => vec![String::new()],
    }
}

/// Describes modifications. Recorded from real EC2: an id not starting
/// `rimod-` is refused, an unknown `rimod-` id matches nothing, an unknown
/// filter is refused, and so is any `NextToken` (the operation has no page
/// size, so every answer is whole).
pub(crate) fn describe_reserved_instances_modifications(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let want = indexed_list(&req.query_params, "ReservedInstancesModificationId");
    if let Some(bad) = want.iter().find(|id| !id.starts_with("rimod-")) {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "Invalid value '{bad}' for ReservedInstancesModificationId."
        )));
    }
    let filters = checked_filters(&req.query_params, &MODIFICATION_FILTERS)?;
    if req
        .query_params
        .get("NextToken")
        .is_some_and(|t| !t.is_empty())
    {
        return Err(invalid_input(" Invalid value for 'nextToken'."));
    }
    let now = chrono::Utc::now();
    let accounts = svc.state.read();
    let mut items: Vec<(String, String)> = accounts
        .regional(&req.account_id, &req.region)
        .map(|state| {
            state
                .reserved_instances_modifications
                .values()
                .filter(|m| want.is_empty() || want.contains(&m.modification_id))
                .filter(|m| {
                    filters.iter().all(|f| {
                        filter_matches(&f.values, &modification_filter_candidates(m, &f.name, now))
                    })
                })
                .map(|m| (m.create_date.clone(), modification_xml(m, now)))
                .collect()
        })
        .unwrap_or_default();
    items.sort();
    let items: Vec<String> = items.into_iter().map(|(_, xml)| xml).collect();
    Ok(Ec2Service::respond(
        "DescribeReservedInstancesModifications",
        &req.request_id,
        &ec2_list("reservedInstancesModificationsSet", &items),
    ))
}

/// The normalization factor of an instance size, as EC2 documents it for
/// the instance size flexibility of Reserved Instances (`nano` 0.25 up to
/// `xlarge` 8, then 8 per `xlarge`); `metal` counts as the largest size of
/// its family.
fn size_factor(instance_type: &str) -> Option<f64> {
    let (family, size) = instance_type.split_once('.')?;
    let named = |size: &str| -> Option<f64> {
        Some(match size {
            "nano" => 0.25,
            "micro" => 0.5,
            "small" => 1.0,
            "medium" => 2.0,
            "large" => 4.0,
            "xlarge" => 8.0,
            other => 8.0 * other.strip_suffix("xlarge")?.parse::<f64>().ok()?,
        })
    };
    if let Some(n) = named(size) {
        return Some(n);
    }
    if let Some(n) = size
        .strip_prefix("metal-")
        .and_then(|s| s.strip_suffix("xl"))
        .and_then(|n| n.parse::<f64>().ok())
    {
        return Some(8.0 * n);
    }
    if size == "metal" {
        return crate::instance_types::catalog()
            .iter()
            .filter_map(|t| t.name.split_once('.'))
            .filter(|(f, _)| *f == family)
            .filter_map(|(_, s)| named(s))
            .reduce(f64::max);
    }
    None
}

/// A stored price times `scale`, written as prices are stored.
fn scaled_price(raw: &str, scale: f64) -> String {
    if (scale - 1.0).abs() < f64::EPSILON {
        return raw.to_string();
    }
    let scaled = price(raw) * scale;
    // Rounded to the micro-dollar, so 0.02 * 0.5 reads 0.01.
    format!("{:?}", (scaled * 1e6).round() / 1e6)
}

fn family_of(instance_type: &str) -> &str {
    instance_type
        .split_once('.')
        .map_or(instance_type, |(f, _)| f)
}

/// One requested target configuration, validated.
struct Target {
    zone: Option<String>,
    zone_id: Option<String>,
    count: i64,
    instance_type: Option<String>,
    platform: Option<String>,
    scope: &'static str,
}

/// The target configurations of a modification, validated in the order real
/// EC2 was recorded validating them.
fn modification_targets(
    params: &std::collections::HashMap<String, String>,
    region: &str,
) -> Result<Vec<Target>, AwsServiceError> {
    const P: &str = "ReservedInstancesConfigurationSetItemType";
    let indexes = struct_indexes(params, P);
    if indexes.is_empty() {
        return Err(error(
            "MissingParameter",
            "TargetReservedInstancesConfigurationSet",
        ));
    }
    let get = |i: usize, name: &str| {
        member(params, P, i, name)
            .filter(|v| !v.is_empty())
            .cloned()
    };
    if indexes.iter().any(|i| get(*i, "InstanceCount").is_none()) {
        return Err(error("MissingParameter", "InstanceCount"));
    }
    let invalid = |value: &str, name: &str| {
        crate::service_helpers::invalid_parameter_value(format!(
            "Invalid value '{value}' for {name}."
        ))
    };
    let zones = crate::defaults::region_zones(region);
    let mut out = Vec::new();
    for i in indexes {
        let count_raw = get(i, "InstanceCount").unwrap_or_default();
        let count: i64 = parse_count(&count_raw).ok_or_else(|| {
            crate::service_helpers::invalid_parameter_value(format!(
                "Invalid value '{count_raw}' for InstanceCount. Instance count must be greater \
                 than zero."
            ))
        })?;
        let instance_type = get(i, "InstanceType");
        if let Some(t) = &instance_type {
            if crate::instance_types::lookup(t).is_none() {
                return Err(invalid(t, "InstanceType"));
            }
        }
        let zone = get(i, "AvailabilityZone");
        if let Some(z) = &zone {
            if !zones.iter().any(|(name, _)| name == z) {
                return Err(invalid(z, "AvailabilityZone"));
            }
        }
        let zone_id = get(i, "AvailabilityZoneId");
        if let Some(z) = &zone_id {
            if !zones.iter().any(|(_, id)| id == z) {
                return Err(invalid(z, "AvailabilityZoneId"));
            }
        }
        let scope = match get(i, "Scope").as_deref() {
            None | Some("Availability Zone") => "Availability Zone",
            Some("Region") => "Region",
            Some(other) => return Err(invalid(other, "Scope")),
        };
        let platform = get(i, "Platform");
        if let Some(p) = &platform {
            if p != "EC2-Classic" && p != "EC2-VPC" {
                return Err(invalid(p, "Platform"));
            }
        }
        if zone.is_some() && zone_id.is_some() {
            return Err(error(
                "InvalidParameterCombination",
                "Filters can be applied on either 'availability-zone' or 'availability-zone-id', \
                 but not both.",
            ));
        }
        out.push(Target {
            zone,
            zone_id,
            count,
            instance_type,
            platform,
            scope,
        });
    }
    Ok(out)
}

/// Modifies Reserved Instances into the target configurations. The request is
/// `processing` for a moment, then `fulfilled`: the reservations modified are
/// retired and one reservation per target configuration is made, from the
/// start of the hour the request was made in to their end, as EC2 documents.
///
/// Recorded from real EC2: every refusal up to an unknown reservation, in this
/// order. Inferred: the checks after it (the reservations must be active,
/// identical but for zone, platform and type, not listed or being modified,
/// and keep their instance size footprint within one family).
pub(crate) fn modify_reserved_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let params = &req.query_params;
    let ids = indexed_list(params, "ReservedInstancesId");
    if ids.is_empty() {
        return Err(error("MissingParameter", "ReservedInstancesSet"));
    }
    let region = crate::service::instance::request_region(req).to_string();
    let targets_result = modification_targets(params, &region);
    // A missing target set is reported before a malformed id, a bad target
    // value after it.
    let targets = match targets_result {
        Err(e) if e.code() == "MissingParameter" => return Err(e),
        other => {
            if let Some(bad) = ids.iter().find(|id| !uuid_shaped(id)) {
                return Err(crate::service_helpers::invalid_parameter_value(format!(
                    "Invalid value '{bad}' for ReservedInstancesId."
                )));
            }
            other?
        }
    };
    for (i, id) in ids.iter().enumerate() {
        if ids[..i].contains(id) {
            return Err(invalid_input(format!(
                " Invalid value for 'reservedInstancesIds'. Reserved Instance {id} is a duplicate."
            )));
        }
    }
    for t in &targets {
        if t.scope == "Region" && (t.zone.is_some() || t.zone_id.is_some()) {
            return Err(invalid_input(
                " Invalid value for 'Availability Zone', Invalid value for 'availabilityZone'. \
                 Can't be specified with 'scope'!='Availability Zone'",
            ));
        }
        if t.scope != "Region" && t.zone.is_none() && t.zone_id.is_none() {
            return Err(error(
                "MissingParameter",
                "The request must contain the parameter  AvailabilityZone",
            ));
        }
    }
    let client_token = params.get("ClientToken").cloned().unwrap_or_default();
    let now = chrono::Utc::now();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let zones = crate::defaults::region_zones(&region);
    if !client_token.is_empty() {
        if let Some(existing) = state
            .reserved_instances_modifications
            .values()
            .find(|m| m.client_token == client_token)
        {
            if existing.reserved_instances_ids != ids {
                return Err(idempotent_mismatch());
            }
            return Ok(Ec2Service::respond(
                "ModifyReservedInstances",
                &req.request_id,
                &ec2_elem("reservedInstancesModificationId", &existing.modification_id),
            ));
        }
    }
    let mut sources = Vec::new();
    for id in &ids {
        let r = state
            .reserved_instances
            .get(id)
            .filter(|r| ri_visible(r, now))
            .ok_or_else(|| {
                invalid_input(format!(
                    " Invalid value for 'reservedInstancesIds'. Reserved Instance {id} is not found."
                ))
            })?;
        sources.push(r.clone());
    }
    let refuse = |id: &str, why: &str| {
        invalid_input(format!(
            " Invalid value for 'reservedInstancesIds'. Reserved Instance {id} {why}."
        ))
    };
    for r in &sources {
        let current = ri_state(r, now);
        if current != "active" {
            return Err(refuse(&r.id, &format!("is {current}, not active")));
        }
        if modification_in_progress(state, &r.id, now) {
            return Err(refuse(&r.id, "has a modification in progress"));
        }
        if state
            .reserved_instances_listings
            .values()
            .any(|l| l.reserved_instances_id == r.id && listing_status(l, Some(r), now) == "active")
        {
            return Err(refuse(
                &r.id,
                "is listed in the Reserved Instance Marketplace",
            ));
        }
    }
    let first = sources[0].clone();
    let end_hour = |r: &ReservedInstances| parse_time(&r.end).map(|t| t.timestamp() / 3600);
    if let Some(r) = sources.iter().find(|r| {
        r.offering_class != first.offering_class
            || r.offering_type != first.offering_type
            || r.product_description != first.product_description
            || r.instance_tenancy != first.instance_tenancy
            || end_hour(r) != end_hour(&first)
            || family_of(&r.instance_type) != family_of(&first.instance_type)
    }) {
        return Err(refuse(
            &r.id,
            &format!(
                "differs from {}: the Reserved Instances to modify must be identical, except for \
                 Availability Zone, network platform, and instance type",
                first.id
            ),
        ));
    }
    let types_change = targets.iter().any(|t| {
        t.instance_type
            .as_ref()
            .is_some_and(|ty| *ty != first.instance_type)
    }) || sources
        .iter()
        .any(|r| r.instance_type != first.instance_type);
    if types_change {
        if !first.product_description.starts_with("Linux/UNIX")
            || first.instance_tenancy != "default"
        {
            return Err(crate::service_helpers::invalid_parameter_value(
                "The instance type can be modified only for Linux/UNIX Reserved Instances with \
                 default tenancy.",
            ));
        }
        if let Some(ty) = targets
            .iter()
            .filter_map(|t| t.instance_type.as_deref())
            .find(|ty| family_of(ty) != family_of(&first.instance_type))
        {
            return Err(crate::service_helpers::invalid_parameter_value(format!(
                "Invalid value '{ty}' for InstanceType. The instance type can be modified only \
                 within the {} family.",
                family_of(&first.instance_type)
            )));
        }
        let footprint = |ty: &str, count: i64| size_factor(ty).map(|f| f * count as f64);
        let have: Option<f64> = sources
            .iter()
            .map(|r| footprint(&r.instance_type, r.instance_count))
            .sum();
        let want: Option<f64> = targets
            .iter()
            .map(|t| {
                footprint(
                    t.instance_type.as_deref().unwrap_or(&first.instance_type),
                    t.count,
                )
            })
            .sum();
        match (have, want) {
            (Some(h), Some(w)) if (h - w).abs() < 1e-9 => {}
            (h, w) => {
                return Err(crate::service_helpers::invalid_parameter_value(format!(
                    "The instance size footprint of the target configurations ({}) must equal \
                     that of the Reserved Instances ({}).",
                    w.map_or_else(|| "unknown".to_string(), |w| w.to_string()),
                    h.map_or_else(|| "unknown".to_string(), |h| h.to_string()),
                )));
            }
        }
    } else {
        let have = checked_total(sources.iter().map(|r| r.instance_count));
        let want = checked_total(targets.iter().map(|t| t.count));
        if have.is_none() || have != want {
            let (have, want) = (have.unwrap_or(0), want.unwrap_or(0));
            return Err(crate::service_helpers::invalid_parameter_value(format!(
                "The target configurations hold {want} instances; the Reserved Instances hold \
                 {have}. A modification keeps the instance count."
            )));
        }
    }
    let mut results = Vec::new();
    for t in &targets {
        let instance_type = t
            .instance_type
            .clone()
            .unwrap_or_else(|| first.instance_type.clone());
        let zone = t.zone.clone().or_else(|| {
            t.zone_id
                .as_ref()
                .and_then(|id| zones.iter().find(|(_, z)| z == id).map(|(n, _)| n.clone()))
        });
        if let Some(z) = &zone {
            if crate::instance_types::lookup(&instance_type)
                .is_some_and(|spec| !spec.offered_in_zone(&region, z))
            {
                return Err(crate::service_helpers::invalid_parameter_value(format!(
                    "Invalid value '{z}' for AvailabilityZone. {instance_type} is not offered in \
                     {z}."
                )));
            }
        }
        results.push(ReservedInstancesModificationResult {
            reserved_instances_id: uuid::Uuid::new_v4().to_string(),
            availability_zone_id: zone.as_deref().map(crate::defaults::zone_id_for),
            availability_zone: zone,
            instance_count: t.count,
            instance_type,
            platform: t.platform.clone(),
            scope: t.scope.to_string(),
        });
    }
    let effective = now
        .with_minute(0)
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_nanosecond(0))
        .unwrap_or(now);
    let fulfilled_at = timestamp(now + chrono::Duration::seconds(MODIFICATION_PROCESSING_SECONDS));
    for r in &sources {
        if let Some(stored) = state.reserved_instances.get_mut(&r.id) {
            stored.retired_at = Some(fulfilled_at.clone());
        }
    }
    let start = timestamp(effective);
    // The reservations keep their term; only when they run changes.
    let duration = first.duration;
    for result in &results {
        // A per-instance price follows the instance's size: half a source
        // instance costs half as much.
        let scale = match (
            size_factor(&result.instance_type),
            size_factor(&first.instance_type),
        ) {
            (Some(to), Some(from)) if from > 0.0 => to / from,
            _ => 1.0,
        };
        state.reserved_instances.insert(
            result.reserved_instances_id.clone(),
            ReservedInstances {
                id: result.reserved_instances_id.clone(),
                instance_type: result.instance_type.clone(),
                availability_zone: if result.scope == "Region" {
                    String::new()
                } else {
                    result.availability_zone.clone().unwrap_or_default()
                },
                instance_count: result.instance_count,
                product_description: first.product_description.clone(),
                state: "active".to_string(),
                duration,
                fixed_price: scaled_price(&first.fixed_price, scale),
                usage_price: scaled_price(&first.usage_price, scale),
                scope: result.scope.clone(),
                offering_class: first.offering_class.clone(),
                offering_type: first.offering_type.clone(),
                instance_tenancy: first.instance_tenancy.clone(),
                start: start.clone(),
                end: first.end.clone(),
                retired_at: None,
                available_from: Some(fulfilled_at.clone()),
            },
        );
    }
    let modification = ReservedInstancesModification {
        modification_id: format!("rimod-{}", uuid::Uuid::new_v4()),
        reserved_instances_ids: ids,
        status: "processing".to_string(),
        client_token,
        create_date: timestamp(now),
        effective_date: start,
        fulfilled_at,
        results,
    };
    let id = modification.modification_id.clone();
    state
        .reserved_instances_modifications
        .insert(id.clone(), modification);
    Ok(Ec2Service::respond(
        "ModifyReservedInstances",
        &req.request_id,
        &ec2_elem("reservedInstancesModificationId", &id),
    ))
}

// ---- exchanges ----

/// A reservation's value as an exchange quote reports it: the upfront price
/// left for the time remaining, the hourly price, and their sum over the
/// hours remaining.
#[derive(Clone, Copy, Default)]
struct Value {
    upfront: f64,
    hourly: f64,
    total: f64,
}

impl Value {
    fn of(fixed: f64, usage: f64, count: i64, remaining_secs: f64, duration_secs: f64) -> Value {
        let share = if duration_secs > 0.0 {
            (remaining_secs / duration_secs).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let upfront = fixed * share * count as f64;
        let hourly = usage * count as f64;
        Value {
            upfront,
            hourly,
            total: upfront + hourly * remaining_secs / 3600.0,
        }
    }

    fn add(self, other: Value) -> Value {
        Value {
            upfront: self.upfront + other.upfront,
            hourly: self.hourly + other.hourly,
            total: self.total + other.total,
        }
    }

    fn xml(&self, name: &str) -> String {
        format!(
            "<{name}>{}{}{}</{name}>",
            ec2_elem("hourlyPrice", &money(self.hourly)),
            ec2_elem("remainingTotalValue", &money(self.total)),
            ec2_elem("remainingUpfrontValue", &money(self.upfront)),
        )
    }
}

/// Money as the exchange quotes write it: six decimals.
fn money(v: f64) -> String {
    format!("{v:.6}")
}

struct QuoteTarget {
    offering_id: String,
    offering: Offering,
    count: i64,
    value: Value,
}

/// An exchange quote: the reservations, the targets, and whether the
/// exchange can be made (`failure` says why not).
struct Quote {
    sources: Vec<(ReservedInstances, Value)>,
    targets: Vec<QuoteTarget>,
    expires: Option<Now>,
    failure: Option<String>,
}

impl Quote {
    fn source_value(&self) -> Value {
        self.sources
            .iter()
            .fold(Value::default(), |acc, (_, v)| acc.add(*v))
    }

    fn target_value(&self) -> Value {
        self.targets
            .iter()
            .fold(Value::default(), |acc, t| acc.add(t.value))
    }

    fn payment_due(&self) -> f64 {
        (self.target_value().upfront - self.source_value().upfront).max(0.0)
    }

    fn xml(&self) -> String {
        let sources: Vec<String> = self
            .sources
            .iter()
            .map(|(r, v)| {
                format!(
                    "{}{}",
                    v.xml("reservationValue"),
                    ec2_elem("reservedInstanceId", &r.id)
                )
            })
            .collect();
        let targets: Vec<String> = self
            .targets
            .iter()
            .map(|t| {
                format!(
                    "{}<targetConfiguration><instanceCount>{}</instanceCount>{}</targetConfiguration>",
                    t.value.xml("reservationValue"),
                    t.count,
                    ec2_elem("offeringId", &t.offering_id)
                )
            })
            .collect();
        format!(
            "{}<isValidExchange>{}</isValidExchange>{}{}{}{}{}{}{}",
            ec2_elem("currencyCode", "USD"),
            self.failure.is_none(),
            self.expires.map_or_else(String::new, |t| ec2_elem(
                "outputReservedInstancesWillExpireAt",
                &t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            )),
            ec2_elem("paymentDue", &money(self.payment_due())),
            self.source_value().xml("reservedInstanceValueRollup"),
            ec2_list("reservedInstanceValueSet", &sources),
            self.target_value().xml("targetConfigurationValueRollup"),
            ec2_list("targetConfigurationValueSet", &targets),
            self.failure
                .as_deref()
                .map_or_else(String::new, |f| ec2_elem("validationFailureReason", f)),
        )
    }
}

fn price(raw: &str) -> f64 {
    raw.parse().unwrap_or(0.0)
}

/// Prices an exchange of Convertible reservations for Convertible offerings.
///
/// Recorded from real EC2, in this order: a missing id list, a target with no
/// offering id, `DryRun`, a malformed reservation id, a malformed offering id,
/// and reservations the account does not own (all of them listed). Inferred:
/// an unknown UUID-shaped offering id is refused like a malformed one, and the
/// rules that make a quote invalid (the reservations must be active,
/// Convertible and named once; the targets Convertible offerings of one term
/// no shorter than the reservations', worth at least as much). Without an
/// instance count, a target takes the fewest instances that are worth as
/// much, as the EC2 console quotes it.
/// `state` is the request's account and Region, read under the guard the
/// caller holds: a read guard to quote, the write guard Accept applies the
/// exchange under, so nothing changes between the check and the exchange.
fn exchange_quote(
    state: Option<&Ec2State>,
    req: &AwsRequest,
    now: Now,
) -> Result<Quote, AwsServiceError> {
    let params = &req.query_params;
    let ids = indexed_list(params, "ReservedInstanceId");
    if ids.is_empty() {
        return Err(error(
            "MissingParameter",
            "The request must include the ReservedInstanceIds parameter. Add the required \
             parameter and retry the request.",
        ));
    }
    const T: &str = "TargetConfiguration";
    let target_indexes = struct_indexes(params, T);
    let get = |i: usize, name: &str| {
        member(params, T, i, name)
            .filter(|v| !v.is_empty())
            .cloned()
    };
    if let Some(i) = target_indexes
        .iter()
        .find(|i| get(**i, "OfferingId").is_none())
    {
        return Err(crate::service_helpers::invalid_parameter_value(format!(
            "1 validation error detected: Value null at 'targetConfigurations.{i}.member.offeringId' \
             failed to satisfy constraint: Member must not be null"
        )));
    }
    crate::service_helpers::refuse_dry_run(params)?;
    if let Some(bad) = ids.iter().find(|id| !uuid_shaped(id)) {
        return Err(error(
            "InvalidReservedInstancesID.NotFound",
            format!("The reserved-instances ID '{bad}' is malformed"),
        ));
    }
    let invalid_offering = || error("InvalidOfferingId", "Invalid Offering ID provided");
    let offering_ids: Vec<String> = target_indexes
        .iter()
        .filter_map(|i| get(*i, "OfferingId"))
        .collect();
    if offering_ids.iter().any(|id| !uuid_shaped(id)) {
        return Err(invalid_offering());
    }
    let found = |id: &str| {
        state
            .and_then(|s| s.reserved_instances.get(id))
            .filter(|r| ri_visible(r, now))
    };
    let unknown: Vec<&str> = ids
        .iter()
        .map(String::as_str)
        .filter(|id| found(id).is_none())
        .collect();
    if !unknown.is_empty() {
        return Err(error(
            "InvalidReservedInstanceId",
            format!(" [{}]", unknown.join(", ")),
        ));
    }
    let region = crate::service::instance::request_region(req);
    let mut offerings = Vec::new();
    for (pos, id) in offering_ids.iter().enumerate() {
        let offering = Offering::from_id(region, id).ok_or_else(invalid_offering)?;
        let count = match get(target_indexes[pos], "InstanceCount") {
            None => None,
            Some(raw) => Some(parse_count(&raw).ok_or_else(|| {
                crate::service_helpers::invalid_parameter_value(format!(
                    "Invalid value '{raw}' for InstanceCount."
                ))
            })?),
        };
        offerings.push((id.clone(), offering, count));
    }
    let sources: Vec<(ReservedInstances, Value)> = ids
        .iter()
        .filter_map(|id| found(id))
        .map(|r| {
            let remaining =
                parse_time(&r.end).map_or(0.0, |end| ((end - now).num_seconds().max(0)) as f64);
            let value = Value::of(
                price(&r.fixed_price),
                price(&r.usage_price),
                r.instance_count,
                remaining,
                r.duration as f64,
            );
            (r.clone(), value)
        })
        .collect();
    let mut failure: Option<String> = None;
    let mut fail = |why: String| {
        if failure.is_none() {
            failure = Some(why);
        }
    };
    for (i, (r, _)) in sources.iter().enumerate() {
        if r.offering_class != "convertible" {
            fail(format!(
                "Reserved Instance {} is a Standard Reserved Instance. Only Convertible Reserved \
                 Instances can be exchanged.",
                r.id
            ));
        }
        let current = ri_state(r, now);
        if current != "active" {
            fail(format!(
                "Reserved Instance {} is {current}, not active.",
                r.id
            ));
        }
        if sources[..i].iter().any(|(s, _)| s.id == r.id) {
            fail(format!(
                "Reserved Instance {} is specified more than once.",
                r.id
            ));
        }
        if state.is_some_and(|s| modification_in_progress(s, &r.id, now)) {
            fail(format!(
                "Reserved Instance {} has a modification in progress.",
                r.id
            ));
        }
    }
    if offerings.is_empty() {
        fail("A target configuration is required to exchange Reserved Instances.".to_string());
    }
    if let Some((id, _, _)) = offerings.iter().find(|(_, o, _)| o.class != "convertible") {
        fail(format!(
            "Offering {id} is not a Convertible Reserved Instance offering."
        ));
    }
    let longest_source = sources.iter().map(|(r, _)| r.duration).max().unwrap_or(0);
    let source_end = sources.iter().filter_map(|(r, _)| parse_time(&r.end)).max();
    let term = offerings.first().map(|(_, o, _)| o.duration);
    if offerings.iter().any(|(_, o, _)| Some(o.duration) != term) {
        fail("All target configurations must have the same term.".to_string());
    }
    // A reservation's duration is its term; one persisted with another
    // length counts as the nearest term.
    let longest_source_term = RI_TERM_SECONDS
        .iter()
        .copied()
        .min_by_key(|t| (t - longest_source).abs())
        .unwrap_or(longest_source);
    let expires = match term {
        Some(t) if t < longest_source_term => {
            fail(
                "The term of the target configuration cannot be shorter than that of the \
                 Reserved Instances."
                    .to_string(),
            );
            source_end
        }
        // The same term keeps the latest end of the reservations exchanged;
        // a longer one starts a new term now.
        Some(t) if t == longest_source_term => source_end,
        Some(t) => Some(now + chrono::Duration::seconds(t)),
        None => source_end,
    };
    let source_value: Value = sources
        .iter()
        .fold(Value::default(), |acc, (_, v)| acc.add(*v));
    let remaining = expires.map_or(0.0, |e| ((e - now).num_seconds().max(0)) as f64);
    let targets: Vec<QuoteTarget> = offerings
        .into_iter()
        .map(|(offering_id, offering, count)| {
            let unit = Value::of(
                price(FIXED_PRICE),
                price(USAGE_PRICE),
                1,
                remaining,
                offering.duration as f64,
            );
            // The fewest instances worth as much, bounded by what an
            // instance count can hold.
            let count = count.unwrap_or_else(|| {
                if unit.total > 0.0 {
                    ((source_value.total / unit.total) - 1e-9)
                        .ceil()
                        .clamp(1.0, f64::from(i32::MAX)) as i64
                } else {
                    1
                }
            });
            let value = Value::of(
                price(FIXED_PRICE),
                price(USAGE_PRICE),
                count,
                remaining,
                offering.duration as f64,
            );
            QuoteTarget {
                offering_id,
                offering,
                count,
                value,
            }
        })
        .collect();
    let target_total: f64 = targets.iter().map(|t| t.value.total).sum();
    if !targets.is_empty() && target_total + 1e-6 < source_value.total {
        fail(
            "The target configurations must be of equal or greater value than the Reserved \
             Instances exchanged."
                .to_string(),
        );
    }
    Ok(Quote {
        sources,
        targets,
        expires,
        failure,
    })
}

pub(crate) fn get_reserved_instances_exchange_quote(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let quote = exchange_quote(
        accounts.regional(&req.account_id, &req.region),
        req,
        chrono::Utc::now(),
    )?;
    Ok(Ec2Service::respond(
        "GetReservedInstancesExchangeQuote",
        &req.request_id,
        &quote.xml(),
    ))
}

/// Makes the exchange a quote prices: the reservations exchanged are retired
/// and one reservation per target is made, from now to the quote's end. An
/// exchange the quote finds invalid is refused with its reason (inferred: the
/// code real EC2 refuses it with is not recorded).
pub(crate) fn accept_reserved_instances_exchange_quote(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let now = chrono::Utc::now();
    // Validated and applied under one write guard: a concurrent Accept or
    // Modify of the same reservations sees them retired, not still active.
    let mut accounts = svc.state.write();
    let mut state = accounts.regional_get_mut(&req.account_id, &req.region);
    let quote = exchange_quote(state.as_deref(), req, now)?;
    if let Some(why) = &quote.failure {
        return Err(crate::service_helpers::invalid_parameter_value(why.clone()));
    }
    let end = quote.expires.unwrap_or(now);
    // Every source was found, so the account and Region exist.
    let Some(state) = state.as_mut() else {
        return Err(crate::service_helpers::invalid_parameter_value(
            "The Reserved Instances to exchange do not exist.",
        ));
    };
    for (r, _) in &quote.sources {
        if let Some(stored) = state.reserved_instances.get_mut(&r.id) {
            stored.retired_at = Some(timestamp(now));
        }
    }
    for t in &quote.targets {
        let id = uuid::Uuid::new_v4().to_string();
        state.reserved_instances.insert(
            id.clone(),
            ReservedInstances {
                id,
                instance_type: t.offering.instance_type.to_string(),
                availability_zone: t.offering.zone.clone().unwrap_or_default(),
                instance_count: t.count,
                product_description: t.offering.product.to_string(),
                state: "payment-pending".to_string(),
                duration: t.offering.duration,
                fixed_price: FIXED_PRICE.to_string(),
                usage_price: USAGE_PRICE.to_string(),
                scope: scope_of(t.offering.zone.as_deref()).to_string(),
                offering_class: t.offering.class.to_string(),
                offering_type: t.offering.offering_type.to_string(),
                instance_tenancy: t.offering.tenancy.to_string(),
                start: timestamp(now),
                end: timestamp(end),
                retired_at: None,
                available_from: None,
            },
        );
    }
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
            .filter(|r| ri_visible(r, now))
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

    // ---- secondary operations ----

    fn call(
        f: fn(&Ec2Service, &AwsRequest) -> Result<AwsResponse, AwsServiceError>,
        svc: &Ec2Service,
        action: &str,
        params: &[(&str, &str)],
    ) -> Result<AwsResponse, AwsServiceError> {
        f(svc, &req(action, params))
    }

    fn refusal(r: Result<AwsResponse, AwsServiceError>) -> (String, String) {
        let e = err_of(r);
        (e.code().to_string(), e.message())
    }

    fn with_state<T>(svc: &Ec2Service, f: impl FnOnce(&mut Ec2State) -> T) -> T {
        f(svc.state.write().regional_mut("000000000000", "us-east-1"))
    }

    /// Buys `count` of the offering the params describe and backdates it an
    /// hour, so it reads `active`.
    fn active_ri(svc: &Ec2Service, params: &[(&str, &str)], count: &str) -> String {
        let offering = offering_ids(svc, params).remove(0);
        let id = elems(
            &body(purchase(
                svc,
                &[
                    ("ReservedInstancesOfferingId", &offering),
                    ("InstanceCount", count),
                ],
            )),
            "reservedInstancesId",
        )
        .remove(0);
        with_state(svc, |s| {
            let r = s.reserved_instances.get_mut(&id).unwrap();
            let start = parse_time(&r.start).unwrap() - chrono::Duration::hours(1);
            r.start = timestamp(start);
            r.end = timestamp(start + chrono::Duration::seconds(r.duration));
        });
        id
    }

    fn zonal_standard(svc: &Ec2Service, zone: &str, count: &str) -> String {
        active_ri(
            svc,
            &[
                ("InstanceType", "m5.large"),
                ("AvailabilityZone", zone),
                ("MaxDuration", "31536000"),
            ],
            count,
        )
    }

    fn create_listing(
        svc: &Ec2Service,
        params: &[(&str, &str)],
    ) -> Result<AwsResponse, AwsServiceError> {
        call(
            create_reserved_instances_listing,
            svc,
            "CreateReservedInstancesListing",
            params,
        )
    }

    fn listing_params<'a>(ri: &'a str, count: &'a str, token: &'a str) -> Vec<(&'a str, &'a str)> {
        vec![
            ("ReservedInstancesId", ri),
            ("InstanceCount", count),
            ("ClientToken", token),
            ("PriceSchedules.1.Term", "11"),
            ("PriceSchedules.1.Price", "100"),
            ("PriceSchedules.2.Term", "5"),
            ("PriceSchedules.2.Price", "50.5"),
        ]
    }

    #[test]
    fn a_listing_offers_an_active_standard_reservation() {
        let svc = Ec2Service::new();
        let ri = zonal_standard(&svc, "us-east-1a", "3");
        let out = body(create_listing(&svc, &listing_params(&ri, "2", "tok-1")));
        let id = elems(&out, "reservedInstancesListingId").remove(0);
        assert!(uuid_shaped(&id), "{id}");
        assert_eq!(elems(&out, "status"), ["active"]);
        assert_eq!(elems(&out, "reservedInstancesId"), [ri.as_str()]);
        assert_eq!(elems(&out, "clientToken"), ["tok-1"]);
        assert_eq!(
            elems(&out, "state"),
            ["available", "sold", "cancelled", "pending"]
        );
        assert_eq!(elems(&out, "instanceCount"), ["2", "0", "0", "0"]);
        // One price per month left (11 of the year), the 11-month price until
        // five remain, the current month the active one.
        let terms: Vec<String> = (1..=11).rev().map(|m| m.to_string()).collect();
        assert_eq!(elems(&out, "term"), terms);
        let prices = elems(&out, "price");
        assert_eq!(prices[..6], ["100.0"; 6]);
        assert_eq!(prices[6..], ["50.5"; 5]);
        let active = elems(&out, "active");
        assert_eq!(active[0], "true");
        assert!(active[1..].iter().all(|a| a == "false"));

        // The same token and arguments answer the same listing; other
        // arguments under it are refused.
        let again = body(create_listing(&svc, &listing_params(&ri, "2", "tok-1")));
        assert_eq!(elems(&again, "reservedInstancesListingId"), [id.as_str()]);
        assert_eq!(
            refusal(create_listing(&svc, &listing_params(&ri, "1", "tok-1"))).0,
            "IdempotentParameterMismatch"
        );
        // Only the one instance not listed is left to list.
        assert_eq!(
            refusal(create_listing(&svc, &listing_params(&ri, "2", "tok-2"))).0,
            "InvalidParameterValue"
        );
        body(create_listing(&svc, &listing_params(&ri, "1", "tok-2")));

        let describe = |params: &[(&str, &str)]| {
            call(
                describe_reserved_instances_listings,
                &svc,
                "DescribeReservedInstancesListings",
                params,
            )
        };
        let count = |params: &[(&str, &str)]| {
            elems(&body(describe(params)), "reservedInstancesListingId").len()
        };
        assert_eq!(count(&[]), 2);
        assert_eq!(count(&[("ReservedInstancesListingId", &id)]), 1);
        assert_eq!(count(&[("ReservedInstancesId", &ri)]), 2);
        assert_eq!(
            count(&[("Filter.1.Name", "status"), ("Filter.1.Value.1", "active")]),
            2
        );
        assert_eq!(
            count(&[
                ("Filter.1.Name", "status"),
                ("Filter.1.Value.1", "cancelled")
            ]),
            0
        );
        assert_eq!(
            refusal(describe(&[
                ("Filter.1.Name", "bogus"),
                ("Filter.1.Value.1", "x")
            ])),
            (
                "InvalidParameterValue".into(),
                "The filter 'bogus' is invalid".into()
            )
        );

        // Cancelled, its instances count as cancelled and it cannot be
        // cancelled twice.
        let cancel = |listing: &str| {
            call(
                cancel_reserved_instances_listing,
                &svc,
                "CancelReservedInstancesListing",
                &[("ReservedInstancesListingId", listing)],
            )
        };
        let out = body(cancel(&id));
        assert_eq!(elems(&out, "status"), ["cancelled"]);
        assert_eq!(elems(&out, "instanceCount"), ["0", "0", "2", "0"]);
        assert!(elems(&out, "active").iter().all(|a| a == "false"));
        assert_eq!(refusal(cancel(&id)).0, "InvalidParameterValue");
        assert_eq!(
            count(&[
                ("Filter.1.Name", "status"),
                ("Filter.1.Value.1", "cancelled")
            ]),
            1
        );
        // Recorded from real EC2.
        assert_eq!(
            refusal(cancel("e5a2ff3b-7d14-494f-90af-0b5d00000000")),
            (
                "InvalidInput".into(),
                " Invalid value for 'listingId'".into()
            )
        );
        assert_eq!(refusal(cancel("ril-1")).0, "InvalidInput");
        // Its instances can be listed again.
        body(create_listing(&svc, &listing_params(&ri, "2", "tok-3")));
        // Once the reservation ends the listing is closed.
        with_state(&svc, |s| {
            s.reserved_instances.get_mut(&ri).unwrap().end =
                timestamp(chrono::Utc::now() - chrono::Duration::seconds(1));
        });
        assert_eq!(
            count(&[("Filter.1.Name", "status"), ("Filter.1.Value.1", "closed")]),
            2
        );
    }

    #[test]
    fn listings_stored_by_earlier_versions_stay_cancellable_and_never_free_capacity() {
        let svc = Ec2Service::new();
        let ri = zonal_standard(&svc, "us-east-1a", "3");
        let legacy = |id: &str, count: i64| crate::state::ReservedInstancesListing {
            listing_id: id.to_string(),
            reserved_instances_id: ri.clone(),
            instance_count: count,
            client_token: id.to_string(),
            status: "active".to_string(),
            status_message: "ACTIVE".to_string(),
            price_schedules: Vec::new(),
            create_date: timestamp(chrono::Utc::now()),
            update_date: timestamp(chrono::Utc::now()),
        };
        with_state(&svc, |s| {
            for l in [legacy("ril-1234abcd", 2), legacy("ril-negative", -5)] {
                s.reserved_instances_listings
                    .insert(l.listing_id.clone(), l);
            }
        });
        // The negative count frees nothing: only one of three is unlisted.
        assert_eq!(
            refusal(create_listing(&svc, &listing_params(&ri, "2", "tok-a"))).0,
            "InvalidParameterValue"
        );
        let out = body(call(
            cancel_reserved_instances_listing,
            &svc,
            "CancelReservedInstancesListing",
            &[("ReservedInstancesListingId", "ril-1234abcd")],
        ));
        assert_eq!(elems(&out, "status"), ["cancelled"]);
        // Its two instances are free again.
        body(create_listing(&svc, &listing_params(&ri, "3", "tok-b")));
    }

    #[test]
    fn a_listing_refuses_what_real_ec2_refuses() {
        let svc = Ec2Service::new();
        let ri = zonal_standard(&svc, "us-east-1a", "1");
        let unknown = "e5a2ff3b-7d14-494f-90af-0b5d00000000";
        let with = |overrides: &[(&str, &str)], drop: &[&str]| {
            let mut params = listing_params(&ri, "1", "t");
            params.retain(|(k, _)| !drop.contains(k) && !overrides.iter().any(|(o, _)| o == k));
            params.extend_from_slice(overrides);
            refusal(create_listing(&svc, &params))
        };
        // Recorded from real EC2.
        assert_eq!(
            with(
                &[],
                &[
                    "PriceSchedules.1.Term",
                    "PriceSchedules.1.Price",
                    "PriceSchedules.2.Term",
                    "PriceSchedules.2.Price"
                ]
            ),
            (
                "MissingParameter".into(),
                "The request must contain the parameter PriceSchedules".into()
            )
        );
        assert_eq!(
            with(&[], &["InstanceCount"]),
            (
                "MissingParameter".into(),
                "The request must contain the parameter InstanceCount".into()
            )
        );
        assert_eq!(
            with(&[], &["PriceSchedules.2.Term"]),
            (
                "MissingParameter".into(),
                "The request must contain the parameter Term".into()
            )
        );
        assert_eq!(
            with(&[], &["ClientToken"]),
            (
                "InvalidInput".into(),
                "1 validation error detected: Value null at 'clientToken' failed to satisfy \
                 constraint: Member must not be null"
                    .into()
            )
        );
        assert_eq!(
            with(&[("ReservedInstancesId", "ri-1")], &[]),
            (
                "InvalidInput".into(),
                "1 validation error detected: Value 'ri-1' at 'reservedInstanceId' failed to \
                 satisfy constraint: Member must satisfy regular expression pattern: \
                 \\w{8}(-\\w{4}){3}-\\w{12}"
                    .into()
            )
        );
        assert_eq!(
            with(&[("InstanceCount", "0")], &[]),
            (
                "InvalidParameterValue".into(),
                "Value (0) for parameter InstanceCount is invalid. InstanceCount must be greater \
                 than zero."
                    .into()
            )
        );
        assert_eq!(
            with(&[("PriceSchedules.1.Price", "-1.0")], &[]),
            (
                "InvalidParameterValue".into(),
                "Value (-1.0) for parameter Price is invalid. Pricing price value cannot be negative."
                    .into()
            )
        );
        assert_eq!(
            with(&[("PriceSchedules.1.Term", "-1")], &[]),
            (
                "InvalidParameterValue".into(),
                "Value (-1) for parameter Term is invalid. Pricing term cannot be negative.".into()
            )
        );
        assert_eq!(
            with(&[("PriceSchedules.1.CurrencyCode", "EUR")], &[]),
            (
                "InvalidParameterValue".into(),
                "Value (EUR) for parameter CurrencyCode is invalid. Invalid currency code specified."
                    .into()
            )
        );
        // Inferred: what real EC2 checks after the seller registration.
        assert_eq!(
            with(&[("ReservedInstancesId", unknown)], &[]).0,
            "InvalidReservedInstancesId"
        );
        assert_eq!(
            with(&[("PriceSchedules.1.Term", "12")], &[]).0,
            "InvalidParameterValue"
        );
        assert_eq!(
            with(&[("PriceSchedules.1.Term", "0")], &[]).0,
            "InvalidParameterValue"
        );
        assert_eq!(
            with(&[("PriceSchedules.2.Term", "11")], &[]).0,
            "InvalidParameterValue"
        );
        assert_eq!(
            with(&[("InstanceCount", "2")], &[]).0,
            "InvalidParameterValue"
        );
        let convertible = active_ri(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MaxDuration", "31536000"),
            ],
            "1",
        );
        assert_eq!(
            with(&[("ReservedInstancesId", &convertible)], &[]).0,
            "InvalidParameterValue"
        );
        // A purchase whose payment has not settled is not active yet.
        let offering = region_offering(&svc);
        let pending = elems(
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
        assert_eq!(
            with(&[("ReservedInstancesId", &pending)], &[]).0,
            "InvalidParameterValue"
        );
        let listings = body(call(
            describe_reserved_instances_listings,
            &svc,
            "DescribeReservedInstancesListings",
            &[],
        ));
        assert!(!listings.contains("<item>"), "{listings}");
    }

    fn modify(svc: &Ec2Service, params: &[(&str, &str)]) -> Result<AwsResponse, AwsServiceError> {
        call(
            modify_reserved_instances,
            svc,
            "ModifyReservedInstances",
            params,
        )
    }

    fn describe_modifications(
        svc: &Ec2Service,
        params: &[(&str, &str)],
    ) -> Result<AwsResponse, AwsServiceError> {
        call(
            describe_reserved_instances_modifications,
            svc,
            "DescribeReservedInstancesModifications",
            params,
        )
    }

    /// Moves a modification (and the reservations it touches) past its
    /// processing.
    fn fulfil(svc: &Ec2Service, modification: &str) {
        let past = timestamp(chrono::Utc::now() - chrono::Duration::seconds(1));
        with_state(svc, |s| {
            let m = s
                .reserved_instances_modifications
                .get_mut(modification)
                .unwrap();
            m.fulfilled_at = past.clone();
            let (sources, made): (Vec<String>, Vec<String>) = (
                m.reserved_instances_ids.clone(),
                m.results
                    .iter()
                    .map(|r| r.reserved_instances_id.clone())
                    .collect(),
            );
            for id in sources {
                s.reserved_instances.get_mut(&id).unwrap().retired_at = Some(past.clone());
            }
            for id in made {
                s.reserved_instances.get_mut(&id).unwrap().available_from = Some(past.clone());
            }
        });
    }

    #[test]
    fn a_modification_is_processed_then_replaces_the_reservations() {
        let svc = Ec2Service::new();
        let a = zonal_standard(&svc, "us-east-1a", "1");
        let b = zonal_standard(&svc, "us-east-1a", "1");
        // Make the two end in the same hour, as identical reservations do.
        let end = with_state(&svc, |s| s.reserved_instances[&a].end.clone());
        with_state(&svc, |s| {
            s.reserved_instances.get_mut(&b).unwrap().end = end.clone()
        });
        let out = body(modify(
            &svc,
            &[
                ("ReservedInstancesId.1", &a),
                ("ReservedInstancesId.2", &b),
                ("ClientToken", "mod-1"),
                (
                    "ReservedInstancesConfigurationSetItemType.1.InstanceCount",
                    "1",
                ),
                (
                    "ReservedInstancesConfigurationSetItemType.1.InstanceType",
                    "m5.xlarge",
                ),
                (
                    "ReservedInstancesConfigurationSetItemType.1.AvailabilityZone",
                    "us-east-1b",
                ),
            ],
        ));
        let id = elems(&out, "reservedInstancesModificationId").remove(0);
        assert!(
            id.starts_with("rimod-") && uuid_shaped(&id["rimod-".len()..]),
            "{id}"
        );

        // Processing: the reservations are untouched and the new one is not
        // made yet.
        let mods = body(describe_modifications(&svc, &[]));
        assert_eq!(elems(&mods, "status"), ["processing"]);
        assert_eq!(elems(&mods, "clientToken"), ["mod-1"]);
        assert_eq!(elems(&mods, "instanceType"), ["m5.xlarge"]);
        assert_eq!(elems(&mods, "availabilityZone"), ["us-east-1b"]);
        assert_eq!(elems(&mods, "availabilityZoneId"), ["use1-az2"]);
        // Only the two modified are named; no result id yet.
        assert_eq!(elems(&mods, "reservedInstancesId").len(), 2, "{mods}");
        let effective = parse_time(&elems(&mods, "effectiveDate")[0]).unwrap();
        assert_eq!((effective.minute(), effective.second()), (0, 0));
        assert_eq!(elems(&describe(&svc), "state"), ["active", "active"]);
        // The same token answers the same modification.
        let again = body(modify(
            &svc,
            &[
                ("ReservedInstancesId.1", &a),
                ("ReservedInstancesId.2", &b),
                ("ClientToken", "mod-1"),
                (
                    "ReservedInstancesConfigurationSetItemType.1.InstanceCount",
                    "1",
                ),
                (
                    "ReservedInstancesConfigurationSetItemType.1.InstanceType",
                    "m5.xlarge",
                ),
                (
                    "ReservedInstancesConfigurationSetItemType.1.AvailabilityZone",
                    "us-east-1b",
                ),
            ],
        ));
        assert_eq!(
            elems(&again, "reservedInstancesModificationId"),
            [id.as_str()]
        );
        // Being modified, they cannot be modified again.
        assert_eq!(
            refusal(modify(
                &svc,
                &[
                    ("ReservedInstancesId.1", &a),
                    (
                        "ReservedInstancesConfigurationSetItemType.1.InstanceCount",
                        "1"
                    ),
                    (
                        "ReservedInstancesConfigurationSetItemType.1.AvailabilityZone",
                        "us-east-1c"
                    ),
                ],
            ))
            .0,
            "InvalidInput"
        );

        fulfil(&svc, &id);
        let mods = body(describe_modifications(&svc, &[]));
        assert_eq!(elems(&mods, "status"), ["fulfilled"]);
        let made = elems(&mods, "reservedInstancesId").pop().unwrap();
        assert!(made != a && made != b);
        let all = describe(&svc);
        let one = |id: &str| {
            body(describe_reserved_instances(
                &svc,
                &req(
                    "DescribeReservedInstances",
                    &[("ReservedInstancesId.1", id)],
                ),
            ))
        };
        assert_eq!(elems(&all, "reservedInstancesId").len(), 3);
        assert_eq!(elems(&one(&a), "state"), ["retired"]);
        assert_eq!(elems(&one(&b), "state"), ["retired"]);
        let new = one(&made);
        assert_eq!(elems(&new, "state"), ["active"]);
        assert_eq!(elems(&new, "instanceType"), ["m5.xlarge"]);
        assert_eq!(elems(&new, "availabilityZone"), ["us-east-1b"]);
        assert_eq!(elems(&new, "instanceCount"), ["1"]);
        assert_eq!(elems(&new, "end"), [end.as_str()]);
        assert_eq!(elems(&new, "start"), [timestamp(effective).as_str()]);

        // Filters, as modelled.
        let count = |params: &[(&str, &str)]| {
            elems(
                &body(describe_modifications(&svc, params)),
                "reservedInstancesModificationId",
            )
            .len()
        };
        for (name, value) in [
            ("status", "fulfilled"),
            ("client-token", "mod-1"),
            ("reserved-instances-id", a.as_str()),
            ("reserved-instances-modification-id", id.as_str()),
            ("modification-result.reserved-instances-id", made.as_str()),
            (
                "modification-result.target-configuration.instance-type",
                "m5.*",
            ),
            (
                "modification-result.target-configuration.instance-count",
                "1",
            ),
            (
                "modification-result.target-configuration.availability-zone",
                "us-east-1b",
            ),
            (
                "modification-result.target-configuration.availability-zone-id",
                "use1-az2",
            ),
            ("effective-date", &timestamp(effective)),
            ("status-message", ""),
        ] {
            assert_eq!(
                count(&[("Filter.1.Name", name), ("Filter.1.Value.1", value)]),
                1,
                "{name}"
            );
            assert_eq!(
                count(&[("Filter.1.Name", name), ("Filter.1.Value.1", "nope")]),
                0,
                "{name}"
            );
        }
        assert_eq!(count(&[("ReservedInstancesModificationId.1", &id)]), 1);
        // Recorded from real EC2.
        assert_eq!(
            count(&[(
                "ReservedInstancesModificationId.1",
                "rimod-e5a2ff3b-7d14-494f-90af-0b5d00000000"
            )]),
            0
        );
        assert_eq!(
            refusal(describe_modifications(
                &svc,
                &[("ReservedInstancesModificationId.1", "mod-1")]
            )),
            (
                "InvalidParameterValue".into(),
                "Invalid value 'mod-1' for ReservedInstancesModificationId.".into()
            )
        );
        assert_eq!(
            refusal(describe_modifications(&svc, &[("NextToken", "bogus")])),
            (
                "InvalidInput".into(),
                " Invalid value for 'nextToken'.".into()
            )
        );
        assert_eq!(
            refusal(describe_modifications(
                &svc,
                &[("Filter.1.Name", "bogus"), ("Filter.1.Value.1", "x")]
            ))
            .1,
            "The filter 'bogus' is invalid"
        );
    }

    #[test]
    fn a_modification_refuses_what_real_ec2_refuses() {
        let svc = Ec2Service::new();
        let ri = zonal_standard(&svc, "us-east-1a", "2");
        let unknown = "e5a2ff3b-7d14-494f-90af-0b5d00000000";
        const T: &str = "ReservedInstancesConfigurationSetItemType.1";
        let refused = |params: &[(&str, &str)]| refusal(modify(&svc, params));
        let target = |extra: &[(&'static str, &'static str)]| {
            let mut p: Vec<(String, String)> = vec![(format!("{T}.InstanceCount"), "2".into())];
            for (k, v) in extra {
                p.retain(|(pk, _)| *pk != format!("{T}.{k}"));
                p.push((format!("{T}.{k}"), v.to_string()));
            }
            p
        };
        let run = |ids: &[&str], target: Vec<(String, String)>| {
            let ids: Vec<(String, String)> = ids
                .iter()
                .enumerate()
                .map(|(i, id)| (format!("ReservedInstancesId.{}", i + 1), id.to_string()))
                .collect();
            let all: Vec<(String, String)> = ids.into_iter().chain(target).collect();
            let params: Vec<(&str, &str)> =
                all.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            refused(&params)
        };
        let zone = &[("AvailabilityZone", "us-east-1b")];
        // Recorded from real EC2, in this order.
        assert_eq!(
            run(&[], target(zone)),
            ("MissingParameter".into(), "ReservedInstancesSet".into())
        );
        assert_eq!(
            run(&[&ri], vec![]),
            (
                "MissingParameter".into(),
                "TargetReservedInstancesConfigurationSet".into()
            )
        );
        assert_eq!(
            run(
                &[&ri],
                vec![(format!("{T}.InstanceType"), "m5.large".into())]
            ),
            ("MissingParameter".into(), "InstanceCount".into())
        );
        assert_eq!(
            run(&["ri-1"], target(zone)),
            (
                "InvalidParameterValue".into(),
                "Invalid value 'ri-1' for ReservedInstancesId.".into()
            )
        );
        for (name, value, message) in [
            (
                "InstanceCount",
                "0",
                "Invalid value '0' for InstanceCount. Instance count must be greater than zero.",
            ),
            (
                "InstanceType",
                "m5.bogus",
                "Invalid value 'm5.bogus' for InstanceType.",
            ),
            (
                "AvailabilityZone",
                "us-east-1z",
                "Invalid value 'us-east-1z' for AvailabilityZone.",
            ),
            (
                "AvailabilityZone",
                "eu-west-1a",
                "Invalid value 'eu-west-1a' for AvailabilityZone.",
            ),
            (
                "AvailabilityZoneId",
                "use1-az99",
                "Invalid value 'use1-az99' for AvailabilityZoneId.",
            ),
            ("Scope", "Galaxy", "Invalid value 'Galaxy' for Scope."),
            ("Platform", "Mars", "Invalid value 'Mars' for Platform."),
        ] {
            assert_eq!(
                run(&[unknown], target(&[(name, value)])),
                ("InvalidParameterValue".into(), message.into()),
                "{name}"
            );
        }
        assert_eq!(
            run(
                &[unknown],
                target(&[
                    ("AvailabilityZone", "us-east-1a"),
                    ("AvailabilityZoneId", "use1-az2")
                ])
            )
            .0,
            "InvalidParameterCombination"
        );
        assert_eq!(
            run(&[unknown, unknown], target(&[])),
            (
                "InvalidInput".into(),
                format!(" Invalid value for 'reservedInstancesIds'. Reserved Instance {unknown} is a duplicate.")
            )
        );
        assert_eq!(
            run(
                &[unknown],
                target(&[("Scope", "Region"), ("AvailabilityZone", "us-east-1a")])
            )
            .0,
            "InvalidInput"
        );
        assert_eq!(
            run(&[unknown], target(&[])),
            (
                "MissingParameter".into(),
                "The request must contain the parameter  AvailabilityZone".into()
            )
        );
        assert_eq!(
            run(&[unknown], target(zone)),
            (
                "InvalidInput".into(),
                format!(" Invalid value for 'reservedInstancesIds'. Reserved Instance {unknown} is not found.")
            )
        );
        // Inferred: the rules after the reservations are found.
        assert_eq!(
            run(
                &[&ri],
                target(&[("AvailabilityZone", "us-east-1b"), ("InstanceCount", "3")])
            )
            .0,
            "InvalidParameterValue"
        );
        assert_eq!(
            run(
                &[&ri],
                target(&[
                    ("AvailabilityZone", "us-east-1b"),
                    ("InstanceType", "c5.large")
                ])
            )
            .0,
            "InvalidParameterValue"
        );
        // Two m5.large (footprint 8) are not one m5.2xlarge (16).
        assert_eq!(
            run(
                &[&ri],
                target(&[
                    ("AvailabilityZone", "us-east-1b"),
                    ("InstanceType", "m5.2xlarge"),
                    ("InstanceCount", "1")
                ])
            )
            .0,
            "InvalidParameterValue"
        );
        // m5.large is not sold in us-east-1e.
        assert_eq!(
            run(&[&ri], target(&[("AvailabilityZone", "us-east-1e")])).0,
            "InvalidParameterValue"
        );
        // Nothing was modified.
        assert!(!body(describe_modifications(&svc, &[])).contains("<item>"));
        // A Region-scoped target needs no zone, and two instances may be
        // split across two targets.
        let params = [
            ("ReservedInstancesId.1", ri.as_str()),
            (
                "ReservedInstancesConfigurationSetItemType.1.InstanceCount",
                "1",
            ),
            (
                "ReservedInstancesConfigurationSetItemType.1.Scope",
                "Region",
            ),
            (
                "ReservedInstancesConfigurationSetItemType.2.InstanceCount",
                "1",
            ),
            (
                "ReservedInstancesConfigurationSetItemType.2.AvailabilityZone",
                "us-east-1c",
            ),
        ];
        let id = elems(
            &body(modify(&svc, &params)),
            "reservedInstancesModificationId",
        )
        .remove(0);
        fulfil(&svc, &id);
        let out = describe(&svc);
        assert_eq!(elems(&out, "reservedInstancesId").len(), 3);
        assert!(out.contains("<scope>Region</scope>"), "{out}");
    }

    fn quote(svc: &Ec2Service, params: &[(&str, &str)]) -> Result<AwsResponse, AwsServiceError> {
        call(
            get_reserved_instances_exchange_quote,
            svc,
            "GetReservedInstancesExchangeQuote",
            params,
        )
    }

    fn accept(svc: &Ec2Service, params: &[(&str, &str)]) -> Result<AwsResponse, AwsServiceError> {
        call(
            accept_reserved_instances_exchange_quote,
            svc,
            "AcceptReservedInstancesExchangeQuote",
            params,
        )
    }

    fn convertible(svc: &Ec2Service, instance_type: &str, max_duration: &str) -> String {
        offering_ids(
            svc,
            &[
                ("InstanceType", instance_type),
                ("OfferingClass", "convertible"),
                ("MinDuration", max_duration),
                ("MaxDuration", max_duration),
            ],
        )
        .remove(0)
    }

    #[test]
    fn an_exchange_is_quoted_then_made() {
        let svc = Ec2Service::new();
        let source = active_ri(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MaxDuration", "31536000"),
            ],
            "2",
        );
        let target = convertible(&svc, "m5.xlarge", "31536000");
        let params = [
            ("ReservedInstanceId.1", source.as_str()),
            ("TargetConfiguration.1.OfferingId", target.as_str()),
        ];
        let out = body(quote(&svc, &params));
        assert_eq!(elems(&out, "isValidExchange"), ["true"], "{out}");
        assert!(!out.contains("validationFailureReason"));
        assert_eq!(elems(&out, "currencyCode"), ["USD"]);
        assert_eq!(elems(&out, "paymentDue"), ["0.000000"]);
        assert_eq!(elems(&out, "reservedInstanceId"), [source.as_str()]);
        assert_eq!(elems(&out, "offeringId"), [target.as_str()]);
        // Every offering is priced alike here, so two instances take two.
        assert_eq!(elems(&out, "instanceCount"), ["2"]);
        let hourly = elems(&out, "hourlyPrice");
        assert_eq!(
            hourly,
            ["0.040000", "0.040000", "0.040000", "0.040000"],
            "{out}"
        );
        let source_end = with_state(&svc, |s| {
            parse_time(&s.reserved_instances[&source].end).unwrap()
        });
        let expires = parse_time(&elems(&out, "outputReservedInstancesWillExpireAt")[0]).unwrap();
        assert_eq!(expires.timestamp(), source_end.timestamp());

        let exchange = elems(&body(accept(&svc, &params)), "exchangeId").remove(0);
        assert!(exchange.starts_with("riex-"), "{exchange}");
        let all = describe(&svc);
        assert_eq!(elems(&all, "reservedInstancesId").len(), 2);
        let states = elems(&all, "state");
        assert!(states.contains(&"retired".to_string()), "{all}");
        assert!(
            all.contains("<instanceType>m5.xlarge</instanceType>"),
            "{all}"
        );
        // Retired, the reservation cannot be exchanged again.
        assert_eq!(
            elems(&body(quote(&svc, &params)), "isValidExchange"),
            ["false"]
        );
        assert_eq!(refusal(accept(&svc, &params)).0, "InvalidParameterValue");
    }

    #[test]
    fn a_longer_term_starts_now_and_a_shorter_one_is_invalid() {
        let svc = Ec2Service::new();
        let one_year = active_ri(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MaxDuration", "31536000"),
            ],
            "1",
        );
        let three = convertible(&svc, "m5.large", "94608000");
        let out = body(quote(
            &svc,
            &[
                ("ReservedInstanceId.1", &one_year),
                ("TargetConfiguration.1.OfferingId", &three),
            ],
        ));
        assert_eq!(elems(&out, "isValidExchange"), ["true"], "{out}");
        let expires = parse_time(&elems(&out, "outputReservedInstancesWillExpireAt")[0]).unwrap();
        let ahead = (expires - chrono::Utc::now()).num_seconds();
        assert!((94_607_990..=94_608_000).contains(&ahead), "{ahead}");

        let three_year = active_ri(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MinDuration", "94608000"),
            ],
            "1",
        );
        let one = convertible(&svc, "m5.large", "31536000");
        let out = body(quote(
            &svc,
            &[
                ("ReservedInstanceId.1", &three_year),
                ("TargetConfiguration.1.OfferingId", &one),
            ],
        ));
        assert_eq!(elems(&out, "isValidExchange"), ["false"]);
        assert!(out.contains("<validationFailureReason>"), "{out}");
    }

    #[test]
    fn an_exchange_refuses_what_real_ec2_refuses() {
        let svc = Ec2Service::new();
        let standard = zonal_standard(&svc, "us-east-1a", "2");
        let source = active_ri(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MaxDuration", "31536000"),
            ],
            "2",
        );
        let target = convertible(&svc, "m5.large", "31536000");
        let standard_offering = offering_ids(
            &svc,
            &[("InstanceType", "m5.large"), ("MaxDuration", "31536000")],
        )
        .remove(0);
        let unknown = "e5a2ff3b-7d14-494f-90af-0b5d00000000";
        for op in [quote, accept] {
            // Recorded from real EC2, in this order.
            assert_eq!(
                refusal(op(&svc, &[])),
                (
                    "MissingParameter".into(),
                    "The request must include the ReservedInstanceIds parameter. Add the required \
                     parameter and retry the request."
                        .into()
                )
            );
            assert_eq!(
                refusal(op(&svc, &[("ReservedInstanceId.1", unknown), ("TargetConfiguration.1.InstanceCount", "1")])),
                (
                    "InvalidParameterValue".into(),
                    "1 validation error detected: Value null at 'targetConfigurations.1.member.offeringId' \
                     failed to satisfy constraint: Member must not be null"
                        .into()
                )
            );
            assert_eq!(
                refusal(op(
                    &svc,
                    &[("ReservedInstanceId.1", "ri-1"), ("DryRun", "true")]
                ))
                .0,
                "DryRunOperation"
            );
            assert_eq!(
                refusal(op(
                    &svc,
                    &[
                        ("ReservedInstanceId.1", unknown),
                        ("ReservedInstanceId.2", "ri-1")
                    ]
                )),
                (
                    "InvalidReservedInstancesID.NotFound".into(),
                    "The reserved-instances ID 'ri-1' is malformed".into()
                )
            );
            assert_eq!(
                refusal(op(
                    &svc,
                    &[
                        ("ReservedInstanceId.1", unknown),
                        ("TargetConfiguration.1.OfferingId", "offering-1")
                    ]
                )),
                (
                    "InvalidOfferingId".into(),
                    "Invalid Offering ID provided".into()
                )
            );
            assert_eq!(
                refusal(op(
                    &svc,
                    &[
                        ("ReservedInstanceId.1", unknown),
                        ("ReservedInstanceId.2", &source),
                        ("ReservedInstanceId.3", unknown)
                    ]
                )),
                (
                    "InvalidReservedInstanceId".into(),
                    format!(" [{unknown}, {unknown}]")
                )
            );
            // Inferred: an unknown offering id of the right shape.
            assert_eq!(
                refusal(op(
                    &svc,
                    &[
                        ("ReservedInstanceId.1", &source),
                        ("TargetConfiguration.1.OfferingId", unknown)
                    ]
                ))
                .0,
                "InvalidOfferingId"
            );
        }
        // Inferred: an exchange the quote finds invalid.
        let invalid = |params: &[(&str, &str)]| {
            let out = body(quote(&svc, params));
            assert_eq!(elems(&out, "isValidExchange"), ["false"], "{out}");
            assert_eq!(refusal(accept(&svc, params)).0, "InvalidParameterValue");
        };
        invalid(&[
            ("ReservedInstanceId.1", &standard),
            ("TargetConfiguration.1.OfferingId", &target),
        ]);
        invalid(&[
            ("ReservedInstanceId.1", &source),
            ("TargetConfiguration.1.OfferingId", &standard_offering),
        ]);
        invalid(&[
            ("ReservedInstanceId.1", &source),
            ("ReservedInstanceId.2", &source),
            ("TargetConfiguration.1.OfferingId", &target),
        ]);
        invalid(&[("ReservedInstanceId.1", &source)]);
        // Two instances are worth more than one.
        invalid(&[
            ("ReservedInstanceId.1", &source),
            ("TargetConfiguration.1.OfferingId", &target),
            ("TargetConfiguration.1.InstanceCount", "1"),
        ]);
        // Nothing was exchanged.
        assert!(!describe(&svc).contains("retired"));
    }

    #[test]
    fn concurrent_accepts_exchange_a_reservation_once() {
        let svc = std::sync::Arc::new(Ec2Service::new());
        let source = active_ri(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MaxDuration", "31536000"),
            ],
            "1",
        );
        let target = convertible(&svc, "m5.large", "31536000");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let (svc, barrier) = (svc.clone(), barrier.clone());
                let (source, target) = (source.clone(), target.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    accept(
                        &svc,
                        &[
                            ("ReservedInstanceId.1", &source),
                            ("TargetConfiguration.1.OfferingId", &target),
                        ],
                    )
                    .is_ok()
                })
            })
            .collect();
        let accepted = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|ok| *ok)
            .count();
        assert_eq!(accepted, 1);
        assert_eq!(elems(&describe(&svc), "reservedInstancesId").len(), 2);
    }

    #[test]
    fn a_modified_reservation_keeps_its_term() {
        let svc = Ec2Service::new();
        let source = active_ri(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MaxDuration", "31536000"),
            ],
            "2",
        );
        // Bought moments ago, so the hour the modification takes effect from
        // starts before the reservation did.
        with_state(&svc, |s| {
            let r = s.reserved_instances.get_mut(&source).unwrap();
            let start = chrono::Utc::now() - chrono::Duration::seconds(3);
            r.start = timestamp(start);
            r.end = timestamp(start + chrono::Duration::seconds(r.duration));
        });
        let id = elems(
            &body(modify(
                &svc,
                &[
                    ("ReservedInstancesId.1", &source),
                    (
                        "ReservedInstancesConfigurationSetItemType.1.InstanceCount",
                        "1",
                    ),
                    (
                        "ReservedInstancesConfigurationSetItemType.1.Scope",
                        "Region",
                    ),
                    (
                        "ReservedInstancesConfigurationSetItemType.2.InstanceCount",
                        "1",
                    ),
                    (
                        "ReservedInstancesConfigurationSetItemType.2.Scope",
                        "Region",
                    ),
                ],
            )),
            "reservedInstancesModificationId",
        )
        .remove(0);
        fulfil(&svc, &id);
        let made = with_state(&svc, |s| {
            s.reserved_instances_modifications[&id].results[0]
                .reserved_instances_id
                .clone()
        });
        let (duration, end) = with_state(&svc, |s| {
            let r = &s.reserved_instances[&made];
            (r.duration, parse_time(&r.end).unwrap())
        });
        assert_eq!(duration, 31_536_000);
        // Still a one-year reservation: a one-year target keeps its end, a
        // three-year one starts a new term.
        let one = convertible(&svc, "m5.large", "31536000");
        let out = body(quote(
            &svc,
            &[
                ("ReservedInstanceId.1", &made),
                ("TargetConfiguration.1.OfferingId", &one),
            ],
        ));
        assert_eq!(elems(&out, "isValidExchange"), ["true"], "{out}");
        let expires = parse_time(&elems(&out, "outputReservedInstancesWillExpireAt")[0]).unwrap();
        assert_eq!(expires.timestamp(), end.timestamp());
        let three = convertible(&svc, "m5.large", "94608000");
        let out = body(quote(
            &svc,
            &[
                ("ReservedInstanceId.1", &made),
                ("TargetConfiguration.1.OfferingId", &three),
            ],
        ));
        let expires = parse_time(&elems(&out, "outputReservedInstancesWillExpireAt")[0]).unwrap();
        assert!(
            (expires - chrono::Utc::now()).num_seconds() > 94_607_000,
            "{out}"
        );
    }

    #[test]
    fn an_exchanged_reservation_keeps_its_term() {
        let svc = Ec2Service::new();
        let source = active_ri(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MinDuration", "94608000"),
            ],
            "1",
        );
        // Less than a year left of three.
        with_state(&svc, |s| {
            let r = s.reserved_instances.get_mut(&source).unwrap();
            let end = chrono::Utc::now() + chrono::Duration::days(200);
            r.end = timestamp(end);
            r.start = timestamp(end - chrono::Duration::seconds(r.duration));
        });
        let three = convertible(&svc, "m5.large", "94608000");
        body(accept(
            &svc,
            &[
                ("ReservedInstanceId.1", &source),
                ("TargetConfiguration.1.OfferingId", &three),
            ],
        ));
        let made = with_state(&svc, |s| {
            s.reserved_instances
                .values()
                .find(|r| r.id != source)
                .map(|r| (r.id.clone(), r.duration))
                .unwrap()
        });
        assert_eq!(made.1, 94_608_000);
        // A three-year reservation, so a one-year target is a shorter term.
        let one = convertible(&svc, "m5.large", "31536000");
        let out = body(quote(
            &svc,
            &[
                ("ReservedInstanceId.1", &made.0),
                ("TargetConfiguration.1.OfferingId", &one),
            ],
        ));
        assert_eq!(elems(&out, "isValidExchange"), ["false"], "{out}");
    }

    #[test]
    fn instance_counts_beyond_an_integer_are_refused() {
        let svc = Ec2Service::new();
        let ri = zonal_standard(&svc, "us-east-1a", "1");
        let too_many = "2147483648";
        let offering = region_offering(&svc);
        assert_eq!(
            refusal(purchase(
                &svc,
                &[
                    ("ReservedInstancesOfferingId", &offering),
                    ("InstanceCount", too_many)
                ]
            ))
            .0,
            "InvalidParameterValue"
        );
        assert_eq!(
            refusal(create_listing(&svc, &listing_params(&ri, too_many, "t"))).0,
            "InvalidParameterValue"
        );
        const T: &str = "ReservedInstancesConfigurationSetItemType";
        assert_eq!(
            refusal(modify(
                &svc,
                &[
                    ("ReservedInstancesId.1", &ri),
                    (&format!("{T}.1.InstanceCount"), too_many),
                    (&format!("{T}.1.AvailabilityZone"), "us-east-1b"),
                ],
            ))
            .0,
            "InvalidParameterValue"
        );
        // Two of the largest counts sum past it without overflowing.
        let max = "2147483647";
        assert_eq!(
            refusal(modify(
                &svc,
                &[
                    ("ReservedInstancesId.1", &ri),
                    (&format!("{T}.1.InstanceCount"), max),
                    (&format!("{T}.1.AvailabilityZone"), "us-east-1b"),
                    (&format!("{T}.2.InstanceCount"), max),
                    (&format!("{T}.2.AvailabilityZone"), "us-east-1c"),
                ],
            ))
            .0,
            "InvalidParameterValue"
        );
        let convertible_ri = active_ri(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("OfferingClass", "convertible"),
                ("MaxDuration", "31536000"),
            ],
            "1",
        );
        let target = convertible(&svc, "m5.large", "31536000");
        assert_eq!(
            refusal(quote(
                &svc,
                &[
                    ("ReservedInstanceId.1", &convertible_ri),
                    ("TargetConfiguration.1.OfferingId", &target),
                    ("TargetConfiguration.1.InstanceCount", too_many),
                ],
            ))
            .0,
            "InvalidParameterValue"
        );
    }

    #[test]
    fn a_split_reservation_splits_its_price() {
        let svc = Ec2Service::new();
        let ri = active_ri(
            &svc,
            &[
                ("InstanceType", "m5.xlarge"),
                ("AvailabilityZone", "us-east-1a"),
                ("MaxDuration", "31536000"),
            ],
            "1",
        );
        let id = elems(
            &body(modify(
                &svc,
                &[
                    ("ReservedInstancesId.1", &ri),
                    (
                        "ReservedInstancesConfigurationSetItemType.1.InstanceCount",
                        "2",
                    ),
                    (
                        "ReservedInstancesConfigurationSetItemType.1.InstanceType",
                        "m5.large",
                    ),
                    (
                        "ReservedInstancesConfigurationSetItemType.1.AvailabilityZone",
                        "us-east-1a",
                    ),
                ],
            )),
            "reservedInstancesModificationId",
        )
        .remove(0);
        fulfil(&svc, &id);
        let made = with_state(&svc, |s| {
            s.reserved_instances_modifications[&id].results[0]
                .reserved_instances_id
                .clone()
        });
        let out = body(describe_reserved_instances(
            &svc,
            &req(
                "DescribeReservedInstances",
                &[("ReservedInstancesId.1", &made)],
            ),
        ));
        // Two m5.large at half the m5.xlarge's hourly price each: the same
        // total value.
        assert_eq!(elems(&out, "usagePrice"), ["0.01"], "{out}");
        assert_eq!(elems(&out, "instanceCount"), ["2"]);
    }
}
