//! DB capacity reservations: CreateDBCapacityReservation,
//! DescribeDBCapacityReservations, ModifyDBCapacityReservation and
//! CancelDBCapacityReservation, plus the placement checks DB instances run
//! when they name a reservation (`DBCapacityReservationIdentifier` on
//! create / read replica / restore / modify / start).
//!
//! A reservation holds `CapacityPerAvailabilityZone` units of one DB
//! instance class + engine in each of its Availability Zones. A running
//! Single-AZ instance holds one unit in its zone, a Multi-AZ instance one in
//! each of two zones; a stopped instance holds none. A pending Modify that
//! moves an instance onto a reservation already holds its units there.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration};

use super::*;
use crate::filters::{parse_filters, warn_unknown_filters, RdsFilter};
use crate::state::{CapacityReservationUse, DbCapacityReservation};

/// Engines a capacity reservation can be made for.
const RESERVATION_ENGINES: &[&str] = &[
    "aurora-mysql",
    "aurora-postgresql",
    "mysql",
    "postgres",
    "mariadb",
    "oracle-ee",
    "oracle-ee-cdb",
    "oracle-se2",
    "oracle-se2-cdb",
    "sqlserver-ee",
    "sqlserver-se",
    "sqlserver-web",
    "sqlserver-ex",
    "sqlserver-dev-ee",
    "sqlserver-dev-se",
    "db2-ae",
    "db2-ce",
    "db2-se",
];

const RESERVATION_FILTERS: &[&str] = &[
    "db-capacity-reservation-name",
    "state",
    "engine",
    "db-instance-class",
    "availability-zone",
];

const DAY_SECONDS: i64 = 86_400;

pub(crate) fn capacity_reservation_not_found(reference: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "DBCapacityReservationNotFound",
        format!("DB capacity reservation {reference} not found."),
    )
}

fn invalid_reservation_state(message: String) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidDBCapacityReservationState",
        message,
    )
}

fn reservation_unsupported(message: String) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "DBCapacityReservationUnsupported",
        message,
    )
}

fn insufficient_capacity(message: String) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InsufficientDBInstanceCapacity",
        message,
    )
}

/// `^[a-z](?:-?[a-z0-9]+){0,15}$`, 1 to 63 characters.
fn valid_reservation_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 || !name.starts_with(|c: char| c.is_ascii_lowercase()) {
        return false;
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return false;
    }
    !name.ends_with('-') && !name.contains("--") && name.split('-').count() <= 16
}

fn parse_timestamp(name: &str, value: &str) -> Result<DateTime<Utc>, AwsServiceError> {
    DateTime::parse_from_rfc3339(value)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|_| invalid_param(&format!("{name} '{value}' is not a valid timestamp.")))
}

fn optional_timestamp(
    request: &AwsRequest,
    name: &str,
) -> Result<Option<DateTime<Utc>>, AwsServiceError> {
    optional_query_param(request, name)
        .map(|v| parse_timestamp(name, &v))
        .transpose()
}

fn ts(t: &DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// vCPUs of a DB instance class, from its size suffix (`large` = 2,
/// `xlarge` = 4, `Nxlarge` = 4N). `None` for a size this can't size.
fn instance_class_vcpus(class: &str) -> Option<i64> {
    let size = class.rsplit('.').next()?;
    match size {
        "micro" | "small" | "medium" | "large" => Some(2),
        "xlarge" => Some(4),
        _ => size
            .strip_suffix("xlarge")
            .and_then(|n| n.parse::<i64>().ok())
            .map(|n| n * 4),
    }
}

/// The family letter and generation digit of `db.<family><gen>...`.
fn instance_class_family(class: &str) -> Option<(char, Option<u32>)> {
    let family = class.strip_prefix("db.")?.split('.').next()?;
    let mut chars = family.chars();
    let letter = chars.next()?;
    let generation = chars.next().and_then(|c| c.to_digit(10));
    Some((letter, generation))
}

/// The license model a reservation holds for `engine`, given the requested
/// one: `Ok(None)` for an engine with no license model.
fn reservation_license_model(
    engine: &str,
    requested: Option<String>,
) -> Result<Option<String>, AwsServiceError> {
    let allowed: &[&str] = match engine {
        "sqlserver-ee" | "sqlserver-se" => &["license-included", "bring-your-own-media"],
        "sqlserver-dev-ee" | "sqlserver-dev-se" => &["bring-your-own-license"],
        "oracle-ee" | "oracle-ee-cdb" => &["bring-your-own-license"],
        "oracle-se2" | "oracle-se2-cdb" => &["bring-your-own-license", "license-included"],
        "db2-se" | "db2-ae" => &["bring-your-own-license", "marketplace-license"],
        _ => &[],
    };
    match requested {
        Some(_) if allowed.is_empty() => Err(reservation_unsupported(format!(
            "LicenseModel can't be specified for engine {engine}."
        ))),
        Some(lm) if !allowed.contains(&lm.as_str()) => Err(reservation_unsupported(format!(
            "License model {lm} isn't supported for engine {engine}."
        ))),
        Some(lm) => Ok(Some(lm)),
        None if allowed.is_empty() => Ok(None),
        // More than one orderable license model: the caller must choose.
        None if allowed.len() > 1 && engine.starts_with("oracle-se2") => {
            Err(reservation_unsupported(format!(
                "Specify a LicenseModel for engine {engine}: {}.",
                allowed.join(", ")
            )))
        }
        None => Ok(Some(allowed[0].to_string())),
    }
}

/// Look a reservation up by `dbcr-` identifier or by name.
pub(crate) fn find_capacity_reservation<'a>(
    state: &'a RdsState,
    reference: &str,
) -> Option<&'a DbCapacityReservation> {
    state.capacity_reservations.get(reference).or_else(|| {
        state
            .capacity_reservations
            .values()
            .find(|r| r.name == reference)
    })
}

/// Whether an instance in `status` holds reserved capacity.
fn holds_capacity(status: &str) -> bool {
    !matches!(
        status,
        "stopped" | "stopping" | "deleting" | "failed" | "deleted"
    )
}

/// Units of `reservation_id` held per Availability Zone, by every instance
/// except `exclude` (an instance whose placement is being recomputed).
fn units_in_use(
    state: &RdsState,
    reservation_id: &str,
    exclude: Option<&str>,
) -> BTreeMap<String, i32> {
    let mut used: BTreeMap<String, i32> = BTreeMap::new();
    for instance in state.instances.values() {
        if Some(instance.db_instance_identifier.as_str()) == exclude
            || !holds_capacity(&instance.db_instance_status)
        {
            continue;
        }
        let mut held: BTreeSet<&str> = BTreeSet::new();
        let current = instance.capacity_reservation.iter();
        let pending = instance
            .pending_modified_values
            .iter()
            .filter_map(|p| p.capacity_reservation.as_ref());
        for used_by in current.chain(pending) {
            if used_by.identifier == reservation_id {
                held.extend(used_by.availability_zones.iter().map(String::as_str));
            }
        }
        for az in held {
            *used.entry(az.to_string()).or_default() += 1;
        }
    }
    used
}

/// Unused units per Availability Zone: zero everywhere unless the
/// reservation is active.
fn available_capacity(
    state: &RdsState,
    reservation: &DbCapacityReservation,
    now: DateTime<Utc>,
    exclude: Option<&str>,
) -> Vec<(String, i32)> {
    let active = reservation.state_at(now) == "active";
    let used = units_in_use(state, &reservation.identifier, exclude);
    reservation
        .availability_zones
        .iter()
        .map(|az| {
            let free = if active {
                (reservation.capacity_per_az - used.get(az).copied().unwrap_or(0)).max(0)
            } else {
                0
            };
            (az.clone(), free)
        })
        .collect()
}

/// What a DB instance needs from the reservation it names.
pub(crate) struct CapacityClaim<'a> {
    /// The instance whose current units are disregarded (Modify / Start).
    pub instance: Option<&'a str>,
    pub db_instance_class: &'a str,
    pub engine: &'a str,
    /// The instance's license model, when the request makes it relevant.
    pub license_model: Option<&'a str>,
    /// The Availability Zone the instance is (or must be) placed in.
    pub availability_zone: Option<&'a str>,
    pub multi_az: bool,
}

/// Validate that the reservation `reference` can place an instance as
/// described by `claim`, and pick the Availability Zones it holds a unit
/// in (the instance's own zone first).
pub(crate) fn claim_capacity(
    state: &RdsState,
    reference: &str,
    claim: &CapacityClaim<'_>,
) -> Result<CapacityReservationUse, AwsServiceError> {
    let now = Utc::now();
    let reservation = find_capacity_reservation(state, reference)
        .ok_or_else(|| capacity_reservation_not_found(reference))?;
    let id = &reservation.identifier;
    let current = reservation.state_at(now);
    if current != "active" {
        return Err(invalid_reservation_state(format!(
            "DB capacity reservation {id} is {current}; only an active reservation can be used."
        )));
    }
    if reservation.db_instance_class != claim.db_instance_class
        || reservation.engine != claim.engine
    {
        return Err(invalid_reservation_state(format!(
            "DB capacity reservation {id} reserves {} {} capacity, not {} {}.",
            reservation.engine,
            reservation.db_instance_class,
            claim.engine,
            claim.db_instance_class
        )));
    }
    if let (Some(reserved), Some(wanted)) =
        (reservation.license_model.as_deref(), claim.license_model)
    {
        if reserved != wanted {
            return Err(invalid_reservation_state(format!(
                "DB capacity reservation {id} reserves capacity for license model {reserved}, not {wanted}."
            )));
        }
    }
    if let Some(az) = claim.availability_zone {
        if !reservation.availability_zones.iter().any(|z| z == az) {
            return Err(invalid_reservation_state(format!(
                "DB capacity reservation {id} doesn't cover Availability Zone {az}."
            )));
        }
    }
    if claim.multi_az && reservation.availability_zones.len() < 2 {
        return Err(invalid_reservation_state(format!(
            "DB capacity reservation {id} covers one Availability Zone; a Multi-AZ DB instance needs two."
        )));
    }
    let free = available_capacity(state, reservation, now, claim.instance);
    let has_room = |az: &str| free.iter().any(|(z, n)| z == az && *n > 0);
    let primary = match claim.availability_zone {
        Some(az) if has_room(az) => az.to_string(),
        Some(az) => {
            return Err(insufficient_capacity(format!(
                "DB capacity reservation {id} has no available capacity in {az}."
            )))
        }
        None => free
            .iter()
            .find(|(_, n)| *n > 0)
            .map(|(z, _)| z.clone())
            .ok_or_else(|| {
                insufficient_capacity(format!(
                    "DB capacity reservation {id} has no available capacity."
                ))
            })?,
    };
    let mut zones = vec![primary.clone()];
    if claim.multi_az {
        let secondary = free
            .iter()
            .find(|(z, n)| *z != primary && *n > 0)
            .map(|(z, _)| z.clone())
            .ok_or_else(|| {
                insufficient_capacity(format!(
                    "DB capacity reservation {id} has no available capacity for a Multi-AZ standby."
                ))
            })?;
        zones.push(secondary);
    }
    Ok(CapacityReservationUse {
        identifier: id.clone(),
        availability_zones: zones,
    })
}

/// The reservation a starting instance returns to: the named one (which
/// must place it), else the one it kept when it stopped while that is still
/// active (with room in its zone), else none (unreserved capacity).
pub(crate) fn capacity_on_start(
    state: &RdsState,
    instance: &DbInstance,
    requested: Option<&str>,
) -> Result<Option<CapacityReservationUse>, AwsServiceError> {
    let claim = CapacityClaim {
        instance: Some(&instance.db_instance_identifier),
        db_instance_class: &instance.db_instance_class,
        engine: &instance.engine,
        license_model: None,
        availability_zone: instance.availability_zone.as_deref(),
        multi_az: instance.multi_az,
    };
    if let Some(reference) = requested {
        return claim_capacity(state, reference, &claim).map(Some);
    }
    let Some(kept) = instance.capacity_reservation.as_ref() else {
        return Ok(None);
    };
    match state.capacity_reservations.get(&kept.identifier) {
        Some(r) if r.state_at(Utc::now()) == "active" => {
            claim_capacity(state, &kept.identifier, &claim).map(Some)
        }
        // Deleted, ended or cancelled: start outside the reservation.
        _ => Ok(None),
    }
}

/// Place a DB instance being restored on the reservation its request names
/// (`DBCapacityReservationIdentifier`), choosing its zone when it has none.
/// On failure the instance's identifier reservation is rolled back.
pub(crate) fn place_on_requested_capacity(
    state: &mut RdsState,
    request: &AwsRequest,
    instance: &mut DbInstance,
) -> Result<(), AwsServiceError> {
    let Some(reference) = optional_query_param(request, "DBCapacityReservationIdentifier") else {
        return Ok(());
    };
    let license = optional_query_param(request, "LicenseModel");
    let claim = CapacityClaim {
        instance: None,
        db_instance_class: &instance.db_instance_class,
        engine: &instance.engine,
        license_model: license.as_deref(),
        availability_zone: instance.availability_zone.as_deref(),
        multi_az: instance.multi_az,
    };
    match claim_capacity(state, &reference, &claim) {
        Ok(used) => {
            if instance.availability_zone.is_none() {
                instance.availability_zone = used.availability_zones.first().cloned();
            }
            instance.capacity_reservation = Some(used);
            Ok(())
        }
        Err(e) => {
            state.cancel_instance_creation(&instance.db_instance_identifier);
            Err(e)
        }
    }
}

/// What a ModifyDBInstance does to the instance's reserved capacity.
pub(crate) enum CapacityChange {
    /// Nothing about the instance's placement changes.
    Keep,
    /// The instance moves to other hardware without naming a
    /// reservation: it lands on unreserved capacity.
    Release,
    /// The instance moves onto (or stays on, re-placed within) a
    /// reservation.
    Move(CapacityReservationUse),
}

/// The settings of a ModifyDBInstance that bear on reserved capacity.
pub(crate) struct ModifyCapacity<'a> {
    pub requested: Option<&'a str>,
    pub db_instance_class: Option<&'a str>,
    pub multi_az: Option<bool>,
    pub license_model: Option<&'a str>,
    pub db_subnet_group_name: Option<&'a str>,
}

/// Validate a ModifyDBInstance against the reservation it names (or the
/// one the instance leaves).
pub(crate) fn plan_modify_capacity(
    state: &RdsState,
    instance: &DbInstance,
    modify: &ModifyCapacity<'_>,
) -> Result<CapacityChange, AwsServiceError> {
    let class_changes = modify
        .db_instance_class
        .is_some_and(|c| c != instance.db_instance_class);
    let multi_az_changes = modify.multi_az.is_some_and(|m| m != instance.multi_az);
    let license_changes = modify
        .license_model
        .is_some_and(|l| instance.license_model.as_deref() != Some(l));
    let moves_hardware = class_changes || multi_az_changes || license_changes;
    let Some(reference) = modify.requested else {
        return Ok(
            if moves_hardware && instance.capacity_reservation.is_some() {
                CapacityChange::Release
            } else {
                CapacityChange::Keep
            },
        );
    };
    let target = find_capacity_reservation(state, reference)
        .ok_or_else(|| capacity_reservation_not_found(reference))?;
    let same = instance
        .capacity_reservation
        .as_ref()
        .is_some_and(|u| u.identifier == target.identifier);
    if same && !moves_hardware {
        return Ok(CapacityChange::Keep);
    }
    if !same && !moves_hardware {
        return Err(invalid_reservation_state(format!(
            "Moving DB instance {} onto DB capacity reservation {} requires a new DBInstanceClass, MultiAZ or LicenseModel value.",
            instance.db_instance_identifier, target.identifier
        )));
    }
    if modify
        .db_subnet_group_name
        .is_some_and(|g| instance.db_subnet_group_name.as_deref() != Some(g))
    {
        return Err(invalid_reservation_state(
            "DBCapacityReservationIdentifier can't be combined with a change of DBSubnetGroupName."
                .to_string(),
        ));
    }
    if class_changes && license_changes {
        return Err(invalid_reservation_state(
            "DBCapacityReservationIdentifier can't be combined with a change of both DBInstanceClass and LicenseModel; submit them separately."
                .to_string(),
        ));
    }
    let claim = CapacityClaim {
        instance: Some(&instance.db_instance_identifier),
        db_instance_class: modify
            .db_instance_class
            .unwrap_or(&instance.db_instance_class),
        engine: &instance.engine,
        license_model: modify.license_model,
        availability_zone: instance.availability_zone.as_deref(),
        multi_az: modify.multi_az.unwrap_or(instance.multi_az),
    };
    claim_capacity(state, reference, &claim).map(CapacityChange::Move)
}

fn capacity_reservation_xml(
    state: &RdsState,
    reservation: &DbCapacityReservation,
    now: DateTime<Utc>,
) -> String {
    let available: String = available_capacity(state, reservation, now, None)
        .into_iter()
        .map(|(az, n)| {
            format!(
                "<AvailableCapacityPerAvailabilityZone><AvailabilityZone>{}</AvailabilityZone><AvailableCapacity>{n}</AvailableCapacity></AvailableCapacityPerAvailabilityZone>",
                xml_escape(&az)
            )
        })
        .collect();
    let opt_ts = |tag: &str, t: Option<DateTime<Utc>>| {
        t.map(|t| format!("<{tag}>{}</{tag}>", ts(&t)))
            .unwrap_or_default()
    };
    let license = reservation
        .license_model
        .as_ref()
        .map(|l| format!("<LicenseModel>{}</LicenseModel>", xml_escape(l)))
        .unwrap_or_default();
    let commitment = match (
        reservation.committed_capacity_per_az,
        reservation.commitment_end_date,
    ) {
        (Some(n), Some(end)) => format!(
            "<CommitmentInfo><CommittedCapacityPerAvailabilityZone>{n}</CommittedCapacityPerAvailabilityZone><CommitmentEndDate>{}</CommitmentEndDate></CommitmentInfo>",
            ts(&end)
        ),
        _ => String::new(),
    };
    format!(
        "<DBCapacityReservationName>{name}</DBCapacityReservationName>\
         <DBInstanceClass>{class}</DBInstanceClass>\
         <Engine>{engine}</Engine>\
         {license}\
         <ReservedCapacityPerAvailabilityZone>{cap}</ReservedCapacityPerAvailabilityZone>\
         <AvailableCapacityPerAvailabilityZoneList>{available}</AvailableCapacityPerAvailabilityZoneList>\
         {end}\
         <CreateDate>{created}</CreateDate>\
         {start}\
         {commitment}\
         <State>{state}</State>\
         <DBCapacityReservationArn>{arn}</DBCapacityReservationArn>\
         <DBCapacityReservationIdentifier>{id}</DBCapacityReservationIdentifier>\
         <TagList>{tags}</TagList>",
        name = xml_escape(&reservation.name),
        class = xml_escape(&reservation.db_instance_class),
        engine = xml_escape(&reservation.engine),
        cap = reservation.capacity_per_az,
        end = opt_ts("EndDate", reservation.end_date),
        created = ts(&reservation.create_date),
        start = opt_ts("StartDate", reservation.start_date),
        state = reservation.state_at(now),
        arn = xml_escape(&reservation.arn),
        id = xml_escape(&reservation.identifier),
        tags = reservation.tags.iter().map(tag_xml).collect::<String>(),
    )
}

fn reservation_matches_filters(
    reservation: &DbCapacityReservation,
    filters: &[RdsFilter],
    now: DateTime<Utc>,
) -> bool {
    filters.iter().all(|filter| match filter.name.as_str() {
        "db-capacity-reservation-name" => filter.matches(Some(&reservation.name)),
        // `expired` isn't a filterable state.
        "state" => {
            let current = reservation.state_at(now);
            current != "expired" && filter.matches(Some(current))
        }
        "engine" => filter.matches(Some(&reservation.engine)),
        "db-instance-class" => filter.matches(Some(&reservation.db_instance_class)),
        "availability-zone" => reservation
            .availability_zones
            .iter()
            .any(|az| filter.matches(Some(az))),
        // A filter name outside the documented set, or one with no
        // values, matches nothing.
        _ => false,
    } && !filter.values.is_empty())
}

fn reservation_response(action: &str, body: String, request: &AwsRequest) -> AwsResponse {
    AwsResponse::xml(
        StatusCode::OK,
        query_response_xml(
            action,
            RDS_NS,
            &format!("<DBCapacityReservation>{body}</DBCapacityReservation>"),
            &request.request_id,
        ),
    )
}

impl RdsService {
    pub(super) fn create_db_capacity_reservation(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "DBCapacityReservationName")?;
        let db_instance_class = required_query_param(request, "DBInstanceClass")?;
        let engine = required_query_param(request, "Engine")?;
        let capacity_raw = required_query_param(request, "CapacityPerAvailabilityZone")?;
        let availability_zones = {
            let mut zones = parse_string_member_list(request, "AvailabilityZones");
            if zones.is_empty() {
                for i in 1.. {
                    match optional_query_param(
                        request,
                        &format!("AvailabilityZones.AvailabilityZone.{i}"),
                    ) {
                        Some(z) => zones.push(z),
                        None => break,
                    }
                }
            }
            zones
        };
        let start_date = optional_timestamp(request, "StartDate")?;
        let end_date = optional_timestamp(request, "EndDate")?;
        let commitment_duration = optional_query_param(request, "CommitmentDuration")
            .map(|v| {
                v.parse::<i64>().map_err(|_| {
                    invalid_param(&format!("CommitmentDuration '{v}' is not a valid integer."))
                })
            })
            .transpose()?;
        let tags = parse_tags(request)?;

        if !valid_reservation_name(&name) {
            return Err(invalid_param(&format!(
                "DBCapacityReservationName '{name}' must be 1 to 63 lowercase letters, digits or hyphens, \
                 start with a letter, not end with a hyphen or contain two consecutive hyphens."
            )));
        }
        let capacity_per_az = capacity_raw
            .parse::<i32>()
            .ok()
            .filter(|n| *n >= 1)
            .ok_or_else(|| {
                invalid_param(&format!(
                    "CapacityPerAvailabilityZone '{capacity_raw}' must be an integer of at least 1."
                ))
            })?;
        if availability_zones.is_empty() {
            return Err(missing_param("AvailabilityZones"));
        }
        let mut seen = BTreeSet::new();
        for az in &availability_zones {
            if !seen.insert(az.as_str()) {
                return Err(invalid_param(&format!(
                    "Availability Zone {az} appears more than once."
                )));
            }
            let in_region = az
                .strip_prefix(request.region.as_str())
                .is_some_and(|suffix| {
                    suffix.len() == 1 && suffix.chars().all(|c| c.is_ascii_lowercase())
                });
            if !in_region {
                return Err(invalid_param(&format!(
                    "Availability Zone {az} isn't in Region {}.",
                    request.region
                )));
            }
        }
        if !RESERVATION_ENGINES.contains(&engine.as_str()) {
            return Err(reservation_unsupported(format!(
                "Engine {engine} isn't supported for DB capacity reservations."
            )));
        }
        validate_db_instance_class(&db_instance_class).map_err(|_| {
            reservation_unsupported(format!(
                "DB instance class {db_instance_class} isn't supported for DB capacity reservations."
            ))
        })?;
        let license_model =
            reservation_license_model(&engine, optional_query_param(request, "LicenseModel"))?;

        let now = Utc::now();
        if end_date.is_some_and(|end| end <= now) {
            return Err(invalid_param("EndDate must be in the future."));
        }
        let (committed_capacity_per_az, commitment_end_date) =
            match (start_date, commitment_duration) {
                (None, None) => (None, None),
                (Some(_), None) | (None, Some(_)) => {
                    return Err(invalid_param(
                        "StartDate and CommitmentDuration must be specified together.",
                    ))
                }
                (Some(start), Some(duration)) => {
                    validate_future_dated(
                        &db_instance_class,
                        capacity_per_az,
                        start,
                        duration,
                        now,
                    )?;
                    let commitment_end = start + Duration::seconds(duration);
                    if end_date.is_some_and(|end| end < commitment_end) {
                        return Err(invalid_param(
                            "EndDate must be at or after StartDate plus CommitmentDuration.",
                        ));
                    }
                    (Some(capacity_per_az), Some(commitment_end))
                }
            };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        if state.capacity_reservations.values().any(|r| r.name == name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DBCapacityReservationAlreadyExists",
                format!("DB capacity reservation {name} already exists."),
            ));
        }
        let identifier = format!("dbcr-{}", uuid::Uuid::new_v4().simple());
        let reservation = DbCapacityReservation {
            arn: rds_arn(
                &request.region,
                &request.account_id,
                "db-capacity-reservation",
                &identifier,
            ),
            identifier: identifier.clone(),
            name,
            db_instance_class,
            engine,
            license_model,
            capacity_per_az,
            availability_zones,
            start_date,
            end_date,
            committed_capacity_per_az,
            commitment_end_date,
            cancelled: false,
            create_date: now,
            tags,
        };
        let xml = capacity_reservation_xml(state, &reservation, now);
        state.capacity_reservations.insert(identifier, reservation);
        Ok(reservation_response(
            "CreateDBCapacityReservation",
            xml,
            request,
        ))
    }

    pub(super) fn describe_db_capacity_reservations(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let reference = optional_query_param(request, "DBCapacityReservationIdentifier");
        let filters = parse_filters(request);
        warn_unknown_filters(&filters, RESERVATION_FILTERS);
        let accounts = self.state.read();
        let empty = RdsState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let now = Utc::now();
        let mut matching: Vec<&DbCapacityReservation> = match reference.as_deref() {
            Some(r) => vec![find_capacity_reservation(state, r)
                .ok_or_else(|| capacity_reservation_not_found(r))?],
            None => state.capacity_reservations.values().collect(),
        };
        matching.retain(|r| reservation_matches_filters(r, &filters, now));
        matching.sort_by(|a, b| {
            a.create_date
                .cmp(&b.create_date)
                .then_with(|| a.identifier.cmp(&b.identifier))
        });
        let page = paginate(
            matching,
            optional_query_param(request, "Marker"),
            optional_query_param(request, "MaxRecords"),
            |r| r.identifier.as_str(),
        )?;
        let items: String = page
            .items
            .iter()
            .map(|r| {
                format!(
                    "<member>{}</member>",
                    capacity_reservation_xml(state, r, now)
                )
            })
            .collect();
        let marker = page
            .next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeDBCapacityReservations",
                RDS_NS,
                &format!("<DBCapacityReservations>{items}</DBCapacityReservations>{marker}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn modify_db_capacity_reservation(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let reference = required_query_param(request, "DBCapacityReservationIdentifier")?;
        let capacity = optional_query_param(request, "CapacityPerAvailabilityZone")
            .map(|v| {
                v.parse::<i32>().ok().filter(|n| *n >= 1).ok_or_else(|| {
                    invalid_param(&format!(
                        "CapacityPerAvailabilityZone '{v}' must be an integer of at least 1."
                    ))
                })
            })
            .transpose()?;
        let end_date_type = optional_query_param(request, "EndDateType");
        let end_date = optional_timestamp(request, "EndDate")?;
        if capacity.is_none() && end_date_type.is_none() && end_date.is_none() {
            return Err(invalid_param(
                "Specify CapacityPerAvailabilityZone, EndDateType, or EndDate.",
            ));
        }
        // Some(None) clears the end date, Some(Some(t)) sets it.
        let new_end = match (end_date_type.as_deref(), end_date) {
            (None, None) => None,
            (None, Some(_)) => return Err(invalid_param("EndDate requires EndDateType limited.")),
            (Some("limited"), Some(t)) => Some(Some(t)),
            (Some("limited"), None) => {
                return Err(invalid_param("EndDateType limited requires EndDate."))
            }
            (Some("unlimited"), None) => Some(None),
            (Some("unlimited"), Some(_)) => {
                return Err(invalid_param(
                    "EndDate can't be specified with EndDateType unlimited.",
                ))
            }
            (Some(other), _) => {
                return Err(invalid_param(&format!(
                    "Invalid EndDateType '{other}'. Valid values: limited, unlimited."
                )))
            }
        };

        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let reservation = find_capacity_reservation(state, &reference)
            .ok_or_else(|| capacity_reservation_not_found(&reference))?;
        let id = reservation.identifier.clone();
        let current = reservation.state_at(now);
        if current != "active" {
            return Err(invalid_reservation_state(format!(
                "DB capacity reservation {id} is {current}; only an active reservation can be modified."
            )));
        }
        if let Some(n) = capacity {
            let in_use = units_in_use(state, &id, None)
                .into_values()
                .max()
                .unwrap_or(0);
            if n < in_use {
                return Err(invalid_reservation_state(format!(
                    "CapacityPerAvailabilityZone {n} is below the {in_use} DB instances using reservation {id} in one Availability Zone."
                )));
            }
            if reservation.in_commitment(now)
                && reservation.committed_capacity_per_az.is_some_and(|c| n < c)
            {
                return Err(invalid_reservation_state(format!(
                    "CapacityPerAvailabilityZone {n} is below the committed capacity of reservation {id}."
                )));
            }
        }
        if let Some(Some(end)) = new_end {
            if end <= now {
                return Err(invalid_param("EndDate must be in the future."));
            }
            if reservation.in_commitment(now)
                && reservation.commitment_end_date.is_some_and(|c| end < c)
            {
                return Err(invalid_param(
                    "EndDate must be at or after the end of the capacity commitment.",
                ));
            }
        }
        let reservation = state
            .capacity_reservations
            .get_mut(&id)
            .expect("resolved above");
        if let Some(n) = capacity {
            reservation.capacity_per_az = n;
        }
        if let Some(end) = new_end {
            reservation.end_date = end;
        }
        let reservation = reservation.clone();
        let xml = capacity_reservation_xml(state, &reservation, now);
        Ok(reservation_response(
            "ModifyDBCapacityReservation",
            xml,
            request,
        ))
    }

    pub(super) fn cancel_db_capacity_reservation(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let reference = required_query_param(request, "DBCapacityReservationIdentifier")?;
        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let reservation = find_capacity_reservation(state, &reference)
            .ok_or_else(|| capacity_reservation_not_found(&reference))?;
        let id = reservation.identifier.clone();
        let current = reservation.state_at(now);
        if current != "active" {
            return Err(invalid_reservation_state(format!(
                "DB capacity reservation {id} is {current} and can't be cancelled."
            )));
        }
        if reservation.in_commitment(now) {
            return Err(invalid_reservation_state(format!(
                "DB capacity reservation {id} is within its capacity commitment and can't be cancelled."
            )));
        }
        // Running instances continue on unreserved capacity; a stopped one
        // keeps the reference and starts outside the reservation.
        for instance in state.instances.values_mut() {
            if holds_capacity(&instance.db_instance_status)
                && instance
                    .capacity_reservation
                    .as_ref()
                    .is_some_and(|u| u.identifier == id)
            {
                instance.capacity_reservation = None;
            }
            if let Some(pending) = instance.pending_modified_values.as_mut() {
                if pending
                    .capacity_reservation
                    .as_ref()
                    .is_some_and(|u| u.identifier == id)
                {
                    pending.capacity_reservation = None;
                }
            }
        }
        let reservation = state
            .capacity_reservations
            .get_mut(&id)
            .expect("resolved above");
        reservation.cancelled = true;
        let reservation = reservation.clone();
        let xml = capacity_reservation_xml(state, &reservation, now);
        Ok(reservation_response(
            "CancelDBCapacityReservation",
            xml,
            request,
        ))
    }
}

/// The constraints a future-dated reservation (one with `StartDate` and
/// `CommitmentDuration`) must meet.
fn validate_future_dated(
    db_instance_class: &str,
    capacity_per_az: i32,
    start: DateTime<Utc>,
    duration: i64,
    now: DateTime<Utc>,
) -> Result<(), AwsServiceError> {
    let Some((family, generation)) = instance_class_family(db_instance_class) else {
        return Err(reservation_unsupported(format!(
            "DB instance class {db_instance_class} isn't supported for a future-dated reservation."
        )));
    };
    let family_ok = match family {
        'c' | 't' | 'x' => true,
        'm' | 'r' => generation.is_some_and(|g| g >= 7),
        _ => false,
    };
    if !family_ok {
        return Err(reservation_unsupported(format!(
            "DB instance class {db_instance_class} isn't supported for a future-dated reservation: \
             it must be a db.c, db.t or db.x class, or a db.m or db.r class of generation 7 or later."
        )));
    }
    let vcpus = instance_class_vcpus(db_instance_class).ok_or_else(|| {
        reservation_unsupported(format!(
            "DB instance class {db_instance_class} isn't supported for a future-dated reservation."
        ))
    })?;
    if vcpus * i64::from(capacity_per_az) < 32 {
        return Err(reservation_unsupported(format!(
            "A future-dated reservation must total at least 32 vCPUs in each Availability Zone; \
             {capacity_per_az} x {db_instance_class} is {}.",
            vcpus * i64::from(capacity_per_az)
        )));
    }
    let min_days = if family == 'x' { 84 } else { 14 };
    if duration < min_days * DAY_SECONDS || duration >= 315_576_000 {
        return Err(invalid_param(&format!(
            "CommitmentDuration must be at least {} seconds ({min_days} days) and less than 315576000 seconds.",
            min_days * DAY_SECONDS
        )));
    }
    let lead = start - now;
    if lead < Duration::days(5) || lead > Duration::days(120) {
        return Err(invalid_param(
            "StartDate must be from 5 to 120 days in the future.",
        ));
    }
    Ok(())
}
