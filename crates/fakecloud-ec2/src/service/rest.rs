//! Long-tail remainder sweep: every remaining EC2 operation. Most handlers
//! validate wire-observable inputs (required scalars, enums, MaxResults) and
//! return a minimal response; EC2 output members are all optional so an empty
//! body deserializes cleanly.
//!
//! The `Modify*`/`Create*` families that own a small piece of state (id-format
//! settings, managed prefix lists, instance event windows, traffic mirroring,
//! VPC block-public-access, default credit specs, route servers, VPC encryption
//! controls, FPGA images, ...) persist their changes into [`crate::state`] so
//! the paired `Describe*`/`Get*` reflects them — a real round-trip rather than a
//! validate-then-drop acknowledgement.
#![allow(clippy::too_many_lines)]

use fakecloud_aws::ec2query::{ec2_elem, ec2_elem_opt, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, require, require_struct, validate_enum, validate_int_range,
    validate_length, validate_max_results,
};
use crate::state::{
    Ec2State, EventWindowTimeRange, FpgaImage, InstanceEventWindow, ManagedPrefixList,
    PrefixListEntry, RouteServer, Tag, TrafficMirrorFilter, TrafficMirrorFilterRule,
    TrafficMirrorSession, TrafficMirrorTarget, VpcBpaExclusion, VpcEncryptionControl,
};

/// `us-east-1` fallback for a request with no explicit region.
fn region_of(req: &AwsRequest) -> String {
    if req.region.is_empty() {
        "us-east-1".to_string()
    } else {
        req.region.clone()
    }
}

/// Collect a 1-based list of `{prefix}.{N}.{field}` values (e.g.
/// `LoadPermission.Add.1.UserId`), stopping at the first absent index.
fn nested_indexed(req: &AwsRequest, prefix: &str, field: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 1usize;
    while let Some(v) = req
        .query_params
        .get(&format!("{prefix}.{i}.{field}"))
        .filter(|v| !v.is_empty())
    {
        out.push(v.clone());
        i += 1;
    }
    out
}

/// Parse a `<prefix>.FromPort` / `<prefix>.ToPort` port-range pair.
fn parse_port_range(req: &AwsRequest, prefix: &str) -> Option<(i64, i64)> {
    let from = req
        .query_params
        .get(&format!("{prefix}.FromPort"))
        .and_then(|v| v.parse::<i64>().ok());
    let to = req
        .query_params
        .get(&format!("{prefix}.ToPort"))
        .and_then(|v| v.parse::<i64>().ok());
    match (from, to) {
        (Some(f), Some(t)) => Some((f, t)),
        (Some(f), None) => Some((f, f)),
        (None, Some(t)) => Some((t, t)),
        (None, None) => None,
    }
}

// These four operations exist in the Smithy model but are not yet exposed by the
// vendored aws-sdk-ec2, so they have no L2 conformance test and are kept out of
// SUPPORTED_ACTIONS. They are still dispatched and validated so the L1 probe
// harness reaches 100% and a real client hitting the wire gets a faithful
// response; they graduate into SUPPORTED_ACTIONS on the next SDK refresh.
pub(crate) fn create_capacity_reservation_cancellation_quote(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CapacityReservationId")?;
    Ok(Ec2Service::respond(
        "CreateCapacityReservationCancellationQuote",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_capacity_reservation_cancellation_quotes(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeCapacityReservationCancellationQuotes",
        &req.request_id,
        "",
    ))
}

fn ipam_pool_allocation_xml(cidr: &str, alloc_id: &str, description: Option<&str>) -> String {
    format!(
        "{}{}{}",
        ec2_elem("cidr", cidr),
        ec2_elem("ipamPoolAllocationId", alloc_id),
        ec2_elem_opt("description", description),
    )
}

pub(crate) fn describe_ipam_pool_allocations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1000, 100000)?;
    let wanted = indexed_list(&req.query_params, "IpamPoolAllocationId");
    // The pool can be narrowed by IpamPoolId when the caller provides it.
    let pool_filter = req.query_params.get("IpamPoolId").filter(|v| !v.is_empty());
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = Vec::new();
    for (pool, allocs) in &state.ipam_pool_allocations {
        if pool_filter.is_some_and(|p| p != pool) {
            continue;
        }
        for (cidr, alloc_id) in allocs {
            if !wanted.is_empty() && !wanted.contains(alloc_id) {
                continue;
            }
            items.push(ipam_pool_allocation_xml(
                cidr,
                alloc_id,
                state
                    .ipam_allocation_descriptions
                    .get(alloc_id)
                    .map(String::as_str),
            ));
        }
    }
    Ok(Ec2Service::respond(
        "DescribeIpamPoolAllocations",
        &req.request_id,
        &ec2_list("ipamPoolAllocationSet", &items),
    ))
}

pub(crate) fn modify_ipam_pool_allocation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let alloc_id = require(&req.query_params, "IpamPoolAllocationId")?;
    let description = req.query_params.get("Description").cloned();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Locate the allocation across pools so the response echoes its real CIDR.
    let found = state
        .ipam_pool_allocations
        .values()
        .flatten()
        .find(|(_, a)| a == &alloc_id)
        .map(|(c, _)| c.clone());
    let Some(cidr) = found else {
        return Err(crate::service_helpers::not_found(
            "InvalidIpamPoolAllocationId.NotFound",
            &alloc_id,
        ));
    };
    if let Some(d) = description.clone() {
        state
            .ipam_allocation_descriptions
            .insert(alloc_id.clone(), d);
    }
    let stored = state.ipam_allocation_descriptions.get(&alloc_id).cloned();
    Ok(Ec2Service::respond(
        "ModifyIpamPoolAllocation",
        &req.request_id,
        &format!(
            "<ipamPoolAllocation>{}</ipamPoolAllocation>",
            ipam_pool_allocation_xml(&cidr, &alloc_id, stored.as_deref())
        ),
    ))
}

pub(crate) fn advertise_byoip_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Cidr")?;
    Ok(Ec2Service::respond(
        "AdvertiseByoipCidr",
        &req.request_id,
        "",
    ))
}

pub(crate) fn associate_enclave_certificate_iam_role(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CertificateArn")?;
    require(&req.query_params, "RoleArn")?;
    Ok(Ec2Service::respond(
        "AssociateEnclaveCertificateIamRole",
        &req.request_id,
        "",
    ))
}

pub(crate) fn associate_iam_instance_profile(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    Ok(Ec2Service::respond(
        "AssociateIamInstanceProfile",
        &req.request_id,
        "",
    ))
}

pub(crate) fn associate_instance_event_window(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceEventWindowId")?;
    let instances = indexed_list(&req.query_params, "AssociationTarget.InstanceId");
    let hosts = indexed_list(&req.query_params, "AssociationTarget.DedicatedHostId");
    let tags =
        crate::service_helpers::parse_tag_pairs(&req.query_params, "AssociationTarget.InstanceTag");
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(entry) = state.instance_event_windows.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidInstanceEventWindowId.NotFound",
            &id,
        ));
    };
    for i in instances {
        if !entry.assoc_instance_ids.contains(&i) {
            entry.assoc_instance_ids.push(i);
        }
    }
    for h in hosts {
        if !entry.assoc_dedicated_host_ids.contains(&h) {
            entry.assoc_dedicated_host_ids.push(h);
        }
    }
    for (k, v) in tags {
        let value = v.unwrap_or_default();
        if !entry.assoc_tags.iter().any(|t| t.key == k) {
            entry.assoc_tags.push(Tag { key: k, value });
        }
    }
    let w = entry.clone();
    let t = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "AssociateInstanceEventWindow",
        &req.request_id,
        &format!(
            "<instanceEventWindow>{}</instanceEventWindow>",
            instance_event_window_xml(&w, &t)
        ),
    ))
}

pub(crate) fn associate_route_server(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "AssociateRouteServer",
        &req.request_id,
        "",
    ))
}

pub(crate) fn associate_trunk_interface(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "BranchInterfaceId")?;
    require(&req.query_params, "TrunkInterfaceId")?;
    Ok(Ec2Service::respond(
        "AssociateTrunkInterface",
        &req.request_id,
        "",
    ))
}

pub(crate) fn attach_classic_link_vpc(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "AttachClassicLinkVpc",
        &req.request_id,
        "",
    ))
}

pub(crate) fn bundle_instance(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    Ok(Ec2Service::respond("BundleInstance", &req.request_id, ""))
}

pub(crate) fn cancel_bundle_task(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "BundleId")?;
    Ok(Ec2Service::respond("CancelBundleTask", &req.request_id, ""))
}

pub(crate) fn cancel_conversion_task(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ConversionTaskId")?;
    Ok(Ec2Service::respond(
        "CancelConversionTask",
        &req.request_id,
        "",
    ))
}

pub(crate) fn cancel_declarative_policies_report(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ReportId")?;
    Ok(Ec2Service::respond(
        "CancelDeclarativePoliciesReport",
        &req.request_id,
        "",
    ))
}

pub(crate) fn cancel_export_task(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ExportTaskId")?;
    Ok(Ec2Service::respond("CancelExportTask", &req.request_id, ""))
}

pub(crate) fn cancel_import_task(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond("CancelImportTask", &req.request_id, ""))
}

pub(crate) fn confirm_product_instance(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    require(&req.query_params, "ProductCode")?;
    Ok(Ec2Service::respond(
        "ConfirmProductInstance",
        &req.request_id,
        "",
    ))
}

pub(crate) fn copy_fpga_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let source = require(&req.query_params, "SourceFpgaImageId")?;
    require(&req.query_params, "SourceRegion")?;
    let id = gen_id("afi");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let src = state.fpga_images.get(&source).cloned();
        let f = FpgaImage {
            id: id.clone(),
            name: req
                .query_params
                .get("Name")
                .cloned()
                .or_else(|| src.as_ref().map(|s| s.name.clone()))
                .unwrap_or_default(),
            description: req
                .query_params
                .get("Description")
                .cloned()
                .or_else(|| src.as_ref().map(|s| s.description.clone()))
                .unwrap_or_default(),
            load_permission_users: Vec::new(),
            load_permission_groups: Vec::new(),
        };
        state.fpga_images.insert(id.clone(), f);
    }
    Ok(Ec2Service::respond(
        "CopyFpgaImage",
        &req.request_id,
        &ec2_elem("fpgaImageId", &id),
    ))
}

pub(crate) fn copy_volumes(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "SourceVolumeId")?;
    validate_enum(
        &req.query_params,
        "VolumeType",
        &["standard", "io1", "io2", "gp2", "sc1", "st1", "gp3"],
    )?;
    Ok(Ec2Service::respond("CopyVolumes", &req.request_id, ""))
}

pub(crate) fn create_capacity_manager_data_export(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "S3BucketName")?;
    require(&req.query_params, "Schedule")?;
    require(&req.query_params, "OutputFormat")?;
    validate_enum(&req.query_params, "Schedule", &["hourly"])?;
    validate_enum(&req.query_params, "OutputFormat", &["csv", "parquet"])?;
    Ok(Ec2Service::respond(
        "CreateCapacityManagerDataExport",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_delegate_mac_volume_ownership_task(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    require(&req.query_params, "MacCredentials")?;
    Ok(Ec2Service::respond(
        "CreateDelegateMacVolumeOwnershipTask",
        &req.request_id,
        "",
    ))
}

fn fpga_global_id(id: &str) -> String {
    format!("agfi-{}", id.strip_prefix("afi-").unwrap_or(id))
}

fn fpga_image_xml(f: &FpgaImage, tags: &[Tag], owner: &str) -> String {
    let public = f.load_permission_groups.iter().any(|g| g == "all");
    format!(
        "{}{}{}{}<state><code>available</code></state>{}{}{}",
        ec2_elem("fpgaImageId", &f.id),
        ec2_elem("fpgaImageGlobalId", &fpga_global_id(&f.id)),
        ec2_elem("name", &f.name),
        ec2_elem("description", &f.description),
        ec2_elem("ownerId", owner),
        ec2_elem("public", &public.to_string()),
        super::tags::tag_set_xml(tags),
    )
}

fn fpga_image_attribute_xml(f: &FpgaImage) -> String {
    let perms: Vec<String> = f
        .load_permission_users
        .iter()
        .map(|u| ec2_elem("userId", u))
        .chain(
            f.load_permission_groups
                .iter()
                .map(|g| ec2_elem("group", g)),
        )
        .collect();
    format!(
        "<fpgaImageAttribute>{}{}{}{}</fpgaImageAttribute>",
        ec2_elem("fpgaImageId", &f.id),
        ec2_elem("name", &f.name),
        ec2_elem("description", &f.description),
        ec2_list("loadPermissions", &perms),
    )
}

pub(crate) fn create_fpga_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("afi");
    let f = FpgaImage {
        id: id.clone(),
        name: req.query_params.get("Name").cloned().unwrap_or_default(),
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        load_permission_users: Vec::new(),
        load_permission_groups: Vec::new(),
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "fpga-image");
        state.fpga_images.insert(id.clone(), f);
    }
    Ok(Ec2Service::respond(
        "CreateFpgaImage",
        &req.request_id,
        &format!(
            "{}{}",
            ec2_elem("fpgaImageId", &id),
            ec2_elem("fpgaImageGlobalId", &fpga_global_id(&id))
        ),
    ))
}

pub(crate) fn create_image_usage_report(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_length(&req.query_params, "ClientToken", 0, 128)?;
    require(&req.query_params, "ImageId")?;
    Ok(Ec2Service::respond(
        "CreateImageUsageReport",
        &req.request_id,
        "",
    ))
}

/// Collect `TimeRange.N.{StartWeekDay,StartHour,EndWeekDay,EndHour}`.
fn parse_event_window_time_ranges(req: &AwsRequest) -> Vec<EventWindowTimeRange> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        let sw = req.query_params.get(&format!("TimeRange.{i}.StartWeekDay"));
        let sh = req.query_params.get(&format!("TimeRange.{i}.StartHour"));
        let ew = req.query_params.get(&format!("TimeRange.{i}.EndWeekDay"));
        let eh = req.query_params.get(&format!("TimeRange.{i}.EndHour"));
        if sw.is_none() && sh.is_none() && ew.is_none() && eh.is_none() {
            break;
        }
        out.push(EventWindowTimeRange {
            start_week_day: sw.cloned().unwrap_or_default(),
            start_hour: sh.and_then(|v| v.parse().ok()).unwrap_or(0),
            end_week_day: ew.cloned().unwrap_or_default(),
            end_hour: eh.and_then(|v| v.parse().ok()).unwrap_or(0),
        });
        i += 1;
    }
    out
}

fn instance_event_window_xml(w: &InstanceEventWindow, tags: &[Tag]) -> String {
    let time_ranges: Vec<String> = w
        .time_ranges
        .iter()
        .map(|t| {
            format!(
                "{}{}{}{}",
                ec2_elem("startWeekDay", &t.start_week_day),
                ec2_elem("startHour", &t.start_hour.to_string()),
                ec2_elem("endWeekDay", &t.end_week_day),
                ec2_elem("endHour", &t.end_hour.to_string()),
            )
        })
        .collect();
    let assoc_targets = {
        let instances =
            fakecloud_aws::ec2query::ec2_scalar_list("instanceIdSet", &w.assoc_instance_ids);
        let hosts = fakecloud_aws::ec2query::ec2_scalar_list(
            "dedicatedHostIdSet",
            &w.assoc_dedicated_host_ids,
        );
        let tag_items: Vec<String> = w
            .assoc_tags
            .iter()
            .map(|t| format!("{}{}", ec2_elem("key", &t.key), ec2_elem("value", &t.value)))
            .collect();
        format!(
            "<associationTarget>{}{}{}</associationTarget>",
            instances,
            hosts,
            ec2_list("tags", &tag_items)
        )
    };
    format!(
        "{}{}{}{}{}{}{}",
        ec2_elem("instanceEventWindowId", &w.id),
        ec2_list("timeRangeSet", &time_ranges),
        ec2_elem_opt("name", w.name.as_deref()),
        ec2_elem_opt("cronExpression", w.cron_expression.as_deref()),
        assoc_targets,
        ec2_elem("state", &w.state),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_instance_event_window(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("iew");
    let w = InstanceEventWindow {
        id: id.clone(),
        name: req.query_params.get("Name").cloned(),
        cron_expression: req.query_params.get("CronExpression").cloned(),
        time_ranges: parse_event_window_time_ranges(req),
        state: "active".to_string(),
        assoc_instance_ids: Vec::new(),
        assoc_dedicated_host_ids: Vec::new(),
        assoc_tags: Vec::new(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "instance-event-window",
        );
        let t = state.tags_for(&id).to_vec();
        state.instance_event_windows.insert(id.clone(), w.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateInstanceEventWindow",
        &req.request_id,
        &format!(
            "<instanceEventWindow>{}</instanceEventWindow>",
            instance_event_window_xml(&w, &tags)
        ),
    ))
}

pub(crate) fn create_instance_export_task(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    require(&req.query_params, "TargetEnvironment")?;
    validate_enum(
        &req.query_params,
        "TargetEnvironment",
        &["citrix", "vmware", "microsoft"],
    )?;
    Ok(Ec2Service::respond(
        "CreateInstanceExportTask",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_mac_system_integrity_protection_modification_task(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    require(&req.query_params, "MacSystemIntegrityProtectionStatus")?;
    validate_enum(
        &req.query_params,
        "MacSystemIntegrityProtectionStatus",
        &["enabled", "disabled"],
    )?;
    Ok(Ec2Service::respond(
        "CreateMacSystemIntegrityProtectionModificationTask",
        &req.request_id,
        "",
    ))
}

/// Collect `<prefix>.N.Cidr` (+ optional `.Description`) into prefix-list entries.
fn parse_prefix_list_entries(req: &AwsRequest, prefix: &str) -> Vec<PrefixListEntry> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        let cidr_key = format!("{prefix}.{i}.Cidr");
        let Some(cidr) = req.query_params.get(&cidr_key).filter(|v| !v.is_empty()) else {
            break;
        };
        let description = req
            .query_params
            .get(&format!("{prefix}.{i}.Description"))
            .filter(|v| !v.is_empty())
            .cloned();
        out.push(PrefixListEntry {
            cidr: cidr.clone(),
            description,
        });
        i += 1;
    }
    out
}

fn managed_prefix_list_xml(
    p: &ManagedPrefixList,
    tags: &[Tag],
    owner: &str,
    region: &str,
) -> String {
    format!(
        "{}{}{}{}{}{}{}{}{}",
        ec2_elem("prefixListId", &p.prefix_list_id),
        ec2_elem("addressFamily", &p.address_family),
        ec2_elem("state", &p.state),
        ec2_elem(
            "prefixListArn",
            &format!(
                "arn:aws:ec2:{region}:{owner}:prefix-list/{}",
                p.prefix_list_id
            )
        ),
        ec2_elem("prefixListName", &p.prefix_list_name),
        ec2_elem("maxEntries", &p.max_entries.to_string()),
        ec2_elem("version", &p.version.to_string()),
        ec2_elem("ownerId", owner),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_managed_prefix_list(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = require(&req.query_params, "PrefixListName")?;
    let max_entries = require(&req.query_params, "MaxEntries")?
        .parse::<i64>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("MaxEntries must be an integer")
        })?;
    let address_family = require(&req.query_params, "AddressFamily")?;
    let id = gen_id("pl");
    let entries = parse_prefix_list_entries(req, "Entry");
    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut version_history = std::collections::BTreeMap::new();
    version_history.insert(1, entries.clone());
    let pl = ManagedPrefixList {
        prefix_list_id: id.clone(),
        prefix_list_name: name,
        address_family,
        max_entries,
        version: 1,
        state: "create-complete".to_string(),
        entries,
        version_history,
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "prefix-list",
        );
        let t = state.tags_for(&id).to_vec();
        state.managed_prefix_lists.insert(id.clone(), pl.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateManagedPrefixList",
        &req.request_id,
        &format!(
            "<prefixList>{}</prefixList>",
            managed_prefix_list_xml(&pl, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn create_public_ipv4_pool(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "CreatePublicIpv4Pool",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_replace_root_volume_task(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    Ok(Ec2Service::respond(
        "CreateReplaceRootVolumeTask",
        &req.request_id,
        "",
    ))
}

/// Map the `PersistRoutes` request enum to the persisted `persistRoutesState`.
fn persist_routes_state(value: Option<&str>) -> String {
    match value {
        Some("enable") => "ENABLED",
        Some("reset") => "RESETTING",
        _ => "DISABLED",
    }
    .to_string()
}

fn route_server_xml(r: &RouteServer, tags: &[Tag]) -> String {
    format!(
        "{}{}{}{}{}{}{}",
        ec2_elem("routeServerId", &r.id),
        ec2_elem("amazonSideAsn", &r.amazon_side_asn.to_string()),
        ec2_elem("state", &r.state),
        ec2_elem("persistRoutesState", &r.persist_routes_state),
        ec2_elem_opt(
            "persistRoutesDuration",
            r.persist_routes_duration.map(|d| d.to_string()).as_deref()
        ),
        ec2_elem(
            "snsNotificationsEnabled",
            &r.sns_notifications_enabled.to_string()
        ),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_route_server(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let asn = require(&req.query_params, "AmazonSideAsn")?
        .parse::<i64>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("AmazonSideAsn must be an integer")
        })?;
    validate_enum(
        &req.query_params,
        "PersistRoutes",
        &["enable", "disable", "reset"],
    )?;
    let id = gen_id("rs");
    let r = RouteServer {
        id: id.clone(),
        amazon_side_asn: asn,
        state: "available".to_string(),
        persist_routes_state: persist_routes_state(
            req.query_params.get("PersistRoutes").map(String::as_str),
        ),
        persist_routes_duration: req
            .query_params
            .get("PersistRoutesDuration")
            .and_then(|v| v.parse::<i64>().ok()),
        sns_notifications_enabled: req
            .query_params
            .get("SnsNotificationsEnabled")
            .map(|v| v == "true")
            .unwrap_or(false),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "route-server",
        );
        let tg = state.tags_for(&id).to_vec();
        state.route_servers.insert(id.clone(), r.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateRouteServer",
        &req.request_id,
        &format!("<routeServer>{}</routeServer>", route_server_xml(&r, &tags)),
    ))
}

pub(crate) fn create_route_server_endpoint(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    require(&req.query_params, "SubnetId")?;
    Ok(Ec2Service::respond(
        "CreateRouteServerEndpoint",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_route_server_peer(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "BgpOptions")?;
    require(&req.query_params, "RouteServerEndpointId")?;
    require(&req.query_params, "PeerAddress")?;
    Ok(Ec2Service::respond(
        "CreateRouteServerPeer",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_secondary_network(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Ipv4CidrBlock")?;
    require(&req.query_params, "NetworkType")?;
    validate_enum(&req.query_params, "NetworkType", &["rdma"])?;
    Ok(Ec2Service::respond(
        "CreateSecondaryNetwork",
        &req.request_id,
        "",
    ))
}

fn traffic_mirror_filter_rule_xml(r: &TrafficMirrorFilterRule) -> String {
    let port_range = |name: &str, range: Option<(i64, i64)>| {
        range
            .map(|(f, t)| format!("<{name}><fromPort>{f}</fromPort><toPort>{t}</toPort></{name}>"))
            .unwrap_or_default()
    };
    format!(
        "{}{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("trafficMirrorFilterRuleId", &r.id),
        ec2_elem("trafficMirrorFilterId", &r.filter_id),
        ec2_elem("trafficDirection", &r.traffic_direction),
        ec2_elem("ruleNumber", &r.rule_number.to_string()),
        ec2_elem("ruleAction", &r.rule_action),
        ec2_elem_opt("protocol", r.protocol.map(|p| p.to_string()).as_deref()),
        port_range("destinationPortRange", r.destination_port_range),
        port_range("sourcePortRange", r.source_port_range),
        ec2_elem_opt("destinationCidrBlock", r.destination_cidr_block.as_deref()),
        ec2_elem_opt("sourceCidrBlock", r.source_cidr_block.as_deref()),
        ec2_elem_opt("description", r.description.as_deref()),
    )
}

fn traffic_mirror_filter_xml(
    f: &TrafficMirrorFilter,
    rules: &[&TrafficMirrorFilterRule],
    tags: &[Tag],
) -> String {
    let ingress: Vec<String> = rules
        .iter()
        .filter(|r| r.traffic_direction == "ingress")
        .map(|r| traffic_mirror_filter_rule_xml(r))
        .collect();
    let egress: Vec<String> = rules
        .iter()
        .filter(|r| r.traffic_direction == "egress")
        .map(|r| traffic_mirror_filter_rule_xml(r))
        .collect();
    format!(
        "{}{}{}{}{}{}",
        ec2_elem("trafficMirrorFilterId", &f.id),
        ec2_list("ingressFilterRuleSet", &ingress),
        ec2_list("egressFilterRuleSet", &egress),
        fakecloud_aws::ec2query::ec2_scalar_list("networkServiceSet", &f.network_services),
        ec2_elem_opt("description", f.description.as_deref()),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_traffic_mirror_filter(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("tmf");
    let f = TrafficMirrorFilter {
        id: id.clone(),
        description: req
            .query_params
            .get("Description")
            .filter(|v| !v.is_empty())
            .cloned(),
        network_services: Vec::new(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "traffic-mirror-filter",
        );
        let tg = state.tags_for(&id).to_vec();
        state.traffic_mirror_filters.insert(id.clone(), f.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorFilter",
        &req.request_id,
        &format!(
            "<trafficMirrorFilter>{}</trafficMirrorFilter>",
            traffic_mirror_filter_xml(&f, &[], &tags)
        ),
    ))
}

pub(crate) fn create_traffic_mirror_filter_rule(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let filter_id = require(&req.query_params, "TrafficMirrorFilterId")?;
    let traffic_direction = require(&req.query_params, "TrafficDirection")?;
    let rule_number = require(&req.query_params, "RuleNumber")?
        .parse::<i64>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("RuleNumber must be an integer")
        })?;
    let rule_action = require(&req.query_params, "RuleAction")?;
    let destination_cidr_block = Some(require(&req.query_params, "DestinationCidrBlock")?);
    let source_cidr_block = Some(require(&req.query_params, "SourceCidrBlock")?);
    validate_enum(
        &req.query_params,
        "TrafficDirection",
        &["ingress", "egress"],
    )?;
    validate_enum(&req.query_params, "RuleAction", &["accept", "reject"])?;
    let id = gen_id("tmfr");
    let r = TrafficMirrorFilterRule {
        id: id.clone(),
        filter_id,
        traffic_direction,
        rule_number,
        rule_action,
        protocol: req
            .query_params
            .get("Protocol")
            .and_then(|v| v.parse::<i64>().ok()),
        destination_cidr_block,
        source_cidr_block,
        destination_port_range: parse_port_range(req, "DestinationPortRange"),
        source_port_range: parse_port_range(req, "SourcePortRange"),
        description: req
            .query_params
            .get("Description")
            .filter(|v| !v.is_empty())
            .cloned(),
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .traffic_mirror_filter_rules
            .insert(id.clone(), r.clone());
    }
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorFilterRule",
        &req.request_id,
        &format!(
            "<trafficMirrorFilterRule>{}</trafficMirrorFilterRule>",
            traffic_mirror_filter_rule_xml(&r)
        ),
    ))
}

fn traffic_mirror_session_xml(s: &TrafficMirrorSession, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("trafficMirrorSessionId", &s.id),
        ec2_elem("trafficMirrorTargetId", &s.target_id),
        ec2_elem("trafficMirrorFilterId", &s.filter_id),
        ec2_elem("networkInterfaceId", &s.network_interface_id),
        ec2_elem("ownerId", owner),
        ec2_elem_opt(
            "packetLength",
            s.packet_length.map(|p| p.to_string()).as_deref()
        ),
        ec2_elem("sessionNumber", &s.session_number.to_string()),
        ec2_elem_opt(
            "virtualNetworkId",
            s.virtual_network_id.map(|v| v.to_string()).as_deref()
        ),
        ec2_elem_opt("description", s.description.as_deref()),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_traffic_mirror_session(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let network_interface_id = require(&req.query_params, "NetworkInterfaceId")?;
    let target_id = require(&req.query_params, "TrafficMirrorTargetId")?;
    let filter_id = require(&req.query_params, "TrafficMirrorFilterId")?;
    let session_number = require(&req.query_params, "SessionNumber")?
        .parse::<i64>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("SessionNumber must be an integer")
        })?;
    let id = gen_id("tms");
    let owner = req.account_id.clone();
    let s = TrafficMirrorSession {
        id: id.clone(),
        target_id,
        filter_id,
        network_interface_id,
        packet_length: req
            .query_params
            .get("PacketLength")
            .and_then(|v| v.parse::<i64>().ok()),
        session_number,
        virtual_network_id: req
            .query_params
            .get("VirtualNetworkId")
            .and_then(|v| v.parse::<i64>().ok()),
        description: req
            .query_params
            .get("Description")
            .filter(|v| !v.is_empty())
            .cloned(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "traffic-mirror-session",
        );
        let tg = state.tags_for(&id).to_vec();
        state.traffic_mirror_sessions.insert(id.clone(), s.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorSession",
        &req.request_id,
        &format!(
            "<trafficMirrorSession>{}</trafficMirrorSession>",
            traffic_mirror_session_xml(&s, &tags, &owner)
        ),
    ))
}

fn traffic_mirror_target_xml(t: &TrafficMirrorTarget, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}{}{}{}",
        ec2_elem("trafficMirrorTargetId", &t.id),
        ec2_elem_opt("networkInterfaceId", t.network_interface_id.as_deref()),
        ec2_elem_opt(
            "networkLoadBalancerArn",
            t.network_load_balancer_arn.as_deref()
        ),
        ec2_elem("type", &t.target_type),
        ec2_elem_opt("description", t.description.as_deref()),
        ec2_elem("ownerId", owner),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_traffic_mirror_target(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let network_interface_id = req
        .query_params
        .get("NetworkInterfaceId")
        .filter(|v| !v.is_empty())
        .cloned();
    let network_load_balancer_arn = req
        .query_params
        .get("NetworkLoadBalancerArn")
        .filter(|v| !v.is_empty())
        .cloned();
    let gateway_lb_endpoint_id = req
        .query_params
        .get("GatewayLoadBalancerEndpointId")
        .filter(|v| !v.is_empty())
        .cloned();
    let target_type = if network_interface_id.is_some() {
        "network-interface"
    } else if network_load_balancer_arn.is_some() {
        "network-load-balancer"
    } else {
        "gateway-load-balancer-endpoint"
    }
    .to_string();
    let id = gen_id("tmt");
    let owner = req.account_id.clone();
    let t = TrafficMirrorTarget {
        id: id.clone(),
        network_interface_id,
        network_load_balancer_arn,
        gateway_lb_endpoint_id,
        target_type,
        description: req
            .query_params
            .get("Description")
            .filter(|v| !v.is_empty())
            .cloned(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "traffic-mirror-target",
        );
        let tg = state.tags_for(&id).to_vec();
        state.traffic_mirror_targets.insert(id.clone(), t.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorTarget",
        &req.request_id,
        &format!(
            "<trafficMirrorTarget>{}</trafficMirrorTarget>",
            traffic_mirror_target_xml(&t, &tags, &owner)
        ),
    ))
}

fn vpc_bpa_exclusion_xml(e: &VpcBpaExclusion, tags: &[Tag]) -> String {
    format!(
        "{}{}{}{}{}",
        ec2_elem("exclusionId", &e.id),
        ec2_elem(
            "internetGatewayExclusionMode",
            &e.internet_gateway_exclusion_mode
        ),
        ec2_elem_opt("resourceArn", e.resource_arn.as_deref()),
        ec2_elem("state", &e.state),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_vpc_block_public_access_exclusion(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let mode = require(&req.query_params, "InternetGatewayExclusionMode")?;
    validate_enum(
        &req.query_params,
        "InternetGatewayExclusionMode",
        &["allow-bidirectional", "allow-egress"],
    )?;
    let region = region_of(req);
    // The exclusion targets a subnet or a VPC; record its ARN.
    let resource_arn = req
        .query_params
        .get("SubnetId")
        .filter(|v| !v.is_empty())
        .map(|s| format!("arn:aws:ec2:{region}:{}:subnet/{s}", req.account_id))
        .or_else(|| {
            req.query_params
                .get("VpcId")
                .filter(|v| !v.is_empty())
                .map(|v| format!("arn:aws:ec2:{region}:{}:vpc/{v}", req.account_id))
        });
    let id = gen_id("vpcbpa-exclude");
    let e = VpcBpaExclusion {
        id: id.clone(),
        internet_gateway_exclusion_mode: mode,
        resource_arn,
        state: "create-complete".to_string(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "vpc-block-public-access-exclusion",
        );
        let t = state.tags_for(&id).to_vec();
        state.vpc_bpa_exclusions.insert(id.clone(), e.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateVpcBlockPublicAccessExclusion",
        &req.request_id,
        &format!(
            "<vpcBlockPublicAccessExclusion>{}</vpcBlockPublicAccessExclusion>",
            vpc_bpa_exclusion_xml(&e, &tags)
        ),
    ))
}

/// The eight resource-type exclusions a VPC encryption control tracks, paired
/// with the ModifyVpcEncryptionControl request param that sets each.
const VPC_ENC_EXCLUSIONS: &[(&str, &str)] = &[
    ("internetGateway", "InternetGatewayExclusion"),
    (
        "egressOnlyInternetGateway",
        "EgressOnlyInternetGatewayExclusion",
    ),
    ("natGateway", "NatGatewayExclusion"),
    ("virtualPrivateGateway", "VirtualPrivateGatewayExclusion"),
    ("vpcPeering", "VpcPeeringExclusion"),
    ("lambda", "LambdaExclusion"),
    ("vpcLattice", "VpcLatticeExclusion"),
    ("elasticFileSystem", "ElasticFileSystemExclusion"),
];

fn vpc_encryption_control_xml(c: &VpcEncryptionControl, tags: &[Tag]) -> String {
    let exclusions: String = VPC_ENC_EXCLUSIONS
        .iter()
        .map(|(name, _)| {
            let st = c
                .exclusions
                .get(*name)
                .map(String::as_str)
                .unwrap_or("disabled");
            format!("<{name}><state>{st}</state></{name}>")
        })
        .collect();
    format!(
        "{}{}{}{}<resourceExclusions>{}</resourceExclusions>{}",
        ec2_elem("vpcId", &c.vpc_id),
        ec2_elem("vpcEncryptionControlId", &c.id),
        ec2_elem("mode", &c.mode),
        ec2_elem("state", &c.state),
        exclusions,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_vpc_encryption_control(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let id = gen_id("vpc-enc");
    let c = VpcEncryptionControl {
        id: id.clone(),
        vpc_id,
        mode: req
            .query_params
            .get("Mode")
            .cloned()
            .unwrap_or_else(|| "monitor".to_string()),
        state: "available".to_string(),
        exclusions: std::collections::BTreeMap::new(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "vpc-encryption-control",
        );
        let tg = state.tags_for(&id).to_vec();
        state.vpc_encryption_controls.insert(id.clone(), c.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateVpcEncryptionControl",
        &req.request_id,
        &format!(
            "<vpcEncryptionControl>{}</vpcEncryptionControl>",
            vpc_encryption_control_xml(&c, &tags)
        ),
    ))
}

pub(crate) fn delete_capacity_manager_data_export(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CapacityManagerDataExportId")?;
    Ok(Ec2Service::respond(
        "DeleteCapacityManagerDataExport",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_fpga_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FpgaImageId")?;
    let removed = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let r = state.fpga_images.remove(&id).is_some();
        if r {
            state.tags.remove(&id);
        }
        r
    };
    Ok(Ec2Service::respond(
        "DeleteFpgaImage",
        &req.request_id,
        &fakecloud_aws::ec2query::ec2_return(removed),
    ))
}

pub(crate) fn delete_image_usage_report(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ReportId")?;
    Ok(Ec2Service::respond(
        "DeleteImageUsageReport",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_instance_event_window(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceEventWindowId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let existed = state.instance_event_windows.remove(&id).is_some();
    if existed {
        state.tags.remove(&id);
    }
    let new_state = if existed { "deleting" } else { "deleted" };
    Ok(Ec2Service::respond(
        "DeleteInstanceEventWindow",
        &req.request_id,
        &format!(
            "<instanceEventWindowState>{}{}</instanceEventWindowState>",
            ec2_elem("instanceEventWindowId", &id),
            ec2_elem("state", new_state),
        ),
    ))
}

pub(crate) fn delete_managed_prefix_list(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "PrefixListId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(mut pl) = state.managed_prefix_lists.remove(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidPrefixListID.NotFound",
            &id,
        ));
    };
    pl.state = "delete-complete".to_string();
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteManagedPrefixList",
        &req.request_id,
        &format!(
            "<prefixList>{}</prefixList>",
            managed_prefix_list_xml(&pl, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn delete_public_ipv4_pool(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PoolId")?;
    Ok(Ec2Service::respond(
        "DeletePublicIpv4Pool",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_route_server(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "RouteServerId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(mut r) = state.route_servers.remove(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidRouteServerId.NotFound",
            &id,
        ));
    };
    r.state = "deleting".to_string();
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteRouteServer",
        &req.request_id,
        &format!("<routeServer>{}</routeServer>", route_server_xml(&r, &tags)),
    ))
}

pub(crate) fn delete_route_server_endpoint(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerEndpointId")?;
    Ok(Ec2Service::respond(
        "DeleteRouteServerEndpoint",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_route_server_peer(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerPeerId")?;
    Ok(Ec2Service::respond(
        "DeleteRouteServerPeer",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_secondary_network(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "SecondaryNetworkId")?;
    Ok(Ec2Service::respond(
        "DeleteSecondaryNetwork",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_traffic_mirror_filter(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorFilterId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let existed = state.traffic_mirror_filters.remove(&id).is_some();
    if existed {
        state.tags.remove(&id);
        state
            .traffic_mirror_filter_rules
            .retain(|_, r| r.filter_id != id);
    }
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorFilter",
        &req.request_id,
        &ec2_elem("trafficMirrorFilterId", &id),
    ))
}

pub(crate) fn delete_traffic_mirror_filter_rule(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorFilterRuleId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.traffic_mirror_filter_rules.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorFilterRule",
        &req.request_id,
        &ec2_elem("trafficMirrorFilterRuleId", &id),
    ))
}

pub(crate) fn delete_traffic_mirror_session(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorSessionId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let existed = state.traffic_mirror_sessions.remove(&id).is_some();
    if existed {
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorSession",
        &req.request_id,
        &ec2_elem("trafficMirrorSessionId", &id),
    ))
}

pub(crate) fn delete_traffic_mirror_target(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorTargetId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let existed = state.traffic_mirror_targets.remove(&id).is_some();
    if existed {
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorTarget",
        &req.request_id,
        &ec2_elem("trafficMirrorTargetId", &id),
    ))
}

pub(crate) fn delete_vpc_block_public_access_exclusion(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ExclusionId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(mut e) = state.vpc_bpa_exclusions.remove(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidVpcBlockPublicAccessExclusionId.NotFound",
            &id,
        ));
    };
    e.state = "delete-complete".to_string();
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVpcBlockPublicAccessExclusion",
        &req.request_id,
        &format!(
            "<vpcBlockPublicAccessExclusion>{}</vpcBlockPublicAccessExclusion>",
            vpc_bpa_exclusion_xml(&e, &tags)
        ),
    ))
}

pub(crate) fn delete_vpc_encryption_control(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcEncryptionControlId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(mut c) = state.vpc_encryption_controls.remove(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidVpcEncryptionControlId.NotFound",
            &id,
        ));
    };
    c.state = "deleting".to_string();
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVpcEncryptionControl",
        &req.request_id,
        &format!(
            "<vpcEncryptionControl>{}</vpcEncryptionControl>",
            vpc_encryption_control_xml(&c, &tags)
        ),
    ))
}

pub(crate) fn deprovision_byoip_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Cidr")?;
    Ok(Ec2Service::respond(
        "DeprovisionByoipCidr",
        &req.request_id,
        "",
    ))
}

pub(crate) fn deprovision_public_ipv4_pool_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PoolId")?;
    require(&req.query_params, "Cidr")?;
    Ok(Ec2Service::respond(
        "DeprovisionPublicIpv4PoolCidr",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_aggregate_id_format(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    // Aggregated = true only when every tracked resource uses long ids (and at
    // least one is tracked), matching how AWS reports the account roll-up.
    let aggregated = !state.id_format.is_empty() && state.id_format.values().all(|v| *v);
    let items: Vec<String> = state
        .id_format
        .iter()
        .map(|(r, v)| id_format_item(r, *v))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeAggregateIdFormat",
        &req.request_id,
        &format!(
            "{}{}",
            ec2_elem("useLongIdsAggregated", &aggregated.to_string()),
            ec2_list("statusSet", &items),
        ),
    ))
}

pub(crate) fn describe_aws_network_performance_metric_subscriptions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 0, 100)?;
    Ok(Ec2Service::respond(
        "DescribeAwsNetworkPerformanceMetricSubscriptions",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_bundle_tasks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeBundleTasks",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_byoip_cidrs(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "MaxResults")?;
    validate_max_results(&req.query_params, 1, 100)?;
    Ok(Ec2Service::respond(
        "DescribeByoipCidrs",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_capacity_manager_data_exports(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeCapacityManagerDataExports",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_classic_link_instances(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeClassicLinkInstances",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_conversion_tasks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeConversionTasks",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_declarative_policies_reports(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeDeclarativePoliciesReports",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_elastic_gpus(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 10, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeElasticGpus",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_export_image_tasks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 500)?;
    Ok(Ec2Service::respond(
        "DescribeExportImageTasks",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_export_tasks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeExportTasks",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_fpga_image_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FpgaImageId")?;
    require(&req.query_params, "Attribute")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["description", "name", "loadPermission", "productCodes"],
    )?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let Some(f) = state.fpga_images.get(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidFpgaImageID.NotFound",
            &id,
        ));
    };
    Ok(Ec2Service::respond(
        "DescribeFpgaImageAttribute",
        &req.request_id,
        &fpga_image_attribute_xml(f),
    ))
}

pub(crate) fn describe_fpga_images(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "FpgaImageId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .fpga_images
        .values()
        .filter(|f| wanted.is_empty() || wanted.contains(&f.id))
        .map(|f| fpga_image_xml(f, state.tags_for(&f.id), &owner))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeFpgaImages",
        &req.request_id,
        &ec2_list("fpgaImageSet", &items),
    ))
}

pub(crate) fn describe_host_reservation_offerings(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 500)?;
    Ok(Ec2Service::respond(
        "DescribeHostReservationOfferings",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_host_reservations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeHostReservations",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_iam_instance_profile_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeIamInstanceProfileAssociations",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_id_format(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let filter = req.query_params.get("Resource").filter(|v| !v.is_empty());
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .id_format
        .iter()
        .filter(|(r, _)| filter.is_none_or(|f| *r == f))
        .map(|(r, v)| id_format_item(r, *v))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeIdFormat",
        &req.request_id,
        &ec2_list("statusSet", &items),
    ))
}

pub(crate) fn describe_identity_id_format(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let principal = require(&req.query_params, "PrincipalArn")?;
    let filter = req.query_params.get("Resource").filter(|v| !v.is_empty());
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .identity_id_format
        .get(&principal)
        .into_iter()
        .flatten()
        .filter(|(r, _)| filter.is_none_or(|f| *r == f))
        .map(|(r, v)| id_format_item(r, *v))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeIdentityIdFormat",
        &req.request_id,
        &ec2_list("statusSet", &items),
    ))
}

pub(crate) fn describe_image_references(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, i64::MAX)?;
    Ok(Ec2Service::respond(
        "DescribeImageReferences",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_image_usage_report_entries(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, i64::MAX)?;
    Ok(Ec2Service::respond(
        "DescribeImageUsageReportEntries",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_image_usage_reports(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, i64::MAX)?;
    Ok(Ec2Service::respond(
        "DescribeImageUsageReports",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_import_image_tasks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeImportImageTasks",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_import_snapshot_tasks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeImportSnapshotTasks",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_instance_event_windows(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 20, 500)?;
    let wanted = indexed_list(&req.query_params, "InstanceEventWindowId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .instance_event_windows
        .values()
        .filter(|w| wanted.is_empty() || wanted.contains(&w.id))
        .map(|w| instance_event_window_xml(w, state.tags_for(&w.id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeInstanceEventWindows",
        &req.request_id,
        &ec2_list("instanceEventWindowSet", &items),
    ))
}

pub(crate) fn describe_instance_image_metadata(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeInstanceImageMetadata",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_instance_sql_ha_history_states(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeInstanceSqlHaHistoryStates",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_instance_sql_ha_states(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeInstanceSqlHaStates",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_instance_type_offerings(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "LocationType",
        &[
            "region",
            "availability-zone",
            "availability-zone-id",
            "outpost",
        ],
    )?;
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeInstanceTypeOfferings",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_ipv6_pools(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeIpv6Pools",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_mac_modification_tasks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 500)?;
    Ok(Ec2Service::respond(
        "DescribeMacModificationTasks",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_managed_prefix_lists(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 100)?;
    let wanted = indexed_list(&req.query_params, "PrefixListId");
    let owner = req.account_id.clone();
    let region = region_of(req);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .managed_prefix_lists
        .values()
        .filter(|p| wanted.is_empty() || wanted.contains(&p.prefix_list_id))
        .map(|p| managed_prefix_list_xml(p, state.tags_for(&p.prefix_list_id), &owner, &region))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeManagedPrefixLists",
        &req.request_id,
        &ec2_list("prefixListSet", &items),
    ))
}

pub(crate) fn describe_outpost_lags(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeOutpostLags",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_prefix_lists(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribePrefixLists",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_principal_id_format(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    let resource_filter = indexed_list(&req.query_params, "Resource");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let keep = |r: &str| resource_filter.is_empty() || resource_filter.iter().any(|f| f == r);
    let mut items: Vec<String> = Vec::new();
    // The account root principal carries the account-default id-format settings.
    let root_arn = format!("arn:aws:iam::{}:root", req.account_id);
    let root_statuses: Vec<String> = state
        .id_format
        .iter()
        .filter(|(r, _)| keep(r))
        .map(|(r, v)| id_format_item(r, *v))
        .collect();
    if !root_statuses.is_empty() {
        items.push(format!(
            "{}{}",
            ec2_elem("arn", &root_arn),
            ec2_list("statusSet", &root_statuses),
        ));
    }
    for (principal, fmts) in &state.identity_id_format {
        let statuses: Vec<String> = fmts
            .iter()
            .filter(|(r, _)| keep(r))
            .map(|(r, v)| id_format_item(r, *v))
            .collect();
        if !statuses.is_empty() {
            items.push(format!(
                "{}{}",
                ec2_elem("arn", principal),
                ec2_list("statusSet", &statuses),
            ));
        }
    }
    Ok(Ec2Service::respond(
        "DescribePrincipalIdFormat",
        &req.request_id,
        &ec2_list("principalSet", &items),
    ))
}

pub(crate) fn describe_public_ipv4_pools(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 10)?;
    Ok(Ec2Service::respond(
        "DescribePublicIpv4Pools",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_replace_root_volume_tasks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 50)?;
    Ok(Ec2Service::respond(
        "DescribeReplaceRootVolumeTasks",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_route_server_endpoints(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeRouteServerEndpoints",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_route_server_peers(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeRouteServerPeers",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_route_servers(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "RouteServerId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .route_servers
        .values()
        .filter(|r| wanted.is_empty() || wanted.contains(&r.id))
        .map(|r| route_server_xml(r, state.tags_for(&r.id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeRouteServers",
        &req.request_id,
        &ec2_list("routeServerSet", &items),
    ))
}

pub(crate) fn describe_scheduled_instance_availability(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "FirstSlotStartTimeRange")?;
    validate_max_results(&req.query_params, 5, 300)?;
    Ok(Ec2Service::respond(
        "DescribeScheduledInstanceAvailability",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_scheduled_instances(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeScheduledInstances",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_secondary_interfaces(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeSecondaryInterfaces",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_secondary_networks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeSecondaryNetworks",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_service_link_virtual_interfaces(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeServiceLinkVirtualInterfaces",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_traffic_mirror_filter_rules(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "TrafficMirrorFilterRuleId");
    let filter_id = req
        .query_params
        .get("TrafficMirrorFilterId")
        .filter(|v| !v.is_empty());
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .traffic_mirror_filter_rules
        .values()
        .filter(|r| wanted.is_empty() || wanted.contains(&r.id))
        .filter(|r| filter_id.is_none_or(|f| &r.filter_id == f))
        .map(traffic_mirror_filter_rule_xml)
        .collect();
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorFilterRules",
        &req.request_id,
        &ec2_list("trafficMirrorFilterRuleSet", &items),
    ))
}

pub(crate) fn describe_traffic_mirror_filters(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "TrafficMirrorFilterId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .traffic_mirror_filters
        .values()
        .filter(|f| wanted.is_empty() || wanted.contains(&f.id))
        .map(|f| {
            let rules: Vec<&TrafficMirrorFilterRule> = state
                .traffic_mirror_filter_rules
                .values()
                .filter(|r| r.filter_id == f.id)
                .collect();
            traffic_mirror_filter_xml(f, &rules, state.tags_for(&f.id))
        })
        .collect();
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorFilters",
        &req.request_id,
        &ec2_list("trafficMirrorFilterSet", &items),
    ))
}

pub(crate) fn describe_traffic_mirror_sessions(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "TrafficMirrorSessionId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .traffic_mirror_sessions
        .values()
        .filter(|s| wanted.is_empty() || wanted.contains(&s.id))
        .map(|s| traffic_mirror_session_xml(s, state.tags_for(&s.id), &owner))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorSessions",
        &req.request_id,
        &ec2_list("trafficMirrorSessionSet", &items),
    ))
}

pub(crate) fn describe_traffic_mirror_targets(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "TrafficMirrorTargetId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .traffic_mirror_targets
        .values()
        .filter(|t| wanted.is_empty() || wanted.contains(&t.id))
        .map(|t| traffic_mirror_target_xml(t, state.tags_for(&t.id), &owner))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorTargets",
        &req.request_id,
        &ec2_list("trafficMirrorTargetSet", &items),
    ))
}

pub(crate) fn describe_trunk_interface_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 255)?;
    Ok(Ec2Service::respond(
        "DescribeTrunkInterfaceAssociations",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_vpc_block_public_access_exclusions(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "ExclusionId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .vpc_bpa_exclusions
        .values()
        .filter(|e| wanted.is_empty() || wanted.contains(&e.id))
        .map(|e| vpc_bpa_exclusion_xml(e, state.tags_for(&e.id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeVpcBlockPublicAccessExclusions",
        &req.request_id,
        &ec2_list("vpcBlockPublicAccessExclusionSet", &items),
    ))
}

fn vpc_bpa_options_xml(account: &str, region: &str, mode: &str) -> String {
    let state = if mode == "off" {
        "default-state"
    } else {
        "update-complete"
    };
    format!(
        "<vpcBlockPublicAccessOptions>{}{}{}{}{}</vpcBlockPublicAccessOptions>",
        ec2_elem("awsAccountId", account),
        ec2_elem("awsRegion", region),
        ec2_elem("state", state),
        ec2_elem("internetGatewayBlockMode", mode),
        ec2_elem("exclusionsAllowed", "allowed"),
    )
}

pub(crate) fn describe_vpc_block_public_access_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let region = region_of(req);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mode = state
        .vpc_bpa_internet_gateway_block_mode
        .clone()
        .unwrap_or_else(|| "off".to_string());
    Ok(Ec2Service::respond(
        "DescribeVpcBlockPublicAccessOptions",
        &req.request_id,
        &vpc_bpa_options_xml(&req.account_id, &region, &mode),
    ))
}

pub(crate) fn describe_vpc_classic_link(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeVpcClassicLink",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_vpc_classic_link_dns_support(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_length(&req.query_params, "NextToken", 1, 1024)?;
    validate_max_results(&req.query_params, 5, 255)?;
    Ok(Ec2Service::respond(
        "DescribeVpcClassicLinkDnsSupport",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_vpc_encryption_controls(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "VpcEncryptionControlId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .vpc_encryption_controls
        .values()
        .filter(|c| wanted.is_empty() || wanted.contains(&c.id))
        .map(|c| vpc_encryption_control_xml(c, state.tags_for(&c.id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeVpcEncryptionControls",
        &req.request_id,
        &ec2_list("vpcEncryptionControlSet", &items),
    ))
}

pub(crate) fn detach_classic_link_vpc(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "DetachClassicLinkVpc",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disable_aws_network_performance_metric_subscription(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(&req.query_params, "Metric", &["aggregate-latency"])?;
    validate_enum(&req.query_params, "Statistic", &["p50"])?;
    Ok(Ec2Service::respond(
        "DisableAwsNetworkPerformanceMetricSubscription",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disable_capacity_manager(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DisableCapacityManager",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disable_instance_sql_ha_standby_detections(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DisableInstanceSqlHaStandbyDetections",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disable_route_server_propagation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    require(&req.query_params, "RouteTableId")?;
    Ok(Ec2Service::respond(
        "DisableRouteServerPropagation",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disable_vgw_route_propagation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "GatewayId")?;
    require(&req.query_params, "RouteTableId")?;
    Ok(Ec2Service::respond(
        "DisableVgwRoutePropagation",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disable_vpc_classic_link(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "DisableVpcClassicLink",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disable_vpc_classic_link_dns_support(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DisableVpcClassicLinkDnsSupport",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disassociate_enclave_certificate_iam_role(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CertificateArn")?;
    require(&req.query_params, "RoleArn")?;
    Ok(Ec2Service::respond(
        "DisassociateEnclaveCertificateIamRole",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disassociate_iam_instance_profile(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "AssociationId")?;
    Ok(Ec2Service::respond(
        "DisassociateIamInstanceProfile",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disassociate_instance_event_window(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceEventWindowId")?;
    let instances = indexed_list(&req.query_params, "AssociationTarget.InstanceId");
    let hosts = indexed_list(&req.query_params, "AssociationTarget.DedicatedHostId");
    let tags =
        crate::service_helpers::parse_tag_pairs(&req.query_params, "AssociationTarget.InstanceTag");
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(entry) = state.instance_event_windows.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidInstanceEventWindowId.NotFound",
            &id,
        ));
    };
    entry.assoc_instance_ids.retain(|i| !instances.contains(i));
    entry
        .assoc_dedicated_host_ids
        .retain(|h| !hosts.contains(h));
    let remove_keys: Vec<String> = tags.into_iter().map(|(k, _)| k).collect();
    entry.assoc_tags.retain(|t| !remove_keys.contains(&t.key));
    let w = entry.clone();
    let t = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "DisassociateInstanceEventWindow",
        &req.request_id,
        &format!(
            "<instanceEventWindow>{}</instanceEventWindow>",
            instance_event_window_xml(&w, &t)
        ),
    ))
}

pub(crate) fn disassociate_route_server(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "DisassociateRouteServer",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disassociate_trunk_interface(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "AssociationId")?;
    Ok(Ec2Service::respond(
        "DisassociateTrunkInterface",
        &req.request_id,
        "",
    ))
}

pub(crate) fn enable_aws_network_performance_metric_subscription(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(&req.query_params, "Metric", &["aggregate-latency"])?;
    validate_enum(&req.query_params, "Statistic", &["p50"])?;
    Ok(Ec2Service::respond(
        "EnableAwsNetworkPerformanceMetricSubscription",
        &req.request_id,
        "",
    ))
}

pub(crate) fn enable_capacity_manager(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "EnableCapacityManager",
        &req.request_id,
        "",
    ))
}

pub(crate) fn enable_instance_sql_ha_standby_detections(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "EnableInstanceSqlHaStandbyDetections",
        &req.request_id,
        "",
    ))
}

pub(crate) fn enable_reachability_analyzer_organization_sharing(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "EnableReachabilityAnalyzerOrganizationSharing",
        &req.request_id,
        "",
    ))
}

pub(crate) fn enable_route_server_propagation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    require(&req.query_params, "RouteTableId")?;
    Ok(Ec2Service::respond(
        "EnableRouteServerPropagation",
        &req.request_id,
        "",
    ))
}

pub(crate) fn enable_vgw_route_propagation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "GatewayId")?;
    require(&req.query_params, "RouteTableId")?;
    Ok(Ec2Service::respond(
        "EnableVgwRoutePropagation",
        &req.request_id,
        "",
    ))
}

pub(crate) fn enable_vpc_classic_link(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "EnableVpcClassicLink",
        &req.request_id,
        "",
    ))
}

pub(crate) fn enable_vpc_classic_link_dns_support(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "EnableVpcClassicLinkDnsSupport",
        &req.request_id,
        "",
    ))
}

pub(crate) fn export_image(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "S3ExportLocation")?;
    require(&req.query_params, "DiskImageFormat")?;
    require(&req.query_params, "ImageId")?;
    validate_enum(
        &req.query_params,
        "DiskImageFormat",
        &["VMDK", "RAW", "VHD"],
    )?;
    Ok(Ec2Service::respond("ExportImage", &req.request_id, ""))
}

pub(crate) fn get_associated_enclave_certificate_iam_roles(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CertificateArn")?;
    Ok(Ec2Service::respond(
        "GetAssociatedEnclaveCertificateIamRoles",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_associated_ipv6_pool_cidrs(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PoolId")?;
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "GetAssociatedIpv6PoolCidrs",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_aws_network_performance_data(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "GetAwsNetworkPerformanceData",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_capacity_manager_attributes(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "GetCapacityManagerAttributes",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_capacity_manager_metric_data(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_int_range(&req.query_params, "Period", 3600, i64::MAX)?;
    require(&req.query_params, "StartTime")?;
    require(&req.query_params, "EndTime")?;
    require(&req.query_params, "Period")?;
    Ok(Ec2Service::respond(
        "GetCapacityManagerMetricData",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_capacity_manager_metric_dimensions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "StartTime")?;
    require(&req.query_params, "EndTime")?;
    Ok(Ec2Service::respond(
        "GetCapacityManagerMetricDimensions",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_capacity_manager_monitored_tag_keys(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "GetCapacityManagerMonitoredTagKeys",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_declarative_policies_report_summary(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ReportId")?;
    Ok(Ec2Service::respond(
        "GetDeclarativePoliciesReportSummary",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_default_credit_specification(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let family = require(&req.query_params, "InstanceFamily")?;
    validate_enum(
        &req.query_params,
        "InstanceFamily",
        &["t2", "t3", "t3a", "t4g"],
    )?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    // AWS default for burstable families is `standard` until explicitly set.
    let cpu_credits = state
        .default_credit_specs
        .get(&family)
        .cloned()
        .unwrap_or_else(|| "standard".to_string());
    Ok(Ec2Service::respond(
        "GetDefaultCreditSpecification",
        &req.request_id,
        &credit_spec_xml(&family, &cpu_credits),
    ))
}

pub(crate) fn get_host_reservation_purchase_preview(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "OfferingId")?;
    Ok(Ec2Service::respond(
        "GetHostReservationPurchasePreview",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_image_ancestry(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ImageId")?;
    Ok(Ec2Service::respond("GetImageAncestry", &req.request_id, ""))
}

pub(crate) fn get_instance_tpm_ek_pub(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    require(&req.query_params, "KeyType")?;
    require(&req.query_params, "KeyFormat")?;
    validate_enum(&req.query_params, "KeyType", &["rsa-2048", "ecc-sec-p384"])?;
    validate_enum(&req.query_params, "KeyFormat", &["der", "tpmt"])?;
    Ok(Ec2Service::respond(
        "GetInstanceTpmEkPub",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_instance_uefi_data(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    Ok(Ec2Service::respond(
        "GetInstanceUefiData",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_managed_prefix_list_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PrefixListId")?;
    validate_max_results(&req.query_params, 5, 255)?;
    Ok(Ec2Service::respond(
        "GetManagedPrefixListAssociations",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_managed_prefix_list_entries(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "PrefixListId")?;
    validate_max_results(&req.query_params, 1, 100)?;
    let target_version = req
        .query_params
        .get("TargetVersion")
        .and_then(|v| v.parse::<i64>().ok());
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let Some(pl) = state.managed_prefix_lists.get(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidPrefixListID.NotFound",
            &id,
        ));
    };
    let entries = match target_version {
        Some(v) => pl.version_history.get(&v).unwrap_or(&pl.entries),
        None => &pl.entries,
    };
    let items: Vec<String> = entries
        .iter()
        .map(|e| {
            format!(
                "{}{}",
                ec2_elem("cidr", &e.cidr),
                ec2_elem_opt("description", e.description.as_deref()),
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "GetManagedPrefixListEntries",
        &req.request_id,
        &ec2_list("entrySet", &items),
    ))
}

fn managed_resource_visibility_xml(default_visibility: &str) -> String {
    format!(
        "<visibility>{}</visibility>",
        ec2_elem("defaultVisibility", default_visibility)
    )
}

pub(crate) fn get_managed_resource_visibility(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let visibility = state
        .managed_resource_default_visibility
        .clone()
        .unwrap_or_else(|| "visible".to_string());
    Ok(Ec2Service::respond(
        "GetManagedResourceVisibility",
        &req.request_id,
        &managed_resource_visibility_xml(&visibility),
    ))
}

pub(crate) fn get_route_server_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    Ok(Ec2Service::respond(
        "GetRouteServerAssociations",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_route_server_propagations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    Ok(Ec2Service::respond(
        "GetRouteServerPropagations",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_route_server_routing_database(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "GetRouteServerRoutingDatabase",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_vpc_resources_blocking_encryption_enforcement(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VpcId")?;
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "GetVpcResourcesBlockingEncryptionEnforcement",
        &req.request_id,
        "",
    ))
}

pub(crate) fn import_image(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "BootMode",
        &["legacy-bios", "uefi", "uefi-preferred"],
    )?;
    Ok(Ec2Service::respond("ImportImage", &req.request_id, ""))
}

pub(crate) fn import_instance(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Platform")?;
    validate_enum(&req.query_params, "Platform", &["Windows"])?;
    Ok(Ec2Service::respond("ImportInstance", &req.request_id, ""))
}

pub(crate) fn import_snapshot(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond("ImportSnapshot", &req.request_id, ""))
}

pub(crate) fn import_volume(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "Image")?;
    require_struct(&req.query_params, "Volume")?;
    Ok(Ec2Service::respond("ImportVolume", &req.request_id, ""))
}

pub(crate) fn modify_availability_zone_group(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let group = require(&req.query_params, "GroupName")?;
    let status = require(&req.query_params, "OptInStatus")?;
    validate_enum(
        &req.query_params,
        "OptInStatus",
        &["opted-in", "not-opted-in"],
    )?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.az_group_optin.insert(group, status);
    }
    Ok(Ec2Service::respond(
        "ModifyAvailabilityZoneGroup",
        &req.request_id,
        &fakecloud_aws::ec2query::ec2_return(true),
    ))
}

fn credit_spec_xml(family: &str, cpu_credits: &str) -> String {
    format!(
        "<instanceFamilyCreditSpecification>{}{}</instanceFamilyCreditSpecification>",
        ec2_elem("instanceFamily", family),
        ec2_elem("cpuCredits", cpu_credits),
    )
}

pub(crate) fn modify_default_credit_specification(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let family = require(&req.query_params, "InstanceFamily")?;
    let cpu_credits = require(&req.query_params, "CpuCredits")?;
    validate_enum(
        &req.query_params,
        "InstanceFamily",
        &["t2", "t3", "t3a", "t4g"],
    )?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .default_credit_specs
            .insert(family.clone(), cpu_credits.clone());
    }
    Ok(Ec2Service::respond(
        "ModifyDefaultCreditSpecification",
        &req.request_id,
        &credit_spec_xml(&family, &cpu_credits),
    ))
}

pub(crate) fn modify_fpga_image_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FpgaImageId")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["description", "name", "loadPermission", "productCodes"],
    )?;
    validate_enum(&req.query_params, "OperationType", &["add", "remove"])?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(f) = state.fpga_images.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidFpgaImageID.NotFound",
            &id,
        ));
    };
    if let Some(n) = req.query_params.get("Name") {
        f.name = n.clone();
    }
    if let Some(d) = req.query_params.get("Description") {
        f.description = d.clone();
    }
    // LoadPermission add/remove for users and groups, shape
    // `LoadPermission.{Add,Remove}.N.{UserId,Group}`.
    let add_users = nested_indexed(req, "LoadPermission.Add", "UserId");
    let add_groups = nested_indexed(req, "LoadPermission.Add", "Group");
    let rm_users = nested_indexed(req, "LoadPermission.Remove", "UserId");
    let rm_groups = nested_indexed(req, "LoadPermission.Remove", "Group");
    for u in add_users {
        if !f.load_permission_users.contains(&u) {
            f.load_permission_users.push(u);
        }
    }
    for g in add_groups {
        if !f.load_permission_groups.contains(&g) {
            f.load_permission_groups.push(g);
        }
    }
    f.load_permission_users.retain(|u| !rm_users.contains(u));
    f.load_permission_groups.retain(|g| !rm_groups.contains(g));
    let out = f.clone();
    Ok(Ec2Service::respond(
        "ModifyFpgaImageAttribute",
        &req.request_id,
        &fpga_image_attribute_xml(&out),
    ))
}

/// Render an `<item>` of the `statusSet` (IdFormat) shape.
fn id_format_item(resource: &str, use_long_ids: bool) -> String {
    format!(
        "{}{}",
        ec2_elem("resource", resource),
        ec2_elem("useLongIds", &use_long_ids.to_string()),
    )
}

pub(crate) fn modify_id_format(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let resource = require(&req.query_params, "Resource")?;
    let use_long_ids = require(&req.query_params, "UseLongIds")? == "true";
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.id_format.insert(resource, use_long_ids);
    }
    Ok(Ec2Service::respond("ModifyIdFormat", &req.request_id, ""))
}

pub(crate) fn modify_identity_id_format(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let resource = require(&req.query_params, "Resource")?;
    let use_long_ids = require(&req.query_params, "UseLongIds")? == "true";
    let principal = require(&req.query_params, "PrincipalArn")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .identity_id_format
            .entry(principal)
            .or_default()
            .insert(resource, use_long_ids);
    }
    Ok(Ec2Service::respond(
        "ModifyIdentityIdFormat",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_instance_event_window(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceEventWindowId")?;
    let time_ranges = parse_event_window_time_ranges(req);
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(entry) = state.instance_event_windows.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidInstanceEventWindowId.NotFound",
            &id,
        ));
    };
    if let Some(n) = req.query_params.get("Name") {
        entry.name = Some(n.clone());
    }
    if let Some(c) = req.query_params.get("CronExpression") {
        entry.cron_expression = Some(c.clone());
        entry.time_ranges.clear();
    }
    if !time_ranges.is_empty() {
        entry.time_ranges = time_ranges;
        entry.cron_expression = None;
    }
    let w = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyInstanceEventWindow",
        &req.request_id,
        &format!(
            "<instanceEventWindow>{}</instanceEventWindow>",
            instance_event_window_xml(&w, &tags)
        ),
    ))
}

pub(crate) fn modify_managed_prefix_list(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "PrefixListId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let add = parse_prefix_list_entries(req, "AddEntry");
    let remove: Vec<String> = {
        let mut out = Vec::new();
        let mut i = 1usize;
        while let Some(c) = req
            .query_params
            .get(&format!("RemoveEntry.{i}.Cidr"))
            .filter(|v| !v.is_empty())
        {
            out.push(c.clone());
            i += 1;
        }
        out
    };
    let new_name = req.query_params.get("PrefixListName").cloned();
    let new_max = req
        .query_params
        .get("MaxEntries")
        .and_then(|v| v.parse::<i64>().ok());
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let (pl, tags) = match state.managed_prefix_lists.get_mut(&id) {
        Some(entry) => {
            let entries_changed = !add.is_empty() || !remove.is_empty();
            if let Some(n) = new_name {
                entry.prefix_list_name = n;
            }
            if let Some(m) = new_max {
                entry.max_entries = m;
            }
            if entries_changed {
                entry.entries.retain(|e| !remove.contains(&e.cidr));
                for a in add {
                    if let Some(existing) = entry.entries.iter_mut().find(|e| e.cidr == a.cidr) {
                        existing.description = a.description;
                    } else {
                        entry.entries.push(a);
                    }
                }
                entry.version += 1;
                entry
                    .version_history
                    .insert(entry.version, entry.entries.clone());
            }
            entry.state = "modify-complete".to_string();
            (entry.clone(), state.tags_for(&id).to_vec())
        }
        None => {
            // Unknown id: EC2 has no error shape here for some callers, but the
            // resource genuinely does not exist, so report NotFound rather than
            // inventing one.
            return Err(crate::service_helpers::not_found(
                "InvalidPrefixListID.NotFound",
                &id,
            ));
        }
    };
    Ok(Ec2Service::respond(
        "ModifyManagedPrefixList",
        &req.request_id,
        &format!(
            "<prefixList>{}</prefixList>",
            managed_prefix_list_xml(&pl, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn modify_managed_resource_visibility(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let visibility = require(&req.query_params, "DefaultVisibility")?;
    validate_enum(
        &req.query_params,
        "DefaultVisibility",
        &["hidden", "visible"],
    )?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.managed_resource_default_visibility = Some(visibility.clone());
    }
    Ok(Ec2Service::respond(
        "ModifyManagedResourceVisibility",
        &req.request_id,
        &managed_resource_visibility_xml(&visibility),
    ))
}

pub(crate) fn modify_private_dns_name_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let instance_id = require(&req.query_params, "InstanceId")?;
    validate_enum(
        &req.query_params,
        "PrivateDnsHostnameType",
        &["ip-name", "resource-name"],
    )?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(inst) = state.instances.get_mut(&instance_id) else {
        return Err(crate::service_helpers::instance_not_found(&instance_id));
    };
    if let Some(t) = req
        .query_params
        .get("PrivateDnsHostnameType")
        .filter(|v| !v.is_empty())
    {
        inst.private_dns_hostname_type = Some(t.clone());
    }
    if let Some(v) = req.query_params.get("EnableResourceNameDnsARecord") {
        inst.enable_resource_name_dns_a_record = v == "true";
    }
    if let Some(v) = req.query_params.get("EnableResourceNameDnsAAAARecord") {
        inst.enable_resource_name_dns_aaaa_record = v == "true";
    }
    Ok(Ec2Service::respond(
        "ModifyPrivateDnsNameOptions",
        &req.request_id,
        &fakecloud_aws::ec2query::ec2_return(true),
    ))
}

pub(crate) fn modify_public_ip_dns_name_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let eni_id = require(&req.query_params, "NetworkInterfaceId")?;
    let hostname_type = require(&req.query_params, "HostnameType")?;
    validate_enum(
        &req.query_params,
        "HostnameType",
        &[
            "public-dual-stack-dns-name",
            "public-ipv4-dns-name",
            "public-ipv6-dns-name",
        ],
    )?;
    // No AWS Describe returns this setting, but the modify still targets a real
    // ENI — persist the chosen hostname type and reject an unknown interface.
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(eni) = state.network_interfaces.get_mut(&eni_id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidNetworkInterfaceID.NotFound",
            &eni_id,
        ));
    };
    eni.public_ip_dns_hostname_type = Some(hostname_type);
    Ok(Ec2Service::respond(
        "ModifyPublicIpDnsNameOptions",
        &req.request_id,
        &ec2_elem("successful", "true"),
    ))
}

pub(crate) fn modify_route_server(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "RouteServerId")?;
    validate_enum(
        &req.query_params,
        "PersistRoutes",
        &["enable", "disable", "reset"],
    )?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(entry) = state.route_servers.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidRouteServerId.NotFound",
            &id,
        ));
    };
    if let Some(p) = req.query_params.get("PersistRoutes") {
        entry.persist_routes_state = persist_routes_state(Some(p.as_str()));
    }
    if let Some(d) = req
        .query_params
        .get("PersistRoutesDuration")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.persist_routes_duration = Some(d);
    }
    if let Some(s) = req.query_params.get("SnsNotificationsEnabled") {
        entry.sns_notifications_enabled = s == "true";
    }
    let r = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyRouteServer",
        &req.request_id,
        &format!("<routeServer>{}</routeServer>", route_server_xml(&r, &tags)),
    ))
}

pub(crate) fn modify_traffic_mirror_filter_network_services(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorFilterId")?;
    let add = indexed_list(&req.query_params, "AddNetworkService");
    let remove = indexed_list(&req.query_params, "RemoveNetworkService");
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(entry) = state.traffic_mirror_filters.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidTrafficMirrorFilterId.NotFound",
            &id,
        ));
    };
    entry.network_services.retain(|s| !remove.contains(s));
    for s in add {
        if !entry.network_services.contains(&s) {
            entry.network_services.push(s);
        }
    }
    let f = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    let rules: Vec<&TrafficMirrorFilterRule> = state
        .traffic_mirror_filter_rules
        .values()
        .filter(|r| r.filter_id == id)
        .collect();
    Ok(Ec2Service::respond(
        "ModifyTrafficMirrorFilterNetworkServices",
        &req.request_id,
        &format!(
            "<trafficMirrorFilter>{}</trafficMirrorFilter>",
            traffic_mirror_filter_xml(&f, &rules, &tags)
        ),
    ))
}

pub(crate) fn modify_traffic_mirror_filter_rule(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorFilterRuleId")?;
    validate_enum(
        &req.query_params,
        "TrafficDirection",
        &["ingress", "egress"],
    )?;
    validate_enum(&req.query_params, "RuleAction", &["accept", "reject"])?;
    let remove_fields = indexed_list(&req.query_params, "RemoveField");
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(entry) = state.traffic_mirror_filter_rules.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidTrafficMirrorFilterRuleId.NotFound",
            &id,
        ));
    };
    if let Some(d) = req.query_params.get("TrafficDirection") {
        entry.traffic_direction = d.clone();
    }
    if let Some(n) = req
        .query_params
        .get("RuleNumber")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.rule_number = n;
    }
    if let Some(a) = req.query_params.get("RuleAction") {
        entry.rule_action = a.clone();
    }
    if let Some(p) = req
        .query_params
        .get("Protocol")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.protocol = Some(p);
    }
    if let Some(c) = req.query_params.get("DestinationCidrBlock") {
        entry.destination_cidr_block = Some(c.clone());
    }
    if let Some(c) = req.query_params.get("SourceCidrBlock") {
        entry.source_cidr_block = Some(c.clone());
    }
    if let Some(pr) = parse_port_range(req, "DestinationPortRange") {
        entry.destination_port_range = Some(pr);
    }
    if let Some(pr) = parse_port_range(req, "SourcePortRange") {
        entry.source_port_range = Some(pr);
    }
    if let Some(d) = req.query_params.get("Description") {
        entry.description = Some(d.clone());
    }
    // RemoveField clears optional members back to unset.
    for field in &remove_fields {
        match field.as_str() {
            "destination-port-range" => entry.destination_port_range = None,
            "source-port-range" => entry.source_port_range = None,
            "protocol" => entry.protocol = None,
            "description" => entry.description = None,
            _ => {}
        }
    }
    let r = entry.clone();
    Ok(Ec2Service::respond(
        "ModifyTrafficMirrorFilterRule",
        &req.request_id,
        &format!(
            "<trafficMirrorFilterRule>{}</trafficMirrorFilterRule>",
            traffic_mirror_filter_rule_xml(&r)
        ),
    ))
}

pub(crate) fn modify_traffic_mirror_session(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorSessionId")?;
    let owner = req.account_id.clone();
    let remove_fields = indexed_list(&req.query_params, "RemoveField");
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(entry) = state.traffic_mirror_sessions.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidTrafficMirrorSessionId.NotFound",
            &id,
        ));
    };
    if let Some(t) = req.query_params.get("TrafficMirrorTargetId") {
        entry.target_id = t.clone();
    }
    if let Some(f) = req.query_params.get("TrafficMirrorFilterId") {
        entry.filter_id = f.clone();
    }
    if let Some(n) = req
        .query_params
        .get("SessionNumber")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.session_number = n;
    }
    if let Some(p) = req
        .query_params
        .get("PacketLength")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.packet_length = Some(p);
    }
    if let Some(v) = req
        .query_params
        .get("VirtualNetworkId")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.virtual_network_id = Some(v);
    }
    if let Some(d) = req.query_params.get("Description") {
        entry.description = Some(d.clone());
    }
    for field in &remove_fields {
        match field.as_str() {
            "packet-length" => entry.packet_length = None,
            "virtual-network-id" => entry.virtual_network_id = None,
            "description" => entry.description = None,
            _ => {}
        }
    }
    let s = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyTrafficMirrorSession",
        &req.request_id,
        &format!(
            "<trafficMirrorSession>{}</trafficMirrorSession>",
            traffic_mirror_session_xml(&s, &tags, &owner)
        ),
    ))
}

pub(crate) fn modify_vpc_block_public_access_exclusion(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ExclusionId")?;
    let mode = require(&req.query_params, "InternetGatewayExclusionMode")?;
    validate_enum(
        &req.query_params,
        "InternetGatewayExclusionMode",
        &["allow-bidirectional", "allow-egress"],
    )?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(entry) = state.vpc_bpa_exclusions.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidVpcBlockPublicAccessExclusionId.NotFound",
            &id,
        ));
    };
    entry.internet_gateway_exclusion_mode = mode;
    entry.state = "update-complete".to_string();
    let e = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyVpcBlockPublicAccessExclusion",
        &req.request_id,
        &format!(
            "<vpcBlockPublicAccessExclusion>{}</vpcBlockPublicAccessExclusion>",
            vpc_bpa_exclusion_xml(&e, &tags)
        ),
    ))
}

pub(crate) fn modify_vpc_block_public_access_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let mode = require(&req.query_params, "InternetGatewayBlockMode")?;
    validate_enum(
        &req.query_params,
        "InternetGatewayBlockMode",
        &["off", "block-bidirectional", "block-ingress"],
    )?;
    let region = region_of(req);
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.vpc_bpa_internet_gateway_block_mode = Some(mode.clone());
    }
    Ok(Ec2Service::respond(
        "ModifyVpcBlockPublicAccessOptions",
        &req.request_id,
        &vpc_bpa_options_xml(&req.account_id, &region, &mode),
    ))
}

pub(crate) fn modify_vpc_encryption_control(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcEncryptionControlId")?;
    validate_enum(&req.query_params, "Mode", &["monitor", "enforce"])?;
    validate_enum(
        &req.query_params,
        "InternetGatewayExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(
        &req.query_params,
        "EgressOnlyInternetGatewayExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(
        &req.query_params,
        "NatGatewayExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(
        &req.query_params,
        "VirtualPrivateGatewayExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(
        &req.query_params,
        "VpcPeeringExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(&req.query_params, "LambdaExclusion", &["enable", "disable"])?;
    validate_enum(
        &req.query_params,
        "VpcLatticeExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(
        &req.query_params,
        "ElasticFileSystemExclusion",
        &["enable", "disable"],
    )?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(entry) = state.vpc_encryption_controls.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidVpcEncryptionControlId.NotFound",
            &id,
        ));
    };
    if let Some(m) = req.query_params.get("Mode") {
        entry.mode = m.clone();
    }
    for (name, param) in VPC_ENC_EXCLUSIONS {
        if let Some(v) = req.query_params.get(*param).filter(|v| !v.is_empty()) {
            let st = if v == "enable" { "enabled" } else { "disabled" };
            entry.exclusions.insert((*name).to_string(), st.to_string());
        }
    }
    let c = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyVpcEncryptionControl",
        &req.request_id,
        &format!(
            "<vpcEncryptionControl>{}</vpcEncryptionControl>",
            vpc_encryption_control_xml(&c, &tags)
        ),
    ))
}

pub(crate) fn provision_byoip_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Cidr")?;
    Ok(Ec2Service::respond(
        "ProvisionByoipCidr",
        &req.request_id,
        "",
    ))
}

pub(crate) fn provision_public_ipv4_pool_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamPoolId")?;
    require(&req.query_params, "PoolId")?;
    require(&req.query_params, "NetmaskLength")?;
    Ok(Ec2Service::respond(
        "ProvisionPublicIpv4PoolCidr",
        &req.request_id,
        "",
    ))
}

pub(crate) fn purchase_host_reservation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "OfferingId")?;
    validate_enum(&req.query_params, "CurrencyCode", &["USD"])?;
    Ok(Ec2Service::respond(
        "PurchaseHostReservation",
        &req.request_id,
        "",
    ))
}

pub(crate) fn purchase_scheduled_instances(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "PurchaseScheduledInstances",
        &req.request_id,
        "",
    ))
}

pub(crate) fn replace_iam_instance_profile_association(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "AssociationId")?;
    Ok(Ec2Service::respond(
        "ReplaceIamInstanceProfileAssociation",
        &req.request_id,
        "",
    ))
}

pub(crate) fn reset_fpga_image_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FpgaImageId")?;
    validate_enum(&req.query_params, "Attribute", &["loadPermission"])?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(f) = state.fpga_images.get_mut(&id) {
            f.load_permission_users.clear();
            f.load_permission_groups.clear();
        }
    }
    Ok(Ec2Service::respond(
        "ResetFpgaImageAttribute",
        &req.request_id,
        &fakecloud_aws::ec2query::ec2_return(true),
    ))
}

pub(crate) fn restore_managed_prefix_list_version(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "PrefixListId")?;
    let previous = require(&req.query_params, "PreviousVersion")?
        .parse::<i64>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("PreviousVersion must be an integer")
        })?;
    require(&req.query_params, "CurrentVersion")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let Some(pl) = state.managed_prefix_lists.get_mut(&id) else {
        return Err(crate::service_helpers::not_found(
            "InvalidPrefixListID.NotFound",
            &id,
        ));
    };
    if let Some(restored) = pl.version_history.get(&previous).cloned() {
        pl.entries = restored;
        pl.version += 1;
        pl.version_history.insert(pl.version, pl.entries.clone());
    }
    pl.state = "modify-complete".to_string();
    let out = pl.clone();
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "RestoreManagedPrefixListVersion",
        &req.request_id,
        &format!(
            "<prefixList>{}</prefixList>",
            managed_prefix_list_xml(&out, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn run_scheduled_instances(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "LaunchSpecification")?;
    require(&req.query_params, "ScheduledInstanceId")?;
    Ok(Ec2Service::respond(
        "RunScheduledInstances",
        &req.request_id,
        "",
    ))
}

pub(crate) fn send_diagnostic_interrupt(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    Ok(Ec2Service::respond(
        "SendDiagnosticInterrupt",
        &req.request_id,
        "",
    ))
}

pub(crate) fn start_declarative_policies_report(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "S3Bucket")?;
    require(&req.query_params, "TargetId")?;
    Ok(Ec2Service::respond(
        "StartDeclarativePoliciesReport",
        &req.request_id,
        "",
    ))
}

pub(crate) fn update_capacity_manager_monitored_tag_keys(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "UpdateCapacityManagerMonitoredTagKeys",
        &req.request_id,
        "",
    ))
}

pub(crate) fn update_capacity_manager_organizations_access(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "OrganizationsAccess")?;
    Ok(Ec2Service::respond(
        "UpdateCapacityManagerOrganizationsAccess",
        &req.request_id,
        "",
    ))
}

pub(crate) fn withdraw_byoip_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Cidr")?;
    Ok(Ec2Service::respond(
        "WithdrawByoipCidr",
        &req.request_id,
        "",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(action: &str, query: &[(&str, &str)]) -> AwsRequest {
        AwsRequest {
            service: "ec2".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn id_format_round_trips() {
        let svc = Ec2Service::new();
        modify_id_format(
            &svc,
            &req(
                "ModifyIdFormat",
                &[("Resource", "vpc"), ("UseLongIds", "true")],
            ),
        )
        .unwrap();
        let out = body(describe_id_format(&svc, &req("DescribeIdFormat", &[])).unwrap());
        assert!(out.contains("<resource>vpc</resource>"), "{out}");
        assert!(out.contains("<useLongIds>true</useLongIds>"), "{out}");
        // Filter by resource.
        let filtered = body(
            describe_id_format(&svc, &req("DescribeIdFormat", &[("Resource", "subnet")])).unwrap(),
        );
        assert!(!filtered.contains("vpc"), "{filtered}");
        // Aggregate reports useLongIdsAggregated.
        let agg = body(
            describe_aggregate_id_format(&svc, &req("DescribeAggregateIdFormat", &[])).unwrap(),
        );
        assert!(
            agg.contains("<useLongIdsAggregated>true</useLongIdsAggregated>"),
            "{agg}"
        );
    }

    #[test]
    fn identity_and_principal_id_format_round_trip() {
        let svc = Ec2Service::new();
        let arn = "arn:aws:iam::000000000000:role/r";
        modify_identity_id_format(
            &svc,
            &req(
                "ModifyIdentityIdFormat",
                &[
                    ("Resource", "instance"),
                    ("UseLongIds", "true"),
                    ("PrincipalArn", arn),
                ],
            ),
        )
        .unwrap();
        let out = body(
            describe_identity_id_format(
                &svc,
                &req("DescribeIdentityIdFormat", &[("PrincipalArn", arn)]),
            )
            .unwrap(),
        );
        assert!(out.contains("<resource>instance</resource>"), "{out}");
        let principals = body(
            describe_principal_id_format(&svc, &req("DescribePrincipalIdFormat", &[])).unwrap(),
        );
        assert!(principals.contains(arn), "{principals}");
    }

    #[test]
    fn managed_prefix_list_round_trips_create_modify_describe_entries() {
        let svc = Ec2Service::new();
        let created = body(
            create_managed_prefix_list(
                &svc,
                &req(
                    "CreateManagedPrefixList",
                    &[
                        ("PrefixListName", "pl-test"),
                        ("MaxEntries", "10"),
                        ("AddressFamily", "IPv4"),
                        ("Entry.1.Cidr", "10.0.0.0/24"),
                        ("Entry.1.Description", "first"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(
            created.contains("<prefixListName>pl-test</prefixListName>"),
            "{created}"
        );
        let id = created
            .split("<prefixListId>")
            .nth(1)
            .unwrap()
            .split("</prefixListId>")
            .next()
            .unwrap()
            .to_string();
        // Add a second entry.
        modify_managed_prefix_list(
            &svc,
            &req(
                "ModifyManagedPrefixList",
                &[("PrefixListId", &id), ("AddEntry.1.Cidr", "10.0.1.0/24")],
            ),
        )
        .unwrap();
        let listed = body(
            describe_managed_prefix_lists(&svc, &req("DescribeManagedPrefixLists", &[])).unwrap(),
        );
        assert!(listed.contains(&id), "{listed}");
        assert!(listed.contains("<version>2</version>"), "{listed}");
        let entries = body(
            get_managed_prefix_list_entries(
                &svc,
                &req("GetManagedPrefixListEntries", &[("PrefixListId", &id)]),
            )
            .unwrap(),
        );
        assert!(entries.contains("10.0.0.0/24"), "{entries}");
        assert!(entries.contains("10.0.1.0/24"), "{entries}");
        // Restore to version 1 drops the second entry.
        restore_managed_prefix_list_version(
            &svc,
            &req(
                "RestoreManagedPrefixListVersion",
                &[
                    ("PrefixListId", &id),
                    ("PreviousVersion", "1"),
                    ("CurrentVersion", "2"),
                ],
            ),
        )
        .unwrap();
        let after = body(
            get_managed_prefix_list_entries(
                &svc,
                &req("GetManagedPrefixListEntries", &[("PrefixListId", &id)]),
            )
            .unwrap(),
        );
        assert!(after.contains("10.0.0.0/24"), "{after}");
        assert!(!after.contains("10.0.1.0/24"), "{after}");
    }

    #[test]
    fn instance_event_window_round_trips() {
        let svc = Ec2Service::new();
        let created = body(
            create_instance_event_window(
                &svc,
                &req(
                    "CreateInstanceEventWindow",
                    &[("Name", "win"), ("CronExpression", "* * * * ? *")],
                ),
            )
            .unwrap(),
        );
        let id = created
            .split("<instanceEventWindowId>")
            .nth(1)
            .unwrap()
            .split("</instanceEventWindowId>")
            .next()
            .unwrap()
            .to_string();
        modify_instance_event_window(
            &svc,
            &req(
                "ModifyInstanceEventWindow",
                &[("InstanceEventWindowId", &id), ("Name", "win2")],
            ),
        )
        .unwrap();
        associate_instance_event_window(
            &svc,
            &req(
                "AssociateInstanceEventWindow",
                &[
                    ("InstanceEventWindowId", &id),
                    ("AssociationTarget.InstanceId.1", "i-123"),
                ],
            ),
        )
        .unwrap();
        let listed = body(
            describe_instance_event_windows(&svc, &req("DescribeInstanceEventWindows", &[]))
                .unwrap(),
        );
        assert!(listed.contains("<name>win2</name>"), "{listed}");
        assert!(listed.contains("i-123"), "{listed}");
    }

    #[test]
    fn default_credit_specification_round_trips() {
        let svc = Ec2Service::new();
        modify_default_credit_specification(
            &svc,
            &req(
                "ModifyDefaultCreditSpecification",
                &[("InstanceFamily", "t3"), ("CpuCredits", "unlimited")],
            ),
        )
        .unwrap();
        let out = body(
            get_default_credit_specification(
                &svc,
                &req("GetDefaultCreditSpecification", &[("InstanceFamily", "t3")]),
            )
            .unwrap(),
        );
        assert!(out.contains("<cpuCredits>unlimited</cpuCredits>"), "{out}");
        // Unset family reports the standard default.
        let other = body(
            get_default_credit_specification(
                &svc,
                &req("GetDefaultCreditSpecification", &[("InstanceFamily", "t2")]),
            )
            .unwrap(),
        );
        assert!(
            other.contains("<cpuCredits>standard</cpuCredits>"),
            "{other}"
        );
    }

    #[test]
    fn vpc_block_public_access_options_round_trip() {
        let svc = Ec2Service::new();
        modify_vpc_block_public_access_options(
            &svc,
            &req(
                "ModifyVpcBlockPublicAccessOptions",
                &[("InternetGatewayBlockMode", "block-bidirectional")],
            ),
        )
        .unwrap();
        let out = body(
            describe_vpc_block_public_access_options(
                &svc,
                &req("DescribeVpcBlockPublicAccessOptions", &[]),
            )
            .unwrap(),
        );
        assert!(
            out.contains(
                "<internetGatewayBlockMode>block-bidirectional</internetGatewayBlockMode>"
            ),
            "{out}"
        );
    }

    #[test]
    fn vpc_block_public_access_exclusion_round_trips() {
        let svc = Ec2Service::new();
        let created = body(
            create_vpc_block_public_access_exclusion(
                &svc,
                &req(
                    "CreateVpcBlockPublicAccessExclusion",
                    &[
                        ("InternetGatewayExclusionMode", "allow-egress"),
                        ("VpcId", "vpc-1"),
                    ],
                ),
            )
            .unwrap(),
        );
        let id = created
            .split("<exclusionId>")
            .nth(1)
            .unwrap()
            .split("</exclusionId>")
            .next()
            .unwrap()
            .to_string();
        modify_vpc_block_public_access_exclusion(
            &svc,
            &req(
                "ModifyVpcBlockPublicAccessExclusion",
                &[
                    ("ExclusionId", &id),
                    ("InternetGatewayExclusionMode", "allow-bidirectional"),
                ],
            ),
        )
        .unwrap();
        let listed = body(
            describe_vpc_block_public_access_exclusions(
                &svc,
                &req("DescribeVpcBlockPublicAccessExclusions", &[]),
            )
            .unwrap(),
        );
        assert!(
            listed.contains(
                "<internetGatewayExclusionMode>allow-bidirectional</internetGatewayExclusionMode>"
            ),
            "{listed}"
        );
    }

    #[test]
    fn traffic_mirror_target_filter_rule_session_round_trip() {
        let svc = Ec2Service::new();
        // Target.
        let t = body(
            create_traffic_mirror_target(
                &svc,
                &req(
                    "CreateTrafficMirrorTarget",
                    &[("NetworkInterfaceId", "eni-1")],
                ),
            )
            .unwrap(),
        );
        let tid = t
            .split("<trafficMirrorTargetId>")
            .nth(1)
            .unwrap()
            .split("</trafficMirrorTargetId>")
            .next()
            .unwrap()
            .to_string();
        // Filter.
        let f = body(
            create_traffic_mirror_filter(
                &svc,
                &req("CreateTrafficMirrorFilter", &[("Description", "f1")]),
            )
            .unwrap(),
        );
        let fid = f
            .split("<trafficMirrorFilterId>")
            .nth(1)
            .unwrap()
            .split("</trafficMirrorFilterId>")
            .next()
            .unwrap()
            .to_string();
        // Filter rule.
        create_traffic_mirror_filter_rule(
            &svc,
            &req(
                "CreateTrafficMirrorFilterRule",
                &[
                    ("TrafficMirrorFilterId", &fid),
                    ("TrafficDirection", "ingress"),
                    ("RuleNumber", "100"),
                    ("RuleAction", "accept"),
                    ("DestinationCidrBlock", "0.0.0.0/0"),
                    ("SourceCidrBlock", "10.0.0.0/16"),
                ],
            ),
        )
        .unwrap();
        let filters = body(
            describe_traffic_mirror_filters(&svc, &req("DescribeTrafficMirrorFilters", &[]))
                .unwrap(),
        );
        assert!(filters.contains(&fid), "{filters}");
        assert!(
            filters.contains("<ruleNumber>100</ruleNumber>"),
            "{filters}"
        );
        // ModifyNetworkServices persists.
        modify_traffic_mirror_filter_network_services(
            &svc,
            &req(
                "ModifyTrafficMirrorFilterNetworkServices",
                &[
                    ("TrafficMirrorFilterId", &fid),
                    ("AddNetworkService.1", "amazon-dns"),
                ],
            ),
        )
        .unwrap();
        let filters2 = body(
            describe_traffic_mirror_filters(&svc, &req("DescribeTrafficMirrorFilters", &[]))
                .unwrap(),
        );
        assert!(filters2.contains("amazon-dns"), "{filters2}");
        // Session.
        let s = body(
            create_traffic_mirror_session(
                &svc,
                &req(
                    "CreateTrafficMirrorSession",
                    &[
                        ("NetworkInterfaceId", "eni-2"),
                        ("TrafficMirrorTargetId", &tid),
                        ("TrafficMirrorFilterId", &fid),
                        ("SessionNumber", "1"),
                    ],
                ),
            )
            .unwrap(),
        );
        let sid = s
            .split("<trafficMirrorSessionId>")
            .nth(1)
            .unwrap()
            .split("</trafficMirrorSessionId>")
            .next()
            .unwrap()
            .to_string();
        modify_traffic_mirror_session(
            &svc,
            &req(
                "ModifyTrafficMirrorSession",
                &[("TrafficMirrorSessionId", &sid), ("Description", "sess")],
            ),
        )
        .unwrap();
        let sessions = body(
            describe_traffic_mirror_sessions(&svc, &req("DescribeTrafficMirrorSessions", &[]))
                .unwrap(),
        );
        assert!(
            sessions.contains("<description>sess</description>"),
            "{sessions}"
        );
        assert!(sessions.contains(&tid), "{sessions}");
    }

    #[test]
    fn route_server_round_trips() {
        let svc = Ec2Service::new();
        let created = body(
            create_route_server(
                &svc,
                &req(
                    "CreateRouteServer",
                    &[("AmazonSideAsn", "65000"), ("PersistRoutes", "enable")],
                ),
            )
            .unwrap(),
        );
        let id = created
            .split("<routeServerId>")
            .nth(1)
            .unwrap()
            .split("</routeServerId>")
            .next()
            .unwrap()
            .to_string();
        assert!(
            created.contains("<persistRoutesState>ENABLED</persistRoutesState>"),
            "{created}"
        );
        modify_route_server(
            &svc,
            &req(
                "ModifyRouteServer",
                &[("RouteServerId", &id), ("PersistRoutes", "disable")],
            ),
        )
        .unwrap();
        let listed = body(describe_route_servers(&svc, &req("DescribeRouteServers", &[])).unwrap());
        assert!(
            listed.contains("<persistRoutesState>DISABLED</persistRoutesState>"),
            "{listed}"
        );
    }

    #[test]
    fn vpc_encryption_control_round_trips() {
        let svc = Ec2Service::new();
        let created = body(
            create_vpc_encryption_control(
                &svc,
                &req("CreateVpcEncryptionControl", &[("VpcId", "vpc-1")]),
            )
            .unwrap(),
        );
        let id = created
            .split("<vpcEncryptionControlId>")
            .nth(1)
            .unwrap()
            .split("</vpcEncryptionControlId>")
            .next()
            .unwrap()
            .to_string();
        modify_vpc_encryption_control(
            &svc,
            &req(
                "ModifyVpcEncryptionControl",
                &[
                    ("VpcEncryptionControlId", &id),
                    ("Mode", "enforce"),
                    ("LambdaExclusion", "enable"),
                ],
            ),
        )
        .unwrap();
        let listed = body(
            describe_vpc_encryption_controls(&svc, &req("DescribeVpcEncryptionControls", &[]))
                .unwrap(),
        );
        assert!(listed.contains("<mode>enforce</mode>"), "{listed}");
        assert!(
            listed.contains("<lambda><state>enabled</state></lambda>"),
            "{listed}"
        );
    }

    #[test]
    fn managed_resource_visibility_round_trip() {
        let svc = Ec2Service::new();
        modify_managed_resource_visibility(
            &svc,
            &req(
                "ModifyManagedResourceVisibility",
                &[("DefaultVisibility", "hidden")],
            ),
        )
        .unwrap();
        let out = body(
            get_managed_resource_visibility(&svc, &req("GetManagedResourceVisibility", &[]))
                .unwrap(),
        );
        assert!(
            out.contains("<defaultVisibility>hidden</defaultVisibility>"),
            "{out}"
        );
    }

    #[test]
    fn fpga_image_attribute_round_trips() {
        let svc = Ec2Service::new();
        let created =
            body(create_fpga_image(&svc, &req("CreateFpgaImage", &[("Name", "img")])).unwrap());
        let id = created
            .split("<fpgaImageId>")
            .nth(1)
            .unwrap()
            .split("</fpgaImageId>")
            .next()
            .unwrap()
            .to_string();
        modify_fpga_image_attribute(
            &svc,
            &req(
                "ModifyFpgaImageAttribute",
                &[
                    ("FpgaImageId", &id),
                    ("Description", "desc"),
                    ("LoadPermission.Add.1.UserId", "111122223333"),
                ],
            ),
        )
        .unwrap();
        let attr = body(
            describe_fpga_image_attribute(
                &svc,
                &req(
                    "DescribeFpgaImageAttribute",
                    &[("FpgaImageId", &id), ("Attribute", "loadPermission")],
                ),
            )
            .unwrap(),
        );
        assert!(attr.contains("<description>desc</description>"), "{attr}");
        assert!(attr.contains("111122223333"), "{attr}");
    }

    fn seed_instance(id: &str) -> crate::state::Instance {
        crate::state::Instance {
            instance_id: id.into(),
            image_id: "ami-1".into(),
            instance_type: "t3.micro".into(),
            state_code: 16,
            state_name: "running".into(),
            private_ip: "10.0.0.1".into(),
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
        }
    }

    #[test]
    fn private_dns_name_options_persist_on_instance() {
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("000000000000");
            state
                .instances
                .insert("i-1".to_string(), seed_instance("i-1"));
        }
        modify_private_dns_name_options(
            &svc,
            &req(
                "ModifyPrivateDnsNameOptions",
                &[
                    ("InstanceId", "i-1"),
                    ("PrivateDnsHostnameType", "resource-name"),
                    ("EnableResourceNameDnsARecord", "true"),
                ],
            ),
        )
        .unwrap();
        let accounts = svc.state.read();
        let inst = &accounts.get("000000000000").unwrap().instances["i-1"];
        assert_eq!(
            inst.private_dns_hostname_type.as_deref(),
            Some("resource-name")
        );
        assert!(inst.enable_resource_name_dns_a_record);
    }

    #[test]
    fn modify_unknown_resource_is_not_found() {
        let svc = Ec2Service::new();
        let result = modify_managed_prefix_list(
            &svc,
            &req("ModifyManagedPrefixList", &[("PrefixListId", "pl-missing")]),
        );
        let err = match result {
            Ok(_) => panic!("expected NotFound for unknown prefix list"),
            Err(e) => e,
        };
        assert!(format!("{err:?}").contains("NotFound"), "{err:?}");
    }
}
