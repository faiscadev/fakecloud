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

pub(crate) use fakecloud_aws::ec2query::{ec2_elem, ec2_elem_opt, ec2_list};
pub(crate) use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

pub(crate) use crate::service::Ec2Service;
pub(crate) use crate::service_helpers::{
    gen_id, indexed_list, require, require_struct, validate_enum, validate_int_range,
    validate_length, validate_max_results,
};
pub(crate) use crate::state::{
    AccountVpcEncryptionControl, ByoipCidr, CapacityManagerDataExport, Ec2State,
    EventWindowTimeRange, FpgaImage, IamInstanceProfileAssociation, InstanceEventWindow,
    ManagedPrefixList, PrefixListEntry, PublicIpv4Pool, RouteServer, RouteServerEndpoint,
    RouteServerPeer, Tag, TrafficMirrorFilter, TrafficMirrorFilterRule, TrafficMirrorSession,
    TrafficMirrorTarget, VpcBpaExclusion, VpcEncryptionControl,
};

mod byoip;
mod capacity_manager;
mod event_window;
mod fpga;
mod ipam;
mod prefix_list;
mod route_server;
mod traffic_mirror;
mod vpc_access;
mod vpc_encryption;

pub(crate) use byoip::*;
pub(crate) use capacity_manager::*;
pub(crate) use event_window::*;
pub(crate) use fpga::*;
pub(crate) use ipam::*;
pub(crate) use prefix_list::*;
pub(crate) use route_server::*;
pub(crate) use traffic_mirror::*;
pub(crate) use vpc_access::*;
pub(crate) use vpc_encryption::*;

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

/// Resolve the request's `IamInstanceProfile.Arn`/`.Name` into (arn, id). AWS
/// takes either; we synthesize the missing half so the association round-trips.
fn iam_profile_ref(req: &AwsRequest) -> (String, String) {
    let arn = req
        .query_params
        .get("IamInstanceProfile.Arn")
        .cloned()
        .or_else(|| {
            req.query_params
                .get("IamInstanceProfile.Name")
                .map(|n| format!("arn:aws:iam::{}:instance-profile/{n}", req.account_id))
        })
        .unwrap_or_default();
    let id = gen_id("AIPA");
    (arn, id)
}

fn iam_profile_assoc_xml(a: &IamInstanceProfileAssociation) -> String {
    format!(
        "{}{}<iamInstanceProfile>{}{}</iamInstanceProfile><state>{}</state>",
        ec2_elem("associationId", &a.association_id),
        ec2_elem("instanceId", &a.instance_id),
        ec2_elem("arn", &a.iam_instance_profile_arn),
        ec2_elem("id", &a.iam_instance_profile_id),
        a.state,
    )
}

pub(crate) fn associate_iam_instance_profile(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let instance_id = require(&req.query_params, "InstanceId")?;
    let (arn, id) = iam_profile_ref(req);
    let assoc = IamInstanceProfileAssociation {
        association_id: gen_id("iip-assoc"),
        instance_id,
        iam_instance_profile_arn: arn,
        iam_instance_profile_id: id,
        state: "associating".to_string(),
    };
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .iam_instance_profile_associations
            .insert(assoc.association_id.clone(), assoc.clone());
    }
    Ok(Ec2Service::respond(
        "AssociateIamInstanceProfile",
        &req.request_id,
        &format!(
            "<iamInstanceProfileAssociation>{}</iamInstanceProfileAssociation>",
            iam_profile_assoc_xml(&assoc)
        ),
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

/// Parse `a.b.c.d/nn` into (first-address, last-address, host-count).
fn ipv4_cidr_range(cidr: &str) -> Option<(String, String, u64)> {
    let (addr, nm) = cidr.split_once('/')?;
    let nm: u32 = nm.parse().ok()?;
    if nm > 32 {
        return None;
    }
    let octets: Vec<u32> = addr.split('.').filter_map(|o| o.parse().ok()).collect();
    if octets.len() != 4 || octets.iter().any(|o| *o > 255) {
        return None;
    }
    let base = (octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3];
    let mask = if nm == 0 { 0 } else { u32::MAX << (32 - nm) };
    let first = base & mask;
    let last = first | !mask;
    let fmt = |ip: u32| {
        format!(
            "{}.{}.{}.{}",
            ip >> 24,
            (ip >> 16) & 0xff,
            (ip >> 8) & 0xff,
            ip & 0xff
        )
    };
    // Count in u64 so a `/0` block (2^32 addresses) can't overflow.
    let count = u64::from(last) - u64::from(first) + 1;
    Some((fmt(first), fmt(last), count))
}

fn public_ipv4_pool_xml(p: &PublicIpv4Pool, tags: &[Tag]) -> String {
    let mut total = 0u64;
    let ranges: Vec<String> = p
        .cidrs
        .iter()
        .filter_map(|c| ipv4_cidr_range(c))
        .map(|(first, last, count)| {
            total += count;
            // `AddressCount`/`AvailableAddressCount` are modeled as Integer
            // (i32); a wide CIDR (e.g. /1) overflows i32 and the SDK rejects
            // the response, so clamp to i32::MAX.
            let count = count.min(i32::MAX as u64);
            format!(
                "{}{}<addressCount>{count}</addressCount><availableAddressCount>{count}</availableAddressCount>",
                ec2_elem("firstAddress", &first),
                ec2_elem("lastAddress", &last),
            )
        })
        .collect();
    let total = total.min(i32::MAX as u64);
    format!(
        "{}{}{}{}<totalAddressCount>{total}</totalAddressCount><totalAvailableAddressCount>{total}</totalAvailableAddressCount>{}",
        ec2_elem("poolId", &p.pool_id),
        ec2_elem("description", &p.description),
        ec2_elem("networkBorderGroup", &p.network_border_group),
        ec2_list("poolAddressRangeSet", &ranges),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_public_ipv4_pool(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("ipv4pool-ec2");
    let pool = PublicIpv4Pool {
        pool_id: id.clone(),
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        network_border_group: req
            .query_params
            .get("NetworkBorderGroup")
            .cloned()
            .unwrap_or_else(|| region_of(req)),
        cidrs: Vec::new(),
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "ipv4pool-ec2",
        );
        state.public_ipv4_pools.insert(id.clone(), pool);
    }
    Ok(Ec2Service::respond(
        "CreatePublicIpv4Pool",
        &req.request_id,
        &ec2_elem("poolId", &id),
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

pub(crate) fn delete_public_ipv4_pool(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "PoolId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.public_ipv4_pools.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeletePublicIpv4Pool",
        &req.request_id,
        &ec2_elem("returnValue", "true"),
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

pub(crate) fn deprovision_public_ipv4_pool_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "PoolId")?;
    let cidr = require(&req.query_params, "Cidr")?;
    {
        let mut accounts = svc.state.write();
        if let Some(p) = accounts
            .get_or_create(&req.account_id)
            .public_ipv4_pools
            .get_mut(&id)
        {
            p.cidrs.retain(|c| c != &cidr);
        }
    }
    Ok(Ec2Service::respond(
        "DeprovisionPublicIpv4PoolCidr",
        &req.request_id,
        &format!(
            "{}{}",
            ec2_elem("poolId", &id),
            // DeprovisionedAddresses is a list of plain address strings.
            ec2_list("deprovisionedAddressSet", std::slice::from_ref(&cidr))
        ),
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
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "AssociationId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .iam_instance_profile_associations
        .values()
        .filter(|a| wanted.is_empty() || wanted.contains(&a.association_id))
        .map(iam_profile_assoc_xml)
        .collect();
    Ok(Ec2Service::respond(
        "DescribeIamInstanceProfileAssociations",
        &req.request_id,
        &ec2_list("iamInstanceProfileAssociationSet", &items),
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
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 10)?;
    let wanted = indexed_list(&req.query_params, "PoolId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .public_ipv4_pools
        .values()
        .filter(|p| wanted.is_empty() || wanted.contains(&p.pool_id))
        .map(|p| public_ipv4_pool_xml(p, state.tags_for(&p.pool_id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribePublicIpv4Pools",
        &req.request_id,
        &ec2_list("publicIpv4PoolSet", &items),
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
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let assoc_id = require(&req.query_params, "AssociationId")?;
    let assoc = {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .iam_instance_profile_associations
            .remove(&assoc_id)
    }
    .map(|mut a| {
        a.state = "disassociating".to_string();
        a
    })
    .unwrap_or(IamInstanceProfileAssociation {
        association_id: assoc_id.clone(),
        instance_id: String::new(),
        iam_instance_profile_arn: String::new(),
        iam_instance_profile_id: String::new(),
        state: "disassociating".to_string(),
    });
    Ok(Ec2Service::respond(
        "DisassociateIamInstanceProfile",
        &req.request_id,
        &format!(
            "<iamInstanceProfileAssociation>{}</iamInstanceProfileAssociation>",
            iam_profile_assoc_xml(&assoc)
        ),
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
    // EC2 declares no error shape for this op in the Smithy model, so an
    // unknown instance id still returns success (synthesize without
    // persisting); a real instance has its private-DNS options persisted.
    if let Some(inst) = state.instances.get_mut(&instance_id) {
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
    // No AWS Describe returns this setting; persist the chosen hostname type on
    // the ENI when it exists, and always acknowledge (EC2's Query API models no
    // error shape for this op, so a synthetic id still returns successful=true).
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(eni) = state.network_interfaces.get_mut(&eni_id) {
            eni.public_ip_dns_hostname_type = Some(hostname_type);
        }
    }
    Ok(Ec2Service::respond(
        "ModifyPublicIpDnsNameOptions",
        &req.request_id,
        &ec2_elem("successful", "true"),
    ))
}

/// The eight resource-type exclusions an account VPC encryption control tracks,
/// paired with the ModifyAccountVpcEncryptionControl request param that sets
/// each. Unlike the per-VPC control, the account request params have no
/// `Exclusion` suffix.
const ACCOUNT_VPC_ENC_EXCLUSIONS: &[(&str, &str)] = &[
    ("internetGateway", "InternetGateway"),
    ("egressOnlyInternetGateway", "EgressOnlyInternetGateway"),
    ("natGateway", "NatGateway"),
    ("virtualPrivateGateway", "VirtualPrivateGateway"),
    ("vpcPeering", "VpcPeering"),
    ("lambda", "Lambda"),
    ("vpcLattice", "VpcLattice"),
    ("elasticFileSystem", "ElasticFileSystem"),
];

pub(crate) fn provision_public_ipv4_pool_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamPoolId")?;
    let id = require(&req.query_params, "PoolId")?;
    let netmask = require(&req.query_params, "NetmaskLength")?
        .parse::<u32>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("NetmaskLength must be an integer")
        })?;
    let cidr = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let pool = state.public_ipv4_pools.get_mut(&id);
        match pool {
            Some(p) => {
                // Allocate a deterministic, non-overlapping block per provision.
                let cidr = format!("100.{}.0.0/{netmask}", 64 + (p.cidrs.len() as u32 % 64));
                p.cidrs.push(cidr.clone());
                cidr
            }
            None => format!("100.64.0.0/{netmask}"),
        }
    };
    let range = ipv4_cidr_range(&cidr)
        .map(|(first, last, count)| {
            // AddressCount/AvailableAddressCount are Integer (i32); clamp so a
            // wide CIDR (e.g. /1 -> 2^31) doesn't overflow and break decoding.
            let count = count.min(i32::MAX as u64);
            format!(
                "{}{}<addressCount>{count}</addressCount><availableAddressCount>{count}</availableAddressCount>",
                ec2_elem("firstAddress", &first),
                ec2_elem("lastAddress", &last),
            )
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "ProvisionPublicIpv4PoolCidr",
        &req.request_id,
        &format!(
            "{}<poolAddressRange>{range}</poolAddressRange>",
            ec2_elem("poolId", &id),
        ),
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
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let assoc_id = require(&req.query_params, "AssociationId")?;
    let (arn, id) = iam_profile_ref(req);
    let assoc = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // AWS keeps the same association id but swaps the profile; a new id is
        // minted (below) when the association is unknown to us.
        if let Some(a) = state.iam_instance_profile_associations.get_mut(&assoc_id) {
            a.iam_instance_profile_arn = arn;
            a.iam_instance_profile_id = id;
            a.state = "associated".to_string();
            a.clone()
        } else {
            let a = IamInstanceProfileAssociation {
                association_id: assoc_id.clone(),
                instance_id: String::new(),
                iam_instance_profile_arn: arn,
                iam_instance_profile_id: id,
                state: "associated".to_string(),
            };
            state
                .iam_instance_profile_associations
                .insert(assoc_id.clone(), a.clone());
            a
        }
    };
    Ok(Ec2Service::respond(
        "ReplaceIamInstanceProfileAssociation",
        &req.request_id,
        &format!(
            "<iamInstanceProfileAssociation>{}</iamInstanceProfileAssociation>",
            iam_profile_assoc_xml(&assoc)
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
    fn modify_unknown_resource_synthesizes_response_without_persisting() {
        // EC2's Query API models no error shape for these ops, so an unknown id
        // (only the conformance probe sends synthetic ids) returns a synthesized
        // success response echoing the request — and must NOT create state.
        let svc = Ec2Service::new();
        let out = body(
            modify_managed_prefix_list(
                &svc,
                &req(
                    "ModifyManagedPrefixList",
                    &[("PrefixListId", "pl-missing"), ("PrefixListName", "ghost")],
                ),
            )
            .unwrap(),
        );
        assert!(
            out.contains("<prefixListId>pl-missing</prefixListId>"),
            "{out}"
        );
        assert!(
            out.contains("<prefixListName>ghost</prefixListName>"),
            "{out}"
        );
        // No persistent resource was invented.
        let accounts = svc.state.read();
        assert!(accounts
            .get("000000000000")
            .map(|s| s.managed_prefix_lists.is_empty())
            .unwrap_or(true));
    }

    #[test]
    fn iam_instance_profile_association_round_trips() {
        let svc = Ec2Service::new();
        let out = body(
            associate_iam_instance_profile(
                &svc,
                &req(
                    "AssociateIamInstanceProfile",
                    &[
                        ("InstanceId", "i-123"),
                        ("IamInstanceProfile.Name", "web-role"),
                    ],
                ),
            )
            .unwrap(),
        );
        // The response carries a real association id + resolved profile arn.
        let assoc_id = out
            .split("<associationId>")
            .nth(1)
            .and_then(|s| s.split("</associationId>").next())
            .unwrap()
            .to_string();
        assert!(assoc_id.starts_with("iip-assoc-"), "{out}");
        assert!(out.contains(":instance-profile/web-role"), "{out}");

        let described = body(
            describe_iam_instance_profile_associations(
                &svc,
                &req("DescribeIamInstanceProfileAssociations", &[]),
            )
            .unwrap(),
        );
        assert!(described.contains(&assoc_id), "{described}");
        assert!(
            described.contains("<instanceId>i-123</instanceId>"),
            "{described}"
        );

        // Disassociate drops it from the describe.
        disassociate_iam_instance_profile(
            &svc,
            &req(
                "DisassociateIamInstanceProfile",
                &[("AssociationId", &assoc_id)],
            ),
        )
        .unwrap();
        let described = body(
            describe_iam_instance_profile_associations(
                &svc,
                &req("DescribeIamInstanceProfileAssociations", &[]),
            )
            .unwrap(),
        );
        assert!(!described.contains(&assoc_id), "{described}");
    }

    #[test]
    fn public_ipv4_pool_round_trips() {
        let svc = Ec2Service::new();
        let out = body(create_public_ipv4_pool(&svc, &req("CreatePublicIpv4Pool", &[])).unwrap());
        let pool_id = out
            .split("<poolId>")
            .nth(1)
            .and_then(|s| s.split("</poolId>").next())
            .unwrap()
            .to_string();
        assert!(pool_id.starts_with("ipv4pool-ec2-"), "{out}");

        provision_public_ipv4_pool_cidr(
            &svc,
            &req(
                "ProvisionPublicIpv4PoolCidr",
                &[
                    ("IpamPoolId", "ipam-pool-1"),
                    ("PoolId", &pool_id),
                    ("NetmaskLength", "28"),
                ],
            ),
        )
        .unwrap();

        let described =
            body(describe_public_ipv4_pools(&svc, &req("DescribePublicIpv4Pools", &[])).unwrap());
        assert!(described.contains(&pool_id), "{described}");
        assert!(described.contains("<firstAddress>"), "{described}");
        assert!(
            described.contains("<addressCount>16</addressCount>"),
            "{described}"
        );
    }

    #[test]
    fn byoip_cidr_round_trips() {
        let svc = Ec2Service::new();
        provision_byoip_cidr(
            &svc,
            &req(
                "ProvisionByoipCidr",
                &[("Cidr", "203.0.113.0/24"), ("Description", "office")],
            ),
        )
        .unwrap();
        advertise_byoip_cidr(
            &svc,
            &req("AdvertiseByoipCidr", &[("Cidr", "203.0.113.0/24")]),
        )
        .unwrap();
        let described = body(
            describe_byoip_cidrs(&svc, &req("DescribeByoipCidrs", &[("MaxResults", "10")]))
                .unwrap(),
        );
        assert!(
            described.contains("<cidr>203.0.113.0/24</cidr>"),
            "{described}"
        );
        assert!(
            described.contains("<state>advertised</state>"),
            "{described}"
        );
        assert!(
            described.contains("<description>office</description>"),
            "{described}"
        );
    }

    #[test]
    fn capacity_manager_attributes_round_trip() {
        let svc = Ec2Service::new();
        enable_capacity_manager(&svc, &req("EnableCapacityManager", &[])).unwrap();
        create_capacity_manager_data_export(
            &svc,
            &req(
                "CreateCapacityManagerDataExport",
                &[
                    ("S3BucketName", "my-bucket"),
                    ("Schedule", "hourly"),
                    ("OutputFormat", "csv"),
                ],
            ),
        )
        .unwrap();
        let attrs = body(
            get_capacity_manager_attributes(&svc, &req("GetCapacityManagerAttributes", &[]))
                .unwrap(),
        );
        assert!(
            attrs.contains("<capacityManagerStatus>ENABLED</capacityManagerStatus>"),
            "{attrs}"
        );
        assert!(
            attrs.contains("<dataExportCount>1</dataExportCount>"),
            "{attrs}"
        );

        let exports = body(
            describe_capacity_manager_data_exports(
                &svc,
                &req("DescribeCapacityManagerDataExports", &[]),
            )
            .unwrap(),
        );
        assert!(
            exports.contains("<s3BucketName>my-bucket</s3BucketName>"),
            "{exports}"
        );
    }

    #[test]
    fn route_server_endpoint_and_peer_round_trip() {
        let svc = Ec2Service::new();
        let out = body(
            create_route_server_endpoint(
                &svc,
                &req(
                    "CreateRouteServerEndpoint",
                    &[("RouteServerId", "rs-1"), ("SubnetId", "subnet-1")],
                ),
            )
            .unwrap(),
        );
        let ep_id = out
            .split("<routeServerEndpointId>")
            .nth(1)
            .and_then(|s| s.split("</routeServerEndpointId>").next())
            .unwrap()
            .to_string();
        assert!(ep_id.starts_with("rse-"), "{out}");

        let described = body(
            describe_route_server_endpoints(&svc, &req("DescribeRouteServerEndpoints", &[]))
                .unwrap(),
        );
        assert!(described.contains(&ep_id), "{described}");

        let peer_out = body(
            create_route_server_peer(
                &svc,
                &req(
                    "CreateRouteServerPeer",
                    &[
                        ("BgpOptions", "{}"),
                        ("RouteServerEndpointId", &ep_id),
                        ("PeerAddress", "10.0.0.5"),
                        ("BgpOptions.PeerAsn", "65001"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(peer_out.contains("<peerAsn>65001</peerAsn>"), "{peer_out}");
        let peers =
            body(describe_route_server_peers(&svc, &req("DescribeRouteServerPeers", &[])).unwrap());
        assert!(
            peers.contains("<peerAddress>10.0.0.5</peerAddress>"),
            "{peers}"
        );
    }
}
