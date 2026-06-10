//! Long-tail remainder sweep: every remaining EC2 operation. Each handler
//! validates wire-observable inputs (required scalars, enums, MaxResults) and
//! returns a minimal response; EC2 output members are all optional so an empty
//! body deserializes cleanly.
#![allow(clippy::too_many_lines)]

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    require, require_struct, validate_enum, validate_int_range, validate_length,
    validate_max_results,
};

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

pub(crate) fn describe_ipam_pool_allocations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1000, 100000)?;
    Ok(Ec2Service::respond(
        "DescribeIpamPoolAllocations",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_ipam_pool_allocation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamPoolAllocationId")?;
    Ok(Ec2Service::respond(
        "ModifyIpamPoolAllocation",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceEventWindowId")?;
    Ok(Ec2Service::respond(
        "AssociateInstanceEventWindow",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "SourceFpgaImageId")?;
    require(&req.query_params, "SourceRegion")?;
    Ok(Ec2Service::respond("CopyFpgaImage", &req.request_id, ""))
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

pub(crate) fn create_fpga_image(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond("CreateFpgaImage", &req.request_id, ""))
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

pub(crate) fn create_instance_event_window(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "CreateInstanceEventWindow",
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

pub(crate) fn create_managed_prefix_list(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PrefixListName")?;
    require(&req.query_params, "MaxEntries")?;
    require(&req.query_params, "AddressFamily")?;
    Ok(Ec2Service::respond(
        "CreateManagedPrefixList",
        &req.request_id,
        "",
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

pub(crate) fn create_route_server(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "AmazonSideAsn")?;
    validate_enum(
        &req.query_params,
        "PersistRoutes",
        &["enable", "disable", "reset"],
    )?;
    Ok(Ec2Service::respond(
        "CreateRouteServer",
        &req.request_id,
        "",
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

pub(crate) fn create_traffic_mirror_filter(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorFilter",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_traffic_mirror_filter_rule(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TrafficMirrorFilterId")?;
    require(&req.query_params, "TrafficDirection")?;
    require(&req.query_params, "RuleNumber")?;
    require(&req.query_params, "RuleAction")?;
    require(&req.query_params, "DestinationCidrBlock")?;
    require(&req.query_params, "SourceCidrBlock")?;
    validate_enum(
        &req.query_params,
        "TrafficDirection",
        &["ingress", "egress"],
    )?;
    validate_enum(&req.query_params, "RuleAction", &["accept", "reject"])?;
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorFilterRule",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_traffic_mirror_session(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "NetworkInterfaceId")?;
    require(&req.query_params, "TrafficMirrorTargetId")?;
    require(&req.query_params, "TrafficMirrorFilterId")?;
    require(&req.query_params, "SessionNumber")?;
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorSession",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_traffic_mirror_target(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorTarget",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_vpc_block_public_access_exclusion(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InternetGatewayExclusionMode")?;
    validate_enum(
        &req.query_params,
        "InternetGatewayExclusionMode",
        &["allow-bidirectional", "allow-egress"],
    )?;
    Ok(Ec2Service::respond(
        "CreateVpcBlockPublicAccessExclusion",
        &req.request_id,
        "",
    ))
}

pub(crate) fn create_vpc_encryption_control(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "CreateVpcEncryptionControl",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "FpgaImageId")?;
    Ok(Ec2Service::respond("DeleteFpgaImage", &req.request_id, ""))
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceEventWindowId")?;
    Ok(Ec2Service::respond(
        "DeleteInstanceEventWindow",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_managed_prefix_list(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PrefixListId")?;
    Ok(Ec2Service::respond(
        "DeleteManagedPrefixList",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    Ok(Ec2Service::respond(
        "DeleteRouteServer",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TrafficMirrorFilterId")?;
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorFilter",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_traffic_mirror_filter_rule(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TrafficMirrorFilterRuleId")?;
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorFilterRule",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_traffic_mirror_session(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TrafficMirrorSessionId")?;
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorSession",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_traffic_mirror_target(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TrafficMirrorTargetId")?;
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorTarget",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_vpc_block_public_access_exclusion(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ExclusionId")?;
    Ok(Ec2Service::respond(
        "DeleteVpcBlockPublicAccessExclusion",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_vpc_encryption_control(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VpcEncryptionControlId")?;
    Ok(Ec2Service::respond(
        "DeleteVpcEncryptionControl",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeAggregateIdFormat",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "FpgaImageId")?;
    require(&req.query_params, "Attribute")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["description", "name", "loadPermission", "productCodes"],
    )?;
    Ok(Ec2Service::respond(
        "DescribeFpgaImageAttribute",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_fpga_images(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeFpgaImages",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond("DescribeIdFormat", &req.request_id, ""))
}

pub(crate) fn describe_identity_id_format(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PrincipalArn")?;
    Ok(Ec2Service::respond(
        "DescribeIdentityIdFormat",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 20, 500)?;
    Ok(Ec2Service::respond(
        "DescribeInstanceEventWindows",
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

pub(crate) fn describe_managed_prefix_lists(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 100)?;
    Ok(Ec2Service::respond(
        "DescribeManagedPrefixLists",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribePrincipalIdFormat",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeRouteServers",
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

pub(crate) fn describe_traffic_mirror_filter_rules(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorFilterRules",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_traffic_mirror_filters(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorFilters",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_traffic_mirror_sessions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorSessions",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_traffic_mirror_targets(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorTargets",
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

pub(crate) fn describe_vpc_block_public_access_exclusions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeVpcBlockPublicAccessExclusions",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_vpc_block_public_access_options(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeVpcBlockPublicAccessOptions",
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

pub(crate) fn describe_vpc_encryption_controls(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeVpcEncryptionControls",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceEventWindowId")?;
    Ok(Ec2Service::respond(
        "DisassociateInstanceEventWindow",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceFamily")?;
    validate_enum(
        &req.query_params,
        "InstanceFamily",
        &["t2", "t3", "t3a", "t4g"],
    )?;
    Ok(Ec2Service::respond(
        "GetDefaultCreditSpecification",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PrefixListId")?;
    validate_max_results(&req.query_params, 1, 100)?;
    Ok(Ec2Service::respond(
        "GetManagedPrefixListEntries",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_managed_resource_visibility(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "GetManagedResourceVisibility",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "GroupName")?;
    require(&req.query_params, "OptInStatus")?;
    validate_enum(
        &req.query_params,
        "OptInStatus",
        &["opted-in", "not-opted-in"],
    )?;
    Ok(Ec2Service::respond(
        "ModifyAvailabilityZoneGroup",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_default_credit_specification(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceFamily")?;
    require(&req.query_params, "CpuCredits")?;
    validate_enum(
        &req.query_params,
        "InstanceFamily",
        &["t2", "t3", "t3a", "t4g"],
    )?;
    Ok(Ec2Service::respond(
        "ModifyDefaultCreditSpecification",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_fpga_image_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "FpgaImageId")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["description", "name", "loadPermission", "productCodes"],
    )?;
    validate_enum(&req.query_params, "OperationType", &["add", "remove"])?;
    Ok(Ec2Service::respond(
        "ModifyFpgaImageAttribute",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_id_format(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Resource")?;
    require(&req.query_params, "UseLongIds")?;
    Ok(Ec2Service::respond("ModifyIdFormat", &req.request_id, ""))
}

pub(crate) fn modify_identity_id_format(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Resource")?;
    require(&req.query_params, "UseLongIds")?;
    require(&req.query_params, "PrincipalArn")?;
    Ok(Ec2Service::respond(
        "ModifyIdentityIdFormat",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_instance_event_window(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceEventWindowId")?;
    Ok(Ec2Service::respond(
        "ModifyInstanceEventWindow",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_managed_prefix_list(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PrefixListId")?;
    Ok(Ec2Service::respond(
        "ModifyManagedPrefixList",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_managed_resource_visibility(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "DefaultVisibility")?;
    validate_enum(
        &req.query_params,
        "DefaultVisibility",
        &["hidden", "visible"],
    )?;
    Ok(Ec2Service::respond(
        "ModifyManagedResourceVisibility",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_private_dns_name_options(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    validate_enum(
        &req.query_params,
        "PrivateDnsHostnameType",
        &["ip-name", "resource-name"],
    )?;
    Ok(Ec2Service::respond(
        "ModifyPrivateDnsNameOptions",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_public_ip_dns_name_options(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "NetworkInterfaceId")?;
    require(&req.query_params, "HostnameType")?;
    validate_enum(
        &req.query_params,
        "HostnameType",
        &[
            "public-dual-stack-dns-name",
            "public-ipv4-dns-name",
            "public-ipv6-dns-name",
        ],
    )?;
    Ok(Ec2Service::respond(
        "ModifyPublicIpDnsNameOptions",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_route_server(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "RouteServerId")?;
    validate_enum(
        &req.query_params,
        "PersistRoutes",
        &["enable", "disable", "reset"],
    )?;
    Ok(Ec2Service::respond(
        "ModifyRouteServer",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_traffic_mirror_filter_network_services(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TrafficMirrorFilterId")?;
    Ok(Ec2Service::respond(
        "ModifyTrafficMirrorFilterNetworkServices",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_traffic_mirror_filter_rule(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TrafficMirrorFilterRuleId")?;
    validate_enum(
        &req.query_params,
        "TrafficDirection",
        &["ingress", "egress"],
    )?;
    validate_enum(&req.query_params, "RuleAction", &["accept", "reject"])?;
    Ok(Ec2Service::respond(
        "ModifyTrafficMirrorFilterRule",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_traffic_mirror_session(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TrafficMirrorSessionId")?;
    Ok(Ec2Service::respond(
        "ModifyTrafficMirrorSession",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_vpc_block_public_access_exclusion(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ExclusionId")?;
    require(&req.query_params, "InternetGatewayExclusionMode")?;
    validate_enum(
        &req.query_params,
        "InternetGatewayExclusionMode",
        &["allow-bidirectional", "allow-egress"],
    )?;
    Ok(Ec2Service::respond(
        "ModifyVpcBlockPublicAccessExclusion",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_vpc_block_public_access_options(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InternetGatewayBlockMode")?;
    validate_enum(
        &req.query_params,
        "InternetGatewayBlockMode",
        &["off", "block-bidirectional", "block-ingress"],
    )?;
    Ok(Ec2Service::respond(
        "ModifyVpcBlockPublicAccessOptions",
        &req.request_id,
        "",
    ))
}

pub(crate) fn modify_vpc_encryption_control(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VpcEncryptionControlId")?;
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
    Ok(Ec2Service::respond(
        "ModifyVpcEncryptionControl",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "FpgaImageId")?;
    validate_enum(&req.query_params, "Attribute", &["loadPermission"])?;
    Ok(Ec2Service::respond(
        "ResetFpgaImageAttribute",
        &req.request_id,
        "",
    ))
}

pub(crate) fn restore_managed_prefix_list_version(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PrefixListId")?;
    require(&req.query_params, "PreviousVersion")?;
    require(&req.query_params, "CurrentVersion")?;
    Ok(Ec2Service::respond(
        "RestoreManagedPrefixListVersion",
        &req.request_id,
        "",
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
