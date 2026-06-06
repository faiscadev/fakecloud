//! EC2 service entrypoint: `ec2Query` dispatch over the per-account state.

mod dhcp;
mod eip;
mod eni;
mod instance;
mod meta;
mod routing;
mod sg;
mod subnet;
mod tags;
mod vpc;

use async_trait::async_trait;
use http::StatusCode;
use parking_lot::RwLock;
use std::sync::Arc;

use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::SharedEc2State;

/// Every EC2 action this build implements. The conformance audit cross-checks
/// this list against the handwritten `#[test_action("ec2", …)]` tests, so an
/// action added here without a test fails the build. Grows one resource-family
/// batch at a time toward full 767-op parity.
pub const SUPPORTED_ACTIONS: &[&str] = &[
    // Tagging subsystem (shared by every resource family)
    "CreateTags",
    "DeleteTags",
    "DescribeTags",
    // Region / AZ / account describe primitives
    "DescribeRegions",
    "DescribeAvailabilityZones",
    "DescribeAccountAttributes",
    // VPCs
    "CreateVpc",
    "CreateDefaultVpc",
    "DeleteVpc",
    "DescribeVpcs",
    "ModifyVpcAttribute",
    "DescribeVpcAttribute",
    "ModifyVpcTenancy",
    "AssociateVpcCidrBlock",
    "DisassociateVpcCidrBlock",
    // DHCP options
    "CreateDhcpOptions",
    "DeleteDhcpOptions",
    "DescribeDhcpOptions",
    "AssociateDhcpOptions",
    // Subnets
    "CreateSubnet",
    "CreateDefaultSubnet",
    "CreateSecondarySubnet",
    "DeleteSubnet",
    "DeleteSecondarySubnet",
    "DescribeSubnets",
    "DescribeSecondarySubnets",
    "ModifySubnetAttribute",
    "AssociateSubnetCidrBlock",
    "DisassociateSubnetCidrBlock",
    "CreateSubnetCidrReservation",
    "DeleteSubnetCidrReservation",
    "GetSubnetCidrReservations",
    // Security groups
    "CreateSecurityGroup",
    "DeleteSecurityGroup",
    "DescribeSecurityGroups",
    "AuthorizeSecurityGroupIngress",
    "AuthorizeSecurityGroupEgress",
    "RevokeSecurityGroupIngress",
    "RevokeSecurityGroupEgress",
    "DescribeSecurityGroupRules",
    "ModifySecurityGroupRules",
    "UpdateSecurityGroupRuleDescriptionsIngress",
    "UpdateSecurityGroupRuleDescriptionsEgress",
    "AssociateSecurityGroupVpc",
    "DisassociateSecurityGroupVpc",
    "DescribeSecurityGroupVpcAssociations",
    "GetSecurityGroupsForVpc",
    "DescribeStaleSecurityGroups",
    "DescribeSecurityGroupReferences",
    // Route tables
    "CreateRouteTable",
    "DeleteRouteTable",
    "DescribeRouteTables",
    "CreateRoute",
    "DeleteRoute",
    "ReplaceRoute",
    "AssociateRouteTable",
    "DisassociateRouteTable",
    "ReplaceRouteTableAssociation",
    // Internet gateways
    "CreateInternetGateway",
    "DeleteInternetGateway",
    "DescribeInternetGateways",
    "AttachInternetGateway",
    "DetachInternetGateway",
    // Egress-only internet gateways
    "CreateEgressOnlyInternetGateway",
    "DeleteEgressOnlyInternetGateway",
    "DescribeEgressOnlyInternetGateways",
    // NAT gateways
    "CreateNatGateway",
    "DeleteNatGateway",
    "DescribeNatGateways",
    "AssignPrivateNatGatewayAddress",
    "AssociateNatGatewayAddress",
    "DisassociateNatGatewayAddress",
    "UnassignPrivateNatGatewayAddress",
    // Elastic IPs
    "AllocateAddress",
    "ReleaseAddress",
    "DescribeAddresses",
    "AssociateAddress",
    "DisassociateAddress",
    "DescribeAddressesAttribute",
    "ModifyAddressAttribute",
    "ResetAddressAttribute",
    "MoveAddressToVpc",
    "RestoreAddressToClassic",
    "AcceptAddressTransfer",
    "EnableAddressTransfer",
    "DisableAddressTransfer",
    "DescribeAddressTransfers",
    "DescribeMovingAddresses",
    // Key pairs
    "CreateKeyPair",
    "ImportKeyPair",
    "DeleteKeyPair",
    "DescribeKeyPairs",
    // Placement groups
    "CreatePlacementGroup",
    "DeletePlacementGroup",
    "DescribePlacementGroups",
    "GetGroupsForCapacityReservation",
    // Network interfaces
    "CreateNetworkInterface",
    "DeleteNetworkInterface",
    "DescribeNetworkInterfaces",
    "AttachNetworkInterface",
    "DetachNetworkInterface",
    "ModifyNetworkInterfaceAttribute",
    "ResetNetworkInterfaceAttribute",
    "DescribeNetworkInterfaceAttribute",
    "CreateNetworkInterfacePermission",
    "DeleteNetworkInterfacePermission",
    "DescribeNetworkInterfacePermissions",
    "AssignPrivateIpAddresses",
    "UnassignPrivateIpAddresses",
    "AssignIpv6Addresses",
    "UnassignIpv6Addresses",
    // Instances
    "RunInstances",
    "StartInstances",
    "StopInstances",
    "RebootInstances",
    "TerminateInstances",
    "MonitorInstances",
    "UnmonitorInstances",
    "DescribeInstances",
    "DescribeInstanceStatus",
    "DescribeInstanceTypes",
    "GetInstanceTypesFromInstanceRequirements",
    "DescribeInstanceAttribute",
    "ModifyInstanceAttribute",
    "ResetInstanceAttribute",
    "ModifyInstancePlacement",
    "ModifyInstanceMetadataOptions",
    "ModifyInstanceMaintenanceOptions",
    "ModifyInstanceCpuOptions",
    "ModifyInstanceNetworkPerformanceOptions",
    "ModifyInstanceEventStartTime",
    "DescribeInstanceCreditSpecifications",
    "ModifyInstanceCreditSpecification",
    "GetInstanceMetadataDefaults",
    "ModifyInstanceMetadataDefaults",
    "RegisterInstanceEventNotificationAttributes",
    "DeregisterInstanceEventNotificationAttributes",
    "DescribeInstanceEventNotificationAttributes",
    "ReportInstanceStatus",
    "DescribeInstanceTopology",
];

/// Amazon EC2 service.
pub struct Ec2Service {
    pub(crate) state: SharedEc2State,
}

impl Ec2Service {
    /// Construct a service over a fresh, empty account-partitioned state.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(MultiAccountState::new(
                "000000000000",
                "us-east-1",
                "",
            ))),
        }
    }

    /// Construct a service over a shared state handle (used by the server so
    /// persistence/snapshots can be wired in later batches).
    pub fn with_state(state: SharedEc2State) -> Self {
        Self { state }
    }
}

impl Default for Ec2Service {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AwsService for Ec2Service {
    fn service_name(&self) -> &str {
        "ec2"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match request.action.as_str() {
            "CreateTags" => tags::create_tags(self, &request),
            "DeleteTags" => tags::delete_tags(self, &request),
            "DescribeTags" => tags::describe_tags(self, &request),
            "DescribeRegions" => meta::describe_regions(self, &request),
            "DescribeAvailabilityZones" => meta::describe_availability_zones(self, &request),
            "DescribeAccountAttributes" => meta::describe_account_attributes(self, &request),
            "CreateVpc" => vpc::create_vpc(self, &request),
            "CreateDefaultVpc" => vpc::create_default_vpc(self, &request),
            "DeleteVpc" => vpc::delete_vpc(self, &request),
            "DescribeVpcs" => vpc::describe_vpcs(self, &request),
            "ModifyVpcAttribute" => vpc::modify_vpc_attribute(self, &request),
            "DescribeVpcAttribute" => vpc::describe_vpc_attribute(self, &request),
            "ModifyVpcTenancy" => vpc::modify_vpc_tenancy(self, &request),
            "AssociateVpcCidrBlock" => vpc::associate_vpc_cidr_block(self, &request),
            "DisassociateVpcCidrBlock" => vpc::disassociate_vpc_cidr_block(self, &request),
            "CreateDhcpOptions" => dhcp::create_dhcp_options(self, &request),
            "DeleteDhcpOptions" => dhcp::delete_dhcp_options(self, &request),
            "DescribeDhcpOptions" => dhcp::describe_dhcp_options(self, &request),
            "AssociateDhcpOptions" => dhcp::associate_dhcp_options(self, &request),
            "CreateSubnet" => subnet::create_subnet(self, &request),
            "CreateDefaultSubnet" => subnet::create_default_subnet(self, &request),
            "CreateSecondarySubnet" => subnet::create_secondary_subnet(self, &request),
            "DeleteSubnet" => subnet::delete_subnet(self, &request),
            "DeleteSecondarySubnet" => subnet::delete_secondary_subnet(self, &request),
            "DescribeSubnets" => subnet::describe_subnets(self, &request),
            "DescribeSecondarySubnets" => subnet::describe_secondary_subnets(self, &request),
            "ModifySubnetAttribute" => subnet::modify_subnet_attribute(self, &request),
            "AssociateSubnetCidrBlock" => subnet::associate_subnet_cidr_block(self, &request),
            "DisassociateSubnetCidrBlock" => subnet::disassociate_subnet_cidr_block(self, &request),
            "CreateSubnetCidrReservation" => subnet::create_subnet_cidr_reservation(self, &request),
            "DeleteSubnetCidrReservation" => subnet::delete_subnet_cidr_reservation(self, &request),
            "GetSubnetCidrReservations" => subnet::get_subnet_cidr_reservations(self, &request),
            "CreateSecurityGroup" => sg::create_security_group(self, &request),
            "DeleteSecurityGroup" => sg::delete_security_group(self, &request),
            "DescribeSecurityGroups" => sg::describe_security_groups(self, &request),
            "AuthorizeSecurityGroupIngress" => sg::authorize_security_group_ingress(self, &request),
            "AuthorizeSecurityGroupEgress" => sg::authorize_security_group_egress(self, &request),
            "RevokeSecurityGroupIngress" => sg::revoke_security_group_ingress(self, &request),
            "RevokeSecurityGroupEgress" => sg::revoke_security_group_egress(self, &request),
            "DescribeSecurityGroupRules" => sg::describe_security_group_rules(self, &request),
            "ModifySecurityGroupRules" => sg::modify_security_group_rules(self, &request),
            "UpdateSecurityGroupRuleDescriptionsIngress" => {
                sg::update_rule_descriptions_ingress(self, &request)
            }
            "UpdateSecurityGroupRuleDescriptionsEgress" => {
                sg::update_rule_descriptions_egress(self, &request)
            }
            "AssociateSecurityGroupVpc" => sg::associate_security_group_vpc(self, &request),
            "DisassociateSecurityGroupVpc" => sg::disassociate_security_group_vpc(self, &request),
            "DescribeSecurityGroupVpcAssociations" => {
                sg::describe_security_group_vpc_associations(self, &request)
            }
            "GetSecurityGroupsForVpc" => sg::get_security_groups_for_vpc(self, &request),
            "DescribeStaleSecurityGroups" => sg::describe_stale_security_groups(self, &request),
            "DescribeSecurityGroupReferences" => {
                sg::describe_security_group_references(self, &request)
            }
            "CreateRouteTable" => routing::create_route_table(self, &request),
            "DeleteRouteTable" => routing::delete_route_table(self, &request),
            "DescribeRouteTables" => routing::describe_route_tables(self, &request),
            "CreateRoute" => routing::create_route(self, &request),
            "DeleteRoute" => routing::delete_route(self, &request),
            "ReplaceRoute" => routing::replace_route(self, &request),
            "AssociateRouteTable" => routing::associate_route_table(self, &request),
            "DisassociateRouteTable" => routing::disassociate_route_table(self, &request),
            "ReplaceRouteTableAssociation" => {
                routing::replace_route_table_association(self, &request)
            }
            "CreateInternetGateway" => routing::create_internet_gateway(self, &request),
            "DeleteInternetGateway" => routing::delete_internet_gateway(self, &request),
            "DescribeInternetGateways" => routing::describe_internet_gateways(self, &request),
            "AttachInternetGateway" => routing::attach_internet_gateway(self, &request),
            "DetachInternetGateway" => routing::detach_internet_gateway(self, &request),
            "CreateEgressOnlyInternetGateway" => routing::create_egress_only_igw(self, &request),
            "DeleteEgressOnlyInternetGateway" => routing::delete_egress_only_igw(self, &request),
            "DescribeEgressOnlyInternetGateways" => {
                routing::describe_egress_only_igws(self, &request)
            }
            "CreateNatGateway" => routing::create_nat_gateway(self, &request),
            "DeleteNatGateway" => routing::delete_nat_gateway(self, &request),
            "DescribeNatGateways" => routing::describe_nat_gateways(self, &request),
            "AssignPrivateNatGatewayAddress" => {
                routing::assign_private_nat_gateway_address(self, &request)
            }
            "AssociateNatGatewayAddress" => routing::associate_nat_gateway_address(self, &request),
            "DisassociateNatGatewayAddress" => {
                routing::disassociate_nat_gateway_address(self, &request)
            }
            "UnassignPrivateNatGatewayAddress" => {
                routing::unassign_private_nat_gateway_address(self, &request)
            }
            "AllocateAddress" => eip::allocate_address(self, &request),
            "ReleaseAddress" => eip::release_address(self, &request),
            "DescribeAddresses" => eip::describe_addresses(self, &request),
            "AssociateAddress" => eip::associate_address(self, &request),
            "DisassociateAddress" => eip::disassociate_address(self, &request),
            "DescribeAddressesAttribute" => eip::describe_addresses_attribute(self, &request),
            "ModifyAddressAttribute" => eip::modify_address_attribute(self, &request),
            "ResetAddressAttribute" => eip::reset_address_attribute(self, &request),
            "MoveAddressToVpc" => eip::move_address_to_vpc(self, &request),
            "RestoreAddressToClassic" => eip::restore_address_to_classic(self, &request),
            "AcceptAddressTransfer" => eip::accept_address_transfer(self, &request),
            "EnableAddressTransfer" => eip::enable_address_transfer(self, &request),
            "DisableAddressTransfer" => eip::disable_address_transfer(self, &request),
            "DescribeAddressTransfers" => eip::describe_address_transfers(self, &request),
            "DescribeMovingAddresses" => eip::describe_moving_addresses(self, &request),
            "CreateKeyPair" => eip::create_key_pair(self, &request),
            "ImportKeyPair" => eip::import_key_pair(self, &request),
            "DeleteKeyPair" => eip::delete_key_pair(self, &request),
            "DescribeKeyPairs" => eip::describe_key_pairs(self, &request),
            "CreatePlacementGroup" => eip::create_placement_group(self, &request),
            "DeletePlacementGroup" => eip::delete_placement_group(self, &request),
            "DescribePlacementGroups" => eip::describe_placement_groups(self, &request),
            "GetGroupsForCapacityReservation" => {
                eip::get_groups_for_capacity_reservation(self, &request)
            }
            "CreateNetworkInterface" => eni::create_network_interface(self, &request),
            "DeleteNetworkInterface" => eni::delete_network_interface(self, &request),
            "DescribeNetworkInterfaces" => eni::describe_network_interfaces(self, &request),
            "AttachNetworkInterface" => eni::attach_network_interface(self, &request),
            "DetachNetworkInterface" => eni::detach_network_interface(self, &request),
            "ModifyNetworkInterfaceAttribute" => {
                eni::modify_network_interface_attribute(self, &request)
            }
            "ResetNetworkInterfaceAttribute" => {
                eni::reset_network_interface_attribute(self, &request)
            }
            "DescribeNetworkInterfaceAttribute" => {
                eni::describe_network_interface_attribute(self, &request)
            }
            "CreateNetworkInterfacePermission" => {
                eni::create_network_interface_permission(self, &request)
            }
            "DeleteNetworkInterfacePermission" => {
                eni::delete_network_interface_permission(self, &request)
            }
            "DescribeNetworkInterfacePermissions" => {
                eni::describe_network_interface_permissions(self, &request)
            }
            "AssignPrivateIpAddresses" => eni::assign_private_ip_addresses(self, &request),
            "UnassignPrivateIpAddresses" => eni::unassign_private_ip_addresses(self, &request),
            "AssignIpv6Addresses" => eni::assign_ipv6_addresses(self, &request),
            "UnassignIpv6Addresses" => eni::unassign_ipv6_addresses(self, &request),
            "RunInstances" => instance::run_instances(self, &request),
            "StartInstances" => instance::start_instances(self, &request),
            "StopInstances" => instance::stop_instances(self, &request),
            "RebootInstances" => instance::reboot_instances(self, &request),
            "TerminateInstances" => instance::terminate_instances(self, &request),
            "MonitorInstances" => instance::monitor_instances(self, &request),
            "UnmonitorInstances" => instance::unmonitor_instances(self, &request),
            "DescribeInstances" => instance::describe_instances(self, &request),
            "DescribeInstanceStatus" => instance::describe_instance_status(self, &request),
            "DescribeInstanceTypes" => instance::describe_instance_types(self, &request),
            "GetInstanceTypesFromInstanceRequirements" => {
                instance::get_instance_types_from_requirements(self, &request)
            }
            "DescribeInstanceAttribute" => instance::describe_instance_attribute(self, &request),
            "ModifyInstanceAttribute" => instance::modify_instance_attribute(self, &request),
            "ResetInstanceAttribute" => instance::reset_instance_attribute(self, &request),
            "ModifyInstancePlacement" => instance::modify_instance_placement(self, &request),
            "ModifyInstanceMetadataOptions" => {
                instance::modify_instance_metadata_options(self, &request)
            }
            "ModifyInstanceMaintenanceOptions" => {
                instance::modify_instance_maintenance_options(self, &request)
            }
            "ModifyInstanceCpuOptions" => instance::modify_instance_cpu_options(self, &request),
            "ModifyInstanceNetworkPerformanceOptions" => {
                instance::modify_instance_network_performance_options(self, &request)
            }
            "ModifyInstanceEventStartTime" => {
                instance::modify_instance_event_start_time(self, &request)
            }
            "DescribeInstanceCreditSpecifications" => {
                instance::describe_instance_credit_specifications(self, &request)
            }
            "ModifyInstanceCreditSpecification" => {
                instance::modify_instance_credit_specification(self, &request)
            }
            "GetInstanceMetadataDefaults" => {
                instance::get_instance_metadata_defaults(self, &request)
            }
            "ModifyInstanceMetadataDefaults" => {
                instance::modify_instance_metadata_defaults(self, &request)
            }
            "RegisterInstanceEventNotificationAttributes" => {
                instance::register_event_notification_attributes(self, &request)
            }
            "DeregisterInstanceEventNotificationAttributes" => {
                instance::deregister_event_notification_attributes(self, &request)
            }
            "DescribeInstanceEventNotificationAttributes" => {
                instance::describe_event_notification_attributes(self, &request)
            }
            "ReportInstanceStatus" => instance::report_instance_status(self, &request),
            "DescribeInstanceTopology" => instance::describe_instance_topology(self, &request),
            other => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidAction",
                format!("The action {other} is not valid for this web service."),
            )),
        }
    }
}

impl Ec2Service {
    /// Render an `ec2Query` response for `action` with `body` as the inner XML.
    pub(crate) fn respond(action: &str, request_id: &str, body: &str) -> AwsResponse {
        AwsResponse::xml(
            StatusCode::OK,
            fakecloud_aws::ec2query::ec2_response(action, request_id, body),
        )
    }
}
