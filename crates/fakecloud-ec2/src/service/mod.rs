//! EC2 service entrypoint: `ec2Query` dispatch over the per-account state.

mod capacity;
mod cvpn;
mod dhcp;
mod eip;
mod endpoint;
mod eni;
mod fleet;
mod image;
mod instance;
mod ipam;
mod ipam_discovery;
mod meta;
mod nacl;
mod reserved;
mod routing;
mod sg;
mod snapshot;
mod subnet;
mod tags;
mod tgw;
mod tgw_mcast;
mod tgw_peering;
mod volume;
mod vpc;
mod vpn;

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
    // EBS volumes
    "CreateVolume",
    "DeleteVolume",
    "DescribeVolumes",
    "AttachVolume",
    "DetachVolume",
    "ModifyVolume",
    "DescribeVolumesModifications",
    "DescribeVolumeStatus",
    "DescribeVolumeAttribute",
    "ModifyVolumeAttribute",
    "EnableVolumeIO",
    "ListVolumesInRecycleBin",
    "RestoreVolumeFromRecycleBin",
    // EBS encryption defaults
    "GetEbsEncryptionByDefault",
    "EnableEbsEncryptionByDefault",
    "DisableEbsEncryptionByDefault",
    "GetEbsDefaultKmsKeyId",
    "ModifyEbsDefaultKmsKeyId",
    "ResetEbsDefaultKmsKeyId",
    // EBS snapshots
    "CreateSnapshot",
    "CreateSnapshots",
    "DeleteSnapshot",
    "DescribeSnapshots",
    "CopySnapshot",
    "DescribeSnapshotAttribute",
    "ModifySnapshotAttribute",
    "ResetSnapshotAttribute",
    "ModifySnapshotTier",
    "DescribeSnapshotTierStatus",
    "RestoreSnapshotTier",
    "ListSnapshotsInRecycleBin",
    "RestoreSnapshotFromRecycleBin",
    "LockSnapshot",
    "UnlockSnapshot",
    "DescribeLockedSnapshots",
    "GetSnapshotBlockPublicAccessState",
    "EnableSnapshotBlockPublicAccess",
    "DisableSnapshotBlockPublicAccess",
    "EnableFastSnapshotRestores",
    "DisableFastSnapshotRestores",
    "DescribeFastSnapshotRestores",
    // AMIs (images)
    "CreateImage",
    "RegisterImage",
    "DeregisterImage",
    "DescribeImages",
    "CopyImage",
    "DescribeImageAttribute",
    "ModifyImageAttribute",
    "ResetImageAttribute",
    "EnableImage",
    "DisableImage",
    "EnableImageDeprecation",
    "DisableImageDeprecation",
    "EnableImageDeregistrationProtection",
    "DisableImageDeregistrationProtection",
    "CancelImageLaunchPermission",
    "RestoreImageFromRecycleBin",
    "ListImagesInRecycleBin",
    "EnableImageBlockPublicAccess",
    "DisableImageBlockPublicAccess",
    "GetImageBlockPublicAccessState",
    "EnableAllowedImagesSettings",
    "DisableAllowedImagesSettings",
    "GetAllowedImagesSettings",
    "ReplaceImageCriteriaInAllowedImagesSettings",
    "CreateStoreImageTask",
    "DescribeStoreImageTasks",
    "CreateRestoreImageTask",
    "DescribeFastLaunchImages",
    // Network ACLs
    "CreateNetworkAcl",
    "DeleteNetworkAcl",
    "DescribeNetworkAcls",
    "CreateNetworkAclEntry",
    "DeleteNetworkAclEntry",
    "ReplaceNetworkAclEntry",
    "ReplaceNetworkAclAssociation",
    // VPC peering
    "CreateVpcPeeringConnection",
    "DeleteVpcPeeringConnection",
    "DescribeVpcPeeringConnections",
    "AcceptVpcPeeringConnection",
    "RejectVpcPeeringConnection",
    "ModifyVpcPeeringConnectionOptions",
    // VPC endpoints
    "CreateVpcEndpoint",
    "DeleteVpcEndpoints",
    "DescribeVpcEndpoints",
    "ModifyVpcEndpoint",
    "DescribeVpcEndpointServices",
    "DescribeVpcEndpointConnections",
    "AcceptVpcEndpointConnections",
    "RejectVpcEndpointConnections",
    "CreateVpcEndpointServiceConfiguration",
    "DeleteVpcEndpointServiceConfigurations",
    "DescribeVpcEndpointServiceConfigurations",
    "ModifyVpcEndpointServiceConfiguration",
    "DescribeVpcEndpointServicePermissions",
    "ModifyVpcEndpointServicePermissions",
    "ModifyVpcEndpointServicePayerResponsibility",
    "StartVpcEndpointServicePrivateDnsVerification",
    "CreateVpcEndpointConnectionNotification",
    "DeleteVpcEndpointConnectionNotifications",
    "DescribeVpcEndpointConnectionNotifications",
    "ModifyVpcEndpointConnectionNotification",
    "DescribeVpcEndpointAssociations",
    // Flow logs
    "CreateFlowLogs",
    "DeleteFlowLogs",
    "DescribeFlowLogs",
    "GetFlowLogsIntegrationTemplate",
    // Launch templates
    "CreateLaunchTemplate",
    "CreateLaunchTemplateVersion",
    "DeleteLaunchTemplate",
    "DeleteLaunchTemplateVersions",
    "DescribeLaunchTemplates",
    "DescribeLaunchTemplateVersions",
    "GetLaunchTemplateData",
    "ModifyLaunchTemplate",
    // Spot instances + fleet
    "RequestSpotInstances",
    "DescribeSpotInstanceRequests",
    "CancelSpotInstanceRequests",
    "RequestSpotFleet",
    "DescribeSpotFleetRequests",
    "CancelSpotFleetRequests",
    "ModifySpotFleetRequest",
    "DescribeSpotFleetInstances",
    "DescribeSpotFleetRequestHistory",
    "DescribeSpotPriceHistory",
    "GetSpotPlacementScores",
    "CreateSpotDatafeedSubscription",
    "DeleteSpotDatafeedSubscription",
    "DescribeSpotDatafeedSubscription",
    // EC2 fleets
    "CreateFleet",
    "DeleteFleets",
    "DescribeFleets",
    "ModifyFleet",
    "DescribeFleetHistory",
    "DescribeFleetInstances",
    // Capacity reservations
    "CreateCapacityReservation",
    "CancelCapacityReservation",
    "DescribeCapacityReservations",
    "ModifyCapacityReservation",
    "GetCapacityReservationUsage",
    "CreateCapacityReservationFleet",
    "CancelCapacityReservationFleets",
    "DescribeCapacityReservationFleets",
    "ModifyCapacityReservationFleet",
    "ModifyInstanceCapacityReservationAttributes",
    "CreateCapacityReservationBySplitting",
    "MoveCapacityReservationInstances",
    "DescribeCapacityReservationBillingRequests",
    "AssociateCapacityReservationBillingOwner",
    "DisassociateCapacityReservationBillingOwner",
    "AcceptCapacityReservationBillingOwnership",
    "RejectCapacityReservationBillingOwnership",
    "DescribeCapacityBlockOfferings",
    "DescribeCapacityBlocks",
    "PurchaseCapacityBlock",
    "DescribeCapacityBlockStatus",
    "DescribeCapacityBlockExtensionHistory",
    "DescribeCapacityBlockExtensionOfferings",
    "PurchaseCapacityBlockExtension",
    "DescribeCapacityReservationTopology",
    "CreateInterruptibleCapacityReservationAllocation",
    "UpdateInterruptibleCapacityReservationAllocation",
    // Reserved instances
    "DescribeReservedInstances",
    "DescribeReservedInstancesOfferings",
    "PurchaseReservedInstancesOffering",
    "DescribeReservedInstancesListings",
    "CreateReservedInstancesListing",
    "CancelReservedInstancesListing",
    "DescribeReservedInstancesModifications",
    "ModifyReservedInstances",
    "GetReservedInstancesExchangeQuote",
    "AcceptReservedInstancesExchangeQuote",
    "DeleteQueuedReservedInstances",
    // Dedicated hosts
    "AllocateHosts",
    "DescribeHosts",
    "ModifyHosts",
    "ReleaseHosts",
    "DescribeMacHosts",
    // Transit gateway core
    "CreateTransitGateway",
    "DeleteTransitGateway",
    "DescribeTransitGateways",
    "ModifyTransitGateway",
    "CreateTransitGatewayVpcAttachment",
    "DeleteTransitGatewayVpcAttachment",
    "DescribeTransitGatewayVpcAttachments",
    "ModifyTransitGatewayVpcAttachment",
    "AcceptTransitGatewayVpcAttachment",
    "RejectTransitGatewayVpcAttachment",
    "DescribeTransitGatewayAttachments",
    "CreateTransitGatewayRouteTable",
    "DeleteTransitGatewayRouteTable",
    "DescribeTransitGatewayRouteTables",
    "AssociateTransitGatewayRouteTable",
    "DisassociateTransitGatewayRouteTable",
    "EnableTransitGatewayRouteTablePropagation",
    "DisableTransitGatewayRouteTablePropagation",
    "CreateTransitGatewayRoute",
    "DeleteTransitGatewayRoute",
    "ReplaceTransitGatewayRoute",
    "SearchTransitGatewayRoutes",
    "ExportTransitGatewayRoutes",
    "GetTransitGatewayRouteTableAssociations",
    "GetTransitGatewayRouteTablePropagations",
    "GetTransitGatewayAttachmentPropagations",
    "CreateTransitGatewayPrefixListReference",
    "DeleteTransitGatewayPrefixListReference",
    "ModifyTransitGatewayPrefixListReference",
    "GetTransitGatewayPrefixListReferences",
    // Transit gateway peering / connect / policy / announcements
    "CreateTransitGatewayPeeringAttachment",
    "DeleteTransitGatewayPeeringAttachment",
    "DescribeTransitGatewayPeeringAttachments",
    "AcceptTransitGatewayPeeringAttachment",
    "RejectTransitGatewayPeeringAttachment",
    "CreateTransitGatewayConnect",
    "DeleteTransitGatewayConnect",
    "DescribeTransitGatewayConnects",
    "CreateTransitGatewayConnectPeer",
    "DeleteTransitGatewayConnectPeer",
    "DescribeTransitGatewayConnectPeers",
    "CreateTransitGatewayPolicyTable",
    "DeleteTransitGatewayPolicyTable",
    "DescribeTransitGatewayPolicyTables",
    "AssociateTransitGatewayPolicyTable",
    "DisassociateTransitGatewayPolicyTable",
    "GetTransitGatewayPolicyTableAssociations",
    "GetTransitGatewayPolicyTableEntries",
    "CreateTransitGatewayRouteTableAnnouncement",
    "DeleteTransitGatewayRouteTableAnnouncement",
    "DescribeTransitGatewayRouteTableAnnouncements",
    // Transit gateway multicast / metering / client-vpn-attach
    "CreateTransitGatewayMulticastDomain",
    "DeleteTransitGatewayMulticastDomain",
    "DescribeTransitGatewayMulticastDomains",
    "AssociateTransitGatewayMulticastDomain",
    "DisassociateTransitGatewayMulticastDomain",
    "AcceptTransitGatewayMulticastDomainAssociations",
    "RejectTransitGatewayMulticastDomainAssociations",
    "GetTransitGatewayMulticastDomainAssociations",
    "RegisterTransitGatewayMulticastGroupMembers",
    "RegisterTransitGatewayMulticastGroupSources",
    "DeregisterTransitGatewayMulticastGroupMembers",
    "DeregisterTransitGatewayMulticastGroupSources",
    "SearchTransitGatewayMulticastGroups",
    "CreateTransitGatewayMeteringPolicy",
    "DeleteTransitGatewayMeteringPolicy",
    "DescribeTransitGatewayMeteringPolicies",
    "ModifyTransitGatewayMeteringPolicy",
    "CreateTransitGatewayMeteringPolicyEntry",
    "DeleteTransitGatewayMeteringPolicyEntry",
    "GetTransitGatewayMeteringPolicyEntries",
    "AcceptTransitGatewayClientVpnAttachment",
    "DeleteTransitGatewayClientVpnAttachment",
    "RejectTransitGatewayClientVpnAttachment",
    // Site-to-Site VPN
    "CreateCustomerGateway",
    "DeleteCustomerGateway",
    "DescribeCustomerGateways",
    "CreateVpnGateway",
    "DeleteVpnGateway",
    "DescribeVpnGateways",
    "AttachVpnGateway",
    "DetachVpnGateway",
    "CreateVpnConnection",
    "DeleteVpnConnection",
    "DescribeVpnConnections",
    "ModifyVpnConnection",
    "ModifyVpnConnectionOptions",
    "CreateVpnConnectionRoute",
    "DeleteVpnConnectionRoute",
    "ModifyVpnTunnelOptions",
    "ModifyVpnTunnelCertificate",
    "ReplaceVpnTunnel",
    "GetActiveVpnTunnelStatus",
    "GetVpnTunnelReplacementStatus",
    "GetVpnConnectionDeviceTypes",
    "GetVpnConnectionDeviceSampleConfiguration",
    "CreateVpnConcentrator",
    "DeleteVpnConcentrator",
    "DescribeVpnConcentrators",
    // Client VPN
    "CreateClientVpnEndpoint",
    "DeleteClientVpnEndpoint",
    "DescribeClientVpnEndpoints",
    "ModifyClientVpnEndpoint",
    "CreateClientVpnRoute",
    "DeleteClientVpnRoute",
    "DescribeClientVpnRoutes",
    "AuthorizeClientVpnIngress",
    "RevokeClientVpnIngress",
    "DescribeClientVpnAuthorizationRules",
    "AssociateClientVpnTargetNetwork",
    "DisassociateClientVpnTargetNetwork",
    "DescribeClientVpnTargetNetworks",
    "ApplySecurityGroupsToClientVpnTargetNetwork",
    "DescribeClientVpnConnections",
    "TerminateClientVpnConnections",
    "ExportClientVpnClientCertificateRevocationList",
    "ExportClientVpnClientConfiguration",
    "ImportClientVpnClientCertificateRevocationList",
    // IPAM core
    "CreateIpam",
    "DeleteIpam",
    "DescribeIpams",
    "ModifyIpam",
    "CreateIpamScope",
    "DeleteIpamScope",
    "DescribeIpamScopes",
    "ModifyIpamScope",
    "CreateIpamPool",
    "DeleteIpamPool",
    "DescribeIpamPools",
    "ModifyIpamPool",
    "ProvisionIpamPoolCidr",
    "DeprovisionIpamPoolCidr",
    "GetIpamPoolCidrs",
    "AllocateIpamPoolCidr",
    "ReleaseIpamPoolAllocation",
    "GetIpamPoolAllocations",
    "GetIpamResourceCidrs",
    "ModifyIpamResourceCidr",
    "GetIpamAddressHistory",
    "EnableIpamOrganizationAdminAccount",
    "DisableIpamOrganizationAdminAccount",
    // IPAM resource discovery / BYOASN / BYOIP / external tokens
    "CreateIpamResourceDiscovery",
    "DeleteIpamResourceDiscovery",
    "DescribeIpamResourceDiscoveries",
    "ModifyIpamResourceDiscovery",
    "AssociateIpamResourceDiscovery",
    "DisassociateIpamResourceDiscovery",
    "DescribeIpamResourceDiscoveryAssociations",
    "GetIpamDiscoveredAccounts",
    "GetIpamDiscoveredPublicAddresses",
    "GetIpamDiscoveredResourceCidrs",
    "AssociateIpamByoasn",
    "DisassociateIpamByoasn",
    "ProvisionIpamByoasn",
    "DeprovisionIpamByoasn",
    "DescribeIpamByoasn",
    "MoveByoipCidrToIpam",
    "CreateIpamExternalResourceVerificationToken",
    "DeleteIpamExternalResourceVerificationToken",
    "DescribeIpamExternalResourceVerificationTokens",
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
            "CreateVolume" => volume::create_volume(self, &request),
            "DeleteVolume" => volume::delete_volume(self, &request),
            "DescribeVolumes" => volume::describe_volumes(self, &request),
            "AttachVolume" => volume::attach_volume(self, &request),
            "DetachVolume" => volume::detach_volume(self, &request),
            "ModifyVolume" => volume::modify_volume(self, &request),
            "DescribeVolumesModifications" => {
                volume::describe_volumes_modifications(self, &request)
            }
            "DescribeVolumeStatus" => volume::describe_volume_status(self, &request),
            "DescribeVolumeAttribute" => volume::describe_volume_attribute(self, &request),
            "ModifyVolumeAttribute" => volume::modify_volume_attribute(self, &request),
            "EnableVolumeIO" => volume::enable_volume_io(self, &request),
            "ListVolumesInRecycleBin" => volume::list_volumes_in_recycle_bin(self, &request),
            "RestoreVolumeFromRecycleBin" => {
                volume::restore_volume_from_recycle_bin(self, &request)
            }
            "GetEbsEncryptionByDefault" => volume::get_ebs_encryption_by_default(self, &request),
            "EnableEbsEncryptionByDefault" => {
                volume::enable_ebs_encryption_by_default(self, &request)
            }
            "DisableEbsEncryptionByDefault" => {
                volume::disable_ebs_encryption_by_default(self, &request)
            }
            "GetEbsDefaultKmsKeyId" => volume::get_ebs_default_kms_key_id(self, &request),
            "ModifyEbsDefaultKmsKeyId" => volume::modify_ebs_default_kms_key_id(self, &request),
            "ResetEbsDefaultKmsKeyId" => volume::reset_ebs_default_kms_key_id(self, &request),
            "CreateSnapshot" => snapshot::create_snapshot(self, &request),
            "CreateSnapshots" => snapshot::create_snapshots(self, &request),
            "DeleteSnapshot" => snapshot::delete_snapshot(self, &request),
            "DescribeSnapshots" => snapshot::describe_snapshots(self, &request),
            "CopySnapshot" => snapshot::copy_snapshot(self, &request),
            "DescribeSnapshotAttribute" => snapshot::describe_snapshot_attribute(self, &request),
            "ModifySnapshotAttribute" => snapshot::modify_snapshot_attribute(self, &request),
            "ResetSnapshotAttribute" => snapshot::reset_snapshot_attribute(self, &request),
            "ModifySnapshotTier" => snapshot::modify_snapshot_tier(self, &request),
            "DescribeSnapshotTierStatus" => snapshot::describe_snapshot_tier_status(self, &request),
            "RestoreSnapshotTier" => snapshot::restore_snapshot_tier(self, &request),
            "ListSnapshotsInRecycleBin" => snapshot::list_snapshots_in_recycle_bin(self, &request),
            "RestoreSnapshotFromRecycleBin" => {
                snapshot::restore_snapshot_from_recycle_bin(self, &request)
            }
            "LockSnapshot" => snapshot::lock_snapshot(self, &request),
            "UnlockSnapshot" => snapshot::unlock_snapshot(self, &request),
            "DescribeLockedSnapshots" => snapshot::describe_locked_snapshots(self, &request),
            "GetSnapshotBlockPublicAccessState" => {
                snapshot::get_snapshot_block_public_access_state(self, &request)
            }
            "EnableSnapshotBlockPublicAccess" => {
                snapshot::enable_snapshot_block_public_access(self, &request)
            }
            "DisableSnapshotBlockPublicAccess" => {
                snapshot::disable_snapshot_block_public_access(self, &request)
            }
            "EnableFastSnapshotRestores" => snapshot::enable_fast_snapshot_restores(self, &request),
            "DisableFastSnapshotRestores" => {
                snapshot::disable_fast_snapshot_restores(self, &request)
            }
            "DescribeFastSnapshotRestores" => {
                snapshot::describe_fast_snapshot_restores(self, &request)
            }
            "CreateImage" => image::create_image(self, &request),
            "RegisterImage" => image::register_image(self, &request),
            "DeregisterImage" => image::deregister_image(self, &request),
            "DescribeImages" => image::describe_images(self, &request),
            "CopyImage" => image::copy_image(self, &request),
            "DescribeImageAttribute" => image::describe_image_attribute(self, &request),
            "ModifyImageAttribute" => image::modify_image_attribute(self, &request),
            "ResetImageAttribute" => image::reset_image_attribute(self, &request),
            "EnableImage" => image::enable_image(self, &request),
            "DisableImage" => image::disable_image(self, &request),
            "EnableImageDeprecation" => image::enable_image_deprecation(self, &request),
            "DisableImageDeprecation" => image::disable_image_deprecation(self, &request),
            "EnableImageDeregistrationProtection" => {
                image::enable_image_deregistration_protection(self, &request)
            }
            "DisableImageDeregistrationProtection" => {
                image::disable_image_deregistration_protection(self, &request)
            }
            "CancelImageLaunchPermission" => image::cancel_image_launch_permission(self, &request),
            "RestoreImageFromRecycleBin" => image::restore_image_from_recycle_bin(self, &request),
            "ListImagesInRecycleBin" => image::list_images_in_recycle_bin(self, &request),
            "EnableImageBlockPublicAccess" => {
                image::enable_image_block_public_access(self, &request)
            }
            "DisableImageBlockPublicAccess" => {
                image::disable_image_block_public_access(self, &request)
            }
            "GetImageBlockPublicAccessState" => {
                image::get_image_block_public_access_state(self, &request)
            }
            "EnableAllowedImagesSettings" => image::enable_allowed_images_settings(self, &request),
            "DisableAllowedImagesSettings" => {
                image::disable_allowed_images_settings(self, &request)
            }
            "GetAllowedImagesSettings" => image::get_allowed_images_settings(self, &request),
            "ReplaceImageCriteriaInAllowedImagesSettings" => {
                image::replace_image_criteria_in_allowed_images_settings(self, &request)
            }
            "CreateStoreImageTask" => image::create_store_image_task(self, &request),
            "DescribeStoreImageTasks" => image::describe_store_image_tasks(self, &request),
            "CreateRestoreImageTask" => image::create_restore_image_task(self, &request),
            "DescribeFastLaunchImages" => image::describe_fast_launch_images(self, &request),
            "CreateNetworkAcl" => nacl::create_network_acl(self, &request),
            "DeleteNetworkAcl" => nacl::delete_network_acl(self, &request),
            "DescribeNetworkAcls" => nacl::describe_network_acls(self, &request),
            "CreateNetworkAclEntry" => nacl::create_network_acl_entry(self, &request),
            "DeleteNetworkAclEntry" => nacl::delete_network_acl_entry(self, &request),
            "ReplaceNetworkAclEntry" => nacl::replace_network_acl_entry(self, &request),
            "ReplaceNetworkAclAssociation" => nacl::replace_network_acl_association(self, &request),
            "CreateVpcPeeringConnection" => nacl::create_vpc_peering_connection(self, &request),
            "DeleteVpcPeeringConnection" => nacl::delete_vpc_peering_connection(self, &request),
            "DescribeVpcPeeringConnections" => {
                nacl::describe_vpc_peering_connections(self, &request)
            }
            "AcceptVpcPeeringConnection" => nacl::accept_vpc_peering_connection(self, &request),
            "RejectVpcPeeringConnection" => nacl::reject_vpc_peering_connection(self, &request),
            "ModifyVpcPeeringConnectionOptions" => {
                nacl::modify_vpc_peering_connection_options(self, &request)
            }
            "CreateVpcEndpoint" => endpoint::create_vpc_endpoint(self, &request),
            "DeleteVpcEndpoints" => endpoint::delete_vpc_endpoints(self, &request),
            "DescribeVpcEndpoints" => endpoint::describe_vpc_endpoints(self, &request),
            "ModifyVpcEndpoint" => endpoint::modify_vpc_endpoint(self, &request),
            "DescribeVpcEndpointServices" => {
                endpoint::describe_vpc_endpoint_services(self, &request)
            }
            "DescribeVpcEndpointConnections" => {
                endpoint::describe_vpc_endpoint_connections(self, &request)
            }
            "AcceptVpcEndpointConnections" => {
                endpoint::accept_vpc_endpoint_connections(self, &request)
            }
            "RejectVpcEndpointConnections" => {
                endpoint::reject_vpc_endpoint_connections(self, &request)
            }
            "CreateVpcEndpointServiceConfiguration" => {
                endpoint::create_vpc_endpoint_service_configuration(self, &request)
            }
            "DeleteVpcEndpointServiceConfigurations" => {
                endpoint::delete_vpc_endpoint_service_configurations(self, &request)
            }
            "DescribeVpcEndpointServiceConfigurations" => {
                endpoint::describe_vpc_endpoint_service_configurations(self, &request)
            }
            "ModifyVpcEndpointServiceConfiguration" => {
                endpoint::modify_vpc_endpoint_service_configuration(self, &request)
            }
            "DescribeVpcEndpointServicePermissions" => {
                endpoint::describe_vpc_endpoint_service_permissions(self, &request)
            }
            "ModifyVpcEndpointServicePermissions" => {
                endpoint::modify_vpc_endpoint_service_permissions(self, &request)
            }
            "ModifyVpcEndpointServicePayerResponsibility" => {
                endpoint::modify_vpc_endpoint_service_payer_responsibility(self, &request)
            }
            "StartVpcEndpointServicePrivateDnsVerification" => {
                endpoint::start_vpc_endpoint_service_private_dns_verification(self, &request)
            }
            "CreateVpcEndpointConnectionNotification" => {
                endpoint::create_vpc_endpoint_connection_notification(self, &request)
            }
            "DeleteVpcEndpointConnectionNotifications" => {
                endpoint::delete_vpc_endpoint_connection_notifications(self, &request)
            }
            "DescribeVpcEndpointConnectionNotifications" => {
                endpoint::describe_vpc_endpoint_connection_notifications(self, &request)
            }
            "ModifyVpcEndpointConnectionNotification" => {
                endpoint::modify_vpc_endpoint_connection_notification(self, &request)
            }
            "DescribeVpcEndpointAssociations" => {
                endpoint::describe_vpc_endpoint_associations(self, &request)
            }
            "CreateFlowLogs" => endpoint::create_flow_logs(self, &request),
            "DeleteFlowLogs" => endpoint::delete_flow_logs(self, &request),
            "DescribeFlowLogs" => endpoint::describe_flow_logs(self, &request),
            "GetFlowLogsIntegrationTemplate" => {
                endpoint::get_flow_logs_integration_template(self, &request)
            }
            "CreateLaunchTemplate" => fleet::create_launch_template(self, &request),
            "CreateLaunchTemplateVersion" => fleet::create_launch_template_version(self, &request),
            "DeleteLaunchTemplate" => fleet::delete_launch_template(self, &request),
            "DeleteLaunchTemplateVersions" => {
                fleet::delete_launch_template_versions(self, &request)
            }
            "DescribeLaunchTemplates" => fleet::describe_launch_templates(self, &request),
            "DescribeLaunchTemplateVersions" => {
                fleet::describe_launch_template_versions(self, &request)
            }
            "GetLaunchTemplateData" => fleet::get_launch_template_data(self, &request),
            "ModifyLaunchTemplate" => fleet::modify_launch_template(self, &request),
            "RequestSpotInstances" => fleet::request_spot_instances(self, &request),
            "DescribeSpotInstanceRequests" => {
                fleet::describe_spot_instance_requests(self, &request)
            }
            "CancelSpotInstanceRequests" => fleet::cancel_spot_instance_requests(self, &request),
            "RequestSpotFleet" => fleet::request_spot_fleet(self, &request),
            "DescribeSpotFleetRequests" => fleet::describe_spot_fleet_requests(self, &request),
            "CancelSpotFleetRequests" => fleet::cancel_spot_fleet_requests(self, &request),
            "ModifySpotFleetRequest" => fleet::modify_spot_fleet_request(self, &request),
            "DescribeSpotFleetInstances" => fleet::describe_spot_fleet_instances(self, &request),
            "DescribeSpotFleetRequestHistory" => {
                fleet::describe_spot_fleet_request_history(self, &request)
            }
            "DescribeSpotPriceHistory" => fleet::describe_spot_price_history(self, &request),
            "GetSpotPlacementScores" => fleet::get_spot_placement_scores(self, &request),
            "CreateSpotDatafeedSubscription" => {
                fleet::create_spot_datafeed_subscription(self, &request)
            }
            "DeleteSpotDatafeedSubscription" => {
                fleet::delete_spot_datafeed_subscription(self, &request)
            }
            "DescribeSpotDatafeedSubscription" => {
                fleet::describe_spot_datafeed_subscription(self, &request)
            }
            "CreateFleet" => fleet::create_fleet(self, &request),
            "DeleteFleets" => fleet::delete_fleets(self, &request),
            "DescribeFleets" => fleet::describe_fleets(self, &request),
            "ModifyFleet" => fleet::modify_fleet(self, &request),
            "DescribeFleetHistory" => fleet::describe_fleet_history(self, &request),
            "DescribeFleetInstances" => fleet::describe_fleet_instances(self, &request),
            "CreateCapacityReservation" => capacity::create_capacity_reservation(self, &request),
            "CancelCapacityReservation" => capacity::cancel_capacity_reservation(self, &request),
            "DescribeCapacityReservations" => {
                capacity::describe_capacity_reservations(self, &request)
            }
            "ModifyCapacityReservation" => capacity::modify_capacity_reservation(self, &request),
            "GetCapacityReservationUsage" => {
                capacity::get_capacity_reservation_usage(self, &request)
            }
            "CreateCapacityReservationFleet" => {
                capacity::create_capacity_reservation_fleet(self, &request)
            }
            "CancelCapacityReservationFleets" => {
                capacity::cancel_capacity_reservation_fleets(self, &request)
            }
            "DescribeCapacityReservationFleets" => {
                capacity::describe_capacity_reservation_fleets(self, &request)
            }
            "ModifyCapacityReservationFleet" => {
                capacity::modify_capacity_reservation_fleet(self, &request)
            }
            "ModifyInstanceCapacityReservationAttributes" => {
                capacity::modify_instance_capacity_reservation_attributes(self, &request)
            }
            "CreateCapacityReservationBySplitting" => {
                capacity::create_capacity_reservation_by_splitting(self, &request)
            }
            "MoveCapacityReservationInstances" => {
                capacity::move_capacity_reservation_instances(self, &request)
            }
            "DescribeCapacityReservationBillingRequests" => {
                capacity::describe_capacity_reservation_billing_requests(self, &request)
            }
            "AssociateCapacityReservationBillingOwner" => {
                capacity::associate_capacity_reservation_billing_owner(self, &request)
            }
            "DisassociateCapacityReservationBillingOwner" => {
                capacity::disassociate_capacity_reservation_billing_owner(self, &request)
            }
            "AcceptCapacityReservationBillingOwnership" => {
                capacity::accept_capacity_reservation_billing_ownership(self, &request)
            }
            "RejectCapacityReservationBillingOwnership" => {
                capacity::reject_capacity_reservation_billing_ownership(self, &request)
            }
            "DescribeCapacityBlockOfferings" => {
                capacity::describe_capacity_block_offerings(self, &request)
            }
            "DescribeCapacityBlocks" => capacity::describe_capacity_blocks(self, &request),
            "PurchaseCapacityBlock" => capacity::purchase_capacity_block(self, &request),
            "DescribeCapacityBlockStatus" => {
                capacity::describe_capacity_block_status(self, &request)
            }
            "DescribeCapacityBlockExtensionHistory" => {
                capacity::describe_capacity_block_extension_history(self, &request)
            }
            "DescribeCapacityBlockExtensionOfferings" => {
                capacity::describe_capacity_block_extension_offerings(self, &request)
            }
            "PurchaseCapacityBlockExtension" => {
                capacity::purchase_capacity_block_extension(self, &request)
            }
            "DescribeCapacityReservationTopology" => {
                capacity::describe_capacity_reservation_topology(self, &request)
            }
            "CreateInterruptibleCapacityReservationAllocation" => {
                capacity::create_interruptible_capacity_reservation_allocation(self, &request)
            }
            "UpdateInterruptibleCapacityReservationAllocation" => {
                capacity::update_interruptible_capacity_reservation_allocation(self, &request)
            }
            "DescribeReservedInstances" => reserved::describe_reserved_instances(self, &request),
            "DescribeReservedInstancesOfferings" => {
                reserved::describe_reserved_instances_offerings(self, &request)
            }
            "PurchaseReservedInstancesOffering" => {
                reserved::purchase_reserved_instances_offering(self, &request)
            }
            "DescribeReservedInstancesListings" => {
                reserved::describe_reserved_instances_listings(self, &request)
            }
            "CreateReservedInstancesListing" => {
                reserved::create_reserved_instances_listing(self, &request)
            }
            "CancelReservedInstancesListing" => {
                reserved::cancel_reserved_instances_listing(self, &request)
            }
            "DescribeReservedInstancesModifications" => {
                reserved::describe_reserved_instances_modifications(self, &request)
            }
            "ModifyReservedInstances" => reserved::modify_reserved_instances(self, &request),
            "GetReservedInstancesExchangeQuote" => {
                reserved::get_reserved_instances_exchange_quote(self, &request)
            }
            "AcceptReservedInstancesExchangeQuote" => {
                reserved::accept_reserved_instances_exchange_quote(self, &request)
            }
            "DeleteQueuedReservedInstances" => {
                reserved::delete_queued_reserved_instances(self, &request)
            }
            "AllocateHosts" => reserved::allocate_hosts(self, &request),
            "DescribeHosts" => reserved::describe_hosts(self, &request),
            "ModifyHosts" => reserved::modify_hosts(self, &request),
            "ReleaseHosts" => reserved::release_hosts(self, &request),
            "DescribeMacHosts" => reserved::describe_mac_hosts(self, &request),
            "CreateTransitGateway" => tgw::create_transit_gateway(self, &request),
            "DeleteTransitGateway" => tgw::delete_transit_gateway(self, &request),
            "DescribeTransitGateways" => tgw::describe_transit_gateways(self, &request),
            "ModifyTransitGateway" => tgw::modify_transit_gateway(self, &request),
            "CreateTransitGatewayVpcAttachment" => {
                tgw::create_transit_gateway_vpc_attachment(self, &request)
            }
            "DeleteTransitGatewayVpcAttachment" => {
                tgw::delete_transit_gateway_vpc_attachment(self, &request)
            }
            "DescribeTransitGatewayVpcAttachments" => {
                tgw::describe_transit_gateway_vpc_attachments(self, &request)
            }
            "ModifyTransitGatewayVpcAttachment" => {
                tgw::modify_transit_gateway_vpc_attachment(self, &request)
            }
            "AcceptTransitGatewayVpcAttachment" => {
                tgw::accept_transit_gateway_vpc_attachment(self, &request)
            }
            "RejectTransitGatewayVpcAttachment" => {
                tgw::reject_transit_gateway_vpc_attachment(self, &request)
            }
            "DescribeTransitGatewayAttachments" => {
                tgw::describe_transit_gateway_attachments(self, &request)
            }
            "CreateTransitGatewayRouteTable" => {
                tgw::create_transit_gateway_route_table(self, &request)
            }
            "DeleteTransitGatewayRouteTable" => {
                tgw::delete_transit_gateway_route_table(self, &request)
            }
            "DescribeTransitGatewayRouteTables" => {
                tgw::describe_transit_gateway_route_tables(self, &request)
            }
            "AssociateTransitGatewayRouteTable" => {
                tgw::associate_transit_gateway_route_table(self, &request)
            }
            "DisassociateTransitGatewayRouteTable" => {
                tgw::disassociate_transit_gateway_route_table(self, &request)
            }
            "EnableTransitGatewayRouteTablePropagation" => {
                tgw::enable_transit_gateway_route_table_propagation(self, &request)
            }
            "DisableTransitGatewayRouteTablePropagation" => {
                tgw::disable_transit_gateway_route_table_propagation(self, &request)
            }
            "CreateTransitGatewayRoute" => tgw::create_transit_gateway_route(self, &request),
            "DeleteTransitGatewayRoute" => tgw::delete_transit_gateway_route(self, &request),
            "ReplaceTransitGatewayRoute" => tgw::replace_transit_gateway_route(self, &request),
            "SearchTransitGatewayRoutes" => tgw::search_transit_gateway_routes(self, &request),
            "ExportTransitGatewayRoutes" => tgw::export_transit_gateway_routes(self, &request),
            "GetTransitGatewayRouteTableAssociations" => {
                tgw::get_transit_gateway_route_table_associations(self, &request)
            }
            "GetTransitGatewayRouteTablePropagations" => {
                tgw::get_transit_gateway_route_table_propagations(self, &request)
            }
            "GetTransitGatewayAttachmentPropagations" => {
                tgw::get_transit_gateway_attachment_propagations(self, &request)
            }
            "CreateTransitGatewayPrefixListReference" => {
                tgw::create_transit_gateway_prefix_list_reference(self, &request)
            }
            "DeleteTransitGatewayPrefixListReference" => {
                tgw::delete_transit_gateway_prefix_list_reference(self, &request)
            }
            "ModifyTransitGatewayPrefixListReference" => {
                tgw::modify_transit_gateway_prefix_list_reference(self, &request)
            }
            "GetTransitGatewayPrefixListReferences" => {
                tgw::get_transit_gateway_prefix_list_references(self, &request)
            }
            "CreateTransitGatewayPeeringAttachment" => {
                tgw_peering::create_transit_gateway_peering_attachment(self, &request)
            }
            "DeleteTransitGatewayPeeringAttachment" => {
                tgw_peering::delete_transit_gateway_peering_attachment(self, &request)
            }
            "DescribeTransitGatewayPeeringAttachments" => {
                tgw_peering::describe_transit_gateway_peering_attachments(self, &request)
            }
            "AcceptTransitGatewayPeeringAttachment" => {
                tgw_peering::accept_transit_gateway_peering_attachment(self, &request)
            }
            "RejectTransitGatewayPeeringAttachment" => {
                tgw_peering::reject_transit_gateway_peering_attachment(self, &request)
            }
            "CreateTransitGatewayConnect" => {
                tgw_peering::create_transit_gateway_connect(self, &request)
            }
            "DeleteTransitGatewayConnect" => {
                tgw_peering::delete_transit_gateway_connect(self, &request)
            }
            "DescribeTransitGatewayConnects" => {
                tgw_peering::describe_transit_gateway_connects(self, &request)
            }
            "CreateTransitGatewayConnectPeer" => {
                tgw_peering::create_transit_gateway_connect_peer(self, &request)
            }
            "DeleteTransitGatewayConnectPeer" => {
                tgw_peering::delete_transit_gateway_connect_peer(self, &request)
            }
            "DescribeTransitGatewayConnectPeers" => {
                tgw_peering::describe_transit_gateway_connect_peers(self, &request)
            }
            "CreateTransitGatewayPolicyTable" => {
                tgw_peering::create_transit_gateway_policy_table(self, &request)
            }
            "DeleteTransitGatewayPolicyTable" => {
                tgw_peering::delete_transit_gateway_policy_table(self, &request)
            }
            "DescribeTransitGatewayPolicyTables" => {
                tgw_peering::describe_transit_gateway_policy_tables(self, &request)
            }
            "AssociateTransitGatewayPolicyTable" => {
                tgw_peering::associate_transit_gateway_policy_table(self, &request)
            }
            "DisassociateTransitGatewayPolicyTable" => {
                tgw_peering::disassociate_transit_gateway_policy_table(self, &request)
            }
            "GetTransitGatewayPolicyTableAssociations" => {
                tgw_peering::get_transit_gateway_policy_table_associations(self, &request)
            }
            "GetTransitGatewayPolicyTableEntries" => {
                tgw_peering::get_transit_gateway_policy_table_entries(self, &request)
            }
            "CreateTransitGatewayRouteTableAnnouncement" => {
                tgw_peering::create_transit_gateway_route_table_announcement(self, &request)
            }
            "DeleteTransitGatewayRouteTableAnnouncement" => {
                tgw_peering::delete_transit_gateway_route_table_announcement(self, &request)
            }
            "DescribeTransitGatewayRouteTableAnnouncements" => {
                tgw_peering::describe_transit_gateway_route_table_announcements(self, &request)
            }
            "CreateTransitGatewayMulticastDomain" => {
                tgw_mcast::create_transit_gateway_multicast_domain(self, &request)
            }
            "DeleteTransitGatewayMulticastDomain" => {
                tgw_mcast::delete_transit_gateway_multicast_domain(self, &request)
            }
            "DescribeTransitGatewayMulticastDomains" => {
                tgw_mcast::describe_transit_gateway_multicast_domains(self, &request)
            }
            "AssociateTransitGatewayMulticastDomain" => {
                tgw_mcast::associate_transit_gateway_multicast_domain(self, &request)
            }
            "DisassociateTransitGatewayMulticastDomain" => {
                tgw_mcast::disassociate_transit_gateway_multicast_domain(self, &request)
            }
            "AcceptTransitGatewayMulticastDomainAssociations" => {
                tgw_mcast::accept_transit_gateway_multicast_domain_associations(self, &request)
            }
            "RejectTransitGatewayMulticastDomainAssociations" => {
                tgw_mcast::reject_transit_gateway_multicast_domain_associations(self, &request)
            }
            "GetTransitGatewayMulticastDomainAssociations" => {
                tgw_mcast::get_transit_gateway_multicast_domain_associations(self, &request)
            }
            "RegisterTransitGatewayMulticastGroupMembers" => {
                tgw_mcast::register_transit_gateway_multicast_group_members(self, &request)
            }
            "RegisterTransitGatewayMulticastGroupSources" => {
                tgw_mcast::register_transit_gateway_multicast_group_sources(self, &request)
            }
            "DeregisterTransitGatewayMulticastGroupMembers" => {
                tgw_mcast::deregister_transit_gateway_multicast_group_members(self, &request)
            }
            "DeregisterTransitGatewayMulticastGroupSources" => {
                tgw_mcast::deregister_transit_gateway_multicast_group_sources(self, &request)
            }
            "SearchTransitGatewayMulticastGroups" => {
                tgw_mcast::search_transit_gateway_multicast_groups(self, &request)
            }
            "CreateTransitGatewayMeteringPolicy" => {
                tgw_mcast::create_transit_gateway_metering_policy(self, &request)
            }
            "DeleteTransitGatewayMeteringPolicy" => {
                tgw_mcast::delete_transit_gateway_metering_policy(self, &request)
            }
            "DescribeTransitGatewayMeteringPolicies" => {
                tgw_mcast::describe_transit_gateway_metering_policies(self, &request)
            }
            "ModifyTransitGatewayMeteringPolicy" => {
                tgw_mcast::modify_transit_gateway_metering_policy(self, &request)
            }
            "CreateTransitGatewayMeteringPolicyEntry" => {
                tgw_mcast::create_transit_gateway_metering_policy_entry(self, &request)
            }
            "DeleteTransitGatewayMeteringPolicyEntry" => {
                tgw_mcast::delete_transit_gateway_metering_policy_entry(self, &request)
            }
            "GetTransitGatewayMeteringPolicyEntries" => {
                tgw_mcast::get_transit_gateway_metering_policy_entries(self, &request)
            }
            "AcceptTransitGatewayClientVpnAttachment" => {
                tgw_mcast::accept_transit_gateway_client_vpn_attachment(self, &request)
            }
            "DeleteTransitGatewayClientVpnAttachment" => {
                tgw_mcast::delete_transit_gateway_client_vpn_attachment(self, &request)
            }
            "RejectTransitGatewayClientVpnAttachment" => {
                tgw_mcast::reject_transit_gateway_client_vpn_attachment(self, &request)
            }
            "CreateCustomerGateway" => vpn::create_customer_gateway(self, &request),
            "DeleteCustomerGateway" => vpn::delete_customer_gateway(self, &request),
            "DescribeCustomerGateways" => vpn::describe_customer_gateways(self, &request),
            "CreateVpnGateway" => vpn::create_vpn_gateway(self, &request),
            "DeleteVpnGateway" => vpn::delete_vpn_gateway(self, &request),
            "DescribeVpnGateways" => vpn::describe_vpn_gateways(self, &request),
            "AttachVpnGateway" => vpn::attach_vpn_gateway(self, &request),
            "DetachVpnGateway" => vpn::detach_vpn_gateway(self, &request),
            "CreateVpnConnection" => vpn::create_vpn_connection(self, &request),
            "DeleteVpnConnection" => vpn::delete_vpn_connection(self, &request),
            "DescribeVpnConnections" => vpn::describe_vpn_connections(self, &request),
            "ModifyVpnConnection" => vpn::modify_vpn_connection(self, &request),
            "ModifyVpnConnectionOptions" => vpn::modify_vpn_connection_options(self, &request),
            "CreateVpnConnectionRoute" => vpn::create_vpn_connection_route(self, &request),
            "DeleteVpnConnectionRoute" => vpn::delete_vpn_connection_route(self, &request),
            "ModifyVpnTunnelOptions" => vpn::modify_vpn_tunnel_options(self, &request),
            "ModifyVpnTunnelCertificate" => vpn::modify_vpn_tunnel_certificate(self, &request),
            "ReplaceVpnTunnel" => vpn::replace_vpn_tunnel(self, &request),
            "GetActiveVpnTunnelStatus" => vpn::get_active_vpn_tunnel_status(self, &request),
            "GetVpnTunnelReplacementStatus" => {
                vpn::get_vpn_tunnel_replacement_status(self, &request)
            }
            "GetVpnConnectionDeviceTypes" => vpn::get_vpn_connection_device_types(self, &request),
            "GetVpnConnectionDeviceSampleConfiguration" => {
                vpn::get_vpn_connection_device_sample_configuration(self, &request)
            }
            "CreateVpnConcentrator" => vpn::create_vpn_concentrator(self, &request),
            "DeleteVpnConcentrator" => vpn::delete_vpn_concentrator(self, &request),
            "DescribeVpnConcentrators" => vpn::describe_vpn_concentrators(self, &request),
            "CreateClientVpnEndpoint" => cvpn::create_client_vpn_endpoint(self, &request),
            "DeleteClientVpnEndpoint" => cvpn::delete_client_vpn_endpoint(self, &request),
            "DescribeClientVpnEndpoints" => cvpn::describe_client_vpn_endpoints(self, &request),
            "ModifyClientVpnEndpoint" => cvpn::modify_client_vpn_endpoint(self, &request),
            "CreateClientVpnRoute" => cvpn::create_client_vpn_route(self, &request),
            "DeleteClientVpnRoute" => cvpn::delete_client_vpn_route(self, &request),
            "DescribeClientVpnRoutes" => cvpn::describe_client_vpn_routes(self, &request),
            "AuthorizeClientVpnIngress" => cvpn::authorize_client_vpn_ingress(self, &request),
            "RevokeClientVpnIngress" => cvpn::revoke_client_vpn_ingress(self, &request),
            "DescribeClientVpnAuthorizationRules" => {
                cvpn::describe_client_vpn_authorization_rules(self, &request)
            }
            "AssociateClientVpnTargetNetwork" => {
                cvpn::associate_client_vpn_target_network(self, &request)
            }
            "DisassociateClientVpnTargetNetwork" => {
                cvpn::disassociate_client_vpn_target_network(self, &request)
            }
            "DescribeClientVpnTargetNetworks" => {
                cvpn::describe_client_vpn_target_networks(self, &request)
            }
            "ApplySecurityGroupsToClientVpnTargetNetwork" => {
                cvpn::apply_security_groups_to_client_vpn_target_network(self, &request)
            }
            "DescribeClientVpnConnections" => cvpn::describe_client_vpn_connections(self, &request),
            "TerminateClientVpnConnections" => {
                cvpn::terminate_client_vpn_connections(self, &request)
            }
            "ExportClientVpnClientCertificateRevocationList" => {
                cvpn::export_client_vpn_client_certificate_revocation_list(self, &request)
            }
            "ExportClientVpnClientConfiguration" => {
                cvpn::export_client_vpn_client_configuration(self, &request)
            }
            "ImportClientVpnClientCertificateRevocationList" => {
                cvpn::import_client_vpn_client_certificate_revocation_list(self, &request)
            }
            "CreateIpam" => ipam::create_ipam(self, &request),
            "DeleteIpam" => ipam::delete_ipam(self, &request),
            "DescribeIpams" => ipam::describe_ipams(self, &request),
            "ModifyIpam" => ipam::modify_ipam(self, &request),
            "CreateIpamScope" => ipam::create_ipam_scope(self, &request),
            "DeleteIpamScope" => ipam::delete_ipam_scope(self, &request),
            "DescribeIpamScopes" => ipam::describe_ipam_scopes(self, &request),
            "ModifyIpamScope" => ipam::modify_ipam_scope(self, &request),
            "CreateIpamPool" => ipam::create_ipam_pool(self, &request),
            "DeleteIpamPool" => ipam::delete_ipam_pool(self, &request),
            "DescribeIpamPools" => ipam::describe_ipam_pools(self, &request),
            "ModifyIpamPool" => ipam::modify_ipam_pool(self, &request),
            "ProvisionIpamPoolCidr" => ipam::provision_ipam_pool_cidr(self, &request),
            "DeprovisionIpamPoolCidr" => ipam::deprovision_ipam_pool_cidr(self, &request),
            "GetIpamPoolCidrs" => ipam::get_ipam_pool_cidrs(self, &request),
            "AllocateIpamPoolCidr" => ipam::allocate_ipam_pool_cidr(self, &request),
            "ReleaseIpamPoolAllocation" => ipam::release_ipam_pool_allocation(self, &request),
            "GetIpamPoolAllocations" => ipam::get_ipam_pool_allocations(self, &request),
            "GetIpamResourceCidrs" => ipam::get_ipam_resource_cidrs(self, &request),
            "ModifyIpamResourceCidr" => ipam::modify_ipam_resource_cidr(self, &request),
            "GetIpamAddressHistory" => ipam::get_ipam_address_history(self, &request),
            "EnableIpamOrganizationAdminAccount" => {
                ipam::enable_ipam_organization_admin_account(self, &request)
            }
            "DisableIpamOrganizationAdminAccount" => {
                ipam::disable_ipam_organization_admin_account(self, &request)
            }
            "CreateIpamResourceDiscovery" => {
                ipam_discovery::create_ipam_resource_discovery(self, &request)
            }
            "DeleteIpamResourceDiscovery" => {
                ipam_discovery::delete_ipam_resource_discovery(self, &request)
            }
            "DescribeIpamResourceDiscoveries" => {
                ipam_discovery::describe_ipam_resource_discoveries(self, &request)
            }
            "ModifyIpamResourceDiscovery" => {
                ipam_discovery::modify_ipam_resource_discovery(self, &request)
            }
            "AssociateIpamResourceDiscovery" => {
                ipam_discovery::associate_ipam_resource_discovery(self, &request)
            }
            "DisassociateIpamResourceDiscovery" => {
                ipam_discovery::disassociate_ipam_resource_discovery(self, &request)
            }
            "DescribeIpamResourceDiscoveryAssociations" => {
                ipam_discovery::describe_ipam_resource_discovery_associations(self, &request)
            }
            "GetIpamDiscoveredAccounts" => {
                ipam_discovery::get_ipam_discovered_accounts(self, &request)
            }
            "GetIpamDiscoveredPublicAddresses" => {
                ipam_discovery::get_ipam_discovered_public_addresses(self, &request)
            }
            "GetIpamDiscoveredResourceCidrs" => {
                ipam_discovery::get_ipam_discovered_resource_cidrs(self, &request)
            }
            "AssociateIpamByoasn" => ipam_discovery::associate_ipam_byoasn(self, &request),
            "DisassociateIpamByoasn" => ipam_discovery::disassociate_ipam_byoasn(self, &request),
            "ProvisionIpamByoasn" => ipam_discovery::provision_ipam_byoasn(self, &request),
            "DeprovisionIpamByoasn" => ipam_discovery::deprovision_ipam_byoasn(self, &request),
            "DescribeIpamByoasn" => ipam_discovery::describe_ipam_byoasn(self, &request),
            "MoveByoipCidrToIpam" => ipam_discovery::move_byoip_cidr_to_ipam(self, &request),
            "CreateIpamExternalResourceVerificationToken" => {
                ipam_discovery::create_ipam_external_resource_verification_token(self, &request)
            }
            "DeleteIpamExternalResourceVerificationToken" => {
                ipam_discovery::delete_ipam_external_resource_verification_token(self, &request)
            }
            "DescribeIpamExternalResourceVerificationTokens" => {
                ipam_discovery::describe_ipam_external_resource_verification_tokens(self, &request)
            }
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
