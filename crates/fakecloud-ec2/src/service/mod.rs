//! EC2 service entrypoint: `ec2Query` dispatch over the per-account state.

mod dhcp;
mod meta;
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
