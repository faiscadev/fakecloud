//! A mount target's footprint in the customer VPC.
//!
//! On AWS, `CreateMountTarget` places a requester-managed network interface in
//! the subnet (description `EFS mount target for fs-... (fsmt-...)`), takes its
//! IP from the subnet's CIDR, and attaches the given security groups, or the
//! VPC's `default` group when none are given. Every one of those ids is real in
//! EC2: `aws_efs_mount_target` reads the interface back, and security-group
//! rules reference the groups. Shared by the API handler and the
//! CloudFormation provisioner so both create the same EC2 records.

use fakecloud_core::service::AwsServiceError;
use fakecloud_ec2::vpc_lookup::{self, EniError, ServiceEni, SubnetInfo};
use fakecloud_ec2::SharedEc2State;
use http::StatusCode;

/// The most security groups a mount target may carry.
pub const MAX_MOUNT_TARGET_SECURITY_GROUPS: usize = 5;

/// Why a mount target could not be placed in the VPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkError {
    SubnetNotFound(String),
    SecurityGroupNotFound(String),
    SecurityGroupNotInVpc { group_id: String, vpc_id: String },
    SecurityGroupLimitExceeded,
    IpAddressInUse(String),
    IpAddressOutsideSubnet { ip: String, subnet_id: String },
    IpAddressReserved { ip: String, subnet_id: String },
    NoFreeAddressesInSubnet(String),
}

impl NetworkError {
    /// The AWS error code EFS returns.
    pub fn code(&self) -> &'static str {
        match self {
            Self::SubnetNotFound(_) => "SubnetNotFound",
            Self::SecurityGroupNotFound(_) | Self::SecurityGroupNotInVpc { .. } => {
                "SecurityGroupNotFound"
            }
            Self::SecurityGroupLimitExceeded => "SecurityGroupLimitExceeded",
            Self::IpAddressInUse(_) => "IpAddressInUse",
            Self::IpAddressOutsideSubnet { .. } | Self::IpAddressReserved { .. } => "BadRequest",
            Self::NoFreeAddressesInSubnet(_) => "NoFreeAddressesInSubnet",
        }
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::IpAddressInUse(_) | Self::NoFreeAddressesInSubnet(_) => StatusCode::CONFLICT,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::SubnetNotFound(id) => {
                format!("The subnet ID '{id}' is invalid or does not exist.")
            }
            Self::SecurityGroupNotFound(id) => {
                format!("The security group '{id}' does not exist.")
            }
            Self::SecurityGroupNotInVpc { group_id, vpc_id } => format!(
                "The security group '{group_id}' does not belong to VPC '{vpc_id}' of the mount target's subnet."
            ),
            Self::SecurityGroupLimitExceeded => format!(
                "A mount target can have at most {MAX_MOUNT_TARGET_SECURITY_GROUPS} security groups."
            ),
            Self::IpAddressInUse(ip) => {
                format!("The IP address '{ip}' is already in use in the subnet.")
            }
            Self::IpAddressOutsideSubnet { ip, subnet_id } => format!(
                "The IP address '{ip}' is not within the CIDR block of subnet '{subnet_id}'."
            ),
            Self::IpAddressReserved { ip, subnet_id } => format!(
                "The IP address '{ip}' is reserved in subnet '{subnet_id}'."
            ),
            Self::NoFreeAddressesInSubnet(id) => {
                format!("The subnet '{id}' has no free IP addresses.")
            }
        }
    }

    pub fn to_aws(&self) -> AwsServiceError {
        AwsServiceError::aws_error(self.status(), self.code(), self.message())
    }
}

/// Where a new mount target landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountTargetNetwork {
    pub subnet: SubnetInfo,
    pub security_groups: Vec<String>,
    pub network_interface_id: String,
    pub ip_address: String,
}

/// Resolve the subnet a mount target is created in.
pub fn resolve_subnet(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    subnet_id: &str,
) -> Result<SubnetInfo, NetworkError> {
    vpc_lookup::resolve_subnets(ec2, account_id, region, &[subnet_id.to_string()])
        .map(|mut v| v.remove(0))
        .map_err(NetworkError::SubnetNotFound)
}

/// Validate `groups` for a mount target in `vpc_id`; an empty list resolves to
/// the VPC's `default` security group.
pub fn resolve_security_groups(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    vpc_id: &str,
    groups: &[String],
) -> Result<Vec<String>, NetworkError> {
    if groups.len() > MAX_MOUNT_TARGET_SECURITY_GROUPS {
        return Err(NetworkError::SecurityGroupLimitExceeded);
    }
    if groups.is_empty() {
        return Ok(
            vpc_lookup::default_security_group_id(ec2, account_id, region, vpc_id)
                .into_iter()
                .collect(),
        );
    }
    let resolved = vpc_lookup::security_group_vpcs(ec2, account_id, region, groups)
        .map_err(NetworkError::SecurityGroupNotFound)?;
    if let Some((group_id, _)) = resolved.iter().find(|(_, v)| v != vpc_id) {
        return Err(NetworkError::SecurityGroupNotInVpc {
            group_id: group_id.clone(),
            vpc_id: vpc_id.to_string(),
        });
    }
    Ok(groups.to_vec())
}

/// Create the mount target's network interface in `subnet` with `groups`
/// (already validated by [`resolve_security_groups`]).
#[allow(clippy::too_many_arguments)]
pub fn create_mount_target_eni(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    file_system_id: &str,
    mount_target_id: &str,
    subnet: &SubnetInfo,
    groups: Vec<String>,
    ip_address: Option<&str>,
) -> Result<MountTargetNetwork, NetworkError> {
    let mut accounts = ec2.write();
    let state = accounts.regional_mut(account_id, region);
    let (eni_id, ip) = vpc_lookup::create_service_eni(
        state,
        ServiceEni {
            subnet_id: &subnet.subnet_id,
            group_ids: groups.clone(),
            description: format!("EFS mount target for {file_system_id} ({mount_target_id})"),
            private_ip: ip_address,
        },
    )
    .map_err(|e| match e {
        EniError::SubnetNotFound(id) => NetworkError::SubnetNotFound(id),
        EniError::IpInUse(ip) => NetworkError::IpAddressInUse(ip),
        EniError::IpOutsideSubnet(ip) => NetworkError::IpAddressOutsideSubnet {
            ip,
            subnet_id: subnet.subnet_id.clone(),
        },
        EniError::IpReserved(ip) => NetworkError::IpAddressReserved {
            ip,
            subnet_id: subnet.subnet_id.clone(),
        },
        EniError::NoFreeAddresses(id) => NetworkError::NoFreeAddressesInSubnet(id),
    })?;
    Ok(MountTargetNetwork {
        subnet: subnet.clone(),
        security_groups: groups,
        network_interface_id: eni_id,
        ip_address: ip,
    })
}

/// Replace the security groups on a mount target's network interface.
pub fn set_mount_target_eni_groups(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    eni_id: &str,
    groups: &[String],
) {
    let mut accounts = ec2.write();
    let state = accounts.regional_mut(account_id, region);
    if let Some(eni) = state.network_interfaces.get_mut(eni_id) {
        eni.group_ids = groups.to_vec();
    }
}

/// Delete a mount target's network interface.
pub fn delete_mount_target_eni(ec2: &SharedEc2State, account_id: &str, region: &str, eni_id: &str) {
    let mut accounts = ec2.write();
    let state = accounts.regional_mut(account_id, region);
    vpc_lookup::delete_service_eni(state, eni_id);
}

/// The VPC id of an existing mount target's network interface, for validating
/// replacement security groups.
pub fn eni_vpc_id(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    eni_id: &str,
) -> Option<String> {
    let mut accounts = ec2.write();
    let state = accounts.regional_mut(account_id, region);
    state
        .network_interfaces
        .get(eni_id)
        .map(|e| e.vpc_id.clone())
}
