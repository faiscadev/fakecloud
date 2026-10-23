//! Where a load balancer sits in the customer VPC.
//!
//! A load balancer's `VpcId` and per-subnet `ZoneName` come from the EC2
//! subnets it is attached to, its security groups must exist in that VPC
//! (an Application Load Balancer created without any gets the VPC's `default`
//! group), and its `CanonicalHostedZoneId` is the Route 53 zone AWS publishes
//! for Elastic Load Balancing in the region. Shared by the API handlers and
//! the CloudFormation provisioner.

use fakecloud_core::service::AwsServiceError;
use fakecloud_ec2::vpc_lookup;
use fakecloud_ec2::SharedEc2State;
use http::StatusCode;

use crate::state::{AvailabilityZone, LoadBalancerAddress};

/// Route 53 hosted zone ids AWS publishes per region for Application and
/// Classic Load Balancers ("Elastic Load Balancing endpoints and quotas").
const ALB_HOSTED_ZONE_IDS: &[(&str, &str)] = &[
    ("af-south-1", "Z268VQBMOI5EKX"),
    ("ap-east-1", "Z3DQVH9N71FHZ0"),
    ("ap-northeast-1", "Z14GRHDCWA56QT"),
    ("ap-northeast-2", "ZWKZPGTI48KDX"),
    ("ap-northeast-3", "Z5LXEXXYW11ES"),
    ("ap-south-1", "ZP97RAFLXTNZK"),
    ("ap-south-2", "Z0173938T07WNTVAEPZN"),
    ("ap-southeast-1", "Z1LMS91P8CMLE5"),
    ("ap-southeast-2", "Z1GM3OXH4ZPM65"),
    ("ap-southeast-3", "Z08888821HLRG5A9ZRTER"),
    ("ap-southeast-4", "Z09517862IB2WZLPXG76F"),
    ("ap-southeast-5", "Z06010284QMVVW7WO5J"),
    ("ap-southeast-7", "Z0390008CMBRTHFGWBCB"),
    ("ca-central-1", "ZQSVJUPU6J1EY"),
    ("ca-west-1", "Z06473681N0SF6OS049SD"),
    ("cn-north-1", "Z1GDH35T77C1KE"),
    ("cn-northwest-1", "ZM7IZAIOVVDZF"),
    ("eu-central-1", "Z215JYRZR1TBD5"),
    ("eu-central-2", "Z06391101F2ZOEP8P5EB3"),
    ("eu-north-1", "Z23TAZ6LKFMNIO"),
    ("eu-south-1", "Z3ULH7SSC9OV64"),
    ("eu-south-2", "Z0956581394HF5D5LXGAP"),
    ("eu-west-1", "Z32O12XQLNTSW2"),
    ("eu-west-2", "ZHURV8PSTC4K8"),
    ("eu-west-3", "Z3Q77PNBQS71R4"),
    ("il-central-1", "Z09170902867EHPV2DABU"),
    ("me-central-1", "Z08230872XQRWHG2XF6I"),
    ("me-south-1", "ZS929ML54UICD"),
    ("mx-central-1", "Z023552324OKD1BB28BH5"),
    ("sa-east-1", "Z2P70J7HTTTPLU"),
    ("us-east-1", "Z35SXDOTRQ7X7K"),
    ("us-east-2", "Z3AADJGX6KTTL2"),
    ("us-gov-east-1", "Z166TLBEWOO7G0"),
    ("us-gov-west-1", "Z33AYJ8TM3BH4J"),
    ("us-west-1", "Z368ELLRRE2KJ0"),
    ("us-west-2", "Z1H1FL5HABSF5"),
];

/// Route 53 hosted zone ids AWS publishes per region for Network Load
/// Balancers.
const NLB_HOSTED_ZONE_IDS: &[(&str, &str)] = &[
    ("af-south-1", "Z203XCE67M25HM"),
    ("ap-east-1", "Z12Y7K3UBGUAD1"),
    ("ap-northeast-1", "Z31USIVHYNEOWT"),
    ("ap-northeast-2", "ZIBE1TIR4HY56"),
    ("ap-northeast-3", "Z1GWIQ4HH19I5X"),
    ("ap-south-1", "ZVDDRBQ08TROA"),
    ("ap-south-2", "Z0711778386UTO08407HT"),
    ("ap-southeast-1", "ZKVM4W9LS7TM"),
    ("ap-southeast-2", "ZCT6FZBF4DROD"),
    ("ap-southeast-3", "Z01971771FYVNCOVWJU1G"),
    ("ap-southeast-4", "Z01156963G8MIIL7X90IV"),
    ("ap-southeast-5", "Z026317210H9ACVTRO6FB"),
    ("ap-southeast-7", "Z054363131YWATEMWRG5L"),
    ("ca-central-1", "Z2EPGBW3API2WT"),
    ("ca-west-1", "Z02754302KBB00W2LKWZ9"),
    ("cn-north-1", "Z3QFB96KMJ7ED6"),
    ("cn-northwest-1", "ZQEIKTCZ8352D"),
    ("eu-central-1", "Z3F0SRJ5LGBH90"),
    ("eu-central-2", "Z02239872DOALSIDCX66S"),
    ("eu-north-1", "Z1UDT6IFJ4EJM"),
    ("eu-south-1", "Z23146JA1KNAFP"),
    ("eu-south-2", "Z1011216NVTVYADP1SSV"),
    ("eu-west-1", "Z2IFOLAFXWLO4F"),
    ("eu-west-2", "ZD4D7Y8KGAS4G"),
    ("eu-west-3", "Z1CMS0P5QUZ6D5"),
    ("il-central-1", "Z0313266YDI6ZRHTGQY4"),
    ("me-central-1", "Z00282643NTTLPANJJG2P"),
    ("me-south-1", "Z3QSRYVP46NYYV"),
    ("mx-central-1", "Z02031231H3ID6HYJ9A7U"),
    ("sa-east-1", "ZTK26PT1VY4CU"),
    ("us-east-1", "Z26RNL4JYFTOTI"),
    ("us-east-2", "ZLMOA37VPKANP"),
    ("us-gov-east-1", "Z1ZSMQQ6Q24QQ8"),
    ("us-gov-west-1", "ZMG1MZ2THAWF1"),
    ("us-west-1", "Z24FKFUX50B4VW"),
    ("us-west-2", "Z18D5FSROUN65G"),
];

/// The `CanonicalHostedZoneId` of a load balancer of `lb_type` in `region`.
/// Network Load Balancers use their own per-region zone; Application (and
/// Gateway) Load Balancers share the Elastic Load Balancing zone. A region
/// outside the published table resolves as `us-east-1`.
pub fn canonical_hosted_zone_id(region: &str, lb_type: &str) -> &'static str {
    let table = if lb_type == "network" {
        NLB_HOSTED_ZONE_IDS
    } else {
        ALB_HOSTED_ZONE_IDS
    };
    table
        .iter()
        .find(|(r, _)| *r == region)
        .or_else(|| table.iter().find(|(r, _)| *r == "us-east-1"))
        .map(|(_, z)| *z)
        .unwrap_or_default()
}

/// The placeholder `CanonicalHostedZoneId` every load balancer reported
/// before the per-region zones were used.
const LEGACY_HOSTED_ZONE_ID: &str = "Z2P70J7EXAMPLE";

/// Give load balancers restored from a snapshot taken before the per-region
/// zones were used their real `CanonicalHostedZoneId` (the region comes from
/// the load balancer's ARN). Returns how many were fixed.
pub fn restore_canonical_hosted_zone_ids(accounts: &mut crate::state::Elbv2Accounts) -> usize {
    let mut fixed = 0;
    for (_, st) in accounts.iter_mut() {
        for lb in st.load_balancers.values_mut() {
            if lb.canonical_hosted_zone_id == LEGACY_HOSTED_ZONE_ID {
                let region = lb.arn.split(':').nth(3).unwrap_or("us-east-1");
                lb.canonical_hosted_zone_id =
                    canonical_hosted_zone_id(region, &lb.lb_type).to_string();
                fixed += 1;
            }
        }
    }
    fixed
}

/// The VPC a load balancer really sits in: its stored `VpcId` when EC2 knows
/// that VPC, otherwise the VPC of the first of its subnets EC2 knows. Load
/// balancers persisted before VPCs were resolved carry an invented (or empty)
/// `VpcId`; this re-derives it instead of enforcing it. `None` when neither
/// resolves.
pub fn effective_vpc(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    stored_vpc: &str,
    subnet_ids: &[String],
) -> Option<String> {
    let mut accounts = ec2.write();
    let state = accounts.regional_mut(account_id, region);
    if !stored_vpc.is_empty() && state.vpcs.contains_key(stored_vpc) {
        return Some(stored_vpc.to_string());
    }
    subnet_ids
        .iter()
        .find_map(|id| state.subnets.get(id).map(|s| s.vpc_id.clone()))
}

/// Re-derive the `VpcId` of restored load balancers whose stored VPC EC2 does
/// not know (snapshots taken before VPCs were resolved), from their subnets.
/// Returns how many were fixed.
pub fn restore_vpc_ids(accounts: &mut crate::state::Elbv2Accounts, ec2: &SharedEc2State) -> usize {
    let mut fixed = 0;
    for (account_id, st) in accounts.iter_mut() {
        for lb in st.load_balancers.values_mut() {
            let subnets: Vec<String> = lb
                .availability_zones
                .iter()
                .map(|z| z.subnet_id.clone())
                .collect();
            let region = lb.arn.split(':').nth(3).unwrap_or("us-east-1");
            if let Some(vpc) = effective_vpc(ec2, account_id, region, &lb.vpc_id, &subnets) {
                if vpc != lb.vpc_id {
                    lb.vpc_id = vpc;
                    fixed += 1;
                }
            }
        }
    }
    fixed
}

/// One requested subnet attachment (`Subnets.member.N` or
/// `SubnetMappings.member.N`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubnetRequest {
    pub subnet_id: String,
    pub allocation_id: Option<String>,
    pub private_ipv4_address: Option<String>,
    pub ipv6_address: Option<String>,
    pub source_nat_ipv6_prefix: Option<String>,
}

impl SubnetRequest {
    pub fn subnet(id: impl Into<String>) -> Self {
        Self {
            subnet_id: id.into(),
            ..Self::default()
        }
    }
}

/// Why a load balancer's subnets or security groups were rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkError {
    SubnetNotFound(String),
    SubnetsInDifferentVpcs,
    SubnetNotInVpc { subnet_id: String, vpc_id: String },
    DuplicateAvailabilityZone(String),
    PrivateIpOutsideSubnet { ip: String, subnet_id: String },
    AllocationIdNotFound(String),
    SecurityGroupNotFound(String),
    SecurityGroupNotInVpc { group_id: String, vpc_id: String },
}

impl NetworkError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::SubnetNotFound(_) => "SubnetNotFound",
            Self::SubnetsInDifferentVpcs | Self::SubnetNotInVpc { .. } => "InvalidSubnet",
            Self::DuplicateAvailabilityZone(_) | Self::PrivateIpOutsideSubnet { .. } => {
                "InvalidConfigurationRequest"
            }
            Self::AllocationIdNotFound(_) => "AllocationIdNotFound",
            Self::SecurityGroupNotFound(_) | Self::SecurityGroupNotInVpc { .. } => {
                "InvalidSecurityGroup"
            }
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::SubnetNotFound(id) => format!("The subnet ID '{id}' is not valid"),
            Self::SubnetsInDifferentVpcs => {
                "The specified subnets must all belong to the same VPC".to_string()
            }
            Self::SubnetNotInVpc { subnet_id, vpc_id } => format!(
                "Subnet '{subnet_id}' is not in the load balancer's VPC '{vpc_id}'"
            ),
            Self::DuplicateAvailabilityZone(az) => format!(
                "A load balancer cannot be attached to multiple subnets in the same Availability Zone '{az}'"
            ),
            Self::PrivateIpOutsideSubnet { ip, subnet_id } => format!(
                "The private IPv4 address '{ip}' is not a usable address in the CIDR block of subnet '{subnet_id}'"
            ),
            Self::AllocationIdNotFound(id) => {
                format!("The allocation ID '{id}' does not exist")
            }
            Self::SecurityGroupNotFound(id) => format!("Security group '{id}' does not exist"),
            Self::SecurityGroupNotInVpc { group_id, vpc_id } => format!(
                "Security group '{group_id}' does not belong to VPC '{vpc_id}'"
            ),
        }
    }

    pub fn to_aws(&self) -> AwsServiceError {
        AwsServiceError::aws_error(StatusCode::BAD_REQUEST, self.code(), self.message())
    }
}

/// The resolved placement of a load balancer.
#[derive(Debug, Clone)]
pub struct LoadBalancerNetwork {
    pub vpc_id: String,
    pub availability_zones: Vec<AvailabilityZone>,
}

/// Resolve `subnets` against EC2: every subnet must exist, all in one VPC
/// (`expected_vpc`, when the load balancer already has one), one per
/// Availability Zone. Pinned private IPs must sit inside their subnet and
/// Elastic IP allocations must exist (their public address is reported). With
/// no subnets the load balancer is placed in the account's default VPC.
pub fn resolve_subnets(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    subnets: &[SubnetRequest],
    expected_vpc: Option<&str>,
) -> Result<LoadBalancerNetwork, NetworkError> {
    let mut accounts = ec2.write();
    let state = accounts.regional_mut(account_id, region);
    let ids: Vec<String> = subnets.iter().map(|s| s.subnet_id.clone()).collect();
    let resolved =
        vpc_lookup::resolve_subnets_in(state, &ids).map_err(NetworkError::SubnetNotFound)?;
    let vpc_id = match (resolved.first(), expected_vpc) {
        (Some(first), _) => first.vpc_id.clone(),
        (None, Some(vpc)) => vpc.to_string(),
        (None, None) => state
            .vpcs
            .values()
            .find(|v| v.is_default)
            .map(|v| v.vpc_id.clone())
            .unwrap_or_default(),
    };
    if resolved.iter().any(|s| s.vpc_id != vpc_id) {
        return Err(NetworkError::SubnetsInDifferentVpcs);
    }
    if let Some(expected) = expected_vpc {
        if let Some(s) = resolved.iter().find(|s| s.vpc_id != expected) {
            return Err(NetworkError::SubnetNotInVpc {
                subnet_id: s.subnet_id.clone(),
                vpc_id: expected.to_string(),
            });
        }
    }
    let mut seen_azs = std::collections::HashSet::new();
    let mut availability_zones = Vec::with_capacity(resolved.len());
    for (info, req) in resolved.iter().zip(subnets) {
        if !seen_azs.insert(info.availability_zone.clone()) {
            return Err(NetworkError::DuplicateAvailabilityZone(
                info.availability_zone.clone(),
            ));
        }
        if let Some(ip) = &req.private_ipv4_address {
            if !vpc_lookup::ip_in_cidr(ip, &info.cidr_block)
                || vpc_lookup::ip_is_reserved(ip, &info.cidr_block)
            {
                return Err(NetworkError::PrivateIpOutsideSubnet {
                    ip: ip.clone(),
                    subnet_id: info.subnet_id.clone(),
                });
            }
        }
        let public_ip = match &req.allocation_id {
            Some(alloc) => Some(
                state
                    .elastic_ips
                    .get(alloc)
                    .map(|e| e.public_ip.clone())
                    .ok_or_else(|| NetworkError::AllocationIdNotFound(alloc.clone()))?,
            ),
            None => None,
        };
        let has_address = req.allocation_id.is_some()
            || req.private_ipv4_address.is_some()
            || req.ipv6_address.is_some();
        availability_zones.push(AvailabilityZone {
            zone_name: info.availability_zone.clone(),
            subnet_id: info.subnet_id.clone(),
            outpost_id: None,
            load_balancer_addresses: if has_address {
                vec![LoadBalancerAddress {
                    ip_address: public_ip,
                    allocation_id: req.allocation_id.clone(),
                    private_ipv4_address: req.private_ipv4_address.clone(),
                    ipv6_address: req.ipv6_address.clone(),
                    ipv4_prefix: None,
                    ipv6_prefix: None,
                }]
            } else {
                Vec::new()
            },
            source_nat_ipv6_prefixes: req.source_nat_ipv6_prefix.iter().cloned().collect(),
        });
    }
    Ok(LoadBalancerNetwork {
        vpc_id,
        availability_zones,
    })
}

/// Validate a load balancer's security groups against its VPC. An Application
/// Load Balancer given none gets the VPC's `default` security group, as on
/// AWS; Network and Gateway Load Balancers stay without any.
pub fn resolve_security_groups(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    vpc_id: &str,
    lb_type: &str,
    groups: &[String],
) -> Result<Vec<String>, NetworkError> {
    if groups.is_empty() {
        if lb_type != "application" {
            return Ok(Vec::new());
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosted_zone_ids_follow_region_and_type() {
        assert_eq!(
            canonical_hosted_zone_id("us-east-1", "application"),
            "Z35SXDOTRQ7X7K"
        );
        assert_eq!(
            canonical_hosted_zone_id("us-east-1", "network"),
            "Z26RNL4JYFTOTI"
        );
        assert_eq!(
            canonical_hosted_zone_id("eu-west-1", "application"),
            "Z32O12XQLNTSW2"
        );
        assert_eq!(
            canonical_hosted_zone_id("eu-west-1", "network"),
            "Z2IFOLAFXWLO4F"
        );
        assert_eq!(
            canonical_hosted_zone_id("nowhere-1", "application"),
            "Z35SXDOTRQ7X7K"
        );
    }

    #[test]
    fn restored_load_balancers_get_the_regional_zone() {
        use crate::state::{Elbv2Accounts, LoadBalancer};
        let mut accounts = Elbv2Accounts::new();
        let arn = crate::state::load_balancer_arn(
            "eu-west-1",
            "123456789012",
            "network",
            "old",
            "0123456789abcdef",
        );
        accounts
            .get_or_create("123456789012")
            .load_balancers
            .insert(
                arn.clone(),
                LoadBalancer {
                    arn: arn.clone(),
                    name: "old".into(),
                    dns_name: String::new(),
                    canonical_hosted_zone_id: LEGACY_HOSTED_ZONE_ID.into(),
                    created_time: chrono::Utc::now(),
                    scheme: "internal".into(),
                    vpc_id: String::new(),
                    state_code: "active".into(),
                    state_reason: None,
                    lb_type: "network".into(),
                    availability_zones: Vec::new(),
                    security_groups: Vec::new(),
                    ip_address_type: "ipv4".into(),
                    customer_owned_ipv4_pool: None,
                    enforce_security_group_inbound_rules_on_private_link_traffic: None,
                    enable_prefix_for_ipv6_source_nat: None,
                    ipv4_ipam_pool_id: None,
                    tags: Vec::new(),
                    attributes: Default::default(),
                    minimum_capacity_units: None,
                    bound_port: None,
                },
            );
        assert_eq!(restore_canonical_hosted_zone_ids(&mut accounts), 1);
        assert_eq!(
            accounts.get("123456789012").unwrap().load_balancers[&arn].canonical_hosted_zone_id,
            "Z2IFOLAFXWLO4F"
        );
        assert_eq!(restore_canonical_hosted_zone_ids(&mut accounts), 0);
    }
}
