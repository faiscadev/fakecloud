//! VPC lookups for services that place resources in a customer VPC.
//!
//! Load balancers, DB subnet groups, EFS mount targets, brokers, EKS clusters
//! and OpenSearch domains all live in subnets the caller created through EC2.
//! On AWS their `VpcId` / `AvailabilityZone` come from those subnets, the
//! security groups they reference must exist, and some of them (EFS mount
//! targets) create a requester-managed network interface in the subnet. These
//! helpers give those services one place to resolve that against the real EC2
//! state instead of inventing ids EC2 has never heard of.

use std::net::Ipv4Addr;

use crate::state::{Ec2State, NetworkInterface, Subnet};
use crate::SharedEc2State;

/// Run `f` against `account_id`'s EC2 state. An account EC2 has not seen yet
/// already owns its default VPC, subnets and security group on AWS, so it is
/// created (with those defaults) the way EC2's own calls create it. Takes the
/// shared read lock when the account exists and the write lock only to create
/// it, so cross-service readers do not serialize against EC2 traffic.
pub fn with_account<R>(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    f: impl FnOnce(&Ec2State) -> R,
) -> R {
    {
        let accounts = ec2.read();
        if let Some(state) = accounts.regional(account_id, region) {
            return f(state);
        }
    }
    let mut accounts = ec2.write();
    f(accounts.regional_mut(account_id, region))
}

/// The facts about a subnet other services report back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubnetInfo {
    pub subnet_id: String,
    pub vpc_id: String,
    pub cidr_block: String,
    pub availability_zone: String,
    pub availability_zone_id: String,
}

impl From<&Subnet> for SubnetInfo {
    fn from(s: &Subnet) -> Self {
        Self {
            subnet_id: s.subnet_id.clone(),
            vpc_id: s.vpc_id.clone(),
            cidr_block: s.cidr_block.clone(),
            availability_zone: s.availability_zone.clone(),
            availability_zone_id: s.availability_zone_id.clone(),
        }
    }
}

/// Resolve every subnet in `subnet_ids` (in order). `Err` carries the first id
/// EC2 does not know, for the caller's service-specific not-found error.
pub fn resolve_subnets(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    subnet_ids: &[String],
) -> Result<Vec<SubnetInfo>, String> {
    with_account(ec2, account_id, region, |state| {
        resolve_subnets_in(state, subnet_ids)
    })
}

/// [`resolve_subnets`] against an already-borrowed account state.
pub fn resolve_subnets_in(
    state: &Ec2State,
    subnet_ids: &[String],
) -> Result<Vec<SubnetInfo>, String> {
    subnet_ids
        .iter()
        .map(|id| {
            state
                .subnets
                .get(id)
                .map(SubnetInfo::from)
                .ok_or_else(|| id.clone())
        })
        .collect()
}

/// Why a subnet group's subnets were rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubnetGroupError {
    /// These subnet ids do not exist.
    NotFound(Vec<String>),
    /// The subnets span more than one VPC.
    DifferentVpcs(Vec<String>),
}

impl SubnetGroupError {
    /// The `InvalidSubnet` message the database services return.
    pub fn message(&self) -> String {
        match self {
            Self::NotFound(ids) => {
                format!("Some input subnets in :[{}] are invalid.", ids.join(", "))
            }
            Self::DifferentVpcs(ids) => format!(
                "The subnets [{}] are not all in the same VPC.",
                ids.join(", ")
            ),
        }
    }
}

/// A subnet group's subnets, all resolved and in one VPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubnetGroupPlacement {
    pub vpc_id: String,
    pub subnets: Vec<SubnetInfo>,
}

impl SubnetGroupPlacement {
    /// The Availability Zone of each subnet, in request order.
    pub fn availability_zones(&self) -> Vec<String> {
        self.subnets
            .iter()
            .map(|s| s.availability_zone.clone())
            .collect()
    }

    /// How many distinct Availability Zones the subnets cover.
    pub fn distinct_availability_zones(&self) -> usize {
        self.subnets
            .iter()
            .map(|s| s.availability_zone.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len()
    }
}

/// Resolve the subnets of a DB / cache / cluster subnet group: every subnet
/// must exist and all of them must be in the same VPC, which becomes the
/// group's `VpcId`.
pub fn resolve_subnet_group(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    subnet_ids: &[String],
) -> Result<SubnetGroupPlacement, SubnetGroupError> {
    with_account(ec2, account_id, region, |state| {
        let missing: Vec<String> = subnet_ids
            .iter()
            .filter(|id| !state.subnets.contains_key(*id))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Err(SubnetGroupError::NotFound(missing));
        }
        let subnets: Vec<SubnetInfo> = subnet_ids
            .iter()
            .map(|id| SubnetInfo::from(&state.subnets[id]))
            .collect();
        let vpc_id = subnets
            .first()
            .map(|s| s.vpc_id.clone())
            .unwrap_or_default();
        if subnets.iter().any(|s| s.vpc_id != vpc_id) {
            return Err(SubnetGroupError::DifferentVpcs(subnet_ids.to_vec()));
        }
        Ok(SubnetGroupPlacement { vpc_id, subnets })
    })
}

/// The `InvalidSubnet` message the database services return when a subnet
/// group modification names subnets outside the group's VPC.
pub const SUBNET_GROUP_VPC_CHANGE_MESSAGE: &str =
    "The new Subnets are not in the same Vpc as the existing subnet group";

/// Whether a subnet group in `existing_vpc` may take subnets in `new_vpc`. A
/// group's VPC is fixed once created, so only the same VPC is allowed. A group
/// persisted before VPCs were resolved (empty or invented VPC id EC2 does not
/// know) is re-resolved instead.
pub fn subnet_group_vpc_change_allowed(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    existing_vpc: &str,
    new_vpc: &str,
) -> bool {
    if existing_vpc.is_empty() || existing_vpc == new_vpc {
        return true;
    }
    with_account(ec2, account_id, region, |state| {
        !state.vpcs.contains_key(existing_vpc)
    })
}

/// [`default_vpc_id`] against an already-borrowed account state.
pub fn default_vpc_id_in(state: &Ec2State) -> Option<String> {
    state
        .vpcs
        .values()
        .find(|v| v.is_default)
        .map(|v| v.vpc_id.clone())
}

/// The default VPC of the account, when it still exists.
pub fn default_vpc_id(ec2: &SharedEc2State, account_id: &str, region: &str) -> Option<String> {
    with_account(ec2, account_id, region, default_vpc_id_in)
}

/// The default VPC's default subnets (one per Availability Zone), ordered by
/// zone. This is where AWS places resources that take no subnets.
pub fn default_vpc_subnets(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
) -> Vec<SubnetInfo> {
    with_account(ec2, account_id, region, |state| {
        let Some(vpc_id) = default_vpc_id_in(state) else {
            return Vec::new();
        };
        let mut subnets: Vec<SubnetInfo> = state
            .subnets
            .values()
            .filter(|s| s.vpc_id == vpc_id && s.default_for_az)
            .map(SubnetInfo::from)
            .collect();
        subnets.sort_by(|a, b| a.availability_zone.cmp(&b.availability_zone));
        subnets
    })
}

/// The id of `vpc_id`'s `default` security group.
pub fn default_security_group_id(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    vpc_id: &str,
) -> Option<String> {
    with_account(ec2, account_id, region, |state| {
        default_security_group_id_in(state, vpc_id)
    })
}

/// [`default_security_group_id`] against an already-borrowed account state.
pub fn default_security_group_id_in(state: &Ec2State, vpc_id: &str) -> Option<String> {
    state
        .security_groups
        .values()
        .find(|g| g.vpc_id == vpc_id && g.group_name == "default")
        .map(|g| g.group_id.clone())
}

/// Resolve each security group id to the VPC it belongs to (in order). `Err`
/// carries the first id EC2 does not know.
pub fn security_group_vpcs(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    group_ids: &[String],
) -> Result<Vec<(String, String)>, String> {
    with_account(ec2, account_id, region, |state| {
        security_group_vpcs_in(state, group_ids)
    })
}

/// [`security_group_vpcs`] against an already-borrowed account state.
pub fn security_group_vpcs_in(
    state: &Ec2State,
    group_ids: &[String],
) -> Result<Vec<(String, String)>, String> {
    group_ids
        .iter()
        .map(|id| {
            state
                .security_groups
                .get(id)
                .map(|g| (id.clone(), g.vpc_id.clone()))
                .ok_or_else(|| id.clone())
        })
        .collect()
}

/// Parse an IPv4 CIDR into `(network, prefix_len)`.
fn parse_ipv4_cidr(cidr: &str) -> Option<(u32, u32)> {
    let (addr, len) = cidr.split_once('/')?;
    let addr: Ipv4Addr = addr.parse().ok()?;
    let len: u32 = len.parse().ok()?;
    if len > 32 {
        return None;
    }
    let mask = if len == 0 { 0 } else { u32::MAX << (32 - len) };
    Some((u32::from(addr) & mask, len))
}

/// Whether two IPv4 CIDR blocks share any address (one contains or equals
/// the other). `false` when either does not parse.
pub fn ipv4_cidrs_overlap(a: &str, b: &str) -> bool {
    let (Some((na, la)), Some((nb, lb))) = (parse_ipv4_cidr(a), parse_ipv4_cidr(b)) else {
        return false;
    };
    // The shorter prefix decides: both networks agree on its bits.
    let len = la.min(lb);
    let mask = if len == 0 { 0 } else { u32::MAX << (32 - len) };
    na & mask == nb & mask
}

/// Whether `ip` is an address inside `cidr`.
pub fn ip_in_cidr(ip: &str, cidr: &str) -> bool {
    let (Some((net, len)), Ok(addr)) = (parse_ipv4_cidr(cidr), ip.parse::<Ipv4Addr>()) else {
        return false;
    };
    let mask = if len == 0 { 0 } else { u32::MAX << (32 - len) };
    u32::from(addr) & mask == net
}

/// Whether `ip` is one of the five addresses AWS reserves in every subnet: the
/// network address, the next three (VPC router, DNS, future use) and the
/// broadcast address.
pub fn ip_is_reserved(ip: &str, cidr: &str) -> bool {
    let (Some((net, len)), Ok(addr)) = (parse_ipv4_cidr(cidr), ip.parse::<Ipv4Addr>()) else {
        return false;
    };
    let size: u64 = 1u64 << (32 - len);
    let offset = u64::from(u32::from(addr).wrapping_sub(net));
    offset < 4 || offset == size - 1
}

/// Every private IPv4 address already in use in `subnet_id`: network
/// interfaces (primary and secondary) and instances.
fn used_ips(state: &Ec2State, subnet_id: &str) -> std::collections::HashSet<String> {
    let mut used = std::collections::HashSet::new();
    for eni in state
        .network_interfaces
        .values()
        .filter(|e| e.subnet_id == subnet_id)
    {
        used.insert(eni.private_ip_address.clone());
        used.extend(eni.private_ips.iter().cloned());
    }
    for inst in state
        .instances
        .values()
        .filter(|i| i.subnet_id.as_deref() == Some(subnet_id))
    {
        used.insert(inst.private_ip.clone());
    }
    used
}

/// The next free private IPv4 address in the subnet. AWS reserves the first
/// four addresses and the last address of every subnet CIDR, so allocation
/// starts at `.4` of the block and never hands out the broadcast address.
pub fn allocate_private_ip(state: &Ec2State, subnet: &SubnetInfo) -> Option<String> {
    let (net, len) = parse_ipv4_cidr(&subnet.cidr_block)?;
    let size: u64 = 1u64 << (32 - len);
    if size < 8 {
        return None;
    }
    let used = used_ips(state, &subnet.subnet_id);
    (4..size - 1)
        .map(|offset| Ipv4Addr::from(net + offset as u32).to_string())
        .find(|ip| !used.contains(ip))
}

/// Why a service-managed network interface could not be created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EniError {
    /// The subnet does not exist.
    SubnetNotFound(String),
    /// The requested private IP is outside the subnet CIDR.
    IpOutsideSubnet(String),
    /// The requested private IP is already assigned in the subnet.
    IpInUse(String),
    /// The requested private IP is one AWS reserves in every subnet.
    IpReserved(String),
    /// The subnet has no free addresses left.
    NoFreeAddresses(String),
}

/// What a service asks EC2 for when it places a network interface in a
/// customer subnet on its own behalf (EFS mount targets, for example).
#[derive(Debug, Clone)]
pub struct ServiceEni<'a> {
    pub subnet_id: &'a str,
    pub group_ids: Vec<String>,
    pub description: String,
    /// A caller-pinned private IP; `None` allocates the next free address.
    pub private_ip: Option<&'a str>,
}

/// Create a requester-managed network interface in the subnet and return its
/// id and primary private IP. The interface is `in-use` (owned by the
/// requesting service) and carries the subnet's VPC and Availability Zone.
pub fn create_service_eni(
    state: &mut Ec2State,
    spec: ServiceEni<'_>,
) -> Result<(String, String), EniError> {
    let subnet = state
        .subnets
        .get(spec.subnet_id)
        .map(SubnetInfo::from)
        .ok_or_else(|| EniError::SubnetNotFound(spec.subnet_id.to_string()))?;
    let ip = match spec.private_ip {
        Some(ip) => {
            if !ip_in_cidr(ip, &subnet.cidr_block) {
                return Err(EniError::IpOutsideSubnet(ip.to_string()));
            }
            if ip_is_reserved(ip, &subnet.cidr_block) {
                return Err(EniError::IpReserved(ip.to_string()));
            }
            if used_ips(state, &subnet.subnet_id).contains(ip) {
                return Err(EniError::IpInUse(ip.to_string()));
            }
            ip.to_string()
        }
        None => allocate_private_ip(state, &subnet)
            .ok_or_else(|| EniError::NoFreeAddresses(subnet.subnet_id.clone()))?,
    };
    let id = crate::service_helpers::gen_id("eni");
    let hex = uuid::Uuid::new_v4().simple().to_string();
    let mac = format!(
        "02:{}:{}:{}:{}:{}",
        &hex[0..2],
        &hex[2..4],
        &hex[4..6],
        &hex[6..8],
        &hex[8..10]
    );
    state.network_interfaces.insert(
        id.clone(),
        NetworkInterface {
            network_interface_id: id.clone(),
            subnet_id: subnet.subnet_id.clone(),
            vpc_id: subnet.vpc_id.clone(),
            availability_zone: subnet.availability_zone.clone(),
            description: spec.description,
            mac_address: mac,
            private_ip_address: ip.clone(),
            status: "in-use".to_string(),
            interface_type: "interface".to_string(),
            source_dest_check: true,
            group_ids: spec.group_ids,
            private_ips: Vec::new(),
            ipv6_addresses: Vec::new(),
            attachment: None,
            public_ip_dns_hostname_type: None,
            requester_managed: true,
        },
    );
    Ok((id, ip))
}

/// Delete a network interface a service created with [`create_service_eni`].
pub fn delete_service_eni(state: &mut Ec2State, eni_id: &str) {
    if state
        .network_interfaces
        .get(eni_id)
        .is_some_and(|e| e.requester_managed)
    {
        state.network_interfaces.remove(eni_id);
        state.tags.remove(eni_id);
    }
}

/// One network interface of a service-managed interface endpoint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EndpointInterface {
    pub network_interface_id: String,
    pub subnet_id: String,
    pub private_ip_address: String,
    pub availability_zone: String,
}

/// A service-managed interface VPC endpoint (Redshift-managed endpoints, for
/// example): a real `vpce-` record in EC2 with one requester-managed network
/// interface per subnet.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ServiceEndpoint {
    pub vpc_endpoint_id: String,
    pub vpc_id: String,
    pub interfaces: Vec<EndpointInterface>,
}

/// Create an interface VPC endpoint for `service_name` in `subnets` (all in
/// `vpc_id`), with one network interface per subnet carrying `group_ids`.
pub fn create_service_endpoint(
    state: &mut Ec2State,
    vpc_id: &str,
    service_name: &str,
    subnets: &[SubnetInfo],
    group_ids: &[String],
) -> Result<ServiceEndpoint, EniError> {
    let vpc_endpoint_id = crate::service_helpers::gen_id("vpce");
    let mut interfaces = Vec::with_capacity(subnets.len());
    for subnet in subnets {
        let created = create_service_eni(
            state,
            ServiceEni {
                subnet_id: &subnet.subnet_id,
                group_ids: group_ids.to_vec(),
                description: format!("VPC Endpoint Interface {vpc_endpoint_id}"),
                private_ip: None,
            },
        );
        let (eni_id, ip) = match created {
            Ok(v) => v,
            Err(e) => {
                for i in &interfaces {
                    let i: &EndpointInterface = i;
                    delete_service_eni(state, &i.network_interface_id);
                }
                return Err(e);
            }
        };
        interfaces.push(EndpointInterface {
            network_interface_id: eni_id,
            subnet_id: subnet.subnet_id.clone(),
            private_ip_address: ip,
            availability_zone: subnet.availability_zone.clone(),
        });
    }
    state.vpc_endpoints.insert(
        vpc_endpoint_id.clone(),
        crate::state::VpcEndpoint {
            id: vpc_endpoint_id.clone(),
            endpoint_type: "Interface".to_string(),
            vpc_id: vpc_id.to_string(),
            service_name: service_name.to_string(),
            state: "available".to_string(),
            subnet_ids: subnets.iter().map(|s| s.subnet_id.clone()).collect(),
            route_table_ids: Vec::new(),
            private_dns_enabled: false,
            security_group_ids: group_ids.to_vec(),
            payer_responsibility: "vpc-endpoint-account".to_string(),
        },
    );
    Ok(ServiceEndpoint {
        vpc_endpoint_id,
        vpc_id: vpc_id.to_string(),
        interfaces,
    })
}

/// The account owning VPC `vpc_id` in `region`. VPCs are regional: a VPC
/// id only names a VPC in the region it was created in, so the lookup is
/// confined to that region's state of every account. An empty `region` (a
/// caller that named none) matches the VPC in any region.
pub fn vpc_owner_account(ec2: &SharedEc2State, vpc_id: &str, region: &str) -> Option<String> {
    ec2.read()
        .iter_regional()
        .find(|(_, r, st)| (region.is_empty() || *r == region) && st.vpcs.contains_key(vpc_id))
        .map(|(account, _, _)| account.to_string())
}

/// Delete an endpoint created with [`create_service_endpoint`] and its
/// network interfaces.
pub fn delete_service_endpoint(state: &mut Ec2State, endpoint: &ServiceEndpoint) {
    for i in &endpoint.interfaces {
        delete_service_eni(state, &i.network_interface_id);
    }
    state.vpc_endpoints.remove(&endpoint.vpc_endpoint_id);
    state.tags.remove(&endpoint.vpc_endpoint_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiRegionState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    const ACCOUNT: &str = "111122223333";

    fn ec2() -> SharedEc2State {
        Arc::new(RwLock::new(MultiRegionState::new(ACCOUNT, "us-east-1", "")))
    }

    #[test]
    fn ipv4_cidrs_overlap_on_containment_and_equality() {
        assert!(ipv4_cidrs_overlap("10.0.0.0/16", "10.0.0.0/16"));
        assert!(ipv4_cidrs_overlap("10.0.0.0/16", "10.0.128.0/24"));
        assert!(ipv4_cidrs_overlap("10.0.128.0/24", "10.0.0.0/16"));
        assert!(!ipv4_cidrs_overlap("10.0.0.0/16", "10.1.0.0/16"));
        assert!(!ipv4_cidrs_overlap("10.0.0.0/24", "10.0.1.0/24"));
        assert!(!ipv4_cidrs_overlap("garbage", "10.0.0.0/16"));
    }

    #[test]
    fn vpc_owner_account_is_confined_to_the_given_region() {
        let ec2 = ec2();
        let west_vpc = with_account(&ec2, "444455556666", "eu-west-1", default_vpc_id_in)
            .expect("default VPC");
        assert_eq!(
            vpc_owner_account(&ec2, &west_vpc, "eu-west-1").as_deref(),
            Some("444455556666")
        );
        // The same id names no VPC in another region.
        assert_eq!(vpc_owner_account(&ec2, &west_vpc, "us-east-1"), None);
        assert_eq!(vpc_owner_account(&ec2, "vpc-missing", "eu-west-1"), None);
    }

    #[test]
    fn with_account_creates_an_unseen_account_with_its_default_vpc() {
        let ec2 = ec2();
        assert!(ec2.read().regional("444455556666", "us-east-1").is_none());
        let vpc = with_account(&ec2, "444455556666", "us-east-1", default_vpc_id_in)
            .expect("default VPC");
        let sg = with_account(&ec2, "444455556666", "us-east-1", |s| {
            default_security_group_id_in(s, &vpc)
        });
        assert!(sg.is_some());
        assert!(ec2.read().regional("444455556666", "us-east-1").is_some());
    }

    #[test]
    fn resolves_default_subnets_and_reports_unknown_ids() {
        let ec2 = ec2();
        let defaults = default_vpc_subnets(&ec2, ACCOUNT, "us-east-1");
        assert_eq!(defaults.len(), 3);
        let vpc = default_vpc_id(&ec2, ACCOUNT, "us-east-1").unwrap();
        assert!(defaults.iter().all(|s| s.vpc_id == vpc));
        let ids: Vec<String> = defaults.iter().map(|s| s.subnet_id.clone()).collect();
        assert_eq!(
            resolve_subnets(&ec2, ACCOUNT, "us-east-1", &ids).unwrap(),
            defaults
        );
        let mut bad = ids.clone();
        bad.push("subnet-0000dead".into());
        assert_eq!(
            resolve_subnets(&ec2, ACCOUNT, "us-east-1", &bad).unwrap_err(),
            "subnet-0000dead"
        );
        let sg = default_security_group_id(&ec2, ACCOUNT, "us-east-1", &vpc).unwrap();
        assert_eq!(
            security_group_vpcs(&ec2, ACCOUNT, "us-east-1", std::slice::from_ref(&sg)).unwrap(),
            vec![(sg, vpc)]
        );
        assert_eq!(
            security_group_vpcs(&ec2, ACCOUNT, "us-east-1", &["sg-nope".to_string()]).unwrap_err(),
            "sg-nope"
        );
    }

    #[test]
    fn subnet_group_requires_existing_subnets_in_one_vpc() {
        let ec2 = ec2();
        let defaults = default_vpc_subnets(&ec2, ACCOUNT, "us-east-1");
        let ids: Vec<String> = defaults.iter().map(|s| s.subnet_id.clone()).collect();
        let placed = resolve_subnet_group(&ec2, ACCOUNT, "us-east-1", &ids).unwrap();
        assert_eq!(placed.vpc_id, defaults[0].vpc_id);
        assert_eq!(placed.distinct_availability_zones(), 3);
        assert_eq!(
            resolve_subnet_group(
                &ec2,
                ACCOUNT,
                "us-east-1",
                &["subnet-x".into(), ids[0].clone()]
            )
            .unwrap_err(),
            SubnetGroupError::NotFound(vec!["subnet-x".into()])
        );
        // A subnet in another VPC.
        {
            let mut accounts = ec2.write();
            let state = accounts.regional_mut(ACCOUNT, "us-east-1");
            let mut other = state.subnets[&ids[0]].clone();
            other.subnet_id = "subnet-other".into();
            other.vpc_id = "vpc-other".into();
            state.subnets.insert(other.subnet_id.clone(), other);
        }
        assert!(matches!(
            resolve_subnet_group(
                &ec2,
                ACCOUNT,
                "us-east-1",
                &[ids[0].clone(), "subnet-other".into()]
            ),
            Err(SubnetGroupError::DifferentVpcs(_))
        ));
    }

    #[test]
    fn cidr_math() {
        for reserved in ["172.31.16.0", "172.31.16.1", "172.31.16.3", "172.31.31.255"] {
            assert!(ip_is_reserved(reserved, "172.31.16.0/20"), "{reserved}");
        }
        assert!(!ip_is_reserved("172.31.16.4", "172.31.16.0/20"));
        assert!(!ip_is_reserved("172.31.31.254", "172.31.16.0/20"));
        assert!(ip_in_cidr("172.31.16.9", "172.31.16.0/20"));
        assert!(!ip_in_cidr("172.31.32.9", "172.31.16.0/20"));
        assert!(!ip_in_cidr("garbage", "172.31.16.0/20"));
    }

    #[test]
    fn service_eni_allocates_distinct_ips_inside_the_subnet() {
        let ec2 = ec2();
        let subnet = default_vpc_subnets(&ec2, ACCOUNT, "us-east-1").remove(0);
        let mut accounts = ec2.write();
        let state = accounts.regional_mut(ACCOUNT, "us-east-1");
        let spec = |ip| ServiceEni {
            subnet_id: &subnet.subnet_id,
            group_ids: vec!["sg-1".into()],
            description: "svc".into(),
            private_ip: ip,
        };
        let (a, ip_a) = create_service_eni(state, spec(None)).unwrap();
        let (_, ip_b) = create_service_eni(state, spec(None)).unwrap();
        assert_ne!(ip_a, ip_b);
        assert!(ip_in_cidr(&ip_a, &subnet.cidr_block));
        // First four addresses of the block are reserved.
        assert_eq!(ip_a, "172.31.0.4");
        let eni = &state.network_interfaces[&a];
        assert_eq!(eni.vpc_id, subnet.vpc_id);
        assert_eq!(eni.availability_zone, subnet.availability_zone);
        assert!(eni.requester_managed);
        assert_eq!(
            create_service_eni(state, spec(Some(&ip_a))).unwrap_err(),
            EniError::IpInUse(ip_a.clone())
        );
        assert_eq!(
            create_service_eni(state, spec(Some("172.31.0.1"))).unwrap_err(),
            EniError::IpReserved("172.31.0.1".into())
        );
        assert_eq!(
            create_service_eni(state, spec(Some("10.9.9.9"))).unwrap_err(),
            EniError::IpOutsideSubnet("10.9.9.9".into())
        );
        delete_service_eni(state, &a);
        assert!(!state.network_interfaces.contains_key(&a));
    }
}
