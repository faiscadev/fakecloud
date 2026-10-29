//! Count-based EC2 and VPC quotas: what each one counts, in which scope, and
//! the error AWS returns once a request would go past it.
//!
//! Every counter here is shared by the enforcing handler and by
//! [`super::quota::Ec2QuotaUsage`], so a utilization report shows exactly the
//! number the handler checks. A quota scoped to a resource (per VPC, per
//! Availability Zone, per route table, per network ACL) reports the busiest
//! resource as its usage, the one closest to the limit.
//!
//! Enforcement is opt-in, as for every quota: handlers resolve the limit with
//! [`Ec2Service::enforced_count_quota`] before taking the EC2 state lock (the
//! Service Quotas provider answers it), then count and check under the lock
//! that inserts the resource, so two concurrent creates cannot both slip in.

use std::collections::BTreeMap;

use fakecloud_core::quota::VPC_SERVICE_CODE as VPC;
use fakecloud_core::service::AwsServiceError;

use crate::service::Ec2Service;
use crate::state::{Ec2State, Instance, ManagedPrefixList, NetworkAcl, Route, RouteTable, Vpc};

/// A quota by service code and quota code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CountQuota {
    pub(crate) service: &'static str,
    pub(crate) code: &'static str,
}

const fn vpc(code: &'static str) -> CountQuota {
    CountQuota { service: VPC, code }
}

const fn ec2(code: &'static str) -> CountQuota {
    CountQuota {
        service: "ec2",
        code,
    }
}

pub(crate) const VPCS_PER_REGION: CountQuota = vpc("L-F678F1CE");
pub(crate) const INTERNET_GATEWAYS_PER_REGION: CountQuota = vpc("L-A4707A72");
pub(crate) const SUBNETS_PER_VPC: CountQuota = vpc("L-407747CB");
pub(crate) const SECURITY_GROUPS_PER_REGION: CountQuota = vpc("L-E79EC296");
pub(crate) const ROUTE_TABLES_PER_VPC: CountQuota = vpc("L-589F43AA");
pub(crate) const ROUTES_PER_ROUTE_TABLE: CountQuota = vpc("L-93826ACB");
pub(crate) const NETWORK_ACLS_PER_VPC: CountQuota = vpc("L-B4A6D682");
pub(crate) const RULES_PER_NETWORK_ACL: CountQuota = vpc("L-2AEEBF1A");
pub(crate) const NAT_GATEWAYS_PER_AZ: CountQuota = vpc("L-FE5A380F");
pub(crate) const NETWORK_INTERFACES_PER_REGION: CountQuota = vpc("L-DF5E4CA3");
pub(crate) const IPV4_CIDR_BLOCKS_PER_VPC: CountQuota = vpc("L-83CA0A9D");
pub(crate) const ACTIVE_PEERINGS_PER_VPC: CountQuota = vpc("L-7E9ECCDB");
pub(crate) const OUTSTANDING_PEERING_REQUESTS: CountQuota = vpc("L-DC9F7029");
pub(crate) const GATEWAY_ENDPOINTS_PER_REGION: CountQuota = vpc("L-1B52E74A");
pub(crate) const INTERFACE_ENDPOINTS_PER_VPC: CountQuota = vpc("L-29B6F2EB");
pub(crate) const ELASTIC_IPS: CountQuota = ec2("L-0263D0A3");
pub(crate) const VPN_CONNECTIONS_PER_REGION: CountQuota = ec2("L-3E6EC3A3");

/// The On-Demand vCPU quota of the Standard (A, C, D, H, I, M, R, T, Z)
/// families.
pub(crate) const ON_DEMAND_STANDARD: CountQuota = ec2("L-1216C47A");
pub(crate) const ON_DEMAND_F: CountQuota = ec2("L-74FC7D96");
pub(crate) const ON_DEMAND_G_VT: CountQuota = ec2("L-DB2E81BA");
pub(crate) const ON_DEMAND_INF: CountQuota = ec2("L-1945791B");
pub(crate) const ON_DEMAND_P: CountQuota = ec2("L-417A185B");
pub(crate) const ON_DEMAND_X: CountQuota = ec2("L-7295265B");
pub(crate) const ON_DEMAND_HIGH_MEMORY: CountQuota = ec2("L-43DA4232");
/// "All Standard (A, C, D, H, I, M, R, T, Z) Spot Instance Requests", in
/// vCPUs.
pub(crate) const SPOT_STANDARD: CountQuota = ec2("L-34B43A08");

/// Counted for utilization but not enforced: AWS documents no error code for
/// egress-only internet gateways or transit gateways, and fakecloud keeps one
/// IPv6 block per VPC.
pub(crate) const EGRESS_ONLY_IGWS_PER_REGION: CountQuota = vpc("L-45FE3B85");
pub(crate) const IPV6_CIDR_BLOCKS_PER_VPC: CountQuota = vpc("L-085A6257");
pub(crate) const TRANSIT_GATEWAYS: CountQuota = ec2("L-A2478D36");

/// Every quota counted here.
const ALL: &[CountQuota] = &[
    VPCS_PER_REGION,
    INTERNET_GATEWAYS_PER_REGION,
    SUBNETS_PER_VPC,
    SECURITY_GROUPS_PER_REGION,
    ROUTE_TABLES_PER_VPC,
    ROUTES_PER_ROUTE_TABLE,
    NETWORK_ACLS_PER_VPC,
    RULES_PER_NETWORK_ACL,
    NAT_GATEWAYS_PER_AZ,
    NETWORK_INTERFACES_PER_REGION,
    IPV4_CIDR_BLOCKS_PER_VPC,
    ACTIVE_PEERINGS_PER_VPC,
    OUTSTANDING_PEERING_REQUESTS,
    GATEWAY_ENDPOINTS_PER_REGION,
    INTERFACE_ENDPOINTS_PER_VPC,
    ELASTIC_IPS,
    VPN_CONNECTIONS_PER_REGION,
    ON_DEMAND_STANDARD,
    ON_DEMAND_F,
    ON_DEMAND_G_VT,
    ON_DEMAND_INF,
    ON_DEMAND_P,
    ON_DEMAND_X,
    ON_DEMAND_HIGH_MEMORY,
    SPOT_STANDARD,
    EGRESS_ONLY_IGWS_PER_REGION,
    IPV6_CIDR_BLOCKS_PER_VPC,
    TRANSIT_GATEWAYS,
];

/// The quota counted here for `service_code`/`quota_code`.
pub(crate) fn by_code(service_code: &str, quota_code: &str) -> Option<CountQuota> {
    ALL.iter()
        .copied()
        .find(|q| q.service == service_code && q.code == quota_code)
}

/// Every On-Demand vCPU quota, for usage reporting.
pub(crate) const ON_DEMAND_VCPU_QUOTAS: &[CountQuota] = &[
    ON_DEMAND_STANDARD,
    ON_DEMAND_F,
    ON_DEMAND_G_VT,
    ON_DEMAND_INF,
    ON_DEMAND_P,
    ON_DEMAND_X,
    ON_DEMAND_HIGH_MEMORY,
];

impl Ec2Service {
    /// The limit of `quota` when it is enforced for this account, `None`
    /// while it is not. Call before taking the EC2 state lock.
    pub(crate) fn enforced_count_quota(
        &self,
        account_id: &str,
        region: &str,
        quota: CountQuota,
    ) -> Option<usize> {
        self.enforced_quota(account_id, region, quota.service, quota.code)
    }
}

fn limit_exceeded(code: &str, message: String) -> AwsServiceError {
    AwsServiceError::aws_error(http::StatusCode::BAD_REQUEST, code, message)
}

/// `code` with `message(limit)` when adding `adding` to `current` would go
/// past `limit`. A `None` limit (the quota is not enforced) accepts anything,
/// and so does a request that adds nothing.
pub(crate) fn check(
    limit: Option<usize>,
    current: usize,
    adding: usize,
    code: &str,
    message: impl FnOnce(usize) -> String,
) -> Result<(), AwsServiceError> {
    match limit {
        Some(limit) if adding > 0 && current.saturating_add(adding) > limit => {
            Err(limit_exceeded(code, message(limit)))
        }
        _ => Ok(()),
    }
}

/// The largest per-resource count, the usage of a quota scoped to a resource.
fn busiest(counts: impl Iterator<Item = usize>) -> usize {
    counts.max().unwrap_or(0)
}

/// Counts per key, for quotas scoped to a VPC or an Availability Zone.
fn tally<'a>(keys: impl Iterator<Item = &'a str>) -> BTreeMap<&'a str, usize> {
    let mut out = BTreeMap::new();
    for k in keys {
        *out.entry(k).or_insert(0) += 1;
    }
    out
}

// ---- VPCs, gateways, subnets, security groups ----

pub(crate) fn vpcs(state: &Ec2State) -> usize {
    state.vpcs.len()
}

pub(crate) fn internet_gateways(state: &Ec2State) -> usize {
    state.internet_gateways.len()
}

pub(crate) fn subnets_in_vpc(state: &Ec2State, vpc_id: &str) -> usize {
    state
        .subnets
        .values()
        .filter(|s| s.vpc_id == vpc_id)
        .count()
}

/// Every security group counts, the default group of each VPC included.
pub(crate) fn security_groups(state: &Ec2State) -> usize {
    state.security_groups.len()
}

// ---- route tables ----

/// Route tables of a VPC; the main route table counts, as on AWS.
pub(crate) fn route_tables_in_vpc(state: &Ec2State, vpc_id: &str) -> usize {
    state
        .route_tables
        .values()
        .filter(|rt| rt.vpc_id == vpc_id)
        .count()
}

/// What one route weighs against "Routes per route table", as `[IPv4,
/// IPv6]`: AWS enforces the quota separately for each family.
///
/// The quota covers non-propagated routes; fakecloud does not propagate
/// routes into VPC route tables, so every route counts except the table's
/// implicit `local` route, which is part of the table rather than a route
/// added to it. A route whose destination is a prefix list weighs the list's
/// maximum entries (a customer-managed list) or published weight (an
/// AWS-managed list), as AWS counts it, toward that list's family.
pub(crate) fn route_weight(
    r: &Route,
    prefix_lists: &BTreeMap<String, ManagedPrefixList>,
    region: &str,
) -> [usize; 2] {
    // The local route, also once ReplaceRoute retargeted it (it keeps the
    // `CreateRouteTable` origin).
    if r.gateway_id.as_deref() == Some("local") || r.origin == "CreateRouteTable" {
        return [0, 0];
    }
    if let Some(id) = &r.destination_prefix_list_id {
        let (ipv6, weight) = match prefix_lists.get(id) {
            Some(l) => (
                l.address_family.eq_ignore_ascii_case("IPv6"),
                l.max_entries.max(1) as usize,
            ),
            None => match super::aws_prefix_lists::by_id(region, id) {
                Some(l) => (l.address_family.eq_ignore_ascii_case("IPv6"), l.weight),
                None => (false, 1),
            },
        };
        return if ipv6 { [0, weight] } else { [weight, 0] };
    }
    if r.destination_ipv6_cidr_block.is_some() {
        [0, 1]
    } else {
        [1, 0]
    }
}

/// The routes of a table that count toward "Routes per route table", as
/// `[IPv4, IPv6]` (see [`route_weight`]).
pub(crate) fn route_counts(
    rt: &RouteTable,
    prefix_lists: &BTreeMap<String, ManagedPrefixList>,
    region: &str,
) -> [usize; 2] {
    rt.routes.iter().fold([0, 0], |[v4, v6], r| {
        let [w4, w6] = route_weight(r, prefix_lists, region);
        [v4.saturating_add(w4), v6.saturating_add(w6)]
    })
}

/// `RouteLimitExceeded` when adding a route weighing `adding` to a table
/// holding `current` grows either family past `limit`. A family the route
/// does not count toward is left alone, even when already over the limit.
pub(crate) fn check_routes(
    limit: Option<usize>,
    current: [usize; 2],
    adding: [usize; 2],
) -> Result<(), AwsServiceError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    if (0..2).any(|i| adding[i] > 0 && current[i].saturating_add(adding[i]) > limit) {
        return Err(limit_exceeded(
            "RouteLimitExceeded",
            "The maximum number of routes has been reached.".to_string(),
        ));
    }
    Ok(())
}

// ---- network ACLs ----

pub(crate) fn network_acls_in_vpc(state: &Ec2State, vpc_id: &str) -> usize {
    state
        .network_acls
        .values()
        .filter(|a| a.vpc_id == vpc_id)
        .count()
}

/// Rules of one direction of a network ACL. AWS counts inbound and outbound
/// rules separately, and the default deny rule (`*`, rule number 32767) every
/// ACL carries is not one of the rules the quota limits.
pub(crate) fn nacl_rules(acl: &NetworkAcl, egress: bool) -> usize {
    acl.entries
        .iter()
        .filter(|e| e.egress == egress && e.rule_number < 32767)
        .count()
}

// ---- NAT gateways, network interfaces ----

/// The Availability Zone of a subnet, when fakecloud knows the subnet.
pub(crate) fn subnet_az<'a>(state: &'a Ec2State, subnet_id: &str) -> Option<&'a str> {
    state
        .subnets
        .get(subnet_id)
        .map(|s| s.availability_zone.as_str())
        .filter(|az| !az.is_empty())
}

/// NAT gateways in `az`. Only the `pending`, `available` and `deleting`
/// states count toward the quota; a gateway whose subnet fakecloud does not
/// know is in no zone.
pub(crate) fn nat_gateways_by_az(state: &Ec2State) -> BTreeMap<&str, usize> {
    tally(
        state
            .nat_gateways
            .values()
            .filter(|n| matches!(n.state.as_str(), "pending" | "available" | "deleting"))
            .filter_map(|n| subnet_az(state, &n.subnet_id)),
    )
}

/// Network interfaces per Availability Zone. The quota is named per Region,
/// but AWS enforces it per zone: with interfaces in three zones each zone
/// holds the full quota.
pub(crate) fn network_interfaces_by_az(state: &Ec2State) -> BTreeMap<&str, usize> {
    tally(
        state
            .network_interfaces
            .values()
            .map(|e| e.availability_zone.as_str()),
    )
}

// ---- VPC CIDR blocks, peering, endpoints ----

/// IPv4 CIDR blocks of a VPC: the primary block and every associated
/// secondary block count.
pub(crate) fn ipv4_cidr_blocks(vpc: &Vpc) -> usize {
    1 + vpc
        .cidr_associations
        .iter()
        .filter(|a| a.state == "associated" || a.state == "associating")
        .count()
}

/// Active peering connections per VPC, either side counting. A connection
/// of a VPC with itself counts once.
pub(crate) fn active_peerings_by_vpc(state: &Ec2State) -> BTreeMap<&str, usize> {
    let mut out = BTreeMap::new();
    for p in state.vpc_peerings.values().filter(|p| p.status == "active") {
        *out.entry(p.requester_vpc_id.as_str()).or_insert(0) += 1;
        if p.accepter_vpc_id != p.requester_vpc_id {
            *out.entry(p.accepter_vpc_id.as_str()).or_insert(0) += 1;
        }
    }
    out
}

/// Active peering connections with `vpc_id` on either side.
pub(crate) fn active_peerings_of(state: &Ec2State, vpc_id: &str) -> usize {
    active_peerings_by_vpc(state)
        .get(vpc_id)
        .copied()
        .unwrap_or(0)
}

/// Peering connection requests this account made that still await
/// acceptance.
pub(crate) fn outstanding_peering_requests(state: &Ec2State) -> usize {
    state
        .vpc_peerings
        .values()
        .filter(|p| p.status == "pending-acceptance")
        .count()
}

fn is_gateway_endpoint(t: &str) -> bool {
    t.eq_ignore_ascii_case("Gateway")
}

/// Whether an endpoint type counts toward "Interface VPC endpoints per VPC",
/// a combined quota for interface and Gateway Load Balancer endpoints.
pub(crate) fn is_interface_endpoint(t: &str) -> bool {
    t.eq_ignore_ascii_case("Interface") || t.eq_ignore_ascii_case("GatewayLoadBalancer")
}

pub(crate) fn gateway_endpoints(state: &Ec2State) -> usize {
    state
        .vpc_endpoints
        .values()
        .filter(|e| is_gateway_endpoint(&e.endpoint_type))
        .count()
}

pub(crate) fn interface_endpoints_in_vpc(state: &Ec2State, vpc_id: &str) -> usize {
    state
        .vpc_endpoints
        .values()
        .filter(|e| e.vpc_id == vpc_id && is_interface_endpoint(&e.endpoint_type))
        .count()
}

// ---- Elastic IPs, VPN connections ----

/// Elastic IPs from Amazon's pool. Addresses allocated from a BYOIP pool
/// (`PublicIpv4Pool`) do not count toward the quota.
pub(crate) fn elastic_ips(state: &Ec2State) -> usize {
    state
        .elastic_ips
        .values()
        .filter(|e| e.public_ipv4_pool == "amazon")
        .count()
}

pub(crate) fn vpn_connections(state: &Ec2State) -> usize {
    state
        .vpn_connections
        .values()
        .filter(|c| c.state != "deleted")
        .count()
}

// ---- vCPU quotas ----

/// The On-Demand vCPU quota an instance type counts toward, `None` for a
/// family whose quota fakecloud does not enforce (Trn, DL, HPC, Mac).
pub(crate) fn on_demand_quota(instance_type: &str) -> Option<CountQuota> {
    let family = instance_type.split('.').next().unwrap_or_default();
    let starts = |p: &str| family.starts_with(p);
    if starts("inf") {
        Some(ON_DEMAND_INF)
    } else if starts("trn") || starts("dl") || starts("hpc") || starts("mac") {
        None
    } else if starts("u") {
        Some(ON_DEMAND_HIGH_MEMORY)
    } else if starts("f") {
        Some(ON_DEMAND_F)
    } else if starts("g") || starts("vt") {
        Some(ON_DEMAND_G_VT)
    } else if starts("p") {
        Some(ON_DEMAND_P)
    } else if starts("x") {
        Some(ON_DEMAND_X)
    } else if family.starts_with(['a', 'c', 'd', 'h', 'i', 'm', 'r', 't', 'z']) {
        Some(ON_DEMAND_STANDARD)
    } else {
        None
    }
}

/// The vCPU quota an instance counts toward: the Spot quota of the Standard
/// families for a Spot instance, its family's On-Demand quota otherwise.
pub(crate) fn vcpu_quota(instance_type: &str, spot: bool) -> Option<CountQuota> {
    let on_demand = on_demand_quota(instance_type)?;
    if !spot {
        return Some(on_demand);
    }
    (on_demand == ON_DEMAND_STANDARD).then_some(SPOT_STANDARD)
}

/// The default vCPUs of an instance type, `None` for a type fakecloud has no
/// data for.
pub(crate) fn vcpus_of(instance_type: &str) -> Option<usize> {
    crate::instance_types::default_vcpus(instance_type).map(|v| v as usize)
}

/// The vCPUs an instance of `instance_type` adds to a vCPU quota whose
/// enforced limit is `limit`. A type fakecloud has no vCPU data for cannot
/// be counted, so while the quota is enforced it is refused the way AWS
/// refuses an instance type it does not offer; unenforced, it counts 0.
pub(crate) fn launch_vcpus(
    instance_type: &str,
    limit: Option<usize>,
) -> Result<usize, AwsServiceError> {
    match (vcpus_of(instance_type), limit) {
        (Some(v), _) => Ok(v),
        (None, None) => Ok(0),
        (None, Some(_)) => Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("Invalid value '{instance_type}' for InstanceType."),
        )),
    }
}

/// Whether a running or launching instance occupies vCPUs. Instances on a
/// Dedicated Host use the host's capacity, not the account's vCPU quota.
pub(crate) fn occupies_vcpus(i: &Instance) -> bool {
    matches!(i.state_code, 0 | 16) && i.placement_tenancy.as_deref() != Some("host")
}

/// The vCPU quota a start of `i` adds vCPUs to: only a stopped instance
/// that does not sit on a Dedicated Host. One already pending or running
/// holds its vCPUs already.
pub(crate) fn starting_vcpu_quota(i: &Instance) -> Option<CountQuota> {
    let stopped = !matches!(i.state_code, 0 | 16);
    let on_host = i.placement_tenancy.as_deref() == Some("host");
    if stopped && !on_host {
        instance_vcpu_quota(i)
    } else {
        None
    }
}

/// The vCPU quota an instance counts toward.
pub(crate) fn instance_vcpu_quota(i: &Instance) -> Option<CountQuota> {
    vcpu_quota(
        &i.instance_type,
        i.instance_lifecycle.as_deref() == Some("spot"),
    )
}

/// vCPUs counted toward `quota`: running and pending instances, plus open
/// and active requests for the Spot quota. A type with no vCPU data (stored
/// while the quota was not enforced) counts 0.
pub(crate) fn instance_vcpus(state: &Ec2State, quota: CountQuota) -> usize {
    let spot_requests = if quota == SPOT_STANDARD {
        state
            .spot_requests
            .values()
            .filter(|r| r.state == "open" || r.state == "active")
            .filter_map(|r| r.instance_type.as_deref())
            .filter(|t| vcpu_quota(t, true) == Some(quota))
            .filter_map(vcpus_of)
            .fold(0usize, usize::saturating_add)
    } else {
        0
    };
    state
        .instances
        .values()
        .filter(|i| occupies_vcpus(i) && instance_vcpu_quota(i) == Some(quota))
        .filter_map(|i| vcpus_of(&i.instance_type))
        .fold(spot_requests, usize::saturating_add)
}

/// `VcpuLimitExceeded` (or `MaxSpotInstanceCountExceeded` for Spot) when
/// adding `adding` vCPUs to the `current` vCPUs of `quota` goes past
/// `limit`.
pub(crate) fn check_vcpus(
    quota: CountQuota,
    limit: Option<usize>,
    current: usize,
    adding: usize,
) -> Result<(), AwsServiceError> {
    if quota == SPOT_STANDARD {
        return check(
            limit,
            current,
            adding,
            "MaxSpotInstanceCountExceeded",
            |_| "Max spot instance count exceeded".to_string(),
        );
    }
    check(limit, current, adding, "VcpuLimitExceeded", |limit| {
        format!(
            "You have requested more vCPU capacity than your current vCPU limit of {limit} \
             allows for the instance bucket that the specified instance type belongs to. \
             Please visit http://aws.amazon.com/contact-us/ec2-request to request an \
             adjustment to this limit."
        )
    })
}

// ---- usage ----

/// Usage of a count-based quota, `None` for a quota not counted here.
/// "Network interfaces per Region" reports its busiest Availability Zone,
/// the scope AWS enforces it in.
pub(crate) fn usage(state: &Ec2State, quota: CountQuota) -> Option<usize> {
    let region = state.region.as_str();
    let per_vpc = |count: &dyn Fn(&str) -> usize| busiest(state.vpcs.keys().map(|v| count(v)));
    let n = match quota {
        VPCS_PER_REGION => vpcs(state),
        INTERNET_GATEWAYS_PER_REGION => internet_gateways(state),
        SECURITY_GROUPS_PER_REGION => security_groups(state),
        SUBNETS_PER_VPC => per_vpc(&|v| subnets_in_vpc(state, v)),
        ROUTE_TABLES_PER_VPC => per_vpc(&|v| route_tables_in_vpc(state, v)),
        NETWORK_ACLS_PER_VPC => per_vpc(&|v| network_acls_in_vpc(state, v)),
        INTERFACE_ENDPOINTS_PER_VPC => per_vpc(&|v| interface_endpoints_in_vpc(state, v)),
        ACTIVE_PEERINGS_PER_VPC => busiest(active_peerings_by_vpc(state).into_values()),
        IPV6_CIDR_BLOCKS_PER_VPC => busiest(
            state
                .vpcs
                .values()
                .map(|v| usize::from(v.ipv6_cidr_block.is_some()) + v.ipv6_cidr_associations.len()),
        ),
        EGRESS_ONLY_IGWS_PER_REGION => state.egress_only_igws.len(),
        TRANSIT_GATEWAYS => state
            .transit_gateways
            .values()
            .filter(|t| t.state != "deleted")
            .count(),
        IPV4_CIDR_BLOCKS_PER_VPC => busiest(state.vpcs.values().map(ipv4_cidr_blocks)),
        ROUTES_PER_ROUTE_TABLE => busiest(state.route_tables.values().map(|rt| {
            let [v4, v6] = route_counts(rt, &state.managed_prefix_lists, region);
            v4.max(v6)
        })),
        RULES_PER_NETWORK_ACL => busiest(
            state
                .network_acls
                .values()
                .map(|a| nacl_rules(a, false).max(nacl_rules(a, true))),
        ),
        NAT_GATEWAYS_PER_AZ => busiest(nat_gateways_by_az(state).into_values()),
        NETWORK_INTERFACES_PER_REGION => busiest(network_interfaces_by_az(state).into_values()),
        OUTSTANDING_PEERING_REQUESTS => outstanding_peering_requests(state),
        GATEWAY_ENDPOINTS_PER_REGION => gateway_endpoints(state),
        ELASTIC_IPS => elastic_ips(state),
        VPN_CONNECTIONS_PER_REGION => vpn_connections(state),
        q if q == SPOT_STANDARD || ON_DEMAND_VCPU_QUOTAS.contains(&q) => instance_vcpus(state, q),
        _ => return None,
    };
    Some(n)
}

#[cfg(test)]
#[path = "resource_quotas_tests.rs"]
mod tests;
