//! Enforcement and usage of the count-based EC2 and VPC quotas.

use std::sync::Arc;

use fakecloud_core::quota::{FixedQuotas, QuotaUsageSource};
use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::*;
use crate::service::quota::Ec2QuotaUsage;
use crate::service::{eip, endpoint, eni, fleet, instance, nacl, routing, sg, subnet, vpc, vpn};
use crate::test_support::ec2_request as req;

const ACCT: &str = "000000000000";

fn enforcing(quotas: &[(CountQuota, f64)]) -> Ec2Service {
    let provider = quotas.iter().fold(FixedQuotas::default(), |p, (q, n)| {
        p.with(q.service, q.code, *n)
    });
    Ec2Service::new().with_quota_provider(Some(Arc::new(provider)))
}

fn body(r: Result<AwsResponse, AwsServiceError>) -> String {
    match r {
        Ok(resp) => String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap(),
        Err(e) => panic!("expected success, got {}: {}", e.code(), e.message()),
    }
}

fn code(r: Result<AwsResponse, AwsServiceError>) -> String {
    crate::test_support::err_of(r).code().to_string()
}

fn xml(body: &str, tag: &str) -> String {
    body.split(&format!("<{tag}>"))
        .nth(1)
        .and_then(|s| s.split(&format!("</{tag}>")).next())
        .unwrap_or_else(|| panic!("no <{tag}> in {body}"))
        .to_string()
}

/// The usage `Ec2QuotaUsage` reports for `q`.
fn usage(svc: &Ec2Service, q: CountQuota) -> usize {
    Ec2QuotaUsage::new(svc.state.clone())
        .usage(ACCT, "us-east-1", q.service, q.code)
        .unwrap() as usize
}

/// Touch the account so its default resources exist before counting.
fn seeded(svc: Ec2Service) -> Ec2Service {
    svc.state.write().regional_mut(ACCT, "us-east-1");
    body(vpc::describe_vpcs(&svc, &req("DescribeVpcs", &[])));
    svc
}

fn make_vpc(svc: &Ec2Service) -> String {
    xml(
        &body(vpc::create_vpc(
            svc,
            &req("CreateVpc", &[("CidrBlock", "10.0.0.0/16")]),
        )),
        "vpcId",
    )
}

fn make_subnet(svc: &Ec2Service, vpc_id: &str, az: &str) -> String {
    xml(
        &body(subnet::create_subnet(
            svc,
            &req(
                "CreateSubnet",
                &[
                    ("VpcId", vpc_id),
                    ("CidrBlock", "10.0.1.0/24"),
                    ("AvailabilityZone", az),
                ],
            ),
        )),
        "subnetId",
    )
}

#[test]
fn nothing_is_enforced_without_a_provider() {
    let svc = seeded(Ec2Service::new());
    for _ in 0..8 {
        make_vpc(&svc);
        body(routing::create_internet_gateway(
            &svc,
            &req("CreateInternetGateway", &[]),
        ));
        body(eip::allocate_address(&svc, &req("AllocateAddress", &[])));
    }
    assert!(usage(&svc, VPCS_PER_REGION) >= 9);
    assert_eq!(usage(&svc, ELASTIC_IPS), 8);
}

#[test]
fn vpcs_per_region() {
    let probe = seeded(Ec2Service::new());
    let current = usage(&probe, VPCS_PER_REGION);
    let svc = seeded(enforcing(&[(VPCS_PER_REGION, (current + 1) as f64)]));
    make_vpc(&svc);
    assert_eq!(usage(&svc, VPCS_PER_REGION), current + 1);
    let r = vpc::create_vpc(&svc, &req("CreateVpc", &[("CidrBlock", "10.1.0.0/16")]));
    assert_eq!(code(r), "VpcLimitExceeded");
    assert_eq!(usage(&svc, VPCS_PER_REGION), current + 1);

    // Exhausting one region must not consume another region's allowance.
    let mut west = req("CreateVpc", &[("CidrBlock", "10.1.0.0/16")]);
    west.region = "us-west-2".into();
    body(vpc::create_vpc(&svc, &west));
    let usage_source = Ec2QuotaUsage::new(svc.state.clone());
    assert_eq!(
        usage_source.usage(
            ACCT,
            "us-west-2",
            VPCS_PER_REGION.service,
            VPCS_PER_REGION.code
        ),
        Some((current + 1) as f64)
    );
    assert_eq!(usage(&svc, VPCS_PER_REGION), current + 1);
    assert_eq!(code(vpc::create_vpc(&svc, &west)), "VpcLimitExceeded");
}

#[test]
fn internet_gateways_per_region() {
    let probe = seeded(Ec2Service::new());
    let current = usage(&probe, INTERNET_GATEWAYS_PER_REGION);
    let svc = seeded(enforcing(&[(
        INTERNET_GATEWAYS_PER_REGION,
        (current + 1) as f64,
    )]));
    body(routing::create_internet_gateway(
        &svc,
        &req("CreateInternetGateway", &[]),
    ));
    let r = routing::create_internet_gateway(&svc, &req("CreateInternetGateway", &[]));
    assert_eq!(code(r), "InternetGatewayLimitExceeded");
    assert_eq!(usage(&svc, INTERNET_GATEWAYS_PER_REGION), current + 1);
}

#[test]
fn subnets_per_vpc_are_scoped_to_the_vpc() {
    let svc = seeded(enforcing(&[(SUBNETS_PER_VPC, 1.0)]));
    let a = make_vpc(&svc);
    let b = make_vpc(&svc);
    make_subnet(&svc, &a, "us-east-1a");
    let r = subnet::create_subnet(
        &svc,
        &req(
            "CreateSubnet",
            &[("VpcId", &a), ("CidrBlock", "10.0.2.0/24")],
        ),
    );
    assert_eq!(code(r), "SubnetLimitExceeded");
    // Another VPC has its own allowance.
    make_subnet(&svc, &b, "us-east-1a");
    // Usage is the busiest VPC: the default VPC with its default subnets.
    let default_subnets = usage(&seeded(Ec2Service::new()), SUBNETS_PER_VPC);
    assert_eq!(usage(&svc, SUBNETS_PER_VPC), default_subnets.max(1));
}

#[test]
fn a_new_default_subnet_counts_toward_the_default_vpc() {
    let svc = seeded(enforcing(&[(SUBNETS_PER_VPC, 0.0)]));
    // The default VPC has default subnets in its first zones only.
    let unseeded = {
        let accounts = svc.state.read();
        let state = accounts.regional(ACCT, "us-east-1").unwrap();
        let zones: Vec<String> = state
            .subnets
            .values()
            .filter(|s| s.default_for_az)
            .map(|s| s.availability_zone.clone())
            .collect();
        crate::defaults::region_zones("us-east-1")
            .into_iter()
            .map(|(name, _)| name)
            .find(|z| !zones.contains(z))
            .expect("a zone without a default subnet")
    };
    // The zone's existing default subnet is returned, not created.
    body(subnet::create_default_subnet(
        &svc,
        &req("CreateDefaultSubnet", &[("AvailabilityZone", "us-east-1a")]),
    ));
    let r = subnet::create_default_subnet(
        &svc,
        &req("CreateDefaultSubnet", &[("AvailabilityZone", &unseeded)]),
    );
    assert_eq!(code(r), "SubnetLimitExceeded");
}

#[test]
fn security_groups_per_region_count_default_groups() {
    let probe = seeded(Ec2Service::new());
    let current = usage(&probe, SECURITY_GROUPS_PER_REGION);
    assert!(current >= 1, "the default VPC's default group counts");
    let svc = seeded(enforcing(&[(
        SECURITY_GROUPS_PER_REGION,
        (current + 1) as f64,
    )]));
    let create = |name: &str| {
        sg::create_security_group(
            &svc,
            &req(
                "CreateSecurityGroup",
                &[("GroupName", name), ("GroupDescription", "d")],
            ),
        )
    };
    body(create("one"));
    assert_eq!(code(create("two")), "SecurityGroupLimitExceeded");
}

#[test]
fn route_tables_per_vpc_count_the_main_table() {
    let svc = seeded(enforcing(&[(ROUTE_TABLES_PER_VPC, 2.0)]));
    let a = make_vpc(&svc);
    let create = |vpc_id: &str| {
        routing::create_route_table(&svc, &req("CreateRouteTable", &[("VpcId", vpc_id)]))
    };
    // The VPC's main route table is the first of two.
    body(create(&a));
    assert_eq!(code(create(&a)), "RouteTableLimitExceeded");
    let b = make_vpc(&svc);
    body(create(&b));
    assert_eq!(usage(&svc, ROUTE_TABLES_PER_VPC), 2);
}

#[test]
fn routes_per_route_table_are_counted_per_family_without_the_local_route() {
    let svc = seeded(enforcing(&[(ROUTES_PER_ROUTE_TABLE, 1.0)]));
    let v = make_vpc(&svc);
    let rtb = xml(
        &body(routing::create_route_table(
            &svc,
            &req("CreateRouteTable", &[("VpcId", &v)]),
        )),
        "routeTableId",
    );
    let route = |key: &str, dest: &str| {
        routing::create_route(
            &svc,
            &req(
                "CreateRoute",
                &[("RouteTableId", &rtb), (key, dest), ("GatewayId", "igw-1")],
            ),
        )
    };
    body(route("DestinationCidrBlock", "0.0.0.0/0"));
    assert_eq!(
        code(route("DestinationCidrBlock", "192.168.0.0/16")),
        "RouteLimitExceeded"
    );
    // IPv6 routes are limited separately.
    body(route("DestinationIpv6CidrBlock", "::/0"));
    assert_eq!(
        code(route("DestinationIpv6CidrBlock", "2001:db8::/32")),
        "RouteLimitExceeded"
    );
    assert_eq!(usage(&svc, ROUTES_PER_ROUTE_TABLE), 1);
    // Replacing an existing route adds nothing.
    body(routing::replace_route(
        &svc,
        &req(
            "ReplaceRoute",
            &[
                ("RouteTableId", &rtb),
                ("DestinationCidrBlock", "0.0.0.0/0"),
                ("GatewayId", "igw-2"),
            ],
        ),
    ));
}

#[test]
fn network_acls_per_vpc_count_the_default_acl() {
    let svc = seeded(enforcing(&[(NETWORK_ACLS_PER_VPC, 2.0)]));
    let v = make_vpc(&svc);
    let create = || nacl::create_network_acl(&svc, &req("CreateNetworkAcl", &[("VpcId", &v)]));
    body(create());
    assert_eq!(code(create()), "NetworkAclLimitExceeded");
}

#[test]
fn rules_per_network_acl_are_limited_per_direction() {
    let svc = seeded(enforcing(&[(RULES_PER_NETWORK_ACL, 1.0)]));
    let v = make_vpc(&svc);
    let acl = xml(
        &body(nacl::create_network_acl(
            &svc,
            &req("CreateNetworkAcl", &[("VpcId", &v)]),
        )),
        "networkAclId",
    );
    let entry = |n: &str, egress: &str| {
        nacl::create_network_acl_entry(
            &svc,
            &req(
                "CreateNetworkAclEntry",
                &[
                    ("NetworkAclId", &acl),
                    ("RuleNumber", n),
                    ("Protocol", "-1"),
                    ("RuleAction", "allow"),
                    ("Egress", egress),
                    ("CidrBlock", "0.0.0.0/0"),
                ],
            ),
        )
    };
    // The default deny rule does not count: one inbound rule fits.
    body(entry("100", "false"));
    assert_eq!(code(entry("110", "false")), "NetworkAclEntryLimitExceeded");
    body(entry("100", "true"));
    assert_eq!(code(entry("110", "true")), "NetworkAclEntryLimitExceeded");
    assert_eq!(usage(&svc, RULES_PER_NETWORK_ACL), 1);
}

#[test]
fn nat_gateways_are_limited_per_availability_zone() {
    let svc = seeded(enforcing(&[(NAT_GATEWAYS_PER_AZ, 1.0)]));
    let v = make_vpc(&svc);
    let a1 = make_subnet(&svc, &v, "us-east-1a");
    let a2 = make_subnet(&svc, &v, "us-east-1a");
    let b = make_subnet(&svc, &v, "us-east-1b");
    let nat = |subnet_id: &str| {
        routing::create_nat_gateway(
            &svc,
            &req(
                "CreateNatGateway",
                &[("SubnetId", subnet_id), ("ConnectivityType", "private")],
            ),
        )
    };
    body(nat(&a1));
    // Another subnet of the same zone shares the zone's quota.
    assert_eq!(code(nat(&a2)), "NatGatewayLimitExceeded");
    body(nat(&b));
    assert_eq!(usage(&svc, NAT_GATEWAYS_PER_AZ), 1);
}

#[test]
fn network_interfaces_are_limited_per_availability_zone() {
    let svc = seeded(enforcing(&[(NETWORK_INTERFACES_PER_REGION, 1.0)]));
    let v = make_vpc(&svc);
    let a = make_subnet(&svc, &v, "us-east-1a");
    let b = make_subnet(&svc, &v, "us-east-1b");
    let eni = |subnet_id: &str| {
        eni::create_network_interface(
            &svc,
            &req("CreateNetworkInterface", &[("SubnetId", subnet_id)]),
        )
    };
    let out = body(eni(&a));
    assert!(out.contains("<availabilityZone>us-east-1a</availabilityZone>"));
    assert_eq!(code(eni(&a)), "NetworkInterfaceLimitExceeded");
    let out = body(eni(&b));
    assert!(out.contains("<availabilityZone>us-east-1b</availabilityZone>"));
    assert_eq!(usage(&svc, NETWORK_INTERFACES_PER_REGION), 1);
}

#[test]
fn ipv4_cidr_blocks_per_vpc_count_the_primary_block() {
    let svc = seeded(enforcing(&[(IPV4_CIDR_BLOCKS_PER_VPC, 2.0)]));
    let v = make_vpc(&svc);
    let assoc = |cidr: &str| {
        vpc::associate_vpc_cidr_block(
            &svc,
            &req(
                "AssociateVpcCidrBlock",
                &[("VpcId", &v), ("CidrBlock", cidr)],
            ),
        )
    };
    body(assoc("10.1.0.0/16"));
    let e = crate::test_support::err_of(assoc("10.2.0.0/16"));
    assert_eq!(e.code(), "CidrLimitExceeded");
    assert_eq!(
        e.message(),
        format!("This network '{v}' has met its maximum number of allowed CIDRs: 2")
    );
    assert_eq!(usage(&svc, IPV4_CIDR_BLOCKS_PER_VPC), 2);
}

#[test]
fn peering_quotas() {
    let svc = seeded(enforcing(&[
        (OUTSTANDING_PEERING_REQUESTS, 1.0),
        (ACTIVE_PEERINGS_PER_VPC, 1.0),
    ]));
    let a = make_vpc(&svc);
    let b = make_vpc(&svc);
    let c = make_vpc(&svc);
    let peer = |from: &str, to: &str| {
        nacl::create_vpc_peering_connection(
            &svc,
            &req(
                "CreateVpcPeeringConnection",
                &[("VpcId", from), ("PeerVpcId", to)],
            ),
        )
    };
    let accept = |id: &str| {
        nacl::accept_vpc_peering_connection(
            &svc,
            &req(
                "AcceptVpcPeeringConnection",
                &[("VpcPeeringConnectionId", id)],
            ),
        )
    };
    let ab = xml(&body(peer(&a, &b)), "vpcPeeringConnectionId");
    assert_eq!(
        code(peer(&a, &c)),
        "OutstandingVpcPeeringConnectionLimitExceeded"
    );
    assert_eq!(usage(&svc, OUTSTANDING_PEERING_REQUESTS), 1);
    body(accept(&ab));
    // Accepted, the request no longer awaits acceptance.
    let ac = xml(&body(peer(&a, &c)), "vpcPeeringConnectionId");
    // `a` already has its one active connection.
    assert_eq!(
        code(accept(&ac)),
        "ActiveVpcPeeringConnectionPerVpcLimitExceeded"
    );
    assert_eq!(usage(&svc, ACTIVE_PEERINGS_PER_VPC), 1);
}

#[test]
fn vpc_endpoint_quotas_by_type() {
    let svc = seeded(enforcing(&[
        (GATEWAY_ENDPOINTS_PER_REGION, 1.0),
        (INTERFACE_ENDPOINTS_PER_VPC, 1.0),
    ]));
    let a = make_vpc(&svc);
    let b = make_vpc(&svc);
    let endpoint = |vpc_id: &str, kind: &str| {
        endpoint::create_vpc_endpoint(
            &svc,
            &req(
                "CreateVpcEndpoint",
                &[
                    ("VpcId", vpc_id),
                    ("VpcEndpointType", kind),
                    ("ServiceName", "com.amazonaws.us-east-1.s3"),
                ],
            ),
        )
    };
    body(endpoint(&a, "Gateway"));
    // Gateway endpoints are counted per Region.
    assert_eq!(code(endpoint(&b, "Gateway")), "VpcEndpointLimitExceeded");
    body(endpoint(&a, "Interface"));
    // Interface and Gateway Load Balancer endpoints share a per-VPC quota.
    assert_eq!(
        code(endpoint(&a, "GatewayLoadBalancer")),
        "VpcEndpointLimitExceeded"
    );
    body(endpoint(&b, "Interface"));
    assert_eq!(usage(&svc, GATEWAY_ENDPOINTS_PER_REGION), 1);
    assert_eq!(usage(&svc, INTERFACE_ENDPOINTS_PER_VPC), 1);
}

#[test]
fn elastic_ips() {
    let svc = seeded(enforcing(&[(ELASTIC_IPS, 2.0)]));
    let allocate = || eip::allocate_address(&svc, &req("AllocateAddress", &[("Domain", "vpc")]));
    body(allocate());
    body(allocate());
    let e = crate::test_support::err_of(allocate());
    assert_eq!(e.code(), "AddressLimitExceeded");
    assert_eq!(
        e.message(),
        "The maximum number of addresses has been reached."
    );
    assert_eq!(usage(&svc, ELASTIC_IPS), 2);
}

#[test]
fn vpn_connections_per_region() {
    let svc = seeded(enforcing(&[(VPN_CONNECTIONS_PER_REGION, 1.0)]));
    let vpn = || {
        vpn::create_vpn_connection(
            &svc,
            &req(
                "CreateVpnConnection",
                &[("CustomerGatewayId", "cgw-1"), ("Type", "ipsec.1")],
            ),
        )
    };
    body(vpn());
    assert_eq!(code(vpn()), "VpnConnectionLimitExceeded");
    assert_eq!(usage(&svc, VPN_CONNECTIONS_PER_REGION), 1);
}

#[test]
fn instance_families_map_to_their_vcpu_quota() {
    for (t, q) in [
        ("t3.micro", Some(ON_DEMAND_STANDARD)),
        ("m5.large", Some(ON_DEMAND_STANDARD)),
        ("im4gn.large", Some(ON_DEMAND_STANDARD)),
        ("z1d.large", Some(ON_DEMAND_STANDARD)),
        ("inf2.xlarge", Some(ON_DEMAND_INF)),
        ("f1.2xlarge", Some(ON_DEMAND_F)),
        ("g5.xlarge", Some(ON_DEMAND_G_VT)),
        ("vt1.3xlarge", Some(ON_DEMAND_G_VT)),
        ("p4d.24xlarge", Some(ON_DEMAND_P)),
        ("x2idn.16xlarge", Some(ON_DEMAND_X)),
        ("u-6tb1.metal", Some(ON_DEMAND_HIGH_MEMORY)),
        ("u7i-12tb.224xlarge", Some(ON_DEMAND_HIGH_MEMORY)),
        ("trn1.2xlarge", None),
        ("hpc7g.16xlarge", None),
        ("mac2.metal", None),
    ] {
        assert_eq!(on_demand_quota(t), q, "{t}");
    }
    assert_eq!(vcpu_quota("m5.large", true), Some(SPOT_STANDARD));
    assert_eq!(vcpu_quota("g5.xlarge", true), None);
}

async fn run(svc: &Ec2Service, params: &[(&str, &str)]) -> Result<AwsResponse, AwsServiceError> {
    let mut p = vec![("ImageId", "ami-12345678")];
    p.extend_from_slice(params);
    instance::run_instances(svc, &req("RunInstances", &p)).await
}

#[tokio::test]
async fn on_demand_vcpus_are_summed_per_family() {
    // t3.micro has 2 vCPUs.
    let svc = seeded(enforcing(&[
        (ON_DEMAND_STANDARD, 5.0),
        (ON_DEMAND_G_VT, 0.0),
    ]));
    let one = [
        ("InstanceType", "t3.micro"),
        ("MinCount", "1"),
        ("MaxCount", "1"),
    ];
    body(run(&svc, &one).await);
    body(run(&svc, &one).await);
    let e = crate::test_support::err_of(run(&svc, &one).await);
    assert_eq!(e.code(), "VcpuLimitExceeded");
    assert!(
        e.message().contains("current vCPU limit of 5"),
        "{}",
        e.message()
    );
    assert_eq!(usage(&svc, ON_DEMAND_STANDARD), 4);
    // A G instance counts toward its own quota (0 vCPUs on a new account).
    let g = [
        ("InstanceType", "g5.xlarge"),
        ("MinCount", "1"),
        ("MaxCount", "1"),
    ];
    assert_eq!(code(run(&svc, &g).await), "VcpuLimitExceeded");
    // An instance on a Dedicated Host uses the host, not the account's vCPUs.
    body(
        run(
            &svc,
            &[
                ("InstanceType", "r5.large"),
                ("MinCount", "1"),
                ("MaxCount", "1"),
                ("Placement.Tenancy", "host"),
            ],
        )
        .await,
    );
}

#[tokio::test]
async fn a_launch_takes_as_many_instances_as_fit_down_to_min_count() {
    let svc = seeded(enforcing(&[(ON_DEMAND_STANDARD, 6.0)]));
    let out = body(
        run(
            &svc,
            &[
                ("InstanceType", "t3.micro"),
                ("MinCount", "1"),
                ("MaxCount", "5"),
            ],
        )
        .await,
    );
    assert_eq!(out.matches("<instanceId>").count(), 3);
    assert_eq!(
        code(
            run(
                &svc,
                &[
                    ("InstanceType", "t3.micro"),
                    ("MinCount", "1"),
                    ("MaxCount", "1"),
                ],
            )
            .await
        ),
        "VcpuLimitExceeded"
    );
}

#[tokio::test]
async fn starting_a_stopped_instance_needs_room_for_its_vcpus() {
    let svc = seeded(enforcing(&[(ON_DEMAND_STANDARD, 2.0)]));
    let one = [
        ("InstanceType", "t3.micro"),
        ("MinCount", "1"),
        ("MaxCount", "1"),
    ];
    let first = xml(&body(run(&svc, &one).await), "instanceId");
    let ids = |id: &str| req("StopInstances", &[("InstanceId.1", id)]);
    body(instance::stop_instances(&svc, &ids(&first)).await);
    assert_eq!(usage(&svc, ON_DEMAND_STANDARD), 0);
    let second = xml(&body(run(&svc, &one).await), "instanceId");
    let start = req("StartInstances", &[("InstanceId.1", &first)]);
    assert_eq!(
        code(instance::start_instances(&svc, &start).await),
        "VcpuLimitExceeded"
    );
    body(instance::stop_instances(&svc, &ids(&second)).await);
    body(instance::start_instances(&svc, &start).await);
    // Starting an instance that already runs adds nothing.
    body(instance::start_instances(&svc, &start).await);
}

#[tokio::test]
async fn spot_vcpus_count_launches_and_requests() {
    let svc = seeded(enforcing(&[
        (SPOT_STANDARD, 4.0),
        (ON_DEMAND_STANDARD, 0.0),
    ]));
    body(
        run(
            &svc,
            &[
                ("InstanceType", "t3.micro"),
                ("MinCount", "1"),
                ("MaxCount", "1"),
                ("InstanceMarketOptions.MarketType", "spot"),
            ],
        )
        .await,
    );
    let spot = |count: &str| {
        fleet::request_spot_instances(
            &svc,
            &req(
                "RequestSpotInstances",
                &[
                    ("InstanceCount", count),
                    ("LaunchSpecification.InstanceType", "m5.large"),
                ],
            ),
        )
    };
    let e = crate::test_support::err_of(spot("2"));
    assert_eq!(e.code(), "MaxSpotInstanceCountExceeded");
    assert_eq!(e.message(), "Max spot instance count exceeded");
    body(spot("1"));
    assert_eq!(usage(&svc, SPOT_STANDARD), 4);
    assert_eq!(usage(&svc, ON_DEMAND_STANDARD), 0);
}

#[tokio::test]
async fn secondary_interfaces_of_a_launch_count_toward_their_zone() {
    let svc = seeded(enforcing(&[(NETWORK_INTERFACES_PER_REGION, 1.0)]));
    let v = make_vpc(&svc);
    let a = make_subnet(&svc, &v, "us-east-1a");
    let params = [
        ("InstanceType", "t3.micro"),
        ("MinCount", "1"),
        ("MaxCount", "1"),
        ("NetworkInterface.1.DeviceIndex", "0"),
        ("NetworkInterface.1.SubnetId", a.as_str()),
        ("NetworkInterface.2.DeviceIndex", "1"),
        ("NetworkInterface.2.SubnetId", a.as_str()),
    ];
    body(run(&svc, &params).await);
    assert_eq!(
        code(run(&svc, &params).await),
        "NetworkInterfaceLimitExceeded"
    );
}

#[test]
fn describe_instance_types_reports_real_vcpus() {
    let out = body(instance::describe_instance_types(
        &Ec2Service::new(),
        &req("DescribeInstanceTypes", &[("InstanceType.1", "t2.micro")]),
    ));
    assert!(out.contains("<defaultVCpus>1</defaultVCpus>"), "{out}");
}

#[test]
fn an_account_ec2_has_not_stored_reports_its_default_network() {
    let usage = Ec2QuotaUsage::new(Ec2Service::new().state.clone());
    let of = |q: CountQuota| {
        usage
            .usage("111122223333", "us-east-1", q.service, q.code)
            .unwrap()
    };
    assert_eq!(of(VPCS_PER_REGION), 1.0);
    assert_eq!(of(SECURITY_GROUPS_PER_REGION), 1.0);
    assert!(of(SUBNETS_PER_VPC) >= 1.0);
    assert_eq!(of(ELASTIC_IPS), 0.0);
    // A quota EC2 does not count.
    assert_eq!(
        usage.usage("111122223333", "us-east-1", "ec2", "L-NOPE"),
        None
    );
}

// ---- review follow-ups ----

fn route_table(svc: &Ec2Service) -> String {
    let v = make_vpc(svc);
    xml(
        &body(routing::create_route_table(
            svc,
            &req("CreateRouteTable", &[("VpcId", &v)]),
        )),
        "routeTableId",
    )
}

fn routes_of(svc: &Ec2Service, rtb: &str) -> Vec<crate::state::Route> {
    svc.state
        .read()
        .regional(ACCT, "us-east-1")
        .unwrap()
        .route_tables[rtb]
        .routes
        .clone()
}

#[test]
fn replace_route_matches_the_destination_it_names() {
    let svc = seeded(Ec2Service::new());
    let rtb = route_table(&svc);
    let route = |action: &str, key: &str, dest: &str, gw: &str| {
        let r = req(
            action,
            &[("RouteTableId", &rtb), (key, dest), ("GatewayId", gw)],
        );
        if action == "CreateRoute" {
            routing::create_route(&svc, &r)
        } else {
            routing::replace_route(&svc, &r)
        }
    };
    body(route(
        "CreateRoute",
        "DestinationCidrBlock",
        "0.0.0.0/0",
        "igw-4",
    ));
    body(route(
        "CreateRoute",
        "DestinationIpv6CidrBlock",
        "::/0",
        "igw-6",
    ));
    body(route(
        "CreateRoute",
        "DestinationPrefixListId",
        "pl-1",
        "igw-p",
    ));
    let before = routes_of(&svc, &rtb).len();
    body(route(
        "ReplaceRoute",
        "DestinationIpv6CidrBlock",
        "::/0",
        "igw-6b",
    ));
    body(route(
        "ReplaceRoute",
        "DestinationPrefixListId",
        "pl-1",
        "igw-pb",
    ));
    let routes = routes_of(&svc, &rtb);
    assert_eq!(routes.len(), before, "a replace adds no route");
    let gw = |f: &dyn Fn(&crate::state::Route) -> bool| {
        routes
            .iter()
            .find(|r| f(r))
            .unwrap()
            .gateway_id
            .clone()
            .unwrap()
    };
    assert_eq!(
        gw(&|r| r.destination_ipv6_cidr_block.as_deref() == Some("::/0")),
        "igw-6b"
    );
    assert_eq!(
        gw(&|r| r.destination_prefix_list_id.as_deref() == Some("pl-1")),
        "igw-pb"
    );
    assert_eq!(
        gw(&|r| r.destination_cidr_block.as_deref() == Some("0.0.0.0/0")),
        "igw-4"
    );
    // No route with the destination: refused, nothing appended.
    for (key, dest) in [
        ("DestinationCidrBlock", "192.168.0.0/16"),
        ("DestinationIpv6CidrBlock", "2001:db8::/32"),
        ("DestinationPrefixListId", "pl-2"),
    ] {
        let e = crate::test_support::err_of(route("ReplaceRoute", key, dest, "igw-x"));
        assert_eq!(e.code(), "InvalidParameterValue");
        assert_eq!(
            e.message(),
            format!("There is no route defined for '{dest}' in the route table. Use CreateRoute instead.")
        );
    }
    assert_eq!(routes_of(&svc, &rtb).len(), before);
}

#[test]
fn replace_route_is_not_held_to_the_routes_quota() {
    let svc = seeded(enforcing(&[(ROUTES_PER_ROUTE_TABLE, 1.0)]));
    let rtb = route_table(&svc);
    let r = |action: &str, gw: &str| {
        req(
            action,
            &[
                ("RouteTableId", &rtb),
                ("DestinationCidrBlock", "0.0.0.0/0"),
                ("GatewayId", gw),
            ],
        )
    };
    body(routing::create_route(&svc, &r("CreateRoute", "igw-1")));
    body(routing::replace_route(&svc, &r("ReplaceRoute", "igw-2")));
}

#[test]
fn quota_arithmetic_saturates() {
    assert_eq!(
        check(Some(5), usize::MAX, usize::MAX, "X", |_| String::new())
            .unwrap_err()
            .code(),
        "X"
    );
    assert!(check(Some(5), usize::MAX, 0, "X", |_| String::new()).is_ok());
    assert!(check_routes(Some(1), [usize::MAX, 0], [1, 0]).is_err());
    // A huge Spot request is refused, not overflowed.
    let svc = seeded(enforcing(&[(SPOT_STANDARD, 4.0)]));
    let huge = usize::MAX.to_string();
    let e = crate::test_support::err_of(fleet::request_spot_instances(
        &svc,
        &req(
            "RequestSpotInstances",
            &[
                ("InstanceCount", &huge),
                ("LaunchSpecification.InstanceType", "m5.large"),
            ],
        ),
    ));
    assert_eq!(e.code(), "MaxSpotInstanceCountExceeded");
}

#[tokio::test]
async fn an_unknown_instance_type_is_refused_only_while_its_vcpu_quota_is_enforced() {
    let one = [
        ("InstanceType", "m5.huge"),
        ("MinCount", "1"),
        ("MaxCount", "1"),
    ];
    body(run(&seeded(Ec2Service::new()), &one).await);
    let svc = seeded(enforcing(&[(ON_DEMAND_STANDARD, 100.0)]));
    let e = crate::test_support::err_of(run(&svc, &one).await);
    assert_eq!(e.code(), "InvalidParameterValue");
    assert_eq!(e.message(), "Invalid value 'm5.huge' for InstanceType.");
    // A family whose quota is not enforced is not refused.
    body(
        run(
            &svc,
            &[
                ("InstanceType", "g5.huge"),
                ("MinCount", "1"),
                ("MaxCount", "1"),
            ],
        )
        .await,
    );
    // Spot requests too.
    let svc = seeded(enforcing(&[(SPOT_STANDARD, 100.0)]));
    let r = fleet::request_spot_instances(
        &svc,
        &req(
            "RequestSpotInstances",
            &[("LaunchSpecification.InstanceType", "m5.huge")],
        ),
    );
    assert_eq!(code(r), "InvalidParameterValue");
}

#[tokio::test]
async fn starting_counts_each_instance_once() {
    let svc = seeded(enforcing(&[(ON_DEMAND_STANDARD, 2.0)]));
    let one = [
        ("InstanceType", "t3.micro"),
        ("MinCount", "1"),
        ("MaxCount", "1"),
    ];
    let id = xml(&body(run(&svc, &one).await), "instanceId");
    body(instance::stop_instances(&svc, &req("StopInstances", &[("InstanceId.1", &id)])).await);
    // The same instance named twice adds its 2 vCPUs once.
    body(
        instance::start_instances(
            &svc,
            &req(
                "StartInstances",
                &[("InstanceId.1", &id), ("InstanceId.2", &id)],
            ),
        )
        .await,
    );
    assert_eq!(usage(&svc, ON_DEMAND_STANDARD), 2);
}

#[test]
fn accepting_a_connection_not_pending_acceptance_is_an_invalid_transition() {
    let svc = seeded(Ec2Service::new());
    let a = make_vpc(&svc);
    let b = make_vpc(&svc);
    let peer = |from: &str, to: &str| {
        xml(
            &body(nacl::create_vpc_peering_connection(
                &svc,
                &req(
                    "CreateVpcPeeringConnection",
                    &[("VpcId", from), ("PeerVpcId", to)],
                ),
            )),
            "vpcPeeringConnectionId",
        )
    };
    let accept = |id: &str| {
        nacl::accept_vpc_peering_connection(
            &svc,
            &req(
                "AcceptVpcPeeringConnection",
                &[("VpcPeeringConnectionId", id)],
            ),
        )
    };
    let active = peer(&a, &b);
    body(accept(&active));
    let e = crate::test_support::err_of(accept(&active));
    assert_eq!(e.code(), "InvalidStateTransition");
    assert_eq!(
        e.message(),
        format!(
            "Invalid state transition for {active}, attempted to transition from active to active"
        )
    );
    let rejected = peer(&a, &b);
    body(nacl::reject_vpc_peering_connection(
        &svc,
        &req(
            "RejectVpcPeeringConnection",
            &[("VpcPeeringConnectionId", &rejected)],
        ),
    ));
    assert_eq!(code(accept(&rejected)), "InvalidStateTransition");
}

#[test]
fn counted_but_unenforced_quotas_report_usage() {
    let svc = seeded(Ec2Service::new());
    let v = make_vpc(&svc);
    body(routing::create_egress_only_igw(
        &svc,
        &req("CreateEgressOnlyInternetGateway", &[("VpcId", &v)]),
    ));
    body(vpc::associate_vpc_cidr_block(
        &svc,
        &req(
            "AssociateVpcCidrBlock",
            &[("VpcId", &v), ("AmazonProvidedIpv6CidrBlock", "true")],
        ),
    ));
    assert_eq!(usage(&svc, EGRESS_ONLY_IGWS_PER_REGION), 1);
    assert_eq!(usage(&svc, IPV6_CIDR_BLOCKS_PER_VPC), 1);
    assert_eq!(usage(&svc, TRANSIT_GATEWAYS), 0);
}

#[test]
fn create_vpc_is_held_only_to_the_vpc_quota() {
    let probe = seeded(Ec2Service::new());
    let groups = usage(&probe, SECURITY_GROUPS_PER_REGION);
    // No room for another security group, nor for a network ACL or route
    // table: the VPC and its default resources are still created.
    let svc = seeded(enforcing(&[
        (SECURITY_GROUPS_PER_REGION, groups as f64),
        (NETWORK_ACLS_PER_VPC, 0.0),
        (ROUTE_TABLES_PER_VPC, 0.0),
    ]));
    make_vpc(&svc);
    // Its default group counts toward the security group usage.
    assert_eq!(usage(&svc, SECURITY_GROUPS_PER_REGION), groups + 1);
}

#[tokio::test]
async fn starting_adds_vcpus_only_for_stopped_instances_off_dedicated_hosts() {
    let svc = seeded(enforcing(&[(ON_DEMAND_STANDARD, 2.0)]));
    let launch = |tenancy: Option<&'static str>| {
        let mut p = vec![
            ("InstanceType", "t3.micro"),
            ("MinCount", "1"),
            ("MaxCount", "1"),
        ];
        if let Some(t) = tenancy {
            p.push(("Placement.Tenancy", t));
        }
        p
    };
    let running = xml(&body(run(&svc, &launch(None)).await), "instanceId");
    let host = xml(&body(run(&svc, &launch(Some("host"))).await), "instanceId");
    let stop = |id: &str| req("StopInstances", &[("InstanceId.1", id)]);
    let start = |id: &str| req("StartInstances", &[("InstanceId.1", id)]);
    // The quota is full: the running instance holds both vCPUs.
    assert_eq!(usage(&svc, ON_DEMAND_STANDARD), 2);
    // Starting an instance that already runs adds nothing.
    body(instance::start_instances(&svc, &start(&running)).await);
    // A Dedicated Host instance starts on the host, not the account's vCPUs.
    body(instance::stop_instances(&svc, &stop(&host)).await);
    body(instance::start_instances(&svc, &start(&host)).await);
    assert_eq!(usage(&svc, ON_DEMAND_STANDARD), 2);
}

#[tokio::test]
async fn starting_an_unknown_type_counts_no_vcpus() {
    // Launched while the quota was not enforced.
    let svc = seeded(Ec2Service::new());
    let id = xml(
        &body(
            run(
                &svc,
                &[
                    ("InstanceType", "m5.huge"),
                    ("MinCount", "1"),
                    ("MaxCount", "1"),
                ],
            )
            .await,
        ),
        "instanceId",
    );
    body(instance::stop_instances(&svc, &req("StopInstances", &[("InstanceId.1", &id)])).await);
    let svc = Ec2Service::with_state(svc.state.clone()).with_quota_provider(Some(Arc::new(
        FixedQuotas::default().with(ON_DEMAND_STANDARD.service, ON_DEMAND_STANDARD.code, 0.0),
    )));
    body(instance::start_instances(&svc, &req("StartInstances", &[("InstanceId.1", &id)])).await);
}

#[test]
fn a_replaced_local_route_stays_uncounted() {
    let svc = seeded(enforcing(&[(ROUTES_PER_ROUTE_TABLE, 1.0)]));
    let rtb = route_table(&svc);
    let local = routes_of(&svc, &rtb)
        .into_iter()
        .find(|r| r.gateway_id.as_deref() == Some("local"))
        .unwrap()
        .destination_cidr_block
        .unwrap();
    body(routing::replace_route(
        &svc,
        &req(
            "ReplaceRoute",
            &[
                ("RouteTableId", &rtb),
                ("DestinationCidrBlock", &local),
                ("NetworkInterfaceId", "eni-1"),
            ],
        ),
    ));
    let replaced = routes_of(&svc, &rtb);
    assert_eq!(replaced[0].origin, "CreateRouteTable");
    {
        let accounts = svc.state.read();
        let state = accounts.regional(ACCT, "us-east-1").unwrap();
        let rt = &state.route_tables[&rtb];
        assert_eq!(
            route_counts(rt, &state.managed_prefix_lists, "us-east-1"),
            [0, 0]
        );
    }
    // One route still fits next to the retargeted local route.
    body(routing::create_route(
        &svc,
        &req(
            "CreateRoute",
            &[
                ("RouteTableId", &rtb),
                ("DestinationCidrBlock", "0.0.0.0/0"),
                ("GatewayId", "igw-1"),
            ],
        ),
    ));
}

#[test]
fn creating_an_existing_route_is_refused() {
    let svc = seeded(Ec2Service::new());
    let rtb = route_table(&svc);
    let create = |key: &str, dest: &str| {
        routing::create_route(
            &svc,
            &req(
                "CreateRoute",
                &[("RouteTableId", &rtb), (key, dest), ("GatewayId", "igw-1")],
            ),
        )
    };
    for (key, dest) in [
        ("DestinationCidrBlock", "0.0.0.0/0"),
        ("DestinationIpv6CidrBlock", "::/0"),
        ("DestinationPrefixListId", "pl-1"),
    ] {
        body(create(key, dest));
        let e = crate::test_support::err_of(create(key, dest));
        assert_eq!(e.code(), "RouteAlreadyExists");
        assert_eq!(
            e.message(),
            format!("The route identified by {dest} already exists.")
        );
    }
    // Same address range, other family field: a different destination.
    assert_eq!(routes_of(&svc, &rtb).len(), 4);
}

#[test]
fn default_counts_are_not_another_accounts_state() {
    let usage = Ec2QuotaUsage::new(Ec2Service::new().state.clone());
    let q = VPCS_PER_REGION;
    let a = usage.usage("111122223333", "us-east-1", q.service, q.code);
    let b = usage.usage("444455556666", "us-east-1", q.service, q.code);
    assert_eq!(a, Some(1.0));
    assert_eq!(a, b);
}
