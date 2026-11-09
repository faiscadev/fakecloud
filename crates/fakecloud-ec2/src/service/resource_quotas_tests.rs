//! Enforcement and usage of the count-based EC2 and VPC quotas.

use std::sync::Arc;

use fakecloud_core::quota::{FixedQuotas, QuotaUsageSource};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

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

fn items_of(out: &str) -> Vec<String> {
    out.split("<instanceType>")
        .skip(1)
        .map(|s| s.split("</instanceType>").next().unwrap().to_string())
        .collect()
}

/// Every page of `describe`, one after the other.
fn all_pages(
    describe: fn(&Ec2Service, &AwsRequest) -> Result<AwsResponse, AwsServiceError>,
    action: &str,
    params: &[(&str, &str)],
    region: Option<&str>,
) -> String {
    let svc = Ec2Service::new();
    let mut out = String::new();
    let mut token: Option<String> = None;
    loop {
        let mut request = params.to_vec();
        if let Some(t) = &token {
            request.push(("NextToken", t));
        }
        let mut r = req(action, &request);
        if let Some(region) = region {
            r.region = region.to_string();
        }
        let page = body(describe(&svc, &r));
        token = page
            .split("<nextToken>")
            .nth(1)
            .and_then(|s| s.split('<').next())
            .map(str::to_string);
        out.push_str(&page);
        if token.is_none() {
            return out;
        }
    }
}

fn describe_types(params: &[(&str, &str)]) -> String {
    all_pages(
        instance::describe_instance_types,
        "DescribeInstanceTypes",
        params,
        None,
    )
}

fn types_from_requirements(params: &[(&str, &str)]) -> Vec<String> {
    let mut all = vec![
        ("ArchitectureType.1", "x86_64"),
        ("ArchitectureType.2", "arm64"),
        ("VirtualizationType.1", "hvm"),
        ("VirtualizationType.2", "paravirtual"),
        ("InstanceRequirements.VCpuCount.Min", "1"),
        ("InstanceRequirements.MemoryMiB.Min", "0"),
    ];
    all.extend_from_slice(params);
    items_of(&body(instance::get_instance_types_from_requirements(
        &Ec2Service::new(),
        &req("GetInstanceTypesFromInstanceRequirements", &all),
    )))
}

#[test]
fn describe_instance_types_serves_the_full_catalog_and_filters() {
    let graviton = describe_types(&[("InstanceType.1", "m7g.large")]);
    assert!(graviton.contains("<item>arm64</item>"), "{graviton}");
    assert!(
        graviton.contains("<sizeInMiB>8192</sizeInMiB>"),
        "{graviton}"
    );
    assert!(graviton.contains("<defaultThreadsPerCore>1</defaultThreadsPerCore>"));
    assert!(graviton.contains("<hypervisor>nitro</hypervisor>"));
    assert!(graviton.contains("<baselineThroughputInMBps>78.75</baselineThroughputInMBps>"));
    assert!(graviton.contains("<maximumThroughputInMBps>1250.0</maximumThroughputInMBps>"));
    let amd = describe_types(&[("InstanceType.1", "t3a.large")]);
    assert!(amd.contains("<item>x86_64</item>"), "{amd}");
    assert!(amd.contains("<defaultCores>1</defaultCores>"));
    let filtered = describe_types(&[
        ("Filter.1.Name", "instance-type"),
        ("Filter.1.Value.1", "m7g.*"),
        ("Filter.2.Name", "processor-info.supported-architecture"),
        ("Filter.2.Value.1", "arm64"),
    ]);
    assert!(filtered.contains("<instanceType>m7g.large</instanceType>"));
    assert!(!filtered.contains("<instanceType>m5.large</instanceType>"));
}

#[test]
fn describe_instance_types_reports_real_per_type_values() {
    let t2 = describe_types(&[("InstanceType.1", "t2.micro")]);
    assert!(t2.contains("<hypervisor>xen</hypervisor>"), "{t2}");
    assert!(t2.contains("<item>i386</item>"), "{t2}");
    assert!(t2.contains("<burstablePerformanceSupported>true"), "{t2}");
    assert!(!t2.contains("<item>cluster</item>"), "{t2}");
    let c1 = describe_types(&[("InstanceType.1", "c1.medium")]);
    assert!(c1.contains("<currentGeneration>false"), "{c1}");
    assert!(c1.contains("<instanceStorageSupported>true"), "{c1}");
    assert!(c1.contains("<totalSizeInGB>350</totalSizeInGB>"), "{c1}");
    assert!(c1.contains("<type>hdd</type>"), "{c1}");
    assert!(c1.contains("<ipv6AddressesPerInterface>0<"), "{c1}");
}

#[test]
fn responses_carry_every_block_the_api_has() {
    let m5 = describe_types(&[("InstanceType.1", "m5.large")]);
    for present in [
        "<ebsInfo>",
        "<networkInfo>",
        "<placementGroupInfo>",
        "<networkCards><item>",
        "<maximumEbsAttachments>27</maximumEbsAttachments>",
        "<rebootMigrationSupport>supported</rebootMigrationSupport>",
        "<supportedBootModes><item>legacy-bios</item><item>uefi</item></supportedBootModes>",
        "<validThreadsPerCore><item>1</item><item>2</item></validThreadsPerCore>",
        "<nitroTpmInfo><supportedVersions><item>2.0</item></supportedVersions></nitroTpmInfo>",
        "<efaSupported>false</efaSupported>",
        "<encryptionSupport>supported</encryptionSupport>",
        "<sustainedClockSpeedInGhz>3.1</sustainedClockSpeedInGhz>",
        "<supportedInRegion>true</supportedInRegion>",
    ] {
        assert!(m5.contains(present), "{present}: {m5}");
    }
    // Only what the type has: no instance store on an EBS-only type.
    assert!(!m5.contains("<instanceStorageInfo>"), "{m5}");
    assert!(!m5.contains("<gpuInfo>"), "{m5}");
    // A type no enabled Region sells is known, and flagged, only on request.
    let dl1 = describe_types(&[
        ("InstanceType.1", "dl1.24xlarge"),
        ("IncludeUnsupportedInRegion", "true"),
    ]);
    assert!(dl1.contains("<supportedInRegion>false<"), "{dl1}");
    assert!(dl1.contains("<manufacturer>Habana</manufacturer>"), "{dl1}");
    assert!(dl1.contains("<name>Gaudi HL-205</name>"), "{dl1}");
    assert!(
        dl1.contains("<totalGpuMemoryInMiB>262144</totalGpuMemoryInMiB>"),
        "{dl1}"
    );
}

#[test]
fn every_evaluated_filter_narrows_the_catalog() {
    let m7g_large = |filter: &str, value: &str| {
        items_of(&describe_types(&[
            ("Filter.1.Name", filter),
            ("Filter.1.Value.1", value),
        ]))
        .contains(&"m7g.large".to_string())
    };
    for (filter, hit, miss) in [
        ("memory-info.size-in-mib", "8192", "4096"),
        ("current-generation", "true", "false"),
        ("hypervisor", "nitro", "xen"),
        ("instance-storage-supported", "false", "true"),
        ("bare-metal", "false", "true"),
        ("burstable-performance-supported", "false", "true"),
        ("vcpu-info.default-cores", "2", "1"),
        ("vcpu-info.default-threads-per-core", "1", "2"),
        ("ebs-info.ebs-optimized-support", "default", "unsupported"),
        ("ebs-info.nvme-support", "required", "unsupported"),
        ("ebs-info.ebs-optimized-info.maximum-iops", "40000", "1"),
        (
            "ebs-info.ebs-optimized-info.maximum-throughput-in-mbps",
            "1250.0",
            "1",
        ),
        ("network-info.ena-support", "required", "unsupported"),
        ("network-info.ipv4-addresses-per-interface", "10", "1"),
        ("network-info.ipv6-supported", "true", "false"),
        ("network-info.maximum-network-interfaces", "3", "1"),
        ("network-info.network-performance", "Up to *", "1 Gigabit"),
        ("nitro-enclaves-support", "supported", "unsupported"),
        ("supported-usage-class", "spot", "capacity-block"),
        ("supported-virtualization-type", "hvm", "paravirtual"),
    ] {
        assert!(m7g_large(filter, hit), "{filter}={hit}");
        assert!(!m7g_large(filter, miss), "{filter}={miss}");
    }
    let hdd = items_of(&describe_types(&[
        ("Filter.1.Name", "instance-storage-info.disk.type"),
        ("Filter.1.Value.1", "hdd"),
    ]));
    assert!(hdd.contains(&"d3.xlarge".to_string()));
    assert!(!hdd.contains(&"i3.large".to_string()));
}

/// Every filter the catalog could not answer before, against the number of
/// us-east-1 types the raw `DescribeInstanceTypes` dump has that value on (computed
/// from the dump, not the catalog).
#[test]
fn filters_match_the_answers_computed_from_the_dump() {
    for (filter, value, expected) in [
        ("network-info.efa-supported", "true", 289),
        ("hibernation-supported", "true", 582),
        ("free-tier-eligible", "true", 8),
        ("reboot-migration-support", "supported", 753),
        ("ebs-info.attachment-limit-type", "dedicated", 714),
        ("ebs-info.maximum-ebs-attachments", "28", 6),
        ("instance-storage-info.encryption-support", "required", 527),
        ("network-info.flexible-ena-queues-support", "supported", 552),
        ("network-info.bandwidth-weightings", "vpc-1", 299),
        ("nitro-tpm-support", "supported", 1109),
        ("nitro-tpm-info.supported-versions", "2.0", 1109),
        ("processor-info.supported-features", "amd-sev-snp", 16),
        ("processor-info.sustained-clock-speed-in-ghz", "3.1", 99),
        ("supported-boot-mode", "legacy-bios", 957),
        ("supported-root-device-type", "instance-store", 33),
        ("vcpu-info.valid-cores", "3", 544),
        ("vcpu-info.valid-threads-per-core", "1", 1156),
        ("network-info.encryption-in-transit-supported", "true", 1096),
        ("auto-recovery-supported", "true", 825),
        ("network-info.maximum-network-cards", "2", 52),
        ("network-info.efa-info.maximum-efa-interfaces", "1", 231),
        ("ebs-info.encryption-support", "supported", 1369),
        ("nitro-enclaves-support", "supported", 940),
    ] {
        let found = items_of(&describe_types(&[
            ("Filter.1.Name", filter),
            ("Filter.1.Value.1", value),
        ]));
        assert_eq!(found.len(), expected, "{filter}={value}");
    }
    // Regions differ on this one, and us-east-1 answers with its own value.
    let hosts = items_of(&describe_types(&[
        ("Filter.1.Name", "dedicated-hosts-supported"),
        ("Filter.1.Value.1", "true"),
    ]));
    assert_eq!(hosts.len(), 1152);
}

#[test]
fn only_an_unknown_filter_is_refused() {
    for (filter, _) in crate::instance_types::INSTANCE_TYPE_FILTERS {
        let r = instance::describe_instance_types(
            &Ec2Service::new(),
            &req(
                "DescribeInstanceTypes",
                &[("Filter.1.Name", filter), ("Filter.1.Value.1", "x")],
            ),
        );
        assert!(r.is_ok(), "{filter}");
    }
    let bogus = [("Filter.1.Name", "bogus"), ("Filter.1.Value.1", "x")];
    let r = instance::describe_instance_types(
        &Ec2Service::new(),
        &req("DescribeInstanceTypes", &bogus),
    );
    assert_eq!(
        aws_error(r),
        (
            "InvalidParameterValue".to_string(),
            "The filter 'bogus' is invalid".to_string()
        )
    );
    let r = crate::service::rest::describe_instance_type_offerings(
        &Ec2Service::new(),
        &req("DescribeInstanceTypeOfferings", &bogus),
    );
    assert_eq!(
        aws_error(r),
        (
            "InvalidParameterValue".to_string(),
            "The filter 'bogus' is invalid".to_string()
        )
    );
}

/// Code and message of a refused request.
fn aws_error(r: Result<AwsResponse, AwsServiceError>) -> (String, String) {
    let e = crate::test_support::err_of(r);
    (e.code().to_string(), e.message())
}

#[test]
fn location_type_refusal_and_counts_are_ec2s() {
    let run = |location_type: &str| {
        crate::service::rest::describe_instance_type_offerings(
            &Ec2Service::new(),
            &req(
                "DescribeInstanceTypeOfferings",
                &[
                    ("LocationType", location_type),
                    ("Filter.1.Name", "instance-type"),
                    ("Filter.1.Value.1", "m5.large"),
                ],
            ),
        )
    };
    assert_eq!(
        aws_error(run("bogus")),
        (
            "InvalidParameterValue".to_string(),
            "Invalid value for location type: bogus. \
             Must be one of [availability-zone, availability-zone-id, outpost, region]"
                .to_string(),
        )
    );
    // m5.large in us-east-1, as real EC2 answers it.
    for (location_type, count) in [
        ("region", 1),
        ("availability-zone", 5),
        ("availability-zone-id", 5),
        ("outpost", 0),
    ] {
        let out = body(run(location_type));
        assert_eq!(items_of(&out).len(), count, "{location_type}: {out}");
    }
    // The zone real EC2 does not sell it in, under both of its names.
    let az = body(run("availability-zone"));
    assert!(!az.contains("us-east-1e<"), "{az}");
    let id = body(run("availability-zone-id"));
    assert!(!id.contains(">use1-az5<"), "{id}");
    assert!(id.contains(">use1-az6<"), "{id}");
}

/// x86_64 + hvm, the request every recorded accelerator answer shares.
fn x86_types_from_requirements(region: &str, extra: &[(&str, &str)]) -> Vec<String> {
    let mut all = vec![
        ("ArchitectureType.1", "x86_64"),
        ("VirtualizationType.1", "hvm"),
        ("InstanceRequirements.VCpuCount.Min", "1"),
        ("InstanceRequirements.MemoryMiB.Min", "1"),
    ];
    all.extend_from_slice(extra);
    let mut r = req("GetInstanceTypesFromInstanceRequirements", &all);
    r.region = region.to_string();
    items_of(&body(instance::get_instance_types_from_requirements(
        &Ec2Service::new(),
        &r,
    )))
}

/// As recorded from real us-east-1 (`req_acc_*`).
#[test]
fn accelerator_requirements_match_ec2s_recorded_answers() {
    let by_name = |name: &str| {
        x86_types_from_requirements(
            "us-east-1",
            &[
                ("InstanceRequirements.AcceleratorCount.Min", "1"),
                ("InstanceRequirements.AcceleratorName.1", name),
            ],
        )
    };
    assert_eq!(by_name("a100"), ["p4d.24xlarge", "p4de.24xlarge"]);
    assert_eq!(by_name("h100"), ["p5.48xlarge"]);
    assert_eq!(
        by_name("inferentia2"),
        [
            "inf2.24xlarge",
            "inf2.48xlarge",
            "inf2.8xlarge",
            "inf2.xlarge"
        ]
    );
    assert_eq!(
        by_name("trainium"),
        ["trn1.2xlarge", "trn1.32xlarge", "trn1n.32xlarge"]
    );
    // Not sold in us-east-1, or not x86_64 (t4g).
    for empty in ["vu9p", "gaudi-hl-205", "trainium2", "t4g"] {
        assert!(by_name(empty).is_empty(), "{empty}");
    }
    let inf = [
        "inf1.24xlarge",
        "inf1.2xlarge",
        "inf1.6xlarge",
        "inf1.xlarge",
        "inf2.24xlarge",
        "inf2.48xlarge",
        "inf2.8xlarge",
        "inf2.xlarge",
    ];
    assert_eq!(
        x86_types_from_requirements(
            "us-east-1",
            &[("InstanceRequirements.AcceleratorType.1", "inference")]
        ),
        inf
    );
    let mut amazon = inf.to_vec();
    amazon.extend(["trn1.2xlarge", "trn1.32xlarge", "trn1n.32xlarge"]);
    assert_eq!(
        x86_types_from_requirements(
            "us-east-1",
            &[(
                "InstanceRequirements.AcceleratorManufacturer.1",
                "amazon-web-services"
            )]
        ),
        amazon
    );
    let e = crate::test_support::err_of(instance::get_instance_types_from_requirements(
        &Ec2Service::new(),
        &req(
            "GetInstanceTypesFromInstanceRequirements",
            &[
                ("ArchitectureType.1", "x86_64"),
                ("VirtualizationType.1", "hvm"),
                ("InstanceRequirements.VCpuCount.Min", "1"),
                ("InstanceRequirements.MemoryMiB.Min", "1"),
                ("InstanceRequirements.AcceleratorName.1", "inferentia1"),
            ],
        ),
    ));
    assert_eq!(e.code(), "InvalidParameterValue");
    assert_eq!(
        e.message(),
        "The value 'inferentia1' for AcceleratorNames is not valid. Valid values: [a10g, a100, \
         h100, inferentia, k520, k80, m60, radeon-pro-v520, t4, t4g, vu9p, v100, l40s, l4, \
         gaudi-hl-205, inferentia2, trainium, trainium2, u30]"
    );
    // The names that came back empty above do select their types where they are sold.
    let select = |region: &str, name: &str| {
        x86_types_from_requirements(region, &[("InstanceRequirements.AcceleratorName.1", name)])
    };
    assert!(select("us-gov-west-1", "vu9p").contains(&"f1.2xlarge".to_string()));
    assert!(select("cn-north-1", "gaudi-hl-205").contains(&"dl1.24xlarge".to_string()));
    assert!(select("us-east-2", "trainium2").contains(&"trn2.48xlarge".to_string()));
}

/// Recorded for x86_64 and arm64, hvm and paravirtual, vCPUs and memory from 1.
fn recorded_types(extra: &[(&str, &str)]) -> Vec<String> {
    let mut all = vec![
        ("ArchitectureType.1", "x86_64"),
        ("ArchitectureType.2", "arm64"),
        ("VirtualizationType.1", "hvm"),
        ("VirtualizationType.2", "paravirtual"),
        ("InstanceRequirements.VCpuCount.Min", "1"),
        ("InstanceRequirements.MemoryMiB.Min", "1"),
    ];
    all.extend_from_slice(extra);
    items_of(&body(instance::get_instance_types_from_requirements(
        &Ec2Service::new(),
        &req("GetInstanceTypesFromInstanceRequirements", &all),
    )))
}

#[derive(serde::Deserialize)]
struct RecordedRequirements {
    base: Vec<String>,
    accelerator_count_min_1: Vec<String>,
}

fn recorded_requirements() -> RecordedRequirements {
    serde_json::from_str(include_str!("../testdata/instance_requirements_all.json")).unwrap()
}

/// Real EC2 lists neither of these under any requirements, wherever they are sold.
#[test]
fn the_base_listing_is_ec2s_recorded_one() {
    let base = recorded_types(&[]);
    assert_eq!(base.len(), 1184);
    assert_eq!(base, recorded_requirements().base);
    assert!(!base.contains(&"p5.4xlarge".to_string()));
    let everything = recorded_types(&[
        ("InstanceRequirements.BareMetal", "included"),
        ("InstanceRequirements.BurstablePerformance", "included"),
    ]);
    assert_eq!(everything.len(), 1358);
    assert!(!everything.contains(&"p5.4xlarge".to_string()));
    assert!(!everything.contains(&"t1.micro".to_string()));
    // Though both are described.
    let described = describe_types(&[
        ("InstanceType.1", "p5.4xlarge"),
        ("InstanceType.2", "t1.micro"),
    ]);
    assert_eq!(items_of(&described).len(), 2);
}

#[test]
fn accelerator_counts_match_ec2s_recorded_answers() {
    let fractional = [
        "g6f.2xlarge",
        "g6f.4xlarge",
        "g6f.large",
        "g6f.xlarge",
        "gr6f.4xlarge",
    ];
    let count = |key, value| recorded_types(&[(key, value)]);
    let min = "InstanceRequirements.AcceleratorCount.Min";
    let max = "InstanceRequirements.AcceleratorCount.Max";
    let min_1 = count(min, "1");
    assert_eq!(min_1, recorded_requirements().accelerator_count_min_1);
    let max_0 = count(max, "0");
    assert_eq!(max_0.len(), 1106);
    let max_1 = count(max, "1");
    assert_eq!(max_1.len(), 1150);
    assert_eq!(count(min, "0").len(), 1184);
    // A fractional GPU is above 0 and below 1.
    for name in fractional {
        let name = name.to_string();
        assert!(!min_1.contains(&name), "{name}");
        assert!(!max_0.contains(&name), "{name}");
        assert!(max_1.contains(&name), "{name}");
    }
    // x86_64 + hvm only, as recorded first.
    assert_eq!(
        x86_types_from_requirements("us-east-1", &[(max, "0")]).len(),
        757
    );
    let kind = |value| recorded_types(&[("InstanceRequirements.AcceleratorType.1", value)]);
    let gpu = kind("gpu");
    assert_eq!(gpu.len(), 61);
    assert!(gpu.contains(&"g6f.large".to_string()));
    assert!(gpu.contains(&"p5.48xlarge".to_string()));
    assert!(!gpu.iter().any(|t| t.starts_with("trn")));
    assert_eq!(kind("fpga"), ["f2.12xlarge", "f2.48xlarge", "f2.6xlarge"]);
    assert_eq!(kind("inference").len(), 8);
    assert_eq!(
        kind("media"),
        ["vt1.24xlarge", "vt1.3xlarge", "vt1.6xlarge"]
    );
    let maker =
        |value| recorded_types(&[("InstanceRequirements.AcceleratorManufacturer.1", value)]);
    assert_eq!(maker("amazon-web-services").len(), 11);
    assert_eq!(maker("nvidia").len(), 61);
    // Every device block counts for memory, Trainium's and a fractional GPU's too.
    let memory = |key, value| recorded_types(&[(key, value)]).len();
    assert_eq!(
        memory("InstanceRequirements.AcceleratorTotalMemoryMiB.Min", "1"),
        78
    );
    assert_eq!(
        memory("InstanceRequirements.AcceleratorTotalMemoryMiB.Max", "0"),
        1106
    );
}

const CPU_REFERENCE: &str =
    "InstanceRequirements.BaselinePerformanceFactors.Cpu.Reference.1.InstanceFamily";

/// As real EC2 answered each reference.
#[test]
fn baseline_performance_references_match_ec2s_recorded_answers() {
    let refer = |family| recorded_types(&[(CPU_REFERENCE, family)]);
    let m5 = refer("m5");
    assert_eq!(m5.len(), 1043);
    assert_eq!(refer("c6g").len(), 923);
    // A family it holds no answer for is no answer: real EC2 lists nothing there too.
    for empty in ["t3", "a1", "bogus", "-", "u7i-12tb"] {
        assert!(refer(empty).is_empty(), "{empty}");
    }
    // Case does not matter, and the family's own members are in.
    assert_eq!(refer("M5"), m5);
    assert!(m5.contains(&"m5.large".to_string()));
    for excluded in [
        "m5a.large",
        "r5a.large",
        "i8g.large",
        "m1.large",
        "c1.medium",
    ] {
        assert!(!m5.contains(&excluded.to_string()), "{excluded}");
    }
    assert_eq!(refer(&"a".repeat(30)).len(), 0);
    // Recorded: the burstable policy rules a reference out or in, bare metal only narrows.
    let with =
        |family, member, policy| recorded_types(&[(CPU_REFERENCE, family), (member, policy)]).len();
    let burstable = "InstanceRequirements.BurstablePerformance";
    let metal = "InstanceRequirements.BareMetal";
    assert_eq!(with("t3", burstable, "included"), 1084);
    assert_eq!(with("t3", burstable, "required"), 21);
    assert_eq!(with("t3", metal, "included"), 0);
    assert_eq!(with("m5", burstable, "required"), 0);
    assert_eq!(with("m5", burstable, "included"), 1064);
    assert_eq!(with("m5", metal, "included"), 1179);
    assert_eq!(with("m5", metal, "required"), 136);
    // Recorded: a Region not selling the reference family lists nothing for it.
    let reference = [(CPU_REFERENCE, "x2gd")];
    assert!(!x86_types_from_requirements("us-east-1", &reference).is_empty());
    assert!(x86_types_from_requirements("ca-central-1", &reference).is_empty());
}

#[test]
fn baseline_performance_references_are_refused_as_recorded() {
    let run = |extra: &[(&str, &str)]| {
        let mut all = vec![
            ("ArchitectureType.1", "x86_64"),
            ("VirtualizationType.1", "hvm"),
            ("InstanceRequirements.VCpuCount.Min", "1"),
            ("InstanceRequirements.MemoryMiB.Min", "1"),
        ];
        all.extend_from_slice(extra);
        instance::get_instance_types_from_requirements(
            &Ec2Service::new(),
            &req("GetInstanceTypesFromInstanceRequirements", &all),
        )
    };
    let invalid = "CPU.References contain invalid value for the referred Instance Family. \
                   Correct it, and try again.";
    let thirty_one = "a".repeat(31);
    for family in ["m5.large", "a_b", "a b", "", thirty_one.as_str()] {
        let e = crate::test_support::err_of(run(&[(CPU_REFERENCE, family)]));
        assert_eq!(
            (e.code(), e.message().as_str()),
            ("InvalidParameterValue", invalid),
            "{family}"
        );
    }
    let second = "InstanceRequirements.BaselinePerformanceFactors.Cpu.Reference.2.InstanceFamily";
    for other in ["c6g", "m5"] {
        let e = crate::test_support::err_of(run(&[(CPU_REFERENCE, "m5"), (second, other)]));
        assert_eq!(
            e.message(),
            "CPU.References contain more than the maximum allowed number of items (1). \
             Remove additional references, and try again."
        );
    }
}

/// Recorded: the same 135 types with no price protection and with each of the three
/// at 1%.
#[test]
fn price_protection_does_not_narrow_as_recorded() {
    let expected: Vec<String> =
        serde_json::from_str(include_str!("../testdata/instance_requirements_base.json")).unwrap();
    assert_eq!(expected.len(), 135);
    let range = [
        ("InstanceRequirements.VCpuCount.Max", "4"),
        ("InstanceRequirements.MemoryMiB.Max", "16384"),
    ];
    let run = |extra: Option<(&'static str, &'static str)>| {
        let mut all = vec![
            ("ArchitectureType.1", "x86_64"),
            ("VirtualizationType.1", "hvm"),
            ("InstanceRequirements.VCpuCount.Min", "2"),
            ("InstanceRequirements.MemoryMiB.Min", "4096"),
        ];
        all.extend_from_slice(&range);
        all.extend(extra);
        items_of(&body(instance::get_instance_types_from_requirements(
            &Ec2Service::new(),
            &req("GetInstanceTypesFromInstanceRequirements", &all),
        )))
    };
    assert_eq!(run(None), expected);
    for member in [
        "InstanceRequirements.OnDemandMaxPricePercentageOverLowestPrice",
        "InstanceRequirements.SpotMaxPricePercentageOverLowestPrice",
        "InstanceRequirements.MaxSpotPriceAsPercentageOfOptimalOnDemandPrice",
    ] {
        assert_eq!(run(Some((member, "1"))), expected, "{member}");
    }
}

/// A type kept with basic specs says nothing about what it does not list, so it
/// matches neither side of a requirement on it.
#[test]
fn an_unknown_attribute_does_not_match() {
    let i3p = "i3p.16xlarge".to_string();
    let has =
        |extra: &[(&str, &str)]| x86_types_from_requirements("us-gov-west-1", extra).contains(&i3p);
    assert!(has(&[]));
    assert!(!has(&[("InstanceRequirements.LocalStorage", "excluded")]));
    assert!(!has(&[("InstanceRequirements.LocalStorage", "required")]));
    assert!(!has(&[(
        "InstanceRequirements.TotalLocalStorageGB.Max",
        "0"
    )]));
    assert!(!has(&[("InstanceRequirements.AcceleratorCount.Max", "0")]));
    assert!(!has(&[(
        "InstanceRequirements.AcceleratorTotalMemoryMiB.Max",
        "0"
    )]));
}

/// As recorded from real us-east-1.
#[tokio::test]
async fn run_instances_refuses_a_zone_that_does_not_sell_the_type() {
    let svc = seeded(Ec2Service::new());
    let wrong_zone = [
        ("InstanceType", "m5.large"),
        ("MinCount", "1"),
        ("MaxCount", "1"),
        ("Placement.AvailabilityZone", "us-east-1e"),
    ];
    let recorded = "Your requested instance type (m5.large) is not supported in your requested \
                    Availability Zone (us-east-1e). Please retry your request by not specifying \
                    an Availability Zone or choosing us-east-1a, us-east-1b, us-east-1c, \
                    us-east-1d, us-east-1f.";
    let e = crate::test_support::err_of(run(&svc, &wrong_zone).await);
    assert_eq!((e.code(), e.message().as_str()), ("Unsupported", recorded));

    // The subnet path gives the same answer.
    let vpc = make_vpc(&svc);
    let subnet = xml(
        &body(subnet::create_subnet(
            &svc,
            &req(
                "CreateSubnet",
                &[
                    ("VpcId", &vpc),
                    ("CidrBlock", "10.0.200.0/24"),
                    ("AvailabilityZone", "us-east-1e"),
                ],
            ),
        )),
        "subnetId",
    );
    let in_subnet = [
        ("InstanceType", "m5.large"),
        ("MinCount", "1"),
        ("MaxCount", "1"),
        ("SubnetId", &subnet),
    ];
    let e = crate::test_support::err_of(run(&svc, &in_subnet).await);
    assert_eq!((e.code(), e.message().as_str()), ("Unsupported", recorded));
    // A type that zone does sell launches there.
    body(
        run(
            &svc,
            &[
                ("InstanceType", "m5.large"),
                ("MinCount", "1"),
                ("MaxCount", "1"),
                ("Placement.AvailabilityZone", "us-east-1f"),
            ],
        )
        .await,
    );

    // Recorded: a dry run does not check the zone, but does check the type.
    let mut dry = wrong_zone.to_vec();
    dry.push(("DryRun", "true"));
    assert_eq!(
        crate::test_support::err_of(run(&svc, &dry).await).code(),
        "DryRunOperation"
    );
    let e = crate::test_support::err_of(
        run(
            &svc,
            &[
                ("InstanceType", "m5.bogus"),
                ("MinCount", "1"),
                ("MaxCount", "1"),
                ("DryRun", "true"),
            ],
        )
        .await,
    );
    assert_eq!(e.code(), "InvalidParameterValue");
    assert_eq!(e.message(), "Invalid value 'm5.bogus' for InstanceType.");
}

#[tokio::test]
async fn a_launch_naming_no_zone_lands_where_the_type_is_sold() {
    let svc = seeded(Ec2Service::new());
    // us-east-1a does not sell it.
    let out = body(
        run(
            &svc,
            &[
                ("InstanceType", "inf2.xlarge"),
                ("MinCount", "1"),
                ("MaxCount", "1"),
            ],
        )
        .await,
    );
    assert_eq!(xml(&out, "availabilityZone"), "us-east-1b", "{out}");
    // Inferred: a Region selling it nowhere refuses with the same code, naming no zone
    // when none was asked for.
    let e = crate::test_support::err_of(
        run(
            &svc,
            &[
                ("InstanceType", "dl1.24xlarge"),
                ("MinCount", "1"),
                ("MaxCount", "1"),
            ],
        )
        .await,
    );
    assert_eq!(e.code(), "Unsupported");
    assert_eq!(
        e.message(),
        "Your requested instance type (dl1.24xlarge) is not supported in us-east-1."
    );
    let e = crate::test_support::err_of(
        run(
            &svc,
            &[
                ("InstanceType", "dl1.24xlarge"),
                ("MinCount", "1"),
                ("MaxCount", "1"),
                ("Placement.AvailabilityZone", "us-east-1b"),
            ],
        )
        .await,
    );
    assert_eq!(
        e.message(),
        "Your requested instance type (dl1.24xlarge) is not supported in your requested \
         Availability Zone (us-east-1b)."
    );
}

/// The instance and its subnet are in the same zone, whatever default subnets exist.
#[tokio::test]
async fn a_zone_without_a_default_subnet_does_not_borrow_another_zones() {
    let svc = seeded(Ec2Service::new());
    let in_zone = |out: &str| xml(out, "availabilityZone");
    let params = |zone: &'static str| {
        [
            ("InstanceType", "m5.large"),
            ("MinCount", "1"),
            ("MaxCount", "1"),
            ("Placement.AvailabilityZone", zone),
        ]
    };
    // Past the three default subnets: the zone it asked for, and no other's subnet.
    let out = body(run(&svc, &params("us-east-1f")).await);
    assert_eq!(in_zone(&out), "us-east-1f");
    assert!(!out.contains("<subnetId>"), "{out}");
    // A deleted default subnet behaves the same.
    let default_b = {
        let accounts = svc.state.read();
        accounts
            .regional(ACCT, "us-east-1")
            .unwrap()
            .subnets
            .values()
            .find(|s| s.default_for_az && s.availability_zone == "us-east-1b")
            .unwrap()
            .subnet_id
            .clone()
    };
    svc.state
        .write()
        .regional_mut(ACCT, "us-east-1")
        .subnets
        .remove(&default_b);
    let out = body(run(&svc, &params("us-east-1b")).await);
    assert_eq!(in_zone(&out), "us-east-1b");
    assert!(!out.contains(&default_b), "{out}");
    assert!(!out.contains("<subnetId>"), "{out}");
    // With one, it uses it.
    let out = body(run(&svc, &params("us-east-1a")).await);
    assert_eq!(in_zone(&out), "us-east-1a");
    assert!(out.contains("<subnetId>"), "{out}");
    // And a launch naming no zone prefers one with a default subnet.
    let out = body(
        run(
            &svc,
            &[
                ("InstanceType", "inf2.xlarge"),
                ("MinCount", "1"),
                ("MaxCount", "1"),
            ],
        )
        .await,
    );
    assert_eq!(in_zone(&out), "us-east-1c", "{out}");
}

/// A dry run validates everything but the zone, and changes nothing.
#[tokio::test]
async fn a_dry_run_validates_the_rest_of_the_launch() {
    let svc = seeded(Ec2Service::new());
    let dry = |extra: &'static [(&'static str, &'static str)]| {
        let mut all = vec![
            ("InstanceType", "m5.large"),
            ("MinCount", "1"),
            ("MaxCount", "1"),
            ("DryRun", "true"),
        ];
        all.extend_from_slice(extra);
        all
    };
    let e = crate::test_support::err_of(run(&svc, &dry(&[])).await);
    assert_eq!(e.code(), "DryRunOperation");
    let e = crate::test_support::err_of(
        run(
            &svc,
            &dry(&[("InstanceInitiatedShutdownBehavior", "explode")]),
        )
        .await,
    );
    assert_eq!(e.code(), "InvalidParameterValue");
    // No ImageId: `run` adds one, so the request goes in by hand.
    let e = crate::test_support::err_of(
        instance::run_instances(&svc, &req("RunInstances", &dry(&[]))).await,
    );
    assert_eq!(e.code(), "MissingParameter");
    // Nothing was launched by any of them.
    assert!(svc
        .state
        .read()
        .regional(ACCT, "us-east-1")
        .unwrap()
        .instances
        .is_empty());
}

/// The Regions and zones without data of their own keep working.
#[tokio::test]
async fn carried_over_and_unknown_zones_keep_launching() {
    let in_region = |region: &'static str, itype: &'static str, zone: Option<&'static str>| async move {
        let mut all = vec![
            ("ImageId", "ami-12345678"),
            ("InstanceType", itype),
            ("MinCount", "1"),
            ("MaxCount", "1"),
        ];
        if let Some(z) = zone {
            all.push(("Placement.AvailabilityZone", z));
        }
        let mut r = req("RunInstances", &all);
        r.region = region.to_string();
        instance::run_instances(&Ec2Service::new(), &r).await
    };
    // No per-zone data for an opt-in Region.
    body(in_region("af-south-1", "m5.large", Some("af-south-1c")).await);
    body(in_region("af-south-1", "m5.large", None).await);
    // A letter the dumping account does not have.
    body(in_region("us-west-1", "m5.large", Some("us-west-1b")).await);
    // A Region the catalog does not know at all.
    body(in_region("cn-north-1", "dl1.24xlarge", None).await);
    // Sold only in a carried-over Region.
    assert_eq!(
        code(in_region("us-east-1", "i3p.16xlarge", None).await),
        "Unsupported"
    );
    body(in_region("us-gov-west-1", "i3p.16xlarge", None).await);
}

/// The recorded answers of real EC2 for the same request.
#[test]
fn requirements_match_ec2s_recorded_answer() {
    let run = |extra: &[(&str, &str)]| {
        let mut all = vec![
            ("ArchitectureType.1", "x86_64"),
            ("VirtualizationType.1", "hvm"),
            ("InstanceRequirements.VCpuCount.Min", "2"),
            ("InstanceRequirements.VCpuCount.Max", "2"),
            ("InstanceRequirements.MemoryMiB.Min", "4096"),
            ("InstanceRequirements.MemoryMiB.Max", "4096"),
        ];
        all.extend_from_slice(extra);
        items_of(&body(instance::get_instance_types_from_requirements(
            &Ec2Service::new(),
            &req("GetInstanceTypesFromInstanceRequirements", &all),
        )))
    };
    assert_eq!(
        run(&[]),
        [
            "c5.large",
            "c5a.large",
            "c5ad.large",
            "c5d.large",
            "c6a.large",
            "c6i.large",
            "c6id.large",
            "c6in.large",
            "c7a.large",
            "c7i-flex.large",
            "c7i.large",
            "c8a.large",
            "c8i-flex.large",
            "c8i.large",
            "c8ib.large",
            "c8id.large",
            "c8in.large",
            "c8ine.large",
        ]
    );
    assert_eq!(
        run(&[("InstanceRequirements.LocalStorageType.1", "ssd")]),
        ["c5ad.large", "c5d.large", "c6id.large", "c8id.large"]
    );
}

#[test]
fn requirements_evaluate_what_the_api_reports() {
    let has =
        |params: &[(&str, &str)], t: &str| types_from_requirements(params).contains(&t.to_string());
    let hibernate = ("InstanceRequirements.RequireHibernateSupport", "true");
    assert!(has(&[hibernate], "m5.large"));
    assert!(!has(&[hibernate], "g6.xlarge"));
    let not_asked = ("InstanceRequirements.RequireHibernateSupport", "false");
    assert!(has(&[not_asked], "g6.xlarge"));
    let in_transit = ("InstanceRequirements.RequireEncryptionInTransit", "true");
    assert!(has(&[in_transit], "m5n.large"));
    assert!(!has(&[in_transit], "m5.large"));
    // Baseline network bandwidth, summed over a type's cards.
    let net = ("InstanceRequirements.NetworkBandwidthGbps.Min", "1");
    assert!(!has(&[net], "m5.large"), "0.75 Gbps baseline");
    assert!(has(
        &[("InstanceRequirements.NetworkBandwidthGbps.Max", "1")],
        "m5.large"
    ));
    assert!(has(
        &[("InstanceRequirements.NetworkBandwidthGbps.Min", "3200")],
        "p5.48xlarge"
    ));
    // Recorded not to narrow.
    let everything = types_from_requirements(&[]);
    for unnarrowed in [
        (
            "InstanceRequirements.SpotMaxPricePercentageOverLowestPrice",
            "1",
        ),
        (
            "InstanceRequirements.OnDemandMaxPricePercentageOverLowestPrice",
            "1",
        ),
        (
            "InstanceRequirements.MaxSpotPriceAsPercentageOfOptimalOnDemandPrice",
            "1",
        ),
    ] {
        assert_eq!(
            types_from_requirements(&[unnarrowed]),
            everything,
            "{unnarrowed:?}"
        );
    }
}

#[test]
fn requirements_honor_vcpus_memory_and_architecture() {
    let found = |params: &[(&str, &str)]| {
        let mut all = vec![
            ("ArchitectureType.1", "arm64"),
            ("VirtualizationType.1", "hvm"),
            ("InstanceRequirements.VCpuCount.Min", "2"),
            ("InstanceRequirements.VCpuCount.Max", "2"),
            ("InstanceRequirements.MemoryMiB.Min", "8192"),
            ("InstanceRequirements.MemoryMiB.Max", "8192"),
        ];
        all.extend_from_slice(params);
        items_of(&body(instance::get_instance_types_from_requirements(
            &Ec2Service::new(),
            &req("GetInstanceTypesFromInstanceRequirements", &all),
        )))
    };
    let arm = found(&[]);
    assert!(arm.contains(&"m7g.large".to_string()), "{arm:?}");
    assert!(!arm.contains(&"m5.large".to_string()), "{arm:?}");
    assert!(!arm.contains(&"m7g.xlarge".to_string()), "{arm:?}");
    assert!(
        !arm.contains(&"t4g.large".to_string()),
        "burstable: {arm:?}"
    );
    let with_burstable = found(&[("InstanceRequirements.BurstablePerformance", "included")]);
    assert!(with_burstable.contains(&"t4g.large".to_string()));
    let only_burstable = found(&[("InstanceRequirements.BurstablePerformance", "required")]);
    assert_eq!(only_burstable, ["t4g.large"]);
}

#[test]
fn requirements_honor_the_other_catalog_backed_fields() {
    let has =
        |params: &[(&str, &str)], t: &str| types_from_requirements(params).contains(&t.to_string());
    assert!(!has(&[], "m5.metal"), "bare metal is excluded by default");
    assert!(has(
        &[("InstanceRequirements.BareMetal", "included")],
        "m5.metal"
    ));
    assert!(has(&[], "c1.medium"));
    assert!(!has(
        &[("InstanceRequirements.InstanceGeneration.1", "current")],
        "c1.medium"
    ));
    assert!(!has(
        &[("InstanceRequirements.CpuManufacturer.1", "amd")],
        "m5.large"
    ));
    assert!(has(
        &[("InstanceRequirements.CpuManufacturer.1", "intel")],
        "m5.large"
    ));
    assert!(!has(
        &[("InstanceRequirements.ExcludedInstanceType.1", "m5.*")],
        "m5.large"
    ));
    assert_eq!(
        types_from_requirements(&[("InstanceRequirements.AllowedInstanceType.1", "m7g.large")]),
        ["m7g.large"]
    );
    assert!(!has(
        &[("InstanceRequirements.LocalStorage", "required")],
        "m5.large"
    ));
    assert!(has(
        &[("InstanceRequirements.LocalStorage", "excluded")],
        "m5.large"
    ));
    assert!(!has(
        &[("InstanceRequirements.LocalStorage", "excluded")],
        "i3.large"
    ));
    assert!(!has(
        &[("InstanceRequirements.LocalStorageType.1", "ssd")],
        "d3.xlarge"
    ));
    assert!(!has(
        &[("InstanceRequirements.NetworkInterfaceCount.Min", "8")],
        "m5.large"
    ));
    assert!(!has(
        &[("InstanceRequirements.MemoryGiBPerVCpu.Min", "8")],
        "m5.large"
    ));
    assert!(has(
        &[("InstanceRequirements.BaselineEbsBandwidthMbps.Min", "100")],
        "m7g.large"
    ));
    assert!(!has(
        &[("InstanceRequirements.NetworkBandwidthGbps.Min", "100")],
        "m7g.large"
    ));
    assert!(has(
        &[("InstanceRequirements.TotalLocalStorageGB.Min", "1000")],
        "d3.xlarge"
    ));
    assert!(!has(
        &[("InstanceRequirements.TotalLocalStorageGB.Min", "1000")],
        "m7g.large"
    ));
}

#[test]
fn requirements_need_the_modeled_required_members() {
    let run = |params: &[(&str, &str)]| {
        instance::get_instance_types_from_requirements(
            &Ec2Service::new(),
            &req("GetInstanceTypesFromInstanceRequirements", params),
        )
    };
    let vcpu = ("InstanceRequirements.VCpuCount.Min", "1");
    let mem = ("InstanceRequirements.MemoryMiB.Min", "1");
    let arch = ("ArchitectureType.1", "x86_64");
    let virt = ("VirtualizationType.1", "hvm");
    assert_eq!(code(run(&[vcpu, mem, virt])), "MissingParameter");
    assert_eq!(code(run(&[vcpu, mem, arch])), "MissingParameter");
    assert_eq!(code(run(&[mem, arch, virt])), "MissingParameter");
    assert_eq!(code(run(&[vcpu, arch, virt])), "MissingParameter");
    assert_eq!(
        code(run(&[
            vcpu,
            mem,
            arch,
            virt,
            ("InstanceRequirements.BareMetal", "maybe"),
        ])),
        "InvalidParameterValue"
    );
    assert!(run(&[vcpu, mem, arch, virt]).is_ok());
}

#[test]
fn requirements_honor_the_accelerator_fields() {
    let has =
        |params: &[(&str, &str)], t: &str| types_from_requirements(params).contains(&t.to_string());
    let count_max_0 = ("InstanceRequirements.AcceleratorCount.Max", "0");
    assert!(has(&[count_max_0], "m5.large"));
    assert!(!has(&[count_max_0], "g4dn.xlarge"));
    // Inference chips have a real count now, so 0 excludes them.
    assert!(!has(&[count_max_0], "inf1.xlarge"));
    // As does a fractional GPU's logical count, and a Trainium.
    assert!(!has(&[count_max_0], "g6f.large"));
    assert!(!has(&[count_max_0], "gr6f.4xlarge"));
    assert!(!has(&[count_max_0], "trn1.2xlarge"));
    assert!(!has(&[count_max_0], "vt1.3xlarge"));
    let gpu = ("InstanceRequirements.AcceleratorType.1", "gpu");
    assert!(has(&[gpu], "g4dn.xlarge"));
    assert!(!has(&[gpu], "f2.12xlarge"));
    assert!(!has(&[gpu], "m5.large"));
    let inference = ("InstanceRequirements.AcceleratorType.1", "inference");
    assert!(has(&[inference], "inf1.xlarge"));
    // Recorded: only Inferentia is `inference`, Trainium is not.
    assert!(!has(&[inference], "trn1.2xlarge"));
    let media = ("InstanceRequirements.AcceleratorType.1", "media");
    assert!(has(&[media], "vt1.3xlarge"));
    let nvidia = ("InstanceRequirements.AcceleratorManufacturer.1", "nvidia");
    assert!(has(&[nvidia], "g4dn.xlarge"));
    assert!(!has(&[nvidia], "inf1.xlarge"));
    let amazon = (
        "InstanceRequirements.AcceleratorManufacturer.1",
        "amazon-web-services",
    );
    assert!(has(&[amazon], "inf1.xlarge"));
    assert!(has(&[amazon], "trn1.2xlarge"));
    let t4 = ("InstanceRequirements.AcceleratorName.1", "t4");
    assert!(has(&[t4], "g4dn.xlarge"));
    assert!(!has(&[t4], "g5g.xlarge"));
    let inferentia = ("InstanceRequirements.AcceleratorName.1", "inferentia");
    assert!(has(&[inferentia], "inf1.xlarge"));
    let memory = (
        "InstanceRequirements.AcceleratorTotalMemoryMiB.Min",
        "16384",
    );
    assert!(has(&[memory], "g4dn.xlarge"));
    assert!(!has(&[memory], "m5.large"));
    // An FPGA's memory is real too.
    assert!(has(&[memory], "f2.12xlarge"));
    let count_min_2 = ("InstanceRequirements.AcceleratorCount.Min", "2");
    assert!(has(&[count_min_2], "f2.12xlarge"));
    assert!(!has(&[count_min_2], "g4dn.xlarge"));
}

#[test]
fn a_known_diskless_type_has_zero_local_storage() {
    let none = ("InstanceRequirements.TotalLocalStorageGB.Max", "0");
    let found = types_from_requirements(&[none]);
    assert!(found.contains(&"m5.large".to_string()));
    assert!(!found.contains(&"i3.large".to_string()));
}

#[test]
fn requirements_validate_their_lists() {
    let base = [
        ("ArchitectureType.1", "x86_64"),
        ("VirtualizationType.1", "hvm"),
        ("InstanceRequirements.VCpuCount.Min", "1"),
        ("InstanceRequirements.MemoryMiB.Min", "1"),
    ];
    let run = |extra: &[(&str, &str)]| {
        let mut all = base.to_vec();
        all.extend_from_slice(extra);
        instance::get_instance_types_from_requirements(
            &Ec2Service::new(),
            &req("GetInstanceTypesFromInstanceRequirements", &all),
        )
    };
    for bad in [
        "InstanceRequirements.CpuManufacturer.1",
        "InstanceRequirements.InstanceGeneration.1",
        "InstanceRequirements.LocalStorageType.1",
        "InstanceRequirements.AcceleratorType.1",
        "InstanceRequirements.AcceleratorManufacturer.1",
        "InstanceRequirements.AcceleratorName.1",
    ] {
        assert_eq!(
            code(run(&[(bad, "nope")])),
            "InvalidParameterValue",
            "{bad}"
        );
    }
    assert_eq!(
        code(instance::get_instance_types_from_requirements(
            &Ec2Service::new(),
            &req(
                "GetInstanceTypesFromInstanceRequirements",
                &[
                    ("ArchitectureType.1", "nope"),
                    ("VirtualizationType.1", "hvm"),
                    ("InstanceRequirements.VCpuCount.Min", "1"),
                    ("InstanceRequirements.MemoryMiB.Min", "1"),
                ],
            ),
        )),
        "InvalidParameterValue"
    );
    assert_eq!(
        code(run(&[
            ("InstanceRequirements.AllowedInstanceType.1", "m5.*"),
            ("InstanceRequirements.ExcludedInstanceType.1", "c5.*"),
        ])),
        "InvalidParameterCombination"
    );
}

#[test]
fn requirements_refuse_a_bound_they_cannot_parse() {
    let run = |extra: &[(&str, &str)]| {
        let mut all = vec![
            ("ArchitectureType.1", "x86_64"),
            ("VirtualizationType.1", "hvm"),
        ];
        all.extend_from_slice(extra);
        instance::get_instance_types_from_requirements(
            &Ec2Service::new(),
            &req("GetInstanceTypesFromInstanceRequirements", &all),
        )
    };
    let vcpu = ("InstanceRequirements.VCpuCount.Min", "1");
    let mem = ("InstanceRequirements.MemoryMiB.Min", "1");
    for (member, value) in [
        ("InstanceRequirements.VCpuCount.Min", "oops"),
        ("InstanceRequirements.VCpuCount.Max", "2.5"),
        ("InstanceRequirements.MemoryMiB.Max", "99999999999"),
        ("InstanceRequirements.NetworkInterfaceCount.Min", "1.5"),
        ("InstanceRequirements.BaselineEbsBandwidthMbps.Max", "x"),
        ("InstanceRequirements.AcceleratorCount.Min", "1.5"),
        ("InstanceRequirements.AcceleratorTotalMemoryMiB.Max", "x"),
        ("InstanceRequirements.MemoryGiBPerVCpu.Min", "oops"),
        ("InstanceRequirements.TotalLocalStorageGB.Max", "NaN"),
        ("InstanceRequirements.NetworkBandwidthGbps.Min", "inf"),
    ] {
        let params = if member.ends_with("VCpuCount.Min") {
            vec![mem, (member, value)]
        } else {
            vec![vcpu, mem, (member, value)]
        };
        assert_eq!(
            code(run(&params)),
            "InvalidParameterValue",
            "{member}={value}"
        );
    }
    // The double members take fractions, and an integer form of them.
    for (member, value) in [
        ("InstanceRequirements.MemoryGiBPerVCpu.Min", "0.5"),
        ("InstanceRequirements.TotalLocalStorageGB.Min", "1"),
        ("InstanceRequirements.NetworkBandwidthGbps.Max", "12.5"),
    ] {
        assert!(run(&[vcpu, mem, (member, value)]).is_ok(), "{member}");
    }
}

#[test]
fn a_local_storage_type_needs_a_disk_of_that_type() {
    let ssd = types_from_requirements(&[("InstanceRequirements.LocalStorageType.1", "ssd")]);
    assert!(ssd.contains(&"i3.large".to_string()));
    assert!(!ssd.contains(&"d3.xlarge".to_string()), "hdd");
    assert!(!ssd.contains(&"m5.large".to_string()), "known diskless");
    assert!(!ssd.contains(&"p3.8xlarge".to_string()), "unknown storage");
    let hdd = types_from_requirements(&[("InstanceRequirements.LocalStorageType.1", "hdd")]);
    assert!(hdd.contains(&"d3.xlarge".to_string()));
    assert!(!hdd.contains(&"m5.large".to_string()), "known diskless");
    // With no list asked for, a type without disks is still in.
    assert!(types_from_requirements(&[]).contains(&"m5.large".to_string()));
}

#[test]
fn requirements_max_results_must_make_progress() {
    let run = |size: &str| {
        instance::get_instance_types_from_requirements(
            &Ec2Service::new(),
            &req(
                "GetInstanceTypesFromInstanceRequirements",
                &[
                    ("ArchitectureType.1", "x86_64"),
                    ("VirtualizationType.1", "hvm"),
                    ("InstanceRequirements.VCpuCount.Min", "1"),
                    ("InstanceRequirements.MemoryMiB.Min", "1"),
                    ("MaxResults", size),
                ],
            ),
        )
    };
    for bad in ["0", "-1", "2147483648"] {
        assert_eq!(code(run(bad)), "InvalidParameterValue", "{bad}");
    }
    assert_eq!(items_of(&body(run("1"))).len(), 1);
}

/// Settled by EC2's own record of dl1.24xlarge: `GpuInfo`, Habana, Gaudi HL-205. No
/// Region the catalog knows sells it, so it is asked of one the catalog does not.
#[test]
fn dl1_counts_as_a_habana_gpu_not_an_inference_chip() {
    let has = |requirement: (&str, &str)| {
        let mut r = req(
            "GetInstanceTypesFromInstanceRequirements",
            &[
                ("ArchitectureType.1", "x86_64"),
                ("VirtualizationType.1", "hvm"),
                ("InstanceRequirements.VCpuCount.Min", "96"),
                ("InstanceRequirements.VCpuCount.Max", "96"),
                ("InstanceRequirements.MemoryMiB.Min", "1"),
                requirement,
            ],
        );
        r.region = "cn-north-1".to_string();
        items_of(&body(instance::get_instance_types_from_requirements(
            &Ec2Service::new(),
            &r,
        )))
        .contains(&"dl1.24xlarge".to_string())
    };
    assert!(has(("InstanceRequirements.AcceleratorType.1", "gpu")));
    assert!(!has((
        "InstanceRequirements.AcceleratorType.1",
        "inference"
    )));
    assert!(has((
        "InstanceRequirements.AcceleratorManufacturer.1",
        "habana"
    )));
    assert!(has((
        "InstanceRequirements.AcceleratorName.1",
        "gaudi-hl-205"
    )));
    assert!(has(("InstanceRequirements.AcceleratorCount.Min", "8")));
    assert!(!has(("InstanceRequirements.AcceleratorCount.Min", "9")));
}

#[test]
fn ipv6_addresses_per_interface_filters_on_the_reported_value() {
    let names = |value: &str| {
        items_of(&describe_types(&[
            ("Filter.1.Name", "network-info.ipv6-addresses-per-interface"),
            ("Filter.1.Value.1", value),
        ]))
    };
    let zero = names("0");
    assert!(zero.contains(&"c1.medium".to_string()), "{zero:?}");
    assert!(!zero.contains(&"m7g.large".to_string()), "{zero:?}");
    // What the response says, the filter finds.
    for name in &zero {
        let one = describe_types(&[("InstanceType.1", name)]);
        assert!(
            one.contains("<ipv6AddressesPerInterface>0<"),
            "{name}: {one}"
        );
    }
    assert!(names("10").contains(&"m5.large".to_string()));
}

#[test]
fn the_three_catalog_operations_agree_on_the_region() {
    let in_region = |action: &str, region: &str, params: &[(&str, &str)]| {
        let mut r = req(action, params);
        r.region = region.to_string();
        r
    };
    let svc = Ec2Service::new();
    let only_gov = [("InstanceType.1", "i3p.16xlarge")];

    // Not sold in this Region: unknown here, unless the caller asks for it.
    let r = in_region("DescribeInstanceTypes", "us-east-1", &only_gov);
    assert_eq!(
        code(instance::describe_instance_types(&svc, &r)),
        "InvalidInstanceType"
    );
    let r = in_region(
        "DescribeInstanceTypes",
        "us-east-1",
        &[
            ("InstanceType.1", "i3p.16xlarge"),
            ("IncludeUnsupportedInRegion", "true"),
        ],
    );
    let out = body(instance::describe_instance_types(&svc, &r));
    assert!(out.contains("<supportedInRegion>false<"), "{out}");
    let r = in_region("DescribeInstanceTypes", "us-gov-west-1", &only_gov);
    let out = body(instance::describe_instance_types(&svc, &r));
    assert!(out.contains("<supportedInRegion>true<"), "{out}");

    let requirements = [
        ("ArchitectureType.1", "x86_64"),
        ("VirtualizationType.1", "hvm"),
        ("InstanceRequirements.VCpuCount.Min", "1"),
        ("InstanceRequirements.MemoryMiB.Min", "1"),
    ];
    let names = |region: &str| {
        let types = items_of(&all_pages(
            instance::describe_instance_types,
            "DescribeInstanceTypes",
            &[],
            Some(region),
        ));
        let offerings = items_of(&all_pages(
            crate::service::rest::describe_instance_type_offerings,
            "DescribeInstanceTypeOfferings",
            &[],
            Some(region),
        ));
        let from_requirements = items_of(&body(instance::get_instance_types_from_requirements(
            &svc,
            &in_region(
                "GetInstanceTypesFromInstanceRequirements",
                region,
                &requirements,
            ),
        )));
        (types, offerings, from_requirements)
    };
    for region in ["us-east-1", "us-west-2", "cn-north-1"] {
        let (types, offerings, from_requirements) = names(region);
        assert_eq!(types, offerings, "{region}");
        assert!(
            from_requirements.iter().all(|t| types.contains(t)),
            "{region}"
        );
        assert!(types.contains(&"m5.large".to_string()), "{region}");
    }
    // A Region the dataset does not know gets the whole catalog on all three.
    let (types, offerings, _) = names("cn-north-1");
    assert_eq!(types.len(), crate::instance_types::catalog().len());
    assert_eq!(offerings.len(), types.len());
    assert!(names("us-east-1").0.len() < types.len());
    assert!(!names("us-east-1").0.contains(&"i3p.16xlarge".to_string()));
}

#[test]
fn instance_type_offerings_follow_the_catalog() {
    let offerings = |params: &[(&str, &str)]| {
        all_pages(
            crate::service::rest::describe_instance_type_offerings,
            "DescribeInstanceTypeOfferings",
            params,
            None,
        )
    };
    let in_region = offerings(&[]);
    assert!(in_region.contains("<instanceType>m7g.large</instanceType>"));
    assert!(in_region.contains("<locationType>region</locationType>"));
    let one = offerings(&[
        ("LocationType", "availability-zone"),
        ("Filter.1.Name", "instance-type"),
        ("Filter.1.Value.1", "m7g.large"),
        ("Filter.2.Name", "location"),
        ("Filter.2.Value.1", "*b"),
    ]);
    assert_eq!(items_of(&one), ["m7g.large"], "{one}");
    assert!(one.contains("<locationType>availability-zone</locationType>"));
    assert!(one.contains("b</location>"), "{one}");
    // An account here has no Outposts, so none of them offers anything.
    assert!(items_of(&offerings(&[("LocationType", "outpost")])).is_empty());
    // An empty value is absent, as `validate_enum` treats it: the default.
    let empty = offerings(&[("LocationType", "")]);
    assert_eq!(items_of(&empty), items_of(&in_region));
    assert!(
        empty.contains("<locationType>region</locationType>"),
        "{empty}"
    );
    let r = crate::service::rest::describe_instance_type_offerings(
        &Ec2Service::new(),
        &req(
            "DescribeInstanceTypeOfferings",
            &[("Filter.1.Name", "nope"), ("Filter.1.Value.1", "x")],
        ),
    );
    assert_eq!(code(r), "InvalidParameterValue");
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
async fn an_unknown_instance_type_is_refused() {
    let one = [
        ("InstanceType", "m5.huge"),
        ("MinCount", "1"),
        ("MaxCount", "1"),
    ];
    // Whether or not a quota is enforced.
    for svc in [
        seeded(Ec2Service::new()),
        seeded(enforcing(&[(ON_DEMAND_STANDARD, 100.0)])),
    ] {
        let e = crate::test_support::err_of(run(&svc, &one).await);
        assert_eq!(e.code(), "InvalidParameterValue");
        assert_eq!(e.message(), "Invalid value 'm5.huge' for InstanceType.");
    }
    // Spot requests only while their quota is.
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
    // A launch refuses such a type, but an attribute change does not check it.
    let svc = seeded(Ec2Service::new());
    let id = xml(
        &body(
            run(
                &svc,
                &[
                    ("InstanceType", "t3.micro"),
                    ("MinCount", "1"),
                    ("MaxCount", "1"),
                ],
            )
            .await,
        ),
        "instanceId",
    );
    body(instance::stop_instances(&svc, &req("StopInstances", &[("InstanceId.1", &id)])).await);
    body(instance::modify_instance_attribute(
        &svc,
        &req(
            "ModifyInstanceAttribute",
            &[("InstanceId", &id), ("InstanceType.Value", "m5.huge")],
        ),
    ));
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
