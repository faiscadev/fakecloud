//! EC2 VPC control-plane field-presence: default resources, IPv6 CIDR
//! generation, subnet DNS options, and ENI private DNS / default SG.

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn create_vpc_provisions_default_resources() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    let vpc = c
        .create_vpc()
        .cidr_block("10.30.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();

    // A `default` security group exists for the new VPC.
    let sgs = c
        .describe_security_groups()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("vpc-id")
                .values(&vpc_id)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(sgs
        .security_groups()
        .iter()
        .any(|g| g.group_name() == Some("default")));

    // A default network ACL exists for the new VPC.
    let acls = c
        .describe_network_acls()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("vpc-id")
                .values(&vpc_id)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(acls
        .network_acls()
        .iter()
        .any(|a| a.is_default() == Some(true)));

    // A main route table exists for the new VPC.
    let rts = c
        .describe_route_tables()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("vpc-id")
                .values(&vpc_id)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(rts
        .route_tables()
        .iter()
        .any(|rt| rt.associations().iter().any(|a| a.main() == Some(true))));
}

#[tokio::test]
async fn create_vpc_with_amazon_provided_ipv6() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    let vpc = c
        .create_vpc()
        .cidr_block("10.31.0.0/16")
        .amazon_provided_ipv6_cidr_block(true)
        .send()
        .await
        .unwrap();
    let set = vpc.vpc().unwrap().ipv6_cidr_block_association_set();
    assert_eq!(set.len(), 1);
    assert!(set[0].ipv6_cidr_block().unwrap().ends_with("::/56"));

    // Associating IPv6 separately works too, and the association is filterable.
    let vpc2 = c
        .create_vpc()
        .cidr_block("10.32.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc2_id = vpc2.vpc().unwrap().vpc_id().unwrap().to_string();
    let assoc = c
        .associate_vpc_cidr_block()
        .vpc_id(&vpc2_id)
        .amazon_provided_ipv6_cidr_block(true)
        .send()
        .await
        .unwrap();
    let assoc_id = assoc
        .ipv6_cidr_block_association()
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();
    let described = c
        .describe_vpcs()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("ipv6-cidr-block-association.association-id")
                .values(&assoc_id)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(described.vpcs().len(), 1);
    assert_eq!(described.vpcs()[0].vpc_id(), Some(vpc2_id.as_str()));
}

#[tokio::test]
async fn subnet_reports_private_dns_hostname_type() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    let vpc = c
        .create_vpc()
        .cidr_block("10.33.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();
    let subnet = c
        .create_subnet()
        .vpc_id(&vpc_id)
        .cidr_block("10.33.1.0/24")
        .send()
        .await
        .unwrap();
    assert_eq!(
        subnet
            .subnet()
            .unwrap()
            .private_dns_name_options_on_launch()
            .and_then(|o| o.hostname_type())
            .map(|h| h.as_str()),
        Some("ip-name")
    );
}

#[tokio::test]
async fn network_interface_derives_private_dns_and_default_sg() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    let vpc = c
        .create_vpc()
        .cidr_block("10.34.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();
    let subnet = c
        .create_subnet()
        .vpc_id(&vpc_id)
        .cidr_block("10.34.0.0/24")
        .send()
        .await
        .unwrap();
    let subnet_id = subnet.subnet().unwrap().subnet_id().unwrap().to_string();

    let eni = c
        .create_network_interface()
        .subnet_id(&subnet_id)
        .private_ip_address("10.34.0.20")
        .send()
        .await
        .unwrap();
    let n = eni.network_interface().unwrap();
    assert_eq!(n.private_dns_name(), Some("ip-10-34-0-20.ec2.internal"));
    // No SecurityGroupId was given, so the VPC's default SG is attached.
    assert_eq!(n.groups().len(), 1);
}

#[tokio::test]
async fn subnet_ipv6_association_and_assign_on_creation() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    let vpc = c
        .create_vpc()
        .cidr_block("10.50.0.0/16")
        .amazon_provided_ipv6_cidr_block(true)
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();

    let subnet = c
        .create_subnet()
        .vpc_id(&vpc_id)
        .cidr_block("10.50.1.0/24")
        .ipv6_cidr_block("2600:1f16:abc:1::/64")
        .send()
        .await
        .unwrap();
    let subnet_id = subnet.subnet().unwrap().subnet_id().unwrap().to_string();
    let set = subnet.subnet().unwrap().ipv6_cidr_block_association_set();
    assert_eq!(set.len(), 1);
    let assoc_id = set[0].association_id().unwrap().to_string();

    // ModifySubnetAttribute flips AssignIpv6AddressOnCreation, which must then
    // round-trip on DescribeSubnets (the resource waits for `true`).
    c.modify_subnet_attribute()
        .subnet_id(&subnet_id)
        .assign_ipv6_address_on_creation(
            aws_sdk_ec2::types::AttributeBooleanValue::builder()
                .value(true)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // The association is filterable, and the assign flag persists on read.
    let described = c
        .describe_subnets()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("ipv6-cidr-block-association.association-id")
                .values(&assoc_id)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(described.subnets().len(), 1);
    assert_eq!(described.subnets()[0].subnet_id(), Some(subnet_id.as_str()));
    assert_eq!(
        described.subnets()[0].assign_ipv6_address_on_creation(),
        Some(true)
    );
}

#[tokio::test]
async fn security_group_all_traffic_rule_omits_ports() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    let vpc = c
        .create_vpc()
        .cidr_block("10.51.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();

    let sg = c
        .create_security_group()
        .group_name("all-traffic-sg")
        .description("d")
        .vpc_id(&vpc_id)
        .send()
        .await
        .unwrap();
    let sg_id = sg.group_id().unwrap().to_string();

    c.authorize_security_group_egress()
        .group_id(&sg_id)
        .ip_permissions(
            aws_sdk_ec2::types::IpPermission::builder()
                .ip_protocol("-1")
                .ip_ranges(
                    aws_sdk_ec2::types::IpRange::builder()
                        .cidr_ip("0.0.0.0/0")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let described = c
        .describe_security_groups()
        .group_ids(&sg_id)
        .send()
        .await
        .unwrap();
    let egress = described.security_groups()[0].ip_permissions_egress();
    // The all-traffic (`-1`) rule must report no port range (AWS omits them).
    let all = egress
        .iter()
        .find(|p| p.ip_protocol() == Some("-1"))
        .expect("all-traffic egress rule");
    assert!(all.from_port().is_none());
    assert!(all.to_port().is_none());
}

#[tokio::test]
async fn subnet_auto_associates_with_default_nacl() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    let vpc = c
        .create_vpc()
        .cidr_block("10.60.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();
    let subnet = c
        .create_subnet()
        .vpc_id(&vpc_id)
        .cidr_block("10.60.1.0/24")
        .send()
        .await
        .unwrap();
    let subnet_id = subnet.subnet().unwrap().subnet_id().unwrap().to_string();

    // DescribeNetworkAcls filtered by the subnet resolves the one default NACL.
    let acls = c
        .describe_network_acls()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("association.subnet-id")
                .values(&subnet_id)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(acls.network_acls().len(), 1);
    assert_eq!(acls.network_acls()[0].is_default(), Some(true));
}

#[tokio::test]
async fn replace_route_table_association_moves_main_to_new_table() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    let vpc = c
        .create_vpc()
        .cidr_block("10.61.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();

    // The VPC's default main route table.
    let main = c
        .describe_route_tables()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("vpc-id")
                .values(&vpc_id)
                .build(),
        )
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("association.main")
                .values("true")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(main.route_tables().len(), 1);
    let main_assoc = main.route_tables()[0].associations()[0]
        .route_table_association_id()
        .unwrap()
        .to_string();

    // A fresh route table to become the new main.
    let new_rt = c.create_route_table().vpc_id(&vpc_id).send().await.unwrap();
    let new_rt_id = new_rt
        .route_table()
        .unwrap()
        .route_table_id()
        .unwrap()
        .to_string();

    c.replace_route_table_association()
        .association_id(&main_assoc)
        .route_table_id(&new_rt_id)
        .send()
        .await
        .unwrap();

    // The main association now resolves to the new route table only.
    let after = c
        .describe_route_tables()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("vpc-id")
                .values(&vpc_id)
                .build(),
        )
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("association.main")
                .values("true")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(after.route_tables().len(), 1);
    assert_eq!(
        after.route_tables()[0].route_table_id(),
        Some(new_rt_id.as_str())
    );
}

#[tokio::test]
async fn associate_address_rejects_unknown_allocation_id() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    // A real allocation associates fine.
    let alloc = c
        .allocate_address()
        .domain(aws_sdk_ec2::types::DomainType::Vpc)
        .send()
        .await
        .expect("allocate_address");
    let alloc_id = alloc.allocation_id().expect("allocation_id").to_string();
    let ok = c
        .associate_address()
        .allocation_id(&alloc_id)
        .network_interface_id("eni-00000000000000000")
        .send()
        .await
        .expect("associate real allocation");
    assert!(ok.association_id().is_some());

    // An unknown AllocationId must error, not fabricate a phantom association.
    let err = c
        .associate_address()
        .allocation_id("eipalloc-deadbeefdeadbeef0")
        .network_interface_id("eni-00000000000000000")
        .send()
        .await
        .expect_err("unknown allocation must be rejected");
    // EC2's query-protocol error envelope isn't parsed into meta().code() by
    // aws-sdk-ec2 (a separate, pre-existing format gap), so assert on the raw
    // error text which carries the code.
    let msg = format!("{}", aws_sdk_ec2::error::DisplayErrorContext(&err));
    assert!(
        msg.contains("InvalidAllocationID.NotFound"),
        "expected InvalidAllocationID.NotFound, got: {msg}"
    );
}
