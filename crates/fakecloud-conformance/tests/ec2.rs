//! Handwritten Level-2 conformance tests for EC2 (ec2Query protocol).
//!
//! One `#[test_action]` per declared `SUPPORTED_ACTIONS` entry — the audit
//! cross-checks this list against the service crate and fails the build on any
//! gap. Grows one resource-family batch at a time toward full op parity.

#![recursion_limit = "512"]

mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("ec2", "CreateTags", checksum = "3557d13e")]
#[tokio::test]
async fn ec2_create_tags() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;

    client
        .create_tags()
        .resources("vpc-0123456789abcdef0")
        .tags(
            aws_sdk_ec2::types::Tag::builder()
                .key("Name")
                .value("web")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Tag is now visible via DescribeTags (round-trip through the ec2Query
    // flattened-list request encoder + response serializer).
    let described = client.describe_tags().send().await.unwrap();
    assert!(described
        .tags()
        .iter()
        .any(|t| t.key() == Some("Name") && t.value() == Some("web")));
}

#[test_action("ec2", "DeleteTags", checksum = "112f144c")]
#[tokio::test]
async fn ec2_delete_tags() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;

    client
        .create_tags()
        .resources("i-0123456789abcdef0")
        .tags(
            aws_sdk_ec2::types::Tag::builder()
                .key("env")
                .value("prod")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Key-only delete removes the tag regardless of value.
    client
        .delete_tags()
        .resources("i-0123456789abcdef0")
        .tags(aws_sdk_ec2::types::Tag::builder().key("env").build())
        .send()
        .await
        .unwrap();

    let described = client.describe_tags().send().await.unwrap();
    assert!(!described.tags().iter().any(|t| t.key() == Some("env")));
}

#[test_action("ec2", "DescribeTags", checksum = "aa62d5db")]
#[tokio::test]
async fn ec2_describe_tags() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;

    client
        .create_tags()
        .resources("subnet-0123456789abcdef0")
        .tags(
            aws_sdk_ec2::types::Tag::builder()
                .key("tier")
                .value("private")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Filter by resource-id returns exactly the tag we set, with the
    // inferred resource-type.
    let described = client
        .describe_tags()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("resource-id")
                .values("subnet-0123456789abcdef0")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let tags = described.tags();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), Some("tier"));
    assert_eq!(
        tags[0].resource_type(),
        Some(&aws_sdk_ec2::types::ResourceType::Subnet)
    );
}

#[test_action("ec2", "DescribeRegions", checksum = "a618a443")]
#[tokio::test]
async fn ec2_describe_regions() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;

    let response = client.describe_regions().send().await.unwrap();
    let regions = response.regions();
    assert!(regions.len() >= 15);
    let us_east_1 = regions
        .iter()
        .find(|r| r.region_name() == Some("us-east-1"))
        .expect("us-east-1 present");
    assert_eq!(us_east_1.endpoint(), Some("ec2.us-east-1.amazonaws.com"));

    // Explicit region-name filter narrows the result.
    let filtered = client
        .describe_regions()
        .region_names("eu-west-1")
        .send()
        .await
        .unwrap();
    assert_eq!(filtered.regions().len(), 1);
    assert_eq!(filtered.regions()[0].region_name(), Some("eu-west-1"));
}

#[test_action("ec2", "DescribeAvailabilityZones", checksum = "375077ee")]
#[tokio::test]
async fn ec2_describe_availability_zones() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;

    let response = client.describe_availability_zones().send().await.unwrap();
    let zones = response.availability_zones();
    assert_eq!(zones.len(), 3);
    assert!(zones.iter().all(|z| z.region_name() == Some("us-east-1")));
    assert!(zones.iter().any(|z| z.zone_name() == Some("us-east-1a")));
    let a = zones
        .iter()
        .find(|z| z.zone_name() == Some("us-east-1a"))
        .unwrap();
    assert_eq!(a.zone_id(), Some("use1-az1"));
}

#[test_action("ec2", "DescribeAccountAttributes", checksum = "62e5ea8d")]
#[tokio::test]
async fn ec2_describe_account_attributes() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;

    let response = client.describe_account_attributes().send().await.unwrap();
    let attrs = response.account_attributes();
    let supported = attrs
        .iter()
        .find(|a| a.attribute_name() == Some("supported-platforms"))
        .expect("supported-platforms present");
    assert!(supported
        .attribute_values()
        .iter()
        .any(|v| v.attribute_value() == Some("VPC")));
}

// ---- VPCs ----

#[test_action("ec2", "CreateVpc", checksum = "d6e43867")]
#[tokio::test]
async fn ec2_create_vpc() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let vpc = client
        .create_vpc()
        .cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    let v = vpc.vpc().unwrap();
    assert!(v.vpc_id().unwrap().starts_with("vpc-"));
    assert_eq!(v.cidr_block(), Some("10.0.0.0/16"));
    assert_eq!(v.state(), Some(&aws_sdk_ec2::types::VpcState::Available));
}

#[test_action("ec2", "CreateDefaultVpc", checksum = "e508c9f8")]
#[tokio::test]
async fn ec2_create_default_vpc() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let vpc = client.create_default_vpc().send().await.unwrap();
    assert_eq!(vpc.vpc().unwrap().is_default(), Some(true));
}

#[test_action("ec2", "DescribeVpcs", checksum = "970ba030")]
#[tokio::test]
async fn ec2_describe_vpcs() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let created = client
        .create_vpc()
        .cidr_block("10.1.0.0/16")
        .send()
        .await
        .unwrap();
    let id = created.vpc().unwrap().vpc_id().unwrap().to_string();

    let resp = client.describe_vpcs().vpc_ids(&id).send().await.unwrap();
    let vpcs = resp.vpcs();
    assert_eq!(vpcs.len(), 1);
    assert_eq!(vpcs[0].vpc_id(), Some(id.as_str()));
}

#[test_action("ec2", "DeleteVpc", checksum = "6338344b")]
#[tokio::test]
async fn ec2_delete_vpc() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let created = client
        .create_vpc()
        .cidr_block("10.2.0.0/16")
        .send()
        .await
        .unwrap();
    let id = created.vpc().unwrap().vpc_id().unwrap().to_string();
    client.delete_vpc().vpc_id(&id).send().await.unwrap();
    let resp = client.describe_vpcs().send().await.unwrap();
    assert!(!resp.vpcs().iter().any(|v| v.vpc_id() == Some(id.as_str())));
}

#[test_action("ec2", "ModifyVpcAttribute", checksum = "08de0eb7")]
#[tokio::test]
async fn ec2_modify_vpc_attribute() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let created = client
        .create_vpc()
        .cidr_block("10.3.0.0/16")
        .send()
        .await
        .unwrap();
    let id = created.vpc().unwrap().vpc_id().unwrap().to_string();
    client
        .modify_vpc_attribute()
        .vpc_id(&id)
        .enable_dns_hostnames(
            aws_sdk_ec2::types::AttributeBooleanValue::builder()
                .value(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let attr = client
        .describe_vpc_attribute()
        .vpc_id(&id)
        .attribute(aws_sdk_ec2::types::VpcAttributeName::EnableDnsHostnames)
        .send()
        .await
        .unwrap();
    assert_eq!(
        attr.enable_dns_hostnames().and_then(|v| v.value()),
        Some(true)
    );
}

#[test_action("ec2", "DescribeVpcAttribute", checksum = "ece38141")]
#[tokio::test]
async fn ec2_describe_vpc_attribute() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let created = client
        .create_vpc()
        .cidr_block("10.4.0.0/16")
        .send()
        .await
        .unwrap();
    let id = created.vpc().unwrap().vpc_id().unwrap().to_string();
    let attr = client
        .describe_vpc_attribute()
        .vpc_id(&id)
        .attribute(aws_sdk_ec2::types::VpcAttributeName::EnableDnsSupport)
        .send()
        .await
        .unwrap();
    assert_eq!(
        attr.enable_dns_support().and_then(|v| v.value()),
        Some(true)
    );
}

#[test_action("ec2", "ModifyVpcTenancy", checksum = "bbc0fe05")]
#[tokio::test]
async fn ec2_modify_vpc_tenancy() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let created = client
        .create_vpc()
        .cidr_block("10.5.0.0/16")
        .send()
        .await
        .unwrap();
    let id = created.vpc().unwrap().vpc_id().unwrap().to_string();
    let resp = client
        .modify_vpc_tenancy()
        .vpc_id(&id)
        .instance_tenancy(aws_sdk_ec2::types::VpcTenancy::Default)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.return_value(), Some(true));
}

#[test_action("ec2", "AssociateVpcCidrBlock", checksum = "9e813022")]
#[tokio::test]
async fn ec2_associate_vpc_cidr_block() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let created = client
        .create_vpc()
        .cidr_block("10.6.0.0/16")
        .send()
        .await
        .unwrap();
    let id = created.vpc().unwrap().vpc_id().unwrap().to_string();
    let resp = client
        .associate_vpc_cidr_block()
        .vpc_id(&id)
        .cidr_block("10.7.0.0/16")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.vpc_id(), Some(id.as_str()));
    assert!(resp
        .cidr_block_association()
        .unwrap()
        .association_id()
        .unwrap()
        .starts_with("vpc-cidr-assoc-"));
}

#[test_action("ec2", "DisassociateVpcCidrBlock", checksum = "056b5875")]
#[tokio::test]
async fn ec2_disassociate_vpc_cidr_block() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let created = client
        .create_vpc()
        .cidr_block("10.8.0.0/16")
        .send()
        .await
        .unwrap();
    let id = created.vpc().unwrap().vpc_id().unwrap().to_string();
    let assoc = client
        .associate_vpc_cidr_block()
        .vpc_id(&id)
        .cidr_block("10.9.0.0/16")
        .send()
        .await
        .unwrap();
    let assoc_id = assoc
        .cidr_block_association()
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();
    let resp = client
        .disassociate_vpc_cidr_block()
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.cidr_block_association().unwrap().association_id(),
        Some(assoc_id.as_str())
    );
}

// ---- DHCP options ----

#[test_action("ec2", "CreateDhcpOptions", checksum = "49e69e77")]
#[tokio::test]
async fn ec2_create_dhcp_options() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let resp = client
        .create_dhcp_options()
        .dhcp_configurations(
            aws_sdk_ec2::types::NewDhcpConfiguration::builder()
                .key("domain-name")
                .values("example.com")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let opts = resp.dhcp_options().unwrap();
    assert!(opts.dhcp_options_id().unwrap().starts_with("dopt-"));
    assert!(opts
        .dhcp_configurations()
        .iter()
        .any(|c| c.key() == Some("domain-name")));
}

#[test_action("ec2", "DescribeDhcpOptions", checksum = "9844758c")]
#[tokio::test]
async fn ec2_describe_dhcp_options() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let created = client
        .create_dhcp_options()
        .dhcp_configurations(
            aws_sdk_ec2::types::NewDhcpConfiguration::builder()
                .key("domain-name-servers")
                .values("10.0.0.2")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let id = created
        .dhcp_options()
        .unwrap()
        .dhcp_options_id()
        .unwrap()
        .to_string();
    let resp = client
        .describe_dhcp_options()
        .dhcp_options_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.dhcp_options().len(), 1);
}

#[test_action("ec2", "DeleteDhcpOptions", checksum = "50d3c84c")]
#[tokio::test]
async fn ec2_delete_dhcp_options() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let created = client
        .create_dhcp_options()
        .dhcp_configurations(
            aws_sdk_ec2::types::NewDhcpConfiguration::builder()
                .key("domain-name")
                .values("x.com")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let id = created
        .dhcp_options()
        .unwrap()
        .dhcp_options_id()
        .unwrap()
        .to_string();
    client
        .delete_dhcp_options()
        .dhcp_options_id(&id)
        .send()
        .await
        .unwrap();
    let resp = client.describe_dhcp_options().send().await.unwrap();
    assert!(!resp
        .dhcp_options()
        .iter()
        .any(|o| o.dhcp_options_id() == Some(id.as_str())));
}

#[test_action("ec2", "AssociateDhcpOptions", checksum = "90fad717")]
#[tokio::test]
async fn ec2_associate_dhcp_options() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let vpc = client
        .create_vpc()
        .cidr_block("10.10.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();
    let dopt = client
        .create_dhcp_options()
        .dhcp_configurations(
            aws_sdk_ec2::types::NewDhcpConfiguration::builder()
                .key("domain-name")
                .values("y.com")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let dopt_id = dopt
        .dhcp_options()
        .unwrap()
        .dhcp_options_id()
        .unwrap()
        .to_string();
    client
        .associate_dhcp_options()
        .dhcp_options_id(&dopt_id)
        .vpc_id(&vpc_id)
        .send()
        .await
        .unwrap();
    let resp = client
        .describe_vpcs()
        .vpc_ids(&vpc_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.vpcs()[0].dhcp_options_id(), Some(dopt_id.as_str()));
}

// ---- Subnets ----

#[test_action("ec2", "CreateSubnet", checksum = "1be65903")]
#[tokio::test]
async fn ec2_create_subnet() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .create_subnet()
        .vpc_id("vpc-0123456789abcdef0")
        .cidr_block("10.0.1.0/24")
        .send()
        .await
        .unwrap();
    let s = r.subnet().unwrap();
    assert!(s.subnet_id().unwrap().starts_with("subnet-"));
    assert_eq!(s.cidr_block(), Some("10.0.1.0/24"));
}

#[test_action("ec2", "CreateDefaultSubnet", checksum = "5c6a7212")]
#[tokio::test]
async fn ec2_create_default_subnet() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .create_default_subnet()
        .availability_zone("us-east-1a")
        .send()
        .await
        .unwrap();
    assert_eq!(r.subnet().unwrap().default_for_az(), Some(true));
}

#[test_action("ec2", "CreateSecondarySubnet", checksum = "22b49aff")]
#[tokio::test]
async fn ec2_create_secondary_subnet() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .create_secondary_subnet()
        .ipv4_cidr_block("10.5.0.0/24")
        .secondary_network_id("sn-net-1")
        .send()
        .await
        .unwrap();
    assert!(r
        .secondary_subnet()
        .unwrap()
        .secondary_subnet_id()
        .unwrap()
        .starts_with("subnet-"));
}

#[test_action("ec2", "DescribeSubnets", checksum = "3fb46cf3")]
#[tokio::test]
async fn ec2_describe_subnets() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let c = client
        .create_subnet()
        .vpc_id("vpc-1")
        .cidr_block("10.0.2.0/24")
        .send()
        .await
        .unwrap();
    let id = c.subnet().unwrap().subnet_id().unwrap().to_string();
    let r = client
        .describe_subnets()
        .subnet_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.subnets().len(), 1);
}

#[test_action("ec2", "DescribeSecondarySubnets", checksum = "23a04947")]
#[tokio::test]
async fn ec2_describe_secondary_subnets() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client.describe_secondary_subnets().send().await.unwrap();
    assert!(r.secondary_subnets().is_empty());
}

#[test_action("ec2", "DeleteSubnet", checksum = "eb39b3b7")]
#[tokio::test]
async fn ec2_delete_subnet() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let c = client
        .create_subnet()
        .vpc_id("vpc-1")
        .cidr_block("10.0.3.0/24")
        .send()
        .await
        .unwrap();
    let id = c.subnet().unwrap().subnet_id().unwrap().to_string();
    client.delete_subnet().subnet_id(&id).send().await.unwrap();
    let r = client.describe_subnets().send().await.unwrap();
    assert!(!r
        .subnets()
        .iter()
        .any(|s| s.subnet_id() == Some(id.as_str())));
}

#[test_action("ec2", "DeleteSecondarySubnet", checksum = "304e6ef5")]
#[tokio::test]
async fn ec2_delete_secondary_subnet() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    client
        .delete_secondary_subnet()
        .secondary_subnet_id("subnet-sec-1")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifySubnetAttribute", checksum = "c60b1a50")]
#[tokio::test]
async fn ec2_modify_subnet_attribute() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let c = client
        .create_subnet()
        .vpc_id("vpc-1")
        .cidr_block("10.0.4.0/24")
        .send()
        .await
        .unwrap();
    let id = c.subnet().unwrap().subnet_id().unwrap().to_string();
    client
        .modify_subnet_attribute()
        .subnet_id(&id)
        .map_public_ip_on_launch(
            aws_sdk_ec2::types::AttributeBooleanValue::builder()
                .value(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let r = client
        .describe_subnets()
        .subnet_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.subnets()[0].map_public_ip_on_launch(), Some(true));
}

#[test_action("ec2", "AssociateSubnetCidrBlock", checksum = "b03e421a")]
#[tokio::test]
async fn ec2_associate_subnet_cidr_block() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .associate_subnet_cidr_block()
        .subnet_id("subnet-0123456789abcdef0")
        .ipv6_cidr_block("2600:1f00:1::/64")
        .send()
        .await
        .unwrap();
    assert!(r
        .ipv6_cidr_block_association()
        .unwrap()
        .association_id()
        .unwrap()
        .starts_with("subnet-cidr-assoc-"));
}

#[test_action("ec2", "DisassociateSubnetCidrBlock", checksum = "f4423888")]
#[tokio::test]
async fn ec2_disassociate_subnet_cidr_block() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .disassociate_subnet_cidr_block()
        .association_id("subnet-cidr-assoc-0123456789abcdef0")
        .send()
        .await
        .unwrap();
    assert!(r.ipv6_cidr_block_association().is_some());
}

#[test_action("ec2", "CreateSubnetCidrReservation", checksum = "ab036383")]
#[tokio::test]
async fn ec2_create_subnet_cidr_reservation() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .create_subnet_cidr_reservation()
        .subnet_id("subnet-0123456789abcdef0")
        .cidr("10.0.1.16/28")
        .reservation_type(aws_sdk_ec2::types::SubnetCidrReservationType::Prefix)
        .send()
        .await
        .unwrap();
    assert!(r
        .subnet_cidr_reservation()
        .unwrap()
        .subnet_cidr_reservation_id()
        .unwrap()
        .starts_with("scr-"));
}

#[test_action("ec2", "GetSubnetCidrReservations", checksum = "5bae7cff")]
#[tokio::test]
async fn ec2_get_subnet_cidr_reservations() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    client
        .create_subnet_cidr_reservation()
        .subnet_id("subnet-aaaa")
        .cidr("10.0.2.16/28")
        .reservation_type(aws_sdk_ec2::types::SubnetCidrReservationType::Explicit)
        .send()
        .await
        .unwrap();
    let r = client
        .get_subnet_cidr_reservations()
        .subnet_id("subnet-aaaa")
        .send()
        .await
        .unwrap();
    assert_eq!(r.subnet_ipv4_cidr_reservations().len(), 1);
}

#[test_action("ec2", "DeleteSubnetCidrReservation", checksum = "06145f84")]
#[tokio::test]
async fn ec2_delete_subnet_cidr_reservation() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let c = client
        .create_subnet_cidr_reservation()
        .subnet_id("subnet-bbbb")
        .cidr("10.0.3.16/28")
        .reservation_type(aws_sdk_ec2::types::SubnetCidrReservationType::Prefix)
        .send()
        .await
        .unwrap();
    let id = c
        .subnet_cidr_reservation()
        .unwrap()
        .subnet_cidr_reservation_id()
        .unwrap()
        .to_string();
    let r = client
        .delete_subnet_cidr_reservation()
        .subnet_cidr_reservation_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.deleted_subnet_cidr_reservation()
            .unwrap()
            .subnet_cidr_reservation_id(),
        Some(id.as_str())
    );
}

// ---- Security groups ----

async fn make_sg(client: &aws_sdk_ec2::Client) -> String {
    client
        .create_security_group()
        .group_name({
            static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            format!(
                "sg-test-{}",
                N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            )
        })
        .description("test")
        .send()
        .await
        .unwrap()
        .group_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateSecurityGroup", checksum = "a08ce784")]
#[tokio::test]
async fn ec2_create_security_group() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .create_security_group()
        .group_name("web")
        .description("web sg")
        .send()
        .await
        .unwrap();
    assert!(r.group_id().unwrap().starts_with("sg-"));
}

#[test_action("ec2", "DescribeSecurityGroups", checksum = "76d99894")]
#[tokio::test]
async fn ec2_describe_security_groups() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let id = make_sg(&client).await;
    let r = client
        .describe_security_groups()
        .group_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.security_groups().len(), 1);
    assert_eq!(r.security_groups()[0].group_id(), Some(id.as_str()));
}

#[test_action("ec2", "DeleteSecurityGroup", checksum = "1dc96802")]
#[tokio::test]
async fn ec2_delete_security_group() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let id = make_sg(&client).await;
    client
        .delete_security_group()
        .group_id(&id)
        .send()
        .await
        .unwrap();
    let r = client.describe_security_groups().send().await.unwrap();
    assert!(!r
        .security_groups()
        .iter()
        .any(|g| g.group_id() == Some(id.as_str())));
}

#[test_action("ec2", "AuthorizeSecurityGroupIngress", checksum = "1aab945e")]
#[tokio::test]
async fn ec2_authorize_ingress() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let id = make_sg(&client).await;
    let r = client
        .authorize_security_group_ingress()
        .group_id(&id)
        .ip_permissions(
            aws_sdk_ec2::types::IpPermission::builder()
                .ip_protocol("tcp")
                .from_port(80)
                .to_port(80)
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
    assert_eq!(r.r#return(), Some(true));
    assert!(!r.security_group_rules().is_empty());
}

#[test_action("ec2", "AuthorizeSecurityGroupEgress", checksum = "851ec8cb")]
#[tokio::test]
async fn ec2_authorize_egress() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let id = make_sg(&client).await;
    let r = client
        .authorize_security_group_egress()
        .group_id(&id)
        .ip_permissions(
            aws_sdk_ec2::types::IpPermission::builder()
                .ip_protocol("-1")
                .ip_ranges(
                    aws_sdk_ec2::types::IpRange::builder()
                        .cidr_ip("10.0.0.0/8")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "RevokeSecurityGroupIngress", checksum = "fb623089")]
#[tokio::test]
async fn ec2_revoke_ingress() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let id = make_sg(&client).await;
    client
        .revoke_security_group_ingress()
        .group_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "RevokeSecurityGroupEgress", checksum = "8e867b68")]
#[tokio::test]
async fn ec2_revoke_egress() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let id = make_sg(&client).await;
    client
        .revoke_security_group_egress()
        .group_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeSecurityGroupRules", checksum = "f9e733db")]
#[tokio::test]
async fn ec2_describe_security_group_rules() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let id = make_sg(&client).await;
    client
        .authorize_security_group_ingress()
        .group_id(&id)
        .ip_permissions(
            aws_sdk_ec2::types::IpPermission::builder()
                .ip_protocol("tcp")
                .from_port(22)
                .to_port(22)
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
    let r = client.describe_security_group_rules().send().await.unwrap();
    assert!(r
        .security_group_rules()
        .iter()
        .any(|x| x.from_port() == Some(22)));
}

#[test_action("ec2", "ModifySecurityGroupRules", checksum = "2a014064")]
#[tokio::test]
async fn ec2_modify_security_group_rules() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let id = make_sg(&client).await;
    client
        .modify_security_group_rules()
        .group_id(&id)
        .security_group_rules(
            aws_sdk_ec2::types::SecurityGroupRuleUpdate::builder()
                .security_group_rule_id("sgr-1")
                .security_group_rule(
                    aws_sdk_ec2::types::SecurityGroupRuleRequest::builder()
                        .ip_protocol("tcp")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "UpdateSecurityGroupRuleDescriptionsIngress",
    checksum = "3ee3289d"
)]
#[tokio::test]
async fn ec2_update_descriptions_ingress() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let id = make_sg(&client).await;
    client
        .update_security_group_rule_descriptions_ingress()
        .group_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "UpdateSecurityGroupRuleDescriptionsEgress",
    checksum = "32ae7304"
)]
#[tokio::test]
async fn ec2_update_descriptions_egress() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let id = make_sg(&client).await;
    client
        .update_security_group_rule_descriptions_egress()
        .group_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AssociateSecurityGroupVpc", checksum = "df1e5167")]
#[tokio::test]
async fn ec2_associate_security_group_vpc() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .associate_security_group_vpc()
        .group_id("sg-1")
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    assert!(r.state().is_some());
}

#[test_action("ec2", "DisassociateSecurityGroupVpc", checksum = "90e47c4b")]
#[tokio::test]
async fn ec2_disassociate_security_group_vpc() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .disassociate_security_group_vpc()
        .group_id("sg-1")
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    assert!(r.state().is_some());
}

#[test_action("ec2", "DescribeSecurityGroupVpcAssociations", checksum = "0ab905a5")]
#[tokio::test]
async fn ec2_describe_security_group_vpc_associations() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .describe_security_group_vpc_associations()
        .send()
        .await
        .unwrap();
    assert!(r.security_group_vpc_associations().is_empty());
}

#[test_action("ec2", "GetSecurityGroupsForVpc", checksum = "d1ef3542")]
#[tokio::test]
async fn ec2_get_security_groups_for_vpc() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .get_security_groups_for_vpc()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    let _ = r.security_group_for_vpcs();
}

#[test_action("ec2", "DescribeStaleSecurityGroups", checksum = "dd50f4fa")]
#[tokio::test]
async fn ec2_describe_stale_security_groups() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .describe_stale_security_groups()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    assert!(r.stale_security_group_set().is_empty());
}

#[test_action("ec2", "DescribeSecurityGroupReferences", checksum = "a579bf47")]
#[tokio::test]
async fn ec2_describe_security_group_references() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;
    let r = client
        .describe_security_group_references()
        .group_id("sg-1")
        .send()
        .await
        .unwrap();
    assert!(r.security_group_reference_set().is_empty());
}

// ---- Route tables / gateways ----

#[test_action("ec2", "CreateRouteTable", checksum = "bc2d13d4")]
#[tokio::test]
async fn ec2_create_route_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.create_route_table().vpc_id("vpc-1").send().await.unwrap();
    assert!(r
        .route_table()
        .unwrap()
        .route_table_id()
        .unwrap()
        .starts_with("rtb-"));
}

#[test_action("ec2", "DescribeRouteTables", checksum = "52a4f78c")]
#[tokio::test]
async fn ec2_describe_route_tables() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_route_table()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .route_table()
        .unwrap()
        .route_table_id()
        .unwrap()
        .to_string();
    let r = c
        .describe_route_tables()
        .route_table_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.route_tables().len(), 1);
}

#[test_action("ec2", "DeleteRouteTable", checksum = "0c1e822d")]
#[tokio::test]
async fn ec2_delete_route_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_route_table()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .route_table()
        .unwrap()
        .route_table_id()
        .unwrap()
        .to_string();
    c.delete_route_table()
        .route_table_id(&id)
        .send()
        .await
        .unwrap();
    assert!(!c
        .describe_route_tables()
        .send()
        .await
        .unwrap()
        .route_tables()
        .iter()
        .any(|t| t.route_table_id() == Some(id.as_str())));
}

#[test_action("ec2", "CreateRoute", checksum = "8bcd436f")]
#[tokio::test]
async fn ec2_create_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_route_table()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .route_table()
        .unwrap()
        .route_table_id()
        .unwrap()
        .to_string();
    let r = c
        .create_route()
        .route_table_id(&id)
        .destination_cidr_block("0.0.0.0/0")
        .gateway_id("igw-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "ReplaceRoute", checksum = "fa0d2693")]
#[tokio::test]
async fn ec2_replace_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_route_table()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .route_table()
        .unwrap()
        .route_table_id()
        .unwrap()
        .to_string();
    c.create_route()
        .route_table_id(&id)
        .destination_cidr_block("0.0.0.0/0")
        .gateway_id("igw-1")
        .send()
        .await
        .unwrap();
    c.replace_route()
        .route_table_id(&id)
        .destination_cidr_block("0.0.0.0/0")
        .gateway_id("igw-2")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteRoute", checksum = "86ac2fb5")]
#[tokio::test]
async fn ec2_delete_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_route_table()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .route_table()
        .unwrap()
        .route_table_id()
        .unwrap()
        .to_string();
    c.create_route()
        .route_table_id(&id)
        .destination_cidr_block("0.0.0.0/0")
        .gateway_id("igw-1")
        .send()
        .await
        .unwrap();
    c.delete_route()
        .route_table_id(&id)
        .destination_cidr_block("0.0.0.0/0")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AssociateRouteTable", checksum = "feeb356d")]
#[tokio::test]
async fn ec2_associate_route_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_route_table()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .route_table()
        .unwrap()
        .route_table_id()
        .unwrap()
        .to_string();
    let r = c
        .associate_route_table()
        .route_table_id(&id)
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap();
    assert!(r.association_id().unwrap().starts_with("rtbassoc-"));
}

#[test_action("ec2", "DisassociateRouteTable", checksum = "128016f1")]
#[tokio::test]
async fn ec2_disassociate_route_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_route_table()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .route_table()
        .unwrap()
        .route_table_id()
        .unwrap()
        .to_string();
    let a = c
        .associate_route_table()
        .route_table_id(&id)
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();
    c.disassociate_route_table()
        .association_id(&a)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ReplaceRouteTableAssociation", checksum = "18b61684")]
#[tokio::test]
async fn ec2_replace_route_table_association() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_route_table()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .route_table()
        .unwrap()
        .route_table_id()
        .unwrap()
        .to_string();
    let a = c
        .associate_route_table()
        .route_table_id(&id)
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();
    let r = c
        .replace_route_table_association()
        .association_id(&a)
        .route_table_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.new_association_id().unwrap().starts_with("rtbassoc-"));
}

#[test_action("ec2", "CreateInternetGateway", checksum = "88dcf2c8")]
#[tokio::test]
async fn ec2_create_internet_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.create_internet_gateway().send().await.unwrap();
    assert!(r
        .internet_gateway()
        .unwrap()
        .internet_gateway_id()
        .unwrap()
        .starts_with("igw-"));
}

#[test_action("ec2", "DescribeInternetGateways", checksum = "471f22f0")]
#[tokio::test]
async fn ec2_describe_internet_gateways() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_internet_gateway()
        .send()
        .await
        .unwrap()
        .internet_gateway()
        .unwrap()
        .internet_gateway_id()
        .unwrap()
        .to_string();
    assert_eq!(
        c.describe_internet_gateways()
            .internet_gateway_ids(&id)
            .send()
            .await
            .unwrap()
            .internet_gateways()
            .len(),
        1
    );
}

#[test_action("ec2", "AttachInternetGateway", checksum = "c4730d4f")]
#[tokio::test]
async fn ec2_attach_internet_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_internet_gateway()
        .send()
        .await
        .unwrap()
        .internet_gateway()
        .unwrap()
        .internet_gateway_id()
        .unwrap()
        .to_string();
    c.attach_internet_gateway()
        .internet_gateway_id(&id)
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DetachInternetGateway", checksum = "34aed96e")]
#[tokio::test]
async fn ec2_detach_internet_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_internet_gateway()
        .send()
        .await
        .unwrap()
        .internet_gateway()
        .unwrap()
        .internet_gateway_id()
        .unwrap()
        .to_string();
    c.attach_internet_gateway()
        .internet_gateway_id(&id)
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    c.detach_internet_gateway()
        .internet_gateway_id(&id)
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteInternetGateway", checksum = "603ad93b")]
#[tokio::test]
async fn ec2_delete_internet_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_internet_gateway()
        .send()
        .await
        .unwrap()
        .internet_gateway()
        .unwrap()
        .internet_gateway_id()
        .unwrap()
        .to_string();
    c.delete_internet_gateway()
        .internet_gateway_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateEgressOnlyInternetGateway", checksum = "d1592658")]
#[tokio::test]
async fn ec2_create_eigw() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_egress_only_internet_gateway()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    assert!(r
        .egress_only_internet_gateway()
        .unwrap()
        .egress_only_internet_gateway_id()
        .unwrap()
        .starts_with("eigw-"));
}

#[test_action("ec2", "DescribeEgressOnlyInternetGateways", checksum = "1956120d")]
#[tokio::test]
async fn ec2_describe_eigws() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_egress_only_internet_gateway()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .egress_only_internet_gateway()
        .unwrap()
        .egress_only_internet_gateway_id()
        .unwrap()
        .to_string();
    assert_eq!(
        c.describe_egress_only_internet_gateways()
            .egress_only_internet_gateway_ids(&id)
            .send()
            .await
            .unwrap()
            .egress_only_internet_gateways()
            .len(),
        1
    );
}

#[test_action("ec2", "DeleteEgressOnlyInternetGateway", checksum = "84415c8c")]
#[tokio::test]
async fn ec2_delete_eigw() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_egress_only_internet_gateway()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .egress_only_internet_gateway()
        .unwrap()
        .egress_only_internet_gateway_id()
        .unwrap()
        .to_string();
    let r = c
        .delete_egress_only_internet_gateway()
        .egress_only_internet_gateway_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.return_code(), Some(true));
}

#[test_action("ec2", "CreateNatGateway", checksum = "d0d28d06")]
#[tokio::test]
async fn ec2_create_nat_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_nat_gateway()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap();
    assert!(r
        .nat_gateway()
        .unwrap()
        .nat_gateway_id()
        .unwrap()
        .starts_with("nat-"));
}

#[test_action("ec2", "DescribeNatGateways", checksum = "02910559")]
#[tokio::test]
async fn ec2_describe_nat_gateways() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_nat_gateway()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .nat_gateway()
        .unwrap()
        .nat_gateway_id()
        .unwrap()
        .to_string();
    assert_eq!(
        c.describe_nat_gateways()
            .nat_gateway_ids(&id)
            .send()
            .await
            .unwrap()
            .nat_gateways()
            .len(),
        1
    );
}

#[test_action("ec2", "DeleteNatGateway", checksum = "447b0c15")]
#[tokio::test]
async fn ec2_delete_nat_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_nat_gateway()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .nat_gateway()
        .unwrap()
        .nat_gateway_id()
        .unwrap()
        .to_string();
    let r = c
        .delete_nat_gateway()
        .nat_gateway_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.nat_gateway_id(), Some(id.as_str()));
}

#[test_action("ec2", "AssignPrivateNatGatewayAddress", checksum = "69cb4f56")]
#[tokio::test]
async fn ec2_assign_private_nat_address() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .assign_private_nat_gateway_address()
        .nat_gateway_id("nat-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.nat_gateway_id(), Some("nat-1"));
}

#[test_action("ec2", "AssociateNatGatewayAddress", checksum = "4d7961e4")]
#[tokio::test]
async fn ec2_associate_nat_address() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .associate_nat_gateway_address()
        .nat_gateway_id("nat-1")
        .allocation_ids("eipalloc-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.nat_gateway_id(), Some("nat-1"));
}

#[test_action("ec2", "DisassociateNatGatewayAddress", checksum = "11c57af5")]
#[tokio::test]
async fn ec2_disassociate_nat_address() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disassociate_nat_gateway_address()
        .nat_gateway_id("nat-1")
        .association_ids("eipassoc-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.nat_gateway_id(), Some("nat-1"));
}

#[test_action("ec2", "UnassignPrivateNatGatewayAddress", checksum = "eefaa86c")]
#[tokio::test]
async fn ec2_unassign_private_nat_address() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .unassign_private_nat_gateway_address()
        .nat_gateway_id("nat-1")
        .private_ip_addresses("10.0.0.5")
        .send()
        .await
        .unwrap();
    assert_eq!(r.nat_gateway_id(), Some("nat-1"));
}

// ---- Elastic IPs / key pairs / placement groups ----

#[test_action("ec2", "AllocateAddress", checksum = "72e9819e")]
#[tokio::test]
async fn ec2_allocate_address() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.allocate_address().send().await.unwrap();
    assert!(r.allocation_id().unwrap().starts_with("eipalloc-"));
}

#[test_action("ec2", "DescribeAddresses", checksum = "56b4e26b")]
#[tokio::test]
async fn ec2_describe_addresses() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .allocate_address()
        .send()
        .await
        .unwrap()
        .allocation_id()
        .unwrap()
        .to_string();
    let r = c
        .describe_addresses()
        .allocation_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.addresses().len(), 1);
}

#[test_action("ec2", "ReleaseAddress", checksum = "ddf3c731")]
#[tokio::test]
async fn ec2_release_address() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .allocate_address()
        .send()
        .await
        .unwrap()
        .allocation_id()
        .unwrap()
        .to_string();
    c.release_address().allocation_id(&id).send().await.unwrap();
    assert!(c
        .describe_addresses()
        .send()
        .await
        .unwrap()
        .addresses()
        .is_empty());
}

#[test_action("ec2", "AssociateAddress", checksum = "f9d7114a")]
#[tokio::test]
async fn ec2_associate_address() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .allocate_address()
        .send()
        .await
        .unwrap()
        .allocation_id()
        .unwrap()
        .to_string();
    let r = c
        .associate_address()
        .allocation_id(&id)
        .instance_id("i-1")
        .send()
        .await
        .unwrap();
    assert!(r.association_id().unwrap().starts_with("eipassoc-"));
}

#[test_action("ec2", "DisassociateAddress", checksum = "7ba65bc1")]
#[tokio::test]
async fn ec2_disassociate_address() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .allocate_address()
        .send()
        .await
        .unwrap()
        .allocation_id()
        .unwrap()
        .to_string();
    let a = c
        .associate_address()
        .allocation_id(&id)
        .instance_id("i-1")
        .send()
        .await
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();
    c.disassociate_address()
        .association_id(&a)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeAddressesAttribute", checksum = "de81c82d")]
#[tokio::test]
async fn ec2_describe_addresses_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_addresses_attribute().send().await.unwrap();
    assert!(r.addresses().is_empty());
}

#[test_action("ec2", "ModifyAddressAttribute", checksum = "826d8f03")]
#[tokio::test]
async fn ec2_modify_address_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .modify_address_attribute()
        .allocation_id("eipalloc-1")
        .domain_name("x.com")
        .send()
        .await
        .unwrap();
    assert!(r.address().is_some());
}

#[test_action("ec2", "ResetAddressAttribute", checksum = "6d8e2e96")]
#[tokio::test]
async fn ec2_reset_address_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .reset_address_attribute()
        .allocation_id("eipalloc-1")
        .attribute(aws_sdk_ec2::types::AddressAttributeName::DomainName)
        .send()
        .await
        .unwrap();
    assert!(r.address().is_some());
}

#[test_action("ec2", "MoveAddressToVpc", checksum = "504d8ee5")]
#[tokio::test]
async fn ec2_move_address_to_vpc() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .move_address_to_vpc()
        .public_ip("52.1.2.3")
        .send()
        .await
        .unwrap();
    assert!(r.allocation_id().is_some());
}

#[test_action("ec2", "RestoreAddressToClassic", checksum = "993eac2f")]
#[tokio::test]
async fn ec2_restore_address_to_classic() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .restore_address_to_classic()
        .public_ip("52.1.2.3")
        .send()
        .await
        .unwrap();
    assert_eq!(r.public_ip(), Some("52.1.2.3"));
}

#[test_action("ec2", "AcceptAddressTransfer", checksum = "2c5acb12")]
#[tokio::test]
async fn ec2_accept_address_transfer() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .accept_address_transfer()
        .address("52.1.2.3")
        .send()
        .await
        .unwrap();
    assert!(r.address_transfer().is_some());
}

#[test_action("ec2", "EnableAddressTransfer", checksum = "e7f63186")]
#[tokio::test]
async fn ec2_enable_address_transfer() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .enable_address_transfer()
        .allocation_id("eipalloc-1")
        .transfer_account_id("123456789012")
        .send()
        .await
        .unwrap();
    assert!(r.address_transfer().is_some());
}

#[test_action("ec2", "DisableAddressTransfer", checksum = "21ada00d")]
#[tokio::test]
async fn ec2_disable_address_transfer() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disable_address_transfer()
        .allocation_id("eipalloc-1")
        .send()
        .await
        .unwrap();
    assert!(r.address_transfer().is_some());
}

#[test_action("ec2", "DescribeAddressTransfers", checksum = "ac471d03")]
#[tokio::test]
async fn ec2_describe_address_transfers() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    assert!(c
        .describe_address_transfers()
        .send()
        .await
        .unwrap()
        .address_transfers()
        .is_empty());
}

#[test_action("ec2", "DescribeMovingAddresses", checksum = "1bb88145")]
#[tokio::test]
async fn ec2_describe_moving_addresses() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    assert!(c
        .describe_moving_addresses()
        .send()
        .await
        .unwrap()
        .moving_address_statuses()
        .is_empty());
}

#[test_action("ec2", "CreateKeyPair", checksum = "fda0b5e7")]
#[tokio::test]
async fn ec2_create_key_pair() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.create_key_pair().key_name("kp1").send().await.unwrap();
    assert!(r.key_pair_id().unwrap().starts_with("key-"));
    assert!(r.key_material().is_some());
}

#[test_action("ec2", "ImportKeyPair", checksum = "dd40237e")]
#[tokio::test]
async fn ec2_import_key_pair() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .import_key_pair()
        .key_name("kp2")
        .public_key_material(aws_sdk_ec2::primitives::Blob::new(b"ssh-rsa AAAA"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.key_name(), Some("kp2"));
}

#[test_action("ec2", "DescribeKeyPairs", checksum = "0dc1eaa5")]
#[tokio::test]
async fn ec2_describe_key_pairs() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_key_pair().key_name("kp3").send().await.unwrap();
    let r = c
        .describe_key_pairs()
        .key_names("kp3")
        .send()
        .await
        .unwrap();
    assert_eq!(r.key_pairs().len(), 1);
}

#[test_action("ec2", "DeleteKeyPair", checksum = "a77af389")]
#[tokio::test]
async fn ec2_delete_key_pair() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_key_pair().key_name("kp4").send().await.unwrap();
    c.delete_key_pair().key_name("kp4").send().await.unwrap();
    assert!(c
        .describe_key_pairs()
        .send()
        .await
        .unwrap()
        .key_pairs()
        .iter()
        .all(|k| k.key_name() != Some("kp4")));
}

#[test_action("ec2", "CreatePlacementGroup", checksum = "d75eaee7")]
#[tokio::test]
async fn ec2_create_placement_group() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_placement_group()
        .group_name("pg1")
        .strategy(aws_sdk_ec2::types::PlacementStrategy::Cluster)
        .send()
        .await
        .unwrap();
    assert_eq!(r.placement_group().unwrap().group_name(), Some("pg1"));
}

#[test_action("ec2", "DescribePlacementGroups", checksum = "50d35e54")]
#[tokio::test]
async fn ec2_describe_placement_groups() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_placement_group()
        .group_name("pg2")
        .strategy(aws_sdk_ec2::types::PlacementStrategy::Spread)
        .send()
        .await
        .unwrap();
    let r = c
        .describe_placement_groups()
        .group_names("pg2")
        .send()
        .await
        .unwrap();
    assert_eq!(r.placement_groups().len(), 1);
}

#[test_action("ec2", "DeletePlacementGroup", checksum = "57a1ff80")]
#[tokio::test]
async fn ec2_delete_placement_group() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_placement_group()
        .group_name("pg3")
        .strategy(aws_sdk_ec2::types::PlacementStrategy::Cluster)
        .send()
        .await
        .unwrap();
    c.delete_placement_group()
        .group_name("pg3")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetGroupsForCapacityReservation", checksum = "636a8af3")]
#[tokio::test]
async fn ec2_get_groups_for_capacity_reservation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_groups_for_capacity_reservation()
        .capacity_reservation_id("cr-1")
        .send()
        .await
        .unwrap();
    assert!(r.capacity_reservation_groups().is_empty());
}

// ---- Network interfaces ----

#[test_action("ec2", "CreateNetworkInterface", checksum = "fb8b07c5")]
#[tokio::test]
async fn ec2_create_network_interface() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_network_interface()
        .subnet_id("subnet-1")
        .description("eni")
        .send()
        .await
        .unwrap();
    assert!(r
        .network_interface()
        .unwrap()
        .network_interface_id()
        .unwrap()
        .starts_with("eni-"));
}

#[test_action("ec2", "DescribeNetworkInterfaces", checksum = "a535ece3")]
#[tokio::test]
async fn ec2_describe_network_interfaces() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_network_interface()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .network_interface()
        .unwrap()
        .network_interface_id()
        .unwrap()
        .to_string();
    let r = c
        .describe_network_interfaces()
        .network_interface_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.network_interfaces().len(), 1);
}

#[test_action("ec2", "DeleteNetworkInterface", checksum = "2440679a")]
#[tokio::test]
async fn ec2_delete_network_interface() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_network_interface()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .network_interface()
        .unwrap()
        .network_interface_id()
        .unwrap()
        .to_string();
    c.delete_network_interface()
        .network_interface_id(&id)
        .send()
        .await
        .unwrap();
    assert!(c
        .describe_network_interfaces()
        .send()
        .await
        .unwrap()
        .network_interfaces()
        .is_empty());
}

#[test_action("ec2", "AttachNetworkInterface", checksum = "50e5af9a")]
#[tokio::test]
async fn ec2_attach_network_interface() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_network_interface()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .network_interface()
        .unwrap()
        .network_interface_id()
        .unwrap()
        .to_string();
    let r = c
        .attach_network_interface()
        .network_interface_id(&id)
        .instance_id("i-1")
        .device_index(1)
        .send()
        .await
        .unwrap();
    assert!(r.attachment_id().unwrap().starts_with("eni-attach-"));
}

#[test_action("ec2", "DetachNetworkInterface", checksum = "2a90588a")]
#[tokio::test]
async fn ec2_detach_network_interface() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_network_interface()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .network_interface()
        .unwrap()
        .network_interface_id()
        .unwrap()
        .to_string();
    let a = c
        .attach_network_interface()
        .network_interface_id(&id)
        .instance_id("i-1")
        .device_index(1)
        .send()
        .await
        .unwrap()
        .attachment_id()
        .unwrap()
        .to_string();
    c.detach_network_interface()
        .attachment_id(&a)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyNetworkInterfaceAttribute", checksum = "5295ba2f")]
#[tokio::test]
async fn ec2_modify_network_interface_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_network_interface()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .network_interface()
        .unwrap()
        .network_interface_id()
        .unwrap()
        .to_string();
    c.modify_network_interface_attribute()
        .network_interface_id(&id)
        .description(
            aws_sdk_ec2::types::AttributeValue::builder()
                .value("x")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ResetNetworkInterfaceAttribute", checksum = "74b31afb")]
#[tokio::test]
async fn ec2_reset_network_interface_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_network_interface()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .network_interface()
        .unwrap()
        .network_interface_id()
        .unwrap()
        .to_string();
    c.reset_network_interface_attribute()
        .network_interface_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeNetworkInterfaceAttribute", checksum = "dd5cfed6")]
#[tokio::test]
async fn ec2_describe_network_interface_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_network_interface()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .network_interface()
        .unwrap()
        .network_interface_id()
        .unwrap()
        .to_string();
    let r = c
        .describe_network_interface_attribute()
        .network_interface_id(&id)
        .attribute(aws_sdk_ec2::types::NetworkInterfaceAttribute::SourceDestCheck)
        .send()
        .await
        .unwrap();
    assert_eq!(r.network_interface_id(), Some(id.as_str()));
}

#[test_action("ec2", "CreateNetworkInterfacePermission", checksum = "047e38ad")]
#[tokio::test]
async fn ec2_create_eni_permission() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_network_interface_permission()
        .network_interface_id("eni-1")
        .aws_account_id("123456789012")
        .permission(aws_sdk_ec2::types::InterfacePermissionType::InstanceAttach)
        .send()
        .await
        .unwrap();
    assert!(r
        .interface_permission()
        .unwrap()
        .network_interface_permission_id()
        .unwrap()
        .starts_with("eni-perm-"));
}

#[test_action("ec2", "DescribeNetworkInterfacePermissions", checksum = "e4c9ad17")]
#[tokio::test]
async fn ec2_describe_eni_permissions() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_network_interface_permission()
        .network_interface_id("eni-1")
        .aws_account_id("123456789012")
        .permission(aws_sdk_ec2::types::InterfacePermissionType::InstanceAttach)
        .send()
        .await
        .unwrap();
    let r = c
        .describe_network_interface_permissions()
        .send()
        .await
        .unwrap();
    assert!(!r.network_interface_permissions().is_empty());
}

#[test_action("ec2", "DeleteNetworkInterfacePermission", checksum = "6ba0312c")]
#[tokio::test]
async fn ec2_delete_eni_permission() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_network_interface_permission()
        .network_interface_id("eni-1")
        .aws_account_id("123456789012")
        .permission(aws_sdk_ec2::types::InterfacePermissionType::InstanceAttach)
        .send()
        .await
        .unwrap()
        .interface_permission()
        .unwrap()
        .network_interface_permission_id()
        .unwrap()
        .to_string();
    c.delete_network_interface_permission()
        .network_interface_permission_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AssignPrivateIpAddresses", checksum = "b0fceb60")]
#[tokio::test]
async fn ec2_assign_private_ips() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .assign_private_ip_addresses()
        .network_interface_id("eni-1")
        .private_ip_addresses("10.0.0.31")
        .send()
        .await
        .unwrap();
    assert_eq!(r.network_interface_id(), Some("eni-1"));
}

#[test_action("ec2", "UnassignPrivateIpAddresses", checksum = "65c70924")]
#[tokio::test]
async fn ec2_unassign_private_ips() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.unassign_private_ip_addresses()
        .network_interface_id("eni-1")
        .private_ip_addresses("10.0.0.31")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AssignIpv6Addresses", checksum = "cac8c175")]
#[tokio::test]
async fn ec2_assign_ipv6() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .assign_ipv6_addresses()
        .network_interface_id("eni-1")
        .ipv6_addresses("2600:1f00::5")
        .send()
        .await
        .unwrap();
    assert_eq!(r.network_interface_id(), Some("eni-1"));
}

#[test_action("ec2", "UnassignIpv6Addresses", checksum = "0c460cb5")]
#[tokio::test]
async fn ec2_unassign_ipv6() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .unassign_ipv6_addresses()
        .network_interface_id("eni-1")
        .ipv6_addresses("2600:1f00::5")
        .send()
        .await
        .unwrap();
    assert_eq!(r.network_interface_id(), Some("eni-1"));
}

// ---- Instances ----

async fn run_one(c: &aws_sdk_ec2::Client) -> String {
    c.run_instances()
        .min_count(1)
        .max_count(1)
        .image_id("ami-12345678")
        .send()
        .await
        .unwrap()
        .instances()[0]
        .instance_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "RunInstances", checksum = "b21a67b4")]
#[tokio::test]
async fn ec2_run_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .run_instances()
        .min_count(2)
        .max_count(2)
        .image_id("ami-1")
        .instance_type(aws_sdk_ec2::types::InstanceType::T3Micro)
        .send()
        .await
        .unwrap();
    assert_eq!(r.instances().len(), 2);
    assert!(r.instances()[0].instance_id().unwrap().starts_with("i-"));
    assert!(r.reservation_id().unwrap().starts_with("r-"));
}

#[test_action("ec2", "DescribeInstances", checksum = "94aa1152")]
#[tokio::test]
async fn ec2_describe_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    let r = c
        .describe_instances()
        .instance_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.reservations().len(), 1);
    assert_eq!(
        r.reservations()[0].instances()[0].instance_id(),
        Some(id.as_str())
    );
}

#[test_action("ec2", "StopInstances", checksum = "fe40d609")]
#[tokio::test]
async fn ec2_stop_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    let r = c.stop_instances().instance_ids(&id).send().await.unwrap();
    assert_eq!(
        r.stopping_instances()[0].current_state().unwrap().name(),
        Some(&aws_sdk_ec2::types::InstanceStateName::Stopped)
    );
}

#[test_action("ec2", "StartInstances", checksum = "6285e152")]
#[tokio::test]
async fn ec2_start_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    c.stop_instances().instance_ids(&id).send().await.unwrap();
    let r = c.start_instances().instance_ids(&id).send().await.unwrap();
    // AWS returns the instance in `pending`; it transitions to `running`
    // asynchronously as the backing host boots (it never returns `running`
    // synchronously from StartInstances).
    assert_eq!(
        r.starting_instances()[0].current_state().unwrap().name(),
        Some(&aws_sdk_ec2::types::InstanceStateName::Pending)
    );
}

#[test_action("ec2", "RebootInstances", checksum = "93386e10")]
#[tokio::test]
async fn ec2_reboot_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    c.reboot_instances().instance_ids(&id).send().await.unwrap();
}

#[test_action("ec2", "TerminateInstances", checksum = "9274b22b")]
#[tokio::test]
async fn ec2_terminate_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    let r = c
        .terminate_instances()
        .instance_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.terminating_instances()[0].current_state().unwrap().name(),
        Some(&aws_sdk_ec2::types::InstanceStateName::Terminated)
    );
}

#[test_action("ec2", "MonitorInstances", checksum = "49cc83aa")]
#[tokio::test]
async fn ec2_monitor_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    let r = c
        .monitor_instances()
        .instance_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.instance_monitorings()[0].instance_id(), Some(id.as_str()));
}

#[test_action("ec2", "UnmonitorInstances", checksum = "c0b0fd97")]
#[tokio::test]
async fn ec2_unmonitor_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    c.unmonitor_instances()
        .instance_ids(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeInstanceStatus", checksum = "b46a8879")]
#[tokio::test]
async fn ec2_describe_instance_status() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    // A freshly launched instance is `pending` until the backing host boots;
    // DescribeInstanceStatus only reports `running` instances unless
    // IncludeAllInstances is set, so set it to observe the pending instance.
    let r = c
        .describe_instance_status()
        .instance_ids(&id)
        .include_all_instances(true)
        .send()
        .await
        .unwrap();
    assert!(r
        .instance_statuses()
        .iter()
        .any(|x| x.instance_id() == Some(id.as_str())));
}

#[test_action("ec2", "DescribeInstanceTypes", checksum = "a9635bcd")]
#[tokio::test]
async fn ec2_describe_instance_types() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .describe_instance_types()
        .instance_types(aws_sdk_ec2::types::InstanceType::T3Micro)
        .send()
        .await
        .unwrap();
    assert!(!r.instance_types().is_empty());
}

#[test_action(
    "ec2",
    "GetInstanceTypesFromInstanceRequirements",
    checksum = "f4b22cfc"
)]
#[tokio::test]
async fn ec2_get_instance_types_from_requirements() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_instance_types_from_instance_requirements()
        .architecture_types(aws_sdk_ec2::types::ArchitectureType::X8664)
        .virtualization_types(aws_sdk_ec2::types::VirtualizationType::Hvm)
        .instance_requirements(
            aws_sdk_ec2::types::InstanceRequirementsRequest::builder()
                .v_cpu_count(
                    aws_sdk_ec2::types::VCpuCountRangeRequest::builder()
                        .min(1)
                        .build(),
                )
                .memory_mib(
                    aws_sdk_ec2::types::MemoryMiBRequest::builder()
                        .min(512)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(!r.instance_types().is_empty());
}

#[test_action("ec2", "DescribeInstanceAttribute", checksum = "85432719")]
#[tokio::test]
async fn ec2_describe_instance_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    let r = c
        .describe_instance_attribute()
        .instance_id(&id)
        .attribute(aws_sdk_ec2::types::InstanceAttributeName::InstanceType)
        .send()
        .await
        .unwrap();
    assert_eq!(r.instance_id(), Some(id.as_str()));
}

#[test_action("ec2", "ModifyInstanceAttribute", checksum = "fe95a738")]
#[tokio::test]
async fn ec2_modify_instance_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    c.modify_instance_attribute()
        .instance_id(&id)
        .source_dest_check(
            aws_sdk_ec2::types::AttributeBooleanValue::builder()
                .value(false)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ResetInstanceAttribute", checksum = "0c895dd4")]
#[tokio::test]
async fn ec2_reset_instance_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    c.reset_instance_attribute()
        .instance_id(&id)
        .attribute(aws_sdk_ec2::types::InstanceAttributeName::SourceDestCheck)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyInstancePlacement", checksum = "f39ce806")]
#[tokio::test]
async fn ec2_modify_instance_placement() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    c.modify_instance_placement()
        .instance_id(&id)
        .tenancy(aws_sdk_ec2::types::HostTenancy::Default)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyInstanceMetadataOptions", checksum = "50682246")]
#[tokio::test]
async fn ec2_modify_instance_metadata_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    let r = c
        .modify_instance_metadata_options()
        .instance_id(&id)
        .http_tokens(aws_sdk_ec2::types::HttpTokensState::Required)
        .send()
        .await
        .unwrap();
    assert_eq!(r.instance_id(), Some(id.as_str()));
}

#[test_action("ec2", "ModifyInstanceMaintenanceOptions", checksum = "9e473aa4")]
#[tokio::test]
async fn ec2_modify_instance_maintenance_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    let r = c
        .modify_instance_maintenance_options()
        .instance_id(&id)
        .auto_recovery(aws_sdk_ec2::types::InstanceAutoRecoveryState::Default)
        .send()
        .await
        .unwrap();
    assert_eq!(r.instance_id(), Some(id.as_str()));
}

#[test_action("ec2", "ModifyInstanceCpuOptions", checksum = "81502683")]
#[tokio::test]
async fn ec2_modify_instance_cpu_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    let r = c
        .modify_instance_cpu_options()
        .instance_id(&id)
        .core_count(2)
        .threads_per_core(1)
        .send()
        .await
        .unwrap();
    assert_eq!(r.instance_id(), Some(id.as_str()));
}

#[test_action(
    "ec2",
    "ModifyInstanceNetworkPerformanceOptions",
    checksum = "e9023ce5"
)]
#[tokio::test]
async fn ec2_modify_instance_network_performance_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    c.modify_instance_network_performance_options()
        .instance_id(&id)
        .bandwidth_weighting(aws_sdk_ec2::types::InstanceBandwidthWeighting::Default)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyInstanceEventStartTime", checksum = "c31aaff7")]
#[tokio::test]
async fn ec2_modify_instance_event_start_time() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    let r = c
        .modify_instance_event_start_time()
        .instance_id(&id)
        .instance_event_id("instance-event-1")
        .not_before(aws_sdk_ec2::primitives::DateTime::from_secs(1735689600))
        .send()
        .await
        .unwrap();
    assert!(r.event().is_some());
}

#[test_action("ec2", "DescribeInstanceCreditSpecifications", checksum = "a1407717")]
#[tokio::test]
async fn ec2_describe_instance_credit_specifications() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    let r = c
        .describe_instance_credit_specifications()
        .instance_ids(&id)
        .send()
        .await
        .unwrap();
    assert!(r
        .instance_credit_specifications()
        .iter()
        .any(|x| x.instance_id() == Some(id.as_str())));
}

#[test_action("ec2", "ModifyInstanceCreditSpecification", checksum = "fc50a3fd")]
#[tokio::test]
async fn ec2_modify_instance_credit_specification() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    c.modify_instance_credit_specification()
        .instance_credit_specifications(
            aws_sdk_ec2::types::InstanceCreditSpecificationRequest::builder()
                .instance_id(&id)
                .cpu_credits("standard")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetInstanceMetadataDefaults", checksum = "698833f6")]
#[tokio::test]
async fn ec2_get_instance_metadata_defaults() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.get_instance_metadata_defaults().send().await.unwrap();
    assert!(r.account_level().is_some());
}

#[test_action("ec2", "ModifyInstanceMetadataDefaults", checksum = "fc3530be")]
#[tokio::test]
async fn ec2_modify_instance_metadata_defaults() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_instance_metadata_defaults()
        .http_tokens(aws_sdk_ec2::types::MetadataDefaultHttpTokensState::Required)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "RegisterInstanceEventNotificationAttributes",
    checksum = "094fcc20"
)]
#[tokio::test]
async fn ec2_register_event_notification_attributes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .register_instance_event_notification_attributes()
        .instance_tag_attribute(
            aws_sdk_ec2::types::RegisterInstanceTagAttributeRequest::builder()
                .include_all_tags_of_instance(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(r.instance_tag_attribute().is_some());
}

#[test_action(
    "ec2",
    "DeregisterInstanceEventNotificationAttributes",
    checksum = "4e4aa744"
)]
#[tokio::test]
async fn ec2_deregister_event_notification_attributes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.deregister_instance_event_notification_attributes()
        .instance_tag_attribute(
            aws_sdk_ec2::types::DeregisterInstanceTagAttributeRequest::builder()
                .include_all_tags_of_instance(false)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "DescribeInstanceEventNotificationAttributes",
    checksum = "18c8e6c9"
)]
#[tokio::test]
async fn ec2_describe_event_notification_attributes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .describe_instance_event_notification_attributes()
        .send()
        .await
        .unwrap();
    assert!(r.instance_tag_attribute().is_some());
}

#[test_action("ec2", "ReportInstanceStatus", checksum = "4e11dd5d")]
#[tokio::test]
async fn ec2_report_instance_status() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run_one(&c).await;
    c.report_instance_status()
        .instances(&id)
        .status(aws_sdk_ec2::types::ReportStatusType::Ok)
        .reason_codes(aws_sdk_ec2::types::ReportInstanceReasonCodes::Other)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeInstanceTopology", checksum = "ebb979fc")]
#[tokio::test]
async fn ec2_describe_instance_topology() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_instance_topology().send().await.unwrap();
    assert!(r.instances().is_empty());
}

// ---- EBS volumes ----

async fn make_vol(c: &aws_sdk_ec2::Client) -> String {
    c.create_volume()
        .availability_zone("us-east-1a")
        .size(8)
        .send()
        .await
        .unwrap()
        .volume_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateVolume", checksum = "0e36827b")]
#[tokio::test]
async fn ec2_create_volume() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_volume()
        .availability_zone("us-east-1a")
        .size(10)
        .volume_type(aws_sdk_ec2::types::VolumeType::Gp3)
        .send()
        .await
        .unwrap();
    assert!(r.volume_id().unwrap().starts_with("vol-"));
    assert_eq!(r.size(), Some(10));
}

#[test_action("ec2", "DescribeVolumes", checksum = "9dfe1d7b")]
#[tokio::test]
async fn ec2_describe_volumes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    let r = c.describe_volumes().volume_ids(&id).send().await.unwrap();
    assert_eq!(r.volumes().len(), 1);
}

#[test_action("ec2", "DeleteVolume", checksum = "bbaa86a2")]
#[tokio::test]
async fn ec2_delete_volume() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    c.delete_volume().volume_id(&id).send().await.unwrap();
    assert!(c
        .describe_volumes()
        .send()
        .await
        .unwrap()
        .volumes()
        .is_empty());
}

#[test_action("ec2", "AttachVolume", checksum = "2567d4b9")]
#[tokio::test]
async fn ec2_attach_volume() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    let r = c
        .attach_volume()
        .volume_id(&id)
        .instance_id("i-1")
        .device("/dev/sdf")
        .send()
        .await
        .unwrap();
    assert_eq!(r.volume_id(), Some(id.as_str()));
}

#[test_action("ec2", "DetachVolume", checksum = "459f4614")]
#[tokio::test]
async fn ec2_detach_volume() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    c.attach_volume()
        .volume_id(&id)
        .instance_id("i-1")
        .device("/dev/sdf")
        .send()
        .await
        .unwrap();
    let r = c.detach_volume().volume_id(&id).send().await.unwrap();
    assert_eq!(r.volume_id(), Some(id.as_str()));
}

#[test_action("ec2", "ModifyVolume", checksum = "a66e5eb0")]
#[tokio::test]
async fn ec2_modify_volume() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    let r = c
        .modify_volume()
        .volume_id(&id)
        .size(16)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.volume_modification().unwrap().volume_id(),
        Some(id.as_str())
    );
}

#[test_action("ec2", "DescribeVolumesModifications", checksum = "1a5f1257")]
#[tokio::test]
async fn ec2_describe_volumes_modifications() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    let r = c
        .describe_volumes_modifications()
        .volume_ids(&id)
        .send()
        .await
        .unwrap();
    assert!(r
        .volumes_modifications()
        .iter()
        .any(|m| m.volume_id() == Some(id.as_str())));
}

#[test_action("ec2", "DescribeVolumeStatus", checksum = "f77ed7db")]
#[tokio::test]
async fn ec2_describe_volume_status() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    let r = c
        .describe_volume_status()
        .volume_ids(&id)
        .send()
        .await
        .unwrap();
    assert!(r
        .volume_statuses()
        .iter()
        .any(|x| x.volume_id() == Some(id.as_str())));
}

#[test_action("ec2", "DescribeVolumeAttribute", checksum = "3b62cb3d")]
#[tokio::test]
async fn ec2_describe_volume_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    let r = c
        .describe_volume_attribute()
        .volume_id(&id)
        .attribute(aws_sdk_ec2::types::VolumeAttributeName::AutoEnableIo)
        .send()
        .await
        .unwrap();
    assert_eq!(r.volume_id(), Some(id.as_str()));
}

#[test_action("ec2", "ModifyVolumeAttribute", checksum = "c35563ee")]
#[tokio::test]
async fn ec2_modify_volume_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    c.modify_volume_attribute()
        .volume_id(&id)
        .auto_enable_io(
            aws_sdk_ec2::types::AttributeBooleanValue::builder()
                .value(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableVolumeIO", checksum = "fd7c6596")]
#[tokio::test]
async fn ec2_enable_volume_io() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    c.enable_volume_io().volume_id(&id).send().await.unwrap();
}

#[test_action("ec2", "ListVolumesInRecycleBin", checksum = "ae137dee")]
#[tokio::test]
async fn ec2_list_volumes_in_recycle_bin() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.list_volumes_in_recycle_bin().send().await.unwrap();
    assert!(r.volumes().is_empty());
}

#[test_action("ec2", "RestoreVolumeFromRecycleBin", checksum = "3c097b95")]
#[tokio::test]
async fn ec2_restore_volume_from_recycle_bin() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vol(&c).await;
    c.restore_volume_from_recycle_bin()
        .volume_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetEbsEncryptionByDefault", checksum = "79f3757d")]
#[tokio::test]
async fn ec2_get_ebs_encryption_by_default() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.get_ebs_encryption_by_default().send().await.unwrap();
    assert_eq!(r.ebs_encryption_by_default(), Some(false));
}

#[test_action("ec2", "EnableEbsEncryptionByDefault", checksum = "39c27fb0")]
#[tokio::test]
async fn ec2_enable_ebs_encryption_by_default() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.enable_ebs_encryption_by_default().send().await.unwrap();
    assert_eq!(r.ebs_encryption_by_default(), Some(true));
}

#[test_action("ec2", "DisableEbsEncryptionByDefault", checksum = "7a73e9a7")]
#[tokio::test]
async fn ec2_disable_ebs_encryption_by_default() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.disable_ebs_encryption_by_default().send().await.unwrap();
    assert_eq!(r.ebs_encryption_by_default(), Some(false));
}

#[test_action("ec2", "GetEbsDefaultKmsKeyId", checksum = "1f4cf06a")]
#[tokio::test]
async fn ec2_get_ebs_default_kms_key_id() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.get_ebs_default_kms_key_id().send().await.unwrap();
    assert!(r.kms_key_id().is_some());
}

#[test_action("ec2", "ModifyEbsDefaultKmsKeyId", checksum = "fe8784cc")]
#[tokio::test]
async fn ec2_modify_ebs_default_kms_key_id() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .modify_ebs_default_kms_key_id()
        .kms_key_id("alias/my-key")
        .send()
        .await
        .unwrap();
    assert_eq!(r.kms_key_id(), Some("alias/my-key"));
}

#[test_action("ec2", "ResetEbsDefaultKmsKeyId", checksum = "176ef84e")]
#[tokio::test]
async fn ec2_reset_ebs_default_kms_key_id() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.reset_ebs_default_kms_key_id().send().await.unwrap();
    assert!(r.kms_key_id().is_some());
}

// ---- EBS snapshots ----

async fn make_snap(c: &aws_sdk_ec2::Client) -> String {
    c.create_snapshot()
        .volume_id("vol-0123456789abcdef0")
        .send()
        .await
        .unwrap()
        .snapshot_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateSnapshot", checksum = "1b208998")]
#[tokio::test]
async fn ec2_create_snapshot() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_snapshot()
        .volume_id("vol-1")
        .description("snap")
        .send()
        .await
        .unwrap();
    assert!(r.snapshot_id().unwrap().starts_with("snap-"));
}

#[test_action("ec2", "CreateSnapshots", checksum = "530f0177")]
#[tokio::test]
async fn ec2_create_snapshots() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_snapshots()
        .instance_specification(
            aws_sdk_ec2::types::InstanceSpecification::builder()
                .instance_id("i-1")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(!r.snapshots().is_empty());
}

#[test_action("ec2", "DescribeSnapshots", checksum = "9e5404cd")]
#[tokio::test]
async fn ec2_describe_snapshots() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_snap(&c).await;
    let r = c
        .describe_snapshots()
        .snapshot_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.snapshots().len(), 1);
}

#[test_action("ec2", "DeleteSnapshot", checksum = "6d8abe5b")]
#[tokio::test]
async fn ec2_delete_snapshot() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_snap(&c).await;
    c.delete_snapshot().snapshot_id(&id).send().await.unwrap();
    assert!(c
        .describe_snapshots()
        .send()
        .await
        .unwrap()
        .snapshots()
        .is_empty());
}

#[test_action("ec2", "CopySnapshot", checksum = "d0ffe09e")]
#[tokio::test]
async fn ec2_copy_snapshot() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .copy_snapshot()
        .source_region("us-west-2")
        .source_snapshot_id("snap-1")
        .send()
        .await
        .unwrap();
    assert!(r.snapshot_id().unwrap().starts_with("snap-"));
}

#[test_action("ec2", "DescribeSnapshotAttribute", checksum = "1d9eec7e")]
#[tokio::test]
async fn ec2_describe_snapshot_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_snap(&c).await;
    let r = c
        .describe_snapshot_attribute()
        .snapshot_id(&id)
        .attribute(aws_sdk_ec2::types::SnapshotAttributeName::CreateVolumePermission)
        .send()
        .await
        .unwrap();
    assert_eq!(r.snapshot_id(), Some(id.as_str()));
}

#[test_action("ec2", "ModifySnapshotAttribute", checksum = "01263a58")]
#[tokio::test]
async fn ec2_modify_snapshot_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_snap(&c).await;
    c.modify_snapshot_attribute()
        .snapshot_id(&id)
        .attribute(aws_sdk_ec2::types::SnapshotAttributeName::CreateVolumePermission)
        .operation_type(aws_sdk_ec2::types::OperationType::Add)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ResetSnapshotAttribute", checksum = "8815b07b")]
#[tokio::test]
async fn ec2_reset_snapshot_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_snap(&c).await;
    c.reset_snapshot_attribute()
        .snapshot_id(&id)
        .attribute(aws_sdk_ec2::types::SnapshotAttributeName::CreateVolumePermission)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifySnapshotTier", checksum = "90f04b23")]
#[tokio::test]
async fn ec2_modify_snapshot_tier() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_snap(&c).await;
    let r = c
        .modify_snapshot_tier()
        .snapshot_id(&id)
        .storage_tier(aws_sdk_ec2::types::TargetStorageTier::Archive)
        .send()
        .await
        .unwrap();
    assert_eq!(r.snapshot_id(), Some(id.as_str()));
}

#[test_action("ec2", "DescribeSnapshotTierStatus", checksum = "36947df9")]
#[tokio::test]
async fn ec2_describe_snapshot_tier_status() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_snap(&c).await;
    let r = c.describe_snapshot_tier_status().send().await.unwrap();
    assert!(!r.snapshot_tier_statuses().is_empty());
}

#[test_action("ec2", "RestoreSnapshotTier", checksum = "eb57ff4e")]
#[tokio::test]
async fn ec2_restore_snapshot_tier() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_snap(&c).await;
    let r = c
        .restore_snapshot_tier()
        .snapshot_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.snapshot_id(), Some(id.as_str()));
}

#[test_action("ec2", "ListSnapshotsInRecycleBin", checksum = "b5671c23")]
#[tokio::test]
async fn ec2_list_snapshots_in_recycle_bin() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.list_snapshots_in_recycle_bin().send().await.unwrap();
    assert!(r.snapshots().is_empty());
}

#[test_action("ec2", "RestoreSnapshotFromRecycleBin", checksum = "6c5d93bf")]
#[tokio::test]
async fn ec2_restore_snapshot_from_recycle_bin() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_snap(&c).await;
    c.restore_snapshot_from_recycle_bin()
        .snapshot_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "LockSnapshot", checksum = "66fee00e")]
#[tokio::test]
async fn ec2_lock_snapshot() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_snap(&c).await;
    let r = c
        .lock_snapshot()
        .snapshot_id(&id)
        .lock_mode(aws_sdk_ec2::types::LockMode::Governance)
        .send()
        .await
        .unwrap();
    assert_eq!(r.snapshot_id(), Some(id.as_str()));
}

#[test_action("ec2", "UnlockSnapshot", checksum = "a3352f4f")]
#[tokio::test]
async fn ec2_unlock_snapshot() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_snap(&c).await;
    c.lock_snapshot()
        .snapshot_id(&id)
        .lock_mode(aws_sdk_ec2::types::LockMode::Governance)
        .send()
        .await
        .unwrap();
    let r = c.unlock_snapshot().snapshot_id(&id).send().await.unwrap();
    assert_eq!(r.snapshot_id(), Some(id.as_str()));
}

#[test_action("ec2", "DescribeLockedSnapshots", checksum = "d1c98335")]
#[tokio::test]
async fn ec2_describe_locked_snapshots() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_locked_snapshots().send().await.unwrap();
    assert!(r.snapshots().is_empty());
}

#[test_action("ec2", "GetSnapshotBlockPublicAccessState", checksum = "507d9bcf")]
#[tokio::test]
async fn ec2_get_snapshot_bpa_state() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_snapshot_block_public_access_state()
        .send()
        .await
        .unwrap();
    assert!(r.state().is_some());
}

#[test_action("ec2", "EnableSnapshotBlockPublicAccess", checksum = "0a5b93f4")]
#[tokio::test]
async fn ec2_enable_snapshot_bpa() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .enable_snapshot_block_public_access()
        .state(aws_sdk_ec2::types::SnapshotBlockPublicAccessState::BlockAllSharing)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.state(),
        Some(&aws_sdk_ec2::types::SnapshotBlockPublicAccessState::BlockAllSharing)
    );
}

#[test_action("ec2", "DisableSnapshotBlockPublicAccess", checksum = "03130acb")]
#[tokio::test]
async fn ec2_disable_snapshot_bpa() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disable_snapshot_block_public_access()
        .send()
        .await
        .unwrap();
    assert!(r.state().is_some());
}

#[test_action("ec2", "EnableFastSnapshotRestores", checksum = "3bd2a3e8")]
#[tokio::test]
async fn ec2_enable_fast_snapshot_restores() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .enable_fast_snapshot_restores()
        .source_snapshot_ids("snap-1")
        .availability_zones("us-east-1a")
        .send()
        .await
        .unwrap();
    assert!(!r.successful().is_empty());
}

#[test_action("ec2", "DisableFastSnapshotRestores", checksum = "b54a5341")]
#[tokio::test]
async fn ec2_disable_fast_snapshot_restores() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disable_fast_snapshot_restores()
        .source_snapshot_ids("snap-1")
        .availability_zones("us-east-1a")
        .send()
        .await
        .unwrap();
    assert!(!r.successful().is_empty());
}

#[test_action("ec2", "DescribeFastSnapshotRestores", checksum = "57fc68c7")]
#[tokio::test]
async fn ec2_describe_fast_snapshot_restores() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_fast_snapshot_restores().send().await.unwrap();
    assert!(r.fast_snapshot_restores().is_empty());
}

// ---- AMIs / images ----

async fn make_ami(c: &aws_sdk_ec2::Client) -> String {
    c.register_image()
        .name("ami-test")
        .send()
        .await
        .unwrap()
        .image_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateImage", checksum = "e8a35566")]
#[tokio::test]
async fn ec2_create_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_image()
        .instance_id("i-1")
        .name("my-ami")
        .send()
        .await
        .unwrap();
    assert!(r.image_id().unwrap().starts_with("ami-"));
}

#[test_action("ec2", "RegisterImage", checksum = "034deb69")]
#[tokio::test]
async fn ec2_register_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .register_image()
        .name("reg-ami")
        .architecture(aws_sdk_ec2::types::ArchitectureValues::X8664)
        .send()
        .await
        .unwrap();
    assert!(r.image_id().unwrap().starts_with("ami-"));
}

#[test_action("ec2", "DescribeImages", checksum = "b00eefc4")]
#[tokio::test]
async fn ec2_describe_images() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    let r = c.describe_images().image_ids(&id).send().await.unwrap();
    assert_eq!(r.images().len(), 1);
}

#[test_action("ec2", "DeregisterImage", checksum = "30602ffb")]
#[tokio::test]
async fn ec2_deregister_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    c.deregister_image().image_id(&id).send().await.unwrap();
    // The deregistered AMI is gone. (A no-filter DescribeImages is NOT empty —
    // every account sees the seeded public AMI catalogue, as in real AWS — so
    // assert on the specific id rather than the whole image set.)
    assert!(c
        .describe_images()
        .image_ids(&id)
        .send()
        .await
        .unwrap()
        .images()
        .is_empty());
}

#[test_action("ec2", "CopyImage", checksum = "021e759a")]
#[tokio::test]
async fn ec2_copy_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .copy_image()
        .name("copy")
        .source_image_id("ami-1")
        .source_region("us-west-2")
        .send()
        .await
        .unwrap();
    assert!(r.image_id().unwrap().starts_with("ami-"));
}

#[test_action("ec2", "DescribeImageAttribute", checksum = "78760c11")]
#[tokio::test]
async fn ec2_describe_image_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    let r = c
        .describe_image_attribute()
        .image_id(&id)
        .attribute(aws_sdk_ec2::types::ImageAttributeName::LaunchPermission)
        .send()
        .await
        .unwrap();
    assert_eq!(r.image_id(), Some(id.as_str()));
}

#[test_action("ec2", "ModifyImageAttribute", checksum = "08749225")]
#[tokio::test]
async fn ec2_modify_image_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    c.modify_image_attribute()
        .image_id(&id)
        .operation_type(aws_sdk_ec2::types::OperationType::Add)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ResetImageAttribute", checksum = "1eb818d6")]
#[tokio::test]
async fn ec2_reset_image_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    c.reset_image_attribute()
        .image_id(&id)
        .attribute(aws_sdk_ec2::types::ResetImageAttributeName::LaunchPermission)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableImage", checksum = "75872386")]
#[tokio::test]
async fn ec2_enable_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    let r = c.enable_image().image_id(&id).send().await.unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "DisableImage", checksum = "29789b5d")]
#[tokio::test]
async fn ec2_disable_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    let r = c.disable_image().image_id(&id).send().await.unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "EnableImageDeprecation", checksum = "f22f3a28")]
#[tokio::test]
async fn ec2_enable_image_deprecation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    c.enable_image_deprecation()
        .image_id(&id)
        .deprecate_at(aws_sdk_ec2::primitives::DateTime::from_secs(1893456000))
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisableImageDeprecation", checksum = "c494e658")]
#[tokio::test]
async fn ec2_disable_image_deprecation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    c.disable_image_deprecation()
        .image_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableImageDeregistrationProtection", checksum = "bc98cae8")]
#[tokio::test]
async fn ec2_enable_image_deregistration_protection() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    c.enable_image_deregistration_protection()
        .image_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisableImageDeregistrationProtection", checksum = "2ad10899")]
#[tokio::test]
async fn ec2_disable_image_deregistration_protection() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    c.disable_image_deregistration_protection()
        .image_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CancelImageLaunchPermission", checksum = "33a8e901")]
#[tokio::test]
async fn ec2_cancel_image_launch_permission() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    c.cancel_image_launch_permission()
        .image_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "RestoreImageFromRecycleBin", checksum = "e9b377e7")]
#[tokio::test]
async fn ec2_restore_image_from_recycle_bin() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    c.restore_image_from_recycle_bin()
        .image_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableImageBlockPublicAccess", checksum = "cf19eba9")]
#[tokio::test]
async fn ec2_enable_image_bpa() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .enable_image_block_public_access()
        .image_block_public_access_state(
            aws_sdk_ec2::types::ImageBlockPublicAccessEnabledState::BlockNewSharing,
        )
        .send()
        .await
        .unwrap();
    assert!(r.image_block_public_access_state().is_some());
}

#[test_action("ec2", "DisableImageBlockPublicAccess", checksum = "e89c624a")]
#[tokio::test]
async fn ec2_disable_image_bpa() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.disable_image_block_public_access().send().await.unwrap();
    assert!(r.image_block_public_access_state().is_some());
}

#[test_action("ec2", "GetImageBlockPublicAccessState", checksum = "d3b3b93f")]
#[tokio::test]
async fn ec2_get_image_bpa_state() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_image_block_public_access_state()
        .send()
        .await
        .unwrap();
    assert!(r.image_block_public_access_state().is_some());
}

#[test_action("ec2", "EnableAllowedImagesSettings", checksum = "d9532acd")]
#[tokio::test]
async fn ec2_enable_allowed_images_settings() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .enable_allowed_images_settings()
        .allowed_images_settings_state(
            aws_sdk_ec2::types::AllowedImagesSettingsEnabledState::Enabled,
        )
        .send()
        .await
        .unwrap();
    assert!(r.allowed_images_settings_state().is_some());
}

#[test_action("ec2", "DisableAllowedImagesSettings", checksum = "d7b64e84")]
#[tokio::test]
async fn ec2_disable_allowed_images_settings() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.disable_allowed_images_settings().send().await.unwrap();
    assert!(r.allowed_images_settings_state().is_some());
}

#[test_action("ec2", "GetAllowedImagesSettings", checksum = "4cd2eb06")]
#[tokio::test]
async fn ec2_get_allowed_images_settings() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.get_allowed_images_settings().send().await.unwrap();
    assert!(r.state().is_some());
}

#[test_action(
    "ec2",
    "ReplaceImageCriteriaInAllowedImagesSettings",
    checksum = "83c01581"
)]
#[tokio::test]
async fn ec2_replace_image_criteria() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.replace_image_criteria_in_allowed_images_settings()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ListImagesInRecycleBin", checksum = "f7d00dab")]
#[tokio::test]
async fn ec2_list_images_in_recycle_bin() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.list_images_in_recycle_bin().send().await.unwrap();
    assert!(r.images().is_empty());
}

#[test_action("ec2", "CreateStoreImageTask", checksum = "9a801eaf")]
#[tokio::test]
async fn ec2_create_store_image_task() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;
    let r = c
        .create_store_image_task()
        .image_id(&id)
        .bucket("my-bucket")
        .send()
        .await
        .unwrap();
    assert!(r.object_key().is_some());
}

#[test_action("ec2", "DescribeStoreImageTasks", checksum = "5289475e")]
#[tokio::test]
async fn ec2_describe_store_image_tasks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_store_image_tasks().send().await.unwrap();
    assert!(r.store_image_task_results().is_empty());
}

#[test_action("ec2", "CreateRestoreImageTask", checksum = "61e48f5a")]
#[tokio::test]
async fn ec2_create_restore_image_task() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_restore_image_task()
        .bucket("my-bucket")
        .object_key("ami.bin")
        .send()
        .await
        .unwrap();
    assert!(r.image_id().unwrap().starts_with("ami-"));
}

#[test_action("ec2", "DescribeFastLaunchImages", checksum = "375419a1")]
#[tokio::test]
async fn ec2_describe_fast_launch_images() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_fast_launch_images().send().await.unwrap();
    assert!(r.fast_launch_images().is_empty());
}

// ---- network ACLs ----

async fn make_nacl(c: &aws_sdk_ec2::Client) -> String {
    c.create_network_acl()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .network_acl()
        .unwrap()
        .network_acl_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateNetworkAcl", checksum = "7ceb4c2d")]
#[tokio::test]
async fn ec2_create_network_acl() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.create_network_acl().vpc_id("vpc-1").send().await.unwrap();
    assert!(r
        .network_acl()
        .unwrap()
        .network_acl_id()
        .unwrap()
        .starts_with("acl-"));
}

#[test_action("ec2", "DescribeNetworkAcls", checksum = "990d0d1e")]
#[tokio::test]
async fn ec2_describe_network_acls() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_nacl(&c).await;
    let r = c
        .describe_network_acls()
        .network_acl_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.network_acls().len(), 1);
}

#[test_action("ec2", "DeleteNetworkAcl", checksum = "58322c5e")]
#[tokio::test]
async fn ec2_delete_network_acl() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_nacl(&c).await;
    c.delete_network_acl()
        .network_acl_id(&id)
        .send()
        .await
        .unwrap();
    // The deleted NACL is gone. (The account's default NACL still exists, like
    // AWS — so assert the specific id is absent rather than an empty list.)
    assert!(!c
        .describe_network_acls()
        .send()
        .await
        .unwrap()
        .network_acls()
        .iter()
        .any(|n| n.network_acl_id() == Some(id.as_str())));
}

#[test_action("ec2", "CreateNetworkAclEntry", checksum = "084c817c")]
#[tokio::test]
async fn ec2_create_network_acl_entry() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_nacl(&c).await;
    c.create_network_acl_entry()
        .network_acl_id(&id)
        .rule_number(100)
        .protocol("-1")
        .rule_action(aws_sdk_ec2::types::RuleAction::Allow)
        .egress(false)
        .cidr_block("0.0.0.0/0")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ReplaceNetworkAclEntry", checksum = "598ce276")]
#[tokio::test]
async fn ec2_replace_network_acl_entry() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_nacl(&c).await;
    c.create_network_acl_entry()
        .network_acl_id(&id)
        .rule_number(100)
        .protocol("-1")
        .rule_action(aws_sdk_ec2::types::RuleAction::Allow)
        .egress(false)
        .cidr_block("0.0.0.0/0")
        .send()
        .await
        .unwrap();
    c.replace_network_acl_entry()
        .network_acl_id(&id)
        .rule_number(100)
        .protocol("6")
        .rule_action(aws_sdk_ec2::types::RuleAction::Deny)
        .egress(false)
        .cidr_block("0.0.0.0/0")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteNetworkAclEntry", checksum = "183877c6")]
#[tokio::test]
async fn ec2_delete_network_acl_entry() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_nacl(&c).await;
    c.create_network_acl_entry()
        .network_acl_id(&id)
        .rule_number(100)
        .protocol("-1")
        .rule_action(aws_sdk_ec2::types::RuleAction::Allow)
        .egress(false)
        .cidr_block("0.0.0.0/0")
        .send()
        .await
        .unwrap();
    c.delete_network_acl_entry()
        .network_acl_id(&id)
        .rule_number(100)
        .egress(false)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ReplaceNetworkAclAssociation", checksum = "77edbfc0")]
#[tokio::test]
async fn ec2_replace_network_acl_association() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_nacl(&c).await;
    let r = c
        .replace_network_acl_association()
        .association_id("aclassoc-1")
        .network_acl_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.new_association_id().is_some());
}

// ---- VPC peering ----

async fn make_pcx(c: &aws_sdk_ec2::Client) -> String {
    c.create_vpc_peering_connection()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .vpc_peering_connection()
        .unwrap()
        .vpc_peering_connection_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateVpcPeeringConnection", checksum = "654dd690")]
#[tokio::test]
async fn ec2_create_vpc_peering_connection() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_vpc_peering_connection()
        .vpc_id("vpc-1")
        .peer_vpc_id("vpc-2")
        .send()
        .await
        .unwrap();
    assert!(r
        .vpc_peering_connection()
        .unwrap()
        .vpc_peering_connection_id()
        .unwrap()
        .starts_with("pcx-"));
}

#[test_action("ec2", "DescribeVpcPeeringConnections", checksum = "5e2969bd")]
#[tokio::test]
async fn ec2_describe_vpc_peering_connections() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pcx(&c).await;
    let r = c
        .describe_vpc_peering_connections()
        .vpc_peering_connection_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.vpc_peering_connections().len(), 1);
}

#[test_action("ec2", "AcceptVpcPeeringConnection", checksum = "5a22d8e9")]
#[tokio::test]
async fn ec2_accept_vpc_peering_connection() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pcx(&c).await;
    let r = c
        .accept_vpc_peering_connection()
        .vpc_peering_connection_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.vpc_peering_connection()
            .unwrap()
            .vpc_peering_connection_id(),
        Some(id.as_str())
    );
}

#[test_action("ec2", "RejectVpcPeeringConnection", checksum = "f51d6b1e")]
#[tokio::test]
async fn ec2_reject_vpc_peering_connection() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pcx(&c).await;
    c.reject_vpc_peering_connection()
        .vpc_peering_connection_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteVpcPeeringConnection", checksum = "cbeea771")]
#[tokio::test]
async fn ec2_delete_vpc_peering_connection() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pcx(&c).await;
    c.delete_vpc_peering_connection()
        .vpc_peering_connection_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyVpcPeeringConnectionOptions", checksum = "e81c8d38")]
#[tokio::test]
async fn ec2_modify_vpc_peering_connection_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pcx(&c).await;
    let r = c
        .modify_vpc_peering_connection_options()
        .vpc_peering_connection_id(&id)
        .requester_peering_connection_options(
            aws_sdk_ec2::types::PeeringConnectionOptionsRequest::builder()
                .allow_dns_resolution_from_remote_vpc(false)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(r.requester_peering_connection_options().is_some());
}

// ---- VPC endpoints ----

async fn make_vpce(c: &aws_sdk_ec2::Client) -> String {
    c.create_vpc_endpoint()
        .vpc_id("vpc-1")
        .service_name("com.amazonaws.us-east-1.s3")
        .send()
        .await
        .unwrap()
        .vpc_endpoint()
        .unwrap()
        .vpc_endpoint_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateVpcEndpoint", checksum = "5d77f198")]
#[tokio::test]
async fn ec2_create_vpc_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_vpc_endpoint()
        .vpc_id("vpc-1")
        .service_name("com.amazonaws.us-east-1.s3")
        .vpc_endpoint_type(aws_sdk_ec2::types::VpcEndpointType::Gateway)
        .send()
        .await
        .unwrap();
    assert!(r
        .vpc_endpoint()
        .unwrap()
        .vpc_endpoint_id()
        .unwrap()
        .starts_with("vpce-"));
}

#[test_action("ec2", "DescribeVpcEndpoints", checksum = "08305ed3")]
#[tokio::test]
async fn ec2_describe_vpc_endpoints() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpce(&c).await;
    let r = c
        .describe_vpc_endpoints()
        .vpc_endpoint_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.vpc_endpoints().len(), 1);
}

#[test_action("ec2", "DeleteVpcEndpoints", checksum = "ee333256")]
#[tokio::test]
async fn ec2_delete_vpc_endpoints() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpce(&c).await;
    c.delete_vpc_endpoints()
        .vpc_endpoint_ids(&id)
        .send()
        .await
        .unwrap();
    assert!(c
        .describe_vpc_endpoints()
        .send()
        .await
        .unwrap()
        .vpc_endpoints()
        .is_empty());
}

#[test_action("ec2", "ModifyVpcEndpoint", checksum = "ccad418e")]
#[tokio::test]
async fn ec2_modify_vpc_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpce(&c).await;
    let r = c
        .modify_vpc_endpoint()
        .vpc_endpoint_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "DescribeVpcEndpointServices", checksum = "9a48aaa6")]
#[tokio::test]
async fn ec2_describe_vpc_endpoint_services() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_vpc_endpoint_services().send().await.unwrap();
    assert!(!r.service_names().is_empty());
}

#[test_action("ec2", "DescribeVpcEndpointConnections", checksum = "1f07a4c8")]
#[tokio::test]
async fn ec2_describe_vpc_endpoint_connections() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_vpc_endpoint_connections().send().await.unwrap();
    assert!(r.vpc_endpoint_connections().is_empty());
}

#[test_action("ec2", "AcceptVpcEndpointConnections", checksum = "c0f5e44f")]
#[tokio::test]
async fn ec2_accept_vpc_endpoint_connections() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .accept_vpc_endpoint_connections()
        .service_id("vpce-svc-1")
        .vpc_endpoint_ids("vpce-1")
        .send()
        .await
        .unwrap();
    assert!(r.unsuccessful().is_empty());
}

#[test_action("ec2", "RejectVpcEndpointConnections", checksum = "9267e834")]
#[tokio::test]
async fn ec2_reject_vpc_endpoint_connections() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .reject_vpc_endpoint_connections()
        .service_id("vpce-svc-1")
        .vpc_endpoint_ids("vpce-1")
        .send()
        .await
        .unwrap();
    assert!(r.unsuccessful().is_empty());
}

#[test_action("ec2", "CreateVpcEndpointServiceConfiguration", checksum = "e7523b64")]
#[tokio::test]
async fn ec2_create_vpc_endpoint_service_configuration() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_vpc_endpoint_service_configuration()
        .network_load_balancer_arns("arn:nlb")
        .send()
        .await
        .unwrap();
    assert!(r
        .service_configuration()
        .unwrap()
        .service_id()
        .unwrap()
        .starts_with("vpce-svc-"));
}

#[test_action(
    "ec2",
    "DescribeVpcEndpointServiceConfigurations",
    checksum = "da49eaab"
)]
#[tokio::test]
async fn ec2_describe_vpc_endpoint_service_configurations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_vpc_endpoint_service_configuration()
        .send()
        .await
        .unwrap();
    let r = c
        .describe_vpc_endpoint_service_configurations()
        .send()
        .await
        .unwrap();
    assert!(!r.service_configurations().is_empty());
}

#[test_action("ec2", "DeleteVpcEndpointServiceConfigurations", checksum = "2ab9546c")]
#[tokio::test]
async fn ec2_delete_vpc_endpoint_service_configurations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_vpc_endpoint_service_configuration()
        .send()
        .await
        .unwrap()
        .service_configuration()
        .unwrap()
        .service_id()
        .unwrap()
        .to_string();
    let r = c
        .delete_vpc_endpoint_service_configurations()
        .service_ids(&id)
        .send()
        .await
        .unwrap();
    assert!(r.unsuccessful().is_empty());
}

#[test_action("ec2", "ModifyVpcEndpointServiceConfiguration", checksum = "99fe7c86")]
#[tokio::test]
async fn ec2_modify_vpc_endpoint_service_configuration() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .modify_vpc_endpoint_service_configuration()
        .service_id("vpce-svc-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "DescribeVpcEndpointServicePermissions", checksum = "d427ceca")]
#[tokio::test]
async fn ec2_describe_vpc_endpoint_service_permissions() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .describe_vpc_endpoint_service_permissions()
        .service_id("vpce-svc-1")
        .send()
        .await
        .unwrap();
    assert!(r.allowed_principals().is_empty());
}

#[test_action("ec2", "ModifyVpcEndpointServicePermissions", checksum = "cd80a919")]
#[tokio::test]
async fn ec2_modify_vpc_endpoint_service_permissions() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_vpc_endpoint_service_permissions()
        .service_id("vpce-svc-1")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "ModifyVpcEndpointServicePayerResponsibility",
    checksum = "602d6807"
)]
#[tokio::test]
async fn ec2_modify_vpc_endpoint_service_payer_responsibility() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_vpc_endpoint_service_payer_responsibility()
        .service_id("vpce-svc-1")
        .payer_responsibility(aws_sdk_ec2::types::PayerResponsibility::ServiceOwner)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "StartVpcEndpointServicePrivateDnsVerification",
    checksum = "4decf2c2"
)]
#[tokio::test]
async fn ec2_start_vpc_endpoint_service_private_dns_verification() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.start_vpc_endpoint_service_private_dns_verification()
        .service_id("vpce-svc-1")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "CreateVpcEndpointConnectionNotification",
    checksum = "4a635d6a"
)]
#[tokio::test]
async fn ec2_create_vpc_endpoint_connection_notification() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_vpc_endpoint_connection_notification()
        .connection_notification_arn("arn:sns:topic")
        .connection_events("Accept")
        .send()
        .await
        .unwrap();
    assert!(r
        .connection_notification()
        .unwrap()
        .connection_notification_id()
        .unwrap()
        .starts_with("vpce-nfn-"));
}

#[test_action(
    "ec2",
    "DescribeVpcEndpointConnectionNotifications",
    checksum = "d11d1b46"
)]
#[tokio::test]
async fn ec2_describe_vpc_endpoint_connection_notifications() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_vpc_endpoint_connection_notification()
        .connection_notification_arn("arn:sns:topic")
        .connection_events("Accept")
        .send()
        .await
        .unwrap();
    let r = c
        .describe_vpc_endpoint_connection_notifications()
        .send()
        .await
        .unwrap();
    assert!(!r.connection_notification_set().is_empty());
}

#[test_action(
    "ec2",
    "DeleteVpcEndpointConnectionNotifications",
    checksum = "ed6fb014"
)]
#[tokio::test]
async fn ec2_delete_vpc_endpoint_connection_notifications() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_vpc_endpoint_connection_notification()
        .connection_notification_arn("arn:sns:topic")
        .connection_events("Accept")
        .send()
        .await
        .unwrap()
        .connection_notification()
        .unwrap()
        .connection_notification_id()
        .unwrap()
        .to_string();
    let r = c
        .delete_vpc_endpoint_connection_notifications()
        .connection_notification_ids(&id)
        .send()
        .await
        .unwrap();
    assert!(r.unsuccessful().is_empty());
}

#[test_action(
    "ec2",
    "ModifyVpcEndpointConnectionNotification",
    checksum = "eafce495"
)]
#[tokio::test]
async fn ec2_modify_vpc_endpoint_connection_notification() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_vpc_endpoint_connection_notification()
        .connection_notification_id("vpce-nfn-1")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeVpcEndpointAssociations", checksum = "28569dd0")]
#[tokio::test]
async fn ec2_describe_vpc_endpoint_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_vpc_endpoint_associations().send().await.unwrap();
    assert!(r.vpc_endpoint_associations().is_empty());
}

// ---- flow logs ----

#[test_action("ec2", "CreateFlowLogs", checksum = "11dcdafd")]
#[tokio::test]
async fn ec2_create_flow_logs() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_flow_logs()
        .resource_ids("vpc-1")
        .resource_type(aws_sdk_ec2::types::FlowLogsResourceType::Vpc)
        .traffic_type(aws_sdk_ec2::types::TrafficType::All)
        .send()
        .await
        .unwrap();
    assert!(!r.flow_log_ids().is_empty());
}

#[test_action("ec2", "DescribeFlowLogs", checksum = "9207ff36")]
#[tokio::test]
async fn ec2_describe_flow_logs() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_flow_logs()
        .resource_ids("vpc-1")
        .resource_type(aws_sdk_ec2::types::FlowLogsResourceType::Vpc)
        .traffic_type(aws_sdk_ec2::types::TrafficType::All)
        .send()
        .await
        .unwrap();
    let r = c.describe_flow_logs().send().await.unwrap();
    assert!(!r.flow_logs().is_empty());
}

#[test_action("ec2", "DeleteFlowLogs", checksum = "39f0a74d")]
#[tokio::test]
async fn ec2_delete_flow_logs() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_flow_logs()
        .resource_ids("vpc-1")
        .resource_type(aws_sdk_ec2::types::FlowLogsResourceType::Vpc)
        .traffic_type(aws_sdk_ec2::types::TrafficType::All)
        .send()
        .await
        .unwrap();
    let id = r.flow_log_ids()[0].clone();
    let d = c.delete_flow_logs().flow_log_ids(&id).send().await.unwrap();
    assert!(d.unsuccessful().is_empty());
}

#[test_action("ec2", "GetFlowLogsIntegrationTemplate", checksum = "eaca6b3d")]
#[tokio::test]
async fn ec2_get_flow_logs_integration_template() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_flow_logs_integration_template()
        .flow_log_id("fl-1")
        .config_delivery_s3_destination_arn("arn:s3:bucket")
        .integrate_services(aws_sdk_ec2::types::IntegrateServices::builder().build())
        .send()
        .await
        .unwrap();
    assert!(r.result().is_some());
}

// ---- launch templates ----

async fn make_lt(c: &aws_sdk_ec2::Client) -> String {
    c.create_launch_template()
        .launch_template_name("tpl")
        .launch_template_data(
            aws_sdk_ec2::types::RequestLaunchTemplateData::builder()
                .instance_type(aws_sdk_ec2::types::InstanceType::T3Micro)
                .build(),
        )
        .send()
        .await
        .unwrap()
        .launch_template()
        .unwrap()
        .launch_template_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateLaunchTemplate", checksum = "d10d065a")]
#[tokio::test]
async fn ec2_create_launch_template() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_launch_template()
        .launch_template_name("tpl")
        .launch_template_data(
            aws_sdk_ec2::types::RequestLaunchTemplateData::builder()
                .instance_type(aws_sdk_ec2::types::InstanceType::T3Micro)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(r
        .launch_template()
        .unwrap()
        .launch_template_id()
        .unwrap()
        .starts_with("lt-"));
}

#[test_action("ec2", "CreateLaunchTemplateVersion", checksum = "1782f934")]
#[tokio::test]
async fn ec2_create_launch_template_version() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_lt(&c).await;
    let r = c
        .create_launch_template_version()
        .launch_template_id(&id)
        .launch_template_data(
            aws_sdk_ec2::types::RequestLaunchTemplateData::builder()
                .instance_type(aws_sdk_ec2::types::InstanceType::T3Small)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.launch_template_version().unwrap().version_number(),
        Some(2)
    );
}

#[test_action("ec2", "DescribeLaunchTemplates", checksum = "5a101c40")]
#[tokio::test]
async fn ec2_describe_launch_templates() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_lt(&c).await;
    let r = c
        .describe_launch_templates()
        .launch_template_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.launch_templates().len(), 1);
}

#[test_action("ec2", "DescribeLaunchTemplateVersions", checksum = "43bf8e26")]
#[tokio::test]
async fn ec2_describe_launch_template_versions() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_lt(&c).await;
    let r = c
        .describe_launch_template_versions()
        .launch_template_id(&id)
        .send()
        .await
        .unwrap();
    assert!(!r.launch_template_versions().is_empty());
}

#[test_action("ec2", "DeleteLaunchTemplate", checksum = "5ccc84b9")]
#[tokio::test]
async fn ec2_delete_launch_template() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_lt(&c).await;
    c.delete_launch_template()
        .launch_template_id(&id)
        .send()
        .await
        .unwrap();
    assert!(c
        .describe_launch_templates()
        .send()
        .await
        .unwrap()
        .launch_templates()
        .is_empty());
}

#[test_action("ec2", "DeleteLaunchTemplateVersions", checksum = "9309e7fa")]
#[tokio::test]
async fn ec2_delete_launch_template_versions() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_lt(&c).await;
    c.delete_launch_template_versions()
        .launch_template_id(&id)
        .versions("1")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetLaunchTemplateData", checksum = "8427323a")]
#[tokio::test]
async fn ec2_get_launch_template_data() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_launch_template_data()
        .instance_id("i-1")
        .send()
        .await
        .unwrap();
    assert!(r.launch_template_data().is_some());
}

#[test_action("ec2", "ModifyLaunchTemplate", checksum = "eb1af25b")]
#[tokio::test]
async fn ec2_modify_launch_template() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_lt(&c).await;
    let r = c
        .modify_launch_template()
        .launch_template_id(&id)
        .default_version("1")
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.launch_template().unwrap().launch_template_id(),
        Some(id.as_str())
    );
}

// ---- spot instance requests ----

#[test_action("ec2", "RequestSpotInstances", checksum = "67f9936b")]
#[tokio::test]
async fn ec2_request_spot_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .request_spot_instances()
        .spot_price("0.05")
        .instance_count(1)
        .send()
        .await
        .unwrap();
    assert!(!r.spot_instance_requests().is_empty());
}

#[test_action("ec2", "DescribeSpotInstanceRequests", checksum = "d76b6440")]
#[tokio::test]
async fn ec2_describe_spot_instance_requests() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.request_spot_instances()
        .instance_count(1)
        .send()
        .await
        .unwrap();
    let r = c.describe_spot_instance_requests().send().await.unwrap();
    assert!(!r.spot_instance_requests().is_empty());
}

#[test_action("ec2", "CancelSpotInstanceRequests", checksum = "5ee35720")]
#[tokio::test]
async fn ec2_cancel_spot_instance_requests() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .request_spot_instances()
        .instance_count(1)
        .send()
        .await
        .unwrap()
        .spot_instance_requests()[0]
        .spot_instance_request_id()
        .unwrap()
        .to_string();
    let r = c
        .cancel_spot_instance_requests()
        .spot_instance_request_ids(&id)
        .send()
        .await
        .unwrap();
    assert!(!r.cancelled_spot_instance_requests().is_empty());
}

// ---- spot fleet ----

async fn make_sfr(c: &aws_sdk_ec2::Client) -> String {
    c.request_spot_fleet()
        .spot_fleet_request_config(
            aws_sdk_ec2::types::SpotFleetRequestConfigData::builder()
                .iam_fleet_role("arn:role")
                .target_capacity(1)
                .build(),
        )
        .send()
        .await
        .unwrap()
        .spot_fleet_request_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "RequestSpotFleet", checksum = "ef198fa7")]
#[tokio::test]
async fn ec2_request_spot_fleet() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_sfr(&c).await;
    assert!(id.starts_with("sfr-"));
}

#[test_action("ec2", "DescribeSpotFleetRequests", checksum = "c233fdb4")]
#[tokio::test]
async fn ec2_describe_spot_fleet_requests() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_sfr(&c).await;
    let r = c.describe_spot_fleet_requests().send().await.unwrap();
    assert!(!r.spot_fleet_request_configs().is_empty());
}

#[test_action("ec2", "CancelSpotFleetRequests", checksum = "041466d4")]
#[tokio::test]
async fn ec2_cancel_spot_fleet_requests() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_sfr(&c).await;
    let r = c
        .cancel_spot_fleet_requests()
        .spot_fleet_request_ids(&id)
        .terminate_instances(true)
        .send()
        .await
        .unwrap();
    assert!(!r.successful_fleet_requests().is_empty());
}

#[test_action("ec2", "ModifySpotFleetRequest", checksum = "2e883c1b")]
#[tokio::test]
async fn ec2_modify_spot_fleet_request() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_sfr(&c).await;
    c.modify_spot_fleet_request()
        .spot_fleet_request_id(&id)
        .target_capacity(2)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeSpotFleetInstances", checksum = "d933444b")]
#[tokio::test]
async fn ec2_describe_spot_fleet_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_sfr(&c).await;
    let r = c
        .describe_spot_fleet_instances()
        .spot_fleet_request_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.spot_fleet_request_id(), Some(id.as_str()));
}

#[test_action("ec2", "DescribeSpotFleetRequestHistory", checksum = "e9b44ba3")]
#[tokio::test]
async fn ec2_describe_spot_fleet_request_history() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_sfr(&c).await;
    let r = c
        .describe_spot_fleet_request_history()
        .spot_fleet_request_id(&id)
        .start_time(aws_sdk_ec2::primitives::DateTime::from_secs(1704067200))
        .send()
        .await
        .unwrap();
    assert_eq!(r.spot_fleet_request_id(), Some(id.as_str()));
}

#[test_action("ec2", "DescribeSpotPriceHistory", checksum = "4da11bc7")]
#[tokio::test]
async fn ec2_describe_spot_price_history() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_spot_price_history().send().await.unwrap();
    assert!(!r.spot_price_history().is_empty());
}

#[test_action("ec2", "GetSpotPlacementScores", checksum = "87fe06d2")]
#[tokio::test]
async fn ec2_get_spot_placement_scores() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_spot_placement_scores()
        .target_capacity(10)
        .send()
        .await
        .unwrap();
    assert!(!r.spot_placement_scores().is_empty());
}

// ---- spot datafeed ----

#[test_action("ec2", "CreateSpotDatafeedSubscription", checksum = "d5529374")]
#[tokio::test]
async fn ec2_create_spot_datafeed_subscription() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_spot_datafeed_subscription()
        .bucket("my-bucket")
        .send()
        .await
        .unwrap();
    assert!(r.spot_datafeed_subscription().is_some());
}

#[test_action("ec2", "DescribeSpotDatafeedSubscription", checksum = "45fbe068")]
#[tokio::test]
async fn ec2_describe_spot_datafeed_subscription() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_spot_datafeed_subscription()
        .bucket("my-bucket")
        .send()
        .await
        .unwrap();
    let r = c
        .describe_spot_datafeed_subscription()
        .send()
        .await
        .unwrap();
    assert!(r.spot_datafeed_subscription().is_some());
}

#[test_action("ec2", "DeleteSpotDatafeedSubscription", checksum = "aadba863")]
#[tokio::test]
async fn ec2_delete_spot_datafeed_subscription() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_spot_datafeed_subscription()
        .bucket("my-bucket")
        .send()
        .await
        .unwrap();
    c.delete_spot_datafeed_subscription().send().await.unwrap();
}

// ---- EC2 fleets ----

async fn make_fleet(c: &aws_sdk_ec2::Client) -> String {
    c.create_fleet()
        .launch_template_configs(
            aws_sdk_ec2::types::FleetLaunchTemplateConfigRequest::builder().build(),
        )
        .target_capacity_specification(
            aws_sdk_ec2::types::TargetCapacitySpecificationRequest::builder()
                .total_target_capacity(1)
                .build(),
        )
        .send()
        .await
        .unwrap()
        .fleet_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateFleet", checksum = "2e02f576")]
#[tokio::test]
async fn ec2_create_fleet() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_fleet(&c).await;
    assert!(id.starts_with("fleet-"));
}

#[test_action("ec2", "DescribeFleets", checksum = "e8367cfd")]
#[tokio::test]
async fn ec2_describe_fleets() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_fleet(&c).await;
    let r = c.describe_fleets().send().await.unwrap();
    assert!(!r.fleets().is_empty());
}

#[test_action("ec2", "DeleteFleets", checksum = "17f71091")]
#[tokio::test]
async fn ec2_delete_fleets() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_fleet(&c).await;
    let r = c
        .delete_fleets()
        .fleet_ids(&id)
        .terminate_instances(true)
        .send()
        .await
        .unwrap();
    assert!(!r.successful_fleet_deletions().is_empty());
}

#[test_action("ec2", "ModifyFleet", checksum = "15803ab1")]
#[tokio::test]
async fn ec2_modify_fleet() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_fleet(&c).await;
    let r = c.modify_fleet().fleet_id(&id).send().await.unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "DescribeFleetHistory", checksum = "77bc863e")]
#[tokio::test]
async fn ec2_describe_fleet_history() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_fleet(&c).await;
    let r = c
        .describe_fleet_history()
        .fleet_id(&id)
        .start_time(aws_sdk_ec2::primitives::DateTime::from_secs(1704067200))
        .send()
        .await
        .unwrap();
    assert_eq!(r.fleet_id(), Some(id.as_str()));
}

#[test_action("ec2", "DescribeFleetInstances", checksum = "0a87acc3")]
#[tokio::test]
async fn ec2_describe_fleet_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_fleet(&c).await;
    let r = c
        .describe_fleet_instances()
        .fleet_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.fleet_id(), Some(id.as_str()));
}

// ---- capacity reservations ----

async fn make_cr(c: &aws_sdk_ec2::Client) -> String {
    c.create_capacity_reservation()
        .instance_type("t3.micro")
        .instance_platform(aws_sdk_ec2::types::CapacityReservationInstancePlatform::LinuxUnix)
        .instance_count(1)
        .send()
        .await
        .unwrap()
        .capacity_reservation()
        .unwrap()
        .capacity_reservation_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateCapacityReservation", checksum = "109889e7")]
#[tokio::test]
async fn ec2_create_capacity_reservation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_capacity_reservation()
        .instance_type("t3.micro")
        .instance_platform(aws_sdk_ec2::types::CapacityReservationInstancePlatform::LinuxUnix)
        .instance_count(2)
        .send()
        .await
        .unwrap();
    assert!(r
        .capacity_reservation()
        .unwrap()
        .capacity_reservation_id()
        .unwrap()
        .starts_with("cr-"));
}

#[test_action("ec2", "DescribeCapacityReservations", checksum = "86846084")]
#[tokio::test]
async fn ec2_describe_capacity_reservations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cr(&c).await;
    let r = c
        .describe_capacity_reservations()
        .capacity_reservation_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.capacity_reservations().len(), 1);
}

#[test_action("ec2", "CancelCapacityReservation", checksum = "72d1c5ef")]
#[tokio::test]
async fn ec2_cancel_capacity_reservation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cr(&c).await;
    let r = c
        .cancel_capacity_reservation()
        .capacity_reservation_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "ModifyCapacityReservation", checksum = "2ca1d61c")]
#[tokio::test]
async fn ec2_modify_capacity_reservation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cr(&c).await;
    let r = c
        .modify_capacity_reservation()
        .capacity_reservation_id(&id)
        .instance_count(3)
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "GetCapacityReservationUsage", checksum = "58a97958")]
#[tokio::test]
async fn ec2_get_capacity_reservation_usage() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cr(&c).await;
    let r = c
        .get_capacity_reservation_usage()
        .capacity_reservation_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.capacity_reservation_id(), Some(id.as_str()));
}

#[test_action("ec2", "CreateCapacityReservationFleet", checksum = "a33a6641")]
#[tokio::test]
async fn ec2_create_capacity_reservation_fleet() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_capacity_reservation_fleet()
        .instance_type_specifications(
            aws_sdk_ec2::types::ReservationFleetInstanceSpecification::builder()
                .instance_type(aws_sdk_ec2::types::InstanceType::T3Micro)
                .build(),
        )
        .total_target_capacity(1)
        .send()
        .await
        .unwrap();
    assert!(r
        .capacity_reservation_fleet_id()
        .unwrap()
        .starts_with("crf-"));
}

#[test_action("ec2", "DescribeCapacityReservationFleets", checksum = "7e40b03a")]
#[tokio::test]
async fn ec2_describe_capacity_reservation_fleets() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_capacity_reservation_fleet()
        .total_target_capacity(1)
        .send()
        .await
        .unwrap();
    let r = c
        .describe_capacity_reservation_fleets()
        .send()
        .await
        .unwrap();
    assert!(!r.capacity_reservation_fleets().is_empty());
}

#[test_action("ec2", "CancelCapacityReservationFleets", checksum = "1996c962")]
#[tokio::test]
async fn ec2_cancel_capacity_reservation_fleets() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_capacity_reservation_fleet()
        .total_target_capacity(1)
        .send()
        .await
        .unwrap()
        .capacity_reservation_fleet_id()
        .unwrap()
        .to_string();
    let r = c
        .cancel_capacity_reservation_fleets()
        .capacity_reservation_fleet_ids(&id)
        .send()
        .await
        .unwrap();
    assert!(!r.successful_fleet_cancellations().is_empty());
}

#[test_action("ec2", "ModifyCapacityReservationFleet", checksum = "a873bc15")]
#[tokio::test]
async fn ec2_modify_capacity_reservation_fleet() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .modify_capacity_reservation_fleet()
        .capacity_reservation_fleet_id("crf-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action(
    "ec2",
    "ModifyInstanceCapacityReservationAttributes",
    checksum = "cd67dbe2"
)]
#[tokio::test]
async fn ec2_modify_instance_capacity_reservation_attributes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .modify_instance_capacity_reservation_attributes()
        .instance_id("i-1")
        .capacity_reservation_specification(
            aws_sdk_ec2::types::CapacityReservationSpecification::builder()
                .capacity_reservation_preference(
                    aws_sdk_ec2::types::CapacityReservationPreference::Open,
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "CreateCapacityReservationBySplitting", checksum = "de4f15b5")]
#[tokio::test]
async fn ec2_create_capacity_reservation_by_splitting() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cr(&c).await;
    let r = c
        .create_capacity_reservation_by_splitting()
        .source_capacity_reservation_id(&id)
        .instance_count(1)
        .send()
        .await
        .unwrap();
    assert!(r.destination_capacity_reservation().is_some());
}

#[test_action("ec2", "MoveCapacityReservationInstances", checksum = "d3c04631")]
#[tokio::test]
async fn ec2_move_capacity_reservation_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .move_capacity_reservation_instances()
        .source_capacity_reservation_id("cr-1")
        .destination_capacity_reservation_id("cr-2")
        .instance_count(1)
        .send()
        .await
        .unwrap();
    assert_eq!(r.instance_count(), Some(1));
}

#[test_action(
    "ec2",
    "DescribeCapacityReservationBillingRequests",
    checksum = "c9ec72d0"
)]
#[tokio::test]
async fn ec2_describe_capacity_reservation_billing_requests() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .describe_capacity_reservation_billing_requests()
        .role(aws_sdk_ec2::types::CallerRole::OdcrOwner)
        .send()
        .await
        .unwrap();
    assert!(r.capacity_reservation_billing_requests().is_empty());
}

#[test_action(
    "ec2",
    "AssociateCapacityReservationBillingOwner",
    checksum = "e1bc0790"
)]
#[tokio::test]
async fn ec2_associate_capacity_reservation_billing_owner() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .associate_capacity_reservation_billing_owner()
        .capacity_reservation_id("cr-1")
        .unused_reservation_billing_owner_id("123456789012")
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action(
    "ec2",
    "DisassociateCapacityReservationBillingOwner",
    checksum = "a18b20a8"
)]
#[tokio::test]
async fn ec2_disassociate_capacity_reservation_billing_owner() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disassociate_capacity_reservation_billing_owner()
        .capacity_reservation_id("cr-1")
        .unused_reservation_billing_owner_id("123456789012")
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action(
    "ec2",
    "AcceptCapacityReservationBillingOwnership",
    checksum = "80e3b60e"
)]
#[tokio::test]
async fn ec2_accept_capacity_reservation_billing_ownership() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .accept_capacity_reservation_billing_ownership()
        .capacity_reservation_id("cr-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action(
    "ec2",
    "RejectCapacityReservationBillingOwnership",
    checksum = "a8c692c0"
)]
#[tokio::test]
async fn ec2_reject_capacity_reservation_billing_ownership() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .reject_capacity_reservation_billing_ownership()
        .capacity_reservation_id("cr-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "DescribeCapacityBlockOfferings", checksum = "a83060bf")]
#[tokio::test]
async fn ec2_describe_capacity_block_offerings() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .describe_capacity_block_offerings()
        .capacity_duration_hours(24)
        .send()
        .await
        .unwrap();
    assert!(r.capacity_block_offerings().is_empty());
}

#[test_action("ec2", "DescribeCapacityBlocks", checksum = "0b57ee2c")]
#[tokio::test]
async fn ec2_describe_capacity_blocks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_capacity_blocks().send().await.unwrap();
    assert!(r.capacity_blocks().is_empty());
}

#[test_action("ec2", "PurchaseCapacityBlock", checksum = "3794b5e7")]
#[tokio::test]
async fn ec2_purchase_capacity_block() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .purchase_capacity_block()
        .capacity_block_offering_id("cbo-1")
        .instance_platform(aws_sdk_ec2::types::CapacityReservationInstancePlatform::LinuxUnix)
        .send()
        .await
        .unwrap();
    assert!(r.capacity_reservation().is_some());
}

#[test_action("ec2", "DescribeCapacityBlockStatus", checksum = "1fa80f6e")]
#[tokio::test]
async fn ec2_describe_capacity_block_status() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_capacity_block_status().send().await.unwrap();
    assert!(r.capacity_block_statuses().is_empty());
}

#[test_action("ec2", "DescribeCapacityBlockExtensionHistory", checksum = "b580d0a1")]
#[tokio::test]
async fn ec2_describe_capacity_block_extension_history() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .describe_capacity_block_extension_history()
        .send()
        .await
        .unwrap();
    assert!(r.capacity_block_extensions().is_empty());
}

#[test_action(
    "ec2",
    "DescribeCapacityBlockExtensionOfferings",
    checksum = "d06c73be"
)]
#[tokio::test]
async fn ec2_describe_capacity_block_extension_offerings() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .describe_capacity_block_extension_offerings()
        .capacity_block_extension_duration_hours(24)
        .capacity_reservation_id("cr-1")
        .send()
        .await
        .unwrap();
    assert!(r.capacity_block_extension_offerings().is_empty());
}

#[test_action("ec2", "PurchaseCapacityBlockExtension", checksum = "75eb37c1")]
#[tokio::test]
async fn ec2_purchase_capacity_block_extension() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .purchase_capacity_block_extension()
        .capacity_block_extension_offering_id("cbeo-1")
        .capacity_reservation_id("cr-1")
        .send()
        .await
        .unwrap();
    assert!(r.capacity_block_extensions().is_empty());
}

#[test_action("ec2", "DescribeCapacityReservationTopology", checksum = "775ada1f")]
#[tokio::test]
async fn ec2_describe_capacity_reservation_topology() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_capacity_reservation_topology()
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "CreateInterruptibleCapacityReservationAllocation",
    checksum = "d116b139"
)]
#[tokio::test]
async fn ec2_create_interruptible_capacity_reservation_allocation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_interruptible_capacity_reservation_allocation()
        .capacity_reservation_id("cr-1")
        .instance_count(1)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "UpdateInterruptibleCapacityReservationAllocation",
    checksum = "645c8a4d"
)]
#[tokio::test]
async fn ec2_update_interruptible_capacity_reservation_allocation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.update_interruptible_capacity_reservation_allocation()
        .capacity_reservation_id("cr-1")
        .target_instance_count(2)
        .send()
        .await
        .unwrap();
}

// ---- reserved instances ----

#[test_action("ec2", "DescribeReservedInstances", checksum = "e0393329")]
#[tokio::test]
async fn ec2_describe_reserved_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_reserved_instances().send().await.unwrap();
}

#[test_action("ec2", "DescribeReservedInstancesOfferings", checksum = "46de9215")]
#[tokio::test]
async fn ec2_describe_reserved_instances_offerings() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .describe_reserved_instances_offerings()
        .send()
        .await
        .unwrap();
    assert!(!r.reserved_instances_offerings().is_empty());
}

#[test_action("ec2", "PurchaseReservedInstancesOffering", checksum = "ed565a4e")]
#[tokio::test]
async fn ec2_purchase_reserved_instances_offering() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .purchase_reserved_instances_offering()
        .instance_count(1)
        .reserved_instances_offering_id("offering-1")
        .send()
        .await
        .unwrap();
    assert!(r.reserved_instances_id().unwrap().starts_with("ri-"));
}

#[test_action("ec2", "DescribeReservedInstancesListings", checksum = "3ef7be7d")]
#[tokio::test]
async fn ec2_describe_reserved_instances_listings() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_reserved_instances_listings()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateReservedInstancesListing", checksum = "cbfdaa8c")]
#[tokio::test]
async fn ec2_create_reserved_instances_listing() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_reserved_instances_listing()
        .reserved_instances_id("ri-1")
        .instance_count(1)
        .client_token("tok")
        .price_schedules(
            aws_sdk_ec2::types::PriceScheduleSpecification::builder()
                .price(1.0)
                .term(12)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(!r.reserved_instances_listings().is_empty());
}

#[test_action("ec2", "CancelReservedInstancesListing", checksum = "529138f5")]
#[tokio::test]
async fn ec2_cancel_reserved_instances_listing() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .cancel_reserved_instances_listing()
        .reserved_instances_listing_id("ril-1")
        .send()
        .await
        .unwrap();
    assert!(!r.reserved_instances_listings().is_empty());
}

#[test_action("ec2", "DescribeReservedInstancesModifications", checksum = "585ddb52")]
#[tokio::test]
async fn ec2_describe_reserved_instances_modifications() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_reserved_instances_modifications()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyReservedInstances", checksum = "7df70278")]
#[tokio::test]
async fn ec2_modify_reserved_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .modify_reserved_instances()
        .reserved_instances_ids("ri-1")
        .target_configurations(
            aws_sdk_ec2::types::ReservedInstancesConfiguration::builder()
                .instance_count(1)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(r.reserved_instances_modification_id().is_some());
}

#[test_action("ec2", "GetReservedInstancesExchangeQuote", checksum = "962e59d8")]
#[tokio::test]
async fn ec2_get_reserved_instances_exchange_quote() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_reserved_instances_exchange_quote()
        .reserved_instance_ids("ri-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.is_valid_exchange(), Some(true));
}

#[test_action("ec2", "AcceptReservedInstancesExchangeQuote", checksum = "c0177fbd")]
#[tokio::test]
async fn ec2_accept_reserved_instances_exchange_quote() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .accept_reserved_instances_exchange_quote()
        .reserved_instance_ids("ri-1")
        .send()
        .await
        .unwrap();
    assert!(r.exchange_id().is_some());
}

#[test_action("ec2", "DeleteQueuedReservedInstances", checksum = "a9cd4137")]
#[tokio::test]
async fn ec2_delete_queued_reserved_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .delete_queued_reserved_instances()
        .reserved_instances_ids("ri-1")
        .send()
        .await
        .unwrap();
    assert!(!r.successful_queued_purchase_deletions().is_empty());
}

// ---- dedicated hosts ----

async fn make_host(c: &aws_sdk_ec2::Client) -> String {
    c.allocate_hosts()
        .availability_zone("us-east-1a")
        .instance_type("m5.large")
        .quantity(1)
        .send()
        .await
        .unwrap()
        .host_ids()[0]
        .clone()
}

#[test_action("ec2", "AllocateHosts", checksum = "ee797dd1")]
#[tokio::test]
async fn ec2_allocate_hosts() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .allocate_hosts()
        .availability_zone("us-east-1a")
        .instance_type("m5.large")
        .quantity(2)
        .send()
        .await
        .unwrap();
    assert_eq!(r.host_ids().len(), 2);
}

#[test_action("ec2", "DescribeHosts", checksum = "69d55287")]
#[tokio::test]
async fn ec2_describe_hosts() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_host(&c).await;
    let r = c.describe_hosts().host_ids(&id).send().await.unwrap();
    assert_eq!(r.hosts().len(), 1);
}

#[test_action("ec2", "ModifyHosts", checksum = "bc52ab28")]
#[tokio::test]
async fn ec2_modify_hosts() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_host(&c).await;
    let r = c
        .modify_hosts()
        .host_ids(&id)
        .auto_placement(aws_sdk_ec2::types::AutoPlacement::Off)
        .send()
        .await
        .unwrap();
    assert!(!r.successful().is_empty());
}

#[test_action("ec2", "ReleaseHosts", checksum = "f16cf851")]
#[tokio::test]
async fn ec2_release_hosts() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_host(&c).await;
    let r = c.release_hosts().host_ids(&id).send().await.unwrap();
    assert!(!r.successful().is_empty());
}

#[test_action("ec2", "DescribeMacHosts", checksum = "71f0aafb")]
#[tokio::test]
async fn ec2_describe_mac_hosts() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_mac_hosts().send().await.unwrap();
    assert!(r.mac_hosts().is_empty());
}

// ---- transit gateway core ----

async fn make_tgw(c: &aws_sdk_ec2::Client) -> String {
    c.create_transit_gateway()
        .send()
        .await
        .unwrap()
        .transit_gateway()
        .unwrap()
        .transit_gateway_id()
        .unwrap()
        .to_string()
}
async fn make_tgw_rtb(c: &aws_sdk_ec2::Client) -> String {
    let t = make_tgw(c).await;
    c.create_transit_gateway_route_table()
        .transit_gateway_id(&t)
        .send()
        .await
        .unwrap()
        .transit_gateway_route_table()
        .unwrap()
        .transit_gateway_route_table_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateTransitGateway", checksum = "11bf8ed7")]
#[tokio::test]
async fn ec2_create_transit_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_transit_gateway()
        .description("tgw")
        .send()
        .await
        .unwrap();
    assert!(r
        .transit_gateway()
        .unwrap()
        .transit_gateway_id()
        .unwrap()
        .starts_with("tgw-"));
}

#[test_action("ec2", "DescribeTransitGateways", checksum = "fbd656a7")]
#[tokio::test]
async fn ec2_describe_transit_gateways() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw(&c).await;
    let r = c
        .describe_transit_gateways()
        .transit_gateway_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.transit_gateways().len(), 1);
}

#[test_action("ec2", "ModifyTransitGateway", checksum = "d826d43f")]
#[tokio::test]
async fn ec2_modify_transit_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw(&c).await;
    let r = c
        .modify_transit_gateway()
        .transit_gateway_id(&id)
        .description("new")
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.transit_gateway().unwrap().transit_gateway_id(),
        Some(id.as_str())
    );
}

#[test_action("ec2", "DeleteTransitGateway", checksum = "fe955325")]
#[tokio::test]
async fn ec2_delete_transit_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw(&c).await;
    c.delete_transit_gateway()
        .transit_gateway_id(&id)
        .send()
        .await
        .unwrap();
    assert!(c
        .describe_transit_gateways()
        .send()
        .await
        .unwrap()
        .transit_gateways()
        .is_empty());
}

#[test_action("ec2", "CreateTransitGatewayVpcAttachment", checksum = "ce9759ac")]
#[tokio::test]
async fn ec2_create_transit_gateway_vpc_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let t = make_tgw(&c).await;
    let r = c
        .create_transit_gateway_vpc_attachment()
        .transit_gateway_id(&t)
        .vpc_id("vpc-1")
        .subnet_ids("subnet-1")
        .send()
        .await
        .unwrap();
    assert!(r
        .transit_gateway_vpc_attachment()
        .unwrap()
        .transit_gateway_attachment_id()
        .unwrap()
        .starts_with("tgw-attach-"));
}

#[test_action("ec2", "DescribeTransitGatewayVpcAttachments", checksum = "d6994c8c")]
#[tokio::test]
async fn ec2_describe_transit_gateway_vpc_attachments() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let t = make_tgw(&c).await;
    c.create_transit_gateway_vpc_attachment()
        .transit_gateway_id(&t)
        .vpc_id("vpc-1")
        .subnet_ids("subnet-1")
        .send()
        .await
        .unwrap();
    let r = c
        .describe_transit_gateway_vpc_attachments()
        .send()
        .await
        .unwrap();
    assert!(!r.transit_gateway_vpc_attachments().is_empty());
}

#[test_action("ec2", "ModifyTransitGatewayVpcAttachment", checksum = "d2edc31c")]
#[tokio::test]
async fn ec2_modify_transit_gateway_vpc_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    // ModifyTransitGatewayVpcAttachment now validates the attachment exists
    // (returns InvalidTransitGatewayAttachmentID.NotFound otherwise), so create
    // one first and modify that id rather than a hardcoded (non-existent) id.
    let t = make_tgw(&c).await;
    let created = c
        .create_transit_gateway_vpc_attachment()
        .transit_gateway_id(&t)
        .vpc_id("vpc-1")
        .subnet_ids("subnet-1")
        .send()
        .await
        .unwrap();
    let id = created
        .transit_gateway_vpc_attachment()
        .unwrap()
        .transit_gateway_attachment_id()
        .unwrap()
        .to_string();
    let r = c
        .modify_transit_gateway_vpc_attachment()
        .transit_gateway_attachment_id(id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_vpc_attachment().is_some());
}

#[test_action("ec2", "AcceptTransitGatewayVpcAttachment", checksum = "fe7211d6")]
#[tokio::test]
async fn ec2_accept_transit_gateway_vpc_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .accept_transit_gateway_vpc_attachment()
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_vpc_attachment().is_some());
}

#[test_action("ec2", "RejectTransitGatewayVpcAttachment", checksum = "ca26cf71")]
#[tokio::test]
async fn ec2_reject_transit_gateway_vpc_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .reject_transit_gateway_vpc_attachment()
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_vpc_attachment().is_some());
}

#[test_action("ec2", "DeleteTransitGatewayVpcAttachment", checksum = "83b7dc5f")]
#[tokio::test]
async fn ec2_delete_transit_gateway_vpc_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .delete_transit_gateway_vpc_attachment()
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_vpc_attachment().is_some());
}

#[test_action("ec2", "DescribeTransitGatewayAttachments", checksum = "5a838b8e")]
#[tokio::test]
async fn ec2_describe_transit_gateway_attachments() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .describe_transit_gateway_attachments()
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_attachments().is_empty());
}

#[test_action("ec2", "CreateTransitGatewayRouteTable", checksum = "228d421c")]
#[tokio::test]
async fn ec2_create_transit_gateway_route_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_rtb(&c).await;
    assert!(id.starts_with("tgw-rtb-"));
}

#[test_action("ec2", "DescribeTransitGatewayRouteTables", checksum = "3f382e0f")]
#[tokio::test]
async fn ec2_describe_transit_gateway_route_tables() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_rtb(&c).await;
    let r = c
        .describe_transit_gateway_route_tables()
        .transit_gateway_route_table_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.transit_gateway_route_tables().len(), 1);
}

#[test_action("ec2", "DeleteTransitGatewayRouteTable", checksum = "16793190")]
#[tokio::test]
async fn ec2_delete_transit_gateway_route_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_rtb(&c).await;
    c.delete_transit_gateway_route_table()
        .transit_gateway_route_table_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AssociateTransitGatewayRouteTable", checksum = "771b03bd")]
#[tokio::test]
async fn ec2_associate_transit_gateway_route_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .associate_transit_gateway_route_table()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.association().is_some());
}

#[test_action("ec2", "DisassociateTransitGatewayRouteTable", checksum = "6c240a44")]
#[tokio::test]
async fn ec2_disassociate_transit_gateway_route_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disassociate_transit_gateway_route_table()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.association().is_some());
}

#[test_action(
    "ec2",
    "EnableTransitGatewayRouteTablePropagation",
    checksum = "bf6d9ab3"
)]
#[tokio::test]
async fn ec2_enable_transit_gateway_route_table_propagation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .enable_transit_gateway_route_table_propagation()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.propagation().is_some());
}

#[test_action(
    "ec2",
    "DisableTransitGatewayRouteTablePropagation",
    checksum = "d1cebe36"
)]
#[tokio::test]
async fn ec2_disable_transit_gateway_route_table_propagation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disable_transit_gateway_route_table_propagation()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.propagation().is_some());
}

#[test_action("ec2", "CreateTransitGatewayRoute", checksum = "d9ba1a2d")]
#[tokio::test]
async fn ec2_create_transit_gateway_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_transit_gateway_route()
        .destination_cidr_block("10.0.0.0/16")
        .transit_gateway_route_table_id("tgw-rtb-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.route().unwrap().destination_cidr_block(),
        Some("10.0.0.0/16")
    );
}

#[test_action("ec2", "DeleteTransitGatewayRoute", checksum = "625b75ca")]
#[tokio::test]
async fn ec2_delete_transit_gateway_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_transit_gateway_route()
        .destination_cidr_block("10.0.0.0/16")
        .transit_gateway_route_table_id("tgw-rtb-1")
        .send()
        .await
        .unwrap();
    let r = c
        .delete_transit_gateway_route()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .destination_cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    assert!(r.route().is_some());
}

#[test_action("ec2", "ReplaceTransitGatewayRoute", checksum = "eed38a62")]
#[tokio::test]
async fn ec2_replace_transit_gateway_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .replace_transit_gateway_route()
        .destination_cidr_block("10.0.0.0/16")
        .transit_gateway_route_table_id("tgw-rtb-1")
        .send()
        .await
        .unwrap();
    assert!(r.route().is_some());
}

#[test_action("ec2", "SearchTransitGatewayRoutes", checksum = "e3986c4d")]
#[tokio::test]
async fn ec2_search_transit_gateway_routes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .search_transit_gateway_routes()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("state")
                .values("active")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let _ = r.routes();
}

#[test_action("ec2", "ExportTransitGatewayRoutes", checksum = "d901d823")]
#[tokio::test]
async fn ec2_export_transit_gateway_routes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .export_transit_gateway_routes()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .s3_bucket("my-bucket")
        .send()
        .await
        .unwrap();
    assert!(r.s3_location().is_some());
}

#[test_action(
    "ec2",
    "GetTransitGatewayRouteTableAssociations",
    checksum = "0fb3fc39"
)]
#[tokio::test]
async fn ec2_get_transit_gateway_route_table_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_transit_gateway_route_table_associations()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .send()
        .await
        .unwrap();
    assert!(r.associations().is_empty());
}

#[test_action(
    "ec2",
    "GetTransitGatewayRouteTablePropagations",
    checksum = "a66d95de"
)]
#[tokio::test]
async fn ec2_get_transit_gateway_route_table_propagations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_transit_gateway_route_table_propagations()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_route_table_propagations().is_empty());
}

#[test_action(
    "ec2",
    "GetTransitGatewayAttachmentPropagations",
    checksum = "76141a6e"
)]
#[tokio::test]
async fn ec2_get_transit_gateway_attachment_propagations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_transit_gateway_attachment_propagations()
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_attachment_propagations().is_empty());
}

#[test_action(
    "ec2",
    "CreateTransitGatewayPrefixListReference",
    checksum = "310929a2"
)]
#[tokio::test]
async fn ec2_create_transit_gateway_prefix_list_reference() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_transit_gateway_prefix_list_reference()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .prefix_list_id("pl-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_prefix_list_reference().is_some());
}

#[test_action(
    "ec2",
    "ModifyTransitGatewayPrefixListReference",
    checksum = "a22aeff0"
)]
#[tokio::test]
async fn ec2_modify_transit_gateway_prefix_list_reference() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .modify_transit_gateway_prefix_list_reference()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .prefix_list_id("pl-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_prefix_list_reference().is_some());
}

#[test_action(
    "ec2",
    "DeleteTransitGatewayPrefixListReference",
    checksum = "a8c7a942"
)]
#[tokio::test]
async fn ec2_delete_transit_gateway_prefix_list_reference() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .delete_transit_gateway_prefix_list_reference()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .prefix_list_id("pl-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_prefix_list_reference().is_some());
}

#[test_action("ec2", "GetTransitGatewayPrefixListReferences", checksum = "fcab772e")]
#[tokio::test]
async fn ec2_get_transit_gateway_prefix_list_references() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_transit_gateway_prefix_list_references()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_prefix_list_references().is_empty());
}

// ---- transit gateway peering / connect / policy / announcements ----

async fn make_tgw_peer(c: &aws_sdk_ec2::Client) -> String {
    c.create_transit_gateway_peering_attachment()
        .transit_gateway_id("tgw-1")
        .peer_transit_gateway_id("tgw-2")
        .peer_account_id("123456789012")
        .peer_region("us-west-2")
        .send()
        .await
        .unwrap()
        .transit_gateway_peering_attachment()
        .unwrap()
        .transit_gateway_attachment_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateTransitGatewayPeeringAttachment", checksum = "af9d51e6")]
#[tokio::test]
async fn ec2_create_transit_gateway_peering_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_peer(&c).await;
    assert!(id.starts_with("tgw-attach-"));
}

#[test_action(
    "ec2",
    "DescribeTransitGatewayPeeringAttachments",
    checksum = "397c4b40"
)]
#[tokio::test]
async fn ec2_describe_transit_gateway_peering_attachments() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_tgw_peer(&c).await;
    let r = c
        .describe_transit_gateway_peering_attachments()
        .send()
        .await
        .unwrap();
    assert!(!r.transit_gateway_peering_attachments().is_empty());
}

#[test_action("ec2", "AcceptTransitGatewayPeeringAttachment", checksum = "2b168a56")]
#[tokio::test]
async fn ec2_accept_transit_gateway_peering_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_peer(&c).await;
    let r = c
        .accept_transit_gateway_peering_attachment()
        .transit_gateway_attachment_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_peering_attachment().is_some());
}

#[test_action("ec2", "RejectTransitGatewayPeeringAttachment", checksum = "310d63ac")]
#[tokio::test]
async fn ec2_reject_transit_gateway_peering_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_peer(&c).await;
    let r = c
        .reject_transit_gateway_peering_attachment()
        .transit_gateway_attachment_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_peering_attachment().is_some());
}

#[test_action("ec2", "DeleteTransitGatewayPeeringAttachment", checksum = "dc92c124")]
#[tokio::test]
async fn ec2_delete_transit_gateway_peering_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_peer(&c).await;
    let r = c
        .delete_transit_gateway_peering_attachment()
        .transit_gateway_attachment_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_peering_attachment().is_some());
}

#[test_action("ec2", "CreateTransitGatewayConnect", checksum = "2408f4f5")]
#[tokio::test]
async fn ec2_create_transit_gateway_connect() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_transit_gateway_connect()
        .transport_transit_gateway_attachment_id("tgw-attach-1")
        .options(
            aws_sdk_ec2::types::CreateTransitGatewayConnectRequestOptions::builder()
                .protocol(aws_sdk_ec2::types::ProtocolValue::Gre)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(r
        .transit_gateway_connect()
        .unwrap()
        .transit_gateway_attachment_id()
        .unwrap()
        .starts_with("tgw-attach-"));
}

#[test_action("ec2", "DescribeTransitGatewayConnects", checksum = "2f471a0d")]
#[tokio::test]
async fn ec2_describe_transit_gateway_connects() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_transit_gateway_connect()
        .transport_transit_gateway_attachment_id("tgw-attach-1")
        .options(
            aws_sdk_ec2::types::CreateTransitGatewayConnectRequestOptions::builder()
                .protocol(aws_sdk_ec2::types::ProtocolValue::Gre)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let r = c.describe_transit_gateway_connects().send().await.unwrap();
    assert!(!r.transit_gateway_connects().is_empty());
}

#[test_action("ec2", "DeleteTransitGatewayConnect", checksum = "433d1af3")]
#[tokio::test]
async fn ec2_delete_transit_gateway_connect() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_transit_gateway_connect()
        .transport_transit_gateway_attachment_id("tgw-attach-1")
        .options(
            aws_sdk_ec2::types::CreateTransitGatewayConnectRequestOptions::builder()
                .protocol(aws_sdk_ec2::types::ProtocolValue::Gre)
                .build(),
        )
        .send()
        .await
        .unwrap()
        .transit_gateway_connect()
        .unwrap()
        .transit_gateway_attachment_id()
        .unwrap()
        .to_string();
    let r = c
        .delete_transit_gateway_connect()
        .transit_gateway_attachment_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_connect().is_some());
}

#[test_action("ec2", "CreateTransitGatewayConnectPeer", checksum = "cad18588")]
#[tokio::test]
async fn ec2_create_transit_gateway_connect_peer() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_transit_gateway_connect_peer()
        .transit_gateway_attachment_id("tgw-attach-1")
        .peer_address("10.0.0.1")
        .inside_cidr_blocks("169.254.6.0/29")
        .send()
        .await
        .unwrap();
    assert!(r
        .transit_gateway_connect_peer()
        .unwrap()
        .transit_gateway_connect_peer_id()
        .unwrap()
        .starts_with("tgw-connect-peer-"));
}

#[test_action("ec2", "DescribeTransitGatewayConnectPeers", checksum = "db3f1872")]
#[tokio::test]
async fn ec2_describe_transit_gateway_connect_peers() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_transit_gateway_connect_peer()
        .transit_gateway_attachment_id("tgw-attach-1")
        .peer_address("10.0.0.1")
        .inside_cidr_blocks("169.254.6.0/29")
        .send()
        .await
        .unwrap();
    let r = c
        .describe_transit_gateway_connect_peers()
        .send()
        .await
        .unwrap();
    assert!(!r.transit_gateway_connect_peers().is_empty());
}

#[test_action("ec2", "DeleteTransitGatewayConnectPeer", checksum = "5e56083e")]
#[tokio::test]
async fn ec2_delete_transit_gateway_connect_peer() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_transit_gateway_connect_peer()
        .transit_gateway_attachment_id("tgw-attach-1")
        .peer_address("10.0.0.1")
        .inside_cidr_blocks("169.254.6.0/29")
        .send()
        .await
        .unwrap()
        .transit_gateway_connect_peer()
        .unwrap()
        .transit_gateway_connect_peer_id()
        .unwrap()
        .to_string();
    let r = c
        .delete_transit_gateway_connect_peer()
        .transit_gateway_connect_peer_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_connect_peer().is_some());
}

async fn make_tgw_pt(c: &aws_sdk_ec2::Client) -> String {
    c.create_transit_gateway_policy_table()
        .transit_gateway_id("tgw-1")
        .send()
        .await
        .unwrap()
        .transit_gateway_policy_table()
        .unwrap()
        .transit_gateway_policy_table_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateTransitGatewayPolicyTable", checksum = "2bd6ed75")]
#[tokio::test]
async fn ec2_create_transit_gateway_policy_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_pt(&c).await;
    assert!(id.starts_with("tgw-ptb-"));
}

#[test_action("ec2", "DescribeTransitGatewayPolicyTables", checksum = "64425b2c")]
#[tokio::test]
async fn ec2_describe_transit_gateway_policy_tables() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_tgw_pt(&c).await;
    let r = c
        .describe_transit_gateway_policy_tables()
        .send()
        .await
        .unwrap();
    assert!(!r.transit_gateway_policy_tables().is_empty());
}

#[test_action("ec2", "DeleteTransitGatewayPolicyTable", checksum = "bac39d61")]
#[tokio::test]
async fn ec2_delete_transit_gateway_policy_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_pt(&c).await;
    let r = c
        .delete_transit_gateway_policy_table()
        .transit_gateway_policy_table_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_policy_table().is_some());
}

#[test_action("ec2", "AssociateTransitGatewayPolicyTable", checksum = "7518c1bf")]
#[tokio::test]
async fn ec2_associate_transit_gateway_policy_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .associate_transit_gateway_policy_table()
        .transit_gateway_policy_table_id("tgw-ptb-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.association().is_some());
}

#[test_action("ec2", "DisassociateTransitGatewayPolicyTable", checksum = "33a05767")]
#[tokio::test]
async fn ec2_disassociate_transit_gateway_policy_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disassociate_transit_gateway_policy_table()
        .transit_gateway_policy_table_id("tgw-ptb-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.association().is_some());
}

#[test_action(
    "ec2",
    "GetTransitGatewayPolicyTableAssociations",
    checksum = "6a44702a"
)]
#[tokio::test]
async fn ec2_get_transit_gateway_policy_table_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_transit_gateway_policy_table_associations()
        .transit_gateway_policy_table_id("tgw-ptb-1")
        .send()
        .await
        .unwrap();
    assert!(r.associations().is_empty());
}

#[test_action("ec2", "GetTransitGatewayPolicyTableEntries", checksum = "5cb16c6c")]
#[tokio::test]
async fn ec2_get_transit_gateway_policy_table_entries() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_transit_gateway_policy_table_entries()
        .transit_gateway_policy_table_id("tgw-ptb-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_policy_table_entries().is_empty());
}

async fn make_tgw_announce(c: &aws_sdk_ec2::Client) -> String {
    c.create_transit_gateway_route_table_announcement()
        .transit_gateway_route_table_id("tgw-rtb-1")
        .peering_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap()
        .transit_gateway_route_table_announcement()
        .unwrap()
        .transit_gateway_route_table_announcement_id()
        .unwrap()
        .to_string()
}

#[test_action(
    "ec2",
    "CreateTransitGatewayRouteTableAnnouncement",
    checksum = "edad2f14"
)]
#[tokio::test]
async fn ec2_create_transit_gateway_route_table_announcement() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_announce(&c).await;
    assert!(id.starts_with("tgw-rtb-announce-"));
}

#[test_action(
    "ec2",
    "DescribeTransitGatewayRouteTableAnnouncements",
    checksum = "f34518cd"
)]
#[tokio::test]
async fn ec2_describe_transit_gateway_route_table_announcements() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_tgw_announce(&c).await;
    let r = c
        .describe_transit_gateway_route_table_announcements()
        .send()
        .await
        .unwrap();
    assert!(!r.transit_gateway_route_table_announcements().is_empty());
}

#[test_action(
    "ec2",
    "DeleteTransitGatewayRouteTableAnnouncement",
    checksum = "e3f5a5c4"
)]
#[tokio::test]
async fn ec2_delete_transit_gateway_route_table_announcement() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_announce(&c).await;
    let r = c
        .delete_transit_gateway_route_table_announcement()
        .transit_gateway_route_table_announcement_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_route_table_announcement().is_some());
}

// ---- transit gateway multicast / metering / client-vpn-attach ----

async fn make_tgw_mcast(c: &aws_sdk_ec2::Client) -> String {
    c.create_transit_gateway_multicast_domain()
        .transit_gateway_id("tgw-1")
        .send()
        .await
        .unwrap()
        .transit_gateway_multicast_domain()
        .unwrap()
        .transit_gateway_multicast_domain_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateTransitGatewayMulticastDomain", checksum = "4557638b")]
#[tokio::test]
async fn ec2_create_transit_gateway_multicast_domain() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_mcast(&c).await;
    assert!(id.starts_with("tgw-mcast-domain-"));
}

#[test_action("ec2", "DescribeTransitGatewayMulticastDomains", checksum = "6bbc5746")]
#[tokio::test]
async fn ec2_describe_transit_gateway_multicast_domains() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_tgw_mcast(&c).await;
    let r = c
        .describe_transit_gateway_multicast_domains()
        .send()
        .await
        .unwrap();
    assert!(!r.transit_gateway_multicast_domains().is_empty());
}

#[test_action("ec2", "DeleteTransitGatewayMulticastDomain", checksum = "51fe70c4")]
#[tokio::test]
async fn ec2_delete_transit_gateway_multicast_domain() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_mcast(&c).await;
    let r = c
        .delete_transit_gateway_multicast_domain()
        .transit_gateway_multicast_domain_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_multicast_domain().is_some());
}

#[test_action("ec2", "AssociateTransitGatewayMulticastDomain", checksum = "0e802855")]
#[tokio::test]
async fn ec2_associate_transit_gateway_multicast_domain() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .associate_transit_gateway_multicast_domain()
        .transit_gateway_multicast_domain_id("tgw-mcast-domain-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .subnet_ids("subnet-1")
        .send()
        .await
        .unwrap();
    assert!(r.associations().is_some());
}

#[test_action(
    "ec2",
    "DisassociateTransitGatewayMulticastDomain",
    checksum = "d89205ec"
)]
#[tokio::test]
async fn ec2_disassociate_transit_gateway_multicast_domain() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disassociate_transit_gateway_multicast_domain()
        .transit_gateway_multicast_domain_id("tgw-mcast-domain-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .subnet_ids("subnet-1")
        .send()
        .await
        .unwrap();
    assert!(r.associations().is_some());
}

#[test_action(
    "ec2",
    "AcceptTransitGatewayMulticastDomainAssociations",
    checksum = "716c35a2"
)]
#[tokio::test]
async fn ec2_accept_transit_gateway_multicast_domain_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .accept_transit_gateway_multicast_domain_associations()
        .transit_gateway_multicast_domain_id("tgw-mcast-domain-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.associations().is_some());
}

#[test_action(
    "ec2",
    "RejectTransitGatewayMulticastDomainAssociations",
    checksum = "3a78cbd9"
)]
#[tokio::test]
async fn ec2_reject_transit_gateway_multicast_domain_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .reject_transit_gateway_multicast_domain_associations()
        .transit_gateway_multicast_domain_id("tgw-mcast-domain-1")
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.associations().is_some());
}

#[test_action(
    "ec2",
    "GetTransitGatewayMulticastDomainAssociations",
    checksum = "285c2663"
)]
#[tokio::test]
async fn ec2_get_transit_gateway_multicast_domain_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_transit_gateway_multicast_domain_associations()
        .transit_gateway_multicast_domain_id("tgw-mcast-domain-1")
        .send()
        .await
        .unwrap();
    assert!(r.multicast_domain_associations().is_empty());
}

#[test_action(
    "ec2",
    "RegisterTransitGatewayMulticastGroupMembers",
    checksum = "40a25219"
)]
#[tokio::test]
async fn ec2_register_transit_gateway_multicast_group_members() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .register_transit_gateway_multicast_group_members()
        .transit_gateway_multicast_domain_id("tgw-mcast-domain-1")
        .network_interface_ids("eni-1")
        .send()
        .await
        .unwrap();
    assert!(r.registered_multicast_group_members().is_some());
}

#[test_action(
    "ec2",
    "RegisterTransitGatewayMulticastGroupSources",
    checksum = "78a68388"
)]
#[tokio::test]
async fn ec2_register_transit_gateway_multicast_group_sources() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .register_transit_gateway_multicast_group_sources()
        .transit_gateway_multicast_domain_id("tgw-mcast-domain-1")
        .network_interface_ids("eni-1")
        .send()
        .await
        .unwrap();
    assert!(r.registered_multicast_group_sources().is_some());
}

#[test_action(
    "ec2",
    "DeregisterTransitGatewayMulticastGroupMembers",
    checksum = "0e43fb63"
)]
#[tokio::test]
async fn ec2_deregister_transit_gateway_multicast_group_members() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .deregister_transit_gateway_multicast_group_members()
        .transit_gateway_multicast_domain_id("tgw-mcast-domain-1")
        .send()
        .await
        .unwrap();
    assert!(r.deregistered_multicast_group_members().is_some());
}

#[test_action(
    "ec2",
    "DeregisterTransitGatewayMulticastGroupSources",
    checksum = "47179207"
)]
#[tokio::test]
async fn ec2_deregister_transit_gateway_multicast_group_sources() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .deregister_transit_gateway_multicast_group_sources()
        .transit_gateway_multicast_domain_id("tgw-mcast-domain-1")
        .send()
        .await
        .unwrap();
    assert!(r.deregistered_multicast_group_sources().is_some());
}

#[test_action("ec2", "SearchTransitGatewayMulticastGroups", checksum = "81780859")]
#[tokio::test]
async fn ec2_search_transit_gateway_multicast_groups() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .search_transit_gateway_multicast_groups()
        .transit_gateway_multicast_domain_id("tgw-mcast-domain-1")
        .send()
        .await
        .unwrap();
    assert!(r.multicast_groups().is_empty());
}

async fn make_tgw_mp(c: &aws_sdk_ec2::Client) -> String {
    c.create_transit_gateway_metering_policy()
        .transit_gateway_id("tgw-1")
        .send()
        .await
        .unwrap()
        .transit_gateway_metering_policy()
        .unwrap()
        .transit_gateway_metering_policy_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateTransitGatewayMeteringPolicy", checksum = "4b2d73d2")]
#[tokio::test]
async fn ec2_create_transit_gateway_metering_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_mp(&c).await;
    assert!(id.starts_with("tgw-mp-"));
}

#[test_action("ec2", "DescribeTransitGatewayMeteringPolicies", checksum = "694e42db")]
#[tokio::test]
async fn ec2_describe_transit_gateway_metering_policies() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_tgw_mp(&c).await;
    let r = c
        .describe_transit_gateway_metering_policies()
        .send()
        .await
        .unwrap();
    assert!(!r.transit_gateway_metering_policies().is_empty());
}

#[test_action("ec2", "DeleteTransitGatewayMeteringPolicy", checksum = "3b4a3b96")]
#[tokio::test]
async fn ec2_delete_transit_gateway_metering_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_mp(&c).await;
    let r = c
        .delete_transit_gateway_metering_policy()
        .transit_gateway_metering_policy_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_metering_policy().is_some());
}

#[test_action("ec2", "ModifyTransitGatewayMeteringPolicy", checksum = "effc01c3")]
#[tokio::test]
async fn ec2_modify_transit_gateway_metering_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_tgw_mp(&c).await;
    let r = c
        .modify_transit_gateway_metering_policy()
        .transit_gateway_metering_policy_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_metering_policy().is_some());
}

#[test_action(
    "ec2",
    "CreateTransitGatewayMeteringPolicyEntry",
    checksum = "9d2f5e91"
)]
#[tokio::test]
async fn ec2_create_transit_gateway_metering_policy_entry() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_transit_gateway_metering_policy_entry()
        .transit_gateway_metering_policy_id("tgw-mp-1")
        .policy_rule_number(1)
        .metered_account(aws_sdk_ec2::types::TransitGatewayMeteringPayerType::SourceAttachmentOwner)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_metering_policy_entry().is_some());
}

#[test_action(
    "ec2",
    "DeleteTransitGatewayMeteringPolicyEntry",
    checksum = "93f0f0c4"
)]
#[tokio::test]
async fn ec2_delete_transit_gateway_metering_policy_entry() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .delete_transit_gateway_metering_policy_entry()
        .transit_gateway_metering_policy_id("tgw-mp-1")
        .policy_rule_number(1)
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_metering_policy_entry().is_some());
}

#[test_action("ec2", "GetTransitGatewayMeteringPolicyEntries", checksum = "6fdbaa10")]
#[tokio::test]
async fn ec2_get_transit_gateway_metering_policy_entries() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_transit_gateway_metering_policy_entries()
        .transit_gateway_metering_policy_id("tgw-mp-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_metering_policy_entries().is_empty());
}

#[test_action(
    "ec2",
    "AcceptTransitGatewayClientVpnAttachment",
    checksum = "e0ac13e1"
)]
#[tokio::test]
async fn ec2_accept_transit_gateway_client_vpn_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .accept_transit_gateway_client_vpn_attachment()
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_client_vpn_attachment().is_some());
}

#[test_action(
    "ec2",
    "DeleteTransitGatewayClientVpnAttachment",
    checksum = "eb510237"
)]
#[tokio::test]
async fn ec2_delete_transit_gateway_client_vpn_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .delete_transit_gateway_client_vpn_attachment()
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_client_vpn_attachment().is_some());
}

#[test_action(
    "ec2",
    "RejectTransitGatewayClientVpnAttachment",
    checksum = "d85fb8ab"
)]
#[tokio::test]
async fn ec2_reject_transit_gateway_client_vpn_attachment() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .reject_transit_gateway_client_vpn_attachment()
        .transit_gateway_attachment_id("tgw-attach-1")
        .send()
        .await
        .unwrap();
    assert!(r.transit_gateway_client_vpn_attachment().is_some());
}

// ---- site-to-site VPN ----

async fn make_cgw(c: &aws_sdk_ec2::Client) -> String {
    c.create_customer_gateway()
        .r#type(aws_sdk_ec2::types::GatewayType::Ipsec1)
        .ip_address("203.0.113.1")
        .send()
        .await
        .unwrap()
        .customer_gateway()
        .unwrap()
        .customer_gateway_id()
        .unwrap()
        .to_string()
}
async fn make_vgw(c: &aws_sdk_ec2::Client) -> String {
    c.create_vpn_gateway()
        .r#type(aws_sdk_ec2::types::GatewayType::Ipsec1)
        .send()
        .await
        .unwrap()
        .vpn_gateway()
        .unwrap()
        .vpn_gateway_id()
        .unwrap()
        .to_string()
}
async fn make_vpn(c: &aws_sdk_ec2::Client) -> String {
    let cgw = make_cgw(c).await;
    c.create_vpn_connection()
        .customer_gateway_id(&cgw)
        .r#type("ipsec.1")
        .send()
        .await
        .unwrap()
        .vpn_connection()
        .unwrap()
        .vpn_connection_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateCustomerGateway", checksum = "d905541f")]
#[tokio::test]
async fn ec2_create_customer_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cgw(&c).await;
    assert!(id.starts_with("cgw-"));
}

#[test_action("ec2", "DescribeCustomerGateways", checksum = "236ecaa5")]
#[tokio::test]
async fn ec2_describe_customer_gateways() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cgw(&c).await;
    let r = c
        .describe_customer_gateways()
        .customer_gateway_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.customer_gateways().len(), 1);
}

#[test_action("ec2", "DeleteCustomerGateway", checksum = "80f713ea")]
#[tokio::test]
async fn ec2_delete_customer_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cgw(&c).await;
    c.delete_customer_gateway()
        .customer_gateway_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateVpnGateway", checksum = "980342bd")]
#[tokio::test]
async fn ec2_create_vpn_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vgw(&c).await;
    assert!(id.starts_with("vgw-"));
}

#[test_action("ec2", "DescribeVpnGateways", checksum = "814a790c")]
#[tokio::test]
async fn ec2_describe_vpn_gateways() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vgw(&c).await;
    let r = c
        .describe_vpn_gateways()
        .vpn_gateway_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.vpn_gateways().len(), 1);
}

#[test_action("ec2", "DeleteVpnGateway", checksum = "774edd00")]
#[tokio::test]
async fn ec2_delete_vpn_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vgw(&c).await;
    c.delete_vpn_gateway()
        .vpn_gateway_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AttachVpnGateway", checksum = "5e83e0e9")]
#[tokio::test]
async fn ec2_attach_vpn_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vgw(&c).await;
    let r = c
        .attach_vpn_gateway()
        .vpn_gateway_id(&id)
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    assert!(r.vpc_attachment().is_some());
}

#[test_action("ec2", "DetachVpnGateway", checksum = "b430b510")]
#[tokio::test]
async fn ec2_detach_vpn_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vgw(&c).await;
    c.detach_vpn_gateway()
        .vpn_gateway_id(&id)
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateVpnConnection", checksum = "b84874a8")]
#[tokio::test]
async fn ec2_create_vpn_connection() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    assert!(id.starts_with("vpn-"));
}

#[test_action("ec2", "DescribeVpnConnections", checksum = "a2885e4c")]
#[tokio::test]
async fn ec2_describe_vpn_connections() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    let r = c
        .describe_vpn_connections()
        .vpn_connection_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.vpn_connections().len(), 1);
}

#[test_action("ec2", "DeleteVpnConnection", checksum = "34a21588")]
#[tokio::test]
async fn ec2_delete_vpn_connection() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    c.delete_vpn_connection()
        .vpn_connection_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyVpnConnection", checksum = "cf8776fd")]
#[tokio::test]
async fn ec2_modify_vpn_connection() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    let r = c
        .modify_vpn_connection()
        .vpn_connection_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.vpn_connection().is_some());
}

#[test_action("ec2", "ModifyVpnConnectionOptions", checksum = "3df1ad70")]
#[tokio::test]
async fn ec2_modify_vpn_connection_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    let r = c
        .modify_vpn_connection_options()
        .vpn_connection_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.vpn_connection().is_some());
}

#[test_action("ec2", "CreateVpnConnectionRoute", checksum = "4507af0e")]
#[tokio::test]
async fn ec2_create_vpn_connection_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    c.create_vpn_connection_route()
        .vpn_connection_id(&id)
        .destination_cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteVpnConnectionRoute", checksum = "40bb1673")]
#[tokio::test]
async fn ec2_delete_vpn_connection_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    c.create_vpn_connection_route()
        .vpn_connection_id(&id)
        .destination_cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    c.delete_vpn_connection_route()
        .vpn_connection_id(&id)
        .destination_cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyVpnTunnelOptions", checksum = "03603e7d")]
#[tokio::test]
async fn ec2_modify_vpn_tunnel_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    let r = c
        .modify_vpn_tunnel_options()
        .vpn_connection_id(&id)
        .vpn_tunnel_outside_ip_address("1.2.3.4")
        .tunnel_options(aws_sdk_ec2::types::ModifyVpnTunnelOptionsSpecification::builder().build())
        .send()
        .await
        .unwrap();
    assert!(r.vpn_connection().is_some());
}

#[test_action("ec2", "ModifyVpnTunnelCertificate", checksum = "d74ba53a")]
#[tokio::test]
async fn ec2_modify_vpn_tunnel_certificate() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    let r = c
        .modify_vpn_tunnel_certificate()
        .vpn_connection_id(&id)
        .vpn_tunnel_outside_ip_address("1.2.3.4")
        .send()
        .await
        .unwrap();
    assert!(r.vpn_connection().is_some());
}

#[test_action("ec2", "ReplaceVpnTunnel", checksum = "1b04822a")]
#[tokio::test]
async fn ec2_replace_vpn_tunnel() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    let r = c
        .replace_vpn_tunnel()
        .vpn_connection_id(&id)
        .vpn_tunnel_outside_ip_address("1.2.3.4")
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "GetActiveVpnTunnelStatus", checksum = "9de13f9c")]
#[tokio::test]
async fn ec2_get_active_vpn_tunnel_status() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    let r = c
        .get_active_vpn_tunnel_status()
        .vpn_connection_id(&id)
        .vpn_tunnel_outside_ip_address("1.2.3.4")
        .send()
        .await
        .unwrap();
    assert!(r.active_vpn_tunnel_status().is_some());
}

#[test_action("ec2", "GetVpnTunnelReplacementStatus", checksum = "09719f6f")]
#[tokio::test]
async fn ec2_get_vpn_tunnel_replacement_status() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpn(&c).await;
    let r = c
        .get_vpn_tunnel_replacement_status()
        .vpn_connection_id(&id)
        .vpn_tunnel_outside_ip_address("1.2.3.4")
        .send()
        .await
        .unwrap();
    assert_eq!(r.vpn_connection_id(), Some(id.as_str()));
}

#[test_action("ec2", "GetVpnConnectionDeviceTypes", checksum = "7f37d1e0")]
#[tokio::test]
async fn ec2_get_vpn_connection_device_types() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.get_vpn_connection_device_types().send().await.unwrap();
    assert!(!r.vpn_connection_device_types().is_empty());
}

#[test_action(
    "ec2",
    "GetVpnConnectionDeviceSampleConfiguration",
    checksum = "8631d8f7"
)]
#[tokio::test]
async fn ec2_get_vpn_connection_device_sample_configuration() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_vpn_connection_device_sample_configuration()
        .vpn_connection_id("vpn-1")
        .vpn_connection_device_type_id("0123abcd")
        .send()
        .await
        .unwrap();
    assert!(r.vpn_connection_device_sample_configuration().is_some());
}

async fn make_vpnc(c: &aws_sdk_ec2::Client) -> String {
    c.create_vpn_concentrator()
        .r#type(aws_sdk_ec2::types::VpnConcentratorType::Ipsec1)
        .send()
        .await
        .unwrap()
        .vpn_concentrator()
        .unwrap()
        .vpn_concentrator_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateVpnConcentrator", checksum = "fa1d1630")]
#[tokio::test]
async fn ec2_create_vpn_concentrator() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpnc(&c).await;
    assert!(id.starts_with("vpnc-"));
}

#[test_action("ec2", "DescribeVpnConcentrators", checksum = "f81af537")]
#[tokio::test]
async fn ec2_describe_vpn_concentrators() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_vpnc(&c).await;
    let r = c.describe_vpn_concentrators().send().await.unwrap();
    assert!(!r.vpn_concentrators().is_empty());
}

#[test_action("ec2", "DeleteVpnConcentrator", checksum = "573a9a28")]
#[tokio::test]
async fn ec2_delete_vpn_concentrator() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vpnc(&c).await;
    c.delete_vpn_concentrator()
        .vpn_concentrator_id(&id)
        .send()
        .await
        .unwrap();
}

// ---- client VPN ----

async fn make_cvpn(c: &aws_sdk_ec2::Client) -> String {
    c.create_client_vpn_endpoint()
        .client_cidr_block("10.0.0.0/22")
        .server_certificate_arn("arn:aws:acm:us-east-1:123456789012:certificate/abc")
        .authentication_options(
            aws_sdk_ec2::types::ClientVpnAuthenticationRequest::builder()
                .r#type(aws_sdk_ec2::types::ClientVpnAuthenticationType::CertificateAuthentication)
                .build(),
        )
        .connection_log_options(
            aws_sdk_ec2::types::ConnectionLogOptions::builder()
                .enabled(false)
                .build(),
        )
        .send()
        .await
        .unwrap()
        .client_vpn_endpoint_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateClientVpnEndpoint", checksum = "2a23e2d7")]
#[tokio::test]
async fn ec2_create_client_vpn_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    assert!(id.starts_with("cvpn-endpoint-"));
}

#[test_action("ec2", "DescribeClientVpnEndpoints", checksum = "bde17783")]
#[tokio::test]
async fn ec2_describe_client_vpn_endpoints() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .describe_client_vpn_endpoints()
        .client_vpn_endpoint_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.client_vpn_endpoints().len(), 1);
}

#[test_action("ec2", "DeleteClientVpnEndpoint", checksum = "9cd0d115")]
#[tokio::test]
async fn ec2_delete_client_vpn_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    c.delete_client_vpn_endpoint()
        .client_vpn_endpoint_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyClientVpnEndpoint", checksum = "9404f05d")]
#[tokio::test]
async fn ec2_modify_client_vpn_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .modify_client_vpn_endpoint()
        .client_vpn_endpoint_id(&id)
        .description("new")
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

#[test_action("ec2", "CreateClientVpnRoute", checksum = "bd87ca84")]
#[tokio::test]
async fn ec2_create_client_vpn_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .create_client_vpn_route()
        .client_vpn_endpoint_id(&id)
        .destination_cidr_block("0.0.0.0/0")
        .target_vpc_subnet_id("subnet-1")
        .send()
        .await
        .unwrap();
    assert!(r.status().is_some());
}

#[test_action("ec2", "DescribeClientVpnRoutes", checksum = "0aee6ee1")]
#[tokio::test]
async fn ec2_describe_client_vpn_routes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    c.create_client_vpn_route()
        .client_vpn_endpoint_id(&id)
        .destination_cidr_block("0.0.0.0/0")
        .target_vpc_subnet_id("subnet-1")
        .send()
        .await
        .unwrap();
    let r = c
        .describe_client_vpn_routes()
        .client_vpn_endpoint_id(&id)
        .send()
        .await
        .unwrap();
    assert!(!r.routes().is_empty());
}

#[test_action("ec2", "DeleteClientVpnRoute", checksum = "22204595")]
#[tokio::test]
async fn ec2_delete_client_vpn_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    c.create_client_vpn_route()
        .client_vpn_endpoint_id(&id)
        .destination_cidr_block("0.0.0.0/0")
        .target_vpc_subnet_id("subnet-1")
        .send()
        .await
        .unwrap();
    c.delete_client_vpn_route()
        .client_vpn_endpoint_id(&id)
        .destination_cidr_block("0.0.0.0/0")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AuthorizeClientVpnIngress", checksum = "93f07f78")]
#[tokio::test]
async fn ec2_authorize_client_vpn_ingress() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .authorize_client_vpn_ingress()
        .client_vpn_endpoint_id(&id)
        .target_network_cidr("10.0.0.0/16")
        .authorize_all_groups(true)
        .send()
        .await
        .unwrap();
    assert!(r.status().is_some());
}

#[test_action("ec2", "RevokeClientVpnIngress", checksum = "6df5ca65")]
#[tokio::test]
async fn ec2_revoke_client_vpn_ingress() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .revoke_client_vpn_ingress()
        .client_vpn_endpoint_id(&id)
        .target_network_cidr("10.0.0.0/16")
        .revoke_all_groups(true)
        .send()
        .await
        .unwrap();
    assert!(r.status().is_some());
}

#[test_action("ec2", "DescribeClientVpnAuthorizationRules", checksum = "b6ae5a77")]
#[tokio::test]
async fn ec2_describe_client_vpn_authorization_rules() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .describe_client_vpn_authorization_rules()
        .client_vpn_endpoint_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.authorization_rules().is_empty());
}

#[test_action("ec2", "AssociateClientVpnTargetNetwork", checksum = "4189edac")]
#[tokio::test]
async fn ec2_associate_client_vpn_target_network() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .associate_client_vpn_target_network()
        .client_vpn_endpoint_id(&id)
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap();
    assert!(r.association_id().is_some());
}

#[test_action("ec2", "DisassociateClientVpnTargetNetwork", checksum = "79b84774")]
#[tokio::test]
async fn ec2_disassociate_client_vpn_target_network() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let assoc = c
        .associate_client_vpn_target_network()
        .client_vpn_endpoint_id(&id)
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();
    let r = c
        .disassociate_client_vpn_target_network()
        .client_vpn_endpoint_id(&id)
        .association_id(&assoc)
        .send()
        .await
        .unwrap();
    assert!(r.association_id().is_some());
}

#[test_action("ec2", "DescribeClientVpnTargetNetworks", checksum = "b61ca5d7")]
#[tokio::test]
async fn ec2_describe_client_vpn_target_networks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    c.associate_client_vpn_target_network()
        .client_vpn_endpoint_id(&id)
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap();
    let r = c
        .describe_client_vpn_target_networks()
        .client_vpn_endpoint_id(&id)
        .send()
        .await
        .unwrap();
    assert!(!r.client_vpn_target_networks().is_empty());
}

#[test_action(
    "ec2",
    "ApplySecurityGroupsToClientVpnTargetNetwork",
    checksum = "3f3b107e"
)]
#[tokio::test]
async fn ec2_apply_security_groups_to_client_vpn_target_network() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .apply_security_groups_to_client_vpn_target_network()
        .client_vpn_endpoint_id(&id)
        .vpc_id("vpc-1")
        .security_group_ids("sg-1")
        .send()
        .await
        .unwrap();
    assert!(!r.security_group_ids().is_empty());
}

#[test_action("ec2", "DescribeClientVpnConnections", checksum = "bdd2a91d")]
#[tokio::test]
async fn ec2_describe_client_vpn_connections() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .describe_client_vpn_connections()
        .client_vpn_endpoint_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.connections().is_empty());
}

#[test_action("ec2", "TerminateClientVpnConnections", checksum = "f5949995")]
#[tokio::test]
async fn ec2_terminate_client_vpn_connections() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .terminate_client_vpn_connections()
        .client_vpn_endpoint_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.client_vpn_endpoint_id(), Some(id.as_str()));
}

#[test_action(
    "ec2",
    "ExportClientVpnClientCertificateRevocationList",
    checksum = "7a9a2f56"
)]
#[tokio::test]
async fn ec2_export_client_vpn_client_certificate_revocation_list() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .export_client_vpn_client_certificate_revocation_list()
        .client_vpn_endpoint_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.status().is_some());
}

#[test_action("ec2", "ExportClientVpnClientConfiguration", checksum = "018ef43b")]
#[tokio::test]
async fn ec2_export_client_vpn_client_configuration() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .export_client_vpn_client_configuration()
        .client_vpn_endpoint_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.client_configuration().is_some());
}

#[test_action(
    "ec2",
    "ImportClientVpnClientCertificateRevocationList",
    checksum = "0c559dc5"
)]
#[tokio::test]
async fn ec2_import_client_vpn_client_certificate_revocation_list() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_cvpn(&c).await;
    let r = c
        .import_client_vpn_client_certificate_revocation_list()
        .client_vpn_endpoint_id(&id)
        .certificate_revocation_list("-----BEGIN-----")
        .send()
        .await
        .unwrap();
    assert_eq!(r.r#return(), Some(true));
}

// ---- IPAM core ----

async fn make_ipam(c: &aws_sdk_ec2::Client) -> String {
    c.create_ipam()
        .send()
        .await
        .unwrap()
        .ipam()
        .unwrap()
        .ipam_id()
        .unwrap()
        .to_string()
}
async fn make_scope(c: &aws_sdk_ec2::Client) -> String {
    c.create_ipam_scope()
        .ipam_id("ipam-1")
        .send()
        .await
        .unwrap()
        .ipam_scope()
        .unwrap()
        .ipam_scope_id()
        .unwrap()
        .to_string()
}
async fn make_pool(c: &aws_sdk_ec2::Client) -> String {
    c.create_ipam_pool()
        .ipam_scope_id("ipam-scope-1")
        .address_family(aws_sdk_ec2::types::AddressFamily::Ipv4)
        .send()
        .await
        .unwrap()
        .ipam_pool()
        .unwrap()
        .ipam_pool_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateIpam", checksum = "3485527d")]
#[tokio::test]
async fn ec2_create_ipam() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ipam(&c).await;
    assert!(id.starts_with("ipam-"));
}

#[test_action("ec2", "DescribeIpams", checksum = "4c22ad7c")]
#[tokio::test]
async fn ec2_describe_ipams() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ipam(&c).await;
    let r = c.describe_ipams().ipam_ids(&id).send().await.unwrap();
    assert_eq!(r.ipams().len(), 1);
}

#[test_action("ec2", "ModifyIpam", checksum = "43870206")]
#[tokio::test]
async fn ec2_modify_ipam() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ipam(&c).await;
    let r = c
        .modify_ipam()
        .ipam_id(&id)
        .description("d")
        .send()
        .await
        .unwrap();
    assert_eq!(r.ipam().unwrap().ipam_id(), Some(id.as_str()));
}

#[test_action("ec2", "DeleteIpam", checksum = "2dc73cb6")]
#[tokio::test]
async fn ec2_delete_ipam() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ipam(&c).await;
    c.delete_ipam().ipam_id(&id).send().await.unwrap();
}

#[test_action("ec2", "CreateIpamScope", checksum = "61270911")]
#[tokio::test]
async fn ec2_create_ipam_scope() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_scope(&c).await;
    assert!(id.starts_with("ipam-scope-"));
}

#[test_action("ec2", "DescribeIpamScopes", checksum = "6acf9e38")]
#[tokio::test]
async fn ec2_describe_ipam_scopes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_scope(&c).await;
    let r = c.describe_ipam_scopes().send().await.unwrap();
    assert!(!r.ipam_scopes().is_empty());
}

#[test_action("ec2", "ModifyIpamScope", checksum = "1b880baa")]
#[tokio::test]
async fn ec2_modify_ipam_scope() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_scope(&c).await;
    let r = c
        .modify_ipam_scope()
        .ipam_scope_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_scope().is_some());
}

#[test_action("ec2", "DeleteIpamScope", checksum = "dd96106c")]
#[tokio::test]
async fn ec2_delete_ipam_scope() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_scope(&c).await;
    c.delete_ipam_scope()
        .ipam_scope_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateIpamPool", checksum = "025e7679")]
#[tokio::test]
async fn ec2_create_ipam_pool() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pool(&c).await;
    assert!(id.starts_with("ipam-pool-"));
}

#[test_action("ec2", "DescribeIpamPools", checksum = "bba93e30")]
#[tokio::test]
async fn ec2_describe_ipam_pools() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_pool(&c).await;
    let r = c.describe_ipam_pools().send().await.unwrap();
    assert!(!r.ipam_pools().is_empty());
}

#[test_action("ec2", "ModifyIpamPool", checksum = "c674e6f4")]
#[tokio::test]
async fn ec2_modify_ipam_pool() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pool(&c).await;
    let r = c.modify_ipam_pool().ipam_pool_id(&id).send().await.unwrap();
    assert!(r.ipam_pool().is_some());
}

#[test_action("ec2", "DeleteIpamPool", checksum = "bfa3df80")]
#[tokio::test]
async fn ec2_delete_ipam_pool() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pool(&c).await;
    c.delete_ipam_pool().ipam_pool_id(&id).send().await.unwrap();
}

#[test_action("ec2", "ProvisionIpamPoolCidr", checksum = "42fed347")]
#[tokio::test]
async fn ec2_provision_ipam_pool_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pool(&c).await;
    let r = c
        .provision_ipam_pool_cidr()
        .ipam_pool_id(&id)
        .cidr("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    assert!(r.ipam_pool_cidr().is_some());
}

#[test_action("ec2", "GetIpamPoolCidrs", checksum = "7a3e0d1c")]
#[tokio::test]
async fn ec2_get_ipam_pool_cidrs() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pool(&c).await;
    c.provision_ipam_pool_cidr()
        .ipam_pool_id(&id)
        .cidr("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    let r = c
        .get_ipam_pool_cidrs()
        .ipam_pool_id(&id)
        .send()
        .await
        .unwrap();
    assert!(!r.ipam_pool_cidrs().is_empty());
}

#[test_action("ec2", "DeprovisionIpamPoolCidr", checksum = "0d4add91")]
#[tokio::test]
async fn ec2_deprovision_ipam_pool_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pool(&c).await;
    c.provision_ipam_pool_cidr()
        .ipam_pool_id(&id)
        .cidr("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    let r = c
        .deprovision_ipam_pool_cidr()
        .ipam_pool_id(&id)
        .cidr("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    assert!(r.ipam_pool_cidr().is_some());
}

#[test_action("ec2", "AllocateIpamPoolCidr", checksum = "6da44777")]
#[tokio::test]
async fn ec2_allocate_ipam_pool_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pool(&c).await;
    let r = c
        .allocate_ipam_pool_cidr()
        .ipam_pool_id(&id)
        .cidr("10.0.0.0/24")
        .send()
        .await
        .unwrap();
    assert!(r.ipam_pool_allocation().is_some());
}

#[test_action("ec2", "GetIpamPoolAllocations", checksum = "f83978dd")]
#[tokio::test]
async fn ec2_get_ipam_pool_allocations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pool(&c).await;
    c.allocate_ipam_pool_cidr()
        .ipam_pool_id(&id)
        .cidr("10.0.0.0/24")
        .send()
        .await
        .unwrap();
    let r = c
        .get_ipam_pool_allocations()
        .ipam_pool_id(&id)
        .send()
        .await
        .unwrap();
    assert!(!r.ipam_pool_allocations().is_empty());
}

#[test_action("ec2", "ReleaseIpamPoolAllocation", checksum = "3f9121a6")]
#[tokio::test]
async fn ec2_release_ipam_pool_allocation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_pool(&c).await;
    let alloc = c
        .allocate_ipam_pool_cidr()
        .ipam_pool_id(&id)
        .cidr("10.0.0.0/24")
        .send()
        .await
        .unwrap()
        .ipam_pool_allocation()
        .unwrap()
        .ipam_pool_allocation_id()
        .unwrap()
        .to_string();
    let r = c
        .release_ipam_pool_allocation()
        .ipam_pool_id(&id)
        .cidr("10.0.0.0/24")
        .ipam_pool_allocation_id(&alloc)
        .send()
        .await
        .unwrap();
    assert_eq!(r.success(), Some(true));
}

#[test_action("ec2", "GetIpamResourceCidrs", checksum = "d06547a1")]
#[tokio::test]
async fn ec2_get_ipam_resource_cidrs() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_ipam_resource_cidrs()
        .ipam_scope_id("ipam-scope-1")
        .send()
        .await
        .unwrap();
    assert!(r.ipam_resource_cidrs().is_empty());
}

#[test_action("ec2", "ModifyIpamResourceCidr", checksum = "bfdff1ed")]
#[tokio::test]
async fn ec2_modify_ipam_resource_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .modify_ipam_resource_cidr()
        .resource_id("vpc-1")
        .resource_cidr("10.0.0.0/16")
        .resource_region("us-east-1")
        .current_ipam_scope_id("ipam-scope-1")
        .monitored(true)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_resource_cidr().is_some());
}

#[test_action("ec2", "GetIpamAddressHistory", checksum = "96d1874c")]
#[tokio::test]
async fn ec2_get_ipam_address_history() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_ipam_address_history()
        .cidr("10.0.0.0/16")
        .ipam_scope_id("ipam-scope-1")
        .send()
        .await
        .unwrap();
    assert!(r.history_records().is_empty());
}

#[test_action("ec2", "EnableIpamOrganizationAdminAccount", checksum = "c7a70a45")]
#[tokio::test]
async fn ec2_enable_ipam_organization_admin_account() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .enable_ipam_organization_admin_account()
        .delegated_admin_account_id("123456789012")
        .send()
        .await
        .unwrap();
    assert_eq!(r.success(), Some(true));
}

#[test_action("ec2", "DisableIpamOrganizationAdminAccount", checksum = "b2beab46")]
#[tokio::test]
async fn ec2_disable_ipam_organization_admin_account() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disable_ipam_organization_admin_account()
        .delegated_admin_account_id("123456789012")
        .send()
        .await
        .unwrap();
    assert_eq!(r.success(), Some(true));
}

// ---- IPAM resource discovery / BYOASN / BYOIP / external tokens ----

async fn make_rd(c: &aws_sdk_ec2::Client) -> String {
    c.create_ipam_resource_discovery()
        .send()
        .await
        .unwrap()
        .ipam_resource_discovery()
        .unwrap()
        .ipam_resource_discovery_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateIpamResourceDiscovery", checksum = "b6b75a20")]
#[tokio::test]
async fn ec2_create_ipam_resource_discovery() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_rd(&c).await;
    assert!(id.starts_with("ipam-res-disco-"));
}

#[test_action("ec2", "DescribeIpamResourceDiscoveries", checksum = "8bdfceb2")]
#[tokio::test]
async fn ec2_describe_ipam_resource_discoveries() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_rd(&c).await;
    let r = c.describe_ipam_resource_discoveries().send().await.unwrap();
    assert!(!r.ipam_resource_discoveries().is_empty());
}

#[test_action("ec2", "ModifyIpamResourceDiscovery", checksum = "48f4900c")]
#[tokio::test]
async fn ec2_modify_ipam_resource_discovery() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_rd(&c).await;
    let r = c
        .modify_ipam_resource_discovery()
        .ipam_resource_discovery_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_resource_discovery().is_some());
}

#[test_action("ec2", "DeleteIpamResourceDiscovery", checksum = "a1e94d5d")]
#[tokio::test]
async fn ec2_delete_ipam_resource_discovery() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_rd(&c).await;
    c.delete_ipam_resource_discovery()
        .ipam_resource_discovery_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AssociateIpamResourceDiscovery", checksum = "0acbb307")]
#[tokio::test]
async fn ec2_associate_ipam_resource_discovery() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_rd(&c).await;
    let r = c
        .associate_ipam_resource_discovery()
        .ipam_id("ipam-1")
        .ipam_resource_discovery_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_resource_discovery_association().is_some());
}

#[test_action(
    "ec2",
    "DescribeIpamResourceDiscoveryAssociations",
    checksum = "29bd1bbd"
)]
#[tokio::test]
async fn ec2_describe_ipam_resource_discovery_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_rd(&c).await;
    c.associate_ipam_resource_discovery()
        .ipam_id("ipam-1")
        .ipam_resource_discovery_id(&id)
        .send()
        .await
        .unwrap();
    let r = c
        .describe_ipam_resource_discovery_associations()
        .send()
        .await
        .unwrap();
    assert!(!r.ipam_resource_discovery_associations().is_empty());
}

#[test_action("ec2", "DisassociateIpamResourceDiscovery", checksum = "755dde2a")]
#[tokio::test]
async fn ec2_disassociate_ipam_resource_discovery() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_rd(&c).await;
    let assoc = c
        .associate_ipam_resource_discovery()
        .ipam_id("ipam-1")
        .ipam_resource_discovery_id(&id)
        .send()
        .await
        .unwrap()
        .ipam_resource_discovery_association()
        .unwrap()
        .ipam_resource_discovery_association_id()
        .unwrap()
        .to_string();
    let r = c
        .disassociate_ipam_resource_discovery()
        .ipam_resource_discovery_association_id(&assoc)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_resource_discovery_association().is_some());
}

#[test_action("ec2", "GetIpamDiscoveredAccounts", checksum = "0ac6e259")]
#[tokio::test]
async fn ec2_get_ipam_discovered_accounts() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_ipam_discovered_accounts()
        .ipam_resource_discovery_id("ipam-res-disco-1")
        .discovery_region("us-east-1")
        .send()
        .await
        .unwrap();
    assert!(r.ipam_discovered_accounts().is_empty());
}

#[test_action("ec2", "GetIpamDiscoveredPublicAddresses", checksum = "e0549d32")]
#[tokio::test]
async fn ec2_get_ipam_discovered_public_addresses() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_ipam_discovered_public_addresses()
        .ipam_resource_discovery_id("ipam-res-disco-1")
        .address_region("us-east-1")
        .send()
        .await
        .unwrap();
    assert!(r.ipam_discovered_public_addresses().is_empty());
}

#[test_action("ec2", "GetIpamDiscoveredResourceCidrs", checksum = "6f4dd930")]
#[tokio::test]
async fn ec2_get_ipam_discovered_resource_cidrs() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_ipam_discovered_resource_cidrs()
        .ipam_resource_discovery_id("ipam-res-disco-1")
        .resource_region("us-east-1")
        .send()
        .await
        .unwrap();
    assert!(r.ipam_discovered_resource_cidrs().is_empty());
}

#[test_action("ec2", "AssociateIpamByoasn", checksum = "7af62e68")]
#[tokio::test]
async fn ec2_associate_ipam_byoasn() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .associate_ipam_byoasn()
        .asn("64512")
        .cidr("10.0.0.0/24")
        .send()
        .await
        .unwrap();
    assert!(r.asn_association().is_some());
}

#[test_action("ec2", "DisassociateIpamByoasn", checksum = "6a248997")]
#[tokio::test]
async fn ec2_disassociate_ipam_byoasn() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .disassociate_ipam_byoasn()
        .asn("64512")
        .cidr("10.0.0.0/24")
        .send()
        .await
        .unwrap();
    assert!(r.asn_association().is_some());
}

#[test_action("ec2", "ProvisionIpamByoasn", checksum = "9da68566")]
#[tokio::test]
async fn ec2_provision_ipam_byoasn() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .provision_ipam_byoasn()
        .ipam_id("ipam-1")
        .asn("64512")
        .asn_authorization_context(
            aws_sdk_ec2::types::AsnAuthorizationContext::builder()
                .message("m")
                .signature("sig")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(r.byoasn().is_some());
}

#[test_action("ec2", "DeprovisionIpamByoasn", checksum = "d5d5c14d")]
#[tokio::test]
async fn ec2_deprovision_ipam_byoasn() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .deprovision_ipam_byoasn()
        .ipam_id("ipam-1")
        .asn("64512")
        .send()
        .await
        .unwrap();
    assert!(r.byoasn().is_some());
}

#[test_action("ec2", "DescribeIpamByoasn", checksum = "c039e002")]
#[tokio::test]
async fn ec2_describe_ipam_byoasn() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.associate_ipam_byoasn()
        .asn("64512")
        .cidr("10.0.0.0/24")
        .send()
        .await
        .unwrap();
    let r = c.describe_ipam_byoasn().send().await.unwrap();
    assert!(!r.byoasns().is_empty());
}

#[test_action("ec2", "MoveByoipCidrToIpam", checksum = "666a0d06")]
#[tokio::test]
async fn ec2_move_byoip_cidr_to_ipam() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .move_byoip_cidr_to_ipam()
        .cidr("10.0.0.0/24")
        .ipam_pool_id("ipam-pool-1")
        .ipam_pool_owner("123456789012")
        .send()
        .await
        .unwrap();
    assert!(r.byoip_cidr().is_some());
}

async fn make_token(c: &aws_sdk_ec2::Client) -> String {
    c.create_ipam_external_resource_verification_token()
        .ipam_id("ipam-1")
        .send()
        .await
        .unwrap()
        .ipam_external_resource_verification_token()
        .unwrap()
        .ipam_external_resource_verification_token_id()
        .unwrap()
        .to_string()
}

#[test_action(
    "ec2",
    "CreateIpamExternalResourceVerificationToken",
    checksum = "68eda0ae"
)]
#[tokio::test]
async fn ec2_create_ipam_external_resource_verification_token() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_token(&c).await;
    assert!(id.starts_with("ipam-ext-token-"));
}

#[test_action(
    "ec2",
    "DescribeIpamExternalResourceVerificationTokens",
    checksum = "bbd3bdc9"
)]
#[tokio::test]
async fn ec2_describe_ipam_external_resource_verification_tokens() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_token(&c).await;
    let r = c
        .describe_ipam_external_resource_verification_tokens()
        .send()
        .await
        .unwrap();
    assert!(!r.ipam_external_resource_verification_tokens().is_empty());
}

#[test_action(
    "ec2",
    "DeleteIpamExternalResourceVerificationToken",
    checksum = "f2173a16"
)]
#[tokio::test]
async fn ec2_delete_ipam_external_resource_verification_token() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_token(&c).await;
    c.delete_ipam_external_resource_verification_token()
        .ipam_external_resource_verification_token_id(&id)
        .send()
        .await
        .unwrap();
}

// ---- IPAM policies + prefix-list resolvers ----

async fn make_policy(c: &aws_sdk_ec2::Client) -> String {
    c.create_ipam_policy()
        .ipam_id("ipam-1")
        .send()
        .await
        .unwrap()
        .ipam_policy()
        .unwrap()
        .ipam_policy_id()
        .unwrap()
        .to_string()
}
async fn make_resolver(c: &aws_sdk_ec2::Client) -> String {
    c.create_ipam_prefix_list_resolver()
        .ipam_id("ipam-1")
        .address_family(aws_sdk_ec2::types::AddressFamily::Ipv4)
        .send()
        .await
        .unwrap()
        .ipam_prefix_list_resolver()
        .unwrap()
        .ipam_prefix_list_resolver_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateIpamPolicy", checksum = "15092a13")]
#[tokio::test]
async fn ec2_create_ipam_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_policy(&c).await;
    assert!(id.starts_with("ipam-policy-"));
}

#[test_action("ec2", "DescribeIpamPolicies", checksum = "bbe66478")]
#[tokio::test]
async fn ec2_describe_ipam_policies() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_policy(&c).await;
    let r = c.describe_ipam_policies().send().await.unwrap();
    assert!(!r.ipam_policies().is_empty());
}

#[test_action("ec2", "DeleteIpamPolicy", checksum = "63e733ca")]
#[tokio::test]
async fn ec2_delete_ipam_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_policy(&c).await;
    c.delete_ipam_policy()
        .ipam_policy_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableIpamPolicy", checksum = "1be5d705")]
#[tokio::test]
async fn ec2_enable_ipam_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_policy(&c).await;
    let r = c
        .enable_ipam_policy()
        .ipam_policy_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.ipam_policy_id(), Some(id.as_str()));
}

#[test_action("ec2", "DisableIpamPolicy", checksum = "fdb0f04b")]
#[tokio::test]
async fn ec2_disable_ipam_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_policy(&c).await;
    // Response carries only a <return> boolean; send() succeeding proves the op
    // routes and the response deserializes.
    c.disable_ipam_policy()
        .ipam_policy_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetEnabledIpamPolicy", checksum = "78e1f24f")]
#[tokio::test]
async fn ec2_get_enabled_ipam_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.get_enabled_ipam_policy().send().await.unwrap();
    assert_eq!(r.ipam_policy_enabled(), Some(false));
}

#[test_action("ec2", "GetIpamPolicyAllocationRules", checksum = "e6b9b7de")]
#[tokio::test]
async fn ec2_get_ipam_policy_allocation_rules() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_policy(&c).await;
    let r = c
        .get_ipam_policy_allocation_rules()
        .ipam_policy_id(&id)
        .resource_type(aws_sdk_ec2::types::IpamPolicyResourceType::Eip)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_policy_documents().is_empty());
}

#[test_action("ec2", "ModifyIpamPolicyAllocationRules", checksum = "6e206a4c")]
#[tokio::test]
async fn ec2_modify_ipam_policy_allocation_rules() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_policy(&c).await;
    let r = c
        .modify_ipam_policy_allocation_rules()
        .ipam_policy_id(&id)
        .locale("us-east-1")
        .resource_type(aws_sdk_ec2::types::IpamPolicyResourceType::Eip)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_policy_document().is_some());
}

#[test_action("ec2", "GetIpamPolicyOrganizationTargets", checksum = "1c19b6ef")]
#[tokio::test]
async fn ec2_get_ipam_policy_organization_targets() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_policy(&c).await;
    let r = c
        .get_ipam_policy_organization_targets()
        .ipam_policy_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.organization_targets().is_empty());
}

#[test_action("ec2", "CreateIpamPrefixListResolver", checksum = "a8163dc8")]
#[tokio::test]
async fn ec2_create_ipam_prefix_list_resolver() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_resolver(&c).await;
    assert!(id.starts_with("ipam-pl-res-"));
}

#[test_action("ec2", "DescribeIpamPrefixListResolvers", checksum = "2e6cdacf")]
#[tokio::test]
async fn ec2_describe_ipam_prefix_list_resolvers() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_resolver(&c).await;
    let r = c
        .describe_ipam_prefix_list_resolvers()
        .send()
        .await
        .unwrap();
    assert!(!r.ipam_prefix_list_resolvers().is_empty());
}

#[test_action("ec2", "ModifyIpamPrefixListResolver", checksum = "d6f970c6")]
#[tokio::test]
async fn ec2_modify_ipam_prefix_list_resolver() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_resolver(&c).await;
    let r = c
        .modify_ipam_prefix_list_resolver()
        .ipam_prefix_list_resolver_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_prefix_list_resolver().is_some());
}

#[test_action("ec2", "DeleteIpamPrefixListResolver", checksum = "b2094029")]
#[tokio::test]
async fn ec2_delete_ipam_prefix_list_resolver() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_resolver(&c).await;
    c.delete_ipam_prefix_list_resolver()
        .ipam_prefix_list_resolver_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateIpamPrefixListResolverTarget", checksum = "564bc0d9")]
#[tokio::test]
async fn ec2_create_ipam_prefix_list_resolver_target() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rid = make_resolver(&c).await;
    let r = c
        .create_ipam_prefix_list_resolver_target()
        .ipam_prefix_list_resolver_id(&rid)
        .prefix_list_id("pl-1")
        .prefix_list_region("us-east-1")
        .track_latest_version(true)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_prefix_list_resolver_target().is_some());
}

#[test_action("ec2", "DescribeIpamPrefixListResolverTargets", checksum = "1bd06910")]
#[tokio::test]
async fn ec2_describe_ipam_prefix_list_resolver_targets() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rid = make_resolver(&c).await;
    c.create_ipam_prefix_list_resolver_target()
        .ipam_prefix_list_resolver_id(&rid)
        .prefix_list_id("pl-1")
        .prefix_list_region("us-east-1")
        .track_latest_version(true)
        .send()
        .await
        .unwrap();
    let r = c
        .describe_ipam_prefix_list_resolver_targets()
        .send()
        .await
        .unwrap();
    assert!(!r.ipam_prefix_list_resolver_targets().is_empty());
}

#[test_action("ec2", "ModifyIpamPrefixListResolverTarget", checksum = "012d41a3")]
#[tokio::test]
async fn ec2_modify_ipam_prefix_list_resolver_target() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rid = make_resolver(&c).await;
    let tid = c
        .create_ipam_prefix_list_resolver_target()
        .ipam_prefix_list_resolver_id(&rid)
        .prefix_list_id("pl-1")
        .prefix_list_region("us-east-1")
        .track_latest_version(true)
        .send()
        .await
        .unwrap()
        .ipam_prefix_list_resolver_target()
        .unwrap()
        .ipam_prefix_list_resolver_target_id()
        .unwrap()
        .to_string();
    let r = c
        .modify_ipam_prefix_list_resolver_target()
        .ipam_prefix_list_resolver_target_id(&tid)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_prefix_list_resolver_target().is_some());
}

#[test_action("ec2", "DeleteIpamPrefixListResolverTarget", checksum = "dfe0ad7a")]
#[tokio::test]
async fn ec2_delete_ipam_prefix_list_resolver_target() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rid = make_resolver(&c).await;
    let tid = c
        .create_ipam_prefix_list_resolver_target()
        .ipam_prefix_list_resolver_id(&rid)
        .prefix_list_id("pl-1")
        .prefix_list_region("us-east-1")
        .track_latest_version(true)
        .send()
        .await
        .unwrap()
        .ipam_prefix_list_resolver_target()
        .unwrap()
        .ipam_prefix_list_resolver_target_id()
        .unwrap()
        .to_string();
    c.delete_ipam_prefix_list_resolver_target()
        .ipam_prefix_list_resolver_target_id(&tid)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetIpamPrefixListResolverRules", checksum = "7415750b")]
#[tokio::test]
async fn ec2_get_ipam_prefix_list_resolver_rules() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rid = make_resolver(&c).await;
    let r = c
        .get_ipam_prefix_list_resolver_rules()
        .ipam_prefix_list_resolver_id(&rid)
        .send()
        .await
        .unwrap();
    assert!(r.rules().is_empty());
}

#[test_action("ec2", "GetIpamPrefixListResolverVersions", checksum = "0b06aea9")]
#[tokio::test]
async fn ec2_get_ipam_prefix_list_resolver_versions() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rid = make_resolver(&c).await;
    let r = c
        .get_ipam_prefix_list_resolver_versions()
        .ipam_prefix_list_resolver_id(&rid)
        .send()
        .await
        .unwrap();
    assert!(r.ipam_prefix_list_resolver_versions().is_empty());
}

#[test_action(
    "ec2",
    "GetIpamPrefixListResolverVersionEntries",
    checksum = "d68bce61"
)]
#[tokio::test]
async fn ec2_get_ipam_prefix_list_resolver_version_entries() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rid = make_resolver(&c).await;
    let r = c
        .get_ipam_prefix_list_resolver_version_entries()
        .ipam_prefix_list_resolver_id(&rid)
        .ipam_prefix_list_resolver_version(1)
        .send()
        .await
        .unwrap();
    assert!(r.entries().is_empty());
}

// ---- Verified Access ----

async fn make_vai(c: &aws_sdk_ec2::Client) -> String {
    c.create_verified_access_instance()
        .send()
        .await
        .unwrap()
        .verified_access_instance()
        .unwrap()
        .verified_access_instance_id()
        .unwrap()
        .to_string()
}
async fn make_vatp(c: &aws_sdk_ec2::Client) -> String {
    c.create_verified_access_trust_provider()
        .trust_provider_type(aws_sdk_ec2::types::TrustProviderType::User)
        .policy_reference_name("pol")
        .send()
        .await
        .unwrap()
        .verified_access_trust_provider()
        .unwrap()
        .verified_access_trust_provider_id()
        .unwrap()
        .to_string()
}
async fn make_vagr(c: &aws_sdk_ec2::Client) -> String {
    let inst = make_vai(c).await;
    c.create_verified_access_group()
        .verified_access_instance_id(&inst)
        .send()
        .await
        .unwrap()
        .verified_access_group()
        .unwrap()
        .verified_access_group_id()
        .unwrap()
        .to_string()
}
async fn make_vae(c: &aws_sdk_ec2::Client) -> String {
    let g = make_vagr(c).await;
    c.create_verified_access_endpoint()
        .verified_access_group_id(&g)
        .endpoint_type(aws_sdk_ec2::types::VerifiedAccessEndpointType::LoadBalancer)
        .attachment_type(aws_sdk_ec2::types::VerifiedAccessEndpointAttachmentType::Vpc)
        .send()
        .await
        .unwrap()
        .verified_access_endpoint()
        .unwrap()
        .verified_access_endpoint_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateVerifiedAccessInstance", checksum = "e83a8aae")]
#[tokio::test]
async fn ec2_create_verified_access_instance() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vai(&c).await;
    assert!(id.starts_with("vai-"));
}

#[test_action("ec2", "DescribeVerifiedAccessInstances", checksum = "b9ee58a7")]
#[tokio::test]
async fn ec2_describe_verified_access_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_vai(&c).await;
    let r = c.describe_verified_access_instances().send().await.unwrap();
    assert!(!r.verified_access_instances().is_empty());
}

#[test_action("ec2", "ModifyVerifiedAccessInstance", checksum = "11288d69")]
#[tokio::test]
async fn ec2_modify_verified_access_instance() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vai(&c).await;
    let r = c
        .modify_verified_access_instance()
        .verified_access_instance_id(&id)
        .description("d")
        .send()
        .await
        .unwrap();
    assert!(r.verified_access_instance().is_some());
}

#[test_action("ec2", "DeleteVerifiedAccessInstance", checksum = "e5c130ed")]
#[tokio::test]
async fn ec2_delete_verified_access_instance() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vai(&c).await;
    c.delete_verified_access_instance()
        .verified_access_instance_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateVerifiedAccessTrustProvider", checksum = "5efc2ca6")]
#[tokio::test]
async fn ec2_create_verified_access_trust_provider() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vatp(&c).await;
    assert!(id.starts_with("vatp-"));
}

#[test_action("ec2", "DescribeVerifiedAccessTrustProviders", checksum = "a300c6bd")]
#[tokio::test]
async fn ec2_describe_verified_access_trust_providers() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_vatp(&c).await;
    let r = c
        .describe_verified_access_trust_providers()
        .send()
        .await
        .unwrap();
    assert!(!r.verified_access_trust_providers().is_empty());
}

#[test_action("ec2", "ModifyVerifiedAccessTrustProvider", checksum = "295b8b17")]
#[tokio::test]
async fn ec2_modify_verified_access_trust_provider() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vatp(&c).await;
    let r = c
        .modify_verified_access_trust_provider()
        .verified_access_trust_provider_id(&id)
        .description("d")
        .send()
        .await
        .unwrap();
    assert!(r.verified_access_trust_provider().is_some());
}

#[test_action("ec2", "DeleteVerifiedAccessTrustProvider", checksum = "a257a90b")]
#[tokio::test]
async fn ec2_delete_verified_access_trust_provider() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vatp(&c).await;
    c.delete_verified_access_trust_provider()
        .verified_access_trust_provider_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AttachVerifiedAccessTrustProvider", checksum = "a8ee0768")]
#[tokio::test]
async fn ec2_attach_verified_access_trust_provider() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let inst = make_vai(&c).await;
    let tp = make_vatp(&c).await;
    let r = c
        .attach_verified_access_trust_provider()
        .verified_access_instance_id(&inst)
        .verified_access_trust_provider_id(&tp)
        .send()
        .await
        .unwrap();
    assert!(r.verified_access_instance().is_some());
}

#[test_action("ec2", "DetachVerifiedAccessTrustProvider", checksum = "59cc2823")]
#[tokio::test]
async fn ec2_detach_verified_access_trust_provider() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let inst = make_vai(&c).await;
    let tp = make_vatp(&c).await;
    c.attach_verified_access_trust_provider()
        .verified_access_instance_id(&inst)
        .verified_access_trust_provider_id(&tp)
        .send()
        .await
        .unwrap();
    let r = c
        .detach_verified_access_trust_provider()
        .verified_access_instance_id(&inst)
        .verified_access_trust_provider_id(&tp)
        .send()
        .await
        .unwrap();
    assert!(r.verified_access_instance().is_some());
}

#[test_action("ec2", "CreateVerifiedAccessGroup", checksum = "ee1b40da")]
#[tokio::test]
async fn ec2_create_verified_access_group() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vagr(&c).await;
    assert!(id.starts_with("vagr-"));
}

#[test_action("ec2", "DescribeVerifiedAccessGroups", checksum = "de91c56f")]
#[tokio::test]
async fn ec2_describe_verified_access_groups() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_vagr(&c).await;
    let r = c.describe_verified_access_groups().send().await.unwrap();
    assert!(!r.verified_access_groups().is_empty());
}

#[test_action("ec2", "ModifyVerifiedAccessGroup", checksum = "33ce27f4")]
#[tokio::test]
async fn ec2_modify_verified_access_group() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vagr(&c).await;
    let r = c
        .modify_verified_access_group()
        .verified_access_group_id(&id)
        .description("d")
        .send()
        .await
        .unwrap();
    assert!(r.verified_access_group().is_some());
}

#[test_action("ec2", "DeleteVerifiedAccessGroup", checksum = "f3b3ed0e")]
#[tokio::test]
async fn ec2_delete_verified_access_group() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vagr(&c).await;
    c.delete_verified_access_group()
        .verified_access_group_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyVerifiedAccessGroupPolicy", checksum = "9c39f661")]
#[tokio::test]
async fn ec2_modify_verified_access_group_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vagr(&c).await;
    let r = c
        .modify_verified_access_group_policy()
        .verified_access_group_id(&id)
        .policy_enabled(true)
        .policy_document("permit(principal,action,resource);")
        .send()
        .await
        .unwrap();
    assert_eq!(r.policy_enabled(), Some(true));
}

#[test_action("ec2", "GetVerifiedAccessGroupPolicy", checksum = "5a55cb57")]
#[tokio::test]
async fn ec2_get_verified_access_group_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vagr(&c).await;
    c.modify_verified_access_group_policy()
        .verified_access_group_id(&id)
        .policy_enabled(true)
        .policy_document("permit(principal,action,resource);")
        .send()
        .await
        .unwrap();
    let r = c
        .get_verified_access_group_policy()
        .verified_access_group_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.policy_document(),
        Some("permit(principal,action,resource);")
    );
}

#[test_action("ec2", "CreateVerifiedAccessEndpoint", checksum = "e44490ec")]
#[tokio::test]
async fn ec2_create_verified_access_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vae(&c).await;
    assert!(id.starts_with("vae-"));
}

#[test_action("ec2", "DescribeVerifiedAccessEndpoints", checksum = "38a4d17c")]
#[tokio::test]
async fn ec2_describe_verified_access_endpoints() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_vae(&c).await;
    let r = c.describe_verified_access_endpoints().send().await.unwrap();
    assert!(!r.verified_access_endpoints().is_empty());
}

#[test_action("ec2", "ModifyVerifiedAccessEndpoint", checksum = "be54b578")]
#[tokio::test]
async fn ec2_modify_verified_access_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vae(&c).await;
    let r = c
        .modify_verified_access_endpoint()
        .verified_access_endpoint_id(&id)
        .description("d")
        .send()
        .await
        .unwrap();
    assert!(r.verified_access_endpoint().is_some());
}

#[test_action("ec2", "DeleteVerifiedAccessEndpoint", checksum = "42d685b3")]
#[tokio::test]
async fn ec2_delete_verified_access_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vae(&c).await;
    c.delete_verified_access_endpoint()
        .verified_access_endpoint_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyVerifiedAccessEndpointPolicy", checksum = "546e40f8")]
#[tokio::test]
async fn ec2_modify_verified_access_endpoint_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vae(&c).await;
    let r = c
        .modify_verified_access_endpoint_policy()
        .verified_access_endpoint_id(&id)
        .policy_enabled(true)
        .policy_document("permit(principal,action,resource);")
        .send()
        .await
        .unwrap();
    assert_eq!(r.policy_enabled(), Some(true));
}

#[test_action("ec2", "GetVerifiedAccessEndpointPolicy", checksum = "e018c6ce")]
#[tokio::test]
async fn ec2_get_verified_access_endpoint_policy() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vae(&c).await;
    c.modify_verified_access_endpoint_policy()
        .verified_access_endpoint_id(&id)
        .policy_enabled(true)
        .policy_document("permit(principal,action,resource);")
        .send()
        .await
        .unwrap();
    let r = c
        .get_verified_access_endpoint_policy()
        .verified_access_endpoint_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.policy_document(),
        Some("permit(principal,action,resource);")
    );
}

#[test_action("ec2", "GetVerifiedAccessEndpointTargets", checksum = "3420c2d1")]
#[tokio::test]
async fn ec2_get_verified_access_endpoint_targets() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vae(&c).await;
    let r = c
        .get_verified_access_endpoint_targets()
        .verified_access_endpoint_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.verified_access_endpoint_targets().is_empty());
}

#[test_action(
    "ec2",
    "DescribeVerifiedAccessInstanceLoggingConfigurations",
    checksum = "95729583"
)]
#[tokio::test]
async fn ec2_describe_verified_access_instance_logging_configurations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .describe_verified_access_instance_logging_configurations()
        .send()
        .await
        .unwrap();
    assert!(r.logging_configurations().is_empty());
}

#[test_action(
    "ec2",
    "ModifyVerifiedAccessInstanceLoggingConfiguration",
    checksum = "2ca1bb40"
)]
#[tokio::test]
async fn ec2_modify_verified_access_instance_logging_configuration() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vai(&c).await;
    let r = c
        .modify_verified_access_instance_logging_configuration()
        .verified_access_instance_id(&id)
        .access_logs(aws_sdk_ec2::types::VerifiedAccessLogOptions::builder().build())
        .send()
        .await
        .unwrap();
    assert!(r.logging_configuration().is_some());
}

#[test_action(
    "ec2",
    "ExportVerifiedAccessInstanceClientConfiguration",
    checksum = "71eb0af2"
)]
#[tokio::test]
async fn ec2_export_verified_access_instance_client_configuration() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vai(&c).await;
    let r = c
        .export_verified_access_instance_client_configuration()
        .verified_access_instance_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.verified_access_instance_id(), Some(id.as_str()));
}

// ---- Network Insights ----

async fn make_path(c: &aws_sdk_ec2::Client) -> String {
    c.create_network_insights_path()
        .source("eni-1")
        .protocol(aws_sdk_ec2::types::Protocol::Tcp)
        .client_token("tok")
        .send()
        .await
        .unwrap()
        .network_insights_path()
        .unwrap()
        .network_insights_path_id()
        .unwrap()
        .to_string()
}
async fn make_ni_scope(c: &aws_sdk_ec2::Client) -> String {
    c.create_network_insights_access_scope()
        .client_token("tok")
        .send()
        .await
        .unwrap()
        .network_insights_access_scope()
        .unwrap()
        .network_insights_access_scope_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateNetworkInsightsPath", checksum = "dd7c97ff")]
#[tokio::test]
async fn ec2_create_network_insights_path() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_path(&c).await;
    assert!(id.starts_with("nip-"));
}

#[test_action("ec2", "DescribeNetworkInsightsPaths", checksum = "0d34bc2f")]
#[tokio::test]
async fn ec2_describe_network_insights_paths() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_path(&c).await;
    let r = c.describe_network_insights_paths().send().await.unwrap();
    assert!(!r.network_insights_paths().is_empty());
}

#[test_action("ec2", "DeleteNetworkInsightsPath", checksum = "2416b2c7")]
#[tokio::test]
async fn ec2_delete_network_insights_path() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_path(&c).await;
    let r = c
        .delete_network_insights_path()
        .network_insights_path_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.network_insights_path_id(), Some(id.as_str()));
}

#[test_action("ec2", "StartNetworkInsightsAnalysis", checksum = "58d89237")]
#[tokio::test]
async fn ec2_start_network_insights_analysis() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let p = make_path(&c).await;
    let r = c
        .start_network_insights_analysis()
        .network_insights_path_id(&p)
        .client_token("tok2")
        .send()
        .await
        .unwrap();
    assert!(r.network_insights_analysis().is_some());
}

#[test_action("ec2", "DescribeNetworkInsightsAnalyses", checksum = "d802e189")]
#[tokio::test]
async fn ec2_describe_network_insights_analyses() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let p = make_path(&c).await;
    c.start_network_insights_analysis()
        .network_insights_path_id(&p)
        .client_token("tok2")
        .send()
        .await
        .unwrap();
    let r = c.describe_network_insights_analyses().send().await.unwrap();
    assert!(!r.network_insights_analyses().is_empty());
}

#[test_action("ec2", "DeleteNetworkInsightsAnalysis", checksum = "e9aeb3e1")]
#[tokio::test]
async fn ec2_delete_network_insights_analysis() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let p = make_path(&c).await;
    let a = c
        .start_network_insights_analysis()
        .network_insights_path_id(&p)
        .client_token("tok2")
        .send()
        .await
        .unwrap()
        .network_insights_analysis()
        .unwrap()
        .network_insights_analysis_id()
        .unwrap()
        .to_string();
    let r = c
        .delete_network_insights_analysis()
        .network_insights_analysis_id(&a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.network_insights_analysis_id(), Some(a.as_str()));
}

#[test_action("ec2", "CreateNetworkInsightsAccessScope", checksum = "df8f0d0a")]
#[tokio::test]
async fn ec2_create_network_insights_access_scope() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ni_scope(&c).await;
    assert!(id.starts_with("nis-"));
}

#[test_action("ec2", "DescribeNetworkInsightsAccessScopes", checksum = "61d6847c")]
#[tokio::test]
async fn ec2_describe_network_insights_access_scopes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_ni_scope(&c).await;
    let r = c
        .describe_network_insights_access_scopes()
        .send()
        .await
        .unwrap();
    assert!(!r.network_insights_access_scopes().is_empty());
}

#[test_action("ec2", "DeleteNetworkInsightsAccessScope", checksum = "17504ef3")]
#[tokio::test]
async fn ec2_delete_network_insights_access_scope() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ni_scope(&c).await;
    let r = c
        .delete_network_insights_access_scope()
        .network_insights_access_scope_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(r.network_insights_access_scope_id(), Some(id.as_str()));
}

#[test_action("ec2", "GetNetworkInsightsAccessScopeContent", checksum = "d14b8dd2")]
#[tokio::test]
async fn ec2_get_network_insights_access_scope_content() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ni_scope(&c).await;
    let r = c
        .get_network_insights_access_scope_content()
        .network_insights_access_scope_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.network_insights_access_scope_content().is_some());
}

#[test_action(
    "ec2",
    "StartNetworkInsightsAccessScopeAnalysis",
    checksum = "81da18fe"
)]
#[tokio::test]
async fn ec2_start_network_insights_access_scope_analysis() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let sc = make_ni_scope(&c).await;
    let r = c
        .start_network_insights_access_scope_analysis()
        .network_insights_access_scope_id(&sc)
        .client_token("tok2")
        .send()
        .await
        .unwrap();
    assert!(r.network_insights_access_scope_analysis().is_some());
}

#[test_action(
    "ec2",
    "DescribeNetworkInsightsAccessScopeAnalyses",
    checksum = "627666bd"
)]
#[tokio::test]
async fn ec2_describe_network_insights_access_scope_analyses() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let sc = make_ni_scope(&c).await;
    c.start_network_insights_access_scope_analysis()
        .network_insights_access_scope_id(&sc)
        .client_token("tok2")
        .send()
        .await
        .unwrap();
    let r = c
        .describe_network_insights_access_scope_analyses()
        .send()
        .await
        .unwrap();
    assert!(!r.network_insights_access_scope_analyses().is_empty());
}

#[test_action(
    "ec2",
    "DeleteNetworkInsightsAccessScopeAnalysis",
    checksum = "32ba3b0a"
)]
#[tokio::test]
async fn ec2_delete_network_insights_access_scope_analysis() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let sc = make_ni_scope(&c).await;
    let a = c
        .start_network_insights_access_scope_analysis()
        .network_insights_access_scope_id(&sc)
        .client_token("tok2")
        .send()
        .await
        .unwrap()
        .network_insights_access_scope_analysis()
        .unwrap()
        .network_insights_access_scope_analysis_id()
        .unwrap()
        .to_string();
    let r = c
        .delete_network_insights_access_scope_analysis()
        .network_insights_access_scope_analysis_id(&a)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.network_insights_access_scope_analysis_id(),
        Some(a.as_str())
    );
}

#[test_action(
    "ec2",
    "GetNetworkInsightsAccessScopeAnalysisFindings",
    checksum = "1e1b5203"
)]
#[tokio::test]
async fn ec2_get_network_insights_access_scope_analysis_findings() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let sc = make_ni_scope(&c).await;
    let a = c
        .start_network_insights_access_scope_analysis()
        .network_insights_access_scope_id(&sc)
        .client_token("tok2")
        .send()
        .await
        .unwrap()
        .network_insights_access_scope_analysis()
        .unwrap()
        .network_insights_access_scope_analysis_id()
        .unwrap()
        .to_string();
    let r = c
        .get_network_insights_access_scope_analysis_findings()
        .network_insights_access_scope_analysis_id(&a)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.network_insights_access_scope_analysis_id(),
        Some(a.as_str())
    );
}

// ---- local gateway / outpost / coip ----

async fn make_lgrt(c: &aws_sdk_ec2::Client) -> String {
    c.create_local_gateway_route_table()
        .local_gateway_id("lgw-1")
        .send()
        .await
        .unwrap()
        .local_gateway_route_table()
        .unwrap()
        .local_gateway_route_table_id()
        .unwrap()
        .to_string()
}
async fn make_vifg(c: &aws_sdk_ec2::Client) -> String {
    c.create_local_gateway_virtual_interface_group()
        .local_gateway_id("lgw-1")
        .send()
        .await
        .unwrap()
        .local_gateway_virtual_interface_group()
        .unwrap()
        .local_gateway_virtual_interface_group_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateCarrierGateway", checksum = "d0d31b02")]
#[tokio::test]
async fn ec2_create_carrier_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_carrier_gateway()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    assert!(r
        .carrier_gateway()
        .unwrap()
        .carrier_gateway_id()
        .unwrap()
        .starts_with("cagw-"));
}

#[test_action("ec2", "DescribeCarrierGateways", checksum = "4508ce3f")]
#[tokio::test]
async fn ec2_describe_carrier_gateways() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_carrier_gateway()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    let r = c.describe_carrier_gateways().send().await.unwrap();
    assert!(!r.carrier_gateways().is_empty());
}

#[test_action("ec2", "DeleteCarrierGateway", checksum = "c8a1e4a5")]
#[tokio::test]
async fn ec2_delete_carrier_gateway() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_carrier_gateway()
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .carrier_gateway()
        .unwrap()
        .carrier_gateway_id()
        .unwrap()
        .to_string();
    c.delete_carrier_gateway()
        .carrier_gateway_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateCoipPool", checksum = "6ae7afd8")]
#[tokio::test]
async fn ec2_create_coip_pool() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .create_coip_pool()
        .local_gateway_route_table_id("lgw-rtb-1")
        .send()
        .await
        .unwrap();
    assert!(r.coip_pool().is_some());
}

#[test_action("ec2", "DescribeCoipPools", checksum = "c0695b56")]
#[tokio::test]
async fn ec2_describe_coip_pools() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_coip_pool()
        .local_gateway_route_table_id("lgw-rtb-1")
        .send()
        .await
        .unwrap();
    let r = c.describe_coip_pools().send().await.unwrap();
    assert!(!r.coip_pools().is_empty());
}

#[test_action("ec2", "DeleteCoipPool", checksum = "be24433b")]
#[tokio::test]
async fn ec2_delete_coip_pool() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_coip_pool()
        .local_gateway_route_table_id("lgw-rtb-1")
        .send()
        .await
        .unwrap()
        .coip_pool()
        .unwrap()
        .pool_id()
        .unwrap()
        .to_string();
    c.delete_coip_pool().coip_pool_id(&id).send().await.unwrap();
}

#[test_action("ec2", "CreateCoipCidr", checksum = "56c01e87")]
#[tokio::test]
async fn ec2_create_coip_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_coip_pool()
        .local_gateway_route_table_id("lgw-rtb-1")
        .send()
        .await
        .unwrap()
        .coip_pool()
        .unwrap()
        .pool_id()
        .unwrap()
        .to_string();
    let r = c
        .create_coip_cidr()
        .cidr("10.0.0.0/24")
        .coip_pool_id(&id)
        .send()
        .await
        .unwrap();
    assert!(r.coip_cidr().is_some());
}

#[test_action("ec2", "DeleteCoipCidr", checksum = "095a80a5")]
#[tokio::test]
async fn ec2_delete_coip_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = c
        .create_coip_pool()
        .local_gateway_route_table_id("lgw-rtb-1")
        .send()
        .await
        .unwrap()
        .coip_pool()
        .unwrap()
        .pool_id()
        .unwrap()
        .to_string();
    c.create_coip_cidr()
        .cidr("10.0.0.0/24")
        .coip_pool_id(&id)
        .send()
        .await
        .unwrap();
    c.delete_coip_cidr()
        .cidr("10.0.0.0/24")
        .coip_pool_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetCoipPoolUsage", checksum = "959a2c31")]
#[tokio::test]
async fn ec2_get_coip_pool_usage() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_coip_pool_usage()
        .pool_id("ipv4pool-coip-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.coip_pool_id(), Some("ipv4pool-coip-1"));
}

#[test_action("ec2", "CreateLocalGatewayRouteTable", checksum = "aefa7f05")]
#[tokio::test]
async fn ec2_create_local_gateway_route_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_lgrt(&c).await;
    assert!(id.starts_with("lgw-rtb-"));
}

#[test_action("ec2", "DescribeLocalGatewayRouteTables", checksum = "b3d081da")]
#[tokio::test]
async fn ec2_describe_local_gateway_route_tables() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_lgrt(&c).await;
    let r = c
        .describe_local_gateway_route_tables()
        .send()
        .await
        .unwrap();
    assert!(!r.local_gateway_route_tables().is_empty());
}

#[test_action("ec2", "DeleteLocalGatewayRouteTable", checksum = "ad6b3025")]
#[tokio::test]
async fn ec2_delete_local_gateway_route_table() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_lgrt(&c).await;
    c.delete_local_gateway_route_table()
        .local_gateway_route_table_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateLocalGatewayRoute", checksum = "37808a0c")]
#[tokio::test]
async fn ec2_create_local_gateway_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rt = make_lgrt(&c).await;
    let r = c
        .create_local_gateway_route()
        .local_gateway_route_table_id(&rt)
        .destination_cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    assert!(r.route().is_some());
}

#[test_action("ec2", "DeleteLocalGatewayRoute", checksum = "da5e2d52")]
#[tokio::test]
async fn ec2_delete_local_gateway_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rt = make_lgrt(&c).await;
    c.create_local_gateway_route()
        .local_gateway_route_table_id(&rt)
        .destination_cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    let r = c
        .delete_local_gateway_route()
        .local_gateway_route_table_id(&rt)
        .destination_cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    assert!(r.route().is_some());
}

#[test_action("ec2", "ModifyLocalGatewayRoute", checksum = "0b8a7aa2")]
#[tokio::test]
async fn ec2_modify_local_gateway_route() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rt = make_lgrt(&c).await;
    let r = c
        .modify_local_gateway_route()
        .local_gateway_route_table_id(&rt)
        .destination_cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    assert!(r.route().is_some());
}

#[test_action("ec2", "SearchLocalGatewayRoutes", checksum = "1553e86c")]
#[tokio::test]
async fn ec2_search_local_gateway_routes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rt = make_lgrt(&c).await;
    c.create_local_gateway_route()
        .local_gateway_route_table_id(&rt)
        .destination_cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    let r = c
        .search_local_gateway_routes()
        .local_gateway_route_table_id(&rt)
        .send()
        .await
        .unwrap();
    assert!(!r.routes().is_empty());
}

#[test_action(
    "ec2",
    "CreateLocalGatewayRouteTableVpcAssociation",
    checksum = "d60005c1"
)]
#[tokio::test]
async fn ec2_create_local_gateway_route_table_vpc_association() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rt = make_lgrt(&c).await;
    let r = c
        .create_local_gateway_route_table_vpc_association()
        .local_gateway_route_table_id(&rt)
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    assert!(r.local_gateway_route_table_vpc_association().is_some());
}

#[test_action(
    "ec2",
    "DescribeLocalGatewayRouteTableVpcAssociations",
    checksum = "2cbe006a"
)]
#[tokio::test]
async fn ec2_describe_local_gateway_route_table_vpc_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rt = make_lgrt(&c).await;
    c.create_local_gateway_route_table_vpc_association()
        .local_gateway_route_table_id(&rt)
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap();
    let r = c
        .describe_local_gateway_route_table_vpc_associations()
        .send()
        .await
        .unwrap();
    assert!(!r.local_gateway_route_table_vpc_associations().is_empty());
}

#[test_action(
    "ec2",
    "DeleteLocalGatewayRouteTableVpcAssociation",
    checksum = "9c2b6b3c"
)]
#[tokio::test]
async fn ec2_delete_local_gateway_route_table_vpc_association() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rt = make_lgrt(&c).await;
    let id = c
        .create_local_gateway_route_table_vpc_association()
        .local_gateway_route_table_id(&rt)
        .vpc_id("vpc-1")
        .send()
        .await
        .unwrap()
        .local_gateway_route_table_vpc_association()
        .unwrap()
        .local_gateway_route_table_vpc_association_id()
        .unwrap()
        .to_string();
    c.delete_local_gateway_route_table_vpc_association()
        .local_gateway_route_table_vpc_association_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateLocalGatewayVirtualInterface", checksum = "fe0cd2d7")]
#[tokio::test]
async fn ec2_create_local_gateway_virtual_interface() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let g = make_vifg(&c).await;
    let r = c
        .create_local_gateway_virtual_interface()
        .local_gateway_virtual_interface_group_id(&g)
        .outpost_lag_id("ola-1")
        .vlan(100)
        .local_address("10.0.0.1")
        .peer_address("10.0.0.2")
        .send()
        .await
        .unwrap();
    assert!(r.local_gateway_virtual_interface().is_some());
}

#[test_action("ec2", "DescribeLocalGatewayVirtualInterfaces", checksum = "cab4e48c")]
#[tokio::test]
async fn ec2_describe_local_gateway_virtual_interfaces() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let g = make_vifg(&c).await;
    c.create_local_gateway_virtual_interface()
        .local_gateway_virtual_interface_group_id(&g)
        .outpost_lag_id("ola-1")
        .vlan(100)
        .local_address("10.0.0.1")
        .peer_address("10.0.0.2")
        .send()
        .await
        .unwrap();
    let r = c
        .describe_local_gateway_virtual_interfaces()
        .send()
        .await
        .unwrap();
    assert!(!r.local_gateway_virtual_interfaces().is_empty());
}

#[test_action("ec2", "DeleteLocalGatewayVirtualInterface", checksum = "ccfd27a9")]
#[tokio::test]
async fn ec2_delete_local_gateway_virtual_interface() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let g = make_vifg(&c).await;
    let id = c
        .create_local_gateway_virtual_interface()
        .local_gateway_virtual_interface_group_id(&g)
        .outpost_lag_id("ola-1")
        .vlan(100)
        .local_address("10.0.0.1")
        .peer_address("10.0.0.2")
        .send()
        .await
        .unwrap()
        .local_gateway_virtual_interface()
        .unwrap()
        .local_gateway_virtual_interface_id()
        .unwrap()
        .to_string();
    c.delete_local_gateway_virtual_interface()
        .local_gateway_virtual_interface_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "CreateLocalGatewayVirtualInterfaceGroup",
    checksum = "797074d8"
)]
#[tokio::test]
async fn ec2_create_local_gateway_virtual_interface_group() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vifg(&c).await;
    assert!(id.starts_with("lgw-vif-grp-"));
}

#[test_action(
    "ec2",
    "DescribeLocalGatewayVirtualInterfaceGroups",
    checksum = "d8875d7a"
)]
#[tokio::test]
async fn ec2_describe_local_gateway_virtual_interface_groups() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_vifg(&c).await;
    let r = c
        .describe_local_gateway_virtual_interface_groups()
        .send()
        .await
        .unwrap();
    assert!(!r.local_gateway_virtual_interface_groups().is_empty());
}

#[test_action(
    "ec2",
    "DeleteLocalGatewayVirtualInterfaceGroup",
    checksum = "a6e75ff3"
)]
#[tokio::test]
async fn ec2_delete_local_gateway_virtual_interface_group() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_vifg(&c).await;
    c.delete_local_gateway_virtual_interface_group()
        .local_gateway_virtual_interface_group_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation",
    checksum = "db7084d2"
)]
#[tokio::test]
async fn ec2_create_local_gateway_route_table_virtual_interface_group_association() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rt = make_lgrt(&c).await;
    let g = make_vifg(&c).await;
    let r = c
        .create_local_gateway_route_table_virtual_interface_group_association()
        .local_gateway_route_table_id(&rt)
        .local_gateway_virtual_interface_group_id(&g)
        .send()
        .await
        .unwrap();
    assert!(r
        .local_gateway_route_table_virtual_interface_group_association()
        .is_some());
}

#[test_action(
    "ec2",
    "DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations",
    checksum = "5917423b"
)]
#[tokio::test]
async fn ec2_describe_local_gateway_route_table_virtual_interface_group_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rt = make_lgrt(&c).await;
    let g = make_vifg(&c).await;
    c.create_local_gateway_route_table_virtual_interface_group_association()
        .local_gateway_route_table_id(&rt)
        .local_gateway_virtual_interface_group_id(&g)
        .send()
        .await
        .unwrap();
    let r = c
        .describe_local_gateway_route_table_virtual_interface_group_associations()
        .send()
        .await
        .unwrap();
    assert!(!r
        .local_gateway_route_table_virtual_interface_group_associations()
        .is_empty());
}

#[test_action(
    "ec2",
    "DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation",
    checksum = "41436d48"
)]
#[tokio::test]
async fn ec2_delete_local_gateway_route_table_virtual_interface_group_association() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let rt = make_lgrt(&c).await;
    let g = make_vifg(&c).await;
    let id = c
        .create_local_gateway_route_table_virtual_interface_group_association()
        .local_gateway_route_table_id(&rt)
        .local_gateway_virtual_interface_group_id(&g)
        .send()
        .await
        .unwrap()
        .local_gateway_route_table_virtual_interface_group_association()
        .unwrap()
        .local_gateway_route_table_virtual_interface_group_association_id()
        .unwrap()
        .to_string();
    c.delete_local_gateway_route_table_virtual_interface_group_association()
        .local_gateway_route_table_virtual_interface_group_association_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeLocalGateways", checksum = "56a8f2fa")]
#[tokio::test]
async fn ec2_describe_local_gateways() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.describe_local_gateways().send().await.unwrap();
    assert!(r.local_gateways().is_empty());
}

// ---- instance connect / fast launch / serial console ----

async fn make_ice(c: &aws_sdk_ec2::Client) -> String {
    c.create_instance_connect_endpoint()
        .subnet_id("subnet-1")
        .send()
        .await
        .unwrap()
        .instance_connect_endpoint()
        .unwrap()
        .instance_connect_endpoint_id()
        .unwrap()
        .to_string()
}

#[test_action("ec2", "CreateInstanceConnectEndpoint", checksum = "8d75bdcd")]
#[tokio::test]
async fn ec2_create_instance_connect_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ice(&c).await;
    assert!(id.starts_with("eice-"));
}

#[test_action("ec2", "DescribeInstanceConnectEndpoints", checksum = "0739217e")]
#[tokio::test]
async fn ec2_describe_instance_connect_endpoints() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    make_ice(&c).await;
    let r = c
        .describe_instance_connect_endpoints()
        .send()
        .await
        .unwrap();
    assert!(!r.instance_connect_endpoints().is_empty());
}

#[test_action("ec2", "ModifyInstanceConnectEndpoint", checksum = "d396bb49")]
#[tokio::test]
async fn ec2_modify_instance_connect_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ice(&c).await;
    // Response carries only a <return> boolean.
    c.modify_instance_connect_endpoint()
        .instance_connect_endpoint_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteInstanceConnectEndpoint", checksum = "c439d3c1")]
#[tokio::test]
async fn ec2_delete_instance_connect_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ice(&c).await;
    c.delete_instance_connect_endpoint()
        .instance_connect_endpoint_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableFastLaunch", checksum = "f4018703")]
#[tokio::test]
async fn ec2_enable_fast_launch() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .enable_fast_launch()
        .image_id("ami-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.image_id(), Some("ami-1"));
}

#[test_action("ec2", "DisableFastLaunch", checksum = "0b6d25a1")]
#[tokio::test]
async fn ec2_disable_fast_launch() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.enable_fast_launch()
        .image_id("ami-1")
        .send()
        .await
        .unwrap();
    let r = c
        .disable_fast_launch()
        .image_id("ami-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.image_id(), Some("ami-1"));
}

#[test_action("ec2", "EnableSerialConsoleAccess", checksum = "9f87f3ee")]
#[tokio::test]
async fn ec2_enable_serial_console_access() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.enable_serial_console_access().send().await.unwrap();
    assert_eq!(r.serial_console_access_enabled(), Some(true));
}

#[test_action("ec2", "DisableSerialConsoleAccess", checksum = "cf2f876d")]
#[tokio::test]
async fn ec2_disable_serial_console_access() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c.disable_serial_console_access().send().await.unwrap();
    assert_eq!(r.serial_console_access_enabled(), Some(false));
}

#[test_action("ec2", "GetSerialConsoleAccessStatus", checksum = "f6c43e91")]
#[tokio::test]
async fn ec2_get_serial_console_access_status() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.enable_serial_console_access().send().await.unwrap();
    let r = c.get_serial_console_access_status().send().await.unwrap();
    assert_eq!(r.serial_console_access_enabled(), Some(true));
}

#[test_action("ec2", "GetConsoleOutput", checksum = "2d4ba879")]
#[tokio::test]
async fn ec2_get_console_output() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_console_output()
        .instance_id("i-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.instance_id(), Some("i-1"));
}

#[test_action("ec2", "GetConsoleScreenshot", checksum = "d6fd7694")]
#[tokio::test]
async fn ec2_get_console_screenshot() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_console_screenshot()
        .instance_id("i-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.instance_id(), Some("i-1"));
}

#[test_action("ec2", "GetPasswordData", checksum = "8352ee62")]
#[tokio::test]
async fn ec2_get_password_data() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let r = c
        .get_password_data()
        .instance_id("i-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.instance_id(), Some("i-1"));
}

// ---- remainder sweep ----
#[test_action("ec2", "AdvertiseByoipCidr", checksum = "6182b651")]
#[tokio::test]
async fn ec2_advertise_byoip_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.advertise_byoip_cidr().cidr("x").send().await.unwrap();
}

#[test_action("ec2", "AssociateEnclaveCertificateIamRole", checksum = "25562e2e")]
#[tokio::test]
async fn ec2_associate_enclave_certificate_iam_role() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.associate_enclave_certificate_iam_role()
        .certificate_arn("x")
        .role_arn("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AssociateIamInstanceProfile", checksum = "c5a5d3c8")]
#[tokio::test]
async fn ec2_associate_iam_instance_profile() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.associate_iam_instance_profile()
        .instance_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AssociateInstanceEventWindow", checksum = "3cf2dabd")]
#[tokio::test]
async fn ec2_associate_instance_event_window() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.associate_instance_event_window()
        .instance_event_window_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AssociateRouteServer", checksum = "d453ae0e")]
#[tokio::test]
async fn ec2_associate_route_server() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.associate_route_server()
        .route_server_id("x")
        .vpc_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AssociateTrunkInterface", checksum = "66bd6437")]
#[tokio::test]
async fn ec2_associate_trunk_interface() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.associate_trunk_interface()
        .branch_interface_id("x")
        .trunk_interface_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "AttachClassicLinkVpc", checksum = "def79306")]
#[tokio::test]
async fn ec2_attach_classic_link_vpc() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.attach_classic_link_vpc()
        .instance_id("x")
        .vpc_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "BundleInstance", checksum = "769d9852")]
#[tokio::test]
async fn ec2_bundle_instance() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.bundle_instance().instance_id("x").send().await.unwrap();
}

#[test_action("ec2", "CancelBundleTask", checksum = "17e14890")]
#[tokio::test]
async fn ec2_cancel_bundle_task() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.cancel_bundle_task().bundle_id("x").send().await.unwrap();
}

#[test_action("ec2", "CancelConversionTask", checksum = "f3946f87")]
#[tokio::test]
async fn ec2_cancel_conversion_task() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.cancel_conversion_task()
        .conversion_task_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CancelDeclarativePoliciesReport", checksum = "806a63e2")]
#[tokio::test]
async fn ec2_cancel_declarative_policies_report() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.cancel_declarative_policies_report()
        .report_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CancelExportTask", checksum = "ced63f63")]
#[tokio::test]
async fn ec2_cancel_export_task() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.cancel_export_task()
        .export_task_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CancelImportTask", checksum = "e0deffdf")]
#[tokio::test]
async fn ec2_cancel_import_task() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.cancel_import_task().send().await.unwrap();
}

#[test_action("ec2", "ConfirmProductInstance", checksum = "ad04610f")]
#[tokio::test]
async fn ec2_confirm_product_instance() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.confirm_product_instance()
        .instance_id("x")
        .product_code("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CopyFpgaImage", checksum = "892d8cf1")]
#[tokio::test]
async fn ec2_copy_fpga_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.copy_fpga_image()
        .source_fpga_image_id("x")
        .source_region("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CopyVolumes", checksum = "b7d12bb9")]
#[tokio::test]
async fn ec2_copy_volumes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.copy_volumes().source_volume_id("x").send().await.unwrap();
}

#[test_action("ec2", "CreateCapacityManagerDataExport", checksum = "5bc1d840")]
#[tokio::test]
async fn ec2_create_capacity_manager_data_export() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_capacity_manager_data_export()
        .s3_bucket_name("x")
        .schedule(aws_sdk_ec2::types::Schedule::Hourly)
        .output_format(aws_sdk_ec2::types::OutputFormat::Csv)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateDelegateMacVolumeOwnershipTask", checksum = "bb874277")]
#[tokio::test]
async fn ec2_create_delegate_mac_volume_ownership_task() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_delegate_mac_volume_ownership_task()
        .instance_id("x")
        .mac_credentials("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateFpgaImage", checksum = "745b65b9")]
#[tokio::test]
async fn ec2_create_fpga_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_fpga_image().send().await.unwrap();
}

#[test_action("ec2", "CreateImageUsageReport", checksum = "27fd3c86")]
#[tokio::test]
async fn ec2_create_image_usage_report() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_image_usage_report()
        .image_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateInstanceEventWindow", checksum = "9153b568")]
#[tokio::test]
async fn ec2_create_instance_event_window() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_instance_event_window().send().await.unwrap();
}

#[test_action("ec2", "CreateInstanceExportTask", checksum = "3dcf91ae")]
#[tokio::test]
async fn ec2_create_instance_export_task() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_instance_export_task()
        .instance_id("x")
        .target_environment(aws_sdk_ec2::types::ExportEnvironment::Citrix)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "CreateMacSystemIntegrityProtectionModificationTask",
    checksum = "95b0a29c"
)]
#[tokio::test]
async fn ec2_create_mac_system_integrity_protection_modification_task() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_mac_system_integrity_protection_modification_task()
        .instance_id("x")
        .mac_system_integrity_protection_status(
            aws_sdk_ec2::types::MacSystemIntegrityProtectionSettingStatus::Enabled,
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateManagedPrefixList", checksum = "fcdd6c4a")]
#[tokio::test]
async fn ec2_create_managed_prefix_list() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_managed_prefix_list()
        .prefix_list_name("x")
        .max_entries(1)
        .address_family("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreatePublicIpv4Pool", checksum = "102bce7b")]
#[tokio::test]
async fn ec2_create_public_ipv4_pool() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_public_ipv4_pool().send().await.unwrap();
}

#[test_action("ec2", "CreateReplaceRootVolumeTask", checksum = "b6653727")]
#[tokio::test]
async fn ec2_create_replace_root_volume_task() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_replace_root_volume_task()
        .instance_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateRouteServer", checksum = "66197c80")]
#[tokio::test]
async fn ec2_create_route_server() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_route_server()
        .amazon_side_asn(1)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateRouteServerEndpoint", checksum = "57e78f86")]
#[tokio::test]
async fn ec2_create_route_server_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_route_server_endpoint()
        .route_server_id("x")
        .subnet_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateRouteServerPeer", checksum = "58233885")]
#[tokio::test]
async fn ec2_create_route_server_peer() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_route_server_peer()
        .route_server_endpoint_id("x")
        .peer_address("x")
        .bgp_options(
            aws_sdk_ec2::types::RouteServerBgpOptionsRequest::builder()
                .peer_asn(1)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateSecondaryNetwork", checksum = "28c1a44a")]
#[tokio::test]
async fn ec2_create_secondary_network() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_secondary_network()
        .ipv4_cidr_block("x")
        .network_type(aws_sdk_ec2::types::SecondaryNetworkType::Rdma)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateTrafficMirrorFilter", checksum = "29878f45")]
#[tokio::test]
async fn ec2_create_traffic_mirror_filter() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_traffic_mirror_filter().send().await.unwrap();
}

#[test_action("ec2", "CreateTrafficMirrorFilterRule", checksum = "21144e98")]
#[tokio::test]
async fn ec2_create_traffic_mirror_filter_rule() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_traffic_mirror_filter_rule()
        .traffic_mirror_filter_id("x")
        .traffic_direction(aws_sdk_ec2::types::TrafficDirection::Ingress)
        .rule_number(1)
        .rule_action(aws_sdk_ec2::types::TrafficMirrorRuleAction::Accept)
        .destination_cidr_block("x")
        .source_cidr_block("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateTrafficMirrorSession", checksum = "95d18892")]
#[tokio::test]
async fn ec2_create_traffic_mirror_session() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_traffic_mirror_session()
        .network_interface_id("x")
        .traffic_mirror_target_id("x")
        .traffic_mirror_filter_id("x")
        .session_number(1)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateTrafficMirrorTarget", checksum = "abbf8faa")]
#[tokio::test]
async fn ec2_create_traffic_mirror_target() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_traffic_mirror_target().send().await.unwrap();
}

#[test_action("ec2", "CreateVpcBlockPublicAccessExclusion", checksum = "09dff4a8")]
#[tokio::test]
async fn ec2_create_vpc_block_public_access_exclusion() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_vpc_block_public_access_exclusion()
        .internet_gateway_exclusion_mode(
            aws_sdk_ec2::types::InternetGatewayExclusionMode::AllowBidirectional,
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "CreateVpcEncryptionControl", checksum = "b21408b9")]
#[tokio::test]
async fn ec2_create_vpc_encryption_control() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.create_vpc_encryption_control()
        .vpc_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteCapacityManagerDataExport", checksum = "cd77b480")]
#[tokio::test]
async fn ec2_delete_capacity_manager_data_export() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_capacity_manager_data_export()
        .capacity_manager_data_export_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteFpgaImage", checksum = "66e2e3a7")]
#[tokio::test]
async fn ec2_delete_fpga_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_fpga_image()
        .fpga_image_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteImageUsageReport", checksum = "5256f1cc")]
#[tokio::test]
async fn ec2_delete_image_usage_report() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_image_usage_report()
        .report_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteInstanceEventWindow", checksum = "0d66407b")]
#[tokio::test]
async fn ec2_delete_instance_event_window() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_instance_event_window()
        .instance_event_window_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteManagedPrefixList", checksum = "1fd8f360")]
#[tokio::test]
async fn ec2_delete_managed_prefix_list() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_managed_prefix_list()
        .prefix_list_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeletePublicIpv4Pool", checksum = "b9bd4867")]
#[tokio::test]
async fn ec2_delete_public_ipv4_pool() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_public_ipv4_pool()
        .pool_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteRouteServer", checksum = "2b89acfb")]
#[tokio::test]
async fn ec2_delete_route_server() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_route_server()
        .route_server_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteRouteServerEndpoint", checksum = "188ad9b2")]
#[tokio::test]
async fn ec2_delete_route_server_endpoint() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_route_server_endpoint()
        .route_server_endpoint_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteRouteServerPeer", checksum = "f3f61607")]
#[tokio::test]
async fn ec2_delete_route_server_peer() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_route_server_peer()
        .route_server_peer_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteSecondaryNetwork", checksum = "987d3c9b")]
#[tokio::test]
async fn ec2_delete_secondary_network() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_secondary_network()
        .secondary_network_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteTrafficMirrorFilter", checksum = "93caac5b")]
#[tokio::test]
async fn ec2_delete_traffic_mirror_filter() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_traffic_mirror_filter()
        .traffic_mirror_filter_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteTrafficMirrorFilterRule", checksum = "b776bcd3")]
#[tokio::test]
async fn ec2_delete_traffic_mirror_filter_rule() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_traffic_mirror_filter_rule()
        .traffic_mirror_filter_rule_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteTrafficMirrorSession", checksum = "0b2a231c")]
#[tokio::test]
async fn ec2_delete_traffic_mirror_session() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_traffic_mirror_session()
        .traffic_mirror_session_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteTrafficMirrorTarget", checksum = "78b64f5e")]
#[tokio::test]
async fn ec2_delete_traffic_mirror_target() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_traffic_mirror_target()
        .traffic_mirror_target_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteVpcBlockPublicAccessExclusion", checksum = "af7d7573")]
#[tokio::test]
async fn ec2_delete_vpc_block_public_access_exclusion() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_vpc_block_public_access_exclusion()
        .exclusion_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeleteVpcEncryptionControl", checksum = "da644903")]
#[tokio::test]
async fn ec2_delete_vpc_encryption_control() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.delete_vpc_encryption_control()
        .vpc_encryption_control_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DeprovisionByoipCidr", checksum = "640626b7")]
#[tokio::test]
async fn ec2_deprovision_byoip_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.deprovision_byoip_cidr().cidr("x").send().await.unwrap();
}

#[test_action("ec2", "DeprovisionPublicIpv4PoolCidr", checksum = "533cf1e5")]
#[tokio::test]
async fn ec2_deprovision_public_ipv4_pool_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.deprovision_public_ipv4_pool_cidr()
        .pool_id("x")
        .cidr("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeAggregateIdFormat", checksum = "89cd5de7")]
#[tokio::test]
async fn ec2_describe_aggregate_id_format() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_aggregate_id_format().send().await.unwrap();
}

#[test_action(
    "ec2",
    "DescribeAwsNetworkPerformanceMetricSubscriptions",
    checksum = "de4aae1c"
)]
#[tokio::test]
async fn ec2_describe_aws_network_performance_metric_subscriptions() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_aws_network_performance_metric_subscriptions()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeBundleTasks", checksum = "7fe1331b")]
#[tokio::test]
async fn ec2_describe_bundle_tasks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_bundle_tasks().send().await.unwrap();
}

#[test_action("ec2", "DescribeByoipCidrs", checksum = "73b1d66d")]
#[tokio::test]
async fn ec2_describe_byoip_cidrs() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_byoip_cidrs()
        .max_results(1)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeCapacityManagerDataExports", checksum = "9bc62522")]
#[tokio::test]
async fn ec2_describe_capacity_manager_data_exports() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_capacity_manager_data_exports()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeClassicLinkInstances", checksum = "4bae0acb")]
#[tokio::test]
async fn ec2_describe_classic_link_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_classic_link_instances().send().await.unwrap();
}

#[test_action("ec2", "DescribeConversionTasks", checksum = "aea0eaad")]
#[tokio::test]
async fn ec2_describe_conversion_tasks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_conversion_tasks().send().await.unwrap();
}

#[test_action("ec2", "DescribeDeclarativePoliciesReports", checksum = "1a6a09da")]
#[tokio::test]
async fn ec2_describe_declarative_policies_reports() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_declarative_policies_reports()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeElasticGpus", checksum = "97abf37d")]
#[tokio::test]
async fn ec2_describe_elastic_gpus() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_elastic_gpus().send().await.unwrap();
}

#[test_action("ec2", "DescribeExportImageTasks", checksum = "fcd38203")]
#[tokio::test]
async fn ec2_describe_export_image_tasks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_export_image_tasks().send().await.unwrap();
}

#[test_action("ec2", "DescribeExportTasks", checksum = "3ae8becf")]
#[tokio::test]
async fn ec2_describe_export_tasks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_export_tasks().send().await.unwrap();
}

#[test_action("ec2", "DescribeFpgaImageAttribute", checksum = "25a324dc")]
#[tokio::test]
async fn ec2_describe_fpga_image_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_fpga_image_attribute()
        .fpga_image_id("x")
        .attribute(aws_sdk_ec2::types::FpgaImageAttributeName::Description)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeFpgaImages", checksum = "b3ad475b")]
#[tokio::test]
async fn ec2_describe_fpga_images() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_fpga_images().send().await.unwrap();
}

#[test_action("ec2", "DescribeHostReservationOfferings", checksum = "9fa55bfb")]
#[tokio::test]
async fn ec2_describe_host_reservation_offerings() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_host_reservation_offerings()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeHostReservations", checksum = "30515b05")]
#[tokio::test]
async fn ec2_describe_host_reservations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_host_reservations().send().await.unwrap();
}

#[test_action("ec2", "DescribeIamInstanceProfileAssociations", checksum = "a724f322")]
#[tokio::test]
async fn ec2_describe_iam_instance_profile_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_iam_instance_profile_associations()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeIdFormat", checksum = "950cd01b")]
#[tokio::test]
async fn ec2_describe_id_format() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_id_format().send().await.unwrap();
}

#[test_action("ec2", "DescribeIdentityIdFormat", checksum = "f7c930ad")]
#[tokio::test]
async fn ec2_describe_identity_id_format() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_identity_id_format()
        .principal_arn("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeImageReferences", checksum = "19b97976")]
#[tokio::test]
async fn ec2_describe_image_references() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_image_references().send().await.unwrap();
}

#[test_action("ec2", "DescribeImageUsageReportEntries", checksum = "d3a7bc8f")]
#[tokio::test]
async fn ec2_describe_image_usage_report_entries() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_image_usage_report_entries()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeImageUsageReports", checksum = "cb9f3f0f")]
#[tokio::test]
async fn ec2_describe_image_usage_reports() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_image_usage_reports().send().await.unwrap();
}

#[test_action("ec2", "DescribeImportImageTasks", checksum = "738035d7")]
#[tokio::test]
async fn ec2_describe_import_image_tasks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_import_image_tasks().send().await.unwrap();
}

#[test_action("ec2", "DescribeImportSnapshotTasks", checksum = "4fe30fd9")]
#[tokio::test]
async fn ec2_describe_import_snapshot_tasks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_import_snapshot_tasks().send().await.unwrap();
}

#[test_action("ec2", "DescribeInstanceEventWindows", checksum = "28630750")]
#[tokio::test]
async fn ec2_describe_instance_event_windows() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_instance_event_windows().send().await.unwrap();
}

#[test_action("ec2", "DescribeInstanceImageMetadata", checksum = "9e1e5cc9")]
#[tokio::test]
async fn ec2_describe_instance_image_metadata() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_instance_image_metadata().send().await.unwrap();
}

#[test_action("ec2", "DescribeInstanceSqlHaHistoryStates", checksum = "36fb33bc")]
#[tokio::test]
async fn ec2_describe_instance_sql_ha_history_states() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_instance_sql_ha_history_states()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeInstanceSqlHaStates", checksum = "df7bd206")]
#[tokio::test]
async fn ec2_describe_instance_sql_ha_states() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_instance_sql_ha_states().send().await.unwrap();
}

#[test_action("ec2", "DescribeInstanceTypeOfferings", checksum = "6b166aa7")]
#[tokio::test]
async fn ec2_describe_instance_type_offerings() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_instance_type_offerings().send().await.unwrap();
}

#[test_action("ec2", "DescribeIpv6Pools", checksum = "cc6019ce")]
#[tokio::test]
async fn ec2_describe_ipv6_pools() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_ipv6_pools().send().await.unwrap();
}

#[test_action("ec2", "DescribeMacModificationTasks", checksum = "67c27a82")]
#[tokio::test]
async fn ec2_describe_mac_modification_tasks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_mac_modification_tasks().send().await.unwrap();
}

#[test_action("ec2", "DescribeManagedPrefixLists", checksum = "76f1804a")]
#[tokio::test]
async fn ec2_describe_managed_prefix_lists() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_managed_prefix_lists().send().await.unwrap();
}

#[test_action("ec2", "DescribeOutpostLags", checksum = "b581387b")]
#[tokio::test]
async fn ec2_describe_outpost_lags() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_outpost_lags().send().await.unwrap();
}

#[test_action("ec2", "DescribePrefixLists", checksum = "ac5525ca")]
#[tokio::test]
async fn ec2_describe_prefix_lists() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_prefix_lists().send().await.unwrap();
}

#[test_action("ec2", "DescribePrincipalIdFormat", checksum = "9f0dd98f")]
#[tokio::test]
async fn ec2_describe_principal_id_format() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_principal_id_format().send().await.unwrap();
}

#[test_action("ec2", "DescribePublicIpv4Pools", checksum = "3afc4108")]
#[tokio::test]
async fn ec2_describe_public_ipv4_pools() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_public_ipv4_pools().send().await.unwrap();
}

#[test_action("ec2", "DescribeReplaceRootVolumeTasks", checksum = "c4c0014b")]
#[tokio::test]
async fn ec2_describe_replace_root_volume_tasks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_replace_root_volume_tasks().send().await.unwrap();
}

#[test_action("ec2", "DescribeRouteServerEndpoints", checksum = "69b2ceb4")]
#[tokio::test]
async fn ec2_describe_route_server_endpoints() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_route_server_endpoints().send().await.unwrap();
}

#[test_action("ec2", "DescribeRouteServerPeers", checksum = "33deecf1")]
#[tokio::test]
async fn ec2_describe_route_server_peers() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_route_server_peers().send().await.unwrap();
}

#[test_action("ec2", "DescribeRouteServers", checksum = "fa121dbe")]
#[tokio::test]
async fn ec2_describe_route_servers() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_route_servers().send().await.unwrap();
}

#[test_action("ec2", "DescribeScheduledInstanceAvailability", checksum = "7946151a")]
#[tokio::test]
async fn ec2_describe_scheduled_instance_availability() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_scheduled_instance_availability()
        .first_slot_start_time_range(
            aws_sdk_ec2::types::SlotDateTimeRangeRequest::builder()
                .earliest_time(aws_smithy_types::DateTime::from_secs(0))
                .latest_time(aws_smithy_types::DateTime::from_secs(0))
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeScheduledInstances", checksum = "c2ac7c78")]
#[tokio::test]
async fn ec2_describe_scheduled_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_scheduled_instances().send().await.unwrap();
}

#[test_action("ec2", "DescribeSecondaryInterfaces", checksum = "34d4b385")]
#[tokio::test]
async fn ec2_describe_secondary_interfaces() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_secondary_interfaces().send().await.unwrap();
}

#[test_action("ec2", "DescribeSecondaryNetworks", checksum = "0c80cba7")]
#[tokio::test]
async fn ec2_describe_secondary_networks() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_secondary_networks().send().await.unwrap();
}

#[test_action("ec2", "DescribeServiceLinkVirtualInterfaces", checksum = "96722db9")]
#[tokio::test]
async fn ec2_describe_service_link_virtual_interfaces() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_service_link_virtual_interfaces()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeTrafficMirrorFilterRules", checksum = "80007238")]
#[tokio::test]
async fn ec2_describe_traffic_mirror_filter_rules() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_traffic_mirror_filter_rules()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeTrafficMirrorFilters", checksum = "7ea22a7c")]
#[tokio::test]
async fn ec2_describe_traffic_mirror_filters() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_traffic_mirror_filters().send().await.unwrap();
}

#[test_action("ec2", "DescribeTrafficMirrorSessions", checksum = "b371615b")]
#[tokio::test]
async fn ec2_describe_traffic_mirror_sessions() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_traffic_mirror_sessions().send().await.unwrap();
}

#[test_action("ec2", "DescribeTrafficMirrorTargets", checksum = "ac39083a")]
#[tokio::test]
async fn ec2_describe_traffic_mirror_targets() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_traffic_mirror_targets().send().await.unwrap();
}

#[test_action("ec2", "DescribeTrunkInterfaceAssociations", checksum = "6b6c852d")]
#[tokio::test]
async fn ec2_describe_trunk_interface_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_trunk_interface_associations()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeVpcBlockPublicAccessExclusions", checksum = "46d6407c")]
#[tokio::test]
async fn ec2_describe_vpc_block_public_access_exclusions() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_vpc_block_public_access_exclusions()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeVpcBlockPublicAccessOptions", checksum = "b1bf4c7b")]
#[tokio::test]
async fn ec2_describe_vpc_block_public_access_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_vpc_block_public_access_options()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeVpcClassicLink", checksum = "f7fb8639")]
#[tokio::test]
async fn ec2_describe_vpc_classic_link() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_vpc_classic_link().send().await.unwrap();
}

#[test_action("ec2", "DescribeVpcClassicLinkDnsSupport", checksum = "eedb5253")]
#[tokio::test]
async fn ec2_describe_vpc_classic_link_dns_support() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_vpc_classic_link_dns_support()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DescribeVpcEncryptionControls", checksum = "b81720a1")]
#[tokio::test]
async fn ec2_describe_vpc_encryption_controls() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.describe_vpc_encryption_controls().send().await.unwrap();
}

#[test_action("ec2", "DetachClassicLinkVpc", checksum = "20413b44")]
#[tokio::test]
async fn ec2_detach_classic_link_vpc() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.detach_classic_link_vpc()
        .instance_id("x")
        .vpc_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "DisableAwsNetworkPerformanceMetricSubscription",
    checksum = "7889a881"
)]
#[tokio::test]
async fn ec2_disable_aws_network_performance_metric_subscription() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disable_aws_network_performance_metric_subscription()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisableCapacityManager", checksum = "acc948cc")]
#[tokio::test]
async fn ec2_disable_capacity_manager() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disable_capacity_manager().send().await.unwrap();
}

#[test_action("ec2", "DisableInstanceSqlHaStandbyDetections", checksum = "c3e03d22")]
#[tokio::test]
async fn ec2_disable_instance_sql_ha_standby_detections() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disable_instance_sql_ha_standby_detections()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisableRouteServerPropagation", checksum = "f4c91c52")]
#[tokio::test]
async fn ec2_disable_route_server_propagation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disable_route_server_propagation()
        .route_server_id("x")
        .route_table_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisableVgwRoutePropagation", checksum = "3bea74e1")]
#[tokio::test]
async fn ec2_disable_vgw_route_propagation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disable_vgw_route_propagation()
        .gateway_id("x")
        .route_table_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisableVpcClassicLink", checksum = "bf93e7cf")]
#[tokio::test]
async fn ec2_disable_vpc_classic_link() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disable_vpc_classic_link()
        .vpc_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisableVpcClassicLinkDnsSupport", checksum = "05af5ce6")]
#[tokio::test]
async fn ec2_disable_vpc_classic_link_dns_support() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disable_vpc_classic_link_dns_support()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisassociateEnclaveCertificateIamRole", checksum = "9b7b551d")]
#[tokio::test]
async fn ec2_disassociate_enclave_certificate_iam_role() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disassociate_enclave_certificate_iam_role()
        .certificate_arn("x")
        .role_arn("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisassociateIamInstanceProfile", checksum = "865568f3")]
#[tokio::test]
async fn ec2_disassociate_iam_instance_profile() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disassociate_iam_instance_profile()
        .association_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisassociateInstanceEventWindow", checksum = "03fcfa72")]
#[tokio::test]
async fn ec2_disassociate_instance_event_window() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disassociate_instance_event_window()
        .instance_event_window_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisassociateRouteServer", checksum = "88af6294")]
#[tokio::test]
async fn ec2_disassociate_route_server() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disassociate_route_server()
        .route_server_id("x")
        .vpc_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "DisassociateTrunkInterface", checksum = "82efb571")]
#[tokio::test]
async fn ec2_disassociate_trunk_interface() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.disassociate_trunk_interface()
        .association_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "EnableAwsNetworkPerformanceMetricSubscription",
    checksum = "6fcbc673"
)]
#[tokio::test]
async fn ec2_enable_aws_network_performance_metric_subscription() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.enable_aws_network_performance_metric_subscription()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableCapacityManager", checksum = "73d9094c")]
#[tokio::test]
async fn ec2_enable_capacity_manager() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.enable_capacity_manager().send().await.unwrap();
}

#[test_action("ec2", "EnableInstanceSqlHaStandbyDetections", checksum = "a36f4cd5")]
#[tokio::test]
async fn ec2_enable_instance_sql_ha_standby_detections() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.enable_instance_sql_ha_standby_detections()
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "EnableReachabilityAnalyzerOrganizationSharing",
    checksum = "b87aadf2"
)]
#[tokio::test]
async fn ec2_enable_reachability_analyzer_organization_sharing() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.enable_reachability_analyzer_organization_sharing()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableRouteServerPropagation", checksum = "c548fc1a")]
#[tokio::test]
async fn ec2_enable_route_server_propagation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.enable_route_server_propagation()
        .route_server_id("x")
        .route_table_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableVgwRoutePropagation", checksum = "7bf8e692")]
#[tokio::test]
async fn ec2_enable_vgw_route_propagation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.enable_vgw_route_propagation()
        .gateway_id("x")
        .route_table_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableVpcClassicLink", checksum = "cb70668b")]
#[tokio::test]
async fn ec2_enable_vpc_classic_link() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.enable_vpc_classic_link()
        .vpc_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "EnableVpcClassicLinkDnsSupport", checksum = "733c6237")]
#[tokio::test]
async fn ec2_enable_vpc_classic_link_dns_support() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.enable_vpc_classic_link_dns_support()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ExportImage", checksum = "936219fd")]
#[tokio::test]
async fn ec2_export_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.export_image()
        .disk_image_format(aws_sdk_ec2::types::DiskImageFormat::Vmdk)
        .image_id("x")
        .s3_export_location(
            aws_sdk_ec2::types::ExportTaskS3LocationRequest::builder()
                .s3_bucket("x")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "GetAssociatedEnclaveCertificateIamRoles",
    checksum = "eaee8632"
)]
#[tokio::test]
async fn ec2_get_associated_enclave_certificate_iam_roles() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_associated_enclave_certificate_iam_roles()
        .certificate_arn("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetAssociatedIpv6PoolCidrs", checksum = "efd0a8b7")]
#[tokio::test]
async fn ec2_get_associated_ipv6_pool_cidrs() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_associated_ipv6_pool_cidrs()
        .pool_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetAwsNetworkPerformanceData", checksum = "812cd275")]
#[tokio::test]
async fn ec2_get_aws_network_performance_data() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_aws_network_performance_data().send().await.unwrap();
}

#[test_action("ec2", "GetCapacityManagerAttributes", checksum = "d7bdbf51")]
#[tokio::test]
async fn ec2_get_capacity_manager_attributes() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_capacity_manager_attributes().send().await.unwrap();
}

#[test_action("ec2", "GetCapacityManagerMetricData", checksum = "b1c1f09d")]
#[tokio::test]
async fn ec2_get_capacity_manager_metric_data() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_capacity_manager_metric_data()
        .start_time(aws_smithy_types::DateTime::from_secs(0))
        .end_time(aws_smithy_types::DateTime::from_secs(0))
        .period(3600)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetCapacityManagerMetricDimensions", checksum = "c0952eea")]
#[tokio::test]
async fn ec2_get_capacity_manager_metric_dimensions() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_capacity_manager_metric_dimensions()
        .start_time(aws_smithy_types::DateTime::from_secs(0))
        .end_time(aws_smithy_types::DateTime::from_secs(0))
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetCapacityManagerMonitoredTagKeys", checksum = "d2dcae42")]
#[tokio::test]
async fn ec2_get_capacity_manager_monitored_tag_keys() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_capacity_manager_monitored_tag_keys()
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetDeclarativePoliciesReportSummary", checksum = "a98b4deb")]
#[tokio::test]
async fn ec2_get_declarative_policies_report_summary() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_declarative_policies_report_summary()
        .report_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetDefaultCreditSpecification", checksum = "7001ff01")]
#[tokio::test]
async fn ec2_get_default_credit_specification() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_default_credit_specification()
        .instance_family(aws_sdk_ec2::types::UnlimitedSupportedInstanceFamily::T2)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetHostReservationPurchasePreview", checksum = "0aef6ee8")]
#[tokio::test]
async fn ec2_get_host_reservation_purchase_preview() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_host_reservation_purchase_preview()
        .offering_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetImageAncestry", checksum = "8f1486e9")]
#[tokio::test]
async fn ec2_get_image_ancestry() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_image_ancestry().image_id("x").send().await.unwrap();
}

#[test_action("ec2", "GetInstanceTpmEkPub", checksum = "2a263592")]
#[tokio::test]
async fn ec2_get_instance_tpm_ek_pub() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_instance_tpm_ek_pub()
        .instance_id("x")
        .key_type(aws_sdk_ec2::types::EkPubKeyType::Rsa2048)
        .key_format(aws_sdk_ec2::types::EkPubKeyFormat::Der)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetInstanceUefiData", checksum = "320e3b0f")]
#[tokio::test]
async fn ec2_get_instance_uefi_data() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_instance_uefi_data()
        .instance_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetManagedPrefixListAssociations", checksum = "5b1c2bec")]
#[tokio::test]
async fn ec2_get_managed_prefix_list_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_managed_prefix_list_associations()
        .prefix_list_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetManagedPrefixListEntries", checksum = "b5cc54cf")]
#[tokio::test]
async fn ec2_get_managed_prefix_list_entries() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_managed_prefix_list_entries()
        .prefix_list_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetManagedResourceVisibility", checksum = "f443c6e7")]
#[tokio::test]
async fn ec2_get_managed_resource_visibility() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_managed_resource_visibility().send().await.unwrap();
}

#[test_action("ec2", "GetRouteServerAssociations", checksum = "c6547aff")]
#[tokio::test]
async fn ec2_get_route_server_associations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_route_server_associations()
        .route_server_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetRouteServerPropagations", checksum = "6be35948")]
#[tokio::test]
async fn ec2_get_route_server_propagations() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_route_server_propagations()
        .route_server_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "GetRouteServerRoutingDatabase", checksum = "ac68248a")]
#[tokio::test]
async fn ec2_get_route_server_routing_database() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_route_server_routing_database()
        .route_server_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "GetVpcResourcesBlockingEncryptionEnforcement",
    checksum = "33732a87"
)]
#[tokio::test]
async fn ec2_get_vpc_resources_blocking_encryption_enforcement() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.get_vpc_resources_blocking_encryption_enforcement()
        .vpc_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ImportImage", checksum = "5eb964dd")]
#[tokio::test]
async fn ec2_import_image() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.import_image().send().await.unwrap();
}

#[test_action("ec2", "ImportInstance", checksum = "ed6b6f7b")]
#[tokio::test]
async fn ec2_import_instance() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.import_instance()
        .platform(aws_sdk_ec2::types::PlatformValues::Windows)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ImportSnapshot", checksum = "8f0efb29")]
#[tokio::test]
async fn ec2_import_snapshot() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.import_snapshot().send().await.unwrap();
}

#[test_action("ec2", "ImportVolume", checksum = "3460627c")]
#[tokio::test]
async fn ec2_import_volume() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.import_volume()
        .image(
            aws_sdk_ec2::types::DiskImageDetail::builder()
                .format(aws_sdk_ec2::types::DiskImageFormat::Vmdk)
                .bytes(1)
                .import_manifest_url("x")
                .build(),
        )
        .volume(aws_sdk_ec2::types::VolumeDetail::builder().size(1).build())
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyAvailabilityZoneGroup", checksum = "2d21db89")]
#[tokio::test]
async fn ec2_modify_availability_zone_group() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_availability_zone_group()
        .group_name("x")
        .opt_in_status(aws_sdk_ec2::types::ModifyAvailabilityZoneOptInStatus::OptedIn)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyDefaultCreditSpecification", checksum = "0fb628a0")]
#[tokio::test]
async fn ec2_modify_default_credit_specification() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_default_credit_specification()
        .instance_family(aws_sdk_ec2::types::UnlimitedSupportedInstanceFamily::T2)
        .cpu_credits("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyFpgaImageAttribute", checksum = "f79fdbf3")]
#[tokio::test]
async fn ec2_modify_fpga_image_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_fpga_image_attribute()
        .fpga_image_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyIdFormat", checksum = "5e2c993c")]
#[tokio::test]
async fn ec2_modify_id_format() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_id_format()
        .resource("x")
        .use_long_ids(true)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyIdentityIdFormat", checksum = "d1063710")]
#[tokio::test]
async fn ec2_modify_identity_id_format() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_identity_id_format()
        .resource("x")
        .use_long_ids(true)
        .principal_arn("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyInstanceEventWindow", checksum = "05564f87")]
#[tokio::test]
async fn ec2_modify_instance_event_window() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_instance_event_window()
        .instance_event_window_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyManagedPrefixList", checksum = "10c810dd")]
#[tokio::test]
async fn ec2_modify_managed_prefix_list() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_managed_prefix_list()
        .prefix_list_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyManagedResourceVisibility", checksum = "3a5a3655")]
#[tokio::test]
async fn ec2_modify_managed_resource_visibility() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_managed_resource_visibility()
        .default_visibility(aws_sdk_ec2::types::ManagedResourceDefaultVisibility::Hidden)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyPrivateDnsNameOptions", checksum = "6bc02199")]
#[tokio::test]
async fn ec2_modify_private_dns_name_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_private_dns_name_options()
        .instance_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyPublicIpDnsNameOptions", checksum = "b2b52fff")]
#[tokio::test]
async fn ec2_modify_public_ip_dns_name_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_public_ip_dns_name_options()
        .network_interface_id("x")
        .hostname_type(aws_sdk_ec2::types::PublicIpDnsOption::PublicDualStackDnsName)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyRouteServer", checksum = "294b7012")]
#[tokio::test]
async fn ec2_modify_route_server() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_route_server()
        .route_server_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "ModifyTrafficMirrorFilterNetworkServices",
    checksum = "407f3984"
)]
#[tokio::test]
async fn ec2_modify_traffic_mirror_filter_network_services() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_traffic_mirror_filter_network_services()
        .traffic_mirror_filter_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyTrafficMirrorFilterRule", checksum = "d6942d6a")]
#[tokio::test]
async fn ec2_modify_traffic_mirror_filter_rule() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_traffic_mirror_filter_rule()
        .traffic_mirror_filter_rule_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyTrafficMirrorSession", checksum = "78777316")]
#[tokio::test]
async fn ec2_modify_traffic_mirror_session() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_traffic_mirror_session()
        .traffic_mirror_session_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyVpcBlockPublicAccessExclusion", checksum = "97c686e0")]
#[tokio::test]
async fn ec2_modify_vpc_block_public_access_exclusion() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_vpc_block_public_access_exclusion()
        .exclusion_id("x")
        .internet_gateway_exclusion_mode(
            aws_sdk_ec2::types::InternetGatewayExclusionMode::AllowBidirectional,
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyVpcBlockPublicAccessOptions", checksum = "2e39a9d0")]
#[tokio::test]
async fn ec2_modify_vpc_block_public_access_options() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_vpc_block_public_access_options()
        .internet_gateway_block_mode(aws_sdk_ec2::types::InternetGatewayBlockMode::Off)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ModifyVpcEncryptionControl", checksum = "6b1c12e5")]
#[tokio::test]
async fn ec2_modify_vpc_encryption_control() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.modify_vpc_encryption_control()
        .vpc_encryption_control_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ProvisionByoipCidr", checksum = "740650f7")]
#[tokio::test]
async fn ec2_provision_byoip_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.provision_byoip_cidr().cidr("x").send().await.unwrap();
}

#[test_action("ec2", "ProvisionPublicIpv4PoolCidr", checksum = "b230f9ed")]
#[tokio::test]
async fn ec2_provision_public_ipv4_pool_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.provision_public_ipv4_pool_cidr()
        .ipam_pool_id("x")
        .pool_id("x")
        .netmask_length(1)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "PurchaseHostReservation", checksum = "463a389d")]
#[tokio::test]
async fn ec2_purchase_host_reservation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.purchase_host_reservation()
        .offering_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "PurchaseScheduledInstances", checksum = "c75b873a")]
#[tokio::test]
async fn ec2_purchase_scheduled_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.purchase_scheduled_instances().send().await.unwrap();
}

#[test_action("ec2", "ReplaceIamInstanceProfileAssociation", checksum = "e7e7519e")]
#[tokio::test]
async fn ec2_replace_iam_instance_profile_association() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.replace_iam_instance_profile_association()
        .association_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "ResetFpgaImageAttribute", checksum = "b3bf8066")]
#[tokio::test]
async fn ec2_reset_fpga_image_attribute() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.reset_fpga_image_attribute()
        .fpga_image_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "RestoreManagedPrefixListVersion", checksum = "6d3ca4a6")]
#[tokio::test]
async fn ec2_restore_managed_prefix_list_version() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.restore_managed_prefix_list_version()
        .prefix_list_id("x")
        .previous_version(1)
        .current_version(1)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "RunScheduledInstances", checksum = "b1d413f4")]
#[tokio::test]
async fn ec2_run_scheduled_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.run_scheduled_instances()
        .scheduled_instance_id("x")
        .launch_specification(
            aws_sdk_ec2::types::ScheduledInstancesLaunchSpecification::builder()
                .image_id("x")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "SendDiagnosticInterrupt", checksum = "13c7645c")]
#[tokio::test]
async fn ec2_send_diagnostic_interrupt() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.send_diagnostic_interrupt()
        .instance_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "StartDeclarativePoliciesReport", checksum = "d130f2fd")]
#[tokio::test]
async fn ec2_start_declarative_policies_report() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.start_declarative_policies_report()
        .s3_bucket("x")
        .target_id("x")
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "UpdateCapacityManagerMonitoredTagKeys", checksum = "41035efb")]
#[tokio::test]
async fn ec2_update_capacity_manager_monitored_tag_keys() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.update_capacity_manager_monitored_tag_keys()
        .send()
        .await
        .unwrap();
}

#[test_action(
    "ec2",
    "UpdateCapacityManagerOrganizationsAccess",
    checksum = "2760ae77"
)]
#[tokio::test]
async fn ec2_update_capacity_manager_organizations_access() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.update_capacity_manager_organizations_access()
        .organizations_access(true)
        .send()
        .await
        .unwrap();
}

#[test_action("ec2", "WithdrawByoipCidr", checksum = "63483baa")]
#[tokio::test]
async fn ec2_withdraw_byoip_cidr() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    c.withdraw_byoip_cidr().cidr("x").send().await.unwrap();
}

// ---- ops absent from the vendored aws-sdk-ec2 client: exercised via a raw
// ec2Query POST so the audit + L1 probe still cover them. They graduate to
// typed SDK calls on the next SDK refresh. ----

const EC2_RAW_AUTH: &str =
    "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/ec2/aws4_request, SignedHeaders=host, Signature=0";

async fn ec2_raw(server: &TestServer, body: &str) -> reqwest::Response {
    let resp = reqwest::Client::new()
        .post(server.endpoint())
        .header("content-type", "application/x-www-form-urlencoded")
        .header("Authorization", EC2_RAW_AUTH)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "raw ec2 query returned {}",
        resp.status()
    );
    resp
}

#[test_action(
    "ec2",
    "CreateCapacityReservationCancellationQuote",
    checksum = "be8ec5c8"
)]
#[tokio::test]
async fn ec2_create_capacity_reservation_cancellation_quote() {
    let server = TestServer::start().await;
    ec2_raw(&server, "Action=CreateCapacityReservationCancellationQuote&Version=2016-11-15&CapacityReservationId=cr-1").await;
}

#[test_action(
    "ec2",
    "DescribeCapacityReservationCancellationQuotes",
    checksum = "c59b3611"
)]
#[tokio::test]
async fn ec2_describe_capacity_reservation_cancellation_quotes() {
    let server = TestServer::start().await;
    ec2_raw(
        &server,
        "Action=DescribeCapacityReservationCancellationQuotes&Version=2016-11-15",
    )
    .await;
}

#[test_action("ec2", "DescribeIpamPoolAllocations", checksum = "74fcc825")]
#[tokio::test]
async fn ec2_describe_ipam_pool_allocations() {
    let server = TestServer::start().await;
    ec2_raw(
        &server,
        "Action=DescribeIpamPoolAllocations&Version=2016-11-15",
    )
    .await;
}

#[test_action("ec2", "ModifyIpamPoolAllocation", checksum = "690cb39c")]
#[tokio::test]
async fn ec2_modify_ipam_pool_allocation() {
    let server = TestServer::start().await;
    ec2_raw(
        &server,
        "Action=ModifyIpamPoolAllocation&Version=2016-11-15&IpamPoolAllocationId=ipam-pool-alloc-1",
    )
    .await;
}

#[test_action("ec2", "DescribeAccountVpcEncryptionControl", checksum = "424c5aa4")]
#[tokio::test]
async fn ec2_describe_account_vpc_encryption_control() {
    let server = TestServer::start().await;
    let resp = ec2_raw(
        &server,
        "Action=DescribeAccountVpcEncryptionControl&Version=2016-11-15",
    )
    .await;
    let body = resp.text().await.unwrap();
    // Unset account reports the default unmanaged / default-state control.
    assert!(body.contains("<accountVpcEncryptionControl>"), "{body}");
    assert!(body.contains("<mode>unmanaged</mode>"), "{body}");
    assert!(body.contains("<state>default-state</state>"), "{body}");
}

#[test_action("ec2", "ModifyAccountVpcEncryptionControl", checksum = "a1e6d839")]
#[tokio::test]
async fn ec2_modify_account_vpc_encryption_control() {
    let server = TestServer::start().await;
    let resp = ec2_raw(
        &server,
        "Action=ModifyAccountVpcEncryptionControl&Version=2016-11-15\
         &Mode=attempt-enforce&Lambda=enable",
    )
    .await;
    let body = resp.text().await.unwrap();
    assert!(body.contains("<mode>attempt-enforce</mode>"), "{body}");
    // The enabled Lambda exclusion is reflected in the response.
    assert!(
        body.contains("<lambda><state>enabled</state></lambda>"),
        "{body}"
    );
}

#[test_action("ec2", "ModifyVpcEndpointPayerResponsibility", checksum = "f3767a31")]
#[tokio::test]
async fn ec2_modify_vpc_endpoint_payer_responsibility() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;
    let id = make_vpce(&c).await;
    let resp = ec2_raw(
        &server,
        &format!(
            "Action=ModifyVpcEndpointPayerResponsibility&Version=2016-11-15\
             &VpcEndpointId={id}&PayerResponsibility=vpc-endpoint-service-account\
             &Scope=vpc-endpoint-charges"
        ),
    )
    .await;
    let body = resp.text().await.unwrap();
    assert!(
        body.contains(&format!("<vpcEndpointId>{id}</vpcEndpointId>")),
        "{body}"
    );
    assert!(
        body.contains(
            "<payerResponsibilityType>vpc-endpoint-service-account</payerResponsibilityType>"
        ),
        "{body}"
    );
}

#[test_action("ec2", "AttachImageWatermark", checksum = "fb734a5c")]
#[tokio::test]
async fn ec2_attach_image_watermark() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;
    let ami = make_ami(&c).await;
    let resp = ec2_raw(
        &server,
        &format!(
            "Action=AttachImageWatermark&Version=2016-11-15&ImageId={ami}&WatermarkName=brand"
        ),
    )
    .await;
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<watermarkKey>"),
        "missing watermarkKey in response: {body}"
    );
}

#[test_action("ec2", "DetachImageWatermark", checksum = "00e5b4c6")]
#[tokio::test]
async fn ec2_detach_image_watermark() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;
    let ami = make_ami(&c).await;
    let attach = ec2_raw(
        &server,
        &format!(
            "Action=AttachImageWatermark&Version=2016-11-15&ImageId={ami}&WatermarkName=brand"
        ),
    )
    .await;
    let body = attach.text().await.unwrap();
    let key = body
        .split("<watermarkKey>")
        .nth(1)
        .and_then(|s| s.split("</watermarkKey>").next())
        .expect("watermarkKey in attach response");
    let resp = ec2_raw(
        &server,
        &format!("Action=DetachImageWatermark&Version=2016-11-15&ImageId={ami}&WatermarkKey={key}"),
    )
    .await;
    let body = resp.text().await.unwrap();
    assert!(body.contains("<return>true</return>"), "unexpected: {body}");
}
