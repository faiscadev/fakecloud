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

#[test_action("ec2", "CreatePlacementGroup", checksum = "66a063d2")]
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

#[test_action("ec2", "DescribePlacementGroups", checksum = "a75a2b4a")]
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
    assert_eq!(
        r.starting_instances()[0].current_state().unwrap().name(),
        Some(&aws_sdk_ec2::types::InstanceStateName::Running)
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
    let r = c
        .describe_instance_status()
        .instance_ids(&id)
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

#[test_action("ec2", "DescribeVolumesModifications", checksum = "9c511c39")]
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

#[test_action("ec2", "DescribeImages", checksum = "5a0f040c")]
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
    assert!(c
        .describe_images()
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

#[test_action("ec2", "GetAllowedImagesSettings", checksum = "5412ccbd")]
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
    checksum = "3cecaf96"
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
    assert!(c
        .describe_network_acls()
        .send()
        .await
        .unwrap()
        .network_acls()
        .is_empty());
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

#[test_action("ec2", "CreateVpcEndpoint", checksum = "71acdc45")]
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

#[test_action("ec2", "DescribeVpcEndpoints", checksum = "9443c193")]
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

#[test_action("ec2", "DescribeVpcEndpointConnections", checksum = "7ecaebfb")]
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

#[test_action("ec2", "CreateFlowLogs", checksum = "2d5b67e2")]
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

#[test_action("ec2", "DescribeFlowLogs", checksum = "579d9fb3")]
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
