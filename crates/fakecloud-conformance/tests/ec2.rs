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
