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
