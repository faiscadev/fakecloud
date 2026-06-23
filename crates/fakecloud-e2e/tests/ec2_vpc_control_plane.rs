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
