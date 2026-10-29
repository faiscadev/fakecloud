//! EC2 VPC control-plane field-presence: default resources, IPv6 CIDR
//! generation, subnet DNS options, and ENI private DNS / default SG.

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn secondary_ipv4_cidr_is_readable_by_terraform_filter() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;
    let created = c
        .create_vpc()
        .cidr_block("10.40.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = created.vpc().unwrap().vpc_id().unwrap();
    let associated = c
        .associate_vpc_cidr_block()
        .vpc_id(vpc_id)
        .cidr_block("10.41.0.0/16")
        .send()
        .await
        .unwrap();
    let association_id = associated
        .cidr_block_association()
        .unwrap()
        .association_id()
        .unwrap();
    let filter = aws_sdk_ec2::types::Filter::builder()
        .name("cidr-block-association.association-id")
        .values(association_id)
        .build();
    // This is the DescribeVpcs request Terraform's create waiter and refresh use.
    let described = c
        .describe_vpcs()
        .filters(filter.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(described.vpcs().len(), 1);
    assert_eq!(described.vpcs()[0].vpc_id(), Some(vpc_id));
    let association = described.vpcs()[0]
        .cidr_block_association_set()
        .iter()
        .find(|a| a.association_id() == Some(association_id))
        .unwrap();
    assert_eq!(association.cidr_block(), Some("10.41.0.0/16"));
    assert_eq!(
        association.cidr_block_state().unwrap().state(),
        Some(&aws_sdk_ec2::types::VpcCidrBlockStateCode::Associated)
    );
    c.disassociate_vpc_cidr_block()
        .association_id(association_id)
        .send()
        .await
        .unwrap();
    assert!(c
        .describe_vpcs()
        .filters(filter)
        .send()
        .await
        .unwrap()
        .vpcs()
        .is_empty());
}

/// AssociateVpcCidrBlock refuses an IPv4 block that overlaps any block of
/// the VPC (`InvalidVpc.Range`) and accepts further Amazon-provided IPv6
/// blocks, each with its own association.
#[tokio::test]
async fn vpc_cidr_associations_reject_overlap_and_allow_several_ipv6() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;
    let vpc = c
        .create_vpc()
        .cidr_block("10.50.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap();
    c.associate_vpc_cidr_block()
        .vpc_id(vpc_id)
        .cidr_block("10.51.0.0/16")
        .send()
        .await
        .unwrap();
    for cidr in ["10.50.0.0/16", "10.50.4.0/24", "10.51.0.0/16"] {
        let err = c
            .associate_vpc_cidr_block()
            .vpc_id(vpc_id)
            .cidr_block(cidr)
            .send()
            .await
            .expect_err("overlapping block must be refused");
        assert_eq!(
            err.into_service_error().meta().code(),
            Some("InvalidVpc.Range"),
            "{cidr}"
        );
    }

    let mut ids = Vec::new();
    for _ in 0..2 {
        let out = c
            .associate_vpc_cidr_block()
            .vpc_id(vpc_id)
            .amazon_provided_ipv6_cidr_block(true)
            .send()
            .await
            .unwrap();
        ids.push(
            out.ipv6_cidr_block_association()
                .unwrap()
                .association_id()
                .unwrap()
                .to_string(),
        );
    }
    assert_ne!(ids[0], ids[1]);
    let described = c.describe_vpcs().vpc_ids(vpc_id).send().await.unwrap();
    let set = described.vpcs()[0].ipv6_cidr_block_association_set();
    assert_eq!(set.len(), 2);
    assert_ne!(set[0].ipv6_cidr_block(), set[1].ipv6_cidr_block());
}

/// Each region has its own default VPC and default subnets: no id is shared
/// between two regions of one account.
#[tokio::test]
async fn default_vpcs_are_distinct_per_region() {
    let server = TestServer::start().await;
    let mut seen = std::collections::BTreeSet::new();
    for region in ["us-east-1", "eu-west-1"] {
        let rc = aws_sdk_ec2::Client::new(&server.aws_config_in(region).await);
        let vpcs = rc
            .describe_vpcs()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("is-default")
                    .values("true")
                    .build(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(vpcs.vpcs().len(), 1, "{region}");
        let vpc_id = vpcs.vpcs()[0].vpc_id().unwrap().to_string();
        let subnets = rc
            .describe_subnets()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("vpc-id")
                    .values(&vpc_id)
                    .build(),
            )
            .send()
            .await
            .unwrap();
        assert!(!subnets.subnets().is_empty());
        for subnet in subnets.subnets() {
            assert!(subnet.availability_zone().unwrap().starts_with(region));
            assert!(seen.insert(subnet.subnet_id().unwrap().to_string()));
        }
        assert!(seen.insert(vpc_id), "{region} default VPC id reused");
    }
}

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

#[tokio::test]
async fn describe_regions_and_zone_ids_follow_aws_naming() {
    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    let regions = c.describe_regions().send().await.unwrap();
    let find = |name: &str| {
        regions
            .regions()
            .iter()
            .find(|r| r.region_name() == Some(name))
            .cloned()
    };
    for name in [
        "ap-southeast-6",
        "ap-southeast-7",
        "mx-central-1",
        "ap-east-2",
    ] {
        let r = find(name).unwrap_or_else(|| panic!("{name} missing"));
        assert_eq!(r.opt_in_status(), Some("opted-in"), "{name}");
    }
    assert_eq!(
        find("us-east-1").unwrap().opt_in_status(),
        Some("opt-in-not-required")
    );

    // Zone ids use AWS's prefixes: ap-southeast-2 is `apse2`, and ap-south-1
    // (`aps1`) does not collide with ap-southeast-1 (`apse1`).
    // Each region lists its real number of zones (us-east-1 has six).
    for (region, prefix, count) in [
        ("ap-southeast-2", "apse2", 3),
        ("ap-south-1", "aps1", 3),
        ("ap-southeast-1", "apse1", 3),
        ("us-west-2", "usw2", 4),
        ("us-east-1", "use1", 6),
        // The union of the letters different accounts see.
        ("us-west-1", "usw1", 3),
        ("ap-northeast-1", "apne1", 4),
    ] {
        let rc = aws_sdk_ec2::Client::new(&server.aws_config_in(region).await);
        let zones = rc.describe_availability_zones().send().await.unwrap();
        let ids: Vec<&str> = zones
            .availability_zones()
            .iter()
            .filter_map(|z| z.zone_id())
            .collect();
        let expected: Vec<String> = (1..=count).map(|n| format!("{prefix}-az{n}")).collect();
        assert_eq!(ids, expected, "{region}");
    }

    // us-east-1d..f are real zones: a subnet can be placed there.
    let east = aws_sdk_ec2::Client::new(&server.aws_config_in("us-east-1").await);
    let east_vpc = east
        .create_vpc()
        .cidr_block("10.41.0.0/16")
        .send()
        .await
        .unwrap();
    let east_subnet = east
        .create_subnet()
        .vpc_id(east_vpc.vpc().unwrap().vpc_id().unwrap())
        .cidr_block("10.41.1.0/24")
        .availability_zone("us-east-1f")
        .send()
        .await
        .unwrap();
    assert_eq!(
        east_subnet.subnet().unwrap().availability_zone_id(),
        Some("use1-az6")
    );

    // A subnet's zone id agrees with DescribeAvailabilityZones, and a subnet
    // can be placed by zone id.
    let rc = aws_sdk_ec2::Client::new(&server.aws_config_in("ap-southeast-2").await);
    let vpc = rc
        .create_vpc()
        .cidr_block("10.40.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap();
    let by_name = rc
        .create_subnet()
        .vpc_id(vpc_id)
        .cidr_block("10.40.1.0/24")
        .availability_zone("ap-southeast-2b")
        .send()
        .await
        .unwrap();
    assert_eq!(
        by_name.subnet().unwrap().availability_zone_id(),
        Some("apse2-az2")
    );
    let by_id = rc
        .create_subnet()
        .vpc_id(vpc_id)
        .cidr_block("10.40.2.0/24")
        .availability_zone_id("apse2-az3")
        .send()
        .await
        .unwrap();
    let s = by_id.subnet().unwrap();
    assert_eq!(s.availability_zone(), Some("ap-southeast-2c"));
    assert_eq!(s.availability_zone_id(), Some("apse2-az3"));

    // A zone or zone id outside the region is InvalidParameterValue, not a
    // subnet silently placed in another zone (and a non-ASCII name must not
    // crash the handler).
    for (az, az_id) in [
        (Some("us-east-1a"), None),
        (Some("ap-southeast-2d"), None),
        (Some("us-\u{e9}-1a"), None),
        (None, Some("use1-az1")),
        (None, Some("apse2-az99")),
        (None, Some("garbage")),
    ] {
        let err = rc
            .create_subnet()
            .vpc_id(vpc_id)
            .cidr_block("10.40.9.0/24")
            .set_availability_zone(az.map(str::to_string))
            .set_availability_zone_id(az_id.map(str::to_string))
            .send()
            .await
            .expect_err("foreign zone must be rejected");
        assert_eq!(
            err.into_service_error().meta().code(),
            Some("InvalidParameterValue"),
            "{az:?} {az_id:?}"
        );
    }
}

/// A permission authorized with several CIDRs lists every one of them, with its
/// description, under the permission's single `IpRanges` list -- through both
/// the AWS CLI (botocore) and the Rust SDK.
#[tokio::test]
async fn describe_security_groups_lists_every_cidr_of_a_permission() {
    let server = TestServer::start().await;
    let out = server
        .aws_cli(&[
            "ec2",
            "create-security-group",
            "--group-name",
            "demo",
            "--description",
            "Testing SG CIDRs",
        ])
        .await;
    assert!(out.success(), "create failed: {}", out.stderr_text());
    let sg_id = out.stdout_json()["GroupId"].as_str().unwrap().to_string();
    let out = server
        .aws_cli(&[
            "ec2",
            "authorize-security-group-ingress",
            "--group-id",
            &sg_id,
            "--ip-permissions",
            r#"[{"IpProtocol":"tcp","FromPort":22,"ToPort":22,"IpRanges":[{"Description":"Allow SSH","CidrIp":"172.0.0.0/12"},{"Description":"Allow SSH","CidrIp":"192.168.0.0/16"},{"CidrIp":"10.0.0.0/8"}]}]"#,
        ])
        .await;
    assert!(out.success(), "authorize failed: {}", out.stderr_text());

    let out = server
        .aws_cli(&["ec2", "describe-security-groups", "--group-ids", &sg_id])
        .await;
    assert!(out.success(), "describe failed: {}", out.stderr_text());
    let json = out.stdout_json();
    let sg = &json["SecurityGroups"][0];
    assert_eq!(
        sg["IpPermissions"],
        serde_json::json!([{
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [
                {"CidrIp": "172.0.0.0/12", "Description": "Allow SSH"},
                {"CidrIp": "192.168.0.0/16", "Description": "Allow SSH"},
                {"CidrIp": "10.0.0.0/8"}
            ]
        }])
    );
    // The default egress rule has no description, so none is reported.
    assert_eq!(
        sg["IpPermissionsEgress"][0]["IpRanges"],
        serde_json::json!([{"CidrIp": "0.0.0.0/0"}])
    );

    let c = server.ec2_client().await;
    let resp = c
        .describe_security_groups()
        .group_ids(&sg_id)
        .send()
        .await
        .unwrap();
    let perms = resp.security_groups()[0].ip_permissions();
    assert_eq!(perms.len(), 1);
    let cidrs: Vec<(&str, Option<&str>)> = perms[0]
        .ip_ranges()
        .iter()
        .map(|r| (r.cidr_ip().unwrap(), r.description()))
        .collect();
    assert_eq!(
        cidrs,
        vec![
            ("172.0.0.0/12", Some("Allow SSH")),
            ("192.168.0.0/16", Some("Allow SSH")),
            ("10.0.0.0/8", None),
        ]
    );
}

/// DescribeSecurityGroupRules honors its `group-id`, `security-group-rule-id`
/// and `tag:<key>` filters instead of returning every rule in the account.
#[tokio::test]
async fn describe_security_group_rules_honors_filters() {
    use aws_sdk_ec2::types::{Filter, IpPermission, IpRange, ResourceType, Tag, TagSpecification};

    let server = TestServer::start().await;
    let c = server.ec2_client().await;
    let a = c
        .create_security_group()
        .group_name("rules-a")
        .description("a")
        .send()
        .await
        .unwrap()
        .group_id()
        .unwrap()
        .to_string();
    let b = c
        .create_security_group()
        .group_name("rules-b")
        .description("b")
        .send()
        .await
        .unwrap()
        .group_id()
        .unwrap()
        .to_string();
    let ssh = IpPermission::builder()
        .ip_protocol("tcp")
        .from_port(22)
        .to_port(22)
        .ip_ranges(IpRange::builder().cidr_ip("10.0.0.0/8").build())
        .build();
    let authorized = c
        .authorize_security_group_ingress()
        .group_id(&a)
        .ip_permissions(ssh.clone())
        .tag_specifications(
            TagSpecification::builder()
                .resource_type(ResourceType::SecurityGroupRule)
                .tags(Tag::builder().key("team").value("net").build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    let a_rule = authorized.security_group_rules()[0].clone();
    assert_eq!(a_rule.tags().len(), 1);
    assert_eq!(a_rule.description(), None);
    c.authorize_security_group_ingress()
        .group_id(&b)
        .ip_permissions(ssh)
        .send()
        .await
        .unwrap();

    // group-id: sg-a's ingress rule + its default egress rule, nothing else.
    let out = server
        .aws_cli(&[
            "ec2",
            "describe-security-group-rules",
            "--filters",
            &format!("Name=group-id,Values={a}"),
        ])
        .await;
    assert!(out.success(), "describe failed: {}", out.stderr_text());
    let rules = out.stdout_json()["SecurityGroupRules"].clone();
    let rules = rules.as_array().unwrap();
    assert_eq!(rules.len(), 2, "{rules:?}");
    assert!(
        rules.iter().all(|r| r["GroupId"] == a.as_str()),
        "{rules:?}"
    );

    let by_id = c
        .describe_security_group_rules()
        .filters(
            Filter::builder()
                .name("security-group-rule-id")
                .values(a_rule.security_group_rule_id().unwrap())
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(by_id.security_group_rules().len(), 1);

    let by_tag = c
        .describe_security_group_rules()
        .filters(Filter::builder().name("tag:team").values("net").build())
        .send()
        .await
        .unwrap();
    let tagged = by_tag.security_group_rules();
    assert_eq!(tagged.len(), 1);
    assert_eq!(
        tagged[0].security_group_rule_id(),
        a_rule.security_group_rule_id()
    );
    assert_eq!(tagged[0].tags()[0].key(), Some("team"));
}
