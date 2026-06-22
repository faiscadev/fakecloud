mod helpers;

use helpers::TestServer;

/// A VPC, subnet, security group, and instance all survive a restart in
/// persistent mode. (Metadata-only -- no container runtime in this test.)
#[tokio::test]
async fn persistence_round_trip_vpc_subnet_sg_instance() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let ec2 = server.ec2_client().await;

    let vpc_id = ec2
        .create_vpc()
        .cidr_block("10.20.0.0/16")
        .send()
        .await
        .unwrap()
        .vpc
        .unwrap()
        .vpc_id
        .unwrap();

    let subnet_id = ec2
        .create_subnet()
        .vpc_id(&vpc_id)
        .cidr_block("10.20.1.0/24")
        .send()
        .await
        .unwrap()
        .subnet
        .unwrap()
        .subnet_id
        .unwrap();

    let group_id = ec2
        .create_security_group()
        .group_name("persist-sg")
        .description("persist me")
        .vpc_id(&vpc_id)
        .send()
        .await
        .unwrap()
        .group_id
        .unwrap();

    let instance_id = ec2
        .run_instances()
        .image_id("ami-12345678")
        .min_count(1)
        .max_count(1)
        .subnet_id(&subnet_id)
        .send()
        .await
        .unwrap()
        .instances
        .unwrap()
        .first()
        .unwrap()
        .instance_id
        .clone()
        .unwrap();

    server.restart().await;
    let ec2 = server.ec2_client().await;

    // VPC survives.
    assert!(ec2
        .describe_vpcs()
        .vpc_ids(&vpc_id)
        .send()
        .await
        .unwrap()
        .vpcs()
        .iter()
        .any(|v| v.vpc_id() == Some(vpc_id.as_str())));

    // Subnet survives.
    assert!(ec2
        .describe_subnets()
        .subnet_ids(&subnet_id)
        .send()
        .await
        .unwrap()
        .subnets()
        .iter()
        .any(|s| s.subnet_id() == Some(subnet_id.as_str())));

    // Security group survives.
    assert!(ec2
        .describe_security_groups()
        .group_ids(&group_id)
        .send()
        .await
        .unwrap()
        .security_groups()
        .iter()
        .any(|g| g.group_id() == Some(group_id.as_str())));

    // Instance survives.
    let instances = ec2
        .describe_instances()
        .instance_ids(&instance_id)
        .send()
        .await
        .unwrap();
    assert!(instances
        .reservations()
        .iter()
        .flat_map(|r| r.instances())
        .any(|i| i.instance_id() == Some(instance_id.as_str())));
}

/// A deleted VPC stays gone after restart.
#[tokio::test]
async fn persistence_delete_vpc_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let ec2 = server.ec2_client().await;

    let vpc_id = ec2
        .create_vpc()
        .cidr_block("10.30.0.0/16")
        .send()
        .await
        .unwrap()
        .vpc
        .unwrap()
        .vpc_id
        .unwrap();
    ec2.delete_vpc().vpc_id(&vpc_id).send().await.unwrap();

    server.restart().await;
    let ec2 = server.ec2_client().await;

    let vpcs = ec2.describe_vpcs().send().await.unwrap();
    assert!(!vpcs
        .vpcs()
        .iter()
        .any(|v| v.vpc_id() == Some(vpc_id.as_str())));
}
