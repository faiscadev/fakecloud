//! EC2 Auto Scaling reconciles a group's desired capacity to REAL
//! container-backed EC2 instances (the LocalStack #8367 wedge — every free
//! rival's ASG scales to a mock instance). The instances an ASG launches must
//! show up in EC2 DescribeInstances, and scale-in must terminate them.

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn asg_launches_and_terminates_real_ec2_instances() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let asg = aws_sdk_autoscaling::Client::new(&cfg);
    let ec2 = s.ec2_client().await;

    asg.create_launch_configuration()
        .launch_configuration_name("lc")
        .image_id("ami-0a1b2c3d4e5f60001")
        .instance_type("t3.micro")
        .send()
        .await
        .unwrap();
    asg.create_auto_scaling_group()
        .auto_scaling_group_name("asg")
        .launch_configuration_name("lc")
        .min_size(1)
        .max_size(3)
        .desired_capacity(2)
        .availability_zones("us-east-1a")
        .send()
        .await
        .unwrap();

    // The group reports its 2 instances...
    let groups = asg
        .describe_auto_scaling_groups()
        .auto_scaling_group_names("asg")
        .send()
        .await
        .unwrap();
    let g = &groups.auto_scaling_groups()[0];
    let asg_ids: Vec<String> = g
        .instances()
        .iter()
        .filter_map(|i| i.instance_id().map(String::from))
        .collect();
    assert_eq!(asg_ids.len(), 2, "ASG should have 2 instances");

    // ...and those exact instances are REAL EC2 instances (not mock ids).
    let running = ec2
        .describe_instances()
        .instance_ids(asg_ids[0].clone())
        .instance_ids(asg_ids[1].clone())
        .send()
        .await
        .unwrap();
    let ec2_ids: Vec<String> = running
        .reservations()
        .iter()
        .flat_map(|r| r.instances())
        .filter_map(|i| i.instance_id().map(String::from))
        .collect();
    assert_eq!(
        ec2_ids.len(),
        2,
        "both ASG instances must exist in EC2: {ec2_ids:?}"
    );

    // Scale in to 0 -> the ASG drops them and EC2 terminates them.
    asg.set_desired_capacity()
        .auto_scaling_group_name("asg")
        .desired_capacity(0)
        .send()
        .await
        .unwrap();
    let after = asg
        .describe_auto_scaling_groups()
        .auto_scaling_group_names("asg")
        .send()
        .await
        .unwrap();
    assert!(
        after.auto_scaling_groups()[0].instances().is_empty(),
        "scale-in should drain the group"
    );
    let states = ec2
        .describe_instances()
        .instance_ids(asg_ids[0].clone())
        .send()
        .await
        .unwrap();
    let state = states.reservations()[0].instances()[0]
        .state()
        .and_then(|s| s.name())
        .map(|n| n.as_str().to_string());
    assert_eq!(
        state.as_deref(),
        Some("terminated"),
        "scaled-in instance terminated"
    );
}
