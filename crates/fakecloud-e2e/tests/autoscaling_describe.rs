//! DescribeAutoScalingGroups must report a launch-template-backed group's
//! LaunchTemplate and its LoadBalancerNames, or terraform reads them back empty
//! and shows perpetual drift. DesiredCapacity=0 keeps this container-free.

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn describe_asg_round_trips_launch_template_and_load_balancers() {
    let s = TestServer::start().await;
    let asg = aws_sdk_autoscaling::Client::new(&s.aws_config().await);

    asg.create_auto_scaling_group()
        .auto_scaling_group_name("lt-asg")
        .launch_template(
            aws_sdk_autoscaling::types::LaunchTemplateSpecification::builder()
                .launch_template_id("lt-0abc1230000000000")
                .version("$Latest")
                .build(),
        )
        .min_size(0)
        .max_size(2)
        .desired_capacity(0)
        .availability_zones("us-east-1a")
        .load_balancer_names("classic-elb-1")
        .send()
        .await
        .expect("create ASG");

    let d = asg
        .describe_auto_scaling_groups()
        .auto_scaling_group_names("lt-asg")
        .send()
        .await
        .unwrap();
    let g = d
        .auto_scaling_groups()
        .iter()
        .find(|g| g.auto_scaling_group_name() == Some("lt-asg"))
        .expect("ASG should exist");

    let lt = g
        .launch_template()
        .expect("DescribeAutoScalingGroups must report the LaunchTemplate");
    assert_eq!(lt.launch_template_id(), Some("lt-0abc1230000000000"));
    assert_eq!(lt.version(), Some("$Latest"));

    assert_eq!(
        g.load_balancer_names(),
        &["classic-elb-1".to_string()],
        "Classic-ELB attachment must round-trip"
    );
}
