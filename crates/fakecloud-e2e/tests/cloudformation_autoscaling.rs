//! CloudFormation provisions AWS::AutoScaling::LaunchConfiguration +
//! AutoScalingGroup as real records in the `autoscaling` service.

mod helpers;

use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "Resources": {
    "LC": {
      "Type": "AWS::AutoScaling::LaunchConfiguration",
      "Properties": { "ImageId": "ami-0a1b2c3d4e5f60001", "InstanceType": "t3.micro" }
    },
    "ASG": {
      "Type": "AWS::AutoScaling::AutoScalingGroup",
      "Properties": {
        "MinSize": "1", "MaxSize": "3", "DesiredCapacity": "2",
        "LaunchConfigurationName": { "Ref": "LC" },
        "AvailabilityZones": ["us-east-1a"]
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_provisions_autoscaling_group() {
    let s = TestServer::start().await;
    let cfn = s.cloudformation_client().await;
    let asg = aws_sdk_autoscaling::Client::new(&s.aws_config().await);

    cfn.create_stack()
        .stack_name("asg-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("asg-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        described.stacks()[0].stack_status().unwrap().as_str(),
        "CREATE_COMPLETE"
    );

    // The launch configuration + group exist in the autoscaling service.
    let lcs = asg.describe_launch_configurations().send().await.unwrap();
    assert!(
        !lcs.launch_configurations().is_empty(),
        "CFN launch configuration should exist"
    );

    let groups = asg.describe_auto_scaling_groups().send().await.unwrap();
    let g = groups
        .auto_scaling_groups()
        .iter()
        .find(|g| g.desired_capacity() == Some(2))
        .expect("CFN ASG with desired=2");
    assert_eq!(g.instances().len(), 2, "ASG reconciled to desired capacity");

    // Deleting the stack removes the group.
    cfn.delete_stack()
        .stack_name("asg-stack")
        .send()
        .await
        .unwrap();
    let after = asg.describe_auto_scaling_groups().send().await.unwrap();
    assert!(
        after
            .auto_scaling_groups()
            .iter()
            .all(|g| g.desired_capacity() != Some(2)
                || g.auto_scaling_group_name()
                    != groups.auto_scaling_groups()[0].auto_scaling_group_name()),
        "stack delete should remove the ASG"
    );
}

// bug-audit 2026-06-27, T1.8: a CFN ASG declared with a LaunchTemplate (not the
// legacy LaunchConfigurationName) must carry the launch template, not drop it.
const TEMPLATE_LT: &str = r#"{
  "Resources": {
    "ASG": {
      "Type": "AWS::AutoScaling::AutoScalingGroup",
      "Properties": {
        "MinSize": "1", "MaxSize": "2", "DesiredCapacity": "1",
        "LaunchTemplate": { "LaunchTemplateId": "lt-0abc123", "Version": "3" },
        "AvailabilityZones": ["us-east-1a"]
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_asg_honors_launch_template() {
    let s = TestServer::start().await;
    let cfn = s.cloudformation_client().await;
    let asg = aws_sdk_autoscaling::Client::new(&s.aws_config().await);

    cfn.create_stack()
        .stack_name("asg-lt-stack")
        .template_body(TEMPLATE_LT)
        .send()
        .await
        .expect("create_stack");

    let groups = asg.describe_auto_scaling_groups().send().await.unwrap();
    let g = groups
        .auto_scaling_groups()
        .iter()
        .find(|g| g.auto_scaling_group_name() == Some("ASG"))
        .expect("CFN ASG exists");
    let lt = g
        .launch_template()
        .expect("launch template carried from CFN");
    assert_eq!(lt.launch_template_id(), Some("lt-0abc123"));
    assert_eq!(lt.version(), Some("3"));
}
