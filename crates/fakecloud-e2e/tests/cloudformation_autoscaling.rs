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

    // Reconciliation to desired capacity happens in a detached task after
    // CreateStack returns (so the call never blocks on launching instances), so
    // poll until the group reports its 2 instances.
    let groups = helpers::wait_until(std::time::Duration::from_secs(10), || {
        let asg = asg.clone();
        async move {
            let out = asg.describe_auto_scaling_groups().send().await.ok()?;
            let found = out
                .auto_scaling_groups()
                .iter()
                .any(|g| g.desired_capacity() == Some(2) && g.instances().len() == 2);
            found.then_some(out)
        }
    })
    .await
    .expect("CFN ASG reconciled to desired capacity of 2");
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

// batch 2: a CFN-provisioned ASG must reconcile to REAL container-backed EC2
// instances (via the same RunInstances path the direct CreateAutoScalingGroup
// API uses), not the phantom placeholder metadata the provisioner used to
// insert at CFN time. The launched instances must therefore appear in EC2
// DescribeInstances. This holds with or without a container runtime: with no
// runtime the EC2 records are metadata-only but still real EC2 instances.
#[tokio::test]
async fn cfn_asg_launches_real_ec2_instances() {
    let s = TestServer::start().await;
    let cfn = s.cloudformation_client().await;
    let asg = aws_sdk_autoscaling::Client::new(&s.aws_config().await);
    let ec2 = s.ec2_client().await;

    cfn.create_stack()
        .stack_name("asg-real-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    // Reconciliation runs in a detached task after CreateStack returns; poll
    // until the group reports its 2 instances and capture their ids.
    let asg_ids = helpers::wait_until(std::time::Duration::from_secs(10), || {
        let asg = asg.clone();
        async move {
            let out = asg.describe_auto_scaling_groups().send().await.ok()?;
            let g = out
                .auto_scaling_groups()
                .iter()
                .find(|g| g.desired_capacity() == Some(2))?;
            if g.instances().len() != 2 {
                return None;
            }
            let ids: Vec<String> = g
                .instances()
                .iter()
                .filter_map(|i| i.instance_id().map(String::from))
                .collect();
            (ids.len() == 2).then_some(ids)
        }
    })
    .await
    .expect("CFN ASG reconciled to 2 real instances");

    // Those exact instances are REAL EC2 instances (not phantom ASG metadata).
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
        "both CFN-provisioned ASG instances must exist in EC2: {ec2_ids:?}"
    );
}
