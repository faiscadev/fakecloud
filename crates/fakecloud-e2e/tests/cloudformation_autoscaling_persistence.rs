//! A CloudFormation-provisioned AWS::AutoScaling::* stack survives a restart in
//! persistent mode. Regression for the #1766-class gap where the CFN provisioner
//! wrote straight into `autoscaling_state` but no `cfn_snapshot_hooks` entry for
//! "autoscaling" was registered, so the ASG vanished on restart.

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
async fn cfn_provisioned_asg_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("asg-persist")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    server.restart().await;
    let asg = aws_sdk_autoscaling::Client::new(&server.aws_config().await);

    // The group provisioned by CFN is still present after restart.
    let groups = asg.describe_auto_scaling_groups().send().await.unwrap();
    let g = groups
        .auto_scaling_groups()
        .iter()
        .find(|g| g.desired_capacity() == Some(2))
        .expect("CFN-provisioned ASG should survive restart");
    assert_eq!(g.instances().len(), 2, "ASG capacity persisted");

    // The launch configuration survives too.
    let lcs = asg.describe_launch_configurations().send().await.unwrap();
    assert!(
        !lcs.launch_configurations().is_empty(),
        "CFN launch configuration should survive restart"
    );
}
