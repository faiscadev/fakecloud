//! A CloudFormation-provisioned AWS::Pipes::Pipe survives a restart in
//! persistent mode. Regression for the #1766-class gap: the CFN provisioner
//! writes straight into `pipes_state`, so without a `cfn_snapshot_hooks` entry
//! for "pipes" the pipe would vanish on restart.

mod helpers;

use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "Resources": {
    "SrcQ": { "Type": "AWS::SQS::Queue", "Properties": { "QueueName": "persist-pipe-src" } },
    "TgtQ": { "Type": "AWS::SQS::Queue", "Properties": { "QueueName": "persist-pipe-tgt" } },
    "Pipe": {
      "Type": "AWS::Pipes::Pipe",
      "Properties": {
        "Name": "persist-pipe",
        "RoleArn": "arn:aws:iam::000000000000:role/pipe-role",
        "Source": { "Fn::GetAtt": ["SrcQ", "Arn"] },
        "Target": { "Fn::GetAtt": ["TgtQ", "Arn"] }
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_provisioned_pipe_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("pipe-persist")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    server.restart().await;
    let pipes = aws_sdk_pipes::Client::new(&server.aws_config().await);

    let described = pipes
        .describe_pipe()
        .name("persist-pipe")
        .send()
        .await
        .expect("CFN-provisioned pipe should survive restart");
    assert_eq!(
        described.current_state(),
        Some(&aws_sdk_pipes::types::PipeState::Running)
    );
}
