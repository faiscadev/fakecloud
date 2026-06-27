//! A CloudFormation-provisioned AWS::Batch::* stack survives a restart in
//! persistent mode. Regression for the #1766-class gap: the CFN provisioner
//! writes straight into `batch_state`, so without a `cfn_snapshot_hooks` entry
//! for "batch" the resources would vanish on restart.

mod helpers;

use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "Resources": {
    "CE": {
      "Type": "AWS::Batch::ComputeEnvironment",
      "Properties": { "ComputeEnvironmentName": "persist-ce", "Type": "MANAGED" }
    },
    "JQ": {
      "Type": "AWS::Batch::JobQueue",
      "Properties": {
        "JobQueueName": "persist-q",
        "Priority": 1,
        "ComputeEnvironmentOrder": [
          { "Order": 1, "ComputeEnvironment": { "Ref": "CE" } }
        ]
      }
    },
    "JD": {
      "Type": "AWS::Batch::JobDefinition",
      "Properties": { "JobDefinitionName": "persist-jd", "Type": "container" }
    }
  }
}"#;

#[tokio::test]
async fn cfn_provisioned_batch_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("batch-persist")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    server.restart().await;
    let batch = aws_sdk_batch::Client::new(&server.aws_config().await);

    // The compute environment provisioned by CFN survives the restart.
    let ces = batch.describe_compute_environments().send().await.unwrap();
    assert!(
        ces.compute_environments()
            .iter()
            .any(|c| c.compute_environment_name() == Some("persist-ce")),
        "CFN compute environment should survive restart"
    );

    // The job queue survives too.
    let qs = batch.describe_job_queues().send().await.unwrap();
    assert!(
        qs.job_queues()
            .iter()
            .any(|q| q.job_queue_name() == Some("persist-q")),
        "CFN job queue should survive restart"
    );

    // And the job definition.
    let jds = batch
        .describe_job_definitions()
        .job_definition_name("persist-jd")
        .send()
        .await
        .unwrap();
    assert!(
        !jds.job_definitions().is_empty(),
        "CFN job definition should survive restart"
    );
}
