//! CloudFormation provisions AWS::Batch::ComputeEnvironment / JobQueue /
//! JobDefinition as real records in the `batch` service control plane.

mod helpers;

use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "Resources": {
    "CE": {
      "Type": "AWS::Batch::ComputeEnvironment",
      "Properties": {
        "ComputeEnvironmentName": "cfn-ce",
        "Type": "MANAGED",
        "State": "ENABLED"
      }
    },
    "JQ": {
      "Type": "AWS::Batch::JobQueue",
      "Properties": {
        "JobQueueName": "cfn-q",
        "Priority": 1,
        "ComputeEnvironmentOrder": [
          { "Order": 1, "ComputeEnvironment": { "Ref": "CE" } }
        ]
      }
    },
    "JD": {
      "Type": "AWS::Batch::JobDefinition",
      "Properties": {
        "JobDefinitionName": "cfn-jd",
        "Type": "container",
        "ContainerProperties": {
          "Image": "public.ecr.aws/docker/library/alpine:3.20",
          "Vcpus": 1,
          "Memory": 512
        }
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_provisions_batch_resources() {
    let s = TestServer::start().await;
    let cfn = s.cloudformation_client().await;
    let batch = aws_sdk_batch::Client::new(&s.aws_config().await);

    cfn.create_stack()
        .stack_name("batch-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("batch-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        described.stacks()[0].stack_status().unwrap().as_str(),
        "CREATE_COMPLETE"
    );

    // The compute environment exists in the batch service.
    let ces = batch.describe_compute_environments().send().await.unwrap();
    assert!(
        ces.compute_environments()
            .iter()
            .any(|c| c.compute_environment_name() == Some("cfn-ce")),
        "CFN compute environment should exist"
    );

    // The job queue exists.
    let qs = batch.describe_job_queues().send().await.unwrap();
    assert!(
        qs.job_queues()
            .iter()
            .any(|q| q.job_queue_name() == Some("cfn-q")),
        "CFN job queue should exist"
    );

    // The job definition exists (revision 1).
    let jds = batch
        .describe_job_definitions()
        .job_definition_name("cfn-jd")
        .send()
        .await
        .unwrap();
    let jd = jds
        .job_definitions()
        .iter()
        .find(|d| d.job_definition_name() == Some("cfn-jd"))
        .expect("CFN job definition should exist");
    assert_eq!(jd.revision(), Some(1));

    // Deleting the stack removes the compute environment.
    cfn.delete_stack()
        .stack_name("batch-stack")
        .send()
        .await
        .unwrap();
    let after = batch.describe_compute_environments().send().await.unwrap();
    assert!(
        after
            .compute_environments()
            .iter()
            .all(|c| c.compute_environment_name() != Some("cfn-ce")),
        "stack delete should remove the compute environment"
    );
}
