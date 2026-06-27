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

    // The compute environment exists in the batch service, and (like an
    // API-created one) reports its backing ECS cluster ARN.
    let ces = batch.describe_compute_environments().send().await.unwrap();
    let ce = ces
        .compute_environments()
        .iter()
        .find(|c| c.compute_environment_name() == Some("cfn-ce"))
        .expect("CFN compute environment should exist");
    assert!(
        ce.ecs_cluster_arn()
            .is_some_and(|a| a.contains(":cluster/")),
        "CFN compute environment should report ecsClusterArn, got {:?}",
        ce.ecs_cluster_arn()
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

const GETATT_TEMPLATE: &str = r#"{
  "Resources": {
    "SP": {
      "Type": "AWS::Batch::SchedulingPolicy",
      "Properties": { "Name": "cfn-sp" }
    },
    "CE": {
      "Type": "AWS::Batch::ComputeEnvironment",
      "Properties": { "ComputeEnvironmentName": "getatt-ce", "Type": "MANAGED" }
    },
    "JQ": {
      "Type": "AWS::Batch::JobQueue",
      "Properties": {
        "JobQueueName": "getatt-q",
        "Priority": 1,
        "SchedulingPolicyArn": { "Ref": "SP" },
        "ComputeEnvironmentOrder": [
          { "Order": 1, "ComputeEnvironment": { "Ref": "CE" } }
        ]
      }
    }
  },
  "Outputs": {
    "CeArn": { "Value": { "Fn::GetAtt": ["CE", "ComputeEnvironmentArn"] } },
    "QArn": { "Value": { "Fn::GetAtt": ["JQ", "JobQueueArn"] } },
    "SpArn": { "Value": { "Ref": "SP" } }
  }
}"#;

#[tokio::test]
async fn cfn_batch_getatt_and_scheduling_policy_resolve() {
    let s = TestServer::start().await;
    let cfn = s.cloudformation_client().await;
    let batch = aws_sdk_batch::Client::new(&s.aws_config().await);

    cfn.create_stack()
        .stack_name("getatt-stack")
        .template_body(GETATT_TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("getatt-stack")
        .send()
        .await
        .unwrap();
    let outputs = described.stacks()[0].outputs();
    let out = |key: &str| {
        outputs
            .iter()
            .find(|o| o.output_key() == Some(key))
            .and_then(|o| o.output_value())
            .unwrap_or("")
            .to_string()
    };

    // Fn::GetAtt resolves to the real ARNs, not the "Logical.Attr" placeholder.
    let ce_arn = out("CeArn");
    assert!(
        ce_arn.starts_with("arn:aws:batch:") && ce_arn.contains(":compute-environment/"),
        "CE GetAtt should resolve to the ARN, got {ce_arn:?}"
    );
    let q_arn = out("QArn");
    assert!(
        q_arn.starts_with("arn:aws:batch:") && q_arn.contains(":job-queue/"),
        "JobQueue GetAtt should resolve to the ARN, got {q_arn:?}"
    );

    // The scheduling policy was provisioned (its own resource type), and the
    // JobQueue's SchedulingPolicyArn Ref resolved to its real ARN.
    let sp_arn = out("SpArn");
    assert!(
        sp_arn.contains(":scheduling-policy/"),
        "SchedulingPolicy Ref should resolve to the ARN, got {sp_arn:?}"
    );
    let sps = batch
        .describe_scheduling_policies()
        .arns(sp_arn.clone())
        .send()
        .await
        .unwrap();
    assert!(
        sps.scheduling_policies()
            .iter()
            .any(|p| p.arn() == Some(sp_arn.as_str())),
        "CFN-provisioned scheduling policy should exist in the batch service"
    );
}
