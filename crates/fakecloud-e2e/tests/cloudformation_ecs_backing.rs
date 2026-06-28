//! CloudFormation-provisioned AWS::ECS::Service launches REAL running tasks
//! (the same container-backed tasks the direct CreateService path spawns) to
//! reach its DesiredCount, instead of the phantom service with running_count 0
//! and no tasks it used to insert at CFN time.
//!
//! The tasks are launched in a detached task after CreateStack returns (so the
//! call never blocks on a container boot/pull). The service's control-plane
//! record (desiredCount) is present in both modes, so the test always asserts
//! on it. With a container runtime wired (docker/podman present) the service
//! additionally spawns real task records that reach RUNNING, which the test
//! polls for. With no runtime (CI / metadata-only) no tasks are launched and
//! running_count stays 0 -- matching the provisioner's prior metadata-only
//! behavior and the direct API's no-runtime path -- so the running-task
//! assertion is gated on runtime availability rather than silently skipped.

mod helpers;

use aws_sdk_cloudformation::types::{Capability, OnFailure};
use helpers::TestServer;

/// A container runtime (docker/podman) backs ECS task execution. When present,
/// the CFN provisioner is wired with an ECS runtime and launches real tasks.
fn container_runtime_available() -> bool {
    for cli in ["docker", "podman"] {
        let ok = std::process::Command::new(cli)
            .arg("info")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if ok {
            return true;
        }
    }
    false
}

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Cluster": {
      "Type": "AWS::ECS::Cluster",
      "Properties": { "ClusterName": "cfn-ecs-real-cluster" }
    },
    "TaskDef": {
      "Type": "AWS::ECS::TaskDefinition",
      "Properties": {
        "Family": "cfn-real-task",
        "NetworkMode": "bridge",
        "RequiresCompatibilities": ["EC2"],
        "Cpu": "128",
        "Memory": "128",
        "ContainerDefinitions": [
          {
            "Name": "web",
            "Image": "public.ecr.aws/docker/library/nginx:alpine",
            "Essential": true
          }
        ]
      }
    },
    "Svc": {
      "Type": "AWS::ECS::Service",
      "Properties": {
        "ServiceName": "cfn-real-svc",
        "Cluster": {"Ref": "Cluster"},
        "TaskDefinition": {"Ref": "TaskDef"},
        "DesiredCount": 2,
        "LaunchType": "EC2"
      }
    }
  },
  "Outputs": {
    "ServiceArn": {"Value": {"Fn::GetAtt": ["Svc", "ServiceArn"]}}
  }
}"#;

#[tokio::test]
async fn cfn_ecs_service_launches_real_tasks() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let ecs = aws_sdk_ecs::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("ecs-backing-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("ecs-backing-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // Control-plane record holds in both modes: the service exists with the
    // declared desired count.
    let svcs = ecs
        .describe_services()
        .cluster("cfn-ecs-real-cluster")
        .services("cfn-real-svc")
        .send()
        .await
        .expect("describe_services");
    let svc = svcs.services().first().expect("service present");
    assert_eq!(svc.service_name(), Some("cfn-real-svc"));
    assert_eq!(svc.desired_count(), 2);

    if container_runtime_available() {
        // With a runtime wired the CFN provisioner launches REAL task records
        // (in a detached task after CreateStack returns) to reach the desired
        // count, the same way the direct CreateService path does. Poll until
        // both tasks appear under the cluster.
        let task_arns = helpers::wait_until(std::time::Duration::from_secs(20), || {
            let ecs = ecs.clone();
            async move {
                let out = ecs
                    .list_tasks()
                    .cluster("cfn-ecs-real-cluster")
                    .send()
                    .await
                    .ok()?;
                (out.task_arns().len() == 2)
                    .then(|| out.task_arns().iter().map(String::from).collect::<Vec<_>>())
            }
        })
        .await
        .expect("CFN-provisioned ECS service must launch its 2 real tasks");
        assert_eq!(
            task_arns.len(),
            2,
            "CFN-provisioned ECS service launched its desired-count tasks"
        );
    }

    cfn.delete_stack()
        .stack_name("ecs-backing-stack")
        .send()
        .await
        .expect("delete_stack");
}
