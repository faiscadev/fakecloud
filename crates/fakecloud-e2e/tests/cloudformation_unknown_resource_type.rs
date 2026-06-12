//! A resource type fakecloud has no provisioner for must not fail the whole
//! stack. Real CloudFormation provisions many types fakecloud doesn't model
//! (and SAM/CDK output routinely includes ones like
//! `AWS::CloudFormation::WaitConditionHandle`). The documented contract
//! (docs/services/cloudformation.md) is that such types are accepted and
//! recorded as provisioned with no backing state, so the stack still reaches
//! CREATE_COMPLETE and `Ref` on the resource resolves.

mod helpers;

use helpers::TestServer;

/// A stack pairing a real, provisioned resource (an SQS queue) with a type
/// fakecloud doesn't model (`WaitConditionHandle`). `Ref` on the unmodelled
/// resource is surfaced as an output.
const TEMPLATE: &str = r#"
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  Handle:
    Type: AWS::CloudFormation::WaitConditionHandle
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: unknown-type-queue
Outputs:
  HandleRef:
    Value:
      Ref: Handle
"#;

#[tokio::test]
async fn unknown_resource_type_does_not_fail_stack() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("unknown-type-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("unknown-type-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(
        stack.stack_status().unwrap().as_str(),
        "CREATE_COMPLETE",
        "a stack with an unmodelled resource type should still complete"
    );

    // `Ref` on the unmodelled resource resolves to a stable physical id.
    let handle_ref = stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some("HandleRef"))
        .and_then(|o| o.output_value())
        .expect("HandleRef output");
    assert!(
        !handle_ref.is_empty(),
        "Ref on the unmodelled resource should resolve"
    );

    // The recorded resource is reported as provisioned.
    let resources = cfn
        .describe_stack_resources()
        .stack_name("unknown-type-stack")
        .send()
        .await
        .expect("describe_stack_resources");
    let handle = resources
        .stack_resources()
        .iter()
        .find(|r| r.logical_resource_id() == Some("Handle"))
        .expect("Handle resource recorded");
    assert_eq!(
        handle.resource_status().unwrap().as_str(),
        "CREATE_COMPLETE"
    );

    // Teardown succeeds even though the resource has no backing state.
    cfn.delete_stack()
        .stack_name("unknown-type-stack")
        .send()
        .await
        .expect("delete_stack");
}
