//! SAM transform expands `AWS::Serverless::StateMachine` into a native
//! `AWS::StepFunctions::StateMachine` so a SAM app with Step Functions can
//! deploy through fakecloud CloudFormation without a local aws-sam-translator
//! pre-transform.

mod helpers;

use aws_sdk_cloudformation::types::Capability;
use helpers::TestServer;

/// SAM template from FAKECLOUD-UPSTREAM-BATCH.md §3: an inline ASL
/// `Definition` plus a `Role`, under `Transform: AWS::Serverless-2016-10-31`.
/// An `Outputs` block surfaces the state machine ARN via `Ref`.
const TEMPLATE: &str = r#"
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Resources:
  MySM:
    Type: AWS::Serverless::StateMachine
    Properties:
      Name: sam-sm
      Definition:
        StartAt: Done
        States:
          Done:
            Type: Succeed
      Role: arn:aws:iam::123456789012:role/sfn-role
Outputs:
  Arn:
    Value:
      Ref: MySM
"#;

#[tokio::test]
async fn sam_transform_expands_state_machine() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sfn = aws_sdk_sfn::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("sam-sm-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityIam)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("sam-sm-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(
        stack.stack_status().unwrap().as_str(),
        "CREATE_COMPLETE",
        "SAM state machine stack should reach CREATE_COMPLETE"
    );

    let sm_arn = stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some("Arn"))
        .and_then(|o| o.output_value())
        .expect("Arn output");
    assert!(
        sm_arn.contains(":stateMachine:sam-sm"),
        "state machine arn format: {sm_arn}"
    );

    // The expanded native resource is a real state machine reachable via the
    // Step Functions SDK, carrying the role and the inline definition.
    let sm = sfn
        .describe_state_machine()
        .state_machine_arn(sm_arn)
        .send()
        .await
        .expect("describe_state_machine");
    assert_eq!(sm.name(), "sam-sm");
    assert_eq!(sm.role_arn(), "arn:aws:iam::123456789012:role/sfn-role");
    assert!(
        sm.definition().contains("\"Done\""),
        "inline definition not carried over: {}",
        sm.definition()
    );
}
