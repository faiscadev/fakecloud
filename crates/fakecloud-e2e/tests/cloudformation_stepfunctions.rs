//! CloudFormation provisioner for AWS::StepFunctions::* resources.

mod helpers;

use aws_sdk_cloudformation::types::{Capability, OnFailure};
use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Activity": {
      "Type": "AWS::StepFunctions::Activity",
      "Properties": {"Name": "cfn-activity"}
    },
    "StateMachine": {
      "Type": "AWS::StepFunctions::StateMachine",
      "Properties": {
        "StateMachineName": "cfn-sm",
        "RoleArn": "arn:aws:iam::000000000000:role/StepRole",
        "StateMachineType": "STANDARD",
        "DefinitionString": "{\"Comment\":\"hello\",\"StartAt\":\"Done\",\"States\":{\"Done\":{\"Type\":\"Succeed\"}}}"
      }
    }
  },
  "Outputs": {
    "ActivityArn": {"Value": {"Ref": "Activity"}},
    "StateMachineArn": {"Value": {"Ref": "StateMachine"}}
  }
}"#;

#[tokio::test]
async fn cfn_provisions_stepfunctions_resources() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sfn = aws_sdk_sfn::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("sfn-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("sfn-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    let outputs: std::collections::HashMap<&str, &str> = stack
        .outputs()
        .iter()
        .filter_map(|o| Some((o.output_key()?, o.output_value()?)))
        .collect();

    let activity_arn = outputs.get("ActivityArn").expect("ActivityArn");
    let sm_arn = outputs.get("StateMachineArn").expect("StateMachineArn");

    assert!(
        activity_arn.contains(":activity:cfn-activity"),
        "activity arn format: {activity_arn}"
    );
    assert!(
        sm_arn.contains(":stateMachine:cfn-sm"),
        "state machine arn format: {sm_arn}"
    );

    // Verify state machine via SDK.
    let sm = sfn
        .describe_state_machine()
        .state_machine_arn(*sm_arn)
        .send()
        .await
        .expect("describe_state_machine");
    assert_eq!(sm.name(), "cfn-sm");
    assert_eq!(sm.role_arn(), "arn:aws:iam::000000000000:role/StepRole");

    // Verify activity via SDK.
    let act = sfn
        .describe_activity()
        .activity_arn(*activity_arn)
        .send()
        .await
        .expect("describe_activity");
    assert_eq!(act.name(), "cfn-activity");

    // Tear down.
    cfn.delete_stack()
        .stack_name("sfn-stack")
        .send()
        .await
        .expect("delete_stack");

    let sm_after = sfn
        .describe_state_machine()
        .state_machine_arn(*sm_arn)
        .send()
        .await;
    assert!(sm_after.is_err(), "state machine should be gone");
}

/// Gap 2 (issue #1647): `DefinitionSubstitutions` must replace `${token}`
/// placeholders in the definition. This is how SAM/CFN state machines
/// reference sibling Lambda functions; leaving the literal `${...}` in
/// place breaks executions later.
#[tokio::test]
async fn cfn_state_machine_applies_definition_substitutions() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sfn = aws_sdk_sfn::Client::new(&server.aws_config().await);

    let template = r#"{
      "Resources": {
        "SM": {
          "Type": "AWS::StepFunctions::StateMachine",
          "Properties": {
            "StateMachineName": "subs-sm",
            "RoleArn": "arn:aws:iam::000000000000:role/r",
            "DefinitionString": "{\"StartAt\":\"T\",\"States\":{\"T\":{\"Type\":\"Task\",\"Resource\":\"arn:aws:states:::lambda:invoke\",\"Parameters\":{\"FunctionName\":\"${function_name}\"},\"End\":true}}}",
            "DefinitionSubstitutions": {"function_name": "my-real-function"}
          }
        }
      },
      "Outputs": {"Arn": {"Value": {"Ref": "SM"}}}
    }"#;

    cfn.create_stack()
        .stack_name("subs-stack")
        .template_body(template)
        .send()
        .await
        .expect("create_stack");

    let sm = sfn
        .describe_state_machine()
        .state_machine_arn("arn:aws:states:us-east-1:123456789012:stateMachine:subs-sm")
        .send()
        .await
        .expect("describe_state_machine");
    assert!(
        sm.definition().contains("my-real-function"),
        "substitution not applied: {}",
        sm.definition()
    );
    assert!(
        !sm.definition().contains("${function_name}"),
        "literal placeholder left behind: {}",
        sm.definition()
    );
}

/// Gap 1 (issue #1647): `DefinitionS3Location` must be fetched from S3 and
/// used as the definition, like `sam package` produces.
#[tokio::test]
async fn cfn_state_machine_reads_definition_from_s3() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let s3 = server.s3_client().await;
    let sfn = aws_sdk_sfn::Client::new(&server.aws_config().await);

    let asl = r#"{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}"#;
    s3.create_bucket().bucket("asl-defs").send().await.unwrap();
    s3.put_object()
        .bucket("asl-defs")
        .key("def.json")
        .body(ByteStream::from(asl.as_bytes().to_vec()))
        .send()
        .await
        .unwrap();

    let template = r#"{
      "Resources": {
        "SM": {
          "Type": "AWS::StepFunctions::StateMachine",
          "Properties": {
            "StateMachineName": "s3-sm",
            "RoleArn": "arn:aws:iam::000000000000:role/r",
            "DefinitionS3Location": {"Bucket": "asl-defs", "Key": "def.json"}
          }
        }
      }
    }"#;

    cfn.create_stack()
        .stack_name("s3-def-stack")
        .template_body(template)
        .send()
        .await
        .expect("create_stack");

    let sm = sfn
        .describe_state_machine()
        .state_machine_arn("arn:aws:states:us-east-1:123456789012:stateMachine:s3-sm")
        .send()
        .await
        .expect("describe_state_machine");
    assert!(
        sm.definition().contains("\"Done\""),
        "S3 definition not used: {}",
        sm.definition()
    );
}

/// Gap 3 (issue #1647): a stack update with a changed `DefinitionString`
/// must propagate to the live state machine (UpdateStateMachine is now
/// invoked on update), not silently keep executing the stale ASL.
#[tokio::test]
async fn cfn_state_machine_update_propagates_definition() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sfn = aws_sdk_sfn::Client::new(&server.aws_config().await);

    let template_a = r#"{
      "Resources": {
        "SM2": {
          "Type": "AWS::StepFunctions::StateMachine",
          "Properties": {
            "StateMachineName": "upd-sm",
            "RoleArn": "arn:aws:iam::000000000000:role/r",
            "DefinitionString": "{\"StartAt\":\"A\",\"States\":{\"A\":{\"Type\":\"Succeed\"}}}"
          }
        }
      }
    }"#;
    let template_b = r#"{
      "Resources": {
        "SM2": {
          "Type": "AWS::StepFunctions::StateMachine",
          "Properties": {
            "StateMachineName": "upd-sm",
            "RoleArn": "arn:aws:iam::000000000000:role/r",
            "DefinitionString": "{\"StartAt\":\"B\",\"States\":{\"B\":{\"Type\":\"Succeed\"}}}"
          }
        }
      }
    }"#;

    cfn.create_stack()
        .stack_name("upd-stack")
        .template_body(template_a)
        .send()
        .await
        .expect("create_stack");

    cfn.update_stack()
        .stack_name("upd-stack")
        .template_body(template_b)
        .send()
        .await
        .expect("update_stack");

    let arn = "arn:aws:states:us-east-1:123456789012:stateMachine:upd-sm";
    let sm = sfn
        .describe_state_machine()
        .state_machine_arn(arn)
        .send()
        .await
        .expect("describe_state_machine");
    assert!(
        sm.definition().contains("\"StartAt\":\"B\""),
        "stack update did not propagate the new definition: {}",
        sm.definition()
    );
    assert!(
        !sm.definition().contains("\"StartAt\":\"A\""),
        "stale definition still present: {}",
        sm.definition()
    );
}
