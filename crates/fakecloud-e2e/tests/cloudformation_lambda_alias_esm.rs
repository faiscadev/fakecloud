//! Regression: a stack combining `AWS::Lambda::Alias` with an
//! `AWS::Lambda::EventSourceMapping` whose `FunctionName` is
//! `{"Ref": "<AliasLogicalId>"}` (the expanded-CFN shape of a SAM function
//! using `AutoPublishAlias` + `Events`). This used to fail permanently with
//! "Function <name>:live does not exist yet" because (a) `Ref` on the alias
//! resolved to `name:alias` instead of the alias ARN and (b)
//! `parse_lambda_function_name` didn't strip the trailing qualifier. The
//! function is never invoked, so no container runtime is needed.

mod helpers;

use aws_sdk_cloudformation::types::{Capability, OnFailure};
use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Role": {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "cfn-alias-esm-role",
        "AssumeRolePolicyDocument": {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
      }
    },
    "Func": {
      "Type": "AWS::Lambda::Function",
      "Properties": {
        "FunctionName": "cfn-alias-esm-func",
        "Runtime": "nodejs18.x",
        "Handler": "index.handler",
        "Role": {"Fn::GetAtt": ["Role", "Arn"]},
        "Code": {"ZipFile": "exports.handler = async () => ({ ok: true });"}
      }
    },
    "Queue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "cfn-alias-esm-queue"}
    },
    "Version": {
      "Type": "AWS::Lambda::Version",
      "Properties": {
        "FunctionName": {"Ref": "Func"},
        "Description": "v1 snapshot"
      }
    },
    "Alias": {
      "Type": "AWS::Lambda::Alias",
      "Properties": {
        "FunctionName": {"Ref": "Func"},
        "Name": "live",
        "FunctionVersion": {"Fn::GetAtt": ["Version", "Version"]}
      }
    },
    "Esm": {
      "Type": "AWS::Lambda::EventSourceMapping",
      "Properties": {
        "FunctionName": {"Ref": "Alias"},
        "EventSourceArn": {"Fn::GetAtt": ["Queue", "Arn"]},
        "BatchSize": 5,
        "Enabled": true
      }
    }
  },
  "Outputs": {
    "FuncName": {"Value": {"Ref": "Func"}},
    "AliasArn": {"Value": {"Ref": "Alias"}},
    "EsmId": {"Value": {"Ref": "Esm"}}
  }
}"#;

#[tokio::test]
async fn cfn_event_source_mapping_referencing_lambda_alias_provisions() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let lambda = aws_sdk_lambda::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("cfn-alias-esm-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityNamedIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack");

    // The whole point: the stack reaches CREATE_COMPLETE instead of rolling
    // back on the "Function cfn-alias-esm-func:live does not exist" error.
    let described = cfn
        .describe_stacks()
        .stack_name("cfn-alias-esm-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // `Ref` on the alias yields the alias ARN, like real CloudFormation.
    let mut func_name = None;
    let mut alias_arn = None;
    let mut esm_id = None;
    for o in stack.outputs() {
        match o.output_key() {
            Some("FuncName") => func_name = o.output_value().map(String::from),
            Some("AliasArn") => alias_arn = o.output_value().map(String::from),
            Some("EsmId") => esm_id = o.output_value().map(String::from),
            _ => {}
        }
    }
    let func_name = func_name.expect("FuncName output");
    let alias_arn = alias_arn.expect("AliasArn output");
    let esm_id = esm_id.expect("EsmId output");
    assert_eq!(func_name, "cfn-alias-esm-func");
    assert!(
        alias_arn.starts_with("arn:aws:lambda:")
            && alias_arn.ends_with(":function:cfn-alias-esm-func:live"),
        "Ref on the alias should resolve to the alias ARN, got {alias_arn}"
    );

    // The ESM exists and is wired to the underlying function (the qualifier
    // was stripped to resolve to the function, matching AWS).
    let listed = lambda
        .list_event_source_mappings()
        .function_name(&func_name)
        .send()
        .await
        .expect("list_event_source_mappings");
    let mapping = listed
        .event_source_mappings()
        .iter()
        .find(|m| m.uuid() == Some(esm_id.as_str()))
        .expect("ESM present for the function");
    assert!(
        mapping
            .function_arn()
            .expect("function_arn")
            .ends_with(":function:cfn-alias-esm-func"),
        "ESM should resolve to the underlying function, got {:?}",
        mapping.function_arn()
    );
}
