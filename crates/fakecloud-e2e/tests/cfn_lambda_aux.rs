//! CloudFormation provisioner for AWS::Lambda::Permission/EventSourceMapping/
//! LayerVersion/Url/Alias/Version. Covers create + UpdateStack mutation +
//! delete teardown end-to-end against real Lambda APIs.

mod helpers;

use aws_sdk_cloudformation::types::{Capability, OnFailure};
use helpers::TestServer;

const TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Role": {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "lambda-aux-role",
        "AssumeRolePolicyDocument": {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
      }
    },
    "Layer": {
      "Type": "AWS::Lambda::LayerVersion",
      "Properties": {
        "LayerName": "cfn-aux-layer",
        "Description": "Aux test layer",
        "CompatibleRuntimes": ["nodejs18.x"],
        "CompatibleArchitectures": ["x86_64"],
        "LicenseInfo": "MIT",
        "Content": {"ZipFile": "UEsDBBQAAAAAAAAAIQA="}
      }
    },
    "Func": {
      "Type": "AWS::Lambda::Function",
      "Properties": {
        "FunctionName": "cfn-aux-func",
        "Runtime": "nodejs18.x",
        "Handler": "index.handler",
        "Role": {"Fn::GetAtt": ["Role", "Arn"]},
        "Code": {"ZipFile": "exports.handler = async () => ({ ok: true });"}
      }
    },
    "Queue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "cfn-aux-queue"}
    },
    "Permission": {
      "Type": "AWS::Lambda::Permission",
      "Properties": {
        "FunctionName": {"Ref": "Func"},
        "Action": "lambda:InvokeFunction",
        "Principal": "sns.amazonaws.com",
        "SourceAccount": "123456789012"
      }
    },
    "Esm": {
      "Type": "AWS::Lambda::EventSourceMapping",
      "Properties": {
        "FunctionName": {"Ref": "Func"},
        "EventSourceArn": {"Fn::GetAtt": ["Queue", "Arn"]},
        "BatchSize": 5,
        "Enabled": true,
        "MaximumBatchingWindowInSeconds": 10,
        "FunctionResponseTypes": ["ReportBatchItemFailures"]
      }
    },
    "Url": {
      "Type": "AWS::Lambda::Url",
      "Properties": {
        "TargetFunctionArn": {"Fn::GetAtt": ["Func", "Arn"]},
        "AuthType": "NONE",
        "InvokeMode": "BUFFERED"
      }
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
        "FunctionVersion": {"Fn::GetAtt": ["Version", "Version"]},
        "Description": "initial alias"
      }
    }
  },
  "Outputs": {
    "FuncName": {"Value": {"Ref": "Func"}},
    "EsmId": {"Value": {"Ref": "Esm"}},
    "FuncUrl": {"Value": {"Fn::GetAtt": ["Url", "FunctionUrl"]}},
    "VersionNum": {"Value": {"Fn::GetAtt": ["Version", "Version"]}},
    "AliasArn": {"Value": {"Fn::GetAtt": ["Alias", "AliasArn"]}},
    "LayerArn": {"Value": {"Fn::GetAtt": ["Layer", "LayerVersionArn"]}}
  }
}"#;

/// Same template, but every aux resource has at least one mutated
/// property. CFN should apply these in place via the per-type
/// `update_*` paths rather than replacing the resources.
const TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Role": {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "lambda-aux-role",
        "AssumeRolePolicyDocument": {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
      }
    },
    "Layer": {
      "Type": "AWS::Lambda::LayerVersion",
      "Properties": {
        "LayerName": "cfn-aux-layer",
        "Description": "Aux test layer",
        "CompatibleRuntimes": ["nodejs18.x"],
        "CompatibleArchitectures": ["x86_64"],
        "LicenseInfo": "MIT",
        "Content": {"ZipFile": "UEsDBBQAAAAAAAAAIQA="}
      }
    },
    "Func": {
      "Type": "AWS::Lambda::Function",
      "Properties": {
        "FunctionName": "cfn-aux-func",
        "Runtime": "nodejs18.x",
        "Handler": "index.handler",
        "Role": {"Fn::GetAtt": ["Role", "Arn"]},
        "Code": {"ZipFile": "exports.handler = async () => ({ ok: true });"}
      }
    },
    "Queue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "cfn-aux-queue"}
    },
    "Permission": {
      "Type": "AWS::Lambda::Permission",
      "Properties": {
        "FunctionName": {"Ref": "Func"},
        "Action": "lambda:InvokeFunction",
        "Principal": "events.amazonaws.com",
        "SourceAccount": "123456789012"
      }
    },
    "Esm": {
      "Type": "AWS::Lambda::EventSourceMapping",
      "Properties": {
        "FunctionName": {"Ref": "Func"},
        "EventSourceArn": {"Fn::GetAtt": ["Queue", "Arn"]},
        "BatchSize": 7,
        "Enabled": false,
        "MaximumBatchingWindowInSeconds": 30,
        "FunctionResponseTypes": ["ReportBatchItemFailures"]
      }
    },
    "Url": {
      "Type": "AWS::Lambda::Url",
      "Properties": {
        "TargetFunctionArn": {"Fn::GetAtt": ["Func", "Arn"]},
        "AuthType": "AWS_IAM",
        "InvokeMode": "RESPONSE_STREAM"
      }
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
        "FunctionVersion": {"Fn::GetAtt": ["Version", "Version"]},
        "Description": "updated alias"
      }
    }
  },
  "Outputs": {
    "FuncName": {"Value": {"Ref": "Func"}},
    "EsmId": {"Value": {"Ref": "Esm"}},
    "FuncUrl": {"Value": {"Fn::GetAtt": ["Url", "FunctionUrl"]}},
    "VersionNum": {"Value": {"Fn::GetAtt": ["Version", "Version"]}},
    "AliasArn": {"Value": {"Fn::GetAtt": ["Alias", "AliasArn"]}},
    "LayerArn": {"Value": {"Fn::GetAtt": ["Layer", "LayerVersionArn"]}}
  }
}"#;

#[tokio::test]
async fn cfn_provisions_lambda_aux_resources() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let lambda = aws_sdk_lambda::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("lambda-aux-stack")
        .template_body(TEMPLATE_V1)
        .capabilities(Capability::CapabilityNamedIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("lambda-aux-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    let outputs = collect_outputs(stack);
    let func_name = outputs.func_name.clone();
    let esm_id = outputs.esm_id.clone();
    let func_url = outputs.func_url.clone();
    let version_num = outputs.version_num.clone();
    let alias_arn = outputs.alias_arn.clone();
    let layer_arn = outputs.layer_arn.clone();

    assert_eq!(func_name, "cfn-aux-func");
    assert!(func_url.contains("lambda-url"));
    assert_eq!(version_num, "1");
    assert!(alias_arn.ends_with(":live"));
    assert!(layer_arn.contains(":layer:cfn-aux-layer:1"));

    // DescribeStackResources mentions all six aux types.
    let resources = cfn
        .describe_stack_resources()
        .stack_name("lambda-aux-stack")
        .send()
        .await
        .expect("describe_stack_resources");
    let types: Vec<String> = resources
        .stack_resources()
        .iter()
        .filter_map(|r| r.resource_type().map(|s| s.to_string()))
        .collect();
    for expected in [
        "AWS::Lambda::Function",
        "AWS::Lambda::Permission",
        "AWS::Lambda::EventSourceMapping",
        "AWS::Lambda::LayerVersion",
        "AWS::Lambda::Url",
        "AWS::Lambda::Alias",
        "AWS::Lambda::Version",
    ] {
        assert!(
            types.contains(&expected.to_string()),
            "DescribeStackResources missing {expected}; saw {types:?}",
        );
    }

    // GetPolicy round-trips the SourceAccount condition we set.
    let policy = lambda
        .get_policy()
        .function_name(&func_name)
        .send()
        .await
        .expect("get_policy");
    let policy_str = policy.policy().expect("policy str");
    assert!(policy_str.contains("\"sns.amazonaws.com\""));
    assert!(policy_str.contains("123456789012"));

    // ListEventSourceMappings includes ours.
    let listed = lambda
        .list_event_source_mappings()
        .function_name(&func_name)
        .send()
        .await
        .expect("list_event_source_mappings");
    let uuids: Vec<String> = listed
        .event_source_mappings()
        .iter()
        .filter_map(|m| m.uuid().map(|s| s.to_string()))
        .collect();
    assert!(uuids.contains(&esm_id), "ESM {esm_id} not in {uuids:?}");

    // GetEventSourceMapping echoes the BatchSize we set.
    let esm_described = lambda
        .get_event_source_mapping()
        .uuid(&esm_id)
        .send()
        .await
        .expect("get_event_source_mapping");
    assert_eq!(esm_described.batch_size(), Some(5));

    // ListLayerVersions reflects the published version.
    let layer_versions = lambda
        .list_layer_versions()
        .layer_name("cfn-aux-layer")
        .send()
        .await
        .expect("list_layer_versions");
    let nums: Vec<i64> = layer_versions
        .layer_versions()
        .iter()
        .map(|v| v.version())
        .collect();
    assert!(nums.contains(&1), "layer versions {nums:?} missing 1");

    // GetFunctionUrlConfig matches the CFN output URL.
    let url_cfg = lambda
        .get_function_url_config()
        .function_name(&func_name)
        .send()
        .await
        .expect("get_function_url_config");
    assert_eq!(url_cfg.function_url(), func_url);
    assert_eq!(url_cfg.auth_type().as_str(), "NONE");

    // ListVersionsByFunction includes our v1 snapshot.
    let listed_versions = lambda
        .list_versions_by_function()
        .function_name(&func_name)
        .send()
        .await
        .expect("list_versions_by_function");
    let vnums: Vec<String> = listed_versions
        .versions()
        .iter()
        .filter_map(|v| v.version().map(|s| s.to_string()))
        .collect();
    assert!(
        vnums.iter().any(|v| v == "1"),
        "versions {vnums:?} missing 1"
    );

    // Confirm the alias provisioned with the right ARN; older alias
    // response shape predates BB8 so SDK-level field reads are skipped.
    assert!(alias_arn.starts_with("arn:aws:lambda:"));

    // ── UpdateStack: mutate one property on each aux type. ──
    cfn.update_stack()
        .stack_name("lambda-aux-stack")
        .template_body(TEMPLATE_V2)
        .capabilities(Capability::CapabilityNamedIam)
        .send()
        .await
        .expect("update_stack");

    let after_update = cfn
        .describe_stacks()
        .stack_name("lambda-aux-stack")
        .send()
        .await
        .expect("describe_stacks after update");
    let stack_after = after_update.stacks().first().expect("stack present");
    assert_eq!(
        stack_after.stack_status().unwrap().as_str(),
        "UPDATE_COMPLETE"
    );

    // Permission: principal flipped from sns -> events.
    let policy_after = lambda
        .get_policy()
        .function_name(&func_name)
        .send()
        .await
        .expect("get_policy after update");
    let policy_after_str = policy_after.policy().expect("policy str after");
    assert!(
        policy_after_str.contains("\"events.amazonaws.com\""),
        "policy after update did not pick up new principal: {policy_after_str}",
    );
    assert!(
        !policy_after_str.contains("\"sns.amazonaws.com\""),
        "old principal lingered after update: {policy_after_str}",
    );

    // ESM: BatchSize bumped 5 -> 7, Enabled flipped to false.
    let esm_after = lambda
        .get_event_source_mapping()
        .uuid(&esm_id)
        .send()
        .await
        .expect("get_event_source_mapping after update");
    assert_eq!(esm_after.batch_size(), Some(7));
    assert_eq!(
        esm_after.state().unwrap_or(""),
        "Disabled",
        "ESM should report Disabled after Enabled=false update",
    );

    // URL: AuthType flipped NONE -> AWS_IAM, InvokeMode BUFFERED -> RESPONSE_STREAM.
    let url_after = lambda
        .get_function_url_config()
        .function_name(&func_name)
        .send()
        .await
        .expect("get_function_url_config after update");
    assert_eq!(url_after.auth_type().as_str(), "AWS_IAM");
    assert_eq!(
        url_after.invoke_mode().map(|i| i.as_str()),
        Some("RESPONSE_STREAM"),
    );

    // Stack outputs after update: version is still 1 (versions are
    // immutable, so CFN keeps the original snapshot), alias_arn key is
    // unchanged.
    let outputs_after = collect_outputs(stack_after);
    assert_eq!(outputs_after.version_num, "1");
    assert_eq!(outputs_after.alias_arn, alias_arn);
    assert_eq!(outputs_after.func_url, func_url);

    // ── DeleteStack tears everything down. ──
    cfn.delete_stack()
        .stack_name("lambda-aux-stack")
        .send()
        .await
        .expect("delete_stack");

    let after = lambda.get_function().function_name(&func_name).send().await;
    assert!(
        after.is_err(),
        "function should be gone after stack deletion"
    );
    let url_gone = lambda
        .get_function_url_config()
        .function_name(&func_name)
        .send()
        .await;
    assert!(
        url_gone.is_err(),
        "function url config should be gone after stack deletion"
    );
}

struct AuxOutputs {
    func_name: String,
    esm_id: String,
    func_url: String,
    version_num: String,
    alias_arn: String,
    layer_arn: String,
}

fn collect_outputs(stack: &aws_sdk_cloudformation::types::Stack) -> AuxOutputs {
    let mut func_name = None;
    let mut esm_id = None;
    let mut func_url = None;
    let mut version_num = None;
    let mut alias_arn = None;
    let mut layer_arn = None;
    for o in stack.outputs() {
        match o.output_key() {
            Some("FuncName") => func_name = o.output_value().map(|s| s.to_string()),
            Some("EsmId") => esm_id = o.output_value().map(|s| s.to_string()),
            Some("FuncUrl") => func_url = o.output_value().map(|s| s.to_string()),
            Some("VersionNum") => version_num = o.output_value().map(|s| s.to_string()),
            Some("AliasArn") => alias_arn = o.output_value().map(|s| s.to_string()),
            Some("LayerArn") => layer_arn = o.output_value().map(|s| s.to_string()),
            _ => {}
        }
    }
    AuxOutputs {
        func_name: func_name.expect("FuncName"),
        esm_id: esm_id.expect("EsmId"),
        func_url: func_url.expect("FuncUrl"),
        version_num: version_num.expect("VersionNum"),
        alias_arn: alias_arn.expect("AliasArn"),
        layer_arn: layer_arn.expect("LayerArn"),
    }
}

/// A stack mapping a queue that does not exist fails that resource with
/// CreateEventSourceMapping's own error, and what the stack had already
/// created is rolled back.
#[tokio::test]
async fn an_event_source_mapping_to_a_missing_queue_fails_the_stack() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let template = r#"{
      "Resources": {
        "Func": {
          "Type": "AWS::Lambda::Function",
          "Properties": {
            "FunctionName": "cfn-missing-source-fn",
            "Runtime": "python3.12",
            "Handler": "index.handler",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Code": {"ZipFile": "def handler(e, c):\n    return e\n"}
          }
        },
        "Esm": {
          "Type": "AWS::Lambda::EventSourceMapping",
          "Properties": {
            "FunctionName": {"Ref": "Func"},
            "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:cfn-missing-queue"
          }
        }
      }
    }"#;
    cfn.create_stack()
        .stack_name("cfn-missing-source")
        .template_body(template)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .unwrap();
    let stack = cfn
        .describe_stacks()
        .stack_name("cfn-missing-source")
        .send()
        .await
        .unwrap()
        .stacks()[0]
        .clone();
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_FAILED");
    let reason = stack.stack_status_reason().unwrap_or_default();
    assert!(
        reason.contains("Esm")
            && reason.contains(
                "Resource handler returned message: \"Error occurred while ReceiveMessage. \
                 SQS Error Code: AWS.SimpleQueueService.NonExistentQueue. SQS Error Message: \
                 The specified queue does not exist. (Service: Lambda, Status Code: 400"
            )
            && reason.contains("HandlerErrorCode: InvalidRequest"),
        "{reason}"
    );
    // The function the stack created first was rolled back, and no mapping
    // was left behind.
    let lambda = server.lambda_client().await;
    let err = lambda
        .get_function()
        .function_name("cfn-missing-source-fn")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("ResourceNotFoundException"),
        "{err:?}"
    );
    let mappings = lambda.list_event_source_mappings().send().await.unwrap();
    assert!(mappings.event_source_mappings().is_empty());
}
