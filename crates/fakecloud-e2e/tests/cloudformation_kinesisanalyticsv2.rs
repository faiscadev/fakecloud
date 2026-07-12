//! CloudFormation provisions `AWS::KinesisAnalyticsV2::*` resources as real
//! records in the `kinesisanalyticsv2` (Amazon Managed Service for Apache Flink)
//! control plane: a SQL application (settling to READY), plus an
//! `ApplicationCloudWatchLoggingOption` and an `ApplicationOutput` sub-resource
//! that reference the application via `Ref`. Each reads back through the
//! kinesisanalyticsv2 API (`DescribeApplication`), proving write-through: the
//! CFN-provisioned application stores the same wire state the direct
//! `CreateApplication` handler does (RuntimeEnvironment + ServiceExecutionRole +
//! the added logging option + output), and the stack `Outputs` surface each
//! resource's `Ref`.
//!
//! Runs with the Flink container backend DISABLED
//! (`FAKECLOUD_KINESISANALYTICSV2_DISABLE_BACKEND=1`) so no real Flink cluster is
//! spawned -- `CreateApplication` never starts a job anyway, so this is a pure
//! control-plane / Ref / write-through test in the shared E2E partition.

mod helpers;

use helpers::TestServer;

const ROLE: &str = "arn:aws:iam::123456789012:role/service-role/kinesis-analytics";

// A SQL application plus a CloudWatch logging option and an output, both
// referencing the application by Ref (which resolves to the application name =
// the application's physical id). The multi-pass provisioner must therefore
// order the application first.
const TEMPLATE: &str = r#"{
  "Resources": {
    "App": {
      "Type": "AWS::KinesisAnalyticsV2::Application",
      "Properties": {
        "ApplicationName": "cfn-ka2-app",
        "RuntimeEnvironment": "SQL-1_0",
        "ServiceExecutionRole": "arn:aws:iam::123456789012:role/service-role/kinesis-analytics",
        "ApplicationDescription": "provisioned by cloudformation",
        "Tags": [{ "Key": "env", "Value": "test" }]
      }
    },
    "Logging": {
      "Type": "AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption",
      "Properties": {
        "ApplicationName": { "Ref": "App" },
        "CloudWatchLoggingOption": {
          "LogStreamARN": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/ka2:log-stream:app"
        }
      }
    },
    "Output": {
      "Type": "AWS::KinesisAnalyticsV2::ApplicationOutput",
      "Properties": {
        "ApplicationName": { "Ref": "App" },
        "Output": {
          "Name": "DESTINATION_STREAM",
          "KinesisStreamsOutput": {
            "ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/out-stream"
          },
          "DestinationSchema": { "RecordFormatType": "JSON" }
        }
      }
    }
  },
  "Outputs": {
    "AppRef":     { "Value": { "Ref": "App" } },
    "LoggingRef": { "Value": { "Ref": "Logging" } },
    "OutputRef":  { "Value": { "Ref": "Output" } }
  }
}"#;

fn output<'a>(stack: &'a aws_sdk_cloudformation::types::Stack, key: &str) -> &'a str {
    stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some(key))
        .and_then(|o| o.output_value())
        .unwrap_or_else(|| panic!("missing output {key}"))
}

#[tokio::test]
async fn cfn_provisions_kinesisanalyticsv2_resources_control_plane() {
    let s =
        TestServer::start_with_env(&[("FAKECLOUD_KINESISANALYTICSV2_DISABLE_BACKEND", "1")]).await;
    let cfg = s.aws_config().await;
    let cfn = s.cloudformation_client().await;
    let ka2 = aws_sdk_kinesisanalyticsv2::Client::new(&cfg);

    cfn.create_stack()
        .stack_name("ka2-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("ka2-stack")
        .send()
        .await
        .unwrap();
    let stack = &described.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // --- Ref resolution: the application's Ref is its name (its physical id);
    //     the sub-resources' Refs are their minted numeric ids. ---
    assert_eq!(output(stack, "AppRef"), "cfn-ka2-app");
    assert!(
        !output(stack, "LoggingRef").is_empty(),
        "logging Ref should be the minted logging-option id"
    );
    assert!(
        !output(stack, "OutputRef").is_empty(),
        "output Ref should be the minted output id"
    );

    // --- Write-through: DescribeApplication reads back the exact wire state the
    //     direct CreateApplication + AddApplication* handlers store. ---
    let desc = ka2
        .describe_application()
        .application_name("cfn-ka2-app")
        .send()
        .await
        .expect("describe application");
    let detail = desc.application_detail().expect("application detail");
    assert_eq!(detail.runtime_environment().as_str(), "SQL-1_0");
    assert_eq!(detail.service_execution_role(), Some(ROLE));
    assert_eq!(
        detail.application_description(),
        Some("provisioned by cloudformation")
    );

    // The CloudWatch logging option was written through and reads back.
    let logs = detail.cloud_watch_logging_option_descriptions();
    assert_eq!(logs.len(), 1, "one cloudwatch logging option");
    assert_eq!(
        logs[0].log_stream_arn(),
        "arn:aws:logs:us-east-1:123456789012:log-group:/aws/ka2:log-stream:app"
    );
    assert_eq!(
        logs[0].cloud_watch_logging_option_id(),
        Some(output(stack, "LoggingRef")),
        "logging option id matches its Ref"
    );

    // The output was written through under the SQL application configuration.
    let outputs = detail
        .application_configuration_description()
        .and_then(|c| c.sql_application_configuration_description())
        .map(|s| s.output_descriptions())
        .unwrap_or_default();
    assert_eq!(outputs.len(), 1, "one output description");
    assert_eq!(outputs[0].name(), Some("DESTINATION_STREAM"));
    assert_eq!(
        outputs[0].output_id(),
        Some(output(stack, "OutputRef")),
        "output id matches its Ref"
    );

    // --- The tag applied on the Application resource persisted. ---
    let tags = ka2
        .list_tags_for_resource()
        .resource_arn(detail.application_arn())
        .send()
        .await
        .expect("list tags");
    assert!(
        tags.tags()
            .iter()
            .any(|t| t.key() == "env" && t.value() == Some("test")),
        "env=test tag present"
    );

    // --- Deleting the stack removes the application from the ka2 control plane. ---
    cfn.delete_stack()
        .stack_name("ka2-stack")
        .send()
        .await
        .expect("delete_stack");

    let gone = ka2
        .describe_application()
        .application_name("cfn-ka2-app")
        .send()
        .await;
    assert!(
        gone.is_err(),
        "application should be gone after stack delete"
    );
}
