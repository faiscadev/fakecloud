mod helpers;

use helpers::TestServer;

/// Stack, template, parameters and tags survive a restart.
#[tokio::test]
async fn persistence_round_trip_stack() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let cf = server.cloudformation_client().await;

    let template = r#"{
        "Parameters": { "QueueName": { "Type": "String" } },
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": { "Ref": "QueueName" } }
            }
        }
    }"#;

    cf.create_stack()
        .stack_name("persist-stack")
        .template_body(template)
        .parameters(
            aws_sdk_cloudformation::types::Parameter::builder()
                .parameter_key("QueueName")
                .parameter_value("persist-queue")
                .build(),
        )
        .tags(
            aws_sdk_cloudformation::types::Tag::builder()
                .key("env")
                .value("prod")
                .build(),
        )
        .send()
        .await
        .unwrap();

    drop(cf);
    server.restart().await;
    let cf = server.cloudformation_client().await;

    let described = cf
        .describe_stacks()
        .stack_name("persist-stack")
        .send()
        .await
        .unwrap();
    let stacks = described.stacks();
    assert_eq!(stacks.len(), 1);
    let stack = &stacks[0];
    assert_eq!(stack.stack_name(), Some("persist-stack"));
    assert!(stack
        .tags()
        .iter()
        .any(|t| t.key() == Some("env") && t.value() == Some("prod")));
    assert!(stack
        .parameters()
        .iter()
        .any(|p| p.parameter_key() == Some("QueueName")
            && p.parameter_value() == Some("persist-queue")));

    // Template body recoverable.
    let got_template = cf
        .get_template()
        .stack_name("persist-stack")
        .send()
        .await
        .unwrap();
    assert!(got_template
        .template_body()
        .unwrap_or_default()
        .contains("MyQueue"));

    // Resources list still populated.
    let resources = cf
        .list_stack_resources()
        .stack_name("persist-stack")
        .send()
        .await
        .unwrap();
    assert!(resources
        .stack_resource_summaries()
        .iter()
        .any(|r| r.logical_resource_id() == Some("MyQueue")));
}

/// Deletion survives a restart: a deleted stack does not reappear.
#[tokio::test]
async fn persistence_deletion_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let cf = server.cloudformation_client().await;

    let template = r#"{
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "to-delete" }
            }
        }
    }"#;

    cf.create_stack()
        .stack_name("doomed")
        .template_body(template)
        .send()
        .await
        .unwrap();

    cf.delete_stack().stack_name("doomed").send().await.unwrap();

    drop(cf);
    server.restart().await;
    let cf = server.cloudformation_client().await;

    let listed = cf.list_stacks().send().await.unwrap();
    assert!(!listed
        .stack_summaries()
        .iter()
        .any(|s| s.stack_name() == Some("doomed")
            && s.stack_status()
                != Some(&aws_sdk_cloudformation::types::StackStatus::DeleteComplete)));
}

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Answers Create at once and Delete after a few seconds, so a restart can
/// land while a stack delete waits on it.
const SLOW_DELETE_HANDLER: &str = r#"
import json, time, urllib.request

def lambda_handler(event, context):
    if event["RequestType"] == "Delete":
        time.sleep(6)
    body = json.dumps({
        "Status": "SUCCESS",
        "PhysicalResourceId": "custom-" + event["LogicalResourceId"],
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": {},
    }).encode()
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT")
    req.add_header("Content-Type", "")
    urllib.request.urlopen(req)
"#;

async fn stack_status(cf: &aws_sdk_cloudformation::Client, name: &str) -> Option<String> {
    cf.list_stacks()
        .send()
        .await
        .unwrap()
        .stack_summaries()
        .iter()
        .find(|s| s.stack_name() == Some(name))
        .and_then(|s| s.stack_status())
        .map(|s| s.as_str().to_string())
}

async fn wait_for_status(cf: &aws_sdk_cloudformation::Client, name: &str, status: &str) {
    let mut last = None;
    for _ in 0..120 {
        last = stack_status(cf, name).await;
        if last.as_deref() == Some(status) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let events = cf
        .describe_stack_events()
        .stack_name(name)
        .send()
        .await
        .map(|e| {
            e.stack_events()
                .iter()
                .map(|e| {
                    format!(
                        "{:?} {:?} {:?}",
                        e.logical_resource_id(),
                        e.resource_status(),
                        e.resource_status_reason()
                    )
                })
                .collect::<Vec<_>>()
        });
    panic!("{name} never reached {status} (last {last:?}); events {events:?}");
}

/// A restart while a stack delete waits on a custom resource handler: the
/// delete carries on after the restart instead of leaving the stack stuck
/// in DELETE_IN_PROGRESS, and the stack's name is free again afterwards.
#[tokio::test]
async fn a_stack_delete_interrupted_by_a_restart_finishes() {
    if !docker_available() {
        if std::env::var("CI").is_ok() {
            panic!("docker is required for a_stack_delete_interrupted_by_a_restart_finishes in CI");
        }
        eprintln!(
            "skipping a_stack_delete_interrupted_by_a_restart_finishes: docker is not available"
        );
        return;
    }
    use std::io::Write;
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().display().to_string();
    // Persistent, with the container runtime the handler needs.
    let mut server = TestServer::start_full(
        &[],
        &["--storage-mode", "persistent", "--data-path", &data_path],
    )
    .await;
    let lambda = server.lambda_client().await;
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file("index.py", zip::write::SimpleFileOptions::default())
        .unwrap();
    writer.write_all(SLOW_DELETE_HANDLER.as_bytes()).unwrap();
    let zip = writer.finish().unwrap().into_inner();
    lambda
        .create_function()
        .function_name("slow-delete-handler")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.lambda_handler")
        .timeout(30)
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(zip))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let template = serde_json::json!({
        "Resources": {
            "Thing": {
                "Type": "Custom::Thing",
                "Properties": {
                    "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:slow-delete-handler",
                    "ServiceTimeout": 60
                }
            }
        }
    })
    .to_string();
    let cf = server.cloudformation_client().await;
    cf.create_stack()
        .stack_name("interrupted-delete")
        .template_body(&template)
        .send()
        .await
        .unwrap();
    wait_for_status(&cf, "interrupted-delete", "CREATE_COMPLETE").await;
    cf.delete_stack()
        .stack_name("interrupted-delete")
        .send()
        .await
        .unwrap();
    assert_eq!(
        stack_status(&cf, "interrupted-delete").await.as_deref(),
        Some("DELETE_IN_PROGRESS")
    );

    drop(cf);
    server.restart().await;
    let cf = server.cloudformation_client().await;
    wait_for_status(&cf, "interrupted-delete", "DELETE_COMPLETE").await;
    cf.create_stack()
        .stack_name("interrupted-delete")
        .template_body(&template)
        .send()
        .await
        .expect("the name is free again");
    let events = cf
        .describe_stack_events()
        .stack_name("interrupted-delete")
        .send()
        .await
        .unwrap();
    assert!(events
        .stack_events()
        .iter()
        .all(|e| e.resource_status().map(|s| s.as_str()) != Some("DELETE_FAILED")));
}
