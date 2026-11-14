//! Nested Step Functions executions through `states:startExecution` and its
//! `.sync`, `.sync:2` and `.waitForTaskToken` variants, and through
//! `aws-sdk:sfn:*`. A state machine definition names parameters and reads
//! results in PascalCase, unlike the camelCase Step Functions wire format.

mod helpers;

use aws_sdk_sfn::error::ProvideErrorMetadata;
use helpers::TestServer;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

const ROLE: &str = "arn:aws:iam::123456789012:role/sfn-role";

async fn create_machine(client: &aws_sdk_sfn::Client, name: &str, definition: Value) -> String {
    client
        .create_state_machine()
        .name(name)
        .definition(definition.to_string())
        .role_arn(ROLE)
        .send()
        .await
        .unwrap()
        .state_machine_arn()
        .to_string()
}

/// A child that returns its input under `received`.
async fn create_echo_child(client: &aws_sdk_sfn::Client) -> String {
    create_machine(
        client,
        "child",
        json!({"StartAt":"P","States":{"P":{
            "Type":"Pass",
            "Parameters":{"received.$":"$"},
            "End":true
        }}}),
    )
    .await
}

async fn wait_terminal(
    client: &aws_sdk_sfn::Client,
    arn: &str,
) -> aws_sdk_sfn::operation::describe_execution::DescribeExecutionOutput {
    for _ in 0..400 {
        let desc = client
            .describe_execution()
            .execution_arn(arn)
            .send()
            .await
            .unwrap();
        if desc.status().as_str() != "RUNNING" {
            return desc;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("execution did not finish: {arn}");
}

async fn run_parent(client: &aws_sdk_sfn::Client, definition: Value) -> Value {
    let arn = create_machine(client, "parent", definition).await;
    let started = client
        .start_execution()
        .state_machine_arn(arn)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(client, started.execution_arn()).await;
    assert_eq!(
        desc.status().as_str(),
        "SUCCEEDED",
        "error={:?} cause={:?}",
        desc.error(),
        desc.cause()
    );
    serde_json::from_str(desc.output().expect("output")).unwrap()
}

fn nested_task(resource_suffix: &str, parameters: Value) -> Value {
    json!({"StartAt":"N","States":{"N":{
        "Type":"Task",
        "Resource": format!("arn:aws:states:::{resource_suffix}"),
        "Parameters": parameters,
        "End":true
    }}})
}

#[tokio::test]
async fn sync2_takes_pascal_case_parameters_and_returns_output_as_json() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;

    let output = run_parent(
        &client,
        nested_task(
            "states:startExecution.sync:2",
            json!({"StateMachineArn": child, "Input": {"a": 1}, "Name": "named-child"}),
        ),
    )
    .await;

    assert_eq!(output["Status"], "SUCCEEDED", "{output}");
    assert_eq!(output["StateMachineArn"], child);
    assert_eq!(output["Name"], "named-child");
    assert!(output["ExecutionArn"]
        .as_str()
        .unwrap()
        .ends_with(":named-child"));
    assert_eq!(output["Input"], json!({"a": 1}));
    assert_eq!(output["Output"], json!({"received": {"a": 1}}));
    // Epoch milliseconds, as an integer, and exactly the recorded fields.
    assert_eq!(output.as_object().unwrap().len(), 13, "{output}");
    assert_eq!(output["RedriveCount"], 0);
    assert_eq!(output["RedriveStatus"], "NOT_REDRIVABLE");
    assert_eq!(
        output["RedriveStatusReason"],
        "Execution is SUCCEEDED and cannot be redriven"
    );
    assert!(
        output["StartDate"].as_i64() > Some(1_000_000_000_000),
        "{output}"
    );
    assert!(
        output["StopDate"].as_i64() > Some(1_000_000_000_000),
        "{output}"
    );
    assert_eq!(output["InputDetails"], json!({"Included": true}));
    assert_eq!(output["OutputDetails"], json!({"Included": true}));
}

#[tokio::test]
async fn sync_returns_output_as_an_escaped_string() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;

    let output = run_parent(
        &client,
        nested_task(
            "states:startExecution.sync",
            json!({"StateMachineArn": child, "Input": {"a": 1}}),
        ),
    )
    .await;

    assert_eq!(output["Status"], "SUCCEEDED", "{output}");
    let inner: Value = serde_json::from_str(output["Output"].as_str().unwrap()).unwrap();
    assert_eq!(inner, json!({"received": {"a": 1}}));
    let input: Value = serde_json::from_str(output["Input"].as_str().unwrap()).unwrap();
    assert_eq!(input, json!({"a": 1}));
}

#[tokio::test]
async fn result_selector_reads_the_documented_output_field() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;

    let output = run_parent(
        &client,
        json!({"StartAt":"N","States":{"N":{
            "Type":"Task",
            "Resource":"arn:aws:states:::states:startExecution.sync:2",
            "Parameters":{"StateMachineArn": child, "Input.$": "$"},
            "ResultSelector":{"childResult.$":"$.Output.received"},
            "End":true
        }}}),
    )
    .await;

    assert_eq!(output, json!({"childResult": {}}));
}

#[tokio::test]
async fn string_input_reaches_the_child_unchanged() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;

    let output = run_parent(
        &client,
        nested_task(
            "states:startExecution.sync:2",
            json!({"StateMachineArn": child, "Input": "{\"from\":\"string\"}"}),
        ),
    )
    .await;

    assert_eq!(output["Output"], json!({"received": {"from": "string"}}));
}

#[tokio::test]
async fn sync_child_failure_fails_the_parent_task() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_machine(
        &client,
        "failing-child",
        json!({"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"child broke"}}}),
    )
    .await;

    let parent = create_machine(
        &client,
        "parent",
        nested_task(
            "states:startExecution.sync:2",
            json!({"StateMachineArn": child}),
        ),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(parent)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;

    assert_eq!(desc.status().as_str(), "FAILED");
    assert_eq!(desc.error(), Some("States.TaskFailed"));
    // Recorded: the cause is the child's description, for both `.sync` forms.
    let cause: Value = serde_json::from_str(desc.cause().unwrap()).unwrap();
    assert_eq!(cause["Error"], "Boom");
    assert_eq!(cause["Cause"], "child broke");
    assert_eq!(cause["Status"], "FAILED");
    assert_eq!(cause["RedriveStatus"], "REDRIVABLE");
    assert_eq!(cause["RedriveCount"], 0);
    assert_eq!(cause["StateMachineArn"], child);
    assert_eq!(cause["InputDetails"]["Included"], true);
    assert!(
        cause["StartDate"].as_i64() > Some(1_000_000_000_000),
        "{cause}"
    );
    assert!(cause["ExecutionArn"].is_string());
}

#[tokio::test]
async fn fire_and_forget_returns_the_start_execution_shape() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;

    let output = run_parent(
        &client,
        nested_task(
            "states:startExecution",
            json!({"StateMachineArn": child, "Input": {"a": 1}}),
        ),
    )
    .await;

    assert!(
        output["StartDate"].as_i64() > Some(1_000_000_000_000),
        "{output}"
    );
    assert_eq!(output["SdkHttpMetadata"]["HttpStatusCode"], 200, "{output}");
    assert_eq!(
        output["SdkHttpMetadata"]["HttpHeaders"]["Content-Type"],
        "application/x-amz-json-1.0"
    );
    assert!(
        output["SdkResponseMetadata"]["RequestId"].is_string(),
        "{output}"
    );
    let child_arn = output["ExecutionArn"].as_str().expect("ExecutionArn");
    let desc = wait_terminal(&client, child_arn).await;
    let child_input: Value = serde_json::from_str(desc.input().unwrap()).unwrap();
    assert_eq!(child_input, json!({"a": 1}));
}

#[tokio::test]
async fn wait_for_task_token_passes_the_token_in_the_child_input() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;

    let parent = create_machine(
        &client,
        "parent",
        nested_task(
            "states:startExecution.waitForTaskToken",
            json!({"StateMachineArn": child, "Input": {"token.$": "$$.Task.Token"}}),
        ),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(parent)
        .send()
        .await
        .unwrap();

    let mut token = None;
    for _ in 0..200 {
        let listed = client
            .list_executions()
            .state_machine_arn(&child)
            .send()
            .await
            .unwrap();
        if let Some(exec) = listed.executions().first() {
            let desc = wait_terminal(&client, exec.execution_arn()).await;
            let input: Value = serde_json::from_str(desc.input().unwrap()).unwrap();
            token = input["token"].as_str().map(String::from);
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    let token = token.expect("child input must carry the task token");

    client
        .send_task_success()
        .task_token(token)
        .output(r#"{"done":true}"#)
        .send()
        .await
        .unwrap();

    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.status().as_str(), "SUCCEEDED", "{:?}", desc.cause());
    let output: Value = serde_json::from_str(desc.output().unwrap()).unwrap();
    assert_eq!(output, json!({"done": true}));
}

#[tokio::test]
async fn aws_sdk_sfn_uses_pascal_case_both_ways() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;

    let output = run_parent(
        &client,
        json!({"StartAt":"Start","States":{
            "Start":{
                "Type":"Task",
                "Resource":"arn:aws:states:::aws-sdk:sfn:startExecution",
                "Parameters":{"StateMachineArn": child, "Input":"{\"a\":1}"},
                "ResultPath":"$.started",
                "Next":"Describe"
            },
            "Describe":{
                "Type":"Task",
                "Resource":"arn:aws:states:::aws-sdk:sfn:describeExecution",
                "Parameters":{"ExecutionArn.$":"$.started.ExecutionArn"},
                "Retry":[{"ErrorEquals":["States.ALL"],"MaxAttempts":0}],
                "End":true
            }
        }}),
    )
    .await;

    assert_eq!(output["StateMachineArn"], child, "{output}");
    assert_eq!(output["Input"], "{\"a\":1}");
}

#[tokio::test]
async fn execution_tree_links_a_sync2_child_to_its_parent() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;

    let parent = create_machine(
        &client,
        "parent",
        nested_task(
            "states:startExecution.sync:2",
            json!({"StateMachineArn": child}),
        ),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(parent)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.status().as_str(), "SUCCEEDED", "{:?}", desc.cause());

    let tree = execution_tree(&server, started.execution_arn()).await;
    assert_eq!(only_child(&tree)["stateMachineArn"], child);
}

async fn execution_tree(server: &TestServer, arn: &str) -> Value {
    let url = format!(
        "{}/_fakecloud/stepfunctions/execution-tree/{arn}",
        server.endpoint()
    );
    reqwest::get(url).await.unwrap().json().await.unwrap()
}

/// The single child under the tree's root, failing with the tree when there
/// is not exactly one.
fn only_child(tree: &Value) -> &Value {
    let children = tree["tree"]["children"].as_array().expect("children");
    assert_eq!(children.len(), 1, "{tree}");
    &children[0]
}

#[tokio::test]
async fn execution_tree_links_a_failed_sync_child_to_its_parent() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_machine(
        &client,
        "failing-child",
        json!({"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"no"}}}),
    )
    .await;
    let parent = create_machine(
        &client,
        "parent",
        nested_task(
            "states:startExecution.sync",
            json!({"StateMachineArn": child}),
        ),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(parent)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.status().as_str(), "FAILED");

    let tree = execution_tree(&server, started.execution_arn()).await;
    let node = only_child(&tree);
    assert_eq!(node["stateMachineArn"], child);
    assert_eq!(node["status"], "FAILED");
}

#[tokio::test]
async fn execution_tree_shows_a_sync_child_while_the_parent_waits() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_machine(
        &client,
        "slow-child",
        json!({"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"End":true}}}),
    )
    .await;
    let parent = create_machine(
        &client,
        "parent",
        nested_task(
            "states:startExecution.sync:2",
            json!({"StateMachineArn": child}),
        ),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(parent)
        .send()
        .await
        .unwrap();

    // The child waits 30s, so it is still running whenever the poll finds it.
    let mut tree = Value::Null;
    for _ in 0..200 {
        tree = execution_tree(&server, started.execution_arn()).await;
        if tree["tree"]["children"]
            .as_array()
            .is_some_and(|children| !children.is_empty())
        {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(tree["tree"]["status"], "RUNNING", "{tree}");
    let node = only_child(&tree);
    assert_eq!(node["stateMachineArn"], child);
    assert_eq!(node["status"], "RUNNING");
}

#[tokio::test]
async fn execution_tree_links_a_child_started_through_aws_sdk_sfn() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;
    let parent = create_machine(
        &client,
        "parent",
        nested_task(
            "aws-sdk:sfn:startExecution",
            json!({"StateMachineArn": child}),
        ),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(parent)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.status().as_str(), "SUCCEEDED", "{:?}", desc.cause());

    let tree = execution_tree(&server, started.execution_arn()).await;
    assert_eq!(only_child(&tree)["stateMachineArn"], child);
}

#[tokio::test]
async fn execution_tree_finds_executions_in_a_non_default_account() {
    const ACCOUNT_B: &str = "222222222222";
    let server = TestServer::start().await;
    let (akid, secret) = server.create_admin(ACCOUNT_B, "admin-b").await;
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            akid,
            secret,
            None,
            None,
            "account-b",
        ))
        .load()
        .await;
    let client = aws_sdk_sfn::Client::new(&config);
    let role = format!("arn:aws:iam::{ACCOUNT_B}:role/sfn-role");
    let create = |name: &'static str, definition: Value| {
        let client = client.clone();
        let role = role.clone();
        async move {
            client
                .create_state_machine()
                .name(name)
                .definition(definition.to_string())
                .role_arn(role)
                .send()
                .await
                .unwrap()
                .state_machine_arn()
                .to_string()
        }
    };
    let child = create(
        "child",
        json!({"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}),
    )
    .await;
    let parent = create(
        "parent",
        nested_task(
            "states:startExecution.sync:2",
            json!({"StateMachineArn": child}),
        ),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(parent)
        .send()
        .await
        .unwrap();
    assert!(started.execution_arn().contains(ACCOUNT_B));
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.status().as_str(), "SUCCEEDED", "{:?}", desc.cause());

    let tree = execution_tree(&server, started.execution_arn()).await;
    assert_eq!(tree["rootArn"], started.execution_arn(), "{tree}");
    assert_eq!(only_child(&tree)["stateMachineArn"], child);
}

#[tokio::test]
async fn aws_sdk_sfn_send_task_success_delivers_an_object_output() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_machine(
        &client,
        "token-child",
        json!({"StartAt":"Send","States":{"Send":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:sfn:sendTaskSuccess",
            "Parameters":{"TaskToken.$":"$.token","Output":{"Done":true}},
            "End":true
        }}}),
    )
    .await;

    let output = run_parent(
        &client,
        nested_task(
            "states:startExecution.waitForTaskToken",
            json!({"StateMachineArn": child, "Input": {"token.$": "$$.Task.Token"}}),
        ),
    )
    .await;

    assert_eq!(output, json!({"Done": true}));
}

#[tokio::test]
async fn aws_sdk_sfn_dates_are_iso_strings() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;

    let output = run_parent(
        &client,
        json!({"StartAt":"Start","States":{
            "Start":{
                "Type":"Task",
                "Resource":"arn:aws:states:::states:startExecution.sync:2",
                "Parameters":{"StateMachineArn": child},
                "Next":"History"
            },
            "History":{
                "Type":"Task",
                "Resource":"arn:aws:states:::aws-sdk:sfn:getExecutionHistory",
                "Parameters":{"ExecutionArn.$":"$.ExecutionArn"},
                "ResultPath":"$.history",
                "Next":"Machine"
            },
            "Machine":{
                "Type":"Task",
                "Resource":"arn:aws:states:::aws-sdk:sfn:describeStateMachine",
                "Parameters":{"StateMachineArn.$":"$.StateMachineArn"},
                "ResultPath":"$.machine",
                "End":true
            }
        }}),
    )
    .await;

    let timestamp = output["history"]["Events"][0]["Timestamp"].as_str();
    assert!(timestamp.unwrap().ends_with('Z'), "{output}");
    assert_eq!(output["history"]["Events"][0]["Type"], "ExecutionStarted");
    let created = output["machine"]["CreationDate"].as_str();
    assert!(created.unwrap().ends_with('Z'), "{output}");
    assert_eq!(output["machine"]["Name"], "child");
}

// As AWS refuses it: the wire spelling isn't a definition field.
#[tokio::test]
async fn lowercase_state_machine_arn_is_an_invalid_definition() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let err = client
        .create_state_machine()
        .name("camel")
        .role_arn(ROLE)
        .definition(
            json!({"StartAt":"T","States":{"T":{
                "Type":"Task",
                "Resource":"arn:aws:states:::states:startExecution.sync:2",
                "Parameters":{"stateMachineArn":"arn:aws:states:us-east-1:123456789012:stateMachine:c"},
                "End":true
            }}})
            .to_string(),
        )
        .send()
        .await
        .expect_err("refused at creation")
        .into_service_error();
    assert!(err.is_invalid_definition(), "{err:?}");
    assert_eq!(
        err.meta().message(),
        Some(
            "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The field \
             'StateMachineArn' is required but was missing at /States/T/Parameters'"
        )
    );
}

// Recorded: `{% 1 + 1 %}` in a nested sync's Input reached the child as 2.
#[tokio::test]
async fn jsonata_arguments_evaluate_inside_a_nested_input() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_machine(
        &client,
        "ok-child",
        json!({"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"child":"ok"},"End":true}}}),
    )
    .await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"T","States":{"T":{
            "Type":"Task",
            "Resource":"arn:aws:states:::states:startExecution.sync:2",
            "Arguments":{"StateMachineArn":child,"Input":{"a":"{% 1 + 1 %}"}},
            "End":true
        }}}),
    )
    .await;

    assert_eq!(output["Input"], json!({"a": 2}));
    assert_eq!(output["Output"], json!({"child": "ok"}));
}

// Recorded: `Output` selects from `$states.result`.
#[tokio::test]
async fn jsonata_output_selects_from_the_result() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_echo_child(&client).await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"T","States":{"T":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:sfn:describeStateMachine",
            "Arguments":{"StateMachineArn":child},
            "Output":"{% $states.result.Name %}",
            "End":true
        }}}),
    )
    .await;

    assert_eq!(output, json!("child"));
}

#[tokio::test]
async fn jsonata_runs_a_whole_workflow() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"Init","States":{
            "Init":{
                "Type":"Pass",
                "Assign":{"factor":"{% 10 %}"},
                "Output":{"n":"{% 3 %}"},
                "Next":"Pick"
            },
            "Pick":{
                "Type":"Choice",
                "Choices":[
                    {"Condition":"{% $states.input.n > 5 %}","Next":"Never"},
                    {"Condition":"{% $states.input.n > 1 %}","Next":"Each"}
                ],
                "Default":"Never"
            },
            "Never":{"Type":"Fail","Error":"Wrong","Cause":"{% 'took the wrong branch' %}"},
            "Each":{
                "Type":"Map",
                "Items":"{% [1, $states.input.n] %}",
                "ItemSelector":{"value":"{% $states.context.Map.Item.Value * $factor %}"},
                "ItemProcessor":{
                    "StartAt":"Echo",
                    "States":{"Echo":{"Type":"Pass","Output":"{% $states.input.value %}","End":true}}
                },
                "Output":"{% $sum($states.result) %}",
                "Next":"Both"
            },
            "Both":{
                "Type":"Parallel",
                "Arguments":{"total":"{% $states.input %}"},
                "Branches":[
                    {"StartAt":"A","States":{"A":{"Type":"Pass","Output":"{% $states.input.total + 1 %}","End":true}}},
                    {"StartAt":"B","States":{"B":{"Type":"Pass","Output":"{% $states.input.total + 2 %}","End":true}}}
                ],
                "Next":"Done"
            },
            "Done":{"Type":"Succeed","Output":{"values":"{% $states.input %}","ctx":"{% $states.context.State.Name %}"}}
        }}),
    )
    .await;

    assert_eq!(output, json!({"values": [41, 42], "ctx": "Done"}));
}

#[tokio::test]
async fn jsonata_catch_output_reads_the_error() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"Boom","States":{
            "Boom":{
                "Type":"Task",
                "Resource":"arn:aws:states:::aws-sdk:s3:getObject",
                "Arguments":{"Bucket":"no-such-bucket-here","Key":"k"},
                "Catch":[{
                    "ErrorEquals":["States.ALL"],
                    "Output":{"caught":"{% $states.errorOutput.Error %}"},
                    "Next":"Done"
                }],
                "End":true
            },
            "Done":{"Type":"Pass","End":true}
        }}),
    )
    .await;

    assert_eq!(output, json!({"caught": "S3.NoSuchBucketException"}));
}

#[tokio::test]
async fn jsonata_failure_is_a_query_evaluation_error() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let machine = create_machine(
        &client,
        "bad-query",
        json!({"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{
            "Type":"Pass",
            "Output":"{% $states.input.missing %}",
            "End":true
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.error(), Some("States.QueryEvaluationError"));
}

#[tokio::test]
async fn jsonpath_assign_reads_by_state_type_and_paths_read_variables() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"StartAt":"Pass","States":{
            "Pass":{
                "Type":"Pass",
                "Parameters":{"a":{"b":1}},
                "Assign":{"fromResult.$":"$.a.b","literal":{"k":"v"},"ctx.$":"$$.State.Name"},
                "Next":"Read"
            },
            "Read":{
                "Type":"Pass",
                "Parameters":{
                    "n.$":"$fromResult",
                    "nested.$":"$literal.k",
                    "ctx.$":"$ctx",
                    "text.$":"States.Format('n={}', $fromResult)"
                },
                "Next":"Pick"
            },
            "Pick":{
                "Type":"Choice",
                "Choices":[{
                    "Variable":"$fromResult",
                    "NumericEquals":1,
                    "Assign":{"picked":true},
                    "Next":"Final"
                }],
                "Default":"Final"
            },
            "Final":{
                "Type":"Pass",
                "Parameters":{"read.$":"$","picked.$":"$picked"},
                "End":true
            }
        }}),
    )
    .await;

    assert_eq!(
        output,
        json!({
            "read": {"n": 1, "nested": "v", "ctx": "Pass", "text": "n=1"},
            "picked": true
        })
    );
}

#[tokio::test]
async fn variable_scopes_nest_and_stay_apart() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Each iteration reads the outer variable and keeps its own apart from its
    // siblings'; the inner one is gone after the Map.
    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"Init","States":{
            "Init":{"Type":"Pass","Assign":{"outer":10},"Next":"Each"},
            "Each":{
                "Type":"Map",
                "Items":[1,2],
                "ItemProcessor":{
                    "StartAt":"Set",
                    "States":{
                        "Set":{"Type":"Pass","Assign":{"inner":"{% $states.input * $outer %}"},"Next":"Get"},
                        "Get":{"Type":"Pass","Output":"{% $inner %}","End":true}
                    }
                },
                "Assign":{"seen":"{% $states.result %}"},
                "Next":"After"
            },
            "After":{"Type":"Pass","Output":"{% [$seen, $exists($inner)] %}","End":true}
        }}),
    )
    .await;
    // A JSONata array constructor flattens `$seen`.
    assert_eq!(output, json!([10, 20, false]));
}

#[tokio::test]
async fn distributed_map_iterations_cannot_read_outer_variables() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let machine = create_machine(
        &client,
        "dist-vars",
        json!({"QueryLanguage":"JSONata","StartAt":"Init","States":{
            "Init":{"Type":"Pass","Assign":{"outer":10},"Next":"Each"},
            "Each":{
                "Type":"Map",
                "Items":[1],
                "ItemProcessor":{
                    "ProcessorConfig":{"Mode":"DISTRIBUTED"},
                    "StartAt":"Get",
                    "States":{"Get":{"Type":"Pass","Output":"{% $outer %}","End":true}}
                },
                "End":true
            }
        }}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.error(), Some("States.ExceedToleratedFailureThreshold"));
}

#[tokio::test]
async fn jsonata_catch_assign_reaches_the_enclosing_scope() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"Boom","States":{
            "Boom":{
                "Type":"Parallel",
                "Branches":[{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Bad","Cause":"c"}}}],
                "Catch":[{
                    "ErrorEquals":["States.ALL"],
                    "Assign":{"caught":"{% $states.errorOutput.Error %}"},
                    "Next":"Done"
                }],
                "End":true
            },
            "Done":{"Type":"Pass","Output":"{% $caught %}","End":true}
        }}),
    )
    .await;
    assert_eq!(output, json!("Bad"));
}

// A `.sync` nothing can wait for fails before starting anything.
#[tokio::test]
async fn sync_without_a_waiter_fails_loudly_and_starts_nothing() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let machine = create_machine(
        &client,
        "no-waiter",
        json!({"StartAt":"T","States":{"T":{
            "Type":"Task",
            "Resource":"arn:aws:states:::sqs:createQueue.sync",
            "Parameters":{"QueueName":"never-made"},
            "End":true
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.error(), Some("States.TaskFailed"));
    assert!(desc.cause().unwrap().contains("not supported"), "{desc:?}");

    let queues = server
        .sqs_client()
        .await
        .list_queues()
        .send()
        .await
        .unwrap();
    assert!(queues.queue_urls().is_empty(), "{queues:?}");
}

#[tokio::test]
async fn wait_for_task_token_assigns_from_the_callback_output() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_machine(
        &client,
        "token-child",
        json!({"StartAt":"Send","States":{"Send":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:sfn:sendTaskSuccess",
            "Parameters":{"TaskToken.$":"$.token","Output":{"value":7}},
            "End":true
        }}}),
    )
    .await;

    let output = run_parent(
        &client,
        json!({"StartAt":"Wait","States":{
            "Wait":{
                "Type":"Task",
                "Resource":"arn:aws:states:::states:startExecution.waitForTaskToken",
                "Parameters":{"StateMachineArn":child,"Input":{"token.$":"$$.Task.Token"}},
                "Assign":{"got.$":"$.value"},
                "Next":"Read"
            },
            "Read":{"Type":"Pass","Parameters":{"got.$":"$got"},"End":true}
        }}),
    )
    .await;
    assert_eq!(output, json!({"got": 7}));
}

// A catcher sharing the state's `Next` is still a catcher, and two sharing a
// `Next` keep their own fields.
#[tokio::test]
async fn catchers_are_matched_not_inferred_from_next() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"Boom","States":{
            "Boom":{
                "Type":"Parallel",
                "Branches":[{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Second","Cause":"c"}}}],
                "Catch":[
                    {"ErrorEquals":["First"],"Output":{"which":"first"},"Next":"Done"},
                    {"ErrorEquals":["Second"],"Output":{"which":"second"},"Assign":{"caught":"{% $states.errorOutput.Error %}"},"Next":"Done"}
                ],
                "Next":"Done"
            },
            "Done":{"Type":"Pass","Output":{"in":"{% $states.input %}","caught":"{% $caught %}"},"End":true}
        }}),
    )
    .await;
    assert_eq!(
        output,
        json!({"in": {"which": "second"}, "caught": "Second"})
    );
}

#[tokio::test]
async fn jsonpath_catcher_assign_reads_error_and_context() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"StartAt":"Boom","States":{
            "Boom":{
                "Type":"Parallel",
                "Branches":[{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Bad","Cause":"c"}}}],
                "Catch":[{
                    "ErrorEquals":["States.ALL"],
                    "Assign":{"error.$":"$.Error","state.$":"$$.State.Name"},
                    "Next":"Done"
                }],
                "End":true
            },
            "Done":{"Type":"Pass","Parameters":{"error.$":"$error","state.$":"$state"},"End":true}
        }}),
    )
    .await;
    assert_eq!(output, json!({"error": "Bad", "state": "Boom"}));
}

#[tokio::test]
async fn a_failing_catcher_assign_fails_the_execution() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let machine = create_machine(
        &client,
        "bad-catch",
        json!({"StartAt":"Boom","States":{
            "Boom":{
                "Type":"Parallel",
                "Branches":[{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Bad","Cause":"c"}}}],
                "Catch":[{"ErrorEquals":["States.ALL"],"Assign":{"x.$":"$.missing"},"Next":"Done"}],
                "End":true
            },
            "Done":{"Type":"Succeed"}
        }}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.status().as_str(), "FAILED", "{desc:?}");
    assert_eq!(desc.error(), Some("States.Runtime"));
}

#[tokio::test]
async fn jsonata_item_selector_reads_the_state_input() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"Seed","States":{
            "Seed":{"Type":"Pass","Output":{"factor":3},"Next":"Each"},
            "Each":{
                "Type":"Map",
                "Items":[1,2],
                "ItemSelector":{"v":"{% $states.context.Map.Item.Value * $states.input.factor %}"},
                "ItemProcessor":{
                    "StartAt":"P",
                    "States":{"P":{"Type":"Pass","Output":"{% $states.input.v %}","End":true}}
                },
                "End":true
            }
        }}),
    )
    .await;
    assert_eq!(output, json!([3, 6]));
}

// Rules sharing a target, and a Default equal to one, keep their own Assign.
#[tokio::test]
async fn choice_assign_belongs_to_the_rule_taken() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = |value: i32| {
        json!({"StartAt":"Seed","States":{
            "Seed":{"Type":"Pass","Result":{"n":value},"Next":"Pick"},
            "Pick":{
                "Type":"Choice",
                "Choices":[
                    {"Variable":"$.n","NumericEquals":1,"Assign":{"which":"one"},"Next":"Done"},
                    {"Variable":"$.n","NumericEquals":2,"Assign":{"which":"two"},"Next":"Done"}
                ],
                "Assign":{"which":"default"},
                "Default":"Done"
            },
            "Done":{"Type":"Pass","Parameters":{"which.$":"$which"},"End":true}
        }})
    };
    for (value, expected) in [(1, "one"), (2, "two"), (3, "default")] {
        let arn = create_machine(&client, &format!("rule-{value}"), definition(value)).await;
        let started = client
            .start_execution()
            .state_machine_arn(arn)
            .send()
            .await
            .unwrap();
        let desc = wait_terminal(&client, started.execution_arn()).await;
        let output: Value = serde_json::from_str(desc.output().expect("output")).unwrap();
        assert_eq!(output["which"], expected, "{value}");
    }
}

// On AWS a query error is an ordinary state failure.
#[tokio::test]
async fn jsonata_errors_reach_the_states_catchers() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Before the state runs (`Items`), and after it (`Output`).
    for (name, field, template) in [
        ("before", "Items", "{% $states.input.missing %}"),
        ("after", "Output", "{% $states.result.missing %}"),
    ] {
        let mut map = json!({
            "Type":"Map",
            "ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},
            "Catch":[{
                "ErrorEquals":["States.QueryEvaluationError"],
                "Output":{"caught":"{% $states.errorOutput.Error %}"},
                "Next":"Done"
            }],
            "End":true
        });
        map[field] = json!(template);
        if name == "after" {
            map["Items"] = json!([1]);
        }
        let machine = create_machine(
            &client,
            &format!("catch-{name}"),
            json!({"QueryLanguage":"JSONata","StartAt":"M","States":{
                "M":map,
                "Done":{"Type":"Pass","End":true}
            }}),
        )
        .await;
        let started = client
            .start_execution()
            .state_machine_arn(machine)
            .send()
            .await
            .unwrap();
        let desc = wait_terminal(&client, started.execution_arn()).await;
        assert_eq!(desc.status().as_str(), "SUCCEEDED", "{name}: {desc:?}");
        let output: Value = serde_json::from_str(desc.output().unwrap()).unwrap();
        assert_eq!(
            output,
            json!({"caught": "States.QueryEvaluationError"}),
            "{name}"
        );

        let history = client
            .get_execution_history()
            .execution_arn(started.execution_arn())
            .send()
            .await
            .unwrap();
        let types: Vec<&str> = history
            .events()
            .iter()
            .map(|e| e.r#type().as_str())
            .collect();
        assert!(types.contains(&"MapStateEntered"), "{name}: {types:?}");
        assert!(types.contains(&"EvaluationFailed"), "{name}: {types:?}");
    }
}

// `$var` works wherever a path does, except ResultPath.
#[tokio::test]
async fn input_and_output_paths_read_variables() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"StartAt":"Set","States":{
            "Set":{"Type":"Pass","Assign":{"data":{"a":{"b":1},"c":2}},"Next":"In"},
            "In":{"Type":"Pass","InputPath":"$data.a","ResultPath":"$.copied","OutputPath":"$data","Next":"Out"},
            "Out":{"Type":"Pass","End":true}
        }}),
    )
    .await;
    assert_eq!(output, json!({"a": {"b": 1}, "c": 2}));

    let refused = client
        .create_state_machine()
        .name("result-path-variable")
        .role_arn(ROLE)
        .definition(
            json!({"StartAt":"P","States":{"P":{"Type":"Pass","ResultPath":"$x","End":true}}})
                .to_string(),
        )
        .send()
        .await
        .expect_err("ResultPath can't name a variable")
        .into_service_error();
    assert!(refused.is_invalid_definition(), "{refused:?}");
}

// An inline Map stops at its first failure rather than running the rest.
#[tokio::test]
async fn inline_map_stops_starting_iterations_after_a_failure() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let machine = create_machine(
        &client,
        "inline-stop",
        json!({"StartAt":"M","States":{"M":{
            "Type":"Map",
            "MaxConcurrency":1,
            "ItemProcessor":{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Bad","Cause":"c"}}},
            "End":true
        }}}),
    )
    .await;
    let items: Vec<i32> = (0..100).collect();
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .input(json!(items).to_string())
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.error(), Some("Bad"));
    let history = client
        .get_execution_history()
        .execution_arn(started.execution_arn())
        .max_results(1000)
        .send()
        .await
        .unwrap();
    let iterations = history
        .events()
        .iter()
        .filter(|e| e.r#type().as_str() == "MapIterationStarted")
        .count();
    assert!(iterations <= 2, "started {iterations}");
}

// A state failing after its result keeps none of its assignments; only the
// catcher's apply.
#[tokio::test]
async fn a_state_failing_after_its_result_assigns_nothing() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        // A failure after the result that a catcher can take: a JSONata
        // `Output` (no Retry or Catch takes `States.Runtime`, as recorded).
        json!({"QueryLanguage":"JSONata","StartAt":"Seed","States":{
            "Seed":{"Type":"Pass","Assign":{"x":"before"},"Next":"Boom"},
            "Boom":{
                "Type":"Parallel",
                "Branches":[{"StartAt":"P","States":{"P":{"Type":"Pass","Output":1,"End":true}}}],
                "Assign":{"x":"from-state"},
                "Output":"{% $states.result.nope %}",
                "Catch":[{"ErrorEquals":["States.QueryEvaluationError"],"Assign":{"y":"from-catcher"},"Next":"Done"}],
                "End":true
            },
            "Done":{"Type":"Pass","Output":{"x":"{% $x %}","y":"{% $y %}"},"End":true}
        }}),
    )
    .await;
    assert_eq!(output, json!({"x": "before", "y": "from-catcher"}));
}

// A state failing before it runs still reaches its catcher's Assign.
#[tokio::test]
async fn a_catcher_of_a_preparation_failure_assigns() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"M","States":{
            "M":{
                "Type":"Map",
                "Items":"{% $states.input.missing %}",
                "ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},
                "Catch":[{
                    "ErrorEquals":["States.QueryEvaluationError"],
                    "Assign":{"caught":"{% $states.errorOutput.Error %}"},
                    "Next":"Done"
                }],
                "End":true
            },
            "Done":{"Type":"Pass","Output":"{% $caught %}","End":true}
        }}),
    )
    .await;
    assert_eq!(output, json!("States.QueryEvaluationError"));
}

// History reports the output a JSONata state actually produced, and a state whose
// `Output` fails never exited.
#[tokio::test]
async fn jsonata_exit_events_record_the_evaluated_output() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let machine = create_machine(
        &client,
        "exit-output",
        json!({"QueryLanguage":"JSONata","StartAt":"P","States":{
            "P":{"Type":"Pass","Output":{"v":"{% 1 + 1 %}"},"End":true}
        }}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .send()
        .await
        .unwrap();
    wait_terminal(&client, started.execution_arn()).await;
    let history = client
        .get_execution_history()
        .execution_arn(started.execution_arn())
        .send()
        .await
        .unwrap();
    let exited = history
        .events()
        .iter()
        .find(|e| e.r#type().as_str() == "PassStateExited")
        .expect("exit event");
    let details = exited.state_exited_event_details().unwrap();
    let output: Value = serde_json::from_str(details.output().unwrap()).unwrap();
    assert_eq!(output, json!({"v": 2}));

    let machine = create_machine(
        &client,
        "exit-fail",
        json!({"QueryLanguage":"JSONata","StartAt":"P","States":{
            "P":{"Type":"Pass","Output":"{% $states.input.missing %}","End":true}
        }}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .send()
        .await
        .unwrap();
    wait_terminal(&client, started.execution_arn()).await;
    let history = client
        .get_execution_history()
        .execution_arn(started.execution_arn())
        .send()
        .await
        .unwrap();
    let types: Vec<&str> = history
        .events()
        .iter()
        .map(|e| e.r#type().as_str())
        .collect();
    assert!(!types.contains(&"PassStateExited"), "{types:?}");
    assert!(types.contains(&"EvaluationFailed"), "{types:?}");
}

#[tokio::test]
async fn fail_state_reads_error_and_cause_from_paths() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let machine = create_machine(
        &client,
        "fail-paths",
        json!({"StartAt":"F","States":{"F":{
            "Type":"Fail",
            "ErrorPath":"$.error",
            "CausePath":"States.Format('because {}', $.why)"
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .input(json!({"error": "Custom", "why": "reasons"}).to_string())
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.error(), Some("Custom"));
    assert_eq!(desc.cause(), Some("because reasons"));

    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .input(json!({"error": 5, "why": "x"}).to_string())
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.error(), Some("States.Runtime"));
}

#[tokio::test]
async fn jsonata_item_batcher_evaluates_its_limits() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"Seed","States":{
            "Seed":{"Type":"Pass","Output":{"size":2},"Next":"M"},
            "M":{
                "Type":"Map",
                "Items":[1,2,3],
                "ItemBatcher":{"MaxItemsPerBatch":"{% $states.input.size %}"},
                "ItemProcessor":{
                    "ProcessorConfig":{"Mode":"DISTRIBUTED"},
                    "StartAt":"P",
                    "States":{"P":{"Type":"Pass","End":true}}
                },
                "End":true
            }
        }}),
    )
    .await;
    assert_eq!(output.as_array().unwrap().len(), 2, "{output}");
}

#[tokio::test]
async fn a_bad_item_batcher_limit_fails_the_state() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let machine = create_machine(
        &client,
        "bad-batcher",
        json!({"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{
            "Type":"Map",
            "Items":[1],
            "ItemBatcher":{"MaxItemsPerBatch":"{% 'many' %}"},
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED"},
                "StartAt":"P",
                "States":{"P":{"Type":"Pass","End":true}}
            },
            "End":true
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.status().as_str(), "FAILED");
    assert!(
        desc.cause().unwrap().contains("MaxItemsPerBatch"),
        "{desc:?}"
    );
}

async fn failing_cause(
    client: &aws_sdk_sfn::Client,
    name: &str,
    definition: Value,
    input: Value,
) -> (String, String) {
    let machine = create_machine(client, name, definition).await;
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .input(input.to_string())
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(client, started.execution_arn()).await;
    assert_eq!(desc.status().as_str(), "FAILED", "{name}: {desc:?}");
    (
        desc.error().unwrap_or_default().to_string(),
        desc.cause().unwrap_or_default().to_string(),
    )
}

// Every message here is as recorded.
#[tokio::test]
async fn wait_failures_read_as_recorded() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    const PREFIX: &str =
        "An error occurred while executing the state 'A' (entered at the event id #2). ";
    let wait = |language: &str, field: &str, value: &str| {
        json!({"QueryLanguage": language, "StartAt":"A","States":{"A":{
            "Type":"Wait", field: value, "End":true
        }}})
    };

    for (name, definition, input, error, cause) in [
        (
            "w1",
            wait("JSONata", "Seconds", "{% '5' %}"),
            json!({}),
            "States.QueryEvaluationError",
            "The JSONata expression ''5'' specified for the field 'Seconds' returned an unexpected result type. Expected 'number', but was 'string' for value: \"5\"",
        ),
        (
            "w2",
            wait("JSONata", "Seconds", "{% -1 %}"),
            json!({}),
            "States.QueryEvaluationError",
            "Seconds cannot be negative but was: -1",
        ),
        (
            "w3",
            wait("JSONata", "Timestamp", "{% 'not-a-date' %}"),
            json!({}),
            "States.QueryEvaluationError",
            "The Timestamp field cannot be parsed as an ISO-8601 extended offset date-time format string: not-a-date",
        ),
        (
            "w4",
            wait("JSONPath", "SecondsPath", "$.s"),
            json!({"s": "nope"}),
            "States.Runtime",
            "The SecondsPath parameter cannot be parsed as a long value: $.s == nope",
        ),
        (
            "w5",
            wait("JSONPath", "TimestampPath", "$.s"),
            json!({"s": "nope"}),
            "States.Runtime",
            "The TimestampPath parameter does not reference a valid ISO-8601 extended offset date-time format string: $.s == nope",
        ),
    ] {
        let (got_error, got_cause) = failing_cause(&client, name, definition, input).await;
        assert_eq!(got_error, error, "{name}");
        assert_eq!(got_cause, format!("{PREFIX}{cause}"), "{name}");
    }

    let zero = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"A","States":{"A":{
            "Type":"Wait","Seconds":"{% 0 %}","End":true
        }}}),
    )
    .await;
    assert_eq!(zero, json!({}));
}

#[tokio::test]
async fn an_undefined_output_is_recorded_with_its_location() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let machine = create_machine(
        &client,
        "undefined-output",
        json!({"QueryLanguage":"JSONata","StartAt":"A","States":{"A":{
            "Type":"Pass","Output":"{% $states.input.missing %}","End":true
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .send()
        .await
        .unwrap();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    let expected = "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Output' returned nothing (undefined).";
    assert_eq!(desc.error(), Some("States.QueryEvaluationError"));
    assert_eq!(desc.cause(), Some(expected));

    let history = client
        .get_execution_history()
        .execution_arn(started.execution_arn())
        .send()
        .await
        .unwrap();
    let failed = history
        .events()
        .iter()
        .find(|e| e.r#type().as_str() == "EvaluationFailed")
        .expect("EvaluationFailed");
    assert_eq!(failed.previous_event_id(), 2);
    let details = failed.evaluation_failed_event_details().expect("details");
    assert_eq!(details.error(), Some("States.QueryEvaluationError"));
    assert_eq!(details.cause(), Some(expected));
    assert_eq!(details.location(), Some("Output"));
    assert_eq!(details.state(), "A");
}

#[tokio::test]
async fn seeded_random_matches_the_recording() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"A","States":{"A":{
            "Type":"Pass",
            "Output":"{% [$random(42) = $random(42), $random(42)] %}",
            "End":true
        }}}),
    )
    .await;
    assert_eq!(output, json!([true, 0.7275636800328681]));
}

#[tokio::test]
async fn item_batcher_shape_matches_the_recording() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let output = run_parent(
        &client,
        json!({"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{
            "Type":"Map",
            "Items":[1,2,3],
            "ItemBatcher":{"MaxItemsPerBatch":"{% 2 %}","BatchInput":{"k":"v"}},
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED"},
                "StartAt":"P",
                "States":{"P":{"Type":"Pass","End":true}}
            },
            "End":true
        }}}),
    )
    .await;
    assert_eq!(
        output,
        json!([
            {"BatchInput": {"k": "v"}, "Items": [1, 2]},
            {"BatchInput": {"k": "v"}, "Items": [3]}
        ])
    );
}

// The docs: Task states can `Retry` on `States.QueryEvaluationError`.
#[tokio::test]
async fn task_retry_applies_to_query_errors_in_arguments() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let machine = create_machine(
        &client,
        "retry-query",
        json!({"QueryLanguage":"JSONata","StartAt":"T","States":{"T":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:sqs:listQueues",
            "Arguments":{"QueueNamePrefix":"{% $states.input.missing %}"},
            "Retry":[{
                "ErrorEquals":["States.QueryEvaluationError"],
                "IntervalSeconds":1,
                "MaxAttempts":1
            }],
            "End":true
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(machine)
        .send()
        .await
        .unwrap();
    let begun = std::time::Instant::now();
    let desc = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(desc.error(), Some("States.QueryEvaluationError"));
    assert!(
        begun.elapsed() >= Duration::from_millis(900),
        "no retry happened"
    );
}

async fn create_express_machine(
    client: &aws_sdk_sfn::Client,
    name: &str,
    definition: Value,
) -> String {
    client
        .create_state_machine()
        .name(name)
        .definition(definition.to_string())
        .role_arn(ROLE)
        .r#type(aws_sdk_sfn::types::StateMachineType::Express)
        .send()
        .await
        .unwrap()
        .state_machine_arn()
        .to_string()
}

fn sorted_keys(v: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort_unstable();
    keys
}

/// Recorded against AWS: a STANDARD parent runs an EXPRESS child through
/// `.sync` and `.sync:2`, and catches an EXPRESS child's failure. The
/// results carry no redrive fields, as an EXPRESS execution can't be
/// redriven, and the child's ARN is `express:<machine>:<name>:<id>`.
#[tokio::test]
async fn sync_waits_for_an_express_child_as_recorded() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_express_machine(
        &client,
        "express-child",
        json!({"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"ok":1},"End":true}}}),
    )
    .await;
    let failing = create_express_machine(
        &client,
        "express-child-fail",
        json!({"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"bad"}}}),
    )
    .await;

    let output = run_parent(
        &client,
        json!({"StartAt":"A","States":{
            "A":{"Type":"Task","Resource":"arn:aws:states:::states:startExecution.sync",
                 "Parameters":{"StateMachineArn": child, "Input":{"x":1}},
                 "ResultPath":"$.a","Next":"B"},
            "B":{"Type":"Task","Resource":"arn:aws:states:::states:startExecution.sync:2",
                 "Parameters":{"StateMachineArn": child, "Input":{"x":2}},
                 "ResultPath":"$.b","Next":"C"},
            "C":{"Type":"Task","Resource":"arn:aws:states:::states:startExecution.sync:2",
                 "Parameters":{"StateMachineArn": failing},
                 "Catch":[{"ErrorEquals":["States.ALL"],"ResultPath":"$.c","Next":"D"}],
                 "End":true},
            "D":{"Type":"Pass","End":true}
        }}),
    )
    .await;

    let success_keys = [
        "ExecutionArn",
        "Input",
        "InputDetails",
        "Name",
        "Output",
        "OutputDetails",
        "StartDate",
        "StateMachineArn",
        "Status",
        "StopDate",
    ];
    let (sync, sync2) = (&output["a"], &output["b"]);
    assert_eq!(sorted_keys(sync), success_keys, "{sync}");
    assert_eq!(sorted_keys(sync2), success_keys, "{sync2}");
    assert_eq!(sync["Output"], r#"{"ok":1}"#);
    assert_eq!(sync["Input"], r#"{"x":1}"#);
    assert_eq!(sync2["Output"], json!({"ok": 1}));
    assert_eq!(sync2["Input"], json!({"x": 2}));
    for result in [sync, sync2] {
        let arn = result["ExecutionArn"].as_str().unwrap();
        let name = result["Name"].as_str().unwrap();
        assert!(
            arn.starts_with(&format!(
                "arn:aws:states:us-east-1:123456789012:express:express-child:{name}:"
            )),
            "{arn}"
        );
        assert_eq!(result["Status"], "SUCCEEDED");
    }

    assert_eq!(output["c"]["Error"], "States.TaskFailed");
    let cause: Value = serde_json::from_str(output["c"]["Cause"].as_str().unwrap()).unwrap();
    assert_eq!(
        sorted_keys(&cause),
        [
            "Cause",
            "Error",
            "ExecutionArn",
            "Input",
            "InputDetails",
            "Name",
            "StartDate",
            "StateMachineArn",
            "Status",
            "StopDate",
        ],
        "{cause}"
    );
    assert_eq!(cause["Error"], "Boom");
    assert_eq!(cause["Cause"], "bad");
    assert_eq!(cause["Status"], "FAILED");
}

/// Recorded against AWS: the execution operations refuse an EXPRESS
/// execution ARN, and ListExecutions refuses an EXPRESS state machine.
#[tokio::test]
async fn express_executions_refuse_the_execution_operations() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let sm = create_express_machine(
        &client,
        "express-ops",
        json!({"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":5,"End":true}}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(&sm)
        .name("run")
        .send()
        .await
        .unwrap();
    let arn = started.execution_arn();
    let message = "Invalid Arn: 'Resource type not valid in this context: express'";

    let err = client
        .describe_execution()
        .execution_arn(arn)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), err.message()),
        (Some("InvalidArn"), Some(message))
    );
    let err = client
        .get_execution_history()
        .execution_arn(arn)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), err.message()),
        (Some("InvalidArn"), Some(message))
    );
    let err = client
        .stop_execution()
        .execution_arn(arn)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), err.message()),
        (Some("InvalidArn"), Some(message))
    );
    let err = client
        .redrive_execution()
        .execution_arn(arn)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), err.message()),
        (Some("InvalidArn"), Some(message))
    );
    let err = client
        .describe_state_machine_for_execution()
        .execution_arn(arn)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), err.message()),
        (Some("InvalidArn"), Some(message))
    );

    let err = client
        .list_executions()
        .state_machine_arn(&sm)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), err.message()),
        (
            Some("StateMachineTypeNotSupported"),
            Some("This operation is not supported by this type of state machine")
        )
    );
}

/// The distributed Map recorded against AWS: three items, one failing, under a
/// tolerance, with a `Label`.
fn recorded_map_run(execution_type: &str) -> Value {
    json!({"StartAt":"M","States":{"M":{
        "Type":"Map","ToleratedFailureCount":5,"Label":"lbl",
        "ItemProcessor":{
            "ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":execution_type},
            "StartAt":"C",
            "States":{
                "C":{"Type":"Choice","Choices":[{"Variable":"$","StringEquals":"bad","Next":"F"}],"Default":"P"},
                "P":{"Type":"Pass","End":true},
                "F":{"Type":"Fail","Error":"E","Cause":"c"}
            }
        },
        "End":true
    }}})
}

async fn history_types(client: &aws_sdk_sfn::Client, arn: &str) -> Vec<String> {
    client
        .get_execution_history()
        .execution_arn(arn)
        .send()
        .await
        .unwrap()
        .events()
        .iter()
        .map(|e| e.r#type().as_str().to_string())
        .collect()
}

// A distributed Map's iterations are child executions of their own, which
// ListExecutions lists by map run and DescribeExecution describes, for STANDARD
// and EXPRESS children alike, as recorded against AWS.
#[tokio::test]
async fn map_run_children_are_listed_and_described_as_recorded() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    for (execution_type, kind) in [("STANDARD", "execution"), ("EXPRESS", "express")] {
        let name = format!("dmap-{}", execution_type.to_lowercase());
        let machine = create_machine(&client, &name, recorded_map_run(execution_type)).await;
        let started = client
            .start_execution()
            .state_machine_arn(&machine)
            .name("run1")
            .input(r#"["a","b","bad"]"#)
            .send()
            .await
            .unwrap();
        let parent = wait_terminal(&client, started.execution_arn()).await;
        assert_eq!(parent.status().as_str(), "SUCCEEDED", "{execution_type}");

        // The parent's history follows the map run, not its iterations.
        assert_eq!(
            history_types(&client, started.execution_arn()).await,
            [
                "ExecutionStarted",
                "MapStateEntered",
                "MapStateStarted",
                "MapRunStarted",
                "MapRunSucceeded",
                "MapStateSucceeded",
                "MapStateExited",
                "ExecutionSucceeded",
            ],
            "{execution_type}"
        );

        let labeled = format!("{machine}/lbl");
        let runs = client
            .list_map_runs()
            .execution_arn(started.execution_arn())
            .send()
            .await
            .unwrap();
        let run = &runs.map_runs()[0];
        assert_eq!(run.state_machine_arn(), labeled);
        let run_arn = run.map_run_arn().to_string();
        assert!(
            run_arn.starts_with(&format!(
                "arn:aws:states:us-east-1:123456789012:mapRun:{name}/lbl:"
            )),
            "{run_arn}"
        );
        let described_run = client
            .describe_map_run()
            .map_run_arn(&run_arn)
            .send()
            .await
            .unwrap();
        let counts = described_run.execution_counts().unwrap();
        assert_eq!(
            (
                counts.succeeded(),
                counts.failed(),
                counts.results_written()
            ),
            (2, 1, 3)
        );
        assert_eq!(described_run.redrive_count(), Some(0));

        let children = client
            .list_executions()
            .map_run_arn(&run_arn)
            .send()
            .await
            .unwrap();
        let children = children.executions();
        assert_eq!(children.len(), 3, "{execution_type}");
        let child_prefix = format!("arn:aws:states:us-east-1:123456789012:{kind}:{name}/lbl:");
        for child in children {
            assert!(
                child.execution_arn().starts_with(&child_prefix),
                "{}",
                child.execution_arn()
            );
            assert_eq!(
                child.execution_arn(),
                format!("{child_prefix}{}", child.name())
            );
            assert_eq!(child.state_machine_arn(), labeled);
            assert_eq!(child.map_run_arn(), Some(run_arn.as_str()));
            assert_eq!(child.item_count(), Some(1));
            assert_eq!(child.redrive_count(), Some(0));
        }
        let mut statuses: Vec<&str> = children.iter().map(|c| c.status().as_str()).collect();
        statuses.sort_unstable();
        assert_eq!(statuses, ["FAILED", "SUCCEEDED", "SUCCEEDED"]);

        let failed = client
            .list_executions()
            .map_run_arn(&run_arn)
            .status_filter(aws_sdk_sfn::types::ExecutionStatus::Failed)
            .send()
            .await
            .unwrap();
        assert_eq!(failed.executions().len(), 1);
        let failed_arn = failed.executions()[0].execution_arn().to_string();
        let page = client
            .list_executions()
            .map_run_arn(&run_arn)
            .max_results(1)
            .send()
            .await
            .unwrap();
        assert_eq!(page.executions().len(), 1);
        assert!(page.next_token().is_some());

        let child = client
            .describe_execution()
            .execution_arn(&failed_arn)
            .send()
            .await
            .unwrap();
        assert_eq!(child.input(), Some("\"bad\""));
        assert_eq!((child.error(), child.cause()), (Some("E"), Some("c")));
        assert_eq!(child.map_run_arn(), Some(run_arn.as_str()));
        assert_eq!(child.state_machine_arn(), labeled);

        let child_machine = client
            .describe_state_machine_for_execution()
            .execution_arn(&failed_arn)
            .send()
            .await
            .unwrap();
        assert_eq!(child_machine.state_machine_arn(), labeled);
        assert_eq!(child_machine.name(), name);
        assert_eq!(child_machine.label(), Some("lbl"));
        assert_eq!(child_machine.map_run_arn(), Some(run_arn.as_str()));
        let mut processor =
            recorded_map_run(execution_type)["States"]["M"]["ItemProcessor"].clone();
        processor.as_object_mut().unwrap().remove("ProcessorConfig");
        assert_eq!(
            serde_json::from_str::<Value>(child_machine.definition()).unwrap(),
            processor
        );
        let labeled_machine = client
            .describe_state_machine()
            .state_machine_arn(&labeled)
            .send()
            .await
            .unwrap();
        assert_eq!(labeled_machine.r#type().as_str(), execution_type);
        assert_eq!(labeled_machine.label(), Some("lbl"));
        assert_eq!(labeled_machine.name(), name);

        let history = client
            .get_execution_history()
            .execution_arn(&failed_arn)
            .send()
            .await;
        let redriven = client
            .redrive_execution()
            .execution_arn(&failed_arn)
            .send()
            .await
            .unwrap_err();
        match execution_type {
            "STANDARD" => {
                let types: Vec<&str> = history
                    .as_ref()
                    .unwrap()
                    .events()
                    .iter()
                    .map(|e| e.r#type().as_str())
                    .collect();
                assert_eq!(
                    types,
                    [
                        "ExecutionStarted",
                        "ChoiceStateEntered",
                        "ChoiceStateExited",
                        "FailStateEntered",
                        "ExecutionFailed"
                    ]
                );
                let stopped = client
                    .stop_execution()
                    .execution_arn(&failed_arn)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(stopped.stop_date(), child.stop_date().unwrap());
                assert_eq!(
                    (redriven.code(), redriven.message()),
                    (
                        Some("ValidationException"),
                        Some("Labeled Execution ARNs are not supported for this API")
                    )
                );
            }
            _ => {
                let err = history.unwrap_err();
                assert_eq!(err.code(), Some("InvalidArn"));
                assert_eq!(redriven.code(), Some("InvalidArn"));
            }
        }

        // The state machine lists its own execution only; its labeled one
        // can't be listed.
        let own = client
            .list_executions()
            .state_machine_arn(&machine)
            .send()
            .await
            .unwrap();
        assert_eq!(own.executions().len(), 1);
        assert_eq!(own.executions()[0].name(), "run1");
        assert_eq!(own.executions()[0].map_run_arn(), None);
        let err = client
            .list_executions()
            .state_machine_arn(&labeled)
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            (err.code(), err.message()),
            (
                Some("ValidationException"),
                Some("Labeled State Machine ARNs are not supported for this API")
            )
        );
    }
}

// ListExecutions takes a state machine or a map run, and refuses anything else
// as recorded against AWS.
#[tokio::test]
async fn list_executions_refuses_what_aws_refuses() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let machine = create_machine(&client, "dmap-errors", recorded_map_run("STANDARD")).await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .input(r#"["a"]"#)
        .send()
        .await
        .unwrap();
    wait_terminal(&client, started.execution_arn()).await;
    let run_arn = client
        .list_map_runs()
        .execution_arn(started.execution_arn())
        .send()
        .await
        .unwrap()
        .map_runs()[0]
        .map_run_arn()
        .to_string();

    let pick_one = (
        Some("ValidationException"),
        Some("Must provide a StateMachine ARN or MapRun ARN"),
    );
    let err = client.list_executions().send().await.unwrap_err();
    assert_eq!((err.code(), err.message()), pick_one);
    let err = client
        .list_executions()
        .state_machine_arn(&machine)
        .map_run_arn(&run_arn)
        .send()
        .await
        .unwrap_err();
    assert_eq!((err.code(), err.message()), pick_one);

    let err = client
        .list_executions()
        .map_run_arn("foo")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), err.message()),
        (
            Some("InvalidArn"),
            Some("Invalid Arn: 'Invalid ARN prefix: foo'")
        )
    );
    let err = client
        .list_executions()
        .map_run_arn(&machine)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), err.message()),
        (
            Some("InvalidArn"),
            Some("Invalid Arn: 'Resource type not valid in this context: stateMachine'")
        )
    );
    let missing = "arn:aws:states:us-east-1:123456789012:mapRun:nope/x:y";
    let err = client
        .list_executions()
        .map_run_arn(missing)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), err.message().map(str::to_string)),
        (
            Some("ResourceNotFound"),
            Some(format!("Resource not found: '{missing}'"))
        )
    );
}

// Without a `Label`, AWS names a map run's children by an id stable for the
// state machine and state, so every run of the state shares it, as recorded.
#[tokio::test]
async fn an_unlabeled_map_run_keeps_its_name_across_executions() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let mut definition = recorded_map_run("STANDARD");
    definition["States"]["M"]
        .as_object_mut()
        .unwrap()
        .remove("Label");
    let machine = create_machine(&client, "dmap-unlabeled", definition).await;
    let mut labeled = Vec::new();
    for _ in 0..2 {
        let started = client
            .start_execution()
            .state_machine_arn(&machine)
            .input(r#"["a"]"#)
            .send()
            .await
            .unwrap();
        wait_terminal(&client, started.execution_arn()).await;
        let runs = client
            .list_map_runs()
            .execution_arn(started.execution_arn())
            .send()
            .await
            .unwrap();
        labeled.push(runs.map_runs()[0].state_machine_arn().to_string());
    }
    assert_eq!(labeled[0], labeled[1]);
    let label = labeled[0].strip_prefix(&format!("{machine}/")).unwrap();
    assert_eq!(uuid::Uuid::parse_str(label).unwrap().get_version_num(), 3);
}

// Stopping an execution that has ended changes nothing and reports when it
// stopped, as recorded against AWS.
#[tokio::test]
async fn stopping_an_ended_execution_reports_its_stop_date() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let machine = create_echo_child(&client).await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .send()
        .await
        .unwrap();
    let ended = wait_terminal(&client, started.execution_arn()).await;
    let stopped = client
        .stop_execution()
        .execution_arn(started.execution_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(stopped.stop_date(), ended.stop_date().unwrap());
    let after = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(after.status().as_str(), "SUCCEEDED");
}

// Stopping an execution stops its map run and the children still running in
// it, and starts no more.
#[tokio::test]
async fn stopping_the_parent_aborts_its_map_run_children() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let machine = create_machine(
        &client,
        "dmap-stop",
        json!({"StartAt":"M","States":{"M":{
            "Type":"Map",
            "MaxConcurrency":1,
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},
                "StartAt":"W","States":{"W":{"Type":"Wait","Seconds":1,"End":true}}
            },
            "End":true
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .input("[1,2,3]")
        .send()
        .await
        .unwrap();
    let parent = started.execution_arn();
    let mut run_arn = None;
    for _ in 0..200 {
        let runs = client
            .list_map_runs()
            .execution_arn(parent)
            .send()
            .await
            .unwrap();
        if let Some(run) = runs.map_runs().first() {
            let arn = run.map_run_arn().to_string();
            let children = client
                .list_executions()
                .map_run_arn(&arn)
                .send()
                .await
                .unwrap();
            if !children.executions().is_empty() {
                run_arn = Some(arn);
                break;
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
    let run_arn = run_arn.expect("the first child started");
    client
        .stop_execution()
        .execution_arn(parent)
        .send()
        .await
        .unwrap();
    // Long enough for the running child's Wait to end and the next to start.
    sleep(Duration::from_millis(2500)).await;

    let children = client
        .list_executions()
        .map_run_arn(&run_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(children.executions().len(), 1, "{children:?}");
    let child = &children.executions()[0];
    assert_eq!(child.status().as_str(), "ABORTED");
    assert!(child.stop_date().is_some());
    let types = history_types(&client, child.execution_arn()).await;
    assert_eq!(types.last().map(String::as_str), Some("ExecutionAborted"));

    let run = client
        .describe_map_run()
        .map_run_arn(&run_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(run.status().as_str(), "ABORTED");
    let counts = run.execution_counts().unwrap();
    assert_eq!(
        (
            counts.aborted(),
            counts.running(),
            counts.pending(),
            counts.total()
        ),
        (3, 0, 0, 3)
    );
    let parent_types = history_types(&client, parent).await;
    assert!(
        !parent_types.iter().any(|t| t == "MapRunSucceeded"),
        "{parent_types:?}"
    );
}

// Under an ItemBatcher a child processes its batch: `itemCount` and the map
// run's item counts follow the items, its execution counts the children.
#[tokio::test]
async fn batched_map_run_children_count_their_items() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let machine = create_machine(
        &client,
        "dmap-batched",
        json!({"StartAt":"M","States":{"M":{
            "Type":"Map",
            "ItemBatcher":{"MaxItemsPerBatch":2},
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},
                "StartAt":"P","States":{"P":{"Type":"Pass","End":true}}
            },
            "End":true
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .input("[1,2,3]")
        .send()
        .await
        .unwrap();
    wait_terminal(&client, started.execution_arn()).await;
    let run_arn = client
        .list_map_runs()
        .execution_arn(started.execution_arn())
        .send()
        .await
        .unwrap()
        .map_runs()[0]
        .map_run_arn()
        .to_string();
    let children = client
        .list_executions()
        .map_run_arn(&run_arn)
        .send()
        .await
        .unwrap();
    let mut item_counts: Vec<i32> = children
        .executions()
        .iter()
        .map(|c| c.item_count().unwrap())
        .collect();
    item_counts.sort_unstable();
    assert_eq!(item_counts, [1, 2]);
    let run = client
        .describe_map_run()
        .map_run_arn(&run_arn)
        .send()
        .await
        .unwrap();
    let items = run.item_counts().unwrap();
    let executions = run.execution_counts().unwrap();
    assert_eq!((items.total(), items.succeeded()), (3, 3));
    assert_eq!((executions.total(), executions.succeeded()), (2, 2));
}

/// Wait until `execution_arn` has a map run with at least `children` children
/// started; returns the map run ARN.
async fn wait_for_map_run_children(
    client: &aws_sdk_sfn::Client,
    execution_arn: &str,
    children: usize,
) -> String {
    for _ in 0..300 {
        let runs = client
            .list_map_runs()
            .execution_arn(execution_arn)
            .send()
            .await
            .unwrap();
        if let Some(run) = runs.map_runs().first() {
            let arn = run.map_run_arn().to_string();
            let listed = client
                .list_executions()
                .map_run_arn(&arn)
                .send()
                .await
                .unwrap();
            if listed.executions().len() >= children {
                return arn;
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("{execution_arn} started no map run with {children} children");
}

// A child stopped on its own fails its iteration, whatever its states return.
#[tokio::test]
async fn a_stopped_child_fails_its_iteration() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let machine = create_machine(
        &client,
        "dmap-stop-child",
        json!({"StartAt":"M","States":{"M":{
            "Type":"Map",
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},
                "StartAt":"W","States":{"W":{"Type":"Wait","Seconds":1,"End":true}}
            },
            "End":true
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .input("[1]")
        .send()
        .await
        .unwrap();
    let run_arn = wait_for_map_run_children(&client, started.execution_arn(), 1).await;
    let child = client
        .list_executions()
        .map_run_arn(&run_arn)
        .send()
        .await
        .unwrap()
        .executions()[0]
        .execution_arn()
        .to_string();
    client
        .stop_execution()
        .execution_arn(&child)
        .send()
        .await
        .unwrap();
    let parent = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(parent.status().as_str(), "FAILED");
    assert_eq!(
        parent.error(),
        Some("States.ExceedToleratedFailureThreshold")
    );
}

// Stopping the root stops map runs nested in its children too.
#[tokio::test]
async fn stopping_the_root_aborts_nested_map_runs() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let inner = json!({
        "ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},
        "StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"End":true}}
    });
    let machine = create_machine(
        &client,
        "dmap-nested-stop",
        json!({"StartAt":"M","States":{"M":{
            "Type":"Map",
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},
                "StartAt":"Inner",
                "States":{"Inner":{"Type":"Map","ItemsPath":"$.xs","ItemProcessor":inner,"End":true}}
            },
            "End":true
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .input(r#"[{"xs":[1]}]"#)
        .send()
        .await
        .unwrap();
    let outer_run = wait_for_map_run_children(&client, started.execution_arn(), 1).await;
    let child = client
        .list_executions()
        .map_run_arn(&outer_run)
        .send()
        .await
        .unwrap()
        .executions()[0]
        .execution_arn()
        .to_string();
    let inner_run = wait_for_map_run_children(&client, &child, 1).await;
    client
        .stop_execution()
        .execution_arn(started.execution_arn())
        .send()
        .await
        .unwrap();
    for run in [&outer_run, &inner_run] {
        let described = client
            .describe_map_run()
            .map_run_arn(run)
            .send()
            .await
            .unwrap();
        assert_eq!(described.status().as_str(), "ABORTED", "{run}");
        let children = client
            .list_executions()
            .map_run_arn(run)
            .send()
            .await
            .unwrap();
        for child in children.executions() {
            assert_eq!(
                child.status().as_str(),
                "ABORTED",
                "{}",
                child.execution_arn()
            );
        }
    }
}

// A run that fails past its tolerance leaves what it never started pending:
// its counts still add up to its total.
#[tokio::test]
async fn a_failed_map_run_counts_what_it_never_started_as_pending() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let machine = create_machine(
        &client,
        "dmap-failed-counts",
        json!({"StartAt":"M","States":{"M":{
            "Type":"Map",
            "MaxConcurrency":1,
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},
                "StartAt":"F","States":{"F":{"Type":"Fail","Error":"E"}}
            },
            "End":true
        }}}),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .input("[1,2,3,4,5]")
        .send()
        .await
        .unwrap();
    let parent = wait_terminal(&client, started.execution_arn()).await;
    assert_eq!(parent.status().as_str(), "FAILED");
    let run_arn = client
        .list_map_runs()
        .execution_arn(started.execution_arn())
        .send()
        .await
        .unwrap()
        .map_runs()[0]
        .map_run_arn()
        .to_string();
    let run = client
        .describe_map_run()
        .map_run_arn(&run_arn)
        .send()
        .await
        .unwrap();
    let c = run.execution_counts().unwrap();
    assert_eq!(c.total(), 5);
    assert_eq!(
        c.pending() + c.running() + c.succeeded() + c.failed() + c.timed_out() + c.aborted(),
        5,
        "{c:?}"
    );
    assert!(c.pending() > 0, "{c:?}");
}
