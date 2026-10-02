//! Step Functions path / Choice / intrinsic semantics: every `*Path` Choice
//! comparator, nested intrinsic calls, full JSONPath syntax, and the
//! `States.Runtime` failure AWS raises when a path matches nothing.

mod helpers;

use helpers::TestServer;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

const ROLE: &str = "arn:aws:iam::123456789012:role/test-role";

/// Create a state machine, run it with `input` and return the final
/// DescribeExecution `(status, output, error, cause)`.
async fn run(
    client: &aws_sdk_sfn::Client,
    name: &str,
    definition: Value,
    input: Value,
) -> (String, Option<Value>, Option<String>, Option<String>) {
    let sm = client
        .create_state_machine()
        .name(name)
        .definition(definition.to_string())
        .role_arn(ROLE)
        .send()
        .await
        .unwrap();
    let start = client
        .start_execution()
        .state_machine_arn(sm.state_machine_arn())
        .input(input.to_string())
        .send()
        .await
        .unwrap();
    for _ in 0..200 {
        sleep(Duration::from_millis(50)).await;
        let desc = client
            .describe_execution()
            .execution_arn(start.execution_arn())
            .send()
            .await
            .unwrap();
        let status = desc.status().as_str().to_string();
        if status != "RUNNING" {
            return (
                status,
                desc.output().map(|o| serde_json::from_str(o).unwrap()),
                desc.error().map(str::to_string),
                desc.cause().map(str::to_string),
            );
        }
    }
    panic!("execution {name} did not finish");
}

#[tokio::test]
async fn sfn_choice_path_comparators_route_correctly() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Each choice uses a different *Path comparator; only the last one is
    // true for the input, so earlier ones silently evaluating false is not
    // enough: the matching rule must actually fire.
    let def = json!({
        "StartAt": "C",
        "States": {
            "C": {
                "Type": "Choice",
                "Choices": [
                    {"Variable": "$.n", "NumericGreaterThanPath": "$.limit", "Next": "Wrong"},
                    {"Variable": "$.s", "StringGreaterThanPath": "$.s_hi", "Next": "Wrong"},
                    {"Variable": "$.t", "TimestampGreaterThanPath": "$.t_hi", "Next": "Wrong"},
                    {
                        "And": [
                            {"Variable": "$.n", "NumericLessThanEqualsPath": "$.limit"},
                            {"Variable": "$.s", "StringLessThanPath": "$.s_hi"},
                            {"Variable": "$.t", "TimestampLessThanPath": "$.t_hi"},
                            {"Variable": "$.t", "TimestampEqualsPath": "$.t_same"},
                            {"Variable": "$.s", "StringMatchesPath": "$.pattern"}
                        ],
                        "Next": "Right"
                    }
                ],
                "Default": "Wrong"
            },
            "Right": {"Type": "Pass", "Result": "right", "End": true},
            "Wrong": {"Type": "Pass", "Result": "wrong", "End": true}
        }
    });
    let input = json!({
        "n": 3, "limit": 3,
        "s": "apple", "s_hi": "banana", "pattern": "app*",
        "t": "2024-01-01T00:00:00Z", "t_hi": "2025-01-01T00:00:00Z",
        "t_same": "2024-01-01T01:00:00+01:00"
    });
    let (status, output, _, _) = run(&client, "choice-paths", def, input).await;
    assert_eq!(status, "SUCCEEDED");
    assert_eq!(output, Some(json!("right")));
}

#[tokio::test]
async fn sfn_choice_missing_variable_fails_unless_guarded() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let unguarded = json!({
        "StartAt": "C",
        "States": {
            "C": {
                "Type": "Choice",
                "Choices": [{"Variable": "$.missing", "StringEquals": "x", "Next": "Done"}],
                "Default": "Done"
            },
            "Done": {"Type": "Succeed"}
        }
    });
    let (status, _, error, cause) = run(&client, "choice-missing", unguarded, json!({})).await;
    assert_eq!(status, "FAILED");
    assert_eq!(error.as_deref(), Some("States.Runtime"));
    assert!(cause.unwrap_or_default().contains("$.missing"));

    let guarded = json!({
        "StartAt": "C",
        "States": {
            "C": {
                "Type": "Choice",
                "Choices": [{
                    "And": [
                        {"Variable": "$.missing", "IsPresent": true},
                        {"Variable": "$.missing", "StringEquals": "x"}
                    ],
                    "Next": "Wrong"
                }],
                "Default": "Done"
            },
            "Wrong": {"Type": "Fail", "Error": "Wrong"},
            "Done": {"Type": "Succeed"}
        }
    });
    let (status, _, _, _) = run(&client, "choice-guarded", guarded, json!({})).await;
    assert_eq!(status, "SUCCEEDED");
}

#[tokio::test]
async fn sfn_nested_intrinsics_and_jsonpath_syntax() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let def = json!({
        "StartAt": "P",
        "States": {
            "P": {
                "Type": "Pass",
                "Parameters": {
                    "second.$": "States.ArrayGetItem(States.StringSplit($.csv, ','), 1)",
                    "obj.$": "States.StringToJson(States.Format('\\{\"a\":\\{\\}, \"n\":{}\\}', $.n))",
                    "last.$": "$.items[-1].id",
                    "ids.$": "$.items[*].id",
                    "big.$": "$.items[?(@.id > 1)].id",
                    "spaced.$": "$['odd key']"
                },
                "End": true
            }
        }
    });
    let input = json!({
        "csv": "x,y,z",
        "n": 5,
        "items": [{"id": 1}, {"id": 2}, {"id": 3}],
        "odd key": "ok"
    });
    let (status, output, error, cause) = run(&client, "nested-intrinsics", def, input).await;
    assert_eq!(status, "SUCCEEDED", "{error:?} {cause:?}");
    assert_eq!(
        output.unwrap(),
        json!({
            "second": "y",
            "obj": {"a": {}, "n": 5},
            "last": 3,
            "ids": [1, 2, 3],
            "big": [2, 3],
            "spaced": "ok"
        })
    );
}

#[tokio::test]
async fn sfn_missing_path_fails_with_states_runtime() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let cases = [
        (
            "missing-input-path",
            json!({"Type": "Pass", "InputPath": "$.nope", "End": true}),
        ),
        (
            "missing-output-path",
            json!({"Type": "Pass", "OutputPath": "$.nope", "End": true}),
        ),
        (
            "missing-parameters-ref",
            json!({"Type": "Pass", "Parameters": {"v.$": "$.nope"}, "End": true}),
        ),
        (
            "missing-items-path",
            json!({
                "Type": "Map",
                "ItemsPath": "$.nope",
                "ItemProcessor": {
                    "StartAt": "I",
                    "States": {"I": {"Type": "Pass", "End": true}}
                },
                "End": true
            }),
        ),
    ];
    for (name, state) in cases {
        let def = json!({"StartAt": "S", "States": {"S": state}});
        let (status, _, error, cause) = run(&client, name, def, json!({"present": 1})).await;
        assert_eq!(status, "FAILED", "{name}");
        assert_eq!(error.as_deref(), Some("States.Runtime"), "{name}");
        assert!(cause.unwrap_or_default().contains("$.nope"), "{name}");
    }

    // A Catch on States.ALL does not swallow States.Runtime.
    let def = json!({
        "StartAt": "M",
        "States": {
            "M": {
                "Type": "Map",
                "ItemsPath": "$.nope",
                "ItemProcessor": {
                    "StartAt": "I",
                    "States": {"I": {"Type": "Pass", "End": true}}
                },
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Caught"}],
                "End": true
            },
            "Caught": {"Type": "Succeed"}
        }
    });
    let (status, _, error, _) = run(&client, "runtime-uncatchable", def, json!({})).await;
    assert_eq!(status, "FAILED");
    assert_eq!(error.as_deref(), Some("States.Runtime"));
}

#[tokio::test]
async fn sfn_item_selector_reads_map_context() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let def = json!({
        "StartAt": "M",
        "States": {
            "M": {
                "Type": "Map",
                "ItemsPath": "$.xs",
                "ItemSelector": {
                    "value.$": "$$.Map.Item.Value",
                    "index.$": "$$.Map.Item.Index",
                    "tag.$": "$.tag"
                },
                "ItemProcessor": {
                    "StartAt": "I",
                    "States": {"I": {"Type": "Pass", "End": true}}
                },
                "End": true
            }
        }
    });
    let (status, output, error, cause) = run(
        &client,
        "item-selector",
        def,
        json!({"xs": ["a", "b"], "tag": "t"}),
    )
    .await;
    assert_eq!(status, "SUCCEEDED", "{error:?} {cause:?}");
    assert_eq!(
        output.unwrap(),
        json!([
            {"value": "a", "index": 0, "tag": "t"},
            {"value": "b", "index": 1, "tag": "t"}
        ])
    );
}
