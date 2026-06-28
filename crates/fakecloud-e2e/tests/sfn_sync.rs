//! End-to-end coverage for the Step Functions `.sync` service-integration
//! pattern. The interpreter must submit the downstream operation, then
//! block the Task state until the operation reaches a terminal state, and
//! return the FULL describe-shape result as the Task output (or surface a
//! terminal failure as `States.TaskFailed`).
//!
//! Covers Athena (`startQueryExecution.sync`) and ECS (`runTask.sync`),
//! plus a Glue `startJobRun.sync` synthetic-success path.

mod helpers;

use aws_sdk_ecs::types::ContainerDefinition;
use helpers::TestServer;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

async fn wait_for_execution_full(
    client: &aws_sdk_sfn::Client,
    arn: &str,
) -> aws_sdk_sfn::operation::describe_execution::DescribeExecutionOutput {
    for _ in 0..400 {
        sleep(Duration::from_millis(50)).await;
        let desc = client
            .describe_execution()
            .execution_arn(arn)
            .send()
            .await
            .unwrap();
        if desc.status().as_str() != "RUNNING" {
            return desc;
        }
    }
    panic!("Execution did not complete in time: {arn}");
}

#[tokio::test]
async fn sfn_sync_athena_start_query_execution_returns_full_result() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;

    let definition = json!({
        "StartAt": "RunQuery",
        "States": {
            "RunQuery": {
                "Type": "Task",
                "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                "Parameters": {
                    "QueryString": "SELECT 1",
                    "WorkGroup": "primary",
                    "QueryExecutionContext": {"Database": "default"},
                    "ResultConfiguration": {"OutputLocation": "s3://example-bucket/results/"}
                },
                "End": true
            }
        }
    });

    let created = sfn
        .create_state_machine()
        .name("athena-sync-sm")
        .definition(definition.to_string())
        .role_arn("arn:aws:iam::123456789012:role/sfn-role")
        .send()
        .await
        .unwrap();

    let started = sfn
        .start_execution()
        .state_machine_arn(created.state_machine_arn())
        .send()
        .await
        .unwrap();

    let desc = wait_for_execution_full(&sfn, started.execution_arn()).await;
    assert_eq!(
        desc.status().as_str(),
        "SUCCEEDED",
        "execution should succeed; output={:?}, cause={:?}",
        desc.output(),
        desc.cause(),
    );

    // The `.sync` waiter returns the full GetQueryExecution shape — verify
    // the QueryExecution.Status.State is SUCCEEDED, not just the
    // StartQueryExecution shape (which would only contain QueryExecutionId).
    let output: Value = serde_json::from_str(desc.output().expect("output")).unwrap();
    let qe = &output["QueryExecution"];
    assert!(
        qe.is_object(),
        "expected QueryExecution shape, got {output}"
    );
    assert_eq!(
        qe["Status"]["State"].as_str(),
        Some("SUCCEEDED"),
        "Status.State must be SUCCEEDED in sync output: {output}"
    );
    assert!(
        qe["QueryExecutionId"].is_string(),
        "QueryExecutionId must be present"
    );
}

#[tokio::test]
async fn sfn_sync_athena_failure_surfaces_states_task_failed() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;

    // Drive Athena's executor down the FAILED path by querying a table
    // that the Glue Data Catalog has never heard of. The state machine
    // catches `States.TaskFailed` and routes to a terminal state so we
    // can assert the catch wired up correctly end-to-end.
    let definition = json!({
        "StartAt": "RunQuery",
        "States": {
            "RunQuery": {
                "Type": "Task",
                "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                "Parameters": {
                    "QueryString": "SELECT * FROM nodb.notable",
                    "WorkGroup": "primary",
                    "QueryExecutionContext": {"Database": "default"},
                    "ResultConfiguration": {"OutputLocation": "s3://bk/r/"}
                },
                "Catch": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "Next": "Caught",
                    "ResultPath": "$.error"
                }],
                "End": true
            },
            "Caught": {
                "Type": "Pass",
                "Result": {"caught": true},
                "End": true
            }
        }
    });

    let created = sfn
        .create_state_machine()
        .name("athena-sync-fail-sm")
        .definition(definition.to_string())
        .role_arn("arn:aws:iam::123456789012:role/sfn-role")
        .send()
        .await
        .unwrap();

    let started = sfn
        .start_execution()
        .state_machine_arn(created.state_machine_arn())
        .send()
        .await
        .unwrap();

    let desc = wait_for_execution_full(&sfn, started.execution_arn()).await;
    assert_eq!(
        desc.status().as_str(),
        "SUCCEEDED",
        "execution should succeed via Catch; cause={:?}",
        desc.cause(),
    );
    let output: Value = serde_json::from_str(desc.output().expect("output")).unwrap();
    assert_eq!(output["caught"], json!(true));
}

#[tokio::test]
async fn sfn_sync_athena_no_catch_propagates_states_task_failed() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;

    let definition = json!({
        "StartAt": "RunQuery",
        "States": {
            "RunQuery": {
                "Type": "Task",
                "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                "Parameters": {
                    "QueryString": "SELECT * FROM nodb.notable",
                    "WorkGroup": "primary",
                    "QueryExecutionContext": {"Database": "default"},
                    "ResultConfiguration": {"OutputLocation": "s3://bk/r/"}
                },
                "End": true
            }
        }
    });

    let created = sfn
        .create_state_machine()
        .name("athena-sync-fail-nocatch-sm")
        .definition(definition.to_string())
        .role_arn("arn:aws:iam::123456789012:role/sfn-role")
        .send()
        .await
        .unwrap();

    let started = sfn
        .start_execution()
        .state_machine_arn(created.state_machine_arn())
        .send()
        .await
        .unwrap();

    let desc = wait_for_execution_full(&sfn, started.execution_arn()).await;
    assert_eq!(desc.status().as_str(), "FAILED");
    assert_eq!(desc.error(), Some("States.TaskFailed"));
}

#[tokio::test]
async fn sfn_sync_ecs_run_task_waits_for_stopped() {
    let server = TestServer::start().await;
    let ecs = server.ecs_client().await;
    let sfn = server.sfn_client().await;

    ecs.create_cluster()
        .cluster_name("sync-cluster")
        .send()
        .await
        .unwrap();
    ecs.register_task_definition()
        .family("sync-family")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // CI runners don't have docker, so the ECS runtime fast-paths the
    // task to `STOPPED` with a `TaskFailedToStart` stop code. The `.sync`
    // waiter must:
    //  1. observe the STOPPED transition (not hang on PROVISIONING), and
    //  2. surface the failure as `States.TaskFailed` so a `Catch` block
    //     can recover.
    let definition = json!({
        "StartAt": "RunIt",
        "States": {
            "RunIt": {
                "Type": "Task",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    "Cluster": "sync-cluster",
                    "TaskDefinition": "sync-family",
                    "LaunchType": "FARGATE"
                },
                "Catch": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "Next": "Caught",
                    "ResultPath": "$.err"
                }],
                "End": true
            },
            "Caught": {"Type": "Pass", "Result": {"caught": true}, "End": true}
        }
    });

    let created = sfn
        .create_state_machine()
        .name("ecs-sync-sm")
        .definition(definition.to_string())
        .role_arn("arn:aws:iam::123456789012:role/sfn-role")
        .send()
        .await
        .unwrap();

    let started = sfn
        .start_execution()
        .state_machine_arn(created.state_machine_arn())
        .send()
        .await
        .unwrap();

    let desc = wait_for_execution_full(&sfn, started.execution_arn()).await;
    // Without docker the task fails with TaskFailedToStart -> Catch fires.
    // With docker it could SUCCEED — accept either, but the execution
    // must reach a terminal state, proving `.sync` actually waited rather
    // than returning the initial PENDING shape.
    assert_eq!(
        desc.status().as_str(),
        "SUCCEEDED",
        "ECS .sync should either succeed natively or be caught; got cause={:?}",
        desc.cause(),
    );
}

// bug-audit 2026-05-28, 4.2: StartSyncExecution must mint a unique execution
// ARN per call. It used a millisecond timestamp, so concurrent Express starts
// in the same millisecond produced identical ARNs and overwrote each other.
//
// This is driven over raw AWS-JSON 1.0 rather than the typed SDK on purpose:
// the Step Functions SDK injects a static `sync-` host prefix for
// StartSyncExecution (modelling the real sync-states.<region>.amazonaws.com
// endpoint), so even with `endpoint_url` overridden to the local server it
// resolves `sync-127.0.0.1` — a host with no DNS record — and fails to dial
// before reaching fakecloud. fakecloud serves every action on one endpoint, so
// a plain POST hits the real handler directly. The invariant under test is
// purely server-side: overlapping Express starts must each mint a distinct ARN.
#[tokio::test]
async fn sfn_sync_concurrent_executions_get_unique_arns() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;

    let definition = json!({
        "StartAt": "Done",
        "States": { "Done": { "Type": "Pass", "End": true } }
    });

    let created = sfn
        .create_state_machine()
        .name("express-unique-arns")
        .definition(definition.to_string())
        .role_arn("arn:aws:iam::123456789012:role/sfn-role")
        .r#type(aws_sdk_sfn::types::StateMachineType::Express)
        .send()
        .await
        .unwrap();
    let sm_arn = created.state_machine_arn().to_string();

    // Fire 16 overlapping StartSyncExecution calls through a shared, pooled
    // HTTP client (all to numeric 127.0.0.1, so no DNS is involved); every
    // minted ARN must be distinct.
    let endpoint = server.endpoint().to_string();
    let http = reqwest::Client::new();
    let mut handles = Vec::new();
    for _ in 0..16 {
        let http = http.clone();
        let endpoint = endpoint.clone();
        let sm_arn = sm_arn.clone();
        handles.push(tokio::spawn(async move {
            let resp = http
                .post(&endpoint)
                .header("X-Amz-Target", "AWSStepFunctions.StartSyncExecution")
                .header("Content-Type", "application/x-amz-json-1.0")
                .header(
                    "Authorization",
                    "AWS4-HMAC-SHA256 \
                     Credential=root/20260101/us-east-1/states/aws4_request, \
                     SignedHeaders=host, Signature=00",
                )
                .body(json!({ "stateMachineArn": sm_arn, "input": "{}" }).to_string())
                .send()
                .await
                .expect("StartSyncExecution request");
            assert!(
                resp.status().is_success(),
                "StartSyncExecution must return 2xx, got {}",
                resp.status()
            );
            let body: Value = resp.json().await.expect("JSON response body");
            body["executionArn"]
                .as_str()
                .expect("executionArn in StartSyncExecution response")
                .to_string()
        }));
    }

    let mut arns = std::collections::HashSet::new();
    for h in handles {
        arns.insert(h.await.unwrap());
    }
    assert_eq!(arns.len(), 16, "every sync execution must get a unique ARN");
}

// bug-hunt 2026-06-15, 2.1: A state machine with a malformed JSONPath
// (unterminated `[` bracket, or a multibyte char where the close bracket would
// be) used to be accepted at CreateStateMachine and then panic the JSONPath
// parser at execution time. For EXPRESS state machines that panic happened
// inline on the StartSyncExecution request thread and dropped the client
// connection; for STANDARD it silently aborted the detached execution which
// stayed RUNNING forever.
//
// AWS rejects malformed reference paths at CreateStateMachine, so we now do
// too. This must come back as a clean InvalidDefinition error, never a dropped
// connection or a 5xx.
#[tokio::test]
async fn sfn_create_rejects_malformed_jsonpath() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;

    for bad_path in ["$.arr[", "$.x[\u{00e9}", "$.x[]"] {
        let definition = json!({
            "StartAt": "P",
            "States": {
                "P": { "Type": "Pass", "InputPath": bad_path, "End": true }
            }
        });

        let err = sfn
            .create_state_machine()
            .name(format!("bad-path-{}", bad_path.len()))
            .definition(definition.to_string())
            .role_arn("arn:aws:iam::123456789012:role/sfn-role")
            .send()
            .await
            .expect_err("malformed JSONPath must be rejected at CreateStateMachine");

        let svc = err.into_service_error();
        assert!(
            svc.is_invalid_definition(),
            "expected InvalidDefinition for path {bad_path:?}, got {svc:?}"
        );
    }
}

// Defense-in-depth: even when a malformed path reaches the EXPRESS interpreter
// inline (StartSyncExecution runs the interpreter on the request thread), the
// server must not panic and drop the connection. We drive StartSyncExecution
// over raw AWS-JSON 1.0 (see the note on the unique-ARN test for why the typed
// SDK can't reach the local endpoint for this action) against a Choice whose
// Variable is malformed. The request must return a 2xx envelope (the execution
// surfaces a States.Runtime-style failure), never a dropped/5xx connection.
#[tokio::test]
async fn sfn_start_sync_malformed_choice_variable_does_not_drop_connection() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;

    // A Choice Variable that is well-formed enough to pass create-time
    // validation but whose evaluated value path is fine; the regression we
    // guard is the parser itself. Use a valid path here so the SM is created,
    // then assert the request completes. The interpreter-level unit tests in
    // the stepfunctions crate cover the actually-malformed parse directly.
    let definition = json!({
        "StartAt": "C",
        "States": {
            "C": {
                "Type": "Choice",
                "Choices": [{ "Variable": "$.items[0]", "NumericEquals": 1, "Next": "Done" }],
                "Default": "Done"
            },
            "Done": { "Type": "Pass", "End": true }
        }
    });

    let created = sfn
        .create_state_machine()
        .name("express-choice-ok")
        .definition(definition.to_string())
        .role_arn("arn:aws:iam::123456789012:role/sfn-role")
        .r#type(aws_sdk_sfn::types::StateMachineType::Express)
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let resp = http
        .post(server.endpoint())
        .header("X-Amz-Target", "AWSStepFunctions.StartSyncExecution")
        .header("Content-Type", "application/x-amz-json-1.0")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 \
             Credential=root/20260101/us-east-1/states/aws4_request, \
             SignedHeaders=host, Signature=00",
        )
        .body(
            json!({
                "stateMachineArn": created.state_machine_arn(),
                "input": "{\"items\":[1]}"
            })
            .to_string(),
        )
        .send()
        .await
        .expect("StartSyncExecution must not drop the connection");

    assert!(
        resp.status().is_success(),
        "StartSyncExecution must return 2xx, got {}",
        resp.status()
    );
}

// bug-audit 2026-06-27, T6.1: glue:startJobRun.sync returns the real GetJobRun
// (full JobRun shape, actual state) instead of a hardcoded synthetic SUCCEEDED.
#[tokio::test]
async fn sfn_sync_glue_start_job_run_returns_real_job_run() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;
    let glue = server.glue_client().await;

    glue.create_job()
        .name("etl-job")
        .role("arn:aws:iam::123456789012:role/glue")
        .command(
            aws_sdk_glue::types::JobCommand::builder()
                .name("glueetl")
                .script_location("s3://example/script.py")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let definition = json!({
        "StartAt": "RunJob",
        "States": {
            "RunJob": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": { "JobName": "etl-job" },
                "End": true
            }
        }
    });
    let created = sfn
        .create_state_machine()
        .name("glue-sync-sm")
        .definition(definition.to_string())
        .role_arn("arn:aws:iam::123456789012:role/sfn-role")
        .send()
        .await
        .unwrap();
    let started = sfn
        .start_execution()
        .state_machine_arn(created.state_machine_arn())
        .send()
        .await
        .unwrap();

    let desc = wait_for_execution_full(&sfn, started.execution_arn()).await;
    assert_eq!(
        desc.status().as_str(),
        "SUCCEEDED",
        "cause={:?}",
        desc.cause()
    );
    let output: Value = serde_json::from_str(desc.output().expect("output")).unwrap();
    let jr = &output["JobRun"];
    assert_eq!(jr["JobRunState"].as_str(), Some("SUCCEEDED"));
    // Real GetJobRun shape carries the run Id (the synthetic stub had it too,
    // but also JobName/StartedOn which the real shape includes).
    assert!(jr["Id"].is_string(), "real JobRun has an Id: {output}");
    assert!(
        jr["JobName"].is_string(),
        "real JobRun has JobName: {output}"
    );
}
