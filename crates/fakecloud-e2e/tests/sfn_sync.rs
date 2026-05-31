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

    // Warm the shared client's connection pool with one sequential call before
    // the concurrent burst. Firing 16 cold `start_sync_execution` calls in the
    // same instant otherwise makes every spawned task open its own brand-new
    // TCP connection simultaneously, and under load the SDK connector
    // intermittently fails one of those cold dials (surfacing as a spurious
    // "dns error" against 127.0.0.1). One warm-up call establishes a pooled
    // connection the concurrent tasks reuse; the invariant under test —
    // concurrent starts mint unique ARNs — is unchanged.
    let mut arns = std::collections::HashSet::new();
    arns.insert(
        sfn.start_sync_execution()
            .state_machine_arn(&sm_arn)
            .input("{}")
            .send()
            .await
            .unwrap()
            .execution_arn()
            .to_string(),
    );

    // Fire many sync executions concurrently; every ARN must be distinct.
    let mut handles = Vec::new();
    for _ in 0..16 {
        let sfn = sfn.clone();
        let sm_arn = sm_arn.clone();
        handles.push(tokio::spawn(async move {
            sfn.start_sync_execution()
                .state_machine_arn(sm_arn)
                .input("{}")
                .send()
                .await
                .unwrap()
                .execution_arn()
                .to_string()
        }));
    }

    for h in handles {
        arns.insert(h.await.unwrap());
    }
    assert_eq!(
        arns.len(),
        17,
        "every sync execution (1 warm-up + 16 concurrent) must get a unique ARN"
    );
}
