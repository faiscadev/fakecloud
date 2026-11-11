//! Lambda durable functions, end to end: a real function built on the
//! official durable execution SDK (`@aws/durable-execution-sdk-js`) runs in
//! fakecloud's Node.js runtime container, checkpoints through
//! `CheckpointDurableExecution`, suspends on waits, step retries and
//! callbacks, and is re-invoked to replay until it finishes.
//!
//! The SDK is installed with npm into the target directory on first use
//! (pinned version) and bundled into the deployment package. Without npm
//! these tests fail rather than skip.

mod helpers;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{
    DurableConfig, ErrorObject, EventType, ExecutionStatus, FunctionCode, InvocationType, Runtime,
};
use helpers::TestServer;

const SDK_VERSION: &str = "3.0.0";

/// The durable handler under test. Each feature is switched on by the
/// execution input so one deployment covers every scenario.
const HANDLER: &str = r#"
import { withDurableExecution } from "@aws/durable-execution-sdk-js";

export const handler = withDurableExecution(async (event, context) => {
  const doubled = await context.step("double", async () => (event.n ?? 1) * 2);
  let flaky = null;
  if (event.retryKey) {
    // The attempt number comes from the service's StepDetails.Attempt, so
    // the first attempt fails whichever container runs it.
    flaky = await context.step("flaky", async (ctx) => {
      if (ctx.attempt === 1) {
        throw new Error("transient failure");
      }
      return "recovered";
    }, { retryStrategy: (error, attempt) => ({ shouldRetry: attempt < 3, delay: { seconds: 1 } }) });
  }
  if (event.waitSeconds) {
    await context.wait("pause", { seconds: event.waitSeconds });
  }
  let chained = null;
  if (event.chainTarget) {
    chained = await context.invoke("call-target", event.chainTarget, { hello: "world" });
  }
  let approval = null;
  if (event.callback) {
    approval = await context.waitForCallback("approval", async (callbackId) => {
      console.log("CALLBACK_ID " + callbackId);
    }, { timeout: { seconds: 300 }, heartbeatTimeout: { seconds: 120 } });
  }
  return { doubled, flaky, chained, approval };
});
"#;

const TARGET_HANDLER: &str = r#"
export const handler = async (event) => ({ echo: event });
"#;

fn zip_dir(writer: &mut zip::ZipWriter<std::io::Cursor<Vec<u8>>>, root: &Path, dir: &Path) {
    let mut entries: Vec<_> = std::fs::read_dir(dir).unwrap().flatten().collect();
    entries.sort_by_key(|e| e.path());
    for entry in entries {
        let path = entry.path();
        let rel = path
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let ft = entry.file_type().unwrap();
        if ft.is_dir() {
            zip_dir(writer, root, &path);
        } else if ft.is_file() {
            let options = zip::write::SimpleFileOptions::default().unix_permissions(0o644);
            writer.start_file(rel, options).unwrap();
            writer.write_all(&std::fs::read(&path).unwrap()).unwrap();
        }
    }
}

fn sdk_marker(dir: &Path) -> PathBuf {
    dir.join("node_modules/@aws/durable-execution-sdk-js/package.json")
}

/// `npm install` the SDK once per target directory.
///
/// nextest runs every test in its own process, so several processes reach
/// this at once. Concurrent `npm install`s into one directory leave a tree
/// that has the marker but misses files, and every later run would bundle
/// it. Each process therefore installs into its own staging directory and
/// renames the complete tree into place; the final directory only ever
/// appears complete, and a process that loses the rename uses the winner's.
fn sdk_install_dir() -> PathBuf {
    let tmp = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = tmp.join(format!("durable-sdk-js-{SDK_VERSION}"));
    if sdk_marker(&dir).exists() {
        return dir;
    }
    let staging = tmp.join(format!(
        "durable-sdk-js-{SDK_VERSION}.staging-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(
        staging.join("package.json"),
        r#"{"name":"durable-e2e","private":true}"#,
    )
    .unwrap();
    let mut last = String::new();
    let mut installed = false;
    for attempt in 1..=3 {
        let out = std::process::Command::new("npm")
            .args([
                "install",
                "--no-audit",
                "--no-fund",
                "--omit=dev",
                &format!("@aws/durable-execution-sdk-js@{SDK_VERSION}"),
            ])
            .current_dir(&staging)
            .output()
            .unwrap_or_else(|e| {
                panic!("npm is required to bundle the durable execution SDK for this test: {e}")
            });
        if out.status.success() && sdk_marker(&staging).exists() {
            installed = true;
            break;
        }
        last = String::from_utf8_lossy(&out.stderr).into_owned();
        std::thread::sleep(Duration::from_secs(attempt * 5));
    }
    if !installed {
        let _ = std::fs::remove_dir_all(&staging);
        panic!("npm install of @aws/durable-execution-sdk-js@{SDK_VERSION} failed: {last}");
    }
    // A directory without the marker is a leftover of an interrupted
    // install from before staging; only renames create it now.
    if dir.exists() && !sdk_marker(&dir).exists() {
        let _ = std::fs::remove_dir_all(&dir);
    }
    if std::fs::rename(&staging, &dir).is_err() {
        let _ = std::fs::remove_dir_all(&staging);
    }
    assert!(
        sdk_marker(&dir).exists(),
        "durable execution SDK missing from {}",
        dir.display()
    );
    dir
}

/// The deployment package: the handler plus the SDK's `node_modules`.
fn durable_zip() -> Vec<u8> {
    static ZIP: OnceLock<Vec<u8>> = OnceLock::new();
    ZIP.get_or_init(|| {
        let dir = sdk_install_dir();
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default().unix_permissions(0o644);
        writer.start_file("index.mjs", options).unwrap();
        writer.write_all(HANDLER.as_bytes()).unwrap();
        zip_dir(&mut writer, &dir, &dir.join("node_modules"));
        writer.finish().unwrap().into_inner()
    })
    .clone()
}

fn target_zip() -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default().unix_permissions(0o644);
    writer.start_file("index.mjs", options).unwrap();
    writer.write_all(TARGET_HANDLER.as_bytes()).unwrap();
    writer.finish().unwrap().into_inner()
}

async fn create_function(
    client: &aws_sdk_lambda::Client,
    name: &str,
    zip: Vec<u8>,
    durable: Option<DurableConfig>,
) {
    client
        .create_function()
        .function_name(name)
        .runtime(Runtime::Nodejs22x)
        .role("arn:aws:iam::123456789012:role/durable-role")
        .handler("index.handler")
        .timeout(60)
        .memory_size(256)
        .set_durable_config(durable)
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();
}

async fn create_durable_function(client: &aws_sdk_lambda::Client, name: &str) {
    create_function(
        client,
        name,
        durable_zip(),
        Some(
            DurableConfig::builder()
                .execution_timeout(300)
                .retention_period_in_days(1)
                .build(),
        ),
    )
    .await;
}

/// Poll until the execution leaves RUNNING.
async fn wait_until_done(client: &aws_sdk_lambda::Client, arn: &str) -> ExecutionStatus {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let got = client
            .get_durable_execution()
            .durable_execution_arn(arn)
            .send()
            .await
            .unwrap();
        if *got.status() != ExecutionStatus::Running {
            return got.status().clone();
        }
        assert!(Instant::now() < deadline, "execution {arn} never finished");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Poll the history until the callback has started and the invocation that
/// started it has ended (the execution is suspended on it); returns its id.
async fn wait_for_callback(client: &aws_sdk_lambda::Client, arn: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let history = client
            .get_durable_execution_history()
            .durable_execution_arn(arn)
            .send()
            .await
            .unwrap();
        let events = history.events();
        if let Some(pos) = events
            .iter()
            .position(|e| e.event_type() == Some(&EventType::CallbackStarted))
        {
            let suspended = events[pos..]
                .iter()
                .any(|e| e.event_type() == Some(&EventType::InvocationCompleted));
            if suspended {
                if let Some(d) = events[pos].callback_started_details() {
                    return d.callback_id().to_string();
                }
            }
        }
        assert!(Instant::now() < deadline, "callback of {arn} never started");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn history_types(client: &aws_sdk_lambda::Client, arn: &str) -> Vec<String> {
    client
        .get_durable_execution_history()
        .durable_execution_arn(arn)
        .include_execution_data(true)
        .max_items(1000)
        .send()
        .await
        .unwrap()
        .events()
        .iter()
        .map(|e| e.event_type().unwrap().as_str().to_string())
        .collect()
}

/// Step, step retry, wait, chained invoke and callback, driven to
/// SUCCEEDED, with the execution APIs checked along the way.
#[tokio::test]
async fn durable_execution_runs_steps_retries_waits_chains_and_callbacks() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    create_durable_function(&client, "durable-orders").await;
    create_function(&client, "durable-target", target_zip(), None).await;

    // Durable functions are invoked through a version or alias.
    let err = client
        .invoke()
        .function_name("durable-orders")
        .payload(Blob::new(b"{}".to_vec()))
        .send()
        .await
        .unwrap_err();
    let err = err.into_service_error();
    assert!(err.is_invalid_parameter_value_exception(), "{err:?}");
    assert_eq!(
        err.meta().message(),
        Some("You cannot invoke a durable function using an unqualified ARN.")
    );

    let input = r#"{"n":21,"retryKey":"order-1","waitSeconds":1,"chainTarget":"durable-target","callback":true}"#;
    let started = client
        .invoke()
        .function_name("durable-orders:$LATEST")
        .invocation_type(InvocationType::Event)
        .durable_execution_name("order-1")
        .payload(Blob::new(input.as_bytes().to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(started.status_code(), 202);
    let arn = started.durable_execution_arn().unwrap().to_string();
    assert!(
        arn.starts_with(
            "arn:aws:lambda:us-east-1:123456789012:function:durable-orders:$LATEST/durable-execution/order-1/"
        ),
        "{arn}"
    );

    // The same name and input reattach; another input conflicts.
    let again = client
        .invoke()
        .function_name("durable-orders:$LATEST")
        .invocation_type(InvocationType::Event)
        .durable_execution_name("order-1")
        .payload(Blob::new(input.as_bytes().to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(again.durable_execution_arn(), Some(arn.as_str()));
    let conflict = client
        .invoke()
        .function_name("durable-orders:$LATEST")
        .durable_execution_name("order-1")
        .payload(Blob::new(b"{\"n\":1}".to_vec()))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(
        conflict.is_durable_execution_already_started_exception(),
        "{conflict:?}"
    );

    let callback_id = wait_for_callback(&client, &arn).await;
    let running = client
        .get_durable_execution()
        .durable_execution_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(*running.status(), ExecutionStatus::Running);
    assert_eq!(running.durable_execution_name(), "order-1");
    assert_eq!(running.input_payload(), Some(input));
    assert_eq!(running.version(), Some("$LATEST"));

    // The state API is reserved to the invocation holding a token.
    let err = client
        .get_durable_execution_state()
        .durable_execution_arn(&arn)
        .checkpoint_token("AAAA")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_invalid_parameter_value_exception(), "{err:?}");

    client
        .send_durable_execution_callback_heartbeat()
        .callback_id(&callback_id)
        .send()
        .await
        .unwrap();
    client
        .send_durable_execution_callback_success()
        .callback_id(&callback_id)
        .result(Blob::new(br#"{"approved":true}"#.to_vec()))
        .send()
        .await
        .unwrap();
    let late = client
        .send_durable_execution_callback_success()
        .callback_id(&callback_id)
        .result(Blob::new(b"1".to_vec()))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(late.is_callback_timeout_exception(), "{late:?}");

    assert_eq!(
        wait_until_done(&client, &arn).await,
        ExecutionStatus::Succeeded
    );
    let done = client
        .get_durable_execution()
        .durable_execution_arn(&arn)
        .send()
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_str(done.result().unwrap()).unwrap();
    assert_eq!(
        result,
        serde_json::json!({
            "doubled": 42,
            "flaky": "recovered",
            "chained": {"echo": {"hello": "world"}},
            "approval": "{\"approved\":true}",
        })
    );
    assert!(done.end_timestamp().is_some());

    let types = history_types(&client, &arn).await;
    for expected in [
        "ExecutionStarted",
        "StepStarted",
        "StepSucceeded",
        "StepFailed",
        "WaitStarted",
        "WaitSucceeded",
        "ChainedInvokeStarted",
        "ChainedInvokeSucceeded",
        "ContextStarted",
        "CallbackStarted",
        "CallbackSucceeded",
        "ContextSucceeded",
        "InvocationCompleted",
        "ExecutionSucceeded",
    ] {
        assert!(
            types.iter().any(|t| t == expected),
            "{expected} missing from {types:?}"
        );
    }
    assert_eq!(types.first().map(String::as_str), Some("ExecutionStarted"));
    assert_eq!(types.last().map(String::as_str), Some("ExecutionSucceeded"));
    // Suspensions (retry, wait, chained invoke, callback) mean several
    // invocations replayed the execution.
    assert!(
        types.iter().filter(|t| *t == "InvocationCompleted").count() >= 4,
        "{types:?}"
    );

    let listed = client
        .list_durable_executions_by_function()
        .function_name("durable-orders")
        .statuses(ExecutionStatus::Succeeded)
        .send()
        .await
        .unwrap();
    assert!(listed
        .durable_executions()
        .iter()
        .any(|e| e.durable_execution_arn() == arn));
}

/// A callback completed with a failure fails the execution with the
/// callback's error, as the SDK surfaces it.
#[tokio::test]
async fn durable_callback_failure_fails_the_execution() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    create_durable_function(&client, "durable-approval").await;

    let started = client
        .invoke()
        .function_name("durable-approval:$LATEST")
        .invocation_type(InvocationType::Event)
        .payload(Blob::new(br#"{"n":7,"callback":true}"#.to_vec()))
        .send()
        .await
        .unwrap();
    let arn = started.durable_execution_arn().unwrap().to_string();
    let callback_id = wait_for_callback(&client, &arn).await;
    client
        .send_durable_execution_callback_failure()
        .callback_id(&callback_id)
        .error(
            ErrorObject::builder()
                .error_type("Rejected")
                .error_message("denied")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        wait_until_done(&client, &arn).await,
        ExecutionStatus::Failed
    );
    let failed = client
        .get_durable_execution()
        .durable_execution_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        failed.error().and_then(|e| e.error_message()),
        Some("denied")
    );
    let types = history_types(&client, &arn).await;
    assert!(types.iter().any(|t| t == "CallbackFailed"), "{types:?}");
    assert_eq!(types.last().map(String::as_str), Some("ExecutionFailed"));
}

/// Stopping a suspended execution ends it; the open callback is closed and
/// a second stop reports the same time.
#[tokio::test]
async fn durable_stop_ends_a_suspended_execution() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    create_durable_function(&client, "durable-stoppable").await;

    let started = client
        .invoke()
        .function_name("durable-stoppable:$LATEST")
        .invocation_type(InvocationType::Event)
        .payload(Blob::new(br#"{"callback":true}"#.to_vec()))
        .send()
        .await
        .unwrap();
    let arn = started.durable_execution_arn().unwrap().to_string();
    let callback_id = wait_for_callback(&client, &arn).await;
    let first = client
        .stop_durable_execution()
        .durable_execution_arn(&arn)
        .error(
            ErrorObject::builder()
                .error_type("Manual")
                .error_message("operator stop")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let second = client
        .stop_durable_execution()
        .durable_execution_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(first.stop_timestamp(), second.stop_timestamp());
    let stopped = client
        .get_durable_execution()
        .durable_execution_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(*stopped.status(), ExecutionStatus::Stopped);
    assert_eq!(stopped.error().and_then(|e| e.error_type()), Some("Manual"));
    let late = client
        .send_durable_execution_callback_success()
        .callback_id(&callback_id)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(late.is_callback_timeout_exception(), "{late:?}");
    let types = history_types(&client, &arn).await;
    assert_eq!(types.last().map(String::as_str), Some("ExecutionStopped"));
}

/// A suspended execution is persisted with the Lambda state: after a
/// restart its callback still completes it and the function replays to the
/// end.
#[tokio::test]
async fn durable_execution_survives_a_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().display().to_string();
    // `start_persistent` runs without a container runtime; this test needs
    // the function to run on both sides of the restart.
    let mut server = TestServer::start_full(
        &[],
        &["--storage-mode", "persistent", "--data-path", &data_path],
    )
    .await;
    let client = server.lambda_client().await;
    create_durable_function(&client, "durable-persisted").await;
    let started = client
        .invoke()
        .function_name("durable-persisted:$LATEST")
        .invocation_type(InvocationType::Event)
        .durable_execution_name("persisted-1")
        .payload(Blob::new(br#"{"n":4,"callback":true}"#.to_vec()))
        .send()
        .await
        .unwrap();
    let arn = started.durable_execution_arn().unwrap().to_string();
    let callback_id = wait_for_callback(&client, &arn).await;
    // The scheduler writes the snapshot on its next pass (every 200ms).
    tokio::time::sleep(Duration::from_secs(1)).await;

    server.restart().await;
    let client = server.lambda_client().await;
    let resumed = client
        .get_durable_execution()
        .durable_execution_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(*resumed.status(), ExecutionStatus::Running);
    client
        .send_durable_execution_callback_success()
        .callback_id(&callback_id)
        .result(Blob::new(b"\"after-restart\"".to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        wait_until_done(&client, &arn).await,
        ExecutionStatus::Succeeded
    );
    let done = client
        .get_durable_execution()
        .durable_execution_arn(&arn)
        .send()
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_str(done.result().unwrap()).unwrap();
    assert_eq!(result["doubled"], 8);
    assert_eq!(result["approval"], "\"after-restart\"");
}

/// A synchronous invoke waiting on a durable execution's callback does not
/// hold the server's shutdown past its grace period, and the execution,
/// persisted, resumes after the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_inflight_sync_invoke_does_not_block_shutdown() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().display().to_string();
    let mut server = TestServer::start_full(
        &[],
        &["--storage-mode", "persistent", "--data-path", &data_path],
    )
    .await;
    let client = server.lambda_client().await;
    create_durable_function(&client, "durable-shutdown").await;
    let waiting = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .invoke()
                .function_name("durable-shutdown:$LATEST")
                .durable_execution_name("shutdown-1")
                .payload(Blob::new(br#"{"n":5,"callback":true}"#.to_vec()))
                .send()
                .await
        })
    };
    let arn = loop {
        let listed = client
            .list_durable_executions_by_function()
            .function_name("durable-shutdown")
            .send()
            .await
            .unwrap();
        if let Some(e) = listed.durable_executions().first() {
            break e.durable_execution_arn().to_string();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let callback_id = wait_for_callback(&client, &arn).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !waiting.is_finished(),
        "the sync invoke waits for the callback"
    );
    // 5 s grace, then the drains and container cleanup.
    let took = tokio::task::block_in_place(|| server.terminate(Duration::from_secs(30)))
        .expect("the server exits on its own while the invoke is still waiting");
    assert!(
        took >= Duration::from_secs(4),
        "in-flight requests get the grace period: {took:?}"
    );
    assert!(
        waiting.await.unwrap().is_err(),
        "the waiting caller is dropped"
    );

    server.restart().await;
    let client = server.lambda_client().await;
    client
        .send_durable_execution_callback_success()
        .callback_id(&callback_id)
        .result(Blob::new(b"\"resumed\"".to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        wait_until_done(&client, &arn).await,
        ExecutionStatus::Succeeded
    );
    let done = client
        .get_durable_execution()
        .durable_execution_arn(&arn)
        .send()
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_str(done.result().unwrap()).unwrap();
    assert_eq!(result["doubled"], 10);
}

/// An event source mapping onto a durable function's version starts a
/// durable execution per batch, not a plain invocation.
#[tokio::test]
async fn durable_function_behind_an_sqs_event_source_mapping() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let sqs = server.sqs_client().await;
    create_durable_function(&client, "durable-esm").await;
    let version = client
        .publish_version()
        .function_name("durable-esm")
        .send()
        .await
        .unwrap();
    let version = version.version().unwrap().to_string();
    let queue = sqs
        .create_queue()
        .queue_name("durable-esm-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = attrs
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap()
        .clone();
    client
        .create_event_source_mapping()
        .function_name(format!("durable-esm:{version}"))
        .event_source_arn(&queue_arn)
        .batch_size(1)
        .send()
        .await
        .unwrap();
    sqs.send_message()
        .queue_url(&queue_url)
        .message_body("{\"order\":1}")
        .send()
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(180);
    let arn = loop {
        let listed = client
            .list_durable_executions_by_function()
            .function_name("durable-esm")
            .send()
            .await
            .unwrap();
        if let Some(e) = listed.durable_executions().first() {
            break e.durable_execution_arn().to_string();
        }
        assert!(
            Instant::now() < deadline,
            "the mapping never started an execution"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(
        wait_until_done(&client, &arn).await,
        ExecutionStatus::Succeeded
    );
    let done = client
        .get_durable_execution()
        .durable_execution_arn(&arn)
        .send()
        .await
        .unwrap();
    let input: serde_json::Value = serde_json::from_str(done.input_payload().unwrap()).unwrap();
    assert_eq!(input["Records"][0]["body"], "{\"order\":1}");
    assert_eq!(done.version(), Some(version.as_str()));
}

/// The first durable execution of `function`, once one exists.
async fn first_execution(client: &aws_sdk_lambda::Client, function: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let listed = client
            .list_durable_executions_by_function()
            .function_name(function)
            .send()
            .await
            .unwrap();
        if let Some(e) = listed.durable_executions().first() {
            return e.durable_execution_arn().to_string();
        }
        assert!(
            Instant::now() < deadline,
            "no durable execution of {function} started"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Asynchronous integrations (SNS, EventBridge) delivering to a durable
/// function's qualified ARN start a durable execution, as an `Event`
/// invoke does, rather than running the handler as a plain function.
#[tokio::test]
async fn async_integrations_start_durable_executions() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    create_durable_function(&client, "durable-sns").await;
    create_durable_function(&client, "durable-events").await;
    let qualified =
        |name: &str| format!("arn:aws:lambda:us-east-1:123456789012:function:{name}:$LATEST");

    let sns = server.sns_client().await;
    let topic = sns
        .create_topic()
        .name("durable-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();
    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("lambda")
        .endpoint(qualified("durable-sns"))
        .send()
        .await
        .unwrap();
    sns.publish()
        .topic_arn(&topic_arn)
        .message("{\"n\":3}")
        .send()
        .await
        .unwrap();

    let events = server.eventbridge_client().await;
    events
        .put_rule()
        .name("durable-rule")
        .event_pattern(r#"{"source":["app.orders"]}"#)
        .send()
        .await
        .unwrap();
    events
        .put_targets()
        .rule("durable-rule")
        .targets(
            aws_sdk_eventbridge::types::Target::builder()
                .id("durable")
                .arn(qualified("durable-events"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    events
        .put_events()
        .entries(
            aws_sdk_eventbridge::types::PutEventsRequestEntry::builder()
                .source("app.orders")
                .detail_type("OrderPlaced")
                .detail(r#"{"order":1}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let sns_exec = first_execution(&client, "durable-sns").await;
    let events_exec = first_execution(&client, "durable-events").await;
    for arn in [&sns_exec, &events_exec] {
        assert_eq!(
            wait_until_done(&client, arn).await,
            ExecutionStatus::Succeeded,
            "{arn}"
        );
    }
    let input = |arn: String| {
        let client = client.clone();
        async move {
            let done = client
                .get_durable_execution()
                .durable_execution_arn(&arn)
                .send()
                .await
                .unwrap();
            serde_json::from_str::<serde_json::Value>(done.input_payload().unwrap()).unwrap()
        }
    };
    assert_eq!(
        input(sns_exec).await["Records"][0]["Sns"]["Message"],
        "{\"n\":3}"
    );
    assert_eq!(input(events_exec).await["source"], "app.orders");
}

/// A raw Lambda REST call (the vendored SDK predates `DurableConfig.KMSKeyArn`
/// and the KMS errors of the durable operations): status, JSON body, and
/// error code.
async fn lambda_raw(
    server: &TestServer,
    method: reqwest::Method,
    path: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value, Option<String>) {
    let mut req = reqwest::Client::new()
        .request(method, format!("{}{path}", server.endpoint()))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=0",
        );
    if let Some(b) = body {
        req = req
            .header("content-type", "application/json")
            .body(b.to_string());
    }
    let resp = req.send().await.unwrap();
    let status = resp.status().as_u16();
    let header_code = resp
        .headers()
        .get("x-amz-error-code")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let text = resp.text().await.unwrap();
    let body: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    let code = header_code.or_else(|| body["__type"].as_str().map(str::to_string));
    (status, body, code)
}

fn encode_label(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// `DurableConfig` validation and the customer managed key, against
/// fakecloud's KMS: the key policy must admit the Lambda service principal,
/// and once the key is disabled or pending deletion the execution's data
/// can no longer be read while its metadata still can.
#[tokio::test]
async fn durable_config_validation_and_customer_managed_key() {
    use base64::Engine as _;
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let kms = server.kms_client().await;
    let zip = base64::engine::general_purpose::STANDARD.encode(durable_zip());
    let create = |name: &str, cfg: serde_json::Value| {
        let body = serde_json::json!({
            "FunctionName": name,
            "Runtime": "nodejs22.x",
            "Role": "arn:aws:iam::123456789012:role/durable-role",
            "Handler": "index.handler",
            "Code": {"ZipFile": zip},
            "DurableConfig": cfg,
        });
        lambda_raw(
            &server,
            reqwest::Method::POST,
            "/2015-03-31/functions",
            Some(body),
        )
    };

    let (status, body, code) = create("durable-no-timeout", serde_json::json!({})).await;
    assert_eq!(
        (status, code.as_deref()),
        (400, Some("InvalidParameterValueException"))
    );
    assert_eq!(
        body["message"],
        "You cannot create a function with a durable configuration without an executionTimeout"
    );
    let (status, _, code) = create(
        "durable-bad-retention",
        serde_json::json!({"ExecutionTimeout": 60, "RetentionPeriodInDays": 91}),
    )
    .await;
    assert_eq!(
        (status, code.as_deref()),
        (400, Some("ValidationException"))
    );

    // A key whose policy only admits the account root.
    let key = kms.create_key().send().await.unwrap();
    let key_arn = key.key_metadata().unwrap().arn().unwrap().to_string();
    let key_id = key.key_metadata().unwrap().key_id().to_string();
    let with_key = serde_json::json!({"ExecutionTimeout": 300, "KMSKeyArn": key_arn});
    let (status, _, code) = create("durable-kms", with_key.clone()).await;
    assert_eq!(
        (status, code.as_deref()),
        (403, Some("AccessDeniedException"))
    );

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::123456789012:root"}, "Action": "kms:*", "Resource": "*"},
            {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": ["kms:GenerateDataKey", "kms:Decrypt"], "Resource": "*"}
        ]
    });
    kms.put_key_policy()
        .key_id(&key_id)
        .policy_name("default")
        .policy(policy.to_string())
        .send()
        .await
        .unwrap();
    let (status, body, _) = create("durable-kms", with_key).await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["DurableConfig"]["KMSKeyArn"], key_arn.as_str());
    let missing = "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000000";
    let (status, _, code) = create(
        "durable-kms-missing",
        serde_json::json!({"ExecutionTimeout": 300, "KMSKeyArn": missing}),
    )
    .await;
    assert_eq!(
        (status, code.as_deref()),
        (400, Some("InvalidParameterValueException"))
    );

    let resp = client
        .invoke()
        .function_name("durable-kms:$LATEST")
        .payload(Blob::new(br#"{"n":2}"#.to_vec()))
        .send()
        .await
        .unwrap();
    let arn = resp.durable_execution_arn().unwrap().to_string();
    let path = format!("/2025-12-01/durable-executions/{}", encode_label(&arn));
    let (status, got, _) = lambda_raw(&server, reqwest::Method::GET, &path, None).await;
    assert_eq!(status, 200);
    assert_eq!(got["DurableConfig"]["KMSKeyArn"], key_arn.as_str());

    kms.disable_key().key_id(&key_id).send().await.unwrap();
    let (status, _, code) = lambda_raw(&server, reqwest::Method::GET, &path, None).await;
    assert_eq!(
        (status, code.as_deref()),
        (502, Some("KMSDisabledException"))
    );
    let (status, meta, _) = lambda_raw(
        &server,
        reqwest::Method::GET,
        &format!("{path}?IncludeExecutionData=false"),
        None,
    )
    .await;
    assert_eq!((status, meta["Status"].as_str()), (200, Some("SUCCEEDED")));

    kms.schedule_key_deletion()
        .key_id(&key_id)
        .pending_window_in_days(7)
        .send()
        .await
        .unwrap();
    let (status, _, code) = lambda_raw(
        &server,
        reqwest::Method::GET,
        &format!("{path}/history?IncludeExecutionData=true"),
        None,
    )
    .await;
    assert_eq!(
        (status, code.as_deref()),
        (502, Some("KMSInvalidStateException"))
    );
}

/// A synchronous invoke waits for the whole execution, waits included, and
/// answers with its result.
#[tokio::test]
async fn durable_sync_invoke_waits_for_the_result() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    create_durable_function(&client, "durable-sync").await;

    let resp = client
        .invoke()
        .function_name("durable-sync:$LATEST")
        .payload(Blob::new(br#"{"n":5,"waitSeconds":2}"#.to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.function_error(), None);
    assert_eq!(resp.executed_version(), Some("$LATEST"));
    let arn = resp.durable_execution_arn().unwrap().to_string();
    let body: serde_json::Value = serde_json::from_slice(resp.payload().unwrap().as_ref()).unwrap();
    assert_eq!(body["doubled"], 10);
    let got = client
        .get_durable_execution()
        .durable_execution_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(*got.status(), ExecutionStatus::Succeeded);
    let elapsed = got.end_timestamp().unwrap().secs() - got.start_timestamp().secs();
    assert!(
        elapsed >= 2,
        "the wait must elapse before the result: {elapsed}s"
    );
}
