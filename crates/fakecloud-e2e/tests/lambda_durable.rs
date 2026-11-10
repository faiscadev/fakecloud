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

const flaked = new Set();

export const handler = withDurableExecution(async (event, context) => {
  const doubled = await context.step("double", async () => (event.n ?? 1) * 2);
  let flaky = null;
  if (event.retryKey) {
    flaky = await context.step("flaky", async () => {
      if (!flaked.has(event.retryKey)) {
        flaked.add(event.retryKey);
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

/// `npm install` the SDK once per target directory.
fn sdk_install_dir() -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("durable-sdk-js-{SDK_VERSION}"));
    let marker = dir.join("node_modules/@aws/durable-execution-sdk-js/package.json");
    if marker.exists() {
        return dir;
    }
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("package.json"),
        r#"{"name":"durable-e2e","private":true}"#,
    )
    .unwrap();
    let mut last = String::new();
    for attempt in 1..=3 {
        let out = std::process::Command::new("npm")
            .args([
                "install",
                "--no-audit",
                "--no-fund",
                "--omit=dev",
                &format!("@aws/durable-execution-sdk-js@{SDK_VERSION}"),
            ])
            .current_dir(&dir)
            .output()
            .unwrap_or_else(|e| {
                panic!("npm is required to bundle the durable execution SDK for this test: {e}")
            });
        if out.status.success() && marker.exists() {
            return dir;
        }
        last = String::from_utf8_lossy(&out.stderr).into_owned();
        std::thread::sleep(Duration::from_secs(attempt * 5));
    }
    panic!("npm install of @aws/durable-execution-sdk-js@{SDK_VERSION} failed: {last}");
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

/// Poll the history until the callback has started; returns its id.
async fn wait_for_callback(client: &aws_sdk_lambda::Client, arn: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let history = client
            .get_durable_execution_history()
            .durable_execution_arn(arn)
            .send()
            .await
            .unwrap();
        if let Some(id) = history
            .events()
            .iter()
            .find(|e| e.event_type() == Some(&EventType::CallbackStarted))
            .and_then(|e| e.callback_started_details())
            .map(|d| d.callback_id().to_string())
        {
            return id;
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
