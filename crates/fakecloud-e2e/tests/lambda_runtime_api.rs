//! fakecloud serves the Lambda Runtime API to every function instance, so a
//! function's outcome is what its runtime reported: a returned value is a
//! success whatever its shape, a raised error is `Unhandled` with the
//! runtime's error object, a failed init / exit / timeout is reported with
//! Lambda's error types. Needs a container runtime (Docker or Podman).

mod helpers;

use std::io::Write;
use std::time::Duration;

use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{
    DestinationConfig, FunctionCode, InvocationType, OnFailure, OnSuccess, Runtime,
};
use aws_sdk_sqs::types::QueueAttributeName;
use aws_types::request_id::RequestId;
use helpers::TestServer;

const ROLE: &str = "arn:aws:iam::123456789012:role/test-role";

/// An error-shaped object, as a handler might legitimately return one.
const ERROR_SHAPED: &str =
    r#"{"errorType": "ValidationError", "errorMessage": "returned, not raised"}"#;

fn make_zip(entries: &[(&str, &str)]) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for (name, content) in entries {
        let options = zip::write::SimpleFileOptions::default().unix_permissions(0o755);
        writer.start_file(*name, options).unwrap();
        writer.write_all(content.as_bytes()).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

async fn create(
    client: &aws_sdk_lambda::Client,
    name: &str,
    runtime: Runtime,
    handler: &str,
    files: &[(&str, &str)],
    timeout: Option<i32>,
) {
    let mut req = client
        .create_function()
        .function_name(name)
        .runtime(runtime)
        .role(ROLE)
        .handler(handler)
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(make_zip(files)))
                .build(),
        );
    if let Some(t) = timeout {
        req = req.timeout(t);
    }
    req.send().await.unwrap();
}

async fn invoke(
    client: &aws_sdk_lambda::Client,
    name: &str,
    payload: &str,
) -> aws_sdk_lambda::operation::invoke::InvokeOutput {
    client
        .invoke()
        .function_name(name)
        .payload(Blob::new(payload.as_bytes().to_vec()))
        .send()
        .await
        .unwrap()
}

fn json(resp: &aws_sdk_lambda::operation::invoke::InvokeOutput) -> serde_json::Value {
    let bytes = resp
        .payload()
        .map(|p| p.as_ref().to_vec())
        .unwrap_or_default();
    serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "payload {:?} is not JSON: {e}",
            String::from_utf8_lossy(&bytes)
        )
    })
}

fn error_shaped() -> serde_json::Value {
    serde_json::from_str(ERROR_SHAPED).unwrap()
}

#[tokio::test]
async fn python_returning_an_error_shaped_object_is_a_success() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let code = format!("def handler(event, context):\n    return {ERROR_SHAPED}\n");
    create(
        &client,
        "py-shaped",
        Runtime::Python312,
        "index.handler",
        &[("index.py", &code)],
        None,
    )
    .await;

    let resp = invoke(&client, "py-shaped", "{}").await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(
        resp.function_error(),
        None,
        "a returned value is never a function error"
    );
    assert_eq!(json(&resp), error_shaped());
}

#[tokio::test]
async fn nodejs_returning_an_error_shaped_object_is_a_success() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let code = format!("exports.handler = async () => ({ERROR_SHAPED});\n");
    create(
        &client,
        "node-shaped",
        Runtime::Nodejs22x,
        "index.handler",
        &[("index.js", &code)],
        None,
    )
    .await;

    let resp = invoke(&client, "node-shaped", "{}").await;
    assert_eq!(resp.function_error(), None);
    assert_eq!(json(&resp), error_shaped());
}

/// A custom runtime talks to the Runtime API itself: it reads the event's
/// headers, answers `/response` with an error-shaped body (a success) or
/// `/error` (a function error carrying exactly what it posted).
const CUSTOM_BOOTSTRAP: &str = r#"#!/bin/sh
set -eu
API="http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime"
while true; do
  HEADERS=$(mktemp)
  EVENT=$(curl -sS -D "$HEADERS" "$API/invocation/next")
  header() { grep -Fi "$1:" "$HEADERS" | cut -d: -f2- | tr -d '[:space:]'; }
  ID=$(header Lambda-Runtime-Aws-Request-Id)
  ARN=$(header Lambda-Runtime-Invoked-Function-Arn)
  DEADLINE=$(header Lambda-Runtime-Deadline-Ms)
  TRACE=$(header Lambda-Runtime-Trace-Id)
  case "$EVENT" in
    *raise*)
      curl -sS -o /dev/null -X POST "$API/invocation/$ID/error" \
        -H "Lambda-Runtime-Function-Error-Type: Custom.Failure" \
        -d '{"errorType":"Custom.Failure","errorMessage":"reported by bootstrap"}'
      ;;
    *headers*)
      curl -sS -o /dev/null -X POST "$API/invocation/$ID/response" \
        -d "{\"id\":\"$ID\",\"arn\":\"$ARN\",\"deadline\":\"$DEADLINE\",\"trace\":\"$TRACE\"}"
      ;;
    *)
      curl -sS -o /dev/null -X POST "$API/invocation/$ID/response" \
        -d '{"errorType":"ValidationError","errorMessage":"returned, not raised"}'
      ;;
  esac
done
"#;

#[tokio::test]
async fn custom_runtime_outcomes_are_what_it_reports() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    create(
        &client,
        "custom-rt",
        Runtime::Providedal2023,
        "bootstrap",
        &[("bootstrap", CUSTOM_BOOTSTRAP)],
        Some(7),
    )
    .await;

    let resp = invoke(&client, "custom-rt", "{}").await;
    assert_eq!(resp.function_error(), None);
    assert_eq!(json(&resp), error_shaped());

    let resp = invoke(&client, "custom-rt", r#"{"raise":true}"#).await;
    assert_eq!(resp.function_error(), Some("Unhandled"));
    assert_eq!(
        json(&resp),
        serde_json::json!({"errorType":"Custom.Failure","errorMessage":"reported by bootstrap"})
    );

    // The event carries the invocation's request ID, the ARN it was invoked
    // through and a deadline one timeout away.
    let resp = invoke(&client, "custom-rt", r#"{"headers":true}"#).await;
    let body = json(&resp);
    assert_eq!(body["id"], resp.request_id().unwrap());
    assert_eq!(
        body["arn"],
        "arn:aws:lambda:us-east-1:123456789012:function:custom-rt"
    );
    assert!(
        body["trace"].as_str().unwrap().starts_with("Root=1-"),
        "{body}"
    );
    let deadline: u128 = body["deadline"].as_str().unwrap().parse().unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    assert!(
        deadline > now && deadline <= now + 7_000,
        "deadline {deadline}, now {now}"
    );
}

#[tokio::test]
async fn raised_errors_are_unhandled_with_the_runtime_error_object() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    create(
        &client,
        "py-raise",
        Runtime::Python312,
        "index.handler",
        &[(
            "index.py",
            "def handler(event, context):\n    raise ValueError('boom')\n",
        )],
        None,
    )
    .await;
    create(
        &client,
        "node-raise",
        Runtime::Nodejs22x,
        "index.handler",
        &[(
            "index.js",
            "exports.handler = async () => { throw new TypeError('bad'); };\n",
        )],
        None,
    )
    .await;

    let resp = invoke(&client, "py-raise", "{}").await;
    assert_eq!(resp.function_error(), Some("Unhandled"));
    let body = json(&resp);
    assert_eq!(body["errorType"], "ValueError", "{body}");
    assert_eq!(body["errorMessage"], "boom", "{body}");
    assert!(body["stackTrace"].is_array(), "{body}");

    let resp = invoke(&client, "node-raise", "{}").await;
    assert_eq!(resp.function_error(), Some("Unhandled"));
    let body = json(&resp);
    assert_eq!(body["errorType"], "TypeError", "{body}");
    assert_eq!(body["errorMessage"], "bad", "{body}");
}

#[tokio::test]
async fn init_failure_is_unhandled_with_the_init_error() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    create(
        &client,
        "py-init-fail",
        Runtime::Python312,
        "index.handler",
        &[(
            "index.py",
            "import no_such_module\ndef handler(event, context):\n    return 1\n",
        )],
        None,
    )
    .await;

    for _ in 0..2 {
        let resp = invoke(&client, "py-init-fail", "{}").await;
        assert_eq!(resp.status_code(), 200);
        assert_eq!(resp.function_error(), Some("Unhandled"));
        let body = json(&resp);
        assert_eq!(body["errorType"], "Runtime.ImportModuleError", "{body}");
        assert!(
            body["errorMessage"]
                .as_str()
                .unwrap()
                .contains("No module named 'no_such_module'"),
            "{body}"
        );
    }
}

/// A timed-out invocation resets the environment: the next one starts a
/// fresh runtime (module state is gone) and succeeds.
#[tokio::test]
async fn timeout_resets_the_environment() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let code = "import time\ncount = 0\ndef handler(event, context):\n    global count\n    count += 1\n    if event.get('sleep'):\n        time.sleep(5)\n    return count\n";
    create(
        &client,
        "py-timeout",
        Runtime::Python312,
        "index.handler",
        &[("index.py", code)],
        Some(1),
    )
    .await;

    let resp = invoke(&client, "py-timeout", r#"{"sleep":true}"#).await;
    assert_eq!(resp.function_error(), Some("Unhandled"));
    let body = json(&resp);
    assert_eq!(body["errorType"], "Sandbox.Timedout");
    assert_eq!(
        body["errorMessage"],
        format!(
            "RequestId: {} Error: Task timed out after 1.00 seconds",
            resp.request_id().unwrap()
        )
    );

    let resp = invoke(&client, "py-timeout", "{}").await;
    assert_eq!(resp.function_error(), None);
    assert_eq!(json(&resp), serde_json::json!(1), "a fresh environment");
}

/// A runtime that exits mid-invocation is `Runtime.ExitError` with its exit
/// status, and the next invocation runs on a fresh environment.
#[tokio::test]
async fn runtime_exit_reports_the_exit_status() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let code = "import os\ndef handler(event, context):\n    if event.get('exit'):\n        os._exit(3)\n    return 'alive'\n";
    create(
        &client,
        "py-exit",
        Runtime::Python312,
        "index.handler",
        &[("index.py", code)],
        None,
    )
    .await;

    let resp = invoke(&client, "py-exit", r#"{"exit":true}"#).await;
    assert_eq!(resp.function_error(), Some("Unhandled"));
    let body = json(&resp);
    assert_eq!(body["errorType"], "Runtime.ExitError");
    assert_eq!(
        body["errorMessage"],
        format!(
            "RequestId: {} Error: Runtime exited with error: exit status 3",
            resp.request_id().unwrap()
        )
    );
    let resp = invoke(&client, "py-exit", "{}").await;
    assert_eq!(resp.function_error(), None);
    assert_eq!(json(&resp), serde_json::json!("alive"));
}

/// Consecutive invocations reuse the warm environment (module state
/// survives), and the context carries the request ID, the invoked ARN and
/// the client context.
#[tokio::test]
async fn warm_reuse_and_context() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let code = "count = 0\ndef handler(event, context):\n    global count\n    count += 1\n    cc = context.client_context\n    return {'count': count, 'id': context.aws_request_id, 'arn': context.invoked_function_arn, 'custom': cc.custom if cc else None}\n";
    create(
        &client,
        "py-warm",
        Runtime::Python312,
        "index.handler",
        &[("index.py", code)],
        None,
    )
    .await;
    client
        .publish_version()
        .function_name("py-warm")
        .send()
        .await
        .unwrap();

    for expected in 1..=3 {
        let resp = invoke(&client, "py-warm", "{}").await;
        let body = json(&resp);
        assert_eq!(body["count"], expected, "warm reuse keeps module state");
        assert_eq!(body["id"], resp.request_id().unwrap());
        assert_eq!(
            body["arn"],
            "arn:aws:lambda:us-east-1:123456789012:function:py-warm"
        );
    }

    use base64::Engine as _;
    let client_context = base64::engine::general_purpose::STANDARD
        .encode(r#"{"custom":{"tenant":"t-1"},"client":{},"env":{}}"#);
    let resp = client
        .invoke()
        .function_name("py-warm")
        .qualifier("1")
        .client_context(client_context)
        .payload(Blob::new(b"{}".to_vec()))
        .send()
        .await
        .unwrap();
    let body = json(&resp);
    assert_eq!(
        body["arn"],
        "arn:aws:lambda:us-east-1:123456789012:function:py-warm:1"
    );
    assert_eq!(body["custom"], serde_json::json!({"tenant": "t-1"}));
}

/// Concurrent invocations each get their own answer: the pool scales out
/// and no instance ever answers another's event.
#[tokio::test]
async fn concurrent_invocations_never_cross_wires() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let code = "import time\ndef handler(event, context):\n    time.sleep(1)\n    return {'n': event['n'], 'id': context.aws_request_id}\n";
    create(
        &client,
        "py-conc",
        Runtime::Python312,
        "index.handler",
        &[("index.py", code)],
        Some(30),
    )
    .await;
    // Warm one instance so the burst below exercises reuse and scale-out.
    invoke(&client, "py-conc", r#"{"n":-1}"#).await;

    let mut handles = Vec::new();
    for n in 0..6 {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            let resp = invoke(&client, "py-conc", &format!(r#"{{"n":{n}}}"#)).await;
            let body = json(&resp);
            assert_eq!(resp.function_error(), None, "{body}");
            assert_eq!(body["n"], n);
            assert_eq!(body["id"], resp.request_id().unwrap());
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

async fn queue(sqs: &aws_sdk_sqs::Client, name: &str) -> (String, String) {
    let url = sqs
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let arn = sqs
        .get_queue_attributes()
        .queue_url(&url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();
    (url, arn)
}

async fn receive_one(sqs: &aws_sdk_sqs::Client, url: &str) -> serde_json::Value {
    for _ in 0..60 {
        let out = sqs
            .receive_message()
            .queue_url(url)
            .wait_time_seconds(1)
            .send()
            .await
            .unwrap();
        if let Some(m) = out.messages().first() {
            return serde_json::from_str(m.body().unwrap()).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("no destination record arrived on {url}");
}

/// An asynchronous invocation's outcome follows the runtime's report too:
/// an error-shaped return goes to `OnSuccess`, a raised error to
/// `OnFailure`.
#[tokio::test]
async fn async_invoke_destinations_follow_the_reported_outcome() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let sqs = server.sqs_client().await;
    let (ok_url, ok_arn) = queue(&sqs, "rtapi-on-success").await;
    let (fail_url, fail_arn) = queue(&sqs, "rtapi-on-failure").await;
    let code = format!(
        "def handler(event, context):\n    if event.get('raise'):\n        raise RuntimeError('async boom')\n    return {ERROR_SHAPED}\n"
    );
    create(
        &client,
        "py-async",
        Runtime::Python312,
        "index.handler",
        &[("index.py", &code)],
        None,
    )
    .await;
    client
        .put_function_event_invoke_config()
        .function_name("py-async")
        .maximum_retry_attempts(0)
        .destination_config(
            DestinationConfig::builder()
                .on_success(OnSuccess::builder().destination(&ok_arn).build())
                .on_failure(OnFailure::builder().destination(&fail_arn).build())
                .build(),
        )
        .send()
        .await
        .unwrap();

    for payload in ["{}", r#"{"raise":true}"#] {
        let resp = client
            .invoke()
            .function_name("py-async")
            .invocation_type(InvocationType::Event)
            .payload(Blob::new(payload.as_bytes().to_vec()))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status_code(), 202);
    }

    let success = receive_one(&sqs, &ok_url).await;
    assert_eq!(
        success["responseContext"]["functionError"],
        serde_json::Value::Null
    );
    assert_eq!(success["responsePayload"], error_shaped());
    assert_eq!(success["requestContext"]["condition"], "Success");

    let failure = receive_one(&sqs, &fail_url).await;
    assert_eq!(failure["responseContext"]["functionError"], "Unhandled");
    assert_eq!(failure["responsePayload"]["errorType"], "RuntimeError");
    assert_eq!(failure["responsePayload"]["errorMessage"], "async boom");
}

/// An extension shipped in a layer is started beside the runtime, registers
/// with the Extensions API and receives an `INVOKE` event per invocation.
const EXTENSION: &str = r#"#!/bin/sh
API="http://${AWS_LAMBDA_RUNTIME_API}/2020-01-01/extension"
H=$(mktemp)
curl -sS -D "$H" -o /dev/null -X POST "$API/register" \
  -H "Lambda-Extension-Name: probe-ext" -d '{"events":["INVOKE"]}'
ID=$(grep -Fi Lambda-Extension-Identifier "$H" | cut -d: -f2- | tr -d '[:space:]')
while true; do
  curl -sS "$API/event/next" -H "Lambda-Extension-Identifier: $ID" >> /tmp/ext-events
  echo >> /tmp/ext-events
done
"#;

const EVENTS_BOOTSTRAP: &str = r#"#!/bin/sh
set -eu
API="http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime"
while true; do
  HEADERS=$(mktemp)
  curl -sS -D "$HEADERS" -o /dev/null "$API/invocation/next"
  ID=$(grep -Fi Lambda-Runtime-Aws-Request-Id "$HEADERS" | cut -d: -f2- | tr -d '[:space:]')
  touch /tmp/ext-events
  curl -sS -o /dev/null -X POST "$API/invocation/$ID/response" --data-binary @/tmp/ext-events
done
"#;

#[tokio::test]
async fn layer_extensions_receive_invoke_events() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let layer = client
        .publish_layer_version()
        .layer_name("probe-ext")
        .content(
            aws_sdk_lambda::types::LayerVersionContentInput::builder()
                .zip_file(Blob::new(make_zip(&[("extensions/probe-ext", EXTENSION)])))
                .build(),
        )
        .send()
        .await
        .unwrap();
    client
        .create_function()
        .function_name("with-ext")
        .runtime(Runtime::Providedal2023)
        .role(ROLE)
        .handler("bootstrap")
        .layers(layer.layer_version_arn().unwrap())
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(make_zip(&[("bootstrap", EVENTS_BOOTSTRAP)])))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let first = invoke(&client, "with-ext", "{}").await;
    assert_eq!(first.function_error(), None);
    let first_id = first.request_id().unwrap().to_string();
    // The extension saw the first invocation before the second one starts:
    // an event waits until every extension is done with the previous one.
    let second = invoke(&client, "with-ext", "{}").await;
    let seen = String::from_utf8(second.payload().unwrap().as_ref().to_vec()).unwrap();
    let event: serde_json::Value = seen
        .lines()
        .find(|l| l.contains(&first_id))
        .map(|l| serde_json::from_str(l).unwrap())
        .unwrap_or_else(|| panic!("no INVOKE for {first_id} in {seen:?}"));
    assert_eq!(event["eventType"], "INVOKE");
    assert_eq!(
        event["invokedFunctionArn"],
        "arn:aws:lambda:us-east-1:123456789012:function:with-ext"
    );
}

/// An asynchronous invocation through an alias tells the runtime the alias
/// ARN it was invoked through and passes the caller's trace header on.
#[tokio::test]
async fn async_invoke_keeps_the_invoked_arn_and_trace() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let sqs = server.sqs_client().await;
    let (url, arn) = queue(&sqs, "rtapi-async-ctx").await;
    let code = "import os\ndef handler(event, context):\n    return {'arn': context.invoked_function_arn, 'trace': os.environ.get('_X_AMZN_TRACE_ID')}\n";
    create(
        &client,
        "py-async-ctx",
        Runtime::Python312,
        "index.handler",
        &[("index.py", code)],
        None,
    )
    .await;
    let version = client
        .publish_version()
        .function_name("py-async-ctx")
        .send()
        .await
        .unwrap();
    client
        .create_alias()
        .function_name("py-async-ctx")
        .name("live")
        .function_version(version.version().unwrap())
        .send()
        .await
        .unwrap();
    client
        .put_function_event_invoke_config()
        .function_name("py-async-ctx")
        .qualifier("live")
        .destination_config(
            DestinationConfig::builder()
                .on_success(OnSuccess::builder().destination(&arn).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    let trace = "Root=1-67891233-abcdef012345678912345678;Parent=53995c3f42cd8ad8;Sampled=1";
    client
        .invoke()
        .function_name("py-async-ctx")
        .qualifier("live")
        .invocation_type(InvocationType::Event)
        .payload(Blob::new(b"{}".to_vec()))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("X-Amzn-Trace-Id", trace);
        })
        .send()
        .await
        .unwrap();
    let record = receive_one(&sqs, &url).await;
    assert_eq!(
        record["responsePayload"]["arn"],
        "arn:aws:lambda:us-east-1:123456789012:function:py-async-ctx:live"
    );
    assert_eq!(record["responsePayload"]["trace"], trace);
}

/// `LogType=Tail` returns only this invocation's output, stdout and stderr
/// in the order they were written (the prints are a second apart: Podman
/// timestamps log lines to the second).
#[tokio::test]
async fn log_tail_is_this_invocations_output_only() {
    use base64::Engine as _;
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let code = "import sys, time\ndef handler(event, context):\n    tag = event['tag']\n    print(f'{tag}-one', flush=True)\n    time.sleep(1.1)\n    print(f'{tag}-two', file=sys.stderr, flush=True)\n    time.sleep(1.1)\n    print(f'{tag}-three', flush=True)\n    return tag\n";
    create(
        &client,
        "py-tail",
        Runtime::Python312,
        "index.handler",
        &[("index.py", code)],
        Some(10),
    )
    .await;
    let tail = |tag: &'static str| {
        let client = client.clone();
        async move {
            let resp = client
                .invoke()
                .function_name("py-tail")
                .log_type(aws_sdk_lambda::types::LogType::Tail)
                .payload(Blob::new(format!("{{\"tag\":\"{tag}\"}}").into_bytes()))
                .send()
                .await
                .unwrap();
            let id = resp.request_id().unwrap().to_string();
            let logs = base64::engine::general_purpose::STANDARD
                .decode(resp.log_result().unwrap())
                .unwrap();
            (id, String::from_utf8(logs).unwrap())
        }
    };
    tail("first").await;
    let (id, logs) = tail("second").await;
    assert!(
        !logs.contains("first-"),
        "another invocation's output: {logs}"
    );
    let pos = |needle: &str| {
        logs.find(needle)
            .unwrap_or_else(|| panic!("{needle} missing from {logs}"))
    };
    assert!(pos(&format!("START RequestId: {id}")) < pos("second-one"));
    assert!(pos("second-one") < pos("second-two"), "{logs}");
    assert!(pos("second-two") < pos("second-three"), "{logs}");
    assert!(pos("second-three") < pos(&format!("END RequestId: {id}")));
}

async fn publish_layer(
    client: &aws_sdk_lambda::Client,
    name: &str,
    files: &[(&str, &str)],
) -> String {
    client
        .publish_layer_version()
        .layer_name(name)
        .content(
            aws_sdk_lambda::types::LayerVersionContentInput::builder()
                .zip_file(Blob::new(make_zip(files)))
                .build(),
        )
        .send()
        .await
        .unwrap()
        .layer_version_arn()
        .unwrap()
        .to_string()
}

/// An extension in the style of the Parameters and Secrets extension: it
/// serves `localhost:2773` and registers; the function's init code calls
/// it, which works because extensions start (and register) before the
/// runtime does.
const LOCAL_SERVER_EXTENSION: &str = r#"#!/var/lang/bin/python3
import http.server, json, os, threading, urllib.request
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"from-extension")
    def log_message(self, *a): pass
srv = http.server.HTTPServer(("127.0.0.1", 2773), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
api = "http://" + os.environ["AWS_LAMBDA_RUNTIME_API"] + "/2020-01-01/extension"
req = urllib.request.Request(api + "/register", data=json.dumps({"events": ["INVOKE"]}).encode(),
                             headers={"Lambda-Extension-Name": "local-server"}, method="POST")
ident = urllib.request.urlopen(req).headers["Lambda-Extension-Identifier"]
while True:
    urllib.request.urlopen(urllib.request.Request(api + "/event/next",
                           headers={"Lambda-Extension-Identifier": ident})).read()
"#;

#[tokio::test]
async fn function_init_can_call_an_extension() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let layer = publish_layer(
        &client,
        "local-server",
        &[("extensions/local-server", LOCAL_SERVER_EXTENSION)],
    )
    .await;
    let code = "import urllib.request\nFROM_INIT = urllib.request.urlopen('http://127.0.0.1:2773/', timeout=2).read().decode()\ndef handler(event, context):\n    return FROM_INIT\n";
    client
        .create_function()
        .function_name("init-calls-ext")
        .runtime(Runtime::Python312)
        .role(ROLE)
        .handler("index.handler")
        .layers(layer)
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(make_zip(&[("index.py", code)])))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let resp = invoke(&client, "init-calls-ext", "{}").await;
    assert_eq!(resp.function_error(), None, "{}", json(&resp));
    assert_eq!(json(&resp), serde_json::json!("from-extension"));
}

/// An extension that exits during init fails the invocation with
/// `Extension.Crash` instead of leaving it waiting for the extension.
#[tokio::test]
async fn a_crashing_extension_fails_the_init() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let layer = publish_layer(
        &client,
        "crasher",
        &[("extensions/crasher", "#!/bin/sh\necho crashing\nexit 1\n")],
    )
    .await;
    client
        .create_function()
        .function_name("with-crasher")
        .runtime(Runtime::Python312)
        .role(ROLE)
        .handler("index.handler")
        .layers(layer)
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(make_zip(&[(
                    "index.py",
                    "def handler(e, c):\n    return 1\n",
                )])))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let resp = invoke(&client, "with-crasher", "{}").await;
    assert!(started.elapsed() < Duration::from_secs(30));
    assert_eq!(resp.function_error(), Some("Unhandled"));
    let body = json(&resp);
    assert_eq!(body["errorType"], "Extension.Crash", "{body}");
    assert!(
        body["errorMessage"].as_str().unwrap().contains("crasher"),
        "{body}"
    );
}

/// An extension still working after the response keeps the environment for
/// the rest of the invoke phase (within the timeout) without it being
/// reset: once the work is done, the next event runs on the same warm
/// environment.
#[tokio::test]
async fn post_response_extension_work_does_not_reset_the_environment() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let ext = r#"#!/bin/sh
API="http://${AWS_LAMBDA_RUNTIME_API}/2020-01-01/extension"
H=$(mktemp)
curl -sS -D "$H" -o /dev/null -X POST "$API/register" -H "Lambda-Extension-Name: slow-ext" -d '{"events":["INVOKE"]}'
ID=$(grep -Fi Lambda-Extension-Identifier "$H" | cut -d: -f2- | tr -d '[:space:]')
while true; do
  curl -sS -o /dev/null "$API/event/next" -H "Lambda-Extension-Identifier: $ID"
  sleep 1.5
done
"#;
    let layer = publish_layer(&client, "slow-ext", &[("extensions/slow-ext", ext)]).await;
    let bootstrap = r#"#!/bin/sh
API="http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime"
N=0
while true; do
  HEADERS=$(mktemp)
  curl -sS -D "$HEADERS" -o /dev/null "$API/invocation/next"
  ID=$(grep -Fi Lambda-Runtime-Aws-Request-Id "$HEADERS" | cut -d: -f2- | tr -d '[:space:]')
  N=$((N+1))
  curl -sS -o /dev/null -X POST "$API/invocation/$ID/response" -d "$N"
done
"#;
    client
        .create_function()
        .function_name("slow-ext-fn")
        .runtime(Runtime::Providedal2023)
        .role(ROLE)
        .handler("bootstrap")
        .timeout(10)
        .layers(layer)
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(make_zip(&[("bootstrap", bootstrap)])))
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        json(&invoke(&client, "slow-ext-fn", "{}").await),
        serde_json::json!(1)
    );
    // Once the extension is done the environment is free again: not reset.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let resp = invoke(&client, "slow-ext-fn", "{}").await;
    assert_eq!(resp.function_error(), None);
    assert_eq!(
        json(&resp),
        serde_json::json!(2),
        "the same warm environment"
    );
}

/// An extension that exits between invocations, while parked in
/// `/event/next`, is noticed through its dropped request: the next
/// invocation fails with `Extension.Crash` carrying its exit status, and the
/// one after runs on a fresh environment.
#[tokio::test]
async fn an_extension_exiting_while_idle_fails_the_next_invocation() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let ext = r#"#!/bin/sh
API="http://${AWS_LAMBDA_RUNTIME_API}/2020-01-01/extension"
H=$(mktemp)
curl -sS -D "$H" -o /dev/null -X POST "$API/register" -H "Lambda-Extension-Name: quitter" -d '{"events":["INVOKE"]}'
ID=$(grep -Fi Lambda-Extension-Identifier "$H" | cut -d: -f2- | tr -d '[:space:]')
curl -sS -o /dev/null "$API/event/next" -H "Lambda-Extension-Identifier: $ID"
curl -sS -o /dev/null "$API/event/next" -H "Lambda-Extension-Identifier: $ID" &
NEXT=$!
sleep 3
kill "$NEXT"
exit 7
"#;
    let layer = publish_layer(&client, "quitter", &[("extensions/quitter", ext)]).await;
    let bootstrap = r#"#!/bin/sh
API="http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime"
N=0
while true; do
  HEADERS=$(mktemp)
  curl -sS -D "$HEADERS" -o /dev/null "$API/invocation/next"
  ID=$(grep -Fi Lambda-Runtime-Aws-Request-Id "$HEADERS" | cut -d: -f2- | tr -d '[:space:]')
  N=$((N+1))
  curl -sS -o /dev/null -X POST "$API/invocation/$ID/response" -d "$N"
done
"#;
    client
        .create_function()
        .function_name("quitter-fn")
        .runtime(Runtime::Providedal2023)
        .role(ROLE)
        .handler("bootstrap")
        .timeout(10)
        .layers(layer)
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(make_zip(&[("bootstrap", bootstrap)])))
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        json(&invoke(&client, "quitter-fn", "{}").await),
        serde_json::json!(1)
    );
    // The extension quits three seconds after the first event.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let resp = invoke(&client, "quitter-fn", "{}").await;
    assert_eq!(resp.function_error(), Some("Unhandled"));
    let body = json(&resp);
    assert_eq!(body["errorType"], "Extension.Crash", "{body}");
    assert!(
        body["errorMessage"]
            .as_str()
            .unwrap()
            .contains("extension quitter exited with error: exit status 7"),
        "{body}"
    );
    // A fresh environment (whose extension has not quit yet) runs the next.
    let resp = invoke(&client, "quitter-fn", "{}").await;
    assert_eq!(resp.function_error(), None, "{}", json(&resp));
    assert_eq!(json(&resp), serde_json::json!(1));
}

/// An integration targeting a version runs that version's code and
/// configuration, not `$LATEST`'s.
#[tokio::test]
async fn a_qualified_integration_invoke_runs_that_version() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let sqs = server.sqs_client().await;
    let eb = server.eventbridge_client().await;
    let (url, arn) = queue(&sqs, "rtapi-qualified").await;
    let code = |tag: &str| {
        format!("import os\ndef handler(event, context):\n    return {{'code': '{tag}', 'version': os.environ['AWS_LAMBDA_FUNCTION_VERSION'], 'arn': context.invoked_function_arn}}\n")
    };
    create(
        &client,
        "py-versions",
        Runtime::Python312,
        "index.handler",
        &[("index.py", &code("v1"))],
        None,
    )
    .await;
    client
        .publish_version()
        .function_name("py-versions")
        .send()
        .await
        .unwrap();
    client
        .update_function_code()
        .function_name("py-versions")
        .zip_file(Blob::new(make_zip(&[("index.py", &code("latest"))])))
        .send()
        .await
        .unwrap();
    let fn_arn = "arn:aws:lambda:us-east-1:123456789012:function:py-versions:1";
    client
        .put_function_event_invoke_config()
        .function_name("py-versions")
        .qualifier("1")
        .destination_config(
            DestinationConfig::builder()
                .on_success(OnSuccess::builder().destination(&arn).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    eb.put_rule()
        .name("to-v1")
        .event_pattern(r#"{"source":["rtapi.test"]}"#)
        .send()
        .await
        .unwrap();
    eb.put_targets()
        .rule("to-v1")
        .targets(
            aws_sdk_eventbridge::types::Target::builder()
                .id("fn")
                .arn(fn_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    eb.put_events()
        .entries(
            aws_sdk_eventbridge::types::PutEventsRequestEntry::builder()
                .source("rtapi.test")
                .detail_type("t")
                .detail("{}")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let record = receive_one(&sqs, &url).await;
    assert_eq!(record["responsePayload"]["code"], "v1", "{record}");
    assert_eq!(record["responsePayload"]["version"], "1");
    assert_eq!(record["responsePayload"]["arn"], fn_arn);
    // The record names the version that ran, not `$LATEST`.
    assert_eq!(
        record["responseContext"]["executedVersion"], "1",
        "{record}"
    );
}

/// A version that was never published is not found, synchronously and for
/// `Event`; it never runs `$LATEST` instead.
#[tokio::test]
async fn invoking_an_unknown_version_is_not_found() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    create(
        &client,
        "py-unknown-version",
        Runtime::Python312,
        "index.handler",
        &[("index.py", "def handler(e, c):\n    return 1\n")],
        None,
    )
    .await;
    for invocation_type in [InvocationType::RequestResponse, InvocationType::Event] {
        let err = client
            .invoke()
            .function_name("py-unknown-version")
            .qualifier("99")
            .invocation_type(invocation_type.clone())
            .payload(Blob::new(b"{}".to_vec()))
            .send()
            .await
            .expect_err("an unknown version must not run");
        let err = err.into_service_error();
        assert!(err.is_resource_not_found_exception(), "{err:?}");
        assert_eq!(
            err.meta().message(),
            Some("Function not found: arn:aws:lambda:us-east-1:123456789012:function:py-unknown-version:99"),
            "{invocation_type:?}"
        );
    }
}
