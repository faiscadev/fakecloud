mod helpers;

use std::io::Write;

use aws_sdk_apigatewayv2::types::ProtocolType;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{Environment, FunctionCode, Runtime};
use futures_util::sink::SinkExt;
use helpers::{sqs_receive_at_least, TestServer};

fn dockerized_endpoint(server: &TestServer) -> String {
    format!("http://host.docker.internal:{}", server.port())
}

fn dockerized_queue_url(server: &TestServer, queue_name: &str) -> String {
    format!(
        "http://host.docker.internal:{}/123456789012/{}",
        server.port(),
        queue_name
    )
}

fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    for (name, content) in entries {
        let options = zip::write::SimpleFileOptions::default().unix_permissions(0o755);
        writer.start_file(*name, options).unwrap();
        writer.write_all(content).unwrap();
    }
    let cursor = writer.finish().unwrap();
    cursor.into_inner()
}

fn python_sqs_writer_code() -> &'static str {
    r#"
import json
import os
import urllib.request
import urllib.parse

def lambda_handler(event, context):
    endpoint = os.environ["FAKECLOUD_ENDPOINT"]
    queue_url = os.environ["RESULT_QUEUE_URL"]

    params = urllib.parse.urlencode({
        "Action": "SendMessage",
        "QueueUrl": queue_url,
        "MessageBody": json.dumps({
            "marker": "lambda-executed",
            "route_key": event.get("requestContext", {}).get("routeKey"),
            "event_type": event.get("requestContext", {}).get("eventType"),
            "connection_id": event.get("requestContext", {}).get("connectionId"),
            "body": event.get("body"),
        }),
    }).encode()

    req = urllib.request.Request(endpoint, data=params, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("Authorization", (
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20200101/us-east-1/sqs/aws4_request, "
        "SignedHeaders=host, Signature=fake"
    ))
    urllib.request.urlopen(req)

    return {"statusCode": 200, "body": "marker sent"}
"#
}

async fn create_marker_lambda(
    server: &TestServer,
    sqs: &aws_sdk_sqs::Client,
    lambda: &aws_sdk_lambda::Client,
    queue_name: &str,
    function_name: &str,
) -> String {
    let queue = sqs
        .create_queue()
        .queue_name(queue_name)
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let docker_endpoint = dockerized_endpoint(server);
    let docker_queue_url = dockerized_queue_url(server, queue_name);

    let zip = make_zip(&[("lambda_function.py", python_sqs_writer_code().as_bytes())]);

    lambda
        .create_function()
        .function_name(function_name)
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("lambda_function.lambda_handler")
        .environment(
            Environment::builder()
                .variables("FAKECLOUD_ENDPOINT", &docker_endpoint)
                .variables("RESULT_QUEUE_URL", &docker_queue_url)
                .build(),
        )
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();

    queue_url
}

fn ws_url(server: &TestServer, api_id: &str, stage: &str) -> String {
    format!(
        "ws://127.0.0.1:{}/_fakecloud/apigatewayv2/ws/{}?stage={}",
        server.port(),
        api_id,
        stage
    )
}

fn container_cli_available() -> bool {
    for cli in ["docker", "podman"] {
        if std::process::Command::new(cli)
            .arg("info")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

#[tokio::test]
async fn websocket_connect_and_default_route_dispatches_to_lambda() {
    if !container_cli_available() {
        if std::env::var("CI").is_ok() {
            panic!("container runtime is not available but is required in CI");
        }
        eprintln!("skipping: container runtime not available");
        return;
    }

    let server = TestServer::start().await;
    let apigw = server.apigatewayv2_client().await;
    let lambda = server.lambda_client().await;
    let sqs = server.sqs_client().await;

    let queue_url = create_marker_lambda(
        &server,
        &sqs,
        &lambda,
        "apigwv2-ws-result",
        "apigwv2-ws-handler",
    )
    .await;

    // Create WebSocket API
    let api = apigw
        .create_api()
        .name("ws-test-api")
        .protocol_type(ProtocolType::Websocket)
        .send()
        .await
        .unwrap();
    let api_id = api.api_id().unwrap().to_string();

    // Create Lambda integration
    let integration = apigw
        .create_integration()
        .api_id(&api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
        .integration_uri("arn:aws:lambda:us-east-1:123456789012:function:apigwv2-ws-handler")
        .send()
        .await
        .unwrap();
    let integration_id = integration.integration_id().unwrap().to_string();

    // Create routes
    for route_key in ["$connect", "$default", "$disconnect"] {
        apigw
            .create_route()
            .api_id(&api_id)
            .route_key(route_key)
            .target(format!("integrations/{}", integration_id))
            .send()
            .await
            .unwrap();
    }

    // Create stage
    apigw
        .create_stage()
        .api_id(&api_id)
        .stage_name("dev")
        .send()
        .await
        .unwrap();

    // Connect via WebSocket
    let url = ws_url(&server, &api_id, "dev");
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Wait for $connect Lambda to fire
    let msgs = sqs_receive_at_least(&sqs, &queue_url, 1, std::time::Duration::from_secs(30)).await;
    assert!(!msgs.is_empty(), "$connect lambda should have fired");
    let body: serde_json::Value = serde_json::from_str(msgs[0].body().unwrap()).unwrap();
    assert_eq!(body["route_key"], "$connect");
    assert_eq!(body["event_type"], "CONNECT");
    let connection_id = body["connection_id"].as_str().unwrap().to_string();

    // Purge the queue so we can count new messages cleanly
    sqs.purge_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Send a text message — should dispatch to $default
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"action":"sendmessage"}"#.into(),
        ))
        .await
        .unwrap();

    let msgs = sqs_receive_at_least(&sqs, &queue_url, 1, std::time::Duration::from_secs(30)).await;
    assert!(
        !msgs.is_empty(),
        "$default lambda should have fired for message"
    );
    let body: serde_json::Value = serde_json::from_str(msgs[0].body().unwrap()).unwrap();
    assert_eq!(body["route_key"], "$default");
    assert_eq!(body["event_type"], "MESSAGE");
    assert_eq!(body["body"], r#"{"action":"sendmessage"}"#);
    assert_eq!(body["connection_id"].as_str().unwrap(), connection_id);

    // Purge again
    sqs.purge_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Close the WebSocket
    ws_stream.close(None).await.unwrap();

    // Wait for $disconnect Lambda to fire
    let msgs = sqs_receive_at_least(&sqs, &queue_url, 1, std::time::Duration::from_secs(30)).await;
    assert!(!msgs.is_empty(), "$disconnect lambda should have fired");
    let body: serde_json::Value = serde_json::from_str(msgs[0].body().unwrap()).unwrap();
    assert_eq!(body["route_key"], "$disconnect");
    assert_eq!(body["event_type"], "DISCONNECT");
    assert_eq!(body["connection_id"].as_str().unwrap(), connection_id);
}

#[tokio::test]
async fn websocket_message_falls_back_to_default_route() {
    if !container_cli_available() {
        if std::env::var("CI").is_ok() {
            panic!("container runtime is not available but is required in CI");
        }
        eprintln!("skipping: container runtime not available");
        return;
    }

    let server = TestServer::start().await;
    let apigw = server.apigatewayv2_client().await;
    let lambda = server.lambda_client().await;
    let sqs = server.sqs_client().await;

    let queue_url = create_marker_lambda(
        &server,
        &sqs,
        &lambda,
        "apigwv2-ws-default-result",
        "apigwv2-ws-default-handler",
    )
    .await;

    let api = apigw
        .create_api()
        .name("ws-default-test-api")
        .protocol_type(ProtocolType::Websocket)
        .send()
        .await
        .unwrap();
    let api_id = api.api_id().unwrap().to_string();

    let integration = apigw
        .create_integration()
        .api_id(&api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
        .integration_uri(
            "arn:aws:lambda:us-east-1:123456789012:function:apigwv2-ws-default-handler",
        )
        .send()
        .await
        .unwrap();
    let integration_id = integration.integration_id().unwrap().to_string();

    // Only create $connect and $default — no custom route for "sendmessage"
    for route_key in ["$connect", "$default"] {
        apigw
            .create_route()
            .api_id(&api_id)
            .route_key(route_key)
            .target(format!("integrations/{}", integration_id))
            .send()
            .await
            .unwrap();
    }

    apigw
        .create_stage()
        .api_id(&api_id)
        .stage_name("dev")
        .send()
        .await
        .unwrap();

    let url = ws_url(&server, &api_id, "dev");
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Wait for $connect
    let msgs = sqs_receive_at_least(&sqs, &queue_url, 1, std::time::Duration::from_secs(30)).await;
    assert!(!msgs.is_empty(), "$connect lambda should have fired");

    sqs.purge_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Send a message with an action that has no matching route
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            r#"{"action":"unknown"}"#.into(),
        ))
        .await
        .unwrap();

    let msgs = sqs_receive_at_least(&sqs, &queue_url, 1, std::time::Duration::from_secs(30)).await;
    assert!(
        !msgs.is_empty(),
        "$default lambda should have fired for unknown route"
    );
    let body: serde_json::Value = serde_json::from_str(msgs[0].body().unwrap()).unwrap();
    assert_eq!(body["route_key"], "$default");
    assert_eq!(body["event_type"], "MESSAGE");

    ws_stream.close(None).await.unwrap();
}

/// A WebSocket API whose `$connect` route proxies to a Python function
/// with `code` as `lambda_function.py`.
async fn connect_route_api(server: &TestServer, name: &str, code: &str) -> String {
    let apigw = server.apigatewayv2_client().await;
    let lambda = server.lambda_client().await;
    lambda
        .create_function()
        .function_name(name)
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("lambda_function.lambda_handler")
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(make_zip(&[(
                    "lambda_function.py",
                    code.as_bytes(),
                )])))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let api_id = apigw
        .create_api()
        .name(name)
        .protocol_type(ProtocolType::Websocket)
        .send()
        .await
        .unwrap()
        .api_id()
        .unwrap()
        .to_string();
    let integration_id = apigw
        .create_integration()
        .api_id(&api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::AwsProxy)
        .integration_uri(format!(
            "arn:aws:lambda:us-east-1:123456789012:function:{name}"
        ))
        .send()
        .await
        .unwrap()
        .integration_id()
        .unwrap()
        .to_string();
    apigw
        .create_route()
        .api_id(&api_id)
        .route_key("$connect")
        .target(format!("integrations/{integration_id}"))
        .send()
        .await
        .unwrap();
    apigw
        .create_stage()
        .api_id(&api_id)
        .stage_name("dev")
        .send()
        .await
        .unwrap();
    api_id
}

/// The refused upgrade's HTTP status and body.
async fn refused_connect(server: &TestServer, api_id: &str) -> (u16, String) {
    match tokio_tungstenite::connect_async(ws_url(server, api_id, "dev")).await {
        Ok(_) => panic!("the $connect failure must refuse the connection"),
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            let body = resp
                .body()
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            (resp.status().as_u16(), body)
        }
        Err(e) => panic!("unexpected handshake error: {e}"),
    }
}

/// Recorded on AWS (`apigateway_websocket_connect` in
/// fakecloud-lambda/tests/fixtures/function-errors/aws-recordings.json): a
/// `$connect` function error refuses the connection with 502
/// `{"message": "Internal server error", "connectionId":..., "requestId":...}`,
/// and a non-2xx `statusCode` refuses it with that status and body.
#[tokio::test]
async fn websocket_connect_failure_refuses_the_connection() {
    if !container_cli_available() {
        if std::env::var("CI").is_ok() {
            panic!("container runtime is not available but is required in CI");
        }
        eprintln!("skipping: container runtime not available");
        return;
    }
    let server = TestServer::start().await;

    let api_id = connect_route_api(
        &server,
        "ws-connect-raises",
        "def lambda_handler(event, context):\n    raise ValueError('no')\n",
    )
    .await;
    let (status, body) = refused_connect(&server, &api_id).await;
    assert_eq!(status, 502);
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["message"], "Internal server error");
    assert!(body["connectionId"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(body["requestId"].as_str().is_some_and(|s| !s.is_empty()));

    let api_id = connect_route_api(
        &server,
        "ws-connect-403",
        "def lambda_handler(event, context):\n    return {'statusCode': 403, 'body': 'nope'}\n",
    )
    .await;
    assert_eq!(
        refused_connect(&server, &api_id).await,
        (403, "nope".to_string())
    );
}
