//! Amazon MQ -> Lambda event source mapping E2E: a mapping on a real broker
//! (the `apache/activemq-classic` / `rabbitmq` container behind the broker's
//! data plane) consumes the mapping's queue with the credentials of its
//! `BASIC_AUTH` secret and invokes a real function with AWS's `aws:mq` /
//! `aws:rmq` event. A batch the function accepts is acknowledged (gone from the
//! queue); one it fails is redelivered until the mapping is disabled, and is
//! still on the broker afterwards.
//!
//! Gated like the MQ broker data-plane suite: the heavy broker containers run
//! only in the dedicated `mq-broker` CI job (`FAKECLOUD_E2E_MQ_BROKER=1`), and
//! there a missing Docker hard-fails.

mod helpers;

use std::io::Write;
use std::time::Duration;

use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{FunctionCode, Runtime, SourceAccessConfiguration, SourceAccessType};
use aws_sdk_mq::types::{DeploymentMode, EngineType, User};
use fakecloud_mq::client::amqp::{AmqpConnection, BasicProperties, FieldValue};
use fakecloud_mq::client::stomp::StompConnection;
use helpers::TestServer;
use serde_json::Value;

const USERNAME: &str = "fcesm";
const PASSWORD: &str = "FakecloudMQ1234";

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_broker_or_skip(test: &str) -> bool {
    if std::env::var("FAKECLOUD_E2E_MQ_BROKER").as_deref() != Ok("1") {
        eprintln!(
            "Skipping {test}: FAKECLOUD_E2E_MQ_BROKER!=1 (runs in the dedicated mq-broker CI job)"
        );
        return false;
    }
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in the mq-broker CI job");
    }
    eprintln!("Skipping {test}: docker not available");
    false
}

/// Fails every batch holding a message whose body contains `FAIL`.
const HANDLER: &str = r#"
import base64

def handler(event, context):
    datas = [m["data"] for m in event.get("messages", [])]
    for messages in event.get("rmqMessagesByQueue", {}).values():
        datas += [m["data"] for m in messages]
    for data in datas:
        if b"FAIL" in base64.b64decode(data):
            raise Exception("refusing a FAIL message")
    return {"ok": True}
"#;

fn handler_zip() -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default().unix_permissions(0o644);
    writer.start_file("index.py", options).unwrap();
    writer.write_all(HANDLER.as_bytes()).unwrap();
    writer.finish().unwrap().into_inner()
}

async fn create_function(lambda: &aws_sdk_lambda::Client, name: &str) -> String {
    lambda
        .create_function()
        .function_name(name)
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/mq-esm-role")
        .handler("index.handler")
        .timeout(30)
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(handler_zip()))
                .build(),
        )
        .send()
        .await
        .expect("create function")
        .function_arn()
        .unwrap()
        .to_string()
}

async fn create_secret(server: &TestServer, name: &str) -> String {
    server
        .secretsmanager_client()
        .await
        .create_secret()
        .name(name)
        .secret_string(format!(
            r#"{{"username":"{USERNAME}","password":"{PASSWORD}"}}"#
        ))
        .send()
        .await
        .expect("create secret")
        .arn()
        .unwrap()
        .to_string()
}

/// Create a broker with the mapping's user and wait for it to run; returns
/// `(broker id, broker ARN, endpoints)`.
async fn running_broker(
    server: &TestServer,
    mq: &aws_sdk_mq::Client,
    name: &str,
    engine: EngineType,
) -> (String, String, Vec<String>) {
    let created = mq
        .create_broker()
        .broker_name(name)
        .engine_type(engine)
        .host_instance_type("mq.t3.micro")
        .deployment_mode(DeploymentMode::SingleInstance)
        .publicly_accessible(false)
        .auto_minor_version_upgrade(false)
        .users(
            User::builder()
                .username(USERNAME)
                .password(PASSWORD)
                .console_access(true)
                .build(),
        )
        .send()
        .await
        .expect("create broker");
    let id = created.broker_id().unwrap().to_string();
    let arn = created.broker_arn().unwrap().to_string();
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    loop {
        let resp = mq.describe_broker().broker_id(&id).send().await.unwrap();
        match resp.broker_state().map(|s| s.as_str()).unwrap_or("") {
            "RUNNING" => {
                let endpoints = resp
                    .broker_instances()
                    .iter()
                    .flat_map(|i| i.endpoints().iter().cloned())
                    .collect();
                return (id, arn, endpoints);
            }
            "CREATION_FAILED" => {
                helpers::dump_mq_broker_diagnostics(server, &id).await;
                panic!("broker {id} failed to start");
            }
            state => {
                if std::time::Instant::now() >= deadline {
                    helpers::dump_mq_broker_diagnostics(server, &id).await;
                    panic!("broker {id} did not reach RUNNING (last state: {state})");
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn endpoint_addr(endpoints: &[String], scheme: &str) -> String {
    let endpoint = endpoints
        .iter()
        .find(|e| e.starts_with(&format!("{scheme}://")))
        .unwrap_or_else(|| panic!("a {scheme} endpoint in {endpoints:?}"));
    endpoint.split_once("://").unwrap().1.to_string()
}

/// The payloads of the mapping's recorded invocations of `function_arn`.
async fn payloads(server: &TestServer, function_arn: &str, source: &str) -> Vec<Value> {
    let resp: Value = reqwest::get(format!(
        "{}/_fakecloud/lambda/invocations",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    resp["invocations"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|i| i["functionArn"] == function_arn && i["source"] == source)
        .map(|i| i["payload"].clone())
        .collect()
}

async fn wait_for_payloads(
    server: &TestServer,
    function_arn: &str,
    source: &str,
    what: &str,
    mut done: impl FnMut(&[Value]) -> bool,
) -> Vec<Value> {
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    loop {
        let seen = payloads(server, function_arn, source).await;
        if done(&seen) {
            return seen;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}; invocations so far: {seen:#?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn last_processing_result(lambda: &aws_sdk_lambda::Client, uuid: &str) -> String {
    lambda
        .get_event_source_mapping()
        .uuid(uuid)
        .send()
        .await
        .unwrap()
        .last_processing_result()
        .unwrap_or_default()
        .to_string()
}

async fn wait_for_result(lambda: &aws_sdk_lambda::Client, uuid: &str, want: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let got = last_processing_result(lambda, uuid).await;
        if got == want {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "LastProcessingResult stayed {got:?}, expected {want:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Disable the mapping and give the poller time to drop its consumer.
async fn disable(lambda: &aws_sdk_lambda::Client, uuid: &str) {
    lambda
        .update_event_source_mapping()
        .uuid(uuid)
        .enabled(false)
        .send()
        .await
        .expect("disable mapping");
    tokio::time::sleep(Duration::from_secs(5)).await;
}

fn decoded(data: &Value) -> String {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data.as_str().unwrap())
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

fn basic_auth(secret_arn: &str) -> SourceAccessConfiguration {
    SourceAccessConfiguration::builder()
        .r#type(SourceAccessType::BasicAuth)
        .uri(secret_arn)
        .build()
}

async fn stomp_session(addr: &str) -> StompConnection {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match StompConnection::connect(addr, USERNAME, PASSWORD).await {
            Ok(conn) => return conn,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "STOMP login at {addr} failed: {e}"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[tokio::test]
async fn activemq_mapping_invokes_the_function_and_acks_only_what_succeeds() {
    if !require_broker_or_skip("activemq_mapping_invokes_the_function_and_acks_only_what_succeeds")
    {
        return;
    }
    let server = TestServer::start().await;
    let mq = aws_sdk_mq::Client::new(&server.aws_config().await);
    let lambda = server.lambda_client().await;

    let (broker_id, broker_arn, endpoints) =
        running_broker(&server, &mq, "fc-esm-activemq", EngineType::Activemq).await;
    let addr = endpoint_addr(&endpoints, "stomp");
    let secret_arn = create_secret(&server, "mq-esm-activemq").await;
    let function_arn = create_function(&lambda, "mq-esm-activemq-fn").await;

    // Two messages before the mapping exists: they wait on the broker.
    let mut producer = stomp_session(&addr).await;
    producer
        .send(
            "/queue/orders",
            &[
                ("persistent", "true"),
                ("correlation-id", "corr-1"),
                ("priority", "7"),
                ("region", "eu"),
                ("receipt", "r1"),
            ],
            b"first order",
        )
        .await
        .expect("send first");
    producer
        .send(
            "/queue/orders",
            &[("content-length", "12"), ("receipt", "r2")],
            b"second order",
        )
        .await
        .expect("send second");

    let uuid = lambda
        .create_event_source_mapping()
        .function_name(&function_arn)
        .event_source_arn(&broker_arn)
        .queues("orders")
        .source_access_configurations(basic_auth(&secret_arn))
        .batch_size(10)
        .send()
        .await
        .expect("create mapping")
        .uuid()
        .unwrap()
        .to_string();

    let seen = wait_for_payloads(&server, &function_arn, "aws:mq", "the first batch", |p| {
        p.iter()
            .flat_map(|e| e["messages"].as_array().cloned().unwrap_or_default())
            .count()
            >= 2
    })
    .await;
    let event = &seen[0];
    assert_eq!(event["eventSource"], "aws:mq");
    assert_eq!(event["eventSourceArn"], broker_arn);
    let messages: Vec<Value> = seen
        .iter()
        .flat_map(|e| e["messages"].as_array().cloned().unwrap_or_default())
        .collect();
    let first = messages
        .iter()
        .find(|m| decoded(&m["data"]) == "first order")
        .expect("the first message");
    assert!(first["messageID"].as_str().unwrap().starts_with("ID:"));
    assert_eq!(first["messageType"], "jms/text-message");
    assert_eq!(first["deliveryMode"], 2);
    assert_eq!(first["correlationID"], "corr-1");
    assert_eq!(first["priority"], 7);
    assert_eq!(first["destination"]["physicalName"], "orders");
    assert_eq!(first["redelivered"], false);
    assert_eq!(first["properties"]["region"], "eu");
    assert!(first["timestamp"].as_i64().unwrap() > 0);
    assert!(first["brokerOutTime"].as_i64() >= first["brokerInTime"].as_i64());
    let second = messages
        .iter()
        .find(|m| decoded(&m["data"]) == "second order")
        .expect("the second message");
    assert_eq!(second["messageType"], "jms/bytes-message");
    wait_for_result(&lambda, &uuid, "OK").await;

    // A message the function fails is redelivered, again and again.
    producer
        .send("/queue/orders", &[("receipt", "r3")], b"FAIL this one")
        .await
        .expect("send failing");
    wait_for_payloads(
        &server,
        &function_arn,
        "aws:mq",
        "two failed attempts",
        |p| {
            p.iter()
                .flat_map(|e| e["messages"].as_array().cloned().unwrap_or_default())
                .filter(|m| decoded(&m["data"]) == "FAIL this one")
                .count()
                >= 2
        },
    )
    .await;
    wait_for_result(&lambda, &uuid, "PROBLEM: Function call failed.").await;

    // With the mapping off, the queue holds only the failed message: the
    // others were acknowledged.
    disable(&lambda, &uuid).await;
    let mut consumer = stomp_session(&addr).await;
    consumer
        .subscribe("check", "/queue/orders", "auto", &[])
        .await
        .unwrap();
    let mut left = Vec::new();
    while let Ok(Ok(frame)) =
        tokio::time::timeout(Duration::from_secs(5), consumer.next_message()).await
    {
        left.push(String::from_utf8_lossy(&frame.body).into_owned());
    }
    assert_eq!(left, ["FAIL this one"]);

    mq.delete_broker()
        .broker_id(&broker_id)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn rabbitmq_mapping_invokes_the_function_and_requeues_what_fails() {
    if !require_broker_or_skip("rabbitmq_mapping_invokes_the_function_and_requeues_what_fails") {
        return;
    }
    let server = TestServer::start().await;
    let mq = aws_sdk_mq::Client::new(&server.aws_config().await);
    let lambda = server.lambda_client().await;

    let (broker_id, broker_arn, endpoints) =
        running_broker(&server, &mq, "fc-esm-rabbitmq", EngineType::Rabbitmq).await;
    let addr = endpoint_addr(&endpoints, "amqp");
    let secret_arn = create_secret(&server, "mq-esm-rabbitmq").await;
    let function_arn = create_function(&lambda, "mq-esm-rabbitmq-fn").await;

    let mut producer = {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            match AmqpConnection::connect(&addr, "/", USERNAME, PASSWORD).await {
                Ok(conn) => break conn,
                Err(e) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "AMQP login at {addr} failed: {e}"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    };
    producer.declare_queue("orders", true).await.unwrap();
    producer.enable_confirms().await.unwrap();
    let props = BasicProperties {
        content_type: Some("text/plain".to_string()),
        headers: Some(vec![
            ("region".to_string(), FieldValue::LongString(b"eu".to_vec())),
            ("attempt".to_string(), FieldValue::I32(1)),
        ]),
        delivery_mode: Some(2),
        priority: Some(3),
        correlation_id: Some("corr-1".to_string()),
        message_id: Some("order-1".to_string()),
        ..Default::default()
    };
    producer
        .publish("", "orders", &props, b"first order")
        .await
        .unwrap();

    let uuid = lambda
        .create_event_source_mapping()
        .function_name(&function_arn)
        .event_source_arn(&broker_arn)
        .queues("orders")
        .source_access_configurations(basic_auth(&secret_arn))
        .batch_size(10)
        .send()
        .await
        .expect("create mapping")
        .uuid()
        .unwrap()
        .to_string();

    let seen = wait_for_payloads(&server, &function_arn, "aws:rmq", "the first batch", |p| {
        !p.is_empty()
    })
    .await;
    let event = &seen[0];
    assert_eq!(event["eventSource"], "aws:rmq");
    assert_eq!(event["eventSourceArn"], broker_arn);
    let message = &event["rmqMessagesByQueue"]["orders::/"][0];
    assert_eq!(decoded(&message["data"]), "first order");
    assert_eq!(message["redelivered"], false);
    let p = &message["basicProperties"];
    assert_eq!(p["contentType"], "text/plain");
    assert_eq!(
        p["headers"]["region"]["bytes"],
        serde_json::json!([101, 117])
    );
    assert_eq!(p["headers"]["attempt"], 1);
    assert_eq!(p["deliveryMode"], 2);
    assert_eq!(p["priority"], 3);
    assert_eq!(p["correlationId"], "corr-1");
    assert_eq!(p["messageId"], "order-1");
    assert_eq!(p["bodySize"], 11);
    wait_for_result(&lambda, &uuid, "OK").await;

    // A failed message is requeued and comes back marked redelivered.
    producer
        .publish("", "orders", &BasicProperties::default(), b"FAIL this one")
        .await
        .unwrap();
    let seen = wait_for_payloads(&server, &function_arn, "aws:rmq", "a redelivery", |p| {
        p.iter().any(|e| {
            e["rmqMessagesByQueue"]["orders::/"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|m| decoded(&m["data"]) == "FAIL this one" && m["redelivered"] == true)
        })
    })
    .await;
    assert!(!seen.is_empty());
    wait_for_result(&lambda, &uuid, "PROBLEM: Function call failed.").await;

    // With the mapping off, only the failed message is left in the queue.
    disable(&lambda, &uuid).await;
    producer.qos(10).await.unwrap();
    producer.consume("orders").await.unwrap();
    let mut left = Vec::new();
    while let Ok(Ok(delivery)) =
        tokio::time::timeout(Duration::from_secs(5), producer.next_delivery()).await
    {
        left.push(String::from_utf8_lossy(&delivery.body).into_owned());
    }
    assert_eq!(left, ["FAIL this one"]);
    producer.close().await;

    mq.delete_broker()
        .broker_id(&broker_id)
        .send()
        .await
        .unwrap();
}
