//! Amazon MQ data-plane E2E: proves a fakecloud broker is a REAL, connectable
//! message broker, not a formatted-but-dead `*.amazonaws.com` endpoint.
//!
//! The ActiveMQ test creates a broker with a user, waits for it to reach
//! `RUNNING` (which only happens once the backing `apache/activemq-classic`
//! container actually accepts connections), then opens a raw STOMP session to
//! the broker's real mapped port, authenticates as the created user, and
//! produces + consumes a message through the live broker -- a message round
//! trip is the only thing that proves the data plane works.
//!
//! The RabbitMQ test creates a broker and performs a real AMQP 0-9-1 protocol
//! handshake against the mapped port, proving a genuine RabbitMQ broker is
//! listening.
//!
//! Gated on Docker (the broker containers). In CI's Docker partition a missing
//! Docker hard-fails rather than silently skipping.

mod helpers;

use std::time::Duration;

use aws_sdk_mq::types::{DeploymentMode, EngineType, User};
use helpers::TestServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_docker_or_skip(test: &str) -> bool {
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in CI");
    }
    eprintln!("Skipping {test}: docker not available");
    false
}

async fn mq_client(server: &TestServer) -> aws_sdk_mq::Client {
    aws_sdk_mq::Client::new(&server.aws_config().await)
}

/// Poll DescribeBroker until the broker reaches `RUNNING`, returning the
/// broker's `brokerInstances` endpoints. Fails loudly on `CREATION_FAILED`.
async fn wait_for_running(
    client: &aws_sdk_mq::Client,
    broker_id: &str,
    timeout_secs: u64,
) -> Vec<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let resp = client
            .describe_broker()
            .broker_id(broker_id)
            .send()
            .await
            .expect("describe broker");
        let state = resp.broker_state().map(|s| s.as_str()).unwrap_or("");
        if state == "RUNNING" {
            let mut endpoints = Vec::new();
            for inst in resp.broker_instances() {
                for e in inst.endpoints() {
                    endpoints.push(e.clone());
                }
            }
            assert!(
                !endpoints.is_empty(),
                "a RUNNING broker must advertise real endpoints"
            );
            return endpoints;
        }
        assert_ne!(
            state, "CREATION_FAILED",
            "broker container failed to start (data plane could not come up)"
        );
        if std::time::Instant::now() >= deadline {
            panic!("broker {broker_id} did not reach RUNNING within {timeout_secs}s (last state: {state})");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Parse `scheme://host:port` into `host:port`.
fn host_port(endpoint: &str) -> String {
    endpoint
        .split_once("://")
        .map(|(_, rest)| rest.to_string())
        .unwrap_or_else(|| endpoint.to_string())
}

#[tokio::test]
async fn activemq_broker_delivers_a_message_over_stomp() {
    if !require_docker_or_skip("activemq_broker_delivers_a_message_over_stomp") {
        return;
    }

    let server = TestServer::start().await;
    let client = mq_client(&server).await;

    let username = "fcadmin";
    let password = "FakecloudMQ1234";

    let created = client
        .create_broker()
        .broker_name("fc-activemq-dp")
        .engine_type(EngineType::Activemq)
        .host_instance_type("mq.t3.micro")
        .deployment_mode(DeploymentMode::SingleInstance)
        .publicly_accessible(false)
        .auto_minor_version_upgrade(false)
        .users(
            User::builder()
                .username(username)
                .password(password)
                .console_access(true)
                .build(),
        )
        .send()
        .await
        .expect("create broker");
    let broker_id = created.broker_id().expect("broker id").to_string();

    // The container starts in the background (image pull + JVM boot), so allow
    // a generous window before the broker accepts connections.
    let endpoints = wait_for_running(&client, &broker_id, 300).await;

    // The STOMP endpoint must be a REAL reachable address, not *.amazonaws.com.
    let stomp = endpoints
        .iter()
        .find(|e| e.starts_with("stomp://"))
        .expect("a STOMP endpoint");
    assert!(
        !stomp.contains("amazonaws.com"),
        "endpoint must be a real mapped port, got {stomp}"
    );
    let addr = host_port(stomp);

    // Open a raw STOMP session and round-trip a message through the live broker,
    // authenticating as the injected user (proves user injection too). The
    // broker is protocol-ready by the time it is RUNNING, but a freshly-bound
    // connector can still reset the very first connection a beat after startup,
    // so retry the authenticated handshake within a bounded window.
    let mut stream = stomp_authenticate(&addr, username, password).await;

    let dest = "/queue/fakecloud.dataplane";
    let subscribe = format!("SUBSCRIBE\nid:sub-0\ndestination:{dest}\nack:auto\n\n\0");
    stream
        .write_all(subscribe.as_bytes())
        .await
        .expect("SUBSCRIBE");

    let body = "HELLO_FROM_FAKECLOUD_MQ";
    let send = format!("SEND\ndestination:{dest}\ncontent-type:text/plain\n\n{body}\0");
    stream.write_all(send.as_bytes()).await.expect("SEND");

    // Read frames until the produced message is consumed back.
    let mut acc = String::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut stream))
            .await
            .expect("timed out awaiting MESSAGE frame");
        acc.push_str(&frame);
        if acc.contains("MESSAGE") && acc.contains(body) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "did not receive produced message back from broker; got: {acc}"
        );
    }

    // Deleting the broker tears the real container down.
    client
        .delete_broker()
        .broker_id(&broker_id)
        .send()
        .await
        .expect("delete broker");
}

#[tokio::test]
async fn rabbitmq_broker_speaks_amqp() {
    if !require_docker_or_skip("rabbitmq_broker_speaks_amqp") {
        return;
    }

    let server = TestServer::start().await;
    let client = mq_client(&server).await;

    let created = client
        .create_broker()
        .broker_name("fc-rabbitmq-dp")
        .engine_type(EngineType::Rabbitmq)
        .host_instance_type("mq.t3.micro")
        .deployment_mode(DeploymentMode::SingleInstance)
        .publicly_accessible(false)
        .auto_minor_version_upgrade(false)
        .users(
            User::builder()
                .username("fcrabbit")
                .password("FakecloudMQ1234")
                .console_access(true)
                .build(),
        )
        .send()
        .await
        .expect("create broker");
    let broker_id = created.broker_id().expect("broker id").to_string();

    let endpoints = wait_for_running(&client, &broker_id, 300).await;
    let amqp = endpoints
        .iter()
        .find(|e| e.starts_with("amqp://"))
        .expect("an AMQP endpoint");
    assert!(
        !amqp.contains("amazonaws.com"),
        "endpoint must be a real mapped port, got {amqp}"
    );
    let addr = host_port(amqp);

    // Perform the AMQP 0-9-1 protocol header handshake. A real RabbitMQ broker
    // responds to the protocol header with a `Connection.Start` method frame
    // (frame type 0x01) or, on a version mismatch, echoes its own protocol
    // header (`AMQP…`). Either way a genuine broker answers -- a dead endpoint
    // would refuse or reset the connection. Retry within a bounded window to
    // tolerate a first-connection reset just after the listener binds.
    let (buf, n) = amqp_handshake(&addr).await;
    assert!(n > 0, "RabbitMQ must answer the AMQP handshake");
    assert!(
        buf[0] == 0x01 || &buf[0..4] == b"AMQP",
        "expected an AMQP frame or protocol header, got bytes: {:?}",
        &buf[..n]
    );

    client
        .delete_broker()
        .broker_id(&broker_id)
        .send()
        .await
        .expect("delete broker");
}

/// Establish an authenticated STOMP session, retrying within a bounded window
/// on a first-connection reset (the connector may reset the very first
/// connection a beat after it binds). Returns the connected stream.
async fn stomp_authenticate(addr: &str, username: &str, password: &str) -> tokio::net::TcpStream {
    let connect = format!(
        "CONNECT\naccept-version:1.0,1.1,1.2\nhost:localhost\nlogin:{username}\npasscode:{password}\n\n\0"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match try_stomp_connect(addr, &connect).await {
            Ok(stream) => return stream,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "could not establish an authenticated STOMP session at {addr}: {e}"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// One STOMP connect attempt: TCP connect, send CONNECT, read a frame, and
/// require a `CONNECTED` reply (proving the injected user authenticated).
async fn try_stomp_connect(
    addr: &str,
    connect_frame: &str,
) -> Result<tokio::net::TcpStream, String> {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    stream
        .write_all(connect_frame.as_bytes())
        .await
        .map_err(|e| format!("send CONNECT: {e}"))?;
    let frame = read_frame_fallible(&mut stream)
        .await
        .map_err(|e| format!("read CONNECTED: {e}"))?;
    if frame.starts_with("CONNECTED") {
        Ok(stream)
    } else {
        Err(format!("expected CONNECTED, got: {frame}"))
    }
}

/// Perform the AMQP 0-9-1 protocol header handshake, retrying on a
/// first-connection reset. Returns the response bytes and their length.
async fn amqp_handshake(addr: &str) -> ([u8; 16], usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match try_amqp_handshake(addr).await {
            Ok(v) => return v,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "RabbitMQ did not answer the AMQP handshake at {addr}: {e}"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn try_amqp_handshake(addr: &str) -> Result<([u8; 16], usize), String> {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    stream
        .write_all(b"AMQP\x00\x00\x09\x01")
        .await
        .map_err(|e| format!("send AMQP header: {e}"))?;
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf))
        .await
        .map_err(|_| "timed out awaiting AMQP response".to_string())?
        .map_err(|e| format!("read AMQP response: {e}"))?;
    if n == 0 {
        return Err("broker closed the connection without answering".to_string());
    }
    Ok((buf, n))
}

/// Like `read_frame` but returns an error instead of panicking, so the
/// connect-retry helpers can treat a reset as retryable.
async fn read_frame_fallible(stream: &mut tokio::net::TcpStream) -> Result<String, String> {
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut byte))
            .await
            .map_err(|_| "timed out".to_string())?
            .map_err(|e| e.to_string())?;
        if n == 0 || byte[0] == 0 {
            break;
        }
        out.push(byte[0]);
    }
    Ok(String::from_utf8_lossy(&out).trim_start().to_string())
}

/// Read one STOMP frame (terminated by a NUL byte) and return it as a string,
/// trimming the trailing NUL and any inter-frame newlines.
async fn read_frame(stream: &mut tokio::net::TcpStream) -> String {
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await.expect("read frame byte");
        if n == 0 {
            break;
        }
        if byte[0] == 0 {
            break;
        }
        out.push(byte[0]);
    }
    String::from_utf8_lossy(&out).trim_start().to_string()
}
