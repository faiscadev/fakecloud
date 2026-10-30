//! End-to-end coverage for the SES SMTP listener.
//!
//! Real SES exposes an SMTP submission endpoint
//! (`email-smtp.<region>.amazonaws.com:587/2587`) that authenticates
//! using credentials produced by IAM `CreateServiceSpecificCredential`
//! with `ServiceName=ses.amazonaws.com`. fakecloud opts into the same
//! flow when `FAKECLOUD_SES_SMTP_PORT` is set: a TCP listener on that
//! port speaks the AUTH PLAIN / AUTH LOGIN subset of RFC 5321/4954,
//! validates the supplied user/pass against IAM state, and writes
//! accepted DATA payloads into the SES `sent_emails` ledger as
//! `SentEmail` records. These tests drive the whole loop from a real
//! TCP client and assert the message lands on the introspection
//! endpoint.
//!
//! The listener binds inside the spawned fakecloud process, so the test
//! pre-picks a free port via `TcpListener::bind("127.0.0.1:0")`, drops
//! the listener, and passes that port via the env var. The port may
//! technically be taken by another process between drop and rebind, but
//! the race window is tiny and rerunning the test is safe.

mod helpers;

use std::net::TcpListener as StdTcpListener;
use std::time::Duration;

use aws_sdk_iam::config::Region;
use helpers::TestServer;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

fn pick_free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

async fn read_line(rd: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> String {
    let mut buf = String::new();
    rd.read_line(&mut buf).await.expect("smtp read");
    buf
}

async fn wait_for_smtp(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("SMTP listener never came up on port {port}");
}

#[tokio::test]
async fn smtp_auth_plain_accepts_credentials_and_records_email() {
    let smtp_port = pick_free_port();
    let port_str = smtp_port.to_string();
    let server = TestServer::start_with_env(&[("FAKECLOUD_SES_SMTP_PORT", &port_str)]).await;

    // Provision an IAM user + ses SMTP credential.
    let iam_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await;
    let iam = aws_sdk_iam::Client::new(&iam_config);
    iam.create_user()
        .user_name("smtp-sender")
        .send()
        .await
        .expect("create user");
    let cred = iam
        .create_service_specific_credential()
        .user_name("smtp-sender")
        .service_name("ses.amazonaws.com")
        .send()
        .await
        .expect("create cred");
    let cred = cred.service_specific_credential().expect("cred body");
    let user = cred.service_user_name().to_string();
    let pass = cred.service_password().to_string();

    // Talk SMTP to the listener.
    let stream = wait_for_smtp(smtp_port).await;
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let banner = read_line(&mut rd).await;
    assert!(banner.starts_with("220 "), "banner = {banner:?}");

    wr.write_all(b"EHLO test\r\n").await.unwrap();
    // EHLO multi-line response — drain until terminal `250 `.
    loop {
        let line = read_line(&mut rd).await;
        assert!(line.starts_with("250"), "ehlo line = {line:?}");
        if line.starts_with("250 ") {
            break;
        }
    }

    let blob = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        format!("\0{user}\0{pass}").as_bytes(),
    );
    wr.write_all(format!("AUTH PLAIN {blob}\r\n").as_bytes())
        .await
        .unwrap();
    let auth_resp = read_line(&mut rd).await;
    assert!(auth_resp.starts_with("235 "), "auth = {auth_resp:?}");

    wr.write_all(b"MAIL FROM:<noreply@example.com>\r\n")
        .await
        .unwrap();
    assert!(read_line(&mut rd).await.starts_with("250 "));
    wr.write_all(b"RCPT TO:<dest@example.com>\r\n")
        .await
        .unwrap();
    assert!(read_line(&mut rd).await.starts_with("250 "));
    wr.write_all(b"DATA\r\n").await.unwrap();
    assert!(read_line(&mut rd).await.starts_with("354 "));
    wr.write_all(
        b"Subject: hi\r\nFrom: noreply@example.com\r\nTo: dest@example.com\r\n\r\nbody\r\n.\r\n",
    )
    .await
    .unwrap();
    assert!(read_line(&mut rd).await.starts_with("250 "));
    wr.write_all(b"QUIT\r\n").await.unwrap();
    let _ = read_line(&mut rd).await;

    // Drain the read side so the SMTP task on the server can finish.
    let mut sink = Vec::new();
    let _ = tokio::time::timeout(Duration::from_millis(50), rd.read_to_end(&mut sink)).await;

    // Verify SES recorded the message.
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/_fakecloud/ses/emails", server.endpoint()))
        .send()
        .await
        .expect("introspection get");
    assert!(resp.status().is_success(), "status = {}", resp.status());
    let body: serde_json::Value = resp.json().await.expect("json");
    let emails = body["emails"].as_array().expect("emails array");
    let email = emails
        .iter()
        .find(|e| e["from"].as_str() == Some("noreply@example.com"))
        .unwrap_or_else(|| panic!("expected SMTP-delivered email in introspection list: {body}"));
    // Recorded like an API raw send: plain MIME with its body parsed out.
    assert_eq!(
        email["rawData"],
        "Subject: hi\r\nFrom: noreply@example.com\r\nTo: dest@example.com\r\n\r\nbody\r\n"
    );
    assert_eq!(email["textBody"], "body\r\n");
}

#[tokio::test]
async fn smtp_rejects_unknown_credentials() {
    let smtp_port = pick_free_port();
    let port_str = smtp_port.to_string();
    let _server = TestServer::start_with_env(&[("FAKECLOUD_SES_SMTP_PORT", &port_str)]).await;
    let stream = wait_for_smtp(smtp_port).await;
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let _banner = read_line(&mut rd).await;

    wr.write_all(b"EHLO test\r\n").await.unwrap();
    loop {
        let line = read_line(&mut rd).await;
        if line.starts_with("250 ") {
            break;
        }
    }
    let blob = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"\0nope\0nope".as_slice(),
    );
    wr.write_all(format!("AUTH PLAIN {blob}\r\n").as_bytes())
        .await
        .unwrap();
    let auth_resp = read_line(&mut rd).await;
    assert!(auth_resp.starts_with("535 "), "auth = {auth_resp:?}");
}
