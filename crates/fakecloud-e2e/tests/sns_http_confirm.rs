//! End-to-end coverage for SNS HTTP/HTTPS subscription confirmation.
//!
//! Real AWS POSTs a SubscriptionConfirmation envelope to the subscriber
//! endpoint when an HTTP/HTTPS subscription is created. The subscriber
//! is then expected to call `ConfirmSubscription` with the embedded
//! Token before any notifications fan out. These tests spin up a tiny
//! TCP server, subscribe it to a topic, capture the inbound POST, and
//! drive the round-trip end-to-end.

mod helpers;

use std::sync::Arc;
use std::time::Duration;

use helpers::TestServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// Fetch the signing cert from the envelope's SigningCertURL, rebuild the AWS
/// SubscriptionConfirmation canonical string from the envelope's own fields,
/// and verify the RSA-SHA256 signature against the cert's public key. This is
/// exactly what `sns_message_validator` libraries do across languages.
async fn verify_confirmation_signature(body: &serde_json::Value) {
    use base64::Engine;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::pkcs8::DecodePublicKey;
    use rsa::sha2::Sha256;
    use rsa::signature::Verifier;
    use rsa::RsaPublicKey;
    use x509_cert::der::{DecodePem, Encode};
    use x509_cert::Certificate;

    let cert_url = body["SigningCertURL"].as_str().unwrap();
    assert!(cert_url.ends_with("/_fakecloud/sns/cert.pem"), "{cert_url}");
    let pem = reqwest::get(cert_url).await.unwrap().text().await.unwrap();
    assert!(pem.starts_with("-----BEGIN CERTIFICATE-----"));

    let cert = Certificate::from_pem(pem.as_bytes()).unwrap();
    let spki_der = cert
        .tbs_certificate
        .subject_public_key_info
        .to_der()
        .unwrap();
    let public_key = RsaPublicKey::from_public_key_der(&spki_der).unwrap();
    let verifying = VerifyingKey::<Sha256>::new(public_key);

    // Canonical string: alphabetical Key\nValue\n pairs for a
    // SubscriptionConfirmation envelope.
    let mut canonical = String::new();
    for key in [
        "Message",
        "MessageId",
        "SubscribeURL",
        "Timestamp",
        "Token",
        "TopicArn",
        "Type",
    ] {
        canonical.push_str(key);
        canonical.push('\n');
        canonical.push_str(body[key].as_str().unwrap());
        canonical.push('\n');
    }

    let sig_bytes = base64::engine::general_purpose::STANDARD
        .decode(body["Signature"].as_str().unwrap())
        .unwrap();
    let signature = Signature::try_from(sig_bytes.as_slice()).unwrap();
    verifying
        .verify(canonical.as_bytes(), &signature)
        .expect("confirmation signature must verify against fakecloud's public cert");
}

/// Locate the end-of-headers `\r\n\r\n` marker in a raw HTTP buffer.
/// Returns the index of the first byte of the marker.
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Inbound HTTP request captured by `MockSubscriber`. Stores the raw
/// bytes; tests parse out headers and body as needed.
#[derive(Clone, Debug)]
struct CapturedRequest {
    raw: String,
}

impl CapturedRequest {
    fn header(&self, name: &str) -> Option<String> {
        let lower = name.to_ascii_lowercase();
        for line in self.raw.lines().skip(1) {
            if line.is_empty() {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                if k.trim().to_ascii_lowercase() == lower {
                    return Some(v.trim().to_string());
                }
            }
        }
        None
    }

    fn body(&self) -> &str {
        self.raw
            .split_once("\r\n\r\n")
            .map(|(_, b)| b)
            .unwrap_or("")
    }
}

/// Tiny HTTP server that records every inbound request and replies
/// 200 OK. Stays alive for the duration of the test via `tokio::spawn`.
struct MockSubscriber {
    url: String,
    received: Arc<Mutex<Vec<CapturedRequest>>>,
}

impl MockSubscriber {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/sns-hook");
        let received: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let received_for_task = received.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let received = received_for_task.clone();
                tokio::spawn(async move {
                    // Read until headers complete then keep reading for the
                    // declared Content-Length. A single `read` would race
                    // the kernel splitting the request across packets on
                    // a loaded CI runner.
                    let mut acc: Vec<u8> = Vec::with_capacity(8 * 1024);
                    let mut chunk = [0u8; 4096];
                    let mut headers_end: Option<usize> = None;
                    let mut content_length: usize = 0;
                    loop {
                        let n = match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        acc.extend_from_slice(&chunk[..n]);
                        if headers_end.is_none() {
                            if let Some(idx) = find_header_end(&acc) {
                                headers_end = Some(idx);
                                let header_slice = &acc[..idx];
                                let header_str = String::from_utf8_lossy(header_slice);
                                for line in header_str.lines() {
                                    if let Some(v) =
                                        line.to_ascii_lowercase().strip_prefix("content-length:")
                                    {
                                        content_length = v.trim().parse().unwrap_or(0);
                                    }
                                }
                            }
                        }
                        if let Some(end) = headers_end {
                            let body_start = end + 4; // past \r\n\r\n
                            if acc.len() >= body_start + content_length {
                                break;
                            }
                        }
                    }
                    let raw = String::from_utf8_lossy(&acc).to_string();
                    received.lock().await.push(CapturedRequest { raw });
                    let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        Self { url, received }
    }

    /// Wait up to `timeout` for at least `n` requests to arrive.
    async fn wait_for(&self, n: usize, timeout: Duration) -> Vec<CapturedRequest> {
        let start = std::time::Instant::now();
        loop {
            {
                let guard = self.received.lock().await;
                if guard.len() >= n {
                    return guard.clone();
                }
            }
            if start.elapsed() > timeout {
                let guard = self.received.lock().await;
                panic!("timed out waiting for {n} requests; got {}", guard.len());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

#[tokio::test]
async fn sns_http_subscribe_posts_confirmation_envelope_and_publish_routes_after_confirm() {
    let server = TestServer::start().await;
    let sns = server.sns_client().await;
    let subscriber = MockSubscriber::start().await;

    let topic = sns
        .create_topic()
        .name("http-confirm-e2e")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    // Subscribe with HTTP — fakecloud should POST a
    // SubscriptionConfirmation envelope to the endpoint asynchronously.
    let sub = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("http")
        .endpoint(&subscriber.url)
        .send()
        .await
        .unwrap();
    assert_eq!(sub.subscription_arn().unwrap(), "pending confirmation");

    // Wait for the confirmation POST. Generous timeout because nextest
    // partitions run dozens of E2E tests in parallel and the spawned
    // reqwest can be slow to schedule on a loaded CI runner.
    let requests = subscriber.wait_for(1, Duration::from_secs(30)).await;
    let confirmation = &requests[0];

    // Headers must match AWS conventions.
    assert_eq!(
        confirmation.header("x-amz-sns-message-type").as_deref(),
        Some("SubscriptionConfirmation")
    );
    assert_eq!(
        confirmation.header("x-amz-sns-topic-arn").as_deref(),
        Some(topic_arn.as_str())
    );
    assert_eq!(
        confirmation.header("content-type").as_deref(),
        Some("text/plain; charset=UTF-8")
    );

    // Body shape — JSON with the standard SNS confirmation fields.
    let body: serde_json::Value =
        serde_json::from_str(confirmation.body()).expect("confirmation body is JSON");
    assert_eq!(body["Type"], "SubscriptionConfirmation");
    assert_eq!(body["TopicArn"], topic_arn);
    assert!(body["Message"]
        .as_str()
        .unwrap()
        .contains("You have chosen to subscribe"));
    let token = body["Token"].as_str().expect("Token present").to_string();
    assert_eq!(token.len(), 256, "token should be 256 chars");
    assert!(token.chars().all(|c| c.is_ascii_alphanumeric()));
    assert!(body["MessageId"].is_string());
    assert!(body["Timestamp"].is_string());
    assert_eq!(body["SignatureVersion"], "2");
    let subscribe_url = body["SubscribeURL"].as_str().unwrap();
    assert!(subscribe_url.contains("Action=ConfirmSubscription"));
    assert!(subscribe_url.contains(&token));

    // The SubscriptionConfirmation signature must verify the same way a real
    // SNS verifier library would: fetch the cert from SigningCertURL, rebuild
    // the AWS canonical string from the envelope's own fields, and check the
    // RSA-SHA256 signature against the cert's public key.
    verify_confirmation_signature(&body).await;

    // Publish before confirmation — endpoint must NOT receive it.
    sns.publish()
        .topic_arn(&topic_arn)
        .message("blocked")
        .send()
        .await
        .unwrap();
    // Give any spurious delivery enough wall-clock time to land before
    // we assert nothing arrived. 500ms leaves margin on a busy runner
    // without making the test slow on the happy path.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        subscriber.received.lock().await.len(),
        1,
        "publish before confirmation must not fan out"
    );

    // Confirm with the issued token.
    let confirmed = sns
        .confirm_subscription()
        .topic_arn(&topic_arn)
        .token(&token)
        .send()
        .await
        .unwrap();
    let confirmed_arn = confirmed.subscription_arn().unwrap();
    assert!(
        confirmed_arn.starts_with(&topic_arn),
        "confirmation should return the real subscription ARN, got {confirmed_arn}"
    );

    // Now publish — the endpoint should receive a Notification POST.
    sns.publish()
        .topic_arn(&topic_arn)
        .message("after-confirm")
        .send()
        .await
        .unwrap();
    let requests = subscriber.wait_for(2, Duration::from_secs(30)).await;
    let notification = &requests[1];
    assert_eq!(
        notification.header("x-amz-sns-message-type").as_deref(),
        Some("Notification")
    );
    let n_body: serde_json::Value =
        serde_json::from_str(notification.body()).expect("notification body is JSON");
    assert_eq!(n_body["Type"], "Notification");
    assert_eq!(n_body["TopicArn"], topic_arn);
    assert_eq!(n_body["Message"], "after-confirm");
}

/// Regression for the dead confirmation handshake at the default endpoint:
/// a real HTTP/S subscriber confirms by issuing an UNSIGNED bare GET at the
/// SubscribeURL SNS hands it (no Authorization header). Previously these
/// unsigned `?Action=ConfirmSubscription` / `?Action=Unsubscribe` GETs fell
/// through to the apigateway catch-all and 404'd, so subscriptions stayed
/// PendingConfirmation forever. This drives the real subscriber round-trip:
/// confirm via the SubscribeURL, then unsubscribe via the UnsubscribeUrl,
/// both as plain unauthenticated GETs.
#[tokio::test]
async fn sns_unsigned_subscribe_and_unsubscribe_urls_route_to_sns() {
    let server = TestServer::start().await;
    let sns = server.sns_client().await;
    let subscriber = MockSubscriber::start().await;

    let topic = sns
        .create_topic()
        .name("unsigned-url-e2e")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("http")
        .endpoint(&subscriber.url)
        .send()
        .await
        .unwrap();

    // Capture the confirmation envelope and pull out the SubscribeURL exactly
    // as a real subscriber would.
    let requests = subscriber.wait_for(1, Duration::from_secs(30)).await;
    let body: serde_json::Value =
        serde_json::from_str(requests[0].body()).expect("confirmation body is JSON");
    let subscribe_url = body["SubscribeURL"].as_str().unwrap().to_string();
    assert!(subscribe_url.contains("Action=ConfirmSubscription"));

    // UNSIGNED GET to the SubscribeURL — no auth header at all. Must reach the
    // SNS handler (not 404 via apigateway) and confirm the subscription.
    let resp = reqwest::get(&subscribe_url).await.unwrap();
    assert!(
        resp.status().is_success(),
        "unsigned ConfirmSubscription GET must succeed, got {}",
        resp.status()
    );
    let confirm_body = resp.text().await.unwrap();
    assert!(
        confirm_body.contains("ConfirmSubscriptionResult"),
        "expected SNS ConfirmSubscription XML, got: {confirm_body}"
    );

    // It must no longer be PendingConfirmation: publish should now fan out.
    sns.publish()
        .topic_arn(&topic_arn)
        .message("after-unsigned-confirm")
        .send()
        .await
        .unwrap();
    let requests = subscriber.wait_for(2, Duration::from_secs(30)).await;
    let notification = &requests[1];
    let n_body: serde_json::Value =
        serde_json::from_str(notification.body()).expect("notification body is JSON");
    assert_eq!(n_body["Type"], "Notification");
    assert_eq!(n_body["Message"], "after-unsigned-confirm");

    // The Notification carries the UnsubscribeUrl — also an unsigned GET.
    let unsubscribe_url = n_body["UnsubscribeURL"].as_str().unwrap().to_string();
    assert!(unsubscribe_url.contains("Action=Unsubscribe"));

    let resp = reqwest::get(&unsubscribe_url).await.unwrap();
    assert!(
        resp.status().is_success(),
        "unsigned Unsubscribe GET must succeed, got {}",
        resp.status()
    );

    // After unsubscribe, publishing must not deliver anything new.
    let before = subscriber.received.lock().await.len();
    sns.publish()
        .topic_arn(&topic_arn)
        .message("after-unsubscribe")
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        subscriber.received.lock().await.len(),
        before,
        "publish after unsubscribe must not fan out"
    );
}

#[tokio::test]
async fn sns_http_confirm_rejects_bad_token() {
    let server = TestServer::start().await;
    let sns = server.sns_client().await;
    let subscriber = MockSubscriber::start().await;

    let topic = sns
        .create_topic()
        .name("bad-token-e2e")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("http")
        .endpoint(&subscriber.url)
        .send()
        .await
        .unwrap();

    let err = sns
        .confirm_subscription()
        .topic_arn(&topic_arn)
        .token("not-a-real-token")
        .send()
        .await
        .expect_err("bad token must reject");
    let dbg = format!("{err:?}");
    assert!(
        dbg.contains("InvalidParameter") || dbg.contains("Invalid token"),
        "expected InvalidParameter, got {dbg}"
    );
}
