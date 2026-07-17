//! End-to-end coverage for the SES verified-identity gate.
//!
//! Real AWS SES rejects every send whose `From` is not a verified
//! identity, and additionally rejects every recipient that is not
//! verified while the account is still in the sandbox. fakecloud
//! mirrors both behaviors across SES v1 and SES v2; this suite drives
//! the gate end-to-end through the AWS SDKs so regressions in the
//! Smithy-translated request paths get caught.

mod helpers;

use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};
use helpers::TestServer;

/// Flip the running fakecloud server into sandbox mode for the default
/// account. fakecloud defaults to production-access-on so that the
/// majority of SES tests don't need to pre-verify recipients; this
/// helper opts a single test back into sandbox semantics via the
/// `/_fakecloud/ses/account/sandbox` admin endpoint.
async fn set_sandbox(server: &TestServer, sandbox: bool) {
    let url = format!("{}/_fakecloud/ses/account/sandbox", server.endpoint());
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({ "sandbox": sandbox }))
        .send()
        .await
        .expect("sandbox toggle request");
    assert!(
        resp.status().is_success(),
        "sandbox toggle failed: {}",
        resp.status()
    );
}

#[tokio::test]
async fn sandbox_rejects_unverified_recipient() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;
    set_sandbox(&server, true).await;

    // Sender is verified, recipient is not — sandbox rule says the send
    // must fail with `MessageRejected` (real SES) which fakecloud surfaces
    // unchanged through the SDK error.
    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();

    let err = client
        .send_email()
        .from_email_address("sender@example.com")
        .destination(
            Destination::builder()
                .to_addresses("recipient@unverified.test")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("Hi").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("Hello").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap_err();

    let dbg = format!("{err:?}");
    assert!(
        dbg.contains("MessageRejected") || dbg.contains("not verified"),
        "expected MessageRejected for unverified recipient, got: {dbg}"
    );
}

#[tokio::test]
async fn sandbox_accepts_verified_sender_and_recipient() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;
    set_sandbox(&server, true).await;

    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();
    client
        .create_email_identity()
        .email_identity("recipient@example.com")
        .send()
        .await
        .unwrap();

    let resp = client
        .send_email()
        .from_email_address("sender@example.com")
        .destination(
            Destination::builder()
                .to_addresses("recipient@example.com")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("Hi").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("Hello").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("verified-from + verified-to send should succeed in sandbox");

    assert!(!resp.message_id().unwrap_or_default().is_empty());
}

#[tokio::test]
async fn production_skips_recipient_verification() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;
    // fakecloud defaults to production access on, but flip explicitly so
    // the test documents the requirement rather than relying on the default.
    set_sandbox(&server, false).await;

    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();

    let resp = client
        .send_email()
        .from_email_address("sender@example.com")
        .destination(
            Destination::builder()
                .to_addresses("anyone@elsewhere.test")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("Hi").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("Hello").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("production accounts should not gate on recipient verification");

    assert!(!resp.message_id().unwrap_or_default().is_empty());
}

#[tokio::test]
async fn domain_verification_covers_all_addresses_on_that_domain() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;
    set_sandbox(&server, true).await;

    // Verify only the domain — every address on `example.com` should be
    // accepted as both sender and (sandbox-required) recipient.
    client
        .create_email_identity()
        .email_identity("example.com")
        .send()
        .await
        .unwrap();

    let resp = client
        .send_email()
        .from_email_address("anything@example.com")
        .destination(
            Destination::builder()
                .to_addresses("someone-else@example.com")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("Hi").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("Hello").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("domain identity should cover every address on the domain");

    assert!(!resp.message_id().unwrap_or_default().is_empty());
}

#[tokio::test]
async fn sandbox_rejects_unverified_sender() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;
    set_sandbox(&server, true).await;

    // No identities at all — the From-address gate must trip. AWS SES returns
    // `MessageRejected` ("Email address is not verified...") for an unverified
    // sender on both v1 and v2; `MailFromDomainNotVerifiedException` is reserved
    // for a configured-but-unverified custom MAIL FROM domain.
    let err = client
        .send_email()
        .from_email_address("noreply@unverified.test")
        .destination(
            Destination::builder()
                .to_addresses("anyone@unverified.test")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("Hi").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("Hello").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap_err();

    let dbg = format!("{err:?}");
    assert!(
        dbg.contains("MessageRejected"),
        "expected MessageRejected, got: {dbg}"
    );
}

#[tokio::test]
async fn simulator_addresses_bypass_the_gate() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;
    set_sandbox(&server, true).await;

    // Sender stays verified — the interesting bit is that *recipients*
    // on `simulator.amazonses.com` are accepted without verification, the
    // way real SES treats them.
    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();

    for recipient in [
        "bounce@simulator.amazonses.com",
        "complaint@simulator.amazonses.com",
        "success@simulator.amazonses.com",
        "suppressionlist@simulator.amazonses.com",
    ] {
        let resp = client
            .send_email()
            .from_email_address("sender@example.com")
            .destination(Destination::builder().to_addresses(recipient).build())
            .content(
                EmailContent::builder()
                    .simple(
                        Message::builder()
                            .subject(Content::builder().data("Hi").build().unwrap())
                            .body(
                                Body::builder()
                                    .text(Content::builder().data("Hello").build().unwrap())
                                    .build(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!("simulator recipient {recipient} should be accepted: {e:?}")
            });
        assert!(!resp.message_id().unwrap_or_default().is_empty());
    }

    // Sending *from* the simulator domain should also work even with no
    // verified identities matching it.
    let resp = client
        .send_email()
        .from_email_address("ooto@simulator.amazonses.com")
        .destination(
            Destination::builder()
                .to_addresses("bounce@simulator.amazonses.com")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("Hi").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("Hello").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("simulator sender should bypass the verified-identity gate");
    assert!(!resp.message_id().unwrap_or_default().is_empty());
}

#[tokio::test]
async fn v1_send_email_rejects_unverified_sender_with_message_rejected() {
    let server = TestServer::start().await;
    let v1 = server.ses_client().await;
    set_sandbox(&server, true).await;

    // SES v1 surfaces the same gate as `MessageRejected` (not the v2
    // `MailFromDomainNotVerifiedException`).
    let err = v1
        .send_email()
        .source("noreply@unverified.test")
        .destination(
            aws_sdk_ses::types::Destination::builder()
                .to_addresses("any@unverified.test")
                .build(),
        )
        .message(
            aws_sdk_ses::types::Message::builder()
                .subject(
                    aws_sdk_ses::types::Content::builder()
                        .data("Hi")
                        .build()
                        .unwrap(),
                )
                .body(
                    aws_sdk_ses::types::Body::builder()
                        .text(
                            aws_sdk_ses::types::Content::builder()
                                .data("Hello")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    let dbg = format!("{err:?}");
    assert!(
        dbg.contains("MessageRejected") || dbg.contains("not verified"),
        "expected MessageRejected from SES v1, got: {dbg}"
    );
}

#[tokio::test]
async fn v1_send_email_succeeds_with_verified_sender_in_production() {
    let server = TestServer::start().await;
    let v1 = server.ses_client().await;
    set_sandbox(&server, false).await;

    v1.verify_email_identity()
        .email_address("sender@example.com")
        .send()
        .await
        .unwrap();

    v1.send_email()
        .source("sender@example.com")
        .destination(
            aws_sdk_ses::types::Destination::builder()
                .to_addresses("anyone@elsewhere.test")
                .build(),
        )
        .message(
            aws_sdk_ses::types::Message::builder()
                .subject(
                    aws_sdk_ses::types::Content::builder()
                        .data("Hi")
                        .build()
                        .unwrap(),
                )
                .body(
                    aws_sdk_ses::types::Body::builder()
                        .text(
                            aws_sdk_ses::types::Content::builder()
                                .data("Hello")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("verified v1 sender should succeed in production");
}
