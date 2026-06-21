mod helpers;

use std::path::Path;

use aws_sdk_acm::types::{Tag, ValidationMethod};
use helpers::TestServer;

/// Persistent server with a fast (1s) ACM auto-issue tick so the
/// PENDING_VALIDATION -> ISSUED transition is observable without long waits.
async fn start(data_path: &Path) -> TestServer {
    TestServer::start_full(
        &[
            ("FAKECLOUD_CONTAINER_CLI", "false"),
            ("FAKECLOUD_ACM_AUTO_ISSUE_SECS", "1"),
        ],
        &[
            "--storage-mode",
            "persistent",
            "--data-path",
            &data_path.display().to_string(),
        ],
    )
    .await
}

async fn wait_for_issued(acm: &aws_sdk_acm::Client, arn: &str) {
    for _ in 0..50 {
        let st = acm
            .describe_certificate()
            .certificate_arn(arn)
            .send()
            .await
            .unwrap()
            .certificate
            .and_then(|c| c.status)
            .map(|s| s.as_str().to_string());
        if st.as_deref() == Some("ISSUED") {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("certificate never reached ISSUED");
}

/// An issued certificate, its tags, and ISSUED status all survive a restart.
/// The tag write happens AFTER the cert is ISSUED, forcing a durable snapshot
/// that captures the auto-issue transition (no reliance on tick-timing races).
#[tokio::test]
async fn persistence_issued_cert_and_tags_survive_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = start(tmp.path()).await;
    let acm = server.acm_client().await;

    let arn = acm
        .request_certificate()
        .domain_name("example.com")
        .validation_method(ValidationMethod::Dns)
        .send()
        .await
        .unwrap()
        .certificate_arn
        .unwrap();

    wait_for_issued(&acm, &arn).await;

    acm.add_tags_to_certificate()
        .certificate_arn(&arn)
        .tags(Tag::builder().key("env").value("prod").build().unwrap())
        .send()
        .await
        .unwrap();

    server.restart().await;
    let acm = server.acm_client().await;

    let cert = acm
        .describe_certificate()
        .certificate_arn(&arn)
        .send()
        .await
        .unwrap()
        .certificate
        .unwrap();
    assert_eq!(cert.status().map(|s| s.as_str()), Some("ISSUED"));
    assert_eq!(cert.domain_name(), Some("example.com"));

    let tags = acm
        .list_tags_for_certificate()
        .certificate_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(tags
        .tags()
        .iter()
        .any(|t| t.key() == "env" && t.value() == Some("prod")));
}

/// A still-pending DNS certificate survives a restart and is re-armed so it
/// reaches ISSUED after the restart (rearm_pending_validations).
#[tokio::test]
async fn persistence_pending_cert_reissues_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    // Long auto-issue delay so the cert is still PENDING when we restart.
    let mut server = TestServer::start_full(
        &[
            ("FAKECLOUD_CONTAINER_CLI", "false"),
            ("FAKECLOUD_ACM_AUTO_ISSUE_SECS", "3"),
        ],
        &[
            "--storage-mode",
            "persistent",
            "--data-path",
            &tmp.path().display().to_string(),
        ],
    )
    .await;
    let acm = server.acm_client().await;

    let arn = acm
        .request_certificate()
        .domain_name("pending.example.com")
        .validation_method(ValidationMethod::Dns)
        .send()
        .await
        .unwrap()
        .certificate_arn
        .unwrap();

    // Restart well before the 3s tick fires; the cert persists as PENDING.
    server.restart().await;
    let acm = server.acm_client().await;

    // Survives restart...
    let status = acm
        .describe_certificate()
        .certificate_arn(&arn)
        .send()
        .await
        .unwrap()
        .certificate
        .and_then(|c| c.status)
        .map(|s| s.as_str().to_string());
    assert!(
        matches!(
            status.as_deref(),
            Some("PENDING_VALIDATION") | Some("ISSUED")
        ),
        "unexpected status after restart: {status:?}"
    );

    // ...and the re-armed tick still drives it to ISSUED.
    wait_for_issued(&acm, &arn).await;
}

/// A deleted certificate stays gone after restart.
#[tokio::test]
async fn persistence_delete_cert_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = start(tmp.path()).await;
    let acm = server.acm_client().await;

    let arn = acm
        .request_certificate()
        .domain_name("gone.example.com")
        .validation_method(ValidationMethod::Dns)
        .send()
        .await
        .unwrap()
        .certificate_arn
        .unwrap();
    acm.delete_certificate()
        .certificate_arn(&arn)
        .send()
        .await
        .unwrap();

    server.restart().await;
    let acm = server.acm_client().await;

    let listed = acm.list_certificates().send().await.unwrap();
    assert!(!listed
        .certificate_summary_list()
        .iter()
        .any(|c| c.certificate_arn() == Some(arn.as_str())));
}
